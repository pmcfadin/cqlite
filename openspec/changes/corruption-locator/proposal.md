# corruption-locator — issue #4194 (epic #4192, second slice)

**Milestone:** unmilestoned (owner convention; board `Ready` → claimed). **Priority:** P1.
**Routing:** design-driven (OpenSpec + Seam 1) — extending a stable public report shape
(`VerifyReport`/`VerifyFinding`) and adding a new CLI verb have real latitude in the report's field
shape, the sweep's severity taxonomy and its exit-code contract. The underlying detection itself is
oracle-bound: every location this change reports is derived from the same authoritative boundary
sources (`Index.db`, the BTI `Partitions.db` trie, `CompressionInfo.db`'s chunk table, `CRC.db`) the
existing `verify_sstable` already reads, and its correctness is checked against the captured
Cassandra `sstableverify` corpus (`test-data/datasets/corruption/test_comp_corrupt`).

## Why

`cqlite verify` (epic #970, issue #1000) already tells an operator a component is corrupt and
classifies why. It does not say **which partitions** are affected — an operator with a
`ChunkDecompressionError` on chunk 12 of a 50 GB `Data.db` has no way to know whether that chunk
holds one partition or ten thousand without hand-computing the intersection themselves. It also has
no way to check every table under a data directory in one pass; `verify` takes one generation
directory at a time. Owner ruling 2026-09-09: pairs with salvage (#4196) as the second slice of the
SSTable tool — salvage's own loss manifest is scoped to salvage's write path, but the same
"which partitions does this damage touch" question belongs on the read-only verify surface too, and
salvage's design explicitly assumes `component_findings` can carry this locating detail
(sstable-salvage design.md §D5, §D6).

CQLite already has every primitive needed: `IndexReader::get_partition_entries` names every BIG
partition's key and `Data.db` offset (`index_reader/mod.rs:264`, `PartitionIndexEntry.data_offset:
u64` at line 64); `CompressionInfo::chunk_for_offset` maps a `Data.db` byte offset to its compressed
chunk index (`compression_info.rs:301`); the uncompressed `CRC.db` chunk grid is a fixed
`CRC_CHUNK_SIZE = 64 * 1024` (`writer/crc_writer.rs:67`); and the BTI reader already performs a full
`Partitions.db` trie walk — `iterate_partitions_in_bti_file` (`bti/parser/traversal.rs:384`) yields
every partition's key and `BtiPartitionLocation` in byte-comparable order, and `verify.rs` itself
already imports it (plus `iterate_rows_in_bti_trie` and the row-offset resolver `n`) for its
existing FULL-mode BTI checks. No new boundary-source primitive is needed for either format.

## What changes

**Library (`cqlite-core`).** `VerifyFinding` gains an optional `location: Option<Location>` field:

```
pub struct Location {
    pub component: String,           // e.g. "Data.db"
    pub byte_offset: u64,
    pub byte_len: u64,
    pub chunk_index: Option<usize>,  // None for a finding with no chunk grid (e.g. IndexEntryCorrupt)
    pub partitions: PartitionResolution,
}
pub enum PartitionResolution {
    Resolved(Vec<KeyRef>),
    Unresolved(String),   // names the cause, e.g. "boundary-source-unreadable"
}
pub struct KeyRef { pub key_hex: String, pub rendered: Option<String> }
```

`partitions` names the keys whose `Data.db` range intersects the damaged range, resolved from
`Index.db` (BIG) or the `Partitions.db` trie (BTI) when that boundary source is itself healthy.
When the boundary source is damaged, `partitions` is `Unresolved(<cause>)` — never a guess, never a
silent empty list standing in for "unknown". Location is populated for chunk/offset-anchored
findings (`ChunkDecompressionError`, `UncompressedChunkCrcMismatch`, a truncation surfaced through
`UnexpectedEof`/`DigestMismatch` with a `Data.db` byte range past EOF); findings with no natural
byte range (e.g. `MissingComponent`, `StatisticsHeaderCorrupt`) carry `location: None`. This is an
**additive** change to the verifier contract: existing fields, existing findings, and the existing
`sstable_parity_corruption_verify.rs` parity assertions are unchanged.

**CLI (`cqlite-cli`).** `cqlite verify --mode full` renders `location` in both text and JSON when
present (the existing report shape extended, never reshaped — the same convention `VerifyReport`
already promises its callers). New verb: `cqlite sweep <data-dir> [--mode quick|full] [--out
json|text] [--jobs N]` walks every `<keyspace>/<table>-<id>/` directory under `<data-dir>`, calls the
existing `verify_sstable` on each, and reports one row per table with severity `ok | degraded |
corrupt | unreadable` and a data-dir-wide exit code (`0` all ok, `2` any corrupt/unreadable, `1`
usage). A table directory `verify_sstable` cannot even open (no readable `Data.db`, unopenable
directory) is a row with severity `unreadable`, never a silently-dropped entry.

## What this change must establish

1. Every located finding's `chunk_index`, `byte_offset` and partition set are **derived from the
   clean/authoritative source, never from CQLite's own behaviour on the corrupt copy** — the test
   independently recomputes the expected intersection from the healthy fixture's `Index.db`
   positions and `CompressionInfo.db`/`CRC.db` chunk table (#3041/#3042 doctrine).
2. **Fail closed on a damaged boundary source**: when `Index.db` or `Partitions.db` is itself the
   corrupt component, every location on every OTHER finding reports `Unresolved("boundary-source-
   unreadable")` rather than a plausible-looking but unverifiable partition list.
3. **The existing parity guard is unaffected**: `sstable_parity_corruption_verify.rs`'s class/verdict
   assertions over the full `test_comp_corrupt` corpus still pass unchanged — location is additive
   detail on top of an unchanged classification.
4. **Sweep never omits a table it cannot read**: a directory holding a `Data.db` with no other usable
   component is a row with severity `unreadable` naming the cause, exit code `2`, not an omission a
   green sweep could hide behind.
5. **Sweep memory stays bounded**: one table's `VerifyReport` (and the FULL-mode row scan behind it)
   resident at a time; `--jobs` bounds worker concurrency, never lets every table in a large data
   directory open simultaneously.
6. **Wiring evidence from the binary** on both formats (BIG/BTI), both compressed and uncompressed
   input, exercised through `cqlite verify` and `cqlite sweep`.

## Non-goals

- Repair, salvage, or rebuild of any kind (#4196, #4197) — this change only LOCATES damage, it never
  writes.
- Cross-component invariant checking beyond what `verify_sstable` already performs (#4195).
- Performance/throughput statistics for the sweep itself (#4204).
- A new boundary-source primitive for BTI — the existing full trie walk is reused as-is (confirmed
  present; see design.md).
- Parallel sweep execution correctness beyond bounding concurrency — `--jobs` bounds resource use;
  it does not change per-table verification semantics.

## Impact statements (openspec rules)

- **No-heuristics (#28):** partition boundaries for location resolution come only from `Index.db` /
  `Partitions.db`; no byte-pattern scanning is introduced anywhere in the new location-resolution
  code (mirrors the `sstable-salvage` R4.3 "no header hunting" guard, adapted as a fail-closed test
  here).
- **Public surfaces:** `cqlite-core`'s `verify` module gains the `Location`/`PartitionResolution`/
  `KeyRef` types (additive, no existing signature changes) plus a new sibling module,
  `verify_location` (declared in `cqlite-core/src/storage/sstable/mod.rs` alongside the existing
  `pub mod verify;`), holding the resolution logic so `verify.rs` itself grows minimally (see
  design.md §D5 — `verify.rs` is already 2761 lines, well over the ~800-line campsite target, so
  ANY net growth engages the file-size ratchet; new logic goes in new files by construction).
  CLI gains one new verb (`sweep`); `verify`'s existing flags are unchanged. Python/Node untouched.
- **<128 MB:** sweep holds one table's `VerifyReport` at a time and bounds concurrent table
  verification via `--jobs`; no data-dir-wide manifest is materialized before rendering.
- **Gate:** new test targets must be named in the gate's `core-tests`/`cli-tests` component lists
  (#3522) exactly as `sstable-salvage` did; both `core-tests` (`cqlite-core/tests/*.rs`, feature
  `cli-helpers`) and `cli-tests` (`cqlite-cli/tests/*.rs` glob, #2039) auto-cover new files, so no
  gate-script edit is needed for a target with no unusual `required-features`. Corruption `.db`
  binaries are gitignored — the corpus tests skip-clean when absent, FAIL when present-but-wrong,
  hard-required under `CQLITE_REQUIRE_FIXTURES=1` (#1094 doctrine).
