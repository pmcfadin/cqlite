# Design — corruption-locator (issue #4194)

## D1. Locating a chunk/offset finding: intersect the boundary source with the damaged range

```
existing verify_sstable() finding (e.g. ChunkDecompressionError, chunk 12)
      │
      ▼
resolve_location(finding, boundary_source) :
   boundary source healthy? ──no──► Location { .., partitions: Unresolved("boundary-source-unreadable") }
      │ yes
      ▼
   BIG:  IndexReader::get_partition_entries() -> [(key, data_offset, data_size)]
         CompressionInfo::chunk_for_offset(offset) -> chunk index   (compressed)
         CRC_CHUNK_SIZE grid: offset / 65536 -> chunk index          (uncompressed, CRC.db)
   BTI:  iterate_partitions_in_bti_file() -> [(key, BtiPartitionLocation)] (already used by verify.rs)
      │
      ▼
   partitions whose [data_offset, data_offset + data_size) intersects the finding's damaged
   [byte_offset, byte_offset + byte_len) range
      │
      ▼
   Location { component, byte_offset, byte_len, chunk_index, partitions: Resolved(keys) }
```

- **No new boundary-source primitive.** BIG already exposes `IndexReader::get_partition_entries`
  (`index_reader/mod.rs:264`) with `PartitionIndexEntry.data_offset: u64` (line 64) and a
  `data_size` companion (used at line 324 to build `(data_offset, data_size)` pairs already).
  BTI's `iterate_partitions_in_bti_file` (`bti/parser/traversal.rs:384`) is already imported into
  `verify.rs` for the existing FULL-mode row-scan check, alongside `iterate_rows_in_bti_trie` and the
  row-offset resolver `n` (`bti/parser/rows.rs`) — confirmed present by direct read of both files, so
  premise 0.3 from the sibling `sstable-salvage` change (#4196) holds here too: **the BTI full trie
  walk exists; this change reuses it exactly, no new primitive.**
- **Chunk-to-byte-range translation** differs by input: compressed BIG uses
  `CompressionInfo::chunk_for_offset` (`compression_info.rs:301`, `offset / self.chunk_length`) with
  `chunk_length: u32` (the per-chunk uncompressed size, field at line 70) giving the chunk's
  `Data.db` byte range directly (`chunk_index * chunk_length .. (chunk_index+1) * chunk_length`,
  clamped to the file's logical length); uncompressed BIG uses the fixed `CRC_CHUNK_SIZE = 64 * 1024`
  grid (`writer/crc_writer.rs:67`) the same way. A truncation finding's damaged range is
  `[new_eof, original_logical_length)` — recovered from the difference between the boundary source's
  last partition's expected extent and the truncated file's actual size, not from a second Cassandra
  fixture.
- **Partition intersection is a closed interval test**, no scanning: for each boundary entry, its
  extent is `[data_offset, data_offset + data_size)` (BIG) or `[BtiPartitionLocation.offset,
  next_entry.offset)` (BTI, entries are in byte-comparable/file order so the next entry's offset — or
  EOF for the last — bounds the extent); intersect against the finding's damaged range. This is the
  same style of derivation `sstable-salvage`'s D6 oracle table uses for its own expected-loss-set
  computation, applied here to a READ-ONLY report field instead of a recovery decision.
- **`KeyRef.rendered`** is populated only when a schema is available to decode the raw key (parity
  with `sstable-salvage`'s manifest `key`/`key_hex` split, design.md §D5) — `verify`/`sweep` take no
  mandatory `--schema` today, so `rendered` is commonly `None`; `key_hex` is always populated from
  the boundary source's raw key bytes.

## D2. Fail-closed: a damaged boundary source poisons ALL locations, not just its own finding

If `Index.db` (BIG) or `Partitions.db` (BTI) is itself corrupt, `verify_sstable` already reports that
as its own finding (`IndexEntryCorrupt` / `BtiRootPointerCorrupt`). This change adds: every OTHER
finding in the same report that would otherwise carry a location instead reports
`Unresolved("boundary-source-unreadable")`. Rationale: a corrupt boundary source cannot be trusted to
name ANY partition correctly, including ones nowhere near the byte range that corrupted it — the
resolver either fully trusts the boundary source for a report or it trusts none of it, mirroring
`sstable-salvage`'s D3 refusal contract (`boundary-source-unreadable` is the same named cause,
deliberately — an operator who has read one SSTable-tool report already knows this vocabulary).

## D3. Sweep: directory walk over an unmodified verify

```
sweep(data_dir, mode, jobs) :
   for each <ks>/<table>-<id>/ under data_dir (existence check only; no Data.db content touched
   until that table's own verify_sstable call):
       bounded by a semaphore of size `jobs` (default: available_parallelism)
       ── Data.db unreadable / dir unopenable ──► SweepRow { severity: unreadable, cause }
       ── verify_sstable(dir, mode) fails to even resolve components ──► unreadable
       ── verify_sstable returns Ok(report) ──►
             report.is_ok()                         -> ok
             report.is_ok() == false, no `corrupt`-class finding -> degraded   (e.g. FilterFalseNegative only)
             report.is_ok() == false, any structural/content finding -> corrupt
   SweepReport { rows: Vec<SweepRow>, totals }
   exit 0 iff every row is `ok`; exit 2 iff any row is `corrupt` or `unreadable`
```

- **One table resident at a time in memory**, per row: sweep calls the SAME `verify_sstable` a
  single-directory `cqlite verify` would, so its memory profile per table is identical to today's
  `verify --mode full` (already bounded — no new unbounded structure is introduced). `--jobs` bounds
  how many of those single-table calls run concurrently; it never changes what any one call holds.
- **`degraded` vs `corrupt`**: `FilterFalseNegative` is the one `VerifyErrorClass` that is a CQLite-
  only detection with no Cassandra-verified corruption (issue #1398; `filter_db_bit_flip`'s manifest
  entry records `cassandra_verdict: clean`, `verdict_parity: divergent`) — a report whose ONLY
  finding is `FilterFalseNegative` is `degraded` (worth an operator's attention, not proof the SSTable
  is unreadable). Every other non-empty finding set is `corrupt`. This mapping is a closed function
  of `VerifyErrorClass`, stated once, never re-derived per call site.
  covered by `test_comp_corrupt/filter_db_bit_flip`, the only fixture where `degraded` is reachable.
- **A table dir that cannot even be opened is a row, never an omission**: the walk enumerates
  directories structurally (readdir), so a directory that exists but has no readable `Data.db`, or
  that `resolve_components` itself fails on, still produces exactly one `SweepRow` with severity
  `unreadable` and a named cause — never a skip that shrinks the row count silently.

## D4. Report/CLI shape — additive, campsite-respecting

`VerifyReport`/`VerifyFinding`'s existing fields, `Display` impls and JSON keys are UNCHANGED,
matching the proposal's "existing report shape extended, never reshaped" — a caller reading only the
pre-existing fields sees no behavior change. New surface:

- `cqlite-core/src/storage/sstable/verify_location.rs` (new file, declared `mod verify_location;` in
  `sstable/mod.rs` next to the existing `pub mod verify;`): `Location`, `PartitionResolution`,
  `KeyRef`, and the BIG/BTI resolution functions from D1. `verify.rs` itself is touched only to (a)
  add `pub location: Option<Location>` to `VerifyFinding`, (b) call
  `verify_location::resolve(&finding, &components)` at each chunk/offset-anchored finding site, and
  (c) extend `VerifyReport::summary_line`/the finding `Display` impl to append the location when
  present. **`verify.rs` is already 2761 lines** (the ~800-line campsite target is a fraction of
  that), so this change is designed to add as few lines to `verify.rs` as the field + call-site
  wiring requires; if the net diff still grows the file, the PR runs with
  `CQLITE_ALLOW_FILE_GROWTH=1` and a note linking epic #1116, per CLAUDE.md's documented escape hatch
  — never a silent ratchet bypass.
- `cqlite-cli/src/commands/verify.rs` (112 lines, well under threshold): extend `print_text`/
  `print_json` to render `finding.location` when `Some` — a strictly additive rendering change, the
  same file, no new module needed here.
- `cqlite-cli/src/commands/sweep.rs` (new file, wired like `read_commitlog.rs` /
  `verify.rs`): the directory walk, severity mapping (D3) and text/JSON rendering of `SweepReport`.
  `Commands::Sweep` added to `cli_types.rs` (one new variant, `SweepModeArg`/`SweepOutputArg` reuse
  `VerifyModeArg`/`VerifyOutputArg` — no new arg enums needed since the value sets are identical).

## D5. Oracles (all Cassandra-written; expectations derived from the format, never from CQLite)

| Case | Fixture | Expected location derived by |
|---|---|---|
| compressed chunk CRC flip | `test_comp_corrupt/data_db_bit_flip` (captured Cassandra verdict `corrupt`) | test independently computes, from the CLEAN source's `Index.db` positions and `CompressionInfo.db` chunk table, the partitions whose byte range intersects the flipped chunk; asserted equal to `finding.location.partitions` |
| uncompressed chunk CRC flip | `test_comp_corrupt/uncompressed_data_bit_flip` (`CRC.db`) | same, using `CRC.db`'s fixed 64 KiB chunk grid and the clean source's `Index.db` positions |
| Data.db truncation | `test_comp_corrupt/data_db_truncation` | partitions whose range extends past the corrupted file's actual size, computed from the CLEAN source's `Index.db` positions |
| decodable-but-corrupt row (needle partition) | `corrupt_byte_fixture::stage_control_and_mutated(BIG_COMPOSITE)` / `BTI_MULTICLUSTERING` | the needle partition (the one holding the flipped clustering-key byte) is the sole resolved partition; verified via `index_partition_positions`/the BTI trie walk over `control` |
| boundary source damaged (BIG) | `test_comp_corrupt/index_db_bit_flip_big` | every OTHER finding's location is `Unresolved("boundary-source-unreadable")` |
| boundary source damaged (BTI) | `test_comp_corrupt/bti_partitions_footer_flip`, `bti_rows_truncation` | same, BTI side |
| sweep over the committed corpus | every table under a fetched `CQLITE_DATASETS_ROOT` | every row `ok`, exit 0 |
| sweep with one corrupted copy injected | a temp dir: one healthy table copy + `data_db_bit_flip`'s corrupted copy | the corrupted table's row is `corrupt`, every other row `ok`, exit 2 |
| sweep over an unreadable table dir | a temp dir with a bare `Data.db` and nothing else (no `TOC.txt`, no `Statistics.db`) | that row is `unreadable` naming the cause, exit 2 |
| parity regression guard | the full `test_comp_corrupt` corpus via the EXISTING `sstable_parity_corruption_verify.rs` | unchanged: every class/verdict assertion in that file still passes with `location` present but unread by it |

## D6. No header hunting (mirrors sstable-salvage R4.3)

Partition resolution reads ONLY the boundary source's own structured entries (`Index.db`'s decoded
`PartitionIndexEntry`s, the BTI trie's decoded leaves) and the `CompressionInfo.db`/`CRC.db` chunk
tables — never a byte-pattern search over `Data.db` looking for a plausible partition header. A
`scripts/tests/test_verify_location_no_resync_scan.sh` (registered in `tooling-tests`, same pattern
as `sstable-salvage`'s `test_salvage_no_resync_scan.sh`) greps
`cqlite-core/src/storage/sstable/verify_location.rs` for `memchr`, `windows(`, `find(|b|`,
`position(|b|` outside `#[cfg(test)]` and FAILs naming the line if any is found.
