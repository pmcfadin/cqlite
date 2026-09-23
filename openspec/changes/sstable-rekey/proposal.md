# sstable-rekey — issue #4203 (epic #4192)

**Milestone:** 0.18 (owner ruling 2026-09-20: the whole epic #4192 ships in one release).
**Priority:** P2. **Routing:** design-driven (OpenSpec + Seam 1) — the premise question (does a
table-id change need a byte rewrite, or is it a pure rename?) has one correct answer once checked
against Cassandra source, but the CLI contract, the `--version` delegation boundary, and the
write-guard reuse are design decisions with latitude.

## Why

After `DROP TABLE` + `CREATE TABLE` (same name), Cassandra assigns a fresh table id; the directory
suffix changes and the operator's existing SSTable files — from a backup, a `salvage` (#4196)
output, or a `split` (#4199) part — can no longer be loaded under the new table because the
directory name no longer matches. Cassandra itself has no offline tool for this: `sstableloader`
needs a live, schema-matching target, not a renamed directory. This is a MOVE-family sibling of
#4202 (BIG↔BTI conversion) in the same epic, scoped narrowly to the identity change alone.

## Premise, answered against `cassandra-5.0.8` source (never CQLite's own code, #3041)

**The table id lives ONLY in the directory name, nowhere inside any SSTable component, for every
format CQLite reads (`na`/`nb`/`oa` BIG, `da` BTI).** Verified two ways against the pinned tag:

1. `Descriptor.java`'s `SSTABLE_DIR_PATTERN` (`io/sstable/Descriptor.java`,
   `cassandra-5.0.8`) names the on-disk layout explicitly:
   `{keyspace}/{tableName}-{tableId}[/backups|/snapshots/{tag}][/.{indexName}]/{component}.db` — the
   table id is a path SEGMENT, never a file-content field.
2. `StatsMetadata`'s (de)serializer (`io/sstable/metadata/StatsMetadata.java`,
   `StatsMetadataSerializer.serialize`/`.deserialize`) — the ONLY SSTable component whose job is
   per-table descriptive metadata — has no table/cf-id field anywhere in its wire format, confirmed
   by reading the full serializer. (CQLite's own `parser/statistics.rs` independently reaches the
   same conclusion for the `nb`-format header — `table_id: None, // nb-format does not include
   table_id in header` — and documents that a table id DOES appear in the pre-`na` legacy Statistics
   header, `parse_legacy_format_header`, which matches historical Cassandra versions that predate
   the directory-based id scheme and which CQLite does not read. This is corroborating evidence, not
   the authority — the Cassandra source citations above are.)
3. Component filenames themselves (`na-1-big-Data.db`, `nb-1-big-Index.db`, …) carry only
   `<version>-<generation>-<format>-<Component>.db` — no keyspace/table/id segment
   (`extract_table_name`'s own doc example in `storage/sstable/mod.rs` confirms this for CQLite's
   reader, consistent with Descriptor.java's pattern putting keyspace/table/id in the PATH, not the
   filename).

**Consequence: a same-version rekey (id change only) is a byte-for-byte copy under a renamed
directory — no decode, no writer, no merge.** `SSTableWriter`/`KWayMerger` are not needed anywhere in
this change's core loop.

## What changes

**Library (`cqlite-core`).** A `rekey` module under `storage/write_engine/` exposing
`rekey_table(table_dir, new_table_id: [u8; 16], out_dir, options) -> Result<RekeyReport>`. For every
generation in `table_dir`, copies every component file byte-for-byte into
`<out_dir>/<table_name>-<new_table_id_hex>/`, filenames unchanged (they never carried the old id),
then self-audits with `verify --mode full` at the new location before declaring success — the same
"prove it before publishing" discipline `salvage`/`rebuild` established, cheap here because nothing
was actually transformed.

**CLI (`cqlite-cli`).** `cqlite rekey <table-dir> --table-id <uuid> --out <dir> [--version nb|da]`.
`--table-id` accepts either the standard dashed 36-char UUID form (Cassandra's own
`system_schema.tables.id` rendering) or the bare 32-hex directory form, normalized the same way
`query/parser.rs`'s existing `parse_uuid_literal` strips hyphens for a CQL `uuid` literal. `--out`/
`--manifest` reuse the shared destructive-path `WriteGuard` promoted out of `commands/salvage/
write_guard.rs` into `commands/write_guard.rs` by #4199 (`sstable-extract-split`) — **this change
depends on #4199 having landed** (or at minimum having merged the relocation commit) rather than
re-deriving the same guard a third time.

**`--version`: fully delegated, not partially implemented here.** Any `--version` value that names a
format DIFFERENT from the source's own on-disk version is a format rewrite — squarely #4202's job
(BIG↔BTI conversion), not this change's. Rather than draw a fuzzy line between "within-family"
and "cross-family" version changes and risk overlapping #4202's engine, this change refuses EVERY
`--version` request that does not match the source's actual version, naming `cqlite convert` (#4202)
as the remedy — mirroring `rebuild`'s `--in-place` gated-on-#4195 pattern. `--version` omitted, or
given the source's own version, proceeds as the pure copy above.

## What this change must establish

1. Rekeyed output loads under the new id in the CQLite reader and is dump-equal to the source's own
   golden (trivial once the premise holds, but asserted rather than assumed) —
   `cqlite-core/tests/issue_4203_rekey_parity.rs`.
2. Cassandra itself accepts the rekeyed directory as a real SSTable for the (recreated) table —
   `test-data/scripts/e2e-cassandra-readback.sh` extended with a rekeyed-import case, run in the
   `ci:bindings-full`-style optional tier, recorded as CI-optional evidence in the parity manifest.
   This is the load-bearing oracle: Cassandra reading the renamed directory IS the proof that no
   internal cross-check against the old id exists that this design's source reading missed.
3. Refuses pre-`na` input (`BigVersionGates::from_version` already rejects `< na`; rekey inherits
   this unchanged, never re-implements the floor check), a malformed `--table-id`, and any
   `--version` naming a real format change (delegates to #4202); never modifies the input (sha256).
4. CLI test in `cli-tests` (#3522).

## Non-goals

- Any actual format/version REWRITE (na/nb/oa ↔ da, or any cross-version byte transformation) —
  entirely #4202's scope. This change never drives `SSTableWriter` or a merger.
- Schema migration (column changes, renames) — table NAME is unchanged; only the id changes.
- Generation renumbering or merging across multiple rekey runs into one output tree.
- CommitLog rekeying (explicitly out of scope per the issue).
- Loading the output into a live cluster beyond the CI-optional readback proof — the operator still
  performs the actual `nodetool refresh`/restart step.

## Impact statements (openspec rules)

- **No-heuristics (#28):** the id change is a pure structural copy; nothing is inferred from bytes.
- **Uncompressed boundary (#1406):** not applicable — rekey copies existing components verbatim,
  including a compressed input's `CompressionInfo.db`; it introduces no new write surface that could
  emit compression, so there is nothing for the #1406 boundary to gate here. Stated in `--help`
  anyway for consistency with every other REPAIR/MOVE verb's documented boundary.
- **Cassandra 5.0 only:** inherited unchanged via `BigVersionGates`/`BtiVersionGates`.
- **Oracles never CQLite alone (#3042):** AC2's Cassandra-side readback is the load-bearing oracle
  precisely because a CQLite-only round-trip (write with new id, read with new id) would be
  invariant to a CQLite-side blind spot the same way #3042 always is — Cassandra accepting the
  renamed directory is the only proof that nothing outside the directory name needed to change.
- **Public surfaces:** `cqlite-core` gains `pub mod rekey` (unconditional; pub-surface guard); CLI
  gains one verb; Python/Node untouched.
- **<128 MB:** each component is copied via a plain streaming file copy, never buffered whole; no
  decode occurs in the core path.
- **Gate:** new test targets named in `core-tests`/`write-tests`/`cli-tests` (#3522).

## Open product decisions for the owner

1. **`--version` scope.** This design refuses ANY version change and points entirely at #4202,
   rather than implementing the narrower "same-family bump" case (e.g. a hypothetical `na`→`nb`
   rewrite that never crosses BIG↔BTI) directly here. CQLite's own writer only ever emits `nb`/`da`
   in practice, so a real `na`-source input is uncommon; flagging in case the owner wants that
   narrower case handled in this slice instead of waiting on #4202.
2. **`--table-id` input form.** Accepting both the dashed 36-char and bare 32-hex forms is my
   recommendation (matches how Cassandra itself prints the id vs. how it appears in a directory
   name); flag if a single required form is preferred instead.
3. **Sequencing.** This change's write-guard reuse is a hard dependency on #4199
   (`sstable-extract-split`) having merged the guard relocation. The epic's suggested activation
   order already sequences #4199 before #4203, so this should be a non-issue if that order holds —
   noting it because I did not re-verify #4199's merge state before drafting this spec.
