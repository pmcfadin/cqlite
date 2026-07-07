# Proposal: Typed recursive inner-UDT decode for frozen-UDT elements inside frozen collections

## Why

Milestone: type-system enhancement (post-M5), issue **#1340**, split out from #1289 (part 2).
Found-by: #1240 / #1289 (nested-frozen-collection UDT compaction parity work).

Routing: **design-driven** (reader value-contract change, no single oracle answer for *which
surface* gets the typed contract — the byte-parity oracle only proves both representations
round-trip). Spec produced for a later team; anchors below are at `main` commit `e2694ab5`.

Today an inner `frozen<UDT>` element inside a frozen collection (e.g. the `person` in
`frozen<list<frozen<person>>>`) decodes to opaque `Value::Blob` on any reader that has no
`UdtRegistry` wired — while a **top-level** `frozen<udt>` column already decodes to typed
`Value::Udt` on the same reader, registry-free. The asymmetry is not a byte-parity bug (it is
validated as correct by the #1240 goldens); it is a **dropped-metadata gap**:

- The on-disk SerializationHeader marshal type for the column —
  `FrozenType(ListType(FrozenType(UserType(ks, person, ...))))` — carries the **full inner UDT
  field layout** and is available at decode time (`RowColumnResolution.header_type`,
  `parsing/mod.rs:211`). Top-level frozen UDTs use it (`decode_frozen_udt_from_header_type`,
  `cell_value.rs:966/1108`).
- But the frozen-collection **element** decoder is handed only the schema short form
  (`"frozen<person>"` → bare `"person"`, `cell_value.rs:857-859` → `frozen.rs:123`), the marshal
  type having been dropped. A bare short name resolves only through an optional `UdtRegistry`
  (`raw_value.rs:445-456`); with none, it falls to `Value::Blob` (`raw_value.rs:457-464`).

Because a `frozen<...>` collection is a single-cell (simple) column, this ONE decoder serves
**both** the normal query path and the compaction read path — the issue title says "compaction
read path", but the fix point and the contract change are shared. Registry-wired surfaces (the
k-way merge producer, `merge/mod.rs:499-501`; the CLI when a schema file with `CREATE TYPE` is
supplied, `cqlite-cli/src/main.rs:493`) already see typed inner UDTs today. The gap is every
registry-less reader: default `SSTableReader::open` (`reader/mod.rs:785`), sweep
(`sweep.rs:245-249`), the Vec-materialising `iterate_all_partitions_for_compaction`, and any
embedder that never built a registry from DDL.

## What Changes

- **Thread the authoritative header marshal element type into frozen-collection element
  decode**, so an inner `frozen<UDT>` element decodes to typed `Value::Udt` (recursively,
  wrapped `Value::Frozen(Value::Udt(..))` like top-level) with **no registry required** —
  the same mechanism top-level frozen UDTs already use. Resolution precedence:
  **header marshal type → `UdtRegistry` (existing behavior, kept) → `Value::Blob`** (honest
  opaque fallback when neither authority resolves; never a byte-pattern guess).
- Applies to frozen list/set elements, frozen map **keys and values**, tuples inside frozen
  collections, and UDTs nested at any depth (UDT-in-UDT fields already decode via
  `parse_nested_udt_from_registry` / marshal recursion).
- **Update the #1240 tripwire** (`issue_1240_nested_frozen_collection_udt_parity.rs:718-732`)
  per its own embedded guidance: inner elements now compare as **typed UDTs** (tier 1b becomes
  self-validating against CQLite's own decode, not only the JSONL golden).
- **Byte-parity is untouched**: the compaction writer already round-trips both `Blob` and
  typed `Udt` representations (the registry-wired merge path emits byte-identical output
  today); the #1240/#1289 byte-parity assertions must pass unchanged.

## Non-goals

- **No compaction writer / merge semantics change** — this is a read-side representation
  change only; Data.db/Index.db/Summary.db/Digest output stays byte-for-byte identical.
- **No public API surface additions** — no new config knobs, no registry API change;
  `set_udt_registry` keeps working and keeps precedence as fallback.
- **No pre-`na` format work** (version floor); **no compressed-write work** (#1406 boundary).
- **No change to non-frozen (multi-cell) collection/UDT decode** — the `ComplexColumn`
  machinery (`compaction_row.rs:261-322`) is out of scope.
- **No new heuristics** — if neither the header marshal type nor the registry resolves the
  inner type, the element stays `Value::Blob`; the decoder never infers structure from bytes
  (no-heuristics mandate #28).

## Impact

- Affected specs: **frozen-inner-udt-decode** (new capability).
- Affected code (anchors at `e2694ab5`):
  - `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/cell_value.rs:846-916`
    (frozen-collection dispatch; where the marshal type must start being threaded)
  - `.../row_decoder/frozen.rs:83-172` (element loop / `read_frozen_element`)
  - `.../row_decoder/raw_value.rs:89-467` (recursive element decoder; Blob fallback at 457-464)
  - `.../row_decoder/udt.rs` (`decode_frozen_udt_from_header_type`, marshal helpers — reuse)
  - `cqlite-core/tests/issue_1240_nested_frozen_collection_udt_parity.rs` (tripwire update)
- Doctrine impact: none beyond this spec; CLAUDE.md/site untouched (no workflow change).
- Consumers seeing a contract change: registry-less `iterate_all_partitions_for_compaction`
  and default-opened readers (Blob → typed Udt for these elements). Query-visible CLI/binding
  output with a schema-supplied registry is expected byte-identical (verify via parity suite).
