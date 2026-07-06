# tombstone-reconciliation Specification

## Purpose
TBD - created by archiving change tombstone-merger-confine. Update Purpose after archive.
## Requirements
### Requirement: The O(entries × tombstones) range-tombstone path is removed

The `TombstoneMerger` module SHALL NOT contain the quadratic range-tombstone path the
July 2026 read-path audit named (§Epic G / G4): `TombstoneMerger::apply_range_tombstones`
and its helper `TombstoneMerger::range_tombstone_applies` SHALL be removed, together with
their unit test. Both have zero production call sites (deadness `rg`-proven and pasted in
the PR). After the removal, every retained public method of `TombstoneMerger` SHALL have
worst-case complexity no greater than O(n log n) in the number of generations merged for a
single key, and the crate SHALL build and lint clean under `RUSTFLAGS="-D warnings"` both
with and without the `tombstones` feature.

#### Scenario: The quadratic methods are gone and nothing references them
- **WHEN** the source tree is searched for `apply_range_tombstones` or `range_tombstone_applies`
- **THEN** neither symbol is defined or referenced anywhere in the tree, and the workspace builds and lints clean under `RUSTFLAGS="-D warnings"` with `--features tombstones` and without it

#### Scenario: Retained public methods carry a documented sub-quadratic bound
- **WHEN** the `TombstoneMerger` module source is inspected after the removal
- **THEN** each retained public method (`merge_generations`, `fast_tombstone_check`, `batch_merge_with_tombstones`) has a documented complexity of at most O(n log n) in the per-key generation count, and no retained method performs an entries × tombstones nested iteration

### Requirement: The retained merger is confined behind `tombstones` with doc scoping

The `TombstoneMerger` module SHALL remain `#[cfg(feature = "tombstones")]`-gated and SHALL
carry a module-level doc block that scopes it as legacy: it SHALL state that the module is
built only under the `tombstones` feature, is OFF the default C1/C4 point-read fast path,
records the complexity of each retained live method, states why the retained cost is
acceptable there (parity-pinned semantics; low-cardinality per-key generation sets; not on
the hot default path), and names the future consolidation direction (folding the `get()`
use into a single-key multi-candidate KWay point path once one exists — reuse, not
rewrite). No default (`not(tombstones)`) build SHALL compile the module.

#### Scenario: The module is not compiled into the default build
- **WHEN** the crate is built with default features (no `tombstones`)
- **THEN** `tombstone_merger.rs` is `#[cfg]`-excluded and no `TombstoneMerger` symbol is present in the default build, and the default build compiles and lints clean under `RUSTFLAGS="-D warnings"`

#### Scenario: The module documents its legacy confinement and complexity
- **WHEN** the `tombstone_merger.rs` module header is read
- **THEN** it states the `tombstones`-feature-only gating, the off-default-fast-path status, the per-method complexity of the retained live surface, the rationale for accepting that cost, and the future KWay-point-path consolidation direction

### Requirement: The `tombstones`-build reconciliation semantics are unchanged (parity-pinned)

Confining the merger SHALL NOT change any `tombstones`-build reconciliation result. The
retained live methods SHALL keep byte-identical behavior: `merge_generations` SHALL apply
last-write-wins with tombstone shadowing and TTL expiry across a per-key generation set;
`fast_tombstone_check` SHALL report an active tombstone as deleted and a live value as
visible; and a full-table `SELECT *` over a clustered table in a `tombstones`-enabled build
SHALL return the same rows as the committed `sstabledump` JSONL golden.

#### Scenario: merge_generations picks the newest live value and honors tombstone shadowing
- **WHEN** `merge_generations` is given a per-key set containing an older live value, a newer live value, and a tombstone, exercised through the production `TombstoneMerger::new()` constructor
- **THEN** it returns the newest live value when no active tombstone shadows it, and returns `None` (deleted) when the newest active tombstone shadows all older writes — identical to the pre-change behavior

#### Scenario: A tombstones-build clustered full scan matches the sstabledump golden
- **WHEN** a `--features tombstones` (or `--all-features`) full-table `SELECT *` runs over the real clustered fixture used by the existing full-scan parity test
- **THEN** the returned row count and row contents equal the committed JSONL golden exactly, unchanged from before this change (the test SKIPs cleanly when the fixture binaries are absent, and FAILs loudly on a zero-row or mismatched result when present)

### Requirement: `TombstoneMerger::new()` constructs without an `unwrap()`

`TombstoneMerger::new()` SHALL NOT use `unwrap()`/`expect()` (CQLite library-code hard
rule). It SHALL derive its current time from the system clock with a graceful fallback: if
`SystemTime::now().duration_since(UNIX_EPOCH)` fails (a clock predating 1970), it SHALL
fall back to a zero epoch time rather than panic, under which no tombstone or TTL is
treated as expired. Constructing via `new()` and merging SHALL succeed on a normal clock.

#### Scenario: Production constructor works and never panics on the clock
- **WHEN** a `TombstoneMerger` is built via the production `TombstoneMerger::new()` (not the test-only `with_time`) and `merge_generations` is invoked on a representative per-key generation set
- **THEN** construction succeeds without panicking and returns the expected reconciliation result, and the module source contains no `unwrap()`/`expect()` in `new()`

### Requirement: The legacy duplicate-work parallel table scan stays retired

The legacy duplicate-work parallel table scan SHALL remain retired (issue #1691): the path
where N workers each scanned the full table and kept 1/Nth of the rows SHALL NOT return. A
parallelizable `TableScan` plan SHALL issue exactly ONE whole-table scan pass, and the
work-counter proof SHALL remain in place.

#### Scenario: A parallelizable TableScan issues exactly one whole-table pass
- **WHEN** a `TableScan` plan whose step requests parallelization (`suggested_threads = 4`) is executed
- **THEN** the storage-layer whole-table scan-initiation counter reports exactly `1` (not one per worker), proving no duplicate-work fan-out

