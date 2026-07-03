# Spec: storage-freshness (core + bindings + docs)

## ADDED Requirements

### Requirement: Explicit refresh applies directory changes to the held reader set

`Database::refresh()` SHALL re-discover the data directory using the same
TOC/filename-component-based discovery as `open` (no content sniffing), and SHALL apply
the diff to the held reader set: newly present generations become queryable, removed
generations stop being queried, and unchanged generations keep their existing parsed
reader state (Index/Statistics/bloom are not re-parsed). It SHALL return a
`RefreshReport` with `tables_scanned`, `readers_added`, `readers_removed`.

#### Scenario: New generation invisible until refresh, visible after

- **GIVEN** an open `Database` on a directory with one SSTable generation of a table
  (real corpus binaries, non-empty)
- **WHEN** a second generation containing an additional partition is copied into the
  table directory **and** the same SELECT is re-run *without* refresh
- **THEN** the result equals the pre-copy result (stale-until-refresh contract)
- **WHEN** `refresh()` is called and the SELECT is re-run
- **THEN** the result includes the new generation's partition
- **AND** the `RefreshReport` records `readers_added == 1` and `readers_removed == 0`.

#### Scenario: Removed generation dropped safely

- **GIVEN** an open `Database` on a table with two generations
- **WHEN** one generation's component files are deleted (simulating compaction) and
  `refresh()` is called
- **THEN** `refresh()` succeeds with `readers_removed == 1`
- **AND** a subsequent SELECT returns rows from the remaining generation only, with no
  panic and no `unwrap()`/`expect()` in the library path.

#### Scenario: Unchanged directory is a cheap no-op

- **GIVEN** an open `Database` whose directory has not changed
- **WHEN** `refresh()` is called
- **THEN** it returns `readers_added == 0 && readers_removed == 0`
- **AND** the reader instances for unchanged files are the same objects as before the
  call (pointer identity via `Arc::ptr_eq` in a core-level test — warm state preserved).

### Requirement: Refresh failure is atomic and fail-closed

`refresh()` SHALL be atomic: if any newly discovered generation fails to open
(including a corrupt `Statistics.db`, which fails per the #1626 posture), `refresh()`
SHALL return a typed error and SHALL leave the previously held reader set fully
unchanged — no partial application.

#### Scenario: Corrupt new generation rejects the whole refresh

- **GIVEN** an open `Database` with one valid generation
- **WHEN** a new generation with a truncated/corrupt `Statistics.db` is copied in and
  `refresh()` is called
- **THEN** `refresh()` returns a typed error (no panic)
- **AND** a subsequent SELECT returns exactly the pre-refresh result set (old readers
  still live, new generation not partially visible).

### Requirement: Queries execute against a single consistent reader snapshot

Every query SHALL resolve its reader set exactly once; a concurrent `refresh()` SHALL
NOT affect a query already in flight. Queries started after a completed refresh SHALL
see the post-refresh set.

#### Scenario: In-flight scan unaffected by concurrent refresh

- **GIVEN** a long-running/streaming full scan in progress on the pre-refresh set
- **WHEN** `refresh()` adds and removes generations while the scan is draining
- **THEN** the scan completes without error and its result is exactly the correct
  result for the pre-refresh reader set (assertions on result content, not timing)
- **AND** a query issued after the refresh returns the post-refresh result.

### Requirement: Refresh is exposed through the public binding surfaces (wiring evidence)

`refresh()` SHALL be callable from Python (`db.refresh()`) and Node
(`await db.refresh()`), each returning the report fields, each covered by an
end-to-end test that drives the full stale→refresh→fresh cycle through the binding's
own public API against real SSTable binaries.

#### Scenario: Python end-to-end refresh

- **GIVEN** `cqlite.open(...)` on a temp directory with one generation
- **WHEN** a second generation is copied in, `db.refresh()` is called, and the query is
  re-run via `db.execute(...)`
- **THEN** the new partition appears and the returned report has `readers_added == 1`
- **AND** the test fails (does not skip) if the dataset binaries are absent-but-expected
  (no silent 0-row pass).

#### Scenario: Node end-to-end refresh

- **GIVEN** `Database.open(...)` on a temp directory with one generation
- **WHEN** a second generation is copied in, `await db.refresh()` resolves, and the
  query is re-run
- **THEN** the new partition appears and the report has `readersAdded == 1`.

### Requirement: The per-surface freshness contract is documented

User documentation SHALL state, for each read surface (library handle, CLI one-shot,
Arrow Flight), its freshness semantics (snapshot-at-open + explicit refresh / fresh per
process / fresh per request), and the mid-query file-removal (torn-window) behavior. The
Flight torn-window posture SHALL be recorded as a decision consumed by the Flight
rewrite (#1477). The limitations page SHALL cross-link the contract.

#### Scenario: Contract page ships in the same change

- **WHEN** the change is complete
- **THEN** a docs page exists describing all three surfaces' freshness + torn-window
  semantics, including that auto-refresh/watching is explicitly a non-goal follow-up
- **AND** the limitations page links to it
- **AND** the #1477 issue carries (or links) the recorded Flight posture decision.
