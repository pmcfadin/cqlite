# flight-bti-coverage

## ADDED Requirements

### Requirement: BTI (da) SSTables are exercised end-to-end through Flight do_get

The `cqlite-flight` integration suite SHALL include tests that drive a real `do_get` request over the
Flight transport against BTI (`da`) SSTables from the `test_da` corpus, covering point lookup, full
scan, and LIMIT-k. Prior to this change no `da` SSTable reached `do_get`; every Flight integration
fixture hardcoded a BIG (`nb-*-big`) SSTable.

#### Scenario: Full scan over a BTI table through do_get matches the sstabledump golden

- **GIVEN** the committed `test_da` BTI corpus is present (real `da-2-bti-*` component sets) and
  `CQLITE_DATASETS_ROOT` points at it
- **WHEN** a `do_get` full-scan request is driven over the Flight transport against a `test_da` BTI
  table (e.g. `simple_table`)
- **THEN** the returned rows equal the rows in that table's committed `da-2-bti-Data.db.jsonl`
  sstabledump golden (same rows, values, and order after reconciliation)
- **AND** the row count is greater than zero (a present-but-empty result is a failure, not a skip).

#### Scenario: Point lookup over a BTI table through do_get returns the addressed partition

- **GIVEN** the committed `test_da` BTI corpus is present
- **WHEN** a `do_get` point-read request for a known partition key is driven over the transport against
  a `test_da` BTI table
- **THEN** the returned row(s) equal the corresponding row(s) in the `da-2-bti-Data.db.jsonl` golden for
  that key.

#### Scenario: LIMIT-k over a BTI table through do_get bounds the result set

- **GIVEN** the committed `test_da` BTI corpus is present
- **WHEN** a `do_get` request with a LIMIT of k rows is driven over the transport against a `test_da`
  BTI table whose golden has more than k rows
- **THEN** at most k rows are returned, and each returned row matches its counterpart in the golden.

### Requirement: BTI do_get returns correct rows despite absent Summary-based token pruning

A BTI `do_get` SHALL return the full correct result set regardless of token-pruning absence, and these
tests SHALL catch a regression that turns the current fail-open into fail-closed (dropping BTI rows).
Background: Flight boundary-key resolution derives a `-Summary.db` sibling and opens it via
`SummaryReader::open`; Summary.db is BIG-only, so for a BTI SSTable this yields no boundary and the
token-prune step fail-opens (the table is never pruned). Result correctness MUST NOT depend on that
pruning.

#### Scenario: Fail-open pruning does not drop BTI rows

- **GIVEN** a `test_da` BTI table with no `-Summary.db` sibling
- **WHEN** a `do_get` scan is driven over the transport
- **THEN** every row present in the `da-2-bti-Data.db.jsonl` golden is returned (the missing Summary
  causes pruning to be skipped, never to drop rows)
- **AND** the test does not assert that any partition was token-pruned (BTI pruning is out of scope).

### Requirement: The BTI corpus is present in a stock CI checkout

The `test_da` BTI SSTable binaries required by these tests SHALL be available in a stock repository
checkout (committed via `git add -f`, the pattern used for the existing tracked `.db` fixtures), so the
tests execute in CI rather than skipping. The parity/byte assertions SHALL be verifiable against the
committed tree (a detached checkout of `HEAD`), not only a dirty working tree.

#### Scenario: The tests run (not skip) on a clean checkout

- **GIVEN** a fresh checkout of the branch with no local dataset fetch performed
- **WHEN** the `cqlite-flight` BTI `do_get` tests run with `CQLITE_DATASETS_ROOT` pointing at the
  repository's `test-data/datasets`
- **THEN** the committed `test_da` `da-2-bti-*` binaries are found and the tests execute their
  assertions (they do not take the skip-on-presence branch).

#### Scenario: Committed-tree verification catches a missing force-add

- **GIVEN** the change adds BTI binaries intended to be tracked
- **WHEN** the suite is run from a detached `git worktree add --detach HEAD` of the committed tree
- **THEN** all required `da-2-bti-*` components are present and the BTI tests pass (a binary omitted
  from the commit fails here rather than passing against the dirty tree).

### Requirement: Flight integration fixtures resolve corpus paths through one shared helper

The Flight integration tests SHALL resolve SSTable fixture directories through a single shared
helper rather than each test hardcoding its own `CQLITE_DATASETS_ROOT`-relative join. The existing BIG
tests and the new BTI tests SHALL use the same helper.

#### Scenario: BIG and BTI fixtures resolve through the same helper

- **WHEN** a Flight integration test needs a corpus fixture directory (BIG or BTI)
- **THEN** it obtains the path from the shared fixture-path helper
- **AND** the three pre-existing BIG tests are updated to use that helper (no remaining per-test
  hardcoded `nb-1-big` join for fixture resolution).
