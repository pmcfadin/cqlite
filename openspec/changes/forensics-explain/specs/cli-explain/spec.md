# cli-explain — new capability (forensics-explain, issue #4193, slice F1)

`cqlite explain` SHALL render the reconciliation trail for one partition across every generation of
a table. All requirements are ADDED.

## ADDED Requirements

### Requirement: R5 — The verb exists, resolves like `query`, and states its clock first

The CLI SHALL expose `cqlite explain <keyspace.table> <partition-key> [--clustering <v>…] [--now <ts>] [--out table|json|csv]`.
It SHALL resolve the table directory and schema through the same `--schema` / `--data-dir` /
`--dataset` globals `query` uses, and SHALL print as its first line
`now=<epoch-secs> (<RFC3339>)  generations=<n> [<basename>…]  gc_grace_seconds=<g>` in every output format.

#### Scenario: R5.1 first line and generation list
- **Given** the built `cqlite` binary and `--dataset test_tomb`, table `resurrection_gc_positive`
- **When** `cqlite explain test_tomb.resurrection_gc_positive <pk> --now 1757400000` runs
- **Then** stdout's first line matches the format above with `generations=2` and both `nb-*-big`
  basenames, exit code `0` (`cqlite-cli/tests/explain_cli_tests.rs`, named in the gate's `cli-tests`
  target list).

#### Scenario: R5.2 `--now` absent is wall clock and says so
- **When** the same command runs without `--now`
- **Then** the first line carries the wall-clock `now` and the suffix `(wall clock)`; tests never
  rely on its value.

#### Scenario: R5.3 the naming collision is documented in the binary
- **When** `cqlite explain --help` runs
- **Then** the text contains `for a query plan use \`query --explain\``, and `cqlite query --explain`
  still prints a plan (existing `query` tests unchanged).

### Requirement: R6 — The trail agrees with the answer, on both read paths, in both directions

The `winner` set SHALL be the read result. For every fixture in design.md §D6 and every table in `test-data/query-semantics-oracle.json`, the
set of `winner` cells rendered by `explain` for a partition SHALL equal the row the existing read
path returns for `SELECT *` on that partition at the same pinned `now`.

#### Scenario: R6.1 winners == SELECT *, point and full
- **Given** `cqlite-core/tests/issue_4193_explain_vs_select.rs`
- **When** it runs `explain`'s library entry point and a `SELECT *` under `CQLITE_READ_PATH=point`
  and again under `=full`, at the same pinned `now`
- **Then** for each column: a `winner` exists iff the `SELECT` row carries that column with that
  value, and the failure message names the column and direction (`winner-without-column` /
  `column-without-winner`), per #3890.

### Requirement: R7 — Output formats are stable and machine-readable

`explain` SHALL render `table`, `json` and `csv` with the fixed key set below; `json` and `csv`
MUST be parseable without reference to the human `table` form.

#### Scenario: R7.1 json shape
- **When** `--out json` is used
- **Then** stdout after the first line is one JSON object with exactly the keys `now`,
  `generations` (array of `{run_index, sstable, probe}` with `probe ∈ {hit, absent, scanned}`),
  `cells` (array of `CellDecision` with keys `column, clustering, generation, writetime, ttl,
  expires_at, value, verdict, decided_by`), `tombstones` (array with `kind, generation,
  deletion_time, local_deletion_time, range_start, range_end, droppable_at_now`); asserted by
  `explain_cli_tests.rs` against a committed expected file per fixture.

#### Scenario: R7.2 csv and table
- **When** `--out csv` is used, **Then** the `cells` rows are emitted with the same keys as header,
  tombstones appended with `kind` prefixed `tombstone:`; **When** `--out table` (default), **Then**
  each cell renders on one line as `<column>  <sstable>  writetime=<ts>  <verdict>  <decided_by>`.

#### Scenario: R7.3 `--clustering` filters the rendering only
- **Given** `test_da/multiclustering_table` (committed, BTI)
- **When** `explain … --clustering <ck>` runs
- **Then** only cells of that clustering row render, while `tombstones` still lists every range and
  row tombstone of the partition, and the `winner` set for that row equals R6's for that row.

### Requirement: R8 — Fail closed, loudly

`explain` SHALL exit non-zero naming the generation when any generation cannot be read, SHALL never
emit a shorter trail in place of an error, and MUST NOT write to any file.

#### Scenario: R8.1 unreadable generation
- **Given** a temp table dir with one generation's `Statistics.db` truncated
- **When** `explain` runs
- **Then** exit code is `2`, stderr names the generation's path, and stdout carries no cell lines.

#### Scenario: R8.2 key held by no generation
- **Given** a key absent from every generation
- **When** `explain` runs
- **Then** exit code is `0`, the first line says `generations=<n>`, the body says
  `0 generations hold this key`, and each generation is listed with `probe=absent` (or `scanned`
  for a fail-safe scan that found nothing).

#### Scenario: R8.3 read-only
- **Given** a temp copy of a fixture dir and its recursive sha256 listing
- **When** `explain` runs against it (all formats)
- **Then** the listing is unchanged.

### Requirement: R9 — Byte budget applies

`explain` output SHALL be governed by the same `max_result_bytes` budget as the query surface.

#### Scenario: R9.1 wide partition under `max_result_bytes`
- **Given** `test_wide_rows` and a `max_result_bytes` set below the partition's rendered size
- **When** `explain --out json` runs
- **Then** it terminates with the same budget-exceeded error the query surface emits, never OOM,
  and never a truncated-but-`0` exit.
