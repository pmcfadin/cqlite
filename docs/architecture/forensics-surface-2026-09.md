# The forensics surface — "sstabledump on steroids"

Status: **idea document, 2026-09-09.** Not groomed, not scheduled, no issue yet. Origin: a
Cassandra meetup comment (September 2026) — *"the ability to inspect data on a lower level,
like tombstone visibility and different versions of data from different SSTables, would let
CQLite be a troubleshooting tool as well — a kind of sstabledump on steroids."* This document
records the idea, what CQLite already has that makes it cheap, the questions it should
answer, a proposed shape, and the slicing — so it can be picked up in its own thread.

## 1. The thesis

CQLite's reconciliation engine already knows *why* every cell wins or loses: it has to, to
match Apache Cassandra's compaction byte for byte (v0.12, the differential harness in CI, the
full rule set in `cqlite-core/src/storage/write_engine/reconcile_rules.rs` — complex deletions,
tombstone tie-breaks, `gc_grace` purging, range tombstones, per-cell and dropped-column
purging, non-frozen UDT multi-cell). Today that knowledge is discarded at the egress: a
`SELECT` returns the reconciled answer and nothing else.

The forensics surface exposes the decision trail instead of discarding it. It is not a new
engine; it is a second egress from the engine we have. That is what makes it cheap, and it
is also why it belongs in the storage-engine PRD's frame ("shared reconciliation and
materialization"): an operator-facing surface on the shared reconciliation core, not another
dump tool.

**Against `sstabledump`**, the comparison is exact:

| | `sstabledump` | CQLite forensics |
|---|---|---|
| Scope | one SSTable at a time | every generation of a table (and the CommitLog) |
| Output | raw JSON, one dialect | schema-typed rows, filterable with SQL, any egress (table/JSON/CSV/Parquet/Flight) |
| Reconciliation | none — the reader does it in their head | shown, with Cassandra-parity rules and the tombstone that decided each cell |
| Startup | JVM, seconds | native binary, milliseconds |
| Cluster | one node | any node's snapshot, and replica-to-replica diff |
| Rules | tool of record for physical parity (see below) | must never disagree with it on the physical layer |

`sstabledump` stays the **oracle** for what is physically on disk — CQLite's own physical-dump
parity lane is measured against it. The forensics surface adds the layer above it.

## 2. What already exists (the reason this is plumbing, not research)

| Capability | Where | What it gives the forensics surface |
|---|---|---|
| Cassandra-parity reconciliation | `storage/write_engine/reconcile_rules.rs`, `merge/streaming.rs`, `merge/point_read.rs` | the decision for every cell: winner / shadowed-by-timestamp / shadowed-by-tombstone / expired / purgeable |
| Per-generation cell envelope | delta-export (`export/delta_schema.rs`: `value`, `writetime`, `expires_at`, `replaced`) | the multi-version view already exists as a Parquet export; it needs a CLI/SQL egress |
| Pinned-`now` reconciliation | query-semantics oracle (`test-data/query-semantics-oracle.json`), TTL reconciliation through `do_get` | "as of" evaluation of TTL and `gc_grace` is a parameter, not a rewrite |
| Correctness counters | `merge.rows_in/out`, `tombstones_suppressed/emitted`, `sstables_pruned{format}` (#2163) | tombstone density is already counted; it needs to be *attributed* per partition |
| Statistics.db parsing | `parser/enhanced_statistics_parser/` | min/max timestamps, estimated droppable tombstones, dropped columns, per-generation |
| CommitLog reader | `storage/commitlog/` (#2389, v0.16.1) | "where did my write go" can look past the flushed set |
| Point-vs-full differential | `CQLITE_READ_PATH=point\|full` (#1918) | two independent read paths to cross-check an explanation against |
| Existing CLI verbs | `inspect` (validate/analyze), `verify`, `info`, `read-sstable`, `read-commitlog`, `delta-export`, `compact` | homes for the new verbs; `inspect` already owns component-level checks |

Everything above is **authoritative metadata**: schema, `Statistics.db`, the bytes Cassandra
wrote. Nothing here needs inference from byte patterns, so the whole surface sits inside the
no-heuristics mandate (#28) by construction.

## 3. The questions it answers

Ordered by how often an operator actually asks them.

1. **"Why does this row have this value?"** Which generation the winning cell came from, its
   writetime, and every older version it shadowed.
2. **"Why is my row gone — or why didn't my delete work?"** Which tombstone shadows it
   (partition / range / row / cell / collection), its timestamp against the data's, whether it
   has passed `gc_grace`, and whether a resurrection is possible if a replica missed it.
3. **"Why is this partition slow?"** Tombstone density per partition across all generations,
   live vs droppable-at-`now`, and which generations carry them.
4. **"Where did my write go?"** Which generations hold the key, and whether it is in a CommitLog
   segment but not yet flushed.
5. **"What will compaction do to this partition?"** A dry run of the parity merge at a chosen
   `now`: purged, kept, and the rule that decided each.
6. **"How do two replicas differ?"** One partition diffed across two nodes' snapshots, without
   running repair.
7. **"What did this row look like at 15:00?"** As-of reads by writetime cutoff.
8. **"Is this file healthy?"** Component-level integrity — already largely `inspect`/`verify`.

## 4. Proposed shape

### 4.1 `cqlite explain <table> <partition-key> [--clustering …] [--now <ts>]`

The headline verb. A reconciliation trace for one row (or partition): every cell version from
every generation, one line each, with its **verdict** and the **deciding fact**.

```
partition key = 'sensor-42'  (3 generations: nb-12, nb-9, nb-7; commitlog: none)
row (ts=2026-09-08T14:00:00Z)
  value      nb-12  writetime=1757340000123456  WINNER
  value      nb-9   writetime=1757339000000000  shadowed-by-timestamp (nb-12)
  status     nb-9   writetime=1757339000000000  shadowed-by-tombstone  cell-tombstone nb-12 deletion=1757340000200000
  status     nb-7   writetime=1757300000000000  shadowed-by-tombstone  (same)
  note       nb-7   writetime=1757300000000000  expired  ttl=86400 expires_at=1757386400 (now=1757400000)
row deletion: none.  range tombstones covering this row: none.
tombstones in partition: 1 (cell), droppable at now: 0 (gc_grace=864000, deletion age 6d)
```

Verdict vocabulary is closed and maps 1:1 onto the reconcile rules: `winner`,
`shadowed-by-timestamp`, `shadowed-by-tombstone`, `expired`, `purgeable`, `dropped-column`.
Every verdict names the generation and, for tombstones, the tombstone's kind, deletion time,
and `gc_grace` status. Output in `table` (above), `json`, or `csv`.

### 4.2 `raw.*` tables — the SQLite spin

Expose the **unreconciled layer as queryable tables**, so the questions in §3 become SQL:

| Table | One row per | Columns (beyond the table's own key columns) |
|---|---|---|
| `raw.cells` | cell version | `generation`, `sstable`, `column`, `value`, `writetime`, `ttl`, `expires_at`, `verdict`, `decided_by` |
| `raw.tombstones` | tombstone | `generation`, `kind` (partition/range/row/cell/collection), `deletion_time`, `local_deletion_time`, `range_start`/`range_end`, `droppable_at_now` |
| `raw.generations` | SSTable | `generation`, `format`, `min_ts`, `max_ts`, `rows`, `partitions`, `estimated_droppable_tombstones`, `compression`, `size_bytes` |
| `raw.commitlog` | unflushed mutation | `segment`, `position`, `partition_key`, `mutation_kind`, `writetime` |

```sql
-- tombstone density, the afternoon-long hunt as one query
SELECT partition_key, count(*) AS tombstones,
       sum(CASE WHEN droppable_at_now THEN 1 ELSE 0 END) AS droppable
FROM raw.tombstones GROUP BY 1 ORDER BY 2 DESC LIMIT 20;

-- every version of one cell, newest first
SELECT generation, writetime, value, verdict, decided_by
FROM raw.cells WHERE sensor_id = 'sensor-42' AND column = 'status' ORDER BY writetime DESC;
```

Through the Trino connector the same tables appear as a `cqlite_raw` schema, so a whole
cluster's tombstone density is one `GROUP BY` across every node's Flight pod. `explain` (§4.1)
is then a formatted view over `raw.cells` + `raw.tombstones` for one key — one mechanism, two
egresses.

### 4.3 The rest, in order of value

- `cqlite tombstones <table> [--now <ts>] [--top N]` — the §3.3 report as a verb: per-partition
  density, droppable-at-`now`, per-generation attribution. A formatted `raw.tombstones` query.
- `cqlite compact --dry-run --explain [--now <ts>]` — the parity engine, no bytes written,
  emitting the verdict vocabulary of §4.1 per cell. Doubles as the compaction planner's
  ground truth (PRD SE4 "pure explain/simulate interfaces").
- `cqlite find <table> <key>` — which generations and which CommitLog segments hold the key.
  Uses the bloom filter / index / BTI path per generation, so it also shows *how* each
  generation was probed (`bloom-negative`, `index-miss`, `hit`).
- `cqlite diff <snapshot-a> <snapshot-b> --table <t> [--partition <key>]` — replica divergence
  as a reconciled diff: cells present in one, missing or older in the other.
- `--as-of <ts>` on `SELECT`/`explain` — writetime cutoff on the merge; tombstones newer than
  `ts` are ignored too. Time travel within the retained generations.

## 5. Design constraints (things that would make this wrong)

- **Format authority is never CQLite.** Every verdict must be derivable from `cassandra-5.0.8`
  source or `sstabledump` output; the tests are Cassandra-written fixtures with the expected
  verdict derived from Cassandra's reconciliation code, never from CQLite's prior behaviour
  (#3041, #3042). A round-trip test of CQLite-written data cannot validate a verdict.
- **The verdict vocabulary is closed** and each verdict is asserted by a fixture in which it is
  the *only* correct answer. An `unknown`/`other` verdict is a bug, not a category.
- **`now` is always explicit in the output** (and pinned in tests). `droppable`, `expired` and
  `purgeable` are meaningless without it; `explain` prints the `now` it used on its first line.
- **No reconciliation in the `raw.*` tables except the verdict column.** They show what is on
  disk. The verdict is an annotation from the engine, never a filter — a query that wants only
  winners writes `WHERE verdict = 'winner'`.
- **Fail closed, loudly.** A generation that cannot be read is a row in `raw.generations` with an
  `error` column and an `Err` from `explain`, never a silently shorter trace — this is exactly
  the class #4159 is closing at the scan layer.
- **Read-only.** Nothing in this surface writes, including `compact --dry-run`.
- **Byte budgets apply.** `raw.cells` over a wide partition is large by construction; the
  existing `max_result_bytes` / streaming egress rules govern it like any other query.

## 6. Slicing

| Slice | Delivers | Depends on | Size |
|---|---|---|---|
| **F0 — decision trail** | the merge emits a per-cell verdict + deciding fact instead of discarding it; behind a flag; zero egress change; parity tests unchanged | nothing | the load-bearing piece; medium |
| **F1 — `explain`** | §4.1 verb over F0, `table`/`json`/`csv`, pinned-`now` fixtures for every verdict | F0 | small |
| **F2 — `raw.cells` + `raw.tombstones`** | queryable tables through the CLI query engine | F0 | medium |
| **F3 — `tombstones` report + `compact --dry-run --explain`** | formatted views over F2 / F0 | F0, F2 | small |
| **F4 — `raw.generations` + `find` (incl. CommitLog)** | per-generation metadata table; key search across generations + segments | Statistics parser, CommitLog reader (exist) | small–medium |
| **F5 — Trino `cqlite_raw` schema** | F2 tables through Flight + connector | F2, connector release | medium |
| **F6 — `--as-of`, `diff`** | time travel; replica diff | F0, F2 | medium each |

F0 is the only slice that touches the engine, and it is additive (a trail beside the result).
Everything after it is egress. F0 + F1 is the demo: `cqlite explain` on a real tombstone
problem, next to the `sstabledump` output an operator would otherwise be reading.

## 7. Open questions (for the grooming thread)

- Does the decision trail live in the merge output type (a side channel per cell) or as a
  separate pass that re-runs the merge in "explain mode"? The first is one mechanism and zero
  drift; the second keeps the hot path untouched. Measure the cost of the first before choosing.
- `raw.*` as virtual tables in the CLI query engine vs a dedicated `cqlite raw` verb with a
  restricted filter grammar. The former is the better product; the latter ships sooner.
- Naming: `explain` collides with SQL `EXPLAIN` (a plan). `why` is shorter and unambiguous;
  `trace` is neutral. Decide before F1.
- Whether the verdict vocabulary should also cover the *read-time* reconciliation that differs
  from compaction (e.g. a cell shadowed only at read time by a not-yet-purged tombstone). It
  should; check that `reconcile_rules.rs` exposes the distinction.

## 8. Relationship to the PRD

Fits **SE1** (an explicit, testable statement of what the engine's reconciliation guarantees,
made visible) and **SE4's planner gate** (`compact --dry-run --explain` is the "pure
explain/simulate interface" that gate asks for). It is an operator outcome in the PRD's own
table ("understandable plans") for the storage operator. It does not depend on SE2/SE3 and can
proceed in parallel with them.
