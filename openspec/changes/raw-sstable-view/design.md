# Design: the raw SSTable view

Issue #4222. Three items were marked NEEDS-YOU (owner) in the issue body; the issue is
`resume-dont-ask` sealed, so D1–D3 record the decision, the alternatives considered, and the
rationale rather than a question. D4 is a fourth decision forced by a fact the owner's framing did
not anticipate (the query engine has no `JOIN` executor at all) — recorded the same way. D5 onward
is the supporting architecture, cited to the exact functions this change reuses.

## D1 — Naming: `<keyspace>.<table>_raw_sstable_data`

**Decision: a suffixed table name in the existing single-namespace resolver — `ks.table_raw_sstable_data`
— never a second schema segment and never a table-valued function.**

Alternatives considered:

| Option | Why rejected |
|---|---|
| Schema-prefixed `raw.ks.t` (the forensics-surface doc's own illustrative spelling, §4.2) | The resolver has exactly one two-segment `keyspace.table` shape (`SelectExecutor::extract_table_id`, `select_executor/mod.rs:1275`) and no notion of a schema/namespace above keyspace. Introducing `raw.ks.t` means either a three-segment grammar change to the parser and every `FromClause` consumer, or overloading `raw` as a fake keyspace — both larger and murkier than a name suffix, and the three-segment form doesn't even match CQL's own `keyspace.table` shape a user already knows. |
| Table function `raw_sstable('ks.t')` | Requires table-valued-function call syntax in the FROM position, which the custom parser has no grammar for at all (no precedent found anywhere in `query/select_ast.rs`/the parser). A function call is also a worse joinability target — it isn't a stable name two `FROM` clauses can both reference, undermining the very "joinable, referenced twice" acceptance criterion (#4222 AC5). |
| `<table>_raw_sstable_data` suffix, resolved as an ordinary `keyspace.table` string | **Chosen.** No parser change: the string `ks.table_raw_sstable_data` already parses as a normal two-segment table reference today (`FromClause::Table`/`TableAlias`, `select_ast.rs:187`). Interception happens entirely at *resolution* time (D6), which is the smallest surface change and the one a SQL user can type "without learning a function," per the issue's own framing. |

Consequence: the suffix is a **naming convention over the same flat namespace**, not a new kind of
catalog object — which is honest, since (per D6/D7 below) there still isn't a catalog; the base table
and its raw view are two strings resolved by two different code paths sharing one schema.

## D2 — Fold #4205's SSTable-generation enumeration into this view

**Decision: yes, exactly as scoped — `SELECT DISTINCT sstable, generation FROM ks.t_raw_sstable_data
WHERE <pk> = ?` answers "which generations hold this key," and #4205 (`find`) keeps the CommitLog half
*and* keeps its own finer probe-classification vocabulary for misses.**

This view is a **positive-hits-only** projection by construction: a generation contributes rows only
when the point-read path actually resolved the key inside it (D5). #4205's acceptance criteria ask for
`hit | bloom-negative | index-miss` **per generation, including the generations that do NOT hold the
key** — that distinction (a bloom-filter negative vs. an index-miss after a bloom-filter false
positive) comes from `SSTableReader::might_contain_partition` (`storage/sstable/reader/partition_lookup.rs:469`)
and `has_partition_index` (`partition_lookup.rs:766`), consulted *before* any row is ever produced, and
a query engine result set has no natural way to emit "this generation was consulted and said no." So
the fold is real but partial: this view gives #4205 the "which generations hold it" **enumeration**
for free (no #4205 code needed to reimplement `DISTINCT sstable`), while #4205 keeps building and
owning the negative-probe classification and the CommitLog segment scan, unchanged from its own
grooming. Recorded in #4205's own tracking, not touched by this change.

## D3 — #4193 (`explain`) consumes this view's rows

**Decision: the column contract's per-cell facts are the closed set #4193's reconciliation trace needs
as raw input — write timestamp, TTL, local deletion time, tombstone kind, source SSTable, and
byte position — so #4193 can be built as a formatted view over this one's rows without either surface
re-deriving facts the other already computed differently.**

Concretely, both surfaces bottom out in the same pre-reconciliation per-generation data: this view's
data source (D5) is `SSTableReader::read_single_partition_for_compaction`
(`storage/sstable/reader/data_access/point_compaction.rs:98`), which is the exact per-generation
`runs` entry that `build_single_partition_merger`
(`storage/write_engine/merge/point_read.rs:246-398`) currently discards after handing it to
`KWayMerger::from_row_iterators`. #4193's F0 hooks its `RecordingSink` into the reconcile/merge step
(`merge/reconcile.rs`, `merge/mod.rs`) that runs on that *same* per-generation input one step further
downstream. Because both surfaces read the identical `CompactionRow`/`SimpleCell`/`Value::Tombstone`
facts (D5), a `winner`/`shadowed-by-*` verdict #4193 computes and a physical row this view shows can
never disagree on the underlying timestamp/TTL/LDT/tombstone-kind/source values — only on the
*verdict* layered on top, which is explicitly this view's non-goal. The required columns
(`<col>_timestamp`, `<col>_ttl`, `<col>_local_deletion_time`, `<col>_tombstone`, `sstable`,
`generation`, `position`) are exactly what `reconcile_rules.rs::cell_wins`
(`storage/write_engine/reconcile_rules.rs:70`) and `purge_gc_grace`
(`merge/reconcile.rs:577`) already consume to decide a winner — see D7 for the full column list.

## D4 — The `JOIN` gap (forced decision, not an issue NEEDS-YOU)

The issue's illustrative shape (`SELECT ... FROM ks.t JOIN ks.t_raw_sstable_data USING (<pk>, <ck>)`,
AC5) cannot execute: `cqlite query`'s SQL engine has **no `JOIN` executor at all**.
`m2_select_validator.rs` treats every `JOIN` variant as an explicit M2-unsupported feature and
rejects it with a validation error before planning
(`cqlite-cli/tests/unsupported_query_tests.rs:11-23`, asserting exit code 5); there is no `PlanType::Join`
execution path in `query/executor.rs` or `planner.rs` — the one `PlanType::Join` reference is a
display-label match arm, not an executor.

**Decision: "joinable" (AC5) is satisfied by correlated queries over the shared key columns, not by
literal `JOIN` syntax executing.** `SELECT DISTINCT sstable ... WHERE <pk> = ?` (the #4205-folding
half of AC5) needs no JOIN and works today. The other half — "returns the logical row beside each
physical row" — is satisfied by running the same `WHERE <pk> = ?` predicate against `ks.t` and
`ks.t_raw_sstable_data` as two separate `SELECT`s sharing the identical key columns and asserting the
logical row's values match the reconciled subset of the physical rows' `<col>` values (never the
`<col>_timestamp`/`_ttl`/etc. columns, which have no counterpart on the logical side). This is weaker
than a literal JOIN but is the correct scope boundary: **teaching the query engine to execute `JOIN`
is a cross-cutting query-engine capability, not a raw-view concern**, and is exactly the kind of work
the memory record's DataFusion/Design-A epic (#941, children #1905–#1914) exists to own. This change's
`tasks.md` files a follow-up issue against that epic (never invents JOIN support here) so the literal
syntax in AC5 has a tracked home instead of silently disappearing.

## D5 — Data source: reuse the per-generation, pre-reconciliation primitives, add none new to the engine

**Point-key case (`WHERE <partition key> = ?`).** Per generation, in ascending generation order:

1. Presence oracle: `SSTableReader::might_contain_partition` (`partition_lookup.rs:469`) — branches on
   `bti_partitions_db.is_some()` for BIG bloom-filter vs. BTI trie-descent probing.
2. Offset resolution: BIG via Summary/Index.db-guided lookup (`point_compaction.rs:4/19` module doc);
   BTI via trie descent (`SSTableReader::lookup_partition_via_bti_trie`, `partition_lookup.rs:192`,
   backed by `data_access/bti_point.rs`).
3. Row materialization for **that one generation only**:
   `SSTableReader::read_single_partition_for_compaction` (`data_access/point_compaction.rs:98`) →
   `SinglePartitionCompaction::Rows(Vec<CompactionRow>)`. This is the exact function
   `build_single_partition_merger`'s internal `probe_reader_async` already calls once per candidate
   generation (`merge/point_read.rs:554-604`) to build the `runs` entry it then merges away — this
   change is the first consumer to keep those per-generation rows instead of discarding them into
   `KWayMerger`.

`CompactionRow` (`storage/sstable/reader/compaction_row.rs`) already carries everything the column
contract (D7) needs, unreconciled: `CompactionRowData::Live` cells as `SimpleCell{column, value,
timestamp, ttl, local_deletion_time}` and `ComplexElement` for collections (`compaction_row.rs:559-601`),
`CompactionRowData::RowTombstone`, `CompactionRowData::PartitionDelete{deletion_time,
local_deletion_time}` (`:343-348`), and `CompactionRowData::RangeMarker{start, end, deletion_time,
local_deletion_time}` (`:321-330`) with `CompactionBound::Inclusive|Exclusive` (`:155-159`). Tombstone
*kind* comes from `Value::Tombstone(Box<TombstoneInfo>)` (`cqlite-core/src/types.rs:402-427`), whose
`TombstoneType` enum (`types.rs:462-485`) already distinguishes `CellTombstone` / `RowTombstone` /
`RangeTombstone` / `TtlExpiration` / `PartitionTombstone` — this is the literal source of the
`<col>_tombstone`/`row_tombstone` values in D7, not a new classification this change invents.

**No-predicate case (bounded full scan).** `SSTableReader::stream_all_partitions_for_compaction`
(`data_access/compaction.rs:589`) is a true per-generation streaming callback
(`FnMut(CompactionRow) -> Result<ControlFlow<()>>`) that the module's own doc comment says
"true-streams each partition as the index walk resolves it, rather than materialising the whole
SSTable into one Vec" (`compaction.rs:609-618`). The raw view's full-scan path calls this once per
generation directly (never through `KWayMerger`, which reconciles across generations — exactly what
this view must not do) and emits one `QueryRow` per `CompactionRow` as it arrives.

**Why not `KWayMerger` at all, for either case.** `KWayMerger` (`merge/mod.rs:577`) exists to
*reconcile* — its entire purpose is collapsing N generations' versions of a cell into one winner. This
view's contract (#4222 AC2: "one output row per physical row per SSTable... a key present in N
generations yields ≥N rows") is the deliberate absence of that step, so the view's row producer stops
one layer short of where `KWayMerger` begins, on both the point-key and full-scan paths.

## D6 — Catalog interception: where `_raw_sstable_data` is recognized

Since no catalog exists (Problem #1), the suffix must be recognized at the two points table
resolution already happens:

- **Schema resolution** — `resolve_table_schema` (`select_executor/mod.rs:1289`) calls
  `SchemaRegistry::find_schema_by_table`. A new step ahead of this strips a trailing
  `_raw_sstable_data` from the table name, resolves the **base** table's `TableSchema` from the
  registry unchanged, and — only for the raw-view name — synthesizes the expanded raw-view
  `TableSchema` (D7's column set) from it. The base table's own resolution path is untouched.
- **Data access** — `extract_table_id` (`select_executor/mod.rs:1275`) currently feeds straight into
  `StorageEngine::scan`. For a name carrying the suffix, this change routes to a new scan
  implementation (D5) instead — never to `StorageEngine::scan`/`SSTableManager::scan_with_meter`,
  which reconcile.

This keeps the interception in exactly the two functions that already own "turn a table name into
data," rather than inventing a parallel catalog layer — consistent with D1's naming choice (a string
convention, not a new object kind).

## D7 — Column contract

Per base table, the raw view's columns are (types shown as CQL-visible types):

**Key + regular/static columns** — decoded per schema exactly as the base table (no-heuristics #28) —
plus, per non-key column `<col>`:

| Column | Type | Meaning | Source |
|---|---|---|---|
| `<col>_timestamp` | `bigint` | write time, µs | `SimpleCell::timestamp` / `CellWriteMetadata::write_timestamp_micros` (`types.rs:434-446`) |
| `<col>_ttl` | `int`, NULL if none | declared TTL, seconds | `SimpleCell::ttl` / `CellExpiration::ttl_seconds` (`types.rs:450-458`) |
| `<col>_local_deletion_time` | `int`, NULL if live-no-expiry | expiry/tombstone time, epoch seconds | `SimpleCell::local_deletion_time` |
| `<col>_tombstone` | `text`, NULL \| `cell` \| `expired` | tombstone kind for this cell | `Value::Tombstone(TombstoneInfo{tombstone_type,...})`, `TombstoneType::{CellTombstone,TtlExpiration}` (`types.rs:462-485`) |
| `<col>_complex_deletion` | `boolean`, collections/UDTs only | whether a complex (list/set/map/UDT) deletion marker is present for this cell | `ComplexColumn.complex_deletion: Option<(i64,i32)>` (`compaction_row.rs:581-583`) |
| `<col>_complex_deletion_time` / `_timestamp` | `int`/`bigint`, NULL unless the above is true | the complex deletion's LDT and writetime | same field, tuple halves |

Row level: `row_timestamp` (`bigint`, NULL if no explicit liveness marker —
`RowLiveness::marker_timestamp`, `compaction_row.rs:194-211`), `row_ttl` (`int`), `row_local_deletion_time`
(`int`), `row_tombstone` (`text`, NULL \| `row` \| `expired`, from `RowHeader::row_tombstone()`,
`row_decoder/mod.rs:437-484`).

Partition level: `partition_deletion_time` (`bigint`), `partition_deletion_timestamp` (`bigint`), both
NULL unless this generation carries a partition tombstone for this key —
`CompactionRowData::PartitionDelete` (`compaction_row.rs:343-348`).

Row-kind discriminator, present on every row: `row_kind` (`text`) ∈ `row` \| `partition_tombstone` \|
`range_tombstone_start` \| `range_tombstone_end` — needed because a physical "row" in this view is not
always a data row (issue shape table shows only the `row` case; range tombstones need their own
discriminated shape, next).

Range tombstones — emitted as their own rows (never merged into a data row), one row per bound:
`row_kind` = `range_tombstone_start`/`range_tombstone_end`, the clustering-key columns hold the
bound's component values (`CompactionBound::Inclusive|Exclusive(Vec<(String,Value)>)`,
`compaction_row.rs:155-159` — a prefix of the full clustering key when the bound doesn't specify every
component, per `test_deltas/range_tombstones`' documented prefix-bound case), `bound_inclusive`
(`boolean`), `range_deletion_time` / `range_deletion_timestamp` (from `CompactionRowData::RangeMarker`,
`compaction_row.rs:321-330`). All non-key, non-range columns are NULL on these rows.

Source columns, present on every row: `sstable` (`text`, Data.db file name), `generation` (`int`),
`format` (`text`, `big` \| `bti`), `position` (`bigint`, byte offset of the partition in Data.db —
already resolved as part of D5 step 2, so no extra I/O to expose it).

Dropped columns (AC6): a column present on-disk but absent from the *current* schema is exposed under
its on-disk name with a `dropped` marker rather than hidden. `row_data.rs:404-428` already **detects**
this case (decodes it via a synthetic `Column` from the authoritative on-disk marshal type) but
currently discards the value (`emit = ctp.schema.is_some()` gates it out, `:410`) — this change flips
that gate for the raw-view path specifically (the base table's SELECT behavior is unchanged, staying
in scope of #4222 only). `tasks.md` includes confirming exactly how `TableSchema` records a
historically-dropped column's declared type for this to decode against, since the research pass found
detection but not the schema-side "this name used to exist as type T" bookkeeping — that is
implementation-phase work, not a design gap: the two committed fixtures
(`test_tomb/dropped_regular_col`, `dropped_static_col`) exist for exactly this case and the schema
comment (`test-data/schemas/tombstone-parity.cql`, Table 6 header) documents the on-disk shape.

## D8 — Fail-closed errors (AC6)

Unknown table or missing schema for the *base* table name (after stripping the suffix) raises
`Error::Table`/`Error::Schema` explicitly — the same typed variants that already exist
(`cqlite-core/src/error.rs:265`, `:78`, both mapped to CLI exit code 3 by `classify_error`,
`cqlite-cli/src/error.rs:82-108`) but are raised only in narrow cold paths on the base SELECT path
today (`schema/mod.rs:1027`; the normal path silently returns 0 rows or falls back to row-derived
columns — Problem #1). This is a deliberate, scoped divergence: the raw view is new surface built
under the no-heuristics/fail-closed doctrine from the start, and does not change the base table's
existing (separately-scoped, out-of-issue) permissive behavior.

## D9 — Streaming / memory budget (AC4, oom-audit)

Both D5 row producers are the streaming primitives already used elsewhere
(`stream_all_partitions_for_compaction`'s documented true-streaming callback; per-generation
`read_single_partition_for_compaction` bounded by one partition). The new query-engine-facing
producer function must therefore itself be a `ScanStreamItem`/callback-driven producer, not a
`collect::<Vec<_>>()` — `xtask/src/oom_audit/rule.rs`'s `STREAM_RETURNS_VEC` rule
(`fn_is_bounded`, `oom_audit/rule.rs:348`) flags exactly a scan-shaped function that collects a
row/partition/cell stream into a `Vec`, unless it carries a `ResultBudget`/`limit`/`max_*` parameter or
a `.take(n)` call. The existing `max_result_bytes` check (`query/result_budget.rs:57`,
`enforce_result_budget`) applies to this view's rendered output exactly as it does to any other query
(`select_executor/execute.rs:119,288`) — no new budget mechanism, reuse of the existing one.

## D10 — Test/oracle mapping

Every requirement's scenario names a committed fixture (verified present with `Data.db.jsonl`
goldens in this worktree): `test_tomb/dropped_regular_col`, `dropped_static_col`,
`resurrection_gc_positive`, `skipped_partition_delete`, `wide_range_tombstone`,
`gc_before_boundary` (2-generation tombstone/TTL cases, `test-data/schemas/tombstone-parity.cql`);
`test_deltas/range_tombstones`, `partition_tombstones`, `cell_tombstones`, `ttl_cells` (3 generations
each, `test-data/schemas/deltas.cql`); `test_compactionparity/live_clustering` (multi-generation,
`compaction-parity.cql`); `test_da.wide_table` (BTI/`da`, `test-data/schemas/wide-table-bti.cql`);
`test_comp/lz4_table` (compressed) and `test_comp/uncompressed_table` (uncompressed,
`compression-parity.cql`).
