# flight-single-sstable-bypass — delta for flight-single-sstable-bypass (issue #3058)

## ADDED Requirements

### Requirement: The single-source decision is made from authoritative state, fail-closed
The Flight `do_get` streaming row path SHALL decide whether to take the single-source fast path from
the **exact, authoritative count of post-prune sources** at the decision point in
`MergeProducer::produce_streaming_from_readers` — `prune_readers(readers).len()`
(`cqlite-flight/src/producer_warm.rs:56,134`), where `readers` is the warm registry's reader set
derived from the `GenerationSet` (`warm/identity.rs:96`) that `probe::probe_generation_set`
(`warm/probe.rs:122`) builds from the authoritative `*-Data.db` directory listing. The count SHALL
NEVER be inferred from file sizes, byte content, statistics estimates, or any other guessed signal
(no-heuristics mandate, issue #28).

The fast path SHALL be taken ONLY when ALL of the following hold, and SHALL fall back to the
existing `KWayMerger` path when ANY of them does not:
- the post-prune source count is exactly 1;
- `schema.dropped_columns` is empty (the reconciler's Step 3b timestamp-based dropped-column purge,
  `write_engine/merge/reconcile.rs:406`, has no `PartitionShadow` counterpart);
- the request is not aggregating (already guaranteed by the `is_aggregating()` early return at
  `producer_warm.rs:52`);
- the request did not take the full-PK-equality point-read route (`producer_warm.rs:75`);
- **the schema declares no STATIC column** (see below);
- the forced-path override does not request the merge arm.

**Static-column exclusion (fail-closed, deferral — issue #3095).** Delivery of this change
established, against the pinned `cassandra-5.0.8` source
(`cql3/statements/SelectStatement.java::processPartition`, L1089-1152), that **both** arms are
already wrong for static-bearing tables and wrong in *different* directions: the merge arm emits a
phantom `ck = null` row per static-bearing partition AND leaves the static column null on the real
clustering rows (N+1 rows where Cassandra returns N), while the single-generation arm injects
statics correctly but returns zero rows for a static-only partition where Cassandra returns one.
The merge-arm defect is **pre-existing on `main`** and is not introduced here.

Because the two arms disagree, routing static-bearing tables to the fast arm would **change query
results** — which this change forbids. A static-bearing schema SHALL therefore take the merge arm,
preserving today's (incorrect but unchanged) behavior. This is an explicit **deferral, not a
design position**: issue #3095 owns making static semantics Cassandra-correct on both arms, after
which this precondition SHALL be removed and the bypass SHALL cover static-bearing tables.

The predicate SHALL be conjunctive and fail-closed: any condition that cannot be established from
authoritative state takes the merge arm.

#### Scenario: One post-prune source selects the fast path
- **GIVEN** a warm `do_get` over a table directory whose authoritative generation set contains exactly one `*-Data.db` generation, with an empty `schema.dropped_columns`, no aggregation, and no full-PK-equality predicate
- **WHEN** `produce_streaming_from_readers` reaches its full-scan branch
- **THEN** the post-prune source count observed at the decision point is 1 and the single-source fast path is selected

#### Scenario: Token pruning to one source still selects the fast path
- **GIVEN** a warm `do_get` over a table with two generations where the ticket's token filter prunes one of them away via already-parsed endpoint tokens (`prune_readers`, zero extra I/O)
- **WHEN** the decision point is reached
- **THEN** the count used is the POST-prune count (1), not the pre-prune count (2), and the fast path is selected

#### Scenario: A static-bearing schema falls back to the merger
- **GIVEN** a single-source `do_get` whose `TableSchema` declares at least one STATIC column
- **WHEN** the decision point is reached
- **THEN** the fast path is NOT selected and the request is served by the existing `KWayMerger` path, so its results are byte-for-byte what they are today (see the static-column exclusion above and issue #3095)

#### Scenario: A non-empty dropped_columns map falls back to the merger
- **GIVEN** a single-source `do_get` whose `TableSchema.dropped_columns` is non-empty
- **WHEN** the decision point is reached
- **THEN** the fast path is NOT selected and the request is served by the existing `KWayMerger` path, so the reconciler's timestamp-based dropped-column purge still runs

#### Scenario: The decision reads no byte content
- **WHEN** the bypass predicate implementation is inspected
- **THEN** it consults only the post-prune reader count, the schema's `dropped_columns` map, the aggregation flag, the point-read plan, and the forced-path override — and consults no file size, no `Statistics.db` estimate, and no SSTable byte content

### Requirement: A single-source `do_get` does not enter the k-way merge or compaction reconciliation
When the fast path is selected, the request SHALL NOT construct a `KWayMerger`, SHALL NOT invoke
`KWayMerger::reconcile_cluster_with_overlap_counted`, and SHALL NOT invoke
`CompactionPolicy::on_data_row` (`row_decoder/compaction.rs:585`). This SHALL be verified by a
**path-taken assertion that fails if the merge path is entered** — an explicit observation of which
arm ran (e.g. a counter/probe on the merger construction and on the reconcile entry), NOT a
throughput assertion and NOT a timing inference (issue AC #1).

#### Scenario: A single-SSTable do_get never constructs the merger
- **GIVEN** an end-to-end `do_get` over a real single-SSTable fixture with the fast path enabled
- **WHEN** the whole response stream is drained
- **THEN** the observed merger-construction count for that request is zero and the observed compaction-reconcile entry count is zero, and the assertion FAILS if either is non-zero

#### Scenario: The path-taken observation is explicit, not inferred
- **WHEN** the test that pins AC #1 is inspected
- **THEN** it asserts on a directly observed path marker (merger construction / reconcile entry) and does not assert on elapsed time, throughput, or CPU share

### Requirement: The per-cell write-metadata map is eliminated on the single-source path
On the fast path, no `HashMap<String, CellWriteMetadata>` SHALL be allocated per row and no
per-cell `column.name.clone()` + hash insert (`row_data.rs:585,:757`) SHALL be performed: the row
decoder SHALL run with `want_cell_metadata == false`, matching the normal read path the decoder
documents at `row_data.rs:71-73`. The map SHALL be eliminated by **not entering the path that builds
it**, not by optimizing the map (issue AC #2; acceptance-contract scope fence §3).

#### Scenario: No cell-write-metadata map is built for a single-source scan
- **GIVEN** an end-to-end single-source `do_get` over a real fixture
- **WHEN** the rows are decoded on the fast path
- **THEN** the decoder is invoked with `want_cell_metadata == false` and zero `CellWriteMetadata` maps are constructed for that request

#### Scenario: The emitted rows still carry no cell metadata, exactly as before
- **GIVEN** the same single-source `do_get`
- **WHEN** the emitted `QueryRow`s are inspected
- **THEN** every row's `cell_metadata` is `None`, identical to the pre-change Flight behaviour (`filter.rs:570,579`; `agg.rs:467,738,793`) — no consumer observes a difference

#### Scenario: The compaction writer's metadata path is untouched
- **WHEN** the compaction/write surface is exercised
- **THEN** `CompactionPolicy::on_data_row` still requests per-cell metadata and compaction output is unchanged — the change removes the READ path's use of it, not the capability

### Requirement: The multi-source path is unchanged and still reconciles
With two or more post-prune sources, `do_get` SHALL build and drive the `KWayMerger` exactly as it
does today, with full cross-source reconciliation (LWW resolution, tombstone shadowing, TTL expiry,
range tombstones, static rows). This SHALL be pinned by a Flight-surface test over **at least two
overlapping SSTables** (issue AC #4), which is new coverage: every committed
`test_compaction_tombstone_ttl` fixture directory currently holds exactly ONE `nb-3-big-Data.db`, so
today both query-semantics oracles exercise only a single-SSTable table. The change SHALL NOT
regress issue #2988 (multi-generation SELECT continues to drive the buffered `KWayMerger`).

#### Scenario: Two overlapping SSTables still enter the merger
- **GIVEN** a Flight `do_get` over a table directory containing at least two overlapping `*-Data.db` generations that shadow each other (a value overwritten in the later generation, plus a row deleted in the later generation)
- **WHEN** the stream is drained
- **THEN** the merger IS constructed and the compaction-reconcile entry count is non-zero — the path-taken assertion fails if the fast path was taken

#### Scenario: Two overlapping SSTables produce correctly reconciled rows
- **GIVEN** the same ≥2-generation fixture at a PINNED `now`
- **WHEN** the emitted rows are compared against the recorded expected post-reconciliation result set
- **THEN** the overwritten value is the later generation's, the deleted row is absent, and no shadowed cell surfaces

#### Scenario: The multi-generation core pins stay green
- **WHEN** `issue_1579_streaming_multigen_memory`, `issue_1579_streaming_multigen_order`, `issue_957_streaming_materializing_parity`, `issue_2096_seeking_point_merge_parity`, and the `step_streaming_matches_step_for_*` oracles are run
- **THEN** all pass unchanged, confirming #2988's buffered multi-generation merge is not regressed

### Requirement: Read-time reconciliation semantics are preserved on the fast path
The fast path SHALL apply SELECT-semantic read reconciliation with results indistinguishable from
the merge path for a single source: partition deletions, range tombstones, row tombstones, cell
tombstones, and TTL expiry. **Static columns are OUT OF SCOPE of the fast path** — a static-bearing
schema takes the merge arm (see the static-column exclusion in the predicate requirement and issue
#3095), so static-cell injection is not a property of this change and is not asserted here. It
SHALL run with
`read_shadowing = true` (`scan_stream_windowed.rs:748-751`) so `PartitionShadow`
(`row_decoder/partition_shadow.rs:44`) is active, and its TTL/expiry clock SHALL be the request's
`now_secs` — the SAME clock the merge arm threads via `with_now_secs`
(`producer_warm.rs:115-117`) — never an ambient wall-clock read, so a PINNED `now` is honored on
both arms.

#### Scenario: The fast path runs with read shadowing enabled
- **WHEN** the fast path's parser construction is inspected
- **THEN** it is built with `read_shadowing = true` and `PartitionShadow` is opened for each partition, so tombstone/TTL shadowing is applied

#### Scenario: Tombstone, TTL and range-tombstone cases reconcile identically on both arms
- **GIVEN** a single-SSTable fixture with no static column, containing a partition deletion, a range tombstone, a row deletion, an expired-TTL cell and a live-TTL cell, at a PINNED `now`
- **WHEN** the same `do_get` is run twice over the SAME bytes — once forced to the fast path and once forced to the merge path
- **THEN** the two runs return identical rows, identical column values, and identical row order

#### Scenario: The pinned now is honored on the fast path
- **GIVEN** a fixture whose TTL cell expires between two pinned `now` values
- **WHEN** the fast path is run at each pinned `now`
- **THEN** the cell is present at the earlier `now` and absent at the later one — the fast path reads the request's `now_secs`, not wall-clock

#### Scenario: A row tombstone is suppressed from output
- **GIVEN** a single-source fixture containing a whole-row deletion
- **WHEN** the fast path streams the partition
- **THEN** the deleted row does not appear in the output (the row reaches the adapter as a non-row marker and is suppressed), matching the merge path's `RowData::Tombstone → None` behaviour

#### Scenario: A row with a live data cell but no primary-key liveness marker is still returned
- **GIVEN** a single-source fixture row written by an `UPDATE ... SET v = ? WHERE pk = ? AND ck = ?` (a live data cell, no PK liveness marker), read under both a `SELECT *` and a primary-key-only projection
- **WHEN** the fast path streams it
- **THEN** the row IS returned under both projections, identically to the merge path's `has_live_data_cell` visibility rule (`producer.rs:1199-1220`, issues #2374/#2789)

### Requirement: Query-result output is unchanged, proven by the semantic oracles and a forced-path differential
The change SHALL NOT alter any query result. Note this holds **because** the shapes on which the two
arms are known to disagree are routed to the merge arm fail-closed: static-bearing schemas (issue
#3095) via the bypass predicate. Two further shapes are excluded from the forced-path differential
as **documented pre-existing defects, not as convenient omissions** — a CQLite-written simple cell
tombstone reaching the Arrow encoder as `Value::Tombstone`, which fails the stream on BOTH arms
(issue #3094), and `set<frozen<UDT>>`, which the merge arm alone fails closed on (issue #2339).
Each exclusion SHALL be commented in the test with its issue reference, and each SHALL be retired by
that issue.

This SHALL be proven by (1) both query-semantics
oracles at a PINNED `now` — core `query_semantics_oracle_parity.rs` (gate component
`query-semantics-oracle`) and Flight `query_semantics_flight_parity.rs` (gate component
`flight-query-semantics-oracle`) — since physical-dump parity alone cannot see a
read-time-reconciliation divergence (issue #1742); (2) the point-vs-full differential lane
(`cqlite-core/tests/point_vs_full_differential.rs`, issue #1918); and (3) a **forced-path
differential** over the SAME bytes, enabled by a documented override that pins the fast path or the
merge path.

Issue AC #5 pins the WS0 corpus output at **3,999,890 rows, 12 cells/row, digest
`0x4903ffa446163c4b`**. That corpus is absent from the delivery machine (see the acceptance
requirement below), so AC #5 SHALL be discharged **in form** on the locally generated corpus: a
full-scan `do_get` SHALL produce a byte-identical row count, cells/row, and value digest when run
forced to the merge arm and forced to the bypass arm over the same bytes. The WS0 digest itself
SHALL be recorded as an owed check, and SHALL NOT be reported as verified.

#### Scenario: Both query-semantics oracles pass at a pinned now
- **WHEN** the `query-semantics-oracle` and `flight-query-semantics-oracle` gate components run against `test-data/query-semantics-oracle.json` at its pinned `now`
- **THEN** every recorded post-reconciliation result set matches exactly, with the single-SSTable fixtures now served by the fast path

#### Scenario: The forced-path override makes both arms runnable over the same bytes
- **GIVEN** a documented forced-path override with values selecting the fast path, the merge path, or automatic selection
- **WHEN** a single-SSTable fixture is served under each forced value
- **THEN** the fast-path run and the merge-path run return byte-identical result sets in the same order, and the automatic run matches the fast-path run

#### Scenario: The point-vs-full differential lane stays green
- **WHEN** `point_vs_full_differential.rs` runs the same point-eligible query under the forced point and full read paths at a PINNED `now`
- **THEN** the rows, values, and order are identical, unchanged by this change

#### Scenario: Row count, cells/row and value digest are arm-invariant on the local corpus
- **GIVEN** the locally generated ~4,000,000-row single-SSTable corpus
- **WHEN** a full `do_get` scan is run forced to the merge arm and forced to the bypass arm, and the streamed values are digested in each run
- **THEN** the two runs agree exactly on row count, cells/row, and value digest

#### Scenario: The WS0 digest is recorded as owed
- **WHEN** the change is written up
- **THEN** the WS0 AC #5 triple (3,999,890 rows, 12 cells/row, digest `0x4903ffa446163c4b`) is listed as an unverified owed check against a machine holding that corpus, and is not claimed as reproduced

### Requirement: Existing Flight data-plane features are preserved on the fast path
The fast path SHALL preserve every feature the merge path provides on the streaming row route:
ScanSpec predicate pushdown, projection, token pruning (including the token bound pushed into the
per-SSTable Summary-guided walk), the `max_batch_bytes` byte-bounded batching budget, mid-stream
cancellation via `CancelFlag`, scan-progress reporting, the resolved UDT registry, the
`on_merger_built` phase-boundary fire, and admission control. The aggregate route SHALL be
untouched — aggregating tickets return early at `service.rs:1028` and never reach this branch.

#### Scenario: Predicate pushdown, projection and token pruning behave identically
- **GIVEN** a single-source `do_get` carrying a pushed-down predicate, a narrowed projection, and a token range
- **WHEN** the stream is drained on the fast path
- **THEN** the returned rows are exactly those the merge path returns for the same ticket, with the same columns and the same token-range restriction

#### Scenario: Byte-bounded batching and cancellation still hold
- **GIVEN** a single-source `do_get` with a configured `max_batch_bytes` and a client that cancels mid-stream
- **WHEN** the stream is driven
- **THEN** no emitted batch exceeds the byte budget, and the cancellation stops the scan mid-partition without draining the remaining rows

#### Scenario: A frozen UDT inside a collection still decodes structurally
- **GIVEN** a single-source `do_get` over a table with a `frozen<UDT>` inside a collection, with the UDT registry resolved from the ticket DDL
- **WHEN** the rows are streamed on the fast path
- **THEN** the UDT surfaces as a structured value, identical to the merge path

#### Scenario: Aggregate tickets are unaffected
- **GIVEN** a `do_get` whose ticket carries an aggregation spec
- **WHEN** it is resolved
- **THEN** it takes the aggregate route and never reaches the single-source branch, and its results are unchanged

### Requirement: The change is accepted on an external throughput number, with a stated kill criterion
Acceptance SHALL be an **external throughput measurement**, warm, per **physical** core, reported as
**rows/s AND cycles/row** (never CPU-share). A reduction in the share of CPU spent in the merge with
unmoved rows/s is a **FAIL**.

The WS0 rig (`ws0.events`; `/home/ubuntu/ws0/ws0-corpus/rerun.sh`, `ws0-h2h/`,
`ws0-results/head-to-head-method.md`) is **absent from the delivery machine** and is not committed to
the repository, so the WS0 absolute (`>= ~280,000` rows/s; beat Cassandra's `212,981`; up from
`61,151`) is **not reproducible here**. CQLite's write surface is additionally uncompressed-only
(claim boundary #1406) while the WS0 corpus is LZ4 `chunk_length=16384`, so no locally generated
corpus can reproduce that absolute.

Acceptance for THIS change is therefore **ratio closure on a locally generated corpus** (owner
decision, Seam 1): a corpus of ~4,000,000 rows SHALL be generated on the delivery machine, and
**both** surfaces — the bare scan (`execute_streaming`) and Flight `do_get` — SHALL be measured over
those **identical bytes** on the **same box, same pinned physical cores, same run**. Both the
pre-change and post-change ratio SHALL be measured locally so the delta is self-contained; the WS0
absolute SHALL NOT be restated as if reproduced.

**Delivered result (measured; owner accepted at delivery).** The bypass SHALL demonstrate a
**material, externally-measured throughput gain** on the target surface. Measured warm, pinned,
CPU-wide, median of 3:

| surface / arm | rows/s | cycles/row |
|---|--:|--:|
| bare scan | 312,155 | 22,012 |
| `do_get` merge (pre-change path, same binary) | 53,873 | 122,571 |
| `do_get` **bypass** | **210,192** | **27,600** |

That is **3.90x** on the target surface (4.44x by cycles/row) and closes the bare-scan gap from
**5.73x to 1.49x** — ~90% of the excess. The originally-stated **~1.3x** target is **NOT** reached;
the residual (27,600 vs 22,012 cycles/row, +25%) is Arrow encode + IPC framing, with all
merge-path probe counters at zero. Arrow encode is explicitly out of scope for this change and is
owned by **issue #3096**, whose acceptance criterion is closing 1.49x to ~1.3x. The shortfall SHALL
be recorded plainly in the PR and the issue, and SHALL NOT be rounded toward the target.

The measurement SHALL use CPU-wide `perf stat -C` (never `perf stat -p`, which costs >2x on this
workload) and SHALL pin the workload with `taskset` (unpinned measured 18.74 s vs 11.16 s pinned).
Both warm and cold numbers SHALL be reported, kept as separate claims. The change SHALL record the
WS0 absolute re-measurement as an explicit **owed follow-up** on a machine holding the byte-identical
corpus. If the bypass does not move the local `do_get` rows/s materially, the work SHALL STOP and the
negative result SHALL be posted rather than further levers stacked.

#### Scenario: The local ratio closes materially on the bare scan
- **GIVEN** a locally generated corpus of ~4,000,000 rows and a single SSTable, with the bare scan and Flight `do_get` measured over those identical bytes in the same session, warm, on pinned physical cores, with CPU-wide perf counters, median of at least 3 runs
- **WHEN** the merge arm and the bypass arm are measured on the SAME binary via the forced-path override
- **THEN** the bypass arm is at least 3x the merge arm's rows/s, the bare-scan gap closes from ~5.7x to under 1.6x, and every figure is reported as rows/s AND cycles/row

#### Scenario: The residual shortfall against the original target is reported, not rounded away
- **WHEN** the results are written up
- **THEN** they state plainly that the originally-stated ~1.3x target was not reached, give the achieved 1.49x, attribute the residual to Arrow encode with the supporting cycles/row figures, and name issue #3096 as its owner

#### Scenario: The WS0 absolute is reported as owed, never as reproduced
- **WHEN** the results are written up
- **THEN** they state plainly that the WS0 corpus was unavailable on the delivery machine, report the local ratio as the evidence actually obtained, and record the WS0 absolute re-measurement as an open follow-up — no WS0 number is presented as having been re-measured

#### Scenario: The measurement avoids both known traps
- **WHEN** the measurement procedure is inspected
- **THEN** it uses CPU-wide `perf stat -C` rather than per-process `perf stat -p`, and pins the workload with `taskset` — and it records both choices in the reported method

#### Scenario: Warm and cold are reported as separate claims
- **WHEN** the results are reported
- **THEN** the warm number (this issue's owned claim) and the cold number are stated separately, never blended into a single figure

#### Scenario: An immaterial improvement triggers the kill criterion
- **GIVEN** a correct, wired bypass whose measured `do_get` rows/s does not move materially
- **WHEN** the result is observed
- **THEN** the work STOPS, the negative result (rows/s + cycles/row, warm and cold) is posted, no further optimization is stacked on the unexplained result, and the attribution question is re-opened
