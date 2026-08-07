# Flight `do_get` read path — call graph

The end-to-end call graph for a row read through the Flight adapter (`cqlite-flight`) into
`cqlite-core`. This is the path the WS0 head-to-head and the C(N) concurrency sweep measure
(issue #3100, `docs/reports/ws0-3100-report.md`), and the pipeline #3217's flame-graph
attribution profiles.

Anchors are `file:line` as of `main` @ `f455ac6` (2026-08-01); function names are the stable
reference — re-`grep` if lines have drifted.

## Execution-context summary (read this first)

A single bypass-path `do_get` stream runs across **three OS execution contexts** joined by
**two bounded-channel handoffs**:

| # | Context | Role | Joined by |
|---|---------|------|-----------|
| 1 | tonic reactor (async task) | validate → admit → resolve; then poll/encode/ship batches | ← `tokio::sync::mpsc(4)` (handoff **B**) |
| 2 | blocking-pool thread ("producer", lives for the whole scan) | drive rows → build `RecordBatch`es → credit + send | ← `std::sync::mpsc::sync_channel` (handoff **A**) |
| 3 | `cqlite-query-rows` std thread (bypass arm only) | core Summary-guided walk + decode | — |

So N concurrent streams ≈ **3N contexts** (merge-arm streams have more: one producer thread
per input SSTable). This is the structural source of the handoff/context-switch cost the
pinned-core C(N) sweep observed (#3100: peak at N=2, decline to N=16, 38.3M context
switches; device idle). #3217's off-CPU flame graphs attribute blocked time to handoff A vs
handoff B by stack.

## The graph

```
CLIENT  FlightClient::do_get(Ticket{keyspace, table, ddl, projection, predicate?,
   │                                token-range?, limit?, snapshot?})
   │ gRPC
   ▼
━━ CONTEXT 1: tonic reactor (async task) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CqliteFlightService::do_get                          service.rs:553   span + RpcMetrics
└─ do_get_inner                                      service.rs:764
   ├─ PhaseTimer::start("do_get")                                     phase: validate (#2162)
   ├─ validate_do_get_ticket → FlightTicket::from_bytes  :955         syntax only, pre-admission
   ├─ admission.acquire()                            :839             phase: admission (#2420)
   │                                                                  --max-concurrent-scans; default is
   │                                                                  DERIVED clamp(2xP,2,64) as of #3225,
   │                                                                  64 is the ceiling, not the value
   │                                                                  sheds UNAVAILABLE, never a permit
   │                                                                  for a malformed ticket
   ├─ CancelFlag::new() + drop-guard                 :853             ONE flag spans setup + stream (#1476)
   ├─ do_get_resolve                                 :981             phase: resolve
   │  └─ tokio::spawn_blocking ─── (blocking pool, setup only)
   │     ├─ build_producer(ticket)                                    DDL parse; predicate/projection/
   │     │                                                            aggregation lowering → MergeProducer
   │     ├─ producer.arrow_schema() → Arc<ArrowSchema>
   │     ├─ resolve_dir (DirSource, pathsafe, cached)  :1020
   │     └─ warm.warm_readers(key, ddl_hash, …)        :1049          warm registry (#2356):
   │                                                                  generation-set probe; cached
   │                                                                  Arc<SSTableReader>s or fail-closed
   │                                                                  delta rebuild
   │        → DoGetInput::Rows(Vec<Arc<SSTableReader>>)
   │        (aggregating ticket → DoGetInput::Aggregate(paths) instead — see Variants)
   ├─ spawn_streaming_from_readers → spawn_streaming streaming.rs:375  phase: merge_setup
   │  ├─ mpsc::channel(DO_GET_CHANNEL_CAPACITY = 4)   :386            ← HANDOFF B (RecordBatches)
   │  ├─ EgressCredit::new(budget)                    :389            per-stream in-flight byte-credit
   │  │                                                               pool (#2821); composes with the
   │  │                                                               batch-count capacity
   │  ├─ tokio::spawn_blocking ──────────────────────────────► CONTEXT 2 (below)
   │  └─ returns: ReceiverStream → MeteredDoGetStream → encode_do_get :594
   │             (FlightDataEncoderBuilder wraps the batch stream)
   └─ admitted_stream(stream, permit)                 :938            permit held for the stream's
                                                                      lifetime (RAII release)

━━ CONTEXT 2: blocking-pool thread — "the producer" (lives for the whole scan) ━━
run_merge_catching_panics                            streaming.rs:464
└─ MergeProducer::produce_streaming_from_readers     producer_warm.rs:45
   ├─ prune_readers                                  :185             token-prune via already-parsed
   │                                                                  endpoint tokens — zero I/O warm
   ├─ FORK 1 — point read (#2207): point_read_keys()
   │     → build_single_partition_merger_from_readers → drive_merge_over
   ├─ FORK 2 — single-SSTable BYPASS (#3058):
   │  ├─ bypass_reason(readers, schema, CQLITE_FLIGHT_MERGE_PATH, …)  bypass.rs:204
   │  │     conjunctive, fail-closed predicate (#28); `merge` forces the
   │  │     slow arm, `bypass` never overrides a correctness precondition
   │  ├─ ScanRowSource::open(reader, schema, token_bound, now_secs)   bypass.rs:384
   │  │  └─ SSTableReader::open_query_row_stream ────────────► CONTEXT 3 (below)
   │  └─ drive_row_source                            producer_stream.rs:219
   │        pull rows ← QueryRowStream → limit/filter →
   │        build RecordBatch (8192 rows / byte cap, #2600) →
   │        ChannelSink::emit → credit.reserve() → tx.blocking_send   ← backpressure parks HERE
   └─ FORK 3 — multi-generation MERGE (the pre-#3058 arm; ~3.3× the cyc/row):
      KWayMerger::new_from_readers(…).with_now_secs()                 cross-generation reconcile;
      → drive_merge_over → StreamingMerger → drive_merge_streaming    per-input producer threads

━━ CONTEXT 3 (bypass arm only): std thread "cqlite-query-rows" ━━━━━━━━━━━━━━
SSTableReader::open_query_row_stream    summary_scan/query_rows.rs:300  (cqlite-core)
├─ sync_channel(QUERY_ROWS_CHANNEL_BATCHES)          :307             ← HANDOFF A (row batches)
└─ thread::spawn + current-thread tokio runtime      :322
   └─ drive_query_rows(reader, schema, token_bound, now_secs, …)
        Summary-guided walk (#2412): Summary.db intervals → Index.db →
        Data.db chunk decompress → cell decode under PartitionShadow
        read-shadowing (#1741: partition/range/row/cell tombstones, TTL
        expiry, static injection — all at the pinned now_secs)
        → QueryRowMsg batches into the sync_channel; EXACTLY ONE
        terminal Done/Failed on every exit path (panic included, #3106)

━━ back on CONTEXT 1: the response stream, polled by tonic ━━━━━━━━━━━━━━━━━━
ReceiverStream::poll_next → MeteredDoGetStream                        rows/bytes accounting, credit
   → FlightDataEncoder (RecordBatch → Arrow IPC FlightData frames)    release, cancel race (#2680),
   → tonic/h2 → socket                                                merge abort handle (#2264)
```

## Variants

- **Aggregate ticket** (`count(*)`-class, #944): `do_get_resolve` returns
  `DoGetInput::Aggregate(paths)` and `do_get_inner` takes
  `build_aggregate_response` (`streaming.rs:520`) — bounded one-row-per-group
  materialization on the blocking pool, served as a stream; no client `stream` phase.
- **Point read** (#2207): a pushed full-PK-equality predicate routes to the
  single-partition merger over the same warm readers (FORK 1), streamed row-granularly
  (#2423).
- **Cold path** (`MergeInput::Paths`): retained as the byte-identity regression oracle in
  `streaming_tests.rs`; production row reads take the warm-reader path (#2310).

## Cancellation and failure

One `CancelFlag` spans everything: a client disconnect drops the response stream →
drop-guard trips the flag → the producer's cancellation-aware send unparks (#2264), the
core walk's `CancelBridge` stops the partition walk mid-stream, and the admission permit
releases via RAII. Setup-phase aborts are reason-stamped from typed error variants
(#2681), never inferred from the gRPC code.

## Why this document exists

The three-context / two-handoff structure is the mechanism behind the measured
concurrency ceiling (#3100 Part B: aggregate throughput on a pinned core peaks at N=2 and
declines through N=16 while the device sits idle). When reasoning about scheduler cost,
egress backpressure, or where a profile's off-CPU time attaches, start from this map.
