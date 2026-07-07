# Streaming do_get — eliminate the materialize-then-emit wall (issue #1476, AB1)

## Why

Flight `do_get` runs the entire k-way merge to completion and collects **every**
`RecordBatch` into a `Vec` before the first byte reaches the client
(`cqlite-flight/src/service.rs:405-409`, `producer.rs` `merge_paths`/`drive_merge`).
Server memory is O(split result) per call × N concurrent consumers — the <128MB
budget is unbounded here — and time-to-first-row equals time-to-last-row.

The 2026-07-07 round-2 lab run (epic #2103, finding #2157) hit this wall live:
`count(*)` and full scans produced **zero bytes for minutes** (235s idle-timeout
disconnects upstream; 26-minute `do_get` root spans in Tempo) while
`cqlite_rpc_in_flight_ratio` climbed and nothing completed.

Pre-activation fact-finding (2026-07-07, recorded on #2157) narrowed the scope:

- The core k-way merge is **already streaming** — construction is O(#SSTables),
  a bounded 256-entry channel feeds each input, and `drive_merge` steps one
  partition at a time. The eager part is ONLY the `Vec<RecordBatch>` collection
  seam between producer and gRPC stream.
- The #2135 LIMIT early-stop **already works** (empirical: limit=5 → 5 merge
  steps, ~13ms). This change does NOT claim to fix LIMIT latency; the lab's
  24-min LIMIT 5 is attributed to image skew (dev image predating #2135) and is
  re-tested independently.
- Predicate pushdown's `filterJson=Optional.empty` gap is **#2164** (connector,
  separate oracle-driven bug), not this change.

- **Milestone:** M7 / export-path performance. Epic AB #1467 child AB1 (#1476).
  **Design-driven** — owner decision 2026-07-01 (capstone #5): full streaming
  rewrite GO immediately, AA3 cancellation machinery (#1473, landed) rides it.
- **Creates capability** `flight-streaming-egress`.

## What Changes

- **Additive streaming producer surface.** `MergeProducer` gains a streaming
  produce method that sends each `RecordBatch` into a bounded channel as the
  merge yields it, instead of pushing into a `Vec`. The existing `Vec`-returning
  `produce`/`produce_cancellable` remain (aggregate path + tests + parity oracle).
- **`do_get` streams through a bounded channel.** `service.rs` replaces the
  `spawn_blocking → Vec` block with a bounded `tokio::sync::mpsc::channel(K)`:
  the merge runs on the blocking pool sending batches; the gRPC response wraps
  the receiver stream. First batches reach the wire while the merge is still
  running; peak resident payload is O(K · batch_size), not O(split result).
  Mirrors the proven `delta_scan/scan.rs` bounded-channel pattern.
- **Consumer-driven cancellation.** A dropped client (receiver dropped) makes the
  producer's send fail → the merge stops within a bounded number of steps,
  composing with the existing #1473 `CancelFlag` (polled before each step).
- **Metrics accounting moves to the stream.** rows/bytes attribution
  (`metrics.add_rows_bytes`, today summed after full materialization) accumulates
  per emitted batch and is recorded when the stream ends — including partial
  attribution for cancelled streams (what was actually emitted).
- **Aggregate path unchanged.** `aggregate_paths` output is one row per group —
  inherently small — and keeps materializing; its `Vec` is wrapped in a stream.

## Non-goals

- **LIMIT pushdown latency** — already delivered by #2135 and verified fast; the
  lab symptom is handled as image-skew retest on #2157.
- **Predicate pushdown** (`filterJson` TupleDomain gap) — issue #2164.
- **Split token-bound derivation from PK equality** — follow-on noted on #2164.
- **Incremental read-path observability** (#2162) — sequenced after this change;
  this change deliberately leaves the per-batch metrics seam it needs.
- **Streaming the aggregate path** — bounded output, no benefit.
- **Connector (Java) changes** — the Flight wire protocol is already a stream;
  Trino consumes incrementally today. No ticket/wire format change.
- **Core merge-engine changes** — the merge is already lazy; only the
  producer→gRPC seam changes.

## Doctrine impact

None to CLAUDE.md/site. `docs/reports/performance-audit-program-2026-07.md`
posture (decision #5) is being executed, not changed.
