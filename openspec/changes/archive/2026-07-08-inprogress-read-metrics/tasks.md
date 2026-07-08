# Tasks — In-progress read-path metrics (#2162)

One issue ↔ branch `issue-2162-inprogress-metrics` ↔ this change ↔ one PR. Each stage names the
surface it exercises and carries a red-then-green test (fails on `main`). Anchors are `main`-relative
and WILL drift — the implementer re-greps before editing. Follow the implement loop: `--lite`
(summary-file redirect) each fix round → rust-reviewer + roborev on the lite-green diff (review-first)
→ open PR → hand the endgame to `flow-closer` (ONE full gate → C intent audit → final roborev →
merge-on-green → finalize).

## Stage 0 — catalog surface + guards (write tests FIRST, must fail on main)
- [x] 0.1 Add `attr::RPC_PHASE = "cqlite.rpc.phase"` and `RPC_PHASE_DURATION = "cqlite.rpc.phase.duration"`
  to `cqlite-core/src/observability/catalog.rs`; add to `ALL_METRICS`; extend the namespaced-key +
  unique-name catalog unit tests to cover them. (read-progress-observability)
- [x] 0.2 Update the catalog doc comments for `RPC_ROWS`, `RPC_BYTES`, `QUERY_ROWS_SCANNED`, `READ_ROWS`,
  `READ_PARTITIONS` to state "emitted incrementally during a long-running scan (issue #2162)".
  (read-progress-observability)

## Stage 1 — incremental streaming progress (Flight, public surface)
- [x] 1.1 Move `cqlite.rpc.rows` / `cqlite.rpc.bytes` emission from `RpcMetrics::finish`
  (`cqlite-flight/src/obs.rs:145`) to a per-batch counter delta in the `Poll::Ready(Some(Ok(batch)))`
  arm of `MeteredDoGetStream::poll_next` (`cqlite-flight/src/streaming.rs:370`); `finish` no longer
  re-adds the accumulated totals (avoid double counting). (read-progress-observability)
- [x] 1.2 Red-then-green: extend `streaming_tests.rs` with a slow-consumer test asserting `MetricsCapture`
  sees `cqlite.rpc.rows` non-zero and `< total` while the stream is undrained and `cqlite.rpc.in_flight`
  is non-zero; and a drain-to-completion test asserting the summed total is byte-identical to the
  pre-change single emission. (read-progress-observability)

## Stage 2 — bounded per-do_get phase breakdown (Flight, public surface)
- [x] 2.1 Introduce a bounded phase enum (`resolve | merge_setup | stream`) with a fixed `&'static str`
  slot table (mirror the `RPC_METHODS` pattern in `obs.rs`); record `RPC_PHASE_DURATION` + a span event
  at each transition, driven from `do_get_inner`/`do_get_setup` (`cqlite-flight/src/service.rs:391/468`)
  and the `merge_setup`→`stream` boundary in `spawn_streaming` / `producer.rs`. (read-progress-observability)
- [x] 2.2 Red-then-green: `capture_spans`/`MetricsCapture` test asserting a `merge_setup`-tagged
  `cqlite.rpc.phase.duration` sample (and span event under `flight.do_get`) is recorded before the
  terminal batch; a bounded-attribute test asserting every phase value ∈ the closed set and no
  ticket/key/query attribute is present; an ordering test (`resolve`→`merge_setup`→`stream`, no
  fabricated zero for a skipped phase). (read-progress-observability)

## Stage 3 — incremental core scan counters
- [x] 3.1 **As implemented**: added a named `SCAN_PROGRESS_ROWS` threshold const (aligned to batch size)
  in the new `cqlite-flight/src/scan_progress.rs` and flush `cqlite.query.rows_scanned` (and
  read.rows/read.partitions on the merge scan) as deltas at the threshold in the Flight k-way-merge scan
  loop (`producer.rs::drive_merge`, via `ScanProgressMeter`), with a `Drop`-guaranteed final remainder
  flush covering every exit (completion, LIMIT break, cancel, error, panic). Flight `do_get` drives
  `KWayMerger` directly and never runs through `select_executor`/`execute.rs:343` — that site's
  pre-existing single-shot emission is left as-is (Scenario 3.2 anchors the public Flight-merge surface,
  and an incremental version there is materially riskier given LIMIT-pushdown's `scan_rows` rebaseline
  logic in `limit_pushdown/mod.rs`, for no scenario-required benefit). Added the feature-independent
  `ScanProgress` progress-observation seam (`Relaxed`-atomic delta-flush counter, analogous to
  `StreamProbe`) so the increment count is testable without depending on OTel exporter aggregation.
  (read-progress-observability)
- [x] 3.2 Red-then-green: through the public Flight full-scan merge surface, assert the progress seam
  records ≥2 delta flushes over a threshold-crossing scan (main records ZERO — the seam is net-new for
  this path) and the summed deltas equal the total; a `MetricsCapture` test (own integration-test binary,
  `cqlite-flight/tests/metrics_capture_test.rs`) asserting the incremental total equals the single-shot
  total and the `access_path` attribute set is unchanged. (read-progress-observability)

## Stage 4 — cross-cutting invariants
- [x] 4.1 Test: no new unbounded attribute — collect all metrics from a streaming `do_get` + core scan
  and assert every attribute key/value is bounded (existing `catalog::attr` or the new `cqlite.rpc.phase`).
  (read-progress-observability)
- [x] 4.2 Test/verify: feature-off build (`--no-default-features` path that excludes `observability`)
  compiles the new emission as no-ops and links no OpenTelemetry; confirm no new env var / CLI flag /
  ticket field / public method was added. (read-progress-observability)
- [x] 4.3 If any behaviour is user-facing, update CLAUDE.md + the `agents-developing/` observability
  note in the same change (keep doctrine current).

## Stage 5 — endgame (flow-closer)
- [ ] 5.1 `--lite` green on the full diff (summary-file redirect); rust-reviewer + roborev on the
  lite-green diff (review-first); fix rounds re-run `--lite` + diff-scoped targets.
- [ ] 5.2 Open PR; hand to `flow-closer`: ONE full `scripts/agent-gate.sh` (the run of record) →
  spec-auditor **C** intent audit anchored to `specs/read-progress-observability/spec.md` → final
  roborev → merge-on-green (`gh pr merge --squash --delete-branch`) → `flow-finalize` (archive change,
  close #2162, telemetry stamp).
