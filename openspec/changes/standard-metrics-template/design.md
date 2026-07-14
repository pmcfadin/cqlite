# Design — round-N standard metrics template

## Context

The field-proposed 14-point standard (round-9 report on #2367) groups metrics by what they protect:

- **A. Correctness (pass/fail — the gate)** — 1 per-scenario completes+rows-correct (both snapshot +
  live); 2 row-count parity (Trino vs `nt tablestats`); 3 access path per query shape
  (`cqlite_read_partition_lookup_total{route,result}` + `rows_scanned` — PK-equality must be a bounded
  lookup, never full scan, #2207).
- **B. Hang / liveness (the R7/R8 class)** — 4 query wall time cold vs warm per shape; 5
  `cqlite_sstable_index_parses_total` delta per query (≤ #generations, flat on warm — the direct #2383
  re-parse-storm guard); 6 `cqlite_rpc_phase_active_ratio{phase}` standing/clearing during a scan; 7
  cancellation reclaim seconds (kill → CPU+mem baseline, no restart); 8 flight pod CPU+mem trajectory
  (flat vs unbounded — R7 leaked to 5.5GB).
- **C. Throughput / scale (tracked, non-blocking)** — 9 qps / rows-s / p50 / p99 / error-rate under a
  fixed loadtest (same query set each round); 10 cross-node work distribution (all N pods vs single-node
  bound — R9 was single-node-bound, #2397); 11 cold-parse cost (first-query-per-table wall +
  `index_parses`, the #2385 baseline).
- **D. Hygiene** — 12 snapshot cleanup (`nt listsnapshots | grep cqlite-` == 0 on all nodes); 13
  `cqlite_errors_total` by category (0, note lazy registration); 14 digest pin verified (every pod's
  imageID == the round's INDEX digest).

The field's own recommendation: "bake A/B into the pass/fail gate and report C/D as tracked numbers."
This design adopts that split verbatim.

## Decisions

### D1 — Artifact: canonical Markdown doc + a thin GitHub issue template (RECOMMENDED)

The canonical template lives as **`docs/development/round-validation-metrics.md`** — a diff-reviewable
doctrine doc that holds the 14 metrics, their GATE/TRACKED classification, the round-9 baseline, and the
local-mirror cross-links. A thin **`.github/ISSUE_TEMPLATE/round-tracker.yml`** seeds each new round
tracker with the checklist (the fill-in form) and links back to the doc for rationale + baseline.

- **Beat: doc-only.** Rounds would hand-copy the checklist into each tracker and drift — the exact
  failure the field flagged. An issue template makes "new round → pre-populated tracker" one click.
- **Beat: issue-template-only.** A `.yml` issue form cannot carry the rich baseline table, the
  GATE/TRACKED rationale, or the local-mirror wiring, and is not reviewable as doctrine. The doc is the
  source of truth; the form references it.
- **Beat: a machine-readable manifest (YAML/JSON) + generator.** Over-engineered for a per-round human
  report against a live cluster; the metrics are captured by a human operator, not a CI job. A
  manifest earns its keep only once the whole round is automated (out of scope).

### D2 — Classification is explicit and load-bearing

Every one of the 14 rows is tagged `GATE` (A1–A3, B4–B8) or `TRACKED` (C9–C11, D12–D14). A GATE item
failing means the round verdict is FAIL; a TRACKED item is a recorded number that never blocks the
verdict but must be present for comparability. D12 snapshot-leak and D14 digest-pin are the two
hygiene items that are **binary and cheap**, so although grouped under D they are also asserted (D12 via
the new local mirror; D14 by inspection) — the template marks them `TRACKED (binary)`.

### D3 — Round-9 baseline pre-filled

Each metric row carries its round-9 measured value (from the #2367 round-9 report) as the comparison
anchor, e.g. warm `LIMIT 5` 6.3–7.4s, cold 257s (#2385); `index_parses_total` 22 across the round,
flat on warm; cancellation reclaim ~15–60s no restart; throughput ~0.9 qps / p50 9.4s / p99 17.7s / 0
errors (8 threads × 180s); cross-node single-node-bound (#2397); digest `round9@sha256:4dfad858…`.
Where round-9 did not report a value (e.g. D12 snapshot-leak count), the row is marked `baseline: to
establish (round-10)`.

### D4 — Local mirrors: reference what exists, add exactly one

| Report item | Local mirror | This change |
|---|---|---|
| B5 index-parses-delta ≤ #generations, flat on warm | `cqlite-flight/tests/issue_2370_single_flight_test.rs`, `issue_2383_resolve_spin_test.rs`, `cqlite-core/tests/issue_2385_index_single_parse.rs` | **reference** (pins already exist) |
| B7 cancellation reclaim bounded | `cqlite-flight/tests/issue_2383_resolve_spin_test.rs` (cancel pins) | **reference** |
| A3 access path bounded (PK-eq never full scan) | `cqlite-flight/tests/point_read_route.rs`, `point_read_metrics_test.rs`, `metrics_capture_test.rs` | **reference** |
| C9 loadtest gating (nonzero exit on p99/error SLA breach) | `easy-db-lab-kits/trino-loadtest/driver.py` `--gate` mode | **delegate to #2377** (reference, do not build) |
| D12 snapshot leak == 0 after a run | testbed E2E (loadtest post-run / flight integration) | **ADD** (the one new mirror) |

The D12 mirror is the only new test surface: after the testbed E2E completes its query workload, assert
that no leaked `cqlite-`-prefixed snapshots remain (mirrors the field's `nt listsnapshots | grep cqlite-
== 0` hygiene check). The template must state which items are backed by a local mirror vs field-only, so
a reader knows exactly which regressions the repo already catches between rounds.

### D5 — Offer-back and doctrine cross-link

Post the finalized template as a comment on the round tracker (#2367) for round-10 adoption (the field
asked for it). Cross-link the template from `docs/development/pm-operating-loop.md` and the
validation-playbook so it is the discoverable round-reporting standard.

## Risks / trade-offs

- **The "round gate" is not the agent gate.** A/B pass/fail is a *field-validation* verdict against a
  live cluster, distinct from `scripts/agent-gate.sh`. The template states this explicitly to avoid
  conflation.
- **Baseline drift.** The pre-filled round-9 numbers are a point-in-time anchor; the template notes each
  round supersedes them (v0.14.1/round-10 already changes cold-parse). The baseline is comparison
  context, not a pass threshold.
- **D12 mirror surface.** The exact testbed E2E surface (a driver.py post-run assertion vs a flight
  integration test) is an implementation choice; the spec fixes the *behavior* (assert zero leaked
  snapshots) and leaves the surface to the implementer, with the loadtest post-run path as the
  recommended low-cost home.
