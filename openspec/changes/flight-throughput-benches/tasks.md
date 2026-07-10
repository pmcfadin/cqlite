# Tasks — flight-throughput-benches (AD5, #1494)

> No export/Flight production-code changes. Additive bench + budget-test + gate-wiring + docs only.
> Reuse the epic-H dhat machinery and the existing perf-gate mechanism — do not duplicate either.

## 1. Baseline capture (measurement first)
- [ ] 1.1 With `CQLITE_DATASETS_ROOT` set to fetched canonical datasets, measure current-`main`
      figures for CQL→Arrow conversion, json/csv/parquet + delta export, Flight `do_get` throughput,
      and producer/converter dhat allocation counts+bytes. Record them for the README baseline table.
- [ ] 1.2 Note explicitly that these figures already include the merged #1495 (PR #2312) arrow-convert
      win — this is the post-#1495 `main` floor, the reference #1496 / AB / AE measure against.

## 2. Export + conversion micro-benches (STRICT, cqlite-core)
- [ ] 2.1 Add a Criterion bench for `export::arrow_convert::rows_to_record_batch` over a wide-row /
      type-heavy pinned fixture (per-cell conversion throughput). Assert at setup ≥ 1 row (panic on 0).
- [ ] 2.2 Add Criterion benches for json/csv/parquet export writers and delta export over pinned
      fixtures (feature-gated: `parquet`, `delta-scan`). Setup asserts the public export entry ran and
      produced output.
- [ ] 2.3 Register the `[[bench]]` targets in `cqlite-core/Cargo.toml` (`harness = false`).

## 3. End-to-end Flight do_get throughput bench (ADVISORY, cqlite-flight)
- [ ] 3.1 Add `criterion` as a dev-dependency + a `[[bench]]` target `flight_do_get` to
      `cqlite-flight/Cargo.toml` (`publish = false`).
- [ ] 3.2 Implement the bench driving the **public** tonic `FlightService::do_get` over the in-process
      transport (reuse the `cqlite-flight/tests/do_get_transport_test.rs` harness). Setup asserts the
      stream yields ≥ 1 record batch (panic on 0) — wiring evidence for the public RPC surface.

## 4. Allocation / peak-memory budget guards (dhat, reuse epic-H infra)
- [ ] 4.1 Add a converter allocation-budget test (dhat `#[global_allocator]`, own test binary, epic-H
      pattern) asserting per-cell alloc count/bytes for `rows_to_record_batch` within the documented
      current-main bound. Non-vacuous: fail on 0 rows AND on 0 observed allocations. Wire under the
      `work-counters-guard` / `byte-budget-guard` gate component features.
- [ ] 4.2 Add a Flight-producer peak-memory budget test (dhat-heap) asserting the producer's peak/total
      within bound; non-vacuous (fail on empty/zero). Wire under the `memory-budget` component feature.
- [ ] 4.3 Confirm both land **passing** as baseline locks (current-main figures + headroom). The
      aggressive AB/AE target bounds are the consumer issues' tests, not this change.

## 5. Perf-gate wiring
- [ ] 5.1 `cqlite-core/benches/perf-gate.json`: add STRICT entries (`threshold_pct >= 10`) for the
      conversion + export micro-benches, each with a `_note` citing #1494/AD5.
- [ ] 5.2 `perf-gate.json`: add the Flight `do_get` throughput bench id to `advisory_benches` (reported,
      never fails — runtime/transport dominated, `write/ingest_wal_on` precedent).
- [ ] 5.3 `.github/workflows/perf-regression.yml`: add the new core `--bench` targets to BOTH the PR and
      `main` `cargo bench` invocations, each guarded by the "target may not exist on `main` yet"
      conditional so the first landing SKIPs green. Add a `cargo bench -p cqlite-flight --bench
      flight_do_get` arm (same guard) so the advisory Flight data is present.

## 6. Baseline artifact + docs
- [ ] 6.1 `cqlite-core/benches/README.md`: add an "export/Flight perf" subsection with the current-main
      baseline table (from 1.1), the #1495-provenance note (from 1.2), STRICT vs ADVISORY classification,
      and the drift-free refresh procedure (base re-measured each run; retune = edit json + README).
- [ ] 6.2 Keep CLAUDE.md / website doctrine untouched unless a workflow-visible change requires it
      (this change adds no user-facing behavior).

## 7. Validation
- [ ] 7.1 All new benches compile + run against fetched datasets (`CQLITE_DATASETS_ROOT`); paste the
      baseline numbers in the PR.
- [ ] 7.2 Budget guards green under their gate-component features; demonstrate non-vacuity (point a
      guard at an empty fixture → it FAILs, not passes).
- [ ] 7.3 Red-run 1 (STRICT gate bites): artificially slow the converter on a scratch branch, re-measure,
      show `check_perf_regression.py` reds (non-zero exit) naming the conversion bench. Discard the scratch.
- [ ] 7.4 Red-run 2 (budget bites): inflate a producer allocation on a scratch branch, show the dhat
      guard FAILs. Discard the scratch.
- [ ] 7.5 `scripts/agent-gate.sh` PASS — paste the AGENT-GATE SUMMARY block verbatim (the `memory-budget`
      / `work-counters-guard` / `byte-budget-guard` components exercise the new guards).
- [ ] 7.6 `RUSTFLAGS="-D warnings"` clean; no `unwrap()`/`expect()` in library code (bench/test code
      keeps the existing dhat-test style).
