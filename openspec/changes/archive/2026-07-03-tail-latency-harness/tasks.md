# Tasks — tail-latency-harness

## 1. TDD tests first (must be green on current main, gate on ratios not absolutes)
- [ ] 1.1 `cqlite-core/tests/tail_latency_harness.rs`: pure unit tests (no dataset) for the percentile
      function (nearest-rank p50/p99/p999 on a known vector) and the gate-ratio/advisory logic.
- [ ] 1.2 Same file: self-assertion test — `p99_mixed <= K * p99_scan_free` with `K` a documented const
      chosen from the first measured run on `main` (record the measured convoy ratio in the PR).
      Skip-not-fail when the fixture binary is absent; panic-loud when present-but-0-rows.
- [ ] 1.3 Same file: determinism test — two consecutive scan-free runs' p50 agree within a documented
      wide tolerance; each run satisfies `p50 <= p99 <= p999`.
- [ ] 1.4 `scripts/ci/tests/test_check_tail_latency.py`: advisory breach → exit 0 (reported);
      enforce breach → exit 1; within-threshold → exit 0.

## 2. Harness implementation (additive; no read-path production changes)
- [ ] 2.1 `cqlite-core/benches/tail_latency/mod.rs` (shared module, `crate::fixtures`): percentile math
      (`TailStats { p50, p99, p999 }`), `HarnessReport` (mixed + scan_free + ratios), JSON serialize,
      ledger append; `run_point_read_stream(db, sql, n)`, `run_mixed_load(...)`, `run_scan_free(...)`.
      Background scan on a spawned thread with an `AtomicBool` stop flag over a shared `Arc<Database>`.
- [ ] 2.2 Setup guards: point read returns ≥1 row AND `access_path.is_targeted()` (`PartitionLookup`),
      else panic (wiring-evidence). Reuse A1's `uuid_to_literal` + the `SELECT id, name … WHERE id=<lit>`
      projected shape. Fixed warmup + measured `N` consts (no wall-clock-bounded loops).
- [ ] 2.3 `cqlite-core/benches/tail_latency.rs` (`harness = false` custom main): run mixed + scan-free,
      print JSON to stdout, append the ledger record. Skip-register (no measurement, clear message) when
      the fixture is absent. Register the bench in `cqlite-core/Cargo.toml` (`[[bench]] harness = false`).

## 3. Gate config + checker (advisory first)
- [ ] 3.1 `cqlite-core/benches/tail-latency-gate.json`: `advisory: true`, thresholds for
      `p99_over_p50` and `p99_mixed_over_scan_free`, plus a `_comment` documenting the flip-to-enforcing.
- [ ] 3.2 `scripts/ci/check_tail_latency.py <harness_json> <gate_json>`: report each ratio vs threshold;
      exit 0 while advisory; `--enforce` (or `advisory: false`) → non-zero on breach. Document the flip.
- [ ] 3.3 `.gitignore`: add `cqlite-core/benches/tail-latency-history.jsonl` (generated run data).

## 4. Docs
- [ ] 4.1 `cqlite-core/benches/README.md`: document the tail harness, its JSON/ledger, and the
      advisory-first tail gate + flip-to-enforcing. Note A5 ledger consolidation.

## 5. Validation
- [ ] 5.1 Run the harness locally against the fetched BIG fixture; record the measured "before"
      p99_mixed/p99_scan_free convoy ratio in the PR; set `K` and the gate thresholds from it.
- [ ] 5.2 `scripts/agent-gate.sh` PASS — paste the AGENT-GATE SUMMARY block in the PR.
- [ ] 5.3 `RUSTFLAGS="-D warnings"` clean; no `unwrap()`/`expect()` in library code.
