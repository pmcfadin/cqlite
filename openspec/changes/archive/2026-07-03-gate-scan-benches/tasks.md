# Tasks — gate-scan-benches

## 1. TDD tests first (must red before the code exists)
- [ ] 1.1 Add fixture Criterion trees under `scripts/ci/tests/fixtures/`:
      `scaling_floor_pass` (`concurrent_scan/{buffered,mmap}/{n1,n4}/pr/estimates.json` with
      `median(n4)≈median(n1)` → scaling≈3.0) and `scaling_floor_fail` (`median(n4)≈4·median(n1)` →
      scaling≈1.0). Only `pr` baseline needed (scaling is intra-run).
- [ ] 1.2 Add tests to `scripts/ci/tests/test_check_perf_regression.py`: scaling PASS → exit 0;
      scaling FAIL → non-zero exit AND output names `concurrent_scan/*/n4`; missing-`n4` → SKIP (exit 0).
      These fail today (no `scaling_floors` support) — the red-run proof.

## 2. Gate machinery (additive)
- [ ] 2.1 `cqlite-core/benches/perf-gate.json`: add `scaling_floors` array (buffered + mmap n4/n1,
      `degree_ratio:4`, `min_scaling:1.8`, `_note`s); add `read_while_write/readers6_writers2` to
      `benches` with `threshold_pct:25` and a `_note` (p99 owned by A2 #1563).
- [ ] 2.2 `scripts/ci/check_perf_regression.py`: evaluate `scaling_floors` on the `new` baseline
      (`scaling = degree_ratio·median(baseline_id)/median(id)`); SKIP on missing data, FAIL when
      `scaling < min_scaling`; print a labeled section; widen the "nothing compared" guard to count
      scaling evaluations. `cfg.get("scaling_floors", [])` so legacy configs are unaffected.

## 3. Bench bit-rot fix (bench code only, no library change)
- [ ] 3.1 `cqlite-core/benches/read_while_write.rs`: make the writer loop do-while (ingest once before
      honoring `stop`) so `total_written ≥ WRITERS`; keep the correctness-floor assert. Do not change
      readers, sample count, or the measured metric.

## 4. Workflow wiring
- [ ] 4.1 `.github/workflows/perf-regression.yml`: add `--bench concurrent_scan --bench read_while_write`
      to BOTH the PR and main `cargo bench` invocations.

## 5. Docs
- [ ] 5.1 `cqlite-core/benches/README.md`: add a "concurrency scaling floor" subsection (formula, the
      1.8 floor + derivation, read_while_write median-gated with p99→A2 note); list both benches as gated.
- [ ] 5.2 Update the module docs of `concurrent_scan.rs` and `read_while_write.rs`: replace the "Not a CI
      gate (by design)" note with the scaling-floor / strict-median gating reality.

## 6. Validation
- [ ] 6.1 New Python tests green: `bindings/python/.venv/bin/pytest scripts/ci/tests/test_check_perf_regression.py -v`.
- [ ] 6.2 Both benches compile + run against fetched datasets (`CQLITE_DATASETS_ROOT`); record the
      current-main baseline scaling numbers in the PR.
- [ ] 6.3 Real red-run: wrap the scan hot path in a `Mutex` on a scratch branch, re-measure, show the
      scaling floor reds (non-zero exit). Paste in the PR. Discard the scratch change.
- [ ] 6.4 `scripts/agent-gate.sh` PASS — paste the AGENT-GATE SUMMARY block verbatim.
- [ ] 6.5 `RUSTFLAGS="-D warnings"` clean; no `unwrap()`/`expect()` in library code (bench code exempt but
      keep the existing style).
