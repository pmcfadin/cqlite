# Tasks: nightly-docker-parity-lane

## 1. Lane runner script
- [ ] 1.1 Author `test-data/scripts/nightly-docker-parity.sh` that runs the whole lane with one command: live read-back (reuse `e2e-cassandra-readback.sh`), BTI `da` parity (reuse `gen-wide-bti.sh` + `issue_911_bti_sstabledump_parity`), differential compaction (drive `compaction-parity/` via the existing bootstrap+gradle), and the Bloom legs. It MUST classify each leg hard-fail vs. advisory and emit a per-scenario result + a Bloom FPR summary. *Surface exercised:* `bash test-data/scripts/nightly-docker-parity.sh`.
- [ ] 1.2 Pin Cassandra to the corpus version (5.0.2 / `CASSANDRA_REF: cassandra-5.0.2`) by reusing the existing compose stack + `bootstrap-cassandra.sh`; do NOT introduce a second version pin. *Surface exercised:* the script's reuse of `test-data/docker/docker-compose-cassandra5.yml` and `compaction-parity/scripts/bootstrap-cassandra.sh`.

## 2. Bloom FPR leg
- [ ] 2.1 Wire the no-false-negative gate (reuse `filter_db_strict_parameters_and_no_false_negative` in `cqlite-core/tests/sstable_parity_filter_db_test.rs`) as an ALWAYS hard-fail leg. *Surface exercised:* `cargo test -p cqlite-core --features write-support --test sstable_parity_filter_db_test filter_db_strict_parameters_and_no_false_negative`.
- [ ] 2.2 Wire the statistical FPR check (`filter_db_statistical_false_positive_rate_slow`, `CQLITE_FILTER_FPR_SLOW=1`) to report measured FPR vs. configured `bloom_filter_fp_chance`; gating policy per owner fork F4 (default advisory). *Surface exercised:* the same test target under `CQLITE_FILTER_FPR_SLOW=1`; the lane's FPR summary section.

## 3. Two-class gating + report
- [ ] 3.1 Implement the hard-fail vs. advisory classification in the runner so the workflow fails iff a hard-fail leg fails; advisory legs run `continue-on-error`. *Surface exercised:* the runner's exit code + the workflow step outcomes.
- [ ] 3.2 Emit the nightly report (GitHub step summary + uploaded artifact) with a per-leg table (class + outcome) and a Bloom FPR summary. *Surface exercised:* `$GITHUB_STEP_SUMMARY` + the uploaded report artifact.

## 4. Scheduled workflow
- [ ] 4.1 Author `.github/workflows/nightly-docker-parity.yml`: `on: schedule` (06:11 UTC, off the existing nightly rush per design D1) + `workflow_dispatch`; invoke the runner; upload failure artifacts (Cassandra logs, CQLite logs, fixture metadata, per-scenario JSONL diffs, Bloom FPR summary, reproduction commands) with ≥ 30-day retention. *Surface exercised:* the workflow file.
- [ ] 4.2 Confirm the lane is non-blocking for in-flight PRs (scheduled/dispatch only) and matches the tier contract's `nightly_docker` skip/failure/retention policy. *Surface exercised:* the workflow triggers + retention config.

## 5. Manifest tier wiring + report
- [ ] 5.1 Set `ci.workflow` to the new lane for the `nightly_docker`-tier scenarios it backs (`cass.write_load_path.live_readback.semantic_only`, `cass.bti_big_version_matrix.bti_da_write_read`, `cass.compaction.*` byte tier) in `test-data/cassandra-parity-manifest.yml`. *Surface exercised:* the manifest entries.
- [ ] 5.2 Promote `cass.filter_db.statistical_false_positive_rate` from `manual_debug` into `nightly_docker` with the agreed gating posture (F4) and point its `ci.workflow` at the lane. *Surface exercised:* the manifest scenario.
- [ ] 5.3 Regenerate `docs/reports/cassandra-test-parity.md` (`cargo run -p cassandra-parity -- report`) and confirm `cassandra-parity lint`, `tier-contract-check`, and `report --check` all exit 0. *Surface exercised:* `cargo run -p cassandra-parity -- lint && … tier-contract-check && … report --check`.

## 6. Quality gate (definition of done)
- [ ] 6.1 Run `scripts/agent-gate.sh` (with `CQLITE_DATASETS_ROOT` → the main repo `test-data/datasets`) and paste the AGENT-GATE SUMMARY block verbatim — must PASS.
- [ ] 6.2 Intent audit **C**: run `spec-auditor` anchored to `openspec/changes/nightly-docker-parity-lane/specs/**` — every requirement `satisfied` with public-surface evidence (the workflow file, the runner script, the Bloom test targets, the manifest wiring + regenerated report). Must report PASS.
- [ ] 6.3 roborev: `roborev review --branch --base origin/main --agent codex --wait` clean (run `/roborev-fix` for findings).
- [ ] 6.4 Push branch, open PR linking #1025; do NOT merge (owner's Seam 2).
