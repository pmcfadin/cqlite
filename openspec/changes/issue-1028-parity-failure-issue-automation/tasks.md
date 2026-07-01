## 1. Failure-summary artifact

- [x] 1.1 Define the `parity-failures.json` schema (`[{scenario_id, workflow, test_target, component_path,
      failure_class}]`); normalize `failure_class` to `VerifyErrorClass.code()` where applicable.
- [x] 1.2 Emit it from the parity lanes on failure (start with `compression-corruption-parity.yml`,
      `cql-type-parity.yml`, `tombstone-ttl-parity.yml`, `exhaustive-regeneration.yml`); upload as an artifact
      `if: always()`/`failure()`.

## 2. The automation workflow

- [x] 2.1 Add `.github/workflows/parity-failure-issue.yml` — `workflow_run` (completed) over the parity lanes
      + `workflow_dispatch`; `permissions: {issues: write, contents: read}`; board-sync token-guard idiom.
- [x] 2.2 Act only when `conclusion == failure` AND origin event ∈ {schedule, push(main), workflow_dispatch};
      skip pull_request.
- [x] 2.3 Download the failed run's `parity-failures.json` (degraded summary/log parse + surfaced notice if
      absent); compute the `v1|sha256(...)` fingerprint per failing scenario.
- [x] 2.4 Dedup: `gh issue list --label parity-failure --state open` → substring-match `<!-- PARITY-FAIL:<fp> -->`;
      update existing (dated comment + latest-run link) or create new (epic #974 + scenario ID + tier + artifact
      links + repro cmd + summary). Warn loudly if the list cap is hit.
- [x] 2.5 Green-run path: post a resolution comment on a tracked fingerprint's open issue; do NOT auto-close.
- [x] 2.6 Non-gating + fail-open: never change the parity result; token absent → notice + exit 0.

## 3. Fingerprint/dedup unit tests (smoke, wiring-evidence)

- [x] 3.1 Factor the fingerprint + dedup logic into a testable unit (script/action) accepting injected inputs
      (`--open-issues-json`-style, mirroring `delivery-telemetry.py`), so its behavior is tested without a live
      failing CI run. Tests: stable fingerprint across run-noise; distinct inputs → distinct fingerprints;
      update-not-duplicate on marker match; degraded-fallback notice; zero-parsed-on-failure surfaced.

## 4. Manifest + doctrine

- [x] 4.1 Add the 5 `cass.cli_reporting.failure_issue_*` scenarios (mirrored/tooling_only/smoke) modeled on
      `cass.cli_reporting.parity_manifest_lint_and_report`; regenerate `docs/reports/cassandra-test-parity.md`;
      `cassandra-parity` lint green.
- [x] 4.2 Add the implementation reference to `docs/development/parity-ci-tiers.md` (the nightly_docker
      "files/updates a tracking issue" line) + the agents-developing site if applicable.

## 5. Quality gates (definition of done)

- [ ] 5.1 `scripts/agent-gate.sh` PASS — paste the AGENT-GATE SUMMARY block verbatim.
- [ ] 5.2 Intent audit **C** (`spec-auditor` anchored to
      `openspec/changes/issue-1028-parity-failure-issue-automation/specs/**`) PASS.
- [ ] 5.3 roborev (`--agent codex --base origin/main`) clean.
- [x] 5.4 actionlint clean on the new workflow + any edited lanes.
- [ ] 5.5 PR opened; merge-on-green; then `flow-finalize` (archive change, cleanup, close #1028).
