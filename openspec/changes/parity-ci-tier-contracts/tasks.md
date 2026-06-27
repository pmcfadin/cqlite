# Tasks: parity-ci-tier-contracts

## 1. Tier contract document
- [ ] 1.1 Author `docs/development/parity-ci-tiers.md` with one section per tier (`fast_pr`, `required_parity`, `nightly_docker`, `exhaustive_regeneration`, `manual_debug`) covering purpose, allowed `evidence.type`, skip policy, failure policy, artifact-retention expectations, and promotion rules. *Surface exercised:* the doc itself (read by the cross-check in §3).
- [ ] 1.2 Add the gate-strength classification (smoke / canonical-semantic / byte-for-byte) and map each to `evidence.type` values; state that smoke alone cannot satisfy a P0 data-loss scenario without a recorded gap.
- [ ] 1.3 Embed a single strictly-formatted, machine-parseable tier list (table or fenced block) that the cross-check reads as "the documented enum" (design D2).

## 2. Release checklist document
- [ ] 2.1 Author `docs/development/parity-release-checklist.md` with explicit check items: manifest lint green, `required_parity` green on the release commit, recent `nightly_docker` pass, recent `exhaustive_regeneration` pass for RCs, and a no-unqualified-"same-tests-as-Cassandra"-claims check.
- [ ] 2.2 Link the Cassandra test index (`docs/cassandra_test_index.md`), the assessment report (`docs/reports/cassandra-test-parity-assessment.md`), and the generated parity report.

## 3. Doc ↔ enum cross-check (CI surface)
- [ ] 3.1 Add a `cassandra-parity` subcommand (e.g. `tier-contract-check`) that parses the documented enum from `parity-ci-tiers.md` and asserts it equals `enums::CI_TIER` and the manifest schema enum; exits non-zero with the specific divergent tier on mismatch. *Surface exercised:* `cargo run -p cassandra-parity -- tier-contract-check`.
- [ ] 3.2 Validate that every `ci.tier` used in `test-data/cassandra-parity-manifest.yml` is in the documented enum; report offending scenario ID + tier value on failure.
- [ ] 3.3 Unit tests: one passing fixture (doc/schema/code agree) and ≥1 drifted fixture per failure mode (doc-vs-code drift, unknown manifest tier). No Docker/datasets/live Cassandra.

## 4. CI wiring
- [ ] 4.1 Add a fast-PR workflow step (extend an existing fast-PR job or add a lightweight one) invoking `tier-contract-check`; confirm it needs no Docker, datasets, or live Cassandra.

## 5. Doctrine + docs cross-link
- [ ] 5.1 Cross-link the tier contract from `CLAUDE.md` and the website `agents-developing/` gate-contract page (same-change doctrine rule).

## 6. Quality gate (definition of done)
- [ ] 6.1 Run `scripts/agent-gate.sh` (with `CQLITE_DATASETS_ROOT` → main repo `test-data/datasets`) and paste the AGENT-GATE SUMMARY block verbatim — must PASS.
- [ ] 6.2 Intent audit **C**: run `spec-auditor` anchored to `openspec/changes/parity-ci-tier-contracts/specs/**` — every requirement `satisfied` with public-surface evidence (the doc, the `tier-contract-check` tests, the CI step). Must report PASS.
- [ ] 6.3 roborev: `/roborev-review-branch --base origin/main` clean (run `/roborev-fix` for findings).
- [ ] 6.4 Push branch, open PR linking #1022; do NOT merge (owner's Seam 2).
