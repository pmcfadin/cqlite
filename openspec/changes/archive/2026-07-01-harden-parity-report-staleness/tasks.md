# Tasks: harden-parity-report-staleness

> D1 (the self-heal mechanism) is the owner's approval decision. Tasks below assume the **recommended**
> D1-A (self-healing regeneration PR). If the owner picks D1-B (up-to-date enforcement) at approval,
> §3 is replaced by a branch-protection/merge-queue configuration task; §2 and §4–6 are unchanged.

## 1. `parity-report` agent-gate component (local/gate surface — D2)
- [ ] 1.1 Add a `parity-report` component to `scripts/agent-gate.sh` and to its `COMPONENTS` list, running `cargo run -p cassandra-parity -- report --manifest test-data/cassandra-parity-manifest.yml --output docs/reports/cassandra-test-parity.md --check`. *Surface exercised:* `scripts/agent-gate.sh --only parity-report` and `scripts/agent-gate.sh --list`.
- [ ] 1.2 Make it SKIP-aware (mirror `delivery-telemetry`/python components): SKIP when the `cassandra-parity` crate or the manifest is absent; FAIL (naming the report file) when present and stale; PASS when up to date.
- [ ] 1.3 Extend `scripts/tests/test_agent_gate_summary.sh` (or add a focused self-test) to cover the three outcomes: clean (PASS), stale manifest-without-regen (FAIL), tool-absent (SKIP). No Docker/dataset.

## 2. Self-healing detection logic (CI surface — D1-A, D3, D4)
- [ ] 2.1 In `.github/workflows/cassandra-parity.yml`, on the **push-to-`main`** trigger, when `report --check` reports stale, run `cassandra-parity report --output docs/reports/cassandra-test-parity.md` to regenerate, then drive the auto-PR step (§2.2). The PR trigger keeps `--check` as a plain failing gate (no auto-PR on PRs). *Surface exercised:* the workflow file.
- [ ] 2.2 Open-or-update a single regeneration PR from a fixed bot branch (e.g. `auto/parity-report-regen`) touching only `docs/reports/cassandra-test-parity.md`; if an open PR/branch already exists, update it instead of stacking duplicates (D4 idempotence). Use `GITHUB_TOKEN` with `permissions: { contents: write, pull-requests: write }`; never push to `main` directly.
- [ ] 2.3 Guard against recursion: the bot branch's own push/PR must not retrigger an auto-PR loop (e.g. skip the healing job when the head branch is `auto/parity-report-regen`, or when the actor is the bot). Verify the regen PR's merge makes `--check` green on the new tip (cycle terminates).

## 3. Wire + verify the healing job end-to-end (D1-A)
- [ ] 3.1 Confirm the push-to-`main` job has the needed `permissions:` and that a simulated stale state opens exactly one regen PR (validated via a dry-run/branch test or a documented manual trigger; do not require live Cassandra/Docker).
- [ ] 3.2 Confirm a non-stale push opens **no** PR (no-op path) and adds no noise.

## 4. Doctrine + docs (same-change rule)
- [ ] 4.1 Document the derived-artifact merge-race hazard and the chosen safeguard in `docs/development/parity-ci-tiers.md` (or the manifest doctrine page) and note the `parity-report` gate component + self-healing job in `CLAUDE.md`. Mirror to the `agents-developing/` site page if/when that section lands.

## 5. Manifest/report consistency
- [ ] 5.1 Ensure the change leaves `docs/reports/cassandra-test-parity.md` in sync with the manifest on the branch (regenerate if this change touches the manifest at all) so the branch's own `parity-manifest` check is green.

## 6. Quality gate (definition of done)
- [ ] 6.1 Run `scripts/agent-gate.sh` (with `CQLITE_DATASETS_ROOT` → main repo `test-data/datasets`) and paste the AGENT-GATE SUMMARY block verbatim — must PASS, and must show the new `parity-report` component.
- [ ] 6.2 Intent audit **C**: run `spec-auditor` anchored to `openspec/changes/harden-parity-report-staleness/specs/**` — every requirement `satisfied` with public-surface evidence (the gate component + its self-test, the workflow healing job, the doctrine note). Must report PASS.
- [ ] 6.3 roborev: `/roborev-review-branch --base origin/main` clean (run `/roborev-fix` for findings) — pay special attention to the GitHub Actions command-injection class (no untrusted `${{ }}` interpolated into `run:`; validate + pass via quoted env), since this change adds an auto-PR workflow step.
- [ ] 6.4 Push branch, open PR linking #1338; do NOT merge (owner's Seam 2 / autonomy model).
