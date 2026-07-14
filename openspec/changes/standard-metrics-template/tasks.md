# Tasks — standard-metrics-template

## 1. Canonical template doc
- [ ] 1.1 Author `docs/development/round-validation-metrics.md`: 14 metrics A1–A3/B4–B8/C9–C11/D12–D14
      grouped A/B/C/D, each with field number + one-line "what it protects". (Requirement: canonical
      template)
- [ ] 1.2 Tag every row `GATE` (A1–A3, B4–B8) or `TRACKED` (C9–C11, D12–D14); mark D12/D14 `TRACKED
      (binary)`. State the A/B round gate ≠ `scripts/agent-gate.sh`. (Requirement: GATE/TRACKED)
- [ ] 1.3 Pre-fill the round-9 baseline column from the #2367 round-9 report; mark unreported metrics
      `baseline: to establish`; note baseline is comparison context, not a threshold. (Requirement:
      baseline)
- [ ] 1.4 Add the local-mirror column: cross-link B5 (#2370/#2383/#2385 pins), B7 (#2383 cancel pins),
      A3 (route/rows-scanned pins); mark C9 delegated to #2377; mark D12 backed by the new E2E check;
      mark the rest field-only. (Requirement: local mirrors)

## 2. Round-tracker issue template
- [ ] 2.1 Add `.github/ISSUE_TEMPLATE/round-tracker.yml` seeding the 14-point checklist (GATE
      checkboxes + TRACKED fields, grouped A/B/C/D) with a link to the canonical doc; no baseline
      duplicated. (Requirement: new round trackers seeded)

## 3. D12 snapshot-leak local mirror (the one new test surface)
- [ ] 3.1 Add a post-workload snapshot-leak assertion to the testbed E2E (recommended home:
      `easy-db-lab-kits/trino-loadtest/driver.py` post-run, or a flight integration test): FAIL if any
      `cqlite-`-prefixed snapshot remains; no vacuous pass when no queries ran. (Requirement: D12 mirror)
- [ ] 3.2 Cover the mirror with a unit/integration test that a leaked snapshot fails and a clean run
      passes. Surface exercised: the E2E snapshot-leak assertion. (Requirement: D12 mirror)

## 4. Offer-back + doctrine cross-links
- [ ] 4.1 Cross-link the template from `docs/development/pm-operating-loop.md` and the
      validation-playbook page. (Requirement: offered back and discoverable)
- [ ] 4.2 Post the finalized template as a comment on #2367 for round-10 adoption. (Requirement:
      offered back)
- [ ] 4.3 Keep doctrine current in the same change — update CLAUDE.md pointer + the website
      `agents-developing/` validation page if the round-reporting standard is referenced there.

## 5. Quality gates
- [ ] 5.1 `scripts/agent-gate.sh` (full) PASS — inside flow-closer (covers the new D12 test + docs).
- [ ] 5.2 C intent audit (spec-auditor) PASS — every requirement satisfied with evidence.
- [ ] 5.3 roborev clean on the branch diff.
