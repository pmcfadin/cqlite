# Tasks — standard-metrics-template

## 1. Canonical template doc
- [x] 1.1 Author `docs/development/round-validation-metrics.md`: 14 metrics A1–A3/B4–B8/C9–C11/D12–D14
      grouped A/B/C/D, each with field number + one-line "what it protects". (Requirement: canonical
      template)
- [x] 1.2 Tag every row `GATE` (A1–A3, B4–B8) or `TRACKED` (C9–C11, D12–D14); mark D12/D14 `TRACKED
      (binary)`. State the A/B round gate ≠ `scripts/agent-gate.sh`. (Requirement: GATE/TRACKED)
- [x] 1.3 Pre-fill the round-9 baseline column from the #2367 round-9 report; mark unreported metrics
      `baseline: to establish`; note baseline is comparison context, not a threshold. (Requirement:
      baseline)
- [x] 1.4 Add the local-mirror column: cross-link B5 (#2370/#2383/#2385 pins), B7 (#2383 cancel pins),
      A3 (route/rows-scanned pins); mark C9 delegated to #2377; mark D12 backed by the new E2E check;
      mark the rest field-only. (Requirement: local mirrors)

## 2. Round-tracker issue template
- [x] 2.1 Add `.github/ISSUE_TEMPLATE/round-tracker.yml` seeding the 14-point checklist (GATE
      checkboxes + TRACKED fields, grouped A/B/C/D) with a link to the canonical doc; no baseline
      duplicated. (Requirement: new round trackers seeded)

## 3. D12 snapshot-leak local mirror (the one new test surface)
- [x] 3.1 Add a post-workload snapshot-leak assertion to the testbed E2E (home:
      `easy-db-lab-kits/trino-loadtest/driver.py` `--snapshot-check-cmd`, post-run): FAIL if any
      `cqlite-`-prefixed snapshot remains; SKIPPED (never a silent pass) when unconfigured.
      (Requirement: D12 mirror)
- [x] 3.2 Cover the mirror with unit tests (`test_driver.py`: `find_leaked_snapshots`,
      `run_snapshot_leak_check`, CLI wiring, `make_default_list_snapshots_fn`) that a leaked snapshot
      fails, a clean run passes, and unconfigured reports not-ran. Surface exercised: `driver.py main()`
      wiring via the CLI flag → `run_snapshot_leak_check` → exit code 3 on leak. (Requirement: D12 mirror)
- [x] 3.3 (roborev round 1) A probe command that exits nonzero must FAIL, never a vacuous PASS from
      empty stdout — `make_default_list_snapshots_fn` now raises `RuntimeError` on nonzero exit;
      `main()` catches it and reports FAIL/exit 3. (Requirement: D12 mirror — probe-failure scenario)
- [x] 3.4 (roborev round 2) A blank/whitespace-only `--snapshot-check-cmd` shell-executes as a silent
      no-op (exit 0, empty output) and would also read as a false PASS — `validate_args` now rejects
      it up front. (Requirement: D12 mirror — blank-command scenario)

## 4. Offer-back + doctrine cross-links
- [x] 4.1 Cross-link the template from `docs/development/pm-operating-loop.md` and the
      validation-playbook page. (Requirement: offered back and discoverable)
- [ ] 4.2 Post the finalized template as a comment on #2367 for round-10 adoption — done at
      merge/finalize time (the requirement's scenario is keyed off "the merged change"). (Requirement:
      offered back)
- [x] 4.3 Keep doctrine current in the same change — the website `agents-developing/` validation-playbook
      page now links the template (task 4.1); no CLAUDE.md text change needed (pm-operating-loop.md,
      already linked from CLAUDE.md, carries the new cross-link).

## 5. Quality gates
- [ ] 5.1 `scripts/agent-gate.sh` (full) PASS — inside flow-closer (covers the new D12 test + docs).
- [ ] 5.2 C intent audit (spec-auditor) PASS — every requirement satisfied with evidence.
- [ ] 5.3 roborev clean on the branch diff.
