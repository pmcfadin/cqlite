# Tasks: notify-contract (issue #3119)

> Design decided in `design.md`: option **(b)** — a repo-owned wrapper (`scripts/lib/gate-notify.sh`)
> OWNS the ntfy payload and publishes it to the SERVER ROOT; the external `agent-notify` is demoted to
> an optional local desktop/sound adjunct and is never in the payload chain. Option (a)
> (capability-probe `--category`) is eliminated by AC2+AC3 — the severity map and the topic-URL POST
> live inside the binary. Option (c) (vendor/pin upstream) is eliminated by AC6 plus the cost of a
> ~1500-line fork. AC→requirement map is at the top of `specs/gate-notify-contract/spec.md`.

## 1. The repo-owned wrapper (surface: `scripts/lib/gate-notify.sh`)
- [x] Create the wrapper exposing `gate_notify_publish <severity> <title> <body>` (sourceable, and
      directly executable for the self-test), with a declared contract version constant.
- [x] Resolve the target from `CQLITE_NOTIFY_WEBHOOK`, else `CODEX_NOTIFY_WEBHOOK` (the fleet's
      `/etc/environment` variable), with `CQLITE_NOTIFY_TOPIC`/`CODEX_NOTIFY_NTFY_TOPIC` as the
      explicit topic override.
- [x] Split a topic URL into server ROOT + topic: strip query and fragment, strip trailing slashes,
      strip the trailing topic segment; a URL that is already a bare root requires an explicit topic
      override. No resolvable topic ⇒ silent no-op (never a guessed topic).
- [x] Build the payload with `python3 -c` + `json.dumps` (never hand-quoted shell interpolation), and
      pin the severity map: PASS/`completion` ⇒ priority 3 + `white_check_mark`; FAIL/`error` ⇒
      priority 5 + `rotating_light`.
- [x] Publish with `curl -sS -X POST -H 'Content-Type: application/json' --max-time <N> -d <body> <root>`,
      output discarded, failure swallowed.
- [x] Optional local adjunct: when `agent-notify` is on PATH, invoke it POSITIONALLY
      (`agent-notify "$title" "$body"`) under a `timeout` with `CODEX_NOTIFY_WEBHOOK=`/
      `CQLITE_NOTIFY_WEBHOOK=` NEUTRALIZED so it can never double-publish. Never pass `--category`.
- [x] Advisory containment: no `set -e` reliance, no `exit`, no traps, every call `|| true`, always
      `return 0`; `CQLITE_NOTIFY_DEBUG=1` adds diagnostics WITHOUT changing behaviour.
- [x] Add `--self-test`: publish a PASS and a FAIL payload through a capture shim and validate both
      against the contract (title, body, distinct priority+tag, message-not-JSON); exit non-zero only
      in self-test mode, never in publish mode.

## 2. Gate wiring (surface: `scripts/agent-gate.sh`)
- [x] Replace the `agent-notify --category …` call in `gate_push_signal()` (~line 3585) with a
      defensive source of `scripts/lib/gate-notify.sh` + `gate_notify_publish`; a missing/unreadable
      wrapper stays a silent no-op returning 0.
- [x] Keep the existing call-site guard (FULL gate only — not `--lite`/`--delta`/`--only`/selftest) and
      the #2926 identity derivation (`TREE_COMMIT_LINE`) untouched.
- [x] Keep the summary-write-failure ⇒ FAIL severity behaviour (`SUMMARY_WRITE_FAILED`).
- [x] Verify the extraction contract `test_agent_gate_notify.sh` relies on still holds (the function
      must remain self-contained enough to be extracted, or the test's extraction updated in step 4).
- [x] Confirm no SUMMARY line, component, or exit path is added by the notify change.

## 3. Contract test (surface: `scripts/tests/test_gate_notify_contract.sh`)
- [x] Capture at the transport boundary with a PATH `curl` shim recording argv (target URL + `-d`
      body) — the technique that measured the defect; no network, no real topic.
- [x] Drive the REAL `gate_push_signal` → REAL `gate-notify.sh` chain; stub nothing that produces the
      payload.
- [x] Assert the PASS payload: `title == gate PASS <branch>@<sha>`, `message` starts `RESULT: PASS`,
      `priority == 3`, `tags == ["white_check_mark"]`, POST target is the server ROOT.
- [x] Assert the FAIL payload: `title == gate FAIL <branch>@<sha>`,
      `message == RESULT: FAIL — failing: fmt,clippy`, `priority == 5`, `tags == ["rotating_light"]`.
- [x] Assert PASS vs FAIL differ in BOTH `priority` and `tags`, and that FAIL carries neither
      priority 3 nor `white_check_mark`.
- [x] Regression guard: FAIL the test when the published `message` begins with `{` (defect 3).
- [x] Pristine-binary case: place a stand-in with the pristine upstream behaviour (no `--category`
      arm, manual `$1`/`$2` mode, topic-URL publish) on PATH and assert the published payload is still
      correct and EXACTLY ONE notification is published.
- [x] Register the test in `scripts/agent-gate.sh`'s `tooling-tests` component set (hermetic + fast;
      also acceptable in the `--lite` shell-tooling set).

## 4. Advisory regression cases (surface: `scripts/tests/test_agent_gate_notify.sh`)
- [x] Add a **rejects-all-arguments** stub (exits 2 with a usage error on any argv) and assert
      `gate_push_signal` returns 0 with no stdout/stderr — the class that produced this issue.
- [x] Add a **non-executable** notifier case.
- [x] Add a **non-zero exit** notifier case (today only absent + a recording stub are covered).
- [x] Add a **missing repo-owned wrapper** case (wrapper path absent ⇒ no-op, returns 0).
- [x] Add a **no notify target / no `curl`** case.
- [x] Update the file's header comment: it now tests the ADVISORY contract; payload fidelity is
      `test_gate_notify_contract.sh`'s job, and an argv assertion is explicitly not evidence for it.

## 5. Bootstrap provisioning (surface: `scripts/bootstrap-agent-machine.sh`)
- [x] Add a "Notification channel (ntfy)" section following the existing `have`/`ok`/`warn`/
      `run_or_print` idiom: check `curl` and `python3`, check a resolvable notify target (naming
      `/etc/environment` and printing the exact export when absent).
- [x] Capability assert (the mold-link-probe analogue): run `gate-notify.sh --self-test` and report
      `ok` ONLY when both the PASS and the FAIL payload validate; a present-but-broken path is a
      `warn`, never `ok`.
- [x] Record the pin: print the wrapper's contract version, and `agent-notify --version` when present,
      labelled OPTIONAL local adjunct with no version requirement.
- [x] Keep bootstrap informational — `exit 0` always; default (no `--yes`) mode installs nothing and
      only prints commands.
- [x] Extend `scripts/tests/test_bootstrap_agent_machine.sh` with pure-check assertions for the new
      section (present, no-install in default mode, self-test invoked, pin line emitted).

## 6. Call-site migration (AC6) — pending Open Decision 2
- [x] `scripts/local/worker-supervisor.sh` `notify()` (~line 396-405): route through
      `gate_notify_publish`, dropping `--category`; preserve the existing no-op warning behaviour and
      the #2666 exit-latency ceiling asserted by `test_worker_supervisor.sh`.
- [x] `.claude/skills/flow-board/SKILL.md:215-217`: replace the `agent-notify --category error …`
      prescription with the wrapper.
- [x] Grep the repo for `agent-notify --category` and confirm zero remaining occurrences.

## 7. Doctrine (ships in this change)
- [x] `website/src/content/docs/agents-developing/delivery-pipeline.md` (lines 86-90): the push signal
      is repo-owned, names the PASS/FAIL severity contract, and states that `agent-notify` is an
      optional local adjunct.
- [x] `docs/development/fleet-runbook.md`: the notify channel's env configuration + what bootstrap
      verifies and pins.
- [ ] Accept publication by grepping the SERVED page for a new distinctive phrase (never an HTTP 200;
      CDN staleness ≈3 min). **Deferred to post-merge by construction** — the site deploys from `main`,
      so the served page cannot carry this change's phrase until the PR lands. Grep for:
      `The payload contract is REPO-OWNED`.

## 8. Certification
- [x] `--lite` green each fix round (summary-file redirect).
- [ ] `rust-reviewer` (shell diff — scope review to the shell surfaces) + `roborev` via
      `scripts/flow/roborev-review.sh --agent <a> --model <m> --repo <abs>` on the lite-green diff,
      BEFORE the full gate.
- [ ] ONE full gate inside `flow-closer`; verify `RESULT:` is `PASS|FAIL` and `tree-integrity:` is
      clean.
- [ ] `spec-auditor` C pass against `specs/gate-notify-contract/spec.md`.
- [ ] Live acceptance on this box: restore `/usr/local/bin/agent-notify` from
      `agent-notify.bak.20260730` and confirm a real PASS and a real FAIL page correctly
      (distinct priority/tag, no raw JSON) — this is the AC6 end-to-end evidence.

## 9. Oracle hardening from the C intent audit (review round 5)
- [x] AC5/R8 blocker: assert bootstrap HONOURS the probe verdict — a broken wrapper is
      reported FAILED and never `verified` (the auditor's `; true;` + broken-wrapper
      mutation now reds the suite where it previously stayed 77 PASS / 0 FAIL).
- [x] Positive twin: a healthy wrapper is reported `notify capability verified`.
- [x] No-target case: warns, prints the exact `CODEX_NOTIFY_WEBHOOK=` export, exits 0
      (never exercised before — a fleet box's ambient var always took the other branch).
- [x] Cover `gate_notify_selftest`'s own validator: a vacuous validator now reds.
- [x] R7: cover the DEFAULT supervisor notify path (`NOTIFY_CMD` unset) — both the
      bare-`agent-notify` revert and a broken `--publish` arm now red.
- [x] Spec R1 S4 / R3 S3: cover the push-signal CALL SITE (stamped identity, and
      `SUMMARY_WRITE_FAILED` forcing FAIL) read-only, with no seam added to the gate.
- [x] Give EVERY timing-sensitive case an independent `timeout -s KILL` cap so an
      unbounded-call regression reds instead of hanging the whole gate.
- [x] Make case 3f discriminate probed-and-rejected from attempted-and-failed.
