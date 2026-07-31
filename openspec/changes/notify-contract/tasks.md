# Tasks: notify-contract (issue #3119)

> Design decided in `design.md`: option **(b)** — a repo-owned wrapper (`scripts/lib/gate-notify.sh`)
> OWNS the ntfy payload and publishes it to the SERVER ROOT; the external `agent-notify` is demoted to
> an optional local desktop/sound adjunct and is never in the payload chain. Option (a)
> (capability-probe `--category`) is eliminated by AC2+AC3 — the severity map and the topic-URL POST
> live inside the binary. Option (c) (vendor/pin upstream) is eliminated by AC6 plus the cost of a
> ~1500-line fork. AC→requirement map is at the top of `specs/gate-notify-contract/spec.md`.

## 1. The repo-owned wrapper (surface: `scripts/lib/gate-notify.sh`)
- [ ] Create the wrapper exposing `gate_notify_publish <severity> <title> <body>` (sourceable, and
      directly executable for the self-test), with a declared contract version constant.
- [ ] Resolve the target from `CQLITE_NOTIFY_WEBHOOK`, else `CODEX_NOTIFY_WEBHOOK` (the fleet's
      `/etc/environment` variable), with `CQLITE_NOTIFY_TOPIC`/`CODEX_NOTIFY_NTFY_TOPIC` as the
      explicit topic override.
- [ ] Split a topic URL into server ROOT + topic: strip query and fragment, strip trailing slashes,
      strip the trailing topic segment; a URL that is already a bare root requires an explicit topic
      override. No resolvable topic ⇒ silent no-op (never a guessed topic).
- [ ] Build the payload with `python3 -c` + `json.dumps` (never hand-quoted shell interpolation), and
      pin the severity map: PASS/`completion` ⇒ priority 3 + `white_check_mark`; FAIL/`error` ⇒
      priority 5 + `rotating_light`.
- [ ] Publish with `curl -sS -X POST -H 'Content-Type: application/json' --max-time <N> -d <body> <root>`,
      output discarded, failure swallowed.
- [ ] Optional local adjunct: when `agent-notify` is on PATH, invoke it POSITIONALLY
      (`agent-notify "$title" "$body"`) under a `timeout` with `CODEX_NOTIFY_WEBHOOK=`/
      `CQLITE_NOTIFY_WEBHOOK=` NEUTRALIZED so it can never double-publish. Never pass `--category`.
- [ ] Advisory containment: no `set -e` reliance, no `exit`, no traps, every call `|| true`, always
      `return 0`; `CQLITE_NOTIFY_DEBUG=1` adds diagnostics WITHOUT changing behaviour.
- [ ] Add `--self-test`: publish a PASS and a FAIL payload through a capture shim and validate both
      against the contract (title, body, distinct priority+tag, message-not-JSON); exit non-zero only
      in self-test mode, never in publish mode.

## 2. Gate wiring (surface: `scripts/agent-gate.sh`)
- [ ] Replace the `agent-notify --category …` call in `gate_push_signal()` (~line 3585) with a
      defensive source of `scripts/lib/gate-notify.sh` + `gate_notify_publish`; a missing/unreadable
      wrapper stays a silent no-op returning 0.
- [ ] Keep the existing call-site guard (FULL gate only — not `--lite`/`--delta`/`--only`/selftest) and
      the #2926 identity derivation (`TREE_COMMIT_LINE`) untouched.
- [ ] Keep the summary-write-failure ⇒ FAIL severity behaviour (`SUMMARY_WRITE_FAILED`).
- [ ] Verify the extraction contract `test_agent_gate_notify.sh` relies on still holds (the function
      must remain self-contained enough to be extracted, or the test's extraction updated in step 4).
- [ ] Confirm no SUMMARY line, component, or exit path is added by the notify change.

## 3. Contract test (surface: `scripts/tests/test_gate_notify_contract.sh`)
- [ ] Capture at the transport boundary with a PATH `curl` shim recording argv (target URL + `-d`
      body) — the technique that measured the defect; no network, no real topic.
- [ ] Drive the REAL `gate_push_signal` → REAL `gate-notify.sh` chain; stub nothing that produces the
      payload.
- [ ] Assert the PASS payload: `title == gate PASS <branch>@<sha>`, `message` starts `RESULT: PASS`,
      `priority == 3`, `tags == ["white_check_mark"]`, POST target is the server ROOT.
- [ ] Assert the FAIL payload: `title == gate FAIL <branch>@<sha>`,
      `message == RESULT: FAIL — failing: fmt,clippy`, `priority == 5`, `tags == ["rotating_light"]`.
- [ ] Assert PASS vs FAIL differ in BOTH `priority` and `tags`, and that FAIL carries neither
      priority 3 nor `white_check_mark`.
- [ ] Regression guard: FAIL the test when the published `message` begins with `{` (defect 3).
- [ ] Pristine-binary case: place a stand-in with the pristine upstream behaviour (no `--category`
      arm, manual `$1`/`$2` mode, topic-URL publish) on PATH and assert the published payload is still
      correct and EXACTLY ONE notification is published.
- [ ] Register the test in `scripts/agent-gate.sh`'s `tooling-tests` component set (hermetic + fast;
      also acceptable in the `--lite` shell-tooling set).

## 4. Advisory regression cases (surface: `scripts/tests/test_agent_gate_notify.sh`)
- [ ] Add a **rejects-all-arguments** stub (exits 2 with a usage error on any argv) and assert
      `gate_push_signal` returns 0 with no stdout/stderr — the class that produced this issue.
- [ ] Add a **non-executable** notifier case.
- [ ] Add a **non-zero exit** notifier case (today only absent + a recording stub are covered).
- [ ] Add a **missing repo-owned wrapper** case (wrapper path absent ⇒ no-op, returns 0).
- [ ] Add a **no notify target / no `curl`** case.
- [ ] Update the file's header comment: it now tests the ADVISORY contract; payload fidelity is
      `test_gate_notify_contract.sh`'s job, and an argv assertion is explicitly not evidence for it.

## 5. Bootstrap provisioning (surface: `scripts/bootstrap-agent-machine.sh`)
- [ ] Add a "Notification channel (ntfy)" section following the existing `have`/`ok`/`warn`/
      `run_or_print` idiom: check `curl` and `python3`, check a resolvable notify target (naming
      `/etc/environment` and printing the exact export when absent).
- [ ] Capability assert (the mold-link-probe analogue): run `gate-notify.sh --self-test` and report
      `ok` ONLY when both the PASS and the FAIL payload validate; a present-but-broken path is a
      `warn`, never `ok`.
- [ ] Record the pin: print the wrapper's contract version, and `agent-notify --version` when present,
      labelled OPTIONAL local adjunct with no version requirement.
- [ ] Keep bootstrap informational — `exit 0` always; default (no `--yes`) mode installs nothing and
      only prints commands.
- [ ] Extend `scripts/tests/test_bootstrap_agent_machine.sh` with pure-check assertions for the new
      section (present, no-install in default mode, self-test invoked, pin line emitted).

## 6. Call-site migration (AC6) — pending Open Decision 2
- [ ] `scripts/local/worker-supervisor.sh` `notify()` (~line 396-405): route through
      `gate_notify_publish`, dropping `--category`; preserve the existing no-op warning behaviour and
      the #2666 exit-latency ceiling asserted by `test_worker_supervisor.sh`.
- [ ] `.claude/skills/flow-board/SKILL.md:215-217`: replace the `agent-notify --category error …`
      prescription with the wrapper.
- [ ] Grep the repo for `agent-notify --category` and confirm zero remaining occurrences.

## 7. Doctrine (ships in this change)
- [ ] `website/src/content/docs/agents-developing/delivery-pipeline.md` (lines 86-90): the push signal
      is repo-owned, names the PASS/FAIL severity contract, and states that `agent-notify` is an
      optional local adjunct.
- [ ] `docs/development/fleet-runbook.md`: the notify channel's env configuration + what bootstrap
      verifies and pins.
- [ ] Accept publication by grepping the SERVED page for a new distinctive phrase (never an HTTP 200;
      CDN staleness ≈3 min).

## 8. Certification
- [ ] `--lite` green each fix round (summary-file redirect).
- [ ] `rust-reviewer` (shell diff — scope review to the shell surfaces) + `roborev` via
      `scripts/flow/roborev-review.sh --agent <a> --model <m> --repo <abs>` on the lite-green diff,
      BEFORE the full gate.
- [ ] ONE full gate inside `flow-closer`; verify `RESULT:` is `PASS|FAIL` and `tree-integrity:` is
      clean.
- [ ] `spec-auditor` C pass against `specs/gate-notify-contract/spec.md`.
- [ ] Live acceptance on this box: restore `/usr/local/bin/agent-notify` from
      `agent-notify.bak.20260730` and confirm a real PASS and a real FAIL page correctly
      (distinct priority/tag, no raw JSON) — this is the AC6 end-to-end evidence.
