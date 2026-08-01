# Proposal: A repo-owned gate notification contract (issue #3119)

**Milestone:** maintenance (agent-team automation / delivery pipeline) · **Priority:** P1 ·
**Routing:** design-driven (delivery automation; no external on-disk oracle — the fix is a new
repo-owned surface plus bootstrap provisioning) · **Issue:** #3119 ·
**Related:** #2667 (introduced the gate push signal), #2910 (`required` aggregation), #2140 (golden
AMI — the pin decided here must land in the AMI path), #1930/#2640 (one worker per box).

## Why

`scripts/agent-gate.sh:3585` fires the gate's only push signal as:

```bash
agent-notify --category "$category" "$title" "$body" >/dev/null 2>&1 || true
```

The installed `agent-notify` **v1.1.0** has **no `--category` arm** in its flag `case`
(`/usr/local/bin/agent-notify.bak.20260730:22-120` — the arms are `--version|-v|-V`, `--help|-h`,
`--test*`, `--setup*`, `--update`, and nothing else), so the call falls through to **manual
title/message mode** (`:729-732`, `title="${1:-Codex}"`, `msg="${2:-Task finished}"`). Measured
against the pristine upstream binary with a `curl`-capture shim on PATH:

```
CURL_ARGV: [-s] [-X] [POST] [-H] [Content-Type: application/json] [-d]
  [{"topic": "gate-test-topic", "title": "--category", "message": "error",
    "priority": 3, "tags": ["white_check_mark"]}] [https://ntfy.sh/gate-test-topic]
```

Three independent defects are visible in that single line:

1. **The `--category` flag is swallowed.** `title` becomes the literal `--category`, `message`
   becomes the category VALUE (`error`), and the real `gate FAIL <branch>@<sha>` /
   `RESULT: FAIL — failing: fmt,clippy` are dropped as surplus positional args.
2. **A red gate pages as a routine success.** Manual mode pins `sound_category="completion"`
   (`:514`), so the ntfy `priority_map`/`tag_map` (`:1429-1430`) emit **priority 3** and a green
   **`white_check_mark`** for a FAIL. The phone shows a routine success for a broken gate — the exact
   inversion the signal exists to prevent.
3. **The payload is POSTed to the TOPIC URL, not the server root** (`:1454-1457`). A JSON body
   carrying `"topic"` must be published to the ntfy SERVER ROOT; POSTed to `/<topic>` ntfy treats the
   body as **literal message text**, so the phone renders a serialized JSON blob. The two branches of
   that `if/else` are byte-identical despite the comment claiming special ntfy handling.

`scripts/local/worker-supervisor.sh:404` makes the **same** call (`"$NOTIFY_CMD" --category
"$category" "$title" "$message"`), so every supervisor page on the fleet carries the same corruption,
and `.claude/skills/flow-board/SKILL.md:216` documents the broken shape as the way to page the owner.

**Why the existing test could never catch it.** `scripts/tests/test_agent_gate_notify.sh:46-53` drives
`gate_push_signal` against a PATH-stubbed `agent-notify` that itself implements a `--category` arm:

```bash
if [ "$1" = "--category" ]; then cat="$2"; shift 2; fi
```

The mock encodes the **same wrong assumption as the caller**, so the test asserts the call shape the
gate produces and can never observe whether the real binary accepts it. It is an interface-mismatch
blind spot of exactly the class CLAUDE.md names for symmetric round-trips: both sides make the
identical mistake, the test closes, and the real channel is wrong.

**Fleet-durability is the third problem.** `agent-notify` is an out-of-band per-machine dependency at
`/usr/local/bin/agent-notify` (upstream `paultendo/agent-notify`, `--update` pulls
`releases/latest`). It is **not installed, pinned, verified, or even mentioned** by
`scripts/bootstrap-agent-machine.sh`. Both defects were hand-patched on ONE box (the pristine copy is
preserved at `/usr/local/bin/agent-notify.bak.20260730`; the patch adds a `--category` arm, a
`CODEX_NOTIFY_CATEGORY` override of `sound_category`, and a server-root POST). That patch propagates
to nothing, and `agent-notify --update` reverts it. **The repo must not depend on a hand-patched
binary on one machine.**

## What Changes

1. **A repo-owned notify wrapper, `scripts/lib/gate-notify.sh`, OWNS the payload contract.** It
   builds the ntfy JSON itself (topic, title, message, priority, tags) and publishes it with `curl`
   to the ntfy **server ROOT**, with the topic in the body. The payload is no longer constructed by
   an external binary the repo cannot pin, so AC1–3 hold deterministically on any machine —
   including one running the **pristine** upstream `agent-notify`.
2. **`gate_push_signal` routes through the wrapper** instead of calling `agent-notify --category`.
   The gate keeps building the same `title`/`body` from the same verified tree identity
   (`scripts/agent-gate.sh:6863-6870`); only the delivery mechanism changes.
3. **Severity is delivered, not merely intended.** PASS ⇒ priority 3 + `white_check_mark`;
   FAIL ⇒ priority 5 + `rotating_light` (the upstream ntfy maps, so the fleet's notifications look
   unchanged when they are correct). A red gate is distinguishable at a glance.
4. **`agent-notify` is demoted to an OPTIONAL local desktop/sound adjunct**, invoked positionally
   (`agent-notify "$title" "$body"` — never `--category`) with its webhook env **neutralized** so its
   broken publish path can never double-deliver a JSON-as-message notification. Absent ⇒ nothing lost.
5. **Advisory is enforced, not asserted.** Every failure mode of the notify path — absent wrapper,
   absent `curl`, unset webhook, non-zero exit, a helper that **rejects its arguments**, a hung
   publish — is bounded and swallowed. `test_agent_gate_notify.sh` gains the missing rejects-all-args
   and non-executable cases.
6. **A real-payload contract test**, `scripts/tests/test_gate_notify_contract.sh`, asserts the
   **published payload** (local receiver / `curl`-capture shim) rather than argv, with a hard
   regression guard that a `message` beginning with `{` FAILs, and a case proving the pristine
   upstream binary on PATH cannot corrupt or duplicate the delivery. Wired into the gate.
7. **Bootstrap provisions the channel and records the pin.** A new
   `scripts/bootstrap-agent-machine.sh` section verifies the **capability** (`curl`, `python3`, a
   resolvable webhook target, and the wrapper's own self-test publishing a correct PASS *and* FAIL
   payload to a capture shim) — never merely that a file exists — and records what it pinned: the
   wrapper's in-repo contract version plus the observed optional `agent-notify --version`.
8. **`worker-supervisor.sh` and the doctrine surfaces migrate to the wrapper**, so no repo script,
   skill, or doc prescribes the broken `--category` shape (AC6, pending Open Decision 2).

## Non-goals

- **Not forking or vendoring `agent-notify`.** It is an external upstream script we do not control;
  a vendored copy would be a ~1500-line fork to maintain, and a *pristine* copy still has both
  defects. The wrapper is ~100 lines against a stable published API.
- **Not fixing upstream.** An upstream PR (add `--category`; POST JSON to the server root) is a
  worthwhile follow-up; the fleet needs a correct channel now, and the wrapper stays correct after
  any upstream fix lands.
- **Not a new notification channel or transport.** ntfy over `curl` only; no Slack/Discord/Telegram
  presets, no new topics, no secrets introduced.
- **Not a gate component and not part of the verdict.** The notify path emits no SUMMARY line and can
  never change `RESULT:` or the exit status.
- **No Rust, no library surface, no on-disk format work.** `cqlite-core`, the bindings, the CLI, the
  no-heuristics decode path and the <128MB budget are untouched.
- **Out of scope:** the `claude-code` roborev agent OAuth expiry.

## Impact

- **New:** `scripts/lib/gate-notify.sh` (the payload contract + publisher),
  `scripts/tests/test_gate_notify_contract.sh` (real-payload contract test).
- **Changed:** `scripts/agent-gate.sh` (`gate_push_signal`, ~line 3577-3587),
  `scripts/bootstrap-agent-machine.sh` (new notify-channel section + summary pin line),
  `scripts/local/worker-supervisor.sh` (`notify()`, ~line 396-405),
  `scripts/tests/test_agent_gate_notify.sh` (advisory cases),
  `scripts/tests/test_bootstrap_agent_machine.sh` (pure-check assertions for the new section).
- **Gate:** the contract test joins the `tooling-tests` component set (hermetic, no network, seconds).
- **Docs (ship in this change):** `website/src/content/docs/agents-developing/delivery-pipeline.md`
  (lines 86-90 describe the push signal), `.claude/skills/flow-board/SKILL.md:215-217` (prescribes the
  broken `--category` call), `docs/development/fleet-runbook.md` (the notify dep on a worker box).
  Publication accepted by grepping the served page for a new distinctive phrase, never an HTTP 200.
- **#2140 golden AMI:** the AMI needs no notify **binary** pin at all after this change — only
  `curl`, `python3`, and the webhook env (`/etc/environment` on the current fleet). The pinned
  contract travels in git.
- **Untouched:** `cqlite-core`, `cqlite-cli`, `cqlite-flight`, bindings, `test-data`, all Cargo
  manifests, every CI workflow.
