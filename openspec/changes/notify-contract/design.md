# Design: repo-owned gate notification contract (issue #3119)

## Context

The full gate's push signal (#2667) is the fleet's only out-of-band "your gate finished" event: it
converts the summary file from a passive poll target into a push, so a backgrounded gate calls its
waiting closer back instead of being idle-polled. Its value is entirely in **fidelity** — a signal
that says PASS when the gate FAILed is worse than no signal, because a worker acts on it.

Measured on this box against the pristine upstream binary (`curl`-capture shim, so no network):

| binary | published body | POST target |
|---|---|---|
| `agent-notify.bak.20260730` (pristine v1.1.0) | `{"topic":"gate-test-topic","title":"--category","message":"error","priority":3,"tags":["white_check_mark"]}` | `https://ntfy.sh/gate-test-topic` |
| `agent-notify` (hand-patched, ONE box) | `{"topic":"gate-test-topic","title":"gate FAIL br@abc1234","message":"RESULT: FAIL — failing: fmt,clippy","priority":5,"tags":["rotating_light"]}` | `https://ntfy.sh/` |

The left column is what the entire fleet delivers today, for a **FAIL**.

## The decision, and the constraint that forces it

Three options were on the table. The forcing constraint is issue #3119's **AC6**: the contract test
must pass against the **pristine** upstream binary, which has BOTH defects. That eliminates any design
that leaves payload construction with upstream `agent-notify`:

| option | AC1 title/body | AC2 distinct FAIL severity | AC3 message ≠ JSON | AC6 pristine | bootstrap pin/verify (AC5) | testable w/o network | upstream-drift cost |
|---|---|---|---|---|---|---|---|
| **(a)** capability-probe `--category`, degrade | yes — dropping the flag gives correct positional title/body | **NO** — pristine manual mode hard-pins `sound_category="completion"` (`:514`); priority/tag are unreachable from the caller | **NO** — the topic-URL POST is inside the binary, unreachable from the caller | fails AC2+AC3 | nothing to pin; probe is a proxy, not a capability assert | argv only | probe rots on every upstream flag change |
| **(c)** vendor/pin the upstream script in-repo | only if we vendor a **patched fork** | same — a pristine vendored copy is still broken | same | **NO** by construction: a patched fork is not pristine | pin is easy (a git SHA) | yes | **high** — a ~1500-line fork of an actively-updated script, re-patched on every upstream release |
| **(b)** repo-owned wrapper owning the payload | **yes** | **yes** | **yes** | **yes** — upstream is not in the payload chain | capability self-test + in-repo contract version | yes (local receiver / `curl` shim) | **near zero** — ntfy's publish API is stable and versioned |

**Option (a) is eliminated by AC2 and AC3**, not by taste: two of the three defects live *inside* the
binary, past any caller-side probe. **Option (c) is eliminated by AC6** and by maintenance cost — the
only vendored copy that would pass AC1–3 is a fork, i.e. the same hand-patch, merely relocated into
git.

**Chosen: (b) — `scripts/lib/gate-notify.sh` owns the ntfy payload contract.** The payload is ~40
lines of JSON plus one `curl`; owning it is strictly cheaper than owning a fork, and it makes the
correctness of the fleet's paging channel a property of the repo (git-pinned, gate-tested) rather than
a property of a machine's install history.

## Shape

```
gate_push_signal()                     # scripts/agent-gate.sh — builds title/body, unchanged
  └─ gate_notify_publish <sev> <title> <body>     # scripts/lib/gate-notify.sh (sourced)
       ├─ resolve target: CQLITE_NOTIFY_WEBHOOK || CODEX_NOTIFY_WEBHOOK  (+ CODEX_NOTIFY_NTFY_TOPIC)
       │     split into  root = scheme://host/    and  topic = last path segment
       ├─ build JSON:  {topic, title, message, priority, tags}     (python3 json.dumps — never hand-quoted)
       ├─ POST the JSON to the ROOT (bounded: curl --max-time, output discarded)
       └─ OPTIONAL local adjunct: agent-notify "$title" "$body"   with the webhook env NEUTRALIZED
```

Severity map (identical to upstream's ntfy maps, so correct notifications look unchanged):

| severity | priority | tag |
|---|---|---|
| PASS / `completion` | 3 | `white_check_mark` |
| FAIL / `error` | 5 | `rotating_light` |

### Why the topic/root split lives in the wrapper

The fleet configures exactly one variable — `/etc/environment` on this box carries
`CODEX_NOTIFY_WEBHOOK=https://ntfy.sh/pmcfadin-cloud-100` (plus `CODEX_NOTIFY_AGENT_NAME`) and there
is no `CODEX_NOTIFY_WEBHOOK_PRESET`, no `~/.agent-notify.env`, and no `.codex-notify.env` in the repo.
So the wrapper must accept a **topic URL** and derive the root itself (strip query/fragment, strip the
trailing topic segment, keep the topic for the body). A URL that is already a bare server root is
addressed by `CODEX_NOTIFY_NTFY_TOPIC` (or an explicit `CQLITE_NOTIFY_TOPIC`), and a target from which
no topic can be resolved is a **silent no-op**, never a guessed topic — publishing to a guessed topic
pages a stranger.

### Advisory containment (AC4 is load-bearing)

`gate_push_signal` runs at final-SUMMARY time, after the verdict is computed but before the exit.
Anything it can do must be unable to change `RESULT:` or the exit status. Containment is structural,
not by convention:

- the wrapper is **sourced defensively** — a missing/unreadable `scripts/lib/gate-notify.sh` leaves
  `gate_push_signal` a no-op returning 0, exactly as an absent `agent-notify` does today;
- every external call is bounded (`curl --max-time`, the optional adjunct under `timeout`) so a
  black-holed network or a hung helper cannot stall the gate;
- all stdout/stderr is discarded, every invocation is `|| true`, and the function's last statement is
  `return 0` — `set -e`/`pipefail` in the caller cannot see anything;
- no `exit`, no `trap`, no writes to any gate variable, no writes to the summary file.

The failure catalogue the tests must cover is therefore: absent helper, non-executable helper,
non-zero exit, **rejects its arguments** (usage error — the class that produced this issue), unset
webhook, absent `curl`, absent `python3`, and a hung publish.

### Why the contract test must parse the published payload

The existing test's blind spot is precise: it stubs the **payload producer**. Any replacement that
keeps a stub in the payload-producing chain reproduces the same blind spot. So the contract test
exercises the real `gate_push_signal` → real `gate-notify.sh` → real JSON construction, and captures
at the **transport boundary**: a `curl`-capture shim on PATH (which is exactly how the defect above was
measured) or a loopback HTTP receiver. What is asserted is the **published bytes**: `title`,
`message`, `priority`, `tags`, the POST **target being the server root**, and — as a standing
regression guard for defect 3 — that `message` does not begin with `{`.

A loopback receiver was attempted first here and was unreliable under the agent sandbox; the
`curl`-shim capture is deterministic, network-free, and captures the POST **target** as well as the
body, which a receiver bound to one path cannot. Recommend the shim as primary, with the receiver
optional.

### AC1/AC2 wording divergence, recorded openly

AC1/AC2 say the delivery is "asserted against the **real** `agent-notify` on PATH, not a stub". Under
the chosen design `agent-notify` is **no longer the payload producer**, so asserting *its* output
would assert the wrong component. The requirements implement the AC's **intent** — *no stub anywhere
in the payload-producing chain; the assertion is on the real published payload* — and additionally
pin the property the literal wording was protecting: with the **pristine** upstream binary on PATH,
the delivered payload is still correct and **exactly one** notification is published (the adjunct
cannot double-publish a JSON-as-message). That is strictly stronger than asserting upstream's argv
handling, and it is what AC6 requires anyway.

### Bootstrap: capability, not existence (AC5)

The existing bootstrap idiom is `have <tool>` → `ok`/`warn` → `run_or_print <install cmd>`, with
verification that asserts the *capability* the gate depends on (e.g. mold is only wired after a link
**probe** passes, `:245-260`; roborev's *configured* agent is smoke-tested rather than assumed,
`:775-812`). The new section follows it exactly:

- `curl` and `python3` present (both `have`-checked; each missing is a `warn` + install hint);
- a notify target resolvable from the environment, naming `/etc/environment` as the fleet mechanism —
  absent is a `warn` with the exact export, never a failure;
- **the capability assert**: run the wrapper's own `--self-test`, which publishes a PASS **and** a
  FAIL payload against a capture shim and validates both against the contract (title, body, distinct
  priority/tag, message-not-JSON). This is the analogue of the mold link probe: nothing is declared
  healthy because a file exists;
- **the pin record**: print the wrapper's in-repo contract version and, when present, the observed
  `agent-notify --version` labelled as an OPTIONAL local adjunct with no version requirement. After
  this change the only pinned notify artifact is in git, which is what makes #2140's golden AMI
  path trivial.

Bootstrap stays informational (`exit 0` always) — a machine without a notify target is fully able to
do CQLite work.

## Risks

- **Fleet notifications look different if the severity map drifts.** Mitigated by reusing upstream's
  exact ntfy priority/tag maps and pinning them in the contract test.
- **Silent no-op hides a misconfigured box.** Mitigated by bootstrap's capability self-test being the
  place where a broken channel is loud, and by the wrapper's opt-in `CQLITE_NOTIFY_DEBUG` diagnostic
  that never changes the advisory behaviour.
- **A future upstream `agent-notify` gains `--category`.** No effect: the wrapper does not use it, and
  the adjunct call stays positional.
