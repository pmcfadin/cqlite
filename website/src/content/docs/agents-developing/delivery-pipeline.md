---
title: Delivery pipeline (flow-lead)
description: The manager-orchestrated delivery workflow — the flow-lead agent, the flow-* pipeline, the specialist roster, and the one standing human seam (spec approval; merge is autonomous on green).
sidebar:
  label: Delivery pipeline
---

CQLite delivery is driven by a **manager agent, `flow-lead`**, that orchestrates a team of specialist
agents through a defined pipeline. Start it as your session driver — `claude --agent flow-lead` (it is
the repo's default agent) — and it orients from the board. It orchestrates; the specialists do the
middle; you sit in one standing seat (spec approval — merge is autonomous on green).

## The pipeline

```
  flow-groom ─▶ flow-activate ─▶ flow-implement ─▶ flow-address ─▶ flow-finalize
   idea→issue    Seam 1:           team builds,      resolve PR       archive +
   (oracle vs    spec+design,      gate→C→roborev,   comments         cleanup +
    design)      STOP for you      open PR (no merge)                 close issue
                       ▲                                    ▲
                       │                                    │
                  flow-board surfaces the single next thing waiting on you
```

| Verb | What it does |
|------|--------------|
| `flow-groom` | rough idea → one scoped issue (one `P0`–`P3`, `status:ready`, testable criteria); decides oracle vs design |
| `flow-activate` | worktree + branch + `opsx:propose`; renders spec + design inline; **STOPS at Seam 1** |
| `flow-implement` | implement (TDD) → review-first (`rust-reviewer` + roborev on the lite-green diff) → open PR → spawn `flow-closer` for the endgame (full gate → **C** → final roborev → merge → finalize) |
| `flow-address` | resolves PR review comments; re-verifies; pushes; replies |
| `flow-finalize` | `opsx:archive` + **stamp the telemetry ledger** + remove worktree/branch + close issue (post-merge) |
| `flow-board` | status across in-flight work + drives the one item waiting on you |

## Oracle vs design (the routing decision)

- **Oracle-driven** (SSTable parsing, compaction/tombstone parity, type decode) — a Cassandra/sstabledump
  source of truth exists. Issue + a pinned parity test; **skip OpenSpec**; groom → implement.
- **Design-driven** (bindings/M6, query-engine surface, CLI/REPL UX, perf/M7, process) — no oracle.
  Goes through `flow-activate` (OpenSpec proposal/design/specs/tasks).

## The one standing human seam

1. **Spec approval** (Seam 1, in `flow-activate`) — you approve the OpenSpec spec + design before any
   implementation. The lead renders it inline and stops. **This is the only standing human gate.**

**Merge is autonomous by default** — not a standing seam. A worker's/closer's **terminal state** for an
issue is PR-open + `agent-gate.sh` PASS + C PASS (design-driven) + roborev clean; at that point it **arms
`gh pr merge --auto` and ends its turn**, and GitHub lands the PR on green (see
[Merge-on-green](#merge-on-green-no-ci-busy-wait)) — it does **not** poll the PR's own external CI in a
yield/wake loop. An owner merge decision exists only **conditionally**, when an escalate-and-hold trigger
fires: a genuine design-call roborev finding, a scope/product question, an unmet/uncovered requirement,
work outside the issue, or an explicit `HOLD:` order. *Always escalated, never decided by the lead:*
product decisions, scope/title changes, epic closes.

## Merge-on-green (no CI busy-wait)

Once a worker/closer reaches **local certification** (PR-open + gate PASS + C PASS + roborev clean) — and
after the pre-merge SHA assert + `HOLD` re-read — it **arms auto-merge and stops**. It must **not** busy-poll
its PR's own external CI — repeatedly waking (`ScheduleWakeup`) to watch the cross-platform matrix after the
work is done is pure token bleed and is prohibited. GitHub owns the green-wait:

```bash
gh pr merge <pr> --auto --squash --delete-branch
```

GitHub lands the PR the instant the branch's **`required`** status check passes and auto-closes the issue via
`Closes #N`. This is the single default path — there is **no manager-owned poller/merge-engine** (that
mechanism was never built; it is gone).

**Why `--auto` is safe (#2433):** `main` carries a real `required` status check + `enforce_admins=true` —
**not** an empty `contexts=[]` set (see [GitHub-enforced merge gate](#github-enforced-merge-gate-2433) below).
`--auto` therefore can never land a PR against an unchecked head, and there is no admin bypass. Branch
protection *is* the green-signal guard, machine-enforced by GitHub.

**Finalize crosses a possible session boundary (#2667):** because `--auto` can complete after the arming
session exits, `flow-finalize` runs on whichever wake observes the merge. When the `required` check is
already green at arm time, the closer confirms `state=MERGED` and finalizes in-session (the fast-path
default); otherwise it returns `verdict: auto-armed` and a later wake confirms `state=MERGED` before
finalizing. The [#2667 gate completion push-signal](#gate-completion-push-signal-2667) and GitHub's own
auto-merge notification are the callbacks — the gate summary file is a push signal now, not a poll target.
`ScheduleWakeup` remains valid for genuinely external, harness-untracked state — just not for polling a PR's
own CI after the work is complete.

### Gate completion push-signal (#2667)

The full `agent-gate.sh` fires **one advisory push** at final-SUMMARY write time (title
`gate <RESULT> <branch>@<sha>`, body = RESULT + any failing components), converting the summary file from a
passive poll target into a **push signal**: a backgrounded gate calls its waiting closer/worker back instead
of being idle-polled. `--lite`/`--delta`/`--only` are exempt (iteration aids, never the gate of record). It
is advisory by contract — an absent notifier, an unset target, a failing notifier, or one that **rejects its
arguments** is a silent no-op and the summary file remains the artifact of record, so a broken notifier never
changes the gate's verdict.

**The payload contract is REPO-OWNED (#3119).** `scripts/lib/gate-notify.sh` builds the ntfy JSON itself and
POSTs it to the ntfy **server root** (topic in the body). PASS publishes priority 3 + `white_check_mark`;
FAIL publishes priority 5 + `rotating_light` — **a red gate is distinguishable at a glance**. This is not
cosmetic: the gate previously called `agent-notify` with a `--category` flag that upstream v1.1.0 has no arm
for, so it fell through to manual title/message mode — the title became the literal flag name, the message
became the category value, the real title/body were dropped, and **every FAIL paged as a green priority-3
success**; its ntfy path also POSTed the JSON to the topic URL, so phones rendered a raw JSON blob. Two of
those defects live inside that binary, past any caller-side flag probe, which is why the payload now lives in
git. `agent-notify` remains only an **optional local desktop/sound adjunct**, invoked positionally (never
`--category`) with its webhook env neutralized so it cannot double-publish. Configure the target with
`CODEX_NOTIFY_WEBHOOK=https://ntfy.sh/<topic>` (the fleet uses `/etc/environment`);
`bash scripts/bootstrap-agent-machine.sh` verifies the **capability** via `gate-notify.sh --self-test` and
records the pinned contract version. Payload fidelity is pinned by
`scripts/tests/test_gate_notify_contract.sh` (gate component `tooling-tests`), which asserts the **published
bytes** at the transport boundary — an argv-level assertion is explicitly not evidence, since that is exactly
the blind spot the swallowed flag hid behind.

### GitHub-enforced merge gate (#2433)

`main` now carries **full branch protection**: the `required` status check (the "Required PR Gate" CI
workflow) is a required context, with `enforce_admins` on. Merge-on-green is therefore
**local gate PASS + C (design) + roborev clean *and* the GitHub `required` check green** — the last term
is machine-enforced, not honor-system. Because `enforce_admins` is enabled, even `gh pr merge --admin`
is refused while the check is pending or red (proven on probe PR #2441: plain and `--admin` merges both
rejected with `mergeStateStatus: BLOCKED`), so **there is no bypass**. A red that is a known flake gets
`gh run rerun --failed` — never an admin override. This is load-bearing: if branch-protection settings
ever regress (contexts emptied, `enforce_admins` disabled), this doctrine governs catching it.

### Closer merge protocol (#2456)

The `flow-closer` certifies a **specific SHA** — the tree the full gate of record and the final
roborev pass actually ran on. Three mechanical rules keep the merge honest:

- **Pre-merge SHA + gate-of-record assertion (#2456/#2668/#3465, scripted hard precondition).**
  Immediately before arming `gh pr merge --auto`, the closer does `git push`, then runs
  `scripts/flow/premerge-assert.sh <pr> <certified-sha> <gate-of-record-summary> [<delta-summary>]
  --c-verdict <path|AUTO>` — which asserts the PR is
  OPEN and its `headRefOid` **equals the locally-certified tip**, exiting non-zero (and printing a
  loud refusal) on a moved head, a closed/merged PR, or a gh failure.
  **The third argument is REQUIRED (#3465), and it closes TWO distinct escapes with one mechanism.**
  Verifying the head against a *claimed* certified sha never verified that a certified sha EXISTS.
  **#3408 = no gate at all**: it merged on 22 `--lite` PASSes and no full `scripts/agent-gate.sh` run,
  because nothing in the merge path ever asked for the gate of record. **PR #3616 = a real gate,
  someone else's**: a closer located its run dir by RECENCY (`ls -t /tmp/agent-gate.*`), read a PEER
  LANE's dir, saw 33 of 37 components PASS, and was about to merge #3616 on PR #3580's verdict — the
  count, the dir and the timestamps were all real, and only the `run-id:` line exposed it, read by a
  human. With 14000-27000 stale run dirs per box and up to 4 concurrent gates, recency picks a peer
  routinely. This script **cannot** verify `run-id:` (see below), so the `commit:`+`tree-start:`
  binding is what makes that class a mechanical refusal: a peer's summary carries the OTHER PR's
  branch head. It is now asked for here, at the one point every merge passes through: the summary file must
  hold exactly ONE whole-line-anchored `==== AGENT-GATE SUMMARY ====` block (`--lite`/`--delta` emit
  distinct headers and are refused by name; a second or unterminated block is ambiguous and also
  refused) with `RESULT: PASS` and `tree-integrity: PASS` compared **token-exactly** — `INCOMPLETE` is
  the launch-time liveness sentinel and not a verdict (#3041), and a mutated-mid-run tree is not a
  certification (#2926) — and with BOTH `commit:` (7 hex) and `tree-start:` (12 hex) prefix-matching
  the certified sha **at each value's own width**; a non-hex placeholder (`(not captured)`,
  `unverified`, `selftest`) REFUSES rather than being skipped. An OPTIONAL argument would have left the
  convention honour-system, so the pre-#3465 two-argument call is a loud usage failure.
  **What it does NOT do, stated rather than implied:** it cannot verify `run-id:` (the #2874 reader
  contract requires the party that LAUNCHED the run, which this script is not), and it cannot prove
  the summary came from a genuine gate rather than a hand-written file — a hostile invoker is out of
  the threat model, since whoever runs the script controls the process. What it closes is **accident
  and drift**, which is the observed failure mode. `dirty:` is reported in the success line **and
  enforced** (#3648): the gate-of-record block — and, in Case B, the delta block too — must read
  `dirty: no`, matched affirmatively, so an absent or unrecognised value REFUSES rather than being read
  as clean. A `dirty: yes` run certified the sha PLUS uncommitted tracked edits, which `commit:`/
  `tree-start:` cannot distinguish from the clean tree. No env opt-out exists and none may be added — a
  dirty tree is always re-gateable.
  **The OPTIONAL fourth argument is the only way a `--delta` re-cert can certify a merge.** #1892
  *mandates* `--delta` — "never a repeat full gate" — for a test/docs-only diff on top of a full PASS
  at anchor `X`, and mandates that the PR record BOTH blocks, so a 3-arg-only guard red on correct,
  doctrine-mandated input: the guard agents learn to waive. With four arguments the third is the
  ANCHOR's full PASS (its sha need NOT be the certified sha) and the fourth is exactly one
  `==== AGENT-GATE DELTA SUMMARY ====` block carrying `MODE: delta` (asserted affirmatively — the
  inverse of the full block's `MODE:` belt), `RESULT: PASS`, `tree-integrity: PASS`, a `delta-anchor:`
  naming exactly that anchor (an `(UNRESOLVED)` anchor refuses), and its OWN `commit:`/`tree-start:`
  at the certified sha. A block carrying `nested-under:` (#2874) is refused in either shape: a nested
  sub-gate runs at the SAME tree, so the sha binding provably cannot distinguish it.
  **`--c-verdict` IS REQUIRED AND HAS NO DEFAULT: OMITTING IT IS EXIT 3 (#3751).** The C intent audit
  was the one certification with no step in the merge path asking for it, so a design-routed change could
  merge on an audit that never reported — and a silent "C is not required" would reproduce, inside the
  enforcer, the exact defect the enforcer exists to close (the #3465 precedent, one argument over). It is
  a NAMED flag rather than a fifth positional so it composes with #3752's sibling required flag in EITHER
  landing order, and the missing-flag census names each absent flag independently, so its exit 3 does not
  depend on being the only required flag. **The routing is MEASURED FROM THE CERTIFIED TREE, never taken
  from the caller** — a caller-supplied *"C does not apply here"* is precisely the escape hatch this
  closes. `AUTO` asks git what THIS BRANCH does to `openspec/changes/`: the diff between
  merge-base(`origin/main`, `<certified>`) and `<certified>`, with `openspec/changes/archive/**` excluded
  (archiving is flow-finalize's work, not a routing signal). Non-empty ⇒ design-routed ⇒ **C REQUIRED**,
  and an absent or `NOT-RUN` verdict REFUSES the merge naming the stage and the cause; empty ⇒
  affirmatively `c-verdict: NOT-APPLICABLE (no openspec change on branch)`. **PURE DELETIONS ARE
  EXCLUDED TOO (`--diff-filter=d`, #3751 round 1).** Rename detection is pinned OFF deliberately, so
  a real `openspec archive` MOVE appears as a DELETION from `openspec/changes/<slug>/` plus an
  ADDITION under `archive/` — the addition is excluded, so counting the deletion made every
  archive-only finalize PR read design-routed and REFUSE for want of a C verdict: a false refusal on
  correct, doctrine-mandated input. A path that is ONLY deleted also contributes nothing to audit,
  since there is no spec delta at the certified tree for C to anchor to; every ADDED or MODIFIED
  **AND THE WHOLE C CHECK RUNS TWICE, BECAUSE A CHECK MUST BE INSIDE THE WINDOW IT CERTIFIES (#3751
  round 16).** It was validated ONCE near the top, and then the base-staleness advisory (bounded at
  65s) and the `gh pr view` round trip ran with NOTHING re-checking it — so a concurrent
  `review-stage.sh open --force` superseded the validated PASS and the script still certified
  (measured on the shipped artifact: `PREMERGE: OK b5f49d60aae4...` at exit 0, while
  `review-stage.sh verdict` read an instant later reported the FRESH generation). The remedy is
  roborev job 290's, verbatim — the ruling that governs the gate's own component-set pre-flight:
  **REPEAT the check inside the window and KEEP the earlier one**, the early call being what stops
  an uncertifiable run paying for the advisory and the network call at all. The repeat **RESETS**
  its captured observation, so the single-observation and generation bindings are taken AFRESH; a
  disagreement **REFUSES naming the field that moved**, never last-one-wins. **A repeat is not a
  comparison, and only the comparison catches the interesting case**: a supersede to a DIFFERENT
  generation that itself PASSES at the same head returns an accepting token from an audit this run
  never validated. **And a refusal's own prose may not reproduce the success marker** — the first
  draft said *"runs immediately before the OK line"*, spelled with the literal token, so a grep saw
  certification inside a refusal (#3312's rule, one directory over). **Residual, DECLARED: two
  checks cannot both be last** — the C window narrows to a local measurement and is NOT closed, and
  the `gh` head/state check is no longer the last thing before the success emit.
  path under a live change still routes to C. A plain LISTING of
  `openspec/changes/` cannot answer it — measured 2026-09-01, `origin/main` carries `archive` plus two
  sibling lanes' in-flight change directories, so every branch would read design-routed and the
  "measurement" would be vacuous — and the base is the **MERGE-BASE, never `origin/main`'s TIP** (#3392: a
  tip comparison reports another lane's newly-landed change as a difference of THIS branch and reds a
  correct oracle-driven PR). **AND THE PATHSPEC IS ROOT-ANCHORED — `:(top)openspec/changes/` —
  BECAUSE `diff.relative=false` DOES NOT DO THAT (#3751 round 11).** A bare pathspec is interpreted
  relative to the CALLER'S CWD, so run from a repository subdirectory the routing diff came back
  EMPTY, a genuinely design-routed branch measured `NOT-APPLICABLE`, and the merge PROCEEDED with no
  C verdict at all — reached by nothing more exotic than the working directory (measured:
  `PREMERGE: OK`, exit 0, from `cqlite-core/src/storage`, where the root invocation on the same
  repository, sha and argv refuses with `routing: REQUIRED`). `diff.relative` is a DIFFERENT AXIS —
  it governs the OUTPUT PATH PREFIX, not pathspec interpretation — so both are pinned and neither
  substitutes for the other: `:(top)` anchors what is SELECTED, `diff.relative=false` keeps what is
  PRINTED root-relative. Generalise it: **a pinned config option is a claim about ONE axis, and "cwd
  cannot change this answer" needs the axis your call actually uses.** **Any failure to measure — no git, no `origin/main`, the certified commit
  absent from this checkout — is `UNMEASURED` and is TREATED AS REQUIRED**: never derive a pass from the
  absence of a bad signal. There is deliberately NO spelling of the flag that means "not applicable": a
  supplied PATH can only carry a review-stage verdict token, so a file asserting `NOT-APPLICABLE` is
  refused as an unrecognised token, and inapplicability is reachable ONLY through AUTO's
  measurement. **THREE BINDINGS MAKE `AUTO` THE INTENDED FORM, AND EACH WAS ADDED AFTER A REVIEW
  FOUND IT ABSENT (#3751 rounds 1 and 3).** `AUTO` locates the stage in the CURRENT worktree, so the
  stage must be BOUND to the tree being merged: this worktree's `HEAD` must EQUAL the certified
  commit, else the merge REFUSES naming the divergence. On this fleet every lane is a worktree of
  ONE shared `.git`, so a PEER lane's certified commit RESOLVES from any lane — `rev-parse`,
  `merge-base` and the routing diff all succeed against a commit that has nothing to do with the
  `.review-stage/` records in *this* directory, which is #3616's peer-artifact class one directory
  over. **Resolvability is not provenance.** Rule 1 already asserts `headRefOid == certified`, so
  HEAD == certified binds the local artifact to THIS PR transitively, and correct input is
  unaffected (the closer pushes, then asserts, in the lane it just certified).
  **AND THAT BINDING IS STRUCTURALLY BLIND TO ONE ROUTE INTO THE SAME CLASS — A CAPTURED PATH IS NOT
  THE PATH (#3751 round 18).** Both tools resolved the worktree root with
  `root="$(git rev-parse --show-toplevel)"`, and a command substitution strips **every** trailing
  newline — so a checkout whose *directory name* ends in an LF resolved to a DIFFERENT, EXISTING
  SIBLING, and the captured value then carried no newline for round 17's representability refusal to
  see. Measured on the shipped scripts from `lanetrail<LF>/` beside a peer lane: `review-stage.sh
  verdict` reported `RESULT: PASS … report=…/lanetrail/.review-stage/issue-704/c.<nonce>.md` at
  **exit 0** off a report that lane never opened, a refused `open` created a directory *inside* the
  peer lane, and `AUTO` enumerated the same sibling's stage records. The HEAD binding cannot catch it
  because **HEAD is read in the CWD — the real lane, so it binds — while the ARTIFACT comes from the
  sibling.** The capture now keeps git's own framing (a sentinel appended INSIDE the substitution,
  then the sentinel, then EXACTLY ONE newline, git's terminator, and nothing else) and
  `premerge-assert.sh` goes further and **removes the channel**: its resolver ASSIGNS a shared global
  and prints nothing, so no call site can capture it. **The durable rule is about conclusions, not
  about newlines: a lossy-capture conclusion must be RE-DERIVED PER CONSUMER, never carried.** Round
  13 had enumerated trailing-newline stripping and declared it harmless — true of a report's
  per-line, column-zero-anchored CONTENT, false of a PATH, whose stripped bytes are its identity —
  and the unqualified conclusion is what left this reachable. The sweep that followed found the class
  a second time, in the substitution that locates `review-stage.sh`, i.e. the **enforcer** of the
  verdict this script refuses to merge without: 28 path-bearing or file-locating command
  substitutions examined, 3 affected. **Second binding: the stage RECORD's own `head-sha:` must equal the certified commit
  (#3751 round 3, G1).** The first binding closes the wrong-LANE axis and cannot see a STALE
  ARTIFACT, because the two answer different questions: HEAD == certified binds the WORKTREE and is
  satisfied BY CONSTRUCTION, since a lane stands at the very commit it is certifying. So a
  `result: PASS` recorded BEFORE a further commit, an amend or a rebase persisted in
  `.review-stage/` and certified the NEW tree. `open` therefore resolves `HEAD` and records it in
  the stage record, and `--force` **RE-STAMPS** it — deliberately unlike `spawned-at`, which is
  PRESERVED because elapsed-since-FIRST-spawn is the number that says a stage has produced nothing
  for 70 minutes. A record with **no** `head-sha:`, **several** of them, or a value that is not a
  40-hex sha is a NAMED REFUSAL and never a skip: an older record predating the field must not be
  readable as certifying. **The fail-closed direction is deliberate** — this is the gate-of-record
  rule (any change after the gate INVALIDATES it) applied to the intent audit, and an audit of an
  older tree may not certify a newer one; every one of those refusals prints the same remedy, which
  is to re-open the stage with `--force` at this commit and re-run C. **AND THAT BINDING RESTS ON
  ONE OBSERVATION OF THE RECORD (#3751 round 9, N2)**: `head-sha` was validated from one read, and
  `review-stage.sh verdict` then RE-READ the record to find which report is current (the nonce
  below) — two reads of one record are two different facts, so a replacement in between handed back
  a verdict from a different GENERATION of the stage, possibly bound to a different commit, under a
  binding checked on the old one. Measured, the success line read `C-VERDICT PASS …
  stage-head=273cd3dff12c … report: …/c.decoygenerationB.md` — a binding it never read, beside a
  generation whose own `head-sha` was forty zeros — which defeats this binding and the nonce IN
  COMBINATION, i.e. exactly the pair that stops a stale audit certifying a new tree. So the record
  is captured ONCE, the `head-sha` is parsed from THAT capture rather than a second read, and the
  capture must still be byte-identical before the token is parsed. **A handoff was the wrong fix**:
  resolving the report in `premerge-assert.sh` and passing it to `verdict` would rebuild from the
  other end the control channel round 4 (H2) deleted with `--report` — nothing outside
  `review-stage.sh` may name which file holds a verdict. **AND BYTE EQUALITY IS NOT IDENTITY — AN
  ABA REPLACEMENT DEFEATS THAT COMPARISON (#3751 round 10, P2)**: the record can go from the
  validated generation A to a foreign generation B while `verdict` reads B, and BACK to A before
  the comparison, leaving two byte-identical observations while the ACCEPTED verdict came from B.
  So the verdict is bound to the GENERATION itself, using a value it already reports OUTWARD — its
  mandatory `report=` field carries that generation's nonce (`<kind>.<nonce>.md`), which must equal
  the `report-nonce:` of the SAME capture `head-sha` was parsed from; a verdict read from B returns
  B's nonce, so ABA cannot satisfy it. Reading a value OUT of the verdict line rebuilds no control
  channel (nothing is passed IN), the byte comparison is KEPT as defence in depth (it catches an
  edit under the same nonce, and a vanished record), and every unbindable state REFUSES BY NAME: a
  legacy record with no `report-nonce:`, several of them, an unusable token, a `report=unresolved`,
  a foreign nonce. It gates the two tokens the closed grammar lets PROCEED, because acceptance is
  the only thing that can certify. Third binding: the
  verdict line is validated against its WHOLE documented grammar — `REVIEW-STAGE: <kind> RESULT:
  <token> elapsed=<n> deadline=<n> agent=<t> report=<abs>` — with the **stage KIND compared by
  STRING EQUALITY**, each mandatory key required EXACTLY ONCE, **and each one's VALUE measured**
  (round 7's L3: the census only COUNTED, so a `PASS` line ending in a BARE `report=` was ACCEPTED —
  "counted, not measured". The permitted set is DERIVED FROM WHAT THE EMITTER PRODUCES, which is what
  stops it redding on correct input: digits or `unknown` for the two clocks, non-empty for
  `agent`/`report`, so round 6's honest `unknown`/`unresolved` and a legitimate `deadline=0` all
  still pass). **`report=` is read as the REMAINDER of the line, not as one whitespace-delimited
  field (round 11, Q3)** — it carries an absolute PATH, and a path may legitimately contain
  whitespace (a checkout at `/tmp/work tree`; this repository tracks 40 space-bearing paths under
  `docs/`), so a field read TRUNCATED the value at the first space and the generation binding then
  REFUSED an otherwise VALID verdict: a false refusal on correct input. That rule is sound ONLY
  because `report=` is emitted LAST, so the assumption is **ENFORCED, not assumed** — the emitter's
  states are derived by RUNNING it, no mandatory key may follow `report=` on any line it produces,
  and its single emit site is pinned structurally. Generalise it: **a parser that reads a
  PATH-valued field positionally has a whitespace bug waiting.**
  **The `=`->`~` map has exactly ONE exemption, coupled to the property that justifies it (#3751
  round 16).** A repository root may legally contain `=`, and `report=` went through that map — so
  on such a checkout the verdict line advertised a path that DOES NOT EXIST while the grammar
  promises the absolute report-of-record path (measured at `.../eq=path/lane`: `open` printed the
  real file, `verdict` published `.../eq~path/...`), and `verdict` offers no separate raw channel.
  The exemption is sound only because `report=` is LAST and read as the remainder, so the two facts
  are pinned together in one match, and it is CONFINED to one definition and one call site — one of
  the other `report=` emitters puts `now-verdict=` after it, where the exemption would be unsound.
  The control that proves the confinement is that a `report=` pair smuggled through `agent=` is
  still neutralised, since unmapped it lands AHEAD of the measured one and the reader takes the
  first. Control-character neutralisation is untouched: the exemption is the `=` map alone.
  **And a checkout path this grammar cannot carry is REFUSED at the boundary, never published wrong
  (#3751 round 17).** The repository root is the one path component derivation does not validate,
  and a root the one-line renderer rewrites made the two commands lie DIFFERENTLY about the same
  file: `open` prints the RAW path, so a newline-bearing root SPLIT it across two physical lines
  (the second carrying none of the `REVIEW-STAGE: ` anchor consumers read), while `verdict`
  FLATTENED it and published `.../lane two/...`, which no `open(2)` resolves. An earlier round had
  declared such a path unrepresentable **and "never arriving"** — the second half was false, since
  git resolves the root of whatever checkout the tool runs in — so that declaration is WITHDRAWN
  and `require_repo_root` refuses (exit 64, nothing read or written) at the ONE resolution site, so
  every subcommand inherits it. Generalise it two ways: **a residual whose premise is "this input
  never arrives" is a claim about the world, and it should be re-measured rather than inherited**;
  and **key such a check on the RENDERER's own answer** (does the value survive it unchanged?),
  never on a hand-written list of bad characters, which drifts from the renderer the first time the
  renderer changes. A space is unaffected, which is the control that keeps it from redding correct
  input.
  **And a decision must rest on ONE observation — the third instance of one shape, so the fix is a
  mechanism (#3751 round 17).** Two earlier rounds fixed the same shape at their own sites: a merge
  assert that validated one field from one read of a record and another from a second read, and a
  classifier that read its subject eight times. The third: the hand-audit recorder read the REPORT
  using the generation loaded earlier and then read the RECORD independently, so a concurrent
  re-open publishing generation **B** between those reads left BOTH final re-verifications satisfied
  — an unchanged report **A**, an unchanged record **B** — and it published the merge-proceeding
  token over B **without ever inspecting B's verdict**, with no force flag, and with a trace naming
  **A** (measured: exit 0 while B held a blocking `FINDINGS`). **A trace that names the wrong
  generation is worse than no trace.** The rules to carry: **two reads of one subject are one
  observation only if something re-verifies between them**; **publish a defect as a closed KIND
  beside its detail sentence**, because a consumer keyed on the prose is reading a diagnostic as a
  control (two legitimate sentences shared a keyword and a text match routed the wrong refusal);
  **delete the parameter a function no longer uses**, so a second read becomes unexpressible rather
  than merely untaken; and **a state that was never established gets its own cause on every
  surface**, since "read it again" and "repair the file" are different operator actions. Mechanized
  as a third boundary scanner (one file per property, beside the emit and read ones) which
  attributes every reader call to the function it appears in, requires each decision path to observe
  exactly once, declares what it does not cover on every run, and refuses an undeclared subject
  rather than reporting it clean.
 "Somewhere on this line it says
  `RESULT: PASS`" is not a verdict about C: measured on #3751's own branch, a sibling `code-review`
  stage's PASS line satisfied `--c-verdict`, and a truncated capture with no
  `elapsed=`/`agent=`/`report=` did too. Only
  `PASS` and `AUTHOR-PERFORMED` proceed, and the second prints **under its own token on a
  `PREMERGE: C-VERDICT` line, never folded into `PREMERGE: OK`** — the same reason the roborev wrapper's
  `WAIVED` is distinct from `PASS`: a reader must be able to see that the intent audit was performed by
  the diff's author.
  **And what `PREMERGE: OK` does NOT prove (#3650), which the success path states itself on a
  `PREMERGE: SCOPE` line:** it proves the diff is unchanged since certification and that a full gate
  PASSed on THAT EXACT TREE — not that the change was certified against the `main` it will join. A
  squash-merge composes the diff with main's CURRENT tip, so for any PR whose base is behind main the
  certified tree and the merged tree are different objects (measured on #3358/PR #3362). A gate on the
  MERGE RESULT is #3650 **slice 2** and is deliberately not implemented here. What slice 1 DID add is
  a **non-blocking base-staleness advisory** on `PREMERGE: ADVISORY` lines
  (`scripts/flow/base-staleness.sh`): `N` commits behind the merge-base with `origin/main` and `M` of
  those touching the diff's blast radius — measured at the **certified sha**, not the local checkout's
  `HEAD` (paths the diff touches + a hard-coded gate-global set; every run declares TWO gaps: it is not
  a dependency closure, and the gate-global list is itself curated and NON-CLOSED). It is information, never a verdict: it cannot change
  the exit code, an absent/failing/`UNMEASURED` advisory is non-fatal, and any consumer of it must
  treat `UNMEASURED` as STALE rather than fresh.
  Report a pass as "gate of record verified at `<sha>`", never "certified against main". The closer **refuses to merge on any
  non-zero exit** (fail closed). It also re-reads issue/PR comments for a fresh `HOLD:` order in the
  same pre-merge pass. Motivated by the 2026-07-14 stale-merge escape on
  #2299/PR #2421: the closer certified a rebased-and-fixed tip locally but never pushed it, so
  `gh pr merge` squashed the PR's *stale* pre-fix head and transiently landed a known data-loss
  blocker on `main` (remediated by PR #2455). The GitHub required check re-runs on push but cannot
  catch a "merge of an old green head" — the SHA assertion is the real guard.
- **Unique gate-summary paths.** Each gate writes its `AGENT_GATE_SUMMARY_FILE` to a `mktemp`-unique
  path (e.g. `$(mktemp /tmp/gate-<issue>-XXXXXX.txt)`) — shared `/tmp` names get contended under
  multi-lane load, so one lane's summary can clobber or be misread as another's.
- **Single full gate per machine — enforced mechanically (#2640).** The default posture is one full
  gate at a time on a box: `bootstrap-agent-machine.sh` persists `CQLITE_GATE_MAX_CONCURRENCY=1`
  into `/etc/environment` — which PAM reads at session creation, so non-interactive shells see it —
  and then **verifies from a fresh, profile-free session that the value is visible and that the gate
  honours it** (`gate-pin: VERIFIED`, #3414), rather than trusting that the write happened. That
  verdict is scoped to a PAM-created session, so a gate launched from a systemd unit or container
  entrypoint is not covered by it; it also measures that the file and the session AGREE, not that the
  file is where the session got the value, so a box setting the same value from a sudoers `env_file`
  would read VERIFIED with an `/etc/environment` no PAM stack loads. The per-run authority stays the
  gate's own `cpu-budget:` token.
  A visible value the gate discards or clamps reports `gate-pin: NOT-HONOURED` — its remedy is to
  fix the VALUE, since bootstrap never rewrites an existing one. With the
  pin in effect the #1825 machine-wide cap admits exactly one full gate and the #2640 per-gate core
  budget hands that sole gate the full core count; a gate that resolved its cap from the default
  formula instead says so on its own `cpu-budget:` line as `max-concurrency=N(default)`. The gate also derives `CARGO_BUILD_JOBS` + nextest `--test-threads`
  from the slot count and wraps itself in `taskpolicy -c utility` (macOS) / `nice` (Linux), so even
  if two gates do overlap neither oversubscribes the CPU. No manual `pgrep`-serialization is needed.

## The specialist roster

| Role | Agent / tool |
|------|--------------|
| implement / format debug (TDD) | `sstable-developer` |
| review-first (Rust review) | `rust-reviewer` — on the lite-green diff, BEFORE the full gate |
| endgame owner (full gate → C → final roborev → merge → finalize) | `flow-closer` — per issue, disposable context |
| intent audit (C) | `spec-auditor` (anchored to `openspec/changes/<name>/specs/**`) — see [Spec-driven audit](/cqlite/agents-developing/spec-driven-audit/) |
| parity / test execution | `test-validator` |
| test quality | `coverage-reviewer` |
| code review | roborev (review-first + the closer's final pass) |
| correctness | `scripts/agent-gate.sh` — the ONE gate of record, inside `flow-closer` |

## State model

- **Backlog** = GitHub issues; the Project `Status` field is the authoritative lifecycle
  (`Backlog → Ready → In Progress → In Review → Done`). Each issue carries one `P0`–`P3`; `status:*`
  labels are an **enforced read-mirror of board Status for discovery only** (Path A, #1886; #2855 — see
  [the claim board](#the-shared-claim-board)).
- **1:1:1:1** — one issue ↔ one worktree/branch `issue-<N>-<slug>` ↔ one OpenSpec change `<slug>` ↔ one
  PR. Worktrees branch from `origin/main` and lack the gitignored `Data.db` binaries — run the gate with
  `CQLITE_DATASETS_ROOT` pointed at the main repo's `test-data/datasets`.
- The **definition of done** is the [spec-driven audit](/cqlite/agents-developing/spec-driven-audit/)
  one: `agent-gate.sh` PASS + C PASS + roborev clean.

## The shared claim board

In-flight work is tracked on a shared **GitHub Project (v2)** with a single-select `Status` field
(`Backlog → Ready → In Progress → In Review → Done`). It is the cross-session, cross-machine view — and
the thing a human can also drive from mobile. `flow-board` renders it (`gh project item-list`) showing
each item's status, assignee, and priority; built-in server-side Project automations move items on
GitHub-side events (PR merged / issue closed → `Done`, assigned → `In Progress`), so the board stays
fresh even when an action came from the phone or web with no `flow-*` run.

**One-time setup (the owner's action):** Projects v2 needs the `project` token scope —
`gh auth refresh -s project` — then run `test-data/scripts/setup-project-board.sh` to create + link the
board and normalize the `Status` options. The built-in workflow automations (merge/close → `Done`,
assigned → `In Progress`) cannot be set via CLI; the script prints the manual web-UI step for them.

**Path A — the board is the sole dispatch authority (issue #1886):** work is selected and claimed by
the Project `Status` field ONLY. If the `project` scope or the board is **unreachable, STOP and fix the
auth** (`gh auth refresh -s project`) — do **not** fall back to labels to select work. An empty `Ready`
column means no work is ready (near a release it is *meant* to drain to zero), not a cue to dredge labels.

**`status:*` labels — an enforced read-mirror for cheap discovery (issue #2855):** the labels are no
longer decorative. `.github/workflows/project-board-sync.yml` is the *single writer*, deriving each OPEN
issue's `status:*` label from its board Status (Ready→`status:ready`, In Progress→`status:in-progress`,
In Review→`status:in-review`, Backlog/Done→none) on the 30-min sweep + on issue events, with a
drift-detector that FAILs the run on any label≠Status disagreement. So a session MAY *narrow* candidates
cheaply and server-side with `gh issue list --state open --label status:ready --json number,title` (no
issue bodies, no board pagination). But the label is **eventually-consistent (≤30-min lag) and NEVER the
dispatch/claim authority**: it only narrows the candidate set — the selection decision is by live board
`Status`, and the claim ref plus a fresh board read at claim time remain the sole double-work arbiter.
flow-* skills no longer write the board-derived labels (they set board Status only; the mirror follows);
`status:spec-review`/`status:addressing` stay transient skill-managed sub-markers the mirror does not touch.

## The claim protocol (no duplicate work)

Before working an item, a session claims it so no two sessions — **including two sessions authenticated
as the same GitHub user on different machines** — work the same item. Because assignee `@me` is identical
for the same user on two machines, the assignee is *not* the lock; the deciding lock is the **slugless
fixed-name ref `refs/claims/issue-<N>`**, acquired through `scripts/flow/claim.sh` (issue #2665).
`claim.sh claim <N>` pushes a **unique root commit** to that fixed-name ref; git arbitrates the ref
update server-side, so the winner is decided purely by the push result — regardless of slug or base.
This closes two field hazards the earlier slug-named branch lock left open: two sessions on **different
slugs** (`issue-<N>-a` vs `issue-<N>-b`) both succeeded (the #1632 slug pair), and two sessions branching
the **same `origin/main` tip** pushed an identical SHA, so git reported "up-to-date" to the loser and
both thought they won. The `issue-<N>-<slug>` branch survives only as **worktree/PR plumbing — never the
lock**.

1. **Eligibility** — the item is `Ready` AND has **no** `refs/claims/issue-<N>` claim ref
   (`bash scripts/flow/claim.sh status <N>`) and **no** legacy `issue-<N>-*` branch on origin (mixed-fleet
   safety; older workers still branch-lock). A surviving branch over a **free** claim ref is not a dead
   end — see *Resuming past the legacy-branch guard* below.
2. **Claim** — `bash scripts/flow/claim.sh claim <N>` acquires the lock (`CLAIM HELD` exit 0 / `CLAIM LOST`
   exit 2); only then create the worktree + branch and set assignee `@me` + `Status=In Progress` for board
   visibility. `flow-activate` claims immediately — before any spec work; oracle-driven issues claim in
   `flow-implement`.
3. **Verify** — `claim.sh` re-reads the ref after the push and reports `CLAIM HELD` only if you hold it
   (`claim.sh verify <N>` re-checks holder identity later); on `CLAIM LOST`, back off and take the next
   eligible item.

**Machine prerequisite: git itself must be authenticated (issue #2942).** The lock is a plain `git push`,
and `gh` auth is a *separate* credential path — a box with an authenticated `gh` CLI but no git credential
helper fails every claim with `fatal: could not read Username for 'https://github.com'`, so the claim
protocol does not work at all while `gh auth status` reports a healthy machine. `claim.sh` classifies that
signature as **`CLAIM: ERROR reason=auth … (NOT retryable)`** naming the fix, *not* the old
`reason=infra … (transient — retry)` that sent workers into a retry loop on a fault which can never
self-clear; `reason=infra (transient — retry)` continues to mean a genuine, retryable blip. That
classification covers `claim.sh` (`claim`/`adopt`/`release`/`smoke`) only — `claim-heartbeat.sh` surfaces
git's raw error on its own pushes. Fix a box with `gh auth setup-git` or
`bash scripts/bootstrap-agent-machine.sh --yes`, whose preflight checks git push credentials (configuring
a helper **scoped to the origin host** that dereferences `$GH_TOKEN` at call time — never writing the
token to disk; because it reads the environment it works only where `GH_TOKEN` is exported, so prefer
`gh auth setup-git` for systemd/cron workers) and probes **board access functionally** instead of trusting
the `project` scope string. Full delta list with the identifying messages:
`docs/development/fleet-runbook.md`.

Another machine that finds an existing claim can `git fetch` the branch to **resume** that work instead of
colliding; a **reaped** claim is adopted via compare-and-swap — `claim.sh adopt <N> --expect <old-sha>`,
which replaces the ref with force-with-lease so a resurrected original holder loses the lease and detects
the loss immediately (fixes the #2467/#2499 two-writer race).

**Resuming past the legacy-branch guard (issue #2945)** — when the claim ref is **free** but an
`issue-<N>-*` branch still stands on origin (a parked/reaped/released claim, an owner-approved spec that
lives on that branch, or just a merged-but-undeleted PR branch), `claim` refuses with
`reason=legacy-branch-lock … claim-ref=free resume=documented-procedure`. That refusal is a
**diagnosis, not a hand-off**: it names the blocking branch(es) and tells you the claim ref itself is
free, then points here. The ONE sanctioned resume is documented *only* here and in
`claim.sh -h` — it is deliberately **never printed as a runnable line** (see below):

```bash
bash scripts/flow/claim.sh adopt 1234 --expect none --reason resume-legacy-branch-lock:branch-outlived-claim
```

`--expect none` is git's **empty lease** ("this ref must not exist"), so the create is still arbitrated
server-side: a machine that actually holds the claim ref keeps it and the resumer gets `ADOPT-LOST`
(exit 2), and two machines racing the resume still yield exactly one winner. `--reason` is **required** —
it is recorded in the claim commit next to who took it (machine/actor/ts) and rendered by
`claim.sh status`, so a resume is auditable; a reason with nothing recordable in it (`'   '`, `'---'`, an
unset variable) is a **usage error** (exit 64), never a silent `reason=unspecified` — and so is a bare
**placeholder** (`why`, `todo`, `tbd`, `xxx`, …) or a reason still carrying an **unsubstituted `<…>`**
(a copied `--reason resume-legacy-branch-lock:<branch>` sanitizes to a non-sentinel token, so it is
rejected on the raw text): the record must say why. That is also why the example above substitutes a
concrete issue number and reason — the documented invocation is one that works when run verbatim.
`--actor` is fail-closed the same way (an actor with nothing recordable in it would alias two distinct
identities onto one holder, and the actor gates re-entrancy/`verify`/`release`). A hex
`--expect` must be a **full** object name (40/64 hex) — a truncated sha is a usage error, not a lost race.

**Why the command is never printed for you (owner decision, #2945).** `claim.sh` used to decide, from an
in-script liveness probe, whether to print a copy-pasteable version of that command. That probe is gone.
The readers of a refusal are agents that run printed remediations **literally**, and an older-fleet worker
locks with the *branch* while holding **no claim ref** (`claim-ref=free` is true for it) — so a printed
empty-lease adopt would take an **actively-worked** lane and create a second writer. Judging abandonment
needs signals `claim.sh` cannot read soundly, and three successive revisions of the probe each shipped a
fresh version of that hazard (a vacuous branch-tip date, a cross-process ref race, a fleet-wide permanent
withhold). So the refusal diagnoses and points here, and **you** establish abandonment first with the same
test `flow-board`'s reaper uses:

```bash
bash scripts/flow/claim-heartbeat.sh should-reap <machine>   # exit 0 = reapable, 1 = keep, 2 = no ref
```

i.e. claim age > 4h **and** no open PR **and** (pid-dead, when the claim is local) — plus the board
`Status` and the branch/PR author. Only then run the documented resume. Retrying after a transient
`ERROR reason=infra` is safe: an
adopt whose ref is already held by *this* machine+actor reports `ADOPTED … (re-entrant)` exit 0 rather
than abandoning an issue you own. This is the only sanctioned way past that refusal — **never hand-craft
a claim commit or push the ref directly** (the field failure that motivated #2945). The claiming session also maintains a
liveness **heartbeat** (`scripts/flow/claim-heartbeat.sh beat <N>` — a cheap origin git ref under
`refs/heartbeats/<machine>`, never a GitHub API call — refreshed at claim time and on every stage
transition: activate/implement/gate/PR). `flow-board` reaps **abandoned claims deterministically** (issue
#2089): an `In Progress` item is reaped only when its heartbeat age exceeds the documented threshold (4h —
the `claim-heartbeat.sh` header is the single source of truth) **AND** it has no open PR — reap = a
traceable comment + assignee clear + `Status → Ready` + an adopt-eligibility note on the claim ref (never
deleting a branch that carries commits). This replaces the old "no recent commits" guess. `flow-finalize`
releases the claim (`claim.sh release <N>`, which refuses under an open PR without `--force`) and clears
the heartbeat on cleanup.

For unattended/overnight runs a **worker supervisor** (`scripts/local/worker-supervisor.sh`, issue #2090)
recycles one worker process per issue — the hard context bound is process exit: the worker rehydrates from
the board, resumes this machine's own claim branch first (crash recovery) else claims the next Ready item,
runs it to merged + finalized, writes a `.worker-last-iteration.json` marker, and **exits** (never a second
issue per session). The supervisor adds a **per-LANE** single-instance lock (scoped to the lane's checkout root, so a box runs
several lanes while two supervisors in the *same* worktree still refuse to coexist — it mechanized
one-worker-per-machine until #3393 retracted that),
fail-closed preflight (load/disk/leftover-process/stop-file), a crash-loop breaker, budgets, and ntfy
notifications. See the [fleet runbook](https://github.com/pmcfadin/cqlite/blob/main/docs/development/fleet-runbook.md).

**Supervisor-authored claim + CI-side reaper (issue #2655 / #2499 design).** Heartbeats used to depend on
the worker LLM *remembering* to `beat`, and the reap threshold was enforced only in prose. Liveness is now
**mechanism-driven**: the supervisor stamps `refs/lane-claims/<machine>/<issue>` (issue + supervisor-PID + ts, via
`claim-heartbeat.sh stamp`) at every worker spawn, refreshes it each iteration, and clears it on a clean
exit — where `reap` **refuses to delete a claim whose issue still has an open PR** (an unfinished endgame
stays owned for adoption rather than orphaned; the #2499 orphaned-endgame case). This `refs/lane-claims/*`
namespace is deliberately distinct from `claim.sh`'s per-issue lock `refs/claims/issue-<N>`.
**Claims are PER LANE since #3393's owner ruling** — `refs/lane-claims/<machine>/<issue>`, replacing
one-ref-per-machine. The old layout was justified by #1930's "one worker per machine", which the fleet
had not followed all day: several lanes on a box overwrote each other's claim, so a monitor could see
at most one and two of #3393's three silent lane deaths (both on one host) were structurally
invisible. **#1930 is retracted; design for N lanes per box.** The legacy `refs/machine-claims/*` is
still *read* so a pre-ruling ref is drained rather than pinning its board item at In Progress
forever. A new namespace was required rather than a sub-path because git forbids a ref being both a
file and a directory, and `<machine>-<issue>` is ambiguous when machine names contain dashes.

`claim-heartbeat.sh should-reap` is the single, **fail-safe** reap predicate. It has **two forms, and a
two-argument call is ALWAYS the legacy one** — `should-reap <machine> [threshold_secs]` acts on the legacy
per-machine ref, and a lane needs all three: `should-reap <machine> <issue> <threshold_secs>`. The grammar
refuses to guess from arity, so `should-reap <box> <issue>` reads the issue number as a **threshold** and
answers about the *legacy* ref — a real answer to a different question, which can report an active
per-lane claim as absent. (#3393 round 21: this page previously advertised `<machine> [issue]`, i.e. that
trap written down as doctrine.) Exit `0` = reap,
`1` = keep, `2` = no ref): it reaps ONLY when age > threshold (4h) **AND** the issue has no open PR **AND**
(the PID is dead, *when the claim is local* — a foreign machine's PID is unknowable, so from CI that clause
is skipped and age + no-open-PR govern). It KEEPS on a fresh ref, an open PR, a live local PID, or an
unparseable age; a `gh`/network hiccup in the open-PR probe assumes an open PR (keeps). The
`project-board-sync` **30-minute cron** now carries a `reap-claims` job applying exactly this predicate
server-side, deleting the stale claim ref and flipping the freed board item back to `Ready` with a traceable
comment. Two workflow hardenings ship with it: **`PROJECTS_TOKEN` absence now fails the workflow loudly**
(`::error::` + non-zero exit — a persistent red run is the alert) instead of the old silent green
`::notice::` no-op; and the scheduled board sweep only backlogs a null-status issue once it is **older than a
10-minute auto-add grace window**, so it no longer races the built-in Auto-add workflow's default-status
write on a freshly created issue.

**`should-reap` is a REAP GATE, not a liveness monitor — and that gap cost three lanes (#3393).** It
consults the recorded PID only *after* age > threshold, so a worker the kernel OOM-killed a minute ago is
indistinguishable from a healthy one for **four hours** — and even then the answer is an exit code nobody is
watching. On 2026-08-27/28 the kernel issued **10 global OOM kills** across two 30 GB workers (every victim
a `python3` at 20–28 GB) and three lanes died silently, each leaving a clean worktree, a held claim and an
open PR. **Memory exhaustion is invisible to any monitor that iterates existing sessions**: a dead tmux
session cannot report itself.

**The tool is `claim-heartbeat.sh dead-lanes`, and per-lane claim refs are what make it work.** It asks
"is anything dead RIGHT NOW", inverting both of the reaper's conservative guards on purpose: no age gate (a
fresh claim with a dead PID *is* the shape of an OOM kill), and an open PR does not suppress the report — for
the reaper an open PR means KEEP, but for a report it is the most urgent row on the page. It is a REPORT: no
ref is deleted, no board item moved. Read `dead-lanes --help` for the authoritative verdict set; it lives
beside the code and cannot drift from it.

**Why the layout had to change first (#3393 owner ruling A).** The OLD `refs/machine-claims/<machine>` was
keyed per **MACHINE** and force-updated every supervisor iteration, so on a multi-lane box a surviving lane's
stamp overwrote a dead sibling's PID — a live sibling did not merely hide a dead lane, it **masked** it. Two
of the three deaths above were on one host, which is exactly the case that collapsed. Claims are now
`refs/lane-claims/<machine>/<issue>`, one per lane, and **#1930's one-worker-per-machine invariant is
retracted** — multiple lanes per box is the standing model, so design for it. A new namespace was required
rather than a sub-path: git forbids a ref being both a file and a directory, and `<machine>-<issue>` is
ambiguous when machine names contain dashes. The legacy namespace is still *read* by `list-claims`,
`dead-lanes` and the CI reaper so a pre-ruling ref is drained rather than pinning its board item at In
Progress forever.

Exit codes are what a cron reads, so they are worth knowing: **3** = a dead lane was reported, **1** =
none was reported, which also covers zero claim refs and a run where every claim belongs to another
machine. **This slice never exits 0** (#3393 split ruling) — act on 3, and never read 1 as a clean
bill of health. Per-lane refs do make a sound clean verdict possible, since a surviving sibling now
stamps a different ref and can no longer mask a dead lane; it was split out rather than shipped
because the fail-open defect family clustered in that exit-0 path, and being wrong there is silent.
It claims nothing about lanes that never stamped (a lane run with `CLAIM_CMD=""` is invisible) and
nothing about other machines — a PID is only checkable where it runs, so **run it ON the suspect
box**.

A suspected dead lane still has a diagnostic **order, and it matters** — full procedure in
`docs/development/fleet-runbook.md`. The one line worth memorising: when a box accepts TCP but sends no SSH
banner from inside the VPC, **check `dmesg` for an OOM kill before concluding the instance is broken**.
Reading that symptom as a broken instance already cost one healthy machine (terminated, losing a
measurement lane's 43 minutes and an unpushed commit), and a soft reboot may be silently ignored on a
memory-exhausted host.

**Never block on a question (park-and-resume, #2666).** A worker runs unattended, so `AskUserQuestion` (and
any interactive prompt) is attended-sessions-only. When a worker hits Seam 1 (an unapproved spec) or a
genuine mid-run owner decision it does not wait — it **parks**: posts ONE structured question comment
(options + recommendation + default), adds the `needs-decision` label, writes a `blocked` marker with
`reason: seam1-approval|needs-decision`, and exits, releasing the machine. The supervisor judges this
`parked-on-owner`, pages the owner once, and moves to the next Ready issue; a worker that nonetheless wedges
on a prompt is caught mid-iteration by a log-tail watchdog and paged as `stuck-on-question`. Neither counts
toward the crash breaker. The parked issue resumes only on a strictly-newer owner reply (the worker reads
the answer and clears the label); a durable `resume-dont-ask` label is a standing Seam-1 seal `flow-implement`
honors in place of asking.

## Concurrency model

- **One active worker per machine; the worker paces the machine's load (#1930).** A single lead/worker
  session owns a machine at a time — the load + worktree-isolation rule that sits *above* the claim
  protocol. Two efforts on one box collide on the shared worktree and oversubscribe the CPU, which flakes
  scheduling-sensitive tests (write-throughput, the streaming GIL-release test) and can SIGKILL gates. The
  owning worker is responsible for load: **serialize your own full-gate runs — never two full
  `scripts/agent-gate.sh` at once on one box** (the machine-wide gate cap is a backstop, not a license to
  overlap). **Subagents are exempt:** a worker fanning out `sstable-developer`/reviewers is not "multiple
  workers" — they never launch competing full gates. The rule targets independent lead/worker *sessions*.
- **Default (recommended): one lead → subagents.** A single `flow-lead` spawns subagents and assigns each
  **disjoint** work — zero duplicate work by construction.
- **Multiple independent sessions: the claim protocol is mandatory.** Each acquires work only through the
  claim protocol above — and, per the rule above, independent sessions belong on *separate* machines
  (one-per-machine handles a single box; different machines coordinate via the `refs/claims/issue-<N>` ref lock, #2665).
- **Agent Teams is optional, desktop-only.** `CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1` gives a built-in
  file-locked shared task list for coordinated parallel sessions, but it is experimental and desktop/tmux
  -only (no `/resume`, one team per session). Use it if you want; it is not required.
- **Never run N bare `flow-lead`s without the claim protocol** — independent leads with no claim each pick
  the same top `Ready` item and collide.

## Driving from mobile / remote

The Claude Code mobile app cannot run the local pipeline itself (no local bash, skills, worktrees, or the
dataset binaries). Two supported ways to still drive work from the phone:

- **Remote Control (primary).** Run `claude remote-control` on the laptop and connect from the mobile
  app; the phone drives the **full local `flow-*` pipeline** (worktrees, `agent-gate.sh`, `gh`,
  `openspec`) in that local session. The laptop must stay online.
- **Claude Code on the web (secondary, cloud).** A cloud session uses the repo-committed `.claude/`
  (skills/agents/hooks) but not user-scoped config or local data. Run the **cloud setup script**
  `test-data/scripts/cloud-setup.sh` first — it installs `openspec` + `gh` and fetches the dataset
  (`fetch-datasets.sh`) so `flow-implement` can run the gate in the cloud.

**Spec approval is the only standing human seam, and it is GitHub-mobile-native** regardless of how you
drive: approve the OpenSpec spec + design in the session (Seam 1). For worker-owned issues **merge is no
longer a hand-merge step** — the closer arms `gh pr merge --auto` and GitHub lands the PR on green (see
[merge-on-green](#merge-on-green-no-ci-busy-wait)), and the merge event moves the board item to `Done`. The owner intervenes on merge (from the mobile app / web
UI) **only on escalation** — a genuine design-call roborev finding, a scope/product question, or work
outside the issue.

## The implement loop: review before gate, gate once at the end (issues #1821, #2084, #2086, #2087, #2088)

Inside `flow-implement` the loop is ONE coherent design, not three patches:

```
implement (TDD) → lite (each fix round) → rust-reviewer + roborev on the lite-green diff
  (review-first, DEFAULT) → fix (lite re-cert + diff-scoped targets, NEVER a full gate)
  → open PR → flow-closer { FULL gate ONCE → C → final roborev → merge-on-green → finalize }
```

- **Review-first is the default (issue #2086).** `rust-reviewer` + roborev run on the **lite-green** diff
  **before** the first full gate, so review discovers fixable problems before we pay for the 12–25 min gate.
  Skip only for a genuinely mechanical diff (no `pub`-item change AND single call site AND no new surface).
- **Every delegated stage's verdict is a FILE, pre-stamped BEFORE the spawn (issue #3751).** A review
  stage used to write NOTHING at any point, so its reader had only ABSENCE to reason from — and every
  consumer of an absence has to CHOOSE how to read it. Nine measured spawns across five lanes and four
  agent types produced no report; nine for nine the lanes recorded "not run" and disclosed it, **which is
  discipline, not mechanism.** So `scripts/flow/review-stage.sh` transplants the gate's own idiom (#3041):
  `open <kind> --issue <N> --agent <type>` creates the report-of-record file **before the agent is
  spawned**, carrying a non-verdict sentinel, and prints the absolute path plus **a paste-ready clause to
  put in the spawn prompt verbatim** — the paraphrase is what varied across the measured sessions.
  `verdict` then emits ONE line of a CLOSED grammar — `{PASS, FINDINGS, NOT-RUN, AUTHOR-PERFORMED}`, first
  word, **string equality, never a prefix test** (#3544) — exiting `0/4/5/6`; it reads the report's
  `result:` line **at COLUMN ZERO only**, because the report body is author-controlled text carrying
  example verdict lines BY DESIGN (the sentinel must show the agent the spelling), so while indentation
  was tolerated the template's own examples were valid records held off by `grep -m1` ORDER alone and
  deleting the column-zero sentinel then appending a verdict read the TEMPLATE's `PASS` (measured —
  #3312: anchor the control token where the payload cannot reach, never pick a rarer delimiter) —
  **and EXACTLY ONE of them** (round 3, G2). Anchoring without COUNTING left `grep -m1` deciding
  by ORDER, so a stale `result: PASS` followed by an APPENDED `result: FINDINGS` classified as
  PASS and a merge proceeded over recorded blocking findings; several column-zero records is now
  refused as AMBIGUOUS in EITHER direction, because a last-wins read is no better than a
  first-wins one. **That was a CONSOLIDATION, not a patch, and the reason matters more than the
  fix:** `premerge-assert.sh`'s `_c_verdict_awk` was ALREADY counting its own anchored lines, so
  two readers of one shape had diverged TWICE in two review rounds — once per axis, each time
  with a reviewer naming one side — and patching whichever side was named is what let the second
  divergence exist. Their agreement is now mechanically checked by a DIFFERENTIAL test
  (`scripts/tests/test_premerge_assert.sh` §44g) that drives BOTH readers over ONE shared table
  of adversarial inputs (indented, several, zero, CRLF, a token with trailing junk, a `result:`
  inside a fenced block, a glob-ish value), asserting they agree per row AND that both reach the
  EXPECTED disposition — agreement alone is satisfiable by both being wrong in the same way.
  **AND THE WHOLE REPORT IS READ EXACTLY ONCE PER VERDICT (round 12, R2):** `classify_report` read
  its subject EIGHT times — existence, a readability probe, the body for emptiness, the `result:`
  census, the disclosure, and `performed-by`/`reason`/`evidence` each through their own field read
  — so a report REPLACED between two of those reads let it assemble `AUTHOR-PERFORMED` from fields
  drawn from DIFFERENT, INDIVIDUALLY INVALID versions (one version's usable `reason` beside
  another's usable `evidence`), i.e. working **no single snapshot ever contained**. A verdict is a
  statement about a document; assembled across two documents it is a statement about neither. One
  observation now feeds every field, the `<key>: <value>` grammar has ONE implementation shared by
  the snapshot and file readers, an unclassifiable observation is a NON-VERDICT reported as
  UNREADABLE (never ungrammatical — the bytes were not obtained, so nothing may be asserted about
  content), and `record-author-performed` passes its OWN byte snapshot in so the bytes its write is
  guarded on and the verdict it decides by are one instant. That is round 9's N2 property one level
  down.
  `status` reports
  elapsed/deadline and is **advisory, never a verdict input** — **which is not licence to answer from
  a comparison that never happened (round 8)**: bash's `[ -gt ]` is a FIXED-WIDTH int64 comparison,
  so an ALL-DIGIT `--deadline-secs` wider than int64 was accepted at the boundary and leaked a raw
  `integer expression expected` onto stderr, OUTSIDE the `REVIEW-STAGE: ` anchor, then reported the
  permissive `past-deadline=no`; `$(( ))` is worse because it does not fail at all but **WRAPS
  SILENTLY**, so a record's `spawned-epoch` produced `elapsed=1788315330` (56 years, for a stage
  opened a second earlier) and a `reopen-count` wrap was **written back into the record**, while a
  zero-padded value is read as OCTAL by `$(( ))` and DECIMAL by `[ ]`. **Being digits is not being
  comparable**: ONE predicate (`int_is_comparable`, bound `MAX_INT_DIGITS = 10` — ~317 years as a
  duration, the year 2286 as an epoch) gates all 7 boundaries where argv or a stage-record value
  reaches a fixed-width operation, INCLUDING the clock's own `date -u +%s` reading, which nothing
  else validates. Out of bound from argv is a NAMED usage refusal that writes nothing; from the
  record it is `elapsed=unknown` / `past-deadline=unknown`, with the record's own text still
  DISPLAYED verbatim so a hand edit stays visible in the audit trail. `NOT-RUN` always names ONE OF SEVEN causes
  (`no report written`, `report absent`, `report unreadable`, `report empty`,
  `report ungrammatical: <what>`, `stage never opened`, `stage record unreadable: <what>`), because the operator action differs per cause —
  `report unreadable` is its own cause rather than folded into `report empty` (whose fix is the AGENT,
  where an unreadable file's is `chmod`) or `report ungrammatical` (which would assert something about
  content never observed). **An idle notice is strictly
  WEAKER than the gate's `INCOMPLETE` sentinel** — at least the sentinel names itself a non-verdict — so
  never read one as a completed review. Writes go under `.review-stage/`, whose ignore status is
  **verified with `git check-ignore`, fail-closed**, so a stage opened mid-run cannot dirty a running gate
  (#2926) or make `premerge-assert.sh` refuse on `dirty: yes` (#3648). **The report path is DERIVED and NONCE-BOUND** —
  `<repo-root>/.review-stage/issue-<N>/<kind>.<nonce>.md`, one fresh unpredictable nonce per open
  (a bare `<kind>.md` is READ, never written, for a record predating the field), computed
  identically by the writer and by every reader, with **no `--report` override** (removed in round 4, a deliberate narrowing: the flag was
  mandated by no requirement, used by nothing, and was the caller-controlled component behind a
  finding cluster across four rounds — written raw into the LINE-oriented stage record, a legal
  newline-bearing filename split and the reader took the PREFIX, which could select a different
  pre-existing report recording `PASS`; and the parent directory was created BEFORE containment was
  verified, so a refused outside-the-repository path still created directories outside the checkout).
  **The PER-OPEN NONCE exists because `--force` used to reset the report AT THE SAME PATH (#3751
  round 5, J1)**: the PREVIOUS, idle agent could wake up after the reset and write its OLD-TREE
  verdict into that path, where it was paired with the newly stamped `head-sha:`, so a commit nobody
  audited passed `premerge-assert.sh` — the expected behaviour of a population of agents that return
  late, not an exotic race. A resumed agent now holds a STALE PATH and is STRUCTURALLY unable to
  write into the current report, which a check could not deliver because the harm is a write. The
  stage record names the report as an **OPAQUE TOKEN, never a path** (round 4 removed the `report:`
  path field precisely so no data file can redirect a reader), written in the SAME atomic record as
  `head-sha:`; an absent field is the LEGACY bare name, which is what every earlier version wrote,
  and several lines, an invalid token, or a record that **could not be READ at all** is a
  `stage record unreadable` NON-VERDICT rather than a fallback to that name — **a `|| true` on the
  count once collapsed "could not read" onto "no such field", so an unreadable record reported an
  OLD report's PASS at exit 0 (#3751 round 6, K1)**. **AND THE TOKEN IS GENERATED, NEVER SELECTED
  (round 6, K2)**: the first design numbered the generations and picked the next by SCANNING for an
  unused file, and a value chosen from what is on disk is a value two concurrent `open --force`
  calls can both choose — measured, both printed `c.1.md` and one agent's `FINDINGS` became the
  other's `PASS`. The scan, its attempt bound and its exhaustion refusal are DELETED rather than
  locked: a lock serialises a race a nonce removes and adds its own failure modes, while subtraction
  cannot introduce a false PASS. **BUT GENERATED IS NOT RESERVED, AND THAT ROUND DELETED THE
  RESERVATION ALONG WITH THE SCAN (round 12, R1).** `mktemp -u` invents a NAME and creates NOTHING,
  so an unreserved nonce repeating a report already on disk — a HISTORICAL report of the same stage,
  deliberately kept as the audit trail — let `open` write over that report and REPUBLISH its path
  in the record: a recorded verdict replaced by the sentinel, and the superseded agent still holding
  that path handed the ability to write the CURRENT one, i.e. exactly what round 5's binding exists
  to prevent, reached with **no concurrency at all**. Deleting the scan was right; deleting the
  reservation was not. The name is now CLAIMED — each candidate created under `set -C`
  (`O_CREAT|O_EXCL`), a FRESH RANDOM nonce on collision, and exhausting the bounded attempts is a
  NAMED refusal (`reason=report-nonce-not-reserved`), never a fallback to an unreserved name.
  **That is not the scan returning, and the distinction is the whole point:** the scan SELECTED a
  name by TESTING EXISTENCE and wrote it in a LATER step — two steps, with a window two callers
  could both observe — while an exclusive create IS the choice, so decision and claim are one
  operation. Everything the nonce bought survives (nothing selected by scanning, an opaque token,
  the path from the record, the record written LAST); the reserved name is an owned resource
  registered with the cleanup path the moment it exists and de-registered on fulfilment, so a
  refused open leaves the tree as it found it and the cleanup can never delete the published report.
  `reopen-count:` remains as the human-readable audit number — and it SATURATES at the ten-digit ceiling rather than restarting (#3751 round 9): `$(( prior + 1 ))` walked off round 8's bound, so the next re-open read an eleven-digit value as incomparable and restarted the count at `1` (measured: the record held `10000000000`, then `1`). Refusal was rejected as the fix — round 8's own ruling is that an unusable counter is never a reason to refuse a spawn — so it is HELD, meaning AT LEAST that many, `note`d when the hold happens, and rendered `<n>+` by ONE renderer on both `OPEN-OK` and `status` (which reports the counter as of this change).
  Superseded reports stay on disk as HISTORY — nothing reads them, and since round 6 nothing depends
  on their existence either — so **paste the path `open` PRINTS into the spawn prompt, never a
  remembered one**; it carries a nonce and cannot be reconstructed, so where none was named ask
  `review-stage.sh status <kind> --issue <N>` and read its `report=` field. **And a SYMLINK at the report
  path, at the `.stage` path or at ANY component under `.review-stage/` is REFUSED, never followed
  (#3751 round 1)** — `check-ignore` judges a LEXICAL path while a WRITE follows links, so an
  ignored-but-symlinked report clobbered a TRACKED file and reported `OPEN-OK` (measured); the
  writes themselves go through an UNPREDICTABLE same-directory temporary file (`mktemp -u`)
  CREATED AND OPENED IN ONE STEP under `set -C`, i.e. `O_CREAT|O_EXCL`, then written through the
  ALREADY-OPEN DESCRIPTOR and `mv -f -T`'d into place (#3751 round 3, G3; the `-T` is round 7's L2 —
  a plain `mv -f` does NOT promise to replace the destination NAME, so a `dest` that is or BECOMES a
  directory or a symlink-to-one receives the temp file INSIDE it and `mv` **EXITS 0**, landing the
  write outside the verified path while the tool reports success. `-T` is **REQUIRED, not
  attempted** — no fallback, since a fallback restores the defect exactly where it cannot be
  detected — which makes GNU coreutils a stated HOST PRECONDITION of `review-stage.sh`; a stock
  BSD/macOS `mv` fails the option parse, moves nothing, and every write REFUSES, naming the missing
  option). The first version used a
  PREDICTABLE `.<name>.tmp.$$`, validated it and then REOPENED it BY NAME — a TOCTOU a PEER LANE
  could win (every lane here runs as one user under a shared HOME), making the write clobber a
  planted symlink's target while `mv` installed the link as the report and reported success. The
  window is REMOVED rather than narrowed, because a check placed after a harmful effect can only
  REPORT it and the harm is a WRITE: there is no predictable name to plant at, `O_EXCL` refuses an
  existing path INCLUDING a symlink (dangling or not — measured, without creating its target), and
  no path is re-resolved between validation and writing. The gitignore check keeps its place
  because it has no window of its own: it is lexical, and it is taken on the EXACT name about to be
  created. A concurrent reader still never sees a half-written `result:` line.
  `verdict` establishes that a VERDICT WAS RECORDED, never that a review was PERFORMED — a report whose
  only content is `result: PASS` reads as PASS. Where no independent audit can be obtained, the sanctioned
  fallback is `record-author-performed --reason <why> --evidence <artifact> --performed-by author`
  (the ONLY performer this tool accepts — `peer` was REMOVED in round 6 (K3): it was accepted and then reported under the token `AUTHOR-PERFORMED`, so a PEER audit was stated to be the diff AUTHOR's, and a peer who CAN audit writes the report of record instead, reaching a genuine `PASS`),
  which REQUIRES the working (placeholders refused as `claim.sh` refuses them) and reports the DISTINCT
  token `AUTHOR-PERFORMED`, never `PASS` — *an author's hand audit is not an independent one; weight it
  accordingly*, and it is sanctioned at all because *an audit whose working is shown is auditable, whereas
  an absent one is not*. **It REFUSES to overwrite a report that already RECORDS a verdict without
  `--force`, and a forced replacement NAMES the token it replaced (#3751 round 2)** — it used to write
  unconditionally, so a recorded blocking `FINDINGS` became a merge-PROCEEDING `AUTHOR-PERFORMED` with no
  flag and no trace, while `open` refuses to re-stamp an already-open stage for the far smaller harm of
  restarting a clock: the worse clobber had the weaker guard. **AND IT PREVENTS RATHER THAN REPORTS
  (#3751 round 9)**: the observation the decision was made on — the report's BYTES, since one `FINDINGS`
  replaced by ANOTHER leaves the token equal — is RE-TAKEN immediately before the publication, and any
  change refuses (`reason=report-changed-mid-write`), `--force` included, because `--force` authorizes
  replacing the verdict the operator READ and not one that arrived while the substitute was being
  prepared.
  **AND NARROWING THAT WINDOW WAS NOT ENOUGH — THE OVERWRITE IS NOW UNEXPRESSIBLE (#3751 round 15,
  U1)**: round 9 declared the remaining span (between the re-observation and the `rename(2)` inside one
  `mv`) as a narrow, irreducible residual — no compare-and-swap rename is reachable from a shell, and a
  lock would not help since the counterparty takes none — accepting that a verdict landing there would
  be LOST. **That declaration is WITHDRAWN.** It was right about the shell and wrong about the harm: the
  party who loses a verdict in that span is not a hostile racer, it is **a slow reviewer** — and this
  mechanism exists *because* delegated reviewers are slow and return late — so the loss came from the
  system's own normal behaviour, and what was lost was a recorded review verdict. So
  `record-author-performed` no longer writes to the report of record at all: it reserves a FRESH
  generation (round 6's nonce + round 12's atomic reservation), writes the substitute there, and the
  stage record — written LAST, the publication marker — names it. Measured before the fix, with the
  interleaving driven at that instant: `RECORD-OK … result=AUTHOR-PERFORMED` at exit 0, no `--force`, no
  `replaced-verdict:`, and the blocking `result: FINDINGS` **gone from disk entirely**; after it, the
  same interleaving leaves that `FINDINGS` readable in its own generation, named by
  `supersedes-report-nonce:`, while the published verdict is the substitute. The window is not closed;
  **destruction is**. Whether the command may PROCEED over a prior verdict is a separate question and
  keeps its rule (refuse without `--force`; under `--force` record `replaced-verdict:` plus the
  generation it came from) — and because nothing is overwritten, a wrong decision there is recoverable
  and auditable rather than silent. The stage record is held to the same mid-write rule
  (`reason=stage-record-changed-mid-write`), since this call now rewrites it, and that rewrite carries
  every other byte through VERBATIM: `head-sha:` is NOT re-stamped and `reopen-count:` is not
  incremented. **The generalisable rule: when a check can only NARROW a window, ask whether the harm can
  be made UNEXPRESSIBLE instead — and never declare a residual whose victim is your own system's normal
  behaviour.**
  **AND "COULD NOT READ IT" IS NOT "NOTHING IS RECORDED" (#3751 round 13, S1)**: round 12's
  single-observation classifier introduced an UNREADABLE state, and this guard branched on the
  TOKEN — where that state arrives as `NOT-RUN`, the REPLACEABLE side — so a report whose recorded
  verdict was UNKNOWN, possibly a blocking `FINDINGS`, was overwritten by the merge-proceeding
  `AUTHOR-PERFORMED` with no `--force` and no `replaced-verdict:` trace (measured: a mode-000
  report holding `result: FINDINGS` yielded `RECORD-OK result=AUTHOR-PERFORMED` at exit 0).
  *Unknown is not absent.* The permissive set is now AFFIRMATIVE — `absent` (nothing recorded to
  destroy) and `present` (read, so the token decides) — read through ONE reader of that grammar
  (`report_state`, shared with the classifier), so a state added later refuses at both callers by
  construction; `--force` does not cover it, and `open <kind> --force` is the recovery, superseding
  the stage with a fresh report and leaving the unreadable file on disk as history.
  **AND A CAPTURE THAT NORMALISES ITS INPUT CANNOT BE THE THING THAT VALIDATES IT (#3751 round 13,
  S2)** — a rule for every `$(…)` read of a file, not a fact about one byte. A command substitution
  SILENTLY DISCARDS NUL bytes, so the capture did not merely lose information, it MANUFACTURED
  grammar: `res<NUL>ult: PASS` holds **no** column-zero `result:` line yet `verdict` reported
  `RESULT: PASS`; a record's `report-nonce: STALE<NUL>PASS1` (not a valid token) was read as
  `STALEPASS1` and redirected the reader to a STALE report's `PASS`; and in `premerge-assert.sh` — the
  merge gate — a `--c-verdict` token of `PA<NUL>SS` arrived as `PASS` and printed `PREMERGE: OK`.
  **The fix is in the READ, not in a probe**: a separate probe of the same path is a SECOND
  observation whose disagreement can fail OPEN (the capture reads the NUL-bearing version, the probe a
  clean one), so the one read maps NUL to SOH IN THE STREAM — nothing lost, the forged grammar never
  created, the byte's presence observable so the refusal can NAME it. One literal, the byte DERIVED
  from it, and a literal SOH refused with it (after the mapping the two are indistinguishable without
  a second read). Two further lossy behaviours were enumerated and LEFT with reasons: trailing-newline
  stripping cannot change a per-line column-zero grammar, and locale/encoding is already
  `LC_ALL=C`-pinned at every consumer — now measured by a cross-locale invariance case, after a source
  scan for unpinned tools was discarded for firing on indented comments and the `--help` renderer.
  **AND NEUTRALISING THE VALUE IS WORTHLESS IF THE PRINTING COMMAND RE-INTERPRETS IT — every line is
  `printf` of a LITERAL FORMAT, never `echo` (#3751 round 14).** `emit`, `note` and `die_usage` used
  `echo`, and under the bash option `xpg_echo` — settable by an **inherited** environment
  (`BASHOPTS`, `SHELLOPTS`, a `BASH_ENV` file) and never by the script — `echo` performs BACKSLASH
  ESCAPE PROCESSING on its argument, which makes that argument a **FORMAT**: a control channel
  carrying data. Measured from a LEGAL directory name and nothing else — a `\n` in the checkout path
  split the one-line verdict into **two**, the second a column-zero `REVIEW-STAGE: … RESULT: PASS`
  for a stage with **no report at all**, and octal `\075` put **real** `key=` pairs on it, so
  `field_value`'s `=`→`~` map — the thing that makes a value unable to introduce a field — was
  defeated entirely (`\033` injects terminal control; `\c` truncates). Scoped honestly: the consumer
  refuses on the LINE COUNT, so no false `PREMERGE: OK` was demonstrated — what is void is the
  one-line grammar and the neutralisation guarantee. It is #3312's rule at the last hop, **stop
  sharing the channel rather than escape harder**, and it is pinned structurally: the emit-boundary
  scanner refuses `echo` outright **with no allowlist** and additionally requires every `printf`
  FORMAT to be script-authored, over EVERY logical line rather than only the emit sites, with its own
  declared scope, its own NOT-COVERED set and its own vacuity guard.
  **AND "EVERY READ GOES THROUGH THE BOUNDARY" WAS FALSE FOR TWO READERS FOR A WHOLE ROUND, SO THE
  CLASS IS NOW MECHANIZED RATHER THAN ASSERTED (#3751 round 14).** Round 13 routed three of the five
  non-boundary read sites and left two reading files directly; both were found by the next review
  round. (1) `count_field_lines` read the stage record with `grep -c` on the FILE — **a faithful
  reader is not a faithful ANSWER**: a record whose key is spelt `report-<NUL>nonce:` holds **no**
  `report-nonce:` line, so the count was a *truthful* `0`, and `0` is exactly the value meaning "a
  pre-nonce record whose single report is the LEGACY bare `<kind>.md`" — so a stale legacy `c.md`
  recording `result: PASS` was reported as the stage's verdict at exit 0 while the CURRENT report
  held the sentinel. The byte never has to defeat the COUNTER to defeat the READER; it only has to
  make the current record unparseable while a stale artifact is still on disk. (2) `_gate_awk` read
  the GATE-OF-RECORD summary raw, so `RESULT: PA<NUL>SS` reached the merge gate as `PASS`. Both are
  routed, and the record-line counter is now three-valued (counted / not countable / not
  representable) with the permissive set spelled AFFIRMATIVELY as `0` at both callers and its own
  refusal token for the third state, because the operator action differs (rewrite the record, never a
  chmod). **Three consecutive rounds have found the same shape — a boundary exists and one path
  bypasses it** (round 7's emit sites, round 13's record reads, round 14's remaining two) — and the
  reason round 13's asserts missed both generalises: **they check that the mapping appears exactly
  ONCE, which is a property of the BOUNDARY and not of its CALLERS.**
  `scripts/tests/lib/read-boundary-scan.sh` asks the caller-side question instead, with two
  recognisers (an input redirection from a value; a reading command at the START of a pipeline with a
  `$`-bearing operand), deliberately WITHOUT reducing command substitutions because both defects
  lived inside a `$( … )`, an allowlist whose entries are claims carrying reasons and whose STALE
  entries are their own FAIL, and a printed NOT-COVERED set. **Its own first draft reported CLEAN on
  the very defect it exists for** — every text call in these scripts is spelled `LC_ALL=C grep …`, so
  the text before the command word ends in `C` and matched no spelling of "pipeline start" — caught
  by the positive control, which is why the controls plant the EXACT shape and a clean run proves
  nothing.
  **AND THE SAME RULE AT A DIFFERENT BYTE: AN ANSI STRIP MAY *LOCATE* A LINE AND MAY NEVER *SUPPLY*
  A VALUE (#3751 round 15, U2).** All three of `premerge-assert.sh`'s awk readers deleted every CSI
  sequence from every line BEFORE the closed grammar was applied to the fields that deletion
  produced, so a token spelt `PA<ESC>[31mSS` normalised into `PASS` and **certified a merge**
  (measured: a file whose `grep -c 'RESULT: PASS'` answers `0` published `token=PASS`); the same
  splice in a gate summary's `RESULT:` reached the merge gate as `PASS`, and in a stage record's
  `head-sha:` normalised into a clean 40-hex sha that would have bound the stage to a tree the
  record does not name. **The strip is not gratuitous, so it was SPLIT rather than deleted** — it
  exists for #3400, colour survives redirection, and without it a coloured capture fails every
  marker anchor and reads as having no verdict line at all. Each reader now keeps **two readings**
  of every line: one with each CSI **deleted**, to LOCATE and parse, and one with each CSI replaced
  by a **single space**, for one question — *did the deletion JOIN two runs the file keeps apart?*
  **The transferable rule is SEPARATE VERSUS JOIN**: colour that BRACKETS a token leaves it a whole
  field of the second reading, while colour INSIDE one splits it, so the token the first reading
  shows appears in the second nowhere. `review-stage.sh`'s own artifacts take the STRICT form (one
  producer, no colour); the gate summary takes the VALUE-ONLY form, because a coloured capture is
  legitimate there and real colouring brackets the KEY as readily as the value. **The trailing-CR
  strip is deliberately KEPT by the same rule** — `\r$` removes one byte where nothing follows, so
  it can separate but never join — and **the reader differential is what decided it**: it FAILED on
  the ESC row (`classify_report` reported `unrecognised result token 'PA?[31mSS'` while the awk
  published `PASS`) and PASSED on the CR row, naming exactly one side as wrong. When a differential
  says two readers disagree, **consolidate**, and let the measurement pick the side. **The classifier enforces that working too,
  by calling the SAME function the writer does (#3751 round 1).** `verdict` reads HAND-WRITTEN reports by design, and it used to accept any
  NON-EMPTY `performed-by`/`reason`/`evidence` — so `performed-by: nobody`, `reason: x`, `evidence: tbd`
  reached the token that PROCEEDS at the merge point while the writer would have refused all three. A
  non-emptiness test standing in for a validity test, and the same fact checked in two places with two
  strengths; a report asserting the token without usable working is now
  `NOT-RUN (report ungrammatical: …)`, naming the field and the defect. All six pipeline-gating
  agent definitions carry the matching report-of-record
  clause: the class is *spawns whose silence gates a merge*, so `flow-closer` (which owns the merge) and
  `sstable-developer` (which had queued work it never did) are in it beside the four reviewers.
  **The claim is about the CONSUMER and not about the agents, and stating it narrowly is the point:**
  naming a report path was effective for `spec-auditor` and `flow-closer` and did NOTHING for
  `rust-reviewer` (0 of 3, one of them told IN WRITING that an absent file would be recorded as a
  non-review) — and the mechanical reason surfaced while writing that clause, which is that
  `rust-reviewer` had **no write channel at all** (`Read, Glob, Grep`), so the contract was unsatisfiable
  by construction. It now carries `Write` for that one purpose; that grants nothing its siblings lacked,
  since three of the four "read-only" reviewers already carry `Bash` — **"read-only" here was always
  prose, never a mechanism.** Full record incl. the census, the tally and the limits:
  [`docs/development/review-stage-reporting.md`](https://github.com/pmcfadin/cqlite/blob/main/docs/development/review-stage-reporting.md).
- **Scoped re-cert, one full gate (issue #2087).** A roborev blocker that touches src re-certifies with
  `scripts/agent-gate.sh --lite` (blast-radius-scoped) + any diff-relevant parity/integration target — NOT
  a full gate. The single full gate of record runs **once**, immediately pre-merge; lite re-certs (their
  `MODE: lite` marker) are never the gate of record.
- **Severity-triaged findings (issue #2088).** Findings are classified per the
  [roborev severity rubric](https://github.com/pmcfadin/cqlite/blob/main/docs/development/roborev-severity.md):
  **blockers** (correctness, data-parity, no-heuristics, safety, wiring-evidence, security, any acceptance
  criterion) are fixed pre-merge; **nits** (style, naming, comment/doc polish, no-repro test suggestions)
  are batched into ONE linked follow-up issue at merge time and never trigger a re-verify round. When in
  doubt, blocker.
- **The disposable `flow-closer` owns the endgame (issue #2084/#2668).** `flow-implement` opens the PR, then
  spawns a per-issue `flow-closer` that runs the ONE full `scripts/agent-gate.sh` of record (via
  `run_in_background` + the summary-file pattern — it **never idle-waits**, which would trip the #1855 stall
  watchdog and orphan the gate; polling the summary file is mandatory on a hard 45-min deadline, with
  `grep -qE 'RESULT: (PASS|FAIL)'` — never a bare `grep -q` on the bare `RESULT:` token, which also matches the startup
  `RESULT: INCOMPLETE` liveness placeholder and would accept a just-launched gate as a verdict, #3041),
  the **C**
  intent audit, the final roborev pass, then merges on green and `flow-finalize`s. The closer has **no
  `Agent` tool**, so it never spawns directly: for **C** (and any src-design fix) it emits a structured
  `NEEDS-SPAWN` packet and ends its turn — the lead spawns `spec-auditor`/`sstable-developer` and re-invokes
  the closer with the result. It returns only a terminal packet (verdict, PR URL, summary-file path, ≤10 lines
  residual), so gate stdout and review churn die with its context instead of accreting in the lead session.
  Any src change after the full gate INVALIDATES it — the gate of record must postdate the final src change
  and rebase.
- **Division of labor.** An implementer subagent (`sstable-developer`) edits/commits/pushes and verifies
  with `--lite`/targeted tests **only** — it must **never** invoke the full gate.

Every gate invocation — full and `--lite` — uses the **summary-file redirect** by default
(`AGENT_GATE_SUMMARY_FILE=<path> … > gate.log 2>&1 < /dev/null`, then `cat <path>`); raw gate stdout is
never read into a persistent agent context (issue #2079). See the
[gate contract](/cqlite/agents-developing/gate-contract/) for the summary-file default and the
`accelerators:` line.

## Inter-issue reset for the lead (issue #2085)

The `flow-lead` is the only long-lived agent, so it compacts between issues: after each `flow-finalize` it
carries **zero prior-issue history** (board renders, gate summaries, roborev findings, PR bodies, and
Seam-1 spec renders are dropped — `spec-auditor` re-reads specs from `openspec/changes/<slug>/` anyway),
re-hydrates the **next** item from the **board alone**, and stays re-runnable from board + disk state at any
point (worktree, origin claim branch, issue/PR bodies, OpenSpec files, summary files, telemetry ledger).
Durable cross-issue lessons route to `MEMORY.md` / `process_improvements.md`, never the live window. The
same board-only rehydration rule applies to worker sessions (see the supervisor below).

## Machine setup + accelerators

A fresh machine that will run the pipeline should first run
`bash scripts/bootstrap-agent-machine.sh` (details in `docs/development/agent-machine-setup.md`): it
verifies the gate accelerators (`sccache`, `cargo-nextest`, modern bash — issue #1848), the datasets +
`CQLITE_DATASETS_ROOT`, `gh` auth + the `project` scope, and roborev's local config. **roborev is invoked
ONLY through the fail-closed wrapper `bash scripts/flow/roborev-review.sh --agent <agent> --model <model>
[--repo <abs-path>] [--base <ref>]`** (#2964) — fleet form `--agent codex --model gpt-5.6-sol`; the Claude
reviewer is `--agent claude-code --model claude-opus-5`. **BOTH `--agent` and `--model` are ALWAYS
required** (the wrapper rejects a missing one as a usage error; one alone inherits the mismatched
`.roborev.toml`-pinned model and fails as a silent-looking review outage), and the branch must be **pushed
first** — the wrapper asserts that and FAILs otherwise. Three direct-CLI forms are **NON-SANCTIONED**:
`roborev review --branch` **without an explicit `--repo`** (from a worktree it resolves against the ROOT
checkout), the two-positional commit-range form (its range base is git's empty tree), and a single-SHA
review (it reviews **one commit, not the branch**). Each can report clean having reviewed NOTHING — or, for
the single-SHA form, only the last commit — and a vacuous pass is textually identical to a genuine one.
Measured: **`--repo` is what makes `--branch` correct**, so the wrapper reviews the RANGE `<base>..HEAD` and
verifies BOTH endpoints against the job record (`reviewed-sha:` is a range, not a sha; `job-record:` reports
the record's completeness). Note too that **roborev drops exactly what its configured `exclude_patterns` pathspecs match — it makes no
code/non-code judgement** — so a docs-only diff cannot be roborev-certified at all. "docs-only" means a
**code-free CENSUS**, never a `docs/` path prefix: the `docs/reports/*-artifacts/` measurement harnesses
this repo ships by convention are executable code that IS reviewed, so a PR carrying them must be
certified like any other code change (#3229). The remedy that shipped is the **configuration**: a narrowed
prose/artifact deny-list (`*.md` plus artifact extensions scoped to artifact-bearing *directories*, never a
blanket `docs/**` — which is what swallowed 33 harness executables on PR #3222), measured at 72 `docs/`
executables reaching the reviewer and 0 markdown.
**NOTHING PREDICTS THE EXCLUSION SET PRE-ENQUEUE (#3283 configured, #3278 compiled-in).** A key that did
was built on #3229 and REMOVED by owner ruling: its false-PASS count was *increasing* across review rounds,
and **a guard with known documented false-PASSes is worse than no guard, because it invites reliance it
cannot support**. So a swallowed path — by configuration or by roborev's compiled-in lockfile/cache
deny-list (`**/Cargo.lock`, `**/go.sum`, `**/pnpm-lock.yaml`, …) — surfaces **after** the review under
**`prompt-content:`**, fail-closed, with a cause that names the symptom rather than the mechanism.
Practically: **if `prompt-content:` FAILs, suspect `.roborev.toml` first**; a lockfile-only dependency bump
is still **not** roborev-certifiable; and `prompt-content:` can never print a `PASS (0/0 …)`. Verdicts still
follow one rule — **FAIL where the author can act; NOTICE where only the information is actionable; never
silence** — and no key is exempt from the affirmation backstop: all six deterministic keys must be
affirmatively `PASS`, matched on the exact verdict token, never a prefix glob.
Note also that **a `.roborev.toml` change cannot certify itself**: roborev reads `exclude_patterns` from the
repo **root path** and snapshots it at daemon start, so a worktree edit is invisible and the demonstration
belongs after the merge — generally, *any PR whose subject is a config a daemon or gate reads from root
cannot certify itself*. **Any** non-PASS terminal `RESULT` —
`NOTHING-TO-REVIEW` included — is a failed review round and a blocked merge, never a clean pass. Verify
which reviewer a box can actually serve with `roborev check-agents`; why:
[roborev findings](/cqlite/agents-developing/roborev-findings/) + CLAUDE.md.

## Pipelining independent lanes (retro #1889)

The lead **pipelines** near-independent issues rather than serializing on long waits (a full gate is
15–25 min, plus CI and roborev round-trips):

- While one lane's full gate / CI / roborev runs, the lead advances **other independent lanes** —
  implementation and review stages overlap freely.
- Merge-on-green is **armed per PR** (it lands when green) rather than blocking the queue on each PR's CI.
- **Only the full-gate step serializes** across lanes (respecting the #1825 machine-wide cap and measured
  ~2-gate contention); everything else overlaps.
- Long waits use **scheduled wakeups**, never idle polling.

## Operational caveats

- **Subagent model pin.** The `model:` pinned in a subagent's frontmatter is not always accessible — always
  pass an explicit, accessible `model` (e.g. `opus`) when spawning, or the spawn fails.
- **GitHub REST resilience.** Board / `gh` operations run in bursts and can hit GitHub's secondary rate
  limits. Batch reads (one `gh project item-list` over per-item polls) and, on a `403`/secondary-limit
  response, back off and retry rather than failing the run.

## Self-improvement loop (telemetry + retro)

The pipeline measures itself so improvement is data-driven, not anecdotal — **sense → diagnose → improve**:

- **Sense.** `flow-finalize` stamps one record per delivery cycle (issue, pr) into the append-only
  ledger `docs/reports/delivery-telemetry.jsonl` (governed by
  `docs/reports/delivery-telemetry.schema.json`) using `scripts/delivery-telemetry.py record`. A
  reopened issue that ships more than once legitimately gets one record per shipped PR — retro
  aggregation by issue treats such multi-cycle issues as multiple deliveries, not one (issue #2314).
  The same holds for an issue that ships one or more **slices** while deliberately remaining OPEN
  (issue #3550): stamp each with `--slice`, which writes `closed_at: null` (the marker) and bounds
  `cycle_time_s` on the PR's `mergedAt` — the authoritative terminal timestamp of a slice — and `retro`
  reports those records as their own SLICE class rather than as completed issues. `--slice` asserts the
  issue was open **when the PR merged**, which its CURRENT state cannot decide (GitHub records an
  auto-close AFTER the merge, so an ordinary completed delivery and a late-stamped slice have
  indistinguishable timestamps). Since issue #3559 the tool decides it by replaying the issue's own
  **timeline** to the PR's `mergedAt`, and the rule is a **conjunction**: slice ⟺ the issue was OPEN at
  `mergedAt` **AND** this PR closes NOTHING. Both halves are permanent — every auto-closing PR's issue
  was *also* open at `mergedAt`, because the close is recorded afterwards, so only the PR's own
  `closingIssuesReferences` separates "open because the issue is never closing" from "open because the
  close lands five seconds later" (a slice PR closes NOTHING). A slice is therefore stampable after its
  issue has been closed or reopened — which is what unblocked the three owed #3393 records
  (#3407/#3429/#3467) — and is refused when the **last** `closed`/`reopened` event
  STRICTLY BEFORE `mergedAt` is a `closed`, because that delivery COMPLETED the issue and a later
  reopen does not change it. The *last* one decides, so a close FOLLOWED by a reopen before the merge
  leaves the issue open at `mergedAt` and is ACCEPTED. An
  event in the SAME SECOND as `mergedAt` is a third answer, neither before nor after: both GitHub
  timestamps are one-second resolution, so the tie is **unmeasurable** and is refused as such rather
  than resolved permissively.
  `--slice` is an operator **assertion**: the tool refuses it wherever it can be **disproved**, and
  where it cannot be, the assertion stands. One residual is genuinely **undecidable** and is not
  claimed: a completed delivery whose PR omits `Closes #N` and whose issue is closed by hand later is
  observationally identical to a genuine slice completed later by another PR, so the difference is
  intent — bounded by doctrine (`flow-implement` mandates `Closes #<N>`), not by mechanism. The two available
  workarounds are FORBIDDEN: closing the issue to satisfy the tool (a tool's data model must never
  decide whether a problem is recorded as solved), and hand-appending a line to the JSONL past the
  validator (the tool is the gate on the ledger's shape).
  Records carry **authoritative data only**: GitHub-derived
  timestamps (issue/PR open + merge + close → cycle time and coarse phase durations) plus run-observed
  counters — claim collisions, rebase/conflict events, agent-gate pass/fail + run count, roborev findings,
  and rework. A counter that was not observed is an **error**, never a fabricated `0` (no-heuristics
  mandate). `delivery-telemetry.py lint` schema-validates every line. **The stamp lands via a
  `telemetry-<N>` PR-in-worktree, not a direct push** — `main` blocks direct pushes (PR required for every
  commit, `enforce_admins=true`). `flow-finalize` branches a throwaway worktree off `origin/main`, appends
  the record (note `record` writes to the script's repo ledger, not `$PWD` — verify it lands in the
  worktree and leave root clean), and opens a telemetry-only PR that merges on its own green `required`
  check. The ledger is a hot append-only file: resolve any rebase conflict by **keeping all lines**, never
  dropping a peer's record. Never `git checkout` in the shared root to do this — a closer that switched
  root onto a telemetry branch and died stranded it off `main` and broke every concurrent session.
- **Diagnose.** On a cadence (per-epic or weekly) the manager runs `delivery-telemetry.py retro`, which
  ranks the recorded failure categories by a **documented weighted tally** (`Σ count × weight` — a
  deterministic policy table, not an inferred or learned model) and reports the single highest-cost
  recurring failure. Default is a dry-run print; `--file` files a `flow-meta` improvement issue, **deduped**
  against open `flow-meta` issues by a stable category marker.
- **Improve.** That `flow-meta` issue enters Ready and flows through the normal pipeline.

The `delivery-telemetry` agent-gate component (SKIP-aware on `python3`) covers the tool: schema
round-trip, lint-rejects-malformed, fixture-ledger → expected top failure, and dedupe.
