# Proposal: an absent review must not read as a clean one — the stage verdict becomes an artifact (issue #3751)

**Milestone:** maintenance / delivery-pipeline integrity · **Priority:** P1 · **Routing:** **design-driven**
— there is no external oracle for *"what must a delegated review stage report"*; the deliverable is a
contract plus a mechanism. · **Issue:** #3751 ·
**Refs:** #3041 (the `INCOMPLETE` launch sentinel), #3229 (affirmative measurement), #3312 (remove the
shared channel), #3465/#3616 (a check must sit at the merge point), #2926 (tree integrity), #2084
(flow-closer), #3688 (the mid-edit precursor fault)

## Why

Five lanes on 2026-08-31 and a sixth on 2026-09-01 spawned a review subagent that **produced no report at
all**. Nine spawns across four agent types. Every one was recorded by its lane as *not-run*, and in three
cases the hand-performed substitute **found a real defect the idle agent would have been credited for**.

The defect is not the agents' silence. It is that **silence has no representation**:

| doctrine already in this repo | what it guarantees | what a spawned review stage has |
|---|---|---|
| `RESULT: INCOMPLETE` written at gate launch (#3041) | "did not finish" can never be read as a verdict | **nothing** |
| roborev `review-completed` + `vacuity-tier1/2` + token accounting (#3312) | a review that received nothing cannot pass as one | **nothing** |
| affirmative measurement (#3229) | a positive verdict requires a measurement, never the absence of a bad signal | **nothing** |

A spawned `spec-auditor` that idles leaves **no artifact**, and "no artifact" is indistinguishable — to
any automated consumer — from "not yet finished". `flow-closer` treats **C** as merge-gating; if a closer
ever reads "no findings reported" as PASS, the stage certifies nothing while appearing to certify. The only
thing that has prevented that is six lanes independently choosing to record the stage as not-run. **That is
a discipline outcome, not a mechanism outcome**, which is precisely the shape the affirmative-measurement
rule exists to remove.

### The mechanism, verifiable from committed source (AC5)

Not a model behaviour. From `.claude/agents/*.md` on this branch:

```
rust-reviewer               tools: Read, Glob, Grep
spec-auditor                tools: Read, Grep, Glob, Bash
coverage-reviewer           tools: Read, Grep, Glob, Bash
compaction-parity-auditor   tools: Read, Grep, Glob, Bash
flow-closer                 tools: Read, Write, Edit, Bash, Glob, Grep
sstable-developer           tools: Read, Write, Edit, Bash, Glob, Grep
```

**Not one has `SendMessage`.** Their only route to the caller is the Agent tool's terminal result, so when
that result does not surface there is no fallback and the caller's natural recovery move — ask for it — is
*unavailable*. Two lanes proved it by asking; neither could have been answered. `TaskOutput` is no escape
(deprecated for agents, and its output is the full transcript, which would flood the caller).

So the fix space has exactly three members: give those agents a message channel, make terminal-result
delivery reliable, or **stop routing the verdict through a message at all**. Only the third is available
from inside this repository, and it is the one this repo's own doctrine already prescribes for a channel
that silently drops (#3312: *remove the shared channel; do not pick a rarer delimiter*).

## What changes

**The verdict of record for a delegated review stage becomes a FILE, pre-stamped with a sentinel at spawn
time, read by the consumer under a closed three-valued grammar.** `scripts/flow/review-stage.sh` owns it.

1. **`open`** creates the report path *before the agent is spawned*, pre-stamped
   `RESULT: NOT-RUN (no report written)` — the #3041 move exactly. Silence is now a **positive, readable
   state** instead of an absence, and it carries `spawned-at` and a `deadline`.
2. **`status`** answers AC2: elapsed, deadline, and whether the file is still sentinel-only — the way the
   gate prints `waiting for gate slot` rather than looking hung.
3. **`verdict`** answers AC1: `PASS` / `FINDINGS` / `NOT-RUN` / `AUTHOR-PERFORMED`, exit-coded, under a
   closed grammar where anything unrecognised, empty, or sentinel-only is `NOT-RUN` and **non-passing**.
4. **`record-author-performed`** answers AC4 under the owner's 2026-09-01 ruling: a self- or peer-performed
   substitute is recorded with its working and reported under a **distinct token**, never `PASS`, so nobody
   grepping for a passing verdict reads an author-performed audit as an independent one.
5. **`premerge-assert.sh` fails closed on it** (AC3), because #3465 and #3616 both taught that the check
   must sit *at the merge point*, not upstream of it. Whether C is required at all is **measured** from the
   branch (does an `openspec/changes/<slug>/` exist?), never taken on the caller's word.

## What this explicitly does NOT claim

**The report path is not a fix for the agents.** Measured across two prior sessions: naming an absolute
report path in the spawn prompt rescued `spec-auditor` and `flow-closer`, and did **nothing** for
`rust-reviewer` (0 of 3 spawns wrote the file, including one told in writing that an absent file would be
recorded as a non-review). This change makes the **consuming verdict** correct — an absent review is
*reported as absent*, with its elapsed time, and cannot reach a merge. It does not make a flaky agent
deliver. The operational answers to that remain the owner's ratified ones: prefer a **peer** C, prefer
oracles that write durable artifacts (the roborev wrapper), and disclose a substitute as author-performed.

## Non-goals

- Giving the agents `SendMessage`, or otherwise changing harness-level spawn delivery — out of repository.
- Any change to what a review stage *concludes*. This governs how its verdict is recorded and read.
- Retro churn on the deliveries already shipped with a disclosed self-C (owner ruling: they stand).

## Lead rulings recorded (2026-09-01T18:19:24Z, issue thread) — subject to owner ratification at Seam 1

**Q1 = (a): the scope is ANY pipeline-gating spawn, not only the read-only reviewers.** The lead's reason
is the issue's own principle: *"an absent review must never read as a clean one"* is about the **class** of
spawn whose silence gates a merge, and `flow-closer` is the strongest member of that class — it **owns the
merge** and lacks `SendMessage` identically. So **six** agent definitions, not three. Recorded here rather
than split into a separate change, per the ruling, and **flagged as the one scope question the owner is
ratifying** at Seam 1 (the filed issue's title and table name read-only reviewers).

**Q2 = (a): `premerge-assert --c-verdict` enforces fail-closed immediately — no advisory phase.** The lead
distinguished this from #3650, which earned its advisory-first because the blast-radius *definition* was
unmeasured and the obvious one was falsified by measurement; here the remedy is mechanical and **one
argument per lane**, and *a known one-command remedy does not need a soak*.

**Landing coordination with #3752 (a rebase silently voids a roborev certification).** That issue adds a
`--roborev-block`-shaped requirement to the SAME script for the SAME reason — a merge-gating stage bound to
the head but not to the certification. Two independent arity changes to `premerge-assert.sh` would make
every in-flight lane re-certify **twice**. Per the ruling the two land coordinated so lanes pay **ONE**
re-certification visit. Sequencing is a landing-order question, not a design one: the argument name, the
usage text and the fail-closed semantics here are chosen to compose with a sibling required flag rather
than to assume this change lands first.
