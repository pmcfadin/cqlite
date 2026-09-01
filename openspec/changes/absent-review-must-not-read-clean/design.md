# Design: review-stage verdict as an artifact (issue #3751)

## The one decision everything else follows from

**The stage artifact is created BEFORE the agent is spawned, carrying a non-verdict sentinel.**

This is #3041's mechanism, transplanted. The gate writes `RESULT: INCOMPLETE (gate did not finish)` into
its summary file *at launch*, so a reader can never mistake a just-launched run for a certified one. A
review stage today writes nothing at any point, so its reader has only absence to reason from — and every
consumer of an absence has to *choose* how to read it. Five lanes chose correctly; nothing required them to.

Pre-stamping converts the question from *"is there a report?"* (two-valued, and the permissive answer is
the dangerous one) to *"what does the report say?"* (three-valued, with the unmeasured state named).

## Verdict grammar (closed)

One line, emitted by `verdict`:

```
REVIEW-STAGE: <kind> RESULT: <token> elapsed=<secs> deadline=<secs> agent=<type> report=<abs-path>
```

| token | meaning | exit | may a merge proceed? |
|---|---|---|---|
| `PASS` | the agent wrote a report and it records no blocking finding | 0 | yes |
| `FINDINGS` | the agent wrote a report recording ≥1 blocking finding | 4 | no |
| `NOT-RUN` | sentinel-only, absent, empty, or ungrammatical | 5 | **no** |
| `AUTHOR-PERFORMED` | a disclosed substitute with its working recorded | 6 | **only under §4** |

`NOT-RUN` carries a **cause** in parentheses — `no report written`, `report absent`, `report empty`,
`report ungrammatical: <what>`, `stage never opened` — because the operator action differs per cause and
one token for five states is the shape this issue is about.

Two rules make the grammar closed rather than prefix-tested (#3544's lesson): the token is reduced to its
first word and matched by **string equality**, and **any unrecognised value is `NOT-RUN`**, never
pass-through. `PASS-BUT-UNMEASURED` must not satisfy a `PASS*` test.

## §1 — `open`: the sentinel and the path

```
review-stage.sh open <kind> --issue <N> --agent <type> [--deadline-secs <S>] [--report <path>]
```

- Default path: `.review-stage/issue-<N>/<kind>.md` inside the worktree.
- **The path must be gitignored, verified with `git check-ignore -v`, fail-closed.** Not a convention —
  a measured one. #2926 fails a gate closed on any mid-run tree mutation, and a review stage routinely
  overlaps a running gate. A gitignored path is invisible to `tree-integrity` (which derives its identity
  from tracked content plus HEAD), and an untracked-but-**not**-ignored file shows as `??` and *would*
  dirty the run. A leading dot proves nothing: measured, `.frozen-work.md` is not ignored while `gate.log`
  is. So the script asks git rather than assuming, and refuses to write a path git does not confirm.
- Prefer the worktree to `/tmp`: it survives with the lane, a resuming session finds it without
  remembering a path, and this fleet has had `/tmp` watchdogs deleted by system cleanup.
- **Re-opening an existing stage refuses** unless `--force`. A second spawn silently resetting the clock
  would make the deadline unreadable, and a re-spawn is exactly what a lane does when the first one idles.
- Prints, on stdout, the absolute path **and the exact clause to paste into the spawn prompt**, so the
  contract reaches the agent verbatim rather than being paraphrased per lane.

## §2 — `status`: the deadline is visible (AC2)

```
review-stage.sh status <kind> --issue <N>
```

Prints elapsed, the deadline, and the file's state. Past the deadline while still sentinel-only, it says
so **and names the elapsed time and the fact that nothing was produced** — the gate's `waiting for gate
slot` idiom: a stage that is waiting must not be indistinguishable from one that is hung.

The deadline is **advisory by design**. It changes what is *reported*, never the verdict: a report that
arrives late is still a report, and a stage that is silent inside its deadline is still `NOT-RUN`. Making
the deadline decide the verdict would add a clock to a question that is already answerable from content.

## §3 — the consumer: `premerge-assert.sh` fails closed (AC3)

New flag, and **its absence is a usage failure**, not a skip:

```
premerge-assert.sh <pr> <certified-sha> <gate-summary> [<delta-summary>] --c-verdict <path|AUTO>
```

- `AUTO` (the intended form) makes the script **measure** whether C is required: an
  `openspec/changes/<slug>/` present on the branch ⇒ design-routed ⇒ **C is required**, and an absent or
  `NOT-RUN` verdict REFUSES the merge. No such directory ⇒ oracle-driven ⇒ C is not applicable, and the
  script says so affirmatively (`c-verdict: NOT-APPLICABLE (no openspec change on branch)`).
- **The routing question is answered from committed state, not from the caller's word.** A caller-supplied
  "C doesn't apply here" is exactly the escape hatch this issue is about; a directory listing is a
  measurement.
- `AUTHOR-PERFORMED` is accepted **only** when §4's form is satisfied, and is reported under its own token
  on the `PREMERGE:` line — never folded into `OK`. A reader grepping for a clean pre-merge must be able to
  see that the intent audit was performed by the diff's author.
- A missing `--c-verdict` is exit 3 (usage), which breaks a caller loudly and on purpose — the #3465
  precedent. Silently defaulting to "not required" would reproduce the defect in the enforcer.

## §4 — `record-author-performed` (AC4)

The owner's 2026-09-01 ruling, mechanised: peer-C preferred, self-C is the sanctioned **fallback only**,
never recorded as independent. Required form, adopting `lane-3629`'s wording verbatim:

> an author's hand audit is not an independent one; weight it accordingly

and `lane-3544`'s reason, which is why the fallback is sanctioned at all:

> an audit I performed and showed my working for is auditable, whereas an absent one is not

So the recording **requires the working**: a substantive `--reason`, a named `--evidence` artifact, and a
`--performed-by author|peer`. Placeholders are refused the way `claim.sh --reason` refuses them (a bare
`why`/`todo`/`tbd`, an unsubstituted `<…>`) — a template pasted unfilled is not a disclosure. The token
is `AUTHOR-PERFORMED`, textually distinct from `PASS`, for the reason `WAIVED` is distinct in the roborev
wrapper: nobody grepping the passing token may read a substitute as the real thing.

## §5 — the agent side, and the limit of what it buys

Every agent whose completion is a pipeline gate gains a report-of-record clause in its definition, and
every spawning skill gains the `open`-then-paste step. Per `lane-3634`'s scope correction this is **not**
limited to the read-only reviewers: `flow-closer` (which owns the merge) and `sstable-developer` (which had
a queued task it never performed) lack the channel identically. **That widening is a scope change to the
filed issue and is Seam-1 business, not the lane's** — flagged in the approval request.

**Stated limit, because the narrow claim is the true one:** naming a report path rescued `spec-auditor` and
`flow-closer` in prior measured sessions and did nothing for `rust-reviewer` (0/3, one of them told in
writing that an absent file would be recorded as a non-review). This design's guarantee is therefore about
the **consumer**, not the agent: an absent review is reported as absent, with elapsed time, and cannot
reach a merge. A mechanism that made flaky agents deliver would be a different change, in a different repo.

## §6 — the test, with a positive control (AC1)

`scripts/tests/test_review_stage.sh`, enrolled in the existing `tooling-tests` roster (no new gate
component, so `agent-gate.components` is unchanged):

- **the AC1 case**: `open` a stage, spawn nothing, ask for the `verdict` ⇒ `NOT-RUN`, non-zero exit, and
  `premerge-assert.sh` with that verdict REFUSES.
- **a positive control**: a real report ⇒ `PASS`, exit 0, and the merge assert proceeds. Without it, a
  script that always answered `NOT-RUN` would pass the suite — a guard that cannot green vacuously is the
  standing requirement here.
- the ungrammatical, empty, absent, and never-opened causes each asserted **by name**, not by exit code
  alone: five states behind one exit code is the collapse this issue is about.
- `AUTHOR-PERFORMED` accepted with the full form and refused for each missing element and each placeholder.
- a **case floor** (#3544), because a span-replacing edit silently deleted four cases from a suite that
  then reported `failed: 0`.

## Rejected alternatives

| alternative | why not |
|---|---|
| Instruct agents harder to report back | Tried, in writing, by two lanes. The tool to comply does not exist. |
| Read the agent's transcript via `TaskOutput` | Deprecated for agents; the output is the full JSONL and floods the caller. |
| Infer "clean" from an idle notice | The exact false certification this issue exists to prevent. An idle notice is *weaker* than `INCOMPLETE`: at least the sentinel names itself a non-verdict. |
| Enforce in `flow-closer`'s prose only | Prose is what already failed; five lanes complied and nothing required them to. And a check must sit at the merge point (#3465/#3616). |
| Let the deadline decide the verdict | Adds a clock to a question answerable from content, and would fail a slow-but-real review. |
| Let the caller declare C not-applicable | A caller-supplied exemption is the escape hatch; routing is measurable from the branch. |

## §7 — scope, as ruled (Q1 = widen) and what it costs

The mechanism is agent-agnostic by construction — `open`/`status`/`verdict` know a stage kind, an issue and
a path, never an agent's tool list — so widening is a change to **how many definitions carry the clause**,
not to the design. The six pipeline-gating spawns:

| agent | why its silence gates something |
|---|---|
| `spec-auditor` | **C**, merge-gating, nothing else in the pipeline substitutes for it |
| `rust-reviewer` | review-first round on the lite-green diff |
| `coverage-reviewer` | test-quality sign-off (`docs/development/pm-operating-loop.md` "done" definition) |
| `compaction-parity-auditor` | parity-gap audit where it is the routed oracle |
| `flow-closer` | **owns the merge**; idled three times mid-endgame holding the gate of record |
| `sstable-developer` | idled with queued work undone; its silence was read as progress |

The last two are why the widening matters: they are the measured **worst** instances, and a mechanism
scoped to read-only reviewers would have left the merge-owning stage uncovered while the tool to cover it
sat in the same file. Note the asymmetry that makes them different in kind, not just in tools — a
write-capable agent leaves **disk evidence** (commits, files) that a lane can verify independently, so for
those two the artifact is corroboration; for the four read-only agents there is **no other artifact at
all**, which is why the stage verdict has to be one.

## §8 — composing with #3752 on the same script

#3752 binds `premerge-assert.sh` to the roborev certification (a rebase rewrites the reviewed commit, so a
truthful "roborev: PASS" can describe a commit that is not being merged). Same script, same shape, same
reason. Two consequences for this design, both deliberate:

- **No assumption about landing order.** `--c-verdict` is a named flag, not a positional, so it composes
  with a sibling required flag in either order; the usage line is written to be extended rather than
  replaced, and its exit-3-on-omission does not depend on being the only required flag.
- **One re-certification visit for in-flight lanes**, which is a landing-order decision for the lead, not
  a design constraint discharged here.
