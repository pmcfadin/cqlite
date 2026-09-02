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
| `NOT-RUN` | sentinel-only, absent, unreadable, empty, or ungrammatical | 5 | **no** |
| `AUTHOR-PERFORMED` | a disclosed substitute with its working recorded | 6 | **only under §4** |

`NOT-RUN` carries a **cause** in parentheses — `no report written`, `report absent`, `report unreadable`,
`report empty`, `report ungrammatical: <what>`, `stage never opened` — because the operator action differs per cause and
one token for five states is the shape this issue is about.

Two rules make the grammar closed rather than prefix-tested (#3544's lesson): the token is reduced to its
first word and matched by **string equality**, and **any unrecognised value is `NOT-RUN`**, never
pass-through. `PASS-BUT-UNMEASURED` must not satisfy a `PASS*` test.

## §1 — `open`: the sentinel and the path

```
review-stage.sh open <kind> --issue <N> --agent <type> [--deadline-secs <S>] [--force]
```

- `<S>` is bounded AFFIRMATIVELY at `MAX_INT_DIGITS` (10) decimal digits with no leading zero
  (round 8). Being digits is not being COMPARABLE: `[ -gt ]` is a fixed-width int64 comparison that
  refuses a wider operand with a raw `integer expression expected` on stderr — outside the anchored
  block — and then takes the permissive branch, while `$(( ))` does not refuse at all but WRAPS
  SILENTLY, and a zero-padded value is read as OCTAL by `$(( ))` and DECIMAL by `[ ]`. Ten digits is
  ~317 years as a duration and the year 2286 as a unix epoch, with nine orders of magnitude of
  headroom under int64; `0` is accepted, because `deadline=0` is a legitimate emitter state. ONE
  predicate, `int_is_comparable`, gates every boundary where such a value reaches a fixed-width
  operation — argv, both operands of the elapsed subtraction (the clock's own reading included),
  both operands of the past-deadline comparison, the epoch `--force` copies forward, and the reopen
  counter.
- The path is `<repo-root>/.review-stage/issue-<N>/<kind>.<nonce>.md` inside the worktree,
  **DERIVED, not overridable, and NONCE-BOUND**. The per-open nonce is round 5's (J1) answer to a
  defect in this section as it was first built: `--force` reset the report and re-stamped `head-sha:`
  AT THE SAME PATH, so the previous, idle agent could wake up afterwards and write its old-tree
  verdict into that path, where it was paired with the new `head-sha:` — a commit nobody audited
  passing `premerge-assert.sh`. Since this whole mechanism exists BECAUSE delegated agents return
  late, that is the expected behaviour of the population it serves. So each open records a
  `report-nonce:` and the readers derive the path from THAT TOKEN — never from a path in a data file,
  which is the channel round 4 removed. A bare `<kind>.md` is still READ, for records written before
  the field existed (that version wrote exactly one report, at that name), and is never written. A
  resumed agent therefore holds a stale path and is STRUCTURALLY unable to write into the
  current report; a check could not deliver that, because the harm is a write.
  **Round 6 (K2) replaced a SCANNED GENERATION NUMBER with the GENERATED nonce**: the first design
  picked the next generation by walking the directory for an unused `<kind>.<gen>.md`, and a value
  chosen from what is on disk is a value two concurrent `open --force` calls can both choose — both
  printed `c.1.md`, and one agent's `FINDINGS` was overwritten by the other's `PASS`. The scan, its
  attempt bound and its exhaustion refusal are DELETED rather than locked: a lock would serialise a
  race the nonce removes while adding its own failure modes, and subtraction cannot introduce a false
  PASS. The nonce comes from `mktemp -u`'s name substitution (the source the write path's temporary
  name already uses) with no fallback, and no cryptographic strength is needed or claimed — it is a
  uniqueness token, not a secret. `reopen-count:` remains as the human-readable audit number.
  Superseded reports stay on disk as history, and nothing DEPENDS on their existence, so a deleted
  stage record cannot re-issue a path an older agent holds. A `[--report <path>]` override was specified here and shipped, and it is **REMOVED in
  round 4** — a DELIBERATE NARROWING of this design surface, recorded rather than quietly dropped. It
  was mandated by no requirement and used by NOTHING (measured by grep: no agent definition, no skill,
  no script, no call site — only this usage line and the test suite), and it was the caller-controlled
  component behind a finding CLUSTER across four review rounds: round 1's symlink walk and its
  extension-vs-directory ignore consequence, round 3's temp-path TOCTOU, and round 4's H2 (the raw
  path written into a LINE-oriented record, so a legal newline-bearing filename split and the reader
  took the PREFIX — which could name a DIFFERENT pre-existing report recording `PASS`) and H3 (the
  parent directory created BEFORE containment was verified, so a REFUSED outside-the-repository path
  still created directories outside the checkout). With the path derived, `<kind>` and `<issue>` —
  validated strictly at one boundary — are the whole path-input surface, and both findings are closed
  BY CONSTRUCTION rather than by a check. If a caller ever appears, re-add the flag WITH the hardening
  (CR/LF refused; containment verified BEFORE any `mkdir`), never as it was.
- **The path must be gitignored, verified with `git check-ignore -v`, fail-closed.** Not a convention —
  a measured one. #2926 fails a gate closed on any mid-run tree mutation, and a review stage routinely
  overlaps a running gate. A gitignored path is invisible to `tree-integrity` (which derives its identity
  from tracked content plus HEAD), and an untracked-but-**not**-ignored file shows as `??` and *would*
  dirty the run. A leading dot proves nothing: measured, `.frozen-work.md` is not ignored while `gate.log`
  is. So the script asks git rather than assuming, and refuses to write a path git does not confirm.
- Prefer the worktree to `/tmp`: it survives with the lane, a resuming session finds it without
  remembering a path, and this fleet has had `/tmp` watchdogs deleted by system cleanup.
- **A symlink at the write path (or any component under `.review-stage/`) is REFUSED, never followed**
  (#3751 round 1, F5) — `check-ignore` judges a LEXICAL path while a write follows links. **And the
  TEMPORARY file the write goes through is unpredictable and created exclusively** (#3751 round 3, G3):
  the name comes from `mktemp -u` and the file is created and opened in ONE `O_CREAT|O_EXCL` step
  (`set -C`), then written through the held descriptor and `mv -f -T`'d into place. **The `-T` is
  load-bearing and is REQUIRED, not attempted** (round 7, L2): a plain `mv -f` does not promise to
  replace the destination NAME, so a `dest` that is or BECOMES a directory (or a symlink to one)
  receives the temporary file INSIDE it while `mv` EXITS 0 — the write lands outside the verified path
  and the tool reports success. There is no fallback, which makes GNU coreutils a stated host
  precondition; a `mv` without `-T` fails the option parse, moves nothing, and the write REFUSES,
  naming the missing option. The first version used
  a predictable `.<name>.tmp.$$`, validated it and reopened it BY NAME — a TOCTOU a PEER LANE could win,
  since every lane here runs as one user under a shared HOME, so it was a non-invoker route and a
  defect. The window is REMOVED rather than narrowed: a check placed after a harmful effect can only
  report it, and the harm is a WRITE. The gitignore verification stays where it is because it is
  LEXICAL and is taken on the exact name about to be created; a failure to create the temp exclusively
  is a NAMED refusal with nothing written, never a fallback to a predictable name.
- **Re-opening an existing stage refuses** unless `--force`. A second spawn silently resetting the clock
  would make the deadline unreadable, and a re-spawn is exactly what a lane does when the first one idles.
- **The stage record carries the commit it was opened at (`head-sha:`), and `--force` RE-STAMPS it**
  (#3751 round 3, G1). The merge point requires that recorded sha to equal the certified one IN ADDITION
  to requiring this worktree's `HEAD` to: HEAD-equality binds the WORKTREE and is satisfied by
  construction, so it cannot see a STALE ARTIFACT — a `result: PASS` recorded before a further commit, an
  amend or a rebase persisted and certified the NEW tree. `spawned-at` is preserved across `--force` and
  `head-sha` is not, because the two answer different questions: elapsed-since-FIRST-spawn is the number
  that says a stage has produced nothing for 70 minutes, while a re-opened stage hands the re-spawned
  agent a fresh sentinel and it audits the tree that is there now. An unresolvable `HEAD` records the
  literal `unresolved` — an honest non-measurement, refused at the merge point by name, because `open`
  must still work in a checkout with no commits (a guard that reds on correct input is the guard agents
  learn to waive) while a non-measurement is never a pass.
- **The merge point rests on ONE OBSERVATION of that record** (#3751 round 9, N2). The `head-sha`
  binding was validated from one read while `review-stage.sh verdict` re-read the record to resolve
  which report is current, so a replacement in between yielded a verdict from a different GENERATION,
  possibly bound to a different commit, under a binding checked on the old one — measured, a success
  line naming `stage-head=<the validated sha>` beside a report from a generation carrying forty zeros.
  So `premerge-assert.sh` captures the record ONCE, parses `head-sha` from that capture rather than a
  second read, and requires it to be byte-identical before the token is parsed. A HANDOFF (resolving
  the report and passing it to `verdict`) is deliberately not the fix: it would rebuild from the other
  end the control channel round 4 (H2) deleted with `--report`.
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
`--performed-by author`, the ONLY accepted performer — `peer` was REMOVED in round 6 (K3): it was accepted and then reported under the token `AUTHOR-PERFORMED`, so a PEER audit was stated to be the diff AUTHOR's, and a peer who CAN audit writes the report of record instead, reaching a genuine `PASS`. Placeholders are refused the way `claim.sh --reason` refuses them (a bare
`why`/`todo`/`tbd`, an unsubstituted `<…>`) — a template pasted unfilled is not a disclosure. The token
is `AUTHOR-PERFORMED`, textually distinct from `PASS`, for the reason `WAIVED` is distinct in the roborev
wrapper: nobody grepping the passing token may read a substitute as the real thing.

Round 2 (B2) added the refusal to replace a RECORDED verdict without `--force`; round 9 (N1) made that
refusal PREVENT rather than REPORT. The check read the verdict and only then prepared and renamed its
replacement, so a verdict landing in that window was overwritten anyway — the same harm, now under a
guard. The observation is therefore re-verified immediately before the rename, on the report's BYTES
rather than its token (one `FINDINGS` replaced by another leaves the token equal), and any change refuses
under `--force` too. One `mv` of residual window remains and is declared at the check: no
compare-and-swap rename is reachable from a shell, and a lock cannot help because the counterparty is an
arbitrary agent writing the report with its own tooling and taking no lock.

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
- the ungrammatical, empty, unreadable, absent, and never-opened causes each asserted **by name**, not by exit code
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
