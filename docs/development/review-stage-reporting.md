# Review-stage reporting: why an absent review must not read clean (issue #3751)

**This is the AC5 root-cause record.** It states, from committed source and from measured
sessions, why a delegated review stage could produce nothing and have that read as a clean
review — and, just as importantly, **what this change does NOT fix**. The narrow claim is the
true one, and it is about the CONSUMER, not about the agents.

- Mechanism: `scripts/flow/review-stage.sh` (`open` / `status` / `verdict` /
  `record-author-performed`)
- Consumer: `scripts/flow/premerge-assert.sh --c-verdict <path|AUTO>`
- Tests: `scripts/tests/test_review_stage.sh`, `scripts/tests/test_premerge_assert.sh`
  (Case 44), both under the gate's `tooling-tests` component
- Design: `openspec/changes/absent-review-must-not-read-clean/design.md`

---

## 1. The channel census — derived from the definitions, not asserted

`.claude/agents/` holds **8** agent definitions. Seven carry an explicit `tools:` list;
`flow-lead.md` declares no `tools:` key at all (it is the lead persona, not a spawned
specialist). Re-derive this table before trusting it — it is a claim about files, and a claim
about files decays exactly like a comment:

```bash
for f in .claude/agents/*.md; do
  printf '%-28s %s\n' "$(basename "$f" .md)" "$(grep -m1 '^tools:' "$f" || echo '(none declared)')"
done
grep -rc 'SendMessage' .claude/agents/*.md    # every file: 0, except where #3751's clause NAMES it
```

| agent | `tools:` | can it write a file? |
|---|---|---|
| `compaction-parity-auditor` | `Read, Grep, Glob, Bash` | yes, via `Bash` |
| `coverage-reviewer` | `Read, Grep, Glob, Bash` | yes, via `Bash` |
| `flow-closer` | `Read, Write, Edit, Bash, Glob, Grep` | yes |
| `flow-lead` | *(none declared)* | n/a — the lead, not a spawned stage |
| `rust-reviewer` | `Read, Glob, Grep, Write` | **only since #3751** — see §4 |
| `spec-auditor` | `Read, Grep, Glob, Bash` | yes, via `Bash` |
| `sstable-developer` | `Read, Write, Edit, Bash, Glob, Grep` | yes |
| `test-validator` | `Read, Bash, Glob, Grep` | yes, via `Bash` |

**Not one of them has `SendMessage`.** Before this change the string appeared nowhere in that
directory at all. That is the root cause, and it is not a model behaviour: **the only route from
a spawned stage back to its caller is the Agent tool's terminal result**, so when that result
does not surface there is no fallback and the caller's natural recovery move — ask the agent for
its findings — is *unavailable*. `lane-3634` proved that the hard way: it sent `rust-reviewer` a
detailed written request for its report, and the agent *could not have answered if it wanted
to*.

Two escapes that are not escapes, recorded so nobody re-proposes them:

- **`TaskOutput`** is deprecated for agents, and its output is the full transcript JSONL — it
  would flood the caller's context rather than deliver a verdict.
- **"Instruct the agent harder."** Tried, in writing, by at least three lanes, including
  progressively shorter deliverables (a full table → six lines → *a single literal string to echo
  back*) and an explicit permission to answer partially. None arrived. A prompt instruction
  cannot create a tool.

So the fix space was only ever three options: grant `SendMessage`, make terminal-result delivery
reliable, or **have the stage write its verdict to a FILE the caller reads**. Only the third is
available from inside this repository, and it has a property the other two do not: an artifact is
**durable across the caller's own context loss**, which on this fleet is routine.

---

## 2. The measured instance tally

From the #3751 issue thread (`gh issue view 3751 --json comments`), 2026-08-31 → 2026-09-01. Read
the thread for the full write-ups; this table is the census, and every row is a spawn that
**produced no report**:

| lane | agent | outcome |
|---|---|---|
| `lane-3629` | `spec-auditor` | idle, no verdict → hand C |
| `lane-3629` | `rust-reviewer` | idled **twice** |
| `lane-3515` | `rust-reviewer` | idled ~2 h |
| `lane-3544` | `spec-auditor` | idled twice, **including after a direct re-ask** → self-C |
| `lane-3634` | `sstable-developer` | idled twice; the second time its queued work was **not done** |
| `lane-3634` | `rust-reviewer` | idled twice, **no report ever** |
| `lane-3634` | `flow-closer` | idled **three times**; the lead drove the whole endgame by hand |
| `lane-3725` | `spec-auditor` (`C-3725`) | idled 3x across **3** requests |
| `lane-3725` | `spec-auditor` (`C2-3725`) | idled 2x, including past an explicit one-string escape hatch |

`lane-3725` additionally reports that of **seven** subagents in one lane, *none* reliably
delivered a report: one delivered after two explicit requests, two implementers did the work
(their commits are in the PR) and never reported it, and a reviewer plus both auditors delivered
nothing at all. Its conclusion is the operational one worth carrying: **the instruction channel
works and the report channel does not**, so a lead must verify from artifacts (`git log`,
`git diff`, running the suites) rather than consume verdicts.

**Four facts this tally establishes, each of which shaped the design:**

1. **A re-ask does not recover it.** Two lanes tried; neither got a verdict. This is a terminal
   state that *presents as "still working"*.
2. **It is not limited to read-only reviewers.** The two worst instances were write-capable —
   `sstable-developer` (queued work undone) and `flow-closer` (**it owns the merge**). That is
   why Q1 was ruled (a) and all six pipeline-gating definitions carry the clause. Note the
   asymmetry, though: a write-capable agent leaves **disk evidence** a lane can verify
   independently, so for those two the artifact is *corroboration*; for the four read-only agents
   there is **no other artifact at all**.
3. **An idle notice is strictly WEAKER than the gate's `INCOMPLETE` sentinel** — at least the
   sentinel names itself a non-verdict. Reading an idle notice as a completion report is the
   exact false certification this change prevents.
4. **The only reason this produced no false certification is discipline, not mechanism.** Nine
   for nine, the lanes recorded the stage as not-run and disclosed it rather than banking it as
   clean — `lane-3629`: *"an author's hand audit is not an independent one; weight it
   accordingly"*; `lane-3544`: *"an audit I performed and showed my working for is auditable,
   whereas an absent one is not."* Both sentences are now load-bearing text in
   `review-stage.sh`'s `record-author-performed`.

**And one recorded NON-instance, because the boundary is part of the record.** `lane-3414`'s two
subagents ran 18 h 53 m and 17 h 18 m and were offered as *"the strongest evidence"* for this
issue. **The lane declined, correctly**: both reported substantively (39 of 39 commits; 3
blockers + 9 nits, one of which caught three end-to-end cases silently disabled by a drifted
warning count). Their defect is *no exit after completion*, which is duration, not silence.
Filing it here would have widened #3751 to two unrelated causes. Do not add
long-running-but-productive agents to this pattern.

---

## 3. THE LIMIT — naming a report path is NOT a fix for the agents

**State this before anything else when reporting on this change.** What #3751 delivers is a
correct **consuming** verdict: an absent review is *reported* as absent, with its elapsed time
and a named cause, and it cannot reach a merge. It does **not** deliver reliable agents.

Measured, and recorded in `design.md` §5: naming a report path in the spawn prompt was effective
for `spec-auditor` and `flow-closer` in prior sessions, and did **nothing** for `rust-reviewer` —
**0 of 3** (`lane-3629`, `lane-3515`, `lane-3634`), and **one of those three was told in writing
that an absent file would be recorded as a non-review** and still delivered nothing. A mechanism
that made flaky agents deliver would be a different change, in a different repository.

Scope of that measurement, stated so it is not over-read: it is the lane's own session record.
The nine-row tally in §2 above does not record, per spawn, whether a report path was named, so
"effective for `spec-auditor`" rests on the design record and not on the thread — where four
`spec-auditor` spawns produced nothing.

Three further declared limits of the mechanism itself:

- **`verdict` establishes that a VERDICT WAS RECORDED, never that a review was PERFORMED.** A
  report whose only content is `result: PASS` reads as `PASS` — read at COLUMN ZERO: the report
  body is author-controlled text that carries example verdict lines BY DESIGN (the pre-stamped
  sentinel has to show the agent the spelling), so an indented, quoted or bulleted copy is DATA.
  While indentation was tolerated the template's own examples were grammatically valid records
  held off only by `grep -m1` ORDER, and deleting the column-zero sentinel then appending a
  verdict read the TEMPLATE's `PASS` — measured, #3312's family: an artifact that DESCRIBES the
  escape hatch becoming it. The examples now also sit behind a `| ` gutter, so neither the anchor
  nor the rendering is load-bearing alone. **And there must be EXACTLY ONE such line** (round 3,
  G2): anchoring without COUNTING still left `grep -m1` deciding by ORDER, so a stale
  `result: PASS` followed by an APPENDED `result: FINDINGS` classified as PASS and a merge
  proceeded over recorded blocking findings. Several column-zero records is `NOT-RUN`, in EITHER
  order — a last-wins read is no better than a first-wins one, so the refusal comes from the
  COUNT. That fix is a **CONSOLIDATION**: `premerge-assert.sh`'s `_c_verdict_awk` was already
  counting its own anchored `REVIEW-STAGE: ` lines, so two readers of ONE shape had diverged
  TWICE in two rounds — once per axis, each time with a reviewer naming one side — and patching
  the named side is what let the second divergence happen. Their agreement is now mechanically
  checked: `scripts/tests/test_premerge_assert.sh` §44g drives BOTH readers over ONE shared table
  of adversarial shapes and asserts they agree per row AND reach the EXPECTED disposition
  (agreement alone is satisfiable by both being wrong identically, which is the state the section
  exists to detect). Judging whether the working is
  real is a human's job — and for the author-performed substitute, requiring the working to be
  recorded is the whole point.
- **The deadline is advisory and changes nothing.** A late report is still a report; a stage
  silent inside its deadline is still `NOT-RUN`. Letting a clock decide would add a clock to a
  question already answerable from content, and would fail a slow-but-real review.
- **With an explicit `--c-verdict <path>`, `premerge-assert.sh` verifies the verdict's grammar
  and token, not that the stage belongs to THIS issue.** The grammar check is the FULL emitted
  line — the stage KIND must be `c`, and `elapsed=`/`deadline=`/`agent=`/`report=` must each
  appear exactly once — so a sibling stage's `PASS` line (a `rust-review` verdict, say) can no
  longer certify C; what it cannot check is the ISSUE, because the line carries no sha. The
  report path is printed on the success line so a human can see which stage answered. `AUTO` is
  the intended form because its binding is MECHANICAL: it locates the stage in this worktree,
  refuses two stage records as ambiguous, and applies **TWO independent bindings, because they
  answer different questions**. (a) This worktree's **HEAD must EQUAL the certified commit** —
  every lane on this box is a worktree of ONE shared `.git`, so a peer lane's certified commit
  RESOLVES from any lane; resolvability is not provenance (#3616's peer-artifact class). Rule 1
  asserts `headRefOid` == certified, so HEAD == certified binds the local artifact to THIS PR
  transitively. (b) The **stage RECORD's own `head-sha:` must equal the certified commit** — (a)
  binds the WORKTREE and cannot see a STALE ARTIFACT, because a lane stands at the very commit
  it is certifying BY CONSTRUCTION: a `result: PASS` recorded before a further commit, an amend
  or a rebase persisted in `.review-stage/` and certified the NEW tree (#3751 round 3, G1). So
  `open` resolves `HEAD` and records it in the stage record, and `--force` **RE-STAMPS** it
  (deliberately unlike `spawned-at`, which is preserved because elapsed-since-FIRST-spawn is the
  number that says a stage has produced nothing for 70 minutes). A record with **no**
  `head-sha:`, **several** of them, or a value that is not a 40-hex sha is a NAMED REFUSAL and
  never a skip — an older record predating the field must not be readable as certifying. The
  fail-closed direction is deliberate: this is the gate-of-record rule (any change after the
  gate INVALIDATES it) applied to the intent audit, and an audit of an older tree may not
  certify a newer one. The remedy every one of those refusals prints is the same: re-open the
  stage with `--force` at this commit and re-run C.
- **A partially-written `open` cannot publish a stale verdict (round 4, H1).** The two files are
  not writable atomically together, so the ORDER decides which partial state is reachable — and
  with the stage record written FIRST, the newly-stamped `head-sha:` sat beside the PREVIOUS
  report, so binding (b) was satisfied by the new commit while the verdict read was a `PASS` from
  an audit of the OLD tree. Measured: killed between the two writes, `verdict` reported
  `RESULT: PASS` exit 0 for a tree nobody had audited, permanently. So the REPORT is reset to the
  sentinel FIRST and the **stage record is written LAST, as the publication marker**: no record
  reads as `stage never opened`, a record beside a sentinel reads as `no report written`, and
  every partial state is a non-verdict. A check could not have delivered this — the harm is a
  WRITE, so the control has to be that the pairing is never REACHED.

---

## 4. The finding that came out of writing the clause: an unsatisfiable contract

While adding the report-of-record clause to the six definitions, `rust-reviewer`'s tool list was
`Read, Glob, Grep` — **no `Write`, no `Bash`, therefore no write channel of any kind**. The
clause was **unsatisfiable by construction** for the one agent with the worst measured record.

That is the mechanical explanation of the 0-of-3 above: **the agent was not ignoring the
instruction; it could not comply.** Shipping a contract that cannot be met is the
false-assurance shape this issue exists to remove, so the capability was granted rather than the
clause weakened — `Write`, for exactly one purpose, with that constraint stated in the definition
beside the capability.

Two things follow that are worth more than the fix:

- **"Read-only" in this pipeline has always been a PROSE CONVENTION, never a mechanism.**
  `spec-auditor`, `coverage-reviewer` and `compaction-parity-auditor` all carry `Bash`, which can
  write anything. So `rust-reviewer` gaining `Write` grants nothing its siblings did not already
  have, and a narrow-looking tool list must not be read as the enforcement it is not.
- **Before requiring an agent to produce an artifact, check that its tool list permits one.** The
  generalisation: a contract on a delegated party is only as real as that party's capability to
  satisfy it, and the capability is committed source you can grep.

---

## 5. How to use it

```bash
# BEFORE the spawn — pre-stamp the sentinel and get the paste-ready clause
bash scripts/flow/review-stage.sh open c --issue 1234 --agent spec-auditor
#   ... paste the printed clause VERBATIM into the spawn prompt (the paraphrase is what
#       varied across the measured sessions) ...

# WHILE waiting — advisory only, never a verdict
bash scripts/flow/review-stage.sh status c --issue 1234

# AFTER — the verdict of record. 0 PASS / 4 FINDINGS / 5 NOT-RUN / 6 AUTHOR-PERFORMED
bash scripts/flow/review-stage.sh verdict c --issue 1234

# AT THE MERGE POINT — required, and routing is MEASURED from the certified tree
bash scripts/flow/premerge-assert.sh <pr> <certified-sha> <gate-summary> --c-verdict AUTO
```

`NOT-RUN` always carries one of six named causes — `no report written`, `report absent`,
`report unreadable`, `report empty`, `report ungrammatical: <what>`, `stage never opened` — because the operator
action differs per cause, and one token for five states is the collapse this issue is about.
Everything is written under `.review-stage/`, whose ignore status is **verified with
`git check-ignore`, fail-closed**, so a stage opened mid-run cannot dirty a running gate of
record (#2926) or make `premerge-assert.sh` refuse on `dirty: yes` (#3648).

**And a SYMLINK at the report path, at the `.stage` path or at ANY component under `.review-stage/`
is REFUSED, never followed (#3751 round 1)** — `check-ignore` judges a LEXICAL path while a WRITE
follows links, so an ignored-but-symlinked report clobbered a TRACKED file and reported `OPEN-OK`
(measured); the writes themselves go through an UNPREDICTABLE same-directory temporary file
(`mktemp -u`) CREATED AND OPENED IN ONE STEP under `set -C` — i.e. `O_CREAT|O_EXCL` — then written
through the ALREADY-OPEN DESCRIPTOR and `mv -f`'d into place (#3751 round 3, G3), so a concurrent
reader never sees a half-written `result:` line. **The first version was a TOCTOU**: the temp path
was a PREDICTABLE `.<name>.tmp.$$`, validated and then REOPENED BY NAME, and a PEER LANE could
plant a symlink in that window — every lane on this box runs as one user under a shared HOME, so
this is a NON-INVOKER route and therefore a defect — making the write clobber the link's target
while the following `mv` installed the link as the report and reported success. **The window is
REMOVED, not narrowed**, because a check placed after a harmful effect can only REPORT it and the
harm here is a WRITE: there is no predictable name to plant at, `O_EXCL` refuses an existing path
INCLUDING a symlink (dangling or not — measured, and without creating its target), and no path is
re-resolved between validation and writing. There is deliberately no post-write check that the
file written is the file created; that is the "notice the clobber afterwards" shape this replaces.
The gitignore verification keeps its place because it has no window of its own — it is LEXICAL and
is taken on the EXACT name about to be created — and the temp's symlink walk is gone for a stated
reason rather than by omission: the temp lives in the destination's own directory, whose components
the destination's walk has just checked, and its leaf cannot be a followed link under `O_EXCL`.

If no independent audit can be obtained, the **sanctioned fallback** is to record the substitute
*with its working* — never a hand-asserted pass:

```bash
bash scripts/flow/review-stage.sh record-author-performed c --issue 1234 \
  --reason 'no peer lane available; C performed by hand against the spec deltas' \
  --evidence docs/round-artifacts/issue-1234-hand-c-audit.md \
  --performed-by author
```

That reports the DISTINCT token `AUTHOR-PERFORMED`, never `PASS`, and `premerge-assert.sh`
prints it on its own `PREMERGE: C-VERDICT` line — never folded into `PREMERGE: OK` — for the same
reason the roborev wrapper's `WAIVED` is textually distinct from `PASS`: **nobody grepping the
passing token may read a substitute as the real thing.**

**The classifier holds a HAND-WRITTEN report to the same bar, because it calls the same function
the writer does (#3751 round 1).** `verdict` reads reports the writer never produced — that is what
a report of record IS — and it used to accept any NON-EMPTY `performed-by`/`reason`/`evidence`, so
`performed-by: nobody`, `reason: x`, `evidence: tbd` all reached the token that PROCEEDS at the
merge point while `record-author-performed` would have refused each one. That is a non-emptiness
test standing in for a validity test, and the same fact checked in two places with two strengths.
One judgement (`author_working_defect`) now has two RENDERINGS — a usage error naming the flag for
the writer, a `NOT-RUN (report ungrammatical: …)` cause naming the field for the classifier — and
two renderings cannot drift into two strengths.
