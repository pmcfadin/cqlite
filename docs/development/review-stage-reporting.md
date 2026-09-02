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
- **`status`'s `state=` is one value PER CAUSE (round 4, H4).** The per-cause contract exists
  because the operator action differs per cause, and the status mapper was throwing two of them
  away: every unenumerated cause fell through to `report-ungrammatical`, so `report unreadable`
  (fix: `chmod`) sent the operator to the agent, and a SELF-RECORDED
  `result: NOT-RUN (ran out of context)` — a perfectly grammatical report in which the agent said
  WHY — was called ungrammatical, hiding the one actionable fact. Both now have their own state
  (`report-unreadable`, `not-run-self-reported`) and their own STATUS-NOTE; All SEVEN reachable
  causes were checked at the time, not just the reported one: the five that were already right are pinned, the
  `report ungrammatical: <what>` variants deliberately keep ONE state (same operator action), and
  the enumeration is guarded against drift by a test that DERIVES the built-in cause literals from
  the script and requires each to be mapped — a new cause added to the classifier and not to the
  mapper reds the suite rather than being mislabelled. **That guard then paid out in round 5**: J1's
  new `stage record unreadable` cause was picked up by the derivation, so it arrived with its own
  `stage-record-unreadable` state and STATUS-NOTE (next action: the RECORD, or a fresh `--force`
  open — neither a chmod nor a re-spawn) instead of being mislabelled as self-reported. Eight
  reachable causes now.
- **A report-supplied value is neutralised at ONE emit boundary, and the boundary now delivers what
  it claimed (round 5, J3).** The cause, the quoted token and the `report=` field are all DATA on a
  line whose other fields a consumer scans, so `review-stage.sh` maps them through `one_line` /
  `field_value` and `premerge-assert.sh` through `c_safe_display`. `one_line` mapped only
  `\n`/`\r`/`\t` while its comment asserted that "no control character can break the one-line
  contract", so ESC, BEL, backspace, VT, FF and DEL passed into `verdict`'s line and into the merge
  point's diagnostics — a report could emit terminal escape sequences. The claim being broader than
  the mechanism is the defect, independently of what a sequence can do. Now the whole C0 range plus
  DEL is neutralised: the line-breaking whitespace becomes a space, every other such byte becomes a
  VISIBLE `?` (escaped rather than dropped, so a reader can see that something unprintable was
  there), and ordinary punctuation and non-ASCII prose pass through byte-for-byte — pinned by a
  control case, since a boundary that mangles legitimate text is one people route around. Both
  boundaries are DISPLAY-ONLY: every decision is made on the RAW value first, so neither can change
  a verdict.
- **THE BOUNDARY IS NOW STRUCTURALLY UNAVOIDABLE, BECAUSE THREE ROUNDS FOUND A NEW SITE EACH
  (round 7, L1).** Round 2's S1 (the cause), round 5's J3 (`one_line`'s incomplete map plus two
  premerge print sites) and round 7's L1 (`C_SOURCE` on the SUCCESS line, plus `deadline=`/`agent=`/
  `spawned-at=` read out of the stage record) are one class; every fix was right and the class kept
  regenerating, which is the standing signal to mechanize rather than patch a fourth time —
  CLAUDE.md's rule is to neutralise at ONE boundary and NEVER per interpolation site, "because a
  per-site escape is a list to keep complete". `scripts/tests/lib/emit-boundary-scan.sh` asserts
  that on every line of the operator-facing channel each interpolated value is either ROUTED through
  a boundary or NAMED IN AN ALLOWLIST WITH ITS REASON, so a new interpolation cannot be added
  silently. Both suites run it, each with a POSITIVE CONTROL that plants a bypass in a throwaway
  copy and requires the guard to red AND to name the planted symbol. **It DECLARES its scope on
  every run**, and what it does not cover is part of that output: positional parameters (not
  resolvable to their call sites), `review-stage.sh`'s record/report FILE writers (a different
  channel, whose boundary is `sanitize_field` on the write side) and `die_usage` (invoker argv to the
  invoker's own terminal), and whether a boundary function is CORRECT — it is a ROUTING check, and
  the behaviour is pinned behaviourally elsewhere. An allowlist entry is a CLAIM with a stated
  reason, not a measurement, in the shape #1716's `tools/` disposition guard checks that a
  disposition was RECORDED rather than that it is true.
- **AND THAT GUARD SHIPPED WITH ITS OWN BLIND SPOT, WHICH IS THE CLASS IT EXISTS FOR (round 9,
  N3).** Its scope was anchored `^[[:space:]]*(printf|echo)[[:space:]]` — the START of a line — so
  every COMPOUND statement was invisible to it, and it reported both scripts CLEAN with **three real
  bypasses** in them: `premerge-assert.sh`'s NO-GATE-OF-RECORD block printed the caller-supplied
  `$delta_file` unrouted from a line beginning `[ -n "$delta_file" ] &&`, and `review-stage.sh` had
  `$extra` behind a `[ -z … ] ||` and `$token` in a one-line `case` arm. Measured: the old scanner
  exits `0` with `OK 125` / `OK 44` on exactly those files, while the widened one names all three at
  their own line numbers. This is the repository's recorded shape — *a sweep built to close one blind
  spot shipped with its own and reported CLEAN on four real sites* — so the recogniser is POSITIONAL:
  a command word counts wherever a statement can begin, and the RECOGNISED constructs are **printed
  as a closed list on every run** rather than described (line start, `;`, `&&`, `||`, a pipe, `&`,
  `(`, `{`, `!`, a `case`-pattern `)`, and `then`/`else`/`elif`/`do`). It runs on the REDUCED line, so
  a command name inside a command SUBSTITUTION cannot pose as a statement. **The scan is BOUNDED at
  the command word**: only the text from there to the end of the logical line is examined, because an
  occurrence in a preceding `[ … ]` guard cannot reach the emitted text — and reporting it would force
  allowlist entries claiming "test only", which would ALSO excuse the same variable where it IS
  printed. Both remaining error directions are NOISE and both are declared at run time: a non-output
  command placed after the output command has its values attributed to that site, and an embedded
  program (a single-quoted `awk`/`sed` body, a heredoc) is not distinguished from shell. **Each suite
  now carries a COMPOUND-STATEMENT positive control that reproduces one of the three real
  instances**, requires the guard to red AND to NAME the planted symbol, and asserts the planted
  statement really does not begin its line — without that last assertion it would be a duplicate of
  the line-start control beside it. A bare red is not evidence and this is not hypothetical: on the
  premerge plant the OLD scanner also reds, for an unrelated reason, and never names the plant.
- **Round 7 also made `status`'s `past-deadline=` affirmative.** It guarded with a test for the
  literal `unknown`, so any OTHER non-numeric value read from the record reached
  `[ "$elapsed" -gt "$deadline" ]`, which printed bash's own `integer expression expected` onto
  stderr — a raw diagnostic inside a block every line of which is supposed to carry the
  `REVIEW-STAGE: ` anchor — and then took the permissive `past-deadline=no` branch, an answer derived
  from a comparison that never ran. Only DIGITS are compared now; everything else is
  `past-deadline=unknown`.
- **A VALIDATED-AS-DIGITS VALUE IS NOT A COMPARABLE ONE, and `$(( ))` is the worse half (round 8).**
  Round 7 closed the non-digit route above and left the class open, because bash's `[ -gt ]` is a
  **FIXED-WIDTH (int64) comparison**: an ALL-DIGIT value above `9223372036854775807` is refused with
  the same raw `integer expression expected`, and the enclosing `if` then takes its ELSE branch. So
  `--deadline-secs 9999999999999999999999999` was ACCEPTED at the boundary and surfaced two
  subcommands later as *somebody else's shell error* plus `past-deadline=no`. `$(( ))` does not fail
  at all — **it WRAPS SILENTLY** — so the same class read out of the stage record produced FABRICATED
  numbers with no diagnostic anywhere: `spawned-epoch: 18446744073709551616` yielded
  `elapsed=1788315330` (56 years, for a stage opened one second earlier) with `past-deadline=yes` and
  a `PAST DEADLINE` note, and `reopen-count: 99999999999999999999` wrapped to `7766279631452241920`
  **and was written back into the record**. Leading zeros belong to the same defect from the other
  side: `$(( 010 ))` is **OCTAL** (8) while `[ 010 -gt 9 ]` is **DECIMAL** — one value with two
  readings inside one script, so `spawned-epoch: 01756000000` reported 48 years of elapsed time.
  The fix is **ONE predicate**, `int_is_comparable`, at **every** boundary where a value from argv or
  from the stage record reaches a fixed-width operation — 7 call sites: the flag, both operands of
  the elapsed subtraction, both operands of the past-deadline comparison, the epoch `--force` copies
  forward, and the reopen counter. The bound is `MAX_INT_DIGITS = 10` (≤ `9999999999`): as a
  duration ~317 years, as a unix epoch the year 2286, both comfortably beyond legitimate use while
  leaving nine orders of magnitude of headroom under int64. **`0` is accepted** — round 7's L3
  records `deadline=0` as a legitimate emitter state, so a blanket leading-zero refusal would red on
  correct input. Refused values are a NAMED usage error at the boundary (`--deadline-secs`, exit 64,
  nothing written) or the honest `elapsed=unknown` / `past-deadline=unknown` on the read side.
  **`status` is advisory and decides nothing, and that is not licence to answer from an unperformed
  comparison**: `no` is the permissive answer, and a permissive answer derived from a comparison that
  never happened is this repository's standing prohibition, advisory or not.
  **`now_epoch()`'s own output is checked on the same terms**, not trusted: it is `date -u +%s` and
  is validated nowhere else, so an unusable clock reading would have produced `elapsed=0` — a
  fabricated measurement indistinguishable from a stage opened this second. **The record's own text
  is still DISPLAYED verbatim** (routed through `field_value`), so a hand edit stays visible in the
  audit trail; what is affirmative is the COMPARISON, not the display.
  Audited with it: **19 distinct numeric inputs across 62 fixed-width sites in the two scripts, 6
  affected, all in `review-stage.sh`.** `premerge-assert.sh` has none — every operand there is a
  string LENGTH, a script flag, or an awk line counter that `gate_parse_file`/`c_parse_verdict`
  already validate affirmatively as digits and that is bounded by the summary file's line count, so
  it cannot be an arbitrary digit string. Its `elapsed=`/`deadline=` grammar check (round 7's L3)
  is **deliberately NOT narrowed to this bound**: it never COMPARES those values, and the emitter can
  legitimately still print an incomparable `deadline=` read out of a hand-edited record — narrowing
  it would red on real emitter output, which is L3's own derived-from-the-emitter rule. `--issue`
  is the other unbounded digit string and is **correctly** unbounded: nothing compares it, it is a
  path component and a string.
- **The deadline is advisory and changes nothing.** A late report is still a report; a stage
  silent inside its deadline is still `NOT-RUN`. Letting a clock decide would add a clock to a
  question already answerable from content, and would fail a slow-but-real review.
- **With an explicit `--c-verdict <path>`, `premerge-assert.sh` verifies the verdict's grammar
  and token, not that the stage belongs to THIS issue.** The grammar check is the FULL emitted
  line — the stage KIND must be `c`, and `elapsed=`/`deadline=`/`agent=`/`report=` must each
  appear exactly once **AND CARRY A USABLE VALUE** (round 7, L3: the census only COUNTED them, so a
  `PASS` line ending in a BARE `report=`, or carrying an empty `elapsed=`/`deadline=`/`agent=`, was
  ACCEPTED and certified a merge — "counted, not measured", since a count is an affirmative
  measurement of PRESENCE and of nothing else). The permitted set is **DERIVED FROM WHAT THE EMITTER
  CAN PRODUCE**, which is what stops it redding on correct input: `elapsed`/`deadline` are decimal
  digits (**`0` included**, from `--deadline-secs 0`) or the literal `unknown`, and `agent`/`report`
  need only be NON-EMPTY — which admits `unknown` and `unresolved`, the honest not-measured values
  round 6's K1 emits. A charset is deliberately NOT asserted for `agent`/`report`, and **the reason
  differs per field**. `report=` carries an absolute PATH, so a charset would be a claim about
  anything a filesystem allows — and **it DOES arrive whole, spaces included, since round 11's Q3**:
  it is emitted LAST and read as the REMAINDER of the line. (Read as one whitespace-delimited field
  it TRUNCATED at the first space — a checkout at `/tmp/work tree`, and this repository tracks 40
  space-bearing paths under `docs/` — and round 10's nonce match then REFUSED an otherwise VALID
  verdict: a false refusal on correct input, measured on the shipped artifacts as
  `verdict reported: /tmp/…/work` beside a `validated generation:` that was exactly the one the
  verdict named. The remainder rule is sound ONLY because `report=` is last, so that assumption is
  ENFORCED against the shipped emitter rather than assumed — the 11 states are derived by RUNNING
  it and no mandatory key may follow `report=` on any line it produces.) `agent=` is written through
  `sanitize_field`, whose character class excludes whitespace, so it cannot legitimately carry one; a
  hand-edited record could, and that value truncates — a truncated DIAGNOSTIC, never a wrong verdict,
  since the token is what proceeds and `=` is neutralised at the emit boundary. So a sibling stage's `PASS` line (a
  `rust-review` verdict, say) can no longer certify C; what it cannot check is the ISSUE, because the line carries no sha. The
  report path is printed on the success line so a human can see which stage answered.
- **AUTO's ROUTING MEASURE IS ROOT-ANCHORED, AND `diff.relative=false` IS NOT WHAT DOES THAT
  (#3751 round 11, Q1).** The measure is `git diff <merge-base(origin/main, certified)>
  <certified> -- ':(top)openspec/changes/'`. A BARE pathspec is interpreted relative to the
  CALLER'S CWD, so `premerge-assert.sh` invoked from a repository SUBDIRECTORY got an EMPTY diff, a
  genuinely design-routed branch measured `NOT-APPLICABLE`, and the merge proceeded with NO C
  verdict at all — the escape `--c-verdict` exists to close, reached by nothing more exotic than the
  working directory (measured: `PREMERGE: OK`, exit 0, from `cqlite-core/src/storage`, where the
  root invocation on the same repository, sha and argv refuses with `routing: REQUIRED`).
  `diff.relative` is a DIFFERENT AXIS — it controls the OUTPUT PATH PREFIX, not pathspec
  interpretation (measured: from a subdirectory `-c diff.relative=false diff … --
  openspec/changes/` is still empty, while `-- ':(top)openspec/changes/'` finds the path) — so BOTH
  are pinned and neither substitutes for the other: `:(top)` anchors what is SELECTED,
  `diff.relative=false` keeps what is PRINTED root-relative, which the `archive/` prefix test and
  the slug extraction both depend on. **The generalisation: a pinned config option is a claim about
  ONE axis, and "cwd cannot change this answer" needs the axis your call actually uses.**
  `scripts/flow/base-staleness.sh` carries the identical pin and does NOT share the defect —
  measured, that scan passes no pathspec at all, taking the diff's paths wholesale and intersecting
  them in shell, so there the pin IS the whole cwd story. The stage LOOKUP was cwd-independent from
  the start (`c_stage_root` resolves `--show-toplevel`); the routing measure was the one half that
  was not.
- `AUTO` is
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
  stage with `--force` at this commit and re-run C. (c) **AUTO rests on ONE OBSERVATION of that
  record (#3751 round 9, N2).** (b) validated `head-sha` from one read, and `review-stage.sh
  verdict` then RE-READ the record to find which report is current (the nonce below) — two reads
  of one record are two different facts, so an atomic replacement in between handed back a verdict
  from a different GENERATION of the stage, possibly bound to a different commit, under a binding
  checked on the old one. Measured: the success line read `C-VERDICT PASS … stage-head=273cd3dff12c
  … report: …/c.decoygenerationB.md`, asserting a binding to a generation it never read, while the
  decoy's own `head-sha` was forty zeros. That defeats (b) and the nonce IN COMBINATION — the pair
  that stops a stale audit certifying a new tree. The record is therefore captured ONCE, the
  `head-sha` is parsed from THAT capture rather than from a second read of the file, and the
  capture is re-required to be BYTE-IDENTICAL before the token is parsed (a check after the parse
  could only report where the token came from). **A HANDOFF WAS THE WRONG FIX**: resolving the
  report in `premerge-assert.sh` and passing it to `verdict` would rebuild, from the other end, the
  control channel round 4 (H2) deleted with `--report` — nothing outside `review-stage.sh` may name
  which file holds a verdict. What the comparison does NOT claim: the REPORT can still change after
  `verdict` classified it. A verdict is a snapshot of a file at a time; this is about the record's
  GENERATION. (d) **AND BYTE EQUALITY IS NOT
  IDENTITY — AN ABA REPLACEMENT DEFEATS (c) (#3751 round 10, P2).** The record can go from the
  validated generation A to a foreign generation B while `verdict` reads B, and BACK to A before
  the comparison: two byte-identical observations, (c) passes, and the ACCEPTED verdict came from
  B. Equality of two observations is not identity of the thing observed at a third instant — that
  is what "one observation" could not buy on its own. So the verdict is bound to the GENERATION
  itself, using a value it already reports OUTWARD: its mandatory `report=` field carries that
  generation's nonce (`<kind>.<nonce>.md`, below), and that nonce must equal the `report-nonce:`
  of the SAME capture `head-sha` was parsed from. ABA cannot satisfy it — a verdict read from B
  returns B's nonce. Reading a value OUT of the verdict line rebuilds NO control channel (nothing
  is passed IN; H2's `--report` stays deleted), and (c) is KEPT as defence in depth rather than
  replaced: it catches an edit under the SAME nonce and a vanished record, which (d) cannot, and
  (d) catches what it cannot — neither contains the other. Every state that cannot be bound
  REFUSES BY NAME (a legacy record with no `report-nonce:`, several of them, an unusable token, a
  `report=unresolved`, a foreign nonce), and it gates the two tokens the closed grammar lets
  PROCEED, because acceptance is the only thing that can certify — for every other token
  `review-stage.sh`'s own cause is the more precise operator action. Measured RED: an A→B→A
  interleave produced `C-VERDICT PASS … stage-head=<validated>` beside
  `report: …/c.decoygenerationB.md` with NO record-changed refusal, because the bytes genuinely
  matched; and a legacy record with no `report-nonce:` certified a PASS from a bare `<kind>.md`
  report whose generation nothing named.
- **The report path is NONCE-BOUND, so a resumed agent cannot write into the current report
  (round 5 J1, round 6 K2).** `open --force` reset the report to the sentinel and re-stamped
  `head-sha:` **at the same path**, so the PREVIOUS, idle agent could wake up after the reset and
  write its OLD-TREE verdict there — where it was paired with the NEWLY stamped `head-sha:`, and a
  commit nobody audited passed `premerge-assert.sh`. This mechanism exists BECAUSE delegated agents
  go idle and return late, so that is the expected behaviour of the population it serves, not an
  exotic race. Every open now records a `report-nonce:` and the path INCLUDES it —
  `<kind>.<nonce>.md` — so the resumed agent holds a STALE PATH and is STRUCTURALLY unable to write
  into the current report. A check could not deliver this: the harm is a WRITE, and a check placed
  after it could only report it. Five properties worth keeping in mind.
  (1) The nonce is an **OPAQUE TOKEN in the record, never a path** — the reader derives the path
  from it with the same function `open` used, so there is ONE source of truth for which report
  counts, and round 4's removal of the `report:` path field (a data file that could redirect a
  reader) is not undone. (2) It is written in the SAME atomic record as `head-sha:`, so the tree
  audited and the artifact auditing it are published together or not at all — an interrupted
  `--force` leaves the ENTIRE previous stage in place, which is coherent and refuses at the merge
  point on the sha. (3) An ABSENT field is the LEGACY bare `<kind>.md`, which is an affirmative
  reading of a record written before the field existed (that version wrote exactly one report, at
  that name), while SEVERAL lines, an invalid token, or a record that could NOT BE READ AT ALL
  (round 6, K1) is a `stage record unreadable` NON-VERDICT that derives no path at all — falling
  back to the bare name is exactly how a stale `PASS` would be read as current.
  (4) **The nonce is GENERATED, never SELECTED (round 6, K2).** The first design NUMBERED the
  generations and chose the next one by SCANNING the stage directory for an unused
  `<kind>.<gen>.md`; a value chosen by looking at what is already on disk is a value TWO CONCURRENT
  CALLERS CAN BOTH CHOOSE — two `open --force` runs read the same record, probe the same directory
  before either has written, pick the same generation and hand ONE report path to TWO agents, so
  the superseded agent's write replaces the current verdict, `FINDINGS` included (measured: both
  calls printed `c.1.md`, and A's `result: FINDINGS` became B's `result: PASS`). A nonce makes that
  structurally impossible rather than serialised, and a LOCK would have been the worse answer — it
  serialises a race a nonce removes and adds a mechanism (a stale lock file, a box without `flock`,
  a holder killed mid-open) to a tool whose subject is not taking the permissive branch when
  something cannot be measured. The scan, its attempt bound and its exhaustion refusal are
  **DELETED**: with nothing selected there is nothing to exhaust, and subtraction cannot introduce
  a false PASS. The randomness comes from `mktemp -u`'s name substitution — the same source the
  write path's temporary name already uses — and no cryptographic strength is needed or claimed:
  the nonce is a uniqueness token, not a secret. There is deliberately **no fallback generator**; a
  box that cannot produce one is refused by name. **`reopen-count` SATURATES at the ten-digit
  ceiling rather than restarting (round 9, N4)**: the `$(( prior + 1 ))` walked off round 8's bound,
  so the next re-open read an eleven-digit value as incomparable and silently restarted the count at
  `1` — measured, the record held `10000000000` and then `1`. Refusing the re-open was the other
  option and is wrong for this field: round 8's own ruling is that an unusable counter takes the
  value an absent one gets and is *never a reason to refuse a spawn*, so blocking a spawn over a
  cosmetic audit number would be the guard agents learn to waive. Held, the value means AT LEAST
  that many, it can never decrease, a `note` NAMES the hold when it happens (and does not fire for
  an ordinary increment, since it claims a specific event), and **both surfaces render it `<n>+`
  through ONE renderer** — `open`'s `OPEN-OK` line and `status`, which reports `reopen-count=` for
  the first time as part of this change. The marker appears only at the ceiling, never on a value
  that can still increase and never on one no comparison was performed on. `reopen-count:` remains
  as the human-readable
  audit number, because it answers a different question (how many spawns).
  (5) Superseded reports are LEFT ON DISK as history: nothing reads them, and they are what an
  operator opens to see what the previous agent concluded. Since round 6 nothing DEPENDS on their
  existence either — the nonce is generated, not chosen from what is absent — so deleting one by
  hand costs the audit trail and nothing else.
  The operational consequence for a lane: **paste the path `open` PRINTS, never a remembered one**
  — and it cannot be reconstructed from the kind and the issue, so where no path was named, ask
  `review-stage.sh status <kind> --issue <N>`, whose `report=` field is the authority.
- **The report path is DERIVED, and `--report` is GONE (round 4, H2/H3).** It is always
  `<repo-root>/.review-stage/issue-<N>/<kind>.<nonce>.md` (a bare `<kind>.md` only for a record
  written before the nonce existed), computed the same way by `open` and by every reader — so nothing a caller passes, and nothing written in a data file, can redirect a reader to
  another file. The override is REMOVED rather than hardened, which is a **deliberate narrowing of
  the approved design surface**: it was mandated by no spec requirement and used by NOTHING
  (measured by grep — no agent definition, no skill, no script, no call site), and it was the
  caller-controlled component behind a finding cluster across four review rounds. Two of those were
  round 4's: the path was written RAW into the LINE-oriented stage record, so a LEGAL
  newline-bearing filename split across lines and the reader (`read_field`) took only the PREFIX —
  which could name a DIFFERENT, pre-existing report recording `PASS` while the sentinel went to the
  newline-bearing name; and the report's parent directory was created BEFORE repository containment
  and ignore status were verified, so a REFUSED outside-the-repository path still created
  directories outside the checkout. Derivation closes both BY CONSTRUCTION — no newline to split
  on, no containment question to answer — and leaves `<kind>` (`[A-Za-z0-9][A-Za-z0-9_-]*`) and
  `<issue>` (digits only) as the whole path-input surface, validated at ONE boundary. The stage
  record no longer carries the path as a readable field either: a second source for a value with
  one derivation is only a second thing to disagree. If a caller ever needs a custom location,
  re-add the flag WITH the hardening (CR/LF refused, containment verified before any `mkdir`).
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

`NOT-RUN` always carries one of seven named causes — `no report written`, `report absent`,
`report unreadable`, `report empty`, `report ungrammatical: <what>`, `stage never opened`,
`stage record unreadable: <what>` — because the operator
action differs per cause, and one token for seven states is the collapse this issue is about.
Everything is written under `.review-stage/`, whose ignore status is **verified with
`git check-ignore`, fail-closed**, so a stage opened mid-run cannot dirty a running gate of
record (#2926) or make `premerge-assert.sh` refuse on `dirty: yes` (#3648).

**And a SYMLINK at the report path, at the `.stage` path or at ANY component under `.review-stage/`
is REFUSED, never followed (#3751 round 1)** — `check-ignore` judges a LEXICAL path while a WRITE
follows links, so an ignored-but-symlinked report clobbered a TRACKED file and reported `OPEN-OK`
(measured); the writes themselves go through an UNPREDICTABLE same-directory temporary file
(`mktemp -u`) CREATED AND OPENED IN ONE STEP under `set -C` — i.e. `O_CREAT|O_EXCL` — then written
through the ALREADY-OPEN DESCRIPTOR and `mv -f -T`'d into place (#3751 round 3, G3; the `-T` is
round 7's L2), so a concurrent reader never sees a half-written `result:` line.

**GNU-COREUTILS DEPENDENCY, STATED EXACTLY (#3751 round 7, L2): the rename REQUIRES `mv -T` /
`--no-target-directory`, which a stock BSD/macOS `mv` does not have.** A plain `mv -f SRC DEST` does
not promise to replace the NAME `DEST`: if `DEST` is — or BECOMES — a directory, or a symlink to one,
`mv` puts the temporary file INSIDE it and **EXITS 0**, so the write lands outside the verified path
while the tool reports success (measured: `mv -f` → exit 0 with the temp inside `dest/`; `mv -f -T` →
refused, source left in place). `-T` closes the LEAF for a second reason too — `rename(2)` does not
follow a symlink for the destination. It is **REQUIRED, NOT ATTEMPTED**: there is deliberately no
fallback to a plain `mv -f`, which would restore the defect on exactly the hosts that cannot detect
it, and no probe is needed for safety because an `mv` without `-T` fails the option parse, moves
nothing, and the write REFUSES. A three-valued probe (`yes`/`no`/`unknown`, answered by PERFORMING
`mv -T` on two throwaway files, never by scanning `--help` text) runs ONLY on that refusal path, to
NAME the cause — "this host's mv has no `-T`" and "the rename was refused" are the same exit status
and two completely different operator actions. `review-stage.sh`'s CONSTRAINTS block records the
precondition, and its "macOS bash 3.2 compatible" claim is narrowed to LANGUAGE compatibility
accordingly. **`-T` is DEFENCE IN DEPTH for a TOCTOU window**: the symlink and
not-a-regular-file checks already refuse a PRE-PLANTED directory or link, so what `-T` covers is the
window between those checks and the rename — which is why its coverage is structural plus a measured
host property plus a PATH-shimmed no-`-T` host, and not an induced race. **The first version was a TOCTOU**: the temp path
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

`--performed-by author` is the ONLY accepted performer. `peer` was accepted until round 6 (K3) and
was then reported under the token `AUTHOR-PERFORMED` — so a PEER audit, the more independent of the
two, was stated to be the diff AUTHOR's, which is a false statement in the one line a human reads.
It is REMOVED rather than given a `PEER-PERFORMED` token of its own: a peer who CAN perform the
audit writes the report of record and reaches a genuine `PASS`, which is the PRIMARY path, so the
affordance bought nothing but the false verdict — and a token nobody needs is a maintenance tax at
every site the closed grammar is enumerated (`premerge-assert.sh`, CLAUDE.md, this file, six agent
definitions, two skills, the OpenSpec delta and both website pages). This subcommand exists for the
case where NO independent audit can be obtained, i.e. the author's own, and removing `peer` makes
its name true.

**The already-recorded refusal PREVENTS rather than REPORTS (#3751 round 9, N1).** Round 2 (B2) made
`record-author-performed` refuse to replace a recorded `PASS`/`FINDINGS` without `--force`; it checked the
verdict and THEN spent a `mktemp`, an `O_EXCL` create, a `date` and a dozen `printf`s before installing
its replacement, so a late reviewer recording `FINDINGS` anywhere in that span was silently overwritten by
the merge-proceeding token — with no `--force` and no `replaced-verdict:` trace, i.e. the exact harm the
guard was added for. A check placed before the act it guards, with a window in between, can only report.
So the observation the decision rests on is RE-TAKEN immediately before the rename and compared for
equality; any difference refuses with `reason=report-changed-mid-write` and installs nothing. Three
properties worth knowing: it compares the report's BYTES, not its classified token, because with `--force`
one `FINDINGS` replaced by a DIFFERENT `FINDINGS` leaves the token equal while the report the operator
read is gone; the bytes are captured BEFORE the classification (which re-reads the file), so a change
either read could see is caught by the comparison; and `--force` does NOT cover it, since `--force`
authorizes replacing the verdict the operator READ, never one that arrived afterwards. **The residual is
DECLARED because it cannot be removed**: the rename itself is not conditional — coreutils `mv` exposes
neither `RENAME_EXCHANGE` nor `RENAME_NOREPLACE`, and `mv -n` is the wrong predicate (the destination
legitimately exists) — so one fork/exec of `mv` remains open, and a LOCK would not close it even if it
were free, because the counterparty is an arbitrary agent writing the report with its own tooling and
taking no lock. Only a unilateral compare-and-swap could, and that is what is unavailable.

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
