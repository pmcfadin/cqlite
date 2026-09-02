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

**And the verdict must describe a state the report actually held (round 12, R2).** The classifier read
its subject EIGHT times — existence, a readability probe, the body for emptiness, the `result:` census,
the disclosure, and `performed-by`/`reason`/`evidence` each through their own field read — so a report
REPLACED between two of those reads let it assemble `AUTHOR-PERFORMED` out of fields drawn from
DIFFERENT, INDIVIDUALLY INVALID versions: one version's usable `reason` beside another's usable
`evidence`, working **no single snapshot ever contained**. A verdict is a statement about a document;
assembled across two documents it is a statement about neither. So ONE observation feeds every field,
the `<key>: <value>` grammar has ONE implementation shared by the snapshot and file readers (a second
implementation's agreement is only knowable by testing it), an observation the classifier cannot
recognise is a NON-VERDICT reported as UNREADABLE rather than ungrammatical (the bytes were not
obtained, so nothing may be asserted about the content), and `record-author-performed` passes its OWN
byte snapshot in — so the bytes its write is guarded on (§4) and the verdict it decides by are the same
instant. This is §3's one-observation property (round 9, N2) applied one level down, to the report.

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
  uniqueness token, not a secret. **Round 12 (R1) puts the RESERVATION back — and only the
  reservation, not the scan.** Round 6 deleted both, and `mktemp -u` creates nothing, so an
  unreserved nonce repeating a report already on disk let `open` write over that HISTORICAL report
  and republish its path in the record — a recorded verdict replaced by a sentinel, and the
  superseded agent still holding that path handed the ability to write the CURRENT one, which is
  exactly what round 5's generation binding exists to prevent, reached with no concurrency at all.
  `reserve_report_path` creates each candidate under `set -C` (`O_CREAT|O_EXCL`), generates a FRESH
  RANDOM nonce on collision, and turns exhaustion of a bounded attempt count into a NAMED refusal
  (`reason=report-nonce-not-reserved`) rather than a fallback to an unreserved name. The scan is NOT
  back: it SELECTED a name by TESTING EXISTENCE and wrote it in a LATER step (two steps, with a
  window two callers could both observe), while an exclusive create IS the choice — one operation,
  so the decision and the claim cannot be separated. The reserved name is an owned resource,
  registered with the cleanup path the moment it exists and de-registered on fulfilment, so a
  refused open leaves the stage directory as it found it and the cleanup can never delete the
  published report. `reopen-count:` remains as the human-readable audit number — and it SATURATES at the ten-digit ceiling rather than restarting (#3751 round 9): `$(( prior + 1 ))` walked off round 8's bound, so the next re-open read an eleven-digit value as incomparable and restarted the count at `1` (measured: the record held `10000000000`, then `1`). Refusal was rejected as the fix — round 8's own ruling is that an unusable counter is never a reason to refuse a spawn — so it is HELD, meaning AT LEAST that many, `note`d when the hold happens, and rendered `<n>+` by ONE renderer on both `OPEN-OK` and `status` (which reports the counter as of this change).
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
- **And byte equality is not IDENTITY: an ABA replacement defeats that comparison** (#3751 round 10,
  P2). The record can go from the validated generation A to a foreign generation B while `verdict`
  reads B, and BACK to A before the comparison — two byte-identical observations, the comparison
  passes, and the ACCEPTED verdict came from B. Equality of two observations is not identity of the
  thing observed at a third instant. So the verdict is bound to the GENERATION itself, using a value
  it already reports OUTWARD: its mandatory `report=` field carries that generation's nonce
  (`<kind>.<nonce>.md`), which must equal the `report-nonce:` of the SAME capture `head-sha` was
  parsed from — ABA cannot satisfy that, because a verdict read from B returns B's nonce. Reading a
  value OUT of the verdict line rebuilds no control channel (nothing is passed IN, so H2's deleted
  `--report` is not recreated from the other end), and the byte comparison is KEPT as defence in
  depth: it catches an edit under the SAME nonce and a vanished record, which the nonce match
  cannot, and the nonce match catches what it cannot. Every state that cannot be bound REFUSES BY
  NAME (a legacy record with no `report-nonce:`, several of them, an unusable token, a
  `report=unresolved`, a foreign nonce), and it gates the two tokens the closed grammar lets
  PROCEED, because acceptance is the only thing that can certify.
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
- **AND THE MEASUREMENT MUST NOT DEPEND ON THE CALLER'S WORKING DIRECTORY (round 11, Q1).** The
  pathspec is root-anchored with git's `:(top)` magic, because a BARE `-- openspec/changes/` is
  interpreted relative to the cwd: invoked from a repository subdirectory the diff came back empty, a
  design-routed branch measured `NOT-APPLICABLE`, and the merge proceeded with no C verdict — a
  chdir-shaped escape past the very check this change adds. `diff.relative=false` does **not** cover
  it (it governs the output path prefix, not pathspec interpretation), so both pins stay and neither
  substitutes for the other. Generalised: **a pinned config option is a claim about one axis, and
  "cwd cannot change this answer" needs the axis your call actually uses** — `base-staleness.sh`'s
  identical pin is sufficient there only because that scan passes no pathspec at all.
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
under `--force` too. Round 9 then DECLARED the remaining span — between the re-observation and the
`rename(2)` inside one `mv` — as an irreducible residual: no compare-and-swap rename is reachable from a
shell, and a lock cannot help because the counterparty is an arbitrary agent writing the report with its
own tooling and taking no lock.

**Round 15 (U1) WITHDRAWS that declaration and removes the overwrite instead.** The reasoning is about
WHOSE verdict is lost: the party who writes into that span is not a hostile racer, it is a SLOW REVIEWER
— the population this whole change exists for — so the loss was produced by the system's own normal
behaviour, and what was lost was a recorded review verdict. A declared boundary is not acceptable for
that. `record-author-performed` therefore writes to no existing report at all: it reserves a FRESH
generation with the machinery already here (round 6's nonce, round 12's atomic reservation), writes the
substitute there, and the stage record — written LAST, the publication marker of round 4's H1 — names it.
A late reviewer's verdict in the previous generation is then SUPERSEDED rather than DESTROYED: it stays on
disk, readable, and the new report names the generation it took over from (`supersedes-report-nonce:`).
The window is not closed; destruction is. Whether the command may PROCEED over a prior verdict stays a
separate question with its existing rule, and the record rewrite carries every byte but `report-nonce:`
through verbatim — `head-sha:` is not re-stamped, because re-stamping it would bind a substitute to a
tree the stage was never opened at, which is round 5's own J1 harm. The transferable rule: **when a check
can only NARROW a window, ask whether the harm can be made UNEXPRESSIBLE instead — and never declare a
residual whose victim is your own system's normal behaviour.**

Round 13 (S1) closed the third defect in the same guard, created by round 12's own fix: the guard branched
on the classified TOKEN, and the UNREADABLE observation state R2 introduced arrives there as `NOT-RUN`,
i.e. on the REPLACEABLE side — so a report whose recorded verdict was UNKNOWN, possibly a blocking
`FINDINGS`, was replaced by the merge-proceeding token with no `--force` and no trace. *Unknown is not
absent*, so the permissive set is keyed AFFIRMATIVELY on the two measured states (`absent`, `present`),
read through ONE reader of the observation grammar (`report_state`, shared with the classifier) so a
later state refuses at both callers by construction. `--force` does not cover it — it authorizes
replacing the verdict the operator READ — and the recovery is `open --force`, which supersedes the stage
with a fresh report and leaves the unreadable file on disk as history.

Round 13 (S2) is the same round's second finding and a rule rather than a byte: **a capture that
normalises its input cannot be the thing that validates it.** A command substitution SILENTLY
DISCARDS NUL bytes, so every `$(…)` read of an untrusted file here could MANUFACTURE grammar its
source does not contain — `res\0ult: PASS` (no column-zero `result:` line) reported `RESULT: PASS`,
a record's `report-nonce: STALE\0PASS1` (not a valid token) was read as `STALEPASS1` and redirected
the reader to a STALE report's `PASS`, and in `premerge-assert.sh` a `--c-verdict` token of `PA\0SS`
arrived as `PASS` and printed `PREMERGE: OK` at the merge point. The fix is in the READ, not in a
probe: a separate probe of the same path is a second observation whose disagreement can fail OPEN,
so the one read maps NUL to SOH in the stream (one mapping implementation per script, one literal
with the byte derived from it). Three further behaviours of that capture were enumerated in the same
breath: trailing-newline stripping cannot change a per-line, column-zero grammar — **declared and
left, and that conclusion is bound to THAT consumer, the report's CONTENT; round 18 (X1) below
falsified it for a PATH and every statement of it now names the consumer it was reasoned about**;
locale/encoding is already `LC_ALL=C`-pinned at every consumer (now measured by a cross-locale
invariance case rather than asserted); and the completeness sentinel's own aliasing — a failed read
whose last delivered byte IS the sentinel — is closed by requiring the read's exit status too.

### The emit boundary, and the guard over it

Three consecutive rounds found a new unrouted interpolation, so the completeness is asserted
STRUCTURALLY by `scripts/tests/lib/emit-boundary-scan.sh` rather than site by site (round 7, L1b).
**That guard then shipped with the very defect it exists to catch (round 9, N3)**: its scope was
anchored at the START of a line, so every compound statement was invisible and it reported both
scripts CLEAN with three real bypasses in them — including the caller-supplied `$delta_file` printed
raw from a line beginning `[ -n "$delta_file" ] &&`. The recogniser is positional now; the constructs
it accepts are PRINTED as a closed list on every run, the scan is BOUNDED at the command word (so a
preceding `[ … ]` guard needs no allowlist claim), and each suite carries a control that plants a
bypass inside a compound statement — a control planting only at a line start cannot tell the widened
guard from the blind one.

**And the boundary reaches the printing COMMAND, not only the value (round 14, T2).** `emit`, `note`
and `die_usage` used `echo`, whose argument is a FORMAT under the inherited-environment option
`xpg_echo` — so a `\n` in a LEGAL checkout path split the one-line verdict in two (the second line a
column-zero `REVIEW-STAGE: … RESULT: PASS` for a stage with no report) and octal `\075` put REAL
`key=` pairs on it, defeating the `=`→`~` neutralisation outright. Every line is `printf` of a
literal format now, and the scanner gained a second check — over EVERY logical line, since the
primitive question has one answer everywhere — that refuses `echo` with no allowlist and requires
every `printf` FORMAT to be script-authored. Its scope, its NOT-COVERED set and its own vacuity
guard are declared separately from the value check's.

**And the READ boundary needed the same mechanism, for the same reason (round 14, T1).** Round 13
introduced the faithful-read boundary and routed three of five non-boundary read sites; both
remaining ones were found the next round — `count_field_lines` reading the stage record with
`grep -c` on the FILE (a *faithful* reader whose truthful `0` means "pre-nonce record, LEGACY bare
report", so a stale `c.md` recording `result: PASS` was reported as the verdict at exit 0) and
`_gate_awk` reading the gate-of-record summary raw. Round 13's asserts could not see either, because
they check the mapping appears exactly ONCE — a property of the BOUNDARY, not of its CALLERS.
`scripts/tests/lib/read-boundary-scan.sh` asks the caller-side question, with an allowlist whose
entries are CLAIMS carrying reasons and whose STALE entries are their own failure; its own first
draft reported CLEAN on the defect it exists for (every text call here is `LC_ALL=C grep …`, so the
text before the command word ends in `C`), caught by the positive control — which is why the
controls plant the exact shape rather than a convenient one.

**And the SAME family had a second byte, found one round later (round 15, U2): an ANSI strip may
LOCATE a line and may never SUPPLY a value.** All three of `premerge-assert.sh`'s awk readers deleted
every CSI sequence before the closed grammar ran on the fields that deletion produced, so a token
spelt `PA<ESC>[31mSS` normalised into `PASS` and certified a merge, a gate summary's spliced
`RESULT:` reached the merge gate as `PASS`, and a stage record's spliced `head-sha:` normalised into
a clean 40-hex sha that would have bound the stage to a tree the record does not name. The strip is
NOT gratuitous — #3400: colour survives redirection, and a coloured capture without it fails every
marker anchor — so it was SPLIT rather than deleted: each reader keeps a DELETING reading to locate
and parse, and a SEPARATING reading (each CSI replaced by one space) for one question, *did the
deletion JOIN two runs the file keeps apart?* **Separate versus join is the transferable rule**, and
it is also what KEEPS the trailing-CR strip: `\r$` removes one byte where nothing follows, so it can
separate but never join. That call was decided by the §44g reader differential rather than argued —
it FAILED on the ESC row (`classify_report` reported `unrecognised result token 'PA?[31mSS'` while
the awk published `PASS`) and PASSED on the CR row, naming exactly one side as wrong. Refusing the
CR would have been a unilateral change to one of two readers of one shape, which is the divergence
that section exists to detect.

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

## §9 — round 16: two windows, one lesson each

### V1 — a check outside the window it certifies (`premerge-assert.sh`)

`c_evaluate` ran once, near the top of the merge-point checks. Between it and `PREMERGE: OK` sat the
base-staleness advisory (bounded at 65s) and the `gh pr view` round trip — so a concurrent
`review-stage.sh open --force` superseded the validated PASS and the script still certified. Measured on
the shipped artifact with the supersede planted immediately after the single evaluation:
`PREMERGE: OK b5f49d60aae4…` at exit 0, while `review-stage.sh verdict` read from the same worktree an
instant later reported the FRESH generation.

**The remedy was not invented here.** This repository had already ruled on exactly this shape, on the
gate's own component-set pre-flight (roborev job 290): *a check must be INSIDE the window it certifies —
not before it, not after the harm* — and the remedy applied there was to **REPEAT the check inside the
window while KEEPING the earlier one**, because the early call is what stops an uncertifiable run doing
the expensive work at all. That arrangement is followed verbatim rather than replaced by one late check.

Three properties are worth carrying beyond this issue:

- **The repeat RESETS its inputs.** Rounds 9 and 10 both fail closed on an empty capture, so resetting is
  what forces the single observation and the generation binding to be taken *afresh*. A re-validation that
  left the first capture in place would compare the record against an observation taken before the window
  — a different property, which reads as satisfied while the second evaluation measured nothing of its own.
- **A repeat is not a comparison, and only the comparison catches the interesting case.** A supersede to a
  DIFFERENT generation that itself PASSES at the same head returns an accepting token from an audit this
  run never validated. Running the check twice certifies it; comparing the two answers refuses it. The
  test that pins this is the section's discriminating case, not its headline one.
- **A refusal's own prose may not reproduce the success marker.** This fix's first draft explained itself
  with the words *"runs immediately before `PREMERGE: OK`"*, so a reader — or a grep — saw the
  certification token inside a refusal. That is #3312's rule (a diagnostic must not print the marker it
  describes) one directory over, and it was caught only because the test asserts that NO such line is
  emitted at all rather than merely checking the exit code.

**Residual, declared:** two checks cannot both be last. The C window narrows from "the advisory plus a
network round trip" to "a local git measurement plus one `review-stage.sh` read"; it is not closed,
because a verdict is a snapshot of a file at a time. Symmetrically, the `gh` head/state check is no longer
the last thing before the success emit — a trade recorded at the call site, in the direction that makes
the removed window two orders of magnitude larger than the added one.

### V2 — a legal `=` in the path made `report=` publish a file that does not exist (`review-stage.sh`)

`report=` went through `field_value`, whose `=`→`~` map exists so a value cannot forge a `key=value`
pair that a field-scanning consumer reads instead of the measured one. A repository root may
**legally** contain `=` — so on such a checkout the verdict line advertised a path that does not
exist, while the grammar promises the absolute report-of-record path. Measured on the shipped script
at `…/eq=path/lane`:

```
open  (raw line):  …/eq=path/.review-stage/issue-3751/c.XPRfO9NNsk.md   <- exists
verdict report=:   …/eq~path/.review-stage/issue-3751/c.XPRfO9NNsk.md   <- does not exist
```

`open` was correctly scoped out of the finding: it prints a raw path line of its own, deliberately
unrouted, which is the channel a caller consumes. `verdict` has no such fallback.

**The exemption is coupled to the property that justifies it, not to a comment.** Since round 11 (Q3)
`report=` is emitted LAST and read as the line REMAINDER, so there is no following field for a forged
pair to displace and the consumer is not scanning fields there at all — the anti-forgery reason
simply does not apply to this one field. Because the exemption *depends* on that arrangement, the two
facts are pinned in **one** assertion (the field is last AND routed through the exempt boundary), so
either change alone reds a suite.

**Three things worth carrying beyond this issue:**

- **Confinement is a design property, not bookkeeping.** Six other `report=` emitters keep
  `field_value`. That is not caution: `report-changed-mid-write` emits `now-verdict=` *after*
  `report=`, so the exemption would be **unsound** there — and for the rest, no consumer reads those
  lines as a remainder, so exempting them would rest on *"no consumer exists today"*, which is a
  permission derived from the ABSENCE of a bad signal. The structural pin is therefore one
  definition, one call site.
- **The control proves the confinement, not the fix.** A `report=` pair smuggled through the
  `agent=` field must still be neutralised: unmapped, it puts a **real** `report=` pair *ahead* of
  the measured one and the remainder reader takes the FIRST. Without that case, the headline
  assertion is satisfiable by dropping the map from every field — which would be a strictly worse
  script that passes a strictly weaker test.
- **"Differs in one respect" is a claim about behaviour, so it is tested behaviourally.** The
  differential extracts `field_value` and `remainder_value` from the shipped file and RUNS them:
  `field_value` still maps `=`, `remainder_value` does not, and `remainder_value` still renders a C0
  byte visibly and still flattens a newline. Reading the source would have proved only that the two
  bodies look different.

Adding the new boundary function also **RED the emit-boundary scanner** until it was declared in
`BOUNDARIES` — the round-7 mechanism doing exactly its job, and the reason that entry carries its own
paragraph of reasoning rather than being appended silently.

**Declared residual:** on a `=`-bearing checkout the `status`, `OPEN-OK`, `already-open`,
`AUTHOR-REFUSED`, `report-changed-mid-write` and `RECORD-OK` lines still *display* a
`~`-substituted path. Each is a diagnostic; the two channels that promise the real path are `open`'s
raw line and the verdict line.

## Round 17 (W2) — a checkout path this grammar cannot carry is refused, not published wrong

The `=` case above is the one where the raw value was fine and only the *rendering* was wrong. The
general case is worse, because **the two commands lie differently about the same file**: `open`
prints the RAW path on its own line, so a newline-bearing repository root **split** the value across
two physical lines — the second carrying none of the `REVIEW-STAGE: ` anchor every consumer of this
grammar reads, and the paste-ready spawn clause handing the agent two fragments — while `verdict`
**flattened** it and published `…/lane two/…`, a path no `open(2)` can resolve, on the one line
whose whole promise is the absolute report-of-record path.

- **A residual whose premise is "this input never arrives" is a claim about the world, and this one
  was false.** Round 11 declared a newline-bearing path unrepresentable *and never arriving*. The
  first half is true; the second is not, because git resolves the root of whatever checkout the
  tool is run in, and the root is the ONE path component derivation does not validate. The
  declaration is **withdrawn**, swept over four sites with a positive control, because a stale
  declaration is what stops the next person looking.
- **Refuse at the boundary, and fail closed.** `require_repo_root` — the ONE place the root is
  resolved — refuses (exit 64, nothing read and nothing written), so all four subcommands inherit
  it rather than each carrying its own check. A checkout named this way cannot use the tool, which
  is a clear actionable refusal (rename or re-clone) and strictly better than silent corruption of
  the value the grammar promises.
- **Key the check on the RENDERER's own answer, never on a character list.** The rule is *does this
  root survive `one_line` unchanged* — a rendering that differs IS a published path that does not
  exist, whatever byte caused it. A hand-written class of bad characters would drift from the
  renderer the first time the renderer changed. The probe calls `one_line` rather than
  `remainder_value` so that round 16's exemption-confinement pin keeps counting EMIT sites only,
  and the two are pinned to agree behaviourally.
- **The newline keeps its own detail, because its harm differs in KIND.** A value that spans lines
  cannot be a field of a one-line record under any rendering; every other unrenderable root (a tab,
  another C0 byte, a whitespace run, a leading/trailing space) is a wrong VALUE on an intact line.
  Naming the general cause for a newline would be a true statement that hides the sharper one, and
  the tab case asserts the newline rationale is ABSENT for it.
- **The control is what keeps it off correct input.** A SPACE-bearing checkout — round 11's own
  subject, and the reason `report=` is read as the line remainder — must still work end to end,
  with the whole path published AND that path required to EXIST.
- **No opt-out.** A checkout is always renamable, so an escape hatch could only buy a published path
  that cannot be opened.

## Round 18 (X1) — a captured path is not the path

The refusal round 17 built was **unreachable for the shape that mattered most**. Both tools resolved
the worktree root with `root="$(git rev-parse --show-toplevel)"`, and a command substitution strips
**every** trailing newline — so a checkout whose *directory name* ends in an LF resolved to a
DIFFERENT, EXISTING SIBLING, and the captured value then carried no newline for the representability
check to disagree about. Measured on the shipped scripts, from `lanetrail<LF>/` beside a peer lane
`lanetrail/`: `review-stage.sh verdict` reported
`RESULT: PASS … report=…/lanetrail/.review-stage/issue-704/c.<nonce>.md` at **exit 0**, off a report
that lane never opened; a refused `open` created a directory *inside* the peer lane; and
`premerge-assert.sh`'s AUTO path located, bound and read the same sibling's stage records. It is
**#3616's peer-artifact class reached through a lossy capture instead of a recency scan**, and
`c_assert_head_binds_certified` is structurally blind to it, because HEAD is read in the CWD — the
real lane, so it binds — while the ARTIFACT comes from the sibling.

- **A LOSSY-CAPTURE CONCLUSION MUST BE RE-DERIVED PER CONSUMER, NEVER CARRIED. This is the durable
  rule, and it is a correction to round 13's own ruling.** Round 13 (S2) enumerated trailing-newline
  stripping in the same breath as NUL removal and declared it harmless: *"it cannot change a verdict
  — every grammar here is per-line and column-zero anchored."* That is **true of the report's
  CONTENT and false of a PATH**, where the stripped bytes are part of the value's IDENTITY and a
  shorter string names a different file. The conclusion was right about the consumer it was reasoned
  about and was then carried, unqualified, to a consumer it was never true for. Every doctrine site
  stating it now names that consumer.
- **Keep the source's own framing; do not guess at the value.** A SENTINEL is appended INSIDE the
  substitution, so the stripping has nothing of ours to eat; the sentinel is removed, then **exactly
  one** newline — git's terminator for `--show-toplevel` — and nothing else, because any further
  trailing newline belongs to the directory name. A value with NO terminator is not that command's
  documented shape and is refused rather than accepted. Completeness is asserted by **two** signals,
  the sentinel AND the exit status — round 13's own lesson, applied to round 13's own blind spot.
- **Where the capture can be REMOVED, remove it (#3312).** `premerge-assert.sh`'s resolver had four
  callers and every one captured it a SECOND time, so the newline was stripped twice. It now ASSIGNS
  a global and prints nothing, which makes the defect *unexpressible* at the call sites rather than
  merely absent from them: a fifth call site cannot reintroduce it by writing `$(c_stage_root)`,
  because there is nothing to capture.
- **The class is "a captured path is not the path", not the resolver the finding named — and the
  sweep proves it.** 28 path-bearing or file-locating command substitutions were examined (21 in
  `review-stage.sh`, 7 in `premerge-assert.sh`) and **3** were affected. The third is `self_dir`,
  which went through `$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)` — two nested strips — and
  mislocates `review-stage.sh`, i.e. the **ENFORCER** of the C verdict `premerge-assert.sh` refuses
  to merge without. Fixing only the named site would have left that one live.
- **A TEST HELPER THAT TRANSPORTS A PATH THROUGH A COMMAND SUBSTITUTION CANNOT CONSTRUCT ITS OWN
  SUBJECT.** Round 17's W2 case had the identical blind spot: `w2_repo` PRINTED the fixture path, and
  its LF fixture is `lane<LF>two`, where the newline is EMBEDDED and survives — so the trailing-LF
  shape, the only one that defeats the resolver, could not be presented at all. A refusal case was
  green having never been run against its worst input. The helper now ASSIGNS through `printf -v`,
  and the converted cases were re-verified against the **pre-round-17** script, where 16 of round
  17's own assertions red — which is how *"these cases pass for their own reason"* is established
  rather than asserted.
- **Make the searched directory OBSERVABLE, or a before/after proves nothing.** `premerge-assert.sh`
  prints no root in its "no stage was ever opened" refusal, so the sibling is given TWO stage
  records: AMBIGUOUS is the one branch that PRINTS the directory it enumerated. The RED control
  plants the pre-fix lossy resolver into a scratch copy of the assert (the ARTIFACT substituted,
  never a settable seam) and must NAME the sibling.

## Round 17 (W1) — the record and the report must be ONE observation

`record-author-performed` read the REPORT using the generation loaded earlier and then read the
STAGE RECORD independently. An `open --force` publishing generation **B** between those reads left
**both** final re-verifications satisfied — an unchanged report **A**, an unchanged record **B**,
each individually consistent — so the recording published `AUTHOR-PERFORMED` over **B** *without
ever inspecting B's verdict*, without `--force`, and with a `supersedes-report-nonce:` trace naming
**A**. Measured on the shipped script: `RECORD-OK … supersedes-report-nonce=<A>` at exit 0 while B
held `result: FINDINGS`, and `verdict` then reported AUTHOR-PERFORMED. **Falsifying the audit trail
is the worst failure this tool can have**, and it is the harm this change exists to prevent,
committed by the mechanism itself.

- **THIRD INSTANCE OF ONE SHAPE ⇒ MECHANISM, NOT A THIRD PATCH.** Round 9's N2 (`head-sha` from one
  read of the record, the nonce from a second) and round 12's R2 (`classify_report` reading its
  subject eight times) were the first two, each fixed where it was found. The consolidation is ONE
  primitive: `observe_record` is the only place the stage record FILE is read — the reader path used
  to open it SEVEN times and `open` five more — and `observe_stage` pairs that capture with the
  report of *the generation those bytes name*, then re-reads the record and requires it
  byte-identical. All three decision paths reason from one such observation.
- **Two reads are one observation only if something re-verifies between them.** Without the
  re-read, "nothing changed" is a claim each read makes about itself — which is precisely how two
  internally-consistent halves came to describe two different generations.
- **A defect is published as a closed KIND beside its detail sentence.** A consumer keyed on the
  prose reads a diagnostic as a control (#3312), and it fired during implementation: two legitimate
  sentences both contain the words `report-nonce`, so a text match routed a read-level failure to
  the refusal that says "this record names two".
- **Delete the parameter a function no longer uses.** With the observation required,
  `classify_report`'s report path had no remaining use, and a parameter a function does not use is
  an invitation to read again: removed, so a second observation is *unexpressible*. An unobserved
  caller gets the named non-verdict `stage not observed`, never a fresh read.
- **A moved record is its own cause on every surface** (`stage record changed mid-read`,
  `state=stage-record-changed`, `reason=stage-record-changed-mid-read`), because the operator action
  is *read it again* and not *repair the record or chmod it*. A perfectly readable record reported
  as unreadable is round 2's B7 false rationale.
- **Mechanized: `scripts/tests/lib/observation-boundary-scan.sh`**, a sibling of the round-7 emit
  scanner and the round-14 read scanner rather than a mode of either — one file per property,
  because this one asks about the CALLER (may this function read at all) and needs function-boundary
  tracking the read scanner has no notion of. It attributes every stage-file reader call to its
  function, requires the owner to be the primitive or a statement declared WITH ITS REASON (the two
  in-window re-verifications are declared: being fresh is their whole purpose), and requires each
  decision path to observe exactly once. **Its allowlist carries no in-band delimiter** — the first
  draft used `<function>|<statement>` and the very first entry, the record re-verification with its
  `||`, was truncated and excused nothing, the same defect `read-boundary-scan.sh` hit with its
  reason field — so the channel was REMOVED (an `@in <function>` scope directive) rather than the
  delimiter made rarer.
- **Deleted with a stated replacement:** the file-reading `read_field` (every record field now comes
  from the one capture), the file-reading half of the line counter, and
  `record-author-performed`'s second pair of record-read refusals — which had always been
  unreachable, because the observation refuses on both states earlier under the same reason token.
  Subtraction cannot introduce a false pass.
