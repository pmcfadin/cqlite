# Spec delta: review-stage verdict (issue #3751)

## ADDED Requirements

### Requirement: A delegated review stage's verdict is an artifact, pre-stamped at spawn

The system SHALL provide `scripts/flow/review-stage.sh open <kind> --issue <N> --agent <type>` which
creates the stage's report-of-record file **before** the agent is spawned, pre-stamped with a
non-verdict sentinel recording `spawned-at`, `agent`, `issue`, and a `deadline`.

#### Scenario: silence is a readable state, not an absence
- **WHEN** a stage is opened and the spawned agent writes nothing
- **THEN** the report file EXISTS and its recorded result is the sentinel, not an empty or missing file

#### Scenario: the report path is derived, never caller-supplied
- **WHEN** any subcommand resolves the report of record for `<kind>`/`<issue>`
- **THEN** the path is `<repo-root>/.review-stage/issue-<N>/<kind>.<nonce>.md` (a bare `<kind>.md`
  is READ, never written, for a record predating the nonce), computed
  identically by the writer and by EVERY reader, with NO override flag — so no caller-controlled
  component enters a path this tool builds, reads or writes
- **AND** `<kind>` (`[A-Za-z0-9][A-Za-z0-9_-]*`) and `<issue>` (decimal digits only) are the WHOLE
  path-input surface and are refused by name at ONE boundary — no `/`, no `.`, no leading dash, no
  CR/LF
- **AND** the stage record does NOT carry the path as a readable field: a value split across lines
  by a newline-bearing path was read as its PREFIX and could select a DIFFERENT pre-existing report
  recording `PASS`, so the second source is REMOVED rather than reconciled
- **AND** no `mkdir` can create a directory outside the checkout, because the parent is derived
  (it once could: the parent was created BEFORE containment was verified)

#### Scenario: the report path is verified gitignored, fail-closed
- **WHEN** `open` resolves a report path that `git check-ignore` does not confirm as ignored
- **THEN** it REFUSES to write, naming the path and the reason
- **AND** the refusal is exit-nonzero, so a mid-gate tree mutation (#2926) cannot be caused by this tool

#### Scenario: re-opening does not silently reset the clock
- **WHEN** `open` is called for a stage that is already open, without `--force`
- **THEN** it REFUSES, and the original `spawned-at` is preserved

### Requirement: An absent report is reported as NOT-RUN and is non-passing

`review-stage.sh verdict <kind> --issue <N>` SHALL emit exactly one line of a CLOSED grammar:
`REVIEW-STAGE: <kind> RESULT: <token> elapsed=<secs> deadline=<secs> agent=<type> report=<path>`
with `<token>` from `{PASS, FINDINGS, NOT-RUN, AUTHOR-PERFORMED}`, matched by string equality on the
first word, and with any unrecognised, empty, unreadable, sentinel-only, ungrammatical, absent, or
never-opened state reported as `NOT-RUN`. The recorded result SHALL be read from a `result:` line ANCHORED AT
COLUMN ZERO, of which there SHALL be EXACTLY ONE: the report body is author-controlled text that
carries example verdict lines BY DESIGN (the pre-stamped sentinel has to show the agent the spelling,
and a review report quotes other reports), so an indented, quoted or bulleted copy is DATA and not the
record; and several column-zero records is `NOT-RUN`, naming the count, because resolving them by
ORDER is not a rule in either direction.

#### Scenario: the result line is read at column zero only
- **WHEN** the report's only `result:` line is indented, quoted or bulleted
- **THEN** the verdict is `NOT-RUN`, naming the absent result line — never the value that copy carries
- **AND** an ordinary column-zero `result:` line is still read (the anchor is not a refusal of all input)

#### Scenario: exactly one column-zero result line is a record
- **WHEN** the report carries SEVERAL column-zero `result:` lines — a stale `PASS` with an appended
  `FINDINGS`, or the reverse
- **THEN** the verdict is `NOT-RUN`, naming the COUNT, and NEITHER line's value is reported: resolving
  several records by ORDER is not a rule, and a last-wins read is no better than a first-wins one
- **AND** zero and several remain DISTINCT causes, because the operator action differs
- **AND** a report with ONE column-zero line plus any number of indented or quoted copies still reads
  its verdict (the count is over RECORDS, not over occurrences of the word)
- **AND** the same rule is asserted DIFFERENTIALLY against `premerge-assert.sh`'s verdict-line reader
  over one shared table of adversarial shapes, since two readers of one shape have diverged once per
  axis and a second implementation's agreement is only knowable by testing it

#### Scenario: a stage that produced nothing is never clean and never empty-findings
- **WHEN** the stage's report is sentinel-only
- **THEN** the verdict token is `NOT-RUN` and the exit status is non-zero
- **AND** the token is neither `PASS` nor any value a consumer may read as a passing verdict

#### Scenario: each NOT-RUN cause is named
- **WHEN** the report is absent / unreadable / empty / ungrammatical / the stage was never opened
- **THEN** the `NOT-RUN` token carries a parenthesised cause distinguishing that state from the others
- **AND** an UNREADABLE report is its own cause, not `report empty`: the operator fix is `chmod`, not
  the agent, and calling it ungrammatical would assert something about content never observed
- **AND** a STAGE RECORD that does not name which report is current is its own cause too
  (`stage record unreadable: <what>`) — no report was identified, so neither `report absent` nor
  `report ungrammatical` may be claimed about one, and the operator action is the record itself

#### Scenario: an unrecognised token is not passed through
- **WHEN** a report records a result token outside the closed set
- **THEN** the verdict is `NOT-RUN (report ungrammatical: …)`, never the unrecognised token

### Requirement: The deadline is visible and advisory

`review-stage.sh status <kind> --issue <N>` SHALL report elapsed time, the deadline, and whether the
report is still sentinel-only; past the deadline it SHALL state the elapsed time and the fact that
nothing was produced. The deadline SHALL NOT change the verdict.

#### Scenario: a stage past its deadline does not look hung
- **WHEN** a stage is sentinel-only past its deadline
- **THEN** `status` names the elapsed time and that no report was produced

#### Scenario: each status state names its own cause
- **WHEN** `status` reports a stage whose report is unreadable, or whose report RECORDS a `NOT-RUN`
  of its own with a stated cause
- **THEN** `state=` names THAT cause (`report-unreadable`, `not-run-self-reported`, and
  `stage-record-unreadable` for a record that does not name its current report) and never
  `report-ungrammatical` — the operator action differs per cause (`chmod` / act on what the agent
  said / re-write the verdict line), and a wrong remediation signal is worse than a vague one, since
  it is what stops the operator looking
- **AND** the state set is CLOSED and enumerated, with the several `report ungrammatical: <what>`
  variants deliberately sharing ONE state because their operator action is the same
- **AND** the enumeration is checked against the cause literals DERIVED from the source, so a new
  cause that is not mapped reds the suite instead of being mislabelled as self-reported

#### Scenario: a late report is still a report
- **WHEN** a real report is written after the deadline
- **THEN** the verdict is derived from its content, not from the deadline

### Requirement: The merge point fails closed on a missing C verdict

`scripts/flow/premerge-assert.sh` SHALL accept `--c-verdict <path|AUTO>`, SHALL treat its absence as a
usage failure (exit 3), and SHALL determine whether C is required by MEASURING WHAT THE BRANCH DOES:
the branch's own DIFF between merge-base(`origin/main`, `<certified>`) and `<certified>`, excluding
`openspec/changes/archive/**` — non-empty ⇒ design-routed ⇒ C required; empty ⇒ C not applicable,
reported affirmatively. A plain LISTING of `openspec/changes/` SHALL NOT be used: `origin/main` carries
`archive` plus sibling lanes' in-flight change directories, so a listing reads design-routed for EVERY
branch and measures nothing. The base SHALL be the MERGE-BASE, never `origin/main`'s tip (#3392), the
pathspec SHALL be REPOSITORY-ROOT-ANCHORED so the answer does not depend on the caller's working
directory, and any failure to measure SHALL be `UNMEASURED` and TREATED AS REQUIRED.

#### Scenario: an absent C verdict cannot reach a merge on a design-routed branch
- **WHEN** the branch's diff against the merge-base touches `openspec/changes/` outside `archive/**`
  and the C verdict is absent or `NOT-RUN`
- **THEN** `premerge-assert.sh` REFUSES, naming the stage and the cause

#### Scenario: routing is measured, not asserted by the caller
- **WHEN** the branch's diff against the merge-base touches no `openspec/changes/` path outside `archive/**`
- **THEN** the script reports `c-verdict: NOT-APPLICABLE (no openspec change on branch)` affirmatively
- **AND** no caller-supplied flag value can declare C inapplicable on a branch that carries one

#### Scenario: the routing answer does not depend on the caller's working directory
- **WHEN** `premerge-assert.sh --c-verdict AUTO` is invoked from a SUBDIRECTORY of the repository on a
  branch that touches `openspec/changes/` outside `archive/**`
- **THEN** the routing measure reports `REQUIRED`, naming the change it found, exactly as it does from
  the repository root, and the merge still REFUSES for want of a C verdict
- **AND** the pathspec SHALL carry git's `:(top)` root anchor: `diff.relative=false` governs the OUTPUT
  path prefix and NOT pathspec interpretation, so it is NOT a substitute — a bare pathspec is
  interpreted relative to the caller's cwd, which made a design-routed branch measure
  `NOT-APPLICABLE` and PROCEED with no C verdict
- **AND** `diff.renames=false` and `diff.relative=false` SHALL both REMAIN pinned: `:(top)` anchors what
  is SELECTED, `diff.relative=false` keeps what is PRINTED root-relative, which the `archive/**`
  exclusion and the slug extraction depend on
- **AND** an ORACLE-routed branch invoked from a subdirectory SHALL still report `NOT-APPLICABLE` (the
  fail-open control: anchoring the pathspec must not widen the measure)

#### Scenario: a missing flag is loud
- **WHEN** `--c-verdict` is omitted entirely
- **THEN** the script exits 3 (usage) rather than defaulting to "not required"

#### Scenario: a symlinked write path is refused, not followed
- **WHEN** the report path, the stage-record path, or any component under `.review-stage/` is a SYMLINK
- **THEN** `open` (and `record-author-performed`) REFUSE naming the symlink and the component, the link
  target is left UNTOUCHED, and the ordinary non-symlinked path still succeeds (the positive control)

#### Scenario: the temporary write path cannot be pre-planted
- **WHEN** `open` or `record-author-performed` writes a record
- **THEN** the write goes through a same-directory temporary file whose name is UNPREDICTABLE (not
  derivable from the record path plus a pid), created and opened in ONE `O_CREAT|O_EXCL` step, written
  through the ALREADY-OPEN descriptor, and `mv -f -T`'d into place — so no path is re-resolved between
  validation and writing, and a symlink planted at the temporary name is REFUSED rather than followed
- **AND** the rename SHALL carry `-T` / `--no-target-directory`, so it replaces the EXACT destination
  NAME: a plain `mv -f` puts the temporary file INSIDE a destination that is (or becomes) a directory
  or a symlink-to-one and EXITS 0, landing the write outside the verified path while reporting success
- **AND** `-T` SHALL be REQUIRED, never attempted: there is NO fallback to a plain `mv -f`, and a host
  whose `mv` lacks it gets a NAMED refusal from every write with NOTHING written, the missing option
  named — a fallback would restore the defect on exactly the hosts that cannot detect it. The GNU
  coreutils dependency is a stated HOST PRECONDITION of the tool
- **AND** the temporary path is verified gitignored BEFORE it is created, on the exact name that is then
  created, so the verification has no time-of-check/time-of-use gap of its own
- **AND** the failure to create one EXCLUSIVELY is a NAMED refusal (`reason=tempfile-not-created`) with
  NOTHING written — never a fallback to a predictable name, which is the hole this removes
- **AND** the ordinary write still succeeds and the record is a regular file (the positive control)

#### Scenario: archiving a completed change is not design-routed
- **WHEN** the branch's only change under `openspec/changes/` is a real move of a live change
  directory into `openspec/changes/archive/`
- **THEN** the routing measure reports `NOT-APPLICABLE (no openspec change on branch)` and the merge
  proceeds — a deletion is not a routing signal, and refusing here would be a false refusal on
  doctrine-mandated input
- **AND** an ADDED or MODIFIED path under a live `openspec/changes/<slug>/` still routes to C

#### Scenario: an AUTO-located stage is bound to the certified tree
- **WHEN** `--c-verdict AUTO` locates a stage in a worktree whose `HEAD` is not the certified commit
- **THEN** `premerge-assert.sh` REFUSES, naming the divergence — every lane is a worktree of one shared
  `.git`, so a peer lane's certified commit resolves from any lane and resolvability is not provenance
- **AND** the same stage at the worktree's own `HEAD` certifies (the positive control)

#### Scenario: the accepted verdict comes from the record the binding was validated on
- **WHEN** the stage record is replaced between the `head-sha:` validation and the read that resolves
  which report is current
- **THEN** `premerge-assert.sh` REFUSES, naming that the record changed while its verdict was being read
  — two reads of one record are two different facts, so a verdict from a different GENERATION (possibly
  bound to a different commit) may not be accepted under a binding checked on the old one
- **AND** the `head-sha:` SHALL be parsed from the SAME observation the comparison is made against,
  never from a second read of the file
- **AND** an undisturbed record still certifies (the positive control)

#### Scenario: the accepted verdict NAMES the generation the binding was validated on
- **WHEN** the stage record is replaced with a foreign generation for exactly the span in which the
  verdict is read, and replaced BACK before the byte comparison (an A-B-A interleave, so the two
  observations are byte-IDENTICAL and that comparison cannot see it)
- **THEN** `premerge-assert.sh` REFUSES, naming that the verdict came from a DIFFERENT GENERATION of
  the stage — the `report=` field of the accepted verdict SHALL carry the `report-nonce:` of the SAME
  captured observation the `head-sha:` was parsed from, because equality of two observations is not
  identity of the thing observed at a third instant
- **AND** that expected nonce SHALL be derived from that one capture, never from a fresh read
- **AND** NOTHING SHALL be passed INTO `review-stage.sh` to achieve it: the value is read OUT of the
  verdict line, so the control channel removed with `--report` is not rebuilt from the other end
- **AND** the byte comparison SHALL be RETAINED as defence in depth — it catches an edit under the
  SAME nonce, and a vanished record, which the generation match cannot
- **AND** every state that cannot be bound SHALL REFUSE, naming which state it was: a record with no
  `report-nonce:` (the legacy bare `<kind>.md` report), several `report-nonce:` fields, a value that
  is not an alphanumeric token, a verdict reporting `report=unresolved`, and a well-formed report
  path carrying a foreign nonce
- **AND** a verdict naming the validated generation still certifies (the positive control)

#### Scenario: a stage record is bound to the commit it was opened at
- **WHEN** `open` creates or `--force` re-stamps a stage record
- **THEN** the record carries a `head-sha:` field holding the commit `HEAD` resolved to at that moment
  (or the literal `unresolved` where `HEAD` names no commit), and `--force` RE-STAMPS it — deliberately
  unlike `spawned-at`, which is PRESERVED so elapsed-since-first-spawn stays readable
- **AND WHEN** `--c-verdict AUTO` reads a stage whose recorded `head-sha:` is not the certified commit
- **THEN** `premerge-assert.sh` REFUSES, naming the recorded commit and the certified one — HEAD-equality
  binds the WORKTREE and is satisfied BY CONSTRUCTION (a lane stands at the commit it certifies), so it
  cannot see a `PASS` recorded before a further commit, an amend or a rebase
- **AND** a record with NO `head-sha:`, SEVERAL of them, or a value that is not a 40-hex sha is a NAMED
  REFUSAL, never a skip — an older record predating the field must not be readable as certifying, which
  is the gate-of-record rule (any change after the gate invalidates it) applied to the intent audit
- **AND** re-opening the stage with `--force` at the certified commit and re-running C certifies it (the
  positive control: a guard with no way past it is the guard agents learn to waive)

#### Scenario: a re-opened stage's report path cannot be written by the agent it replaced
- **WHEN** `open --force` re-opens a stage
- **THEN** the stage record carries a FRESH `report-nonce:` and the report path INCLUDES it
  (`<kind>.<nonce>.md`), so the previous, idle agent holds a STALE
  PATH and is STRUCTURALLY unable to write into the current report — a resumed agent's late write into
  the path it was originally given used to be paired with the newly stamped `head-sha:`, certifying a
  commit nobody audited
- **AND** the path `open` PRINTS (and the paste-ready clause it emits) is that new report's path,
  so a re-spawned agent is handed the file that counts
- **AND** the nonce is recorded as an OPAQUE TOKEN, never a path, and every reader derives the path from
  the record with the same function the writer used — one source of truth for which report counts, and
  no data file that can redirect a reader
- **AND** a record with NO `report-nonce:` reads as the bare `<kind>.md` — an affirmative reading of a
  record written before the field existed, which wrote exactly one report at that name, so a correct
  older record is not reported as `report absent`
- **AND** SEVERAL `report-nonce:` lines, or a value that is not a valid token, is a `stage record
  unreadable` NON-VERDICT that derives NO path at all and fabricates none in its emitted line: falling
  back to the bare name is how a stale `PASS` would be read as the current verdict
- **AND** a stage record that EXISTS and cannot be READ is that SAME NON-VERDICT on the read side and a
  NAMED REFUSAL on the write side, never the no-field reading: *read failed* and *read fine, field
  absent* are different facts, and only the second is legitimately permissive
- **AND** the nonce is GENERATED, never SELECTED from what is on disk: two concurrent `open --force`
  calls SHALL be handed two DIFFERENT report paths, so a superseded agent cannot overwrite the current
  verdict, and no scan, attempt bound or exhaustion refusal exists to be raced
- **AND** a run that cannot generate an unpredictable nonce is REFUSED by name, with no fallback to a
  predictable token
- **AND** superseded reports are LEFT on disk as history — nothing reads them, and nothing DEPENDS on
  their existence, so a deleted stage record cannot cause a new agent to be handed a path an older
  agent still holds

#### Scenario: an interrupted open cannot publish a stale verdict
- **WHEN** `open` (or `open --force`) writes its two files and is interrupted between them — by a failed
  write, or by the process being killed
- **THEN** the REPORT has been reset to the sentinel BEFORE the stage record is written, so the record is
  the PUBLICATION MARKER and every partial state is a NON-VERDICT: no record reads as `stage never
  opened`, and a record beside a sentinel report reads as `no report written`
- **AND** the previous report's verdict is NEVER paired with the newly-recorded `head-sha:` — writing the
  record first made a `result: PASS` from an audit of an OLDER tree satisfy both of the merge point's
  bindings at once, and a check could only have REPORTED that pairing, because the harm is a WRITE
- **AND** since the record carries the report NONCE beside `head-sha:` in ONE atomic write, an
  interrupted re-open leaves the ENTIRE previous stage in place — the previous report's verdict beside
  the commit it was really audited at, which is coherent and still refuses at the merge point on the sha
  — instead of destroying the audit it had
- **AND** an uninterrupted `--force` re-open still yields a usable stage that can record a fresh verdict
  (the positive control)

#### Scenario: a report-supplied value cannot carry control characters into an emitted line
- **WHEN** a report-supplied value — a self-recorded `NOT-RUN` cause, an unrecognised token, or the
  `report=` field of a captured verdict line — reaches an emitted line of `review-stage.sh` or a
  diagnostic of `premerge-assert.sh`
- **THEN** it is flattened at that ONE emit boundary so that no line break and NO non-printable
  control character survives — the whole C0 range and DEL, not only `\n`/`\r`/`\t` — with the
  line-breaking whitespace rendered as a space and every other such byte as a VISIBLE placeholder
- **AND** ordinary punctuation and non-ASCII prose pass through READABLE, because a boundary that
  mangles legitimate text is one people route around
- **AND** the boundary is DISPLAY-ONLY: every decision (the token, the exit code, the paths written,
  the stage-kind comparison) is made on the RAW value before any line is built, so it cannot change
  a verdict
- **AND** a comment or claim about that boundary states exactly what it neutralises: asserting more
  than the mechanism delivers is itself a defect, because it is what stops the next reader checking
- **AND** the boundary covers EVERY data value on an emitted line, the SUCCESS path included — not
  only the ones a reviewer has named so far: `C_SOURCE` on `PREMERGE: C-VERDICT`, and `deadline=`/
  `agent=`/`spawned-at=` read out of the stage record, were three further sites of the same class
- **AND** that completeness SHALL be asserted STRUCTURALLY, not site by site: a committed scanner
  requires every interpolated value on the operator-facing channel to be either ROUTED through a
  boundary or NAMED IN AN ALLOWLIST WITH ITS REASON, DECLARES its own scope and subject count at run
  time, and is exercised by a POSITIVE CONTROL that plants a bypass in a throwaway copy and requires
  the scanner to red AND to name the planted symbol — because a scanner that flags nothing exits
  exactly as a clean one does
- **AND** that scanner SHALL examine an output command WHEREVER A STATEMENT CAN BEGIN, not only at the
  start of a line: anchored at line start it reported both scripts CLEAN while three real bypasses sat
  in COMPOUND statements (`[ -n "$delta_file" ] && printf … "$delta_file"`, `[ -z "$extra" ] || emit
  …`, and a one-line `case` arm), which is the same defect class the scanner exists to catch, one level
  up
- **AND** the constructs it recognises SHALL be PRINTED as a closed list at run time, and its remaining
  error directions declared with them — so a construct it does not know is a visible gap rather than an
  inferred one
- **AND** it SHALL carry a positive control that plants a bypass inside a COMPOUND statement and
  requires the scanner to name it, since a control that only plants at the start of a line cannot
  distinguish the widened scanner from the blind one

#### Scenario: an audit counter at its ceiling does not restart
- **WHEN** the stage record's re-open counter is at the widest value this tool can compare and the
  stage is re-opened
- **THEN** the counter is HELD at that value rather than incremented past the bound, the hold is
  NAMED in a note, and both the `open` and `status` lines render it as AT LEAST that many — a counter
  that silently restarts at 1 is a false audit trail
- **AND** the re-open still SUCCEEDS: a cosmetic audit number at its ceiling is not a reason to
  refuse a spawn
- **AND** one below the ceiling still INCREMENTS, and claims no hold (the positive control)
- **AND** `status` SHALL report the counter the record holds, so the two surfaces cannot disagree
  about it

#### Scenario: an unmeasurable clock is not a permissive answer
- **WHEN** `status` reads an `elapsed` or `deadline` value that is not a decimal number of seconds
- **THEN** it reports `past-deadline=unknown` rather than comparing them — a two-valued guard testing
  only for the literal `unknown` let any other non-numeric value reach an integer comparison, which
  emitted a raw shell diagnostic into the anchored output block and then took the permissive branch,
  an answer derived from a comparison that never ran
- **AND** being DIGITS SHALL NOT be treated as being COMPARABLE: bash's `[ -gt ]` is a fixed-width
  comparison that refuses an all-digit value above int64 with the same raw shell diagnostic, and
  `$(( ))` does not refuse at all but WRAPS SILENTLY, so a value from argv or from the stage record
  SHALL be bounded AFFIRMATIVELY — at most a stated maximum number of decimal digits, with no
  leading zero, since a zero-padded value is read as OCTAL by `$(( ))` and as DECIMAL by `[ ]` and
  one value SHALL NOT have two readings inside one script
- **AND** that bound SHALL be checked by ONE predicate at EVERY boundary where such a value reaches
  a fixed-width operation — the flag, both operands of the elapsed subtraction (INCLUDING the
  clock's own reading, which is validated nowhere else), both operands of the past-deadline
  comparison, the epoch a re-open copies forward, and the reopen counter — never per-site, and never
  as a test for the values that happen to break
- **AND** an out-of-bound value from ARGV SHALL be a NAMED usage refusal at the boundary that writes
  nothing, while one read from the RECORD SHALL yield `elapsed=unknown` / `past-deadline=unknown`
  with the record's own text still DISPLAYED verbatim, so a hand edit stays visible in the audit
  trail — what is affirmative is the COMPARISON, not the display
- **AND** `status` being ADVISORY SHALL NOT license a permissive answer: `no` is the permissive
  value, and a verdict-irrelevant report derived from a comparison that never happened is still
  wrong

#### Scenario: a sibling stage's PASS cannot certify C
- **WHEN** the verdict line names a stage kind other than `c`, or omits any of
  `elapsed=`/`deadline=`/`agent=`/`report=`, or carries one of them twice
- **THEN** `premerge-assert.sh` REFUSES as ungrammatical, naming what was wrong — the stage kind is
  compared by STRING EQUALITY and each mandatory key must appear EXACTLY ONCE
- **AND** each mandatory key's VALUE SHALL be measured, not merely counted: a key that is PRESENT but
  carries nothing (a bare `report=`, an empty `elapsed=`/`deadline=`/`agent=`) is REFUSED naming the
  field, because a count is an affirmative measurement of PRESENCE and of nothing else
- **AND** the permitted value set SHALL be DERIVED FROM WHAT THE EMITTER CAN PRODUCE, measured by
  running `review-stage.sh verdict` through EVERY state it has — so `elapsed`/`deadline` admit decimal
  digits INCLUDING `0` or the literal `unknown`, and `agent`/`report` need only be non-empty, which
  admits the honest `unknown`/`unresolved` values an unreadable stage record yields. A validator
  written from what looks reasonable would REFUSE three legitimate emitter outputs, and a guard that
  reds on correct input is the guard agents learn to waive

### Requirement: A hand-performed substitute is recorded as author-performed, never as clean

`review-stage.sh record-author-performed` SHALL require a substantive `--reason`, a named `--evidence`
artifact and `--performed-by author` — the ONLY accepted performer, since the reported token names
the AUTHOR and a peer audit stated as an author's is a false verdict — SHALL refuse placeholder values, and SHALL cause `verdict` to
report the DISTINCT token `AUTHOR-PERFORMED` — never `PASS`. The recorded disclosure SHALL carry the
form: *"an author's hand audit is not an independent one; weight it accordingly."* It SHALL NOT
replace a report that already RECORDS a verdict (`PASS` or `FINDINGS`) unless `--force` is passed, and
a forced replacement SHALL record the replaced token in the new report. That check SHALL PREVENT the
replacement rather than report it: the observation it decides on SHALL be re-verified immediately before
the report is installed, and any change to the report in between SHALL refuse by name — including under
`--force`.

#### Scenario: a recorded verdict is not silently replaced
- **WHEN** the stage's report already records `FINDINGS` and a substitute is recorded without `--force`
- **THEN** the recording is REFUSED, naming the recorded token, and the report is left intact
- **AND** with `--force` the new report NAMES the token it replaced, so the substitution is auditable
- **AND** a sentinel-only report is replaced with no `--force` (the normal path is unaffected)

#### Scenario: a verdict recorded while the substitute is being prepared is not overwritten
- **WHEN** a verdict is recorded into the report AFTER the already-recorded check and BEFORE the
  substitute is installed
- **THEN** the recording is REFUSED naming that interleaving, NOTHING is installed, and the verdict that
  arrived survives
- **AND** `--force` does not authorize it, because `--force` authorizes replacing the verdict the operator
  read, not one that arrived afterwards

#### Scenario: a substitute is distinguishable from an independent audit
- **WHEN** an author-performed C is recorded and the verdict is read
- **THEN** the token is `AUTHOR-PERFORMED` and a reader grepping the passing token does not match it

#### Scenario: an unfilled template is not a disclosure
- **WHEN** `--reason` is a placeholder (`why`/`todo`/`tbd`) or carries an unsubstituted `<…>`
- **THEN** the recording is REFUSED as a usage error

#### Scenario: the classifier is as strong as the writer
- **WHEN** a HAND-WRITTEN report asserts `result: AUTHOR-PERFORMED` with the disclosure but with a
  `performed-by` other than `author` (INCLUDING `peer`), or a `reason`/`evidence` the writer would refuse as a
  placeholder, too short or an unsubstituted `<…>`
- **THEN** `verdict` reports `NOT-RUN (report ungrammatical: AUTHOR-PERFORMED …)`, naming the field and
  the defect — never `AUTHOR-PERFORMED`
- **AND** a hand-written report WITH real working still reports `AUTHOR-PERFORMED` (the positive control)

### Requirement: The mechanism is recorded, with its limits

The change SHALL commit a root-cause record stating, from committed source, that no agent in
`.claude/agents/` has `SendMessage`, that the Agent tool's terminal result is therefore the only channel,
and that naming a report path is NOT a fix for the agents (measured: effective for `spec-auditor` and
`flow-closer`, 0/3 for `rust-reviewer`).

#### Scenario: the claim made is the narrow one
- **WHEN** the record is read
- **THEN** it claims a correct CONSUMING verdict, and does not claim that flaky agents now deliver

### Requirement: The guard cannot green vacuously

`scripts/tests/test_review_stage.sh` SHALL be enrolled in the `tooling-tests` roster and SHALL contain a
positive control (a real report ⇒ `PASS`, exit 0, merge assert proceeds) alongside the absent-report case,
plus a case floor.

#### Scenario: an always-NOT-RUN implementation fails the suite
- **WHEN** the mechanism reports `NOT-RUN` for every input
- **THEN** the positive control FAILS

#### Scenario: a shrunken suite is caught
- **WHEN** the suite's executed case count falls below the committed floor
- **THEN** the suite FAILS rather than reporting zero failures
