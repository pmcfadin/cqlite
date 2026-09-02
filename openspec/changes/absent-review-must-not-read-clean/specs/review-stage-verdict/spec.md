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

#### Scenario: a verdict describes a state the report actually held
- **WHEN** the report of record is REPLACED while the verdict is being classified
- **THEN** the verdict SHALL be derived from ONE observation of that file, so no verdict is assembled
  from fields drawn from DIFFERENT versions: reading the `result:` token, the disclosure and each of
  `performed-by`, `reason` and `evidence` as separate observations let `AUTHOR-PERFORMED` be reported
  from working that NO SINGLE SNAPSHOT contained — one version's usable `reason` beside another's
  usable `evidence`, each version invalid on its own
- **AND** an observation of that file that cannot be classified is a NON-VERDICT, never a permissive
  fall-through, and it is reported as UNREADABLE rather than ungrammatical: the bytes were not
  obtained, so nothing may be asserted about the content
- **AND** the `<key>: <value>` field grammar SHALL have ONE implementation, shared by the snapshot
  reader and the file reader, since a second implementation's agreement is only knowable by testing it
- **AND** the consumer that guards a write on the report's BYTES and decides by its VERDICT SHALL use
  the SAME observation for both, so the token guarding the write cannot classify a state those bytes
  never held

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

#### Scenario: `report=` publishes a path that EXISTS, on a repository path containing `=`
- **WHEN** the repository root legally contains an `=` (e.g. a checkout at `.../eq=path/lane`)
- **THEN** the verdict line's `report=` SHALL carry the report-of-record path VERBATIM — the value
  SHALL name a file that EXISTS, because the grammar promises the absolute report-of-record path and
  `verdict`, unlike `open`, publishes no separate raw path line to fall back to
- **AND** that field alone SHALL be EXEMPT from the `=`→`~` map, for exactly one reason: it is
  emitted LAST and read as the line REMAINDER, so an `=` inside it cannot create an ambiguous field
  and the anti-forgery justification for the map does not apply to it
- **AND** the exemption SHALL be COUPLED to that property STRUCTURALLY — one assertion requiring the
  field to be last AND routed through the exempt boundary — so appending a field after `report=`, or
  routing it back through the mapping boundary, reds a suite rather than silently re-corrupting the
  value or re-enabling forgery
- **AND** the exemption SHALL be CONFINED to that ONE field on that ONE line: every other `report=`
  emitter keeps the mapping boundary, because no consumer reads any of those lines as a remainder and
  one of them emits a further field AFTER `report=`, where the exemption would be unsound
- **AND** a `key=value` pair smuggled through a DIFFERENT field SHALL still be neutralised — the
  remainder rule depends on it, since an unmapped value could put a REAL `report=` pair AHEAD of the
  measured one and the reader takes the FIRST
- **AND** control-character neutralisation SHALL be UNCHANGED in the exempt boundary: every line
  break flattened and the whole C0 range plus DEL rendered visibly, so the exemption is the `=` map
  ALONE
- **AND** an `=`-free path SHALL still be published unchanged (the positive control)

#### Scenario: the C verdict is re-validated INSIDE the window it certifies
- **WHEN** the C stage is SUPERSEDED (a concurrent `review-stage.sh open --force`, or a hand edit)
  AFTER the verdict has been validated and BEFORE `premerge-assert.sh` emits its success verdict —
  the interval that holds the base-staleness advisory (bounded at 65s) and the `gh pr view` round
  trip
- **THEN** `premerge-assert.sh` REFUSES, naming what changed between the two evaluations — a check
  placed outside the window it certifies can only REPORT the harm, never prevent it (the ruling
  this repository already applied to the gate's own component-set pre-flight, roborev job 290)
- **AND** the whole evaluation SHALL be REPEATED immediately before the success emit, AFTER
  everything that can consume time and BEFORE any output a reader could take as certification
- **AND** the EARLY evaluation SHALL be RETAINED — it is what stops a run with no C verdict at all
  from paying for the advisory and a network round trip before being told so, which is the same
  remedy job 290 applied
- **AND** the repeat SHALL RESET the captured observation, so the single-observation discipline and
  the generation binding are taken AFRESH on this window's own capture rather than inherited from
  one taken before the window
- **AND** a supersede to a DIFFERENT generation that ITSELF PASSES at the same head SHALL REFUSE
  too — the comparison, not merely the repeat, is what sees that the audit which answered is not
  the audit that was validated
- **AND** a disagreement SHALL NAME THE FIELD that moved, and SHALL NEVER be resolved as a second
  opinion or last-one-wins
- **AND** no output a reader takes as certification SHALL be emitted on a refusing run — including
  inside the refusal's own prose, which may not reproduce the success marker
- **AND** an UNDISTURBED run still certifies (the positive control — a guard that reds on correct
  input is the guard agents learn to waive)
- **AND** the residual SHALL be DECLARED rather than implied: two checks cannot both be last, so
  the C window is NARROWED (to a local git measurement plus one `review-stage.sh` read) and not
  closed, and the `gh` head/state check is correspondingly no longer the last thing before the
  success emit

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
- **AND** the generated name SHALL be ATOMICALLY RESERVED before anything is written to it — created
  `O_EXCL`, so a nonce that repeats a report already on disk is RETRIED with a fresh random nonce and
  never written through: an unreserved name let `open` overwrite a HISTORICAL report and republish its
  path, handing the superseded agent that still holds it the ability to write the CURRENT verdict. This
  is not the deleted scan: the scan chose a name by TESTING EXISTENCE and wrote it in a LATER step,
  while the exclusive create IS the choice, so there is no window between deciding and claiming
- **AND** exhausting the bounded reservation attempts SHALL be a NAMED refusal that writes nothing and
  publishes no stage record, never a fallback to an unreserved name
- **AND** a reservation that a later refusal leaves unused SHALL be REMOVED, so an `open` that refuses
  leaves the stage directory as it found it
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
- **AND** the PRINTING COMMAND SHALL be a literal printer, because neutralising the value buys nothing
  if the primitive re-interprets it: every emitted line is produced by `printf` of a script-authored
  literal FORMAT and NEVER by `echo`, whose argument is a FORMAT under the inherited-environment
  option `xpg_echo` — a `\n` in a LEGAL checkout path split the one-line verdict into two whose second
  was a column-zero `REVIEW-STAGE: … RESULT: PASS` for a stage with no report, and octal `\075` put
  REAL `key=` pairs on it, defeating the `=`→`~` neutralisation outright
- **AND** that SHALL be asserted STRUCTURALLY over EVERY logical line of both scripts (the primitive
  question has one answer everywhere, while the value question is per-channel), with `echo` refused
  outright and NO allowlist — an entry could only claim that one line's data holds no backslash — and
  with every `printf` FORMAT required to be script-authored, since a data-derived format re-opens the
  identical channel through `%`
- **AND** that check SHALL declare its own scope, its own NOT-COVERED set and its own subject COUNT
  separately from the value check's, SHALL FAIL rather than report clean when it finds no subject at
  all, and SHALL be exercised by positive controls that plant a COMPOUND `echo` and a data-derived
  format and require it to red AND to name what failed
- **AND** the same completeness SHALL hold for the *READ* boundary, and SHALL be asserted
  STRUCTURALLY rather than claimed in a comment: no statement in either script may read FILE CONTENT
  except through that script's faithful-read mapping, unless the statement is NAMED in a committed
  scanner WITH ITS REASON — a claim in one place a reviewer already reads, whose STALE entries (an
  entry matching nothing) are their own FAILURE, since such an entry excuses nothing and is the
  signal that the read it described has CHANGED
- **AND** that scanner SHALL recognise BOTH ways a shell reads a file (an input redirection from a
  value, and a reading command at the START of a pipeline with a `$`-bearing operand and no
  redirection), SHALL NOT reduce command substitutions — every measured instance of this defect lived
  inside a `$( … )` — SHALL declare its own NOT-COVERED set on every run, and SHALL be exercised by a
  positive control planting the EXACT shape of a measured instance rather than a convenient one,
  because a scanner written without an assignment-prefix stripper reported CLEAN on the defect it
  exists for
- **AND** the reader of a stage record's report-nonce COUNT SHALL be THREE-valued — read faithfully /
  read FAILED / not REPRESENTABLE — with the permissive set spelled AFFIRMATIVELY as the faithful
  status alone at every caller, so a status added later refuses by construction: a record whose key
  is spelt `report-<NUL>nonce:` holds NO such line, so a faithful reader counts a TRUTHFUL ZERO,
  which is exactly the value that selects the LEGACY bare report name — and a stale legacy report
  recording `result: PASS` was reported as that stage's verdict while its CURRENT report held the
  sentinel
- **AND** the not-representable state SHALL carry its OWN refusal naming the byte and the next action
  (rewrite the record or open a fresh stage, never a chmod), never a permission-or-I/O rationale about
  a file whose permissions are fine
- **AND** a GENUINE pre-nonce record — one with no such field at all — SHALL still read the legacy
  bare report, because that is the branch the byte impersonated and a guard that broke it would red on
  correct input

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
- **AND** `report=` SHALL be read as the REMAINDER OF THE LINE, not as one whitespace-delimited
  field, because it carries an absolute PATH and a path may legitimately contain WHITESPACE: read as
  a field the value TRUNCATED at the first space and the generation binding above then REFUSED an
  otherwise VALID verdict — a false refusal on correct input
- **AND** that rule SHALL rest on `report=` being emitted LAST, which SHALL be ENFORCED rather than
  assumed: the emitter's states are DERIVED by RUNNING it, no mandatory key may follow `report=` on
  any line it can produce, and its single emit site is pinned structurally, so a field appended after
  `report=` REDS the suite instead of silently truncating verdicts
- **AND** a BARE `report=` SHALL still be REFUSED as EMPTY, so taking the remainder does not turn
  "empty" into "the rest of the line"; and a space-bearing path with a FOREIGN nonce SHALL still be
  refused (the control: the acceptance must not be reachable by ceasing to compare generations)

### Requirement: A hand-performed substitute is recorded as author-performed, never as clean

Every read of an untrusted file by these tools SHALL be FAITHFUL to the file's bytes, or SHALL
refuse: a capture that normalises its input SHALL NOT be the thing that validates it. Since a
command substitution silently discards NUL bytes, each read SHALL neutralise that byte IN THE
STREAM — never by a second observation of the same path, one direction of whose disagreement is a
false pass — and the resulting value SHALL fail every token grammar rather than satisfy one.

`review-stage.sh record-author-performed` SHALL require a substantive `--reason`, a named `--evidence`
artifact and `--performed-by author` — the ONLY accepted performer, since the reported token names
the AUTHOR and a peer audit stated as an author's is a false verdict — SHALL refuse placeholder values, and SHALL cause `verdict` to
report the DISTINCT token `AUTHOR-PERFORMED` — never `PASS`. The recorded disclosure SHALL carry the
form: *"an author's hand audit is not an independent one; weight it accordingly."* It SHALL NOT
replace a report that already RECORDS a verdict (`PASS` or `FINDINGS`) unless `--force` is passed, and
a forced replacement SHALL record the replaced token in the new report. That check SHALL PREVENT the
replacement rather than report it: the observation it decides on SHALL be re-verified immediately before
the report is installed, and any change to the report in between SHALL refuse by name — including under
`--force`. And it SHALL treat an UNREADABLE prior report as a verdict that is UNKNOWN rather than absent:
the replacement SHALL proceed only where the prior state was AFFIRMATIVELY measured as absent or present,
and SHALL otherwise refuse by its own name — `--force` included, since `--force` authorizes replacing the
verdict the operator read and an unreadable report is one nobody read.

#### Scenario: a recorded verdict is not silently replaced
- **WHEN** the stage's report already records `FINDINGS` and a substitute is recorded without `--force`
- **THEN** the recording is REFUSED, naming the recorded token, and the report is left intact
- **AND** with `--force` the new report NAMES the token it replaced, so the substitution is auditable
- **AND** a sentinel-only report is replaced with no `--force` (the normal path is unaffected)

#### Scenario: a verdict recorded while the substitute is being prepared is not overwritten
- **WHEN** a verdict is recorded into the report AFTER the already-recorded check and BEFORE the
  substitute is published
- **THEN** the recording is REFUSED naming that interleaving, NOTHING is published, and the verdict that
  arrived survives
- **AND** `--force` does not authorize it, because `--force` authorizes replacing the verdict the operator
  read, not one that arrived afterwards
- **AND** the STAGE RECORD is held to the same rule under its own cause, since the recording rewrites it:
  a concurrent re-open that published a newer generation SHALL NOT be reverted by a rewrite of bytes read
  before it

#### Scenario: a recorded verdict is SUPERSEDED, never OVERWRITTEN
- **WHEN** `record-author-performed` records a substitute for a stage whose report holds any content
- **THEN** the substitute SHALL be written to a FRESHLY RESERVED report generation and the stage record —
  written LAST — SHALL name it, so no write of this tool has the report of record as its destination
- **AND** the previous generation's report SHALL remain on disk, readable, whatever it holds — including a
  verdict a late reviewer wrote at an instant after every check has run
- **AND** the new report and the `RECORD-OK` line SHALL name the generation superseded
  (`supersedes-report-nonce:`), so the surviving verdict is findable rather than merely retained
- **AND** the record rewrite SHALL carry every other field through VERBATIM — `head-sha:` SHALL NOT be
  re-stamped and `reopen-count:` SHALL NOT be incremented — and SHALL be MEASURED before it is written
  (exactly one `report-nonce:` line, reading back as the generation reserved), refusing under its own
  cause otherwise

#### Scenario: a byte the capture cannot carry does not become a verdict
- **WHEN** the report of record, or the stage record, or a `--c-verdict` file holds a NUL byte
- **THEN** the verdict is a NON-VERDICT naming that byte, and NO reader is redirected and NO
  merge-proceeding token is reported — in particular a report whose bytes are `res<NUL>ult: PASS`,
  which holds no column-zero `result:` line at all, SHALL NOT read as `PASS`
- **AND** a record whose `report-nonce:` value carries a NUL SHALL be a RECORD defect that derives no
  path, never a valid token naming another generation's report
- **AND** `premerge-assert.sh` SHALL refuse a `--c-verdict` token carrying a NUL, since a token is
  matched against a closed set by string equality
- **AND** the same content WITHOUT the NUL still certifies, so the check does not red on correct input

#### Scenario: a normalising transform does not supply the value it validates
- **WHEN** a `--c-verdict` file, a gate-of-record summary, or a stage record carries an ANSI escape
  sequence INSIDE a value the reader validates — a token spelt `PA<ESC>[31mSS`, a `head-sha:` spliced
  mid-sha
- **THEN** the read SHALL refuse BY NAME, naming the escape, and SHALL NOT report the value that
  deleting the escape would produce; in particular `PA<ESC>[31mSS` SHALL NOT read as `PASS`
- **AND** the refusal SHALL be decided BEFORE the grammar checks that read the normalised fields,
  since a check placed after them could only report — and for this shape would report nothing, the
  run having certified
- **AND** the diagnostic SHALL render what the FILE holds, never the normalised line, so it does not
  assert a clean `RESULT: PASS` beside a refusal about an escape
- **AND** colour that BRACKETS a key or a value SHALL still certify, since a coloured gate-summary
  capture is legitimate input (#3400) and a guard that reds on correct input is the guard agents
  learn to waive
- **AND** a trailing CARRIAGE RETURN SHALL remain tolerated at BOTH readers of the shape: it removes
  one byte where nothing follows, so it can separate but never join, and refusing it at one reader
  only would be the reader divergence the differential exists to detect

#### Scenario: an unreadable prior verdict is not replaceable
- **WHEN** the stage's report cannot be READ (permission or I/O) and a substitute is recorded
- **THEN** the recording is REFUSED under its OWN cause, naming the state that could not be read, and the
  report is left byte-intact
- **AND** `--force` does not authorize it either, because `--force` authorizes replacing the verdict the
  operator read, and an unreadable report is one nobody read
- **AND** a VERIFIED-ABSENT report is still replaced with no `--force` (nothing recorded is destroyed), so
  the permissive branch is a named measurement and not "everything that is not present"

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

### Requirement: A checkout path this grammar cannot carry SHALL be refused, never published wrong

`review-stage.sh` SHALL refuse to operate in a checkout whose resolved repository root does not
survive the one-line renderer UNCHANGED, at the single site where that root is resolved, so every
subcommand inherits the refusal and no subcommand publishes a report path that cannot be opened.

#### Scenario: a newline-bearing checkout path is refused by name
- **WHEN** the repository root contains an LF or a CR
- **THEN** every subcommand SHALL REFUSE (usage class, exit 64) with NOTHING read and NOTHING
  written — no stage directory, no report, no record
- **AND** the refusal SHALL name the NEWLINE specifically, because its harm differs in KIND: such a
  value SPLITS across physical lines (leaving a line with no `REVIEW-STAGE: ` anchor) as well as
  being published flattened to a path that does not exist
- **AND** `verdict` SHALL publish NO `report=` field and NO `RESULT:` token for such a checkout — a
  wrong path on that line is worse than no line, because that line is what a consumer binds to
- **AND** round 11's declaration that such a path is unrepresentable *and never arrives* is
  WITHDRAWN: it DOES arrive, because git resolves the root of whatever checkout the tool runs in

#### Scenario: the rule is the renderer's own answer, not a character list
- **WHEN** the root holds any other byte or whitespace run the one-line renderer rewrites (a tab,
  another control character, a run of spaces, a leading or trailing space)
- **THEN** it SHALL be REFUSED under the representability cause, because the published path would
  name a file that does not exist whatever byte caused the rewrite
- **AND** the check SHALL be expressed as *the root survives the renderer unchanged*, so it cannot
  drift from that renderer the way a hand-written class of characters would
- **AND** the newline rationale SHALL NOT be reported for such a path (a false rationale is worse
  than a vague one)

#### Scenario: a space-bearing checkout still works (the false-refusal control)
- **WHEN** the repository root legally contains a SPACE (a checkout at `.../work tree`)
- **THEN** the stage SHALL open and `verdict` SHALL publish the WHOLE space-bearing path, spaces
  included, and that published path SHALL name a file that EXISTS

#### Scenario: there is no opt-out
- **WHEN** an environment variable is sought to proceed anyway
- **THEN** none SHALL exist: a checkout is always renamable, so an escape hatch could only buy a
  published path that cannot be opened

### Requirement: Every decision SHALL rest on ONE coherent observation of the stage

The stage record and the report of record SHALL be observed TOGETHER — the record's bytes, the
generation those bytes name, and THAT generation's report — by one primitive that RE-VERIFIES the
record between the two captures. Every decision path (`verdict`, `status`,
`record-author-performed`) SHALL reason from one such observation and SHALL NOT read a stage file
for itself.

#### Scenario: a generation published between the two reads cannot be superseded unseen
- **WHEN** an `open --force` publishes a new generation B between the read that names the current
  report and the read of the record that a recording will republish
- **THEN** `record-author-performed` SHALL REFUSE, naming the CHANGE, and SHALL write NOTHING that
  is published — the stage record SHALL still name B and `verdict` SHALL still report B's verdict
- **AND** it SHALL NOT record that any other generation was superseded: a
  `supersedes-report-nonce:` trace naming a generation this call did not inspect is worse than no
  trace, because it makes the audit trail affirmatively false
- **AND** `--force` SHALL NOT cover it: `--force` authorizes replacing the verdict the operator
  READ, never one that arrived afterwards in a generation nobody read

#### Scenario: a record that moves inside the observation is a NAMED refusal
- **WHEN** the record changes between the capture of its bytes and the read of the report those
  bytes name
- **THEN** the observation SHALL be DISCARDED and reported as `stage record changed mid-read`, with
  its own `state=stage-record-changed` and its own write-side `reason=stage-record-changed-mid-read`
- **AND** it SHALL NOT be reported as `stage record unreadable`: the record was perfectly readable,
  and the operator action is to read it again rather than to repair or chmod it

#### Scenario: the trace names the generation actually superseded (the control)
- **WHEN** an undisturbed `--force` supersession runs over a recorded verdict
- **THEN** it SHALL succeed, SHALL record `replaced-verdict:` and SHALL name in
  `supersedes-report-nonce:` EXACTLY the generation it inspected, both on its `RECORD-OK` line and
  in the published report's own trace

#### Scenario: no decision path may read a stage file for itself
- **WHEN** a decision path is given a read of the stage record or the report of its own
- **THEN** a structural guard SHALL FAIL, naming the reader, the function and the line, unless that
  statement is DECLARED in the guard with its reason
- **AND** each decision path SHALL take exactly ONE observation: none means it reasons from an
  observation it did not take, several means two observations
- **AND** the guard SHALL declare what it does not cover on every run, and SHALL REFUSE a subject
  for which no primitive is declared rather than reporting it clean
