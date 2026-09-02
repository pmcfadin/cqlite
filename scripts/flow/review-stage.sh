#!/usr/bin/env bash
#
# review-stage.sh — a delegated review stage's verdict, as an ARTIFACT (issue #3751).
#
# WHY THIS EXISTS
# ---------------
# A delegated review stage (C / rust-reviewer / coverage-reviewer / a closer, …) used to
# write NOTHING at any point in its life. Its reader therefore had only ABSENCE to reason
# from, and every consumer of an absence has to CHOOSE how to read it. Every measured
# instance so far was recorded as not-run by its own lane — the discipline held every time,
# and NO false certification has occurred — but nothing REQUIRED it, which is the gap.
#
# This is #3041's mechanism transplanted. The agent gate writes
# `RESULT: INCOMPLETE (gate did not finish)` into its summary file AT LAUNCH — before the
# slot is even granted — so a reader can never mistake a just-launched run for a certified
# one. `open` does the same for a review stage: the report-of-record file is created BEFORE
# the agent is spawned, carrying a NON-VERDICT sentinel. That converts the question from
#
#   "is there a report?"        two-valued, and the PERMISSIVE answer is the dangerous one
# to
#   "what does the report say?" three-valued, with the unmeasured state NAMED.
#
# WHAT IT DOES AND DOES NOT CLAIM (design.md §5, and the narrow claim is the true one)
# -----------------------------------------------------------------------------------
# This mechanism guarantees a correct CONSUMING verdict: an absent review is REPORTED as
# absent, with its elapsed time, and cannot be read as clean. It does NOT claim that flaky
# agents now deliver — naming a report path rescued `spec-auditor` and `flow-closer` in
# measured sessions and did nothing for `rust-reviewer` (0/3, one of them told IN WRITING
# that an absent file would be recorded as a non-review). Second declared limit, same
# direction: `verdict` establishes that a VERDICT WAS RECORDED, never that a review was
# performed — a report whose only content is `result: PASS` reads as PASS. Judging whether
# the working is real is a human's job (and, for the author-performed substitute below,
# the whole point of requiring the working to be recorded).
#
# THE VERDICT GRAMMAR IS CLOSED (#3544's lesson, applied)
# ------------------------------------------------------
#   REVIEW-STAGE: <kind> RESULT: <token> elapsed=<secs> deadline=<secs> agent=<t> report=<abs>
#
#   `report=` IS LAST BY CONTRACT, NOT BY HABIT (#3751 round 11, Q3): its value is a PATH that may
#   contain a SPACE, so `premerge-assert.sh` reads it as the REMAINDER of the line. Adding a field
#   after it would truncate every space-bearing path again; pinned by section 44l of
#   scripts/tests/test_premerge_assert.sh against THIS emitter's own output.
#
#   AND BEING LAST IS WHAT EARNS IT THE ONE EXEMPTION FROM THE '=' MAP (#3751 round 16, V2): a
#   repository root may LEGALLY contain '=', and mapping it published a path that DOES NOT EXIST,
#   so this field alone goes through `remainder_value` rather than `field_value`. The two facts are
#   pinned TOGETHER, because either one alone makes the other wrong — see `remainder_value`.
#
#   token             meaning                                            exit
#   PASS              a report was written recording no blocking finding   0
#   FINDINGS          a report was written recording >=1 blocking finding  4
#   NOT-RUN           sentinel-only / absent / empty / ungrammatical /     5
#                     never-opened  (ALWAYS carries a parenthesised cause)
#   AUTHOR-PERFORMED  a disclosed substitute with its working recorded     6
#
# `AUTHOR-PERFORMED` is reported ONLY when the working is actually there: the required
# disclosure verbatim, a `performed-by` of exactly `author`, and a `reason` and an
# `evidence` that pass the SAME placeholder judgement `record-author-performed` applies
# (`author_working_defect`, one function, called by the writer AND by this classifier). A
# report asserting the token without usable working is `NOT-RUN (report ungrammatical: …)`,
# naming the field and the defect — a non-emptiness test standing in for a validity test let
# `performed-by: nobody` / `reason: x` / `evidence: tbd` reach the PROCEEDING token (#3751
# round 1, F3).
#
# Two rules make the grammar CLOSED rather than prefix-tested: the recorded result is
# reduced to its FIRST WORD and matched by STRING EQUALITY, and any unrecognised value is
# `NOT-RUN`, never passed through. `PASS-BUT-UNMEASURED` must not satisfy a `PASS*` test.
#
# AND THE `result:` LINE IS READ AT COLUMN ZERO ONLY, EXACTLY ONCE (round 2 B1, round 3 G2)
# -----------------------------------------------------------------------------------------
# The report body is AUTHOR-CONTROLLED text that CONTAINS example verdict lines by design —
# the sentinel has to show the agent the exact spelling, and a review report routinely quotes
# another report's line. An INDENTED, quoted or bulleted `result: PASS` is therefore DATA, and
# only a line beginning at column zero is the record. This is #3312's rule (anchor the control
# token where the payload cannot reach; never pick a rarer delimiter) and the same anchor
# `premerge-assert.sh`'s `_c_verdict_awk` already uses. Before it, the sentinel's own indented
# examples were valid records held off only by `grep -m1` ORDER, so deleting the column-zero
# sentinel and appending a verdict read the TEMPLATE's `PASS` (measured). Belt as well as
# braces: the template now renders those examples behind a `| ` gutter, so they do not begin
# with the token even if this anchor were ever loosened again.
#
# THE ANCHOR ALONE WAS NOT ENOUGH, WHICH IS G2. `grep -m1` was still deciding by ORDER among
# the anchored lines, so a stale `result: PASS` followed by an APPENDED `result: FINDINGS`
# classified as PASS and a merge proceeded over recorded blocking findings. So the reader
# requires EXACTLY ONE column-zero record: zero and several are DISTINCT `NOT-RUN` causes (the
# operator action differs), and several is refused in EITHER order, because a last-wins rule is
# no better than a first-wins one. The consolidation half — and the sibling reader in
# `premerge-assert.sh`, plus the DIFFERENTIAL test that keeps the two honest — is stated at
# `classify_report` itself, beside the code the rule lives in.
#
# AND THE WHOLE REPORT IS READ EXACTLY ONCE PER VERDICT (#3751 round 12, R2). `classify_report`
# used to read its subject EIGHT times — existence, a readability probe, the body for emptiness,
# the `result:` census, the disclosure, and `performed-by`/`reason`/`evidence` each through their
# own `read_field` — so a report REPLACED between two of those reads let it assemble
# `AUTHOR-PERFORMED` from fields drawn from DIFFERENT, INDIVIDUALLY INVALID versions: working NO
# SINGLE SNAPSHOT ever contained. A verdict is a statement about a document, and assembled across
# two documents it is a statement about neither. It now takes ONE observation through
# `report_bytes` and classifies every field from that text (`read_field_from`, the one field
# grammar, which the file-reading `read_field` delegates to), and
# `record-author-performed` PASSES ITS OWN snapshot in, so the bytes its write is guarded on and
# the verdict it decides by are the same instant. This is round 9's N2 property one level down.
#
# `NOT-RUN` carries one of SEVEN named causes, because the operator action differs per cause
# and one token for seven states is the collapse this issue is about:
#   no report written          the stage is open and the report is still the sentinel
#   report absent              the stage is open and its report file is GONE
#   report unreadable          the report file exists and CANNOT BE READ (permission, I/O)
#   report empty               the report file exists and holds nothing recordable
#   report ungrammatical: <w>  a result line that is unrecognised, absent, or unsupported —
#                              INCLUDING a report holding a NUL 0x00 or SOH 0x01 byte, which is not
#                              a text record (round 13, S2). It keeps this ONE cause, and this one
#                              `status` state, deliberately: every variant sends the operator to
#                              the AGENT to rewrite the report.
#   stage never opened         no stage was ever opened for this <kind>/<issue>
#   stage record unreadable: <w>  the RECORD does not name which report is current, so no report
#                                 was identified and nothing is claimed about one (round 5, J1)
#
# `report unreadable` was the SIXTH, added in round 2 (B7) rather than folded into an existing
# cause: an unreadable file is NOT empty (the operator fix is `chmod`, not the agent) and calling
# it ungrammatical would assert something about content that was never observed. Reuse would have
# been a false rationale, which is worse than none.
#
# EVERY READ OF AN UNTRUSTED FILE GOES THROUGH `capture_map_nul` (#3751 round 13, S2), because a
# COMMAND SUBSTITUTION SILENTLY DISCARDS NUL BYTES and therefore does not merely lose information —
# it MANUFACTURES grammar. `res\0ult: PASS` holds NO column-zero `result:` line and was reported as
# `RESULT: PASS` at exit 0; a record's `report-nonce: STALE\0PASS1`, not a valid token, was read as
# the valid `STALEPASS1` and redirected the reader to a STALE report's `PASS`. A capture that
# normalises its input cannot be the thing that validates it.
#
# THAT SENTENCE WAS FALSE FOR ONE READER FOR A WHOLE ROUND, AND IS NOW MECHANIZED (#3751 round 14,
# T1). Round 13 routed `read_field` and `report_bytes` and left `count_field_lines` reading the
# stage record with `grep -c` on the FILE, so the header claimed a completeness the code did not
# have — the exact "claim broader than the mechanism" defect round 5's J3 records, one boundary
# over. `grep` is a FAITHFUL reader; the ANSWER is not: a record spelt `report-<NUL>nonce:` holds no
# `report-nonce:` line, so the count was a truthful `0` — the value that means "a pre-nonce record
# whose single report is the LEGACY bare name" — and a stale `c.md` recording `result: PASS` was
# reported as this stage's verdict while the CURRENT report held the sentinel. THREE rounds in a row
# have now found "a boundary exists and one path bypasses it" (round 7's emit sites, round 13's
# record reads, this), so the completeness is asserted STRUCTURALLY by
# `scripts/tests/lib/read-boundary-scan.sh` rather than by this sentence: round 13's own asserts
# could not see the site, because they check that the mapping appears exactly ONCE, which is a
# property of the BOUNDARY and not of its CALLERS.
#
# TWO FILES, AND WHY (the never-opened / report-absent distinction needs them)
# ---------------------------------------------------------------------------
#   <dir>/<kind>.<nonce>.md  the REPORT OF RECORD: what the agent writes, what `verdict` reads.
#                            Its name carries a per-open NONCE, so it is UNPREDICTABLE and is
#                            taken from the record — never reconstructed (both below).
#   <dir>/<kind>.stage       the STAGE RECORD: kind/issue/agent/spawned-at/deadline, plus the
#                            `head-sha:` the stage was opened AT and the `report-nonce:` that
#                            names WHICH report is current (both below).
# A single file cannot tell `stage never opened` from `report absent` — deleting it erases
# the evidence that anything was ever opened, and `verdict` still has to report an agent, a
# deadline and an elapsed time for a stage whose report has gone missing. So the two facts
# live in two files: the stage record is the proof the stage EXISTS, the report is the
# proof of what it CONCLUDED. Both are under `.review-stage/` and both are gitignored.
#
# AND THE STAGE RECORD IS THE PUBLICATION MARKER (#3751 round 4, H1). Two files cannot be
# written atomically together, so ONE of the two orders leaves a false certification behind
# when a write fails or the process is killed between them: with the RECORD first, the NEW
# `head-sha:` sat beside the PREVIOUS report, so a `result: PASS` from an audit of an older
# tree satisfied both of the merge point's bindings at once (measured — `verdict` reported
# `RESULT: PASS` exit 0 for a tree nobody had audited). So the REPORT is reset to the sentinel
# FIRST and the record is written LAST: no record reads as `stage never opened`, a record beside
# a sentinel reads as `no report written`, and every partial state is a NON-VERDICT.
#
# AND THE STAGE RECORD CARRIES THE COMMIT IT WAS OPENED AT (#3751 round 3, G1)
# --------------------------------------------------------------------------
# `premerge-assert.sh --c-verdict AUTO` locates the C stage in the CURRENT worktree and
# already refuses unless this worktree's HEAD is the certified commit. That binds the
# WORKTREE; it does not bind the ARTIFACT. The two are different questions, and the second
# one was unanswerable: nothing in the record said WHICH TREE the audit was about, so a
# `result: PASS` recorded before a further commit, an amend or a rebase persisted in
# `.review-stage/` and certified the NEW tree — open the stage, get a PASS, commit again, and
# the stale PASS still read clean at a merge point whose HEAD-equality check was satisfied by
# construction.
#
# So `open` resolves `HEAD` and records it as `head-sha:`, and the merge point requires that
# RECORDED sha to equal the certified one IN ADDITION TO its HEAD check. FAIL-CLOSED BY
# DESIGN: a record with no `head-sha:`, several of them, or an unparsable value is a NAMED
# REFUSAL at the merge point, never a skip — an older record predating the field must not be
# readable as certifying. This is the gate-of-record rule (any src change after the gate
# INVALIDATES it) applied to the intent audit: an audit of an older tree may not certify a
# newer one.
#
# UNLIKE `spawned-at`, IT IS NOT PRESERVED ACROSS `--force`. The clock is preserved because
# elapsed-since-FIRST-spawn is the number that says "this stage has produced nothing for 70
# minutes"; the head sha is RE-STAMPED because a re-opened stage hands the re-spawned agent a
# fresh sentinel and it audits the tree that is there NOW. Where HEAD cannot be resolved (an
# unborn HEAD, no commits yet) the field records the literal `unresolved` and a note says so —
# an absent field and an unmeasured one are different facts, and both refuse at the merge point.
#
# AND THE REPORT PATH IS GENERATION-BOUND, SO A RESUMED AGENT CANNOT WRITE INTO IT (#3751 round 5, J1)
# --------------------------------------------------------------------------------------------------
# `open --force` reset the report to the sentinel and re-stamped `head-sha:` — at the SAME PATH. So
# the PREVIOUS, idle agent could wake up after the reset and write its OLD-TREE verdict into that
# path, where it was now paired with the NEWLY stamped `head-sha:`, and a commit nobody audited
# passed `premerge-assert.sh`. That is not an exotic race: this issue exists BECAUSE delegated
# agents go idle and return late, so "the late agent wakes up and writes" is the expected behaviour
# of the population this mechanism serves.
#
# So every open records a `report-nonce:` and the report path INCLUDES it:
#   a nonce   ->  <dir>/<kind>.<nonce>.md   (EVERY open this version performs, first or forced)
#   NO nonce  ->  <dir>/<kind>.md           (LEGACY: read, never written — a record written
#                                            before this field existed, whose version wrote
#                                            exactly one report, at that name)
# A resumed agent therefore holds a STALE PATH and is STRUCTURALLY unable to write into the current
# report. A check could not deliver this — the harm is a WRITE, and a check placed after it could
# only report it.
#
# AND THE NONCE IS GENERATED, NEVER SELECTED (#3751 round 6, K2). The first design NUMBERED the
# generations and chose the next one by SCANNING the directory for an unused `<kind>.<gen>.md`. A
# value chosen by looking at what is already on disk is a value TWO CONCURRENT CALLERS CAN BOTH
# CHOOSE: two `open --force` runs read the same record, probe the same directory before either has
# written, pick the same generation and hand ONE report path to TWO agents — so a superseded agent
# overwrites the current verdict, FINDINGS included. A nonce makes that STRUCTURALLY IMPOSSIBLE
# rather than serialised: two concurrent opens produce two nonces and two records, the record
# written LAST is the published one (round 4, H1 — the record is the publication marker) and the
# loser's agent writes where no reader looks. A LOCK would have been the worse answer — it
# serialises a race a nonce removes, and adds a mechanism (a stale lock file, a box without
# `flock`, a holder killed mid-open) to a script whose subject is not taking the permissive branch
# when something cannot be measured. The scan, its 4096-attempt bound and its exhaustion refusal
# are DELETED: with nothing selected there is nothing to exhaust, and subtraction cannot introduce
# a false PASS. `reopen-count:` remains as the human-readable audit number — it answers a
# different question (how many spawns), and the nonce only has to be UNIQUE.
#
# BUT GENERATED IS NOT RESERVED, AND ROUND 6 DELETED THE RESERVATION ALONG WITH THE SCAN (#3751
# round 12, R1). `mktemp -u` invents a NAME and creates NOTHING, so an UNRESERVED nonce that
# repeats a report already on disk — a HISTORICAL report of this stage, deliberately kept as the
# audit trail — let this open write over that report and REPUBLISH its path in the record: a
# recorded verdict replaced by a sentinel, and the superseded agent STILL HOLDING that path handed
# the ability to write the CURRENT one. So the name is CLAIMED, not merely invented:
# `reserve_report_path` creates each candidate under `set -C` (`O_CREAT|O_EXCL`) and generates a
# FRESH random nonce on collision, under a bounded attempt count whose exhaustion is a NAMED
# refusal (`reason=report-nonce-not-reserved`) and never a fallback to an unreserved name. THAT IS
# NOT THE SCAN RETURNING: the scan SELECTED a name by testing existence and wrote it LATER — two
# steps with a window — while the create IS the choice, one operation, nothing to interleave in.
# Everything the nonce bought is intact: nothing is selected (a collision yields a fresh RANDOM
# token, never the "next" one), the token stays opaque, and the record is still written LAST.
#
# THE NONCE IS THE RECORD'S TO NAME, AND IT IS AN OPAQUE TOKEN, NOT A PATH. Round 4 (H2) removed
# the record's `report:` PATH field because a data file naming a location let a reader be
# redirected to another file; a validated alphanumeric token cannot redirect, and the readers
# derive the path from it with the SAME function `open` used, so there is exactly one source of
# truth for which report counts. It is written in the SAME atomic record as `head-sha:`, so the
# pair (the tree audited, the artifact that audits it) is published together or not at all.
# FOUR READINGS, and only two of them are a path: exactly one valid alphanumeric field is that
# nonce; NO field at all is the LEGACY bare `<kind>.md` (an AFFIRMATIVE reading of a record
# written before the field existed — that version wrote exactly one report, at that name, so
# refusing would red on correct input); anything else (several lines, a value that is not a valid
# token) is a RECORD DEFECT that derives no path at all and reports `stage record unreadable`,
# because falling back to the bare name is how a stale report's PASS would be read as the current
# verdict.
# AND THE FOURTH IS "THE RECORD COULD NOT BE READ AT ALL", WHICH IS NOT THE SECOND (#3751 round 6,
# K1). The count was taken with `grep -c … || true`, which threw away the ONE signal separating
# them — `grep` exits 1 for "read fine, no such line" and >= 2 for "could not read" — so an
# unreadable record took the LEGACY reading and an OLD report's PASS was reported as the current
# verdict. `count_field_lines` returns the count ONLY when the file was actually read; a failed
# read is the `stage record unreadable` non-verdict on the read side and a NAMED refusal
# (`reason=stage-record-unreadable`) on the write side. *read failed* and *read fine, field absent*
# are different facts and only the second one is legitimately permissive.
# SUPERSEDED REPORTS STAY ON DISK as history: nothing reads them (the record names exactly one),
# and they are what an operator opens to see what the previous agent concluded. Since round 6 (K2)
# nothing DEPENDS on their existence either — the nonce is generated, not chosen from what is
# absent — so deleting one by hand costs the audit trail and nothing else.
#
# AND THE `report=` FIELD OF THE VERDICT LINE IS NOW LOAD-BEARING FOR A CONSUMER (#3751 round 10,
# P2). `premerge-assert.sh` binds the verdict it accepts to the GENERATION it validated by
# requiring that field to end in the `report-nonce:` it read from the stage record: an ABA
# replacement of the record — swapped to another generation for the span in which this script
# reads it, and swapped back — leaves that consumer's byte comparison satisfied, and the returned
# nonce is what exposes it. So `report=` is not merely a diagnostic for a human: DO NOT drop it,
# abbreviate it, or stop deriving it from the nonce. Nothing is passed INWARD to make this work
# (round 4's H2 `--report` stays deleted); the consumer reads the value OUT of this line.
#
# AND BOTH PATHS ARE DERIVED — THERE IS NO `--report` (#3751 round 4, H2/H3)
# ------------------------------------------------------------------------
# The report of record is ALWAYS `<repo-root>/.review-stage/issue-<N>/<kind>.md`, computed the
# same way by `open` and by every reader. The `--report <path>` override is REMOVED, and this is
# a DELIBERATE NARROWING of the surface: it was mandated by no requirement and used by nothing
# (no agent definition, no skill, no script, no call site — measured by grep, not assumed), and
# it was the caller-controlled component that produced a finding CLUSTER across four review
# rounds. Two of them, both closed BY CONSTRUCTION rather than by a check: the path was written
# RAW into the LINE-ORIENTED stage record, so a LEGAL newline-bearing filename split across lines
# and the reader took the PREFIX — which could name a DIFFERENT pre-existing report recording
# `PASS`; and the report's parent directory was created BEFORE repository containment was
# verified, so a REFUSED outside-the-repository path still created directories outside the
# checkout. With the path derived there is no newline to split on and no containment question to
# answer, and `<kind>`/`<issue>` — validated strictly at ONE boundary — are the whole remaining
# path-input surface. If a caller ever genuinely needs a custom location, re-add the flag WITH
# that hardening (a refused CR/LF, containment verified BEFORE any `mkdir`); do not re-add it as
# it was.
#
# BOTH PATHS ARE VERIFIED GITIGNORED, FAIL-CLOSED
# -----------------------------------------------
# These files are written MID-RUN, routinely while the gate of record is running, and #2926
# FAILs a gate closed on ANY mid-run tree mutation. A gitignored path is invisible to
# `tree-integrity` (which derives its identity from tracked content plus HEAD); an
# untracked-but-NOT-ignored file shows as `??` and WOULD dirty the run — and would make
# `premerge-assert.sh` refuse on `dirty: yes` (#3648). A leading dot proves nothing:
# measured in this repo, `.frozen-work.md` is NOT ignored while `gate.log` is. So this
# script ASKS GIT (`git check-ignore -q`) rather than assuming, and REFUSES to write a path
# git does not confirm. A path outside the repository is also a refusal, not an exemption:
# `check-ignore` cannot confirm it, and "cannot tell" must never take the permissive branch.
#
# AND A SYMLINK IS REFUSED, NEVER FOLLOWED (#3751 round 1, F5)
# -----------------------------------------------------------
# `check-ignore` answers about a LEXICAL path; a WRITE follows symlinks. So the check above was
# satisfiable while the write landed somewhere else entirely — measured: an ignored but SYMLINKED
# report path clobbered a TRACKED file and `open` reported `OPEN-OK`. The report path, the
# `.stage` path and EVERY path component at or below the repo root are therefore checked, and a
# link is a NAMED refusal rather than something to resolve. Both writes then go through an
# UNPREDICTABLE same-directory temporary file, created and opened in ONE `O_EXCL` step and written
# through the held descriptor, plus an atomic `mv -f`: `mv` replaces the destination NAME instead
# of opening it, and no concurrent reader (`premerge-assert.sh` at the merge point) can observe a
# half-written `result:` line. The temp path was itself a TOCTOU until round 3 (G3) — the full
# reasoning is stated at `prepare_write`, beside the code.
#
# THE DEADLINE IS ADVISORY BY DESIGN
# ----------------------------------
# It changes what `status` REPORTS, never the verdict. A report that arrives late is still
# a report; a stage that is silent inside its deadline is still `NOT-RUN`. Letting the clock
# decide would add a clock to a question already answerable from CONTENT, and would fail a
# slow-but-real review. `status` therefore exits 0 for every state it can measure — reading
# status must not be able to decide anything. ADVISORY IS NOT LICENCE TO ANSWER FROM AN
# UNPERFORMED COMPARISON, though: an `elapsed` or `deadline` this tool cannot COMPARE (see
# `int_is_comparable`) reports `elapsed=unknown` / `past-deadline=unknown`, never the permissive
# `no` (#3751 round 8).
#
# SUBCOMMANDS
#   open  <kind> --issue <N> --agent <type> [--deadline-secs <S>] [--force]
#         <S> is at most MAX_INT_DIGITS (10) decimal digits with no leading zero — ~317 years,
#         comfortably beyond any review timeout, and bounded so this tool can always compare it
#         (#3751 round 8). A wider or zero-padded value is a USAGE refusal, exit 64.
#         Pre-stamp the sentinel BEFORE spawning. Refuses an already-open stage without
#         --force; --force NEVER resets `spawned-at` (a second spawn silently restarting the
#         clock would make the deadline unreadable, and a re-spawn is exactly what a lane
#         does when the first agent idles), and it ALWAYS advances the report GENERATION, so
#         the re-spawned agent gets a fresh path and the idle one holds a stale one (round 5,
#         J1). Prints the absolute path OF THAT GENERATION AND the paste-ready
#         clause for the spawn prompt, so the contract reaches the agent VERBATIM rather
#         than being paraphrased per lane — PASTE THE PRINTED PATH, never a remembered one.
#   status <kind> --issue <N>
#         Elapsed / deadline / state / `reopen-count=`. ADVISORY ONLY — never changes the verdict.
#         The counter is the record's own, so both surfaces report the same number, and it is
#         rendered `<n>+` at the ceiling, meaning AT LEAST (#3751 round 9, N4: the widest value
#         this tool can compare is held rather than incremented past — the increment used to walk
#         off the bound and the NEXT re-open restarted the count at 1, which is a false audit
#         trail; see `reopen_display`). `state=` is a
#         CLOSED set, ONE VALUE PER CAUSE, because the operator action differs per cause:
#           reported                a verdict is recorded (PASS/FINDINGS/AUTHOR-PERFORMED)
#           sentinel-only           the report is still the pre-spawn sentinel
#           report-absent           the report file is GONE
#           report-unreadable       it exists and cannot be READ (fix: chmod / the filesystem)
#           report-empty            it exists and holds nothing recordable
#           report-ungrammatical    its verdict line is absent, ambiguous or unrecognised
#           not-run-self-reported   it RECORDS a NOT-RUN of its own, and names why
#           stage-record-unreadable the RECORD does not name which report is current (fix: the
#                                   record, or a fresh --force open — not the agent, not a chmod)
#           never-opened            no stage was ever opened for this kind
#         Two of these were added in round 4 (H4): `report unreadable` and a self-recorded cause
#         both used to be reported as `report-ungrammatical`, i.e. a WRONG remediation signal and,
#         for the second, a false claim about a file that is perfectly grammatical.
#   verdict <kind> --issue <N>
#         EXACTLY ONE line of the closed grammar above. Exit 0/4/5/6.
#   record-author-performed <kind> --issue <N> --reason <why> --evidence <artifact>
#                           --performed-by author [--force]
#         The sanctioned FALLBACK, never recorded as independent. Requires the WORKING:
#         a substantive reason, a named evidence artifact, and who performed it.
#         Placeholders are refused exactly as `claim.sh --reason` refuses them — by the same
#         function `verdict` classifies a HAND-WRITTEN report with, so the two sides cannot
#         hold the same value to two different strengths.
#         REFUSES to supersede a report that already RECORDS a verdict (`PASS`/`FINDINGS`)
#         without `--force`, and a forced replacement RECORDS the token it replaced
#         (`replaced-verdict:`) plus the GENERATION it came from
#         (`supersedes-report-nonce:`) in the new report and on the RECORD-OK line — a
#         replacement that leaves no trace turns a recorded refusal into a proceed at the
#         merge point, which is the audit-trail failure this whole tool exists to remove. A
#         sentinel-only report is freely replaceable: that is the normal path.
#         AND NOTHING IS EVER WRITTEN OVER (#3751 round 15, U1): the substitute lands in a
#         FRESHLY RESERVED generation and the stage record — written LAST — publishes it, so
#         no write in this script has the report of record as its destination. A verdict a
#         late reviewer lands at any instant of this call is therefore SUPERSEDED, never
#         DESTROYED: it stays on disk in its own generation, named by
#         `supersedes-report-nonce:`. Round 9 (N1) narrowed the overwrite window and declared
#         the remainder; that declaration is WITHDRAWN, because the overwrite is gone.
#         THE DECISION IS STILL GUARDED (#3751 round 9, N1): the observation it decides on is
#         RE-TAKEN immediately before the publication, and any change refuses
#         (`reason=report-changed-mid-write`) — under `--force` too, since `--force`
#         authorizes replacing the verdict the operator READ, never one that arrives while the
#         substitute is being prepared. The stage record is held to the same rule
#         (`reason=stage-record-changed-mid-write`), because this call now rewrites it.
#         AND AN UNREADABLE PRIOR REPORT IS *UNKNOWN*, NOT *ABSENT* (#3751 round 13, S1):
#         the guard branched on the TOKEN, where an unreadable report arrives as `NOT-RUN`,
#         i.e. on the REPLACEABLE side, so a possibly-blocking verdict nobody could read was
#         replaced by the merge-proceeding token with no `--force` and no trace. The
#         permissive set is AFFIRMATIVE — `absent` (nothing recorded to destroy) and
#         `present` (read, so the token decides) — read through the ONE state reader
#         `report_state`; anything else is `reason=prior-verdict-unreadable`, and `--force`
#         does NOT cover it. Recovery: `open <kind> --force`, which supersedes the stage with
#         a fresh report and leaves the unreadable file on disk as history.
#
# EXIT CODES
#   0   success (OPEN-OK, STATUS, RECORD-OK, verdict PASS)
#   2   refused (OPEN-REFUSED, AUTHOR-REFUSED) — a state, not a usage error
#   4   verdict FINDINGS
#   5   verdict NOT-RUN
#   6   verdict AUTHOR-PERFORMED
#   64  usage error
#
# CONSTRAINTS
#   macOS bash 3.2 compatible in LANGUAGE (no associative arrays, no readarray/mapfile).
#   REQUIRES GNU coreutils `mv` — specifically `mv -T` / `--no-target-directory` (#3751 round 7,
#   L2). This is a HOST PRECONDITION, and it is not satisfied by a stock BSD/macOS `mv`. The
#   reason it is required rather than attempted is in `commit_write`: without `-T`, a destination
#   that is or BECOMES a directory (or a symlink to one) receives the temporary file INSIDE it
#   while `mv` exits 0, so the write lands outside the path this script verified and the tool
#   reports success. A host without `-T` gets a NAMED refusal from every write and writes nothing;
#   there is deliberately no fallback, which would restore the defect exactly where it cannot be
#   detected. The fleet is Linux, so the requirement costs nothing here.
#   `set -euo pipefail`, written to the same conventions as claim.sh. (NOT verified
#   shellcheck-clean: shellcheck is not installed on this fleet's boxes and no gate component
#   runs it, so the claim is not made.) All informative output is prefixed `REVIEW-STAGE:`;
#   notes and usage errors go to stderr. `verdict` prints exactly one line to stdout and
#   nothing else.
#   EVERY LINE IS PRINTED WITH `printf` OF A LITERAL FORMAT — NEVER `echo` (#3751 round 14, T2).
#   Under the bash option `xpg_echo`, settable by an INHERITED environment (`BASHOPTS`,
#   `SHELLOPTS`, a `BASH_ENV` file) and never by this script, `echo` performs BACKSLASH ESCAPE
#   PROCESSING on its argument, which makes the argument a FORMAT. Measured: a `\n` in a LEGAL
#   checkout path split the one-line verdict into two, the second a column-zero
#   `REVIEW-STAGE: … RESULT: PASS`, and octal `\075` put REAL `key=` pairs on it, defeating the
#   `=`→`~` neutralisation `field_value` exists for. `\033` injects terminal control, `\c`
#   truncates. Structurally pinned by `scripts/tests/lib/emit-boundary-scan.sh`, which also
#   requires every `printf` FORMAT to be a literal this script authored.
#
# ---END-HELP---
set -euo pipefail

prog="$(basename "$0")"

# --- the output primitive: `printf '%s\n'`, NEVER `echo` (#3751 round 14, T2) ----------------
# `echo` IS NOT A LITERAL PRINTER, AND WHETHER IT IS DEPENDS ON THE ENVIRONMENT IT INHERITED.
# Under bash's `xpg_echo` option — settable in `BASHOPTS`/`SHELLOPTS` before this script is read,
# or by a startup file on a differently-configured box — `echo` performs BACKSLASH ESCAPE
# PROCESSING on its argument. That makes the argument a FORMAT, i.e. a control channel, and every
# value these three functions render is DATA: a report path derived from the checkout, a cause read
# out of a report an agent wrote, a field read out of the stage record.
#
# MEASURED ON THE SHIPPED SCRIPT, from a LEGAL directory name and nothing else — the checkout was
# `…/t2d/lane\nREVIEW-STAGE: c RESULT: PASS elapsed\0759 deadline\0759 agent\075a report\075/x`,
# and `verdict` on a stage with NO report at all printed:
#
#   REVIEW-STAGE: c RESULT: NOT-RUN (no report written) elapsed=0 … report=…/t2d/lane
#   REVIEW-STAGE: c RESULT: PASS elapsed=9 deadline=9 agent=a report=/x/…/c.MFpGyTMmP1.md
#
# TWO breakages in one line, and both are properties this file spent rounds 5 and 7 establishing.
# (1) `verdict` PRINTS EXACTLY ONE LINE — the contract in this script's own header — and `\n`
# turned it into two, the second a column-zero `REVIEW-STAGE: … RESULT: PASS` a consumer reads as a
# verdict. (2) `field_value` maps `=` to `~` so a value can NEVER introduce a `key=` pair; `\075`
# is octal `=`, so the forged line carries REAL `=` fields and that neutralisation is DEFEATED
# ENTIRELY. `\033` injects terminal control, `\c` truncates the line at that point.
#
# `printf '%s\n' "$value"` has no such dependence: the format is a script-authored literal and the
# value is copied verbatim, in every bash and under every shell option. The neutralisation stays
# DISPLAY-ONLY exactly as round 5 requires — nothing here changes what any decision is made on;
# every authorization decision is still taken on the RAW value before any renderer runs.
#
# STRUCTURALLY PINNED, not left to review: `emit-boundary-scan.sh` reds on ANY `echo` at a
# statement-start position in either subject, and on a `printf` whose FORMAT argument is
# data-derived (which would re-open the same channel through `%` and `\`).
die_usage() { printf '%s\n' "$prog: $*" >&2; exit 64; }
note()      { printf '%s\n' "[review-stage] $*" >&2; }
emit()      { printf '%s\n' "REVIEW-STAGE: $*"; }

# THE REFUSAL MARKER OF THE RUNNING SUBCOMMAND (#3751 round 2, S2). `assert_ignored`,
# `assert_no_symlink` and the write helpers are SHARED by `open` and
# `record-author-performed`, and they hard-coded `OPEN-REFUSED` — so a
# record-author-performed refusal was reported under the WRONG subcommand's marker while
# every refusal raised in `cmd_record_author_performed` itself said `AUTHOR-REFUSED`. One
# subcommand emitting two markers makes a grep answer about the wrong thing. Set ONCE per
# subcommand; the default is the historical value, so a future subcommand that forgets to set
# it gets a marker that is merely imprecise rather than empty.
REFUSE_MARKER="OPEN-REFUSED"

# The disclosure a hand-performed substitute MUST carry, verbatim (design.md §4, adopting
# lane-3629's wording). `verdict` REQUIRES it to be present before it will report
# AUTHOR-PERFORMED: the token means "a disclosed substitute with its working recorded", so a
# report claiming the token without the disclosure is not one — it is ungrammatical.
AUTHOR_DISCLOSURE="an author's hand audit is not an independent one; weight it accordingly"

# Default deadline. Advisory (see the header): it is a reporting threshold, never a verdict
# input, so the value only has to be a plausible "this should have finished by now".
DEFAULT_DEADLINE_SECS=1800

# --- the capture boundary ----------------------------------------------------
# A CAPTURE THAT NORMALISES ITS INPUT CANNOT BE THE THING THAT VALIDATES IT (#3751 round 13, S2).
#
# THE FINDING. Every read of an untrusted file in this tool goes through a COMMAND SUBSTITUTION, and
# bash SILENTLY DISCARDS NUL bytes there (5.2 warns on stderr, which every call site here redirects
# to /dev/null, so it is silent in practice). That does not merely LOSE information — it MANUFACTURES
# grammar the file does not contain. Measured: a report whose bytes are `res\0ult: PASS\n` holds NO
# column-zero `result:` line (`grep -c '^result:'` exits 1 on it), and `verdict` reported
# `RESULT: PASS` at exit 0. One file over, the same idiom in `read_field` REDIRECTED a reader: a
# stage record whose `report-nonce:` value was `STALE\0PASS1` — not a valid nonce token, since a NUL
# is not alphanumeric — was read as the valid `STALEPASS1`, so `verdict` reported a STALE report's
# `PASS` for a stage whose own current report held the sentinel. That is round 4's H2 defect (a data
# file redirecting a reader) reached through the capture instead of through `--report`.
#
# THE FIX IS IN THE READ, NOT IN A PROBE, and that choice is the whole design. A separate
# `grep -q`/`wc -c` probe of the same path is a SECOND OBSERVATION, and one direction of its
# disagreement is a FALSE PASS: the capture reads the NUL-bearing version while the probe reads a
# clean one, either order. Round 12's R2 lesson, one layer down. So the ONE read maps NUL to SOH IN
# THE STREAM: nothing is lost (the byte count is preserved), the forged grammar is never created
# (`res<SOH>ult:` matches no record anchor and `one_line` renders SOH as `?`, which no token grammar
# accepts), and the byte's PRESENCE is observable — so a reader with a state channel can NAME it
# rather than silently judging a transformed document.
#
# ONE LITERAL, AND THE BYTE IS DERIVED FROM IT. `tr` needs the four characters `\001`; a detector
# needs the actual byte. Spelling both by hand would be a second place for them to diverge, and a
# divergence means the DETECTOR looks for a byte the MAPPER never writes — a silent false PASS. So
# the `tr` spelling is the single literal and the byte comes from `printf %b`.
#
# A LITERAL SOH IN THE FILE IS REFUSED WITH THE NUL, DELIBERATELY. After the mapping the two are
# indistinguishable without a SECOND read of the file, which is exactly what this design refuses to
# take; and both are control bytes no text record may contain, with the same operator action
# (rewrite the report as text). Naming both in the cause is the honest report — asserting "NUL" of a
# file that held a SOH would be a false rationale, which is worse than none (round 2, B7).
CAPTURE_NUL_TR='\001'
CAPTURE_NUL_BYTE="$(printf '%b' "$CAPTURE_NUL_TR")"

# capture_map_nul <path> — read <path> for a value that is about to enter a SHELL VARIABLE. The ONE
# mapping implementation: every capture of untrusted file content in this script goes through it, so
# no reader can drift from the byte the others expect. Reads by REDIRECTION rather than `cat --`,
# which also removes the `-`-prefixed-filename question the `--` was there for.
capture_map_nul() {
  LC_ALL=C tr '\000' "$CAPTURE_NUL_TR" <"$1"
}

# --- field hygiene -----------------------------------------------------------
# sanitize_field <text> — collapse a free-text value into ONE parseable token. Lifted
# verbatim in behaviour from claim.sh (same reasons, same contract): the stage record and the
# report are parsed as `<key>: <value>` LINES, so a value carrying a newline could inject a
# `result:` line and forge a verdict. Keeps [A-Za-z0-9._:/#-] (note ':' is kept so an
# ISO-8601 timestamp and a path survive, and '=' is NOT, so a value can never introduce a
# `key=` pair into the verdict line), maps every other run to a single '-', trims, caps at
# 120 chars, re-trims after the cut (a cut landing on a separator would re-introduce the
# trailing '-' the trim promised to remove), and never prints an empty token.
# LC_ALL=C on BOTH tr and sed is load-bearing: BSD/macOS `tr` aborts with "Illegal byte
# sequence" on non-ASCII input under a UTF-8 locale, and a `--reason` with an em dash is a
# likely invocation in this repo; under `set -euo pipefail` that would kill the script
# inside a command substitution, printing no verdict line at all.
sanitize_field() {
  local s
  s="$(printf '%s' "${1:-}" | LC_ALL=C tr -c 'A-Za-z0-9._:/#-' '-' | LC_ALL=C sed -e 's/--*/-/g' -e 's/^-//' -e 's/-$//')"
  s="$(LC_ALL=C printf '%.120s' "$s")"
  s="${s%-}"
  [ -n "$s" ] || s="unspecified"
  printf '%s\n' "$s"
}

# one_line <text> — flatten to a single line of PRINTABLE text for a diagnostic that is
# INTERPOLATED into an emitted line. Unlike `sanitize_field` it preserves spaces, punctuation and
# non-ASCII (a cause is prose a human reads); what it guarantees is exactly two properties:
#
#   1. NO LINE BREAK survives — the emitted grammar is one line per record, and a second line
#      would be read as a second (forged) record;
#   2. NO NON-PRINTABLE CONTROL CHARACTER survives — the whole C0 range and DEL, not just the
#      three whitespace ones.
#
# (2) IS ROUND 5's J3, AND THE COMMENT IS THE DEFECT IT FIXES. This function used to map only
# `\n`/`\r`/`\t` (plus deleting NUL) while its comment asserted that "no control character can
# break the one-line contract" — so ESC, BEL, backspace, VT, FF and DEL passed through into
# `verdict`'s line and into `premerge-assert.sh`'s diagnostics, where a report-supplied `NOT-RUN`
# cause could emit terminal escape sequences (clear the screen, reposition the cursor, retitle the
# window, or overwrite the verdict token that was just printed with backspaces). The CLAIM being
# broader than the MECHANISM is itself the defect, independently of what an escape sequence can do:
# a comment that promises more than the code delivers is what stops the next person checking.
#
# THREE CLASSES, SPELLED OUT because the mapping is not uniform: the five whitespace controls
# (`\n` `\r` `\t` `\v` `\f`) become a SPACE and runs are squeezed, since that is what they were
# for; EVERY OTHER C0 byte plus DEL becomes a VISIBLE `?`; NUL is deleted (bash cannot hold one in
# a variable, so it can only arrive from a file read, and there is nothing to render).
#
# ESCAPED VISIBLY (each such byte becomes `?`), NOT DELETED, and the reason is the same audit-trail one
# this issue is built on: a diagnostic that silently drops bytes reads as if the agent wrote
# something it did not, while a run of `?` says "there was something unprintable here". The
# surrounding prose is untouched, so the readable part of the cause is still readable.
#
# DISPLAY-ONLY, which is the whole safety argument (see `field_value` below): every decision — the
# token, the exit code, the paths written — is made on the RAW value before any line is built, so
# this can never turn a non-verdict into a verdict.
#
# Reserved characters of the emitted grammar ('(' / ')') are deliberately NOT stripped — the cause
# is already inside parentheses and a reader takes the LAST ')'.
#
# `LC_ALL=C` on every stage is load-bearing: BSD/macOS `tr` aborts with "Illegal byte sequence" on
# non-ASCII input under a UTF-8 locale, and a cause carrying an em dash is a likely input here;
# under `set -euo pipefail` that would kill the script inside a command substitution and print no
# verdict line at all. Byte-oriented also means a UTF-8 sequence (continuation bytes 0x80-0xBF)
# passes through untouched rather than being mangled — asserted by a control case, because a guard
# that mangles legitimate text is a guard people route around.
one_line() {
  printf '%s' "${1:-}" |
    LC_ALL=C tr -d '\000' |
    LC_ALL=C tr '\n\r\t\013\014' '     ' |
    LC_ALL=C tr '\001-\010\016-\037\177' '?' |
    LC_ALL=C sed -e 's/  */ /g' -e 's/^ //' -e 's/ $//'
}

# field_value <text> — THE ONE EMIT BOUNDARY for a DATA value interpolated into one of this
# tool's `key=value` control lines (#3312's rule; #3751 round 2, S1). Flattens to one line and
# maps the ONE reserved character '=' to '~'.
#
# WHY IT IS ONE FUNCTION AND NOT A RULE PER SITE: the `cause` and the `report=` path are both
# DATA on a line whose other fields a consumer scans. The cause is influenced by a party this
# tool is judging (the report's own text); the PATH was too, through `--report`, and left raw a
# LEGAL filename like `a=b elapsed=999.md` put a SECOND `elapsed=` pair on the line while the
# comment above the cause claimed "ONE emit boundary". Since round 4 the path is DERIVED from a
# strictly-validated kind and issue, so it can no longer carry `=` at all — the boundary is kept
# for it deliberately, because ONE rule applied to EVERY data value on these lines is what stops
# the next added field being the one that forgot. Neutralised rather than refused, because both
# values are diagnostics an operator has to read.
#
# DISPLAY-ONLY, WHICH IS THE WHOLE SAFETY ARGUMENT: every decision (the token, the exit code,
# the paths actually written) is made on the RAW value before any line is built, so this can
# never change a verdict — the same reasoning the roborev wrapper's `roborev_safe_line` states.
field_value() {
  one_line "${1:-}" | LC_ALL=C tr '=' '~'
}

# remainder_value <text> — THE EMIT BOUNDARY FOR THE ONE FIELD THAT IS PARSED AS THE REMAINDER OF
# ITS LINE (#3751 round 16, V2). Everything `field_value` does EXCEPT the '=' map.
#
# THE DEFECT IT FIXES. `report=` went through `field_value`, so a repository root containing a
# LEGAL '=' had its report path published with that character rewritten to '~' — the verdict line
# advertised a path THAT DOES NOT EXIST while the grammar promises the absolute report-of-record
# path. Measured on the shipped script in a checkout named `…/eq=path/lane`: `open` printed the
# real `…/eq=path/…/c.XPRfO9NNsk.md` on its raw line and `verdict` published
# `…/eq~path/…/c.XPRfO9NNsk.md`, which no `open(2)` can resolve. Round 10's nonce check and every
# consumer that OPENS that path were reading a corrupted value, and `verdict` — unlike `open`,
# which prints a raw path line of its own — offers NO separate raw channel to fall back to.
#
# WHY THE EXEMPTION IS SOUND, AND IT IS THE ROUND-11 PROPERTY THAT MAKES IT SO. The '=' map exists
# so a value cannot forge a `key=value` pair that a field-scanning consumer reads instead of the
# measured one. Since round 11 (Q3) `report=` is emitted LAST on the verdict line and read as the
# REMAINDER of that line, so an '=' inside it cannot create an ambiguous field — there is no
# following field for a forged pair to displace, and the consumer is not scanning fields there
# anyway. The anti-forgery reason simply does not apply to this one field, which is why this is an
# exemption rather than a weakening.
#
# THE EXEMPTION IS COUPLED TO THAT PROPERTY STRUCTURALLY, NOT BY THIS COMMENT. `report=` being last
# AND being read as the remainder are pinned by section 44l(d)/(e) of
# scripts/tests/test_premerge_assert.sh (its emitter states DERIVED by running this script), and
# section 29 of scripts/tests/test_review_stage.sh additionally pins that this function has exactly
# ONE definition and exactly ONE call site and that it differs from `field_value` in the '=' map
# ALONE. So appending a field after `report=` — which would silently truncate the value again and
# re-open the forgery route — reds a suite instead of shipping.
#
# CONFINED TO ONE FIELD ON ONE LINE, BY CONSTRUCTION. The `status`, `OPEN-OK`, `already-open`,
# `AUTHOR-REFUSED`, `report-changed-mid-write` and `RECORD-OK` lines keep `field_value` for their
# own `report=`. That is deliberate: no consumer reads any of them as a line remainder, so the
# justification above is unavailable there, and a permission derived from "no consumer exists today"
# is a permission derived from the ABSENCE of a bad signal — the shape this repository refuses
# everywhere else. DECLARED RESIDUAL: on a '='-bearing checkout those lines still DISPLAY a
# '~'-substituted path. It is a diagnostic in every one of those cases; the two channels that
# promise the real path are `open`'s raw line and this one.
#
# EVERYTHING ELSE IS UNCHANGED, and that is the other half of the claim: `one_line` still flattens
# every line break and renders the whole C0 range plus DEL visibly (rounds 5/7/13/14), so this
# trades a corrupted path for nothing. DISPLAY-ONLY, like its sibling: every decision — the token,
# the exit code, the paths actually written — is made on the RAW value before this line is built.
remainder_value() {
  one_line "${1:-}"
}

# placeholder_defect <raw-value> — THE ONE JUDGEMENT, shared by every caller that has to
# decide whether a free-text value RECORDS SOMETHING. It prints `<kind>|<token>` for the first
# defect it finds and NOTHING when the value is usable; it never exits, so a caller that must
# refuse (the writer) and a caller that must CLASSIFY (the verdict reader) can share it.
#
# This is the claim.sh (#2945) refusal, reused rather than reinvented. THREE gates, in this
# order and for these reasons:
#   1. an UNSUBSTITUTED '<…>' is refused on the RAW text, BEFORE sanitization, because
#      sanitization turns `--reason c-audit:<slug>` into `c-audit:-slug` — not a sentinel,
#      so the placeholder gate would ACCEPT it and record an unresolved template as the
#      disclosure. These commands are read by agents that run printed text LITERALLY, which
#      is the whole premise of this change, so a surviving '<…>' is a caller bug.
#   2. it must RECORD something: not the `unspecified` sentinel sanitize_field falls back to
#      (so a literal `--reason unspecified` is refused too), and >=3 recordable characters.
#   3. the PLACEHOLDER VOCABULARY is refused BY NAME, because a help line showing
#      `--reason <why>` is run verbatim by these readers and `<why>` sanitizes to `why` —
#      3 recordable chars, so the length gate passes and the record says `reason=why`,
#      exactly as uninformative as no reason at all. Case-insensitive; this is the
#      placeholder vocabulary of help text and templates, not an attempt at judging prose.
placeholder_defect() {
  local raw="${1:-}" tok
  case "$raw" in
    *'<'*'>'*) printf 'unsubstituted|\n'; return 0 ;;
  esac
  tok="$(sanitize_field "$raw")"
  if [ "$tok" = "unspecified" ] || [ "${#tok}" -lt 3 ]; then
    printf 'unrecordable|%s\n' "$tok"; return 0
  fi
  case "$(printf '%s' "$tok" | LC_ALL=C tr 'A-Z' 'a-z')" in
    why | reason | todo | tbd | tba | xxx | xxxx | placeholder | fixme | none | foo | bar | baz | n/a)
      printf 'placeholder|%s\n' "$tok"; return 0
      ;;
  esac
}

# reject_placeholder <flag> <raw-value> <example> — the USAGE-refusing face of
# placeholder_defect, for a value that arrives as a command-line flag. One judgement, two
# faces: the messages differ per gate because the caller's next move does.
reject_placeholder() {
  local flag="$1" raw="$2" example="$3" defect kind tok
  defect="$(placeholder_defect "$raw")"
  kind="${defect%%|*}"
  tok="${defect#*|}"
  case "$kind" in
    unsubstituted)
      die_usage "$flag '$raw' still carries an UNSUBSTITUTED placeholder (<…>) — substitute it, e.g. $flag $example"
      ;;
    unrecordable)
      die_usage "$flag must carry at least 3 recordable characters ([A-Za-z0-9._:/#-]); '$raw' records as '$tok', which is indistinguishable from saying nothing"
      ;;
    placeholder)
      die_usage "$flag '$raw' records as the PLACEHOLDER '$tok' — as uninformative as saying nothing. Say what it IS, e.g. $flag $example"
      ;;
  esac
  printf '%s\n' "$(sanitize_field "$raw")"
}

# author_working_defect <performed-by> <reason> <evidence> — THE ONE PLACE the AUTHOR-PERFORMED
# WORKING IS JUDGED (#3751 round 1, F3), called by BOTH the writer
# (`record-author-performed`) and the CLASSIFIER that reads a report the writer never produced.
# The same fact must not be checked in two places with two strengths, and it WAS: the
# classifier accepted any NON-EMPTY value, so a hand-written `performed-by: nobody`,
# `reason: x`, `evidence: tbd` reached the token that PROCEEDS at the merge point while the
# writer would have refused all three. `verdict` reads hand-written reports by design — that is
# what a report of record IS — so the classifier is the side that has to be as strong.
#
# Prints `<field>|<kind>|<token>` for the FIRST defect, or NOTHING when the triple records real
# working. Kinds: absent | not-in-set | unsubstituted | unrecordable | placeholder. It never
# exits: the writer maps a defect to a usage error, the classifier to a NOT-RUN cause.
author_working_defect() {
  local pb="${1:-}" reason="${2:-}" evidence="${3:-}" d
  [ -n "$pb" ] || { printf 'performed-by|absent|\n'; return 0; }
  # ONE PERFORMER, AND `peer` IS REFUSED (#3751 round 6, K3). `peer` used to be ACCEPTED here and
  # then reported under the token `AUTHOR-PERFORMED` — so a PEER audit, the more independent of the
  # two, was stated to be the diff AUTHOR's. A verdict that misstates WHO audited is a false
  # statement in the one line a human reads. The answer is SUBTRACTION, not a second token: a peer
  # who can perform the audit can write the report of record and produce a genuine `PASS`, which is
  # the PRIMARY path — `record-author-performed` exists for the case where NO independent audit can
  # be obtained, i.e. the author's own. So `peer` was a false affordance whose only effect was a
  # false verdict, and removing it makes this subcommand's NAME true. A `PEER-PERFORMED` token was
  # deliberately not added: the closed grammar is enumerated in `premerge-assert.sh`, CLAUDE.md,
  # `docs/development/review-stage-reporting.md`, six agent definitions, two skills, the OpenSpec
  # delta and both website pages, and a token nobody needs is a maintenance tax at all of them.
  case "$pb" in
    author) ;;
    # SANITIZED for the RETURNED token only: it is rendered into a verdict line, and the raw
    # value comes from a report written by the very agent being judged. Every DECISION above is
    # made on the raw value, so this is display-only (#3312's rule, same as the cause).
    *) printf 'performed-by|not-in-set|%s\n' "$(sanitize_field "$pb")"; return 0 ;;
  esac
  [ -n "$reason" ] || { printf 'reason|absent|\n'; return 0; }
  d="$(placeholder_defect "$reason")"
  [ -z "$d" ] || { printf 'reason|%s\n' "$d"; return 0; }
  [ -n "$evidence" ] || { printf 'evidence|absent|\n'; return 0; }
  d="$(placeholder_defect "$evidence")"
  [ -z "$d" ] || { printf 'evidence|%s\n' "$d"; return 0; }
}

# author_defect_prose <field|kind|token> — render one defect as the tail of a NOT-RUN cause.
# The operator action differs per field and per kind ("that is not a performer" / "that reason
# says nothing" / "you left a template in it" are three different next moves), which is the
# same reason the six NOT-RUN causes are named separately.
author_defect_prose() {
  local d="${1:-}" field kind tok
  field="${d%%|*}"
  kind="${d#*|}"; kind="${kind%%|*}"
  tok="${d##*|}"
  case "$kind" in
    absent)        printf 'with no %s recorded\n' "$field" ;;
    not-in-set)    printf "with performed-by '%s', which is not 'author' — this tool records the AUTHOR's own audit, and a peer who can audit writes the report of record instead\n" "$tok" ;;
    unsubstituted) printf 'whose %s still carries an UNSUBSTITUTED placeholder\n' "$field" ;;
    unrecordable)  printf "whose %s records as '%s' — fewer than 3 recordable characters\n" "$field" "$tok" ;;
    placeholder)   printf "whose %s is the PLACEHOLDER '%s'\n" "$field" "$tok" ;;
    *)             printf 'with an unusable %s\n' "$field" ;;
  esac
}

# validate_kind / validate_issue — THE WHOLE PATH-INPUT SURFACE OF THIS TOOL (#3751 round 4,
# H2/H3). Since `--report` was removed the report path is DERIVED
# (`<repo-root>/.review-stage/issue-<N>/<kind>.md`), so `<kind>` and `<issue>` are the ONLY
# caller-supplied components of any path this script builds, reads or writes. That is why they
# are validated STRICTLY and at ONE boundary rather than sanitized: a kind is also how a caller
# ASKS for a stage, so silently rewriting it would make `open c/../x` and `open c-x` the same
# stage under two spellings.
#
# `<kind>` is `[A-Za-z0-9][A-Za-z0-9_-]*` — CONSERVATIVE on purpose. It admits every kind this
# pipeline uses (`c`, `rust-review`, `fix`, `coverage`) and refuses `/`, `.` (hence `..` and every
# traversal), a leading dash, every shell metacharacter, and CR/LF. `.` is refused even though a
# lone dot cannot traverse: a filename component needs no dot here, and the narrower the surface
# the less there is to reason about. `<issue>` is decimal DIGITS ONLY, so the directory component
# cannot carry a separator, a newline or a traversal either.
validate_kind() {
  local k="${1:-}"
  case "$k" in
    "" ) die_usage "a <kind> is required (e.g. c, rust-review, coverage)" ;;
    *[!A-Za-z0-9_-]* ) die_usage "invalid <kind> '$k': allowed characters are [A-Za-z0-9_-]" ;;
    [!A-Za-z0-9]* ) die_usage "invalid <kind> '$k': must start with a letter or digit" ;;
  esac
  printf '%s\n' "$k"
}

validate_issue() {
  local n="${1:-}"
  case "$n" in
    "" ) die_usage "--issue <N> is required" ;;
    # ONE pattern, and it is exhaustive: anything that is not entirely digits is refused, which
    # covers a separator, a sign, a space and a newline alike.
    *[!0-9]* ) die_usage "--issue must be a decimal issue number, got '$n'" ;;
  esac
  printf '%s\n' "$n"
}

# THE WIDEST INTEGER THIS TOOL WILL COMPARE OR ADD (#3751 round 8). 10 decimal digits, i.e. at
# most 9999999999. Read as a DURATION that is ~317 years; read as a UNIX EPOCH it is the year
# 2286 — both comfortably beyond any legitimate use of a review timeout whose default is 1800s —
# while leaving nine orders of magnitude of headroom under int64, so an accepted value can ALWAYS
# be compared by `[ -gt ]` and `$(( a - b ))` on two accepted values can never wrap.
# ONE LITERAL, AND THE WIDTH IS DERIVED FROM IT (#3751 round 9, N4). The ceiling VALUE is needed as
# well as the width — a counter at the ceiling is HELD there rather than incremented past it — and
# writing both by hand is two places for one fact to drift, which would make the saturation boundary
# and the acceptance boundary disagree.
MAX_INT_VALUE=9999999999
MAX_INT_DIGITS="${#MAX_INT_VALUE}"

# int_is_comparable <value> — is this a value bash can actually compare and add? 0 = yes.
#
# A VALIDATED-AS-DIGITS VALUE IS NOT A COMPARABLE ONE (#3751 round 8, roborev job 379).
# `validate_secs` accepted an arbitrarily long digit string and `status` then handed it to bash's
# FIXED-WIDTH `[ -gt ]`, which above 9223372036854775807 REFUSES the operand with its own
# `integer expression expected` on STDERR — a raw shell diagnostic OUTSIDE the `REVIEW-STAGE: `
# anchor every line of that block carries — and the enclosing `if` then took its ELSE branch, so
# `past-deadline` reported `no` from a comparison that never happened. Measured on the shipped
# script: `--deadline-secs 9999999999999999999999999` was ACCEPTED and `status` printed
# `review-stage.sh: line 1726: [: 9999999999999999999999999: integer expression expected` beside
# `past-deadline=no`.
#
# `$(( ))` IS WORSE, WHICH IS WHY THIS IS ONE PREDICATE AND NOT ONE PATCH: it does not fail at
# all, it WRAPS SILENTLY. A record carrying `spawned-epoch: 18446744073709551616` produced
# `elapsed=1788315330` — 56 years of elapsed time for a stage opened one second earlier — with
# `past-deadline=yes`, a `PAST DEADLINE` note, and NO diagnostic anywhere; and
# `reopen-count: 99999999999999999999` wrapped to `7766279631452241920` and was WRITTEN BACK into
# the record. So the bound is AFFIRMATIVE and is checked at every boundary where a value from argv
# or from the stage record reaches a fixed-width operation — never a test for the values that
# happen to break.
#
# LEADING ZEROS ARE REFUSED WITH IT, because one value then has TWO READINGS inside one script:
# `$(( 010 ))` is OCTAL (8) while `[ 010 -gt 9 ]` is DECIMAL (true) — both measured on this box.
# A value read two ways is refused rather than normalised. `0` itself is accepted: round 7's L3
# records `deadline=0` (from `--deadline-secs 0`) as a legitimate emitter state, so refusing every
# value that starts with a zero would red on correct input.
#
# It NEVER dies and prints nothing, so it is safe to call from any `if`/`&&` in the parent shell
# (round 2's B6 rule: a `die` inside a command substitution cannot reach the top level).
# reopen_display <value> — how a reopen counter is RENDERED on an operator-facing line (#3751
# round 9, N4). ONE renderer, called by `open` and by `status`, because two spellings of "is this
# counter at its ceiling" is two places for it to drift — and the two surfaces are required to
# report the same thing about the same record.
#
# AT THE CEILING THE VALUE IS SUFFIXED `+`, MEANING AT LEAST. The counter cannot pass ten digits
# (see `int_is_comparable`), so at `MAX_INT_VALUE` it is HELD rather than incremented — and a held
# value that rendered as a bare number would assert an exact count it does not have. Below the
# ceiling, and for a value this tool cannot compare at all, the record's own text is rendered
# unchanged: the marker has to MEAN something, so it may not appear on a value that can still
# increase, nor on one no comparison was performed on.
reopen_display() {
  local v="${1:-}"
  if int_is_comparable "$v" && [ "$v" -ge "$MAX_INT_VALUE" ]; then
    printf '%s+\n' "$v"
  else
    printf '%s\n' "$v"
  fi
}

int_is_comparable() {
  local v="${1:-}"
  case "$v" in
    "" | *[!0-9]* ) return 1 ;;
    0 ) return 0 ;;
    0* ) return 1 ;;
  esac
  [ "${#v}" -le "$MAX_INT_DIGITS" ] || return 1
  return 0
}

validate_secs() {
  local s="${1:-}" flag="${2:---deadline-secs}"
  case "$s" in
    "" | *[!0-9]* ) die_usage "$flag must be a non-negative integer number of seconds, got '$s'" ;;
  esac
  # DIGITS ARE NOT ENOUGH — REFUSED AT THE BOUNDARY, BY NAME (#3751 round 8). Accepting a value
  # this tool cannot compare only moves the failure to `status`, where it surfaced as a raw bash
  # diagnostic outside the anchored block plus a permissive `past-deadline=no`. See
  # `int_is_comparable` for the measurement and for why leading zeros go with it.
  int_is_comparable "$s" || die_usage "$flag must be at most $MAX_INT_DIGITS digits and must not have a leading zero (got '$s'). A deadline is a review timeout in seconds: $MAX_INT_DIGITS digits is ~317 years, comfortably beyond any legitimate use, and a wider value is one this tool cannot compare without leaking a shell diagnostic, while a zero-padded one is read as OCTAL by \$(( )) and as DECIMAL by [ ]"
  printf '%s\n' "$s"
}

# --- paths -------------------------------------------------------------------
# THE ROOT IS RESOLVED ONCE, AT TOP LEVEL, BEFORE ANY PATH IS BUILT FROM IT (#3751 round 2,
# B6). `repo_root` used to `die_usage` itself, and its ONLY caller was `$(repo_root)` inside
# `stage_dir` — a COMMAND SUBSTITUTION — so `exit 64` terminated the SUBSHELL and the script
# carried on with an EMPTY root. Measured outside any repository: the diagnostic printed TWICE
# (once per substitution) and `verdict` then emitted `report=/.review-stage/issue-1/c.md`, a
# FABRICATED path, on the line that is otherwise the authority — while exiting 5, not the 64
# the header documents. A `die` that cannot reach the top level is not a die.
#
# So `require_repo_root` runs in the PARENT shell at the head of every subcommand, sets the
# global, and dies there. `repo_root` is then a pure reader of that global, safe to call from
# any substitution. `--help` never calls it: reading the usage text must not require a
# worktree.
REPO_ROOT=""
require_repo_root() {
  local root=""
  root="$(git rev-parse --show-toplevel 2>/dev/null)" || root=""
  [ -n "$root" ] || die_usage "not inside a git worktree (this tool writes into the lane's worktree on purpose — see the header)"
  REPO_ROOT="$root"
}

repo_root() {
  # NEVER a fallback to $PWD or to empty: an unresolved root would build a `/`-rooted path and
  # publish it as the report of record. `require_repo_root` has already died if it is unset.
  [ -n "$REPO_ROOT" ] || die_usage "internal: the repository root was not resolved before a path was built from it"
  printf '%s\n' "$REPO_ROOT"
}

# `abs_path` IS GONE WITH `--report` (#3751 round 4). It existed to absolutise a
# caller-supplied path against `$PWD`; every path is now built from the repo root by
# `stage_dir`/`stage_file`/`report_path` below, so it is already absolute and does not move with
# the caller's cwd. Nothing absolutises anything any more, which is the point: subtraction cannot
# introduce a false pass.

# THE THREE PATHS ARE DERIVED FROM (repo root, issue, kind) AND FROM NOTHING ELSE (#3751 round
# 4, H2/H3). There is no override: `report_path` is THE report of record's location, computed the
# same way by the writer (`open`) and by every reader (`verdict`, `status`,
# `record-author-performed`), so the two can never form two opinions about which file a stage
# means — and no caller-controlled component enters a path, so there is no newline to split a
# record line on and no repository-containment question to answer.
stage_dir()  { printf '%s/.review-stage/issue-%s\n' "$(repo_root)" "$1"; }
stage_file() { printf '%s/%s.stage\n' "$(stage_dir "$1")" "$2"; }

# AND THE REPORT PATH IS NONCE-BOUND (#3751 round 5 J1, round 6 K2). The third argument is the
# stage's REPORT NONCE — see the header section — and it is INTERNAL: `open` generates it, the
# stage record carries it, and every reader takes it from that record. No caller supplies it.
#
# TWO SHAPES, ONE DERIVATION:
#   a nonce      ->  <dir>/<kind>.<nonce>.md      (every open this version performs)
#   NO nonce     ->  <dir>/<kind>.md              (LEGACY: a record written before the field
#                                                  existed, which is the ONE report that version
#                                                  ever wrote)
# They cannot collide and no `<kind>.<nonce>` is ambiguous with another kind, because a `<kind>`
# may not contain a `.` (round 4's narrowing) and a nonce is alphanumeric. The bare name is READ
# but never WRITTEN by this version: keeping it readable is what stops a pre-nonce record being
# reported as `report absent`, and a guard that reds on correct input is the guard agents learn to
# waive.
#
# THE NONCE IS VALIDATED BY ITS PRODUCER, IN THE PARENT SHELL — never by a helper called
# from a substitution. Every call here is `$(report_path …)`, so a `die_usage` inside this
# function would exit only the SUBSTITUTION (round 2's B6 lesson) and leave the caller with an
# empty path; so `cmd_open` and `load_stage` each test the value with `nonce_is_valid` where they
# compute it, with no subshell in between. The `case` below is a BELT whose only reachable effect
# inside a substitution is the diagnostic plus an empty value — which then fails closed at
# `assert_ignored` (git cannot confirm an empty path) rather than writing anywhere.
report_path() {
  local issue="$1" kind="$2" nonce="${3:-}"
  if [ -z "$nonce" ]; then
    printf '%s/%s.md\n' "$(stage_dir "$issue")" "$kind"
    return 0
  fi
  case "$nonce" in
    *[!A-Za-z0-9]* ) die_usage "internal: report_path needs an alphanumeric report nonce, got '$nonce'" ;;
  esac
  printf '%s/%s.%s.md\n' "$(stage_dir "$issue")" "$kind" "$nonce"
}

# nonce_is_valid <token> — the ONE predicate for a usable report nonce. A PREDICATE, not a
# `die_usage`, because both producers call it in the parent shell and each maps a rejection to its
# own outcome (a named refusal at `open`, a record defect at `load_stage`). Alphanumeric only, so
# the token cannot introduce a path separator, a `.` (which would make `<kind>.<nonce>` ambiguous)
# or anything a line-oriented record could not carry; the length floor is what stops a truncated
# or hand-edited one-character value being accepted as unique.
NONCE_MIN_LEN=6
NONCE_MAX_LEN=64
nonce_is_valid() {
  local tok="${1:-}"
  case "$tok" in
    "" | *[!A-Za-z0-9]* ) return 1 ;;
  esac
  [ "${#tok}" -ge "$NONCE_MIN_LEN" ] || return 1
  [ "${#tok}" -le "$NONCE_MAX_LEN" ] || return 1
  return 0
}

# new_report_nonce <dir> — a FRESH, UNPREDICTABLE report nonce, or NOTHING.
#
# WHY A NONCE AND NOT A COUNTER (#3751 round 6, K2). Round 5 chose the generation by SCANNING for
# an unused `<kind>.<gen>.md`, and a value chosen by looking at what is already on disk is a value
# TWO CONCURRENT CALLERS CAN BOTH CHOOSE: two `open --force` runs read the same record, probe the
# same directory before either has written, pick the same generation and hand the SAME report path
# to two agents — so the superseded agent overwrites the current verdict, including replacing
# FINDINGS with PASS. NOTHING IS SELECTED HERE, so nothing races: two concurrent opens produce two
# different nonces and two different records, the record written LAST is the published one (the
# record is the publication marker — round 4, H1) and the loser's agent writes to a path no reader
# derives. That is the J1 property with no lock, and a lock is the worse trade: it would SERIALISE
# a race a nonce makes impossible, and `flock` is one more mechanism to get right (a stale lock
# file, a box without flock, a holder killed mid-open) in a script whose whole subject is not
# taking the permissive branch when something cannot be measured.
#
# THE RANDOMNESS COMES FROM `mktemp -u`'s X-substitution — the same source the temporary file name
# already comes from (`prepare_write`), so this adds no dependency this script did not already
# have. NO CRYPTOGRAPHIC STRENGTH IS NEEDED OR CLAIMED: the nonce is a UNIQUENESS token, not a
# secret. What it must not be is PREDICTABLE ENOUGH TO COLLIDE with a value another live open
# picks, and 10 alphanumeric characters is far past that.
#
# NO FALLBACK GENERATOR, for the same reason `prepare_write` has none: a predictable fallback
# (a pid, a timestamp, a counter) is exactly the collidable value this replaces, and "cannot tell"
# must not take the permissive branch. A box without a usable `mktemp -u` gets the caller's named
# refusal instead of a weaker token it cannot see. Prints nothing on any shape it does not
# recognise; the CALLER validates with `nonce_is_valid` in the parent shell.
new_report_nonce() {
  local dir="${1:-.}" cand tok
  # `-u` CREATES NOTHING — this is a name generator, not a file. The directory only supplies the
  # template's prefix, which is discarded.
  cand="$(mktemp -u "$dir/.nonce.XXXXXXXXXX" 2>/dev/null || true)"
  [ -n "$cand" ] || return 0
  tok="${cand##*.}"
  case "$tok" in
    "" | *[!A-Za-z0-9]* ) return 0 ;;
  esac
  printf '%s\n' "$tok"
}

# RESERVE_ATTEMPTS — how many nonces one `open` may try before refusing. ONE top-level literal, so
# the loop and the refusal that names it cannot drift apart. BOUNDED for the reason
# `prepare_write`'s loop is bounded: an unbounded retry would spin forever on a directory that
# cannot be written, and "cannot tell" must not become "keep trying".
RESERVE_ATTEMPTS=8
# THE RESERVATION IS UNDONE IF THE OPEN NEVER COMPLETES. An `open` that refuses must leave the
# tree exactly as it found it — an empty file at a report path nothing published is indistinguishable
# from a crashed write, which is the same reason `commit_write` removes its temporary file rather
# than leaving it. Registered THE MOMENT the name is claimed and de-registered the moment real
# content holds it (in `commit_write`, matched by path, so no call site has to remember), which is
# round 9's register-before-create ordering.
#
# DECLARED LIMIT, and it is EXACTLY `WRITE_TMP`'s: the EXIT trap covers a normal exit and every
# `exit 2` refusal path, and bash runs no EXIT trap for a signal left at its default disposition —
# so a SIGKILL, and an unhandled INT/TERM/HUP, leave the reservation behind. That residual costs an
# empty file in a gitignored directory that no stage record names, so it is stated rather than
# closed: adding signal handlers here would change `WRITE_TMP`'s lifetime too, which is not this
# item's subject.
RESERVED_PATH=""
cleanup_reserved_path() {
  [ -z "$RESERVED_PATH" ] || rm -f "$RESERVED_PATH" 2>/dev/null || true
}
# The reservation's outputs. GLOBALS, not a printed value, for the `WRITE_TMP` reason stated above:
# the path verification below refuses by EMITTING and exiting 2, and inside a command substitution
# that exit would end only the SUBSHELL while the refusal text was captured into a variable — a
# refusal nobody sees, and a script that carries on writing.
REPORT_NONCE=""
REPORT_RESERVED=""
# reserve_report_path <issue> <kind> <dir> — CLAIM this open's report path ATOMICALLY, rather than
# merely GENERATING a name and hoping (#3751 round 12, R1). Sets `REPORT_NONCE`/`REPORT_RESERVED`
# and returns 0, or returns non-zero having set neither.
#
# THE FINDING. Round 6 replaced the SCANNED generation with a random nonce and, along with the
# scan, deleted the existence belt. `mktemp -u` invents a NAME and creates NOTHING, so a nonce that
# repeats a report already on disk — a HISTORICAL report of this same stage, deliberately kept as
# the audit trail — sent this open's `mv -f -T` straight over that report and REPUBLISHED its path
# in the stage record. Two harms in one: a recorded verdict is replaced by a sentinel, and the
# superseded agent STILL HOLDING that path can then write the CURRENT verdict — exactly the
# property round 5's generation binding exists to prevent, reached with no concurrency at all.
#
# WHY THIS IS NOT THE ROUND-6 SCAN COMING BACK. Deleting the scan was right; deleting the
# reservation was not, and the difference is the whole point. The old loop SELECTED a name by
# TESTING EXISTENCE (`[ -e <kind>.<gen>.md ]`) and wrote to it LATER: two steps with a window
# between them, so two callers observing the same directory both chose the same value and neither
# observation was still true when the write happened. Here the decision and the claim are ONE
# operation — the create under `set -C` (`O_CREAT|O_EXCL`) IS the choice — so there is no window to
# interleave in, and no name is ever derived from a state that has since changed. Everything round
# 6 gained is preserved: nothing is SELECTED by scanning (a collision yields a FRESH RANDOM nonce,
# never the "next" one, so the value is still not a function of what exists), the token stays
# OPAQUE, readers still take the path from the stage record, the record is still written LAST as
# the publication marker (round 4, H1), and the report is still reset to the sentinel FIRST.
#
# THE RESERVATION IS AN EMPTY FILE THAT NO READER CAN SEE. It is claimed here, replaced by the
# sentinel a few lines later (`commit_write`'s `mv -f -T` renames OVER it), and the only report
# path any reader derives comes from the stage record — which is written after both. So it is
# unreachable BY CONSTRUCTION rather than by timing. A reservation LEAKED by a later refusal is an
# empty file in a gitignored directory that no record names; it costs a few bytes and is left in
# place deliberately, exactly as a superseded report is.
#
# NO FALLBACK TO AN UNRESERVED NAME. Running out of attempts is a NAMED refusal: an unreserved
# name is precisely the value this removes, so "cannot claim one" must not take the permissive
# branch. TWO DISTINCT NON-ZERO STATUSES, because the operator action differs — 1 = this box could
# not GENERATE a token at all (no usable `mktemp`; fix the box), 2 = tokens were generated and none
# could be CLAIMED (the directory is not writable, or — astronomically — a run of collisions).
reserve_report_path() {
  local issue="$1" kind="$2" dir="$3"
  local nonce cand attempt=0 had_noclobber opened generated=0
  REPORT_NONCE=""
  REPORT_RESERVED=""
  while [ "$attempt" -lt "$RESERVE_ATTEMPTS" ]; do
    attempt=$((attempt + 1))
    nonce="$(new_report_nonce "$dir")"
    # THE PREDICATE IS APPLIED IN THE PARENT SHELL, never from inside the substitution (#3751
    # round 2, B6). A token this box cannot produce is not retried into existence, but the loop
    # still completes so the caller's cause is decided by `generated`, not by loop position.
    nonce_is_valid "$nonce" || continue
    generated=1
    cand="$(report_path "$issue" "$kind" "$nonce")"
    # THE PATH IS VERIFIED BEFORE IT IS CLAIMED, on the EXACT name about to be created — the same
    # order (and the same reason) as `prepare_write`: `git check-ignore` answers about a path
    # STRING, so checking the string we then create is not a time-of-check/time-of-use gap. Both
    # asserts refuse by emitting and exiting 2, which is why this function runs in the parent
    # shell. `mkdir -p` after the symlink walk, because a component that is a DANGLING symlink
    # makes it fail with "File exists" — an unnamed exit 1 under `set -e` instead of a refusal.
    assert_no_symlink "$cand" report-of-record
    mkdir -p "$dir"
    assert_ignored "$cand" report-of-record
    # THE CLAIM. `set -C` makes this `O_CREAT|O_EXCL`, so an existing path is a REFUSED create and
    # never a clobber — a historical report of this stage, a peer's live reservation, and a
    # symlink (dangling or not) all take the retry branch instead of being written through. The
    # caller's noclobber setting is preserved: this script does not set it, but a future caller
    # sourcing these helpers must not have it silently cleared.
    had_noclobber=0
    case "$-" in *C*) had_noclobber=1 ;; esac
    opened=0
    set -C
    if : >"$cand" 2>/dev/null; then opened=1; fi
    [ "$had_noclobber" -eq 1 ] || set +C
    if [ "$opened" -eq 1 ]; then
      # REGISTERED BEFORE THE CALLER CAN SEE IT, so no path exists that this process created and
      # does not own for cleanup (#3751 round 9's register-before-create rule, applied here).
      RESERVED_PATH="$cand"
      REPORT_NONCE="$nonce"
      REPORT_RESERVED="$cand"
      return 0
    fi
  done
  [ "$generated" -eq 1 ] || return 1
  return 2
}


# assert_ignored <path> <what> — FAIL-CLOSED gitignore verification (see the header). Asks
# git; refuses on anything that is not an affirmative "yes, ignored". `check-ignore -q` exits
# 0 = ignored, 1 = NOT ignored, 128 = error (e.g. the path is outside the repository), and
# every non-zero answer takes the SAME refusing branch: "cannot tell" is not "fine".
assert_ignored() {
  local path="$1" what="$2" extra="${3:-}" rc=0
  git check-ignore -q -- "$path" || rc=$?
  if [ "$rc" -ne 0 ]; then
    emit "$REFUSE_MARKER reason=path-not-gitignored what=$what path=$(field_value "$path") check-ignore-rc=$rc"
    emit "$REFUSE_MARKER detail=git does not confirm this path is ignored, and this tool writes it MID-RUN — an untracked-but-not-ignored write dirties a running gate of record (tree-integrity FAIL, #2926) and makes premerge-assert refuse on dirty: yes (#3648). Add .review-stage/ to .gitignore (the shipped .gitignore does, as a DIRECTORY — this tool writes nowhere else)."
    # An optional caller-supplied line, printed only on the refusal path: a refused TEMPORARY
    # path is confusing without it, because the caller never named that path.
    # ROUTED THROUGH THE ONE BOUNDARY like every other value on an emitted line (#3751 round 9,
    # N3). It is a script-authored literal at its single call site today, but "the caller passes a
    # literal" is a claim about every FUTURE caller too, and the emit boundary costs nothing here.
    [ -z "$extra" ] || emit "$REFUSE_MARKER detail=$(field_value "$extra")"
    exit 2
  fi
}

# assert_no_symlink <path> <what> — REFUSE rather than FOLLOW (#3751 round 1, F5).
#
# `git check-ignore` answers about a LEXICAL path; a WRITE follows symlinks. So an ignored
# `.review-stage/issue-<N>/c.md` that is a SYMLINK puts the write wherever the link points — a
# TRACKED file, or outside the repository altogether — which falsifies the claim the ignore
# verification above exists to make: that a stage opened mid-run cannot dirty a running gate of
# record (#2926) or make `premerge-assert.sh` refuse on `dirty: yes` (#3648). Measured before
# this check existed: a symlinked report path CLOBBERED a tracked file and `open` reported
# OPEN-OK.
#
# REFUSING BEATS RESOLVING. Resolving the link would need a SECOND ignore verification of the
# resolved path plus a decision about intent, and nothing legitimate creates such a link here —
# so "cannot tell what this is for" takes the refusing branch, as everywhere else in this file.
#
# EVERY COMPONENT AT OR BELOW THE REPO ROOT IS CHECKED, not just the leaf: a symlinked
# `.review-stage/` or `.review-stage/issue-<N>` redirects the write just as effectively as a
# symlinked file. The ROOT ITSELF and anything above it are deliberately NOT checked — a fleet
# checkout legitimately sits under symlinked parents, and refusing there would red correct input,
# which is the guard agents learn to waive. A path not under the root returns without a verdict:
# `assert_ignored` already refuses it, because `check-ignore` cannot confirm it.
#
# It runs BEFORE the `mkdir -p` that prepares the write, because a component that is a DANGLING
# symlink makes `mkdir -p` fail with "File exists" — an unnamed exit 1 under `set -e` instead of
# a named refusal.
assert_no_symlink() {
  local path="$1" what="$2" root rel comp cur parent oldifs
  root="$(repo_root)"
  case "$path" in
    "$root"/*) rel="${path#"$root"/}" ;;
    *) return 0 ;;
  esac
  cur="$root"
  parent="$root"
  oldifs="$IFS"
  # NOGLOB while splitting: `set -- $rel` is an UNQUOTED expansion, so a component containing a
  # glob character would be pathname-expanded and the walk would inspect other files entirely.
  set -f
  IFS='/'
  # shellcheck disable=SC2086
  set -- $rel
  IFS="$oldifs"
  set +f
  for comp in "$@"; do
    [ -n "$comp" ] || continue
    # "CANNOT TELL" IS A REFUSAL: if the parent exists but is not searchable, `-L`/`-e` on the
    # child answer FALSE for a component that may well be a symlink — a two-valued predicate
    # collapsing the unknown onto the permissive answer, which is the shape this repo pins.
    if [ -e "$parent" ] && [ ! -x "$parent" ]; then
      emit "$REFUSE_MARKER reason=path-unverifiable what=$what path=$(field_value "$path") component=$(field_value "$parent")"
      emit "$REFUSE_MARKER detail=this directory is not searchable, so whether the next component is a SYMLINK cannot be determined — and a write that follows a link lands outside the verified-gitignored path (#2926/#3648). Refusing rather than guessing: cannot-tell must not take the permissive branch."
      exit 2
    fi
    parent="$cur"
    cur="$cur/$comp"
    if [ -L "$cur" ]; then
      emit "$REFUSE_MARKER reason=path-is-symlink what=$what path=$(field_value "$path") component=$(field_value "$cur")"
      emit "$REFUSE_MARKER detail=git check-ignore verifies a LEXICAL path but a WRITE follows symlinks, so this write would land wherever the link points — possibly a TRACKED file or a path outside the repository — dirtying a running gate of record (tree-integrity FAIL, #2926) and making premerge-assert refuse on dirty: yes (#3648). Remove the link and let this tool create a regular file."
      exit 2
    fi
    if [ -e "$cur" ] && [ ! -d "$cur" ] && [ "$cur" != "$path" ]; then
      emit "$REFUSE_MARKER reason=path-component-not-a-directory what=$what path=$(field_value "$path") component=$(field_value "$cur")"
      emit "$REFUSE_MARKER detail=an intermediate path component exists and is not a directory, so nothing can be written under it."
      exit 2
    fi
  done
  if [ -e "$cur" ] && [ ! -f "$cur" ]; then
    emit "$REFUSE_MARKER reason=path-not-a-regular-file what=$what path=$(field_value "$path")"
    emit "$REFUSE_MARKER detail=this path exists and is not a regular file (a directory, a fifo, a device). This tool writes a text record; it will not write through anything else."
    exit 2
  fi
}

# WRITE_TMP / prepare_write / commit_write — WRITE VIA A SAME-DIRECTORY TEMPORARY FILE PLUS AN
# ATOMIC `mv -f` (#3751 round 1, F5). Two reasons, and both matter:
#   1. `mv -f` REPLACES the destination NAME rather than opening it, so a link that appeared
#      between the check above and the write is replaced, not followed.
#   2. no reader can observe a HALF-WRITTEN report. The report of record is read CONCURRENTLY (by
#      `premerge-assert.sh` at the merge point, and by `status` from another session), and a
#      truncated `result:` line is a verdict nobody wrote.
#
# THE TEMPORARY FILE IS UNPREDICTABLE AND IS CREATED EXCLUSIVELY (#3751 round 3, G3)
# ---------------------------------------------------------------------------------
# The first version built the temp path as `<dir>/.<basename>.tmp.$$` — DERIVABLE from the report
# path plus a pid — then CHECKED it and REOPENED it by name with shell redirection. That is a
# TOCTOU: a symlink planted at that predictable name inside the window made the write clobber the
# link's target, and the following `mv` could install the link as the report while reporting
# success. It is a NON-INVOKER route and therefore a defect, not an accepted residual: every lane
# on this box runs as ONE user under a shared HOME and a shared `.git`, so the planter is a PEER
# LANE.
#
# THE WINDOW IS REMOVED RATHER THAN NARROWED, because a check placed after a harmful effect can
# only REPORT it — and the harm here is a WRITE, so the control has to be that the write CANNOT
# REACH the wrong file:
#   * the NAME comes from `mktemp -u`, so there is no predictable path to pre-plant AT;
#   * the file is CREATED AND OPENED IN ONE STEP under `set -C`, which makes bash open with
#     `O_CREAT|O_EXCL` — measured on this fleet to refuse an existing file, an existing SYMLINK,
#     and a DANGLING symlink WITHOUT creating its target. So the create cannot follow a link, and
#     a lost race is a refusal, never a clobber;
#   * the body writes to the ALREADY-OPEN DESCRIPTOR (`>&9`), so no path is re-resolved between
#     validation and writing. That is the property; the fd is not decoration.
# There is deliberately NO post-write check that the file we wrote is still the file we created:
# a check whose only job is to notice a clobber afterwards is exactly what this replaces.
#
# THE IGNORE CHECK HAS NO WINDOW OF ITS OWN, and that is why it can stay where it is. It is taken
# BEFORE the create, on the EXACT name about to be created: `git check-ignore` answers about a
# path STRING, so checking the string we then create is not a time-of-check/time-of-use gap. The
# symlink walk of the temp path is GONE, and not because it stopped mattering: the temp lives in
# the destination's OWN directory, whose components `assert_no_symlink "$dest"` has just walked,
# and the leaf cannot be a followed symlink because the create is `O_EXCL`.
#
# WRITE_TMP IS A GLOBAL, NOT A PRINTED VALUE. `assert_ignored` and `assert_no_symlink` refuse by
# EMITTING and exiting 2; inside a command substitution that exit would end only the SUBSHELL
# while the refusal text was captured into a variable — a refusal nobody sees, and a script that
# carries on writing.
WRITE_TMP=""
# The descriptor the write is held open on is 9, spelled LITERALLY at both redirections. A fixed
# number rather than `{fd}` auto-assignment, which bash 3.2 (macOS, a declared constraint of this
# script) does not support — and a literal rather than `exec ${VAR}>` , which bash does not expand
# in the descriptor position and would need an `eval` to reach. `scripts/tests/test_review_stage.sh`
# pins the number in ONE place (`WRITE_FD_PIN`) so its two structural asserts cannot drift apart.
# A LEAKED TEMPORARY IS NO LONGER SELF-LIMITING, so it is cleaned up. With the old predictable
# name a leak was overwritten by the next run in the same process-id; an unpredictable name
# accumulates. Covers a normal exit and every `exit 2` refusal path; a SIGKILL runs no trap and
# this does not claim to cover one.
cleanup_write_tmp() {
  [ -z "$WRITE_TMP" ] || rm -f "$WRITE_TMP" 2>/dev/null || true
}
# BOTH OWNED ARTIFACTS ARE REAPED BY THE ONE TRAP: the temporary file and, since round 12's R1,
# the reserved report name. Two handlers behind one `trap` rather than two `trap`s, because bash
# keeps only the LAST registration for a signal and a second `trap … EXIT` would silently replace
# the first.
trap 'cleanup_write_tmp; cleanup_reserved_path' EXIT
prepare_write() {
  local dest="$1" what="$2"
  local dir base cand had_noclobber attempt=0 opened=0
  assert_no_symlink "$dest" "$what"
  dir="$(dirname "$dest")"
  base="$(basename "$dest")"
  # BOUNDED RETRY. `O_EXCL` fails if the name already exists, which for an `mktemp -u` name means
  # a collision or a peer having planted something there; a few attempts distinguish that from a
  # directory we simply cannot write. An UNBOUNDED loop would hang on an unwritable directory.
  while [ "$attempt" -lt 8 ]; do
    attempt=$((attempt + 1))
    cand="$(mktemp -u "$dir/.$base.tmp.XXXXXXXXXX" 2>/dev/null || true)"
    # NO FALLBACK NAME GENERATOR. A predictable fallback would reinstate exactly the hole this
    # removes, and "cannot tell" must not take the permissive branch — so a box without a usable
    # `mktemp -u` gets the named refusal below rather than a weaker name it cannot see.
    [ -n "$cand" ] || break
    # THE SAME BAR AS THE DESTINATION, and the refusal EXPLAINS itself, because the caller never
    # named this path. Consequence worth knowing: a repository whose `.gitignore` covers the
    # report by EXTENSION rather than by DIRECTORY (`.review-stage/**/*.md` and not
    # `.review-stage/`) is refused, since the temp name matches no such pattern and WOULD dirty a
    # running gate. The SHIPPED `.gitignore` ignores `.review-stage/` as a DIRECTORY, so this
    # never fires here.
    assert_ignored "$cand" "$what-tempfile" \
      "this is the TEMPORARY file the write goes through (an unpredictable same-directory temp, created O_EXCL and written through a held descriptor, plus an atomic mv -f -T, so no path is re-resolved between validation and writing and no reader sees a half-written result: line). It is a real file in the tree for the duration of the write, so it is held to the same bar as the destination. A .gitignore pattern that ignores the report by EXTENSION does not match it: ignore the DIRECTORY instead, as the shipped .gitignore does for .review-stage/."
    # CREATE AND OPEN IN ONE STEP. `set -C` (noclobber) makes this `O_CREAT|O_EXCL`, so it
    # refuses an existing path — INCLUDING a symlink, dangling or not — instead of following it.
    # The caller's noclobber setting is preserved: this script does not set it, but a future
    # caller sourcing these helpers must not have it silently cleared.
    had_noclobber=0
    case "$-" in *C*) had_noclobber=1 ;; esac
    set -C
    if exec 9>"$cand" 2>/dev/null; then opened=1; fi
    [ "$had_noclobber" -eq 1 ] || set +C
    if [ "$opened" -eq 1 ]; then
      WRITE_TMP="$cand"
      return 0
    fi
  done
  emit "$REFUSE_MARKER reason=tempfile-not-created what=$what path=$(field_value "$dest") attempts=$attempt"
  emit "$REFUSE_MARKER detail=an unpredictable temporary file could not be created EXCLUSIVELY beside this path in $attempt attempt(s), so NOTHING was written. Either the directory is not writable, or mktemp is unavailable. There is deliberately no fallback to a predictable name: that is the TOCTOU this write path exists to remove (a peer lane can plant a symlink at a guessable temp name), so refusing is the fail-closed answer."
  exit 2
}
# mv_T_supported — does this host's `mv` have `-T` / `--no-target-directory`? ANSWERED BY
# PERFORMING IT on two throwaway files, never by scanning `--help` text: "the option is listed" and
# "the option works" are different claims and only the second one is a measurement.
#
# CONSULTED ONLY ON A FAILURE PATH, so the success path pays nothing for it. THREE-VALUED — `yes`,
# `no`, `unknown` — because "there is no writable temp area to measure in" is not "the option is
# missing", and the two send an operator to different places. It never decides whether to write:
# see `commit_write` for why the fail-closed behaviour needs no probe at all.
mv_T_supported() {
  local d
  d="$(mktemp -d 2>/dev/null)" || { printf 'unknown\n'; return 0; }
  if [ -z "$d" ] || [ ! -d "$d" ]; then printf 'unknown\n'; return 0; fi
  if ! : >"$d/a" 2>/dev/null || ! : >"$d/b" 2>/dev/null; then
    rm -rf "$d" 2>/dev/null || true
    printf 'unknown\n'; return 0
  fi
  if mv -f -T "$d/a" "$d/b" 2>/dev/null; then printf 'yes\n'; else printf 'no\n'; fi
  rm -rf "$d" 2>/dev/null || true
}

commit_write() {
  local dest="$1" what="$2" tsup
  # THE DESCRIPTOR IS CLOSED BEFORE THE RENAME, so the record is complete on disk and the fd is
  # not carried into the next write (the number is reused for both files a stage writes).
  exec 9>&- 2>/dev/null || true
  # `-T` IS LOAD-BEARING AND IS REQUIRED, NOT ATTEMPTED (#3751 round 7, L2). A plain `mv -f SRC
  # DEST` does not promise to replace the NAME `DEST`: if `DEST` is — or BECOMES — a directory, or a
  # symlink to one, `mv` puts the temporary file INSIDE it and EXITS 0. The write then lands outside
  # the path this script verified while the tool reports success, which is the one outcome this
  # whole write path exists to prevent. `-T` (`--no-target-directory`) makes that case an ERROR:
  # measured on this fleet, `mv -T src dir` fails with `cannot overwrite directory 'dir' with
  # non-directory` and LEAVES THE SOURCE IN PLACE. It closes the LEAF properly for a second reason
  # too: `rename(2)` does not follow a symlink for the destination.
  #
  # WHAT ITS ABSENCE MEANS: `-T` is GNU coreutils; a BSD/macOS `mv` does not have it, and there
  # such an `mv` FAILS THE OPTION PARSE, moves nothing, and this function REFUSES — so the
  # fail-closed behaviour needs no probe and there is deliberately NO FALLBACK to a plain `mv -f`,
  # which would restore the defect on exactly the hosts that cannot detect it. The probe below runs
  # ONLY on that refusal path, and only to NAME the cause: "this host's mv has no -T" and "the
  # rename was refused" are the same exit status and two completely different operator actions.
  # The requirement is recorded in this file's CONSTRAINTS block.
  if ! mv -f -T "$WRITE_TMP" "$dest" 2>/dev/null; then
    rm -f "$WRITE_TMP" 2>/dev/null || true
    WRITE_TMP=""
    tsup="$(mv_T_supported)"
    emit "$REFUSE_MARKER reason=write-failed what=$what path=$(field_value "$dest")"
    case "$tsup" in
      no)
        emit "$REFUSE_MARKER detail=this host's mv has NO -T / --no-target-directory (it is GNU coreutils; a BSD/macOS mv lacks it), so NOTHING was written. That option is REQUIRED, not attempted: without it a destination that is or becomes a directory receives the temporary file INSIDE it while mv exits 0 — a write outside the verified path, reported as success. Install GNU coreutils (or run this on a Linux box, which the fleet is); there is deliberately no fallback."
        ;;
      unknown)
        emit "$REFUSE_MARKER detail=the record was written to a temporary file but could not be moved into place, so NOTHING was recorded — and whether this host's mv supports -T could NOT be measured (no writable temp area), so the cause is not attributed. The temporary file has been removed; an unexplained leftover would be indistinguishable from a crashed write."
        ;;
      *)
        emit "$REFUSE_MARKER detail=the record was written to a temporary file but could not be moved into place, so NOTHING was recorded (this host's mv does support -T, so the rename itself was refused — a destination that is not a plain replaceable name, a full or read-only filesystem, or a cross-device path). The temporary file has been removed; an unexplained leftover would be indistinguishable from a crashed write."
        ;;
    esac
    exit 2
  fi
  # THE RESERVATION IS FULFILLED THE MOMENT THE NAME HOLDS REAL CONTENT (#3751 round 12, R1), so
  # it is de-registered HERE and not at the call site: a cleanup that still fired afterwards would
  # delete the PUBLISHED report. It is done BEFORE the line below because that line is the write
  # boundary section 11f instruments — an interruption AT the boundary is an interruption AFTER the
  # rename, and the reservation is already fulfilled at that instant. Matched by PATH, so no caller
  # has to remember, and a no-op for every write whose destination was never reserved — the stage
  # record. (This parenthetical also named `record-author-performed`'s replacement of an existing
  # report until #3751 round 15, U1: that write now lands in a freshly RESERVED generation rather
  # than over the current report, so the de-registration correctly FIRES for it. Corrected here
  # rather than left standing, because a comment asserting a mechanism decays exactly like code.)
  [ "$dest" != "$RESERVED_PATH" ] || RESERVED_PATH=""
  WRITE_TMP=""
}

# read_field_from <text> <key> — THE ONE IMPLEMENTATION of the `<key>: <value>` field grammar
# (#3751 round 12, R2): the FIRST such line's value, flattened to one line. Empty output means
# "absent or empty", which every caller treats as unmeasured.
#
# IT READS TEXT, NOT A FILE, which is what lets `classify_report` classify every field of the
# report from ONE snapshot instead of re-reading the file per field — the defect R2 names. The
# file-reading `read_field` below is a THIN WRAPPER that delegates here, deliberately rather than
# keeping its own `grep`-on-file copy: a second implementation of this grammar is a second place
# for it to drift, its agreement with the first is only knowable by TESTING it rather than by care,
# and this grammar is what decides whether a merge-proceeding token has its working.
#
# A HERE-STRING, NOT A PIPE. `grep -m1` stops at the first match and closes its input, so a
# `printf … | grep -m1` pipeline can leave `printf` killed by SIGPIPE — a status `pipefail` would
# then surface for a read that actually succeeded. A here-string has no upstream process to signal.
read_field_from() {
  local text="${1:-}" key="$2" line
  line="$(LC_ALL=C grep -m1 -i "^[[:space:]]*${key}:" <<<"$text" 2>/dev/null || true)"
  [ -n "$line" ] || return 0
  line="${line#*:}"
  one_line "$line"
}

# read_field <file> <key> — the same grammar, applied to a file's contents.
#
# ONE READ, THEN THE SHARED GRAMMAR. An unreadable file yields empty text, which is the same
# "absent or empty" every caller already treats as unmeasured — the read-failed-vs-field-absent
# distinction is drawn by `count_field_lines` and `report_bytes`, where it decides something
# (#3751 round 6, K1). Reading the whole file also removes a `grep` artifact this shape had: GNU
# `grep` prints `Binary file … matches` INSTEAD of the line when the file holds a NUL.
#
# AND IT READS THROUGH THE ONE CAPTURE BOUNDARY (#3751 round 13, S2). This capture DISCARDED NUL
# bytes, and `one_line` then stripped them from the value too — which was described here as
# harmless and was not: it FORGED a valid token out of an invalid one. Measured, a record whose
# `report-nonce:` value was `STALE\0PASS1` (not a nonce: a NUL is not alphanumeric) was read as the
# valid `STALEPASS1`, so `verdict` reported a STALE report's `PASS` for a stage whose own current
# report held the sentinel — a data file redirecting a reader, which is round 4's H2 defect through
# a different door. Through `capture_map_nul` the NUL arrives as SOH, `one_line` renders it `?`, and
# the value fails `nonce_is_valid` — a RECORD DEFECT that derives no path at all.
read_field() {
  local file="$1" key="$2" text
  [ -f "$file" ] || return 0
  text="$( { capture_map_nul "$file"; } 2>/dev/null || true )"
  read_field_from "$text" "$key"
}

# count_field_lines <file> <key> — HOW MANY TIMES <key> APPEARS, AS AN AFFIRMATIVE MEASUREMENT.
# Prints the count and returns 0 ONLY when the file was READ FAITHFULLY. THREE statuses, because
# there are three facts and only one of them is permissive:
#
#   0  read, faithful, counted            — the count is printed; the caller may act on it
#   1  the read FAILED                    — permission, I/O, a truncated read (nothing printed)
#   2  read, but NOT REPRESENTABLE        — the file holds a NUL 0x00 or SOH 0x01 byte
#
# Every caller branches on that status with the PERMISSIVE set spelled AFFIRMATIVELY (`0` alone),
# so a status added here later refuses at every call site by construction rather than inheriting
# a `!= 1` test.
#
# IT READS THROUGH THE ONE CAPTURE BOUNDARY, AND THAT WAS THE THIRD TIME A BOUNDARY WAS INTRODUCED
# WITH ONE PATH LEFT BYPASSING IT (#3751 round 14, T1; round 7's emit sites and round 13's record
# reads are the first two). Round 13 routed `read_field` and `report_bytes` through
# `capture_map_nul` and left THIS reader reading the file directly with `grep`. `grep` is a FAITHFUL
# reader — it is the answer that is not: a record whose bytes are `report-<NUL>nonce: CURRENTX1`
# holds no `report-nonce:` line at all, so the count was a truthful `0`, which is exactly the value
# that means "a pre-nonce record, whose single report is the LEGACY bare `<kind>.md`". Measured on
# the shipped script: a stale legacy `c.md` recording `result: PASS` beside a current
# `c.CURRENTX1.md` holding the sentinel reported `RESULT: PASS` at exit 0, from the stale file.
# So the byte does not have to defeat the counter to defeat the reader — it only has to make the
# CURRENT record unparseable while a stale artifact is still on disk, and `0` is not a safe
# reading of a document we could not read as text.
#
# Its own STATUS rather than a folded-in `1`, for round 2's B7 reason: the operator action differs
# (rewrite the record or re-open the stage, never a chmod), and a refusal saying "permission or
# I/O" about a file whose permissions are fine is a FALSE RATIONALE, which is worse than none.
#
# THE `|| true` THAT USED TO BE HERE WAS THE DEFECT (#3751 round 6, K1). `grep` separates the two
# facts this reader depends on with its EXIT STATUS: 1 means "the file was READ and holds no such
# line" (and it prints `0`), >= 2 means "the file could NOT BE READ" (permission, I/O — and it
# prints nothing). Swallowing the status with `|| true` and then mapping a non-numeric value to 0
# collapsed the second onto the first, so an UNREADABLE stage record was indistinguishable from a
# record with no field and took the LEGACY reading — reporting an OLD report's `PASS` as the
# current verdict while WHICH report is current was unknown.
#
# *read failed* and *read fine, field absent* are DIFFERENT FACTS, and only the second one is
# legitimately permissive: every earlier version of this tool wrote exactly one report, at the
# bare `<kind>.md`, so an ABSENT field is an affirmative measurement of THAT shape. A read failure
# measures nothing at all, so it takes the fail-closed branch at every caller — the write side
# REFUSES and the read side reports `stage record unreadable`.
#
# The count itself is required to be numeric as well: a count we cannot read is not a count, and
# `""` would otherwise arrive at an arithmetic `[` test as a syntax error rather than a refusal.
count_field_lines() {
  local file="$1" key="$2" text="" out="" rrc=0 grc=0
  # ONE READ, THROUGH THE ONE MAPPING, WITH THE COMPLETE READ ASSERTED BY *TWO* SIGNALS — the same
  # pair, for the same reasons, as `report_bytes`: the sentinel `E` survives a refactor that folds
  # this assignment into its `local` declaration (where the STATUS would silently become `local`'s),
  # and the STATUS catches what the sentinel cannot, a read that fails after delivering a prefix
  # whose last byte happens to BE an `E`. A TRUNCATED read matters here as much as a failed one: a
  # prefix that stops before the `report-nonce:` line counts `0` and takes the LEGACY reading.
  # `|| rrc=$?`, never `if ! …; then rrc=$?`, which reads 0.
  text="$( { capture_map_nul "$file" && printf 'E'; } 2>/dev/null )" || rrc=$?
  [ "$rrc" -eq 0 ] || return 1
  case "$text" in
    *E) ;;
    *) return 1 ;;
  esac
  text="${text%E}"
  # A byte the capture cannot carry is its OWN status, so the caller can name it (see the header).
  case "$text" in
    *"$CAPTURE_NUL_BYTE"*) return 2 ;;
  esac
  # COUNTED OVER THE SNAPSHOT, BY A HERE-STRING RATHER THAN A PIPE. `grep -m1`'s SIGPIPE hazard
  # (recorded at `read_field_from`) does not apply to `-c`, but a here-string keeps the two readers
  # of this grammar in the same shape — and it also TERMINATES the final line, so a record whose
  # last line has no newline is counted exactly as reading the file directly counted it.
  #
  # `grep`'s remaining statuses still separate two facts: 1 is "read, no such line" (it prints
  # `0`), >= 2 is a failure of grep itself, which measures nothing. The `|| true` that used to
  # swallow this was round 6's K1 defect — it made an unreadable record indistinguishable from a
  # record with no field, so the record took the LEGACY reading and reported an OLD report's `PASS`.
  out="$(LC_ALL=C grep -c -i "^[[:space:]]*${key}:" <<<"$text" 2>/dev/null)" || grc=$?
  case "$grc" in
    0 | 1) ;;
    *) return 1 ;;
  esac
  # The count itself is required to be numeric: a count we cannot read is not a count, and `""`
  # would otherwise arrive at an arithmetic `[` test as a syntax error rather than a refusal.
  case "$out" in
    "" | *[!0-9]* ) return 1 ;;
  esac
  printf '%s\n' "$out"
}

# stage_record_text <file> — THE STAGE RECORD'S OWN BYTES, for a rewrite that must PRESERVE every
# field this version does not know about (#3751 round 15, U1). THREE statuses, the same closed set
# and the same reasons as `count_field_lines` above:
#
#   0  read, faithful          — the text is printed (with trailing newlines stripped by the
#                                caller's capture, which the rewrite re-terminates)
#   1  the read FAILED         — permission, I/O, a truncated read (nothing printed)
#   2  read, NOT REPRESENTABLE — the file holds a NUL 0x00 or SOH 0x01 byte
#
# Callers spell the permissive set AFFIRMATIVELY as `0`, so a status added here later refuses by
# construction rather than inheriting a `!= 1` test.
#
# WHY THE WHOLE TEXT AND NOT THE PARSED FIELDS. `record-author-performed` has to publish a record
# naming a NEW report generation, and everything else about the stage must come out unchanged —
# `head-sha:` above all, because re-stamping it would let a substitute certify a tree the stage was
# never opened at, which is round 5's J1 harm. A rewrite assembled from the fields THIS version
# knows would silently DROP any field it does not (one a newer version writes, or one a human
# added), and a record that loses a field on every recording is an audit trail that erodes. So the
# bytes are carried through verbatim and exactly one line is substituted.
#
# IT READS THROUGH THE ONE CAPTURE BOUNDARY, with the two-signal completeness assertion
# (`report_bytes`' pair, for its reasons): the sentinel `E` survives a refactor folding the
# assignment into its `local` declaration, and the STATUS catches a read that dies on a prefix
# whose last byte happens to BE an `E` — and here a TRUNCATED read is the worst case of all,
# because a prefix that stops early would be WRITTEN BACK as the whole record.
stage_record_text() {
  local file="$1" text="" rrc=0
  text="$( { capture_map_nul "$file" && printf 'E'; } 2>/dev/null )" || rrc=$?
  [ "$rrc" -eq 0 ] || return 1
  case "$text" in
    *E) ;;
    *) return 1 ;;
  esac
  text="${text%E}"
  # A byte the capture cannot carry is its OWN status, so the caller can name it — and it must
  # never be written back, because after the mapping a NUL and a literal SOH are indistinguishable
  # and the rewrite would make the mapped byte durable.
  case "$text" in
    *"$CAPTURE_NUL_BYTE"*) return 2 ;;
  esac
  printf '%s' "$text"
}

# record_text_with_nonce <record-text> <nonce> — the record's own text with its `report-nonce:`
# line REPLACED (or APPENDED, when the record predates the field), every other byte carried
# through VERBATIM (#3751 round 15, U1).
#
# THE GRAMMAR IS THE READERS' GRAMMAR, not a second spelling of it: `^[[:space:]]*report-nonce:`,
# case-insensitive, exactly as `read_field_from` and `count_field_lines` anchor it. Two spellings
# of one grammar are two opinions about which line names the current report, and a divergence there
# is a reader taking a nonce this writer did not write.
#
# SEVERAL SUCH LINES CANNOT REACH HERE — `load_stage` reports that record as a DEFECT and the
# caller refuses on it before anything is written — but the substitution is written to collapse
# them to ONE anyway, because a rewrite that could emit two `report-nonce:` lines would produce a
# record every reader then refuses, i.e. this tool bricking its own stage.
#
# THE CANONICAL SPELLING IS WRITTEN BACK (no leading whitespace, one space after the colon), for
# the one line whose value this function chooses. Nothing else is re-spelled.
record_text_with_nonce() {
  local text="$1" nonce="$2"
  printf '%s\n' "$text" | RTWN_NONCE="$nonce" LC_ALL=C awk '
    BEGIN { n = ENVIRON["RTWN_NONCE"]; done = 0 }
    tolower($0) ~ /^[ \t]*report-nonce:/ {
      if (done == 0) { printf "report-nonce: %s\n", n; done = 1 }
      next
    }
    { print }
    END { if (done == 0) printf "report-nonce: %s\n", n }
  '
}

now_epoch() { date -u +%s; }
now_iso()   { date -u +%Y-%m-%dT%H:%M:%SZ; }

# --- open --------------------------------------------------------------------
cmd_open() {
  require_repo_root
  REFUSE_MARKER="OPEN-REFUSED"
  local kind="" issue="" agent="" deadline="$DEFAULT_DEADLINE_SECS" force=0
  kind="$(validate_kind "${1:-}")"; shift || true
  while [ $# -gt 0 ]; do
    case "$1" in
      --issue) shift; issue="${1:-}" ;;
      --agent) shift; agent="${1:-}" ;;
      --deadline-secs) shift; deadline="${1:-}" ;;
      --force) force=1 ;;
      *) die_usage "open: unknown argument '$1'" ;;
    esac
    shift || true
  done
  issue="$(validate_issue "$issue")"
  [ -n "$agent" ] || die_usage "open: --agent <type> is required (the agent whose silence this stage measures)"
  agent="$(reject_placeholder "open: --agent" "$agent" "spec-auditor")"
  deadline="$(validate_secs "$deadline" --deadline-secs)"

  # DERIVED, NEVER SUPPLIED (#3751 round 4, H2/H3). `--report` used to override this, and it was
  # the only caller-controlled component of any path here: written RAW into the line-oriented
  # stage record, a LEGAL filename containing a newline split across lines and the reader took the
  # PREFIX — which could name a DIFFERENT pre-existing report recording PASS while the sentinel
  # went to the newline-bearing name; and the report's parent directory was created BEFORE
  # containment was verified, so a REFUSED outside-the-repository path still created directories
  # outside the checkout. Both are closed BY CONSTRUCTION rather than by a check, because a check
  # here can only report a write that already happened.
  local sfile rpath dir
  sfile="$(stage_file "$issue" "$kind")"

  # THE STAGE RECORD'S PATH IS CHECKED FIRST, because the report's path is not known until this
  # open's nonce has been generated (#3751 round 5 J1, round 6 K2). The report half is checked
  # immediately after, before anything is written to it.
  #
  # THE SYMLINK WALK RUNS FIRST, BEFORE THE `mkdir -p`: a component that is a dangling symlink
  # makes `mkdir -p` fail with "File exists", i.e. an unnamed exit 1 under `set -e` instead of a
  # named refusal — and a component that is a LIVE symlink would have the directory created
  # somewhere else entirely.
  assert_no_symlink "$sfile" stage-record
  dir="$(dirname "$sfile")"; mkdir -p "$dir"
  assert_ignored "$sfile" stage-record

  local spawned_iso spawned_epoch reopen_count=0 prior_iso="" head_sha=""
  local nonce="" prior_nonce="" nnonce_lines=0
  spawned_iso="$(now_iso)"
  spawned_epoch="$(now_epoch)"

  # THE COMMIT THIS STAGE IS ABOUT (#3751 round 3, G1). Recorded here, at open time, because
  # this is the tree the agent about to be spawned will audit — and `premerge-assert.sh`
  # requires this RECORDED sha to equal the certified one, so a PASS recorded before a further
  # commit cannot certify the newer tree. Resolved with `--verify` so only a real commit is
  # recorded, and lowercased so the comparison at the merge point is a plain string equality.
  #
  # AN UNRESOLVABLE HEAD IS RECORDED AS SUCH, NEVER OMITTED. An unborn HEAD (a fresh `git init`
  # with no commit) is a legitimate state for this tool — `open` must still work, or the guard
  # reds on correct input — but it is NOT a binding, so the field says `unresolved` and the
  # merge point refuses on it by name. An omitted field would be indistinguishable from a
  # record written by a version of this script that predates the field, which is a different
  # operator action.
  head_sha="$(git rev-parse --verify --quiet 'HEAD^{commit}' 2>/dev/null || true)"
  head_sha="$(printf '%s' "$head_sha" | LC_ALL=C tr 'A-Z' 'a-z')"
  case "$head_sha" in
    ????????????????????????????????????????) ;;
    *) head_sha="" ;;
  esac
  case "$head_sha" in
    *[!0-9a-f]*) head_sha="" ;;
  esac
  if [ -z "$head_sha" ]; then
    head_sha=unresolved
    note "this checkout's HEAD does not resolve to a commit, so the stage records head-sha: unresolved — premerge-assert.sh will REFUSE to let this stage certify a merge until it is re-opened in a checkout with a resolvable HEAD"
  fi

  if [ -f "$sfile" ]; then
    prior_iso="$(read_field "$sfile" spawned-at)"
    # THE PRIOR NONCE, READ BEFORE ANY REFUSAL, so the `already-open` refusal can name the
    # report that is CURRENT rather than the one a re-open would create. COUNTED, not first-wins:
    # this field decides WHICH ARTIFACT COUNTS, and "several records is refused, never resolved by
    # order" is the rule the `result:` reader follows for exactly the same reason. An unreadable
    # value refuses below; an ABSENT one is the LEGACY bare report (see the header: that is what
    # every earlier version of this tool wrote).
    #
    # A DEFECTIVE RECORD REFUSES EVEN UNDER `--force`, THOUGH THE NEW PATH DOES NOT DEPEND ON IT.
    # The fresh nonce is generated below and cannot collide whatever this record says — but a
    # record we cannot read is also a record whose `spawned-at` we cannot read, so a forced
    # re-open would silently restart a clock a reader is using and destroy the only evidence of
    # which report the previous agent holds. The remedy is a human reading the record.
    #
    # THE READ IS VERIFIED AFFIRMATIVELY (#3751 round 6, K1). `count_field_lines` returns non-zero
    # when the record could not be READ at all, which is a different fact from "read fine, no such
    # field" — the second is the legacy shape, the first measures nothing.
    #
    # AND THE PERMISSIVE SET IS SPELLED AFFIRMATIVELY (#3751 round 14, T1): only status `0` — read
    # FAITHFULLY — may proceed. Status 2, a record holding a byte the capture cannot carry, gets its
    # OWN refusal because the operator action differs (rewrite the record or re-open the stage, never
    # a chmod), and the `*)` arm takes the fail-closed word so a status added to that helper later
    # cannot inherit a permissive branch here.
    local cfl_rc=0
    nnonce_lines="$(count_field_lines "$sfile" report-nonce)" || cfl_rc=$?
    case "$cfl_rc" in
      0) ;;
      2)
        emit "$REFUSE_MARKER reason=stage-record-unrepresentable kind=$kind issue=$issue record=$(field_value "$sfile")"
        emit "$REFUSE_MARKER detail=this stage's record holds a NUL 0x00 or SOH 0x01 byte, which no text record may contain, so it could not be read as one and NOTHING was written. A shell capture silently DROPS a NUL, so a reader would judge lines this file does not hold — a record whose key is spelt report-<NUL>nonce holds NO report-nonce line, which counts as ZERO and is exactly the value that means a pre-nonce record whose single report is the LEGACY bare name. That is how a STALE report's PASS gets reported as this stage's verdict. Rewrite the record as text, or remove the stage directory and open a fresh stage. This is NOT a permission problem: do not chmod it."
        exit 2
        ;;
      *)
        emit "$REFUSE_MARKER reason=stage-record-unreadable kind=$kind issue=$issue record=$(field_value "$sfile")"
        emit "$REFUSE_MARKER detail=this stage's record EXISTS and could not be READ, so which report of this stage is current could not be measured and NOTHING was written. That is not the same as a record with no report-nonce (which reads as the original single report): an unmeasured record may not take the permissive reading, because it is also a record whose spawned-at cannot be read, so a forced re-open would silently restart a clock a reader is using. Fix the record's permissions, or remove the stage directory and open a fresh stage."
        exit 2
        ;;
    esac
    prior_nonce=""
    if [ "$nnonce_lines" -eq 1 ]; then
      prior_nonce="$(read_field "$sfile" report-nonce)"
    fi
    if [ "$nnonce_lines" -gt 1 ] || { [ "$nnonce_lines" -eq 1 ] && ! nonce_is_valid "$prior_nonce"; }; then
      # FAIL-CLOSED, AND IT REFUSES rather than reading the legacy bare name: that name may be a
      # report an EARLIER agent still holds, and reporting it as this stage's current report is
      # exactly the false certification the nonce closes. The remedy is a human reading the
      # record, so the refusal says so.
      emit "$REFUSE_MARKER reason=report-nonce-unreadable kind=$kind issue=$issue record=$(field_value "$sfile") lines=$nnonce_lines value=$(field_value "${prior_nonce:-<none>}")"
      emit "$REFUSE_MARKER detail=the stage record's report-nonce must be exactly ONE line carrying an alphanumeric token; it names which report of this stage is current, so a guess could name a report an earlier agent still holds. Read the record and repair it, or remove the stage directory and open a fresh stage."
      exit 2
    fi
    rpath="$(report_path "$issue" "$kind" "$prior_nonce")"
    if [ "$force" -ne 1 ]; then
      emit "$REFUSE_MARKER reason=already-open kind=$kind issue=$issue spawned-at=$(field_value "${prior_iso:-unknown}") report=$(field_value "$rpath")"
      emit "$REFUSE_MARKER detail=a stage is already open for this kind; re-opening would restart a clock a reader is using. Pass --force to re-stamp the report (the original spawned-at is PRESERVED either way), or read it with: $prog verdict $kind --issue $issue"
      exit 2
    fi
    # --force RE-STAMPS THE REPORT AND KEEPS THE CLOCK. A re-spawn is exactly what a lane
    # does when the first agent idles, and the elapsed time since the FIRST spawn is the
    # number that says "this stage has produced nothing for 70 minutes". Resetting it would
    # hide the very fact the stage exists to report.
    if [ -n "$prior_iso" ]; then
      spawned_iso="$prior_iso"
      local prior_epoch
      prior_epoch="$(read_field "$sfile" spawned-epoch)"
      # COMPARABLE, NOT MERELY NUMERIC (#3751 round 8). This value is copied FORWARD into the
      # fresh record, so an out-of-range or zero-padded one used to be re-written and outlive the
      # edit that introduced it — every later `status` then subtracted it in `$(( ))`, which
      # wraps silently. An unusable prior clock takes the same branch an absent one takes: the
      # clock restarts, and it SAYS so.
      if int_is_comparable "$prior_epoch"; then
        spawned_epoch="$prior_epoch"
      else
        note "the existing stage record has no usable spawned-epoch (absent, non-numeric, zero-padded or too wide for this tool to subtract without wrapping); the clock restarts from now"
      fi
    fi
    local prior_count
    prior_count="$(read_field "$sfile" reopen-count)"
    # SAME PREDICATE, SAME REASON (#3751 round 8): this one goes through `$(( prior_count + 1 ))`,
    # and a record carrying `reopen-count: 99999999999999999999` wrapped to
    # `7766279631452241920`, which was then WRITTEN BACK — a fabricated number made durable, on a
    # field that exists to be read in an audit trail. `017` was read as OCTAL and became 16.
    # An unusable counter falls back to the value an absent one gets; it is never a reason to
    # refuse a spawn.
    # AND THE CEILING IS EXPLICIT, BECAUSE THE INCREMENT WALKS OFF IT (#3751 round 9, N4). The
    # MAXIMUM ACCEPTED value is `MAX_INT_VALUE`; `$(( prior + 1 ))` produced an eleven-digit value
    # which `int_is_comparable` then REJECTED on the next re-open, so the counter silently RESTARTED
    # AT 1 — and a counter that restarts is a false audit trail, which is this issue's own subject.
    # SATURATION, NOT REFUSAL: round 8's ruling for this field is that an unusable counter takes the
    # value an absent one gets and is "never a reason to refuse a spawn", so refusing a re-open
    # because a COSMETIC audit number is at its ceiling would block real work over a number — the
    # guard agents learn to waive. Held, the value means AT LEAST this many, it can never decrease,
    # and both surfaces render it `+` (see `reopen_display`) so a reader is told which it is.
    if ! int_is_comparable "$prior_count"; then
      reopen_count=1
    elif [ "$prior_count" -ge "$MAX_INT_VALUE" ]; then
      reopen_count="$MAX_INT_VALUE"
      note "the reopen counter is AT ITS CEILING ($MAX_INT_VALUE, the widest value this tool can compare) and is HELD there rather than restarted: it now means AT LEAST that many re-opens, and is rendered with a trailing + on both the OPEN-OK and STATUS lines"
    else
      reopen_count=$((prior_count + 1))
    fi
  fi

  # A FRESH NONCE FOR EVERY OPEN, AND THAT IS THE WHOLE FIX (#3751 round 5 J1, round 6 K2).
  #
  # J1's half: `--force` used to reset the report AT THE SAME PATH, so the PREVIOUS, idle agent
  # could wake up afterwards and write its old-tree verdict into that path — where it was paired
  # with the newly stamped `head-sha:`, and a commit nobody audited passed the merge point. #3751
  # exists BECAUSE delegated agents return late, so that is the expected behaviour of this
  # population, not an exotic race. A fresh path makes the resumed agent STRUCTURALLY unable to
  # write into the current report, which a check could not deliver (the harm is a write, and a
  # check after it could only report it).
  #
  # K2's half: the fresh path must not be CHOSEN. Round 5 chose it by scanning for an unused
  # `<kind>.<gen>.md` — a 4096-attempt walk with its own exhaustion refusal — and a value chosen
  # by looking at what is on disk is a value TWO CONCURRENT CALLERS CAN BOTH CHOOSE: two
  # `open --force` runs read the same record, probe the same directory before either has written,
  # pick the same generation, and hand ONE report path to TWO agents, so the superseded agent
  # overwrites the current verdict — FINDINGS replaced by PASS. The scan, its bound and its
  # exhaustion refusal are DELETED, not guarded: with nothing selected there is nothing to
  # exhaust and nothing to race, and the record written LAST is the published one while the
  # loser's agent writes to a path no reader derives. Subtraction cannot introduce a false PASS.
  #
  # THE SCAN'S EXISTENCE BELT IS DELETED WITH IT, but the RESERVATION IS NOT (#3751 round 12, R1).
  # Round 6 removed both, and the second removal was wrong: `mktemp -u` invents a name and creates
  # nothing, so a nonce repeating a report already on disk let this open write over that report and
  # REPUBLISH its path — handing the superseded agent that still holds it the ability to write the
  # CURRENT verdict. `reserve_report_path` claims the name ATOMICALLY instead (one `O_EXCL` create
  # per attempt, a FRESH random nonce on collision), which is not the scan returning: the scan
  # chose a name in one step and wrote it in another, and this decides and claims in the same
  # operation. The counter, the walk and its exhaustion refusal stay deleted; see that function.
  #
  # `reopen-count` REMAINS, and it is where the human-readable audit number lives. It answers a
  # DIFFERENT question from the nonce (how many times this stage was spawned, versus which report
  # is current), and it is what an operator reads beside `reopened-at:` to correlate a surviving
  # report with a re-spawn; the nonce only has to be UNIQUE.
  #
  # THE REPORT HALF OF THE PATH VERIFICATION happens INSIDE the reservation, on each candidate,
  # because the name is not known until it has been claimed. Its PARENT is the SAME directory as
  # the stage record's, created above, and it is `<repo-root>/.review-stage/issue-<N>` BY
  # DERIVATION — so the `mkdir` in there cannot create anything outside the checkout whatever the
  # caller passed (#3751 round 4, H3: it once could, because the caller supplied the path and the
  # containment check came AFTER it).
  #
  # THE STATUS IS CAPTURED WITH `|| rrc=$?`, NEVER `if ! …; then rrc=$?`, which reads 0 — the
  # negation has already consumed the status by then.
  local rrc=0
  reserve_report_path "$issue" "$kind" "$dir" || rrc=$?
  if [ "$rrc" -ne 0 ]; then
    # NO FALLBACK, for the reason `prepare_write` has none: every predictable substitute (a pid, a
    # timestamp, a counter) is exactly the collidable value this replaces, so a box that cannot
    # generate an unpredictable token is REFUSED rather than given a weaker one it cannot see.
    # TWO CAUSES, NAMED SEPARATELY, because the operator action differs: a token that could not be
    # GENERATED means this box has no usable `mktemp`; tokens that could not be CLAIMED mean the
    # directory cannot be written.
    if [ "$rrc" -eq 1 ]; then
      emit "$REFUSE_MARKER reason=report-nonce-not-generated kind=$kind issue=$issue value=$(field_value "${REPORT_NONCE:-<none>}")"
      emit "$REFUSE_MARKER detail=an unpredictable report nonce could not be generated, so NO report path was derived and NOTHING was written. The nonce comes from mktemp -u's name substitution, so this box has no usable mktemp. There is deliberately no fallback to a predictable token (a pid, a timestamp, a counter): a token two concurrent opens could both choose is the collision this nonce exists to remove, so refusing is the fail-closed answer."
    else
      emit "$REFUSE_MARKER reason=report-nonce-not-reserved kind=$kind issue=$issue attempts=$RESERVE_ATTEMPTS"
      emit "$REFUSE_MARKER detail=a report path could not be CLAIMED in $RESERVE_ATTEMPTS attempt(s), so NOTHING was written and no stage record was published. Each attempt generates a fresh nonce and creates that path O_EXCL, so a name already on disk is retried rather than written through — which is what stops this open replacing a HISTORICAL report and republishing its path to an agent that still holds it. Either this stage directory is not writable, or something is occupying the names generated. There is deliberately no fallback to an unreserved name: that is the value this reservation removes."
    fi
    exit 2
  fi
  nonce="$REPORT_NONCE"
  rpath="$REPORT_RESERVED"

  # THE WRITE ORDER IS LOAD-BEARING: THE REPORT IS RESET FIRST AND THE STAGE RECORD IS WRITTEN
  # LAST, SO THE RECORD IS THE PUBLICATION MARKER (#3751 round 4, H1).
  #
  # The two writes cannot be atomic together, so SOME partial state is reachable — by a failed
  # second write, or by a kill between them. The only question is WHICH partial state, and one of
  # the two orders is a false certification. `premerge-assert.sh` proceeds when the record's
  # `head-sha:` equals the certified sha AND the report records a verdict, so writing the RECORD
  # first paired the NEW commit with the PREVIOUS report — a `result: PASS` from an audit of an
  # older tree, certifying the newer one. Measured: killed between the two writes, `verdict`
  # reported `RESULT: PASS` exit 0 for a tree nobody had audited.
  #
  # Reversed, every partial state is a NON-VERDICT, and each is already a named refusal:
  #   report written, record NOT yet   -> `stage never opened` (a first open) or the OLD record,
  #                                       still naming the commit the audit was really made at,
  #                                       beside a SENTINEL report -> `no report written`
  #   both written                     -> the fresh sentinel, which is the normal open state
  # A CHECK COULD NOT DELIVER THIS. The harm is a WRITE, so the control has to be that the
  # harmful pairing is never REACHED — a check placed after it could only report it. Section 11f
  # of `scripts/tests/test_review_stage.sh` observes the on-disk state at BOTH write boundaries
  # and pins that the forbidden pair (new head-sha + stale verdict) exists at neither.

  # SUPERSEDED REPORTS ARE LEFT ON DISK, DELIBERATELY. They are HISTORY: nothing reads them (the
  # record names exactly ONE nonce, and that is the only path any reader derives), and they are
  # what an operator opens to see what the previous agent actually concluded. Deleting them
  # silently would destroy the audit trail this whole issue exists to create. Since round 6 (K2)
  # nothing DEPENDS on their existence — the nonce is generated, not chosen from what is absent —
  # so removing one by hand costs the trail and nothing else. They are under `.review-stage/`, which is
  # gitignored, so they cost nothing but a few kilobytes.
  #
  # THE SENTINEL. `result:` is the FIRST recordable line on purpose: it is what `verdict`
  # reads, and a reader opening the file sees the non-verdict before anything else.
  prepare_write "$rpath" report-of-record
  {
    printf '# review stage: %s — issue #%s\n' "$kind" "$issue"
    printf '\n'
    printf 'result: NOT-RUN (no report written)\n'
    printf '\n'
    printf 'stage: %s\n' "$kind"
    printf 'issue: %s\n' "$issue"
    printf 'agent: %s\n' "$agent"
    printf 'spawned-at: %s\n' "$spawned_iso"
    printf 'deadline-secs: %s\n' "$deadline"
    printf 'report-of-record: %s\n' "$rpath"
    # WHICH REPORT THIS FILE IS (#3751 round 5 J1, round 6 K2). Human-facing: an agent (or an
    # operator) holding a SUPERSEDED file can see that it is not the current one. It is a NOTE,
    # not a location — the reader takes the current nonce from the STAGE RECORD, never from a
    # report, because a report is author-controlled text (round 4, H2).
    printf 'report-nonce: %s\n' "$nonce"
    printf '\n'
    printf '## How to complete this stage\n'
    printf '\n'
    printf 'THIS FILE is your report of record, not your returned message. REPLACE the\n'
    printf '`result:` line above -- AT COLUMN ZERO, the only place this tool reads it, and there\n'
    printf 'must be EXACTLY ONE such line -- with EXACTLY ONE of the two values in the gutter\n'
    printf 'below (write the value, not the leading "| "):\n'
    printf '\n'
    printf '    | result: PASS        # you reviewed the subject and found no blocking finding\n'
    printf '    | result: FINDINGS    # you reviewed the subject and found >=1 blocking finding\n'
    printf '\n'
    printf 'then write your findings below. The token is matched by STRING EQUALITY on its\n'
    printf 'first word against a closed set, so an invented value (e.g. PASS-BUT-UNMEASURED)\n'
    printf 'is read as NOT-RUN, never as a pass.\n'
    printf '\n'
    printf 'REPLACE it -- do NOT append a second verdict below this one. SEVERAL column-zero\n'
    printf '`result:` lines is read as NOT-RUN (AMBIGUOUS), in either order: resolving two\n'
    printf 'records by which came first is not a rule, so neither value is reported.\n'
    printf '\n'
    printf 'THE GUTTER IS DELIBERATE, and it is defence in depth: this file is AUTHOR-CONTROLLED\n'
    printf 'text that has to SHOW you the verdict spelling, so an example rendered as a valid\n'
    printf '`result:` line would be an escape hatch -- an artifact that DESCRIBES the record\n'
    printf 'becoming the record (#3312). The parser is anchored at column zero, and these two\n'
    printf 'lines do not begin with the token either, so neither protection alone is load-bearing.\n'
    printf '\n'
    printf 'If this line still says NOT-RUN when you finish, this stage is recorded as\n'
    printf 'NOT-RUN and cannot reach a merge: an absent review is not a clean one (#3751).\n'
    printf '\n'
    printf '## Findings\n'
    printf '\n'
    printf '(nothing written yet)\n'
  } >&9
  commit_write "$rpath" report-of-record

  # THE STAGE RECORD, WRITTEN LAST: its EXISTENCE is what publishes the stage (see the order
  # note above), so it must not appear until the report beside it is the sentinel.
  prepare_write "$sfile" stage-record
  {
    printf 'kind: %s\n' "$kind"
    printf 'issue: %s\n' "$issue"
    printf 'agent: %s\n' "$agent"
    printf 'deadline-secs: %s\n' "$deadline"
    printf 'spawned-at: %s\n' "$spawned_iso"
    printf 'spawned-epoch: %s\n' "$spawned_epoch"
    # NO `report:` FIELD (#3751 round 4, H2). It used to be written here and READ BACK as the
    # report's LOCATION, which made a line of this record a control channel for the path — the
    # split-value mis-selection above. The path is DERIVED identically by every reader, so a
    # second source for it would only be a second thing to disagree: remove the source rather
    # than reconcile it. The human-facing copy of the path stays in the sentinel report itself
    # (`report-of-record:`), which is the file an operator opens.
    # RE-STAMPED ON EVERY OPEN, INCLUDING --force — deliberately unlike `spawned-at` above. A
    # forced re-open re-writes the sentinel, so the re-spawned agent audits the tree that is
    # there NOW; carrying an older sha forward would bind the verdict to a tree nobody read.
    printf 'head-sha: %s\n' "$head_sha"
    # THE REPORT NONCE — the field that decides WHICH FILE HOLDS THIS STAGE'S VERDICT (#3751
    # round 5 J1, round 6 K2). It is written in the SAME atomic record as `head-sha:`, so the pair
    # (the tree audited, the artifact that audits it) is published together or not at all; that is
    # what makes a superseded agent's write into its own path unreadable rather than a false
    # certification. Readers take the path from THIS token and nothing else, and it is an OPAQUE
    # TOKEN rather than a path on purpose: round 4 (H2) removed the `report:` path field because a
    # data file naming a location let a reader be redirected to another file. An alphanumeric
    # token cannot redirect — it is validated, and the directory and the name shape around it are
    # DERIVED.
    printf 'report-nonce: %s\n' "$nonce"
    printf 'reopen-count: %s\n' "$reopen_count"
    [ "$reopen_count" -eq 0 ] || printf 'reopened-at: %s\n' "$(now_iso)"
  } >&9
  commit_write "$sfile" stage-record

  local reopen_disp
  reopen_disp="$(reopen_display "$reopen_count")"
  emit "OPEN-OK kind=$kind issue=$issue agent=$agent deadline-secs=$deadline spawned-at=$spawned_iso head-sha=$head_sha report-nonce=$nonce reopen-count=$(field_value "$reopen_disp") report=$(field_value "$rpath")"
  # THE RAW PATH, ON A LINE OF ITS OWN — deliberately NOT through `field_value`. A caller
  # consumes this line to open the file, so a neutralised '=' would hand back a path that does
  # not exist. Safe for the reason the fields are not: this is a WHOLE LINE with no `key=value`
  # pairs, so there is no control token for a payload to pose as (the same reason the
  # paste-ready clause below quotes the path verbatim).
  printf '%s\n' "$rpath"
  # THE PASTE-READY CLAUSE. Printed so the contract reaches the agent VERBATIM instead of
  # being paraphrased per lane — the paraphrase is what varied across the NINE measured
  # sessions (the census is docs/development/review-stage-reporting.md §2: nine spawns,
  # five lanes, four agent types).
  cat <<CLAUSE

--- paste this into the spawn prompt (verbatim) ---
REPORT OF RECORD (mandatory): write your report to
  $rpath
That FILE is your report of record, not your returned message. Write it INCREMENTALLY as
you go, not at the end. When you finish, REPLACE its \`result:\` line — the one at COLUMN
ZERO, which is the only place this is read; an indented or quoted copy is data, and there must be
EXACTLY ONE such line, so replace it rather than appending a second verdict below it (several is
read as NOT-RUN, in either order) — with exactly
one of \`result: PASS\` (no blocking finding) or \`result: FINDINGS\` (>=1 blocking finding),
and put your findings below it. If that line still reads \`result: NOT-RUN\` when you stop, this
stage is recorded as NOT-RUN and BLOCKS the merge — an absent review is not a clean one, and
no returned message, idle notice or verbal summary substitutes for the file.
WRITE TO THE PATH IN THIS CLAUSE, not one you were given earlier or remember from another
stage: that filename carries a per-open NONCE, and a re-opened stage reads only the report its
record names — an earlier one is history that nothing consults.
--- end clause ---
CLAUSE
}

# --- verdict machinery -------------------------------------------------------
# report_bytes <path> — ONE OBSERVATION OF THE REPORT OF RECORD, in a form two reads can be
# compared for equality (#3751 round 9, N1). Prints a STATE marker plus the file's bytes; never
# dies, prints nothing else, so it is safe to call from any command substitution (round 2's B6
# rule).
#
# WHY BYTES AND NOT THE CLASSIFIED TOKEN. The token is the thing `record-author-performed`
# DECIDES on, but it is not a sufficient IDENTITY: with `--force`, a concurrent replacement of one
# `FINDINGS` by a DIFFERENT `FINDINGS` leaves the token equal while the report the operator
# actually read is gone. Content equality catches that; the token cannot. It is also strictly
# cheaper to be right about. (Until round 12's R2 `classify_report` read its subject EIGHT times,
# so its token was not an identity of any single instant at all; it now takes ONE observation, and
# `record-author-performed` passes THIS one into it, so the bytes and the token there describe the
# same instant. The argument for bytes rather than the token is unchanged: a `FINDINGS` replaced by
# a different `FINDINGS` leaves the token equal.)
#
# THE STATE MARKER EXISTS SO THAT "ABSENT" AND "EMPTY" ARE DIFFERENT OBSERVATIONS. Both are the
# empty string once read, and they are not the same fact.
#
# THE COMPLETE READ IS ASSERTED AFFIRMATIVELY, not inferred from `cat` having exited 0 inside a
# substitution whose status is easy to lose: the sentinel `E` is printed by a SECOND command
# joined with `&&`, so a truncated or failed read cannot produce a value ending in it. A positive
# verdict requires an affirmative measurement, and "the bytes are unchanged" is a positive verdict.
#
# THE NUL LIMIT THIS COMMENT USED TO DECLARE IS CLOSED, AND IT WAS NOT MERELY A LIMIT (#3751 round
# 13, S2). It read "a change consisting only of NUL bytes is not represented here", which understated
# it in the direction that matters: the capture did not just fail to REPRESENT a NUL, it REMOVED one
# and thereby MANUFACTURED a `result:` record the file did not contain, reaching `RESULT: PASS` at
# exit 0. The read now goes through `capture_map_nul` and a NUL-bearing report is the named
# `state=unrepresentable`.
#
# DECLARED LIMIT THAT REMAINS: the OUTER capture at each call site (`obs="$(report_bytes …)"`)
# strips TRAILING NEWLINES, so `X`, `X\n` and `X\n\n\n` are one observation. Enumerated and left,
# because it cannot change a verdict — every grammar here is per-LINE and column-zero anchored, so
# trailing newlines create no `result:` line, no field and no disclosure, and a file of only newlines
# is `report empty` exactly as an empty one is. It does mean a change consisting ONLY of trailing
# newlines is invisible to the equality guard below; such a report is the same document for every
# question this tool asks of it.
report_bytes() {
  local p="$1" body rc=0
  if [ ! -f "$p" ]; then printf 'state=no-such-file\n'; return 0; fi
  # Measured BY ATTEMPTING THE READ rather than with `[ -r ]`, which answers TRUE for root and
  # cannot see an I/O error. Since round 12's R2 this is the ONLY place the report's readability is
  # measured: `classify_report` had its own redirection probe and now takes its whole observation
  # from here, so there is one answer to "could this file be read" rather than two.
  #
  # THROUGH THE ONE CAPTURE BOUNDARY (#3751 round 13, S2): a raw capture DROPPED NUL bytes and so
  # manufactured records the file did not contain — see `capture_map_nul`.
  #
  # THE COMPLETE READ IS ASSERTED BY *TWO* SIGNALS, because either alone is defeatable. The
  # sentinel `E` survives a refactor that folds this assignment into its `local` declaration, where
  # the STATUS would silently become `local`'s. The STATUS catches the case the sentinel cannot: a
  # read that fails after delivering a prefix whose last byte happens to BE an `E` is textually
  # indistinguishable from a complete one, and `${body%E}` would then eat a real byte — a truncated
  # prefix that drops a SECOND `result:` line turns an AMBIGUOUS refusal into a PASS. `|| rc=$?`,
  # never `if ! …; then rc=$?`, which reads 0.
  body="$( { capture_map_nul "$p" && printf 'E'; } 2>/dev/null )" || rc=$?
  if [ "$rc" -ne 0 ]; then printf 'state=unreadable\n'; return 0; fi
  case "$body" in
    *E) ;;
    *) printf 'state=unreadable\n'; return 0 ;;
  esac
  body="${body%E}"
  # A byte the capture cannot carry is its OWN state, so the refusal can name it. Reported here
  # rather than left to the grammar: without the mapping this was a manufactured PASS, and WITH the
  # mapping alone it would be a bare `no 'result:' line`, telling the operator nothing about the
  # actual defect in their file.
  case "$body" in
    *"$CAPTURE_NUL_BYTE"*) printf 'state=unrepresentable\n'; return 0 ;;
  esac
  printf 'state=present bytes:\n%s' "$body"
}

# report_state <observation> — THE STATE WORD of a `report_bytes` observation: `absent`,
# `present`, or `unreadable`. ONE READER OF THAT GRAMMAR (#3751 round 13, S1).
#
# WHY IT EXISTS. `classify_report` matched `report_bytes`' prefixes itself and
# `record-author-performed`'s clobber guard did not look at the state AT ALL — it branched on the
# TOKEN, where an unreadable report arrives as `NOT-RUN`, i.e. on the REPLACEABLE side. So a report
# whose recorded verdict was UNKNOWN, possibly a blocking `FINDINGS`, was overwritten by the
# merge-proceeding `AUTHOR-PERFORMED` with no `--force` and no `replaced-verdict:` trace. Two
# readers of one grammar are two opinions about whether a report was READ; there is now one.
#
# AN UNRECOGNISED OBSERVATION IS `unreadable`, WHICH IS THE FAIL-CLOSED WORD, not a fall-through:
# unreachable while `report_bytes` is the only producer (its output is a closed set), and here so
# that a state added to that helper later cannot inherit a permissive branch at either caller.
report_state() {
  case "${1:-}" in
    'state=present bytes:'*) printf 'present\n' ;;
    'state=no-such-file') printf 'absent\n' ;;
    # ITS OWN WORD, so the write-side refusal can say WHY the report could not be read — a byte the
    # capture cannot carry, not a permission. It is NOT in the permissive set at either caller.
    'state=unrepresentable') printf 'unrepresentable\n' ;;
    *) printf 'unreadable\n' ;;
  esac
}

# classify_report <report-path> <stage-open:0|1> [<record-defect>] [<observation>] — print
# "<token>|<cause>" and return 0.
# ONE place decides the token, so `status` and `verdict` can never form two opinions about
# the same file (the divergence #3564 records one directory over) — AND it decides from ONE
# OBSERVATION of that file, so the token cannot describe a state the file never held (round 12, R2).
# THE FOURTH ARGUMENT IS AN OBSERVATION THE CALLER ALREADY TOOK, NOT A LOCATION (#3751 round 12,
# R2). It carries the report's CONTENT in `report_bytes`' grammar, so `$rpath` still decides WHICH
# file this is about and nothing can redirect a reader — the channel round 4 (H2) removed by
# deleting `--report` stays removed, because a path is not expressible here. Its one purpose is to
# let `record-author-performed` make its byte snapshot and its classification ONE observation
# instead of two. Omitted, this function takes its own; a value that is not that grammar is a
# NON-VERDICT, never a permissive fall-through.
classify_report() {
  local rpath="$1" open="$2" record_defect="${3:-}" obs="${4:-}" line value tok cause body defect nl

  # THE RECORD IS ASKED FIRST, BECAUSE IT NAMES WHICH REPORT TO READ (#3751 round 5, J1). A record
  # whose `report-nonce:` cannot be read yields no path at all, so there is nothing to
  # classify — and this is its OWN cause, not one of the report ones: `report absent` and `report
  # ungrammatical` would each assert something about a FILE THAT WAS NEVER IDENTIFIED, and the
  # operator action is different again (repair the record, or open a fresh stage).
  if [ -n "$record_defect" ]; then
    printf 'NOT-RUN|stage record unreadable: %s\n' "$record_defect"; return 0
  fi
  if [ "$open" -ne 1 ]; then
    printf 'NOT-RUN|stage never opened\n'; return 0
  fi
  # ONE OBSERVATION OF THE REPORT, AND EVERY FIELD IS CLASSIFIED FROM IT (#3751 round 12, R2).
  #
  # THE FINDING. This function used to read its subject EIGHT times — existence, a readability
  # probe, the body for emptiness, the `result:` census, the disclosure, then `performed-by`,
  # `reason` and `evidence` each through their own `read_field`. A report REPLACED between two of
  # those reads therefore let the classifier assemble a verdict from fields drawn from DIFFERENT,
  # INDIVIDUALLY INVALID versions and report `AUTHOR-PERFORMED` although NO SINGLE SNAPSHOT of the
  # file ever contained valid working. Measured: with `performed-by`/`reason` read from a version
  # whose `evidence:` was the placeholder `tbd`, and `evidence` read from a version whose `reason:`
  # was `x`, the classifier reported the merge-proceeding token. A verdict is a statement about a
  # document; assembled across two documents it is a statement about neither.
  #
  # This is round 9's N2 property one level down: `premerge-assert.sh` captures the stage record
  # ONCE and parses every field from that capture, for the same reason.
  #
  # THE THREE STATES ARE READ AFFIRMATIVELY, and an observation this function does not recognise is
  # a NON-VERDICT rather than a fall-through — unreachable while `report_bytes` is the only
  # producer (its output is a closed three-state set), and here so that a later change to that
  # helper cannot inherit a permissive branch. It is reported as `report unreadable` because that
  # is what it means — the bytes could not be obtained — and NOT as `report ungrammatical`, which
  # would assert something about content that was never observed (#3751 round 2, B7: a false
  # rationale is worse than none, because it is what stops the next person looking).
  #
  # `report_bytes` also measures readability BY ATTEMPTING THE READ rather than with `[ -r ]`,
  # which answers TRUE for root and cannot see an I/O error, and it asserts the COMPLETE read
  # affirmatively — so a file that opens and then fails mid-read is `report unreadable` here
  # instead of the `report empty` the old `tr` read would have produced, which named the wrong
  # operator action (`chmod`, versus the AGENT).
  [ -n "$obs" ] || obs="$(report_bytes "$rpath")"
  nl='
'
  # THE STATE WORD COMES FROM THE ONE READER OF THAT GRAMMAR (#3751 round 13, S1). This `case`
  # used to match `report_bytes`' prefixes itself while `record-author-performed`'s clobber guard
  # did not consult the state at all — two readers, and the second one treated "could not read it"
  # as "nothing is recorded". `report_state` is now the single reader, so the classifier and the
  # write guard cannot form two opinions about whether the report was READ. Its `*` arm takes the
  # fail-closed word, so a state added to `report_bytes` later refuses at both callers.
  case "$(report_state "$obs")" in
    absent) printf 'NOT-RUN|report absent\n'; return 0 ;;
    unreadable) printf 'NOT-RUN|report unreadable\n'; return 0 ;;
    # `report ungrammatical`, not `report unreadable`: the content WAS observed, and what was
    # observed is that this is not a text record — so the operator action is the AGENT's (rewrite
    # the report), not `chmod`. It keeps the ONE `report-ungrammatical` status state that every
    # variant of that cause shares, deliberately, for the same reason (round 4, H4).
    unrepresentable) printf "NOT-RUN|report ungrammatical: holds a NUL 0x00 or SOH 0x01 byte, which no text record may contain — a shell capture silently DROPS a NUL, so a reader would judge a document this file does not contain\n"; return 0 ;;
    present) ;;
    *) printf 'NOT-RUN|report unreadable\n'; return 0 ;;
  esac
  case "$obs" in
    "state=present bytes:$nl"*) body="${obs#"state=present bytes:$nl"}" ;;
    # An EMPTY report: `report_bytes` emits the prefix and no bytes, and the command substitution
    # that captured it stripped the trailing newline. Distinct arm rather than a `*` catch, so a
    # genuinely empty file is measured as empty rather than as unrecognised.
    "state=present bytes:") body="" ;;
    # A `present` observation whose bytes marker this reader does not recognise. Unreachable while
    # `report_bytes` is the only producer; refused rather than defaulted, for the same reason.
    *) printf 'NOT-RUN|report unreadable\n'; return 0 ;;
  esac
  # "empty" means nothing RECORDABLE — a file of blank lines is empty in every sense a
  # reader cares about, and reporting `report ungrammatical` for it would name the wrong
  # operator action.
  if [ -z "$(printf '%s' "$body" | LC_ALL=C tr -d '[:space:]' 2>/dev/null || true)" ]; then
    printf 'NOT-RUN|report empty\n'; return 0
  fi

  # ANCHORED AT COLUMN ZERO, AND THAT IS WHAT THE ANCHOR IS FOR (round 2, B1). The report
  # body is AUTHOR-CONTROLLED text that contains example verdict lines BY DESIGN — the
  # sentinel `open` writes has to tell the agent the exact spelling of `result: PASS`, and a
  # review report routinely QUOTES another report's verdict line. While this pattern allowed
  # leading whitespace those examples were grammatically valid records, and the only thing
  # keeping them out of the verdict was `-m1` ORDER: delete the sentinel line at column zero
  # (`sed -i '/^result:/d'`, which leaves the indented examples intact) and append your own
  # verdict, and the TEMPLATE's `PASS` won. Order is not inertness. Column zero is a place
  # the payload provably cannot reach — every quoted, indented, `>`-quoted or bulleted copy
  # is DATA — which is #3312's rule (anchor the control token; never pick a rarer delimiter)
  # and the same anchor `premerge-assert.sh`'s `_c_verdict_awk` uses on `/^REVIEW-STAGE: /`.
  # Case-insensitivity is KEPT: `Result:` at column zero is one author's spelling of the
  # control line, not a payload posing as one.
  #
  # AND EXACTLY ONE OF THEM (#3751 round 3, G2). Anchoring without COUNTING left `grep -m1`
  # deciding by ORDER: a stale `result: PASS` followed by an APPENDED `result: FINDINGS`
  # classified as PASS, so a merge proceeded over recorded blocking findings. Order is not a
  # rule — it is whichever line happened to come first — and a LAST-wins read is no better,
  # which is why the refusal comes from the COUNT and both orders are pinned. Zero and several
  # are DISTINCT causes because the operator action differs ("your agent wrote no verdict" /
  # "this report records two").
  #
  # THE OTHER READER OF THIS SHAPE IS `premerge-assert.sh`'s `_c_verdict_awk`, which counts its
  # own column-zero `REVIEW-STAGE: ` lines and refuses several as AMBIGUOUS. Neither reads the
  # other's file, but both answer the same three questions (column zero / exactly one / a closed
  # token set) — and they have now DIVERGED TWICE, once per axis, each time with a reviewer
  # naming one side. So the agreement is MECHANICALLY CHECKED, not maintained by care:
  # `scripts/tests/test_premerge_assert.sh`'s section 44g drives BOTH readers over ONE shared
  # table of adversarial inputs and asserts they agree per row AND reach the expected
  # disposition. If you change the rule here, that test is what tells you the other side moved
  # too — a second implementation's correctness is only knowable by testing it against the first.
  local cands ncand=0
  cands="$( { LC_ALL=C grep -i '^result:' <<<"$body"; } 2>/dev/null || true)"
  if [ -n "$cands" ]; then
    ncand="$(printf '%s\n' "$cands" | LC_ALL=C grep -c . 2>/dev/null || true)"
    case "$ncand" in
      "" | *[!0-9]* ) ncand=0 ;;
    esac
  fi
  if [ "$ncand" -eq 0 ]; then
    printf "NOT-RUN|report ungrammatical: no 'result:' line\n"; return 0
  fi
  if [ "$ncand" -gt 1 ]; then
    printf "NOT-RUN|report ungrammatical: %s column-zero 'result:' lines (AMBIGUOUS — several records is refused, never resolved by order)\n" "$ncand"
    return 0
  fi
  line="$cands"
  value="$(one_line "${line#*:}")"
  if [ -z "$value" ]; then
    printf "NOT-RUN|report ungrammatical: empty 'result:' value\n"; return 0
  fi
  # REDUCE TO THE FIRST WORD AND MATCH BY STRING EQUALITY — never a prefix test. This is the
  # whole closure: `PASS-BUT-UNMEASURED` reduces to `PASS-BUT-UNMEASURED`, which equals
  # nothing in the set, so it is NOT-RUN. A `case` glob or a `grep ^PASS` would accept it.
  #
  # A PARAMETER EXPANSION, NOT `set -- $value` (#3751 round 2, B5). The old form was an
  # UNQUOTED expansion, so the AUTHOR-CONTROLLED value went through PATHNAME EXPANSION as well
  # as word splitting: `result: *`, read from a directory holding a file named `PASS`, globbed
  # to that filename and reported PASS — a false PASS produced by the shell, in the one
  # function whose entire job is a closed grammar. `${value%% *}` neither splits nor globs, and
  # needs no positional clobber; `one_line` has already mapped every tab/newline to a space and
  # squeezed runs, so the first space really is the first word boundary.
  tok="${value%% *}"
  # The recorded cause, when the report names one, is preferred over a guess: an agent that
  # legitimately records `result: NOT-RUN (could not read the diff)` is telling us something
  # more precise than "no report written".
  cause=""
  case "$value" in
    *'('*')'*) cause="${value#*(}"; cause="${cause%)*}" ;;
  esac

  case "$tok" in
    PASS)     printf 'PASS|\n' ;;
    FINDINGS) printf 'FINDINGS|\n' ;;
    NOT-RUN)  printf 'NOT-RUN|%s\n' "${cause:-no report written}" ;;
    AUTHOR-PERFORMED)
      # THE TOKEN MEANS "a disclosed substitute WITH ITS WORKING RECORDED", so the working is
      # REQUIRED before it will be reported. A report asserting the token without the
      # disclosure, the performer or the evidence is not a disclosed substitute — it is a
      # pass wearing a rarer name, which is exactly what the distinct token exists to
      # prevent. Refused as ungrammatical (fail-closed: NOT-RUN blocks, AUTHOR-PERFORMED
      # is conditionally acceptable).
      # A QUOTED `case` PATTERN, not a `grep -qF` on the file: the needle is a script constant
      # with no newline, so a whole-text substring test is the same question the per-line `grep`
      # answered — and it asks it of the ONE snapshot. Quoting the expansion makes it literal, so
      # the disclosure cannot be read as a glob.
      case "$body" in
        *"$AUTHOR_DISCLOSURE"*) ;;
        *) printf 'NOT-RUN|report ungrammatical: AUTHOR-PERFORMED without the required disclosure\n'; return 0 ;;
      esac
      # THE WORKING IS JUDGED BY THE SAME FUNCTION THE WRITER USES (#3751 round 1, F3).
      # A NON-EMPTINESS test standing in for a validity test is the shape this repo pins:
      # `performed-by: nobody`, `reason: x`, `evidence: tbd` are all non-empty and all
      # unusable, and each one reached the token that PROCEEDS at the merge point while
      # `record-author-performed` would have refused it. The cause NAMES the field and the
      # defect, because the operator action differs per field.
      # ALL THREE FROM THE ONE SNAPSHOT (R2). Read from the FILE, these were three independent
      # observations, and a replacement between any two of them assembled valid working out of
      # versions that never coexisted.
      defect="$(author_working_defect \
        "$(read_field_from "$body" performed-by)" \
        "$(read_field_from "$body" reason)" \
        "$(read_field_from "$body" evidence)")"
      if [ -n "$defect" ]; then
        printf 'NOT-RUN|report ungrammatical: AUTHOR-PERFORMED %s\n' "$(author_defect_prose "$defect")"
        return 0
      fi
      printf 'AUTHOR-PERFORMED|\n'
      ;;
    *) printf 'NOT-RUN|report ungrammatical: unrecognised result token %s\n' "'$tok'" ;;
  esac
}

# load_stage <issue> <kind> — set the STAGE_* globals from the stage record, or mark it
# never-opened. Fields that cannot be read are `unknown`, never a fabricated 0 (a counter
# not observed is an error, never an invented value).
#
# THE REPORT PATH IS DERIVED HERE, NOT READ FROM THE RECORD (#3751 round 4, H2). It used to be
# read from the record's `report:` line, so the record was a CONTROL channel naming which file
# holds the verdict: a value split across lines by a newline-bearing `--report` was read as its
# PREFIX, which could name a DIFFERENT pre-existing report recording `PASS` — measured, that
# reported `RESULT: PASS` for a stage whose own sentinel had never been replaced. With
# `--report` gone the path has ONE derivation (`report_path`), used by the writer and by every
# reader, so nothing in a data file can redirect a reader to another file.
#
# AND THE NONCE COMES FROM THE RECORD, WHICH IS THE ONE SOURCE OF TRUTH FOR WHICH REPORT
# COUNTS (#3751 round 5 J1, round 6 K2). `report_path` needs it, and taking it from anywhere else
# would recreate the divergence round 4 closed: the writer and the readers must agree about which
# file a stage means. FOUR answers, and only two of them are a path:
#   exactly one valid token        -> that report
#   NO field at all               -> the LEGACY bare `<kind>.md`. This is an AFFIRMATIVE reading of
#                                    a record written before the field existed, not a permissive
#                                    default: every earlier version of this tool wrote exactly ONE
#                                    report, at that name, so it IS that record's report. Refusing
#                                    here would red on correct input.
#   anything else (several lines, -> a RECORD DEFECT. No path is derived, no report is read, and
#   a token that is not valid)      `classify_report` reports `stage record unreadable`. Falling
#                                    back to the bare name would be the permissive branch on
#                                    "cannot tell", and it is precisely how a stale report's PASS
#                                    would be read as the current verdict.
#   THE READ ITSELF FAILED        -> the same RECORD DEFECT, and NOT the legacy reading (#3751
#                                    round 6, K1): *read failed* and *read fine, field absent* are
#                                    different facts, and a `|| true` that collapsed them let an
#                                    unreadable record report an old report's PASS.
# COUNTED rather than first-wins, for the same reason the `result:` reader counts: this field
# decides which artifact is authoritative, and picking one of two answers by order is not a rule.
STAGE_OPEN=0; STAGE_AGENT=unknown; STAGE_DEADLINE=unknown; STAGE_REPORT=""
STAGE_SPAWNED_ISO=unknown; STAGE_ELAPSED=unknown; STAGE_REOPEN=unknown
STAGE_NONCE=""; STAGE_RECORD_DEFECT=""
load_stage() {
  local issue="$1" kind="$2" sfile epoch now
  sfile="$(stage_file "$issue" "$kind")"
  STAGE_NONCE=""; STAGE_RECORD_DEFECT=""
  if [ ! -f "$sfile" ]; then
    # NEVER OPENED: there is no record to name a report, so the path reported is the LEGACY bare
    # one. It is a path nobody has written, which is what `stage never opened` says.
    STAGE_REPORT="$(report_path "$issue" "$kind" "")"
    return 0
  fi
  STAGE_OPEN=1
  local nnonce nval cfl_rc=0
  # THE READ IS VERIFIED AFFIRMATIVELY, AND A FAILED READ IS ITS OWN DEFECT (#3751 round 6, K1).
  # `count_field_lines` returns non-zero only when the record could not be READ; "read fine, no
  # such field" prints 0 and returns 0. The two were collapsed by a `|| true`, so an unreadable
  # record fell through to the LEGACY reading and an OLD report's `PASS` was reported as the
  # current verdict. The legacy reading below is reserved for a record that WAS read.
  #
  # THE PERMISSIVE SET IS `0` AND NOTHING ELSE (#3751 round 14, T1). Status 2 — the record holds a
  # byte the capture cannot carry — is its own defect with its own next action; every other non-zero
  # status takes the read-failed branch, so a status added to that helper later cannot arrive here
  # as "read fine, no such field". THAT is the branch this issue turns on: a record spelt
  # `report-<NUL>nonce:` holds no `report-nonce:` line, so a faithful `grep` counted a truthful ZERO
  # — which means "a pre-nonce record whose single report is the LEGACY bare name" — and a stale
  # legacy `c.md` recording `result: PASS` was then reported as this stage's verdict.
  nnonce="$(count_field_lines "$sfile" report-nonce)" || cfl_rc=$?
  if [ "$cfl_rc" -eq 2 ]; then
    STAGE_RECORD_DEFECT="the record holds a NUL 0x00 or SOH 0x01 byte, which no text record may contain, so it could not be read as text and which report is current was never measured (rewrite the record or open a fresh stage — NOT a chmod)"
  elif [ "$cfl_rc" -ne 0 ]; then
    STAGE_RECORD_DEFECT="the record EXISTS and could not be READ, so which report is current was never measured (permission or I/O — not the same as a record with no report-nonce)"
  elif [ "$nnonce" -gt 1 ]; then
    STAGE_RECORD_DEFECT="report-nonce appears $nnonce times (AMBIGUOUS — several records is refused, never resolved by order)"
  elif [ "$nnonce" -eq 0 ]; then
    STAGE_NONCE=""
  else
    # READ ONLY WHERE IT IS USED, and only after the count above established that the record can
    # be read at all: `read_field` reports "absent or empty" for an unreadable file too, so its
    # empty value is only meaningful once the read itself is known to have succeeded.
    nval="$(read_field "$sfile" report-nonce)"
    if nonce_is_valid "$nval"; then
      STAGE_NONCE="$nval"
    else
      STAGE_RECORD_DEFECT="report-nonce is not an alphanumeric token of $NONCE_MIN_LEN-$NONCE_MAX_LEN characters ($(field_value "${nval:-<empty>}"))"
    fi
  fi
  if [ -n "$STAGE_RECORD_DEFECT" ]; then
    # NO FABRICATED PATH. `report=` is on the line that is otherwise the authority, so an
    # unmeasurable location is NAMED (`unresolved`) exactly as `head-sha:` names an unresolvable
    # HEAD — round 2's B6 lesson: publishing a path that was never derived is worse than saying
    # there is none. Callers render `${STAGE_REPORT:-unresolved}`.
    STAGE_REPORT=""
  else
    STAGE_REPORT="$(report_path "$issue" "$kind" "$STAGE_NONCE")"
  fi
  local v
  v="$(read_field "$sfile" agent)";         [ -z "$v" ] || STAGE_AGENT="$v"
  v="$(read_field "$sfile" deadline-secs)"; [ -z "$v" ] || STAGE_DEADLINE="$v"
  v="$(read_field "$sfile" spawned-at)";    [ -z "$v" ] || STAGE_SPAWNED_ISO="$v"
  # THE REOPEN COUNTER IS READ, NOT DERIVED, and is `unknown` when the record does not carry it —
  # a record written before the field existed, which is a fact about that record and not a zero
  # (#3751 round 9, N4: `status` reports what the record HOLDS, so the saturation is visible on
  # both surfaces and not only where it was written).
  v="$(read_field "$sfile" reopen-count)";  [ -z "$v" ] || STAGE_REOPEN="$v"
  epoch="$(read_field "$sfile" spawned-epoch)"
  now="$(now_epoch)"
  # BOTH OPERANDS, AND THE BOUND IS THE POINT (#3751 round 8). `$(( ))` does not fail on an
  # unusable operand — it WRAPS, silently — so an out-of-range or zero-padded `spawned-epoch` in
  # the record produced a FABRICATED elapsed time on the line an operator reads (measured: 56
  # years for a stage opened one second earlier, with `past-deadline=yes` and a `PAST DEADLINE`
  # note). `now_epoch` is `date -u +%s`, whose output is not validated anywhere either, so an
  # unusable clock reading is checked on the same terms rather than trusted: `elapsed` is a
  # MEASUREMENT, and a number nobody measured is worse here than the honest `unknown`.
  if int_is_comparable "$epoch" && int_is_comparable "$now"; then
    STAGE_ELAPSED=$(( now - epoch ))
    [ "$STAGE_ELAPSED" -ge 0 ] || STAGE_ELAPSED=0
  else
    STAGE_ELAPSED=unknown
  fi
}

parse_kind_issue() {
  KI_KIND="$(validate_kind "${1:-}")"; shift || true
  KI_ISSUE=""
  while [ $# -gt 0 ]; do
    case "$1" in
      --issue) shift; KI_ISSUE="${1:-}" ;;
      *) die_usage "unknown argument '$1'" ;;
    esac
    shift || true
  done
  KI_ISSUE="$(validate_issue "$KI_ISSUE")"
}

# --- verdict -----------------------------------------------------------------
cmd_verdict() {
  require_repo_root
  parse_kind_issue "$@"
  load_stage "$KI_ISSUE" "$KI_KIND"
  local cls token cause rendered
  cls="$(classify_report "$STAGE_REPORT" "$STAGE_OPEN" "$STAGE_RECORD_DEFECT")"
  token="${cls%%|*}"
  cause="${cls#*|}"
  rendered="$token"
  # THE CAUSE IS DATA INTERPOLATED INTO A CONTROL LINE, SO ITS ONE RESERVED CHARACTER IS
  # NEUTRALISED AT THIS ONE EMIT BOUNDARY (#3312's rule). Part of the cause comes from the
  # REPORT — a self-recorded `result: NOT-RUN (…)` cause, and the unrecognised token this
  # names verbatim — and the report is written by the very agent whose stage is being judged.
  # The rest of the line is `key=value` fields a consumer reads, so a cause carrying
  # `agent=peer` or `elapsed=0` could produce a second, earlier `agent=`/`elapsed=` pair and a
  # scanning consumer would read the report's value instead of the measured one. '=' is
  # therefore mapped to '~' HERE, where the value is rendered, and NOT in the parser: every
  # decision (the token, the exit code) is made on the RAW value before this line is built, so
  # this is display-only and cannot change a verdict. Refusing instead of redacting would be
  # wrong — the cause is a diagnostic the operator has to read, and an unreadable NOT-RUN is
  # worse than a slightly-spelled one. The TOKEN needs no such treatment: it comes from a
  # closed set matched by string equality.
  [ -z "$cause" ] || rendered="$token ($(field_value "$cause"))"
  # EXACTLY ONE LINE on stdout. Nothing else is printed here, ever: this line is what a
  # consumer greps, and a second line is a second opinion. EVERY data value on it goes through
  # `field_value`, the one emit boundary.
  #
  # THAT USED TO BE THREE VALUES SHORT (#3751 round 7, L1). `deadline=`, `agent=` and (on the
  # STATUS line) `spawned-at=` are READ FROM THE STAGE RECORD, and `read_field` routes them
  # through `one_line` — which neutralises control characters but deliberately does NOT map '='.
  # So a hand-edited record's `agent: x deadline=0` put a SECOND `deadline=` pair on the line,
  # ahead of the measured one, for any consumer that scans field by field. `elapsed=` is NOT
  # routed and does not need to be: it is `unknown` or the result of integer arithmetic here,
  # never text read from the record. The rule is the one round 2's S1 states — ONE boundary for
  # EVERY data value on these lines, because the alternative is a per-site list to keep complete.
  # `report=` IS A CONSUMER'S BINDING, NOT ONLY A DIAGNOSTIC (#3751 round 10, P2). It carries the
  # generation's nonce, and `premerge-assert.sh` requires that nonce to be the one it validated in
  # the stage record — which is what catches an ABA replacement its byte comparison cannot see.
  # `unresolved` stays the honest non-measurement, and that consumer refuses on it by name.
  #
  # AND IT IS THE ONE FIELD EXEMPT FROM THE '=' MAP (#3751 round 16, V2) — `remainder_value`, not
  # `field_value`. A repository root may LEGALLY contain '=', and mapping it published a path that
  # DOES NOT EXIST while this grammar promises the absolute report-of-record path (measured: a
  # checkout at `…/eq=path/lane` had `open` print the real file and this line advertise
  # `…/eq~path/…`). The exemption is sound for exactly ONE reason, the round-11 property below:
  # this field is LAST and is read as the line REMAINDER, so an '=' inside it cannot create an
  # ambiguous field and the anti-forgery reason does not apply to it. Control characters are still
  # neutralised in full. See `remainder_value` for the coupling and the confinement, both pinned.
  #
  # AND IT MUST STAY LAST ON THIS LINE (#3751 round 11, Q3). A report path may legitimately contain
  # a SPACE (a checkout at `/tmp/work tree`), so that consumer reads `report=` as the REMAINDER of
  # the line rather than as one whitespace-delimited field — a field read truncated the value and
  # REFUSED a correct verdict. Appending a field after `report=` would silently truncate every such
  # path again; the property is pinned against THIS emitter by section 44l of
  # scripts/tests/test_premerge_assert.sh (its 11 states derived by RUNNING this script), so such a
  # change reds that suite rather than shipping.
  emit "$KI_KIND RESULT: $rendered elapsed=$STAGE_ELAPSED deadline=$(field_value "$STAGE_DEADLINE") agent=$(field_value "$STAGE_AGENT") report=$(remainder_value "${STAGE_REPORT:-unresolved}")"
  case "$token" in
    PASS) exit 0 ;;
    FINDINGS) exit 4 ;;
    NOT-RUN) exit 5 ;;
    AUTHOR-PERFORMED) exit 6 ;;
    # ROUTED, NOT ALLOWLISTED (#3751 round 9, N3). This arm is reached only when the token is NOT
    # in the closed set, i.e. precisely where the "it comes from a closed set" claim an allowlist
    # entry would make is false by construction.
    *) note "unreachable: unclassified token '$(field_value "$token")'"; exit 5 ;;
  esac
}

# --- status ------------------------------------------------------------------
# ADVISORY ONLY. It exits 0 for every state it can measure, on purpose: reading status must
# not be able to decide anything, and a caller that could branch on its exit status would
# have built a second, clock-shaped verdict path beside the content-shaped one.
cmd_status() {
  require_repo_root
  parse_kind_issue "$@"
  load_stage "$KI_ISSUE" "$KI_KIND"
  local cls token cause state past=unknown reopen_disp
  cls="$(classify_report "$STAGE_REPORT" "$STAGE_OPEN" "$STAGE_RECORD_DEFECT")"
  token="${cls%%|*}"
  cause="${cls#*|}"
  # --- STATUS-CAUSE-MAP-BEGIN -------------------------------------------------------------
  # ONE STATE PER CAUSE, BECAUSE THE OPERATOR ACTION DIFFERS PER CAUSE (#3751 round 4, H4). That
  # is the entire justification for `classify_report` naming six causes, and this mapper used to
  # throw two of them away: EVERY unenumerated cause fell through to `report-ungrammatical`, so
  #   * `report unreadable` — fix: `chmod` — was reported as a complaint about CONTENT THAT WAS
  #     NEVER OBSERVED, sending the operator to the agent; and
  #   * a SELF-RECORDED `result: NOT-RUN (ran out of context)` — a perfectly GRAMMATICAL report in
  #     which the agent said WHY — was called ungrammatical, which is affirmatively false about the
  #     file and hides the one piece of information that was actionable.
  # A wrong remediation signal is worse than a vague one, because it is what stops the operator
  # looking.
  #
  # THE BUILT-IN CAUSES ARE ENUMERATED, AND THE FALL-THROUGH IS THE REPORT'S OWN CAUSE. That is
  # sound only while this enumeration covers every cause `classify_report` can emit: a NEW built-in
  # cause added there and not here would be mislabelled `not-run-self-reported`. It is not left to
  # care — `scripts/tests/test_review_stage.sh` §7b DERIVES the built-in cause literals from this
  # file and asserts each one is matched by an arm below, so the drift reds the suite. The several
  # `report ungrammatical: <what>` variants deliberately share ONE state: the operator action is
  # the same for all of them (the agent wrote a bad verdict line).
  case "$token" in
    NOT-RUN)
      case "$cause" in
        "no report written") state=sentinel-only ;;
        "report absent") state=report-absent ;;
        "report unreadable") state=report-unreadable ;;
        "report empty") state=report-empty ;;
        "report ungrammatical"*) state=report-ungrammatical ;;
        "stage never opened") state=never-opened ;;
        "stage record unreadable"*) state=stage-record-unreadable ;;
        *) state=not-run-self-reported ;;
      esac
      ;;
    *) state=reported ;;
  esac
  # --- STATUS-CAUSE-MAP-END ---------------------------------------------------------------
  # AFFIRMATIVE, NOT "is it the literal `unknown`" (#3751 round 7, L1) — AND DIGITS ARE NOT THE
  # BOUND (#3751 round 8, roborev job 379). `STAGE_DEADLINE` is read from the stage record and is
  # deliberately NOT validated on the read side (the record's own text is DISPLAYED, routed
  # through `field_value`, so a hand edit stays visible in the audit trail); what has to be
  # affirmative is this COMPARISON. Round 7 closed the non-digit half: a hand-edited
  # `deadline-secs: 1800 agent=forged` fell into `[ ... -gt ... ]`, which printed bash's own
  # `integer expression expected` onto stderr — a raw diagnostic inside a block whose every line
  # is supposed to carry the `REVIEW-STAGE: ` anchor — and then took the `past=no` branch.
  # An ALL-DIGIT value wider than int64 did exactly the same thing, because `[ ]` is a
  # fixed-width comparison, so the gate is `int_is_comparable` (see its comment) and not a digit
  # test. Anything it refuses — `unknown` included — is `unknown` here: `status` is ADVISORY and
  # never a verdict input, but an advisory that answers `no` from a comparison that never ran is
  # still wrong, and `no` is the permissive answer.
  past=unknown
  if int_is_comparable "$STAGE_ELAPSED" && int_is_comparable "$STAGE_DEADLINE"; then
    if [ "$STAGE_ELAPSED" -gt "$STAGE_DEADLINE" ]; then past=yes; else past=no; fi
  fi

  # THE `s` UNIT BELONGS ONLY TO A VALUE THAT IS A COUNT OF SECONDS. `unknown` is a legitimate
  # value for both of these (a stage record whose spawned-epoch or deadline-secs bash cannot
  # compare yields `unknown` rather than a silently wrapped number), and appending the unit
  # unconditionally rendered `unknowns` on the advisory line a human actually reads. Computed
  # HERE, before the branch chain below, because both arms consume it and an assignment inside
  # one arm leaves the other unbound under `set -u`. The comparable case is bare digits and needs
  # no sanitising; the value is sanitised AT THE EMIT SITE like every other, through
  # `field_value`, the one boundary — round 7 made that routing structural, and this comment is
  # here because the guard correctly red my first attempt at this fix. Reuses
  # `int_is_comparable` rather than re-deriving the predicate: two spellings of "is this a number
  # bash can compare" is two places for it to drift.
  elapsed_disp="$STAGE_ELAPSED"
  deadline_disp="$STAGE_DEADLINE"
  # RENDERED BY THE SAME FUNCTION `open` USES (#3751 round 9, N4), so the two surfaces cannot form
  # two opinions about the same record. Incomparable text is displayed VERBATIM and carries no
  # at-least marker — round 8's disposition: what is affirmative is the COMPARISON, not the display.
  reopen_disp="$(reopen_display "$STAGE_REOPEN")"
  if int_is_comparable "$STAGE_ELAPSED"; then elapsed_disp="${STAGE_ELAPSED}s"; fi
  if int_is_comparable "$STAGE_DEADLINE"; then deadline_disp="${STAGE_DEADLINE}s"; fi

  emit "STATUS kind=$KI_KIND issue=$KI_ISSUE state=$state elapsed=$STAGE_ELAPSED deadline=$(field_value "$STAGE_DEADLINE") past-deadline=$past agent=$(field_value "$STAGE_AGENT") spawned-at=$(field_value "$STAGE_SPAWNED_ISO") reopen-count=$(field_value "$reopen_disp") report=$(field_value "${STAGE_REPORT:-unresolved}")"
  if [ "$state" = sentinel-only ] && [ "$past" = yes ]; then
    # A STAGE THAT IS WAITING MUST NOT LOOK LIKE ONE THAT IS HUNG (the gate's
    # `waiting for gate slot` idiom): name the elapsed time AND the fact that nothing was
    # produced, so the operator does not have to infer either.
    emit "STATUS-NOTE kind=$KI_KIND issue=$KI_ISSUE PAST DEADLINE: $(field_value "$elapsed_disp") elapsed against a $(field_value "$deadline_disp") deadline and NOTHING has been produced — the report is still the pre-spawn sentinel. This is ADVISORY: the deadline never changes the verdict, and a report arriving later is still a report. Read the verdict with: $prog verdict $KI_KIND --issue $KI_ISSUE"
  elif [ "$state" = sentinel-only ]; then
    emit "STATUS-NOTE kind=$KI_KIND issue=$KI_ISSUE inside deadline: $(field_value "$elapsed_disp") of $(field_value "$deadline_disp") elapsed and nothing produced yet — the report is still the pre-spawn sentinel, which is NOT a verdict."
  elif [ "$state" = not-run-self-reported ]; then
    # THE AGENT'S OWN CAUSE IS THE ACTIONABLE PART, so it is passed through — via `field_value`,
    # the one emit boundary, because it is report-supplied DATA on a line carrying `key=value`
    # fields (#3312's rule, same as the verdict line's cause).
    emit "STATUS-NOTE kind=$KI_KIND issue=$KI_ISSUE the report RECORDS a NOT-RUN of its own, naming: $(field_value "$cause") — the file is grammatical and the agent said WHY, so the next action is what that cause names, not a chmod and not a re-written verdict line. ADVISORY, as always: read the verdict with: $prog verdict $KI_KIND --issue $KI_ISSUE"
  elif [ "$state" = report-unreadable ]; then
    emit "STATUS-NOTE kind=$KI_KIND issue=$KI_ISSUE the report file EXISTS but could not be READ, so nothing is claimed about its content — the fix is a permission or an I/O one (chmod / the filesystem), NOT a re-spawn: $prog verdict $KI_KIND --issue $KI_ISSUE"
  elif [ "$state" = stage-record-unreadable ]; then
    # THE FIX IS THE RECORD, NOT THE AGENT AND NOT A chmod — a distinct next action, which is the
    # whole reason each cause gets its own state (#3751 round 4, H4).
    emit "STATUS-NOTE kind=$KI_KIND issue=$KI_ISSUE the STAGE RECORD cannot be read for the field that names which report is current ($(field_value "$STAGE_RECORD_DEFECT")) — so NOTHING is claimed about any report, and no report path is reported. Repair the record, or open a fresh stage: $prog open $KI_KIND --issue $KI_ISSUE --agent <type> --force"
  elif [ "$state" = never-opened ]; then
    emit "STATUS-NOTE kind=$KI_KIND issue=$KI_ISSUE no stage was ever opened for this kind — there is nothing to wait for. Open one BEFORE spawning: $prog open $KI_KIND --issue $KI_ISSUE --agent <type>"
  fi
  exit 0
}

# --- record-author-performed -------------------------------------------------
cmd_record_author_performed() {
  require_repo_root
  # THIS subcommand's refusals — including those raised by the shared path/write helpers —
  # report AUTHOR-REFUSED, never open's marker (S2).
  REFUSE_MARKER="AUTHOR-REFUSED"
  local kind="" issue="" reason="" evidence="" performed_by="" force=0
  kind="$(validate_kind "${1:-}")"; shift || true
  while [ $# -gt 0 ]; do
    case "$1" in
      --issue) shift; issue="${1:-}" ;;
      --reason) shift; reason="${1:-}" ;;
      --evidence) shift; evidence="${1:-}" ;;
      --performed-by) shift; performed_by="${1:-}" ;;
      --force) force=1 ;;
      *) die_usage "record-author-performed: unknown argument '$1'" ;;
    esac
    shift || true
  done
  issue="$(validate_issue "$issue")"

  # ALL FOUR ARE REQUIRED, and each names what it is for. The recording REQUIRES THE WORKING
  # (design.md §4): "an audit I performed and showed my working for is auditable, whereas an
  # absent one is not" is the reason the fallback is sanctioned AT ALL, so a recording
  # without the working would be the absent audit wearing the sanctioned token.
  #
  # JUDGED BY author_working_defect — the SAME function `verdict` classifies a hand-written
  # report with (#3751 round 1, F3). Only the RENDERING differs: a flag the caller can fix
  # gets a usage error naming the flag and an example, where the classifier gets a NOT-RUN
  # cause. Two renderings of one judgement cannot drift into two strengths; two judgements did.
  local defect field kind tok flag raw example
  defect="$(author_working_defect "$performed_by" "$reason" "$evidence")"
  if [ -n "$defect" ]; then
    field="${defect%%|*}"
    kind="${defect#*|}"; kind="${kind%%|*}"
    tok="${defect##*|}"
    case "$field" in
      performed-by) flag="--performed-by"; raw="$performed_by"; example="author" ;;
      reason) flag="--reason"; raw="$reason"
        example="'no peer agent available on this box; C performed by hand against the spec deltas'" ;;
      *) flag="--evidence"; raw="$evidence"; example="docs/round-artifacts/issue-3751-hand-c-audit.md" ;;
    esac
    case "$field:$kind" in
      performed-by:absent)
        die_usage "record-author-performed: --performed-by author is required — this subcommand records the DIFF AUTHOR's own audit, and stating so explicitly is the whole disclosure" ;;
      performed-by:not-in-set)
        die_usage "record-author-performed: --performed-by must be exactly 'author', got '$performed_by' — this subcommand records the AUTHOR's own audit and reports the token AUTHOR-PERFORMED, so no other performer can be stated truthfully. A PEER who can perform the audit should write the report of record instead ($prog open <kind> --issue <N> --agent <type>), which reaches a genuine PASS" ;;
      reason:absent)
        die_usage "record-author-performed: --reason <why> is required — say why an independent audit was not available; a substitute with no stated reason is not a disclosure" ;;
      evidence:absent)
        die_usage "record-author-performed: --evidence <artifact> is required — name the artifact that SHOWS THE WORKING (a file, a PR comment, a commit); an audit with no evidence is indistinguishable from an absent one" ;;
      *:unsubstituted)
        die_usage "record-author-performed: $flag '$raw' still carries an UNSUBSTITUTED placeholder (<…>) — substitute it, e.g. $flag $example" ;;
      *:unrecordable)
        die_usage "record-author-performed: $flag must carry at least 3 recordable characters ([A-Za-z0-9._:/#-]); '$raw' records as '$tok', which is indistinguishable from saying nothing" ;;
      *:placeholder)
        die_usage "record-author-performed: $flag '$raw' records as the PLACEHOLDER '$tok' — as uninformative as saying nothing. Say what it IS, e.g. $flag $example" ;;
      *)
        die_usage "record-author-performed: $flag is unusable ($kind)" ;;
    esac
  fi
  local reason_tok evidence_tok
  reason_tok="$(sanitize_field "$reason")"
  evidence_tok="$(sanitize_field "$evidence")"

  load_stage "$issue" "$kind"
  if [ "$STAGE_OPEN" -ne 1 ]; then
    # A recording needs the stage's identity (agent, deadline, spawned-at) to produce a
    # verdict line at all, and a substitute recorded for a stage nobody ever opened has no
    # subject. Refused, not auto-opened: silently creating the stage here would let the
    # recording invent its own clock.
    emit "AUTHOR-REFUSED reason=stage-never-opened kind=$kind issue=$issue"
    emit "AUTHOR-REFUSED detail=open the stage first, so the recording attaches to a stage with a known agent and clock: $prog open $kind --issue $issue --agent <type>"
    exit 2
  fi
  # AND A RECORD THAT DOES NOT NAME ITS CURRENT REPORT IS NOT WRITABLE (#3751 round 5, J1). With
  # no readable `report-nonce:` there is no derived path, so writing would mean GUESSING one —
  # and the guess (the bare name) is the path an earlier agent may still hold. Refused BEFORE the
  # already-recorded check below, because that check reads a report this stage cannot locate.
  if [ -n "$STAGE_RECORD_DEFECT" ]; then
    emit "AUTHOR-REFUSED reason=stage-record-unreadable kind=$kind issue=$issue record=$(field_value "$(stage_file "$issue" "$kind")") defect=$(field_value "$STAGE_RECORD_DEFECT")"
    emit "AUTHOR-REFUSED detail=the stage record does not name which report of this stage is current, so this recording has no destination and NOTHING was written. Repair the record, or open a fresh stage: $prog open $kind --issue $issue --agent <type> --force"
    exit 2
  fi
  # A RECORDED VERDICT IS NOT SILENTLY REPLACEABLE (#3751 round 2, B2). This subcommand used
  # to write the report unconditionally, so a recorded blocking `FINDINGS` became a
  # merge-PROCEEDING `AUTHOR-PERFORMED` with no flag, no warning and no trace of what was
  # destroyed — the asymmetry that makes it a defect rather than a design choice is that
  # `open` refuses to re-stamp an already-open stage without `--force` for the far smaller
  # harm of restarting a clock. The `--reason`/`--evidence` recorded here say why no
  # INDEPENDENT audit was available; they say nothing about findings being discarded, so they
  # cannot stand in for that disclosure. A SENTINEL-ONLY report stays freely replaceable —
  # that is the normal path, and a guard that reds on correct input is the guard agents learn
  # to waive.
  #
  # THE OBSERVATION IS TAKEN BEFORE THE DECISION IS MADE ON IT (#3751 round 9, N1) — AND THE
  # DECISION IS MADE ON THAT SAME OBSERVATION (#3751 round 12, R2). Round 9 read the bytes and then
  # let `classify_report` re-read the file, arguing that taking the bytes FIRST meant any change
  # between the two calls was still caught by the re-verification below. That argument was right
  # about the guard and left a smaller hole in the VERDICT: the token guarding this write could be
  # a classification of a state the snapshot never held. So the snapshot is PASSED IN, and the
  # pair (the bytes this write is guarded on, the verdict read from them) is ONE observation.
  #
  # AND "COULD NOT READ IT" IS NOT "NOTHING IS RECORDED" (#3751 round 13, S1). This guard branched
  # on the TOKEN alone, where an UNREADABLE report — the state round 12's R2 introduced — arrives
  # as `NOT-RUN`, i.e. on the REPLACEABLE side. So a report whose recorded verdict was UNKNOWN,
  # possibly a blocking `FINDINGS`, was overwritten by the merge-proceeding `AUTHOR-PERFORMED`
  # token with no `--force` and no `replaced-verdict:` trace (measured: a mode-000 report holding
  # `result: FINDINGS` yielded `RECORD-OK result=AUTHOR-PERFORMED`, exit 0, findings gone). That is
  # this repository's central rule broken inside its own mechanism: "cannot tell" must never take
  # the permissive branch, and *unknown* is not *absent*.
  #
  # THE PERMISSIVE SET IS THEREFORE AFFIRMATIVE — the two states that were MEASURED, named: `absent`
  # (verified-absent, so there is no recorded verdict to destroy) and `present` (read, so the token
  # below decides). A `!= unreadable` test would admit every state added later; this way a new state
  # refuses by construction.
  #
  # `--force` DELIBERATELY DOES NOT COVER IT, for the same reason the re-verification below is not
  # coverable by it: `--force` authorizes replacing THE VERDICT THE OPERATOR READ, and nobody read
  # this one — and refusing strands no one, because `open <kind> --force` moves the stage to a fresh
  # report at a fresh nonce and leaves the unreadable file on disk as history.
  # THE PATH WALK IS NOW A *READ*-SIDE GUARD, AND IT IS STILL REQUIRED (#3751 round 15, U1). Round
  # 1's F5 walk ran here because this subcommand WROTE `$STAGE_REPORT`; since U1 it writes a fresh
  # generation instead and never touches this path — but it still READS THE PRIOR VERDICT FROM IT,
  # and two of the states the walk refuses would corrupt that reading:
  #   a SYMLINK  -> the decision would be made on ANOTHER FILE's content, and the
  #                 `supersedes-report-nonce:` trail this recording leaves would name a generation
  #                 whose content lives somewhere else entirely;
  #   a DIRECTORY (or any non-regular file) -> `report_bytes`' `[ ! -f ]` probe answers
  #                 `no-such-file`, i.e. the PERMISSIVE `absent` state, so a report nobody could
  #                 measure would be read as "no recorded verdict to supersede". "Cannot tell" must
  #                 not take the permissive branch, and that is what this walk stops here.
  # `assert_ignored` is deliberately NOT called on this path any more: its reason is that this tool
  # WRITES the file mid-run (#2926/#3648), and it no longer does. The two paths this call does write
  # are each verified where they are claimed — the fresh report inside `reserve_report_path`, the
  # stage record immediately before its own `prepare_write`.
  assert_no_symlink "$STAGE_REPORT" report-of-record
  local prior_cls prior_token prior_obs prior_state replaced=""
  prior_obs="$(report_bytes "$STAGE_REPORT")"
  prior_state="$(report_state "$prior_obs")"
  prior_cls="$(classify_report "$STAGE_REPORT" 1 "" "$prior_obs")"
  prior_token="${prior_cls%%|*}"
  case "$prior_state" in
    absent | present) ;;
    *)
      emit "AUTHOR-REFUSED reason=prior-verdict-unreadable kind=$kind issue=$issue prior-state=$(field_value "$prior_state") report=$(field_value "$STAGE_REPORT")"
      emit "AUTHOR-REFUSED detail=the report of record could NOT BE READ, so whether it already records a verdict is UNKNOWN — possibly a blocking FINDINGS — and NOTHING was written. An unreadable prior verdict is not an absent one, so it cannot be known to be replaceable. Make the file readable and read it first ($prog verdict $kind --issue $issue), or supersede the stage with a FRESH report ($prog open $kind --issue $issue --agent <type> --force), which leaves this file on disk as history. --force does NOT cover this: it authorizes replacing the verdict you READ."
      exit 2
      ;;
  esac
  case "$prior_token" in
    PASS | FINDINGS)
      if [ "$force" -ne 1 ]; then
        emit "AUTHOR-REFUSED reason=verdict-already-recorded kind=$kind issue=$issue recorded-verdict=$prior_token report=$(field_value "$STAGE_REPORT")"
        emit "AUTHOR-REFUSED detail=this stage already RECORDS a verdict, and superseding it here without saying so would report a merge-proceeding AUTHOR-PERFORMED over a verdict somebody wrote — a recorded FINDINGS would stop blocking. Read it first ($prog verdict $kind --issue $issue). If the substitute really does supersede it, pass --force: the replaced token AND the generation it came from are then RECORDED in the new report, and that generation's report stays on disk as history."
        exit 2
      fi
      replaced="$prior_token"
      note "--force: SUPERSEDING a recorded $prior_token verdict with AUTHOR-PERFORMED; the replaced token and the generation it came from are recorded in the new report, and that generation's report is left on disk as history"
      ;;
  esac

  # THE SUBSTITUTE LANDS IN A FRESH GENERATION — THE PRIOR REPORT IS NEVER WRITTEN OVER (#3751
  # round 15, U1).
  #
  # THE FINDING, AND WHY ROUND 9's DECLARED RESIDUAL WAS NOT ACCEPTABLE. Every earlier version of
  # this subcommand wrote the substitute AT `$STAGE_REPORT`, so the last act of the recording was a
  # `rename(2)` OVER the report of record. Round 9 (N1) narrowed the window by re-observing the
  # bytes immediately before that rename and DECLARED the remainder as a narrow, irreducible span
  # (a shell has no compare-and-swap rename), accepting that a verdict landing in it would be
  # lost. That was true about the shell and wrong about the HARM. The party who loses their verdict in that span is not a hostile racer: it is A SLOW
  # REVIEWER, and #3751 exists BECAUSE delegated reviewers are slow and return late. So the loss
  # was caused by this population's own normal behaviour, and what was lost was a RECORDED REVIEW
  # VERDICT — precisely the harm this issue was filed to prevent. Measured on the shipped script
  # with the interleaving driven at that instant: `RECORD-OK … result=AUTHOR-PERFORMED` at exit 0,
  # no `--force`, no `replaced-verdict:`, and the blocking `result: FINDINGS` GONE FROM DISK
  # ENTIRELY (`grep -r` across the stage directory found nothing).
  #
  # SO THE OVERWRITE IS MADE STRUCTURALLY IMPOSSIBLE INSTEAD OF NARROWED. This uses the generation
  # machinery that already exists — round 6's nonce and round 12's atomic reservation: the
  # substitute is written to a FRESHLY RESERVED report path, and the stage record (the publication
  # marker, written LAST — round 4's H1) is what names it. Nothing writes to `$STAGE_REPORT` at
  # all, so a late reviewer's `FINDINGS` in the previous generation IS NEVER DESTROYED: it stays on
  # disk, readable, in its own generation, which is what an audit trail is for. That is the same
  # subtraction round 6 made for `open --force` (J1), applied to the one write path that still
  # clobbered.
  #
  # WHAT IS STILL A SEPARATE QUESTION, AND KEEPS ITS EXISTING RULE: whether the command may PROCEED
  # when a prior verdict exists. Protecting the BYTES does not authorize the ACT, so the guard
  # above is unchanged — refuse without `--force`, and under `--force` record the `replaced-verdict:`
  # trace naming the prior token AND the prior generation. Since nothing is overwritten, a wrong
  # decision there is now RECOVERABLE and AUDITABLE rather than silent.
  #
  # ROUND 13's S1 RULE IS UNCHANGED TOO: an UNREADABLE prior verdict still refuses, because
  # *unknown* is not *absent* — and it refuses even though nothing would be destroyed, since a
  # recording that supersedes a verdict nobody could read is still a merge-proceeding token
  # published over an unknown one.
  local sfile dir rec_text srt_rc=0
  sfile="$(stage_file "$issue" "$kind")"
  dir="$(dirname "$sfile")"
  # THE RECORD'S OWN BYTES, READ BEFORE ANYTHING IS WRITTEN. The rewrite below substitutes exactly
  # one line of them, so every other field — `head-sha:` above all — comes out unchanged. A
  # `load_stage` that already reported no `STAGE_RECORD_DEFECT` does not make this read redundant:
  # it is the text that will be WRITTEN BACK, and a read that failed or was truncated must not
  # become the whole record.
  rec_text="$(stage_record_text "$sfile")" || srt_rc=$?
  case "$srt_rc" in
    0) ;;
    2)
      emit "AUTHOR-REFUSED reason=stage-record-unrepresentable kind=$kind issue=$issue record=$(field_value "$sfile")"
      emit "AUTHOR-REFUSED detail=this stage's record holds a NUL 0x00 or SOH 0x01 byte, which no text record may contain, so it could not be read as text and NOTHING was written. This recording has to REPUBLISH the record naming a fresh report generation, and a record that cannot be read as text cannot be rewritten without making that byte durable. Rewrite the record as text, or remove the stage directory and open a fresh stage. This is NOT a permission problem: do not chmod it."
      exit 2
      ;;
    *)
      emit "AUTHOR-REFUSED reason=stage-record-unreadable kind=$kind issue=$issue record=$(field_value "$sfile")"
      emit "AUTHOR-REFUSED detail=this stage's record EXISTS and could not be READ, so the record this recording must REPUBLISH could not be measured and NOTHING was written. A truncated or failed read must not be written back as the whole record — that would silently drop the head-sha this stage is bound to. Fix the record's permissions, or remove the stage directory and open a fresh stage."
      exit 2
      ;;
  esac

  # THE FRESH GENERATION, CLAIMED ATOMICALLY (#3751 round 12, R1) — the same call `open` makes,
  # with the same two named causes, because the operator action differs: a token that could not be
  # GENERATED means this box has no usable `mktemp`; tokens that could not be CLAIMED mean the
  # directory cannot be written. NO FALLBACK to an unreserved name: that is exactly the value the
  # reservation removes, and a predictable substitute would let this recording land on a
  # HISTORICAL report of this stage — destroying the audit trail through the other door.
  local rrc=0 new_nonce new_rpath
  reserve_report_path "$issue" "$kind" "$dir" || rrc=$?
  if [ "$rrc" -ne 0 ]; then
    if [ "$rrc" -eq 1 ]; then
      emit "AUTHOR-REFUSED reason=report-nonce-not-generated kind=$kind issue=$issue value=$(field_value "${REPORT_NONCE:-<none>}")"
      emit "AUTHOR-REFUSED detail=an unpredictable report nonce could not be generated, so NO fresh report path was derived and NOTHING was written. The nonce comes from mktemp -u's name substitution, so this box has no usable mktemp. There is deliberately no fallback to a predictable token: a substitute recorded at a name this tool can guess is a substitute that can land on a HISTORICAL report of this stage."
    else
      emit "AUTHOR-REFUSED reason=report-nonce-not-reserved kind=$kind issue=$issue attempts=$RESERVE_ATTEMPTS"
      emit "AUTHOR-REFUSED detail=a fresh report path could not be CLAIMED in $RESERVE_ATTEMPTS attempt(s), so NOTHING was written and the stage record still names the report it named before. Each attempt generates a fresh nonce and creates that path O_EXCL, so a name already on disk is retried rather than written through. Either this stage directory is not writable, or something is occupying the names generated."
    fi
    exit 2
  fi
  new_nonce="$REPORT_NONCE"
  new_rpath="$REPORT_RESERVED"
  # WHICH GENERATION THIS SUBSTITUTE TAKES OVER FROM. `legacy` is the affirmative reading of a
  # PRE-NONCE record (the one report every earlier version of this tool wrote, at the bare
  # `<kind>.md`), not a placeholder for something unmeasured — a record we could not read has
  # already refused above.
  local prior_gen="${STAGE_NONCE:-legacy}"

  prepare_write "$new_rpath" report-of-record
  {
    printf '# review stage: %s — issue #%s (AUTHOR-PERFORMED substitute)\n' "$kind" "$issue"
    printf '\n'
    printf 'result: AUTHOR-PERFORMED\n'
    printf '\n'
    # WHICH GENERATION THIS FILE IS. A human-facing note, exactly as the sentinel report carries
    # one: an agent (or an operator) holding a SUPERSEDED file can see that it is not the current
    # one. It is NOT a location and no reader takes the path from it — readers take the nonce from
    # the STAGE RECORD and nothing else, because a report is author-controlled text (round 4, H2).
    printf 'report-nonce: %s\n' "$new_nonce"
    # AND WHICH GENERATION IT TOOK OVER FROM — always a fact, so always printed (#3751 round 15,
    # U1). It is what an operator follows to the report that was current before this recording,
    # which is STILL ON DISK: this substitute was written to a fresh generation and overwrote
    # nothing.
    printf 'supersedes-report-nonce: %s\n' "$prior_gen"
    # THE TRACE. Emitted only when something was actually replaced, so its ABSENCE is not a
    # claim: a normal recording over the sentinel says nothing about a replacement. It names the
    # TOKEN that stopped being this stage's verdict; the generation it lived in is on the line
    # above, so the two together say exactly where to read what was superseded.
    [ -z "$replaced" ] || printf 'replaced-verdict: %s\n' "$replaced"
    printf 'performed-by: %s\n' "$performed_by"
    printf 'reason: %s\n' "$reason_tok"
    printf 'evidence: %s\n' "$evidence_tok"
    printf 'recorded-at: %s\n' "$(now_iso)"
    printf 'stage: %s\n' "$kind"
    printf 'issue: %s\n' "$issue"
    printf 'agent: %s\n' "$STAGE_AGENT"
    printf 'spawned-at: %s\n' "$STAGE_SPAWNED_ISO"
    printf '\n'
    printf '## Disclosure (required, verbatim)\n'
    printf '\n'
    printf '%s\n' "$AUTHOR_DISCLOSURE"
    printf '\n'
    printf 'This stage reports the DISTINCT token AUTHOR-PERFORMED, never PASS. A reader\n'
    printf 'grepping the passing token does not match it, for the same reason the roborev\n'
    printf "wrapper's WAIVED is distinct: nobody may read a substitute as the real thing.\n"
    printf 'Peer review is preferred; a hand audit is the sanctioned fallback only, and it\n'
    printf 'is sanctioned at all because an audit whose working is shown is auditable,\n'
    printf 'whereas an absent one is not.\n'
    printf '\n'
    printf 'THIS FILE IS A FRESH GENERATION. It overwrote nothing: the report that was current\n'
    printf 'before this recording is still on disk, under the nonce named by\n'
    printf '`supersedes-report-nonce:` above, and a verdict a late reviewer wrote into it is\n'
    printf 'readable there. A superseded report is HISTORY, which is what an audit trail is for.\n'
  } >&9
  commit_write "$new_rpath" report-of-record

  # THE STAGE RECORD, WRITTEN LAST: its content is what PUBLISHES which report of this stage is
  # current (#3751 round 4, H1), so it must not name the substitute until the substitute is
  # actually on disk. Reversed, a record naming a generation that does not exist yet is a
  # `report absent` non-verdict — which is a refusal, not a false certification — and that is the
  # partial state this order chooses.
  #
  # EVERY OTHER BYTE OF THE RECORD IS CARRIED THROUGH VERBATIM (see `record_text_with_nonce`).
  # `head-sha:` in particular is NOT re-stamped: this recording is not a re-open, and re-stamping
  # it would bind a substitute to a tree the stage was never opened at — round 5's J1 harm. Nor is
  # `reopen-count:` incremented: no agent was re-spawned, and that counter answers a different
  # question.
  local new_rec rtwn_rc=0 nonce_lines=0 nonce_back=""
  new_rec="$(record_text_with_nonce "$rec_text" "$new_nonce")" || rtwn_rc=$?
  # THE REWRITE IS MEASURED, NOT ASSUMED. A record that came out with no `report-nonce:` line, or
  # with several, or with a value that is not the generation just reserved, would publish a stage
  # every reader then refuses — this tool bricking its own stage — so a positive verdict here
  # requires an affirmative measurement, taken over the TEXT ABOUT TO BE WRITTEN and read with the
  # readers' own grammar rather than a second spelling of it.
  if [ "$rtwn_rc" -eq 0 ]; then
    nonce_lines="$(LC_ALL=C grep -c -i '^[[:space:]]*report-nonce:' <<<"$new_rec" 2>/dev/null || true)"
    nonce_back="$(read_field_from "$new_rec" report-nonce)"
  fi
  if [ "$rtwn_rc" -ne 0 ] || [ "$nonce_lines" != 1 ] || [ "$nonce_back" != "$new_nonce" ]; then
    emit "AUTHOR-REFUSED reason=record-rewrite-unverified kind=$kind issue=$issue record=$(field_value "$sfile") rewrite-rc=$rtwn_rc nonce-lines=$(field_value "$nonce_lines") nonce-read-back=$(field_value "${nonce_back:-<none>}") want=$(field_value "$new_nonce")"
    emit "AUTHOR-REFUSED detail=the stage record could not be rewritten to name the fresh report generation, so NOTHING was published and the record still names the report it named before. The substitute report was written at the fresh generation and is left on disk as history, exactly as a superseded report is; nothing was destroyed. Read the record and repair it, or remove the stage directory and open a fresh stage."
    exit 2
  fi
  assert_ignored "$sfile" stage-record
  prepare_write "$sfile" stage-record
  printf '%s\n' "$new_rec" >&9
  # RE-VERIFIED IMMEDIATELY BEFORE THE PUBLICATION (#3751 round 9, N1; retargeted in round 15,
  # U1). Round 9 put this check immediately before the rename that OVERWROTE the report, because
  # the harm it guarded was destruction. Destruction is now impossible by construction, so what
  # this check guards is the DECISION: the recording was authorized against the verdict the
  # operator READ, and if a different one arrived since, nobody authorized superseding THAT.
  #
  # ONE RULE, NOT A MATRIX: the report must be BYTE-IDENTICAL to the observation this call decided
  # on. Any change at all refuses — `--force` included, because `--force` authorizes replacing the
  # verdict the operator read, not one that arrived afterwards.
  #
  # AND THE RECORD MUST STILL BE THE RECORD THIS REWRITE WAS DERIVED FROM. This call now WRITES the
  # record, which round 9's version did not, so it owes the same guarantee about it: a concurrent
  # `open --force` that published a new generation in the meantime would otherwise be silently
  # reverted by a rewrite of the bytes this process read before it.
  #
  # THE REMAINING WINDOW IS DECLARED, AND ITS CONSEQUENCE IS NOT DESTRUCTION. The span between
  # these reads and the `rename(2)` inside the single `mv` below is still one fork/exec wide, and
  # there is still no compare-and-swap rename reachable from a shell (coreutils `mv` exposes
  # neither `RENAME_EXCHANGE` nor `RENAME_NOREPLACE`). What lands in it is a verdict that gets
  # SUPERSEDED rather than DESTROYED: it stays on disk in its own generation and `verdict` reports
  # the published one. Round 9 declared this span as irreducible and accepted that a recorded
  # review verdict could be LOST in it; that declaration is WITHDRAWN, because the harm was the
  # overwrite and the overwrite is gone. The window itself is not closed, and no site may claim
  # a lost verdict is still possible here.
  local now_obs now_cls now_rec_obs
  now_obs="$(report_bytes "$STAGE_REPORT")"
  if [ "$now_obs" != "$prior_obs" ]; then
    # The classification is produced HERE, on the refusal path ONLY: it is a DIAGNOSTIC naming
    # what arrived, never an input to the decision, which was made on the byte comparison above.
    # Keeping it off the success path is also what keeps the window minimal. It is classified FROM
    # `now_obs` rather than by a fresh read (#3751 round 12, R2), so the bytes that FAILED the
    # comparison and the verdict this line reports are the same observation — a re-read could
    # otherwise name a third state, and "what arrived" would be a claim about none of them.
    now_cls="$(classify_report "$STAGE_REPORT" 1 "" "$now_obs")"
    emit "$REFUSE_MARKER reason=report-changed-mid-write kind=$kind issue=$issue report=$(field_value "$STAGE_REPORT") now-verdict=$(field_value "${now_cls%%|*}")"
    emit "$REFUSE_MARKER detail=the report of record CHANGED between the already-recorded check and this publication, so NOTHING was published — the stage record still names the report it named before, and whatever is in that report now is intact and untouched. This is the interleaving that guard exists to stop: a review landing a verdict while a substitute was being prepared must not be superseded by the merge-proceeding AUTHOR-PERFORMED token with no trace. READ what is there now ($prog verdict $kind --issue $issue) and decide again; --force does not cover it, because it authorizes replacing the verdict you read, not one that arrived afterwards. The substitute written at the fresh generation is left on disk as history and nothing reads it."
    exit 2
  fi
  now_rec_obs="$(stage_record_text "$sfile" 2>/dev/null || printf '<unreadable>')"
  if [ "$now_rec_obs" != "$rec_text" ]; then
    emit "$REFUSE_MARKER reason=stage-record-changed-mid-write kind=$kind issue=$issue record=$(field_value "$sfile")"
    emit "$REFUSE_MARKER detail=the STAGE RECORD changed between the read this rewrite was derived from and this publication, so NOTHING was published. Writing the rewrite now would revert whatever landed — most likely a concurrent open --force that moved this stage to a newer generation — back to the record this process read before it. Read the record ($prog status $kind --issue $issue) and decide again. The substitute written at the fresh generation is left on disk as history and nothing reads it."
    exit 2
  fi
  commit_write "$sfile" stage-record

  emit "RECORD-OK kind=$kind issue=$issue result=AUTHOR-PERFORMED performed-by=$performed_by reason=$reason_tok evidence=$evidence_tok report-nonce=$new_nonce supersedes-report-nonce=$prior_gen${replaced:+ replaced-verdict=$replaced} report=$(field_value "$new_rpath")"
  emit "RECORD-NOTE kind=$kind issue=$issue $AUTHOR_DISCLOSURE"
  exit 0
}

usage() {
  sed -n '2,/^# ---END-HELP---$/p' "$0" | sed -e 's/^# \{0,1\}//' -e '/^---END-HELP---$/d'
}

case "${1:-}" in
  open) shift; cmd_open "$@" ;;
  status) shift; cmd_status "$@" ;;
  verdict) shift; cmd_verdict "$@" ;;
  record-author-performed) shift; cmd_record_author_performed "$@" ;;
  -h | --help | help) usage ;;
  "") die_usage "a subcommand is required: open <kind> --issue <N> --agent <type> | status <kind> --issue <N> | verdict <kind> --issue <N> | record-author-performed <kind> --issue <N> --reason <why> --evidence <artifact> --performed-by author" ;;
  *) die_usage "unknown subcommand '$1' (open | status | verdict | record-author-performed)" ;;
esac
