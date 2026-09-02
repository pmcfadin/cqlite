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
# `NOT-RUN` carries one of SEVEN named causes, because the operator action differs per cause
# and one token for seven states is the collapse this issue is about:
#   no report written          the stage is open and the report is still the sentinel
#   report absent              the stage is open and its report file is GONE
#   report unreadable          the report file exists and CANNOT BE READ (permission, I/O)
#   report empty               the report file exists and holds nothing recordable
#   report ungrammatical: <w>  a result line that is unrecognised, absent, or unsupported
#   stage never opened         no stage was ever opened for this <kind>/<issue>
#   stage record unreadable: <w>  the RECORD does not name which report is current, so no report
#                                 was identified and nothing is claimed about one (round 5, J1)
#
# `report unreadable` was the SIXTH, added in round 2 (B7) rather than folded into an existing
# cause: an unreadable file is NOT empty (the operator fix is `chmod`, not the agent) and calling
# it ungrammatical would assert something about content that was never observed. Reuse would have
# been a false rationale, which is worse than none.
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
#         REFUSES to overwrite a report that already RECORDS a verdict (`PASS`/`FINDINGS`)
#         without `--force`, and a forced replacement RECORDS the token it replaced
#         (`replaced-verdict:`) in the new report and on the RECORD-OK line — an overwrite
#         that leaves no trace turns a recorded refusal into a proceed at the merge point,
#         which is the audit-trail failure this whole tool exists to remove. A
#         sentinel-only report is freely replaceable: that is the normal path.
#         THAT CHECK PREVENTS RATHER THAN REPORTS (#3751 round 9, N1): the observation it
#         decides on is RE-TAKEN immediately before the atomic rename, and any change refuses
#         (`reason=report-changed-mid-write`) — under `--force` too, since `--force`
#         authorizes replacing the verdict the operator READ, never one that arrives while the
#         substitute is being prepared. The irreducible residual is one `mv` wide and is
#         declared at the check; see `report_bytes`.
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
#
# ---END-HELP---
set -euo pipefail

prog="$(basename "$0")"

die_usage() { echo "$prog: $*" >&2; exit 64; }
note()      { echo "[review-stage] $*" >&2; }
emit()      { echo "REVIEW-STAGE: $*"; }

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
trap cleanup_write_tmp EXIT
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
  WRITE_TMP=""
}

# read_field <file> <key> — the FIRST `<key>: <value>` line's value, flattened to one line.
# Empty output means "absent or empty", which every caller treats as unmeasured.
read_field() {
  local file="$1" key="$2" line
  [ -f "$file" ] || return 0
  line="$(LC_ALL=C grep -m1 -i "^[[:space:]]*${key}:" "$file" 2>/dev/null || true)"
  [ -n "$line" ] || return 0
  line="${line#*:}"
  one_line "$line"
}

# count_field_lines <file> <key> — HOW MANY TIMES <key> APPEARS, AS AN AFFIRMATIVE MEASUREMENT.
# Prints the count and returns 0 ONLY when the file was actually READ; returns 1, printing
# nothing, when the read FAILED. Every caller must branch on that status.
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
  local file="$1" key="$2" out="" rc=0
  out="$(LC_ALL=C grep -c -i "^[[:space:]]*${key}:" "$file" 2>/dev/null)" || rc=$?
  case "$rc" in
    0 | 1) ;;
    *) return 1 ;;
  esac
  case "$out" in
    "" | *[!0-9]* ) return 1 ;;
  esac
  printf '%s\n' "$out"
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
    if ! nnonce_lines="$(count_field_lines "$sfile" report-nonce)"; then
      emit "$REFUSE_MARKER reason=stage-record-unreadable kind=$kind issue=$issue record=$(field_value "$sfile")"
      emit "$REFUSE_MARKER detail=this stage's record EXISTS and could not be READ, so which report of this stage is current could not be measured and NOTHING was written. That is not the same as a record with no report-nonce (which reads as the original single report): an unmeasured record may not take the permissive reading, because it is also a record whose spawned-at cannot be read, so a forced re-open would silently restart a clock a reader is using. Fix the record's permissions, or remove the stage directory and open a fresh stage."
      exit 2
    fi
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
  # THE EXISTENCE BELT IS DELETED WITH IT, and the property it was for is now structural: a
  # record deleted by hand while its report survives no longer restarts a counter at 0, because
  # there is no counter — a fresh open picks a value nothing on disk can predict.
  #
  # `reopen-count` REMAINS, and it is where the human-readable audit number lives. It answers a
  # DIFFERENT question from the nonce (how many times this stage was spawned, versus which report
  # is current), and it is what an operator reads beside `reopened-at:` to correlate a surviving
  # report with a re-spawn; the nonce only has to be UNIQUE.
  nonce="$(new_report_nonce "$dir")"
  if ! nonce_is_valid "$nonce"; then
    # NO FALLBACK, for the reason `prepare_write` has none: every predictable substitute (a pid, a
    # timestamp, a counter) is exactly the collidable value this replaces, so a box that cannot
    # generate an unpredictable token is REFUSED rather than given a weaker one it cannot see.
    emit "$REFUSE_MARKER reason=report-nonce-not-generated kind=$kind issue=$issue value=$(field_value "${nonce:-<none>}")"
    emit "$REFUSE_MARKER detail=an unpredictable report nonce could not be generated, so NO report path was derived and NOTHING was written. The nonce comes from mktemp -u's name substitution, so this box has no usable mktemp. There is deliberately no fallback to a predictable token (a pid, a timestamp, a counter): a token two concurrent opens could both choose is the collision this nonce exists to remove, so refusing is the fail-closed answer."
    exit 2
  fi
  rpath="$(report_path "$issue" "$kind" "$nonce")"

  # THE REPORT HALF OF THE PATH VERIFICATION, on the path that was just chosen. Its PARENT is the
  # SAME directory as the stage record's, created above, and it is
  # `<repo-root>/.review-stage/issue-<N>` BY DERIVATION — so this `mkdir` cannot create anything
  # outside the checkout whatever the caller passed (#3751 round 4, H3: it once could, because the
  # caller supplied the path and the containment check came AFTER this line).
  assert_no_symlink "$rpath" report-of-record
  mkdir -p "$(dirname "$rpath")"
  assert_ignored "$rpath" report-of-record

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
# cheaper to be right about — `classify_report` reads its subject five times, so it is not an
# identity of any single instant.
#
# THE STATE MARKER EXISTS SO THAT "ABSENT" AND "EMPTY" ARE DIFFERENT OBSERVATIONS. Both are the
# empty string once read, and they are not the same fact.
#
# THE COMPLETE READ IS ASSERTED AFFIRMATIVELY, not inferred from `cat` having exited 0 inside a
# substitution whose status is easy to lose: the sentinel `E` is printed by a SECOND command
# joined with `&&`, so a truncated or failed read cannot produce a value ending in it. A positive
# verdict requires an affirmative measurement, and "the bytes are unchanged" is a positive verdict.
#
# DECLARED LIMIT: bash DISCARDS NUL bytes in a command substitution, so a change consisting only
# of NUL bytes is not represented here. The report of record is text written by an agent (and by
# this script), the classification below is compared alongside this value at its one call site,
# and nothing in this tool writes a NUL — stated rather than left as an unexamined blind spot.
report_bytes() {
  local p="$1" body
  if [ ! -f "$p" ]; then printf 'state=no-such-file\n'; return 0; fi
  # Measured BY ATTEMPTING THE READ rather than with `[ -r ]`, which answers TRUE for root and
  # cannot see an I/O error — the same reason `classify_report` probes with a redirection.
  body="$( { LC_ALL=C cat -- "$p" && printf 'E'; } 2>/dev/null )"
  case "$body" in
    *E) ;;
    *) printf 'state=unreadable\n'; return 0 ;;
  esac
  printf 'state=present bytes:\n%s' "${body%E}"
}

# classify_report <report-path> <stage-open:0|1> — print "<token>|<cause>" and return 0.
# ONE place decides the token, so `status` and `verdict` can never form two opinions about
# the same file (the divergence #3564 records one directory over).
classify_report() {
  local rpath="$1" open="$2" record_defect="${3:-}" line value tok cause body defect

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
  if [ ! -f "$rpath" ]; then
    printf 'NOT-RUN|report absent\n'; return 0
  fi
  # UNREADABLE IS ITS OWN CAUSE, AND IS ASKED BEFORE THE CONTENT (#3751 round 2, B7). The cause
  # list exists because THE OPERATOR ACTION DIFFERS PER CAUSE, and an unreadable report used to
  # be reported as `report empty` — which sends the operator to the AGENT when the fix is
  # `chmod`. Reusing `report ungrammatical` instead would be no better: it asserts something
  # about CONTENT THAT WAS NEVER OBSERVED, and a false rationale is worse than none, because it
  # is what stops the next person looking. Measured BY ATTEMPTING THE OPEN rather than with
  # `[ -r ]`, which answers TRUE for root and cannot see an I/O error; the redirection error is
  # bash's own, so it is suppressed inside the subshell rather than on `tr` (a raw shell error
  # beside the verdict line is not a named refusal).
  if ! ( : <"$rpath" ) 2>/dev/null; then
    printf 'NOT-RUN|report unreadable\n'; return 0
  fi
  # "empty" means nothing RECORDABLE — a file of blank lines is empty in every sense a
  # reader cares about, and reporting `report ungrammatical` for it would name the wrong
  # operator action. The redirection is grouped so a read that fails BETWEEN the probe above
  # and here (a race, a revoked mode) still cannot leak bash's error into the caller's stderr.
  body="$( { LC_ALL=C tr -d '[:space:]' <"$rpath"; } 2>/dev/null || true )"
  if [ -z "$body" ]; then
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
  cands="$( { LC_ALL=C grep -i '^result:' "$rpath"; } 2>/dev/null || true)"
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
      if ! LC_ALL=C grep -qF -- "$AUTHOR_DISCLOSURE" "$rpath"; then
        printf 'NOT-RUN|report ungrammatical: AUTHOR-PERFORMED without the required disclosure\n'; return 0
      fi
      # THE WORKING IS JUDGED BY THE SAME FUNCTION THE WRITER USES (#3751 round 1, F3).
      # A NON-EMPTINESS test standing in for a validity test is the shape this repo pins:
      # `performed-by: nobody`, `reason: x`, `evidence: tbd` are all non-empty and all
      # unusable, and each one reached the token that PROCEEDS at the merge point while
      # `record-author-performed` would have refused it. The cause NAMES the field and the
      # defect, because the operator action differs per field.
      defect="$(author_working_defect \
        "$(read_field "$rpath" performed-by)" \
        "$(read_field "$rpath" reason)" \
        "$(read_field "$rpath" evidence)")"
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
  local nnonce nval
  # THE READ IS VERIFIED AFFIRMATIVELY, AND A FAILED READ IS ITS OWN DEFECT (#3751 round 6, K1).
  # `count_field_lines` returns non-zero only when the record could not be READ; "read fine, no
  # such field" prints 0 and returns 0. The two were collapsed by a `|| true`, so an unreadable
  # record fell through to the LEGACY reading and an OLD report's `PASS` was reported as the
  # current verdict. The legacy reading below is reserved for a record that WAS read.
  if ! nnonce="$(count_field_lines "$sfile" report-nonce)"; then
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
  # AND IT MUST STAY LAST ON THIS LINE (#3751 round 11, Q3). A report path may legitimately contain
  # a SPACE (a checkout at `/tmp/work tree`), so that consumer reads `report=` as the REMAINDER of
  # the line rather than as one whitespace-delimited field — a field read truncated the value and
  # REFUSED a correct verdict. Appending a field after `report=` would silently truncate every such
  # path again; the property is pinned against THIS emitter by section 44l of
  # scripts/tests/test_premerge_assert.sh (its 11 states derived by RUNNING this script), so such a
  # change reds that suite rather than shipping.
  emit "$KI_KIND RESULT: $rendered elapsed=$STAGE_ELAPSED deadline=$(field_value "$STAGE_DEADLINE") agent=$(field_value "$STAGE_AGENT") report=$(field_value "${STAGE_REPORT:-unresolved}")"
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
  # THE OBSERVATION IS TAKEN BEFORE THE DECISION IS MADE ON IT (#3751 round 9, N1), and in that
  # ORDER deliberately: `classify_report` re-reads the file, so a change landing between these two
  # calls is one the classification would see and this snapshot would not. Taking the bytes FIRST
  # means every content change from the EARLIEST observation onward is caught by the
  # re-verification below, whichever of the two reads saw it.
  local prior_cls prior_token prior_obs replaced=""
  prior_obs="$(report_bytes "$STAGE_REPORT")"
  prior_cls="$(classify_report "$STAGE_REPORT" 1)"
  prior_token="${prior_cls%%|*}"
  case "$prior_token" in
    PASS | FINDINGS)
      if [ "$force" -ne 1 ]; then
        emit "AUTHOR-REFUSED reason=verdict-already-recorded kind=$kind issue=$issue recorded-verdict=$prior_token report=$(field_value "$STAGE_REPORT")"
        emit "AUTHOR-REFUSED detail=this stage already RECORDS a verdict, and replacing it here would destroy it with no trace — a recorded FINDINGS would become a merge-proceeding AUTHOR-PERFORMED. Read it first ($prog verdict $kind --issue $issue). If the substitute really does supersede it, pass --force: the replaced token is then RECORDED in the new report."
        exit 2
      fi
      replaced="$prior_token"
      note "--force: REPLACING a recorded $prior_token verdict with AUTHOR-PERFORMED; the replaced token is recorded in the report"
      ;;
  esac

  assert_no_symlink "$STAGE_REPORT" report-of-record
  assert_ignored "$STAGE_REPORT" report-of-record

  prepare_write "$STAGE_REPORT" report-of-record
  {
    printf '# review stage: %s — issue #%s (AUTHOR-PERFORMED substitute)\n' "$kind" "$issue"
    printf '\n'
    printf 'result: AUTHOR-PERFORMED\n'
    printf '\n'
    # THE TRACE. Emitted only when something was actually replaced, so its ABSENCE is not a
    # claim: a normal recording over the sentinel says nothing about a replacement.
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
  } >&9
  # RE-VERIFIED IMMEDIATELY BEFORE THE RENAME (#3751 round 9, N1). The check above REPORTED the
  # verdict it found and then spent a `mktemp`, an `O_EXCL` create, a `date` and a dozen `printf`s
  # before installing its replacement — so a late reviewer recording FINDINGS anywhere in that
  # span was silently overwritten by the merge-proceeding AUTHOR-PERFORMED token, with no
  # `--force` and no `replaced-verdict:` trace. A check placed before the act it guards, with a
  # window in between, can only REPORT; the control has to be that the bad state cannot be
  # REACHED. So the observation the decision was made on is re-taken HERE, after the substitute is
  # fully written to the temporary file and before anything is installed at the destination.
  #
  # ONE RULE, NOT A MATRIX: the report must be BYTE-IDENTICAL to the observation this call
  # decided on. Any change at all refuses — including under `--force`, because `--force`
  # authorizes replacing the verdict the operator READ, and a different verdict arriving
  # afterwards was never authorized by anyone.
  #
  # RESIDUAL WINDOW, DECLARED BECAUSE IT CANNOT BE REMOVED: the rename itself is not conditional.
  # There is no compare-and-swap rename reachable from a shell — coreutils `mv` exposes neither
  # `RENAME_EXCHANGE` nor `RENAME_NOREPLACE`, and `mv -n` is the wrong predicate (it refuses ANY
  # existing destination, and the destination here legitimately exists — it is the sentinel). So
  # what remains open is the span between this read and the `rename(2)` inside the single `mv`
  # below: one fork/exec, with nothing else in between, and it is the minimum this language can
  # express. A declared narrow window is acceptable; a silent one is not. Note also that a LOCK
  # would not help even if one were free: the counterparty is an ARBITRARY AGENT writing the
  # report with its own tooling and taking no lock, so only a unilateral compare-and-swap could
  # close it, which is exactly what is unavailable.
  local now_obs now_cls
  now_obs="$(report_bytes "$STAGE_REPORT")"
  if [ "$now_obs" != "$prior_obs" ]; then
    # The classification is re-read HERE, on the refusal path ONLY: it is a DIAGNOSTIC naming
    # what arrived, never an input to the decision, which was made on the byte comparison above.
    # Keeping it off the success path is also what keeps the window minimal.
    now_cls="$(classify_report "$STAGE_REPORT" 1)"
    emit "$REFUSE_MARKER reason=report-changed-mid-write kind=$kind issue=$issue report=$(field_value "$STAGE_REPORT") now-verdict=$(field_value "${now_cls%%|*}")"
    emit "$REFUSE_MARKER detail=the report of record CHANGED between the already-recorded check and this write, so NOTHING was installed — the prepared substitute is discarded and whatever is in the report now is intact. This is the interleaving that guard exists to stop: a review landing a verdict while a substitute was being prepared would otherwise be replaced by the merge-proceeding AUTHOR-PERFORMED token with no trace. READ what is there now ($prog verdict $kind --issue $issue) and decide again; --force does not cover it, because it authorizes replacing the verdict you read, not one that arrived afterwards."
    exit 2
  fi
  commit_write "$STAGE_REPORT" report-of-record

  emit "RECORD-OK kind=$kind issue=$issue result=AUTHOR-PERFORMED performed-by=$performed_by reason=$reason_tok evidence=$evidence_tok${replaced:+ replaced-verdict=$replaced} report=$(field_value "$STAGE_REPORT")"
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
