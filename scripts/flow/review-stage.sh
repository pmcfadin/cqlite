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
#   token             meaning                                            exit
#   PASS              a report was written recording no blocking finding   0
#   FINDINGS          a report was written recording >=1 blocking finding  4
#   NOT-RUN           sentinel-only / absent / empty / ungrammatical /     5
#                     never-opened  (ALWAYS carries a parenthesised cause)
#   AUTHOR-PERFORMED  a disclosed substitute with its working recorded     6
#
# `AUTHOR-PERFORMED` is reported ONLY when the working is actually there: the required
# disclosure verbatim, a `performed-by` of exactly `author`/`peer`, and a `reason` and an
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
# `NOT-RUN` carries one of SIX named causes, because the operator action differs per cause
# and one token for six states is the collapse this issue is about:
#   no report written          the stage is open and the report is still the sentinel
#   report absent              the stage is open and its report file is GONE
#   report unreadable          the report file exists and CANNOT BE READ (permission, I/O)
#   report empty               the report file exists and holds nothing recordable
#   report ungrammatical: <w>  a result line that is unrecognised, absent, or unsupported
#   stage never opened         no stage was ever opened for this <kind>/<issue>
#
# `report unreadable` was the SIXTH, added in round 2 (B7) rather than folded into an existing
# cause: an unreadable file is NOT empty (the operator fix is `chmod`, not the agent) and calling
# it ungrammatical would assert something about content that was never observed. Reuse would have
# been a false rationale, which is worse than none.
#
# TWO FILES, AND WHY (the never-opened / report-absent distinction needs them)
# ---------------------------------------------------------------------------
#   <dir>/<kind>.md      the REPORT OF RECORD: what the agent writes, what `verdict` reads.
#   <dir>/<kind>.stage   the STAGE RECORD: kind/issue/agent/spawned-at/deadline/report path,
#                        plus the `head-sha:` the stage was opened AT (see below).
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
# status must not be able to decide anything.
#
# SUBCOMMANDS
#   open  <kind> --issue <N> --agent <type> [--deadline-secs <S>] [--report <path>] [--force]
#         Pre-stamp the sentinel BEFORE spawning. Refuses an already-open stage without
#         --force; --force NEVER resets `spawned-at` (a second spawn silently restarting the
#         clock would make the deadline unreadable, and a re-spawn is exactly what a lane
#         does when the first agent idles). Prints the absolute path AND the paste-ready
#         clause for the spawn prompt, so the contract reaches the agent VERBATIM rather
#         than being paraphrased per lane.
#   status <kind> --issue <N>
#         Elapsed / deadline / state. ADVISORY ONLY — never changes the verdict.
#   verdict <kind> --issue <N>
#         EXACTLY ONE line of the closed grammar above. Exit 0/4/5/6.
#   record-author-performed <kind> --issue <N> --reason <why> --evidence <artifact>
#                           --performed-by author|peer [--force]
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
#   macOS bash 3.2 compatible (no associative arrays, no readarray/mapfile).
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

# one_line <text> — flatten to a single line for a diagnostic that is INTERPOLATED into an
# emitted line. Unlike sanitize_field this preserves spaces and punctuation (a cause is
# prose a human reads), and only guarantees the one property the grammar needs: no control
# character can break the one-line contract. Reserved characters of the emitted grammar
# ('(' / ')') are not stripped — the cause is already inside parentheses and a reader takes
# the LAST ')' — but a newline would produce a second line, which is the property that
# matters.
one_line() {
  printf '%s' "${1:-}" | LC_ALL=C tr -d '\000' | LC_ALL=C tr '\n\r\t' '   ' | LC_ALL=C sed -e 's/  */ /g' -e 's/^ //' -e 's/ $//'
}

# field_value <text> — THE ONE EMIT BOUNDARY for a DATA value interpolated into one of this
# tool's `key=value` control lines (#3312's rule; #3751 round 2, S1). Flattens to one line and
# maps the ONE reserved character '=' to '~'.
#
# WHY IT IS ONE FUNCTION AND NOT A RULE PER SITE: the `cause` and the `report=` path are both
# DATA on a line whose other fields a consumer scans, and both are influenced by a party this
# tool is judging — the cause partly by the report's own text, the path by `--report`. A path
# like `a=b elapsed=999.md` is a LEGAL filename, so it cannot be refused (refusing would red
# correct input); left raw it put a SECOND `elapsed=` pair on the line, and the comment above
# the cause claimed "ONE emit boundary" while the neighbouring field had none. Neutralised
# rather than refused, because both values are diagnostics an operator has to read.
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
  case "$pb" in
    author | peer) ;;
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
    not-in-set)    printf "with performed-by '%s', which is not 'author' or 'peer'\n" "$tok" ;;
    unsubstituted) printf 'whose %s still carries an UNSUBSTITUTED placeholder\n' "$field" ;;
    unrecordable)  printf "whose %s records as '%s' — fewer than 3 recordable characters\n" "$field" "$tok" ;;
    placeholder)   printf "whose %s is the PLACEHOLDER '%s'\n" "$field" "$tok" ;;
    *)             printf 'with an unusable %s\n' "$field" ;;
  esac
}

# validate_kind <kind> — a stage kind names a FILE, so it is validated rather than trusted:
# `[A-Za-z0-9][A-Za-z0-9._-]*`, which admits `c`, `rust-review`, `coverage`, and refuses
# every path-traversal and shell-metacharacter shape. Refused, never sanitized: a kind is
# also how a caller ASKS for a stage, so silently rewriting it would make `open c/../x` and
# `open c-x` the same stage under two spellings.
validate_kind() {
  local k="${1:-}"
  case "$k" in
    "" ) die_usage "a <kind> is required (e.g. c, rust-review, coverage)" ;;
    *[!A-Za-z0-9._-]* ) die_usage "invalid <kind> '$k': allowed characters are [A-Za-z0-9._-]" ;;
    [!A-Za-z0-9]* ) die_usage "invalid <kind> '$k': must start with a letter or digit" ;;
  esac
  printf '%s\n' "$k"
}

validate_issue() {
  local n="${1:-}"
  case "$n" in
    "" ) die_usage "--issue <N> is required" ;;
    *[!0-9]* | 0*[!0-9]* ) die_usage "--issue must be a decimal issue number, got '$n'" ;;
  esac
  printf '%s\n' "$n"
}

validate_secs() {
  local s="${1:-}" flag="${2:---deadline-secs}"
  case "$s" in
    "" | *[!0-9]* ) die_usage "$flag must be a non-negative integer number of seconds, got '$s'" ;;
  esac
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

# abs_path <path> — absolutise WITHOUT requiring the file to exist (the report is often the
# file we are about to create). Relative values resolve against $PWD, which is ordinary CLI
# semantics; the DEFAULT path is built from the repo root instead, so it does not move with
# the caller's cwd.
abs_path() {
  local p="$1" dir base
  case "$p" in
    /*) ;;
    *) p="$PWD/$p" ;;
  esac
  dir="$(dirname "$p")"
  base="$(basename "$p")"
  if [ -d "$dir" ]; then
    printf '%s/%s\n' "$(cd "$dir" && pwd)" "$base"
  else
    printf '%s\n' "$p"
  fi
}

stage_dir()  { printf '%s/.review-stage/issue-%s\n' "$(repo_root)" "$1"; }
stage_file() { printf '%s/%s.stage\n' "$(stage_dir "$1")" "$2"; }
default_report() { printf '%s/%s.md\n' "$(stage_dir "$1")" "$2"; }

# assert_ignored <path> <what> — FAIL-CLOSED gitignore verification (see the header). Asks
# git; refuses on anything that is not an affirmative "yes, ignored". `check-ignore -q` exits
# 0 = ignored, 1 = NOT ignored, 128 = error (e.g. the path is outside the repository), and
# every non-zero answer takes the SAME refusing branch: "cannot tell" is not "fine".
assert_ignored() {
  local path="$1" what="$2" extra="${3:-}" rc=0
  git check-ignore -q -- "$path" || rc=$?
  if [ "$rc" -ne 0 ]; then
    emit "$REFUSE_MARKER reason=path-not-gitignored what=$what path=$path check-ignore-rc=$rc"
    emit "$REFUSE_MARKER detail=git does not confirm this path is ignored, and this tool writes it MID-RUN — an untracked-but-not-ignored write dirties a running gate of record (tree-integrity FAIL, #2926) and makes premerge-assert refuse on dirty: yes (#3648). Add the path to .gitignore (the default location .review-stage/ already is), or pass a --report path that is."
    # An optional caller-supplied line, printed only on the refusal path: a refused TEMPORARY
    # path is confusing without it, because the caller never named that path.
    [ -z "$extra" ] || emit "$REFUSE_MARKER detail=$extra"
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
      emit "$REFUSE_MARKER reason=path-unverifiable what=$what path=$path component=$parent"
      emit "$REFUSE_MARKER detail=this directory is not searchable, so whether the next component is a SYMLINK cannot be determined — and a write that follows a link lands outside the verified-gitignored path (#2926/#3648). Refusing rather than guessing: cannot-tell must not take the permissive branch."
      exit 2
    fi
    parent="$cur"
    cur="$cur/$comp"
    if [ -L "$cur" ]; then
      emit "$REFUSE_MARKER reason=path-is-symlink what=$what path=$path component=$cur"
      emit "$REFUSE_MARKER detail=git check-ignore verifies a LEXICAL path but a WRITE follows symlinks, so this write would land wherever the link points — possibly a TRACKED file or a path outside the repository — dirtying a running gate of record (tree-integrity FAIL, #2926) and making premerge-assert refuse on dirty: yes (#3648). Remove the link and let this tool create a regular file, or pass a --report path that is one."
      exit 2
    fi
    if [ -e "$cur" ] && [ ! -d "$cur" ] && [ "$cur" != "$path" ]; then
      emit "$REFUSE_MARKER reason=path-component-not-a-directory what=$what path=$path component=$cur"
      emit "$REFUSE_MARKER detail=an intermediate path component exists and is not a directory, so nothing can be written under it."
      exit 2
    fi
  done
  if [ -e "$cur" ] && [ ! -f "$cur" ]; then
    emit "$REFUSE_MARKER reason=path-not-a-regular-file what=$what path=$path"
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
    # named this path. Consequence worth knowing: a --report in a directory ignored only by
    # EXTENSION (`*.md`) is refused, since the temp name is not matched by that pattern and WOULD
    # dirty a running gate. `.review-stage/` — the default and the only path the pipeline uses —
    # is ignored as a DIRECTORY, so this never fires there.
    assert_ignored "$cand" "$what-tempfile" \
      "this is the TEMPORARY file the write goes through (an unpredictable same-directory temp, created O_EXCL and written through a held descriptor, plus an atomic mv -f, so no path is re-resolved between validation and writing and no reader sees a half-written result: line). It is a real file in the tree for the duration of the write, so it is held to the same bar as the destination. A --report directory ignored only by EXTENSION does not match it: ignore the DIRECTORY instead, as .review-stage/ is."
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
  emit "$REFUSE_MARKER reason=tempfile-not-created what=$what path=$dest attempts=$attempt"
  emit "$REFUSE_MARKER detail=an unpredictable temporary file could not be created EXCLUSIVELY beside this path in $attempt attempt(s), so NOTHING was written. Either the directory is not writable, or mktemp is unavailable. There is deliberately no fallback to a predictable name: that is the TOCTOU this write path exists to remove (a peer lane can plant a symlink at a guessable temp name), so refusing is the fail-closed answer."
  exit 2
}
commit_write() {
  local dest="$1" what="$2"
  # THE DESCRIPTOR IS CLOSED BEFORE THE RENAME, so the record is complete on disk and the fd is
  # not carried into the next write (the number is reused for both files a stage writes).
  exec 9>&- 2>/dev/null || true
  if ! mv -f "$WRITE_TMP" "$dest" 2>/dev/null; then
    rm -f "$WRITE_TMP" 2>/dev/null || true
    WRITE_TMP=""
    emit "$REFUSE_MARKER reason=write-failed what=$what path=$dest"
    emit "$REFUSE_MARKER detail=the record was written to a temporary file but could not be moved into place, so NOTHING was recorded. The temporary file has been removed; an unexplained leftover would be indistinguishable from a crashed write."
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

now_epoch() { date -u +%s; }
now_iso()   { date -u +%Y-%m-%dT%H:%M:%SZ; }

# --- open --------------------------------------------------------------------
cmd_open() {
  require_repo_root
  REFUSE_MARKER="OPEN-REFUSED"
  local kind="" issue="" agent="" deadline="$DEFAULT_DEADLINE_SECS" report="" force=0
  kind="$(validate_kind "${1:-}")"; shift || true
  while [ $# -gt 0 ]; do
    case "$1" in
      --issue) shift; issue="${1:-}" ;;
      --agent) shift; agent="${1:-}" ;;
      --deadline-secs) shift; deadline="${1:-}" ;;
      --report) shift; report="${1:-}" ;;
      --force) force=1 ;;
      *) die_usage "open: unknown argument '$1'" ;;
    esac
    shift || true
  done
  issue="$(validate_issue "$issue")"
  [ -n "$agent" ] || die_usage "open: --agent <type> is required (the agent whose silence this stage measures)"
  agent="$(reject_placeholder "open: --agent" "$agent" "spec-auditor")"
  deadline="$(validate_secs "$deadline" --deadline-secs)"

  local sfile rpath dir
  sfile="$(stage_file "$issue" "$kind")"
  if [ -n "$report" ]; then rpath="$(abs_path "$report")"; else rpath="$(default_report "$issue" "$kind")"; fi

  # BOTH files are verified ignored BEFORE anything is created — including the stage record,
  # which lives under .review-stage/ whatever --report says. Checking only the report would
  # leave the other write dirtying a running gate.
  #
  # THE SYMLINK WALK RUNS FIRST, BEFORE THE `mkdir -p`: a component that is a dangling symlink
  # makes `mkdir -p` fail with "File exists", i.e. an unnamed exit 1 under `set -e` instead of a
  # named refusal — and a component that is a LIVE symlink would have the directory created
  # somewhere else entirely.
  assert_no_symlink "$sfile" stage-record
  dir="$(dirname "$sfile")"; mkdir -p "$dir"
  assert_ignored "$sfile" stage-record
  # The report's PARENT must exist for check-ignore to answer about a path, and for the
  # write to land; creating it is safe because the directory itself is under a verified
  # path or under the caller's chosen tree.
  assert_no_symlink "$rpath" report-of-record
  mkdir -p "$(dirname "$rpath")"
  assert_ignored "$rpath" report-of-record

  local spawned_iso spawned_epoch reopen_count=0 prior_iso="" head_sha=""
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
    if [ "$force" -ne 1 ]; then
      emit "$REFUSE_MARKER reason=already-open kind=$kind issue=$issue spawned-at=${prior_iso:-unknown} report=$(field_value "$(read_field "$sfile" report)")"
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
      case "$prior_epoch" in
        "" | *[!0-9]* ) note "the existing stage record has no readable spawned-epoch; the clock restarts from now" ;;
        *) spawned_epoch="$prior_epoch" ;;
      esac
    fi
    local prior_count
    prior_count="$(read_field "$sfile" reopen-count)"
    case "$prior_count" in
      "" | *[!0-9]* ) reopen_count=1 ;;
      *) reopen_count=$((prior_count + 1)) ;;
    esac
  fi

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
    printf 'report: %s\n' "$rpath"
    # RE-STAMPED ON EVERY OPEN, INCLUDING --force — deliberately unlike `spawned-at` above. A
    # forced re-open re-writes the sentinel, so the re-spawned agent audits the tree that is
    # there NOW; carrying an older sha forward would bind the verdict to a tree nobody read.
    printf 'head-sha: %s\n' "$head_sha"
    printf 'reopen-count: %s\n' "$reopen_count"
    [ "$reopen_count" -eq 0 ] || printf 'reopened-at: %s\n' "$(now_iso)"
  } >&9
  commit_write "$sfile" stage-record

  emit "OPEN-OK kind=$kind issue=$issue agent=$agent deadline-secs=$deadline spawned-at=$spawned_iso head-sha=$head_sha reopen-count=$reopen_count report=$(field_value "$rpath")"
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
--- end clause ---
CLAUSE
}

# --- verdict machinery -------------------------------------------------------
# classify_report <report-path> <stage-open:0|1> — print "<token>|<cause>" and return 0.
# ONE place decides the token, so `status` and `verdict` can never form two opinions about
# the same file (the divergence #3564 records one directory over).
classify_report() {
  local rpath="$1" open="$2" line value tok cause body defect

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
STAGE_OPEN=0; STAGE_AGENT=unknown; STAGE_DEADLINE=unknown; STAGE_REPORT=""
STAGE_SPAWNED_ISO=unknown; STAGE_ELAPSED=unknown
load_stage() {
  local issue="$1" kind="$2" sfile epoch
  sfile="$(stage_file "$issue" "$kind")"
  STAGE_REPORT="$(default_report "$issue" "$kind")"
  [ -f "$sfile" ] || return 0
  STAGE_OPEN=1
  local v
  v="$(read_field "$sfile" agent)";         [ -z "$v" ] || STAGE_AGENT="$v"
  v="$(read_field "$sfile" deadline-secs)"; [ -z "$v" ] || STAGE_DEADLINE="$v"
  v="$(read_field "$sfile" spawned-at)";    [ -z "$v" ] || STAGE_SPAWNED_ISO="$v"
  v="$(read_field "$sfile" report)";        [ -z "$v" ] || STAGE_REPORT="$v"
  epoch="$(read_field "$sfile" spawned-epoch)"
  case "$epoch" in
    "" | *[!0-9]* ) STAGE_ELAPSED=unknown ;;
    *) STAGE_ELAPSED=$(( $(now_epoch) - epoch )); [ "$STAGE_ELAPSED" -ge 0 ] || STAGE_ELAPSED=0 ;;
  esac
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
  cls="$(classify_report "$STAGE_REPORT" "$STAGE_OPEN")"
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
  # consumer greps, and a second line is a second opinion. BOTH data values on it — the cause
  # and the caller-influenced report path — go through `field_value`, the one emit boundary.
  emit "$KI_KIND RESULT: $rendered elapsed=$STAGE_ELAPSED deadline=$STAGE_DEADLINE agent=$STAGE_AGENT report=$(field_value "$STAGE_REPORT")"
  case "$token" in
    PASS) exit 0 ;;
    FINDINGS) exit 4 ;;
    NOT-RUN) exit 5 ;;
    AUTHOR-PERFORMED) exit 6 ;;
    *) note "unreachable: unclassified token '$token'"; exit 5 ;;
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
  local cls token cause state past=unknown
  cls="$(classify_report "$STAGE_REPORT" "$STAGE_OPEN")"
  token="${cls%%|*}"
  cause="${cls#*|}"
  case "$token" in
    NOT-RUN)
      case "$cause" in
        "no report written") state=sentinel-only ;;
        "report absent") state=report-absent ;;
        "report empty") state=report-empty ;;
        "stage never opened") state=never-opened ;;
        *) state=report-ungrammatical ;;
      esac
      ;;
    *) state=reported ;;
  esac
  case "$STAGE_ELAPSED:$STAGE_DEADLINE" in
    unknown:* | *:unknown) past=unknown ;;
    *) if [ "$STAGE_ELAPSED" -gt "$STAGE_DEADLINE" ]; then past=yes; else past=no; fi ;;
  esac

  emit "STATUS kind=$KI_KIND issue=$KI_ISSUE state=$state elapsed=$STAGE_ELAPSED deadline=$STAGE_DEADLINE past-deadline=$past agent=$STAGE_AGENT spawned-at=$STAGE_SPAWNED_ISO report=$(field_value "$STAGE_REPORT")"
  if [ "$state" = sentinel-only ] && [ "$past" = yes ]; then
    # A STAGE THAT IS WAITING MUST NOT LOOK LIKE ONE THAT IS HUNG (the gate's
    # `waiting for gate slot` idiom): name the elapsed time AND the fact that nothing was
    # produced, so the operator does not have to infer either.
    emit "STATUS-NOTE kind=$KI_KIND issue=$KI_ISSUE PAST DEADLINE: ${STAGE_ELAPSED}s elapsed against a ${STAGE_DEADLINE}s deadline and NOTHING has been produced — the report is still the pre-spawn sentinel. This is ADVISORY: the deadline never changes the verdict, and a report arriving later is still a report. Read the verdict with: $prog verdict $KI_KIND --issue $KI_ISSUE"
  elif [ "$state" = sentinel-only ]; then
    emit "STATUS-NOTE kind=$KI_KIND issue=$KI_ISSUE inside deadline: ${STAGE_ELAPSED}s of ${STAGE_DEADLINE}s elapsed and nothing produced yet — the report is still the pre-spawn sentinel, which is NOT a verdict."
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
        die_usage "record-author-performed: --performed-by author|peer is required — peer-C is preferred and self-C is the sanctioned fallback, so which one happened is the whole disclosure" ;;
      performed-by:not-in-set)
        die_usage "record-author-performed: --performed-by must be exactly 'author' or 'peer', got '$performed_by'" ;;
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
  local prior_cls prior_token replaced=""
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
  "") die_usage "a subcommand is required: open <kind> --issue <N> --agent <type> | status <kind> --issue <N> | verdict <kind> --issue <N> | record-author-performed <kind> --issue <N> --reason <why> --evidence <artifact> --performed-by author|peer" ;;
  *) die_usage "unknown subcommand '$1' (open | status | verdict | record-author-performed)" ;;
esac
