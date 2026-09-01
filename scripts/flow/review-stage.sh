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
# `NOT-RUN` carries one of FIVE named causes, because the operator action differs per cause
# and one token for five states is the collapse this issue is about:
#   no report written          the stage is open and the report is still the sentinel
#   report absent              the stage is open and its report file is GONE
#   report empty               the report file exists and holds nothing recordable
#   report ungrammatical: <w>  a result line that is unrecognised, absent, or unsupported
#   stage never opened         no stage was ever opened for this <kind>/<issue>
#
# TWO FILES, AND WHY (the never-opened / report-absent distinction needs them)
# ---------------------------------------------------------------------------
#   <dir>/<kind>.md      the REPORT OF RECORD: what the agent writes, what `verdict` reads.
#   <dir>/<kind>.stage   the STAGE RECORD: kind/issue/agent/spawned-at/deadline/report path.
# A single file cannot tell `stage never opened` from `report absent` — deleting it erases
# the evidence that anything was ever opened, and `verdict` still has to report an agent, a
# deadline and an elapsed time for a stage whose report has gone missing. So the two facts
# live in two files: the stage record is the proof the stage EXISTS, the report is the
# proof of what it CONCLUDED. Both are under `.review-stage/` and both are gitignored.
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
# link is a NAMED refusal rather than something to resolve. Both writes then go through a
# same-directory temporary file plus an atomic `mv -f`: `mv` replaces the destination NAME instead
# of opening it, and no concurrent reader (`premerge-assert.sh` at the merge point) can observe a
# half-written `result:` line.
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
#                           --performed-by author|peer
#         The sanctioned FALLBACK, never recorded as independent. Requires the WORKING:
#         a substantive reason, a named evidence artifact, and who performed it.
#         Placeholders are refused exactly as `claim.sh --reason` refuses them — by the same
#         function `verdict` classifies a HAND-WRITTEN report with, so the two sides cannot
#         hold the same value to two different strengths.
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
# same reason the five NOT-RUN causes are named separately.
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
repo_root() {
  git rev-parse --show-toplevel 2>/dev/null || die_usage "not inside a git worktree (this tool writes into the lane's worktree on purpose — see the header)"
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
    emit "OPEN-REFUSED reason=path-not-gitignored what=$what path=$path check-ignore-rc=$rc"
    emit "OPEN-REFUSED detail=git does not confirm this path is ignored, and this tool writes it MID-RUN — an untracked-but-not-ignored write dirties a running gate of record (tree-integrity FAIL, #2926) and makes premerge-assert refuse on dirty: yes (#3648). Add the path to .gitignore (the default location .review-stage/ already is), or pass a --report path that is."
    # An optional caller-supplied line, printed only on the refusal path: a refused TEMPORARY
    # path is confusing without it, because the caller never named that path.
    [ -z "$extra" ] || emit "OPEN-REFUSED detail=$extra"
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
      emit "OPEN-REFUSED reason=path-unverifiable what=$what path=$path component=$parent"
      emit "OPEN-REFUSED detail=this directory is not searchable, so whether the next component is a SYMLINK cannot be determined — and a write that follows a link lands outside the verified-gitignored path (#2926/#3648). Refusing rather than guessing: cannot-tell must not take the permissive branch."
      exit 2
    fi
    parent="$cur"
    cur="$cur/$comp"
    if [ -L "$cur" ]; then
      emit "OPEN-REFUSED reason=path-is-symlink what=$what path=$path component=$cur"
      emit "OPEN-REFUSED detail=git check-ignore verifies a LEXICAL path but a WRITE follows symlinks, so this write would land wherever the link points — possibly a TRACKED file or a path outside the repository — dirtying a running gate of record (tree-integrity FAIL, #2926) and making premerge-assert refuse on dirty: yes (#3648). Remove the link and let this tool create a regular file, or pass a --report path that is one."
      exit 2
    fi
    if [ -e "$cur" ] && [ ! -d "$cur" ] && [ "$cur" != "$path" ]; then
      emit "OPEN-REFUSED reason=path-component-not-a-directory what=$what path=$path component=$cur"
      emit "OPEN-REFUSED detail=an intermediate path component exists and is not a directory, so nothing can be written under it."
      exit 2
    fi
  done
  if [ -e "$cur" ] && [ ! -f "$cur" ]; then
    emit "OPEN-REFUSED reason=path-not-a-regular-file what=$what path=$path"
    emit "OPEN-REFUSED detail=this path exists and is not a regular file (a directory, a fifo, a device). This tool writes a text record; it will not write through anything else."
    exit 2
  fi
}

# WRITE_TMP / prepare_write / commit_write — WRITE VIA A SAME-DIRECTORY TEMPORARY FILE PLUS AN
# ATOMIC `mv -f` (#3751 round 1, F5). Two reasons, and both matter:
#   1. `mv -f` REPLACES the destination NAME rather than opening it, so a link that appeared
#      between the check above and the write is replaced, not followed. The check is the control;
#      this is the belt, and it costs nothing.
#   2. no reader can observe a HALF-WRITTEN report. The report of record is read CONCURRENTLY (by
#      `premerge-assert.sh` at the merge point, and by `status` from another session), and a
#      truncated `result:` line is a verdict nobody wrote.
# The TEMPORARY path is verified the same way the destination is — not a symlink, and gitignored
# — because for the duration of the write it is a real file in the tree, so a temp beside a
# `--report` in a directory ignored only by EXTENSION would dirty a running gate exactly as the
# report would.
#
# WRITE_TMP IS A GLOBAL, NOT A PRINTED VALUE. `assert_ignored` and `assert_no_symlink` refuse by
# EMITTING and exiting 2; inside a command substitution that exit would end only the SUBSHELL
# while the refusal text was captured into a variable — a refusal nobody sees, and a script that
# carries on writing.
WRITE_TMP=""
prepare_write() {
  local dest="$1" what="$2"
  assert_no_symlink "$dest" "$what"
  WRITE_TMP="$(dirname "$dest")/.$(basename "$dest").tmp.$$"
  assert_no_symlink "$WRITE_TMP" "$what-tempfile"
  # THE TEMPORARY PATH IS HELD TO THE SAME BAR AS THE DESTINATION, and the refusal EXPLAINS
  # itself, because the caller never named this path. Consequence worth knowing: a --report in a
  # directory ignored only by EXTENSION (`*.md`) is refused, since the temp name is not matched
  # by that pattern and WOULD dirty a running gate. `.review-stage/` — the default and the only
  # path the pipeline uses — is ignored as a DIRECTORY, so this never fires there.
  assert_ignored "$WRITE_TMP" "$what-tempfile" \
    "this is the TEMPORARY file the write goes through (a same-directory temp plus an atomic mv -f, so a symlink is replaced rather than followed and no reader sees a half-written result: line). It is a real file in the tree for the duration of the write, so it is held to the same bar as the destination. A --report directory ignored only by EXTENSION does not match it: ignore the DIRECTORY instead, as .review-stage/ is."
}
commit_write() {
  local dest="$1" what="$2"
  if ! mv -f "$WRITE_TMP" "$dest" 2>/dev/null; then
    rm -f "$WRITE_TMP" 2>/dev/null || true
    emit "OPEN-REFUSED reason=write-failed what=$what path=$dest"
    emit "OPEN-REFUSED detail=the record was written to a temporary file but could not be moved into place, so NOTHING was recorded. The temporary file has been removed; an unexplained leftover would be indistinguishable from a crashed write."
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

  local spawned_iso spawned_epoch reopen_count=0 prior_iso=""
  spawned_iso="$(now_iso)"
  spawned_epoch="$(now_epoch)"

  if [ -f "$sfile" ]; then
    prior_iso="$(read_field "$sfile" spawned-at)"
    if [ "$force" -ne 1 ]; then
      emit "OPEN-REFUSED reason=already-open kind=$kind issue=$issue spawned-at=${prior_iso:-unknown} report=$(read_field "$sfile" report)"
      emit "OPEN-REFUSED detail=a stage is already open for this kind; re-opening would restart a clock a reader is using. Pass --force to re-stamp the report (the original spawned-at is PRESERVED either way), or read it with: $prog verdict $kind --issue $issue"
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

  prepare_write "$sfile" stage-record
  {
    printf 'kind: %s\n' "$kind"
    printf 'issue: %s\n' "$issue"
    printf 'agent: %s\n' "$agent"
    printf 'deadline-secs: %s\n' "$deadline"
    printf 'spawned-at: %s\n' "$spawned_iso"
    printf 'spawned-epoch: %s\n' "$spawned_epoch"
    printf 'report: %s\n' "$rpath"
    printf 'reopen-count: %s\n' "$reopen_count"
    [ "$reopen_count" -eq 0 ] || printf 'reopened-at: %s\n' "$(now_iso)"
  } >"$WRITE_TMP"
  commit_write "$sfile" stage-record

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
    printf 'THIS FILE is your report of record, not your returned message. Replace the\n'
    printf '`result:` line above with EXACTLY ONE of:\n'
    printf '\n'
    printf '    result: PASS        # you reviewed the subject and found no blocking finding\n'
    printf '    result: FINDINGS    # you reviewed the subject and found >=1 blocking finding\n'
    printf '\n'
    printf 'then write your findings below. The token is matched by STRING EQUALITY on its\n'
    printf 'first word against a closed set, so an invented value (e.g. PASS-BUT-UNMEASURED)\n'
    printf 'is read as NOT-RUN, never as a pass.\n'
    printf '\n'
    printf 'If this line still says NOT-RUN when you finish, this stage is recorded as\n'
    printf 'NOT-RUN and cannot reach a merge: an absent review is not a clean one (#3751).\n'
    printf '\n'
    printf '## Findings\n'
    printf '\n'
    printf '(nothing written yet)\n'
  } >"$WRITE_TMP"
  commit_write "$rpath" report-of-record

  emit "OPEN-OK kind=$kind issue=$issue agent=$agent deadline-secs=$deadline spawned-at=$spawned_iso reopen-count=$reopen_count report=$rpath"
  printf '%s\n' "$rpath"
  # THE PASTE-READY CLAUSE. Printed so the contract reaches the agent VERBATIM instead of
  # being paraphrased per lane — the paraphrase is what varied across the seven measured
  # sessions.
  cat <<CLAUSE

--- paste this into the spawn prompt (verbatim) ---
REPORT OF RECORD (mandatory): write your report to
  $rpath
That FILE is your report of record, not your returned message. Write it INCREMENTALLY as
you go, not at the end. When you finish, replace its \`result:\` line with exactly one of
\`result: PASS\` (no blocking finding) or \`result: FINDINGS\` (>=1 blocking finding), and
put your findings below it. If that line still reads \`result: NOT-RUN\` when you stop, this
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
  # "empty" means nothing RECORDABLE — a file of blank lines is empty in every sense a
  # reader cares about, and reporting `report ungrammatical` for it would name the wrong
  # operator action.
  body="$(LC_ALL=C tr -d '[:space:]' <"$rpath" 2>/dev/null || true)"
  if [ -z "$body" ]; then
    printf 'NOT-RUN|report empty\n'; return 0
  fi

  line="$(LC_ALL=C grep -m1 -i '^[[:space:]]*result:' "$rpath" 2>/dev/null || true)"
  if [ -z "$line" ]; then
    printf "NOT-RUN|report ungrammatical: no 'result:' line\n"; return 0
  fi
  value="$(one_line "${line#*:}")"
  if [ -z "$value" ]; then
    printf "NOT-RUN|report ungrammatical: empty 'result:' value\n"; return 0
  fi
  # REDUCE TO THE FIRST WORD AND MATCH BY STRING EQUALITY — never a prefix test. This is the
  # whole closure: `PASS-BUT-UNMEASURED` reduces to `PASS-BUT-UNMEASURED`, which equals
  # nothing in the set, so it is NOT-RUN. A `case` glob or a `grep ^PASS` would accept it.
  # shellcheck disable=SC2086
  set -- $value
  tok="$1"
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
  [ -z "$cause" ] || rendered="$token ($(one_line "$cause" | LC_ALL=C tr '=' '~'))"
  # EXACTLY ONE LINE on stdout. Nothing else is printed here, ever: this line is what a
  # consumer greps, and a second line is a second opinion.
  emit "$KI_KIND RESULT: $rendered elapsed=$STAGE_ELAPSED deadline=$STAGE_DEADLINE agent=$STAGE_AGENT report=$STAGE_REPORT"
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

  emit "STATUS kind=$KI_KIND issue=$KI_ISSUE state=$state elapsed=$STAGE_ELAPSED deadline=$STAGE_DEADLINE past-deadline=$past agent=$STAGE_AGENT spawned-at=$STAGE_SPAWNED_ISO report=$STAGE_REPORT"
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
  local kind="" issue="" reason="" evidence="" performed_by=""
  kind="$(validate_kind "${1:-}")"; shift || true
  while [ $# -gt 0 ]; do
    case "$1" in
      --issue) shift; issue="${1:-}" ;;
      --reason) shift; reason="${1:-}" ;;
      --evidence) shift; evidence="${1:-}" ;;
      --performed-by) shift; performed_by="${1:-}" ;;
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
  assert_no_symlink "$STAGE_REPORT" report-of-record
  assert_ignored "$STAGE_REPORT" report-of-record

  prepare_write "$STAGE_REPORT" report-of-record
  {
    printf '# review stage: %s — issue #%s (AUTHOR-PERFORMED substitute)\n' "$kind" "$issue"
    printf '\n'
    printf 'result: AUTHOR-PERFORMED\n'
    printf '\n'
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
  } >"$WRITE_TMP"
  commit_write "$STAGE_REPORT" report-of-record

  emit "RECORD-OK kind=$kind issue=$issue result=AUTHOR-PERFORMED performed-by=$performed_by reason=$reason_tok evidence=$evidence_tok report=$STAGE_REPORT"
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
