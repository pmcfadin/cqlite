# tristate-file-probe.bash — THREE-VALUED file probes for shell test suites.
#
# Source it, having already defined the suite's own `ok` and `bad`:
#
#     . "$SCRIPT_DIR/lib/tristate-file-probe.bash"
#
# WHY THIS EXISTS (#3752, lane-3752 audit).
#
# `if grep -q <ere> "$FILE"; then ok ...; else bad ...; fi` is a TWO-VALUED
# predicate over a THREE-VALUED signal. "matched", "did not match" and "the
# subject could not be read" are three different facts, and the two-valued form
# silently folds the third onto whichever branch the author happened to write
# second. This repository already lints one instance of the family
# (`1699-find-tristate`: `[ -z "$(find …)" ]` collapsing "the scan FAILED" onto
# "no match"), and the standing rule is that a positive verdict requires an
# AFFIRMATIVE MEASUREMENT and that a could-not-tell state is reported as itself.
#
# BOTH DIRECTIONS ARE LIVE IN THESE SUITES, and only one of them is safe. Under
# a PRESENCE assert the fold lands on `bad` — fail-closed, but under a message
# that names the wrong cause. Under an ABSENCE assert
# (`if grep -q <forbidden-shape>; then bad; else ok; fi` — the shape that pins
# "there is no env override" and "the static text carries no PASS") the fold
# lands on `ok`: an unreadable subject CERTIFIES the property. That is a false
# PASS in a merge-gate test, which is strictly worse than no test.
#
# WHY NO PIPELINE, NO `grep`, NO FORK. Measured on this branch: the assert
#
#     grep -vE '^[[:space:]]*#' "$BINDING" | grep -qE 'gh api .*--paginate'
#
# FAILED once and then passed three times over a BYTE-IDENTICAL file. Both
# suites run under `set -o pipefail`, and `grep -q` exits the instant it
# matches — so whenever the producer has not yet finished writing, it takes
# SIGPIPE, the PIPELINE reports 141, and the `if` takes the no-match branch. A
# CORRECT file intermittently reds, and a merge-gate test that reds on correct
# input is the guard agents learn to waive. These helpers read the subject with
# a plain `read` loop (bash 3.2 safe) and match with bash's own `=~`/`==`,
# so there is no pipe, no
# early-exiting consumer and no exit status to lose.
#
# The probes are PURE (0/1/2 or a count on stdout); the `assert_*` wrappers call
# the suite's `ok`/`bad` and are the intended call sites.

for _tsfp_fn in ok bad; do
  if ! declare -F "$_tsfp_fn" >/dev/null 2>&1; then
    printf 'FAIL - tristate-file-probe.bash requires the suite to define `%s` first.\n' \
      "$_tsfp_fn" >&2
    exit 1
  fi
done
unset _tsfp_fn

# PROBE_WHY carries WHICH readability fact failed, so the diagnostic can name it
# rather than saying "something went wrong".
PROBE_WHY=""
PROBE_LINES=()

# probe_read <file> — 0 and fills PROBE_LINES, or 2 with PROBE_WHY set.
# An EMPTY file is `unreadable` on purpose: every subject here is source or a
# fixture that must have content, so zero lines is a failed read, not a
# measurement of absence.
probe_read() {
  PROBE_WHY=""
  PROBE_LINES=()
  if [ ! -e "$1" ]; then
    PROBE_WHY="the path does not exist"
    return 2
  fi
  if [ ! -f "$1" ]; then
    PROBE_WHY="the path is not a regular file"
    return 2
  fi
  if [ ! -r "$1" ]; then
    PROBE_WHY="the file is not readable"
    return 2
  fi
  # PORTABLE, NOT `mapfile` (roborev job 59, finding 4). `mapfile`/`readarray`
  # is bash 4+, and this repo states bash 3.2 compatibility (stock macOS ships
  # 3.2.57). Under 3.2 the builtin is simply absent, so every probe in this
  # library returned 2 and every structural assert built on it reported "could
  # not be measured" — a whole suite degrading to unmeasurable on a SUPPORTED
  # host. The read loop below is byte-equivalent for our subjects and needs no
  # builtin: `read` returns non-zero on a final line with no trailing newline,
  # which is why the last partial line is appended explicitly rather than
  # dropped.
  # `_line` is INITIALISED, not merely declared: this library is sourced into
  # suites running under `set -u`, where the `[ -n "$_line" ]` guard below would
  # abort on an unset variable the first time `read` fails.
  local _line=""
  PROBE_LINES=()
  while IFS= read -r _line || [ -n "$_line" ]; do
    PROBE_LINES+=("$_line")
    _line=""
  done <"$1" || {
    PROBE_WHY="the file could not be read in full"
    PROBE_LINES=()
    return 2
  }
  if [ "${#PROBE_LINES[@]}" -eq 0 ]; then
    PROBE_WHY="the file is empty"
    return 2
  fi
  return 0
}

# _probe_skip <line> <mode> — 0 when this line is excluded from the subject.
# `code` drops whole-line comments, the `grep -vE '^[[:space:]]*#'` these
# asserts used to spell as the first half of a pipeline.
_probe_skip() {
  [ "$2" = code ] || return 1
  [[ $1 =~ ^[[:space:]]*# ]]
}

# probe_file_match <file> <ere> [all|code]
#   0 matched / 1 did not match / 2 COULD NOT BE MEASURED
probe_file_match() {
  local f=$1 re=$2 mode=${3:-all} line
  probe_read "$f" || return 2
  for line in ${PROBE_LINES[@]+"${PROBE_LINES[@]}"}; do
    _probe_skip "$line" "$mode" && continue
    [[ $line =~ $re ]] && return 0
  done
  return 1
}

# probe_file_fixed <file> <substring> [all|code] — the `grep -F` form.
probe_file_fixed() {
  local f=$1 needle=$2 mode=${3:-all} line
  probe_read "$f" || return 2
  for line in ${PROBE_LINES[@]+"${PROBE_LINES[@]}"}; do
    _probe_skip "$line" "$mode" && continue
    [[ $line == *"$needle"* ]] && return 0
  done
  return 1
}

# probe_count <file> <line-exact|contains|nonblank> [needle]
#   Echoes the count and returns 0, or returns 2 having echoed NOTHING. A
#   caller must therefore branch on the RETURN CODE before reading the number —
#   `[ "$(probe_count …)" -eq 2 ]` on an unmeasurable subject is the same fold
#   one level out.
probe_count() {
  local f=$1 kind=$2 needle=${3:-} line n=0
  probe_read "$f" || return 2
  for line in ${PROBE_LINES[@]+"${PROBE_LINES[@]}"}; do
    case "$kind" in
      line-exact) [ "$line" = "$needle" ] && n=$((n + 1)) ;;
      contains) [[ $line == *"$needle"* ]] && n=$((n + 1)) ;;
      nonblank) [ -n "$line" ] && n=$((n + 1)) ;;
      *)
        PROBE_WHY="probe_count: unknown mode '$kind'"
        return 2
        ;;
    esac
  done
  printf '%s' "$n"
  return 0
}

# _probe_unmeasured <label> <file> — the DISTINCT third message. "could not read
# the subject" and "the subject lacks the property" are different failures with
# different operator actions, so they never share wording.
_probe_unmeasured() {
  printf '%s: UNMEASURED — could not read %s (%s), so the property was NOT tested' \
    "${1%%:*}" "$2" "$PROBE_WHY"
}

# assert_src_present <ok-msg> <absent-msg> <file> <ere> [all|code]
assert_src_present() {
  probe_file_match "$3" "$4" "${5:-all}"
  case "$?" in
    0) ok "$1" ;;
    1) bad "$2" ;;
    *) bad "$(_probe_unmeasured "$1" "$3")" ;;
  esac
}

# assert_src_absent <ok-msg> <present-msg> <file> <ere> [all|code]
assert_src_absent() {
  probe_file_match "$3" "$4" "${5:-all}"
  case "$?" in
    1) ok "$1" ;;
    0) bad "$2" ;;
    *) bad "$(_probe_unmeasured "$1" "$3")" ;;
  esac
}

# assert_src_present_fixed <ok-msg> <absent-msg> <file> <substring> [all|code]
assert_src_present_fixed() {
  probe_file_fixed "$3" "$4" "${5:-all}"
  case "$?" in
    0) ok "$1" ;;
    1) bad "$2" ;;
    *) bad "$(_probe_unmeasured "$1" "$3")" ;;
  esac
}

# assert_src_absent_fixed <ok-msg> <present-msg> <file> <substring> [all|code]
assert_src_absent_fixed() {
  probe_file_fixed "$3" "$4" "${5:-all}"
  case "$?" in
    1) ok "$1" ;;
    0) bad "$2" ;;
    *) bad "$(_probe_unmeasured "$1" "$3")" ;;
  esac
}

# assert_count <ok-msg> <wrong-msg> <file> <line-exact|contains|nonblank> <needle> <expected>
# The wrong-msg may carry a %s, which is filled with the OBSERVED count.
assert_count() {
  local got
  got=$(probe_count "$3" "$4" "$5")
  if [ "$?" -ne 0 ]; then
    bad "$(_probe_unmeasured "$1" "$3")"
    return
  fi
  if [ "$got" -eq "$6" ]; then
    ok "$1"
  else
    # shellcheck disable=SC2059
    bad "$(printf -- "$2" "$got")"
  fi
}

# probe_write_code_lines <src> <dest> — write <src>'s NON-COMMENT lines to
# <dest>. 0 on success, 2 (with PROBE_WHY) if the source could not be read or
# the write failed. The `grep -v … > file` this replaces reported neither: a
# failed derivation left an empty file, and every later probe on it then
# measured the derivation rather than the subject.
probe_write_code_lines() {
  local line
  probe_read "$1" || return 2
  : >"$2" || {
    PROBE_WHY="the destination $2 could not be created"
    return 2
  }
  for line in ${PROBE_LINES[@]+"${PROBE_LINES[@]}"}; do
    _probe_skip "$line" code && continue
    printf '%s\n' "$line" >>"$2" || {
      PROBE_WHY="writing $2 failed part-way"
      return 2
    }
  done
  return 0
}

# assert_src_present_all <ok-msg> <absent-msg> <file> <ere>...
# EVERY pattern must be present. A conjunction spelled as `grep -q … && grep -q …`
# has the same fold as a single one, twice over, and an unreadable subject there
# makes it impossible to say WHICH half was not measured.
assert_src_present_all() {
  local okmsg=$1 badmsg=$2 f=$3 re
  shift 3
  for re in "$@"; do
    probe_file_match "$f" "$re"
    case "$?" in
      0) ;;
      1)
        bad "$badmsg"
        return
        ;;
      *)
        bad "$(_probe_unmeasured "$okmsg" "$f")"
        return
        ;;
    esac
  done
  ok "$okmsg"
}

# assert_src_present_line <ok-msg> <absent-msg> <file> <whole-line>
# The `grep -q -x -F` form: whole-line-exact, no regex, no substring.
assert_src_present_line() {
  local got
  got=$(probe_count "$3" line-exact "$4")
  if [ "$?" -ne 0 ]; then
    bad "$(_probe_unmeasured "$1" "$3")"
  elif [ "$got" -gt 0 ]; then
    ok "$1"
  else
    bad "$2"
  fi
}
