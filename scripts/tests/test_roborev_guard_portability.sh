#!/usr/bin/env bash
# PORTABILITY GUARD for the roborev review-guard code path (issue #3296).
#
# WHY THIS FILE EXISTS. `scripts/tests/test_roborev_review_guard.sh` gates a merge (it runs
# inside the gate's `roborev-lints` component, in --lite AND in the full gate of record). At
# `origin/main` it reported `passed: 553  failed: 7` on macOS and `failed: 0` on Linux: three
# GNU-vs-BSD utility differences in the TEST's own scaffolding, not in the wrapper it guards.
# Every hosted CI lane is Linux, so NO hosted signal can see that class — it is found by
# whoever next tries to certify anything locally, as a failure attributed to THEIR diff.
#
# So the regression coverage cannot be "run it on macOS": it must be a DIFFERENTIAL that runs
# on every platform. Two mechanisms, both here:
#
#   (1) STRUCTURAL — the GNU-only constructs that caused #3296 (and their nearest relatives)
#       cannot be reintroduced into the roborev code path. Each pattern carries a POSITIVE
#       CONTROL: a sample violation the pattern must DETECT. A scanner whose regex silently
#       matches nothing is the vacuous pass this repo keeps finding (CLAUDE.md: "never derive a
#       pass from the ABSENCE of a bad signal"), so every pattern is affirmatively measured.
#
#   (2) BEHAVIOURAL under BSD SHIMS — a `sed` that consumes -i's next argument as the backup
#       suffix (BSD semantics) and a `paste` that usage()-errors with no file operand are put
#       first on PATH, and the guard test's OWN helpers — extracted VERBATIM from it, never
#       copied — are exercised against them. Each shim is itself controlled: it must first be
#       shown to REPRODUCE the reported defect, or the differential below proves nothing.
#
# Deliberately TARGETED: it exercises the new helpers and the mutated asserts, NOT all 581
# guard-test cases under shims (that would double a --lite component's runtime for no extra
# information — the full-suite run under both shims was recorded once, by hand, on #3296).
#
# SAME DOCTRINE, DIFFERENT SCOPE as scripts/tests/test_agent_gate_tree_portability.sh (#2926,
# after #2914): behavioural BSD shims for the covered paths plus a static lint for the
# uncovered ones, every lint rule proved discriminating. That file lints the gate's
# tree-integrity functions; this one lints the roborev review-guard path. macOS is a
# FIRST-CLASS host here — the gate carries `Darwin) … taskpolicy` branches and declares a
# /bin/bash 3.2 floor — so this file also avoids bash-4-only constructs and never expands a
# possibly-empty array under `set -u` (a 3.2 unbound-variable abort).
#
# AUTHORITATIVE SOURCES for the three BSD behaviours emulated here (a CQLite file is never
# authority for another program's behaviour):
#   sed -i takes a REQUIRED argument   Apple text_cmds sed/main.c:
#                                      getopt(argc, argv, "EI:ae:f:i:lnru")   <- the `i:`
#   paste needs a file operand         Apple text_cmds paste/paste.c + FreeBSD
#                                      usr.bin/paste/paste.c:
#                                        argc -= optind; argv += optind;
#                                        if (*argv == NULL) usage();
#                                      usage() -> stderr + exit(1), i.e. EMPTY stdout
#   cut is NOT implicated              FreeBSD usr.bin/cut/cut.c: with no operand it reads
#                                      stdin (`if (*argv) ... else fcn(stdin, "stdin")`)
#
# Run standalone:   bash scripts/tests/test_roborev_guard_portability.sh
# Or via the gate:  scripts/agent-gate.sh --lite   (roborev-lints component)
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
GUARD="$SCRIPT_DIR/test_roborev_review_guard.sh"
GATE="$SCRIPT_DIR/../agent-gate.sh"
FLOW_DIR="$SCRIPT_DIR/../flow"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

if [ ! -f "$GUARD" ]; then
  printf 'FAIL - the guard test is not at %s — nothing to keep portable\n' "$GUARD"
  exit 1
fi

tmp=$(mktemp -d "${TMPDIR:-/tmp}/roborev-portability.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

# ===========================================================================
# (1) STRUCTURAL: no GNU-only construct in the roborev code path.
#
# The table is EXPLICIT rather than a generic "GNU long option" sweep: a generic sweep
# false-positives on git/gh (whose long options are portable) and a lint agents learn to
# waive is worse than no lint. Add an entry when a new divergence is actually found.
# Comment-only lines are stripped before scanning, so prose ABOUT a construct (including
# this file's own citations, and the guard test's `sed -i` explanation) is not a violation.
# ===========================================================================
SCAN_FILES=(
  "$GUARD"
  "$FLOW_DIR/roborev-review.sh"
  "$FLOW_DIR/roborev-review-checks.sh"
  "$FLOW_DIR/roborev-review-oracles.sh"
  "$FLOW_DIR/roborev-job-facts.py"
)

# Three parallel arrays: the ERE, why it is not portable, and a sample violation the ERE
# MUST detect (the positive control that keeps the pattern honest).
CONSTRUCT_RE=(); CONSTRUCT_WHY=(); CONSTRUCT_SAMPLE=()
add_construct() { CONSTRUCT_RE+=("$1"); CONSTRUCT_WHY+=("$2"); CONSTRUCT_SAMPLE+=("$3"); }

add_construct '(^|[^[:alnum:]_-])sed[[:space:]]+(-[a-zA-Z]+[[:space:]]+)*-i([[:space:]]|$)' \
  "BSD sed's -i takes a REQUIRED suffix argument, so it eats the EXPRESSION and the edit never lands (#3296 cx28/cx29/cx28b/cx28c) — use the guard test's sed_inplace helper" \
  "  sed -i 's/a/b/' \"\$f\""
add_construct '(^|[^[:alnum:]_-])sed[[:space:]]+-i("")' \
  'the empty-suffix spelling -i"" is GNU-only (BSD needs -i "" or no -i at all) — use sed_inplace' \
  '  sed -i"" -e s/a/b/ f'
add_construct "(^|[^[:alnum:]_-])sed[[:space:]]+-i('')" \
  "the empty-suffix spelling -i'' is GNU-only — use sed_inplace" \
  "  sed -i'' -e s/a/b/ f"
add_construct '(^|[^[:alnum:]_-])paste([[:space:]]+-[^[:space:]|;)&]+)*[[:space:]]*($|\||\)|;|&)' \
  'a paste with NO FILE OPERAND is empty output + exit 1 on BSD (it usage()-errors instead of reading stdin) — pass an explicit `-`, or extract with awk (#3296 case (j2))' \
  '  order=$(grep -n x f | cut -d: -f2 | paste -sd,)'
add_construct '(^|[^[:alnum:]_-])readlink[[:space:]]+(-[a-zA-Z]+[[:space:]]+)*-f' \
  'readlink -f is absent from older BSD readlink — canonicalise with `cd "$p" && pwd -P` (which is what the wrapper does)' \
  '  p=$(readlink -f "$x")'
add_construct '(^|[^[:alnum:]_-])stat[[:space:]]+-c' \
  'stat -c is GNU-only (BSD spells it stat -f)' \
  '  n=$(stat -c %s "$f")'
add_construct '(^|[^[:alnum:]_-])grep[[:space:]]+-[a-zA-Z]*P([[:space:]]|$)' \
  'grep -P (PCRE) is GNU-only — BSD grep has no -P' \
  "  grep -P '\\\\d+' f"
add_construct '(^|[^[:alnum:]_-])date[[:space:]]+(-d[[:space:]]|--date)' \
  'date -d/--date is GNU-only (BSD date uses -r / -v / -j -f)' \
  '  t=$(date -d @1700000000)'
add_construct '(^|[^[:alnum:]_-])(sed|grep)[[:space:]]+-[a-zA-Z]*z([[:space:]]|$)' \
  'sed -z / grep -z (NUL-delimited records) are GNU-only — read `git … -z` output with a shell read loop or awk RS' \
  '  grep -z foo f'
add_construct '\-printf[[:space:]]' \
  'find -printf is GNU-only — use -exec or -print with a shell loop' \
  "  find . -printf '%p\\\\n'"
add_construct '(^|[^[:alnum:]_-])xargs[[:space:]]+(-[a-zA-Z]*r|--)' \
  'xargs -r (and GNU long options) are not in BSD xargs; BSD already skips an empty input line only with -0' \
  '  printf "" | xargs -r rm'
add_construct '(^|[^[:alnum:]_-])base64[[:space:]]+-w' \
  'base64 -w is GNU-only (BSD/macOS base64 has no wrap flag; use -b or fold)' \
  '  base64 -w0 <f'
add_construct '(^|[^[:alnum:]_-])timeout[[:space:]]+[0-9]' \
  'timeout(1) is NOT installed on stock macOS — guard it with `command -v timeout` or restructure' \
  '  timeout 30 some-command'
add_construct '(^|[^[:alnum:]_-])(mapfile|readarray)([[:space:]]|$)|declare[[:space:]]+-A|\$\{[A-Za-z_][A-Za-z_0-9]*,,\}' \
  'bash 4 only — stock macOS /bin/bash is 3.2, so mapfile/readarray/associative arrays/${v,,} can abort the script outright' \
  '  mapfile -t arr <f'

if [ "${#CONSTRUCT_RE[@]}" -ne "${#CONSTRUCT_WHY[@]}" ] ||
  [ "${#CONSTRUCT_RE[@]}" -ne "${#CONSTRUCT_SAMPLE[@]}" ]; then
  bad 'structural: the construct table arrays are not the same length — some pattern has no reason or no positive control'
fi

# The scan body: code only. A construct named in a comment (this repo documents the ones it
# banned) is prose, not an invocation. A line carrying `portability-lint-allow` is exempt —
# the repo's existing escape-marker convention (`injection-lint-allow`, `perf-gate-allow`) —
# so a provably-safe or deliberately-BSD-emulating line has a route that is VISIBLE in the
# diff instead of forcing a rewrite of the lint.
scan_hits() { # scan_hits <ere> <file>
  grep -vE '^[[:space:]]*#' "$2" | grep -v 'portability-lint-allow' | grep -nE -- "$1" || true
}

for _ci in "${!CONSTRUCT_RE[@]}"; do
  _re="${CONSTRUCT_RE[$_ci]}"
  _why="${CONSTRUCT_WHY[$_ci]}"
  # POSITIVE CONTROL FIRST: the pattern must detect its own sample violation. Without this a
  # typo'd regex would report every file clean, forever.
  printf '%s\n' "${CONSTRUCT_SAMPLE[$_ci]}" >"$tmp/sample-$_ci.sh"
  if [ -n "$(scan_hits "$_re" "$tmp/sample-$_ci.sh")" ]; then
    ok "structural control: the pattern detects its sample violation (${CONSTRUCT_SAMPLE[$_ci]})"
  else
    bad "structural control: the pattern MATCHES NOTHING — it cannot detect '${CONSTRUCT_SAMPLE[$_ci]}', so its clean verdict below is vacuous ($_re)"
    continue
  fi
  _hits=""
  for _f in "${SCAN_FILES[@]}"; do
    if [ ! -f "$_f" ]; then
      bad "structural: scan target missing: $_f"
      continue
    fi
    _fh=$(scan_hits "$_re" "$_f")
    [ -n "$_fh" ] && _hits="$_hits $(basename "$_f"):${_fh%%$'\n'*}"
  done
  if [ -z "$_hits" ]; then
    ok "structural: the roborev code path is free of this construct — $_why"
  else
    bad "structural: GNU-only construct in the roborev code path ($_why):$_hits"
  fi
done

# NEGATIVE CONTROL for the paste pattern, whose ERE is the subtlest of the table: a paste WITH
# an explicit operand is portable and must NOT be reported.
printf '%s\n' '  order=$(grep -n x f | cut -d: -f2 | paste -sd, -)' >"$tmp/paste-ok.sh"
printf '%s\n' '  order=$(paste -sd, "$f")' >>"$tmp/paste-ok.sh"
if [ -z "$(scan_hits "${CONSTRUCT_RE[3]}" "$tmp/paste-ok.sh")" ]; then
  ok 'structural control: a paste WITH a file operand (`-` or a path) is not flagged'
else
  bad 'structural control: the paste pattern false-positives on a portable paste with an operand — a lint that reds on correct input is the lint agents learn to waive'
fi

# CONTROL for the escape marker, in BOTH directions: it must exempt the line it is on, and it
# must not be a blanket switch (the same sample WITHOUT the marker is still detected above).
printf '%s\n' "  sed -i 's/a/b/' \"\$f\"   # portability-lint-allow: deliberate BSD-emulation control" \
  >"$tmp/allow.sh"
if [ -z "$(scan_hits "${CONSTRUCT_RE[0]}" "$tmp/allow.sh")" ]; then
  ok 'structural control: a line marked portability-lint-allow is exempt (a visible, per-line escape)'
else
  bad 'structural control: the portability-lint-allow marker does not exempt its line'
fi

# ===========================================================================
# (2) THE BSD SHIMS, and the controls that prove they reproduce the reported defects.
# ===========================================================================
shim="$tmp/shim"
mkdir -p "$shim"
REAL_SED=$(command -v sed || printf '')
REAL_PASTE=$(command -v paste || printf '')
if [ -z "$REAL_SED" ] || [ -z "$REAL_PASTE" ]; then
  bad 'shim setup: sed/paste not found on PATH — the differential cannot run (this is a failure to measure, not a measurement)'
fi

{
  printf '#!/usr/bin/env bash\n# BSD/macOS sed emulation, -i only (see test_roborev_guard_portability.sh).\n'
  printf 'REAL_SED=%q\n' "$REAL_SED"
  cat <<'SHIM_SED'
set -uo pipefail
# A zero-argument call is handled before any array is built: expanding an EMPTY "${arr[@]}"
# under `set -u` aborts on macOS's /bin/bash 3.2, the floor the gate declares.
[ $# -eq 0 ] && exec "$REAL_SED"
# BSD sed declares -i with a REQUIRED argument, so a SEPARATE `-i` consumes the NEXT argv
# entry as the backup suffix. Rewriting it to GNU's attached form reproduces exactly that
# consumption: `sed -i EXPR FILE` becomes suffix=EXPR, script=FILE, no input file — the edit
# never lands and the exit status is non-zero, which is what macOS does.
out=()
while [ $# -gt 0 ]; do
  if [ "$1" = "-i" ]; then
    shift
    suffix="${1-}"
    [ $# -gt 0 ] && shift
    out+=("-i$suffix")
  else
    out+=("$1")
    shift
  fi
done
exec "$REAL_SED" "${out[@]}"
SHIM_SED
} >"$shim/sed"

{
  printf '#!/usr/bin/env bash\n# BSD/macOS paste emulation (see test_roborev_guard_portability.sh).\n'
  printf 'REAL_PASTE=%q\n' "$REAL_PASTE"
  cat <<'SHIM_PASTE'
set -uo pipefail
# getopt(argc, argv, "d:s") then `if (*argv == NULL) usage();` — flags are consumed (with
# bundling, so `-sd,` is -s plus -d,), and a missing FILE OPERAND is a usage error on stderr
# with exit 1 and NOTHING on stdout.
if [ $# -eq 0 ]; then   # see the sed shim: no empty-array expansion under bash 3.2 + set -u
  printf 'usage: paste [-s] [-d delimiters] file ...\n' >&2
  exit 1
fi
orig=("$@")
# A COUNTER, not an array: `${#arr[@]}` on an array that never received an element is not
# reliably safe under bash 3.2 + set -u either.
n_operands=0
end=0
while [ $# -gt 0 ]; do
  a="$1"
  if [ "$end" -eq 0 ] && [ "$a" = "--" ]; then end=1; shift; continue; fi
  if [ "$end" -eq 0 ] && [ "${a#-}" != "$a" ] && [ "$a" != "-" ]; then
    rest="${a#-}"
    while [ -n "$rest" ]; do
      c="${rest:0:1}"
      rest="${rest:1}"
      case "$c" in
        d) if [ -n "$rest" ]; then rest=""; else shift; fi ;;
        *) ;;
      esac
    done
    shift
    continue
  fi
  n_operands=$((n_operands + 1))
  shift
done
if [ "$n_operands" -eq 0 ]; then
  printf 'usage: paste [-s] [-d delimiters] file ...\n' >&2
  exit 1
fi
exec "$REAL_PASTE" "${orig[@]}"
SHIM_PASTE
} >"$shim/paste"
chmod +x "$shim/sed" "$shim/paste"

SHIM_PATH="$shim:$PATH"

# CONTROL A: the sed shim must reproduce the #3296 defect — non-zero exit AND an UNCHANGED
# file. If it silently worked, every sed_inplace assert below would be vacuous.
printf 'foo\n' >"$tmp/ctl-sed.txt"
if PATH="$SHIM_PATH" sed -i 's/foo/bar/' "$tmp/ctl-sed.txt" 2>/dev/null; then
  bad 'shim control: `sed -i EXPR FILE` SUCCEEDED under the BSD shim — the shim does not emulate BSD, so the differential below would prove nothing'
elif [ "$(cat "$tmp/ctl-sed.txt")" = foo ]; then
  ok 'shim control: under BSD -i semantics `sed -i EXPR FILE` fails AND leaves the file unpatched (the #3296 root cause 1, reproduced on this platform)'
else
  bad "shim control: the file changed despite the failure: $(cat "$tmp/ctl-sed.txt")"
fi

# CONTROL B: the paste shim must reproduce case (j2) — empty stdout, non-zero exit.
_ctl_paste=$(printf 'a\nb\n' | PATH="$SHIM_PATH" paste -sd, 2>/dev/null)
_ctl_paste_rc=$?
if [ "$_ctl_paste_rc" -eq 0 ] || [ -n "$_ctl_paste" ]; then
  bad "shim control: operand-less `paste -sd,` produced '$_ctl_paste' (rc $_ctl_paste_rc) under the BSD shim — the shim does not emulate BSD"
else
  ok 'shim control: under BSD semantics an operand-less `paste -sd,` yields EMPTY stdout + non-zero exit (the #3296 root cause 3, reproduced on this platform)'
fi
printf 'a\nb\n' >"$tmp/ctl-paste.txt"
if [ "$(PATH="$SHIM_PATH" paste -sd, "$tmp/ctl-paste.txt" 2>/dev/null)" = 'a,b' ]; then
  ok 'shim control: the paste shim still passes a legitimate `paste -sd, FILE` through'
else
  bad 'shim control: the paste shim broke a legitimate `paste -sd, FILE` — it emulates something other than BSD'
fi

# ---------------------------------------------------------------------------
# The helpers under test are EXTRACTED VERBATIM from the guard test, never copied: a copy
# would drift, and a drifted copy passing here while the real helper is broken is precisely
# the shape of failure this file exists to remove.
# ---------------------------------------------------------------------------
extract_fn() { # extract_fn <name> -> the function's source text
  awk -v fn="$1" '$0 ~ "^" fn "\\(\\) \\{" { inside = 1 } inside { print } inside && /^\}$/ { exit }' "$GUARD"
}

for _fn in sed_inplace summary_key_order; do
  _src=$(extract_fn "$_fn")
  if [ -z "$_src" ]; then
    bad "extraction: $_fn is not defined in $(basename "$GUARD") — the portable helper was removed or renamed, so nothing below tests it"
    continue
  fi
  if printf '%s\n' "$_src" | bash -n 2>/dev/null; then
    ok "extraction: $_fn was read verbatim from the guard test and parses"
  else
    bad "extraction: the $_fn text read from the guard test does not parse"
    continue
  fi
  eval "$_src"
done

if ! declare -f sed_inplace >/dev/null || ! declare -f summary_key_order >/dev/null; then
  bad 'extraction: the guard test helpers are not available — the differential below cannot run'
else
  # --- sed_inplace, single-line substitution, under BSD -i semantics.
  printf 'alpha\nbeta\n' >"$tmp/one.txt"
  if PATH="$SHIM_PATH" sed_inplace "$tmp/one.txt" 's/^alpha$/ALPHA/' &&
    [ "$(cat "$tmp/one.txt")" = "$(printf 'ALPHA\nbeta')" ]; then
    ok 'sed_inplace: a single-line substitution lands under BSD sed semantics (where `sed -i` would not have)'
  else
    bad "sed_inplace: the substitution did not land under the BSD shim: $(cat "$tmp/one.txt")"
  fi

  # --- sed_inplace, the cx29-shaped MULTI-LINE insert, verified the way cx29 verifies it:
  # the inserted line must be the line IMMEDIATELY AFTER the function header.
  printf 'roborev_check_prompt_content() {\n  local x\n}\n' >"$tmp/multi.sh"
  PATH="$SHIM_PATH" sed_inplace "$tmp/multi.sh" \
    's/^roborev_check_prompt_content() {$/roborev_check_prompt_content() {\
  return 0/'
  _mp=$(grep -A1 '^roborev_check_prompt_content() {$' "$tmp/multi.sh" | sed -n '2p')
  if [ "$_mp" = '  return 0' ]; then
    ok 'sed_inplace: the cx29-shaped two-line replacement lands, with the new line immediately after the header'
  else
    bad "sed_inplace: the multi-line replacement did not land as two lines (line 2 = '$_mp')"
  fi

  # --- AC2 AT THE EDIT SITE: an expression that matches NOTHING must be an ERROR, and the
  # file must be byte-identical. Portability must not have become unconditionality.
  cp "$tmp/one.txt" "$tmp/one.before"
  if PATH="$SHIM_PATH" sed_inplace "$tmp/one.txt" 's/^nothing-matches-this$/x/'; then
    bad 'sed_inplace: a no-op patch returned SUCCESS — a silently unapplied mutation would then be asserted against, which is a probe failing in the direction that looks like success'
  elif cmp -s "$tmp/one.txt" "$tmp/one.before"; then
    ok 'sed_inplace: a no-op patch returns NON-ZERO and leaves the file byte-identical (fail-closed preserved)'
  else
    bad 'sed_inplace: a no-op patch changed the file'
  fi
  # And no temp spill: the helper must not leave its scratch file behind on the failure path.
  #
  # AFFIRMATIVE MEASUREMENT (CLAUDE.md: "never derive a pass from the ABSENCE of a bad
  # signal"). The obvious spelling — `[ -z "$(find … 2>/dev/null)" ]` — reads EMPTY OUTPUT as
  # "no spill", so an enumeration that never ran (unreadable dir, a `find` that errored, a
  # mistyped path) reports the same green as a genuinely clean dir. That is the exact vacuous
  # portability pass this file exists to prevent, one level down. So the enumeration is
  # (1) first shown to DETECT a planted spill, and (2) then required to EXIT ZERO before its
  # empty output is allowed to mean anything; its stderr is captured, never discarded.
  #
  # `-maxdepth` is deliberately KEPT: it is NOT a GNU extension — FreeBSD/macOS find(1)
  # documents `-maxdepth n` ("Always true; descend at most n directory levels below the
  # command line arguments"), and scripts/agent-gate.sh already relies on it fleet-wide.
  _spill_err="$tmp/spill-find.err"
  scratch_scan() { find "$tmp" -maxdepth 1 -name '*.sed-inplace.*' 2>"$_spill_err"; }

  _decoy="$tmp/planted.sed-inplace.control"
  : >"$_decoy"
  _ctl_spill=$(scratch_scan); _ctl_spill_rc=$?
  rm -f "$_decoy"
  if [ "$_ctl_spill_rc" -eq 0 ] && printf '%s\n' "$_ctl_spill" | grep -qF 'planted.sed-inplace.control'; then
    ok 'sed_inplace control: the scratch-file enumeration detects a PLANTED spill (so its clean verdict below is a measurement, not an empty pipe)'
  else
    bad "sed_inplace control: the scratch-file enumeration did not find a planted spill (rc $_ctl_spill_rc, stderr: $(tr '\n' ' ' <"$_spill_err")) — every no-spill verdict from it would be vacuous"
  fi

  _spill=$(scratch_scan); _spill_rc=$?
  if [ "$_spill_rc" -ne 0 ]; then
    bad "sed_inplace: the scratch-file enumeration itself FAILED (rc $_spill_rc, stderr: $(tr '\n' ' ' <"$_spill_err")) — a failure to measure is not a measurement, so this is NOT reported as 'no spill'"
  elif [ -n "$_spill" ]; then
    bad "sed_inplace: a .sed-inplace.* scratch file survived the no-op path: $(printf '%s' "$_spill" | tr '\n' ' ')"
  else
    ok 'sed_inplace: no scratch file is left behind on the no-op path (enumeration exited 0, having first been shown to detect a planted spill)'
  fi

  # --- AC2 IN THE CASES: the four cases must STILL verify the mutation landed. Asserted on
  # the guard test's TEXT, because a helper that is fail-closed today does not keep the CASES
  # fail-closed tomorrow. These are the three verification forms cx28 / cx29 / cx28b+cx28c use.
  for _vpair in \
    "grep -qF 'TIER1=\"MEASUREMENT-DID-NOT-HAPPEN\"'|cx28 verifies its unrecognised-verdict patch landed" \
    "= '  return 0'|cx29 verifies its early-return patch is the line after the header" \
    "grep -qF \"TIER1=\\\"\$_np_value\\\"\"|cx28b/cx28c verify their near-prefix patch landed"; do
    _vtext="${_vpair%%|*}"
    _vwhy="${_vpair#*|}"
    if grep -qF -- "$_vtext" "$GUARD"; then
      ok "AC2: $_vwhy"
    else
      bad "AC2: the guard test no longer contains this verification ($_vwhy) — a case that cannot detect an unapplied patch is a regression even when green"
    fi
  done

  # --- summary_key_order under the paste shim, plus the RED side of the differential.
  printf 'vacuity-tier2: PASS\nroborev-exit: PASS\nfindings: NONE\nlog: /tmp/x\n' >"$tmp/block.txt"
  if [ "$(PATH="$SHIM_PATH" summary_key_order "$tmp/block.txt" 'vacuity-tier2|roborev-exit|log')" \
    = 'vacuity-tier2,roborev-exit,log' ]; then
    ok 'summary_key_order: the key order is extracted correctly under BSD paste semantics'
  else
    bad "summary_key_order: wrong extraction under the BSD shim: '$(PATH="$SHIM_PATH" summary_key_order "$tmp/block.txt" 'vacuity-tier2|roborev-exit|log')'"
  fi
  # The pre-#3296 pipeline, run under the same shim, on the same input: it must come back
  # EMPTY. This is the control that the fix fixed something — the reported case (j2) symptom
  # was `unexpected key order: ` with nothing after the colon. The operand-less paste below is
  # DELIBERATE (it is the defect being reproduced) and is why the structural scan above covers
  # the roborev code path rather than this file.
  _old=$(PATH="$SHIM_PATH" bash -c "grep -nE '^(vacuity-tier2|roborev-exit|log):' '$tmp/block.txt' | cut -d: -f2 | paste -sd," 2>/dev/null)
  if [ -z "$_old" ]; then
    ok 'differential: the pre-#3296 `grep | cut | paste -sd,` pipeline returns EMPTY under BSD paste — the reported case (j2) symptom, reproduced'
  else
    bad "differential: the old pipeline returned '$_old' under the shim, so this platform cannot reproduce case (j2) and the fix is unverified here"
  fi
  if printf '%s\n' "$(extract_fn summary_key_order)" | grep -qE '(^|[^[:alnum:]_-])paste([[:space:]]|$)'; then
    bad 'summary_key_order: it still shells out to paste'
  else
    ok 'summary_key_order: the extraction has no paste stage at all'
  fi
fi

# ===========================================================================
# (3) AC3: case (f)'s pinned contract still FAILs when it is VIOLATED.
#
# The fix compares the wrapper's CANONICALISED --repo against a canonicalisation computed the
# same way, instead of the literal fixture path (which differs on macOS, where $TMPDIR is
# under /var, a symlink to /private/var). The hazard of such a fix is loosening the match
# until it accepts anything. So the REAL assert block is extracted from the guard test and
# fed four records: the sanctioned one, and three violations that must each still be caught.
#
# The fixture repo is placed UNDER A SYMLINK, which reproduces /var -> /private/var on any
# platform: `$work` and its canonical form differ, so the canonical-to-canonical comparison is
# genuinely exercised here rather than trivially satisfied.
# ===========================================================================
_cf_block=$(awk '/^# >>> BEGIN case-f-invocation-asserts/,/^# <<< END case-f-invocation-asserts/' "$GUARD")
if [ -z "$_cf_block" ] || ! printf '%s\n' "$_cf_block" | grep -q 'work_canon='; then
  bad 'AC3: the case-f-invocation-asserts block could not be extracted from the guard test (markers removed?) — the contract is then untested here'
else
  mkdir -p "$tmp/real/work"
  ln -s "$tmp/real" "$tmp/link"
  work="$tmp/link/work"
  git init -q -b main "$work" >/dev/null 2>&1
  git -C "$work" config user.email t@e && git -C "$work" config user.name t
  printf 'x\n' >"$work/f.txt"
  git -C "$work" add f.txt >/dev/null 2>&1
  git -C "$work" commit -q -m base >/dev/null 2>&1
  _canon=$(cd "$(git -C "$work" rev-parse --show-toplevel)" && pwd -P)
  if [ "$work" = "$_canon" ]; then
    bad 'AC3: the symlinked fixture did not produce a path that differs from its canonical form — the canonicalisation is not actually exercised, so a PASS below would be vacuous'
  else
    ok "AC3 control: the fixture path differs from its canonical form ($work vs $_canon), reproducing the macOS /var -> /private/var split"
  fi

  probe_case_f() { # probe_case_f <label> <expect: accept|reject> <recorded invocation line>
    local label="$2" expect="$3" line="$4" p0=$PASS f0=$FAIL dp df
    INVOKED="$tmp/invoked-$1.txt"
    printf '%s\n' "$line" >"$INVOKED"
    eval "$_cf_block" >/dev/null 2>&1
    dp=$((PASS - p0)); df=$((FAIL - f0))
    PASS=$p0; FAIL=$f0
    case "$expect" in
      accept)
        if [ "$dp" -eq 2 ] && [ "$df" -eq 0 ]; then
          ok "AC3: $label is ACCEPTED by the real case (f) asserts"
        else
          bad "AC3: $label should be accepted but the real asserts reported $df failure(s) / $dp pass(es)"
        fi ;;
      reject)
        if [ "$df" -eq 2 ] && [ "$dp" -eq 0 ]; then
          ok "AC3: $label is REJECTED by both real case (f) asserts"
        else
          bad "AC3: $label was NOT rejected by both asserts ($df failure(s) / $dp pass(es)) — the canonicalisation fix loosened the pinned contract"
        fi ;;
    esac
  }

  _tail='--agent codex --model gpt-5.6-sol --wait'
  probe_case_f sanctioned 'the sanctioned canonical --repo record' accept \
    "review --branch --base origin/main --repo $_canon $_tail"
  probe_case_f relative 'a RELATIVE --repo' reject \
    "review --branch --base origin/main --repo . $_tail"
  probe_case_f rootco 'a ROOT-CHECKOUT --repo' reject \
    "review --branch --base origin/main --repo $REPO_ROOT $_tail"
  probe_case_f norepo 'a --branch review with NO --repo at all' reject \
    "review --branch --base origin/main $_tail"
fi

# ===========================================================================
# (4) AC5: the #3262 exit-code propagation is NOT weakened — `GUARD-TEST RESULT: FAIL`
# still exits non-zero. The fail-open that returned 0 on a FAIL verdict is what hid three
# of the #3296 failures for weeks, and it is the more expensive of the two defects.
#
# Exercised on the guard test's OWN prologue + OWN tally epilogue (both read out of the real
# file), with one injected failing case in between. Composing it rather than re-running all
# 581 cases keeps this component fast; the text under test is the real text either way.
# ===========================================================================
_pro_end=$(grep -nF "trap 'rm -rf \"\$tmp\"' EXIT" "$GUARD" | head -1 | cut -d: -f1)
_epi_start=$(grep -nF '==== ROBOREV REVIEW GUARD TEST TALLY' "$GUARD" | head -1 | cut -d: -f1)
if [ -z "$_pro_end" ] || [ -z "$_epi_start" ] || [ "$_epi_start" -le "$_pro_end" ]; then
  bad "AC5: could not locate the guard test's prologue/tally epilogue (prologue end '$_pro_end', epilogue start '$_epi_start') — the exit-code contract is untested here"
else
  # The composition is laid out in a MIRROR of the real tree (scripts/tests + scripts/flow with
  # a placeholder wrapper), because the guard test's prologue resolves $SCRIPT_DIR from $0 and
  # exits 1 when ../flow/roborev-review.sh is missing — a composition run from a bare temp dir
  # would fail for that reason instead of the one under test.
  mkdir -p "$tmp/mirror/tests" "$tmp/mirror/flow"
  printf '#!/usr/bin/env bash\n# placeholder: the AC5 composition never invokes the wrapper.\n' \
    >"$tmp/mirror/flow/roborev-review.sh"
  compose_probe() { # compose_probe <out> <middle line>
    {
      awk -v n="$_pro_end" 'NR <= n' "$GUARD"
      printf '%s\n' "$2"
      awk -v n="$_epi_start" 'NR >= n' "$GUARD"
    } >"$1"
  }
  compose_probe "$tmp/mirror/tests/ac5-fail.sh" "bad 'AC5 injected failure'"
  compose_probe "$tmp/mirror/tests/ac5-pass.sh" ":"
  _ac5_out=$(bash "$tmp/mirror/tests/ac5-fail.sh" 2>&1); _ac5_rc=$?
  if [ "$_ac5_rc" -ne 0 ] && printf '%s\n' "$_ac5_out" | grep -qF 'GUARD-TEST RESULT: FAIL' &&
    printf '%s\n' "$_ac5_out" | grep -qF 'failed: 1'; then
    ok "AC5: a failing case still exits NON-ZERO (rc $_ac5_rc) with GUARD-TEST RESULT: FAIL"
  else
    bad "AC5: the guard test's tally epilogue did not fail closed (rc $_ac5_rc): $(printf '%s' "$_ac5_out" | tail -3 | tr '\n' ' ')"
  fi
  _ac5_out=$(bash "$tmp/mirror/tests/ac5-pass.sh" 2>&1); _ac5_rc=$?
  if [ "$_ac5_rc" -eq 0 ] && printf '%s\n' "$_ac5_out" | grep -qF 'GUARD-TEST RESULT: PASS'; then
    ok 'AC5 control: with no failing case the same epilogue exits 0 with GUARD-TEST RESULT: PASS'
  else
    bad "AC5 control: the clean composition did not pass (rc $_ac5_rc) — the injected-failure result above would then mean nothing"
  fi
fi

# The gate must not swallow either script's exit status: `roborev-lints` is where both run in
# --lite and in the full gate of record.
if [ ! -f "$GATE" ]; then
  printf 'SKIP - agent-gate.sh not found; the roborev-lints wiring could not be checked\n'
else
  _lints=$(awk '/^run_roborev_lints_cmd\(\) \{/,/^\}$/' "$GATE")
  if [ -z "$_lints" ]; then
    bad 'wiring: run_roborev_lints_cmd is not defined in agent-gate.sh — this test may not be gate-run at all'
  else
    for _needed in test_roborev_review_guard.sh test_roborev_guard_portability.sh; do
      if printf '%s\n' "$_lints" | grep -qF "$_needed"; then
        ok "wiring: roborev-lints runs $_needed"
      else
        bad "wiring: roborev-lints does not run $_needed — a regression would not FAIL the fast loop"
      fi
    done
    if printf '%s\n' "$_lints" | grep -qE '\|\|[[:space:]]*(true|:)'; then
      bad 'wiring: run_roborev_lints_cmd suppresses a non-zero exit (|| true) — that is the #3262 fail-open again, one level up'
    else
      ok 'wiring: run_roborev_lints_cmd propagates a non-zero exit (no || true)'
    fi
  fi
fi

# The tally line deliberately does NOT start with `RESULT:` — that token belongs to the agent
# gate's summary contract and to the roborev wrapper's own block.
printf '\n==== ROBOREV GUARD PORTABILITY TALLY ====\n'
printf 'passed: %d  failed: %d\n' "$PASS" "$FAIL"
if [ "$FAIL" -ne 0 ]; then
  printf 'PORTABILITY RESULT: FAIL\n'
  exit 1
fi
printf 'PORTABILITY RESULT: PASS\n'
