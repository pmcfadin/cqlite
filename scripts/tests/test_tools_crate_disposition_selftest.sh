#!/usr/bin/env bash
# Self-test for scripts/tests/test_tools_crate_disposition.sh (issue #1716).
#
# A guard is only worth its green if it is capable of red. Every case builds a
# scratch tools/ tree, copies the guard into it at the same relative path (the guard
# resolves its root from its OWN location, so there is deliberately NO path/env seam
# to point at a fixture — CLAUDE.md #3312: a case needing a different subject
# SUBSTITUTES THE ARTIFACT in its own scratch copy of the tree), rewrites the three
# recorded lists for that tree, and asserts the verdict.
#
# NO CARGO ANYWHERE. An earlier version built scratch cargo workspaces to exercise a
# dependency-derivation half of the guard that has since been removed. Those
# workspaces lived OUTSIDE the repository, so they did not inherit
# rust-toolchain.toml, which made a MANDATORY gate component's behaviour depend on
# the host's default toolchain (roborev job 86). The guard is now filesystem-and-list
# only, so this self-test is too: deterministic, offline, toolchain-independent.
#
# TWO GREEN CONTROLS: without one per shape, a guard hardwired to refuse everything
# would satisfy every red case below and look fully tested.
set -uo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
GUARD="$SCRIPT_DIR/test_tools_crate_disposition.sh"
GUARD_BASE=$(basename "$GUARD")
[ -f "$GUARD" ] || { echo "FAIL: guard under test not found at $GUARD" >&2; exit 1; }

TMPROOT=$(mktemp -d "${TMPDIR:-/tmp}/toolsdisp.XXXXXX") || exit 1
trap 'rm -rf "$TMPROOT"' EXIT

fails=0
pass_case() { echo "ok: $*"; }
fail_case() { echo "FAIL: $*" >&2; fails=$((fails + 1)); }

# make_ws <case> <wired> <unwired> <mixed> <disk crates> [readme spec]
#   disk crates : space-separated dir names to create under tools/
#   readme spec : space-separated `crate:kind`, kind =
#                   labeled   -> contains "NOT CI-wired"
#                   unlabeled -> a README that never states the fact
#                 (crates not named get no README at all)
#   a literal "-" for any list means "empty list"
make_ws() {
  local case_name="$1" wired="$2" unwired="$3" mixed="$4" disk="$5" readmes="${6:-}"
  local ws="$TMPROOT/$case_name" c spec crate kind
  mkdir -p "$ws/scripts/tests"
  for c in $disk; do
    mkdir -p "$ws/tools/$c"
    printf '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2021"\npublish = false\n' "$c" \
      > "$ws/tools/$c/Cargo.toml"
  done
  for spec in $readmes; do
    crate="${spec%%:*}"; kind="${spec##*:}"
    case "$kind" in
      labeled) printf '# scratch %s\n\nThis crate is NOT CI-wired.\n' "$crate" ;;
      *)       printf '# scratch %s\n\nProse that never says whether anything runs it.\n' "$crate" ;;
    esac > "$ws/tools/$crate/README.md"
  done
  [ "$wired" = "-" ] && wired=""
  [ "$unwired" = "-" ] && unwired=""
  [ "$mixed" = "-" ] && mixed=""
  # Substitute the three recorded lists for this scratch tree's crates.
  #
  # `skip` must NOT be armed for a SELF-CONTAINED assignment. A single-line list
  # such as `MIXED_TOOLS="format-validator"` both opens and closes its string on one
  # line, so arming skip-until-quote made the rewriter swallow the NEXT
  # `"`-terminated line — `LABEL_MARKER=` — leaving every scratch guard with an
  # unbound variable. It broke the GREEN control rather than producing a wrong
  # verdict, which is why the green controls exist.
  awk -v w="$wired" -v u="$unwired" -v m="$mixed" '
    function multiline(line) { return gsub(/"/, "\"", line) < 2 }
    /^WIRED_TOOLS="/   { print "WIRED_TOOLS=\"" w "\"";   skip=multiline($0); next }
    /^UNWIRED_TOOLS="/ { print "UNWIRED_TOOLS=\"" u "\""; skip=multiline($0); next }
    /^MIXED_TOOLS="/   { print "MIXED_TOOLS=\"" m "\"";   skip=multiline($0); next }
    skip && /"$/ { skip=0; next }
    skip { next }
    { print }
  ' "$GUARD" > "$ws/scripts/tests/$GUARD_BASE"
  chmod +x "$ws/scripts/tests/$GUARD_BASE"
  echo "$ws"
}

run_guard() { bash "$1/scripts/tests/$GUARD_BASE" 2>&1; }

expect_green() {
  local label="$1"; shift
  local w o r
  w=$(make_ws "$@")
  o=$(run_guard "$w"); r=$?
  if [ $r -eq 0 ] && grep -q '^PASS:' <<<"$o"; then
    pass_case "$label"
  else
    fail_case "$label — did NOT pass (rc=$r):"; printf '%s\n' "$o" | sed 's/^/    /' >&2
  fi
}

expect_red() {
  local label="$1" needle="$2"; shift 2
  local w o r
  w=$(make_ws "$@")
  o=$(run_guard "$w"); r=$?
  if [ $r -eq 0 ]; then
    fail_case "$label: guard PASSED on a tree it must reject"
    printf '%s\n' "$o" | sed 's/^/    /' >&2
  elif ! grep -qF "$needle" <<<"$o"; then
    fail_case "$label: guard failed (good) but not for the expected reason (wanted '$needle')"
    printf '%s\n' "$o" | sed 's/^/    /' >&2
  else
    pass_case "$label: guard FAILs, naming the right cause"
  fi
}

# ============================ GREEN CONTROLS ================================
expect_green "GREEN 1: a correctly-classified WIRED+UNWIRED tree PASSes" \
  green 'wiredone' 'orphanone' '-' 'wiredone orphanone' 'orphanone:labeled'

expect_green "GREEN 2: a correctly-labeled MIXED crate PASSes" \
  mixedgreen 'wiredone' '-' 'mixedone' 'wiredone mixedone' 'mixedone:labeled'

# ============================== RED CASES ===================================
# --- classification completeness / consistency
expect_red "new unclassified tools/ crate" \
  "is in NONE of the three recorded lists" \
  newcomer 'wiredone' 'orphanone' '-' 'wiredone orphanone newcomer' 'orphanone:labeled'

expect_red "recorded crate removed from disk" \
  "was renamed or removed without updating" \
  ghost 'wiredone
ghostcrate' 'orphanone' '-' 'wiredone orphanone' 'orphanone:labeled'

expect_red "crate recorded in BOTH WIRED and UNWIRED" \
  "recorded in BOTH WIRED_TOOLS and UNWIRED_TOOLS" \
  bothwu 'wiredone
orphanone' 'orphanone' '-' 'wiredone orphanone' 'orphanone:labeled'

expect_red "crate recorded in BOTH WIRED and MIXED" \
  "recorded in BOTH WIRED_TOOLS and MIXED_TOOLS" \
  bothwm 'wiredone
mixedone' '-' 'mixedone' 'wiredone mixedone' 'mixedone:labeled'

expect_red "crate recorded in BOTH UNWIRED and MIXED" \
  "recorded in BOTH UNWIRED_TOOLS and MIXED_TOOLS" \
  bothum 'wiredone' 'mixedone' 'mixedone' 'wiredone mixedone' 'mixedone:labeled'

expect_red "empty UNWIRED_TOOLS and MIXED_TOOLS lists" \
  "refusing to pass vacuously" \
  emptyboth 'wiredone' '-' '-' 'wiredone' ''

expect_red "empty WIRED_TOOLS list" \
  "WIRED_TOOLS list is empty" \
  emptywired '-' 'orphanone' '-' 'orphanone' 'orphanone:labeled'

# --- the labeling half (#1716's actual acceptance criterion)
expect_red "UNWIRED crate with no README" \
  "has no README.md" \
  noreadme 'wiredone' 'orphanone' '-' 'wiredone orphanone' ''

expect_red "UNWIRED crate README missing the label marker" \
  "does not contain 'NOT CI-wired'" \
  unlabeled 'wiredone' 'orphanone' '-' 'wiredone orphanone' 'orphanone:unlabeled'

expect_red "MIXED crate with no README" \
  "has no README.md" \
  mixednoreadme 'wiredone' '-' 'mixedone' 'wiredone mixedone' ''

expect_red "MIXED crate README missing the label marker" \
  "does not contain 'NOT CI-wired'" \
  mixedunlabeled 'wiredone' '-' 'mixedone' 'wiredone mixedone' 'mixedone:unlabeled'

# --- fail-closed on an absent subject
ws=$(make_ws notools 'wiredone' 'orphanone' '-' 'wiredone orphanone' 'orphanone:labeled')
rm -rf "$ws/tools"
out=$(run_guard "$ws"); rc=$?
if [ $rc -eq 0 ]; then
  fail_case "absent tools/ directory: guard PASSED with no subject at all (vacuous pass)"
elif ! grep -qF "refusing to pass vacuously" <<<"$out"; then
  fail_case "absent tools/ directory: guard failed but not via the fail-closed path:"
  printf '%s\n' "$out" | sed 's/^/    /' >&2
else
  pass_case "absent tools/ directory: guard FAILs closed rather than passing vacuously"
fi

# --- an UNREADABLE README is unmeasurable, not unlabeled
ws=$(make_ws unreadable 'wiredone' 'orphanone' '-' 'wiredone orphanone' 'orphanone:labeled')
chmod 000 "$ws/tools/orphanone/README.md"
out=$(run_guard "$ws"); rc=$?
chmod 644 "$ws/tools/orphanone/README.md" 2>/dev/null || true
if [ "$(id -u)" = 0 ]; then
  pass_case "unreadable README: SKIPPED (running as root, which can read anything)"
elif [ $rc -eq 0 ]; then
  fail_case "unreadable README: guard PASSED though it could not verify the label"
  printf '%s\n' "$out" | sed 's/^/    /' >&2
elif ! grep -qF "unmeasurable is not a pass" <<<"$out"; then
  fail_case "unreadable README: guard failed but not via the fail-closed path:"
  printf '%s\n' "$out" | sed 's/^/    /' >&2
else
  pass_case "unreadable README: guard FAILs closed rather than treating it as unlabeled"
fi

echo
if [ "$fails" -ne 0 ]; then
  echo "FAIL: $fails tools/ disposition self-test case(s) failed" >&2
  exit 1
fi
echo "PASS: tools/ crate disposition self-test (#1716) — 2 green controls + 12 negative controls, no cargo"
