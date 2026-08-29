#!/usr/bin/env bash
# Self-test for scripts/tests/test_tools_crate_disposition.sh (issue #1716).
#
# A guard is only worth its green if it is capable of red. Every case builds a
# scratch tools/ tree, copies the guard into it at the same relative path (the
# guard resolves its root from its OWN location, so there is deliberately no
# path/env seam to point at a fixture — CLAUDE.md #3312: a case needing a
# different subject SUBSTITUTES THE ARTIFACT in its own scratch copy of the
# tree), rewrites the two recorded lists for that tree, and asserts the verdict.
#
# Includes a GREEN control: without it, a guard hardwired to refuse everything
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

# make_ws <case> <wired-list> <unwired-list> <mixed-list> <on-disk crates> <readme spec>
#   on-disk crates: space-separated dir names to create under tools/
#   readme spec   : space-separated `crate:kind`, kind =
#                     labeled   -> says "NOT CI-wired" only (correct for UNWIRED)
#                     mixed     -> says "NOT CI-wired" AND "WIRED" (correct for MIXED)
#                     unlabeled -> says neither
#                   (crates not named get no README at all)
#   a literal "-" for any list means "empty list"
make_ws() {
  local case_name="$1" wired="$2" unwired="$3" mixed="$4" disk="$5" readmes="${6:-}"
  local ws="$TMPROOT/$case_name" c spec crate kind
  mkdir -p "$ws/scripts/tests"
  for c in $disk; do
    mkdir -p "$ws/tools/$c/src"
    printf '[package]\nname = "scratch-%s"\nversion = "0.1.0"\nedition = "2021"\n' "$c" > "$ws/tools/$c/Cargo.toml"
  done
  for spec in $readmes; do
    crate="${spec%%:*}"; kind="${spec##*:}"
    case "$kind" in
      labeled) printf '# scratch %s\n\nThis crate is NOT CI-wired.\n' "$crate" > "$ws/tools/$crate/README.md" ;;
      mixed)   printf '# scratch %s\n\nIts lib is WIRED; its binaries are NOT CI-wired.\n' "$crate" > "$ws/tools/$crate/README.md" ;;
      *)       printf '# scratch %s\n\nSome prose that never says whether anything runs it.\n' "$crate" > "$ws/tools/$crate/README.md" ;;
    esac
  done
  [ "$wired" = "-" ] && wired=""
  [ "$unwired" = "-" ] && unwired=""
  [ "$mixed" = "-" ] && mixed=""
  # Substitute the three recorded lists for this scratch tree's crates.
  #
  # `skip` must NOT be armed for a SELF-CONTAINED assignment. A single-line list
  # such as `MIXED_TOOLS="format-validator"` both opens and closes its string on
  # one line, so arming skip made the rewriter swallow the NEXT `"`-terminated
  # line — which is `LABEL_MARKER=...`, leaving every scratch guard with an unbound
  # variable. That produced a green-control failure rather than a wrong verdict, so
  # it surfaced immediately; the two-quote test below is what makes it correct.
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

# --- CASE 1 (GREEN CONTROL) ---------------------------------------------------
ws=$(make_ws green 'wiredone' 'orphanone' '-' 'wiredone orphanone' 'orphanone:labeled')
out=$(run_guard "$ws"); rc=$?
if [ $rc -eq 0 ] && grep -q '^PASS:' <<<"$out"; then
  pass_case "GREEN control: a correctly-classified tools/ tree PASSes (guard is not hardwired to refuse)"
else
  fail_case "GREEN control did NOT pass (rc=$rc):"; printf '%s\n' "$out" | sed 's/^/    /' >&2
fi

# --- red cases ---------------------------------------------------------------
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

# 2. a NEW crate arrives via the tools/* glob with no recorded disposition.
expect_red "new unclassified tools/ crate" \
  "is in NONE of the three recorded lists" \
  newcomer 'wiredone' 'orphanone' '-' 'wiredone orphanone newcomer' 'orphanone:labeled'

# 3. an unwired crate with no README at all => not labeled.
expect_red "unwired crate with no README" \
  "has no README.md" \
  noreadme 'wiredone' 'orphanone' '-' 'wiredone orphanone' ''

# 4. an unwired crate whose README exists but never states the fact.
expect_red "unwired crate README missing the label marker" \
  "does not contain 'NOT CI-wired'" \
  unlabeled 'wiredone' 'orphanone' '-' 'wiredone orphanone' 'orphanone:unlabeled'

# 5. a recorded crate deleted from disk without updating the list.
expect_red "recorded crate removed from disk" \
  "was renamed or removed without updating" \
  ghost 'wiredone
ghostcrate' 'orphanone' '-' 'wiredone orphanone' 'orphanone:labeled'

# 6. a crate recorded as both wired and unwired.
expect_red "crate recorded in BOTH WIRED and UNWIRED" \
  "recorded in BOTH WIRED_TOOLS and UNWIRED_TOOLS" \
  both 'wiredone
orphanone' 'orphanone' '-' 'wiredone orphanone' 'orphanone:labeled'

# 7. an EMPTY unwired list would make the label requirement enforce nothing.
expect_red "empty UNWIRED_TOOLS and MIXED_TOOLS lists" \
  "refusing to pass vacuously" \
  emptyunwired 'wiredone' '-' '-' 'wiredone' ''

# --- MIXED-category cases (roborev job 75: a two-way split let a partly-live
# --- crate be recorded as wholly unwired, asserting something untrue about it).

# 7b. GREEN control for MIXED: a correctly-labeled mixed crate must PASS. Without
#     this, cases 7c-7e below could all be satisfied by a guard that simply
#     rejects every MIXED crate.
ws=$(make_ws mixedgreen 'wiredone' '-' 'mixedone' 'wiredone mixedone' 'mixedone:mixed')
out=$(run_guard "$ws"); rc=$?
if [ $rc -eq 0 ] && grep -q '^PASS:' <<<"$out"; then
  pass_case "GREEN control (MIXED): a correctly-labeled mixed crate PASSes"
else
  fail_case "GREEN control (MIXED) did NOT pass (rc=$rc):"; printf '%s\n' "$out" | sed 's/^/    /' >&2
fi

# 7c. THE ROBOREV DEFECT ITSELF: a mixed crate whose README says only
#     "NOT CI-wired" and never names its live half reads as wholly dead.
expect_red "MIXED crate labeled as if wholly unwired (roborev job 75)" \
  "reads as wholly dead" \
  mixedhalf 'wiredone' '-' 'mixedone' 'wiredone mixedone' 'mixedone:labeled'

# 7d. a mixed crate with no README at all still owes a label.
expect_red "MIXED crate with no README" \
  "has no README.md" \
  mixednoreadme 'wiredone' '-' 'mixedone' 'wiredone mixedone' ''

# 7e. disjointness must hold for the NEW pairs too, not just wired-vs-unwired.
expect_red "crate recorded in BOTH UNWIRED and MIXED" \
  "recorded in BOTH UNWIRED_TOOLS and MIXED_TOOLS" \
  bothum 'wiredone' 'mixedone' 'mixedone' 'wiredone mixedone' 'mixedone:mixed'
expect_red "crate recorded in BOTH WIRED and MIXED" \
  "recorded in BOTH WIRED_TOOLS and MIXED_TOOLS" \
  bothwm 'wiredone
mixedone' '-' 'mixedone' 'wiredone mixedone' 'mixedone:mixed'

# 7f. MIXED alone must satisfy the label-bearing floor (an empty UNWIRED is fine
#     when MIXED is populated) — asserted via the GREEN case 7b above, and here
#     that the floor is keyed on the UNION, not on UNWIRED alone.

# 8. no tools/ directory at all => the guard's subject is absent.
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

echo
if [ "$fails" -ne 0 ]; then
  echo "FAIL: $fails tools/ disposition self-test case(s) failed" >&2
  exit 1
fi
echo "PASS: tools/ crate disposition self-test (#1716) — 2 green controls + 11 negative controls"
