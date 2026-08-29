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

# make_ws <case> <wired-list> <unwired-list> <on-disk crates> <readme spec>
#   on-disk crates: space-separated dir names to create under tools/
#   readme spec   : space-separated `crate:kind`, kind = labeled | unlabeled
#                   (crates not named get no README at all)
#   a literal "-" for the wired/unwired list means "empty list"
make_ws() {
  local case_name="$1" wired="$2" unwired="$3" disk="$4" readmes="${5:-}"
  local ws="$TMPROOT/$case_name" c spec crate kind
  mkdir -p "$ws/scripts/tests"
  for c in $disk; do
    mkdir -p "$ws/tools/$c/src"
    printf '[package]\nname = "scratch-%s"\nversion = "0.1.0"\nedition = "2021"\n' "$c" > "$ws/tools/$c/Cargo.toml"
  done
  for spec in $readmes; do
    crate="${spec%%:*}"; kind="${spec##*:}"
    if [ "$kind" = labeled ]; then
      printf '# scratch %s\n\nThis crate is NOT CI-wired.\n' "$crate" > "$ws/tools/$crate/README.md"
    else
      printf '# scratch %s\n\nSome prose that never says whether anything runs it.\n' "$crate" > "$ws/tools/$crate/README.md"
    fi
  done
  [ "$wired" = "-" ] && wired=""
  [ "$unwired" = "-" ] && unwired=""
  awk -v w="$wired" -v u="$unwired" '
    /^WIRED_TOOLS="/   { print "WIRED_TOOLS=\"" w "\"";   skip=1; next }
    /^UNWIRED_TOOLS="/ { print "UNWIRED_TOOLS=\"" u "\""; skip=1; next }
    skip && /"$/ { skip=0; next }
    skip { next }
    { print }
  ' "$GUARD" > "$ws/scripts/tests/$GUARD_BASE"
  chmod +x "$ws/scripts/tests/$GUARD_BASE"
  echo "$ws"
}

run_guard() { bash "$1/scripts/tests/$GUARD_BASE" 2>&1; }

# --- CASE 1 (GREEN CONTROL) ---------------------------------------------------
ws=$(make_ws green 'wiredone' 'orphanone' 'wiredone orphanone' 'orphanone:labeled')
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
  "is in NEITHER recorded list" \
  newcomer 'wiredone' 'orphanone' 'wiredone orphanone newcomer' 'orphanone:labeled'

# 3. an unwired crate with no README at all => not labeled.
expect_red "unwired crate with no README" \
  "has no README.md" \
  noreadme 'wiredone' 'orphanone' 'wiredone orphanone' ''

# 4. an unwired crate whose README exists but never states the fact.
expect_red "unwired crate README missing the label marker" \
  "does not contain the label" \
  unlabeled 'wiredone' 'orphanone' 'wiredone orphanone' 'orphanone:unlabeled'

# 5. a recorded crate deleted from disk without updating the list.
expect_red "recorded crate removed from disk" \
  "was renamed or removed without updating" \
  ghost 'wiredone
ghostcrate' 'orphanone' 'wiredone orphanone' 'orphanone:labeled'

# 6. a crate recorded as both wired and unwired.
expect_red "crate recorded in BOTH lists" \
  "recorded as BOTH wired and unwired" \
  both 'wiredone
orphanone' 'orphanone' 'wiredone orphanone' 'orphanone:labeled'

# 7. an EMPTY unwired list would make the label requirement enforce nothing.
expect_red "empty UNWIRED_TOOLS list" \
  "refusing to pass vacuously" \
  emptyunwired 'wiredone' '-' 'wiredone' ''

# 8. no tools/ directory at all => the guard's subject is absent.
ws=$(make_ws notools 'wiredone' 'orphanone' 'wiredone orphanone' 'orphanone:labeled')
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
echo "PASS: tools/ crate disposition self-test (#1716) — 1 green control + 7 negative controls"
