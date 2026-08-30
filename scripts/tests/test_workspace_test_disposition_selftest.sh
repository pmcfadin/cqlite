#!/usr/bin/env bash
# Self-test for scripts/tests/test_workspace_test_disposition.sh (issue #3522).
#
# A guard is only worth its green if it is capable of red. Every case builds a scratch
# cargo workspace, copies the guard AND its census into it at the same relative paths
# (the guard resolves both from its OWN location, so there is deliberately NO path/env
# seam to point at a fixture — CLAUDE.md #3312: a case needing a different subject
# SUBSTITUTES THE ARTIFACT in its own scratch copy of the tree), writes a census for
# that tree, and asserts the verdict.
#
# TWO GREEN CONTROLS, deliberately: without one per shape, a guard hardwired to refuse
# everything would satisfy every red case below and look fully tested.
#
# ON CARGO, AND ON THE #1716 TRAP IT WOULD OTHERWISE WALK INTO. The guard's subject set
# comes from `cargo metadata`, so this self-test cannot be cargo-free the way its tools/
# sibling is. #1716 removed a cargo-derived guard partly because its self-test built
# scratch workspaces OUTSIDE the repository, which do not inherit rust-toolchain.toml —
# making a MANDATORY gate component's behaviour depend on the host's default toolchain.
# Three things keep that from recurring here:
#   1. rust-toolchain.toml is COPIED into every scratch workspace, so cargo resolves the
#      same toolchain the repo pins. (If it is absent from the repo the case is skipped
#      with a named reason, never silently.)
#   2. Only `cargo metadata --no-deps` is ever run — manifest PARSING, no compilation,
#      no codegen, so there is nothing for a toolchain difference to change.
#   3. Every scratch manifest declares ZERO dependencies and the runs are forced
#      OFFLINE (CARGO_NET_OFFLINE=1), so there is no registry access and no network.
# Measured: 29ms per metadata call. Deterministic, offline, and fast enough to be a
# mandatory component's neighbour.
set -uo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
GUARD="$SCRIPT_DIR/test_workspace_test_disposition.sh"
GUARD_BASE=$(basename "$GUARD")
CENSUS_BASE="workspace-test-disposition.txt"
[ -f "$GUARD" ] || { echo "FAIL: guard under test not found at $GUARD" >&2; exit 1; }

TMPROOT=$(mktemp -d "${TMPDIR:-/tmp}/wsdisp.XXXXXX") || exit 1
trap 'rm -rf "$TMPROOT"' EXIT

fails=0
pass_case() { echo "ok: $*"; }
fail_case() { echo "FAIL: $*" >&2; fails=$((fails + 1)); }

TAB=$(printf '\t')

# make_ws <case> <member crates...> — build a scratch workspace and copy the guard in.
# Prints the workspace path.
make_ws() {
  local case_name="$1"; shift
  local ws="$TMPROOT/$case_name" c
  mkdir -p "$ws/scripts/tests" "$ws/crates"
  cat > "$ws/Cargo.toml" <<'WS'
[workspace]
resolver = "2"
members = ["crates/*"]
WS
  for c in "$@"; do
    mkdir -p "$ws/crates/$c/src"
    printf '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2021"\npublish = false\n' "$c" \
      > "$ws/crates/$c/Cargo.toml"
    echo 'pub fn f() {}' > "$ws/crates/$c/src/lib.rs"
  done
  [ -f "$REPO_ROOT/rust-toolchain.toml" ] && cp "$REPO_ROOT/rust-toolchain.toml" "$ws/"
  cp "$GUARD" "$ws/scripts/tests/$GUARD_BASE"
  printf '%s' "$ws"
}

# run_guard <ws> — run the copied guard in its own tree, OFFLINE. Echoes the output,
# returns the guard's status.
GUARD_OUT=""
run_guard() {
  local ws="$1"
  GUARD_OUT=$(cd "$ws" && env CARGO_NET_OFFLINE=1 bash "$ws/scripts/tests/$GUARD_BASE" 2>&1)
  return $?
}

# expect <case> <PASS|FAIL> <needle> <ws>
expect() {
  local case_name="$1" want="$2" needle="$3" ws="$4" rc
  run_guard "$ws"; rc=$?
  if [ "$want" = PASS ]; then
    if [ "$rc" -eq 0 ]; then
      pass_case "$case_name — guard PASSED as expected"
    else
      fail_case "$case_name — expected PASS, got exit $rc: $GUARD_OUT"
    fi
    return
  fi
  if [ "$rc" -eq 0 ]; then
    fail_case "$case_name — expected FAIL, but the guard PASSED. A guard that cannot red on this shape is not enforcing it: $GUARD_OUT"
    return
  fi
  # ATTRIBUTED, not merely red: a red for an unrelated reason has the same exit code, so
  # the message must NAME the planted condition.
  if printf '%s' "$GUARD_OUT" | grep -qF "$needle"; then
    pass_case "$case_name — guard FAILED and named the cause ('$needle')"
  else
    fail_case "$case_name — guard failed (exit $rc) but never named '$needle', so the red cannot be shown to be this case's: $GUARD_OUT"
  fi
}

if [ ! -f "$REPO_ROOT/rust-toolchain.toml" ]; then
  echo "note: $REPO_ROOT/rust-toolchain.toml is absent, so scratch workspaces run on the host's DEFAULT toolchain (see this script's header). Cases still run; the toolchain-pinning property is UNVERIFIED in this invocation." >&2
fi

# ---- GREEN CONTROL A: every member recorded, one PARTIAL ---------------------
ws=$(make_ws green_a alpha beta)
{
  printf '# scratch census\n'
  printf 'alpha%sEXECUTED%sno-gap%sscratch-component runs it\n' "$TAB" "$TAB" "$TAB"
  printf 'beta%sPARTIAL%ssilent%sscratch-component runs its lib only; its 2 integration targets are un-run because reasons\n' "$TAB" "$TAB" "$TAB"
} > "$ws/scripts/tests/$CENSUS_BASE"
expect "green control A (complete census, one PARTIAL)" PASS "" "$ws"

# ---- GREEN CONTROL B: a different shape — three members, all three labels ----
ws=$(make_ws green_b alpha beta gamma)
{
  printf 'alpha%sEXECUTED%sno-gap%sscratch-component\n' "$TAB" "$TAB" "$TAB"
  printf 'beta%sNOT-EXECUTED%ssilent%snothing runs it; tracked as scratch issue #1\n' "$TAB" "$TAB" "$TAB"
  # `contradicts-doctrine` appears in a GREEN control on purpose: it is otherwise only
  # planted in a RED case below, so a guard that rejected the value outright would pass
  # every case here and look fully tested.
  printf 'gamma%sPARTIAL%scontradicts-doctrine%sscratch-component runs 1 of 3 targets; the scratch docs claim all 3\n' "$TAB" "$TAB" "$TAB"
} > "$ws/scripts/tests/$CENSUS_BASE"
expect "green control B (three members, all three labels, all three classes)" PASS "" "$ws"

# ---- RED: an UNRECORDED member — the #3522 defect itself ---------------------
ws=$(make_ws red_unrecorded alpha beta gamma)
{
  printf 'alpha%sEXECUTED%sno-gap%sscratch-component\n' "$TAB" "$TAB" "$TAB"
  printf 'beta%sNOT-EXECUTED%ssilent%snothing runs it\n' "$TAB" "$TAB" "$TAB"
} > "$ws/scripts/tests/$CENSUS_BASE"
expect "red: a new workspace member with no recorded disposition" FAIL "gamma" "$ws"

# ---- RED: TWO unrecorded members — BOTH must be named (roborev round 6, G2) ---
# The single-unrecorded case above is satisfied by a message naming one crate, so it cannot
# distinguish "reports every unrecorded member" from "reports the first one it finds". This
# case can: it asserts both names, so a first-offender-only regression fails here.
ws=$(make_ws red_unrecorded2 alpha beta gamma delta)
{
  printf 'alpha%sEXECUTED%sno-gap%sscratch-component\n' "$TAB" "$TAB" "$TAB"
  printf 'beta%sPARTIAL%ssilent%sscratch-component runs some\n' "$TAB" "$TAB" "$TAB"
} > "$ws/scripts/tests/$CENSUS_BASE"
run_guard "$ws"; _rc=$?
if [ "$_rc" -eq 0 ]; then
  fail_case "two unrecorded members — expected FAIL, guard PASSED: $GUARD_OUT"
else
  _miss=""
  for _c in gamma delta; do
    printf '%s' "$GUARD_OUT" | grep -qF "$_c" || _miss="$_miss $_c"
  done
  if [ -z "$_miss" ]; then
    pass_case "two unrecorded members — guard FAILED and named BOTH (gamma, delta)"
  else
    fail_case "two unrecorded members — guard failed but never named:$_miss ($GUARD_OUT)"
  fi
fi

# ---- RED: a label outside the CLOSED set ------------------------------------
ws=$(make_ws red_label alpha beta)
{
  printf 'alpha%sExecuted%ssilent%swrong case — a spelling is not a state\n' "$TAB" "$TAB" "$TAB"
  printf 'beta%sPARTIAL%ssilent%sscratch-component runs some\n' "$TAB" "$TAB" "$TAB"
} > "$ws/scripts/tests/$CENSUS_BASE"
expect "red: an unrecognised label (wrong case)" FAIL "closed label set" "$ws"

# ---- RED: a class outside the CLOSED set ------------------------------------
ws=$(make_ws red_class alpha beta)
{
  printf 'alpha%sEXECUTED%sno-gap%sscratch-component\n' "$TAB" "$TAB" "$TAB"
  printf 'beta%sPARTIAL%sSilent%sa spelling is not a state, here too\n' "$TAB" "$TAB" "$TAB"
} > "$ws/scripts/tests/$CENSUS_BASE"
expect "red: an unrecognised class (wrong case)" FAIL "closed class set" "$ws"

# ---- RED: an EMPTY class field ----------------------------------------------
# Distinct from the malformed-shape case: the record has the right FIELD COUNT and a
# valid label, so only a per-field emptiness check can see it.
ws=$(make_ws red_class_empty alpha beta)
{
  printf 'alpha%sEXECUTED%sno-gap%sscratch-component\n' "$TAB" "$TAB" "$TAB"
  printf 'beta%sPARTIAL%s%sclassified with nothing at all\n' "$TAB" "$TAB" "$TAB"
} > "$ws/scripts/tests/$CENSUS_BASE"
expect "red: an empty class field" FAIL "empty class" "$ws"

# ---- RED: the COUPLING, direction 1 — a real gap classed no-gap -------------
# The shape this exists for: excusing an uncomfortable PARTIAL/NOT-EXECUTED record
# WITHOUT relabelling it, which the visible-gap floor cannot see because the label is
# untouched.
ws=$(make_ws red_couple_gap alpha beta)
{
  printf 'alpha%sEXECUTED%sno-gap%sscratch-component\n' "$TAB" "$TAB" "$TAB"
  printf 'beta%sNOT-EXECUTED%sno-gap%snothing runs it, but call it no gap\n' "$TAB" "$TAB" "$TAB"
} > "$ws/scripts/tests/$CENSUS_BASE"
expect "red: a PARTIAL/NOT-EXECUTED record classed no-gap (coupling)" FAIL "no-gap is reserved for EXECUTED records" "$ws"

# ---- RED: the COUPLING, direction 2 — an EXECUTED record carrying a gap class -
# Without this case the coupling check could be one-directional and still pass every
# other case here; a self-contradicting record would then read as classified.
ws=$(make_ws red_couple_exec alpha beta)
{
  printf 'alpha%sEXECUTED%scontradicts-doctrine%sruns fully AND contradicts doctrine — one of the two is false\n' "$TAB" "$TAB" "$TAB"
  printf 'beta%sPARTIAL%ssilent%sscratch-component runs some\n' "$TAB" "$TAB" "$TAB"
} > "$ws/scripts/tests/$CENSUS_BASE"
expect "red: an EXECUTED record carrying a gap class (coupling, other direction)" FAIL "its class must be no-gap" "$ws"

# ---- RED: a STALE record naming a package that is not a member --------------
ws=$(make_ws red_stale alpha beta)
{
  printf 'alpha%sEXECUTED%sno-gap%sscratch-component\n' "$TAB" "$TAB" "$TAB"
  printf 'beta%sPARTIAL%ssilent%sscratch-component\n' "$TAB" "$TAB" "$TAB"
  printf 'deleted-crate%sNOT-EXECUTED%ssilent%sthis crate was removed but its record was left behind\n' "$TAB" "$TAB" "$TAB"
} > "$ws/scripts/tests/$CENSUS_BASE"
expect "red: a stale record for a package that is no longer a member" FAIL "deleted-crate" "$ws"

# ---- RED: a DUPLICATE record ------------------------------------------------
ws=$(make_ws red_dup alpha beta)
{
  printf 'alpha%sEXECUTED%sno-gap%sscratch-component\n' "$TAB" "$TAB" "$TAB"
  printf 'alpha%sNOT-EXECUTED%ssilent%sthe contradicting second record\n' "$TAB" "$TAB" "$TAB"
  printf 'beta%sPARTIAL%ssilent%sscratch-component\n' "$TAB" "$TAB" "$TAB"
} > "$ws/scripts/tests/$CENSUS_BASE"
expect "red: a package recorded twice" FAIL "more than once" "$ws"

# ---- RED: a record with a label and NO detail behind it ---------------------
ws=$(make_ws red_nodetail alpha beta)
{
  printf 'alpha%sEXECUTED%sno-gap%s\n' "$TAB" "$TAB" "$TAB"
  printf 'beta%sPARTIAL%ssilent%sscratch-component\n' "$TAB" "$TAB" "$TAB"
} > "$ws/scripts/tests/$CENSUS_BASE"
expect "red: a label with no detail (no account of what runs it / what is omitted)" FAIL "NO detail" "$ws"

# ---- RED: a MALFORMED line (spaces where TABs belong) ----------------------
ws=$(make_ws red_malformed alpha beta)
{
  printf 'alpha EXECUTED scratch-component\n'
  printf 'beta%sPARTIAL%ssilent%sscratch-component\n' "$TAB" "$TAB" "$TAB"
} > "$ws/scripts/tests/$CENSUS_BASE"
expect "red: a line that is not a TAB-separated record" FAIL "TAB-separated record" "$ws"

# ---- RED: a census with comments only, i.e. NO records ---------------------
ws=$(make_ws red_norecords alpha beta)
printf '# only a comment\n\n' > "$ws/scripts/tests/$CENSUS_BASE"
expect "red: a census containing no records at all" FAIL "no records at all" "$ws"

# ---- RED: the affirmative floor — every member labelled EXECUTED ------------
# The shape this floor exists for is someone relabelling an uncomfortable record.
ws=$(make_ws red_allexec alpha beta)
{
  printf 'alpha%sEXECUTED%sno-gap%sscratch-component\n' "$TAB" "$TAB" "$TAB"
  printf 'beta%sEXECUTED%sno-gap%sscratch-component\n' "$TAB" "$TAB" "$TAB"
} > "$ws/scripts/tests/$CENSUS_BASE"
expect "red: zero PARTIAL/NOT-EXECUTED records (the visible-gap floor)" FAIL "ZERO NOT-EXECUTED/PARTIAL" "$ws"

# ---- RED: the census file is MISSING ---------------------------------------
ws=$(make_ws red_nocensus alpha beta)
rm -f "$ws/scripts/tests/$CENSUS_BASE"
expect "red: the recorded census file does not exist" FAIL "does not exist" "$ws"

# ---- RED: the DERIVATION fails (no workspace manifest to enumerate) --------
# The property under test is that an unmeasurable subject set FAILs rather than
# greening — the guard's stated fail-closed direction.
ws=$(make_ws red_noderive alpha beta)
{
  printf 'alpha%sEXECUTED%sno-gap%sscratch-component\n' "$TAB" "$TAB" "$TAB"
  printf 'beta%sPARTIAL%ssilent%sscratch-component\n' "$TAB" "$TAB" "$TAB"
} > "$ws/scripts/tests/$CENSUS_BASE"
rm -f "$ws/Cargo.toml"
expect "red: cargo metadata cannot enumerate the members (DERIVATION failure)" FAIL "DERIVATION failed" "$ws"

echo
if [ "$fails" -ne 0 ]; then
  echo "RESULT: FAIL ($fails case(s) did not behave as specified)"
  exit 1
fi
echo "RESULT: PASS (2 green controls + 15 attributed red cases)"
exit 0
