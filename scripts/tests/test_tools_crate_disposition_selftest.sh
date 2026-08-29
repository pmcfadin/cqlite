#!/usr/bin/env bash
# Self-test for scripts/tests/test_tools_crate_disposition.sh (issue #1716).
#
# A guard is only worth its green if it is capable of red. Every case builds a
# minimal, dependency-free scratch cargo workspace, copies the guard into it at
# the same relative path (the guard resolves its root from its OWN location, so
# there is deliberately NO path/env seam to point at a fixture — CLAUDE.md #3312:
# a case needing a different subject SUBSTITUTES THE ARTIFACT in its own scratch
# copy of the tree), rewrites the three recorded lists for that tree, and asserts
# the verdict.
#
# FIVE GREEN CONTROLS, not one. Without a green control per shape, a guard
# hardwired to refuse everything — or one that rejects every MIXED crate, or every
# crate with a real dependent — would satisfy all the red cases and look tested.
set -uo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
GUARD="$SCRIPT_DIR/test_tools_crate_disposition.sh"
GUARD_BASE=$(basename "$GUARD")
[ -f "$GUARD" ] || { echo "FAIL: guard under test not found at $GUARD" >&2; exit 1; }

export CARGO_NET_OFFLINE=1
TMPROOT=$(mktemp -d "${TMPDIR:-/tmp}/toolsdisp.XXXXXX") || exit 1
trap 'rm -rf "$TMPROOT"' EXIT

fails=0
pass_case() { echo "ok: $*"; }
fail_case() { echo "FAIL: $*" >&2; fails=$((fails + 1)); }

# make_ws <case> <wired> <unwired> <mixed> <disk crates> <readme spec> [consumer-of]
#   disk crates : space-separated dir names to create under tools/
#   readme spec : space-separated `crate:kind`, kind =
#                   labeled      -> says "NOT CI-wired" only
#                   namesdep     -> says "NOT CI-wired" AND names scratch-consumer
#                   genericwired -> says "NOT CI-wired" AND the bare word WIRED,
#                                   but NEVER names the dependent (roborev job 78)
#                   unlabeled    -> says neither
#                 (crates not named get no README at all)
#   consumer-of : if non-empty, create a `consumer` workspace member (package
#                 `scratch-consumer`) that path-depends on tools/<that crate>, so
#                 `cargo tree --workspace --invert` reports a REAL dependent.
#                 Accepts `<crate>` , `<crate>:optional` (dependency behind a
#                 non-default feature) or `<crate>:target` (dependency inside a
#                 `[target.'cfg(...)'.dependencies]` table). The latter two are the
#                 shapes `cargo tree` hides unless queried with `--all-features`
#                 and `--target all` (roborev job 79).
#   a literal "-" for any list means "empty list"
make_ws() {
  local case_name="$1" wired="$2" unwired="$3" mixed="$4" disk="$5" readmes="${6:-}" consumer_of="${7:-}"
  local ws="$TMPROOT/$case_name" c spec crate kind
  mkdir -p "$ws/src" "$ws/scripts/tests"
  for c in $disk; do
    mkdir -p "$ws/tools/$c/src"
    printf '[package]\nname = "scratch-%s"\nversion = "0.1.0"\nedition = "2021"\npublish = false\n' "$c" \
      > "$ws/tools/$c/Cargo.toml"
    : > "$ws/tools/$c/src/lib.rs"
  done
  {
    echo '[workspace]'
    if [ -n "$consumer_of" ]; then
      echo 'members = ["tools/*", "consumer"]'
    else
      echo 'members = ["tools/*"]'
    fi
    echo 'resolver = "2"'
    echo
    echo '[package]'
    echo 'name = "scratch-root"'
    echo 'version = "0.1.0"'
    echo 'edition = "2021"'
    echo 'publish = false'
  } > "$ws/Cargo.toml"
  : > "$ws/src/lib.rs"
  if [ -n "$consumer_of" ]; then
    local dep_crate="${consumer_of%%:*}" dep_kind="plain"
    case "$consumer_of" in *:*) dep_kind="${consumer_of##*:}" ;; esac
    mkdir -p "$ws/consumer/src"
    {
      printf '[package]\nname = "scratch-consumer"\nversion = "0.1.0"\nedition = "2021"\npublish = false\n\n'
      case "$dep_kind" in
        optional)
          # Behind a NON-DEFAULT feature: invisible to a default-feature resolve.
          printf '[features]\ndefault = []\nextra = ["scratch-%s"]\n\n' "$dep_crate"
          printf '[dependencies.scratch-%s]\npath = "../tools/%s"\noptional = true\n' "$dep_crate" "$dep_crate"
          ;;
        target)
          # Target-gated on a cfg that is NOT this host: invisible to a host-only
          # resolve. windows is chosen because the fleet is linux.
          printf '[target."cfg(windows)".dependencies.scratch-%s]\npath = "../tools/%s"\n' "$dep_crate" "$dep_crate"
          ;;
        *)
          printf '[dependencies.scratch-%s]\npath = "../tools/%s"\n' "$dep_crate" "$dep_crate"
          ;;
      esac
    } > "$ws/consumer/Cargo.toml"
    : > "$ws/consumer/src/lib.rs"
  fi
  for spec in $readmes; do
    crate="${spec%%:*}"; kind="${spec##*:}"
    case "$kind" in
      labeled)      printf '# scratch %s\n\nThis crate is NOT CI-wired.\n' "$crate" ;;
      namesdep)     printf '# scratch %s\n\nIts binaries are NOT CI-wired; its library is used by scratch-consumer.\n' "$crate" ;;
      genericwired) printf '# scratch %s\n\nPreviously WIRED, now entirely NOT CI-wired.\n' "$crate" ;;
      *)            printf '# scratch %s\n\nProse that never says whether anything runs it.\n' "$crate" ;;
    esac > "$ws/tools/$crate/README.md"
  done
  [ "$wired" = "-" ] && wired=""
  [ "$unwired" = "-" ] && unwired=""
  [ "$mixed" = "-" ] && mixed=""
  # Substitute the three recorded lists for this scratch tree's crates.
  #
  # `skip` must NOT be armed for a SELF-CONTAINED assignment. A single-line list
  # such as `MIXED_TOOLS="format-validator"` both opens and closes its string on
  # one line, so arming skip-until-quote made the rewriter swallow the NEXT
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

expect_green "GREEN 2 (MIXED): a real dependent + a README naming it PASSes" \
  mixedgreen 'wiredone' '-' 'mixedone' 'wiredone mixedone' 'mixedone:namesdep' 'mixedone'

expect_green "GREEN 3: UNWIRED alongside a consumer of a DIFFERENT crate PASSes" \
  unwiredgreen 'wiredone' 'orphanone' '-' 'wiredone orphanone' 'orphanone:labeled' 'wiredone'

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
mixedone' '-' 'mixedone' 'wiredone mixedone' 'mixedone:namesdep' 'mixedone'

expect_red "crate recorded in BOTH UNWIRED and MIXED" \
  "recorded in BOTH UNWIRED_TOOLS and MIXED_TOOLS" \
  bothum 'wiredone' 'mixedone' 'mixedone' 'wiredone mixedone' 'mixedone:namesdep' 'mixedone'

expect_red "empty UNWIRED_TOOLS and MIXED_TOOLS lists" \
  "refusing to pass vacuously" \
  emptyboth 'wiredone' '-' '-' 'wiredone' ''

# --- the labeling half (#1716's actual acceptance criterion)
expect_red "UNWIRED crate with no README" \
  "has no README.md" \
  noreadme 'wiredone' 'orphanone' '-' 'wiredone orphanone' ''

expect_red "UNWIRED crate README missing the label marker" \
  "does not contain 'NOT CI-wired'" \
  unlabeled 'wiredone' 'orphanone' '-' 'wiredone orphanone' 'orphanone:unlabeled'

expect_red "MIXED crate with no README" \
  "has no README.md" \
  mixednoreadme 'wiredone' '-' 'mixedone' 'wiredone mixedone' '' 'mixedone'

# --- THE DERIVED CROSS-CHECK: the census must match the MANIFESTS, both ways.
# roborev job 78: the MIXED label used to be verified by grepping for the generic
# word "WIRED", which this README satisfies while saying the OPPOSITE. The
# requirement is now to name the crate's ACTUAL, DERIVED dependent.
expect_red "MIXED README carries the word WIRED but never names its dependent (roborev job 78)" \
  "never mentions 'scratch-consumer'" \
  job78 'wiredone' '-' 'mixedone' 'wiredone mixedone' 'mixedone:genericwired' 'mixedone'

expect_red "MIXED recorded but NOTHING in the workspace depends on it" \
  "the 'live half' claim is unsupported" \
  mixednodep 'wiredone' '-' 'mixedone' 'wiredone mixedone' 'mixedone:namesdep'

expect_red "UNWIRED recorded but a workspace package DOES depend on it" \
  "that is a FALSE census" \
  unwireddep 'wiredone' 'orphanone' '-' 'wiredone orphanone' 'orphanone:labeled' 'orphanone'

# --- roborev job 79: a dependency behind an OPTIONAL FEATURE or a NON-HOST TARGET
# --- is invisible to a default `cargo tree` resolve, so an UNWIRED record would
# --- look correct while something really depends on the crate. These two cases
# --- fail unless the query carries BOTH --all-features and --target all.
expect_red "UNWIRED but an OPTIONAL-FEATURE dependency exists (roborev job 79)" \
  "that is a FALSE census" \
  optdep 'wiredone' 'orphanone' '-' 'wiredone orphanone' 'orphanone:labeled' 'orphanone:optional'

expect_red "UNWIRED but a NON-HOST TARGET dependency exists (roborev job 79)" \
  "that is a FALSE census" \
  targetdep 'wiredone' 'orphanone' '-' 'wiredone orphanone' 'orphanone:labeled' 'orphanone:target'

# ...and the same two shapes must SATISFY a MIXED record, so the widened query is
# shown to find real dependents rather than merely to reject more.
expect_green "GREEN 4 (MIXED via an OPTIONAL-FEATURE dependent) PASSes" \
  optmixed 'wiredone' '-' 'mixedone' 'wiredone mixedone' 'mixedone:namesdep' 'mixedone:optional'

expect_green "GREEN 5 (MIXED via a NON-HOST TARGET dependent) PASSes" \
  targetmixed 'wiredone' '-' 'mixedone' 'wiredone mixedone' 'mixedone:namesdep' 'mixedone:target'

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

echo
if [ "$fails" -ne 0 ]; then
  echo "FAIL: $fails tools/ disposition self-test case(s) failed" >&2
  exit 1
fi
echo "PASS: tools/ crate disposition self-test (#1716) — 5 green controls + 15 negative controls"
