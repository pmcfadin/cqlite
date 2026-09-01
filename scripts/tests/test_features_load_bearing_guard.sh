#!/usr/bin/env bash
# test_features_load_bearing_guard.sh — self-test for the features-are-load-bearing
# guard, scripts/ci/check-features-load-bearing.sh (issue #1698, epic #1685).
#
# The point of this suite is to prove the guard FIRES, and fires for the RIGHT reason.
# A bare non-zero exit is NOT evidence: an unrelated silent abort produces the same
# exit code, so every negative case asserts that the diagnostic NAMES the planted
# feature (or names the failed derivation). And every criterion of the predicate is
# pinned by a PAIR — a green fixture plus the differential red obtained by removing
# only that criterion's effect — because a green alone is satisfiable by a guard that
# credits everything, and a red alone by a guard that credits nothing.
#
# NO TEST-ONLY SEAM. The guard's subject is the workspace it finds relative to its own
# location, and that is deliberate (an invoker-selectable subject is an invoker-
# selectable vacuous pass). So each case SUBSTITUTES THE ARTIFACT: a throwaway
# fixture workspace is built in $TMPDIR and the guard is COPIED into its
# scripts/ci/, exactly as it would sit in a real checkout. No path variable, no
# environment override — the guard has neither and must never grow one.
#
# The fixture is a tiny 3-member cargo workspace with PATH dependencies only, so
# `cargo metadata --no-deps` runs offline in milliseconds and nothing is compiled.
#
# Cases (case FLOOR asserted at the end — #3544: a span-replacing edit once silently
# deleted four cases and the suite reported "failed: 0" over a shrunken suite):
#   1.  GREEN  — the base fixture certifies, and prints an affirmative COUNT line.
#   2.  RED    — THE INCIDENT CLASS: a dead leaf named ONLY by an aggregator. Credit
#                must not flow DOWN from the parent; the guard must name `deadleaf`.
#   3.  GREEN  — a legitimate aggregator, two levels deep, whose leaves have effects.
#   4.  GREEN  — a feature whose ONLY effect is a target `required-features`
#                (`duckdb-tests`' real shape).
#   4b. RED    — the differential: drop that `required-features` and the same feature
#                must be reported dead. Proves criterion E3 is what passed case 4.
#   5.  GREEN  — a feature whose ONLY effect is enabling an optional dependency.
#   5b. RED    — the differential: empty its dep list and it must be reported dead.
#   6.  GREEN  — an empty `default = []` is legitimate (cargo defines its meaning).
#   6b. RED    — the differential: the SAME empty feature under any other name must be
#                reported dead. Proves the exemption is by NAME and nothing wider.
#   7.  RED    — DERIVATION FAILURE: an unparsable manifest must FAIL naming
#                `cargo metadata`, and must NOT print the success line. Never a
#                vacuous pass over an empty feature set.
#   8.  RED    — a reference site that exists only inside a COMMENT confers no credit.
#   9.  RED    — the transitive edge, from the other side: break the effect in the
#                OTHER workspace member and both the leaf and its forwarder die.
#  10.  GREEN  — a cargo-IMPLICIT feature (an `optional = true` dependency never named
#                with `dep:`) is SEEN and credited: the asserted count rises by one.
#  11.  USAGE  — an unrecognized argument exits 2 (repo convention).
#  12.  GREEN  — --help exits 0 and documents that there is no opt-out.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GUARD_REL="scripts/ci/check-features-load-bearing.sh"
GUARD="$REPO_ROOT/$GUARD_REL"

[ -f "$GUARD" ] || { echo "FAIL: guard script not found at $GUARD"; exit 1; }
command -v cargo >/dev/null 2>&1 || {
  echo "FAIL: cargo is not on PATH. This suite does not SKIP: the guard's only"
  echo "      derivation is cargo metadata, so a skipped run certifies nothing."
  exit 1
}

TMPROOT="$(mktemp -d "${TMPDIR:-/tmp}/flb-selftest.XXXXXX")"
cleanup() { rm -rf "$TMPROOT"; return 0; }
trap cleanup EXIT INT TERM HUP

CASES=0
fail_case() { echo "FAIL: $*"; exit 1; }
ok() { CASES=$((CASES + 1)); echo "  ok $CASES — $1"; }

# --- Fixture builder ----------------------------------------------------------
# Base fixture: every criterion of the predicate has exactly one representative, so a
# case can delete ONE effect and observe exactly one feature die.
build_fixture() { # <dir>
  local d="$1"
  mkdir -p "$d/scripts/ci" "$d/a/src" "$d/a/tests" "$d/b/src" "$d/optdep/src"
  cp "$GUARD" "$d/scripts/ci/check-features-load-bearing.sh"
  chmod +x "$d/scripts/ci/check-features-load-bearing.sh"

  cat >"$d/Cargo.toml" <<'EOF'
[workspace]
members = ["a", "b", "optdep"]
resolver = "2"
EOF

  cat >"$d/optdep/Cargo.toml" <<'EOF'
[package]
name = "optdep"
version = "0.0.0"
edition = "2021"
EOF
  echo "pub fn nothing() {}" >"$d/optdep/src/lib.rs"

  cat >"$d/a/Cargo.toml" <<'EOF'
[package]
name = "a"
version = "0.0.0"
edition = "2021"

[dependencies]
optdep = { path = "../optdep", optional = true }
b = { path = "../b" }

[features]
default = []
agg = ["aggmid"]
aggmid = ["leafx", "leafy"]
leafx = []
leafy = ["dep:optdep"]
rfonly = []
fwd = ["b/bfeat"]

[[test]]
name = "t"
path = "tests/t.rs"
required-features = ["rfonly"]
EOF
  cat >"$d/a/src/lib.rs" <<'EOF'
#[cfg(feature = "leafx")]
pub fn leafx_only() {}

pub fn always() {}
EOF
  echo "#[test] fn t() {}" >"$d/a/tests/t.rs"

  cat >"$d/b/Cargo.toml" <<'EOF'
[package]
name = "b"
version = "0.0.0"
edition = "2021"

[features]
default = []
bfeat = []
EOF
  cat >"$d/b/src/lib.rs" <<'EOF'
#[cfg(all(feature = "bfeat", not(test)))]
pub fn bfeat_only() {}
EOF
}

fixture() { # <name> -> echoes dir
  local d="$TMPROOT/$1"
  build_fixture "$d"
  echo "$d"
}

run_guard() { # <dir> ; captures combined output, returns guard rc
  local d="$1"
  ( cd "$d" && bash "$d/scripts/ci/check-features-load-bearing.sh" ) >"$TMPROOT/out.txt" 2>&1
}

expect_green() { # <dir> <label>
  if ! run_guard "$1"; then
    echo "--- guard output ---"; cat "$TMPROOT/out.txt"; echo "--------------------"
    fail_case "$2: expected the guard to PASS, it FAILED"
  fi
  grep -q 'declared features load-bearing' "$TMPROOT/out.txt" \
    || { cat "$TMPROOT/out.txt"; fail_case "$2: PASSED without printing its affirmative count line"; }
}

expect_red_naming() { # <dir> <needle> <label>
  if run_guard "$1"; then
    echo "--- guard output ---"; cat "$TMPROOT/out.txt"; echo "--------------------"
    fail_case "$3: expected the guard to FAIL, it PASSED"
  fi
  grep -q -- "$2" "$TMPROOT/out.txt" \
    || { cat "$TMPROOT/out.txt"; fail_case "$3: guard failed but its diagnostic never NAMED '$2' — a bare non-zero exit is not evidence"; }
}

asserted_count() { # reads the last successful output
  sed -n 's/^features-load-bearing: \([0-9]*\)\/.*/\1/p' "$TMPROOT/out.txt" | head -1
}

echo "test_features_load_bearing_guard.sh"

# --- 1. GREEN: the base fixture certifies -------------------------------------
D="$(fixture base)"
expect_green "$D" "case 1"
BASE_COUNT="$(asserted_count)"
# EXACT, not a floor: the fixture declares 9 features — a's default, agg, aggmid,
# leafx, leafy, rfonly, fwd and b's default, bfeat — of which the two `default`s are
# exempt, leaving 7 asserted. Asserting the exact number is what catches a counting
# drift (a member silently not enumerated, an exemption silently widening).
BASE_EXPECTED=7
[ -n "$BASE_COUNT" ] && [ "$BASE_COUNT" -eq "$BASE_EXPECTED" ] \
  || fail_case "case 1: the affirmative line reported '$BASE_COUNT' asserted features, expected exactly $BASE_EXPECTED"
ok "base fixture certifies, reporting $BASE_COUNT asserted features"

# --- 2. RED: a dead leaf named ONLY by an aggregator ---------------------------
D="$(fixture deadleaf)"
sed -i 's/^aggmid = \["leafx", "leafy"\]$/aggmid = ["leafx", "leafy", "deadleaf"]\ndeadleaf = []/' "$D/a/Cargo.toml"
grep -q '^deadleaf = \[\]$' "$D/a/Cargo.toml" || fail_case "case 2: fixture edit did not plant deadleaf"
expect_red_naming "$D" "deadleaf" "case 2"
ok "a dead leaf named only by an aggregator is reported dead (credit does not flow down)"

# --- 3. GREEN: a legitimate two-level aggregator -------------------------------
# The base fixture's `agg` -> `aggmid` -> {leafx, leafy} chain: neither aggregator has
# an effect of its own, and both must pass through the closure.
D="$(fixture aggregator)"
expect_green "$D" "case 3"
ok "a two-level aggregator whose leaves have effects certifies"

# --- 4. GREEN / 4b RED: required-features is an effect -------------------------
D="$(fixture rf-green)"
expect_green "$D" "case 4"
ok "a feature whose only effect is a target required-features certifies"

D="$(fixture rf-red)"
sed -i '/^required-features = \["rfonly"\]$/d' "$D/a/Cargo.toml"
grep -q 'required-features' "$D/a/Cargo.toml" && fail_case "case 4b: fixture edit did not remove required-features"
expect_red_naming "$D" "rfonly" "case 4b"
ok "removing that required-features kills the same feature (E3 is what passed case 4)"

# --- 5. GREEN / 5b RED: an optional dependency is an effect --------------------
D="$(fixture dep-red)"
sed -i 's/^leafy = \["dep:optdep"\]$/leafy = []/' "$D/a/Cargo.toml"
expect_red_naming "$D" "leafy" "case 5b"
ok "a feature whose only effect was an optional dependency dies when that dep is dropped"

# --- 6. GREEN / 6b RED: the `default` exemption is by NAME only ----------------
D="$(fixture default-red)"
# Rename the exempt, effect-free `default` to a non-exempt name. Nothing else changes.
sed -i 's/^default = \[\]$/notdefault = []/' "$D/b/Cargo.toml"
expect_red_naming "$D" "notdefault" "case 6b"
ok "an effect-free feature named anything other than \`default\` is reported dead"

# --- 7. RED: derivation failure fails CLOSED ----------------------------------
D="$(fixture broken-manifest)"
printf 'this is not = = valid toml [[[\n' >>"$D/a/Cargo.toml"
expect_red_naming "$D" "cargo metadata" "case 7"
grep -q 'declared features load-bearing' "$TMPROOT/out.txt" \
  && fail_case "case 7: printed a success line despite a failed derivation"
ok "an unparsable manifest FAILS naming the derivation, never a vacuous pass"

# --- 8. RED: a comment-only reference site confers no credit -------------------
D="$(fixture comment-only)"
cat >"$D/a/src/lib.rs" <<'EOF'
// #[cfg(feature = "leafx")]
/* #[cfg(feature = "leafx")] */
pub fn always() {}
EOF
expect_red_naming "$D" "leafx" "case 8"
ok "a reference site that exists only inside a comment confers no credit"

# --- 9. RED: the transitive edge, from the far side ---------------------------
D="$(fixture transitive)"
cat >"$D/b/src/lib.rs" <<'EOF'
pub fn always() {}
EOF
run_guard "$D" && fail_case "case 9: expected a FAIL once member b lost its only reference site"
grep -q 'bfeat' "$TMPROOT/out.txt" || { cat "$TMPROOT/out.txt"; fail_case "case 9: did not name bfeat"; }
grep -q 'fwd' "$TMPROOT/out.txt" || { cat "$TMPROOT/out.txt"; fail_case "case 9: did not name fwd, the forwarder that depended on it"; }
ok "breaking an effect in another member kills both the leaf and its cross-member forwarder"

# --- 10. GREEN: a cargo-IMPLICIT feature is seen and credited -----------------
D="$(fixture implicit)"
# `optional = true` with no `dep:` reference anywhere: cargo synthesises an implicit
# feature `implicitdep` that no [features] block contains. It must be COUNTED (a
# textual manifest sweep cannot see it) and credited (it enables an optional dep).
sed -i 's|^b = { path = "../b" }$|b = { path = "../b" }\nimplicitdep = { path = "../optdep", package = "optdep", optional = true }|' "$D/a/Cargo.toml"
expect_green "$D" "case 10"
IMPL_COUNT="$(asserted_count)"
[ "$IMPL_COUNT" -eq "$((BASE_COUNT + 1))" ] \
  || fail_case "case 10: expected the asserted count to rise from $BASE_COUNT to $((BASE_COUNT + 1)) for the cargo-implicit feature, got $IMPL_COUNT"
ok "a cargo-implicit feature from an optional dependency is seen ($IMPL_COUNT = $BASE_COUNT + 1) and credited"

# --- 11. USAGE: an unrecognized argument exits 2 ------------------------------
set +e
bash "$GUARD" --bypass >"$TMPROOT/out.txt" 2>&1
rc=$?
set -e
[ "$rc" -eq 2 ] || { cat "$TMPROOT/out.txt"; fail_case "case 11: expected exit 2 for an unrecognized argument, got $rc"; }
ok "an unrecognized argument exits 2"

# --- 12. GREEN: --help documents that there is no opt-out ---------------------
bash "$GUARD" --help >"$TMPROOT/out.txt" 2>&1 || fail_case "case 12: --help did not exit 0"
grep -qi 'no bypass flag' "$TMPROOT/out.txt" \
  || fail_case "case 12: --help does not state that there is no bypass flag / no environment opt-out"
ok "--help exits 0 and states there is no bypass flag and no environment opt-out"

# --- CASE FLOOR ---------------------------------------------------------------
# #3544: a span-replacing edit once deleted four cases from a suite and it reported
# "failed: 0" over the shrunken remainder. A green tally over fewer cases is not a
# green suite, so the floor is asserted rather than trusted.
CASE_FLOOR=12
[ "$CASES" -ge "$CASE_FLOOR" ] \
  || fail_case "CASE FLOOR: only $CASES cases ran, expected at least $CASE_FLOOR. Cases were deleted or skipped; a green tally over a shrunken suite is not a pass."

echo "PASS: $CASES cases (floor $CASE_FLOOR)"
