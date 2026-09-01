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
# THE FIXTURE reproduces, in miniature, every shape the real workspace has that the
# guard has been wrong about (roborev job 50): a two-level aggregator, an optional
# dependency, a RENAMED workspace dependency (`bee = { package = "b" }`, so a
# cross-member edge can only be resolved through the dependency KEY), a weak edge
# (`optdep?/odfeat`) both standalone and alongside its activation, a build script, and
# a NESTED MEMBER whose package directory is the outer member's own `tests/` directory
# — which is exactly where `cqlite-integration-tests` sits relative to the root
# package. Path dependencies only, so `cargo metadata --no-deps` runs offline in
# milliseconds and nothing is compiled.
#
# Cases (the case count is asserted EXACTLY at the end — see the CASE COUNT note):
#   1.  GREEN  — the base fixture certifies, printing an affirmative EXACT count.
#   2.  RED    — THE INCIDENT CLASS: a dead leaf named ONLY by an aggregator. Credit
#                must not flow DOWN from the parent; the guard must name `deadleaf`.
#   3.  GREEN  — a legitimate aggregator, two levels deep, whose leaves have effects.
#   4.  GREEN  — a feature whose ONLY effect is a target `required-features`
#                (`duckdb-tests`' real shape).
#   5.  RED    — the differential: drop that `required-features` and the same feature
#                must be reported dead. Proves criterion E3 is what passed case 4.
#   6.  RED    — empty the dep list of the optional-dependency feature and it must be
#                reported dead. Proves criterion E2 is what credited it in case 1.
#   7.  RED    — the `default` exemption is by NAME and nothing wider: the SAME
#                effect-free feature under any other name must be reported dead.
#   8.  RED    — DERIVATION FAILURE: an unparsable manifest must FAIL naming
#                `cargo metadata`, and must NOT print the success line. Never a
#                vacuous pass over an empty feature set.
#   9.  RED    — a reference site that exists only inside a COMMENT confers no credit.
#  10.  RED    — the cross-member edge, from the far side: break the effect in the
#                other member and BOTH its leaf and the forwarder die. The fixture's
#                edge is written through a RENAMED dependency key (`bee/bfeat`), so
#                this is also the differential for job 50 finding 3 — resolving edges
#                by PACKAGE NAME classifies `bee` as external and auto-credits it,
#                which passes this fixture and must not.
#  11.  GREEN  — a cargo-IMPLICIT feature (an `optional = true` dependency never named
#                with `dep:`) is SEEN and credited: the asserted count rises by one.
#  12.  RED    — job 50 finding 1: `feature = "x"` in a STRING LITERAL, in `//!`/`///`
#                DOC TEXT, and in a `cfg_attr` ATTRIBUTE TAIL confers no credit. The
#                real instance: `arbitrary_precision` appears in
#                cqlite-ffi-common/src/json_number.rs only inside doc comments.
#  13.  GREEN  — a genuine `CARGO_FEATURE_<NAME>` env read in the package's BUILD
#                SCRIPT is an effect (it is how a build script sees a feature).
#  14.  RED    — the differential pair for 13, both halves: a merely TEXTUAL
#                `CARGO_FEATURE_X` in build.rs, and a real env read in a NON-build
#                script source, each confer no credit.
#  15.  RED    — job 50 finding 2: a STANDALONE weak edge (`optdep?/odfeat`) is not an
#                effect. Cargo does nothing with it unless the optional dependency is
#                activated by something else.
#  16.  GREEN  — the over-strictness half of 15 (the false-FAIL direction): the same
#                weak edge ALONGSIDE its activation (`["dep:optdep", "optdep?/odfeat"]`)
#                stays credited. NOTE the isolating green for a live weak edge is
#                UNTESTABLE by construction: activating the dependency is ITSELF an
#                effect (E2), so a live weak edge can never be a feature's sole effect.
#                The real-workspace shape this protects is
#                `observability-testing = ["observability", "opentelemetry_sdk?/testing"]`.
#  17.  GREEN  — job 50 finding 4: a file that is the outer member's OWN test-target
#                source while sitting INSIDE the nested member's package directory
#                credits the OUTER member. Measured differential: the pre-fix
#                directory-prefix ownership reports this fixture's `tfeat` DEAD — a
#                false FAIL — while the target-derived ownership certifies it.
#  18.  RED    — the other half of finding 4: the nested member's OWN lib site does
#                not credit the outer member's same-named feature. Credit must not leak
#                across the package boundary in either direction.
#  19.  USAGE  — an unrecognized argument exits 2 (repo convention).
#  20.  GREEN  — --help exits 0 and documents that there is no opt-out.
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
build_fixture() { # <dir>
  local d="$1"
  mkdir -p "$d/scripts/ci" "$d/a/src" "$d/a/tests/src" "$d/b/src" "$d/optdep/src"
  cp "$GUARD" "$d/scripts/ci/check-features-load-bearing.sh"
  chmod +x "$d/scripts/ci/check-features-load-bearing.sh"

  # `a/tests` is BOTH the outer member `a`'s test directory AND the package directory
  # of the nested member `nest` — the overlap the real workspace has between the root
  # package's tests/ and cqlite-integration-tests.
  cat >"$d/Cargo.toml" <<'EOF'
[workspace]
members = ["a", "b", "optdep", "a/tests"]
resolver = "2"
EOF

  cat >"$d/optdep/Cargo.toml" <<'EOF'
[package]
name = "optdep"
version = "0.0.0"
edition = "2021"

[features]
odfeat = []
EOF
  cat >"$d/optdep/src/lib.rs" <<'EOF'
#[cfg(feature = "odfeat")]
pub fn od() {}
EOF

  cat >"$d/a/Cargo.toml" <<'EOF'
[package]
name = "a"
version = "0.0.0"
edition = "2021"
build = "build.rs"

[dependencies]
optdep = { path = "../optdep", optional = true }
bee = { path = "../b", package = "b" }

[features]
default = []
agg = ["aggmid"]
aggmid = ["leafx", "leafy"]
leafx = []
leafy = ["dep:optdep"]
rfonly = []
fwd = ["bee/bfeat"]
tfeat = []
wkok = ["dep:optdep", "optdep?/odfeat"]

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
  cat >"$d/a/build.rs" <<'EOF'
fn main() {}
EOF
  # `tfeat`'s ONLY site: a's own test-target file, which lives inside the nested
  # member's package directory.
  cat >"$d/a/tests/t.rs" <<'EOF'
#[cfg(feature = "tfeat")]
#[test]
fn t() {}
EOF

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

  cat >"$d/a/tests/Cargo.toml" <<'EOF'
[package]
name = "nest"
version = "0.0.0"
edition = "2021"

[lib]
name = "nest"
path = "src/lib.rs"

[features]
nfeat = []
EOF
  cat >"$d/a/tests/src/lib.rs" <<'EOF'
#[cfg(feature = "nfeat")]
pub fn nested_only() {}
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
# EXACT, not a floor: the fixture declares 13 features — a's default, agg, aggmid,
# leafx, leafy, rfonly, fwd, tfeat, wkok; b's default, bfeat; optdep's odfeat; nest's
# nfeat — of which the two `default`s are exempt, leaving 11 asserted. Asserting the
# exact number is what catches a counting drift (a member silently not enumerated, an
# exemption silently widening, an ownership change losing a whole tree).
BASE_EXPECTED=11
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

# --- 4. GREEN / 5. RED: required-features is an effect -------------------------
D="$(fixture rf-green)"
expect_green "$D" "case 4"
ok "a feature whose only effect is a target required-features certifies"

D="$(fixture rf-red)"
sed -i '/^required-features = \["rfonly"\]$/d' "$D/a/Cargo.toml"
grep -q 'required-features' "$D/a/Cargo.toml" && fail_case "case 5: fixture edit did not remove required-features"
expect_red_naming "$D" "rfonly" "case 5"
ok "removing that required-features kills the same feature (E3 is what passed case 4)"

# --- 6. RED: an optional dependency is an effect -------------------------------
D="$(fixture dep-red)"
sed -i 's/^leafy = \["dep:optdep"\]$/leafy = []/' "$D/a/Cargo.toml"
expect_red_naming "$D" "leafy" "case 6"
ok "a feature whose only effect was an optional dependency dies when that dep is dropped"

# --- 7. RED: the `default` exemption is by NAME only --------------------------
D="$(fixture default-red)"
# Rename the exempt, effect-free `default` to a non-exempt name. Nothing else changes.
sed -i 's/^default = \[\]$/notdefault = []/' "$D/b/Cargo.toml"
expect_red_naming "$D" "notdefault" "case 7"
ok "an effect-free feature named anything other than \`default\` is reported dead"

# --- 8. RED: derivation failure fails CLOSED ---------------------------------
D="$(fixture broken-manifest)"
printf 'this is not = = valid toml [[[\n' >>"$D/a/Cargo.toml"
expect_red_naming "$D" "cargo metadata" "case 8"
grep -q 'declared features load-bearing' "$TMPROOT/out.txt" \
  && fail_case "case 8: printed a success line despite a failed derivation"
ok "an unparsable manifest FAILS naming the derivation, never a vacuous pass"

# --- 9. RED: a comment-only reference site confers no credit -------------------
D="$(fixture comment-only)"
cat >"$D/a/src/lib.rs" <<'EOF'
// #[cfg(feature = "leafx")]
/* #[cfg(feature = "leafx")] */
pub fn always() {}
EOF
expect_red_naming "$D" "leafx" "case 9"
ok "a reference site that exists only inside a comment confers no credit"

# --- 10. RED: the cross-member edge, resolved through a RENAMED dep key --------
D="$(fixture transitive)"
cat >"$D/b/src/lib.rs" <<'EOF'
pub fn always() {}
EOF
run_guard "$D" && fail_case "case 10: expected a FAIL once member b lost its only reference site"
grep -q 'bfeat' "$TMPROOT/out.txt" || { cat "$TMPROOT/out.txt"; fail_case "case 10: did not name bfeat"; }
grep -q 'fwd' "$TMPROOT/out.txt" || { cat "$TMPROOT/out.txt"; fail_case "case 10: did not name fwd, the forwarder that depended on it — resolving \`bee/bfeat\` by PACKAGE NAME would classify it as external and auto-credit it"; }
ok "breaking an effect in another member kills both the leaf and the forwarder that reaches it through a RENAMED dependency key"

# --- 11. GREEN: a cargo-IMPLICIT feature is seen and credited -----------------
D="$(fixture implicit)"
# `optional = true` with no `dep:` reference anywhere: cargo synthesises an implicit
# feature `implicitdep` that no [features] block contains. It must be COUNTED (a
# textual manifest sweep cannot see it) and credited (it enables an optional dep).
sed -i 's|^bee = { path = "../b", package = "b" }$|bee = { path = "../b", package = "b" }\nimplicitdep = { path = "../optdep", package = "optdep", optional = true }|' "$D/a/Cargo.toml"
expect_green "$D" "case 11"
IMPL_COUNT="$(asserted_count)"
[ "$IMPL_COUNT" -eq "$((BASE_COUNT + 1))" ] \
  || fail_case "case 11: expected the asserted count to rise from $BASE_COUNT to $((BASE_COUNT + 1)) for the cargo-implicit feature, got $IMPL_COUNT"
ok "a cargo-implicit feature from an optional dependency is seen ($IMPL_COUNT = $BASE_COUNT + 1) and credited"

# --- 12. RED: string literals, doc text and a cfg_attr TAIL are not sites -----
D="$(fixture not-a-cfg)"
cat >"$D/a/src/lib.rs" <<'EOF'
//! Enable with feature = "leafx" to turn the fast path on.
/// See `feature = "leafx"`.
#[cfg_attr(unix, doc = "feature = \"leafx\"")]
pub fn always() {
    let hint = "feature = \"leafx\"";
    let _ = hint.len();
}
EOF
expect_red_naming "$D" "leafx" "case 12"
ok "a feature named in a string literal, in doc text, or in a cfg_attr attribute TAIL confers no credit"

# --- 13. GREEN / 14. RED: CARGO_FEATURE_* env reads in a build script ---------
D="$(fixture buildscript-green)"
cat >"$D/a/build.rs" <<'EOF'
fn main() {
    if std::env::var("CARGO_FEATURE_TFEAT").is_ok() {
        println!("cargo:rustc-cfg=has_tfeat");
    }
}
EOF
# Remove tfeat's cfg site so the build-script read is its ONLY effect.
cat >"$D/a/tests/t.rs" <<'EOF'
#[test]
fn t() {}
EOF
expect_green "$D" "case 13"
ok "a genuine CARGO_FEATURE_<NAME> env read in the package's build script is an effect"

D="$(fixture buildscript-red)"
cat >"$D/a/build.rs" <<'EOF'
fn main() {
    // Documents CARGO_FEATURE_TFEAT without ever reading it.
    let name = "CARGO_FEATURE_TFEAT";
    let _ = name.len();
}
EOF
cat >"$D/a/tests/t.rs" <<'EOF'
#[test]
fn t() {}
EOF
expect_red_naming "$D" "tfeat" "case 14a"
D="$(fixture buildscript-red-wrong-file)"
cat >"$D/a/src/lib.rs" <<'EOF'
#[cfg(feature = "leafx")]
pub fn leafx_only() {}

pub fn probe() -> bool {
    std::env::var("CARGO_FEATURE_TFEAT").is_ok()
}
EOF
cat >"$D/a/tests/t.rs" <<'EOF'
#[test]
fn t() {}
EOF
expect_red_naming "$D" "tfeat" "case 14b"
ok "a merely textual CARGO_FEATURE_X in build.rs, and a real env read OUTSIDE a build script, each confer no credit"

# --- 15. RED / 16. GREEN: weak dependency edges ------------------------------
D="$(fixture weak-red)"
sed -i 's/^tfeat = \[\]$/tfeat = []\nwkdead = ["optdep?\/odfeat"]/' "$D/a/Cargo.toml"
grep -q '^wkdead = ' "$D/a/Cargo.toml" || fail_case "case 15: fixture edit did not plant wkdead"
expect_red_naming "$D" "wkdead" "case 15"
ok "a STANDALONE weak edge (\`optdep?/odfeat\`) is not an effect — nothing activates the optional dependency"

D="$(fixture weak-green)"
grep -q '^wkok = \["dep:optdep", "optdep?/odfeat"\]$' "$D/a/Cargo.toml" \
  || fail_case "case 16: the base fixture no longer carries the live weak-edge shape this case exists for"
expect_green "$D" "case 16"
ok "the same weak edge ALONGSIDE its activation stays credited (the false-FAIL direction)"

# --- 17. GREEN / 18. RED: overlapping package directories --------------------
# `a/tests/t.rs` is a's OWN test-target source and sits inside the nested member
# `nest`'s package directory. Under directory-prefix ownership it belonged to `nest`,
# so a's `tfeat` — whose only site it is — was reported DEAD. Measured against the
# pre-fix guard, this exact fixture reds; case 1 already proves it now certifies, and
# this case pins the attribution explicitly so a regression names it.
D="$(fixture overlap-green)"
expect_green "$D" "case 17"
grep -q 'tfeat' "$TMPROOT/out.txt" \
  && fail_case "case 17: the success line should not be naming features at all"
ok "a file that is the OUTER member's own test-target source, inside the NESTED member's package dir, credits the outer member"

D="$(fixture overlap-red)"
# a declares a feature with the SAME NAME as the nested member's, and no site of its own.
sed -i 's/^tfeat = \[\]$/tfeat = []\nnfeat = []/' "$D/a/Cargo.toml"
grep -q '^nfeat = \[\]$' "$D/a/Cargo.toml" || fail_case "case 18: fixture edit did not plant a's nfeat"
expect_red_naming "$D" "nfeat" "case 18"
grep -q '\[a\]' "$TMPROOT/out.txt" \
  || { cat "$TMPROOT/out.txt"; fail_case "case 18: the dead feature was not attributed to member \`a\`"; }
ok "the nested member's own lib site does not credit the outer member's same-named feature"

# --- 19. USAGE: an unrecognized argument exits 2 ------------------------------
set +e
bash "$GUARD" --bypass >"$TMPROOT/out.txt" 2>&1
rc=$?
set -e
[ "$rc" -eq 2 ] || { cat "$TMPROOT/out.txt"; fail_case "case 19: expected exit 2 for an unrecognized argument, got $rc"; }
ok "an unrecognized argument exits 2"

# --- 20. GREEN: --help documents that there is no opt-out ---------------------
bash "$GUARD" --help >"$TMPROOT/out.txt" 2>&1 || fail_case "case 20: --help did not exit 0"
grep -qi 'no bypass flag' "$TMPROOT/out.txt" \
  || fail_case "case 20: --help does not state that there is no bypass flag / no environment opt-out"
ok "--help exits 0 and states there is no bypass flag and no environment opt-out"

# --- CASE COUNT: EXACT, not a floor ------------------------------------------
# #3544's lesson is this suite's own subject: a span-replacing edit once deleted four
# cases from a suite and it reported "failed: 0" over the shrunken remainder. A FLOOR
# below the real count tolerates exactly that — one case can be deleted and the guard
# still greens (roborev job 50, finding 5) — so the count is pinned EXACTLY. Adding a
# case means changing this number in the same diff, deliberately.
CASE_COUNT_EXPECTED=20
[ "$CASES" -eq "$CASE_COUNT_EXPECTED" ] \
  || fail_case "CASE COUNT: $CASES cases ran, expected EXACTLY $CASE_COUNT_EXPECTED. Cases were deleted, skipped or added without updating this assertion; a green tally over a changed suite certifies nothing."

echo "PASS: $CASES cases (exact count $CASE_COUNT_EXPECTED)"
