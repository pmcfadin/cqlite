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
#  14.  GREEN  — SOUNDNESS, six spellings in one loop: an aliased import, a constant
#                argument, `env::vars()` iteration, a local wrapper, a `my_env` module
#                and a bare textual mention ALL credit. Each was, at some revision, not
#                recognised and its feature reported DEAD; the rule is now textual and
#                maximal, and the contract line declares it.
#  15.  GREEN  — a BARE `CARGO_FEATURE_` prefix (environment iteration names no
#                individual feature) credits EVERY feature of that package.
#  16.  RED    — a package with NO build script gets no env credit at all, even for a
#                real env read: cargo sets these variables for a build script's
#                execution, so without one nothing reads them.
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
#  18.  GREEN  — the other half of the overlap, in the direction that used to SUBTRACT:
#                a file under the NESTED member's directory credits the OUTER member
#                too. An outer target can reach such a file (`#[path]`, `include!`, a
#                shared helper) and this scan cannot trace that, so dropping the outer
#                owner reported a feature used only there as DEAD.
#  19.  USAGE  — an unrecognized argument exits 2 (repo convention).
#  20.  GREEN  — --help exits 0 and documents that there is no opt-out.
#
#   THE ROUND-2 CLASS (roborev job 52): the scanner used to match TEXT over Rust it did
#   not lexically understand. These pin the ONE lexical pass and the ANCHORED heads that
#   replaced it, each in the direction that was a false PASS:
#  21.  RED    — `doc(cfg(feature = "x"))` inside a `cfg_attr` TAIL confers nothing: it
#                is documentation, and the `cfg(` in it is not an anchored head.
#  22.  RED    — a RAW STRING (any hash count) containing a whole `#[cfg(feature = …)]`
#                attribute confers nothing. Its bytes never reach the code text.
#  24.  RED    — a REDUNDANT external dependency edge (`serde/derive` beside
#                `serde = { features = ["derive"] }`) confers nothing, with a
#                NON-redundant sibling edge (`serde/rc`) as the control that the
#                redundancy test is not simply refusing every external edge.
#  25.  RED    — the same for a WORKSPACE-member edge whose dependency declaration
#                already enables the forwarded feature.
#
#   ROUND 3 (roborev job 55):
#  27.  GREEN  — REGRESSION: a non-weak edge to an OPTIONAL dependency whose declaration
#                already enables the forwarded feature is STILL load-bearing, because the
#                edge ACTIVATES the dependency. Judging redundancy first reported a live
#                feature dead — a false FAIL, worse than the false PASS it came from.
#
#   ROUND 4 (roborev job 57) — THE CONTRACT. The guard is SOUND (never reports a live
#   feature dead) and explicitly INCOMPLETE (a dead feature can escape, and every known
#   route is declared on its own contract line). Cases 29-30 pin the SOUNDNESS direction;
#   31-32 pin declared escape routes, asserting BOTH that the behaviour occurs AND that
#   the contract line NAMES it.
#  29.  GREEN  — SOUNDNESS: a `CARGO_FEATURE_*` read in a build script's HELPER MODULE
#                (not build.rs itself) still credits its feature. Missing it reported a
#                live feature dead; the fix scans the whole package directory, which can
#                only over-credit, and the contract line says so.
#  30.  GREEN  — SOUNDNESS: a NON-MEMBER path dependency that shares a member's package
#                NAME, forwarding a feature only the real dependency declares, must not
#                be reported dead. Name-based resolution looked the feature up in the
#                MEMBER and refused; resolution is by canonical PATH.
#  32.  GREEN  — DECLARED: a `#[cfg(feature = ...)]` inside an UNEXPANDED `macro_rules!`
#                body credits its feature though no expansion applies it here.
#
#   ROUND 5 (roborev job 58) — MAKING THE PUBLISHED CONTRACT TRUE. It claimed soundness
#   and named macro-generated feature names as the SOLE false-failure limit; both were
#   false, and a false rationale in a gate log is worse than none. Cases 33-34 pin the two
#   soundness fixes, and 35 pins the ASYMMETRY they both rest on.
#  33.  GREEN  — SOUNDNESS: a module included from OUTSIDE any target root
#                (`#[path = "../gated.rs"]`) still credits its feature. No tree covered
#                it, so it had NO owner and its feature was reported dead.
#  34.  GREEN  — SOUNDNESS: a HELPER MODULE under the nested member's directory, reached
#                by the OUTER target, credits the outer member. Case 17 only covered an
#                exact target FILE, which is why this survived three rounds.
#  35.  GREEN  — THE ASYMMETRY ITSELF, the invariant everything rests on and the one a
#                refactor would most easily break: three differently-ambiguous files,
#                each the sole site of a distinct feature, must ALL credit. Asserted by
#                COUNT, so dropping any one of them reds this case.
#
#
#   ROUND 6 (roborev job 60) — THE CLAIM ITSELF. Three bounded recogniser fixes, the two
#   NOT-SEEN spellings the scoped claim names, and a case pinning the wording:
#  36.  GREEN  — SOUNDNESS: a `cfg` in a `cfg_attr` TAIL is a real gate
#                (`#[cfg_attr(unix, cfg(feature = "x"))]`); only the condition was read.
#  37.  GREEN  — SOUNDNESS: `# [cfg(...)]` and `# ! [cfg(...)]` — whitespace between `#`,
#                `!` and `[` is legal Rust and the head was not matching it.
#  38.  GREEN  — SOUNDNESS: `"\x66oo"` and `"\u{62}ar"` decode to `foo`/`bar`; recording
#                the undecoded text reported the real feature dead.
#  39.  GREEN  — DECLARED: an escape the scanner cannot decode credits EVERY feature of
#                the package rather than one wrong name.
#  40.  RED    — DECLARED LIMIT: a feature NAME produced by MACRO EXPANSION is NOT SEEN.
#                No lexical scan can see it; this is why the claim is scoped, not absolute.
#  41.  RED    — DECLARED LIMIT: a build-script env key CONSTRUCTED AT RUNTIME (joined
#                from fragments) is NOT SEEN. Also unresolvable lexically.
#  42.  GREEN  — THE CLAIM: the success output must state the SCOPED no-false-FAIL claim,
#                ENUMERATE the recognised spellings, say what is NOT SEEN — and must not
#                contain any form of the word "sound". The unqualified soundness claim was
#                tried and retracted after six rounds of witnesses; a false rationale in a
#                gate log is worse than none, so its return is a test failure.
#
#
#   ROUND 7 (roborev job 62) — gate-infrastructure and two bounded recogniser bugs:
#  43.  GREEN  — SOUNDNESS: `src/target/` and `src/fuzz/` are legitimate module
#                directories; pruning by BASENAME never scanned them, so a gate there was
#                reported dead. Pruning is now anchored.
#  44.  RED    — the control for 43: a `target/` BESIDE a Cargo.toml is cargo build
#                output and stays pruned, so a generated file cannot credit a feature.
#  45.  GREEN  — SOUNDNESS: a `cfg_attr` chain 40 levels deep — past the recursion bound
#                — CREDITS at the bound rather than dropping the gate.
#  46.  STRUCT  — the gate must invoke THIS SUITE behind a `command -v python3` guard with
#                a loud SKIP branch: it needs python3, it lives in the SKIP-aware
#                tooling-tests component, and invoking it unguarded turned a supported
#                SKIP into a red gate of record. Structural (it reads source), because
#                running that component takes ~35 minutes.
#
#   CASE NUMBERS ARE STABLE IDENTIFIERS, NOT POSITIONS — deleted cases leave gaps (the
#   convention scripts/tests/test_pub_surface_guard.sh already uses). The suite asserts
#   the exact NUMBER OF CASES RUN at the end, which is what catches a silent deletion.
#  28.  GREEN  — PINS A DECLARED RESIDUAL, not desired behaviour: an ORPHAN .rs file
#                under a target's source dir (reachable from no `mod` chain) IS scanned,
#                so a cfg gate in dead code credits its feature. The success line says
#                so; this case makes the declaration testable, because a declaration
#                nobody tests is a comment. If module-graph resolution is ever
#                implemented, this case reds and the residual text must change with it.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GUARD_REL="scripts/ci/check-features-load-bearing.sh"
GUARD="$REPO_ROOT/$GUARD_REL"

[ -f "$GUARD" ] || { echo "FAIL: guard script not found at $GUARD"; exit 1; }
# ONCE INVOKED, this suite never skips: the CALLER decides whether to run it (the
# SKIP-aware tooling-tests component skips it when python3 is absent, because the guard
# it exercises is python3-based and its subject cannot run at all there). A silent
# in-suite skip would certify nothing while reading green.
for tool in cargo python3; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "FAIL: $tool is not on PATH. It is a MANDATORY prerequisite of the guard under"
    echo "      test (cargo metadata is its only source of truth; python3 is its reader),"
    echo "      and this suite does not SKIP once invoked — a skipped run certifies"
    echo "      nothing. The caller is responsible for not invoking it on such a box."
    exit 1
  }
done

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

# A NON-MEMBER dependency, as a LOCAL PATH crate created OUTSIDE the fixture workspace
# (a sibling of it under $TMPROOT). Nothing here is fetched, so every case stays offline
# and deterministic — the property `tooling-tests` needs.
make_extdep() { # <fixture dir>
  local ext="$TMPROOT/extdep-$(basename "$1")"
  mkdir -p "$ext/src"
  cat >"$ext/Cargo.toml" <<'EOF'
[package]
name = "extdep"
version = "0.0.0"
edition = "2021"

[features]
derive = []
rc = []
EOF
  echo "pub fn nothing() {}" >"$ext/src/lib.rs"
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

# Every DECLARED escape route must be NAMED in the guard's own contract line, in the
# same run that exhibits it. A declaration nobody tests is a comment that rots, and a
# fixture that exhibits an undeclared behaviour is worse than either.
assert_contract_declares() { # <phrase> <label>
  grep -q 'CONTRACT: NO FALSE FAIL' "$TMPROOT/out.txt" \
    || { cat "$TMPROOT/out.txt"; fail_case "$2: the guard printed no CONTRACT line, so this behaviour is undeclared"; }
  grep -qF -- "$1" "$TMPROOT/out.txt" \
    || { cat "$TMPROOT/out.txt"; fail_case "$2: the CONTRACT line does not name the escape route this fixture exhibits ('$1')"; }
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

# --- 13. GREEN: a genuine env read in a build script -------------------------
D="$(fixture buildscript-green)"
cat >"$D/a/build.rs" <<'EOF'
fn main() {
    if std::env::var("CARGO_FEATURE_TFEAT").is_ok() {
        println!("cargo:rustc-cfg=has_tfeat");
    }
}
EOF
# Remove tfeat's cfg site so the build-script mention is its ONLY effect.
cat >"$D/a/tests/t.rs" <<'EOF'
#[test]
fn t() {}
EOF
expect_green "$D" "case 13"
ok "a CARGO_FEATURE_<NAME> env read in the package's build script is an effect"

# --- 14. GREEN (DECLARED): ANY textual form counts, in six spellings ---------
# The class review kept re-opening. Each of these was, at some revision, NOT recognised
# and its feature reported DEAD — a false FAIL, which the contract forbids. The rule is
# now textual and maximal, so all six credit, and the contract line says so.
for spelling in aliased-import constant-argument env-vars-iteration local-wrapper my-env-module bare-string-mention; do
  D="$(fixture "textual-$spelling")"
  cat >"$D/a/tests/t.rs" <<'EOF'
#[test]
fn t() {}
EOF
  case "$spelling" in
    aliased-import)
      cat >"$D/a/build.rs" <<'EOF'
use std::env::var as get_var;

fn main() {
    if get_var("CARGO_FEATURE_TFEAT").is_ok() {
        println!("cargo:rustc-cfg=has_tfeat");
    }
}
EOF
      ;;
    constant-argument)
      cat >"$D/a/build.rs" <<'EOF'
const KEY: &str = "CARGO_FEATURE_TFEAT";

fn main() {
    if std::env::var(KEY).is_ok() {
        println!("cargo:rustc-cfg=has_tfeat");
    }
}
EOF
      ;;
    env-vars-iteration)
      cat >"$D/a/build.rs" <<'EOF'
fn main() {
    for (key, _value) in std::env::vars() {
        if key == "CARGO_FEATURE_TFEAT" {
            println!("cargo:rustc-cfg=has_tfeat");
        }
    }
}
EOF
      ;;
    local-wrapper)
      cat >"$D/a/build.rs" <<'EOF'
fn var(key: &str) -> Option<String> { std::env::var(key).ok() }

fn main() {
    if var("CARGO_FEATURE_TFEAT").is_some() {
        println!("cargo:rustc-cfg=has_tfeat");
    }
}
EOF
      ;;
    my-env-module)
      cat >"$D/a/build.rs" <<'EOF'
mod my_env {
    pub fn var(key: &str) -> Option<String> { std::env::var(key).ok() }
}

fn main() {
    if my_env::var("CARGO_FEATURE_TFEAT").is_some() {
        println!("cargo:rustc-cfg=has_tfeat");
    }
}
EOF
      ;;
    bare-string-mention)
      cat >"$D/a/build.rs" <<'EOF'
fn main() {
    // Whatever this does with it, the name is here: CARGO_FEATURE_TFEAT
    let keys = ["CARGO_FEATURE_TFEAT"];
    for k in keys {
        if std::env::var_os(k).is_some() {
            println!("cargo:rustc-cfg=has_tfeat");
        }
    }
}
EOF
      ;;
  esac
  expect_green "$D" "case 14 ($spelling)"
  assert_contract_declares "any textual CARGO_FEATURE_* mention in a build-script package's sources" "case 14 ($spelling)"
done
ok "SOUNDNESS: all six CARGO_FEATURE_* spellings credit (aliased import, constant, env::vars(), local wrapper, my_env module, bare mention) — and the contract declares the textual scan"

# --- 15. GREEN (DECLARED): a BARE CARGO_FEATURE_ prefix credits every feature -
# Environment ITERATION names no individual feature, so there is nothing to match and
# the only reading that cannot report a live feature dead is to credit them all.
D="$(fixture bare-prefix)"
sed -i 's/^tfeat = \[\]$/tfeat = []\nprefixonly = []/' "$D/a/Cargo.toml"
grep -q '^prefixonly = \[\]$' "$D/a/Cargo.toml" || fail_case "case 15: fixture edit did not plant prefixonly"
cat >"$D/a/tests/t.rs" <<'EOF'
#[test]
fn t() {}
EOF
cat >"$D/a/build.rs" <<'EOF'
fn main() {
    for (key, _value) in std::env::vars() {
        if let Some(name) = key.strip_prefix("CARGO_FEATURE_") {
            println!("cargo:rustc-cfg=feature_seen_{}", name.to_lowercase());
        }
    }
}
EOF
expect_green "$D" "case 15"
assert_contract_declares "a bare CARGO_FEATURE_ prefix credits every feature of that package" "case 15"
ok "a BARE CARGO_FEATURE_ prefix credits EVERY feature of the package (nothing names an individual one), and the contract declares it"

# --- 16. RED: a package with NO build script gets no env credit --------------
D="$(fixture no-build-script)"
sed -i '/^build = "build.rs"$/d' "$D/a/Cargo.toml"
rm -f "$D/a/build.rs"
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
expect_red_naming "$D" "tfeat" "case 16"
ok "a package with NO build script gets no env credit at all, even for a real env read"

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

D="$(fixture overlap-both)"
# THE ASYMMETRY, in the direction that used to SUBTRACT. `a` declares a feature with the
# SAME NAME as the nested member's and has no site of its own — but the nested member's
# file lies under a directory `a`'s own test target can reach (`#[path]`, `include!`, a
# shared helper), and this scan cannot trace that. Dropping the outer owner reported a
# feature used only there as DEAD, so a file two packages can reach now credits BOTH.
sed -i 's/^tfeat = \[\]$/tfeat = []\nnfeat = []/' "$D/a/Cargo.toml"
grep -q '^nfeat = \[\]$' "$D/a/Cargo.toml" || fail_case "case 18: fixture edit did not plant a's nfeat"
expect_green "$D" "case 18"
ok "a file under a NESTED member's directory credits the OUTER member too (ownership never subtracts)"

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

# --- 21. RED: doc(cfg(...)) in a cfg_attr TAIL is documentation, not a gate ----
D="$(fixture doc-cfg)"
cat >"$D/a/src/lib.rs" <<'EOF'
#[cfg_attr(docsrs, doc(cfg(feature = "leafx")))]
pub fn always() {}
EOF
expect_red_naming "$D" "leafx" "case 21"
ok "a \`doc(cfg(feature = ...))\` inside a cfg_attr TAIL confers no credit"

# --- 22. RED: a raw string containing a cfg attribute is not a site -----------
D="$(fixture raw-string)"
cat >"$D/a/src/lib.rs" <<'EOF'
pub const SNIPPET: &str = r#"
#[cfg(feature = "leafx")]
pub fn gated() {}
"#;
pub const NESTED: &str = r##"#[cfg(feature = "leafx")] mod m;"##;
pub const BYTES: &[u8] = br#"#[cfg(feature = "leafx")]"#;
pub fn always() {}
EOF
expect_red_naming "$D" "leafx" "case 22"
ok "a RAW/byte string (any hash count) carrying a whole cfg attribute confers no credit"

# --- 24. RED: a REDUNDANT external dependency edge ---------------------------
# OFFLINE BY CONSTRUCTION. This suite runs in `tooling-tests`, a MANDATORY gate
# component, so a case that can reach the crates.io index makes the gate of record
# flaky and red on an offline host. `extdep` is therefore a LOCAL PATH crate created
# OUTSIDE the fixture workspace (so it is not a member and is never scanned), which
# gives the guard exactly what this case needs — a non-member dependency — with no
# network and no lockfile. Verified with CARGO_NET_OFFLINE=1.
D="$(fixture redundant-external)"
make_extdep "$D"
python3 - "$D/a/Cargo.toml" <<'PYEOF'
import sys
p = sys.argv[1]
s = open(p).read()
dep = 'bee = { path = "../b", package = "b" }'
s = s.replace(dep, dep + '\nextdep = { path = "../../extdep-redundant-external", features = ["derive"] }')
# `redext` forwards a feature the DECLARATION already enables (a no-op); `newext`
# forwards one it does not (a real effect) — the control.
s = s.replace('tfeat = []', 'tfeat = []\nredext = ["extdep/derive"]\nnewext = ["extdep/rc"]')
open(p, 'w').write(s)
PYEOF
grep -q '^redext = ' "$D/a/Cargo.toml" || fail_case "case 24: fixture edit did not plant redext"
expect_red_naming "$D" "redext" "case 24"
if grep -q 'newext' "$TMPROOT/out.txt"; then
  cat "$TMPROOT/out.txt"
  fail_case "case 24: the NON-redundant sibling edge serde/rc was also reported dead — the redundancy test is refusing every external edge, not just the no-op one"
fi
ok "a REDUNDANT external dependency edge confers no credit, while a non-redundant sibling still does"

# --- 25. RED: a REDUNDANT workspace-member dependency edge -------------------
D="$(fixture redundant-workspace)"
python3 - "$D/a/Cargo.toml" "$D/b/Cargo.toml" "$D/b/src/lib.rs" <<'PYEOF'
import sys
a, b, blib = sys.argv[1], sys.argv[2], sys.argv[3]
s = open(b).read().replace('bfeat = []', 'bfeat = []\nbfeat2 = []')
open(b, 'w').write(s)
s = open(blib).read() + '\n#[cfg(feature = "bfeat2")]\npub fn bfeat2_only() {}\n'
open(blib, 'w').write(s)
s = open(a).read()
# The declaration already enables bfeat2, so forwarding it enables nothing. `fwd`
# (bee/bfeat) is deliberately untouched, so this case isolates the redundant edge.
s = s.replace('bee = { path = "../b", package = "b" }',
              'bee = { path = "../b", package = "b", features = ["bfeat2"] }')
s = s.replace('tfeat = []', 'tfeat = []\nredws = ["bee/bfeat2"]')
open(a, 'w').write(s)
PYEOF
grep -q '^redws = ' "$D/a/Cargo.toml" || fail_case "case 25: fixture edit did not plant redws"
expect_red_naming "$D" "redws" "case 25"
if grep -qE '^ +[^ ]+  fwd ' "$TMPROOT/out.txt"; then
  cat "$TMPROOT/out.txt"
  fail_case "case 25: fwd was reported dead too — its own edge (bee/bfeat) is not redundant, so the redundancy test is over-reaching"
fi
ok "a REDUNDANT workspace-member edge confers no credit, and a non-redundant edge on the same dependency is unaffected"

# --- 27. GREEN: activation outranks redundancy (regression) ------------------
# `actfeat` forwards a feature the OPTIONAL dependency's declaration already enables,
# so the forwarding half is a no-op — but the edge still ACTIVATES `optdep`, which is
# an effect. Judging redundancy first reported this LIVE feature as dead.
D="$(fixture activation-vs-redundancy)"
python3 - "$D/a/Cargo.toml" <<'PYEOF'
import sys
p = sys.argv[1]
s = open(p).read()
s = s.replace('optdep = { path = "../optdep", optional = true }',
              'optdep = { path = "../optdep", optional = true, features = ["odfeat"] }')
s = s.replace('tfeat = []', 'tfeat = []\nactfeat = ["optdep/odfeat"]')
open(p, 'w').write(s)
PYEOF
grep -q '^actfeat = ' "$D/a/Cargo.toml" || fail_case "case 27: fixture edit did not plant actfeat"
expect_green "$D" "case 27"
ACT_COUNT="$(asserted_count)"
[ "$ACT_COUNT" -eq "$((BASE_COUNT + 1))" ] \
  || fail_case "case 27: expected $((BASE_COUNT + 1)) asserted features, got $ACT_COUNT"
ok "a non-weak edge to an OPTIONAL dependency stays load-bearing even when the forwarded feature is already enabled (it ACTIVATES the dependency)"

# --- 28. GREEN: pins the DECLARED orphan-file residual ----------------------
# NOT desired behaviour — a declared one. The guard's success line states that an
# ORPHAN .rs file under a target's source dir is scanned as if compiled; this case
# makes that statement testable. If module-graph resolution is ever implemented this
# case must red, and the residual text must be updated in the same change.
D="$(fixture orphan-file)"
sed -i 's/^tfeat = \[\]$/tfeat = []\norphanfeat = []/' "$D/a/Cargo.toml"
grep -q '^orphanfeat = \[\]$' "$D/a/Cargo.toml" || fail_case "case 28: fixture edit did not plant orphanfeat"
# Reachable from NO `mod` chain: a/src/lib.rs does not declare `mod obsolete`.
cat >"$D/a/src/obsolete.rs" <<'EOF'
#[cfg(feature = "orphanfeat")]
pub fn long_dead() {}
EOF
grep -q 'mod obsolete' "$D/a/src/lib.rs" \
  && fail_case "case 28: the fixture's lib.rs declares the module, so the file is not an orphan"
expect_green "$D" "case 28"
ORPHAN_COUNT="$(asserted_count)"
[ "$ORPHAN_COUNT" -eq "$((BASE_COUNT + 1))" ] \
  || fail_case "case 28: expected $((BASE_COUNT + 1)) asserted features, got $ORPHAN_COUNT"
assert_contract_declares "orphan .rs files under a target source dir" "case 28"
ok "an ORPHAN .rs file under a target's source dir IS scanned (a credited dead feature) and the success line DECLARES exactly that"

# --- 29. GREEN (SOUNDNESS): a build script's HELPER MODULE env read counts ---
# The read lives in a/src/buildhelp.rs, reached from build.rs by `#[path]`. Resolving
# that means implementing Rust's module graph; missing it reported a LIVE feature dead.
# The guard therefore scans EVERY .rs file of a build-script package — over-permissive
# on purpose, and declared.
D="$(fixture buildscript-helper)"
cat >"$D/a/tests/t.rs" <<'EOF'
#[test]
fn t() {}
EOF
cat >"$D/a/build.rs" <<'EOF'
#[path = "src/buildhelp.rs"]
mod buildhelp;

fn main() {
    if buildhelp::wants_tfeat() {
        println!("cargo:rustc-cfg=has_tfeat");
    }
}
EOF
cat >"$D/a/src/buildhelp.rs" <<'EOF'
pub fn wants_tfeat() -> bool {
    std::env::var("CARGO_FEATURE_TFEAT").is_ok()
}
EOF
expect_green "$D" "case 29"
assert_contract_declares "any textual CARGO_FEATURE_* mention in a build-script package's sources" "case 29"
ok "SOUNDNESS: a CARGO_FEATURE_* read in a build script's HELPER MODULE still credits its feature (and the contract declares the textual scan)"

# --- 30. GREEN (SOUNDNESS): a same-named NON-MEMBER path dependency ----------
# `bx` is a non-member crate whose package NAME is `b`, the same as the member's, and it
# declares a feature the member does not. Under name-based resolution the guard looked
# `onlyext` up in the MEMBER, found nothing and REFUSED — a false FAIL on correct input.
D="$(fixture same-name-nonmember)"
EXTB="$TMPROOT/extb-same-name-nonmember"
mkdir -p "$EXTB/src"
cat >"$EXTB/Cargo.toml" <<'EOF'
[package]
name = "b"
version = "0.0.0"
edition = "2021"

[features]
onlyext = []
EOF
echo "pub fn nothing() {}" >"$EXTB/src/lib.rs"
python3 - "$D/a/Cargo.toml" <<'PYEOF'
import sys
p = sys.argv[1]
s = open(p).read()
dep = 'bee = { path = "../b", package = "b" }'
s = s.replace(dep, dep + '\nbx = { path = "../../extb-same-name-nonmember", package = "b" }')
s = s.replace('tfeat = []', 'tfeat = []\nextonly = ["bx/onlyext"]')
open(p, 'w').write(s)
PYEOF
grep -q '^extonly = ' "$D/a/Cargo.toml" || fail_case "case 30: fixture edit did not plant extonly"
expect_green "$D" "case 30"
ok "SOUNDNESS: a NON-MEMBER path dependency sharing a member's package name resolves by PATH, so a feature only it declares is not a refusal"

# --- 32. GREEN (DECLARED): cfg inside an UNEXPANDED macro body --------------
D="$(fixture macro-body-cfg)"
sed -i 's/^tfeat = \[\]$/tfeat = []\nmacrofeat = []/' "$D/a/Cargo.toml"
grep -q '^macrofeat = \[\]$' "$D/a/Cargo.toml" || fail_case "case 32: fixture edit did not plant macrofeat"
cat >>"$D/a/src/lib.rs" <<'EOF'

macro_rules! never_invoked {
    () => {
        #[cfg(feature = "macrofeat")]
        pub fn gated_by_expansion() {}
    };
}
EOF
expect_green "$D" "case 32"
assert_contract_declares "cfgs inside unexpanded macro bodies" "case 32"
ok "DECLARED: a cfg inside an UNEXPANDED macro_rules! body credits its feature, and the contract names that escape route"

# --- 33. GREEN (SOUNDNESS): a module included from OUTSIDE any target root ---
# `a/gated.rs` sits in the package root, under no target's source tree, and is reached
# by `#[path = "../gated.rs"]` from a/src/lib.rs. No tree covers it, so it used to have
# NO owner and the feature it gates was reported DEAD. Ownership now falls back to every
# member whose package DIRECTORY contains the file.
D="$(fixture out-of-tree-path-module)"
sed -i 's/^tfeat = \[\]$/tfeat = []\npathfeat = []/' "$D/a/Cargo.toml"
grep -q '^pathfeat = \[\]$' "$D/a/Cargo.toml" || fail_case "case 33: fixture edit did not plant pathfeat"
cat >"$D/a/gated.rs" <<'EOF'
#[cfg(feature = "pathfeat")]
pub fn gated_by_path_module() {}
EOF
cat >>"$D/a/src/lib.rs" <<'EOF'

#[path = "../gated.rs"]
pub mod gated;
EOF
expect_green "$D" "case 33"
ok "SOUNDNESS: a module included from OUTSIDE any target root (#[path]) still credits its feature"

# --- 34. GREEN (SOUNDNESS): a nested-member HELPER reached by the outer target -
# `a/tests/helpers/util.rs` lies inside the NESTED member's package directory but under
# `a`'s own test-target directory, and it is the only site of a's `helperfeat`. The
# existing overlap case only covered an EXACT target file, which is why this survived
# three rounds.
D="$(fixture nested-helper-module)"
sed -i 's/^tfeat = \[\]$/tfeat = []\nhelperfeat = []/' "$D/a/Cargo.toml"
grep -q '^helperfeat = \[\]$' "$D/a/Cargo.toml" || fail_case "case 34: fixture edit did not plant helperfeat"
mkdir -p "$D/a/tests/helpers"
cat >"$D/a/tests/helpers/util.rs" <<'EOF'
#[cfg(feature = "helperfeat")]
pub fn helper_only() {}
EOF
cat >>"$D/a/tests/t.rs" <<'EOF'

#[path = "helpers/util.rs"]
mod util;
EOF
expect_green "$D" "case 34"
ok "SOUNDNESS: a helper module under the NESTED member's dir, reached by the OUTER target, credits the outer member"

# --- 35. THE ASYMMETRY ITSELF ------------------------------------------------
# This is the invariant everything else rests on, and the one a future refactor would
# most easily break: WHEN OWNERSHIP IS AMBIGUOUS, CREDIT — never drop. Three files, each
# ambiguous in a different way, each the SOLE site of a distinct feature, in ONE run: a
# file no target tree covers (package root), a file under a nested member's directory,
# and a file in a directory no target names at all. If any of them is dropped instead of
# credited, its feature is reported dead and this case reds — which is exactly the false
# FAIL the contract forbids.
D="$(fixture ambiguity-credits)"
python3 - "$D/a/Cargo.toml" <<'PYEOF'
import sys
p = sys.argv[1]
s = open(p).read()
s = s.replace('tfeat = []', 'tfeat = []\nambig1 = []\nambig2 = []\nambig3 = []')
open(p, 'w').write(s)
PYEOF
for f in ambig1 ambig2 ambig3; do
  grep -q "^$f = \[\]$" "$D/a/Cargo.toml" || fail_case "case 35: fixture edit did not plant $f"
done
cat >"$D/a/ambig1.rs" <<'EOF'
#[cfg(feature = "ambig1")]
pub fn one() {}
EOF
mkdir -p "$D/a/tests/aux"
cat >"$D/a/tests/aux/ambig2.rs" <<'EOF'
#[cfg(feature = "ambig2")]
pub fn two() {}
EOF
mkdir -p "$D/a/extra/deep"
cat >"$D/a/extra/deep/ambig3.rs" <<'EOF'
#[cfg(feature = "ambig3")]
pub fn three() {}
EOF
expect_green "$D" "case 35"
AMBIG_COUNT="$(asserted_count)"
[ "$AMBIG_COUNT" -eq "$((BASE_COUNT + 3))" ] \
  || fail_case "case 35: expected $((BASE_COUNT + 3)) asserted features, got $AMBIG_COUNT — one of the three ambiguous files was DROPPED rather than credited"
ok "THE ASYMMETRY: three differently-ambiguous files each credit their feature ($AMBIG_COUNT = $BASE_COUNT + 3) — ambiguity resolves toward CREDITING, never toward dropping"

# --- 36. GREEN (SOUNDNESS): a cfg in a cfg_attr TAIL is a real gate ----------
# `#[cfg_attr(unix, cfg(feature = "x"))]` applies the tail attribute when the condition
# holds, so it gates x. Scanning only the CONDITION reported such a feature dead.
D="$(fixture cfg-attr-tail)"
sed -i 's/^tfeat = \[\]$/tfeat = []\ntailfeat = []/' "$D/a/Cargo.toml"
grep -q '^tailfeat = \[\]$' "$D/a/Cargo.toml" || fail_case "case 36: fixture edit did not plant tailfeat"
cat >>"$D/a/src/lib.rs" <<'EOF'

#[cfg_attr(unix, cfg(feature = "tailfeat"))]
pub fn gated_through_a_tail() {}
EOF
expect_green "$D" "case 36"
ok "SOUNDNESS: a cfg inside a cfg_attr TAIL is a real gate and credits its feature"

# --- 37. GREEN (SOUNDNESS): whitespace in the attribute head -----------------
# `# [cfg(...)]` and `# ! [cfg(...)]` are legal Rust; requiring `#[` contiguous meant a
# legal gate was NOT SEEN.
D="$(fixture whitespace-attr-head)"
sed -i 's/^tfeat = \[\]$/tfeat = []\nwsouter = []\nwsinner = []/' "$D/a/Cargo.toml"
for f in wsouter wsinner; do
  grep -q "^$f = \[\]$" "$D/a/Cargo.toml" || fail_case "case 37: fixture edit did not plant $f"
done
cat >"$D/a/src/gapped.rs" <<'EOF'
# ! [cfg(feature = "wsinner")]

# [cfg(feature = "wsouter")]
pub fn spaced_out() {}
EOF
cat >>"$D/a/src/lib.rs" <<'EOF'

pub mod gapped;
EOF
expect_green "$D" "case 37"
ok "SOUNDNESS: \`# [cfg(...)]\` and \`# ! [cfg(...)]\` (whitespace in the head) are seen"

# --- 38. GREEN (SOUNDNESS): Rust string escapes are decoded ------------------
# `"\x66oo"` IS the feature `foo`; recording `x66oo` reported `foo` dead.
D="$(fixture escape-decoding)"
sed -i 's/^tfeat = \[\]$/tfeat = []\nfoo = []\nbar = []/' "$D/a/Cargo.toml"
for f in foo bar; do
  grep -q "^$f = \[\]$" "$D/a/Cargo.toml" || fail_case "case 38: fixture edit did not plant $f"
done
cat >>"$D/a/src/lib.rs" <<'EOF'

#[cfg(feature = "\x66oo")]
pub fn hex_escaped() {}

#[cfg(feature = "\u{62}ar")]
pub fn unicode_escaped() {}
EOF
expect_green "$D" "case 38"
ok "SOUNDNESS: \\xHH and \\u{...} escapes are decoded, so the gated feature is the one credited"

# --- 39. GREEN (DECLARED): an UNDECODABLE escape credits every feature -------
D="$(fixture undecodable-escape)"
sed -i 's/^tfeat = \[\]$/tfeat = []\nnowhere = []/' "$D/a/Cargo.toml"
grep -q '^nowhere = \[\]$' "$D/a/Cargo.toml" || fail_case "case 39: fixture edit did not plant nowhere"
cat >>"$D/a/src/lib.rs" <<'EOF'

#[cfg(feature = "\q")]
pub fn escape_this_scanner_cannot_read() {}
EOF
expect_green "$D" "case 39"
assert_contract_declares "an undecodable string escape credits every feature of the package" "case 39"
ok "DECLARED: an escape the scanner cannot decode credits EVERY feature of the package rather than one wrong name"

# --- 40. RED (DECLARED LIMIT): a feature NAME produced by macro expansion ----
# The name exists nowhere in a `feature =` position, so no lexical scan can see it. This
# is a NOT-SEEN spelling, and the contract line says so — that is why the claim is
# scoped rather than absolute.
D="$(fixture macro-expanded-name)"
sed -i 's/^tfeat = \[\]$/tfeat = []\nexpandedname = []/' "$D/a/Cargo.toml"
grep -q '^expandedname = \[\]$' "$D/a/Cargo.toml" || fail_case "case 40: fixture edit did not plant expandedname"
cat >>"$D/a/src/lib.rs" <<'EOF'

macro_rules! gate_it {
    ($flag:literal) => {
        #[cfg(feature = $flag)]
        pub fn gated_by_expansion() {}
    };
}

gate_it!("expandedname");
EOF
expect_red_naming "$D" "expandedname" "case 40"
ok "DECLARED LIMIT: a feature NAME produced by macro expansion is NOT SEEN (reported dead), which is why the claim is scoped"

# --- 41. RED (DECLARED LIMIT): a build-script env key built at runtime -------
D="$(fixture runtime-env-key)"
sed -i 's/^tfeat = \[\]$/tfeat = []\nruntimekey = []/' "$D/a/Cargo.toml"
grep -q '^runtimekey = \[\]$' "$D/a/Cargo.toml" || fail_case "case 41: fixture edit did not plant runtimekey"
cat >"$D/a/build.rs" <<'EOF'
fn main() {
    let key = ["CARGO", "FEATURE", "RUNTIMEKEY"].join("_");
    if std::env::var(&key).is_ok() {
        println!("cargo:rustc-cfg=has_runtimekey");
    }
}
EOF
expect_red_naming "$D" "runtimekey" "case 41"
ok "DECLARED LIMIT: a build-script env key CONSTRUCTED AT RUNTIME is NOT SEEN (reported dead), as the contract states"

# --- 42. THE CLAIM ITSELF ----------------------------------------------------
# The published claim must be SCOPED. An unqualified soundness claim was tried and
# RETRACTED (six review rounds, six more valid Rust spellings), so the success output
# must carry the bounded wording AND must not contain any form of the word "sound" — a
# false rationale in a gate log is worse than none, because it is what stops the next
# person looking.
D="$(fixture claim-wording)"
expect_green "$D" "case 42"
grep -q 'CONTRACT: NO FALSE FAIL for a gate written in a RECOGNISED spelling' "$TMPROOT/out.txt" \
  || { cat "$TMPROOT/out.txt"; fail_case "case 42: the success output does not state the SCOPED no-false-FAIL claim"; }
grep -q 'NOT SEEN' "$TMPROOT/out.txt" \
  || fail_case "case 42: the claim does not say that a spelling outside the recognised set is NOT SEEN"
for spelling in '#\[cfg\]' '#!\[cfg\]' 'cfg!' 'cfg_attr'; do
  grep -qE -- "$spelling" "$TMPROOT/out.txt" \
    || fail_case "case 42: the claim does not ENUMERATE the recognised spelling '$spelling' — an unenumerated scope is not a scope"
done
if grep -qi 'sound' "$TMPROOT/out.txt"; then
  cat "$TMPROOT/out.txt"
  fail_case "case 42: the success output makes a SOUNDNESS claim. That claim is absolute over a lexical scan of Rust, it was RETRACTED on roborev job 60, and it must not come back."
fi
ok "THE CLAIM: the success output states the SCOPED no-false-FAIL claim, enumerates the recognised spellings, says what is NOT SEEN, and makes no soundness claim"

# --- 43. GREEN (SOUNDNESS): a src/target/ module is real source --------------
# Pruning by BASENAME never scanned any directory called `target` or `fuzz`, but
# `src/target/` and `src/fuzz/` are legitimate Rust module directories — a gate there, in
# a RECOGNISED spelling, was reported dead, contradicting the printed claim. Pruning is
# now by anchored path: `target` only beside a Cargo.toml, `fuzz` only the workspace root.
D="$(fixture src-target-module)"
sed -i 's/^tfeat = \[\]$/tfeat = []\ntargetfeat = []\nfuzzfeat = []/' "$D/a/Cargo.toml"
for f in targetfeat fuzzfeat; do
  grep -q "^$f = \[\]$" "$D/a/Cargo.toml" || fail_case "case 43: fixture edit did not plant $f"
done
mkdir -p "$D/a/src/target" "$D/a/src/fuzz"
cat >"$D/a/src/target/mod.rs" <<'EOF'
#[cfg(feature = "targetfeat")]
pub fn in_a_module_called_target() {}
EOF
cat >"$D/a/src/fuzz/mod.rs" <<'EOF'
#[cfg(feature = "fuzzfeat")]
pub fn in_a_module_called_fuzz() {}
EOF
cat >>"$D/a/src/lib.rs" <<'EOF'

pub mod target;
pub mod fuzz;
EOF
expect_green "$D" "case 43"
ok "SOUNDNESS: modules named \`target\` and \`fuzz\` under src/ are real source and are scanned (pruning is by anchored path, not basename)"

# --- 44. GREEN (SOUNDNESS): build output IS still pruned ---------------------
# The control for 43: a `target/` BESIDE a Cargo.toml is cargo build output and must not
# be scanned. A generated file there naming an otherwise-dead feature must not credit it.
D="$(fixture build-output-pruned)"
sed -i 's/^tfeat = \[\]$/tfeat = []\ngeneratedfeat = []/' "$D/a/Cargo.toml"
grep -q '^generatedfeat = \[\]$' "$D/a/Cargo.toml" || fail_case "case 44: fixture edit did not plant generatedfeat"
mkdir -p "$D/a/target/debug/build"
cat >"$D/a/target/debug/build/generated.rs" <<'EOF'
#[cfg(feature = "generatedfeat")]
pub fn generated() {}
EOF
expect_red_naming "$D" "generatedfeat" "case 44"
ok "the control for 43: a target/ dir BESIDE a Cargo.toml is build output and stays pruned"

# --- 45. GREEN (SOUNDNESS): a cfg_attr chain DEEPER than the recursion bound --
# The recursion is bounded so a pathological token stream cannot hang a mandatory gate
# component — but AT the bound the ambiguity is CREDITED, never dropped, because a deeper
# valid chain is inside the advertised recognised spelling and dropping it would report a
# live feature dead.
D="$(fixture deep-cfg-attr-chain)"
sed -i 's/^tfeat = \[\]$/tfeat = []\ndeepfeat = []/' "$D/a/Cargo.toml"
grep -q '^deepfeat = \[\]$' "$D/a/Cargo.toml" || fail_case "case 45: fixture edit did not plant deepfeat"
python3 - "$D/a/src/lib.rs" <<'PYEOF'
import sys
# 40 nested cfg_attr levels — well past the bound — with the real gate at the bottom.
DEPTH = 40
inner = '#[' + 'cfg_attr(unix, ' * DEPTH + 'cfg(feature = "deepfeat")' + ')' * DEPTH + ']'
with open(sys.argv[1], 'a') as fh:
    fh.write('\n' + inner + '\npub fn very_deeply_gated() {}\n')
PYEOF
grep -q 'cfg_attr(unix, cfg_attr' "$D/a/src/lib.rs" || fail_case "case 45: fixture did not build a nested chain"
expect_green "$D" "case 45"
ok "SOUNDNESS: a cfg_attr chain deeper than the recursion bound CREDITS at the bound instead of dropping the gate"

# --- 46. STRUCTURAL: the gate invokes this suite behind a python3 guard ------
# This suite needs python3 (the guard it exercises is python3-based), and it lives in
# `tooling-tests`, a component documented SKIP-aware for exactly that. Invoking it
# UNGUARDED there converted a supported SKIP into a red gate of record — so the guard is
# asserted STRUCTURALLY here, in the file that knows the requirement, rather than left to
# be rediscovered. Labelled structural on purpose: it reads source, it does not run the
# component (which takes ~35 minutes).
GATE="$REPO_ROOT/scripts/agent-gate.sh"
[ -f "$GATE" ] || fail_case "case 46: scripts/agent-gate.sh not found"
inv_line="$(grep -n 'bash "\$REPO_ROOT/scripts/tests/test_features_load_bearing_guard.sh"' "$GATE" | head -1 | cut -d: -f1)"
[ -n "$inv_line" ] \
  || fail_case "case 46: no invocation of this suite found in scripts/agent-gate.sh — either it was unwired (then nothing runs it) or the invocation was reworded (then update this case)"
window_start=$((inv_line > 12 ? inv_line - 12 : 1))
sed -n "${window_start},${inv_line}p" "$GATE" | grep -q 'command -v python3' \
  || fail_case "case 46: the invocation at scripts/agent-gate.sh:$inv_line is NOT inside a \`command -v python3\` guard. tooling-tests is SKIP-aware for missing python3; invoking this suite unguarded turns a supported SKIP into a red gate of record (roborev job 62). The MANDATORY requirement belongs to the features-load-bearing component, which FAILs on it."
sed -n "${inv_line},$((inv_line + 25))p" "$GATE" | grep -q 'SKIP scripts/tests/test_features_load_bearing_guard.sh' \
  || fail_case "case 46: the python3 guard around scripts/agent-gate.sh:$inv_line has no LOUD SKIP branch. A silently skipped self-test is indistinguishable from one that ran."
ok "STRUCTURAL: the gate invokes this suite inside a \`command -v python3\` guard with a LOUD SKIP branch, so tooling-tests' documented no-python3 SKIP still governs"

# --- CASE COUNT: EXACT, not a floor ------------------------------------------
# #3544's lesson is this suite's own subject: a span-replacing edit once deleted four
# cases from a suite and it reported "failed: 0" over the shrunken remainder. A FLOOR
# below the real count tolerates exactly that — one case can be deleted and the guard
# still greens (roborev job 50, finding 5) — so the count is pinned EXACTLY. Adding a
# case means changing this number in the same diff, deliberately.
CASE_COUNT_EXPECTED=45
[ "$CASES" -eq "$CASE_COUNT_EXPECTED" ] \
  || fail_case "CASE COUNT: $CASES cases ran, expected EXACTLY $CASE_COUNT_EXPECTED. Cases were deleted, skipped or added without updating this assertion; a green tally over a changed suite certifies nothing."

echo "PASS: $CASES cases (exact count $CASE_COUNT_EXPECTED)"
