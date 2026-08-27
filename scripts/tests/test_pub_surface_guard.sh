#!/usr/bin/env bash
# test_pub_surface_guard.sh — self-test for the cqlite-core public-surface snapshot
# guard, scripts/ci/check-pub-surface.sh (issue #1712, epic #1688).
#
# The point of this suite is to prove the guard FIRES, not merely that it exists. A
# guard nobody has watched fail is indistinguishable from a guard that always passes,
# and this repo has paid for that lesson repeatedly (CLAUDE.md: "a positive verdict
# requires an affirmative measurement"). So every negative case asserts not just a
# non-zero exit but a distinctive substring of the INTENDED diagnostic — a bare
# exit-code assertion passes on an unrelated silent abort.
#
# Cases:
#   1. GREEN  — the committed snapshot verifies clean on the real tree.
#   2. RED    — the CONSISTENCY ASSERT. A scratch checkout is reverted to the
#               pre-#1712 shape (bare ungated `pub mod benchmarks;` at the crate root
#               + the inner `#![cfg(feature = "benchmarks")]` back in
#               benchmarks/mod.rs) and the guard must FAIL naming `benchmarks`. This
#               is the pre-change-main red, PINNED so it can never silently stop
#               being a red.
#   3. RED    — snapshot drift. A new public item is added to an existing public
#               module; VERIFY must FAIL with a diff naming it.
#   4. RED    — the committed snapshot is missing; VERIFY must FAIL naming the
#               regenerate command, never pass vacuously over an absent baseline.
#   5. USAGE  — an unrecognized argument exits 2 (repo convention).
#   6. RED    — a new `pub fn` on an EXISTING public struct is named in the diff.
#               The first cut of the guard enumerated only standalone rustdoc pages
#               and was blind to this (roborev round 1, blocker 1).
#   7. RED    — a new VARIANT on an EXISTING public enum is named in the diff (same
#               blind spot). The variant is added together with its arms in the two
#               exhaustive matches, so the crate still compiles and the case tests
#               variant coverage rather than the cargo-doc failure path.
#   8. RED    — a purely cosmetic `#[cfg_attr(...)]` at the declaration site must NOT
#               exempt a crate-root `pub mod` from the consistency assert (roborev
#               round 1, blocker 2: treating every cfg_attr as an exemption reopened
#               the very bypass the assert closes).
#
# NO TEST-ONLY SEAM. The guard's subject is hard-coded on purpose, so the negative
# cases SUBSTITUTE THE ARTIFACT: each runs in its own `git worktree add --detach HEAD`
# scratch checkout whose files are edited in place (CLAUDE.md — a test that needs a
# different subject substitutes the artifact, never a path variable; a path variable
# is one more thing a real invoker can set).
#
# Cost: each case is one `cargo doc --no-deps` of cqlite-core (~6s) because
# CARGO_TARGET_DIR is pointed at the main checkout's target dir, so every dependency
# is already built. Whole suite well under two minutes.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GUARD_REL="scripts/ci/check-pub-surface.sh"
GUARD="$REPO_ROOT/$GUARD_REL"
SNAPSHOT_REL="cqlite-core/pub-surface.snapshot"

[ -f "$GUARD" ] || { echo "FAIL: guard script not found at $GUARD"; exit 1; }

# Reuse the main checkout's target dir so the scratch worktrees never rebuild
# dependencies. Respect an already-exported CARGO_TARGET_DIR (the gate sets one).
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"

WORKTREES=()
TMPROOT="$(mktemp -d "${TMPDIR:-/tmp}/pub-surface-selftest.XXXXXX")"
cleanup() {
  local wt
  for wt in "${WORKTREES[@]:-}"; do
    [ -n "$wt" ] || continue
    git -C "$REPO_ROOT" worktree remove --force "$wt" >/dev/null 2>&1 || rm -rf "$wt"
  done
  git -C "$REPO_ROOT" worktree prune >/dev/null 2>&1 || true
  rm -rf "$TMPROOT"
}
trap cleanup EXIT

# scratch_tree <name>: create a detached worktree at HEAD and publish its path in
# the global SCRATCH. Deliberately NOT a command substitution: `$(scratch_tree …)`
# would run the body in a subshell, so the WORKTREES bookkeeping the EXIT trap
# cleans up would be discarded and every scratch checkout would be left behind.
SCRATCH=""
scratch_tree() {
  local nm="$1"
  local path="$TMPROOT/$nm"
  git -C "$REPO_ROOT" worktree add --detach --quiet "$path" HEAD >/dev/null 2>&1 \
    || { echo "FAIL: could not create scratch worktree $path"; exit 1; }
  WORKTREES+=("$path")
  # A worktree materialises HEAD, so without this the negative cases would be
  # validating the LAST COMMIT rather than the change under test — an uncommitted
  # guard fix silently goes untested, and an uncommitted guard REGRESSION silently
  # passes. Overlay the live checkout's guard + snapshot (the two artifacts the
  # cases are about) so the suite always measures the tree you are working in.
  cp "$REPO_ROOT/$GUARD_REL" "$path/$GUARD_REL"
  cp "$REPO_ROOT/$SNAPSHOT_REL" "$path/$SNAPSHOT_REL"
  SCRATCH="$path"
}

fail_case() { echo "FAIL: $*"; exit 1; }

# ---------------------------------------------------------------------------
# 5. USAGE first — it is the cheapest and needs no worktree.
# ---------------------------------------------------------------------------
set +e
bash "$GUARD" --definitely-not-a-flag >"$TMPROOT/usage.out" 2>&1
usage_rc=$?
set -e
[ "$usage_rc" -eq 2 ] || fail_case "unrecognized argument exited $usage_rc, expected 2 (repo convention)"
grep -q "unrecognized argument" "$TMPROOT/usage.out" \
  || fail_case "unrecognized-argument diagnostic missing; got: $(cat "$TMPROOT/usage.out")"
echo "OK (5): an unrecognized argument exits 2 with a usage diagnostic"

# ---------------------------------------------------------------------------
# 1. GREEN — the real tree verifies clean.
# ---------------------------------------------------------------------------
set +e
bash "$GUARD" >"$TMPROOT/green.out" 2>&1
green_rc=$?
set -e
if [ "$green_rc" -ne 0 ]; then
  echo "FAIL: the guard FAILED on the REAL tree — the committed snapshot has drifted"
  echo "      from cqlite-core's public API, or the crate root is inconsistent."
  echo "      Regenerate with: bash $GUARD_REL --regenerate"
  cat "$TMPROOT/green.out"
  exit 1
fi
grep -q "public items + .* associated items" "$TMPROOT/green.out" \
  || fail_case "the guard passed but printed no affirmative measurement line; got: $(cat "$TMPROOT/green.out")"
echo "OK (1): real tree verifies clean — $(cat "$TMPROOT/green.out")"

# ---------------------------------------------------------------------------
# 2. RED — the consistency assert, against the pre-#1712 source shape.
# ---------------------------------------------------------------------------
scratch_tree pre-1712; wt2="$SCRATCH"
# Restore the bare, ungated crate-root declaration: drop a
# `#[cfg(feature = "benchmarks")]` line that sits immediately above
# `pub mod benchmarks;`. Pure awk — no perl/GNU-sed dependency.
awk '
  /^#\[cfg\(feature = "benchmarks"\)\]$/ { held = $0; next }
  {
    if (held != "" && $0 != "pub mod benchmarks;") print held
    held = ""
    print
  }
  END { if (held != "") print held }
' "$wt2/cqlite-core/src/lib.rs" >"$wt2/lib.rs.reverted"
mv "$wt2/lib.rs.reverted" "$wt2/cqlite-core/src/lib.rs"
grep -qx 'pub mod benchmarks;' "$wt2/cqlite-core/src/lib.rs" \
  || fail_case "case 2 setup: could not restore the bare \`pub mod benchmarks;\` in the scratch tree"
# The declaration-site gate must be GONE, or the case would pass for the wrong reason.
if grep -A1 -x '#\[cfg(feature = "benchmarks")\]' "$wt2/cqlite-core/src/lib.rs" | grep -qx 'pub mod benchmarks;'; then
  fail_case "case 2 setup: the declaration-site cfg gate survived the revert"
fi
# …and put the hidden inner gate back inside the module file.
printf '%s\n%s\n' '#![cfg(feature = "benchmarks")]' "$(cat "$wt2/cqlite-core/src/benchmarks/mod.rs")" \
  >"$wt2/cqlite-core/src/benchmarks/mod.rs.new"
mv "$wt2/cqlite-core/src/benchmarks/mod.rs.new" "$wt2/cqlite-core/src/benchmarks/mod.rs"

set +e
bash "$wt2/$GUARD_REL" >"$TMPROOT/case2.out" 2>&1
case2_rc=$?
set -e
[ "$case2_rc" -ne 0 ] || {
  echo "FAIL: case 2 — the guard PASSED on the pre-#1712 source shape (bare ungated"
  echo "      \`pub mod benchmarks;\` whose cfg gate hides inside the module file)."
  echo "      That is precisely the defect this guard exists to catch."
  cat "$TMPROOT/case2.out"
  exit 1
}
grep -q "pub mod benchmarks" "$TMPROOT/case2.out" \
  || fail_case "case 2 — the guard failed but never named \`benchmarks\`; got: $(cat "$TMPROOT/case2.out")"
grep -q "INCONSISTENT with the real public surface" "$TMPROOT/case2.out" \
  || fail_case "case 2 — the guard failed for some OTHER reason than the consistency assert; got: $(cat "$TMPROOT/case2.out")"
echo "OK (2): the consistency assert FAILS on the pre-#1712 shape and names \`benchmarks\`"

# ---------------------------------------------------------------------------
# 3. RED — snapshot drift on a genuinely new public item.
# ---------------------------------------------------------------------------
scratch_tree new-pub-item; wt3="$SCRATCH"
cat >>"$wt3/cqlite-core/src/version_hints.rs" <<'RS'

/// Self-test-only probe item (scripts/tests/test_pub_surface_guard.sh, issue #1712).
/// Exists solely inside a throwaway scratch worktree to prove the snapshot guard
/// notices a NEW public item. It is never committed.
pub struct PubSurfaceSelfTestProbe;
RS
set +e
bash "$wt3/$GUARD_REL" >"$TMPROOT/case3.out" 2>&1
case3_rc=$?
set -e
[ "$case3_rc" -ne 0 ] || fail_case "case 3 — a NEW public item did not trip the snapshot diff; got: $(cat "$TMPROOT/case3.out")"
grep -q "PubSurfaceSelfTestProbe" "$TMPROOT/case3.out" \
  || fail_case "case 3 — the guard failed but the diff never named the new item; got: $(cat "$TMPROOT/case3.out")"
grep -q -- "--regenerate" "$TMPROOT/case3.out" \
  || fail_case "case 3 — the drift diagnostic did not print the regenerate command"
echo "OK (3): a new public item trips the snapshot diff and is named in it"

# ---------------------------------------------------------------------------
# 4. RED — the committed snapshot is missing.
# ---------------------------------------------------------------------------
scratch_tree no-snapshot; wt4="$SCRATCH"
rm -f "$wt4/$SNAPSHOT_REL"
set +e
bash "$wt4/$GUARD_REL" >"$TMPROOT/case4.out" 2>&1
case4_rc=$?
set -e
[ "$case4_rc" -ne 0 ] || fail_case "case 4 — a MISSING snapshot passed vacuously; got: $(cat "$TMPROOT/case4.out")"
grep -q "MISSING or unreadable" "$TMPROOT/case4.out" \
  || fail_case "case 4 — the guard failed but not with the missing-snapshot diagnostic; got: $(cat "$TMPROOT/case4.out")"
grep -q -- "--regenerate" "$TMPROOT/case4.out" \
  || fail_case "case 4 — the missing-snapshot diagnostic did not print the regenerate command"
echo "OK (4): a missing snapshot FAILs and names the regenerate command"

# ---------------------------------------------------------------------------
# 6. RED — a new `pub fn` on an EXISTING public struct (roborev round 1, B1).
#
#    This is the case the first cut of the guard could NOT see: only standalone
#    rustdoc pages were enumerated, so an added method moved nothing.
# ---------------------------------------------------------------------------
scratch_tree new-method; wt6="$SCRATCH"
cat >>"$wt6/cqlite-core/src/version_hints.rs" <<'RS'

impl ResolvedVersion {
    /// Self-test-only probe method (scripts/tests/test_pub_surface_guard.sh, #1712).
    /// Added to an ALREADY-PUBLIC struct inside a throwaway scratch worktree, to prove
    /// the snapshot notices an added associated item. Never committed.
    pub fn pub_surface_self_test_probe(&self) -> bool {
        true
    }
}
RS
set +e
bash "$wt6/$GUARD_REL" >"$TMPROOT/case6.out" 2>&1
case6_rc=$?
set -e
[ "$case6_rc" -ne 0 ] || fail_case "case 6 — a new \`pub fn\` on an existing public struct did not trip the snapshot; got: $(cat "$TMPROOT/case6.out")"
grep -q "pub_surface_self_test_probe" "$TMPROOT/case6.out" \
  || fail_case "case 6 — the guard failed but the diff never named the new method; got: $(cat "$TMPROOT/case6.out")"
grep -q "^+method cqlite_core::version_hints::ResolvedVersion::pub_surface_self_test_probe" "$TMPROOT/case6.out" \
  || fail_case "case 6 — the new method was mentioned but not recorded as a \`method\` line at its real path; got: $(grep pub_surface_self_test_probe "$TMPROOT/case6.out")"
echo "OK (6): a new pub fn on an existing public struct is named in the diff"

# ---------------------------------------------------------------------------
# 7. RED — a new VARIANT on an EXISTING public enum (roborev round 1, B1).
#
#    The variant is added together with its arms in the two exhaustive matches in
#    the same file: without them the crate would not compile, `cargo doc` would
#    fail, and the case would be testing the cargo-doc failure path instead of
#    variant coverage.
# ---------------------------------------------------------------------------
scratch_tree new-variant; wt7="$SCRATCH"
awk '
  { print }
  /^    Unknown,$/ && !seen_variant { print "    /// Self-test-only probe variant (#1712)."; print "    PubSurfaceSelfTestVariant,"; seen_variant = 1 }
  /VersionSource::Unknown => 255,/ { print "            VersionSource::PubSurfaceSelfTestVariant => 254," }
  /VersionSource::Unknown => "Unknown \(no version information available\)",/ { print "            VersionSource::PubSurfaceSelfTestVariant => \"self-test probe\"," }
' "$wt7/cqlite-core/src/version_hints.rs" >"$wt7/version_hints.probe.rs"
mv "$wt7/version_hints.probe.rs" "$wt7/cqlite-core/src/version_hints.rs"
grep -q 'PubSurfaceSelfTestVariant,' "$wt7/cqlite-core/src/version_hints.rs" \
  || fail_case "case 7 setup: could not add the probe variant to VersionSource"
set +e
bash "$wt7/$GUARD_REL" >"$TMPROOT/case7.out" 2>&1
case7_rc=$?
set -e
[ "$case7_rc" -ne 0 ] || fail_case "case 7 — a new enum variant did not trip the snapshot; got: $(cat "$TMPROOT/case7.out")"
grep -q "^+variant cqlite_core::version_hints::VersionSource::PubSurfaceSelfTestVariant" "$TMPROOT/case7.out" \
  || fail_case "case 7 — the guard failed but the diff never recorded the new variant at its real path; got: $(grep -i pubsurfaceselftestvariant "$TMPROOT/case7.out"; tail -20 "$TMPROOT/case7.out")"
echo "OK (7): a new enum variant is named in the diff"

# ---------------------------------------------------------------------------
# 8. RED — a cosmetic `cfg_attr` must NOT buy an exemption from the consistency
#    assert (roborev round 1, B2).
#
#    Treating every `cfg_attr` as an exemption reopened the bypass the assert
#    exists to close: the module keeps its real gate hidden inside its own file
#    while a purely cosmetic attribute at the declaration site silences the check.
# ---------------------------------------------------------------------------
scratch_tree cfg-attr-bypass; wt8="$SCRATCH"
awk '
  /^#\[cfg\(feature = "benchmarks"\)\]$/ { held = 1; next }
  {
    if (held && $0 == "pub mod benchmarks;") {
      print "#[cfg_attr(feature = \"benchmarks\", doc = \"opt-in perf runs\")]"
    } else if (held) {
      print "#[cfg(feature = \"benchmarks\")]"
    }
    held = 0
    print
  }
' "$wt8/cqlite-core/src/lib.rs" >"$wt8/lib.rs.cfgattr"
mv "$wt8/lib.rs.cfgattr" "$wt8/cqlite-core/src/lib.rs"
grep -q '^#\[cfg_attr(feature = "benchmarks", doc = "opt-in perf runs")\]$' "$wt8/cqlite-core/src/lib.rs" \
  || fail_case "case 8 setup: could not substitute the cosmetic cfg_attr at the declaration site"
grep -q '^#\[cfg(feature = "benchmarks")\]$' "$wt8/cqlite-core/src/lib.rs" \
  && fail_case "case 8 setup: a real declaration-site cfg gate survived — the case would pass for the wrong reason"
printf '%s\n%s\n' '#![cfg(feature = "benchmarks")]' "$(cat "$wt8/cqlite-core/src/benchmarks/mod.rs")" \
  >"$wt8/cqlite-core/src/benchmarks/mod.rs.new"
mv "$wt8/cqlite-core/src/benchmarks/mod.rs.new" "$wt8/cqlite-core/src/benchmarks/mod.rs"
set +e
bash "$wt8/$GUARD_REL" >"$TMPROOT/case8.out" 2>&1
case8_rc=$?
set -e
[ "$case8_rc" -ne 0 ] || {
  echo "FAIL: case 8 — a purely cosmetic \`cfg_attr\` at the declaration site bought an"
  echo "      exemption from the consistency assert, so the module kept hiding its real"
  echo "      gate inside its own file. That is the roborev B2 bypass."
  cat "$TMPROOT/case8.out"
  exit 1
}
grep -q "INCONSISTENT with the real public surface" "$TMPROOT/case8.out" \
  || fail_case "case 8 — the guard failed for some OTHER reason than the consistency assert; got: $(cat "$TMPROOT/case8.out")"
grep -q "pub mod benchmarks" "$TMPROOT/case8.out" \
  || fail_case "case 8 — the guard failed but never named \`benchmarks\`; got: $(cat "$TMPROOT/case8.out")"
echo "OK (8): a cosmetic cfg_attr does not exempt a crate-root pub mod from the assert"

echo ""
echo "PASS: test_pub_surface_guard.sh — all 8 cases (1 green, 6 reds, 1 usage)"
