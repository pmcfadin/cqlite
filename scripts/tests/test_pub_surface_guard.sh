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
grep -q "public items over" "$TMPROOT/green.out" \
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
rm -f "$wt4/cqlite-core/pub-surface.snapshot"
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

echo ""
echo "PASS: test_pub_surface_guard.sh — all 5 cases (1 green, 3 reds, 1 usage)"
