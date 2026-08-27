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
#   Crate-root PARSE shapes (lead review round 2). The scan is a lexical scan with a
#   pinned edge-case suite, not a Rust parser, so every shape it claims to handle is
#   pinned here:
#   9.  RED    — a SAME-LINE `#[attr] pub mod x;` must be seen at all. The old
#                accumulator dropped it entirely, so the module escaped the assert
#                AND the snapshot: a false PASS.
#  10.  SHAPES — a multi-line attribute must join onto its declaration, a trailing
#                `// comment` must not leak into the module name, and a `pub mod`
#                inside a `/* */` block must stay a phantom. All three used to be
#                false FAILs.
#  11.  RED    — when the two independent crate-root derivations disagree the guard
#                must REFUSE, not quietly use the smaller set.
#  12.  PINNED — plain / `#[cfg]` / `#[doc(hidden)]` / multi-line `pub use`, the
#                shapes that always worked, asserted straight off the snapshot.
#
#   Public-surface ENUMERATION and ATTRIBUTE reading (roborev round 2):
#  13.  RED    — deleting a public RE-EXPORT of an otherwise-public item must red and
#                name it. A filesystem walk cannot see this at all.
#  14.  GREEN  — renaming a PRIVATE, only-re-exported-through module must NOT be a
#                public API change.
#  15.  RED    — a tell-tale token (`doc(hidden)`, `cfg(`) inside an attribute's
#                STRING VALUE must not exempt a declaration from the assert.
#  16.  GREEN  — a real `#[cfg]` separated from its item by blank/comment lines must
#                still gate it.
#
#   Trust boundaries of the measurement itself (roborev round 3):
#  17.  GREEN  — a scratch built from a DIRTY tree (uncommitted API change + its
#                regenerated snapshot) must verify clean, so the ordinary
#                change -> regenerate -> test -> commit workflow is not a booby trap.
#  18.  RED    — a `doc` cfg predicate must make the guard REFUSE: `cargo doc`
#                compiles with `doc` SET, so a `#[cfg(not(doc))]` item ships while
#                being invisible to rustdoc and to this snapshot.
#  19.  KILL   — a killed run must not leave registered git worktrees behind (measured:
#                a 2-minute tool timeout left 11, and `git worktree prune` could not
#                reclaim them). Pins the trap list structurally, that cleanup reclaims
#                such a worktree BY EXPLICIT PATH, and — the regression guard — that a
#                fresh run leaves a CONCURRENT run's scratch worktrees alone.
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

# Scratch-worktree management lives in a shared library so the KILL-SAFETY case can
# drive the very same code path from a tiny second process (see case 19). It owns the
# scratch root, the cleanup and the EXIT/INT/TERM/HUP traps.
# shellcheck source=scripts/tests/lib/pub-surface-scratch-lib.sh
. "$REPO_ROOT/scripts/tests/lib/pub-surface-scratch-lib.sh"
ps_scratch_init "$REPO_ROOT"
TMPROOT="$PS_TMPROOT"
scratch_tree_from() { ps_scratch_tree_from "$@"; }
scratch_tree() { ps_scratch_tree_from "$REPO_ROOT" "$1"; }

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
# 3/6/7. RED — new public surface must appear in the diff, at its real path.
#
#    Three additions in ONE scratch checkout (they share a `cargo doc`, which keeps
#    the suite fast) with three independent assertions:
#      3 — a new standalone `pub struct`,
#      6 — a new `pub fn` on an ALREADY-PUBLIC struct,
#      7 — a new VARIANT on an ALREADY-PUBLIC enum.
#    6 and 7 are the cases the first cut of the guard could NOT see: it enumerated
#    only standalone rustdoc pages, so an added associated item moved nothing.
#
#    The variant is added together with its arms in the two exhaustive matches in
#    the same file — without them the crate would not compile, `cargo doc` would
#    fail, and the case would be exercising the cargo-doc failure path instead of
#    variant coverage.
# ---------------------------------------------------------------------------
scratch_tree new-surface; wt3="$SCRATCH"
awk '
  { print }
  /^    Unknown,$/ && !seen_variant { print "    /// Self-test-only probe variant (#1712)."; print "    PubSurfaceSelfTestVariant,"; seen_variant = 1 }
  /VersionSource::Unknown => 255,/ { print "            VersionSource::PubSurfaceSelfTestVariant => 254," }
  /VersionSource::Unknown => "Unknown \(no version information available\)",/ { print "            VersionSource::PubSurfaceSelfTestVariant => \"self-test probe\"," }
' "$wt3/cqlite-core/src/version_hints.rs" >"$wt3/version_hints.probe.rs"
mv "$wt3/version_hints.probe.rs" "$wt3/cqlite-core/src/version_hints.rs"
grep -q 'PubSurfaceSelfTestVariant,' "$wt3/cqlite-core/src/version_hints.rs" \
  || fail_case "case 7 setup: could not add the probe variant to VersionSource"
cat >>"$wt3/cqlite-core/src/version_hints.rs" <<'RS'

/// Self-test-only probe item (scripts/tests/test_pub_surface_guard.sh, issue #1712).
/// Exists solely inside a throwaway scratch worktree to prove the snapshot guard
/// notices new public surface. Never committed.
pub struct PubSurfaceSelfTestProbe;

impl ResolvedVersion {
    /// Self-test-only probe method (#1712), added to an ALREADY-PUBLIC struct.
    pub fn pub_surface_self_test_probe(&self) -> bool {
        true
    }
}
RS
set +e
bash "$wt3/$GUARD_REL" >"$TMPROOT/case3.out" 2>&1
case3_rc=$?
set -e
[ "$case3_rc" -ne 0 ] || fail_case "case 3/6/7 — new public surface did not trip the snapshot diff; got: $(cat "$TMPROOT/case3.out")"
grep -q -- "--regenerate" "$TMPROOT/case3.out" \
  || fail_case "case 3 — the drift diagnostic did not print the regenerate command"
grep -q "^+struct cqlite_core::version_hints::PubSurfaceSelfTestProbe$" "$TMPROOT/case3.out" \
  || fail_case "case 3 — a new standalone \`pub struct\` was not recorded at its real path; got: $(grep PubSurfaceSelfTestProbe "$TMPROOT/case3.out")"
echo "OK (3): a new standalone public item is named in the diff"
grep -q "^+method cqlite_core::version_hints::ResolvedVersion::pub_surface_self_test_probe$" "$TMPROOT/case3.out" \
  || fail_case "case 6 — a new \`pub fn\` on an existing public struct was not recorded as a \`method\` line at its real path; got: $(grep pub_surface_self_test_probe "$TMPROOT/case3.out")"
echo "OK (6): a new pub fn on an existing public struct is named in the diff"
grep -q "^+variant cqlite_core::version_hints::VersionSource::PubSurfaceSelfTestVariant$" "$TMPROOT/case3.out" \
  || fail_case "case 7 — a new enum variant was not recorded at its real path; got: $(grep -i pubsurfaceselftestvariant "$TMPROOT/case3.out")"
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

# ---------------------------------------------------------------------------
# 9. RED — CRATE-ROOT PARSE, same-line `#[attr] pub mod x;` (lead review, defect 1).
#
#    The FALSE PASS. The old line-oriented accumulator consumed the attribute line
#    and `next`ed, so a declaration sharing its line with an attribute was dropped
#    ENTIRELY: the module escaped the consistency assert and vanished from the
#    snapshot's declarations section. Here the gate is cosmetic and the real one
#    hides inside the module file, so the assert MUST fire — pre-fix it could not,
#    because the declaration did not exist as far as the guard was concerned.
# ---------------------------------------------------------------------------
scratch_tree sameline-decl; wt9="$SCRATCH"
awk '
  /^#\[cfg\(feature = "benchmarks"\)\]$/ { held = 1; next }
  {
    if (held && $0 == "pub mod benchmarks;") {
      print "#[cfg_attr(feature = \"benchmarks\", doc = \"opt-in perf runs\")] pub mod benchmarks;"
      held = 0
      next
    }
    if (held) print "#[cfg(feature = \"benchmarks\")]"
    held = 0
    print
  }
' "$wt9/cqlite-core/src/lib.rs" >"$wt9/lib.rs.sameline"
mv "$wt9/lib.rs.sameline" "$wt9/cqlite-core/src/lib.rs"
grep -qx '#\[cfg_attr(feature = "benchmarks", doc = "opt-in perf runs")\] pub mod benchmarks;' "$wt9/cqlite-core/src/lib.rs" \
  || fail_case "case 9 setup: could not put the attribute and the declaration on one line"
printf '%s\n%s\n' '#![cfg(feature = "benchmarks")]' "$(cat "$wt9/cqlite-core/src/benchmarks/mod.rs")" \
  >"$wt9/cqlite-core/src/benchmarks/mod.rs.new"
mv "$wt9/cqlite-core/src/benchmarks/mod.rs.new" "$wt9/cqlite-core/src/benchmarks/mod.rs"
set +e
bash "$wt9/$GUARD_REL" >"$TMPROOT/case9.out" 2>&1
case9_rc=$?
set -e
[ "$case9_rc" -ne 0 ] || fail_case "case 9 — a same-line \`#[attr] pub mod x;\` passed; got: $(cat "$TMPROOT/case9.out")"
grep -q "INCONSISTENT with the real public surface" "$TMPROOT/case9.out" \
  || fail_case "case 9 — the consistency assert did NOT fire on a same-line declaration, so the declaration was dropped by the crate-root scan (the false PASS); got: $(cat "$TMPROOT/case9.out")"
grep -q "pub mod benchmarks" "$TMPROOT/case9.out" \
  || fail_case "case 9 — the guard failed but never named \`benchmarks\`; got: $(cat "$TMPROOT/case9.out")"
echo "OK (9): a same-line \`#[attr] pub mod x;\` is seen by the crate-root scan and asserted"

# ---------------------------------------------------------------------------
# 10. GREEN-ish — the three FALSE-FAIL shapes (lead review, defects 2-4).
#
#     Multi-line attributes, a trailing `// comment`, and a `pub mod` inside a
#     `/* */` block. Each used to produce a spurious INCONSISTENT. The guard still
#     exits non-zero here (the snapshot legitimately changed — new declarations),
#     so the assertion is on the CONTENT: no consistency complaint at all, and each
#     shape rendered correctly in the declarations diff.
# ---------------------------------------------------------------------------
scratch_tree parse-shapes; wt10="$SCRATCH"
cat >"$wt10/cqlite-core/src/probe_trailing.rs" <<'RS'
//! Self-test-only probe module (scripts/tests/test_pub_surface_guard.sh, #1712).
RS
cat >>"$wt10/cqlite-core/src/lib.rs" <<'RS'

#[cfg(all(
    feature = "probe-a",
    feature = "probe-b"
))]
pub mod probe_multiline;

pub mod probe_trailing; // a trailing comment must not land inside the module name

/*
pub mod probe_phantom;
*/
RS
set +e
bash "$wt10/$GUARD_REL" >"$TMPROOT/case10.out" 2>&1
set -e
grep -q "INCONSISTENT" "$TMPROOT/case10.out" \
  && fail_case "case 10 — a correctly-written crate root was reported INCONSISTENT (false FAIL); got: $(cat "$TMPROOT/case10.out")"
grep -q "probe_phantom" "$TMPROOT/case10.out" \
  && fail_case "case 10 — a \`pub mod\` inside a /* */ block was recorded as a real declaration (phantom)"
grep -qF '+#[cfg(all( feature = "probe-a", feature = "probe-b" ))] pub mod probe_multiline;' "$TMPROOT/case10.out" \
  || fail_case "case 10 — a MULTI-LINE attribute was not joined onto its declaration; got: $(grep probe_multiline "$TMPROOT/case10.out")"
grep -qxF '+pub mod probe_trailing;' "$TMPROOT/case10.out" \
  || fail_case "case 10 — a trailing // comment leaked into the recorded declaration; got: $(grep probe_trailing "$TMPROOT/case10.out")"
echo "OK (10): multi-line attrs join, trailing comments strip, block-commented decls stay phantoms"

# ---------------------------------------------------------------------------
# 11. RED — the two independent crate-root derivations must DISAGREE loudly when
#     the structured scan under-collects (lead review: the fail-safe split).
#
#     Two gated declarations on one line: the simple scan finds both, the
#     structured scan stops after the first. Neither result can be trusted, so the
#     guard must refuse rather than assert over the smaller set.
# ---------------------------------------------------------------------------
scratch_tree scan-disagreement; wt11="$SCRATCH"
cat >>"$wt11/cqlite-core/src/lib.rs" <<'RS'

#[cfg(feature = "probe-a")] pub mod probe_first; #[cfg(feature = "probe-a")] pub mod probe_second;
RS
set +e
bash "$wt11/$GUARD_REL" >"$TMPROOT/case11.out" 2>&1
case11_rc=$?
set -e
[ "$case11_rc" -ne 0 ] || fail_case "case 11 — the structured scan under-collected and the guard still passed; got: $(cat "$TMPROOT/case11.out")"
grep -q "disagree about which modules the crate root declares" "$TMPROOT/case11.out" \
  || fail_case "case 11 — the guard failed but not with the scan-disagreement diagnostic; got: $(cat "$TMPROOT/case11.out")"
grep -q "probe_second" "$TMPROOT/case11.out" \
  || fail_case "case 11 — the disagreement diagnostic did not name the module only one scan saw"
echo "OK (11): the two crate-root derivations disagreeing is a loud FAIL, not a silent smaller set"

# ---------------------------------------------------------------------------
# 12. The three shapes that ALWAYS worked, pinned so a future rewrite cannot lose
#     them. Read straight off the committed snapshot — no worktree, no cargo.
# ---------------------------------------------------------------------------
decls_section() { sed -n '/^## crate-root-declarations/,$p' "$REPO_ROOT/$SNAPSHOT_REL"; }
decls_section | grep -qx 'pub mod config;' \
  || fail_case "case 12 — a PLAIN \`pub mod\` is missing from the snapshot's declarations section"
decls_section | grep -qx '#\[cfg(feature = "state_machine")\] pub mod query;' \
  || fail_case "case 12 — a normal-form \`#[cfg]\` declaration is missing or mis-rendered"
decls_section | grep -qx '#\[doc(hidden)\] pub mod testing;' \
  || fail_case "case 12 — a \`#[doc(hidden)]\` declaration is missing or mis-rendered"
decls_section | grep -q '^pub use crate::{ config::Config,' \
  || fail_case "case 12 — a MULTI-LINE \`pub use\` was not joined onto one recorded line"
echo "OK (12): plain / #[cfg] / #[doc(hidden)] / multi-line pub use are all pinned in the snapshot"

# ---------------------------------------------------------------------------
# 13. RED — deleting a PUBLIC RE-EXPORT of an otherwise-public item (roborev r2 F1).
#
#     `schema::SchemaLoadWarning` is public through `pub use aggregator::{…}`. The
#     type itself stays public at its canonical path, so nothing about the
#     filesystem tree changes — which is exactly why a directory walk passed this
#     breaking change green. Removing the re-export must now RED and name it.
# ---------------------------------------------------------------------------
scratch_tree drop-reexport; wt13="$SCRATCH"
awk '{ if ($0 == "    SchemaLoadWarning,") next; print }' \
  "$wt13/cqlite-core/src/schema/mod.rs" >"$wt13/schema.mod.rs"
mv "$wt13/schema.mod.rs" "$wt13/cqlite-core/src/schema/mod.rs"
grep -q '^    SchemaLoadWarning,$' "$wt13/cqlite-core/src/schema/mod.rs" \
  && fail_case "case 13 setup: could not drop SchemaLoadWarning from the re-export list"
set +e
bash "$wt13/$GUARD_REL" >"$TMPROOT/case13.out" 2>&1
case13_rc=$?
set -e
[ "$case13_rc" -ne 0 ] || fail_case "case 13 — deleting a public re-export passed GREEN. That is a breaking change the guard must see; got: $(cat "$TMPROOT/case13.out")"
grep -q "^-reexport cqlite_core::schema::SchemaLoadWarning = " "$TMPROOT/case13.out" \
  || fail_case "case 13 — the guard failed but the diff did not name the deleted re-export; got: $(grep -i schemaloadwarning "$TMPROOT/case13.out")"
echo "OK (13): deleting a public re-export REDs and names it"

# ---------------------------------------------------------------------------
# 14. GREEN — renaming a PRIVATE module that only re-exports through is NOT a
#     public API change (roborev r2 F2).
#
#     `schema::udt_registry` is `mod`, not `pub mod`: nothing public reaches it, and
#     its items are public only via `pub use udt_registry::{…}`. A directory walk
#     recorded it and its items, so this rename read as an API change — noise, and
#     the kind of noise that teaches people to regenerate without reading. The
#     guard must now PASS OUTRIGHT.
# ---------------------------------------------------------------------------
scratch_tree rename-private-mod; wt14="$SCRATCH"
mv "$wt14/cqlite-core/src/schema/udt_registry.rs" "$wt14/cqlite-core/src/schema/udt_registry_renamed.rs"
sed -e 's/^mod udt_registry;$/mod udt_registry_renamed;/' \
    -e 's/^pub use udt_registry::/pub use udt_registry_renamed::/' \
    "$wt14/cqlite-core/src/schema/mod.rs" >"$wt14/schema.mod.rs"
mv "$wt14/schema.mod.rs" "$wt14/cqlite-core/src/schema/mod.rs"
grep -q '^mod udt_registry_renamed;$' "$wt14/cqlite-core/src/schema/mod.rs" \
  || fail_case "case 14 setup: could not rename the private udt_registry module"
set +e
bash "$wt14/$GUARD_REL" >"$TMPROOT/case14.out" 2>&1
case14_rc=$?
set -e
[ "$case14_rc" -eq 0 ] || {
  echo "FAIL: case 14 — renaming a PRIVATE, re-exported-through module was reported as a"
  echo "      public API change. Private module paths must not be in the snapshot at all."
  cat "$TMPROOT/case14.out"
  exit 1
}
echo "OK (14): renaming a private re-exported-through module is not a public API change"

# ---------------------------------------------------------------------------
# 15. RED — a COSMETIC attribute whose TEXT contains `doc(hidden)` / `cfg(` must not
#     exempt a declaration from the consistency assert (roborev r2 F3).
#
#     Both attributes below hide and gate exactly nothing; the tell-tale tokens are
#     inside string-literal VALUES. Under the previous substring test either one
#     bought an exemption, so a module could keep hiding its real gate inside its
#     own file — a false PASS.
# ---------------------------------------------------------------------------
scratch_tree cosmetic-attrs; wt15="$SCRATCH"
awk '
  /^#\[cfg\(feature = "benchmarks"\)\]$/ {
    print "#[doc = \"this text mentions doc(hidden) but hides nothing\"]"
    print "#[cfg_attr(docsrs, doc(alias = \"cfg(foo)\"))]"
    next
  }
  { print }
' "$wt15/cqlite-core/src/lib.rs" >"$wt15/lib.rs.cosmetic"
mv "$wt15/lib.rs.cosmetic" "$wt15/cqlite-core/src/lib.rs"
grep -q 'mentions doc(hidden) but hides nothing' "$wt15/cqlite-core/src/lib.rs" \
  || fail_case "case 15 setup: could not substitute the cosmetic attributes"
grep -q '^#\[cfg(feature = "benchmarks")\]$' "$wt15/cqlite-core/src/lib.rs" \
  && fail_case "case 15 setup: the real declaration-site cfg gate survived"
printf '%s\n%s\n' '#![cfg(feature = "benchmarks")]' "$(cat "$wt15/cqlite-core/src/benchmarks/mod.rs")" \
  >"$wt15/cqlite-core/src/benchmarks/mod.rs.new"
mv "$wt15/cqlite-core/src/benchmarks/mod.rs.new" "$wt15/cqlite-core/src/benchmarks/mod.rs"
set +e
bash "$wt15/$GUARD_REL" >"$TMPROOT/case15.out" 2>&1
case15_rc=$?
set -e
[ "$case15_rc" -ne 0 ] || fail_case "case 15 — cosmetic attributes bought an exemption; got: $(cat "$TMPROOT/case15.out")"
grep -q "INCONSISTENT with the real public surface" "$TMPROOT/case15.out" \
  || fail_case "case 15 — the consistency assert did NOT fire: an attribute whose STRING VALUE mentions doc(hidden)/cfg( was read as structure; got: $(cat "$TMPROOT/case15.out")"
grep -q "pub mod benchmarks" "$TMPROOT/case15.out" \
  || fail_case "case 15 — the guard failed but never named \`benchmarks\`"
echo "OK (15): a tell-tale token inside an attribute VALUE does not exempt a declaration"

# ---------------------------------------------------------------------------
# 16. GREEN — a real `#[cfg]` separated from its item by a blank line and comments
#     still gates it (roborev r2 F4).
#
#     Rust permits blank lines, `//` comments and `///` doc comments between an
#     attribute and the item it applies to. Breaking the attribute run on them made
#     a genuinely gated module read as unconditional — a false FAIL. The guard must
#     PASS OUTRIGHT here: the rendered declaration is unchanged, so there is not
#     even a snapshot diff.
# ---------------------------------------------------------------------------
scratch_tree separated-attr; wt16="$SCRATCH"
awk '
  /^#\[cfg\(feature = "benchmarks"\)\]$/ {
    print
    print ""
    print "/// A doc comment between the gate and the item."
    print "// …and an ordinary comment too."
    print ""
    next
  }
  { print }
' "$wt16/cqlite-core/src/lib.rs" >"$wt16/lib.rs.separated"
mv "$wt16/lib.rs.separated" "$wt16/cqlite-core/src/lib.rs"
grep -q 'A doc comment between the gate and the item' "$wt16/cqlite-core/src/lib.rs" \
  || fail_case "case 16 setup: could not insert the separator lines"
set +e
bash "$wt16/$GUARD_REL" >"$TMPROOT/case16.out" 2>&1
case16_rc=$?
set -e
[ "$case16_rc" -eq 0 ] || {
  echo "FAIL: case 16 — a real #[cfg] separated from its item by a blank line and comments"
  echo "      stopped gating it, so a correctly-written crate root was reported wrong."
  cat "$TMPROOT/case16.out"
  exit 1
}
echo "OK (16): a #[cfg] separated from its item by blank/comment lines still gates it"

# ---------------------------------------------------------------------------
# 17. GREEN — the ORDINARY PRE-COMMIT WORKFLOW must pass (roborev r3 F3).
#
#     Change the public API, regenerate the snapshot, run the tests BEFORE
#     committing. Scratch worktrees used to carry committed HEAD sources with the
#     live snapshot copied over them, so HEAD's API could not match the regenerated
#     baseline and the green cases failed — a false FAIL that looks exactly like a
#     real defect, sitting in the path every future contributor walks.
#
#     Proved end to end, through the REAL code path rather than a re-implementation:
#     an outer scratch stands in for a dirty working tree (a new public item plus its
#     regenerated snapshot, both UNCOMMITTED), and a child scratch is created from it
#     with the same scratch_tree_from() the whole suite uses. The child must verify
#     clean.
# ---------------------------------------------------------------------------
scratch_tree dirty-worktree; wt17="$SCRATCH"
cat >>"$wt17/cqlite-core/src/version_hints.rs" <<'RS'

/// Self-test-only probe item (scripts/tests/test_pub_surface_guard.sh, issue #1712),
/// standing in for an UNCOMMITTED public-API change. Never committed.
pub struct PubSurfaceUncommittedProbe;
RS
set +e
bash "$wt17/$GUARD_REL" --regenerate >"$TMPROOT/case17-regen.out" 2>&1
case17_regen_rc=$?
set -e
[ "$case17_regen_rc" -eq 0 ] \
  || fail_case "case 17 setup: --regenerate failed in the outer scratch; got: $(cat "$TMPROOT/case17-regen.out")"
git -C "$wt17" status --porcelain | grep -q 'pub-surface.snapshot' \
  || fail_case "case 17 setup: the regenerated snapshot is not an uncommitted change in the outer scratch"

scratch_tree_from "$wt17" dirty-worktree-child; wt17c="$SCRATCH"
grep -q 'PubSurfaceUncommittedProbe' "$wt17c/cqlite-core/src/version_hints.rs" \
  || fail_case "case 17 — the child scratch did not receive the outer scratch's uncommitted source change"
grep -q 'PubSurfaceUncommittedProbe' "$wt17c/$SNAPSHOT_REL" \
  || fail_case "case 17 — the child scratch did not receive the outer scratch's regenerated snapshot"
set +e
bash "$wt17c/$GUARD_REL" >"$TMPROOT/case17.out" 2>&1
case17_rc=$?
set -e
[ "$case17_rc" -eq 0 ] || {
  echo "FAIL: case 17 — a scratch checkout built from a tree with an UNCOMMITTED public-API"
  echo "      change plus its regenerated snapshot did not verify clean. Scratch source and"
  echo "      baseline are describing different trees, which breaks the ordinary"
  echo "      change-API -> regenerate -> run-tests -> commit workflow."
  cat "$TMPROOT/case17.out"
  exit 1
}
echo "OK (17): a scratch built from a DIRTY tree (uncommitted API change + regenerated snapshot) verifies clean"

# ---------------------------------------------------------------------------
# 18. RED — the crate must not use a `doc` cfg predicate (roborev r3 F1).
#
#     `cargo doc` compiles with `doc` SET, so an item behind `#[cfg(not(doc))]`
#     ships but never reaches rustdoc — invisible to this guard and to every other
#     rustdoc-derived oracle. The guard must REFUSE rather than certify a surface it
#     knows may differ from the compiled one.
# ---------------------------------------------------------------------------
scratch_tree cfg-doc; wt18="$SCRATCH"
cat >>"$wt18/cqlite-core/src/version_hints.rs" <<'RS'

#[cfg(not(doc))]
/// Self-test-only probe (#1712): present in a normal build, invisible to rustdoc.
pub fn pub_surface_cfg_not_doc_probe() {}
RS
set +e
bash "$wt18/$GUARD_REL" >"$TMPROOT/case18.out" 2>&1
case18_rc=$?
set -e
[ "$case18_rc" -ne 0 ] || fail_case "case 18 — a \`cfg(not(doc))\` item passed GREEN. It ships but never reaches rustdoc, so the snapshot cannot be trusted; got: $(cat "$TMPROOT/case18.out")"
grep -q 'cfg cfg\|`doc` cfg predicate' "$TMPROOT/case18.out" \
  || fail_case "case 18 — the guard failed but not with the cfg(doc) diagnostic; got: $(cat "$TMPROOT/case18.out")"
grep -q 'cargo-public-api' "$TMPROOT/case18.out" \
  || fail_case "case 18 — the diagnostic did not record that the blind spot is shared by every rustdoc-derived oracle"
echo "OK (18): a \`doc\` cfg predicate makes the guard REFUSE rather than certify"

# ---------------------------------------------------------------------------
# 19. KILL SAFETY — cleanup must reclaim a registered worktree, BY EXPLICIT PATH,
#     and must never touch a concurrent run's.
#
#     THE OBSERVED FAILURE. A 2-minute tool timeout on this suite left ELEVEN
#     registered worktrees in the repository, and `git worktree prune` could not
#     reclaim them because their directories and admin files were intact; they had to
#     be removed by hand. This suite runs for minutes inside `tooling-tests`, and
#     CLAUDE.md records the 600s stall watchdog, so this is routine.
#
#     WHY THE TRAP LIST IS ONLY HALF OF IT, measured rather than assumed: `kill -TERM
#     <pid>` on the script alone runs the EXIT trap ANYWAY, so a single-PID signal test
#     passes with or without INT/TERM trapped and cannot discriminate; a PROCESS-GROUP
#     kill skips the traps even when they are installed; and SIGKILL cannot be trapped.
#     A post-SIGKILL leftover is therefore ACCEPTED as rare manual cleanup — see the
#     library comment for why a name-shape sweep is not the answer.
#
#     So what is pinned here is what cleanup must actually do, plus the property that
#     stops the tempting-but-destructive "fix" from coming back.
# ---------------------------------------------------------------------------
_lib="$REPO_ROOT/scripts/tests/lib/pub-surface-scratch-lib.sh"
for _sig in INT TERM HUP; do
  grep -qE "^  trap 'ps_cleanup; exit [0-9]+' $_sig\$" "$_lib" \
    || fail_case "case 19 — the scratch library no longer traps $_sig. It is what covers the catchable kills; do not drop it."
done
grep -qE "^  trap 'ps_cleanup' EXIT\$" "$_lib" \
  || fail_case "case 19 — the EXIT trap is gone from the scratch library; the normal-exit path would stop cleaning up"
grep -qF 'PS_CLEANED=1' "$_lib" \
  || fail_case "case 19 — cleanup is no longer idempotent, but a signal handler runs it and then the EXIT trap runs it again"

# (a) Manufacture exactly the state a killed run leaves — a REGISTERED worktree whose
#     directory and admin files are intact, which is the state `git worktree prune`
#     refuses to reclaim — and remove it BY EXPLICIT PATH through the same call the
#     cleanup path uses.
leaked_root="$(mktemp -d "${TMPDIR:-/tmp}/pub-surface-selftest.leakedXXXXXX")"
git -C "$REPO_ROOT" worktree add --detach --quiet "$leaked_root/leaked" HEAD >/dev/null 2>&1 \
  || fail_case "case 19 setup: could not create the decoy leaked worktree"
git -C "$REPO_ROOT" worktree list --porcelain | grep -qF "$leaked_root/leaked" \
  || fail_case "case 19 setup: the decoy worktree was never registered, so the case would prove nothing"
git -C "$REPO_ROOT" worktree prune >/dev/null 2>&1 || true
git -C "$REPO_ROOT" worktree list --porcelain | grep -qF "$leaked_root/leaked" \
  || fail_case "case 19 setup: \`git worktree prune\` reclaimed the decoy, so it is not the state a killed run leaves"

ps_remove_worktree "$REPO_ROOT" "$leaked_root/leaked"

git -C "$REPO_ROOT" worktree list --porcelain | grep -qF "$leaked_root/leaked" \
  && { echo "FAIL: case 19 — cleanup left a registered worktree behind:"
       git -C "$REPO_ROOT" worktree list | grep -F "pub-surface-selftest" || true
       rm -rf "$leaked_root"; exit 1; }
[ ! -d "$leaked_root/leaked" ] || fail_case "case 19 — cleanup dropped the registration but left the directory $leaked_root/leaked"
rm -rf "$leaked_root"

# (b) THE REGRESSION GUARD FOR THE DESTRUCTIVE "FIX". A startup sweep over every
#     registered worktree whose path merely LOOKS like ours would delete a CONCURRENT
#     run's live checkouts — certainly, not as a race — and across all five
#     `/data/lanes/lane-*` worktrees of this one repository. So: stand up a decoy that
#     represents another live run, start a FRESH init in a second process (the real
#     startup path), and require the decoy to SURVIVE it.
peer_root="$(mktemp -d "${TMPDIR:-/tmp}/pub-surface-selftest.peerXXXXXX")"
git -C "$REPO_ROOT" worktree add --detach --quiet "$peer_root/live" HEAD >/dev/null 2>&1 \
  || fail_case "case 19 setup: could not create the concurrent-run decoy"
cat >"$TMPROOT/peer-probe.sh" <<PEER
#!/usr/bin/env bash
set -euo pipefail
. "$REPO_ROOT/scripts/tests/lib/pub-surface-scratch-lib.sh"
ps_scratch_init "$REPO_ROOT"
PEER
bash "$TMPROOT/peer-probe.sh" >"$TMPROOT/peer-probe.out" 2>&1 \
  || fail_case "case 19 setup: a bare init failed; got: $(cat "$TMPROOT/peer-probe.out")"
if ! git -C "$REPO_ROOT" worktree list --porcelain | grep -qF "$peer_root/live"; then
  echo "FAIL: case 19 — starting a run DESTROYED a concurrent run's live scratch worktree"
  echo "      ($peer_root/live). Scratch removal must be by EXPLICIT PATH; a sweep over"
  echo "      paths that look like ours deletes peers, and this repository's worktree"
  echo "      registry is shared across every /data/lanes/lane-* checkout."
  rm -rf "$peer_root"; exit 1
fi
[ -d "$peer_root/live" ] \
  || { echo "FAIL: case 19 — a concurrent run's scratch DIRECTORY was deleted by a fresh init"; rm -rf "$peer_root"; exit 1; }
ps_remove_worktree "$REPO_ROOT" "$peer_root/live"
rm -rf "$peer_root"
# …and this run's own scratch root must still be there.
[ -d "$TMPROOT" ] || fail_case "case 19 — the live run's own scratch root was deleted"
echo "OK (19): cleanup reclaims a registered worktree by explicit path, and a fresh run leaves a concurrent run's alone"

echo ""
echo "PASS: test_pub_surface_guard.sh — all 19 cases (6 green, 11 reds, 1 usage, 1 kill-safety)"
