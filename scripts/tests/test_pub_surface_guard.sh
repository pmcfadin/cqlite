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
#   A SHARED blind spot of the two derivations (roborev r4 F2):
#  20.  RED    — an INLINE crate-root `pub mod NAME { #![cfg(…)] … }` is invisible to
#                BOTH derivations, so they AGREE while both are blind and the gate
#                hiding inside the body passes green. The scan must REFUSE over any
#                top-level `pub mod` form it does not recognise.
#
#   Trust boundaries, second pass (roborev r4 F1/F3) — both were FALSE PASSES:
#  21.  RED    — the §1b `doc`-cfg refusal was itself a lexical stand-in for a
#                structural read, and was defeated THREE times: `#[cfg(doc)]` and
#                `#[cfg_attr(not(doc), cfg(any()))]` sailed past the original grep,
#                and `#[cfg(/* explanation */ doc)]` sailed past the meta-item walk
#                that replaced it — each a CERTIFIED snapshot listing an item the
#                shipped crate does not have. TEN shapes must now refuse, including
#                the three comment-bearing ones, `#[cfg_attr(doc, doc = "x")]` (bare
#                CONDITION, assigned attribute) and the DELIBERATE over-fire on
#                attribute-position `doc(hidden)` — while CONDITIONAL DOCUMENTATION
#                (`#[cfg_attr(feature = "x", doc = "…")]`) must CERTIFY, since
#                `doc = "…"` is never a cfg predicate.
#  22.  RED    — a RELATIVE `CARGO_TARGET_DIR` was resolved against the CALLER's cwd
#                while cargo resolves it against the repo root, so the guard locked,
#                deleted and inspected a different tree than the one cargo wrote.
#
#   Trust boundaries, third pass (roborev r6 F3) — a FALSE FAIL:
#  27.  GREEN  — the same workflow as case 17 but with the change `git add`ed. The
#                overlay reproduced `git diff HEAD` (staged content) as an UNSTAGED
#                edit while the proof step compared porcelain EXACTLY, and porcelain
#                distinguishes `M ` from ` M` — so staging anything aborted the suite
#                before it tested anything. Staged and unstaged deltas are reproduced
#                separately; both index states are pinned here.
#
#   The COMPONENT that certifies the guard (roborev r6 F2) — also a FALSE PASS:
#  26.  RED    — `agent-gate.sh`'s pub-surface component assigned PASS on the guard's
#                EXIT STATUS alone, echoing the measurement line with `|| true`. So a
#                guard that exited 0 having enumerated nothing reported
#                `pub-surface: PASS` — a pass derived from the absence of a bad signal,
#                in the one component that certifies the public-API guard. The
#                component must require the affirmative measurement line and otherwise
#                record a NAMED failure. Carries its own positive control.
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
#
#     This case pins the plainest shape. Case 21 pins the shapes that defeated the
#     three detectors §1b has had, and the deliberate over-approximation of the blunt
#     one that replaced them.
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
# 20. RED — an INLINE crate-root `pub mod NAME { … }` is a SHARED BLIND SPOT of the
#     two derivations, so the cross-check cannot see it (roborev r4 F2).
#
#     Both derivations recognise only the STATEMENT form `pub mod NAME;`. An inline
#     module declaration is invisible to BOTH, so they AGREE (each derived the empty
#     set for it) while both are blind — and a `#![cfg(feature = "…")]` gate hiding
#     inside that inline body, i.e. EXACTLY the bypass this assert was filed to
#     close, passed GREEN. A shared blind spot is not a disagreement, so the
#     mutual cross-check can never catch it: the scan has to REFUSE instead.
# ---------------------------------------------------------------------------
scratch_tree inline-mod; wt20="$SCRATCH"
cat >>"$wt20/cqlite-core/src/lib.rs" <<'RS'

pub mod probe_inline_gated {
    #![cfg(feature = "benchmarks")]
    /// Self-test-only probe (#1712 r4 F2): the gate hides INSIDE the inline body.
    pub fn probe() {}
}
RS
set +e
bash "$wt20/$GUARD_REL" >"$TMPROOT/case20.out" 2>&1
case20_rc=$?
set -e
[ "$case20_rc" -ne 0 ] || fail_case "case 20 — an inline crate-root \`pub mod NAME { … }\` hiding a \`#![cfg]\` gate in its body passed GREEN. Both derivations are blind to the inline form, so their cross-check AGREES while neither saw it; got: $(cat "$TMPROOT/case20.out")"
grep -q "unrecognized top-level \`pub mod\` form" "$TMPROOT/case20.out" \
  || fail_case "case 20 — the guard failed but not with the unrecognized-\`pub mod\`-form refusal; got: $(cat "$TMPROOT/case20.out")"
grep -q "probe_inline_gated" "$TMPROOT/case20.out" \
  || fail_case "case 20 — the refusal did not name the offending declaration; got: $(cat "$TMPROOT/case20.out")"
grep -qE "line [0-9]+" "$TMPROOT/case20.out" \
  || fail_case "case 20 — the refusal did not name the offending LINE, so an operator cannot act on it; got: $(cat "$TMPROOT/case20.out")"
echo "OK (20): an inline crate-root \`pub mod NAME { … }\` makes the scan REFUSE (shared blind spot, not a disagreement)"

# ---------------------------------------------------------------------------
# 21. RED — every shape that carries a bare `doc` TOKEN inside a `cfg`/`cfg_attr`
#     attribute must make the guard REFUSE, INCLUDING the shapes that defeated its
#     three previous, cleverer detectors (roborev r4 F1, r6 F1) — while CONDITIONAL
#     DOCUMENTATION (`doc = "…"`, an assignment, never a cfg predicate) must CERTIFY.
#
#     HISTORY, because it is the whole reason this case exists. §1b has been the
#     guard's own instance of the defect class it fences off — a LEXICAL pattern
#     standing in for a STRUCTURAL read — three times over:
#
#       #[cfg(doc)]                          -> PASSED GREEN   (false PASS, r4 F1)
#       #[cfg_attr(not(doc), cfg(any()))]    -> PASSED GREEN   (false PASS, r4 F1)
#       #[cfg_attr(doc, doc(hidden))]        -> failed, but with the SNAPSHOT-DRIFT
#                                               diagnostic; the refusal never fired
#       #[cfg(/* explanation */ doc)]        -> PASSED GREEN   (false PASS, r6 F1:
#                                               the meta-item walk that fixed the
#                                               two above did not skip comments)
#
#     MEASURED against the meta-item walk immediately before this change, all three
#     comment-bearing shapes below exited 0 with the refusal never firing: a block
#     comment before the predicate, a `//` comment inside a multi-line attribute, and
#     a multi-line `cfg(all(…))` with a comment in it.
#
#     Under `cargo doc` the `doc` cfg is SET, so `#[cfg(doc)]` KEEPS an item in
#     rustdoc's output while a default build DROPS it, and `cfg_attr(not(doc),
#     cfg(any()))` is the mirror image — rustdoc sees the item, the shipped crate
#     does not. In both cases the guard certified a snapshot listing a public item the
#     compiled crate does not have.
#
#     The detector is now DELIBERATELY BLUNT (arm on a bare `cfg`/`cfg_attr` token,
#     disarm at the next `]`, refuse on a bare `doc` token seen while armed), so
#     there is no predicate-position analysis left to defeat. Its ONE exclusion is a
#     `doc` token immediately followed by `=`; the last sub-case below pins that
#     conditional documentation therefore CERTIFIES, and `#[cfg_attr(doc, doc = "x")]`
#     in the shape list pins that the exclusion did not weaken the CONDITION check.
# ---------------------------------------------------------------------------
scratch_tree cfg-doc-shapes; wt21="$SCRATCH"
t21="$wt21/cqlite-core/src/version_hints.rs"
cp "$t21" "$TMPROOT/case21.orig.rs"
# Attach the attribute to an ALREADY-PUBLIC item, deliberately: a brand-new item
# would move the snapshot and red as ordinary drift, which cannot tell a fired
# refusal from an unfired one.
#
# Shapes are given with `\n` escapes and rendered with `printf %b`, so MULTI-LINE
# attributes are testable — a single-line-only shape list is how the split-attribute
# blind spot survived as long as it did.
apply21() {
  printf '%b\n' "$1" >"$TMPROOT/case21.attr"
  awk -v f="$TMPROOT/case21.attr" \
    '/^pub struct VersionHintResolver;/ && !done { while ((getline l < f) > 0) print l; done = 1 } { print }' \
    "$TMPROOT/case21.orig.rs" >"$t21"
  # Proof the substitution landed, line by line: a case whose subject never reached
  # the file would prove nothing.
  while IFS= read -r line21; do
    [ -n "$line21" ] || continue
    grep -qF "$line21" "$t21" \
      || fail_case "case 21 setup: could not attach \`$1\` to an existing public item (line \`$line21\` missing), so the case would prove nothing"
  done <"$TMPROOT/case21.attr"
}

c21=0
while IFS= read -r shape; do
  [ -n "$shape" ] || continue
  c21=$((c21 + 1))
  apply21 "$shape"
  set +e
  bash "$wt21/$GUARD_REL" >"$TMPROOT/case21.$c21.out" 2>&1
  rc21=$?
  set -e
  [ "$rc21" -ne 0 ] \
    || fail_case "case 21.$c21 — \`$shape\` on a public item passed GREEN. \`cargo doc\` compiles with the \`doc\` cfg SET, so the snapshot records a surface the shipped crate does not have; got: $(cat "$TMPROOT/case21.$c21.out")"
  grep -q '`doc` cfg predicate' "$TMPROOT/case21.$c21.out" \
    || fail_case "case 21.$c21 — \`$shape\` failed, but NOT with the \`doc\`-cfg-predicate refusal. A failure with another cause is not this guard firing; got: $(cat "$TMPROOT/case21.$c21.out")"
  grep -q 'version_hints.rs' "$TMPROOT/case21.$c21.out" \
    || fail_case "case 21.$c21 — the refusal did not name the offending FILE, so an operator cannot act on it; got: $(cat "$TMPROOT/case21.$c21.out")"
  grep -qE 'version_hints\.rs:[0-9]+' "$TMPROOT/case21.$c21.out" \
    || fail_case "case 21.$c21 — the refusal did not name the offending LINE, so an operator cannot act on it; got: $(cat "$TMPROOT/case21.$c21.out")"
done <<'SHAPES'
#[cfg(doc)]
#[cfg(not(doc))]
#[cfg(all(doc, unix))]
#[cfg_attr(doc, doc(hidden))]
#[cfg_attr(not(doc), cfg(any()))]
#[cfg(/* explanation */ doc)]
#[cfg_attr(doc, doc = "x")]
#[cfg( // explanation of why this is gated\n    doc\n)]
#[cfg(all(\n    /* why this shape exists */ doc,\n    unix\n))]
SHAPES
[ "$c21" -eq 9 ] || fail_case "case 21 — only $c21 of the 9 pinned shapes ran; a case that does not run cannot fail"

# …and CONDITIONAL DOCUMENTATION, which must NOT refuse. `doc` here is an
# ASSIGNMENT (`doc = "…"`), i.e. the attribute form `#[doc = "text"]` applied under a
# feature — a standard Rust idiom for prose that only appears in some builds. It gates
# NOTHING and the guard must certify normally.
#
# THIS ASSERTION WAS ONCE THE OPPOSITE, and the inversion is the point. The blunt rule
# refused on every bare `doc` token seen while armed, and this case pinned that
# over-fire as INTENDED. It was not acceptable: the shape is one anyone may add to
# cqlite-core at any time, so shipping it would have bricked the public-API gate for
# an ordinary edit, with a remedy text telling the author to delete their
# documentation. "0 occurrences today" was a true measurement of the wrong thing.
#
# The fix stays blunt — no parser, no position analysis, no comment or string
# handling. It is ONE lookahead over TWO characters: a `doc` token immediately followed
# by `=` or `(` is not reported. The justification is rustc's, and it was MEASURED, not
# reasoned: an earlier version of this comment claimed "Rust has no `cfg(doc = …)` form"
# and THAT IS FALSE — `#[cfg(doc = "x")]` compiles. The exclusion is still right, for two
# verified reasons: `doc(` in PREDICATE position is a SYNTAX ERROR (`#[cfg(doc(hidden))]`
# => `error[E0539]`), so there is no predicate to exempt; and `doc =` in predicate
# position is valid but ALWAYS FALSE, because `cargo doc` sets `doc` as a BARE flag, so
# an item behind `#[cfg(doc = "x")]` is absent even under `--cfg doc` — absent in BOTH
# builds is not a DIVERGENCE, and divergence is all this refusal prevents. The
# shapes above prove the blunt rule is otherwise untouched, and
# `#[cfg_attr(doc, doc = "x")]` up there — bare CONDITION, assigned ATTRIBUTE — is
# the shape that separates this fix from one that merely greps for `doc =`.
# All THREE documentation-only shapes, each falsified a narrower rule or would have:
#   * `doc = "…"`        conditional prose            (falsified the blunt rule, case 8)
#   * `doc(alias = "…")` conditional docs.rs metadata (falsified the `doc =` rule, case 15)
#   * `doc(hidden)`      conditional doc visibility   (same class; §1b is not its judge —
#                        `attrs_verdict` decides snapshot membership, cases 8 and 15)
c21n=0
for exempt_shape in \
  '#[cfg_attr(feature = "parquet", doc = "conditional prose, not a cfg predicate")]' \
  '#[cfg_attr(docsrs, doc(alias = "cfg(foo)"))]' \
  '#[cfg_attr(feature = "parquet", doc(hidden))]'
do
  c21n=$((c21n + 1))
  apply21 "$exempt_shape"
  set +e
  bash "$wt21/$GUARD_REL" >"$TMPROOT/case21.condoc.$c21n.out" 2>&1
  rc21n=$?
  set -e
  [ "$rc21n" -eq 0 ] \
    || fail_case "case 21.exempt.$c21n — \`$exempt_shape\` on a public item was REFUSED. It conditions DOCUMENTATION, not item visibility, so it cannot make the rustdoc-derived surface diverge from the shipped one; refusing it bricks the public-API gate for an ordinary edit; got: $(cat "$TMPROOT/case21.condoc.$c21n.out")"
  grep -q 'doc` cfg predicate' "$TMPROOT/case21.condoc.$c21n.out" \
    && fail_case "case 21.exempt.$c21n — the guard exited 0 but still printed the \`doc\`-cfg-predicate refusal for \`$exempt_shape\`; got: $(cat "$TMPROOT/case21.condoc.$c21n.out")"
  grep -q "public items + .* associated items" "$TMPROOT/case21.condoc.$c21n.out" \
    || fail_case "case 21.exempt.$c21n — \`$exempt_shape\` passed, but the guard printed no affirmative measurement line, so the pass cannot be distinguished from a vacuous one; got: $(cat "$TMPROOT/case21.condoc.$c21n.out")"
done
[ "$c21n" -eq 3 ] || fail_case "case 21 — only $c21n of the 3 exempt shapes ran; a case that does not run cannot fail"
cp "$TMPROOT/case21.orig.rs" "$t21"
echo "OK (21): all 9 predicate shapes REFUSE (incl. the three comment-bearing ones that passed GREEN before, and \`#[cfg_attr(doc, …)]\` whose CONDITION is bare) while all 3 documentation-only shapes — \`doc = \"…\"\`, \`doc(alias = …)\`, \`doc(hidden)\` — certify normally"

# ---------------------------------------------------------------------------
# 22. RED — a RELATIVE `CARGO_TARGET_DIR` must not make the guard inspect a
#     DIFFERENT tree than the one cargo wrote (roborev r4 F3).
#
#     THE INVARIANT: the tree the script locks, deletes, enumerates and compares must
#     be the tree cargo just wrote. The guard runs cargo from the REPO ROOT
#     (`cd "$REPO_ROOT" && cargo doc`) and cargo resolves a relative
#     `CARGO_TARGET_DIR` against ITS OWN cwd (measured), so a script that resolves the
#     same value against the CALLER's cwd is pointed somewhere else entirely.
#
#     MEASURED pre-fix, invoked from a foreign cwd with `CARGO_TARGET_DIR=probe-target`:
#     the guard created and LOCKED `<caller-cwd>/probe-target/.pub-surface-doc.lock`
#     — not the lock every other run takes, so the mutual exclusion that exists to
#     stop one run swapping the doc tree under another silently did not apply — and
#     then reported `the emitted item tree probe-target/doc/cqlite_core is ABSENT`
#     about a directory cargo was never asked to write.
#
#     The scratch's relative target dir is a SYMLINK to the suite's shared target dir,
#     so the correct resolution is also the fast one (no dependency rebuild) and the
#     case cannot pass merely because both paths were empty.
# ---------------------------------------------------------------------------
scratch_tree rel-target; wt22="$SCRATCH"
mkdir -p "$CARGO_TARGET_DIR"
abs22="$(cd "$CARGO_TARGET_DIR" && pwd)"
ln -s "$abs22" "$wt22/probe-target"
mkdir -p "$TMPROOT/case22-cwd"
set +e
( cd "$TMPROOT/case22-cwd" && CARGO_TARGET_DIR=probe-target bash "$wt22/$GUARD_REL" ) \
  >"$TMPROOT/case22.out" 2>&1
case22_rc=$?
set -e
[ "$case22_rc" -eq 0 ] || {
  echo "FAIL: case 22 — under a RELATIVE CARGO_TARGET_DIR invoked from a cwd other than the"
  echo "      repo root, the guard did not verify the tree cargo wrote. Resolve the doc dir"
  echo "      against the same base cargo uses (the repo root), or refuse fail-closed."
  cat "$TMPROOT/case22.out"
  exit 1
}
[ ! -e "$TMPROOT/case22-cwd/probe-target" ] || {
  echo "FAIL: case 22 — the guard operated on the CALLER-relative path"
  echo "      $TMPROOT/case22-cwd/probe-target, which cargo never wrote. Whatever it locked,"
  echo "      deleted and inspected there was not the tree under test."
  find "$TMPROOT/case22-cwd" -maxdepth 3 | head -10
  exit 1
}
echo "OK (22): a relative CARGO_TARGET_DIR resolves against the repo root — the guard inspects the tree cargo wrote"

# ---------------------------------------------------------------------------
# 23. RED — an INDENTED crate-root `pub mod NAME;` at BRACE DEPTH ZERO is a SHARED
#     BLIND SPOT of the two derivations, exactly like the inline form (roborev r5 F1).
#
#     Rust does not require top-level declarations to sit at column zero, but EVERY
#     crate-root scan path here skipped a line with leading whitespace. So an
#     indented `pub mod x;` whose module file carries an inner `#![cfg(feature = …)]`
#     was absent from derivation S, absent from derivation P **and** absent from
#     rustdoc — the two scans therefore AGREED, the cross-check was satisfied, and the
#     crate-root inconsistency this guard exists to catch passed GREEN.
#
#     MEASURED against the unfixed script: exit 0, i.e. a clean certification of a
#     crate root that advertises a module the compiled crate does not have.
# ---------------------------------------------------------------------------
scratch_tree indented-mod; wt23="$SCRATCH"
cat >"$wt23/cqlite-core/src/probe_indented_gated.rs" <<'RS'
#![cfg(feature = "benchmarks")]
//! Self-test-only probe (#1712 r5 F1): the gate sits in the MODULE FILE while the
//! crate root advertises the module unconditionally — just INDENTED.

/// Self-test-only probe.
pub fn probe() {}
RS
cat >>"$wt23/cqlite-core/src/lib.rs" <<'RS'

    /// Self-test-only probe (#1712 r5 F1): INDENTED, but at brace depth ZERO, so
    /// this IS a crate-root declaration however it is laid out.
    pub mod probe_indented_gated;
RS
set +e
bash "$wt23/$GUARD_REL" >"$TMPROOT/case23.out" 2>&1
case23_rc=$?
set -e
[ "$case23_rc" -ne 0 ] || fail_case "case 23 — an INDENTED crate-root \`pub mod NAME;\` whose module file hides a \`#![cfg]\` gate passed GREEN. Indentation is not depth: the declaration is at brace depth 0, so it is a crate-root declaration, and skipping it makes both derivations blind in the same place; got: $(cat "$TMPROOT/case23.out")"
grep -q "probe_indented_gated" "$TMPROOT/case23.out" \
  || fail_case "case 23 — the guard failed but never named the offending declaration; got: $(cat "$TMPROOT/case23.out")"
grep -qE "line [0-9]+" "$TMPROOT/case23.out" \
  || fail_case "case 23 — the diagnostic did not name the offending LINE, so an operator cannot act on it; got: $(cat "$TMPROOT/case23.out")"
grep -qi "indent" "$TMPROOT/case23.out" \
  || fail_case "case 23 — the diagnostic did not say the problem is the INDENTATION, so its remedy is not actionable; got: $(cat "$TMPROOT/case23.out")"
echo "OK (23): an INDENTED crate-root \`pub mod NAME;\` at brace depth 0 makes the scan REFUSE"

# ---------------------------------------------------------------------------
# 24. GREEN — an indented `pub mod` NESTED inside a module block must stay green.
#
#     This case is the other half of 23 and is exactly as important. An indented
#     `pub mod inner;` is perfectly ordinary Rust when it sits inside `mod outer { … }`
#     — it is NOT a crate-root declaration, and a blanket "refuse any indented
#     `pub mod`" would red every such crate. A refusal that fires on legitimate code
#     gets waived, and a waived guard guards nothing; the property that decides a
#     crate-root declaration is BRACE DEPTH ZERO, not column zero.
#
#     Both nested shapes are covered: the statement form (whose module lives in
#     `probe_outer_block/`) and the inline form, which case 20's refusal must also
#     keep ignoring below the crate root.
# ---------------------------------------------------------------------------
scratch_tree nested-indented-mod; wt24="$SCRATCH"
mkdir -p "$wt24/cqlite-core/src/probe_outer_block"
cat >"$wt24/cqlite-core/src/probe_outer_block/probe_inner_file.rs" <<'RS'
//! Self-test-only probe (#1712 r5 F1): a nested module, reached only through a
//! PRIVATE parent, so it changes no public surface.

/// Self-test-only probe.
pub fn probe() {}
RS
cat >>"$wt24/cqlite-core/src/lib.rs" <<'RS'

// Self-test-only probe (#1712 r5 F1): legitimate INDENTED `pub mod` declarations,
// nested inside a private module block and therefore at brace depth 1.
mod probe_outer_block {
    pub mod probe_inner_file;
    pub mod probe_inner_inline {
        /// Self-test-only probe.
        pub fn probe() {}
    }
}
RS
set +e
bash "$wt24/$GUARD_REL" >"$TMPROOT/case24.out" 2>&1
case24_rc=$?
set -e
[ "$case24_rc" -eq 0 ] || {
  echo "FAIL: case 24 — an indented \`pub mod\` NESTED inside \`mod outer { … }\` (brace depth 1)"
  echo "      was rejected. That is ordinary Rust and not a crate-root declaration; the"
  echo "      refusal must key on brace depth, not on leading whitespace, or it reds"
  echo "      correct code and gets waived."
  cat "$TMPROOT/case24.out"
  exit 1
}
echo "OK (24): an indented \`pub mod\` nested inside a module block (brace depth 1) stays GREEN"

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

# ---------------------------------------------------------------------------
# 25. RED — a UNICODE-NAMED associated item added to a page that ALREADY carries
#     ASCII members must be NAMED in the diff (roborev round 5, finding 2).
#
#     Rust identifiers are Unicode: `pub fn café(&self)` is ordinary public API. The
#     anchor scan used to read the name as `[A-Za-z0-9_]+` with a required closing
#     quote, so such an anchor matched NOTHING and the item was SILENTLY DROPPED —
#     and neither backstop could see it. The per-section emptiness check is satisfied
#     by the page's ASCII siblings (the section is non-empty, just incomplete), and
#     `all.html` lists item PAGES only, never associated items. Measured on the
#     unfixed guard: appending `pub fn café_probe` + `pub const PROBE_CONSTÉ` to
#     `ResolvedVersion` produced `RESULT: exit 0`, "1128 public items + 3785
#     associated items … match cqlite-core/pub-surface.snapshot" — a real public-API
#     addition certified green. That is the sixth instance in #1712 of one class: a
#     lexical character class standing in for a structural read.
#
#     WHAT RUSTDOC ACTUALLY EMITS, measured rather than assumed (a scratch crate with
#     `café`, `日本語`, `CONSTÉ`, `Struct変 { fieldé }`, `méthode`, `Assocé`):
#       * identifier bytes appear RAW UTF-8 in anchors — `id="method.café"`,
#         `id="variant.Struct変.field.fieldé"` — and are NEVER percent-encoded.
#         Percent-encoding appears only in `impl-Borrow%3CT%3E-for-T`-shaped anchors,
#         i.e. `<`/`>`/`'` inside impl headers, which live in EXCLUDED sections.
#       * rustc normalises a non-NFC source identifier to NFC, and rustdoc emits that
#         NFC form in BOTH the anchor and the page filename (`struct.Café.html`,
#         `href="struct.Café.html"` in all.html). So there is exactly ONE spelling and
#         the item page and the all.html cross-check cannot disagree about it.
#     Hence no decoding, re-encoding or normalisation anywhere in this guard — which
#     is the point: the anchor's delimited value is taken verbatim.
#
#     The case pins the OTHER direction in the SAME scratch, at no extra `cargo doc`:
#     the diff must contain NOTHING BUT the two added lines. Widening the anchor read
#     must not re-spell, drop or duplicate any pre-existing ASCII item.
# ---------------------------------------------------------------------------
scratch_tree unicode-assoc; wt25="$SCRATCH"
cat >>"$wt25/cqlite-core/src/version_hints.rs" <<'RS'

impl ResolvedVersion {
    /// Self-test-only probe with a NON-ASCII name
    /// (scripts/tests/test_pub_surface_guard.sh, issue #1712 r5 F2). Exists solely
    /// inside a throwaway scratch worktree. Never committed.
    pub fn café_probe(&self) -> bool {
        true
    }

    /// Self-test-only probe associated const with a NON-ASCII name (#1712 r5 F2).
    pub const PROBE_CONSTÉ: u8 = 1;
}
RS
grep -qF 'café_probe' "$wt25/cqlite-core/src/version_hints.rs" \
  || fail_case "case 25 setup: the Unicode probe items were not written to the scratch source"
set +e
bash "$wt25/$GUARD_REL" >"$TMPROOT/case25.out" 2>&1
case25_rc=$?
set -e
[ "$case25_rc" -ne 0 ] \
  || fail_case "case 25 — a UNICODE-named associated item did NOT trip the snapshot diff. The anchor scan dropped it silently, which is the #1712 r5 F2 FALSE PASS: a real public-API addition with a green guard. Got: $(cat "$TMPROOT/case25.out")"
grep -qF -- "+method cqlite_core::version_hints::ResolvedVersion::café_probe" "$TMPROOT/case25.out" \
  || fail_case "case 25 — a \`pub fn\` with a NON-ASCII name was not recorded as a \`method\` line at its real path; got: $(grep -F 'café' "$TMPROOT/case25.out" || echo '(no line mentions it at all)')"
grep -qF -- "+associatedconstant cqlite_core::version_hints::ResolvedVersion::PROBE_CONSTÉ" "$TMPROOT/case25.out" \
  || fail_case "case 25 — a \`pub const\` with a NON-ASCII name was not recorded as an \`associatedconstant\` line at its real path; got: $(grep -F 'PROBE_CONST' "$TMPROOT/case25.out" || echo '(no line mentions it at all)')"
# No spurious drift: the ONLY changed snapshot lines are the two additions.
awk '/^\+\+\+/ || /^---/ { next } /^[+-]/ { print }' "$TMPROOT/case25.out" >"$TMPROOT/case25.changed"
if [ "$(wc -l <"$TMPROOT/case25.changed" | tr -d ' ')" -ne 2 ] || grep -q '^-' "$TMPROOT/case25.changed"; then
  echo "FAIL: case 25 — widening the anchor read re-spelled, dropped or duplicated"
  echo "      pre-existing ASCII surface. Only the two added lines may change:"
  cat "$TMPROOT/case25.changed"
  exit 1
fi
echo "OK (25): a Unicode-named associated item is named in the diff, and no ASCII item is re-spelled"

# ---------------------------------------------------------------------------
# 26. RED — the GATE COMPONENT must not report PASS when the guard measured
#     NOTHING (roborev round 6, finding 2). A VACUOUS PASS.
#
#     `agent-gate.sh`'s pub-surface component used to assign PASS on the guard's
#     exit status alone and then echo its measurement line with `grep … || true`,
#     so an accidental early zero-exit inside the guard — a `return 0` on a path
#     that never enumerated anything — produced a component that reports PASS
#     while nothing was measured. Measured on the unfixed gate, with a stub guard
#     that exits 0 after printing one unrelated line:
#         >>> [pub-surface] bash scripts/ci/check-pub-surface.sh
#         >>> [pub-surface] PASS (0s)
#         pub-surface:       PASS (0s)
#     That is CLAUDE.md's named rule violated in the one component that certifies
#     the guard that certifies the public API: never derive a pass from the ABSENCE
#     of a bad signal — a positive verdict requires an AFFIRMATIVE MEASUREMENT.
#
#     NO TEST-ONLY SEAM IN agent-gate.sh. The component's guard path is hard-coded
#     on purpose, so this case SUBSTITUTES THE ARTIFACT: it replaces
#     scripts/ci/check-pub-surface.sh inside a throwaway scratch worktree and runs
#     THAT tree's gate. A seam is one more thing a real invoker can set.
#
#     `--only pub-surface` self-exempts from the #1825 gate slot, so this nested
#     invocation cannot deadlock against an enclosing gate; the stub guard means no
#     cargo runs at all, so the whole case is seconds.
#
#     The POSITIVE CONTROL is in the same scratch, at no extra cost: a stub that DOES
#     print the measurement line must still reach PASS. Without it the fix could be
#     hardwired to FAIL and this case would not notice.
# ---------------------------------------------------------------------------
scratch_tree gate-vacuous-pass; wt26="$SCRATCH"
# Deliberately does NOT touch `set -e`: re-enabling it inside a function that then
# returns non-zero exits the SUITE at the call site (measured — the case aborted with
# no output at all). Each caller wraps the call in `set +e` / `set -e`, as every other
# case in this suite does.
gate26() { # <summary-file> <log>
  ( cd "$wt26" && env AGENT_GATE_SUMMARY_FILE="$1" \
      bash "$wt26/scripts/agent-gate.sh" --only pub-surface >"$2" 2>&1 )
}

# (a) THE RED: a guard that exits 0 having measured nothing.
cat >"$wt26/$GUARD_REL" <<'STUB'
#!/usr/bin/env bash
# Self-test stub (#1712 r6 F2): stands in for an ACCIDENTAL EARLY ZERO-EXIT — the
# guard returns success on a path that never enumerated a public surface, so it
# emits no `pub-surface: …` measurement line. Exists only in a scratch worktree.
echo "check-pub-surface: returned early without enumerating anything"
exit 0
STUB
set +e
gate26 "$TMPROOT/case26a-summary.txt" "$TMPROOT/case26a.out"
case26a_rc=$?
set -e
# A BARE non-zero rc assert would be non-discriminating here: a SUCCESSFUL `--only`
# run exits 3 (PARTIAL, "does NOT count as the gate"), so `-ne 0` is satisfied by the
# vacuous pass too. The verdict lives in the SUMMARY block and in the FAIL exit (1).
grep -qE '^pub-surface: +FAIL' "$TMPROOT/case26a-summary.txt" || {
  echo "FAIL: case 26 — the pub-surface GATE COMPONENT did not record FAIL for a guard that"
  echo "      exited 0 without emitting its affirmative measurement line. That is the"
  echo "      #1712 r6 F2 VACUOUS PASS: the component certifying the public-API guard"
  echo "      passing while nothing was measured."
  grep -E 'pub-surface|RESULT:' "$TMPROOT/case26a-summary.txt" || echo '(no pub-surface line at all)'
  exit 1
}
grep -q '^RESULT: FAIL' "$TMPROOT/case26a-summary.txt" \
  || fail_case "case 26 — the component recorded FAIL but the run's RESULT did not become FAIL; got: $(grep -E 'RESULT:' "$TMPROOT/case26a-summary.txt" || echo '(no RESULT line)')"
[ "$case26a_rc" -eq 1 ] \
  || fail_case "case 26 — expected the FAIL exit status 1, got $case26a_rc (3 is a SUCCESSFUL --only run, i.e. the vacuous pass)"
grep -qF 'affirmative measurement' "$TMPROOT/case26a.out" \
  || fail_case "case 26 — the component failed but NOT with a NAMED diagnostic saying the affirmative measurement line was missing (a bare unexplained FAIL gets waived); got: $(cat "$TMPROOT/case26a.out")"

# (b) THE POSITIVE CONTROL: the measurement line present ⇒ PASS is still reachable.
cat >"$wt26/$GUARD_REL" <<'STUB'
#!/usr/bin/env bash
# Self-test stub (#1712 r6 F2) — the POSITIVE CONTROL: emits the guard's real
# affirmative measurement line, so the component must still reach PASS.
echo "pub-surface: 3 public items + 0 associated items + 0 re-exports + 0 globs over 1 public modules match cqlite-core/pub-surface.snapshot (3 item pages, cross-checked against rustdoc all.html); 1 crate-root declarations consistent"
exit 0
STUB
set +e
gate26 "$TMPROOT/case26b-summary.txt" "$TMPROOT/case26b.out"
case26b_rc=$?
set -e
grep -qE '^pub-surface: +PASS' "$TMPROOT/case26b-summary.txt" \
  || fail_case "case 26 control — a guard that DID emit its measurement line was not recorded PASS, so the requirement is hardwired to FAIL and case 26(a) proves nothing; got: $(grep -E 'pub-surface|RESULT:' "$TMPROOT/case26b-summary.txt" || echo '(no pub-surface line at all)')"
[ "$case26b_rc" -eq 3 ] \
  || fail_case "case 26 control — expected the successful --only exit status 3 (PARTIAL), got $case26b_rc"
grep -qF 'public items + ' "$TMPROOT/case26b.out" \
  || fail_case "case 26 control — the component passed but did not echo the measurement line, so a pasted gate log would not show the check RAN"

# (c) THE PREFIX RED (roborev r7 finding 3). The first version of the requirement
#     matched any line STARTING `pub-surface: `, which is the same vacuous-pass shape
#     one level down: a guard that prints a progress line and exits 0 satisfied it.
#     A check against a PREFIX tests a SPELLING; the requirement must test the STATE.
cat >"$wt26/$GUARD_REL" <<'STUB'
#!/usr/bin/env bash
# Self-test stub (#1712 r7 F3) — prints a line that BEGINS `pub-surface: ` but carries
# no measurement at all, then exits 0. The component must NOT read this as a pass.
echo "pub-surface: starting"
exit 0
STUB
set +e
gate26 "$TMPROOT/case26c-summary.txt" "$TMPROOT/case26c.out"
case26c_rc=$?
set -e
grep -qE '^pub-surface: +FAIL' "$TMPROOT/case26c-summary.txt" \
  || fail_case "case 26(c) — a guard printing only \`pub-surface: starting\` and exiting 0 was NOT recorded FAIL. The requirement is matching a PREFIX, so it is satisfiable without measuring anything; got: $(grep -E 'pub-surface|RESULT:' "$TMPROOT/case26c-summary.txt" || echo '(no pub-surface line at all)')"
[ "$case26c_rc" -eq 1 ] \
  || fail_case "case 26(c) — expected the FAIL exit status 1 for a prefix-only measurement line, got $case26c_rc"
grep -qE 'affirmative measurement' "$TMPROOT/case26c.out" \
  || fail_case "case 26(c) — the component failed but not with the NAMED missing-measurement diagnostic; got: $(cat "$TMPROOT/case26c.out")"
echo "OK (26): the pub-surface GATE COMPONENT requires the guard's affirmative measurement line before PASS — a line that merely BEGINS \`pub-surface: \` does not satisfy it"

# ---------------------------------------------------------------------------
# 27. GREEN — the ORDINARY PRE-COMMIT WORKFLOW *WITH STAGED CHANGES* must pass
#     (roborev round 6, finding 3). Case 17's sibling, and another FALSE FAIL.
#
#     `git add` your work, then run the tests: that is a completely normal
#     workflow, and it used to abort the suite BEFORE it tested anything. The
#     overlay reproduced the source's delta with `git diff HEAD` (which captures
#     STAGED content) and replayed it with a plain `git apply` (which recreates it
#     UNSTAGED), while the proof step compares `git status --porcelain` EXACTLY —
#     and porcelain distinguishes `M ` (staged) from ` M` (unstaged). So every
#     scratch case failed with "the scratch worktree does not reproduce the working
#     tree" the moment anything was staged.
#
#     Pinned here in the shape that actually occurs: the API change and its
#     regenerated snapshot are STAGED (`M `), and the source file carries a further
#     UNSTAGED edit on top (`MM`). The child scratch must reproduce BOTH index
#     states and the guard must verify clean inside it.
# ---------------------------------------------------------------------------
scratch_tree staged-worktree; wt27="$SCRATCH"
cat >>"$wt27/cqlite-core/src/version_hints.rs" <<'RS'

/// Self-test-only probe item (scripts/tests/test_pub_surface_guard.sh, issue #1712
/// r6 F3), standing in for a STAGED public-API change. Never committed.
pub struct PubSurfaceStagedProbe;
RS
set +e
bash "$wt27/$GUARD_REL" --regenerate >"$TMPROOT/case27-regen.out" 2>&1
case27_regen_rc=$?
set -e
[ "$case27_regen_rc" -eq 0 ] \
  || fail_case "case 27 setup: --regenerate failed in the outer scratch; got: $(cat "$TMPROOT/case27-regen.out")"
git -C "$wt27" add -- cqlite-core/src/version_hints.rs "$SNAPSHOT_REL" \
  || fail_case "case 27 setup: could not stage the API change and its regenerated snapshot"
# …and one further UNSTAGED edit on top of a staged file, so the case covers `MM`
# too. A comment cannot change the public surface, so the guard must stay green.
cat >>"$wt27/cqlite-core/src/version_hints.rs" <<'RS'
// Self-test-only UNSTAGED trailing comment (#1712 r6 F3). Not public API.
RS
# Prove the SETUP is genuinely what the case is about — a staged snapshot (`M `) and a
# staged-plus-modified source file (`MM`). Without this the case could pass by never
# having staged anything.
git -C "$wt27" status --porcelain | grep -qE "^M  $SNAPSHOT_REL\$" \
  || fail_case "case 27 setup: the regenerated snapshot is not STAGED (expected porcelain \`M \`); got: $(git -C "$wt27" status --porcelain)"
git -C "$wt27" status --porcelain | grep -qE '^MM cqlite-core/src/version_hints\.rs$' \
  || fail_case "case 27 setup: the source file is not staged-plus-further-modified (expected porcelain \`MM\`); got: $(git -C "$wt27" status --porcelain)"

scratch_tree_from "$wt27" staged-worktree-child; wt27c="$SCRATCH"
grep -q 'PubSurfaceStagedProbe' "$wt27c/cqlite-core/src/version_hints.rs" \
  || fail_case "case 27 — the child scratch did not receive the outer scratch's STAGED source change"
grep -q 'PubSurfaceStagedProbe' "$wt27c/$SNAPSHOT_REL" \
  || fail_case "case 27 — the child scratch did not receive the outer scratch's STAGED regenerated snapshot"
grep -q 'UNSTAGED trailing comment' "$wt27c/cqlite-core/src/version_hints.rs" \
  || fail_case "case 27 — the child scratch did not receive the outer scratch's UNSTAGED edit"
# The index state itself must be reproduced, not just the file contents: that is what
# keeps the overlay's proof step an EXACT porcelain comparison rather than a weakened one.
git -C "$wt27c" status --porcelain | grep -qE "^M  $SNAPSHOT_REL\$" \
  || fail_case "case 27 — the child scratch did not reproduce the STAGED index state of the snapshot; got: $(git -C "$wt27c" status --porcelain)"
git -C "$wt27c" status --porcelain | grep -qE '^MM cqlite-core/src/version_hints\.rs$' \
  || fail_case "case 27 — the child scratch did not reproduce the staged-plus-modified (\`MM\`) index state; got: $(git -C "$wt27c" status --porcelain)"
set +e
bash "$wt27c/$GUARD_REL" >"$TMPROOT/case27.out" 2>&1
case27_rc=$?
set -e
[ "$case27_rc" -eq 0 ] || {
  echo "FAIL: case 27 — a scratch built from a tree whose public-API change and regenerated"
  echo "      snapshot are STAGED did not verify clean. Staging your work before running the"
  echo "      tests is an ordinary workflow; failing it is a false FAIL in the path every"
  echo "      contributor walks."
  cat "$TMPROOT/case27.out"
  exit 1
}
echo "OK (27): a scratch built from a tree with STAGED changes (plus an unstaged edit on top) verifies clean"

echo ""
echo "PASS: test_pub_surface_guard.sh — all 27 cases (8 green, 17 reds, 1 usage, 1 kill-safety)"
