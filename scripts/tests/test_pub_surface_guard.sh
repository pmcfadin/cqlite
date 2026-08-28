#!/usr/bin/env bash
# test_pub_surface_guard.sh — self-test for the cqlite-core crate-root
# declaration-consistency guard, scripts/ci/check-pub-surface.sh (issue #1712,
# epic #1688).
#
# The point of this suite is to prove the guard FIRES, not merely that it exists. A
# guard nobody has watched fail is indistinguishable from a guard that always passes,
# and this repo has paid for that lesson repeatedly (CLAUDE.md: "a positive verdict
# requires an affirmative measurement"). So every negative case asserts not just a
# non-zero exit but a distinctive substring of the INTENDED diagnostic — a bare
# exit-code assertion passes on an unrelated silent abort.
#
# WHAT THE GUARD IS, AFTER THE #1712 DESCOPE. It answers ONE question: does the crate
# root tell the truth about the modules it declares? Two halves, each a bounded read
# of source:
#
#   THE INHERITED HALF — the crate-root scan of cqlite-core/src/lib.rs, which reads
#   each top-level declaration's attributes STRUCTURALLY (`attrs_verdict`) and
#   refuses over shapes it does not handle. This half is UNCHANGED by the descope and
#   is pinned by cases 2, 8, 9, 10, 11, 15, 16, 20, 23, 24 (and case 28, which closes
#   the r7 F2 residual in it).
#
#   THE NEW HALF — the MODULE-FILE ORACLE. For each declaration the scan calls OPEN,
#   the module's own file is resolved (NAME.rs xor NAME/mod.rs) and its PROLOGUE read,
#   asking whether an inner `#![...]` attribute mentions `cfg`. This half replaced a
#   rustdoc-derived item list, so it is where the risk is, and every one of its
#   REFUSAL paths is pinned: cases 29-36.
#
# WHAT IS GONE, so nobody looks for its coverage: the rustdoc-derived public-API
# snapshot, `cqlite-core/pub-surface.snapshot`, `--regenerate`, the `all.html`
# cross-check, associated-item enumeration, the re-export/glob rendering and the §1b
# `doc`-cfg fence were all REMOVED (#1712 lead ruling). Five review findings across
# rounds 4-7 were one defect class — a scanner that had to find declarations anywhere
# in arbitrary source, an unbounded parsing problem that cannot abstain. The cases
# that tested that machinery (old 3, 4, 6, 7, 12, 13, 14, 17, 18, 21, 22, 25, 27) were
# deleted with it. Public-API DRIFT DETECTION IS NOT A PROPERTY OF THIS GUARD any
# more; issue #3366 is the principled route to it.
#
# Cases:
#   1. GREEN  — the real tree verifies clean and prints its affirmative measurement.
#   2. RED    — the CONSISTENCY ASSERT. A scratch checkout is reverted to the
#               pre-#1712 shape (bare ungated `pub mod benchmarks;` at the crate root
#               + the inner `#![cfg(feature = "benchmarks")]` back in
#               benchmarks/mod.rs) and the guard must FAIL naming `benchmarks`. This
#               is the pre-change-main red, PINNED so it can never silently stop
#               being a red.
#   5. USAGE  — an unrecognized argument exits 2 (repo convention).
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
#                and was never checked against its module file: a false PASS.
#  10.  GREEN  — a multi-line attribute must join onto its declaration, a trailing
#                `// comment` must not leak into the module name, and a `pub mod`
#                inside a `/* */` block must stay a phantom. All three used to be
#                false FAILs.
#  11.  RED    — when the two independent crate-root derivations disagree the guard
#                must REFUSE, not quietly use the smaller set.
#
#   ATTRIBUTE reading at the declaration site (roborev round 2):
#  15.  RED    — a tell-tale token (`doc(hidden)`, `cfg(`) inside an attribute's
#                STRING VALUE must not exempt a declaration from the assert.
#  16.  GREEN  — a real `#[cfg]` separated from its item by blank/comment lines must
#                still gate it.
#
#  19.  KILL   — a killed run must not leave registered git worktrees behind (measured:
#                a 2-minute tool timeout left 11, and `git worktree prune` could not
#                reclaim them). Pins the trap list structurally, that cleanup reclaims
#                such a worktree BY EXPLICIT PATH, and — the regression guard — that a
#                fresh run leaves a CONCURRENT run's scratch worktrees alone.
#
#   SHARED blind spots of the two derivations — a disagreement they cannot express:
#  20.  RED    — an INLINE crate-root `pub mod NAME { #![cfg(…)] … }` is invisible to
#                BOTH derivations, so they AGREE while both are blind and the gate
#                hiding inside the body passes green. The scan must REFUSE over any
#                top-level `pub mod` form it does not recognise.
#  23.  RED    — an INDENTED `pub mod NAME;` at brace depth ZERO, same shape.
#  24.  GREEN  — an indented `pub mod` NESTED inside a module block (depth 1) is
#                ordinary Rust and must stay green, or the refusal reds correct code.
#  28.  RED    — `*/ pub mod NAME;` — CODE AFTER A CLOSING DELIMITER on one line
#                (roborev r7 finding 2). `INCODE` records only LINE-START comment
#                state, so this declaration is invisible to BOTH derivations too. Its
#                positive control is in the same case: the identical tree with the
#                declaration on its own line must be GREEN.
#
#   The COMPONENT that certifies the guard (roborev r6 F2) — also a FALSE PASS:
#  26.  RED    — `agent-gate.sh`'s pub-surface component assigned PASS on the guard's
#                EXIT STATUS alone, echoing the measurement line with `|| true`. So a
#                guard that exited 0 having enumerated nothing reported
#                `pub-surface: PASS` — a pass derived from the absence of a bad signal,
#                in the one component that certifies the public-API guard. The
#                component must require the affirmative measurement line and otherwise
#                record a NAMED failure. Carries its own positive control, plus the
#                r7 F3 prefix red.
#
#   THE NEW HALF — every REFUSAL path of the module-file oracle (#1712 descope). Each
#   one exists because the guard must REFUSE rather than guess: a skipped declaration
#   is a silent false PASS in the one assert this guard is. Measured against the same
#   guard with the refusal branch removed, each of these inputs exits 0 — the numbers
#   are recorded on the individual cases.
#  29.  RED    — the module file resolves to NEITHER of its two legal paths.
#  30.  RED    — the module file resolves to BOTH of them.
#  31.  RED    — the module file exists but is not a readable regular file.
#  32.  RED    — a BLOCK COMMENT opens in the prologue. Deliberately NOT modelled:
#                `/* #![cfg(feature = "x")] */` is a delimiter inside a comment, the
#                same class as the five deleted findings, and the lead's ruling is to
#                prefer the refusal because it is bounded and cannot rot.
#  33.  RED    — an inner attribute that MENTIONS a `cfg` token without being named
#                `cfg` (`#![cfg_attr(...)]`) is refused, not exempted.
#  34.  RED    — content follows an inner attribute on the SAME line. Without this,
#                `#![doc = "]"] #![cfg(x)]` hides a real gate — a false PASS.
#  35.  RED    — an inner attribute whose `[` never closes.
#  36.  GREEN  — the positive control for 32-35: a prologue of `//!` comments, blank
#                lines and INERT inner attributes (`#![allow(...)]`, `#![doc = "…"]`)
#                must certify normally. Without it, 32-35 would be satisfied by a
#                guard hardwired to refuse every prologue.
#
# NO TEST-ONLY SEAM. The guard's subject is hard-coded on purpose, so the negative
# cases SUBSTITUTE THE ARTIFACT: each runs in its own `git worktree add --detach HEAD`
# scratch checkout whose files are edited in place (CLAUDE.md — a test that needs a
# different subject substitutes the artifact, never a path variable; a path variable
# is one more thing a real invoker can set).
#
# Cost: the guard is SOURCE-ONLY since the #1712 descope — no `cargo doc`, no cargo at
# all — so every case is a sub-second awk/bash run over a scratch worktree. Only case
# 26, which drives a nested `agent-gate.sh --only pub-surface`, takes seconds.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GUARD_REL="scripts/ci/check-pub-surface.sh"
GUARD="$REPO_ROOT/$GUARD_REL"

[ -f "$GUARD" ] || { echo "FAIL: guard script not found at $GUARD"; exit 1; }

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

# The guard's affirmative measurement line, as a REGEX matched WHOLE. Kept in sync BY
# HAND with the guard's own success line and with `agent-gate.sh`'s `pub-surface`
# component — a wording change must land in all three at once (#1712 descope).
MEASURED_RE='^pub-surface: [0-9]+ crate-root declarations scanned in cqlite-core/src/lib\.rs \([0-9]+ pub mod, of which [1-9][0-9]* unconditional\); [1-9][0-9]* module-file prologues read from source; 0 inconsistent$'
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
  echo "FAIL: the guard FAILED on the REAL tree — a crate-root \`pub mod\` disagrees with"
  echo "      its own module file, or the guard refused over input it cannot classify."
  cat "$TMPROOT/green.out"
  exit 1
fi
grep -qE "$MEASURED_RE" "$TMPROOT/green.out" \
  || fail_case "the guard passed but printed no affirmative measurement line matching the WHOLE success shape; got: $(cat "$TMPROOT/green.out")"
echo "OK (1): real tree verifies clean — $(cat "$TMPROOT/green.out")"
# ---------------------------------------------------------------------------
# 2. RED — the consistency assert, against the pre-#1712 source shape.
# ---------------------------------------------------------------------------
scratch_tree pre-1712; wt2="$SCRATCH"
# Restore the bare, ungated crate-root declaration: drop a
# `#[cfg(feature = "benchmarks")]` line that sits immediately above
# `pub mod benchmarks;`. Pure awk — no perl/GNU-sed dependency.
awk '
  /^#\[cfg\(feature = "benchmarks"\)\]/ { held = $0; next }
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
# Matched WITHOUT a `$` anchor and WITHOUT `-x`: the real declaration carries a
# trailing `// #1712: gate HERE …` comment, and an anchored pattern could not see it —
# so this revert silently no-opped and the case certified the very shape it exists to
# red. (#1712 descope: found by the suite failing once the guard's other half stopped
# masking it.)
if grep -B1 -x 'pub mod benchmarks;' "$wt2/cqlite-core/src/lib.rs" | grep -q 'cfg(feature = "benchmarks")'; then
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
grep -q "INCONSISTENT with the module's own file" "$TMPROOT/case2.out" \
  || fail_case "case 2 — the guard failed for some OTHER reason than the consistency assert; got: $(cat "$TMPROOT/case2.out")"
echo "OK (2): the consistency assert FAILS on the pre-#1712 shape and names \`benchmarks\`"

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
  /^#\[cfg\(feature = "benchmarks"\)\]/ { held = 1; next }
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
grep -q "INCONSISTENT with the module's own file" "$TMPROOT/case8.out" \
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
  /^#\[cfg\(feature = "benchmarks"\)\]/ { held = 1; next }
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
grep -q "INCONSISTENT with the module's own file" "$TMPROOT/case9.out" \
  || fail_case "case 9 — the consistency assert did NOT fire on a same-line declaration, so the declaration was dropped by the crate-root scan (the false PASS); got: $(cat "$TMPROOT/case9.out")"
grep -q "pub mod benchmarks" "$TMPROOT/case9.out" \
  || fail_case "case 9 — the guard failed but never named \`benchmarks\`; got: $(cat "$TMPROOT/case9.out")"
echo "OK (9): a same-line \`#[attr] pub mod x;\` is seen by the crate-root scan and asserted"

# ---------------------------------------------------------------------------
# 10. GREEN — the three FALSE-FAIL shapes (lead review, defects 2-4).
#
#     Multi-line attributes, a trailing `// comment`, and a `pub mod` inside a
#     `/* */` block. Each used to produce a spurious INCONSISTENT.
#
#     Since the #1712 descope this case is a STRICTER pin than it was, and for free:
#     the guard now resolves a module FILE for every declaration it reads as
#     unconditional. So if the multi-line attribute failed to join, `probe_multiline`
#     would read OPEN, no `probe_multiline.rs` exists, and the run would REFUSE — the
#     shape is pinned by the guard exiting 0 at all, not merely by rendered text.
#     `probe_trailing` is the mirror image: it IS unconditional, its file DOES exist,
#     so a trailing comment leaking into the module name would make the guard look
#     for `probe_trailing;.rs` and refuse.
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
case10_rc=$?
set -e
[ "$case10_rc" -eq 0 ] || {
  echo "FAIL: case 10 — a correctly-written crate root was REJECTED (false FAIL). Either a"
  echo "      MULTI-LINE attribute did not join onto its declaration (so a gated module read"
  echo "      as unconditional and its absent file refused), a trailing // comment leaked"
  echo "      into the recorded module name, or a \`pub mod\` inside a /* */ block was taken"
  echo "      for a real declaration."
  cat "$TMPROOT/case10.out"
  exit 1
}
grep -q "probe_phantom" "$TMPROOT/case10.out" \
  && fail_case "case 10 — a \`pub mod\` inside a /* */ block was named by the guard, so it was recorded as a real declaration (phantom); got: $(cat "$TMPROOT/case10.out")"
grep -qE "$MEASURED_RE" "$TMPROOT/case10.out" \
  || fail_case "case 10 — the guard exited 0 without its affirmative measurement line; got: $(cat "$TMPROOT/case10.out")"
# The declaration counts must have MOVED, or the three shapes were not read at all.
base_open="$(sed -E 's/.*of which ([0-9]+) unconditional.*/\1/' "$TMPROOT/green.out")"
case10_open="$(sed -E 's/.*of which ([0-9]+) unconditional.*/\1/' "$TMPROOT/case10.out")"
[ "$case10_open" -eq "$((base_open + 1))" ] \
  || fail_case "case 10 — adding one unconditional (\`probe_trailing\`) and one gated (\`probe_multiline\`) declaration moved the unconditional count from $base_open to $case10_open, expected $((base_open + 1)). The shapes were not read as intended."
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
  /^#\[cfg\(feature = "benchmarks"\)\]/ {
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
grep -q "INCONSISTENT with the module's own file" "$TMPROOT/case15.out" \
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
  /^#\[cfg\(feature = "benchmarks"\)\]/ {
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
# affirmative measurement line (kept in sync BY HAND with the guard and with the
# component regex in agent-gate.sh), so the component must still reach PASS.
echo "pub-surface: 26 crate-root declarations scanned in cqlite-core/src/lib.rs (20 pub mod, of which 14 unconditional); 14 module-file prologues read from source; 0 inconsistent"
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
grep -qF 'module-file prologues read from source' "$TMPROOT/case26b.out" \
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
# 28. RED — CODE AFTER A CLOSING DELIMITER on the same line (roborev r7 finding 2),
#     with its own POSITIVE CONTROL.
#
#     `INCODE[i]` records the comment/string state only at the START of line i, so
#     `*/ pub mod benchmarks;` is skipped by BOTH derivations (each bails on
#     `!INCODE`). They therefore AGREE while both are blind — a shared blind spot the
#     mutual cross-check can never see, exactly like the inline and indented forms —
#     and an inner-gated module declared that way passed GREEN.
#
#     MEASURED with Refusal X removed from the guard: exit 0, i.e. a clean
#     certification of a crate root advertising a module whose own file gates it.
#
#     (b) is the control that makes (a) mean something: the SAME tree with the
#     declaration on its own line must be GREEN, so the red in (a) is caused by the
#     mixed line and not by the scratch setup.
# ---------------------------------------------------------------------------
mixed_line_tree() { # <label> <declaration-line>  -> $SCRATCH
  scratch_tree "$1"
  awk '
    /^#\[cfg\(feature = "benchmarks"\)\]/ { next }
    $0 == "pub mod benchmarks;" { next }
    { print }
  ' "$SCRATCH/cqlite-core/src/lib.rs" >"$SCRATCH/lib.rs.stripped"
  mv "$SCRATCH/lib.rs.stripped" "$SCRATCH/cqlite-core/src/lib.rs"
  grep -q 'pub mod benchmarks' "$SCRATCH/cqlite-core/src/lib.rs" \
    && fail_case "case 28 setup ($1): the original benchmarks declaration survived the strip"
  printf '%s\n%s\n%s\n' '/* a block comment that closes on the SAME line as the declaration' '   (issue #1712 roborev r7 F2)' "$2" >>"$SCRATCH/cqlite-core/src/lib.rs"
  # …and the gate hides inside the module file, which is the whole point.
  printf '%s\n%s\n' '#![cfg(feature = "benchmarks")]' "$(cat "$SCRATCH/cqlite-core/src/benchmarks/mod.rs")" \
    >"$SCRATCH/cqlite-core/src/benchmarks/mod.rs.new"
  mv "$SCRATCH/cqlite-core/src/benchmarks/mod.rs.new" "$SCRATCH/cqlite-core/src/benchmarks/mod.rs"
}

# (a) THE RED: the declaration shares its line with the closing `*/`.
mixed_line_tree mixed-line-decl '*/ pub mod benchmarks;'; wt28a="$SCRATCH"
set +e
bash "$wt28a/$GUARD_REL" >"$TMPROOT/case28a.out" 2>&1
case28a_rc=$?
set -e
[ "$case28a_rc" -ne 0 ] || fail_case "case 28 — \`*/ pub mod benchmarks;\` on ONE line passed GREEN while benchmarks/mod.rs hides an inner \`#![cfg]\`. Both derivations skip a line that does not START in code, so they AGREE while neither saw the declaration; got: $(cat "$TMPROOT/case28a.out")"
grep -q "code follows a closing block-comment/string delimiter" "$TMPROOT/case28a.out" \
  || fail_case "case 28 — the guard failed but NOT with the mixed-line refusal, so it failed for some other reason and the blind spot is unproven; got: $(cat "$TMPROOT/case28a.out")"
grep -qE "line [0-9]+" "$TMPROOT/case28a.out" \
  || fail_case "case 28 — the refusal did not name the offending LINE, so an operator cannot act on it; got: $(cat "$TMPROOT/case28a.out")"

# (b) THE POSITIVE CONTROL: same tree, declaration on its own line -> GREEN.
#     `benchmarks` keeps its inner gate here, so the declaration must be GATED at the
#     site for this to be green; that is what the real branch does.
mixed_line_tree own-line-decl '*/'; wt28b="$SCRATCH"
printf '%s\n%s\n' '#[cfg(feature = "benchmarks")]' 'pub mod benchmarks;' >>"$wt28b/cqlite-core/src/lib.rs"
set +e
bash "$wt28b/$GUARD_REL" >"$TMPROOT/case28b.out" 2>&1
case28b_rc=$?
set -e
[ "$case28b_rc" -eq 0 ] || {
  echo "FAIL: case 28 control — the same tree with the declaration on its OWN line was"
  echo "      REJECTED, so case 28(a)'s red is not attributable to the mixed line and the"
  echo "      refusal may simply be firing on the scratch setup."
  cat "$TMPROOT/case28b.out"
  exit 1
}
echo "OK (28): code after a closing \`*/\` on one line makes the scan REFUSE, and the same tree with the declaration on its own line stays GREEN"

# ---------------------------------------------------------------------------
# THE MODULE-FILE ORACLE — the NEW half (#1712 descope). Cases 29-36.
#
# Shared setup helper: replace the crate-root `benchmarks` declaration with an
# UNCONDITIONAL `pub mod probe_oracle;`, so every case below drives the OPEN path of
# the oracle over a module the case controls completely. `benchmarks` itself keeps its
# real declaration-site gate, so it stays exempt and cannot confuse the verdict.
# ---------------------------------------------------------------------------
oracle_tree() { # <label>  -> $SCRATCH, with `pub mod probe_oracle;` declared
  scratch_tree "$1"
  printf '\n%s\n' 'pub mod probe_oracle;' >>"$SCRATCH/cqlite-core/src/lib.rs"
}
# Every oracle case asserts the guard names the module file it could not read; a bare
# non-zero exit would pass on an unrelated abort.
oracle_expect_refusal() { # <case> <outfile> <needle>
  grep -q "probe_oracle" "$2" \
    || fail_case "case $1 — the guard failed but never named \`probe_oracle\`, so it failed for some other reason; got: $(cat "$2")"
  grep -qF "$3" "$2" \
    || fail_case "case $1 — the guard failed but NOT with the intended diagnostic (\"$3\"); got: $(cat "$2")"
}

# ---------------------------------------------------------------------------
# 29. RED — the module file resolves to NEITHER legal path.
#
#     A declaration the guard cannot match to a file is a declaration it did not
#     examine. MEASURED with the found==0 branch replaced by `continue`: exit 0, with
#     the declaration silently unchecked — a false PASS, and the shape a stray
#     `#[path = "..."]` also produces.
# ---------------------------------------------------------------------------
oracle_tree no-module-file; wt29="$SCRATCH"
set +e
bash "$wt29/$GUARD_REL" >"$TMPROOT/case29.out" 2>&1
case29_rc=$?
set -e
[ "$case29_rc" -ne 0 ] || fail_case "case 29 — an unconditional \`pub mod probe_oracle;\` with NO module file passed GREEN, so the declaration was never examined; got: $(cat "$TMPROOT/case29.out")"
oracle_expect_refusal 29 "$TMPROOT/case29.out" "NEITHER of its two legal module files"
grep -qF "cqlite-core/src/probe_oracle.rs" "$TMPROOT/case29.out" \
  || fail_case "case 29 — the refusal did not name the file path it looked for; got: $(cat "$TMPROOT/case29.out")"
grep -qF "cqlite-core/src/probe_oracle/mod.rs" "$TMPROOT/case29.out" \
  || fail_case "case 29 — the refusal did not name the directory-module path it looked for"
echo "OK (29): a module file resolving to NEITHER legal path makes the oracle REFUSE, naming both paths"

# ---------------------------------------------------------------------------
# 30. RED — the module file resolves to BOTH legal paths.
#
#     rustc rejects this too ("file for module found at both"); the guard will not
#     choose one, because choosing is guessing which file carries the gate. MEASURED
#     with the found>1 branch removed (last-writer-wins on `resolved`): exit 0 while
#     reading only ONE of the two files — a false PASS whenever the gate is in the
#     other one.
# ---------------------------------------------------------------------------
oracle_tree both-module-files; wt30="$SCRATCH"
cat >"$wt30/cqlite-core/src/probe_oracle.rs" <<'RS'
//! Self-test-only probe (#1712 descope): the FILE form.
RS
mkdir -p "$wt30/cqlite-core/src/probe_oracle"
cat >"$wt30/cqlite-core/src/probe_oracle/mod.rs" <<'RS'
#![cfg(feature = "benchmarks")]
//! Self-test-only probe (#1712 descope): the DIRECTORY form, carrying the gate — so a
//! guard that silently picked the FILE form would certify an inner-gated module.
RS
set +e
bash "$wt30/$GUARD_REL" >"$TMPROOT/case30.out" 2>&1
case30_rc=$?
set -e
[ "$case30_rc" -ne 0 ] || fail_case "case 30 — a module resolving to BOTH legal paths passed GREEN, so the guard picked one of them and the gate in the other went unread; got: $(cat "$TMPROOT/case30.out")"
oracle_expect_refusal 30 "$TMPROOT/case30.out" "resolves to BOTH of its legal module files"
echo "OK (30): a module file resolving to BOTH legal paths makes the oracle REFUSE rather than choose"

# ---------------------------------------------------------------------------
# 31. RED — the module path exists but is not a READABLE REGULAR FILE.
#
#     A directory named `probe_oracle.rs`, a dangling symlink, or an unreadable mode:
#     the guard must not read "exists" as "examined". MEASURED with the readability
#     branch removed: awk fails, and without the branch the failure was reported as an
#     unexplained abort rather than a named refusal.
#
#     A DIRECTORY is used rather than chmod 000 — root ignores the mode bits, and this
#     suite runs in containers where the test would silently stop testing anything.
# ---------------------------------------------------------------------------
oracle_tree unreadable-module-file; wt31="$SCRATCH"
mkdir -p "$wt31/cqlite-core/src/probe_oracle.rs"
set +e
bash "$wt31/$GUARD_REL" >"$TMPROOT/case31.out" 2>&1
case31_rc=$?
set -e
[ "$case31_rc" -ne 0 ] || fail_case "case 31 — a module path that exists but is not a readable regular file passed GREEN; got: $(cat "$TMPROOT/case31.out")"
oracle_expect_refusal 31 "$TMPROOT/case31.out" "not a READABLE REGULAR FILE"
echo "OK (31): a module path that is not a readable regular file makes the oracle REFUSE"

# ---------------------------------------------------------------------------
# 32. RED — a BLOCK COMMENT in the prologue. THE CONDITION-2 REFUSAL.
#
#     `/* #![cfg(feature = "x")] */` before the first item is a delimiter inside a
#     comment — the SAME defect class as the five findings that got the rustdoc half
#     deleted — and handling it means a block-comment state machine (nesting, `/*`
#     inside a string, `*/` inside a string). The lead's ruling is explicit: prefer the
#     refusal, because it is bounded, obviously correct and cannot rot.
#
#     The prologue here carries a commented-out gate AND a real one after it, so a
#     guard that tried to model the comment and got it wrong in either direction is
#     visibly wrong: MEASURED with the `/*` refusal replaced by "skip the line", the
#     run reported the COMMENTED-OUT attribute as the defect at the wrong line.
# ---------------------------------------------------------------------------
oracle_tree prologue-block-comment; wt32="$SCRATCH"
cat >"$wt32/cqlite-core/src/probe_oracle.rs" <<'RS'
//! Self-test-only probe (#1712 descope, condition 2).
/* #![cfg(feature = "benchmarks")] */
pub fn probe() {}
RS
set +e
bash "$wt32/$GUARD_REL" >"$TMPROOT/case32.out" 2>&1
case32_rc=$?
set -e
[ "$case32_rc" -ne 0 ] || fail_case "case 32 — a BLOCK COMMENT in a module prologue passed GREEN, so the guard is modelling block-comment state instead of refusing; got: $(cat "$TMPROOT/case32.out")"
oracle_expect_refusal 32 "$TMPROOT/case32.out" "a BLOCK COMMENT opens in the module prologue"
grep -qF "cqlite-core/src/probe_oracle.rs:2" "$TMPROOT/case32.out" \
  || fail_case "case 32 — the refusal did not name the file AND line of the block comment; got: $(cat "$TMPROOT/case32.out")"
echo "OK (32): a block comment in a module prologue makes the oracle REFUSE, naming file and line"

# ---------------------------------------------------------------------------
# 33. RED — an inner attribute that MENTIONS `cfg` without being named `cfg`.
#
#     `#![cfg_attr(...)]` can itself apply a `cfg`, and deciding that a particular one
#     does not means parsing meta-items and erasing string contents — the parser this
#     guard has already paid for five times. So it is REFUSED, not exempted. The two
#     verdicts are deliberately DIFFERENT text (this one says "cannot be confidently
#     classified"; the `#![cfg(...)]` defect says INCONSISTENT) so the operator knows
#     which one they hit. MEASURED with the mentions_cfg branch removed: exit 0.
# ---------------------------------------------------------------------------
oracle_tree prologue-cfg-mention; wt33="$SCRATCH"
cat >"$wt33/cqlite-core/src/probe_oracle.rs" <<'RS'
//! Self-test-only probe (#1712 descope).
#![cfg_attr(feature = "benchmarks", allow(dead_code))]

pub fn probe() {}
RS
set +e
bash "$wt33/$GUARD_REL" >"$TMPROOT/case33.out" 2>&1
case33_rc=$?
set -e
[ "$case33_rc" -ne 0 ] || fail_case "case 33 — an inner \`#![cfg_attr(...)]\` in a module prologue was EXEMPTED and passed GREEN. A cfg_attr can apply a cfg; classifying it means a meta-item parser, so it must refuse; got: $(cat "$TMPROOT/case33.out")"
oracle_expect_refusal 33 "$TMPROOT/case33.out" "mentions a \`cfg\` token and cannot be confidently classified"
grep -q "INCONSISTENT" "$TMPROOT/case33.out" \
  && fail_case "case 33 — a cfg MENTION was reported as the INCONSISTENT defect. The two verdicts must be distinguishable, or the operator cannot tell a refusal from a real gate; got: $(cat "$TMPROOT/case33.out")"
echo "OK (33): an inner attribute merely MENTIONING \`cfg\` is REFUSED (not exempted), with text distinct from the defect verdict"

# ---------------------------------------------------------------------------
# 34. RED — content follows an inner attribute on the SAME line.
#
#     THE FALSE PASS THIS CLOSES: `#![doc = "]"] #![cfg(feature = "x")]`. Bracket
#     balance ends the first attribute at the `]` inside the string literal, and
#     everything after it — INCLUDING A REAL GATE — is unexamined. MEASURED with the
#     `rest != ""` refusal removed: exit 0 on this exact file, i.e. a certified
#     crate-root declaration whose module gates itself.
# ---------------------------------------------------------------------------
oracle_tree prologue-trailing-content; wt34="$SCRATCH"
cat >"$wt34/cqlite-core/src/probe_oracle.rs" <<'RS'
//! Self-test-only probe (#1712 descope).
#![doc = "]"] #![cfg(feature = "benchmarks")]

pub fn probe() {}
RS
set +e
bash "$wt34/$GUARD_REL" >"$TMPROOT/case34.out" 2>&1
case34_rc=$?
set -e
[ "$case34_rc" -ne 0 ] || fail_case "case 34 — a second inner attribute on the same line hid a real \`#![cfg]\` gate and the guard passed GREEN; got: $(cat "$TMPROOT/case34.out")"
oracle_expect_refusal 34 "$TMPROOT/case34.out" "content follows an inner attribute on the SAME line"
echo "OK (34): content after an inner attribute on one line makes the oracle REFUSE (a same-line gate cannot hide)"

# ---------------------------------------------------------------------------
# 35. RED — an inner attribute whose `[` never closes.
#
#     Unreadable input, not an exemption. MEASURED with the unterminated branch
#     returning instead of refusing: the reader walked off the end and printed CLEAN.
# ---------------------------------------------------------------------------
oracle_tree prologue-unterminated-attr; wt35="$SCRATCH"
cat >"$wt35/cqlite-core/src/probe_oracle.rs" <<'RS'
//! Self-test-only probe (#1712 descope).
#![cfg_attr(feature = "benchmarks",
RS
set +e
bash "$wt35/$GUARD_REL" >"$TMPROOT/case35.out" 2>&1
case35_rc=$?
set -e
[ "$case35_rc" -ne 0 ] || fail_case "case 35 — an unterminated inner attribute passed GREEN; got: $(cat "$TMPROOT/case35.out")"
oracle_expect_refusal 35 "$TMPROOT/case35.out" "never closes its \`[\`"
echo "OK (35): an inner attribute that never closes its \`[\` makes the oracle REFUSE"

# ---------------------------------------------------------------------------
# 36. GREEN — THE POSITIVE CONTROL for 29-35.
#
#     Without it, every case above would be satisfied by a guard hardwired to refuse
#     any prologue it is handed — a refusal that reds correct code gets waived, and a
#     waived guard guards nothing. So an ORDINARY prologue must certify: `//!` inner
#     doc comments, blank lines, a multi-line INERT inner attribute, and an inner
#     `#![doc = "…"]`.
#
#     It also pins the SCOPE of the case-32 refusal, which is the other half of not
#     reding correct code: a `/* */` block comment AFTER the first item is OUTSIDE the
#     prologue — rustc forbids an inner attribute there, so the reader has already
#     stopped and never sees it — and must NOT refuse. (The cost this accepts, stated:
#     a `/* */` LICENSE HEADER at the very top of a module file declared
#     unconditionally at the crate root DOES refuse. No file in cqlite-core has one —
#     case 1 reads all 14 of them — and the remedy is one `//`.)
# ---------------------------------------------------------------------------
oracle_tree prologue-inert; wt36="$SCRATCH"
cat >"$wt36/cqlite-core/src/probe_oracle.rs" <<'RS'
//! Self-test-only probe (#1712 descope): an ORDINARY prologue that must certify.
//!
//! Note that this line mentions a /* block comment */ inside a `//` comment, which
//! is unambiguously terminated by the newline and must not trip the refusal.

#![allow(
    dead_code,
    unused_imports
)]
#![doc = "an inner doc attribute mentioning nothing structural"]

/// Self-test-only probe: the FIRST ITEM ends the prologue here.
pub fn probe() {}

/* A block comment AFTER the first item is OUTSIDE the prologue: rustc forbids an
   inner attribute here (measured), so the reader has already stopped and must never
   see this — including the `#![cfg(feature = "benchmarks")]` mentioned in this very
   sentence, which is inert text in a file region the guard does not read. */
pub fn probe_two() {}
RS
set +e
bash "$wt36/$GUARD_REL" >"$TMPROOT/case36.out" 2>&1
case36_rc=$?
set -e
[ "$case36_rc" -eq 0 ] || {
  echo "FAIL: case 36 — an ORDINARY module prologue (//! comments, blank lines, a multi-line"
  echo "      #![allow(...)] and an #![doc = \"...\"]) was REFUSED, or a /* */ block AFTER the"
  echo "      first item — outside the prologue, which rustc guarantees — was read. It was"
  echo "      REFUSED. Cases 29-35 then prove nothing: a guard that refuses everything"
  echo "      satisfies all of them, and a refusal that reds correct code gets waived."
  cat "$TMPROOT/case36.out"
  exit 1
}
grep -qE "$MEASURED_RE" "$TMPROOT/case36.out" \
  || fail_case "case 36 — the guard exited 0 without its affirmative measurement line; got: $(cat "$TMPROOT/case36.out")"
c36_open="$(sed -E 's/.*of which ([0-9]+) unconditional.*/\1/' "$TMPROOT/case36.out")"
c36_read="$(sed -E 's/.*; ([0-9]+) module-file prologues read.*/\1/' "$TMPROOT/case36.out")"
[ "$c36_open" -eq "$((base_open + 1))" ] \
  || fail_case "case 36 — the added unconditional declaration did not move the count ($base_open -> $c36_open), so \`probe_oracle\` was never examined and the green is vacuous"
[ "$c36_read" -eq "$c36_open" ] \
  || fail_case "case 36 — $c36_open unconditional declarations but only $c36_read prologues read; one was skipped"
echo "OK (36): an ordinary prologue with INERT inner attributes certifies normally, and the added module really was examined"
echo ""
echo "PASS: test_pub_surface_guard.sh — all 23 cases (5 green, 16 reds, 1 usage, 1 kill-safety)"
