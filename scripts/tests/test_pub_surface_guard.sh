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
#  35.  RED    — an inner attribute whose `[` never closes, and (b) one whose NAME
#                cannot be read — without (b) an unreadable attribute falls through to
#                "mentions no cfg, therefore inert".
#  39.  RED    — a crate root with ZERO unconditional declarations. Every exemption is
#                a `continue`, so an empty OPEN set walks the loop examining nothing;
#                unguarded that prints a success line and exits 0. REACHABLE BY ACCIDENT
#                from one over-broad `#[doc(hidden)]` sweep over the crate root.
#  40.  RED    — a RAW STRING in a declaration attribute leaks structure into the
#                meta-item parse and can flip OPEN to an EXEMPTING verdict.
#  41.  RED    — a `#[path]` DECOY: a clean file at the standard path while the real,
#                gated module lives where `#[path]` points.
#  42.  RED    — a first-line SHEBANG ending the prologue scan and hiding a gate.
#  43.  RED    — TWO declarations on one line (`#[cfg(any())] pub mod x; pub mod x;`,
#                valid Rust) deduplicated into agreement by `sort -u`, skipping the
#                UNCONDITIONAL one.
#  44.  RED    — a raw BYTE string (`br#"`) slipping past a refusal that recognised
#                only `r#"`.
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

# --- Shared fixture helpers for the `benchmarks` declaration -------------------
#
# WHY THESE EXIST, and why the awk they replace was a liability. Cases 2/8/9 used to
# carry three near-identical awk programs keyed on `/^#\[cfg\(feature =
# "benchmarks"\)\]$/` and on the gate being the line IMMEDIATELY above the
# declaration. Both assumptions broke, twice, on cosmetic edits to lib.rs that had
# nothing to do with them: a trailing `// #1712: gate HERE …` comment defeated the `$`
# anchor (case 2's revert then silently NO-OPPED and it CERTIFIED the shape it exists
# to red), and rustfmt subsequently moving that comment onto its OWN line put a line
# between the gate and the declaration. A fixture that can quietly stop reproducing its
# own subject is worse than no fixture, so the setup is now one helper with an
# AFFIRMATIVE post-condition: after stripping, `lib.rs` must not mention the
# `benchmarks` feature at all.
bench_strip_site_gate() { # <lib.rs path> <case label>
  # Restores the EXACT pre-#1712 shape — a bare `pub mod benchmarks;` — whatever the
  # current tree's layout is: the gate on its own line or trailing, and the `#1712`
  # note as its own line or trailing the declaration. Keyed on the DECLARATION, never
  # on the gate's position relative to it.
  awk '
    /^#\[cfg\(feature = "benchmarks"\)\]/ { next }
    /^\/\/ #1712: gate HERE/ { next }
    /^pub mod benchmarks;/ { print "pub mod benchmarks;"; next }
    { print }
  ' "$1" >"$1.stripped"
  mv "$1.stripped" "$1"
  grep -qx 'pub mod benchmarks;' "$1" \
    || fail_case "case $2 setup: the bare \`pub mod benchmarks;\` declaration is not there after stripping the site gate"
  # THE POST-CONDITION. Not "the anchored pattern no longer matches" — the FEATURE is
  # not mentioned anywhere in the crate root, which is the state these cases need and
  # the one an anchored grep could not establish.
  if grep -q 'cfg(feature = "benchmarks")' "$1"; then
    grep -n 'cfg(feature = "benchmarks")' "$1" >&2
    fail_case "case $2 setup: a declaration-site gate on the \`benchmarks\` feature SURVIVED the strip (above), so the case would pass for the wrong reason"
  fi
}

bench_attr_above_decl() { # <lib.rs path> <attribute text> <case label>
  awk -v attr="$2" '
    /^pub mod benchmarks;/ { print attr }
    { print }
  ' "$1" >"$1.attr"
  mv "$1.attr" "$1"
  grep -qxF "$2" "$1" \
    || fail_case "case $3 setup: could not put \`$2\` at the declaration site"
}

bench_attr_same_line() { # <lib.rs path> <attribute text> <case label>
  awk -v attr="$2" '
    /^pub mod benchmarks;/ { print attr " " $0; next }
    { print }
  ' "$1" >"$1.same"
  mv "$1.same" "$1"
  grep -qxF "$2 pub mod benchmarks;" "$1" \
    || fail_case "case $3 setup: could not put the attribute and the declaration on ONE line"
}

# Put the gate back where #1712 found it: INSIDE the module's own file, invisible to
# every reader of the crate root.
bench_add_inner_gate() { # <scratch root>
  printf '%s\n%s\n' '#![cfg(feature = "benchmarks")]' "$(cat "$1/cqlite-core/src/benchmarks/mod.rs")" \
    >"$1/cqlite-core/src/benchmarks/mod.rs.new"
  mv "$1/cqlite-core/src/benchmarks/mod.rs.new" "$1/cqlite-core/src/benchmarks/mod.rs"
  head -1 "$1/cqlite-core/src/benchmarks/mod.rs" | grep -qx '#!\[cfg(feature = "benchmarks")\]' \
    || fail_case "setup: could not put the inner gate back into benchmarks/mod.rs"
}

# The guard's affirmative measurement line, as a REGEX matched WHOLE. Kept in sync BY
# HAND with the guard's own success line and with `agent-gate.sh`'s `pub-surface`
# component — a wording change must land in all three at once (#1712 descope).
MEASURED_RE='^pub-surface: [0-9]+ crate-root declarations scanned in cqlite-core/src/lib\.rs \([0-9]+ pub mod, of which [1-9][0-9]* unconditional\); [1-9][0-9]* module-file prologues read from source; 0 inconsistent$'

# ps_measured_field <what> <file> <sed-body> — pull ONE integer out of the guard's OWN
# measurement line, and REFUSE anything else.
#
# THIS FILE WAS INCONSISTENT WITH ITSELF, and it cost a gate of record. `MEASURED_RE` above
# is properly line-anchored, while the extractions below were `sed -E 's/.*of which
# ([0-9]+) unconditional.*/\1/'` over the WHOLE output — the SUBSTITUTE form, which passes
# every NON-matching line through UNCHANGED. The guard's output is multi-line, so the moment
# `check-pub-surface.sh` gained a second line carrying the word `unconditional` (#3162's
# `AGENT-GATE-CENSUS:` contract line, which had no `of which` — since reverted, but the
# anchoring stands on its own and the guard's output will gain lines again), the result became a
# TWO-LINE string beginning `AGENT-GATE-CENSUS: 14 unconditional …`; `$((base_open + 1))`
# then read `AGENT` as a variable name and `set -u` made it fatal:
#     test_pub_surface_guard.sh: line 403: AGENT: unbound variable
# The defect is an UNANCHORED PARSE OF MULTI-LINE OUTPUT — #3400's parse-site rule, one
# directory over — not the wording of the new line. Rewording the guard to dodge a colliding
# word would trade a descriptive census for a taboo, and the next colliding word brings it
# straight back.
#
# So: `-n … p` (print ONLY matching lines, never pass-through), anchored to the guard's own
# `^pub-surface: ` line, and the result VALIDATED as a single integer before any caller does
# arithmetic on it. A non-integer or a multi-line result is a NAMED failure here instead of a
# bash arithmetic error thirty lines away.
#
# NOTE the contrast with the gate's own consumer, which is SAFE and must not be "fixed" to
# match: `run_pub_surface` greps the anchored MEASURED_RE into a single-line `$measured`
# FIRST and only then seds that one line. Anchoring first is what makes the substitute form
# harmless there.
ps_measured_field() {
  local what="$1" f="$2" body="$3" v
  v="$(sed -nE "s/^pub-surface: ${body}/\1/p" "$f")"
  case "$v" in
    ''|*[!0-9]*)
      fail_case "extraction of '$what' from $f did not yield a single integer (got: $(printf '%s' "$v" | tr '\n' '|')). The guard's measurement line is missing, reshaped, or another line matched — an unanchored parse of multi-line output is what broke this suite once (#3162)." ;;
  esac
  printf '%s' "$v"
}
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
base_open_probe="$(ps_measured_field 'baseline unconditional count' "$TMPROOT/green.out" '.*of which ([0-9]+) unconditional.*')"
base_read_probe="$(ps_measured_field 'baseline prologues-read count' "$TMPROOT/green.out" '.*; ([0-9]+) module-file prologues read.*')"
# ---------------------------------------------------------------------------
# 1b. THE EXTRACTION SURVIVES A SECOND LINE CARRYING THE SAME KEYWORD (#3162).
#
#     This is the durable half of the fix, and the reason it is a case rather than a
#     comment: the guard's output is MULTI-LINE and will gain lines again. What must hold
#     is not "no other line says `unconditional`" — that is a taboo on wording, and the
#     next colliding word reinstates the bug — but that the extraction READS ONLY THE
#     GUARD'S OWN MEASUREMENT LINE.
#
#     The first decoy is the line that HISTORICALLY broke this suite — #3162's
#     `AGENT-GATE-CENSUS:` contract line, since reverted, so it is now synthetic. That is
#     the right shape for this case and not a weakening: the property is about ANY second
#     line carrying the keyword, and pinning it to a line that still exists would make the
#     case lapse the moment that line changed again. The second decoy carries
#     `module-file prologues read` so BOTH extractions are exercised. The RED half is
#     inline and explicit — the OLD
#     substitute form is run over the same input and required to produce something OTHER
#     than the integer — because a green here would otherwise prove nothing about whether
#     the anchoring is what is doing the work.
# ---------------------------------------------------------------------------
{
  printf 'AGENT-GATE-CENSUS: 14 unconditional crate-root pub mod declaration(s) verified against their module prologues\n'
  printf 'note: 99 module-file prologues read from source (a decoy, not the guard line)\n'
  cat "$TMPROOT/green.out"
} >"$TMPROOT/decoyed.out"
d_open="$(ps_measured_field 'decoyed unconditional count' "$TMPROOT/decoyed.out" '.*of which ([0-9]+) unconditional.*')"
d_read="$(ps_measured_field 'decoyed prologues-read count' "$TMPROOT/decoyed.out" '.*; ([0-9]+) module-file prologues read.*')"
if [ "$d_open" = "$base_open_probe" ] && [ "$d_read" = "$base_read_probe" ]; then
  echo "OK (1b): both extractions ignore decoy lines carrying the same keywords and read only the \`pub-surface: \` measurement line ($d_open unconditional, $d_read prologues)"
else
  fail_case "case 1b — a decoy line changed the extracted counts (unconditional $base_open_probe -> $d_open, prologues $base_read_probe -> $d_read). The parse is not confined to the guard's own line."
fi
# RED control: the pre-fix unanchored SUBSTITUTE form over the SAME input. It must NOT
# yield the integer — otherwise the anchoring above is not what makes 1b pass.
old_form="$(sed -E 's/.*of which ([0-9]+) unconditional.*/\1/' "$TMPROOT/decoyed.out")"
if [ "$old_form" = "$base_open_probe" ]; then
  fail_case "case 1b RED — the pre-fix unanchored form ALSO returned '$old_form' on the decoyed input, so this case does not demonstrate that anchoring is load-bearing"
else
  echo "OK (1b RED): the pre-fix unanchored form returns a multi-line, non-integer value on the same input — the anchoring is what carries the correctness"
fi
# ---------------------------------------------------------------------------
# 2. RED — the consistency assert, against the pre-#1712 source shape.
# ---------------------------------------------------------------------------
scratch_tree pre-1712; wt2="$SCRATCH"
# Restore the bare, ungated crate-root declaration, then put the gate back inside the
# module file — the exact pre-#1712 shape, which is still origin/main's shape at the
# time of writing.
bench_strip_site_gate "$wt2/cqlite-core/src/lib.rs" 2
bench_add_inner_gate "$wt2"

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
bench_strip_site_gate "$wt8/cqlite-core/src/lib.rs" 8
bench_attr_above_decl "$wt8/cqlite-core/src/lib.rs" \
  '#[cfg_attr(feature = "benchmarks", doc = "opt-in perf runs")]' 8
bench_add_inner_gate "$wt8"
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
bench_strip_site_gate "$wt9/cqlite-core/src/lib.rs" 9
bench_attr_same_line "$wt9/cqlite-core/src/lib.rs" \
  '#[cfg_attr(feature = "benchmarks", doc = "opt-in perf runs")]' 9
bench_add_inner_gate "$wt9"
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
base_open="$(ps_measured_field 'baseline unconditional count' "$TMPROOT/green.out" '.*of which ([0-9]+) unconditional.*')"
case10_open="$(ps_measured_field 'case-10 unconditional count' "$TMPROOT/case10.out" '.*of which ([0-9]+) unconditional.*')"
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
bench_strip_site_gate "$wt15/cqlite-core/src/lib.rs" 15
bench_attr_above_decl "$wt15/cqlite-core/src/lib.rs" \
  '#[doc = "this text mentions doc(hidden) but hides nothing"]' 15
bench_attr_above_decl "$wt15/cqlite-core/src/lib.rs" \
  '#[cfg_attr(docsrs, doc(alias = "cfg(foo)"))]' 15
bench_add_inner_gate "$wt15"
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
  /^pub mod benchmarks;/ {
    print ""
    print "/// A doc comment between the gate and the item."
    print "// …and an ordinary comment too."
    print ""
  }
  { print }
' "$wt16/cqlite-core/src/lib.rs" >"$wt16/lib.rs.separated"
mv "$wt16/lib.rs.separated" "$wt16/cqlite-core/src/lib.rs"
grep -q 'A doc comment between the gate and the item' "$wt16/cqlite-core/src/lib.rs" \
  || fail_case "case 16 setup: could not insert the separator lines"
# The gate must STILL be there — this case's whole point is that a real one keeps
# gating across the separators.
grep -q 'cfg(feature = "benchmarks")' "$wt16/cqlite-core/src/lib.rs" \
  || fail_case "case 16 setup: the real declaration-site gate is gone, so a green here would prove nothing"
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
ps_register_extra "$leaked_root/leaked" "$leaked_root"
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
ps_register_extra "$peer_root/live" "$peer_root"
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
# (d) THE ARITHMETIC (roborev r9 F4). A stub whose line matches the WORDING exactly
#     but is arithmetically IMPOSSIBLE — 14 unconditional declarations, 1 prologue
#     read, i.e. 13 silently unexamined. The regex alone accepts it. The guard itself
#     asserts prologues == unconditional, so such a line proves the guard is not what
#     produced it (stub, truncation, stale build), and this component exists to be
#     INDEPENDENT of the guard rather than to trust it.
cat >"$wt26/$GUARD_REL" <<'STUB'
#!/usr/bin/env bash
# Self-test stub (#1712 r9 F4): the WORDING is right, the ARITHMETIC is impossible.
echo "pub-surface: 26 crate-root declarations scanned in cqlite-core/src/lib.rs (20 pub mod, of which 14 unconditional); 1 module-file prologues read from source; 0 inconsistent"
exit 0
STUB
set +e
gate26 "$TMPROOT/case26d-summary.txt" "$TMPROOT/case26d.out"
case26d_rc=$?
set -e
grep -qE '^pub-surface: +FAIL' "$TMPROOT/case26d-summary.txt" \
  || fail_case "case 26(d) — an ARITHMETICALLY INCOHERENT measurement line (14 unconditional, 1 prologue read) was recorded PASS. Matching the wording is not evidence of a measurement: 13 declarations went unexamined; got: $(grep -E 'pub-surface|RESULT:' "$TMPROOT/case26d-summary.txt" || echo '(no pub-surface line)')"
grep -qF 'ARITHMETICALLY INCOHERENT' "$TMPROOT/case26d.out" \
  || fail_case "case 26(d) — the component failed but not with the named incoherence diagnostic; got: $(cat "$TMPROOT/case26d.out")"
[ "$case26d_rc" -eq 1 ] \
  || fail_case "case 26(d) — expected FAIL exit 1, got $case26d_rc (3 is a successful --only run, i.e. the vacuous pass)"

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
  bench_strip_site_gate "$SCRATCH/cqlite-core/src/lib.rs" "28 ($1)"
  # Drop the now-bare declaration; the case re-adds it in the shape under test.
  grep -v '^pub mod benchmarks;' "$SCRATCH/cqlite-core/src/lib.rs" >"$SCRATCH/lib.rs.nodecl"
  mv "$SCRATCH/lib.rs.nodecl" "$SCRATCH/cqlite-core/src/lib.rs"
  grep -q 'pub mod benchmarks' "$SCRATCH/cqlite-core/src/lib.rs" \
    && fail_case "case 28 setup ($1): the original benchmarks declaration survived the strip"
  printf '%s\n%s\n%s\n' '/* a block comment that closes on the SAME line as the declaration' '   (issue #1712 roborev r7 F2)' "$2" >>"$SCRATCH/cqlite-core/src/lib.rs"
  # …and the gate hides inside the module file, which is the whole point.
  bench_add_inner_gate "$SCRATCH"
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
#     examine. This is also the shape a stray `#[path = "..."]` produces.
#
#     MEASURED with the found==0 branch replaced by `continue`: the run still FAILS —
#     caught by the guard's READ_COUNT == OPEN_COUNT backstop, which is DEFENCE IN
#     DEPTH working as intended — but with a diagnostic that names only the COUNTS
#     ("15 unconditional declarations … only 14 prologues read"), not the declaration
#     or the file. Both layers are kept deliberately: the backstop makes a skip
#     impossible, and this refusal makes it ACTIONABLE.
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
#     the guard must not read "exists" as "examined".
#
#     MEASURED with the readability branch removed: exit 0 — a FALSE PASS, not an
#     abort. awk handed a DIRECTORY emits a warning on stderr (which the guard
#     captures and only prints on failure) and reads NO RECORDS, so the reader's END
#     block prints CLEAN and the declaration is certified having been read from
#     nothing. That is precisely "a positive verdict from an absent measurement", so
#     the existence test alone can never stand in for the readability test.
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
#     THE FIXTURE IS BUILT TO FALSIFY THE NAIVE ALTERNATIVE, because the obvious
#     "just skip the `/*` line" is what a future reader will reach for. It carries the
#     lead's literal shape (`/* #![cfg(feature = "x")] */`), then a MULTI-LINE block
#     comment whose CONTENT looks like an item, then a REAL `#![cfg]` gate. MEASURED
#     with the refusal replaced by "skip the line": exit 0 — the skip lands on
#     `pub fn not_really_an_item() {}` inside the comment, reads it as the first item,
#     ends the prologue there, and never sees the real gate two lines later. A FALSE
#     PASS, from the very simplification that looks harmless.
# ---------------------------------------------------------------------------
oracle_tree prologue-block-comment; wt32="$SCRATCH"
cat >"$wt32/cqlite-core/src/probe_oracle.rs" <<'RS'
//! Self-test-only probe (#1712 descope, condition 2).
/* #![cfg(feature = "benchmarks")] */
/* a multi-line block comment whose CONTENT looks like the first item:
   pub fn not_really_an_item() {}
*/
#![cfg(feature = "benchmarks")]
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

# (b) THE SAME CHANNEL'S OTHER ARM: an inner attribute whose NAME cannot be read.
#     Classification must START from an identifier; with no identifier there is nothing
#     to compare against `cfg`, so the reader must REFUSE rather than fall through to
#     "mentions no cfg, therefore inert". MEASURED with the `nm == ""` branch removed:
#     exit 0 — an unreadable attribute was treated as an inert one.
oracle_tree prologue-unreadable-attr-name; wt35b="$SCRATCH"
cat >"$wt35b/cqlite-core/src/probe_oracle.rs" <<'RS'
//! Self-test-only probe (#1712 descope).
#![ 42 ]

pub fn probe() {}
RS
set +e
bash "$wt35b/$GUARD_REL" >"$TMPROOT/case35b.out" 2>&1
case35b_rc=$?
set -e
[ "$case35b_rc" -ne 0 ] || fail_case "case 35(b) — an inner attribute whose NAME cannot be read passed GREEN, i.e. it was treated as inert; got: $(cat "$TMPROOT/case35b.out")"
oracle_expect_refusal "35(b)" "$TMPROOT/case35b.out" "an inner attribute whose name cannot be read"
echo "OK (35): an inner attribute that never closes its \`[\`, or whose NAME cannot be read, makes the oracle REFUSE"

# ---------------------------------------------------------------------------
# 37. RED — a bogus `]` inside a LINE COMMENT that ENDS THE LINE (roborev r8 F1).
#
#     THE FALSE PASS THIS CLOSES: `#![allow(dead_code, // ]`. Bracket balance counts
#     the `]` inside the comment, so the attribute window closes EARLY and the scan
#     resumes MID-ATTRIBUTE, never examining the real `#![cfg(...)]` below it.
#
#     WHY CASE 34 DOES NOT ALREADY COVER THIS, which is the whole point of adding a
#     separate case: 34's refusal fires on content AFTER the closing `]` on the same
#     line. Here the bogus `]` IS THE END OF THE LINE — nothing follows it, `rest` is
#     empty, and that refusal cannot see it. Two neighbouring shapes, two mechanisms.
#
#     MEASURED against the PRE-FIX reader on this exact fixture: `CLEAN 3` — a
#     certified crate-root declaration whose module gates itself. The line comment is
#     refused rather than parsed because lines are JOINED WITH A SPACE for balancing,
#     so a `//`'s newline terminator is already gone and its end is not locatable.
# ---------------------------------------------------------------------------
oracle_tree prologue-comment-bracket; wt37="$SCRATCH"
cat >"$wt37/cqlite-core/src/probe_oracle.rs" <<'RS'
//! Self-test-only probe (#1712 roborev r8 F1).
#![allow(dead_code, // ]
    unused_imports)]
#![cfg(feature = "benchmarks")]

pub fn probe() {}
RS
set +e
bash "$wt37/$GUARD_REL" >"$TMPROOT/case37.out" 2>&1
case37_rc=$?
set -e
[ "$case37_rc" -ne 0 ] || fail_case "case 37 — a \`]\` inside a LINE COMMENT closed the attribute window early, hiding the real \`#![cfg(feature = \"benchmarks\")]\` two lines below it, and the guard passed GREEN. This is the false PASS roborev r8 F1 named; got: $(cat "$TMPROOT/case37.out")"
oracle_expect_refusal 37 "$TMPROOT/case37.out" "contains a COMMENT"
echo "OK (37): a \`]\` inside a line comment makes the oracle REFUSE rather than close the window early and miss a later gate"

# ---------------------------------------------------------------------------
# 38. RED — a leading UTF-8 BOM before `#![cfg(...)]` (roborev r8 F2).
#
#     rustc ACCEPTS AND IGNORES one leading BOM, so the gate really does apply. Without
#     stripping it the `#![` test does not match, the prologue reads CLEAN, and the
#     module is certified while the compiled crate does not contain it.
#     MEASURED against the PRE-FIX reader on this exact fixture: `CLEAN 1`.
#
#     THIS CASE ASSERTS THE DEFECT VERDICT, NOT A REFUSAL, and that distinction is the
#     reason it is written this way: a BOM is EXACTLY modellable (rustc skips exactly
#     one), so the honest answer is the named #1712 INCONSISTENT diagnostic carrying the
#     hoist remedy — not "cannot classify". Refusing here would have been the lazy fix
#     and would have degraded a precise verdict into an unactionable one.
#
#     IT ALSO PINS `LC_ALL=C` ON THE READER, which is load-bearing and invisible: under
#     a UTF-8 locale awk's `sprintf("%c", 239)` yields the CHARACTER U+00EF (two bytes),
#     not the byte 0xEF, so the BOM comparison silently never matches. Demonstrated
#     both ways on the SAME awk program: UTF-8 locale => CLEAN (the bug), LC_ALL=C =>
#     GATED. Anyone who drops that prefix reds this case.
# ---------------------------------------------------------------------------
oracle_tree prologue-bom; wt38="$SCRATCH"
{ printf '\357\273\277'
  cat <<'RS'
#![cfg(feature = "benchmarks")]

//! Self-test-only probe (#1712 roborev r8 F2).
pub fn probe() {}
RS
} >"$wt38/cqlite-core/src/probe_oracle.rs"
set +e
bash "$wt38/$GUARD_REL" >"$TMPROOT/case38.out" 2>&1
case38_rc=$?
set -e
[ "$case38_rc" -ne 0 ] || fail_case "case 38 — a UTF-8 BOM hid an inner \`#![cfg(...)]\` from the reader and the guard passed GREEN. rustc ignores a leading BOM, so the gate APPLIES; got: $(cat "$TMPROOT/case38.out")"
grep -q "probe_oracle" "$TMPROOT/case38.out" \
  || fail_case "case 38 — the guard failed but never named \`probe_oracle\`, so it failed for some other reason; got: $(cat "$TMPROOT/case38.out")"
grep -q "INCONSISTENT" "$TMPROOT/case38.out" \
  || fail_case "case 38 — a BOM-prefixed \`#![cfg(...)]\` was not reported as the INCONSISTENT #1712 defect. A BOM is exactly modellable (rustc skips one), so the verdict must be the NAMED defect carrying the hoist remedy, not a refusal; got: $(cat "$TMPROOT/case38.out")"
echo "OK (38): a BOM-prefixed inner \`#![cfg(...)]\` is caught as the NAMED defect (not merely refused), which also pins LC_ALL=C on the reader"

# ---------------------------------------------------------------------------
# 40/41. RED — DECLARATION-SIDE attributes that defeat the parse (roborev r9 F1/F2).
#
#     POLARITY IS THE WHOLE POINT HERE. `GATED` and `HIDDEN` are the EXEMPTING
#     verdicts — only an `OPEN` declaration gets its module file read — so anything
#     that flips OPEN to GATED, or that makes resolution read the WRONG file, skips
#     the inspection entirely and certifies an inner-gated module.
#
#     40: a RAW STRING in a declaration attribute. `strip_strings` erases ordinary
#         string contents before structure is read, but does not model `r#*"`, so
#         `doc = r##"", cfg(any()), ""##` leaks a comma and a `cfg(...)` into
#         `split_meta`. MEASURED pre-fix: exit 0.
#     41: a `#[path]` DECOY — a CLEAN `NAME.rs` beside `#[path = "actual.rs"]`, with
#         the real gate in `actual.rs`. Resolution reads the decoy and certifies.
#         MEASURED pre-fix: exit 0. This is the exploit, not just the attribute:
#         `#[path]` with NO standard-path file already refused via found==0.
# ---------------------------------------------------------------------------
scratch_tree decl-rawstring; wt40="$SCRATCH"
printf '\n#[cfg_attr(feature = "x", doc = r##"", cfg(any()), ""##)]\npub mod probe_decl;\n' >>"$wt40/cqlite-core/src/lib.rs"
printf '//! probe\npub fn p() {}\n' >"$wt40/cqlite-core/src/probe_decl.rs"
set +e
bash "$wt40/$GUARD_REL" >"$TMPROOT/case40.out" 2>&1
case40_rc=$?
set -e
[ "$case40_rc" -ne 0 ] || fail_case "case 40 — a RAW STRING in a declaration attribute leaked structural text into the meta-item parse and the guard passed GREEN. A flipped verdict EXEMPTS the module from inspection; got: $(cat "$TMPROOT/case40.out")"
grep -qF "RAW STRING" "$TMPROOT/case40.out" \
  || fail_case "case 40 — the guard failed but NOT with the raw-string diagnostic, so it failed for another reason; got: $(cat "$TMPROOT/case40.out")"
echo "OK (40): a RAW STRING in a crate-root declaration attribute makes the guard REFUSE rather than let leaked structure flip it to an EXEMPTING verdict"


scratch_tree decl-path-decoy; wt41="$SCRATCH"
printf '\n#[path = "probe_actual.rs"]\npub mod probe_decl;\n' >>"$wt41/cqlite-core/src/lib.rs"
printf '//! CLEAN DECOY — the file resolution would reach\npub fn p() {}\n' >"$wt41/cqlite-core/src/probe_decl.rs"
printf '#![cfg(feature = "benchmarks")]\n//! the REAL module, and it GATES ITSELF\npub fn p() {}\n' >"$wt41/cqlite-core/src/probe_actual.rs"
set +e
bash "$wt41/$GUARD_REL" >"$TMPROOT/case41.out" 2>&1
case41_rc=$?
set -e
[ "$case41_rc" -ne 0 ] || fail_case "case 41 — a \`#[path]\` declaration with a CLEAN DECOY at the standard path passed GREEN: the guard read the decoy while the real module file gates itself; got: $(cat "$TMPROOT/case41.out")"
grep -qF 'path\` attribute' "$TMPROOT/case41.out" \
  || grep -qF "path" "$TMPROOT/case41.out" \
  || fail_case "case 41 — the guard failed but never named the \`path\` attribute as the cause; got: $(cat "$TMPROOT/case41.out")"
echo "OK (41): a \`#[path]\` declaration with a CLEAN DECOY at the standard path REFUSES rather than certify from the wrong file"

# ---------------------------------------------------------------------------
# 42. RED — a first-line SHEBANG hiding an inner gate (roborev r9 F3).
#
#     rustc accepts `#!...` on line 1 when it is not `#![` — VERIFIED with rustc
#     1.98.0: `#!/usr/bin/env rust` + `#![cfg(feature = "nope")]` compiles and the
#     gate APPLIES (no symbol emitted). Pre-fix the shebang read as the first item,
#     the prologue ended at line 1, and the module was CERTIFIED. MEASURED: CLEAN 1.
#
#     Asserts the NAMED DEFECT, not a refusal: exactly one shebang is possible and
#     only on line 1, so this is exactly modellable and the honest verdict is the
#     #1712 INCONSISTENT diagnostic with its hoist remedy.
# ---------------------------------------------------------------------------
oracle_tree prologue-shebang; wt42="$SCRATCH"
printf '#!/usr/bin/env rust\n#![cfg(feature = "benchmarks")]\n\n//! Self-test-only probe (#1712 r9 F3).\npub fn probe() {}\n' >"$wt42/cqlite-core/src/probe_oracle.rs"
set +e
bash "$wt42/$GUARD_REL" >"$TMPROOT/case42.out" 2>&1
case42_rc=$?
set -e
[ "$case42_rc" -ne 0 ] || fail_case "case 42 — a first-line shebang ended the prologue scan and HID the inner \`#![cfg(...)]\` below it; the guard passed GREEN on a module rustc configures out; got: $(cat "$TMPROOT/case42.out")"
grep -q "probe_oracle" "$TMPROOT/case42.out" \
  || fail_case "case 42 — the guard failed but never named \`probe_oracle\`; got: $(cat "$TMPROOT/case42.out")"
grep -q "INCONSISTENT" "$TMPROOT/case42.out" \
  || fail_case "case 42 — a shebang-hidden \`#![cfg(...)]\` was not reported as the INCONSISTENT #1712 defect. A shebang is exactly modellable (one, line 1 only), so the verdict must be the NAMED defect carrying the hoist remedy, not a refusal; got: $(cat "$TMPROOT/case42.out")"
echo "OK (42): a first-line shebang does not hide an inner \`#![cfg(...)]\` — it is caught as the NAMED defect"

# ---------------------------------------------------------------------------
# 43. RED — TWO declarations on ONE line, deduplicated into agreement (roborev r10).
#
#     THE FALSE PASS THIS CLOSES, and it is the subtlest one in this diff:
#         #[cfg(any())] pub mod probe_dup; pub mod probe_dup;
#     That is VALID Rust — the first is configured OUT, so there is no duplicate
#     definition — and the SECOND is unconditional. The structured scan reads only the
#     FIRST statement on a line, so it derives ONE (gated) record; the simple scan
#     derives TWO. The cross-check used `sort -u`, which made the two derivations AGREE,
#     and the unconditional declaration was then never examined.
#
#     MEASURED against the PRE-FIX guard on this exact tree: exit 0, with the success
#     line reporting "14 unconditional; 14 module-file prologues read" — a certified
#     tree containing an unconditional declaration whose module file gates itself.
#
#     The fix compares MULTISETS (`sort`, not `sort -u`), which routes this into the
#     pre-existing "the two scans disagree" refusal — the correct verdict, because the
#     guard genuinely cannot pin down the module set.
# ---------------------------------------------------------------------------
scratch_tree decl-duplicate-line; wt43="$SCRATCH"
printf '\n#[cfg(any())] pub mod probe_dup; pub mod probe_dup;\n' >>"$wt43/cqlite-core/src/lib.rs"
printf '#![cfg(feature = "benchmarks")]\n//! inner-gated: must NOT be certified\npub fn p() {}\n' >"$wt43/cqlite-core/src/probe_dup.rs"
set +e
bash "$wt43/$GUARD_REL" >"$TMPROOT/case43.out" 2>&1
case43_rc=$?
set -e
[ "$case43_rc" -ne 0 ] || fail_case "case 43 — two declarations on one line were deduplicated into agreement, so the UNCONDITIONAL one was never examined and the guard certified a module that gates itself; got: $(cat "$TMPROOT/case43.out")"
grep -qF "disagree about which modules" "$TMPROOT/case43.out" \
  || fail_case "case 43 — the guard failed but NOT via the scan-disagreement refusal, so it failed for another reason; got: $(cat "$TMPROOT/case43.out")"
grep -qF "probe_dup" "$TMPROOT/case43.out" \
  || fail_case "case 43 — the failure never named \`probe_dup\`; got: $(cat "$TMPROOT/case43.out")"
echo "OK (43): two declarations on one line are compared as MULTISETS, so dedup cannot make the two scans agree and skip an unconditional declaration"

# ---------------------------------------------------------------------------
# 44. RED — a raw BYTE string in a declaration attribute (roborev r10).
#
#     A FAIR HIT ON THE r9 F1 FIX, which is why it gets its own case. That fix refused
#     `r#*"` and was anchored on a non-identifier boundary so an ordinary string ending
#     in `r` (`doc = "for"`) would not false-fire. But `b` and `c` ARE identifier
#     characters, so `br#"…"#` and `cr#"…"#` slipped straight past the check meant to
#     stop them: THE LEAK WAS NARROWER THAN THE FIX. Rust has `r`, `br` and `cr` raw
#     prefixes; all three are now refused, and the ordinary-string controls still pass.
# ---------------------------------------------------------------------------
scratch_tree decl-raw-byte-string; wt44="$SCRATCH"
printf '\n#[cfg_attr(feature = "x", doc = br##"", cfg(any()), ""##)]\npub mod probe_decl;\n' >>"$wt44/cqlite-core/src/lib.rs"
printf '//! probe\npub fn p() {}\n' >"$wt44/cqlite-core/src/probe_decl.rs"
set +e
bash "$wt44/$GUARD_REL" >"$TMPROOT/case44.out" 2>&1
case44_rc=$?
set -e
[ "$case44_rc" -ne 0 ] || fail_case "case 44 — a raw BYTE string (\`br#\"\`) in a declaration attribute passed GREEN; the r9 raw-string refusal only recognised \`r#\"\` and \`b\` is an identifier character, so the boundary anchor let it through; got: $(cat "$TMPROOT/case44.out")"
grep -qF "RAW STRING" "$TMPROOT/case44.out" \
  || fail_case "case 44 — the guard failed but NOT with the raw-string diagnostic; got: $(cat "$TMPROOT/case44.out")"
echo "OK (44): raw BYTE and raw C string prefixes (\`br#\"\`, \`cr#\"\`) are refused too, not just \`r#\"\`"

# ---------------------------------------------------------------------------
# 46. RED — `cfg_attr` must not propagate a CONDITIONAL exemption as unconditional.
#
#     `#[cfg_attr(any(), doc(hidden))] pub mod probe;` is neither hidden nor gated — the
#     predicate is FALSE — yet the inner `doc(hidden)` was propagated outward as an
#     unconditional HIDDEN, and HIDDEN is an EXEMPTING verdict, so the module file was
#     never read and an inner `#![cfg(...)]` passed undetected (roborev r11 F2).
#
#     The fix is OPEN, not a refusal: OPEN means "read the module file", which is
#     harmless for a genuinely-hidden module and correct for a self-gating one. Refusing
#     would red `#[cfg_attr(docsrs, doc(hidden))]`, a standard idiom, for no gain.
# ---------------------------------------------------------------------------
# NO python3 here (roborev r12 F3): this suite is documented and gated as needing no
# Python, and `set -e` would abort the whole "dependency-free" suite where it is absent.
# `scratch_tree` + printf does the same job with the shell already in use.
scratch_tree cfg-attr-conditional-hidden; wt46="$SCRATCH"
printf '\n#[cfg_attr(any(), doc(hidden))]\npub mod probe_oracle;\n' >>"$wt46/cqlite-core/src/lib.rs"
printf '#![cfg(feature = "benchmarks")]\n//! Self-test-only probe (#1712 r11 F2): inner-gated, must NOT be exempted.\npub fn probe() {}\n' >"$wt46/cqlite-core/src/probe_oracle.rs"
set +e
bash "$wt46/$GUARD_REL" >"$TMPROOT/case46.out" 2>&1
case46_rc=$?
set -e
[ "$case46_rc" -ne 0 ] || fail_case "case 46 — a FALSE-predicate \`cfg_attr(any(), doc(hidden))\` bought an exemption and the inner-gated module was never examined; got: $(cat "$TMPROOT/case46.out")"
grep -q "probe_oracle" "$TMPROOT/case46.out" \
  || fail_case "case 46 — the guard failed but never named \`probe_oracle\`; got: $(cat "$TMPROOT/case46.out")"
grep -q "INCONSISTENT" "$TMPROOT/case46.out" \
  || fail_case "case 46 — the conditional exemption was not resolved into the INCONSISTENT defect verdict; got: $(cat "$TMPROOT/case46.out")"
echo "OK (46): a conditional \`cfg_attr\` exemption never becomes an unconditional one — the module file is still read"

# ---------------------------------------------------------------------------
# 47. GREEN — the FALSE-FAIL control for the `path` refusal (roborev r11 F3).
#
#     THE FIRST FALSE FAIL ON THIS ISSUE, and it was in a fix added two rounds earlier:
#     the `path =` refusal ran BEFORE ordinary string contents were erased, so
#     `#[doc = "the path = ..."]` — an entirely ordinary doc attribute — read as a
#     `#[path]` attribute and FAILED THE FULL GATE.
#
#     Order is now load-bearing: raw-string refusal on RAW text (erasure cannot model
#     raw delimiters), then erase strings, then the structural `path` test. This case
#     pins the GREEN side; case 41 pins the RED side. Both are needed — a refusal that
#     reds correct code is the one agents learn to waive, and a waived refusal devalues
#     every other refusal in this guard.
# ---------------------------------------------------------------------------
scratch_tree path-in-string-cosmetic; wt47="$SCRATCH"
printf '\n#[doc = "the path = something cosmetic, and a stray r in for"]\npub mod probe_oracle;\n' >>"$wt47/cqlite-core/src/lib.rs"
printf '//! Self-test-only probe (#1712 r11 F3): ordinary prologue, must CERTIFY.\npub fn probe() {}\n' >"$wt47/cqlite-core/src/probe_oracle.rs"
set +e
bash "$wt47/$GUARD_REL" >"$TMPROOT/case47.out" 2>&1
case47_rc=$?
set -e
[ "$case47_rc" -eq 0 ] || fail_case "case 47 — a cosmetic \`#[doc = \"... path = ...\"]\` was REFUSED as if it were a \`#[path]\` attribute. That is a FALSE FAIL on ordinary code, which is the kind of refusal agents learn to waive; got: $(cat "$TMPROOT/case47.out")"
grep -qE "$MEASURED_RE" "$TMPROOT/case47.out" \
  || fail_case "case 47 — the guard exited 0 without its affirmative measurement line; got: $(cat "$TMPROOT/case47.out")"
echo "OK (47): a cosmetic string mentioning \`path =\` (or containing \`r\`) does NOT trip the \`#[path]\` or raw-string refusals"

# ---------------------------------------------------------------------------
# 48. GREEN+RED — STRING CONTENTS ARE NOT CODE (roborev r13 F2, and Refusal X narrowed).
#
#     `normalize()` used to copy string and RAW-string CONTENTS verbatim into the text
#     BOTH derivations read. Derivation S scans a line UNANCHORED while P is line-start
#     anchored, so a one-line `const X: &str = r#"pub mod fake;"#;` was found by S,
#     missed by P, and the cross-check called a DISAGREEMENT — the MANDATORY gate
#     REJECTING VALID RUST. A false FAIL.
#
#     (a)-(c) GREEN: literals containing declaration-like text, and a MULTI-LINE raw
#     string whose closing line is `"#;` — that last one also pins the Refusal X
#     narrowing, since X used to fire on ANY residue after a closing delimiter and `"#;`
#     is how every multi-line raw string ends.
#     (d) RED: `"#; pub mod x;` — a real declaration after a closing delimiter MUST still
#     refuse, or the narrowing would have bought the false PASS Refusal X exists to stop.
# ---------------------------------------------------------------------------
c48_i=0
for c48lit in 'const A: &str = r#"pub mod fake_a;"#;' 'const C: &str = "pub mod fake_c;";'; do
  c48_i=$((c48_i + 1))
  scratch_tree "literal-decl-text-$c48_i"; wt48="$SCRATCH"
  printf '\n%s\n' "$c48lit" >>"$wt48/cqlite-core/src/lib.rs"
  set +e
  bash "$wt48/$GUARD_REL" >"$TMPROOT/case48.out" 2>&1
  c48rc=$?
  set -e
  [ "$c48rc" -eq 0 ] || fail_case "case 48($c48_i) — a STRING LITERAL containing \`pub mod\` text was treated as a declaration, so the two scans disagreed and the mandatory gate REJECTED VALID RUST; got: $(cat "$TMPROOT/case48.out")"
done

scratch_tree literal-multiline-raw; wt48c="$SCRATCH"
printf '\nconst B: &str = r#"\npub mod fake_b;\n"#;\n' >>"$wt48c/cqlite-core/src/lib.rs"
set +e
bash "$wt48c/$GUARD_REL" >"$TMPROOT/case48c.out" 2>&1
c48c_rc=$?
set -e
[ "$c48c_rc" -eq 0 ] || fail_case "case 48(c) — a MULTI-LINE raw string was rejected. Its closing line is \`\"#;\`, and Refusal X used to fire on ANY residue after a closing delimiter, so every crate root containing one was refused; got: $(cat "$TMPROOT/case48c.out")"

scratch_tree literal-close-then-decl; wt48d="$SCRATCH"
printf '\nconst D: &str = r#"\nz\n"#; pub mod probe_x;\n' >>"$wt48d/cqlite-core/src/lib.rs"
printf '#![cfg(feature = "benchmarks")]\n//! inner-gated\npub fn p() {}\n' >"$wt48d/cqlite-core/src/probe_x.rs"
set +e
bash "$wt48d/$GUARD_REL" >"$TMPROOT/case48d.out" 2>&1
c48d_rc=$?
set -e
[ "$c48d_rc" -ne 0 ] || fail_case "case 48(d) — a real declaration AFTER a closing string delimiter on the same line passed GREEN. Narrowing Refusal X must not buy the false PASS it exists to stop; got: $(cat "$TMPROOT/case48d.out")"
grep -qF "code follows a closing" "$TMPROOT/case48d.out" \
  || fail_case "case 48(d) — refused, but not via Refusal X; got: $(cat "$TMPROOT/case48d.out")"
echo "OK (48): string and RAW-string CONTENTS are not read as code — literals with declaration text and multi-line raw strings CERTIFY, while a real declaration after a closing delimiter still REFUSES"

# ---------------------------------------------------------------------------
# 50. GREEN+RED — a ONE-LINE inline module is not a crate-root declaration.
#
#     `mod outer { pub mod inner; }` is ordinary Rust in which `inner` is NOT declared at
#     the crate root. Derivation S scans a line UNANCHORED, so it collected `inner`
#     anyway while P correctly ignored it, and the cross-check called a DISAGREEMENT —
#     the MANDATORY gate rejecting valid Rust. Case 24 already pinned this property for
#     the INDENTED multi-line form; the one-line form was uncovered. Found by probing.
#
#     S is now depth-aware WITHIN a line. That does not cost the cross-check its value:
#     S keeps its own collection RULE (an unanchored scan with no attribute parsing),
#     which is where its independence from P lives; counting braces is reliable because
#     `normalize()` blanks comments and string contents, and Refusal I already depends on
#     the same brace data.
#
#     (c)/(d) RED — the narrowing must not buy a false PASS: a genuine crate-root
#     declaration whose module file gates itself is still INCONSISTENT, and one placed
#     after a closing brace at depth 0 is still seen.
# ---------------------------------------------------------------------------
scratch_tree inline-module-one-line; wt50="$SCRATCH"
printf '\nmod probe_outer { pub mod probe_inner; }\n' >>"$wt50/cqlite-core/src/lib.rs"
set +e
bash "$wt50/$GUARD_REL" >"$TMPROOT/case50.out" 2>&1
c50rc=$?
set -e
[ "$c50rc" -eq 0 ] || fail_case "case 50 — a ONE-LINE inline module \`mod o { pub mod i; }\` was rejected. \`i\` is not a crate-root declaration; S collected it unanchored while P correctly ignored it, so the scans disagreed on valid Rust; got: $(cat "$TMPROOT/case50.out")"

scratch_tree inline-module-multiline; wt50b="$SCRATCH"
printf '\nmod probe_outer2 {\n    pub mod probe_inner2;\n}\n' >>"$wt50b/cqlite-core/src/lib.rs"
set +e
bash "$wt50b/$GUARD_REL" >"$TMPROOT/case50b.out" 2>&1
c50b_rc=$?
set -e
[ "$c50b_rc" -eq 0 ] || fail_case "case 50(b) — the MULTI-LINE nested form regressed (case 24's property); got: $(cat "$TMPROOT/case50b.out")"

oracle_tree inline-module-still-detects; wt50c="$SCRATCH"
printf '#![cfg(feature = "benchmarks")]\n//! inner-gated: must still be caught\npub fn probe() {}\n' >"$wt50c/cqlite-core/src/probe_oracle.rs"
set +e
bash "$wt50c/$GUARD_REL" >"$TMPROOT/case50c.out" 2>&1
c50c_rc=$?
set -e
[ "$c50c_rc" -ne 0 ] || fail_case "case 50(c) — making S depth-aware bought a FALSE PASS: a real crate-root declaration whose module file gates itself was certified; got: $(cat "$TMPROOT/case50c.out")"
grep -q "INCONSISTENT" "$TMPROOT/case50c.out" \
  || fail_case "case 50(c) — refused, but not as the INCONSISTENT defect; got: $(cat "$TMPROOT/case50c.out")"

scratch_tree inline-module-after-close; wt50d="$SCRATCH"
printf '\nmod probe_o4 {\n}\npub mod probe_after;\n' >>"$wt50d/cqlite-core/src/lib.rs"
printf '#![cfg(feature = "benchmarks")]\n//! inner-gated\npub fn p() {}\n' >"$wt50d/cqlite-core/src/probe_after.rs"
set +e
bash "$wt50d/$GUARD_REL" >"$TMPROOT/case50d.out" 2>&1
c50d_rc=$?
set -e
[ "$c50d_rc" -ne 0 ] || fail_case "case 50(d) — a declaration at depth 0 AFTER a closing brace was missed once S became depth-aware; got: $(cat "$TMPROOT/case50d.out")"
echo "OK (50): a one-line inline module is not read as a crate-root declaration, the multi-line form still is not, and a REAL crate-root declaration is still examined either side of a closing brace"

# ---------------------------------------------------------------------------
# 51. RED+GREEN — WHITESPACE IS LEGAL BETWEEN ATTRIBUTE TOKENS (roborev r15 F1).
#
#     rustc accepts `#! [cfg(feature = "x")]` — VERIFIED on rustc 1.98.0: it compiles and
#     the gate APPLIES (no symbol emitted). A contiguous `#![` test read it as a shebang
#     on line 1, or as the first item later, and CERTIFIED the module. MEASURED pre-fix:
#     `CLEAN 3`.
#
#     Only two `#` shapes are legal in a prologue and they mean OPPOSITE things, so both
#     directions are pinned: `#` ws* `!` ws* `[` is an INNER attribute (keep reading),
#     while `#` ws* `[` is an OUTER attribute and ENDS the prologue. Anything else
#     starting with `#` now REFUSES instead of being taken for prologue-end.
# ---------------------------------------------------------------------------
c51_i=0
for c51a in '#! [cfg(feature = "benchmarks")]' '#!	[cfg(feature = "benchmarks")]' '# ! [cfg(feature = "benchmarks")]'; do
  c51_i=$((c51_i + 1))
  oracle_tree "attr-ws-$c51_i"; wt51="$SCRATCH"
  printf '%s\n\n//! Self-test-only probe (#1712 r15 F1).\npub fn probe() {}\n' "$c51a" >"$wt51/cqlite-core/src/probe_oracle.rs"
  set +e
  bash "$wt51/$GUARD_REL" >"$TMPROOT/case51.out" 2>&1
  c51rc=$?
  set -e
  [ "$c51rc" -ne 0 ] || fail_case "case 51($c51_i) — an inner cfg written with whitespace between its tokens was CERTIFIED. rustc accepts it and the gate APPLIES, so the module is absent from the compiled crate; got: $(cat "$TMPROOT/case51.out")"
  grep -q "INCONSISTENT" "$TMPROOT/case51.out" \
    || fail_case "case 51($c51_i) — refused rather than reported as the INCONSISTENT defect. Whitespace between tokens is exactly modellable, so the verdict must be the NAMED defect with its hoist remedy; got: $(cat "$TMPROOT/case51.out")"
done

# GREEN: an OUTER attribute (with or without a space) ends the prologue and must certify.
c51_j=0
for c51b in '#[derive(Debug)]' '# [derive(Debug)]'; do
  c51_j=$((c51_j + 1))
  oracle_tree "attr-outer-$c51_j"; wt51b="$SCRATCH"
  printf '//! Self-test-only probe.\n%s\npub struct Probe;\n' "$c51b" >"$wt51b/cqlite-core/src/probe_oracle.rs"
  set +e
  bash "$wt51b/$GUARD_REL" >"$TMPROOT/case51b.out" 2>&1
  c51b_rc=$?
  set -e
  [ "$c51b_rc" -eq 0 ] || fail_case "case 51(b$c51_j) — an OUTER attribute \`$c51b\` was treated as unclassifiable instead of ending the prologue; got: $(cat "$TMPROOT/case51b.out")"
done
echo "OK (51): an inner cfg written with whitespace between its tokens is caught as the NAMED defect, while an outer attribute (spaced or not) still ends the prologue"

# ---------------------------------------------------------------------------
# 52. RED+GREEN — a DECLARATION SPLIT ACROSS LINES (roborev r15 F2, Refusal V).
#
#     `pub` NEWLINE `mod probe;` is valid Rust — verified, it compiles — and EVERY scan
#     here requires `pub` and `mod` on the SAME line, so the declaration was invisible to
#     both derivations AND to Refusal U: they AGREE while both are blind, and an
#     inner-gated module passed unchecked.
#
#     Tokenizing item declarations across newlines would be a second implementation of
#     Rust's item grammar — the class this guard exists to avoid — so a bare visibility
#     qualifier alone on a depth-0 line is refused BY SHAPE. It is unambiguous: nothing
#     but a split declaration looks like that.
# ---------------------------------------------------------------------------
c52_i=0
for c52 in 'pub' 'pub(crate)' 'pub(super)'; do
  c52_i=$((c52_i + 1))
  scratch_tree "split-decl-$c52_i"; wt52="$SCRATCH"
  printf '\n%s\nmod probe_split;\n' "$c52" >>"$wt52/cqlite-core/src/lib.rs"
  printf '#![cfg(feature = "benchmarks")]\n//! inner-gated\npub fn p() {}\n' >"$wt52/cqlite-core/src/probe_split.rs"
  set +e
  bash "$wt52/$GUARD_REL" >"$TMPROOT/case52.out" 2>&1
  c52rc=$?
  set -e
  [ "$c52rc" -ne 0 ] || fail_case "case 52($c52_i) — a declaration split as \`$c52\` NEWLINE \`mod probe_split;\` passed GREEN; it is invisible to both derivations and to Refusal U; got: $(cat "$TMPROOT/case52.out")"
  grep -qF "nothing but a visibility qualifier" "$TMPROOT/case52.out" \
    || fail_case "case 52($c52_i) — refused, but not via Refusal V; got: $(cat "$TMPROOT/case52.out")"
done

scratch_tree split-decl-control; wt52b="$SCRATCH"
printf '\npub fn probe_ok() {}\npub struct ProbeS;\n' >>"$wt52b/cqlite-core/src/lib.rs"
set +e
bash "$wt52b/$GUARD_REL" >"$TMPROOT/case52b.out" 2>&1
c52b_rc=$?
set -e
[ "$c52b_rc" -eq 0 ] || fail_case "case 52(b) — ordinary one-line \`pub\` items were refused as split declarations; got: $(cat "$TMPROOT/case52b.out")"

# (c) THE FAMILY, NOT THE SPELLING (roborev r16). Refusal V first matched only a line
#     consisting SOLELY of `pub`, so `#[allow(dead_code)] pub` followed by `mod probe;`
#     slipped through — the same "pattern narrower than the hole" shape as the macro
#     sweep's line-anchored version, written one round AFTER that lesson was recorded.
#     It now matches a line ENDING in a dangling visibility token.
c52_k=0
for c52c in '#[allow(dead_code)] pub' '#[allow(dead_code)] pub(crate)'; do
  c52_k=$((c52_k + 1))
  scratch_tree "split-decl-attr-$c52_k"; wt52c="$SCRATCH"
  printf '\n%s\nmod probe_split;\n' "$c52c" >>"$wt52c/cqlite-core/src/lib.rs"
  printf '#![cfg(feature = "benchmarks")]\n//! inner-gated\npub fn p() {}\n' >"$wt52c/cqlite-core/src/probe_split.rs"
  set +e
  bash "$wt52c/$GUARD_REL" >"$TMPROOT/case52c.out" 2>&1
  c52c_rc=$?
  set -e
  [ "$c52c_rc" -ne 0 ] || fail_case "case 52(c$c52_k) — \`$c52c\` followed by \`mod probe_split;\` passed GREEN: a TRAILING visibility token after a same-line attribute is the same split declaration; got: $(cat "$TMPROOT/case52c.out")"
  grep -qF "nothing but a visibility qualifier" "$TMPROOT/case52c.out" \
    || fail_case "case 52(c$c52_k) — refused, but not via Refusal V; got: $(cat "$TMPROOT/case52c.out")"
done

# (d) GREEN controls for the widened match: an identifier ENDING in "pub", and a comment
#     and a string literal ending in the word. Blanking makes the last two inert; the
#     first relies on the token boundary anchor.
scratch_tree split-decl-lookalikes; wt52d="$SCRATCH"
printf '\nconst REPUBLIC: u8 = 1;\nfn republic() {}\n// a note about pub\nconst SP: &str = "trailing pub";\n' >>"$wt52d/cqlite-core/src/lib.rs"
set +e
bash "$wt52d/$GUARD_REL" >"$TMPROOT/case52d.out" 2>&1
c52d_rc=$?
set -e
[ "$c52d_rc" -eq 0 ] || fail_case "case 52(d) — an identifier ending in \"pub\" (\`republic\`), a comment ending in \`pub\`, or a string ending in \`pub\` was refused as a split declaration; got: $(cat "$TMPROOT/case52d.out")"

# (e) CORROBORATION, NOT JUST A DANGLING `pub` (roborev r17). A macro TOKEN TREE can
#     legally contain a bare `pub` — `const S: &str = stringify!(\n    pub\n);` — and the
#     widened match refused it, failing the MANDATORY gate on valid Rust that declares no
#     module at all. The refusal now requires the NEXT non-blank in-code line to BEGIN a
#     module declaration: two adjacent facts, neither needing a parser.
#
#     This is Refusal V's second correction — too narrow in r16, too broad in r17 — the
#     same lifecycle the item-macro refusal ran before being removed. It is KEPT because
#     the split-declaration hole IS closable by a bounded LOCAL rule, whereas separating
#     an item macro from an expression macro provably required item boundaries.
scratch_tree split-decl-macro-tokentree; wt52e="$SCRATCH"
printf '\nconst SS: &str = stringify!(\n    pub\n);\n' >>"$wt52e/cqlite-core/src/lib.rs"
set +e
bash "$wt52e/$GUARD_REL" >"$TMPROOT/case52e.out" 2>&1
c52e_rc=$?
set -e
[ "$c52e_rc" -eq 0 ] || fail_case "case 52(e) — a macro TOKEN TREE containing a bare \`pub\` was refused as a split declaration. It declares no module; the mandatory gate must not red it; got: $(cat "$TMPROOT/case52e.out")"

scratch_tree split-decl-nonmod; wt52f="$SCRATCH"
printf '\npub\nfn probe_fn9() {}\n' >>"$wt52f/cqlite-core/src/lib.rs"
set +e
bash "$wt52f/$GUARD_REL" >"$TMPROOT/case52f.out" 2>&1
c52f_rc=$?
set -e
[ "$c52f_rc" -eq 0 ] || fail_case "case 52(f) — a dangling \`pub\` followed by a NON-module item was refused. Only a split MODULE declaration is this guard's business; got: $(cat "$TMPROOT/case52f.out")"

# and the corroboration must survive a blank line between the two halves
scratch_tree split-decl-blankline; wt52g="$SCRATCH"
printf '\npub\n\nmod probe_split;\n' >>"$wt52g/cqlite-core/src/lib.rs"
printf '#![cfg(feature = "benchmarks")]\n//! inner-gated\npub fn p() {}\n' >"$wt52g/cqlite-core/src/probe_split.rs"
set +e
bash "$wt52g/$GUARD_REL" >"$TMPROOT/case52g.out" 2>&1
c52g_rc=$?
set -e
[ "$c52g_rc" -ne 0 ] || fail_case "case 52(g) — a split declaration with a BLANK LINE between the halves passed GREEN; the corroboration lookahead must skip blank lines; got: $(cat "$TMPROOT/case52g.out")"

# (h) THE LOOKAHEAD MUST CROSS COMMENT-ONLY REGIONS (roborev r18 F2). It used to bail on
#     the first line not starting in ordinary code, so `pub`, a MULTI-LINE comment whose
#     closing delimiter sits on its own line, then `mod probe;` defeated it — missed by
#     both derivations AND every refusal. Safe to cross rather than model: normalize()
#     blanks comments and string contents, so comment-body, closing-delimiter and blank
#     lines are all indistinguishable whitespace in N[].
c52_m=0
for c52h in '/* a comment\n   spanning lines\n*/' '// a single-line note'; do
  c52_m=$((c52_m + 1))
  scratch_tree "split-decl-comment-gap-$c52_m"; wt52h="$SCRATCH"
  printf '\npub\n%b\nmod probe_split;\n' "$c52h" >>"$wt52h/cqlite-core/src/lib.rs"
  printf '#![cfg(feature = "benchmarks")]\n//! inner-gated\npub fn p() {}\n' >"$wt52h/cqlite-core/src/probe_split.rs"
  set +e
  bash "$wt52h/$GUARD_REL" >"$TMPROOT/case52h.out" 2>&1
  c52h_rc=$?
  set -e
  [ "$c52h_rc" -ne 0 ] || fail_case "case 52(h$c52_m) — a split declaration separated by a COMMENT passed GREEN; the corroboration lookahead must cross comment-only regions; got: $(cat "$TMPROOT/case52h.out")"
  grep -qF "split across lines" "$TMPROOT/case52h.out" \
    || fail_case "case 52(h$c52_m) — refused, but not via Refusal V; got: $(cat "$TMPROOT/case52h.out")"
done

# (i) THE `mod` IDENTIFIER MAY BE ON A LATER LINE STILL (roborev r19 F1): `pub` / `mod` /
#     `probe;` across THREE lines. Requiring `mod NAME` together missed it. Accepting a
#     bare `mod` is a SIMPLIFICATION — one fewer requirement — which is why this refusal
#     kept its bounded form instead of being deleted like the item-macro one.
c52_p=0
for c52i in 'pub\nmod\nprobe_split;' 'pub\n// note\nmod\nprobe_split;'; do
  c52_p=$((c52_p + 1))
  scratch_tree "split-decl-threeline-$c52_p"; wt52i="$SCRATCH"
  printf '\n%b\n' "$c52i" >>"$wt52i/cqlite-core/src/lib.rs"
  printf '#![cfg(feature = "benchmarks")]\n//! inner-gated\npub fn p() {}\n' >"$wt52i/cqlite-core/src/probe_split.rs"
  set +e
  bash "$wt52i/$GUARD_REL" >"$TMPROOT/case52i.out" 2>&1
  c52i_rc=$?
  set -e
  [ "$c52i_rc" -ne 0 ] || fail_case "case 52(i$c52_p) — a THREE-LINE split declaration passed GREEN; the \`mod\` identifier may sit on a later line still; got: $(cat "$TMPROOT/case52i.out")"
done
echo "OK (52): a split MODULE declaration REFUSES across two lines, THREE lines, blank lines and comments — while a macro token tree containing \`pub\`, a dangling \`pub\` before a NON-module item, one-line \`pub\` items and \`republic\` all stay GREEN"

# ---------------------------------------------------------------------------
# 54. GREEN — TWO FALSE FAILS THE MANDATORY GATE USED TO PRODUCE (roborev r19 F2/F3).
#
#     (a) An attribute run separated from its declaration by a MULTI-LINE COMMENT. The
#         collection loop skipped blanks only while INCODE was true, but normalize()
#         blanks comments to SPACES and a line inside a block comment carries INCODE 0 —
#         so it stopped at both, dropped the `#[cfg(...)]`, and recorded a CORRECTLY
#         GATED module as OPEN. The guard then ACCUSED that module of the AK1 defect.
#         That is the worst class of false FAIL: it indicts correct code by name.
#     (b) A macro token tree containing a COMPLETE declaration —
#         `swallow!( pub mod phantom; );` is valid Rust that emits no module. Brace depth
#         alone read it as crate-root, so S collected it and P did not.
#
#     (c) RED control — the AK1 defect must still be caught through a comment gap, or
#         these two fixes would have bought a false PASS.
# ---------------------------------------------------------------------------
# NO python3 (r12 F3 removed it from this suite and it must not come back):
# scratch_tree + printf, the same idiom every other case uses.
scratch_tree attr-comment-gap-gated; wt54="$SCRATCH"
printf '\n#[cfg(feature = "benchmarks")]\n/* a comment\n   spanning lines\n*/\npub mod probe_oracle;\n' >>"$wt54/cqlite-core/src/lib.rs"
printf '#![cfg(feature = "benchmarks")]\n//! correctly gated at BOTH sites\npub fn probe() {}\n' >"$wt54/cqlite-core/src/probe_oracle.rs"
set +e
bash "$wt54/$GUARD_REL" >"$TMPROOT/case54.out" 2>&1
c54rc=$?
set -e
[ "$c54rc" -eq 0 ] || fail_case "case 54(a) — a CORRECTLY GATED module whose \`#[cfg]\` is separated from its declaration by a multi-line comment was accused of the AK1 defect. The attribute run must survive comment-only regions; got: $(cat "$TMPROOT/case54.out")"

scratch_tree macro-tokentree-full-decl; wt54b="$SCRATCH"
printf '\nswallow!( pub mod phantom; );\n' >>"$wt54b/cqlite-core/src/lib.rs"
set +e
bash "$wt54b/$GUARD_REL" >"$TMPROOT/case54b.out" 2>&1
c54b_rc=$?
set -e
[ "$c54b_rc" -eq 0 ] || fail_case "case 54(b) — a macro token tree containing a complete \`pub mod phantom;\` was read as a crate-root declaration. It emits no module; paren depth must exclude it; got: $(cat "$TMPROOT/case54b.out")"

scratch_tree attr-comment-gap-still-detects; wt54c="$SCRATCH"
printf '\n#[allow(dead_code)]\n/* a comment\n   spanning lines\n*/\npub mod probe_gapped;\n' >>"$wt54c/cqlite-core/src/lib.rs"
printf '#![cfg(feature = "benchmarks")]\n//! inner-gated while the crate root is UNCONDITIONAL\npub fn p() {}\n' >"$wt54c/cqlite-core/src/probe_gapped.rs"
set +e
bash "$wt54c/$GUARD_REL" >"$TMPROOT/case54c.out" 2>&1
c54c_rc=$?
set -e
[ "$c54c_rc" -ne 0 ] || fail_case "case 54(c) — the r19 F2/F3 fixes bought a FALSE PASS: an unconditional declaration whose module file gates itself was certified; got: $(cat "$TMPROOT/case54c.out")"
grep -q "INCONSISTENT" "$TMPROOT/case54c.out" \
  || fail_case "case 54(c) — refused but not as the INCONSISTENT defect; got: $(cat "$TMPROOT/case54c.out")"
echo "OK (54): an attribute run survives a multi-line comment gap and a macro token tree is not a declaration — while the AK1 defect is still caught"


# ---------------------------------------------------------------------------
# 53. RED+GREEN — `# [path = "..."]` with whitespace between `#` and `[` (roborev r18 F1).
#
#     r15 canonicalised whitespace between attribute tokens in the PROLOGUE reader; the
#     CRATE-ROOT scanner kept the contiguous test, so `# [path = "actual.rs"]` before
#     `pub mod probe;` was DISCARDED — both scans agreed the module was OPEN and resolution
#     certified a clean standard-path DECOY while the real, self-gated file went unread.
#     One fix, two homes: the same lexical assumption lived in two scanners.
#
#     (b) GREEN — `# [derive(Debug)]` on an ordinary item must still certify.
# ---------------------------------------------------------------------------
scratch_tree attr-ws-path-decoy; wt53="$SCRATCH"
printf '\n# [path = "probe_actual.rs"]\npub mod probe_decoy;\n' >>"$wt53/cqlite-core/src/lib.rs"
printf '//! CLEAN DECOY at the standard path\npub fn p() {}\n' >"$wt53/cqlite-core/src/probe_decoy.rs"
printf '#![cfg(feature = "benchmarks")]\n//! the REAL module, and it gates itself\npub fn p() {}\n' >"$wt53/cqlite-core/src/probe_actual.rs"
set +e
bash "$wt53/$GUARD_REL" >"$TMPROOT/case53.out" 2>&1
c53rc=$?
set -e
[ "$c53rc" -ne 0 ] || fail_case "case 53 — a \`# [path = ...]\` attribute written with a space was discarded, so the guard certified a clean DECOY while the real module file gates itself; got: $(cat "$TMPROOT/case53.out")"
grep -qF "path" "$TMPROOT/case53.out" \
  || fail_case "case 53 — refused but never named the \`path\` attribute; got: $(cat "$TMPROOT/case53.out")"

scratch_tree attr-ws-outer-ok; wt53b="$SCRATCH"
printf '\n# [derive(Debug)]\npub struct ProbeSpaced;\n' >>"$wt53b/cqlite-core/src/lib.rs"
set +e
bash "$wt53b/$GUARD_REL" >"$TMPROOT/case53b.out" 2>&1
c53b_rc=$?
set -e
[ "$c53b_rc" -eq 0 ] || fail_case "case 53(b) — a spaced outer attribute \`# [derive(Debug)]\` on an ordinary item was rejected; got: $(cat "$TMPROOT/case53b.out")"
echo "OK (53): \`#\` and \`[\` separated by whitespace are canonicalised in the CRATE-ROOT scanner too, so a spaced \`# [path]\` cannot hide behind a decoy — while a spaced outer attribute on an ordinary item still certifies"


# ---------------------------------------------------------------------------
# 55. RED+GREEN — MULTILINE macro token trees, and INDENTED crate-root attributes
#     (roborev r20 F2 / F1).
#
#     F2 was a FALSE FAIL on the mandatory gate: paren depth was tracked only WITHIN a
#     line, so `swallow!(\n    pub mod phantom;\n);` — valid Rust emitting no module —
#     had its indented content read as an indented crate-root declaration and hit
#     Refusal I. normalize() now carries paren/bracket depth ACROSS lines, exactly as it
#     already carried brace depth, comment state and string state. Bracket-delimited
#     trees behave the same.
#
#     F1 was a FALSE PASS: both derivations skip indented lines, so an INDENTED
#     `#[path = "actual.rs"]` above a column-zero `pub mod probe;` was discarded, the
#     module read as attribute-free and OPEN, and resolution certified a clean
#     standard-path DECOY while the real self-gated file went unexamined. Refusal W
#     refuses an indented attribute at crate-root depth rather than teaching the
#     derivations to read one, which would put a second rule underneath their primary
#     collection rule — where a defect becomes a blind spot they SHARE.
# ---------------------------------------------------------------------------
c55_i=0
for c55 in 'swallow!(\n    pub mod phantom;\n);' 'swallow![\n    pub mod phantom;\n];' 'swallow!( pub mod phantom; );'; do
  c55_i=$((c55_i + 1))
  scratch_tree "tokentree-multiline-$c55_i"; wt55="$SCRATCH"
  printf '\n%b\n' "$c55" >>"$wt55/cqlite-core/src/lib.rs"
  set +e
  bash "$wt55/$GUARD_REL" >"$TMPROOT/case55.out" 2>&1
  c55rc=$?
  set -e
  [ "$c55rc" -eq 0 ] || fail_case "case 55($c55_i) — a macro token tree spanning lines was read as a crate-root declaration. It emits no module; delimiter depth must carry ACROSS lines; got: $(cat "$TMPROOT/case55.out")"
done

# F1's RED: an indented attribute at crate-root depth must REFUSE, and the decoy must not
# be certified.
scratch_tree indented-attr-path-decoy; wt55d="$SCRATCH"
printf '\n  #[path = "probe_actual.rs"]\npub mod probe_decoy;\n' >>"$wt55d/cqlite-core/src/lib.rs"
printf '//! CLEAN DECOY at the standard path\npub fn p() {}\n' >"$wt55d/cqlite-core/src/probe_decoy.rs"
printf '#![cfg(feature = "benchmarks")]\n//! the REAL module, and it gates itself\npub fn p() {}\n' >"$wt55d/cqlite-core/src/probe_actual.rs"
set +e
bash "$wt55d/$GUARD_REL" >"$TMPROOT/case55d.out" 2>&1
c55d_rc=$?
set -e
[ "$c55d_rc" -ne 0 ] || fail_case "case 55(d) — an INDENTED \`#[path]\` was discarded, so the guard certified a clean DECOY while the real module file gates itself; got: $(cat "$TMPROOT/case55d.out")"
grep -qF "INDENTED attribute" "$TMPROOT/case55d.out" \
  || fail_case "case 55(d) — refused, but not via Refusal W; got: $(cat "$TMPROOT/case55d.out")"

# GREEN scoping controls: Refusal W must not touch attributes below crate-root depth.
c55_j=0
for c55g in 'mod o10 {\n    #[allow(dead_code)]\n    pub fn q() {}\n}' 'pub struct S10 {\n    #[allow(dead_code)]\n    f: u8,\n}' 'swallow!(\n    #[path = "x.rs"]\n    pub mod phantom;\n);'; do
  c55_j=$((c55_j + 1))
  scratch_tree "indented-attr-scoped-$c55_j"; wt55g="$SCRATCH"
  printf '\n%b\n' "$c55g" >>"$wt55g/cqlite-core/src/lib.rs"
  set +e
  bash "$wt55g/$GUARD_REL" >"$TMPROOT/case55g.out" 2>&1
  c55g_rc=$?
  set -e
  [ "$c55g_rc" -eq 0 ] || fail_case "case 55(g$c55_j) — Refusal W fired on an indented attribute BELOW crate-root depth (inside a mod block, a struct, or a macro token tree). It is scoped to brace depth 0 AND delimiter depth 0; got: $(cat "$TMPROOT/case55g.out")"
done
echo "OK (55): macro token trees spanning lines are not declarations, and an INDENTED crate-root attribute REFUSES — while indented attributes inside mod blocks, structs and token trees stay GREEN"

# ---------------------------------------------------------------------------
# 56. RED+GREEN — an attribute AFTER other code on a line, and BRACKET token trees
#     (roborev r21 F1 / F2).
#
#     F1 was a FALSE PASS: the structured scan recognises an attribute only at a line's
#     START, so `const X: () = (); #[path = "actual.rs"]` above a column-zero
#     `pub mod probe;` DISCARDED the `#[path]` — both scans agreed `probe` was OPEN and
#     resolution certified a clean standard-path DECOY while the real self-gated module
#     went unexamined. Refusal Y refuses it, scoped by DELIMITER DEPTH AT THE ATTRIBUTE so
#     a parameter-list attribute and a token-tree attribute are untouched.
#
#     F2 was a FALSE FAIL, and it was an inconsistency of ours: normalize()'s CROSS-LINE
#     counter tracked `[`/`]` but derivation S's WITHIN-LINE walk did not, so a single-line
#     `swallow![ pub mod phantom; ];` was collected by S and not by P. Third time on this
#     issue that one fix needed two homes — and the second time the two homes were the
#     cross-line and within-line halves of the SAME counter.
# ---------------------------------------------------------------------------
scratch_tree attr-after-item-decoy; wt56="$SCRATCH"
printf '\nconst PROBE_XX: () = (); #[path = "probe_actual.rs"]\npub mod probe_decoy;\n' >>"$wt56/cqlite-core/src/lib.rs"
printf '//! CLEAN DECOY at the standard path\npub fn p() {}\n' >"$wt56/cqlite-core/src/probe_decoy.rs"
printf '#![cfg(feature = "benchmarks")]\n//! the REAL module, and it gates itself\npub fn p() {}\n' >"$wt56/cqlite-core/src/probe_actual.rs"
set +e
bash "$wt56/$GUARD_REL" >"$TMPROOT/case56.out" 2>&1
c56rc=$?
set -e
[ "$c56rc" -ne 0 ] || fail_case "case 56 — an attribute FOLLOWING other code on the same line was discarded, so the guard certified a clean DECOY while the real module gates itself; got: $(cat "$TMPROOT/case56.out")"
grep -qF "FOLLOWS other code" "$TMPROOT/case56.out" \
  || fail_case "case 56 — refused, but not via Refusal Y; got: $(cat "$TMPROOT/case56.out")"

# F2's GREEN: bracket-delimited token trees, single-line and multiline.
c56_i=0
for c56b in 'swallow![ pub mod phantom; ];' 'swallow![\n    pub mod phantom;\n];' 'swallow!( pub mod phantom; );'; do
  c56_i=$((c56_i + 1))
  scratch_tree "tokentree-bracket-$c56_i"; wt56b="$SCRATCH"
  printf '\n%b\n' "$c56b" >>"$wt56b/cqlite-core/src/lib.rs"
  set +e
  bash "$wt56b/$GUARD_REL" >"$TMPROOT/case56b.out" 2>&1
  c56b_rc=$?
  set -e
  [ "$c56b_rc" -eq 0 ] || fail_case "case 56(b$c56_i) — a bracket- or paren-delimited token tree was read as a crate-root declaration; the within-line and cross-line depth counters must agree on which delimiters they track; got: $(cat "$TMPROOT/case56b.out")"
done

# GREEN scoping controls for Refusal Y.
c56_j=0
for c56g in 'pub fn probe_pf(#[allow(unused)] a: u8) { let _ = a; }' 'swallow!(\n    #[path = "x.rs"]\n    pub mod phantom;\n);' 'const PROBE_YY: u8 = 1;'; do
  c56_j=$((c56_j + 1))
  scratch_tree "attr-after-item-scoped-$c56_j"; wt56g="$SCRATCH"
  printf '\n%b\n' "$c56g" >>"$wt56g/cqlite-core/src/lib.rs"
  set +e
  bash "$wt56g/$GUARD_REL" >"$TMPROOT/case56g.out" 2>&1
  c56g_rc=$?
  set -e
  [ "$c56g_rc" -eq 0 ] || fail_case "case 56(g$c56_j) — Refusal Y fired on an attribute at delimiter depth > 0 (a parameter list or a macro token tree) or on an ordinary item; it is scoped by depth AT THE ATTRIBUTE; got: $(cat "$TMPROOT/case56g.out")"
done
echo "OK (56): an attribute FOLLOWING other code on a line REFUSES, and bracket-delimited token trees are not declarations — while parameter-list attributes, token-tree attributes and ordinary items stay GREEN"

# ---------------------------------------------------------------------------
# 57. GREEN+RED — CRATE-ROOT DEPTH IS COMPUTED ONCE (roborev r22 F1/F2 + the sixth site).
#
#     r22's two findings were not about behaviour; both said "these two implementations do
#     not match". Five sites computed "is this position at crate-root depth?" with five
#     different answers, and two of them were wrong in opposite ways:
#       * Refusal Y tracked parens and brackets but NOT braces, so an attribute inside a
#         one-line nested module false-FAILed;
#       * Refusal U checked no delimiter depth at all, so `mod outer { pub mod inner {} }`
#         was refused as an unrecognised top-level form.
#     Both are valid Rust rejected by the MANDATORY gate.
#
#     `root_depth_at(i, pos)` now answers it once and every site calls it, deleting their
#     private counters. THE SWEEP THEN FOUND A SIXTH SITE nobody had reported — Refusal U's
#     own loop — which is the point: one implementation cannot disagree with itself.
#
#     (d)/(e) RED — the consolidation must not have bought a false PASS: Refusal U still
#     fires at real crate-root depth, and the AK1 defect is still caught.
# ---------------------------------------------------------------------------
c57_i=0
for c57 in 'mod probe_o57 { const X: () = (); #[allow(dead_code)] fn f() {} }' 'mod probe_outer57 { pub mod probe_inner57 {} }' 'mod probe_o58 {\n    pub mod probe_i58 {}\n}'; do
  c57_i=$((c57_i + 1))
  scratch_tree "root-depth-once-$c57_i"; wt57="$SCRATCH"
  printf '\n%b\n' "$c57" >>"$wt57/cqlite-core/src/lib.rs"
  set +e
  bash "$wt57/$GUARD_REL" >"$TMPROOT/case57.out" 2>&1
  c57rc=$?
  set -e
  [ "$c57rc" -eq 0 ] || fail_case "case 57($c57_i) — a construct at brace depth > 0 was treated as crate-root. Depth must be computed AT THE POSITION, by the shared helper, not per-site; got: $(cat "$TMPROOT/case57.out")"
done

# (d) RED — Refusal U must still fire on a REAL crate-root inline module.
scratch_tree root-depth-once-u-still-fires; wt57d="$SCRATCH"
printf '\npub mod probe_inline57 { #![cfg(feature = "benchmarks")] }\n' >>"$wt57d/cqlite-core/src/lib.rs"
set +e
bash "$wt57d/$GUARD_REL" >"$TMPROOT/case57d.out" 2>&1
c57d_rc=$?
set -e
[ "$c57d_rc" -ne 0 ] || fail_case "case 57(d) — routing Refusal U through the shared depth helper bought a FALSE PASS: a crate-root INLINE \`pub mod\` carrying its own inner cfg was certified; got: $(cat "$TMPROOT/case57d.out")"
grep -qF "unrecognized top-level" "$TMPROOT/case57d.out" \
  || fail_case "case 57(d) — refused, but not via Refusal U; got: $(cat "$TMPROOT/case57d.out")"

# (e) RED — and the AK1 defect itself.
oracle_tree root-depth-once-ak1; wt57e="$SCRATCH"
printf '#![cfg(feature = "benchmarks")]\n//! inner-gated\npub fn probe() {}\n' >"$wt57e/cqlite-core/src/probe_oracle.rs"
set +e
bash "$wt57e/$GUARD_REL" >"$TMPROOT/case57e.out" 2>&1
c57e_rc=$?
set -e
[ "$c57e_rc" -ne 0 ] || fail_case "case 57(e) — the consolidation bought a FALSE PASS on the AK1 defect itself; got: $(cat "$TMPROOT/case57e.out")"
grep -q "INCONSISTENT" "$TMPROOT/case57e.out" \
  || fail_case "case 57(e) — refused but not as the INCONSISTENT defect; got: $(cat "$TMPROOT/case57e.out")"
echo "OK (57): crate-root depth is computed ONCE and shared — nested modules and attributes below depth 0 certify, while Refusal U at real crate-root depth and the AK1 defect both still fire"

# ---------------------------------------------------------------------------
# 36. GREEN — THE POSITIVE CONTROL for 29-38.
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
c36_open="$(ps_measured_field 'case-36 unconditional count' "$TMPROOT/case36.out" '.*of which ([0-9]+) unconditional.*')"
c36_read="$(ps_measured_field 'case-36 prologues-read count' "$TMPROOT/case36.out" '.*; ([0-9]+) module-file prologues read.*')"
[ "$c36_open" -eq "$((base_open + 1))" ] \
  || fail_case "case 36 — the added unconditional declaration did not move the count ($base_open -> $c36_open), so \`probe_oracle\` was never examined and the green is vacuous"
[ "$c36_read" -eq "$c36_open" ] \
  || fail_case "case 36 — $c36_open unconditional declarations but only $c36_read prologues read; one was skipped"
echo "OK (36): an ordinary prologue with INERT inner attributes certifies normally, and the added module really was examined"
# ---------------------------------------------------------------------------
# 39. RED — ZERO unconditional declarations must FAIL, not pass quietly.
#
#     THE VACUOUS PASS THIS CLOSES. Every exemption in the assert (`GATED`, `HIDDEN`) is
#     a `continue`, so a crate root in which NO declaration is OPEN walks the whole loop,
#     examines nothing, reads no module file — and, without the backstop, prints a success
#     line and exits 0. That is a positive verdict from an ABSENT MEASUREMENT: the shape
#     CLAUDE.md names, and the shape this entire issue is about.
#
#     IT IS ALSO REACHABLE BY ACCIDENT, which is why it is PINNED rather than trusted: ONE
#     over-broad `#[doc(hidden)]` sweep over the crate root would silence the guard
#     completely and green the gate, WITH NO DIFF TO THE GUARD AT ALL. An unpinned backstop
#     is one refactor away from being deleted as dead code.
#
#     Here every crate-root `pub mod` is marked `#[doc(hidden)]` — the cheapest way to
#     empty the OPEN set without deleting anything.
# ---------------------------------------------------------------------------
scratch_tree zero-unconditional; wt39="$SCRATCH"
awk '
  /^pub mod [A-Za-z_][A-Za-z0-9_]*[[:space:]]*;/ { print "#[doc(hidden)]" }
  { print }
' "$wt39/cqlite-core/src/lib.rs" >"$wt39/lib.rs.hidden"
mv "$wt39/lib.rs.hidden" "$wt39/cqlite-core/src/lib.rs"
hidden39="$(grep -c '^#\[doc(hidden)\]$' "$wt39/cqlite-core/src/lib.rs")"
[ "$hidden39" -ge 10 ] \
  || fail_case "case 39 setup: only $hidden39 doc(hidden) markers were inserted, so the OPEN set is probably not empty and the case would prove nothing"
set +e
bash "$wt39/$GUARD_REL" >"$TMPROOT/case39.out" 2>&1
case39_rc=$?
set -e
[ "$case39_rc" -ne 0 ] || fail_case "case 39 — a crate root with ZERO unconditional declarations passed GREEN, so the assert reported success having examined nothing; got: $(cat "$TMPROOT/case39.out")"
grep -qF "NOT ONE of them is an unconditional" "$TMPROOT/case39.out" \
  || fail_case "case 39 — the guard failed but NOT with the zero-unconditional diagnostic, so it failed for some other reason; got: $(cat "$TMPROOT/case39.out")"
grep -qF "affirmative measurement" "$TMPROOT/case39.out" \
  || fail_case "case 39 — the diagnostic did not say WHY zero is a failure (a positive verdict requires an affirmative measurement), so it reads as an arbitrary refusal; got: $(cat "$TMPROOT/case39.out")"
echo "OK (39): a crate root with ZERO unconditional declarations FAILs — the assert never reports success having examined nothing"

echo ""
echo "PASS: test_pub_surface_guard.sh — all 42 cases (10 green, 30 reds, 1 usage, 1 kill-safety)"
