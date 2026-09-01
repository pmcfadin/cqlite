#!/usr/bin/env bash
# test_dep_duplicates_ratchet.sh — self-test for the ADVISORY duplicate-dependency
# ratchet, scripts/ci/check-dep-duplicates.sh, and for its gate component
# `dep-duplicates` (issue #1700 AH7).
#
# THE POINT IS TO PROVE THE GUARD FIRES — and, for an ADVISORY guard, that it fires
# WITHOUT failing, which is a strictly harder property to pin than a red. A guard that
# may never fail is one wrong branch away from being a guard that never says anything,
# and "exit 0" is then indistinguishable between "measured, no increase" and "measured
# nothing at all". So every case here asserts the emitted TOKENS, not the exit status
# alone: a bare exit-code assertion passes on an unrelated silent abort, and for this
# script it would pass on the vacuous states the whole design exists to keep visible.
#
# THE SUBJECT IS SUBSTITUTED, NEVER SEAMED. The guard derives its workspace and its
# baseline path from its own location and takes no flag or environment variable for
# either (a guard whose invoker picks its subject can be pointed at a trivial subject and
# greened). So each planted case builds a SCRATCH TREE — `scripts/ci/` plus a stub
# `Cargo.toml` — copies the real guard into it, and puts a shim `cargo` on PATH that
# prints planted `cargo tree` output. Nothing in the guard knows it is under test.
#
# Cases (P = planted, L = live, G = gate component):
#   P1  no increase        -> exit 0, `verdict NO-INCREASE`, `0 INCREASE RECOGNISED`
#                             (affirmative — never a bare 0)
#   P2  a crate GREW       -> exit 0 (ADVISORY: non-failing), `ADVISORY-INCREASE`,
#                             names `foo(2->3)`
#   P3  a NEW duplicate    -> exit 0, `ADVISORY-INCREASE`, names `baz(2)`
#   P4  DECREASE           -> exit 0, `NO-INCREASE` + `RATCHET-LOOSE` (an improvement is
#                             an invitation to re-tighten, never a failure)
#   P5  COLOURED output    -> parses identically to P1 (#3400 colour immunity, the
#                             regression that reads as a clean pass in both directions)
#   P6  EMPTY output       -> a legitimate measurement of ZERO against a zero baseline,
#                             NOT conflated with an unparseable read
#   P7  unparseable output -> exit 3, `SKIP-UNMEASURABLE cause=unparseable-output`, and
#                             NO verdict line (a parser failure may not become a pass)
#   P8  cargo tree non-zero-> exit 3, `cause=cargo-tree-failed`
#   P9  cargo absent       -> exit 3, `cause=cargo-absent`
#   P10 baseline MISSING   -> exit 4, `cause=baseline-missing`
#   P11 baseline GARBAGE   -> exit 4, `cause=baseline-garbage`, five shapes: an unknown
#                             line, INCOHERENT totals (grammar-valid but arithmetically
#                             impossible), leading/trailing whitespace, a repeated crate,
#                             and a `crate x 1` (a duplicate needs 2)
#   P12 --regenerate       -> writes a baseline the checker then accepts, carrying its own
#                             regeneration command; and an INCREASED graph against that
#                             fresh baseline still reports ADVISORY-INCREASE (proving the
#                             file was written from the measurement, not a template)
#   P13 usage              -> an unrecognized argument exits 2
#   P14 no bounded probe   -> a PATH with no timeout(1) is exit 3,
#                             `cause=probe-unboundable`, and NO probe is claimed: a
#                             missing capability may not inherit the permissive branch,
#                             and the permissive branch is an unbounded cargo run
#   P15 the bound is HARD  -> a SIGTERM-IGNORING cargo is SIGKILLed at the bound (the
#                             `-k` half; `timeout <n>` alone would wait forever), reported
#                             exit 3 `cause=cargo-tree-failed` naming the SIGKILL
#   L1  the real tree      -> the committed guard + committed baseline agree; BOTH
#                             affirmative verdicts pass and a documented UNMEASURABLE is
#                             reported SKIPPED (see G3 for why)
#   G1  gate component     -> PLANTED, in a scratch worktree with the guard substituted:
#                             a clean measurement records PASS with the driver-named
#                             annotation and the affirmative line echoed (G1a); an
#                             ADVISORY-INCREASE records PASS too, naming the crates (G1b);
#                             a verdict with no `probe … INVOKED` line records the driver
#                             as NOT REACHED rather than claiming a cargo run (G1c)
#   G2  gate SKIP paths    -> same substitution: the component records SKIP for exit 3,
#                             for a zero exit with NO verdict line, and for an unexpected
#                             rc — never PASS. This is the vacuous-pass guard, and it is
#                             the reason the component may never fail: SKIP is the only
#                             way it can say "nothing was measured".
#   G3  the live component -> the committed component over the REAL workspace. The
#                             DETERMINISTIC assertions live in G1/G2 against planted
#                             input, deliberately: asserting them here made the suite —
#                             and therefore the FULL GATE, via tooling-tests — red on
#                             correct input, since a legitimate ADVISORY-INCREASE or a
#                             documented UNMEASURABLE are both correct behaviour. G3
#                             accepts either affirmative verdict, reports a live SKIP as
#                             SKIPPED, and still FAILS on a FAIL, a missing component line
#                             or a PASS with no echoed measurement.

set -uo pipefail

PASS=0
FAIL=0
ok()      { PASS=$((PASS + 1)); printf 'ok   - %s\n' "$*"; }
bad()     { FAIL=$((FAIL + 1)); printf 'FAIL - %s\n' "$*"; }
skipped() { printf 'skip - %s\n' "$*"; }

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GUARD_REL="scripts/ci/check-dep-duplicates.sh"
GUARD="$REPO_ROOT/$GUARD_REL"
BASELINE_REL="scripts/ci/dep-duplicates-baseline.txt"
GATE="$REPO_ROOT/scripts/agent-gate.sh"

[ -f "$GUARD" ] || { echo "FAIL - guard script not found at $GUARD"; exit 1; }

TMPROOT="$(mktemp -d "${TMPDIR:-/tmp}/dep-dup-selftest.XXXXXX")"
WORKTREES=()
cleanup() {
  local w
  for w in ${WORKTREES[@]+"${WORKTREES[@]}"}; do
    git -C "$REPO_ROOT" worktree remove --force "$w" >/dev/null 2>&1 || true
  done
  rm -rf "$TMPROOT"
  return 0
}
trap 'cleanup' EXIT
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM
trap 'cleanup; exit 129' HUP

# --- fixtures ---------------------------------------------------------------
# The BASELINE census these fixtures are built around: foo x2, bar x2 = 4 instances /
# 2 crates. Small on purpose — the parser's subject is the SHAPE of cargo tree output,
# and a 71-line fixture would pin nothing a 9-line one does not.
tree_baseline() {
  cat <<'EOF'
foo v1.0.0
├── x v0.1.0
└── y v0.2.0

foo v2.0.0
└── z v0.3.0

bar v0.1.0 (*)
bar v0.2.0
└── q v1.2.3
EOF
}
tree_grew() { tree_baseline; printf 'foo v3.0.0\n└── w v9.9.9\n'; }
tree_new_crate() { tree_baseline; printf '\nbaz v1.0.0\nbaz v2.0.0\n'; }
tree_smaller() { printf 'foo v1.0.0\n└── x v0.1.0\n\nfoo v2.0.0\n'; }
# Colour EXACTLY where cargo puts it — around the rendered entry — and colour survives
# redirection to a file, which is why an un-stripped parse reads zero instances here.
tree_coloured() {
  local e
  e=$(printf '\033')
  tree_baseline | sed -E "s/^([a-z]+) (v[0-9.]+)/${e}[1m\\1${e}[0m ${e}[32m\\2${e}[0m/"
}
# Content, but no COLUMN-ZERO `<name> v<version>` line: every line is an indented branch.
tree_unparseable() { printf '├── foo v1.0.0\n│   └── x v0.1.0\n    (nothing at column zero)\n'; }

baseline_matching() { printf 'instances 4\ncrates 2\ncrate foo 2\ncrate bar 2\n'; }

# new_tree <name> [baseline-writer]: a scratch tree holding a COPY of the real guard.
new_tree() {
  local d="$TMPROOT/$1"
  mkdir -p "$d/scripts/ci" "$d/bin"
  cp "$GUARD" "$d/$GUARD_REL"
  printf '[workspace]\nmembers = []\n' > "$d/Cargo.toml"
  if [ "${2:-}" = none ]; then :; else baseline_matching > "$d/$BASELINE_REL"; fi
  printf '%s' "$d"
}

# plant_cargo <dir> <rc> <tree-writer-name>: a shim `cargo` whose `tree` prints the
# planted output and exits <rc>. It is a SHIM, not a seam: the guard resolves cargo from
# PATH exactly as it does in production.
plant_cargo() {
  local d="$1" rc="$2" writer="$3"
  "$writer" > "$d/planted-tree.txt"
  cat > "$d/bin/cargo" <<EOF
#!/usr/bin/env bash
if [ "\${1:-}" = tree ]; then
  cat "$d/planted-tree.txt"
  [ "$rc" -eq 0 ] || echo "error: planted cargo failure" >&2
  exit $rc
fi
echo "shim cargo: unexpected subcommand \${1:-}" >&2
exit 97
EOF
  chmod +x "$d/bin/cargo"
}

# run_guard <dir> [args…]: run the scratch tree's own copy, with the shim first on PATH.
# Sets RC and OUT.
run_guard() {
  local d="$1"; shift
  OUT="$(PATH="$d/bin:$PATH" bash "$d/$GUARD_REL" "$@" 2>&1)"
  RC=$?
  return 0
}

has() { case "$OUT" in *"$1"*) return 0 ;; *) return 1 ;; esac; }

# assert_case <label> <expected-rc> <token…>: the shared shape — the exit status AND
# every token. Tokens, because an exit status alone cannot tell a measured verdict from
# a silent abort, which for an always-non-failing guard is the whole risk.
assert_case() {
  local label="$1" want_rc="$2"; shift 2
  local t missing=""
  [ "$RC" = "$want_rc" ] || missing="rc=$RC(want $want_rc)"
  for t in "$@"; do
    has "$t" || missing="$missing '$t'"
  done
  if [ -z "$missing" ]; then
    ok "$label"
  else
    bad "$label — missing:$missing; output was: $(printf '%s' "$OUT" | tr '\n' '|')"
  fi
}

# --- P1: no increase --------------------------------------------------------
d=$(new_tree p1); plant_cargo "$d" 0 tree_baseline; run_guard "$d"
assert_case "P1: a matching census exits 0 with an AFFIRMATIVE '0 INCREASE RECOGNISED' and verdict NO-INCREASE" \
  0 'MEASURED 4 duplicate instance(s) / 2 duplicated crate(s)' '0 INCREASE RECOGNISED' 'verdict NO-INCREASE (4/2 vs baseline 4/2)'
# The affirmative wording is the property, not decoration: a bare `0` in a gate log reads
# as a verified all-clear from a scan that may never have run.
case "$OUT" in
  *'ADVISORY-INCREASE'*) bad "P1: a clean census must not print the ADVISORY-INCREASE token" ;;
  *) ok "P1: a clean census does not print ADVISORY-INCREASE (the tokens are distinguishable)" ;;
esac
# The bound is STATED, on every measuring run: a bound nobody can see in a gate log is a
# bound a reader has to take on trust (and this suite's P15 is what proves it is enforced).
case "$OUT" in
  *'probe bound: '*'then SIGKILL after a further '*)
    ok "P1: the run STATES the bound it applied to the probe (tool, bound, hard-kill grace)" ;;
  *) bad "P1: no 'probe bound:' line — the applied bound is invisible in the gate log" ;;
esac

# --- P2: a crate grew ------------------------------------------------------
d=$(new_tree p2); plant_cargo "$d" 0 tree_grew; run_guard "$d"
assert_case "P2: a grown crate exits 0 (ADVISORY — never fails) and names foo(2->3) under ADVISORY-INCREASE" \
  0 'ADVISORY-INCREASE the duplicate census GREW: 5 instance(s) vs baseline 4 (delta +1)' \
  'ADVISORY-INCREASE crates that gained instances: foo(2->3)' \
  'verdict ADVISORY-INCREASE (5/2 vs baseline 4/2)' \
  'does NOT fail the gate'

# --- P3: a newly duplicated crate -----------------------------------------
d=$(new_tree p3); plant_cargo "$d" 0 tree_new_crate; run_guard "$d"
assert_case "P3: a newly duplicated crate exits 0 and is NAMED as newly duplicated" \
  0 'ADVISORY-INCREASE' 'crates newly duplicated: baz(2)' 'verdict ADVISORY-INCREASE (6/3 vs baseline 4/2)'

# --- P4: a decrease -------------------------------------------------------
d=$(new_tree p4); plant_cargo "$d" 0 tree_smaller; run_guard "$d"
assert_case "P4: an IMPROVED census is NO-INCREASE plus a RATCHET-LOOSE invitation to re-tighten, never a failure" \
  0 '0 INCREASE RECOGNISED' 'RATCHET-LOOSE the census IMPROVED (2/1 vs baseline 4/2)' 'verdict NO-INCREASE'

# --- P5: colour immunity (#3400) ------------------------------------------
d=$(new_tree p5); plant_cargo "$d" 0 tree_coloured; run_guard "$d"
assert_case "P5: ANSI-coloured cargo output parses to the same counts (#3400 — colour survives redirection to a file)" \
  0 'MEASURED 4 duplicate instance(s) / 2 duplicated crate(s)' 'verdict NO-INCREASE (4/2 vs baseline 4/2)'
# POSITIVE CONTROL for the control: the fixture must really carry escapes, or P5 passes
# for the wrong reason — the exact way a colour-immunity test goes inert.
if tree_coloured | grep -q "$(printf '\033')"; then
  ok "P5: the coloured fixture really contains ANSI escapes (so P5 cannot pass vacuously)"
else
  bad "P5: the coloured fixture carries NO escape bytes — the colour-immunity case is inert"
fi

# --- P6: empty output is a legitimate ZERO --------------------------------
d=$(new_tree p6); printf 'instances 0\ncrates 0\n' > "$d/$BASELINE_REL"
plant_cargo "$d" 0 true; run_guard "$d"
assert_case "P6: EMPTY cargo tree output is a measured ZERO against a zero baseline, not an unparseable read" \
  0 'MEASURED 0 duplicate instance(s) / 0 duplicated crate(s)' '0 INCREASE RECOGNISED' 'verdict NO-INCREASE (0/0 vs baseline 0/0)'

# --- P7: unparseable output -----------------------------------------------
d=$(new_tree p7); plant_cargo "$d" 0 tree_unparseable; run_guard "$d"
assert_case "P7: output with content but no recognisable duplicate line is UNMEASURABLE (exit 3), naming the cause" \
  3 'SKIP-UNMEASURABLE cause=unparseable-output' 'NOT a pass'
case "$OUT" in
  *'verdict '*) bad "P7: an unparseable read must not print a verdict at all" ;;
  *) ok "P7: an unparseable read prints NO verdict — a parser failure cannot become a pass" ;;
esac

# --- P8: cargo tree failed ------------------------------------------------
d=$(new_tree p8); plant_cargo "$d" 101 tree_baseline; run_guard "$d"
assert_case "P8: a non-zero cargo tree is UNMEASURABLE (exit 3), naming the cause and cargo's own first error line" \
  3 'probe cargo tree -d --workspace INVOKED (rc 101)' 'SKIP-UNMEASURABLE cause=cargo-tree-failed' 'planted cargo failure'

# --- P9: cargo absent -----------------------------------------------------
d=$(new_tree p9); plant_cargo "$d" 0 tree_baseline
if [ -n "$(PATH=/usr/bin:/bin type -P cargo || true)" ]; then
  skipped "P9: cargo is installed in /usr/bin or /bin on this host, so a cargo-less PATH cannot be constructed without losing the coreutils the guard needs"
else
  OUT="$(PATH=/usr/bin:/bin bash "$d/$GUARD_REL" 2>&1)"; RC=$?
  assert_case "P9: no cargo on PATH is UNMEASURABLE (exit 3) naming cargo-absent — never a pass" \
    3 'SKIP-UNMEASURABLE cause=cargo-absent' 'NOT a pass'
  case "$OUT" in
    *'INVOKED'*) bad "P9: with no cargo the guard must not claim a probe was INVOKED" ;;
    *) ok "P9: with no cargo the guard claims NO probe invocation (the reach signal is not fabricated)" ;;
  esac
fi

# --- P10: baseline missing ------------------------------------------------
d=$(new_tree p10 none); plant_cargo "$d" 0 tree_baseline; run_guard "$d"
assert_case "P10: a MISSING baseline is exit 4 naming baseline-missing, with the regeneration remedy" \
  4 'SKIP-BASELINE-UNUSABLE cause=baseline-missing' 'check-dep-duplicates.sh --regenerate'
case "$OUT" in
  *'verdict '*) bad "P10: with no baseline there is nothing to compare, so no verdict may be printed" ;;
  *) ok "P10: with no baseline NO verdict is printed" ;;
esac

# --- P11: baseline garbage, five shapes -----------------------------------
p11_case() { # <label> <baseline content> <expected detail token>
  local d2
  d2=$(new_tree "p11-$RANDOM$RANDOM")
  printf '%s' "$2" > "$d2/$BASELINE_REL"
  plant_cargo "$d2" 0 tree_baseline
  run_guard "$d2"
  assert_case "P11 ($1): exit 4, cause=baseline-garbage, naming the offence" \
    4 'SKIP-BASELINE-UNUSABLE cause=baseline-garbage' "$3"
}
p11_case "an unknown line" \
  'instances 4
crates 2
crate foo 2
crate bar 2
totally not a directive
' 'line 5 is not'
# GRAMMAR-VALID BUT ARITHMETICALLY IMPOSSIBLE: the shape check alone accepts this, and
# comparing against it would silently excuse real growth (the pub-surface coherence lesson).
p11_case "INCOHERENT totals" \
  'instances 4
crates 3
crate foo 2
crate bar 2
' 'INCOHERENT'
p11_case "leading whitespace (a parser that trims is a parser that guesses)" \
  'instances 4
crates 2
 crate foo 2
crate bar 2
' 'leading whitespace'
p11_case "a repeated crate line" \
  'instances 4
crates 2
crate foo 2
crate foo 2
' 'recorded twice'
p11_case "a crate recorded ONCE (a duplicate needs at least 2)" \
  'instances 4
crates 2
crate foo 1
crate bar 3
' 'a DUPLICATE needs at least 2'

# --- P12: --regenerate round trip ----------------------------------------
d=$(new_tree p12 none); plant_cargo "$d" 0 tree_grew
run_guard "$d" --regenerate
assert_case "P12a: --regenerate measures and publishes a baseline" \
  0 "REGENERATED $BASELINE_REL: 5 instance(s) / 2 crate(s)"
if grep -q 'check-dep-duplicates.sh --regenerate' "$d/$BASELINE_REL" \
   && grep -qx 'instances 5' "$d/$BASELINE_REL" \
   && grep -qx 'crate foo 3' "$d/$BASELINE_REL"; then
  ok "P12b: the regenerated baseline carries its own ONE regeneration command and the measured census"
else
  bad "P12b: the regenerated baseline is missing its regeneration command or its measured census"
fi
run_guard "$d"
assert_case "P12c: the checker ACCEPTS the file --regenerate wrote (the grammar is round-trippable)" \
  0 '0 INCREASE RECOGNISED' 'verdict NO-INCREASE (5/2 vs baseline 5/2)'
# …and the fresh baseline is a real measurement, not a template: growth beyond it is
# still reported. Without this, P12 would pass over a --regenerate that wrote constants.
plant_cargo "$d" 0 tree_new_crate; run_guard "$d"
assert_case "P12d: growth beyond the REGENERATED baseline is still ADVISORY-INCREASE (the file was written from the measurement)" \
  0 'ADVISORY-INCREASE' 'crates newly duplicated: baz(2)'

# --- P13: usage ----------------------------------------------------------
d=$(new_tree p13); plant_cargo "$d" 0 tree_baseline; run_guard "$d" --bogus
assert_case "P13: an unrecognized argument exits 2 (repo convention)" 2 "unrecognized argument '--bogus'"

# --- P14: the probe cannot be BOUNDED -------------------------------------
# A host with no usable `timeout` must NOT fall through to an unbounded `cargo tree`: a
# missing capability may not inherit the permissive branch, and the permissive branch
# here is an ADVISORY component that can hang the whole gate with no verdict at all.
# PATH is rebuilt from scratch with the handful of binaries the guard genuinely needs and
# NO timeout/gtimeout — the same substitution discipline as everywhere else in this file
# (no seam, no env var: the guard has none).
d=$(new_tree p14); plant_cargo "$d" 0 tree_baseline
mkdir -p "$d/minbin"
p14_missing=""
for _b in bash env mktemp rm sed awk mv dirname; do
  _p="$(type -P "$_b" || true)"
  if [ -n "$_p" ]; then ln -sf "$_p" "$d/minbin/$_b"; else p14_missing="$p14_missing $_b"; fi
done
ln -sf "$d/bin/cargo" "$d/minbin/cargo"
if [ -n "$p14_missing" ]; then
  skipped "P14: this host is missing$p14_missing, so a timeout-less PATH cannot be built without also removing what the guard needs"
elif [ -n "$(PATH="$d/minbin" type -P timeout || true)" ] || [ -n "$(PATH="$d/minbin" type -P gtimeout || true)" ]; then
  bad "P14: the constructed minimal PATH still resolves a timeout binary — the case would pass for the wrong reason"
else
  OUT="$(PATH="$d/minbin" bash "$d/$GUARD_REL" 2>&1)"; RC=$?
  assert_case "P14: a host where the probe cannot be BOUNDED is UNMEASURABLE (exit 3), naming probe-unboundable — never an unbounded run" \
    3 'SKIP-UNMEASURABLE cause=probe-unboundable' 'was NOT run rather than run unbounded' 'NOT a pass'
  case "$OUT" in
    *'INVOKED'*) bad "P14: with no way to bound the probe the guard must not claim a probe was INVOKED" ;;
    *)           ok "P14: with no way to bound the probe NO invocation is claimed (the reach signal is not fabricated)" ;;
  esac
fi

# --- P15: the bound is a HARD bound (SIGTERM alone is not one) -------------
# THE DEFECT THIS PINS: `timeout <n>` sends SIGTERM ONLY, so a cargo (or a child) that
# ignores it keeps running while `timeout` keeps WAITING — the bound is claimed and not
# enforced. `-k` makes it real. The planted cargo IGNORES SIGTERM, so this case can only
# pass if the hard kill actually happens.
#
# The 600s/30s constants are not settable by flag or environment (deliberately — no seam),
# so this case SUBSTITUTES THE ARTIFACT: it rewrites them in its OWN scratch copy of the
# guard and VERIFIES the rewrite took, because a silently-unapplied edit would make the
# case time out under its own outer bound and read as an unrelated failure.
d=$(new_tree p15)
sed -i.bak -e 's/^readonly PROBE_TIMEOUT_SECS=600$/readonly PROBE_TIMEOUT_SECS=1/' \
           -e 's/^readonly PROBE_KILL_GRACE_SECS=30$/readonly PROBE_KILL_GRACE_SECS=1/' \
           "$d/$GUARD_REL"
cat > "$d/bin/cargo" <<'SHIM'
#!/usr/bin/env bash
if [ "${1:-}" = tree ]; then
  # IGNORE SIGTERM, exactly the process this bound has to survive.
  trap '' TERM
  while [ "$SECONDS" -lt 25 ]; do sleep 1 2>/dev/null || :; done
  echo "shim cargo: outlived the bound — the hard kill did NOT happen"
  exit 0
fi
exit 97
SHIM
chmod +x "$d/bin/cargo"
if grep -qx 'readonly PROBE_TIMEOUT_SECS=1' "$d/$GUARD_REL" \
   && grep -qx 'readonly PROBE_KILL_GRACE_SECS=1' "$d/$GUARD_REL"; then
  ok "P15: the scratch copy's bound was really shortened (the substitution took, so P15 cannot pass or fail for the wrong reason)"
else
  bad "P15: could not shorten the bound in the scratch copy — the constants were renamed or reformatted"
fi
# An OUTER bound, not a wall-clock threshold assert: if the guard's own bound does NOT
# work the outer timeout kills it and rc is 124, which fails the assertion below with the
# guard's own claim visible. Nothing here depends on how long anything took.
OUT="$(PATH="$d/bin:$PATH" timeout 60 bash "$d/$GUARD_REL" 2>&1)"; RC=$?
assert_case "P15: a SIGTERM-IGNORING cargo is HARD-KILLED at the bound and reported UNMEASURABLE (exit 3), not waited on forever" \
  3 'probe bound:' 'INVOKED (rc 137)' 'SKIP-UNMEASURABLE cause=cargo-tree-failed' 'SIGKILL'
case "$OUT" in
  *'outlived the bound'*) bad "P15: the planted cargo ran to completion — the bound was not enforced" ;;
  *)                      ok "P15: the planted cargo did NOT outlive the bound" ;;
esac

# --- L1: the live tree ---------------------------------------------------
if [ -z "$(type -P cargo || true)" ]; then
  skipped "L1: no cargo on PATH — the live tree cannot be measured here"
elif [ ! -f "$REPO_ROOT/$BASELINE_REL" ]; then
  bad "L1: the committed baseline $BASELINE_REL is absent from this checkout"
else
  OUT="$(cd "$REPO_ROOT" && bash "$GUARD" 2>&1)"; RC=$?
  case "$RC:$OUT" in
    0:*'verdict NO-INCREASE'*)   ok "L1: the committed guard and the committed baseline agree on the real workspace (verdict NO-INCREASE)" ;;
    0:*'verdict ADVISORY-INCREASE'*) ok "L1: the committed guard measured the real workspace and reports ADVISORY-INCREASE (non-failing; the baseline wants regenerating)" ;;
    3:*)                         skipped "L1: the real workspace could not be measured here ($(printf '%s' "$OUT" | grep -o 'cause=[a-z-]*' | head -1))" ;;
    # DELIBERATELY NOT SKIPPED, and the asymmetry with G3 is the point: exit 4 means the
    # COMMITTED baseline in THIS checkout does not parse. That is a repository defect (a
    # truncation, a hand-edit, a bad merge), not a property of this host, and it is the one
    # thing L1 exists to catch — a suite that skipped here would let a corrupt baseline ride.
    # G3 accepts the same state as a SKIP because it asserts the COMPONENT'S MAPPING from
    # guard output to SUMMARY status, which is correct for every non-measuring cause.
    4:*)                         bad "L1: the COMMITTED baseline $BASELINE_REL does not parse: $(printf '%s' "$OUT" | grep -o 'cause=[a-z-]* detail=.*' | head -1)" ;;
    *)                           bad "L1: rc=$RC with no verdict line: $(printf '%s' "$OUT" | tr '\n' '|')" ;;
  esac
fi

# --- G/G2/G3: the gate component -----------------------------------------
# TWO KINDS OF CASE, AND THE SPLIT IS THE POINT (roborev finding on #1700).
#
# The component's contract has a DETERMINISTIC half (guard output -> SUMMARY status)
# and a HOST-DEPENDENT half (what the real workspace measures today). Asserting the
# deterministic half AGAINST THE LIVE WORKSPACE is what made this suite red on correct
# input: a legitimate `ADVISORY-INCREASE` failed the `0 INCREASE RECOGNISED` assertion,
# and a documented UNMEASURABLE (offline registry, no cargo, broken lockfile) failed the
# PASS assertion — and because this suite runs under `tooling-tests`, EITHER state failed
# the FULL GATE. That defeats the component's own advisory/SKIP contract through the back
# door and is exactly CLAUDE.md's "a lane that reds on correct input is the lane agents
# learn to waive".
#
#   G1a/G1b/G1c + G2a/G2b/G2c   PLANTED. The guard is SUBSTITUTED in a scratch worktree
#                               (the component has no seam either), so every mapping from
#                               guard output to SUMMARY status is pinned EXACTLY, with no
#                               dependence on what this box's dependency graph looks like.
#   G3                          LIVE. The real component over the real workspace. BOTH
#                               affirmative verdicts are correct component behaviour, so
#                               BOTH pass; a documented SKIP is reported SKIPPED, never a
#                               failure. It keeps an affirmative assertion — the SUMMARY
#                               line must exist and the guard's own measurement must be
#                               echoed — so it is not a tautology; only the set of
#                               CORRECT outcomes is widened.
if [ ! -f "$GATE" ]; then
  skipped "G1/G2/G3: scripts/agent-gate.sh is absent"
else
  wt="$TMPROOT/wt"
  if git -C "$REPO_ROOT" worktree add --detach "$wt" HEAD >"$TMPROOT/wt.log" 2>&1; then
    WORKTREES+=("$wt")
    # COMPONENT_LOG is set by the CALLER, not inside component_run: `line=$(component_run …)`
    # runs the function in a command-substitution SUBSHELL, so an assignment made in there
    # is discarded (it was, and the suite died on an unbound variable mid-run).
    COMPONENT_LOG=""
    component_run() { # <stub body> -> echoes the component's SUMMARY status; log -> $COMPONENT_LOG
      printf '%s\n' "$1" > "$wt/$GUARD_REL"
      chmod +x "$wt/$GUARD_REL"
      local s="${COMPONENT_LOG%.log}.summary.txt"
      ( cd "$wt" && AGENT_GATE_SUMMARY_FILE="$s" AGENT_GATE_ALLOW_MISSING_FIXTURES=1 \
          bash "$wt/scripts/agent-gate.sh" --only dep-duplicates ) >"$COMPONENT_LOG" 2>&1
      grep -E '^dep-duplicates: +(PASS|FAIL|SKIP)' "$s" 2>/dev/null | head -1
    }

    # G1a — THE CLEAN MEASUREMENT, planted. This is where the deterministic PASS
    # assertion belongs: the stub emits exactly what the real guard emits on a clean
    # tree, so PASS, the driver-named annotation and the echoed affirmative line are
    # asserted against an input this suite CONTROLS.
    COMPONENT_LOG="$TMPROOT/g1a.log"
    line=$(component_run '#!/usr/bin/env bash
echo "dep-duplicates: probe cargo tree -d --workspace INVOKED (rc 0)"
echo "dep-duplicates: MEASURED 4 duplicate instance(s) / 2 duplicated crate(s) from cargo tree -d --workspace"
echo "dep-duplicates: 0 INCREASE RECOGNISED — 4 duplicate instance(s) / 2 duplicated crate(s) vs baseline 4/2"
echo "dep-duplicates: verdict NO-INCREASE (4/2 vs baseline 4/2)"
exit 0')
    case "$line" in
      *PASS*) ok "G1a: a clean NO-INCREASE measurement is recorded PASS" ;;
      '')     bad "G1a: no dep-duplicates component line in the SUMMARY — the component did not run" ;;
      *)      bad "G1a: expected PASS, got: $line" ;;
    esac
    case "$line" in
      *UNDECLARED*|*UNCLASSIFIED*)
        bad "G1a: the feature-matrix annotation reads UNDECLARED/UNCLASSIFIED — the declared class does not match how cargo is really invoked (#3453): $line" ;;
      *'via check-dep-duplicates.sh'*'feature set NOT observed'*)
        ok "G1a: the annotation NAMES the driver and claims no feature set (#3453 indirect class, recorded from the guard's own probe line)" ;;
      *)  bad "G1a: unexpected annotation: $line" ;;
    esac
    if grep -q '0 INCREASE RECOGNISED' "$COMPONENT_LOG"; then
      ok "G1a: the component echoes the guard's AFFIRMATIVE measurement into the gate log (never a bare 0)"
    else
      bad "G1a: the component recorded a status without echoing the guard's affirmative measurement"
    fi

    # G1b — AN INCREASE IS STILL PASS, and LOUDLY. The other half of #1700 AC2: this
    # component emits no FAIL at all, so an increase must be recorded PASS with the
    # ADVISORY-INCREASE block echoed where a human will see it.
    COMPONENT_LOG="$TMPROOT/g1b.log"
    line=$(component_run '#!/usr/bin/env bash
echo "dep-duplicates: probe cargo tree -d --workspace INVOKED (rc 0)"
echo "dep-duplicates: MEASURED 6 duplicate instance(s) / 3 duplicated crate(s) from cargo tree -d --workspace"
echo "dep-duplicates: ADVISORY-INCREASE the duplicate census GREW: 6 instance(s) vs baseline 4 (delta +2), 3 crate(s) vs baseline 2 (delta +1)"
echo "dep-duplicates: ADVISORY-INCREASE crates newly duplicated: baz(2)"
echo "dep-duplicates: verdict ADVISORY-INCREASE (6/3 vs baseline 4/2)"
exit 0')
    case "$line" in
      *PASS*) ok "G1b: an ADVISORY-INCREASE is recorded PASS — this component emits no FAIL at all (#1700 AC2)" ;;
      *)      bad "G1b: an increase was recorded as '${line:-<no component line>}' instead of PASS" ;;
    esac
    if grep -q 'ADVISORY-INCREASE crates newly duplicated: baz(2)' "$COMPONENT_LOG"; then
      ok "G1b: the ADVISORY-INCREASE block is echoed into the gate log NAMING the crates responsible"
    else
      bad "G1b: an increase was recorded PASS without echoing the crates responsible — a silent advisory is no advisory"
    fi

    # G1c — REACH IS READ FROM THE PROBE LINE, NEVER FROM THE STATUS (#3453). A guard
    # that reached a verdict without printing `probe … INVOKED` must NOT have an
    # invocation attributed to it: claiming a cargo run that did not happen is the
    # defect the reach recording exists to fix.
    COMPONENT_LOG="$TMPROOT/g1c.log"
    line=$(component_run '#!/usr/bin/env bash
echo "dep-duplicates: MEASURED 4 duplicate instance(s) / 2 duplicated crate(s) from cargo tree -d --workspace"
echo "dep-duplicates: 0 INCREASE RECOGNISED — 4 duplicate instance(s) / 2 duplicated crate(s) vs baseline 4/2"
echo "dep-duplicates: verdict NO-INCREASE (4/2 vs baseline 4/2)"
exit 0')
    case "$line" in
      *'never reached'*) ok "G1c: with no probe line the annotation records the driver as NOT REACHED rather than claiming an unobserved cargo run (#3453)" ;;
      *)                 bad "G1c: a verdict with no probe line produced: ${line:-<no component line>}" ;;
    esac

    # G2 — THE VACUOUS-PASS GUARD. Three stubs, same scratch checkout.
    COMPONENT_LOG="$TMPROOT/g2a.log"
    line=$(component_run '#!/usr/bin/env bash
echo "dep-duplicates: SKIP-UNMEASURABLE cause=planted-cause detail=planted"
exit 3')
    case "$line" in
      *SKIP*) ok "G2a: an UNMEASURABLE guard (exit 3) is recorded SKIP, never PASS — a pass may not rest on an unmeasured state" ;;
      *)      bad "G2a: expected SKIP, got: ${line:-<no component line>}" ;;
    esac
    if grep -q 'cause=planted-cause' "$COMPONENT_LOG"; then
      ok "G2a: the component NAMES the guard's own cause rather than a generic SKIP"
    else
      bad "G2a: the guard's cause did not reach the component's output"
    fi
    COMPONENT_LOG="$TMPROOT/g2b.log"
    line=$(component_run '#!/usr/bin/env bash
echo "dep-duplicates: probe cargo tree -d --workspace INVOKED (rc 0)"
echo "dep-duplicates: MEASURED 0 duplicate instance(s) / 0 duplicated crate(s)"
exit 0')
    case "$line" in
      *SKIP*) ok "G2b: a zero exit with NO verdict line is recorded SKIP — the vacuous pass this component must never emit" ;;
      *)      bad "G2b: a verdict-less zero exit was recorded as '${line:-<no component line>}' instead of SKIP" ;;
    esac
    COMPONENT_LOG="$TMPROOT/g2c.log"
    line=$(component_run '#!/usr/bin/env bash
echo "not our prefix at all"
exit 55')
    case "$line" in
      *SKIP*) ok "G2c: an unexpected exit status is recorded SKIP (the component emits no FAIL at all, by mandate #1700 AC2)" ;;
      *)      bad "G2c: an unexpected rc was recorded as '${line:-<no component line>}' instead of SKIP" ;;
    esac
  else
    skipped "G1/G2: could not create a detached scratch worktree ($(tail -1 "$TMPROOT/wt.log" 2>/dev/null))"
  fi

  # G3 — THE LIVE COMPONENT. It still has to prove the COMMITTED component, the COMMITTED
  # guard and the COMMITTED baseline agree on a REAL tree — so the assertion is
  # affirmative — but every outcome the component is DESIGNED to produce here is accepted:
  #   PASS + NO-INCREASE       the ratchet holds
  #   PASS + ADVISORY-INCREASE the ratchet grew; ADVISORY, non-failing BY MANDATE
  #   SKIP + a named cause     nothing could be measured on this host (reported SKIPPED)
  # A FAIL, a missing component line, or a PASS with no echoed measurement remain
  # failures: those are states the component must never reach.
  if [ -z "$(type -P cargo || true)" ]; then
    skipped "G3: no cargo on PATH — the live component would SKIP for an unrelated reason"
  else
    g3_sum="$TMPROOT/g3-summary.txt"
    g3_log="$TMPROOT/g3.log"
    ( cd "$REPO_ROOT" && AGENT_GATE_SUMMARY_FILE="$g3_sum" AGENT_GATE_ALLOW_MISSING_FIXTURES=1 \
        bash "$GATE" --only dep-duplicates ) >"$g3_log" 2>&1
    g3_line="$(grep -E '^dep-duplicates: +(PASS|FAIL|SKIP)' "$g3_sum" 2>/dev/null | head -1)"
    case "$g3_line" in
      '')
        bad "G3: no dep-duplicates component line in the SUMMARY — the component did not run" ;;
      *FAIL*)
        bad "G3: the component recorded FAIL, which it may never do (#1700 AC2): $g3_line" ;;
      *SKIP*)
        skipped "G3: the live workspace could not be measured here ($(grep -o 'cause=[a-z-]*' "$g3_log" | head -1)) — a SKIP is a documented, correct outcome and not a failure" ;;
      *PASS*)
        if grep -qE 'dep-duplicates: (0 INCREASE RECOGNISED|ADVISORY-INCREASE)' "$g3_log"; then
          ok "G3: the live component records PASS and echoes an AFFIRMATIVE reading ($(grep -oE 'verdict (NO-INCREASE|ADVISORY-INCREASE)' "$g3_log" | head -1))"
        else
          bad "G3: PASS with NEITHER an affirmative '0 INCREASE RECOGNISED' nor an ADVISORY-INCREASE in the log — that is the vacuous pass this component must never emit"
        fi
        case "$g3_line" in
          *UNDECLARED*|*UNCLASSIFIED*)
            bad "G3: the live annotation reads UNDECLARED/UNCLASSIFIED — the declared class does not match how cargo is really invoked (#3453): $g3_line" ;;
          *'via check-dep-duplicates.sh'*'feature set NOT observed'*)
            ok "G3: the live annotation NAMES the driver and claims no feature set (#3453 indirect class)" ;;
          *)  bad "G3: unexpected live annotation: $g3_line" ;;
        esac
        ;;
      *)
        bad "G3: unrecognised component status: $g3_line" ;;
    esac
  fi
fi

echo
echo "dep-duplicates ratchet self-test: $PASS passed, $FAIL failed"
# A CASE FLOOR beside the tally (#3544's lesson): a span-replacing edit that deletes cases
# leaves a green "0 failed" over a shrunken suite, which certifies nothing. The floor sits
# below the leanest host (no cargo, no git worktree => L1/G1/G2/P9 skip) so it reds on a
# structural loss and never on a lean box.
CASE_FLOOR=22
if [ $((PASS + FAIL)) -lt "$CASE_FLOOR" ]; then
  printf 'FAIL - only %s verdicts were produced (floor %s): cases are being skipped or dying silently.\n' \
    "$((PASS + FAIL))" "$CASE_FLOOR" >&2
  exit 1
fi
[ "$FAIL" -eq 0 ] || exit 1
exit 0
