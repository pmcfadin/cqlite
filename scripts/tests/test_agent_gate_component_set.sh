#!/usr/bin/env bash
# Regression test for issue #3544: the agent-gate COMPONENT-SET pre-flight must detect a
# gate script that is BEHIND origin/main's component set — the skew that let PR #3467's
# gate report a true `31/31 nonpass=0` while being SILENT about 4 components #3403 had
# added 39 minutes earlier.
#
# POSITIVE CONTROL is the point of this file. A bare red is NOT evidence: an unrelated
# breakage produces an identical exit code and an identical `RESULT: FAIL` line, so every
# case below asserts the check NAMES the missing/failing symbol it is supposed to name.
# And every incident class is planted in a THROWAWAY git repo (never the live tree): a
# scratch checkout holding only a COPY of the gate, plus a LOCAL bare `origin` — which
# makes the fetch real (`git fetch` against a path remote) while needing no network.
#
# The classes covered, each in both directions where a direction exists:
#   1. baseline has a component the branch lacks, main NOT an ancestor  -> FAIL naming it
#   2. `git fetch` fails (unreachable origin)                           -> non-PASS naming the fetch
#   3. baseline `--list` broken / empty / non-component output          -> FAIL naming the derivation
#   4. deliberate removal (main IS an ancestor of HEAD)                 -> DECLARED, run NOT failed
#   5. no skew                                                          -> affirmative PASS + baseline sha
#   6. --lite with a real skew                                          -> line present, run NOT failed
#   7. the REAL full-gate emit path                                     -> FAIL block + exit 1, no cargo
#
# Run standalone:   bash scripts/tests/test_agent_gate_component_set.sh
# Or via the gate:  scripts/agent-gate.sh runs it inside the `tooling-tests` component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"

# Never inherit a caller's summary path / parent marker (#2751/#2874 discipline).
unset AGENT_GATE_SUMMARY_FILE
unset AGENT_GATE_PARENT_RUN_ID

PASS=0
FAIL=0
# Counters live in the CURRENT shell on purpose: an `ok`/`bad` call inside `( … )` or the
# right-hand side of a pipe increments a copy that dies with the subshell, and the suite
# then prints FAILs while reporting `failed: 0`.
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

# Scratch root VALIDATED before anything is built under it: this script runs without
# `errexit` (every case must run so one failure does not hide the rest), so an unchecked
# `mktemp -d` would leave $tmp EMPTY, every derived path would resolve under `/`, and the
# EXIT trap would `rm -rf ""`.
tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-cset.XXXXXX") || {
  echo "FATAL: mktemp -d failed; refusing to run with an unset scratch root" >&2; exit 1; }
if [ -z "$tmp" ] || [ ! -d "$tmp" ]; then
  echo "FATAL: mktemp -d produced no usable directory ('$tmp'); refusing to run" >&2; exit 1
fi
trap 'rm -rf "$tmp"' EXIT INT TERM

GIT_ID=(-c user.email=gate@example.invalid -c user.name=gate-selftest)

# ---------------------------------------------------------------------------
# Fixture builders.
#
# mkbaseline <name> <sed-program|-> -> echoes a BARE repo path serving as `origin`, whose
# `main` holds a copy of the real gate transformed by <sed-program> (`-` = verbatim).
# Copying ONLY the gate into <root>/scripts/ makes its `cd "$(dirname "$0")/.."` resolve
# REPO_ROOT to <root>, so every path the fixture touches stays inside this run's mktemp
# namespace.
# ---------------------------------------------------------------------------
mkbaseline() {
  local name="$1" prog="$2" work="$tmp/$1-src" bare="$tmp/$1.git"
  mkdir -p "$work/scripts"
  if [ "$prog" = - ]; then
    cp "$GATE" "$work/scripts/agent-gate.sh"
  else
    sed "$prog" "$GATE" >"$work/scripts/agent-gate.sh"
  fi
  printf 'baseline fixture\n' >"$work/README.md"
  git init -q --bare "$bare" >/dev/null 2>&1
  # Point the bare repo's HEAD at `main` BEFORE any clone: with `init.defaultBranch`
  # still `master` (the git default on many boxes) a clone of this bare repo checks out
  # NOTHING, and the fixture's next commit becomes an UNRELATED ROOT COMMIT — which
  # silently converts every `--from-origin` fixture into the BEHIND shape and would make
  # the DECLARED and no-skew cases pass for the wrong reason (observed, first run).
  git -C "$bare" symbolic-ref HEAD refs/heads/main >/dev/null 2>&1
  ( cd "$work" \
    && git init -q . \
    && git add -A \
    && git "${GIT_ID[@]}" commit -qm baseline \
    && git push -q "$bare" HEAD:refs/heads/main ) >/dev/null 2>&1 \
    || { echo "FATAL: could not build baseline fixture '$name'" >&2; exit 1; }
  printf '%s\n' "$bare"
}

# mkbranch <name> <origin-bare|-> <sed-program|-> [--from-origin]
#   -> echoes a working repo whose scripts/agent-gate.sh is the real gate transformed by
#      <sed-program>. Without --from-origin the repo has its OWN root commit (so
#      origin/main is NOT an ancestor of HEAD: the BEHIND shape). With it, the repo is
#      cloned from <origin-bare> first (so origin/main IS an ancestor: the DECLARED /
#      no-skew shape). `-` for <origin-bare> configures no remote at all.
mkbranch() {
  local name="$1" bare="$2" prog="$3" from_origin="${4:-}" root="$tmp/$1"
  if [ "$from_origin" = --from-origin ]; then
    git clone -q "$bare" "$root" >/dev/null 2>&1 \
      || { echo "FATAL: could not clone baseline for branch '$name'" >&2; exit 1; }
  else
    mkdir -p "$root/scripts"
    printf 'branch fixture\n' >"$root/README.md"
    ( cd "$root" && git init -q . ) >/dev/null 2>&1
    [ "$bare" = - ] || ( cd "$root" && git remote add origin "$bare" ) >/dev/null 2>&1
  fi
  mkdir -p "$root/scripts"
  if [ "$prog" = - ]; then
    cp "$GATE" "$root/scripts/agent-gate.sh"
  else
    sed "$prog" "$GATE" >"$root/scripts/agent-gate.sh"
  fi
  # A per-fixture marker so the branch commit ALWAYS has content: a `--from-origin`
  # fixture whose gate copy is byte-identical to the baseline's has an empty index, and
  # `git commit` would fail (it did) — leaving the fixture on the baseline commit and the
  # case passing/failing for a reason that has nothing to do with the component set.
  printf 'branch fixture %s\n' "$name" >"$root/.branch-marker"
  ( cd "$root" && git add -A && git "${GIT_ID[@]}" commit -qm branch ) >/dev/null 2>&1 \
    || { echo "FATAL: could not commit branch fixture '$name'" >&2; exit 1; }
  printf '%s\n' "$root"
}

# The transformation that makes a gate script's component set DIFFER: append a sentinel
# component to the COMPONENTS array. The name is deliberately distinctive so an assertion
# can require the check to NAME it (a bare red proves nothing).
SENTINEL=zz-baseline-only-3544
ADD_SENTINEL="s|^COMPONENTS=(file-size|COMPONENTS=($SENTINEL file-size|"

# hook <repo> [mode] -> stdout of the repo's own gate `--component-set-line` hook.
# Stderr is dropped: the hook's decision is on stdout, and the gate prints accelerator
# notices to stderr on every invocation.
hook() {
  local repo="$1" mode="${2:-full}"
  ( cd "$repo" && bash "$repo/scripts/agent-gate.sh" --component-set-line "$mode" 2>/dev/null )
}
field() { # field <name> <hook-output>
  printf '%s\n' "$2" | grep "^$1: " | sed "s/^$1: //"
}

# ---------------------------------------------------------------------------
# 0. Sanity: the sentinel transformation really changes the baseline's `--list`, and
#    `--list` does NOT run the pre-flight. Without this, every case below could be
#    passing for the wrong reason (a sed that matched nothing produces two identical
#    scripts and a PASS that means nothing).
# ---------------------------------------------------------------------------
base_ok=$(mkbaseline base-ok "$ADD_SENTINEL")
base_list=$( cd "$tmp/base-ok-src" && bash scripts/agent-gate.sh --list 2>/dev/null )
if grep -qx -- "$SENTINEL" <<<"$base_list" && [ "$(printf '%s\n' "$base_list" | wc -l)" -gt 30 ]; then
  ok "3544-fixture: the baseline fixture's --list really carries the sentinel component"
else
  bad "3544-fixture: the sentinel transformation did not change the baseline component set"
fi

# `--list` must exit at the arg-parse case, BEFORE the pre-flight — otherwise the
# baseline derivation would recurse (and, with an unreachable origin, could not answer at
# all). Proven where it is observable: a repo with a DEAD origin still lists fine.
dead_list_repo=$(mkbranch dead-list "$tmp/nonexistent-origin.git" - )
dl_out=$( cd "$dead_list_repo" && bash scripts/agent-gate.sh --list 2>/dev/null ); dl_rc=$?
if [ "$dl_rc" -eq 0 ] && [ "$(printf '%s\n' "$dl_out" | wc -l)" -gt 30 ] \
   && ! grep -q 'component-set' <<<"$dl_out"; then
  ok "3544-list-no-preflight: --list exits at arg parse (no fetch, no pre-flight, no recursion)"
else
  bad "3544-list-no-preflight: --list did not stay pre-flight-free (rc=$dl_rc)"
fi

# ---------------------------------------------------------------------------
# 1. BRANCH BEHIND: the baseline has a component this tree lacks, and origin/main is NOT
#    an ancestor of HEAD. Must FAIL and must NAME the missing component.
# ---------------------------------------------------------------------------
behind=$(mkbranch behind "$base_ok" - )
b_out=$(hook "$behind")
if [ "$(field VERDICT "$b_out")" = BEHIND ] \
   && [ "$(field ANCESTOR "$b_out")" = no ] \
   && grep -qw -- "$SENTINEL" <<<"$(field MISSING "$b_out")" \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$(field COMPONENT_SET_LINE "$b_out")" \
   && grep -q -- "$SENTINEL" <<<"$(field COMPONENT_SET_LINE "$b_out")"; then
  ok "3544-behind: a behind branch FAILs and the line NAMES the missing component"
else
  bad "3544-behind: expected VERDICT BEHIND + FAIL-CLOSED naming $SENTINEL"
  printf '%s\n' "$b_out"
fi

# The recorded baseline sha must be the ACTUAL origin/main tip — a verdict that does not
# name its baseline cannot be audited (#3544 amendment, requirement 3).
b_expect=$(git -C "$base_ok" rev-parse refs/heads/main)
if [ "$(field SHA "$b_out")" = "$b_expect" ] \
   && grep -q "origin/main $b_expect" <<<"$(field COMPONENT_SET_LINE "$b_out")"; then
  ok "3544-baseline-sha: the emitted line records the origin/main sha40 it compared against"
else
  bad "3544-baseline-sha: expected the line to name $b_expect (got '$(field SHA "$b_out")')"
fi

# ---------------------------------------------------------------------------
# 2. FETCH FAILURE. Two shapes: an origin that cannot be reached, and no origin at all.
#    Both are an explicit non-PASS NAMING the fetch/baseline — never a SKIP, never a pass.
#    A stale remote-tracking ref is exactly why the fetch is asserted (#3544 amendment).
# ---------------------------------------------------------------------------
dead=$(mkbranch dead "$tmp/nonexistent-origin.git" - )
d_out=$(hook "$dead")
d_line=$(field COMPONENT_SET_LINE "$d_out")
if [ "$(field VERDICT "$d_out")" = UNMEASURED ] \
   && [ "$(field KIND "$d_out")" = fetch-failed ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$d_line" \
   && grep -qi 'fetch' <<<"$d_line" \
   && ! grep -qi 'SKIP' <<<"$d_line"; then
  ok "3544-fetch-dead: an unreachable origin is a FAIL naming the fetch, not a skip"
else
  bad "3544-fetch-dead: expected UNMEASURED/fetch-failed naming the fetch"
  printf '%s\n' "$d_out"
fi

noremote=$(mkbranch noremote - - )
nr_out=$(hook "$noremote")
nr_line=$(field COMPONENT_SET_LINE "$nr_out")
if [ "$(field KIND "$nr_out")" = no-remote ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$nr_line" \
   && grep -q "origin" <<<"$nr_line" \
   && ! grep -qi 'SKIP' <<<"$nr_line"; then
  ok "3544-fetch-noremote: no 'origin' remote is a FAIL naming it, never a permissive pass"
else
  bad "3544-fetch-noremote: expected KIND no-remote + FAIL-CLOSED"
  printf '%s\n' "$nr_out"
fi

# ---------------------------------------------------------------------------
# 3. FAILED BASELINE DERIVATION. Three shapes, each a FAIL that NAMES the derivation —
#    never a fallback to an empty or assumed baseline, which would excuse every branch
#    (the vacuous pass this issue exists to close, inverted).
# ---------------------------------------------------------------------------
# 3a. the baseline script errors out before it can list anything
base_broken=$(mkbaseline base-broken '2i\
echo "baseline exploded" >\&2; exit 7')
broken=$(mkbranch broken "$base_broken" - )
br_out=$(hook "$broken")
br_line=$(field COMPONENT_SET_LINE "$br_out")
if [ "$(field VERDICT "$br_out")" = UNMEASURED ] \
   && [ "$(field KIND "$br_out")" = baseline-list-failed ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$br_line" \
   && grep -q -- '--list' <<<"$br_line"; then
  ok "3544-baseline-broken: a baseline whose --list exits non-zero FAILs, naming the derivation"
else
  bad "3544-baseline-broken: expected KIND baseline-list-failed naming --list"
  printf '%s\n' "$br_out"
fi

# 3b. the baseline lists NOTHING (an empty baseline must never be accepted)
base_empty=$(mkbaseline base-empty 's|^  --list) printf|  --list) : \&\& printf|; s|printf '"'"'%s\\n'"'"' "${COMPONENTS\[@\]}"; exit 0 ;;|exit 0 ;;|')
empty=$(mkbranch empty "$base_empty" - )
e_out=$(hook "$empty")
e_line=$(field COMPONENT_SET_LINE "$e_out")
if [ "$(field VERDICT "$e_out")" = UNMEASURED ] \
   && [ "$(field KIND "$e_out")" = baseline-list-empty ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$e_line" \
   && grep -q 'excuse' <<<"$e_line"; then
  ok "3544-baseline-empty: an EMPTY baseline is a FAIL, never a set that excuses the branch"
else
  bad "3544-baseline-empty: expected KIND baseline-list-empty"
  printf '%s\n' "$e_out"
fi

# 3c. the baseline prints something that is NOT a component name. A filter that skipped
#     unrecognised lines would silently SHRINK the baseline; the grammar is closed.
base_garbage=$(mkbaseline base-garbage 's|^  --list) printf|  --list) echo "Compiling cqlite v0.15.0"; printf|')
garbage=$(mkbranch garbage "$base_garbage" - )
g_out=$(hook "$garbage")
g_line=$(field COMPONENT_SET_LINE "$g_out")
if [ "$(field VERDICT "$g_out")" = UNMEASURED ] \
   && [ "$(field KIND "$g_out")" = baseline-list-garbage ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$g_line" \
   && grep -q 'Compiling' <<<"$g_line"; then
  ok "3544-baseline-garbage: a non-component line FAILs the derivation and is quoted back"
else
  bad "3544-baseline-garbage: expected KIND baseline-list-garbage quoting the offending line"
  printf '%s\n' "$g_out"
fi

# 3d. the baseline does not carry the gate script at all under scripts/
base_nofile=$(mkbaseline base-nofile - )
( cd "$tmp/base-nofile-src" && git rm -q scripts/agent-gate.sh \
  && git "${GIT_ID[@]}" commit -qm "drop the gate" \
  && git push -qf "$base_nofile" HEAD:refs/heads/main ) >/dev/null 2>&1
nofile=$(mkbranch nofile "$base_nofile" - )
nf_out=$(hook "$nofile")
if [ "$(field KIND "$nf_out")" = baseline-missing ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$(field COMPONENT_SET_LINE "$nf_out")" \
   && grep -q 'scripts/agent-gate.sh' <<<"$(field COMPONENT_SET_LINE "$nf_out")"; then
  ok "3544-baseline-missing: an absent baseline script FAILs, naming the path it looked for"
else
  bad "3544-baseline-missing: expected KIND baseline-missing naming scripts/agent-gate.sh"
  printf '%s\n' "$nf_out"
fi

# ---------------------------------------------------------------------------
# 4. DELIBERATE REMOVAL: components are missing AND origin/main IS an ancestor of HEAD,
#    so this branch removed them in its own diff. A loud DECLARED line, NOT a FAIL — a
#    guard that reds on correct input is the guard agents learn to waive.
# ---------------------------------------------------------------------------
base_rm=$(mkbaseline base-rm "$ADD_SENTINEL")
declared=$(mkbranch declared "$base_rm" - --from-origin)
dc_out=$(hook "$declared")
dc_line=$(field COMPONENT_SET_LINE "$dc_out")
if [ "$(field VERDICT "$dc_out")" = DECLARED ] \
   && [ "$(field ANCESTOR "$dc_out")" = yes ] \
   && grep -q 'DECLARED (#3544)' <<<"$dc_line" \
   && grep -q -- "$SENTINEL" <<<"$dc_line" \
   && ! grep -q 'FAIL-CLOSED' <<<"$dc_line"; then
  ok "3544-declared: a branch that REMOVES a component is DECLARED (named), not failed"
else
  bad "3544-declared: expected VERDICT DECLARED naming $SENTINEL, with no FAIL-CLOSED"
  printf '%s\n' "$dc_out"
fi

# The fixpoint: BOTH behind AND removing a component must FAIL as BEHIND first, and can
# only reach the DECLARED case after rebasing. Same tree as `declared`, except origin/main
# has moved on (a second baseline commit), so HEAD is no longer a descendant of its tip.
( cd "$tmp/base-rm-src" && printf 'moved on\n' >>README.md \
  && git "${GIT_ID[@]}" commit -qam "advance main" \
  && git push -q "$base_rm" HEAD:refs/heads/main ) >/dev/null 2>&1
fp_out=$(hook "$declared")
if [ "$(field VERDICT "$fp_out")" = BEHIND ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$(field COMPONENT_SET_LINE "$fp_out")"; then
  ok "3544-fixpoint: behind AND removing -> FAILs as BEHIND first (DECLARED only after rebase)"
else
  bad "3544-fixpoint: expected BEHIND to win over DECLARED (got '$(field VERDICT "$fp_out")')"
  printf '%s\n' "$fp_out"
fi

# ---------------------------------------------------------------------------
# 5. NO SKEW: an affirmative PASS line carrying the baseline sha and a real count. A
#    positive verdict requires an affirmative MEASUREMENT — never a bare `0`, and never
#    silence that a reader could mistake for a check that ran.
# ---------------------------------------------------------------------------
base_same=$(mkbaseline base-same - )
same=$(mkbranch same "$base_same" - --from-origin)
s_out=$(hook "$same")
s_line=$(field COMPONENT_SET_LINE "$s_out")
s_sha=$(git -C "$base_same" rev-parse refs/heads/main)
n_components=$( cd "$same" && bash scripts/agent-gate.sh --list 2>/dev/null | wc -l | tr -d ' ' )
if [ "$(field VERDICT "$s_out")" = PASS ] \
   && grep -q "^component-set: PASS ($n_components/$n_components vs origin/main $s_sha)$" <<<"$s_line"; then
  ok "3544-no-skew: an in-sync tree stamps an affirmative PASS naming its baseline sha"
else
  bad "3544-no-skew: expected 'component-set: PASS ($n_components/$n_components vs origin/main $s_sha)'"
  printf '%s\n' "$s_out"
fi

# A component the BRANCH adds and main lacks is NOT skew: this branch may be the one
# adding it. It must not produce a non-PASS.
extra=$(mkbranch extra "$base_same" "$ADD_SENTINEL" --from-origin)
x_out=$(hook "$extra")
if [ "$(field VERDICT "$x_out")" = PASS ] \
   && grep -qw -- "$SENTINEL" <<<"$(field EXTRA "$x_out")" \
   && grep -q 'branch-only, NOT skew' <<<"$(field COMPONENT_SET_LINE "$x_out")"; then
  ok "3544-branch-only: a component present here but not on main PASSes (recorded, not skew)"
else
  bad "3544-branch-only: expected PASS with $SENTINEL recorded as branch-only"
  printf '%s\n' "$x_out"
fi

# ---------------------------------------------------------------------------
# 6. LENIENCY: --lite and --only stamp the SAME line ADVISORY. --lite runs every fix
#    round and must not require the network to function, so a real skew (and a dead
#    origin) must be VISIBLE there without failing the run.
# ---------------------------------------------------------------------------
l_out=$(hook "$behind" lite)
l_line=$(field COMPONENT_SET_LINE "$l_out")
if [ "$(field STRICT "$l_out")" = no ] \
   && grep -q '^component-set: ADVISORY-BEHIND (#3544)' <<<"$l_line" \
   && grep -q -- "$SENTINEL" <<<"$l_line" \
   && grep -q -- '--lite is lenient' <<<"$l_line" \
   && ! grep -q 'FAIL-CLOSED' <<<"$l_line"; then
  ok "3544-lite-advisory: --lite stamps the skew ADVISORY (named) and does not fail on it"
else
  bad "3544-lite-advisory: expected an ADVISORY-BEHIND line naming $SENTINEL under --lite"
  printf '%s\n' "$l_out"
fi

lo_out=$(hook "$dead" lite)
if grep -q '^component-set: ADVISORY-UNMEASURED (#3544)' <<<"$(field COMPONENT_SET_LINE "$lo_out")" \
   && grep -q 'asserts NOTHING' <<<"$(field COMPONENT_SET_LINE "$lo_out")"; then
  ok "3544-lite-offline: an unfetchable baseline under --lite is ADVISORY-UNMEASURED, not a PASS"
else
  bad "3544-lite-offline: expected ADVISORY-UNMEASURED under --lite"
  printf '%s\n' "$lo_out"
fi

o_out=$(hook "$behind" only:fmt)
if [ "$(field STRICT "$o_out")" = no ] \
   && grep -q '^component-set: ADVISORY-BEHIND (#3544)' <<<"$(field COMPONENT_SET_LINE "$o_out")" \
   && grep -q -- '--only fmt is lenient' <<<"$(field COMPONENT_SET_LINE "$o_out")"; then
  ok "3544-only-advisory: --only stamps the same line ADVISORY"
else
  bad "3544-only-advisory: expected an ADVISORY-BEHIND line naming --only"
  printf '%s\n' "$o_out"
fi

# --delta is a CERTIFYING mode and stays STRICT (its block is recorded in a PR).
dm_out=$(hook "$behind" delta)
if [ "$(field STRICT "$dm_out")" = yes ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$(field COMPONENT_SET_LINE "$dm_out")"; then
  ok "3544-delta-strict: --delta is a certifying mode and fails closed on skew"
else
  bad "3544-delta-strict: expected --delta to stay strict"
  printf '%s\n' "$dm_out"
fi

# ---------------------------------------------------------------------------
# 7. THE REAL EMIT PATH, not just the hook. A behind tree running the FULL gate must exit
#    at the pre-flight with a FAIL SUMMARY carrying the `component-set:` line and the
#    rebase remedy — and must NOT reach any component (no cargo, no dataset preflight, no
#    #1825 slot). That last property is what keeps this case hermetic and fast.
# ---------------------------------------------------------------------------
sum="$tmp/full-summary.txt"
fout="$tmp/full.log"
( cd "$behind" && AGENT_GATE_SUMMARY_FILE="$sum" CQLITE_DATASETS_ROOT="$tmp/no-datasets" \
    bash scripts/agent-gate.sh >"$fout" 2>&1 ); frc=$?
if [ "$frc" -ne 0 ] \
   && grep -q '^RESULT: FAIL' "$sum" 2>/dev/null \
   && grep -q "^component-set: FAIL-CLOSED (#3544).*$SENTINEL" "$sum" 2>/dev/null \
   && grep -q '^preflight: FAIL (component-set skew' "$sum" 2>/dev/null \
   && grep -q 'git rebase origin/main' "$sum" 2>/dev/null; then
  ok "3544-full-emit: the FULL gate exits at the pre-flight with a FAIL block + the remedy"
else
  bad "3544-full-emit: expected a FAIL SUMMARY naming the skew and the rebase remedy (rc=$frc)"
  sed -n '1,40p' "$sum" 2>/dev/null
  tail -15 "$fout" 2>/dev/null
fi

# It must have stopped BEFORE any component ran: a pre-flight that lets file-size/fmt
# start has already spent the run it was supposed to refuse.
if ! grep -q '^file-size' "$sum" 2>/dev/null && ! grep -q '>>> \[file-size\]' "$fout" 2>/dev/null; then
  ok "3544-full-emit-early: no component ran (the pre-flight refused before file-size)"
else
  bad "3544-full-emit-early: a component ran despite the fail-closed pre-flight"
fi

# The in-sync tree must NOT be blocked by the pre-flight, and its emitted block must
# CARRY the PASS line — a guard that reds on correct input is the guard agents learn to
# waive. Driven through `--delta` rather than the full gate deliberately: --delta is the
# other CERTIFYING (strict) mode AND is exempt from the #1825 machine-wide slot cap, so
# this case cannot queue behind a real gate on the same box (it did, for 13 minutes,
# before this was restructured). The delta itself REFUSES (its anchor..HEAD diff touches
# scripts/agent-gate.sh, a production file) — which is the point: the refusal block is
# emitted before any executor, so the case stays hermetic while still proving the
# pre-flight let the run through and stamped its PASS line into a real block.
sum2="$tmp/delta-insync-summary.txt"
fout2="$tmp/delta-insync.log"
( cd "$same" && AGENT_GATE_SUMMARY_FILE="$sum2" CQLITE_DATASETS_ROOT="$tmp/no-datasets" \
    bash scripts/agent-gate.sh --delta HEAD~1 --anchor-run-id selftest >"$fout2" 2>&1 ) >/dev/null 2>&1
if grep -q "^component-set: PASS ($n_components/$n_components vs origin/main $s_sha)$" "$sum2" 2>/dev/null \
   && grep -q '^==== AGENT-GATE DELTA SUMMARY ====' "$sum2" 2>/dev/null \
   && ! grep -q 'component-set: FAIL-CLOSED' "$sum2" 2>/dev/null; then
  ok "3544-strict-inSync: an in-sync tree passes the strict pre-flight; its block carries the PASS line"
else
  bad "3544-strict-inSync: the in-sync tree did not get past the component-set pre-flight"
  sed -n '1,30p' "$sum2" 2>/dev/null
fi

# …and the same --delta on the BEHIND tree must be REFUSED BY THE PRE-FLIGHT, before the
# delta's own classification: --delta is a certifying mode, so it fails closed on skew.
sum3="$tmp/delta-behind-summary.txt"
( cd "$behind" && AGENT_GATE_SUMMARY_FILE="$sum3" CQLITE_DATASETS_ROOT="$tmp/no-datasets" \
    bash scripts/agent-gate.sh --delta HEAD --anchor-run-id selftest >/dev/null 2>&1 ); drc=$?
if [ "$drc" -ne 0 ] \
   && grep -q '^RESULT: FAIL' "$sum3" 2>/dev/null \
   && grep -q "^component-set: FAIL-CLOSED (#3544).*$SENTINEL" "$sum3" 2>/dev/null; then
  ok "3544-delta-emit: --delta on a behind tree emits a FAIL block naming the missing component"
else
  bad "3544-delta-emit: expected --delta to fail closed on the skew (rc=$drc)"
  sed -n '1,30p' "$sum3" 2>/dev/null
fi

# ---------------------------------------------------------------------------
# 8. NO OPT-OUT. The remedy (rebase) is universally available, so an escape hatch could
#    only buy a vacuous green. Structural, because the absence of a variable cannot be
#    observed behaviourally: assert no env read inside the pre-flight block gates it.
# ---------------------------------------------------------------------------
cs_block=$(awk '/^# ---- issue #3544: component-set skew pre-flight/,/^# ---- issue #2081:/' "$GATE")
if [ -n "$cs_block" ] \
   && ! grep -qE '\$\{(AGENT_GATE|CQLITE)_[A-Z_]*(SKIP|ALLOW|DISABLE|IGNORE)[A-Z_]*' <<<"$cs_block" \
   && ! grep -qE '\$\{[A-Z_]*COMPONENT_SET[A-Z_]*:-' <<<"$cs_block"; then
  ok "3544-no-optout: the pre-flight reads no skip/allow/disable env var (#3544 requirement 9)"
else
  bad "3544-no-optout: an env-var opt-out appeared in the component-set pre-flight"
fi

printf '\n%s\n' "----------------------------------------"
printf 'passed: %d  failed: %d\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
