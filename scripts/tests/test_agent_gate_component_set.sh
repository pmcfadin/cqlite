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
# CENSUS, stated so a later reader can tell "covered" from "forgotten" (a silent gap is
# the shape this whole issue is about). The pre-flight has FOUR verdicts and TEN non-`ok`
# probe kinds, and EVERY one is exercised below:
#   verdicts (4) — PASS (case 5), DECLARED (4), BEHIND (1), INDETERMINATE (4c).
#   kinds   (10) — fetch-failed, no-remote (case 2); baseline-list-failed,
#                  baseline-list-empty, baseline-list-garbage, baseline-missing (3a–3d);
#                  no-git, baseline-workspace, no-tool (3f); unboundable (3g).
# TEN is the count of DISTINCT non-`ok` values assigned to `_CS_KIND` (twelve assignment
# SITES: `fetch-failed` is set from two places, and `ok` is the eleventh value). It was
# written as "six" for two rounds while the enumeration beneath it listed nine — a census
# that miscounts its own list is worse than none, because a reader who trusts the number
# and counts the entries concludes three kinds are uncovered extras. The count is now
# ASSERTED against the gate at run time (`3544-kind-census`, near the end of this file),
# so the two cannot drift again silently.
# None is declared unreachable. TWO cases are conditional — `no-tool` and `unboundable` —
# because each needs a curated PATH with a tool omitted (`git`, and `timeout`/`gtimeout`/
# `sleep` respectively). Each first verifies the SAME PATH can reach a real verdict (a
# positive control that must report `KIND: ok`) and, if it cannot on this host, prints a
# `skip -` naming that precondition rather than passing silently or reporting a false
# defect. Both absence branches are unreachable naturally on any box we develop on — this
# one has git, timeout and sleep — which is exactly why they are FORCED rather than awaited.
#
# The PASS/FAIL counters are incremented ONLY at top level — never inside `( … )` or the
# right-hand side of a pipe, either of which would increment a copy that dies with the
# subshell and leave the suite printing FAILs while reporting `failed: 0`. Verified
# EMPIRICALLY, not by reading: overriding `ok()` to call `bad()` makes all 33 cases print
# FAIL and the tally read `failed: 33` with exit 1 (a lexical paren scan cannot answer this
# — `case` patterns and same-line `( … )` closes defeat it, and the first attempt at one
# reported a bogus growing depth for every case in the file).
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

# mkbin <name> [tool-to-OMIT ...] -> echoes a curated tool directory holding symlinks to
# the host's real tools MINUS the named ones. Used to force capability-absence states
# (`no-tool`, `unboundable`) that no box we develop on can reach naturally — this one has
# `git`, `timeout` and `sleep`, so the absence branches are unreachable without this.
#
# Every case that uses it pairs it with a POSITIVE CONTROL: a curated PATH that cannot
# start the gate at all would make an absence case pass for the wrong reason, so the case
# first proves the SAME PATH plus the omitted tool reaches the expected non-absence answer.
mkbin() {
  local name="$1"; shift
  local dir="$tmp/$name-bin" t src omit=" $* "
  mkdir -p "$dir"
  for t in bash sh sed awk grep cut tr mktemp date basename dirname cat head tail wc sort \
           uniq rm mkdir cp mv ln uname nproc env find touch stat comm od xargs kill ps df \
           readlink id iconv git sleep timeout gtimeout nice; do
    case "$omit" in *" $t "*) continue ;; esac
    src=$(command -v "$t" 2>/dev/null) && [ -n "$src" ] && ln -sf "$src" "$dir/$t"
  done
  printf '%s\n' "$dir"
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
# 3f. THE REMAINING UNMEASURED KINDS. `_component_set_probe` has TEN non-`ok` kinds; 3a–3d
#     cover the three baseline-derivation ones plus `baseline-missing` (4), and case 2
#     covers `fetch-failed`/`no-remote` (2). These three (3) plus `unboundable` in 3g (1)
#     complete 10, so no fail-closed branch of the pre-flight is untested in either
#     direction. The count is machine-checked at the end of this file, so a new kind cannot
#     land without this prose being re-read. Nothing here is declared
#     unreachable: all three are reachable in a throwaway tree, which is why they are
#     asserted rather than excused.
# ---------------------------------------------------------------------------
# 3f-i. no-git: a checkout that is not a git worktree at all (a tarball export). The
#       run cannot obtain a baseline, so the certifying modes FAIL — the tree-integrity
#       guard already SKIPs there, and a SKIP here would be the vacuous pass.
nogit="$tmp/nogit-tree"
mkdir -p "$nogit/scripts"
cp "$GATE" "$nogit/scripts/agent-gate.sh"
ng_out=$(hook "$nogit")
if [ "$(field VERDICT "$ng_out")" = UNMEASURED ] \
   && [ "$(field KIND "$ng_out")" = no-git ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$(field COMPONENT_SET_LINE "$ng_out")" \
   && grep -qF "$nogit" <<<"$(field COMPONENT_SET_LINE "$ng_out")"; then
  ok "3544-no-git: a non-git checkout FAILs, naming the directory that is not a worktree"
else
  bad "3544-no-git: expected KIND no-git naming $nogit"
  printf '%s\n' "$ng_out"
fi

# 3f-ii. baseline-workspace: the scratch dir for extracting the baseline script cannot be
#        created. Forced with a TARGETED `mktemp` stub that fails ONLY for this
#        pre-flight's own template (the gate's other mktemp calls — its LOG_DIR — must keep
#        working, or the run would die before reaching the branch under test).
mt_repo=$(mkbranch mtfail "$base_ok" - )
mt_stub="$tmp/mtfail-stub"
mkdir -p "$mt_stub"
{ printf '#!/bin/sh\n'
  printf 'case "$*" in *agent-gate-cs*) exit 1 ;; esac\n'
  printf 'exec %s "$@"\n' "$(command -v mktemp)"
} >"$mt_stub/mktemp"
chmod +x "$mt_stub/mktemp"
mt_out=$( cd "$mt_repo" && PATH="$mt_stub:$PATH" bash "$mt_repo/scripts/agent-gate.sh" \
            --component-set-line full 2>/dev/null )
if [ "$(field VERDICT "$mt_out")" = UNMEASURED ] \
   && [ "$(field KIND "$mt_out")" = baseline-workspace ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$(field COMPONENT_SET_LINE "$mt_out")" \
   && grep -q 'temp dir' <<<"$(field COMPONENT_SET_LINE "$mt_out")"; then
  ok "3544-baseline-workspace: an uncreatable scratch dir FAILs, naming the extraction"
else
  bad "3544-baseline-workspace: expected KIND baseline-workspace"
  printf '%s\n' "$mt_out"
fi

# 3f-iii. no-tool: `git` absent from PATH entirely. Driven over a CURATED tool directory,
#         which needs a PRECONDITION: if that directory is not sufficient to start the
#         gate on this host, the case cannot distinguish "no-tool was misreported" from
#         "the stub PATH was too thin", so it is SKIPPED with that named cause rather than
#         reported as a defect. The precondition is measured AFFIRMATIVELY — the same PATH
#         *with* git linked in must reach `KIND: ok` — never assumed from the tool list.
nt_bin="$tmp/notool-bin"
mkdir -p "$nt_bin"
for _t in bash sh sed awk grep cut tr mktemp date basename dirname cat head tail wc sort \
          uniq rm mkdir cp mv ln uname nproc env find touch stat comm od xargs sleep kill \
          ps df readlink id iconv timeout nice; do
  _src=$(command -v "$_t" 2>/dev/null) && [ -n "$_src" ] && ln -sf "$_src" "$nt_bin/$_t"
done
nt_repo=$(mkbranch notool "$base_ok" - )
ln -sf "$(command -v git)" "$nt_bin/git"
nt_control=$( cd "$nt_repo" && PATH="$nt_bin" bash "$nt_repo/scripts/agent-gate.sh" \
                --component-set-line full 2>/dev/null )
rm -f "$nt_bin/git"
if [ "$(field KIND "$nt_control")" != ok ]; then
  echo "skip - 3544-no-tool: the curated tool PATH cannot start the gate on this host (control KIND='$(field KIND "$nt_control")') — the no-tool branch is not exercisable here"
else
  nt_out=$( cd "$nt_repo" && PATH="$nt_bin" bash "$nt_repo/scripts/agent-gate.sh" \
              --component-set-line full 2>/dev/null )
  if [ "$(field VERDICT "$nt_out")" = UNMEASURED ] \
     && [ "$(field KIND "$nt_out")" = no-tool ] \
     && grep -q 'FAIL-CLOSED (#3544)' <<<"$(field COMPONENT_SET_LINE "$nt_out")" \
     && grep -q 'git is not on PATH' <<<"$(field COMPONENT_SET_LINE "$nt_out")"; then
    ok "3544-no-tool: an absent git FAILs, naming the missing tool (control reached KIND ok)"
  else
    bad "3544-no-tool: expected KIND no-tool naming git (control reached KIND ok, so the PATH is sufficient)"
    printf '%s\n' "$nt_out"
  fi
fi

# ---------------------------------------------------------------------------
# 3g. THE PROBE IS ALWAYS BOUNDED — and when it cannot be, it is NOT RUN (roborev job 207,
#     Medium). The pre-flight used to fall back to `else shift; "$@"` when neither `timeout`
#     nor `gtimeout` was on PATH: a missing CAPABILITY inheriting the PERMISSIVE branch, the
#     same error as deriving a pass from the absence of a bad signal. On a default macOS box
#     that meant a hung fetch or an auth prompt could stall `--lite` INDEFINITELY, in the
#     mode that runs every fix round — and a wedged `--lite` is how a worker gets
#     stall-watchdog-killed.
#
#     NOTHING ON THIS BOX WOULD EVER HAVE CAUGHT IT: it has `timeout`, so the fallback is
#     dead code here. Hence every case below FORCES the absence with a curated PATH and asks
#     the code what it DECIDED, rather than hoping a host lacks a tool.
#
#     No timing is asserted anywhere here (a wall-clock threshold in a correctness test is
#     itself a defect class, #2642): the evidence is the returned STATUS, whether the child
#     is still running, and whether the command ran at all.
# ---------------------------------------------------------------------------
bound_of() { # bound_of <PATH> -> the mechanism token the gate reports under that PATH
  ( cd "$behind" && PATH="$1" bash "$behind/scripts/agent-gate.sh" --component-set-bound 2>/dev/null ) \
    | sed -n 's/^MECHANISM: //p'
}
bin_no_timeout=$(mkbin nowd timeout gtimeout)          # watchdog territory
bin_no_bound=$(mkbin nobound timeout gtimeout sleep)   # nothing can bound
host_mech=$(bound_of "$PATH")
wd_mech=$(bound_of "$bin_no_timeout")
none_mech=$(bound_of "$bin_no_bound")
case "$host_mech" in timeout|gtimeout|bash-watchdog) host_ok=1 ;; *) host_ok=0 ;; esac
if [ "$host_ok" -eq 1 ] && [ "$wd_mech" = bash-watchdog ] && [ "$none_mech" = none ]; then
  ok "3544-bound-mechanism: the bound is NAMED per host capability (here '$host_mech'; no timeout -> bash-watchdog; no sleep -> none) — never 'unbounded'"
else
  bad "3544-bound-mechanism: expected host in {timeout,gtimeout,bash-watchdog} (got '$host_mech'), bash-watchdog without timeout (got '$wd_mech'), none without sleep (got '$none_mech')"
fi

# The pure-bash watchdog must actually BOUND a hanging command and leave no live child.
# Liveness is judged by whether the child is still WORKING (its output file keeps growing),
# not by elapsed time: a `sleep` here only SEQUENCES the two observations.
tick="$tmp/watchdog-tick.txt"
ticker="$tmp/watchdog-ticker.sh"
{ printf '#!/bin/sh\n'; printf 'while : ; do echo tick >> "%s"; sleep 1; done\n' "$tick"; } >"$ticker"
chmod +x "$ticker"
: >"$tick"
# The invocation carries an OUTER host bound. Without it, a gate that fails to bound would
# HANG this suite instead of failing it — the assert would be right and useless, since a
# hung run reports nothing (RED-verified: a mutation that never fires the deadline hung the
# whole file). The outer bound is a HARNESS guard, not an assertion: nothing below compares
# elapsed time, and if the host has no `timeout` the sub-assert says so rather than relying
# on the mechanism under test to bound its own test.
# ABSOLUTE path, resolved from the host PATH BEFORE the override: the curated PATH used
# below deliberately omits `timeout`, so a bare `timeout 30` prefix would not be found and
# the case would fail with an empty rc — which it did, first run.
wd_outer=""
wd_timeout_bin=$(command -v timeout 2>/dev/null || true)
[ -n "$wd_timeout_bin" ] && wd_outer="$wd_timeout_bin 30"
# DECIDE THE SKIP BEFORE INVOKING (roborev job 210, finding 3b). The first cut ran the
# bounded probe and only then checked whether an outer bound existed — so on precisely the
# hosts with no `timeout`, a broken watchdog HUNG THE SUITE instead of skipping the case.
# The safeguard has to be in place before the thing it guards runs.
if [ -z "$wd_outer" ]; then
  echo "skip - 3544-bound-enforced: no host 'timeout' to bound this case from the OUTSIDE; letting the mechanism under test bound its own test would be circular"
  echo "skip - 3544-bound-grandchild: same precondition (no outer host bound available)"
else
wd_rc_line=$( cd "$behind" && PATH="$bin_no_timeout" $wd_outer bash "$behind/scripts/agent-gate.sh" \
                --component-set-bounded-run 1 "$ticker" 2>/dev/null | sed -n 's/^RC: //p' )
ticks_at_return=$(wc -l <"$tick" | tr -d ' ')
sleep 3
ticks_later=$(wc -l <"$tick" | tr -d ' ')
if [ "$wd_rc_line" = 124 ] && [ "$ticks_later" = "$ticks_at_return" ]; then
  ok "3544-bound-enforced: the bash watchdog bounds a hanging command (rc 124) and leaves no live child"
else
  bad "3544-bound-enforced: expected rc 124 and a dead child (rc='$wd_rc_line' ticks $ticks_at_return -> $ticks_later)"
fi

# THE GRANDCHILD CASE (roborev job 210, finding 2). A bound that signals only its direct
# child is not a bound: the grandchild survives, keeps the command-substitution pipe open,
# and the "bounded" call never returns. Measured before the fix: direct-child TERM left the
# grandchild ticking 2 -> 5; the process-group signal froze it at 2 -> 2. The parent here
# spawns a ticker and `wait`s, which is the shape of a git transport helper.
gtick="$tmp/grandchild-tick.txt"
gparent="$tmp/grandchild-parent.sh"
{ printf '#!/bin/sh\n'
  printf 'sh -c '"'"'while : ; do echo tick >> "%s"; sleep 1; done'"'"' &\n' "$gtick"
  printf 'wait\n'
} >"$gparent"
chmod +x "$gparent"
: >"$gtick"
g_rc_line=$( cd "$behind" && PATH="$bin_no_timeout" $wd_outer bash "$behind/scripts/agent-gate.sh" \
               --component-set-bounded-run 1 "$gparent" 2>/dev/null | sed -n 's/^RC: //p' )
g_at_return=$(wc -l <"$gtick" | tr -d ' ')
sleep 3
g_later=$(wc -l <"$gtick" | tr -d ' ')
if [ "$g_rc_line" = 124 ] && [ "$g_later" = "$g_at_return" ]; then
  ok "3544-bound-grandchild: a GRANDCHILD does not outlive the bound (process-group signal), and the call returns"
else
  bad "3544-bound-grandchild: expected rc 124 and a dead grandchild (rc='$g_rc_line' ticks $g_at_return -> $g_later)"
fi

# A TERM-IGNORING DESCENDANT is bounded on BOTH mechanisms (roborev job 214). This is the
# case that showed a TERM-only bound is not a bound at all: measured before the fix,
# `timeout 2 <script with trap '' TERM>` held for the FULL 2 minutes of an outer bound and
# never returned. Covering only the bash arm would leave the more common branch — the one
# every coreutils host takes — unproven, so the case runs the SAME fixture through both.
#
# Accepted statuses are 124 OR 137: an external `timeout` reports 137 once `--kill-after`
# escalates to SIGKILL, and both values mean "bound exceeded". Liveness is judged by whether
# the descendant is still writing, never by elapsed time.
tignore="$tmp/term-ignoring.sh"
titick="$tmp/term-ignoring-tick.txt"
{ printf '#!/bin/sh\n'
  printf "trap '' TERM\n"
  printf 'while : ; do echo tick >> "%s"; sleep 1; done\n' "$titick"
} >"$tignore"
chmod +x "$tignore"
# mech_label <PATH> is only for the failure message; the assertion is on behaviour.
for _mech_path in "$PATH" "$bin_no_timeout"; do
  _mech=$(bound_of "$_mech_path")
  : >"$titick"
  _ti_rc=$( cd "$behind" && PATH="$_mech_path" $wd_outer bash "$behind/scripts/agent-gate.sh" \
              --component-set-bounded-run 1 "$tignore" 2>/dev/null | sed -n 's/^RC: //p' )
  _ti_at=$(wc -l <"$titick" | tr -d ' ')
  sleep 3
  _ti_later=$(wc -l <"$titick" | tr -d ' ')
  case "$_ti_rc" in
    124|137) _ti_rc_ok=1 ;;
    *)       _ti_rc_ok=0 ;;
  esac
  if [ "$_ti_rc_ok" -eq 1 ] && [ "$_ti_later" = "$_ti_at" ]; then
    ok "3544-bound-term-ignoring[$_mech]: a TERM-IGNORING descendant is KILLed within the bound (rc $_ti_rc) and stops"
  else
    bad "3544-bound-term-ignoring[$_mech]: expected rc 124|137 and a dead descendant (rc='$_ti_rc' ticks $_ti_at -> $_ti_later)"
  fi
done

# A HANGING `git` is bounded too — the composition that covers the partial-clone `git show`
# without a 120-second test: this proves the RUNNER bounds a hanging `git`, and the
# structural enumeration assert (case 9b) proves `git show` goes THROUGH that runner. Each
# half is cheap; together they are the property. Asserting it end-to-end would mean waiting
# out the real 120s bound, and a test nobody will run is not coverage.
ghang="$tmp/hanging-git.sh"
{ printf '#!/bin/sh\n'; printf 'exec sleep 300\n'; } >"$ghang"
chmod +x "$ghang"
gh_rc_line=$( cd "$behind" && PATH="$bin_no_timeout" $wd_outer bash "$behind/scripts/agent-gate.sh" \
                --component-set-bounded-run 1 "$ghang" 2>/dev/null | sed -n 's/^RC: //p' )
if [ "$gh_rc_line" = 124 ]; then
  ok "3544-bound-hanging-git: a hanging git-shaped command is bounded (rc 124), not waited on forever"
else
  bad "3544-bound-hanging-git: expected rc 124 (got '$gh_rc_line')"
fi
fi

# …and with NO mechanism at all the command must NOT RUN. This is the load-bearing half:
# reporting "unboundable" while still running the command unbounded would fix nothing.
nb_marker="$tmp/must-not-run-marker"
rm -f "$nb_marker"
nb_rc_line=$( cd "$behind" && PATH="$bin_no_bound" bash "$behind/scripts/agent-gate.sh" \
                --component-set-bounded-run 1 touch "$nb_marker" 2>/dev/null | sed -n 's/^RC: //p' )
if [ "$nb_rc_line" = 199 ] && [ ! -e "$nb_marker" ]; then
  ok "3544-bound-none-refuses: with no bounding mechanism the command is REFUSED (rc 199), not run unbounded"
else
  bad "3544-bound-none-refuses: expected rc 199 and NO side effect (rc='$nb_rc_line', marker $( [ -e "$nb_marker" ] && echo CREATED || echo absent ))"
fi

# Probe level: an unboundable host is its own named UNMEASURED kind, fail-closed in the
# certifying modes and ADVISORY under --lite — and the fetch is never attempted, so there is
# no branch on which this pre-flight can hang. Positive control first: the SAME curated PATH
# WITH a bounding tool must reach a real verdict, or the case would pass for the wrong reason.
ub_control=$( cd "$behind" && PATH="$bin_no_timeout" bash "$behind/scripts/agent-gate.sh" \
                --component-set-line full 2>/dev/null )
if [ "$(field KIND "$ub_control")" != ok ]; then
  echo "skip - 3544-unboundable: the curated tool PATH cannot complete a probe on this host (control KIND='$(field KIND "$ub_control")') — the unboundable branch is not exercisable here"
else
  ub_out=$( cd "$behind" && PATH="$bin_no_bound" bash "$behind/scripts/agent-gate.sh" \
              --component-set-line full 2>/dev/null )
  ub_lite=$( cd "$behind" && PATH="$bin_no_bound" bash "$behind/scripts/agent-gate.sh" \
              --component-set-line lite 2>/dev/null )
  ub_line=$(field COMPONENT_SET_LINE "$ub_out")
  if [ "$(field VERDICT "$ub_out")" = UNMEASURED ] \
     && [ "$(field KIND "$ub_out")" = unboundable ] \
     && [ "$(field SHA "$ub_out")" = "-" ] \
     && grep -q 'FAIL-CLOSED (#3544)' <<<"$ub_line" \
     && grep -q 'UNBOUNDED' <<<"$ub_line" \
     && grep -q '^component-set: ADVISORY-UNMEASURED (#3544)' <<<"$(field COMPONENT_SET_LINE "$ub_lite")" \
     && grep -q 'unboundable' <<<"$(field COMPONENT_SET_LINE "$ub_lite")"; then
    ok "3544-unboundable: an unboundable host FAILs closed naming the refusal (full) and is ADVISORY under --lite (control reached KIND ok)"
  else
    bad "3544-unboundable: expected KIND unboundable + FAIL-CLOSED (full) and ADVISORY-UNMEASURED (lite)"
    printf '%s\n' "$ub_out"; printf '%s\n' "$ub_lite"
  fi
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
# 4c. INDETERMINATE — the THIRD ancestry state, and the one a two-way test cannot express.
#     `git merge-base --is-ancestor` answers 0 (ancestor) or 1 (not), and ANYTHING ELSE is
#     an error, so collapsing "cannot tell" onto either answer is a false verdict on
#     correct input: onto BEHIND it reds a legitimate removal, onto DECLARED it SWALLOWS
#     the skew this whole guard exists to catch. It must therefore be its own fail-closed
#     verdict, and this case is the only thing standing between that branch and a
#     miswiring nobody would notice — the rc classification is invisible from a
#     BEHIND/DECLARED pair, both of which pass whichever way an error is bucketed.
#
#     Forced with an UNBORN HEAD (a fixture with a remote and a fetchable origin/main but
#     no commit of its own): the probe then measures the baseline fine — `_CS_KIND` is `ok`
#     and the missing component is named — while `--is-ancestor` exits 128 because HEAD
#     resolves to nothing. That is a REAL rc≠0,1, not a simulated one, and it is reached
#     through the shipped code path rather than by stubbing git.
#
#     Note the branch's own set comes from the RUNNING script's COMPONENTS array, not from
#     a commit, which is exactly why an unborn HEAD still produces a measurable comparison.
# ---------------------------------------------------------------------------
base_ind=$(mkbaseline base-ind "$ADD_SENTINEL")
ind="$tmp/indeterminate"
mkdir -p "$ind/scripts"
cp "$GATE" "$ind/scripts/agent-gate.sh"
( cd "$ind" && git init -q . && git remote add origin "$base_ind" ) >/dev/null 2>&1
ind_out=$(hook "$ind")
ind_line=$(field COMPONENT_SET_LINE "$ind_out")
ind_sha=$(git -C "$base_ind" rev-parse refs/heads/main)
if [ "$(field VERDICT "$ind_out")" = INDETERMINATE ] \
   && [ "$(field KIND "$ind_out")" = ok ] \
   && [ "$(field ANCESTOR "$ind_out")" = unknown ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$ind_line" \
   && grep -q -- "$SENTINEL" <<<"$ind_line" \
   && grep -q 'ancestry probe could not tell BEHIND from a deliberate removal' <<<"$ind_line" \
   && grep -q 'merge-base --is-ancestor' <<<"$ind_line" \
   && grep -q "origin/main $ind_sha" <<<"$ind_line"; then
  ok "3544-indeterminate: an unanswerable ancestry probe is its OWN fail-closed verdict, naming the probe"
else
  bad "3544-indeterminate: expected VERDICT INDETERMINATE naming the ancestry probe and $SENTINEL"
  printf '%s\n' "$ind_out"
fi

# The REAL emit for it, and its OWN diagnosis: a shared "the baseline could not be
# measured" message would be FALSE here (the baseline WAS measured; the ancestry was not)
# and would send its reader to fix the wrong thing, so the block's `preflight:` line and
# `hint:` must name the ancestry probe, and must NOT offer `git rebase` — which is not the
# remedy for an unresolvable HEAD.
ind_sum="$tmp/indeterminate-summary.txt"
( cd "$ind" && AGENT_GATE_SUMMARY_FILE="$ind_sum" CQLITE_DATASETS_ROOT="$tmp/no-datasets" \
    bash scripts/agent-gate.sh >"$tmp/indeterminate.log" 2>&1 ); ind_rc=$?
if [ "$ind_rc" -ne 0 ] \
   && grep -q '^RESULT: FAIL' "$ind_sum" 2>/dev/null \
   && grep -q '^preflight: FAIL (component-set: components missing vs origin/main and the ancestry probe could not classify' "$ind_sum" 2>/dev/null \
   && grep -q '^hint: repair the repository so' "$ind_sum" 2>/dev/null \
   && ! grep -q 'hint: git fetch origin && git rebase' "$ind_sum" 2>/dev/null \
   && ! grep -q 'baseline NOT measured' "$ind_sum" 2>/dev/null; then
  ok "3544-indeterminate-emit: the FULL gate FAILs with the ancestry-specific cause and remedy"
else
  bad "3544-indeterminate-emit: expected an ancestry-named preflight line + its own hint (rc=$ind_rc)"
  sed -n '1,20p' "$ind_sum" 2>/dev/null
fi

# …and under --lite it is ADVISORY-INDETERMINATE: the line is there, named, and the run is
# NOT failed by it (--lite runs every fix round; a repo state it cannot classify must not
# stop the fast loop).
ind_lite=$(hook "$ind" lite)
ind_lite_line=$(field COMPONENT_SET_LINE "$ind_lite")
ind_lsum="$tmp/indeterminate-lite.txt"
( cd "$ind" && AGENT_GATE_SUMMARY_FILE="$ind_lsum" CQLITE_DATASETS_ROOT="$tmp/no-datasets" \
    bash scripts/agent-gate.sh --lite >"$tmp/indeterminate-lite.log" 2>&1 ) >/dev/null 2>&1
if [ "$(field STRICT "$ind_lite")" = no ] \
   && grep -q '^component-set: ADVISORY-INDETERMINATE (#3544)' <<<"$ind_lite_line" \
   && grep -q -- '--lite is lenient' <<<"$ind_lite_line" \
   && ! grep -q 'FAIL-CLOSED' <<<"$ind_lite_line" \
   && grep -q '^component-set: ADVISORY-INDETERMINATE (#3544)' "$ind_lsum" 2>/dev/null \
   && ! grep -q 'preflight: FAIL (component-set' "$ind_lsum" 2>/dev/null \
   && grep -qE '^fmt: +(PASS|FAIL)' "$ind_lsum" 2>/dev/null; then
  ok "3544-indeterminate-lite: --lite stamps ADVISORY-INDETERMINATE and still reaches its components"
else
  bad "3544-indeterminate-lite: expected an ADVISORY-INDETERMINATE line and a lite run that proceeds"
  printf '%s\n' "$ind_lite"
  sed -n '1,20p' "$ind_lsum" 2>/dev/null
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
# 5b. THE BASELINE IS FETCHED, NOT READ FROM THE CACHE — the #3544 amendment's core
#     requirement, and the one whose violation is SILENT: comparing against a stale
#     `origin/main` returns "no skew" against a superseded baseline. Measured on the #3393
#     lane, its remote-tracking ref was 23 minutes stale at the moment it acted on it.
#
#     Driven by ADVANCING the bare origin AFTER the fixture cloned it, so the cached
#     `refs/remotes/origin/main` and the real tip DIFFER: the reported sha must be the NEW
#     tip. The same case pins the complement — `refs/remotes/origin/main` must NOT MOVE,
#     because a lane is a `git worktree` of a shared `.git` where that ref is shared with
#     its siblings, so writing it both contends on a lock (a concurrent FAIL-CLOSED) and
#     side-effects a peer's baseline mid-run.
# ---------------------------------------------------------------------------
# Its OWN baseline, never a shared one: this case ADVANCES origin/main, and a fixture
# mutated by one case is a fixture the next case is no longer measuring what it thinks it
# is (case 7 reuses `$same` and its recorded sha — it broke exactly that way, first run).
base_fresh=$(mkbaseline base-fresh - )
fresh=$(mkbranch fresh "$base_fresh" - --from-origin)
fresh_cached_before=$(git -C "$fresh" rev-parse refs/remotes/origin/main 2>/dev/null)
( cd "$tmp/base-fresh-src" && printf 'advanced after the clone\n' >>README.md \
  && git "${GIT_ID[@]}" commit -qam advance \
  && git push -q "$base_fresh" HEAD:refs/heads/main ) >/dev/null 2>&1
fresh_tip=$(git -C "$base_fresh" rev-parse refs/heads/main)
fr_out=$(hook "$fresh")
fresh_cached_after=$(git -C "$fresh" rev-parse refs/remotes/origin/main 2>/dev/null)
if [ "$(field SHA "$fr_out")" = "$fresh_tip" ] \
   && [ "$fresh_tip" != "$fresh_cached_before" ] \
   && [ "$fresh_cached_after" = "$fresh_cached_before" ]; then
  ok "3544-fresh-baseline: the comparison uses the FETCHED tip, and leaves the shared cached ref alone"
else
  bad "3544-fresh-baseline: expected the new tip $fresh_tip (got '$(field SHA "$fr_out")'), cached ref unmoved (before='$fresh_cached_before' after='$fresh_cached_after')"
  printf '%s\n' "$fr_out"
fi

# ---------------------------------------------------------------------------
# 5c. THE FETCH WRITES NO SHARED REF — tags included (roborev job 214). `--refmap=` stops
#     the opportunistic `refs/remotes/origin/*` write, but `git fetch` ALSO auto-follows
#     tags into the SHARED `refs/tags/*`, which reintroduced exactly the cross-lane ref
#     contention `--refmap=` was added to remove: four lanes share one `.git` here, so a new
#     upstream tag meant concurrent fetches racing a tag ref, and the loser's non-zero fetch
#     made this fail-closed pre-flight reject a run for a purely CONCURRENT cause.
#
#     The assertion is on REF STATE, not on the fetch's exit status: a fetch that succeeds
#     while writing a tag is precisely the passing-but-wrong case (5b makes the same point
#     for branch refs, and the two together are the whole guarantee).
# ---------------------------------------------------------------------------
base_tag=$(mkbaseline base-tag - )
tagged=$(mkbranch tagged "$base_tag" - --from-origin)
# A NEW tag on the baseline, created AFTER the fixture cloned it — so an auto-following
# fetch would have something to write.
( cd "$tmp/base-tag-src" && git "${GIT_ID[@]}" tag -a v99.99.99-selftest -m 'tag the baseline' \
    && git push -q "$base_tag" refs/tags/v99.99.99-selftest ) >/dev/null 2>&1
tags_before=$(git -C "$tagged" for-each-ref --format='%(refname) %(objectname)' refs/tags | sort)
tg_out=$(hook "$tagged")
tags_after=$(git -C "$tagged" for-each-ref --format='%(refname) %(objectname)' refs/tags | sort)
upstream_tag=$(git -C "$base_tag" for-each-ref --format='%(refname)' refs/tags | grep -c 'v99.99.99-selftest')
if [ "$upstream_tag" -ge 1 ] \
   && [ "$(field KIND "$tg_out")" = ok ] \
   && [ "$tags_after" = "$tags_before" ] \
   && ! printf '%s\n' "$tags_after" | grep -q 'v99.99.99-selftest'; then
  ok "3544-no-tag-writes: the baseline fetch leaves shared refs/tags/* UNCHANGED even when upstream gained a tag"
else
  bad "3544-no-tag-writes: expected an unchanged tag ref set (upstream_tag=$upstream_tag kind=$(field KIND "$tg_out"))"
  echo "   before: [$tags_before]"
  echo "   after:  [$tags_after]"
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
   && grep -q '^preflight: FAIL (component-set BEHIND origin/main' "$sum" 2>/dev/null \
   && grep -q '^hint: git fetch origin && git rebase origin/main' "$sum" 2>/dev/null; then
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
# 7b. A REAL --lite run (not the hook): the LITE block must CARRY the advisory line AND
#     the run must reach the components — the fail-closed path exits BEFORE any component
#     and emits no per-component row, so the presence of a `fmt:` row is what proves the
#     pre-flight let the run through. (The fixture has no Cargo.toml, so the cargo
#     components legitimately FAIL and RESULT is FAIL; that is why "did not fail on the
#     component set" is asserted from the ABSENCE of the fail-closed marker plus the
#     PRESENCE of component rows, never from RESULT.)
# ---------------------------------------------------------------------------
lsum="$tmp/lite-summary.txt"
( cd "$behind" && AGENT_GATE_SUMMARY_FILE="$lsum" CQLITE_DATASETS_ROOT="$tmp/no-datasets" \
    bash scripts/agent-gate.sh --lite >"$tmp/lite.log" 2>&1 ) >/dev/null 2>&1
if grep -q '^==== AGENT-GATE LITE SUMMARY ====' "$lsum" 2>/dev/null \
   && grep -q "^component-set: ADVISORY-BEHIND (#3544).*$SENTINEL" "$lsum" 2>/dev/null \
   && grep -q -- '--lite is lenient' "$lsum" 2>/dev/null \
   && ! grep -q 'component-set: FAIL-CLOSED' "$lsum" 2>/dev/null \
   && ! grep -q 'preflight: FAIL (component-set' "$lsum" 2>/dev/null \
   && grep -qE '^fmt: +(PASS|FAIL)' "$lsum" 2>/dev/null; then
  ok "3544-lite-emit: a real --lite run stamps the advisory line and still reaches its components"
else
  bad "3544-lite-emit: expected a LITE block with the ADVISORY-BEHIND line AND component rows"
  sed -n '1,25p' "$lsum" 2>/dev/null
fi

# ---------------------------------------------------------------------------
# 7c. EVERY summary emitter accounts for the line — the emitter set DERIVED from the gate
#     at run time, never a list maintained here. The first cut was a hardcoded heredoc of
#     8 anchors under a message saying "every", and roborev found what a curated list
#     always eventually hides: the four early `--delta` anchor-validation emitters and the
#     summary-integrity failure emitter were in neither the stamping nor the list. A guard
#     whose CLAIM exceeds its CHECK is a false assurance — the same shape as this file's
#     own six-vs-nine count, and as #3544 itself, three instances in one change.
#
#     The derivation classifies every `emit_summary` / `_emit_terminal_summary` CALL SITE
#     (definitions and comments excluded) as one of:
#       STAMPED-DIRECT           the line is in the call's own argument list;
#       STAMPED-VIA-<ARRAY>      the call passes "${ARRAY[@]}" and the array's OWN
#                                construction region pushes the line;
#       STAMPED-VIA-RENDERER-<f> the array is filled from a function whose body emits the
#                                line (one level of resolution — `_tree_boundary_meta_lines`
#                                stamps it itself, so a site fed from it IS stamped);
#       EXEMPT                   the site carries `component-set-exempt: <reason>` (the
#                                pre-dispatch self-test hooks and the shared forwarder,
#                                which have no verdict to stamp because the pre-flight has
#                                not run yet);
#       GAP                      none of the above ⇒ FAIL, naming the site.
#     So a NEW emitter is picked up with NO edit to this file: it lands in GAP until its
#     author stamps it or exempts it with a stated reason.
#
#     WHAT IT CLAIMS: every emit site is ACCOUNTED FOR in source. It does NOT claim the
#     stamp reaches the emitted block — that is proved BEHAVIOURALLY, and only for the
#     blocks a hermetic fixture can produce: the pre-flight FAIL block (case 7), the
#     --delta REFUSED block (7 + 4c) and the real --lite block (7b). The two TERMINAL
#     blocks (full and --delta) are structural-only here for a reason that has not changed:
#     reaching them needs a real re-cert (cargo fmt + scoped tests + a corpus + an #1825
#     slot), which no hermetic fixture can supply. And per-SITE beats a stamp COUNT because
#     a count reds when someone adds a correctly-stamped emitter — a guard that reds on
#     correct input is the guard agents learn to waive.
#
#     The derivation is itself FAIL-CLOSED and carries a POSITIVE CONTROL below: if it
#     finds no call sites at all, or if removing a known stamp does not produce a GAP, it
#     FAILs rather than reporting a clean census of nothing.
# ---------------------------------------------------------------------------
CENSUS_AWK="$tmp/emit-census.awk"
cat >"$CENSUS_AWK" <<'CENSUS_PROG'
{ line[NR]=$0 }
function _stamping_fn(fn,   k, inside) {
  inside=0
  for (k=1; k<=NR; k++) {
    if (line[k] ~ ("^" fn "\\(\\) \\{")) { inside=1; continue }
    if (inside && line[k] ~ /^\}/) return 0
    if (inside && line[k] ~ /COMPONENT_SET_LINE|_component_set_meta/) return 1
  }
  return 0
}
END {
  for (i=1; i<=NR; i++) {
    l=line[i]
    if (l ~ /^[ \t]*#/) continue
    if (l ~ /^(emit_summary|_emit_terminal_summary)\(\)/) continue
    if (l !~ /(^|[^_a-zA-Z])(emit_summary|_emit_terminal_summary)[ \t]/) continue
    args=l; j=i
    while (args ~ /\\[ \t]*$/) { j++; args = args "\n" line[j] }
    verdict="GAP"
    if (args ~ /COMPONENT_SET_LINE|_component_set_meta/) verdict="STAMPED-DIRECT"
    else if (args ~ /component-set-exempt:[ \t]*[^ \t]/ || line[i-1] ~ /component-set-exempt:[ \t]*[^ \t]/) verdict="EXEMPT"
    else if (match(args, /\$\{[A-Za-z_]+\[@\]\}/)) {
      nm=substr(args, RSTART+2, RLENGTH-6)
      for (k=i; k>0; k--) {
        # Creation point / enclosing-function boundary. BOTH bounds matter: keyed on
        # `nm=()` alone, a multi-line `nm=(` literal never matched, the scan ran to the top
        # of the file and "found" an unrelated renderer 700 lines away — a FALSE STAMPED on
        # a block that stamps nothing. A stamp outside the array's own construction cannot
        # feed it.
        if (line[k] ~ ("(declare -a |local -a )?" nm "=\\(")) break
        if (k < i && line[k] ~ /^[A-Za-z_][A-Za-z0-9_]*\(\) \{|^\}/) break
        if (line[k] ~ ("" nm "\\+=\\(\"\\$COMPONENT_SET_LINE\"\\)") || line[k] ~ ("" nm "\\+=\\(\"\\$\\(_component_set_meta\\)\"\\)")) { verdict="STAMPED-VIA-" nm; break }
        if (match(line[k], /< <\(([A-Za-z_][A-Za-z0-9_]*)\)/)) {
          fn=substr(line[k], RSTART+4, RLENGTH-5)
          if (_stamping_fn(fn)) { verdict="STAMPED-VIA-RENDERER-" fn; break }
        }
      }
    }
    printf "%s\t%d\t%s\n", verdict, i, substr(l,1,70)
  }
}
CENSUS_PROG

emit_census() { awk -f "$CENSUS_AWK" "${1:-$GATE}"; }
census_out=$(emit_census)
census_sites=$(printf '%s\n' "$census_out" | grep -c '	')
census_gaps=$(printf '%s\n' "$census_out" | grep -c '^GAP	')
if [ "$census_sites" -eq 0 ]; then
  bad "3544-every-emitter: the emit-site derivation found NO call sites in $GATE — the call shape changed or the scan broke (fail-closed: this is not a clean census)"
elif [ "$census_gaps" -eq 0 ]; then
  ok "3544-every-emitter: all $census_sites emit sites account for the component-set line (stamped, or exempt with a reason) — source census; the lite/delta/pre-flight blocks are proved behaviourally above"
else
  bad "3544-every-emitter: $census_gaps of $census_sites emit site(s) neither stamp the component-set line nor carry 'component-set-exempt: <reason>':"
  printf '%s\n' "$census_out" | grep '^GAP	' | while IFS='	' read -r _v _ln _src; do
    echo "   line $_ln: $_src"
  done
fi

# POSITIVE CONTROL for the derivation itself: strip the stamp from ONE known-stamped site
# in a THROWAWAY copy and require the census to report exactly that site as a GAP. Without
# this, a scan that silently stopped matching would report "all sites accounted for" — a
# clean census of nothing, which is the vacuous pass this whole issue is about.
ctl_gate="$tmp/census-control-gate.sh"
# Portable FIRST-MATCH deletion. This was `sed '0,/re/{...}'`, whose `0,` address is a GNU
# EXTENSION that BSD/macOS sed rejects outright (roborev job 210, finding 3) — so this
# tooling test would have failed on the very macOS hosts the `gtimeout` branch exists to
# serve. awk with an EXACT string compare is both portable and more precise than the range.
awk 'BEGIN { done = 0 }
     { if (!done && $0 == "      \"$(_component_set_meta)\" \\") { done = 1; next }
       print }' "$GATE" >"$ctl_gate"
if ! cmp -s "$GATE" "$ctl_gate"; then
  ctl_gaps=$(emit_census "$ctl_gate" | grep -c '^GAP	')
  if [ "$ctl_gaps" -ge 1 ]; then
    ok "3544-every-emitter-control: removing one stamp makes the census report a GAP ($ctl_gaps) — the scan is live, not inert"
  else
    bad "3544-every-emitter-control: a gate with a stamp REMOVED still censused clean — the derivation is inert"
  fi
else
  bad "3544-every-emitter-control: could not build the control (no stamped site matched) — the census cannot be shown to discriminate"
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

# 3e. The grammar must NOT red on a PLAUSIBLE FUTURE component name. It is deliberately
#     wider than today's `[a-z0-9-]` names: a component added on main as `write_tests` or
#     `bti.v2` must be measured, not reported as garbage — a guard that reds on correct
#     input is the guard agents learn to waive. (The garbage case above pins the other
#     direction, so this pair brackets the grammar from both sides.)
base_future=$(mkbaseline base-future 's|^COMPONENTS=(file-size|COMPONENTS=(write_tests bti.v2 file-size|')
future=$(mkbranch future "$base_future" - )
fu_out=$(hook "$future")
if [ "$(field VERDICT "$fu_out")" = BEHIND ] \
   && [ "$(field KIND "$fu_out")" = ok ] \
   && grep -qw -- 'write_tests' <<<"$(field MISSING "$fu_out")" \
   && grep -qw -- 'bti.v2' <<<"$(field MISSING "$fu_out")"; then
  ok "3544-grammar-future: an underscore/dot component name is MEASURED, not called garbage"
else
  bad "3544-grammar-future: expected KIND ok + both future-shaped names in MISSING"
  printf '%s\n' "$fu_out"
fi

# ---------------------------------------------------------------------------
# 9. THE CENSUS COUNT IS MECHANICAL. A PROSE count drifts, and this file proved it: the
#    header said "six" for two rounds while its own enumeration listed nine. That is the
#    exact shape #3544's 06:15Z comment describes one level out — "nothing anywhere relates
#    the exemption's claim to the named component's actual scope, so the two drift silently
#    and the drift is invisible from either file alone" — so the relation is made explicit
#    here: derive the kind count from the GATE at run time, compare it to the ONE declared
#    constant below, and FAIL naming BOTH numbers.
#
#    WHAT THIS ASSERT CLAIMS, EXACTLY — and it is narrow: *the census was re-read when the
#    gate's kind set changed*. It does NOT claim, and must never be read as claiming, that
#    every kind is COVERED. Coverage is what cases 2, 3a–3d, 3f and 4c do, by driving each
#    kind through the shipped code and requiring the verdict to NAME it.
#
#    THE PROXY THIS DELIBERATELY IS NOT: asserting "each kind NAME appears somewhere in
#    this file" would be satisfied by a kind mentioned ONLY in the census comment above —
#    a false PASS, and this repository's standing ruling is that a guard with a known
#    false-PASS is worse than no guard, because it invites reliance it cannot support. So
#    the assert compares two DERIVED NUMBERS and nothing else; it is safe precisely because
#    it does not attempt to answer the question it cannot answer.
#
#    FAIL-CLOSED on its own derivation: an empty derived set means the assignment shape in
#    the gate changed (or the scan broke), which must be a FAIL naming the derivation —
#    never a comparison against 0 that quietly agrees with nothing. Same rule as the
#    baseline `--list` derivation this whole pre-flight is built on.
# ---------------------------------------------------------------------------
# The ONE declared constant. Bump it in the SAME change that adds/removes a `_CS_KIND`
# value, and extend the census above and a case below at the same time.
DECLARED_KIND_COUNT=10
# Scan the WHOLE gate, not just `_component_set_probe`: every assignment lives inside that
# function today, but a scan scoped to it would MISS a kind set elsewhere later and the
# count would silently keep agreeing with this constant. A superset scan cannot miss.
derived_kinds=$(grep -o '_CS_KIND=[A-Za-z][A-Za-z0-9-]*' "$GATE" 2>/dev/null \
                  | sed 's/_CS_KIND=//' | grep -vx ok | sort -u)
derived_count=$(printf '%s\n' "$derived_kinds" | grep -c '[a-z]')
if [ "$derived_count" -eq 0 ]; then
  bad "3544-kind-census: could not DERIVE any _CS_KIND value from $GATE — the assignment shape changed or the scan broke (fail-closed: this is not a count of 0)"
elif [ "$derived_count" -eq "$DECLARED_KIND_COUNT" ]; then
  ok "3544-kind-census: the declared kind count ($DECLARED_KIND_COUNT) matches the gate's derived set ($derived_count) — the census was re-read; this asserts NOTHING about coverage"
else
  bad "3544-kind-census: the gate has $derived_count non-ok _CS_KIND values but this file declares $DECLARED_KIND_COUNT — update DECLARED_KIND_COUNT, the census comment AND a case for each new kind. Derived: $(printf '%s' "$derived_kinds" | tr '\n' ' ')"
fi

# ---------------------------------------------------------------------------
# 9b. NO UNBOUNDED EXTERNAL OPERATION IN THE PRE-FLIGHT — derived from source, because this
#     family regenerated three times (the fetch, then `git show`'s lazy blob read in a
#     partial clone, then a grandchild surviving a direct-child kill). Each site LOOKED
#     local, which is exactly why "audit it again" is not a fix and an assert is.
#
#     Two questions, both answered from the gate's own text:
#       (i)  every `git` invocation in the region is either routed through
#            `_component_set_bounded` or carries a `# local-only: <reason>` annotation at
#            its own site — an unannotated one is a GAP;
#       (ii) the set of EXTERNAL PROGRAMS the region invokes equals a DECLARED list, so a
#            new one (`curl`, `ssh`, `python3`, another `bash`) cannot appear unclassified.
#
#     WHAT IT CLAIMS: every external invocation is CLASSIFIED. It cannot prove a command
#     is truly network-free — that is a judgement, recorded per site in the annotations and
#     in the enumeration comment at the head of the block, which is where a reviewer should
#     check it. What it does guarantee is that the judgement was MADE and is visible in the
#     diff, which is what would have caught `git show`.
# ---------------------------------------------------------------------------
region="$tmp/preflight-region.sh"
awk '/^# ---- issue #3544: component-set skew pre-flight/ { inr = 1 }
     /^# ---- issue #2081:/ { inr = 0 }
     inr { print }' "$GATE" >"$region"
region_lines=$(wc -l <"$region" | tr -d ' ')

# ONE audit program, written once and used for BOTH the region and its positive controls.
# The first cut inlined the same awk twice; two copies of one rule is the drift this file
# keeps finding elsewhere, and the control is worthless if it audits a different rule.
#
# `git` must be at COMMAND POSITION. The first cut matched `git ` anywhere and flagged 12
# DIAGNOSTIC STRINGS (`_CS_DETAIL="git fetch … exited $rc"`, `hint="… git rebase …"`) as
# unbounded invocations — a guard that reds on correct input, which is the guard agents
# learn to waive. So quoted spans are removed BEFORE matching, and the line is split into
# command fragments.
# ONE program, two records — `GAP` (a git invocation that is neither bounded nor annotated)
# and `EXT` (an external program word at command position). Both halves need the SAME
# strip-and-split rules, and two copies of one rule is the drift this file keeps finding:
# the first cut split the census on `;` WITHOUT removing quoted spans, so prose inside a
# diagnostic string (`"… no components; an empty baseline would excuse …"`) was reported as
# external programs `a` and `an`. The controls run through this same program for the same
# reason — a control that audits a different rule proves nothing.
GIT_AUDIT_AWK="$tmp/preflight-audit.awk"
cat >"$GIT_AUDIT_AWK" <<'GIT_AUDIT_PROG'
{ line[NR] = $0 }
function annotated(i) {
  return (line[i]   ~ /# local-only:[ \t]*[^ \t]/ \
       || line[i-1] ~ /# local-only:[ \t]*[^ \t]/ \
       || line[i-2] ~ /# local-only:[ \t]*[^ \t]/ \
       || line[i-3] ~ /# local-only:[ \t]*[^ \t]/)
}
END {
  for (i = 1; i <= NR; i++) {
    if (line[i] ~ /^[ \t]*#/) continue
    l = line[i]
    # ORDER IS LOAD-BEARING: quoted spans come off BEFORE the comment strip. Stripping
    # comments first truncates at a `#` INSIDE a string (`"… (measured: PR #3467, 31 of
    # 35)."`), which leaves an unbalanced quote, defeats the quote strip and reported
    # `measured:` as an external program. A `#` inside quotes is not a comment.
    # Strip quoted spans FIRST: string CONTENT must never look like a command.
    while (match(l, /"[^"]*"/)) l = substr(l, 1, RSTART-1) " " substr(l, RSTART+RLENGTH)
    while (match(l, /'"'"'[^'"'"']*'"'"'/)) l = substr(l, 1, RSTART-1) " " substr(l, RSTART+RLENGTH)
    # Arithmetic spans: `$(( waited + 1 ))` is not a command, and splitting it on `(`
    # reported `waited` and `n_missing` as external programs.
    while (match(l, /\$\(\([^)]*\)\)/)) l = substr(l, 1, RSTART-1) " " substr(l, RSTART+RLENGTH)
    sub(/#.*$/, "", l)
    # A `case` LABEL is not a command either: splitting on `)` reported `none`, `no` and
    # `yes` as programs. Strip a leading label (never a `name()` definition, which the
    # character class excludes by refusing `(`).
    sub(/^[ \t]*[A-Za-z0-9_*?.:\/|-]+\)[ \t]*/, "", l)
    bounded = (l ~ /_component_set_bounded/)
    probe   = (l ~ /command -v/)
    gsub(/\$\(/, "\n", l); gsub(/`/, "\n", l)
    gsub(/&&|\|\||[;|&(){}]/, "\n", l)
    n = split(l, frag, "\n")
    for (f = 1; f <= n; f++) {
      t = frag[f]
      sub(/^[ \t]*/, "", t)
      sub(/^![ \t]*/, "", t)
      while (t ~ /^(if|while|until|then|else|elif|do|not)[ \t]+/ || t ~ /^[A-Za-z_][A-Za-z0-9_]*=[^ \t]*[ \t]+/)
        sub(/^([A-Za-z_][A-Za-z0-9_]*=[^ \t]*|[a-z]+)[ \t]+/, "", t)
      split(t, w, /[ \t]/)
      cmd = w[1]
      if (cmd == "" || cmd !~ /^[a-z_:][a-z0-9_.:-]*$/) continue
      printf "EXT\t%s\n", cmd
      if (cmd == "git" && !bounded && !probe && !annotated(i))
        printf "GAP\t%d\t%s\n", i, substr(line[i], 1, 60)
    }
  }
}
GIT_AUDIT_PROG

if [ "$region_lines" -lt 100 ]; then
  bad "3544-no-unbounded: could not extract the pre-flight region from $GATE (got $region_lines lines) — the block markers changed (fail-closed: not a clean audit)"
else
  audit_out=$(awk -f "$GIT_AUDIT_AWK" "$region")
  git_gaps=$(printf '%s\n' "$audit_out" | sed -n 's/^GAP\t//p')
  # (ii) the external-program census: first word of every command fragment, minus shell
  #      keywords/builtins and minus every function this gate defines (derived, not listed).
  gate_fns=$(grep -Eo '^[A-Za-z_][A-Za-z0-9_]*\(\) \{' "$GATE" | sed 's/() {//' | sort -u)
  # The DECLARED external set, mirroring the enumeration comment at the head of the
  # pre-flight block. `timeout`/`gtimeout` are the bounding mechanisms themselves (local,
  # no network); the rest are local utilities. A program not in this list FAILs the case
  # until someone classifies it — which is the property that closes the family.
  declared_externals="basename bash cat cut git gtimeout kill mkdir mktemp rm sleep timeout tr true"
  externals=$(printf '%s\n' "$audit_out" | sed -n 's/^EXT\t//p' | sort -u)
  undeclared=""
  for _w in $externals; do
    case " then if fi else elif return local echo printf esac case do done while for shift exit continue wait set true : command read eval test unset export trap cd type hash pwd let declare readonly source break " in
      *" $_w "*) continue ;;
    esac
    printf '%s\n' "$gate_fns" | grep -qx -- "$_w" && continue
    case " $declared_externals " in *" $_w "*) continue ;; esac
    undeclared="${undeclared:+$undeclared }$_w"
  done
  if [ -n "$git_gaps" ]; then
    bad "3544-no-unbounded: git invocation(s) in the pre-flight neither bounded nor annotated '# local-only: <reason>':"
    printf '%s\n' "$git_gaps" | while IFS= read -r _g; do echo "   $_g"; done
  elif [ -n "$undeclared" ]; then
    bad "3544-no-unbounded: UNDECLARED external program(s) in the pre-flight region: $undeclared — classify each in the enumeration comment (bounded or local-only) and add it to declared_externals"
  else
    ok "3544-no-unbounded: every git invocation in the pre-flight is bounded or annotated local-only, and no undeclared external program appears (region $region_lines lines)"
  fi

  # POSITIVE CONTROLS for BOTH halves, through the SAME audit program: plant each defect
  # class in a throwaway copy and require it to be reported. Without this the audit could
  # stop matching and report a clean bill of health on a region it no longer parses — the
  # shape of every finding in this issue.
  ctl_unbounded="$tmp/region-unbounded.sh"
  {   cat "$region"; printf 'run_probe() { git -C "$REPO_ROOT" fetch origin main >/dev/null 2>&1; }\n'; } >"$ctl_unbounded"
  ctl_annotated="$tmp/region-annotated.sh"
  {   cat "$region"
      printf '# local-only: a declared reason, so this one must NOT be reported\n'
      printf 'run_probe() { git -C "$REPO_ROOT" rev-parse --git-dir >/dev/null 2>&1; }\n'; } >"$ctl_annotated"
  ctl_curl="$tmp/region-curl.sh"
  {   cat "$region"; printf 'run_probe() { curl -sS https://example.invalid/x >/dev/null; }\n'; } >"$ctl_curl"
  ctl_gaps=$(awk -f "$GIT_AUDIT_AWK" "$ctl_unbounded" | grep -c '^GAP	')
  ctl_ann_gaps=$(awk -f "$GIT_AUDIT_AWK" "$ctl_annotated" | grep -c '^GAP	')
  ctl_curl_seen=$(awk -f "$GIT_AUDIT_AWK" "$ctl_curl" | sed -n 's/^EXT\t//p' | grep -cx curl)
  if [ "$ctl_gaps" -eq 1 ] && [ "$ctl_ann_gaps" -eq 0 ] && [ "$ctl_curl_seen" -ge 1 ]; then
    ok "3544-no-unbounded-control: the audit reports a planted UNBOUNDED git (1), stays silent on an ANNOTATED one (0), and the census sees a planted network program — live in both directions"
  else
    bad "3544-no-unbounded-control: audit not discriminating (unbounded=$ctl_gaps expected 1, annotated=$ctl_ann_gaps expected 0, curl seen=$ctl_curl_seen expected >=1)"
  fi
fi

printf '\n%s\n' "----------------------------------------"
printf 'passed: %d  failed: %d\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
