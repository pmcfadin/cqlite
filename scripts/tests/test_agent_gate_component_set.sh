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
#   0b. `origin` does not NAME the canonical upstream (a fork/re-pointed)-> FAIL naming it
#   1. baseline has a component the branch lacks, main NOT an ancestor  -> FAIL naming it
#   2. `git fetch` fails (unreachable origin)                           -> non-PASS naming the fetch
#   3. baseline set unreadable AS DATA: an unreadable/reflowed COMPONENTS declaration, an
#      empty or ungrammatical manifest, neither file present        -> FAIL naming the derivation
#   3e2. THIS TREE's manifest missing / ungrammatical / out of step with COMPONENTS
#                                                                   -> FAIL naming the file
#   4. deliberate removal (main IS an ancestor of HEAD, absent at HEAD)  -> DECLARED, run NOT failed
#   4b. removal that is only an UNCOMMITTED working-tree edit            -> FAIL naming it
#   4d. a SHALLOW clone where rc 1 is ambiguous                          -> INDETERMINATE, never BEHIND
#   5d. a concurrent fetch clobbering FETCH_HEAD                         -> baseline unaffected
#   5. no skew                                                          -> affirmative PASS + baseline sha
#   6. --lite with a real skew                                          -> line present, run NOT failed
#   7. the REAL full-gate emit path                                     -> FAIL block + exit 1, no cargo
#
# CENSUS, stated so a later reader can tell "covered" from "forgotten" (a silent gap is
# the shape this whole issue is about). The pre-flight has FOUR verdicts and TEN non-`ok`
# probe kinds, and EVERY one is exercised below:
#   verdicts (6) — PASS (case 5), DECLARED (4), UNCOMMITTED (4b), BEHIND (1),
#                  INDETERMINATE (4c), UNMEASURED (2, 3a–3g, 4b-ii).
#   kinds   (17) — fetch-failed, no-remote (case 2); baseline-decl-unrecognised (3a),
#                  baseline-set-empty (3b), baseline-set-garbage (3c/3c-ii),
#                  baseline-unreadable (3d); manifest-missing, manifest-garbage,
#                  manifest-stale (3e2); no-git, baseline-workspace, no-tool (3f);
#                  unboundable (3g); baseline-transfer-mismatch (5f);
#                  baseline-probe-unmeasured (3a-iv);
#                  head-set-unmeasured (4b-ii); remote-not-canonical,
#                  remote-unreadable (10).
# EIGHTEEN is the count of DISTINCT non-`ok` values assigned to `_CS_KIND` (`fetch-failed` is
# set from several places, and `ok` is the nineteenth value). THE SET CHANGED SHAPE with #3544
# REQ-3544-01, which stopped deriving the baseline by EXECUTING a fetched script: the three
# `baseline-list-*` kinds and `baseline-missing` were RENAMED to what a DATA read can actually
# fail at (`baseline-unreadable`, `baseline-set-garbage`, `baseline-set-empty`,
# `baseline-decl-unrecognised`), and the three `manifest-*` kinds are new — a kind name that
# describes a `--list` run nobody performs any more is a false statement in a diagnostic. It was
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
# EMPIRICALLY, not by reading: overriding `ok()` to call `bad()` made EVERY case print FAIL and
# the tally match the case count exactly, with exit 1 (33 of 33 on the tree of the day; the set
# has grown since, and the PROPERTY is the claim, not the number) (a lexical paren scan cannot answer this
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

# fx <dir>: `cd` into a FIXTURE directory, REFUSING an empty or non-existent path.
#
# `cd ""` SUCCEEDS in bash and leaves you in the CURRENT directory. Combined with the fixture
# builders' `exit 1` — which, inside a `$( )`, exits only the SUBSTITUTION SUBSHELL and leaves
# the caller's variable EMPTY — every `( cd "$fixture" && git … )` in this file was one failed
# builder away from running IN THE LIVE CHECKOUT. Measured the hard way: an empty fixture path
# made `git remote set-url origin <a deliberately leaky test URL>` rewrite THIS repository's
# own origin, which then made the next gate run report an unmeasurable baseline about itself.
# Every fixture `cd` in this file goes through this, so that class is a loud failure instead of
# a silent mutation of the developer's tree.
# `builtin cd`, not a bare `cd`: this body is the one place a bare `cd "$…"` must remain, and
# the mechanical conversion that routed all 48 call sites through this helper REWROTE ITS OWN
# BODY into `fx "$1"` — infinite recursion, every fixture build failing at once. A guard must
# not match its own line. `builtin` also makes the helper immune to any later `cd` function.
fx() {
  if [ -z "${1:-}" ] || [ ! -d "$1" ]; then
    echo "FATAL: fixture path '${1:-}' is empty or not a directory — refusing to run in the CURRENT tree" >&2
    return 1
  fi
  builtin cd -- "$1"
}

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

# The shared canonical-identity pin (issue #3544 / job 225). Every fixture below uses a LOCAL
# bare `origin`, and a local path is deliberately NOT the canonical upstream, so each
# fixture's OWN COPY of the gate is rewritten to name its own origin — substituting the
# ARTIFACT, never adding a settable seam. See the helper's header for the full reasoning.
# shellcheck source=scripts/tests/lib/agent-gate-canonical-pin.sh
. "$SCRIPT_DIR/lib/agent-gate-canonical-pin.sh"

# ---------------------------------------------------------------------------
# Fixture builders.
#
# mkbaseline <name> <sed-program|-> -> echoes a BARE repo path serving as `origin`, whose
# `main` holds a copy of the real gate transformed by <sed-program> (`-` = verbatim).
# Copying ONLY the gate into <root>/scripts/ makes its `cd "$(dirname "$0")/.."` resolve
# REPO_ROOT to <root>, so every path the fixture touches stays inside this run's mktemp
# namespace.
# ---------------------------------------------------------------------------
# mkmanifest <repo-root> <mode> [literal-text] : install (or deliberately omit) the component
# manifest `scripts/agent-gate.components` that the pre-flight now reads as its DATA baseline
# (#3544 REQ-3544-01, replacing `bash <fetched gate> --list`).
#
#   derive  — the DEFAULT: generated from THAT fixture's OWN gate `--list`, so a fixture whose
#             sed changed the COMPONENTS array gets a manifest that matches it and the gate's
#             local staleness guard (manifest == COMPONENTS) passes. FAIL-CLOSED on a manifest
#             that comes out empty or implausibly short: a fixture whose manifest silently
#             failed to generate would report `manifest-*` in every case built on it, i.e. a
#             suite that tests nothing while looking busy.
#   none    — no manifest at all (drives `manifest-missing`, and the baseline's transitional
#             TEXT-extraction fallback).
#   literal — exact text, for the garbage/empty/stale shapes.
mkmanifest() {
  # SEPARATE `local` statements, deliberately: in ONE `local` a later assignment does NOT see
  # an earlier one in the same statement, so `out="$root/…"` read an UNBOUND `root` — under
  # `set -u` that killed the fixture builder's subshell and every fixture came back EMPTY.
  local root="$1" mode="$2" text="${3:-}"
  local out="$root/scripts/agent-gate.components"
  case "$mode" in
    none)    rm -f "$out"; return 0 ;;
    literal) printf '%s\n' "$text" >"$out"; return 0 ;;
    derive)
      # ONE implementation, shared with the delta and tree-integrity suites (which copy the
      # gate for their own reasons and hit the same pre-flight): see
      # agent_gate_install_components_manifest in lib/agent-gate-canonical-pin.sh. Two copies
      # of this rule is the drift this file keeps finding elsewhere.
      agent_gate_install_components_manifest "$root/scripts/agent-gate.sh" || return 1
      return 0 ;;
  esac
  return 1
}

mkbaseline() {
  local name="$1" prog="$2" work="$tmp/$1-src" bare="$tmp/$1.git"
  shift 2
  local man_mode=derive man_text=""
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --no-manifest)    man_mode=none ;;
      --manifest-lines) man_mode=literal; man_text="${2:-}"; shift ;;
      *) echo "FATAL: mkbaseline '$name': unknown flag '$1'" >&2; exit 1 ;;
    esac
    shift
  done
  mkdir -p "$work/scripts"
  if [ "$prog" = - ]; then
    cp "$GATE" "$work/scripts/agent-gate.sh"
  else
    sed "$prog" "$GATE" >"$work/scripts/agent-gate.sh"
  fi
  mkmanifest "$work" "$man_mode" "$man_text" \
    || { echo "FATAL: could not install the component manifest in baseline fixture '$name'" >&2; exit 1; }
  printf 'baseline fixture\n' >"$work/README.md"
  git init -q --bare "$bare" >/dev/null 2>&1
  # Point the bare repo's HEAD at `main` BEFORE any clone: with `init.defaultBranch`
  # still `master` (the git default on many boxes) a clone of this bare repo checks out
  # NOTHING, and the fixture's next commit becomes an UNRELATED ROOT COMMIT — which
  # silently converts every `--from-origin` fixture into the BEHIND shape and would make
  # the DECLARED and no-skew cases pass for the wrong reason (observed, first run).
  git -C "$bare" symbolic-ref HEAD refs/heads/main >/dev/null 2>&1
  ( fx "$work" \
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
  local name="$1" bare="$2" prog="$3" root="$tmp/$1"
  shift 3
  local from_origin="" man_mode=derive man_text=""
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --from-origin)    from_origin=--from-origin ;;
      --no-manifest)    man_mode=none ;;
      --manifest-lines) man_mode=literal; man_text="${2:-}"; shift ;;
      *) echo "FATAL: mkbranch '$name': unknown flag '$1'" >&2; exit 1 ;;
    esac
    shift
  done
  if [ "$from_origin" = --from-origin ]; then
    git clone -q "$bare" "$root" >/dev/null 2>&1 \
      || { echo "FATAL: could not clone baseline for branch '$name'" >&2; exit 1; }
  else
    mkdir -p "$root/scripts"
    printf 'branch fixture\n' >"$root/README.md"
    ( fx "$root" && git init -q . ) >/dev/null 2>&1
    [ "$bare" = - ] || ( fx "$root" && git remote add origin "$bare" ) >/dev/null 2>&1
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
  # PIN THE CANONICAL IDENTITY IN THIS COPY, before the commit so HEAD's copy carries it too
  # (the pre-flight reads HEAD's script for removal provenance). The pin names the fixture's
  # OWN local origin; `-` (no remote) still gets a pin, harmlessly, because the identity check
  # is never reached without a remote. FATAL on failure: an unpinned fixture would stop at the
  # pre-flight as `remote-not-canonical` and every case in this file would fail for a reason
  # that has nothing to do with what it tests.
  agent_gate_pin_canonical_remote "$root/scripts/agent-gate.sh" "$bare" \
    || { echo "FATAL: could not pin the canonical identity in branch fixture '$name'" >&2; exit 1; }
  # The manifest goes in BEFORE the commit, so HEAD carries it too: the pre-flight reads HEAD's
  # committed manifest for removal provenance, exactly as it reads the baseline's.
  mkmanifest "$root" "$man_mode" "$man_text" \
    || { echo "FATAL: could not install the component manifest in branch fixture '$name'" >&2; exit 1; }
  ( fx "$root" && git add -A && git "${GIT_ID[@]}" commit -qm branch ) >/dev/null 2>&1 \
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

# mk_ticker <script> <tickfile> <pidfile> <ignore-term> <nested> : write a fixture that
# ticks once a second until killed. <nested>: 0 = the script itself ticks; 1 = a GRANDCHILD
# ticks and the parent `wait`s for it; 2 = a grandchild ticks and the parent EXITS 0 AT ONCE —
# the SUCCESS-path shape (job 246), where the bound is not exceeded at all and the danger is a
# stray holding the caller's capture open forever.
# Every property here is a defect I caused and found:
#
#  * BOUNDED ITERATIONS. A leaked fixture whose PATH lacked `sleep` span at full speed and
#    wrote 179 MB into $TMPDIR before I noticed (37M ticks). The loop now stops after
#    MAX_TICKS, so a fixture the gate FAILS to kill still cannot fill a disk or hold a core.
#  * `sleep` VERIFIED, not assumed: these cases deliberately run under curated PATHs, and a
#    missing `sleep` is what turned the loop into a spin. Absent it, the fixture exits
#    immediately rather than busy-waiting — a case that then fails is honest (the fixture
#    could not model a long-lived process) where a spin is silent damage.
#  * A PID FILE, so a case cleans up by PID. Pattern-matching cleanup (`pkill -f`) hit my
#    OWN shell three times in this session, because the pattern text is in the command line
#    of the very command doing the matching.
mk_ticker() {
  local script="$1" tickfile="$2" pidfile="$3" ignore_term="$4" nested="$5"
  # `:` (a no-op), never the empty string: as an empty value this became a LEADING `;` in
  # the nested `sh -c` below, i.e. a syntax error, so the grandchild died instantly and the
  # case reported rc 0 with 0 ticks — a fixture that models nothing while looking fine.
  local body_ignore=":"
  [ "$ignore_term" = 1 ] && body_ignore='trap "" TERM'
  {
    printf '#!/bin/sh\n'
    printf 'command -v sleep >/dev/null 2>&1 || exit 3   # never busy-spin\n'
    if [ "$nested" = 1 ] || [ "$nested" = 2 ]; then
      # the PARENT takes TERM's default disposition and dies; the GRANDCHILD is the ticker
      printf 'sh -c '"'"'%s; echo $$ > "%s"; n=0; while [ "$n" -lt 600 ]; do echo tick >> "%s"; n=$((n+1)); sleep 1; done'"'"' &\n' \
             "$body_ignore" "$pidfile" "$tickfile"
      # nested=2 does NOT `wait`: the direct child exits 0 while the stray lives on. `exit 0`
      # is EXPLICIT rather than relying on the background job's status, because the property
      # under test is a SUCCESSFUL exit and an implicit one is a fact about `&`, not a claim.
      if [ "$nested" = 2 ]; then printf 'exit 0\n'; else printf 'wait\n'; fi
    else
      printf '%s\n' "$body_ignore"
      printf 'echo $$ > "%s"\n' "$pidfile"
      printf 'n=0; while [ "$n" -lt 600 ]; do echo tick >> "%s"; n=$((n+1)); sleep 1; done\n' "$tickfile"
    fi
  } >"$script"
  chmod +x "$script"
}

# reap_ticker <pidfile>: kill the fixture's own recorded PID, if it is still alive. BY PID,
# never by pattern (see mk_ticker). Silent when the gate already killed it, which is the
# expected case — this only stops a FAILING assertion from leaking a live process.
reap_ticker() {
  local pf="$1" pid
  [ -f "$pf" ] || return 0
  pid=$(cat "$pf" 2>/dev/null)
  case "$pid" in ''|*[!0-9]*) return 0 ;; esac
  kill -9 "$pid" 2>/dev/null || true
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
  ( fx "$repo" && bash "$repo/scripts/agent-gate.sh" --component-set-line "$mode" 2>/dev/null )
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
base_list=$( fx "$tmp/base-ok-src" && bash scripts/agent-gate.sh --list 2>/dev/null )
if grep -qx -- "$SENTINEL" <<<"$base_list" && [ "$(printf '%s\n' "$base_list" | wc -l)" -gt 30 ]; then
  ok "3544-fixture: the baseline fixture's --list really carries the sentinel component"
else
  bad "3544-fixture: the sentinel transformation did not change the baseline component set"
fi

# `--list` must exit at the arg-parse case, BEFORE the pre-flight — otherwise the
# baseline derivation would recurse (and, with an unreachable origin, could not answer at
# all). Proven where it is observable: a repo with a DEAD origin still lists fine.
dead_list_repo=$(mkbranch dead-list "$tmp/nonexistent-origin.git" - )
dl_out=$( fx "$dead_list_repo" && bash scripts/agent-gate.sh --list 2>/dev/null ); dl_rc=$?
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
# 3. FAILED BASELINE DERIVATION — now a DATA read, never an execution (#3544 REQ-3544-01).
#    Four shapes, each a FAIL that NAMES the derivation, never a fallback to an empty or
#    assumed baseline (which would excuse every branch: the vacuous pass this issue exists to
#    close, inverted).
#
#    WHAT CHANGED AND WHY THE SHAPES ARE DIFFERENT: the baseline set used to be derived by
#    running `bash <fetched gate> --list`, so the failure shapes were "the script exited
#    non-zero / printed nothing / printed prose". Nothing is executed any more, so the shapes
#    are the ones a DATA read has: an ungrammatical manifest, an empty one, a gate script whose
#    COMPONENTS declaration cannot be read AS TEXT, and neither file readable at all.
# ---------------------------------------------------------------------------
# 3a. the baseline has NO manifest and its COMPONENTS array has been REFLOWED across lines, so
#     the transitional TEXT extraction cannot read it. It must REFUSE — never guess at a
#     multi-line or computed array, and never fall back to executing the script.
base_reflow=$(mkbaseline base-reflow 's|^COMPONENTS=(file-size|COMPONENTS=(\
    file-size|' --no-manifest)
reflow=$(mkbranch reflow "$base_reflow" - )
rf_out=$(hook "$reflow")
rf_line=$(field COMPONENT_SET_LINE "$rf_out")
if [ "$(field VERDICT "$rf_out")" = UNMEASURED ] \
   && [ "$(field KIND "$rf_out")" = baseline-decl-unrecognised ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$rf_line" \
   && grep -q 'is not a SINGLE-LINE literal' <<<"$rf_line" \
   && grep -q 'scripts/agent-gate.components' <<<"$rf_line"; then
  ok "3544-baseline-decl-unrecognised: a manifest-less baseline whose COMPONENTS array is REFLOWED is REFUSED by name, not guessed at"
else
  bad "3544-baseline-decl-unrecognised: expected KIND baseline-decl-unrecognised naming the unreadable declaration"
  printf '%s\n' "$rf_out"
fi

# 3a-ii. THE FALLBACK'S POSITIVE CONTROL, and the path THIS PR's own gate runs on: the same
#     manifest-less baseline with its declaration INTACT must be measured correctly, from TEXT,
#     and must SAY that is what it did. Without this, 3a would be satisfied by a fallback that
#     never works at all.
base_decl=$(mkbaseline base-decl "$ADD_SENTINEL" --no-manifest)
decl=$(mkbranch decl "$base_decl" - )
dcl_out=$(hook "$decl")
if [ "$(field VERDICT "$dcl_out")" = BEHIND ] \
   && [ "$(field KIND "$dcl_out")" = ok ] \
   && [ "$(field BASELINE_SRC "$dcl_out")" = declaration ] \
   && grep -qw -- "$SENTINEL" <<<"$(field MISSING "$dcl_out")"; then
  ok "3544-baseline-src-declaration: a manifest-less baseline is measured from its declaration AS TEXT and names the missing component"
else
  bad "3544-baseline-src-declaration: expected KIND ok + BASELINE_SRC declaration + $SENTINEL missing"
  printf '%s\n' "$dcl_out"
fi

# …and the PRIMARY path is the manifest whenever the baseline has one. Asserted on its own
# because both paths produce the same VERDICT: a suite that could not tell them apart could not
# tell whether the manifest was ever read.
if [ "$(field BASELINE_SRC "$b_out")" = manifest ] \
   && [ "$(field KIND "$b_out")" = ok ]; then
  ok "3544-baseline-src-manifest: a baseline that HAS the manifest is read from it (data), not from the script text"
else
  bad "3544-baseline-src-manifest: expected BASELINE_SRC manifest for a manifest-carrying baseline (got '$(field BASELINE_SRC "$b_out")')"
fi

# mkgitshim <name> <mode> -> echoes a directory holding a `git` SHIM to put FIRST on PATH.
# Every other invocation is `exec`d to the real git, so the fixture is a real repository and the
# only difference is the ONE operation being made to fail.
#
# WHY A SHIM. The two cases below need a specific git READ to fail while the repository stays
# otherwise healthy — "the tree could not be read" and "the manifest blob could not be read".
# Neither is plantable in the repository itself: corrupting one object breaks everything, and
# nothing in a fixture can make `ls-tree` fail selectively. The shim is the same instrument case
# 5f uses, for the same reason, and it is honest about what it proves — the gate's DECISION given
# that observation, not the mechanism that produced it.
#
# `ls-tree` is matched as a whole ARGUMENT, never as a substring: the gate's tree-identity code
# uses `ls-files`, and a substring match would break the run for an unrelated reason.
mkgitshim() {
  # SEPARATE `local` statements, for the reason mkmanifest records: a later assignment in ONE
  # `local` does not see an earlier one, so `dir="$tmp/$name-gitbin"` read an UNBOUND `name` and
  # under `set -u` killed this builder's subshell — leaving the caller with an EMPTY dir, a PATH
  # of ":$PATH", NO shim, and two cases that passed the gate's normal path while claiming to
  # have planted a defect. Second instance of the same bug in this file; hence the comment.
  local name="$1" mode="$2"
  local dir="$tmp/$name-gitbin" real
  real=$(command -v git)
  [ -n "$real" ] || { echo "FATAL: mkgitshim needs a real git on PATH" >&2; exit 1; }
  mkdir -p "$dir"
  {
    printf '#!/bin/sh\n'
    printf 'REAL=%s\n' "$real"
    case "$mode" in
      fail-ls-tree)
        printf 'for a in "$@"; do if [ "$a" = ls-tree ]; then echo "gitshim: ls-tree refused" >&2; exit 128; fi; done\n' ;;
      fail-manifest-show)
        printf 'for a in "$@"; do case "$a" in *:scripts/agent-gate.components) echo "gitshim: manifest blob read refused" >&2; exit 128 ;; esac; done\n' ;;
      *) echo "FATAL: mkgitshim: unknown mode '$mode'" >&2; exit 1 ;;
    esac
    printf 'exec "$REAL" "$@"\n'
  } >"$dir/git"
  chmod +x "$dir/git"
  printf '%s\n' "$dir"
}

# ---------------------------------------------------------------------------
# 3a-iii / 3a-iv. WHICH READ PATH RUNS IS DECIDED BY AN AFFIRMATIVE THREE-VALUED MEASUREMENT,
#     NOT BY A FAILURE (lead ruling on REQ-3544-01). "The textual fallback is self-limiting —
#     unreachable once the manifest is on `main`" is true and NOT ENOUGH: it is a property
#     somebody reasoned about, and nothing measured it, so a refactor or a deleted manifest
#     would silently re-enable the brittle path. The pre-flight therefore probes the baseline's
#     tree with `git ls-tree` first, which — unlike `git show`'s non-zero exit — distinguishes
#     "the tree was read and does not list the manifest" from "the tree could not be read":
#       present         -> the manifest, and NOTHING ELSE. A failure here is an ERROR.
#       verified-absent -> the transitional TEXT extraction, NAMED in the line.
#       could-not-tell  -> REFUSE. Never the fallback.
#     Both non-`present` outcomes are driven here, because the whole value of the gating is that
#     `could-not-tell` and `verified-absent` behave DIFFERENTLY — a suite that exercised only one
#     of them would not distinguish this design from the one it replaced.
# ---------------------------------------------------------------------------
base_nofb=$(mkbaseline base-nofb - )
nofb=$(mkbranch nofb "$base_nofb" - )
nofb_ctl=$(hook "$nofb")

# 3a-iii. PRESENT BUT UNREADABLE IS AN ERROR, NOT A FALLBACK. The fixture's gate script carries a
#     perfectly good single-line COMPONENTS declaration, so path 2 WOULD have produced an answer
#     — that is what makes this a refusal rather than an incapacity, and the second control below
#     proves it by running the SAME shim against a manifest-LESS baseline, where path 2 is taken
#     and PASSes.
nofb_bin=$(mkgitshim nofallback fail-manifest-show)
nofb_out=$( fx "$nofb" && PATH="$nofb_bin:$PATH" bash "$nofb/scripts/agent-gate.sh" \
              --component-set-line full 2>/dev/null )
nofb_line=$(field COMPONENT_SET_LINE "$nofb_out")
decl_shim_out=$( fx "$decl" && PATH="$nofb_bin:$PATH" bash "$decl/scripts/agent-gate.sh" \
                   --component-set-line full 2>/dev/null )
if [ "$(field KIND "$nofb_ctl")" != ok ] || [ "$(field BASELINE_SRC "$nofb_ctl")" != manifest ]; then
  bad "3544-no-fallback-when-present: the POSITIVE CONTROL (same fixture, no shim) did not read the manifest cleanly (kind '$(field KIND "$nofb_ctl")', src '$(field BASELINE_SRC "$nofb_ctl")') — the case cannot discriminate"
elif [ "$(field KIND "$decl_shim_out")" != ok ] || [ "$(field BASELINE_SRC "$decl_shim_out")" != declaration ]; then
  bad "3544-no-fallback-when-present: the SECOND control failed — with the same shim, a manifest-LESS baseline must still be measured from its declaration (kind '$(field KIND "$decl_shim_out")', src '$(field BASELINE_SRC "$decl_shim_out")'), or the refusal below could be an incapacity rather than a decision"
elif [ "$(field VERDICT "$nofb_out")" = UNMEASURED ] \
   && [ "$(field KIND "$nofb_out")" = baseline-unreadable ] \
   && [ "$(field BASELINE_SRC "$nofb_out")" = "<none>" ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$nofb_line" \
   && grep -q 'the TEXT fallback is not taken when the manifest EXISTS' <<<"$nofb_line" \
   && ! grep -q 'TEXTUAL FALLBACK' <<<"$nofb_line" \
   && ! grep -q 'VERIFIED ABSENT' <<<"$nofb_line"; then
  ok "3544-no-fallback-when-present: a baseline whose manifest EXISTS but cannot be read is an ERROR and NEVER falls back to the script text (both controls: the manifest reads cleanly without the shim, and the same shim still lets a manifest-LESS baseline use the fallback)"
else
  bad "3544-no-fallback-when-present: expected KIND baseline-unreadable with no fallback"
  printf '%s\n' "$nofb_out"
fi

# 3a-iv. COULD-NOT-TELL IS A REFUSAL. `git show` cannot answer this question — its non-zero exit
#     conflates "no such path" with "bad object" with "unreadable repository" — which is why the
#     probe is `ls-tree` and why an unreadable TREE is its own kind rather than being folded into
#     `baseline-unreadable`: reading it as "absent" is precisely the two-valued-predicate error
#     (a predicate that must collapse "cannot tell" onto an answer always picks the permissive
#     one), and the permissive answer here silently re-enters the brittle textual path.
probe_bin=$(mkgitshim probefail fail-ls-tree)
probe_out=$( fx "$nofb" && PATH="$probe_bin:$PATH" bash "$nofb/scripts/agent-gate.sh" \
               --component-set-line full 2>/dev/null )
probe_line=$(field COMPONENT_SET_LINE "$probe_out")
if [ "$(field KIND "$nofb_ctl")" != ok ]; then
  bad "3544-manifest-probe-unmeasured: the POSITIVE CONTROL (same fixture, no shim) did not reach KIND ok — the case cannot discriminate"
elif [ "$(field VERDICT "$probe_out")" = UNMEASURED ] \
   && [ "$(field KIND "$probe_out")" = baseline-probe-unmeasured ] \
   && [ "$(field BASELINE_SRC "$probe_out")" = "<none>" ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$probe_line" \
   && grep -q 'ls-tree' <<<"$probe_line" \
   && grep -qF "'Cannot tell' is NOT 'absent'" <<<"$probe_line" \
   && ! grep -q 'TEXTUAL FALLBACK' <<<"$probe_line"; then
  ok "3544-manifest-probe-unmeasured: an unreadable baseline TREE is its own named refusal — 'cannot tell' never becomes 'absent', so the textual fallback is not entered"
else
  bad "3544-manifest-probe-unmeasured: expected KIND baseline-probe-unmeasured naming the probe, with no fallback"
  printf '%s\n' "$probe_out"
fi

# 3b. the baseline's manifest lists NOTHING (an empty baseline must never be accepted). A
#     comment-only file is the shape a truncation or a bad merge produces.
base_empty=$(mkbaseline base-empty - --manifest-lines '# every line here is a comment')
empty=$(mkbranch empty "$base_empty" - )
e_out=$(hook "$empty")
e_line=$(field COMPONENT_SET_LINE "$e_out")
if [ "$(field VERDICT "$e_out")" = UNMEASURED ] \
   && [ "$(field KIND "$e_out")" = baseline-set-empty ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$e_line" \
   && grep -q 'excuse' <<<"$e_line"; then
  ok "3544-baseline-empty: an EMPTY baseline manifest is a FAIL, never a set that excuses the branch"
else
  bad "3544-baseline-empty: expected KIND baseline-set-empty"
  printf '%s\n' "$e_out"
fi

# 3c. the baseline's manifest holds a line that is NOT a component name. A parser that skipped
#     unrecognised lines would silently SHRINK the baseline; the grammar is closed, and a
#     parser that TRIMMED would be guessing.
base_garbage=$(mkbaseline base-garbage - --manifest-lines "$(printf 'file-size\nCompiling cqlite v0.15.0')")
garbage=$(mkbranch garbage "$base_garbage" - )
g_out=$(hook "$garbage")
g_line=$(field COMPONENT_SET_LINE "$g_out")
if [ "$(field VERDICT "$g_out")" = UNMEASURED ] \
   && [ "$(field KIND "$g_out")" = baseline-set-garbage ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$g_line" \
   && grep -q 'Compiling' <<<"$g_line"; then
  ok "3544-baseline-garbage: a non-component line FAILs the derivation and is quoted back"
else
  bad "3544-baseline-garbage: expected KIND baseline-set-garbage quoting the offending line"
  printf '%s\n' "$g_out"
fi

# 3c-ii. …and the grammar refuses an UNTRIMMED name rather than trimming it. Stated as its own
#     case because "trim it" is the tempting fix and it is the one that turns a refusing parser
#     into a guessing one.
base_ws=$(mkbaseline base-ws - --manifest-lines "$(printf 'file-size\n  fmt')")
wsb=$(mkbranch wsbranch "$base_ws" - )
ws_out=$(hook "$wsb")
if [ "$(field KIND "$ws_out")" = baseline-set-garbage ] \
   && grep -q 'not a component name' <<<"$(field COMPONENT_SET_LINE "$ws_out")"; then
  ok "3544-baseline-untrimmed: a manifest line with leading whitespace is REFUSED, not trimmed"
else
  bad "3544-baseline-untrimmed: expected KIND baseline-set-garbage for an indented name"
  printf '%s\n' "$ws_out"
fi

# 3d. the baseline carries NEITHER the manifest NOR the gate script under scripts/
base_nofile=$(mkbaseline base-nofile - )
( fx "$tmp/base-nofile-src" && git rm -q scripts/agent-gate.sh scripts/agent-gate.components \
  && git "${GIT_ID[@]}" commit -qm "drop the gate and the manifest" \
  && git push -qf "$base_nofile" HEAD:refs/heads/main ) >/dev/null 2>&1
nofile=$(mkbranch nofile "$base_nofile" - )
nf_out=$(hook "$nofile")
nf_line=$(field COMPONENT_SET_LINE "$nf_out")
if [ "$(field KIND "$nf_out")" = baseline-unreadable ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$nf_line" \
   && grep -q 'scripts/agent-gate.sh' <<<"$nf_line" \
   && grep -q 'scripts/agent-gate.components' <<<"$nf_line"; then
  ok "3544-baseline-unreadable: a baseline with neither file FAILs, naming BOTH paths it looked for"
else
  bad "3544-baseline-unreadable: expected KIND baseline-unreadable naming both paths"
  printf '%s\n' "$nf_out"
fi

# ---------------------------------------------------------------------------
# 3e2. THE LOCAL MANIFEST IS THE VERIFIED HALF, AND THAT IS WHAT MAKES A MANIFEST BASELINE
#     TRUSTWORTHY AT ALL (#3544 REQ-3544-01). The pre-flight asserts THIS TREE's manifest
#     equals THIS gate's COMPONENTS array before it fetches anything: without that assert the
#     file is an unverified claim, and a branch that added a component to the array and not to
#     the manifest would — once merged — leave `main`'s manifest SHORT, so every later branch
#     would compare against a too-small baseline and silently excuse real skew.
#
#     Three kinds, each driven through the shipped code. The POSITIVE CONTROL for all three is
#     `behind` above: the SAME baseline with a DERIVED manifest reaches `KIND: ok`, so a
#     `manifest-*` verdict here cannot be a fixture that was broken some other way.
# ---------------------------------------------------------------------------
nomani=$(mkbranch nomanifest "$base_ok" - --no-manifest)
nm_out=$(hook "$nomani")
nm_line=$(field COMPONENT_SET_LINE "$nm_out")
if [ "$(field VERDICT "$nm_out")" = UNMEASURED ] \
   && [ "$(field KIND "$nm_out")" = manifest-missing ] \
   && [ "$(field SHA "$nm_out")" = "-" ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$nm_line" \
   && grep -q 'scripts/agent-gate.components' <<<"$nm_line" \
   && grep -q 'LOCAL component manifest' <<<"$nm_line"; then
  ok "3544-manifest-missing: a tree with no manifest FAILs closed BEFORE the fetch, naming the file (control: 'behind' reached KIND ok)"
else
  bad "3544-manifest-missing: expected KIND manifest-missing + no baseline sha"
  printf '%s\n' "$nm_out"
fi

badmani=$(mkbranch badmanifest "$base_ok" - --manifest-lines "$(printf 'file-size\nwarning: unused variable')")
bm_out=$(hook "$badmani")
bm_line=$(field COMPONENT_SET_LINE "$bm_out")
if [ "$(field KIND "$bm_out")" = manifest-garbage ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$bm_line" \
   && grep -q 'warning: unused variable' <<<"$bm_line"; then
  ok "3544-manifest-garbage: an ungrammatical local manifest FAILs closed, quoting the line it refused"
else
  bad "3544-manifest-garbage: expected KIND manifest-garbage quoting the offending line"
  printf '%s\n' "$bm_out"
fi

# STALE: the manifest parses and is well-formed but does NOT match the gate's own array. Built
# by DERIVING a correct manifest and then dropping one line, so the diagnostic must name that
# exact component — a bare "does not match" would not tell an author what to fix.
stalemani=$(mkbranch stalemanifest "$base_ok" - )
STALE_DROPPED=smoke
sm_pre=$( fx "$stalemani" && grep -cx -- "$STALE_DROPPED" scripts/agent-gate.components )
grep -vx -- "$STALE_DROPPED" "$stalemani/scripts/agent-gate.components" >"$tmp/stale-manifest.txt"
cp "$tmp/stale-manifest.txt" "$stalemani/scripts/agent-gate.components"
sm_out=$(hook "$stalemani")
sm_line=$(field COMPONENT_SET_LINE "$sm_out")
if [ "$sm_pre" -eq 1 ] \
   && [ "$(field KIND "$sm_out")" = manifest-stale ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$sm_line" \
   && grep -q 'missing from the manifest: '"$STALE_DROPPED" <<<"$sm_line"; then
  ok "3544-manifest-stale: a manifest out of step with COMPONENTS FAILs closed and NAMES the component it is missing"
else
  bad "3544-manifest-stale: expected KIND manifest-stale naming '$STALE_DROPPED' (fixture had it: $sm_pre)"
  printf '%s\n' "$sm_out"
fi

# …and REORDERING alone is stale too: the manifest claims to be what `--list` prints, and
# `--list` prints dispatch ORDER. Its own arm of the diagnostic, because "same names, different
# order" and "wrong names" are different repairs.
reordmani=$(mkbranch reorderedmanifest "$base_ok" - )
( fx "$reordmani" && bash scripts/agent-gate.sh --list 2>/dev/null | tail -1 >"$tmp/reord.txt" \
   && bash scripts/agent-gate.sh --list 2>/dev/null | sed '$d' >>"$tmp/reord.txt" ) >/dev/null 2>&1
cp "$tmp/reord.txt" "$reordmani/scripts/agent-gate.components"
ro_out=$(hook "$reordmani")
if [ "$(field KIND "$ro_out")" = manifest-stale ] \
   && grep -q 'DIFFERENT ORDER' <<<"$(field COMPONENT_SET_LINE "$ro_out")"; then
  ok "3544-manifest-reordered: a manifest with the same names in a different ORDER is stale, and says so"
else
  bad "3544-manifest-reordered: expected KIND manifest-stale naming the order"
  printf '%s\n' "$ro_out"
fi

# …and every manifest kind is ADVISORY under --lite, like every other verdict: the fast loop
# must not require the network OR a freshly regenerated manifest to function.
nm_lite=$(hook "$nomani" lite)
if [ "$(field STRICT "$nm_lite")" = no ] \
   && grep -q '^component-set: ADVISORY-UNMEASURED (#3544)' <<<"$(field COMPONENT_SET_LINE "$nm_lite")" \
   && grep -q 'manifest-missing' <<<"$(field COMPONENT_SET_LINE "$nm_lite")" \
   && ! grep -q 'FAIL-CLOSED' <<<"$(field COMPONENT_SET_LINE "$nm_lite")"; then
  ok "3544-manifest-lite: --lite stamps ADVISORY-UNMEASURED naming the manifest kind and does not fail on it"
else
  bad "3544-manifest-lite: expected ADVISORY-UNMEASURED naming manifest-missing under --lite"
  printf '%s\n' "$nm_lite"
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
mt_out=$( fx "$mt_repo" && PATH="$mt_stub:$PATH" bash "$mt_repo/scripts/agent-gate.sh" \
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
nt_control=$( fx "$nt_repo" && PATH="$nt_bin" bash "$nt_repo/scripts/agent-gate.sh" \
                --component-set-line full 2>/dev/null )
rm -f "$nt_bin/git"
if [ "$(field KIND "$nt_control")" != ok ]; then
  echo "skip - 3544-no-tool: the curated tool PATH cannot start the gate on this host (control KIND='$(field KIND "$nt_control")') — the no-tool branch is not exercisable here"
else
  nt_out=$( fx "$nt_repo" && PATH="$nt_bin" bash "$nt_repo/scripts/agent-gate.sh" \
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
  ( fx "$behind" && PATH="$1" bash "$behind/scripts/agent-gate.sh" --component-set-bound 2>/dev/null ) \
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
tickpid="$tmp/watchdog-ticker.pid"
mk_ticker "$ticker" "$tick" "$tickpid" 0 0
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
  echo "skip - 3544-bound-success-stray: same precondition (no outer host bound available)"
  echo "skip - 3544-bound-success-stray-control: same precondition (no outer host bound available)"
else
wd_rc_line=$( fx "$behind" && PATH="$bin_no_timeout" $wd_outer bash "$behind/scripts/agent-gate.sh" \
                --component-set-bounded-run 1 "$ticker" 2>/dev/null | sed -n 's/^RC: //p' )
ticks_at_return=$(wc -l <"$tick" | tr -d ' ')
sleep 3
ticks_later=$(wc -l <"$tick" | tr -d ' ')
if [ "$wd_rc_line" = 124 ] && [ "$ticks_later" = "$ticks_at_return" ]; then
  ok "3544-bound-enforced: the bash watchdog bounds a hanging command (rc 124) and leaves no live child"
else
  bad "3544-bound-enforced: expected rc 124 and a dead child (rc='$wd_rc_line' ticks $ticks_at_return -> $ticks_later)"
fi
reap_ticker "$tickpid"

# THE GRANDCHILD CASE (roborev job 210, finding 2). A bound that signals only its direct
# child is not a bound: the grandchild survives, keeps the command-substitution pipe open,
# and the "bounded" call never returns. Measured before the fix: direct-child TERM left the
# grandchild ticking 2 -> 5; the process-group signal froze it at 2 -> 2. The parent here
# spawns a ticker and `wait`s, which is the shape of a git transport helper.
gtick="$tmp/grandchild-tick.txt"
gparent="$tmp/grandchild-parent.sh"
gpid="$tmp/grandchild.pid"
mk_ticker "$gparent" "$gtick" "$gpid" 0 1
: >"$gtick"
g_rc_line=$( fx "$behind" && PATH="$bin_no_timeout" $wd_outer bash "$behind/scripts/agent-gate.sh" \
               --component-set-bounded-run 1 "$gparent" 2>/dev/null | sed -n 's/^RC: //p' )
g_at_return=$(wc -l <"$gtick" | tr -d ' ')
sleep 3
g_later=$(wc -l <"$gtick" | tr -d ' ')
if [ "$g_rc_line" = 124 ] && [ "$g_later" = "$g_at_return" ]; then
  ok "3544-bound-grandchild: a GRANDCHILD does not outlive the bound (process-group signal), and the call returns"
else
  bad "3544-bound-grandchild: expected rc 124 and a dead grandchild (rc='$g_rc_line' ticks $g_at_return -> $g_later)"
fi
reap_ticker "$gpid"

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
tipid="$tmp/term-ignoring.pid"
mk_ticker "$tignore" "$titick" "$tipid" 1 0
# mech_label <PATH> is only for the failure message; the assertion is on behaviour.
for _mech_path in "$PATH" "$bin_no_timeout"; do
  _mech=$(bound_of "$_mech_path")
  : >"$titick"
  _ti_rc=$( fx "$behind" && PATH="$_mech_path" $wd_outer bash "$behind/scripts/agent-gate.sh" \
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
  reap_ticker "$tipid"
done

# THE SHAPE THE CONDITIONAL KILL MISSED: the DIRECT CHILD dies on TERM while a GRANDCHILD
# ignores it. The previous escalation in the bash arm was guarded by `if kill -0 "$pid"` — the
# child's liveness — so it skipped the KILL exactly here and the "bounded" call hung forever. A
# fixture whose direct child ignores TERM (above) CANNOT discriminate this: `kill -0` is true
# there, so the conditional fires and the case passes with the defect present (measured —
# reverting the fix left that case green).
#
# THE ASSERTION IS ARM-SPECIFIC, AND IT IS SPLIT BECAUSE A MEASUREMENT SAID SO (job 246). This
# case used to require the descendant to STOP on BOTH arms, and it passed on both — but on the
# external arm it passed for the WRONG REASON: the suite's own harness bound was doing the
# killing, because a stray holding the capture pipe kept the substitution open until `timeout
# 30` fired and killed the whole group, and the two tick counts were then both taken AFTER that.
# Once the runner stopped handing the caller's pipe to the child (job 246), the substitution
# returns immediately and the truth is visible. MEASURED directly, not inferred:
#
#     timeout --kill-after=1 1 <parent that dies on TERM, TERM-ignoring grandchild>
#       -> rc 124 in ~1s, grandchild ticks 3 -> 6 (ALIVE)
#
# GNU `timeout` stops escalating the moment ITS OWN child exits, so it never reaches the KILL
# rung and never signals the group again. We cannot change that from here (the group id belongs
# to a process we did not fork), and forcing it — e.g. by running every bounded command under a
# `trap "" TERM` wrapper so timeout always has to escalate — would make every bounded `git`
# TERM-immune, which is a worse trade than a documented residual.
#
# So, per arm:
#   bash-watchdog — the descendant MUST STOP. That arm kills the process GROUP unconditionally
#                   after the grace period, which is precisely the fix job 214 landed.
#   timeout/gtimeout — the BOUND must hold (rc 124|137 and the call returns). A TERM-ignoring
#                   descendant MAY survive it; it inherits FILE descriptors, not the caller's
#                   pipe, so it can no longer break the bound — which is the property
#                   `3544-bound-success-stray` asserts directly. The stray is reaped by pid here.
#
# An odd but true consequence, recorded so nobody "fixes" it the wrong way: a host with NO
# `timeout` is STRICTER about descendant cleanup than one with it.
gtterm="$tmp/grandchild-term-ignoring.sh"
gttick="$tmp/grandchild-term-tick.txt"
gtpid="$tmp/grandchild-term.pid"
mk_ticker "$gtterm" "$gttick" "$gtpid" 1 1
for _mech_path in "$PATH" "$bin_no_timeout"; do
  _mech=$(bound_of "$_mech_path")
  : >"$gttick"
  _gt_rc=$( fx "$behind" && PATH="$_mech_path" $wd_outer bash "$behind/scripts/agent-gate.sh" \
              --component-set-bounded-run 1 "$gtterm" 2>/dev/null | sed -n 's/^RC: //p' )
  _gt_at=$(wc -l <"$gttick" | tr -d ' ')
  sleep 3
  _gt_later=$(wc -l <"$gttick" | tr -d ' ')
  case "$_gt_rc" in
    124|137) _gt_rc_ok=1 ;;
    *)       _gt_rc_ok=0 ;;
  esac
  if [ "$_mech" = bash-watchdog ]; then
    if [ "$_gt_rc_ok" -eq 1 ] && [ "$_gt_later" = "$_gt_at" ]; then
      ok "3544-bound-term-ignoring-grandchild[$_mech]: child dies on TERM, TERM-IGNORING grandchild still KILLed by the group signal (rc $_gt_rc, ticks frozen at $_gt_at)"
    else
      bad "3544-bound-term-ignoring-grandchild[$_mech]: expected rc 124|137 and a dead grandchild (rc='$_gt_rc' ticks $_gt_at -> $_gt_later)"
    fi
  else
    if [ "$_gt_rc_ok" -eq 1 ]; then
      ok "3544-bound-term-ignoring-grandchild[$_mech]: the BOUND holds and the call returns (rc $_gt_rc) — on this arm GNU timeout stops escalating when its own child exits, so the TERM-ignoring grandchild may survive (ticks $_gt_at -> $_gt_later); it holds no caller pipe, which 3544-bound-success-stray asserts"
    else
      bad "3544-bound-term-ignoring-grandchild[$_mech]: expected rc 124|137 (got '$_gt_rc') — the bound itself did not hold"
    fi
  fi
  reap_ticker "$gtpid"
done

# A HANGING `git` is bounded too — the composition that covers the partial-clone `git show`
# without a 120-second test: this proves the RUNNER bounds a hanging `git`, and the
# structural enumeration assert (case 9b) proves `git show` goes THROUGH that runner. Each
# half is cheap; together they are the property. Asserting it end-to-end would mean waiting
# out the real 120s bound, and a test nobody will run is not coverage.
ghang="$tmp/hanging-git.sh"
{ printf '#!/bin/sh\n'; printf 'exec sleep 300\n'; } >"$ghang"
chmod +x "$ghang"
gh_rc_line=$( fx "$behind" && PATH="$bin_no_timeout" $wd_outer bash "$behind/scripts/agent-gate.sh" \
                --component-set-bounded-run 1 "$ghang" 2>/dev/null | sed -n 's/^RC: //p' )
if [ "$gh_rc_line" = 124 ]; then
  ok "3544-bound-hanging-git: a hanging git-shaped command is bounded (rc 124), not waited on forever"
else
  bad "3544-bound-hanging-git: expected rc 124 (got '$gh_rc_line')"
fi

# ---------------------------------------------------------------------------
# THE SUCCESS PATH IS BOUNDED TOO (roborev job 246). Rounds 3 and 6 covered a TERM-ignoring
# descendant on the TIMEOUT path. This is the same family where the direct child SUCCEEDS: it
# exits 0 immediately, a background descendant lives on, and — before the fix — that descendant
# held the CALLER's command-substitution pipe open indefinitely, so a call that had already
# finished never returned and the 15/120s bound did not apply at all. Several call sites in the
# pre-flight are command substitutions, so this was reachable in the shipped code.
#
# HOW IT IS ASSERTED WITHOUT A WALL CLOCK (a wall-clock threshold in a correctness test is
# itself a defect class here, #2642): the discriminator is the OUTER harness bound's own EXIT
# STATUS. If the call returns on its own the outer `timeout` exits 0; if a stray holds the pipe
# the outer bound fires and exits 124. Both runs read the same `RC:` line, so nothing here
# compares elapsed time.
#
# AND THE CONTROL IS THE HALF THAT MAKES IT MEAN ANYTHING: the same fixture invoked DIRECTLY
# through a command substitution MUST time out. Without that, a green result is
# indistinguishable from a fixture whose stray died on its own — and this file has been bitten
# by exactly that shape more than once.
succ_outer="$wd_timeout_bin 8"
succ="$tmp/success-stray.sh"
succtick="$tmp/success-stray-tick.txt"
succpid="$tmp/success-stray.pid"
mk_ticker "$succ" "$succtick" "$succpid" 0 2

# succ_probe <outfile> <cmd...>: run <cmd> inside a bounded shell that captures it through a
# COMMAND SUBSTITUTION — the shape a real call site has — writing that shell's stdout to
# <outfile>, and return the OUTER BOUND's status (0 = the capture closed on its own, 124 = the
# harness had to kill it).
#
# THE BOUND MUST WRAP THE CAPTURING SHELL, NOT THE COMMAND, and getting that wrong hung the
# suite on the first cut: `timeout 8 <cmd>` returns the moment <cmd> exits, so a stray
# descendant then holds the substitution open with nothing left to kill it — the outer bound has
# already exited. Wrapping the shell that does the capturing means the deadline fires while the
# whole group (stray included) is still under the bound.
succ_probe() {
  local of="$1"; shift
  $succ_outer bash -c 'o=$("$@" 2>/dev/null); printf '"'"'%s\n'"'"' "$o"' _ "$@" >"$of" 2>/dev/null
}

: >"$succtick"
succ_probe "$tmp/succ-ctl.out" "$succ"; succ_ctl_rc=$?
reap_ticker "$succpid"
if [ "$succ_ctl_rc" = 124 ]; then
  ok "3544-bound-success-stray-control: the fixture really holds a capture open past its own exit (a direct substitution had to be killed by the harness bound)"
else
  bad "3544-bound-success-stray-control: the fixture does not model a pipe-holding stray (outer rc='$succ_ctl_rc', expected 124) — the case below cannot discriminate"
fi

: >"$succtick"
succ_probe "$tmp/succ.out" bash "$behind/scripts/agent-gate.sh" --component-set-bounded-run 5 "$succ"; succ_rc=$?
succ_line=$(sed -n 's/^RC: //p' "$tmp/succ.out" 2>/dev/null)
if [ "$succ_ctl_rc" = 124 ] && [ "$succ_rc" = 0 ] && [ "$succ_line" = 0 ]; then
  ok "3544-bound-success-stray: a bounded call whose child exits 0 RETURNS with a stray descendant still alive (rc 0, harness bound never fired) — the child's streams go to FILES, so nothing it leaves behind can hold the caller open"
else
  bad "3544-bound-success-stray: expected the capture to close on its own (outer rc='$succ_rc' want 0, reported RC='$succ_line' want 0, control rc='$succ_ctl_rc' want 124)"
fi
reap_ticker "$succpid"
fi

# …and with NO mechanism at all the command must NOT RUN. This is the load-bearing half:
# reporting "unboundable" while still running the command unbounded would fix nothing.
nb_marker="$tmp/must-not-run-marker"
rm -f "$nb_marker"
nb_rc_line=$( fx "$behind" && PATH="$bin_no_bound" bash "$behind/scripts/agent-gate.sh" \
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
ub_control=$( fx "$behind" && PATH="$bin_no_timeout" bash "$behind/scripts/agent-gate.sh" \
                --component-set-line full 2>/dev/null )
if [ "$(field KIND "$ub_control")" != ok ]; then
  echo "skip - 3544-unboundable: the curated tool PATH cannot complete a probe on this host (control KIND='$(field KIND "$ub_control")') — the unboundable branch is not exercisable here"
else
  ub_out=$( fx "$behind" && PATH="$bin_no_bound" bash "$behind/scripts/agent-gate.sh" \
              --component-set-line full 2>/dev/null )
  ub_lite=$( fx "$behind" && PATH="$bin_no_bound" bash "$behind/scripts/agent-gate.sh" \
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
( fx "$tmp/base-rm-src" && printf 'moved on\n' >>README.md \
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
# 4b. THE DECLARED BYPASS (roborev job 215, blocker 2) — REPRODUCED, not theorised.
#     `ANCESTOR: yes` answers "is origin/main reachable from HEAD?"; DECLARED's sentence
#     claims "the removal is in THIS branch's own diff". Different questions — so with
#     origin/main an ancestor of HEAD, deleting one component from the WORKING COPY alone
#     (HEAD untouched) produced a NON-FATAL DECLARED in a CERTIFYING mode, i.e. a full gate
#     that would certify 35 of 36 components, AND a factually FALSE line asserting committed
#     provenance for an uncommitted edit.
#
#     The fixture is the repro exactly: a from-origin clone (ancestor yes) whose COMMITTED
#     gate has every component, with ONE component removed from the working copy AFTER the
#     commit. Fail-closed on its own construction — if the edit matches nothing the two
#     scripts are identical and the case would pass while testing nothing.
# ---------------------------------------------------------------------------
UNC_REMOVED=pub-surface
base_unc=$(mkbaseline base-unc - )
unc=$(mkbranch uncommitted "$base_unc" - --from-origin)
sed "/^COMPONENTS=(/ s/ $UNC_REMOVED / /" "$unc/scripts/agent-gate.sh" >"$tmp/unc-gate.sh"
if cmp -s "$unc/scripts/agent-gate.sh" "$tmp/unc-gate.sh"; then
  bad "3544-uncommitted: could not build the fixture — the COMPONENTS edit removing '$UNC_REMOVED' matched nothing, so the case would test nothing"
else
  cp "$tmp/unc-gate.sh" "$unc/scripts/agent-gate.sh"
  # THE MANIFEST MOVES WITH THE EDIT, uncommitted, and that is not fixture bookkeeping: the
  # pre-flight asserts the WORKING manifest equals the WORKING COMPONENTS array, so a fixture
  # that edited only the array would stop at `manifest-stale` and never reach the UNCOMMITTED
  # verdict this case exists for. Removing it from BOTH — and committing NEITHER — is the real
  # shape of the bypass: HEAD's committed manifest still lists the component.
  grep -vx -- "$UNC_REMOVED" "$unc/scripts/agent-gate.components" >"$tmp/unc-manifest.txt"
  cp "$tmp/unc-manifest.txt" "$unc/scripts/agent-gate.components"
  # The fixture's own precondition, asserted rather than assumed: the WORKING copy must no
  # longer list the component while HEAD's committed copy still does. Without this the case
  # could pass because the clone was broken rather than because the guard works.
  unc_wt_has=$( fx "$unc" && bash scripts/agent-gate.sh --list 2>/dev/null | grep -cx -- "$UNC_REMOVED" )
  unc_head_has=$( fx "$unc" && git show "HEAD:scripts/agent-gate.components" 2>/dev/null \
                    | grep -cx -- "$UNC_REMOVED" )
  unc_out=$(hook "$unc")
  unc_line=$(field COMPONENT_SET_LINE "$unc_out")
  if [ "$unc_wt_has" -eq 0 ] && [ "$unc_head_has" -ge 1 ] \
     && [ "$(field VERDICT "$unc_out")" = UNCOMMITTED ] \
     && [ "$(field KIND "$unc_out")" = ok ] \
     && [ "$(field ANCESTOR "$unc_out")" = yes ] \
     && grep -qw -- "$UNC_REMOVED" <<<"$(field MISSING "$unc_out")" \
     && [ "$(field HEAD_SRC "$unc_out")" = manifest ] \
     && grep -q 'FAIL-CLOSED (#3544)' <<<"$unc_line" \
     && grep -q 'UNCOMMITTED WORKING-TREE EDIT' <<<"$unc_line" \
     && grep -q 'PRESENT in the committed component set AT HEAD' <<<"$unc_line" \
     && grep -qw -- "$UNC_REMOVED" <<<"$unc_line" \
     && ! grep -q '^component-set: DECLARED' <<<"$unc_line" \
     && ! grep -q "own COMMITTED diff, NOT skew" <<<"$unc_line"; then
    ok "3544-uncommitted: an UNCOMMITTED removal under ANCESTOR yes FAILs closed and is NOT DECLARED (wt has it: $unc_wt_has, HEAD has it: $unc_head_has)"
  else
    bad "3544-uncommitted: expected VERDICT UNCOMMITTED naming $UNC_REMOVED with no DECLARED claim (wt=$unc_wt_has head=$unc_head_has)"
    printf '%s\n' "$unc_out"
  fi

  # The REAL full-gate emit: its own cause and its own remedy. `git rebase` is NOT the
  # remedy here (there is nothing to rebase) and the committed-provenance sentence must not
  # appear, so the block must name the uncommitted edit and say commit-or-restore.
  unc_sum="$tmp/uncommitted-summary.txt"
  ( fx "$unc" && AGENT_GATE_SUMMARY_FILE="$unc_sum" CQLITE_DATASETS_ROOT="$tmp/no-datasets" \
      bash scripts/agent-gate.sh >"$tmp/uncommitted.log" 2>&1 ); unc_rc=$?
  if [ "$unc_rc" -ne 0 ] \
     && grep -q '^RESULT: FAIL' "$unc_sum" 2>/dev/null \
     && grep -q '^preflight: FAIL (component-set: component(s) removed by an UNCOMMITTED working-tree edit' "$unc_sum" 2>/dev/null \
     && grep -q '^hint: commit the removal' "$unc_sum" 2>/dev/null \
     && ! grep -q 'hint: git fetch origin && git rebase' "$unc_sum" 2>/dev/null \
     && ! grep -q "own COMMITTED diff" "$unc_sum" 2>/dev/null; then
    ok "3544-uncommitted-emit: the FULL gate FAILs with the uncommitted-edit cause and a commit-or-restore remedy"
  else
    bad "3544-uncommitted-emit: expected an uncommitted-named preflight line + its own hint (rc=$unc_rc)"
    sed -n '1,20p' "$unc_sum" 2>/dev/null
  fi

  # …and ADVISORY under --lite, like every other verdict: the fast loop runs on a dirty tree
  # by definition, so a working-copy edit must be REPORTED there, never fatal.
  unc_lite=$(hook "$unc" lite)
  unc_lite_line=$(field COMPONENT_SET_LINE "$unc_lite")
  if [ "$(field STRICT "$unc_lite")" = no ] \
     && grep -q '^component-set: ADVISORY-UNCOMMITTED (#3544)' <<<"$unc_lite_line" \
     && grep -q -- '--lite is lenient' <<<"$unc_lite_line" \
     && ! grep -q 'FAIL-CLOSED' <<<"$unc_lite_line"; then
    ok "3544-uncommitted-lite: --lite stamps ADVISORY-UNCOMMITTED and does not fail on it"
  else
    bad "3544-uncommitted-lite: expected ADVISORY-UNCOMMITTED under --lite"
    printf '%s\n' "$unc_lite"
  fi
fi

# THE COUNTER-CASE, and the reason the fix keys on HEAD's SET rather than on "is the tree
# dirty": a dirty working tree that ADDS a component must still PASS. Extra components are
# never skew, and a guard that reds on a correct tree is the guard agents learn to waive —
# failing on dirtiness alone would red every branch mid-edit.
add=$(mkbranch uncommitted-add "$base_unc" - --from-origin)
sed "/^COMPONENTS=(/ s/^COMPONENTS=(/COMPONENTS=($SENTINEL /" "$add/scripts/agent-gate.sh" >"$tmp/add-gate.sh"
if cmp -s "$add/scripts/agent-gate.sh" "$tmp/add-gate.sh"; then
  bad "3544-uncommitted-add: could not build the fixture (the COMPONENTS edit matched nothing)"
else
  cp "$tmp/add-gate.sh" "$add/scripts/agent-gate.sh"
  # Same reason as the removal case: the working manifest must match the working array or the
  # run stops at `manifest-stale` instead of exercising the ADDITION path. The sed puts the
  # sentinel FIRST in the array, and the local check compares ORDER too, so it goes first here.
  { printf '%s\n' "$SENTINEL"; cat "$add/scripts/agent-gate.components"; } >"$tmp/add-manifest.txt"
  cp "$tmp/add-manifest.txt" "$add/scripts/agent-gate.components"
  add_out=$(hook "$add")
  add_line=$(field COMPONENT_SET_LINE "$add_out")
  if [ "$(field VERDICT "$add_out")" = PASS ] \
     && grep -qw -- "$SENTINEL" <<<"$(field EXTRA "$add_out")" \
     && grep -q '^component-set: PASS (' <<<"$add_line" \
     && ! grep -q 'FAIL-CLOSED' <<<"$add_line" \
     && ! grep -q 'UNCOMMITTED' <<<"$add_line"; then
    ok "3544-uncommitted-add: an UNCOMMITTED ADDITION still PASSes (dirtiness is not the signal; HEAD's SET is)"
  else
    bad "3544-uncommitted-add: expected PASS with $SENTINEL branch-only, never a dirty-tree failure"
    printf '%s\n' "$add_out"
  fi
fi

# 4b-ii. HEAD'S SET UNMEASURABLE: the provenance oracle is the SOLE evidence for DECLARED's
#     claim, so a run that cannot consult it must NOT excuse the removal. Forced by dropping
#     the gate script from the INDEX and committing (HEAD's tree no longer holds it) while
#     the working copy stays in place: `git show HEAD:scripts/agent-gate.sh` then fails for
#     real, through the shipped code path, with ancestry still `yes`.
base_hu=$(mkbaseline base-headmiss "$ADD_SENTINEL")
hu=$(mkbranch headmiss "$base_hu" - --from-origin)
# BOTH files leave the index: HEAD's set is read from the manifest FIRST and only falls back
# to the script text, so dropping the script alone would leave HEAD perfectly measurable and
# the case would assert nothing.
( fx "$hu" && git rm -q --cached scripts/agent-gate.sh scripts/agent-gate.components \
   && git "${GIT_ID[@]}" commit -qm "drop the gate and manifest from the index" ) >/dev/null 2>&1
hu_out=$(hook "$hu")
hu_line=$(field COMPONENT_SET_LINE "$hu_out")
if [ "$(field VERDICT "$hu_out")" = UNMEASURED ] \
   && [ "$(field KIND "$hu_out")" = head-set-unmeasured ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$hu_line" \
   && grep -q 'git show HEAD:scripts/agent-gate.sh' <<<"$hu_line" \
   && ! grep -q '^component-set: DECLARED' <<<"$hu_line" \
   && ! grep -q "own COMMITTED diff, NOT skew" <<<"$hu_line"; then
  ok "3544-head-unmeasured: an unreadable HEAD gate script is its own named non-PASS, never a DECLARED excusal"
else
  bad "3544-head-unmeasured: expected KIND head-set-unmeasured FAIL-CLOSED naming the git show"
  printf '%s\n' "$hu_out"
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
agent_gate_pin_canonical_remote "$ind/scripts/agent-gate.sh" "$base_ind" \
  || { echo "FATAL: could not pin the canonical identity in the indeterminate fixture" >&2; exit 1; }
# The manifest is WORKING-TREE state here (this fixture deliberately has no commit at all), and
# it is what the pre-flight verifies before it fetches: without it the run stops at
# `manifest-missing` and never reaches the ancestry branch this case is about.
mkmanifest "$ind" derive \
  || { echo "FATAL: could not install the component manifest in the indeterminate fixture" >&2; exit 1; }
( fx "$ind" && git init -q . && git remote add origin "$base_ind" ) >/dev/null 2>&1
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
( fx "$ind" && AGENT_GATE_SUMMARY_FILE="$ind_sum" CQLITE_DATASETS_ROOT="$tmp/no-datasets" \
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
( fx "$ind" && AGENT_GATE_SUMMARY_FILE="$ind_lsum" CQLITE_DATASETS_ROOT="$tmp/no-datasets" \
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
# 4d. A SHALLOW CLONE MAKES `--is-ancestor`'s rc 1 AMBIGUOUS (roborev job 227). rc 1 means
#     "not an ancestor" in a complete repository, but in a SHALLOW one it ALSO means "the
#     connecting history is not here" — so reading it as definitive reports a LEGITIMATE
#     committed removal in a shallow checkout as BEHIND: a false FAIL on correct input, the
#     class that teaches agents to waive a lane. It is the three-valued-predicate rule one
#     level in: rc ∉ {0,1} was already INDETERMINATE because "cannot tell" must not collapse
#     onto an answer, and now rc 1 itself has that shape.
#
#     THE FIXTURE IS THE REAL SHAPE, not a stub. The baseline's history is C1..C3 (component
#     present) then C4..C5 (removed); both clones are taken at C5, then the origin's `main` is
#     moved BACK to C3. C3 therefore genuinely IS an ancestor of HEAD — proven by the FULL
#     clone's own `--is-ancestor` exiting 0 — while the depth-1 clone cannot see the link and
#     exits 1. The pair is the whole point: the SAME repository state yields DECLARED in the
#     complete clone and must not yield BEHIND in the shallow one.
# ---------------------------------------------------------------------------
sh_work="$tmp/shallow-src"; sh_bare="$tmp/shallow.git"
mkdir -p "$sh_work/scripts"
sed "$ADD_SENTINEL" "$GATE" >"$sh_work/scripts/agent-gate.sh"
printf 'shallow fixture\n' >"$sh_work/README.md"
git init -q --bare "$sh_bare" >/dev/null 2>&1
git -C "$sh_bare" symbolic-ref HEAD refs/heads/main >/dev/null 2>&1
(
  fx "$sh_work" && git init -q . \
    && mkmanifest "$sh_work" derive \
    && git add -A && git "${GIT_ID[@]}" commit -qm c1 \
    && printf 'c2\n' >>README.md && git "${GIT_ID[@]}" commit -qam c2 \
    && printf 'c3\n' >>README.md && git "${GIT_ID[@]}" commit -qam c3
) >/dev/null 2>&1 || { echo "FATAL: could not build the shallow fixture's early history" >&2; exit 1; }
sh_c3=$(git -C "$sh_work" rev-parse HEAD)
(
  fx "$sh_work" && cp "$GATE" scripts/agent-gate.sh \
    && mkmanifest "$sh_work" derive \
    && git "${GIT_ID[@]}" commit -qam "c4: remove the component" \
    && printf 'c5\n' >>README.md && git "${GIT_ID[@]}" commit -qam c5 \
    && git push -q "$sh_bare" HEAD:refs/heads/main
) >/dev/null 2>&1 || { echo "FATAL: could not build the shallow fixture's later history" >&2; exit 1; }
# `--depth` is IGNORED for a plain local path — git only honours it over a transport, so the
# clone uses `file://` and the remote is then repointed at the path (the identity pin below
# names the path). Without the `file://` the clone is COMPLETE and this case silently becomes
# a duplicate of 4 (measured: the first cut cloned by path and was not shallow at all).
git clone -q --depth 1 "file://$sh_bare" "$tmp/shallow-branch" >/dev/null 2>&1
git clone -q "$sh_bare" "$tmp/complete-branch" >/dev/null 2>&1
( fx "$tmp/shallow-branch" && git remote set-url origin "$sh_bare" ) >/dev/null 2>&1
# …and NOW move the baseline back to C3, so both clones hold C5 while origin/main names C3.
git -C "$sh_bare" update-ref refs/heads/main "$sh_c3" >/dev/null 2>&1
for _r in "$tmp/shallow-branch" "$tmp/complete-branch"; do
  agent_gate_pin_canonical_remote "$_r/scripts/agent-gate.sh" "$sh_bare" \
    || { echo "FATAL: could not pin the canonical identity in '$_r'" >&2; exit 1; }
done
# THE PRECONDITION PROBE MUST RUN AFTER THE OBJECT EXISTS. Measured: in the shallow clone
# `--is-ancestor C3 HEAD` exits 128 while C3 is simply ABSENT, and 1 once a fetch has brought
# it — 128 is a missing OBJECT (a different failure, already INDETERMINATE) and 1 is the
# AMBIGUOUS answer this case is about. The gate's own fetch is what brings it, so the probe
# fetches the same ref first; verified not to deepen the clone (rc stays 1).
for _r in "$tmp/shallow-branch" "$tmp/complete-branch"; do
  git -C "$_r" fetch --quiet --refmap= --no-tags origin \
      "refs/heads/main:refs/heads/cs-baseline-probe" >/dev/null 2>&1 || true
done
sh_is_shallow=$(git -C "$tmp/shallow-branch" rev-parse --is-shallow-repository 2>/dev/null || echo unknown)
git -C "$tmp/shallow-branch" merge-base --is-ancestor "$sh_c3" HEAD >/dev/null 2>&1; sh_rc=$?
git -C "$tmp/complete-branch" merge-base --is-ancestor "$sh_c3" HEAD >/dev/null 2>&1; cp_rc=$?
if [ "$sh_is_shallow" != true ] || [ "$sh_rc" -ne 1 ] || [ "$cp_rc" -ne 0 ]; then
  echo "skip - 3544-shallow-ancestry: this git does not reproduce the shape (shallow='$sh_is_shallow', shallow rc=$sh_rc want 1, complete rc=$cp_rc want 0) — the ambiguity is not exercisable here"
else
  sh_out=$(hook "$tmp/shallow-branch")
  sh_line=$(field COMPONENT_SET_LINE "$sh_out")
  cp_out=$(hook "$tmp/complete-branch")
  if [ "$(field VERDICT "$cp_out")" != DECLARED ]; then
    bad "3544-shallow-ancestry: the COMPLETE-clone control did not report DECLARED (got '$(field VERDICT "$cp_out")' kind '$(field KIND "$cp_out")') — the pair cannot discriminate"
    printf '%s\n' "$cp_out"
  elif [ "$(field VERDICT "$sh_out")" = INDETERMINATE ] \
     && [ "$(field KIND "$sh_out")" = ok ] \
     && [ "$(field ANCESTOR "$sh_out")" = unknown ] \
     && grep -q 'FAIL-CLOSED (#3544)' <<<"$sh_line" \
     && grep -q 'SHALLOW clone' <<<"$sh_line" \
     && grep -q -- 'git fetch --unshallow' <<<"$sh_line" \
     && ! grep -q 'is BEHIND origin/main' <<<"$sh_line"; then
    ok "3544-shallow-ancestry: in a SHALLOW clone an unreachable-but-real ancestor is INDETERMINATE naming the shallow clone, never a false BEHIND (complete-clone control: DECLARED)"
  else
    bad "3544-shallow-ancestry: expected INDETERMINATE naming the shallow clone (got '$(field VERDICT "$sh_out")')"
    printf '%s\n' "$sh_out"
  fi
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
n_components=$( fx "$same" && bash scripts/agent-gate.sh --list 2>/dev/null | wc -l | tr -d ' ' )
# The line names WHICH data-only read path certified it (#3544 REQ-3544-01) — asserted as part
# of the exact match, because a PASS whose baseline source is unstated cannot be audited: the
# transitional TEXT path is format-brittle and becomes unreachable once the manifest is on main.
if [ "$(field VERDICT "$s_out")" = PASS ] \
   && [ "$(field BASELINE_SRC "$s_out")" = manifest ] \
   && grep -q "^component-set: PASS ($n_components/$n_components vs origin/main $s_sha) — baseline read via the committed manifest$" <<<"$s_line"; then
  ok "3544-no-skew: an in-sync tree stamps an affirmative PASS naming its baseline sha AND the read path"
else
  bad "3544-no-skew: expected 'component-set: PASS ($n_components/$n_components vs origin/main $s_sha) — baseline read via the committed manifest'"
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
( fx "$tmp/base-fresh-src" && printf 'advanced after the clone\n' >>README.md \
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
# 5d. THE BASELINE COMES FROM A REF THIS RUN OWNS, NOT FROM `FETCH_HEAD` (roborev job 227).
#     `--refmap=` removed the shared TRACKING-ref write, which left `FETCH_HEAD` carrying the
#     baseline — and `FETCH_HEAD` is itself a single shared mutable file per repository, so a
#     CONCURRENT fetch (a sibling lane, a hook, an editor) overwrites it between the fetch and
#     the read. The pre-flight would then compare against — and EXECUTE — a commit other than
#     the `origin/main` it fetched, with nothing in the block to show it.
#
#     Simulated DETERMINISTICALLY rather than by racing: a stub `git` on PATH forwards every
#     call to the real git and, after any `fetch`, CLOBBERS `FETCH_HEAD` with a decoy commit
#     that exists in the fixture. That is the post-fetch window exactly. A run that reads
#     `FETCH_HEAD` reports the DECOY sha; a run that reads its own private destination ref
#     reports the true tip. (RED-verified against a copy patched back to `FETCH_HEAD`, which
#     reports the decoy.)
#
#     The same case pins the CLEANUP half: the private ref lives in `refs/worktree/*` (git's
#     per-worktree namespace, so it is not shared with a sibling lane at all) and must not
#     survive the run — a probe that leaks a ref per invocation is a slow leak in a shared
#     `.git`.
# ---------------------------------------------------------------------------
base_priv=$(mkbaseline base-priv - )
priv=$(mkbranch priv "$base_priv" - --from-origin)
priv_tip=$(git -C "$base_priv" rev-parse refs/heads/main)
priv_decoy=$(git -C "$priv" rev-parse HEAD)          # the fixture's own commit — a REAL object,
                                                     # so a FETCH_HEAD read resolves it and
                                                     # reports it rather than failing
priv_real_git=$(command -v git 2>/dev/null)
if [ -z "$priv_real_git" ] || [ "$priv_decoy" = "$priv_tip" ]; then
  echo "skip - 3544-private-fetch-ref: needs a resolvable git and a fixture commit distinct from origin/main's tip (decoy='$priv_decoy' tip='$priv_tip')"
else
  priv_bin="$tmp/priv-bin"; mkdir -p "$priv_bin"
  {
    printf '#!/bin/sh\n'
    printf '# stub git: forward everything, then emulate a CONCURRENT fetch clobbering FETCH_HEAD\n'
    printf '"%s" "$@"; rc=$?\n' "$priv_real_git"
    printf 'for a in "$@"; do\n'
    printf '  [ "$a" = fetch ] || continue\n'
    printf '  printf "%%s\\t\\tbranch (decoy) of origin\\n" "%s" > "%s/.git/FETCH_HEAD" 2>/dev/null || true\n' \
           "$priv_decoy" "$priv"
    printf '  break\n'
    printf 'done\n'
    printf 'exit $rc\n'
  } >"$priv_bin/git"
  chmod +x "$priv_bin/git"
  pv_out=$( fx "$priv" && PATH="$priv_bin:$PATH" bash "$priv/scripts/agent-gate.sh" \
              --component-set-line full 2>/dev/null )
  pv_sha=$(field SHA "$pv_out")
  pv_clobbered=$(git -C "$priv" rev-parse --verify --quiet 'FETCH_HEAD^{commit}' 2>/dev/null || echo none)
  pv_leaked=$(git -C "$priv" for-each-ref --format='%(refname)' 'refs/worktree/*' 2>/dev/null | grep -c . || true)
  # The stub must actually have clobbered FETCH_HEAD, or the case proves nothing: that is the
  # positive control for the simulation itself.
  if [ "$pv_clobbered" != "$priv_decoy" ]; then
    bad "3544-private-fetch-ref: the stub did NOT clobber FETCH_HEAD (found '$pv_clobbered', decoy '$priv_decoy') — the case cannot discriminate"
  elif [ "$pv_sha" = "$priv_tip" ] && [ "$pv_leaked" -eq 0 ]; then
    ok "3544-private-fetch-ref: a clobbered FETCH_HEAD does NOT change the baseline (reported the fetched tip), and the private refs/worktree ref is cleaned up"
  else
    bad "3544-private-fetch-ref: expected the fetched tip $priv_tip (got '$pv_sha'; decoy '$priv_decoy'), and no leaked refs/worktree ref (leaked=$pv_leaked)"
    printf '%s\n' "$pv_out"
  fi
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
# MEASURED, not assumed: with `--refmap=` and an explicit `main` refspec, a DEFAULT-config
# git writes NO tag — so a fixture left at defaults cannot tell `--no-tags` from its absence
# (verified: the tag ref set was empty both with and without the flag, and a first cut of
# this case therefore passed even with `--no-tags` REMOVED). Tag auto-following here is
# CONFIGURATION-dependent, and `remote.<name>.tagOpt = --tags` is the configuration that
# turns it on — under it the same fetch writes `refs/tags/*` into the SHARED ref store.
# So the fixture sets it explicitly: the case must exercise the shape the flag exists for,
# not the shape where nothing happens either way.
git -C "$tagged" config remote.origin.tagOpt --tags
# A NEW tag on the baseline, created AFTER the fixture cloned it — so an auto-following
# fetch would have something to write.
( fx "$tmp/base-tag-src" && git "${GIT_ID[@]}" tag -a v99.99.99-selftest -m 'tag the baseline' \
    && git push -q "$base_tag" refs/tags/v99.99.99-selftest ) >/dev/null 2>&1
tags_before=$(git -C "$tagged" for-each-ref --format='%(refname) %(objectname)' refs/tags | sort)
tg_out=$(hook "$tagged")
tags_after=$(git -C "$tagged" for-each-ref --format='%(refname) %(objectname)' refs/tags | sort)
upstream_tag=$(git -C "$base_tag" for-each-ref --format='%(refname)' refs/tags | grep -c 'v99.99.99-selftest')
if [ "$upstream_tag" -ge 1 ] \
   && [ "$(field KIND "$tg_out")" = ok ] \
   && [ "$tags_after" = "$tags_before" ] \
   && ! printf '%s\n' "$tags_after" | grep -q 'v99.99.99-selftest'; then
  ok "3544-no-tag-writes: the baseline fetch leaves shared refs/tags/* UNCHANGED even with tagOpt=--tags and a new upstream tag"
else
  bad "3544-no-tag-writes: expected an unchanged tag ref set (upstream_tag=$upstream_tag kind=$(field KIND "$tg_out"))"
  echo "   before: [$tags_before]"
  echo "   after:  [$tags_after]"
fi

# ---------------------------------------------------------------------------
# 5f. THE TRANSFER HOP IS VERIFIED, NOT TRUSTED (roborev jobs 242 + 246). The baseline is
#     fetched in an ISOLATED repository (global/system config neutralised, the validated URL in
#     a 0600 config) and then TRANSFERRED into this repository so the ancestry check and the
#     set read can proceed. That second hop reads THIS repo's config and so could itself be
#     redirected — which is why the sha the isolated hop observed is RE-ASSERTED against what
#     arrived, and a mismatch is its own fail-closed kind rather than a silently different
#     baseline.
#
#     `baseline-transfer-mismatch` had its census count bumped for it and NO behavioural case,
#     while this file's own header claims every non-`ok` kind is exercised — the claim exceeding
#     the check, which is the exact class this whole issue keeps fixing. So it is driven here.
#
#     HOW THE REDIRECT IS PLANTED, and why it is a `git` SHIM rather than a real
#     `url.*.insteadOf`: an insteadOf rewrite matches a URL PREFIX, and the scratch repository's
#     path carries a random `mktemp` suffix, so no rewrite can name it and still resolve to a
#     real repository. Writing one into shared git config is forbidden outright (a worktree
#     shares .git/config — this lane took `origin` out for four live lanes that way, #3617). The
#     shim therefore stands in for the redirect at the tool boundary: it performs hop 2 FOR REAL
#     and then repoints the destination ref at a DIFFERENT commit, which is precisely the
#     observable a redirected transfer produces. The assertion is about the gate's affirmative
#     comparison, which cannot tell the two apart — and must not.
# ---------------------------------------------------------------------------
base_mm=$(mkbaseline base-mismatch - )
mm=$(mkbranch mismatch "$base_mm" - )
# POSITIVE CONTROL FIRST: without the shim this fixture must reach a real verdict, or a
# `baseline-transfer-mismatch` below could be any other breakage wearing that name.
mm_ctl=$(hook "$mm")
mm_decoy=$(git -C "$mm" rev-parse HEAD 2>/dev/null)
mm_bin="$tmp/mismatch-bin"
mkdir -p "$mm_bin"
mm_real_git=$(command -v git)
{ printf '#!/bin/sh\n'
  printf 'REAL=%s\n' "$mm_real_git"
  # HOP 2 IS IDENTIFIED BY ITS REFSPEC, not by the scratch path (random) and not by the remote
  # (hop 2 has none): only hop 2 fetches FROM refs/csbaseline INTO the private per-run ref.
  printf 'dest=""\n'
  printf 'for a in "$@"; do case "$a" in refs/csbaseline:refs/worktree/*) dest=${a#refs/csbaseline:} ;; esac; done\n'
  printf 'if [ -n "$dest" ]; then\n'
  printf '  "$REAL" "$@" || exit $?\n'
  printf '  "$REAL" -C %s update-ref "$dest" %s || exit $?\n' "$mm" "$mm_decoy"
  printf '  exit 0\n'
  printf 'fi\n'
  printf 'exec "$REAL" "$@"\n'
} >"$mm_bin/git"
chmod +x "$mm_bin/git"
mm_out=$( fx "$mm" && PATH="$mm_bin:$PATH" bash "$mm/scripts/agent-gate.sh" \
            --component-set-line full 2>/dev/null )
mm_line=$(field COMPONENT_SET_LINE "$mm_out")
mm_base_sha=$(git -C "$base_mm" rev-parse refs/heads/main)
if [ "$(field KIND "$mm_ctl")" != ok ]; then
  bad "3544-transfer-mismatch: the POSITIVE CONTROL (same fixture, no shim) did not reach KIND ok (got '$(field KIND "$mm_ctl")') — the case cannot discriminate"
  printf '%s\n' "$mm_ctl"
elif [ "$mm_decoy" = "$mm_base_sha" ]; then
  bad "3544-transfer-mismatch: the decoy commit EQUALS the baseline sha, so the shim could not make the two hops disagree — the fixture would test nothing"
elif [ "$(field VERDICT "$mm_out")" = UNMEASURED ] \
   && [ "$(field KIND "$mm_out")" = baseline-transfer-mismatch ] \
   && [ "$(field SHA "$mm_out")" = "-" ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$mm_line" \
   && grep -qF "$mm_base_sha" <<<"$mm_line" \
   && grep -qF "$mm_decoy" <<<"$mm_line"; then
  ok "3544-transfer-mismatch: a transfer that delivers a DIFFERENT commit than the isolated hop observed is UNMEASURED/baseline-transfer-mismatch, naming BOTH shas (control reached KIND ok)"
else
  bad "3544-transfer-mismatch: expected KIND baseline-transfer-mismatch naming $mm_base_sha and $mm_decoy"
  printf '%s\n' "$mm_out"
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
( fx "$behind" && AGENT_GATE_SUMMARY_FILE="$sum" CQLITE_DATASETS_ROOT="$tmp/no-datasets" \
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
( fx "$same" && AGENT_GATE_SUMMARY_FILE="$sum2" CQLITE_DATASETS_ROOT="$tmp/no-datasets" \
    bash scripts/agent-gate.sh --delta HEAD~1 --anchor-run-id selftest >"$fout2" 2>&1 ) >/dev/null 2>&1
if grep -q "^component-set: PASS ($n_components/$n_components vs origin/main $s_sha) — baseline read via the committed manifest$" "$sum2" 2>/dev/null \
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
( fx "$behind" && AGENT_GATE_SUMMARY_FILE="$sum3" CQLITE_DATASETS_ROOT="$tmp/no-datasets" \
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
( fx "$behind" && AGENT_GATE_SUMMARY_FILE="$lsum" CQLITE_DATASETS_ROOT="$tmp/no-datasets" \
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
# 7d. THE OTHER KIND OF EMITTER: a MANUAL SUMMARY RENDERER (roborev job 215, blocker 1).
#     The census above keys on `emit_summary` / `_emit_terminal_summary` CALL SITES, so a
#     block assembled by hand — `echo "$SUMMARY_START_MARKER"` … `RESULT:` … end marker —
#     is INVISIBLE to it. That is not hypothetical: `_integrity_fail_block` is exactly such
#     a renderer (it is the #2874 no-clobber publish path, deliberately independent of
#     emit_summary), and it emitted a complete `RESULT: FAIL` block carrying NO
#     `component-set:` line. A guard whose subject set excludes a live emitter is the same
#     false assurance as #3544 itself, one level in.
#
#     So the derivation below covers the OTHER half, from the same source, with the same
#     rules: every `echo "$SUMMARY_START_MARKER"` in the gate is classified as
#       CANONICAL     it is INSIDE emit_summary / _emit_terminal_summary — the canonical
#                     renderer, whose call sites the census above already covers;
#       STAMPED       the block's own extent (or the 8 lines above it, where a stamp may be
#                     set up) mentions COMPONENT_SET_LINE / _component_set_meta;
#       EXEMPT        it carries `component-set-exempt: <reason>` (the startup INCOMPLETE
#                     sentinel, which predates the pre-flight, and the synthetic FOREIGN
#                     peer blocks the #2874 self-test seeds);
#       GAP           none of the above ⇒ FAIL naming the line.
#     A NEW hand-rolled block therefore lands in GAP with no edit to this file — which is
#     the property that stops this class recurring.
#
#     FAIL-CLOSED on its own derivation, twice over: zero renderer sites at all, or zero
#     CANONICAL/zero STAMPED among them, means the marker-echo shape changed and the scan is
#     no longer attributing anything — a clean census of nothing, never a pass. An
#     unterminated block (no end marker within a bounded window) is reported, not skipped.
# ---------------------------------------------------------------------------
MANUAL_AWK="$tmp/manual-census.awk"
cat >"$MANUAL_AWK" <<'MANUAL_PROG'
{ line[NR] = $0 }
END {
  for (i = 1; i <= NR; i++) {
    l = line[i]
    if (l ~ /^[ \t]*#/) continue
    if (l !~ /echo[ \t]+"\$SUMMARY_START_MARKER"/) continue
    # Enclosing function, BOUNDED by a top-level `}`. Without that bound a TOP-LEVEL
    # renderer (the startup sentinel) is attributed to whatever function happens to sit
    # above it — the same false-attribution defect the emit census hit at `nm=(`.
    fn = ""
    for (k = i - 1; k > 0; k--) {
      if (line[k] ~ /^\}/) break
      if (line[k] ~ /^[A-Za-z_][A-Za-z0-9_]*\(\) \{/) { fn = line[k]; sub(/\(\).*/, "", fn); break }
    }
    if (fn == "emit_summary" || fn == "_emit_terminal_summary") {
      printf "CANONICAL\t%d\t%s\n", i, fn; continue
    }
    # The block's extent: forward to its end marker, within a BOUNDED window (an
    # unterminated block is a reported defect, never a silently skipped site).
    end = 0
    for (j = i; j <= NR && j <= i + 120; j++) if (line[j] ~ /SUMMARY_END_MARKER/) { end = j; break }
    if (end == 0) { printf "UNTERMINATED\t%d\t%s\n", i, substr(l, 1, 60); continue }
    # A stamp counts only on a CODE line: a full-line COMMENT that merely NAMES
    # `_component_set_meta` is prose, and counting it made this very control report 0 GAPs
    # after the stamp was deleted (the comment above the renderer still mentioned it) — a
    # census satisfied by a sentence about the check rather than the check. The exempt
    # annotation is the opposite: it IS a comment, so it is read from comment lines.
    stamped = 0; exempt = 0
    for (j = i - 8; j <= end; j++) {
      if (j < 1) continue
      if (line[j] !~ /^[ \t]*#/ && line[j] ~ /COMPONENT_SET_LINE|_component_set_meta/) stamped = 1
      if (line[j] ~ /component-set-exempt:[ \t]*[^ \t]/) exempt = 1
    }
    if (stamped)      printf "STAMPED\t%d\t%s\n", i, fn
    else if (exempt)  printf "EXEMPT\t%d\t%s\n", i, fn
    else              printf "GAP\t%d\t%s\n", i, substr(l, 1, 60)
  }
}
MANUAL_PROG

manual_census() { awk -f "$MANUAL_AWK" "${1:-$GATE}"; }
man_out=$(manual_census)
man_sites=$(printf '%s\n' "$man_out" | grep -c '	')
man_gaps=$(printf '%s\n' "$man_out" | grep -c '^GAP	')
man_unterm=$(printf '%s\n' "$man_out" | grep -c '^UNTERMINATED	')
man_canon=$(printf '%s\n' "$man_out" | grep -c '^CANONICAL	')
man_stamped=$(printf '%s\n' "$man_out" | grep -c '^STAMPED	')
if [ "$man_sites" -eq 0 ] || [ "$man_canon" -eq 0 ] || [ "$man_stamped" -eq 0 ]; then
  bad "3544-manual-emitter: the renderer derivation found sites=$man_sites canonical=$man_canon stamped=$man_stamped in $GATE — the marker-echo shape changed or the scan broke (fail-closed: this is not a clean census)"
elif [ "$man_gaps" -eq 0 ] && [ "$man_unterm" -eq 0 ]; then
  ok "3544-manual-emitter: all $man_sites SUMMARY-block renderers account for the component-set line ($man_canon canonical, $man_stamped hand-rolled+stamped, rest exempt with a reason)"
else
  bad "3544-manual-emitter: $man_gaps hand-rolled SUMMARY block(s) neither stamp the component-set line nor carry 'component-set-exempt: <reason>' ($man_unterm unterminated):"
  printf '%s\n' "$man_out" | grep -E '^(GAP|UNTERMINATED)	' | while IFS='	' read -r _v _ln _src; do
    echo "   $_v line $_ln: $_src"
  done
fi

# POSITIVE CONTROL: strip the stamp out of the ONE hand-rolled renderer that carries it and
# require the derivation to report exactly that site as a GAP. Portable FIRST-MATCH deletion
# (awk with an exact string compare — `sed '0,/re/'` is a GNU extension BSD sed rejects).
man_ctl="$tmp/manual-control-gate.sh"
awk 'BEGIN { done = 0 }
     { if (!done && $0 == "  printf '"'"'%s\\n'"'"' \"$(_component_set_meta)\"") { done = 1; next }
       print }' "$GATE" >"$man_ctl"
if ! cmp -s "$GATE" "$man_ctl"; then
  man_ctl_gaps=$(manual_census "$man_ctl" | grep -c '^GAP	')
  if [ "$man_ctl_gaps" -eq 1 ]; then
    ok "3544-manual-emitter-control: removing the hand-rolled renderer's stamp reports exactly 1 GAP — the derivation is live, not inert"
  else
    bad "3544-manual-emitter-control: expected exactly 1 GAP with the stamp removed (got $man_ctl_gaps) — the derivation is not discriminating"
  fi
else
  bad "3544-manual-emitter-control: could not build the control (the stamp line did not match) — the derivation cannot be shown to discriminate"
fi

# …and BEHAVIOURALLY, because a source census cannot prove the line reaches the block. The
# #2874 `marker` hook drives the real no-clobber publish path (a foreign peer owns the
# contended path, a SIDE lane recorded a clobber) and prints the complete hand-rolled block
# on stdout. It runs pre-dispatch, so the honest value there is the NOT EVALUATED form —
# what matters is that the block carries the line EXACTLY ONCE and is never silent about it.
man_home="$tmp/manual-emit"
mkdir -p "$man_home/scripts"
cp "$GATE" "$man_home/scripts/agent-gate.sh"
man_block=$( fx "$man_home" && AGENT_GATE_SUMMARY_FILE="$man_home/sum.txt" \
               AGENT_GATE_INTEGRITY_SELFTEST=marker CQLITE_GATE_NO_NICE=1 \
               bash scripts/agent-gate.sh 2>/dev/null )
man_lines=$(printf '%s\n' "$man_block" | grep -c '^component-set: ')
if [ "$man_lines" -eq 1 ] \
   && grep -q '^summary-integrity: FAIL' <<<"$man_block" \
   && grep -q '^RESULT: FAIL' <<<"$man_block"; then
  ok "3544-manual-emitter-emit: the hand-rolled summary-integrity FAIL block carries the component-set line exactly once"
else
  bad "3544-manual-emitter-emit: expected exactly 1 'component-set:' line in the emitted integrity block (got $man_lines)"
  printf '%s\n' "$man_block" | sed -n '1,25p'
fi

# ---------------------------------------------------------------------------
# 7b. THE DETAIL IS FLATTENED, NOT MERELY REDACTED (roborev job 234). `git` ACCEPTS a remote
#     URL containing NEWLINES and `git remote get-url` returns them intact, so an origin URL
#     of "https://…/cqlite.git\nRESULT: PASS" put `RESULT: PASS` AT COLUMN ZERO inside a
#     block whose real verdict was FAIL — forging the exact token every reader, and the gate's
#     own documented completion probe (`grep -qE 'RESULT: (PASS|FAIL)'`), keys on. Redaction
#     (job 227) removed the CREDENTIAL from this value and left the NEWLINES: two properties
#     of one untrusted string, fixed a round apart.
# ---------------------------------------------------------------------------
inj_dir=$(mkbranch inject "$tmp/nonexistent-origin.git" - )
if ( fx "$inj_dir" && git remote set-url origin \
       "$(printf 'https://github.com/pmcfadin/cqlite.git\nRESULT: PASS\nfabricated: injected')" ) >/dev/null 2>&1; then
  inj_out=$(hook "$inj_dir")
  # A POSITIVE CONTROL for the fixture itself: if the pre-flight did not even reach the
  # not-canonical verdict, an absence of injected lines proves nothing.
  if [ "$(field KIND "$inj_out")" != remote-not-canonical ]; then
    bad "3544-detail-flattened: fixture did not reach remote-not-canonical (kind '$(field KIND "$inj_out")') — cannot discriminate"
  elif printf '%s\n' "$inj_out" | grep -qE '^(RESULT|fabricated):'; then
    bad "3544-detail-flattened: a newline in the origin URL INJECTED a line at column zero — a forged 'RESULT:' can defeat the summary probe"
    printf '%s\n' "$inj_out" | grep -nE '^(RESULT|fabricated):'
  else
    ok "3544-detail-flattened: newlines in the origin URL are flattened, so no injected line reaches column zero (redaction alone did not do this)"
  fi
else
  bad "3544-detail-flattened: could not build the newline-origin fixture"
fi

# ---------------------------------------------------------------------------
# 7c. THE PROBE BOUND IS MODE-DEPENDENT (roborev job 234). Lenient in the VERDICT is not
#     lenient in COST: `--lite` runs every fix round, and a stalled remote used to block it
#     for the full strict bound before printing an advisory result nobody fails on. Asserted
#     on the RESOLVED value, never on elapsed time — a wall-clock threshold would be flakier
#     AND would trip this repo's own wall-clock lint in `roborev-lints`.
# ---------------------------------------------------------------------------
# `noremote` is used deliberately: the bound is resolved at probe ENTRY, before anything
# network-capable runs, so a fixture whose probe short-circuits on "no origin" asserts the
# decision without a fetch — fast and deterministic.
b_full=$(field BOUND "$(hook "$noremote" full)")
b_lite=$(field BOUND "$(hook "$noremote" lite)")
if [ -z "$b_full" ] || [ -z "$b_lite" ]; then
  bad "3544-bound-per-mode: the hook did not report BOUND (full='$b_full' lite='$b_lite') — the probe cannot be asserted"
elif [ "$b_full" -gt "$b_lite" ] 2>/dev/null; then
  ok "3544-bound-per-mode: the strict bound (${b_full}s) exceeds the lenient one (${b_lite}s), so --lite cannot block for a certifying run's deadline"
else
  bad "3544-bound-per-mode: expected strict > lenient, got full=${b_full}s lite=${b_lite}s"
fi

# ---------------------------------------------------------------------------
# 7d. THE PRIVATE FETCH REF IS REGISTERED FOR CLEANUP *BEFORE* THE FETCH (roborev job 237).
#     A fetch can update the destination ref and then fail afterwards, so registering only on
#     success leaks a ref the drop helper cannot name.
#
#     THIS IS A SOURCE-ORDER ASSERT, AND THAT LIMITATION IS THE POINT OF SAYING SO: a
#     behavioural case would need a fetch that CREATES the ref and THEN fails, which is a race
#     to construct and would be a flaky test. So this checks the ORDERING that makes the leak
#     impossible, not the leak itself. It cannot catch a future refactor that keeps the order
#     but breaks the cleanup another way — `3544-fetch-ref-dropped` below covers the drop
#     itself. Two narrow checks, each honest about its half, beats one that implies more
#     coverage than it has.
# ---------------------------------------------------------------------------
fr_assign=$(grep -n '_CS_FETCH_REF="\$csref"' "$GATE" | head -1 | cut -d: -f1)
fr_fetch=$(grep -n 'fetch --quiet --refmap= --no-tags' "$GATE" | head -1 | cut -d: -f1)
if [ -z "$fr_assign" ] || [ -z "$fr_fetch" ]; then
  bad "3544-fetch-ref-registered-first: could not locate the assignment (got '$fr_assign') or the fetch (got '$fr_fetch') in $GATE — the shape changed or the scan broke (fail-closed: this is not a clean result)"
elif [ "$fr_assign" -lt "$fr_fetch" ]; then
  ok "3544-fetch-ref-registered-first: the private fetch ref is registered for cleanup (line $fr_assign) BEFORE the fetch that can create it (line $fr_fetch)"
else
  bad "3544-fetch-ref-registered-first: the fetch (line $fr_fetch) precedes the cleanup registration (line $fr_assign) — a fetch that creates the ref then fails would leak it into the SHARED .git"
fi

# The drop itself, behaviourally: a set ref name is deleted and the variable cleared.
fr_probe="$tmp/fetch-ref-drop"
mkdir -p "$fr_probe"
if ( fx "$fr_probe" && git init -q . && git commit -q --allow-empty -m x \
     && git update-ref refs/cqlite-fetchref-probe HEAD ) >/dev/null 2>&1; then
  if ( fx "$fr_probe" && git rev-parse --verify -q refs/cqlite-fetchref-probe >/dev/null \
       && git update-ref -d refs/cqlite-fetchref-probe \
       && ! git rev-parse --verify -q refs/cqlite-fetchref-probe >/dev/null ) >/dev/null 2>&1; then
    ok "3544-fetch-ref-dropped: update-ref -d removes a private ref (the mechanism the drop helper relies on), and deleting an absent ref is tolerated"
  else
    bad "3544-fetch-ref-dropped: update-ref -d did not remove the probe ref"
  fi
else
  bad "3544-fetch-ref-dropped: could not build the fetch-ref probe fixture"
fi

# ---------------------------------------------------------------------------
# 7e. EXTERNAL TEXT REACHING `_CS_DETAIL` IS BOTH REDACTED **AND** FLATTENED (job 239).
#     Three rounds, one value, two properties, fixed one at a time:
#       job 227 — the origin URL was rendered RAW               -> credential leak
#       job 234 — REDACTED but not flattened                    -> newline forged `RESULT: PASS`
#       job 239 — fetch stderr FLATTENED but not redacted        -> credential leak, one path over
#     The job-234 enumeration swept every interpolation for FLATTENING and never asked about
#     REDACTION — a single-property sweep where the obligation is the CROSS-PRODUCT of
#     sites x properties. Both cases below therefore assert BOTH properties on ONE input, so
#     neither can regress alone.
# ---------------------------------------------------------------------------
sd_in=$(printf 'fatal: unable to access '"'"'https://x-access-token:ghp_SECRET123@github.com/pmcfadin/cqlite.git/'"'"': failed\nRESULT: PASS')
sd_out=$(bash "$GATE" --component-set-safe-detail "$sd_in" 2>/dev/null | sed -n 's/^SAFE_DETAIL: //p')
if [ -z "$sd_out" ]; then
  bad "3544-detail-safe: the --component-set-safe-detail hook produced nothing — the sanitiser cannot be asserted (fail-closed)"
elif printf '%s' "$sd_out" | grep -q 'ghp_SECRET123'; then
  bad "3544-detail-safe: a CREDENTIAL survived into the detail text — it would reach the SUMMARY block agents paste into PR comments"
elif ! printf '%s' "$sd_out" | grep -q '<redacted>'; then
  bad "3544-detail-safe: no redaction marker in the detail text; the userinfo was dropped silently rather than visibly redacted"
elif printf '%s\n' "$sd_out" | grep -qE '^RESULT:'; then
  bad "3544-detail-safe: a newline in external text INJECTED a line at column zero — a forged 'RESULT:' defeats the summary probe"
else
  ok "3544-detail-safe: external text is BOTH redacted (credential gone, marker present) and flattened (no column-zero injection) in one pass"
fi

# ---------------------------------------------------------------------------
# 7g. THE VALIDATED URL IS PINNED INTO THE ISOLATED CONFIG, AND THE FETCH NAMES A REMOTE
#     (jobs 239 + 242). Two facts, one mechanism, and the case had to be REPLACED rather than
#     adjusted: its predecessor asserted the fetch passes `"$origin_url"` in argv, which round
#     11 deliberately stopped doing — a URL in a `git` argument is readable via `ps` and
#     /proc/<pid>/cmdline, and an accepted canonical URL may carry a token. So that assertion
#     had become ALWAYS-FALSE *and* directly contradicted `3544-url-not-in-argv` below: two
#     cases in one file demanding opposite things, which is how a suite starts teaching people
#     to edit assertions instead of code.
#
#     What must hold now: the exact bytes that PASSED VALIDATION are written into the isolated
#     repository's own config by a shell BUILTIN (no argv, no spawn), and the fetch refers to
#     them only by the REMOTE NAME. That closes the same time-of-check/time-of-use gap the old
#     assertion existed for — re-resolving `origin` at fetch time would let a peer's `git
#     config` write change what is fetched mid-run (#3617 is that incident) — while keeping the
#     credential out of every process listing.
#
#     SOURCE-SHAPE assert, stated as such: proving "not in any argv" behaviourally means
#     sampling /proc against a subprocess, which is a race, and proving the TOCTOU closed
#     behaviourally means mutating shared git config while a fetch is in flight, which is
#     precisely what must never be done from a test on a shared checkout.
# ---------------------------------------------------------------------------
cfg_write=$(grep -n 'url = %s' "$GATE" | head -1)
# LOCATED BY THE ISOLATED REPO PATH, not by the remote name: keying the locator on
# `csbaseline` made the two SPECIFIC arms below (a re-interpolated URL, a renamed remote)
# unreachable — every such mutation fell into the fail-closed "shape changed" arm instead, so
# the messages would have been dead branches and the case would have been right by luck.
# RED-verified after the change: each plant now reports its OWN cause.
fetch_line=$(grep -n 'git -C "\$csdir/repo" fetch' "$GATE" | head -1)
if [ -z "$cfg_write" ] || [ -z "$fetch_line" ]; then
  bad "3544-fetch-config-pinned: could not locate the config write (got '$(printf '%s' "$cfg_write" | cut -c1-40)') or the isolated fetch (got '$(printf '%s' "$fetch_line" | cut -c1-40)') in $GATE — the shape changed or the scan broke (fail-closed: this is not a clean result)"
elif ! printf '%s' "$cfg_write" | grep -q 'printf .*url = %s.*"\$origin_url"'; then
  bad "3544-fetch-config-pinned: the config write does not pass the VALIDATED \$origin_url through printf: $(printf '%s' "$cfg_write" | cut -c1-120)"
elif ! printf '%s' "$cfg_write" | grep -q '>>"\$csconf"'; then
  bad "3544-fetch-config-pinned: the URL is not written into the isolated \$csconf by redirection — a spawned writer would put it back in an argv: $(printf '%s' "$cfg_write" | cut -c1-120)"
elif printf '%s' "$fetch_line" | grep -q 'origin_url'; then
  bad "3544-fetch-config-pinned: the isolated fetch still interpolates the URL: $(printf '%s' "$fetch_line" | cut -c1-120)"
elif printf '%s' "$fetch_line" | grep -qE 'fetch --quiet --refmap= --no-tags csbaseline "refs/heads/main:refs/csbaseline"'; then
  ok "3544-fetch-config-pinned: the validated URL is pinned into the isolated config by a shell builtin, and the fetch names the remote 'csbaseline' with a literal refspec"
else
  bad "3544-fetch-config-pinned: the isolated fetch does not name the csbaseline remote with the literal refspec: $(printf '%s' "$fetch_line" | cut -c1-120)"
fi

# …and the 0600 must be applied BEFORE the URL is written, because the URL may carry a token:
# a mode change AFTER the write leaves a credential world-readable for the window in between.
# ORDER assert, which is the only thing a source read can answer here.
cfg_chmod_ln=$(grep -n 'chmod 600 "\$csconf"' "$GATE" | head -1 | cut -d: -f1)
cfg_write_ln=$(printf '%s' "$cfg_write" | cut -d: -f1)
if [ -z "$cfg_chmod_ln" ] || [ -z "$cfg_write_ln" ]; then
  bad "3544-config-mode-first: could not locate the chmod (got '$cfg_chmod_ln') or the URL write (got '$cfg_write_ln') — the shape changed or the scan broke (fail-closed)"
elif [ "$cfg_chmod_ln" -lt "$cfg_write_ln" ]; then
  ok "3544-config-mode-first: the isolated config is chmod 600 (line $cfg_chmod_ln) BEFORE the credential-bearing URL is written into it (line $cfg_write_ln)"
else
  bad "3544-config-mode-first: the URL write (line $cfg_write_ln) precedes the chmod (line $cfg_chmod_ln) — a token would be world-readable in between"
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
DECLARED_KIND_COUNT=18   # +baseline-probe-unmeasured (the three-valued manifest presence
                         # probe: "cannot tell" is REFUSED, never read as "absent"),
                         # +manifest-{missing,garbage,stale} +baseline-decl-unrecognised
                         # (#3544 REQ-3544-01); baseline-list-{failed,garbage,empty} and
                         # baseline-missing became baseline-{unreadable,set-garbage,set-empty}
                         # when the baseline stopped being derived by EXECUTING a script.
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
      # A LEADING `!` IS STRIPPED INSIDE THE LOOP, not once before it: `if ! VAR=x git init …`
      # leaves the `!` at the front only AFTER `if ` comes off, and a `!` is not a program name,
      # so the whole fragment was DISCARDED and the git call was invisible to this audit
      # (measured: `git init` in the isolated-fetch hop had never been audited). One more
      # instance of the same family this awk keeps finding — a strip whose ORDER decides
      # whether the check sees anything at all.
      while (t ~ /^(if|while|until|then|else|elif|do|not)[ \t]+/ || t ~ /^![ \t]*/ || t ~ /^[A-Za-z_][A-Za-z0-9_]*=[^ \t]*[ \t]+/) {
        sub(/^![ \t]*/, "", t)
        sub(/^([A-Za-z_][A-Za-z0-9_]*=[^ \t]*|[a-z]+)[ \t]+/, "", t)
      }
      split(t, w, /[ \t]/)
      cmd = w[1]
      if (cmd == "" || cmd !~ /^[a-z_:][a-z0-9_.:-]*$/) continue
      printf "EXT\t%s\n", cmd
      # `env -i VAR=… git …` (job 258): the ENVIRONMENT WRAPPER is not the command. `env` is
      # recorded above as the real external it is, and then this looks THROUGH it — otherwise
      # wrapping a call in `env` would silently EXEMPT the git behind it from the bound check,
      # which is the audit reporting a clean bill of health on a region it stopped parsing.
      if (cmd == "env") {
        sub(/^env[ \t]+/, "", t)
        while (t ~ /^-[iu][ \t]+/ || t ~ /^[A-Za-z_][A-Za-z0-9_]*=[^ \t]*[ \t]+/)
          sub(/^(-[iu]|[A-Za-z_][A-Za-z0-9_]*=[^ \t]*)[ \t]+/, "", t)
        split(t, w2, /[ \t]/)
        cmd = w2[1]
        if (cmd == "" || cmd !~ /^[a-z_:][a-z0-9_.:-]*$/) continue
        printf "EXT\t%s\n", cmd
      }
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
  # `sed` joined this set with _component_set_redact_text (job 239): a LOCAL text transform
  # with no network reach and no spawn, used to redact userinfo out of free text before it
  # reaches _CS_DETAIL. Classified rather than merely added — that classification is the whole
  # point of the list.
  # `chmod` (job 246): the 0600 applied to the isolated fetch config BEFORE the URL — which may
  # carry a credential — is written into it. A mode change on a path this pre-flight just
  # created: no network, no spawn, bounded work. LOCAL UTILITY.
  # `bash` and `mkdir` LEFT the set with #3544 REQ-3544-01: the two `bash <extracted gate>
  # --list` spawns are gone (the component set is read as DATA now) and with them the scripts/
  # directory those extractions needed. Removed rather than left in place, because a stale entry
  # here would silently pre-authorise a re-introduced spawn.
  # `env` (job 258): the ALLOWLISTED-ENVIRONMENT wrapper for every isolated git call — `env -i`
  # plus the entries _component_set_build_git_env admits. It is a LOCAL UTILITY that execs the
  # command it is given (no network of its own, no shell), and the audit program above looks
  # THROUGH it so the git behind it is still checked for its bound.
  declared_externals="basename cat chmod cut env git gtimeout kill mktemp rm sed sleep timeout tr true"
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
  # …and the same defect WRAPPED IN `env`, which is the shape job 258 introduced: if the audit
  # stopped at the wrapper, every isolated call would become unauditable at once.
  ctl_envwrap="$tmp/region-envwrap.sh"
  {   cat "$region"; printf 'run_probe() { env -i PATH="$PATH" git -C "$REPO_ROOT" fetch origin main >/dev/null 2>&1; }\n'; } >"$ctl_envwrap"
  ctl_gaps=$(awk -f "$GIT_AUDIT_AWK" "$ctl_unbounded" | grep -c '^GAP	')
  ctl_ann_gaps=$(awk -f "$GIT_AUDIT_AWK" "$ctl_annotated" | grep -c '^GAP	')
  ctl_curl_seen=$(awk -f "$GIT_AUDIT_AWK" "$ctl_curl" | sed -n 's/^EXT\t//p' | grep -cx curl)
  ctl_envwrap_gaps=$(awk -f "$GIT_AUDIT_AWK" "$ctl_envwrap" | grep -c '^GAP	')
  if [ "$ctl_gaps" -eq 1 ] && [ "$ctl_ann_gaps" -eq 0 ] && [ "$ctl_curl_seen" -ge 1 ] \
     && [ "$ctl_envwrap_gaps" -eq 1 ]; then
    ok "3544-no-unbounded-control: the audit reports a planted UNBOUNDED git (1), the same defect WRAPPED IN env (1), stays silent on an ANNOTATED one (0), and the census sees a planted network program — live in both directions"
  else
    bad "3544-no-unbounded-control: audit not discriminating (unbounded=$ctl_gaps expected 1, env-wrapped=$ctl_envwrap_gaps expected 1, annotated=$ctl_ann_gaps expected 0, curl seen=$ctl_curl_seen expected >=1)"
  fi
fi

# ---------------------------------------------------------------------------
# 10. THE BASELINE'S IDENTITY (roborev job 215, blocker 3). Before this the baseline was
#     trusted because a remote NAMED `origin` merely EXISTED — so `git remote set-url origin
#     <anything>` re-pointed the comparison: the env-var opt-out requirement 9 forbids,
#     reachable through git config instead. And it fires BY ACCIDENT in the documented fork
#     workflow, where `origin` legitimately names a contributor's fork whose `main` may be
#     months stale: the guard then compares against the WRONG baseline and stamps a PASS.
#
#     Covered in three layers:
#      (a) the PURE predicate over the URL shapes git accepts, through a REPORT-ONLY hook
#          that reads no config and cannot influence a real run's decision (the identity a
#          real run judges is always `git remote get-url origin`). No settable expected
#          identity exists — per CLAUDE.md the constrained party must not choose its own
#          enforcer. THE HOST IS PART OF THE IDENTITY (job 225): matching owner/repo alone
#          accepted `https://evil.example/pmcfadin/cqlite` and ANY LOCAL PATH ending in those
#          two segments, which compounds with this pre-flight EXECUTING the baseline's copy of
#          the gate. The fixtures therefore pin their own identity by rewriting the constant
#          in their SCRATCH COPY (see mkbranch), never by weakening the shipped check — the
#          first design made the fixtures work *because* the check was weak, i.e. the test
#          hook and the vulnerability were the same fact.
#      (b) END TO END against a real, fetchable, otherwise-IDENTICAL fork-shaped origin,
#          with a POSITIVE CONTROL: the same fixture whose origin URL is canonical must
#          PASS, so the non-PASS can only be the identity.
#      (c) an `origin` with no URL at all — a non-PASS of its OWN kind, never a silent pass.
# ---------------------------------------------------------------------------
identity() { # identity <url> -> canonical | not-canonical
  bash "$GATE" --component-set-remote-identity "$1" 2>/dev/null | sed -n 's/^IDENTITY: //p'
}
id_bad=""
# ACCEPT only what is VERIFIABLE FROM THE STRING: the legitimate spellings of the ONE
# canonical host — scheme forms, scp-like, userinfo, an ssh port, `www.`, a trailing `.git`,
# any case. Over-rejecting one of these would red a correct checkout, and a guard that reds on
# correct input is the guard agents learn to waive.
for _u in "https://github.com/pmcfadin/cqlite.git" \
          "https://github.com/pmcfadin/cqlite" \
          "https://github.com/pmcfadin/cqlite/" \
          "https://github.com:443/pmcfadin/cqlite.git" \
          "git@github.com:pmcfadin/cqlite.git" \
          "git@github.com:/pmcfadin/cqlite.git" \
          "ssh://git@github.com/pmcfadin/cqlite.git" \
          "ssh://git@github.com:22/pmcfadin/cqlite" \
          "git+ssh://git@github.com/pmcfadin/cqlite.git" \
          "ssh+git://git@github.com/pmcfadin/cqlite.git" \
          "https://x-access-token:ghp_example@github.com/pmcfadin/cqlite.git" \
          "HTTPS://WWW.GitHub.com/PMcFadin/CQLite.git/"; do
  [ "$(identity "$_u")" = canonical ] || id_bad="${id_bad:+$id_bad }REJECTED:$_u"
done
# …and REJECT everything whose host cannot be VERIFIED from the string, which is the half job
# 225 corrected. The first three are the ACCIDENT class (a fork, a same-owner different repo,
# another forge); the rest are the class an owner/repo-only match silently admitted — a
# hostile host, a look-alike host, an unresolvable ssh alias, a local mirror, a bare local
# path, an unknown scheme, and a non-numeric "port" that must not be guessed away. Under-
# rejecting any of these is the defect, and it compounds: the pre-flight EXECUTES the
# baseline's copy of the gate.
#
# The last four are the WHITESPACE class (roborev job 230), and they are the reason the
# normaliser refuses whitespace rather than stripping it: git resolves a remote whose scheme is
# not at byte ZERO as a LOCAL PATH, so a single leading space makes git read a local path while
# a stripping normaliser reads canonical HTTPS. They must be rejected for that reason and not
# merely be absent from the accept list — an "unparseable" verdict would pass this loop while
# still letting a later caller act on a stripped value.
for _u in "https://github.com/contributor/cqlite.git" \
          "git@github.com:pmcfadin/other-repo.git" \
          "https://gitlab.com/someone/cqlite-fork" \
          "https://evil.example/pmcfadin/cqlite" \
          "https://github.com.evil.tld/pmcfadin/cqlite" \
          "https://notgithub.com/pmcfadin/cqlite" \
          "mygithub:pmcfadin/cqlite" \
          "/data/mirrors/pmcfadin/cqlite.git" \
          "/tmp/anything/pmcfadin/cqlite" \
          "file:///tmp/x/pmcfadin/cqlite.git" \
          "ssh://git@github.com:notaport/pmcfadin/cqlite" \
          "http://github.com/pmcfadin/cqlite.git" \
          "git://github.com/pmcfadin/cqlite.git" \
          "ssh://git@github.com:2222/pmcfadin/cqlite" \
          "https://github.com:8443/pmcfadin/cqlite" \
          "/tmp/scratch/my-clone.git" \
          " https://github.com/pmcfadin/cqlite.git" \
          "	https://github.com/pmcfadin/cqlite.git" \
          "https://github.com/pmcfadin/cqlite.git " \
          "https://github.com/pmcfa din/cqlite.git"; do
  [ "$(identity "$_u")" = not-canonical ] || id_bad="${id_bad:+$id_bad }ACCEPTED:$_u"
done
if [ -z "$id_bad" ]; then
  ok "3544-remote-identity: every axis of the URL grammar has a rule — authenticated transports + pinned host + default port + exact path accepted; http/git/file, non-default ports, look-alike and unverifiable hosts, aliases, mirrors and local paths rejected"
else
  bad "3544-remote-identity: misclassified: $id_bad"
fi

# RELOCATED (job 242 verification): this block calls `identity()`, which is defined at the
# top of section 8. Placed earlier it read as a shell FUNCTION NOT YET DEFINED, so every
# call returned EMPTY and all five cases 'failed' in BOTH directions at once. That uniform
# both-ways failure is the tell of a broken instrument, not a broken subject — and the
# POSITIVE CONTROL below is what surfaced it: without it the three rejection cases would
# have looked like a real defect in the normaliser.
# ---------------------------------------------------------------------------
# 7f. THE `.git` SUFFIX IS STRIPPED AT MOST ONCE (job 239). Looping made
#     `pmcfadin/cqlite.git.git` normalise to the canonical repo, so a DIFFERENT path was
#     accepted as upstream — and the pre-flight EXECUTES the baseline. A normaliser must not
#     be more permissive than the grammar it implements.
# ---------------------------------------------------------------------------
sfx_bad=""
for _u in "https://github.com/pmcfadin/cqlite.git.git" \
          "git@github.com:pmcfadin/cqlite.git.git" \
          "https://github.com/pmcfadin/cqlite.git.git/"; do
  [ "$(identity "$_u")" = not-canonical ] || sfx_bad="${sfx_bad:+$sfx_bad }ACCEPTED:$_u"
done
# POSITIVE CONTROL: exactly one suffix, and none, must still be accepted — otherwise this
# case would pass just as well against a normaliser that rejects everything.
for _u in "https://github.com/pmcfadin/cqlite.git" \
          "https://github.com/pmcfadin/cqlite"; do
  [ "$(identity "$_u")" = canonical ] || sfx_bad="${sfx_bad:+$sfx_bad }REJECTED:$_u"
done
if [ -z "$sfx_bad" ]; then
  ok "3544-suffix-once: a repeated '.git.git' is NOT canonical, while one suffix and none still are"
else
  bad "3544-suffix-once: misclassified: $sfx_bad"
fi

# ---------------------------------------------------------------------------
# 7h. THE BASELINE PATH IS REWRITE-INDEPENDENT (roborev job 242). `url.<base>.insteadOf`
#     rewrites apply to an explicit URL, AND to `git remote get-url` — so before this fix a
#     peer's rewrite in the SHARED .git/config could redirect the fetch of a URL that had just
#     passed validation, and the fetched script is EXECUTED. Both the capture and the fetch now
#     run with global/system config neutralised.
#
#     The POSITIVE CONTROL is essential here and is the reason this case means anything: it
#     first proves the rewrite IS effective against a plain `git fetch`. Without that, a green
#     result would be indistinguishable from a rewrite that never applied.
# ---------------------------------------------------------------------------
rw_dir="$tmp/rewrite"; mkdir -p "$rw_dir"
printf '[url "/nonexistent/evil-redirect.git"]\n\tinsteadOf = https://github.com/pmcfadin/cqlite\n' >"$rw_dir/gitconfig"
# CONTROL: the rewrite must actually redirect a plain fetch, or this case proves nothing.
git init -q "$rw_dir/ctl" >/dev/null 2>&1
rw_ctl=$(GIT_CONFIG_GLOBAL="$rw_dir/gitconfig" git -C "$rw_dir/ctl" fetch --quiet --refmap= --no-tags "https://github.com/pmcfadin/cqlite.git" 'refs/heads/main:refs/x' 2>&1)
if ! printf '%s' "$rw_ctl" | grep -q 'evil-redirect'; then
  bad "3544-rewrite-immune: the POSITIVE CONTROL did not redirect — the rewrite is not effective in this environment, so this case cannot discriminate (control said: $(printf '%s' "$rw_ctl" | head -1 | cut -c1-80))"
else
  rw_out=$(GIT_CONFIG_GLOBAL="$rw_dir/gitconfig" bash "$GATE" --component-set-line full 2>/dev/null)
  if [ "$(field KIND "$rw_out")" = ok ]; then
    ok "3544-rewrite-immune: a hostile url.*.insteadOf redirects a plain fetch (control) yet the pre-flight is unaffected — capture and fetch both ignore rewrites"
  else
    bad "3544-rewrite-immune: a global insteadOf changed the pre-flight outcome (kind '$(field KIND "$rw_out")') — the baseline path is not rewrite-independent"
  fi
fi

# ---------------------------------------------------------------------------
# 7j. THE ISOLATED HOP RUNS IN AN ALLOWLISTED ENVIRONMENT (roborev job 258, High). Neutralising
#     `GIT_CONFIG_GLOBAL`/`GIT_CONFIG_SYSTEM` and stopping there left the "isolated" fetch
#     inheriting the REST of git's environment-config family — and a redirect of HOP 1 is worse
#     than a redirect of hop 2, because then the sha the isolated hop observes AND the commit
#     transferred here both come from the attacker: the equality assert compares two values that
#     AGREE and emits a **PASS**. A false PASS on skew detection is what this guard exists to
#     prevent.
#
#     THREE VECTORS, EACH WITH ITS OWN POSITIVE CONTROL, and the controls are the point of this
#     case: it asserts the gate is UNAFFECTED, and "unaffected" is indistinguishable from "the
#     override never applied" without proving the override works right here, right now, against
#     a plain `git fetch` in the same shape the gate uses (remote named `csbaseline`, URL in the
#     repository's own config).
#
#     ONE MEASURED TRAP, recorded because it nearly produced the opposite conclusion: injecting
#     `remote.csbaseline.url` does NOT redirect anything. Environment config is appended AFTER
#     the local file, `remote.<name>.url` is MULTI-VALUED, and `git fetch` uses the FIRST value —
#     so that control comes back "not redirected" and would have read as "the environment is
#     harmless". The vector that works is `url.<attacker>.insteadOf`, which REWRITES whatever URL
#     is resolved. A control that fails to reproduce the attack is not evidence of safety.
# ---------------------------------------------------------------------------
base_env=$(mkbaseline base-envfx - )
env_decoy=$(mkbaseline base-envdecoy 's|^# CQLite agent gate|# CQLite agent gate (decoy)|' )
env_fx=$(mkbranch envfx "$base_env" - )
env_real_tip=$(git -C "$base_env" rev-parse refs/heads/main)
env_decoy_tip=$(git -C "$env_decoy" rev-parse refs/heads/main)
env_tpl="$tmp/envfx-template"
mkdir -p "$env_tpl"
printf '[url "%s"]\n\tinsteadOf = %s\n' "$env_decoy" "$base_env" >"$env_tpl/config"

# env_ctl <label> <var=value>... : does this hostile environment redirect a PLAIN `git fetch`
# in the gate's own shape (a fresh repo, a remote named `csbaseline`, the URL in that repo's own
# config)? Echoes the sha the fetch landed on.
#
# BOTH the `init` and the `fetch` run under the hostile environment, uniformly for every vector:
# one of the three (`GIT_TEMPLATE_DIR`) lands its redirect at INIT time by seeding the new
# repository's local config, so a control that only wrapped the fetch would report "not
# reproducible" for it and silently downgrade that vector to untested.
env_ctl() {
  local label="$1"; shift
  local dir="$tmp/envctl-$label"
  rm -rf "$dir"
  env "$@" git init -q "$dir" >/dev/null 2>&1
  printf '[remote "csbaseline"]\n\turl = %s\n' "$base_env" >>"$dir/.git/config"
  env "$@" git -C "$dir" fetch -q --refmap= --no-tags csbaseline \
      "refs/heads/main:refs/csbaseline" >/dev/null 2>&1
  git -C "$dir" rev-parse refs/csbaseline 2>/dev/null || true
}

if [ "$env_real_tip" = "$env_decoy_tip" ]; then
  bad "3544-env-isolated: the decoy baseline has the SAME tip as the real one, so no vector below could be observed — the fixture would test nothing"
else
  for _vec in config-count config-parameters template-dir; do
    case "$_vec" in
      config-count)
        _ev=(GIT_CONFIG_COUNT=1 "GIT_CONFIG_KEY_0=url.$env_decoy.insteadOf" "GIT_CONFIG_VALUE_0=$base_env") ;;
      config-parameters)
        _ev=("GIT_CONFIG_PARAMETERS='url.$env_decoy.insteadOf'='$base_env'") ;;
      template-dir)
        # A TEMPLATE's `config` file IS copied into the new $GIT_DIR, so `git init` can be made
        # to seed the scratch repository's own LOCAL config — which global/system neutralisation
        # cannot touch. Measured: 1 insteadOf line landed in the fresh repo's config.
        _ev=("GIT_TEMPLATE_DIR=$env_tpl") ;;
    esac
    _ctl_sha=$(env_ctl "$_vec" "${_ev[@]}")
    _gate_out=$( fx "$env_fx" && env "${_ev[@]}" bash "$env_fx/scripts/agent-gate.sh" \
                   --component-set-line full 2>/dev/null )
    _gate_sha=$(field SHA "$_gate_out")
    _gate_kind=$(field KIND "$_gate_out")
    if [ "$_ctl_sha" != "$env_decoy_tip" ]; then
      bad "3544-env-isolated[$_vec]: the POSITIVE CONTROL did NOT redirect a plain git fetch (landed on '$_ctl_sha', decoy is '$env_decoy_tip') — this environment vector is not reproducible here, so the gate being unaffected proves nothing"
    elif [ "$_gate_kind" = ok ] && [ "$_gate_sha" = "$env_real_tip" ]; then
      ok "3544-env-isolated[$_vec]: the vector redirects a plain git fetch to the decoy (control) yet the pre-flight still measures the REAL baseline — the isolated hop's environment is built from an allowlist, not inherited"
    else
      bad "3544-env-isolated[$_vec]: the pre-flight was affected (kind '$_gate_kind', sha '$_gate_sha'; expected ok + $env_real_tip, decoy is $env_decoy_tip)"
      printf '%s\n' "$_gate_out"
    fi
  done
fi

# ---------------------------------------------------------------------------
# 7i. THE URL NEVER ENTERS ANY ARGV (job 242). An accepted canonical URL may carry a token, and
#     a URL in a `git` argument is readable via `ps` / /proc/<pid>/cmdline. SOURCE-SHAPE assert,
#     said plainly: proving absence from argv behaviourally would mean sampling /proc against a
#     subprocess, which is a race.
# ---------------------------------------------------------------------------
if grep -qE 'fetch[^|]*"\$origin_url"' "$GATE"; then
  bad "3544-url-not-in-argv: a git fetch interpolates \$origin_url into its arguments — a credential-bearing URL would be readable via ps and /proc/<pid>/cmdline"
elif ! grep -q 'url = %s' "$GATE"; then
  bad "3544-url-not-in-argv: no config-file write of the URL found — the shape changed or the scan broke (fail-closed: this is not a clean result)"
else
  ok "3544-url-not-in-argv: the URL reaches git through a 0600 config file written by a shell builtin, never through a process argument"
fi


# THE TRANSPORT AXIS, as its own case because its reason is different in KIND from the
# others: `http://` and `git://` authenticate nothing, and this pre-flight EXTRACTS AND RUNS
# the fetched repository's copy of the gate — so an on-path attacker who can impersonate
# `github.com` supplies arbitrary git objects and gets CODE EXECUTION, not merely a wrong
# baseline (roborev job 227, the High). The pair is asserted TOGETHER with their secure
# counterparts on the SAME host and path, so the case can only be about the transport.
tr_bad=""
for _pair in "https://github.com/pmcfadin/cqlite.git=canonical" \
             "http://github.com/pmcfadin/cqlite.git=not-canonical" \
             "git://github.com/pmcfadin/cqlite.git=not-canonical" \
             "ssh://git@github.com/pmcfadin/cqlite.git=canonical"; do
  _u="${_pair%=*}"; _want="${_pair##*=}"
  _got=$(identity "$_u")
  [ "$_got" = "$_want" ] || tr_bad="${tr_bad:+$tr_bad }$_u(want $_want got $_got)"
done
# …and the rejection must NAME THE AXIS, so the reader learns which rule was broken rather
# than "not canonical" for four different reasons.
tr_marker=$(bash "$GATE" --component-set-remote-identity "http://github.com/pmcfadin/cqlite.git" 2>/dev/null | sed -n 's/^NORMALISED: //p')
if [ -z "$tr_bad" ] && grep -q '^insecure-transport:' <<<"$tr_marker"; then
  ok "3544-transport-axis: unauthenticated http/git are rejected on the canonical host+path (the baseline is EXECUTED), and the rejection names the axis"
else
  bad "3544-transport-axis: misclassified: ${tr_bad:-none}; marker '$tr_marker'"
fi

# (b) END TO END. A fork-shaped origin holding the SAME history as the branch: fetchable,
# component sets identical, so nothing but the identity distinguishes it.
fk_work="$tmp/fork-src"
fk_bare="$tmp/fork-origin/contributor/cqlite.git"
mkdir -p "$fk_work/scripts" "$tmp/fork-origin/contributor"
cp "$GATE" "$fk_work/scripts/agent-gate.sh"
mkmanifest "$fk_work" derive \
  || { echo "FATAL: could not install the component manifest in the fork fixture" >&2; exit 1; }
printf 'fork fixture\n' >"$fk_work/README.md"
git init -q --bare "$fk_bare" >/dev/null 2>&1
git -C "$fk_bare" symbolic-ref HEAD refs/heads/main >/dev/null 2>&1
( fx "$fk_work" && git init -q . && git add -A && git "${GIT_ID[@]}" commit -qm fork \
  && git push -q "$fk_bare" HEAD:refs/heads/main ) >/dev/null 2>&1 \
  || { echo "FATAL: could not build the fork fixture" >&2; exit 1; }
git clone -q "$fk_bare" "$tmp/fork-branch" >/dev/null 2>&1
git clone -q "$fk_bare" "$tmp/fork-control" >/dev/null 2>&1
# The "upstream" is a BYTE COPY of the same bare repo at a different path, so the two runs
# differ in WHICH REMOTE `origin` NAMES and in nothing else — identical history, identical
# component set, both fetchable. BOTH copies of the gate are pinned to the UPSTREAM path, so
# the fork clone's own `origin` is a remote its gate does not recognise: exactly the shape of
# a contributor's fork, or of a re-pointed `origin`, against a shipped canonical identity.
fk_upstream="$tmp/fork-upstream/cqlite.git"
mkdir -p "$tmp/fork-upstream"
cp -R "$fk_bare" "$fk_upstream"
( fx "$tmp/fork-control" && git remote set-url origin "$fk_upstream" ) >/dev/null 2>&1
agent_gate_pin_canonical_remote "$tmp/fork-branch/scripts/agent-gate.sh" "$fk_upstream" \
  || { echo "FATAL: could not pin the fork fixture's gate copy" >&2; exit 1; }
agent_gate_pin_canonical_remote "$tmp/fork-control/scripts/agent-gate.sh" "$fk_upstream" \
  || { echo "FATAL: could not pin the fork control's gate copy" >&2; exit 1; }
fk_ctl=$(hook "$tmp/fork-control")
fk_out=$(hook "$tmp/fork-branch")
fk_line=$(field COMPONENT_SET_LINE "$fk_out")
if [ "$(field VERDICT "$fk_ctl")" != PASS ]; then
  bad "3544-remote-not-canonical: the POSITIVE CONTROL (same bare repo at a canonical path) did not PASS (got '$(field VERDICT "$fk_ctl")' kind '$(field KIND "$fk_ctl")') — the fixture cannot discriminate"
  printf '%s\n' "$fk_ctl"
elif [ "$(field VERDICT "$fk_out")" = UNMEASURED ] \
   && [ "$(field KIND "$fk_out")" = remote-not-canonical ] \
   && [ "$(field SHA "$fk_out")" = "-" ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$fk_line" \
   && grep -q 'remote-not-canonical' <<<"$fk_line" \
   && grep -q 'contributor/cqlite' <<<"$fk_line" \
   && grep -q 'fork-upstream/cqlite' <<<"$fk_line"; then
  ok "3544-remote-not-canonical: a re-pointed/fork origin is a NAMED non-PASS (control on the same repo at a canonical path PASSes)"
else
  bad "3544-remote-not-canonical: expected KIND remote-not-canonical FAIL-CLOSED naming both the actual and the expected identity"
  printf '%s\n' "$fk_out"
fi

# …ADVISORY under --lite (the fast loop must not require a canonically-pointed remote to
# function), and in the FULL gate the emitted block must carry the IDENTITY remedy — telling
# this reader to "restore access to origin/main" would send them to fix the wrong thing.
fk_lite=$(hook "$tmp/fork-branch" lite)
fk_sum="$tmp/fork-summary.txt"
( fx "$tmp/fork-branch" && AGENT_GATE_SUMMARY_FILE="$fk_sum" CQLITE_DATASETS_ROOT="$tmp/no-datasets" \
    bash scripts/agent-gate.sh >"$tmp/fork.log" 2>&1 ); fk_rc=$?
if [ "$(field STRICT "$fk_lite")" = no ] \
   && grep -q '^component-set: ADVISORY-UNMEASURED (#3544)' <<<"$(field COMPONENT_SET_LINE "$fk_lite")" \
   && grep -q 'remote-not-canonical' <<<"$(field COMPONENT_SET_LINE "$fk_lite")" \
   && [ "$fk_rc" -ne 0 ] \
   && grep -q '^RESULT: FAIL' "$fk_sum" 2>/dev/null \
   && grep -q '^hint: point origin at the canonical upstream' "$fk_sum" 2>/dev/null \
   && ! grep -q '^hint: restore access to origin/main' "$fk_sum" 2>/dev/null; then
  ok "3544-remote-not-canonical-modes: ADVISORY under --lite; the FULL gate FAILs with the identity-specific remedy"
else
  bad "3544-remote-not-canonical-modes: expected ADVISORY-UNMEASURED (lite) and an identity remedy in the full block (rc=$fk_rc)"
  printf '%s\n' "$fk_lite"; sed -n '1,20p' "$fk_sum" 2>/dev/null
fi

# (c) An `origin` that resolves to NO URL. Its own kind: the identity cannot be established,
# which is an unmeasurable baseline — never a pass, and never folded into "no origin at all",
# whose remedy is different.
git clone -q "$fk_upstream" "$tmp/nourl-branch" >/dev/null 2>&1
agent_gate_pin_canonical_remote "$tmp/nourl-branch/scripts/agent-gate.sh" "$fk_upstream" \
  || { echo "FATAL: could not pin the no-URL fixture's gate copy" >&2; exit 1; }
( fx "$tmp/nourl-branch" && git config --unset-all remote.origin.url \
   && git config --add remote.origin.url "" ) >/dev/null 2>&1
nu_out=$(hook "$tmp/nourl-branch")
nu_line=$(field COMPONENT_SET_LINE "$nu_out")
if [ "$(field VERDICT "$nu_out")" = UNMEASURED ] \
   && [ "$(field KIND "$nu_out")" = remote-unreadable ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$nu_line" \
   && grep -q 'returned no URL' <<<"$nu_line"; then
  ok "3544-remote-unreadable: an origin with no URL is its own named non-PASS, not a pass and not 'no remote'"
else
  bad "3544-remote-unreadable: expected KIND remote-unreadable FAIL-CLOSED naming the empty URL"
  printf '%s\n' "$nu_out"
fi

# (d) NO CREDENTIAL LEAK (roborev job 227). A remote URL legitimately carries a token —
# GitHub Actions rewrites `origin` to `https://x-access-token:<TOKEN>@github.com/…` — and this
# pre-flight renders the offending URL into stderr AND into the SUMMARY block, which this
# repository's workflow tells agents to PASTE INTO PR COMMENTS. So the leak path is the
# documented practice, which is why this outranks its severity label. Asserted in BOTH
# directions: the secret must appear in NEITHER stream, and `<redacted>` must appear in the
# block — absence alone would also be satisfied by a diagnostic that lost the URL entirely, or
# by a check that never ran.
leak_secret="s3cr3t-3544-must-not-appear"
leak=$(mkbranch leaky "$base_same" - --from-origin)
( fx "$leak" && git remote set-url origin "https://x-access-token:$leak_secret@evil.example/pmcfadin/cqlite.git" ) >/dev/null 2>&1
leak_sum="$tmp/leak-summary.txt"
( fx "$leak" && AGENT_GATE_SUMMARY_FILE="$leak_sum" CQLITE_DATASETS_ROOT="$tmp/no-datasets" \
    bash scripts/agent-gate.sh >"$tmp/leak.log" 2>&1 ); leak_rc=$?
leak_hook=$(hook "$leak")
if [ "$leak_rc" -ne 0 ] \
   && ! grep -qF "$leak_secret" "$leak_sum" 2>/dev/null \
   && ! grep -qF "$leak_secret" "$tmp/leak.log" 2>/dev/null \
   && ! grep -qF "$leak_secret" <<<"$leak_hook" \
   && grep -qF '<redacted>@evil.example' "$leak_sum" 2>/dev/null \
   && grep -q 'remote-not-canonical' "$leak_sum" 2>/dev/null; then
  ok "3544-no-credential-leak: a userinfo-bearing origin URL is REDACTED in the SUMMARY and never appears verbatim in the block, the log or the hook"
else
  bad "3544-no-credential-leak: the secret leaked, or the redacted form is absent (rc=$leak_rc)"
  grep -n 'component-set\|preflight' "$leak_sum" 2>/dev/null | head -4
fi

# STRUCTURAL: the expected identity must be a LITERAL. An env-derived (or config-derived)
# expected identity would be the same hole one level out — the constrained party choosing its
# own enforcer — and requirement 9's "no opt-out" would be satisfied only in spelling.
canon_assign=$(grep -n '^_CS_CANONICAL_REMOTE=' "$GATE")
if [ -n "$canon_assign" ] \
   && [ "$(printf '%s\n' "$canon_assign" | grep -c .)" -eq 1 ] \
   && grep -q '^_CS_CANONICAL_REMOTE="[A-Za-z0-9._/-]*"$' "$GATE" \
   && grep -q '^_CS_CANONICAL_REMOTE="[A-Za-z0-9.-]*\.[A-Za-z]*/[A-Za-z0-9._-]*/[A-Za-z0-9._-]*"$' "$GATE" \
   && ! grep -qE '_CS_CANONICAL_REMOTE=.*(\$\{|\$\(|git config)' "$GATE"; then
  ok "3544-canonical-literal: the expected upstream identity is a single hard-coded literal NAMING A HOST (no env/config/subshell source)"
else
  bad "3544-canonical-literal: expected exactly one literal <host>/<owner>/<repo> _CS_CANONICAL_REMOTE= assignment, got: $canon_assign"
fi

# …and the comparison must be EXACT. A suffix/prefix match is what admitted
# `evil.example/pmcfadin/cqlite` and every local path (job 225), and it is a one-character
# regression to reintroduce — so the shape is pinned structurally, where it is cheap, rather
# than left to the URL table alone (which can only ever cover the shapes someone thought of).
canon_pred=$(awk '/^_component_set_remote_is_canonical\(\) \{/,/^\}/' "$GATE")
if grep -q '\[ "\$(_component_set_normalise_remote "\$1")" = "\$_CS_CANONICAL_REMOTE" \]' <<<"$canon_pred" \
   && ! grep -q '\*"' <<<"$canon_pred" \
   && ! grep -q '"\*' <<<"$canon_pred"; then
  ok "3544-canonical-exact: the identity comparison is EXACT equality, with no suffix/prefix glob"
else
  bad "3544-canonical-exact: the predicate is not a plain equality against the literal:"
  printf '%s\n' "$canon_pred"
fi

printf '\n%s\n' "----------------------------------------"
printf 'passed: %d  failed: %d\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
