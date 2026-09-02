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
#   3757. HEAD is resolved UNPEELED in the live repository and PEELED in the isolated scratch
#      (#3757): the region's CODE lines mentioning `_cs_live_git` or `$REPO_ROOT` are pinned as
#      WHOLE LINES, both directions (structural, with TEN planted routes; no parser — see the pin
#      block for why the tokeniser was deleted), the scratch peel is bounded and three-valued
#      (structural, with a control), `$_CS_READ_DIR` never names the live checkout (structural +
#      two behavioural cases, one of which measures that NO live read happened), a FIFO at HEAD's
#      own object is refused BY NAME, and an unpeelable HEAD stays INDETERMINATE
#   5. no skew                                                          -> affirmative PASS + baseline sha
#   6. --lite with a real skew                                          -> line present, run NOT failed
#   7. the REAL full-gate emit path                                     -> FAIL block + exit 1, no cargo
#
# CENSUS, stated so a later reader can tell "covered" from "forgotten" (a silent gap is
# the shape this whole issue is about). The pre-flight has SIX verdicts and one non-`ok` probe
# kind per row below, and EVERY one is exercised here.
#
# THE KIND COUNT IS NOT REPEATED IN THIS PROSE, AND THAT IS THE FIX (roborev job 325, nit 5). It
# was written as three different numbers at once — "TEN", "(17)" and "NINETEEN" — over an
# enumeration that listed a fourth, while the file's own rule is that a census which miscounts its
# own list is worse than none: a reader who trusts the number and counts the entries concludes
# some kinds are uncovered extras. The count now lives in EXACTLY ONE place, `DECLARED_KIND_COUNT`
# near the end of this file, which is ASSERTED against the gate's own derived set at run time
# (`3544-kind-census`), so a new kind cannot arrive unannounced and prose cannot drift from it.
#   verdicts (6) — PASS (case 5), DECLARED (4), UNCOMMITTED (4b), BEHIND (1),
#                  INDETERMINATE (4c), UNMEASURED (2, 3a–3g, 4b-ii).
#   kinds        — fetch-failed, no-remote (case 2); baseline-decl-unrecognised (3a),
#                  baseline-set-empty (3b), baseline-set-garbage (3c/3c-ii),
#                  baseline-unreadable (3d); manifest-missing, manifest-garbage,
#                  manifest-stale (3e2); no-git, baseline-workspace, no-tool (3f);
#                  unboundable (3g);
#                  baseline-probe-unmeasured (3a-iv); baseline-ref-unparsable (3a-v);
#                  head-set-unmeasured (4b-ii); remote-not-canonical,
#                  remote-unreadable (10); repo-read-blocked (3a-iv-ter/quater,
#                  3757-head-object-fifo); read-dir-unisolated (3757-read-dir-unisolated).
# THE SET CHANGED SHAPE with #3544
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
# shellcheck source=scripts/tests/lib/agent-gate-canonical-pin.bash
. "$SCRIPT_DIR/lib/agent-gate-canonical-pin.bash"

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
      # agent_gate_install_components_manifest in lib/agent-gate-canonical-pin.bash. Two copies
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
  # `chmod` and `find` are in this list because the PRE-FLIGHT declares them (the 0600 on the
  # isolated fetch config, and the portable exact-mode verification of it — job 276). A curated
  # PATH missing a tool the gate legitimately needs does not test an absence branch; it makes the
  # POSITIVE CONTROL unreachable, and both cases that use this helper then SKIP with a misleading
  # cause (measured: they reported `baseline-workspace`).
  for t in bash sh sed awk grep cut tr mktemp date basename dirname cat head tail wc sort \
           uniq rm mkdir cp mv ln uname nproc env find touch stat comm od xargs kill ps df \
           readlink id iconv git sleep timeout gtimeout nice chmod; do
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

# THE PRE-FLIGHT REGION, EXTRACTED ONCE (#3757). Section 9b's unbounded-operation audit and
# #3757's live-call allowlist must not disagree about where the region starts and ends: two
# copies of the marker pair is the drift this file keeps finding elsewhere, and here a
# disagreement would silently move a call in or out of a guard's subject.
#
#   cs_region_stream <gate> [numbered|plain]   (default: numbered)
#
# `numbered` prefixes each line with its ORIGINAL line number and a colon — the `grep -n` shape —
# so a finding can name a line an author has to open. `plain` reproduces the region byte for byte,
# which is what an awk program run OVER the region needs.
CS_REGION_BEGIN='^# ---- issue #3544: component-set skew pre-flight'
CS_REGION_END='^# ---- issue #2081:'
cs_region_stream() {
  local gate="$1" mode="${2:-numbered}"
  awk -v b="$CS_REGION_BEGIN" -v e="$CS_REGION_END" -v m="$mode" '
    $0 ~ b { inr = 1 }
    $0 ~ e { inr = 0 }
    inr { if (m == "plain") print $0; else printf "%d:%s\n", NR, $0 }' "$gate"
}

# cs_hook_watchdog <repo> <mode> <secs>: the `--component-set-line` hook run under an INDEPENDENT
# OUTER WATCHDOG. Stdout is the hook's; the exit status is `timeout`'s, so 124/137 means the
# watchdog fired.
#
# WHY (roborev job 347, item 2): the FIFO cases exist to prove that a blocking read is BOUNDED. If
# that bounding regresses, the plain `$( … )` form HANGS FOREVER — the case never reaches its
# elapsed-time assertion, never restores the object it replaced, and the suite stops with no
# verdict. A test for a hang that itself hangs on failure is not a test. Expiration is a NAMED
# FAILURE at the call site, never a skip: it is the exact regression the case is for.
#
# `timeout` is the suite's own dependency elsewhere, but its absence is reported by the caller as a
# skipped PRECONDITION rather than assumed — an unbounded run is what this helper exists to avoid,
# so it must not silently fall back to one.
cs_hook_watchdog() {
  local repo="$1" mode="$2" secs="$3"
  ( fx "$repo" && timeout -k 5 "$secs" bash "$repo/scripts/agent-gate.sh" --component-set-line "$mode" 2>/dev/null )
}

# cs_region_code <gate>: the region's CODE lines only, numbered. The comment strip is ANCHORED at
# the start of the payload (`^<digits>:<space>*#`) — an UNANCHORED `:[[:space:]]*#` also matches a
# `#` anywhere later on the line, so a real call line carrying a trailing `# note: …` was dropped
# from the scan, which is a silent false PASS in every guard built on it (roborev job 325, nit 3).
cs_region_code() {
  cs_region_stream "$1" numbered | grep -v '^[0-9][0-9]*:[[:space:]]*#'
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
      lsremote-not-a-sha)
        printf 'for a in "$@"; do if [ "$a" = ls-remote ]; then printf "%%s\\t%%s\\n" "deadbeef-not-an-object-id" "refs/heads/main"; exit 0; fi; done\n' ;;
      lsremote-wrong-ref)
        printf 'for a in "$@"; do if [ "$a" = ls-remote ]; then printf "%%s\\t%%s\\n" "0000000000000000000000000000000000000000" "refs/heads/somewhere-else"; exit 0; fi; done\n' ;;
      # A WELL-FORMED sha256 OBJECT ID FOR THE RIGHT REF (job 309). This is the shape the two
      # other shims cannot produce: 64 lowercase hex characters and `refs/heads/main`, i.e. a
      # value the old grammar ACCEPTED. It must be refused, because the isolated scratch
      # repository is created in git's default object format and could not read it.
      # HANG ONLY AT `--is-shallow-repository`, so the earlier probes succeed and execution
      # actually REACHES `_component_set_is_shallow` (roborev job 314 asked for exactly this
      # coverage). A blocking config include cannot express it: it would stop the FIRST
      # repository read, and the probe would refuse before this helper is ever called.
      hang-is-shallow)
        printf 'for a in "$@"; do if [ "$a" = --is-shallow-repository ]; then sleep 90; fi; done\n' ;;
      lsremote-sha256)
        printf 'for a in "$@"; do if [ "$a" = ls-remote ]; then printf "%%s\\t%%s\\n" "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" "refs/heads/main"; exit 0; fi; done\n' ;;
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

# ---------------------------------------------------------------------------
# 3a-iv-bis. A BLOCKING CONFIG INCLUDE MUST REFUSE, NOT HANG (roborev job 312, Medium). Four
#     live-repository probes carried a `# local-only:` annotation justified against the NETWORK
#     ("touches no remote", "NAMES a remote; it does not contact one") and SILENT about blocking —
#     while the invariant this region claims is "there is NO branch on which it can hang". Every
#     git command reads the repository config, and `include.path` names a file git OPENS AND READS,
#     so a FIFO there never returns. `env -i` cannot help: only the ENVIRONMENT is sanitisable, and
#     a `.git/config` is a FILE (job 264's lesson). On this fleet every lane on a box is a worktree
#     of ONE shared `.git`, so the planter is a PEER LANE — a defect under the triage rule, whose
#     blast radius is every gate on the box, presenting as a gate that simply never finishes.
#
#     THE CASE IS THE MEASUREMENT, and it is bounded by construction: the fixture's own config gets
#     the include, so nothing outside the scratch tree is touched. The POSITIVE CONTROL is the same
#     fixture WITHOUT the include — without it a refusal proves nothing, since any breakage in a
#     fixture produces a non-`ok` kind. And the assertion is on the KIND, not merely on
#     "it returned": collapsing "the bound fired" onto `no-git`/`no-remote` would report a
#     confidently WRONG cause for a hang, which is the permissive-answer error one level down.
# ---------------------------------------------------------------------------
base_fifo=$(mkbaseline base-fifo - )
fifo_fx=$(mkbranch fifo-include "$base_fifo" - )
fifo_ctl=$(hook "$fifo_fx")
fifo_path="$tmp/fifo-include-blocker"
if ! mkfifo "$fifo_path" 2>/dev/null; then
  bad "3544-config-include-blocks: could not create a FIFO under \$tmp, so this case CANNOT be measured (fail-closed: not reported as a pass)"
elif [ "$(field KIND "$fifo_ctl")" != ok ]; then
  bad "3544-config-include-blocks: the POSITIVE CONTROL (same fixture, no include) did not reach KIND ok (got '$(field KIND "$fifo_ctl")') — the case cannot discriminate"
elif [ -z "$fifo_fx" ] || [ ! -d "$fifo_fx" ] || [ ! -d "$fifo_fx/.git" ] \
     || case "$fifo_fx" in "$tmp"/?*) false ;; *) true ;; esac; then
  # BLAST-RADIUS GUARD, AFFIRMATIVE AND BEFORE THE WRITE. `git -C "" config include.path <fifo>`
  # targets the CURRENT directory, and on this fleet that is a worktree of the SHARED
  # /data/lanes/repo/.git — so one empty variable would hang EVERY lane and EVERY concurrent gate
  # on the box, for as long as nobody notices. `fx` already refuses an empty path for exactly this
  # reason when it `cd`s; `git -C` had no such guard. So the target must be a non-empty DIRECTORY
  # holding its own `.git` and lying STRICTLY under $tmp — checked, not assumed.
  #
  # No separate "and it is not the real checkout" clause: the $tmp-prefix test already implies it,
  # and the obvious spelling (`$SCRIPT_DIR/../..`) is not canonicalised, so comparing it against an
  # absolute fixture path could never fire — a guard that reads as meaningful and cannot act is
  # worse than its absence, because it invites reliance it does not support.
  bad "3544-config-include-blocks: refusing to plant a blocking config include: fixture path '$fifo_fx' is not a git directory strictly under \$tmp ('$tmp') — writing include.path outside the scratch tree would hang every lane on this box"
else
  git -C "$fifo_fx" config include.path "$fifo_path"
  fifo_out=$( fx "$fifo_fx" && bash "$fifo_fx/scripts/agent-gate.sh" --component-set-line full 2>/dev/null )
  fifo_kind=$(field KIND "$fifo_out")
  fifo_line=$(field COMPONENT_SET_LINE "$fifo_out")
  # REMOVE THE FIFO **FIRST**, AND NEVER RUN ANOTHER `git config` HERE (measured: this cleanup
  # hung for 10m43s). `git config --unset include.path` must itself READ the config to rewrite it,
  # so it blocks on the very FIFO the case just planted — the case's own subject, one line down,
  # and `2>/dev/null || true` cannot rescue a HANG (it only handles a non-zero exit). Deleting the
  # FIFO is enough: git treats a MISSING include path as a silent no-op, and the fixture is a
  # throwaway under $tmp, so the stale `include.path` entry never needs unsetting at all.
  #
  # THE LESSON, because it is this file's own subject: reading a test tells you what it INTENDS to
  # measure, never that it TERMINATES. The assertion and the positive control were both right; the
  # cleanup was what never returned.
  rm -f "$fifo_path"
  case "$fifo_kind" in
    repo-read-blocked)
      if grep -q 'include.path' <<<"$fifo_line" && grep -q 'SHARED by every lane' <<<"$fifo_line"; then
        ok "3544-config-include-blocks: a config include.path naming a FIFO is REFUSED by name (repo-read-blocked) instead of hanging, and the detail names the mechanism AND that the config is shared"
      else
        bad "3544-config-include-blocks: refused as repo-read-blocked but the detail does not name include.path / the shared config: $fifo_line"
      fi ;;
    no-git|no-remote|baseline-workspace)
      bad "3544-config-include-blocks: the blocked read was reported as '$fifo_kind' — a two-valued collapse naming a WRONG cause for a hang" ;;
    *)
      bad "3544-config-include-blocks: expected KIND repo-read-blocked, got '$fifo_kind'"
      printf '%s\n' "$fifo_out" ;;
  esac
fi

# ---------------------------------------------------------------------------
# 3a-iv-ter. THE SHALLOWNESS PROBE IS BOUNDED TOO (roborev job 314, Medium) — the job-312 class in
#     a helper the job-312 fix did not reach. That fix converted the four live-repository calls in
#     the probe BODY; `_component_set_is_shallow` is defined ABOVE the body, so the enumeration
#     window that found the four could not see its two. Patching the reported sites and stopping
#     would have been defensible and wrong; a SWEEP of the whole file is what found them.
#
#     REACHING THE HELPER IS THE HARD PART, and it is why this case uses a git shim rather than a
#     config include: the helper is consulted ONLY when `merge-base --is-ancestor` answers rc 1, so
#     the fixture must be BEHIND — and a blocking `include.path` would stop the FIRST repository
#     read, refusing long before this code runs. The shim hangs on `--is-shallow-repository`
#     ALONE, so every earlier probe succeeds.
#
#     RUN UNDER THE LITE BOUND ON PURPOSE: 15s lenient vs 120s strict. The property under test is
#     "this read is bounded at all", which either bound demonstrates, and a case that costs two
#     minutes to prove a 15-second fact is a case people delete.
#
#     AND THE REFUSAL IS NAMED IN THE PARENT. The helper runs inside `$( … )`, so it cannot set
#     `_CS_KIND` — it prints a fourth token and the caller converts it. Mapping the blocked read
#     onto `unknown` would also have been fail-closed, but it would have blamed "shallowness could
#     not be determined" for a poisoned config: a true refusal with a misleading cause.
# ---------------------------------------------------------------------------
sh_bin=$(mkgitshim hang-is-shallow hang-is-shallow)
sh_ctl=$( fx "$behind" && bash "$behind/scripts/agent-gate.sh" --component-set-line lite 2>/dev/null )
if [ "$(field KIND "$sh_ctl")" != ok ]; then
  bad "3544-is-shallow-bounded: the POSITIVE CONTROL (same BEHIND fixture, no shim, lite) did not reach KIND ok (got '$(field KIND "$sh_ctl")') — the case cannot discriminate"
else
  sh_t0=$(date +%s)
  sh_out=$( fx "$behind" && PATH="$sh_bin:$PATH" bash "$behind/scripts/agent-gate.sh" \
              --component-set-line lite 2>/dev/null )
  sh_el=$(( $(date +%s) - sh_t0 ))
  sh_kind=$(field KIND "$sh_out")
  sh_line=$(field COMPONENT_SET_LINE "$sh_out")
  if [ "$sh_kind" = repo-read-blocked ] && [ "$sh_el" -lt 80 ] \
     && grep -q 'SHALLOW' <<<"$sh_line" && grep -q 'include.path' <<<"$sh_line"; then
    ok "3544-is-shallow-bounded: a hanging shallowness probe is BOUNDED and refused by name (repo-read-blocked in ${sh_el}s, well inside the shim's 90s sleep), and the detail names both the shallow decision and the include.path mechanism"
  else
    bad "3544-is-shallow-bounded: expected KIND repo-read-blocked well under the shim's 90s sleep (got '$sh_kind' in ${sh_el}s) — an unbounded read here would have run to the shim's sleep"
    printf '%s\n' "$sh_out"
  fi
fi

# ---------------------------------------------------------------------------
# 3a-iv-quater. THE ANCESTRY WALK READS THE SHARED OBJECT STORE, SO IT IS BOUNDED (roborev job
#     315, Medium) — and this case is the coverage that finding asked for. A LOOSE object is read
#     with `open()` + `read()` on a zlib stream, so a FIFO at an object path blocks; a pack
#     `.idx`/`.pack` FIFO does NOT (git mmaps those, and mmap on a FIFO fails rather than waiting).
#     I measured the pack case first and wrongly generalised to "the object store is not exposed" —
#     a harder test along the WRONG AXIS. This case pins the axis that actually blocks.
#
#     THE PLANT TARGETS THE **PARENT** COMMIT, and that is what makes it a test of the ancestry
#     walk rather than of an earlier read. Verified standalone before writing it:
#       parent object = real file :  rev-parse --verify HEAD^{commit} 2ms   merge-base 2ms
#       parent object = FIFO      :  rev-parse --verify HEAD^{commit} 2ms   merge-base BLOCKED
#     FIFOing HEAD's OWN object would block the PEEL of HEAD instead (which since #3757 runs in
#     the scratch, not in the live repository) and report `repo-read-blocked` from that site,
#     passing this case for the wrong reason. `3757-head-object-fifo` below is that other case.
#
#     LITE BOUND (15s) not strict (120s): the property is "this read is bounded at all".
# ---------------------------------------------------------------------------
anc_fx=$(mkbranch anc-fifo "$(mkbaseline base-anc - )" - )
anc_ctl=$(hook "$anc_fx")
anc_objdir=$(git -C "$anc_fx" rev-parse --git-path objects 2>/dev/null)
case "$anc_objdir" in /*) : ;; *) anc_objdir="$anc_fx/$anc_objdir" ;; esac
# THE CASE MAKES ITS OWN PARENT. A `mkbranch` fixture is a clone of the baseline plus ONE commit,
# and measured, its HEAD has NO parent — so `HEAD~1` was empty and this case refused itself. Rather
# than depend on a builder's history depth (which is not this case's subject and can change), add
# an empty commit: HEAD~1 is then the fixture's original branch commit, which is LOOSE in the
# fixture's own store and — the property that makes this a test of the WALK — is read by nothing
# earlier. HEAD's own object is peeled IN THE SCRATCH through the alternate (#3757 moved that
# peel out of the live repository; the case below plants a FIFO on HEAD's own object and asserts
# THAT site by name); the baseline's objects are read in the scratch; only `merge-base` has to
# traverse HEAD~1.
( fx "$anc_fx" && git "${GIT_ID[@]}" commit -q --allow-empty -m anc-parent ) >/dev/null 2>&1 || true
anc_parent=$(git -C "$anc_fx" rev-parse --verify --quiet 'HEAD~1^{commit}' 2>/dev/null || true)
# BLAST-RADIUS GUARD, AND IT MATTERS MORE HERE THAN FOR THE CONFIG CASE. A WORKTREE's object
# directory is NOT under its own path — it is the SHARED /data/lanes/repo/.git/objects on this
# fleet. So planting a FIFO in "the fixture's" object store without resolving and checking that
# path would hang EVERY LANE ON THE BOX. The resolved objects dir must itself lie strictly under
# $tmp; the fixture path being under $tmp is NOT sufficient evidence for that.
if [ -z "$anc_objdir" ] || case "$anc_objdir" in "$tmp"/?*) false ;; *) true ;; esac; then
  # SEPARATE CAUSES, SEPARATE MESSAGES. The first cut reported "the objects dir is not under \$tmp,
  # OR HEAD has no parent" as one sentence, and when it fired I spent a minute suspecting the path
  # check — which had in fact passed. A diagnostic that ORs two causes is the two-valued collapse
  # this whole file argues against, in the diagnostic rather than in the predicate.
  bad "3544-ancestry-bounded: refusing to plant a blocking object: resolved objects dir '$anc_objdir' is not strictly under \$tmp ('$tmp') — a FIFO in a SHARED object store would hang every lane on this box"
elif [ -z "$anc_parent" ]; then
  bad "3544-ancestry-bounded: the fixture's HEAD has no parent commit even after adding one, so there is no object the ancestry walk must traverse — the plant has no subject"
elif [ "$(field KIND "$anc_ctl")" != ok ]; then
  bad "3544-ancestry-bounded: the POSITIVE CONTROL (same fixture, no FIFO) did not reach KIND ok (got '$(field KIND "$anc_ctl")') — the case cannot discriminate"
else
  anc_d=${anc_parent%${anc_parent#??}}; anc_f=${anc_parent#??}
  anc_obj="$anc_objdir/$anc_d/$anc_f"
  if [ ! -f "$anc_obj" ]; then
    echo "skip - 3544-ancestry-bounded: HEAD's parent is packed, not loose ('$anc_obj' absent) — the loose-object path is not plantable in this fixture"
  else
    cp "$anc_obj" "$tmp/anc-parent-backup" && rm -f "$anc_obj" && mkfifo "$anc_obj"
    anc_t0=$(date +%s)
    anc_out=$( fx "$anc_fx" && bash "$anc_fx/scripts/agent-gate.sh" --component-set-line lite 2>/dev/null )
    anc_el=$(( $(date +%s) - anc_t0 ))
    # CLEANUP FIRST AND WITHOUT GIT: remove the FIFO, then RESTORE the real object bytes. The
    # include.path case taught this — its `git config --unset` cleanup blocked for 10m43s on the
    # very FIFO it had planted, and `|| true` cannot rescue a hang. Nothing here invokes git.
    rm -f "$anc_obj" && cp "$tmp/anc-parent-backup" "$anc_obj"
    anc_kind=$(field KIND "$anc_out")
    anc_line=$(field COMPONENT_SET_LINE "$anc_out")
    if [ "$anc_kind" = repo-read-blocked ] && [ "$anc_el" -lt 80 ] \
       && grep -q 'ancestry walk' <<<"$anc_line" && grep -q 'SHARED object store' <<<"$anc_line"; then
      ok "3544-ancestry-bounded: a FIFO at a LOOSE object path the ancestry walk must read is BOUNDED and refused by name (repo-read-blocked in ${anc_el}s), and the detail names the walk AND the shared object store"
    else
      bad "3544-ancestry-bounded: expected KIND repo-read-blocked well inside the bound (got '$anc_kind' in ${anc_el}s)"
      printf '%s\n' "$anc_out"
    fi
  fi
fi

# ---------------------------------------------------------------------------
# 3757. THE LIVE REPOSITORY IS ASKED FOR HEAD'S REF, NEVER FOR HEAD'S OBJECT (issue #3757).
#
#     THE DEFECT. The ancestry probe resolved HEAD with `rev-parse --verify --quiet
#     "HEAD^{commit}"` IN THE LIVE REPOSITORY. A peel is an OBJECT read, and in a promisor clone
#     a missing object is answered by a LAZY FETCH under that repository's OWN local config —
#     where a `url.*.insteadOf` rewrite invokes a remote HELPER. That is the route jobs 268/299
#     removed from every other read in this pre-flight, left open at one call site because the
#     comment above it argued the read was "genuinely local" on the grounds that no partial-clone
#     filter omits COMMITS. True, and the wrong question: the hazard is the object being absent
#     for any OTHER reason (a pruned or corrupted store, a hand-written ref, a peer lane editing
#     the SHARED `.git`).
#
#     MEASURED before the fix was written, with a discriminating control (git 2.43.0). Promisor
#     clones of a local bare origin at `--filter=blob:none` AND `--filter=tree:0`
#     (`uploadpack.allowfilter=true`), HEAD pointed at a commit present on the remote and ABSENT
#     locally (absence confirmed with `GIT_NO_LAZY_FETCH=1 cat-file -e`), the promisor URL
#     replaced by an `ext::` recorder that logs every invocation:
#       rev-parse --verify --quiet HEAD             rc=0    4ms   helper invocations: 0
#       rev-parse --verify --quiet 'HEAD^{commit}'  rc=1   10ms   helper invocations: 2
#     and, separating the OBJECT READ from the network with a FIFO at HEAD's LOOSE object path:
#       rev-parse --verify --quiet HEAD             rc=0    3ms   (sha printed)
#       rev-parse --verify --quiet 'HEAD^{commit}'  BLOCKED       (bound fired at 3s)
#
#     FOUR CASES: the shape is pinned STRUCTURALLY in both directions (no live peel; the peel is
#     in the scratch AND bounded AND three-valued), and the two consequences that a structural
#     scan cannot see are driven BEHAVIOURALLY (a FIFO at HEAD's own object is refused by name
#     rather than hanging; an UNPEELABLE HEAD stays INDETERMINATE and never becomes a false
#     BEHIND).
# ---------------------------------------------------------------------------

# ---- THE TWO WHOLE-LINE PINS (#3757, roborev job 347 / option A) -----------------------------
#
# WHAT THIS ASSERTS, and it is the whole of it: the pre-flight region's CODE lines that mention
# `_cs_live_git` or `$REPO_ROOT` are EXACTLY the lines pinned below, compared as WHOLE STRINGS,
# in BOTH directions. An unpinned line is a FINDING; a pinned line that no longer appears is a
# stale-pin FINDING. Nothing is parsed.
#
# WHY THERE IS NO TOKENISER ANY MORE. Three review rounds each closed the previously-found
# spelling of ONE unbounded question — "does this author-chosen bash line reach a command in the
# live repository?" — and the false-PASS count went 1 -> 1 -> 4, with each round's defects living
# inside the previous round's fix. The last one was
# `local sha=$(… git -C "$REPO_ROOT" rev-parse --verify --quiet "HEAD^{commit}")`, which the
# word-class scan excused because the command word was `local`. Parsing author-controlled bash to
# decide data-versus-control IS the shared channel #3312 says to REMOVE rather than delimit more
# finely, and #3229's ruling is that a guard with documented false PASSes is worse than none. So
# the recogniser (`R2`, `cs_first_word`, `CS_DECLARED_REPO_ROOT_HARMLESS`, the word-class direct-
# `git` test and the fragment splitter) is DELETED, not narrowed, and the property is re-expressed
# with the one mechanism that cannot be evaded by rewriting a line: literal equality of the line.
#
# ACCEPTED COST, stated so nobody "improves" it back into a parser: the pin is BRITTLE to
# reformatting. Any legitimate edit to a pinned line — including rewording a diagnostic string —
# must update the pin, and the both-directions leg turns a forgotten update into a loud finding
# rather than a silent hole. That is the same cost the live-call pin has always carried.
#
# WHAT IT DOES NOT CLAIM. It does not understand bash, and it cannot see a live read that names
# the repository some OTHER way: a path copied into a variable outside this region, a `cd` plus a
# bare `git` with no `$REPO_ROOT` token, or a `GIT_DIR=` built from an expression that does not
# spell `$REPO_ROOT`. Those are real residuals, not covered here, and the runtime
# `_cs_read_dir_isolated_or_refuse` refusal plus the `/nonexistent/` sentinel are what bound them.
#
# THE PARTITION IS A SUBSTRING TEST, so the two pins never disagree about a line: a line
# mentioning `_cs_live_git` is judged by the live-call pin (that is what makes a SECOND wrapper
# such as `_cs_live_git_quiet` a finding rather than an unpinned stranger); every other
# `$REPO_ROOT` line is judged by the second pin.
#
# Both blocks are QUOTED HEREDOCS, so `"`, `$`, `\` and `'` inside the pinned lines need no
# escaping and cannot drift from the gate's own text through a quoting mistake.
CS_PINNED_LIVE_CALL_LINES=$(cat <<'CS_PIN_LIVE_EOF'
  _cs_live_git -C "$REPO_ROOT" rev-parse --is-shallow-repository
  _cs_live_git -C "$REPO_ROOT" rev-parse --git-path shallow
_cs_live_git() {
  _cs_live_git -C "$REPO_ROOT" rev-parse --git-dir
  _cs_live_git -C "$REPO_ROOT" remote get-url origin
  _cs_live_git -C "$REPO_ROOT" rev-parse --git-path objects
  _cs_live_git --no-replace-objects -C "$REPO_ROOT" rev-parse --verify --quiet HEAD
CS_PIN_LIVE_EOF
)
# Every other region CODE line that names the live repository. The six `_cs_live_git` call lines
# also mention `$REPO_ROOT` and are deliberately NOT repeated here (see the partition above).
CS_PINNED_REPO_ROOT_LINES=$(cat <<'CS_PIN_RR_EOF'
  local f="$REPO_ROOT/$_CS_MANIFEST_REL" rc only_gate="" only_man="" c padded_man padded_gate
    _CS_DETAIL="the component manifest $_CS_MANIFEST_REL is missing or unreadable in $REPO_ROOT; it is COMMITTED SOURCE and the baseline comparison is derived from it, so its absence is not an excusable state — remedy: regenerate it from this gate's own --list"
      _CS_DETAIL="reading $1 from $REPO_ROOT EXCEEDED its ${_CS_BOUND_HINT}s bound — the read never returned. Every git command reads the repository config, and a \`include.path\` there naming a FIFO or other blocking file hangs it; on this fleet that config is SHARED by every lane on the box. Inspect it with \`git config --show-origin --get-all include.path\`"
      _CS_DETAIL="no bounded-run mechanism available (no timeout, no gtimeout, no sleep for the bash watchdog, or no capture file) — refusing to run an UNBOUNDED read of $1 from $REPO_ROOT, which could hang the gate outright; a missing capability must not inherit the permissive branch"
    "$REPO_ROOT")              why="the LIVE checkout ($_CS_READ_DIR)" ;;
    _CS_KIND=no-git; _CS_DETAIL="$REPO_ROOT is not a git worktree"; return 0
    _CS_DETAIL="no 'origin' remote is configured in $REPO_ROOT, so the baseline is unobtainable"
    ?*) lane_objects="$REPO_ROOT/$lane_objects" ;;
           _CS_DETAIL="deciding whether $REPO_ROOT is a SHALLOW clone required reading its repository state, and that read EXCEEDED its ${_CS_BOUND_HINT}s bound — the read never returned. Every git command reads the repository config, and an \`include.path\` there naming a FIFO or other blocking file hangs it; on this fleet that config is SHARED by every lane on the box. Inspect it with \`git config --show-origin --get-all include.path\`"
CS_PIN_RR_EOF
)
cs_pin_live_n=$(printf '%s\n' "$CS_PINNED_LIVE_CALL_LINES" | grep -c .)
cs_pin_rr_n=$(printf '%s\n' "$CS_PINNED_REPO_ROOT_LINES" | grep -c .)

# cs_pinned_line_findings <gate-path>: one FINDING per deviation from either pin. Empty = both
# pins hold. `grep -Fxq` is WHOLE-LINE fixed-string membership, and the needle is always a SINGLE
# line read by `IFS= read -r`, so the multi-line-needle trap (where a needle's first line "proves"
# its presence) cannot apply here.
cs_pinned_line_findings() {
  local g="$1" code line num raw pin
  code=$(cs_region_code "$g" | sed 's/^[0-9][0-9]*://')
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    num="${line%%:*}"; raw="${line#*:}"
    case "$raw" in
      *_cs_live_git*)
        grep -Fxq -- "$raw" <<<"$CS_PINNED_LIVE_CALL_LINES" \
          || printf 'FINDING[live-call]: %s: this line mentions _cs_live_git and is NOT one of the %s PINNED live-call lines (whole-line equality; the line is not read, only compared): %s\n' "$num" "$cs_pin_live_n" "$raw"
        continue ;;
    esac
    case "$raw" in
      *'$REPO_ROOT'*)
        grep -Fxq -- "$raw" <<<"$CS_PINNED_REPO_ROOT_LINES" \
          || printf 'FINDING[repo-root]: %s: this line names the LIVE repository ($REPO_ROOT) and is NOT one of the %s PINNED lines (whole-line equality; the line is not read, only compared): %s\n' "$num" "$cs_pin_rr_n" "$raw" ;;
    esac
  done < <(cs_region_code "$g")
  while IFS= read -r pin; do
    [ -n "$pin" ] || continue
    grep -Fxq -- "$pin" <<<"$code" \
      || printf 'FINDING[live-call]: a PINNED live-call line no longer appears in the region (stale pin, or the line changed): %s\n' "$pin"
  done <<<"$CS_PINNED_LIVE_CALL_LINES"
  while IFS= read -r pin; do
    [ -n "$pin" ] || continue
    grep -Fxq -- "$pin" <<<"$code" \
      || printf 'FINDING[repo-root]: a PINNED $REPO_ROOT line no longer appears in the region (stale pin, or the line changed): %s\n' "$pin"
  done <<<"$CS_PINNED_REPO_ROOT_LINES"
}

pin_findings=$(cs_pinned_line_findings "$GATE")
pin_live_bad=$(grep -F 'FINDING[live-call]' <<<"$pin_findings" || true)
pin_rr_bad=$(grep -F 'FINDING[repo-root]' <<<"$pin_findings" || true)
if [ -z "$pin_live_bad" ]; then
  ok "3757-live-call-allowlist: every region CODE line mentioning _cs_live_git is one of the $cs_pin_live_n PINNED lines, whole-line, both directions — so HEAD's live call is the UNPEELED bare ref and nothing shares its line"
else
  bad "3757-live-call-allowlist: the live-call pin does not hold:"
  printf '%s\n' "$pin_live_bad"
fi
if [ -z "$pin_rr_bad" ]; then
  ok "3757-repo-root-line-pin: every OTHER region CODE line naming \$REPO_ROOT is one of the $cs_pin_rr_n PINNED lines, whole-line, both directions — no new line names the live repository"
else
  bad "3757-repo-root-line-pin: the \$REPO_ROOT line pin does not hold:"
  printf '%s\n' "$pin_rr_bad"
fi

# TEN PLANTED ROUTES, EACH IN ITS OWN THROWAWAY COPY. Every route found across roborev jobs 325,
# 339 and 347 is planted here, because a control that plants only the shape a previous guard caught
# proves nothing about the ones it missed. Each mutation is verified to have been SUBSTITUTED at
# all — a sed that matched nothing would "pass" by proving nothing — and each must produce a
# finding NAMING what was planted.
#
# NOTE WHAT THE ASSERTION IS NOW: most of these are rejected because the mutated line is not
# LITERALLY one of the pinned lines, not because anything recognised their syntax. The finding text
# says exactly that ("is NOT one of the N PINNED lines"), and the needles below are the planted
# TEXT, which the finding prints verbatim — so a route is proved reported without the guard
# pretending to have understood it.
lc_dir="$tmp/3757-live-call-controls"; mkdir -p "$lc_dir"
lc_ids=(a b c d e f g h i j)
lc_whats=(
  'a dereferencing rev that contains no ^{ (HEAD~1)'
  'the rev held in a VARIABLE, so no rev token appears on the call line'
  'the call split over a \ line CONTINUATION'
  'a live call spelled --git-dir= instead of -C "$REPO_ROOT"'
  'a SAME-LINE -C OVERRIDE on an isolated read (git honours the LAST -C)'
  'a --git-dir="$REPO_ROOT/.git" appended to an isolated read'
  'a live call routed through a SECOND WRAPPER function'
  'a live call whose command word is a VARIABLE, not the literal git'
  'a live peel inside a COMMAND SUBSTITUTION on a local declaration (the round-3 High)'
  'an UNDECLARED read SMUGGLED IN FRONT of an allowed live call (roborev 347 item 2)'
)
lc_progs=(
  's|^\(  _cs_live_git --no-replace-objects -C "\$REPO_ROOT" rev-parse --verify --quiet \)HEAD$|\1HEAD~1|'
  's|^\(  _cs_live_git --no-replace-objects -C "\$REPO_ROOT" rev-parse --verify --quiet \)HEAD$|\1"$_cs_planted_rev"|'
  's|^  _cs_live_git --no-replace-objects -C "\$REPO_ROOT" rev-parse --verify --quiet HEAD$|  _cs_live_git --no-replace-objects -C "$REPO_ROOT" \\\n    rev-parse --verify --quiet "HEAD^{commit}"|'
  's|^  _cs_live_git --no-replace-objects -C "\$REPO_ROOT" rev-parse --verify --quiet HEAD$|  _component_set_bounded "$_CS_BOUND_SECS" env -i "${_CS_GIT_ENV[@]}" git --git-dir="$REPO_ROOT/.git" rev-parse --verify --quiet "HEAD^{commit}"|'
  's|-C "\$_CS_READ_DIR" rev-parse --verify --quiet "\${head_unpeeled}|-C "$_CS_READ_DIR" -C "$REPO_ROOT" rev-parse --verify --quiet "${head_unpeeled}|'
  's|-C "\$_CS_READ_DIR" merge-base --is-ancestor|-C "$_CS_READ_DIR" --git-dir="$REPO_ROOT/.git" merge-base --is-ancestor|'
  's|^  _cs_live_git --no-replace-objects -C "\$REPO_ROOT" rev-parse --verify --quiet HEAD$|  _cs_live_git_quiet --no-replace-objects -C "$REPO_ROOT" rev-parse --verify --quiet "HEAD^{commit}"|'
  's|^  _cs_live_git --no-replace-objects -C "\$REPO_ROOT" rev-parse --verify --quiet HEAD$|  $CS_PLANTED_GIT_BIN --no-replace-objects -C "$REPO_ROOT" rev-parse --verify --quiet "HEAD^{commit}"|'
  's|^  _cs_live_git --no-replace-objects -C "\$REPO_ROOT" rev-parse --verify --quiet HEAD$|  local _cs_planted_sha=$(env -i "${_CS_GIT_ENV[@]}" git -C "$REPO_ROOT" rev-parse --verify --quiet "HEAD^{commit}")\n\&|'
  's|^  _cs_live_git --no-replace-objects -C "\$REPO_ROOT" rev-parse --verify --quiet HEAD$|  _cs_planted_undeclared_read; _cs_live_git --no-replace-objects -C "$REPO_ROOT" rev-parse --verify --quiet HEAD|'
)
lc_tooks=(
  'rev-parse --verify --quiet HEAD~1$'
  'rev-parse --verify --quiet "\$_cs_planted_rev"$'
  '_cs_live_git .*\\$'
  'git --git-dir="\$REPO_ROOT/\.git"'
  '\-C "\$_CS_READ_DIR" -C "\$REPO_ROOT"'
  '\-C "\$_CS_READ_DIR" --git-dir="\$REPO_ROOT/\.git"'
  '_cs_live_git_quiet --no-replace-objects'
  '\$CS_PLANTED_GIT_BIN --no-replace-objects'
  'local _cs_planted_sha=\$(env'
  '_cs_planted_undeclared_read; _cs_live_git'
)
lc_needles=(
  'HEAD~1'
  '_cs_planted_rev'
  '-C "$REPO_ROOT" \'
  '--git-dir="$REPO_ROOT/.git"'
  '-C "$_CS_READ_DIR" -C "$REPO_ROOT"'
  '--git-dir="$REPO_ROOT/.git"'
  '_cs_live_git_quiet'
  '$CS_PLANTED_GIT_BIN'
  '_cs_planted_sha'
  '_cs_planted_undeclared_read'
)
for lc_i in "${!lc_ids[@]}"; do
  lc_id="${lc_ids[$lc_i]}"
  lc_copy="$lc_dir/route-$lc_id.sh"
  sed "${lc_progs[$lc_i]}" "$GATE" >"$lc_copy"
  lc_n=$(grep -c "${lc_tooks[$lc_i]}" "$lc_copy" 2>/dev/null || true)
  lc_out=$(cs_pinned_line_findings "$lc_copy")
  if [ "$lc_n" != 1 ]; then
    bad "3757-evasion-route[$lc_id]: the mutation did not take (matched $lc_n times, expected 1) — this route is NOT under test, so a clean scan on the real gate is not evidence for it. Route: ${lc_whats[$lc_i]}"
  elif grep -qF -- "${lc_needles[$lc_i]}" <<<"$lc_out"; then
    ok "3757-evasion-route[$lc_id]: ${lc_whats[$lc_i]} is REPORTED and NAMED (needle '${lc_needles[$lc_i]}') — rejected by whole-line inequality against the pin, with no attempt to parse it"
  else
    bad "3757-evasion-route[$lc_id]: planting ${lc_whats[$lc_i]} produced no finding naming '${lc_needles[$lc_i]}' — a silent false PASS. Findings: $lc_out"
  fi
done

# ---------------------------------------------------------------------------
# 3757b. THE PEEL MOVED TO THE ISOLATED SCRATCH, AND IT IS BOUNDED AND THREE-VALUED THERE.
#     Removing the peel from the live repository is only half the fix: the peel still reads a
#     commit object, now out of the LANE's SHARED store through the alternate, where a LOOSE
#     object is read as a stream and a FIFO planted by a PEER LANE blocks it (job 315's finding,
#     one call site over). So the scratch read must be BOUNDED and must map
#     124/137/`$_CS_UNBOUNDABLE_RC` onto the EXISTING `repo-read-blocked` refusal — a missing
#     capability must never inherit the permissive branch.
# ---------------------------------------------------------------------------
cs_scratch_peel_findings() {
  local g="$1" site n body
  # ANCHORED comment strip (roborev job 325, nit 3): an unanchored `:[[:space:]]*#` also matches a
  # `#` LATER on the line, so a real call line carrying a trailing `# note: …` was dropped.
  site=$(grep -n 'rev-parse --verify --quiet "\${head_unpeeled}\^{commit}"' "$g" | grep -v '^[0-9][0-9]*:[[:space:]]*#')
  n=$(printf '%s\n' "$site" | grep -c . )
  if [ "$n" -ne 1 ]; then
    printf 'FINDING: expected exactly ONE scratch peel of HEAD, found %s\n' "$n"
    return 0
  fi
  grep -q '_component_set_bounded "\$_CS_BOUND_SECS"' <<<"$site" \
    || printf 'FINDING: the scratch peel is NOT bounded: %s\n' "$site"
  grep -q -- '-C "\$_CS_READ_DIR"' <<<"$site" \
    || printf 'FINDING: the scratch peel does not run in $_CS_READ_DIR: %s\n' "$site"
  grep -q '_CS_READ_ENV' <<<"$site" \
    || printf 'FINDING: the scratch peel does not carry $_CS_READ_ENV (the alternate): %s\n' "$site"
  # The rc mapping lives in the lines just after the read; read it as CODE. The window is 20 raw
  # lines, not 10, because it is counted BEFORE comments are stripped and the mapping arms carry
  # doctrine comments — a window sized to the code would shrink every time a rationale is added,
  # and the guard would report a mapping "missing" that is simply further down.
  body=$(awk -v s="${site%%:*}" 'NR>s+0 && NR<=s+20' "$g" | grep -v '^[[:space:]]*#')
  grep -q 'peel_rc" -eq 124' <<<"$body" || printf 'FINDING: 124 (bound fired) is not mapped after the scratch peel\n'
  grep -q 'peel_rc" -eq 137' <<<"$body" || printf 'FINDING: 137 (KILLed) is not mapped after the scratch peel\n'
  grep -q 'peel_rc" -eq "\$_CS_UNBOUNDABLE_RC"' <<<"$body" || printf 'FINDING: $_CS_UNBOUNDABLE_RC (no bounding mechanism) is not mapped after the scratch peel\n'
  grep -q '_CS_KIND=repo-read-blocked' <<<"$body" || printf 'FINDING: the blocked/unboundable peel does not take the existing repo-read-blocked kind\n'
  return 0
}
sp_findings=$(cs_scratch_peel_findings "$GATE")
sp_dir="$tmp/3757-scratch-peel-control"; mkdir -p "$sp_dir"
sp_ctl="$sp_dir/unbounded-gate.sh"
sed 's|\(_CS_HEAD_SHA=\$(\)_component_set_bounded "\$_CS_BOUND_SECS" \(env -i\)|\1\2|' "$GATE" >"$sp_ctl"
sp_ctl_took=$(grep -c '_CS_HEAD_SHA=\$(env -i' "$sp_ctl")
sp_ctl_findings=$(cs_scratch_peel_findings "$sp_ctl")
# Property first, control-integrity second — same reasoning as the case above.
if [ -n "$sp_findings" ]; then
  bad "3757-scratch-peel-bounded: the scratch peel does not have the required shape:"
  printf '%s\n' "$sp_findings"
elif [ "$sp_ctl_took" -ne 1 ]; then
  bad "3757-scratch-peel-bounded: the POSITIVE CONTROL could not unbound the scratch peel (substitution took $sp_ctl_took times) — the scan cannot be shown to discriminate"
elif ! grep -q 'NOT bounded' <<<"$sp_ctl_findings"; then
  bad "3757-scratch-peel-bounded: unbounding the peel in a scratch copy was NOT reported — a bare pass is not evidence. Control findings: $sp_ctl_findings"
else
  ok "3757-scratch-peel-bounded: HEAD's peel runs in \$_CS_READ_DIR with the alternate, BOUNDED, and maps 124/137/UNBOUNDABLE onto repo-read-blocked; unbounding it in a scratch copy is REPORTED"
fi

# ---------------------------------------------------------------------------
# 3757c. `$_CS_READ_DIR` NEVER NAMES THE LIVE CHECKOUT — asserted, not assumed (roborev job 325,
#     blocker 2). The claim this diff adds to the region's execution-route enumeration ("since
#     #3757 it reads NO object in the live repository") used to hold only because every earlier
#     failure `return 0`s before the scratch assignment, while the initialiser was `$REPO_ROOT` —
#     an ORDERING property nothing checked, which is the reasoning job 314 rejected in this same
#     function. There is no reachable route to it today, so this is HARDENING; the point of these
#     cases is that the property is now MECHANICAL rather than traced by hand.
#
#     AND AN EMPTY SENTINEL IS NOT SELF-PROTECTING: `git -C ""` leaves the working directory
#     UNCHANGED, i.e. it MEANS the live checkout, so the initialiser alone fixes nothing. Three
#     properties, each with its own planted control:
#       (i)   the initialiser is not the live checkout;
#       (ii)  every `-C "$_CS_READ_DIR"` consumer in the region appears AFTER the scratch
#             assignment (the ordering, made explicit);
#       (iii) the peel calls the runtime refusal before handing the value to git.
# ---------------------------------------------------------------------------
cs_read_dir_findings() {
  local g="$1" init sentinel_decl assign assign_ln body_lo body_hi consumers ln guard_ln guard_n refuse_body glob_init
  init=$(cs_region_code "$g" | grep -F '_CS_READ_DIR=' | grep -F '_CS_READ_ENV=(); _CS_HEAD_SHA=' | head -1)
  if [ -z "$init" ]; then
    printf 'FINDING: could not locate the probe initialiser line that sets _CS_READ_DIR — the shape changed or the scan broke (fail-closed)\n'
  else
    case "$init" in
      *'_CS_READ_DIR="$REPO_ROOT"'*)
        printf 'FINDING: %s: the initialiser makes THE LIVE CHECKOUT the default read repository, so any consumer reached before the scratch assignment reads objects live: %s\n' "${init%%:*}" "${init#*:}" ;;
      *'_CS_READ_DIR=""'*)
        printf 'FINDING: %s: the initialiser leaves the read repository EMPTY, and git reads -C "" as the CURRENT directory (measured: rc 0, and cat-file -e HEAD^{commit} succeeds), so an unguarded consumer reads the LIVE repository: %s\n' "${init%%:*}" "${init#*:}" ;;
      *'_CS_READ_DIR="$_CS_READ_DIR_UNSET"'*) : ;;
      *) printf 'FINDING: %s: the initialiser sets _CS_READ_DIR to an unrecognised value; only the declared UNSET sentinel is allowed: %s\n' "${init%%:*}" "${init#*:}" ;;
    esac
  fi
  # THE GLOBAL INITIALISER IS PINNED TOO (roborev job 339, item 1). It sat at `""` — the value this
  # file's own control and finding text call unsafe — one line below the sentinel introduced to
  # replace it, because the check covered only the PROBE initialiser. Two initialisers, one
  # property; pin both.
  glob_init=$(cs_region_code "$g" | grep '^[0-9][0-9]*:_CS_READ_DIR=' | head -1)
  case "$glob_init" in
    *'_CS_READ_DIR="$_CS_READ_DIR_UNSET"'*) : ;;
    '') printf 'FINDING: could not locate the GLOBAL _CS_READ_DIR initialiser (fail-closed)\n' ;;
    *)  printf 'FINDING: %s: the GLOBAL initialiser does not use the UNSET sentinel: %s\n' "${glob_init%%:*}" "${glob_init#*:}" ;;
  esac
  # THE SENTINEL MUST BE A PATH THAT CANNOT EXIST — that is what makes an unguarded consumer fail
  # CLOSED rather than read live, and it is the half no source scan could otherwise supply.
  # CAPTURED, NOT PIPED INTO `grep -q`. This suite runs under `pipefail`, and a successful
  # `grep -q` EXITS EARLY, which SIGPIPEs the upstream producer — so the PIPELINE status is 141
  # even though the pattern was found, and `if ! …` then fires on a correct tree. Measured here on
  # the first run: this exact predicate reported the sentinel "not declared" while a standalone
  # `grep -c` on the same input answered 1.
  sentinel_decl=$(cs_region_code "$g" | grep "^[0-9][0-9]*:_CS_READ_DIR_UNSET='/nonexistent/" || true)
  if [ -z "$sentinel_decl" ]; then
    printf 'FINDING: the UNSET sentinel is not declared as a literal /nonexistent/... path — a sentinel git can resolve is a live read waiting to happen\n'
  fi
  assign=$(cs_region_code "$g" | grep -F '_CS_READ_DIR="$csdir/repo"' | head -1)
  if [ -z "$assign" ]; then
    printf 'FINDING: could not locate the scratch assignment _CS_READ_DIR="$csdir/repo" — the shape changed or the scan broke (fail-closed)\n'
    return 0
  fi
  assign_ln="${assign%%:*}"
  # ORDERING, SCOPED TO `_component_set_probe_inner`'s OWN BODY — and the scope is the honest part.
  # LEXICAL POSITION IS NOT EXECUTION ORDER FOR A FUNCTION BODY: three `-C "$_CS_READ_DIR"`
  # consumers live in helper functions DEFINED hundreds of lines EARLIER than the assignment and
  # CALLED after it, so a region-wide rule reported all three as "before the assignment" — a false
  # FAIL on correct code, measured on the first run. Inside one function body the two orders DO
  # coincide, so that is where the rule applies. DECLARED RESIDUAL: consumers in helper bodies are
  # NOT covered by this rule; they are covered by the UNSET sentinel above (an unguarded read fails
  # closed) and, for the peel, by the runtime refusal below.
  body_lo=$(cs_region_stream "$g" | grep '^[0-9][0-9]*:_component_set_probe_inner() {$' | head -1)
  body_lo="${body_lo%%:*}"
  if [ -z "$body_lo" ]; then
    printf 'FINDING: could not locate _component_set_probe_inner in the region (fail-closed)\n'
    return 0
  fi
  body_hi=$(cs_region_stream "$g" | awk -F: -v lo="$body_lo" '$1+0 > lo+0 && $0 ~ /^[0-9]+:}$/ { print $1; exit }')
  if [ -z "$body_hi" ]; then
    printf 'FINDING: could not locate the end of _component_set_probe_inner (fail-closed)\n'
    return 0
  fi
  consumers=$(cs_region_code "$g" | grep -F -- '-C "$_CS_READ_DIR"')
  if [ -z "$consumers" ]; then
    printf 'FINDING: no -C "$_CS_READ_DIR" consumer found in the region — the guard has no subject (fail-closed)\n'
  fi
  while IFS= read -r ln; do
    [ -n "$ln" ] || continue
    [ "${ln%%:*}" -ge "$body_lo" ] || continue
    [ "${ln%%:*}" -le "$body_hi" ] || continue
    if [ "${ln%%:*}" -lt "$assign_ln" ]; then
      printf 'FINDING: %s: a -C "$_CS_READ_DIR" consumer runs BEFORE the scratch assignment at line %s, in the probe body where lexical order IS execution order: %s\n' "${ln%%:*}" "$assign_ln" "${ln#*:}"
    fi
  done <<<"$consumers"
  # THE REFUSAL MUST ASK AN AFFIRMATIVE QUESTION (roborev job 339, item 5). A list of bad states is
  # sound only for the assignment sites that exist today, and nothing pins that number: a future
  # fourth `_CS_READ_DIR=<live-ish path>` would pass a `*)` arm silently. The value must BE the
  # scratch this run created.
  refuse_body=$(awk '/^_cs_read_dir_isolated_or_refuse\(\) \{$/,/^\}$/' "$g")
  if [ -z "$refuse_body" ]; then
    printf 'FINDING: could not locate _cs_read_dir_isolated_or_refuse (fail-closed)\n'
  elif ! grep -qF '[ "$_CS_READ_DIR" = "$_CS_SCRATCH_DIR/repo" ]' <<<"$refuse_body"; then
    printf 'FINDING: the refusal is not AFFIRMATIVE — it does not require _CS_READ_DIR to BE the scratch this run created ($_CS_SCRATCH_DIR/repo), so an unlisted bad value passes silently\n'
  fi
  # THE GUARD MUST DOMINATE EVERY CONSUMER, NOT MERELY PRECEDE THE PEEL (roborev job 347). The
  # first version asserted only "the call is before the peel", and that is the standing error this
  # repo keeps ruling on: FOUR object reads run before the peel (the fast path's `cat-file -e` and
  # `rev-list`, and the manifest `ls-tree`/`show` through `_component_set_set_at_rev`), so a check
  # there could only REPORT a prohibited live read that had already happened. Two consumer kinds,
  # so two comparisons: the direct `-C "$_CS_READ_DIR"` sites in the probe body, and the probe
  # body's CALLS to the helpers whose own bodies are lexically earlier.
  guard_ln=$(cs_region_code "$g" | grep -F '_cs_read_dir_isolated_or_refuse "' | head -1)
  guard_n=$(cs_region_code "$g" | grep -cF '_cs_read_dir_isolated_or_refuse "' || true)
  if [ -z "$guard_ln" ]; then
    printf 'FINDING: nothing calls _cs_read_dir_isolated_or_refuse — an unisolated value would be handed to git instead of refused by name\n'
    return 0
  fi
  if [ "$guard_n" != 1 ]; then
    printf 'FINDING: %s calls to _cs_read_dir_isolated_or_refuse; ONE placement that dominates every consumer is the contract, and N scattered asserts is the drift this region removes\n' "$guard_n"
  fi
  guard_ln="${guard_ln%%:*}"
  if [ "$guard_ln" -le "$assign_ln" ]; then
    printf 'FINDING: the _cs_read_dir_isolated_or_refuse call (line %s) does not follow the scratch assignment (line %s), so it cannot be asserting what that assignment produced\n' "$guard_ln" "$assign_ln"
  fi
  while IFS= read -r ln; do
    [ -n "$ln" ] || continue
    [ "${ln%%:*}" -ge "$body_lo" ] || continue
    [ "${ln%%:*}" -le "$body_hi" ] || continue
    if [ "${ln%%:*}" -lt "$guard_ln" ]; then
      printf 'FINDING: %s: an object read through $_CS_READ_DIR runs BEFORE the isolation assertion at line %s — the assertion does not dominate it, so a prohibited LIVE read happens and is only then reported: %s\n' "${ln%%:*}" "$guard_ln" "${ln#*:}"
    fi
  done <<<"$consumers"
  while IFS= read -r ln; do
    [ -n "$ln" ] || continue
    [ "${ln%%:*}" -ge "$body_lo" ] || continue
    [ "${ln%%:*}" -le "$body_hi" ] || continue
    if [ "${ln%%:*}" -lt "$guard_ln" ]; then
      printf 'FINDING: %s: the probe CALLS a helper that reads through $_CS_READ_DIR before the isolation assertion at line %s — lexically the helper body is elsewhere, but this call site runs first: %s\n' "${ln%%:*}" "$guard_ln" "${ln#*:}"
    fi
  done <<<"$(cs_region_code "$g" | grep -E '^[0-9]+:[[:space:]]*(if )?_component_set_set_at_rev ')"
  return 0
}
rd_findings=$(cs_read_dir_findings "$GATE")
if [ -z "$rd_findings" ]; then
  ok "3757-read-dir-shape: the read repository is a NON-EXISTENT-path sentinel until the scratch exists (so an unguarded consumer fails closed, not live), no consumer in the probe body precedes the scratch assignment, and the peel refuses an unisolated value before calling git"
else
  bad "3757-read-dir-shape: the read-repository invariants are not met:"
  printf '%s\n' "$rd_findings"
fi

# SIX PLANTED CONTROLS over the three properties (roborev job 339, item 3: this said "THREE
# PLANTED CONTROLS, one per property" over four entries, and neither half was true — i and ii both
# target the INITIALISER). Each mutates a COPY and must be REPORTED and NAMED.
rd_dir="$tmp/3757-read-dir-controls"; mkdir -p "$rd_dir"
rd_ids=(i ii iii iv v vi)
rd_whats=(
  'the initialiser set back to the LIVE checkout'
  'the initialiser left EMPTY, which git reads as the CURRENT directory'
  'a -C "$_CS_READ_DIR" consumer planted BEFORE the scratch assignment, in the probe body'
  'the runtime refusal call removed entirely'
  'the refusal reverted to a DENY-LIST of bad states instead of an affirmative test'
  'the refusal moved back DOWN to the peel, behind four object reads (the job-347 regression)'
)
rd_progs=(
  's|^  _CS_READ_DIR="\$_CS_READ_DIR_UNSET"; _CS_READ_ENV=(); _CS_HEAD_SHA=""$|  _CS_READ_DIR="$REPO_ROOT"; _CS_READ_ENV=(); _CS_HEAD_SHA=""|'
  's|^  _CS_READ_DIR="\$_CS_READ_DIR_UNSET"; _CS_READ_ENV=(); _CS_HEAD_SHA=""$|  _CS_READ_DIR=""; _CS_READ_ENV=(); _CS_HEAD_SHA=""|'
  's|^  _CS_READ_DIR="\$csdir/repo"$|  : "$(git --no-replace-objects -C "$_CS_READ_DIR" cat-file -e planted-by-the-selftest 2>/dev/null)"\n  _CS_READ_DIR="$csdir/repo"|'
  '/_cs_read_dir_isolated_or_refuse "/d'
  's|if \[ -n "\$_CS_SCRATCH_DIR" \] && \[ "\$_CS_READ_DIR" = "\$_CS_SCRATCH_DIR/repo" \]; then|if [ "$_CS_READ_DIR" != "$REPO_ROOT" ]; then|'
  '/_cs_read_dir_isolated_or_refuse "/d; s|^\(    \)_CS_HEAD_SHA=\$(_component_set_bounded|\1if _cs_read_dir_isolated_or_refuse "peel HEAD"; then return 0; fi\n\1_CS_HEAD_SHA=$(_component_set_bounded|'
)
rd_tooks=(
  '^  _CS_READ_DIR="\$REPO_ROOT"; _CS_READ_ENV'
  '^  _CS_READ_DIR=""; _CS_READ_ENV'
  'cat-file -e planted-by-the-selftest'
  '_cs_read_dir_isolated_or_refuse "'
  'if \[ "\$_CS_READ_DIR" != "\$REPO_ROOT" \]; then'
  '_cs_read_dir_isolated_or_refuse "peel HEAD"'
)
rd_expect_n=(1 1 1 0 1 1)
rd_needles=(
  'THE LIVE CHECKOUT'
  'reads -C "" as the CURRENT directory'
  'BEFORE the scratch assignment'
  'nothing calls _cs_read_dir_isolated_or_refuse'
  'not AFFIRMATIVE'
  'the assertion does not dominate it'
)
for rd_i in "${!rd_ids[@]}"; do
  rd_id="${rd_ids[$rd_i]}"
  rd_copy="$rd_dir/prop-$rd_id.sh"
  sed "${rd_progs[$rd_i]}" "$GATE" >"$rd_copy"
  rd_n=$(grep -c "${rd_tooks[$rd_i]}" "$rd_copy" 2>/dev/null || true)
  rd_out=$(cs_read_dir_findings "$rd_copy")
  if [ "$rd_n" != "${rd_expect_n[$rd_i]}" ]; then
    bad "3757-read-dir-control[$rd_id]: the mutation did not take (matched $rd_n, expected ${rd_expect_n[$rd_i]}) — this property is NOT under test. Property: ${rd_whats[$rd_i]}"
  elif grep -qF -- "${rd_needles[$rd_i]}" <<<"$rd_out"; then
    ok "3757-read-dir-control[$rd_id]: ${rd_whats[$rd_i]} is REPORTED and NAMED (needle '${rd_needles[$rd_i]}')"
  else
    bad "3757-read-dir-control[$rd_id]: planting '${rd_whats[$rd_i]}' produced no finding naming '${rd_needles[$rd_i]}' — a silent false PASS. Findings: $rd_out"
  fi
done

# ---------------------------------------------------------------------------
# 3757c-bis. BEHAVIOURAL: THE REFUSAL PREVENTS THE LIVE READS, IT DOES NOT MERELY REPORT THEM
#     (roborev job 347, item 1). The structural case above pins WHERE the assertion sits; this one
#     measures that no prohibited read HAPPENED, which is what the finding actually asked for.
#
#     HOW THE ABSENCE OF A READ IS OBSERVED, without inventing an oracle: the gate records its own
#     progress in fields the self-test hook already prints. `BASELINE_OBJECTS` is set by the FAST
#     PATH'S FIRST OBJECT READ (`cat-file -e <sha>^{commit}` + `rev-list`), and `BASELINE_SRC` by
#     the manifest read (`ls-tree`/`show`). Both run through `$_CS_READ_DIR`, so with the scratch
#     assignment rewritten to the LIVE checkout they are LIVE reads — and if the assertion
#     dominates them, BOTH fields must still read `<none>` while the KIND is `read-dir-unisolated`.
#     `<none>` here is not an absence of evidence: it is the gate reporting that the step which
#     would have set the field never ran.
#
#     THE CONTROL IS WHAT MAKES THAT MEAN ANYTHING: the same fixture with the assertion DELETED
#     must show those fields POPULATED, i.e. the live reads really are on this path and really do
#     happen when nothing stops them. Without it, `<none>` could just as well mean the fixture
#     never got that far for some unrelated reason.
# ---------------------------------------------------------------------------
dom_base=$(mkbaseline base-readdir-dom - )
dom_live_sed='s|^  _CS_READ_DIR="\$csdir/repo"$|  _CS_READ_DIR="$REPO_ROOT"|'
dom_fx=$(mkbranch readdir-dom "$dom_base" "$dom_live_sed" --from-origin)
dom_ctl_fx=$(mkbranch readdir-dom-noassert "$dom_base" "$dom_live_sed"'; /_cs_read_dir_isolated_or_refuse "/d' --from-origin)
dom_took=$(grep -c '^  _CS_READ_DIR="\$REPO_ROOT"$' "$dom_fx/scripts/agent-gate.sh" 2>/dev/null || true)
dom_ctl_took=$(grep -c '_cs_read_dir_isolated_or_refuse "' "$dom_ctl_fx/scripts/agent-gate.sh" 2>/dev/null || true)
dom_out=$(hook "$dom_fx")
dom_ctl=$(hook "$dom_ctl_fx")
dom_kind=$(field KIND "$dom_out")
dom_obj=$(field BASELINE_OBJECTS "$dom_out")
dom_src=$(field BASELINE_SRC "$dom_out")
dom_ctl_obj=$(field BASELINE_OBJECTS "$dom_ctl")
dom_ctl_src=$(field BASELINE_SRC "$dom_ctl")
if [ "$dom_took" != 1 ]; then
  bad "3757-read-dir-dominates: the fixture mutation did not take (matched $dom_took, expected 1) — nothing is under test"
elif [ "$dom_ctl_took" != 0 ]; then
  bad "3757-read-dir-dominates: the CONTROL still calls the assertion ($dom_ctl_took occurrence(s)) — it cannot show what happens without it"
elif [ "$dom_ctl_obj" = "<none>" ] && [ "$dom_ctl_src" = "<none>" ]; then
  bad "3757-read-dir-dominates: the CONTROL (assertion deleted) read NOTHING either (objects='$dom_ctl_obj' src='$dom_ctl_src'), so the live reads are not on this path and a '<none>' in the subject would prove nothing"
elif [ "$dom_kind" = read-dir-unisolated ] && [ "$dom_obj" = "<none>" ] && [ "$dom_src" = "<none>" ]; then
  ok "3757-read-dir-dominates: with the read repository left at the LIVE checkout the run refuses (read-dir-unisolated) having performed NO object read and NO manifest read (BASELINE_OBJECTS/BASELINE_SRC both <none>), while the same fixture without the assertion records both (objects='$dom_ctl_obj' src='$dom_ctl_src') — the assertion PREVENTS the reads, it does not report them afterwards"
else
  bad "3757-read-dir-dominates: expected read-dir-unisolated with NO read recorded (got kind='$dom_kind' objects='$dom_obj' src='$dom_src'; control objects='$dom_ctl_obj' src='$dom_ctl_src')"
  printf '%s\n' "$dom_out"
fi

# ---------------------------------------------------------------------------
# 3757d. BEHAVIOURAL: an unisolated read repository is REFUSED BY NAME, not read live.
#     The structural cases above pin the SHAPE; this one drives the runtime refusal, because a
#     guard that exists in source and is never reached is not a guard. The fixture's own gate copy
#     has the scratch assignment rewritten to the LIVE checkout — the exact state the initialiser
#     used to leave — and the fixture is cloned FROM its origin so the baseline read still
#     succeeds and execution actually reaches the peel. Control: the same fixture unmutated, which
#     must reach KIND ok, so the case cannot pass because the fixture is simply broken.
# ---------------------------------------------------------------------------
rdu_base=$(mkbaseline base-readdir - )
rdu_ctl_fx=$(mkbranch readdir-ok "$rdu_base" - --from-origin)
rdu_fx=$(mkbranch readdir-live "$rdu_base" 's|^  _CS_READ_DIR="\$csdir/repo"$|  _CS_READ_DIR="$REPO_ROOT"|' --from-origin)
rdu_took=$(grep -c '^  _CS_READ_DIR="\$REPO_ROOT"$' "$rdu_fx/scripts/agent-gate.sh" 2>/dev/null || true)
rdu_ctl=$(hook "$rdu_ctl_fx")
rdu_out=$(hook "$rdu_fx")
rdu_kind=$(field KIND "$rdu_out")
rdu_line=$(field COMPONENT_SET_LINE "$rdu_out")
if [ "$rdu_took" != 1 ]; then
  bad "3757-read-dir-unisolated: the fixture mutation did not take (matched $rdu_took, expected 1) — the refusal is not under test"
elif [ "$(field KIND "$rdu_ctl")" != ok ]; then
  bad "3757-read-dir-unisolated: the POSITIVE CONTROL (same fixture, unmutated) did not reach KIND ok (got '$(field KIND "$rdu_ctl")') — the case cannot discriminate"
elif [ "$rdu_kind" = read-dir-unisolated ] \
     && grep -q 'isolated read repository was never established' <<<"$rdu_line" \
     && [ "$(field VERDICT "$rdu_out")" = UNMEASURED ]; then
  ok "3757-read-dir-unisolated: a read repository left at the LIVE checkout is REFUSED by name (read-dir-unisolated, verdict UNMEASURED) instead of peeling HEAD there"
else
  bad "3757-read-dir-unisolated: expected KIND read-dir-unisolated naming the unestablished scratch (got '$rdu_kind', verdict '$(field VERDICT "$rdu_out")')"
  printf '%s\n' "$rdu_out"
fi

# ---------------------------------------------------------------------------
# 3757c. BEHAVIOURAL: a FIFO at HEAD's OWN loose object is REFUSED BY NAME, not hung.
#     The plant targets HEAD's own commit object — the object the SCRATCH peel now reads
#     through the alternate — and the assertion is on the DETAIL text, because
#     `repo-read-blocked` alone is produced by three other sites (the config include, the
#     shallowness probe, the ancestry walk) and could not tell this read from those.
#     Same blast-radius guard as `3544-ancestry-bounded`, and for the same reason: on this
#     fleet a WORKTREE's object dir is the SHARED store, so a FIFO planted without resolving
#     and checking that path would hang every lane on the box.
# ---------------------------------------------------------------------------
hp_fx=$(mkbranch head-obj-fifo "$(mkbaseline base-headobj - )" - )
hp_ctl=$(hook "$hp_fx")
hp_objdir=$(git -C "$hp_fx" rev-parse --git-path objects 2>/dev/null)
case "$hp_objdir" in /*) : ;; *) hp_objdir="$hp_fx/$hp_objdir" ;; esac
hp_head=$(git -C "$hp_fx" rev-parse --verify --quiet 'HEAD^{commit}' 2>/dev/null || true)
if [ -z "$hp_objdir" ] || case "$hp_objdir" in "$tmp"/?*) false ;; *) true ;; esac; then
  bad "3757-head-object-fifo: refusing to plant a blocking object: resolved objects dir '$hp_objdir' is not strictly under \$tmp ('$tmp') — a FIFO in a SHARED object store would hang every lane on this box"
elif [ -z "$hp_head" ]; then
  bad "3757-head-object-fifo: the fixture's HEAD does not resolve to a commit, so the plant has no subject"
elif [ "$(field KIND "$hp_ctl")" != ok ]; then
  bad "3757-head-object-fifo: the POSITIVE CONTROL (same fixture, no FIFO) did not reach KIND ok (got '$(field KIND "$hp_ctl")') — the case cannot discriminate"
elif ! command -v timeout >/dev/null 2>&1; then
  # A PRECONDITION, NOT A VERDICT: without `timeout` the run cannot be given the independent
  # watchdog below, and running it unbounded is the hang this case must not risk.
  echo "skip - 3757-head-object-fifo: no timeout(1) on PATH, so the planted read cannot be run under an independent outer watchdog"
else
  hp_obj="$hp_objdir/${hp_head:0:2}/${hp_head:2}"
  if [ ! -f "$hp_obj" ]; then
    echo "skip - 3757-head-object-fifo: HEAD's object is packed, not loose ('$hp_obj' absent) — the loose-object path is not plantable in this fixture"
  else
    cp "$hp_obj" "$tmp/3757-head-backup" && rm -f "$hp_obj" && mkfifo "$hp_obj"
    hp_t0=$(date +%s)
    # UNDER AN INDEPENDENT WATCHDOG (roborev job 347, item 2). Without it, a regression in the
    # bounding this case tests would hang the SUITE here — no verdict, and the planted FIFO left in
    # place. 90s is well above the observed 15-16s and well below forever.
    hp_out=$(cs_hook_watchdog "$hp_fx" lite 90); hp_rc=$?
    hp_el=$(( $(date +%s) - hp_t0 ))
    # CLEANUP FIRST AND WITHOUT GIT, AND ON EVERY PATH INCLUDING THE WATCHDOG'S (the include.path
    # case's lesson: a `git config --unset` cleanup once blocked for 10m43s on the very FIFO it had
    # planted, and `|| true` cannot rescue a hang). It runs here, before any assertion, so no
    # branch below can skip it.
    rm -f "$hp_obj" && cp "$tmp/3757-head-backup" "$hp_obj"
    hp_kind=$(field KIND "$hp_out")
    hp_line=$(field COMPONENT_SET_LINE "$hp_out")
    if [ "$hp_rc" -eq 124 ] || [ "$hp_rc" -eq 137 ]; then
      bad "3757-head-object-fifo: the OUTER WATCHDOG fired after 90s — the read this case plants was NOT bounded by the gate, which is precisely the regression this case exists to catch (rc=$hp_rc)"
    elif [ "$hp_kind" = repo-read-blocked ] && [ "$hp_el" -lt 80 ] \
       && grep -q 'peeling HEAD' <<<"$hp_line" && grep -q 'SHARED object store' <<<"$hp_line"; then
      ok "3757-head-object-fifo: a FIFO at HEAD's OWN loose object is BOUNDED and refused by name (repo-read-blocked in ${hp_el}s), and the detail names the PEEL and the shared object store"
    else
      bad "3757-head-object-fifo: expected KIND repo-read-blocked naming the peel, well inside the bound (got '$hp_kind' in ${hp_el}s, rc=$hp_rc)"
      printf '%s\n' "$hp_out"
    fi
  fi
fi

# ---------------------------------------------------------------------------
# 3757d. BEHAVIOURAL: an UNPEELABLE HEAD is INDETERMINATE, never a false BEHIND.
#     The live call resolves the REF and does not check the object exists — measured, and this
#     case is what pins it end to end: HEAD is pointed at a well-formed sha naming NO object, so
#     the live read succeeds, the SCRATCH peel fails, and the rc=128 semantics carry it to
#     `_CS_ANCESTOR=unknown`. The control is the SAME fixture untouched, which is genuinely
#     BEHIND — so the pair discriminates INDETERMINATE from the verdict it must not become.
#     Nothing is fetched to satisfy the peel: a missing HEAD object is a broken checkout, not a
#     reason for this pre-flight to reach the network.
# ---------------------------------------------------------------------------
up_fx=$(mkbranch head-unpeelable "$(mkbaseline base-unpeelable "$ADD_SENTINEL" )" - )
up_ctl=$(hook "$up_fx")
up_branch=$(git -C "$up_fx" symbolic-ref --short HEAD 2>/dev/null || true)
up_bogus=0123456789012345678901234567890123456789
if [ "$(field VERDICT "$up_ctl")" != BEHIND ]; then
  bad "3757-unpeelable-head: the POSITIVE CONTROL (same fixture, real HEAD) is not BEHIND (got '$(field VERDICT "$up_ctl")') — the case cannot show INDETERMINATE is not the default"
elif [ -z "$up_branch" ] || [ ! -f "$up_fx/.git/refs/heads/$up_branch" ]; then
  bad "3757-unpeelable-head: could not locate the fixture's HEAD ref file (branch='$up_branch') — the plant has no subject"
else
  printf '%s\n' "$up_bogus" >"$up_fx/.git/refs/heads/$up_branch"
  up_live=$(git -C "$up_fx" rev-parse --verify --quiet HEAD 2>/dev/null || true)
  up_out=$(hook "$up_fx" lite)
  up_v=$(field VERDICT "$up_out"); up_a=$(field ANCESTOR "$up_out")
  if [ "$up_live" = "$up_bogus" ] && [ "$up_v" = INDETERMINATE ] && [ "$up_a" = unknown ]; then
    ok "3757-unpeelable-head: the live read resolves HEAD's ref WITHOUT checking the object exists, and a HEAD that cannot be peeled to a commit is INDETERMINATE (ancestor unknown) — never a false BEHIND"
  else
    bad "3757-unpeelable-head: expected the live read to return the bogus sha and the verdict to be INDETERMINATE/unknown (live='$up_live' verdict='$up_v' ancestor='$up_a')"
    printf '%s\n' "$up_out"
  fi
fi

# ---------------------------------------------------------------------------
# 3a-v. THE REF ORACLE'S OUTPUT IS REMOTE-CONTROLLED TEXT, SO IT IS VALIDATED (job 258). The
#     pre-flight now learns the baseline sha from `git ls-remote` — which downloads no objects
#     and replaced a 92 MB full-history fetch — and that value is interpolated into later `git`
#     arguments AND into the SUMMARY block this repository tells agents to paste into PR
#     comments. So it is CHECKED, not merely split: an object id of a known length, and the ref
#     name that was asked for. Both halves are driven, because a validator that accepts the
#     wrong ref would compare against a baseline nobody named.
# ---------------------------------------------------------------------------
#     THE THIRD MODE IS THE NARROWING (job 309): a 64-character sha256 id for the RIGHT ref is
#     WELL-FORMED and was ACCEPTED here, while the isolated scratch repository is created in
#     git's default object format and so could neither read the lane's objects as an alternate
#     nor transfer sha256 objects into itself. Accepting it deferred the failure to a generic
#     `fetch-failed` that blamed the network for a format mismatch, so the value is refused at
#     this line WITH ITS REASON — and the case asserts the reason, not just the refusal, because
#     the other two modes already produce the refusal and could not tell this fix from them.
base_lsr=$(mkbaseline base-lsr - )
for _lsr_mode in lsremote-not-a-sha lsremote-wrong-ref lsremote-sha256; do
  _lsr_fx=$(mkbranch "lsr-${_lsr_mode}" "$base_lsr" - )
  _lsr_ctl=$(hook "$_lsr_fx")
  _lsr_bin=$(mkgitshim "$_lsr_mode" "$_lsr_mode")
  _lsr_out=$( fx "$_lsr_fx" && PATH="$_lsr_bin:$PATH" bash "$_lsr_fx/scripts/agent-gate.sh" \
                --component-set-line full 2>/dev/null )
  _lsr_line=$(field COMPONENT_SET_LINE "$_lsr_out")
  if [ "$(field KIND "$_lsr_ctl")" != ok ]; then
    bad "3544-ref-unparsable[$_lsr_mode]: the POSITIVE CONTROL (same fixture, no shim) did not reach KIND ok (got '$(field KIND "$_lsr_ctl")') — the case cannot discriminate"
  elif [ "$(field VERDICT "$_lsr_out")" = UNMEASURED ] \
     && [ "$(field KIND "$_lsr_out")" = baseline-ref-unparsable ] \
     && [ "$(field SHA "$_lsr_out")" = "-" ] \
     && grep -q 'FAIL-CLOSED (#3544)' <<<"$_lsr_line" \
     && grep -q 'remote-controlled text is validated' <<<"$_lsr_line" \
     && { [ "$_lsr_mode" != lsremote-sha256 ] || grep -q 'sha256' <<<"$_lsr_line"; }; then
    ok "3544-ref-unparsable[$_lsr_mode]: an advertisement that is not <40-hex object-id> refs/heads/main is refused BY NAME with its reason, never passed on to a git argument or a SUMMARY line"
  else
    bad "3544-ref-unparsable[$_lsr_mode]: expected KIND baseline-ref-unparsable + SHA '-'"
    printf '%s\n' "$_lsr_out"
  fi
done

# ---------------------------------------------------------------------------
# 3a-vi. THE OBJECTS ARE FETCHED ONLY WHEN THIS REPOSITORY LACKS THEM (job 258, the perf fix)
#     — and BOTH paths must produce the SAME verdict for the same baseline. Measured on this box
#     before the change: 3.74 s and 92 MB of full history downloaded into a fresh scratch
#     repository on EVERY invocation, then deleted. After: 0.51 s and no object transfer when the
#     commit is already here, which on this fleet is the common case (lanes are worktrees of ONE
#     shared `.git`, so a peer's fetch — or this pre-flight's own slow path — already put it
#     there). The sha still comes from the remote in THIS invocation; only the OBJECT transfer is
#     skipped, and a git object is content-addressed, so "we hold <sha>" and "the remote
#     advertises <sha>" is the same commit.
#
#     THE PAIR IS THE ASSERTION. A case that only saw `reused` could not tell the fast path from
#     a broken fetch, and one that only saw `fetched` would not notice the fast path never
#     running. Two fixtures off ONE baseline, in separate repositories so neither warms the
#     other — the slow path leaves the objects behind, which is exactly how the first cut of the
#     transfer-mismatch case stopped discriminating.
# ---------------------------------------------------------------------------
base_obj=$(mkbaseline base-obj - )
obj_reuse=$(mkbranch obj-reuse "$base_obj" - --from-origin)
obj_fetch=$(mkbranch obj-fetch "$base_obj" - --from-origin)
# Advance origin/main only for the SECOND fixture's baseline read: it cloned before the advance,
# so it cannot hold the new tip and must fetch.
( fx "$tmp/base-obj-src" && printf 'advanced past both clones\n' >>README.md \
  && git "${GIT_ID[@]}" commit -qam advance \
  && git push -q "$base_obj" HEAD:refs/heads/main ) >/dev/null 2>&1
obj_tip=$(git -C "$base_obj" rev-parse refs/heads/main)
obj_fetch_out=$(hook "$obj_fetch")
# …and the reuse fixture is brought up to date the way a peer's fetch would: fetching the tip
# into a ref of its own. That is the state the fast path exists for.
( fx "$obj_reuse" && git fetch -q origin refs/heads/main:refs/heads/peer-fetched ) >/dev/null 2>&1
obj_reuse_out=$(hook "$obj_reuse")
if [ "$(field BASELINE_OBJECTS "$obj_fetch_out")" = fetched ] \
   && [ "$(field BASELINE_OBJECTS "$obj_reuse_out")" = reused ] \
   && [ "$(field SHA "$obj_fetch_out")" = "$obj_tip" ] \
   && [ "$(field SHA "$obj_reuse_out")" = "$obj_tip" ] \
   && [ "$(field KIND "$obj_fetch_out")" = ok ] \
   && [ "$(field KIND "$obj_reuse_out")" = ok ] \
   && [ "$(field VERDICT "$obj_fetch_out")" = "$(field VERDICT "$obj_reuse_out")" ]; then
  ok "3544-baseline-objects: objects are fetched only when absent (fetched/reused observed as a PAIR), and both paths report the SAME baseline sha and verdict"
else
  bad "3544-baseline-objects: expected fetched+reused as a pair on the same tip $obj_tip (fetch: objects=$(field BASELINE_OBJECTS "$obj_fetch_out") sha=$(field SHA "$obj_fetch_out") kind=$(field KIND "$obj_fetch_out"); reuse: objects=$(field BASELINE_OBJECTS "$obj_reuse_out") sha=$(field SHA "$obj_reuse_out") kind=$(field KIND "$obj_reuse_out"))"
fi

# ---------------------------------------------------------------------------
# 3a-vii. A REPLACEMENT REF MUST NOT SUBSTITUTE THE COMMIT UNDER US (roborev job 264, High).
#     `refs/replace/<sha>` transparently swaps another commit in for that sha EVERYWHERE git
#     reads objects. Unfixed, this pre-flight would report the CANONICAL baseline sha while
#     reading a FORGED, smaller manifest — and stamp PASS. That is the worst available pairing:
#     the sha in the audit trail is correct and the bytes it stands for are not, so nothing in
#     the block looks wrong. The config sources had been closed and "untrusted repository STATE"
#     treated as closed with them; replace refs are the rest of that space.
#
#     THE FIXTURE IS THE ATTACK. The baseline carries the sentinel component, so the HONEST
#     verdict is BEHIND naming it. The replacement points the baseline sha at the LANE's OWN
#     commit, whose manifest does NOT carry the sentinel — so a run that honours the replacement
#     reads a set with nothing missing and reports PASS. BEHIND vs PASS is the starkest pair
#     available here.
#
#     TWO CONTROLS, in both directions, because "unaffected" is meaningless unless the
#     replacement demonstrably applies in this repository right now: a plain `git show` of the
#     baseline sha must return the FORGED manifest, and the same read with
#     `--no-replace-objects` must return the TRUE one.
# ---------------------------------------------------------------------------
base_rep=$(mkbaseline base-replace "$ADD_SENTINEL")
rep_fx=$(mkbranch replaced "$base_rep" - )
rep_tip=$(git -C "$base_rep" rev-parse refs/heads/main)
# Bring the baseline commit into this repository the way a peer's fetch would — `git replace`
# needs both objects present — without making origin/main an ancestor of HEAD.
( fx "$rep_fx" && git fetch -q origin refs/heads/main:refs/heads/peer-fetched ) >/dev/null 2>&1
rep_decoy=$(git -C "$rep_fx" rev-parse HEAD 2>/dev/null)
( fx "$rep_fx" && git replace -f "$rep_tip" "$rep_decoy" ) >/dev/null 2>&1
rep_forged=$( fx "$rep_fx" && git show "$rep_tip:scripts/agent-gate.components" 2>/dev/null | grep -cx -- "$SENTINEL" )
rep_true=$( fx "$rep_fx" && git --no-replace-objects show "$rep_tip:scripts/agent-gate.components" 2>/dev/null | grep -cx -- "$SENTINEL" )
rep_out=$(hook "$rep_fx")
rep_line=$(field COMPONENT_SET_LINE "$rep_out")
if [ "$rep_decoy" = "$rep_tip" ]; then
  bad "3544-no-replace-objects: the decoy commit EQUALS the baseline sha, so the replacement could substitute nothing — the fixture would test nothing"
elif [ "$rep_forged" -ne 0 ] || [ "$rep_true" -eq 0 ]; then
  bad "3544-no-replace-objects: the CONTROLS do not discriminate (plain read saw the sentinel $rep_forged times, expected 0; --no-replace-objects read saw it $rep_true times, expected >=1) — the replacement ref is not in effect here, so the gate being unaffected proves nothing"
elif [ "$(field VERDICT "$rep_out")" = BEHIND ] \
   && [ "$(field KIND "$rep_out")" = ok ] \
   && [ "$(field SHA "$rep_out")" = "$rep_tip" ] \
   && grep -qw -- "$SENTINEL" <<<"$(field MISSING "$rep_out")" \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$rep_line"; then
  ok "3544-no-replace-objects: a refs/replace entry forges the baseline manifest for a plain git read (control) yet the pre-flight reads the TRUE one and still reports the skew"
else
  bad "3544-no-replace-objects: expected BEHIND naming $SENTINEL at $rep_tip — a replacement ref changed what the pre-flight read"
  printf '%s\n' "$rep_out"
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
# ---------------------------------------------------------------------------
# LOCALE-INDEPENDENT MANIFEST GRAMMAR (roborev job 297, Low). The manifest documents ASCII
# `[A-Za-z0-9._-]+`, but the parser used `[:alnum:]`, which is LOCALE-DEPENDENT — so the same
# manifest was valid on one host and invalid on another. The discriminator is precise: with
# `[:alnum:]` under a UTF-8 locale a non-ASCII name PARSES, so the run reaches `manifest-stale`
# (the set simply does not match `COMPONENTS`); with ASCII ranges it is refused as
# `manifest-garbage`. So KIND alone separates the two implementations.
#
# THE CONTROL IS THE LOCALE PICK ITSELF: the case only runs under a locale in which `[:alnum:]`
# demonstrably DOES accept `café`. On a host where no such locale exists there is nothing to
# discriminate, and the case says so rather than asserting on an untested premise.
loc_pick=""
for _l in en_US.UTF-8 C.UTF-8 en_GB.UTF-8; do
  if [ "$(LC_ALL="$_l" bash -c 'case "café" in *[![:alnum:]._-]*) echo REJ;; *) echo ACC;; esac' 2>/dev/null)" = ACC ]; then
    loc_pick="$_l"; break
  fi
done
if [ -z "$loc_pick" ]; then
  echo "skip - 3544-manifest-locale: no locale on this host makes [:alnum:] accept a non-ASCII name, so the two implementations are indistinguishable here (host precondition, not a waived assertion)"
else
  locmani=$(mkbranch locmanifest "$base_ok" - --manifest-lines "$(printf 'file-size\ncaf\303\251')")
  loc_out=$(LC_ALL="$loc_pick" hook "$locmani")
  loc_kind=$(field KIND "$loc_out")
  if [ "$loc_kind" = manifest-garbage ]; then
    ok "3544-manifest-locale: a non-ASCII manifest name is refused as manifest-garbage even under $loc_pick, where [:alnum:] accepts it (control: that locale was verified to accept it) — the grammar is the documented ASCII one on every host"
  else
    bad "3544-manifest-locale: expected KIND manifest-garbage under $loc_pick; got '$loc_kind' (manifest-stale means the non-ASCII name PARSED, i.e. the grammar is still locale-dependent)"
    printf '%s\n' "$loc_out" | head -4
  fi
fi

# ---------------------------------------------------------------------------
# THE CAPTURE REPLAY IS BOUNDED (roborev job 297, Medium). An unbounded `cat` of the captured
# stdout is not covered by the execution deadline: a descendant outliving a SUCCESSFUL child keeps
# writing, and `cat` stops only at EOF, which never arrives while a writer outpaces the reader.
# Measured off-suite with a fast writer: `cat` did not terminate in 6s and the file reached 4.1 GB.
#
# WHAT THIS CASE COVERS, AND WHAT IT DELIBERATELY DOES NOT. It pins the CAP — an over-cap capture is
# REFUSED (not truncated, not replayed) — which is the mechanism that makes the runaway case
# impossible. It does NOT reproduce the unbounded hang itself: doing so needs a writer with no size
# limit, and this suite is not going to write gigabytes to a shared filesystem to prove a bound that
# the cap already makes unreachable. Stated rather than left as an apparent oversight.
#
# TWO ARMS, because a cap that refuses EVERYTHING would pass the first arm alone.
#
# THE CAP IS SUBSTITUTED IN A SCRATCH COPY OF THE GATE, not written up to (job 299). The real
# stdout cap is 64 MiB — it has to exceed the largest LEGITIMATE capture, which is a `git show` of
# the whole gate script (>1 MB and growing; at 1 MiB it sat BELOW that and refused every
# declaration read, 9 cases of this suite). Writing 64 MiB to a shared filesystem to prove a bound
# is exactly what this case's own note above declines to do, so the ARTIFACT is substituted instead
# — the idiom `agent_gate_pin_canonical_remote` uses, and never a settable seam, which would be one
# more thing a real invoker can set. THE SUBSTITUTION IS VERIFIED STRUCTURALLY (awk `exit 3` when
# the literal is absent): without that, a renamed constant would leave the copy at 64 MiB, the
# oversize arm would return 0, and the case would red for a reason that reads like a broken cap.
cap_gate="$tmp/capgate.sh"
cap_small_cap=65536
cap_ok=1
if ! awk -v v="$cap_small_cap" '
      BEGIN { done = 0 }
      { if (!done && $0 ~ /^_CS_CAP_MAX_BYTES=[0-9]+$/) { print "_CS_CAP_MAX_BYTES=" v; done = 1; next }
        print }
      END { if (!done) exit 3 }' "$GATE" >"$cap_gate"; then
  cap_ok=0
fi
osz_prog="$tmp/oversize-writer.sh"
{ printf '#!/usr/bin/env bash\n'; printf 'head -c 200000 /dev/zero | tr "\\\\0" "x"\n'; printf 'exit 0\n'; } >"$osz_prog"
chmod +x "$osz_prog"
osz_small="$tmp/small-writer.sh"
{ printf '#!/usr/bin/env bash\n'; printf 'printf "component-set-small\\\\n"\n'; printf 'exit 0\n'; } >"$osz_small"
chmod +x "$osz_small"
osz_out="$tmp/osz.out"; osz_rc=""; small_out="$tmp/small.out"; small_rc=""
if [ "$cap_ok" -ne 1 ]; then
  bad "3544-bound-replay-capped: no '_CS_CAP_MAX_BYTES=<n>' literal in $GATE — the constant was renamed, so this case cannot bound the copy it measures (it must NEVER fall back to the shipped 64 MiB value, which would make the oversize arm vacuous)"
else
  timeout 30 bash "$cap_gate" --component-set-bounded-run 10 "$osz_prog" >"$osz_out" 2>/dev/null; osz_rc=$?
  timeout 30 bash "$cap_gate" --component-set-bounded-run 10 "$osz_small" >"$small_out" 2>/dev/null; small_rc=$?
  osz_line=$(sed -n 's/^RC: //p' "$osz_out" 2>/dev/null)
  small_line=$(sed -n 's/^RC: //p' "$small_out" 2>/dev/null)
  if [ "$osz_rc" != 124 ] && [ "$osz_line" = 198 ] && [ "$small_line" = 0 ] \
     && grep -q 'component-set-small' "$small_out" 2>/dev/null; then
    ok "3544-bound-replay-capped: with the cap substituted to ${cap_small_cap}B in a scratch copy (substitution structurally verified), an over-cap capture is REFUSED (RC 198) promptly rather than replayed, while an under-cap one still returns 0 AND its bytes reach the caller — the cap discriminates by size, it does not refuse everything"
  else
    bad "3544-bound-replay-capped: expected oversize RC=198 (got '$osz_line', outer rc='$osz_rc' — 124 means the read was NOT bounded) and small RC=0 with its output replayed (got '$small_line')"
  fi
fi

# ---------------------------------------------------------------------------
# THE STDERR REPLAY IS BOUNDED TOO, AND IT TRUNCATES WHERE STDOUT REFUSES (roborev job 299, Medium).
# The stdout cap above left stderr as a bare `cat`, which is the SAME defect on the other stream: a
# descendant that outlives a SUCCESSFUL child keeps the stderr fd it inherited, and `cat` on a
# regular file stops only at EOF, which never arrives while a writer outpaces the reader.
#
# WHAT DISCRIMINATES, and the two arms are chosen so that neither disposition can pass by accident:
#   * OVERSIZE STDERR MUST NOT BECOME A REFUSAL. `RC: 0` — the CHILD'S OWN status — is the
#     assertion: stderr feeds diagnostics only, nothing parses it, and turning a runaway writer on
#     an unparsed stream into a failed pre-flight would be a false FAIL on a measured result. An
#     `RC: 198` here would mean the stdout disposition had been copied onto the wrong stream.
#   * THE BYTES MUST ACTUALLY BE CUT. The child emits 8 MiB to stderr and the caller must receive a
#     small fraction of it. This is what reds against the bare `cat` it replaces (which delivers all
#     8 MiB), so the case has a live subject rather than asserting a property that already held.
#   * SECOND ARM: a SMALL stderr message must still arrive IN FULL. A bound that dropped stderr
#     entirely, or replayed nothing, would pass the first arm alone.
#
# THE WRITER IS BOUNDED AT 8 MiB, NOT ENDLESS, and that is a deliberate limit of this case rather
# than an oversight: an endless writer is the residual declared at the replay site (#3717) — nothing
# reaps it, so it would go on filling a SHARED filesystem after this suite exits, and on this fleet a
# full disk breaks every lane. 8 MiB is 128x the 64 KiB stderr cap, which is enough to observe the
# cut; it is not enough to observe a HANG, and no assertion here claims to.
# ---------------------------------------------------------------------------
serr_prog="$tmp/stderr-writer.sh"
{ printf '#!/usr/bin/env bash\n'
  printf '( head -c 8388608 /dev/zero | tr "\\\\0" "e" >&2 ) &\n'
  printf 'exit 0\n'
} >"$serr_prog"
chmod +x "$serr_prog"
serr_small="$tmp/stderr-small.sh"
{ printf '#!/usr/bin/env bash\n'; printf 'printf "component-set-stderr-small\\\\n" >&2\n'; printf 'exit 0\n'; } >"$serr_small"
chmod +x "$serr_small"
serr_out="$tmp/serr.out"; serr_err="$tmp/serr.err"; serr_rc=""
serr_sout="$tmp/serr-small.out"; serr_serr="$tmp/serr-small.err"
timeout 60 bash "$behind/scripts/agent-gate.sh" --component-set-bounded-run 10 "$serr_prog" >"$serr_out" 2>"$serr_err"; serr_rc=$?
timeout 60 bash "$behind/scripts/agent-gate.sh" --component-set-bounded-run 10 "$serr_small" >"$serr_sout" 2>"$serr_serr"
serr_line=$(sed -n 's/^RC: //p' "$serr_out" 2>/dev/null)
serr_small_line=$(sed -n 's/^RC: //p' "$serr_sout" 2>/dev/null)
serr_bytes=$(wc -c <"$serr_err" 2>/dev/null)
case "$serr_bytes" in ''|*[!0-9]*) serr_bytes=-1 ;; esac
if [ "$serr_rc" != 124 ] && [ "$serr_line" = 0 ] \
   && [ "$serr_bytes" -gt 0 ] && [ "$serr_bytes" -le 131072 ] \
   && [ "$serr_small_line" = 0 ] \
   && grep -q 'component-set-stderr-small' "$serr_serr" 2>/dev/null; then
  ok "3544-bound-replay-err-bounded: a SUCCESSFUL child that backgrounds a fast 8MiB stderr writer returns promptly with its OWN status (RC 0, never the 198 refusal stdout gets) and the replay is CUT to ${serr_bytes}B, while a small stderr message still arrives in full — the two streams have opposite dispositions by design"
else
  bad "3544-bound-replay-err-bounded: expected RC=0 (child's own status, NOT 198) with a truncated stderr replay (got RC='$serr_line', outer rc='$serr_rc' — 124 means the stderr read was NOT bounded, stderr bytes=$serr_bytes of 8388608 written; >131072 means it was replayed unbounded) and the small arm RC=0 with its message replayed (got '$serr_small_line')"
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
# ---------------------------------------------------------------------------
# 3f-iii. NEWLINE-BEARING CHECKOUT PATH (roborev job 295, Medium). `_CS_DETAIL` interpolates
#         externally influenced values — `$REPO_ROOT` among them — and a checkout path may contain
#         newlines. Unflattened, that injects PHYSICAL LINES into a SUMMARY block: measured on the
#         pre-fix gate, a path containing "\nRESULT: PASS\n" put `RESULT: PASS` at COLUMN ZERO
#         inside a block whose real verdict was FAIL-CLOSED. That matters twice over — the gate's
#         own completion probe greps for that token, and `--delta` anchor validation accepts any
#         matching `RESULT: PASS` line, so a FAILING block could pass as a valid anchor.
#
#         The property is enforced at the ONE point the line is assembled, so this case is about the
#         CHOKE POINT and not about `$REPO_ROOT` specifically: the sweep that produced the fix found
#         `$TMPDIR`, remote-controlled `ls-remote` output and refused manifest lines in the same
#         position, and the set of externally influenced values is not enumerable by inspection.
nl_dir="$tmp/evil
RESULT: PASS
x"
mkdir -p "$nl_dir/scripts"
cp "$GATE" "$nl_dir/scripts/agent-gate.sh"
nl_out=$(hook "$nl_dir")
nl_forged=$(printf '%s\n' "$nl_out" | grep -c '^RESULT: PASS')
nl_fields=$(printf '%s\n' "$nl_out" | grep -c '^COMPONENT_SET_LINE: ')
# THE CONTROL, and it is what makes the zero above mean anything: the injected text must still be
# PRESENT somewhere in the emitted line, flattened inline. Without this, a run that rejected the
# path BEFORE rendering it would also report 0 forged lines and the case would pass while testing
# nothing — an absence proving a property it never exercised.
nl_rendered=$(printf '%s\n' "$nl_out" | grep -c 'RESULT: PASS')
if [ "$nl_forged" -eq 0 ] && [ "$nl_fields" -eq 1 ] && [ "$nl_rendered" -ge 1 ]; then
  ok "3544-detail-one-line: a newline-bearing checkout path is FLATTENED into one physical line — the injected text is rendered inline (control) yet adds NO field at column zero"
else
  bad "3544-detail-one-line: expected forged-at-column-zero=0, COMPONENT_SET_LINE fields=1, injected-text-rendered>=1; got $nl_forged / $nl_fields / $nl_rendered"
  printf '%s\n' "$nl_out" | head -6
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
          ps df readlink id iconv timeout nice chmod; do
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
  echo "skip - 3544-bound-owned-pgid: same precondition (no outer host bound available)"
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

# ---------------------------------------------------------------------------
# THE PROCESS GROUP IS STILL OURS WHEN IT IS SIGNALLED (roborev job 279). The watchdog arm used to
# background the COMMAND, making the pgid the command's own pid, and then sent an unconditional
# `kill -KILL -$pid` after a one-second grace — by which time the command and its descendants may
# all have exited and bash may already have REAPED the leader, releasing that id. On a box running
# four lanes the group that inherits it is most likely A PEER LANE'S GATE; this repository has the
# incident on record (a pattern-based `pkill` killed a peer's gate at component 28 of 30).
#
# WHAT IS OBSERVABLE, AND WHAT IS NOT. The race itself cannot be constructed: pid reuse is not
# controllable, so there is no honest positive control for "the signal went to a released group",
# and inventing one would be worse than not having it. What the fix DOES make observable is that
# the group is still ours to kill on the SUCCESS path — a stray descendant is now REAPED there,
# where the previous shape left it running (it only signalled on the timeout path). That is a
# genuine before/after difference on this arm, so it is what the case asserts; the ownership
# invariant itself is asserted STRUCTURALLY below and labelled as such.
#
# The bash-watchdog arm is forced with a curated PATH, because a host with `timeout` never takes it.
own="$tmp/owned-stray.sh"
owntick="$tmp/owned-stray-tick.txt"
ownpid="$tmp/owned-stray.pid"
mk_ticker "$own" "$owntick" "$ownpid" 0 2
: >"$owntick"
own_rc=$( fx "$behind" && PATH="$bin_no_timeout" $wd_outer bash "$behind/scripts/agent-gate.sh" \
            --component-set-bounded-run 5 "$own" 2>/dev/null | sed -n 's/^RC: //p' )
own_at=$(wc -l <"$owntick" | tr -d ' ')
sleep 3
own_later=$(wc -l <"$owntick" | tr -d ' ')
if [ "$own_rc" = 0 ] && [ "$own_later" = "$own_at" ]; then
  ok "3544-bound-owned-pgid: on the bash-watchdog arm a SUCCESSFUL call still reaps its process group (stray frozen at $own_at) — the group is signalled while the supervisor holding it is alive, not after its id could have been released"
else
  bad "3544-bound-owned-pgid: expected rc 0 and a reaped stray (rc='$own_rc' ticks $own_at -> $own_later)"
fi
reap_ticker "$ownpid"
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
     && [ "$(field HEAD_SRC "$unc_out")" = declaration ] \
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

# 4b-ii. HEAD'S SET UNMEASURABLE — and since job 290 the diagnostic names the GATE SCRIPT ONLY,
#     because HEAD's provenance is read from the committed `COMPONENTS` DECLARATION and the manifest
#     is NOT CONSULTED there: HEAD's manifest had no equivalent of the local `manifest-stale` check,
#     so a stale one could excuse an uncommitted removal. Naming a file this run never opened would
#     send the reader to look in the wrong place, so the case asserts its ABSENCE from the line.
#     (The diagnostic names the PATHS, not a literal `HEAD:` rev —
#     since job 268 HEAD is resolved to a SHA in this checkout and the read happens in the isolated
#     repository, because `HEAD` inside the scratch would mean the SCRATCH's own unborn HEAD): the provenance oracle is the SOLE evidence for DECLARED's
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
   && grep -q 'scripts/agent-gate.sh' <<<"$hu_line" \
   && ! grep -q 'scripts/agent-gate.components' <<<"$hu_line" \
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
# _cs_pass_line_ok <line> <expected-head>: the PASS line equals <expected-head> followed by EXACTLY
# ONE of the three legal object-provenance clauses (#3746 / roborev job 311). Pinned as a CLOSED SET
# rather than one literal because which clause is emitted is ENVIRONMENT-DEPENDENT — it says whether
# this box's shared object store already held the fixture's baseline commit, which a peer lane's
# fetch can change between runs. Pinning one literal would red on correct input; pinning nothing
# would let the declaration be dropped in a wording pass, which is what the exact match exists to
# prevent. So: string EQUALITY against a closed set, no regex (the head carries `(`/`)`/`/`, and
# escaping it for ERE at two call sites is one fact written twice).
_cs_pass_line_ok() {
  local l="$1" h="$2" tail
  case "$l" in "$h"*) tail="${l#"$h"}" ;; *) return 1 ;; esac
  case "$tail" in
    "; objects: baseline REUSED from this lane's SHARED store — store TRUSTED, not verified (#3746)") return 0 ;;
    "; objects: baseline FETCHED from the canonical remote, HEAD's own from this lane's SHARED store — store TRUSTED, not verified (#3746)") return 0 ;;
    "; objects: provenance NOT RECORDED — treat the store as TRUSTED, not verified (#3746)") return 0 ;;
  esac
  return 1
}

# THE SCOPE CLAUSE IS INSIDE THE EXACT MATCH ON PURPOSE. `NAMES ONLY — not implementations, and no
# component is run here` is what stops a reader over-reading `PASS (37/37)` as "the component set is
# verified". Pinning it here means it cannot be quietly dropped in a later wording pass: a comment
# describing the limitation serves the next author, and only the EMITTED line serves whoever is
# holding a pasted SUMMARY block. The exactness IS the enforcement.
if [ "$(field VERDICT "$s_out")" = PASS ] \
   && [ "$(field BASELINE_SRC "$s_out")" = manifest ] \
   && _cs_pass_line_ok "$s_line" "component-set: PASS ($n_components/$n_components names vs origin/main $s_sha; NAMES ONLY — not implementations, and no component is run here) — baseline read via the committed manifest"; then
  ok "3544-no-skew: an in-sync tree stamps an affirmative PASS naming its baseline sha, the read path AND the object-store trust boundary"
else
  bad "3544-no-skew: expected 'component-set: PASS ($n_components/$n_components names vs origin/main $s_sha; NAMES ONLY — not implementations, and no component is run here) — baseline read via the committed manifest' + one of the three legal '; objects: …(#3746)' clauses"
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
   && [ "$(field BASELINE_OBJECTS "$fr_out")" = fetched ] \
   && [ "$fresh_cached_after" = "$fresh_cached_before" ]; then
  ok "3544-fresh-baseline: the comparison uses the FETCHED tip (objects really were fetched), and leaves the shared cached ref alone"
else
  bad "3544-fresh-baseline: expected the new tip $fresh_tip (got '$(field SHA "$fr_out")'), cached ref unmoved (before='$fresh_cached_before' after='$fresh_cached_after')"
  printf '%s\n' "$fr_out"
fi

# ---------------------------------------------------------------------------
# 5d. RETIRED (job 264): "the baseline comes from a ref this run owns, not FETCH_HEAD".
#
#     That case existed because the pre-flight fetched the baseline INTO this repository, and the
#     question was which ref carried the result — `FETCH_HEAD` being a single shared mutable file
#     that a concurrent fetch overwrites between the write and the read. There is now NO fetch
#     into this repository at all (the objects are read out of the isolated scratch store), so
#     there is no destination ref to own and no `FETCH_HEAD` to race. The property is stronger and
#     is asserted where it now lives: `3544-no-import` above requires that after a slow-path run
#     this repository still LACKS the baseline commit, holds no `refs/worktree/*` entry, and has
#     an unchanged `FETCH_HEAD`.
#
#     Recorded rather than silently dropped: a reader who finds the old case in git history should
#     see why it went, and a future change that reintroduces a fetch into the live repository has
#     to reintroduce this coverage with it.
# ---------------------------------------------------------------------------

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
# fetch would have something to write. The COMMIT is advanced in the same step, because the
# pre-flight only fetches objects it does not already hold (job 258): a fixture still sitting on
# origin/main's tip takes the fast path, performs NO fetch, and this case would then be asserting
# that a fetch which never happened wrote no tags. `BASELINE_OBJECTS: fetched` below is the guard
# against that recurring silently.
( fx "$tmp/base-tag-src" && printf 'advance for the fetch path\n' >>README.md \
    && git "${GIT_ID[@]}" commit -qam advance \
    && git "${GIT_ID[@]}" tag -a v99.99.99-selftest -m 'tag the baseline' \
    && git push -q "$base_tag" HEAD:refs/heads/main \
    && git push -q "$base_tag" refs/tags/v99.99.99-selftest ) >/dev/null 2>&1
tags_before=$(git -C "$tagged" for-each-ref --format='%(refname) %(objectname)' refs/tags | sort)
tg_out=$(hook "$tagged")
tags_after=$(git -C "$tagged" for-each-ref --format='%(refname) %(objectname)' refs/tags | sort)
upstream_tag=$(git -C "$base_tag" for-each-ref --format='%(refname)' refs/tags | grep -c 'v99.99.99-selftest')
if [ "$upstream_tag" -ge 1 ] \
   && [ "$(field KIND "$tg_out")" = ok ] \
   && [ "$(field BASELINE_OBJECTS "$tg_out")" = fetched ] \
   && [ "$tags_after" = "$tags_before" ] \
   && ! printf '%s\n' "$tags_after" | grep -q 'v99.99.99-selftest'; then
  ok "3544-no-tag-writes: the baseline fetch leaves shared refs/tags/* UNCHANGED even with tagOpt=--tags and a new upstream tag (and a fetch DID happen: BASELINE_OBJECTS=fetched)"
else
  bad "3544-no-tag-writes: expected an unchanged tag ref set after a REAL fetch (upstream_tag=$upstream_tag kind=$(field KIND "$tg_out") objects=$(field BASELINE_OBJECTS "$tg_out"))"
  echo "   before: [$tags_before]"
  echo "   after:  [$tags_after]"
fi

# ---------------------------------------------------------------------------
# 5f. NOTHING IS WRITTEN INTO THIS REPOSITORY, AND THE BASELINE OBJECTS ARE NOT IMPORTED
#     (roborev job 264, High — superseding the transfer-verification case that stood here).
#
#     WHAT THIS REPLACES AND WHY. The pre-flight used to fetch the baseline from the isolated
#     scratch repository INTO this one and then compare the sha that arrived against the sha the
#     isolated hop had observed. That comparison was called "untrusted but safe" and it was wrong
#     in KIND: `git fetch` in the live repository reads the live repository's LOCAL config, which
#     `env -i` cannot suppress, so a local `url.*.insteadOf` plus `protocol.ext.allow=always`
#     rewrites the scratch path to an `ext::` remote helper and RUNS COMMANDS DURING THE FETCH —
#     before any comparison can happen. A check after the fact cannot defend against harm that
#     happens during, and on this fleet lanes are worktrees of ONE shared `.git`, so a peer's
#     config write reaches it. The transfer is therefore GONE, and with it the
#     `baseline-transfer-mismatch` kind: the objects are read where they landed, through
#     `GIT_ALTERNATE_OBJECT_DIRECTORIES`, which resolves no URL and can name no helper.
#
#     THE ASSERTION IS THE ABSENCE OF AN IMPORT, MEASURED POSITIVELY. After a run that took the
#     SLOW path (asserted: `BASELINE_OBJECTS: fetched`) and reported the correct tip, this
#     repository must STILL NOT HOLD that commit — the scratch store is gone by then, so
#     `git cat-file -e <tip>` must FAIL. That single check is what distinguishes "read without
#     importing" from "imported quietly", and it could not have been made against the old design.
#     Plus the shared-state invariants the old case pinned: no `refs/worktree/*` ref, and
#     `FETCH_HEAD` untouched.
# ---------------------------------------------------------------------------
base_noimp=$(mkbaseline base-noimport "$ADD_SENTINEL")
noimp=$(mkbranch noimport "$base_noimp" - )
noimp_tip=$(git -C "$base_noimp" rev-parse refs/heads/main)
noimp_fh_before=$(cat "$noimp/.git/FETCH_HEAD" 2>/dev/null || echo '<none>')
noimp_out=$(hook "$noimp")
noimp_fh_after=$(cat "$noimp/.git/FETCH_HEAD" 2>/dev/null || echo '<none>')
noimp_refs=$(git -C "$noimp" for-each-ref --format='%(refname)' 'refs/worktree/*' 2>/dev/null | grep -c . || true)
git -C "$noimp" cat-file -e "$noimp_tip^{commit}" 2>/dev/null && noimp_have=yes || noimp_have=no
if [ "$(field BASELINE_OBJECTS "$noimp_out")" != fetched ]; then
  bad "3544-no-import: the run did not take the slow path (BASELINE_OBJECTS='$(field BASELINE_OBJECTS "$noimp_out")'), so there was no object transfer to be absent — the case cannot discriminate"
elif [ "$(field SHA "$noimp_out")" != "$noimp_tip" ] || [ "$(field KIND "$noimp_out")" != ok ]; then
  bad "3544-no-import: the baseline was not measured correctly (kind='$(field KIND "$noimp_out")' sha='$(field SHA "$noimp_out")', expected ok + $noimp_tip) — reading through the alternate object store is broken"
  printf '%s\n' "$noimp_out"
elif [ "$noimp_have" = yes ]; then
  bad "3544-no-import: this repository now HOLDS the baseline commit $noimp_tip — the objects were imported after all, which is the transport hop this fix removed"
elif [ "${noimp_refs:-0}" -ne 0 ] || [ "$noimp_fh_after" != "$noimp_fh_before" ]; then
  bad "3544-no-import: the pre-flight wrote shared state (refs/worktree entries=$noimp_refs, FETCH_HEAD changed=$( [ "$noimp_fh_after" = "$noimp_fh_before" ] && echo no || echo yes ))"
else
  ok "3544-no-import: the baseline is measured correctly from the isolated store WITHOUT importing it (this repo still lacks the commit afterwards), and no ref or FETCH_HEAD is written"
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
if _cs_pass_line_ok "$(grep -m1 '^component-set: ' "$sum2" 2>/dev/null)" "component-set: PASS ($n_components/$n_components names vs origin/main $s_sha; NAMES ONLY — not implementations, and no component is run here) — baseline read via the committed manifest" \
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
# 7d. RETIRED (job 264): the private fetch ref's registration order and its cleanup.
#
#     Both cases were about a ref this pre-flight created in the live repository. It creates none
#     now — the baseline objects are read out of the isolated scratch store rather than fetched in
#     — so there is no registration to order and no ref to drop. What replaced the ordering rule
#     is broader: `_component_set_cleanup_resources` is ONE entry point covering every resource,
#     it is installed as an INT/TERM/HUP handler BEFORE any resource exists (7l), and
#     `3544-no-import` asserts that a completed slow-path run leaves no ref behind at all.
# ---------------------------------------------------------------------------

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
# 7s. STRUCTURAL: EVERY SIGNAL IN THE BOUNDED RUNNER TARGETS THE SUPERVISOR IT KEEPS ALIVE
#     (roborev job 279). Labelled structural on purpose — the race it prevents (a released pgid
#     reused by an unrelated group, most likely a peer lane's gate) cannot be constructed in a
#     test, so this asserts the PROPERTY THAT MAKES IT IMPOSSIBLE rather than the absence of the
#     symptom: the group leader is a supervisor that parks after running the command, so its pid
#     cannot be released while the escalation is in flight, and no `kill` names anything else.
#
#     Fail-closed on its own derivation: if the arm cannot be located the case FAILs rather than
#     reporting a clean scan of nothing.
# ---------------------------------------------------------------------------
# COMMENT LINES ARE EXCLUDED, and that is not cosmetic: this function's header DISCUSSES the
# signals it used to send (`kill -KILL -$pid`), so counting them made the assert report 2 of 6 and
# fail on correct code. A structural check must read code, not prose about code.
own_arm=$(awk '/^_component_set_bounded\(\) \{/,/^\}/' "$GATE" | grep -v '^[[:space:]]*#')
own_kills=$(printf '%s\n' "$own_arm" | grep -c 'kill -' || true)
own_sup_kills=$(printf '%s\n' "$own_arm" | grep -c 'kill -[A-Z]* "-\$sup"' || true)
own_park=$(printf '%s\n' "$own_arm" | grep -c 'sleep "\$(( secs + 5 ))"' || true)
own_stale=$(printf '%s\n' "$own_arm" | grep -c 'kill -[A-Z]* "-\$pid"' || true)
if [ "${own_kills:-0}" -lt 2 ] || [ "${own_park:-0}" -lt 1 ]; then
  bad "3544-owned-pgid-structural: could not locate the escalation in _component_set_bounded (kills=$own_kills park=$own_park) — the shape changed or the scan broke (fail-closed: this is not a clean result)"
elif [ "${own_stale:-0}" -ne 0 ]; then
  bad "3544-owned-pgid-structural: a signal still targets '-\$pid' ($own_stale occurrence(s)) — that id is the COMMAND's, which bash may have reaped by the time the escalation runs; signal the supervisor instead"
elif [ "${own_sup_kills:-0}" -eq "${own_kills:-0}" ]; then
  ok "3544-owned-pgid-structural: every signal ($own_sup_kills/$own_kills) targets the supervisor's group, and the supervisor parks for the bound plus 5s so its id cannot be released mid-escalation"
else
  bad "3544-owned-pgid-structural: $own_sup_kills of $own_kills signals target the supervisor — the others name an id whose ownership is not guaranteed at signal time"
fi

# ---------------------------------------------------------------------------
# 7t. THE REJECTED URL IS NOT RENDERED — SANITISING IT HARDER IS NOT THE FIX (job 282, the FIFTH
#     finding in one family: raw -> unflattened -> unredacted stderr -> scheme-only redaction ->
#     query strings and multi-`@` authorities). Every earlier fix improved the sanitiser, and the
#     set of places a secret can hide in a URL does not close, so the value is no longer published:
#     the diagnostic names the AXIS the origin was rejected on, and the normalised identity ONLY
#     when that identity is itself grammatically clean.
#
#     THE TWO SHAPES THE FINDING NAMED are driven directly, because they are the ones a redactor
#     misses: a secret in a QUERY STRING (no `@` at all, so userinfo redaction never fires) and a
#     MULTI-`@` authority (redaction to the first `@` leaves the rest). Each is checked through the
#     report-only identity hook AND through a real run's emitted line.
# ---------------------------------------------------------------------------
for _ur in 'query:https://github.com/pmcfadin/cqlite?access_token=SEK_query_3544' \
           'multiat:https://a@b:SEK_multi_3544@github.com/pmcfadin/cqlite.git'; do
  _ur_label="${_ur%%:*}"
  _ur_url="${_ur#*:}"
  _ur_secret=$(printf '%s' "$_ur_url" | sed -n 's/.*\(SEK_[a-z]*_3544\).*/\1/p')
  _ur_norm=$(bash "$GATE" --component-set-remote-identity "$_ur_url" 2>/dev/null | sed -n 's/^NORMALISED: //p')
  _ur_id=$(bash "$GATE" --component-set-remote-identity "$_ur_url" 2>/dev/null | sed -n 's/^IDENTITY: //p')
  _ur_fx="$tmp/urlrender-$_ur_label"
  mkdir -p "$_ur_fx/scripts"
  cp "$GATE" "$_ur_fx/scripts/agent-gate.sh"
  ( fx "$_ur_fx" && git init -q . && git remote add origin "$_ur_url" ) >/dev/null 2>&1
  _ur_out=$( fx "$_ur_fx" && bash "$_ur_fx/scripts/agent-gate.sh" --component-set-line full 2>/dev/null )
  _ur_line=$(field COMPONENT_SET_LINE "$_ur_out")
  if [ -z "$_ur_secret" ] || [ -z "$_ur_norm" ]; then
    bad "3544-url-not-rendered[$_ur_label]: could not build the case (secret='$_ur_secret' normalised='$_ur_norm') — fail-closed rather than assert on nothing"
  elif [ "$_ur_id" = canonical ]; then
    bad "3544-url-not-rendered[$_ur_label]: this URL was accepted as CANONICAL — a credential-bearing or query-bearing origin must be refused"
  elif printf '%s' "$_ur_norm" | grep -qF "$_ur_secret"; then
    bad "3544-url-not-rendered[$_ur_label]: the NORMALISED value carries the secret, and that value is rendered into the diagnostic"
  elif printf '%s' "$_ur_line" | grep -qF "$_ur_secret"; then
    bad "3544-url-not-rendered[$_ur_label]: the emitted component-set line carries the secret"
  elif ! printf '%s' "$_ur_line" | grep -qF 'NOT RENDERED'; then
    bad "3544-url-not-rendered[$_ur_label]: the line does not state that the URL is withheld — a reader cannot tell a withheld value from a lost one"
  else
    ok "3544-url-not-rendered[$_ur_label]: a secret in this position never reaches the normalised value or the emitted line, and the line says the URL is withheld rather than dropping it silently"
  fi
done

# ---------------------------------------------------------------------------
# 7u. THE PRE-FLIGHT RUNS INSIDE THE WINDOW IT CERTIFIES (roborev job 290, High). THE RULE, which
#     is the mirror of the earlier ruling in this issue ("a check placed AFTER the harmful effect
#     can only report it, never prevent it"):
#
#         A CHECK MUST BE INSIDE THE WINDOW IT CERTIFIES — NOT BEFORE IT, NOT AFTER THE HARM.
#
#     The pre-flight ran at the mode dispatch, BEFORE the #1825 slot wait, and
#     `_tree_recapture_after_slot` then RESET the certification window to whatever the tree was when
#     the slot was granted. So an edit made WHILE QUEUED became the new starting tree carrying a
#     STALE `component-set:` verdict — a full PASS about a set that is no longer the one being
#     dispatched. The recapture is deliberate and stays; the pre-flight is repeated inside the
#     window instead.
#
#     ONE ARM: the MANIFEST — the DATA the pre-flight re-reads from disk. It is edited while the run
#     is queued, so a verdict computed before the queue would still call the set unchanged.
#
#     THE THREE ARMS THAT COMPARED THE GATE SCRIPT ITSELF ARE GONE, WITH THE CHECK THEY DROVE
#     (issue #3705). "Is the code I am executing the code in the tree I certify" cannot be answered
#     from inside the running process: bash parses INCREMENTALLY, so any digest of `$GATE_SELF` is
#     taken after thousands of lines are already parsed, and an atomic replace before that point
#     leaves bash executing the OLD inode while the digest reads the NEW path (roborev job 294). It
#     needs a bootstrap/re-exec handshake — a change to how the gate STARTS UP — and cannot ride
#     inside a component-set comparison.
#
#     DRIVEN THROUGH THE REAL SLOT WAIT: the fixture holds the only slot with a lock file, so the
#     gate genuinely QUEUES; the manifest is edited during that queue; then the lock is released.
#     A POSITIVE CONTROL asserts the edit really landed inside the queue window (the gate had not
#     yet started work when it was made), because otherwise this case could pass by editing after
#     the run had already finished the pre-flight for unrelated reasons.
# ---------------------------------------------------------------------------
# THE BOUND on each arm's post-release wait (#3698). Generous on purpose: after the slot is granted
# the fixture gate does one ref-oracle round trip against a LOCAL origin, then file-size, then the
# dataset preflight — seconds on an idle box, and this suite shares boxes with up to four gates. The
# number is a HANG BOUND, not a performance assertion: nothing here compares elapsed time, and a case
# that trips it FAILS naming the unmet wait rather than making a claim about speed.
WIN_BOUND_SECS=120
base_win=$(mkbaseline base-window - )
win_fx=$(mkbranch windowed "$base_win" - --from-origin)
# The fixture needs the gate's OWN slot daemon, or `acquire_gate_slot` reports it missing and
# DISABLES the cap — and then the run never queues, so the edit could not land inside the window.
mkdir -p "$win_fx/scripts/lib"
cp "$SCRIPT_DIR/../lib/gate_slot_daemon.py" "$win_fx/scripts/lib/" 2>/dev/null
if [ ! -f "$win_fx/scripts/lib/gate_slot_daemon.py" ] || ! command -v python3 >/dev/null 2>&1; then
  echo "skip - 3544-preflight-in-window[manifest]: the slot daemon or python3 is unavailable, so the queue window cannot be held open"
else
# THE LOOP SHAPE IS KEPT AT ONE ARM (issue #3705 took the other three). Everything below —
# the per-arm fixture, the slot holder, the bounded wait, the raced-slot control — is written
# per `$win_edit` and reads correctly for one arm; unrolling it would be a large mechanical diff
# through the trickiest code in this file for no behavioural change.
for win_edit in manifest; do
  case "$win_edit" in
    manifest)    win_want=manifest-stale ;;
  esac
  # A FRESH FIXTURE PER EDIT: an iteration leaves its edit in place, and reusing a fixture would
  # make a later case observe an earlier case's damage rather than its own.
  win_fx=$(mkbranch "windowed-$win_edit" "$base_win" - --from-origin)
  mkdir -p "$win_fx/scripts/lib"
  cp "$SCRIPT_DIR/../lib/gate_slot_daemon.py" "$win_fx/scripts/lib/" 2>/dev/null
  win_slots="$tmp/window-slots-$win_edit"
  win_ready="$tmp/window-holder-$win_edit.ready"
  win_sum="$tmp/window-summary-$win_edit.txt"
  win_log="$tmp/window-$win_edit.log"
  win_done="$tmp/window-done-$win_edit.rc"
  rm -f "$win_done" "$win_done.part"
  mkdir -p "$win_slots"
  # HOLD THE ONLY SLOT with a second instance of the gate's own daemon, tied to a throwaway pid.
  sleep 300 &
  win_holder=$!
  python3 "$win_fx/scripts/lib/gate_slot_daemon.py" --slots-dir "$win_slots" --slots 1 \
      --gate-pid "$win_holder" --ready-file "$win_ready" --poll-secs 1 </dev/null >/dev/null 2>&1 &
  win_daemon=$!
  win_held=0
  win_i=0
  while [ "$win_i" -lt 100 ]; do
    [ -f "$win_ready" ] && { win_held=1; break; }
    sleep 0.2
    win_i=$((win_i + 1))
  done
  if [ "$win_held" -ne 1 ]; then
    bad "3544-preflight-in-window: could not hold a slot with the gate's own daemon, so the queue window cannot be opened (fail-closed rather than assert on nothing)"
    kill "$win_holder" 2>/dev/null || true
    kill "$win_daemon" 2>/dev/null || true
  else
    # THE STATUS IS RECORDED TO A FILE WITH A COMPLETENESS MARKER, not read from `wait`. Two
    # reasons, and the second is why the marker exists at all: (1) the bounded wait below may have
    # to KILL this child, and a killed child's `wait` status says nothing about the gate's verdict;
    # (2) "no status was recorded" and "the gate exited 0" must not be the same observation — an
    # unmeasured rc is a FAIL of its own, never a passing one. Same shape as the gate's own bounded
    # runner (`_component_set_bounded`), which records its status to a file with a marker for
    # exactly this reason.
    ( fx "$win_fx" && CQLITE_GATE_MAX_CONCURRENCY=1 CQLITE_GATE_SLOTS_DIR="$win_slots" \
        AGENT_GATE_SUMMARY_FILE="$win_sum" CQLITE_DATASETS_ROOT="$tmp/no-datasets" \
        bash scripts/agent-gate.sh >"$win_log" 2>&1
      printf 'RC=%s\n' "$?" >"$win_done.part" && mv "$win_done.part" "$win_done" ) &
    win_pid=$!
    # WAIT ON THE CONDITION, not a timer: the gate prints its queued notice once.
    win_queued=0
    win_j=0
    while [ "$win_j" -lt 200 ]; do
      if grep -q 'waiting for gate slot' "$win_log" 2>/dev/null; then win_queued=1; break; fi
      kill -0 "$win_pid" 2>/dev/null || break
      sleep 0.2
      win_j=$((win_j + 1))
    done
    # THE EDIT, made while the run is QUEUED:
    #   manifest — drop a component from the manifest so it no longer matches the array
    #              -> `manifest-stale`
    # THE EDIT MUST LAND WHILE THE RUN IS STILL QUEUED, and "the queued notice was printed" does not
    # prove that it did: the holder is only killed after the edit, but on a loaded box a slot could in
    # principle be granted (the holder daemon dying, a scheduling stall) between the notice and the
    # edit. The gate prints ONE line when it acquires — `gate slot acquired -- proceeding (#1825)` —
    # so the ordering is OBSERVABLE, and it is sampled either side of the edit. A case that could not
    # discriminate must SAY SO rather than be read as "the guard did not fire" (which is what a
    # non-reproducing failure of this arm looked like while this control was missing).
    win_raced=0
    if [ "$win_queued" -eq 1 ]; then
      grep -q 'gate slot acquired' "$win_log" 2>/dev/null && win_raced=1
      case "$win_edit" in
        manifest)
          # `cp` IS CORRECT HERE, AND ONLY BECAUSE THE TARGET IS DATA. The manifest is a file the
          # gate RE-READS from disk, not one any process is EXECUTING, so an in-place overwrite
          # corrupts nobody's read stream.
          #
          # NEVER `cp` OVER A SCRIPT A PROCESS IS EXECUTING. Kept here because it is the reason this
          # verb is chosen per target, and the arm that carried the full explanation has been removed
          # (#3705): bash reads a script INCREMENTALLY, keeping a byte offset into an open file, and
          # `cp` overwrites IN PLACE, SAME INODE — so the running process's next read comes from the
          # MODIFIED file at its old offset and it re-executes whatever now lives there. MEASURED,
          # not theorised: a fixture gate re-entered `acquire_gate_slot`, TWO slot daemons appeared
          # for ONE gate-pid, the ready-file was never written and the queue WEDGED FOREVER — a hang,
          # which a log filter cannot tell from a pass. `mv` is a RENAME: the running process keeps
          # its original inode while the TREE holds the new bytes, which is both safe and the
          # realistic shape (an editor saving, `git checkout`, `git stash`).
          grep -vx -- 'smoke' "$win_fx/scripts/agent-gate.components" >"$tmp/window-manifest.txt"
          cp "$tmp/window-manifest.txt" "$win_fx/scripts/agent-gate.components" ;;
      esac
      grep -q 'gate slot acquired' "$win_log" 2>/dev/null && win_raced=1
    fi
    kill "$win_holder" 2>/dev/null || true
    # ---- THE WAIT IS BOUNDED (#3698). `wait "$win_pid"` was unbounded, and a since-removed arm of
    # this loop (#3705) HUNG FOREVER under an in-place `cp` plant over the running gate script: the
    # fixture gate re-executed `acquire_gate_slot`, a second slot daemon appeared for the same
    # gate-pid, and the queue wedged permanently. THAT PLANT IS GONE AND THE BOUND STAYS ANYWAY:
    # this suite runs in `tooling-tests` on the FULL GATE, so an unbounded wait here hangs the gate of record — and a
    # gate that hangs gets waived by the next agent rather than investigated. The bound is what
    # contains the NEXT unknown wedge.
    #
    # ON EXPIRY THIS CASE FAILS. It does NOT skip: a case that cannot run is a FAIL, because a skip
    # converts a hang into a silent non-execution, which is the exact defect class #3544 is about.
    #
    # POLLED ON THE COMPLETENESS MARKER, not on `kill -0`: an exited-but-unreaped child is a ZOMBIE
    # and `kill -0` SUCCEEDS for it, so a liveness poll would never break. (The queue-detection loop
    # above polls a LOG LINE, which has no such problem; this one needs a signal that means
    # TERMINATED, and only the child can give it.)
    #
    # THE MARKER IS WRITTEN BY A RENAME, so its PRESENCE implies its CONTENT: `> "$win_done"` creates
    # the file BEFORE printf writes into it, so a poll could observe an EMPTY marker and read "no exit
    # status recorded" from a run that completed fine — an existence test standing in for a
    # completeness test. `printf > .part && mv` makes the two the same observation.
    #
    # THE KILL IS BY A PID WE OWN — never a pattern kill. `pkill -f gate_slot_daemon` (or
    # `agent-gate`) selects by what a process IS, not whose it is, and has destroyed a peer lane's
    # gate at component 28 of 30 on this fleet. The gate's own slot daemon self-reaps once its
    # `--gate-pid` is gone, so killing the gate is enough to release the fixture's slot dir.
    win_timedout=0
    win_k=0
    while [ ! -f "$win_done" ]; do
      if [ "$win_k" -ge $((WIN_BOUND_SECS * 5)) ]; then win_timedout=1; break; fi
      sleep 0.2
      win_k=$((win_k + 1))
    done
    if [ "$win_timedout" -eq 1 ]; then
      kill "$win_pid" 2>/dev/null || true
      sleep 1
      kill -KILL "$win_pid" 2>/dev/null || true
    fi
    win_rc=$(sed -n 's/^RC=//p' "$win_done" 2>/dev/null)
    # THE REAP IS BOUNDED ON BOTH PATHS, and it is bounded by construction rather than by a second
    # timer. The verdict has already been read out of the marker, so nothing below needs this child
    # alive: on the timeout path it has been SIGKILLed (unignorable), and on the normal path the
    # marker's rename was the subshell's LAST action, so all that remains is its exit — and the
    # signal makes even that not something this suite waits on. A bare `wait` here (what this line
    # used to be) is the last place a future wedge could hang `tooling-tests` inside the gate of
    # record, which is the whole point of #3698.
    kill "$win_pid" 2>/dev/null || true
    wait "$win_pid" 2>/dev/null || true
    kill "$win_daemon" 2>/dev/null || true
    wait "$win_holder" 2>/dev/null
    if [ "$win_queued" -ne 1 ]; then
      bad "3544-preflight-in-window: the gate never reported queueing for a slot, so the edit could not be made INSIDE the window — the case cannot discriminate"
      sed -n '1,5p' "$win_log" 2>/dev/null
    elif [ "$win_timedout" -eq 1 ]; then
      bad "3544-preflight-in-window[$win_edit]: the fixture gate did not COMPLETE within ${WIN_BOUND_SECS}s of the slot being released — the unmet wait is the gate exiting after its post-slot pre-flight; FAILED rather than skipped or waited on forever (#3698)"
      grep -n 'gate slot' "$win_log" 2>/dev/null | head -3
      tail -3 "$win_log" 2>/dev/null
    elif ! printf '%s' "$win_rc" | grep -qE '^[0-9]+$'; then
      bad "3544-preflight-in-window[$win_edit]: the fixture gate recorded NO exit status (marker '$win_done' absent or unparseable: '$win_rc'), so no verdict can be derived from it — an unmeasured status is not a passing one"
      tail -3 "$win_log" 2>/dev/null
    elif [ "$win_raced" -eq 1 ]; then
      bad "3544-preflight-in-window[$win_edit]: the gate had ALREADY been granted its slot when the edit was made, so the edit did not land inside the queue window — this case cannot discriminate (a loaded box or a dead holder daemon, NOT a verdict about the guard)"
      grep -n 'gate slot' "$win_log" 2>/dev/null | head -3
    elif [ "$win_rc" -ne 0 ] \
       && grep -q '^component-set: FAIL-CLOSED (#3544)' "$win_sum" 2>/dev/null \
       && grep -q "$win_want" "$win_sum" 2>/dev/null; then
      ok "3544-preflight-in-window[$win_edit]: an edit made WHILE THE RUN WAS QUEUED is caught after the slot wait ($win_want) — the check AND the input it reasons about are both inside the window the recapture opens"
    else
      bad "3544-preflight-in-window[$win_edit]: expected a FAIL-CLOSED component-set line naming $win_want after an in-queue edit (rc=$win_rc)"
      grep -E '^(RESULT|component-set|preflight)' "$win_sum" 2>/dev/null | head -4
    fi
  fi
done
fi

# ---------------------------------------------------------------------------
# 7v. HEAD'S PROVENANCE COMES FROM THE COMMITTED DECLARATION, NOT FROM HEAD'S MANIFEST (roborev
#     job 290, Medium). The asymmetry: the LOCAL manifest is checked against the LOCAL `COMPONENTS`
#     array on every run (`manifest-stale`), so it is a VERIFIED claim — while HEAD's manifest was
#     trusted with no equivalent check. A STALE manifest at HEAD, omitting a component that HEAD's
#     gate still DECLARES, then matched an uncommitted removal of that component and classified it
#     as the NON-FATAL `DECLARED`: a false green produced by the oracle that exists to refuse one.
#
#     THE FIXTURE IS THAT EXACT STATE, and its construction is the assertion: HEAD's manifest is
#     committed WITHOUT the component while HEAD's gate still declares it, and the working tree then
#     removes it from both. A run that reads HEAD's MANIFEST sees "absent at HEAD" and says
#     DECLARED; a run that reads HEAD's DECLARATION sees it present and says UNCOMMITTED.
#
#     The local `manifest-stale` guard is why the working tree must edit BOTH — otherwise the run
#     stops before provenance is ever consulted, which is a different (also correct) refusal.
# ---------------------------------------------------------------------------
# A component with a space on BOTH sides in the array: `smoke` is LAST (`… minimal-build smoke)`),
# so a ` smoke ` pattern matches nothing and the fixture builds silently wrong — which its own
# fail-closed construction check caught on the first run.
PROV_REMOVED=pub-surface
base_pv=$(mkbaseline base-provenance - )
pv_fx=$(mkbranch provenance "$base_pv" - --from-origin)
pv_ok=1
# COMMIT a stale manifest at HEAD: the component is dropped from the manifest only, leaving the
# gate's own declaration intact.
( fx "$pv_fx" && grep -vx -- "$PROV_REMOVED" scripts/agent-gate.components >../prov-manifest.txt \
  && cp ../prov-manifest.txt scripts/agent-gate.components \
  && git add -A && git "${GIT_ID[@]}" commit -qm "stale manifest at HEAD" ) >/dev/null 2>&1 || pv_ok=0
# …then remove the component in the WORKING TREE from both the array and the manifest, uncommitted.
sed "/^COMPONENTS=(/ s/ $PROV_REMOVED / /" "$pv_fx/scripts/agent-gate.sh" >"$tmp/prov-gate.sh" 2>/dev/null || pv_ok=0
if [ "$pv_ok" -ne 1 ] || cmp -s "$pv_fx/scripts/agent-gate.sh" "$tmp/prov-gate.sh"; then
  bad "3544-head-provenance-declaration: could not build the fixture (the COMPONENTS edit removing '$PROV_REMOVED' matched nothing, or the stale-manifest commit failed) — the case would test nothing"
else
  cp "$tmp/prov-gate.sh" "$pv_fx/scripts/agent-gate.sh"
  pv_head_manifest=$(git -C "$pv_fx" show "HEAD:scripts/agent-gate.components" 2>/dev/null | grep -cx -- "$PROV_REMOVED")
  pv_head_decl=$(git -C "$pv_fx" show "HEAD:scripts/agent-gate.sh" 2>/dev/null | grep -c "^COMPONENTS=(.* $PROV_REMOVED ")
  pv_out=$(hook "$pv_fx")
  pv_line=$(field COMPONENT_SET_LINE "$pv_out")
  if [ "$pv_head_manifest" -ne 0 ] || [ "$pv_head_decl" -lt 1 ]; then
    bad "3544-head-provenance-declaration: the fixture is not in the stale state (HEAD manifest has it: $pv_head_manifest want 0; HEAD declaration has it: $pv_head_decl want >=1) — the two sources do not disagree, so the case cannot discriminate"
  elif [ "$(field VERDICT "$pv_out")" = UNCOMMITTED ] \
     && [ "$(field HEAD_SRC "$pv_out")" = declaration ] \
     && grep -q 'FAIL-CLOSED (#3544)' <<<"$pv_line" \
     && grep -qw -- "$PROV_REMOVED" <<<"$pv_line" \
     && ! grep -q '^component-set: DECLARED' <<<"$pv_line"; then
    ok "3544-head-provenance-declaration: with HEAD's manifest STALE and HEAD's declaration intact, the removal is classified UNCOMMITTED from the DECLARATION — a stale manifest at HEAD can no longer excuse an uncommitted edit"
  else
    bad "3544-head-provenance-declaration: expected UNCOMMITTED from HEAD_SRC=declaration naming $PROV_REMOVED (verdict='$(field VERDICT "$pv_out")' head_src='$(field HEAD_SRC "$pv_out")')"
    printf '%s\n' "$pv_out"
  fi
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
DECLARED_KIND_COUNT=20   # 19 -> 20: +read-dir-unisolated (#3757 — `$_CS_READ_DIR` still naming
                         # the LIVE checkout, or left at the empty sentinel that `git -C ""`
                         # reads AS the live checkout, at the point an object read would be
                         # handed to git. Hardening: no reachable route today, and the refusal is
                         # what makes "this pre-flight reads no object in the live repository" a
                         # CHECKED claim rather than an ordering nobody verified.)
                         # 18 -> 19: +repo-read-blocked (roborev job 312 — a live-repository read
                         # that EXCEEDED its bound, which a config `include.path` naming a FIFO
                         # causes). The step before was 20 -> 18 (-gate-script-changed,
                         # -gate-script-unverifiable, both
                         # belonged to the in-queue "is the code I am executing the code in the
                         # tree I certify" check, which MOVED OUT of this pre-flight to issue
                         # #3705 — the question needs a bootstrap/re-exec handshake at startup
                         # (bash parses INCREMENTALLY, so no digest taken from inside the running
                         # process can answer it) and cannot ride inside a component-set
                         # comparison. Nothing about the component set regressed: the pre-flight
                         # is still REPEATED after the slot is granted (job 290).
                         # -baseline-transfer-mismatch: the transfer it detected is GONE (job
                         # 264) — the baseline objects are read out of the isolated scratch
                         # store instead of being fetched into this repository, so the class is
                         # ELIMINATED rather than detected.
                         # +baseline-ref-unparsable (the ref oracle's output is
                         # remote-controlled text and is VALIDATED, not merely parsed — job 258)
                         # +baseline-probe-unmeasured (the three-valued manifest presence
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
#       (i)  EVERY `git` invocation in the region is routed through `_component_set_bounded`
#            (or `_cs_live_git`, which supplies its own bound) — there is no annotation
#            escape any more, because three consecutive findings came through one;
#       (ii) the set of EXTERNAL PROGRAMS the region invokes equals a DECLARED list, so a
#            new one (`curl`, `ssh`, `python3`, another `bash`) cannot appear unclassified;
#       (iii) the set of GIT OPERATIONS (subcommands) it invokes equals a DECLARED list, in
#            BOTH directions — a new `git diff`/`git status`/`git archive` cannot arrive
#            unclassified, and a declared operation that no longer appears must be removed,
#            because a stale entry pre-authorises a re-introduction.
#
#     GRANULARITY, stated rather than left to be assumed: (iii) is keyed on the SUBCOMMAND, not
#     on subcommand-plus-flags. That is the granularity of the property — which git COMMANDS run
#     here decides the execution routes the enumeration comment analyses — and a flag-level key
#     would red on a reordering of correct code. A new FLAG on a declared subcommand is therefore
#     NOT caught here; it is caught by review, at the enumeration comment.
#
#     WHAT IT CLAIMS: every external invocation is CLASSIFIED. It cannot prove a command
#     is truly network-free — that is a judgement, recorded per site in the annotations and
#     in the enumeration comment at the head of the block, which is where a reviewer should
#     check it. What it does guarantee is that the judgement was MADE and is visible in the
#     diff, which is what would have caught `git show`.
# ---------------------------------------------------------------------------
region="$tmp/preflight-region.sh"
# ONE marker pair, shared with #3757's live-call allowlist (see `cs_region_stream`): a guard that
# disagreed with this one about the region boundary would move calls in and out of its subject
# silently.
cs_region_stream "$GATE" plain >"$region"
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
# classify(text, i, bounded, probe): emit EXT for the first word of <text> and GAP if it is an
# unbounded, unannotated `git`. ONE implementation, called from BOTH the de-quoted fragment loop
# and the substitution scan below — the blind spot this fixes came from having only the former.
function classify(t, i, bounded, probe,   w, cmd) {
  sub(/^[ \t]*/, "", t)
  sub(/^![ \t]*/, "", t)
  # THE CONDITION IS AN ALLOWLIST AND THE BODY MUST USE THE SAME ONE (roborev job 309). The body's
  # second `sub` matched `[a-z]+`, i.e. ANY lowercase word — so once the loop had been entered for a
  # real keyword it stripped the COMMAND as well. Measured: `if ! env -i … git init …` audited as
  # `EXT then` and NOTHING ELSE — no `env`, no `git`, no GAP — and `if ! git fetch origin main` was
  # reported as an external program named `fetch` with no GAP at all. Fourteen `if ! …` sites live in
  # this region, so the guard's own subject was partly invisible to it; `chmod` sat in
  # declared_externals for a site the census could not see. Two copies of one rule, which is the
  # drift this file keeps finding — the alternation is now written once, in both places.
  while (t ~ /^(if|while|until|then|else|elif|do|not)[ \t]+/ || t ~ /^![ \t]*/ || t ~ /^[A-Za-z_][A-Za-z0-9_]*=[^ \t]*[ \t]+/) {
    sub(/^![ \t]*/, "", t)
    sub(/^([A-Za-z_][A-Za-z0-9_]*=[^ \t]*|if|while|until|then|else|elif|do|not)[ \t]+/, "", t)
  }
  split(t, w, /[ \t]/)
  cmd = w[1]
  if (cmd == "" || cmd !~ /^[a-z_:][a-z0-9_.:-]*$/) return
  printf "EXT\t%s\n", cmd
  # `env -i VAR=… git …` (job 258): the ENVIRONMENT WRAPPER is not the command. `env` is recorded
  # above as the real external it is, and then this looks THROUGH it — otherwise wrapping a call in
  # `env` would silently EXEMPT the git behind it from the bound check.
  # LOOK THROUGH THE WRAPPERS, BOTH OF THEM. `env -i VAR=… git …` (job 258) is the ENVIRONMENT
  # wrapper and `_component_set_bounded <secs> …` (job 309) is the BOUND wrapper; neither IS the
  # command. Each is recorded above as what it is and then looked THROUGH, or wrapping a call would
  # silently exempt it. The bound wrapper had to join for the OP census below to have its own
  # subject: EVERY network-capable git here is bounded, so an operation inventory blind to that
  # form would be blind to `fetch` and `ls-remote` — the two operations it exists to notice.
  # The BOUND ARGUMENT is stripped by an ALLOWLIST of the shapes that survive the quote strip
  # (digits, or a `$`-expansion remnant), never by position: a blind "drop one token" would eat
  # the command itself at the sites where the bound was quoted and is therefore already blank.
  # `_cs_live_git` JOINED THE WRAPPER LIST, AND NOT COSMETICALLY. Routing the four live-repository
  # probes through it (job 312) took them OUT of this census: measured, the OP set lost `remote`
  # ENTIRELY and `rev-parse` fell 6 -> 3, and the GAP half could not see them either, since no
  # `git` sits at command position any more. So the very calls that were just BOUNDED became
  # invisible to the audit that checks boundedness — a coverage regression created by a fix, which
  # is why this list is derived at run time rather than believed. The wrapper supplies its own
  # bound, so a call through it is bounded BY CONSTRUCTION and the GAP half is satisfied.
  while (cmd == "env" || cmd == "_component_set_bounded" || cmd == "_cs_live_git") {
    prev = t
    if (cmd == "env") {
      sub(/^env[ \t]+/, "", t)
      while (t ~ /^-[iu][ \t]+/ || t ~ /^[A-Za-z_][A-Za-z0-9_]*=[^ \t]*[ \t]+/)
        sub(/^(-[iu]|[A-Za-z_][A-Za-z0-9_]*=[^ \t]*)[ \t]+/, "", t)
    } else if (cmd == "_cs_live_git") {
      # its arguments ARE the git arguments; synthesise the `git` the census keys on.
      sub(/^_cs_live_git[ \t]+/, "git ", t)
      bounded = 1
    } else {
      sub(/^_component_set_bounded[ \t]+/, "", t)
      sub(/^([0-9]+|\$[A-Za-z_{][^ \t]*)[ \t]+/, "", t)
    }
    if (t == prev) return          # nothing consumed: refuse rather than spin
    split(t, w, /[ \t]/)
    cmd = w[1]
    if (cmd == "" || cmd !~ /^[a-z_:][a-z0-9_.:-]*$/) return
    printf "EXT\t%s\n", cmd
  }
  # NO ANNOTATION ESCAPE (roborev job 315). `# local-only: <why>` used to excuse an unbounded git
  # call, and three consecutive findings came through it — 312, 314, 315 — each with an annotation
  # that was true about the NETWORK and silent about BLOCKING. It had ZERO remaining users once
  # every call was bounded, so it is DELETED rather than trusted a fourth time: subtraction cannot
  # introduce a false PASS. Every git call in the region must now be bounded, full stop.
  if (cmd == "git" && !bounded && !probe)
    printf "GAP\t%d\t%s\n", i, substr(line[i], 1, 60)
  # THE GIT OPERATION ITSELF (job 309). Keyed on the SUBCOMMAND, deliberately, not on subcommand
  # plus flags: the property worth guarding is which git COMMANDS run here, because that is what
  # decides the execution routes the enumeration comment analyses (hooks fire on ref writes,
  # `core.fsmonitor` only for index-reading commands like `status`/`diff`/`checkout`/`stash`,
  # textconv only for `diff`/`archive`). A new FLAG on an existing subcommand opens no new route,
  # while keying on flags would red on a reordering — a guard that reds on correct input is the
  # guard agents learn to waive. Global options are skipped, so `-C`/`--no-replace-objects` do not
  # masquerade as the subcommand.
  if (cmd == "git") {
    op = t
    if (op ~ /^git[ \t]/) sub(/^git[ \t]+/, "", op)
    else sub(/^.*[ \t]git[ \t]+/, "", op)
    while (op ~ /^-[^ \t]*[ \t]+/) sub(/^-[^ \t]*[ \t]+/, "", op)
    # REDIRECTIONS ARE NOT SUBCOMMANDS, anywhere. They are stripped here rather than special-cased
    # below, because `2>/dev/null` reaching the subcommand slot is what made the wrapper's own line
    # read as an operation named `2>/dev/null`.
    while (op ~ /^[0-9]*[<>]+[&]?[^ \t]*[ \t]*/) sub(/^[0-9]*[<>]+[&]?[^ \t]*[ \t]*/, "", op)
    sub(/^[ \t]+/, "", op)
    split(op, gw, /[ \t]/)
    # AN EMPTY SUBCOMMAND SLOT IS THE WRAPPER'S OWN PASS-THROUGH, and it is emitted as its own
    # token rather than skipped. `git "$@"` cannot be matched literally: the quote strip above
    # removes `"$@"` as a quoted span, so nothing of it survives to compare against — the slot is
    # simply empty. That is the ONE legitimate variable subcommand here (the operation is decided
    # by `_cs_live_git`'s CALLERS, censused above), and the consumer asserts it appears AT MOST
    # ONCE: an excusal with no count is an excusal a second site can hide behind.
    printf "OP\t%s\n", (gw[1] ~ /^[a-z][a-z-]*$/ ? gw[1] : (gw[1] == "" ? "<wrapper-passthrough>" : "<unrecognised:" gw[1] ">"))
  }
}
END {
  for (i = 1; i <= NR; i++) {
    if (line[i] ~ /^[ \t]*#/) continue
    l = line[i]
    # COMMAND SUBSTITUTIONS ARE CLASSIFIED FIRST, BEFORE THE QUOTE STRIP (roborev job 279). The
    # quote strip below removes `"$(` … `)"` as a matched pair, which ERASED the command inside a
    # quoted substitution — `[ -z "$(find "$f" -perm 600)" ]` left no trace of `find`, so it was
    # missing from declared_externals and the promised structural check did not notice. The claim
    # exceeded the check, inside the guard that exists to stop exactly that.
    #
    # The scan is deliberately naive — content up to the next `)` — because only the FIRST WORD is
    # needed and it is at the start. A nested substitution truncates the tail, which can lose a
    # SECOND command inside one substitution; that is a stated limit, not a silent one.
    subj = line[i]
    while (match(subj, /\$\(/)) {
      rest = substr(subj, RSTART + 2)
      subj = rest
      pclose = index(rest, ")")
      inner = (pclose > 0) ? substr(rest, 1, pclose - 1) : rest
      if (inner !~ /^\(/)   # `$((` is arithmetic, not a command
        classify(inner, i, (inner ~ /_component_set_bounded/), (inner ~ /command -v/))
    }
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
    for (f = 1; f <= n; f++)
      classify(frag[f], i, bounded, probe)
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
  # `find` (job 279): the PORTABLE exact-mode test on the isolated fetch config
  # (`find <file> -perm 600`), used because `stat` is `-c %a` on GNU and `-f %Lp` on BSD and this
  # script must run on both. LOCAL UTILITY — one named path, no recursion into a tree, no network,
  # no spawn. It was ABSENT from this list while being used, because the audit erased it: the
  # quote strip removed `"$(find …)"` as a matched pair, which is the blind spot job 279 found and
  # the substitution scan above now closes.
  # `head` and `wc` (job 297): the BOUNDED capture replay. `head -c N <file>` reads at most N bytes
  # and terminates, and `wc -c` counts what that produced — together they are the bound itself, which
  # is why they are here rather than being wrapped in one. LOCAL UTILITIES: one named path, no
  # network, no spawn, no shell. They replaced an unbounded `cat "$_CS_CAP_OUT"` that a descendant
  # outliving a successful child could keep feeding — measured off-suite as 4.1 GB in 6s with the read
  # never terminating.
  #
  # NOTE `cat` REMAINS in this set and is still used for STDERR, whose replay is best-effort by
  # design: losing part of a diagnostic must not turn a measured result into a refusal. The bound
  # matters on the STDOUT path because that is the one whose bytes are PARSED.
  declared_externals="basename cat chmod cut env find git gtimeout head kill mktemp rm sed sleep timeout tr true wc"
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
  # (iii) THE GIT OPERATION INVENTORY (roborev job 309), checked in BOTH DIRECTIONS. The
  #       enumeration comment at the head of the pre-flight block says "the whole set is
  #       enumerated here" and this test was cited beside it as the thing that asserts it — but
  #       the assert covered external program NAMES only, so the git operations were guarded by
  #       nothing and had drifted BOTH ways: `init`, `ls-remote`, `cat-file`, `rev-list` and two
  #       `rev-parse` forms had appeared unlisted, while `update-ref -d` and
  #       `rev-parse FETCH_HEAD^{commit}` were still listed with ZERO live uses.
  #
  #       AN ABSENT DECLARED OPERATION IS ALSO A FAILURE, not merely untidy: a stale entry
  #       PRE-AUTHORISES a re-introduction, which is exactly why `bash` and `mkdir` were REMOVED
  #       from declared_externals rather than left in place when the two `--list` spawns went. The
  #       same rule now applies to both sets.
  git_ops=$(printf '%s\n' "$audit_out" | sed -n 's/^OP\t//p' | sort -u)
  declared_git_ops="cat-file fetch init ls-remote ls-tree merge-base remote rev-list rev-parse show"
  undeclared_ops=""; stale_ops=""; stale_externals=""
  passthrough_n=$(printf '%s\n' "$audit_out" | sed -n 's/^OP\t//p' | grep -cx '<wrapper-passthrough>')
  for _o in $git_ops; do
    [ "$_o" = "<wrapper-passthrough>" ] && continue
    case " $declared_git_ops " in *" $_o "*) continue ;; esac
    undeclared_ops="${undeclared_ops:+$undeclared_ops }$_o"
  done
  for _o in $declared_git_ops; do
    printf '%s\n' "$git_ops" | grep -qx -- "$_o" && continue
    stale_ops="${stale_ops:+$stale_ops }$_o"
  done
  for _w in $declared_externals; do
    printf '%s\n' "$externals" | grep -qx -- "$_w" && continue
    stale_externals="${stale_externals:+$stale_externals }$_w"
  done
  if [ "$passthrough_n" -gt 1 ]; then
    bad "3544-no-unbounded: $passthrough_n \`git \"\$@\"\` pass-through sites in the pre-flight region — exactly ONE is legitimate (_cs_live_git, whose operation is decided by its callers). A second would let a variable subcommand escape the operation census behind the first one's excusal"
  elif [ -z "$git_ops" ]; then
    bad "3544-no-unbounded: the git-operation census came back EMPTY on a region of $region_lines lines — the derivation broke (fail-closed: an empty subject set would declare every operation classified)"
  elif [ -n "$git_gaps" ]; then
    bad "3544-no-unbounded: UNBOUNDED git invocation(s) in the pre-flight — every one must go through _component_set_bounded or _cs_live_git; the '# local-only' excusal was removed (job 315) after three findings came through it:"
    printf '%s\n' "$git_gaps" | while IFS= read -r _g; do echo "   $_g"; done
  elif [ -n "$undeclared" ]; then
    bad "3544-no-unbounded: UNDECLARED external program(s) in the pre-flight region: $undeclared — classify each in the enumeration comment (bounded or local-only) and add it to declared_externals"
  elif [ -n "$undeclared_ops" ]; then
    bad "3544-no-unbounded: UNDECLARED git operation(s) in the pre-flight region: $undeclared_ops — add each to the enumeration comment at the head of the pre-flight block (BOUNDED or LOCAL-ONLY, with its reason) and to declared_git_ops"
  elif [ -n "$stale_ops" ] || [ -n "$stale_externals" ]; then
    bad "3544-no-unbounded: DECLARED but NO LONGER USED — git operation(s): ${stale_ops:-none}; external program(s): ${stale_externals:-none}. Remove each from its declared set and from the enumeration comment: a stale entry pre-authorises a re-introduction (the reason `bash` and `mkdir` were removed rather than left)"
  else
    ok "3544-no-unbounded: every git invocation in the pre-flight is bounded or annotated local-only, no undeclared external program appears, and the git-operation inventory matches the region EXACTLY in both directions ($(printf '%s' "$git_ops" | wc -w | tr -d ' ') operations, region $region_lines lines)"
  fi

  # POSITIVE CONTROLS for BOTH halves, through the SAME audit program: plant each defect
  # class in a throwaway copy and require it to be reported. Without this the audit could
  # stop matching and report a clean bill of health on a region it no longer parses — the
  # shape of every finding in this issue.
  ctl_unbounded="$tmp/region-unbounded.sh"
  {   cat "$region"; printf 'run_probe() { git -C "$REPO_ROOT" fetch origin main >/dev/null 2>&1; }\n'; } >"$ctl_unbounded"
  # THE ANNOTATION CONTROL IS INVERTED (job 315): it used to prove `# local-only:` SUPPRESSED the
  # report; it now proves the excusal is GONE. An unbounded call carrying the old annotation must
  # be reported like any other — otherwise the escape is still live and three findings' worth of
  # lesson is undone silently.
  ctl_annotated="$tmp/region-annotated.sh"
  {   cat "$region"
      printf '# local-only: the old excusal, which must NO LONGER suppress the report\n'
      printf 'run_probe() { git -C "$REPO_ROOT" rev-parse --git-dir >/dev/null 2>&1; }\n'; } >"$ctl_annotated"
  ctl_curl="$tmp/region-curl.sh"
  {   cat "$region"; printf 'run_probe() { curl -sS https://example.invalid/x >/dev/null; }\n'; } >"$ctl_curl"
  # …and the same defect WRAPPED IN `env`, which is the shape job 258 introduced: if the audit
  # stopped at the wrapper, every isolated call would become unauditable at once.
  # …and the NESTED-QUOTE SUBSTITUTION shape, which is the blind spot itself: a command inside
  # `"$( … )"` was erased by the quote strip, so `find` went undeclared while being used. The
  # control plants the exact shape and requires the census to SEE it.
  ctl_qsub="$tmp/region-quoted-subst.sh"
  {   cat "$region"
      printf 'run_probe() { [ -z "$(newprog7279 "$CONF" -perm 600 -print 2>/dev/null)" ]; }\n'; } >"$ctl_qsub"
  ctl_envwrap="$tmp/region-envwrap.sh"
  {   cat "$region"; printf 'run_probe() { env -i PATH="$PATH" git -C "$REPO_ROOT" fetch origin main >/dev/null 2>&1; }\n'; } >"$ctl_envwrap"
  # …and the `if ! <cmd>` shape, which is the blind spot the loop fix closes (job 309). Before it,
  # this exact plant audited as an external program named `fetch` with NO GAP: the keyword strip
  # consumed the command that followed `!`. Fourteen live sites in this region have that shape, so
  # the control is not hypothetical — `chmod` was declared for a site the census could not see.
  ctl_ifbang="$tmp/region-ifbang.sh"
  {   cat "$region"; printf 'run_probe() { if ! git -C "$REPO_ROOT" fetch origin main; then return 1; fi; }\n'; } >"$ctl_ifbang"
  # …and a NEW GIT OPERATION behind the BOUND wrapper — the form every network-capable call here
  # uses. `git diff` is chosen deliberately: the enumeration comment's route analysis names
  # index-reading commands as the ones that would open `core.fsmonitor`, so this is the class of
  # change the inventory exists to stop arriving unclassified.
  ctl_newop="$tmp/region-newop.sh"
  {   cat "$region"; printf 'run_probe() { _component_set_bounded 5 git -C "$REPO_ROOT" diff --name-only >/dev/null 2>&1; }\n'; } >"$ctl_newop"
  ctl_gaps=$(awk -f "$GIT_AUDIT_AWK" "$ctl_unbounded" | grep -c '^GAP	')
  ctl_ann_gaps=$(awk -f "$GIT_AUDIT_AWK" "$ctl_annotated" | grep -c '^GAP	')
  ctl_curl_seen=$(awk -f "$GIT_AUDIT_AWK" "$ctl_curl" | sed -n 's/^EXT\t//p' | grep -cx curl)
  ctl_envwrap_gaps=$(awk -f "$GIT_AUDIT_AWK" "$ctl_envwrap" | grep -c '^GAP	')
  ctl_qsub_seen=$(awk -f "$GIT_AUDIT_AWK" "$ctl_qsub" | sed -n 's/^EXT\t//p' | grep -cx newprog7279)
  ctl_ifbang_gaps=$(awk -f "$GIT_AUDIT_AWK" "$ctl_ifbang" | grep -c '^GAP	')
  ctl_newop_seen=$(awk -f "$GIT_AUDIT_AWK" "$ctl_newop" | sed -n 's/^OP\t//p' | grep -cx diff)
  if [ "$ctl_gaps" -eq 1 ] && [ "$ctl_ann_gaps" -eq 1 ] && [ "$ctl_curl_seen" -ge 1 ] \
     && [ "$ctl_envwrap_gaps" -eq 1 ] && [ "$ctl_qsub_seen" -ge 1 ] \
     && [ "$ctl_ifbang_gaps" -eq 1 ] && [ "$ctl_newop_seen" -ge 1 ]; then
    ok "3544-no-unbounded-control: the audit reports a planted UNBOUNDED git (1), the same defect WRAPPED IN env (1), the same defect after \`if !\` (1 — the job-309 blind spot), a program inside a QUOTED SUBSTITUTION (the job-279 blind spot), a NEW git operation behind the bound wrapper, REPORTS an ANNOTATED one too (1 — the excusal is gone, job 315), and the census sees a planted network program — live in both directions"
  else
    bad "3544-no-unbounded-control: audit not discriminating (unbounded=$ctl_gaps expected 1, env-wrapped=$ctl_envwrap_gaps expected 1, if-bang=$ctl_ifbang_gaps expected 1, quoted-subst seen=$ctl_qsub_seen expected >=1, new-op seen=$ctl_newop_seen expected >=1, annotated=$ctl_ann_gaps expected 1 (the excusal was removed), curl seen=$ctl_curl_seen expected >=1)"
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
# canonical host — scheme forms, scp-like, `git@` (which is how ssh addresses github.com, not a
# credential), an ssh port, `www.`, a trailing `.git`, any case. USERINFO OTHER THAN `git@` IS NOW
# REJECTED (job 276): the config-file mitigation for a token-bearing URL did not hold, because git
# passes the configured URL to a transport helper whose argv then carries it. Over-rejecting one of these would red a correct checkout, and a guard that reds on
# correct input is the guard agents learn to waive.
for _u in "https://github.com/pmcfadin/cqlite.git" \
          "https://github.com/pmcfadin/cqlite" \
          "https://github.com/pmcfadin/cqlite/" \
          "https://github.com:443/pmcfadin/cqlite.git" \
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
          "ghp_scpsecret_3544@github.com:pmcfadin/cqlite.git" \
          "x-access-token:ghp_scpsecret_3544@github.com:pmcfadin/cqlite.git" \
          "https://x-access-token:ghp_example@github.com/pmcfadin/cqlite.git" \
          "https://ghp_example@github.com/pmcfadin/cqlite.git" \
          "https://gitlab.com/someone/cqlite-fork" \
          "https://evil.example/pmcfadin/cqlite" \
          "https://github.com.evil.tld/pmcfadin/cqlite" \
          "https://notgithub.com/pmcfadin/cqlite" \
          "mygithub:pmcfadin/cqlite" \
          "/data/mirrors/pmcfadin/cqlite.git" \
          "/tmp/anything/pmcfadin/cqlite" \
          "file:///tmp/x/pmcfadin/cqlite.git" \
          "ssh://git@github.com:notaport/pmcfadin/cqlite" \
          "git@github.com:pmcfadin/cqlite.git" \
          "git@github.com:/pmcfadin/cqlite.git" \
          "ssh://git@github.com/pmcfadin/cqlite.git" \
          "ssh://git@github.com:22/pmcfadin/cqlite" \
          "git+ssh://git@github.com/pmcfadin/cqlite.git" \
          "ssh+git://git@github.com/pmcfadin/cqlite.git" \
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
  ok "3544-remote-identity: every axis of the URL grammar has a rule — HTTPS ONLY + pinned host + default port + exact path accepted (ssh/git+ssh/scp are NO LONGER canonical — job 296); http/git/file, non-default ports, look-alike and unverifiable hosts, aliases, mirrors and local paths rejected"
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
# 7k. SCP-FORM USERINFO IS NOT CANONICAL, AND ITS VERDICT LEAKS NO PART OF IT (job 264, Medium).
#     `TOKEN@github.com:pmcfadin/cqlite` was accepted as the canonical upstream — the normaliser
#     dropped userinfo before comparing, which is right for identity and wrong for what happens
#     next: an ssh transport error quotes the URL it was given, and that text reaches `_CS_DETAIL`
#     and so the SUMMARY block this repository tells agents to paste into PR comments.
#
#     THIRD INSTANCE OF ONE FAMILY (rendered raw → flattened-but-not-redacted → redacted for
#     `scheme://user@` only), and the first two fixes both WIDENED THE SCRUBBER. This one
#     NARROWS WHAT IS ACCEPTED: the canonical upstream is reachable as `git@` or with no userinfo
#     at all, so every other scp userinfo is refused and never reaches the renderer. The
#     scrubber is extended too, because a REJECTED value is still rendered — and this case
#     asserts both halves, since either alone leaves a live path.
# ---------------------------------------------------------------------------
scp_secret=ghp_scp_must_not_appear_3544
scp_url="$scp_secret@github.com:pmcfadin/cqlite.git"
scp_norm=$(bash "$GATE" --component-set-remote-identity "$scp_url" 2>/dev/null | sed -n 's/^NORMALISED: //p')
scp_verdict=$(bash "$GATE" --component-set-remote-identity "$scp_url" 2>/dev/null | sed -n 's/^IDENTITY: //p')
scp_detail=$(bash "$GATE" --component-set-safe-detail "$(printf 'fatal: %s: Permission denied (publickey).\nRESULT: PASS' "$scp_url")" 2>/dev/null | sed -n 's/^SAFE_DETAIL: //p')
if [ -z "$scp_norm" ] || [ -z "$scp_detail" ]; then
  bad "3544-scp-userinfo: the identity or sanitiser hook produced nothing (norm='$scp_norm') — neither half can be asserted (fail-closed)"
elif [ "$scp_verdict" != not-canonical ]; then
  bad "3544-scp-userinfo: a credential-bearing scp remote was accepted as canonical ('$scp_verdict') — the identity check must refuse it (ONE rule now covers every URL form: userinfo must be absent or exactly 'git'), not merely drop the userinfo before comparing"
elif printf '%s' "$scp_norm" | grep -qF "$scp_secret"; then
  bad "3544-scp-userinfo: the REJECTION MARKER carries part of the value — it is rendered into _CS_DETAIL and thence into a pasted SUMMARY block"
elif printf '%s' "$scp_detail" | grep -qF "$scp_secret"; then
  bad "3544-scp-userinfo: an scp-style credential survived the sanitiser — the redactor only handled scheme://user@ forms"
elif ! printf '%s' "$scp_detail" | grep -q '<redacted>'; then
  bad "3544-scp-userinfo: no redaction marker in the sanitised text; the credential was dropped silently rather than visibly redacted"
elif printf '%s\n' "$scp_detail" | grep -qE '^RESULT:'; then
  bad "3544-scp-userinfo: the sanitised text still injects a line at column zero (flattening regressed)"
else
  ok "3544-scp-userinfo: a credential-bearing scp remote is NOT canonical, its verdict marker carries no part of the value, and the same text is redacted AND flattened if it is rendered anyway"
fi

# ---------------------------------------------------------------------------
# 7l. A SIGNAL DURING THE PROBE MUST NOT LEAVE THE SCRATCH REPOSITORY BEHIND (job 264, Medium).
#     Cleanup used to run only after the probe RETURNED, and bash runs no EXIT trap for a signal
#     that still has its default disposition — so a Ctrl-C, a supervisor's `kill`, or an ssh
#     hangup during a network operation left the isolated scratch repository on disk WITH ITS
#     CREDENTIAL-BEARING CONFIG, plus the bounded runner's capture files. Round 9 closed the
#     RETURN-path axis of "cleanup registration precedes resource creation"; this is the signal
#     axis of the same rule.
#
#     DRIVEN DETERMINISTICALLY, not on a timer: a `git` shim SLEEPS on the ref-oracle call, so the
#     probe is parked inside a bounded operation with its resources already created. The case
#     POLLS FOR THE SCRATCH DIRECTORY TO EXIST — the condition, not a delay — then signals, then
#     polls for the process to be gone. Its own precondition is asserted: if the directory never
#     appeared there is nothing to clean up and the case says so rather than passing.
#
#     HERMETIC BY TMPDIR: the invocation gets a private TMPDIR, so "no scratch directory remains"
#     is a statement about THIS run and can never read a sibling lane's probe (four lanes share
#     one box here) or be tricked by one.
# ---------------------------------------------------------------------------
base_sig=$(mkbaseline base-signal - )
sig_fx=$(mkbranch signalled "$base_sig" - )
sig_bin="$tmp/signal-bin"
mkdir -p "$sig_bin"
sig_real_git=$(command -v git 2>/dev/null)
sig_pidfile="$tmp/signal-command.pid"
{ printf '#!/bin/sh\n'
  printf 'REAL=%s\n' "$sig_real_git"
  # Park on the ref oracle: the scratch repository and its config already exist by then. The PID is
  # recorded so the case can assert the COMMAND died, not merely that the gate did (job 282).
  printf 'for a in "$@"; do if [ "$a" = ls-remote ]; then echo $$ > "%s"; sleep 60; exit 0; fi; done\n' "$sig_pidfile"
  printf 'exec "$REAL" "$@"\n'
} >"$sig_bin/git"
chmod +x "$sig_bin/git"
if [ -z "$sig_real_git" ]; then
  echo "skip - 3544-signal-cleanup: no resolvable git to build the parking shim"
else
  # BOTH BOUND MECHANISMS, because they leave DIFFERENT things to clean up (job 282). On the
  # external-`timeout` arm the bounded command is a foreground descendant of the gate, so a group
  # signal reaches it. On the BASH-WATCHDOG arm the command lives under a SUPERVISOR that `set -m`
  # put in its OWN process group — so the gate's group signal does NOT reach it, and unless the
  # supervisor is registered for cleanup the command survives the gate, unbounded, still holding
  # the capture-file paths it can recreate. That arm is the one the finding is about, and it is
  # reachable only under a curated PATH: a host with `timeout` never takes it.
  for _sig_mech in host watchdog; do
    case "$_sig_mech" in
      host)     _sig_path="$sig_bin:$PATH" ;;
      watchdog) _sig_path="$sig_bin:$bin_no_timeout" ;;
    esac
    _sig_tmp="$tmp/signal-tmpdir-$_sig_mech"
    mkdir -p "$_sig_tmp"
    rm -f "$sig_pidfile"
    # `exec`, so `$!` IS THE GATE'S OWN PID and not a wrapper subshell's — signalling the wrapper
    # left the gate running with its resources (measured: the first cut of this case reported a leak
    # that was really an unsignalled process).
    #
    # AND THE SIGNAL GOES TO THE PROCESS GROUP, which is what a terminal Ctrl-C does. Bash defers a
    # trap handler until the current FOREGROUND command completes, so a signal delivered only to the
    # gate while it is blocked in a bounded network operation cannot run the cleanup until that
    # operation finishes. Signalling the group kills the in-flight child too, which is the shape of
    # every real interruption here (Ctrl-C, a tmux/ssh hangup, a supervisor killing a lane).
    set -m
    ( fx "$sig_fx" && exec env TMPDIR="$_sig_tmp" PATH="$_sig_path" \
        bash "$sig_fx/scripts/agent-gate.sh" --component-set-line full >/dev/null 2>&1 ) &
    _sig_pid=$!
    set +m
    _sig_seen=0
    _sig_i=0
    while [ "$_sig_i" -lt 150 ]; do
      if ls -d "$_sig_tmp"/cs-baseline.* >/dev/null 2>&1 && [ -s "$sig_pidfile" ]; then _sig_seen=1; break; fi
      kill -0 "$_sig_pid" 2>/dev/null || break
      sleep 0.2
      _sig_i=$((_sig_i + 1))
    done
    _sig_cmd_pid=$(cat "$sig_pidfile" 2>/dev/null)
    kill -TERM "-$_sig_pid" 2>/dev/null || kill -TERM "$_sig_pid" 2>/dev/null || true
    wait "$_sig_pid" 2>/dev/null
    _sig_j=0
    while kill -0 "$_sig_pid" 2>/dev/null && [ "$_sig_j" -lt 50 ]; do sleep 0.2; _sig_j=$((_sig_j + 1)); done
    # The COMMAND, given a moment to die: the handler TERMs, waits a second, then KILLs.
    _sig_k=0
    while [ "$_sig_k" -lt 40 ]; do
      case "$_sig_cmd_pid" in ''|*[!0-9]*) break ;; esac
      kill -0 "$_sig_cmd_pid" 2>/dev/null || break
      sleep 0.2
      _sig_k=$((_sig_k + 1))
    done
    _sig_cmd_alive=no
    case "$_sig_cmd_pid" in
      ''|*[!0-9]*) : ;;
      *) kill -0 "$_sig_cmd_pid" 2>/dev/null && _sig_cmd_alive=yes ;;
    esac
    # Give anything that survived a chance to RECREATE the files, which is the concrete harm: a
    # command still running holds the capture paths its writer redirects to.
    sleep 1
    _sig_left=$(ls -d "$_sig_tmp"/cs-baseline.* 2>/dev/null | grep -c . || true)
    _sig_caps=$(ls "$_sig_tmp"/agent-gate-bcap.* 2>/dev/null | grep -c . || true)
    if [ "$_sig_seen" -ne 1 ]; then
      bad "3544-signal-cleanup[$_sig_mech]: the scratch repository and the command's pidfile never both appeared under the private TMPDIR, so there was nothing to leak — the case cannot discriminate (the parking shim or the probe shape changed)"
    elif [ "$_sig_cmd_alive" = yes ]; then
      bad "3544-signal-cleanup[$_sig_mech]: the bounded COMMAND (pid $_sig_cmd_pid) SURVIVED the signal — it is unbounded now and still holds the capture-file paths"
    elif [ "${_sig_left:-0}" -eq 0 ] && [ "${_sig_caps:-0}" -eq 0 ]; then
      ok "3544-signal-cleanup[$_sig_mech]: a signal during the probe stops the bounded command AND removes the isolated scratch repository (its config holds the origin URL) and the capture files — with nothing left running to recreate them"
    else
      bad "3544-signal-cleanup[$_sig_mech]: a signalled probe LEAKED (scratch dirs left=$_sig_left, capture files left=$_sig_caps)"
      ls -la "$_sig_tmp" 2>/dev/null | head -5
    fi
    reap_ticker "$sig_pidfile"
  done
fi

# ---------------------------------------------------------------------------
# 7m. THE REMOTE-HELPER ROUTE IS UNREACHABLE, NOT MERELY DETECTED (job 264, High + the lead's
#     ruling). THE RULE THIS CASE ENFORCES: a check placed AFTER a harmful effect can only report
#     it, never prevent it — so if the harm is EXECUTION, the control must be that the execution
#     cannot be REACHED. The sha-equality assert that used to guard the transfer sat DOWNSTREAM of
#     the fetch it was meant to validate, so `url.*.insteadOf` + `protocol.ext.allow=always` ran
#     commands during that fetch and the comparison never got a turn.
#
#     THE MECHANISM IS GONE, so this case asserts absence of execution rather than detection of
#     it: the pre-flight performs NO fetch in the live repository, so there is no URL for an
#     `insteadOf` to rewrite and no point at which a remote helper could be named.
#
#     THE POSITIVE CONTROL IS THE WHOLE CASE. "No marker appeared" is worthless unless the same
#     hostile configuration demonstrably EXECUTES here and now, so the control performs a plain
#     `git fetch` of a scratch-shaped path in a throwaway repository carrying the SAME config and
#     requires the helper to have run. Three details were measured rather than assumed: the
#     rewrite is a PREFIX substitution, so the helper command must tolerate the random `mktemp`
#     suffix being appended (a trailing `#` swallows it as an ignored argument); the helper must
#     contain NO quotes or spaces, because `ext::` splits its own arguments and a quoted `sh -c`
#     payload arrives mangled (measured: the shell ran but reported a syntax error, which is
#     execution WITHOUT a usable marker — a control that would have looked like a failure); and
#     the config goes into the fixture's own `.git/config` by `printf`, since `git config` splits
#     a key at its dots and the subsection here is a path.
#
#     The hostile config lives in a THROWAWAY fixture repository. Never in a worktree of the real
#     checkout: `.git/config` is shared there, and this lane has already taken `origin` out for
#     four live lanes that way (#3617).
# ---------------------------------------------------------------------------
base_ext=$(mkbaseline base-extfx "$ADD_SENTINEL")
ext_fx=$(mkbranch extfx "$base_ext" - )
ext_tip=$(git -C "$base_ext" rev-parse refs/heads/main)
ext_tmp="$tmp/ext-tmpdir"
ext_helper="$tmp/ext-helper"
ext_marker="$tmp/ext-EXECUTED"
mkdir -p "$ext_tmp"
{ printf '#!/bin/sh\n'; printf 'touch %s\n' "$ext_marker"; printf 'exit 1\n'; } >"$ext_helper"
chmod +x "$ext_helper"
# The hostile local config, in the exact shape that reached the removed hop: any URL beginning
# with the scratch prefix becomes an `ext::` helper invocation.
ext_conf() {
  { printf '[protocol "ext"]\n\tallow = always\n'
    printf '[url "ext::%s #"]\n\tinsteadOf = %s/cs-baseline.\n' "$ext_helper" "$ext_tmp"
  } >>"$1/.git/config"
}
ext_conf "$ext_fx"
# CONTROL: the same config, a plain fetch, a scratch-shaped source path.
ext_ctl="$tmp/ext-control"
mkdir -p "$ext_tmp/cs-baseline.CONTROL"
cp -R "$base_ext" "$ext_tmp/cs-baseline.CONTROL/repo" 2>/dev/null
git init -q "$ext_ctl" >/dev/null 2>&1
ext_conf "$ext_ctl"
rm -f "$ext_marker"
git -C "$ext_ctl" fetch --quiet --refmap= --no-tags "$ext_tmp/cs-baseline.CONTROL/repo" \
    "refs/heads/main:refs/csbaseline" >/dev/null 2>&1
if [ ! -f "$ext_marker" ]; then
  bad "3544-ext-helper-unreachable: the POSITIVE CONTROL did not execute the helper — protocol.ext.allow + url.*.insteadOf is not reproducible in this environment, so the pre-flight not executing it proves nothing"
else
  rm -f "$ext_marker"
  ext_out=$( fx "$ext_fx" && env TMPDIR="$ext_tmp" bash "$ext_fx/scripts/agent-gate.sh" \
               --component-set-line full 2>/dev/null )
  ext_ran=no
  [ -f "$ext_marker" ] && ext_ran=yes
  if [ "$ext_ran" = yes ]; then
    bad "3544-ext-helper-unreachable: THE HELPER RAN during the pre-flight — a remote helper is still reachable, and a downstream check cannot undo an execution"
  elif [ "$(field BASELINE_OBJECTS "$ext_out")" != fetched ]; then
    bad "3544-ext-helper-unreachable: the run did not take the SLOW path (BASELINE_OBJECTS='$(field BASELINE_OBJECTS "$ext_out")'), which is the path the removed hop lived on — the case cannot discriminate"
  elif [ "$(field VERDICT "$ext_out")" = BEHIND ] \
     && [ "$(field KIND "$ext_out")" = ok ] \
     && [ "$(field SHA "$ext_out")" = "$ext_tip" ] \
     && grep -qw -- "$SENTINEL" <<<"$(field MISSING "$ext_out")"; then
    ok "3544-ext-helper-unreachable: a local insteadOf + protocol.ext.allow=always EXECUTES a helper for a plain fetch (control) yet the pre-flight — which performs no fetch in this repository at all — runs nothing and still measures the baseline correctly"
  else
    bad "3544-ext-helper-unreachable: no execution, but the baseline was not measured (kind='$(field KIND "$ext_out")' verdict='$(field VERDICT "$ext_out")' sha='$(field SHA "$ext_out")')"
    printf '%s\n' "$ext_out"
  fi
fi

# ---------------------------------------------------------------------------
# 7n. IN A PARTIAL CLONE, NO BASELINE READ MAY REACH THE PROMISOR (roborev job 268, High).
#     A partial clone answers a read of a missing object by FETCHING it from its promisor remote,
#     under THIS repository's local config — so `url.*.insteadOf` + `protocol.ext.allow=always`
#     executes a remote helper, and the fetch also writes objects into the shared store. A local
#     read becoming a network operation is the third route of one family (`insteadOf` on the fetch,
#     `ext::` on the transfer hop, now the promisor), and per-call-site suppression had failed each
#     time — so the reads moved into the isolated repository entirely.
#
#     THE CONTROL IS THE FINDING, REPRODUCED. Measured while writing this case, in a real partial
#     clone with the hostile config: `git cat-file -e <absent-sha>` and `git show <absent-sha>:p`
#     BOTH executed the helper. Without that, "no marker appeared" would be indistinguishable from
#     a fixture that was never a partial clone or never missing an object.
#
#     THE FIXTURE'S THREE NON-OBVIOUS PROPERTIES, each measured rather than assumed:
#       * `--filter=blob:none` needs `uploadpack.allowFilter` on the source AND a NON-LOCAL
#         transport (`file://`); over a plain path the filter is ignored and everything arrives.
#       * a checked-out clone HAS its own blobs (checkout fetches them), so the object that must be
#         missing is one from a commit the lane has never materialised.
#       * `git cat-file -e` in a partial clone REPORTS PRESENT for an object it does not hold —
#         measured with `GIT_NO_LAZY_FETCH=1` set, where `cat-file -e` still answered 0 for a blob
#         whose `git show` FAILED. It answers about PROMISED objects, not local ones, so it cannot
#         be used to probe presence here at all.
#
#     WHAT DISCRIMINATES, STATED EXACTLY, because two of these three assertions are belts and only
#     one is the discriminator:
#       * THE CONTROL above proves the route is live in this fixture (a read of an absent object
#         reaches the promisor and EXECUTES the helper).
#       * `BASELINE_OBJECTS: fetched` is THE DISCRIMINATOR, and since job 299 it holds FOR A NEW
#         REASON worth stating, because the old one is gone. It used to be "a partial clone must
#         never take the fast path, because that path READS THE BASELINE IN THIS REPOSITORY" — a
#         config-derived gate, removed with the live read it protected. What forces the isolated
#         path now is OBJECT COMPLETENESS: this lane holds c2's COMMIT and not its trees or blobs,
#         so the baseline content cannot be read from here at all. RED-verified in this very
#         fixture, and it is why the precondition is a conjunction rather than `cat-file -e`:
#             cat-file -e <c2>^{commit}                          -> 0  (PRESENT)
#             rev-list --objects --missing=print --no-walk <c2>   -> ?<blob>  (ABSENT)
#         With commit-presence as the whole precondition the run took the fast path and then FAILed
#         `baseline-unreadable` — a false FAIL-CLOSED on a correct partial checkout, which is what
#         this assertion caught.
#       * "no helper ran" is a BELT. Its reachability depends on git's filtered-fetch behaviour for
#         the specific blobs this fixture leaves absent, which is why it is not relied on: with the
#         gate removed the case reds on the assertion above BEFORE this one can speak. Kept because
#         it costs nothing and would catch a future read that goes to the lane by another route.
# ---------------------------------------------------------------------------
pc_src="$tmp/pc-src"; pc_bare="$tmp/pc-src.git"; pc_lane="$tmp/pc-lane"
pc_helper="$tmp/pc-helper"; pc_marker="$tmp/pc-EXECUTED"
mkdir -p "$pc_src/scripts"
# c1 is the PLAIN gate: the clone below is made from it, so the clone's own blobs are c1's.
cp "$GATE" "$pc_src/scripts/agent-gate.sh"
printf 'partial-clone fixture\n' >"$pc_src/README.md"
git init -q --bare "$pc_bare" >/dev/null 2>&1
git -C "$pc_bare" symbolic-ref HEAD refs/heads/main >/dev/null 2>&1
git -C "$pc_bare" config uploadpack.allowFilter true >/dev/null 2>&1
pc_ok=1
( fx "$pc_src" && git init -q . && mkmanifest "$pc_src" derive \
    && git add -A && git "${GIT_ID[@]}" commit -qm c1 \
    && git push -q "$pc_bare" HEAD:refs/heads/main ) >/dev/null 2>&1 || pc_ok=0
git clone -q --filter=blob:none --no-local "file://$pc_bare" "$pc_lane" >/dev/null 2>&1 || pc_ok=0
# A SECOND baseline commit that CHANGES THE GATE AND THE MANIFEST, so its blobs are objects this
# clone has never held — it is the read of THAT manifest which, in the live repository, would go to
# the promisor. Advancing only an unrelated file would leave the manifest blob unchanged and
# therefore LOCAL, and the execution half of this case would be unreachable while looking covered
# (measured: that is exactly what the first cut did).
( fx "$pc_src" && sed "$ADD_SENTINEL" scripts/agent-gate.sh >scripts/agent-gate.sh.new \
    && mv scripts/agent-gate.sh.new scripts/agent-gate.sh \
    && mkmanifest "$pc_src" derive \
    && printf 'advanced past the clone\n' >>README.md \
    && git add -A && git "${GIT_ID[@]}" commit -qm c2 \
    && git push -q "$pc_bare" HEAD:refs/heads/main ) >/dev/null 2>&1 || pc_ok=0
pc_tip=$(git -C "$pc_bare" rev-parse refs/heads/main 2>/dev/null)
git -C "$pc_lane" fetch -q --filter=blob:none origin refs/heads/main:refs/heads/peer >/dev/null 2>&1 || pc_ok=0
# The gate's own copy, pinned to the PATH form of the remote (`file://` is deliberately not a
# canonical transport, and the filtered clone needs a non-local one).
cp "$GATE" "$pc_lane/scripts/agent-gate.sh" 2>/dev/null || pc_ok=0
git -C "$pc_lane" remote set-url origin "$pc_bare" >/dev/null 2>&1 || pc_ok=0
agent_gate_pin_canonical_remote "$pc_lane/scripts/agent-gate.sh" "$pc_bare" >/dev/null 2>&1 || pc_ok=0
agent_gate_install_components_manifest "$pc_lane/scripts/agent-gate.sh" >/dev/null 2>&1 || pc_ok=0
{ printf '#!/bin/sh\n'; printf 'touch %s\n' "$pc_marker"; printf 'exit 1\n'; } >"$pc_helper"
chmod +x "$pc_helper"
# THE PROMISOR IS A SEPARATE REMOTE FROM `origin`, and that separation is what makes the case
# discriminate rather than a convenience: `git remote get-url` APPLIES `insteadOf`, so an
# insteadOf on origin's own URL makes the pre-flight stop at `remote-not-canonical` — fail-closed,
# but it never reaches a baseline read, and the case would then be asserting nothing (measured:
# that is exactly what the first cut did). `extensions.partialClone` names the remote a lazy fetch
# uses, so pointing it at a second copy of the bare repo lets the hostile rewrite target the
# PROMISOR URL alone, leaving origin canonical.
pc_bare2="$tmp/pc-promisor.git"
cp -R "$pc_bare" "$pc_bare2" 2>/dev/null || pc_ok=0
git -C "$pc_lane" config --unset remote.origin.promisor >/dev/null 2>&1
git -C "$pc_lane" config remote.promisorsrc.url "$pc_bare2" >/dev/null 2>&1 || pc_ok=0
git -C "$pc_lane" config remote.promisorsrc.promisor true >/dev/null 2>&1 || pc_ok=0
git -C "$pc_lane" config extensions.partialclone promisorsrc >/dev/null 2>&1 || pc_ok=0
{ printf '[protocol "ext"]\n\tallow = always\n'
  printf '[url "ext::%s #"]\n\tinsteadOf = %s\n' "$pc_helper" "$pc_bare2"
} >>"$pc_lane/.git/config" 2>/dev/null || pc_ok=0
pc_promisor=$(git -C "$pc_lane" config --get-regexp 'remote\..*\.promisor|extensions\.partialclone' 2>/dev/null | grep -c . || true)
# CONTROL: a read of an object this clone does not have must reach the promisor and execute the
# helper. The sha is a hash of text no repository here contains, so it is absent by construction.
rm -f "$pc_marker"
git -C "$pc_lane" cat-file -e "$(printf 'absent-object-for-3544-partial-clone-case' | git hash-object --stdin 2>/dev/null)" >/dev/null 2>&1
pc_ctl=no
[ -f "$pc_marker" ] && pc_ctl=yes
if [ "$pc_ok" -ne 1 ] || [ -z "$pc_tip" ]; then
  echo "skip - 3544-partial-clone-unreachable: could not build the partial-clone fixture on this host (filtered clone/fetch unsupported?)"
elif [ "${pc_promisor:-0}" -lt 1 ]; then
  echo "skip - 3544-partial-clone-unreachable: the clone is not a partial clone here (no promisor remote), so the route is not exercisable"
elif [ "$pc_ctl" != yes ]; then
  bad "3544-partial-clone-unreachable: the POSITIVE CONTROL did not execute the helper — a read of an absent object did not reach the promisor in this fixture, so the pre-flight not reaching it proves nothing"
else
  rm -f "$pc_marker"
  pc_out=$( fx "$pc_lane" && bash "$pc_lane/scripts/agent-gate.sh" --component-set-line full 2>/dev/null )
  pc_ran=no
  [ -f "$pc_marker" ] && pc_ran=yes
  if [ "$pc_ran" = yes ]; then
    bad "3544-partial-clone-unreachable: THE HELPER RAN during the pre-flight — a baseline read reached the promisor remote, and a check downstream of an execution cannot undo it"
  elif [ "$(field BASELINE_OBJECTS "$pc_out")" != fetched ]; then
    bad "3544-partial-clone-unreachable: a PARTIAL clone must never take the fast path (BASELINE_OBJECTS='$(field BASELINE_OBJECTS "$pc_out")'), because that path reads the baseline in this repository"
    printf '%s\n' "$pc_out"
  elif [ "$(field KIND "$pc_out")" = ok ] && [ "$(field SHA "$pc_out")" = "$pc_tip" ]; then
    ok "3544-partial-clone-unreachable: a read of an absent object DOES reach the promisor and execute a helper here (control), yet the pre-flight measures the baseline from the isolated store and runs nothing"
  else
    bad "3544-partial-clone-unreachable: no execution, but the baseline was not measured (kind='$(field KIND "$pc_out")' sha='$(field SHA "$pc_out")' want $pc_tip)"
    printf '%s\n' "$pc_out"
  fi
fi

# ---------------------------------------------------------------------------
# 7o. ANCESTRY IS CORRECT ACROSS TWO OBJECT SOURCES, IN BOTH DIRECTIONS (job 268). The walk now
#     runs INSIDE the isolated repository, where the baseline commit is native and HEAD's comes
#     from the lane through an alternate — so the answer depends on objects from two stores being
#     visible at once, and a single-direction case could not tell a working join from a broken one
#     that happens to answer "no". `merge-base` walks COMMIT objects only, which no partial-clone
#     filter omits; that is why ancestry can cross the join while a manifest read (trees and blobs)
#     could not.
# ---------------------------------------------------------------------------
base_anc=$(mkbaseline base-ancestry "$ADD_SENTINEL")
anc_no=$(mkbranch ancestry-no "$base_anc" - )
anc_no_out=$(hook "$anc_no")
# THE `yes` DIRECTION CANNOT BE REACHED IN A NORMAL CLONE, and the reason is worth stating because
# it looks like a gap: a repository that HOLDS a descendant of the baseline necessarily holds the
# baseline commit too (history is complete in a normal clone), so "ancestor = yes" and "the baseline
# objects are absent locally" cannot both be true — the fast path would take it. The direction is
# therefore driven through the PARTIAL clone from 7n, which HOLDS c2's commit while its trees and
# blobs are filtered out.
#
# HOW THE FIXTURE WAS REBUILT (job 299). It used to push origin/main BACK to the commit the partial
# clone was made from, relying on "a partial clone always takes the isolated path". That premise is
# GONE: the fast path's precondition is now OBJECT COMPLETENESS at the baseline commit, and a
# partial clone that has CHECKED OUT that commit holds its blobs, so it legitimately reuses them —
# which reddened this case with BASELINE_OBJECTS='reused', a same-store read that cannot exercise
# the join at all. Asserting `KIND ok` and calling it covered would have been a proxy for a
# property no longer measured.
#
# SO THE LANE IS GIVEN A HEAD THAT DESCENDS FROM THE COMMIT WHOSE CONTENT IT LACKS. `commit-tree`
# on the lane's OWN index tree with c2 as the parent does it without a checkout — a checkout of c2
# would lazily FETCH exactly the blobs whose absence this case needs, and would also revert the
# pinned gate. HEAD's own objects are all local (the index tree was just written here), so HEAD's
# committed declaration still reads; the BASELINE's are not, so the run is forced onto the isolated
# path and ancestry is computed with the baseline native to the scratch and HEAD arriving through
# the alternate — the two-store join, in the `yes` direction.
anc_yes_ok=0
anc_yes_out=""
if [ "${pc_ok:-0}" -eq 1 ] && [ "${pc_promisor:-0}" -ge 1 ] && [ -n "${pc_tip:-}" ]; then
  anc_tree=""; anc_head=""
  if ( fx "$pc_lane" && git add -A ) >/dev/null 2>&1; then
    anc_tree=$( fx "$pc_lane" && git write-tree 2>/dev/null )
  fi
  if [ -n "$anc_tree" ]; then
    anc_head=$( fx "$pc_lane" && git "${GIT_ID[@]}" commit-tree "$anc_tree" -p "$pc_tip" -m local-descendant 2>/dev/null )
  fi
  if [ -n "$anc_head" ] && ( fx "$pc_lane" && git update-ref HEAD "$anc_head" ) >/dev/null 2>&1; then
    anc_yes_out=$( fx "$pc_lane" && bash "$pc_lane/scripts/agent-gate.sh" --component-set-line full 2>/dev/null )
    anc_yes_ok=1
  fi
fi
if [ "$(field BASELINE_OBJECTS "$anc_no_out")" != fetched ]; then
  bad "3544-ancestry-cross-source: the unrelated-history run did not take the isolated path (BASELINE_OBJECTS='$(field BASELINE_OBJECTS "$anc_no_out")'), so ancestry was not computed across two object sources — the case cannot discriminate"
elif [ "$anc_yes_ok" -ne 1 ]; then
  echo "skip - 3544-ancestry-cross-source: the ancestor direction needs the partial-clone fixture from 7n, which was not built on this host"
elif [ "$(field BASELINE_OBJECTS "$anc_yes_out")" != fetched ]; then
  bad "3544-ancestry-cross-source: the descendant run did not take the isolated path (BASELINE_OBJECTS='$(field BASELINE_OBJECTS "$anc_yes_out")') — a partial clone must always use the isolated store"
elif [ "$(field ANCESTOR "$anc_no_out")" = no ] \
   && [ "$(field ANCESTOR "$anc_yes_out")" = yes ] \
   && [ "$(field KIND "$anc_no_out")" = ok ] \
   && [ "$(field KIND "$anc_yes_out")" = ok ]; then
  ok "3544-ancestry-cross-source: ancestry computed INSIDE the isolated repository is right in BOTH directions with HEAD's objects arriving through an alternate (unrelated history -> no, descendant -> yes)"
else
  bad "3544-ancestry-cross-source: expected ANCESTOR no for an unrelated history and yes for a descendant (got no='$(field ANCESTOR "$anc_no_out")' yes='$(field ANCESTOR "$anc_yes_out")', kinds '$(field KIND "$anc_no_out")'/'$(field KIND "$anc_yes_out")')"
fi

# ---------------------------------------------------------------------------
# 7p. THE REUSE PATH TRANSFERS NOTHING (job 258, RESTATED by job 285). This case used to assert
#     that the reuse path creates NO SCRATCH REPOSITORY, which it no longer does — deliberately.
#     Ancestry now runs in the isolated repository on BOTH paths, because in the live repository
#     `$GIT_DIR/info/grafts` can forge parentage and `--no-replace-objects` does not disable it
#     (measured: see `3544-graft-ignored`). The reuse path keeps what it exists for — no fetch and
#     no object transfer — and gives up only the `mktemp -d` plus `git init`, which is milliseconds
#     against a 15-second bound. The assertion therefore moves from "creates nothing" to
#     "TRANSFERS nothing", which is the property that was ever worth having.
#
#     Measured rather than inferred: this repository's object count is identical before and after.
# ---------------------------------------------------------------------------
base_fp=$(mkbaseline base-fastpath - )
fp_fx=$(mkbranch fastpath "$base_fp" - --from-origin)
fp_before=$(git -C "$fp_fx" count-objects -v 2>/dev/null | tr '\n' ' ')
fp_fh_before=$(cat "$fp_fx/.git/FETCH_HEAD" 2>/dev/null || echo '<none>')
fp_out=$(hook "$fp_fx")
fp_after=$(git -C "$fp_fx" count-objects -v 2>/dev/null | tr '\n' ' ')
fp_fh_after=$(cat "$fp_fx/.git/FETCH_HEAD" 2>/dev/null || echo '<none>')
fp_refs=$(git -C "$fp_fx" for-each-ref --format='%(refname)' 'refs/worktree/*' 2>/dev/null | grep -c . || true)
if [ "$(field BASELINE_OBJECTS "$fp_out")" = reused ] \
   && [ "$(field KIND "$fp_out")" = ok ] \
   && [ "$fp_after" = "$fp_before" ] \
   && [ "$fp_fh_after" = "$fp_fh_before" ] \
   && [ "${fp_refs:-0}" -eq 0 ]; then
  ok "3544-fast-path-no-transfer: when the baseline commit is already here the run reuses it and transfers NOTHING — object count unchanged, FETCH_HEAD untouched, no ref written"
else
  bad "3544-fast-path-no-transfer: expected BASELINE_OBJECTS reused + KIND ok + an unchanged object count (objects='$(field BASELINE_OBJECTS "$fp_out")' kind='$(field KIND "$fp_out")' refs=$fp_refs; count before/after: [$fp_before] / [$fp_after])"
fi

# ---------------------------------------------------------------------------
# 7p-ii. A GRAFT MUST NOT DECIDE ANCESTRY (roborev job 285, High). `$GIT_DIR/info/grafts` rewrites
#     PARENTAGE for every read in that repository, and `--no-replace-objects` does NOT disable it —
#     they are separate mechanisms. With ancestry running in the live repository (which the reuse
#     path used to do), a graft could make `origin/main` look like an ancestor of HEAD, turning
#     missing components from a fatal BEHIND into the NON-FATAL `DECLARED`: a false green, and the
#     precise outcome this guard exists to prevent.
#
#     THREE-WAY CONTROL, because the second and third parts are what make the case meaningful:
#       plain `merge-base --is-ancestor`            -> must answer YES (the graft is effective)
#       the same with `--no-replace-objects`        -> must ALSO answer YES (the flag is no defence)
#       the pre-flight                              -> must answer NO (it reads elsewhere)
#     Measured while writing this: no / YES / YES for the first two, which is the finding.
# ---------------------------------------------------------------------------
base_gr=$(mkbaseline base-graft "$ADD_SENTINEL")
gr_fx=$(mkbranch grafted "$base_gr" - )
gr_tip=$(git -C "$base_gr" rev-parse refs/heads/main)
# Hold the baseline commit locally the way a peer's fetch would, so the run takes the REUSE path —
# the path whose ancestry used to be computed in this repository.
( fx "$gr_fx" && git fetch -q origin refs/heads/main:refs/heads/peer-fetched ) >/dev/null 2>&1
gr_head=$(git -C "$gr_fx" rev-parse HEAD 2>/dev/null)
gr_pre=$(git -C "$gr_fx" merge-base --is-ancestor "$gr_tip" "$gr_head" 2>/dev/null && echo yes || echo no)
mkdir -p "$gr_fx/.git/info"
printf '%s %s\n' "$gr_head" "$gr_tip" >"$gr_fx/.git/info/grafts"
gr_plain=$(git -C "$gr_fx" merge-base --is-ancestor "$gr_tip" "$gr_head" 2>/dev/null && echo yes || echo no)
gr_norepl=$(git -C "$gr_fx" --no-replace-objects merge-base --is-ancestor "$gr_tip" "$gr_head" 2>/dev/null && echo yes || echo no)
gr_out=$(hook "$gr_fx")
gr_line=$(field COMPONENT_SET_LINE "$gr_out")
if [ "$gr_pre" != no ] || [ "$gr_plain" != yes ] || [ "$gr_norepl" != yes ]; then
  bad "3544-graft-ignored: the CONTROLS do not establish the route (before graft='$gr_pre' want no, after='$gr_plain' want yes, with --no-replace-objects='$gr_norepl' want yes) — this git does not honour info/grafts the way the finding describes, so the pre-flight ignoring them proves nothing"
elif [ "$(field BASELINE_OBJECTS "$gr_out")" != reused ]; then
  bad "3544-graft-ignored: the run did not take the REUSE path (BASELINE_OBJECTS='$(field BASELINE_OBJECTS "$gr_out")'), which is the path whose ancestry used to run in this repository — the case cannot discriminate"
elif [ "$(field ANCESTOR "$gr_out")" = no ] \
   && [ "$(field VERDICT "$gr_out")" = BEHIND ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$gr_line" \
   && grep -qw -- "$SENTINEL" <<<"$gr_line"; then
  ok "3544-graft-ignored: a graft makes the baseline look like an ancestor for a plain merge-base AND for --no-replace-objects (controls), yet the pre-flight still answers 'not an ancestor' and FAILs as BEHIND — the walk runs in the isolated repository, where the graft does not exist"
else
  bad "3544-graft-ignored: expected ANCESTOR no + BEHIND naming $SENTINEL (got ancestor='$(field ANCESTOR "$gr_out")' verdict='$(field VERDICT "$gr_out")')"
  printf '%s\n' "$gr_out"
fi

# ---------------------------------------------------------------------------
# 7p-iii. A SYMLINK MANIFEST PUBLISHES A DIFFERENT DOCUMENT THAN IT VALIDATES (job 285, High). A
#     SYMLINK IS A BLOB — the difference is the mode — so accepting every blob made the two halves
#     read different things: the working-tree check FOLLOWS the link and sees a full manifest, while
#     `git show <rev>:<path>` prints the link's TARGET TEXT. `agent-gate.components -> fmt` then
#     passes local validation and publishes a ONE-COMPONENT baseline, after which all skew passes.
#
#     THE CONTROL IS THE ASYMMETRY ITSELF: the working-tree read must yield many components and the
#     committed object exactly one line. Without that, a refusal could be any other rejection.
# ---------------------------------------------------------------------------
base_sl=$(mkbaseline base-symlink - )
sl_fx=$(mkbranch symlinked "$base_sl" - --from-origin)
sl_ok=1
( fx "$sl_fx" && mv scripts/agent-gate.components scripts/real-manifest \
  && ln -s real-manifest scripts/agent-gate.components \
  && git add -A && git "${GIT_ID[@]}" commit -qm "symlink the manifest" ) >/dev/null 2>&1 || sl_ok=0
sl_wt=$(grep -c . "$sl_fx/scripts/agent-gate.components" 2>/dev/null || echo 0)
sl_obj=$(git -C "$sl_fx" show "HEAD:scripts/agent-gate.components" 2>/dev/null | grep -c . || echo 0)
sl_mode=$(git -C "$sl_fx" ls-tree HEAD -- scripts/agent-gate.components 2>/dev/null | cut -d' ' -f1)
sl_out=$(hook "$sl_fx")
sl_line=$(field COMPONENT_SET_LINE "$sl_out")
if [ "$sl_ok" -ne 1 ] || [ "$sl_mode" != 120000 ]; then
  echo "skip - 3544-symlink-manifest: this filesystem or git did not record a symlink manifest (mode='$sl_mode')"
elif [ "${sl_wt:-0}" -le 10 ] || [ "${sl_obj:-0}" -ne 1 ]; then
  bad "3544-symlink-manifest: the CONTROL does not show the asymmetry (working-tree read=$sl_wt components, committed object=$sl_obj line(s); want many and exactly 1) — without it a refusal proves nothing"
elif [ "$(field VERDICT "$sl_out")" = UNMEASURED ] \
   && [ "$(field KIND "$sl_out")" = manifest-garbage ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$sl_line" \
   && grep -q 'SYMLINK' <<<"$sl_line"; then
  ok "3544-symlink-manifest: a symlink manifest reads as $sl_wt components through the working tree and 1 line from the commit (control), and is REFUSED by name rather than resolved"
else
  bad "3544-symlink-manifest: expected KIND manifest-garbage naming the SYMLINK (kind='$(field KIND "$sl_out")')"
  printf '%s\n' "$sl_out"
fi

# …and the same shape at the BASELINE, where the local check cannot see it: the presence probe must
# refuse on the MODE. A branch with a perfectly good manifest is used, so the only thing wrong is
# the baseline's.
base_slb=$(mkbaseline base-symlink-baseline - )
slb_ok=1
( fx "$tmp/base-symlink-baseline-src" && mv scripts/agent-gate.components scripts/real-manifest \
  && ln -s real-manifest scripts/agent-gate.components \
  && git add -A && git "${GIT_ID[@]}" commit -qm "symlink the manifest" \
  && git push -qf "$base_slb" HEAD:refs/heads/main ) >/dev/null 2>&1 || slb_ok=0
slb_fx=$(mkbranch symlinked-baseline "$base_slb" - )
slb_mode=$(git -C "$base_slb" ls-tree refs/heads/main -- scripts/agent-gate.components 2>/dev/null | cut -d' ' -f1)
slb_out=$(hook "$slb_fx")
slb_line=$(field COMPONENT_SET_LINE "$slb_out")
if [ "$slb_ok" -ne 1 ] || [ "$slb_mode" != 120000 ]; then
  echo "skip - 3544-symlink-baseline: the baseline fixture did not record a symlink manifest (mode='$slb_mode')"
elif [ "$(field VERDICT "$slb_out")" = UNMEASURED ] \
   && [ "$(field KIND "$slb_out")" = baseline-probe-unmeasured ] \
   && grep -q 'FAIL-CLOSED (#3544)' <<<"$slb_line" \
   && grep -q 'SYMLINK' <<<"$slb_line"; then
  ok "3544-symlink-baseline: a symlink manifest AT THE BASELINE is refused on its MODE (120000), so a one-line target text can never become the published component set"
else
  bad "3544-symlink-baseline: expected KIND baseline-probe-unmeasured naming the SYMLINK (kind='$(field KIND "$slb_out")')"
  printf '%s\n' "$slb_out"
fi

# ---------------------------------------------------------------------------
# 7q. THE OBJECT READS RUN UNDER THE ALLOWLIST TOO (roborev job 276, High). They used to run under
#     a bare `env`, inheriting the caller's environment — the round-13 hole re-opened at the sites
#     the job-268 migration ADDED. An inherited `GIT_DIR` points a read at ANOTHER repository, and
#     `GIT_CONFIG_COUNT`/`KEY_0`/`VALUE_0` injects a promisor remote or an `insteadOf`, which is
#     the lazy-fetch-and-execute path the migration removed.
#
#     TWO VECTORS, EACH WITH ITS OWN POSITIVE CONTROL proving it takes effect on a plain git here:
#       GIT_DIR         — a plain `git rev-parse --git-dir` must report the INJECTED directory.
#       GIT_CONFIG_*    — a plain `git config --get` must return the INJECTED value.
#     Then the pre-flight must be unaffected: same verdict, same baseline sha, same missing set as
#     an unpolluted run of the SAME fixture. Comparing against that run rather than against a
#     hard-coded expectation is deliberate — it makes the case about the environment and nothing
#     else.
#
#     WHICH VECTOR DISCRIMINATES, measured rather than assumed: with the fix reverted (a bare
#     `env`), `GIT_DIR` reds this case — the reads land in the decoy repository and the verdict
#     becomes `baseline-probe-unmeasured`. The two config-injection vectors do NOT change the
#     outcome in this fixture even unfixed, because a promisor pointing at a remote that does not
#     exist alters nothing the reads need; they are kept because they cost one run each and would
#     catch a future read that DOES consult remote config. Saying so is the point: a case that
#     silently relies on one of three inputs reads as three times the coverage it has.
# ---------------------------------------------------------------------------
base_ei=$(mkbaseline base-envread "$ADD_SENTINEL")
ei_fx=$(mkbranch envread "$base_ei" - )
ei_decoy="$tmp/envread-decoy"
mkdir -p "$ei_decoy"
git init -q "$ei_decoy/other" >/dev/null 2>&1
ei_clean=$(hook "$ei_fx")
# CONTROL 1: GIT_DIR really redirects a plain git in this environment.
ei_gd_ctl=$( cd "$ei_fx" && GIT_DIR="$ei_decoy/other/.git" git rev-parse --git-dir 2>/dev/null )
# CONTROL 2: GIT_CONFIG_COUNT really injects config.
ei_cc_ctl=$(GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=cs3544.injected GIT_CONFIG_VALUE_0=yes \
              git config --get cs3544.injected 2>/dev/null)
if [ "$(field KIND "$ei_clean")" != ok ]; then
  bad "3544-read-env-allowlisted: the unpolluted baseline run did not reach KIND ok ('$(field KIND "$ei_clean")') — nothing to compare a polluted run against"
elif [ -z "$ei_gd_ctl" ] || [ "$ei_gd_ctl" = ".git" ]; then
  bad "3544-read-env-allowlisted: the GIT_DIR CONTROL did not redirect a plain git (got '$ei_gd_ctl') — the vector is not reproducible here, so the pre-flight being unaffected proves nothing"
elif [ "$ei_cc_ctl" != yes ]; then
  bad "3544-read-env-allowlisted: the GIT_CONFIG_COUNT CONTROL did not inject config (got '$ei_cc_ctl') — the vector is not reproducible here"
else
  ei_bad=""
  for _ei_vec in git-dir config-count config-parameters; do
    case "$_ei_vec" in
      git-dir)   _ei_env=("GIT_DIR=$ei_decoy/other/.git") ;;
      config-count)
        # A promisor remote plus an insteadOf: the exact pair that turns a read into an executing
        # fetch. If any of it reached the read, the baseline set would change or the run would fail.
        _ei_env=(GIT_CONFIG_COUNT=2
                 "GIT_CONFIG_KEY_0=remote.injected.promisor" "GIT_CONFIG_VALUE_0=true"
                 "GIT_CONFIG_KEY_1=extensions.partialclone" "GIT_CONFIG_VALUE_1=injected") ;;
      config-parameters)
        _ei_env=("GIT_CONFIG_PARAMETERS='extensions.partialclone'='injected'") ;;
    esac
    _ei_out=$( fx "$ei_fx" && env "${_ei_env[@]}" bash "$ei_fx/scripts/agent-gate.sh" \
                 --component-set-line full 2>/dev/null )
    if [ "$(field KIND "$_ei_out")" != "$(field KIND "$ei_clean")" ] \
       || [ "$(field SHA "$_ei_out")" != "$(field SHA "$ei_clean")" ] \
       || [ "$(field VERDICT "$_ei_out")" != "$(field VERDICT "$ei_clean")" ] \
       || [ "$(field MISSING "$_ei_out")" != "$(field MISSING "$ei_clean")" ]; then
      ei_bad="${ei_bad:+$ei_bad }$_ei_vec(kind=$(field KIND "$_ei_out") sha=$(field SHA "$_ei_out") verdict=$(field VERDICT "$_ei_out"))"
    fi
  done
  if [ -z "$ei_bad" ]; then
    ok "3544-read-env-allowlisted: GIT_DIR and injected git config both take effect on a plain git here (controls) yet change NOTHING about the pre-flight's verdict, baseline sha or missing set — every read runs under env -i plus the one allowlist"
  else
    bad "3544-read-env-allowlisted: the inherited environment changed the pre-flight: $ei_bad (clean run was kind=$(field KIND "$ei_clean") sha=$(field SHA "$ei_clean") verdict=$(field VERDICT "$ei_clean"))"
  fi
fi

# ---------------------------------------------------------------------------
# 7r. THE ISOLATED CONFIG'S MODE IS VERIFIED, NOT ASSUMED (roborev job 276, Medium). The 0600 was
#     applied with `|| true` — a control specified and then not required to have worked. On a
#     filesystem where the write succeeds and the chmod does not, a credential-bearing URL would
#     have been written into a broadly readable file anyway.
#
#     TWO HALVES, because "chmod exited 0" and "the file is 0600" are different claims: the mode is
#     applied AND the result is verified with `find -perm 600` (POSIX; `stat` is unusable — `-c %a`
#     is GNU and `-f %Lp` is BSD). Driven by making `chmod` fail: a stub on PATH that exits 1 for
#     this config file and forwards everything else, so the pre-flight must REFUSE and must NOT
#     have written the URL.
#
#     The positive control is the same fixture without the stub reaching a real verdict — otherwise
#     "it refused" could be any other breakage wearing that name.
# ---------------------------------------------------------------------------
base_cm=$(mkbaseline base-chmod - )
cm_fx=$(mkbranch chmodfail "$base_cm" - )
cm_ctl=$(hook "$cm_fx")
cm_real=$(command -v chmod 2>/dev/null)
cm_real_find=$(command -v find 2>/dev/null)
if [ -z "$cm_real" ] || [ -z "$cm_real_find" ]; then
  echo "skip - 3544-config-mode-verified: no resolvable chmod/find to build the failing stubs"
elif [ "$(field KIND "$cm_ctl")" != ok ]; then
  bad "3544-config-mode-verified: the POSITIVE CONTROL (same fixture, no stub) did not reach KIND ok (got '$(field KIND "$cm_ctl")') — the case cannot discriminate"
else
  # ALL THREE STATES of the check are driven, because they are three different facts with three
  # different sentences and a shared one would be false for two of them:
  #   chmod-fails  — the mode could not be SET.
  #   chmod-noop   — chmod reported success and the mode is NOT 0600 (a filesystem where the call
  #                  is accepted and ignored; the stub models it by doing nothing and exiting 0).
  #   find-fails   — the mode could not be VERIFIED. "Cannot tell" is not "the mode is wrong",
  #                  which is what `[ -z "$(find …)" ]` collapsed them into (the repository's own
  #                  `1699-find-tristate` lint caught that shape here on its first run).
  cm_bad=""
  for _cm_mode in chmod-fails chmod-noop find-fails; do
    _cm_bin="$tmp/chmod-stub-$_cm_mode"
    mkdir -p "$_cm_bin"
    case "$_cm_mode" in
      chmod-fails)
        { printf '#!/bin/sh\n'
          printf 'for a in "$@"; do case "$a" in */cs-baseline.*/repo/.git/config) exit 1 ;; esac; done\n'
          printf 'exec %s "$@"\n' "$cm_real"; } >"$_cm_bin/chmod"
        _cm_want='FAILED' ;;
      chmod-noop)
        { printf '#!/bin/sh\n'
          printf 'for a in "$@"; do case "$a" in */cs-baseline.*/repo/.git/config) exit 0 ;; esac; done\n'
          printf 'exec %s "$@"\n' "$cm_real"; } >"$_cm_bin/chmod"
        _cm_want='NOT mode 0600' ;;
      find-fails)
        { printf '#!/bin/sh\n'
          printf 'for a in "$@"; do case "$a" in */cs-baseline.*/repo/.git/config) exit 3 ;; esac; done\n'
          printf 'exec %s "$@"\n' "$cm_real_find"; } >"$_cm_bin/find"
        _cm_want='could not be VERIFIED' ;;
    esac
    chmod +x "$_cm_bin"/* 2>/dev/null
    _cm_out=$( fx "$cm_fx" && PATH="$_cm_bin:$PATH" bash "$cm_fx/scripts/agent-gate.sh" \
                 --component-set-line full 2>/dev/null )
    _cm_line=$(field COMPONENT_SET_LINE "$_cm_out")
    if [ "$(field VERDICT "$_cm_out")" != UNMEASURED ] \
       || [ "$(field KIND "$_cm_out")" != baseline-workspace ] \
       || ! grep -q 'FAIL-CLOSED (#3544)' <<<"$_cm_line" \
       || ! grep -qF "$_cm_want" <<<"$_cm_line" \
       || ! grep -q 'credential' <<<"$_cm_line"; then
      cm_bad="${cm_bad:+$cm_bad }$_cm_mode(kind=$(field KIND "$_cm_out"))"
    fi
  done
  if [ -z "$cm_bad" ]; then
    ok "3544-config-mode-verified: all three states refuse BEFORE the credential-bearing URL is written, each with its own cause — chmod failed, chmod succeeded but the mode is not 0600, and the mode could not be verified at all (control reached KIND ok)"
  else
    bad "3544-config-mode-verified: expected a fail-closed baseline-workspace naming each cause; wrong for: $cm_bad"
  fi
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
             "ssh://git@github.com/pmcfadin/cqlite.git=not-canonical" \
             "git+ssh://git@github.com/pmcfadin/cqlite.git=not-canonical" \
             "git@github.com:pmcfadin/cqlite.git=not-canonical"; do
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
   && grep -q 'rejected on: local' <<<"$fk_line" \
   && ! grep -q 'fork-origin/contributor' <<<"$fk_line" \
   && grep -q 'fork-upstream/cqlite' <<<"$fk_line"; then
  # WHAT THIS ASSERTS CHANGED WITH JOB 282, for a reason that is a property of the FIXTURE rather
  # than of the product: a rejected origin's own bytes are no longer rendered, so the line names
  # the AXIS (`local-path` here) plus the EXPECTED identity — which is this fixture's pinned
  # constant and therefore ours to print. A fork on a REAL host normalises to a clean
  # `github.com/<owner>/<repo>` and IS still rendered whole, which is what shows a reader they
  # pointed at a fork; that path is covered by the identity table's `contributor/cqlite` entry.
  # This fixture must use a LOCAL PATH origin to stay hermetic, and a local path is one of the
  # shapes whose bytes are withheld.
  ok "3544-remote-not-canonical: a re-pointed/fork origin is a NAMED non-PASS naming the AXIS and the EXPECTED identity, with its own bytes withheld (control on the same repo at a canonical path PASSes)"
else
  bad "3544-remote-not-canonical: expected KIND remote-not-canonical FAIL-CLOSED naming the rejection axis and the expected identity, and NOT the origin's own bytes"
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
# THE URL IS NOT RENDERED AT ALL ANY MORE (job 282, the FIFTH finding in this family). The
# previous version of this case required the REDACTED form to be present — `<redacted>@evil.example`
# — which was the right assertion while the value was still being published: it distinguished
# "redacted" from "silently dropped". It is the wrong assertion now, because publishing a sanitised
# URL is exactly what five successive findings kept finding a hole in (raw, unflattened,
# unredacted stderr, scp-form-only, then query strings and multi-`@` authorities). So the
# requirement inverts: the secret must be absent AND SO MUST THE URL — no host, no path — with the
# AXIS named in its place.
if [ "$leak_rc" -ne 0 ] \
   && ! grep -qF "$leak_secret" "$leak_sum" 2>/dev/null \
   && ! grep -qF "$leak_secret" "$tmp/leak.log" 2>/dev/null \
   && ! grep -qF "$leak_secret" <<<"$leak_hook" \
   && ! grep -qF 'evil.example' "$leak_sum" 2>/dev/null \
   && ! grep -qF 'evil.example' <<<"$leak_hook" \
   && grep -qF 'userinfo-bearing' "$leak_sum" 2>/dev/null \
   && grep -qF 'NOT RENDERED' "$leak_sum" 2>/dev/null \
   && grep -q 'remote-not-canonical' "$leak_sum" 2>/dev/null; then
  ok "3544-no-credential-leak: a credential-bearing origin is refused and NEITHER the secret NOR the URL (not even its host) is rendered anywhere — the rejection AXIS is named instead"
else
  bad "3544-no-credential-leak: the secret or the URL leaked, or the axis is not named (rc=$leak_rc)"
  grep -n 'component-set\|preflight' "$leak_sum" 2>/dev/null | head -4
fi

# STRUCTURAL: the expected identity must be a LITERAL. An env-derived (or config-derived)
# expected identity would be the same hole one level out — the constrained party choosing its
# own enforcer — and requirement 9's "no opt-out" would be satisfied only in spelling.
canon_assign=$(grep -n '^_CS_CANONICAL_REMOTE=' "$GATE")
# THE NEGATIVE SCAN READS CODE, NOT PROSE ABOUT CODE. A COMMENT explaining why the constant must
# not be `${…}`-derived necessarily contains both the name and a `${`, so scanning the whole file
# made this assert red on a correct tree — the same defect the pgid structural case hit from the
# other side, and the same rule: a structural check must read code. The POSITIVE greps below stay
# whole-file because they are `^`-anchored and a comment cannot satisfy them.
canon_code=$(grep -v '^[[:space:]]*#' "$GATE")
if [ -n "$canon_assign" ] \
   && [ "$(printf '%s\n' "$canon_assign" | grep -c .)" -eq 1 ] \
   && grep -q '^_CS_CANONICAL_REMOTE="[A-Za-z0-9._/-]*"$' "$GATE" \
   && grep -q '^_CS_CANONICAL_REMOTE="[A-Za-z0-9.-]*\.[A-Za-z]*/[A-Za-z0-9._-]*/[A-Za-z0-9._-]*"$' "$GATE" \
   && ! grep -qE '_CS_CANONICAL_REMOTE=.*(\$\{|\$\(|git config)' <<<"$canon_code"; then
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

# ---------------------------------------------------------------------------
# THE CASE COUNT HAS A FLOOR, AND I ADDED IT BECAUSE I SILENTLY DELETED FOUR CASES.
#
# A span-replacing edit to this file (rewriting the signal-cleanup case) overwrote the four cases
# that happened to sit between its anchors — `3544-ext-helper-unreachable`,
# `3544-partial-clone-unreachable`, `3544-ancestry-cross-source` and the fast-path case — and the
# suite went from 105 green to 102 green. NOTHING NOTICED: a shrinking suite reports `failed: 0`,
# which is the same sentence a complete one reports, and I read the tally instead of the trend for
# a full round. That is this issue's own subject — a true statement about a set that is no longer
# the coverage claim — committed inside its own test file.
#
# So the count is asserted against a FLOOR, the way `test_agent_gate_summary.sh` already does it.
# The floor is a MINIMUM, not an equality: adding cases must not require editing it, and removing
# one must be deliberate enough to edit a number with this comment attached to it.
# 105 -> 107 with the two cases job 293 added; 107 -> 103 when the in-queue gate-script check moved
# to issue #3705, taking FOUR cases with it (`3544-gate-symlink` plus the `gate-script`, `executor`
# and `unrelated` arms of `3544-preflight-in-window`). Lowered by EXACTLY the four removed, so the
# floor keeps the same slack it was written with: it still catches a DELETION without being an
# equality nobody can add a case past.
# 110 -> 113 with #3757's four cases, RAISED BY THREE and not four: `3757-head-object-fifo` can
# legitimately `skip` when the fixture's HEAD object is packed rather than loose (the same
# conditional `3544-ancestry-bounded` carries), and a floor that counts a skippable case would red
# on correct input.
# 113 -> 123 with roborev job 325's #3757 work: ELEVEN cases ADDED — the allowlist clean case + its
# FOUR planted evasion routes, the read-dir shape case + its FOUR planted controls, and the
# behavioural read-dir refusal — and ONE REMOVED (the deny-pattern `3757-no-live-peel` case the
# allowlist replaces), so the net is TEN and the floor is raised by exactly ten. All eleven are
# unconditional; the one skippable case in this family remains `3757-head-object-fifo`, which the
# floor does not count.
# 123 -> 128 with roborev job 339's five: evasion routes e-h (the same-line -C override, the
# --git-dir override, a second wrapper, a variable command word) and read-dir control v (the
# refusal reverted to a deny-list). All five unconditional.
# 128 -> 130 with roborev job 347's two: read-dir control vi (the assertion moved back down to the
# peel) and the behavioural `3757-read-dir-dominates` (no live read HAPPENS, measured from the
# gate's own progress fields against a control with the assertion deleted). Both unconditional.
# 130 -> 133 with option A (roborev job 347): the syntax-recognising half was DELETED and the
# property re-expressed as two whole-line pins, so the route set grew from EIGHT to TEN (+2: the
# round-3 `local sha=$( … )` High and 347's prefix-smuggling item 2) and the second pin brought its
# own clean case (+1). Net +3; nothing was removed from the case set, only from the mechanism.
CASE_FLOOR=133
if [ "$PASS" -lt "$CASE_FLOOR" ] && [ "$FAIL" -eq 0 ]; then
  printf 'FAIL - 3544-case-floor: %d cases ran but this suite declares a floor of %d — cases were REMOVED (or are skipping) without the floor being lowered deliberately. A green tally over a shrunken suite is the exact defect #3544 is about.\n' "$PASS" "$CASE_FLOOR"
  FAIL=$((FAIL + 1))
fi

printf '\n%s\n' "----------------------------------------"
printf 'passed: %d  failed: %d  (floor %d)\n' "$PASS" "$FAIL" "$CASE_FLOOR"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
