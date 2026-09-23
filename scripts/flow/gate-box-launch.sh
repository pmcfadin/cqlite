#!/usr/bin/env bash
# gate-box-launch.sh — the ONE committed way to start a fleet gate of record on a Linux
# box (issue #4267).
#
# WHY THIS EXISTS
# ----------------
# Every closer used to be briefed with a hand-recited list of box settings (lane
# worktree, npx-free PATH, `env -u LANE_ID`, `TMPDIR=/data/tmp`, a fresh summary path,
# the dataset root, file-growth disclosure...). Five gates of record (~3h each) were lost
# in one week to getting ONE of those wrong. This script is the single, tested substitute
# for that recital: it resolves the PR/branch head, refuses a stale base, cuts or
# refreshes a per-lane worktree from the box's canonical clone, builds the gate's
# environment from an ALLOWLIST (never the caller's ambient environment), and hands off
# to scripts/flow/gate-detached.sh.
#
# WHAT IT DOES NOT DO
# --------------------
# It does not replace scripts/flow/gate-detached.sh's own cgroup/heartbeat machinery
# (#3473) — it calls that script for the actual detached launch. It does not skip the
# #1825 single-gate-slot cap. It does not run the gate itself; --dry-run stops before any
# git or process side effect.
#
# Usage:
#   bash scripts/flow/gate-box-launch.sh <pr-number-or-branch> [options]
#
# Options:
#   --box <name>          box profile to use (scripts/flow/boxes/<name>.env). Default:
#                          `hostname -s` (or `hostname`).
#   --box-dir <path>       directory holding box profiles. Default: scripts/flow/boxes/
#                          (a committed, tracked directory). ONLY the self-test should
#                          ever point this at a scratch directory — pointing a real launch
#                          here would use an unreviewed profile.
#   --allow-file-growth    sets CQLITE_ALLOW_FILE_GROWTH=1 for this launch. This is the
#                          ONLY way that variable reaches the gate through this script.
#   --summary <path>       forwarded to gate-detached.sh. Default: a fresh path under the
#                          box profile's BOX_LOG_DIR, one per launch.
#   --log <path>           forwarded to gate-detached.sh. Default: alongside --summary.
#   --dry-run              resolve everything (box profile, PR/branch head, staleness,
#                          disk, env, command) and PRINT it; make no git/process changes.
#   -h, --help              this text.
#
# Exit codes:
#   0   launched (or, under --dry-run, resolved cleanly)
#   1   refused: a named precondition failed (stale base, npx on PATH, LANE_ID set,
#       disk below the admission bar, missing profile var, unusable dataset root...)
#   2   usage error
#   69  a capability this host needs is missing (propagated from gate-detached.sh, or a
#       missing `git`/`hostname`/`df` on this one)
set -uo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

_usage() {
  cat <<'HELPTEXT'
gate-box-launch.sh — the ONE committed way to start a fleet gate of record (#4267).

USAGE
  bash scripts/flow/gate-box-launch.sh <pr-number-or-branch> [options]

OPTIONS
  --box <name>           box profile (scripts/flow/boxes/<name>.env). Default: this
                         host's short hostname.
  --box-dir <path>        profile directory (default: scripts/flow/boxes/; self-test only).
  --allow-file-growth     sets CQLITE_ALLOW_FILE_GROWTH=1 for this launch only.
  --summary <path>        forwarded to gate-detached.sh.
  --log <path>            forwarded to gate-detached.sh.
  --dry-run               resolve and PRINT the env + command; launch nothing.
  -h, --help              this text.

EXIT CODES
  0   launched (or --dry-run resolved cleanly)
  1   refused — a named precondition failed
  2   usage error
  69  a required capability is missing on this host

See docs/development/fleet-runbook.md for the mechanism and scripts/flow/boxes/*.env for
box profiles.
HELPTEXT
}

PR_OR_BRANCH=""
BOX_NAME=""
BOX_DIR_OVERRIDE=""
ALLOW_FILE_GROWTH=0
DRY_RUN=0
OUT_SUMMARY=""
OUT_LOG=""
while [ $# -gt 0 ]; do
  case "$1" in
    --box) BOX_NAME="${2:?--box needs a name}"; shift 2 ;;
    --box-dir) BOX_DIR_OVERRIDE="${2:?--box-dir needs a path}"; shift 2 ;;
    --allow-file-growth) ALLOW_FILE_GROWTH=1; shift ;;
    --summary) OUT_SUMMARY="${2:?--summary needs a path}"; shift 2 ;;
    --log) OUT_LOG="${2:?--log needs a path}"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) _usage; exit 0 ;;
    --)
      # This launcher starts THE gate of record, always with no extra arguments — never a
      # silent passthrough. `-- --only clippy` or `-- --lite` would otherwise run a full
      # 30-50 minute gate while looking like it took the flag, which is worse than refusing
      # (roborev finding, #4267 round 2).
      if [ $# -gt 1 ]; then
        echo "gate-box-launch: unexpected arguments after '--': $*" >&2
        echo "                 This launcher always runs the full gate of record with no" >&2
        echo "                 extra flags. Run scripts/flow/gate-detached.sh directly (see" >&2
        echo "                 docs/development/fleet-runbook.md) if you need --lite/--only." >&2
        exit 2
      fi
      shift ;;
    -*)
      echo "gate-box-launch: unknown option '$1'." >&2
      _usage >&2
      exit 2 ;;
    *)
      if [ -n "$PR_OR_BRANCH" ]; then
        echo "gate-box-launch: unexpected extra argument '$1' (PR/branch already given as '$PR_OR_BRANCH')." >&2
        exit 2
      fi
      PR_OR_BRANCH="$1"; shift ;;
  esac
done
if [ -z "$PR_OR_BRANCH" ]; then
  echo "gate-box-launch: a PR number or branch name is required." >&2
  _usage >&2
  exit 2
fi

for _tool in git hostname df; do
  command -v "$_tool" >/dev/null 2>&1 || {
    echo "gate-box-launch: required tool '$_tool' is not on PATH." >&2
    exit 69
  }
done

# ---------------------------------------------------------------------------
# 1. Resolve and load the box profile.
# ---------------------------------------------------------------------------
if [ -z "$BOX_NAME" ]; then
  BOX_NAME="$(hostname -s 2>/dev/null || true)"
  [ -n "$BOX_NAME" ] || BOX_NAME="$(hostname 2>/dev/null || true)"
fi
if [ -z "$BOX_NAME" ]; then
  echo "gate-box-launch: could not determine this host's name (hostname gave nothing)," >&2
  echo "                 and no --box was given. Pass --box <name> explicitly." >&2
  exit 2
fi
# BOX_NAME becomes part of a filesystem path below — reject anything that could walk out
# of the box directory or be read as an option (roborev finding, #4267 round 2).
case "$BOX_NAME" in
  */*|*..*|-*)
    echo "gate-box-launch: refusing box name '$BOX_NAME' — it must not contain '/', '..', or" >&2
    echo "                 start with '-'. Pass an explicit --box <name>." >&2
    exit 2 ;;
esac
BOXES_DIR="${BOX_DIR_OVERRIDE:-$REPO_ROOT/scripts/flow/boxes}"
BOX_PROFILE="$BOXES_DIR/$BOX_NAME.env"
if [ ! -f "$BOX_PROFILE" ]; then
  echo "gate-box-launch: no committed profile at '$BOX_PROFILE'." >&2
  echo "                 Known profiles:" >&2
  for _p in "$BOXES_DIR"/*.env; do
    [ -e "$_p" ] && echo "                   $(basename "$_p" .env)" >&2
  done
  echo "                 Add one (see scripts/flow/boxes/astro-processor.env for the shape)" >&2
  echo "                 or pass --box <name> to select an existing one." >&2
  exit 1
fi
# The profile is committed, reviewed source (same trust boundary as any other script in
# this repo) — plain NAME=value assignments, sourced directly rather than re-parsed.
# shellcheck source=/dev/null
. "$BOX_PROFILE"

for _v in BOX_CANONICAL_CLONE BOX_LANES_DIR BOX_TMPDIR BOX_DATASETS_ROOT BOX_PATH \
          BOX_JOBS BOX_RUST_TEST_THREADS BOX_MAX_CONCURRENCY BOX_MIN_FREE_GB BOX_LOG_DIR; do
  if [ -z "${!_v:-}" ]; then
    echo "gate-box-launch: profile '$BOX_PROFILE' does not set required variable $_v." >&2
    exit 1
  fi
done
# Four of those must be PLAIN NON-NEGATIVE INTEGERS — every one feeds a `-lt`/arithmetic
# comparison below, and bash's `[ n -lt m ]` on a non-numeric operand errors to stderr and
# evaluates FALSE, i.e. the PERMISSIVE branch. For BOX_MIN_FREE_GB specifically that means a
# typo (`"150G"`) silently ADMITS every launch past the one check meant to prevent the vhdx
# exhaustion incidents (roborev finding, #4267 round 2) — refuse by name instead.
for _v in BOX_JOBS BOX_RUST_TEST_THREADS BOX_MAX_CONCURRENCY BOX_MIN_FREE_GB; do
  case "${!_v}" in
    *[!0-9]*|'')
      echo "gate-box-launch: profile '$BOX_PROFILE' sets $_v='${!_v}', which is not a plain" >&2
      echo "                 non-negative integer. Refusing rather than silently admitting" >&2
      echo "                 past a numeric check that would otherwise error and pass." >&2
      exit 1 ;;
  esac
done

echo "gate-box-launch: box=$BOX_NAME profile=$BOX_PROFILE"

# ---------------------------------------------------------------------------
# 2. Env scrubbing: refuse the two hazards a hand-recited briefing kept re-explaining.
# ---------------------------------------------------------------------------
# LANE_ID must NEVER reach a gate launched from a linked lane worktree (#4252 r4, cost
# 5046s): the worker-supervisor self-tests exercise DERIVED identity and an explicit
# LANE_ID in the inherited environment fails both. It was only ever a primary-clone
# workaround, and this launcher always uses a linked worktree — so an inherited LANE_ID
# is refused outright rather than silently dropped (a silent drop would leave the caller
# believing their override took effect).
if [ -n "${LANE_ID+x}" ]; then
  echo "gate-box-launch: REFUSING — LANE_ID='${LANE_ID}' is set in the calling environment." >&2
  echo "                 A gate launched from a linked lane worktree must NEVER carry LANE_ID" >&2
  echo "                 (it fails the worker-supervisor's derived-identity self-tests, #4252)." >&2
  echo "                 Unset it: env -u LANE_ID bash scripts/flow/gate-box-launch.sh ..." >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# 3. Build the gate's PATH from the profile's allowlist and assert it cannot resolve npx
#    (#4238: with npx resolvable, two cqlite-core tests time out at 240s).
# ---------------------------------------------------------------------------
GATE_PATH="$BOX_PATH"
if _npx_hit=$(PATH="$GATE_PATH" command -v npx 2>/dev/null) && [ -n "$_npx_hit" ]; then
  echo "gate-box-launch: REFUSING — 'npx' resolves on the profile's PATH, at '$_npx_hit'." >&2
  echo "                 With npx resolvable, test_negative_tests_coordination and" >&2
  echo "                 test_hooks_integration shell out to 'npx claude-flow@alpha' and" >&2
  echo "                 time out at 240s (#4238). Fix scripts/flow/boxes/$BOX_NAME.env's" >&2
  echo "                 BOX_PATH so it never contains an npx-bearing directory (node/npm" >&2
  echo "                 should come from symlinks with no npx sibling)." >&2
  exit 1
fi
echo "gate-box-launch: PATH is npx-free (asserted)."

# ---------------------------------------------------------------------------
# 4. Resolve the PR/branch head against the box's canonical clone, and refuse a stale
#    base (CLAUDE.md: "A gate script behind origin/main cannot certify. Rebase before
#    the gate of record.") — the mechanized form of that rule.
# ---------------------------------------------------------------------------
if [ ! -d "$BOX_CANONICAL_CLONE/.git" ]; then
  echo "gate-box-launch: profile's BOX_CANONICAL_CLONE '$BOX_CANONICAL_CLONE' is not a git" >&2
  echo "                 checkout (no .git). A PRIMARY clone under a scratch dir fails 68" >&2
  echo "                 worker-supervisor cases (#3393) — this must be the box's own" >&2
  echo "                 long-lived clone that lane worktrees are cut FROM." >&2
  exit 1
fi

_git_clone() { git -C "$BOX_CANONICAL_CLONE" "$@"; }

if ! _git_clone fetch origin --quiet main 2>&1; then
  echo "gate-box-launch: 'git fetch origin main' failed in '$BOX_CANONICAL_CLONE'." >&2
  exit 1
fi
ORIGIN_MAIN_SHA=$(_git_clone rev-parse origin/main 2>/dev/null) || {
  echo "gate-box-launch: could not resolve origin/main in '$BOX_CANONICAL_CLONE' after fetch." >&2
  exit 1
}

case "$PR_OR_BRANCH" in
  ''|*[!0-9]*) IS_PR=0 ;;
  *)           IS_PR=1 ;;
esac

if [ "$IS_PR" -eq 1 ]; then
  if ! _git_clone fetch origin --quiet "refs/pull/${PR_OR_BRANCH}/head" 2>&1; then
    echo "gate-box-launch: could not fetch refs/pull/${PR_OR_BRANCH}/head from origin." >&2
    echo "                 Check the PR number, and that this clone's 'origin' remote is" >&2
    echo "                 the GitHub repo (PR refs are a GitHub-side mechanism)." >&2
    exit 1
  fi
  LANE_NAME="pr-${PR_OR_BRANCH}"
else
  if ! _git_clone fetch origin --quiet "$PR_OR_BRANCH" 2>&1; then
    echo "gate-box-launch: could not fetch branch '$PR_OR_BRANCH' from origin." >&2
    exit 1
  fi
  # Lane directory names must never smuggle a path separator or leading dash from a
  # branch name into BOX_LANES_DIR.
  LANE_NAME=$(printf '%s' "$PR_OR_BRANCH" | tr -c 'A-Za-z0-9_.-' '-')
  case "$LANE_NAME" in -*) LANE_NAME="branch-$LANE_NAME" ;; esac
fi
HEAD_SHA=$(_git_clone rev-parse FETCH_HEAD 2>/dev/null) || {
  echo "gate-box-launch: could not resolve FETCH_HEAD after fetching '$PR_OR_BRANCH'." >&2
  exit 1
}

if ! _git_clone merge-base --is-ancestor "$ORIGIN_MAIN_SHA" "$HEAD_SHA" 2>/dev/null; then
  _behind=$(_git_clone rev-list --count "${HEAD_SHA}..${ORIGIN_MAIN_SHA}" 2>/dev/null || echo unknown)
  echo "gate-box-launch: REFUSING — $PR_OR_BRANCH (head $HEAD_SHA) does not have current" >&2
  echo "                 origin/main ($ORIGIN_MAIN_SHA) as an ancestor; it is $_behind" >&2
  echo "                 commit(s) behind. 'A gate script behind origin/main cannot" >&2
  echo "                 certify. Rebase before the gate of record.' (CLAUDE.md)" >&2
  exit 1
fi
echo "gate-box-launch: $PR_OR_BRANCH resolves to $HEAD_SHA, a descendant of origin/main ($ORIGIN_MAIN_SHA)."

# ---------------------------------------------------------------------------
# 5. Disk admission — report always, refuse below the bar (both directions of the WSL
#    vhdx incidents in the memory file: `/mnt/r` and `df -h /` inside WSL can disagree).
# ---------------------------------------------------------------------------
_disk_free_gb() {  # <path> -> free GB, or empty if unmeasurable
  local kb
  kb=$(df -Pk "$1" 2>/dev/null | awk 'NR==2 {print $4}')
  case "$kb" in ''|*[!0-9]*) return 1 ;; esac
  printf '%s' "$((kb / 1024 / 1024))"
}
_check_disk() {  # <path> <label>
  local path="$1" label="$2" free measure_path="$1"
  if [ ! -d "$path" ]; then
    # Walk up to the nearest EXISTING ancestor so a not-yet-created lanes/tmp directory
    # does not silently skip the admission check — it is the underlying filesystem's
    # free space that matters, not whether this exact directory has been mkdir'd yet.
    measure_path="$path"
    while [ ! -d "$measure_path" ] && [ "$measure_path" != "/" ] && [ -n "$measure_path" ]; do
      measure_path=$(dirname -- "$measure_path")
    done
    echo "gate-box-launch: NOTE — $label path '$path' does not exist yet (will be created);" >&2
    echo "                 measuring free space on its nearest existing ancestor '$measure_path'." >&2
  fi
  if ! free=$(_disk_free_gb "$measure_path"); then
    echo "gate-box-launch: REFUSING — could not measure free space for $label ('$path')." >&2
    return 1
  fi
  echo "gate-box-launch: disk($label): ${free}G free at $path (admission bar ${BOX_MIN_FREE_GB}G)"
  if [ "$free" -lt "$BOX_MIN_FREE_GB" ]; then
    echo "gate-box-launch: REFUSING — $label has only ${free}G free, below the ${BOX_MIN_FREE_GB}G" >&2
    echo "                 admission bar. See the astro-processor memory notes: the WSL vhdx has" >&2
    echo "                 been driven to emergency_ro by exactly this before." >&2
    return 1
  fi
  return 0
}
_DISK_OK=1
_check_disk "$BOX_LANES_DIR" "lanes" || _DISK_OK=0
_check_disk "$BOX_TMPDIR" "tmpdir" || _DISK_OK=0
if [ "$_DISK_OK" -ne 1 ]; then
  exit 1
fi

# ---------------------------------------------------------------------------
# 6. Best-effort busy-lane check: refuse if a live cqlite-gate-* unit's process is
#    already running inside this lane. Declared, not exhaustively provable from shell —
#    absence of systemctl or /proc degrades to a printed NOTE, never a silent pass.
# ---------------------------------------------------------------------------
LANE_DIR="$BOX_LANES_DIR/$LANE_NAME"
if command -v systemctl >/dev/null 2>&1; then
  while IFS= read -r _unit; do
    [ -n "$_unit" ] || continue
    _pid=$(systemctl --user show -p ExecMainPID --value "$_unit" 2>/dev/null || echo 0)
    case "$_pid" in ''|*[!0-9]*|0) continue ;; esac
    [ -e "/proc/$_pid/cwd" ] || continue
    _cwd=$(readlink -f "/proc/$_pid/cwd" 2>/dev/null || true)
    case "$_cwd" in
      "$LANE_DIR"|"$LANE_DIR"/*)
        echo "gate-box-launch: REFUSING — unit '$_unit' (pid $_pid) is already running in" >&2
        echo "                 lane '$LANE_DIR'. Only one gate may occupy a lane at a time." >&2
        exit 1 ;;
    esac
  done < <(systemctl --user list-units 'cqlite-gate-*' --no-legend --plain 2>/dev/null | awk '{print $1}')
else
  echo "gate-box-launch: NOTE — no systemctl on PATH; skipped the busy-lane check (best-effort)." >&2
fi

# ---------------------------------------------------------------------------
# 7. Verify the dataset root (never mutates: fetch-datasets.sh --verify-only). Skipped
#    under --dry-run — it is safe, but a --dry-run promises to touch nothing beyond git
#    reads and df, so a caller resolving a box profile from a machine without that
#    dataset root (e.g. this repo's own worktree, off-box) still gets a clean resolve.
# ---------------------------------------------------------------------------
if [ "$DRY_RUN" -eq 0 ]; then
  if ! CQLITE_DATASETS_ROOT="$BOX_DATASETS_ROOT" bash "$BOX_CANONICAL_CLONE/test-data/scripts/fetch-datasets.sh" --verify-only; then
    echo "gate-box-launch: REFUSING — dataset root '$BOX_DATASETS_ROOT' is not usable" >&2
    echo "                 (fetch-datasets.sh --verify-only failed). See its output above." >&2
    exit 1
  fi
else
  echo "gate-box-launch: [dry-run] would verify dataset root '$BOX_DATASETS_ROOT' via" \
       "fetch-datasets.sh --verify-only"
fi

# ---------------------------------------------------------------------------
# 8. Cut or refresh the lane worktree.
# ---------------------------------------------------------------------------
# `git worktree add` writes `.git` as a REGULAR FILE (`gitdir: …`), never a directory —
# verified against this repo's own git. `[ -d "$LANE_DIR/.git" ]` is therefore false for
# EVERY lane this launcher itself created, so control fell into the `else` (create)
# branch on every refresh, which failed against an already-registered worktree and told
# the operator to `worktree remove --force`/`rm -rf` it — destroying the 120-190G in-tree
# target/ this design exists to preserve (roborev High finding, #4267 round 2). Test
# worktree MEMBERSHIP instead of a directory shape.
_lane_is_worktree() {  # <dir> -> 0 if it is a git worktree (linked or otherwise)
  [ -e "$1/.git" ] && git -C "$1" rev-parse --is-inside-work-tree >/dev/null 2>&1
}
if [ "$DRY_RUN" -eq 1 ]; then
  if _lane_is_worktree "$LANE_DIR"; then
    echo "gate-box-launch: [dry-run] would refresh existing lane worktree '$LANE_DIR' to $HEAD_SHA"
  else
    echo "gate-box-launch: [dry-run] would create lane worktree '$LANE_DIR' @ $HEAD_SHA"
  fi
else
  mkdir -p "$BOX_LANES_DIR" || {
    echo "gate-box-launch: could not create BOX_LANES_DIR '$BOX_LANES_DIR'." >&2
    exit 1
  }
  if _lane_is_worktree "$LANE_DIR"; then
    if ! git -C "$LANE_DIR" fetch origin --quiet 2>&1; then
      echo "gate-box-launch: could not fetch in existing lane worktree '$LANE_DIR'." >&2
      exit 1
    fi
    # --force so a tracked-file modification left by an interrupted prior run (a stopped
    # fix round, a generated file) is DISCARDED rather than either blocking the checkout
    # or riding along into this run — a dirty lane stamps `dirty: yes` in the gate's own
    # summary, which by CLAUDE.md means it cannot certify, and that costs the 30-50 minute
    # run this launcher exists to protect (roborev finding, #4267 round 2). Untracked
    # files (target/, the dataset root) are never touched by --force.
    if ! git -C "$LANE_DIR" checkout --quiet --force --detach "$HEAD_SHA" 2>&1; then
      echo "gate-box-launch: could not check out $HEAD_SHA in lane worktree '$LANE_DIR'." >&2
      exit 1
    fi
    _dirty=$(git -C "$LANE_DIR" status --porcelain --untracked-files=no 2>/dev/null)
    if [ -n "$_dirty" ]; then
      echo "gate-box-launch: REFUSING — lane worktree '$LANE_DIR' still has tracked-file" >&2
      echo "                 changes after a forced checkout:" >&2
      echo "$_dirty" | sed 's/^/                   /' >&2
      exit 1
    fi
  else
    # Prune stale worktree registrations first: a lane directory left over from a removed
    # `.git` file (a half-cleaned lane) makes `worktree add` fail with "already
    # registered"/"already exists" and no remedy (roborev finding, #4267 round 2).
    _git_clone worktree prune 2>&1 || true
    if ! _git_clone worktree add --quiet --detach "$LANE_DIR" "$HEAD_SHA" 2>&1; then
      echo "gate-box-launch: 'git worktree add' failed for '$LANE_DIR' @ $HEAD_SHA." >&2
      echo "                 If '$LANE_DIR' exists but is not a usable worktree, remove it:" >&2
      echo "                   git -C '$BOX_CANONICAL_CLONE' worktree remove --force '$LANE_DIR'" >&2
      echo "                 or, if that fails too: rm -rf '$LANE_DIR'" >&2
      exit 1
    fi
  fi
  echo "gate-box-launch: lane worktree ready: $LANE_DIR @ $HEAD_SHA"
fi

# ---------------------------------------------------------------------------
# 9. Build the gate environment from an ALLOWLIST — never the caller's ambient
#    environment (env -i semantics: only what is listed here reaches the gate).
# ---------------------------------------------------------------------------
GATE_ENV=(
  "HOME=${HOME:-/root}"
  "LANG=C.UTF-8"
  "PATH=$GATE_PATH"
  "AGENT_GATE_JOBS=$BOX_JOBS"
  "AGENT_GATE_KEEP_LOGS=1"
  "CQLITE_GATE_MAX_CONCURRENCY=$BOX_MAX_CONCURRENCY"
  "CQLITE_DATASETS_ROOT=$BOX_DATASETS_ROOT"
  "RUST_TEST_THREADS=$BOX_RUST_TEST_THREADS"
  "TMPDIR=$BOX_TMPDIR"
)
if [ "$ALLOW_FILE_GROWTH" -eq 1 ]; then
  GATE_ENV+=("CQLITE_ALLOW_FILE_GROWTH=1")
fi
# gate-detached.sh REQUIRES a working `systemd-run --user` (its own precondition checks),
# and sd-bus resolves the user bus from DBUS_SESSION_BUS_ADDRESS, else
# $XDG_RUNTIME_DIR/bus — both stripped by `env -i` and neither was in the original
# allowlist, so the first real launch would fail the systemd-run probe and exit 69 naming
# the box rather than this gap (roborev finding, #4267 round 2).
# gate-detached.sh:1087 already treats /run/user/<uid> as XDG_RUNTIME_DIR's canonical
# value, so that is the default when the caller's environment does not set it.
_uid=$(id -u 2>/dev/null || echo "")
GATE_ENV+=("XDG_RUNTIME_DIR=${XDG_RUNTIME_DIR:-${_uid:+/run/user/$_uid}}")
if [ -n "${DBUS_SESSION_BUS_ADDRESS:-}" ]; then
  GATE_ENV+=("DBUS_SESSION_BUS_ADDRESS=$DBUS_SESSION_BUS_ADDRESS")
fi
# The canonical clone's `origin` may be an ssh remote, and this launcher does a network
# `git fetch` against it — forward the caller's ssh-agent socket when present so that
# fetch (and the gate's own fetches) can authenticate.
if [ -n "${SSH_AUTH_SOCK:-}" ]; then
  GATE_ENV+=("SSH_AUTH_SOCK=$SSH_AUTH_SOCK")
fi
# gate-notify.sh's completion push (#2667/#3119) reads one of these webhook variables and
# returns 1 with only a debug line when neither is set — so dropping them here would
# SILENTLY disable notifications for every gate started through this launcher, with no
# diagnostic pointing at the cause (roborev finding, #4267 round 2). Disclosed either way,
# matching the sccache line below.
_NOTIFY_WIRED=""
if [ -n "${CQLITE_NOTIFY_WEBHOOK:-}" ]; then
  GATE_ENV+=("CQLITE_NOTIFY_WEBHOOK=$CQLITE_NOTIFY_WEBHOOK")
  _NOTIFY_WIRED="${_NOTIFY_WIRED:+$_NOTIFY_WIRED, }CQLITE_NOTIFY_WEBHOOK"
fi
if [ -n "${CODEX_NOTIFY_WEBHOOK:-}" ]; then
  GATE_ENV+=("CODEX_NOTIFY_WEBHOOK=$CODEX_NOTIFY_WEBHOOK")
  _NOTIFY_WIRED="${_NOTIFY_WIRED:+$_NOTIFY_WIRED, }CODEX_NOTIFY_WEBHOOK"
fi
if [ -n "$_NOTIFY_WIRED" ]; then
  echo "gate-box-launch: notifications wired: $_NOTIFY_WIRED"
else
  echo "gate-box-launch: notifications not wired: neither CQLITE_NOTIFY_WEBHOOK nor CODEX_NOTIFY_WEBHOOK is set"
fi
_SCCACHE_NOTE="not wired: sccache not found on the profile PATH"
if _sccache_bin=$(PATH="$GATE_PATH" command -v sccache 2>/dev/null) && [ -n "$_sccache_bin" ]; then
  GATE_ENV+=("RUSTC_WRAPPER=$_sccache_bin")
  if [ -n "${BOX_SCCACHE_DIR:-}" ]; then
    GATE_ENV+=("SCCACHE_DIR=$BOX_SCCACHE_DIR")
  fi
  if [ -n "${BOX_SCCACHE_SIZE:-}" ]; then
    GATE_ENV+=("SCCACHE_CACHE_SIZE=$BOX_SCCACHE_SIZE")
  fi
  _SCCACHE_NOTE="wired: RUSTC_WRAPPER=$_sccache_bin"
fi
echo "gate-box-launch: sccache $_SCCACHE_NOTE"

# ---------------------------------------------------------------------------
# 10. Resolve summary/log paths (one fresh pair per launch unless the caller pinned one).
# ---------------------------------------------------------------------------
_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
SUMMARY_PATH="${OUT_SUMMARY:-$BOX_LOG_DIR/gate-${LANE_NAME}-${_STAMP}.summary.txt}"
LOG_PATH="${OUT_LOG:-$BOX_LOG_DIR/gate-${LANE_NAME}-${_STAMP}.log}"

GATE_CMD=(bash "$LANE_DIR/scripts/flow/gate-detached.sh" --summary "$SUMMARY_PATH" --log "$LOG_PATH")

if [ "$DRY_RUN" -eq 1 ]; then
  echo "gate-box-launch: [dry-run] resolved environment:"
  for _e in "${GATE_ENV[@]}"; do
    printf '  %s\n' "$_e"
  done
  echo "gate-box-launch: [dry-run] resolved command:"
  printf '  '
  printf '%q ' "${GATE_CMD[@]}"
  printf '\n'
  echo "gate-box-launch: [dry-run] no git/process side effects beyond the fetches above."
  echo "GATE-BOX-LAUNCH: dry-run box=$BOX_NAME lane=$LANE_NAME head=$HEAD_SHA summary=$SUMMARY_PATH log=$LOG_PATH"
  exit 0
fi

if [ ! -d "$BOX_LOG_DIR" ]; then
  mkdir -p "$BOX_LOG_DIR" || {
    echo "gate-box-launch: could not create BOX_LOG_DIR '$BOX_LOG_DIR'." >&2
    exit 1
  }
fi

# ---------------------------------------------------------------------------
# 11. Launch, from inside the lane, with the allowlisted environment ONLY.
# ---------------------------------------------------------------------------
_LAUNCH_OUT=$(cd "$LANE_DIR" && env -i "${GATE_ENV[@]}" "${GATE_CMD[@]}" 2>&1)
_LAUNCH_RC=$?
printf '%s\n' "$_LAUNCH_OUT"
if [ "$_LAUNCH_RC" -ne 0 ]; then
  echo "gate-box-launch: gate-detached.sh exited $_LAUNCH_RC; see its output above." >&2
  exit "$_LAUNCH_RC"
fi

_UNIT=$(printf '%s\n' "$_LAUNCH_OUT" | awk -F': *' '/^unit:/ {print $2; exit}')
_RUN_ID=$(grep -m1 '^run-id: ' "$SUMMARY_PATH" 2>/dev/null | sed 's/^run-id: //')
echo "GATE-BOX-LAUNCH: box=$BOX_NAME lane=$LANE_NAME head=$HEAD_SHA unit=${_UNIT:-unknown} run-id=${_RUN_ID:-unknown} summary=$SUMMARY_PATH log=$LOG_PATH"
exit 0
