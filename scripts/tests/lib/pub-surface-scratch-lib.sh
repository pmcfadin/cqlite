#!/usr/bin/env bash
# pub-surface-scratch-lib.sh — scratch-worktree management for the pub-surface
# guard self-test (issue #1712).
#
# WHY THIS IS A LIBRARY AND NOT INLINE IN THE SUITE. The behaviour that matters most
# here is what happens when the suite is KILLED, and a test for that needs a second,
# tiny process that uses the SAME code path and can be signalled without recursing
# into the suite. Sourcing one library from both is how that case exercises the real
# implementation rather than a re-implementation of it.
#
# THE DEFECT THIS EXISTS FOR. `trap cleanup EXIT` does NOT fire on SIGTERM/SIGINT in
# bash. The suite creates one registered git worktree per case, so a killed run left
# one leaked worktree PER CASE in the repository's worktree registry — and
# `git worktree prune` could not reclaim them, because their directories and admin
# files were still intact. Observed for real: a 2-minute tool timeout on this suite
# left 11 registered worktrees behind that had to be removed by hand. That is not a
# corner case: CLAUDE.md records that subagents are killed by a 600s stall watchdog
# under CPU contention, and this suite runs for minutes inside `tooling-tests`.
#
# WHAT A TRAP CAN AND CANNOT DO — MEASURED, because the obvious fix is NOT sufficient
# and it would be easy to ship a vacuous test of it:
#
#   * `kill -TERM <pid>` on the script alone: bash runs the EXIT trap ANYWAY (measured:
#     a script carrying only `trap cleanup EXIT` cleaned up and exited 143). So a
#     signal-based test that signals a single PID CANNOT tell a fixed suite from a
#     broken one — it passes either way, which makes it worse than no test.
#   * A PROCESS-GROUP kill (`kill -TERM -<pgid>` — what a tool timeout and a watchdog
#     send) skips the traps: bash is waiting on a foreground child that the same signal
#     kills, and dies by signal. Measured with and without an explicit TERM trap, with
#     and without `set -e`, and with the `sleep & wait` idiom: no cleanup in any of them.
#   * SIGKILL cannot be trapped at all, by anyone.
#
# So the trap list below is cheap hygiene for the single-PID case, and it is NOT the
# fix. The fix is the STARTUP REAP: the next run cleans up after the killed one, which
# works however the previous run died — the only property worth having here.
#
# The reap is BOUNDED, not a heuristic: the suite holds a single-instance LOCK, so while
# it runs no other instance can exist, and therefore every `pub-surface-selftest.*`
# worktree registration other than this run's own is provably from a dead run.

# ps_reap_stale_scratch <repo-root>: remove every `pub-surface-selftest.*` worktree
# registration that is not this run's. Safe because the caller holds the single-instance
# lock, so no concurrent run's checkouts can be in that set.
ps_reap_stale_scratch() {
  local repo="$1" wt
  while IFS= read -r wt; do
    [ -n "$wt" ] || continue
    case "$wt" in
      *"/pub-surface-selftest."*) ;;
      *) continue ;;
    esac
    if [ -n "${PS_TMPROOT:-}" ]; then
      case "$wt" in "$PS_TMPROOT"/*) continue ;; esac
    fi
    git -C "$repo" worktree remove --force "$wt" >/dev/null 2>&1 || true
    rm -rf "$wt"
  done < <(git -C "$repo" worktree list --porcelain 2>/dev/null | awk '/^worktree /{ print substr($0, 10) }')
  git -C "$repo" worktree prune >/dev/null 2>&1 || true
  return 0
}

# ps_scratch_init <repo-root>: take the single-instance lock, reap whatever a killed
# predecessor left registered, create this run's scratch root, install the traps.
ps_scratch_init() {
  PS_REPO_ROOT="$1"
  PS_WORKTREES=()
  PS_CLEANED=0
  PS_LOCK_DIR=""
  PS_TMPROOT=""
  PS_LOCK="${TMPDIR:-/tmp}/pub-surface-selftest.lock"
  local wait_secs=600

  # Single instance. `flock(1)` is util-linux and ABSENT ON macOS, a gate host here, so
  # the fallback is an atomic mkdir mutex. Both give the property the reap needs: while
  # this runs, no other instance of this suite exists.
  if command -v flock >/dev/null 2>&1; then
    exec 8>"$PS_LOCK" || { echo "FAIL: cannot create the self-test lock $PS_LOCK"; exit 1; }
    flock -w "$wait_secs" 8 \
      || { echo "FAIL: timed out after ${wait_secs}s waiting for the self-test lock $PS_LOCK - another run of this suite is active."; exit 1; }
  else
    local deadline=$(( $(date +%s) + wait_secs ))
    until mkdir "$PS_LOCK.d" 2>/dev/null; do
      [ "$(date +%s)" -lt "$deadline" ] \
        || { echo "FAIL: timed out after ${wait_secs}s waiting for the self-test lock $PS_LOCK.d."
             echo "      Either another run is active, or a killed run left it behind: rmdir '$PS_LOCK.d'"
             exit 1; }
      sleep 1
    done
    PS_LOCK_DIR="$PS_LOCK.d"
  fi

  # With the lock held, anything still registered belongs to a dead run. Reap BEFORE
  # this run's own scratch root exists, so the "not mine" test cannot be got wrong.
  ps_reap_stale_scratch "$PS_REPO_ROOT"

  PS_TMPROOT="$(mktemp -d "${TMPDIR:-/tmp}/pub-surface-selftest.XXXXXX")"
  trap 'ps_cleanup' EXIT
  trap 'ps_cleanup; exit 130' INT
  trap 'ps_cleanup; exit 143' TERM
  trap 'ps_cleanup; exit 129' HUP
}

# ps_cleanup: idempotent (EXIT runs after a signal handler has already cleaned up).
ps_cleanup() {
  [ "${PS_CLEANED:-0}" -eq 1 ] && return 0
  PS_CLEANED=1
  local wt
  for wt in "${PS_WORKTREES[@]:-}"; do
    [ -n "$wt" ] || continue
    # Both, always, in this order: `remove` drops the REGISTRATION (which is what a
    # bare `rm -rf` leaves behind and `prune` then refuses to reclaim), and the
    # `rm -rf` covers a removal that failed for any reason.
    git -C "$PS_REPO_ROOT" worktree remove --force "$wt" >/dev/null 2>&1 || true
    rm -rf "$wt"
  done
  # Unconditional, not only on failure: reclaims any registration whose directory is
  # already gone, which is the state `remove` cannot address.
  git -C "$PS_REPO_ROOT" worktree prune >/dev/null 2>&1 || true
  [ -n "${PS_TMPROOT:-}" ] && rm -rf "$PS_TMPROOT"
  if [ -n "${PS_LOCK_DIR:-}" ]; then
    rmdir "$PS_LOCK_DIR" 2>/dev/null || true
    PS_LOCK_DIR=""
  fi
  return 0
}

# ps_scratch_tree_from <source-checkout> <name>: a detached worktree that reproduces
# <source-checkout>'s WORKING TREE, not its HEAD, and publishes the path in SCRATCH.
#
# WHY THE WORKING TREE AND NOT HEAD. A `git worktree add` materialises a COMMIT, so a
# scratch built from HEAD carries HEAD's sources. Copying only the live guard and the
# live snapshot into it leaves source and baseline describing DIFFERENT trees, and
# that breaks the ordinary pre-commit workflow: change the public API, regenerate the
# snapshot, run the tests before committing, and the real-tree case passes while the
# green scratch cases fail, because HEAD's API cannot match the newly regenerated
# snapshot. A false FAIL that looks exactly like a real defect, sitting in the path
# every future contributor walks.
#
# So the whole uncommitted delta is applied: tracked modifications via a binary
# `git diff HEAD` patch, plus untracked-but-not-ignored files copied in. After that
# the scratch's `git status --porcelain` must MATCH the source's; a mismatch means the
# overlay did not reproduce the tree, and the suite FAILS rather than testing
# something other than what you are working on.
#
# Deliberately NOT usable as a command substitution: `$(…)` would run the body in a
# subshell, discarding the PS_WORKTREES bookkeeping the cleanup depends on.
SCRATCH=""

# ps_overlay <source-checkout> <worktree>: reproduce <source-checkout>'s uncommitted
# delta inside <worktree>, then PROVE the two describe the same tree.
ps_overlay() {
  local src="$1" path="$2"
  local patch="$path.live.patch"

  # 1. tracked modifications (staged + unstaged), including the snapshot.
  git -C "$src" diff HEAD --binary >"$patch" 2>/dev/null || true
  if [ -s "$patch" ]; then
    git -C "$path" apply --whitespace=nowarn "$patch" \
      || { echo "FAIL: could not apply $src's uncommitted changes to the scratch worktree $path."
           echo "      The scratch would then describe a different tree than the one under test."
           exit 1; }
  fi

  # 2. untracked, non-ignored files (a brand-new source file is part of the tree too).
  local rel
  while IFS= read -r rel; do
    [ -n "$rel" ] || continue
    mkdir -p "$path/$(dirname "$rel")"
    cp "$src/$rel" "$path/$rel"
  done < <(git -C "$src" ls-files --others --exclude-standard)

  # 3. Prove it: the scratch must describe the SAME tree as the source.
  local src_status scratch_status
  src_status="$(git -C "$src" status --porcelain --untracked-files=all | LC_ALL=C sort)"
  scratch_status="$(git -C "$path" status --porcelain --untracked-files=all | LC_ALL=C sort)"
  if [ "$src_status" != "$scratch_status" ]; then
    echo "FAIL: the scratch worktree $path does not reproduce $src's working tree."
    echo "      Source-only / scratch-only entries:"
    diff <(printf '%s\n' "$src_status") <(printf '%s\n' "$scratch_status") | head -20
    exit 1
  fi
  SCRATCH="$path"
}

# ps_scratch_tree_from <source-checkout> <name>: a NEW detached worktree that
# reproduces <source-checkout>'s WORKING TREE, not its HEAD.
#
# WHY THE WORKING TREE AND NOT HEAD. A `git worktree add` materialises a COMMIT, so a
# scratch built from HEAD carries HEAD's sources. Copying only the live guard and the
# live snapshot into it leaves source and baseline describing DIFFERENT trees, and
# that breaks the ordinary pre-commit workflow: change the public API, regenerate the
# snapshot, run the tests before committing, and the real-tree case passes while the
# green scratch cases fail, because HEAD's API cannot match the newly regenerated
# snapshot. A false FAIL that looks exactly like a real defect, sitting in the path
# every future contributor walks.
#
# Deliberately NOT usable as a command substitution: `$(…)` would run the body in a
# subshell, discarding the PS_WORKTREES bookkeeping the cleanup depends on.
ps_scratch_tree_from() {
  local src="$1" nm="$2"
  local path="$PS_TMPROOT/$nm"
  git -C "$src" worktree add --detach --quiet "$path" HEAD >/dev/null 2>&1 \
    || { echo "FAIL: could not create scratch worktree $path"; exit 1; }
  PS_WORKTREES+=("$path")
  ps_overlay "$src" "$path"
}

# MEASURED AND REJECTED (issue #1712, round 5): reusing ONE scratch worktree across
# cases, reset between them, to keep cargo's fingerprint stable. It bought NOTHING —
# 4m13 shared vs 4m00 one-per-case on the same box — because the suite's cost is
# almost entirely `cargo doc`, not worktree setup: a bare
# `cargo doc --no-deps -p cqlite-core --lib` measured 5.8s early in that session and
# 22.4s later on the same machine, while the guard's own logic is ~1s. So the suite's
# duration tracks machine state, and the only real lever is FEWER doc builds, not
# fewer worktrees. One worktree per case is kept because it is simpler and gives each
# case total isolation.
