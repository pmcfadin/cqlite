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
# RESIDUAL, stated rather than implied: SIGKILL cannot be trapped by any process, so
# a `kill -9` still leaks. The remedy is the same manual one
# (`git worktree remove --force <path>`); nothing here can prevent it, and a
# startup reaper that guessed which stale worktrees were "definitely dead" would be
# a heuristic that could delete a CONCURRENT run's checkouts.

# ps_scratch_init <repo-root>: create the scratch root and install the cleanup trap.
ps_scratch_init() {
  PS_REPO_ROOT="$1"
  PS_WORKTREES=()
  PS_CLEANED=0
  PS_TMPROOT="$(mktemp -d "${TMPDIR:-/tmp}/pub-surface-selftest.XXXXXX")"
  # EXIT alone is not enough — it does not fire on a signal. INT/TERM/HUP are the
  # signals a tool timeout, a Ctrl-C and a watchdog actually send. The handler
  # cleans up and then re-raises the conventional 128+signo status, so callers
  # still see that the run was killed rather than that it failed.
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
  rm -rf "$PS_TMPROOT"
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

# ps_scratch_reuse <source-checkout>: hand back ONE shared scratch worktree, reset to
# <source-checkout>'s working tree.
#
# WHY REUSE. Every distinct worktree PATH is a distinct cargo fingerprint, so a fresh
# worktree per case made `cargo doc` re-do work and thrash the shared target dir:
# measured per-case cost climbed 13s -> 31s across the suite, ~230s total. Reusing one
# path keeps cargo's view stable, so only the file a case actually changed is
# re-documented.
#
# Correctness is not traded away for that: the tree is hard-reset (`checkout -f` +
# `clean -fd`) and then re-overlaid, and ps_overlay's status-equality assert runs
# every single time — so a case can only ever see the working tree, never the
# previous case's mutations. Cases needing two trees at once (the dirty-tree case)
# still call ps_scratch_tree_from for a genuinely separate one.
PS_SHARED=""
ps_scratch_reuse() {
  local src="$1"
  if [ -z "$PS_SHARED" ]; then
    ps_scratch_tree_from "$src" shared
    PS_SHARED="$SCRATCH"
    return 0
  fi
  git -C "$PS_SHARED" checkout -f --quiet HEAD -- . 2>/dev/null \
    || { echo "FAIL: could not reset the shared scratch worktree $PS_SHARED"; exit 1; }
  git -C "$PS_SHARED" clean -fdq \
    || { echo "FAIL: could not clean the shared scratch worktree $PS_SHARED"; exit 1; }
  ps_overlay "$src" "$PS_SHARED"
}
