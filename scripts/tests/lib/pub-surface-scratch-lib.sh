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
# WHAT A TRAP CAN AND CANNOT DO — MEASURED, because the obvious fix is not sufficient
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
# WHAT IS THEREFORE ACCEPTED, DELIBERATELY: a run killed by SIGKILL (or by a
# process-group signal) can leave its scratch root and its worktree registrations
# behind, and the remedy is manual — `git worktree remove --force <path>`. That is
# rare, loud when it happens, and bounded.
#
# AND WHY THERE IS NO STALE-RUN REAPER, so nobody adds one back. A startup sweep over
# every registered worktree whose PATH looks like ours is not a cleanup, it is a
# DELETE-BY-NAME-SHAPE: two concurrent runs have distinct mktemp roots, so each would
# destroy the other's LIVE checkouts — not a race window, a certainty. Worse here than
# elsewhere, because `/data/lanes/lane-*` are worktrees of ONE repository and
# `git worktree list` is shared across all of them, so one lane's gate would delete
# another lane's live scratch and the victim would fail with missing-file noise that
# looks nothing like the cause. Inferring "stale" from a path shape is the same defect
# class as reading a grammar by substring, which this issue has already fixed twice
# (#1712 finding 3 and r2 F3). A pidfile-plus-`kill -0` mechanism would be sounder, but
# it buys only best-effort recovery from the one signal nothing can catch, at the price
# of a new mechanism with its own permissive branches (stale pidfile, PID reuse, a root
# with no pidfile). Removal is BY EXPLICIT PATH only — ps_remove_worktree, below, which
# is the same call the cleanup path uses.

# ps_remove_worktree <repo-root> <path>: drop ONE worktree, by explicit path.
# Both steps, always, in this order: `remove` drops the REGISTRATION (which is what a
# bare `rm -rf` leaves behind and `prune` then refuses to reclaim — the state a killed
# run leaves), the `rm -rf` covers a removal that failed for any reason, and the
# `prune` reclaims a registration whose directory is already gone.
ps_remove_worktree() {
  local repo="$1" wt="$2"
  [ -n "$wt" ] || return 0
  git -C "$repo" worktree remove --force "$wt" >/dev/null 2>&1 || true
  rm -rf "$wt"
  git -C "$repo" worktree prune >/dev/null 2>&1 || true
  return 0
}

# ps_scratch_init <repo-root>: take the single-instance lock, reap whatever a killed
# predecessor left registered, create this run's scratch root, install the traps.
ps_scratch_init() {
  PS_REPO_ROOT="$1"
  PS_WORKTREES=()
  # Worktrees a CASE stands up outside the normal scratch bookkeeping — case 19's
  # decoys, which must exist OUTSIDE $PS_TMPROOT to be realistic. They are reclaimed
  # only at exit/signal, never mid-run, so registering them here cannot interfere with a
  # case that removes them explicitly as part of what it measures (roborev r16).
  PS_EXTRA_WORKTREES=()
  PS_EXTRA_ROOTS=()
  PS_CLEANED=0
  PS_TMPROOT="$(mktemp -d "${TMPDIR:-/tmp}/pub-surface-selftest.XXXXXX")"
  # EXIT alone is not enough — it does not fire on a signal. INT/TERM/HUP are what a
  # Ctrl-C and a single-PID timeout send. The handler cleans up and then re-raises the
  # conventional 128+signo status, so callers still see that the run was killed rather
  # than that it failed.
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
    ps_remove_worktree "$PS_REPO_ROOT" "$wt"
  done
  for wt in "${PS_EXTRA_WORKTREES[@]:-}"; do
    ps_remove_worktree "$PS_REPO_ROOT" "$wt"
  done
  local xr
  for xr in "${PS_EXTRA_ROOTS[@]:-}"; do
    [ -n "$xr" ] && rm -rf "$xr"
  done
  [ -n "${PS_TMPROOT:-}" ] && rm -rf "$PS_TMPROOT"
  return 0
}

SCRATCH=""

# ps_overlay <source-checkout> <worktree>: reproduce <source-checkout>'s uncommitted
# delta inside <worktree>, then PROVE the two describe the same tree.
ps_overlay() {
  local src="$1" path="$2"
  local staged_patch="$path.staged.patch" unstaged_patch="$path.unstaged.patch"

  # 1. tracked modifications, including the snapshot — STAGED and UNSTAGED reproduced
  #    SEPARATELY, in that order (issue #1712 r6 F3).
  #
  #    WHY NOT ONE `git diff HEAD` PATCH. That single patch carries staged content, but
  #    a plain `git apply` recreates it UNSTAGED — and step 3 below compares
  #    `git status --porcelain` EXACTLY, where `M ` (staged) and ` M` (unstaged) are
  #    different states. So the moment anything in the source tree was `git add`ed, the
  #    overlay aborted the whole suite with "does not reproduce" — a FALSE FAIL sitting
  #    on the ordinary stage-then-test workflow.
  #
  #    The fix keeps the proof step EXACT (rather than weakening it to compare only file
  #    content and modes, which would stop noticing an index the scratch got wrong):
  #    `--cached` gives HEAD→index and applies with `--index` (index AND worktree), then
  #    the worktree-vs-index diff applies to the worktree alone. Together they reproduce
  #    `M `, ` M`, `MM`, `A `, `D ` and the rest, not just the file bytes.
  git -C "$src" diff --cached HEAD --binary >"$staged_patch" 2>/dev/null || true
  if [ -s "$staged_patch" ]; then
    git -C "$path" apply --index --whitespace=nowarn "$staged_patch" \
      || { echo "FAIL: could not apply $src's STAGED changes to the scratch worktree $path."
           echo "      The scratch would then describe a different tree than the one under test."
           exit 1; }
  fi
  git -C "$src" diff --binary >"$unstaged_patch" 2>/dev/null || true
  if [ -s "$unstaged_patch" ]; then
    git -C "$path" apply --whitespace=nowarn "$unstaged_patch" \
      || { echo "FAIL: could not apply $src's UNSTAGED changes to the scratch worktree $path."
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

# ps_register_extra <worktree-path> [containing-root]: record a worktree a case created
# outside $PS_TMPROOT so a signal or an assertion failure before its explicit removal
# cannot leave the directory AND the git registration behind (roborev r16). A leaked
# registration is worse than a leaked directory here: this repository's worktree registry
# is shared across every /data/lanes/lane-* checkout, so it is visible to other lanes.
ps_register_extra() {
  PS_EXTRA_WORKTREES+=("$1")
  [ -n "${2:-}" ] && PS_EXTRA_ROOTS+=("$2")
  return 0
}
