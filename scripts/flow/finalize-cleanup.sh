#!/usr/bin/env bash
#
# finalize-cleanup.sh — guarded destructive cleanup for `flow-finalize` (issue #1162).
#
# WHY THIS EXISTS
# ---------------
# The old flow-finalize cleanup globbed `issue-<N>-*` and unconditionally
# `--force`-removed worktrees + `--delete`d origin branches. On 2026-06-27 that
# destroyed an UNRELATED active claim: PR #1156 merged from
# `issue-1143-read-p99-regression`, but the glob also matched a separate active
# effort `issue-1143-scan-window-offload` (unmerged, dirty worktree) and deleted
# it. This script makes finalize cleanup deterministic and safe:
#
#   1. It only ever targets the MERGED PR's branch (headRefName), never a glob.
#   2. It refuses if >1 `issue-<N>-*` lock exists on origin (1:1:1:1 violation).
#   3. It refuses to remove a worktree with uncommitted changes or unpushed
#      commits (no blind --force).
#   4. It never deletes an origin branch whose tip is not contained in `main`
#      and is not the confirmed-merged branch, unless --confirm-unmerged.
#
# The merged-branch name is the authoritative "this one was merged" signal — the
# caller (flow-finalize SKILL) resolves it from `gh pr view <pr> --json
# headRefName` AFTER confirming the PR state is MERGED, then passes it here. This
# keeps the script pure-git and unit-testable against a local bare remote.
#
# USAGE
#   scripts/flow/finalize-cleanup.sh \
#     --issue <N> \
#     --merged-branch <headRefName> \
#     [--repo-root <path>]          (default: git toplevel)
#     [--worktrees-dir <path>]      (default: <repo-root>/.claude/worktrees)
#     [--remote <name>]             (default: origin)
#     [--main-ref <ref>]            (default: <remote>/main)
#     [--confirm-unmerged]          (allow deleting a branch whose tip is not in main)
#     [--dry-run]                   (print actions, change nothing)
#
# --dry-run previews the destructive git actions but STILL honors the refusal
# guards: a dirty/unpushed worktree or an unmerged tip aborts with exit 3/4 even
# in dry-run, since those are the conditions worth surfacing before any real run.
#
# EXIT CODES
#   0  cleanup completed (or nothing to do)
#   2  refused: >1 lock for the issue (1:1:1:1 violation)
#   3  refused: target worktree is dirty / has unpushed commits
#   4  refused: target branch tip not contained in main (use --confirm-unmerged)
#   64 usage error
#
set -euo pipefail

prog="$(basename "$0")"

die_usage() { echo "$prog: $*" >&2; exit 64; }
note()      { echo "[finalize-cleanup] $*" >&2; }

ISSUE=""
MERGED_BRANCH=""
REPO_ROOT=""
WORKTREES_DIR=""
REMOTE="origin"
MAIN_REF=""
CONFIRM_UNMERGED=0
DRY_RUN=0

while [ $# -gt 0 ]; do
  case "$1" in
    --issue)            ISSUE="${2:-}"; shift 2 ;;
    --merged-branch)    MERGED_BRANCH="${2:-}"; shift 2 ;;
    --repo-root)        REPO_ROOT="${2:-}"; shift 2 ;;
    --worktrees-dir)    WORKTREES_DIR="${2:-}"; shift 2 ;;
    --remote)           REMOTE="${2:-}"; shift 2 ;;
    --main-ref)         MAIN_REF="${2:-}"; shift 2 ;;
    --confirm-unmerged) CONFIRM_UNMERGED=1; shift ;;
    --dry-run)          DRY_RUN=1; shift ;;
    -h|--help)          sed -n '2,40p' "$0"; exit 0 ;;
    *)                  die_usage "unknown argument: $1" ;;
  esac
done

[ -n "$ISSUE" ] || die_usage "--issue is required"
[ -n "$MERGED_BRANCH" ] || die_usage "--merged-branch is required (the merged PR's headRefName)"

if [ -z "$REPO_ROOT" ]; then
  REPO_ROOT="$(git rev-parse --show-toplevel)"
fi
[ -d "$REPO_ROOT/.git" ] || [ -f "$REPO_ROOT/.git" ] || die_usage "not a git repo: $REPO_ROOT"
[ -n "$WORKTREES_DIR" ] || WORKTREES_DIR="$REPO_ROOT/.claude/worktrees"
[ -n "$MAIN_REF" ] || MAIN_REF="$REMOTE/main"

git_root() { git -C "$REPO_ROOT" "$@"; }

run() {
  if [ "$DRY_RUN" -eq 1 ]; then
    echo "DRY-RUN: $*" >&2
  else
    "$@"
  fi
}

# ---------------------------------------------------------------------------
# Guard 2: exactly one issue-<N>-* lock on origin (1:1:1:1).
# ---------------------------------------------------------------------------
# bash 3.2 compatible (no `mapfile`)
LOCKS=()
while IFS= read -r _lock; do
  [ -n "$_lock" ] && LOCKS+=("$_lock")
done < <(git_root ls-remote --heads "$REMOTE" "issue-${ISSUE}-*" \
           | awk '{print $2}' | sed 's,^refs/heads/,,')
note "origin locks for issue #$ISSUE: ${LOCKS[*]:-<none>}"

if [ "${#LOCKS[@]}" -gt 1 ]; then
  echo "$prog: REFUSED — ${#LOCKS[@]} 'issue-${ISSUE}-*' locks exist on $REMOTE:" >&2
  for l in "${LOCKS[@]}"; do echo "  - $l" >&2; done
  echo "$prog: this is a 1:1:1:1 violation. Resolve manually; not deleting anything." >&2
  exit 2
fi

# ---------------------------------------------------------------------------
# Guard 3: never --force-remove a dirty / unpushed worktree.
# Only the worktree whose checked-out branch == MERGED_BRANCH is a target.
# ---------------------------------------------------------------------------
target_wt=""
# `git worktree list --porcelain` groups: worktree <path> / HEAD <sha> / branch refs/heads/<name>
while IFS= read -r line; do
  case "$line" in
    worktree\ *) cur_path="${line#worktree }" ;;
    branch\ *)
      cur_branch="${line#branch refs/heads/}"
      if [ "$cur_branch" = "$MERGED_BRANCH" ]; then
        target_wt="$cur_path"
      fi
      ;;
  esac
done < <(git_root worktree list --porcelain)

if [ -n "$target_wt" ]; then
  note "merged-branch worktree: $target_wt"
  # uncommitted changes?
  if [ -n "$(git -C "$target_wt" status --porcelain 2>/dev/null)" ]; then
    echo "$prog: REFUSED — worktree '$target_wt' has uncommitted changes. Not removing." >&2
    exit 3
  fi
  # Unpushed commits? Compare HEAD against the best available "pushed" ref:
  #   1. the configured upstream (@{u}), else
  #   2. the live origin tip for this branch (in case @{u} was never set).
  # If neither exists we cannot prove the work was pushed — refuse when HEAD is
  # ahead of main unless --confirm-unmerged authorizes it (the merged-branch
  # authority). This closes the no-upstream blind spot.
  cmp_ref=""
  if cmp_ref="$(git -C "$target_wt" rev-parse --abbrev-ref --symbolic-full-name '@{u}' 2>/dev/null)"; then
    :
  else
    remote_sha="$(git_root ls-remote --heads "$REMOTE" "$MERGED_BRANCH" | awk '{print $1}')"
    cmp_ref="$remote_sha"
  fi
  if [ -n "$cmp_ref" ]; then
    ahead="$(git -C "$target_wt" rev-list --count "${cmp_ref}..HEAD" 2>/dev/null || echo 0)"
    if [ "${ahead:-0}" -gt 0 ]; then
      echo "$prog: REFUSED — worktree '$target_wt' has $ahead unpushed commit(s) vs $cmp_ref. Not removing." >&2
      exit 3
    fi
  else
    ahead_main="$(git -C "$target_wt" rev-list --count "${MAIN_REF}..HEAD" 2>/dev/null || echo 0)"
    if [ "${ahead_main:-0}" -gt 0 ] && [ "$CONFIRM_UNMERGED" -eq 0 ]; then
      echo "$prog: REFUSED — worktree '$target_wt' has no upstream and no origin branch, and HEAD is" >&2
      echo "  $ahead_main commit(s) ahead of $MAIN_REF — cannot confirm the work was pushed." >&2
      echo "  Pass --confirm-unmerged only if you have verified the PR is MERGED. Not removing." >&2
      exit 3
    fi
  fi
  run git_root worktree remove "$target_wt"
  note "removed worktree $target_wt"
else
  note "no worktree checked out on '$MERGED_BRANCH' — skipping worktree removal"
fi

# ---------------------------------------------------------------------------
# Guard 1 + 4: delete only the MERGED_BRANCH on origin; never a glob, and never
# an unmerged tip without explicit confirmation.
# ---------------------------------------------------------------------------
remote_has_branch=0
if git_root ls-remote --heads "$REMOTE" "$MERGED_BRANCH" | grep -q .; then
  remote_has_branch=1
fi

# Is the merged-branch tip already contained in main? (true for ff / merge-commit
# merges; false for squash merges, where --confirm-unmerged is the authority.)
tip_in_main=0
remote_sha="$(git_root ls-remote --heads "$REMOTE" "$MERGED_BRANCH" | awk '{print $1}')"
if [ -n "$remote_sha" ] && git_root merge-base --is-ancestor "$remote_sha" "$MAIN_REF" 2>/dev/null; then
  tip_in_main=1
fi

if [ "$remote_has_branch" -eq 1 ]; then
  # Guard 4: never delete an origin branch whose tip is not in main unless the
  # caller explicitly confirms it was merged (e.g. squash-merge: tip never
  # becomes an ancestor of main). flow-finalize passes --confirm-unmerged after
  # verifying the PR state is MERGED in step 1.
  if [ "$tip_in_main" -eq 0 ] && [ "$CONFIRM_UNMERGED" -eq 0 ]; then
    echo "$prog: REFUSED — origin branch '$MERGED_BRANCH' tip is not contained in $MAIN_REF" >&2
    echo "  and --confirm-unmerged was not given. If this branch's PR is MERGED (e.g. squash)," >&2
    echo "  re-run with --confirm-unmerged. Not deleting." >&2
    exit 4
  fi
  run git_root push "$REMOTE" --delete "$MERGED_BRANCH"
  note "deleted origin lock $REMOTE/$MERGED_BRANCH"
else
  note "origin branch '$MERGED_BRANCH' already absent (likely deleted by gh pr merge --delete-branch) — nothing to delete"
fi

# Local branch: safe-delete (-d refuses unmerged). Force (-D) only when the tip
# is in main or the caller confirmed the merge; otherwise leave the local ref.
if git_root show-ref --verify --quiet "refs/heads/${MERGED_BRANCH}"; then
  if run git_root branch -d "$MERGED_BRANCH" 2>/dev/null; then
    note "deleted local branch $MERGED_BRANCH"
  elif [ "$tip_in_main" -eq 1 ] || [ "$CONFIRM_UNMERGED" -eq 1 ]; then
    run git_root branch -D "$MERGED_BRANCH"
    note "force-deleted local branch $MERGED_BRANCH (confirmed merged)"
  else
    note "local branch '$MERGED_BRANCH' left in place (unmerged tip; pass --confirm-unmerged to force-delete)"
  fi
fi

note "cleanup complete for issue #$ISSUE (branch $MERGED_BRANCH)"
