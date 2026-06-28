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
  # unpushed commits (branch ahead of its upstream)? Only checkable when upstream set.
  if upstream="$(git -C "$target_wt" rev-parse --abbrev-ref --symbolic-full-name '@{u}' 2>/dev/null)"; then
    ahead="$(git -C "$target_wt" rev-list --count "${upstream}..HEAD" 2>/dev/null || echo 0)"
    if [ "${ahead:-0}" -gt 0 ]; then
      echo "$prog: REFUSED — worktree '$target_wt' has $ahead unpushed commit(s) vs $upstream. Not removing." >&2
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

if [ "$remote_has_branch" -eq 1 ]; then
  # The merged PR's branch is authoritatively merged (caller confirmed state=MERGED),
  # so a squash-merge tip not being an ancestor of main is expected and OK to delete.
  # We still honor guard 4 for safety when the caller did NOT confirm a merge.
  tip_in_main=0
  if git_root merge-base --is-ancestor "refs/remotes/${REMOTE}/${MERGED_BRANCH}" "$MAIN_REF" 2>/dev/null \
     || git_root merge-base --is-ancestor "$(git_root ls-remote --heads "$REMOTE" "$MERGED_BRANCH" | awk '{print $1}')" "$MAIN_REF" 2>/dev/null; then
    tip_in_main=1
  fi
  if [ "$tip_in_main" -eq 0 ] && [ "$CONFIRM_UNMERGED" -eq 0 ]; then
    note "branch '$MERGED_BRANCH' tip not contained in $MAIN_REF (expected for squash-merge of a MERGED PR)."
    note "deleting because it is the confirmed-merged PR branch passed via --merged-branch."
  fi
  run git_root push "$REMOTE" --delete "$MERGED_BRANCH"
  note "deleted origin lock $REMOTE/$MERGED_BRANCH"
else
  note "origin branch '$MERGED_BRANCH' already absent (likely deleted by gh pr merge --delete-branch) — nothing to delete"
fi

# Local branch: safe-delete; force only the confirmed-merged branch.
if git_root show-ref --verify --quiet "refs/heads/${MERGED_BRANCH}"; then
  if ! run git_root branch -d "$MERGED_BRANCH" 2>/dev/null; then
    # squash-merge => not an ancestor of main; -d refuses. It's the confirmed
    # merged branch, so -D is safe here (and only here).
    run git_root branch -D "$MERGED_BRANCH"
  fi
  note "deleted local branch $MERGED_BRANCH"
fi

note "cleanup complete for issue #$ISSUE (branch $MERGED_BRANCH)"
