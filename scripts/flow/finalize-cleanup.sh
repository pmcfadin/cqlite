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
# guards: a dirty/unpushed worktree, an unmerged tip, or a remote-query failure
# aborts with exit 3/4/5 even in dry-run — those are the conditions worth
# surfacing before any real run.
#
# EXIT CODES
#   0  cleanup completed (or nothing to do)
#   2  refused: >1 lock for the issue (1:1:1:1 violation)
#   3  refused: target worktree is dirty / has unpushed commits
#   4  refused: target branch tip not contained in main (use --confirm-unmerged)
#   5  refused: remote query (ls-remote) failed — fail closed, changed nothing
#   6  refused: the lane lock's incarnation is not the one being finalized (#3436). TWO
#      causes, both meaning a live peer may be in that lane, and in both nothing is released
#      and the worktree is NOT removed:
#        - Guard 5: the on-disk lease differs from --lane-lease (detected before any mutation)
#        - the release itself failed: the incarnation changed between Guard 5 and the release
#   64 usage error
#
# ---END-HELP---
set -euo pipefail

prog="$(basename "$0")"

die_usage() { echo "$prog: $*" >&2; exit 64; }
note()      { echo "[finalize-cleanup] $*" >&2; }
# Assert a value-taking flag actually has a value (else `shift 2` would exit 1
# under set -e instead of a clean usage error). Call as: need2 "$@".
need2()     { [ "$#" -ge 2 ] || die_usage "$1 requires a value"; }

ISSUE=""
LANE_LEASE=""
MERGED_BRANCH=""
REPO_ROOT=""
WORKTREES_DIR=""
REMOTE="origin"
MAIN_REF=""
CONFIRM_UNMERGED=0
DRY_RUN=0

while [ $# -gt 0 ]; do
  case "$1" in
    --issue)            need2 "$@"; ISSUE="$2"; shift 2 ;;
    --lane-lease)       need2 "$@"; LANE_LEASE="$2"; shift 2 ;;
    --merged-branch)    need2 "$@"; MERGED_BRANCH="$2"; shift 2 ;;
    --repo-root)        need2 "$@"; REPO_ROOT="$2"; shift 2 ;;
    --worktrees-dir)    need2 "$@"; WORKTREES_DIR="$2"; shift 2 ;;
    --remote)           need2 "$@"; REMOTE="$2"; shift 2 ;;
    --main-ref)         need2 "$@"; MAIN_REF="$2"; shift 2 ;;
    --confirm-unmerged) CONFIRM_UNMERGED=1; shift ;;
    --dry-run)          DRY_RUN=1; shift ;;
    -h|--help)          awk 'NR>=2 && /^# ---END-HELP---/{exit} NR>=2' "$0"; exit 0 ;;
    *)                  die_usage "unknown argument: $1" ;;
  esac
done

[ -n "$ISSUE" ] || die_usage "--issue is required"
case "$ISSUE" in
  *[!0-9]*|'') die_usage "--issue must be a positive integer (got '$ISSUE')" ;;
esac
[ -n "$MERGED_BRANCH" ] || die_usage "--merged-branch is required (the merged PR's headRefName)"
# Identity guard: the merged branch MUST belong to this issue (1:1:1:1). Catches a
# typo or a cross-issue branch name before any lock is touched.
case "$MERGED_BRANCH" in
  issue-${ISSUE}-*) : ;;
  *) die_usage "--merged-branch '$MERGED_BRANCH' does not match --issue $ISSUE (expected issue-${ISSUE}-*)" ;;
esac

if [ -z "$REPO_ROOT" ]; then
  REPO_ROOT="$(git rev-parse --show-toplevel)"
fi
[ -d "$REPO_ROOT/.git" ] || [ -f "$REPO_ROOT/.git" ] || die_usage "not a git repo: $REPO_ROOT"
[ -n "$WORKTREES_DIR" ] || WORKTREES_DIR="$REPO_ROOT/.claude/worktrees"
[ -n "$MAIN_REF" ] || MAIN_REF="$REMOTE/main"

git_root() { git -C "$REPO_ROOT" "$@"; }

run() {
  if [ "$DRY_RUN" -eq 1 ]; then
    # Render the internal git_root wrapper as the real command for an honest preview.
    if [ "${1:-}" = "git_root" ]; then
      shift
      echo "DRY-RUN: git -C $REPO_ROOT $*" >&2
    else
      echo "DRY-RUN: $*" >&2
    fi
  else
    "$@"
  fi
}

# ---------------------------------------------------------------------------
# Guard 2: exactly one issue-<N>-* lock on origin (1:1:1:1).
# FAIL CLOSED: a remote query error (auth/network/rate-limit) must NOT look like
# "0 locks" — that would bypass the very guard the #1143 incident created. Capture
# ls-remote in a checked statement and refuse (exit 5) on failure.
# ---------------------------------------------------------------------------
if ! locks_raw="$(git_root ls-remote --heads "$REMOTE" "issue-${ISSUE}-*" 2>/dev/null)"; then
  echo "$prog: REFUSED — cannot query $REMOTE for 'issue-${ISSUE}-*' locks (remote error). Failing closed." >&2
  exit 5
fi
# bash 3.2 compatible (no `mapfile`)
LOCKS=()
while IFS= read -r _lock; do
  [ -n "$_lock" ] && LOCKS+=("$_lock")
done < <(printf '%s\n' "$locks_raw" | awk 'NF{print $2}' | sed 's,^refs/heads/,,')
note "origin locks for issue #$ISSUE: ${LOCKS[*]:-<none>}"

if [ "${#LOCKS[@]}" -gt 1 ]; then
  echo "$prog: REFUSED — ${#LOCKS[@]} 'issue-${ISSUE}-*' locks exist on $REMOTE:" >&2
  for l in "${LOCKS[@]}"; do echo "  - $l" >&2; done
  echo "$prog: this is a 1:1:1:1 violation. Resolve manually; not deleting anything." >&2
  exit 2
fi

# Canonicalize a path (portable; falls back to the literal if it can't cd).
canon() { ( cd "$1" 2>/dev/null && pwd -P ) || echo "$1"; }

# ===========================================================================
# PHASE 1 — VALIDATE. Gather state and run EVERY refusal guard (2/3/4) BEFORE
# any destructive action, so a refused run never half-deletes (the #1143 lesson:
# no mutation on a guarded path).
# ===========================================================================

# Locate the worktree (if any) checked out on MERGED_BRANCH — the ONLY removal
# target. `git worktree list --porcelain` groups: worktree <path> / HEAD / branch.
target_wt=""
while IFS= read -r line; do
  case "$line" in
    worktree\ *) cur_path="${line#worktree }" ;;
    branch\ *)
      cur_branch="${line#branch refs/heads/}"
      [ "$cur_branch" = "$MERGED_BRANCH" ] && target_wt="$cur_path"
      ;;
  esac
done < <(git_root worktree list --porcelain)

# Stale entry: `git worktree list` records the path but the directory is gone
# (removed out-of-band). Don't fall through to `git worktree remove` (which would
# abort under set -e with git's raw 128). Mark it for a prune in PHASE 2 instead.
stale_wt=0
if [ -n "$target_wt" ] && [ ! -d "$target_wt" ]; then
  note "recorded worktree '$target_wt' is missing on disk — will prune the stale entry, not remove"
  stale_wt=1
  target_wt=""
fi

# Remote state for the merged branch — also FAIL CLOSED on a remote query error,
# else a blip would read as "branch already deleted, nothing to do".
if ! merged_raw="$(git_root ls-remote --heads "$REMOTE" "$MERGED_BRANCH" 2>/dev/null)"; then
  echo "$prog: REFUSED — cannot query $REMOTE for '$MERGED_BRANCH' (remote error). Failing closed." >&2
  exit 5
fi
remote_sha="$(printf '%s\n' "$merged_raw" | awk 'NF{print $1}')"
remote_has_branch=0
[ -n "$remote_sha" ] && remote_has_branch=1
# tip_in_main: true for ff/merge-commit merges; false for squash (where
# --confirm-unmerged is the authority that the PR was MERGED).
tip_in_main=0
if [ -n "$remote_sha" ] && git_root merge-base --is-ancestor "$remote_sha" "$MAIN_REF" 2>/dev/null; then
  tip_in_main=1
fi
# Local-branch mergeability (independent of remote; governs -d vs -D vs skip).
local_tip_in_main=0
if git_root show-ref --verify --quiet "refs/heads/${MERGED_BRANCH}" \
   && git_root merge-base --is-ancestor "refs/heads/${MERGED_BRANCH}" "$MAIN_REF" 2>/dev/null; then
  local_tip_in_main=1
fi

# Guard 3 — worktree must be under --worktrees-dir, clean, and pushed.
if [ -n "$target_wt" ]; then
  note "merged-branch worktree: $target_wt"
  # 3a: containment — never remove a worktree outside the expected dir.
  wt_canon="$(canon "$target_wt")"
  dir_canon="$(canon "$WORKTREES_DIR")"
  case "$wt_canon/" in
    "$dir_canon"/*) : ;;  # under the expected worktrees dir — OK
    *)
      echo "$prog: REFUSED — worktree '$target_wt' is not under --worktrees-dir '$WORKTREES_DIR'. Not removing." >&2
      exit 3
      ;;
  esac
  # 3b: uncommitted changes?
  if [ -n "$(git -C "$target_wt" status --porcelain 2>/dev/null)" ]; then
    echo "$prog: REFUSED — worktree '$target_wt' has uncommitted changes. Not removing." >&2
    exit 3
  fi
  # 3c: unpushed commits? Compare HEAD against a "pushed" ref RESOLVED TO A SHA.
  # A configured-but-deleted upstream (the common `gh pr merge --delete-branch`
  # case) resolves by NAME but not as a ref, so we use `rev-parse --verify`, which
  # only succeeds when @{u} names a live commit; otherwise fall back to the live
  # origin tip. If neither resolves we cannot prove the work was pushed: refuse
  # when HEAD is ahead of main unless --confirm-unmerged authorizes it.
  cmp_sha=""
  if cmp_sha="$(git -C "$target_wt" rev-parse --verify --quiet '@{u}' 2>/dev/null)"; then
    :
  fi
  # Fall back to the live origin tip ONLY if that object is present locally — an
  # ls-remote SHA need not have been fetched, and a missing object would make the
  # comparison fail. If it's absent we drop to the conservative indeterminate path.
  if [ -z "$cmp_sha" ] && [ -n "$remote_sha" ] \
     && git -C "$target_wt" cat-file -e "${remote_sha}^{commit}" 2>/dev/null; then
    cmp_sha="$remote_sha"
  fi
  if [ -n "$cmp_sha" ]; then
    ahead="$(git -C "$target_wt" rev-list --count "${cmp_sha}..HEAD" 2>/dev/null || echo "")"
    if [ -z "$ahead" ]; then
      echo "$prog: REFUSED — cannot compare worktree '$target_wt' against pushed ref $cmp_sha. Not removing." >&2
      exit 3
    fi
    if [ "$ahead" -gt 0 ]; then
      # NOTE: --confirm-unmerged does NOT override this. That flag confirms the PR
      # was MERGED (a branch-tip-vs-main question); it says nothing about local
      # commits that were never pushed. Unpushed work is unrecoverable once the
      # worktree is gone, so this always fails closed.
      echo "$prog: REFUSED — worktree '$target_wt' has $ahead unpushed commit(s) vs $cmp_sha. Not removing." >&2
      exit 3
    fi
  elif [ "$CONFIRM_UNMERGED" -eq 0 ]; then
    # No resolvable pushed ref. Fall back to "is HEAD ahead of main?" — but FAIL
    # CLOSED if even that comparison can't be made (MAIN_REF unresolvable: not
    # fetched, renamed default, --main-ref typo). `|| echo 0` here would be a
    # fail-open data-loss hole, so capture the rc explicitly.
    if ! ahead_main="$(git -C "$target_wt" rev-list --count "${MAIN_REF}..HEAD" 2>/dev/null)"; then
      echo "$prog: REFUSED — cannot compare worktree '$target_wt' against $MAIN_REF (unresolvable)." >&2
      echo "  Cannot confirm the work was pushed. Not removing." >&2
      exit 3
    fi
    if [ "${ahead_main:-0}" -gt 0 ]; then
      echo "$prog: REFUSED — worktree '$target_wt' has no locally-resolvable pushed ref (no upstream;" >&2
      echo "  origin tip absent or unfetched) and HEAD is $ahead_main commit(s) ahead of $MAIN_REF —" >&2
      echo "  cannot confirm the work was pushed. Pass --confirm-unmerged only if the PR is MERGED. Not removing." >&2
      exit 3
    fi
  fi
fi

# Guard 4 — never delete an origin branch whose tip is not in main unless the
# caller confirms the merge (squash). Validated here, BEFORE the worktree is
# touched, so an exit-4 refusal leaves everything intact.
if [ "$remote_has_branch" -eq 1 ] && [ "$tip_in_main" -eq 0 ] && [ "$CONFIRM_UNMERGED" -eq 0 ]; then
  echo "$prog: REFUSED — origin branch '$MERGED_BRANCH' tip is not contained in $MAIN_REF" >&2
  echo "  and --confirm-unmerged was not given. If this branch's PR is MERGED (e.g. squash)," >&2
  echo "  re-run with --confirm-unmerged. Not deleting." >&2
  exit 4
fi

# Guard 5 — THE LANE LOCK IS RELEASED BY TEARDOWN, AND NEVER BLINDLY (#3436).
#
# Before this, finalize removed the worktree and never touched the lane lock at all. The lock
# root is a SIBLING of the lane directories (outside every worktree, deliberately — see
# lane-lock.sh's header), so removing the lane ORPHANED its record: nothing on the box would
# ever delete it, and the next lane to reuse that issue number would meet a holder that cannot
# be reaped by liveness because its recorded pid belongs to a session that is long gone.
#
# WHY --force IS REQUIRED HERE, AND WHY THAT IS NOT A WEAKENING. finalize runs OUTSIDE the lane
# (it is deleting that worktree), so it is not the holder and a plain `release` correctly refuses
# with `reason=not-holder`. MEASURED, all three arms, on a scratch lock root:
#   release --expect <right>            -> RELEASE-REFUSED reason=not-holder   (record kept)
#   release --force --expect <WRONG>    -> RELEASE-LOST reason=lease-mismatch  (record KEPT)
#   release --force --expect <right>    -> RELEASED mode=forced                (record deleted)
# So `--force` bypasses the HOLDER gate while `--expect` still bites: the incarnation check is
# independent of who is asking. `--force` alone would be an unconditional delete, which is what
# this guard exists to avoid.
#
# THE LEASE IS THE CALLER'S ASSERTION. With --lane-lease the caller names the incarnation it is
# finalizing, and a mismatch REFUSES (exit 6) without touching the worktree either — a different
# live incarnation in that lane means a peer session is working there, and removing its worktree
# would be a far worse version of the #3436 damage than the orphan this guard was written for.
# Without --lane-lease the lease is READ at teardown, which is weaker: it cannot distinguish
# "the incarnation I finalized" from "an incarnation that appeared since", so it is DECLARED on
# the note rather than presented as a checked release.
LANE_LOCK_SH="$(dirname -- "$0")/lane-lock.sh"
LANE_LEASE_TO_RELEASE=""
LANE_LEASE_BASIS=""
# `-r`, not `-x || -r`: this invokes `bash "$LANE_LOCK_SH"`, which READS the file — the execute
# bit is irrelevant and an OR admits an executable-but-unreadable script that bash then cannot
# run, turning a declared skip into a confusing failure.
if [ -r "$LANE_LOCK_SH" ]; then
  # THREE-VALUED, because `[ -n "$lane_cur" ]` is not (roborev job 442, High). The probe was
  # run under `|| true` and its output parsed for `lease=`, so EVERY unmeasurable outcome — a
  # nonzero exit, a kill, an empty capture, an output shape this script does not recognise —
  # produced an empty `lane_cur` and fell into the "no record to release" note, after which
  # PHASE 2 removed the worktree. That is a permissive branch taken on an ABSENCE OF
  # INFORMATION immediately before a destructive, irreversible action: the lane may be held by
  # a live peer and we would have deleted its checkout while its lock survived. A positive
  # verdict requires an AFFIRMATIVE MEASUREMENT, so only `FREE ... record=absent` — the
  # probe's own affirmative statement that there is nothing there — permits proceeding.
  # `&& rc=0 || rc=$?`, not `; rc=$?`: this script runs under `set -e`, where a FAILING command
  # substitution in a plain assignment ABORTS the script before the next statement — so the
  # capture never ran and an unmeasurable probe exited 1 rather than reaching this guard at all.
  # The guard existed and was unreachable on exactly the input it was written for.
  lane_probe="$(bash "$LANE_LOCK_SH" probe "$ISSUE" 2>/dev/null)" && lane_probe_rc=0 || lane_probe_rc=$?
  lane_cur="$(printf '%s' "$lane_probe" | tr ' ' '\n' | sed -n 's/^lease=//p' | head -1)"
  lane_free=0
  case "$lane_probe" in
    *"LANE-LOCK: FREE "*) case "$lane_probe" in *"record=absent"*) lane_free=1 ;; esac ;;
  esac
  if [ "$lane_probe_rc" -ne 0 ] || { [ -z "$lane_cur" ] && [ "$lane_free" -eq 0 ]; }; then
    echo "$prog: REFUSED — the lane lock for issue #$ISSUE could not be MEASURED" >&2
    echo "  probe exit: $lane_probe_rc" >&2
    echo "  probe said: ${lane_probe:-<no output>}" >&2
    echo "  Neither a lease nor an affirmative 'FREE ... record=absent' was read, so this run" >&2
    echo "  cannot tell an empty lane from a lane held by a live peer. Proceeding would remove" >&2
    echo "  a worktree on unproven information. Nothing was released and nothing was removed." >&2
    echo "  Remedy: run '\''bash $LANE_LOCK_SH probe $ISSUE'\'' by hand and act on its verdict." >&2
    exit 6
  fi
  if [ -n "$lane_cur" ]; then
    if [ -n "$LANE_LEASE" ]; then
      if [ "$LANE_LEASE" != "$lane_cur" ]; then
        echo "$prog: REFUSED — lane lock for issue #$ISSUE holds a DIFFERENT incarnation" >&2
        echo "  expected (--lane-lease): $LANE_LEASE" >&2
        echo "  actual   (on disk):      $lane_cur" >&2
        echo "  A live peer session may be working in that lane. Releasing its lock or removing" >&2
        echo "  its worktree is #3436's own damage. Nothing was released and nothing was removed." >&2
        exit 6
      fi
      LANE_LEASE_TO_RELEASE="$lane_cur"; LANE_LEASE_BASIS="asserted by --lane-lease"
    else
      # REFUSE — do not release an incarnation nobody asserted (roborev job 450, High; #4055).
      #
      # This branch used to ADOPT whatever lease was on disk and force-release it, declaring the
      # weakness on the note. Declaring it does not make it safe: a peer session that acquired
      # this lane AFTER finalization began owns that lease, and the next block removes its
      # WORKTREE. That is #3436's own damage arriving through the teardown written to prevent
      # it, and a note in a log does not stop it — the same "a check placed before the harmful
      # effect must PREVENT it, not report it" rule this file already applies at Guard 5.
      #
      # SCOPED DELIBERATELY TO "A LEASE EXISTS": an absent record still proceeds (the
      # affirmative FREE path above), so this cannot red a lane that never held a lock — which
      # is most of them today, because #4024 means nothing acquires from production code. It
      # fires only where a live incarnation would be destroyed unasserted, i.e. exactly the
      # dangerous case, and it is why this is not a blanket `--lane-lease` requirement: a
      # mandatory flag would refuse every no-lock lane and be the guard agents learn to waive.
      echo "$prog: REFUSED — lane #$ISSUE HOLDS a lock and no --lane-lease was asserted" >&2
      echo "  on disk (lease): $lane_cur" >&2
      echo "  Releasing an incarnation this run never named would destroy a peer's lock and" >&2
      echo "  then its worktree if that peer acquired the lane after finalization began." >&2
      echo "  Nothing was released and nothing was removed." >&2
      echo "  Remedy: pass --lane-lease with the lease you acquired, e.g." >&2
      echo "    lease=\$(bash $LANE_LOCK_SH probe $ISSUE | tr ' ' '\\n' | sed -n 's/^lease=//p' | head -1)" >&2
      echo "    $prog --issue $ISSUE --merged-branch <branch> --lane-lease \"\$lease\"" >&2
      exit 6
    fi
  else
    note "lane lock for issue #$ISSUE: no record to release"
  fi
else
  note "lane lock: $LANE_LOCK_SH not readable — no release attempted (declared, not silent)"
fi

# ===========================================================================
# PHASE 2 — EXECUTE. All guards passed; mutations below are safe.
# ===========================================================================

# Guard 1: remove ONLY the merged-branch worktree (never a glob). The success
# `note` is suppressed in --dry-run (the DRY-RUN: line already states the action),
# so dry-run never reports work as done that was only previewed.
#
# THE WORKTREE GOES FIRST, AND THE LANE LOCK IS STILL HELD WHILE IT DOES (#3436, roborev
# job 462, High). The previous order released the lock here and removed the worktree
# afterwards, which opens a window that destroys exactly what this issue exists to protect:
# once the CAS release succeeds the lane reads FREE while its directory still EXISTS, so a
# peer session with a cwd inside the lane can `acquire` it, be told it owns the lane, start
# writing — and then the very next line deletes its LIVE worktree. Guard 5 cannot see that
# peer, because it ran before the release created the opening.
#
# Holding the lock across the removal NARROWS that window; it does NOT close it, and this
# comment said "closes" until roborev job 463 (High) corrected it. The distinction is
# load-bearing, so it is stated rather than glossed:
#
#   * WHAT IS EXCLUDED. A peer whose `acquire` finds our record with a LIVE holder is told
#     OCCUPIED and does not adopt the lane. That covers the ordinary case, and it is the
#     whole of the old unconditional window — before this reordering the lane read FREE for
#     the entire removal, so ANY peer acquire in the gap destroyed live work.
#
#   * WHAT REMAINS (DECLARED GAP). Leaving the record present is not the same as holding a
#     MUTEX across the removal. Nothing here serialises against `lane-lock.sh`'s own per-issue
#     mutex, so if the RECORDED HOLDER EXITS while `git worktree remove` is still running, a
#     peer's acquire may legitimately classify the record as DEAD-*, reclaim it (exactly what
#     AC3's auto-reclaim is for) and begin working in a directory that is being deleted
#     underneath it. The lease mismatch below then detects the race only AFTER the damage.
#     The residual therefore needs the holder to die DURING the removal — a seconds-long
#     window, not the unconditional one — but it is a real hole and not a theoretical one.
#
#   * WHY IT IS NOT CLOSED HERE. Closing it means running the removal INSIDE lane-lock.sh's
#     per-issue mutex, which needs a capability that script does not expose today (a
#     `with-lock <issue> -- <cmd>` form). Re-deriving the mutex path in this file instead
#     would duplicate a derivation that already has a mangling/cksum fallback, and a
#     second copy of that is how two writers disagree about one lock. That is new surface,
#     so it is proposed as a follow-up rather than smuggled into this change.
#
# The old justification for release-first was READABILITY ("a release after the lane is gone
# reads like an afterthought") plus keeping the lane diagnosable if the release failed. That
# is not worth an irreversible loss of a peer's work, and it was also unnecessary: releasing
# after the removal is fully supported. `cmd_release` takes the lane directory from the
# RECORD (`parse_record` -> `REC_LANE_DIR`), never from the cwd — lane-lock.sh:1834-1841
# documents this once-broken case in as many words ("once that worktree was REMOVED the cwd
# identity walk had nothing to match") — and the `--force` this call passes resolves NO
# identity at all, so nothing on that path can refuse for want of a directory.
#
# What this trades into is strictly smaller and recoverable: if the removal succeeds and the
# release then fails, the record is ORPHANED (it names a lane directory that is gone) rather
# than a peer's work being deleted. That is clearable with `release --force`.
if [ -n "$target_wt" ]; then
  run git_root worktree remove "$target_wt"
  [ "$DRY_RUN" -eq 1 ] || note "removed worktree $target_wt"
elif [ "$stale_wt" -eq 1 ]; then
  run git_root worktree prune
  [ "$DRY_RUN" -eq 1 ] || note "pruned stale worktree entry for '$MERGED_BRANCH'"
else
  note "no worktree checked out on '$MERGED_BRANCH' — skipping worktree removal"
fi

if [ -n "$LANE_LEASE_TO_RELEASE" ]; then
  note "lane lock: releasing issue #$ISSUE, lease basis: $LANE_LEASE_BASIS"
  if ! run bash "$LANE_LOCK_SH" release "$ISSUE" --force --expect "$LANE_LEASE_TO_RELEASE"; then
    # ABORT, STILL. This was once a `note` and a fall-through, which roborev job 439 called
    # High and was right about: a lease check whose failure is non-fatal checks nothing.
    # The reason to abort is now different, and the message says so rather than inheriting
    # the old one. Guard 5 validated this incarnation; a mismatch HERE means the record
    # changed underneath us, so the lane may have been reclaimed (a DEAD-* verdict against
    # our own holder) while we were removing the worktree. We do not force past it: the
    # remaining teardown deletes BRANCHES, and a lane whose ownership is in doubt is not a
    # lane to keep tearing down unattended.
    echo "$prog: REFUSED — the lane lock for issue #$ISSUE could not be released at the lease" >&2
    echo "  Guard 5 validated ($LANE_LEASE_TO_RELEASE). The incarnation changed underneath us," >&2
    echo "  which means this lane's ownership is no longer the one finalization started with." >&2
    echo "  The worktree HAS already been removed (it is deleted while the lock is still held," >&2
    echo "  so no peer could have adopted this lane mid-removal); the lock RECORD may now be" >&2
    echo "  orphaned, naming a directory that is gone. No branch was deleted." >&2
    echo "  Remedy: inspect it, then clear it explicitly:" >&2
    echo "    bash $LANE_LOCK_SH probe $ISSUE" >&2
    echo "    bash $LANE_LOCK_SH release $ISSUE --force" >&2
    exit 6
  fi
fi

# Delete the origin lock (only the merged branch). Non-fatal: a TOCTOU race (ref
# removed concurrently) or a network blip must not abort under set -e after the
# worktree is already gone — surface it instead.
if [ "$remote_has_branch" -eq 1 ]; then
  if run git_root push "$REMOTE" --delete "$MERGED_BRANCH"; then
    [ "$DRY_RUN" -eq 1 ] || note "deleted origin lock $REMOTE/$MERGED_BRANCH"
  else
    note "could not delete origin lock $REMOTE/$MERGED_BRANCH — left in place (delete it manually)"
  fi
else
  note "origin branch '$MERGED_BRANCH' already absent (likely deleted by gh pr merge --delete-branch) — nothing to delete"
fi

# Local branch: delete when we've PROVEN containment in $MAIN_REF
# (local_tip_in_main) or the merge is confirmed; otherwise leave it. We use -D in
# both delete cases because we've done the merge check ourselves against
# $MAIN_REF — `git branch -d` would re-judge against the repo's *local* main,
# which often lags origin/main and would 128-abort post-mutation. Deletion is also
# made non-fatal (|| note) so it can never abort after the worktree/lock are gone.
# Decision is computed (not inferred from run's echo) so --dry-run previews truly.
if git_root show-ref --verify --quiet "refs/heads/${MERGED_BRANCH}"; then
  if [ "$local_tip_in_main" -eq 1 ] || [ "$CONFIRM_UNMERGED" -eq 1 ]; then
    if run git_root branch -D "$MERGED_BRANCH"; then
      [ "$DRY_RUN" -eq 1 ] || note "deleted local branch $MERGED_BRANCH"
    else
      note "could not delete local branch '$MERGED_BRANCH' — left in place"
    fi
  else
    note "local branch '$MERGED_BRANCH' left in place (unmerged tip; pass --confirm-unmerged to force-delete)"
  fi
fi

note "cleanup complete for issue #$ISSUE (branch $MERGED_BRANCH)"
