#!/usr/bin/env bash
#
# lane-builddir.sh -- find, remove and report a lane's gate build dir (#4641).
#
# Every gate run builds into `$CARGO_TARGET_DIR/wt/<16 hex>` (~25 GiB, #4387). Nothing used to
# delete it when the lane ended, so the cache volume filled until a full gate failed disk admission.
#
# SUBCOMMANDS
#   path <worktree-path>                  print the lane's wt dir (works after the worktree is gone)
#   remove <worktree-path> [--worktree-removed]
#   remove --dir <wt-dir> [--worktree-removed]
#                                         delete the lane's wt dir. Refuses a live git worktree
#                                         unless --worktree-removed (the caller has just removed
#                                         it: finalize-cleanup.sh). --dir takes the `target-dir:`
#                                         of the lane's gate summary; its marker names the lane.
#                                         Refuses anything that is not a direct `<base>/wt/<16 hex>`
#                                         child, or is a symlink.
#   stale                                 REPORT ONLY: wt/* dirs whose hash matches no current
#                                         `git worktree list` entry, with size and age.
#
# THE HASH IS THE GATE'S, NOT A COPY: both `path` and `stale` call gate_worktree_target_path from
# scripts/lib/worktree-target-dir.sh, the function the gate itself builds its target dir with.
#
# EXIT CODES  0 ok / nothing to do · 2 refused · 64 usage
set -uo pipefail

prog=$(basename "$0")
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCRIPT_REPO=$(cd "$SCRIPT_DIR/../.." && pwd -P)
# shellcheck source=../lib/worktree-target-dir.sh
. "$SCRIPT_REPO/scripts/lib/worktree-target-dir.sh"

die()    { echo "$prog: $*" >&2; exit 64; }
refuse() { echo "$prog: REFUSED -- $*" >&2; exit 2; }

# The repo whose worktree list is authoritative: the caller's cwd repo, else this script's.
repo_for_list() {
  if git rev-parse --git-dir >/dev/null 2>&1; then pwd -P; else printf '%s' "$SCRIPT_REPO"; fi
}

live_worktrees() {
  git -C "$(repo_for_list)" worktree list --porcelain 2>/dev/null | sed -n 's/^worktree //p'
}

# base_dir: the shared base `<base>` under which `wt/` lives.
base_dir() {
  local d
  d=$(gate_worktree_target_path "$SCRIPT_REPO" "$SCRIPT_REPO"); local rc=$?
  [ "$rc" = 0 ] || return "$rc"
  printf '%s' "${d%/wt/*}"
}

# lane_dir <worktree path>: the wt dir for that path (no side effects, path need not exist).
lane_dir() {
  case "$1" in /*) ;; *) set -- "$(pwd -P)/$1" ;; esac
  gate_worktree_target_path "$1" "$SCRIPT_REPO"
}

is_hex16() { case "$1" in *[!0-9a-f]*|'') return 1 ;; esac; [ "${#1}" = 16 ]; }

human() {  # KiB -> GiB string
  awk -v k="$1" 'BEGIN{printf "%.1f GiB", k/1048576}'
}

cmd_path() {
  [ $# -eq 1 ] || die "usage: path <worktree-path>"
  local d rc
  d=$(lane_dir "$1"); rc=$?
  [ "$rc" = 0 ] || { echo "$prog: no per-worktree target dir applies (rc=$rc)" >&2; exit 2; }
  printf '%s\n' "$d"
}

cmd_remove() {
  local wt="" dir="" removed=0 base rc live marker root
  while [ $# -gt 0 ]; do
    case "$1" in
      --worktree-removed) removed=1; shift ;;
      --dir) [ $# -ge 2 ] || die "--dir requires a value"; dir="$2"; shift 2 ;;
      -*) die "unknown flag $1" ;;
      *) [ -z "$wt" ] || die "one worktree path only"; wt="$1"; shift ;;
    esac
  done
  [ -n "$wt" ] || [ -n "$dir" ] || die "usage: remove <worktree-path>|--dir <wt-dir> [--worktree-removed]"
  [ -z "$wt" ] || [ -z "$dir" ] || die "give a worktree path OR --dir, not both"
  base=$(base_dir); rc=$?
  [ "$rc" = 0 ] || refuse "cannot establish the shared target base (rc=$rc)"
  if [ -n "$wt" ]; then
    dir=$(lane_dir "$wt") || refuse "no per-worktree target dir applies to $wt"
    root=$(gate_worktree_canon "$(case "$wt" in /*) printf '%s' "$wt" ;; *) printf '%s/%s' "$(pwd -P)" "$wt" ;; esac)")
  fi
  # Containment: a direct `<base>/wt/<16 hex>` child, never a symlink, nothing else.
  case "$dir" in /*) ;; *) refuse "$dir is not an absolute path" ;; esac
  dir=${dir%/}
  [ "$(dirname "$dir")" = "$base/wt" ] || refuse "$dir is not directly under $base/wt/"
  is_hex16 "$(basename "$dir")" || refuse "$dir is not a <16 hex> lane dir"
  [ ! -L "$dir" ] || refuse "$dir is a symlink"
  [ -d "$dir" ] || { echo "$prog: nothing to remove ($dir absent)"; exit 0; }
  if [ -z "$wt" ]; then
    marker=$(head -1 "$dir/.worktree-root" 2>/dev/null || true)
    root=$marker
  fi
  # Liveness: the lane's worktree is in `git worktree list`, or its checkout still exists.
  live=0
  if [ -n "$root" ]; then
    if live_worktrees | grep -Fxq -- "$root"; then live=1; fi
    [ -e "$root/.git" ] && live=1
  elif [ "$removed" = 0 ]; then
    refuse "$dir has no .worktree-root marker, so its worktree cannot be identified; pass the worktree path or --worktree-removed"
  fi
  if [ "$live" = 1 ] && [ "$removed" = 0 ]; then
    refuse "worktree $root is still a live git worktree; remove it first (finalize-cleanup.sh does) or pass --worktree-removed"
  fi
  if [ "$live" = 1 ]; then
    refuse "worktree $root still exists even though --worktree-removed was given"
  fi
  rm -rf -- "$dir" || refuse "could not delete $dir"
  echo "$prog: removed $dir"
}

cmd_stale() {
  [ $# -eq 0 ] || die "usage: stale"
  local base rc d h wt live_hashes="" total=0 n=0 kb age now mt
  base=$(base_dir); rc=$?
  [ "$rc" = 0 ] || { echo "$prog: cannot establish the shared target base (rc=$rc)" >&2; exit 2; }
  while IFS= read -r wt; do
    [ -n "$wt" ] || continue
    d=$(gate_worktree_target_path "$wt" "$SCRIPT_REPO") || continue
    live_hashes="$live_hashes ${d##*/}"
  done < <(live_worktrees)
  echo "stale gate build dirs under $base/wt (no matching live worktree; REPORT ONLY, nothing deleted):"
  now=$(date +%s)
  for d in "$base"/wt/*; do
    [ -d "$d" ] && [ ! -L "$d" ] || continue
    h=${d##*/}
    is_hex16 "$h" || continue
    case "$live_hashes " in *" $h "*) continue ;; esac
    kb=$(du -sk "$d" 2>/dev/null | cut -f1); kb=${kb:-0}
    mt=$(stat -c %Y "$d" 2>/dev/null || echo "$now")
    age=$(( (now - mt) / 86400 ))
    printf '  %s  %s  untouched %sd  root=%s\n' "$(human "$kb")" "$d" "$age" "$(head -1 "$d/.worktree-root" 2>/dev/null || echo '?')"
    total=$((total + kb)); n=$((n + 1))
  done
  echo "  total: $n stale dir(s), $(human "$total") reclaimable; free one with: bash scripts/flow/lane-builddir.sh remove --dir <dir>"
}

sub=${1:-}; [ $# -gt 0 ] && shift
case "$sub" in
  path) cmd_path "$@" ;;
  remove) cmd_remove "$@" ;;
  stale) cmd_stale "$@" ;;
  *) die "usage: $prog path|remove|stale ..." ;;
esac
