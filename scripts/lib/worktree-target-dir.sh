#!/usr/bin/env bash
#
# worktree-target-dir.sh -- per-worktree CARGO_TARGET_DIR under a shared base (#4387).
#
# WHY
# ---
# A box that exports one CARGO_TARGET_DIR for every worktree (the dev box does, from
# ~/.profile) shares cargo fingerprints across worktrees: a workspace member's fingerprint
# does not include the checkout's absolute path. Worktree B therefore REUSES the test
# binaries built in worktree A, and those binaries have A's `env!("CARGO_MANIFEST_DIR")`
# baked in:
#   - A deleted  -> "No such file or directory" failures that are not in B's diff;
#   - A present  -> B's tests silently read A's test-data/fixtures, so a gate can certify
#                   B against another branch's files (the quiet half of #4387).
#
# WHAT
# ----
# The target dir for a checkout is `<base>/wt/<16 hex of sha256(physical root)>`, so every
# worktree gets its own workspace artifacts. Third-party compiles still hit sccache's shared
# object cache (RUSTC_WRAPPER), which is what keeps a new worktree's first build cheap.
# `<dir>/.worktree-root` records the root so cargo-target-prune.sh can delete the dir once
# that worktree is gone. A base that already ends in `/wt/<16 hex>` AND carries our `.worktree-root` marker (inherited from an outer
# gate or shell) is stripped first, so resolution is idempotent and a nested gate on a
# different root moves to its own dir rather than nesting.
#
# USAGE
#   worktree-target-dir.sh [REPO_ROOT]     print the isolated dir (creates it + marker); rc 1
#                                          with no output when isolation does not apply
#   source it and call gate_worktree_target_dir REPO_ROOT
#
# Applies only when CARGO_TARGET_DIR is set to an absolute path: unset means cargo's default
# `<root>/target`, already per-checkout. Opt out with CQLITE_SHARED_TARGET=1.

gate_worktree_target_base() {
  local b=$1
  case "$b" in
    */wt/[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]) [ -f "$b/.worktree-root" ] && b=${b%/wt/*} ;;
  esac
  printf '%s' "$b"
}

gate_worktree_target_dir() {
  local root base h dir
  [ "${CQLITE_SHARED_TARGET:-0}" = 1 ] && return 1
  case "${CARGO_TARGET_DIR:-}" in /*) ;; *) return 1 ;; esac
  root=$(cd "${1:-.}" 2>/dev/null && pwd -P) || return 1
  base=$(gate_worktree_target_base "$CARGO_TARGET_DIR")
  if command -v sha256sum >/dev/null 2>&1; then
    h=$(sha256sum <<<"$root" | cut -c1-16)
  else
    h=$(shasum -a 256 <<<"$root" | cut -c1-16)
  fi
  dir="$base/wt/$h"
  # The prune refuses a dir without cargo's CACHEDIR.TAG, and cargo only writes one inside the
  # dir it builds in, so a fresh base needs the standard tag from us.
  if [ ! -f "$base/CACHEDIR.TAG" ]; then
    mkdir -p "$base" 2>/dev/null || return 1
    printf 'Signature: 8a477f597d28d172789f06886806bc55\n# This file is a cache directory tag created by cqlite (worktree-target-dir.sh).\n# For information about cache directory tags see https://bford.info/cachedir/\n' >"$base/CACHEDIR.TAG" 2>/dev/null || true
  fi
  mkdir -p "$dir" 2>/dev/null || return 1
  printf '%s\n' "$root" >"$dir/.worktree-root" 2>/dev/null || true
  printf '%s' "$dir"
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  gate_worktree_target_dir "${1:-.}"
fi
