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
# that worktree is gone; it is the PRUNE's ownership proof only. Recognising an already-isolated
# target does NOT depend on it (`cargo clean` deletes it): a target ending in `/wt/<h>` where
# `<h>` is THIS checkout's hash resolves to itself, and the marker is rewritten every call.
# A target ending in `/wt/<other 16 hex>` that carries a marker belongs to an outer gate on a
# different root: it is stripped so a nested gate gets a sibling dir rather than nesting.
#
# The EFFECTIVE base follows cargo's own precedence: CARGO_TARGET_DIR, then
# CARGO_BUILD_TARGET_DIR, then the `build.target-dir` config (read with
# `cargo metadata --no-deps`, only when neither variable is set). The result is always exported
# as CARGO_TARGET_DIR by the gate, which overrides all three.
#
# USAGE
#   worktree-target-dir.sh [REPO_ROOT]     print the isolated dir (creates it + marker); rc 1
#                                          with no output when isolation does not apply
#   source it and call gate_worktree_target_dir REPO_ROOT
#
# Does not apply when no ABSOLUTE base resolves, or the resolved base lies inside the checkout
# (cargo's default `<root>/target` is already per-checkout). Opt out with CQLITE_SHARED_TARGET=1.

gate_worktree_target_hash() {  # <physical root>
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum <<<"$1" | cut -c1-16
  else
    shasum -a 256 <<<"$1" | cut -c1-16
  fi
}

# gate_worktree_target_effective <root>: cargo's effective target dir, or rc 1 if none is absolute.
gate_worktree_target_effective() {
  local b="${CARGO_TARGET_DIR:-${CARGO_BUILD_TARGET_DIR:-}}"
  if [ -z "$b" ]; then
    b=$(cd "$1" 2>/dev/null && cargo metadata --no-deps --format-version 1 2>/dev/null \
          | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')
  fi
  case "$b" in /*) printf '%s' "$b" ;; *) return 1 ;; esac
}

gate_worktree_target_dir() {
  local root base h dir
  [ "${CQLITE_SHARED_TARGET:-0}" = 1 ] && return 1
  root=$(cd "${1:-.}" 2>/dev/null && pwd -P) || return 1
  base=$(gate_worktree_target_effective "$root") || return 1
  case "$base/" in "$root"/*) return 1 ;; esac   # checkout-local default: already per-worktree
  h=$(gate_worktree_target_hash "$root")
  case "$base" in
    */wt/"$h") base=${base%/wt/*} ;;
    */wt/[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f])
      [ -f "$base/.worktree-root" ] && base=${base%/wt/*} ;;
  esac
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
