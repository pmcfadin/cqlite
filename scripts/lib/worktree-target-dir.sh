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
# Exit codes of gate_worktree_target_dir: 0 isolated (dir printed); 1 not applicable (opt-out, or
# cargo's checkout-local default `<root>/target`, already per-worktree); 2 COULD NOT RESOLVE or
# create the dir (reason on stderr). The gate fails closed on 2. Opt out with CQLITE_SHARED_TARGET=1.

gate_worktree_target_hash() {  # <physical root>
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum <<<"$1" | cut -c1-16
  else
    shasum -a 256 <<<"$1" | cut -c1-16
  fi
}

# gate_worktree_canon <abs path>: physical path, also for components that do not exist yet. Walks
# the components: `.` is dropped, `..` pops one, an existing directory is resolved with `cd -P`
# (so a symlinked `target` is seen for what it is), a missing one is appended as written. This
# normalises `<checkout>/missing/../../shared` correctly (a `..` after a missing component pops it).
gate_worktree_canon() {
  local IFS=/ cur=/ c next
  local -a parts
  read -r -a parts <<<"${1#/}"
  for c in "${parts[@]}"; do
    case "$c" in
      ''|.) ;;
      ..) cur=$(dirname "$cur") ;;
      *)
        next="${cur%/}/$c"
        if [ -d "$next" ]; then cur=$(cd -P "$next" 2>/dev/null && pwd -P) || return 1; else cur=$next; fi ;;
    esac
  done
  printf '%s' "$cur"
}

# gate_worktree_target_effective <root>: cargo's effective target dir (absolute).
#   rc 0 prints it; rc 1 = nothing external is configured (metadata succeeded and the target is
#   cargo's checkout-local default, or the root has no Cargo.toml so there is nothing to build);
#   rc 2 = COULD NOT RESOLVE (metadata failed, no JSON reader, unparsable): the caller must fail
#   closed, because a shared target may be in play. A one-line reason is printed to stderr.
# The config route reads ONLY the top-level `.target_directory` of `cargo metadata` (never a
# `[workspace.metadata]` key), offline and --locked so it cannot touch the network or Cargo.lock.
gate_worktree_target_effective() {
  local b="${CARGO_TARGET_DIR:-${CARGO_BUILD_TARGET_DIR:-}}" json
  if [ -z "$b" ]; then
    [ -f "$1/Cargo.toml" ] || return 1
    json=$(cd "$1" 2>/dev/null && cargo metadata --no-deps --offline --locked --format-version 1 2>/dev/null) || json=""
    if [ -z "$json" ]; then
      echo "worktree-target-dir: cannot resolve cargo's target dir ('cargo metadata --offline --locked' failed in $1)" >&2
      return 2
    elif command -v jq >/dev/null 2>&1; then
      b=$(jq -r '.target_directory // empty' <<<"$json" 2>/dev/null)
    elif command -v python3 >/dev/null 2>&1; then
      b=$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("target_directory") or "")' <<<"$json" 2>/dev/null)
    else
      echo "worktree-target-dir: cannot resolve cargo's target dir (neither jq nor python3 to read it)" >&2
      return 2
    fi
  fi
  case "$b" in
    /*) gate_worktree_canon "$b" || { echo "worktree-target-dir: cannot canonicalise target dir '$b'" >&2; return 2; } ;;
    *) echo "worktree-target-dir: cannot resolve cargo's target dir (got '${b:-<empty>}', not absolute)" >&2; return 2 ;;
  esac
}

gate_worktree_target_dir() {
  local root base h dir rc
  [ "${CQLITE_SHARED_TARGET:-0}" = 1 ] && return 1
  root=$(cd "${1:-.}" 2>/dev/null && pwd -P) || return 1
  base=$(gate_worktree_target_effective "$root"); rc=$?
  [ "$rc" = 0 ] || return "$rc"
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
    mkdir -p "$base" 2>/dev/null || { echo "worktree-target-dir: cannot create $base" >&2; return 2; }
    printf 'Signature: 8a477f597d28d172789f06886806bc55\n# This file is a cache directory tag created by cqlite (worktree-target-dir.sh).\n# For information about cache directory tags see https://bford.info/cachedir/\n' >"$base/CACHEDIR.TAG" 2>/dev/null || true
  fi
  mkdir -p "$dir" 2>/dev/null || { echo "worktree-target-dir: cannot create $dir" >&2; return 2; }
  printf '%s\n' "$root" >"$dir/.worktree-root" 2>/dev/null || true
  printf '%s' "$dir"
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  gate_worktree_target_dir "${1:-.}"
fi
