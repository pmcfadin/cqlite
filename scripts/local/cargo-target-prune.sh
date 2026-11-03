#!/usr/bin/env bash
#
# cargo-target-prune.sh — weekly prune of the shared Cargo target dir (#4411).
#
# WHY THIS EXISTS
# ---------------
# Every session on the dev box (Claude and Codex share the `dev` user) builds into
# one CARGO_TARGET_DIR on /home/dev/.cache-dev, and nothing deletes cache there
# automatically. Two things accumulate:
#
#   1. `incremental/` session dirs, which cargo never evicts.
#   2. Workspace-crate test binaries whose fingerprint still matches but which
#      have ANOTHER worktree's CARGO_MANIFEST_DIR baked in. When that worktree is
#      deleted, the next gate runs them and fails on "No such file or directory"
#      (#4387); while it still exists they are a false-green risk.
#
# This script removes stale incremental dirs and the WORKSPACE crates'
# `.fingerprint/` entries in every profile dir (debug, release*, and every
# `agent-gate-side/<component>/<profile>`), so workspace crates rebuild once and
# third-party dependencies stay cached.
#
# SAFETY
# ------
# - Never runs under a build: it SKIPS (exit 0) if a cargo/rustc/cargo-nextest
#   process or an active `cqlite-gate-*` user unit exists, and it holds every
#   profile's `.cargo-lock` (the lock cargo itself takes) while deleting, so a
#   build cannot start mid-prune.
# - Refuses a directory that lacks cargo's CACHEDIR.TAG.
# - Deletes only under `<profile>/incremental/` and `<profile>/.fingerprint/<ws-pkg>-*`.
#
# USAGE
#   cargo-target-prune.sh [--dry-run] [--days N] [--target DIR] [--repo DIR]
#   cargo-target-prune.sh --install-timer [--repo DIR]
#   cargo-target-prune.sh --uninstall-timer
#
# Exit: 0 = PRUNED / DRY-RUN / SKIPPED-BUSY / timer change, 1 = ERROR, 2 = usage.
#
# Test seams (scripts/tests/test_cargo_target_prune.sh):
#   CQLITE_PRUNE_PGREP, CQLITE_PRUNE_SYSTEMCTL — command overrides
#   CQLITE_PRUNE_PACKAGES — space-separated workspace package names (skips cargo metadata)

set -euo pipefail

PGREP=${CQLITE_PRUNE_PGREP:-pgrep}
SYSTEMCTL=${CQLITE_PRUNE_SYSTEMCTL:-systemctl}
UNIT=cqlite-cargo-prune

dry_run=0 days=7 target="" repo="" action=prune

usage() { sed -n '/^# USAGE/,/^# Exit/p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2; }

while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) dry_run=1 ;;
    --days) [ $# -ge 2 ] || usage; days=$2; shift ;;
    --target) [ $# -ge 2 ] || usage; target=$2; shift ;;
    --repo) [ $# -ge 2 ] || usage; repo=$2; shift ;;
    --install-timer) action=install ;;
    --uninstall-timer) action=uninstall ;;
    -h|--help) usage ;;
    *) echo "cargo-target-prune: unknown argument: $1" >&2; usage ;;
  esac
  shift
done
case "$days" in ''|*[!0-9]*) echo "cargo-target-prune: --days must be a non-negative integer" >&2; exit 2 ;; esac

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
[ -n "$repo" ] || repo=$(cd "$script_dir/../.." && pwd)

result() { echo "RESULT: $1"; }

# ---- timer install / uninstall -------------------------------------------------
unit_dir=${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user
libexec=${XDG_DATA_HOME:-$HOME/.local/share}/cqlite

if [ "$action" = uninstall ]; then
  "$SYSTEMCTL" --user disable --now "$UNIT.timer" >/dev/null 2>&1 || true
  rm -f "$unit_dir/$UNIT.service" "$unit_dir/$UNIT.timer"
  "$SYSTEMCTL" --user daemon-reload
  result UNINSTALLED
  exit 0
fi

if [ "$action" = install ]; then
  # Install a COPY of the script and pin everything a scheduled run needs, so
  # it never depends on the installing worktree (which flow-finalize deletes)
  # or on the systemd user manager's PATH (which usually lacks ~/.cargo/bin).
  cargo_bin=$(command -v cargo) || { echo "PRUNE: cargo not on PATH; cannot pin it for the timer"; result ERROR; exit 1; }
  # The main checkout outlives every linked worktree: the parent of git's common dir.
  common=$(git -C "$repo" rev-parse --path-format=absolute --git-common-dir 2>/dev/null) || common=""
  stable_repo=${common:+$(dirname "$common")}
  [ -n "$stable_repo" ] && [ -f "$stable_repo/Cargo.toml" ] || stable_repo=$repo
  pin_target=${target:-${CARGO_TARGET_DIR:-}}
  if [ -z "$pin_target" ]; then
    pin_target=$(cd "$stable_repo" && cargo metadata --no-deps --format-version 1 2>/dev/null |
      python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])') || pin_target=""
  fi
  [ -n "$pin_target" ] || { echo "PRUNE: cannot resolve the target dir to pin"; result ERROR; exit 1; }
  mkdir -p "$unit_dir" "$libexec"
  install -m 0755 "$script_dir/cargo-target-prune.sh" "$libexec/cargo-target-prune.sh"
  cat >"$unit_dir/$UNIT.service" <<EOF
[Unit]
Description=CQLite: prune stale incremental + workspace fingerprints in the shared Cargo target (#4411)

[Service]
Type=oneshot
Nice=19
IOSchedulingClass=idle
Environment=PATH=$(dirname "$cargo_bin"):/usr/local/bin:/usr/bin:/bin
ExecStart=/usr/bin/env bash $libexec/cargo-target-prune.sh --repo $stable_repo --target $pin_target
EOF
  cat >"$unit_dir/$UNIT.timer" <<EOF
[Unit]
Description=Weekly CQLite Cargo target prune (#4411)

[Timer]
OnCalendar=Sun *-*-* 04:00:00
RandomizedDelaySec=1h
Persistent=true

[Install]
WantedBy=timers.target
EOF
  "$SYSTEMCTL" --user daemon-reload
  "$SYSTEMCTL" --user enable --now "$UNIT.timer"
  echo "PRUNE: timer installed: $unit_dir/$UNIT.timer -> $libexec/cargo-target-prune.sh --repo $stable_repo --target $pin_target"
  result INSTALLED
  exit 0
fi

# ---- resolve the target dir ----------------------------------------------------
if [ -z "$target" ]; then
  target=${CARGO_TARGET_DIR:-}
  if [ -z "$target" ]; then
    target=$(cd "$repo" && cargo metadata --no-deps --format-version 1 2>/dev/null |
      python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])') || true
  fi
fi
if [ -z "$target" ] || [ ! -d "$target" ]; then
  echo "PRUNE: cannot resolve a target dir (got '${target}')"; result ERROR; exit 1
fi
target=$(cd "$target" && pwd -P)
if [ "$target" = / ] || [ "$target" = "$(cd "$HOME" && pwd -P)" ] || [ ! -f "$target/CACHEDIR.TAG" ]; then
  echo "PRUNE: refusing $target: not a cargo target dir (no CACHEDIR.TAG)"; result ERROR; exit 1
fi

# ---- workspace package names ---------------------------------------------------
if [ -n "${CQLITE_PRUNE_PACKAGES:-}" ]; then
  pkgs=$CQLITE_PRUNE_PACKAGES
else
  pkgs=$(cd "$repo" && cargo metadata --no-deps --format-version 1 2>/dev/null |
    python3 -c 'import json,sys; print(" ".join(p["name"] for p in json.load(sys.stdin)["packages"]))') || true
fi
if [ -z "$pkgs" ]; then
  echo "PRUNE: could not list workspace packages under $repo"; result ERROR; exit 1
fi

# ---- busy check ----------------------------------------------------------------
busy=""
for p in cargo rustc cargo-nextest; do
  if "$PGREP" -u "$(id -u)" -x "$p" >/dev/null 2>&1; then busy="$busy process:$p"; fi
done
units=$("$SYSTEMCTL" --user list-units --state=active --no-legend --plain 'cqlite-gate-*' 2>/dev/null || true)
if [ -n "$units" ]; then busy="$busy unit:$(awk '{print $1; exit}' <<<"$units")"; fi

profiles=()
while IFS= read -r fp; do profiles+=("$(dirname "$fp")"); done < <(find "$target" -maxdepth 4 -type d -name .fingerprint | sort)

# Hold every profile's build lock for the whole prune.
if [ -z "$busy" ]; then
  for prof in "${profiles[@]}"; do
    [ -f "$prof/.cargo-lock" ] || continue
    exec {fd}<>"$prof/.cargo-lock"
    if ! flock -n "$fd"; then busy="$busy lock:${prof#"$target"/}"; break; fi
  done
fi

if [ -n "$busy" ]; then
  echo "PRUNE: skipped, a build is active:$busy"
  result SKIPPED-BUSY
  exit 0
fi

# ---- prune -------------------------------------------------------------------
before=$(du -sb "$target" 2>/dev/null | awk '{print $1}')
n_inc=0 n_fp=0
rm_dir() { if [ "$dry_run" = 1 ]; then echo "would remove ${1#"$target"/}"; else rm -rf -- "$1"; fi; }

for prof in "${profiles[@]}"; do
  if [ -d "$prof/incremental" ]; then
    while IFS= read -r d; do rm_dir "$d"; n_inc=$((n_inc + 1)); done \
      < <(find "$prof/incremental" -mindepth 1 -maxdepth 1 -type d -mtime +"$days")
  fi
  for p in $pkgs; do
    names=("$p"); [ "${p//_/-}" = "$p" ] || names+=("${p//_/-}")
    for name in "${names[@]}"; do
      for d in "$prof/.fingerprint/$name"-*; do
        [ -d "$d" ] || continue
        # Exactly `<name>-<16 hex>`: never let `cqlite` match `cqlite-core-…`.
        [[ "${d##*/}" =~ ^"$name"-[0-9a-f]{16}$ ]] || continue
        rm_dir "$d"; n_fp=$((n_fp + 1))
      done
    done
  done
done

after=$(du -sb "$target" 2>/dev/null | awk '{print $1}')
echo "PRUNE: target=$target profiles=${#profiles[@]} days=$days incremental-removed=$n_inc fingerprints-removed=$n_fp freed-bytes=$(( ${before:-0} - ${after:-0} ))"
if [ "$dry_run" = 1 ]; then result DRY-RUN; else result PRUNED; fi
