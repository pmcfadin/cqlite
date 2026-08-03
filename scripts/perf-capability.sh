#!/usr/bin/env bash
# shellcheck shell=bash
# perf-capability.sh — the ONE place that knows how a CQLite box is made
# profileable, and how that is VERIFIED (issue #3249).
#
# WHY THIS FILE EXISTS. Agent/worker images ship with
# `kernel.perf_event_paranoid = 4` and set it NOWHERE in /etc/sysctl.conf or
# /etc/sysctl.d — so every profiling run starts from a hard EACCES whose help text
# ("access limited") reads like a CAPABILITY verdict when it is a PERMISSION
# verdict. That has already cost two measurement cycles. The same three-line
# incantation was then copy-pasted into ad-hoc harnesses; it now lives here, is
# git-pinned, and is asserted by the gate's tooling-tests.
#
# WHY -1 AND NOT 1. perf_event_paranoid is CUMULATIVE — higher is MORE
# restrictive, and each level keeps the restrictions of the levels below it:
#   >= 2  no kernel profiling
#   >= 1  no CPU-WIDE event access  <-- kills `perf stat -C <cpu>`
#   >= 0  no raw tracepoint access
#     -1  no restriction, and the perf mlock limit is lifted too
# CQLite's measurement doctrine mandates per-CPU collection (`perf stat -C`), so
# `1` is not "almost right", it is a hard denial. `0` is the bare minimum that
# works; `-1` additionally avoids `perf record` ring-buffer surprises.
# `kernel.kptr_restrict = 0` is a separate control needed for kernel SYMBOL
# resolution — without it kernel frames render as unresolved addresses, which is a
# SILENT attribution loss, not an error.
#
# SECURITY POSTURE. This is a deliberate loosening appropriate for DEDICATED
# SINGLE-TENANT measurement/agent boxes. Never apply it to a shared or
# multi-tenant host. See docs/development/fleet-runbook.md.
#
# BPF IS A DIFFERENT PERMISSION. A permissive perf_event_paranoid does NOT grant
# BPF map creation — bpftrace/bcc collectors still need sudo (#3217 finding).
#
# Sourceable AND executable. Sourcing has NO side effects: this file only defines
# `perf_capability_*` functions (nothing runs, no shell options are changed, no
# variables outside that namespace are touched). Every function is `set -u` safe.
#
# Usage (executed):
#   bash scripts/perf-capability.sh --token        # free /proc read -> one token
#   bash scripts/perf-capability.sh --verify       # functional perf stat check
#   bash scripts/perf-capability.sh --drop-in      # canonical sysctl.d file bytes
#   bash scripts/perf-capability.sh --drop-in-path # where that file belongs
#
# Test-only env seams (NEVER set these in production; they exist so the gate's
# tooling-tests can exercise the write/read-back paths without touching /etc):
#   CQLITE_PERF_PROC_DIR    stand-in for /proc/sys/kernel
#   CQLITE_PERF_SYSCTL_DIR  stand-in for /etc/sysctl.d

# ---- locations (both overridable for tests only) ----------------------------
perf_capability_proc_dir()   { printf '%s' "${CQLITE_PERF_PROC_DIR:-/proc/sys/kernel}"; }
perf_capability_sysctl_dir() { printf '%s' "${CQLITE_PERF_SYSCTL_DIR:-/etc/sysctl.d}"; }
perf_capability_dropin_path() { printf '%s/99-cqlite-perf.conf' "$(perf_capability_sysctl_dir)"; }

# perf_capability_dropin_content: the EXACT bytes of the managed drop-in. It is a
# WHOLE managed file (not a delimited block inside a foreign one), so idempotency
# is a plain byte-compare of the entire file — simpler and safer than editing
# someone else's config. Callers may pipe this straight into `sudo tee`, which is
# also the remedy line bootstrap prints, so a hand-applied fix produces
# byte-identical content and the next bootstrap run is a silent no-op.
perf_capability_dropin_content() {
  cat <<'EOF'
# Managed by scripts/bootstrap-agent-machine.sh — CQLite issue #3249. Do not edit.
# Unprivileged perf profiling on a DEDICATED SINGLE-TENANT measurement/agent box.
#
# perf_event_paranoid is cumulative (higher = more restrictive). >= 1 forbids
# CPU-WIDE event access, which is exactly what `perf stat -C <cpu>` needs, so -1
# (not 1) is required; -1 also lifts the perf mlock limit. kptr_restrict = 0 lets
# kernel symbols resolve — otherwise kernel frames are unresolved addresses, a
# silent attribution loss rather than an error.
#
# This is a deliberate loosening. NOT for shared or multi-tenant hosts.
# Rationale + verification: docs/development/fleet-runbook.md
kernel.perf_event_paranoid = -1
kernel.kptr_restrict = 0
EOF
}

# perf_capability_dropin_current: rc 0 iff the drop-in exists with EXACTLY the
# managed bytes (the idempotency test — a matching file means write nothing).
perf_capability_dropin_current() {
  local path
  path=$(perf_capability_dropin_path)
  [ -f "$path" ] || return 1
  perf_capability_dropin_content | diff -q - "$path" >/dev/null 2>&1
}

# perf_capability_proc_value <name>: the CURRENT kernel value read straight from
# /proc/sys/kernel/<name> (rc 1 + no output when unreadable). NEVER trust a
# `sysctl -w`'s return code — a write can report success while the value does not
# take (container, read-only sysfs, a competing drop-in applied later). Read back.
perf_capability_proc_value() {
  local f
  f="$(perf_capability_proc_dir)/$1"
  [ -r "$f" ] || return 1
  local v
  v=$(cat "$f" 2>/dev/null) || return 1
  v="${v%%[[:space:]]*}"
  [ -n "$v" ] || return 1
  printf '%s' "$v"
}

# perf_capability_is_int <value>: rc 0 iff <value> is a plain optionally-negative
# integer NARROW ENOUGH for shell arithmetic. Both halves are load-bearing: `[ 1abc
# -ge 1 ]` and `[ 99999999999999999999999 -ge 1 ]` do NOT compare — each prints
# "integer expression expected" to stderr and returns FALSE, so a malformed or
# oversized value would fall past the `>= 1` test and be reported as `ok` (a WRONG
# capability claim) while leaking an error line into the gate's output. Validate the
# shape here instead of trusting the read.
perf_capability_is_int() {
  local body="${1#-}"
  [ -n "$body" ] || return 1
  case "$body" in *[!0-9]*) return 1 ;; esac
  [ "${#body}" -le 10 ]
}

# perf_capability_token: the FREE capability read — pure /proc, ZERO subprocesses
# that fork a binary beyond `cat`, no `perf` exec, no measurable time cost. This is
# what the gate's accelerators line calls, so it may never grow an exec.
#   ok               unprivileged per-CPU profiling AND kernel symbols available
#   paranoid-<N>     perf_event_paranoid = N >= 1 -> CPU-wide `perf stat -C` denied
#   kptr-restricted  paranoid is fine but kptr_restrict != 0 -> no kernel symbols
#   absent           the /proc controls are not present (container, non-Linux)
#   unknown          present but unparseable (never guess a capability)
perf_capability_token() {
  local p k
  p=$(perf_capability_proc_value perf_event_paranoid) || { printf 'absent'; return 0; }
  k=$(perf_capability_proc_value kptr_restrict) || { printf 'absent'; return 0; }
  perf_capability_is_int "$p" || { printf 'unknown'; return 0; }
  perf_capability_is_int "$k" || { printf 'unknown'; return 0; }
  if [ "$p" -ge 1 ]; then printf 'paranoid-%s' "$p"; return 0; fi
  if [ "$k" -ne 0 ]; then printf 'kptr-restricted'; return 0; fi
  printf 'ok'
}

# perf_capability_verify: the FUNCTIONAL verification (issue #3249 AC2). A
# bootstrap that silently leaves a box unprofileable is the failure mode being
# fixed, so the verdict comes from RUNNING the collection the doctrine mandates —
# `perf stat -C 0 -e cycles` — and requires BOTH exit 0 AND a non-zero cycle
# count. `perf stat` exits 0 while printing `<not supported>` / `<not counted>`
# (and a virtualised PMU can report a flat 0), so an rc-only check is exactly the
# false green this exists to prevent.
#
# CSV mode (`-x,`) is parsed rather than the human table: the human renderer is
# locale-formatted (`1.234.567`) and column layout has changed across perf
# releases, while the CSV shape `<count>,<unit>,<event>,...` is stable.
# Prints a short machine-greppable reason (stdout) either way; rc 0 = verified.
perf_capability_verify() {
  command -v perf >/dev/null 2>&1 || { printf 'no-perf-binary'; return 1; }
  local bound="" out rc count
  bound=$(command -v timeout 2>/dev/null || command -v gtimeout 2>/dev/null || true)
  if [ -n "$bound" ]; then
    out=$(LC_ALL=C "$bound" 30 perf stat -x, -e cycles -C 0 -- sleep 0.1 2>&1); rc=$?
  else
    out=$(LC_ALL=C perf stat -x, -e cycles -C 0 -- sleep 0.1 2>&1); rc=$?
  fi
  if [ "$rc" -ne 0 ]; then
    printf 'perf-stat-failed rc=%s: %s' "$rc" "$(printf '%s' "$out" | tr '\n' ';' | cut -c1-160)"
    return 1
  fi
  count=$(printf '%s\n' "$out" | awk -F, '$3 ~ /^cycles/ { print $1; exit }')
  case "$count" in
    '') printf 'no-cycles-row: %s' "$(printf '%s' "$out" | tr '\n' ';' | cut -c1-160)"; return 1 ;;
    *'not supported'*|*'not counted'*) printf 'counter-%s' "$(printf '%s' "$count" | tr -d '<>' | tr ' ' '-')"; return 1 ;;
  esac
  case "$count" in
    ''|*[!0-9]*) printf 'unparseable-count=%s' "$count"; return 1 ;;
  esac
  if [ "$count" -le 0 ]; then printf 'zero-cycles'; return 1; fi
  printf 'cycles=%s' "$count"
}

perf_capability_usage() {
  printf 'usage: %s --token | --verify | --drop-in | --drop-in-path\n' "${0##*/}"
  printf '  --token         free /proc capability read: ok|paranoid-<N>|kptr-restricted|absent|unknown\n'
  printf '  --verify        functional check: perf stat -C 0 -e cycles (rc 0 = verified)\n'
  printf '  --drop-in       print the canonical /etc/sysctl.d/99-cqlite-perf.conf bytes\n'
  printf '  --drop-in-path  print where that file belongs\n'
}

perf_capability_main() {
  case "${1:-}" in
    --token)        perf_capability_token; printf '\n' ;;
    --verify)       local v rc=0; v=$(perf_capability_verify) || rc=1; printf '%s\n' "$v"; return $rc ;;
    --drop-in)      perf_capability_dropin_content ;;
    --drop-in-path) perf_capability_dropin_path; printf '\n' ;;
    -h|--help|'')   perf_capability_usage ;;
    *)              printf 'perf-capability: unknown arg: %s\n' "$1" >&2; perf_capability_usage >&2; return 2 ;;
  esac
}

# Executed directly (never when sourced): shell options are set HERE, inside the
# guard, so sourcing can never change a caller's `set` flags.
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
  set -uo pipefail
  perf_capability_main "$@"
fi
