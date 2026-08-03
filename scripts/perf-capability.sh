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
#   >= 3  (Debian/Ubuntu kernels carry this extra level) disallow ALL unprivileged
#         perf event use — which is why the images' `4` denies EVERYTHING, down to a
#         plain `perf stat`, not just the CPU-wide collection
#   >= 2  no kernel profiling
#   >= 1  no CPU-WIDE event access  <-- kills `perf stat -C <cpu>`
#   >= 0  no raw tracepoint access
#     -1  (almost) all events permitted, and the perf mlock limit is lifted too
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
# Sourceable AND executable. Source it ONCE (the gate does, at script scope) and call
# the functions; a per-use re-source re-reads 300+ lines for nothing. Sourcing has NO
# side effects: this file only defines
# `perf_capability_*` functions plus the three `PERF_CAPABILITY_*` path constants
# (nothing runs, no shell options are changed, no variables outside those namespaces
# are touched). Every function is `set -u` safe.
#
# Usage (executed):
#   bash scripts/perf-capability.sh --token        # free /proc read -> one token
#   bash scripts/perf-capability.sh --verify       # functional perf stat check
#   bash scripts/perf-capability.sh --drop-in      # canonical sysctl.d file bytes
#   bash scripts/perf-capability.sh --drop-in-path # where that file belongs
#
# TEST-ONLY ENV SEAMS — INERT UNLESS CQLITE_PERF_TEST_MODE=1 (issue #3249 review).
# The PRODUCTION paths below are HARDCODED LITERALS (/etc/sysctl.d, /proc/sys/kernel)
# because bootstrap pipes the drop-in through `sudo tee <path>`: if that path were
# env-derived, a single stray export (say CQLITE_PERF_SYSCTL_DIR=/etc/sudoers.d)
# would make ROOT write an attacker/accident-chosen file while the real drop-in was
# never installed — and an unparsable sudoers entry can wedge `sudo` outright.
# Likewise an env-chosen /proc stand-in would let a paranoid-4 box print a
# FABRICATED "verified" verdict. So the seams take effect ONLY under the explicit
# marker, and the marker is itself hermetic: with it set, a REAL `sudo`/`sysctl`
# reachable on PATH is a hard refusal (the suite PATH-shims both and declares the
# shim directory), so test mode can never reach a real privileged tool.
#   CQLITE_PERF_TEST_MODE=1     the marker; without it the two seams below are INERT
#   CQLITE_PERF_PROC_DIR        stand-in for /proc/sys/kernel   (test mode only)
#   CQLITE_PERF_SYSCTL_DIR      stand-in for /etc/sysctl.d      (test mode only)
#   CQLITE_PERF_TEST_PRIV_DIR   absolute dir holding the suite's sudo/sysctl shims

# ---- production locations: HARDCODED. Never env-derived outside test mode. ----
PERF_CAPABILITY_PROC_DIR_DEFAULT='/proc/sys/kernel'
PERF_CAPABILITY_SYSCTL_DIR_DEFAULT='/etc/sysctl.d'
PERF_CAPABILITY_DROPIN_BASENAME='99-cqlite-perf.conf'

# perf_capability_test_mode: rc 0 iff the explicit test marker is set.
perf_capability_test_mode() { [ "${CQLITE_PERF_TEST_MODE:-}" = 1 ]; }

# perf_capability_seam_set: rc 0 iff either test-only path seam is non-empty.
perf_capability_seam_set() {
  [ -n "${CQLITE_PERF_PROC_DIR:-}" ] || [ -n "${CQLITE_PERF_SYSCTL_DIR:-}" ]
}

# perf_capability_env_guard: rc 0 iff this process may act on the perf capability
# path at all. Both refusals are LOUD on stderr and FAIL CLOSED (the caller does
# nothing privileged and claims no verdict):
#   * a seam set WITHOUT the marker — the seams are inert there, and a caller that
#     was handed one is misconfigured, so nothing privileged may proceed;
#   * the marker set while a REAL sudo/sysctl is reachable — test mode is hermetic
#     by construction, so a reachable real privileged tool is a bug in the harness,
#     not something to run.
perf_capability_env_guard() {
  if ! perf_capability_test_mode; then
    perf_capability_seam_set || return 0
    printf 'perf-capability: REFUSING to act: CQLITE_PERF_PROC_DIR/CQLITE_PERF_SYSCTL_DIR are TEST-ONLY seams (inert without CQLITE_PERF_TEST_MODE=1). Unset them; the production paths are %s and %s.\n' \
      "$PERF_CAPABILITY_SYSCTL_DIR_DEFAULT" "$PERF_CAPABILITY_PROC_DIR_DEFAULT" >&2
    return 1
  fi
  local dir="${CQLITE_PERF_TEST_PRIV_DIR:-}" tool resolved
  for tool in sudo sysctl; do
    resolved=$(command -v "$tool" 2>/dev/null) || continue
    case "$dir" in
      /*) ;;
      *) printf 'perf-capability: REFUSING: CQLITE_PERF_TEST_MODE=1 with a reachable real %s and no absolute CQLITE_PERF_TEST_PRIV_DIR — test mode must PATH-shim every privileged tool.\n' "$tool" >&2
         return 1 ;;
    esac
    case "$resolved" in
      "$dir"/*) ;;
      *) printf 'perf-capability: REFUSING: CQLITE_PERF_TEST_MODE=1 but %s resolves to %s, outside the declared shim dir %s — test mode may never invoke a real privileged tool.\n' \
           "$tool" "$resolved" "$dir" >&2
         return 1 ;;
    esac
  done
  return 0
}

# ---- resolved locations (the seams apply ONLY in test mode) -------------------
# THE `*_into <outvar>` CONVENTION. The gate's summary path calls the token chain
# below and is contractually FREE — no external process AND no command substitution
# (each `$( )` forks a subshell, which is a process too). A function that answers on
# stdout therefore CANNOT be on that path: its caller must fork to read it. So every
# function the gate touches has an `_into <outvar>` core that assigns through a
# caller-named variable, and the stdout-printing form is a thin wrapper kept for the
# CLI/bootstrap ergonomics — the wrapper is the ONLY place a fork is paid, and it is
# not on the gate's path.
#   Assignment is `eval "$1=\$var"`, NOT a `local -n` nameref: bash 3.2 (macOS
#   /bin/bash, a supported gate host) has no namerefs. The RHS is an assignment, so no
#   word-splitting or globbing applies to the value. <outvar> must be a plain shell
#   identifier; every caller passes a literal, and the `__pcd_`/`__pcr_`/`__pct_`
#   local prefixes keep a caller-named variable from colliding with an internal one.
perf_capability_proc_dir_into() {
  if perf_capability_test_mode; then
    eval "$1=\${CQLITE_PERF_PROC_DIR:-\$PERF_CAPABILITY_PROC_DIR_DEFAULT}"
  else
    eval "$1=\$PERF_CAPABILITY_PROC_DIR_DEFAULT"
  fi
}
perf_capability_proc_dir() {
  local __pcd_v
  perf_capability_proc_dir_into __pcd_v
  printf '%s' "$__pcd_v"
}
perf_capability_sysctl_dir() {
  if perf_capability_test_mode; then
    printf '%s' "${CQLITE_PERF_SYSCTL_DIR:-$PERF_CAPABILITY_SYSCTL_DIR_DEFAULT}"
  else
    printf '%s' "$PERF_CAPABILITY_SYSCTL_DIR_DEFAULT"
  fi
}
perf_capability_dropin_path() {
  printf '%s/%s' "$(perf_capability_sysctl_dir)" "$PERF_CAPABILITY_DROPIN_BASENAME"
}

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
# NOTE ON PRECEDENCE: /etc/sysctl.conf is applied AFTER every sysctl.d drop-in by
# both `sysctl --system` and systemd-sysctl, so a stale perf_event_paranoid there
# BEATS this file. Check it if the values do not take.
#
# This is a deliberate loosening. NOT for shared or multi-tenant hosts.
# Rationale + verification: docs/development/fleet-runbook.md
kernel.perf_event_paranoid = -1
kernel.kptr_restrict = 0
EOF
}

# perf_capability_dropin_current: rc 0 iff the drop-in exists with EXACTLY the
# managed bytes (the idempotency test — a matching file means write nothing). The
# compare is an in-shell string compare, NOT `diff -q`: on a box without diffutils
# `diff` exits 127, which would report "different" on every run — so bootstrap
# would re-write the file each time AND then report it could not write it.
perf_capability_dropin_current() {
  local path want got
  path=$(perf_capability_dropin_path)
  [ -f "$path" ] && [ -r "$path" ] || return 1
  want=$(perf_capability_dropin_content)
  got=$(<"$path") || return 1
  [ "$want" = "$got" ]
}

# perf_capability_proc_read <outvar> <name>: the CURRENT kernel value read straight
# from /proc/sys/kernel/<name> into <outvar> (rc 1 + <outvar> emptied when
# unreadable). NEVER trust a `sysctl -w`/`--system` return code — a write can report
# success while the value does not take (container, read-only sysfs, a competing
# drop-in applied later), and it can report FAILURE for an unrelated entry while ours
# applied fine. Read back.
# Fully fork-free: `read` is a builtin, the directory comes back through a variable,
# and nothing here is wrapped in `$( )`. This sits in the gate's summary path, which
# may not grow a process for a diagnostic line. `read` returns non-zero at EOF on a
# file with no trailing newline yet still assigns, so emptiness — not read's rc — is
# the failure test.
perf_capability_proc_read() {
  local __pcr_out="$1" __pcr_dir="" __pcr_v=""
  perf_capability_proc_dir_into __pcr_dir
  eval "$__pcr_out="
  [ -r "$__pcr_dir/$2" ] || return 1
  read -r __pcr_v <"$__pcr_dir/$2" 2>/dev/null
  __pcr_v="${__pcr_v%%[[:space:]]*}"
  [ -n "$__pcr_v" ] || return 1
  eval "$__pcr_out=\$__pcr_v"
}

# perf_capability_proc_value <name>: the stdout form of the read above, for CLI and
# bootstrap use (NOT the gate path — reading it costs the caller a `$( )`).
perf_capability_proc_value() {
  local __pcv_v
  perf_capability_proc_read __pcv_v "$1" || return 1
  printf '%s' "$__pcv_v"
}

# perf_capability_is_int <value>: rc 0 iff <value> is a plain optionally-negative
# integer NARROW ENOUGH for shell arithmetic. Both halves are load-bearing: `[ 1abc
# -ge 1 ]` and `[ 99999999999999999999999 -ge 1 ]` do NOT compare — each prints
# "integer expression expected" to stderr and returns 2 (neither true NOR false), so
# a malformed or oversized value would fall past BOTH a `>= 1` and a `<= 0` test and
# be reported as good (a WRONG capability claim) while leaking an error line into the
# gate's output. Validate the shape here instead of trusting the read.
perf_capability_is_int() {
  local body="${1#-}"
  [ -n "$body" ] || return 1
  case "$body" in *[!0-9]*) return 1 ;; esac
  [ "${#body}" -le 10 ]
}

# perf_capability_token_into <outvar>: the FREE capability read, and THE function the
# gate's accelerators line calls. Free is a hard contract, enforced by
# test_agent_gate_summary.sh case 9f-free: pure /proc through shell builtins — no
# `perf` exec, no external process of ANY kind, and no command substitution anywhere
# in the chain (which is why the answer comes back through <outvar> rather than
# stdout; every `$( )` is a forked subshell, and the gate emits this line on every
# summary).
#   ok               unprivileged per-CPU profiling AND kernel symbols available
#   paranoid-<N>     perf_event_paranoid = N >= 1 -> CPU-wide `perf stat -C` denied
#   kptr-restricted  paranoid is fine but kptr_restrict != 0 -> no kernel symbols
#   absent           the /proc controls are not present (container, non-Linux)
#   unknown          present but unparseable (never guess a capability)
# A seam exported without the marker cannot steer this read (the seams are inert
# there), but it IS reported once on stderr so a stray export is never silent.
perf_capability_token_into() {
  local __pct_out="$1" __pct_p="" __pct_k=""
  if ! perf_capability_test_mode && perf_capability_seam_set; then
    printf 'perf-capability: ignoring CQLITE_PERF_PROC_DIR/CQLITE_PERF_SYSCTL_DIR — TEST-ONLY seams, inert without CQLITE_PERF_TEST_MODE=1; reading %s\n' \
      "$PERF_CAPABILITY_PROC_DIR_DEFAULT" >&2
  fi
  if ! perf_capability_proc_read __pct_p perf_event_paranoid \
     || ! perf_capability_proc_read __pct_k kptr_restrict; then
    eval "$__pct_out=absent"; return 0
  fi
  if ! perf_capability_is_int "$__pct_p" || ! perf_capability_is_int "$__pct_k"; then
    eval "$__pct_out=unknown"; return 0
  fi
  if [ "$__pct_p" -ge 1 ]; then eval "$__pct_out=\"paranoid-\$__pct_p\""; return 0; fi
  if [ "$__pct_k" -ne 0 ]; then eval "$__pct_out=kptr-restricted"; return 0; fi
  eval "$__pct_out=ok"
}

# perf_capability_token: the stdout form, for the `--token` CLI and bootstrap. NOT the
# gate path — reading stdout costs the caller a `$( )` fork, which is precisely what
# the `_into` core above exists to avoid.
perf_capability_token() {
  local __ptk_v
  perf_capability_token_into __ptk_v
  printf '%s' "$__ptk_v"
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
  # Event-name matching must accept a QUALIFIED cycle event: on a hybrid-PMU CPU
  # (Intel 12th-gen+ P/E cores) perf emits one row per PMU named `cpu_core/cycles/`
  # and `cpu_atom/cycles/`, commonly with `<not supported>` on the sibling that did
  # not run — so a parser keyed on a literal leading `cycles` reports `no-cycles-row`
  # on a perfectly good collection. Normalise the event field (drop the PMU prefix, a
  # trailing `/`, and any `:u`/`:k` modifier) and take the FIRST row carrying a
  # positive numeric count; keep the first matching row's raw field as the fallback so
  # the `<not supported>` / zero diagnostics below still fire when none is positive.
  count=$(printf '%s\n' "$out" | awk -F, '
    {
      ev = tolower($3)
      sub(/\/$/, "", ev)
      sub(/^.*\//, "", ev)
      sub(/:.*$/, "", ev)
      if (ev != "cycles") next
      if (fallback == "") fallback = $1
      if ($1 ~ /^[0-9]+$/ && $1 + 0 > 0) { print $1; found = 1; exit }
    }
    END { if (!found && fallback != "") print fallback }
  ')
  case "$count" in
    '') printf 'no-cycles-row: %s' "$(printf '%s' "$out" | tr '\n' ';' | cut -c1-160)"; return 1 ;;
    *'not supported'*|*'not counted'*) printf 'counter-%s' "$(printf '%s' "$count" | tr -d '<>' | tr ' ' '-')"; return 1 ;;
  esac
  # Shape-validate BEFORE any arithmetic: `[ <malformed> -le 0 ]` returns 2, which is
  # FALSE, so an unvalidated operand would fall straight through to the VERIFIED
  # return while leaking "integer expression expected" onto stderr.
  perf_capability_is_int "$count" || { printf 'unparseable-count=%s' "$count"; return 1; }
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
