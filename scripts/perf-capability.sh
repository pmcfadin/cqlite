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
#   CQLITE_PERF_SYSCTL_EXTRA_DIRS  colon-separated stand-ins for the LOWER-precedence
#                               search-path entries (/run/sysctl.d, /usr/lib/sysctl.d, …
#                               and a `sysctl.conf` file), in descending precedence;
#                               optional, test mode only
#   CQLITE_PERF_TEST_PRIV_DIR   absolute dir holding the suite's sudo/sysctl shims
#
# TEST MODE HAS NO FALLBACK (issue #3249 review R4-3). Under the marker BOTH path
# seams are MANDATORY and must be absolute, non-production directories. The earlier
# shape — marker set, seam unset, fall back to the real directory — meant test mode
# could pass the env guard (sudo/sysctl absent, or present as declared shims) and a
# subsequent root `--yes` run would then invoke a bare `tee` against the REAL
# /etc/sysctl.d, mutating the host from a test run. "Hermetic" cannot be a claim that
# depends on a variable being set: it is enforced here, fail-closed and loudly.

# FAIL-OPEN AUDIT — every path that can reach a POSITIVE verdict, and what validates it
# (issue #3249 review round 4; four findings in this file were all one defect class:
# "identity/state unknown => assume the good case"). Keep this list closed: a new
# positive-verdict path SHALL be added here with its validator, or it is a regression.
#   token = ok                  both /proc values READ (non-empty) from the resolved dir
#                               AND both `perf_capability_is_int`-validated AND
#                               paranoid <= 0 AND kptr == 0. Whitespace is trimmed, never
#                               TRUNCATED — `0 1` stays malformed -> `unknown`.
#   verify -> cycles=<n>, rc 0  `perf` present, collection rc 0, a cycles row whose count
#                               is a positive integer by BOTH awk's `^[0-9]+$` and
#                               `is_int` + `-le 0`. `<not supported>`/`<not counted>`/
#                               empty/oversized/malformed all return 1.
#   state = self-unprivileged   `perf_capability_self_uid_into` succeeded (an `id -u` that
#                               EXISTS, exits 0, prints a validated non-negative int) AND
#                               that uid != 0. An unusable `id -u` => identity-unknown, rc 1.
#   state = dropped:setpriv     numeric uid+gid, both validated ints AND both > 0.
#   state = dropped:runuser     the same numerics PLUS a `SUDO_USER` the passwd database
#                               confirms IS that non-zero uid/gid, and whose characters are
#                               safe for the caller's word-split.
#   state = dropped:sudo        numeric uid only (sudo's `#<uid>` form) — no name trusted.
#   env_guard rc 0              production: NO test seam set (paths are hardcoded literals).
#                               test mode: BOTH seams present and their CANONICAL
#                               destinations (`.`, `..` and symlinked ancestors resolved)
#                               absolute and outside the production dirs and /etc /proc
#                               /sys; plus every reachable sudo/sysctl resolving inside an
#                               absolute declared shim dir.
#   dropin_path rc 0            in test mode, the seam RESOLVES inside its sandbox (R5-1) —
#                               re-checked independently of the guard, because this is the
#                               last thing between the canonical bytes and a root `tee`.
#   dropin_current rc 0         a BYTE-exact compare (trailing newlines included) against
#                               the generated canonical content, from a read that reached
#                               EOF — a NUL-delimited read is NOT current, whatever the
#                               bytes before the NUL are (R5-3).
# KNOWN RESIDUALS, deliberately not papered over:
#   * "dropped:<mech>" asserts the mechanism was INVOKED; it cannot prove the kernel
#     changed uid. Harmless by construction: the caller's verdict is `token = ok` AND the
#     functional pass, and a box whose /proc says ok IS profileable by an unprivileged
#     process, so a mislabelled drop cannot manufacture a capability that is absent.
#   * the READ-side seam check is textual (fork-free, gate contract): it rejects `.`/`..`
#     components and a symlinked seam, but a symlinked ANCESTOR could still steer a
#     test-mode /proc STAND-IN read. Bounded and harmless: nothing on that path writes,
#     and a wrong read yields `absent`/`unknown`, never a fabricated capability. Every
#     WRITE-side check canonicalizes instead (R5-1).
#
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

# TWO VALIDATORS, SPLIT BY WHAT THE CALLER DOES (issue #3249 review R5-1). Textual
# validation of a path is the wrong tool for a guard that steers a privileged write:
# each round closed one more SPELLING of "somewhere else" (raw production path, then a
# symlinked seam, then `..`). So the two callers get different validators:
#   READ side  (the gate's emit-time token chain) -> perf_capability_test_dir_valid:
#              pure builtins, ZERO forks, because that path is contractually free. It
#              never writes anything, so a mis-accepted seam there can only mis-READ a
#              stand-in /proc, which the token then reports as `absent`/`unknown`.
#   WRITE side (env guard + the drop-in path a root `tee` is pointed at) ->
#              perf_capability_test_dir_resolved_valid: CANONICALIZES the whole path
#              (resolving `.`, `..` AND symlinked ancestors) and validates the RESOLVED
#              destination. A fork costs nothing there, and the destination — not its
#              spelling — is what gets written.
#
# perf_capability_test_dir_valid <value> <production-default>: rc 0 iff <value> is a
# usable NON-PRODUCTION stand-in — non-empty, ABSOLUTE, free of `.`/`..` components (a
# path whose spelling is not its destination cannot be validated textually at all), and
# neither the production directory itself nor a path under it, nor anywhere under the
# host's own configuration surfaces (/proc, /sys, /etc), nor itself a symlink
# (`ln -s /etc/sysctl.d /tmp/x` passes every other textual test). An unset or
# production-shaped seam is NOT "use the real thing": in test mode it is a refusal
# (R4-3). Builtins only — this is reached from the gate's fork-free token path.
perf_capability_test_dir_valid() {
  [ -n "${1:-}" ] || return 1
  case "$1" in /*) ;; *) return 1 ;; esac
  case "/$1/" in */../*|*/./*) return 1 ;; esac
  case "$1" in
    "${2:-/dev/null/never}"|"${2:-/dev/null/never}"/*) return 1 ;;
    /proc|/proc/*|/sys|/sys/*|/etc|/etc/*) return 1 ;;
  esac
  [ ! -L "$1" ]
}

# perf_capability_test_dir_resolved_valid <value> <production-default>: the WRITE-side
# validator. It canonicalizes <value> and requires the RESOLVED path to satisfy the same
# non-production predicate, so `/tmp/../etc/sysctl.d` and a symlinked ANCESTOR
# (`ln -s /etc /tmp/a`, seam `/tmp/a/sysctl.d`) are refused by their DESTINATION rather
# than by a new textual special case. `cd -P` + `pwd -P` is the canonicalizer: one
# subshell, no external binary, and correct on bash 3.2 (no `realpath`, no `readlink -f`,
# both of which are absent or non-GNU on some supported hosts). A path that cannot be
# entered at all resolves to nothing and is refused — fail-closed, and correct for a
# write target, which must exist to be written into.
perf_capability_test_dir_resolved_valid() {
  local __ptv_real=''
  perf_capability_test_dir_valid "$1" "$2" || return 1
  __ptv_real=$(cd -P -- "$1" 2>/dev/null && pwd -P) || return 1
  [ -n "$__ptv_real" ] || return 1
  perf_capability_test_dir_valid "$__ptv_real" "$2"
}

# perf_capability_test_seams_ok: rc 0 iff test mode has BOTH mandatory seams pointing
# at explicit non-production directories, JUDGED BY THEIR CANONICAL DESTINATION (R5-1).
# This is the gate on every privileged action below, so it takes the strict validator:
# refusing here is what stops a root `--yes` test run from resolving a seam back into
# the production directory and overwriting the host's own drop-in. Refuses loudly and
# names the offending seam, because the failure it prevents is silent otherwise.
perf_capability_test_seams_ok() {
  local ok=0
  if ! perf_capability_test_dir_resolved_valid "${CQLITE_PERF_PROC_DIR:-}" "$PERF_CAPABILITY_PROC_DIR_DEFAULT"; then
    printf 'perf-capability: REFUSING: CQLITE_PERF_TEST_MODE=1 requires an explicit NON-PRODUCTION CQLITE_PERF_PROC_DIR (absolute, and RESOLVING — . / .. / symlinked ancestors and all — outside %s and outside /proc /sys /etc); got %s. Test mode NEVER falls back to the real directory.\n' \
      "$PERF_CAPABILITY_PROC_DIR_DEFAULT" "'${CQLITE_PERF_PROC_DIR:-<unset>}'" >&2
    ok=1
  fi
  if ! perf_capability_test_dir_resolved_valid "${CQLITE_PERF_SYSCTL_DIR:-}" "$PERF_CAPABILITY_SYSCTL_DIR_DEFAULT"; then
    printf 'perf-capability: REFUSING: CQLITE_PERF_TEST_MODE=1 requires an explicit NON-PRODUCTION CQLITE_PERF_SYSCTL_DIR (absolute, and RESOLVING — . / .. / symlinked ancestors and all — outside %s and outside /proc /sys /etc); got %s. Test mode NEVER falls back to the real directory.\n' \
      "$PERF_CAPABILITY_SYSCTL_DIR_DEFAULT" "'${CQLITE_PERF_SYSCTL_DIR:-<unset>}'" >&2
    ok=1
  fi
  [ "$ok" = 0 ]
}

# perf_capability_env_guard: rc 0 iff this process may act on the perf capability
# path at all. Every refusal is LOUD on stderr and FAILS CLOSED (the caller does
# nothing privileged and claims no verdict):
#   * a seam set WITHOUT the marker — the seams are inert there, and a caller that
#     was handed one is misconfigured, so nothing privileged may proceed;
#   * the marker set WITHOUT both non-production path seams — test mode has no
#     fallback (R4-3): allowing one would let a root `--yes` test run `tee` the REAL
#     /etc/sysctl.d, which is exactly the host mutation the marker promises cannot
#     happen. Checked FIRST, before the tool checks, because it is the more
#     fundamental precondition: a test run with no sandbox has nowhere safe to act;
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
  perf_capability_test_seams_ok || return 1
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
#   In TEST MODE the seam is MANDATORY and validated (R4-3): an unset or
#   production-shaped value is rc 1 with an empty answer, never a silent fallback to
#   the real /proc or the real /etc/sysctl.d. Every caller propagates that rc, so an
#   unsandboxed test run reads nothing and writes nothing.
perf_capability_proc_dir_into() {
  eval "$1="
  if perf_capability_test_mode; then
    perf_capability_test_dir_valid "${CQLITE_PERF_PROC_DIR:-}" "$PERF_CAPABILITY_PROC_DIR_DEFAULT" || {
      printf 'perf-capability: REFUSING to read /proc: CQLITE_PERF_TEST_MODE=1 with no valid non-production CQLITE_PERF_PROC_DIR (got %s) — test mode never falls back to %s.\n' \
        "'${CQLITE_PERF_PROC_DIR:-<unset>}'" "$PERF_CAPABILITY_PROC_DIR_DEFAULT" >&2
      return 1
    }
    eval "$1=\$CQLITE_PERF_PROC_DIR"
    return 0
  fi
  eval "$1=\$PERF_CAPABILITY_PROC_DIR_DEFAULT"
}
perf_capability_proc_dir() {
  local __pcd_v
  perf_capability_proc_dir_into __pcd_v || return 1
  printf '%s' "$__pcd_v"
}
perf_capability_sysctl_dir() {
  if perf_capability_test_mode; then
    perf_capability_test_dir_valid "${CQLITE_PERF_SYSCTL_DIR:-}" "$PERF_CAPABILITY_SYSCTL_DIR_DEFAULT" || {
      printf 'perf-capability: REFUSING to resolve a sysctl.d path: CQLITE_PERF_TEST_MODE=1 with no valid non-production CQLITE_PERF_SYSCTL_DIR (got %s) — test mode never falls back to %s.\n' \
        "'${CQLITE_PERF_SYSCTL_DIR:-<unset>}'" "$PERF_CAPABILITY_SYSCTL_DIR_DEFAULT" >&2
      return 1
    }
    printf '%s' "$CQLITE_PERF_SYSCTL_DIR"
    return 0
  fi
  printf '%s' "$PERF_CAPABILITY_SYSCTL_DIR_DEFAULT"
}
# perf_capability_dropin_path: the path a root `tee` is pointed at, so this is the WRITE
# side and it takes the STRICT validator (R5-1) — the seam's CANONICAL destination is
# checked, not its spelling. Independent of the env guard on purpose: the guard is the
# gate, this is the last line before the bytes land, and a write target may never be
# named from a path that resolves into production.
perf_capability_dropin_path() {
  local __pdi_d
  __pdi_d=$(perf_capability_sysctl_dir) || return 1
  if perf_capability_test_mode \
     && ! perf_capability_test_dir_resolved_valid "$__pdi_d" "$PERF_CAPABILITY_SYSCTL_DIR_DEFAULT"; then
    printf 'perf-capability: REFUSING to name a write target: CQLITE_PERF_SYSCTL_DIR=%s RESOLVES (through . / .. / a symlinked ancestor) outside its sandbox or is unenterable — the canonical DESTINATION is what a root tee writes, not the spelling.\n' \
      "'$__pdi_d'" >&2
    return 1
  fi
  printf '%s/%s' "$__pdi_d" "$PERF_CAPABILITY_DROPIN_BASENAME"
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
# THE "99-" PREFIX IS LOAD-BEARING — DO NOT RENAME THIS FILE. sysctl.d drop-ins are
# applied in lexicographic order of BASENAME and the LAST assignment wins. Stock Ubuntu
# ships /etc/sysctl.d/10-kernel-hardening.conf containing `kernel.kptr_restrict = 1`,
# so this file only wins because "99-cqlite-perf.conf" sorts after "10-...". Renaming it
# to cqlite-perf.conf or any lower number silently hands kptr_restrict back to the
# hardening drop-in at the next boot — the "it silently reverts" mystery in three
# separate measurement reports.
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
#
# TRAILING NEWLINES ARE PART OF THE BYTES (issue #3249 review R4-4). `$( )` strips
# EVERY trailing newline from its output, so comparing two command substitutions made
# a file missing its final newline — or carrying extra blank lines at the end —
# compare EQUAL to the canonical content: "byte-exact" was a false claim, and such a
# file was never rewritten. The file side is now read with `read -r -d ''`, which
# consumes the whole file verbatim (builtin, so no `cat`/`diff` dependency), and the
# canonical side carries an in-substitution sentinel so its own final newline survives
# the stripping.
#
# A NUL BYTE IS THE THIRD SPELLING OF "NOT EXACT" (issue #3249 review R5-3). `read -d ''`
# stops at a NUL and returns SUCCESS, leaving `got` holding only the bytes BEFORE it — so
# canonical content followed by a NUL and ARBITRARY trailing bytes compared EQUAL and was
# judged current. Read's rc is therefore load-bearing: rc 0 means a NUL delimiter was
# consumed, i.e. the file is not our text drop-in AND the rest of it was never even seen,
# so it is NOT current; only an rc != 0 (EOF reached, whole file in `got`) may be compared.
perf_capability_dropin_current() {
  local path want got=''
  path=$(perf_capability_dropin_path) || return 1
  [ -f "$path" ] && [ -r "$path" ] || return 1
  want=$(perf_capability_dropin_content; printf 'X')
  if IFS= read -r -d '' got <"$path"; then
    return 1
  fi
  [ "$want" = "${got}X" ]
}

# perf_capability_sysctl_search_path: the COMPLETE set of locations `sysctl --system`
# (procps-ng) and systemd-sysctl load, one per line, in DESCENDING NAME-MASKING
# PRECEDENCE — the order both tools scan (sysctl(8) SYSTEM FILE PRECEDENCE, sysctl.d(5)
# CONFIGURATION DIRECTORIES AND PRECEDENCE):
#   /etc/sysctl.d  /run/sysctl.d  /usr/local/lib/sysctl.d  /usr/lib/sysctl.d
#   /lib/sysctl.d  and finally the FILE /etc/sysctl.conf
# TWO INDEPENDENT RULES decide who wins, and the scan below implements both:
#   MASKING  "once a file of a given filename is loaded, any file of the same name in
#            subsequent directories is ignored" — so /etc/sysctl.d/50-x.conf REPLACES
#            /usr/lib/sysctl.d/50-x.conf outright, and reporting the masked one would
#            name a file that is not in effect.
#   ORDERING the surviving files are applied in lexicographic BASENAME order regardless
#            of which directory they came from; the LAST assignment wins.
#   /etc/sysctl.conf is applied AFTER every drop-in, so it wins on grounds that have
#   nothing to do with its name — it gets its own verdict rather than a sort comparison.
#
# WHY THE WHOLE PATH (issue #3249 review R5-4). Scanning only /etc/sysctl.d meant a
# later-sorting file in /run/sysctl.d or /usr/lib/sysctl.d could override our drop-in
# while bootstrap reported NO competitor — recreating the exact "it silently reverts and
# nobody knows why" mystery this diagnostic exists to end.
#
# In TEST MODE the path is the sandbox seam plus the optional colon-separated
# CQLITE_PERF_SYSCTL_EXTRA_DIRS (lower-precedence stand-ins, in the same descending
# order, each validated non-production): the real /run and /usr/lib are never read, so a
# case's verdict can never depend on the host's own drop-ins.
perf_capability_sysctl_search_path() {
  local __psp_d __psp_e
  if perf_capability_test_mode; then
    __psp_d=$(perf_capability_sysctl_dir) || return 1
    printf '%s\n' "$__psp_d"
    local -a __psp_extra=()
    # `read -a` splits on IFS WITHOUT globbing (an unquoted `for x in $var` would glob).
    IFS=':' read -r -a __psp_extra <<<"${CQLITE_PERF_SYSCTL_EXTRA_DIRS:-}"
    for __psp_e in ${__psp_extra[@]+"${__psp_extra[@]}"}; do
      [ -n "$__psp_e" ] || continue
      perf_capability_test_dir_valid "$__psp_e" "$PERF_CAPABILITY_SYSCTL_DIR_DEFAULT" || {
        printf 'perf-capability: REFUSING: CQLITE_PERF_SYSCTL_EXTRA_DIRS entry %s is not an absolute NON-PRODUCTION path — a test-mode scan may never read the host'"'"'s real sysctl.d directories.\n' \
          "'$__psp_e'" >&2
        return 1
      }
      printf '%s\n' "$__psp_e"
    done
    return 0
  fi
  printf '%s\n' /etc/sysctl.d /run/sysctl.d /usr/local/lib/sysctl.d /usr/lib/sysctl.d \
                /lib/sysctl.d /etc/sysctl.conf
}

# perf_capability_file_sets_controls <path>: rc 0 iff the file ASSIGNS either control.
# Both spellings sysctl accepts are matched (`kernel.x` and `kernel/x`, sysctl.d(5)) plus
# the optional leading `-` ignore-failure prefix; a commented-out line assigns nothing.
perf_capability_file_sets_controls() {
  grep -Eq '^[[:space:]]*-?kernel[./](perf_event_paranoid|kptr_restrict)[[:space:]]*=' "$1" 2>/dev/null
}

# perf_capability_competing_files: every OTHER file ANYWHERE ON THE SEARCH PATH ABOVE
# that also sets kernel.perf_event_paranoid or kernel.kptr_restrict AND is actually in
# effect (masked files are skipped), one per line as
#   <override|earlier|last> <path>
# `override` = its basename sorts AFTER ours, so it is applied LAST and IT WINS;
# `earlier` = ours wins; `last` = /etc/sysctl.conf, applied after every drop-in, so it
# wins regardless of name.
#
# WHY THIS EXISTS. Three separate reports (ws0-3217, ws3-3029, the 2026-07-27 Cassandra
# baseline) recorded that a hand-set perf_event_paranoid/kptr_restrict "silently
# reverts" and none identified the cause. The cause is a NAMED FILE: stock Ubuntu ships
# /etc/sysctl.d/10-kernel-hardening.conf with `kernel.kptr_restrict = 1`, re-asserted at
# every boot and by every `sysctl --system`. "It silently reverts" is unactionable;
# "10-kernel-hardening.conf sets kptr_restrict = 1 and sorts BEFORE ours, so ours wins"
# is a diagnosis. Ordering is lexicographic by BASENAME in BYTE order, which is what
# systemd-sysctl/`sysctl --system` use — and `[ "$a" \> "$b" ]` is the right operator
# for it: the `[` builtin compares with strcmp (verified: byte order even under a UTF-8
# LC_ALL), whereas `[[ > ]]` would switch to locale collation and could mis-rank names
# whose only difference is punctuation.
perf_capability_competing_files() {
  local base f name entry seen paths lf='
'
  base="$PERF_CAPABILITY_DROPIN_BASENAME"
  paths=$(perf_capability_sysctl_search_path) || return 1
  seen="$lf"
  # An entry that does not exist genuinely holds no competitor (skipped, no output — most
  # boxes have no /run/sysctl.d at all). One that exists but cannot be READ is an UNKNOWN
  # and returns rc 1, so the caller reports a failed scan instead of the reassuring "no
  # competing file" — this diagnostic exists to replace an unknown with a named file, so
  # it may not answer an unknown with good news.
  while IFS= read -r entry; do
    [ -n "$entry" ] || continue
    if [ "${entry##*/}" = sysctl.conf ]; then
      [ -e "$entry" ] || continue
      [ -f "$entry" ] && [ -r "$entry" ] || return 1
      perf_capability_file_sets_controls "$entry" && printf 'last %s\n' "$entry"
      continue
    fi
    [ -e "$entry" ] || continue
    [ -d "$entry" ] && [ -r "$entry" ] || return 1
    for f in "$entry"/*.conf; do
      [ -f "$f" ] && [ -r "$f" ] || continue
      name="${f##*/}"
      [ "$name" = "$base" ] && continue
      # MASKING (see the precedence rules above): a higher-precedence directory already
      # supplied this basename, so sysctl ignores THIS file entirely. Recorded BEFORE the
      # content test, because a same-named file that sets nothing still masks one that does.
      case "$seen" in *"$lf$name$lf"*) continue ;; esac
      seen="$seen$name$lf"
      perf_capability_file_sets_controls "$f" || continue
      if [ "$name" \> "$base" ]; then
        printf 'override %s\n' "$f"
      else
        printf 'earlier %s\n' "$f"
      fi
    done
  done <<EOF
$paths
EOF
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
# the failure test. It also propagates the test-mode sandbox refusal (R4-3): with no
# valid seam there is no directory to read, and that is rc 1, never the real /proc.
#
# WHITESPACE IS TRIMMED, NEVER TRUNCATED AT (fail-open audit, R4 round). The earlier
# `${v%%[[:space:]]*}` cut the value at its FIRST space, so a malformed `0 1` became a
# perfectly capable-looking `0` — an unknown resolving to the good case, in the one
# function the gate's `perf=` token is computed from. Surrounding whitespace (including a
# CRLF's `\r`) is stripped; anything interior is left in place so `is_int` rejects it and
# the token becomes `unknown`. `IFS=` makes that independent of the caller's IFS, and both
# trims are parameter expansions — no fork on the gate's emit path.
perf_capability_proc_read() {
  local __pcr_out="$1" __pcr_dir="" __pcr_v=""
  eval "$__pcr_out="
  perf_capability_proc_dir_into __pcr_dir || return 1
  [ -r "$__pcr_dir/$2" ] || return 1
  IFS= read -r __pcr_v <"$__pcr_dir/$2" 2>/dev/null
  __pcr_v="${__pcr_v#"${__pcr_v%%[![:space:]]*}"}"
  __pcr_v="${__pcr_v%"${__pcr_v##*[![:space:]]}"}"
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

# ---- WHOSE capability? the privilege dimension (issue #3249 review) -----------
# perf_event_paranoid restricts UNPRIVILEGED users; ROOT BYPASSES IT ENTIRELY. So
# `perf stat -C 0 -e cycles` run by root SUCCEEDS on a paranoid=4 box on which every
# unprivileged agent process still gets EACCES — and `sudo bash
# scripts/bootstrap-agent-machine.sh` is a completely normal provisioning invocation
# (arguably the most likely one, since installing /etc/sysctl.d/99-cqlite-perf.conf
# needs root). A root-run functional check reported as "perf capability verified" is
# therefore a FALSE verification of an unprofileable box: precisely the failure mode
# the functional check exists to remove, reintroduced through the privilege dimension.
#
# The property actually under test is "an UNPRIVILEGED process can collect CPU-WIDE
# cycles", and a root-run probe cannot demonstrate it. So the probe DROPS PRIVILEGE
# when it can, and when it cannot it says so — the caller then subordinates the
# functional result to the /proc token, which is identity-independent.
#
# perf_capability_self_uid_into <outvar>: THIS process's uid, and rc 0 ONLY when it is
# genuinely known — `id -u` must EXIST, exit 0, and print a validated non-negative
# integer. rc 1 (with <outvar> emptied) means "identity unknown", which is NOT the same
# as "unprivileged" (issue #3249 review R4-1).
#
# The previous shape, `$(id -u 2>/dev/null || echo 1000)`, FAILED OPEN: a missing or
# broken `id` made a ROOT process look unprivileged, so its root perf run was accepted
# as unprivileged evidence and printed a false VERIFIED — the very R3-1 defect, through
# the detector written to prevent it. An unknown identity can never resolve to the
# reassuring case.
perf_capability_self_uid_into() {
  local __psu_v=''
  eval "$1="
  command -v id >/dev/null 2>&1 || return 1
  __psu_v=$(id -u 2>/dev/null) || return 1
  perf_capability_is_int "$__psu_v" || return 1
  case "$__psu_v" in -*) return 1 ;; esac   # a negative uid is not an identity
  eval "$1=\$__psu_v"
}

# perf_capability_name_is_uid <name> <uid> <gid>: rc 0 iff <name> is a real account in
# the passwd database whose uid AND gid equal the supplied (already validated) numerics
# and whose uid is non-zero.
#
# WHY (issue #3249 review R4-2). `runuser -u <name>` / `sudo -u <name>` drop to whatever
# the NAME resolves to, not to the numeric ids we validated. SUDO_USER and SUDO_UID are
# independent environment strings: `SUDO_UID=1000 SUDO_USER=root` (stale, hand-set, or
# inconsistent) would run the probe AS ROOT while the code reported a successful
# privilege drop — a false VERIFIED again. A name is therefore only usable once the
# passwd database confirms it IS the validated uid/gid.
# The shape check is equally load-bearing: the prefix is word-split by the caller, so a
# name containing whitespace or glob characters could inject extra argv tokens.
perf_capability_name_is_uid() {
  local __pni_n="${1:-}" __pni_u="${2:-}" __pni_g="${3:-}" __pni_ru __pni_rg
  [ -n "$__pni_n" ] || return 1
  case "$__pni_n" in -*|*[!A-Za-z0-9._-]*) return 1 ;; esac
  command -v id >/dev/null 2>&1 || return 1
  __pni_ru=$(id -u "$__pni_n" 2>/dev/null) || return 1
  __pni_rg=$(id -g "$__pni_n" 2>/dev/null) || return 1
  perf_capability_is_int "$__pni_ru" && perf_capability_is_int "$__pni_rg" || return 1
  [ "$__pni_ru" = "$__pni_u" ] && [ "$__pni_rg" = "$__pni_g" ] && [ "$__pni_ru" -gt 0 ]
}

# perf_capability_drop_target_into <outvar_uid> <outvar_gid> <outvar_name>:
# resolve an UNPRIVILEGED identity to probe as. Never invented — a box that offers
# none reports none (rc 1) rather than guessing a uid:
#   1. SUDO_UID/SUDO_GID/SUDO_USER — the identity that actually invoked `sudo
#      bootstrap`, i.e. the very account whose profiling capability is in question.
#      Strongest available evidence.
#   2. `nobody`, resolved from the passwd database (never a hardcoded 65534: a box
#      without that account must report "no target", not probe a uid nobody owns).
# THE NUMERIC IDS ARE THE TARGET; the NAME is optional and only ever set when the
# passwd database confirms it resolves to exactly those non-zero ids (R4-2). An
# unverifiable SUDO_USER is dropped, not trusted: the numeric-only mechanisms
# (setpriv, `sudo -u '#<uid>'`) still work, and a name-requiring mechanism correctly
# reports that it has nothing safe to use.
perf_capability_drop_target_into() {
  local __pdt_u='' __pdt_g='' __pdt_n=''
  if perf_capability_is_int "${SUDO_UID:-}" && perf_capability_is_int "${SUDO_GID:-}" \
     && [ "${SUDO_UID:-0}" -gt 0 ] && [ "${SUDO_GID:-0}" -gt 0 ]; then
    __pdt_u="${SUDO_UID}"; __pdt_g="${SUDO_GID}"
    if perf_capability_name_is_uid "${SUDO_USER:-}" "$__pdt_u" "$__pdt_g"; then
      __pdt_n="${SUDO_USER}"
    else
      __pdt_n=''
    fi
  else
    __pdt_u=$(id -u nobody 2>/dev/null) || __pdt_u=''
    __pdt_g=$(id -g nobody 2>/dev/null) || __pdt_g=''
    # gid > 0 as well, matching the SUDO branch above: a target resolving to gid 0 is a
    # partial drop, and "partially dropped" is not a state this code is allowed to
    # report as an unprivileged probe (fail-open audit).
    if perf_capability_is_int "$__pdt_u" && perf_capability_is_int "$__pdt_g" \
       && [ "${__pdt_u:-0}" -gt 0 ] && [ "${__pdt_g:-0}" -gt 0 ]; then
      __pdt_n=nobody
    else
      __pdt_u=''; __pdt_g=''; __pdt_n=''
    fi
  fi
  eval "$1=\$__pdt_u"; eval "$2=\$__pdt_g"; eval "$3=\$__pdt_n"
  [ -n "$__pdt_u" ]
}

# perf_capability_drop_prefix_into <outvar_prefix> <outvar_state>: the command prefix
# that makes the probe UNPRIVILEGED, plus the honest state label for it. rc 0 iff the
# result IS evidence about an unprivileged process.
#   self-unprivileged             we are not root: the probe already measures the
#                                 right thing, prefix empty (rc 0)
#   dropped:<mech>:<identity>     root, and privilege is dropped for the probe (rc 0)
#   identity-unknown              `id -u` is unusable, so WHO runs the probe is unknown
#                                 and the result is not evidence either way (rc 1)
#   root-no-unprivileged-target   root, and no unprivileged identity is resolvable (rc 1)
#   root-no-drop-mechanism        root, target known, but no usable setpriv/runuser/sudo (rc 1)
# Mechanism order: `setpriv` (util-linux; a plain setresuid, no PAM, no session, no
# shell), then `runuser`, then `sudo -n -u`. Two of the three take the VALIDATED NUMERIC
# ids and never a name — `setpriv --reuid/--regid`, and `sudo -u '#<uid>'` (sudo's
# documented numeric-uid form) — so no name has to be trusted (R4-2). `runuser` accepts
# only a name and is therefore used ONLY with a passwd-confirmed one.
#   The `#<uid>` token is safe through the caller's word-split: `#` only starts a comment
#   during tokenisation of the source line, and this value arrives by EXPANSION
#   afterwards, so it is passed to sudo as a literal argument.
# The prefix is composed ONLY of literal tokens plus a validated numeric uid/gid or a
# passwd-confirmed name, so the caller may word-split it. A non-zero rc is NOT an error
# to fail on: it is the caller's cue to label the functional result as what it is — not
# evidence about an unprivileged process — and to let the /proc token be the authority.
perf_capability_drop_prefix_into() {
  local __pdp_u='' __pdp_g='' __pdp_n='' __pdp_self=''
  eval "$1="
  if ! perf_capability_self_uid_into __pdp_self; then
    eval "$2=identity-unknown"; return 1
  fi
  if [ "$__pdp_self" != 0 ]; then
    eval "$2=self-unprivileged"; return 0
  fi
  if ! perf_capability_drop_target_into __pdp_u __pdp_g __pdp_n; then
    eval "$2=root-no-unprivileged-target"; return 1
  fi
  if command -v setpriv >/dev/null 2>&1; then
    eval "$1=\"setpriv --reuid=\$__pdp_u --regid=\$__pdp_g --clear-groups\""
    eval "$2=\"dropped:setpriv:uid=\$__pdp_u\""
    return 0
  fi
  if [ -n "$__pdp_n" ] && command -v runuser >/dev/null 2>&1; then
    eval "$1=\"runuser -u \$__pdp_n --\""
    eval "$2=\"dropped:runuser:\$__pdp_n\""
    return 0
  fi
  if command -v sudo >/dev/null 2>&1; then
    eval "$1=\"sudo -n -u #\$__pdp_u --\""
    eval "$2=\"dropped:sudo:uid=\$__pdp_u\""
    return 0
  fi
  eval "$2=root-no-drop-mechanism"
  return 1
}

# perf_capability_verify [prefix-word...]: the FUNCTIONAL verification (issue #3249
# AC2). A bootstrap that silently leaves a box unprofileable is the failure mode being
# fixed, so the verdict comes from RUNNING the collection the doctrine mandates —
# `perf stat -C 0 -e cycles` — and requires BOTH exit 0 AND a non-zero cycle
# count. `perf stat` exits 0 while printing `<not supported>` / `<not counted>`
# (and a virtualised PMU can report a flat 0), so an rc-only check is exactly the
# false green this exists to prevent.
#
# Any arguments are a command prefix the collection runs under — the
# privilege-dropping prefix above. This function makes NO claim about identity: it
# runs what it is given and reports the counter. Deciding WHOSE capability was
# measured (and whether that answers the question) is the caller's job, because the
# caller is the one that owns the verdict.
#
# CSV mode (`-x,`) is parsed rather than the human table: the human renderer is
# locale-formatted (`1.234.567`) and column layout has changed across perf
# releases, while the CSV shape `<count>,<unit>,<event>,...` is stable.
# Prints a short machine-greppable reason (stdout) either way; rc 0 = verified.
perf_capability_verify() {
  command -v perf >/dev/null 2>&1 || { printf 'no-perf-binary'; return 1; }
  local bound="" out rc count
  local -a pre=()
  [ "$#" -eq 0 ] || pre=("$@")
  bound=$(command -v timeout 2>/dev/null || command -v gtimeout 2>/dev/null || true)
  # `timeout` stays OUTERMOST so the bound covers the privilege-dropping helper too.
  if [ -n "$bound" ]; then
    out=$(LC_ALL=C "$bound" 30 ${pre[@]+"${pre[@]}"} perf stat -x, -e cycles -C 0 -- sleep 0.1 2>&1); rc=$?
  else
    out=$(LC_ALL=C ${pre[@]+"${pre[@]}"} perf stat -x, -e cycles -C 0 -- sleep 0.1 2>&1); rc=$?
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
  printf 'usage: %s --token | --verify | --verify-unpriv | --drop-in | --drop-in-path\n' "${0##*/}"
  printf '  --token         free /proc capability read: ok|paranoid-<N>|kptr-restricted|absent|unknown\n'
  printf '  --verify        functional check AS THIS USER: perf stat -C 0 -e cycles (rc 0 = it worked\n'
  printf '                  for THIS identity; run as root that proves nothing about an agent process)\n'
  printf '  --verify-unpriv the same check as an UNPRIVILEGED identity (drops privilege when root);\n'
  printf '                  prints "<result> identity=<state>" — rc 0 only when the state is unprivileged\n'
  printf '  --drop-in       print the canonical /etc/sysctl.d/99-cqlite-perf.conf bytes\n'
  printf '  --drop-in-path  print where that file belongs\n'
}

perf_capability_main() {
  case "${1:-}" in
    --token)        perf_capability_token; printf '\n' ;;
    --verify)       local v rc=0; v=$(perf_capability_verify) || rc=1; printf '%s\n' "$v"; return $rc ;;
    --verify-unpriv)
      # Identity-aware form: rc 0 requires BOTH a functional pass AND that the pass
      # came from an unprivileged identity. A root run with no way to drop privilege
      # reports its result AND that the result is not evidence about an agent process.
      local pre='' state='' v rc=0 unpriv=0
      perf_capability_drop_prefix_into pre state && unpriv=1
      # shellcheck disable=SC2086  # deliberate split of our own literal prefix tokens
      v=$(perf_capability_verify $pre) || rc=1
      printf '%s identity=%s\n' "$v" "$state"
      [ "$unpriv" = 1 ] || rc=1
      return $rc ;;
    --drop-in)      perf_capability_dropin_content ;;
    # rc PROPAGATED, never masked by the trailing newline: under an unsandboxed test
    # mode the path cannot be resolved at all (R4-3), and a caller must see that as a
    # failure rather than as an empty-but-successful answer.
    --drop-in-path) perf_capability_dropin_path || return 1; printf '\n' ;;
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
