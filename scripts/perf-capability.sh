#!/usr/bin/env bash
# shellcheck shell=bash
# perf-capability.sh — the ONE place that knows how a CQLite box is made
# profileable, and how that is VERIFIED (issue #3249).
#
# WHY THIS FILE EXISTS. Agent/worker images ship `kernel.perf_event_paranoid = 4` and set
# it NOWHERE in /etc/sysctl.conf or /etc/sysctl.d — so every profiling run starts from a
# hard EACCES whose help text ("access limited") reads like a CAPABILITY verdict when it is
# a PERMISSION verdict. That has already cost two measurement cycles. The same three-line
# incantation was then copy-pasted into ad-hoc harnesses; it now lives here, is git-pinned,
# and is asserted by the gate's tooling-tests.
#
# WHY -1 AND NOT 1. perf_event_paranoid is CUMULATIVE — higher is MORE restrictive and
# each level keeps the ones below it: `>= 3` (an extra Debian/Ubuntu level) denies ALL
# unprivileged perf use, which is why the images' `4` kills even a plain `perf stat`;
# `>= 2` no kernel profiling; `>= 1` no CPU-WIDE access, which is exactly what
# `perf stat -C <cpu>` needs; `>= 0` no raw tracepoints; `-1` (almost) everything, and the
# perf mlock limit lifted too. CQLite's doctrine mandates per-CPU collection, so `1` is
# not "almost right", it is a hard denial. `kernel.kptr_restrict = 0` is a SEPARATE control
# for kernel SYMBOL resolution — without it kernel frames are unresolved addresses, a
# SILENT attribution loss rather than an error. Same rationale in the drop-in's own bytes.
#
# SECURITY POSTURE. A deliberate loosening, appropriate for DEDICATED SINGLE-TENANT
# measurement/agent boxes. Never apply it to a shared or multi-tenant host. See
# docs/development/fleet-runbook.md. BPF IS A DIFFERENT PERMISSION: a permissive
# perf_event_paranoid does NOT grant BPF map creation — bpftrace/bcc collectors still need
# sudo (#3217 finding).
#
# Sourceable AND executable. Source it ONCE (the gate does, at script scope) and call the
# functions; a per-use re-source re-reads 300+ lines for nothing. Sourcing has NO side
# effects: this file only defines `perf_capability_*` functions plus the four
# `PERF_CAPABILITY_*` constants (nothing runs, no shell options are changed, no variables
# outside those namespaces are touched). Every function is `set -u` safe.
#
# Usage when executed: `bash scripts/perf-capability.sh --help` (the modes are listed by
# perf_capability_usage below, so they are not duplicated here).
#
# TEST-ONLY ENV SEAMS — INERT UNLESS CQLITE_PERF_TEST_MODE=1 (issue #3249 review).
# The PRODUCTION paths below are HARDCODED LITERALS (/etc/sysctl.d, /proc/sys/kernel) because
# bootstrap installs the drop-in through the STAGED installer below (mktemp + atomic rename, no
# `tee <path>`): were that path env-derived, one stray
# export (say CQLITE_PERF_SYSCTL_DIR=/etc/sudoers.d) would make ROOT write an
# attacker/accident-chosen file while the real drop-in was never installed — and an unparsable
# sudoers entry can wedge `sudo` outright. Likewise an env-chosen /proc stand-in would let a
# paranoid-4 box print a FABRICATED "verified" verdict. So the seams take effect ONLY under
# the explicit marker, and the marker is itself hermetic: with it set, a REAL `sudo`/`sysctl`
# reachable on PATH is a hard refusal (the suite PATH-shims both and declares the shim
# directory), so test mode can never reach a real privileged tool.
#   CQLITE_PERF_TEST_MODE=1     the marker; without it every seam below is INERT
#   CQLITE_PERF_TEST_SANDBOX    THE SANDBOX ROOT — the one absolute directory every other
#                               seam must be provably INSIDE (test mode only)
#   CQLITE_PERF_PROC_DIR        stand-in for /proc/sys/kernel   (test mode only)
#   CQLITE_PERF_SYSCTL_DIR      stand-in for /etc/sysctl.d      (test mode only)
#   CQLITE_PERF_SYSCTL_EXTRA_DIRS  colon-separated stand-ins for the LOWER-precedence
#                               search-path entries (/run/sysctl.d, /usr/lib/sysctl.d, …
#                               and a `sysctl.conf` file), in descending precedence;
#                               optional, test mode only
#   CQLITE_PERF_TEST_PRIV_DIR   absolute dir holding the suite's sudo/sysctl shims
#
# POSITIVE CONTAINMENT, NOT A LIST OF FORBIDDEN PLACES (review R6-1/R6-2). Four rounds each
# closed ONE MORE SPELLING of "the production directory": the raw path (B3), a symlinked seam,
# `..` (R5-1), then `//etc` (R6-1 — POSIX leaves two leading slashes implementation-defined,
# `pwd -P` may PRESERVE them, and on Linux `//etc` IS `/etc`). A denylist over path spellings
# cannot be completed — `.`, `..`, symlinks, `//`, trailing slashes, bind mounts,
# `/proc/self/root/…` all name the same directory — and scattered prohibitions also let a NEW
# entry point silently miss them (R6-2). So the rule is INVERTED and there is exactly ONE: a
# seam is usable IFF it is strictly contained in the declared sandbox root. Every spelling of
# "somewhere else", including every future one, fails that single check for the same reason.
# TEST MODE HAS NO FALLBACK (R4-3). Under the marker the sandbox root and BOTH path seams are
# MANDATORY. The earlier shape — marker set, seam unset, fall back to the real directory —
# meant test mode could pass the env guard (sudo/sysctl absent, or present as declared shims)
# and a subsequent root `--yes` run would `tee` the REAL /etc/sysctl.d, mutating the host from
# a test run. "Hermetic" cannot be a claim that depends on a variable being set.

# FAIL-OPEN AUDIT — every path that can reach a POSITIVE verdict, and what validates it
# (review round 4; four findings in this file were one defect class: "identity/state unknown
# => assume the good case"). Keep this list CLOSED: a new positive-verdict path SHALL be added
# here with its validator, or it is a regression.
#   token = ok                  both /proc values READ (non-empty) from the resolved dir AND
#                               both `is_int`-validated AND paranoid <= 0 AND kptr == 0.
#                               Whitespace trimmed, never TRUNCATED — `0 1` stays malformed.
#   verify -> cycles=<n>, rc 0  `perf` present, collection rc 0, a cycles row whose count is a
#                               positive integer by BOTH awk's `^[0-9]+$` and `is_int` + `-le
#                               0`. `<not supported>`/`<not counted>`/empty/oversized/malformed
#                               all return 1.
#   state = self-unprivileged   `self_uid_into` succeeded (an `id -u` that EXISTS, exits 0 and
#                               prints a validated non-negative int) AND that uid != 0; an
#                               unusable `id -u` => identity-unknown, rc 1.
#   state = dropped:setpriv     numeric uid+gid, both validated ints AND both > 0.
#   state = dropped:runuser     the same numerics PLUS a `SUDO_USER` the passwd database
#                               confirms IS that non-zero uid/gid, with characters safe for
#                               the caller's word-split.
#   state = dropped:sudo        numeric uid only (sudo's `#<uid>` form) — no name trusted.
#   env_guard rc 0              production: NO test seam set (paths are hardcoded literals).
#                               test mode: a PROVEN sandbox root plus BOTH seams RESOLVING
#                               strictly inside it, plus every reachable sudo/sysctl inside
#                               an absolute declared shim dir.
#   dropin_path rc 0            its directory came from `sysctl_dir`, i.e. RESOLVES inside the
#                               sandbox — the single gate between the bytes and a root `tee`.
#   dropin_current rc 0         a BYTE-exact compare (trailing newlines included) against the
#                               canonical content from a read that reached EOF — a
#                               NUL-delimited read is NOT current (R5-3).
# KNOWN RESIDUALS, deliberately not papered over:
#   * "dropped:<mech>" asserts the mechanism was INVOKED; it cannot prove the kernel changed
#     uid. Harmless by construction: the caller's verdict is `token = ok` AND the functional
#     pass, and a box whose /proc says ok IS profileable unprivileged, so a mislabelled drop
#     cannot manufacture a capability that is absent.
#   * the READ-side containment check is SYNTACTIC (fork-free, gate contract) while every
#     write / host-config read canonicalizes — SAME predicate, different input form. The read
#     side judges the spelling, so a symlinked ancestor INSIDE the sandbox could still steer a
#     test-mode /proc STAND-IN read. Bounded by that path's whole contract: the seams are
#     honoured only under the test marker, which is never set in production, and nothing there
#     writes — so the worst case is a read of a caller-chosen file reported as
#     `absent`/`unknown`, never a fabricated capability.
#
# ---- production locations: HARDCODED. Never env-derived outside test mode. ----
PERF_CAPABILITY_PROC_DIR_DEFAULT='/proc/sys/kernel'
PERF_CAPABILITY_SYSCTL_DIR_DEFAULT='/etc/sysctl.d'
PERF_CAPABILITY_DROPIN_BASENAME='99-cqlite-perf.conf'

# perf_capability_test_mode: rc 0 iff the explicit test marker is set.
perf_capability_test_mode() { [ "${CQLITE_PERF_TEST_MODE:-}" = 1 ]; }

# perf_capability_seam_set: rc 0 iff ANY test-only seam is non-empty. The ONE
# seam reader outside the containment gate below, and deliberately so: it asks only
# "was a seam handed to us at all" (for the marker-less refusal) and never uses the
# VALUE as a path. The structural audit in test_perf_capability.sh allowlists it by
# name, so a future function cannot join it silently.
#
# EVERY non-marker seam MUST be listed here (roborev round 32, Medium). This named only PROC_DIR
# and SYSCTL_DIR while the file had grown three more, so any of those exported WITHOUT the marker
# passed the guard — the marker-less refusal failing OPEN, which is the same incomplete-list-of-
# names shape this whole file exists to avoid. The round-6 audit that was supposed to protect this
# policed WHICH FUNCTIONS may read a seam, never WHICH SEAMS this list must name, so it was blind
# to an omission by construction. The completeness direction is now enforced by CENSUS in
# test_perf_capability.sh (1c-iv): every CQLITE_PERF_* name the library reads, minus the marker
# itself, must appear below — so adding a seam without listing it here FAILS the suite.
perf_capability_seam_set() {
  [ -n "${CQLITE_PERF_PROC_DIR:-}" ] || [ -n "${CQLITE_PERF_SYSCTL_DIR:-}" ] \
    || [ -n "${CQLITE_PERF_TEST_SANDBOX:-}" ] || [ -n "${CQLITE_PERF_SYSCTL_EXTRA_DIRS:-}" ] \
    || [ -n "${CQLITE_PERF_TEST_PRIV_DIR:-}" ]
}

# ---- ONE GATE: POSITIVE SANDBOX CONTAINMENT (review R6-1/R6-2) --------------------
# THE sandbox root is caller-declared (CQLITE_PERF_TEST_SANDBOX) and must PROVE itself: an
# absolute, canonically spelled, existing directory carrying the stamp file below. The stamp is
# what makes the declaration unforgeable by environment alone — a stray
# CQLITE_PERF_TEST_SANDBOX=/etc cannot turn /etc into a sandbox, because the proof lives on the
# FILESYSTEM and writing it into a system directory already needs the privilege this guard
# protects. No denylist appears below: a path is usable because it is provably INSIDE the
# sandbox, never because it failed to look like somewhere forbidden.
#
# FIVE thin functions, ONE predicate — everything ends in perf_capability_path_within:
#   sandbox_root_into O        the declared root, validated; rc 1 + empty when unproven
#   path_within P R            THE predicate (below)
# (rationale condensed; full reasoning in the commit history for #3261.)
PERF_CAPABILITY_SANDBOX_STAMP='.cqlite-perf-sandbox'
# A literal LF and CR, for the line-safety predicate below. Spelled as a literal newline inside
# single quotes rather than `$'\n'` so this stays correct on bash 3.2 (a supported gate host).
PERF_CAPABILITY_LF='
'
PERF_CAPABILITY_CR=$'\r'

# perf_capability_path_lines_ok: rc 0 iff <path> contains NO CR and NO LF.
# NOT a containment defect — the path IS contained — a SERIALIZATION one: the search path is emitted
# one entry per line, so a contained directory NAMED with an embedded newline splits into two
# entries, the second being the real /etc/sysctl.d. Rejected at the boundary rather than escaped
# downstream. Full rationale: docs/development/fleet-runbook.md, "perf seam containment — why".
perf_capability_path_lines_ok() {
  case "${1:-}" in
    *"$PERF_CAPABILITY_LF"*|*"$PERF_CAPABILITY_CR"*) return 1 ;;
  esac
  return 0
}

perf_capability_sandbox_root_into() {
  local __psr_v="${CQLITE_PERF_TEST_SANDBOX:-}"
  eval "$1="
  perf_capability_path_lines_ok "$__psr_v" || return 1
  # ALL trailing slashes, not one (roborev round 31, Low). Stripping a single slash left a root ending
  # in two slashes as one ending in one, which passed the doubled-slash rejection below -- and then the
  # fork-free containment pattern appended its own separator and rejected EVERY child, while the
  # resolving write path still accepted the same root. Read and write disagreeing about the same sandbox
  # is worse than either answer alone. The length guard keeps a bare root slash from collapsing to the
  # empty string, which the absolute-path test below then refuses on its own merits.
  # NO BACKTICKS ANYWHERE IN THIS FUNCTION, comments included: it is in the closed fork-free audit set
  # of scripts/tests/test_agent_gate_summary.sh, which COUNTS backticks over the whole function text and
  # cannot tell a quoted path spelling in prose from a real command substitution. Twelve of them here
  # reddened the gate's tooling-tests component; quote path spellings in words instead.
  while [ "${__psr_v%/}" != "$__psr_v" ] && [ "${#__psr_v}" -gt 1 ]; do __psr_v="${__psr_v%/}"; done
  case "$__psr_v" in *//*) return 1 ;; /?*) ;; *) return 1 ;; esac
  case "/$__psr_v/" in */../*|*/./*) return 1 ;; esac
  # NO SYMLINKED COMPONENT, including the root's own final component (roborev round 32, Medium).
  # Without this the function advertised a canonically spelled root while accepting one reached
  # THROUGH a symlink, and the two containment paths then disagreed about the identical root and
  # child: measured rc 1 from the fork-free sandbox_ok versus rc 0 from the resolving
  # sandbox_ok_resolved. One sandbox must not be both contained and not contained. REJECTING rather
  # than canonicalizing is forced, not preferred: this function is in the closed fork-free audit set
  # and canonicalizing would need cd -P plus pwd -P, i.e. a forked subshell. A root must be spelled
  # as its own destination -- the same rule the drop-in destination and the shim tools already obey.
  perf_capability_nosymlink "$__psr_v" || return 1
  [ -d "$__psr_v" ] && [ -f "$__psr_v/$PERF_CAPABILITY_SANDBOX_STAMP" ] || return 1
  eval "$1=\$__psr_v"
}

# THE containment predicate, and the only place a path is ever judged: rc 0 iff <path> is
# absolute, canonically spelled (no `.`, `..` or `//` component — `//etc` IS `/etc`, R6-1),
# free of CR/LF (so a contained path can never SERIALIZE into two entries, roborev round 3)
# and STRICTLY inside <root>, with the `/` boundary explicit so `/tmp/sandboxevil` is NOT
# inside `/tmp/sandbox`. An empty root refuses; it is never a wildcard.
# The line check lives HERE, in the one predicate every entry point ends in, for the same reason
# containment does: one choke point cannot be skipped by a future consumer.
perf_capability_path_within() {
  [ -n "${2:-}" ] || return 1
  perf_capability_path_lines_ok "${1:-}" || return 1
  perf_capability_path_lines_ok "$2" || return 1
  case "${1:-}" in *//*) return 1 ;; /?*) ;; *) return 1 ;; esac
  case "/$1/" in */../*|*/./*) return 1 ;; esac
  case "$1" in "$2"/?*) return 0 ;; esac
  return 1
}

# perf_capability_nosymlink: rc 0 iff <path> is absolute and NO path component — the final one
# INCLUDED — is a symlink. FORK-FREE: `[ -L ]` is a shell builtin test, so this is usable on the
# gate's contractually fork-free token path, where `cd -P`/`pwd -P` (a subshell, i.e. a process)
# is not.
#
# WHY (issue #3261 AC2). Containment of a SPELLING is not containment of a DESTINATION. The
# inversion to positive containment made the fork-free read check purely textual and thereby
# (rationale condensed; full reasoning in the commit history for #3261.)
perf_capability_nosymlink() {
  local __pns_rest="${1:-}" __pns_acc='' __pns_seg
  case "$__pns_rest" in /*) ;; *) return 1 ;; esac
  __pns_rest="${__pns_rest#/}"
  while [ -n "$__pns_rest" ]; do
    __pns_seg="${__pns_rest%%/*}"
    if [ "$__pns_seg" = "$__pns_rest" ]; then __pns_rest=''; else __pns_rest="${__pns_rest#*/}"; fi
    [ -n "$__pns_seg" ] || continue
    __pns_acc="$__pns_acc/$__pns_seg"
    if [ -L "$__pns_acc" ]; then return 1; fi
  done
  [ -n "$__pns_acc" ]
}

perf_capability_sandbox_ok() {
  local __pso_root=''
  perf_capability_sandbox_root_into __pso_root || return 1
  perf_capability_path_within "${1:-}" "$__pso_root" || return 1
  # ...and no component may be a symlink out of it (#3261 AC2), by builtins alone.
  perf_capability_nosymlink "$1"
}

perf_capability_sandbox_ok_resolved() {
  local __pdr_root='' __pdr_real=''
  # LINE-SAFETY IS CHECKED ON THE ORIGINAL CANDIDATE, BEFORE ANY COMMAND SUBSTITUTION (roborev round
  # 12, Medium). `$(cd -P -- "$1" && pwd -P)` STRIPS trailing newlines, so a directory whose name ends
  # in LF arrived here, lost the LF during canonicalization, and passed a check that only ever saw the
  # stripped form — while every later caller still emits the ORIGINAL spelling, which then splits the
  # one-per-line search path into two entries. The CR/LF guard was added in round 3 for exactly that
  # split; it was simply running too late to see it. Order matters more than the predicate here.
  perf_capability_path_lines_ok "${1:-}" || return 1
  perf_capability_sandbox_root_into __pdr_root || return 1
# RESIDUAL — #3323 entry 3: bind mounts defeat lexical containment. `cd -P`/`pwd -P` resolve
# symlinks but not MOUNTS, so a bind-mounted sandbox path looks contained. Deliberately unfixed:
# the fix is mount-aware fd-relative containment (openat2), not expressible in shell, and this
# escape class is CLOSED by owner ruling. Read #3323 before widening this trust.
  __pdr_root=$(cd -P -- "$__pdr_root" 2>/dev/null && pwd -P) || return 1
  __pdr_real=$(cd -P -- "${1:-/dev/null/never}" 2>/dev/null && pwd -P) || return 1
  perf_capability_path_within "$__pdr_real" "${__pdr_root%/}"
}

# The FILE variant. Judged as <CANONICAL PARENT>/<basename> (issue #3261 AC3): canonicalizing the
# parent and asking whether THE PARENT is contained refused `<sandbox-root>/sysctl.conf`, because
# the parent there IS the root and a root is not STRICTLY inside itself — a legitimate,
# strictly-contained file rejected, which is how a guard teaches people to route around it. The
# assembled path is the thing being authorized, so it is the thing judged.
# The final component may not be a SYMLINK (the AC1 lesson, here on a read whose CONTENTS are
# consumed): a symlinked `sysctl.conf` inside the sandbox would feed the competing-file scan the
# host's real configuration.
perf_capability_sandbox_file_ok_resolved() {
  # Same ordering fix as the directory variant (roborev round 12, Medium): checked on the ORIGINAL
  # argument, so a file whose PARENT ends in LF cannot launder the newline through `pwd -P`.
  perf_capability_path_lines_ok "${1:-}" || return 1
  case "${1:-}" in */?*) ;; *) return 1 ;; esac
  local __pfr_base="${1##*/}" __pfr_root='' __pfr_parent=''
  case "$__pfr_base" in ''|.|..) return 1 ;; esac
  if [ -L "$1" ]; then return 1; fi
  perf_capability_sandbox_root_into __pfr_root || return 1
  __pfr_root=$(cd -P -- "$__pfr_root" 2>/dev/null && pwd -P) || return 1
  __pfr_parent=$(cd -P -- "${1%/*}" 2>/dev/null && pwd -P) || return 1
  perf_capability_path_within "${__pfr_parent%/}/$__pfr_base" "${__pfr_root%/}"
}

# ONE message shape for both mandatory seams: the refusal must NAME the offending seam (that
# is what makes it actionable), so it is parameterised rather than duplicated per seam.
perf_capability_seam_refusal() {
  printf 'perf-capability: REFUSING: CQLITE_PERF_TEST_MODE=1 requires %s INSIDE the declared sandbox %s — its RESOLVED destination (. / .. / // / symlinked ancestors and all) must be strictly contained there; got %s. Test mode NEVER falls back to the real directory.\n' \
    "${1:-<seam>}" "'${3:-}'" "'${2:-<unset>}'" >&2
}

# perf_capability_test_seams_ok: rc 0 iff test mode has a PROVEN sandbox root and BOTH
# mandatory seams RESOLVING strictly inside it. This is the gate on every privileged action
# below, so it takes the resolving form: refusing here is what stops a root `--yes` test run
# from resolving a seam back into the production directory and overwriting the host's own
# drop-in. It refuses loudly, because the failure it prevents is silent otherwise.
perf_capability_test_seams_ok() {
  local ok=0 root=''
  if ! perf_capability_sandbox_root_into root; then
    printf 'perf-capability: REFUSING: CQLITE_PERF_TEST_MODE=1 requires CQLITE_PERF_TEST_SANDBOX to name an absolute existing directory holding the stamp file %s; got %s. Test mode acts only INSIDE a proven sandbox and NEVER falls back to the real directory.\n' \
      "$PERF_CAPABILITY_SANDBOX_STAMP" "'${CQLITE_PERF_TEST_SANDBOX:-<unset>}'" >&2
    return 1
  fi
  perf_capability_sandbox_ok_resolved "${CQLITE_PERF_PROC_DIR:-}" \
    || { perf_capability_seam_refusal CQLITE_PERF_PROC_DIR "${CQLITE_PERF_PROC_DIR:-}" "$root"; ok=1; }
  perf_capability_sandbox_ok_resolved "${CQLITE_PERF_SYSCTL_DIR:-}" \
    || { perf_capability_seam_refusal CQLITE_PERF_SYSCTL_DIR "${CQLITE_PERF_SYSCTL_DIR:-}" "$root"; ok=1; }
  [ "$ok" = 0 ]
}

# perf_capability_env_guard: rc 0 iff this process may act on the perf capability path at
# all. Every refusal is LOUD on stderr and FAILS CLOSED (the caller does nothing privileged
# and claims no verdict):
#   * a seam set WITHOUT the marker — the seams are inert there, so a caller handed one is
#     misconfigured and nothing privileged may proceed;
#   * the marker set WITHOUT a proven sandbox root and both seams inside it — test mode has
#     no fallback (R4-3): allowing one would let a root `--yes` test run `tee` the REAL
#     /etc/sysctl.d, the host mutation the marker promises cannot happen. Checked FIRST,
#     before the tool checks: a test run with no sandbox has nowhere safe to act;
#   * the marker set while a REAL sudo/sysctl is reachable — test mode is hermetic by
#     construction, so a reachable real privileged tool is a harness bug, not something to run.
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
    # The seams' containment predicate, one level down: the tool must be strictly inside the
    # DECLARED shim dir, not merely spelled that way.
    perf_capability_path_within "$resolved" "${dir%/}" || {
      printf 'perf-capability: REFUSING: CQLITE_PERF_TEST_MODE=1 but %s resolves to %s, outside the declared shim dir %s — test mode may never invoke a real privileged tool.\n' \
        "$tool" "$resolved" "$dir" >&2
      return 1
    }
    perf_capability_priv_tool_ok "$resolved" "$tool" || return 1
  done
  # RESIDUAL — #3323 entry 2: privileged-tool validation resolves sudo/sysctl, DISCARDS the
  # resolved paths, and later callers re-resolve by name, so a writable shim dir can swap in a
  # link to the host binary afterwards. Deliberately unfixed (13th escape; class CLOSED by owner
  # ruling; the fix needs held fds). Test-mode only. Read #3323 before widening this trust.
  if [ -n "$dir" ] && [ -d "$dir" ]; then
    for tool in sudo sysctl; do
      [ -e "$dir/$tool" ] || [ -L "$dir/$tool" ] || continue
      perf_capability_priv_tool_ok "$dir/$tool" "$tool" || return 1
    done
  fi
  return 0
}

# perf_capability_priv_tool_ok <resolved-path> <tool-name>: rc 0 iff this privileged executable's
# RESOLVED destination is positively contained beneath the PROVEN sandbox root. Refuses loudly.
# The declared NAME is not the DESTINATION — a `/usr` shim dir and a symlink to the real sudo both
# passed a textual check (AC4, the eighth escape).
# Full rationale: docs/development/fleet-runbook.md, "perf seam containment — why".
perf_capability_priv_tool_ok() {
  perf_capability_sandbox_file_ok_resolved "${1:-}" && return 0
  printf 'perf-capability: REFUSING: CQLITE_PERF_TEST_MODE=1 but the privileged tool %s at %s does not RESOLVE to an executable strictly inside the declared sandbox %s (a symlink to the real tool, or a shim dir that is not itself in the sandbox, resolves OUT of it) — test mode may never invoke a real privileged tool.\n' \
    "${2:-<tool>}" "'${1:-}'" "'${CQLITE_PERF_TEST_SANDBOX:-<unset>}'" >&2
  return 1
}

# ---- resolved locations (the seams apply ONLY in test mode) -------------------
# THE `*_into <outvar>` CONVENTION. The gate's summary path calls the token chain below and
# is contractually FREE — no external process AND no command substitution (each `$( )` forks
# a subshell, which is a process too). A function that answers on stdout therefore CANNOT be
# on that path: its caller must fork to read it. So every function the gate touches has an
# `_into <outvar>` core assigning through a caller-named variable, and the stdout-printing
# form is a thin wrapper for CLI/bootstrap ergonomics — the wrapper is the ONLY place a fork
# is paid, and it is not on the gate's path. Assignment is `eval "$1=\$var"`, NOT a
# (rationale condensed; full reasoning in the commit history for #3261.)
perf_capability_proc_dir_into() {
  eval "$1="
  if perf_capability_test_mode; then
    perf_capability_sandbox_ok "${CQLITE_PERF_PROC_DIR:-}" || {
      printf 'perf-capability: REFUSING to read /proc: CQLITE_PERF_TEST_MODE=1 with CQLITE_PERF_PROC_DIR (%s) not INSIDE the declared sandbox %s — test mode never falls back to %s.\n' \
        "'${CQLITE_PERF_PROC_DIR:-<unset>}'" "'${CQLITE_PERF_TEST_SANDBOX:-<unset>}'" "$PERF_CAPABILITY_PROC_DIR_DEFAULT" >&2
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
# perf_capability_sysctl_dir: the sysctl.d directory — the WRITE side (a root `tee` is aimed
# inside it) and the directory whose CONTENTS the competing-file scan reads, so it takes the
# RESOLVING gate. EVERY consumer of that location comes through this one function, so there is
# exactly one place the check could be forgotten — and it is here.
perf_capability_sysctl_dir() {
  if perf_capability_test_mode; then
    perf_capability_sandbox_ok_resolved "${CQLITE_PERF_SYSCTL_DIR:-}" || {
      printf 'perf-capability: REFUSING to resolve a sysctl.d path: CQLITE_PERF_TEST_MODE=1 with CQLITE_PERF_SYSCTL_DIR (%s) not RESOLVING inside the declared sandbox %s (or unenterable) — test mode never falls back to %s.\n' \
        "'${CQLITE_PERF_SYSCTL_DIR:-<unset>}'" "'${CQLITE_PERF_TEST_SANDBOX:-<unset>}'" "$PERF_CAPABILITY_SYSCTL_DIR_DEFAULT" >&2
      return 1
    }
    printf '%s' "$CQLITE_PERF_SYSCTL_DIR"
    return 0
  fi
  printf '%s' "$PERF_CAPABILITY_SYSCTL_DIR_DEFAULT"
}
# perf_capability_dropin_path: the path a root `tee` is pointed at. The DIRECTORY's gate lives in
# perf_capability_sysctl_dir (one gate, not a prohibition a future entry point could skip), and the
# WRITE TARGET is validated too — a symlink at the managed basename is rc 1 + empty + a named
# reason, because directory containment is NOT write-target containment (AC1).
# Full rationale: docs/development/fleet-runbook.md, "perf seam containment — why".
perf_capability_dropin_path() {
  local __pdi_d __pdi_p
  __pdi_d=$(perf_capability_sysctl_dir) || return 1
  __pdi_p="$__pdi_d/$PERF_CAPABILITY_DROPIN_BASENAME"
  if [ -L "$__pdi_p" ]; then
    printf 'perf-capability: REFUSING: the managed drop-in name %s is a SYMLINK. A privileged `tee` FOLLOWS it and would overwrite the link target instead of the managed file — a contained directory does not license writing through its entries. Inspect where it points and remove it; nothing here will write through it.\n' \
      "'$__pdi_p'" >&2
    return 1
  fi
  printf '%s' "$__pdi_p"
}

# perf_capability_dropin_install [<priv-cmd>...]: write the managed drop-in as an ATOMIC
# DIRECTORY-ENTRY REPLACEMENT, so a pre-existing symlink at the managed name is REPLACED, never
# FOLLOWED (issue #3261 AC1). argv is the privilege prefix (empty when already root); rc 0 iff the
# managed bytes are in place at the managed path afterwards, verified by re-reading the file.
#
# Content goes to a fresh staging entry in the ALREADY-VALIDATED directory, then `mv -fT` —
# rename(2), which replaces the NAME and never dereferences the destination. Same directory, so the
# rename is same-filesystem and atomic.
#
# WHAT MAKES THIS SAFE IS THE PRECONDITION, NOT THE STAGING MECHANICS. Three successive fixes here
# were each defended with a claim that proved FALSE, so the reasoning is recorded rather than the
# conclusion alone (full history: #3261, roborev rounds 1-3):
#   * a FIXED staging name, checked-then-opened, claimed safe because the race "cannot happen". It
#     could: anyone able to create entries in the directory could re-plant that known name as a
#     symlink between the check and the privileged open.
#   * `mktemp` (O_CREAT|O_EXCL, 6 random chars, created under the SAME privilege that writes) closed
#     the CREATE race — but mktemp returns a NAME and each later step REOPENS it, so the window moved
#     rather than closing. A pid suffix would not have helped either; a pid is predictable.
#   * grouping every step into ONE privileged `sh -c` was then defended with a claim THIS COMMENT
#     ITSELF MADE AND WHICH IS FALSE: that no unprivileged process is scheduled between the steps.
#     `sh -c` gives SEQUENCING WITHIN ONE PROCESS, never MUTUAL EXCLUSION against other processes,
#     which run concurrently on other CPUs regardless of how we group our own commands. Consolidation
#     is kept — it removes needless windows — but it is NOT what makes this safe.
#   * what closes the class: REMOVE THE ATTACKER'S PRECONDITION. Every step of the race needs the
#     ability to create or replace entries in the target directory, so the install REFUSES a target
#     directory that anyone less privileged than the writer can write — it must be owned by the
#     identity performing the privileged write and be neither group- nor world-writable. There is
#     then no actor to race against, whatever the timing.
# The ownership/mode test runs INSIDE the privileged shell against `id -u` of that shell, so it tests
# the identity that will actually write (root in production, the shim under test mode) rather than
# whoever invoked us. Undeterminable ownership or mode is a REFUSAL, not an assumption. Deliberately
# conservative: group-writable is refused even with the sticky bit, because "arguably safe" is what
# already cost this function three review rounds.
#   `chmod 0644` after the write is load-bearing: `mktemp` creates 0600, and the idempotency compare
#   runs from an UNPRIVILEGED bootstrap process that could not read a root-owned 0600 file — every
#   later run would see "not current" and rewrite. The old `tee` got 0644 from root's umask.
#   The staging name begins with `.` and does not end in `.conf`, so the competing-file scan (which
#   globs `*.conf`) can never mistake it for a rival drop-in.
#   GNU-COREUTILS DEPENDENCY, STATED EXACTLY: `mv -fT` and `stat -c` are GNU-only. The PRODUCTION
#   path is genuinely gated — bootstrap reaches this function only when PLATFORM=linux (set at :85,
#   branch at :412, PERF_SECTION_OK initialised to 0 at :405 so no ambient export can steer it).
#   NOT gated: scripts/tests/test_perf_capability.sh calls this DIRECTLY, so its staged-install cases
#   are capability-probed and COUNTED-skipped off GNU (roborev round 5). Neither portability guard in
#   the repo scans this file, so nothing mechanically protects the gate; recorded, not papered over.
perf_capability_dropin_install() {
  local __pin_d __pin_p __pin_rc
  __pin_d=$(perf_capability_sysctl_dir) || return 1
  # TRAILING SLASHES ARE STRIPPED BEFORE ANY CHECK OR PATH CONSTRUCTION (roborev round 10, Low).
  # `[ -L "$d" ]` FOLLOWS a trailing slash: for a symlinked directory `link`, `[ -L link ]` is true
  # but `[ -L link/ ]` and `[ -L link// ]` are FALSE, so the destination-symlink refusal this
  # function explicitly promises could be walked past with one extra character. Stripping is the
  # right shape here and NOT another spelling denylist: normalising the input to ONE canonical form
  # makes the affirmative check total, whereas enumerating bad spellings is the unbounded game this
  # family lost eleven times. The length guard keeps `/` itself from becoming the empty string —
  # a root destination then fails the ownership/writability precondition on its own merits rather
  # than by accident.
  while [ "${__pin_d%/}" != "$__pin_d" ] && [ "${#__pin_d}" -gt 1 ]; do __pin_d="${__pin_d%/}"; done
  __pin_p="$__pin_d/$PERF_CAPABILITY_DROPIN_BASENAME"
  # CONTENT IS GENERATED AND CHECKED **BEFORE** ANY PRIVILEGED COMMAND RUNS (roborev round 9,
  # Medium). This used to pipe the generator straight into the privileged shell, so the pipeline's
  # status was the LAST command's and a failed generator was invisible unless the CALLER happened to
  # have `pipefail` set — a correctness property no library function should delegate to its caller.
  # Worse, the privileged write would already have started on empty or partial content. Generating
  # first means a generator failure returns before privilege is acquired at all. Same sentinel trick
  # as dropin_current, for the same reason: `$( )` strips trailing newlines, and the drop-in's final
  # newline is part of the canonical bytes the idempotency compare comes back for.
  local __pin_c
  __pin_c=$(perf_capability_dropin_content; __pdc_rc=$?; printf 'X'; exit "$__pdc_rc") || return 1
  __pin_c=${__pin_c%X}
  [ -n "$__pin_c" ] || { printf 'perf-capability: REFUSING: the drop-in content generator produced nothing.\n' >&2; return 1; }
  # ONE privileged invocation for the WHOLE staged install. The content arrives on this shell's
  # stdin; `mktemp`, the write, `chmod` and the rename all run inside it.
  printf '%s' "$__pin_c" | "$@" sh -c '
    set -u
    d=$1; p=$2; b=$3
    # Normalised again INSIDE the privileged shell, deliberately: the outer caller strips trailing
    # slashes, but this block is the thing holding privilege and must not depend on someone else
    # having done it. Same reason the mktemp answer is re-checked here rather than trusted.
    while [ "${d%/}" != "$d" ] && [ "${#d}" -gt 1 ]; do d="${d%/}"; done
    # TOOL COMPATIBILITY IS EXERCISED HERE, IN THE PRIVILEGED SHELL (roborev round 17, Medium — a
    # (rationale relocated: docs/development/fleet-runbook.md, "perf seam containment — why", tool-compatibility-is-exercised-here-in-the-priv.)
    # `/` NOT "$d": statting the destination conflated "no GNU stat" with "destination missing" (r23).
    stat -c '%a' -- / >/dev/null 2>&1 || {
      printf "perf-capability: UNSUPPORTED on this host: stat -c is unavailable (GNU coreutils required), so ownership and mode cannot be established before a privileged write.\n" >&2
      exit 2; }
    # THE PRECONDITION (roborev round 3): nobody less privileged than this shell may be able to
    # create or replace entries in $d. Without that there is an actor to race; with it there is not.
    #   WHY THIS IS NOT A TWELFTH ATTEMPT TO OUT-TIME THE RACE (owner ruling A-prime): escapes 9-11
    #   narrowed a WINDOW (unpredictable name, O_EXCL create, one privileged process). This touches
    #   no window. It removes the ATTACKER PRECONDITION, turning "production is safe because
    #   /etc/sysctl.d is root-owned" from a recorded ASSUMPTION into an ENFORCED INVARIANT: assume
    #   nothing about the destination, measure it, fail closed.
    #   RESIDUAL — #3323 entry 1: the ancestor-chain rename race. This validates the target
    #   directory, NOT the path by which it is reached, so an actor able to write an ANCESTOR can
    #   swap the validated directory for a symlink after these checks. Deliberately unfixed (12th
    #   escape; class CLOSED; needs openat2). Read #3323 before widening this trust.
    if [ -L "$d" ]; then
      printf "perf-capability: REFUSING: the drop-in directory %s is a SYMLINK — its owner and mode say nothing about where entries would actually be created, so it cannot be proven un-writable by less-privileged users.\n" "$d" >&2
      exit 1
    fi
    if [ ! -d "$d" ]; then
      printf "perf-capability: REFUSING: the drop-in directory %s is not a directory.\n" "$d" >&2
      exit 1
    fi
    me=$(id -u 2>/dev/null) || {
      printf "perf-capability: REFUSING: cannot determine the privileged writer identity (id -u failed), so the drop-in directory cannot be proven un-writable by less-privileged users.\n" >&2
      exit 1; }
    dinfo=$(stat -c "%u %a" -- "$d" 2>/dev/null) || {
      printf "perf-capability: REFUSING: cannot determine owner/mode of the drop-in directory %s, so it cannot be proven un-writable by less-privileged users.\n" "$d" >&2
      exit 1; }
    downer=${dinfo%% *}; dmode=${dinfo##* }
    # ZERO-PAD BEFORE TAKING THE LAST THREE DIGITS. `stat -c %a` drops leading zeros, so mode 0033
    # arrives as "33" — and `${dmode%???}` cannot match a 2-character string, leaving `dperm` EMPTY,
    # matching none of the write-bit patterns below, and PASSING a group- AND world-writable
    # directory. That was a real bypass of this very precondition (roborev round 5, High), and it
    # survived a hand audit that reasoned about the 3- and 4-digit cases and never considered a
    # SHORTER one. The suffix-strip idiom is only safe once the string is known to be long enough,
    # so the padding is not cosmetic: it is what makes the check below total.
    case "$dmode" in
      ?)   dperm="00$dmode" ;;
      ??)  dperm="0$dmode" ;;
      ???) dperm="$dmode" ;;
      *)   dperm=${dmode#"${dmode%???}"} ;;
    esac
    case "$dperm" in
      *[!0-7]*|"")
        printf "perf-capability: REFUSING: the drop-in directory %s reported a mode (%s) that is not octal digits, so it cannot be proven un-writable by less-privileged users.\n" "$d" "$dmode" >&2
        exit 1 ;;
    esac
    if [ "$downer" != "$me" ]; then
      printf "perf-capability: REFUSING: the drop-in directory %s is owned by uid %s, not by the privileged writer uid %s — a directory someone else owns can have its entries replaced under a privileged write.\n" "$d" "$downer" "$me" >&2
      exit 1
    fi
    case "$dperm" in
      ?[2367]?|??[2367])
        printf "perf-capability: REFUSING: the drop-in directory %s is mode %s — group- or world-writable, so a less-privileged user can replace entries inside it while a privileged write is in progress. Tighten it (chmod go-w) before installing.\n" "$d" "$dmode" >&2
        exit 1 ;;
    esac
    # mv -T IS EXERCISED HERE, AFTER the ownership precondition and BEFORE the real staging entry.
    # Placement is deliberate on both sides. AFTER the precondition, because that is what establishes
    # no less-privileged actor can create entries in $d — which is precisely what makes a PREDICTABLE
    # probe name safe, so this does not need (and must not consume) mktemp. NOT consuming mktemp also
    # keeps it from pre-empting the staging entry checks below, which have their own cases.
    __x1="$d/.perfcap-probe.$$"; __x2="$__x1.b"
    rm -f -- "$__x1" "$__x2" 2>/dev/null
    # ABSENCE IS PROVEN, NOT ASSUMED (roborev round 21, High). `rm` can fail (read-only mount) and its
    # status was ignored, and `: >` FOLLOWS a symlink — so a leftover link at this predictable name
    # could truncate an arbitrary file under privilege. The mode precondition proves nobody
    # less-privileged can CREATE entries here, not that a pre-existing one was removed.
    if [ -e "$__x1" ] || [ -L "$__x1" ] || [ -e "$__x2" ] || [ -L "$__x2" ]; then
      printf "perf-capability: REFUSING: probe entries under %s could not be cleared, so opening them could follow a leftover symlink under privilege.\n" "$d" >&2
      exit 1
    fi
    # CREATION FAILURE IS ITS OWN OUTCOME (roborev round 21, Low): sharing one branch with the `mv`
    # test reported an unwritable directory as an unsupported host, making bootstrap suppress the retry
    # remedy exactly where retrying is right. rc 1 REFUSED here; rc 2 is reserved for `mv` lacking -T.
    # SUBSHELL because `:` is a POSIX SPECIAL builtin: a redirection failure on it makes a
    # non-interactive shell EXIT, and dash exits 2 — silently colliding with the rc 2 sentinel. The
    # subshell contains both the exit and the leaked diagnostic, so the caller sees rc 1.
    if ! ( : >"$__x1" ) 2>/dev/null; then
      printf "perf-capability: REFUSING: cannot create a probe entry in %s — the directory is not writable by the privileged writer.\n" "$d" >&2
      exit 1
    fi
    if ! mv -fT -- "$__x1" "$__x2" 2>/dev/null; then
      rm -f -- "$__x1" "$__x2" 2>/dev/null
      printf "perf-capability: UNSUPPORTED on this host: mv --no-target-directory (-T) does not work (GNU coreutils required), so the drop-in cannot be replaced atomically without risking a symlinked destination.\n" >&2
      exit 2
    fi
    rm -f -- "$__x2" 2>/dev/null
    t=$(mktemp -- "$d/.$b.XXXXXX") || exit 1
    # mktemp CREATED the entry, so mktemp is what must be checked, INSIDE this privileged shell and
    # fail-closed: it has to name a fresh regular file (never a symlink) directly in the directory
    # the caller already validated. Anything else is not a safe staging entry.
    case "$t" in
      "$d"/.?*) ;;
      *) printf "perf-capability: REFUSING: mktemp did not create a staging entry inside the validated directory %s (got %s).\n" "$d" "${t:-<empty>}" >&2
         exit 1 ;;
    esac
    if [ -L "$t" ] || [ ! -f "$t" ]; then
      printf "perf-capability: REFUSING: the staging entry %s is not a fresh regular file.\n" "$t" >&2
      rm -f -- "$t"; exit 1
    fi
    # `mv -T` (--no-target-directory) is REQUIRED, not cosmetic: without it a symlink-to-DIRECTORY
    # planted at the managed name makes `mv` move the staging file INTO that directory, i.e. the
    # rename that exists to avoid following a symlink follows one instead. With -T the destination
    # is always treated as a plain name to replace.
    if ! tee -- "$t" >/dev/null || ! chmod 0644 -- "$t" || ! mv -fT -- "$t" "$p"; then
      rm -f -- "$t"; exit 1
    fi
  ' perf-capability-install "$__pin_d" "$__pin_p" "$PERF_CAPABILITY_DROPIN_BASENAME" >/dev/null
  # rc PROPAGATED, not collapsed: `|| return 1` here used to flatten the privileged shell's status,
  # which silently destroyed the rc 2 UNSUPPORTED signal the caller is meant to distinguish.
  __pin_rc=$?
  [ "$__pin_rc" -eq 0 ] || return "$__pin_rc"
  perf_capability_dropin_current
}

# perf_capability_dropin_content: the EXACT bytes of the managed drop-in. It is a WHOLE
# managed file (not a delimited block inside a foreign one), so idempotency is a plain
# byte-compare of the entire file — simpler and safer than editing someone else's config.
# Callers wanting to INSTALL must use perf_capability_dropin_install (staged, containment-checked,
# atomic rename) — never `sudo tee <path>`, which opens the destination by name and follows a
# symlink planted there. This function only PRINTS the canonical bytes, which is also what bootstrap
# prints, so a hand-applied fix is byte-identical and the next bootstrap run is a no-op.
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

# perf_capability_dropin_current: rc 0 iff the drop-in exists with EXACTLY the managed
# (rationale: fleet-runbook.md, perf seam containment, perf-capability-dropin-current-rc-0-iff-the)
perf_capability_dropin_current() {
  local path want got=''
  path=$(perf_capability_dropin_path) || return 1
  [ -f "$path" ] && [ -r "$path" ] || return 1
  # THE SENTINEL MUST NOT SWALLOW THE GENERATOR'S STATUS (roborev round 9, Medium). `printf X`
  # (rationale: fleet-runbook.md, perf seam containment, the-sentinel-must-not-swallow-the-generator)
  want=$(perf_capability_dropin_content; __pdc_rc=$?; printf 'X'; exit "$__pdc_rc") || return 1
  if IFS= read -r -d '' got <"$path"; then
    return 1
  fi
  [ "$want" = "${got}X" ]
}

# perf_capability_sysctl_search_path: the COMPLETE set of locations `sysctl --system`
# (procps-ng) and systemd-sysctl load, one per line, in DESCENDING NAME-MASKING PRECEDENCE —
# the order both tools scan (sysctl(8) SYSTEM FILE PRECEDENCE, sysctl.d(5) CONFIGURATION
# DIRECTORIES AND PRECEDENCE): /etc/sysctl.d, /run/sysctl.d, /usr/local/lib/sysctl.d,
# /usr/lib/sysctl.d, /lib/sysctl.d, and finally the FILE /etc/sysctl.conf.
# TWO INDEPENDENT RULES decide who wins, and the scan below implements both:
#   MASKING  "once a file of a given filename is loaded, any file of the same name in
#            subsequent directories is ignored" — so /etc/sysctl.d/50-x.conf REPLACES
#            /usr/lib/sysctl.d/50-x.conf outright, and reporting the masked one would name
# (rationale relocated: docs/development/fleet-runbook.md, "perf seam containment — why", usr-lib-sysctl-d-50-x-conf-outright-and-reporti.)
# In TEST MODE the path is the sandbox seam plus the optional colon-separated
# CQLITE_PERF_SYSCTL_EXTRA_DIRS (lower-precedence stand-ins, same descending order): the real
# /run and /usr/lib are never read. EVERY entry goes through the SAME RESOLVING gate as the
# write path (R6-2 — this entry point once used the syntactic one, so a symlinked ancestor
# could point a "sandboxed" scan at the host's real configuration).
perf_capability_sysctl_search_path() {
  local __psp_d __psp_e __psp_ok
  if perf_capability_test_mode; then
    __psp_d=$(perf_capability_sysctl_dir) || return 1
    printf '%s\n' "$__psp_d"
    local -a __psp_extra=()
    # THE WHOLE UNSPLIT VALUE IS LINE-CHECKED FIRST (roborev round 31, Medium). `read` consumes only the
    # FIRST LINE of its input, so an EXTRA_DIRS value whose first line is a perfectly valid contained
    # directory succeeded while SILENTLY DISCARDING everything after the newline -- the scan then reported
    # "no competing files" having never looked at the rest, which is the falsely-reassuring answer this
    # diagnostic exists to prevent. The round-3 CR/LF work validated the SPLIT ENTRIES; it never validated
    # the value being split, so a newline hid entries instead of forging one.
    perf_capability_path_lines_ok "${CQLITE_PERF_SYSCTL_EXTRA_DIRS:-}" || {
      printf 'perf-capability: REFUSING: CQLITE_PERF_SYSCTL_EXTRA_DIRS contains CR or LF, so a read would silently keep only its first line and the competing-file scan would report on an incomplete set.\n' >&2
      return 1
    }
    # `read -a` splits on IFS WITHOUT globbing (an unquoted `for x in $var` would glob).
    IFS=':' read -r -a __psp_extra <<<"${CQLITE_PERF_SYSCTL_EXTRA_DIRS:-}"
    for __psp_e in ${__psp_extra[@]+"${__psp_extra[@]}"}; do
      [ -n "$__psp_e" ] || continue
      __psp_ok=0
      case "${__psp_e##*/}" in
        sysctl.conf) perf_capability_sandbox_file_ok_resolved "$__psp_e" && __psp_ok=1 ;;
        *)           perf_capability_sandbox_ok_resolved "$__psp_e" && __psp_ok=1 ;;
      esac
      [ "$__psp_ok" = 1 ] || {
        printf 'perf-capability: REFUSING: CQLITE_PERF_SYSCTL_EXTRA_DIRS entry %s does not RESOLVE inside the declared sandbox %s — a test-mode scan may never read the host'"'"'s real sysctl configuration.\n' \
          "'$__psp_e'" "'${CQLITE_PERF_TEST_SANDBOX:-<unset>}'" >&2
        return 1
      }
      printf '%s\n' "$__psp_e"
    done
    return 0
  fi
  printf '%s\n' /etc/sysctl.d /run/sysctl.d /usr/local/lib/sysctl.d /usr/lib/sysctl.d \
                /lib/sysctl.d /etc/sysctl.conf
}

# rc 0 iff the file ASSIGNS either control. Both spellings sysctl accepts are matched
# (`kernel.x` and `kernel/x`, sysctl.d(5)) plus the optional leading `-` ignore-failure
# prefix; a commented-out line assigns nothing.
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
# baseline) recorded a hand-set perf_event_paranoid/kptr_restrict "silently reverting" and
# none identified the cause. The cause is a NAMED FILE: stock Ubuntu ships
# /etc/sysctl.d/10-kernel-hardening.conf with `kernel.kptr_restrict = 1`, re-asserted at
# every boot and by every `sysctl --system`. "It silently reverts" is unactionable;
# "10-kernel-hardening.conf sets kptr_restrict = 1 and sorts BEFORE ours, so ours wins" is a
# diagnosis. Ordering is lexicographic by BASENAME in BYTE order (what systemd-sysctl and
# `sysctl --system` use) and `[ "$a" \> "$b" ]` is the right operator: the `[` builtin
# compares with strcmp (verified: byte order even under a UTF-8 LC_ALL), whereas `[[ > ]]`
# switches to locale collation and could mis-rank names differing only in punctuation.
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
  # it may not answer an unknown with good news. The SAME rule applies per FILE inside a
  # readable directory (R8-4, in the loop below).
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
      # EVERY GLOBBED FILE IS VALIDATED IN TEST MODE, NOT JUST ITS DIRECTORY (roborev round 11,
      # (rationale relocated: docs/development/fleet-runbook.md, "perf seam containment — why", every-globbed-file-is-validated-in-test-mode-not.)
      if perf_capability_test_mode && [ -e "$f" ] \
         && ! perf_capability_sandbox_file_ok_resolved "$f"; then
        printf 'perf-capability: REFUSING to scan %s — CQLITE_PERF_TEST_MODE=1 and it does not resolve to a real file strictly inside the declared sandbox (a symlink, or a path leading outside it), so scanning it would fabricate diagnostics from HOST state.\n' \
          "'$f'" >&2
        return 1
      fi
      # AN UNMATCHED GLOB IS NOT AN UNREADABLE FILE (issue #3249 review R8-4). With no
      # match bash leaves the PATTERN itself in $f (nullglob is not set) and it does not
      # exist — that directory genuinely holds no competitor, skip it. But a file that
      # EXISTS and cannot be READ is an UNKNOWN, and `sysctl --system` runs as ROOT: it
      # can read and APPLY exactly the file we could not open. Skipping it would let the
      # caller print "no other file sets these keys" about an unexamined competitor —
      # the reassuring answer this whole diagnostic exists to stop giving. Fail the scan.
      if [ ! -r "$f" ]; then
        [ -e "$f" ] || continue
        printf 'perf-capability: could not scan %s — it exists but is unreadable, and a privileged `sysctl --system` CAN read it, so whether it competes for perf_event_paranoid/kptr_restrict is UNKNOWN.\n' \
          "'$f'" >&2
        return 1
      fi
      [ -f "$f" ] || continue   # readable but a directory/socket named *.conf: sysctl reads none
      name="${f##*/}"
      [ "$name" = "$base" ] && continue
      # MASKING (precedence rules above): a higher-precedence directory already supplied this
      # basename, so sysctl ignores THIS file. Recorded BEFORE the content test, because a
      # same-named file that sets nothing still masks one that does.
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

# perf_capability_proc_read <outvar> <name>: the CURRENT kernel value read straight from
# (rationale: fleet-runbook.md, perf seam containment, perf-capability-proc-read-outvar-name-the-cu)
# WHITESPACE IS TRIMMED, NEVER TRUNCATED AT (fail-open audit, R4 round). The earlier
# `${v%%[[:space:]]*}` cut the value at its FIRST space, so a malformed `0 1` became a
# perfectly capable-looking `0` — an unknown resolving to the good case, in the one function
# the gate's `perf=` token comes from. Surrounding whitespace (a CRLF's `\r` included) is
# stripped; anything interior stays so `is_int` rejects it and the token becomes `unknown`.
# `IFS=` makes that independent of the caller's IFS, and both trims are parameter
# expansions — no fork on the gate's emit path.
perf_capability_proc_read() {
  local __pcr_out="$1" __pcr_dir="" __pcr_v=""
  eval "$__pcr_out="
  perf_capability_proc_dir_into __pcr_dir || return 1
  # THE CONTROL FILE ITSELF IS CHECKED, NOT JUST ITS DIRECTORY (roborev round 25, Medium). The
  # directory gate proved the DIRECTORY contained and symlink-free; it said nothing about the ENTRY.
  # A regular contained PROC_DIR could hold perf_event_paranoid as a symlink to the host file, so the
  # token read attacker-chosen or real values and fabricated an ok capability -- the same
  # directory-is-not-its-entries lesson as the write path (AC1), one surface over. Fork-free, so the
  # token path keeps its contract; in test mode containment is required too.
  # NOTE: no backticks in this function comment on purpose -- the fork-free emit-path audit in
  # test_agent_gate_summary.sh counts them WITHOUT stripping comments, so prose alone can red it.
  # RESIDUAL -- #3323 entry 4: these checks are CHECK-THEN-OPEN. A concurrent writer can replace the
  # validated entry, or an ancestor, between the check and the redirection below, so the read can still
  # land outside the sandbox; the competing-file scan shares the shape. Deliberately unfixed: the fix is
  # fd-relative no-follow/beneath opens (openat2), not expressible in shell, and this escape class is
  # CLOSED by owner ruling -- recorded, not re-attempted. Test-mode only. Read #3323 first.
  perf_capability_nosymlink "$__pcr_dir/$2" || return 1
  if perf_capability_test_mode; then
    perf_capability_sandbox_ok "$__pcr_dir/$2" || return 1
  fi
  [ -r "$__pcr_dir/$2" ] || return 1
  IFS= read -r __pcr_v <"$__pcr_dir/$2" 2>/dev/null
  __pcr_v="${__pcr_v#"${__pcr_v%%[![:space:]]*}"}"
  __pcr_v="${__pcr_v%"${__pcr_v##*[![:space:]]}"}"
  [ -n "$__pcr_v" ] || return 1
  eval "$__pcr_out=\$__pcr_v"
}

# the stdout form of the read above, for CLI/bootstrap use — NOT the gate path (reading it
# costs the caller a `$( )`).
perf_capability_proc_value() {
  local __pcv_v
  perf_capability_proc_read __pcv_v "$1" || return 1
  printf '%s' "$__pcv_v"
}

# perf_capability_is_int <value>: rc 0 iff <value> is a plain optionally-negative integer
# NARROW ENOUGH for shell arithmetic. Both halves are load-bearing: `[ 1abc -ge 1 ]` and
# `[ 99999999999999999999999 -ge 1 ]` do NOT compare — each prints "integer expression
# expected" and returns 2 (neither true NOR false), so a malformed or oversized value would
# fall past BOTH a `>= 1` and a `<= 0` test and be reported as good (a WRONG capability
# claim) while leaking an error line into the gate's output.
perf_capability_is_int() {
  local body="${1#-}"
  [ -n "$body" ] || return 1
  case "$body" in *[!0-9]*) return 1 ;; esac
  [ "${#body}" -le 10 ]
}

# perf_capability_token_into <outvar>: the FREE capability read, and THE function the gate's
# accelerators line calls. Free is a hard contract, enforced by test_agent_gate_summary.sh
# case 9f-free: pure /proc through shell builtins — no `perf` exec, no external process of
# ANY kind, and no command substitution anywhere in the chain (hence the <outvar>: every
# `$( )` is a forked subshell, and the gate emits this line on every summary).
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

# the stdout form, for the `--token` CLI and bootstrap. NOT the gate path — reading stdout
# costs the caller the very `$( )` fork the `_into` core above exists to avoid.
perf_capability_token() {
  local __ptk_v
  perf_capability_token_into __ptk_v
  printf '%s' "$__ptk_v"
}

# ---- WHOSE capability? the privilege dimension (issue #3249 review) -----------
# perf_event_paranoid restricts UNPRIVILEGED users; ROOT BYPASSES IT ENTIRELY. So
# `perf stat -C 0 -e cycles` run by root SUCCEEDS on a paranoid=4 box where every
# unprivileged agent process still gets EACCES — and `sudo bash
# scripts/bootstrap-agent-machine.sh` is a completely normal provisioning invocation
# (arguably the most likely one, since installing the drop-in needs root). A root-run
# functional check reported as "perf capability verified" is therefore a FALSE verification
# of an unprofileable box: the failure mode the functional check exists to remove,
# reintroduced through the privilege dimension. The property under test is "an UNPRIVILEGED
# process can collect CPU-WIDE cycles", which a root-run probe cannot demonstrate — so the
# probe DROPS PRIVILEGE when it can and SAYS SO when it cannot, and the caller then
# subordinates the functional result to the identity-independent /proc token.
#
# perf_capability_self_uid_into <outvar>: THIS process's uid, rc 0 ONLY when genuinely known
# — `id -u` must EXIST, exit 0, and print a validated non-negative integer. rc 1 (<outvar>
# emptied) means "identity unknown", which is NOT "unprivileged" (review R4-1). The previous
# shape, `$(id -u 2>/dev/null || echo 1000)`, FAILED OPEN: a missing or broken `id` made a
# ROOT process look unprivileged, so its root perf run was accepted as unprivileged evidence
# and printed a false VERIFIED — the R3-1 defect, through the detector written to prevent it.
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
# (rationale: fleet-runbook.md, perf seam containment, b951)
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
# THE NUMERIC IDS ARE THE TARGET; the NAME is optional and set only when the passwd database
# confirms it resolves to exactly those non-zero ids (R4-2). An unverifiable SUDO_USER is
# dropped, not trusted: the numeric-only mechanisms (setpriv, `sudo -u '#<uid>'`) still work,
# and a name-requiring mechanism correctly reports it has nothing safe to use.
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
# Mechanism order: `setpriv` (util-linux; a plain setresuid — no PAM, no session, no shell),
# (rationale relocated: docs/development/fleet-runbook.md, "perf seam containment — why", mechanism-order-setpriv-util-linux-a-plain-setre.)
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

# perf_capability_verify [prefix-word...]: the FUNCTIONAL verification (AC2). A bootstrap
# (rationale relocated: docs/development/fleet-runbook.md, "perf seam containment — why", perf-capability-verify-prefix-word-the-functiona.)
# CSV mode (`-x,`) is parsed rather than the human table: the human renderer is
# locale-formatted (`1.234.567`) and its column layout has changed across perf releases,
# while the CSV shape `<count>,<unit>,<event>,...` is stable. Prints a short
# machine-greppable reason (stdout) either way; rc 0 = verified.
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
  # Event-name matching must accept a QUALIFIED cycle event: on a hybrid-PMU CPU (Intel
  # (rationale: fleet-runbook.md, perf seam containment, event-name-matching-must-accept-a-qualified)
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
      # rc 0 requires BOTH a functional pass AND that it came from an unprivileged
      # identity: a root run with no way to drop privilege reports its result AND that
      # the result is not evidence about an agent process.
      local pre='' state='' v rc=0 unpriv=0
      perf_capability_drop_prefix_into pre state && unpriv=1
      # shellcheck disable=SC2086  # deliberate split of our own literal prefix tokens
      v=$(perf_capability_verify $pre) || rc=1
      printf '%s identity=%s\n' "$v" "$state"
      [ "$unpriv" = 1 ] || rc=1
      return $rc ;;
    --drop-in)      perf_capability_dropin_content ;;
    --drop-in-path) perf_capability_dropin_path || return 1; printf '\n' ;;
    -h|--help|'')   perf_capability_usage ;;
    *)              printf 'perf-capability: unknown arg: %s\n' "$1" >&2; perf_capability_usage >&2; return 2 ;;
  esac
}

# Executed directly (never when sourced): shell options are set HERE, inside the guard, so
# sourcing can never change a caller's `set` flags.
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
  set -uo pipefail
  perf_capability_main "$@"
fi
