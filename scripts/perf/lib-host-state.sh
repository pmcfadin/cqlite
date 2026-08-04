#!/usr/bin/env bash
# lib-host-state.sh — the HOST STATE this rig mutates, and the guarantee that it puts
# it back (issue #3272 finding 3, hardened by review round 1's B3).
#
# Sourced, not executed, and it sets NO shell options: `set -euo pipefail` in a
# library mutates the SOURCING shell's options, which is the caller's decision.
#
# Split into its own file under the campsite rule, along a real responsibility seam:
# this is the only part of the rig that changes anything OUTSIDE the repository and
# outside its own process tree, and it is the part whose failure is security-adjacent
# rather than merely a wrong number. Two entry points the driver uses:
#
#   `relax_perf_sysctls`  — capture the priors, enroll them, then weaken. It never
#                           weakens a knob whose prior it could not capture.
#   `restore_sysctls`     — put back everything enrolled, per knob, reporting each.
#
# The caller is responsible for invoking `restore_sysctls` from a trap that covers
# EXIT and the signals; `ws0-baseline.sh` composes it into its single `on_exit`.

# ---------------------------------------------------------------------------
# Host sysctl state — CAPTURED before mutation, RESTORED on every exit path
# ---------------------------------------------------------------------------
# Issue #3272, finding 3 (security-adjacent). This rig weakens two host hardening
# knobs so `perf stat -C` can count CPU-wide:
#
#   kernel.perf_event_paranoid = -1   (unprivileged CPU-wide + kernel counting)
#   kernel.kptr_restrict       = 0    (kernel pointers exposed via /proc)
#
# It used to set both and NEVER put them back: the only trap was
# `trap stop_server EXIT`, so a success, a FATAL and a Ctrl-C all left the box less
# hardened than the rig found it — permanently, for every subsequent process on a
# shared fleet machine, with nothing in the output saying so.
#
# The prior values are captured BEFORE the mutation (there is nothing to restore to
# otherwise) and restored from ONE exit handler that also stops the server. Three
# properties the restore must have, because it runs on the failure paths too:
#
#  * IDEMPOTENT — `SYSCTLS_MUTATED` gates it, so a handler that runs twice, or a run
#    that never touched the knobs, is a no-op rather than a spurious `sysctl -w`.
#  * NON-FATAL, per step — every write runs as an `if` CONDITION, which `set -e` does
#    not act on, and the function ends in an explicit `return 0`. This is cleanup: a
#    restore that can exit non-zero would turn a successful measurement into a failed
#    one, and a restore that inherits `set -e` and dies on the first knob would leave
#    the SECOND one weakened, which is the exact bug being fixed.
#  * REGISTERED ON SIGNALS TOO — `EXIT` alone does not fire for SIGINT/SIGTERM/SIGHUP
#    while a foreground child (a long `perf stat` leg) is running, and Ctrl-C during
#    a 45s step is the single most likely way this rig ends.
#
# # PER-KNOB, because the first fix of this finding was itself partial (#3272 round 1)
#
# The restore's success/warning split used to be keyed on "was ANYTHING restored":
#
#     if [[ "${#restored[@]}" -gt 0 ]]; then echo "restored host sysctls: …"
#     else echo "WARNING: …"; fi
#
# and each knob's restore was gated on `[[ -n "$<KNOB>_PRIOR" ]]`. So a PARTIAL
# restore took the AFFIRMATIVE branch. MEASURED against that code: with
# `PARANOID_PRIOR=2` and `KPTR_PRIOR=""` (the value the capture below falls back to
# when `/proc/sys/kernel/kptr_restrict` is unreadable — while the mutation wrote
# `kernel.kptr_restrict=0` regardless), `restore_sysctls` issued ONE `sysctl -w`,
# printed `restored host sysctls: perf_event_paranoid=2`, and emitted no warning. The
# operator was told the host was restored while `kptr_restrict=0` was left behind
# permanently. That is finding 3's own defect in narrower form — a fix that moved the
# problem — so it is closed from BOTH ends:
#
#  1. ROOT CAUSE: a knob whose prior could not be CAPTURED is never MUTATED. Nothing
#     is weakened that cannot be put back, so the unrestorable case does not arise.
#  2. REPORTING: each knob is tracked independently. Every knob that was written and
#     NOT restored is named in a warning carrying a COMPLETE runnable command, and the
#     affirmative line is printed only for the knobs that genuinely went back.
#
# `SYSCTLS_WRITTEN` holds one `<sysctl-name>=<prior-value>` per line — exactly the
# knobs that were written, each paired with the value to put back. It is the single
# source of truth for the restore, so a knob cannot be mutated without being
# enrolled, and a knob cannot be "restored" that was never touched.
PARANOID_PRIOR=""
KPTR_PRIOR=""
SYSCTLS_MUTATED=0
SYSCTLS_WRITTEN=""

restore_sysctls() {
  [[ "$SYSCTLS_MUTATED" == "1" ]] || return 0
  SYSCTLS_MUTATED=0
  local entry knob prior
  local -a restored=() failed=()
  while IFS= read -r entry; do
    [[ -n "$entry" ]] || continue
    knob="${entry%%=*}"
    prior="${entry#*=}"
    # An `if` CONDITION: `set -e` never acts on one, so a failing knob neither aborts
    # the handler nor orphans the knobs after it in the list.
    if sudo -n sysctl -w "$knob=$prior" >/dev/null 2>&1; then
      restored+=("$knob=$prior")
    else
      failed+=("$knob=$prior")
    fi
  done <<<"$SYSCTLS_WRITTEN"
  if [[ "${#restored[@]}" -gt 0 ]]; then
    echo "restored host sysctls: ${restored[*]}" >&2
  fi
  if [[ "${#failed[@]}" -gt 0 ]]; then
    # Loud and per-knob, because the host is left weakened and only the operator can
    # fix it — and because a PARTIAL restore used to print the affirmative line.
    echo "WARNING: ${#failed[@]} host sysctl(s) this rig WEAKENED could not be restored:" >&2
    echo "         ${failed[*]}" >&2
    echo "         This host is still relaxed. Restore it by hand — complete command:" >&2
    echo "           sudo sysctl -w ${failed[*]}" >&2
    if [[ "${#restored[@]}" -gt 0 ]]; then
      echo "         (${#restored[@]} other knob(s) WERE restored: ${restored[*]} — this" >&2
      echo "          run was a PARTIAL restore, not a successful one.)" >&2
    fi
  fi
  # Cleanup may never fail the run: an empty `failed` array would otherwise make the
  # last `[[ ]]` test the function's exit status.
  return 0
}

# CAPTURE BEFORE MUTATE, and NEVER MUTATE WHAT WAS NOT CAPTURED (issue #3272,
# finding 3 + review round 1's B3 root cause).
#
# The prior values are read first — both knobs, including `kptr_restrict`, which the
# mutation also relaxes and which the pre-#3272 code never mentioned again. Round 1
# then found the remaining half: `KPTR_PRIOR` fell back to `""` when
# `/proc/sys/kernel/kptr_restrict` was unreadable, and the mutation wrote
# `kernel.kptr_restrict=0` ANYWAY, after which the restore's `[[ -n "$KPTR_PRIOR" ]]`
# guard silently skipped it. So the failure mode was: weaken a knob, be unable to put
# it back, and report success.
#
# A knob whose prior cannot be READ is therefore never WRITTEN. That is the honest
# ordering — the ability to restore is a PRECONDITION of weakening, not a best-effort
# afterthought — and it is what makes `restore_sysctls`'s enrollment list total: every
# entry in `SYSCTLS_WRITTEN` has a value to go back to, by construction.
#
# `perf_event_paranoid` is REQUIRED (CPU-wide counting is impossible without it), so an
# unreadable prior there is FATAL. `kptr_restrict` is a nice-to-have (it only affects
# kernel-symbol resolution in a `perf` report), so an unreadable prior there SKIPS the
# knob with a note rather than failing the run — a measurement that can still be taken
# correctly should be, and leaving a knob alone is always safe.
read_sysctl_prior() { # read_sysctl_prior <proc-path> — echo the value, rc=1 if unreadable
  local path="$1" value
  [[ -r "$path" ]] || return 1
  value="$(cat "$path" 2>/dev/null)" || return 1
  [[ -n "$value" ]] || return 1
  printf '%s' "$value"
}

# Enroll a knob as WRITTEN with the prior to restore it to. Called BEFORE the write,
# because a `sysctl -w` that sets the first knob and fails on the second has still
# mutated the host — an enrollment after the write would skip the half that landed.
enroll_sysctl() { # enroll_sysctl <sysctl-name> <prior-value>
  SYSCTLS_WRITTEN+="${SYSCTLS_WRITTEN:+$'\n'}$1=$2"
  SYSCTLS_MUTATED=1
}

# Weaken the two knobs CPU-wide counting needs, enrolling each for restore first.
# A no-op when `perf_event_paranoid` is already -1: nothing to weaken, nothing to
# restore, and `SYSCTLS_MUTATED` stays 0 so the exit handler issues no `sysctl`.
#
# The caller MUST have registered `restore_sysctls` in a trap before calling this — the
# window between the write and the trap being armed is the one interval in which a
# signal could leave the host weakened.
relax_perf_sysctls() {
  local paranoid
  paranoid="$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo "")"
  [[ "$paranoid" != "-1" ]] || return 0
  if ! PARANOID_PRIOR="$(read_sysctl_prior /proc/sys/kernel/perf_event_paranoid)"; then
    echo "FATAL: /proc/sys/kernel/perf_event_paranoid is not readable, so its prior value" >&2
    echo "       cannot be captured — and this rig will not weaken a host knob it would be" >&2
    echo "       unable to put back. Weakening something unrestorable and then reporting a" >&2
    echo "       successful restore is the defect this ordering exists to prevent (#3272)." >&2
    exit 2
  fi
  echo "perf_event_paranoid is $paranoid; CPU-wide counting needs -1. Trying sudo -n…"
  # A list, so a knob whose prior could not be read is simply not in it.
  local -a writes=("kernel.perf_event_paranoid=-1")
  enroll_sysctl kernel.perf_event_paranoid "$PARANOID_PRIOR"
  if KPTR_PRIOR="$(read_sysctl_prior /proc/sys/kernel/kptr_restrict)"; then
    writes+=("kernel.kptr_restrict=0")
    enroll_sysctl kernel.kptr_restrict "$KPTR_PRIOR"
  else
    KPTR_PRIOR=""
    echo "  NOTE: /proc/sys/kernel/kptr_restrict is unreadable, so it is left ALONE." >&2
    echo "        This rig never weakens a knob whose prior it could not capture — it" >&2
    echo "        would have no value to restore, and a knob left weakened forever is" >&2
    echo "        worse than a perf report without kernel symbol names (#3272)." >&2  # perf-lint-allow: prose
  fi
  echo "  (prior values captured for restore on exit: ${SYSCTLS_WRITTEN//$'\n'/ })"
  sudo -n sysctl -w "${writes[@]}" >/dev/null || {
    echo "FATAL: cannot set kernel.perf_event_paranoid=-1 (needed for perf stat -C)." >&2  # perf-lint-allow: a diagnostic STRING
    exit 2
  }
}
