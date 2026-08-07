#!/usr/bin/env bash
# test_ws0_host_state_guards.sh — the HOST STATE the WS0 rig mutates, and the guarantee
# that it puts it back (issue #3272 finding 3, hardened by review rounds 1 and 3).
#
# Split out of `test_ws0_report_guards.sh` under the campsite rule (test target ~1500
# lines), along the same responsibility seam the rig itself follows: `lib-host-state.sh` is
# the only part of the rig that changes anything OUTSIDE its own process tree, and it is the
# part whose failure is SECURITY-ADJACENT rather than a wrong number. The reporter tests ask
# what the rig does with observations; this file asks what the rig does to the BOX.
#
# What it covers, and why each was a real defect:
#
#   * the rig weakens `kernel.perf_event_paranoid` and `kernel.kptr_restrict` and used to
#     NEVER put them back — its only trap was `trap stop_server EXIT`, so a success, a
#     FATAL and a Ctrl-C all left the host less hardened than the rig found it,
#     permanently, with nothing in the output saying so;
#   * the first fix of that was itself PARTIAL (review round 1's B3): the success/warning
#     split keyed on "was ANYTHING restored", so a partial restore printed the affirmative
#     line and NO warning. Both halves are per-knob now, and the ROOT CAUSE is closed too —
#     a knob whose prior could not be captured is never mutated;
#   * the restore must fire on the SIGNALS, not only `EXIT`: `EXIT` does not run for
#     SIGINT/SIGTERM/SIGHUP while a foreground child (a 45s `perf stat` leg) is running, and
#     Ctrl-C during a step is the single most likely way this rig ends.
#
# Per #3249 (a hardcoded `_PERF_STATE="ok"` survived 118/118 tests) the bar is "OBSERVED TO
# FIRE": the structural greps pin the SHAPE, and the behavioural cases drive the SHIPPED
# `restore_sysctls` — sourced, never re-implemented — against a RECORDING `sudo` shim, plus a
# real SIGINT probe on the driver's own trap wiring.
#
# Hermetic: no privileged call ever happens, no host knob is touched, and the exact
# `sysctl -w` argv the handler WOULD issue is asserted instead.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DRIVER="$REPO_ROOT/scripts/perf/ws0-baseline.sh"
# The host-state subsystem the driver sources: the sysctl capture/mutate/restore that is the
# only part of the rig changing anything outside its own process tree. Driven HERE as the
# SHIPPED implementation, sourced rather than re-extracted, so a divergence between what is
# tested and what runs cannot exist.
HOST_STATE="$REPO_ROOT/scripts/perf/lib-host-state.sh"

fails=0
# `checks` counts what actually RAN, so the floor at the end can see a block that silently
# never executed (#3272 review round 3 nit).
checks=0
pass() { checks=$((checks + 1)); echo "ok   - $1"; }
fail() { checks=$((checks + 1)); echo "FAIL - $1"; fails=$((fails + 1)); }

[ -f "$DRIVER" ] || { echo "FAIL - missing $DRIVER"; exit 1; }
[ -f "$HOST_STATE" ] || { echo "FAIL - missing $HOST_STATE"; exit 1; }

TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

# ==========================================================================
# #3272 finding 3 — the driver RESTORES the host sysctls it mutates
# ==========================================================================
# NON-VACUITY: the pre-fix driver ran
#     sudo -n sysctl -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0
# and its ONLY trap was `trap stop_server EXIT`, so every run — success, FATAL or
# Ctrl-C — left the host's perf hardening weakened. `grep -c 'kptr_restrict'` on the
# pre-fix file finds exactly ONE occurrence (the weakening), and none in any trap.
#
# Structural, because the behaviour needs root: the restore must be REGISTERED on
# EXIT **and** on the signals, and it must be part of the same trap that stops the
# server rather than replacing it (a second bare `trap ... EXIT` would silently
# discard the first).
if awk '/^trap /' "$DRIVER" | grep -q 'INT TERM HUP'; then
  pass "the driver traps INT/TERM/HUP, not only EXIT (a Ctrl-C used to skip cleanup)"
else
  fail "the driver must trap INT/TERM/HUP as well as EXIT"
fi
if [ "$(grep -c '^trap ' "$DRIVER")" -eq 1 ]; then
  pass "there is exactly ONE top-level trap registration (a second would discard the first)"
else
  fail "multiple top-level 'trap' lines: a later bare EXIT trap discards the earlier one"
fi
if grep -q 'restore_sysctls' "$DRIVER" \
  && awk '/^on_exit\(\)/,/^}/' "$DRIVER" | grep -q 'restore_sysctls' \
  && awk '/^on_exit\(\)/,/^}/' "$DRIVER" | grep -q 'stop_server'; then
  pass "the single exit handler runs BOTH stop_server and restore_sysctls"
else
  fail "the exit handler must run stop_server AND restore_sysctls"
fi
# The prior values must be CAPTURED BEFORE the mutation, or there is nothing to
# restore to: assert the capture precedes the `sysctl -w` in file order.
cap_line=$(grep -n 'PARANOID_PRIOR=' "$HOST_STATE" | head -1 | cut -d: -f1)
mut_line=$(grep -n 'sysctl -w "\${writes\[@\]}"' "$HOST_STATE" | head -1 | cut -d: -f1)
if [ -n "$cap_line" ] && [ -n "$mut_line" ] && [ "$cap_line" -lt "$mut_line" ]; then
  pass "the prior sysctl values are captured BEFORE the mutation (line $cap_line < $mut_line)"
else
  fail "prior values must be captured before mutating (capture=$cap_line mutate=$mut_line)"
fi
# Both sysctls the driver weakens must be ENROLLED for restore — not just the one in
# the message. The enrollment list is what `restore_sysctls` iterates.
for knob in perf_event_paranoid kptr_restrict; do
  if grep -q "enroll_sysctl kernel.$knob" "$HOST_STATE"; then
    pass "kernel.$knob is enrolled for restore where it is weakened"
  else
    fail "kernel.$knob must be enrolled for restore (the driver weakens it)"
  fi
done
# The restore must be IDEMPOTENT and must never fail the run: it is cleanup, and a
# cleanup that can exit non-zero turns a successful measurement into a failed one.
if awk '/^restore_sysctls\(\)/,/^}/' "$HOST_STATE" | grep -q 'SYSCTLS_MUTATED' \
  && awk '/^restore_sysctls\(\)/,/^}/' "$HOST_STATE" | grep -q '^  return 0$'; then
  pass "restore_sysctls is guarded by a mutated-flag and returns 0 unconditionally"
else
  fail "restore_sysctls must be flag-guarded (idempotent) and end in an explicit return 0"
fi

# ---- BEHAVIOURAL, not merely structural -----------------------------------
# The greps above pin the SHAPE; per #3249 (hardcoding `_PERF_STATE="ok"` survived
# 118/118 tests) shape is not evidence that the thing FIRES. The real restore needs
# root, so the functions are extracted verbatim from the driver and run against a
# RECORDING `sudo` shim: no privileged call ever happens, and the exact
# `sysctl -w` argv the handler would issue is asserted. Hermetic, sub-second.
#
# `sudo_ok` selects which knobs the shim lets through, so the PARTIAL-restore case
# (#3272 review B3) can be driven: `paranoid` = only perf_event_paranoid succeeds.
sysctl_probe() { # sysctl_probe <case> <enrollment-lines> [sudo_ok: all|none|paranoid]
  local case_name="$1" written="$2" sudo_ok="${3:-all}"
  local calls="$TMP/sysctl-calls-$1.txt" out="$TMP/sysctl-out-$1.txt"
  : > "$calls"
  (
    set -uo pipefail
    sudo() {
      printf '%s\n' "$*" >> "$calls"
      case "$sudo_ok" in
        all)      return 0 ;;
        none)     return 1 ;;
        paranoid) [[ "$*" == *perf_event_paranoid* ]] ;;
      esac
    }
    # SOURCED, not re-implemented: this drives the shipped restore_sysctls, so the
    # test and the run can never be different code.
    # shellcheck disable=SC1090
    source "$HOST_STATE"
    SERVER_PID=""
    SYSCTLS_WRITTEN="$written"
    SYSCTLS_MUTATED=1
    case "$case_name" in
      never-mutated) SYSCTLS_MUTATED=0 ;;
      errexit)       set -e ;;   # cleanup must survive errexit
    esac
    # THE RETURN CODE OF `restore_sysctls`, captured on the NEXT statement (#3272
    # review). It used to be read after an intervening `case`, so `$?` was the
    # CASE's status — 0 for every non-`idempotent` case — and the "cleanup cannot
    # fail the run" half of the failing-sudo case measured nothing at all. The
    # second (idempotency) call is issued only after the code is banked.
    # stderr is NOT discarded: the affirmative/warning DIAGNOSTIC is half of what
    # B3 is about, so the probe must be able to read it.
    restore_sysctls
    printf 'RC=%s\n' "$?" > "$TMP/sysctl-rc-$case_name.txt"
    case "$case_name" in
      idempotent) : > "$calls"; restore_sysctls ;;
    esac
  ) >"$out" 2>&1
  cat "$calls"
}
probe_rc()  { cat "$TMP/sysctl-rc-$1.txt"; }
probe_out() { cat "$TMP/sysctl-out-$1.txt"; }

BOTH_KNOBS=$'kernel.perf_event_paranoid=2\nkernel.kptr_restrict=1'

got=$(sysctl_probe restores "$BOTH_KNOBS")
if grep -q 'sysctl -w kernel.perf_event_paranoid=2' <<<"$got" \
  && grep -q 'sysctl -w kernel.kptr_restrict=1' <<<"$got"; then
  pass "OBSERVED: restore_sysctls writes BOTH captured priors back (paranoid=2, kptr=1)"
else
  fail "restore_sysctls must write both captured priors back (recorded: $got)"
fi
# Pre-fix there was no restore at all, so this is the case that could not pass:
# the driver's only sysctl write was the WEAKENING one.
if [ "$(grep -c 'sysctl -w' <<<"$got")" -eq 2 ]; then
  pass "OBSERVED: exactly two restore writes, no stray sysctl mutation"
else
  fail "expected exactly 2 restore writes (recorded: $got)"
fi
# The affirmative line is printed, and it NAMES both knobs — the case the partial
# check below is distinguished from.
if grep -q 'restored host sysctls:.*perf_event_paranoid=2' <<<"$(probe_out restores)" \
  && grep -q 'kptr_restrict=1' <<<"$(probe_out restores)" \
  && ! grep -q 'WARNING' <<<"$(probe_out restores)"; then
  pass "OBSERVED: a FULL restore prints the affirmative line for both knobs and NO warning"
else
  fail "a full restore must print both knobs and no warning (out: $(probe_out restores))"
fi
if [ "$(probe_rc restores)" = "RC=0" ]; then
  pass "OBSERVED: restore_sysctls returns 0 on the success path (measured, not inferred)"
else
  fail "restore_sysctls must return 0 (got $(probe_rc restores))"
fi

got=$(sysctl_probe idempotent "$BOTH_KNOBS")
if [ -z "$got" ]; then
  pass "OBSERVED: a SECOND restore_sysctls call is a no-op (idempotent)"
else
  fail "restore_sysctls must be idempotent (second call recorded: $got)"
fi

got=$(sysctl_probe never-mutated "$BOTH_KNOBS")
if [ -z "$got" ]; then
  pass "OBSERVED: a run that never mutated the knobs issues NO sysctl on exit"
else
  fail "an unmutated run must not sysctl on exit (recorded: $got)"
fi

# A FAILING sudo must neither abort the handler under `set -e` nor stop it trying the
# SECOND knob — the failure mode that would leave kptr_restrict=0 behind forever. The
# rc is now read off `restore_sysctls` itself (see the probe), so the "cannot fail the
# run" half is genuinely measured.
got=$(sysctl_probe errexit "$BOTH_KNOBS" none)
if grep -q 'kernel.kptr_restrict=1' <<<"$got" && [ "$(probe_rc errexit)" = "RC=0" ]; then
  pass "OBSERVED: a FAILING sudo still attempts both knobs and cannot fail the run (rc=0)"
else
  fail "a failing sudo must not orphan the second knob or fail the run (recorded: $got / $(probe_rc errexit))"
fi
# ...and it must say so: a total failure is a WARNING with a complete runnable command,
# never the affirmative line.
out=$(probe_out errexit)
if grep -q 'WARNING' <<<"$out" \
  && grep -q 'sudo sysctl -w kernel.perf_event_paranoid=2 kernel.kptr_restrict=1' <<<"$out" \
  && ! grep -q '^restored host sysctls' <<<"$out"; then
  pass "OBSERVED: a TOTAL restore failure warns with a COMPLETE runnable sysctl command"
else
  fail "a total restore failure must warn with the full command (out: $out)"
fi

# ---- #3272 review B3: a PARTIAL restore must WARN, not report success -------
# NON-VACUITY. The first fix of finding 3 keyed the success/warning split on "was
# ANYTHING restored":
#
#     if [[ "${#restored[@]}" -gt 0 ]]; then echo "restored host sysctls: …"
#     else echo "WARNING: …"; fi
#
# so a PARTIAL restore took the AFFIRMATIVE branch. MEASURED against that code with
# perf_event_paranoid restorable and kptr_restrict not: ONE `sysctl -w`, the line
# `restored host sysctls: perf_event_paranoid=2`, and NO warning — the operator told
# the host was restored while `kptr_restrict=0` was left behind permanently. That is
# finding 3's own defect in narrower form; both directions are asserted here.
got=$(sysctl_probe partial "$BOTH_KNOBS" paranoid)
out=$(probe_out partial)
if [ "$(grep -c 'sysctl -w' <<<"$got")" -eq 2 ]; then
  pass "OBSERVED: a partial restore still ATTEMPTS both knobs (the failure does not stop the loop)"
else
  fail "a partial restore must attempt both knobs (recorded: $got)"
fi
if grep -q 'WARNING' <<<"$out" && grep -q 'kernel.kptr_restrict=1' <<<"$out"; then
  pass "OBSERVED: a PARTIAL restore WARNS, naming the knob left weakened (pre-fix: silent)"
else
  fail "a partial restore must warn and name the unrestored knob (out: $out)"
fi
if grep -q 'sudo sysctl -w kernel.kptr_restrict=1' <<<"$out"; then
  pass "OBSERVED: the partial warning carries a COMPLETE runnable restoration command"
else
  fail "the partial warning must carry a runnable command (out: $out)"
fi
if grep -q 'PARTIAL restore, not a successful one' <<<"$out"; then
  pass "OBSERVED: the partial case says it is PARTIAL (pre-fix it read as a success)"
else
  fail "a partial restore must not read as a success (out: $out)"
fi
# The counted knob must be the one that actually went back — the affirmative half may
# not name a knob the sudo refused.
if grep -q 'restored host sysctls: kernel.perf_event_paranoid=2$' <<<"$out"; then
  pass "OBSERVED: the affirmative half names ONLY the knob that was genuinely restored"
else
  fail "the affirmative line must name only the restored knob (out: $out)"
fi

# ---- B3 ROOT CAUSE: a knob whose prior was not captured is never MUTATED ----
# The reporting fix above is the second half. The first is that the unrestorable case
# must not arise: `kptr_restrict` used to be WRITTEN even when its prior read as `""`
# (an unreadable /proc entry), which is what created a knob with nothing to restore
# to. Driven over the driver's own capture/enrollment functions with an injected
# unreadable path.
if bash -c '
  set -uo pipefail
  # shellcheck disable=SC1090
  source "'"$HOST_STATE"'"
  SYSCTLS_WRITTEN=""; SYSCTLS_MUTATED=0
  # An unreadable path yields rc=1, NOT an empty success — so the caller can branch.
  read_sysctl_prior /nonexistent/kptr_restrict >/dev/null 2>&1 \
    && { echo "an unreadable path returned SUCCESS"; exit 1; }
  # An EMPTY file is also a failed capture: "" is not a value to restore to.
  tmp=$(mktemp); : > "$tmp"
  read_sysctl_prior "$tmp" >/dev/null 2>&1 && { echo "an empty file read as a value"; exit 1; }
  rm -f "$tmp"
  # A readable one yields the value and enrolls exactly one line.
  tmp=$(mktemp); printf "2\n" > "$tmp"
  v=$(read_sysctl_prior "$tmp") || { echo "a readable path failed"; exit 1; }
  [ "$v" = "2" ] || { echo "wrong value: $v"; exit 1; }
  enroll_sysctl kernel.perf_event_paranoid "$v"
  [ "$SYSCTLS_WRITTEN" = "kernel.perf_event_paranoid=2" ] || { echo "bad enrollment: $SYSCTLS_WRITTEN"; exit 1; }
  [ "$SYSCTLS_MUTATED" = "1" ] || { echo "enrollment did not set the mutated flag"; exit 1; }
  rm -f "$tmp"
' >/dev/null 2>&1; then
  pass "OBSERVED: read_sysctl_prior FAILS on an unreadable/empty prior, and enrollment pairs knob+prior"
else
  fail "read_sysctl_prior must fail-closed on an unreadable or empty prior"
fi
# And the driver must WIRE that: the kptr write is inside the successful-capture
# branch, so an unreadable prior leaves the knob alone rather than weakening it.
if awk '/^  if KPTR_PRIOR=/,/^  fi$/' "$HOST_STATE" | grep -q 'kernel.kptr_restrict=0' \
  && awk '/^  if KPTR_PRIOR=/,/^  fi$/' "$HOST_STATE" | grep -q 'left ALONE'; then
  pass "the driver weakens kptr_restrict ONLY inside the successful-capture branch"
else
  fail "the kptr_restrict write must be gated on its prior having been captured"
fi
# An unreadable perf_event_paranoid prior is FATAL rather than a silent weakening: it
# is the knob the measurement REQUIRES, so there is no correct run without it.
if grep -q 'if ! PARANOID_PRIOR=' "$HOST_STATE" \
  && awk '/^  if ! PARANOID_PRIOR=/,/^  fi$/' "$HOST_STATE" | grep -q 'exit 2'; then
  pass "an unreadable perf_event_paranoid prior is FATAL (never weakened unrestorably)"
else
  fail "an unreadable perf_event_paranoid prior must be fatal"
fi

# The signal path end-to-end: a driver-shaped script carrying the driver's OWN
# on_exit/trap wiring must run the restore when it is SIGINTed mid-work. `EXIT`
# alone does not fire for SIGINT while a foreground child is running, which is how
# a Ctrl-C during a 45s perf leg used to skip cleanup entirely.
cat > "$TMP/trap-probe.sh" <<PROBE
set -euo pipefail
MARK="$TMP/trap-fired.txt"
SERVER_PID=""
SYSCTLS_MUTATED=1
PARANOID_PRIOR=2
KPTR_PRIOR=1
stop_server() { :; }
restore_sysctls() { printf 'restored\n' >> "\$MARK"; SYSCTLS_MUTATED=0; }
$(awk '/^on_exit\(\)/,/^}/' "$DRIVER")
$(grep '^trap on_exit' "$DRIVER")
printf 'ready\n' > "$TMP/probe-ready.txt"
sleep 30
PROBE
rm -f "$TMP/trap-fired.txt" "$TMP/probe-ready.txt"
bash "$TMP/trap-probe.sh" >/dev/null 2>&1 &
probe_pid=$!
for _ in $(seq 1 50); do [ -f "$TMP/probe-ready.txt" ] && break; sleep 0.1; done
kill -INT "$probe_pid" 2>/dev/null || true
wait "$probe_pid" 2>/dev/null; probe_rc=$?
if [ -f "$TMP/trap-fired.txt" ]; then
  pass "OBSERVED: the driver's trap wiring runs the restore on SIGINT (rc=$probe_rc)"
else
  fail "a SIGINT must reach restore_sysctls through the driver's trap (rc=$probe_rc)"
fi
# Same wiring, ordinary exit — the handler must not be signal-only.
cat > "$TMP/trap-probe-exit.sh" <<PROBE
set -euo pipefail
MARK="$TMP/trap-fired-exit.txt"
SERVER_PID=""
SYSCTLS_MUTATED=1
stop_server() { :; }
restore_sysctls() { printf 'restored\n' >> "\$MARK"; }
$(awk '/^on_exit\(\)/,/^}/' "$DRIVER")
$(grep '^trap on_exit' "$DRIVER")
exit 7
PROBE
rm -f "$TMP/trap-fired-exit.txt"
bash "$TMP/trap-probe-exit.sh" >/dev/null 2>&1; exit_rc=$?
if [ -f "$TMP/trap-fired-exit.txt" ] && [ "$exit_rc" -eq 7 ]; then
  pass "OBSERVED: the handler also runs on a normal FATAL exit and PRESERVES its code (7)"
else
  fail "the handler must run on a normal exit and preserve the exit code (rc=$exit_rc)"
fi


# ==========================================================================
# A MINIMUM CHECK COUNT, because `set -uo pipefail` carries no `-e` (#3272 round 3 nit)
# ==========================================================================
# Without `-e` a block that silently never executes LOWERS the check count and registers NO
# failure, and the gate reads only the exit code. The floor is the suite-level `0/0` guard:
# deliberately BELOW the current count (adding a case must not red the suite) and far above
# zero.
MIN_CHECKS=22
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would lower the count with no failure"
  echo "       registered, and the gate reads only the exit code (#3272 round 3)."
  exit 1
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "ws0 host-state guards: all $checks checks passed"
  exit 0
fi
echo "ws0 host-state guards: $fails of $checks check(s) FAILED"
exit 1
