#!/usr/bin/env bash
# test_gate_detached.sh — pins the #3473 AC2 mechanism and the AC3 fix.
#
# Two things are asserted, and the first is the reason the second exists:
#
#   (1) THE MECHANISM. Background work dies with its launcher because it INHERITS the
#       launcher's cgroup, and a `KillMode=control-group` teardown signals every task in
#       that cgroup. Detaching from the terminal, the process group and the session
#       (`nohup`, `setsid`, closed fds, ppid 1) does NOT help, because cgroup membership
#       is inherited across fork and cannot be shed that way. Section 3 demonstrates
#       both halves on a cgroup this test creates and destroys itself.
#
#       This is pinned as a TEST rather than left as prose because it is the load-bearing
#       premise of the fix: if a future systemd/tmux configuration changed KillMode, or
#       gave panes a delegated cgroup, the fix would be solving a problem that no longer
#       exists in that shape — and we should be told, not discover it during an incident.
#
#   (2) THE FIX. scripts/flow/gate-detached.sh must put the gate in a cgroup that is NOT
#       a descendant of the caller's, must forward the caller's environment (a transient
#       systemd unit does not inherit it), and must REFUSE rather than silently fall back
#       to a session-scoped launch where it cannot deliver that.
#
# SKIP-aware, loudly: sections 3 and 4 need a working user systemd manager. On a host
# without one they record SKIP with a reason — never a silent pass, and never a FAIL,
# since the absence is a property of the host and not of the change.
set -uo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
LAUNCHER="$REPO_ROOT/scripts/flow/gate-detached.sh"
TMP=$(mktemp -d "${TMPDIR:-/tmp}/gate-detached-test.XXXXXX")
UNITS_FILE="$TMP/units"
: > "$UNITS_FILE"

# A PID IS NOT AN IDENTITY (roborev job 204). These suites deliberately kill processes long before
# cleanup runs, and the kernel reuses pids — so an unverified `kill` at cleanup can signal an
# unrelated same-user process, including a CONCURRENT GATE on this box. Same failure as killing by
# pattern: the selector describes what a process is, not whose it is.
#
# So every recorded pid carries the start identity it had when we started it, and cleanup signals
# only on a MATCH. An identity that cannot be read means DO NOT SIGNAL: a leaked helper under $TMP
# is harmless (removed with the directory; a beater self-terminates with its gate), whereas killing
# a stranger is not. The conservative branch is chosen by consequence, not by convenience.
_pid_identity() {  # <pid> -> "proc:<starttime>" | "ps:<lstart>" | "" if unreadable
  local raw rest ls
  raw=$(cat "/proc/$1/stat" 2>/dev/null)
  if [ -n "$raw" ]; then
    rest="${raw##*) }"
    # shellcheck disable=SC2086  # deliberate word-split into positional params
    set -- $rest
    if [ $# -ge 20 ]; then printf 'proc:%s' "${20}"; return 0; fi
  fi
  ls=$(ps -o lstart= -p "$1" 2>/dev/null | tr -s ' ')
  [ -n "$ls" ] && { printf 'ps:%s' "$ls"; return 0; }
  return 1
}
# remember_pid <pid> — record it WITH its identity, in ONE file that cleanup actually reads.
remember_pid() {
  local id
  id=$(_pid_identity "$1" 2>/dev/null || true)
  printf '%s\t%s\n' "$1" "$id" >> "$TMP/tracked-pids"
}
# kill_tracked <signal> — signal only pids whose identity still matches what we recorded.
kill_tracked() {
  local sig="$1" pid want now
  [ -f "$TMP/tracked-pids" ] || return 0
  while IFS=$'\t' read -r pid want; do
    case "$pid" in ''|*[!0-9]*) continue ;; esac
    [ -n "$want" ] || continue          # never recorded => cannot verify => do not signal
    now=$(_pid_identity "$pid" 2>/dev/null || true)
    [ -n "$now" ] || continue           # gone, or unreadable => nothing to signal safely
    [ "$now" = "$want" ] || continue    # pid reused: this is SOMEONE ELSE
    kill "$sig" "$pid" 2>/dev/null || true
  done < "$TMP/tracked-pids"
}

# shellcheck disable=SC2317
cleanup() {
  local u p
  for u in $(cat "$UNITS_FILE" 2>/dev/null); do
    systemctl --user stop "$u" >/dev/null 2>&1
    systemctl --user reset-failed "$u" >/dev/null 2>&1
  done
  kill_tracked -9
  rm -rf "$TMP"
}
trap cleanup EXIT

pass=0; fail=0; skip=0
# Counters live in the TOP-LEVEL shell only — never wrap a case in `( … )`, or the
# increments are discarded and the suite reports failed:0 while printing FAILs.
ok()   { pass=$((pass+1)); printf 'ok   %s\n' "$1"; }
bad()  { fail=$((fail+1)); printf 'FAIL %s\n' "$1"; [ $# -ge 2 ] && printf '     %s\n' "$2"; }
skipc(){ skip=$((skip+1)); printf 'SKIP %s — %s\n' "$1" "$2"; }

HAVE_SYSTEMD=no
if command -v systemd-run >/dev/null 2>&1 && systemd-run --user --scope --quiet true >/dev/null 2>&1; then
  HAVE_SYSTEMD=yes
fi

# THE LAUNCHER HAS A SECOND PRECONDITION NOW (roborev job 208): `Linger=yes`. `HAVE_SYSTEMD` asks
# only whether a user manager works, so on a host with a manager but lingering DISABLED every
# launch-dependent case would run and fail against the linger refusal — reporting a broken launcher
# when the host is simply unprepared, and telling us nothing about the behaviour each case is about.
#
# So if lingering is not affirmatively enabled, put a `loginctl` stub answering `yes` on PATH for the
# whole suite. That is not hiding the precondition: 4b.115-4b.118 test it explicitly with their own
# stubs (no / no-answer / cannot-answer / yes), which is a STRONGER test than this host's
# configuration would give, because it exercises all four states on any box.
if [ "$HAVE_SYSTEMD" = yes ] \
   && [ "$(loginctl show-user "$(id -un)" -p Linger --value 2>/dev/null || true)" != yes ]; then
  LINGER_STUB="$TMP/linger-yes"
  mkdir -p "$LINGER_STUB"
  printf '#!/usr/bin/env bash\nprintf "yes\\n"\n' > "$LINGER_STUB/loginctl"
  chmod +x "$LINGER_STUB/loginctl"
  PATH="$LINGER_STUB:$PATH"; export PATH
  echo "NOTE: lingering is not enabled on this host; a loginctl stub answering 'yes' is on PATH so"
  echo "      launch-dependent cases test the launcher rather than the host. 4b.115-4b.118 test the"
  echo "      precondition itself, with their own stubs."
fi

echo "=== section 1: usage ==="
out=$(bash "$LAUNCHER" --help 2>&1); rc=$?
[ "$rc" = 0 ] && printf '%s' "$out" | grep -q 'gate-detached' \
  && ok "1.1 --help exits 0 and describes itself" || bad "1.1 --help exits 0 and describes itself" "rc=$rc"

echo "=== section 2: the refusal path is a NAMED refusal, never a silent fallback ==="
# A caller who asked for a detached gate and got a session-scoped one would believe the
# gate was protected when it was not — the false-assurance direction this issue is about.
# There are TWO ways the capability can be missing and BOTH must refuse: the binary is
# absent, or it is present but the user manager does not work (a container, a box with
# no user@.service). A launcher that only checked the first would sail past the second.
#
# Building a PATH that lacks systemd-run but still has bash needs a curated bin dir:
# systemd-run and bash both live in /usr/bin, so no PATH filter can drop one and keep
# the other. We symlink exactly the tools the launcher touches before its capability
# check, which also documents that surface.
fakebin="$TMP/nobin"; mkdir -p "$fakebin"
for t in bash dirname cat sed env date mktemp; do
  src=$(command -v "$t" 2>/dev/null) && ln -sf "$src" "$fakebin/$t"
done
out=$(PATH="$fakebin" "$fakebin/bash" "$LAUNCHER" --summary "$TMP/never.txt" -- --only file-size 2>&1); rc=$?
if [ "$rc" = 69 ]; then ok "2.1 systemd-run ABSENT => exit 69 (EX_UNAVAILABLE)"
else bad "2.1 systemd-run ABSENT => exit 69 (EX_UNAVAILABLE)" "rc=$rc: $out"; fi
printf '%s' "$out" | grep -q 'WILL die with the session' \
  && ok "2.2 the refusal states the consequence" || bad "2.2 the refusal states the consequence" "$out"
printf '%s' "$out" | grep -qi 'ssh' \
  && ok "2.3 the refusal names the alternative" || bad "2.3 the refusal names the alternative" "$out"
[ ! -f "$TMP/never.txt" ] \
  && ok "2.4 a refused launch starts no gate" || bad "2.4 a refused launch starts no gate" "summary exists"
# Present but non-functional: the probe must be a real invocation, not a `command -v`.
brokenbin="$TMP/brokenbin"; mkdir -p "$brokenbin"
printf '#!/bin/sh\nexit 1\n' > "$brokenbin/systemd-run"; chmod +x "$brokenbin/systemd-run"
out=$(PATH="$brokenbin:$PATH" bash "$LAUNCHER" --summary "$TMP/never2.txt" -- --only file-size 2>&1); rc=$?
if [ "$rc" = 69 ]; then ok "2.5 systemd-run PRESENT but broken => exit 69"
else bad "2.5 systemd-run PRESENT but broken => exit 69" "rc=$rc: $out"; fi
printf '%s' "$out" | grep -q "does not work here" \
  && ok "2.6 the broken-manager refusal is textually distinct from the absent one" \
  || bad "2.6 the broken-manager refusal is textually distinct from the absent one" "$out"
[ ! -f "$TMP/never2.txt" ] \
  && ok "2.7 a refused launch starts no gate (broken manager)" || bad "2.7 a refused launch starts no gate (broken manager)" "summary exists"

echo "=== section 3: THE MECHANISM — cgroup membership decides, detachment does not ==="
if [ "$HAVE_SYSTEMD" != yes ]; then
  skipc "3.x cgroup teardown demonstration" "no working 'systemd-run --user' on this host"
elif [ ! -d /proc/1 ]; then
  skipc "3.x cgroup teardown demonstration" "no /proc on this host"
else
  # A ticker that appends a line every second and traps every catchable signal, so its
  # log distinguishes a signalled death from an uncatchable one.
  cat > "$TMP/ticker.sh" <<'TICK'
#!/usr/bin/env bash
log="$1"
echo "start pid=$$ pgid=$(ps -o pgid= -p $$|tr -d ' ') sid=$(ps -o sid= -p $$|tr -d ' ')" >> "$log"
echo "cgroup=$(cat /proc/self/cgroup)" >> "$log"
for s in HUP TERM INT QUIT; do trap "echo \"SIGNAL $s\" >> '$log'; exit 100" "$s"; done
i=0; while [ "$i" -lt 300 ]; do i=$((i+1)); echo "tick $i" >> "$log"; sleep 1; done
TICK
  # The cage stands in for a lane pane: the main process of a KillMode=control-group
  # cgroup. It launches one ticker that INHERITS its cgroup (using every detachment
  # idiom available) and one moved into a scope of its own.
  cat > "$TMP/cage.sh" <<CAGE
#!/usr/bin/env bash
setsid nohup bash "$TMP/ticker.sh" "$TMP/in.log" </dev/null >/dev/null 2>&1 &
systemd-run --user --unit=cqlite-t3473-out-$$ --collect --quiet bash "$TMP/ticker.sh" "$TMP/out.log"
echo "cqlite-t3473-out-$$" >> "$UNITS_FILE"
sleep 300
CAGE
  cage_unit="cqlite-t3473-cage-$$"
  echo "$cage_unit" >> "$UNITS_FILE"
  if ! systemd-run --user --unit="$cage_unit" --collect --quiet bash "$TMP/cage.sh"; then
    bad "3.0 start the cage cgroup" "systemd-run failed"
  else
    ok "3.0 start the cage cgroup"
    for ((_i_=0; _i_<60; _i_++)); do
      [ -s "$TMP/in.log" ] && [ -s "$TMP/out.log" ] && break
      sleep 1
    done
    in_cg=$(sed -n 's/^cgroup=//p' "$TMP/in.log" | head -1)
    out_cg=$(sed -n 's/^cgroup=//p' "$TMP/out.log" | head -1)
    in_pid=$(sed -n 's/^start pid=\([0-9]*\).*/\1/p' "$TMP/in.log" | head -1)
    out_pid=$(sed -n 's/^start pid=\([0-9]*\).*/\1/p' "$TMP/out.log" | head -1)
    [ -n "$in_pid" ] && remember_pid "$in_pid"
    [ -n "$out_pid" ] && remember_pid "$out_pid"

    if [ -z "$in_pid" ] || [ -z "$out_pid" ]; then
      bad "3.1 both tickers start" "in_pid='$in_pid' out_pid='$out_pid' (in.log/out.log did not populate)"
    else
      ok "3.1 both tickers start"
      # The premise of the whole case: they really are in different cgroups, and the
      # inheriting one really did detach at the session/process-group level.
      [ -n "$in_cg" ] && [ -n "$out_cg" ] && [ "$in_cg" != "$out_cg" ] \
        && ok "3.2 the two tickers are in DIFFERENT cgroups" \
        || bad "3.2 the two tickers are in DIFFERENT cgroups" "in='$in_cg' out='$out_cg'"
      in_sid=$(sed -n 's/.*sid=\([0-9]*\).*/\1/p' "$TMP/in.log" | head -1)
      [ "$in_sid" = "$in_pid" ] \
        && ok "3.3 the inheriting ticker DID detach (own session leader)" \
        || bad "3.3 the inheriting ticker DID detach (own session leader)" "sid=$in_sid pid=$in_pid"

      systemctl --user stop "$cage_unit" >/dev/null 2>&1
      # Bounded wait for the kill to land, then assert the split.
      for ((_i_=0; _i_<60; _i_++)); do
        kill -0 "$in_pid" 2>/dev/null || break
        sleep 1
      done
      if kill -0 "$in_pid" 2>/dev/null; then
        bad "3.4 cgroup teardown kills the INHERITING ticker despite setsid+nohup" "still alive"
      else
        ok "3.4 cgroup teardown kills the INHERITING ticker despite setsid+nohup"
      fi
      # The control, and the half that makes the fix work: same work, own cgroup, alive.
      if kill -0 "$out_pid" 2>/dev/null; then
        ok "3.5 the ticker in its OWN cgroup survives the teardown"
      else
        bad "3.5 the ticker in its OWN cgroup survives the teardown" "it died too — the fix's premise does not hold on this host"
      fi
      # And it keeps WORKING, not merely existing: a stopped-but-unreaped process would
      # satisfy kill -0 while producing nothing.
      before=$(wc -l < "$TMP/out.log" 2>/dev/null || echo 0)
      sleep 3
      after=$(wc -l < "$TMP/out.log" 2>/dev/null || echo 0)
      [ "$after" -gt "$before" ] \
        && ok "3.6 the surviving ticker is still doing work, not just resident" \
        || bad "3.6 the surviving ticker is still doing work, not just resident" "$before -> $after lines"
    fi
  fi
fi

echo "=== section 4: THE FIX — the gate lands outside the caller's cgroup, with its env ==="
if [ "$HAVE_SYSTEMD" != yes ]; then
  skipc "4.x detached gate launch" "no working 'systemd-run --user' on this host"
else
  our_cg=$(cat /proc/self/cgroup 2>/dev/null | sed 's|^0::||')
  summ="$TMP/detached-summary.txt"
  # AGENT_GATE_TEST_SCCACHE_STATE is an EXISTING gate test hook (#2641) whose effect is
  # visible in the SUMMARY's `sccache-health=` token. Using it means the env-forwarding
  # assertion needs no new seam in production code: set it here, and if it reaches the
  # detached unit the block says `sccache-health=na` instead of the detected value.
  out=$(AGENT_GATE_TEST_SCCACHE_STATE=absent CQLITE_T3473_MARKER=forwarded \
        bash "$LAUNCHER" --summary "$summ" --log "$TMP/detached.log" -- --only file-size 2>&1); rc=$?
  unit=$(printf '%s' "$out" | sed -n 's/^unit:  *//p')
  [ -n "$unit" ] && echo "$unit" >> "$UNITS_FILE"
  if [ "$rc" != 0 ] || [ -z "$unit" ]; then
    bad "4.1 the launcher starts a unit and reports it" "rc=$rc out=$out"
  else
    ok "4.1 the launcher starts a unit and reports it"
    gate_cg=$(printf '%s' "$out" | sed -n 's/^cgroup:  *//p')
    # THE property. Not "a different string" — NOT A DESCENDANT: a child cgroup of the
    # caller's would be torn down right along with it, so a prefix match must fail.
    if [ -z "$gate_cg" ]; then
      bad "4.2 the gate's cgroup is not a descendant of ours" "launcher reported no cgroup"
    elif [ -n "$our_cg" ] && [ "${gate_cg#"$our_cg"}" != "$gate_cg" ]; then
      bad "4.2 the gate's cgroup is not a descendant of ours" "gate '$gate_cg' is under ours '$our_cg'"
    else
      ok "4.2 the gate's cgroup is not a descendant of ours"
    fi
    printf '%s' "$out" | grep -qE '^env: *forwarded [1-9][0-9]* variables' \
      && ok "4.3 the launcher reports how many variables it forwarded" \
      || bad "4.3 the launcher reports how many variables it forwarded" "$out"
    # Wait for the gate to reach a verdict, using the mechanism under test.
    for ((_i_=0; _i_<60; _i_++)); do
      grep -qE '^RESULT: (PASS|FAIL|PARTIAL|ERROR|REFUSED)' "$summ" 2>/dev/null && break
      sleep 2
    done
    if ! grep -qE '^RESULT: ' "$summ" 2>/dev/null; then
      bad "4.4 the detached gate reaches a verdict" "no RESULT in $summ; log: $(tail -5 "$TMP/detached.log" 2>/dev/null)"
    else
      ok "4.4 the detached gate reaches a verdict"
      # Env forwarding, observed through the gate's own output rather than asserted at
      # the argv level: an argv assertion is not evidence that the value ARRIVED.
      if grep -q 'sccache-health=na' "$summ"; then
        ok "4.5 a caller env var REACHED the detached unit (sccache-health=na)"
      else
        bad "4.5 a caller env var REACHED the detached unit" "accelerators: $(grep -m1 '^accelerators:' "$summ")"
      fi
      # The summary must land where the CALLER asked, i.e. AGENT_GATE_SUMMARY_FILE was
      # forwarded and honoured — otherwise a detached gate is unreadable.
      grep -q "^summary-file: $summ$" "$summ" \
        && ok "4.6 the gate wrote the summary path the caller pinned" \
        || bad "4.6 the gate wrote the summary path the caller pinned" "$(grep -m1 '^summary-file:' "$summ")"
      # And the heartbeat rides along, at the documented sibling path.
      [ -f "$summ.heartbeat" ] \
        && ok "4.7 the detached gate published a heartbeat" \
        || bad "4.7 the detached gate published a heartbeat" "absent"
      bash "$REPO_ROOT/scripts/gate-liveness.sh" "$summ" >/dev/null 2>&1
      [ "$?" = 0 ] && ok "4.8 gate-liveness reads the detached run as COMPLETE" \
                   || bad "4.8 gate-liveness reads the detached run as COMPLETE" "rc=$?"
    fi
  fi
fi

echo "=== section 4b: an UNMONITORABLE gate is refused before it starts (job 160) ==="
# SKIP-gated on systemd like sections 3, 4 and 5 (roborev job 162, Medium). Without a working
# user systemd manager the launcher exits at its CAPABILITY check — before it ever reaches the
# summary-location checks these cases are about — so the expected diagnostics never appear and
# the cases failed deterministically on macOS and on any box with no user manager.
if [ "$HAVE_SYSTEMD" != yes ]; then
  skipc "4b.x unmonitorable-launch refusals" "no working 'systemd-run --user' on this host (the launcher refuses earlier, at the capability check)"
else
out=$(bash "$LAUNCHER" --summary /nonexistent-dir-3473/s.txt --log "$TMP/nx.log" -- --only file-size 2>&1); rc=$?
[ "$rc" != 0 ] && ok "4b.1 a nonexistent summary directory is refused (exit $rc)" \
               || bad "4b.1 a nonexistent summary directory is refused" "exit 0: $out"
printf '%s' "$out" | grep -q 'does not exist' \
  && ok "4b.2 the refusal names the cause" || bad "4b.2 the refusal names the cause" "$out"
if [ "$(id -u)" != 0 ]; then
  ro="$TMP/readonly"; mkdir -p "$ro"; chmod 500 "$ro"
  out=$(bash "$LAUNCHER" --summary "$ro/s.txt" --log "$TMP/ro.log" -- --only file-size 2>&1); rc=$?
  [ "$rc" != 0 ] && ok "4b.3 an unwritable summary directory is refused (exit $rc)" \
                 || bad "4b.3 an unwritable summary directory is refused" "exit 0: $out"
  printf '%s' "$out" | grep -qE 'cannot create a file|cannot RENAME' \
    && ok "4b.4 the refusal names the missing capability" || bad "4b.4 the refusal names the missing capability" "$out"
  chmod 700 "$ro"
else
  skipc "4b.3-4b.4 unwritable summary directory" "running as root (permissions do not deny root)"
fi
# THE CONTROL that matters most: the probe must not be destructive. Under #2874 the path
# could hold a LIVE PEER's summary block, and truncating it to test writability would cause
# exactly the data loss that contract exists to prevent — before the gate's own foreign
# run-id detection ever saw it. So a pre-existing file at the summary path must survive a
# REFUSED launch untouched.
peer="$TMP/peer-summary.txt"
printf '==== AGENT-GATE SUMMARY ====\nrun-id: peers-run\nRESULT: PASS\n==== END AGENT-GATE SUMMARY ====\n' > "$peer"
before=$(cat "$peer")
bash "$LAUNCHER" --summary "$peer" --log /nonexistent-dir-3473/nope.log -- --only file-size >/dev/null 2>&1
after=$(cat "$peer" 2>/dev/null)
[ "$before" = "$after" ] \
  && ok "4b.5 a refused launch leaves a pre-existing summary at that path UNTOUCHED" \
  || bad "4b.5 a refused launch leaves a pre-existing summary at that path UNTOUCHED" "the probe clobbered it"
# ...and no probe litter is left behind in either direction.
if ls "$TMP"/peer-summary.txt.heartbeat.tmp.* >/dev/null 2>&1; then
  bad "4b.6 the launch probe leaves no temp litter" "$(ls "$TMP"/peer-summary.txt.heartbeat.tmp.* 2>/dev/null)"
else
  ok "4b.6 the launch probe leaves no temp litter"
fi
# An EXISTING but unwritable summary file: directory permissions alone would pass this.
if [ "$(id -u)" != 0 ]; then
  roF="$TMP/ro-summary.txt"; : > "$roF"; chmod 400 "$roF"
  out=$(bash "$LAUNCHER" --summary "$roF" --log "$TMP/rof.log" -- --only file-size 2>&1); rc=$?
  [ "$rc" != 0 ] && ok "4b.7 an existing UNWRITABLE summary file is refused (exit $rc)" \
                 || bad "4b.7 an existing UNWRITABLE summary file is refused" "exit 0: $out"
  printf '%s' "$out" | grep -q 'NOT writable' \
    && ok "4b.8 the refusal names writability, not the directory" || bad "4b.8 the refusal names writability" "$out"
  chmod 600 "$roF"
else
  skipc "4b.7-4b.8 unwritable existing summary" "running as root"
fi
# A non-regular file at the summary path must be refused rather than written through.
fifo="$TMP/fifo-summary.txt"
if mkfifo "$fifo" 2>/dev/null; then
  out=$(bash "$LAUNCHER" --summary "$fifo" --log "$TMP/fifo.log" -- --only file-size 2>&1); rc=$?
  [ "$rc" != 0 ] && ok "4b.9 a non-regular file at the summary path is refused (exit $rc)" \
                 || bad "4b.9 a non-regular file at the summary path is refused" "exit 0: $out"
  rm -f "$fifo"
else
  skipc "4b.9 non-regular summary path" "mkfifo unavailable"
fi
# The heartbeat DESTINATION must be validated, not just rename-between-two-new-siblings
# (roborev job 164): a rename to a fresh name proves the directory allows renames, not that
# the beater can REPLACE $SUMMARY.heartbeat.
hbdir="$TMP/hbdest.txt"
mkdir -p "$hbdir.heartbeat"          # the destination is a DIRECTORY
out=$(bash "$LAUNCHER" --summary "$hbdir" --log "$TMP/hbd.log" -- --only file-size 2>&1); rc=$?
[ "$rc" != 0 ] && ok "4b.11 a DIRECTORY at the heartbeat destination is refused (exit $rc)" \
               || bad "4b.11 a DIRECTORY at the heartbeat destination is refused" "exit 0: $out"
printf '%s' "$out" | grep -q 'not a regular file' \
  && ok "4b.12 the refusal names the destination's file type" || bad "4b.12 the refusal names the file type" "$out"
rmdir "$hbdir.heartbeat" 2>/dev/null || true
# An existing heartbeat with mode 400 must still LAUNCH FINE — and this case is kept because
# it documents why the old append-probe was wrong in BOTH directions. POSIX takes rename and
# unlink permission from the DIRECTORY, not the file, so the beater's temp+rename replaces an
# unwritable file without difficulty. The append-probe therefore REFUSED a configuration that
# works (a false refusal) while still missing the sticky-directory case that does not (job
# 166). Verifying by outcome gets both right without modelling either.
if [ "$(id -u)" != 0 ]; then
  hbro="$TMP/hbro.txt"; : > "$hbro.heartbeat"; chmod 400 "$hbro.heartbeat"
  out=$(bash "$LAUNCHER" --summary "$hbro" --log "$TMP/hbro.log" -- --only file-size 2>&1); rc=$?
  unit=$(printf '%s' "$out" | sed -n 's/^unit:  *//p'); [ -n "$unit" ] && echo "$unit" >> "$UNITS_FILE"
  [ "$rc" = 0 ] && ok "4b.13 a mode-400 existing heartbeat still launches (rename permission is the DIRECTORY's)" \
               || bad "4b.13 a mode-400 existing heartbeat still launches" "rc=$rc: $out"
  chmod 600 "$hbro.heartbeat" 2>/dev/null || true
else
  skipc "4b.13 mode-400 existing heartbeat" "running as root"
fi
# The destination probe must be NON-DESTRUCTIVE too: under #2874 an existing heartbeat may be
# a live peer's beat, and our beater replaces it seconds after launch anyway — so clobbering
# it during a check that might then REFUSE would destroy data for no benefit.
peerhb="$TMP/peerhb.txt"
printf '==== AGENT-GATE HEARTBEAT ====\nrun-id: peer\nbeat-epoch: 1\n==== END AGENT-GATE HEARTBEAT ====\n' > "$peerhb.heartbeat"
hbbefore=$(cat "$peerhb.heartbeat")
bash "$LAUNCHER" --summary "$peerhb" --log /nonexistent-dir-3473/x.log -- --only file-size >/dev/null 2>&1
hbafter=$(cat "$peerhb.heartbeat" 2>/dev/null)
[ "$hbbefore" = "$hbafter" ] \
  && ok "4b.14 a refused launch leaves an existing heartbeat UNTOUCHED" \
  || bad "4b.14 a refused launch leaves an existing heartbeat UNTOUCHED" "the probe clobbered it"
# And when the destination does NOT exist, the probe proves real replacement and cleans up.
fresh="$TMP/fresh.txt"
out=$(bash "$LAUNCHER" --summary "$fresh" --log "$TMP/fresh.log" -- --only file-size 2>&1); rc=$?
unit=$(printf '%s' "$out" | sed -n 's/^unit:  *//p'); [ -n "$unit" ] && echo "$unit" >> "$UNITS_FILE"
[ "$rc" = 0 ] && ok "4b.15 a fresh destination launches (real replacement proven)" \
             || bad "4b.15 a fresh destination launches" "rc=$rc: $out"
if ls "$TMP"/fresh.txt.heartbeat.tmp.* >/dev/null 2>&1; then
  bad "4b.16 the destination probe leaves no temp litter" "$(ls "$TMP"/fresh.txt.heartbeat.tmp.* 2>/dev/null)"
else
  ok "4b.16 the destination probe leaves no temp litter"
fi

# VERIFY BY OUTCOME (roborev job 166): the launcher must require a real first beat rather than
# model permissions. Appending zero bytes proves write access to a FILE, not permission to
# REPLACE it — in a sticky directory a file owned by another user is appendable but not
# renameable-over, so the old probe passed while the beater would fail forever.
# Re-pointed (job 251): the message now reads "published no readable liveness ... within 20s, plus one
# confirmation of up to 65s where the clock domain is unproven", because the single fallback is allowed to
# block. Asserting the refusal's EXISTENCE plus the stop ACTION, rather than one phrasing, so a future
# rewording does not read as a missing check (the 11b.17e lesson).
if grep -q 'published no readable liveness' "$LAUNCHER" \
   && grep -q 'systemctl --user stop "$UNIT"' "$LAUNCHER"; then
  ok "4b.17 the launcher verifies a first heartbeat after launching"
else
  bad "4b.17 the launcher verifies a first heartbeat after launching" "not found"
fi
if grep -q 'systemctl --user stop "$UNIT"' "$LAUNCHER"; then
  ok "4b.18 ...and STOPS the unit rather than leaving an unmonitorable gate running"
else
  bad "4b.18 ...and STOPS the unit on failure" "not found"
fi
# The permission MODELLING must be gone, or the family it belongs to stays open.
if grep -q ': >> "$_hbdest"' "$LAUNCHER"; then
  bad "4b.19 the heartbeat append-probe (a permission model) is gone" "still present"
else
  ok "4b.19 the heartbeat append-probe (a permission model) is gone"
fi
# A directory the gate cannot write at all: refused, and nothing is left running.
if [ "$(id -u)" != 0 ]; then
  nd=$(mktemp -d); chmod 500 "$nd"
  out=$(bash "$LAUNCHER" --summary "$nd/s.txt" --log "$TMP/nd.log" -- --only file-size 2>&1); rc=$?
  [ "$rc" != 0 ] && ok "4b.20 an unwritable summary directory is refused end-to-end (exit $rc)" \
                 || bad "4b.20 an unwritable summary directory is refused end-to-end" "exit 0: $out"
  chmod 700 "$nd"; rm -rf "$nd"
else
  skipc "4b.20 unwritable summary directory" "running as root"
fi
# A gate that reaches a TERMINAL VERDICT without us observing a beat must NOT be refused:
# preflight refusals and very short --only runs legitimately finish that fast, and stopping
# them would be a false negative that kills a perfectly good gate.
# The property is unchanged; its implementation moved from a local grep to a delegated call to
# gate-liveness.sh (job 172), so this assert follows it there. Both halves must hold: the guard
# sits in the no-heartbeat fallback, and it is bound to the run we launched.
if grep -q 'if \[ "\$_hb_seen" -ne 1 \] && \[ -n "\$_new_rid" \]; then' "$LAUNCHER" \
   && grep -q 'gate-liveness.sh" "\$SUMMARY" --run-id "\$_new_rid"' "$LAUNCHER"; then
  ok "4b.21 an early terminal verdict is accepted (via the delegated, run-bound check)"
else
  bad "4b.21 an early terminal verdict is accepted (via the delegated, run-bound check)" "guard not found"
fi
# The gate's own re-exec markers must not be forwarded (job 166, Low): they would claim
# wrapped-ness the new unit does not have, so it would skip nice AND report itself wrapped.
if grep -q 'AGENT_GATE_WRAPPED|AGENT_GATE_WRAPPER) continue' "$LAUNCHER"; then
  ok "4b.22 the gate's own wrapper markers are excluded from env forwarding"
else
  bad "4b.22 the gate's own wrapper markers are excluded from env forwarding" "not in the deny-list"
fi

# roborev job 169: the post-launch check must be BOUND TO THE NEW RUN. It used to accept any
# heartbeat containing `beat-epoch:`, so a stale or foreign beat already at that path excused an
# unmonitorable launch — precisely the sticky-directory case the check exists to catch.
# SUPERSEDED FORM: this asserted a pre-launch run-id snapshot, which the launch NONCE replaced
# (job 190) and which then lingered as dead code until job 193. The property is unchanged — the
# check must be bound to the run WE launched — but the mechanism is now a token we generate rather
# than a run-id we could not predict, which is strictly stronger. Asserted here in that form.
if grep -q 'LAUNCH_NONCE=' "$LAUNCHER" && grep -q '_new_rid' "$LAUNCHER"; then
  ok "4b.23 the launcher binds the post-launch check to a run it can PROVE is its own"
else
  bad "4b.23 the launcher binds the post-launch check to its own run" "no nonce binding"
fi
# The BINDING MOVED, it did not disappear (job 198). The launcher used to grep the heartbeat for the
# run-id itself — a second implementation of the reader's grammar, which accepted beats the reader
# rejects. It now delegates with `--run-id "$_new_rid"`, so the run-binding is enforced by the one
# component that owns it (asserted by 4b.100) and the nonce remains the launcher's own check (4b.101).
# Job 178's fixed-string concern is moot here because the launcher no longer builds that pattern at
# all; where it DOES still match the run-id (the nonce/owner paths) 4b.24b keeps regexes out.
if grep -q 'gate-liveness.sh" "$SUMMARY" --run-id "$_new_rid"' "$LAUNCHER"; then
  ok "4b.24 the heartbeat's run-binding is delegated to the reader, not re-implemented"
else
  bad "4b.24 the heartbeat's run-binding is delegated to the reader" "delegation not found"
fi
body=$(sed 's/[[:space:]]*#.*$//' "$LAUNCHER")
if printf '%s\n' "$body" | grep -qE 'grep -q "\^run-id: '; then
  bad "4b.24b the run-id binding uses no regex interpolation" "a regex form remains"
else
  ok "4b.24b the run-id binding uses no regex interpolation"
fi
# ...including the terminal-verdict fallback, which is the same mistake one branch over.
# Both binding sites must exist: the heartbeat match (`^run-id: $_new_rid`) and the delegated
# terminal check (`--run-id "$_new_rid"`). Counting only the first form missed the second once
# delegation replaced the launcher's own grep.
_fb=$(grep -cE 'run-id: \$_new_rid|--run-id "\$_new_rid"' "$LAUNCHER")
[ "$_fb" -ge 2 ] && ok "4b.25 both the heartbeat and the terminal check are bound to the new run ($_fb sites)" \
                 || bad "4b.25 both the heartbeat and the terminal check are bound to the new run" "only $_fb binding site(s)"
# BEHAVIOURAL: a stale heartbeat sitting at the destination, in a directory the gate cannot
# write, must still be refused — the stale beat must not stand in for a real one.
if [ "$(id -u)" != 0 ]; then
  sd=$(mktemp -d)
  printf '==== AGENT-GATE HEARTBEAT ====\nrun-id: ancient\ngate-pid: 1\nbeat-epoch: 1\n==== END AGENT-GATE HEARTBEAT ====\n' > "$sd/s.txt.heartbeat"
  chmod 500 "$sd"
  out=$(bash "$LAUNCHER" --summary "$sd/s.txt" --log "$TMP/stale.log" -- --only file-size 2>&1); rc=$?
  [ "$rc" != 0 ] && ok "4b.26 a STALE pre-existing heartbeat does not excuse an unmonitorable launch (exit $rc)" \
                 || bad "4b.26 a STALE pre-existing heartbeat does not excuse an unmonitorable launch" "exit 0: $out"
  chmod 700 "$sd"; rm -rf "$sd"
else
  skipc "4b.26 stale heartbeat + unwritable directory" "running as root"
fi

# roborev job 169: env values must NOT ride in argv — /proc/<pid>/cmdline is world-readable
# while /proc/<pid>/environ is owner-only, and this fleet's environment holds real tokens.
# Comments are stripped first: the launcher explains WHY it avoids --setenv, and a naive scan
# matched its own explanation (the same self-match trap as the portability guard).
if printf '%s\n' "$(sed 's/[[:space:]]*#.*$//' "$LAUNCHER")" | grep -q -- '--setenv='; then
  bad "4b.27 no environment value is passed via --setenv (argv is world-readable)" "still present in code"
else
  ok "4b.27 no environment value is passed via --setenv (argv is world-readable)"
fi
if grep -q "umask 077" "$LAUNCHER" && grep -q 'printf .export %s=%q' "$LAUNCHER"; then
  ok "4b.28 the env is written to a 0600 script with shell-exact quoting"
else
  bad "4b.28 the env is written to a 0600 script with shell-exact quoting" "not found"
fi
# BEHAVIOURAL end-to-end: the value must REACH the gate while never appearing in any argv.
if [ "$HAVE_SYSTEMD" = yes ]; then
  probe="cqlite3473secret$$"
  ls="$TMP/leak-summary.txt"
  out=$(env "SECRET_PROBE_3473=$probe" bash "$LAUNCHER" --summary "$ls" --log "$TMP/leak.log" -- --only roborev-lints 2>&1)
  lu=$(printf '%s' "$out" | sed -n 's/^unit:  *//p'); [ -n "$lu" ] && echo "$lu" >> "$UNITS_FILE"
  mp=$(systemctl --user show "$lu" -p MainPID --value 2>/dev/null)
  if [ -n "$mp" ] && [ -r "/proc/$mp/environ" ]; then
    LC_ALL=C tr '\0' '\n' < "/proc/$mp/environ" | grep -q "SECRET_PROBE_3473=$probe" \
      && ok "4b.29 a caller variable REACHES the detached unit's environment" \
      || bad "4b.29 a caller variable REACHES the detached unit's environment" "absent"
    leak=0
    for c in /proc/[0-9]*/cmdline; do
      [ -r "$c" ] || continue
      LC_ALL=C tr '\0' ' ' < "$c" 2>/dev/null | grep -q "$probe" && { leak=1; break; }
    done
    [ "$leak" -eq 0 ] && ok "4b.30 ...and appears in NO process command line" \
                      || bad "4b.30 the value leaked into a process command line" "found in argv"
  else
    skipc "4b.29-4b.30 env delivery" "unit exited before it could be inspected"
  fi
  systemctl --user stop "$lu" >/dev/null 2>&1 || true
else
  skipc "4b.29-4b.30 env delivery" "no working systemd-run --user"
fi
# roborev job 169: a symlinked log destination must be refused, not truncated through.
lnk="$TMP/victim.txt"; : > "$lnk"; ln -sf "$lnk" "$TMP/log-link"
printf 'do not clobber me\n' > "$lnk"
out=$(bash "$LAUNCHER" --summary "$TMP/ls2.txt" --log "$TMP/log-link" -- --only file-size 2>&1); rc=$?
[ "$rc" != 0 ] && ok "4b.31 a SYMLINKED log path is refused (exit $rc)" \
               || bad "4b.31 a SYMLINKED log path is refused" "exit 0: $out"
[ "$(cat "$lnk")" = "do not clobber me" ] \
  && ok "4b.32 ...and the symlink target is untouched" || bad "4b.32 the symlink target was clobbered" "$(cat "$lnk")"
rm -f "$TMP/log-link" "$lnk"

# roborev job 172: the launcher must DELEGATE the terminal-verdict decision to the reader, not
# re-implement its grammar. Its own version grepped `^RESULT: (PASS|FAIL|...)` with no end anchor
# and no framing check, so `RESULT: PASSENGER` or a truncated block made the LAUNCHER report
# success while the reader would answer UNKNOWN — round 1's prefix-matching defect, reproduced in
# a second implementation.
if grep -q 'gate-liveness.sh" "$SUMMARY" --run-id "$_new_rid"' "$LAUNCHER"; then
  ok "4b.33 the launcher delegates the terminal-verdict decision to gate-liveness.sh"
else
  bad "4b.33 the launcher delegates the terminal-verdict decision to gate-liveness.sh" "not found"
fi
body=$(sed 's/[[:space:]]*#.*$//' "$LAUNCHER")
if printf '%s\n' "$body" | grep -qE "RESULT: \(PASS\|FAIL"; then
  bad "4b.34 no second copy of the verdict grammar remains in the launcher" "still grepping RESULT itself"
else
  ok "4b.34 no second copy of the verdict grammar remains in the launcher"
fi
# roborev job 172: the env script holds tokens and was never deleted — 51 copies had piled up in
# /tmp during development. Needs no attacker to write anything; a credential-at-rest leak.
if grep -q '_cleanup_env' "$LAUNCHER" && grep -q "trap _cleanup_env EXIT" "$LAUNCHER"; then
  ok "4b.35 the env script is removed by an EXIT trap (every path, success and failure)"
else
  bad "4b.35 the env script is removed by an EXIT trap" "no unconditional cleanup"
fi
# The trap alone is not enough (job 178, Medium): it cannot run if the LAUNCHER is SIGKILLed
# after the unit started, leaving the 0600 secrets file forever. The wrapper must unlink ITSELF,
# tying the file's lifetime to the process that consumed it.
if grep -q "printf '%q -f -- %q" "$LAUNCHER"; then
  ok "4b.35b the generated wrapper unlinks itself before exec"
else
  bad "4b.35b the generated wrapper unlinks itself before exec" "self-unlink not emitted"
fi
# ...and the self-unlink must be the LAST thing before exec, or bash may not have read the file.
if [ "$HAVE_SYSTEMD" = yes ]; then
  et="$TMP/envorder"; mkdir -p "$et"
  TMPDIR="$et" bash "$LAUNCHER" --summary "$TMP/eo.txt" --log "$TMP/eo.log" -- --only file-size >/dev/null 2>&1
  # the script is gone by now, so assert the ORDER from the generator instead
  gen_rm=$(grep -n "printf '%q -f -- %q" "$LAUNCHER" | head -1 | cut -d: -f1)
  gen_exec=$(grep -n "printf 'exec %q %q" "$LAUNCHER" | head -1 | cut -d: -f1)
  if [ -n "$gen_rm" ] && [ -n "$gen_exec" ] && [ "$gen_rm" -lt "$gen_exec" ]; then
    ok "4b.35c the self-unlink is emitted immediately before the exec line"
  else
    bad "4b.35c the self-unlink is emitted immediately before the exec line" "rm at ${gen_rm:-?}, exec at ${gen_exec:-?}"
  fi
else
  skipc "4b.35c self-unlink ordering" "no working systemd-run --user"
fi
if [ "$HAVE_SYSTEMD" = yes ]; then
  # Scoped to a PRIVATE TMPDIR (roborev job 176, Medium). The first version counted every
  # ${TMPDIR}/cqlite-gate-*/gate-env.sh on the HOST and required the global total to be zero, so
  # a CONCURRENT lane's launch — or any artifact predating the test — failed the gate even though
  # this invocation cleaned up perfectly. On a box that runs several lanes that is not a
  # hypothetical. It also captured a `before` count and never used it, which is the
  # "declared but not actually done" shape.
  #
  # The launcher derives its private directory from TMPDIR, so pointing TMPDIR at our own
  # scratch makes the assertion about THIS launch and nothing else.
  envtmp="$TMP/envscope"; mkdir -p "$envtmp"
  cs="$TMP/clean-summary.txt"
  out=$(TMPDIR="$envtmp" bash "$LAUNCHER" --summary "$cs" --log "$TMP/clean.log" -- --only file-size 2>&1)
  cu=$(printf '%s' "$out" | sed -n 's/^unit:  *//p'); [ -n "$cu" ] && echo "$cu" >> "$UNITS_FILE"
  leftover=$(find "$envtmp" -name 'gate-env.sh' 2>/dev/null | wc -l | tr -d ' ')
  [ "$leftover" -eq 0 ] && ok "4b.36 a successful launch leaves NO env script behind (scoped TMPDIR)" \
                        || bad "4b.36 a successful launch leaves NO env script behind" "$leftover found under $envtmp"
  # ...and a REFUSED launch must not leave one either.
  envtmp2="$TMP/envscope2"; mkdir -p "$envtmp2"
  TMPDIR="$envtmp2" bash "$LAUNCHER" --summary /nonexistent-dir-3473/x.txt --log "$TMP/ref.log" -- --only file-size >/dev/null 2>&1
  leftover2=$(find "$envtmp2" -name 'gate-env.sh' 2>/dev/null | wc -l | tr -d ' ')
  [ "$leftover2" -eq 0 ] && ok "4b.37 a REFUSED launch leaves NO env script behind (scoped TMPDIR)" \
                         || bad "4b.37 a REFUSED launch leaves NO env script behind" "$leftover2 found under $envtmp2"
  # NON-VACUITY: the scoped directory must be where the launcher actually works, or the two
  # assertions above are satisfied by an empty directory nobody wrote to. A launch that KEEPS
  # its private dir (default paths) must leave it there, proving TMPDIR is honoured.
  envtmp3="$TMP/envscope3"; mkdir -p "$envtmp3"
  out3=$(TMPDIR="$envtmp3" bash "$LAUNCHER" -- --only file-size 2>&1)
  u3=$(printf '%s' "$out3" | sed -n 's/^unit:  *//p'); [ -n "$u3" ] && echo "$u3" >> "$UNITS_FILE"
  if [ "$(find "$envtmp3" -maxdepth 1 -name 'cqlite-gate-*' 2>/dev/null | wc -l | tr -d ' ')" -ge 1 ]; then
    ok "4b.36b the launcher honours TMPDIR (so 4b.36/4b.37 are not vacuous)"
  else
    bad "4b.36b the launcher honours TMPDIR" "no private dir appeared under $envtmp3"
  fi
else
  skipc "4b.36-4b.37 env script cleanup" "no working systemd-run --user"
fi
# roborev job 172: the advertised poll command must carry --run-id (the launcher KNOWS it) and be
# shell-escaped, or it is wrong for a path with a space and can be fooled by a peer's artifacts.
if [ "$HAVE_SYSTEMD" = yes ]; then
  sp="$TMP/with space.txt"
  out=$(bash "$LAUNCHER" --summary "$sp" --log "$TMP/sp.log" -- --only file-size 2>&1)
  su=$(printf '%s' "$out" | sed -n 's/^unit:  *//p'); [ -n "$su" ] && echo "$su" >> "$UNITS_FILE"
  pc=$(printf '%s' "$out" | grep -A1 'poll it with' | tail -1)
  printf '%s' "$pc" | grep -q -- '--run-id' \
    && ok "4b.38 the advertised poll command carries --run-id" \
    || bad "4b.38 the advertised poll command carries --run-id" "$pc"
  # It must be RUNNABLE as printed, for a path containing a space.
  if eval "$pc" >/dev/null 2>&1 || [ $? -le 4 ]; then
    ok "4b.39 the printed command is runnable verbatim for a path with a space"
  else
    bad "4b.39 the printed command is runnable verbatim for a path with a space" "$pc"
  fi
else
  skipc "4b.38-4b.39 advertised poll command" "no working systemd-run --user"
fi

# roborev job 183: the log must not ALIAS the summary or the heartbeat. If log == summary, the
# gate's `>` rewrite truncates the accumulated log and two writers contend; if log == heartbeat,
# the beater's rename unlinks the log's open inode and the advertised log holds heartbeat data.
al="$TMP/alias.txt"
out=$(bash "$LAUNCHER" --summary "$al" --log "$al" -- --only file-size 2>&1); rc=$?
[ "$rc" != 0 ] && ok "4b.40 --log aliasing --summary is refused (exit $rc)" \
               || bad "4b.40 --log aliasing --summary is refused" "exit 0: $out"
printf '%s' "$out" | grep -q 'is the summary' \
  && ok "4b.41 the refusal names which artifact it collides with" || bad "4b.41 the refusal names the collision" "$out"
out=$(bash "$LAUNCHER" --summary "$al" --log "$al.heartbeat" -- --only file-size 2>&1); rc=$?
[ "$rc" != 0 ] && ok "4b.42 --log aliasing the heartbeat is refused (exit $rc)" \
               || bad "4b.42 --log aliasing the heartbeat is refused" "exit 0: $out"
# A DIFFERENT SPELLING of the same file must be caught too — string equality is not enough.
hl="$TMP/hardlink.log"; : > "$hl"; ln -f "$hl" "$TMP/hardlink-alias.log" 2>/dev/null && {
  out=$(bash "$LAUNCHER" --summary "$hl" --log "$TMP/hardlink-alias.log" -- --only file-size 2>&1); rc=$?
  [ "$rc" != 0 ] && ok "4b.43 a HARD LINK to the summary is refused (same inode, different name)" \
                 || bad "4b.43 a HARD LINK to the summary is refused" "exit 0: $out"
} || skipc "4b.43 hard-link alias" "ln -f unavailable on this filesystem"
# CONTROL: distinct paths must still launch, or the check is just a refusal.
out=$(bash "$LAUNCHER" --summary "$TMP/ok-s.txt" --log "$TMP/ok-l.log" -- --only file-size 2>&1); rc=$?
ou=$(printf '%s' "$out" | sed -n 's/^unit:  *//p'); [ -n "$ou" ] && echo "$ou" >> "$UNITS_FILE"
[ "$rc" = 0 ] && ok "4b.44 control: distinct summary and log paths still launch" \
             || bad "4b.44 control: distinct summary and log paths still launch" "rc=$rc: $out"

GATE_SH="$REPO_ROOT/scripts/agent-gate.sh"
# (Job 183's beater-identity assertions lived here and were SUPERSEDED by 4b.56-4b.58 below,
# which assert the same properties against the three-valued `_hb_state` that replaced the
# two-valued `_hb_is_ours` in job 185. Kept as one note rather than two sets of near-duplicate
# cases, one of which would silently rot.)

# BEHAVIOURAL: a real gate must still start a beater and leave none behind.
if [ "$HAVE_SYSTEMD" = yes ]; then
  bs="$TMP/beater-life.txt"
  AGENT_GATE_SUMMARY_FILE="$bs" bash "$GATE_SH" --only file-size >/dev/null 2>&1 </dev/null
  sleep 1
  # Counted by reading /proc in THIS shell, not via `ps | grep`: the grep process's own argv
  # contains the needle, so it matches itself and the count is never zero. That self-match has
  # now bitten this change three times (a portability guard, a --setenv scan, and here), so the
  # rule is: never search a process table with a pattern that appears in the searching command.
  left=0
  for _c in /proc/[0-9]*/cmdline; do
    [ -r "$_c" ] || continue
    case "$(LC_ALL=C tr '\0' ' ' < "$_c" 2>/dev/null)" in
      *"gate-heartbeat.sh --file $bs"*) left=$((left + 1)) ;;
    esac
  done
  [ "$left" -eq 0 ] && ok "4b.49 a completed gate leaves no beater running" \
                    || bad "4b.49 a completed gate leaves no beater running" "$left still alive"
  grep -q '^heartbeat: on ' "$bs" && ok "4b.50 ...and it did publish a heartbeat while running" \
                                  || bad "4b.50 the gate published a heartbeat" "$(grep '^heartbeat' "$bs")"
else
  skipc "4b.49-4b.50 beater lifecycle" "no working systemd-run --user"
fi

# roborev job 185: alias detection must work on paths that DO NOT EXIST YET. `-ef` needs both
# files present, and the log normally does not exist at check time — so two nonexistent spellings
# of one file slipped through and creating the log created the summary too.
ad=$(mktemp -d)
out=$(bash "$LAUNCHER" --summary "$ad/x" --log "$ad/./x" -- --only file-size 2>&1); rc=$?
[ "$rc" != 0 ] && ok "4b.51 two NONEXISTENT paths resolving to one file are refused (exit $rc)" \
               || bad "4b.51 two NONEXISTENT paths resolving to one file are refused" "exit 0: $out"
out=$(bash "$LAUNCHER" --summary "$ad/y" --log "$ad//y" -- --only file-size 2>&1); rc=$?
[ "$rc" != 0 ] && ok "4b.52 a doubled-slash spelling is refused too (exit $rc)" \
               || bad "4b.52 a doubled-slash spelling is refused too" "exit 0: $out"
# The post-creation -ef re-check must exist as well: canonicalisation cannot see every case.
if grep -q 'creating the log revealed it is the SAME FILE' "$LAUNCHER"; then
  ok "4b.53 the inode comparison is repeated AFTER the log is created"
else
  bad "4b.53 the inode comparison is repeated AFTER the log is created" "not found"
fi
# CONTROL: distinct paths in the same directory must still launch.
out=$(bash "$LAUNCHER" --summary "$ad/s1.txt" --log "$ad/l1.log" -- --only file-size 2>&1); rc=$?
au=$(printf '%s' "$out" | sed -n 's/^unit:  *//p'); [ -n "$au" ] && echo "$au" >> "$UNITS_FILE"
[ "$rc" = 0 ] && ok "4b.54 control: distinct paths in one directory still launch" \
             || bad "4b.54 control: distinct paths in one directory still launch" "rc=$rc: $out"
rm -rf "$ad"

# roborev job 185: identity must be PORTABLE, and "cannot tell" must not cause a respawn. On a
# host without /proc the previous version made _hb_is_ours always false, so the gate spawned a
# NEW beater at every component boundary without stopping the old one (~30 on a full gate).
if grep -q 'ps -o lstart= -p' "$GATE_SH" && grep -q 'ps -o lstart= -p' "$REPO_ROOT/scripts/lib/gate-heartbeat.sh"; then
  ok "4b.55 both the gate and the beater have a portable (non-/proc) identity fallback"
else
  bad "4b.55 both the gate and the beater have a portable identity fallback" "ps -o lstart= missing"
fi
if grep -q '^_hb_state() {' "$GATE_SH"; then
  ok "4b.56 the beater state is THREE-valued (ours / gone / unverifiable)"
else
  bad "4b.56 the beater state is three-valued" "_hb_state not found"
fi
ensblk2=$(sed -n '/^_hb_ensure() {$/,/^}$/p' "$GATE_SH")
if printf '%s\n' "$ensblk2" | grep -q '_hb_state)" = gone'; then
  ok "4b.57 _hb_ensure respawns ONLY on a verifiable 'gone' (never on 'unverifiable')"
else
  bad "4b.57 _hb_ensure respawns only on a verifiable 'gone'" "respawn condition not restricted"
fi
stopblk2=$(sed -n '/^_hb_stop() {$/,/^}$/p' "$GATE_SH")
if printf '%s\n' "$stopblk2" | grep -q '_hb_state)" != ours'; then
  ok "4b.58 _hb_stop signals ONLY on a verifiable 'ours'"
else
  bad "4b.58 _hb_stop signals only on a verifiable 'ours'" "signal condition not restricted"
fi
# BEHAVIOURAL: a real full-ish gate must end with exactly ZERO beaters, not N.
bs2="$TMP/beaters2.txt"
AGENT_GATE_SUMMARY_FILE="$bs2" bash "$GATE_SH" --only roborev-lints >/dev/null 2>&1 </dev/null
sleep 1
n=0
for _c in /proc/[0-9]*/cmdline; do
  [ -r "$_c" ] || continue
  case "$(LC_ALL=C tr '\0' ' ' < "$_c" 2>/dev/null)" in *"gate-heartbeat.sh --file $bs2"*) n=$((n + 1)) ;; esac
done
[ "$n" -eq 0 ] && ok "4b.59 a multi-boundary gate leaves ZERO beaters (no per-boundary accumulation)" \
              || bad "4b.59 a multi-boundary gate leaves ZERO beaters" "$n still running"

# roborev job 188: --help must print the USAGE. It used to be `sed -n '2,45p'` over the header,
# and as the header grew that range ended mid-sentence inside the threat-model commentary and
# omitted the invocation syntax entirely. A range that must be re-tuned whenever a comment is
# edited is a latent defect.
h=$(bash "$LAUNCHER" --help 2>&1)
printf '%s' "$h" | grep -q 'bash scripts/flow/gate-detached.sh \[--summary' \
  && ok "4b.60 --help prints the invocation syntax" || bad "4b.60 --help prints the invocation syntax" "absent"
printf '%s' "$h" | grep -q -- '--summary <path>' && printf '%s' "$h" | grep -q -- '--log <path>' \
  && ok "4b.61 --help documents both options" || bad "4b.61 --help documents both options" "absent"
printf '%s' "$h" | grep -q '69 ' \
  && ok "4b.62 --help documents the exit codes (incl. 69)" || bad "4b.62 --help documents the exit codes" "absent"
# It must not be a fixed line range any more, or it will drift again.
if grep -qE "sed -n '[0-9]+,[0-9]+p' \"\\$0\"" "$LAUNCHER"; then
  bad "4b.63 --help is not a fixed line range over the header" "a sed range remains"
else
  ok "4b.63 --help is not a fixed line range over the header"
fi

# roborev job 188: the beater's identity COMPARISON must cover every tier that HAS an identity.
# The lstart tier was labelled in the beat but never compared — it fell through to a bare
# `kill -0`, which a recycled pid satisfies, while the beat still advertised `parent-check:
# lstart` so a reader would trust it. Adding a tier without wiring its comparison buys only the
# appearance of a guarantee.
BEATER_SH="$REPO_ROOT/scripts/lib/gate-heartbeat.sh"
aliveblk=$(sed -n '/^_gate_alive() {$/,/^}$/p' "$BEATER_SH")
if printf '%s\n' "$aliveblk" | grep -q 'starttime|lstart'; then
  ok "4b.64 _gate_alive compares the identity for BOTH proc and lstart tiers"
else
  bad "4b.64 _gate_alive compares the identity for both tiers" "lstart still falls through to kill -0"
fi
if printf '%s\n' "$aliveblk" | grep -q 'kill -0'; then
  ok "4b.65 ...and bare existence remains only as the kill0-tier fallback"
else
  bad "4b.65 bare existence remains as the kill0 fallback" "fallback missing entirely"
fi
# BEHAVIOURAL: a beater whose gate dies must stop beating and exit, on this host's tier.
bash -c 'while :; do sleep 1; done' >/dev/null 2>&1 &
_g=$!; remember_pid "$_g"
_hbf="$TMP/tier.hb"
bash "$BEATER_SH" --file "$_hbf" --run-id tier --gate-pid "$_g" --interval 1 </dev/null >/dev/null 2>&1 &
_b=$!; remember_pid "$_b"
for ((_i_=0; _i_<40; _i_++)); do [ -s "$_hbf" ] && break; sleep 0.5; done
_s1=$(sed -n 's/^beat-seq: //p' "$_hbf" 2>/dev/null)
kill -9 "$_g" 2>/dev/null; wait "$_g" 2>/dev/null || true
sleep 3
_s2=$(sed -n 's/^beat-seq: //p' "$_hbf" 2>/dev/null)
[ -n "$_s1" ] && [ "$_s1" = "$_s2" ] \
  && ok "4b.66 the beater stops advancing once its gate dies (tier: $(sed -n 's/^parent-check: //p' "$_hbf"))" \
  || bad "4b.66 the beater stops advancing once its gate dies" "beat-seq $_s1 -> $_s2"
kill -9 "$_b" 2>/dev/null || true

# roborev job 190: verification must be bound to a token WE generate, not to "the first run-id
# that differs from the pre-launch value" — a concurrent gate on the same summary path can publish
# first, and the launcher would then report success and print a poll command bound to the PEER's
# run. A run-id we cannot predict is no basis for the claim.
if grep -q 'LAUNCH_NONCE=' "$LAUNCHER" && grep -q 'AGENT_GATE_LAUNCH_NONCE' "$LAUNCHER"; then
  ok "4b.67 the launcher generates a nonce and forwards it to the gate"
else
  bad "4b.67 the launcher generates a nonce and forwards it" "not found"
fi
_nb=$(grep -c 'launch-nonce: $LAUNCH_NONCE' "$LAUNCHER")
[ "$_nb" -ge 2 ] && ok "4b.68 BOTH artifacts must carry that nonce ($_nb sites)" \
                 || bad "4b.68 both artifacts must carry that nonce" "only $_nb site(s)"
# BEHAVIOURAL: the nonce must actually reach both artifacts of a real launch.
if [ "$HAVE_SYSTEMD" = yes ]; then
  ns="$TMP/nonce-s.txt"
  out=$(bash "$LAUNCHER" --summary "$ns" --log "$TMP/nonce.log" -- --only file-size 2>&1)
  nu=$(printf '%s' "$out" | sed -n 's/^unit:  *//p'); [ -n "$nu" ] && echo "$nu" >> "$UNITS_FILE"
  for ((_i_=0; _i_<60; _i_++)); do grep -q '^launch-nonce: ' "$ns" 2>/dev/null && break; sleep 0.5; done
  sn=$(sed -n 's/^launch-nonce: //p' "$ns" 2>/dev/null | head -1)
  hn=$(sed -n 's/^launch-nonce: //p' "$ns.heartbeat" 2>/dev/null | head -1)
  if [ -n "$sn" ] && [ "$sn" = "$hn" ]; then
    ok "4b.69 the same nonce reaches the summary AND the heartbeat"
  else
    bad "4b.69 the same nonce reaches both artifacts" "summary='$sn' heartbeat='$hn'"
  fi
  # ...and a PEER's artifacts bearing a DIFFERENT nonce must not satisfy verification. Modelled by
  # an unwritable directory already holding a fresh-looking peer beat: before the nonce, the
  # pre-existing pair could stand in for a real launch.
  if [ "$(id -u)" != 0 ]; then
    pd=$(mktemp -d)
    printf '==== AGENT-GATE SUMMARY ====\nrun-id: peers-run\nlaunch-nonce: not-ours\nRESULT: INCOMPLETE (x)\n==== END AGENT-GATE SUMMARY ====\n' > "$pd/s.txt"
    printf '==== AGENT-GATE HEARTBEAT ====\nrun-id: peers-run\nlaunch-nonce: not-ours\ngate-pid: 1\nparent-check: starttime\ninterval: 20\nbeat-seq: 9\nbeat-epoch: %s\n==== END AGENT-GATE HEARTBEAT ====\n' "$(date +%s)" > "$pd/s.txt.heartbeat"
    chmod 500 "$pd"
    out=$(bash "$LAUNCHER" --summary "$pd/s.txt" --log "$TMP/peer2.log" -- --only file-size 2>&1); rc=$?
    [ "$rc" != 0 ] && ok "4b.70 a PEER's artifacts (different nonce) do not satisfy verification (exit $rc)" \
                   || bad "4b.70 a PEER's artifacts do not satisfy verification" "exit 0: $out"
    chmod 700 "$pd"; rm -rf "$pd"
  else
    skipc "4b.70 peer-artifact rejection" "running as root"
  fi
else
  skipc "4b.69-4b.70 launch nonce" "no working systemd-run --user"
fi

# roborev job 190: the beater must start BEFORE the tree-identity capture, or a slow capture makes
# a healthy gate look unmonitorable — and the first seconds of every gate publish no liveness.
GATE_SH2="$REPO_ROOT/scripts/agent-gate.sh"
hb_ln=$(grep -n '^  _hb_start$' "$GATE_SH2" | tail -1 | cut -d: -f1)
cap_ln=$(grep -n '^  _tree_capture_start$' "$GATE_SH2" | tail -1 | cut -d: -f1)
if [ -n "$hb_ln" ] && [ -n "$cap_ln" ] && [ "$hb_ln" -lt "$cap_ln" ]; then
  ok "4b.71 the beater starts BEFORE the tree-identity capture (line $hb_ln < $cap_ln)"
else
  bad "4b.71 the beater starts before the tree-identity capture" "_hb_start at ${hb_ln:-?}, capture at ${cap_ln:-?}"
fi

# roborev job 193: the pre-launch run-id captures were DEAD CODE once the nonce replaced them —
# code that reads like a check but checks nothing. And the nonce does not stop two launchers
# pointing at ONE summary path: each proves ownership of its own artifacts while their heartbeat
# renames and summary rewrites destroy each other.
body=$(sed 's/[[:space:]]*#.*$//' "$LAUNCHER")
if printf '%s\n' "$body" | grep -q '_pre_sum_rid\|_pre_hb_rid'; then
  bad "4b.72 the dead pre-launch run-id captures are gone" "still present"
else
  ok "4b.72 the dead pre-launch run-id captures are gone"
fi
# (4b.73's O_EXCL FILE lock was superseded by the atomic DIRECTORY lock in job 194 — see
#  4b.80-4b.87. Asserting the reservation exists at all is kept here; its mechanism is asserted
#  there, so the two do not drift apart.)
if grep -q '_reserve="$SUMMARY.launch-lock"' "$LAUNCHER"; then
  ok "4b.73 the summary path is reserved before launch"
else
  bad "4b.73 the summary path is reserved before launch" "not found"
fi
# Re-pointed (job 205): this asserted the bare `is-active --quiet` form, which 4b.106 now requires to
# be ABSENT — the two would have contradicted each other. Liveness is two readings, either of which
# holds the reservation: the launcher pid (alive throughout its own acquisition) and the unit.
# Re-pointed again (R6 inversion): the raw `kill -0` moved inside `_pid_state`, so assert the two
# READINGS are consulted — the owner pid's state and the unit's — not the spelling of either.
# Re-pointed AGAIN (roborev job 316): this asserted `_unit_is_live "$_own_unit"` — a SPELLING, in a
# case whose own comment directly above says "not the spelling of either". The reclamation site now
# asks the STRONGER `_unit_runs_a_gate`, so the old grep FAILED on a fix that satisfies the stated
# property BETTER. Assert the PROPERTY: both readings are consulted — the owner pid's state, and
# the unit through EITHER of the file's two unit predicates. 4b.74b then pins WHICH one the
# reclamation path must use, so a strengthening stays free while a weakening reds.
if grep -qF '_pid_state "$_own_pid"' "$LAUNCHER" \
   && grep -qE '_unit_(is_live|runs_a_gate) "\$_own_unit"' "$LAUNCHER"; then
  ok "4b.74 a reservation is only honoured while its owner is LIVE (self-healing)"
else
  bad "4b.74 a reservation is only honoured while its owner is live" "no staleness test"
fi
# 4b.74b (roborev job 316, Medium): the RECLAMATION site must ask whether a GATE RUNS, not whether
# the unit is merely non-inactive. `_unit_is_live` returns 0 for "live OR unmeasurable", and an
# ORPHANED process keeps a unit active indefinitely — so an affirmatively dead owner was promoted
# back to live and the path was refused FOREVER: the exact permanent-refusal defect that
# `_unit_runs_a_gate` exists to prevent. Scoped to the reclamation site, because the OTHER
# `_unit_is_live` caller (post-launch monitorability of our OWN unit) is correct as it stands —
# there the gate may not have exec'd yet, so the gate-aware predicate would refuse a healthy launch.
if grep -qE '_unit_runs_a_gate "\$_own_unit"' "$LAUNCHER"; then
  if grep -qE '_unit_is_live "\$_own_unit"' "$LAUNCHER"; then
    bad "4b.74b the reclamation site asks whether a GATE runs, not whether the unit is active" \
        "_unit_is_live is still applied to \$_own_unit — the orphan case would refuse forever"
  else
    ok "4b.74b the reclamation site asks whether a GATE runs (an orphan cannot block forever)"
  fi
else
  bad "4b.74b the reclamation site asks whether a GATE runs" "_unit_runs_a_gate \$_own_unit absent"
fi
if [ "$HAVE_SYSTEMD" = yes ]; then
  cp1="$TMP/concurrent.txt"
  o1=$(bash "$LAUNCHER" --summary "$cp1" --log "$TMP/c1.log" -- --only roborev-lints 2>&1); r1=$?
  u1=$(printf '%s' "$o1" | sed -n 's/^unit:  *//p'); [ -n "$u1" ] && echo "$u1" >> "$UNITS_FILE"
  o2=$(bash "$LAUNCHER" --summary "$cp1" --log "$TMP/c2.log" -- --only file-size 2>&1); r2=$?
  u2=$(printf '%s' "$o2" | sed -n 's/^unit:  *//p'); [ -n "$u2" ] && echo "$u2" >> "$UNITS_FILE"
  if [ "$r1" = 0 ] && [ "$r2" != 0 ]; then
    ok "4b.75 a SECOND launcher on the same summary path is refused (first=$r1, second=$r2)"
  else
    bad "4b.75 a second launcher on the same summary path is refused" "first=$r1 second=$r2"
  fi
  printf '%s' "$o2" | grep -q 'already owned by a LIVE run' \
    && ok "4b.76 the refusal says the owner is LIVE" || bad "4b.76 the refusal says the owner is live" "$o2"
  systemctl --user stop "$u1" >/dev/null 2>&1 || true
  # ...and once that owner is gone, the reservation is reclaimed rather than blocking forever.
  o3=$(bash "$LAUNCHER" --summary "$cp1" --log "$TMP/c3.log" -- --only file-size 2>&1); r3=$?
  u3=$(printf '%s' "$o3" | sed -n 's/^unit:  *//p'); [ -n "$u3" ] && echo "$u3" >> "$UNITS_FILE"
  [ "$r3" = 0 ] && ok "4b.77 a STALE reservation is reclaimed (a dead owner does not block forever)" \
                || bad "4b.77 a stale reservation is reclaimed" "exit $r3: $o3"
else
  skipc "4b.75-4b.77 concurrent-launch detection" "no working systemd-run --user"
fi

# roborev job 193 (Low): the log was TRUNCATED before the summary/heartbeat were validated, so a
# later refusal destroyed a previous log for a launch that never happened.
pl="$TMP/preserve.log"
printf 'previous log content\n' > "$pl"
bash "$LAUNCHER" --summary /nonexistent-dir-3473/s.txt --log "$pl" -- --only file-size >/dev/null 2>&1
[ "$(cat "$pl" 2>/dev/null)" = "previous log content" ] \
  && ok "4b.78 a REFUSED launch preserves an existing log" \
  || bad "4b.78 a refused launch preserves an existing log" "it was truncated"
# ...and a launch that does NOT exist must not leave a probe file behind either.
pn="$TMP/nonexistent-probe.log"
bash "$LAUNCHER" --summary /nonexistent-dir-3473/s.txt --log "$pn" -- --only file-size >/dev/null 2>&1
[ ! -e "$pn" ] && ok "4b.79 a refused launch leaves no log probe behind" \
              || bad "4b.79 a refused launch leaves no log probe behind" "$pn exists"

# roborev job 194: reclaiming a stale reservation must be ATOMIC. The file-based lock was racy in
# two ways — a second launcher could read a freshly acquired lock BEFORE its owner's unit became
# active and judge it stale, and two reclaimers could delete each other's replacement locks. Both
# ended with two gates writing one summary path.
# The reservation is a SYMLINK whose TARGET encodes the owner (job 199). `ln -s` fails if the path
# exists — mutual exclusion — and its target is arbitrary text, so ownership is published by the very
# act of acquiring. That is what removed the acquisition WINDOW the directory design had.
if grep -qF '_res_target="unit=$UNIT|pid=$$|start=$_res_ident"' "$LAUNCHER" \
   && grep -qF 'ln -s "$_res_target" "$_reserve"' "$LAUNCHER"; then
  ok "4b.80 the reservation is an atomic, SELF-IDENTIFYING symlink"
else
  bad "4b.80 the reservation is an atomic, self-identifying symlink" "not found"
fi
# 4b.81 REPLACED, and the old assertion was WORSE than stale — it pinned a claim that was FALSE.
# It read "reclamation is claimed by an atomic RENAME into an mktemp scratch dir", asserting that
# only one of two concurrent reclaimers could succeed. `mv` is not a compare-and-swap: it moves
# whatever occupies the path and compares nothing against an expected value. Demonstrated
# interleaving (roborev job 203): A reclaims and launches, then B's delayed `mv` moves A's LIVE
# reservation away and installs its own — both gates on one summary path. A test whose NAME asserts
# a false property is worse than no test, because it is cited as evidence.
#
# Reclamation is now serialised by `flock`, with the classification RE-READ inside the mutex.
if grep -q 'flock -w 30 9' "$LAUNCHER" && grep -q 'RE-READ under the mutex' "$LAUNCHER"; then
  ok "4b.81 reclamation is serialised by flock, and re-reads the owner INSIDE the mutex"
else
  bad "4b.81 reclamation is serialised by flock and re-reads inside the mutex" "not found"
fi
# flock, not a mkdir mutex: the kernel drops it when the fd closes, so a reclaimer dying mid-sequence
# leaves nothing to time out — the stale-lock window this design already refused to reintroduce.
if grep -q 'command -v flock' "$LAUNCHER"; then
  ok "4b.81b an unavailable flock REFUSES rather than racing unserialised"
else
  bad "4b.81b an unavailable flock refuses rather than racing" "no fail-closed check"
fi
# `exec` with no command applies redirections to the current shell, so a `2>/dev/null` there is
# permanent and silences every later refusal. Pinned because the symptom is a SILENT non-zero exit.
# CODE ONLY. This assertion first read the whole file and failed, because the COMMENT above the fix
# quotes the defective form verbatim to explain it — prose defeating the guard that reads it, the
# same channel-sharing shape CLAUDE.md anchors the roborev census matcher at column zero for. The
# separation is the fix, not a reworded comment: strip comment lines, then judge the code.
_launcher_code=$(grep -vE '^[[:space:]]*#' "$LAUNCHER")
if printf '%s' "$_launcher_code" | grep -qF 'exec 9>>"$_mutex"' \
   && ! printf '%s' "$_launcher_code" | grep -qF 'exec 9>"$_mutex" 2>/dev/null'; then
  ok "4b.81c the mutex fd is opened without a permanent stderr redirection"
else
  bad "4b.81c the mutex fd is opened without a permanent stderr redirection" "stderr may be silenced"
fi
# `kill -0` now lives in `_pid_state`; what matters is that the owner PID is still consulted, which
# is what closes the window between reserving the path and the unit becoming active.
if grep -qF 'pid=$$' "$LAUNCHER" && grep -qF '_pid_state "$_own_pid"' "$LAUNCHER"; then
  ok "4b.82 liveness counts the LAUNCHER PID too, closing the unit-startup window"
else
  bad "4b.82 liveness counts the launcher pid" "startup window still open"
fi
if [ "$HAVE_SYSTEMD" = yes ]; then
  dl="$TMP/dirlock.txt"
  o1=$(bash "$LAUNCHER" --summary "$dl" --log "$TMP/dl1.log" -- --only roborev-lints 2>&1); r1=$?
  du1=$(printf '%s' "$o1" | sed -n 's/^unit:  *//p'); [ -n "$du1" ] && echo "$du1" >> "$UNITS_FILE"
  [ -L "$dl.launch-lock" ] && ok "4b.83 a launch creates the reservation symlink" \
                           || bad "4b.83 a launch creates the reservation symlink" "absent"
  _lt=$(readlink "$dl.launch-lock" 2>/dev/null)
  case "$_lt" in
    *unit=*\|pid=*\|start=*) ok "4b.84 the link target names the unit, the launcher pid AND its start identity" ;;
    *) bad "4b.84 the link target names unit, pid and start identity" "target='$_lt'" ;;
  esac
  o2=$(bash "$LAUNCHER" --summary "$dl" --log "$TMP/dl2.log" -- --only file-size 2>&1); r2=$?
  du2=$(printf '%s' "$o2" | sed -n 's/^unit:  *//p'); [ -n "$du2" ] && echo "$du2" >> "$UNITS_FILE"
  [ "$r1" = 0 ] && [ "$r2" != 0 ] \
    && ok "4b.85 a LIVE owner refuses a second launcher (first=$r1 second=$r2)" \
    || bad "4b.85 a live owner refuses a second launcher" "first=$r1 second=$r2"
  systemctl --user stop "$du1" >/dev/null 2>&1 || true
  o3=$(bash "$LAUNCHER" --summary "$dl" --log "$TMP/dl3.log" -- --only file-size 2>&1); r3=$?
  du3=$(printf '%s' "$o3" | sed -n 's/^unit:  *//p'); [ -n "$du3" ] && echo "$du3" >> "$UNITS_FILE"
  [ "$r3" = 0 ] && ok "4b.86 a STALE owner is reclaimed (a finished gate does not block the path)" \
                || bad "4b.86 a stale owner is reclaimed" "exit $r3: $o3"
  # No stale-rename litter may survive a successful reclamation.
  if ls -d "$dl.launch-lock.stale."* >/dev/null 2>&1; then
    bad "4b.87 reclamation leaves no .stale.* litter" "$(ls -d "$dl.launch-lock.stale."* 2>/dev/null)"
  else
    ok "4b.87 reclamation leaves no .stale.* litter"
  fi
else
  skipc "4b.83-4b.87 directory reservation" "no working systemd-run --user"
fi

# THE "INCOMPLETE OWNER" FAMILY IS GONE, AND ITS TESTS WITH IT (job 199).
#
# Seven cases used to live here — an incomplete record reading as acquisition-in-progress, an
# unwritable record failing closed, an age deadline, both stat spellings, an unmeasurable age
# counting as fresh, and the aged-vs-fresh reclamation pair. Every one of them tested a state that
# only existed because acquisition took TWO operations (`mkdir`, then write `owner`), leaving a
# window in which the lock existed but its owner was unknown. Both readings of that window were
# wrong: refusing forever let a launcher killed mid-acquisition block the path permanently, and
# reclaiming after an age deadline let a launcher merely PAUSED have its live lock stolen.
#
# A symlink publishes ownership in the SAME atomic operation that acquires the lock, so there is no
# window, no incomplete state, and no timer to tune. These cases are deleted rather than re-pointed
# because the states they covered cannot occur — keeping them would mean asserting behaviour about
# a situation the design no longer admits.
if grep -q '_path_age_secs\|_res_age' "$LAUNCHER"; then
  bad "4b.88 no age heuristic remains in the reservation logic" \
      "an age-based reclamation can steal a PAUSED launcher's live lock"
else
  ok "4b.88 no age heuristic remains — ownership is atomic, so there is no window to time"
fi
# Reclamation must therefore rest on PROOF that the owner is gone, never on elapsed time.
if grep -q 'Refusing rather than reclaiming a lock that may be live' "$LAUNCHER"; then
  ok "4b.89 an unreadable owner is refused, not treated as proof of death"
else
  bad "4b.89 an unreadable owner is refused" "not found"
fi

# 4b.90 is RESTORED. It was re-pointed at the symlink and then deleted in the same edit, because it
# lived inside the region the obsolete family occupied — so the pid-reuse invariant briefly had no
# test at all. Enumerating the surviving case IDs is what caught that; the re-point alone did not.
if grep -qF 'start=$_res_ident' "$LAUNCHER" && grep -q '_proc_identity' "$LAUNCHER"; then
  ok "4b.90 the launcher pid is PINNED by a start identity (pid reuse cannot fake liveness)"
else
  bad "4b.90 the launcher pid is pinned by a start identity" "not found"
fi

# --- roborev job 200 ------------------------------------------------------------------------------
# The reservation path belongs in the LOG ALIAS set, and for a reason specific to it: this script
# CREATES A SYMLINK there, so the early `-L` refusal answers about a tree in which the launch-lock
# does not exist yet. `--log <summary>.launch-lock` therefore passed every check and the pre-launch
# `>` followed the reservation link, writing the gate's log into a file named after the link's own
# owner text.
if grep -qF '_c_lock=$(_canon "$SUMMARY.launch-lock")' "$LAUNCHER"; then
  ok "4b.91 the reservation path is in the log alias set"
else
  bad "4b.91 the reservation path is in the log alias set" "not found"
fi
# ...and the log is re-checked at the POINT OF USE, because the early answer describes an earlier
# tree. This is the general form of the defect: any symlink appearing at the log path between the
# two points defeats a check made only at the first.
if grep -q 'became a symlink after it was checked' "$LAUNCHER"; then
  ok "4b.92 the log is re-checked for symlink-ness immediately before the truncate"
else
  bad "4b.92 the log is re-checked for symlink-ness before the truncate" "point-of-use check absent"
fi
if [ "$HAVE_SYSTEMD" = yes ]; then
  za="$TMP/alias.txt"
  ao=$(bash "$LAUNCHER" --summary "$za" --log "$za.launch-lock" -- --only file-size 2>&1); ar=$?
  au=$(printf '%s' "$ao" | sed -n 's/^unit:  *//p'); [ -n "$au" ] && echo "$au" >> "$UNITS_FILE"
  if [ "$ar" != 0 ] && printf '%s' "$ao" | grep -q 'FOLLOWS a symlink'; then
    ok "4b.93 --log aliasing the reservation is REFUSED, naming the symlink mechanism (exit $ar)"
  else
    bad "4b.93 --log aliasing the reservation is refused naming the mechanism" "exit $ar: $ao"
  fi
  # The junk file the defect produced is named after the link target text, so its absence is the
  # positive evidence that nothing followed the link.
  if ls "$TMP"/unit=* >/dev/null 2>&1; then
    bad "4b.94 no file named after the link target is created" "$(ls "$TMP"/unit=* 2>/dev/null)"
  else
    ok "4b.94 no file named after the link target is created"
  fi
else
  skip=$((skip+1)); echo "SKIP 4b.93/4b.94 (no user systemd manager on this host)"
fi

# A ZOMBIE launcher is GONE: `kill -0` succeeds on one, so without this a launcher that died
# un-reaped reads as LIVE and its reservation can never self-heal — the same permanent-block
# failure the incomplete-owner window caused, in a different place.
# The inversion turned this from a negated guard into its own branch: a zombie owner is classified
# GONE outright. Assert the branch, since the negation no longer exists.
if grep -qF 'if _proc_is_zombie "$_own_pid"; then' "$LAUNCHER"; then
  ok "4b.95 reservation liveness excludes a zombie launcher"
else
  bad "4b.95 reservation liveness excludes a zombie launcher" "kill -0 alone treats a zombie as live"
fi
# Behavioural, against a REAL zombie — the structural grep above cannot show that the parse works.
# `set -- $_state` after stripping through the last ')' is what makes it correct for a comm
# containing spaces or parens.
eval "$(sed -n '/^_proc_is_zombie()/,/^}/p' "$LAUNCHER")"
python3 -c 'import subprocess,time; c=subprocess.Popen(["/bin/true"]); time.sleep(0.4); print(c.pid, flush=True); time.sleep(8)' > "$TMP/zpid" 2>/dev/null &
_zwatch=$!
_zp=""
for _i in 1 2 3 4 5 6 7 8 9 10; do _zp=$(cat "$TMP/zpid" 2>/dev/null); [ -n "$_zp" ] && break; sleep 0.4; done
if [ -n "$_zp" ] && [ "$(ps -o state= -p "$_zp" 2>/dev/null | tr -d ' ')" = "Z" ]; then
  # The premise of the finding, asserted rather than assumed: kill -0 calls this zombie ALIVE.
  if kill -0 "$_zp" 2>/dev/null; then
    ok "4b.96 premise: kill -0 reports a zombie as alive (why the check is needed)"
  else
    bad "4b.96 premise: kill -0 reports a zombie as alive" "kill -0 already says dead; finding moot"
  fi
  _proc_is_zombie "$_zp" && ok "4b.97 a real zombie is detected as GONE"                          || bad "4b.97 a real zombie is detected as gone" "pid $_zp state=Z not detected"
else
  skip=$((skip+2)); echo "SKIP 4b.96/4b.97 (could not produce a zombie on this host)"
fi
kill "$_zwatch" 2>/dev/null || true; wait "$_zwatch" 2>/dev/null || true
# No false positive on a LIVE process, and an UNMEASURABLE state must read as NOT-a-zombie so the
# caller keeps refusing: "I could not tell" may never license reclaiming a lock that may be live.
# A genuinely LIVE process, whose pid is captured from `$!` so there is no doubt what was passed.
# This case previously read `_proc_is_zombie $` — a LITERAL dollar, an unmeasurable "pid" — so it
# passed by taking the unmeasurable branch and was a duplicate of 4b.99, testing nothing about a
# live process. It passed for the wrong reason, which is the only kind of green worth distrusting.
sleep 30 & _livepid=$!
if _proc_is_zombie "$_livepid"; then
  bad "4b.98 a LIVE process is not called a zombie" "false positive on live pid $_livepid"
else
  ok "4b.98 a LIVE process is not called a zombie (pid $_livepid)"
fi
kill "$_livepid" 2>/dev/null || true; wait "$_livepid" 2>/dev/null || true
_proc_is_zombie 999999999 && bad "4b.99 an unmeasurable state reads as NOT-a-zombie (conservative)"                                  "an unreadable pid was called a zombie — that licenses lock theft"                              || ok "4b.99 an unmeasurable state reads as NOT-a-zombie (conservative)"

# Control: a writable existing summary is FINE — the check must not reject the normal case.
okF="$TMP/ok-summary.txt"; printf 'previous content\n' > "$okF"
before=$(cat "$okF")
out=$(bash "$LAUNCHER" --summary "$okF" --log "$TMP/okf.log" -- --only file-size 2>&1); rc=$?
unit=$(printf '%s' "$out" | sed -n 's/^unit:  *//p'); [ -n "$unit" ] && echo "$unit" >> "$UNITS_FILE"
[ "$rc" = 0 ] && ok "4b.10 control: a writable existing summary still launches" \
             || bad "4b.10 control: a writable existing summary still launches" "rc=$rc: $out"
fi

echo "=== section 5: DEFAULT artifact paths are private, not predictable /tmp names ==="
# roborev job 157, Medium. The defaults used to be derived from the timestamp and pid, so
# they were guessable — and this script TRUNCATES the log with `>`, so on a multi-user box
# another local user could pre-create a symlink at the predicted path and have the launcher
# clobber any file the gate user can write. `mktemp -d` gives an unguessable 0700
# directory, which closes both the prediction and the symlink step.
if [ "$HAVE_SYSTEMD" != yes ]; then
  skipc "5.x default artifact paths" "no working 'systemd-run --user' on this host"
else
  out=$(bash "$LAUNCHER" -- --only file-size 2>&1); rc=$?
  unit=$(printf '%s' "$out" | sed -n 's/^unit:  *//p')
  [ -n "$unit" ] && echo "$unit" >> "$UNITS_FILE"
  dsum=$(printf '%s' "$out" | sed -n 's/^summary:  *//p')
  dlog=$(printf '%s' "$out" | sed -n 's/^log:  *//p')
  if [ "$rc" != 0 ] || [ -z "$dsum" ] || [ -z "$dlog" ]; then
    bad "5.1 a launch with no --summary/--log still reports both paths" "rc=$rc out=$out"
  else
    ok "5.1 a launch with no --summary/--log still reports both paths"
    # Both defaults must live in ONE private directory...
    ddir=$(dirname "$dsum")
    [ "$(dirname "$dlog")" = "$ddir" ] \
      && ok "5.2 both default artifacts share one directory" \
      || bad "5.2 both default artifacts share one directory" "$dsum vs $dlog"
    # ...whose mode is owner-only. This is the property that defeats the symlink step.
    mode=$(ls -ld "$ddir" 2>/dev/null | cut -c1-10)
    case "$mode" in
      drwx------) ok "5.3 the private directory is owner-only ($mode)" ;;
      *)          bad "5.3 the private directory is owner-only" "mode is '$mode', want drwx------" ;;
    esac
    # ...and the name must NOT be the old predictable timestamp-pid shape, which is what
    # made pre-creation possible. A control on the FIX, not just on the mode.
    case "$(basename "$dsum")" in
      gate-summary-*Z-*) bad "5.4 the default summary name is not the predictable timestamp-pid form" "$(basename "$dsum")" ;;
      *)                 ok "5.4 the default summary name is not the predictable timestamp-pid form" ;;
    esac
    case "$ddir" in
      */cqlite-gate-*) ok "5.5 the default directory is an mkdtemp under TMPDIR" ;;
      *)               bad "5.5 the default directory is an mkdtemp under TMPDIR" "$ddir" ;;
    esac
    # And the gate must still actually work through the default path.
    for ((_i_=0; _i_<60; _i_++)); do
      grep -qE '^RESULT: (PASS|FAIL|PARTIAL|ERROR|REFUSED)' "$dsum" 2>/dev/null && break
      sleep 2
    done
    grep -qE '^RESULT: ' "$dsum" 2>/dev/null \
      && ok "5.6 the default-path gate reaches a verdict" \
      || bad "5.6 the default-path gate reaches a verdict" "no RESULT in $dsum"
    rm -rf "$ddir" 2>/dev/null || true
  fi
fi

# --- roborev job 204: the carve-out must exclude ARTIFACTS, never SUBTREES -----------------------
# A `case` glob matches `/` — both `*` and `?` — so an arm ending in a wildcard excludes everything
# beneath a directory of that name, and a source file placed there is invisible to tree-integrity:
# a false clean result from the gate of record. Job 203 narrowed one arm and left two. Exercised
# through the REAL predicate, lifted out of agent-gate.sh, so this cannot drift from the shipped code.
_tx="$TMP/tree-excluded.sh"
{
  echo 'TREE_EXCLUDE_REL=".agent-gate-summary.txt"; TREE_STDOUT_REL=""; TREE_STDERR_REL=""'
  sed -n '/^_tree_excluded()/,/^}/p' "$REPO_ROOT/scripts/agent-gate.sh"
} > "$_tx"
_texcl() { ( . "$_tx"; _tree_excluded "$1" ); }
_tx_fails=0
# The real artifacts must still be excused...
for _a in ".agent-gate-summary.txt.heartbeat" \
          ".agent-gate-summary.txt.heartbeat.tmp.aB3xyZ" \
          ".agent-gate-summary.txt.integrity-fail.run-1" \
          ".agent-gate-summary.txt.launch-lock" \
          ".agent-gate-summary.txt.launch-lock.mutex"; do
  _texcl "$_a" || { _tx_fails=$((_tx_fails+1)); echo "     not excluded: $_a"; }
done
[ "$_tx_fails" = 0 ] && ok "4b.100 every real sibling artifact is still excused" \
                     || bad "4b.100 every real sibling artifact is still excused" "$_tx_fails missing"
# ...and NOTHING nested may be, on ANY arm.
_tx_leaks=0
for _n in ".agent-gate-summary.txt.heartbeat.tmp.foo/src/lib.rs" \
          ".agent-gate-summary.txt.heartbeat.tmp.a/b/cd" \
          ".agent-gate-summary.txt.integrity-fail.x/src/main.rs" \
          ".agent-gate-summary.txt.launch-lock/src/evil.rs"; do
  if _texcl "$_n"; then _tx_leaks=$((_tx_leaks+1)); echo "     EXCLUDED a nested path: $_n"; fi
done
[ "$_tx_leaks" = 0 ] && ok "4b.101 no arm excludes a nested path (subtree blindness closed)" \
                     || bad "4b.101 no arm excludes a nested path" "$_tx_leaks arm(s) still excuse a subtree"

# --- roborev job 206 (High): LINGERING is a second, equally load-bearing precondition ------------
# Escaping the pane cgroup is necessary but not sufficient. Without lingering the USER MANAGER is
# stopped when the last session ends, and stopping `user@<uid>.service` tears down the transient unit
# holding the gate — so the gate still dies at logout. `systemd-run --user` succeeding proves the
# manager is running NOW, not that it survives a logout.
#
# Tested through a STUBBED `loginctl` on PATH, so these cases assert the launcher's logic rather than
# this host's configuration — and so they keep working on a box configured either way.
_lstub() {  # <what-loginctl-should-do> -> prints a PATH prefix dir
  local d; d=$(mktemp -d "$TMP/lstub.XXXXXX")
  { echo '#!/usr/bin/env bash'
    case "$1" in
      yes)     echo 'printf "yes\n"' ;;
      no)      echo 'printf "no\n"' ;;
      silent)  echo 'exit 0' ;;              # answers nothing
      broken)  echo 'exit 1' ;;              # cannot answer
    esac
  } > "$d/loginctl"
  chmod +x "$d/loginctl"
  printf '%s' "$d"
}
if [ "$HAVE_SYSTEMD" = yes ]; then
  _d=$(_lstub no)
  _o=$(PATH="$_d:$PATH" bash "$LAUNCHER" --summary "$TMP/lg1.txt" --log "$TMP/lg1.log" -- --only fmt 2>&1); _r=$?
  if [ "$_r" = 69 ] && printf '%s' "$_o" | grep -q 'lingering is DISABLED'; then
    ok "4b.115 Linger=no REFUSES with 69 rather than claiming an unprotected gate is protected"
  else
    bad "4b.115 Linger=no refuses with 69" "exit $_r: $_o"
  fi
  # A refusal is only useful if it says what to do about it.
  printf '%s' "$_o" | grep -q 'loginctl enable-linger' \
    && ok "4b.116 the Linger=no refusal names the one-command remedy" \
    || bad "4b.116 the Linger=no refusal names the remedy" "no remedy in the message"
  # UNMEASURABLE must refuse too: a positive verdict needs an affirmative measurement, and
  # "I could not ask" is not one. Both shapes — answers nothing, and cannot answer.
  _bad_unknown=0
  for _mode in silent broken; do
    _d2=$(_lstub "$_mode")
    _o2=$(PATH="$_d2:$PATH" bash "$LAUNCHER" --summary "$TMP/lg2.txt" --log "$TMP/lg2.log" -- --only fmt 2>&1); _r2=$?
    if [ "$_r2" != 69 ] || ! printf '%s' "$_o2" | grep -q 'could NOT determine'; then
      _bad_unknown=$((_bad_unknown+1)); echo "     mode=$_mode exit=$_r2"
    fi
  done
  [ "$_bad_unknown" = 0 ] \
    && ok "4b.117 an UNMEASURABLE linger state refuses too (both no-answer and cannot-answer)" \
    || bad "4b.117 an unmeasurable linger state refuses" "$_bad_unknown of 2 proceeded"
  # Control: with lingering affirmatively enabled the check must NOT be what stops the launch, or
  # the three cases above would prove only that the launcher always refuses.
  _d3=$(_lstub yes)
  _o3=$(PATH="$_d3:$PATH" bash "$LAUNCHER" --summary "$TMP/lg3.txt" --log "$TMP/lg3.log" -- --only fmt 2>&1); _r3=$?
  _u3=$(printf '%s' "$_o3" | sed -n 's/^unit:  *//p'); [ -n "$_u3" ] && echo "$_u3" >> "$UNITS_FILE"
  if printf '%s' "$_o3" | grep -qE 'lingering is DISABLED|could NOT determine'; then
    bad "4b.118 control: Linger=yes passes the precondition" "refused anyway: $_o3"
  else
    ok "4b.118 control: Linger=yes passes the precondition (exit $_r3)"
  fi
  [ -n "$_u3" ] && systemctl --user stop "$_u3" >/dev/null 2>&1
else
  skip=$((skip+4)); echo "SKIP 4b.115-4b.118 (no user systemd manager on this host)"
fi

# The fallback must accept 0 OR 2. As first written it was `if bash ...; then`, which succeeds only on
# exit 0 — so a healthy unproven-clock gate returning 2 (RUNNING) was DISCARDED and the unit stopped
# anyway. The fix job 251 added did not work for the case it was added for (job 256).
# Re-pointed (roborev job 318): this grepped the SPELLING `0|2) _hb_seen=1`, which the job-318 fix
# replaced with separate `0)` and `2)` arms so RUNNING can be gated on the unit still existing. The
# STATED property -- the fallback accepts RUNNING too, not only COMPLETE -- is unchanged and still
# holds. Asserted as TWO ARMS rather than one alternation: a fallback with only an `0)` arm is the
# defect job 251 was written for, and that is what this case exists to catch.
_j147=$(sed -n '/THIS ONE CALL IS ALLOWED TO BLOCK/,/esac/p' "$LAUNCHER")
if printf '%s' "$_j147" | grep -qE '^[[:space:]]*0\)[[:space:]]*_hb_seen=1' \
   && printf '%s' "$_j147" | grep -qE '^[[:space:]]*2\)'; then
  ok "4b.147 the blocking fallback accepts COMPLETE or RUNNING, not just COMPLETE"
else
  bad "4b.147 the blocking fallback accepts COMPLETE or RUNNING" "a RUNNING gate would still be stopped"
fi

# --- roborev job 256: the artifact-set check must be ATOMIC with the reservation -------------------
# Job 251 added the check but left it CHECK-THEN-LOCK: two concurrent launches with `--summary x` and
# `--summary x.heartbeat` could both observe no foreign reservation, both acquire their DISTINCT locks,
# and then overwrite each other's files. The sequential case was closed and the concurrent one was not.
#
# The lock must key on something the colliding launches SHARE. Every per-summary name differs by
# construction, so the key is the DIRECTORY — and a launch's artifacts all live in it. One lock, so no
# acquisition order and no deadlock; held only across check-and-acquire.
if grep -q '_dirlock=' "$LAUNCHER" && grep -q 'flock -w 30 8' "$LAUNCHER"; then
  ok "4b.148 the artifact-set check and the reservation are serialised by a per-directory lock"
else
  bad "4b.148 the check and the reservation are serialised" "check-then-lock leaves the concurrent case open"
fi
# The lock must be RELEASED after the reservation exists, not before — releasing early reopens the race,
# holding it for the gate's lifetime would block every later launch in that directory.
if sed -n '/flock -u 9/,/exec 8>&-/p' "$LAUNCHER" | grep -q 'flock -u 8'; then
  ok "4b.149 the directory lock is released AFTER the reservation is acquired"
else
  bad "4b.149 the directory lock is released after the reservation" "released too early, or never"
fi
if [ "$HAVE_SYSTEMD" = yes ]; then
  # TRULY CONCURRENT, as the finding asked. A sequential proxy cannot see a check-then-lock race.
  _cd="$TMP/conc"
  mkdir -p "$_cd"
  ( bash "$LAUNCHER" --summary "$_cd/x" --log "$_cd/a.log" -- --only fmt > "$_cd/a.out" 2>&1; echo $? > "$_cd/a.rc" ) &
  _c1=$!
  ( bash "$LAUNCHER" --summary "$_cd/x.heartbeat" --log "$_cd/b.log" -- --only fmt > "$_cd/b.out" 2>&1; echo $? > "$_cd/b.rc" ) &
  _c2=$!
  wait "$_c1" 2>/dev/null; wait "$_c2" 2>/dev/null
  for _u in $(sed -n 's/^unit:  *//p' "$_cd/a.out" "$_cd/b.out" 2>/dev/null); do echo "$_u" >> "$UNITS_FILE"; done
  _ca=$(cat "$_cd/a.rc" 2>/dev/null); _cb=$(cat "$_cd/b.rc" 2>/dev/null)
  _cwon=0
  [ "$_ca" = 0 ] && _cwon=$((_cwon+1))
  [ "$_cb" = 0 ] && _cwon=$((_cwon+1))
  if [ "$_cwon" = 1 ]; then
    ok "4b.150 of two CONCURRENT aliasing launches, exactly one wins (A=$_ca B=$_cb)"
  else
    bad "4b.150 of two concurrent aliasing launches, exactly one wins" \
        "$_cwon accepted (A=$_ca B=$_cb) — both means the race is open, neither means the lock deadlocks"
  fi
  for _u in $(sed -n 's/^unit:  *//p' "$_cd/a.out" "$_cd/b.out" 2>/dev/null); do systemctl --user stop "$_u" >/dev/null 2>&1; done
else
  skip=$((skip+1)); echo "SKIP 4b.150 (no user systemd manager on this host)"
fi

# --- CROSS-FILE AGREEMENT: the launcher's advertised confirmation cap vs the READER's real cap -----
# The launcher's refusal now tells the operator the wait can add "up to 65s where the clock domain is
# unproven". That 65 is enforced in a DIFFERENT FILE — `gate-liveness.sh`'s
# `[ "$_confirm_wait" -le 65 ] || _confirm_wait=65`. Today they agree, but only by coincidence: nothing
# tied them together, so changing the reader's cap would silently turn the launcher's message into a
# lie. Two cross-file contracts have already broken in this change that way (the probe names vs the
# gate's carve-out shape, and the pub-surface banner vs its invocation), so this one is asserted rather
# than left to luck.
_reader_cap=$(grep -oE '_confirm_wait" -le [0-9]+' "$REPO_ROOT/scripts/gate-liveness.sh" | grep -oE '[0-9]+$' | head -1)
_adv_cap=$(grep -oE 'up to [0-9]+s where the clock domain is unproven' "$LAUNCHER" | grep -oE '[0-9]+' | head -1)
if [ -z "$_reader_cap" ] || [ -z "$_adv_cap" ]; then
  bad "4b.146 the advertised confirmation cap matches the reader's real cap" \
      "could not extract one of them (reader='${_reader_cap:-?}' advertised='${_adv_cap:-?}') — this proves nothing"
elif [ "$_reader_cap" = "$_adv_cap" ]; then
  ok "4b.146 the advertised confirmation cap (${_adv_cap}s) matches the reader's enforced cap"
else
  bad "4b.146 the advertised confirmation cap matches the reader's real cap" \
      "launcher advertises ${_adv_cap}s, reader enforces ${_reader_cap}s"
fi

# --- roborev job 266: a STALE extra marker must be RECLAIMED, not tolerated -----------------------
# Job 261 reserved every write destination. When one of those paths already carried a marker whose owner
# was provably dead, the first version left it in place — with the comment "a stale marker of our own
# shape; harmless". That comment was FALSE, and it is the second time in this change that a confident
# comment licensed the defect beneath it (the first: `host` described as "not an input to any verdict"
# directly above `|| echo unknown`). A wrong comment is worse than none, because it tells the next reader
# the branch was considered.
#
# The consequence: a LIVE run's heartbeat or log stays represented by a DEAD owner, so a later launch
# reads the path as reclaimable, takes it as ITS summary, and two writers land on one file. Reproduced: a
# launch succeeded with its heartbeat lock still naming pid 999999999.
if sed -n '/A STALE MARKER MUST BE REPLACED/,/esac/p' "$LAUNCHER" | grep -q 'rm -f "$_art.launch-lock"'; then
  ok "4b.151 a provably-stale extra marker is reclaimed, not left naming a dead owner"
else
  bad "4b.151 a stale extra marker is reclaimed" "a live run would stay represented by a dead owner"
fi
if [ "$HAVE_SYSTEMD" = yes ]; then
  _sm="$TMP/stalemark"
  ln -s "unit=cqlite-gate-dead.service|pid=999999999|start=proc:1" "$_sm.txt.heartbeat.launch-lock"
  ln -s "unit=cqlite-gate-dead2.service|pid=999999998|start=proc:1" "$_sm.log.launch-lock"
  _so=$(bash "$LAUNCHER" --summary "$_sm.txt" --log "$_sm.log" -- --only roborev-lints 2>&1); _sr=$?
  _su=$(printf '%s' "$_so" | sed -n 's/^unit:  *//p'); [ -n "$_su" ] && echo "$_su" >> "$UNITS_FILE"
  _stale_left=0
  for _f in "$_sm.txt.heartbeat.launch-lock" "$_sm.log.launch-lock"; do
    case "$(readlink "$_f" 2>/dev/null)" in
      *999999*) _stale_left=$((_stale_left+1)); echo "     still names a dead owner: $(basename "$_f")" ;;
    esac
  done
  if [ "$_sr" = 0 ] && [ "$_stale_left" = 0 ] && [ -n "$_su" ]; then
    ok "4b.152 stale heartbeat AND log markers are both reclaimed to the live unit"
  else
    bad "4b.152 stale extra markers are reclaimed to the live unit" \
        "exit=$_sr stale_left=$_stale_left unit='${_su:-none}'"
  fi
  [ -n "$_su" ] && systemctl --user stop "$_su" >/dev/null 2>&1
  # CONTROL: a LIVE foreign marker must still refuse, or reclamation has become "take everything".
  _lv="$TMP/livemark"
  _lo=$(bash "$LAUNCHER" --summary "$_lv-a" --log "$_lv-shared" -- --only roborev-lints 2>&1)
  _lu=$(printf '%s' "$_lo" | sed -n 's/^unit:  *//p'); [ -n "$_lu" ] && echo "$_lu" >> "$UNITS_FILE"
  _l2=$(bash "$LAUNCHER" --summary "$_lv-b" --log "$_lv-shared" -- --only fmt 2>&1); _l2r=$?
  _l2u=$(printf '%s' "$_l2" | sed -n 's/^unit:  *//p'); [ -n "$_l2u" ] && echo "$_l2u" >> "$UNITS_FILE"
  [ "$_l2r" != 0 ] && ok "4b.153 control: a LIVE foreign marker still refuses (exit $_l2r)" \
                   || bad "4b.153 control: a live foreign marker still refuses" "accepted: $_l2"
  for _u in $_lu $_l2u; do systemctl --user stop "$_u" >/dev/null 2>&1; done
else
  skip=$((skip+2)); echo "SKIP 4b.152/4b.153 (no user systemd manager on this host)"
fi

# --- roborev job 251: an UNPROVEN clock domain must not get a healthy gate killed ------------------
# Two earlier fixes interacted. Job 221 made an unverifiable hostname ABSENT, so the clock domain reads
# unproven. Job 231 put `--no-wait` on every launcher call, so the reader cannot take a second sample.
# With an unproven clock the reader cannot judge freshness from the epoch and needs PROGRESSION — two
# samples — so every stateless call returned UNKNOWN, the loop accepts only 0|2, and after 20s the
# launcher STOPPED A HEALTHY GATE. On any host where `uname -n` fails, that is every detached launch.
#
# The fast loop stays bounded and non-blocking; the SINGLE post-loop fallback is allowed its confirmation
# wait. Tracking beat-seq progression inside the launcher would have been a second implementation of the
# reader's progression grammar, which jobs 172 and 198 exist to prevent.
_lcode2=$(grep -vE '^[[:space:]]*#' "$LAUNCHER")
_loop_nw=$(printf '%s' "$_lcode2" | grep -c 'bash "\$REPO_ROOT/scripts/gate-liveness\.sh" "\$SUMMARY" --run-id "\$_new_rid" --no-wait' || true)
# Re-pointed by job 256: the fallback is no longer `if bash ...; then` — it is a bare call followed by
# `case "$?" in 0|2)`, because accepting only exit 0 discarded the RUNNING verdict this fallback exists to
# receive. Matching the OLD shape made this assertion report fallback-blocking=0 against a correct fix.
_fallback=$(printf '%s' "$_lcode2" | grep -c 'gate-liveness\.sh" "\$SUMMARY" --run-id "\$_new_rid" >/dev/null 2>&1$' || true)
if [ "$_loop_nw" -ge 1 ] && [ "$_fallback" = 1 ]; then
  ok "4b.141 the loop call is non-blocking and exactly ONE fallback may block"
else
  bad "4b.141 the loop is non-blocking and exactly one fallback may block" "loop --no-wait=$_loop_nw fallback-blocking=$_fallback"
fi
if [ "$HAVE_SYSTEMD" = yes ]; then
  # Behavioural: stub `uname` to FAIL, so the beater omits host and the clock domain is unproven. This is
  # the configuration that killed every launch.
  _ud2=$(mktemp -d "$TMP/unamefail.XXXXXX")
  printf '#!/usr/bin/env bash\nexit 1\n' > "$_ud2/uname"; chmod +x "$_ud2/uname"
  # THE TEST NEEDS A DISCRIMINATOR, NOT A SLOWER COMPONENT. `exit 0` cannot distinguish "the blocking
  # fallback accepted RUNNING" from "the gate finished before we looked", and this case passed for the
  # second reason while the fallback still discarded RUNNING=2 (job 256). Two component guesses were both
  # wrong: `--only fmt` finishes in ~1s, and `--only scoped-tests` in ~5s on a warm cache.
  #
  # ELAPSED TIME is the discriminator: the fast loop runs 20s before the fallback is even reached, so an
  # acceptance in >20s can only have come from the fallback. `--only roborev-lints` (~41s) outlives the
  # loop reliably. Measured: exit 0 at 45s with `host:` absent from the beat.
  _ut0=$(date +%s)
  _uo=$(PATH="$_ud2:$PATH" bash "$LAUNCHER" --summary "$TMP/unproven.txt" --log "$TMP/unproven.log" -- --only roborev-lints 2>&1); _ur=$?
  _uel=$(( $(date +%s) - _ut0 ))
  # The premise, asserted rather than assumed: the beater must have OMITTED host, or the clock domain was
  # never unproven and this case tests nothing about job 251.
  # `grep -c` EXITS 1 WHEN THE COUNT IS ZERO, so `$(grep -c ... || echo 0)` yields "0" from grep AND "0"
  # from the fallback — a two-line value that compares unequal to 0 and fired this very premise check
  # against a beat that correctly omitted host. The guard misread its own measurement, in the failing
  # direction. No `||`: capture the count, and default only when grep printed NOTHING (a missing file,
  # which exits 2).
  _uhost=$(grep -c '^host:' "$TMP/unproven.txt.heartbeat" 2>/dev/null)
  _uhost=${_uhost:-0}
  _uu=$(printf '%s' "$_uo" | sed -n 's/^unit:  *//p'); [ -n "$_uu" ] && echo "$_uu" >> "$UNITS_FILE"
  if [ "$_uhost" != 0 ]; then
    bad "4b.142 a healthy gate launches with an UNPROVEN clock domain" \
        "the beat carries host, so the clock domain was PROVEN — the premise failed and this proves nothing"
  elif [ "$_ur" != 0 ]; then
    bad "4b.142 a healthy gate launches with an UNPROVEN clock domain" "exit $_ur after ${_uel}s: $_uo"
  elif [ "$_uel" -le 20 ]; then
    bad "4b.142 a healthy gate launches with an UNPROVEN clock domain" \
        "accepted in ${_uel}s, so COMPLETION answered, not the blocking fallback — the job-256 path is untested"
  else
    ok "4b.142 an unproven-clock gate is accepted by the BLOCKING FALLBACK (${_uel}s > 20s loop)"
  fi
  [ -n "$_uu" ] && systemctl --user stop "$_uu" >/dev/null 2>&1
else
  skip=$((skip+1)); echo "SKIP 4b.142 (no user systemd manager on this host)"
fi

# --- roborev job 251: the reservation must cover the ARTIFACT SET, not one path --------------------
# The lock is NAMED AFTER the summary, so two launches whose summary paths differ can both acquire and
# still collide: with `--summary x` and `--summary x.heartbeat`, A locks x.launch-lock and B locks
# x.heartbeat.launch-lock — both succeed — and A's BEATER then overwrites B's SUMMARY every interval,
# destroying its terminal verdict. Measured before the fix: both launches returned 0.
if grep -q '_foreign_reservation()' "$LAUNCHER" && grep -q 'artifact-set collision' "$LAUNCHER"; then
  ok "4b.143 the launcher checks whether its artifacts are another run's reserved summary"
else
  bad "4b.143 the launcher checks its artifact set" "only the literal summary path is protected"
fi
if [ "$HAVE_SYSTEMD" = yes ]; then
  _ax="$TMP/alias-x"
  _ao=$(bash "$LAUNCHER" --summary "$_ax" --log "$TMP/alias-a.log" -- --only roborev-lints 2>&1); _ar=$?
  _au=$(printf '%s' "$_ao" | sed -n 's/^unit:  *//p'); [ -n "$_au" ] && echo "$_au" >> "$UNITS_FILE"
  _bo=$(bash "$LAUNCHER" --summary "$_ax.heartbeat" --log "$TMP/alias-b.log" -- --only fmt 2>&1); _br=$?
  _bu=$(printf '%s' "$_bo" | sed -n 's/^unit:  *//p'); [ -n "$_bu" ] && echo "$_bu" >> "$UNITS_FILE"
  # Once every write destination is reserved (job 261), this case trips the SUMMARY-first check: B's
  # summary path IS reserved — by A, as A's heartbeat. The accurate statement is "already owned by a LIVE
  # run", and the reservation target records the owner rather than the role, so the launcher cannot
  # honestly say which. The generic "artifact-set collision" wording remains reachable for the LOG
  # collision, covered by 4b.144b.
  if [ "$_ar" = 0 ] && [ "$_br" != 0 ] && printf '%s' "$_bo" | grep -q 'already owned by a LIVE run'; then
    ok "4b.144 a launch whose SUMMARY is a live run's beat destination is refused (A=$_ar B=$_br)"
  else
    bad "4b.144 an aliasing launch is refused" "A=$_ar B=$_br: $_bo"
  fi
  # THE LOG COLLISION — the case job 261 was filed for, and the one the generic message serves. A holds
  # `--log x`; B then asks for `--summary x`. Before every destination was reserved, A held no claim on
  # its log at all and B was accepted while A wrote into B's summary.
  _lg="$TMP/logcoll"
  _la=$(bash "$LAUNCHER" --summary "$_lg-a" --log "$_lg-shared" -- --only roborev-lints 2>&1); _lar=$?
  _lau=$(printf '%s' "$_la" | sed -n 's/^unit:  *//p'); [ -n "$_lau" ] && echo "$_lau" >> "$UNITS_FILE"
  _lb=$(bash "$LAUNCHER" --summary "$_lg-shared" --log "$_lg-c" -- --only fmt 2>&1); _lbr=$?
  _lbu=$(printf '%s' "$_lb" | sed -n 's/^unit:  *//p'); [ -n "$_lbu" ] && echo "$_lbu" >> "$UNITS_FILE"
  if [ "$_lar" = 0 ] && [ "$_lbr" != 0 ]; then
    ok "4b.144b a launch whose SUMMARY is a live run's LOG is refused (A=$_lar B=$_lbr)"
  else
    bad "4b.144b a launch whose summary is a live run's log is refused" "A=$_lar B=$_lbr: $_lb"
  fi
  for _u in $_lau $_lbu; do systemctl --user stop "$_u" >/dev/null 2>&1; done
  # CONTROL: disjoint launches must still both work, or "refuse everything" would pass 4b.144.
  _co=$(bash "$LAUNCHER" --summary "$TMP/alias-disjoint" --log "$TMP/alias-c.log" -- --only fmt 2>&1); _cr=$?
  _cu=$(printf '%s' "$_co" | sed -n 's/^unit:  *//p'); [ -n "$_cu" ] && echo "$_cu" >> "$UNITS_FILE"
  [ "$_cr" = 0 ] && ok "4b.145 control: a DISJOINT launch still succeeds" \
                 || bad "4b.145 control: a disjoint launch still succeeds" "exit $_cr: $_co"
  for _u in $_au $_bu $_cu; do systemctl --user stop "$_u" >/dev/null 2>&1; done
else
  skip=$((skip+2)); echo "SKIP 4b.144/4b.145 (no user systemd manager on this host)"
fi

# --- roborev job 241: the state grammar must be closed on the TERMINAL side ----------------------
# Job 205 fixed this function's EXIT-CODE form: `is-active --quiet` answers 0 only for "active", so every
# other outcome fell into "dead, reclaim it". The replacement then listed the LIVE states and made
# everything else `return 1` — so `maintenance`, or any state a future systemd introduces, read as
# AFFIRMATIVELY GONE and a live reservation could be reclaimed, putting two gates on one summary path.
# The same rule, the same function, two rounds apart, one layer over. Knowing the rule and having just
# applied it did not prevent violating it; only closing the grammar on the terminal side does.
# The grammar now lives in `_unit_state` (job 319 made the state three-valued so each caller names its
# polarity), and `_unit_is_live` is a thin wrapper over it. This test follows the PROPERTY to wherever
# the state is decided rather than pinning one function's spelling — it failed on a correct refactor,
# which is the shape a test agents learn to delete.
_uil=$(sed -n '/^_unit_state()/,/^}/p' "$LAUNCHER"
       sed -n '/^_unit_is_live()/,/^}/p' "$LAUNCHER")
_us_only=$(sed -n '/^_unit_state()/,/^}/p' "$LAUNCHER")
if printf '%s' "$_us_only" | grep -qE "inactive\|failed\) +printf 'terminal'" \
   && printf '%s' "$_us_only" | grep -qE "\*\) +printf 'unknown'"; then
  ok "4b.139 only inactive|failed are terminal; every other state reads LIVE"
else
  bad "4b.139 only inactive|failed are terminal" "the grammar is closed on the wrong side"
fi
# Behavioural, through the shipped function with a stubbed systemctl, because the grep above cannot show
# what an UNRECOGNISED value does — and that is the whole defect.
_ud=$(mktemp -d "$TMP/uilstub.XXXXXX")
printf '#!/usr/bin/env bash\nprintf "%%s\\n" "$FAKE_STATE"\n' > "$_ud/systemctl"
chmod +x "$_ud/systemctl"
_uil_bad=0
_uil_check() {  # <state> <expected LIVE|GONE>
  local got
  # `$_uil` carries _unit_state AND _unit_is_live: the wrapper delegates, so extracting only the
  # wrapper produced `_unit_state: command not found` and a uniform GONE — a false RED on correct code.
  if ( export PATH="$_ud:$PATH"; eval "$_uil"; FAKE_STATE="$1" _unit_is_live fake.service ); then got=LIVE; else got=GONE; fi
  [ "$got" = "$2" ] || { _uil_bad=$((_uil_bad+1)); echo "     state '${1:-<empty>}' -> $got, expected $2"; }
}
for st in active activating reloading refreshing deactivating maintenance some-future-state ""; do
  _uil_check "$st" LIVE
done
_uil_check inactive GONE
_uil_check failed GONE
[ "$_uil_bad" = 0 ] && ok "4b.140 state classification: 8 live/unknown states LIVE, only inactive+failed GONE" \
                    || bad "4b.140 state classification" "$_uil_bad state(s) misclassified"

# --- roborev job 231: a deadline must bound the BLOCKING CALL, not just the loop top ---------------
# Job 228 added a wall-clock deadline checked before each iteration. That bounds nothing on its own: the
# reader itself sleeps `interval + 5` (capped 65s) to confirm whether a non-advancing beat is stalled, so
# ONE call could overshoot the advertised 20s by more than three times. The launcher's own calls now pass
# `--no-wait`, which can only WEAKEN a verdict to UNKNOWN — and this loop already treats UNKNOWN as
# "keep waiting", so nothing is lost.
# COUNT INVOCATIONS, NOT MENTIONS. The first version matched `gate-liveness.sh" "$SUMMARY"`, which also
# matches the `printf` that BUILDS the advertised poll command — so this case demanded `--no-wait` on a
# string construction while 4b.138 demanded its absence there. Two of my own tests contradicting each
# other, caught by this one failing "2 of 3 bounded". An invocation is preceded by `bash `; in the
# printf the script path is an ARGUMENT to printf, so the distinction is exact.
# NARROWED BY JOB 251, and this case is why the change was caught. "EVERY call is non-blocking" was right
# about the LOOP and wrong as a blanket: it made an UNPROVEN clock domain unanswerable, because the reader
# needs a second sample to judge progression, `--no-wait` denies it, and the launcher then killed healthy
# gates. The asymmetric requirement now lives in 4b.141 (loop non-blocking AND exactly one fallback may
# block), which is STRICTER than the old blanket — it forbids zero blocking calls (the job-251 bug) and
# more than one (unbounded), where a count of `--no-wait` occurrences permitted either. This case keeps
# only the loop half, since that is the part the deadline depends on.
_lcnw=$(grep -vE '^[[:space:]]*#' "$LAUNCHER" | grep -c 'bash "\$REPO_ROOT/scripts/gate-liveness\.sh" "\$SUMMARY" --run-id "\$_new_rid" --no-wait' || true)
if [ "$_lcnw" -ge 1 ]; then
  ok "4b.137 the LOOP's reader call is non-blocking (the deadline depends on it)"
else
  bad "4b.137 the loop's reader call is non-blocking" "an unbounded call inside the loop defeats the deadline"
fi
# ...and the OPPOSITE requirement on the command it ADVERTISES: a human polling wants the stall
# confirmation, so the printed command must NOT carry --no-wait. Two requirements, one script; asserting
# only the first would let a well-meaning sweep bound the advertised command too and silently remove
# STALLED from every human poll.
if grep -q "POLL_CMD=\$(printf 'bash %q %q --run-id %q'" "$LAUNCHER" \
   && ! grep -q "POLL_CMD=.*--no-wait" "$LAUNCHER"; then
  ok "4b.138 the ADVERTISED poll command keeps the confirmation (no --no-wait)"
else
  bad "4b.138 the advertised poll command keeps the confirmation" "a human poll would lose STALLED"
fi

# --- roborev job 228: the verification phase needs a WALL-CLOCK bound, not an iteration count ------
# The loop advertises "within 20s" and runs up to 40 iterations, but each iteration may call
# `gate-liveness.sh`, which BLOCKS for `interval + 5` (capped at 65s) to confirm whether a
# non-advancing beat is stalled. Forty of those is roughly seventeen minutes, so an unmonitorable gate
# could run far longer than the message promised. A count bounds work only when each unit of work is
# bounded, and this one is not.
if grep -q '_verify_deadline=' "$LAUNCHER" \
   && grep -qF '[ "$(date +%s)" -ge "$_verify_deadline" ]' "$LAUNCHER"; then
  ok "4b.134 the verification phase is bounded by WALL CLOCK, not just an iteration count"
else
  bad "4b.134 the verification phase is bounded by wall clock" "only an iteration count bounds it"
fi
# The advertised limit and the enforced limit must be the SAME NUMBER, or the diagnostic is a claim the
# code does not keep — the same defect class as a comment asserting a property the code lacks.
_adv=$(grep -oE "within [0-9]+s" "$LAUNCHER" | head -1 | grep -oE '[0-9]+')
_enf=$(grep -oE '_verify_deadline=\$\(\( \$\(date \+%s\) \+ [0-9]+ \)\)' "$LAUNCHER" | grep -oE '\+ [0-9]+ ' | grep -oE '[0-9]+')
if [ -n "$_adv" ] && [ -n "$_enf" ] && [ "$_adv" = "$_enf" ]; then
  ok "4b.135 the advertised limit (${_adv}s) equals the enforced deadline (${_enf}s)"
else
  bad "4b.135 the advertised limit equals the enforced deadline" "advertised='${_adv:-?}' enforced='${_enf:-?}'"
fi
# COVERAGE LIMIT, stated rather than implied: the pathological path this deadline protects — a gate that
# STARTS but never publishes a beat carrying our nonce — could not be constructed here, because the
# launcher's preflight already refuses every heartbeat destination that would produce it (a directory, a
# non-regular file, an unwritable path). So the deadline is DEFENCE IN DEPTH behind preflight, and these
# two cases assert the bound exists and is honest, not that it has been observed firing. Claiming
# behavioural proof for a path I could not reach would be worse than saying so.
if [ "$HAVE_SYSTEMD" = yes ]; then
  _dt="$TMP/deadline.txt"; _ds=$(date +%s)
  _do=$(bash "$LAUNCHER" --summary "$_dt" --log "$TMP/deadline.log" -- --only fmt 2>&1); _dr=$?
  _du=$(printf '%s' "$_do" | sed -n 's/^unit:  *//p'); [ -n "$_du" ] && echo "$_du" >> "$UNITS_FILE"
  _del=$(( $(date +%s) - _ds ))
  if [ "$_dr" = 0 ] && [ "$_del" -le 25 ]; then
    ok "4b.136 a healthy launch still verifies well inside the deadline (${_del}s)"
  else
    bad "4b.136 a healthy launch verifies inside the deadline" "exit $_dr after ${_del}s"
  fi
  [ -n "$_du" ] && systemctl --user stop "$_du" >/dev/null 2>&1
else
  skip=$((skip+1)); echo "SKIP 4b.136 (no user systemd manager on this host)"
fi

# --- NO SILENT REFUSAL, asserted at the SOURCE ---------------------------------------------------
# `4b.76` established that asserting a refusal by EXIT CODE ALONE cannot see a launcher that exits
# non-zero while printing NOTHING — it caught exactly that, caused by an `exec` redirection applying to
# the whole shell. A sibling audit of this suite then found ~15 cases that check only `$rc != 0`, any of
# which would pass a silently-refusing launcher, and the launcher has refusal paths no case exercises
# at all.
#
# Rewriting 15 assertions would cover only the paths those assertions happen to visit. The PROPERTY is
# "every refusal explains itself", and it is checkable directly against the source for EVERY exit,
# tested or not. Measured at introduction: 0 violations, so this pins a clean state rather than
# excusing a dirty one.
_silent=$(python3 - "$LAUNCHER" <<'PYEOF_INNER'
import re, sys
lines = open(sys.argv[1]).read().split('\n')
bad = []
for i, l in enumerate(lines):
    m = re.match(r'\s*exit\s+(\d+)\s*$', l)
    if not m or int(m.group(1)) == 0:
        continue
    if '>&2' not in '\n'.join(lines[max(0, i - 12):i]):
        bad.append(str(i + 1))
print(','.join(bad))
PYEOF_INNER
) || _silent="PROBE-FAILED"
if [ "$_silent" = "PROBE-FAILED" ]; then
  bad "4b.133 every non-zero exit in the launcher explains itself" "the probe could not run — this proves nothing"
elif [ -z "$_silent" ]; then
  ok "4b.133 every non-zero exit in the launcher explains itself (no silent refusal)"
else
  bad "4b.133 every non-zero exit in the launcher explains itself" "silent refusal at line(s): $_silent"
fi

# --- roborev job 223: the nonce and the run-id must come from ONE snapshot ----------------------
# They were read by two separate `grep`s of a file a concurrent peer can rewrite between them, so the
# launcher could pair ITS OWN nonce with a PEER's run-id, accept the peer's heartbeat as proof of
# monitorability, and print a poll command bound to the wrong run. `gate-liveness.sh` had already
# solved this for its own reads by deciding from an immutable copy; the launcher never inherited it.
if grep -q '_snap_pair()' "$LAUNCHER" && grep -qF 'cp -- "$src" "$snap"' "$LAUNCHER"; then
  ok "4b.127 the launcher decides from an immutable snapshot, not a live file"
else
  bad "4b.127 the launcher decides from an immutable snapshot" "no snapshot helper"
fi
# The defect SHAPE must be gone: no run-id read straight from the live artifacts.
_lcode=$(grep -vE '^[[:space:]]*#' "$LAUNCHER")
if printf '%s' "$_lcode" | grep -qE 'grep -m1 .\^run-id: . "\$(_hbdest|SUMMARY)"'; then
  bad "4b.128 no run-id is read from a live artifact" "a direct read remains"
else
  ok "4b.128 no run-id is read from a live artifact"
fi
# Both facts must be checked against the SAME copy, so the nonce grep targets the snapshot too.
if printf '%s' "$_lcode" | grep -qF 'grep -qxF "launch-nonce: $LAUNCH_NONCE" "$snap"'; then
  ok "4b.129 the nonce is verified against that same snapshot"
else
  bad "4b.129 the nonce is verified against the same snapshot" "nonce and run-id may come from different reads"
fi
if [ "$HAVE_SYSTEMD" = yes ]; then
  # Behavioural, on BOTH artifact-path shapes, because PRIVDIR (where the snapshot is taken) is created
  # by one of two different blocks depending on whether the caller supplied paths.
  _sp1="$TMP/snap1.txt"
  _o1=$(bash "$LAUNCHER" --summary "$_sp1" --log "$TMP/snap1.log" -- --only fmt 2>&1); _r1=$?
  _u1=$(printf '%s' "$_o1" | sed -n 's/^unit:  *//p'); [ -n "$_u1" ] && echo "$_u1" >> "$UNITS_FILE"
  [ "$_r1" = 0 ] && ok "4b.130 explicit --summary AND --log still launch (snapshot path reachable)" \
                 || bad "4b.130 explicit --summary and --log still launch" "exit $_r1: $_o1"
  [ -n "$_u1" ] && systemctl --user stop "$_u1" >/dev/null 2>&1
  _o2=$(bash "$LAUNCHER" -- --only fmt 2>&1); _r2=$?
  _u2=$(printf '%s' "$_o2" | sed -n 's/^unit:  *//p'); [ -n "$_u2" ] && echo "$_u2" >> "$UNITS_FILE"
  [ "$_r2" = 0 ] && ok "4b.131 DEFAULT paths still launch (the other PRIVDIR block)" \
                 || bad "4b.131 default paths still launch" "exit $_r2: $_o2"
  [ -n "$_u2" ] && systemctl --user stop "$_u2" >/dev/null 2>&1
  # The snapshot must not litter: it is removed whether or not it matched.
  if ls /tmp/cqlite-gate-*/launchsnap.* >/dev/null 2>&1; then
    bad "4b.132 launch snapshots leave no litter" "$(ls /tmp/cqlite-gate-*/launchsnap.* 2>/dev/null | wc -l) left"
  else
    ok "4b.132 launch snapshots leave no litter"
  fi
else
  skip=$((skip+3)); echo "SKIP 4b.130-4b.132 (no user systemd manager on this host)"
fi

# --- roborev job 213: a gate that finishes FAST must not be refused ------------------------------
# The verification loop broke the moment the unit went inactive. A fast gate (a preflight refusal, a
# tiny `--only`) can publish its terminal summary and exit in the window between the artifact reads
# and that check, leaving `_new_rid` empty — and the post-loop terminal check is guarded on it, so a
# launch that HAD produced a verdict was refused and its unit stopped. Once the unit is inactive the
# artifacts cannot change, so one settled re-derivation races nothing.
# ANCHOR ON THE PROPERTY, NOT THE IMPLEMENTATION TEXT (roborev job 272). This case used to bound its
# sed range with the literal `is-active --quiet "$UNIT"` line and then look for any `_new_rid=`. Both
# halves were wrong the same way: job 272 replaced that line with `_unit_is_live` (because
# `is-active --quiet` reads transitional and unmeasurable states as dead), which emptied the range and
# failed this case on a CORRECT fix -- the second time in this suite a structural check pinned to
# implementation spelling has red-flagged a correct refactor. And `_new_rid=` was satisfied by the
# UNSAFE two-grep re-derivation this case nominally guarded, so it locked the defect in. It now
# requires the branch to re-derive through `_snap_pair`, which is the property that actually matters.
if grep -qE 'SETTLED SNAPSHOT|ONE IMMUTABLE SNAPSHOT' "$LAUNCHER" \
   && sed -n '/if ! _unit_is_live "\$UNIT"/,/^  fi$/p' "$LAUNCHER" | grep -q '_snap_pair'; then
  ok "4b.124 the inactive-unit branch re-derives the run-id from a settled snapshot"
else
  bad "4b.124 the inactive-unit branch re-derives from a settled snapshot" "a fast gate can still be refused"
fi
if [ "$HAVE_SYSTEMD" = yes ]; then
  # `--only fmt` is the fastest component available, i.e. the shape most likely to finish before any
  # beat is published. It must be ACCEPTED, and must reach a terminal verdict.
  _ft="$TMP/fast.txt"
  _fo=$(bash "$LAUNCHER" --summary "$_ft" --log "$TMP/fast.log" -- --only fmt 2>&1); _fr=$?
  _fu=$(printf '%s' "$_fo" | sed -n 's/^unit:  *//p'); [ -n "$_fu" ] && echo "$_fu" >> "$UNITS_FILE"
  if [ "$_fr" = 0 ]; then
    ok "4b.125 a very fast gate is ACCEPTED, not refused as unmonitorable"
  else
    bad "4b.125 a very fast gate is accepted" "exit $_fr: $_fo"
  fi
  for _i in {1..40}; do grep -qE '^RESULT: (PASS|FAIL|PARTIAL|ERROR|REFUSED)' "$_ft" 2>/dev/null && break; sleep 1; done
  if grep -qE '^RESULT: (PASS|FAIL|PARTIAL|ERROR|REFUSED)' "$_ft" 2>/dev/null; then
    ok "4b.126 ...and it reached a terminal verdict ($(sed -n 's/^RESULT: //p' "$_ft" | head -1))"
  else
    bad "4b.126 a very fast gate reached a terminal verdict" "no RESULT in $_ft"
  fi
  [ -n "$_fu" ] && systemctl --user stop "$_fu" >/dev/null 2>&1
else
  skip=$((skip+2)); echo "SKIP 4b.125/4b.126 (no user systemd manager on this host)"
fi

# --- roborev job 211 (High): the USER MANAGER's environment is a third channel ------------------
# A `--user` transient unit inherits the user manager's environment block, so anything
# `systemctl --user set-environment` holds reaches the gate. The caller-side deny-list cannot see
# that door: it stops us FORWARDING AGENT_GATE_* / summary-path variables while the manager can
# supply the same names. The concrete danger is an opt-out reaching a gate that never asked for it —
# a manager-set AGENT_GATE_ALLOW_MISSING_FIXTURES or CQLITE_ALLOW_FILE_GROWTH silently relaxes the
# gate's own validation.
# Re-pointed (roborev job 318): the interpreters are RESOLVED variables now, not literal
# /usr/bin/env and /bin/bash, so the fixed-string grep failed on a change that KEEPS the property.
# What matters here is the `-i`: the unit must start from an EMPTY environment.
if grep -qE '"[$]_env_abs" -i "[$]_bash_abs" "[$]ENV_SCRIPT"' "$LAUNCHER"; then
  ok "4b.121 the unit starts from an EMPTY environment (manager leakage cannot reach the gate)"
else
  bad "4b.121 the unit starts from an empty environment" "no env -i; manager variables reach the gate"
fi
# Absolute paths, because env -i leaves no PATH to find them with.
# Re-pointed (roborev job 318): absoluteness is ESTABLISHED now rather than hard-coded -- both are
# resolved with `command -v` and the launcher REFUSES unless each is an absolute executable, which is
# strictly stronger than a literal. Hence the CONJUNCTION: the exec uses the resolved variables AND
# both went through the validate-or-refuse loop. Asserting only the first would let a future edit
# resolve without validating -- the same hole, one step later.
if grep -qE '"[$]_env_abs" -i "[$]_bash_abs"' "$LAUNCHER" \
   && grep -qE 'for _tool_pair in .*"bash:[$]_bash_abs".*"env:[$]_env_abs"' "$LAUNCHER" \
   && grep -qE 'cannot resolve .* to an absolute path' "$LAUNCHER"; then
  ok "4b.122 env and bash are absolute paths (there is no PATH after env -i)"
else
  bad "4b.122 env and bash are absolute paths" "a bare name cannot resolve with an empty PATH"
fi
# BEHAVIOURAL, because the structural greps above cannot show the leak is actually closed. Reads the
# gate process's OWN /proc/<pid>/environ — the unit's `Environment=` property is NOT a discriminator
# (we never set it, so it reads empty with or without the fix, and would look like a pass).
# A component long enough to sample is required: `--only fmt` finishes and is --collect'ed before it
# can be read, and that produced a "0" that meant "never measured" rather than "absent".
if [ "$HAVE_SYSTEMD" = yes ]; then
  systemctl --user set-environment GATE_ENV_LEAK_PROBE=iamhere >/dev/null 2>&1
  _lt="$TMP/envleak.txt"
  _lo=$(bash "$LAUNCHER" --summary "$_lt" --log "$TMP/envleak.log" -- --only roborev-lints 2>&1)
  _lu=$(printf '%s' "$_lo" | sed -n 's/^unit:  *//p'); [ -n "$_lu" ] && echo "$_lu" >> "$UNITS_FILE"
  _cg="/sys/fs/cgroup/user.slice/user-$(id -u).slice/user@$(id -u).service/app.slice/$_lu.service/cgroup.procs"
  _gp=""
  for _i in {1..60}; do
    _gp=$(head -1 "$_cg" 2>/dev/null)
    [ -n "$_gp" ] && [ -r "/proc/$_gp/environ" ] && break
    _gp=""
  done
  if [ -n "$_gp" ]; then
    _n=$(tr '\0' '\n' < "/proc/$_gp/environ" 2>/dev/null | grep -c '^GATE_ENV_LEAK_PROBE=' || true)
    if [ "$_n" = 0 ]; then
      ok "4b.123 a manager-only variable is ABSENT from the gate's own environ (pid $_gp)"
    else
      bad "4b.123 a manager-only variable is absent from the gate's environ" \
          "GATE_ENV_LEAK_PROBE reached the gate $_n time(s) — the leak is open"
    fi
  else
    # An unsamplable probe is a FAILURE, not a skip: "could not look" must never read as "absent".
    bad "4b.123 a manager-only variable is absent from the gate's environ" \
        "MEASUREMENT DID NOT HAPPEN — no readable /proc/<pid>/environ for unit '$_lu'"
  fi
  [ -n "$_lu" ] && systemctl --user stop "$_lu" >/dev/null 2>&1
  systemctl --user unset-environment GATE_ENV_LEAK_PROBE >/dev/null 2>&1
else
  skip=$((skip+1)); echo "SKIP 4b.123 (no user systemd manager on this host)"
fi

# --- roborev job 209: .gitignore is a SECOND channel for subtree blindness ----------------------
# `_tree_excluded` was narrowed twice (jobs 203, 204) so the gate's carve-out excuses exact artifacts
# rather than whole subtrees. But tree-integrity enumerates untracked files with
# `git ls-files --others --exclude-standard`, which honours .gitignore — and a gitignore pattern
# matches a file OR A DIRECTORY of that name, and git does not descend into an ignored directory. So
# source under `.agent-gate-summary.txt.launch-lock/` was invisible anyway: the same false-clean
# behaviour through a channel the earlier fixes never touched.
#
# Asserted as the END-TO-END PROPERTY — "is planted source visible to the enumeration tree-integrity
# actually uses" — because that is what covers BOTH channels at once. Run in a throwaway git repo, so
# it never plants files in the real worktree (which would risk voiding a concurrent gate's
# tree-integrity), and the subject paths are DERIVED from the committed .gitignore, so a new artifact
# entry is covered without editing this test.
_gi="$REPO_ROOT/.gitignore"
if [ -r "$_gi" ] && command -v git >/dev/null 2>&1; then
  _gw=$(mktemp -d "$TMP/gitignore-probe.XXXXXX")
  ( cd "$_gw" && git init -q . && printf 'x\n' > seed && git add seed \
      && git -c user.email=t@t -c user.name=t commit -qm seed ) >/dev/null 2>&1
  cp "$_gi" "$_gw/.gitignore"
  # Derive the artifact entries: anchored gate-summary siblings, ignoring the negations themselves.
  _subjects=$(grep -E '^/\.agent-gate-(summary|lite-summary|delta-summary)\.txt\.' "$_gi" \
              | grep -v '^!' | sed 's|^/||' | sed 's/\*$/aB3xyZ/' | sort -u)
  _gi_n=0; _gi_blind=0
  for _sub in $_subjects; do
    _gi_n=$((_gi_n+1))
    mkdir -p "$_gw/$_sub" 2>/dev/null || continue
    echo 'fn evil() {}' > "$_gw/$_sub/evil.rs" 2>/dev/null || continue
    if ! ( cd "$_gw" && git ls-files --others --exclude-standard ) 2>/dev/null | grep -q "^$_sub/evil.rs$"; then
      _gi_blind=$((_gi_blind+1)); echo "     INVISIBLE to tree-integrity: $_sub/evil.rs"
    fi
  done
  if [ "$_gi_n" -lt 6 ]; then
    bad "4b.119 source under an artifact-named DIRECTORY is visible to tree-integrity" \
        "only $_gi_n subjects derived from .gitignore — the derivation failed, so this proves nothing"
  elif [ "$_gi_blind" = 0 ]; then
    ok "4b.119 source under any of $_gi_n artifact-named directories is VISIBLE to tree-integrity"
  else
    bad "4b.119 source under an artifact-named directory is visible to tree-integrity" \
        "$_gi_blind of $_gi_n still hide a subtree"
  fi
  # ...while the real artifact FILES must still be ignored, or the negations have merely traded
  # blindness for a dirty checkout and a heartbeat that counts against its own gate.
  _gi_noisy=0
  for _sub in $_subjects; do
    rm -rf "$_gw/$_sub" 2>/dev/null || true
    printf 'x' > "$_gw/$_sub" 2>/dev/null || continue
    ( cd "$_gw" && git check-ignore -q "$_sub" ) 2>/dev/null || { _gi_noisy=$((_gi_noisy+1)); echo "     no longer ignored: $_sub"; }
  done
  [ "$_gi_noisy" = 0 ] && ok "4b.120 the artifact FILES are still ignored (negations added no noise)" \
                       || bad "4b.120 the artifact files are still ignored" "$_gi_noisy leaked"
else
  skip=$((skip+2)); echo "SKIP 4b.119/4b.120 (no .gitignore or no git)"
fi

# --- #3473-R6 owner ruling: the lock's failure mode is INVERTED -----------------------------------
# Reclamation may follow only an AFFIRMATIVE reading of the owner's death. Every "I could not tell"
# refuses, so a defect in this component can produce a loud false refusal but NEVER two gates writing
# one summary path — the harm it exists to prevent. Noise, never blindness.
if grep -q '_live=unknown' "$LAUNCHER" && grep -q '\[ "$_live" = unknown \]' "$LAUNCHER"; then
  ok "4b.109 owner liveness is THREE-valued, and unknown reaches a refusal"
else
  bad "4b.109 owner liveness is three-valued and unknown refuses" "unknown may still reclaim"
fi
_pscode=$(sed -n '/^_pid_state()/,/^}/p' "$LAUNCHER")
if printf '%s' "$_pscode" | grep -q 'EPERM, not ESRCH'; then
  ok "4b.110 an existing-but-unsignallable pid is EXISTS, not gone (kill -0 cannot tell them apart)"
else
  bad "4b.110 an existing-but-unsignallable pid is exists" "kill -0 alone still decides"
fi
# Behavioural, through the shipped function.
eval "$_pscode"
_ps_bad=0
[ "$(_pid_state 1)" = exists ]           || { _ps_bad=$((_ps_bad+1)); echo "     pid 1 not exists"; }
[ "$(_pid_state 999999999)" = gone ]     || { _ps_bad=$((_ps_bad+1)); echo "     absent pid not gone"; }
[ "$(_pid_state abc)" = unknown ]        || { _ps_bad=$((_ps_bad+1)); echo "     non-numeric not unknown"; }
[ "$(_pid_state '')" = unknown ]         || { _ps_bad=$((_ps_bad+1)); echo "     empty not unknown"; }
[ "$_ps_bad" = 0 ] && ok "4b.111 _pid_state: live=exists, absent=gone, unparseable=unknown" \
                   || bad "4b.111 _pid_state classification" "$_ps_bad case(s) wrong"

# THE DEADLOCK CHECK the ruling explicitly demanded. A genuinely dead owner has no /proc entry, so
# its identity is unmeasurable too — inverting naively would make the NORMAL stale case
# unreclaimable and reintroduce job 196's permanent block. `gone` must therefore stay AFFIRMATIVE.
if [ "$HAVE_SYSTEMD" = yes ]; then
  _dl="$TMP/inv.txt"
  # A reservation owned by a pid that cannot exist, and a unit that does not exist.
  ln -s "unit=cqlite-gate-nonexistent-$.service|pid=999999999|start=proc:1" "$_dl.launch-lock"
  _o=$(bash "$LAUNCHER" --summary "$_dl" --log "$TMP/inv.log" -- --only file-size 2>&1); _r=$?
  _u=$(printf '%s' "$_o" | sed -n 's/^unit:  *//p'); [ -n "$_u" ] && echo "$_u" >> "$UNITS_FILE"
  if [ "$_r" = 0 ]; then
    ok "4b.112 NO DEADLOCK: a provably-dead owner is still reclaimed after the inversion"
  else
    bad "4b.112 NO DEADLOCK: a provably-dead owner is still reclaimed" \
        "the inversion made the normal stale case unreclaimable — exit $_r: $_o"
  fi
  # ...while an UNPARSEABLE owner refuses, and says how to get out of it.
  _dl2="$TMP/inv2.txt"
  ln -s "unit=|pid=abc|start=" "$_dl2.launch-lock"
  _o2=$(bash "$LAUNCHER" --summary "$_dl2" --log "$TMP/inv2.log" -- --only file-size 2>&1); _r2=$?
  _u2=$(printf '%s' "$_o2" | sed -n 's/^unit:  *//p'); [ -n "$_u2" ] && echo "$_u2" >> "$UNITS_FILE"
  if [ "$_r2" != 0 ] && printf '%s' "$_o2" | grep -q 'could NOT be'; then
    ok "4b.113 an unparseable owner REFUSES instead of being reclaimed (exit $_r2)"
  else
    bad "4b.113 an unparseable owner refuses instead of being reclaimed" "exit $_r2: $_o2"
  fi
  # A refusal with no way out IS a permanent block, so the message must name the remedy.
  if printf '%s' "$_o2" | grep -q 'remove that one file and retry'; then
    ok "4b.114 the unknown-owner refusal names its manual remedy (not a silent dead end)"
  else
    bad "4b.114 the unknown-owner refusal names its manual remedy" "no remedy in the message"
  fi
else
  skip=$((skip+3)); echo "SKIP 4b.112/4b.113/4b.114 (no user systemd manager on this host)"
fi

# --- roborev job 205: the launcher's artifacts and the gate's carve-out must AGREE -------------
# gate-detached.sh writes artifacts beside the summary; agent-gate.sh's tree-integrity carve-out
# decides which sibling names are excused. Those are two files with one contract, and job 204 broke
# it: narrowing the carve-out to the six-character mktemp shape put the launcher's
# `.heartbeat.tmp.probeXXXXXX` outside it, so a concurrent gate would call the probe a tree mutation
# and FAIL ITSELF. The dependency was stated in a comment at the probe and still broke, because a
# comment is read by whoever happens to look.
#
# DERIVED, not curated (CLAUDE.md): every `$SUMMARY.`-anchored name in the launcher is extracted from
# source at run time and put through the gate's REAL predicate, so a newly-added artifact shape is
# covered without editing this test.
_shapes=$(grep -oE '\$SUMMARY\.[A-Za-z0-9._-]+' "$LAUNCHER" | sed 's/^\$SUMMARY//' | sort -u)
# `$_reserve` is `$SUMMARY.launch-lock`, so names built on IT are siblings too.
_shapes="$_shapes
.launch-lock.mutex"
_shape_n=0; _shape_bad=0
for _sh in $_shapes; do
  [ -n "$_sh" ] || continue
  _concrete=${_sh//XXXXXX/aB3xyZ}
  case "$_concrete" in *X*) continue ;; esac   # an unresolved template we cannot instantiate
  _shape_n=$((_shape_n+1))
  if ! _texcl ".agent-gate-summary.txt$_concrete"; then
    _shape_bad=$((_shape_bad+1)); echo "     NOT excused by the gate: .agent-gate-summary.txt$_concrete"
  fi
done
# RUNTIME-OBSERVED ARTIFACTS, because the source-derived list above is INCOMPLETE BY CONSTRUCTION
# (found by auditing this very test, job 261). It greps literal `$SUMMARY.x` occurrences — but the
# launcher now COMPOSES paths (`"$_art.launch-lock"` where `_art="$SUMMARY.heartbeat"`), so
# `$SUMMARY.heartbeat.launch-lock` never appears as a literal and the derivation cannot see it. The gate
# excuses that name only because a carve-out was added by hand; this case would have passed regardless,
# which is a false negative in the guard that exists to prevent exactly that.
#
# So a real launch is performed and EVERY file that appears beside the summary is checked against the
# gate's predicate. Observation covers composed names, and anything added later, without curation.
if [ "$HAVE_SYSTEMD" = yes ]; then
  _obsdir=$(mktemp -d "$TMP/observed.XXXXXX")
  _oo=$(bash "$LAUNCHER" --summary "$_obsdir/s.txt" --log "$_obsdir/s.log" -- --only fmt 2>&1)
  _ou=$(printf '%s' "$_oo" | sed -n 's/^unit:  *//p'); [ -n "$_ou" ] && echo "$_ou" >> "$UNITS_FILE"
  _obs_n=0; _obs_bad=0
  for _f in "$_obsdir"/*; do
    [ -e "$_f" ] || [ -L "$_f" ] || continue
    _base=$(basename "$_f")
    # The LOG's own lock is a gate artifact too, and skipping `s.log.*` is how the first version of this
    # case would have missed it. The log is excused by the gate's STDOUT carve-out, so it is checked
    # against that name rather than the summary prefix.
    case "$_base" in
      s.log) continue ;;                                  # the log itself, excused as TREE_STDOUT_REL
      s.log.launch-lock)
        _obs_n=$((_obs_n+1))
        # SET THE VARIABLES AFTER SOURCING. `$_tx` begins with a line that resets TREE_STDOUT_REL to
        # "", so assignments made before the source were silently clobbered and the log's lock read as
        # unexcused — a defect in this harness, not in the gate.
        ( . "$_tx"; TREE_EXCLUDE_REL=".agent-gate-summary.txt"; TREE_STDOUT_REL="gate.log"; TREE_STDERR_REL=""
          _tree_excluded "gate.log.launch-lock" ) \
          || { _obs_bad=$((_obs_bad+1)); echo "     observed but NOT excused: <log>.launch-lock"; }
        continue ;;
    esac
    case "$_base" in s.txt) continue ;; esac           # the summary itself is the carve-out anchor
    _obs_n=$((_obs_n+1))
    _texcl ".agent-gate-summary.txt${_base#s.txt}" || {
      _obs_bad=$((_obs_bad+1)); echo "     observed but NOT excused: ${_base#s.txt}"
    }
  done
  [ -n "$_ou" ] && systemctl --user stop "$_ou" >/dev/null 2>&1
  if [ "$_obs_n" -lt 1 ]; then
    bad "4b.105b every OBSERVED artifact is excused by the gate's carve-out" \
        "a real launch produced no observable artifacts — the observation failed, so this proves nothing"
  elif [ "$_obs_bad" = 0 ]; then
    ok "4b.105b all $_obs_n artifacts a REAL launch creates are excused by the gate's carve-out"
  else
    bad "4b.105b every observed artifact is excused" "$_obs_bad of $_obs_n would be read as a tree mutation"
  fi
else
  skip=$((skip+1)); echo "SKIP 4b.105b (no user systemd manager on this host)"
fi
if [ "$_shape_n" -lt 3 ]; then
  bad "4b.105 every launcher artifact shape is excused by the gate's carve-out" \
      "only $_shape_n shapes derived — the derivation failed, so this proves nothing"
elif [ "$_shape_bad" = 0 ]; then
  ok "4b.105 all $_shape_n derived launcher artifact shapes are excused by the gate's carve-out"
else
  bad "4b.105 every launcher artifact shape is excused by the gate's carve-out" \
      "$_shape_bad of $_shape_n would be read as a tree mutation"
fi

# Unit liveness must be an AFFIRMATIVE terminal reading. `is-active --quiet` answers 0 only for
# exactly "active", so `activating` (a unit still STARTING) and any query failure fell into the
# "dead, reclaim it" branch — two gates on one summary path.
if grep -q '_unit_is_live' "$LAUNCHER" && ! grep -q 'is-active --quiet "$_own_unit"' "$LAUNCHER"; then
  ok "4b.106 reservation liveness reads ActiveState, not a bare is-active exit code"
else
  bad "4b.106 reservation liveness reads ActiveState" "a bare is-active still decides reclamation"
fi
# BEHAVIOURAL, not a grep for the state NAMES (job 319). The closed terminal grammar deliberately
# stopped ENUMERATING the transitional states — they fall to `*) live`, which is what makes a state
# systemd invents later safe — so a test demanding the names in the source FAILS on the stronger
# implementation. Ask the function instead.
_lifecode=$(sed -n '/^_unit_state()/,/^}/p' "$LAUNCHER"
            sed -n '/^_unit_is_live()/,/^}/p' "$LAUNCHER")
_ud107=$(mktemp -d "$TMP/uil107.XXXXXX")
printf '#!/usr/bin/env bash\nif [ "$FAKE_STATE" = FAIL ]; then exit 1; fi\nprintf "%%s\\n" "$FAKE_STATE"\n' > "$_ud107/systemctl"
chmod +x "$_ud107/systemctl"
_missing=""
for _st in activating deactivating reloading; do
  ( export PATH="$_ud107:$PATH"; eval "$_lifecode"; FAKE_STATE="$_st" _unit_is_live fake.service ) \
    || _missing="$_missing $_st"
done
[ -z "$_missing" ] && ok "4b.107 transitional unit states count as LIVE" \
                   || bad "4b.107 transitional unit states count as live" "read as GONE:$_missing"
# Likewise behavioural: the old form grepped for a COMMENT, which any rewrite silently defeats.
if ( export PATH="$_ud107:$PATH"; eval "$_lifecode"; FAKE_STATE=FAIL _unit_is_live fake.service ); then
  ok "4b.108 an unmeasurable unit state counts as LIVE (refuse), never as dead"
else
  bad "4b.108 an unmeasurable unit state counts as live" "unmeasurable may reach the reclaim branch"
fi

# --- roborev job 204: cleanup must not signal a pid it cannot prove is ours ----------------------
# A pid is not an identity. These suites kill processes long before cleanup, and pids are reused, so
# an unverified `kill` can hit an unrelated same-user process — including a concurrent gate.
if grep -q 'kill_tracked' "$0" && grep -q '\[ "$now" = "$want" \] || continue' "$0"; then
  ok "4b.102 cleanup signals only pids whose start identity still matches"
else
  bad "4b.102 cleanup signals only pids whose start identity still matches" "unverified kill remains"
fi
# Behavioural: a RECYCLED pid record must not be signalled. Simulated by recording a pid with a
# deliberately wrong identity — the shape a reused pid presents — against a process we then check
# survived. Uses the suite's own helpers, so it tests the shipped mechanism.
sleep 20 & _victim=$!
printf '%s\t%s\n' "$_victim" "proc:0-not-the-real-identity" >> "$TMP/tracked-pids"
kill_tracked -TERM
if kill -0 "$_victim" 2>/dev/null; then
  ok "4b.103 a pid whose identity does NOT match is left alone (pid reuse is survivable)"
else
  bad "4b.103 a pid whose identity does not match is left alone" "the victim was signalled"
fi
kill -9 "$_victim" 2>/dev/null || true; wait "$_victim" 2>/dev/null || true
# Control: a CORRECTLY recorded pid IS signalled — or the case above would pass by doing nothing.
sleep 20 & _target=$!
remember_pid "$_target"
kill_tracked -TERM
sleep 0.3
if kill -0 "$_target" 2>/dev/null; then
  bad "4b.104 control: a correctly-identified pid IS signalled" "not signalled — 4b.103 proves nothing"
  kill -9 "$_target" 2>/dev/null || true
else
  ok "4b.104 control: a correctly-identified pid IS signalled"
fi
wait "$_target" 2>/dev/null || true

# --- roborev job 269: the three Mediums -----------------------------------------------------------
# F1. The rollback list was a space-joined string iterated UNQUOTED, so it word-split and
# glob-expanded. Structural pin first: an array, iterated quoted, and NO unquoted iteration left.
_f1_src="$REPO_ROOT/scripts/flow/gate-detached.sh"
# The property is "an ARRAY, iterated with the elements QUOTED" — not one spelling of it. Job 319
# replaced the iteration with the bash-3.2-safe ${A[@]+"${A[@]}"}, which still quotes every element
# (verified behaviourally by 4b.189 below), so accept either and keep refusing the unquoted string.
if grep -q '^_extra_locks=()' "$_f1_src" \
   && { grep -q 'for _l in "${_extra_locks\[@\]}"' "$_f1_src" \
        || grep -q 'for _l in ${_extra_locks\[@\]+"${_extra_locks\[@\]}"}' "$_f1_src"; } \
   && ! grep -q 'in \$_extra_locks' "$_f1_src"; then
  ok "4b.154 the rollback list is an ARRAY iterated QUOTED (no word-split, no glob)"
else
  bad "4b.154 the rollback list is an array iterated quoted" \
      "$(grep -n '_extra_locks' "$_f1_src" | head -4)"
fi

# 4b.189 (job 319): 4b.154 now accepts a SECOND spelling, so the equivalence it rests on is proved
# here rather than assumed. ${A[@]+"${A[@]}"} must (a) preserve every element verbatim, (b) NOT
# word-split or glob-expand, and (c) expand to NOTHING when the array is empty — which is the whole
# reason for the form, since a bare "${A[@]}" aborts under `set -u` on bash 3.2.
_q189d=$(mktemp -d "$TMP/q189.XXXXXX"); : > "$_q189d/GLOBBED"
_q189=$( cd "$_q189d" && set -uo pipefail
  _a=("a b" "*" "c"); _n=0; _seen=""
  for _x in ${_a[@]+"${_a[@]}"}; do _n=$((_n+1)); _seen="$_seen|$_x"; done
  _e=(); _m=0
  for _x in ${_e[@]+"${_e[@]}"}; do _m=$((_m+1)); done
  printf '%s %s %s' "$_n" "$_m" "$_seen" )
if [ "$_q189" = '3 0 |a b|*|c' ]; then
  ok "4b.189 the safe form preserves elements, does not split or glob, and vanishes when empty"
else
  bad "4b.189 the safe form preserves elements, does not split or glob, vanishes when empty" \
      "got '$_q189' (expected '3 0 |a b|*|c'; a GLOBBED element means the form expanded unquoted)"
fi

# NOT a discriminator for the array fix, and the honest version of this case says so. I wrote it as
# one -- "force the rollback with a space-bearing path and require the lock to be gone" -- and MEASURED
# it against the pre-fix script: it PASSED there too, i.e. it was vacuous, asserting an absence that
# holds trivially. Three probes explain why, and they are worth recording because they establish a
# property, not just a test outcome: a live foreign reservation on the log, an uncreatable log
# (read-only parent), and a lock path that is a DIRECTORY are EACH refused by the artifact-set
# PRE-CHECK (or the log-writability check) BEFORE any lock is created. The pre-check evaluates every
# artifact as free / live / owner-unestablished under the global launch lock, and the lock prevents a
# peer interleaving between check and `ln -s`. So the `_extra_ok=0` rollback is unreachable from
# outside; only an I/O-level `ln -s` failure (ENOSPC/EROFS after the writability check passed) reaches
# it, which cannot be induced reliably here. The array fix is therefore DEFENCE IN DEPTH, covered
# STRUCTURALLY by 4b.154 -- and this case now pins the reachability fact instead of pretending to test
# the rollback: a conflicting artifact set must be refused with NO lock left behind. If someone later
# weakens the pre-check, the rollback becomes reachable and this case catches the leftover.
if [ "$HAVE_SYSTEMD" = yes ]; then
  _sp="$TMP/has space"
  mkdir -p "$_sp"
  _spa=$(bash "$LAUNCHER" --summary "$_sp/a.txt" --log "$_sp/shared.log" -- --only roborev-lints 2>&1)
  _spau=$(printf '%s' "$_spa" | sed -n 's/^unit:  *//p'); [ -n "$_spau" ] && echo "$_spau" >> "$UNITS_FILE"
  _spb=$(bash "$LAUNCHER" --summary "$_sp/b.txt" --log "$_sp/shared.log" -- --only fmt 2>&1); _spbr=$?
  _spbu=$(printf '%s' "$_spb" | sed -n 's/^unit:  *//p'); [ -n "$_spbu" ] && echo "$_spbu" >> "$UNITS_FILE"
  if [ -n "$_spau" ] && [ "$_spbr" != 0 ] && [ ! -e "$_sp/b.txt.heartbeat.launch-lock" ] \
     && printf '%s' "$_spb" | grep -q 'artifact-set collision'; then
    ok "4b.155 a conflicting artifact set is refused by the PRE-CHECK, leaving no lock behind"
  else
    bad "4b.155 a conflicting artifact set is refused before any lock is created" \
        "first-unit='${_spau:-none}' second-exit=$_spbr leftover=$([ -e "$_sp/b.txt.heartbeat.launch-lock" ] && echo yes || echo no)"
  fi
  for _u in $_spau $_spbu; do systemctl --user stop "$_u" >/dev/null 2>&1; done
else
  skipc "4b.155 artifact-set pre-check" "no user systemd manager on this host"
fi

# F2. The global launch lock fell back to ${TMPDIR:-/tmp} — a shared, PREDICTABLE, fixed-NAME path any
# local user can pre-create and hold to refuse every detached launch on the box. It must now REFUSE on
# a runtime dir it cannot affirmatively verify, and must NOT silently use TMPDIR instead.
# THE INDUCTION MECHANISM WAS THE VULNERABILITY (roborev job 321 F1). This case used to make the
# runtime dir unverifiable by setting `XDG_RUNTIME_DIR` to a mode-0777 directory — which worked only
# because the lock path was READ FROM THAT VARIABLE, and that is precisely the defect job 321 found:
# a lock selected by caller-controlled env is not global. Fixing it necessarily removed this test's
# hook. Same shape as #3544, where "the test hook and the vulnerability were the same fact", and the
# same remedy: SUBSTITUTE THE ARTIFACT rather than reintroduce a settable seam.
#
# The canonical path cannot be made unverifiable in situ (`/run/user/$(id -u)` is this session's own
# and shared with every peer lane on the box), so the VALIDATION BLOCK is extracted and unit-tested
# with a fake directory. It is taken to the `fi` that closes the REFUSAL, not the first one — the
# block contains a nested `if _rd_stat=...` whose `fi` is also at column 0.
_badrt="$TMP/badrt"; _goodtmp="$TMP/goodtmp"
mkdir -p "$_badrt" "$_goodtmp"; chmod 777 "$_badrt"
_rt_src=$(awk '
  /^_rundir="\/run\/user\/\$\(id -u\)"$/ {inb=1}
  inb {print}
  inb && /exit 69/ {seen=1; next}
  inb && seen && /^fi$/ {exit}
' "$LAUNCHER")
if [ -z "$_rt_src" ] || ! printf '%s' "$_rt_src" | grep -q 'exit 69'; then
  bad "4b.156 the runtime-dir validation block could be extracted" \
      "extraction produced nothing usable — this case proves nothing"
else
  # Substitute the canonical literal in the EXTRACTED copy, and verify the substitution took: a
  # rewrite that silently missed would leave the case testing the real directory and passing.
  _rt_fake=$(printf '%s' "$_rt_src" | sed "s#^_rundir=\"/run/user/\$(id -u)\"#_rundir=\"$_badrt\"#")
  if ! printf '%s' "$_rt_fake" | grep -qF "_rundir=\"$_badrt\""; then
    bad "4b.156 the canonical literal could be pinned to a fake dir" "substitution did not take"
  else
    _f2o=$( TMPDIR="$_goodtmp" bash -c "set -uo pipefail; $_rt_fake" 2>&1 ); _f2r=$?
    if [ "$_f2r" = 69 ] && ! [ -e "$_goodtmp/cqlite-gate-launch.lock" ] \
       && printf '%s' "$_f2o" | grep -q 'per-user runtime directory'; then
      ok "4b.156 a mode-0777 runtime dir refuses (69) and does NOT fall back to TMPDIR"
    else
      bad "4b.156 an unverifiable runtime dir refuses without falling back to TMPDIR" \
          "exit=$_f2r tmpdir-lock=$([ -e "$_goodtmp/cqlite-gate-launch.lock" ] && echo created || echo absent) out=$(printf '%s' "$_f2o" | head -2 | tr '\n' ' ')"
    fi
    # CONTROL: the same extracted block must ACCEPT a 0700 dir we own, or the case above passes by
    # refusing everything and says nothing about the check.
    _okrt="$TMP/okrt"; mkdir -p "$_okrt"; chmod 700 "$_okrt"
    _rt_ok=$(printf '%s' "$_rt_src" | sed "s#^_rundir=\"/run/user/\$(id -u)\"#_rundir=\"$_okrt\"#")
    ( bash -c "set -uo pipefail; $_rt_ok" ) >/dev/null 2>&1
    [ "$?" = 0 ] \
      && ok "4b.156b control: a 0700 dir we own is ACCEPTED by the same extracted block" \
      || bad "4b.156b control: a 0700 dir we own is accepted" "the block refuses everything"
  fi
fi
# RE-POINTED, because after job 321 F1 this case was a VACUOUS PASS. It set XDG_RUNTIME_DIR to a
# valid 0700 dir and asserted no refusal — but that variable no longer selects the lock path, so it
# passed because the CANONICAL directory happens to be fine, saying nothing about what it claimed.
# A test that passes for a reason unrelated to its name is worse than one that fails.
#
# Its acceptance-control role moved to 4b.156b (which substitutes the artifact). What it asserts now
# is F1's actual property, BEHAVIOURALLY and as the counterpart to 4b.193's structural check: a
# caller-set XDG_RUNTIME_DIR must NOT move the global lock. The decisive direction is the ABSENCE of
# a lock in the caller's directory — the canonical lock usually pre-exists, so its presence proves
# nothing on its own.
_goodrt="$TMP/goodrt"; mkdir -p "$_goodrt"; chmod 700 "$_goodrt"
rm -f "$_goodrt/cqlite-gate-launch.lock"
_f2c=$(XDG_RUNTIME_DIR="$_goodrt" bash "$LAUNCHER" \
         --summary "$TMP/f2c.txt" --log "$TMP/f2c.log" -- --only fmt 2>&1)
_f2cu=$(printf '%s' "$_f2c" | sed -n 's/^unit:  *//p'); [ -n "$_f2cu" ] && echo "$_f2cu" >> "$UNITS_FILE"
if printf '%s' "$_f2c" | grep -q 'per-user runtime directory'; then
  bad "4b.157 a caller-set XDG_RUNTIME_DIR neither refuses nor moves the lock" \
      "refused on a canonical dir that is fine"
elif [ -e "$_goodrt/cqlite-gate-launch.lock" ]; then
  bad "4b.157 a caller-set XDG_RUNTIME_DIR neither refuses nor moves the lock" \
      "the caller-set dir got the lock — the env still selects the path (job 321 F1)"
else
  ok "4b.157 a caller-set XDG_RUNTIME_DIR neither refuses nor MOVES the global lock"
fi
[ -n "$_f2cu" ] && systemctl --user stop "$_f2cu" >/dev/null 2>&1

# F3. The wrapper exports the CALLER's PATH before its last two lines run, so an unqualified `rm`
# (self-unlink of the 0600 file holding every forwarded secret) and `bash` (what we exec) resolved
# through a PATH this script does not control. Structural: both must be emitted as absolute, resolved
# in the LAUNCHER's PATH.
if grep -q "printf '%q -f -- %q" "$_f1_src" && grep -q 'printf .exec %q %q' "$_f1_src" \
   && ! grep -q "printf 'rm -f -- %q" "$_f1_src" && ! grep -q "printf 'exec bash %q" "$_f1_src" \
   && grep -q '_rm_abs="\$(command -v rm' "$_f1_src"; then
  ok "4b.158 the wrapper's self-unlink and exec are emitted as ABSOLUTE resolved paths"
else
  bad "4b.158 the wrapper emits absolute rm/bash" "$(grep -n 'ENV_SCRIPT\"$' "$_f1_src" | tail -3)"
fi
# BEHAVIOURAL: capture the generated wrapper by stubbing systemd-run so it never runs (and therefore
# never self-deletes), then assert the emitted lines carry absolute paths rather than bare words. A
# structural grep alone cannot see what the launcher actually WROTE.
_stub="$TMP/f3stub"; mkdir -p "$_stub"
cat > "$_stub/systemd-run" <<'STUB'
#!/bin/bash
# Find the wrapper: the argument after the interpreter, and copy it aside instead of executing it.
prev=""; for a in "$@"; do
  case "$prev" in */bash) [ -f "$a" ] && cp "$a" "$CAPTURE_TO" 2>/dev/null && break ;;
  esac
  prev="$a"
done
exit 0
STUB
chmod +x "$_stub/systemd-run"
_cap="$TMP/f3-wrapper.txt"
CAPTURE_TO="$_cap" PATH="$_stub:$PATH" bash "$LAUNCHER" \
  --summary "$TMP/f3.txt" --log "$TMP/f3.log" -- --only fmt >/dev/null 2>&1 || true
if [ ! -s "$_cap" ]; then
  skipc "4b.159 generated wrapper uses absolute rm/bash" "wrapper not captured (stub did not observe it)"
else
  _rml=$(grep -E -- '^/[^ ]*rm -f -- ' "$_cap" | head -1)
  _exl=$(grep -E '^exec /' "$_cap" | head -1)
  if [ -n "$_rml" ] && [ -n "$_exl" ]; then
    ok "4b.159 the GENERATED wrapper self-unlinks and execs via absolute paths"
  else
    bad "4b.159 the generated wrapper uses absolute rm/bash" \
        "rm-line='${_rml:-none}' exec-line='${_exl:-none}' captured: $(tr '\n' '|' < "$_cap" | tail -c 200)"
  fi
fi

# --- round-48 class audit: the two-valued predicate at the base of the pre-check -------------------
# _foreign_reservation opened with `[ -L $lk ] || [ -e $lk ] || { printf free; return 0; }`. Both
# predicates are two-valued, so "no such path" and "not permitted to look" both come back FALSE and
# the branch answered `free` -- the PERMISSIVE value -- for BOTH. That licenses this launch to take a
# path a LIVE peer may hold. Tested by EXTRACTING the function rather than driving the whole launcher,
# because (as the 4b.155 retraction established) the launcher validates its directories before it ever
# reaches the pre-check, so the blind case is not reachable from the CLI -- the same reason the
# rollback is not. A unit test reaches it; a launcher-level test would pass vacuously.
_fr_src=$(sed -n '/^_foreign_reservation() {/,/^}$/p' "$LAUNCHER")
if [ -z "$_fr_src" ] || ! printf '%s' "$_fr_src" | grep -q 'launch-lock'; then
  bad "4b.160 _foreign_reservation could be extracted for unit test" "extraction produced nothing usable"
else
  # POSITIVE CONTROL FIRST: a searchable directory with no lock must still answer `free`, or the case
  # below passes because the function answers `unknown` for everything and the launcher is broken.
  _frd="$TMP/fr-free"; mkdir -p "$_frd"
  _fr1=$(eval "$_fr_src"; _foreign_reservation "$_frd/s" 2>/dev/null)
  if [ "$_fr1" = free ]; then
    ok "4b.160 control: a searchable dir with no lock answers 'free'"
  else
    bad "4b.160 control: a searchable dir with no lock answers 'free'" "got '${_fr1:-empty}'"
  fi
  # THE DEFECT: the containing directory cannot be searched, so absence is UNVERIFIABLE.
  _frb="$TMP/fr-blind"; mkdir -p "$_frb"; chmod 000 "$_frb"
  _fr2=$(eval "$_fr_src"; _foreign_reservation "$_frb/s" 2>/dev/null)
  chmod 700 "$_frb"
  if [ "$_fr2" = unknown ]; then
    ok "4b.161 an UNVERIFIABLE absence answers 'unknown', never 'free'"
  else
    bad "4b.161 an unverifiable absence answers 'unknown', never 'free'" \
        "got '${_fr2:-empty}' — 'free' hands a live peer's path to this launch"
  fi
fi

# Class 1, and it was DUPLICATED: `set -- $_state` word-split and glob-expanded every
# /proc/<pid>/stat field in BOTH gate-detached.sh and scripts/lib/gate-heartbeat.sh. shellcheck found
# the copy; reading the file did not. Pinned in both, since fixing one and leaving its mirror is a
# shape this issue has already produced twice.
_split_hits=0
for _f in "$LAUNCHER" "$REPO_ROOT/scripts/lib/gate-heartbeat.sh"; do
  # An UNGUARDED split is the defect. Where splitting is genuinely wanted (/proc stat field 20 =
  # starttime) it must say so with `set -f` on the same line, so globbing is off while splitting is on
  # — a blanket `disable=SC2086` suppressed BOTH warnings and is why the enumerator was silent at
  # exactly the site that needed it. So: every `set -- $...` line must also carry `set -f`.
  _split_hits=$((_split_hits + $(awk '
    /set -f/                          { guard = 3 }
    /(^|;)[[:space:]]*set -- \$/       { if (guard <= 0) bad++ }
                                      { if (guard > 0) guard-- }
    END                               { print bad+0 }
  ' "$_f")))
done
if [ "$_split_hits" = 0 ]; then
  ok "4b.162 neither copy of _proc_is_zombie word-splits /proc stat (class 1, both files)"
else
  bad "4b.162 neither copy word-splits /proc stat" "$_split_hits file(s) still use 'set -- \$...'"
fi

# --- roborev job 272: two sites that BYPASSED a safe primitive this file already had ---------------
# Both findings were the same shape, and it is a shape the round-48 class audit did NOT catch: the
# audit enumerated PREDICATES and PATHS, i.e. the definitions of the safe primitives, and never
# enumerated their CALL SITES to check that none was bypassed. So the tests below are PROPERTY tests
# over call sites, not spelling tests. The guard that missed job 272's second finding looked for the
# literal `grep -q "^run-id: ` while the live defect read `grep -m1 '^run-id: '` — a NAME, not a
# PROPERTY, which is the failure mode this repo has already recorded twice.

# (a) EVERY direct read of the run-id must live inside _snap_pair, whatever variable it reads from and
#     however the grep is spelled. Two separate greps against a LIVE artifact can pair our nonce with a
#     peer's run-id; _snap_pair takes one immutable copy and reads both fields from it.
_sp_start=$(grep -n '_snap_pair() {' "$LAUNCHER" | head -1 | cut -d: -f1)
if [ -z "$_sp_start" ]; then
  bad "4b.163 every run-id read is inside _snap_pair" "_snap_pair not found"
else
  _sp_end=$(awk -v s="$_sp_start" 'NR>s && /^[[:space:]]*\}[[:space:]]*$/ { print NR; exit }' "$LAUNCHER")
  _rid_outside=0; _rid_total=0
  while IFS=: read -r _ln _rest; do
    [ -n "$_ln" ] || continue
    case "$_rest" in *'#'*) ;; esac
    _rid_total=$((_rid_total+1))
    if [ "$_ln" -lt "$_sp_start" ] || [ "$_ln" -gt "$_sp_end" ]; then
      _rid_outside=$((_rid_outside+1)); echo "     direct run-id read outside _snap_pair at line $_ln"
    fi
  done < <(grep -nE "grep[^|]*'\^run-id: '" "$LAUNCHER" || true)
  # Non-vacuity: there must BE at least one such read, or the property is trivially satisfied by a
  # file that reads the run-id nowhere.
  if [ "$_rid_total" -ge 1 ] && [ "$_rid_outside" = 0 ]; then
    ok "4b.163 every direct run-id read is inside _snap_pair ($_rid_total total, 0 outside)"
  else
    bad "4b.163 every direct run-id read is inside _snap_pair" \
        "total=$_rid_total outside=$_rid_outside (total 0 would make this vacuous)"
  fi
fi

# (b) The unit-death decision must go through _unit_is_live, never `is-active --quiet`.
#     `is-active --quiet` exits nonzero for every TRANSITIONAL state and for every QUERY FAILURE, so a
#     healthy-but-unsettled gate (or one we could not ask about) read as dead and got stopped as
#     unmonitorable. _unit_is_live treats only `inactive|failed` as affirmative terminal answers.
_ua_start=$(grep -n '_unit_is_live() {' "$LAUNCHER" | head -1 | cut -d: -f1)
if [ -z "$_ua_start" ]; then
  bad "4b.164 unit liveness decided by _unit_is_live" "_unit_is_live not found"
else
  _ua_end=$(awk -v s="$_ua_start" 'NR>s && /^[[:space:]]*\}[[:space:]]*$/ { print NR; exit }' "$LAUNCHER")
  _ia_outside=0
  while IFS=: read -r _ln _rest; do
    [ -n "$_ln" ] || continue
    case "$_rest" in [[:space:]]*'#'*|'#'*) continue ;; esac
    if [ "$_ln" -lt "$_ua_start" ] || [ "$_ln" -gt "$_ua_end" ]; then
      _ia_outside=$((_ia_outside+1)); echo "     is-active --quiet in a decision at line $_ln"
    fi
  done < <(grep -n 'is-active --quiet' "$LAUNCHER" | grep -v ':[[:space:]]*#' || true)
  if [ "$_ia_outside" = 0 ]; then
    ok "4b.164 unit liveness is decided by _unit_is_live, never a bare 'is-active --quiet'"
  else
    bad "4b.164 unit liveness is decided by _unit_is_live" "$_ia_outside bare is-active decision(s)"
  fi
fi

# --- orphan-holds-scope: ask WHAT is in the cgroup, not WHETHER anything is -------------------------
# An orphaned process keeps a scope ActiveState=active forever (measured: one box, 12 orphaned sleeps,
# 0 gate scopes), so _foreign_reservation's fall-through turned an affirmative "owner is dead" into
# `live` and refused the path PERMANENTLY. Tested by extracting the helper: the launcher validates its
# directories long before the fall-through, so this is not reachable from the CLI -- the same reason the
# rollback is not (4b.155). A unit test reaches it; a launcher-level test would pass vacuously.
_ug_src=$(sed -n '/^_unit_runs_a_gate() {/,/^}$/p' "$LAUNCHER")
if [ -z "$_ug_src" ]; then
  bad "4b.165 _unit_runs_a_gate could be extracted" "extraction produced nothing"
else
  # A fake cgroup tree: `procs` lists pids we control, so the helper's verdict is fully determined.
  _ugd="$TMP/ug"; mkdir -p "$_ugd"
  # stub `systemctl` so ControlGroup resolves into our fake tree, and point the helper at it.
  _ug_run() {  # <procs-content> -> prints rc
    local content="$1" fakecg="$_ugd/fs" rc
    mkdir -p "$fakecg/unit"
    printf '%s\n' "$content" > "$fakecg/unit/cgroup.procs"
    ( eval "${_ug_src//\/sys\/fs\/cgroup/$fakecg}"
      systemctl() { printf '/unit\n'; }
      _unit_runs_a_gate fake.service ) >/dev/null 2>&1; rc=$?
    printf '%s' "$rc"
  }
  # (a) a cgroup containing ONLY an orphan must be affirmatively NOT-a-gate -> reservation reads free
  sleep 120 & _orphan=$!
  _rc_orphan=$(_ug_run "$_orphan")
  if [ "$_rc_orphan" = 1 ]; then
    ok "4b.165 a cgroup holding only an orphan is NOT a live gate (the permanent-refusal case)"
  else
    bad "4b.165 a cgroup holding only an orphan is not a live gate" "rc=$_rc_orphan (1 expected)"
  fi
  # (b) CONTROL: a real full gate in the cgroup must still read live, or (a) passes by answering
  #     "not a gate" for everything and the reservation protection is gone.
  _gate_pid=""
  for _q in $(pgrep -f 'agent-gate\.sh' 2>/dev/null); do
    _h=0; while IFS= read -r -d '' _a; do case "$_a" in *agent-gate.sh) _h=1 ;; esac; done < "/proc/$_q/cmdline" 2>/dev/null
    [ "$_h" = 1 ] && { _gate_pid=$_q; break; }
  done
  if [ -n "$_gate_pid" ]; then
    _rc_gate=$(_ug_run "$_gate_pid")
    [ "$_rc_gate" = 0 ] && ok "4b.166 control: a real full gate in the cgroup still reads LIVE" \
                        || bad "4b.166 control: a real full gate reads live" "rc=$_rc_gate (0 expected)"
  else
    skipc "4b.166 control: a real full gate reads live" "no agent-gate.sh running on this host"
  fi
  # (c) an UNREADABLE cgroup.procs must be the THIRD value, so the caller refuses rather than guessing
  _rc_unread=$( { mkdir -p "$_ugd/fs/unit"; : > "$_ugd/fs/unit/cgroup.procs"; chmod 000 "$_ugd/fs/unit/cgroup.procs"; } 2>/dev/null
                _ug_run "" ; chmod 644 "$_ugd/fs/unit/cgroup.procs" 2>/dev/null )
  case "$_rc_unread" in
    2) ok "4b.167 an unreadable cgroup.procs returns the THIRD value (caller refuses)" ;;
    *) bad "4b.167 an unreadable cgroup.procs returns the third value" "rc=$_rc_unread (2 expected)" ;;
  esac
  kill "$_orphan" 2>/dev/null; wait "$_orphan" 2>/dev/null || true
  chmod -R u+w "$_ugd" 2>/dev/null || true
fi

# The matcher must be an exact ARGV ELEMENT, never a substring of the joined cmdline: a searching shell
# carries the pattern INSIDE an element, so it is excluded by construction with no exclusion list.
# Exact argv element, AND no mode exclusion: ownership is not the same question as gate-of-record.
# Excluding --lite/--delta/--only here would let a LIVE partial run's reservation be reclaimed (4b.153,
# 4b.155 create their live owners with --only file-size), putting two writers on one summary path.
if grep -q 'read -r -d "" a' "$LAUNCHER" \
   && grep -qE 'case "\$a" in \*agent-gate\.sh\)' "$LAUNCHER" \
   && ! grep -q '\*agent-gate\.sh\*' "$LAUNCHER" \
   && ! sed -n '/^_unit_runs_a_gate() {/,/^}$/p' "$LAUNCHER" | grep -qE '^\s+--lite\|--delta\|--only\)'; then
  ok "4b.168 exact argv element, and ownership does NOT exclude partial-mode runs"
else
  bad "4b.168 exact argv element, ownership does not exclude partial modes" \
      "$(sed -n '/^_unit_runs_a_gate() {/,/^}$/p' "$LAUNCHER" | grep -nE 'agent-gate|--lite' | head -4)"
fi

# And scope state must no longer be consulted where the owner is affirmatively dead.
if sed -n '/gone) : ;;/,/printf .free./p' "$LAUNCHER" | grep -q '_unit_is_live'; then
  bad "4b.169 the dead-owner fall-through no longer consults ActiveState" "_unit_is_live still reachable there"
else
  ok "4b.169 the dead-owner fall-through no longer consults ActiveState"
fi

# --- job 319 F1: AN UNINSPECTABLE PID IS NOT AN AFFIRMATIVE "NO GATE" ---------------------------
# _unit_runs_a_gate defaults to found=1, so `continue`-ing past a pid whose argv could not be read
# returned 1 = "affirmatively no gate" and the caller RECLAIMED a live gate's summary path. But
# refusing on every unreadable pid would resurrect the job-196 permanent block, since a genuinely
# dead owner's pids are unreadable too. The matrix below pins BOTH directions, and (d)/(e) are the
# controls without which (a)-(c) could pass by answering "unmeasurable" to everything.
#
# /proc is redirected at a fake tree so the uninspectable states are CONSTRUCTIBLE: `kill -0` and
# `ps` still see the real pid, so _pid_state and _proc_is_zombie answer about a real process.
_j319f1_src=$( sed -n '/^_pid_state() {/,/^}$/p'        "$LAUNCHER"
           sed -n '/^_proc_is_zombie() {/,/^}$/p'   "$LAUNCHER"
           sed -n '/^_pid_ruled_out() {/,/^}$/p'    "$LAUNCHER"
           sed -n '/^_unit_runs_a_gate() {/,/^}$/p' "$LAUNCHER" )
if ! printf '%s' "$_j319f1_src" | grep -q '_pid_ruled_out()' \
   || ! printf '%s' "$_j319f1_src" | grep -q '^_unit_runs_a_gate()'; then
  bad "4b.177 job-319 F1 helpers could be extracted" "extraction produced nothing usable"
else
  _f1d="$TMP/f1"; mkdir -p "$_f1d/fs/unit" "$_f1d/proc"
  _f1_run() {  # <procs-content> -> rc
    local content="$1" src rc
    printf '%s\n' "$content" > "$_f1d/fs/unit/cgroup.procs"
    src=${_j319f1_src//\/sys\/fs\/cgroup/$_f1d\/fs}
    src=${src//\/proc/$_f1d\/proc}
    ( eval "$src"
      systemctl() { printf '/unit\n'; }
      _unit_runs_a_gate fake.service ) >/dev/null 2>&1; rc=$?
    printf '%s' "$rc"
  }
  sleep 300 & _f1_live=$!
  # (a) PRESENT but argv unreadable => the THIRD value, so the caller refuses instead of reclaiming.
  mkdir -p "$_f1d/proc/$_f1_live"
  : > "$_f1d/proc/$_f1_live/cmdline"; chmod 000 "$_f1d/proc/$_f1_live/cmdline" 2>/dev/null
  _rc_a=$(_f1_run "$_f1_live")
  if [ ! -r "$_f1d/proc/$_f1_live/cmdline" ]; then
    [ "$_rc_a" = 2 ] \
      && ok "4b.177 a PRESENT pid with unreadable argv is UNMEASURABLE, not 'no gate'" \
      || bad "4b.177 a present pid with unreadable argv is unmeasurable" "rc=$_rc_a (2 expected)"
  else
    skipc "4b.177 present-but-unreadable argv" "cannot make a file unreadable as this user (root?)"
  fi
  # (b) AFFIRMATIVELY GONE => still 1, or a dead owner's reservation blocks forever (job 196).
  sleep 0.1 & _f1_dead=$!; wait "$_f1_dead" 2>/dev/null || true
  rm -rf "$_f1d/proc/$_f1_dead"
  _rc_b=$(_f1_run "$_f1_dead")
  [ "$_rc_b" = 1 ] \
    && ok "4b.178 an affirmatively GONE pid stays reclaimable (no permanent block)" \
    || bad "4b.178 an affirmatively gone pid stays reclaimable" "rc=$_rc_b (1 expected)"
  # (c) READABLE BUT EMPTY argv (exiting / mid-exec) is the same defect one step deeper.
  mkdir -p "$_f1d/proc/$_f1_live"; chmod 644 "$_f1d/proc/$_f1_live/cmdline" 2>/dev/null
  : > "$_f1d/proc/$_f1_live/cmdline"
  _rc_c=$(_f1_run "$_f1_live")
  [ "$_rc_c" = 2 ] \
    && ok "4b.179 a READABLE-but-EMPTY argv is unmeasurable, not 'no gate'" \
    || bad "4b.179 a readable-but-empty argv is unmeasurable" "rc=$_rc_c (2 expected)"
  # (d) CONTROL: a real gate argv must still be FOUND, or (a)-(c) pass by never finding anything.
  printf '/bin/bash\0/x/scripts/agent-gate.sh\0' > "$_f1d/proc/$_f1_live/cmdline"
  _rc_d=$(_f1_run "$_f1_live")
  [ "$_rc_d" = 0 ] \
    && ok "4b.180 control: a readable gate argv is still found LIVE (0)" \
    || bad "4b.180 control: a readable gate argv is still found live" "rc=$_rc_d (0 expected)"
  # (e) CONTROL: a readable NON-gate argv must still be an affirmative 1, not the new third value.
  printf '/bin/sleep\0300\0' > "$_f1d/proc/$_f1_live/cmdline"
  _rc_e=$(_f1_run "$_f1_live")
  [ "$_rc_e" = 1 ] \
    && ok "4b.181 control: a readable NON-gate argv is still affirmatively 1" \
    || bad "4b.181 control: a readable non-gate argv is still affirmatively 1" "rc=$_rc_e (1 expected)"
  kill "$_f1_live" 2>/dev/null; wait "$_f1_live" 2>/dev/null || true
  chmod -R u+w "$_f1d" 2>/dev/null || true
fi

# --- job 319 F2: ONE function, TWO questions with OPPOSITE safe answers -------------------------
# _unit_is_live collapses live+unmeasurable onto 0. Where 0 means REFUSE (reclamation) that is
# conservative; where 0 means ACCEPT (heartbeat acceptance) it re-admits one-beat-then-dead, which is
# the case that gate exists to reject. The polarity is now named by the CALLER, and the row that
# matters is `unknown`: the two predicates must DISAGREE there, and agree everywhere else.
_f2_src=$( sed -n '/^_unit_state() {/,/^}$/p'                  "$LAUNCHER"
           sed -n '/^_unit_is_affirmatively_live() {/,/^}$/p'  "$LAUNCHER"
           sed -n '/^_unit_is_live() {/,/^}$/p'                "$LAUNCHER" )
if ! printf '%s' "$_f2_src" | grep -q '^_unit_state()' \
   || ! printf '%s' "$_f2_src" | grep -q '^_unit_is_affirmatively_live()'; then
  bad "4b.182 job-319 F2 helpers could be extracted" "extraction produced nothing usable"
else
  # `_f2_st`, NOT `st`: bash locals are DYNAMICALLY scoped, and `_unit_state` declares its own
  # `local st`. A stub named after the callee's local therefore read the callee's EMPTY variable, so
  # every state arrived as "" => unknown and three of these cases failed on a test defect that looked
  # exactly like a real one. Name stub state after the stub.
  _f2() {  # <ActiveState-or-FAIL> <predicate> -> rc
    local _f2_st="$1" fn="$2" rc
    ( eval "$_f2_src"
      systemctl() { if [ "$_f2_st" = FAIL ]; then return 1; fi; printf '%s\n' "$_f2_st"; }
      "$fn" u.service ) >/dev/null 2>&1; rc=$?
    printf '%s' "$rc"
  }
  _f2_unk_live=$(_f2 FAIL _unit_is_live); _f2_unk_aff=$(_f2 FAIL _unit_is_affirmatively_live)
  if [ "$_f2_unk_live" = 0 ] && [ "$_f2_unk_aff" != 0 ]; then
    ok "4b.182 on an UNMEASURABLE unit the two polarities DISAGREE (refuse-to-reclaim 0, accept-refuses)"
  else
    bad "4b.182 on an unmeasurable unit the two polarities disagree" \
        "_unit_is_live=$_f2_unk_live (0 expected) _unit_is_affirmatively_live=$_f2_unk_aff (nonzero expected)"
  fi
  # CONTROLS: without these, 4b.182 passes for a predicate that refuses unconditionally.
  _f2_act_aff=$(_f2 active _unit_is_affirmatively_live)
  [ "$_f2_act_aff" = 0 ] \
    && ok "4b.183 control: an AFFIRMATIVELY active unit is accepted (0)" \
    || bad "4b.183 control: an affirmatively active unit is accepted" "rc=$_f2_act_aff (0 expected)"
  _f2_inact_aff=$(_f2 inactive _unit_is_affirmatively_live)
  _f2_inact_live=$(_f2 inactive _unit_is_live)
  if [ "$_f2_inact_aff" != 0 ] && [ "$_f2_inact_live" != 0 ]; then
    ok "4b.184 an affirmatively TERMINAL unit is rejected by both polarities"
  else
    bad "4b.184 an affirmatively terminal unit is rejected by both" \
        "aff=$_f2_inact_aff live=$_f2_inact_live (both nonzero expected)"
  fi
  # Job 241's property, asserted on the side it belongs to: an UNRECOGNISED state must never be read
  # as terminal, so the REFUSE side still refuses to reclaim. (It was previously asserted through the
  # ACCEPT predicate with `refreshing`, which the job-320 allowlist now recognises — so the case was
  # both misnamed and testing the wrong polarity.)
  _f2_unrec_live=$(_f2 some-future-state _unit_is_live)
  [ "$_f2_unrec_live" = 0 ] \
    && ok "4b.185 an unrecognised state is never TERMINAL (reclamation still refuses)" \
    || bad "4b.185 an unrecognised state is never terminal" "rc=$_f2_unrec_live (0 expected)"

  # 4b.190-4b.192 (roborev job 320, Medium): the ACCEPT side is an ALLOWLIST, and the two sides take
  # OPPOSITE closures. My job-319 fix made the state three-valued but gave both callers ONE partition
  # tuned for the refuse side, so `deactivating` — a unit definitively shutting down — was accepted as
  # affirmatively live, which is the one-beat-then-dead case the gate exists to reject.
  _f2_deact_aff=$(_f2 deactivating _unit_is_affirmatively_live)
  _f2_deact_live=$(_f2 deactivating _unit_is_live)
  if [ "$_f2_deact_aff" != 0 ] && [ "$_f2_deact_live" = 0 ]; then
    ok "4b.190 'deactivating' does NOT accept, yet still refuses reclamation (opposite closures)"
  else
    bad "4b.190 'deactivating' does not accept, yet still refuses reclamation" \
        "accept=$_f2_deact_aff (nonzero expected) reclaim-refusal=$_f2_deact_live (0 expected)"
  fi
  _f2_maint_aff=$(_f2 maintenance _unit_is_affirmatively_live)
  _f2_fut_aff=$(_f2 some-future-state _unit_is_affirmatively_live)
  if [ "$_f2_maint_aff" != 0 ] && [ "$_f2_fut_aff" != 0 ]; then
    ok "4b.191 'maintenance' and an unfamiliar token cannot ACCEPT (excusal is a positive verdict)"
  else
    bad "4b.191 'maintenance' and an unfamiliar token cannot accept" \
        "maintenance=$_f2_maint_aff future=$_f2_fut_aff (both nonzero expected)"
  fi
  # NON-VACUITY CONTROL, without which an allowlist that accepts NOTHING passes every case above.
  _f2_allow_bad=""
  for _st_ok in active activating reloading refreshing; do
    [ "$(_f2 "$_st_ok" _unit_is_affirmatively_live)" = 0 ] || _f2_allow_bad="$_f2_allow_bad $_st_ok"
  done
  [ -z "$_f2_allow_bad" ] \
    && ok "4b.192 control: every allowlisted running state DOES accept (the allowlist is not empty)" \
    || bad "4b.192 control: every allowlisted running state accepts" "refused:$_f2_allow_bad"
  # And the ACCEPT sites must not go back to the two-valued predicate.
  _f2_bad=0
  while IFS= read -r _ln; do
    case "$_ln" in *_unit_is_affirmatively_live*) : ;; *) _f2_bad=$((_f2_bad+1)); echo "     $_ln" ;; esac
  done < <(grep -nE '^\s+2\)' "$LAUNCHER" | grep -F '_unit_is' || true)
  [ "$_f2_bad" = 0 ] \
    && ok "4b.186 every RUNNING-acceptance site uses the affirmative predicate" \
    || bad "4b.186 every RUNNING-acceptance site uses the affirmative predicate" "$_f2_bad site(s) do not"
fi

# --- job 319 F3: empty-array expansion under `set -u` on bash 3.2 --------------------------------
# This script sets `set -uo pipefail` and the repo supports stock macOS /bin/bash 3.2, where an empty
# "${ARRAY[@]}" is UNBOUND and aborts. A bare `gate-detached.sh` leaves GATE_ARGS empty, so the
# DEFAULT full-gate invocation was the broken one. Structural, because this host's bash 5.2 accepts
# the unsafe form — the defect is unobservable here by construction.
# STRIP THE SAFE FORM FIRST. The safe form ${A[@]+"${A[@]}"} CONTAINS the literal "${A[@]}", so a
# naive grep for the unsafe spelling matches every correct site — this test failed on the fixed tree
# and would have "passed" only by reverting the fix.
_f3_bad=0
while IFS= read -r _ln; do _f3_bad=$((_f3_bad+1)); echo "     $_ln"; done < <(
  sed -E 's/\$\{(GATE_ARGS|_extra_locks)\[@\]\+"\$\{(GATE_ARGS|_extra_locks)\[@\]\}"\}//g' "$LAUNCHER" \
    | grep -nE '"\$\{(GATE_ARGS|_extra_locks)\[@\]\}"' || true )
[ "$_f3_bad" = 0 ] \
  && ok "4b.187 no bare \"\${ARRAY[@]}\" expansion (bash 3.2 + set -u safe form)" \
  || bad "4b.187 no bare array expansion under set -u" "$_f3_bad unsafe site(s)"
if grep -qE '\$\{GATE_ARGS\[@\]\+"\$\{GATE_ARGS\[@\]\}"\}' "$LAUNCHER"; then
  ok "4b.188 the gate exec uses the \${A[@]+\"\${A[@]}\"} form"
else
  bad "4b.188 the gate exec uses the safe form" "not found at the systemd-run exec"
fi

# --- #3740: this launcher must not FORWARD build-flag contamination ------------------------------
# agent-gate.sh's header says "never export global RUSTFLAGS on a worker": a non-empty RUSTFLAGS
# SUPPRESSES cargo's managed block and the gate then APPENDS its own, yielding a doubled
# `-D warnings -D warnings` applied to components the gate deliberately scopes it AWAY from. That
# contamination made binding-rust-tests FAIL on a clean tree and halted the fleet for ~an hour on a
# P0 that did not exist. This launcher forwards the caller's whole environment, so it is the
# propagation vector -- and the flag arrives via systemd-run, where no command line shows it.
#
# Asserted on the GENERATED WRAPPER, not on the source: the wrapper is what the unit actually
# executes. Captured by stubbing systemd-run so it is never run and so never self-deletes.
_bf_stub="$TMP/bfstub"; mkdir -p "$_bf_stub"
cat > "$_bf_stub/systemd-run" <<'BFSTUB'
#!/bin/bash
prev=""; for a in "$@"; do
  case "$prev" in */bash) [ -f "$a" ] && cp "$a" "$CAPTURE_TO" 2>/dev/null && break ;; esac
  prev="$a"
done
exit 0
BFSTUB
chmod +x "$_bf_stub/systemd-run"
_bf_cap="$TMP/bf-wrapper.sh"
RUSTFLAGS='-D warnings' CARGO_ENCODED_RUSTFLAGS='-Dwarnings' RUSTDOCFLAGS='-D warnings' \
  CAPTURE_TO="$_bf_cap" PATH="$_bf_stub:$PATH" \
  bash "$LAUNCHER" --summary "$TMP/bf.txt" --log "$TMP/bf.log" -- --only fmt >/dev/null 2>&1 || true
if [ ! -s "$_bf_cap" ]; then
  skipc "4b.170 build-flag vars are not forwarded" "wrapper not captured on this host"
else
  _bf_hits=$(grep -cE 'RUSTFLAGS|CARGO_ENCODED_RUSTFLAGS|RUSTDOCFLAGS' "$_bf_cap" || true)
  # NON-VACUITY: the wrapper must actually be a wrapper (it forwards SOMETHING), or a zero above
  # would just mean we captured an empty file.
  _bf_exports=$(grep -c '^export ' "$_bf_cap" || true)
  if [ "$_bf_hits" = 0 ] && [ "$_bf_exports" -ge 1 ]; then
    ok "4b.170 RUSTFLAGS/CARGO_ENCODED_RUSTFLAGS/RUSTDOCFLAGS are NOT forwarded ($_bf_exports other exports present)"
  else
    bad "4b.170 build-flag vars are not forwarded" \
        "build-flag hits=$_bf_hits (want 0), other exports=$_bf_exports (want >=1)"
  fi
fi
# The drop must be DISCLOSED, not silent: the deny arm names itself in SKIPPED, and SKIPPED is
# emitted on the launch banner. Asserted structurally because a stubbed launch refuses before the
# banner prints, so the emitted line cannot be observed here -- the channel is shared with the
# non-identifier and newline-in-value categories.
if grep -q 'build-flag-contamination' "$LAUNCHER" \
   && grep -q 'DROPPED: \$SKIPPED' "$LAUNCHER"; then
  ok "4b.171 the drop names itself in SKIPPED and SKIPPED reaches the banner"
else
  bad "4b.171 the drop is disclosed via SKIPPED on the banner" \
      "$(grep -n 'SKIPPED' "$LAUNCHER" | tail -2)"
fi
# 4b.172 (roborev job 316, Medium): an OUTPUT path that is also a GENERATED RESERVATION path must be
# refused BEFORE any launch. `--summary <log>.launch-lock --log <log>` made the extra-lock loop plant
# its reservation SYMLINK at the advertised summary path; the gate then wrote its summary through the
# link and the exit-time reclamation deleted it.
#
# THE EXIT CODE IS NOT THE DISCRIMINATOR AND MEASURING IT WOULD BE A VACUOUS TEST: the pre-fix
# launcher ALSO exits 1 — it launches, waits 20-65s for a heartbeat that can never appear, and stops
# the unit. The discriminators are (a) that NO unit was launched at all, and (b) that the refusal
# NAMES the collision. Both are required: a bare non-zero exit is produced by the defect too.
rp_t=$(mktemp -d)
rp_out=$(bash "$LAUNCHER" --summary "$rp_t/g.log.launch-lock" --log "$rp_t/g.log" 2>&1 </dev/null)
rp_u=$(printf '%s' "$rp_out" | sed -n 's/^unit:  *//p'); [ -n "$rp_u" ] && echo "$rp_u" >> "$UNITS_FILE"
if printf '%s' "$rp_out" | grep -q 'is also a reservation path this launcher creates'; then
  if [ -z "$rp_u" ] && ! printf '%s' "$rp_out" | grep -q 'gate started but published no'; then
    ok "4b.172 an output path that doubles as a reservation path is refused BEFORE any launch"
  else
    bad "4b.172 an output path that doubles as a reservation path is refused before any launch" \
        "named the collision but a unit was launched anyway (unit=${rp_u:-none})"
  fi
else
  bad "4b.172 an output path that doubles as a reservation path is refused" \
      "did not name the collision: $(printf '%s' "$rp_out" | head -3)"
fi
# 4b.173: the tailored diagnoses must WIN over that backstop, or a reader is sent to the wrong
# mechanism (4b.93 caught exactly this when the backstop was placed first).
if printf '%s' "$rp_out" | grep -q 'SYMLINK at every reservation path'; then
  ok "4b.173 the backstop refusal explains the symlink-then-remove mechanism"
else
  bad "4b.173 the backstop refusal explains the mechanism" "$(printf '%s' "$rp_out" | head -3)"
fi
rm -rf "$rp_t"
# 4b.174/4b.175 (roborev job 318, Medium): the launcher must not accept the reader's RUNNING (exit 2)
# as proof of monitorability without asking whether the unit still exists. A gate that publishes ONE
# heartbeat and then dies before its terminal summary answers RUNNING for the whole staleness window,
# so a bare `0|2)` arm let the launcher exit 0 and advertise a poll for a verdict that will never
# arrive. Asserted STRUCTURALLY at BOTH acceptance sites, because the two differ only in what a
# rejection means and a fix applied to one is the call-site defect this file already has a case for.
_j318_bare=$(grep -cE '^\s*0\|2\)\s*_hb_seen=1' "$LAUNCHER" || true)
if [ "${_j318_bare:-0}" -eq 0 ]; then
  ok "4b.174 no acceptance site treats RUNNING (exit 2) as monitorable without a unit check"
else
  bad "4b.174 no acceptance site treats RUNNING as monitorable without a unit check" \
      "$_j318_bare bare 0|2) arm(s) remain — a one-beat-then-dead gate would exit 0"
fi
# 4b.175 IS DELETED, NOT RE-POINTED A THIRD TIME. Its content was "both acceptance sites gate on
# <predicate-name>", and it broke on a CORRECT change twice: at job 320 when the predicate became
# `_unit_is_affirmatively_live`, and at job 323 when it became `_unit_accepts_as_monitorable`. A claim
# that needs relocating at every correct refactor is pinned to the wrong thing — the rule is to stop
# relocating the promise and pin it at the shared definition instead. Its surviving content (BOTH
# sites, counted) now lives in 4b.201, which counts the two-leg helper and is strictly stronger:
# 4b.174 above still forbids a bare `0|2)` arm, so the "accepts RUNNING with no unit check at all"
# regression remains covered from the other direction.
# 4b.176 (roborev job 318, Low): the launcher validated an absolute `bash` and then exec'd a
# hard-coded /bin/bash via a hard-coded /usr/bin/env, so a valid non-FHS systemd host passed every
# capability check and failed at exec. Assert the resolved paths are what systemd-run is handed AND
# that `env` went through the same validation loop as rm/bash -- resolving without validating would
# reintroduce the hole one step later.
if grep -qE '"\$_env_abs" -i "\$_bash_abs"' "$LAUNCHER" \
   && ! grep -qE '/usr/bin/env -i /bin/bash' "$LAUNCHER" \
   && grep -qE 'for _tool_pair in .*"env:\$_env_abs"' "$LAUNCHER"; then
  ok "4b.176 systemd-run is handed the RESOLVED env/bash, and env is validated alongside rm/bash"
else
  bad "4b.176 systemd-run is handed the resolved env/bash, validated alongside rm/bash" \
      "resolved-exec=$(grep -cE '"\$_env_abs" -i "\$_bash_abs"' "$LAUNCHER") hardcoded=$(grep -cE '/usr/bin/env -i /bin/bash' "$LAUNCHER") validated=$(grep -cE '"env:\$_env_abs"' "$LAUNCHER")"
fi



# --- job 321 F1: a GLOBAL lock selected by caller-controlled env is not global ---------------------
# The lock is only global if every launch on the box picks the SAME path. It was read from
# ${XDG_RUNTIME_DIR:-...}, so two launchers with two individually VALID 0700 runtime directories took
# two DIFFERENT locks and the artifact-set check plus reservation stopped being mutually exclusive.
# Not an invoker's choice: the script's own refusal used to advertise "export XDG_RUNTIME_DIR to a
# 0700 dir you own", so an operator following the printed remedy opted out of the lock BY ACCIDENT.
if grep -qE '^_rundir="/run/user/\$\(id -u\)"$' "$LAUNCHER" \
   && ! grep -qE '^_rundir=.*XDG_RUNTIME_DIR' "$LAUNCHER"; then
  ok "4b.193 the global launch lock path is canonical per-UID, not read from XDG_RUNTIME_DIR"
else
  bad "4b.193 the global launch lock path is canonical per-UID" \
      "$(grep -n '^_rundir=' "$LAUNCHER" | head -2)"
fi
# Scoped to ECHOED lines, not the whole file: the retired advice is still QUOTED in a comment that
# explains why it was retired, and a whole-file grep matched that citation and failed on the fixed
# tree. Third time in this change a test of mine matched its own explanatory text (cf. 4b.187, which
# matched the safe form it was meant to forbid). The property is "the script does not TELL anyone to
# do it", so only emitted lines can violate it.
if grep -E '^\s*echo ' "$LAUNCHER" | grep -q 'export XDG_RUNTIME_DIR to a 0700 dir you own'; then
  bad "4b.194 the runtime-dir refusal no longer advertises the XDG_RUNTIME_DIR bypass" \
      "still advertised in an emitted line"
else
  ok "4b.194 the runtime-dir refusal no longer advertises the XDG_RUNTIME_DIR bypass"
fi

# --- job 321 F2: a HARD LINK is an alias a pathname cannot show -----------------------------------
# Reservations and the artifact-set check identify destinations BY PATH, so two launches naming two
# hard links to ONE inode reserve separately and then write the same file. Second half of an axis the
# script already closed for SYMLINKS (job 169) — same threat, different spelling.
_lc_src=$(sed -n '/^_link_count_state() {/,/^}$/p' "$LAUNCHER")
if [ -z "$_lc_src" ]; then
  bad "4b.195 _link_count_state could be extracted" "extraction produced nothing"
else
  _lcd=$(mktemp -d "$TMP/lc.XXXXXX")
  _lc() { ( eval "$_lc_src"; _link_count_state "$1" ); }
  : > "$_lcd/one"
  : > "$_lcd/real"; ln "$_lcd/real" "$_lcd/alias" 2>/dev/null
  _lc_one=$(_lc "$_lcd/one"); _lc_multi=$(_lc "$_lcd/alias"); _lc_absent=$(_lc "$_lcd/nope")
  if [ "$_lc_multi" = multi ] && [ "$_lc_one" = single ] && [ "$_lc_absent" = single ]; then
    ok "4b.195 _link_count_state: hard-linked=multi, lone file=single, absent=single"
  else
    bad "4b.195 _link_count_state classifies links" \
        "alias='$_lc_multi'(multi) lone='$_lc_one'(single) absent='$_lc_absent'(single)"
  fi
  # `find`, never `stat`: stat's format flags are GNU-vs-BSD incompatible and this script refuses to
  # depend on them elsewhere. And the scan must be THREE-valued — `[ -z "$(find …)" ]` collapses "the
  # scan FAILED" onto "no match", the shape this repo lints for (1699-find-tristate).
  if printf '%s' "$_lc_src" | grep -q 'find ' \
     && ! printf '%s' "$_lc_src" | grep -qE '\bstat ' \
     && printf '%s' "$_lc_src" | grep -q "printf 'unknown'"; then
    ok "4b.196 the link scan uses find (not stat) and has a third UNKNOWN answer"
  else
    bad "4b.196 the link scan uses find and is three-valued" \
        "$(printf '%s' "$_lc_src" | grep -nE 'stat |find |unknown' | head -3)"
  fi
  _al_missing=""
  for _dest in '"$LOGFILE" log' '"$SUMMARY" summary' '"$_hbdest" heartbeat'; do
    grep -qF "_refuse_if_aliased $_dest" "$LAUNCHER" || _al_missing="$_al_missing [$_dest]"
  done
  [ -z "$_al_missing" ] \
    && ok "4b.197 all three write destinations are alias-checked (log, summary, heartbeat)" \
    || bad "4b.197 all three write destinations are alias-checked" "missing:$_al_missing"
  rm -rf "$_lcd"
fi

# --- job 323 F1: acceptance needs TWO affirmative legs, and the orphan case is why ------------------
# `_unit_is_affirmatively_live` reads only ActiveState, and an ORPHANED NON-GATE process keeps a unit
# `active` forever (the very fact `_unit_runs_a_gate` was written for, measured in 4b.165). So a gate
# that published one beat, died and left a child was ACCEPTED as monitorable, and no terminal verdict
# would ever arrive. The veto is keyed on the AFFIRMATIVE absence (rc 1), not on "not rc 0", because
# rc 2 is unmeasurable and a cgroup-v1 host answers 2 ALWAYS (job 322's declared precondition) — so
# demanding rc 0 would red every launch on such a host, which is the guard agents learn to waive.
_acc_src=$( sed -n '/^_unit_state() {/,/^}$/p'                    "$LAUNCHER"
            sed -n '/^_unit_is_affirmatively_live() {/,/^}$/p'    "$LAUNCHER"
            sed -n '/^_unit_accepts_as_monitorable() {/,/^}$/p'   "$LAUNCHER" )
if ! printf '%s' "$_acc_src" | grep -q '^_unit_accepts_as_monitorable()'; then
  bad "4b.198 _unit_accepts_as_monitorable could be extracted" "extraction produced nothing usable"
else
  _acc() {  # <ActiveState> <_unit_runs_a_gate rc> -> rc
    ( eval "$_acc_src"
      eval "systemctl() { printf '%s\n' $1; }"
      eval "_unit_runs_a_gate() { return $2; }"
      _unit_accepts_as_monitorable u.service ) >/dev/null 2>&1; printf '%s' "$?"
  }
  _acc_orphan=$(_acc active 1)     # active unit, affirmatively NO gate => an orphan holds it
  _acc_gate=$(_acc active 0)       # active unit with a gate in the cgroup
  if [ "$_acc_orphan" != 0 ] && [ "$_acc_gate" = 0 ]; then
    ok "4b.198 an ORPHAN-held active unit is NOT accepted, while a real gate still is"
  else
    bad "4b.198 an orphan-held active unit is not accepted" \
        "orphan=$_acc_orphan (nonzero expected) gate=$_acc_gate (0 expected)"
  fi
  # rc 2 must NOT veto, or every cgroup-v1 launch fails after startup.
  _acc_unmeas=$(_acc active 2)
  [ "$_acc_unmeas" = 0 ] \
    && ok "4b.199 an UNMEASURABLE cgroup does not veto acceptance (cgroup-v1 hosts still launch)" \
    || bad "4b.199 an unmeasurable cgroup does not veto acceptance" "rc=$_acc_unmeas (0 expected)"
  # And the ActiveState leg still governs: a terminal or stopping unit refuses even WITH a gate present.
  _acc_bad=""
  for _st in inactive failed deactivating; do
    [ "$(_acc "$_st" 0)" = 0 ] && _acc_bad="$_acc_bad $_st"
  done
  [ -z "$_acc_bad" ] \
    && ok "4b.200 a terminal/stopping unit refuses even with a gate in the cgroup" \
    || bad "4b.200 a terminal/stopping unit refuses even with a gate present" "accepted:$_acc_bad"
  # Both acceptance sites must go through the two-leg helper, not the single-leg predicate.
  _acc_sites=$(grep -cE '_unit_accepts_as_monitorable "\$UNIT"' "$LAUNCHER" || true)
  _acc_single=$(grep -cE '^\s+2\).*_unit_is_affirmatively_live "\$UNIT"' "$LAUNCHER" || true)
  if [ "${_acc_sites:-0}" -ge 2 ] && [ "${_acc_single:-0}" -eq 0 ]; then
    ok "4b.201 BOTH RUNNING acceptance sites use the two-leg helper (found $_acc_sites)"
  else
    bad "4b.201 both RUNNING acceptance sites use the two-leg helper" \
        "two-leg=$_acc_sites single-leg-remaining=$_acc_single"
  fi
fi

# --- job 323 F2: the READER's EMITTED text must not carry a fixed wait figure ----------------------
# This change corrected the "~850s" bound in four DOCUMENTATION sites and left the one an operator
# actually reads — the reader's own STALLED output — untouched. Correcting a claim at the sites you
# happen to notice is not correcting it; the disclosure path needs its own census.
if grep -n 'verdict STALLED' "$REPO_ROOT/scripts/gate-liveness.sh" | grep -qE '~?8[0-9][0-9] ?s'; then
  bad "4b.202 the emitted STALLED guidance carries no fixed wait figure" \
      "a fixed duration is still printed to operators"
else
  ok "4b.202 the emitted STALLED guidance carries no fixed wait figure"
fi
if grep -E 'verdict STALLED' "$REPO_ROOT/scripts/gate-liveness.sh" | grep -q 'LONGEST COMPONENT OF THIS RUN'; then
  ok "4b.203 the emitted STALLED guidance tells the reader to DERIVE the bound"
else
  bad "4b.203 the emitted STALLED guidance tells the reader to derive the bound" "not found"
fi

# --- job 323 F3 / #3769: the reservation is a SET, and every PRE-LAUNCH refusal must release it ----
# The reservation is the summary launch-lock PLUS one marker per remaining artifact, and it was
# released PER SITE — so three sites had drifted into three different answers about one invariant:
# the acquisition failure rolled back all of it, the symlink refusal only `$_reserve`, and the
# truncation failure NONE. The heartbeat and log markers outlived a launch that never happened.
#
# The behavioural cases below run SUBSTITUTED COPIES of the shipped launcher: the two defects are
# point-of-use races (a symlink appearing, or a truncate failing, between an earlier check and the
# launch) and neither is inducible from outside the process. The copy is DERIVED from the shipped
# file and the injection anchor must match EXACTLY ONE line, so a moved or reworded site FAILs the
# case rather than silently passing it.
# THE ANCHOR IS PASSED THROUGH THE ENVIRONMENT, NOT `awk -v`: awk expands escape sequences in a
# `-v` value, and one of the three anchors ENDS IN A BACKSLASH (the multi-line `systemd-run`
# invocation). Through `-v` that anchor would never match its own source line, and the case would
# report "the site moved" about a launcher that had not changed — a false FAIL from the test's own
# plumbing. ENVIRON[] is verbatim.
# AND THE COPY'S `REPO_ROOT` IS PINNED TO THE REAL CHECKOUT (roborev job 50 on this change, Low).
# The launcher derives it from its OWN path, so a copy under $TMP derives a root holding neither
# `scripts/agent-gate.sh` nor `scripts/gate-liveness.sh`: the unit would start and die instantly, and
# 4b.212 would then reach the post-launch refusal because the gate CANNOT RUN — passing while proving
# nothing about a gate that started successfully. A case that reaches its assertion by the wrong route
# is the same defect as one that never reaches it. 4b.212b is the positive control for this pin.
# The substitution is VERIFIED to have applied (exactly one matching line), because a reworded
# derivation would silently restore the broken root.
_inject_before() {  # <tag> <exact-anchor-line> <injected-line> -> prints copy path | rc 1 on any unmet precondition
  local tag="$1" d n m
  d="$TMP/inj-$tag/scripts/flow"                 # .../scripts/flow so the copy derives a REPO_ROOT of the same shape
  mkdir -p "$d" || return 1
  n=$(_INJ_A="$2" awk '$0==ENVIRON["_INJ_A"]{c++} END{print c+0}' "$LAUNCHER")
  [ "$n" = 1 ] || return 1
  _INJ_A="$2" _INJ_I="$3" awk '{ if ($0==ENVIRON["_INJ_A"]) print ENVIRON["_INJ_I"]; print }' \
      "$LAUNCHER" > "$d/gate-detached.sh" || return 1
  _pin_repo_root "$d/gate-detached.sh" || return 1
  printf '%s' "$d/gate-detached.sh"
}
_pin_repo_root() {  # <copy-path> — replace the self-derived REPO_ROOT with this checkout's
  local f="$1" n
  n=$(awk '/^REPO_ROOT=\$\(cd /{c++} END{print c+0}' "$f")
  [ "${n:-0}" = 1 ] || return 1
  _PIN_R="$REPO_ROOT" awk '{ if ($0 ~ /^REPO_ROOT=\$\(cd /) printf "REPO_ROOT=%c%s%c\n", 34, ENVIRON["_PIN_R"], 34
                             else print }' "$f" > "$f.pin" && mv "$f.pin" "$f" || return 1
  grep -qxF "REPO_ROOT=\"$REPO_ROOT\"" "$f" || return 1
}
# NEGATIVE CONTROL. A bare "no locks survived" is not evidence on its own: an assertion that can
# never fail passes for free. Neutering every CALL (never the definition) must make the same
# observable flip, or the case is not measuring the release.
_neuter_release() {  # <copy-path>
  local f="$1" n
  n=$(awk '/^[[:space:]]*_release_reservations([[:space:]]|$)/{c++} END{print c+0}' "$f")
  [ "${n:-0}" -ge 1 ] || return 1
  awk '{ if ($0 ~ /^[[:space:]]*_release_reservations([[:space:]]|$)/) print ":"; else print }' \
      "$f" > "$f.tmp" && mv "$f.tmp" "$f"
}
# A LOCK IS A SYMLINK TO AN OWNER STRING, so its target does NOT exist and `-e` is FALSE for it.
# Testing survival with `-e` alone would report every lock as already gone — a vacuous pass.
# COUNTED WITH `grep -c .`, NOT `wc -l`: command substitution strips the trailing newline, so a
# three-line result carries two newlines and `wc -l` answers 2 — which is how the first version of
# 4b.212 FAILED while holding exactly the three markers it demanded. An off-by-one in a test's own
# arithmetic is indistinguishable, in the log, from the defect it pins.
_locks_count() { printf '%s\n' "$1" | grep -c . ; }
_locks_left() {  # <summary> <log> -> prints the surviving reservation paths, one per line
  local l
  for l in "$1.launch-lock" "$1.heartbeat.launch-lock" "$2.launch-lock"; do
    { [ -L "$l" ] || [ -e "$l" ]; } && printf '%s\n' "$l"
  done
  return 0
}

# The function itself, extracted and run: the two hazards its comment names are the two the
# acquisition loop was fixed for (job 269 / job 319), and a second implementation of that idiom is a
# second place for it to regress.
if ! sed -n '/^_release_reservations() {/,/^}$/p' "$LAUNCHER" | grep -q '^_release_reservations()'; then
  bad "4b.204 _release_reservations exists and could be extracted" "no such function in the launcher"
  bad "4b.205 _release_reservations survives an empty _extra_locks under set -u" "not run: extraction failed"
  bad "4b.206 a second release does not delete a peer's lock" "not run: extraction failed"
else
  _rr_src=$(sed -n '/^_release_reservations() {/,/^}$/p' "$LAUNCHER")
  _rr_d="$TMP/rr"; mkdir -p "$_rr_d"
  ln -s "unit=x|pid=1" "$_rr_d/sum with space.launch-lock"
  ln -s "unit=x|pid=1" "$_rr_d/sum with space.heartbeat.launch-lock"
  # `$?` of the substitution would be the last `printf`'s and so always 0 — a vacuous check. The
  # release's own status is recorded explicitly instead, and stderr is folded in INSIDE the
  # substitution (a `2>&1` after it redirects the assignment, which writes nothing).
  _rr_out=$( ( set -u
               eval "$_rr_src"
               _reserve="$_rr_d/sum with space.launch-lock"
               _extra_locks=("$_rr_d/sum with space.heartbeat.launch-lock")
               _release_reservations || echo FIRST-NONZERO
               _release_reservations || echo SECOND-NONZERO   # a refusal may follow the acquisition rollback
               echo DONE ) 2>&1 )
  _rr_left=$(_locks_left "$_rr_d/sum with space" "$_rr_d/nolog")
  if [ "$_rr_out" = DONE ] && [ -z "$_rr_left" ]; then
    ok "4b.204 _release_reservations removes the WHOLE set, twice, through a space-bearing path"
  else
    bad "4b.204 _release_reservations removes the whole set through a space-bearing path" \
        "out=[$_rr_out] survived: $(printf '%s' "$_rr_left" | tr '\n' ' ')"
  fi
  # An EMPTY array expanded as "${a[@]}" is UNBOUND under `set -u` on bash 3.2 (job 319 F3), and the
  # acquisition-failure site can call this with nothing acquired yet.
  _rr_empty=$( ( set -u; eval "$_rr_src"; _reserve="$_rr_d/absent.launch-lock"; _extra_locks=()
                 _release_reservations; printf 'ok' ) 2>&1 )
  [ "$_rr_empty" = ok ] \
    && ok "4b.205 _release_reservations survives an EMPTY _extra_locks under set -u" \
    || bad "4b.205 _release_reservations survives an empty _extra_locks under set -u" "got: $_rr_empty"
  # IDEMPOTENT AGAINST THE HAZARD, not merely "does not error twice" (roborev job 45 on this change).
  # Calling it twice on ALREADY-REMOVED paths cannot detect the defect it was meant to cover: the
  # question is what the SECOND call does once a PEER has legitimately acquired the summary path in
  # between. So the peer's lock is planted between the calls and must survive — the same
  # delete-a-live-peer's-lock outcome job 269 fixed on the acquisition loop.
  _rr_p="$_rr_d/peer"
  ln -s "unit=mine|pid=1" "$_rr_p.launch-lock"
  _rr_pout=$( ( set -u
                eval "$_rr_src"
                _reserve="$_rr_p.launch-lock"
                _extra_locks=()
                _release_reservations
                ln -s "unit=PEER|pid=2" "$_rr_p.launch-lock"   # a peer acquires the freed path
                _release_reservations
                echo DONE ) 2>&1 )
  _rr_pown=$(readlink "$_rr_p.launch-lock" 2>/dev/null || true)
  if [ "$_rr_pout" = DONE ] && [ "$_rr_pown" = "unit=PEER|pid=2" ]; then
    ok "4b.206 a SECOND release does not delete a lock a peer acquired in between"
  else
    bad "4b.206 a second release does not delete a peer's lock" \
        "out=[$_rr_pout] owner=[${_rr_pown:-<gone>}] (expected unit=PEER|pid=2)"
  fi
fi

if [ "$HAVE_SYSTEMD" != yes ]; then
  skip=$((skip+7)); echo "SKIP 4b.207-4b.212b (no user systemd manager on this host)"
else
  # (a) THE LATE SYMLINK REFUSAL — released only `$_reserve`, leaving the heartbeat and log markers.
  _f3a=$(_inject_before symlink-race 'if [ -L "$LOGFILE" ]; then' \
                        'rm -f "$LOGFILE"; ln -s /dev/null "$LOGFILE"   # INJECTED: the job-200 race') || _f3a=""
  if [ -z "$_f3a" ]; then
    bad "4b.207 the late symlink refusal releases the whole reservation" \
        "the point-of-use symlink check is no longer a unique anchor line in the launcher"
  else
    _f3s="$TMP/f3a-sum"; _f3l="$TMP/f3a.log"
    _f3o=$(bash "$_f3a" --summary "$_f3s" --log "$_f3l" -- --only fmt 2>&1); _f3r=$?
    _f3left=$(_locks_left "$_f3s" "$_f3l")
    if [ "$_f3r" != 0 ] && printf '%s' "$_f3o" | grep -q 'became a symlink after it was checked' \
       && [ -z "$_f3left" ]; then
      ok "4b.207 the late symlink refusal leaves NO reservation behind (exit $_f3r)"
    else
      bad "4b.207 the late symlink refusal leaves no reservation behind" \
          "exit $_f3r survived: $(printf '%s' "$_f3left" | tr '\n' ' ') out=$_f3o"
    fi
    # NEGATIVE CONTROL on the same injected copy: with the release calls neutered the markers MUST
    # survive, or 4b.207 is passing on an observable it cannot actually move.
    _f3c=$(_inject_before symlink-race-ctl 'if [ -L "$LOGFILE" ]; then' \
                          'rm -f "$LOGFILE"; ln -s /dev/null "$LOGFILE"   # INJECTED: the job-200 race')
    if [ -n "$_f3c" ] && _neuter_release "$_f3c"; then
      _f3cs="$TMP/f3c-sum"; _f3cl="$TMP/f3c.log"
      _f3co=$(bash "$_f3c" --summary "$_f3cs" --log "$_f3cl" -- --only fmt 2>&1); _f3cr=$?
      _f3cleft=$(_locks_left "$_f3cs" "$_f3cl")
      if [ "$_f3cr" != 0 ] && [ -n "$_f3cleft" ]; then
        ok "4b.208 control: with the release neutered the markers DO survive ($(_locks_count "$_f3cleft") left)"
      else
        bad "4b.208 control: with the release neutered the markers survive" \
            "exit $_f3cr survived nothing — 4b.207 cannot detect the defect it pins"
      fi
      rm -f "$_f3cleft" 2>/dev/null || true
    else
      bad "4b.208 control: with the release neutered the markers survive" \
          "could not build the control copy (no _release_reservations CALL to neuter)"
    fi
  fi
  # (b) THE TRUNCATION REFUSAL — released nothing at all.
  # PERMISSIONS CANNOT INDUCE THIS, and the first attempt at this case proves it: the early
  # writability probe REMOVES the log again ("so a later refusal leaves the filesystem exactly as it
  # was"), so the pre-launch `>` is a CREATE, and a `chmod 000` on a path that no longer exists is a
  # silent no-op — the copy launched a real gate and the case failed for its own reason, not the
  # launcher's. A DIRECTORY at the path is what fails a create-or-truncate, it is a shape a
  # concurrent peer really can leave behind, and — unlike chmodding the directory — it leaves the
  # rollback able to remove the markers it owns, which is the very thing under test.
  _f3b=$(_inject_before truncate-fail '( : > "$LOGFILE" ) 2>/dev/null || {' \
                        'rm -f "$LOGFILE"; mkdir -p "$LOGFILE"   # INJECTED: make the `>` fail') || _f3b=""
  if [ -z "$_f3b" ]; then
    bad "4b.209 the truncation refusal releases the whole reservation" \
        "the pre-launch truncate is no longer a unique anchor line in the launcher"
  else
    _f3bs="$TMP/f3b-sum"; _f3bl="$TMP/f3b.log"
    _f3bo=$(bash "$_f3b" --summary "$_f3bs" --log "$_f3bl" -- --only fmt 2>&1); _f3br=$?
    _f3bleft=$(_locks_left "$_f3bs" "$_f3bl")
    if [ "$_f3br" != 0 ] && printf '%s' "$_f3bo" | grep -q 'cannot truncate the log' \
       && [ -z "$_f3bleft" ]; then
      ok "4b.209 the truncation refusal leaves NO reservation behind (exit $_f3br)"
    else
      bad "4b.209 the truncation refusal leaves no reservation behind" \
          "exit $_f3br survived: $(printf '%s' "$_f3bleft" | tr '\n' ' ') out=$_f3bo"
    fi
  fi
  # (c) THE FAILED LAUNCH. Guarded differently ON PURPOSE: `systemd-run` has already spoken to the
  # manager, so releasing — the PERMISSIVE act, since it admits a peer onto these paths — is licensed
  # only by an AFFIRMATIVE terminal reading of the unit (`_unit_is_live` rc 1), the same polarity every
  # other reclamation site in this file uses. A shell FUNCTION is injected rather than a PATH shim so
  # the launcher's own earlier capability probe still runs against the real binary.
  _f3d=$(_inject_before launch-fail 'if ! systemd-run --user --unit="$UNIT" --collect --same-dir --quiet \' \
                        'systemd-run() { return 1; }   # INJECTED: the start job fails') || _f3d=""
  if [ -z "$_f3d" ]; then
    bad "4b.210 a FAILED systemd-run releases the reservation when the unit is affirmatively terminal" \
        "the systemd-run invocation is no longer a unique anchor line in the launcher"
  else
    _f3ds="$TMP/f3d-sum"; _f3dl="$TMP/f3d.log"
    _f3do=$(bash "$_f3d" --summary "$_f3ds" --log "$_f3dl" -- --only fmt 2>&1); _f3dr=$?
    _f3dleft=$(_locks_left "$_f3ds" "$_f3dl")
    if [ "$_f3dr" != 0 ] && printf '%s' "$_f3do" | grep -q 'systemd-run failed to start unit' \
       && [ -z "$_f3dleft" ]; then
      ok "4b.210 a FAILED systemd-run leaves NO reservation behind (exit $_f3dr)"
    else
      bad "4b.210 a failed systemd-run leaves no reservation behind" \
          "exit $_f3dr survived: $(printf '%s' "$_f3dleft" | tr '\n' ' ') out=$_f3do"
    fi
  fi
  # (d) THE OTHER HALF OF THAT GUARD — the case an UNCONDITIONAL rollback would also pass (roborev
  # job 45 on this change, Medium). 4b.210 shows the release HAPPENS on a terminal unit; on its own it
  # says nothing about the branch being conditional, so deleting the `_unit_is_live` guard would leave
  # the suite green while the launcher handed a LIVE unit's paths to a peer. `_unit_is_live` is
  # overridden to its permissive answer (0 = live or unmeasurable) — the two states that must NOT
  # release, together, since the launcher deliberately treats them alike — and the markers must survive.
  _f3e=$(_inject_before launch-fail-live 'if ! systemd-run --user --unit="$UNIT" --collect --same-dir --quiet \' \
                        'systemd-run() { return 1; }; _unit_is_live() { return 0; }   # INJECTED: start fails, unit reads LIVE') || _f3e=""
  if [ -z "$_f3e" ]; then
    bad "4b.211 a failed systemd-run KEEPS the reservation when the unit is live or unmeasurable" \
        "the systemd-run invocation is no longer a unique anchor line in the launcher"
  else
    _f3es="$TMP/f3e-sum"; _f3el="$TMP/f3e.log"
    _f3eo=$(bash "$_f3e" --summary "$_f3es" --log "$_f3el" -- --only fmt 2>&1); _f3er=$?
    _f3eleft=$(_locks_left "$_f3es" "$_f3el")
    # ALL THREE, not "at least one" (roborev job 50 on this change, Medium). `-n` passes when a single
    # marker survives, and releasing two of three admits a peer onto a LIVE gate's other two artifacts
    # — the whole failure this branch is conditional for. The count is the requirement.
    if [ "$_f3er" != 0 ] && [ "$(_locks_count "$_f3eleft")" = 3 ]; then
      ok "4b.211 a failed systemd-run KEEPS the reservation when the unit is live/unmeasurable ($(_locks_count "$_f3eleft") held)"
    else
      bad "4b.211 a failed systemd-run keeps the reservation when the unit is live/unmeasurable" \
          "exit $_f3er held $(_locks_count "$_f3eleft") of 3: a partial release still hands a LIVE unit's paths to a peer"
    fi
  fi
  # (e) AND THE POST-LAUNCH REFUSAL MUST KEEP THE WHOLE SET — asserted BEHAVIOURALLY, because 4b.214
  # only reads a comment and a comment cannot stop someone adding a release call underneath it
  # (roborev job 45, same finding). A gate really starts here, so `_hb_seen` is forced to 0 after the
  # verification loop has already run: the refusal fires on its own terms, stops the unit it started,
  # and every marker must still be there when it exits — the unit's processes may still be draining.
  _f3f=$(_inject_before hb-refusal 'if [ "$_hb_seen" -ne 1 ]; then' \
                        '_hb_seen=0   # INJECTED: force the post-launch refusal') || _f3f=""
  if [ -z "$_f3f" ]; then
    bad "4b.212 the post-launch heartbeat refusal KEEPS the whole reservation" \
        "the post-launch refusal is no longer a unique anchor line in the launcher"
  else
    _f3fs="$TMP/f3f-sum"; _f3fl="$TMP/f3f.log"
    _f3fo=$(bash "$_f3f" --summary "$_f3fs" --log "$_f3fl" -- --only fmt 2>&1); _f3fr=$?
    _f3fleft=$(_locks_left "$_f3fs" "$_f3fl")
    # The copy stops the unit itself; record whatever it named so cleanup can still reach it.
    _f3fu=$(printf '%s' "$_f3fo" | sed -n 's/^unit:  *//p'); [ -n "$_f3fu" ] && echo "$_f3fu" >> "$UNITS_FILE"
    if [ "$_f3fr" != 0 ] && printf '%s' "$_f3fo" | grep -q 'published no readable liveness' \
       && [ "$(_locks_count "$_f3fleft")" = 3 ]; then
      ok "4b.212 the post-launch heartbeat refusal KEEPS all three markers (exit $_f3fr)"
    else
      bad "4b.212 the post-launch heartbeat refusal keeps the whole reservation" \
          "exit $_f3fr held: $(printf '%s' "$_f3fleft" | tr '\n' ' ') out=$(printf '%s' "$_f3fo" | head -2)"
    fi
  fi
  # POSITIVE CONTROL FOR THE COPIES THEMSELVES. 4b.212 only means what it says if an UNINJECTED copy
  # launches a gate that DOES become monitorable — otherwise its refusal is an artifact of running a
  # copy at all, and the case would pass on a launcher that had lost the injected line entirely. The
  # copy is built through the same helper (so it carries the same REPO_ROOT pin) with the injection
  # aimed at a line whose only effect is a comment: nothing about the launcher's behaviour changes.
  _f3g=$(_inject_before control-nochange 'if [ "$_hb_seen" -ne 1 ]; then' ':   # INJECTED: no-op') || _f3g=""
  if [ -z "$_f3g" ]; then
    bad "4b.212b control: an UNINJECTED copy still launches a monitorable gate" \
        "could not build the control copy"
  else
    _f3gs="$TMP/f3g-sum"; _f3gl="$TMP/f3g.log"
    _f3go=$(bash "$_f3g" --summary "$_f3gs" --log "$_f3gl" -- --only fmt 2>&1); _f3gr=$?
    _f3gu=$(printf '%s' "$_f3go" | sed -n 's/^unit:  *//p'); [ -n "$_f3gu" ] && echo "$_f3gu" >> "$UNITS_FILE"
    if [ "$_f3gr" = 0 ]; then
      ok "4b.212b control: an uninjected copy launches a MONITORABLE gate, so 4b.212's refusal is the injection's"
    else
      bad "4b.212b control: an uninjected copy launches a monitorable gate" \
          "exit $_f3gr: 4b.212 may be reaching its refusal because a COPY cannot run a gate at all. out=$(printf '%s' "$_f3go" | head -3)"
    fi
    [ -n "$_f3gu" ] && systemctl --user stop "$_f3gu" >/dev/null 2>&1
  fi
fi
# DRIFT ALARM, and DECLARED AS ONE: this counts call sites, it does not prove the span is covered.
# Deciding "every refusal between acquisition and launch calls it" needs block analysis of arbitrary
# shell, which is the unbounded-parse shape this repo has twice descoped for rising false-PASS counts
# (#1712's snapshot scanner, #3229's census-exclusion key). The four paths are pinned BEHAVIOURALLY
# above; this line only tells us if a fifth appears without one.
_rel_calls=$(grep -cE '(^|[[:space:]]|\|\| )_release_reservations([[:space:]]|$)' "$LAUNCHER" || true)
if [ "${_rel_calls:-0}" -ge 4 ]; then
  ok "4b.213 every known pre-launch refusal routes through the one release (call sites: $_rel_calls)"
else
  bad "4b.213 every known pre-launch refusal routes through the one release" \
      "found only ${_rel_calls:-0} call sites; a site has gone back to releasing part of the set"
fi
# And the ONE path that deliberately does NOT release must SAY so, or its omission reads as the same
# oversight this finding was: the post-launch heartbeat refusal stops an already-started gate, whose
# processes may still be draining, so handing its paths to a peer is worse than the litter.
if sed -n '/^if \[ "\$_hb_seen" -ne 1 \]; then/,/^fi$/p' "$LAUNCHER" | grep -q 'DOES \*NOT\* RELEASE'; then
  ok "4b.214 the post-launch refusal declares why it keeps the reservation"
else
  bad "4b.214 the post-launch refusal declares why it keeps the reservation" \
      "an undeclared omission is indistinguishable from the F3 defect"
fi

echo
echo "==== test_gate_detached.sh: passed=$pass failed=$fail skipped=$skip ===="
[ "$fail" -eq 0 ] || exit 1
exit 0
