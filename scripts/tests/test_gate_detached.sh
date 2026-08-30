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

# shellcheck disable=SC2317
cleanup() {
  local u p
  for u in $(cat "$UNITS_FILE" 2>/dev/null); do
    systemctl --user stop "$u" >/dev/null 2>&1
    systemctl --user reset-failed "$u" >/dev/null 2>&1
  done
  for p in $(cat "$TMP/pids" 2>/dev/null); do kill -9 "$p" 2>/dev/null || true; done
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
    for _ in $(seq 1 60); do
      [ -s "$TMP/in.log" ] && [ -s "$TMP/out.log" ] && break
      sleep 1
    done
    in_cg=$(sed -n 's/^cgroup=//p' "$TMP/in.log" | head -1)
    out_cg=$(sed -n 's/^cgroup=//p' "$TMP/out.log" | head -1)
    in_pid=$(sed -n 's/^start pid=\([0-9]*\).*/\1/p' "$TMP/in.log" | head -1)
    out_pid=$(sed -n 's/^start pid=\([0-9]*\).*/\1/p' "$TMP/out.log" | head -1)
    [ -n "$in_pid" ] && echo "$in_pid" >> "$TMP/pids"
    [ -n "$out_pid" ] && echo "$out_pid" >> "$TMP/pids"

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
      for _ in $(seq 1 60); do
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
    for _ in $(seq 1 60); do
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
if grep -q 'published NO heartbeat' "$LAUNCHER"; then
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
if grep -q '_pre_sum_rid' "$LAUNCHER" && grep -q '_new_rid' "$LAUNCHER"; then
  ok "4b.23 the launcher snapshots pre-launch run-ids and binds the check to a NEW one"
else
  bad "4b.23 the launcher binds the post-launch check to a NEW run-id" "no pre-launch snapshot"
fi
# Fixed-string whole-line match (job 178, Low): the run-id is a mktemp PATH, so interpolating it
# into a regex broke on a TMPDIR containing `[` and could stop a HEALTHY gate.
if grep -q 'grep -qxF "run-id: \$_new_rid"' "$LAUNCHER"; then
  ok "4b.24 the heartbeat must carry the NEW run-id, matched as a fixed string"
else
  bad "4b.24 the heartbeat must carry the NEW run-id, matched as a fixed string" "binding not found"
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
if grep -q "printf 'rm -f -- %q" "$LAUNCHER"; then
  ok "4b.35b the generated wrapper unlinks itself before exec"
else
  bad "4b.35b the generated wrapper unlinks itself before exec" "self-unlink not emitted"
fi
# ...and the self-unlink must be the LAST thing before exec, or bash may not have read the file.
if [ "$HAVE_SYSTEMD" = yes ]; then
  et="$TMP/envorder"; mkdir -p "$et"
  TMPDIR="$et" bash "$LAUNCHER" --summary "$TMP/eo.txt" --log "$TMP/eo.log" -- --only file-size >/dev/null 2>&1
  # the script is gone by now, so assert the ORDER from the generator instead
  gen_rm=$(grep -n "printf 'rm -f -- %q" "$LAUNCHER" | head -1 | cut -d: -f1)
  gen_exec=$(grep -n "printf 'exec bash %q" "$LAUNCHER" | head -1 | cut -d: -f1)
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
_g=$!; echo "$_g" >> "$TMP/pids"
_hbf="$TMP/tier.hb"
bash "$BEATER_SH" --file "$_hbf" --run-id tier --gate-pid "$_g" --interval 1 </dev/null >/dev/null 2>&1 &
_b=$!; echo "$_b" >> "$TMP/beater-pids"
for _ in $(seq 1 40); do [ -s "$_hbf" ] && break; sleep 0.5; done
_s1=$(sed -n 's/^beat-seq: //p' "$_hbf" 2>/dev/null)
kill -9 "$_g" 2>/dev/null; wait "$_g" 2>/dev/null || true
sleep 3
_s2=$(sed -n 's/^beat-seq: //p' "$_hbf" 2>/dev/null)
[ -n "$_s1" ] && [ "$_s1" = "$_s2" ] \
  && ok "4b.66 the beater stops advancing once its gate dies (tier: $(sed -n 's/^parent-check: //p' "$_hbf"))" \
  || bad "4b.66 the beater stops advancing once its gate dies" "beat-seq $_s1 -> $_s2"
kill -9 "$_b" 2>/dev/null || true

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
    for _ in $(seq 1 60); do
      grep -qE '^RESULT: (PASS|FAIL|PARTIAL|ERROR|REFUSED)' "$dsum" 2>/dev/null && break
      sleep 2
    done
    grep -qE '^RESULT: ' "$dsum" 2>/dev/null \
      && ok "5.6 the default-path gate reaches a verdict" \
      || bad "5.6 the default-path gate reaches a verdict" "no RESULT in $dsum"
    rm -rf "$ddir" 2>/dev/null || true
  fi
fi

echo
echo "==== test_gate_detached.sh: passed=$pass failed=$fail skipped=$skip ===="
[ "$fail" -eq 0 ] || exit 1
exit 0
