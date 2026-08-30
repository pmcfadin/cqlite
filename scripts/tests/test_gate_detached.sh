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
    for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
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
      for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
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
# Only the log path used to be validated, so a bad summary location launched a gate that
# could publish neither its verdict nor its liveness: 30-50 minutes burned certifying
# nothing, and every poll answering UNKNOWN with no way to tell that from a slow queue.
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
