#!/usr/bin/env bash
# lib-server.sh — the Flight SERVER LIFECYCLE for the WS0 measurement rig: start only
# our own, prove the socket is ours, stop only what we started (issue #3096 review,
# hardened by #3272 review round 3, B4).
#
# Sourced, not executed, and it sets NO shell options: `set -euo pipefail` in a library
# mutates the SOURCING shell's options, which is the caller's decision. The driver sets
# all three itself.
#
# Split into its own file under the campsite rule, along the same responsibility seam the
# other four libraries follow — `lib-cpu.sh` owns the topology, `lib-host-state.sh` the
# host sysctls, `lib-args.sh` the argument grammar, `lib-perf-lint.sh` the perf-invocation
# guard, and this file owns the one thing that is a PROCESS AND A SOCKET rather than a
# number: which program the Flight arm actually measured.
#
# The driver keeps `on_exit` and its single `trap` registration, because composing them is
# a decision about the DRIVER's exit paths (a second top-level `trap ... EXIT` would
# silently discard the first). This file provides the pieces that handler calls.
#
# Four entry points:
#
#   stop_server            — kill ONLY the pid we launched (never a `pkill` by name).
#   require_port_free      — is the port free BEFORE we spawn? Fail closed if not.
#   require_socket_prober  — establish ONCE that ownership can be determined at all.
#   await_server_ready     — wait until OUR server is serving, and FAIL otherwise.
#
# It reads `$PORT` and `$OUT_DIR` from the sourcing driver, and owns `$SERVER_PID`.

# ---------------------------------------------------------------------------
# Server lifecycle — ONLY the process THIS script started (issue #3096 review)
# ---------------------------------------------------------------------------
# This rig used to open each Flight rep with `pkill -x cqlite-flight`, which kills
# EVERY matching process on the box — including a PEER LANE's Flight server on a
# shared fleet machine (one worker per machine is the convention, but the fleet
# runs concurrent gates, e2e tiers and loadgen lanes that start their own
# servers). Clearing the box to make room for a measurement is a destructive
# cross-lane action, and it is silent: the peer just dies.
#
# Instead: remember the PID we launched, kill only that, and treat an occupied
# port as a FAILURE to be reported rather than an obstacle to be removed.
#
# `:=` NOT `=` (#3272 review round 3, found while writing B4's own test). A bare
# `SERVER_PID=""` at library top level CLOBBERS a value the sourcing shell already set,
# which is a real defect in both directions: the self-tests set `SERVER_PID` before
# sourcing to drive `await_server_ready` against a known process (and got an empty one,
# so every case reported "the server exited" instead of exercising the ownership check),
# and a future driver that recorded a pid before sourcing would silently lose it. A
# library may INITIALIZE state it owns; it may not reset its caller's.
: "${SERVER_PID:=}"

# Host sysctl capture/mutate/restore lives in scripts/perf/lib-host-state.sh — the
# only part of this rig that changes state outside its own process tree. The
# driver composes `restore_sysctls` into its single `on_exit` handler below and
# calls `relax_perf_sysctls` once, before the results dir exists.


stop_server() {
  [[ -n "$SERVER_PID" ]] || return 0
  local pid="$SERVER_PID"
  SERVER_PID=""
  kill "$pid" 2>/dev/null || true
  local i
  for i in $(seq 1 20); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.5
  done
  kill -9 "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

# --- WHO OWNS THE LISTENING SOCKET? (issue #3272 review round 3, B4) -----------
# `socket_owner_pid <port>` — the pid LISTENING on <port>, or empty. Empty means
# "nothing is listening" ONLY because `require_socket_prober` (below) established at
# startup that the prober WORKS; without that, an empty answer would be
# indistinguishable from a prober that cannot answer, and reading it as "nobody is
# listening" would be a positive verdict derived from an unmeasured state.
#
# `ss` first (iproute2, the Linux tool this rig targets), `lsof` as the fallback.
socket_owner_pid() {
  local port="$1" out=""
  if command -v ss >/dev/null 2>&1; then
    # `-H` is not portable across iproute2 versions, so the header is filtered by shape:
    # only lines carrying a `pid=<n>` are read.
    out="$(ss -ltnpH "sport = :$port" 2>/dev/null || ss -ltnp 2>/dev/null | grep -F ":$port ")"
    sed -n 's/.*pid=\([0-9]\{1,\}\).*/\1/p' <<<"$out" | head -1
    return 0
  fi
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$port" -sTCP:LISTEN -t 2>/dev/null | head -1
    return 0
  fi
  return 0
}

# The prober must WORK, established once at startup against a socket whose owner this
# script KNOWS. Without this, `socket_owner_pid` returning empty for every port would
# make every ownership check below pass vacuously — a guard reading a clean verdict off
# an oracle that cannot answer (#3272: a positive verdict requires an affirmative
# measurement). Failing here is correct: `ss` is iproute2, present on every Linux host
# this rig runs on, and a rig that cannot tell WHOSE server it measured cannot measure.
require_socket_prober() {
  command -v ss >/dev/null 2>&1 || command -v lsof >/dev/null 2>&1 || {
    echo "FATAL: neither 'ss' nor 'lsof' is installed, so this rig cannot establish that" >&2
    echo "       the Flight server it measures is the one it STARTED. A port that accepts" >&2
    echo "       connections proves only that SOMETHING is listening: if our server fails" >&2
    echo "       to bind and another process holds the port, the load generator measures" >&2
    echo "       THAT server while perf counts our pinned CPUs — a number attributed to" >&2  # perf-lint-allow: a diagnostic STRING
    echo "       the wrong program, with nothing in the output saying so (#3272 B4)." >&2
    echo "       Install iproute2 (ss) or lsof." >&2
    exit 2
  }
  # ...and it must ANSWER. Probed against a listener started HERE, whose pid is known, so
  # a prober that runs but returns nothing (a container without /proc visibility, an `ss`
  # lacking the process-info build option) is caught before it can wave a run through.
  local probe_port="$1" probe_pid observed
  # ACCEPTS in a loop rather than `listen(1)` + `sleep`: a listener that never accepts
  # fills its backlog on the first connect and refuses every later one, so a probe that
  # connected more than once would fail for a reason unrelated to ownership.
  python3 -c '
import socket, sys, time
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", int(sys.argv[1])))
s.listen(64)
s.settimeout(float(sys.argv[2]))
deadline = time.monotonic() + float(sys.argv[2])
while time.monotonic() < deadline:
    try:
        c, _ = s.accept()
        c.close()
    except OSError:
        break
' "$probe_port" 10 &
  probe_pid=$!
  local i
  for i in $(seq 1 20); do
    (echo >"/dev/tcp/127.0.0.1/$probe_port") >/dev/null 2>&1 && break
    sleep 0.25
  done
  observed="$(socket_owner_pid "$probe_port")"
  kill "$probe_pid" 2>/dev/null || true
  wait "$probe_pid" 2>/dev/null || true
  if [[ "$observed" != "$probe_pid" ]]; then
    echo "FATAL: the socket-ownership prober cannot answer. It was pointed at a listener" >&2
    echo "       this script started on 127.0.0.1:$probe_port (pid $probe_pid) and reported" >&2
    echo "       '${observed:-<nothing>}'." >&2
    echo "       An ownership check that cannot identify a KNOWN socket would pass" >&2
    echo "       vacuously for every server below, so the run stops here rather than" >&2
    echo "       measuring a process it cannot attribute (#3272 B4). Common causes: an" >&2
    echo "       'ss' built without process info, a container without /proc visibility," >&2
    echo "       or insufficient privilege to see the socket's owner." >&2
    exit 2
  fi
  echo "socket ownership: verified prober (identified a known listener on port $probe_port)"
}

# Is $PORT free? Fail closed if not: an occupied port means either an orphan of
# ours (report it, do not silently reap something that might not be ours) or
# another lane's server (never ours to kill). `--port` is the remedy.
#
# SCOPE, stated because B4 turned on exactly this: this answers "is the port free NOW",
# BEFORE the spawn. It says nothing about who owns the port AFTERWARDS — a server of ours
# that fails to bind, with any other process holding the port by the time the readiness
# probe runs, satisfies every check here. `await_server_ready` closes that half.
require_port_free() {
  local where="$1" i
  for i in $(seq 1 10); do
    (echo >"/dev/tcp/127.0.0.1/$PORT") >/dev/null 2>&1 || return 0
    sleep 1
  done
  echo "FATAL: 127.0.0.1:$PORT is already accepting connections ($where)." >&2
  echo "       This rig will NOT clear the box: a matching process may be another" >&2
  echo "       lane's Flight server on a shared machine, and killing it is a" >&2
  echo "       destructive cross-lane action (issue #3096 review)." >&2
  echo "       Pick a free port with --port N, or stop the listener yourself after" >&2
  echo "       confirming whose it is (e.g. 'ss -ltnp \"sport = :$PORT\"')." >&2
  exit 2
}

# --- READINESS IS OURS, OR IT IS NOT READINESS (issue #3272 review round 3, B4) --
# Wait until the server THIS FUNCTION'S CALLER STARTED is serving, and FAIL on a timeout.
#
# # What was wrong
#
# Readiness was inferred SOLELY from `(echo >/dev/tcp/127.0.0.1/$PORT)` succeeding, inside
# a `for i in $(seq 1 120)` whose exhaustion was not an error — the loop just ended and the
# measurement proceeded. Three failures that hides, in ascending order of cost:
#
#  1. THE SERVER NEVER CAME UP. The loop times out, control falls through, and the load
#     generator runs against a dead port. Cheap: the loadgen fails and the rep aborts.
#  2. THE SERVER DIED after binding. Same detection, same cost.
#  3. **THE SERVER FAILED TO BIND AND ANOTHER PROCESS HOLDS THE PORT.** The port ACCEPTS,
#     so the probe succeeds on the first attempt. The load generator then measures THAT
#     server — any gRPC listener, another lane's `cqlite-flight`, a stale orphan of an
#     earlier run built from different code — while `perf stat -C` counts OUR pinned CPUs.
#     Nothing in the output says so, and the number is published as `flight_do_get_<arm>`.
#     That is this rig's defining failure mode: an instrument reporting success without
#     having measured the thing it names.
#
# The #3096 preflight check does NOT cover it, and the distinction is worth stating
# precisely because it looks like it should: `require_port_free` runs BEFORE the spawn and
# answers "is the port free now". Case 3 is a port that was free at preflight and is held
# by someone else at measurement time — our bind losing a race, or failing for its own
# reason (a bad `--data-dir`, a panic on startup) while a peer's server binds in the gap.
# Every existing check passes on that sequence.
#
# # What is asserted now, and why each part
#
#  * OUR PID IS ALIVE — `kill -0`. A dead child cannot be what a live socket belongs to.
#  * THE LISTENING SOCKET BELONGS TO OUR PID — `socket_owner_pid`. This is the part no
#    connect-probe can establish, and case 3 is exactly the gap. The observed owner is
#    NAMED in the refusal, since "some other process holds the port" and "our server is
#    not up" are different problems with different remedies.
#  * A TIMEOUT IS FATAL. Falling through a `for` loop is not a readiness verdict; the
#    pre-fix loop's exhaustion was silent.
#
# The socket owner may legitimately be a CHILD of our pid (a supervisor that forks), so a
# match against the process GROUP is accepted and reported — and the reason for that
# leniency is recorded here at the branch rather than left to be re-derived.
# descends_from <pid> <ancestor> — 0 when <pid> IS <ancestor> or a descendant of it.
#
# Walks the real ppid chain, because the cheaper tests are both wrong: a PROCESS GROUP
# match accepts any sibling background job of the same shell (measured — see the branch in
# `await_server_ready`), and comparing `ps -o ppid=` once accepts only a DIRECT child,
# which would refuse a supervisor that forks twice.
#
# Bounded at 64 hops and stops at pid 1 or an unreadable ppid, so a cycle or a
# `ps` that cannot answer terminates the walk with a REFUSAL rather than looping — an
# unmeasurable ancestry is not an ancestry.
descends_from() {
  local pid="$1" ancestor="$2" hops=0 parent
  while [[ -n "$pid" && "$pid" != "0" && "$pid" != "1" && "$hops" -lt 64 ]]; do
    [[ "$pid" == "$ancestor" ]] && return 0
    parent="$(ps -o ppid= -p "$pid" 2>/dev/null | tr -d ' ')"
    [[ -n "$parent" ]] || return 1
    pid="$parent"
    hops=$((hops + 1))
  done
  return 1
}

await_server_ready() {
  local tag="$1" i owner=""
  for i in $(seq 1 120); do
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      echo "FATAL: the Flight server this rig started for $tag (pid $SERVER_PID) is not" >&2
      echo "       running. It exited before serving; see $OUT_DIR/$tag.server.log." >&2
      echo "       This is refused rather than waited out: the pre-fix loop inferred" >&2
      echo "       readiness from the PORT ACCEPTING alone, so a dead server plus any" >&2
      echo "       other listener on the port measured that listener instead (#3272 B4)." >&2
      exit 1
    fi
    if (echo >"/dev/tcp/127.0.0.1/$PORT") >/dev/null 2>&1; then
      # Something is listening. WHOSE is it? The prober was verified at startup, so an
      # empty answer here means "no owner could be identified", not "the check is broken".
      owner="$(socket_owner_pid "$PORT")"
      if [[ "$owner" == "$SERVER_PID" ]]; then
        echo "  $tag server ready (pid $SERVER_PID owns 127.0.0.1:$PORT)"
        return 0
      fi
      # A DESCENDANT of ours is still ours: a supervisor that forks its listener is a
      # legitimate shape, and refusing it would red a correct run.
      #
      # ANCESTRY, not the PROCESS GROUP (#3272 review round 3, found by this fix's own
      # test). A pgid match looked equivalent and is not: every background job of ONE shell
      # inherits that shell's process group, so `sleep 60 &` and an unrelated listener
      # started by the same script share a pgid. MEASURED — the foreign-listener case
      # reported `server ready (pid 28133, a child of 28173)` for two processes with no
      # relationship beyond a common parent, i.e. the guard accepted exactly the situation
      # it exists to refuse. `descends_from` walks the real ppid chain instead.
      if [[ -n "$owner" ]] && kill -0 "$owner" 2>/dev/null \
         && descends_from "$owner" "$SERVER_PID"; then
        echo "  $tag server ready (pid $owner, a descendant of $SERVER_PID, owns 127.0.0.1:$PORT)"
        return 0
      fi
      echo "FATAL: 127.0.0.1:$PORT is being served by pid ${owner:-<unidentified>}, which is" >&2
      echo "       NOT the Flight server this rig started for $tag (pid $SERVER_PID)." >&2
      echo "       The load generator would measure THAT server while perf counted OUR" >&2  # perf-lint-allow: a diagnostic STRING
      echo "       pinned CPUs — a figure published as 'flight do_get' for a program this" >&2
      echo "       rig did not start and cannot identify (#3272 B4). A port that accepts" >&2
      echo "       connections proves only that SOMETHING is listening." >&2
      echo "       Our server's own output: $OUT_DIR/$tag.server.log (it most likely failed" >&2
      echo "       to bind). Pick a free port with --port N after confirming whose listener" >&2
      echo "       that is (e.g. 'ss -ltnp \"sport = :$PORT\"')." >&2
      exit 1
    fi
    sleep 1
  done
  echo "FATAL: the Flight server for $tag (pid $SERVER_PID) did not begin serving" >&2
  echo "       127.0.0.1:$PORT within 120s. A readiness TIMEOUT is a failure, not a" >&2
  echo "       condition to proceed under: the pre-fix loop simply ended and the" >&2
  echo "       measurement ran anyway (#3272 B4). See $OUT_DIR/$tag.server.log." >&2
  exit 1
}

