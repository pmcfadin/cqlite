#!/usr/bin/env bash
# Regression test for issue #2667: the full agent-gate must fire ONE advisory
# push at final-SUMMARY time, converting the summary file from a passive poll
# target into a PUSH signal for a waiting closer/worker. Contract:
#   - title: "gate <RESULT> <branch>@<short-sha>"
#   - body:  "RESULT: <RESULT>" (+ "— failing: c1,c2" when components FAILed)
#   - ADVISORY: for EVERY failure mode of the notify path, gate_push_signal is a
#     silent no-op that returns 0 — it never affects the gate verdict/exit.
#
# SCOPE (issue #3119). This file owns the **ADVISORY** half of the contract: the
# catalogue of ways the notify path can fail without touching the verdict. It
# does NOT — and CANNOT — establish payload fidelity: it asserts the arguments
# the gate produces, and an argv assertion can never observe what the notifier
# ACCEPTS or PUBLISHES. That blind spot is exactly how the swallowed `--category`
# defect survived (the old stub here implemented a `--category` arm the real
# upstream binary does not have, encoding the caller's own wrong assumption).
# Payload fidelity is asserted against the PUBLISHED bytes in
# scripts/tests/test_gate_notify_contract.sh.
#
# Hermetic + fast by design: it does NOT run the 5-8 min real gate. It extracts
# the self-contained gate_push_signal() function from agent-gate.sh, sources just
# that, and drives it with a stubbed notify path.
#
# Run standalone:   bash scripts/tests/test_agent_gate_notify.sh
# Or via the gate:  scripts/agent-gate.sh runs it as the `tooling-tests` component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-notify-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

# Extract just the gate_push_signal() function body (from its opening line to the
# first line that is exactly "}") so we can source it in isolation without
# executing the whole gate.
fnfile="$tmp/gate_push_signal.sh"
awk '/^gate_push_signal\(\) \{/{grab=1} grab{print} grab&&/^\}$/{exit}' "$GATE" > "$fnfile"
if ! grep -q '^gate_push_signal() {' "$fnfile" || ! grep -q '^}$' "$fnfile"; then
  bad "could not extract gate_push_signal() from $GATE"
  echo "----- extracted -----"; cat "$fnfile"; echo "---------------------"
  exit 1
fi
# shellcheck disable=SC1090
. "$fnfile"

# ---------------------------------------------------------------------------
# The notify path under test. gate_push_signal delegates delivery to the
# repo-owned scripts/lib/gate-notify.sh (#3119), so REPO_ROOT must resolve to
# this checkout for the real wrapper to be found.
# ---------------------------------------------------------------------------
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
export REPO_ROOT
LIB="$REPO_ROOT/scripts/lib/gate-notify.sh"
WEBHOOK="https://ntfy.invalid/advisory-topic-3119"

stubdir="$tmp/bin"
mkdir -p "$stubdir"

# A curl capture shim: one line per published payload. Used ONLY to count
# publishes and to inspect the flags the wrapper passes; payload CONTENT is
# asserted in scripts/tests/test_gate_notify_contract.sh.
cat > "$stubdir/curl" <<'CURLSHIM'
#!/usr/bin/env bash
{ printf 'CURL'; for a in "$@"; do printf '\t%s' "$a"; done; printf '\n'; } >> "$CURL_LOG"
CURLSHIM
chmod +x "$stubdir/curl"

# drive <log> <extra-PATH-dir> <result> [env assignments...]
# Runs the REAL gate_push_signal + REAL wrapper, capturing stdout/stderr.
drive() {
  local log="$1" bindir="$2" result="$3"; shift 3
  : > "$log"
  capped env CURL_LOG="$log" CQLITE_NOTIFY_WEBHOOK="$WEBHOOK" PATH="$bindir:$PATH" "$@" \
    bash -c '. "$0"; gate_push_signal "$1" advisory-branch abc1234 ""' \
    "$fnfile" "$result" >"$tmp/out.txt" 2>"$tmp/err.txt"
  return $?
}
silent() { [ ! -s "$tmp/out.txt" ] && [ ! -s "$tmp/err.txt" ]; }
# TIMING-CASE CAP (issue #3119, review round 5). Every case that measures elapsed
# time is asserting "this call is bounded". Under the mutation such a case exists to
# catch, the call is NOT bounded — so without an INDEPENDENT cap the case does not go
# red, it HANGS, and because scripts/agent-gate.sh has no per-component timeout the
# whole gate hangs with it. Measured: an audit run was SIGKILLed at 250s with no
# verdict. SIGKILL (-s KILL) because the very thing under test may ignore SIGTERM.
# INVARIANT: every `date +%s` case in this file routes its subject through capped().
TIMING_CAP="${TIMING_CAP:-25}"
capped() { timeout -s KILL "$TIMING_CAP" "$@"; }

# grep -c prints 0 and exits 1 on no match; capture the count, ignore the status.
publishes() { local n; n=$(grep -c '^CURL' "$1" 2>/dev/null); printf '%s\n' "${n:-0}"; }

# ---- Case 1: the happy path publishes exactly once, silently, rc=0 -----------
log="$tmp/case1.log"
drive "$log" "$stubdir" PASS
rc=$?
if [ "$rc" -eq 0 ] && [ "$(publishes "$log")" -eq 1 ] && silent; then
  ok "happy path: ONE publish, rc=0, silent"
else
  bad "happy path (rc=$rc publishes=$(publishes "$log"))"; cat "$log" "$tmp/err.txt"
fi

# ---- Case 2: FAIL also publishes exactly once, silently, rc=0 ----------------
log="$tmp/case2.log"
drive "$log" "$stubdir" FAIL
rc=$?
if [ "$rc" -eq 0 ] && [ "$(publishes "$log")" -eq 1 ] && silent; then
  ok "FAIL result: ONE publish, rc=0, silent"
else
  bad "FAIL result (rc=$rc publishes=$(publishes "$log"))"; cat "$log" "$tmp/err.txt"
fi

# ---- Case 3: the publish is TIME-BOUNDED (curl carries --max-time) -----------
if grep -q $'\t--max-time\t' "$tmp/case2.log"; then
  ok "publish is time-bounded: curl is invoked with --max-time"
else
  bad "publish is NOT time-bounded: no --max-time in the curl invocation"
fi

# ---- Case 3b: the bound is an OUTER one, not just the cooperative flag --------
# #3119 B2: --max-time is honoured only by the REAL curl, and this very file proves
# a bash script can BE curl on PATH. gate_push_signal runs after the terminal
# summary emit but before the gate's exit, so an unbounded transport would leave
# the gate process alive forever and its EXIT trap would never release the #1825
# gate slot — every later gate on the box would queue indefinitely. Assert with a
# transport that IGNORES --max-time and hangs.
hangcurl="$tmp/hangcurl"; mkdir -p "$hangcurl"
printf '#!/usr/bin/env bash\nsleep 600\n' > "$hangcurl/curl"
chmod +x "$hangcurl/curl"
t0=$(date +%s)
capped env CURL_LOG="$tmp/case3b.log" CQLITE_NOTIFY_WEBHOOK="$WEBHOOK" PATH="$hangcurl:$PATH" \
  GATE_NOTIFY_CURL_TIMEOUT=2 GATE_NOTIFY_ADJUNCT_TIMEOUT=2 \
  bash -c '. "$0"; gate_push_signal PASS advisory-branch abc1234 ""' "$fnfile" \
  >"$tmp/out.txt" 2>"$tmp/err.txt"
rc=$?
elapsed=$(( $(date +%s) - t0 ))
# Loose ceiling (2s bound + slack): the property is "bounded at all". rc must be 0 —
# rc=137 at TIMING_CAP is the cap firing, i.e. the bound escaped.
if [ "$rc" -eq 0 ] && [ "$elapsed" -lt "$TIMING_CAP" ] && silent; then
  ok "hanging transport that ignores --max-time: abandoned at the OUTER bound (${elapsed}s), rc=0"
else
  bad "hanging-transport case (rc=$rc elapsed=${elapsed}s) — the publish is not outer-bounded"
fi

# ---- Case 3c: a wedged payload ENCODER cannot stall the gate either -----------
# Same class as 3b for python3 (a pyenv/conda/NFS shim that stalls): the encoder
# runs inside a command substitution, so an unbounded one blocks gate_push_signal.
hangpy="$tmp/hangpy"; mkdir -p "$hangpy"; cp "$stubdir/curl" "$hangpy/curl"
printf '#!/usr/bin/env bash\nsleep 600\n' > "$hangpy/python3"
chmod +x "$hangpy/python3"
t0=$(date +%s)
capped env CURL_LOG="$tmp/case3c.log" CQLITE_NOTIFY_WEBHOOK="$WEBHOOK" PATH="$hangpy:$PATH" \
  GATE_NOTIFY_PAYLOAD_TIMEOUT=2 GATE_NOTIFY_ADJUNCT_TIMEOUT=2 \
  bash -c '. "$0"; gate_push_signal PASS advisory-branch abc1234 ""' "$fnfile" \
  >"$tmp/out.txt" 2>"$tmp/err.txt"
rc=$?
elapsed=$(( $(date +%s) - t0 ))
if [ "$rc" -eq 0 ] && [ "$elapsed" -lt "$TIMING_CAP" ] && silent; then
  ok "wedged payload encoder: abandoned at its bound (${elapsed}s), rc=0, nothing published"
else
  bad "wedged-encoder case (rc=$rc elapsed=${elapsed}s) — the encoder is not bounded"
fi

# ---- Case 3e: a helper that IGNORES SIGTERM is still killed --------------------
# THE defect this case exists for: plain `timeout <secs> <cmd>` sends SIGTERM ONLY.
# Measured against a `trap "" TERM` helper, `timeout 2` NEVER RETURNED (still alive
# when a 20s harness SIGKILLed it) — the bound bought nothing, re-opening the exact
# signature this issue exists to prevent (gate_push_signal hanging after the summary
# emit, so the gate never exits and the #1825 slot is never released). Every wedge in
# the cases above uses `sleep`, which DIES on SIGTERM, so none of them can see this.
# The fix is `--kill-after=<grace>`, whose SIGKILL cannot be trapped.
trapdir="$tmp/trapterm"; mkdir -p "$trapdir"
for helper in curl python3 agent-notify; do
  printf '#!/usr/bin/env bash\ntrap "" TERM\nwhile :; do sleep 0.2; done\n' > "$trapdir/$helper"
  chmod +x "$trapdir/$helper"
done
# An INDEPENDENT hard cap (SIGKILL, so it cannot itself be ignored) wraps the call:
# under a regression to a plain SIGTERM-only `timeout` this case must produce a clean
# RED, not hang the component until the gate's own timeout. Verified by mutation:
# without this cap the whole suite had to be SIGKILLed at 90s with no verdict at all.
t0=$(date +%s)
capped env CURL_LOG="$tmp/case3e.log" CQLITE_NOTIFY_WEBHOOK="$WEBHOOK" PATH="$trapdir:$PATH" \
  GATE_NOTIFY_PAYLOAD_TIMEOUT=1 GATE_NOTIFY_CURL_TIMEOUT=1 GATE_NOTIFY_ADJUNCT_TIMEOUT=1 \
  bash -c '. "$0"; gate_push_signal PASS advisory-branch abc1234 ""' "$fnfile" \
  >"$tmp/out.txt" 2>"$tmp/err.txt"
rc=$?
elapsed=$(( $(date +%s) - t0 ))
# Two SIGTERM-ignoring steps at (1s bound + 1s grace) each, so a correct
# implementation returns in a few seconds; rc=137 or elapsed at the cap means the
# bound escaped and the caller was never released.
if [ "$rc" -eq 0 ] && [ "$elapsed" -lt "$TIMING_CAP" ] && silent; then
  ok "SIGTERM-IGNORING helpers: killed at bound+grace (${elapsed}s), rc=0 — the bound is unignorable"
else
  bad "SIGTERM-ignoring-helper case (rc=$rc elapsed=${elapsed}s, cap ${TIMING_CAP}s) — an escapable SIGTERM-only bound"
fi

# ---- Case 3f: a SIGTERM-only timeout is PROBED AND REJECTED, not merely failed --
# Capability is PROBED, not assumed: a `timeout` lacking --kill-after can only be
# escaped, so it must be treated as NO bounding tool (publish nothing) rather than
# trusted.
#
# DISCRIMINATION (review round 5). "publishes nothing, rc=0" is NOT sufficient
# evidence: with the probe REMOVED the wrapper still attempts each bounded call, the
# stub still rejects it, and the observable outcome is identical — so the assertion
# passed on the stub's own rc=125 rather than on the probe existing. Measured:
# deleting the probe left the suite 18/18 green. The discriminator is WHICH
# invocations the stub receives:
#   probed-and-rejected  -> exactly the probe (`--kill-after=1 1 true`), and NOTHING
#                           else is ever attempted
#   attempted-and-failed -> the stub is handed the real work (python3/curl), i.e. the
#                           wrapper ran behind a bound it had not verified
sigtermonly="$tmp/sigtermonly"; mkdir -p "$sigtermonly"
cp "$stubdir/curl" "$sigtermonly/curl"
cat > "$sigtermonly/timeout" <<'TO'
#!/usr/bin/env bash
# A SIGTERM-only timeout: rejects --kill-after exactly as pre-8.5 coreutils would,
# and RECORDS every invocation so the caller's intent is observable.
{ printf 'TIMEOUT'; for a in "$@"; do printf '\t%s' "$a"; done; printf '\n'; } >> "$TIMEOUT_LOG"
case "${1:-}" in --kill-after*|-k) echo "timeout: unrecognized option '$1'" >&2; exit 125 ;; esac
exec /usr/bin/timeout "$@"
TO
chmod +x "$sigtermonly/timeout"
: > "$tmp/case3f.log"
: > "$tmp/case3f.timeout.log"
# capped OUTSIDE env: `capped` is a shell function, so `env … capped` would exec a
# non-existent binary (rc=127). This ordering also means the CAP uses the real
# timeout(1) from the outer PATH, never the SIGTERM-only stub under test.
capped env CURL_LOG="$tmp/case3f.log" TIMEOUT_LOG="$tmp/case3f.timeout.log" \
  CQLITE_NOTIFY_WEBHOOK="$WEBHOOK" PATH="$sigtermonly:$PATH" \
  bash -c '. "$0"; gate_push_signal PASS advisory-branch abc1234 ""' "$fnfile" \
  >"$tmp/out.txt" 2>"$tmp/err.txt"
rc=$?
probe_seen=$(grep -c $'^TIMEOUT\t--kill-after=1\t1\ttrue$' "$tmp/case3f.timeout.log" 2>/dev/null)
work_attempted=$(grep -cE $'^TIMEOUT\t.*\t(python3|curl|agent-notify)' "$tmp/case3f.timeout.log" 2>/dev/null)
if [ "$rc" -eq 0 ] && [ "$(publishes "$tmp/case3f.log")" -eq 0 ] && silent \
   && [ "${probe_seen:-0}" -ge 1 ] && [ "${work_attempted:-0}" -eq 0 ]; then
  ok "SIGTERM-only timeout: PROBED and rejected (probe seen, zero work attempted), publishes nothing, rc=0"
else
  bad "SIGTERM-only-timeout case (rc=$rc publishes=$(publishes "$tmp/case3f.log") probe=${probe_seen:-0} work-attempted=${work_attempted:-0}) — the capability was not probed before use"
fi

# ---- Case 3d: with NO bounding tool, publish NOTHING rather than run unbounded -
nobound="$tmp/nobound"; mkdir -p "$nobound"
cp "$stubdir/curl" "$nobound/curl"
# A PATH holding ONLY curl + python3: no timeout(1), no gtimeout(1).
py=$(command -v python3); [ -n "$py" ] && ln -sf "$py" "$nobound/python3"
: > "$tmp/case3d.log"
# Absolute interpreter: `env PATH=… bash` would resolve bash from the STRIPPED
# PATH and die 127 before the code under test ever runs.
env CURL_LOG="$tmp/case3d.log" CQLITE_NOTIFY_WEBHOOK="$WEBHOOK" PATH="$nobound" \
  "${BASH:-/bin/bash}" -c '. "$0"; gate_push_signal PASS advisory-branch abc1234 ""' "$fnfile" \
  >"$tmp/out.txt" 2>"$tmp/err.txt"
rc=$?
if [ "$rc" -eq 0 ] && [ "$(publishes "$tmp/case3d.log")" -eq 0 ] && silent; then
  ok "no timeout(1)/gtimeout(1): publishes NOTHING rather than running unbounded, rc=0"
else
  bad "no-bounding-tool case (rc=$rc publishes=$(publishes "$tmp/case3d.log")) — ran unbounded"
fi

# ---------------------------------------------------------------------------
# The ADVISORY failure catalogue (#3119 AC4). For every one of these the
# function must return 0 and write nothing — a notification path must NEVER be
# able to fail a gate.
# ---------------------------------------------------------------------------

# ---- Case 4: agent-notify absent -> silent no-op, still returns 0 ------------
# An empty PATH also removes curl, so nothing can be published either.
log="$tmp/case4.log"
: > "$log"
CURL_LOG="$log" PATH="/nonexistent-dir-2667" gate_push_signal PASS somebranch cafef00d "" \
  >"$tmp/out.txt" 2>"$tmp/err.txt"
rc=$?
if [ "$rc" -eq 0 ] && [ "$(publishes "$log")" -eq 0 ] && silent; then
  ok "absent notifier + absent curl: silent no-op, returns 0"
else
  bad "absent case (rc=$rc publishes=$(publishes "$log"))"
fi

# ---- Case 5: a notifier that REJECTS ALL ARGUMENTS ---------------------------
# THE hole the old argv-stub left: the real upstream agent-notify has no
# --category arm, and a helper that usage-errors on everything it is handed must
# still be harmless. This is the exact class that produced issue #3119.
rejectdir="$tmp/reject"; mkdir -p "$rejectdir"; cp "$stubdir/curl" "$rejectdir/curl"
cat > "$rejectdir/agent-notify" <<'REJECT'
#!/usr/bin/env bash
echo "agent-notify: error: unrecognised arguments: $*" >&2
exit 2
REJECT
chmod +x "$rejectdir/agent-notify"
log="$tmp/case5.log"
drive "$log" "$rejectdir" FAIL
rc=$?
if [ "$rc" -eq 0 ] && silent; then
  ok "notifier rejects ALL arguments: rc=0, nothing on stdout/stderr"
else
  bad "rejects-all-arguments case (rc=$rc)"; cat "$tmp/err.txt"
fi

# ---- Case 6: a notifier that exits non-zero ---------------------------------
faildir="$tmp/failing"; mkdir -p "$faildir"; cp "$stubdir/curl" "$faildir/curl"
printf '#!/usr/bin/env bash\nexit 17\n' > "$faildir/agent-notify"
chmod +x "$faildir/agent-notify"
log="$tmp/case6.log"
drive "$log" "$faildir" PASS
rc=$?
if [ "$rc" -eq 0 ] && silent; then
  ok "notifier exits non-zero: rc=0, silent"
else
  bad "failing-notifier case (rc=$rc)"
fi

# ---- Case 7: a notifier present but NOT EXECUTABLE --------------------------
noexecdir="$tmp/noexec"; mkdir -p "$noexecdir"; cp "$stubdir/curl" "$noexecdir/curl"
printf '#!/usr/bin/env bash\nexit 0\n' > "$noexecdir/agent-notify"
chmod 644 "$noexecdir/agent-notify"
log="$tmp/case7.log"
drive "$log" "$noexecdir" PASS
rc=$?
if [ "$rc" -eq 0 ] && silent; then
  ok "notifier present but not executable: rc=0, silent"
else
  bad "non-executable-notifier case (rc=$rc)"; cat "$tmp/err.txt"
fi

# ---- Case 8: a notifier that HANGS is abandoned at its own bound -------------
hangdir="$tmp/hang"; mkdir -p "$hangdir"; cp "$stubdir/curl" "$hangdir/curl"
printf '#!/usr/bin/env bash\nsleep 600\n' > "$hangdir/agent-notify"
chmod +x "$hangdir/agent-notify"
log="$tmp/case8.log"
t0=$(date +%s)
drive "$log" "$hangdir" PASS GATE_NOTIFY_ADJUNCT_TIMEOUT=2
rc=$?
elapsed=$(( $(date +%s) - t0 ))
# Ceiling is the shared TIMING_CAP (2s bound + generous slack) so CPU contention
# cannot flake it; the property under test is "bounded at all", not a latency SLO.
# `drive` routes through capped(), so an unbounded adjunct reds here instead of hanging.
if [ "$rc" -eq 0 ] && [ "$elapsed" -lt "$TIMING_CAP" ] && silent; then
  ok "hanging notifier: abandoned at its bound (${elapsed}s), rc=0, silent"
else
  bad "hanging-notifier case (rc=$rc elapsed=${elapsed}s)"
fi

# ---- Case 9: the repo-owned wrapper missing -> no-op ------------------------
log="$tmp/case9.log"
: > "$log"
env CURL_LOG="$log" CQLITE_NOTIFY_WEBHOOK="$WEBHOOK" PATH="$stubdir:$PATH" \
  REPO_ROOT="$tmp/no-such-checkout" \
  bash -c '. "$0"; gate_push_signal PASS advisory-branch abc1234 ""' "$fnfile" \
  >"$tmp/out.txt" 2>"$tmp/err.txt"
rc=$?
if [ "$rc" -eq 0 ] && [ "$(publishes "$log")" -eq 0 ] && silent; then
  ok "repo-owned wrapper missing: silent no-op, returns 0"
else
  bad "missing-wrapper case (rc=$rc publishes=$(publishes "$log"))"; cat "$tmp/err.txt"
fi

# ---- Case 10: no notify target configured -> nothing published --------------
log="$tmp/case10.log"
: > "$log"
env CURL_LOG="$log" PATH="$stubdir:$PATH" \
  CQLITE_NOTIFY_WEBHOOK= CODEX_NOTIFY_WEBHOOK= \
  bash -c '. "$0"; gate_push_signal PASS advisory-branch abc1234 ""' "$fnfile" \
  >"$tmp/out.txt" 2>"$tmp/err.txt"
rc=$?
if [ "$rc" -eq 0 ] && [ "$(publishes "$log")" -eq 0 ] && silent; then
  ok "no notify target: nothing published, rc=0, silent"
else
  bad "no-target case (rc=$rc publishes=$(publishes "$log"))"; cat "$tmp/err.txt"
fi

# ---- Case 11: a bare server root with no topic override never guesses -------
log="$tmp/case11.log"
: > "$log"
env CURL_LOG="$log" PATH="$stubdir:$PATH" \
  CQLITE_NOTIFY_WEBHOOK="https://ntfy.invalid" CQLITE_NOTIFY_TOPIC= CODEX_NOTIFY_NTFY_TOPIC= \
  bash -c '. "$0"; gate_push_signal PASS advisory-branch abc1234 ""' "$fnfile" \
  >"$tmp/out.txt" 2>"$tmp/err.txt"
rc=$?
if [ "$rc" -eq 0 ] && [ "$(publishes "$log")" -eq 0 ] && silent; then
  ok "unresolvable topic: nothing published (never a guessed topic), rc=0"
else
  bad "unresolvable-topic case (rc=$rc publishes=$(publishes "$log"))"; cat "$log"
fi

# ---- Case 11b: the supervisor's EXIT-PATH notify budget fits under #2666 -----
# #2666 pins a <15s supervisor exit latency; its own Test 22 stubs the notifier, so
# it cannot see the real bound. finalize_exit fires TWO notifies, and each runs THREE
# bounded steps SEQUENTIALLY (encoder, publish, adjunct) — so the per-notify worst
# case is their SUM, not the two the first revision of that block counted. Read the
# three values out of worker-supervisor.sh and measure the real thing with ALL THREE
# helpers wedged, so the arithmetic cannot drift silently.
SUPERVISOR="$SCRIPT_DIR/../local/worker-supervisor.sh"
LIBFILE="$SCRIPT_DIR/../lib/gate-notify.sh"
bound_of() { # <var-name> <file>
  sed -n "s/^$1=\"\${$1:-\([0-9]*\)}\".*/\1/p" "$2" | head -1
}
ep=$(bound_of NOTIFY_EXIT_PAYLOAD_TIMEOUT "$SUPERVISOR")
ec=$(bound_of NOTIFY_EXIT_CURL_TIMEOUT "$SUPERVISOR")
ea=$(bound_of NOTIFY_EXIT_ADJUNCT_TIMEOUT "$SUPERVISOR")
# The SIGKILL grace is ADDITIVE WALL-CLOCK and must be counted ONCE PER STEP.
# Omitting a term from this sum is the exact defect this assertion exists to catch —
# it has already happened twice (the encoder bound, then this grace).
gr=$(bound_of GATE_NOTIFY_KILL_GRACE "$LIBFILE")
if [ -z "$ep" ] || [ -z "$ec" ] || [ -z "$ea" ] || [ -z "$gr" ]; then
  bad "exit-path budget: could not read the NOTIFY_EXIT_* bounds and/or GATE_NOTIFY_KILL_GRACE"
else
  # SIGTERM-IGNORING wedges. A `sleep`-based wedge DIES on SIGTERM, so it cannot
  # observe an escapable bound at all — that structural blindness is why the plain
  # `timeout` defect reached review instead of the fast loop.
  wedged="$tmp/wedged"; mkdir -p "$wedged"
  for helper in curl python3 agent-notify; do
    printf '#!/usr/bin/env bash\ntrap "" TERM\nwhile :; do sleep 0.2; done\n' > "$wedged/$helper"
    chmod +x "$wedged/$helper"
  done
  # 2 notifies x 3 sequential steps, each step costing (bound + grace).
  budget=$(( 2 * ((ep + gr) + (ec + gr) + (ea + gr)) ))
  # Independent SIGKILL cap per notify, for the same reason case 3e has one: under a
  # regression to an escapable bound this must RED, not hang the component until the
  # gate's own timeout. Measured: without a cap the suite ran past 200s with no verdict.
  cap=$(( budget + 10 ))
  t0=$(date +%s)
  for _ in 1 2; do   # finalize_exit's two notifies
    env CURL_LOG="$tmp/case11b.log" CQLITE_NOTIFY_WEBHOOK="$WEBHOOK" PATH="$wedged:$PATH" \
      GATE_NOTIFY_PAYLOAD_TIMEOUT="$ep" GATE_NOTIFY_CURL_TIMEOUT="$ec" \
      GATE_NOTIFY_ADJUNCT_TIMEOUT="$ea" \
      timeout -s KILL "$cap" \
      bash -c '. "$0"; gate_push_signal FAIL advisory-branch abc1234 "fmt"' "$fnfile" \
      >"$tmp/out.txt" 2>"$tmp/err.txt"
  done
  elapsed=$(( $(date +%s) - t0 ))
  # The DECLARED budget must fit the pinned ceiling, and the MEASURED worst case must
  # not exceed the declared budget (plus process-spawn slack).
  if [ "$budget" -lt 15 ] && [ "$elapsed" -le $((budget + 8)) ]; then
    ok "exit-path notify budget: 2 x [($ep+$gr)+($ec+$gr)+($ea+$gr)] = ${budget}s < 15s (#2666), measured ${elapsed}s against SIGTERM-ignoring wedges"
  else
    bad "exit-path notify budget: declared ${budget}s (2 x [($ep+$gr)+($ec+$gr)+($ea+$gr)]) vs #2666's 15s ceiling; measured ${elapsed}s"
  fi
fi

# ---------------------------------------------------------------------------
# The CALL SITE (issue #3119 spec R1 scenario 4 / R3 scenario 3). Every case above
# drives the extracted gate_push_signal FUNCTION, so none of them observes how the
# gate DECIDES what to pass it. Those two scenarios are about exactly that decision,
# so they are asserted here by extracting the call-site block from agent-gate.sh and
# driving it in a sandbox with the surrounding gate state faked — READ-ONLY: nothing
# in agent-gate.sh is modified, and no testability seam was added to it.
# ---------------------------------------------------------------------------
callsite="$tmp/callsite.sh"
awk '/^if \[ -z "\$ONLY" \] && \[ "\$LITE" -eq 0 \] && \[ "\$DELTA" -eq 0 \] && \[ "\$SELFTEST" -eq 0 \]; then/{grab=1}
     grab{print}
     grab&&/^fi$/{exit}' "$GATE" > "$callsite"
nonfailing_src=$(sed -n '/^_status_is_nonfailing() {/,/^}$/p' "$GATE")
if ! grep -q 'gate_push_signal "\$_push_result"' "$callsite" \
   || ! grep -q 'TREE_COMMIT_LINE' "$callsite" \
   || ! grep -q 'SUMMARY_WRITE_FAILED' "$callsite" \
   || [ -z "$nonfailing_src" ]; then
  bad "could not extract the push-signal CALL SITE (or _status_is_nonfailing) from $GATE"
else
  # drive_callsite <overall> <summary-write-failed> <commit-line> [statuses...]
  # Replaces gate_push_signal with a recorder so the assertion is about the
  # ARGUMENTS THE GATE CHOSE, which is what both scenarios specify.
  drive_callsite() {
    local overall="$1" swf="$2" commitline="$3"; shift 3
    ONLY="" LITE=0 DELTA=0 SELFTEST=0 \
    OVERALL="$overall" SUMMARY_WRITE_FAILED="$swf" TREE_COMMIT_LINE="$commitline" \
    STATUS_LIST="$*" \
    bash -c '
      ONLY="$ONLY"; LITE=$LITE; DELTA=$DELTA; SELFTEST=$SELFTEST
      OVERALL="$OVERALL"; SUMMARY_WRITE_FAILED=$SUMMARY_WRITE_FAILED
      TREE_COMMIT_LINE="$TREE_COMMIT_LINE"
      NAMES=(); STATUSES=()
      i=0; for st in $STATUS_LIST; do NAMES+=("c$i"); STATUSES+=("$st"); i=$((i+1)); done
      gate_push_signal() { printf "%s|%s|%s|%s\n" "$1" "$2" "$3" "$4"; }
      # #3625: the call site asks _status_is_nonfailing which components are non-passing
      # (the CLOSED set — PASS and SKIP — rather than the single literal FAIL token), so
      # that ONE definition must be in scope here. Sourced from the SHIPPED gate, never
      # re-implemented: a copy of a closed set is a second place for it to drift, and an
      # UNDEFINED function would silently make every component "non-passing" (127 is
      # non-zero) and this case would then be asserting on a command-not-found.
      eval "$2"
      . "$1"
    ' _ "$callsite" "$(sed -n '/^_status_is_nonfailing() {/,/^}$/p' "$GATE")" 2>/dev/null
  }

  # ---- R1 S4: the title names the identity the SUMMARY BLOCK stamped ----------
  # A fresh emit-time `git` read would report the CURRENT checkout; the block's
  # stamped line is the authority (#2926 review C1). Feed a commit line that could
  # not possibly match this worktree and assert it is what comes through.
  out=$(drive_callsite PASS 0 "commit: feedface branch: stamped-branch dirty: no" PASS PASS)
  if [ "$out" = "PASS|stamped-branch|feedface|" ]; then
    ok "R1 S4: push identity is taken from the SUMMARY's stamped line, not a fresh git read"
  else
    bad "R1 S4: expected 'PASS|stamped-branch|feedface|', got '$out'"
  fi
  # A malformed/absent stamp must degrade to the documented placeholders, never to a
  # silently wrong identity.
  out=$(drive_callsite PASS 0 "" PASS)
  case "$out" in
    PASS\|unknown\|unknown\|*) ok "R1 S4: an unparseable stamp degrades to 'unknown', never a guess" ;;
    *) bad "R1 S4 placeholder: got '$out'" ;;
  esac

  # ---- R3 S3: a summary-write failure forces FAIL severity --------------------
  # Correctness components all PASSed, but the run produced no artifact of record,
  # so the signal must say FAIL — a green page for a run with no summary file is the
  # inversion this whole issue exists to prevent.
  out=$(drive_callsite PASS 1 "commit: abc1234 branch: b dirty: no" PASS PASS)
  if [ "${out%%|*}" = FAIL ]; then
    ok "R3 S3: SUMMARY_WRITE_FAILED forces the FAIL severity despite all-PASS components"
  else
    bad "R3 S3: expected FAIL severity, got '$out'"
  fi
  # ...and the failing-component list is still assembled from the real statuses.
  out=$(drive_callsite FAIL 0 "commit: abc1234 branch: b dirty: no" PASS FAIL FAIL)
  if [ "$out" = "FAIL|b|abc1234|c1,c2" ]; then
    ok "R3 S3: the body lists exactly the FAILed components, comma-joined"
  else
    bad "R3 S3 components: expected 'FAIL|b|abc1234|c1,c2', got '$out'"
  fi
fi

# ---- Case 12: structural — the function cannot alter gate state -------------
# rc=0 in every case above is necessary but not sufficient: the function must
# also be incapable of exiting, trapping, or rewriting the artifact of record.
if ! grep -qE '(^|[^_[:alnum:]])exit([^_[:alnum:]]|$)' "$fnfile" \
   && ! grep -q 'trap ' "$fnfile" \
   && ! grep -q 'SUMMARY_FILE' "$fnfile" \
   && grep -q 'return 0' "$fnfile"; then
  ok "structural: gate_push_signal never exits, traps or writes the summary file"
else
  bad "structural: gate_push_signal can alter gate state"; cat "$fnfile"
fi

echo "----------------------------------------"
echo "test_agent_gate_notify: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
