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
  env CURL_LOG="$log" CQLITE_NOTIFY_WEBHOOK="$WEBHOOK" PATH="$bindir:$PATH" "$@" \
    bash -c '. "$0"; gate_push_signal "$1" advisory-branch abc1234 ""' \
    "$fnfile" "$result" >"$tmp/out.txt" 2>"$tmp/err.txt"
  return $?
}
silent() { [ ! -s "$tmp/out.txt" ] && [ ! -s "$tmp/err.txt" ]; }
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
# Ceiling is deliberately loose (2s bound + generous slack) so CPU contention
# cannot flake it; the property under test is "bounded at all", not a latency SLO.
if [ "$rc" -eq 0 ] && [ "$elapsed" -lt 30 ] && silent; then
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
