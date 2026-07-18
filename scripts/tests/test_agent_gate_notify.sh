#!/usr/bin/env bash
# Regression test for issue #2667: the full agent-gate must fire ONE advisory
# `agent-notify` push at final-SUMMARY time, converting the summary file from a
# passive poll target into a PUSH signal for a waiting closer/worker. Contract:
#   - title: "gate <RESULT> <branch>@<short-sha>"
#   - body:  "RESULT: <RESULT>" (+ "— failing: c1,c2" when components FAILed)
#   - category: completion on PASS, error on FAIL
#   - ADVISORY: if agent-notify is absent OR fails, gate_push_signal is a silent
#     no-op that returns 0 — it never affects the gate verdict/exit.
#
# Hermetic + fast by design: it does NOT run the 5-8 min real gate. It extracts
# the self-contained gate_push_signal() function from agent-gate.sh, sources just
# that, and drives it against a PATH-stubbed agent-notify that records its argv.
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

# A stub agent-notify that records "<category>\t<title>\t<body>" to $NOTIFY_LOG.
stubdir="$tmp/bin"
mkdir -p "$stubdir"
cat > "$stubdir/agent-notify" <<'STUB'
#!/usr/bin/env bash
# --category C "Title" "Message"  ->  record C<TAB>Title<TAB>Message
cat=""
if [ "$1" = "--category" ]; then cat="$2"; shift 2; fi
printf '%s\t%s\t%s\n' "$cat" "${1:-}" "${2:-}" >> "$NOTIFY_LOG"
STUB
chmod +x "$stubdir/agent-notify"

# ---- Case 1: PASS fires a completion push with the expected title/body --------
NOTIFY_LOG="$tmp/case1.log"; : > "$NOTIFY_LOG"; export NOTIFY_LOG
PATH="$stubdir:$PATH" gate_push_signal PASS issue-2667-poll-to-push abc1234 ""
rc=$?
n=$(wc -l < "$NOTIFY_LOG" | tr -d ' ')
if [ "$rc" -eq 0 ] && [ "$n" -eq 1 ] \
   && grep -qP '^completion\tgate PASS issue-2667-poll-to-push@abc1234\tRESULT: PASS$' "$NOTIFY_LOG" 2>/dev/null; then
  ok "PASS: exactly ONE completion push, correct title + body"
elif [ "$rc" -eq 0 ] && [ "$n" -eq 1 ] \
   && awk -F'\t' 'NR==1 && $1=="completion" && $2=="gate PASS issue-2667-poll-to-push@abc1234" && $3=="RESULT: PASS"{found=1} END{exit found?0:1}' "$NOTIFY_LOG"; then
  # Fallback for greps without -P (BSD grep): assert via awk field split.
  ok "PASS: exactly ONE completion push, correct title + body"
else
  bad "PASS case (rc=$rc lines=$n)"; echo "--- log ---"; cat "$NOTIFY_LOG"; echo "-----------"
fi

# ---- Case 2: FAIL fires an error push and lists failing components ------------
NOTIFY_LOG="$tmp/case2.log"; : > "$NOTIFY_LOG"; export NOTIFY_LOG
PATH="$stubdir:$PATH" gate_push_signal FAIL issue-2667-poll-to-push deadbee "fmt,clippy"
rc=$?
n=$(wc -l < "$NOTIFY_LOG" | tr -d ' ')
if [ "$rc" -eq 0 ] && [ "$n" -eq 1 ] \
   && awk -F'\t' 'NR==1 && $1=="error" && $2=="gate FAIL issue-2667-poll-to-push@deadbee" && $3=="RESULT: FAIL — failing: fmt,clippy"{found=1} END{exit found?0:1}' "$NOTIFY_LOG"; then
  ok "FAIL: ONE error push, title + failing components in body"
else
  bad "FAIL case (rc=$rc lines=$n)"; echo "--- log ---"; cat "$NOTIFY_LOG"; echo "-----------"
fi

# ---- Case 3: agent-notify absent -> silent no-op, still returns 0 -------------
# Empty PATH means `command -v agent-notify` fails; the function must no-op.
NOTIFY_LOG="$tmp/case3.log"; : > "$NOTIFY_LOG"; export NOTIFY_LOG
PATH="/nonexistent-dir-2667" gate_push_signal PASS somebranch cafef00d "" 2>/dev/null
rc=$?
n=$(wc -l < "$NOTIFY_LOG" | tr -d ' ')
if [ "$rc" -eq 0 ] && [ "$n" -eq 0 ]; then
  ok "absent agent-notify: silent no-op, returns 0"
else
  bad "absent case (rc=$rc lines=$n — expected rc=0, no push)"
fi

echo "----------------------------------------"
echo "test_agent_gate_notify: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
