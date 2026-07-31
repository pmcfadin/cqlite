#!/usr/bin/env bash
# Contract test for issue #3119: the full gate's push signal must DELIVER the
# right payload, not merely produce the right argv.
#
# Why this file exists at all. The pre-existing scripts/tests/test_agent_gate_notify.sh
# drove gate_push_signal against a PATH-stubbed `agent-notify` that itself
# implemented a `--category` arm — i.e. the mock encoded the SAME wrong assumption
# as the caller. The real installed `agent-notify` v1.1.0 has no `--category` arm,
# so the flag fell through to manual "$1"="$2" mode: title became the literal
# `--category`, body became the category VALUE, and a FAIL gate published
# priority 3 + a green white_check_mark. Measured, pristine binary, curl-capture:
#   {"topic":"…","title":"--category","message":"error","priority":3,
#    "tags":["white_check_mark"]}   ->  POSTed to https://ntfy.sh/<topic>
# An argv-level assertion can NEVER see any of that. So this test asserts the
# PUBLISHED PAYLOAD, captured at the TRANSPORT boundary.
#
# Intercept surface (this is the load-bearing property — see the spec's
# "The contract test intercepts only the transport" scenario):
#   - REAL   : scripts/agent-gate.sh's gate_push_signal (extracted verbatim)
#   - REAL   : scripts/lib/gate-notify.sh (the payload producer)
#   - STUBBED: `curl` only — a PATH shim that records argv (target URL + -d body)
# Nothing that PRODUCES the payload is stubbed, mocked or re-implemented here.
#
# Hermetic: no network, no real ntfy topic, no dependence on any machine's
# install history. The "pristine upstream agent-notify" fixture is COPIED into
# the tmpdir when /usr/local/bin/agent-notify.bak.20260730 happens to exist, and
# otherwise SYNTHESIZED to the same observable behaviour, so the test is
# identical on a freshly bootstrapped box.
#
# Run standalone:   bash scripts/tests/test_gate_notify_contract.sh
# Or via the gate:  scripts/agent-gate.sh runs it as the `tooling-tests` component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
export REPO_ROOT
GATE="$REPO_ROOT/scripts/agent-gate.sh"
LIB="$REPO_ROOT/scripts/lib/gate-notify.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/gate-notify-contract.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

TOPIC=gate-topic-3119
ROOT=https://ntfy.invalid/
WEBHOOK="https://ntfy.invalid/$TOPIC"

if ! command -v python3 >/dev/null 2>&1; then
  echo "test_gate_notify_contract: SKIP (no python3 — payload parsing needs it)"
  exit 0
fi

# ---------------------------------------------------------------------------
# The payload producers, both REAL.
# ---------------------------------------------------------------------------
if [ ! -r "$LIB" ]; then
  bad "the repo-owned payload producer scripts/lib/gate-notify.sh is missing"
  echo "test_gate_notify_contract: $PASS passed, $FAIL failed"
  exit 1
fi

fnfile="$tmp/gate_push_signal.sh"
awk '/^gate_push_signal\(\) \{/{grab=1} grab{print} grab&&/^\}$/{exit}' "$GATE" > "$fnfile"
if ! grep -q '^gate_push_signal() {' "$fnfile" || ! grep -q '^}$' "$fnfile"; then
  bad "could not extract gate_push_signal() from $GATE"
  echo "test_gate_notify_contract: $PASS passed, $FAIL failed"
  exit 1
fi
# shellcheck disable=SC1090
. "$fnfile"

# ---------------------------------------------------------------------------
# Transport shim: the ONLY stubbed component. Records "<url>\t<body>" per POST.
# ---------------------------------------------------------------------------
shimdir="$tmp/bin"
mkdir -p "$shimdir"
cat > "$shimdir/curl" <<'SHIM'
#!/usr/bin/env bash
# Record the POST target (last arg) and the -d body, one line per invocation.
body=""; url=""; prev=""
for a in "$@"; do
  [ "$prev" = "-d" ] && body="$a"
  prev="$a"; url="$a"
done
printf '%s\t%s\n' "$url" "$body" >> "$CURL_LOG"
SHIM
chmod +x "$shimdir/curl"

# publish <log> <result> <branch> <sha> <fail-components> [extra env assignments...]
#
# HERMETIC BY CONSTRUCTION. Two properties this function must not lose:
#
#  1. It NEUTRALIZES every ambient variable the wrapper honours — the topic
#     overrides (`CQLITE_NOTIFY_TOPIC`/`CODEX_NOTIFY_NTFY_TOPIC` BEAT the
#     URL-derived topic), the alternate webhook var, and the timeout bounds.
#     Measured before this was added: `CODEX_NOTIFY_NTFY_TOPIC=pmcfadin-cloud-100`
#     in the environment reds `AC1 PASS payload` — i.e. `tooling-tests`, i.e. the
#     FULL gate of record — on any box that merely exports a variable this repo's
#     own docs tell it to export.
#  2. It sources ONLY the extracted gate_push_signal and lets the REAL delegation
#     inside it source the wrapper. Sourcing $LIB here would supply the very wiring
#     the test claims to assert: mutation-proven, deleting the `. "$notify_lib"`
#     line from gate_push_signal left this test 12/12 green.
publish() {
  local log="$1" result="$2" branch="$3" sha="$4" fails="$5"; shift 5
  : > "$log"
  env CURL_LOG="$log" PATH="$shimdir:$PATH" \
    CQLITE_NOTIFY_WEBHOOK="$WEBHOOK" CODEX_NOTIFY_WEBHOOK= \
    CQLITE_NOTIFY_TOPIC= CODEX_NOTIFY_NTFY_TOPIC= \
    CQLITE_NOTIFY_DEBUG=0 \
    GATE_NOTIFY_CURL_TIMEOUT=10 GATE_NOTIFY_PAYLOAD_TIMEOUT=10 GATE_NOTIFY_ADJUNCT_TIMEOUT=5 \
    "$@" \
    bash -c '. "$0"; gate_push_signal "$1" "$2" "$3" "$4"' \
    "$fnfile" "$result" "$branch" "$sha" "$fails" >"$tmp/out.txt" 2>"$tmp/err.txt"
  return $?
}

# field <log> <key>: parse the captured JSON body's <key> via python3.
field() {
  python3 - "$1" "$2" <<'PY'
import json, sys
line = open(sys.argv[1]).read().splitlines()
if not line:
    print("<no-payload>"); raise SystemExit(0)
url, _, body = line[0].partition("\t")
if sys.argv[2] == "__url__":
    print(url); raise SystemExit(0)
try:
    d = json.loads(body)
except Exception:
    print("<unparseable:%s>" % body[:60]); raise SystemExit(0)
v = d.get(sys.argv[2], "<missing>")
print(json.dumps(v) if isinstance(v, (list, dict)) else v)
PY
}
lines_of() { wc -l < "$1" | tr -d ' '; }

# message_is_json <log>: 0 when the published message begins with '{' (the
# defect-3 regression). Used both as the guard AND, below, against a planted
# bad payload so the guard can never be vacuous.
message_is_json() {
  case "$(field "$1" message)" in '{'*) return 0 ;; *) return 1 ;; esac
}

# ---- 1. AC1: a PASS gate publishes the PASS title, body and severity ---------
plog="$tmp/pass.log"
publish "$plog" PASS issue-3119-notify-contract abc1234 ""
rc=$?
n=$(lines_of "$plog")
if [ "$rc" -eq 0 ] && [ "$n" -eq 1 ] \
   && [ "$(field "$plog" title)"    = "gate PASS issue-3119-notify-contract@abc1234" ] \
   && [ "$(field "$plog" message)"  = "RESULT: PASS" ] \
   && [ "$(field "$plog" priority)" = "3" ] \
   && [ "$(field "$plog" tags)"     = '["white_check_mark"]' ] \
   && [ "$(field "$plog" topic)"    = "$TOPIC" ]; then
  ok "AC1 PASS: published title/body/severity/topic are correct"
else
  bad "AC1 PASS payload (rc=$rc lines=$n)"; cat "$plog"
fi

# ---- 2. AC1/AC3: the POST target is the SERVER ROOT, topic in the body -------
if [ "$(field "$plog" __url__)" = "$ROOT" ]; then
  ok "AC3 target: POSTed to the ntfy server ROOT, topic carried in the body"
else
  bad "AC3 target: expected $ROOT, got $(field "$plog" __url__)"
fi

# ---- 3. AC2: a FAIL gate publishes a payload distinguishable at a glance ----
flog="$tmp/fail.log"
publish "$flog" FAIL issue-3119-notify-contract deadbee "fmt,clippy"
rc=$?
n=$(lines_of "$flog")
if [ "$rc" -eq 0 ] && [ "$n" -eq 1 ] \
   && [ "$(field "$flog" title)"    = "gate FAIL issue-3119-notify-contract@deadbee" ] \
   && [ "$(field "$flog" message)"  = "RESULT: FAIL — failing: fmt,clippy" ] \
   && [ "$(field "$flog" priority)" = "5" ] \
   && [ "$(field "$flog" tags)"     = '["rotating_light"]' ]; then
  ok "AC2 FAIL: published title, failing components and FAIL severity"
else
  bad "AC2 FAIL payload (rc=$rc lines=$n)"; cat "$flog"
fi

# ---- 4. AC2: a red gate can NEVER page as a routine success ------------------
if [ "$(field "$flog" priority)" != "$(field "$plog" priority)" ] \
   && [ "$(field "$flog" tags)" != "$(field "$plog" tags)" ] \
   && [ "$(field "$flog" priority)" != "3" ] \
   && [ "$(field "$flog" tags)" != '["white_check_mark"]' ]; then
  ok "AC2 distinctness: FAIL differs from PASS in BOTH priority and tags"
else
  bad "AC2 distinctness: FAIL payload is not distinguishable from PASS"
fi

# ---- 5. AC3: the message is body text, never a JSON document ----------------
if ! message_is_json "$plog" && ! message_is_json "$flog" \
   && ! grep -q '"topic"' <<<"$(field "$plog" message)"; then
  ok "AC3 message: body text, no JSON braces, no topic key"
else
  bad "AC3 message: a published message is a serialized document"
fi

# ---- 6. AC3: the guard itself is live (planted raw-JSON message must FAIL) ---
badlog="$tmp/planted.log"
printf '%s\t%s\n' "$ROOT" '{"message": "{\"topic\": \"x\", \"message\": \"y\"}"}' > "$badlog"
if message_is_json "$badlog"; then
  ok "AC3 guard is live: a planted message beginning with '{' is detected"
else
  bad "AC3 guard is VACUOUS: a raw-JSON message was not detected"
fi

# ---- 7. AC6: the contract holds with the PRISTINE upstream binary on PATH ----
# Prefer a copy of the real pristine v1.1.0; otherwise synthesize its observable
# behaviour (no --category arm -> manual "$1"/"$2" mode -> completion severity ->
# JSON POSTed to the TOPIC url). Either way the fixture lives in the tmpdir.
# DETERMINISM (review R4): the fixture is the REPO-OWNED stand-in below by
# DEFAULT, always — never a machine-specific /usr/local/bin backup, whose presence
# would make the gate's outcome depend on external host state. A real pristine
# binary can be substituted deliberately for a local cross-check by exporting
# CQLITE_NOTIFY_PRISTINE_BIN=/path/to/agent-notify-v1.1.0; that path is never
# probed for implicitly.
pristine_kind=repo-owned-stand-in
if [ -n "${CQLITE_NOTIFY_PRISTINE_BIN:-}" ] && [ -r "${CQLITE_NOTIFY_PRISTINE_BIN}" ]; then
  cp "$CQLITE_NOTIFY_PRISTINE_BIN" "$shimdir/agent-notify" && pristine_kind=real-opt-in
fi
if [ "$pristine_kind" = repo-owned-stand-in ]; then
  cat > "$shimdir/agent-notify" <<'PRISTINE'
#!/usr/bin/env bash
# Stand-in reproducing pristine agent-notify v1.1.0's OBSERVABLE behaviour:
# no --category arm (falls through to manual mode), severity pinned to
# completion, and a JSON body POSTed to the TOPIC url (not the server root).
set -uo pipefail
title="${1:-Codex}"; msg="${2:-Task finished}"
url="${CODEX_NOTIFY_WEBHOOK:-}"
if [ -n "$url" ]; then
  topic="${url##*/}"
  curl -s -X POST -H "Content-Type: application/json" \
    -d "{\"topic\": \"$topic\", \"title\": \"$title\", \"message\": \"$msg\", \"priority\": 3, \"tags\": [\"white_check_mark\"]}" \
    "$url" >/dev/null 2>&1 || true
fi
exit 0
PRISTINE
fi
chmod +x "$shimdir/agent-notify"

# The AC6 cases deliberately supply an AMBIENT CODEX_NOTIFY_WEBHOOK (review B4).
# Probed: the pristine binary publishes ONLY when CODEX_NOTIFY_WEBHOOK is set and
# ignores CQLITE_NOTIFY_WEBHOOK entirely (1 curl vs 0). Without this the
# "exactly ONE payload" assertion is VACUOUS on precisely the machines the header
# calls identical — a CI runner or a freshly bootstrapped box with no ambient
# webhook — and would pass whether or not the wrapper's env-neutralization exists.
# With it set, a regression in that neutralization publishes a SECOND payload and
# these cases red.
ADJUNCT_AMBIENT_WEBHOOK="https://ntfy.invalid/adjunct-would-double-publish"
for case_pair in "PASS:3:white_check_mark" "FAIL:5:rotating_light"; do
  res=${case_pair%%:*}; rest=${case_pair#*:}; prio=${rest%%:*}; tag=${rest#*:}
  alog="$tmp/adjunct-$res.log"
  publish "$alog" "$res" issue-3119-notify-contract abc1234 "" \
    CODEX_NOTIFY_WEBHOOK="$ADJUNCT_AMBIENT_WEBHOOK"
  rc=$?
  n=$(lines_of "$alog")
  if [ "$rc" -eq 0 ] && [ "$n" -eq 1 ] \
     && [ "$(field "$alog" title)"    = "gate $res issue-3119-notify-contract@abc1234" ] \
     && [ "$(field "$alog" priority)" = "$prio" ] \
     && [ "$(field "$alog" tags)"     = "[\"$tag\"]" ] \
     && [ "$(field "$alog" __url__)"  = "$ROOT" ] \
     && ! message_is_json "$alog" \
     && ! grep -q 'adjunct-would-double-publish' "$alog"; then
    ok "AC6 $res, pristine agent-notify on PATH ($pristine_kind) + ambient webhook: ONE correct payload, adjunct published nothing"
  else
    bad "AC6 $res with pristine agent-notify on PATH ($pristine_kind) (rc=$rc lines=$n)"; cat "$alog"
  fi
done

# ---- 7b. a query-credentialed target DELIVERS with its query intact -----------
# `https://host/topic?auth=<token>` is a documented ntfy credential shape. Review
# round 2 added a query-strip to keep credentials out of LOGS but applied it to the
# single value that was ALSO the POST target, so such a target published
# UNAUTHENTICATED — and silently, because publish failure is a deliberate no-op. The
# delivery URL and the printable/redacted string are now separate values; this case
# pins the delivery half so they can never be re-collapsed.
qlog="$tmp/query.log"
: > "$qlog"
env CURL_LOG="$qlog" PATH="$shimdir:$PATH" \
  CQLITE_NOTIFY_WEBHOOK="https://ntfy.invalid/$TOPIC?auth=tk_AbC123" CODEX_NOTIFY_WEBHOOK= \
  CQLITE_NOTIFY_TOPIC= CODEX_NOTIFY_NTFY_TOPIC= \
  GATE_NOTIFY_CURL_TIMEOUT=10 GATE_NOTIFY_PAYLOAD_TIMEOUT=10 GATE_NOTIFY_ADJUNCT_TIMEOUT=5 \
  bash -c '. "$0"; gate_push_signal PASS issue-3119-notify-contract abc1234 ""' \
  "$fnfile" >"$tmp/out.txt" 2>"$tmp/err.txt"
rc=$?
qurl=$(field "$qlog" __url__)
if [ "$rc" -eq 0 ] && [ "$(lines_of "$qlog")" -eq 1 ] \
   && [ "$qurl" = "https://ntfy.invalid/?auth=tk_AbC123" ] \
   && [ "$(field "$qlog" topic)" = "$TOPIC" ] \
   && [ "$(field "$qlog" title)" = "gate PASS issue-3119-notify-contract@abc1234" ]; then
  ok "query-credentialed target: POSTed to the server root WITH the query preserved, topic parsed from the path"
else
  bad "query-credentialed target: delivery URL was '$qurl' (expected the query preserved)"; cat "$qlog"
fi

# ---- 7c. ...while the PRINTABLE form of that same target carries no credential --
# The redaction property from round 2, asserted directly on the function whose output
# is the only one allowed to be echoed. A regression that starts printing the delivery
# URL — or stops redacting — reds here.
# `_ "$LIB"` (not `"$LIB"` as $0): when $0 equals the sourced path the wrapper's
# direct-execution block fires and prints usage instead of exposing its functions.
redact_probe=$(bash -c '. "$1"; for u in \
    "https://ntfy.invalid/t?token=s3cr3t" \
    "https://ntfy.invalid/t#frag-s3cr3t" \
    "https://alice:s3cr3t@ntfy.invalid/t?auth=tk_s3cr3t"; do
      _gate_notify_redact "$u"; done' _ "$LIB" 2>&1)
if ! printf '%s' "$redact_probe" | grep -qE 's3cr3t|alice:|token=|auth=' \
   && [ "$(printf '%s\n' "$redact_probe" | grep -c 'https://ntfy.invalid/')" -eq 3 ]; then
  ok "printable target form: query, fragment and userinfo all redacted (no credential can reach a log)"
else
  bad "printable target form leaked a credential: $redact_probe"
fi

# ---- 8. intercept surface: only the transport is substituted -----------------
substituted=$(cd "$shimdir" && ls | sort | tr '\n' ' ')
if [ "$substituted" = "agent-notify curl " ] \
   && [ ! -e "$tmp/gate-notify.sh" ] \
   && grep -q 'GATE_NOTIFY_CONTRACT_VERSION' "$LIB" \
   && grep -q 'gate_notify_publish' "$fnfile"; then
  ok "intercept surface: payload producers are the repo's own; only curl (+ the pristine fixture) is substituted"
else
  bad "intercept surface: unexpected substitutions [$substituted]"
fi

# ---- 9. hermetic: the resolved curl IS the shim, so no network is used -------
resolved=$(env PATH="$shimdir:$PATH" bash -c 'command -v curl')
if [ "$resolved" = "$shimdir/curl" ]; then
  ok "hermetic: curl resolves to the capture shim — no network, no real topic"
else
  bad "hermetic: curl resolved to $resolved, not the shim"
fi

# ---- 10. the publish path is silent on stdout and stderr --------------------
if [ ! -s "$tmp/out.txt" ] && [ ! -s "$tmp/err.txt" ]; then
  ok "publish path writes nothing to stdout or stderr"
else
  bad "publish path emitted output"; head -5 "$tmp/out.txt" "$tmp/err.txt"
fi

# ---- 11. AC6 doctrine: no repo surface prescribes the swallowed flag --------
# Mechanized so a reintroduction FAILs the gate instead of silently corrupting
# every fleet page again. Scope is the EXECUTABLE/PRESCRIPTIVE surfaces — scripts,
# agent skills/commands, workflows — because that is where the shape can actually
# be invoked; prose that DOCUMENTS the historical defect (openspec artifacts, the
# guide, archived reports) is not an offence. A line that must quote the shape
# explanatorily marks itself `notify-flag-allow`, mirroring the repo's existing
# `injection-lint-allow` idiom. The needle is ASSEMBLED so this file's own prose
# can never be the thing it flags.
needle="agent-notify"' --category'
offenders=$(cd "$REPO_ROOT" && grep -rIn --exclude-dir=.git --exclude-dir=target \
  -e "$needle" scripts .claude .github 2>/dev/null \
  | grep -v 'notify-flag-allow' \
  | grep -v '^scripts/tests/test_gate_notify_contract.sh:' || true)
if [ -z "$offenders" ]; then
  ok "AC6 doctrine: no script, skill or workflow invokes the swallowed-flag shape"
else
  bad "AC6 doctrine: the swallowed-flag shape survives in:"; printf '%s\n' "$offenders"
fi

echo "----------------------------------------"
echo "test_gate_notify_contract: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
