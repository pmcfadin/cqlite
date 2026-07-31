#!/usr/bin/env bash
# shellcheck shell=bash
# gate-notify.sh — the repo-owned notification contract (issue #3119).
#
# WHY THIS FILE OWNS THE PAYLOAD. The gate used to call
# `agent-notify --category <cat> <title> <body>`. The installed upstream  notify-flag-allow
# `agent-notify` v1.1.0 has NO `--category` arm, so that call fell through to its
# manual "$1"/"$2" mode: the title became the literal string `--category`, the
# body became the category VALUE, and the real title/body were dropped as surplus
# positional args. Worse, manual mode pins its severity to `completion`, so a
# FAIL gate published ntfy priority 3 with a green white_check_mark — a red gate
# paging as a routine success. And its ntfy branch POSTs the JSON body to the
# TOPIC url instead of the server ROOT, so ntfy renders the serialized JSON as
# the message text. Measured against the pristine binary with a curl-capture:
#   {"topic":"…","title":"--category","message":"error","priority":3,
#    "tags":["white_check_mark"]}  ->  POSTed to https://ntfy.sh/<topic>
# Two of those three defects live INSIDE that binary, unreachable from any
# caller-side flag probe. So the payload is built HERE, in the repo, git-pinned
# and gate-tested; `agent-notify` is demoted to an OPTIONAL local desktop/sound
# adjunct that is never in the payload chain.
#
# ADVISORY BY CONTRACT (load-bearing). Nothing in this file may change a caller's
# verdict or exit status: every external call is time-bounded, its output is
# discarded and its failure swallowed; no function here ever `exit`s, installs a
# trap, or writes to any caller state. `gate_notify_publish` always returns 0.
#
# Configuration (the fleet already sets the second form in /etc/environment):
#   CQLITE_NOTIFY_WEBHOOK / CODEX_NOTIFY_WEBHOOK   ntfy target (topic URL or root)
#   CQLITE_NOTIFY_TOPIC   / CODEX_NOTIFY_NTFY_TOPIC  explicit topic override
#   CQLITE_NOTIFY_DEBUG=1                          diagnostics (never changes behaviour)
#
# Self-test:  bash scripts/lib/gate-notify.sh --self-test
# Contract test: scripts/tests/test_gate_notify_contract.sh (gate: tooling-tests)

# Contract version — bumped when the published payload shape changes. Bootstrap
# records this as the pinned notify artifact (it travels in git, not in an
# out-of-band per-machine binary).
GATE_NOTIFY_CONTRACT_VERSION=1

# Bounds. A black-holed network or a wedged helper must never stall a gate.
GATE_NOTIFY_CURL_TIMEOUT="${GATE_NOTIFY_CURL_TIMEOUT:-10}"
GATE_NOTIFY_ADJUNCT_TIMEOUT="${GATE_NOTIFY_ADJUNCT_TIMEOUT:-5}"

_gate_notify_debug() {
  [ "${CQLITE_NOTIFY_DEBUG:-0}" = 1 ] || return 0
  printf 'gate-notify: %s\n' "$*" >&2
  return 0
}

# _gate_notify_severity <result>: normalise PASS/FAIL (or completion/error) to
# the two severities the contract defines. Anything unrecognised is FAIL — the
# safe direction for a signal whose job is to surface breakage.
_gate_notify_severity() {
  case "${1:-}" in
    PASS | pass | completion) printf 'PASS\n' ;;
    *) printf 'FAIL\n' ;;
  esac
}

# The severity map, pinned. Deliberately the same priority/tag pairs upstream's
# ntfy preset uses, so a CORRECT notification looks exactly like the fleet
# expects — the change fixes fidelity, not appearance.
_gate_notify_priority() { case "$1" in PASS) printf '3\n' ;; *) printf '5\n' ;; esac; }
_gate_notify_tag() { case "$1" in PASS) printf 'white_check_mark\n' ;; *) printf 'rotating_light\n' ;; esac; }

# _gate_notify_target: print "<server-root><TAB><topic>" for the configured
# target, or return 1 when either cannot be resolved AUTHORITATIVELY.
#
# ntfy JSON publishing requires POSTing to the SERVER ROOT with the topic in the
# body; POSTed to /<topic> the body is taken as literal message text. The fleet
# configures a TOPIC URL, so the split happens here.
#
# A target from which no topic can be resolved returns 1 rather than guessing:
# publishing to a guessed topic pages a stranger.
_gate_notify_target() {
  local url="${CQLITE_NOTIFY_WEBHOOK:-${CODEX_NOTIFY_WEBHOOK:-}}"
  local topic="${CQLITE_NOTIFY_TOPIC:-${CODEX_NOTIFY_NTFY_TOPIC:-}}"
  [ -n "$url" ] || { _gate_notify_debug "no notify target configured"; return 1; }
  local u="${url%%\?*}"          # drop any query string
  u="${u%%#*}"                   # drop any fragment
  while [ "${u: -1}" = "/" ]; do u="${u%/}"; done
  local root
  if printf '%s' "$u" | grep -Eq '^[A-Za-z][A-Za-z0-9+.-]*://[^/]+$'; then
    # Already a bare server root: the topic can only come from the override.
    root="$u/"
  else
    root="${u%/*}/"
    [ -n "$topic" ] || topic="${u##*/}"
  fi
  [ -n "$topic" ] || { _gate_notify_debug "no topic resolvable from '$url'"; return 1; }
  printf '%s\t%s\n' "$root" "$topic"
}

# _gate_notify_payload <topic> <title> <body> <priority> <tag>: emit the ntfy JSON.
# Built with python3's json.dumps — never hand-quoted shell interpolation, so a
# title/body carrying quotes, backslashes or non-ASCII (the body's em dash) can
# never produce a malformed document.
_gate_notify_payload() {
  command -v python3 >/dev/null 2>&1 || return 1
  python3 -c '
import json, sys
topic, title, message, priority, tag = sys.argv[1:6]
print(json.dumps({
    "topic": topic,
    "title": title,
    "message": message,
    "priority": int(priority),
    "tags": [tag],
}))' "$1" "$2" "$3" "$4" "$5" 2>/dev/null
}

# _gate_notify_bounded_timeout: print a timeout command prefix, or return 1 when
# no bounded runner exists. No bound means we do not run the adjunct at all —
# an unbounded helper could stall a gate, and the adjunct is optional by design.
_gate_notify_bounded_timeout() {
  if command -v timeout >/dev/null 2>&1; then printf 'timeout\n'; return 0; fi
  if command -v gtimeout >/dev/null 2>&1; then printf 'gtimeout\n'; return 0; fi
  return 1
}

# _gate_notify_adjunct <title> <body>: OPTIONAL local desktop/sound notification.
# Three properties make this safe against the defects above:
#   1. POSITIONAL arguments only — never `--category`, the flag upstream swallows.
#   2. Its webhook environment is NEUTRALIZED, so its own (broken) publish path
#      cannot deliver a second, JSON-as-message notification.
#   3. Time-bounded, output discarded, failure swallowed.
_gate_notify_adjunct() {
  command -v agent-notify >/dev/null 2>&1 || return 0
  local to
  to=$(_gate_notify_bounded_timeout) || { _gate_notify_debug "no timeout(1); skipping adjunct"; return 0; }
  env CODEX_NOTIFY_WEBHOOK= CQLITE_NOTIFY_WEBHOOK= CODEX_NOTIFY_WEBHOOK_PRESET= \
    "$to" "$GATE_NOTIFY_ADJUNCT_TIMEOUT" agent-notify "$1" "$2" >/dev/null 2>&1 </dev/null || true
  return 0
}

# gate_notify_publish <result> <title> <body>
#
# Publish ONE notification for <result> (PASS/FAIL). ALWAYS returns 0 — every
# failure mode (no target, no python3, no curl, a publish that never completes,
# an adjunct that rejects its arguments or is not executable) is a silent no-op.
gate_notify_publish() {
  local result="${1:-FAIL}" title="${2:-}" body="${3:-}"
  local severity root topic payload
  severity=$(_gate_notify_severity "$result")

  local target
  if target=$(_gate_notify_target); then
    root="${target%%	*}"
    topic="${target##*	}"
    if command -v curl >/dev/null 2>&1; then
      payload=$(_gate_notify_payload "$topic" "$title" "$body" \
        "$(_gate_notify_priority "$severity")" "$(_gate_notify_tag "$severity")") || payload=""
      if [ -n "$payload" ]; then
        _gate_notify_debug "POST $root $payload"
        curl -sS -X POST -H 'Content-Type: application/json' \
          --max-time "$GATE_NOTIFY_CURL_TIMEOUT" \
          -d "$payload" "$root" >/dev/null 2>&1 </dev/null || true
      else
        _gate_notify_debug "payload build failed (python3 missing or errored)"
      fi
    else
      _gate_notify_debug "curl absent; nothing published"
    fi
  fi

  _gate_notify_adjunct "$title" "$body"
  return 0
}

# --self-test: publish a PASS and a FAIL through a private curl capture shim and
# validate BOTH against the contract. This is the CAPABILITY assertion
# bootstrap runs — the analogue of the mold link probe: nothing is declared
# healthy because a file exists. Exits non-zero ONLY in this mode.
gate_notify_selftest() {
  local tmp rc=0
  tmp=$(mktemp -d "${TMPDIR:-/tmp}/gate-notify-selftest.XXXXXX") || return 1
  mkdir -p "$tmp/bin"
  cat > "$tmp/bin/curl" <<'SHIM'
#!/usr/bin/env bash
body=""; url=""; prev=""
for a in "$@"; do
  [ "$prev" = "-d" ] && body="$a"
  prev="$a"; url="$a"
done
printf '%s\t%s\n' "$url" "$body" >> "$CURL_LOG"
SHIM
  chmod +x "$tmp/bin/curl"

  local self="${BASH_SOURCE[0]}"
  local severity expect_prio expect_tag log
  for severity in PASS FAIL; do
    case "$severity" in
      PASS) expect_prio=3; expect_tag=white_check_mark ;;
      *) expect_prio=5; expect_tag=rotating_light ;;
    esac
    log="$tmp/$severity.log"
    : > "$log"
    env CURL_LOG="$log" PATH="$tmp/bin:$PATH" \
      CQLITE_NOTIFY_WEBHOOK="${CQLITE_NOTIFY_WEBHOOK:-${CODEX_NOTIFY_WEBHOOK:-https://ntfy.invalid/selftest}}" \
      bash -c '. "$1"; gate_notify_publish "$2" "gate '"$severity"' selftest@0000000" "RESULT: '"$severity"'"' \
      _ "$self" "$severity" >/dev/null 2>&1
    if ! python3 - "$log" "$expect_prio" "$expect_tag" <<'PY'
import json, sys
lines = open(sys.argv[1]).read().splitlines()
if len(lines) != 1:
    print("expected exactly 1 published payload, got %d" % len(lines)); raise SystemExit(1)
url, _, body = lines[0].partition("\t")
try:
    d = json.loads(body)
except Exception as e:
    print("payload is not JSON: %s" % e); raise SystemExit(1)
if not url.endswith("/") or url.rstrip("/").count("/") != 2:
    print("POST target is not a server root: %s" % url); raise SystemExit(1)
if str(d.get("priority")) != sys.argv[2] or d.get("tags") != [sys.argv[3]]:
    print("severity mismatch: %s / %s" % (d.get("priority"), d.get("tags"))); raise SystemExit(1)
msg = d.get("message", "")
if not msg or msg.lstrip().startswith("{"):
    print("message is empty or a serialized document: %r" % msg); raise SystemExit(1)
if not d.get("title", "").startswith("gate "):
    print("title is not the gate's own: %r" % d.get("title")); raise SystemExit(1)
PY
    then
      echo "gate-notify --self-test: $severity payload FAILED the contract" >&2
      rc=1
    fi
  done
  rm -rf "$tmp"
  [ "$rc" -eq 0 ] && echo "gate-notify --self-test: PASS (contract v$GATE_NOTIFY_CONTRACT_VERSION)"
  return "$rc"
}

# Direct execution: --publish / --self-test / --version. Sourced use never reaches
# here. `--publish` is the CLI form for callers that hold a command STRING rather
# than a sourced function (scripts/local/worker-supervisor.sh's $NOTIFY_CMD seam);
# it keeps the advisory contract and always exits 0.
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
  case "${1:-}" in
    --publish)
      shift
      gate_notify_publish "${1:-FAIL}" "${2:-}" "${3:-}" >/dev/null 2>&1 || true
      exit 0
      ;;
    --self-test) gate_notify_selftest ;;
    --version) echo "gate-notify contract v$GATE_NOTIFY_CONTRACT_VERSION" ;;
    *)
      echo "usage: gate-notify.sh --publish <PASS|FAIL> <title> <body> | --self-test | --version" >&2
      echo "       (or source it and call gate_notify_publish)" >&2
      exit 2
      ;;
  esac
fi
