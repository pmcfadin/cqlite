#!/usr/bin/env bash
# shellcheck shell=bash
# gate-notify.sh — the repo-owned notification contract (issue #3119).
#
# WHY THIS FILE OWNS THE PAYLOAD. The gate used to call
# `agent-notify --category <cat> <title> <body>`. The installed upstream notify-flag-allow
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
# verdict or exit status: EVERY external call — the payload encoder, the publish,
# and the optional adjunct — runs under `timeout`, its output is discarded and its
# failure swallowed; no function here ever `exit`s, installs a trap, or writes to
# any caller state. `gate_notify_publish` always returns 0.
#
# Why the encoder needs a bound too, not just curl (#3119 review B2): the gate
# calls this AFTER the terminal summary emit but BEFORE its own exit, so a wedged
# `python3` (a pyenv/conda shim, an NFS-backed interpreter) would block
# `payload=$(...)` forever — the gate process would never exit, its EXIT trap would
# never release the #1825 gate slot, and every later gate on the box would queue
# indefinitely. `curl --max-time` is NOT sufficient either: only the real curl
# honours it (this repo's own tests prove a bash script can be `curl` on PATH), so
# the outer bound is what actually guarantees termination. With NO bounding tool
# available (`timeout`/`gtimeout`) we publish NOTHING rather than run unbounded —
# a missed notification is advisory, a wedged gate is not.
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

# Bounds. A black-holed network or a wedged helper must never stall a gate. Each is
# an OUTER bound enforced with SIGTERM **and then SIGKILL**, so it does not depend on
# the helper cooperating.
#
# Why the SIGKILL escalation is required, not belt-and-braces: plain
# `timeout <secs> <cmd>` sends SIGTERM ONLY. Measured — a helper containing
# `trap "" TERM` under `timeout 2` NEVER RETURNED (still alive when a 20s harness
# SIGKILLed it), so the bound bought nothing at all. `--kill-after=<grace>` follows up
# with SIGKILL, which cannot be trapped: the same helper then returned rc=137 at 3s
# (bound 2 + grace 1). `agent-notify` is THIRD-PARTY upstream and mis-assuming its
# behaviour is the original defect of this whole issue, so its signal handling is not
# something to assume either.
#
# GRACE IS ADDITIVE WALL-CLOCK, never a substitute for the bound: a step's true worst
# case is `bound + GATE_NOTIFY_KILL_GRACE`. Every caller with a latency ceiling must
# count the grace once PER STEP (scripts/local/worker-supervisor.sh's exit path does,
# and scripts/tests/test_agent_gate_notify.sh asserts that arithmetic).
#
# A `timeout` that does not support `--kill-after` is treated as NO bounding tool at
# all — we publish nothing rather than run behind an escapable bound.
GATE_NOTIFY_CURL_TIMEOUT="${GATE_NOTIFY_CURL_TIMEOUT:-10}"
GATE_NOTIFY_ADJUNCT_TIMEOUT="${GATE_NOTIFY_ADJUNCT_TIMEOUT:-5}"
GATE_NOTIFY_PAYLOAD_TIMEOUT="${GATE_NOTIFY_PAYLOAD_TIMEOUT:-10}"
GATE_NOTIFY_KILL_GRACE="${GATE_NOTIFY_KILL_GRACE:-1}"

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

# _gate_notify_redact <url>: the PRINTABLE form of a target — scheme://host/ with the
# query, the fragment and any URL userinfo removed. Three credential shapes reach a
# terminal or a CI log otherwise: `?token=`, `?auth=` and `user:pass@`. This is the
# ONLY function whose output may be echoed; see _gate_notify_target's contract.
_gate_notify_redact() {
  local in="${1:-}"
  in="${in%%\?*}"
  in="${in%%#*}"
  case "$in" in
    *://*)
      local scheme="${in%%://*}" rest="${in#*://}"
      local hostpart="${rest%%/*}"
      case "$hostpart" in *@*) hostpart="${hostpart##*@}" ;; esac
      printf '%s://%s/\n' "$scheme" "$hostpart"
      ;;
    *) printf '(unparseable target; not echoed)\n' ;;
  esac
}

# _gate_notify_target: resolve the configured target into THREE values with THREE
# distinct purposes, or return 1 when the topic cannot be resolved AUTHORITATIVELY.
# (Named variables rather than a delimited string on stdout: a whitespace delimiter
# in shell source is silently destroyed by a reformat.) Its ONLY caller declares all
# three names `local` first, so bash's dynamic scoping keeps these assignments inside
# the caller's frame — nothing is written into the gate's shell.
#
#   GATE_NOTIFY_DELIVERY_URL  what CURL POSTS TO. **Query string PRESERVED**, because
#                             `?auth=<token>` is a documented ntfy credential shape and
#                             dropping it publishes UNAUTHENTICATED — and since publish
#                             failure is a deliberate no-op, it fails SILENTLY. This
#                             value MUST NEVER be logged or echoed.
#   GATE_NOTIFY_TOPIC         the body's `topic`. Parsed from the PATH only, so the
#                             query/fragment can never end up inside the topic name.
#   GATE_NOTIFY_SAFE_TARGET   the ONLY value safe to print. Query, fragment AND URL
#                             userinfo removed, so no `?token=`/`?auth=`/`user:pass@`
#                             credential can reach a terminal, a CI log or the debug
#                             stream.
#
# DO NOT RE-COLLAPSE THESE. Round 2 of this PR's review added the query-strip to keep
# credentials out of logs, but applied it to the single value that was ALSO the POST
# target — so a valid `?auth=` target silently published unauthenticated. The names
# above are deliberately explicit about which one curl receives; a logging change must
# only ever touch GATE_NOTIFY_SAFE_TARGET.
#
# ntfy JSON publishing requires POSTing to the SERVER ROOT with the topic in the body;
# POSTed to /<topic> the body is taken as literal message text. The fleet configures a
# TOPIC URL, so the split happens here.
#
# A target from which no topic can be resolved returns 1 rather than guessing:
# publishing to a guessed topic pages a stranger.
_gate_notify_target() {
  GATE_NOTIFY_DELIVERY_URL=""
  GATE_NOTIFY_TOPIC=""
  GATE_NOTIFY_SAFE_TARGET=""
  local url="${CQLITE_NOTIFY_WEBHOOK:-${CODEX_NOTIFY_WEBHOOK:-}}"
  local topic="${CQLITE_NOTIFY_TOPIC:-${CODEX_NOTIFY_NTFY_TOPIC:-}}"
  [ -n "$url" ] || { _gate_notify_debug "no notify target configured"; return 1; }
  # A fragment is never transmitted by HTTP, so it is dropped from everything.
  local nofrag="${url%%#*}"
  # The query is SPLIT OFF (not discarded) so it can be re-attached to the delivery
  # URL after the topic is parsed out of the path.
  local query=""
  case "$nofrag" in *\?*) query="?${nofrag#*\?}" ;; esac
  local u="${nofrag%%\?*}"
  while [ "${u: -1}" = "/" ]; do u="${u%/}"; done
  local root
  # Native regex, NOT `printf | grep -q`: under the caller's `set -o pipefail`
  # (agent-gate.sh) a pipeline whose reader exits early makes printf die on EPIPE
  # and the pipeline return 141, which would MIS-BRANCH into the has-a-path arm.
  if [[ "$u" =~ ^[A-Za-z][A-Za-z0-9+.-]*://[^/]+$ ]]; then
    # Already a bare server root: the topic can only come from the override.
    root="$u/"
  else
    root="${u%/*}/"
    [ -n "$topic" ] || topic="${u##*/}"
  fi
  [ -n "$topic" ] || { _gate_notify_debug "no topic resolvable from the configured target"; return 1; }
  GATE_NOTIFY_DELIVERY_URL="${root}${query}"
  GATE_NOTIFY_TOPIC="$topic"
  GATE_NOTIFY_SAFE_TARGET=$(_gate_notify_redact "$root")
  return 0
}

# _gate_notify_payload <timeout-cmd> <topic> <title> <body> <priority> <tag>: emit
# the ntfy JSON. Built with python3's json.dumps — never hand-quoted shell
# interpolation, so a title/body carrying quotes, backslashes or non-ASCII (the
# body's em dash) can never produce a malformed document. Runs under the caller's
# OUTER timeout: a wedged interpreter must not block the gate's exit (#3119 B2).
_gate_notify_payload() {
  local to="$1"; shift
  command -v python3 >/dev/null 2>&1 || return 1
  _gate_notify_bounded "$to" "$GATE_NOTIFY_PAYLOAD_TIMEOUT" python3 -c '
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

# _gate_notify_bounded_timeout: print the name of a timeout(1) that supports
# `--kill-after`, or return 1 when no such runner exists. The capability is PROBED,
# not assumed: a `timeout` without `--kill-after` can only SIGTERM, which a helper may
# ignore, so it does not count as a bounding tool here. No bounding tool means we run
# NOTHING external — neither the publish nor the adjunct. Every notification here is
# advisory; a wedged gate is not.
_gate_notify_bounded_timeout() {
  local c
  for c in timeout gtimeout; do
    command -v "$c" >/dev/null 2>&1 || continue
    "$c" --kill-after=1 1 true >/dev/null 2>&1 || continue
    printf '%s\n' "$c"
    return 0
  done
  return 1
}

# _gate_notify_bounded <timeout-cmd> <secs> <cmd...>: run <cmd> under an
# UNIGNORABLE bound — SIGTERM at <secs>, SIGKILL at <secs>+GATE_NOTIFY_KILL_GRACE.
_gate_notify_bounded() {
  local to="$1" secs="$2"
  shift 2
  "$to" --kill-after="$GATE_NOTIFY_KILL_GRACE" "$secs" "$@"
}

# _gate_notify_adjunct <title> <body>: OPTIONAL local desktop/sound notification.
# Three properties make this safe against the defects above:
#   1. POSITIONAL arguments only — never `--category`, the flag upstream swallows.
#   2. Its webhook environment is NEUTRALIZED, so its own (broken) publish path
#      cannot deliver a second, JSON-as-message notification.
#   3. Time-bounded, output discarded, failure swallowed. The `</dev/null` is
#      load-bearing: measured, the pristine upstream binary HANGS (rc=124) when it
#      inherits a tty stdin.
# GATE_NOTIFY_DISABLE_ADJUNCT=1 suppresses it entirely — used by --self-test so a
# capability probe never fires a real desktop/sound notification (review R3).
# _gate_notify_adjunct <timeout-cmd> <title> <body>
_gate_notify_adjunct() {
  [ "${GATE_NOTIFY_DISABLE_ADJUNCT:-0}" = 1 ] && return 0
  local to="$1"
  [ -n "$to" ] || { _gate_notify_debug "no unignorable bound available; skipping adjunct"; return 0; }
  command -v agent-notify >/dev/null 2>&1 || return 0
  # Subshell + export (not `env`) so the neutralization survives while the bound is
  # applied by a shell FUNCTION. `</dev/null` is load-bearing: measured, the pristine
  # upstream binary HANGS on an inherited tty stdin.
  (
    export CODEX_NOTIFY_WEBHOOK= CQLITE_NOTIFY_WEBHOOK= CODEX_NOTIFY_WEBHOOK_PRESET=
    _gate_notify_bounded "$to" "$GATE_NOTIFY_ADJUNCT_TIMEOUT" agent-notify "$2" "$3"
  ) >/dev/null 2>&1 </dev/null || true
  return 0
}

# gate_notify_publish <result> <title> <body>
#
# Publish ONE notification for <result> (PASS/FAIL). ALWAYS returns 0 — every
# failure mode (no target, no python3, no curl, a publish that never completes,
# an adjunct that rejects its arguments or is not executable) is a silent no-op.
gate_notify_publish() {
  local result="${1:-FAIL}" title="${2:-}" body="${3:-}"
  local severity payload to
  # Declared local HERE so _gate_notify_target's assignments land in THIS frame
  # (bash dynamic scoping) and never in the caller's shell.
  local GATE_NOTIFY_DELIVERY_URL GATE_NOTIFY_TOPIC GATE_NOTIFY_SAFE_TARGET
  severity=$(_gate_notify_severity "$result")

  # One bounding tool for ALL external calls, probed ONCE per publish. Absent (or
  # present without `--kill-after`, i.e. SIGTERM-only and therefore escapable) ⇒
  # publish nothing.
  to=$(_gate_notify_bounded_timeout) || to=""
  if [ -z "$to" ]; then
    _gate_notify_debug "no timeout(1) supporting --kill-after; publishing nothing rather than running behind an escapable bound"
  elif _gate_notify_target; then
    if command -v curl >/dev/null 2>&1; then
      payload=$(_gate_notify_payload "$to" "$GATE_NOTIFY_TOPIC" "$title" "$body" \
        "$(_gate_notify_priority "$severity")" "$(_gate_notify_tag "$severity")") || payload=""
      if [ -n "$payload" ]; then
        # SAFE_TARGET, never DELIVERY_URL: the latter carries any `?auth=`/`?token=`
        # credential and must not reach the debug stream.
        _gate_notify_debug "POST $GATE_NOTIFY_SAFE_TARGET $payload"
        # UNIGNORABLE outer bound + the cooperative flag: --max-time alone is honoured
        # only by the REAL curl, so it cannot be the guarantee (#3119 B2).
        # DELIVERY_URL here — query PRESERVED, or a query-credentialed ntfy target
        # publishes unauthenticated, and silently (publish failure is a no-op).
        _gate_notify_bounded "$to" "$GATE_NOTIFY_CURL_TIMEOUT" \
          curl -sS -X POST -H 'Content-Type: application/json' \
          --max-time "$GATE_NOTIFY_CURL_TIMEOUT" \
          -d "$payload" "$GATE_NOTIFY_DELIVERY_URL" >/dev/null 2>&1 </dev/null || true
      else
        _gate_notify_debug "payload build failed (python3 missing, wedged, or errored)"
      fi
    else
      _gate_notify_debug "curl absent; nothing published"
    fi
  fi

  _gate_notify_adjunct "$to" "$title" "$body"
  return 0
}

# --self-test: publish a PASS and a FAIL through a private curl capture shim and
# validate BOTH against the contract. This is the CAPABILITY assertion
# bootstrap runs — the analogue of the mold link probe: nothing is declared
# healthy because a file exists. Exits non-zero ONLY in this mode.
#
# FULLY INDEPENDENT of ambient configuration (review round 2). It pins its OWN
# private topic URL rather than inheriting the machine's webhook: a legitimate
# bare-server-root target plus a separate CQLITE_NOTIFY_TOPIC/CODEX_NOTIFY_NTFY_TOPIC
# override would otherwise resolve to NO topic once the probe cleared those
# overrides, publish nothing, and make bootstrap report a FAILED capability on a
# CORRECTLY configured machine. Whether the machine's own target is usable is a
# separate question, answered by bootstrap's target check — this probe's job is the
# CODE PATH, so it must not vary with the environment.
GATE_NOTIFY_SELFTEST_WEBHOOK="https://gate-notify-selftest.invalid/selftest-topic"

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
  # The validator below is an external call like any other, so it gets the same
  # unignorable bound (review round 4 nit). No bounding tool ⇒ the probe cannot make a
  # bounded claim, so it reports that rather than running the validator unbounded.
  local selftest_to
  if ! selftest_to=$(_gate_notify_bounded_timeout); then
    echo "gate-notify --self-test: SKIP (no timeout(1) supporting --kill-after; cannot bound the validator)" >&2
    rm -rf "$tmp"
    return 1
  fi
  for severity in PASS FAIL; do
    case "$severity" in
      PASS) expect_prio=3; expect_tag=white_check_mark ;;
      *) expect_prio=5; expect_tag=rotating_light ;;
    esac
    log="$tmp/$severity.log"
    : > "$log"
    # GATE_NOTIFY_DISABLE_ADJUNCT: a capability PROBE must not fire a real
    # desktop/sound notification, nor depend on a local helper (review R3).
    # Every target/topic variable is pinned to the probe's OWN private values, so
    # the probe's verdict is about the code path and never about ambient config.
    env CURL_LOG="$log" PATH="$tmp/bin:$PATH" GATE_NOTIFY_DISABLE_ADJUNCT=1 \
      CQLITE_NOTIFY_TOPIC= CODEX_NOTIFY_NTFY_TOPIC= CODEX_NOTIFY_WEBHOOK= \
      GATE_NOTIFY_PAYLOAD_TIMEOUT=10 GATE_NOTIFY_CURL_TIMEOUT=10 \
      CQLITE_NOTIFY_WEBHOOK="$GATE_NOTIFY_SELFTEST_WEBHOOK" \
      bash -c '. "$1"; gate_notify_publish "$2" "gate '"$severity"' selftest@0000000" "RESULT: '"$severity"'"' \
      _ "$self" "$severity" >/dev/null 2>&1
    if ! _gate_notify_bounded "$selftest_to" "$GATE_NOTIFY_PAYLOAD_TIMEOUT" \
      python3 - "$log" "$expect_prio" "$expect_tag" <<'PY'
import json, sys
lines = open(sys.argv[1]).read().splitlines()
if len(lines) != 1:
    print("expected exactly 1 published payload, got %d" % len(lines)); raise SystemExit(1)
url, _, body = lines[0].partition("\t")
try:
    d = json.loads(body)
except Exception as e:
    print("payload is not JSON: %s" % e); raise SystemExit(1)
if not url.endswith("/") or url.rstrip("/").endswith("/" + str(d.get("topic", "\0"))):
    print("POST target is not a server root (topic must be in the body): %s" % url)
    raise SystemExit(1)
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
