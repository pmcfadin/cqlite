#!/usr/bin/env bash
# scripts/tests/test_worker_supervisor.sh — fast, self-contained tests for
# scripts/local/worker-supervisor.sh (issue #2090). No cargo, no gate, no
# network: every external probe/worker/notify is a stub script written to a
# per-test mktemp dir. Target: <30s total.
set -uo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SELF_DIR/../.." && pwd)"
SUPERVISOR="$REPO_ROOT/scripts/local/worker-supervisor.sh"

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
pass() {
  PASS_COUNT=$((PASS_COUNT + 1))
  echo "PASS: $1"
}
fail() {
  FAIL_COUNT=$((FAIL_COUNT + 1))
  echo "FAIL: $1"
}

# t <test-fn> — run a top-level test. IT MUST EXIST, AND IT MUST RETURN ZERO.
#
# THE SUITE REPORTED GREEN THROUGH A TEST THAT DID NOT EXIST (roborev round 27, Medium).
# `test_claim_transition_survives_failed_replacement` was invoked at the bottom of this file and never
# defined. The harness runs under `set -uo pipefail` with NO errexit, so bash printed
# "command not found" to stderr, the status was discarded, and the summary still said
# "80 passed, 0 failed" — through ELEVEN gates. A suite that can report success while a named case never
# runs is the vacuity failure one level up from the individual asserts: every non-vacuity probe in here
# was guarding its own case while the HARNESS had no guard at all.
#
# Both halves are closed: an undefined name is a FAILURE rather than a silent no-op, and a test that
# returns non-zero without having called `fail` is also a failure — otherwise an early `return 1` inside a
# case would vanish the same way.
t() {
  local name="$1" rc=0
  if ! declare -F "$name" >/dev/null 2>&1; then
    fail "harness: test function '$name' is INVOKED but UNDEFINED — it has never run"
    return 0
  fi
  "$name" || rc=$?
  [[ "$rc" -eq 0 ]] || fail "harness: test function '$name' returned non-zero ($rc) without reporting a failure"
}

# skip: an ENVIRONMENTAL non-result (e.g. a live control process that never
# scheduled within the wait cap) — explicitly reported, never counted as failure.
skip() {
  SKIP_COUNT=$((SKIP_COUNT + 1))
  echo "SKIP: $1"
}

TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/cqlite-supervisor-test.XXXXXX")"
T_LOCKFN="$TMP_ROOT/lockfn"
cleanup() { rm -rf "$TMP_ROOT" 2>/dev/null || true; }
trap cleanup EXIT

new_case_dir() {
  local d
  d="$(mktemp -d "$TMP_ROOT/case.XXXXXX")"
  mkdir -p "$d/bin" "$d/logs"
  echo "$d"
}

# ---------------------------------------------------------------------------
# Stub writers
# ---------------------------------------------------------------------------
write_notify_stub() {
  cat >"$1" <<'EOF'
#!/usr/bin/env bash
# $NOTIFY_CMD convention (issue #3119): THREE positional args, <severity>
# <title> <message>. The old `--category <cat>` flag form is gone — the real
# upstream agent-notify has no such arm and silently swallowed it.
printf '%s|%s|%s\n' "${1:-}" "${2:-}" "${3:-}" >>"${NOTIFY_LOG:?NOTIFY_LOG not set}"
EOF
  chmod +x "$1"
}

# Always finalizes; issue number = contents of $2, incremented each call.
# Optional $3 = seconds to sleep after writing the marker, before exit.
write_finalize_stub() {
  local path="$1" counter_file="$2" sleep_s="${3:-0}"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
n=0
[[ -f "$counter_file" ]] && n=\$(cat "$counter_file")
n=\$((n + 1))
echo "\$n" >"$counter_file"
cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":\$n,"pr":"https://github.com/pmcfadin/cqlite/pull/\$n","duration_s":1}
JSON
sleep "$sleep_s"
EOF
  chmod +x "$path"
}

# issue #2841 (design decision A): a HEALTHY worker that emits activity to stdout
# (as a real `claude -p --output-format stream-json --verbose` worker does) BEFORE
# writing its finalize marker — so the supervisor's `>"$logfile"` redirect captures a
# NON-EMPTY iter-N.log. Proves the watchdog has a live stream to scan under `-p`.
write_verbose_finalize_stub() {
  local path="$1" counter_file="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
n=0
[[ -f "$counter_file" ]] && n=\$(cat "$counter_file")
n=\$((n + 1))
echo "\$n" >"$counter_file"
# Stream-style activity to stdout (captured into iter-N.log by the supervisor).
echo '{"type":"system","subtype":"init"}'
echo '{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash"}]}}'
echo '{"type":"assistant","message":{"content":[{"type":"text","text":"dispatching subagent"}]}}'
cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":\$n,"pr":"https://github.com/pmcfadin/cqlite/pull/\$n","duration_s":1}
JSON
EOF
  chmod +x "$path"
}

# Finalize stub that always claims the SAME issue/PR (roborev 1839): used to exercise
# the per-PR auto-merge-stuck path (the same PR observed unmerged N times), distinct
# from write_finalize_stub's incrementing PR (used for the healthy distinct-PR case).
write_fixed_pr_finalize_stub() {
  local path="$1" counter_file="$2" pr="$3"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
n=0
[[ -f "$counter_file" ]] && n=\$(cat "$counter_file")
n=\$((n + 1))
echo "\$n" >"$counter_file"
cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":$pr,"pr":"https://github.com/pmcfadin/cqlite/pull/$pr","duration_s":1}
JSON
EOF
  chmod +x "$path"
}

# Always exits 1 without writing a marker (abnormal iteration).
write_abnormal_stub() {
  cat >"$1" <<'EOF'
#!/usr/bin/env bash
exit 1
EOF
  chmod +x "$1"
}

# First call: outcome=no-work. Every call after: outcome=finalized (issue
# counter separate from the call counter, so budget-vs-no-work is unambiguous).
write_nowork_then_finalize_stub() {
  local path="$1" call_ctr="$2" issue_ctr="$3"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"no-work","issue":null,"pr":null,"duration_s":1}
JSON
else
  n=0
  [[ -f "$issue_ctr" ]] && n=\$(cat "$issue_ctr")
  n=\$((n + 1))
  echo "\$n" >"$issue_ctr"
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":\$n,"pr":"https://github.com/pmcfadin/cqlite/pull/\$n","duration_s":1}
JSON
fi
EOF
  chmod +x "$path"
}

# F2 regression: writes outcome=blocked with a fixed issue number on every
# call (never finalizes) — used to prove the supervisor stops after the SAME
# issue reports blocked on two consecutive iterations, rather than looping.
write_blocked_same_issue_stub() {
  local path="$1" issue="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cat >"\$MARKER_FILE" <<JSON
{"outcome":"blocked","issue":$issue,"pr":null,"duration_s":1,"reason":"needs owner decision"}
JSON
EOF
  chmod +x "$path"
}

# Parks on the EXACT reason token the supervisor keys on (#3393 round 20). Note the sibling stub above
# uses free text ("needs owner decision"), which is deliberately NOT a park token — that is why it
# retains its issue and this one releases it.
write_park_stub() {
  local path="$1" issue="$2" reason="$3"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cat >"\$MARKER_FILE" <<JSON
{"outcome":"blocked","issue":$issue,"pr":null,"duration_s":1,"reason":"$reason"}
JSON
EOF
  chmod +x "$path"
}

# F5 regression: writes outcome=finalized with issue set but pr MISSING
# entirely (not just null) — the marker contract requires BOTH issue and pr
# on "finalized"; a marker missing either must be judged abnormal.
write_finalize_missing_pr_stub() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cat >"$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":42,"duration_s":1}
JSON
EOF
  chmod +x "$path"
}

# F3 regression: writes outcome=blocked with a "reason" containing a double
# quote and an embedded literal newline (via printf %b), to prove the journal
# line stays valid JSON end-to-end (marker_field's python3 read handles the
# marker side; journal_line's json_or_null handles the journal-write side).
write_blocked_nasty_reason_stub() {
  local path="$1"
  cat >"$path" <<'PYEOF'
#!/usr/bin/env bash
set -euo pipefail
python3 - "$MARKER_FILE" <<'PY'
import json, sys
d = {"outcome": "blocked", "issue": 55, "pr": None, "duration_s": 1,
     "reason": 'has a "quote" and\na newline'}
open(sys.argv[1], "w").write(json.dumps(d))
PY
PYEOF
  chmod +x "$path"
}

# Fails loudly (sentinel + exit 1) if the marker file is already present at
# start (proves the supervisor removed a stale marker before spawning).
write_stale_check_stub() {
  local path="$1" sentinel="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
if [[ -f "\$MARKER_FILE" ]]; then
  touch "$sentinel"
  exit 1
fi
cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":1,"pr":"https://github.com/pmcfadin/cqlite/pull/1","duration_s":1}
JSON
EOF
  chmod +x "$path"
}

# issue #2666 CLEAN PARK: first call writes a `blocked` marker with a park
# reason (seam1-approval | needs-decision) and an optional one-line question;
# every call after finalizes. Used to prove a park is judged parked-on-owner,
# fires a high page, never trips the breaker, and the loop advances.
write_park_then_finalize_stub() {
  local path="$1" call_ctr="$2" reason="$3" question="${4:-}"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"blocked","issue":77,"pr":null,"duration_s":1,"reason":"$reason","question":"$question"}
JSON
else
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":78,"pr":"https://github.com/pmcfadin/cqlite/pull/78","duration_s":1}
JSON
fi
EOF
  chmod +x "$path"
}

# issue #2666: writes a marker with an UNKNOWN outcome value — must be judged
# abnormal (counts toward the breaker), never silently trusted.
write_unknown_outcome_stub() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cat >"$MARKER_FILE" <<JSON
{"outcome":"weird-outcome","issue":9,"pr":null,"duration_s":1}
JSON
EOF
  chmod +x "$path"
}

# issue #2666 stuck-on-question: first call prints an interactive-prompt line
# then sleeps past MAX_ITER_SECS (so the watchdog detects + pages and it gets
# timeout-killed WITHOUT a marker); every call after finalizes so the test
# terminates at MAX_ISSUES.
write_stuck_then_finalize_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  echo "AskUserQuestion: Do you want to proceed with option A?"
  sleep 120
else
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":88,"pr":"https://github.com/pmcfadin/cqlite/pull/88","duration_s":1}
JSON
fi
EOF
  chmod +x "$path"
}

# issue #2666 / roborev 1769: abnormal → stuck → abnormal → abnormal → finalize.
# The stuck iteration (call 2) must RESET the consecutive-abnormal counter so the
# crash chain is broken and BREAKER_N=3 never trips; call 5 finalizes so the run
# terminates at MAX_ISSUES.
write_abnormal_stuck_abnormal_finalize_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
case \$calls in
  1|3|4) exit 1 ;;
  2)
    echo "AskUserQuestion: choose an option"
    sleep 120 ;;
  *)
    cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":91,"pr":"https://github.com/pmcfadin/cqlite/pull/91","duration_s":1}
JSON
    ;;
esac
EOF
  chmod +x "$path"
}

# issue #2666 / roborev 1769: parks the SAME issue with a park reason on EVERY
# call (never finalizes) — proves the park-path head-block guard stops after the
# same issue parks on two consecutive iterations.
write_park_same_issue_stub() {
  local path="$1" issue="$2" reason="${3:-needs-decision}"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cat >"\$MARKER_FILE" <<JSON
{"outcome":"blocked","issue":$issue,"pr":null,"duration_s":1,"reason":"$reason","question":"same question"}
JSON
EOF
  chmod +x "$path"
}

# issue #2666 / roborev 1769: parks issue 41, then a DIFFERENT issue 42, then
# finalizes — proves distinct-issue parks do NOT trip the head-block guard.
write_park_two_issues_then_finalize_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
case \$calls in
  1) cat >"\$MARKER_FILE" <<JSON
{"outcome":"blocked","issue":41,"pr":null,"duration_s":1,"reason":"needs-decision","question":"q41"}
JSON
    ;;
  2) cat >"\$MARKER_FILE" <<JSON
{"outcome":"blocked","issue":42,"pr":null,"duration_s":1,"reason":"needs-decision","question":"q42"}
JSON
    ;;
  *) cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":43,"pr":"https://github.com/pmcfadin/cqlite/pull/43","duration_s":1}
JSON
    ;;
esac
EOF
  chmod +x "$path"
}

# issue #2666 / roborev 1773 (case a): prints a stray signature line, then keeps
# WRITING many lines (log grows + signature scrolls out of the tail) and exits 1
# WITHOUT a marker. No wedge evidence → must stay ABNORMAL (counts to breaker),
# never misclassified as stuck.
write_crash_stray_signature_stub() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
echo "AskUserQuestion: stray tool-name printed in normal trace"
for i in $(seq 1 60); do
  echo "working on step $i ..."
  sleep 0.1
done
exit 1
EOF
  chmod +x "$path"
}

# issue #2666 / roborev 1773 (case c): a BUSY worker that prints the signature on
# EVERY line (so it is always in the tail) but keeps WRITING (log grows every
# scan) — the no-growth evidence fails, so it must NOT be classified stuck. Call 1
# runs until killed at the deadline (abnormal); call 2 finalizes to terminate.
write_busy_signature_then_finalize_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  while true; do
    echo "AskUserQuestion tick — still working, writing more output"
    sleep 0.3
  done
else
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":95,"pr":"https://github.com/pmcfadin/cqlite/pull/95","duration_s":1}
JSON
fi
EOF
  chmod +x "$path"
}

# issue #2670: a gh-verify stub that FAILS (exit 1, no output → unverified) on the
# first call and returns MERGED JSON on every call after. Used to prove an
# unverified finalize is not counted and does not trip the breaker, while still
# terminating the run (the second, verified-merged finalize hits MAX_ISSUES=1).
write_gh_flaky_then_merged_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  exit 1
fi
printf %s '{"state":"MERGED","mergedAt":"2026-01-01T00:00:00Z"}'
EOF
  chmod +x "$path"
}

# issue #2670 (roborev 1810): a gh-verify stub that ALWAYS fails transport (exit 1,
# NO stderr → unverified). Used to prove UNVERIFIED_MAX consecutive unverified
# finalizes stop the loop (verify-unavailable).
write_gh_transport_fail_stub() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
exit 1
EOF
  chmod +x "$path"
}

# issue #2670 (roborev 1810): a gh-verify stub that emulates `gh pr view` on a PR
# number that does NOT exist — a resolve failure (stderr signature + nonzero exit),
# distinct from a transport outage. verify_finalized_pr must classify this
# mismatch:UNRESOLVED (forged marker), not unverified.
write_gh_notfound_stub() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
echo "GraphQL: Could not resolve to a PullRequest with the number of $1. (repository.pullRequest)" >&2
exit 1
EOF
  chmod +x "$path"
}

# issue #2670 (roborev 1810): worker finalizes with a FORGED pr on every call —
# call 1 a not-found number (999999), call 2 a garbage non-numeric string — both
# must be judged abnormal mismatch:UNRESOLVED. Never finalizes cleanly.
write_finalize_forged_pr_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":61,"pr":"999999","duration_s":1}
JSON
else
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":62,"pr":"not-a-real-pr","duration_s":1}
JSON
fi
EOF
  chmod +x "$path"
}

# issue #2670 (roborev 1813): gh-verify stub whose FIRST read reports OPEN and
# every read after reports MERGED — proves the mismatch-grace retry absorbs
# read-after-merge lag (ends up merged, never mismatch).
write_gh_open_then_merged_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  printf %s '{"state":"OPEN","mergedAt":null,"autoMergeRequest":null}'
else
  printf %s '{"state":"MERGED","mergedAt":"2026-01-01T00:00:00Z","autoMergeRequest":null}'
fi
EOF
  chmod +x "$path"
}

# issue #2670 (roborev 1813): gh-verify stub — FIRST read OPEN with auto-merge
# ARMED (pending-automerge verdict), every read after MERGED (so a following
# iteration terminates the run). Proves the auto-merge path is not a false mismatch.
write_gh_automerge_then_merged_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  printf %s '{"state":"OPEN","mergedAt":null,"autoMergeRequest":{"enabledAt":"2026-01-01T00:00:00Z"}}'
else
  printf %s '{"state":"MERGED","mergedAt":"2026-01-01T00:00:00Z","autoMergeRequest":null}'
fi
EOF
  chmod +x "$path"
}

# issue #2670 (roborev 1813, finding 4): worker finalizes with a FOREIGN-host PR
# URL (correct path shape, wrong host/repo) — must classify mismatch:UNRESOLVED,
# never merged. Never finalizes cleanly.
write_finalize_foreign_url_stub() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cat >"$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":71,"pr":"https://github.com/evil/other/pull/5","duration_s":1}
JSON
EOF
  chmod +x "$path"
}

# ---------------------------------------------------------------------------
# R7 (issue #3119): the DEFAULT notify path — the one production actually uses.
#
# Every other case in this file injects NOTIFY_CMD with a recording stub, so the
# DEFAULT resolution (NOTIFY_ARGV=(bash <repo>/scripts/lib/gate-notify.sh --publish))
# and the wrapper's `--publish` arm were exercised by NO test. Two mutations survived
# green: reverting the default to bare `agent-notify` — whose pristine 3-positional
# mode puts the SEVERITY in the title slot, i.e. the original defect of this issue,
# reintroduced silently — and breaking the `--publish` arm outright.
#
# So: NOTIFY_CMD UNSET, a curl-capture shim on PATH, and a pre-created stop-file so
# finalize_exit fires immediately. What is asserted is the PUBLISHED payload, which
# only the real default chain can produce.
# ---------------------------------------------------------------------------
test_default_notify_path_publishes() {
  local d curl_log rc title
  d="$(new_case_dir)"
  common_env "$d"
  # THE point of the case: no injected notify command.
  unset NOTIFY_CMD
  curl_log="$d/curl.log"; : >"$curl_log"
  cat >"$d/bin/curl" <<'CURLSHIM'
#!/usr/bin/env bash
body=""; prev=""
for a in "$@"; do
  [[ "$prev" == "-d" ]] && body="$a"
  prev="$a"
done
printf '%s\n' "$body" >>"$CURL_LOG"
CURLSHIM
  chmod +x "$d/bin/curl"
  export CURL_LOG="$curl_log"
  export CQLITE_NOTIFY_WEBHOOK="https://ntfy.invalid/r7-default-path"
  export CODEX_NOTIFY_WEBHOOK= CQLITE_NOTIFY_TOPIC= CODEX_NOTIFY_NTFY_TOPIC=
  export PATH="$d/bin:$PATH"
  touch "$STOP_FILE"   # stop at the first loop top -> finalize_exit -> notify
  timeout -s KILL 60 bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  # The stop page is an `info`, so the wrapper must publish priority 3 and put the
  # supervisor's own TITLE in the title field — not a severity token (the pristine
  # `agent-notify` positional bug) and not nothing (a broken --publish arm).
  title=$(python3 - "$curl_log" <<'PYP'
import json, sys
for line in open(sys.argv[1]):
    line = line.strip()
    if not line:
        continue
    try:
        d = json.loads(line)
    except Exception:
        continue
    print("%s|%s" % (d.get("title", ""), d.get("priority", "")))
    break
PYP
)
  unset CURL_LOG CQLITE_NOTIFY_WEBHOOK CODEX_NOTIFY_WEBHOOK CQLITE_NOTIFY_TOPIC CODEX_NOTIFY_NTFY_TOPIC
  if [[ "$rc" -eq 0 && "$title" == "worker-supervisor stopped|3" ]]; then
    pass "R7 default notify path: NOTIFY_CMD unset -> wrapper published title='worker-supervisor stopped' priority=3"
  else
    fail "R7 default notify path: rc=$rc published='$title' (expected 'worker-supervisor stopped|3'; see $curl_log)"
  fi
}

# ---------------------------------------------------------------------------
# Common env baseline: every test starts here, then overrides what it needs.
# Clear preflight (no holds), generous budgets, fast polling/backoff.
# ---------------------------------------------------------------------------
common_env() {
  local d="$1"
  export MARKER_FILE="$d/marker.json"
  export STOP_FILE="$d/stop"
  export LOG_DIR="$d/logs"
  export JOURNAL_FILE="$d/logs/journal.jsonl"
  export SUPERVISOR_LOCK="$d/lock"
  export NOTIFY_LOG="$d/notify.log"
  : >"$NOTIFY_LOG"
  write_notify_stub "$d/bin/notify.sh"
  export NOTIFY_CMD="$d/bin/notify.sh"
  # The per-issue LOCK seam off by default: `REPO_ROOT` is the REAL lane checkout in these cases, so its
  # branch genuinely names an issue and the legacy-claim migration would fire a network `claim.sh status`
  # in every one of them. Dedicated cases below supply a stub instead.
  export LOCK_CMD=""
  export LOAD_PROBE_CMD="echo 0"
  export DISK_PROBE_CMD="echo 999999"
  # roborev 1839: preflight bounds the two leftover families separately, so it reads
  # per-family probes (the old combined PROC_PROBE_CMD is gone). Default both to clear;
  # leftover-hold tests override the family they exercise.
  export PROC_PROBE_WORKER_CMD="echo 0"
  export PROC_PROBE_BUILD_CMD="echo 0"
  # issue #2670: every "finalized" marker is now GH-verified. Default the mock to
  # MERGED so all pre-existing finalize-based cases credit the issue as before;
  # verification tests override GH_VERIFY_CMD to exercise mismatch/unverified.
  export GH_VERIFY_CMD='printf %s "{\"state\":\"MERGED\",\"mergedAt\":\"2026-01-01T00:00:00Z\",\"autoMergeRequest\":null}"'
  # roborev 1813: mismatch grace re-reads gh a few times; keep the wait at 0 so no
  # test ever sleeps for it.
  export MISMATCH_RETRY_WAIT_SECS=0
  export LOAD_MAX=999999
  export MAX_ISSUES=100
  export MAX_HOURS=8
  export BREAKER_N=3
  export BACKOFF_NOWORK_SECS=1
  export HOLD_POLL_SECS=1
  export MAX_ITER_SECS=10
  export STUCK_POLL_SECS=1
  unset LOAD_CONTROL_FILE 2>/dev/null || true
  unset WORKER_CMD 2>/dev/null || true
  # Reset every knob a test may override but common_env does not explicitly re-set, so
  # one test's override (e.g. MAX_HOURS_SECS=3, PENDING_AUTOMERGE_*) cannot leak into the
  # next test in this shared shell and cause a spurious pass/fail.
  unset MAX_HOURS_SECS DISK_FLOOR_GB PENDING_AUTOMERGE_MAX PENDING_AUTOMERGE_MIN_SECS \
        BUILD_HOLD_MAX LEFTOVER_HOLD_MAX UNVERIFIED_MAX MISMATCH_RETRIES \
        MISMATCH_GRACE_CAP_SECS PROC_LIST_WORKER_CMD PROC_LIST_BUILD_CMD 2>/dev/null || true
  # Claim stamping (issue #2655) OFF by default so most tests stay focused; the
  # dedicated claim tests set a hermetic CLAIM_CMD stub that logs its args.
  export CLAIM_CMD=""
  unset HEARTBEAT_MACHINE 2>/dev/null || true
}

jline_count() { grep -c "$2" "$1" 2>/dev/null || true; }

# A hermetic claim-heartbeat.sh stand-in: append "<subcmd> <args...>" to
# $CLAIM_LOG on every call, always succeed. Lets a test assert the supervisor
# invoked `stamp`/`reap` with the right shape, without any origin/network.
write_claim_stub() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${CLAIM_LOG:?CLAIM_LOG not set}"
# `stamp` prints the sha it wrote on STDOUT (roborev round 19), which the supervisor captures and
# passes back as a `reap` LEASE. A stub that printed nothing would exercise the NO-lease path instead,
# and every lease assertion below would pass vacuously while proving the opposite of the property.
# Fixed and hex so assertions can name it exactly.
[ "${1:-}" = stamp ] && printf 'deadbeefdeadbeefdeadbeefdeadbeefdeadbeef\n'
exit 0
EOF
  chmod +x "$path"
}

# write_claim_stub_failing_issue_stamp <path> — logs every call like the normal stub but FAILS any
# `stamp <numeric-issue> ...`, i.e. the replacement stamp of a lane transition (#3393, roborev round
# 2). Used to prove the transition cannot open a liveness gap: the OLD ref must survive a failed
# replacement, because a lane with no claim ref at all is invisible to dead-lanes and the reaper.
write_claim_stub_failing_issue_stamp() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${CLAIM_LOG:?CLAIM_LOG not set}"
if [ "${1:-}" = "stamp" ]; then
  case "${2:-}" in
    p*) exit 0 ;;          # the placeholder stamp still succeeds
    *[!0-9]*) exit 0 ;;
    '') exit 0 ;;
    *) exit 1 ;;           # an ISSUE-named stamp fails: the replacement cannot land
  esac
fi
exit 0
EOF
  chmod +x "$path"
}

# ---------------------------------------------------------------------------
# Test 1: happy path — 2 finalized iterations, then MAX_ISSUES=2 budget stop.
# ---------------------------------------------------------------------------
test_happy_path_budget_stop() {
  local d counter jf rc fcount scount
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=2
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  scount=$(jline_count "$jf" '"outcome":"summary"')
  if [[ "$rc" -eq 0 && "$fcount" -eq 2 && "$scount" -eq 1 ]] && grep -q '"reason":"budget-issues"' "$jf"; then
    pass "happy path: 2 finalized + budget-issues summary stop"
  else
    fail "happy path: rc=$rc finalized=$fcount summary=$scount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 2: crash-loop breaker — N abnormal exits stop + alert, no hot respawn.
# ---------------------------------------------------------------------------
test_breaker_stops_on_abnormal() {
  local d jf rc acount ncount
  d="$(new_case_dir)"
  common_env "$d"
  write_abnormal_stub "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export BREAKER_N=3
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  ncount=$(grep -c '^error|.*BREAKER' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && "$acount" -eq 3 ]] && grep -q '"reason":"breaker"' "$jf" && [[ "$ncount" -ge 1 ]]; then
    pass "breaker: 3 consecutive abnormal exits stop with ALERT, no hot respawn"
  else
    fail "breaker: rc=$rc abnormal=$acount notify_breaker=$ncount (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 3: stop-file honored between iterations (clean exit, exactly 1 ran).
# ---------------------------------------------------------------------------
test_stop_file_honored() {
  local d counter jf sup_pid rc waited
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  # Sleep after the marker write so the test can create the stop-file while
  # this iteration is still "in flight" — guarantees the NEXT loop-top check
  # sees it, with no race on how fast the stub itself runs.
  write_finalize_stub "$d/bin/worker.sh" "$counter" 1
  export WORKER_CMD="$d/bin/worker.sh"
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 &
  sup_pid=$!
  waited=0
  while [[ ! -f "$counter" && "$waited" -lt 100 ]]; do
    sleep 0.1
    waited=$((waited + 1))
  done
  touch "$STOP_FILE"
  wait "$sup_pid"
  rc=$?
  local fcount
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  if [[ "$rc" -eq 0 && "$fcount" -eq 1 ]] && grep -q '"reason":"stop-file"' "$jf"; then
    pass "stop-file: honored between iterations, exactly 1 ran"
  else
    fail "stop-file: rc=$rc finalized=$fcount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 4: preflight hold — high load blocks the spawn until it clears.
# ---------------------------------------------------------------------------
test_preflight_load_hold() {
  local d counter sup_pid rc hold_notifies
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export LOAD_CONTROL_FILE="$d/load"
  echo 99 >"$LOAD_CONTROL_FILE"
  # shellcheck disable=SC2016  # deferred: expanded later by the supervisor's own `bash -c`, not here.
  export LOAD_PROBE_CMD='cat "$LOAD_CONTROL_FILE"'
  export LOAD_MAX=1

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 &
  sup_pid=$!
  # Load-proof: this suite is now gate-wired (agent-gate.sh tooling-tests), so a
  # fixed sleep-then-assert window flakes when the box is busy. POLL (hard-capped
  # at 30s) until the HOLD notify appears instead — the semantic is unchanged
  # (HOLD fires while load is high, spawn is deferred). Load stays pinned high
  # (LOAD_CONTROL_FILE=99) throughout, so the spawn cannot happen until we clear
  # it below; the counter must remain absent the whole time.
  local waited=0
  hold_notifies=0
  while [[ "$waited" -lt 300 ]]; do
    hold_notifies=$(grep -c '^error|worker-supervisor HOLD|HOLD: load' "$NOTIFY_LOG" 2>/dev/null || true)
    [[ "$hold_notifies" -ge 1 ]] && break
    sleep 0.1
    waited=$((waited + 1))
  done
  local invoked_while_high="no"
  [[ -f "$counter" ]] && invoked_while_high="yes"

  echo 0 >"$LOAD_CONTROL_FILE"
  waited=0
  while [[ ! -f "$counter" && "$waited" -lt 300 ]]; do
    sleep 0.1
    waited=$((waited + 1))
  done
  wait "$sup_pid"
  rc=$?

  if [[ "$invoked_while_high" == "no" && "$rc" -eq 0 && -f "$counter" && "$hold_notifies" -ge 1 ]]; then
    pass "preflight: high load holds the spawn (no invoke), then proceeds once clear (HOLD notify fired)"
  else
    fail "preflight: invoked_while_high=$invoked_while_high rc=$rc hold_notifies=$hold_notifies (see $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 5: no-work backoff does NOT count against MAX_ISSUES.
# ---------------------------------------------------------------------------
test_nowork_not_counted() {
  local d call_ctr issue_ctr jf rc
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  issue_ctr="$d/issues"
  common_env "$d"
  write_nowork_then_finalize_stub "$d/bin/worker.sh" "$call_ctr" "$issue_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  local nwcount fcount calls
  nwcount=$(jline_count "$jf" '"outcome":"no-work"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  calls=$(cat "$call_ctr" 2>/dev/null || echo -1)
  # MAX_ISSUES=1: if no-work counted, the supervisor would stop after the
  # very first (no-work) iteration and never reach a second, finalizing call.
  if [[ "$rc" -eq 0 && "$nwcount" -eq 1 && "$fcount" -eq 1 && "$calls" -eq 2 ]]; then
    pass "no-work: backoff sleeps but does not count toward MAX_ISSUES"
  else
    fail "no-work: rc=$rc no-work=$nwcount finalized=$fcount calls=$calls (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 6: single-instance lock — second concurrent invocation refuses loudly.
# ---------------------------------------------------------------------------
test_single_instance_lock() {
  local d counter pid_a rc_b stderr_b
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter" 3
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100

  bash "$SUPERVISOR" >"$d/stdout_a.log" 2>&1 &
  pid_a=$!
  local waited=0
  while [[ ! -f "$counter" && "$waited" -lt 50 ]]; do
    sleep 0.1
    waited=$((waited + 1))
  done

  bash "$SUPERVISOR" >"$d/stdout_b.log" 2>&1
  rc_b=$?
  stderr_b="$(cat "$d/stdout_b.log")"

  kill "$pid_a" 2>/dev/null || true
  wait "$pid_a" 2>/dev/null || true

  if [[ "$rc_b" -ne 0 ]] && echo "$stderr_b" | grep -q "already running" && echo "$stderr_b" | grep -q "pid $pid_a"; then
    pass "flock: second concurrent instance refuses loudly with holder pid"
  else
    fail "flock: rc_b=$rc_b pid_a=$pid_a stderr_b='$stderr_b'"
  fi
}

# ---------------------------------------------------------------------------
# Test 7: stale marker is removed before spawn (never re-judged).
# ---------------------------------------------------------------------------
test_stale_marker_removed() {
  local d sentinel jf rc
  d="$(new_case_dir)"
  sentinel="$d/stale-detected"
  common_env "$d"
  write_stale_check_stub "$d/bin/worker.sh" "$sentinel"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  jf="$JOURNAL_FILE"
  mkdir -p "$(dirname "$MARKER_FILE")"
  echo '{"outcome":"finalized","issue":999,"pr":"stale"}' >"$MARKER_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  if [[ "$rc" -eq 0 && ! -f "$sentinel" ]] && grep -q '"issue":1,' "$jf" && ! grep -q '"issue":999' "$jf"; then
    pass "stale marker: removed before spawn, fresh outcome judged (issue 1, not stale 999)"
  else
    fail "stale marker: rc=$rc sentinel_present=$([[ -f "$sentinel" ]] && echo yes || echo no) (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 8 (F2 regression): the SAME issue reports "blocked" on two consecutive
# iterations → supervisor stops after the second with a head-blocked notify,
# clean exit, and never reaches MAX_HOURS/MAX_ISSUES.
# ---------------------------------------------------------------------------
test_repeated_blocked_head_of_queue_stops() {
  local d jf rc bcount hb_notifies
  d="$(new_case_dir)"
  common_env "$d"
  write_blocked_same_issue_stub "$d/bin/worker.sh" 7
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  bcount=$(jline_count "$jf" '"outcome":"blocked"')
  hb_notifies=$(grep -c '^error|.*persistently blocked' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$bcount" -eq 2 ]] && grep -q '"reason":"head-blocked"' "$jf" && [[ "$hb_notifies" -ge 1 ]]; then
    pass "F2: same issue blocked twice in a row stops cleanly with head-blocked notify"
  else
    fail "F2: rc=$rc blocked_iters=$bcount head_blocked_notifies=$hb_notifies (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 9 (F5 regression): a "finalized" marker missing "pr" is judged
# abnormal — ISSUES_DONE must not advance, and it must count toward the
# crash-loop breaker (proven here by tripping BREAKER_N=1 immediately).
# ---------------------------------------------------------------------------
test_finalized_missing_pr_is_abnormal() {
  local d jf rc fcount acount
  d="$(new_case_dir)"
  common_env "$d"
  write_finalize_missing_pr_stub "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  if [[ "$rc" -ne 0 && "$fcount" -eq 0 && "$acount" -eq 1 ]] && grep -q '"reason":"breaker"' "$jf"; then
    pass "F5: finalized marker missing pr is judged abnormal, not counted done"
  else
    fail "F5: rc=$rc finalized=$fcount abnormal=$acount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 10 (F3 regression): a "reason" containing a double-quote and an
# embedded newline must still produce a journal line that parses as valid
# JSON — proves journal_line's json_or_null escaping (not just printf %s).
# ---------------------------------------------------------------------------
test_journal_escapes_nasty_reason() {
  local d jf rc line all_valid
  d="$(new_case_dir)"
  common_env "$d"
  write_blocked_nasty_reason_stub "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  if [[ ! -f "$jf" ]]; then
    fail "F3: no journal file written ($jf)"
    return
  fi
  all_valid="yes"
  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    printf '%s' "$line" | python3 -c 'import json,sys; json.loads(sys.stdin.read())' 2>/dev/null || all_valid="no"
  done <"$jf"
  if [[ "$rc" -eq 0 && "$all_valid" == "yes" ]] && grep -q '"outcome":"blocked"' "$jf"; then
    pass "F3: reason with embedded quote+newline still yields valid JSON journal lines"
  else
    fail "F3: rc=$rc all_valid=$all_valid (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 11 (#2666): blocked/seam1-approval is a CLEAN PARK → verdict
# parked-on-owner, ONE high-priority page, never abnormal, never trips the
# breaker (BREAKER_N=1 here would stop before finalizing if it did), and the
# loop advances to the next issue.
# ---------------------------------------------------------------------------
test_park_seam1_parked_on_owner() {
  local d call_ctr jf rc pcount fcount page
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_park_then_finalize_stub "$d/bin/worker.sh" "$call_ctr" "seam1-approval" ""
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  pcount=$(jline_count "$jf" '"outcome":"parked-on-owner"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  page=$(grep -c '^error|worker-supervisor: parked issue 77' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$pcount" -eq 1 && "$fcount" -eq 1 && "$page" -ge 1 ]] &&
     ! grep -q '"outcome":"abnormal"' "$jf" && ! grep -q '"reason":"breaker"' "$jf"; then
    pass "park(seam1-approval): parked-on-owner + high page, no breaker, loop advances"
  else
    fail "park(seam1): rc=$rc parked=$pcount finalized=$fcount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 12 (#2666): blocked/needs-decision parks the same way AND the page title
# carries the marker's one-line "question" field (issue # + first line).
# ---------------------------------------------------------------------------
test_park_needs_decision_question_in_title() {
  local d call_ctr jf rc pcount page
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_park_then_finalize_stub "$d/bin/worker.sh" "$call_ctr" "needs-decision" "Which compaction strategy for wide rows?"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  pcount=$(jline_count "$jf" '"outcome":"parked-on-owner"')
  page=$(grep -c 'parked issue 77 — Which compaction strategy for wide rows?' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$pcount" -eq 1 && "$page" -ge 1 ]] && grep -q '"reason":"needs-decision"' "$jf"; then
    pass "park(needs-decision): parked-on-owner + question text in the page title"
  else
    fail "park(needs-decision): rc=$rc parked=$pcount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 13 (#2666): a marker with an UNKNOWN outcome value is still judged
# abnormal (counts toward the breaker) — parks must not have widened the set of
# "trusted" outcomes.
# ---------------------------------------------------------------------------
test_unknown_outcome_is_abnormal() {
  local d jf rc acount pcount
  d="$(new_case_dir)"
  common_env "$d"
  write_unknown_outcome_stub "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  pcount=$(jline_count "$jf" '"outcome":"parked-on-owner"')
  if [[ "$rc" -ne 0 && "$acount" -eq 1 && "$pcount" -eq 0 ]] && grep -q '"reason":"breaker"' "$jf"; then
    pass "unknown outcome: judged abnormal, trips breaker (not parked/trusted)"
  else
    fail "unknown outcome: rc=$rc abnormal=$acount parked=$pcount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 14 (#2666): a worker WEDGED on an interactive prompt is detected
# mid-iteration → immediate high page + verdict stuck-on-question when it exits
# without a marker; NEVER abnormal, never trips the breaker (BREAKER_N=1 here).
# The second iteration finalizes so the run terminates at MAX_ISSUES.
# ---------------------------------------------------------------------------
test_stuck_on_question_detected() {
  local d call_ctr jf rc scount fcount page
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_stuck_then_finalize_stub "$d/bin/worker.sh" "$call_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  # Generous deadline headroom: the watchdog detects on its first poll (~1s in);
  # a large MAX_ITER_SECS keeps detection well ahead of the deadline-kill even
  # under a heavily loaded box (the wedged stub sleeps 120s, so it is always
  # killed by the deadline, never by exiting on its own).
  export MAX_ITER_SECS=10
  export STUCK_POLL_SECS=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  scount=$(jline_count "$jf" '"outcome":"stuck-on-question"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  page=$(grep -c '^error|worker-supervisor: stuck-on-question' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$scount" -eq 1 && "$fcount" -eq 1 && "$page" -ge 1 ]] &&
     ! grep -q '"outcome":"abnormal"' "$jf" && ! grep -q '"reason":"breaker"' "$jf" &&
     grep -q 'AskUserQuestion' "$NOTIFY_LOG"; then
    pass "stuck-on-question: detected mid-iteration, high page, no breaker, loop advances"
  else
    fail "stuck-on-question: rc=$rc stuck=$scount finalized=$fcount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 15 (#2666): unit-test the prompt-signature grep directly by SOURCING the
# supervisor (the source-guard keeps main() from running) and calling
# detect_prompt_signature/captured_question against fixture logs — fires on a
# menu block, stays silent on a clean log, and captures the question text.
# ---------------------------------------------------------------------------
test_prompt_signature_grep() {
  local d fixture clean out
  d="$(new_case_dir)"
  fixture="$d/iter.log"
  clean="$d/clean.log"
  printf 'building project...\nrunning tests\n\xe2\x9d\xaf 1. Yes\n  2. No\n' >"$fixture"
  printf 'building project...\nall tests green\nfinalized issue 5\n' >"$clean"

  out="$(SUPERVISOR="$SUPERVISOR" FIX="$fixture" CLN="$clean" bash -c '
    # shellcheck disable=SC1090
    source "$SUPERVISOR"
    set +e
    if detect_prompt_signature "$FIX"; then echo MATCH; else echo NOMATCH; fi
    if detect_prompt_signature "$CLN"; then echo CLEAN-MATCH; else echo CLEAN-NOMATCH; fi
    captured_question "$FIX"
  ' 2>/dev/null)"

  if echo "$out" | grep -q '^MATCH$' && echo "$out" | grep -q '^CLEAN-NOMATCH$' &&
     echo "$out" | grep -q '1. Yes'; then
    pass "prompt-signature grep: fires on menu block, silent on clean log, captures text"
  else
    fail "prompt-signature grep: out='$out'"
  fi
}

# ---------------------------------------------------------------------------
# Test 16 (#2666 / roborev 1769): a stuck-on-question iteration RESETS the
# consecutive-abnormal counter — the chain abnormal→stuck→abnormal→abnormal must
# NOT trip a BREAKER_N=3 breaker (the stuck iteration breaks the chain). Call 5
# finalizes so the run terminates at MAX_ISSUES=1.
# ---------------------------------------------------------------------------
test_stuck_breaks_abnormal_chain() {
  local d call_ctr jf rc scount acount fcount
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_abnormal_stuck_abnormal_finalize_stub "$d/bin/worker.sh" "$call_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=3
  # Headroom so the stuck iteration (call 2) is reliably DETECTED (not
  # deadline-killed before the first poll) even under load — the whole point of
  # this test is that a detected stuck iteration resets the abnormal chain.
  export MAX_ITER_SECS=10
  export STUCK_POLL_SECS=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  scount=$(jline_count "$jf" '"outcome":"stuck-on-question"')
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  if [[ "$rc" -eq 0 && "$scount" -eq 1 && "$acount" -eq 3 && "$fcount" -eq 1 ]] &&
     ! grep -q '"reason":"breaker"' "$jf"; then
    pass "stuck breaks abnormal chain: 3 abnormals split by a stuck iter never trip BREAKER_N=3"
  else
    fail "stuck-chain: rc=$rc stuck=$scount abnormal=$acount finalized=$fcount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 17 (#2666 / roborev 1769): the SAME issue parking on two consecutive
# iterations → head-block-on-decision page + clean stop (mirrors the F2
# blocked-path guard); never loops to MAX_ISSUES.
# ---------------------------------------------------------------------------
test_repeated_park_same_issue_stops() {
  local d jf rc pcount hb
  d="$(new_case_dir)"
  common_env "$d"
  write_park_same_issue_stub "$d/bin/worker.sh" 33 "needs-decision"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  pcount=$(jline_count "$jf" '"outcome":"parked-on-owner"')
  hb=$(grep -c '^error|worker-supervisor: issue 33 head-blocked on decision' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$pcount" -eq 2 && "$hb" -ge 1 ]] && grep -q '"reason":"head-blocked-decision"' "$jf"; then
    pass "repeated park (same issue): head-blocked-on-decision page + clean stop after 2"
  else
    fail "repeated-park: rc=$rc parked=$pcount head_block=$hb (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 18 (#2666 / roborev 1769): parks of DIFFERENT issues do NOT trip the
# head-block guard — issue 41 then 42 park, then a finalize terminates the run.
# ---------------------------------------------------------------------------
test_different_issue_parks_do_not_head_block() {
  local d call_ctr jf rc pcount fcount
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_park_two_issues_then_finalize_stub "$d/bin/worker.sh" "$call_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=100
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  pcount=$(jline_count "$jf" '"outcome":"parked-on-owner"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  if [[ "$rc" -eq 0 && "$pcount" -eq 2 && "$fcount" -eq 1 ]] &&
     ! grep -q '"reason":"head-blocked-decision"' "$jf" && ! grep -q 'head-blocked on decision' "$NOTIFY_LOG"; then
    pass "different-issue parks: no head-block, loop advances through both then finalizes"
  else
    fail "different-park: rc=$rc parked=$pcount finalized=$fcount (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 19 (#2666 / roborev 1773, case a): a crash whose ONLY signature is a stray
# match in scrollback (log grew + match scrolled out of the tail) must stay
# ABNORMAL and count toward the breaker — NOT be misclassified as stuck.
# ---------------------------------------------------------------------------
test_stray_signature_scrollback_is_abnormal() {
  local d jf rc acount scount
  d="$(new_case_dir)"
  common_env "$d"
  write_crash_stray_signature_stub "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=1
  export STUCK_POLL_SECS=1
  export MAX_ITER_SECS=20
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  scount=$(jline_count "$jf" '"outcome":"stuck-on-question"')
  if [[ "$rc" -ne 0 && "$acount" -eq 1 && "$scount" -eq 0 ]] && grep -q '"reason":"breaker"' "$jf"; then
    pass "stray scrollback signature: crash stays ABNORMAL (breaker), not stuck"
  else
    fail "stray-scrollback: rc=$rc abnormal=$acount stuck=$scount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 20 (#2666 / roborev 1773, case b): a GENUINE wedge — alive + signature in
# the tail + log frozen across two consecutive scans → stuck-on-question, high
# page, never toward the breaker. Call 2 finalizes so the run terminates.
# ---------------------------------------------------------------------------
test_genuine_wedge_frozen_is_stuck() {
  local d call_ctr jf rc scount fcount page
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_stuck_then_finalize_stub "$d/bin/worker.sh" "$call_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  export STUCK_POLL_SECS=1
  export MAX_ITER_SECS=10
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  scount=$(jline_count "$jf" '"outcome":"stuck-on-question"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  page=$(grep -c '^error|worker-supervisor: stuck-on-question' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$scount" -eq 1 && "$fcount" -eq 1 && "$page" -ge 1 ]] &&
     ! grep -q '"outcome":"abnormal"' "$jf" && ! grep -q '"reason":"breaker"' "$jf"; then
    pass "genuine wedge (frozen log + tail signature x2 polls): stuck, high page, no breaker"
  else
    fail "genuine-wedge: rc=$rc stuck=$scount finalized=$fcount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 21 (#2666 / roborev 1773, case c): a BUSY worker printing the signature on
# every line while STILL WRITING (log grows between scans) → no-growth evidence
# fails → NOT stuck (marker-less kill stays abnormal). Call 2 finalizes.
# ---------------------------------------------------------------------------
test_busy_writing_signature_not_stuck() {
  local d call_ctr jf rc scount acount fcount
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_busy_signature_then_finalize_stub "$d/bin/worker.sh" "$call_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=2
  export STUCK_POLL_SECS=1
  export MAX_ITER_SECS=5
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  scount=$(jline_count "$jf" '"outcome":"stuck-on-question"')
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  if [[ "$rc" -eq 0 && "$scount" -eq 0 && "$acount" -eq 1 && "$fcount" -eq 1 ]]; then
    pass "busy worker printing signature while writing: NOT stuck (growth defeats it)"
  else
    fail "busy-writing: rc=$rc stuck=$scount abnormal=$acount finalized=$fcount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 22 (#2666 / roborev 1773, case d): exit-latency — a fast-finalizing worker
# is judged on the ~1s exit cadence, NOT held until the 30s wedge-scan cadence.
# Loose, load-proof cap (well under STUCK_POLL_SECS=30).
# ---------------------------------------------------------------------------
test_fast_exit_latency() {
  local d counter t0 t1 elapsed rc fcount
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export STUCK_POLL_SECS=30
  export MAX_ITER_SECS=7200

  t0=$(date +%s)
  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  t1=$(date +%s)
  elapsed=$((t1 - t0))
  fcount=$(jline_count "$JOURNAL_FILE" '"outcome":"finalized"')
  if [[ "$rc" -eq 0 && "$fcount" -eq 1 && "$elapsed" -lt 15 ]]; then
    pass "exit-latency: fast finalize judged in ${elapsed}s (<15s, not the 30s scan cadence)"
  else
    fail "exit-latency: rc=$rc finalized=$fcount elapsed=${elapsed}s (see $JOURNAL_FILE)"
  fi
}

# ---------------------------------------------------------------------------
# Test 23 (#2670): a "finalized" marker whose PR gh-verifies as MERGED is
# credited normally — outcome finalized, journal `verified: merged`, counted
# toward MAX_ISSUES (proven by the budget-issues stop).
# ---------------------------------------------------------------------------
test_finalized_verified_merged_counts() {
  local d counter jf rc fcount
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  # explicit MERGED mock (common_env already defaults to this; pin it here so the
  # case is self-describing)
  export GH_VERIFY_CMD='printf %s "{\"state\":\"MERGED\",\"mergedAt\":\"2026-01-01T00:00:00Z\"}"'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  if [[ "$rc" -eq 0 && "$fcount" -eq 1 ]] &&
     grep -q '"outcome":"finalized".*"verified":"merged"' "$jf" &&
     grep -q '"reason":"budget-issues"' "$jf"; then
    pass "verify(merged): finalized credited, journal verified=merged, counts to budget"
  else
    fail "verify(merged): rc=$rc finalized=$fcount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 23-claim (#2655): the supervisor STAMPS refs/lane-claims/<machine>/<issue> before each
# spawn and CLEARS (reap) it on a clean exit — via CLAIM_CMD, mechanically, without the
# worker LLM. A hermetic CLAIM_CMD stub logs every invocation; we assert one
# `stamp <issue> <pid>` per iteration and exactly one `reap <machine>` at stop.
# ---------------------------------------------------------------------------
test_claim_stamp_each_iter_and_clear_on_exit() {
  local d counter jf rc stamps reaps
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=2
  export CLAIM_LOG="$d/claim.log"
  : >"$CLAIM_LOG"
  write_claim_stub "$d/bin/claim.sh"
  export CLAIM_CMD="bash $d/bin/claim.sh"
  export HEARTBEAT_MACHINE="testbox"
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  # 2 finalized iterations => 2 stamps; a clean budget stop => exactly 1 reap.
  stamps=$(grep -c '^stamp ' "$CLAIM_LOG" 2>/dev/null || true)
  reaps=$(grep -c '^reap testbox' "$CLAIM_LOG" 2>/dev/null || true)
  # Every stamp carries a LANE ID then the SUPERVISOR pid (#3393). The lane id is the issue number
  # when known, or `p<pid>` when it is not — which is the case here, because `CLAIM_ISSUE` is
  # cleared on `finalized`, so a supervisor finalising issue after issue never knows its issue at
  # spawn time. The placeholder MUST be unique per supervisor: the old shared "0" made every
  # unknown-issue supervisor on a machine write the same per-lane ref, re-creating the masking that
  # per-lane refs exist to remove.
  local well_formed="yes"
  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    [[ "$line" =~ ^stamp\ ([0-9]+|p[0-9]+-[0-9a-f]+)\ [0-9]+$ ]] || well_formed="no"
    [[ "$line" =~ ^stamp\ 0\  ]] && well_formed="no"   # the shared placeholder must be gone
  done < <(grep '^stamp ' "$CLAIM_LOG" 2>/dev/null)
  # ...and both stamps must name the SAME placeholder, since it is this supervisor's identity.
  local uniq_ids
  uniq_ids=$(grep '^stamp ' "$CLAIM_LOG" 2>/dev/null | awk '{print $2}' | sort -u | wc -l | tr -d ' ')
  [[ "$uniq_ids" == "1" ]] || well_formed="no"
  if [[ "$rc" -eq 0 && "$stamps" -eq 2 && "$reaps" -eq 1 && "$well_formed" == "yes" ]]; then
    pass "claim: stamp per iteration (unique p<pid> lane id + supervisor pid, never the shared 0) + one reap on clean exit"
  else
    fail "claim: rc=$rc stamps=$stamps reaps=$reaps well_formed=$well_formed (see $CLAIM_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 24 (#2670): a "finalized" marker whose PR gh-verifies as OPEN is judged
# ABNORMAL — a HIGH page names the discrepancy, ISSUES_DONE does NOT advance
# (the false finalize is not counted), and it counts toward the breaker (proven
# by tripping BREAKER_N=1 immediately).
# ---------------------------------------------------------------------------
test_finalized_mismatch_open_is_abnormal() {
  local d counter jf rc fcount acount page
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=1
  export GH_VERIFY_CMD='printf %s "{\"state\":\"OPEN\",\"mergedAt\":null}"'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  page=$(grep -c '^error|worker-supervisor: finalized MISMATCH' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && "$fcount" -eq 0 && "$acount" -eq 1 && "$page" -ge 1 ]] &&
     grep -q '"outcome":"abnormal".*"verified":"mismatch:OPEN"' "$jf" &&
     grep -q '"reason":"breaker"' "$jf"; then
    pass "verify(mismatch OPEN): abnormal + high page, not counted, trips breaker"
  else
    fail "verify(mismatch): rc=$rc finalized=$fcount abnormal=$acount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 25 (#2670): gh unavailable → UNVERIFIED — outcome finalized-unverified
# (journal `verified: unverified`), NOT counted toward MAX_ISSUES, and NEUTRAL to
# the breaker (BREAKER_N=1 here must NOT trip on it). The gh mock fails on call 1
# (unverified) then returns MERGED, so the second, verified-merged finalize hits
# MAX_ISSUES=1 — proving the unverified iteration was not counted (else the run
# would have stopped before it).
# ---------------------------------------------------------------------------
test_finalized_unverified_not_counted_no_breaker() {
  local d counter gh_ctr jf rc ucount fcount page
  d="$(new_case_dir)"
  counter="$d/counter"
  gh_ctr="$d/gh-calls"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  write_gh_flaky_then_merged_stub "$d/bin/gh.sh" "$gh_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  # shellcheck disable=SC2016  # $1 expanded later by the supervisor's own `bash -c`.
  export GH_VERIFY_CMD="$d/bin/gh.sh \"\$1\""
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  ucount=$(jline_count "$jf" '"outcome":"finalized-unverified"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  page=$(grep -c 'finalized UNVERIFIED' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$ucount" -eq 1 && "$fcount" -eq 1 && "$page" -ge 1 ]] &&
     grep -q '"outcome":"finalized-unverified".*"verified":"unverified"' "$jf" &&
     ! grep -q '"reason":"breaker"' "$jf"; then
    pass "verify(unverified): finalized-unverified, not counted, breaker neutral, loop continues"
  else
    fail "verify(unverified): rc=$rc unverified=$ucount finalized=$fcount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 26 (#2670 / #2841): PROC_PROBE discriminates the supervisor's OWN
# unattended worker spawn shape (`claude … -p … --agent flow-lead …`, issue
# #2841) from a legitimate INTERACTIVE `claude --agent flow-lead` lead session
# (no `-p`) and a plain `claude` REPL. Portable PROPERTY proof is a pure-string
# `grep -E` regex check (always runs, deterministic); the live-process PID check
# is a bonus that SKIPs (never fails) if the control process never schedules
# within the wait cap — an environmental non-result, not a property failure
# (roborev 1819 finding 7).
# ---------------------------------------------------------------------------
test_proc_probe_discriminates_worker_claude() {
  # Source the ACTUAL pattern from the script (anti-drift): a regex edit that
  # broke discrimination would break this test, not silently pass a stale copy.
  local pat
  pat="$(grep -E "^PROC_MATCH_WORKER=" "$SUPERVISOR" | head -1 | sed -E "s/^PROC_MATCH_WORKER='(.*)'$/\1/")"
  # Pure-string property proof (no live process): the pattern matches the
  # unattended `-p` (and long-form `--print`) worker argv shape and NOT an
  # interactive lead / plain REPL. The `-p` MUST be matched as a whitespace-
  # delimited token (roborev #2841): a `claude --dangerously-skip-permissions
  # --agent flow-lead` interactive lead has a `-p` INSIDE `ski-p-ermissions` but
  # NO real print flag, and must NOT match.
  if ! printf "claude -p --output-format stream-json --verbose --dangerously-skip-permissions --agent flow-lead '/worker'\n" | grep -qE "$pat" ||
       ! printf "claude --print --dangerously-skip-permissions --agent flow-lead '/worker'\n" | grep -qE "$pat" ||
       printf 'claude --dangerously-skip-permissions --agent flow-lead review the board\n' | grep -qE "$pat" ||
       printf 'claude --agent flow-lead review the board\n' | grep -qE "$pat" ||
       printf 'claude\n' | grep -qE "$pat"; then
    fail "proc-probe: pure-string regex does not discriminate -p/--print worker vs interactive lead (incl. skip-permissions) / REPL"
    return
  fi
  # Bonus live check: spawn the three argv-shaped stubs and confirm the same
  # discrimination against real PIDs.
  bash -c "exec -a 'claude -p --dangerously-skip-permissions --agent flow-lead /worker' sleep 30" &
  local wpid=$!
  bash -c 'exec -a "claude --agent flow-lead review the board" sleep 30' &
  local ipid=$!
  bash -c 'exec -a "claude" sleep 30' &
  local rpid=$!
  local waited=0 control_up="no"
  while [[ "$waited" -lt 50 ]]; do
    pgrep -f "$pat" | grep -qw "$wpid" && { control_up="yes"; break; }
    sleep 0.1
    waited=$((waited + 1))
  done
  if [[ "$control_up" == "no" ]]; then
    kill "$wpid" "$ipid" "$rpid" 2>/dev/null || true
    wait "$wpid" "$ipid" "$rpid" 2>/dev/null || true
    skip "proc-probe (live): control worker process never scheduled within cap — pure-string proof held"
    return
  fi
  local worker_matched="yes" interactive_matched="no" repl_matched="no"
  pgrep -f "$pat" | grep -qw "$ipid" && interactive_matched="yes"
  pgrep -f "$pat" | grep -qw "$rpid" && repl_matched="yes"
  kill "$wpid" "$ipid" "$rpid" 2>/dev/null || true
  wait "$wpid" "$ipid" "$rpid" 2>/dev/null || true
  if [[ "$worker_matched" == "yes" && "$interactive_matched" == "no" && "$repl_matched" == "no" ]]; then
    pass "proc-probe: matches -p --agent flow-lead worker, excludes interactive lead + plain REPL"
  else
    fail "proc-probe: worker=$worker_matched interactive=$interactive_matched repl=$repl_matched"
  fi
}

# ---------------------------------------------------------------------------
# Test 27 (#2670 / roborev 1810 HIGH, 1839): a `leftover-worker` preflight hold (an
# orphaned worker CLI) that NEVER clears must STOP the supervisor loudly
# (leftover-worker, exit 1, high page naming survivors) after the TIGHT
# LEFTOVER_HOLD_MAX passes — not latch it silently until MAX_HOURS.
# ---------------------------------------------------------------------------
test_leftover_hold_bounded_stops() {
  local d counter jf rc page
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  # Worker-family probe never clears (always reports an orphaned worker CLI); the
  # worker would finalize if ever spawned (it must not be).
  export PROC_PROBE_WORKER_CMD="echo 1"
  export PROC_LIST_WORKER_CMD="echo '12345 claude --agent worker orphan'"
  export LEFTOVER_HOLD_MAX=3
  export HOLD_POLL_SECS=1
  export MAX_HOURS=8
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  page=$(grep -c '^error|worker-supervisor: leftover worker CLI will not clear' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && ! -f "$counter" && "$page" -ge 1 ]] &&
     grep -q '"reason":"leftover-worker"' "$jf" &&
     grep -q '12345' "$NOTIFY_LOG"; then
    pass "leftover-worker bound: never-clearing worker orphan stops loudly (exit 1, survivors named), no spawn"
  else
    fail "leftover-worker: rc=$rc spawned=$([[ -f "$counter" ]] && echo yes || echo no) page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 28 (#2670 / roborev 1810 MED): UNVERIFIED_MAX consecutive unverified
# finalizes STOP the supervisor (verify-unavailable, exit 1, high page) — a
# persistent verification outage must not let uncounted-forever iterations drift
# past the MAX_ISSUES ceiling.
# ---------------------------------------------------------------------------
test_persistent_unverified_stops() {
  local d counter jf rc ucount page
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  write_gh_transport_fail_stub "$d/bin/gh.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export UNVERIFIED_MAX=2
  # shellcheck disable=SC2016  # $1 expanded later by the supervisor's own `bash -c`.
  export GH_VERIFY_CMD="$d/bin/gh.sh \"\$1\""
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  ucount=$(jline_count "$jf" '"outcome":"finalized-unverified"')
  page=$(grep -c '^error|worker-supervisor: verification unavailable' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && "$ucount" -eq 2 && "$page" -ge 1 ]] &&
     grep -q '"reason":"verify-unavailable"' "$jf"; then
    pass "persistent unverified: 2 consecutive stop the loop (verify-unavailable, exit 1, high page)"
  else
    fail "persistent-unverified: rc=$rc unverified=$ucount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 29 (#2670 / roborev 1810 MED): a FORGED `pr` is an escalation, not a blip —
# a gh-not-found number (999999) and a garbage non-numeric string both classify
# mismatch:UNRESOLVED (abnormal, high MISMATCH page, breaker-counting), NEVER
# unverified. BREAKER_N=2 stops after the two forged finalizes.
# ---------------------------------------------------------------------------
test_forged_pr_is_unresolved_mismatch() {
  local d call_ctr jf rc acount ucount page
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_finalize_forged_pr_stub "$d/bin/worker.sh" "$call_ctr"
  write_gh_notfound_stub "$d/bin/gh.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=2
  # shellcheck disable=SC2016  # $1 expanded later by the supervisor's own `bash -c`.
  export GH_VERIFY_CMD="$d/bin/gh.sh \"\$1\""
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  ucount=$(jline_count "$jf" '"outcome":"finalized-unverified"')
  page=$(grep -c '^error|worker-supervisor: finalized MISMATCH' "$NOTIFY_LOG" 2>/dev/null || true)
  # both forged finalizes → mismatch:UNRESOLVED (one via gh not-found, one via
  # shape-check), never unverified; breaker trips at 2.
  if [[ "$rc" -ne 0 && "$acount" -eq 2 && "$ucount" -eq 0 && "$page" -ge 2 ]] &&
     [[ "$(jline_count "$jf" '"verified":"mismatch:UNRESOLVED"')" -eq 2 ]] &&
     grep -q '"reason":"breaker"' "$jf"; then
    pass "forged pr: not-found number + garbage string both mismatch:UNRESOLVED (escalation, not unverified)"
  else
    fail "forged-pr: rc=$rc abnormal=$acount unverified=$ucount mismatch_page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 30 (#2670 / roborev 1810 HIGH): the bounded hold loop re-checks exit
# conditions on EVERY pass — a stop-file created WHILE preflight is holding (a
# non-leftover reason, so the leftover cap is not what stops it) exits cleanly
# from inside the hold loop, never spawning. Deterministic (no timing race): load
# stays pinned high until the stop-file lands.
# ---------------------------------------------------------------------------
test_stop_file_honored_mid_hold() {
  local d counter sup_pid rc hold_notifies
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export LOAD_CONTROL_FILE="$d/load"
  echo 99 >"$LOAD_CONTROL_FILE"
  # shellcheck disable=SC2016  # deferred: expanded later by the supervisor's own `bash -c`.
  export LOAD_PROBE_CMD='cat "$LOAD_CONTROL_FILE"'
  export LOAD_MAX=1

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 &
  sup_pid=$!
  local waited=0
  hold_notifies=0
  while [[ "$waited" -lt 300 ]]; do
    hold_notifies=$(grep -c '^error|worker-supervisor HOLD|HOLD: load' "$NOTIFY_LOG" 2>/dev/null || true)
    [[ "$hold_notifies" -ge 1 ]] && break
    sleep 0.1
    waited=$((waited + 1))
  done
  # stop while still holding (load never cleared)
  touch "$STOP_FILE"
  wait "$sup_pid"
  rc=$?
  if [[ "$rc" -eq 0 && ! -f "$counter" && "$hold_notifies" -ge 1 ]] &&
     grep -q '"reason":"stop-file"' "$JOURNAL_FILE"; then
    pass "stop-file mid-hold: bounded hold loop exits cleanly from inside the hold, no spawn"
  else
    fail "stop-mid-hold: rc=$rc spawned=$([[ -f "$counter" ]] && echo yes || echo no) holds=$hold_notifies (see $JOURNAL_FILE)"
  fi
}

# ---------------------------------------------------------------------------
# Test 31 (#2670 / roborev 1813 MED-HIGH): the DEFAULT proc probe must not count
# its own brace-group `bash -c` wrapper (whose argv carries the pattern text). On
# Linux a naive pattern matches that wrapper → a phantom leftover at EVERY boot,
# hard-stopping every supervisor (macOS `pgrep -f` happens not to, but the bracket
# trick is the portable fix). Proven portably + deterministically: a process whose
# argv holds the LITERAL pattern text (as the wrapper's does) must NOT match the
# bracketed pattern, while a REAL `claude --agent worker` process MUST. Plus a
# sanity run of the verbatim default probe: well-formed, and its worker-Claude
# sub-probe is 0 with no worker running.
test_probe_no_self_match() {
  # Source the ACTUAL pattern from the script (anti-drift, roborev #2841): a naive
  # hardcoded copy would silently desync from an edit to PROC_MATCH_WORKER.
  local pat
  pat="$(env -u PROC_MATCH_WORKER SUP="$SUPERVISOR" bash -c 'source "$SUP"; printf %s "$PROC_MATCH_WORKER"' 2>/dev/null)"
  # PROPERTY proof (pure-string, always runs, deterministic): the bracketed pattern
  # matches a REAL unattended `-p` worker argv, and does NOT match a process whose
  # argv literally contains the bracketed PATTERN TEXT (exactly as the probe's own
  # `bash -c` wrapper does) — `[c]laude` = literal `c`+`laude`; the text `[c]laude`
  # has `c` followed by `]`, so no match. This is the self-exclusion property.
  if ! printf "claude -p --dangerously-skip-permissions --agent flow-lead '/worker'\n" | grep -qE "$pat" ||
       printf 'wrap %s probe\n' "$pat" | grep -qE "$pat"; then
    fail "probe self-match: pure-string regex fails the self-exclusion property"
    return
  fi
  # static sanity: the real DEFAULT per-family probe strings (the ones preflight
  # ACTUALLY executes, roborev 1840) each carry the bracket trick + the $$/$PPID
  # self-exclusion. Source with both family probes unset so a leaked test override can't
  # mask the default; assert the strings, don't execute them.
  local worker_probe build_probe defaulted="no"
  # shellcheck disable=SC2016  # $SUP/$PROC_* expand inside the sub-bash, not here.
  worker_probe="$(env -u PROC_PROBE_WORKER_CMD SUP="$SUPERVISOR" bash -c 'source "$SUP"; printf %s "$PROC_PROBE_WORKER_CMD"' 2>/dev/null)"
  # shellcheck disable=SC2016
  build_probe="$(env -u PROC_PROBE_BUILD_CMD SUP="$SUPERVISOR" bash -c 'source "$SUP"; printf %s "$PROC_PROBE_BUILD_CMD"' 2>/dev/null)"
  [[ "$worker_probe" == *"$pat"* && "$worker_probe" == *'grep -vxF'* &&
     "$build_probe" == *'[c]argo '* && "$build_probe" == *'grep -vxF'* ]] && defaulted="yes"
  if [[ "$defaulted" != "yes" ]]; then
    fail "probe self-match: default per-family probe strings missing bracket trick / self-exclusion: worker='${worker_probe:0:50}' build='${build_probe:0:50}'"
    return
  fi
  # Bonus live check: real `claude -p … --agent flow-lead` stub matches, wrapper-text
  # stub does not. SKIPs (never fails) if the control worker never schedules.
  bash -c "exec -a 'claude -p --dangerously-skip-permissions --agent flow-lead /worker' sleep 30" &
  local wpid=$!
  bash -c "exec -a 'wrap $pat probe' sleep 30" &
  local xpid=$!
  local waited=0 control_up="no"
  while [[ "$waited" -lt 50 ]]; do
    pgrep -f "$pat" | grep -qw "$wpid" && { control_up="yes"; break; }
    sleep 0.1
    waited=$((waited + 1))
  done
  if [[ "$control_up" == "no" ]]; then
    kill "$wpid" "$xpid" 2>/dev/null || true
    wait "$wpid" "$xpid" 2>/dev/null || true
    skip "probe self-match (live): control worker process never scheduled within cap — pure-string proof held"
    return
  fi
  local wrapper_matched="no"
  pgrep -f "$pat" | grep -qw "$xpid" && wrapper_matched="yes"
  kill "$wpid" "$xpid" 2>/dev/null || true
  wait "$wpid" "$xpid" 2>/dev/null || true
  if [[ "$wrapper_matched" == "no" ]]; then
    pass "probe self-match: bracket trick matches a real worker, excludes the wrapper-argv text"
  else
    fail "probe self-match: wrapper-argv text was matched (self-exclusion broken)"
  fi
}

# ---------------------------------------------------------------------------
# Test 32 (#2670 / roborev 1813 MED): a tooling gap (NO json parser present) on a
# VALID gh response must read as `unverified` (transport class), NEVER
# mismatch:UNRESOLVED — a missing parser is our problem, not the worker's forgery.
# Unit-tests verify_finalized_pr under a PATH with jq/python3 removed.
test_parser_absent_is_unverified() {
  local d bindir t src out
  d="$(new_case_dir)"
  bindir="$d/nobin"
  mkdir -p "$bindir"
  # symlink only the tools sourcing + verify_finalized_pr's unverified path need
  # (dirname/date are used at source time); jq AND python3 are deliberately absent.
  for t in bash mktemp cat rm grep dirname date; do
    src="$(command -v "$t" 2>/dev/null)" && ln -s "$src" "$bindir/$t"
  done
  # shellcheck disable=SC2016  # $1 expands inside the sub-bash (source target), not here.
  out="$(PATH="$bindir" \
        GH_VERIFY_CMD='printf %s "{\"state\":\"MERGED\",\"autoMergeRequest\":null}"' \
        MISMATCH_RETRIES=1 MISMATCH_RETRY_WAIT_SECS=0 \
        "$bindir/bash" -c 'source "$1"; verify_finalized_pr 42' _ "$SUPERVISOR" 2>/dev/null)"
  if [[ "$out" == "unverified" ]]; then
    pass "parser-absent: no jq/python3 on a valid response → unverified (tooling gap, not forgery)"
  else
    fail "parser-absent: got '$out' (expected unverified)"
  fi
}

# ---------------------------------------------------------------------------
# Test 33 (#2670 / roborev 1813 MED, 1839): OPEN with auto-merge ARMED is a legitimate
# pending state, not a false finalize — verdict finalized-pending-automerge: NOT
# counted toward MAX_ISSUES immediately, default-priority page, breaker-NEUTRAL. The
# PR is re-verified on the NEXT iteration and, now MERGED, RETROACTIVELY credited
# (pending-credited), reaching MAX_ISSUES=1 — proving the armed PR both wasn't
# double-counted and wasn't lost.
test_pending_automerge_verdict() {
  local d counter gh_ctr jf rc pcount ccount page
  d="$(new_case_dir)"
  counter="$d/counter"
  gh_ctr="$d/gh-calls"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  write_gh_automerge_then_merged_stub "$d/bin/gh.sh" "$gh_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  # shellcheck disable=SC2016  # $1 expanded later by the supervisor's own `bash -c`.
  export GH_VERIFY_CMD="$d/bin/gh.sh \"\$1\""
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  pcount=$(jline_count "$jf" '"outcome":"finalized-pending-automerge"')
  ccount=$(jline_count "$jf" '"outcome":"pending-credited"')
  page=$(grep -c 'finalized PENDING AUTO-MERGE' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$pcount" -eq 1 && "$ccount" -eq 1 && "$page" -ge 1 ]] &&
     grep -q '"outcome":"finalized-pending-automerge".*"verified":"pending-automerge"' "$jf" &&
     grep -q '"outcome":"pending-credited".*"verified":"merged"' "$jf" &&
     grep -q '"reason":"budget-issues"' "$jf" &&
     ! grep -q '"reason":"breaker"' "$jf"; then
    pass "pending-automerge: armed → not counted yet, then retroactively credited on MERGED (breaker-neutral)"
  else
    fail "pending-automerge: rc=$rc pending=$pcount credited=$ccount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 34 (#2670 / roborev 1813 MED): mismatch grace absorbs read-after-merge lag —
# gh reports OPEN on read 1, MERGED on read 2, so the verdict is `merged` (counted),
# never a spurious mismatch. Proves the retry re-reads gh (call counter reaches 2).
test_mismatch_grace_absorbs_lag() {
  local d counter gh_ctr jf rc fcount acount calls
  d="$(new_case_dir)"
  counter="$d/counter"
  gh_ctr="$d/gh-calls"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  write_gh_open_then_merged_stub "$d/bin/gh.sh" "$gh_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  export MISMATCH_RETRIES=3
  export MISMATCH_RETRY_WAIT_SECS=0
  # shellcheck disable=SC2016  # $1 expanded later by the supervisor's own `bash -c`.
  export GH_VERIFY_CMD="$d/bin/gh.sh \"\$1\""
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  calls=$(cat "$gh_ctr" 2>/dev/null || echo -1)
  if [[ "$rc" -eq 0 && "$fcount" -eq 1 && "$acount" -eq 0 && "$calls" -ge 2 ]] &&
     grep -q '"verified":"merged"' "$jf"; then
    pass "mismatch grace: OPEN-then-MERGED across a retry → merged, no spurious mismatch (gh read ${calls}x)"
  else
    fail "mismatch-grace: rc=$rc finalized=$fcount abnormal=$acount gh_calls=$calls (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 35 (#2670 / roborev 1813 finding 4): a foreign-host PR URL (right path
# shape, wrong host/repo) is a forged reference → mismatch:UNRESOLVED (abnormal,
# high page), never merged. BREAKER_N=1 stops on the single forged finalize.
test_foreign_url_is_unresolved() {
  local d jf rc acount page
  d="$(new_case_dir)"
  common_env "$d"
  write_finalize_foreign_url_stub "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  page=$(grep -c '^error|worker-supervisor: finalized MISMATCH' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && "$acount" -eq 1 && "$page" -ge 1 ]] &&
     grep -q '"verified":"mismatch:UNRESOLVED"' "$jf" &&
     grep -q '"reason":"breaker"' "$jf"; then
    pass "foreign URL: non-pmcfadin/cqlite PR URL → mismatch:UNRESOLVED (escalation), never merged"
  else
    fail "foreign-url: rc=$rc abnormal=$acount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 36 (#2670 / roborev 1813 finding 5, 1839): leftover-worker holds are counted
# CUMULATIVELY across the invocation — a transient load blip interleaved between
# leftover holds must NOT reset the bound. Alternating load(high)/leftover holds
# still trip the leftover-worker bound and stop the loop. (With the pre-fix reset, the
# leftover tally would zero on each load pass and never trip.)
test_alternating_holds_still_bounded() {
  local d counter jf rc page
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  # load probe toggles high/low each poll via a counter file; worker probe always
  # reports a leftover. preflight checks load BEFORE procs, so odd polls hold on
  # `load`, even polls hold on `leftover-worker` — never clearing to a spawn.
  export LOAD_CONTROL_FILE="$d/loadctr"
  echo 0 >"$LOAD_CONTROL_FILE"
  # shellcheck disable=SC2016  # deferred: expanded later by the supervisor's own `bash -c`.
  export LOAD_PROBE_CMD='n=$(cat "$LOAD_CONTROL_FILE"); n=$((n+1)); echo "$n" >"$LOAD_CONTROL_FILE"; if [ $((n % 2)) -eq 1 ]; then echo 99; else echo 0; fi'
  export LOAD_MAX=1
  export PROC_PROBE_WORKER_CMD="echo 1"
  export PROC_LIST_WORKER_CMD="echo '999 claude --agent worker orphan'"
  export LEFTOVER_HOLD_MAX=2
  export HOLD_POLL_SECS=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  page=$(grep -c '^error|worker-supervisor: leftover worker CLI will not clear' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && ! -f "$counter" && "$page" -ge 1 ]] &&
     grep -q '"reason":"leftover-worker"' "$jf"; then
    pass "alternating holds: leftover-worker tally is cumulative across a load blip → still bounded (stops)"
  else
    fail "alternating-holds: rc=$rc spawned=$([[ -f "$counter" ]] && echo yes || echo no) page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 37 (#2670 / roborev 1819 HIGH, finding 1): a hold entered with ONLY MAX_HOURS
# set (MAX_HOURS_SECS derived, not passed) must NOT spuriously abort budget-wallclock
# from inside the hold loop — proves the derived budget is defined on the hold path.
# Load pinned high, then cleared; the run holds, then finalizes normally.
# ---------------------------------------------------------------------------
test_maxhours_only_hold_no_abort() {
  local d counter jf rc sup_pid waited hold_notifies
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export MAX_HOURS=8
  unset MAX_HOURS_SECS 2>/dev/null || true   # force derivation on the hold path
  export LOAD_CONTROL_FILE="$d/load"; echo 99 >"$LOAD_CONTROL_FILE"
  # shellcheck disable=SC2016  # deferred: expanded later by the supervisor's own `bash -c`.
  export LOAD_PROBE_CMD='cat "$LOAD_CONTROL_FILE"'
  export LOAD_MAX=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 &
  sup_pid=$!
  waited=0
  while [[ "$waited" -lt 300 ]]; do
    hold_notifies=$(grep -c '^error|worker-supervisor HOLD|HOLD: load' "$NOTIFY_LOG" 2>/dev/null || true)
    [[ "${hold_notifies:-0}" -ge 1 ]] && break
    sleep 0.1
    waited=$((waited + 1))
  done
  echo 0 >"$LOAD_CONTROL_FILE"
  waited=0
  while [[ ! -f "$counter" && "$waited" -lt 300 ]]; do sleep 0.1; waited=$((waited + 1)); done
  wait "$sup_pid"
  rc=$?
  if [[ "$rc" -eq 0 && -f "$counter" ]] &&
     grep -q '"outcome":"finalized"' "$jf" &&
     ! grep -q '"reason":"budget-wallclock"' "$jf"; then
    pass "maxhours-only hold: derived budget on hold path, no spurious budget-wallclock abort"
  else
    fail "maxhours-only: rc=$rc counter=$([[ -f "$counter" ]] && echo yes || echo no) (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 38 (#2670 / roborev 1819 HIGH, finding 2): a TRANSPORT error whose stderr
# merely contains "not found" (`dial tcp ... host not found`) must classify
# `unverified`, NOT mismatch:UNRESOLVED — the tightened classifier keys only on
# gh's actual resolve-failure signature, so a DNS/proxy 404 is never read as forgery.
# ---------------------------------------------------------------------------
test_transport_notfound_is_unverified() {
  local d counter jf rc ucount acount
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export UNVERIFIED_MAX=1   # a single unverified stops the loop → deterministic end
  export GH_VERIFY_CMD='echo "dial tcp: lookup github.com: no such host: host not found" >&2; exit 1'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  ucount=$(jline_count "$jf" '"outcome":"finalized-unverified"')
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  if [[ "$ucount" -eq 1 && "$acount" -eq 0 ]] &&
     grep -q '"verified":"unverified"' "$jf" &&
     ! grep -q '"verified":"mismatch:UNRESOLVED"' "$jf" &&
     grep -q '"reason":"verify-unavailable"' "$jf"; then
    pass "transport not-found: DNS/host-not-found stderr → unverified, never forgery"
  else
    fail "transport-notfound: rc=$rc unverified=$ucount abnormal=$acount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 39 (#2670 / roborev 1819 MED, finding 3): with jq ABSENT but python3
# present, verify_finalized_pr falls through to the python3 parser and correctly
# classifies an OPEN+auto-merge-armed response as pending-automerge. Unit-tests the
# function under a PATH with jq removed (python3 kept).
# ---------------------------------------------------------------------------
test_python_only_parser_automerge() {
  local d bindir t src out
  d="$(new_case_dir)"
  bindir="$d/pybin"
  mkdir -p "$bindir"
  # symlink the tools the function needs PLUS python3; jq deliberately absent.
  for t in bash mktemp cat rm grep dirname date sed python3; do
    src="$(command -v "$t" 2>/dev/null)" && ln -s "$src" "$bindir/$t"
  done
  # shellcheck disable=SC2016  # $1 expands inside the sub-bash (source target), not here.
  out="$(PATH="$bindir" \
        GH_VERIFY_CMD='printf %s "{\"state\":\"OPEN\",\"autoMergeRequest\":{\"enabledAt\":\"x\"}}"' \
        MISMATCH_RETRIES=1 MISMATCH_RETRY_WAIT_SECS=0 STOP_FILE=/nonexistent \
        "$bindir/bash" -c 'source "$1"; verify_finalized_pr 42' _ "$SUPERVISOR" 2>/dev/null)"
  if [[ "$out" == "pending-automerge" ]]; then
    pass "python-only parser: jq absent, python3 parses OPEN+auto-merge → pending-automerge"
  else
    fail "python-only: got '$out' (expected pending-automerge)"
  fi
}

# ---------------------------------------------------------------------------
# Test 40 (#2670 / roborev 1819 MED, finding 4): the mismatch-grace retry loop
# honors the stop-file mid-grace — a shutdown request must not wait out the full
# grace. gh always returns OPEN (never merges), MISMATCH_RETRIES large with a 1s
# wait; the stop-file is created while grace is sleeping, and the supervisor exits
# cleanly (stop-file) well under the would-be full grace time.
# ---------------------------------------------------------------------------
test_stop_file_honored_mid_grace() {
  local d counter jf rc sup_pid t0 elapsed waited
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export GH_VERIFY_CMD='printf %s "{\"state\":\"OPEN\",\"mergedAt\":null,\"autoMergeRequest\":null}"'
  export MISMATCH_RETRIES=100
  export MISMATCH_RETRY_WAIT_SECS=1   # would be ~100s of grace without the stop check
  jf="$JOURNAL_FILE"

  t0=$(date +%s)
  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 &
  sup_pid=$!
  # wait until the worker has run (counter written → we're into verify/grace)
  waited=0
  while [[ ! -f "$counter" && "$waited" -lt 100 ]]; do sleep 0.1; waited=$((waited + 1)); done
  sleep 1   # let grace enter its sleep
  touch "$STOP_FILE"
  wait "$sup_pid"
  rc=$?
  elapsed=$(( $(date +%s) - t0 ))
  if [[ "$rc" -eq 0 && "$elapsed" -lt 30 ]] && grep -q '"reason":"stop-file"' "$jf"; then
    pass "stop-file mid-grace: grace loop honors the stop-file, exits in ${elapsed}s (not full grace)"
  else
    fail "stop-mid-grace: rc=$rc elapsed=${elapsed}s (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 41 (#2670 / roborev 1819 MED, 1839): the SAME PR observed OPEN-with-auto-merge-
# armed across PENDING_AUTOMERGE_MAX consecutive observations is auto-merge-stuck — the
# supervisor pages high and STOPS (automerge-stuck, exit 1) rather than looping forever.
# The stub finalizes the SAME fixed PR each iteration; gh always returns OPEN+armed for
# it; PENDING_AUTOMERGE_MAX=2.
# ---------------------------------------------------------------------------
test_persistent_pending_automerge_stops() {
  local d counter jf rc page
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_fixed_pr_finalize_stub "$d/bin/worker.sh" "$counter" 7
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export PENDING_AUTOMERGE_MAX=2
  export PENDING_AUTOMERGE_MIN_SECS=0   # count alone trips; the wall-clock floor is exercised by test 48
  export GH_VERIFY_CMD='printf %s "{\"state\":\"OPEN\",\"mergedAt\":null,\"autoMergeRequest\":{\"enabledAt\":\"x\"}}"'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  page=$(grep -c '^error|worker-supervisor: auto-merge stuck' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && "$page" -ge 1 ]] &&
     grep -q '"reason":"automerge-stuck"' "$jf" &&
     grep -q '/pull/7' "$NOTIFY_LOG"; then
    pass "persistent pending-automerge: SAME PR unmerged x2 stops the loop (automerge-stuck, exit 1, high page)"
  else
    fail "persistent-pending: rc=$rc page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 45 (#2670 / roborev 1839 HIGH): the HEALTHY case — a fast fleet finalizes N
# DISTINCT PRs that are each briefly OPEN+armed then land. Under the per-PR model this
# must NEVER trip automerge-stuck: each distinct PR is retroactively credited toward
# MAX_ISSUES once it reaches MERGED, and the run ends cleanly at budget-issues. (Under
# the old across-PR streak this false-tripped after PENDING_AUTOMERGE_MAX distinct PRs.)
# gh stub is per-PR: OPEN+armed on the FIRST view of a PR (its finalize), MERGED after.
# ---------------------------------------------------------------------------
test_healthy_multi_pr_no_false_stop() {
  local d counter jf rc scount ccount
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"   # distinct incrementing PRs
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=3
  export PENDING_AUTOMERGE_MAX=2   # would false-trip on 2 distinct PRs under the old model
  mkdir -p "$d/ghviews"
  # Per-PR view counter: OPEN+armed on first view, MERGED thereafter.
  # shellcheck disable=SC2016  # $1 expands inside the supervisor's own `bash -c`.
  export GH_VERIFY_CMD='n="${1##*/}"; f="'"$d"'/ghviews/$n"; c=0; [ -f "$f" ] && c=$(cat "$f"); c=$((c+1)); echo "$c">"$f"; if [ "$c" -le 1 ]; then printf %s "{\"state\":\"OPEN\",\"mergedAt\":null,\"autoMergeRequest\":{\"enabledAt\":\"x\"}}"; else printf %s "{\"state\":\"MERGED\",\"mergedAt\":\"x\",\"autoMergeRequest\":null}"; fi'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  ccount=$(jline_count "$jf" '"outcome":"pending-credited"')
  scount=$(grep -c '^error|worker-supervisor: auto-merge stuck' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$ccount" -ge 3 && "$scount" -eq 0 ]] &&
     grep -q '"reason":"budget-issues"' "$jf" &&
     ! grep -q '"reason":"automerge-stuck"' "$jf"; then
    pass "healthy multi-PR: N distinct armed PRs are credited on MERGED, never false-trip automerge-stuck"
  else
    fail "healthy-multi-pr: rc=$rc credited=$ccount stuck-page=$scount (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 46 (#2670 / roborev 1839 HIGH): the self-clearing build/gate family is bounded
# by the LOOSE BUILD_HOLD_MAX, NOT the tight LEFTOVER_HOLD_MAX — a legitimate concurrent
# gate must be waited out, not killed at 15 min. A `leftover-build` hold that never
# clears must survive past LEFTOVER_HOLD_MAX and only stop at BUILD_HOLD_MAX (as
# `leftover-build`, exit 1). LEFTOVER_HOLD_MAX=1 (would stop immediately if it governed
# builds); BUILD_HOLD_MAX=3.
# ---------------------------------------------------------------------------
test_build_hold_uses_loose_bound() {
  local d counter jf rc page holds
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export PROC_PROBE_BUILD_CMD="echo 1"   # a concurrent build/gate that never clears
  export PROC_LIST_BUILD_CMD="echo '4242 cargo test --workspace'"
  export LEFTOVER_HOLD_MAX=1             # tight worker bound — must NOT govern builds
  export BUILD_HOLD_MAX=3               # loose build bound governs
  export HOLD_POLL_SECS=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  page=$(grep -c '^error|worker-supervisor: build/gate processes will not clear' "$NOTIFY_LOG" 2>/dev/null || true)
  # It must have held on leftover-build MORE than LEFTOVER_HOLD_MAX(=1) times before
  # stopping — proving the tight worker bound did NOT govern the build family.
  holds=$(grep -c 'HOLD: leftover-build' "$d/stdout.log" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && ! -f "$counter" && "$page" -ge 1 && "$holds" -ge 2 ]] &&
     grep -q '"reason":"leftover-build"' "$jf" &&
     grep -q '4242' "$NOTIFY_LOG"; then
    pass "build hold: self-clearing family uses the LOOSE BUILD_HOLD_MAX (survives LEFTOVER_HOLD_MAX, stops at BUILD_HOLD_MAX)"
  else
    fail "build-hold-loose: rc=$rc spawned=$([[ -f "$counter" ]] && echo yes || echo no) page=$page holds=$holds (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 47 (#2670 / roborev 1839 HIGH): a concurrent build/gate that CLEARS after a few
# polls (a legitimate gate finishing) must be WAITED OUT — the supervisor then spawns
# the worker, which finalizes normally. Proves the loose build bound doesn't kill a run
# that merely had a gate running. Build probe reports busy for 2 polls then clears.
# ---------------------------------------------------------------------------
test_build_hold_clears_then_proceeds() {
  local d counter jf rc
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BUILD_HOLD_MAX=12   # loose; the build clears well before this
  export HOLD_POLL_SECS=1
  echo 0 >"$d/buildctr"
  # shellcheck disable=SC2016  # expanded later by the supervisor's own `bash -c`.
  export PROC_PROBE_BUILD_CMD='n=$(cat "'"$d"'/buildctr"); n=$((n+1)); echo "$n">"'"$d"'/buildctr"; if [ "$n" -le 2 ]; then echo 1; else echo 0; fi'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  if [[ "$rc" -eq 0 && -f "$counter" ]] &&
     grep -q '"outcome":"finalized"' "$jf" &&
     grep -q '"reason":"budget-issues"' "$jf" &&
     ! grep -q '"reason":"leftover-build"' "$jf"; then
    pass "build hold clears: a concurrent gate that finishes is waited out, then the worker runs"
  else
    fail "build-hold-clears: rc=$rc spawned=$([[ -f "$counter" ]] && echo yes || echo no) (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 48 (#2670 / roborev 1840): the wall-clock floor — a burst of fast no-progress
# iterations must NOT trip automerge-stuck on a PR whose CI simply hasn't finished. The
# same PR is observed OPEN+armed well past PENDING_AUTOMERGE_MAX observations, but with
# PENDING_AUTOMERGE_MIN_SECS set high the run instead ends at MAX_ISSUES/wall-clock, never
# `automerge-stuck`. (Here MAX_ITER_SECS-independent: worker no-work after 1 finalize so
# iterations are instant; MAX_HOURS is the terminating budget.)
# ---------------------------------------------------------------------------
test_pending_time_floor_blocks_fast_stuck() {
  local d counter jf rc stuck
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_fixed_pr_finalize_stub "$d/bin/worker.sh" "$counter" 9
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export PENDING_AUTOMERGE_MAX=2
  export PENDING_AUTOMERGE_MIN_SECS=100000   # far above the test's wall-clock — never met
  export MAX_HOURS_SECS=3                     # terminate cleanly on wall-clock instead
  export GH_VERIFY_CMD='printf %s "{\"state\":\"OPEN\",\"mergedAt\":null,\"autoMergeRequest\":{\"enabledAt\":\"x\"}}"'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  stuck=$(grep -c '"reason":"automerge-stuck"' "$jf" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$stuck" -eq 0 ]] &&
     grep -q '"reason":"budget-wallclock"' "$jf"; then
    pass "pending time-floor: fast repeated observations do NOT trip automerge-stuck before PENDING_AUTOMERGE_MIN_SECS"
  else
    fail "pending-time-floor: rc=$rc stuck=$stuck (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 49 (#2670 / roborev 1840): a tracked armed PR that ends CLOSED-unmerged (auto-
# merge dropped / PR closed) must NOT be swallowed silently — it is the failure this
# feature catches. It re-verifies as a non-merged mismatch on the next iteration and
# fires a HIGH "armed PR did not land" page + a `pending-dropped` journal line. gh:
# PR 1 = OPEN+armed on first view then CLOSED; any later PR = MERGED (so iter2's finalize
# credits toward MAX_ISSUES=1 and the run exits budget-issues deterministically — NOT
# wallclock, so the credit re-verify always runs before the stop).
# ---------------------------------------------------------------------------
test_pending_pr_closed_pages_high() {
  local d counter jf rc page
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  mkdir -p "$d/ghviews"
  # shellcheck disable=SC2016  # $1 expands inside the supervisor's own `bash -c`.
  export GH_VERIFY_CMD='n="${1##*/}"; f="'"$d"'/ghviews/$n"; c=0; [ -f "$f" ] && c=$(cat "$f"); c=$((c+1)); echo "$c">"$f"; if [ "$n" != "1" ]; then printf %s "{\"state\":\"MERGED\",\"mergedAt\":\"x\",\"autoMergeRequest\":null}"; elif [ "$c" -le 1 ]; then printf %s "{\"state\":\"OPEN\",\"mergedAt\":null,\"autoMergeRequest\":{\"enabledAt\":\"x\"}}"; else printf %s "{\"state\":\"CLOSED\",\"mergedAt\":null,\"autoMergeRequest\":null}"; fi'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  page=$(grep -c 'armed PR did not land' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$page" -ge 1 ]] &&
     grep -q '"outcome":"pending-dropped".*"verified":"mismatch:CLOSED"' "$jf"; then
    pass "pending closed: an armed PR that ends CLOSED-unmerged pages HIGH (not silently swallowed)"
  else
    fail "pending-closed: rc=$rc page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 50 (#2670 / roborev 1840): the verification-outage streak is reset ONLY by a
# gh-SUCCESS outcome, NOT by an intervening abnormal/no-work iteration — otherwise a
# persistent gh outage interleaved with unrelated iterations would never trip. Sequence:
# unverified finalize → abnormal → unverified finalize must reach UNVERIFIED_MAX=2 and
# STOP (verify-unavailable). Worker alternates: finalize (odd), crash (even).
# ---------------------------------------------------------------------------
test_unverified_streak_survives_intervening_abnormal() {
  local d counter jf rc
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  # Odd calls write a finalized marker; even calls exit 1 with NO marker (abnormal).
  cat >"$d/bin/worker.sh" <<EOF
#!/usr/bin/env bash
n=0
[[ -f "$counter" ]] && n=\$(cat "$counter")
n=\$((n + 1))
echo "\$n" >"$counter"
if [[ \$((n % 2)) -eq 1 ]]; then
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":\$n,"pr":"https://github.com/pmcfadin/cqlite/pull/\$n","duration_s":1}
JSON
else
  exit 1
fi
EOF
  chmod +x "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100          # do not let the abnormal trip the crash breaker first
  export UNVERIFIED_MAX=2
  export GH_VERIFY_CMD='printf %s "GH DOWN"'   # unparseable ⇒ unverified (transport gap)
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  if [[ "$rc" -ne 0 ]] &&
     grep -q '"reason":"verify-unavailable"' "$jf" &&
     ! grep -q '"reason":"breaker"' "$jf"; then
    pass "unverified streak: an intervening abnormal does NOT reset it — persistent outage still trips verify-unavailable"
  else
    fail "unverified-streak: rc=$rc (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 53 (#2670 / roborev 1843): the deferred automerge-stuck stop. Two tracked PRs in
# the same credit pass — one that MERGES (credited) and one that is STUCK — must leave a
# clean exit report: the stuck PR gets its OWN `finalized-pending-automerge` + HIGH page
# and is NOT re-listed as a generic `pending-at-exit`; the merged PR (resolved earlier in
# the same pass) is likewise never announced as still-pending. gh stub is per-PR: PR 21
# arms once then MERGES; PR 22 stays OPEN+armed forever.
# ---------------------------------------------------------------------------
test_deferred_stuck_stop_clean_exit() {
  local d jf rc pae_stuck pae_merged fpa_stuck stuckpage
  d="$(new_case_dir)"
  common_env "$d"
  # Worker finalizes PR 21 then PR 22 (two distinct armed PRs), then no-work.
  cat >"$d/bin/worker.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
n=0; [[ -f "$d/counter" ]] && n=\$(cat "$d/counter"); n=\$((n+1)); echo "\$n">"$d/counter"
if [[ \$n -eq 1 ]]; then pr=21; elif [[ \$n -eq 2 ]]; then pr=22; else
  printf '{"outcome":"no-work"}' >"\$MARKER_FILE"; exit 0
fi
cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":\$pr,"pr":"https://github.com/pmcfadin/cqlite/pull/\$pr","duration_s":1}
JSON
EOF
  chmod +x "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export PENDING_AUTOMERGE_MAX=2
  export PENDING_AUTOMERGE_MIN_SECS=0
  export BACKOFF_NOWORK_SECS=1
  export MAX_HOURS_SECS=20
  mkdir -p "$d/ghviews"
  # PR 21: OPEN+armed on first view, MERGED after. PR 22: always OPEN+armed (stuck).
  # shellcheck disable=SC2016  # $1 expands inside the supervisor's own `bash -c`.
  export GH_VERIFY_CMD='p="${1##*/}"; f="'"$d"'/ghviews/$p"; c=0; [ -f "$f" ] && c=$(cat "$f"); c=$((c+1)); echo "$c">"$f"; if [ "$p" = "21" ] && [ "$c" -ge 2 ]; then printf %s "{\"state\":\"MERGED\",\"mergedAt\":\"x\",\"autoMergeRequest\":null}"; else printf %s "{\"state\":\"OPEN\",\"mergedAt\":null,\"autoMergeRequest\":{\"enabledAt\":\"x\"}}"; fi'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  pae_stuck=$(grep -c '"outcome":"pending-at-exit".*/pull/22' "$jf" 2>/dev/null || true)
  pae_merged=$(grep -c '"outcome":"pending-at-exit".*/pull/21' "$jf" 2>/dev/null || true)
  fpa_stuck=$(grep -c '"outcome":"finalized-pending-automerge".*/pull/22' "$jf" 2>/dev/null || true)
  stuckpage=$(grep -c '^error|worker-supervisor: auto-merge stuck' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && "$pae_stuck" -eq 0 && "$pae_merged" -eq 0 && "$fpa_stuck" -ge 1 && "$stuckpage" -ge 1 ]] &&
     grep -q '"reason":"automerge-stuck"' "$jf"; then
    pass "deferred stuck stop: stuck PR paged once (not re-listed at exit), merged PR not announced pending (clean exit report)"
  else
    fail "deferred-stuck: rc=$rc pae22=$pae_stuck pae21=$pae_merged fpa22=$fpa_stuck stuckpage=$stuckpage (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 51 (#2670 / roborev 1841): the wall-clock floor is genuinely CROSSED — with
# PENDING_AUTOMERGE_MAX=2 and PENDING_AUTOMERGE_MIN_SECS=2, the same PR observed pending
# holds through the first observations (count reached quickly) and only trips
# automerge-stuck once ~2s of wall-clock have elapsed. Proves the AND actually binds on
# the time term (not just the two degenerate MIN_SECS=0 / huge extremes). Asserts the
# stop IS automerge-stuck and the run lasted >= 2s.
# ---------------------------------------------------------------------------
test_pending_time_floor_crossed_trips() {
  local d counter jf rc elapsed
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  # worker finalizes the same PR each time with a small sleep so the loop doesn't spin
  # thousands of times per second while the 2s floor elapses.
  cat >"$d/bin/worker.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
n=0; [[ -f "$counter" ]] && n=\$(cat "$counter"); n=\$((n+1)); echo "\$n">"$counter"
cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":11,"pr":"https://github.com/pmcfadin/cqlite/pull/11","duration_s":1}
JSON
sleep 0.3
EOF
  chmod +x "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export PENDING_AUTOMERGE_MAX=2
  export PENDING_AUTOMERGE_MIN_SECS=2
  export MAX_HOURS_SECS=30   # backstop so a broken floor can't hang the suite
  export GH_VERIFY_CMD='printf %s "{\"state\":\"OPEN\",\"mergedAt\":null,\"autoMergeRequest\":{\"enabledAt\":\"x\"}}"'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  elapsed=$(grep -o '"reason":"automerge-stuck","issues_done":[0-9]*,"elapsed_s":[0-9]*' "$jf" 2>/dev/null | grep -o 'elapsed_s":[0-9]*' | grep -o '[0-9]*' | tail -1)
  local iters pcount
  iters=$(cat "$counter" 2>/dev/null || echo 0)
  pcount=$(jline_count "$jf" '"outcome":"finalized-pending-automerge"')
  # Trip only after the TIME term (elapsed >= 2s): a `||`-instead-of-`&&` bug (OR a
  # time-term-ignored bug) would trip on the 1st observation at ~0.35s → elapsed<2, caught
  # here. The COUNT term is pinned separately by test 47 (MIN_SECS=0, so only the count can
  # gate the trip). iters/pcount>=2 just confirm the run genuinely accumulated observations.
  if [[ "$rc" -ne 0 ]] &&
     grep -q '"reason":"automerge-stuck"' "$jf" &&
     [[ -n "$elapsed" && "$elapsed" -ge 2 && "$iters" -ge 2 && "$pcount" -ge 2 ]]; then
    pass "pending time-floor crossed: held through $pcount observations (${iters} iters), trips automerge-stuck only after MIN_SECS (elapsed ${elapsed}s)"
  else
    fail "pending-time-floor-crossed: rc=$rc elapsed=${elapsed:-?} iters=$iters pcount=$pcount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 52 (#2670 / roborev 1841/1842): numeric-knob validation is fail-CLOSED for a
# malformed INTEGER knob (a `MAX_HOURS=abc` typo must page + exit 2, never silently
# derive a 0 budget), but fail-OPEN-safe values are honored — a fractional DISK_FLOOR_GB
# (float-compared) is ACCEPTED and the supervisor runs normally.
# ---------------------------------------------------------------------------
test_numeric_knob_validation() {
  local d counter rc page
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  # (a) malformed integer knob → FATAL exit 2, no worker spawn, bad-config page.
  export MAX_HOURS="abc"
  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  page=$(grep -c 'bad config' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 2 || -f "$counter" || "$page" -lt 1 ]]; then
    fail "knob-validation(bad-int): rc=$rc (want 2) spawned=$([[ -f "$counter" ]] && echo yes) page=$page"
    return
  fi
  # (b) fractional DISK_FLOOR_GB is a valid float — the run proceeds and finalizes.
  local d2 counter2
  d2="$(new_case_dir)"
  counter2="$d2/counter"
  common_env "$d2"
  write_finalize_stub "$d2/bin/worker.sh" "$counter2"
  export WORKER_CMD="$d2/bin/worker.sh"
  export MAX_ISSUES=1
  export DISK_FLOOR_GB="37.5"
  bash "$SUPERVISOR" >"$d2/stdout.log" 2>&1
  rc=$?
  if [[ "$rc" -eq 0 && -f "$counter2" ]] &&
     grep -q '"reason":"budget-issues"' "$JOURNAL_FILE"; then
    pass "knob validation: malformed MAX_HOURS fails closed (exit 2, paged); fractional DISK_FLOOR_GB accepted"
  else
    fail "knob-validation(float): rc=$rc spawned=$([[ -f "$counter2" ]] && echo yes || echo no) (see $d2)"
  fi
  # (c) ZERO is not a lax bound for CLAIM_MIGRATION_RETRIES, it is a SILENT SKIP (roborev round 35).
  # A 0 makes the retry loop body never execute, so the legacy claim is never read and the lane runs
  # foreign to its own lock with no error anywhere. It therefore belongs to a strictly-POSITIVE group,
  # unlike the count knobs where 0 is a meaningful value. Found because a harness left it unset — the
  # same failure a plist typo would produce in production, where nothing would be watching.
  local d3 counter3 rc3
  d3="$(new_case_dir)"; counter3="$d3/counter"
  common_env "$d3"
  write_finalize_stub "$d3/bin/worker.sh" "$counter3"
  export WORKER_CMD="$d3/bin/worker.sh"
  export CLAIM_MIGRATION_RETRIES=0
  bash "$SUPERVISOR" >"$d3/stdout.log" 2>&1
  rc3=$?
  if [[ "$rc3" -eq 2 && ! -f "$counter3" ]] &&
     grep -q "CLAIM_MIGRATION_RETRIES" "$d3/stdout.log"; then
    pass "knob validation: CLAIM_MIGRATION_RETRIES=0 fails closed and names the knob (0 would silently skip the migration)"
  else
    fail "knob-validation(zero-retries): rc=$rc3 (want 2) spawned=$([[ -f "$counter3" ]] && echo yes || echo no)"
  fi
  # NON-VACUITY: a positive value is accepted, so (c) is about ZERO and not about the knob being
  # rejected outright.
  local d4 counter4 rc4
  d4="$(new_case_dir)"; counter4="$d4/counter"
  common_env "$d4"
  write_finalize_stub "$d4/bin/worker.sh" "$counter4"
  export WORKER_CMD="$d4/bin/worker.sh"
  export MAX_ISSUES=1
  export CLAIM_MIGRATION_RETRIES=2
  bash "$SUPERVISOR" >"$d4/stdout.log" 2>&1
  rc4=$?
  unset CLAIM_MIGRATION_RETRIES
  if [[ "$rc4" -eq 0 && -f "$counter4" ]]; then
    pass "NON-VACUITY: CLAIM_MIGRATION_RETRIES=2 is accepted and the run proceeds"
  else
    fail "knob-validation(positive-retries): rc=$rc4 spawned=$([[ -f "$counter4" ]] && echo yes || echo no)"
  fi
}

# ---------------------------------------------------------------------------
# Test 42 (#2670 / roborev 1821, 1840): each family's count probe AND its list probe
# DERIVE from that family's shared match pattern (PROC_MATCH_BUILD / PROC_MATCH_WORKER)
# — the "what counts" set and the "what we name" set cannot drift, per family. Source
# with the family probes unset and assert each command string embeds its own pattern.
test_probe_list_derives_from_count_set() {
  local out build worker wprobe bprobe wlist blist
  # shellcheck disable=SC2016  # $SUP/$PROC_* expand inside the sub-bash, not here.
  out="$(env -u PROC_PROBE_WORKER_CMD -u PROC_PROBE_BUILD_CMD -u PROC_LIST_WORKER_CMD -u PROC_LIST_BUILD_CMD SUP="$SUPERVISOR" bash -c '
    # shellcheck disable=SC1090
    source "$SUP"
    printf "%s\n%s\n%s\n%s\n%s\n%s\n" "$PROC_MATCH_BUILD" "$PROC_MATCH_WORKER" "$PROC_PROBE_WORKER_CMD" "$PROC_PROBE_BUILD_CMD" "$PROC_LIST_WORKER_CMD" "$PROC_LIST_BUILD_CMD"' 2>/dev/null)"
  build="$(printf '%s' "$out" | sed -n 1p)"
  worker="$(printf '%s' "$out" | sed -n 2p)"
  wprobe="$(printf '%s' "$out" | sed -n 3p)"
  bprobe="$(printf '%s' "$out" | sed -n 4p)"
  wlist="$(printf '%s' "$out" | sed -n 5p)"
  blist="$(printf '%s' "$out" | sed -n 6p)"
  if [[ -n "$build" && -n "$worker" &&
        "$wprobe" == *"$worker"* && "$wlist" == *"$worker"* &&
        "$bprobe" == *"$build"* && "$blist" == *"$build"* ]]; then
    pass "probe derivation: each family's count + list probe derives from its own match pattern"
  else
    fail "probe-derivation: worker-ok=$([[ "$wprobe" == *"$worker"* && "$wlist" == *"$worker"* ]] && echo y) build-ok=$([[ "$bprobe" == *"$build"* && "$blist" == *"$build"* ]] && echo y)"
  fi
}

# ---------------------------------------------------------------------------
# Test 43 (#2670 / roborev 1821, finding b): MISMATCH_GRACE_CAP_SECS<=0 DISABLES
# the wall-clock cap — grace stays bounded solely by the retry count and must NOT
# be blocked. gh reports OPEN then MERGED; with cap=-1, retries=3, wait=0 the grace
# still retries and resolves `merged` (never a spurious mismatch). Unit-tests
# verify_finalized_pr directly.
test_grace_cap_disabled_semantics() {
  local d ctr out
  d="$(new_case_dir)"
  ctr="$d/gh-calls"
  cat >"$d/gh.sh" <<EOF
#!/usr/bin/env bash
n=0; [[ -f "$ctr" ]] && n=\$(cat "$ctr"); n=\$((n + 1)); echo "\$n" >"$ctr"
if [[ \$n -eq 1 ]]; then printf %s '{"state":"OPEN","autoMergeRequest":null}'
else printf %s '{"state":"MERGED","autoMergeRequest":null}'; fi
EOF
  chmod +x "$d/gh.sh"
  # shellcheck disable=SC2016  # $1 expands inside the sub-bash, not here.
  out="$(GH_VERIFY_CMD="$d/gh.sh \"\$1\"" \
        MISMATCH_RETRIES=3 MISMATCH_RETRY_WAIT_SECS=0 MISMATCH_GRACE_CAP_SECS=-1 STOP_FILE=/nonexistent \
        bash -c 'source "$1"; verify_finalized_pr 42' _ "$SUPERVISOR" 2>/dev/null)"
  if [[ "$out" == "merged" && "$(cat "$ctr" 2>/dev/null)" -ge 2 ]]; then
    pass "grace cap<=0: disabled ceiling, grace stays count-bounded (OPEN→MERGED resolves merged)"
  else
    fail "grace-cap-disabled: got '$out' gh_calls=$(cat "$ctr" 2>/dev/null)"
  fi
}

# ---------------------------------------------------------------------------
# Test 44 (#2670 / roborev 1837 MED): a grace loop CUT SHORT by the stop-file
# (a requested shutdown mid-grace) is NOT a confirmed mismatch — the PR state was
# never allowed to settle, so verify_finalized_pr must return the NEUTRAL `aborted`
# verdict, NEVER `mismatch:OPEN` (which the caller turns into an abnormal "worker
# forged a finalize" HIGH page + breaker+1). `aborted` is also distinct from
# `unverified` so an ordinary shutdown cannot accumulate the unverified-outage
# streak. Unit-tests verify_finalized_pr directly with an already-present stop-file.
# ---------------------------------------------------------------------------
test_mid_grace_stop_is_aborted() {
  local d out
  d="$(new_case_dir)"
  touch "$d/stop"
  # shellcheck disable=SC2016  # $1 expands inside the sub-bash, not here.
  local out1
  out="$(GH_VERIFY_CMD='printf %s "{\"state\":\"OPEN\",\"autoMergeRequest\":null}"' \
        MISMATCH_RETRIES=5 MISMATCH_RETRY_WAIT_SECS=0 STOP_FILE="$d/stop" \
        bash -c 'source "$1"; verify_finalized_pr 42' _ "$SUPERVISOR" 2>/dev/null)"
  # roborev 1838: also cover MISMATCH_RETRIES=1 — the loop never reaches the mid-loop
  # guard, so the final-read stop-file re-check is what defuses the forgery verdict.
  out1="$(GH_VERIFY_CMD='printf %s "{\"state\":\"OPEN\",\"autoMergeRequest\":null}"' \
        MISMATCH_RETRIES=1 MISMATCH_RETRY_WAIT_SECS=0 STOP_FILE="$d/stop" \
        bash -c 'source "$1"; verify_finalized_pr 42' _ "$SUPERVISOR" 2>/dev/null)"
  if [[ "$out" == "aborted" && "$out1" == "aborted" ]]; then
    pass "mid-grace stop: shutdown cuts grace short → aborted (neutral; retries=5 AND retries=1)"
  else
    fail "mid-grace-stop: got retries5='$out' retries1='$out1' (expected aborted, NOT mismatch:* / unverified)"
  fi
}

# ---------------------------------------------------------------------------
# Test 24-claim (#2655): the NEXT spawn's claim stamp carries the issue LEARNED from a
# non-finalized (blocked) marker — so the reaper's open-PR guard tracks the real
# issue. Iter 1 blocks issue 88; iter 2's stamp must name issue 88 (before the
# head-block guard stops the run on the 2nd consecutive block).
# ---------------------------------------------------------------------------
test_claim_issue_learned_from_marker() {
  local d rc second_stamp
  d="$(new_case_dir)"
  common_env "$d"
  write_blocked_same_issue_stub "$d/bin/worker.sh" 88
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export CLAIM_LOG="$d/claim.log"
  : >"$CLAIM_LOG"
  write_claim_stub "$d/bin/claim.sh"
  export CLAIM_CMD="bash $d/bin/claim.sh"
  export HEARTBEAT_MACHINE="testbox"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  # Iter1 stamps the `p<pid>` placeholder (issue unknown — it is no longer the shared "0", #3393);
  # iter2 stamps issue 88, learned from iter1's blocked marker. Assert on the ISSUE-NAMED stamp
  # rather than on line position, which is what the property is actually about.
  second_stamp=$(grep -E '^stamp 88 [0-9]+$' "$CLAIM_LOG" 2>/dev/null | head -1)
  # ...and the placeholder it replaced must have been cleared, or the transition leaks a ref that
  # holds a dead pid and dead-lanes reports it as a dead lane forever.
  local placeholder_id placeholder_reaped
  placeholder_id=$(grep -E '^stamp p[0-9]+-[0-9a-f]+ [0-9]+$' "$CLAIM_LOG" 2>/dev/null | head -1 | awk '{print $2}')
  placeholder_reaped=no
  # WITH THE LEASE the stamp reported (roborev round 19): a reap of a lane ref must never run
  # unleased, or a retry landing after another supervisor took the lane id deletes ITS live claim.
  [[ -n "$placeholder_id" ]] && grep -qE "^reap testbox ${placeholder_id} deadbeefdeadbeefdeadbeefdeadbeefdeadbeef\$" "$CLAIM_LOG" 2>/dev/null && placeholder_reaped=yes
  if [[ "$rc" -eq 0 ]] && printf '%s' "$second_stamp" | grep -qE '^stamp 88 [0-9]+$' \
    && [[ "$placeholder_reaped" == "yes" ]]; then
    pass "claim: issue learned from a blocked marker names the next stamp (issue 88), and the p<pid> placeholder ref it replaced was cleared (no leaked ref)"
  else
    fail "claim-learn: rc=$rc second_stamp='$second_stamp' placeholder='$placeholder_id' reaped=$placeholder_reaped (see $CLAIM_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 25-claim (#3393, roborev round 2): a lane TRANSITION whose replacement stamp FAILS must not
# leave the lane with no claim ref. Deleting the old ref first would open exactly that gap — the
# worker still starts, but dead-lanes and the reaper cannot see it for the whole iteration. So the
# old ref must SURVIVE a failed replacement.
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Test 26-claim (#3393, roborev round 6): the pending-cleanup queue must NEVER delete the lane ref
# just stamped. If cleaning placeholder P fails during P -> issue, P stays queued; a later
# issue -> P transition REFRESHES P and then drains, which without protection deletes that fresh
# CURRENT ref and leaves the running lane unobservable — the failure this change exists to prevent,
# produced by the retry logic that was added to fix a leak.
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Test 27-claim (#3393, roborev round 18): clear_claim must NOT delete a PLACEHOLDER lane ref on an
# ABNORMAL exit. finalize_exit runs on every exit path (breaker, leftover-*, automerge-stuck,
# verify-unavailable), and a `p<pid>` id names no issue, so `reap` cannot consult the open-PR
# safeguard and deletes unconditionally — destroying the only liveness signal of a lane whose worker
# may have claimed an issue and opened a PR before the supervisor ever saw the marker (#2499 reached
# from the other side). A NUMERIC lane id is unaffected: there the guard runs inside reap.
# ---------------------------------------------------------------------------
test_clear_claim_keeps_placeholder_on_abnormal_exit() {
  local d out
  d="$(new_case_dir)"
  common_env "$d"
  export CLAIM_LOG="$d/claim.log"
  cat >"$d/bin/claim.sh" <<'STUBEOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${CLAIM_LOG:?CLAIM_LOG not set}"
[ "${1:-}" = stamp ] && printf 'deadbeefdeadbeefdeadbeefdeadbeefdeadbeef\n'
exit 0
STUBEOF
  chmod +x "$d/bin/claim.sh"
  # FOUR cases. The two NUMERIC ones are the round-23 correction: a numeric lane id used to be cleared
  # on any exit, on the reasoning that reap's open-PR guard makes it safe. It does not — PRE-PR work has
  # no open PR, so the guard passes and the ref is deleted, erasing the only signal that an unfinished
  # lane held that issue. "No open PR" is a correct answer to the wrong question.
  : >"$CLAIM_LOG"
  out="$(
    CLAIM_CMD="bash $d/bin/claim.sh" HEARTBEAT_MACHINE=testbox CLAIM_LOG="$CLAIM_LOG" \
    bash -c '
      source "$1"
      # A real supervisor always holds a lease unless the stamp reported no sha; round 32 makes an
      # empty lease refuse outright, so these legs supply one and the empty case is asserted below.
      CLAIM_STAMPED_SHA="feed0001"
      CLAIM_STAMPED_ISSUE="p777-dead1"; clear_claim 0
      CLAIM_STAMPED_ISSUE="p888-dead2"; clear_claim 1
      CLAIM_STAMPED_ISSUE="4242";       clear_claim 0
      CLAIM_STAMPED_ISSUE="5353";       clear_claim 1
      # ROUND 32: no lease => no automated delete, even when concluded.
      CLAIM_STAMPED_SHA=""
      CLAIM_STAMPED_ISSUE="6464";       clear_claim 1
    ' _ "$SUPERVISOR" 2>&1
  )"
  if printf '%s' "$out" | grep -q 'the work on lane p777-dead1 has not concluded' \
    && printf '%s' "$out" | grep -q 'the work on lane 4242 has not concluded' \
    && ! grep -qE '^reap testbox p777-dead1( |$)' "$CLAIM_LOG" \
    && ! grep -qE '^reap testbox 4242( |$)' "$CLAIM_LOG" \
    && grep -qE '^reap testbox p888-dead2( |$)' "$CLAIM_LOG" \
    && grep -qE '^reap testbox 5353( |$)' "$CLAIM_LOG" \
    && ! grep -qE '^reap testbox 6464' "$CLAIM_LOG" \
    && printf '%s' "$out" | grep -q 'DECLINED for lane 6464: no lease was recorded'; then
    pass "claim: an UNCONCLUDED lane survives regardless of its id shape (placeholder AND numeric), and a concluded one is cleared either way"
  else
    fail "clear-claim-concluded: out=[$out] log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
  # WIRING: finalize_exit must pass the WORK-CONCLUDED state, not a code-derived clean flag. The exit
  # code was the previous discriminator and it is exactly what this round falsified — a clean stop
  # mid-issue must keep the ref just as a breaker must.
  if grep -qE 'clear_claim "\$CLAIM_WORK_CONCLUDED"' "$SUPERVISOR" \
    && ! grep -qE 'clear_claim "\$clean_exit"' "$SUPERVISOR"; then
    pass "claim: finalize_exit passes CLAIM_WORK_CONCLUDED and no longer derives a clean flag from the exit code"
  else
    fail "clear-claim-wiring: finalize_exit must pass \$CLAIM_WORK_CONCLUDED, not an exit-code flag"
  fi
  # ...and the LIFECYCLE must hold, which the round-23 version of this case did not check. It asserted
  # that the shipped file contained particular `case` arms — i.e. it tested a MODEL of the code, and
  # when round 24 moved the assignment to the accept points the model went stale while the property it
  # was standing in for was never being measured at all. Replaced with behaviour.
  #
  # (a) UNCONCLUDED AT SPAWN. The flag must be reset where the ref is stamped, so every path that
  #     returns early — a crash, the stuck watchdog, an early finalize_exit — inherits the SAFE value.
  #     Round 24: it kept its initial 1, so a breaker after abnormal iterations deleted the live ref.
  local spawn_block
  spawn_block="$(sed -n '/^run_iteration()/,/^}/p' "$SUPERVISOR" | sed -n '1,/CLAIM_WORK_CONCLUDED=0/p')"
  if printf '%s' "$spawn_block" | grep -q 'stamp_claim' \
    && printf '%s' "$spawn_block" | grep -q 'CLAIM_WORK_CONCLUDED=0'; then
    pass "claim: run_iteration resets work-concluded to 0 at the stamp, so every early exit inherits the safe value"
  else
    fail "clear-claim-spawn-reset: run_iteration must set CLAIM_WORK_CONCLUDED=0 at/after stamp_claim"
  fi
  # (b) A MALFORMED `finalized` MARKER MUST NOT CONCLUDE THE WORK. Behavioural: the marker claims
  #     success with no pr, the supervisor judges it abnormal, and the lane's ref must SURVIVE.
  #     Round 24: the flag was set from the outcome STRING before that validation ran.
  local d2
  d2="$(new_case_dir)"
  common_env "$d2"
  export CLAIM_LOG="$d2/claim.log"
  : >"$CLAIM_LOG"
  write_finalize_missing_pr_stub "$d2/bin/worker.sh"
  export WORKER_CMD="$d2/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  write_claim_stub "$d2/bin/claim.sh"
  export CLAIM_CMD="bash $d2/bin/claim.sh"
  export HEARTBEAT_MACHINE="testbox"
  bash "$SUPERVISOR" >"$d2/stdout.log" 2>&1 || true
  local stamped reaped_it
  stamped=$(grep -oE '^stamp [^ ]+' "$CLAIM_LOG" 2>/dev/null | head -1 | awk '{print $2}')
  reaped_it=no
  [[ -n "$stamped" ]] && grep -qE "^reap testbox ${stamped}( |$)" "$CLAIM_LOG" 2>/dev/null && reaped_it=yes
  if [[ -n "$stamped" && "$reaped_it" == no ]] \
    && grep -q 'has not concluded' "$d2/stdout.log"; then
    pass "claim: a malformed 'finalized' marker does NOT conclude the work — lane $stamped keeps its ref"
  else
    fail "clear-claim-untrusted-finalize: stamped='$stamped' reaped=$reaped_it log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
  # NON-VACUITY: the run really did stamp a lane and really did reach its exit path, so "no reap" is a
  # DECISION rather than an absence of activity.
  #
  # KEYED ON A SIGNAL THAT EXISTS IN BOTH DIRECTIONS. The first cut looked for the DECLINE message —
  # which only appears when the fix works, so under RED (fix removed) this probe failed too. A
  # non-vacuity check that can only pass when the assertion passes measures nothing; it has to be true
  # of the broken code as well. The journal's `summary` record is written by `finalize_exit` on every
  # exit path, whatever the claim decision was.
  local jf_summary=no
  grep -rqs '"outcome":"summary"' "$d2/logs" 2>/dev/null && jf_summary=yes
  if grep -qE '^stamp ' "$CLAIM_LOG" && [[ "$jf_summary" == yes ]]; then
    pass "NON-VACUITY: the run stamped a lane and journalled an exit summary, so the surviving ref is a decision"
  else
    fail "clear-claim-untrusted-finalize-nonvacuity: stamp=$(grep -cE '^stamp ' "$CLAIM_LOG") summary=$jf_summary"
  fi
}

# ---------------------------------------------------------------------------
# Test 28-claim (#3393, roborev round 19): the single-instance lock must be PER LANE. A
# machine-global default made a second lane exit during lock acquisition, so the per-lane claim refs
# this change adds were unreachable with the documented default invocation — the retracted #1930
# invariant surviving in a second mechanism.
# ---------------------------------------------------------------------------
test_supervisor_lock_is_per_lane() {
  local body a b same
  body="$T_LOCKFN/lockfn.sh"
  mkdir -p "$T_LOCKFN"
  # The functions alone, so the case does not depend on sourcing the whole supervisor. BOTH are needed:
  # `supervisor_lock_path` now BUILDS ON `supervisor_lane_id` (roborev round 34) rather than carrying a
  # second copy of its body, so extracting the lock function alone yields an undefined call and an EMPTY
  # path — which is how this case caught the change, loudly and in the right place.
  # DRIVEN BY `LANE_ID`, THE GIVEN IDENTITY (lead ruling B, 2026-08-30). This case used to drive the
  # lock by REPO_ROOT, because the lock inferred its own identity from the script's location — which is
  # exactly the coincidence the ruling rejected. The PROPERTY is unchanged (distinct lanes get distinct
  # locks; one lane is stable); only the SOURCE of the identity moved, from an inference to a value.
  {
    printf '%s\n' '#!/usr/bin/env bash'
    sed -n '/^supervisor_lane_id()/,/^}/p' "$SUPERVISOR"
    sed -n '/^supervisor_lock_path()/,/^}/p' "$SUPERVISOR"
    printf '%s\n' 'supervisor_lock_path; printf "%s\n" "$SUPERVISOR_LOCK"'
  } >"$body"
  a=$(SUPERVISOR_LOCK="" LANE_ID=lane-1111 REPO_ROOT=/data/lanes/lane-1111 TMPDIR=/tmp bash "$body")
  b=$(SUPERVISOR_LOCK="" LANE_ID=lane-2222 REPO_ROOT=/data/lanes/lane-2222 TMPDIR=/tmp bash "$body")
  same=$(SUPERVISOR_LOCK="" LANE_ID=lane-1111 REPO_ROOT=/data/lanes/lane-1111 TMPDIR=/tmp bash "$body")
  if [[ -n "$a" && -n "$b" && "$a" != "$b" && "$a" == "$same" ]]; then
    pass "claim: two lanes get DIFFERENT default locks and one lane is stable across runs ($a vs $b)"
  else
    fail "lock-per-lane: a=[$a] b=[$b] same=[$same] — two lanes must differ and one lane must be stable"
  fi
  # Two lanes whose directories share a BASENAME must still differ, or the readable half would alias
  # them onto one lock and reintroduce the machine-global failure for the common fleet layout.
  local c e
  c=$(SUPERVISOR_LOCK="" LANE_ID=boxA-lane REPO_ROOT=/data/boxA/lane TMPDIR=/tmp bash "$body")
  e=$(SUPERVISOR_LOCK="" LANE_ID=boxB-lane REPO_ROOT=/data/boxB/lane TMPDIR=/tmp bash "$body")
  if [[ "$c" != "$e" ]]; then
    pass "claim: two distinct LANE_IDs get different locks (the basename coincidence is no longer load-bearing)"
  else
    fail "lock-per-lane-basename: both resolved to [$c]"
  fi
  # An explicit SUPERVISOR_LOCK still wins — the fix must not take the override away.
  local ov
  ov=$(SUPERVISOR_LOCK=/tmp/explicit.lock LANE_ID=lane-1111 REPO_ROOT=/data/lanes/lane-1111 bash "$body")
  if [[ "$ov" == "/tmp/explicit.lock" ]]; then
    pass "claim: an explicit SUPERVISOR_LOCK is still honoured"
  else
    fail "lock-per-lane-override: got [$ov]"
  fi
  # ONE CONSTRUCTION (roborev round 34, Medium): the lock path must be built FROM `supervisor_lane_id`,
  # not from a second copy of its body — two spellings of one identity drift, and the bound added to one
  # would silently not apply to the other.
  # ONE IDENTITY, TWO CONSUMERS (lead ruling B): the lock and the claim actor must BOTH derive from
  # `LANE_ID`. Two independent derivations of "which lane am I" is two things to keep in step, and the
  # one that drifts is found in production. The earlier form of this assert required the lock to call
  # `supervisor_lane_id`; that was the same property when identity was inferred, and is the wrong
  # spelling of it now that identity is given.
  local lock_uses actor_uses
  lock_uses=$(sed -n '/^supervisor_lock_path()/,/^}/p' "$SUPERVISOR" | grep -c 'LANE_ID')
  actor_uses=$(sed -n '/^supervisor_claim_actor()/,/^}/p' "$SUPERVISOR" | grep -c 'LANE_ID')
  if [[ "$lock_uses" -ge 1 && "$actor_uses" -ge 1 ]]; then
    pass "identity: the lock AND the claim actor both derive from the given LANE_ID (one identity, two consumers)"
  else
    fail "identity-drift: lock refs LANE_ID $lock_uses time(s), actor $actor_uses — a consumer re-inferring its own lane identity will drift from the other"
  fi
  # BUILTINS ONLY (#3464 family 2, reintroduced in the first cut of this very fix). Several cases
  # SOURCE the supervisor under a stripped PATH to prove the no-jq/no-python3 paths, so an external
  # tool anywhere in this resolution breaks them. Driven by an EMPTY PATH.
  local stripped
  # `$BASH` is the ABSOLUTE path of the running shell. `PATH="" bash …` cannot find bash itself, so
  # the first cut of this case failed with "bash: No such file or directory" — and its NON-VACUITY
  # control PASSED for that same wrong reason, which is the shape this whole change keeps meeting.
  stripped=$(SUPERVISOR_LOCK="" LANE_ID=lane-1111 REPO_ROOT=/data/lanes/lane-1111 TMPDIR=/tmp PATH="" "$BASH" "$body" 2>&1)
  if [[ "$stripped" == "$a" ]]; then
    pass "claim: the lock path resolves with an EMPTY PATH — builtins only, no tr/cksum/awk"
  else
    fail "lock-per-lane-builtins: with PATH='' got [$stripped], expected [$a] — an external tool crept into the resolution"
  fi
  # NON-VACUITY: the same harness with a deliberately external-tool implementation DOES fail under
  # the stripped PATH, so the case above is a measurement rather than a tautology.
  local ext_body ext_out
  ext_body="$T_LOCKFN/ext.sh"
  {
    printf '%s\n' '#!/usr/bin/env bash'
    printf '%s\n' 'h="$(printf %s "$REPO_ROOT" | cksum | awk "{print \$1}")"'
    printf '%s\n' 'printf "%s\n" "/tmp/x-$h.lock"'
  } >"$ext_body"
  local ext_expected
  ext_expected="/tmp/x-$(printf %s /data/lanes/lane-1111 | cksum | awk '{print $1}').lock"
  # Sanity: WITH a normal PATH the control must produce that value, or the comparison below is
  # meaningless regardless of what the stripped run does.
  local ext_ok
  ext_ok=$(REPO_ROOT=/data/lanes/lane-1111 "$BASH" "$ext_body" 2>/dev/null)
  ext_out=$(REPO_ROOT=/data/lanes/lane-1111 PATH="" "$BASH" "$ext_body" 2>/dev/null)
  if [[ "$ext_ok" == "$ext_expected" && "$ext_out" != "$ext_expected" ]]; then
    pass "NON-VACUITY: an external-tool implementation of the same resolution DOES break under PATH='' (so the builtin case above measures something)"
  else
    fail "NON-VACUITY broken: control with PATH gave [$ext_ok] (expected [$ext_expected]) and with PATH='' gave [$ext_out] — the external-tool control must WORK normally and BREAK stripped, or the builtins assertion proves nothing"
  fi
}

# ---------------------------------------------------------------------------
# Test 29-claim (#3393, roborev round 19): a lane-ref reap must carry the LEASE this supervisor
# stamped, and a lease-not-held result (rc=4) means ownership TRANSFERRED — drop the entry rather
# than retry, because retrying can only delete the new owner's live claim.
# ---------------------------------------------------------------------------
test_claim_cleanup_uses_lease_and_drops_on_transfer() {
  local d out
  d="$(new_case_dir)"
  common_env "$d"
  export CLAIM_LOG="$d/claim.log"
  : >"$CLAIM_LOG"
  # A reap stub that reports rc=4 (lease not held) for lane 77, and success otherwise.
  cat >"$d/bin/claim.sh" <<'STUBEOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${CLAIM_LOG:?CLAIM_LOG not set}"
if [ "${1:-}" = reap ] && [ "${3:-}" = 77 ]; then exit 4; fi
[ "${1:-}" = stamp ] && printf 'deadbeefdeadbeefdeadbeefdeadbeefdeadbeef\n'
exit 0
STUBEOF
  chmod +x "$d/bin/claim.sh"
  out="$(
    CLAIM_CMD="bash $d/bin/claim.sh" HEARTBEAT_MACHINE=testbox CLAIM_LOG="$CLAIM_LOG" \
    bash -c '
      source "$1"
      CLAIM_PENDING_CLEANUP=" 77:cafe1234 88:beef5678 "
      claim_drain_pending_cleanup
      printf "PENDING_AFTER=[%s]\n" "$CLAIM_PENDING_CLEANUP"
    ' _ "$SUPERVISOR" 2>&1
  )"
  if grep -qE '^reap testbox 77 cafe1234$' "$CLAIM_LOG" \
    && grep -qE '^reap testbox 88 beef5678$' "$CLAIM_LOG" \
    && printf '%s' "$out" | grep -q 'pending cleanup of 77 dropped: the lease at cafe1234 is no longer held' \
    && printf '%s' "$out" | grep -q 'PENDING_AFTER=\[\]'; then
    pass "claim: the drain passes each entry's LEASE and DROPS the one whose lease transferred (never retries it)"
  else
    fail "claim-lease-drain: out=[$out] log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
  # CONTROL: an entry whose reap SUCCEEDS is also removed, so the drop above is attributable to the
  # rc=4 branch rather than to "the drain empties the queue regardless".
  : >"$CLAIM_LOG"
  out="$(
    CLAIM_CMD="bash $d/bin/claim.sh" HEARTBEAT_MACHINE=testbox CLAIM_LOG="$CLAIM_LOG" \
    bash -c '
      source "$1"
      CLAIM_PENDING_CLEANUP=" 99:aaa111 "
      claim_drain_pending_cleanup
      printf "PENDING_AFTER=[%s]\n" "$CLAIM_PENDING_CLEANUP"
    ' _ "$SUPERVISOR" 2>&1
  )"
  if grep -qE '^reap testbox 99 aaa111$' "$CLAIM_LOG" \
    && printf '%s' "$out" | grep -q 'stale lane ref 99 cleared (lease held at aaa111)' \
    && printf '%s' "$out" | grep -q 'PENDING_AFTER=\[\]'; then
    pass "claim: a successful leased reap clears the entry and names the lease it held"
  else
    fail "claim-lease-success: out=[$out] log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
  # A NON-TRANSFER FAILURE MUST STILL BE RETAINED — dropping every non-zero rc would turn the lease
  # fix into a ref leak, the mirror mistake (#3464 family 4, fail-shut).
  : >"$CLAIM_LOG"
  cat >"$d/bin/claim-fail.sh" <<'STUBEOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${CLAIM_LOG:?CLAIM_LOG not set}"
[ "${1:-}" = reap ] && exit 3
exit 0
STUBEOF
  chmod +x "$d/bin/claim-fail.sh"
  out="$(
    CLAIM_CMD="bash $d/bin/claim-fail.sh" HEARTBEAT_MACHINE=testbox CLAIM_LOG="$CLAIM_LOG" \
    bash -c '
      source "$1"
      CLAIM_PENDING_CLEANUP=" 55:bbb222 "
      claim_drain_pending_cleanup
      printf "PENDING_AFTER=[%s]\n" "$CLAIM_PENDING_CLEANUP"
    ' _ "$SUPERVISOR" 2>&1
  )"
  if printf '%s' "$out" | grep -q 'PENDING_AFTER=\[ 55:bbb222\]' \
    && printf '%s' "$out" | grep -q 'retained for retry'; then
    pass "claim: an open-PR refusal (rc=3) is RETAINED with its lease, not dropped — only a transfer drops"
  else
    fail "claim-lease-retain: a non-transfer failure must be retained: out=[$out]"
  fi
}

# ---------------------------------------------------------------------------
# Test 30-claim (#3393, roborev round 20, High): a park on `seam1-approval`/`needs-decision` RELEASES
# the issue — it is excluded from the next pickup until the owner answers — so the next spawn must NOT
# be stamped under that issue's ref. It was, which let another lane legitimately resuming the issue
# overwrite the ref and hide a dead supervisor behind it: the collision per-lane refs exist to remove.
# ---------------------------------------------------------------------------
test_park_releases_issue_so_next_lane_is_a_placeholder() {
  local d rc stamps placeholders named
  for reason in needs-decision seam1-approval; do
    d="$(new_case_dir)"
    common_env "$d"
    write_park_stub "$d/bin/worker.sh" 88 "$reason"
    export WORKER_CMD="$d/bin/worker.sh"
    export MAX_ISSUES=100
    export BREAKER_N=100
    export CLAIM_LOG="$d/claim.log"
    : >"$CLAIM_LOG"
    write_claim_stub "$d/bin/claim.sh"
    export CLAIM_CMD="bash $d/bin/claim.sh"
    export HEARTBEAT_MACHINE="testbox"
    bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
    rc=$?
    stamps=$(grep -cE '^stamp ' "$CLAIM_LOG" 2>/dev/null || true)
    placeholders=$(grep -cE '^stamp p[0-9]+-[0-9a-f]+ [0-9]+$' "$CLAIM_LOG" 2>/dev/null || true)
    named=$(grep -cE '^stamp 88 [0-9]+$' "$CLAIM_LOG" 2>/dev/null || true)
    # NON-VACUITY is built in: the run must actually have stamped more than once, or "no stamp names
    # 88" would hold trivially for a supervisor that never reached a second iteration.
    if [[ "$stamps" -ge 2 && "$named" -eq 0 && "$placeholders" -eq "$stamps" ]]; then
      pass "claim: a '$reason' park releases issue 88, so all $stamps stamps are unique placeholders and none names the released issue"
    else
      fail "park-releases-issue ($reason): stamps=$stamps placeholders=$placeholders named=$named rc=$rc log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
    fi
  done
  # CONTROL: a TECHNICAL block (free-text reason, not a park token) must still CARRY the issue forward,
  # or the fix would have thrown away the liveness accuracy it exists to protect. This is the existing
  # claim-learn behaviour, asserted here so the two directions sit side by side.
  d="$(new_case_dir)"
  common_env "$d"
  write_blocked_same_issue_stub "$d/bin/worker.sh" 88
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export CLAIM_LOG="$d/claim.log"
  : >"$CLAIM_LOG"
  write_claim_stub "$d/bin/claim.sh"
  export CLAIM_CMD="bash $d/bin/claim.sh"
  export HEARTBEAT_MACHINE="testbox"
  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 || true
  if grep -qE '^stamp 88 [0-9]+$' "$CLAIM_LOG"; then
    pass "claim: CONTROL — a technical block still carries the issue forward (stamp names 88), so only the park path releases"
  else
    fail "park-releases-issue control: a technical block must still name the issue: log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
}

# ---------------------------------------------------------------------------
# Test 31-claim (#3393, roborev round 25, Medium — a REGRESSION from round 24): an idle shutdown must
# CLEAR the placeholder it stamped. Round 24 reset work-concluded to 0 at the stamp (correct, so early
# exits inherit the safe value) which left `no-work` permanently unconcluded — and placeholders are never
# automatically reaped, so every NORMAL idle shutdown leaked a stale ref that dead-lanes then reported as
# a dead lane. A monitor that fires falsely on every idle stop is one an operator learns to ignore.
# ---------------------------------------------------------------------------
test_no_work_shutdown_clears_its_placeholder() {
  local d out stamped
  d="$(new_case_dir)"
  common_env "$d"
  export CLAIM_LOG="$d/claim.log"
  : >"$CLAIM_LOG"
  # A worker that reports no-work AND asks the loop to stop, so the run is exactly one idle iteration
  # followed by the normal stop-file exit — the commonest shutdown shape on an empty Ready queue.
  cat >"$d/bin/worker.sh" <<'WEOF'
#!/usr/bin/env bash
set -euo pipefail
cat >"$MARKER_FILE" <<JSON
{"outcome":"no-work","issue":null,"pr":null,"duration_s":1}
JSON
: >"${STOP_FILE:?STOP_FILE not set}"
WEOF
  chmod +x "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export BACKOFF_NOWORK_SECS=0
  export MAX_ISSUES=5
  export BREAKER_N=5
  write_claim_stub "$d/bin/claim.sh"
  export CLAIM_CMD="bash $d/bin/claim.sh"
  export HEARTBEAT_MACHINE="testbox"
  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 || true
  stamped=$(grep -oE '^stamp p[0-9]+-[0-9a-f]+' "$CLAIM_LOG" 2>/dev/null | head -1 | awk '{print $2}')
  if [[ -n "$stamped" ]] && grep -qE "^reap testbox ${stamped}( |$)" "$CLAIM_LOG"; then
    pass "claim: a no-work idle shutdown CLEARS the placeholder it stamped ($stamped) — no leaked ref for dead-lanes to misreport"
  else
    fail "no-work-clears-placeholder: stamped='$stamped' log=[$(tr '\n' ';' <"$CLAIM_LOG")] out=[$(tail -5 "$d/stdout.log")]"
  fi
  # NON-VACUITY, true of the BROKEN code too: the run must have stamped a placeholder and journalled an
  # exit summary. Both hold whether or not the clear happens, so this establishes the run did the work
  # rather than that the fix fired.
  local jf_summary=no
  grep -rqs '"outcome":"summary"' "$d/logs" 2>/dev/null && jf_summary=yes
  if [[ -n "$stamped" && "$jf_summary" == yes ]] && grep -rqs '"outcome":"no-work"' "$d/logs"; then
    pass "NON-VACUITY: the run stamped a placeholder, journalled a no-work iteration and reached its exit summary"
  else
    fail "no-work-clears-placeholder-nonvacuity: stamped='$stamped' summary=$jf_summary"
  fi
  # ...and a no-work marker that DOES name an issue must NOT conclude it — a no-work carrying an issue is
  # not evidence that issue finished, so the ref stays.
  local d2 stamped2
  d2="$(new_case_dir)"
  common_env "$d2"
  export CLAIM_LOG="$d2/claim.log"
  : >"$CLAIM_LOG"
  cat >"$d2/bin/worker.sh" <<'WEOF'
#!/usr/bin/env bash
set -euo pipefail
cat >"$MARKER_FILE" <<JSON
{"outcome":"no-work","issue":777,"pr":null,"duration_s":1}
JSON
: >"${STOP_FILE:?STOP_FILE not set}"
WEOF
  chmod +x "$d2/bin/worker.sh"
  export WORKER_CMD="$d2/bin/worker.sh"
  export BACKOFF_NOWORK_SECS=0
  write_claim_stub "$d2/bin/claim.sh"
  export CLAIM_CMD="bash $d2/bin/claim.sh"
  export HEARTBEAT_MACHINE="testbox"
  bash "$SUPERVISOR" >"$d2/stdout.log" 2>&1 || true
  stamped2=$(grep -oE '^stamp [^ ]+' "$CLAIM_LOG" 2>/dev/null | head -1 | awk '{print $2}')
  if [[ -n "$stamped2" ]] && ! grep -qE "^reap testbox ${stamped2}( |$)" "$CLAIM_LOG"; then
    pass "claim: a no-work marker that NAMES an issue does not conclude it — lane $stamped2 keeps its ref"
  else
    fail "no-work-with-issue: lane '$stamped2' must not be cleared: log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
}

# ---------------------------------------------------------------------------
# Test 33-claim (#3393, roborev round 29, Medium — a REGRESSION from round 25's guard): a `no-work`
# iteration must conclude only a PLACEHOLDER lane. Round 25 keyed on the MARKER's issue field, which is
# empty for every no-work — but the STAMPED ref can be a NUMERIC issue carried forward from a prior
# technical block, and concluding that cleared the only liveness signal for a still-unresolved issue.
# ---------------------------------------------------------------------------
test_no_work_does_not_conclude_a_numeric_lane() {
  local d stamped_issue reaped
  d="$(new_case_dir)"
  common_env "$d"
  export CLAIM_LOG="$d/claim.log"
  : >"$CLAIM_LOG"
  # BEHAVIOURAL, not a model of the code. The first cut of this case COPIED the supervisor's `case` arms
  # into the test and classified with the copy — which is exactly the defect round 24 found and fixed
  # here: a test that validates a MODEL stays green when the shipped logic moves. Driven instead through
  # the real loop with a two-phase worker: iteration 1 blocks on issue 88 for a TECHNICAL reason (so the
  # issue is carried forward), iteration 2 reports no-work and asks the loop to stop.
  cat >"$d/bin/worker.sh" <<'WEOF'
#!/usr/bin/env bash
set -euo pipefail
n_file="${LOG_DIR:?LOG_DIR not set}/.phase"
n=0; [[ -f "$n_file" ]] && n=$(cat "$n_file")
n=$((n + 1)); printf '%s' "$n" >"$n_file"
if [[ "$n" -eq 1 ]]; then
  cat >"$MARKER_FILE" <<JSON
{"outcome":"blocked","issue":88,"pr":null,"duration_s":1,"reason":"a technical block, not an owner park"}
JSON
else
  cat >"$MARKER_FILE" <<JSON
{"outcome":"no-work","issue":null,"pr":null,"duration_s":1}
JSON
  : >"${STOP_FILE:?STOP_FILE not set}"
fi
WEOF
  chmod +x "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export BACKOFF_NOWORK_SECS=0
  export MAX_ISSUES=10
  export BREAKER_N=10
  write_claim_stub "$d/bin/claim.sh"
  export CLAIM_CMD="bash $d/bin/claim.sh"
  export HEARTBEAT_MACHINE="testbox"
  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 || true
  stamped_issue=$(grep -cE '^stamp 88 [0-9]+$' "$CLAIM_LOG" 2>/dev/null || true)
  reaped=no
  grep -qE '^reap testbox 88( |$)' "$CLAIM_LOG" 2>/dev/null && reaped=yes
  # The numeric lane must have been stamped (iteration 2 carried issue 88 forward) and must NOT be reaped:
  # a no-work says nothing about an issue this lane is still holding.
  if [[ "$stamped_issue" -ge 1 && "$reaped" == no ]] \
    && grep -q 'has not concluded' "$d/stdout.log"; then
    pass "claim: a no-work after a technical block does NOT conclude the numeric lane (88) it still holds"
  else
    fail "no-work-numeric-lane: stamp88=$stamped_issue reaped=$reaped log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
  # NON-VACUITY, true of the BROKEN code too: the run really did reach a second iteration and a shutdown.
  # Both hold whether or not the fix is present — under the old guard the same run reaps 88 instead.
  local phases jf_summary=no
  phases=$(cat "$d/logs/.phase" 2>/dev/null || echo 0)
  grep -rqs '"outcome":"summary"' "$d/logs" 2>/dev/null && jf_summary=yes
  if [[ "$phases" -ge 2 && "$jf_summary" == yes ]]; then
    pass "NON-VACUITY: the run reached iteration $phases and journalled an exit summary, so the surviving ref is a decision"
  else
    fail "no-work-numeric-lane-nonvacuity: phases=$phases summary=$jf_summary"
  fi
}

# ---------------------------------------------------------------------------
# Test 34-claim (#3393, roborev round 31, Medium): a lane TRANSITION must not queue an unconcluded
# NUMERIC predecessor for reaping. Round 29 protected the shutdown path and left this one — the same
# guard, a second route. Technical block on 88 -> no-work (unconcluded, but CLAIM_ISSUE released) -> the
# next stamp is a placeholder and the transition reaped 88, deleting an unresolved issue's only signal.
#
# THREE iterations are required, which is why this case did not exist before: the defect needs a
# transition AFTER the numeric lane, so a two-iteration run cannot reach it.
# ---------------------------------------------------------------------------
test_transition_keeps_an_unconcluded_numeric_lane() {
  local d reaped88 stamped88 placeholders
  d="$(new_case_dir)"
  common_env "$d"
  export CLAIM_LOG="$d/claim.log"
  : >"$CLAIM_LOG"
  cat >"$d/bin/worker.sh" <<'WEOF'
#!/usr/bin/env bash
set -euo pipefail
n_file="${LOG_DIR:?LOG_DIR not set}/.phase"
n=0; [[ -f "$n_file" ]] && n=$(cat "$n_file")
n=$((n + 1)); printf '%s' "$n" >"$n_file"
case "$n" in
  1)  # technical block: carries issue 88 forward
      cat >"$MARKER_FILE" <<JSON
{"outcome":"blocked","issue":88,"pr":null,"duration_s":1,"reason":"a technical block, not an owner park"}
JSON
      ;;
  2)  # no-work: leaves 88 UNCONCLUDED but releases CLAIM_ISSUE
      cat >"$MARKER_FILE" <<JSON
{"outcome":"no-work","issue":null,"pr":null,"duration_s":1}
JSON
      ;;
  *)  # a third iteration happens, stamping a placeholder — this is the transition under test
      cat >"$MARKER_FILE" <<JSON
{"outcome":"no-work","issue":null,"pr":null,"duration_s":1}
JSON
      : >"${STOP_FILE:?STOP_FILE not set}"
      ;;
esac
WEOF
  chmod +x "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export BACKOFF_NOWORK_SECS=0
  export MAX_ISSUES=10
  export BREAKER_N=10
  write_claim_stub "$d/bin/claim.sh"
  export CLAIM_CMD="bash $d/bin/claim.sh"
  export HEARTBEAT_MACHINE="testbox"
  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 || true
  stamped88=$(grep -cE '^stamp 88 [0-9]+$' "$CLAIM_LOG" 2>/dev/null || true)
  reaped88=no
  grep -qE '^reap testbox 88( |$)' "$CLAIM_LOG" 2>/dev/null && reaped88=yes
  if [[ "$stamped88" -ge 1 && "$reaped88" == no ]] \
    && grep -q 'SKIPPED: its work has not concluded' "$d/stdout.log"; then
    pass "claim: a transition past an UNCONCLUDED numeric lane (88) does not queue it for reaping"
  else
    fail "transition-keeps-numeric: stamp88=$stamped88 reaped88=$reaped88 log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
  # A PLACEHOLDER predecessor must still be collected — the exception exists so the round-5 leak stays
  # fixed, and getting it wrong in the other direction trades one leak for another.
  placeholders=$(grep -cE '^reap testbox p[0-9]+-[0-9a-f]+' "$CLAIM_LOG" 2>/dev/null || true)
  if [[ "$placeholders" -ge 1 ]]; then
    pass "claim: a PLACEHOLDER predecessor is still queued and reaped ($placeholders), so the round-5 leak stays fixed"
  else
    fail "transition-placeholder-still-reaped: no placeholder was reaped: log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
  # NON-VACUITY, true of the BROKEN code too: the run reached a THIRD iteration, which is what makes the
  # transition-after-numeric reachable at all. Under the old code the same run reaps 88 instead.
  local phases
  phases=$(cat "$d/logs/.phase" 2>/dev/null || echo 0)
  if [[ "$phases" -ge 3 ]]; then
    pass "NON-VACUITY: the run reached iteration $phases, so the transition after the numeric lane really occurred"
  else
    fail "transition-keeps-numeric-nonvacuity: only $phases iteration(s) — the transition under test never happened"
  fi
}

# ---------------------------------------------------------------------------
# Test 32-claim (#3393, roborev round 28, Medium): an ENDGAME IN FLIGHT keeps its ref. Owner ruling (b)
# on #2499 semantics — a pending auto-merge PR IS an open PR, and `delete_ref_guarded` already refuses to
# delete an issue-named ref in that state. But `CLAIM_WORK_CONCLUDED` reflects only the LATEST iteration,
# so after a pending-automerge finalize a later no-work/finalize/park set it to 1 and the shutdown cleared
# the lane's ref anyway. "Concluded" is necessary and NOT sufficient: nothing may be pending either.
# ---------------------------------------------------------------------------
test_pending_pr_keeps_the_claim() {
  local d out
  d="$(new_case_dir)"
  common_env "$d"
  export CLAIM_LOG="$d/claim.log"
  : >"$CLAIM_LOG"
  write_claim_stub "$d/bin/claim.sh"
  # Unit-tested deliberately: reaching this state end to end needs a pending-automerge finalize followed
  # by a concluding iteration AND a budget exit, which no existing stub sequences. The invariant is one
  # condition in one function, so it is exercised directly — the approach the parser tests take.
  #
  # THE STAMPED LANE IS AN ISSUE NUMBER, AND THAT NOW MATTERS (roborev round 36). This case originally
  # staged a `p999-abc` PLACEHOLDER, which was incidental to what it asserts — its stated invariant is
  # "a pending auto-merge PR keeps the lane ref", and that is what an ISSUE-numbered lane still does.
  # The PLACEHOLDER path deliberately behaves differently now: keeping a placeholder was a trap, because
  # `should-reap` permanently refuses placeholders, so after the supervisor exited NOTHING could ever
  # clear it. Its protection is transferred to an issue-numbered ref instead, and that path is pinned by
  # `test_placeholder_endgame_protection_transfers` below rather than by weakening this case.
  # Changed the PREMISE to keep the invariant honest — not the assertion to match new behaviour.
  out="$(
    CLAIM_CMD="bash $d/bin/claim.sh" HEARTBEAT_MACHINE=testbox CLAIM_LOG="$CLAIM_LOG" \
    bash -c '
      source "$1"
      CLAIM_STAMPED_ISSUE="88"
      CLAIM_STAMPED_SHA="feed0002"
      PENDING_PR_LIST="4242'$'\t''88'$'\t''1'$'\t''0"
      clear_claim 1          # CONCLUDED=1, but a PR is pending
      printf "AFTER_PENDING=%s\n" "$(grep -c "^reap" "$CLAIM_LOG" 2>/dev/null || echo 0)"
      PENDING_PR_LIST=""
      clear_claim 1          # concluded AND nothing pending => clears
      printf "AFTER_EMPTY=%s\n" "$(grep -c "^reap" "$CLAIM_LOG" 2>/dev/null || echo 0)"
    ' _ "$SUPERVISOR" 2>&1
  )"
  if printf '%s' "$out" | grep -q 'auto-merge PR is still pending' \
    && printf '%s' "$out" | grep -q 'AFTER_PENDING=0' \
    && printf '%s' "$out" | grep -q 'AFTER_EMPTY=1'; then
    pass "claim: a pending auto-merge PR KEEPS the lane ref even when concluded=1, and the same call clears once nothing is pending"
  else
    fail "pending-pr-keeps-claim: out=[$out] log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
  # NON-VACUITY, AND IT MUST HOLD ON THE BROKEN CODE TOO — which the first cut did not. It required
  # `AFTER_EMPTY=1` exactly, but with the fix removed BOTH calls reap, so the count becomes 2 and the
  # probe failed alongside the assertion it was meant to qualify. That is the round-24 rule violated by
  # the very case that cites it. Keyed on "at least one reap happened" instead, which is true whether or
  # not the pending-PR hold is present, so it establishes reachability rather than the fix.
  local reaps_seen
  reaps_seen=$(printf '%s' "$out" | sed -n 's/.*AFTER_EMPTY=\([0-9][0-9]*\).*/\1/p' | head -1)
  if [[ -n "$reaps_seen" && "$reaps_seen" -ge 1 ]]; then
    pass "NON-VACUITY: the reap path IS reachable in this harness (${reaps_seen} reap(s) seen), so AFTER_PENDING=0 is a refusal"
  else
    fail "pending-pr-nonvacuity: the reap path never fires here, so the refusal proves nothing: out=[$out]"
  fi
}

# ---------------------------------------------------------------------------
# Test 25-claim (#3393, roborev round 2, Medium): a lane TRANSITION must not open a liveness GAP. The
# replacement is stamped BEFORE the old ref is deleted, so if the replacement FAILS the OLD ref must
# SURVIVE — a lane with no claim ref at all is invisible to dead-lanes and to the reaper for the whole
# iteration, which is a gap introduced by the leak fix rather than by the leak.
#
# THIS FUNCTION WAS INVOKED AND NEVER DEFINED until roborev round 27 (Medium). The suite reported
# "80 passed, 0 failed" through eleven gates while this case never ran; the `t` wrapper above now makes an
# undefined invocation a failure. The regression it was meant to pin is finally pinned here.
# ---------------------------------------------------------------------------
test_claim_transition_survives_failed_replacement() {
  local d placeholder stamped_issue reaped
  d="$(new_case_dir)"
  common_env "$d"
  export CLAIM_LOG="$d/claim.log"
  : >"$CLAIM_LOG"
  # A technical block (free-text reason, NOT a park token) retains the issue, so iteration 2 attempts the
  # ISSUE-named replacement stamp — which this stub fails, while letting the placeholder stamp succeed.
  write_blocked_same_issue_stub "$d/bin/worker.sh" 88
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  write_claim_stub_failing_issue_stamp "$d/bin/claim.sh"
  export CLAIM_CMD="bash $d/bin/claim.sh"
  export HEARTBEAT_MACHINE="testbox"
  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 || true
  placeholder=$(grep -oE '^stamp p[0-9]+-[0-9a-f]+' "$CLAIM_LOG" 2>/dev/null | head -1 | awk '{print $2}')
  stamped_issue=$(grep -cE '^stamp 88 [0-9]+$' "$CLAIM_LOG" 2>/dev/null || true)
  reaped=no
  [[ -n "$placeholder" ]] && grep -qE "^reap testbox ${placeholder}( |$)" "$CLAIM_LOG" 2>/dev/null && reaped=yes
  # The failed replacement must have been ATTEMPTED (or the case proves nothing), and the old ref must
  # still be there.
  if [[ -n "$placeholder" && "$stamped_issue" -ge 1 && "$reaped" == no ]]; then
    pass "claim: a FAILED replacement stamp leaves the old ref ($placeholder) in place — no liveness gap"
  else
    fail "claim-transition-gap: placeholder='$placeholder' issue_stamp_attempts=$stamped_issue reaped=$reaped log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
  # NON-VACUITY / CONTROL: with a stub whose replacement SUCCEEDS, the old placeholder IS cleared. So the
  # survival above is caused by the failure, not by the transition never happening or by a reap that never
  # runs in this shape.
  local d2 ph2 reaped2
  d2="$(new_case_dir)"
  common_env "$d2"
  export CLAIM_LOG="$d2/claim.log"
  : >"$CLAIM_LOG"
  write_blocked_same_issue_stub "$d2/bin/worker.sh" 88
  export WORKER_CMD="$d2/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  write_claim_stub "$d2/bin/claim.sh"
  export CLAIM_CMD="bash $d2/bin/claim.sh"
  export HEARTBEAT_MACHINE="testbox"
  bash "$SUPERVISOR" >"$d2/stdout.log" 2>&1 || true
  ph2=$(grep -oE '^stamp p[0-9]+-[0-9a-f]+' "$CLAIM_LOG" 2>/dev/null | head -1 | awk '{print $2}')
  reaped2=no
  [[ -n "$ph2" ]] && grep -qE "^reap testbox ${ph2}( |$)" "$CLAIM_LOG" 2>/dev/null && reaped2=yes
  if [[ -n "$ph2" && "$reaped2" == yes ]]; then
    pass "NON-VACUITY: when the replacement SUCCEEDS the old placeholder IS cleared, so the survival above is attributable to the failure"
  else
    fail "claim-transition-gap-control: ph2='$ph2' reaped2=$reaped2 — the control must clear the old ref"
  fi
}

test_claim_drain_never_deletes_current_lane() {
  local d out
  d="$(new_case_dir)"
  common_env "$d"
  export CLAIM_LOG="$d/claim.log"
  : >"$CLAIM_LOG"
  # A reap stub that always FAILS, so a queued cleanup stays queued and the drain keeps retrying it.
  cat >"$d/bin/claim.sh" <<'STUBEOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${CLAIM_LOG:?CLAIM_LOG not set}"
[ "${1:-}" = "reap" ] && exit 1
exit 0
STUBEOF
  chmod +x "$d/bin/claim.sh"

  # UNIT-TESTED, deliberately. Reaching the protected state end to end needs three stamps in the
  # order placeholder -> issue -> placeholder, which requires a worker that blocks on one iteration
  # and finalizes on the next; no existing stub alternates that way, and building one would test the
  # stub more than the invariant. The invariant itself is one function, so it is exercised directly —
  # the same approach the parser tests take with verify_finalized_pr.
  out="$(
    # HEARTBEAT_MACHINE, not CLAIM_MACHINE: sourcing the supervisor DERIVES CLAIM_MACHINE from it,
    # so presetting CLAIM_MACHINE is overwritten at source time and the reap lands on the real hostname.
    CLAIM_CMD="bash $d/bin/claim.sh" HEARTBEAT_MACHINE=testbox \
    CLAIM_LOG="$CLAIM_LOG" \
    bash -c '
      source "$1"
      CLAIM_PENDING_CLEANUP=" p123-abc:aaa111 88:bbb222 "
      # Draining while lane p123-abc is the CURRENT one must skip it and retry only 88.
      claim_drain_pending_cleanup "p123-abc"
      printf "PENDING_AFTER=[%s]\n" "$CLAIM_PENDING_CLEANUP"
      # ROUND 32: a BARE entry (no lease recorded) must be DROPPED, not drained. Round 19 deliberately
      # kept draining those "so an entry queued by an older process is still cleaned" — and that was
      # itself the defect: draining without a lease IS the unleased delete that can remove a
      # successor'"'"'s live claim.
      CLAIM_PENDING_CLEANUP=" 77 "
      claim_drain_pending_cleanup
      printf "BARE_AFTER=[%s]\n" "$CLAIM_PENDING_CLEANUP"
    ' _ "$SUPERVISOR" 2>&1
  )"
  # Three things must hold: the current lane is announced as skipped, it is NOT reaped, and the other
  # id IS retried and retained (its reap failed).
  if printf '%s' "$out" | grep -q 'pending cleanup of p123-abc dropped: it is the lane currently stamped' \
    && ! grep -qE '^reap testbox p123-abc( |$)' "$CLAIM_LOG" \
    && grep -qE '^reap testbox 88 bbb222$' "$CLAIM_LOG" \
    && ! grep -qE '^reap testbox 77' "$CLAIM_LOG" \
    && printf '%s' "$out" | grep -q 'DROPPED: no lease was recorded' \
    && printf '%s' "$out" | grep -q 'BARE_AFTER=\[\]' \
    && printf '%s' "$out" | grep -q 'PENDING_AFTER=\[ 88:bbb222\]'; then
    pass "claim: the drain SKIPS the current lane, retries the other and RETAINS it with its lease on failure, and DROPS a bare leaseless entry"
  else
    fail "claim-drain-current: protection did not hold. out=[$out] log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
}

# ---------------------------------------------------------------------------
# F1 (stale-lock reclaim double-acquire race) note: the fix makes reclaim
# atomic via `mv "$LOCK" "$LOCK.stale.$$" && rm -rf "$LOCK.stale.$$"` instead
# of `rm -rf "$LOCK"; mkdir "$LOCK"`. Reliably reproducing the ORIGINAL race
# requires two processes hitting the reclaim window at the exact same instant
# — any test harness recreation of that is inherently sleep/timing-dependent
# and would be flaky by construction (the class of test this suite explicitly
# avoids per its <30s/no-sleep-loop design goal). Covered by code inspection
# instead: `mv` on the same filesystem is atomic (POSIX rename(2)), so of two
# racers only one `mv "$LOCK" "$LOCK.stale.$$"` can succeed against a given
# stale directory name; the loser's `mv` fails (source already gone) and it
# falls through to its own `mkdir "$LOCK"`, which fails against the winner's
# fresh lock, hitting the existing loud "failed to acquire lock" exit path.
# No test function here by design — see comment in acquire_lock() itself.

# ---------------------------------------------------------------------------
# Test (#2841): the resolved DEFAULT WORKER_CMD (caller does not export one)
# is a headless-executable invocation — source the supervisor with WORKER_CMD
# unset (the source-guard keeps main() from running) and assert the resolved
# value carries `-p`, `--dangerously-skip-permissions`, and `--agent flow-lead`,
# and does NOT name the non-existent `--agent worker`. A future edit that drops
# any of these fails here rather than shipping a silently-broken default.
# ANTI-DRIFT (roborev #2841): also assert PROC_MATCH_WORKER actually MATCHES the
# resolved default WORKER_CMD — a flag reorder or regex edit that desynced the
# orphan-probe pattern from the spawn shape (the #2670 coupling) fails HERE.
# ---------------------------------------------------------------------------
test_default_worker_cmd_is_headless() {
  local resolved pat
  # shellcheck disable=SC2016  # $SUP/$WORKER_CMD expand inside the sub-bash, not here.
  resolved="$(env -u WORKER_CMD SUP="$SUPERVISOR" bash -c 'source "$SUP"; printf %s "$WORKER_CMD"' 2>/dev/null)"
  # shellcheck disable=SC2016  # $SUP/$PROC_MATCH_WORKER expand inside the sub-bash, not here.
  pat="$(env -u WORKER_CMD SUP="$SUPERVISOR" bash -c 'source "$SUP"; printf %s "$PROC_MATCH_WORKER"' 2>/dev/null)"
  if [[ "$resolved" == *' -p '* && "$resolved" == *'--dangerously-skip-permissions'* &&
        "$resolved" == *'--agent flow-lead'* && "$resolved" != *'--agent worker'* ]] &&
     printf '%s' "$resolved" | grep -qE "$pat"; then
    pass "default WORKER_CMD: headless (-p + skip-permissions + --agent flow-lead) AND matched by PROC_MATCH_WORKER"
  else
    fail "default WORKER_CMD: resolved='$resolved' pat='$pat' matched=$(printf '%s' "$resolved" | grep -qE "$pat" && echo yes || echo no)"
  fi
}

# ---------------------------------------------------------------------------
# Test (#2841 / design decision A, R3): a HEALTHY worker whose stub emits stream
# activity to stdout produces a NON-EMPTY iter-N.log (the redirect captures the
# `-p --output-format stream-json --verbose` event stream the watchdog scans),
# so the watchdog is not blinded under `-p`. The existing wedge classifier tests
# (test_genuine_wedge_frozen_is_stuck etc.) cover the frozen-log+signature side.
# ---------------------------------------------------------------------------
test_healthy_worker_iterlog_nonempty() {
  local d counter jf rc fcount logsize
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_verbose_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  logsize=0
  [[ -f "$LOG_DIR/iter-1.log" ]] && logsize=$(wc -c <"$LOG_DIR/iter-1.log" | tr -d ' ')
  if [[ "$rc" -eq 0 && "$fcount" -eq 1 && "$logsize" -gt 0 ]] &&
     grep -q 'tool_use' "$LOG_DIR/iter-1.log"; then
    pass "healthy worker: -p stream activity captured into non-empty iter-1.log ($logsize bytes)"
  else
    fail "healthy iter-log: rc=$rc finalized=$fcount logsize=$logsize (see $LOG_DIR/iter-1.log)"
  fi
}

# ---------------------------------------------------------------------------
# Test (#2849 REGRESSION): setting CLAIM_CMD="" MUST truly disable claim
# stamping — it must NOT be silently re-defaulted back to the real
# claim-heartbeat.sh (git push / gh pr list — network ops). The original defect
# used `${CLAIM_CMD:-default}` (colon), which substitutes the default for an
# EMPTY string too, so common_env's `export CLAIM_CMD=""` hit the real network
# path and a slow/contended origin push or `gh pr list` WEDGED the supervisor —
# the non-deterministic tooling-tests hang. Pinned three ways:
#   (a) sourced with CLAIM_CMD="", the resolved value stays empty. Guarded against
#       a VACUOUS pass (an aborted `source "$SUP"` under `set -euo pipefail` would
#       ALSO print nothing): the sub-bash prints a `MARK:` sentinel, so success is
#       the exact string `MARK:` (empty CLAIM_CMD) — never empty-because-aborted.
#   (b) the config line uses the colonless `${CLAIM_CMD-` form (source-level pin,
#       survives a refactor that moves the resolution).
#   (c) LIVE: a full nasty-reason iteration with CLAIM_CMD="" invokes NO claim
#       command on EITHER path — success (`claim stamped/cleared`) OR failure
#       (`claim stamp/clear failed|declined`, which is what a re-defaulted call
#       WOULD log in a no-push/hermetic env). The run is BOUNDED by a
#       background+poll+kill watchdog (macOS has no `timeout(1)`): a re-introduced
#       slow-network claim path is caught as a wedge (kill + FAIL), not a hang.
# ---------------------------------------------------------------------------
test_claim_cmd_empty_truly_disables_no_network() {
  local resolved cfg_line d jf rc invoked sup_pid waited finished
  # (a) NON-VACUOUS resolved pin: MARK: prefix distinguishes "CLAIM_CMD is empty"
  # from "source aborted and printed nothing" (mirrors the sibling anti-drift pins).
  # shellcheck disable=SC2016  # $SUP/$CLAIM_CMD expand inside the sub-bash, not here.
  resolved="$(env CLAIM_CMD="" SUP="$SUPERVISOR" bash -c 'source "$SUP"; printf "MARK:%s" "$CLAIM_CMD"' 2>/dev/null)"
  # (b) config line uses the colonless default form.
  cfg_line="$(grep -E '^CLAIM_CMD=' "$SUPERVISOR" | head -1)"
  # (c) LIVE, BOUNDED: with CLAIM_CMD="" the supervisor must invoke NO claim command
  # at all. Background it and poll for exit up to a 60s cap; a re-defaulted slow claim
  # path would exceed the cap → kill + FAIL (proving the no-hang property), never a
  # silent suite wedge. The nasty-reason marker still drives a bounded head-blocked stop.
  d="$(new_case_dir)"
  common_env "$d" # sets CLAIM_CMD=""
  write_blocked_nasty_reason_stub "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh" MAX_ISSUES=1 BREAKER_N=1
  jf="$JOURNAL_FILE"
  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 &
  sup_pid=$!
  waited=0
  finished="no"
  while [[ "$waited" -lt 600 ]]; do # 600 * 0.1s = 60s bound
    kill -0 "$sup_pid" 2>/dev/null || { finished="yes"; break; }
    sleep 0.1
    waited=$((waited + 1))
  done
  if [[ "$finished" != "yes" ]]; then
    kill -KILL "$sup_pid" 2>/dev/null || true
    wait "$sup_pid" 2>/dev/null || true
    fail "#2849: supervisor did NOT finish within 60s with CLAIM_CMD='' — a re-defaulted claim path is wedging it (see $d/stdout.log)"
    return
  fi
  wait "$sup_pid"
  rc=$?
  # Match a claim invocation on BOTH the success AND failure log paths: a real
  # claim-heartbeat.sh call in a hermetic/no-push env FAILS and logs a WARN
  # ("claim stamp failed" / "claim clear declined/failed"), which a success-only
  # grep would miss — letting a reintroduced `${CLAIM_CMD:-…}` pass unnoticed.
  invoked="no"
  grep -qiE 'claim (stamped|cleared)|claim (stamp|clear) (failed|declined)' "$d/stdout.log" && invoked="yes"
  if [[ "$resolved" == "MARK:" && "$cfg_line" == *'${CLAIM_CMD-'* && "$cfg_line" != *'${CLAIM_CMD:-'* &&
        "$rc" -eq 0 && "$invoked" == "no" ]] && grep -q '"outcome":"blocked"' "$jf"; then
    pass "#2849: CLAIM_CMD='' truly disables claim stamping (no network, no re-default); nasty run completes within 60s bound"
  else
    fail "#2849: resolved='$resolved' cfg='$cfg_line' rc=$rc claim_invoked=$invoked (see $d/stdout.log)"
  fi
}

# ---------------------------------------------------------------------------
# Test (#2849 HERMETICITY, documented + enforced): every REAL pgrep process-table
# scan in THIS suite matches the whole host, so on a dev box concurrently running
# Claude Code / a gate (cargo|nextest|gate_slot_daemon) it WILL match host
# processes. Each such line MUST therefore scope its assertion to the test's OWN
# spawned PID via `grep -qw "$...pid"` on the same line — never assert on a bare
# match count and never block on a host match. This meta-check fails if a future
# edit adds an un-PID-scoped real pgrep scan, re-introducing host contamination.
# The scan matches `pgrep` + any flag group containing `f` (`-f`, `-af`, `-fl`,
# `-lf`) in ANY position (`if pgrep`, `out="$(pgrep …)"`, `while ! pgrep`) on a
# NON-comment line, so it is not fooled by a form other than a line-leading
# `pgrep -f`. (Its own pass/fail text says "pgrep process scan" — no `-flag` —
# and the pattern literal has no whitespace after `pgrep`, so neither self-matches.)
# ---------------------------------------------------------------------------
test_real_pgrep_usages_are_pid_scoped() {
  local bad="" line
  # Strip comment lines (first non-blank char `#`), then flag any real pgrep scan
  # whose line does not PID-scope via `grep -qw`.
  # TWO acceptable scopings, and `pgrep-lint-allow` is not a blanket exemption — it asserts the SECOND:
  #   * `grep -qw $pid`      — pid-scoped: the scan can only match a pid this test owns.
  #   * a RUN-UNIQUE MARKER  — the pattern contains a token minted for this run ($$ + $RANDOM), so no
  #                            host process can carry it. Used by the probe two-direction control, which
  #                            must exercise the REAL pgrep pipeline and therefore cannot pid-scope it.
  # Both bound the scan to this test's own processes, which is the property #2849 is about.
  while IFS= read -r line; do
    [[ "$line" == *'grep -qw'* || "$line" == *'pgrep-lint-allow'* ]] || bad="${bad}${line}\n"
  done < <(grep -vE '^[[:space:]]*#' "${BASH_SOURCE[0]}" | grep -E 'pgrep[[:space:]]+-[a-zA-Z]*f')
  if [[ -z "$bad" ]]; then
    pass "#2849: every real pgrep process scan is PID-scoped (grep -qw \$pid) — hermetic vs host processes"
  else
    fail "#2849: un-PID-scoped real pgrep process scan(s) can match host processes:\n$(printf '%b' "$bad")"
  fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
echo "=== worker-supervisor test suite ==="
t test_happy_path_budget_stop
t test_breaker_stops_on_abnormal
t test_stop_file_honored
t test_preflight_load_hold
t test_nowork_not_counted
t test_single_instance_lock
t test_stale_marker_removed
t test_repeated_blocked_head_of_queue_stops
t test_finalized_missing_pr_is_abnormal
t test_journal_escapes_nasty_reason
t test_park_seam1_parked_on_owner
t test_park_needs_decision_question_in_title
t test_unknown_outcome_is_abnormal
t test_stuck_on_question_detected
t test_prompt_signature_grep
t test_stuck_breaks_abnormal_chain
t test_repeated_park_same_issue_stops
t test_different_issue_parks_do_not_head_block
t test_stray_signature_scrollback_is_abnormal
t test_genuine_wedge_frozen_is_stuck
t test_busy_writing_signature_not_stuck
t test_fast_exit_latency
t test_claim_stamp_each_iter_and_clear_on_exit
t test_claim_issue_learned_from_marker
t test_claim_transition_survives_failed_replacement
t test_claim_drain_never_deletes_current_lane
t test_clear_claim_keeps_placeholder_on_abnormal_exit
t test_supervisor_lock_is_per_lane
t test_claim_cleanup_uses_lease_and_drops_on_transfer
t test_park_releases_issue_so_next_lane_is_a_placeholder
t test_no_work_shutdown_clears_its_placeholder
t test_no_work_does_not_conclude_a_numeric_lane
t test_transition_keeps_an_unconcluded_numeric_lane
t test_pending_pr_keeps_the_claim
t test_finalized_verified_merged_counts
t test_finalized_mismatch_open_is_abnormal
t test_finalized_unverified_not_counted_no_breaker
t test_proc_probe_discriminates_worker_claude
t test_leftover_hold_bounded_stops
t test_persistent_unverified_stops
t test_forged_pr_is_unresolved_mismatch
t test_stop_file_honored_mid_hold
t test_probe_no_self_match
t test_parser_absent_is_unverified
t test_pending_automerge_verdict
t test_mismatch_grace_absorbs_lag
t test_foreign_url_is_unresolved
t test_alternating_holds_still_bounded
t test_maxhours_only_hold_no_abort
t test_transport_notfound_is_unverified
t test_python_only_parser_automerge
t test_stop_file_honored_mid_grace
t test_persistent_pending_automerge_stops
t test_healthy_multi_pr_no_false_stop
t test_pending_time_floor_blocks_fast_stuck
t test_pending_pr_closed_pages_high
t test_unverified_streak_survives_intervening_abnormal
t test_deferred_stuck_stop_clean_exit
t test_pending_time_floor_crossed_trips
t test_numeric_knob_validation
t test_build_hold_uses_loose_bound
t test_build_hold_clears_then_proceeds
t test_probe_list_derives_from_count_set
t test_grace_cap_disabled_semantics
t test_mid_grace_stop_is_aborted
t test_default_worker_cmd_is_headless
t test_healthy_worker_iterlog_nonempty
t test_claim_cmd_empty_truly_disables_no_network
t test_real_pgrep_usages_are_pid_scoped
t test_default_notify_path_publishes

# ---------------------------------------------------------------------------
# Test 29-claim (#3393, roborev round 33 High): the claim lock's holder identity is machine+ACTOR, and
# every lane defaulted to the shared actor `flow`. Harmless while a machine-global lock made a second
# lane impossible; THIS change made the lock per-lane, so two default lanes can now run and each would
# read the other's claim as its own (`verify` false-positive / `release` cross-delete). Removing the
# coarse guard exposed the finer defect it was masking.
# ---------------------------------------------------------------------------
test_claim_actor_is_lane_unique() {
  local body a b same c e
  body="$T_LOCKFN/actorfn.sh"
  mkdir -p "$T_LOCKFN"
  {
    printf '%s\n' '#!/usr/bin/env bash'
    printf '%s\n' 'log() { :; }'
    sed -n '/^supervisor_lane_id()/,/^}/p' "$SUPERVISOR"
    sed -n '/^supervisor_claim_actor()/,/^}/p' "$SUPERVISOR"
    # Read it back out of the ENVIRONMENT, not the shell variable: the worker that calls claim.sh is a
    # CHILD process, so a merely-set value would leave it on the shared default. `env` is the assertion.
    # THE PREMISE HAS TO BE **UNSET**, NOT EMPTY. `CLAIM_ACTOR="" cmd` marks the name EXPORTED in the
    # child's environment, so a later plain assignment propagates to grandchildren with no `export` at
    # all — the first cut of this case staged it that way and its own RED did not fire, because the
    # assert was true of the un-exported code too. `unset` is a builtin, so it survives PATH="".
    printf '%s\n' '[[ "${T_UNSET_ACTOR:-}" == 1 ]] && unset CLAIM_ACTOR'
    printf '%s\n' 'supervisor_claim_actor; "$BASH" -c '"'"'printf "%s\n" "${CLAIM_ACTOR:-UNSET-IN-CHILD}"'"'"''
  } >"$body"
  # FROM `LANE_ID` (lead ruling B): the actor no longer re-infers the lane from REPO_ROOT.
  a=$(T_UNSET_ACTOR=1 LANE_ID=lane-1111 "$BASH" "$body")
  b=$(T_UNSET_ACTOR=1 LANE_ID=lane-2222 "$BASH" "$body")
  same=$(T_UNSET_ACTOR=1 LANE_ID=lane-1111 "$BASH" "$body")
  if [[ "$a" != "UNSET-IN-CHILD" && "$a" != "$b" && "$a" == "$same" ]]; then
    pass "claim-actor: EXPORTED to the child, derived from the GIVEN LANE_ID, and stable ($a vs $b)"
  else
    fail "claim-actor: a=[$a] b=[$b] same=[$same] — must reach the child, differ per lane, and be stable"
  fi
  c=$(T_UNSET_ACTOR=1 LANE_ID=boxA-lane "$BASH" "$body")
  e=$(T_UNSET_ACTOR=1 LANE_ID=boxB-lane "$BASH" "$body")
  if [[ "$c" != "$e" ]]; then
    pass "claim-actor: two distinct LANE_IDs get different actors"
  else
    fail "claim-actor-basename: both resolved to [$c]"
  fi
  # claim.sh REFUSES an actor with fewer than 3 recordable characters, so a degenerate value would be a
  # fail-closed claim rather than an alias. Assert the shape the lock will actually accept.
  if [[ "${#a}" -ge 3 && "$a" == flow-* && "$a" != *[!A-Za-z0-9._-]* ]]; then
    pass "claim-actor: recordable single token >=3 chars, claim.sh-acceptable ($a)"
  else
    fail "claim-actor-shape: [$a] is not a recordable single token of >=3 chars"
  fi
  # THE BOUND AND THE ORDER ARE PROPERTIES OF THE FALLBACK DERIVATION, so they are tested THERE.
  # They used to be reached through the actor, because the actor inferred its own lane; after the
  # ruling the actor takes a GIVEN identity, and it is `supervisor_lane_id` — the fallback used when
  # `LANE_ID` is unset — that must stay bounded and hash-first. `claim.sh`'s `sanitize_field` caps a
  # field at 120 chars, so a hash placed LAST is truncatable and two lanes could collapse onto one.
  local lidbody long_a long_b
  lidbody="$T_LOCKFN/lidonly.sh"
  {
    printf '%s\n' '#!/usr/bin/env bash'
    sed -n '/^supervisor_lane_id()/,/^}/p' "$SUPERVISOR"
    printf '%s\n' 'supervisor_lane_id'
  } >"$lidbody"
  long_a=$(REPO_ROOT="/data/lanes/$(printf 'l%.0s' $(seq 1 200))" "$BASH" "$lidbody")
  long_b=$(REPO_ROOT="/data/other/$(printf 'l%.0s' $(seq 1 200))" "$BASH" "$lidbody")
  if [[ "${#long_a}" -le 60 && "$long_a" =~ ^[0-9]+- ]]; then
    pass "fallback-derivation: a 200-char basename yields a bounded, hash-FIRST id (${#long_a} chars) — truncation costs readability, never uniqueness"
  else
    fail "fallback-derivation-bound: [$long_a] is ${#long_a} chars and/or not hash-first"
  fi
  if [[ "$long_a" != "$long_b" ]]; then
    pass "fallback-derivation: two 200-char-basename lanes still derive DIFFERENT ids"
  else
    fail "fallback-derivation-alias: both long lanes derived [$long_a]"
  fi

  # An operator-set actor still wins — the fix must not seize the override.
  local ov
  ov=$(CLAIM_ACTOR=owner-run LANE_ID=lane-1111 "$BASH" "$body")
  if [[ "$ov" == "owner-run" ]]; then
    pass "claim-actor: an explicit CLAIM_ACTOR is still honoured"
  else
    fail "claim-actor-override: got [$ov]"
  fi
  # Builtins only, same reason as the lock path (#3464 family 2): cases source this file under a
  # stripped PATH. `$BASH` absolute, since PATH='' cannot find bash itself.
  local stripped
  stripped=$(T_UNSET_ACTOR=1 LANE_ID=lane-1111 PATH="" "$BASH" "$body" 2>&1)
  if [[ "$stripped" == "$a" ]]; then
    pass "claim-actor: resolves with an EMPTY PATH — builtins only"
  else
    fail "claim-actor-builtins: with PATH='' got [$stripped], expected [$a]"
  fi
  # WIRED, not merely defined: the resolution must run on the documented path before any worker spawn.
  # A helper nothing calls is #3464's check-whose-subject-never-ran family.
  if grep -qE '^[[:space:]]*supervisor_claim_actor$' "$SUPERVISOR"; then
    pass "claim-actor: supervisor_claim_actor is CALLED (not just defined)"
  else
    fail "claim-actor-unwired: supervisor_claim_actor is defined but never invoked"
  fi
}

# ---------------------------------------------------------------------------
# Test 30-claim (#3393, roborev round 33 High): A CONCLUSION IS ABOUT AN ISSUE, AND THE FLAG IS ABOUT A
# LANE. A marker concluding issue 99 set the global flag while the stamped lane was issue 88, so
# `clear_claim` could delete issue 88's liveness ref with its work unresolved.
# ---------------------------------------------------------------------------
test_conclusion_must_match_the_stamped_lane() {
  local body out
  body="$T_LOCKFN/conclfn.sh"
  mkdir -p "$T_LOCKFN"
  {
    printf '%s\n' '#!/usr/bin/env bash'
    sed -n '/^conclusion_matches_stamped_lane()/,/^}/p' "$SUPERVISOR"
    printf '%s\n' 'CLAIM_STAMPED_ISSUE="$1"; if conclusion_matches_stamped_lane "$2"; then echo MATCH; else echo MISMATCH; fi'
  } >"$body"
  # The defect's exact shape: stamped 88, marker concludes 99.
  out=$("$BASH" "$body" 88 99)
  if [[ "$out" == "MISMATCH" ]]; then
    pass "conclusion-lane: stamped 88 + marker concluding 99 is a MISMATCH (ref preserved)"
  else
    fail "conclusion-lane: stamped 88 / marker 99 reported [$out] — the round-33 defect"
  fi
  out=$("$BASH" "$body" 88 88)
  if [[ "$out" == "MATCH" ]]; then
    pass "conclusion-lane: the matching issue still concludes (the fix does not strand the normal path)"
  else
    fail "conclusion-lane: stamped 88 / marker 88 reported [$out] — over-tightened"
  fi
  # A PLACEHOLDER lane has no issue to match, and an EMPTY stamped value means no lease was recorded.
  # Both must stay permissive, or a placeholder iteration could never conclude and its ref would be
  # refused by automated reaping forever (the round-28/31 failure mode, in reverse).
  out=$("$BASH" "$body" p1234 77)
  if [[ "$out" == "MATCH" ]]; then
    pass "conclusion-lane: a PLACEHOLDER stamped lane still concludes (no issue to match)"
  else
    fail "conclusion-lane-placeholder: reported [$out]"
  fi
  out=$("$BASH" "$body" "" 77)
  if [[ "$out" == "MATCH" ]]; then
    pass "conclusion-lane: an EMPTY stamped lane still concludes"
  else
    fail "conclusion-lane-empty: reported [$out]"
  fi
  # WIRED at BOTH accept points. The predicate exists to guard them; guarding one is half a fix.
  local guarded
  guarded=$(grep -cE 'conclusion_matches_stamped_lane' "$SUPERVISOR")
  if [[ "$guarded" -ge 3 ]]; then
    pass "conclusion-lane: the predicate guards both accept points ($guarded references incl. definition)"
  else
    fail "conclusion-lane-unwired: only $guarded reference(s) — definition plus BOTH accept points expected"
  fi
}

# ---------------------------------------------------------------------------
# Test 31-claim (#3393, roborev round 33 High, second half): the RETRACTED #1930 invariant in the
# OPERATIVE worker contract. `.claude/commands/worker.md` is what a `/worker` session actually obeys, so
# leaving "Exactly ONE flow-lead worker runs per machine" there means the second lane STOPS in preflight
# and every mechanism this change adds is unreachable by the documented invocation. Third instance of
# #3464's retracted-invariant-in-a-second-carrier family.
# ---------------------------------------------------------------------------
test_worker_contract_does_not_assert_one_worker_per_machine() {
  local doc="$REPO_ROOT/.claude/commands/worker.md" bad=""
  if [[ ! -r "$doc" ]]; then
    fail "worker-contract: $doc is not readable — the carrier this case exists for is missing"
    return 0
  fi
  # The retracted claim, in the spellings the file used. Comment lines are not a concern: this is
  # markdown, all of it operative.
  while IFS= read -r line; do bad="${bad}${line}\n"; done < <(
    grep -nE 'Exactly ONE flow-lead worker|One worker per machine — you are the sole' "$doc" |
      grep -v 'RETRACTED'
  )
  if [[ -z "$bad" ]]; then
    pass "worker-contract: worker.md no longer asserts the retracted one-worker-per-machine invariant"
  else
    fail "worker-contract: retracted #1930 invariant still live in worker.md:\n$(printf '%b' "$bad")"
  fi
  # The retraction must be POSITIVE, not just an absence — a silent deletion leaves a reader with no
  # statement either way, and #1930 is cited across the fleet docs.
  if grep -q 'RETRACTED by #3393' "$doc"; then
    pass "worker-contract: the retraction is stated explicitly, citing #3393"
  else
    fail "worker-contract: nothing in worker.md records that #1930 was retracted"
  fi
  # AND THE TRUE PARTS MUST SURVIVE. The retraction is scoped to the worker-COUNT invariant; the
  # full-gate concurrency bound is a RESOURCE bound and still holds, and dropping it with the
  # retraction would trade one wrong doc for another.
  if grep -qE 'full-gate concurrency = \*\*1\*\*|full-gate concurrency = 1' "$doc"; then
    pass "worker-contract: the surviving resource bound (full-gate concurrency = 1) is retained"
  else
    fail "worker-contract: the full-gate concurrency bound was dropped along with the retraction"
  fi
  # The actor requirement is the thing that makes multi-lane SAFE, so the contract must name it.
  if grep -q 'CLAIM_ACTOR' "$doc"; then
    pass "worker-contract: worker.md names the per-lane CLAIM_ACTOR requirement"
  else
    fail "worker-contract: multi-lane is now permitted but CLAIM_ACTOR is unmentioned"
  fi
}

t test_claim_actor_is_lane_unique
t test_conclusion_must_match_the_stamped_lane
t test_worker_contract_does_not_assert_one_worker_per_machine

# ---------------------------------------------------------------------------
# Test 32-claim (#3393, roborev round 34 finding 1): the legacy-claim migration. Every claim stamped
# before the lane-actor change carries `actor=flow`, so a lane that resolves a lane-unique actor reads
# its OWN claim as foreign and can neither verify nor non-forcibly release its lock. The migration
# CAS-adopts it — but ONLY on an affirmative reading, and ONLY for the issue this lane's own branch
# names, because on a four-lane box all four legacy claims are textually identical.
# ---------------------------------------------------------------------------
mig_case() {
  # mig_case <status-line-or-FAIL> <actor> <branch-issue> -> echoes the stub's recorded call log
  local status_line="$1" actor="$2" branch_issue="$3" d body repo
  d="$(new_case_dir)"
  repo="$d/lane"
  mkdir -p "$repo" "$d/bin"
  git -C "$repo" init -q 2>/dev/null
  git -C "$repo" checkout -q -b "$branch_issue" 2>/dev/null
  # A REAL COMMIT, because a real lane always has one. The first cut left the repo unborn, and with the
  # old `rev-parse --abbrev-ref HEAD` probe that made the happy path resolve NO branch — so it made no
  # call, and all NINE refusal cases below passed VACUOUSLY (they assert the absence of an adopt, and
  # nothing was called at all). The positive control is the only reason that was visible.
  git -C "$repo" -c user.email=t@t -c user.name=t commit -q --allow-empty -m init 2>/dev/null
  # The stub records every invocation and answers `status` with the staged line.
  cat >"$d/bin/claim.sh" <<STUB
#!/usr/bin/env bash
printf '%s\n' "\$*" >>"$d/calls.log"
if [ "\$1" = status ]; then
  [ "$status_line" = FAIL ] && exit 1
  printf '%s\n' "$status_line"
fi
exit 0
STUB
  chmod +x "$d/bin/claim.sh"
  : >"$d/calls.log"
  {
    printf '%s\n' '#!/usr/bin/env bash'
    printf '%s\n' 'log() { :; }'
    # The KNOBS the function depends on, extracted too (roborev round 35). Leaving them out silently
    # unset CLAIM_MIGRATION_RETRIES, the retry loop body never ran, and the happy path made no call —
    # a green-looking harness hiding a disabled subject. It also revealed the production hazard:
    # the knob is now validated as strictly positive.
    sed -n '/^CLAIM_MIGRATION_SETTLED=/p' "$SUPERVISOR"
    sed -n '/^CLAIM_MIGRATION_RETRIES=/p' "$SUPERVISOR"
    sed -n '/^supervisor_msg_token()/,/^}/p' "$SUPERVISOR"
    sed -n '/^supervisor_migrate_legacy_claim()/,/^}/p' "$SUPERVISOR"
    printf '%s\n' 'supervisor_migrate_legacy_claim'
  } >"$d/mig.sh"
  LOCK_CMD="bash $d/bin/claim.sh" CLAIM_ACTOR="$actor" CLAIM_MACHINE=boxA \
    LEGACY_CLAIM_ACTOR=flow REPO_ROOT="$repo" bash "$d/mig.sh" >/dev/null 2>&1
  cat "$d/calls.log"
}

test_legacy_claim_migration() {
  local sha40 out
  sha40="1111111111111111111111111111111111111111"
  # (a) HAPPY PATH: this machine, legacy actor => CAS-adopt on the exact sha.
  out=$(mig_case "CLAIM: STATUS issue=88 sha=$sha40 machine=boxA actor=flow" flow-9-lane issue-88-x)
  if printf '%s\n' "$out" | grep -q "^adopt 88 --expect $sha40"; then
    pass "legacy-migration: a pre-upgrade claim on THIS machine is CAS-adopted on its exact sha"
  else
    fail "legacy-migration: expected 'adopt 88 --expect $sha40', got:
$out"
  fi
  # (b) A DIFFERENT MACHINE is not ours to take.
  out=$(mig_case "CLAIM: STATUS issue=88 sha=$sha40 machine=boxZ actor=flow" flow-9-lane issue-88-x)
  if ! printf '%s\n' "$out" | grep -q '^adopt'; then
    pass "legacy-migration: a claim held by ANOTHER machine is never adopted"
  else
    fail "legacy-migration-foreign-machine: adopted anyway:
$out"
  fi
  # (c) An actor that is ALREADY lane-scoped needs no migration.
  out=$(mig_case "CLAIM: STATUS issue=88 sha=$sha40 machine=boxA actor=flow-7-other" flow-9-lane issue-88-x)
  if ! printf '%s\n' "$out" | grep -q '^adopt'; then
    pass "legacy-migration: a claim already under a lane actor is left alone (no cross-lane grab)"
  else
    fail "legacy-migration-other-lane: adopted a sibling lane's claim:
$out"
  fi
  # (d) AN UNREADABLE STATUS IS NOT A LICENCE (#3229's affirmative-measurement rule). A failed probe
  # must not reach the adopt; doing nothing costs a diagnosed refusal, guessing costs someone's lock.
  out=$(mig_case FAIL flow-9-lane issue-88-x)
  if ! printf '%s\n' "$out" | grep -q '^adopt'; then
    pass "legacy-migration: an UNREADABLE status does not reach the adopt (affirmative measurement)"
  else
    fail "legacy-migration-unreadable: adopted on a failed probe:
$out"
  fi
  # (e) A branch that names no issue must not even ASK — there is no candidate to migrate.
  out=$(mig_case "CLAIM: STATUS issue=88 sha=$sha40 machine=boxA actor=flow" flow-9-lane main)
  if [[ -z "$out" ]]; then
    pass "legacy-migration: a branch naming no issue makes no claim.sh call at all"
  else
    fail "legacy-migration-no-issue-branch: called claim.sh anyway:
$out"
  fi
  # (f) An OPERATOR-PINNED legacy actor is not ours to migrate away from.
  out=$(mig_case "CLAIM: STATUS issue=88 sha=$sha40 machine=boxA actor=flow" flow issue-88-x)
  if [[ -z "$out" ]]; then
    pass "legacy-migration: an operator-pinned actor=flow is left exactly as the operator set it"
  else
    fail "legacy-migration-pinned: touched a pinned actor:
$out"
  fi
  # (g) A MALFORMED sha cannot be a CAS lease. Adopting on a short sha would either fail or, worse,
  # be interpreted; neither belongs in a lock path.
  out=$(mig_case "CLAIM: STATUS issue=88 sha=1111 machine=boxA actor=flow" flow-9-lane issue-88-x)
  if ! printf '%s\n' "$out" | grep -q '^adopt'; then
    pass "legacy-migration: a malformed sha is not used as a CAS lease"
  else
    fail "legacy-migration-badsha: adopted on a non-40-hex sha:
$out"
  fi
  # (h) A SUBSTRING KEY IS NOT A KEY (#3464 family 6): `notmachine=boxA` must not satisfy `machine`.
  # Staged so the ONLY `machine=` token is a foreign one, with a decoy that ends in our machine name.
  out=$(mig_case "CLAIM: STATUS issue=88 sha=$sha40 notmachine=boxA machine=boxZ actor=flow" flow-9-lane issue-88-x)
  if ! printf '%s\n' "$out" | grep -q '^adopt'; then
    pass "legacy-migration: a decoy 'notmachine=' token does not satisfy the machine match"
  else
    fail "legacy-migration-substring: a decoy key satisfied the machine match:
$out"
  fi
  # (i) THE SEAM GENUINELY DISABLES. `LOCK_CMD=""` must make no call — the colonless default exists
  # precisely so an empty value is not silently replaced by the real network path.
  local d2 repo2
  d2="$(new_case_dir)"; repo2="$d2/lane"; mkdir -p "$repo2"
  git -C "$repo2" init -q 2>/dev/null; git -C "$repo2" checkout -q -b issue-88-x 2>/dev/null
  {
    printf '%s\n' '#!/usr/bin/env bash'
    printf '%s\n' 'log() { :; }'
    sed -n '/^supervisor_msg_token()/,/^}/p' "$SUPERVISOR"
    sed -n '/^supervisor_migrate_legacy_claim()/,/^}/p' "$SUPERVISOR"
    printf '%s\n' 'supervisor_migrate_legacy_claim; echo RETURNED'
  } >"$d2/mig.sh"
  local dis
  dis=$(LOCK_CMD="" CLAIM_ACTOR=flow-9-lane CLAIM_MACHINE=boxA LEGACY_CLAIM_ACTOR=flow \
    REPO_ROOT="$repo2" bash "$d2/mig.sh" 2>&1)
  if [[ "$dis" == "RETURNED" ]]; then
    pass "legacy-migration: LOCK_CMD='' disables the migration and returns cleanly"
  else
    fail "legacy-migration-seam: LOCK_CMD='' produced [$dis]"
  fi
  # WIRED: a migration nothing calls is #3464's check-whose-subject-never-ran.
  if grep -qE '^[[:space:]]*supervisor_migrate_legacy_claim$' "$SUPERVISOR"; then
    pass "legacy-migration: supervisor_migrate_legacy_claim is CALLED (not just defined)"
  else
    fail "legacy-migration-unwired: defined but never invoked"
  fi
}

t test_legacy_claim_migration

# ---------------------------------------------------------------------------
# Test 33-claim (#3393, roborev round 35 High): the worker orphan probe must be attributed to THIS
# LANE. Counting every matching worker on the box made each supervisor read its SIBLINGS' healthy
# workers as leftover debris and stop after LEFTOVER_HOLD_MAX polls — so per-lane claim refs would
# have shipped while multi-lane operation stayed serialized by a different machine-global mechanism.
# ---------------------------------------------------------------------------
test_worker_probe_is_lane_attributed() {
  local d filt lane sib a b c out
  d="$(new_case_dir)"
  lane="$d/lane"; sib="$d/sibling"
  mkdir -p "$lane/sub" "$sib"
  filt="$d/filt.sh"
  {
    printf '%s\n' '#!/usr/bin/env bash'
    printf '%s\n' 'REPO_ROOT="$1"'
    # The SHIPPED filter definition, evaluated with this REPO_ROOT — not a reimplementation of it.
    printf '%s\n' 'eval "$(sed -n "/^LANE_PID_FILTER=/p" "$2")"'
    printf '%s\n' 'eval "$LANE_PID_FILTER"'
  } >"$filt"
  # Ordinary processes, distinguished ONLY by their working directory. No fake `claude` argv is needed:
  # the property under test is the ATTRIBUTION half, and driving it with real cwds keeps the case
  # hermetic and free of any dependence on the machine's actual process table.
  ( cd "$lane" && exec sleep 30 ) & a=$!
  ( cd "$sib" && exec sleep 30 ) & b=$!
  ( cd "$lane/sub" && exec sleep 30 ) & c=$!
  sleep 1
  out=$(printf '%s\n%s\n%s\n' "$a" "$b" "$c" | bash "$filt" "$lane" "$SUPERVISOR")
  if printf '%s\n' "$out" | grep -qxF "$a" && printf '%s\n' "$out" | grep -qxF "$c" \
    && ! printf '%s\n' "$out" | grep -qxF "$b"; then
    pass "worker-probe: lane root and lane SUBDIR are attributed to the lane; a SIBLING lane's process is not"
  else
    fail "worker-probe-attribution: lane=[$a] sub=[$c] sibling=[$b] but filter returned:
$out"
  fi
  # NON-VACUITY, and it must be true of the broken code too: the SAME three pids, filtered for the
  # SIBLING's root, must return the sibling and neither lane pid. Without this, an always-empty filter
  # would satisfy the case above.
  out=$(printf '%s\n%s\n%s\n' "$a" "$b" "$c" | bash "$filt" "$sib" "$SUPERVISOR")
  if printf '%s\n' "$out" | grep -qxF "$b" && ! printf '%s\n' "$out" | grep -qxF "$a"; then
    pass "NON-VACUITY: the same pids filtered for the SIBLING root return the sibling — the filter discriminates rather than returning nothing"
  else
    fail "worker-probe-nonvacuity: filtering for the sibling root returned:
$out"
  fi
  # A pid whose cwd cannot be read is attributed to NOBODY (affirmative attribution). Driven with a
  # pid that does not exist, which is the same unreadable condition as a process exiting mid-probe.
  out=$(printf '%s\n' 999999 | bash "$filt" "$lane" "$SUPERVISOR")
  if [[ -z "$out" ]]; then
    pass "worker-probe: an unreadable cwd is attributed to nobody — a positive verdict needs a positive measurement"
  else
    fail "worker-probe-unreadable: a nonexistent pid was attributed: [$out]"
  fi
  kill "$a" "$b" "$c" 2>/dev/null
  wait "$a" "$b" "$c" 2>/dev/null
  # The BUILD family must stay machine-wide: one gate at a time per MACHINE is a resource bound that
  # survived #1930's retraction, so a sibling's cargo IS this lane's business. Asserted structurally,
  # because the distinction is the whole point of scoping only one family.
  if sed -n '/^if \[\[ -z "${PROC_PROBE_BUILD_CMD:-}"/,/^fi/p' "$SUPERVISOR" | grep -q 'LANE_PID_FILTER'; then
    fail "worker-probe-build-scoped: the BUILD probe was lane-scoped too — a sibling's gate is still this lane's business"
  else
    pass "worker-probe: the BUILD family is deliberately NOT lane-scoped (machine-wide gate serialization survives)"
  fi
  # LIST-FROM-COUNT-SET (roborev 1839/1821) must survive the change: both probes must apply the filter.
  local cnt lst
  cnt=$(sed -n '/^if \[\[ -z "${PROC_PROBE_WORKER_CMD:-}"/,/^fi/p' "$SUPERVISOR" | grep -c 'LANE_PID_FILTER')
  lst=$(sed -n '/^if \[\[ -z "${PROC_LIST_WORKER_CMD:-}"/,/^fi/p' "$SUPERVISOR" | grep -c 'LANE_PID_FILTER')
  if [[ "$cnt" -ge 1 && "$lst" -ge 1 ]]; then
    pass "worker-probe: COUNT and LIST both derive from the same lane filter — the named set cannot drift from the triggering set"
  else
    fail "worker-probe-drift: count=$cnt list=$lst references to LANE_PID_FILTER — the two sets can diverge"
  fi
}

# ---------------------------------------------------------------------------
# Test 34-claim (#3393, roborev round 35 Medium/Low): the migration must never leave the lane
# permanently foreign to its own lock, and must accept a SHA-256 lease.
# ---------------------------------------------------------------------------
mig2_case() {
  # mig2_case <fail-first-N> <sha> ; echoes the recorded claim.sh calls
  local failn="$1" sha="$2" d repo
  d="$(new_case_dir)"; repo="$d/lane"; mkdir -p "$repo" "$d/bin"
  git -C "$repo" init -q 2>/dev/null
  git -C "$repo" checkout -q -b issue-88-x 2>/dev/null
  git -C "$repo" -c user.email=t@t -c user.name=t commit -q --allow-empty -m init 2>/dev/null
  cat >"$d/bin/claim.sh" <<STUB
#!/usr/bin/env bash
printf '%s\n' "\$*" >>"$d/calls.log"
if [ "\$1" = status ]; then
  n=\$(grep -c '^status' "$d/calls.log")
  if [ "\$n" -le "$failn" ]; then exit 1; fi
  printf '%s\n' "CLAIM: STATUS issue=88 sha=$sha machine=boxA actor=flow"
fi
exit 0
STUB
  chmod +x "$d/bin/claim.sh"; : >"$d/calls.log"
  {
    printf '%s\n' '#!/usr/bin/env bash' 'log() { :; }'
    sed -n '/^CLAIM_MIGRATION_SETTLED=/p' "$SUPERVISOR"
    sed -n '/^CLAIM_MIGRATION_RETRIES=/p' "$SUPERVISOR"
    sed -n '/^supervisor_msg_token()/,/^}/p' "$SUPERVISOR"
    sed -n '/^supervisor_migrate_legacy_claim()/,/^}/p' "$SUPERVISOR"
    printf '%s\n' 'supervisor_migrate_legacy_claim'
  } >"$d/mig.sh"
  LOCK_CMD="bash $d/bin/claim.sh" CLAIM_ACTOR=flow-9-lane CLAIM_MACHINE=boxA \
    LEGACY_CLAIM_ACTOR=flow CLAIM_MIGRATION_RETRIES=3 REPO_ROOT="$repo" \
    bash "$d/mig.sh" >/dev/null 2>&1
  cat "$d/calls.log"
}

test_migration_retries_and_sha256() {
  local sha40 sha64 out
  sha40="1111111111111111111111111111111111111111"
  sha64="$(printf '2%.0s' $(seq 1 64))"
  # A BLIP: the first two status reads fail, the third succeeds -> the adopt still happens.
  out=$(mig2_case 2 "$sha40")
  if printf '%s\n' "$out" | grep -q "^adopt 88 --expect $sha40"; then
    pass "migration-retry: two failed status reads are retried and the adopt still happens (a blip does not strand the lane)"
  else
    fail "migration-retry: expected an adopt after retries, got:
$out"
  fi
  # ALL attempts fail -> NO adopt (never guess), and the bounded burst really did retry rather than
  # giving up after one read. The count is the evidence the retry loop ran.
  out=$(mig2_case 99 "$sha40")
  local tries
  tries=$(printf '%s\n' "$out" | grep -c '^status')
  if [[ "$tries" -eq 3 ]] && ! printf '%s\n' "$out" | grep -q '^adopt'; then
    pass "migration-retry: a total outage is retried CLAIM_MIGRATION_RETRIES=3 times and never adopts on a guess"
  else
    fail "migration-retry-exhausted: status attempts=$tries (expected 3), calls:
$out"
  fi
  # A 64-hex SHA-256 object id is a valid CAS lease.
  out=$(mig2_case 0 "$sha64")
  if printf '%s\n' "$out" | grep -q "^adopt 88 --expect $sha64"; then
    pass "migration-sha256: a 64-hex object id is accepted as a CAS lease (claim.sh imposes no length check of its own)"
  else
    fail "migration-sha256: a 64-hex sha was skipped, calls:
$out"
  fi
  # A 41-hex value is neither, and must still be refused — widening to 64 must not become "any length".
  out=$(mig2_case 0 "${sha40}1")
  if ! printf '%s\n' "$out" | grep -q '^adopt'; then
    pass "migration-sha256: 41 hex is still refused — the widening is 40-OR-64, not 'any length'"
  else
    fail "migration-sha256-any: a 41-char sha was accepted:
$out"
  fi
  # THE RE-ENTRY IS WIRED: an unsettled migration must be re-attempted from the main loop, or a
  # transient outage is still permanent for the run.
  if grep -cE '^[[:space:]]*supervisor_migrate_legacy_claim$' "$SUPERVISOR" | grep -qE '^[2-9]'; then
    pass "migration-retry: the migration is invoked from BOTH lock acquisition and the iteration loop"
  else
    fail "migration-retry-unwired: only one call site — an unsettled migration would never be retried"
  fi
}

t test_worker_probe_is_lane_attributed
t test_migration_retries_and_sha256

# ---------------------------------------------------------------------------
# Test 35-claim (#3393, roborev round 36 Medium): a p<pid> PLACEHOLDER cannot carry endgame
# protection past our own exit. `should-reap` permanently refuses placeholders (round 3), so keeping
# one for a pending auto-merge PR meant NOTHING could ever clear it — not the CI reaper, not a later
# merge of the very PR it protected. The protection must move to issue-numbered refs.
# ---------------------------------------------------------------------------
clearclaim_case() {
  # clearclaim_case <stamped-lane> <pending-list> <stamp-rc> -> echoes the recorded claim-cmd calls
  local stamped="$1" pending="$2" stamp_rc="$3" d
  d="$(new_case_dir)"; mkdir -p "$d/bin"
  cat >"$d/bin/hb.sh" <<STUB
#!/usr/bin/env bash
printf '%s\n' "\$*" >>"$d/calls.log"
[ "\$1" = stamp ] && exit $stamp_rc
exit 0
STUB
  chmod +x "$d/bin/hb.sh"; : >"$d/calls.log"
  {
    printf '%s\n' '#!/usr/bin/env bash' 'log() { :; }' 'claim_drain_pending_cleanup() { :; }'
    sed -n '/^clear_claim()/,/^}/p' "$SUPERVISOR"
    printf '%s\n' 'clear_claim 1'
  } >"$d/cc.sh"
  CLAIM_CMD="bash $d/bin/hb.sh" CLAIM_MACHINE=boxA CLAIM_STAMPED_ISSUE="$stamped" \
    CLAIM_STAMPED_SHA=deadbeef PENDING_PR_LIST="$pending" bash "$d/cc.sh" >/dev/null 2>&1
  cat "$d/calls.log"
}

test_placeholder_endgame_protection_transfers() {
  local out nl
  nl=$'\n'
  # (a) A PLACEHOLDER with a pending PR naming issue 88: stamp lane 88, THEN clear the placeholder.
  out=$(clearclaim_case "p1234-abc" "3467${nl:0:0}"$'\t'"88"$'\t'"1"$'\t'"1000$nl" 0)
  if printf '%s\n' "$out" | grep -q '^stamp 88' && printf '%s\n' "$out" | grep -q '^reap boxA p1234-abc'; then
    pass "placeholder-transfer: the pending endgame is re-stamped as lane 88 and the placeholder is then cleared"
  else
    fail "placeholder-transfer: expected 'stamp 88' then 'reap boxA p1234-abc', got:
$out"
  fi
  # (b) IF THE TRANSFER FAILS the placeholder must be KEPT — a stale ref beats an unprotected
  # endgame. Driven by a stamp that exits non-zero.
  out=$(clearclaim_case "p1234-abc" "3467"$'\t'"88"$'\t'"1"$'\t'"1000$nl" 1)
  if printf '%s\n' "$out" | grep -q '^stamp 88' && ! printf '%s\n' "$out" | grep -q '^reap'; then
    pass "placeholder-transfer: a FAILED stamp keeps the placeholder (all-or-nothing — a stale ref beats an unprotected endgame)"
  else
    fail "placeholder-transfer-failed-stamp: the placeholder was cleared anyway:
$out"
  fi
  # (c) A pending PR with NO recorded issue is UNTRANSFERABLE, so the placeholder is kept. This is the
  # case that must not silently clear: there is nothing for the reaper to evaluate.
  out=$(clearclaim_case "p1234-abc" "3467"$'\t'""$'\t'"1"$'\t'"1000$nl" 0)
  if ! printf '%s\n' "$out" | grep -q '^reap'; then
    pass "placeholder-transfer: a pending PR with no issue is untransferable and keeps the placeholder"
  else
    fail "placeholder-transfer-no-issue: cleared the placeholder with an untransferable endgame:
$out"
  fi
  # (d) AN ISSUE-NUMBERED lane with a pending PR is unchanged — it keeps, as #2499 ruling (b) requires,
  # and must NOT be re-stamped. Without this the fix could have widened into the case that was correct.
  out=$(clearclaim_case "88" "3467"$'\t'"88"$'\t'"1"$'\t'"1000$nl" 0)
  if ! printf '%s\n' "$out" | grep -qE '^(reap|stamp)'; then
    pass "placeholder-transfer: an ISSUE-numbered lane with a pending PR still just KEEPS (#2499 ruling (b) untouched)"
  else
    fail "placeholder-transfer-issue-lane: an issue lane was altered:
$out"
  fi
  # (e) NON-VACUITY: with NO pending PR at all, a placeholder is cleared with no stamping — so the
  # transfer above is attributable to the pending endgame rather than to placeholders always clearing.
  out=$(clearclaim_case "p1234-abc" "" 0)
  if printf '%s\n' "$out" | grep -q '^reap boxA p1234-abc' && ! printf '%s\n' "$out" | grep -q '^stamp'; then
    pass "NON-VACUITY: with no pending PR the placeholder clears WITHOUT any stamp — the transfer is caused by the endgame"
  else
    fail "placeholder-transfer-nonvacuity: got:
$out"
  fi
}

t test_placeholder_endgame_protection_transfers

# ---------------------------------------------------------------------------
# Test 36-claim (#3393, roborev round 36; lead ruling B + C, 2026-08-30): lane identity is GIVEN, and a
# fallback that cannot prove it landed in a lane REFUSES rather than degrades. The earlier cut of this
# case asserted a WARN; the ruling replaced warning with refusal, because a warning still starts four
# silently-degraded mechanisms.
#
# TWO REFUSALS AND THEY ARE INDEPENDENT — the case that proves it is MAIN-worktree + LANE_ID given:
# identity is then fine and attribution is still impossible, because an identity token is not a
# directory.
# ---------------------------------------------------------------------------
lane_identity_case() {
  # lane_identity_case <linked|main> [env...] -> echoes the FATAL token, or the accepted-identity line
  local kind="$1"; shift
  local d root
  d="$(new_case_dir)"
  if [[ "$kind" == linked ]]; then
    root="$d/lanewt"
    mkdir -p "$d/main"; git -C "$d/main" init -q
    git -C "$d/main" -c user.email=t@t -c user.name=t commit -q --allow-empty -m init 2>/dev/null
    git -C "$d/main" worktree add -q -b issue-88-x "$root" 2>/dev/null
    [[ -e "$root/.git" ]] || { skip "lane-identity: host would not create a linked worktree — premise unstageable"; return 1; }
  else
    root="$d/mainwt"
    mkdir -p "$root"; git -C "$root" init -q
    git -C "$root" -c user.email=t@t -c user.name=t commit -q --allow-empty -m init 2>/dev/null
  fi
  mkdir -p "$root/scripts/local" "$root/scripts/lib"
  cp "$SUPERVISOR" "$root/scripts/local/worker-supervisor.sh"
  # scripts/lib is needed by the default notify path (learned the hard way — an incomplete scratch tree
  # produced an unattributable failure).
  cp "$REPO_ROOT/scripts/lib/gate-notify.sh" "$root/scripts/lib/" 2>/dev/null || true
  # A FATAL WINS OVER THE IDENTITY LINE, because identity is resolved and LOGGED first and the
  # attribution refusal fires after it. Taking `head -1` across both alternatives returned the
  # identity line and hid the refusal — the case reported "started fine" for a run that refused.
  local raw
  # `PROC_PROBE_WORKER_CMD=` FIRST, and this is the whole case. `common_env` EXPORTS that variable to
  # stub the probe, so it leaks into every later case — and the attribution refusal deliberately yields
  # to an operator who set it. The refusal was therefore ALWAYS yielding to a phantom override, and the
  # two cases below both passed for the same wrong reason. Cleared here; "$@" comes after, so a case
  # that genuinely wants the override still gets it (later `env` assignments win).
  raw=$(env PROC_PROBE_WORKER_CMD= "$@" NOTIFY_CMD=true STOP_FILE=/nonexistent LOCK_CMD="" CLAIM_CMD="" MAX_ISSUES=1 \
    timeout 30 bash "$root/scripts/local/worker-supervisor.sh" 2>&1)
  if printf '%s\n' "$raw" | grep -oE 'FATAL: lane-[a-z-]+' | head -1 | grep .; then
    return 0
  fi
  printf '%s\n' "$raw" | grep -oE 'lane identity given explicitly|LANE_ID unset; derived' | head -1
  return 0
}

test_lane_identity_is_given_or_refused() {
  local out
  # (a) a LANE worktree with LANE_ID unset: the fallback may derive, because it can PROVE it is a lane.
  out=$(lane_identity_case linked X=1) || return 0
  if [[ "$out" == "LANE_ID unset; derived" ]]; then
    pass "lane-identity: in a LANE worktree the fallback derives an identity (it can prove where it is)"
  else
    fail "lane-identity(linked-fallback): got [$out]"
  fi
  # (b) MAIN worktree, LANE_ID unset -> lane-identity-unprovable. Nothing to derive FROM.
  out=$(lane_identity_case main X=1) || return 0
  if [[ "$out" == "FATAL: lane-identity-unprovable" ]]; then
    pass "lane-identity: MAIN worktree + LANE_ID unset REFUSES (lane-identity-unprovable), rather than sharing one identity across lanes"
  else
    fail "lane-identity(main-unprovable): got [$out]"
  fi
  # (c) THE CASE THAT PROVES THE TWO REFUSALS ARE INDEPENDENT: MAIN worktree + LANE_ID GIVEN. Identity
  # is satisfied; attribution is still impossible, because an identity token is not a directory.
  out=$(lane_identity_case main LANE_ID=explicit-lane-x) || return 0
  if [[ "$out" == "FATAL: lane-attribution-impossible" ]]; then
    pass "lane-identity: LANE_ID satisfies IDENTITY but not ATTRIBUTION — the second refusal is independent (an identity token is not a directory)"
  else
    fail "lane-identity(main-attribution): got [$out] — expected the attribution refusal, since LANE_ID cannot supply a directory"
  fi
  # (d) an operator who overrode the probe has taken responsibility, so it starts.
  out=$(lane_identity_case main LANE_ID=explicit-lane-x PROC_PROBE_WORKER_CMD="echo 0") || return 0
  if [[ "$out" == "lane identity given explicitly" ]]; then
    pass "lane-identity: an explicit PROC_PROBE_WORKER_CMD yields the attribution refusal to the operator"
  else
    fail "lane-identity(probe-override): got [$out]"
  fi
  # (e) a LANE_ID that claim.sh would refuse is refused HERE, loudly, rather than failing every claim.
  out=$(lane_identity_case linked LANE_ID=ab) || return 0
  if [[ "$out" == "FATAL: lane-identity-unusable" ]]; then
    pass "lane-identity: a LANE_ID under 3 recordable chars is refused at startup (claim.sh would reject the actor on every call)"
  else
    fail "lane-identity(short): got [$out]"
  fi
  # NO LAYOUT HEURISTIC: the worktrees above are named `lanewt`/`mainwt`, matching no fleet convention,
  # and the implementation must contain no such pattern — that assumption is what made AC3 unimplementable.
  if sed -n '/^lane_worktree_ok()/,/^}/p' "$SUPERVISOR" | grep -qiE '/data/lanes|lane-\[0-9\]'; then
    fail "lane-identity-heuristic: lane_worktree_ok references a lane-directory naming convention"
  else
    pass "lane-identity: the proof is structural (git worktree), assuming NO directory naming convention"
  fi
}

t test_lane_identity_is_given_or_refused

# ---------------------------------------------------------------------------
# Test 37-claim (#3393, roborev round 36 row 4; lead condition 1): the worker-orphan probe needs a
# TWO-DIRECTION control, not a passing test. A probe whose subject set can be EMPTY passes vacuously
# when it is — the same shape as `--delta-classify`'s ALLOW on an empty subject set (#3480). So:
#   POSITIVE: a leftover IS in this lane  -> counted  (would STOP)
#   NEGATIVE: no leftover in this lane    -> zero     (would NOT stop)
# and the OLD machine-wide probe counted the sibling too, which is the false STOP being fixed.
#
# The marker must be IN THE ARGV, which took three wrong attempts worth recording: a `# comment` is
# stripped by bash before exec so it never reaches /proc/<pid>/cmdline; a pattern containing regex
# metacharacters MATCHES ITS OWN TEXT in the probe subshell (the bracket trick only defeats a LITERAL
# self-match, and the real probe's $$/$PPID exclusion is load-bearing); and `exec sleep` replaces the
# process image, discarding the marker. Hence: a marker-named SCRIPT that does not exec.
# ---------------------------------------------------------------------------
test_worker_probe_two_direction_control() {
  local d lane sib marker script match probe neg pos machine_wide
  d="$(new_case_dir)"
  lane="$d/lane"; sib="$d/sibling"
  mkdir -p "$lane" "$sib"
  marker="probe$$x$RANDOM"
  script="$d/${marker}-worker.sh"
  printf '%s\n%s\n' '#!/usr/bin/env bash' 'sleep 120' >"$script"
  chmod +x "$script"
  # LITERAL match only, plus the real probe's self-exclusion — both for the reasons in the header.
  match="[p]${marker#p}-worker"
  # REPO_ROOT MUST BE SET BEFORE THE EVAL. `LANE_PID_FILTER`'s literal contains `'$REPO_ROOT'`, which
  # expands AT EVAL TIME — so eval'ing it first and passing REPO_ROOT to the probe later bakes in the
  # TEST's own lane and the case measures the wrong directory. That is how the first cut of this case
  # reported POSITIVE=0 while the machine-wide count was 2: the marker matched, the attribution did not.
  # REPO_ROOT MUST BE SET IN *THIS* SHELL BEFORE THE EVAL. `LANE_PID_FILTER`'s literal contains
  # `'$REPO_ROOT'`, and `eval` expands it in the shell that RUNS the eval — so wrapping the command
  # substitution in a subshell that sets REPO_ROOT does nothing at all (my first fix was a no-op, and
  # the case still measured the test's own lane). A function-local shadow is what actually applies.
  local LANE_PID_FILTER REPO_ROOT="$lane"
  eval "$(sed -n '/^LANE_PID_FILTER=/p' "$SUPERVISOR")"
  probe="pgrep -f '$match' 2>/dev/null | grep -vxF -e \$\$ -e \$PPID | $LANE_PID_FILTER | wc -l | tr -d ' '" # pgrep-lint-allow: run-unique marker scoping
  # NEGATIVE first, before anything is spawned: if this is not 0, the harness is matching itself and
  # every later number is meaningless.
  neg=$(bash -c "$probe")
  ( cd "$lane" && exec bash "$script" ) >/dev/null 2>&1 &
  ( cd "$sib"  && exec bash "$script" ) >/dev/null 2>&1 &
  sleep 1
  pos=$(bash -c "$probe")
  machine_wide=$(bash -c "pgrep -f '$match' 2>/dev/null | grep -vxF -e \$\$ -e \$PPID | wc -l | tr -d ' '") # pgrep-lint-allow: run-unique marker scoping
  pkill -f "${marker}-worker.sh" >/dev/null 2>&1
  if [[ "$neg" == "0" ]]; then
    pass "probe-two-direction NEGATIVE: no leftover in this lane counts 0 (so the probe does not fire unconditionally)"
  else
    fail "probe-two-direction: NEGATIVE control counted $neg before anything was spawned — the harness is matching itself, so no later number means anything"
  fi
  if [[ "$pos" == "1" ]]; then
    pass "probe-two-direction POSITIVE: a leftover IN this lane counts 1 (so the probe DOES fire, and the negative above is a measurement)"
  else
    fail "probe-two-direction: POSITIVE control counted $pos (expected 1) — a probe that cannot count its subject passes vacuously"
  fi
  if [[ "$machine_wide" == "2" ]]; then
    pass "probe-two-direction: the OLD machine-wide probe counts 2 (lane + sibling) — the false STOP this fixes, measured rather than asserted"
  else
    fail "probe-two-direction: machine-wide counted $machine_wide (expected 2); the comparison that motivates lane scoping is not established"
  fi
}

t test_worker_probe_two_direction_control
# ---------------------------------------------------------------------------
# Tests 38..42-lock (#3549): PRE-#3467 LEGACY GLOBAL LOCK COMPATIBILITY.
#
# #3467 moved the derived default lock from ONE MACHINE-GLOBAL path to a PER-LANE one. The per-lane
# path is the correct end state (#3393), but nothing consulted the old path — so a supervisor from a
# pre-#3467 checkout holds a lock the new one never looks at and BOTH run in one worktree, sharing
# markers, branch, logs and `.worker-last-iteration.json`.
#
# EVERY EXISTING CASE IN THIS FILE IS UNAFFECTED BY DESIGN: `common_env` exports an explicit
# `SUPERVISOR_LOCK`, which is the AC4 override path and skips the check entirely. These cases are the
# only ones that UNSET it, and they scope `TMPDIR` to the case dir so the legacy path they build is
# never the real machine-global one.
#
# THE PIDS ARE REAL (AC5): a genuinely running child for `live`, a started-and-reaped one for `dead`.
# The liveness probe is never stubbed.
# ---------------------------------------------------------------------------

# legacy_lock_drive <tmpdir> <lane-id> [explicit-lock] — run the REAL `acquire_lock`, read out of the
# shipped supervisor at run time (sourced, so the guard under test is the shipped code and never a
# re-implementation). Echoes stdout+stderr; the caller reads `$?`.
#
# `LOCK_CMD=""`/`CLAIM_CMD=""`: with `SUPERVISOR_LOCK` unset the run also does lane-identity
# resolution, claim-actor derivation and `supervisor_migrate_legacy_claim`, the last of which can fire
# a network `claim.sh status`. Empty disables both seams (the colonless `${VAR-default}` form in the
# supervisor preserves an explicitly-empty override), so these cases stay hermetic.
legacy_lock_drive() {
  local tmp="$1" lane="$2" explicit="${3:-}"
  local body='source "$1"; acquire_lock; printf "ACQUIRED=%s\n" "$SUPERVISOR_LOCK"; [[ -d "$SUPERVISOR_LOCK" ]] && printf "LOCKDIR=yes\n"; exit 0'
  if [[ -n "$explicit" ]]; then
    env TMPDIR="$tmp" SUPERVISOR_LOCK="$explicit" LANE_ID="$lane" LOCK_CMD="" CLAIM_CMD="" \
      bash -c "$body" _ "$SUPERVISOR" 2>&1
  else
    env -u SUPERVISOR_LOCK TMPDIR="$tmp" LANE_ID="$lane" LOCK_CMD="" CLAIM_CMD="" \
      bash -c "$body" _ "$SUPERVISOR" 2>&1
  fi
}

# The refusal must be the LEGACY one, not the per-lane "another instance is already running" — an
# operator and a test both have to be able to tell the two locks apart.
legacy_refusal_ok() {
  local out="$1"
  [[ "$out" == *"LEGACY GLOBAL supervisor lock"* ]] \
    && [[ "$out" == *"#3549"* ]] \
    && [[ "$out" != *"another instance is already running"* ]]
}

test_legacy_global_lock_refuses_live_holder() {
  local d tmp lane legacy live out rc control crc derived
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"; lane="lane3549live$$"
  mkdir -p "$tmp"
  legacy="$tmp/cqlite-worker-supervisor.lock"

  # NON-VACUITY 1: the collision this guard prevents is REAL — the derived per-lane path and the
  # legacy global path genuinely DIFFER for this LANE_ID, so a refusal cannot be coming from the
  # per-lane lock. (This is the whole reason the old lock is invisible to the new one.)
  derived="$tmp/cqlite-worker-supervisor-$lane.lock"
  if [[ "$derived" != "$legacy" ]]; then
    pass "legacy-lock NON-VACUITY: the derived per-lane path differs from the legacy global path (the collision is real, and no refusal below can come from the per-lane lock)"
  else
    fail "legacy-lock-nonvacuity: derived [$derived] == legacy [$legacy]; the two paths must differ or these cases measure nothing"
  fi

  # NON-VACUITY 2 (two-direction control): the SAME harness with NO legacy lock present must ACQUIRE.
  # Without this, a refusal could be any earlier failure — an unresolvable lane identity, a missing
  # stub — wearing the guard's clothes.
  control="$(legacy_lock_drive "$tmp" "$lane")"; crc=$?
  if [[ "$crc" -eq 0 && "$control" == *"LOCKDIR=yes"* && "$control" == *"ACQUIRED=$derived"* ]]; then
    pass "legacy-lock NON-VACUITY: with NO legacy lock the same harness ACQUIRES the per-lane lock (so it reaches the guard, and a refusal below is attributable to the guard)"
  else
    fail "legacy-lock-nonvacuity-control: rc=$crc out=[$control] — the harness must succeed when no legacy lock exists, or nothing below is attributable"
  fi
  rm -rf "$derived"

  # AC1, with a REAL live pid.
  sleep 300 &
  live=$!
  mkdir -p "$legacy"
  printf '%s\n' "$live" >"$legacy/pid"
  out="$(legacy_lock_drive "$tmp" "$lane")"; rc=$?
  kill "$live" 2>/dev/null || true
  wait "$live" 2>/dev/null || true

  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ "$out" == *"$live"* ]]; then
    pass "legacy-lock AC1: a LIVE pre-#3467 holder (real pid $live) refuses the start, loudly, naming the legacy lock and the holder — and NOT with the per-lane message"
  else
    fail "legacy-lock-live: rc=$rc (expected non-zero) out=[$out] — expected the LEGACY GLOBAL refusal naming pid $live"
  fi
  # A REFUSED START ACQUIRES NOTHING. Asserted on the RUN'S OUTPUT as well as the filesystem, because
  # the filesystem half alone is vacuous: a successful acquisition removes the per-lane lock again on
  # exit (the EXIT trap), so `! -e` is true either way — it passed under the guard-removed mutant.
  if [[ "$out" != *"ACQUIRED="* && ! -e "$derived" ]]; then
    pass "legacy-lock AC1: the refusal acquired NOTHING — no ACQUIRED line and no per-lane lock (a refused start leaves nothing behind)"
  else
    fail "legacy-lock-live-sideeffect: out=[$out] derived-exists=$([[ -e "$derived" ]] && echo yes || echo no) — a refusal must not acquire the per-lane lock"
  fi
  rm -rf "$legacy"
}

test_legacy_global_lock_reclaims_confirmed_stale() {
  local d tmp lane legacy dead out rc derived
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"; lane="lane3549stale$$"
  mkdir -p "$tmp"
  legacy="$tmp/cqlite-worker-supervisor.lock"
  derived="$tmp/cqlite-worker-supervisor-$lane.lock"

  # A REAL dead pid: started and REAPED, so the kernel has genuinely released it (AC5). Not a made-up
  # large number, which would only ever exercise "not found" and never the reaped case.
  sleep 0.1 &
  dead=$!
  wait "$dead" 2>/dev/null || true
  mkdir -p "$legacy"
  printf '%s\n' "$dead" >"$legacy/pid"

  out="$(legacy_lock_drive "$tmp" "$lane")"; rc=$?
  if [[ "$rc" -eq 0 && "$out" == *"LOCKDIR=yes"* && "$out" == *"reclaiming STALE legacy global supervisor lock"* ]]; then
    pass "legacy-lock AC2: a legacy lock whose recorded pid is CONFIRMED dead (real reaped pid $dead) is reclaimed and the start proceeds"
  else
    fail "legacy-lock-stale: rc=$rc out=[$out] — expected a reclaim and a successful acquisition"
  fi
  if [[ ! -e "$legacy" ]]; then
    pass "legacy-lock AC2: the stale legacy lock directory is GONE after the reclaim (rename-aside, then removed)"
  else
    fail "legacy-lock-stale-residue: $legacy still exists after the reclaim"
  fi
  rm -rf "$derived"
}

test_legacy_global_lock_unknown_shapes_refuse() {
  local d tmp lane legacy derived out rc shape
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"; lane="lane3549unk$$"
  mkdir -p "$tmp"
  legacy="$tmp/cqlite-worker-supervisor.lock"
  derived="$tmp/cqlite-worker-supervisor-$lane.lock"

  # AC3. Every one of these is "cannot tell", and "cannot tell" must NOT collapse onto the permissive
  # answer — the two-valued-file-predicate trap named in CLAUDE.md. Each shape is asserted separately
  # so a single passing shape cannot hide a permissive sibling.
  for shape in regular-file no-pid-file non-numeric-pid empty-pid; do
    rm -rf "$legacy" "$derived"
    case "$shape" in
      regular-file) printf 'not a lock dir\n' >"$legacy" ;;
      no-pid-file) mkdir -p "$legacy" ;;
      non-numeric-pid) mkdir -p "$legacy"; printf 'pid-1234\n' >"$legacy/pid" ;;
      empty-pid) mkdir -p "$legacy"; : >"$legacy/pid" ;;
    esac
    out="$(legacy_lock_drive "$tmp" "$lane")"; rc=$?
    if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ ! -e "$derived" ]]; then
      pass "legacy-lock AC3 ($shape): an undeterminable legacy lock REFUSES the start and creates no per-lane lock"
    else
      fail "legacy-lock-unknown($shape): rc=$rc derived-exists=$([[ -e "$derived" ]] && echo yes || echo no) out=[$out]"
    fi
  done
  rm -rf "$legacy" "$derived"
}

# THE SYMLINK SHAPES (#3549, roborev job 180 Medium). `-d`, `-f`, `-r`, `-x` and `-e` all FOLLOW
# symlinks, so a link is not merely another malformed shape — it is the one shape whose predicates
# answer as a VALID lock would. The classifier must therefore reject `-L` BEFORE any of them, and the
# case that matters is the destructive direction: a link to a real, well-formed lock directory whose
# recorded pid is CONFIRMED DEAD classifies as `stale` without the `-L` test, and the reclaim then
# MOVES AND DELETES the object at that name. Every case below asserts BOTH halves — the refusal, and
# that NOTHING was moved or deleted (the link, its target, the target's `pid`, and no renamed-aside
# residue).
test_legacy_global_lock_symlink_shapes_refuse() {
  local d tmp lane legacy derived out rc dead target link_before aside_count
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"; lane="lane3549sym$$"
  mkdir -p "$tmp"
  legacy="$tmp/cqlite-worker-supervisor.lock"
  derived="$tmp/cqlite-worker-supervisor-$lane.lock"

  # A REAL dead pid: started and REAPED, so the kernel has genuinely released it — the same standard
  # as the reclaim case (AC5). A made-up large number would only ever exercise "not found".
  sleep 0.1 &
  dead=$!
  wait "$dead" 2>/dev/null || true

  # ---- (a) THE DANGEROUS ONE: a symlink to a VALID lock directory with a CONFIRMED-DEAD pid. -------
  # This is the case that is NOT caught by any other predicate: follow the link and it is a textbook
  # reclaimable lock. `$target` is deliberately NOT a lock — it is any directory that happens to hold
  # a well-formed `pid`, which is exactly the object the guard must not destroy.
  rm -rf "$legacy" "$derived"
  target="$tmp/not-a-lock-at-all"
  mkdir -p "$target"
  printf '%s\n' "$dead" >"$target/pid"
  ln -s "$target" "$legacy"
  link_before="$(readlink "$legacy")"
  out="$(legacy_lock_drive "$tmp" "$lane")"; rc=$?
  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ "$out" == *"path-is-a-symlink"* ]] && [[ ! -e "$derived" ]]; then
    pass "legacy-lock symlink(a): a symlink to a VALID dead-pid lock directory REFUSES with the symlink cause (not 'not-a-directory') and creates no per-lane lock"
  else
    fail "legacy-lock-symlink-a: rc=$rc derived-exists=$([[ -e "$derived" ]] && echo yes || echo no) out=[$out] — expected a refusal naming path-is-a-symlink"
  fi
  # NOTHING MOVED, NOTHING DELETED — the whole harm of the finding.
  aside_count="$(find "$tmp" -maxdepth 1 -name '*.aside.*' 2>/dev/null | wc -l | tr -d ' ')"
  if [[ -L "$legacy" && "$(readlink "$legacy")" == "$link_before" ]] \
     && [[ -d "$target" && -f "$target/pid" && "$(cat "$target/pid")" == "$dead" ]] \
     && [[ "$aside_count" == "0" ]]; then
    pass "legacy-lock symlink(a): the link, its TARGET directory and the target's pid file are all untouched, and no renamed-aside residue was created"
  else
    fail "legacy-lock-symlink-a-destroyed: link=$([[ -L "$legacy" ]] && readlink "$legacy" || echo GONE) target-dir=$([[ -d "$target" ]] && echo yes || echo GONE) target-pid=[$(cat "$target/pid" 2>/dev/null || echo GONE)] aside-residue=$aside_count"
  fi

  # ---- (b) a DANGLING symlink at the legacy path -------------------------------------------------
  # `-e` is false and `-L` is true, so this must NOT be read as verified absence (which would let the
  # start proceed as if no legacy supervisor existed) and must NOT be read as `not-a-directory` either.
  rm -rf "$legacy" "$derived"
  ln -s "$tmp/nothing-is-here" "$legacy"
  out="$(legacy_lock_drive "$tmp" "$lane")"; rc=$?
  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ "$out" == *"path-is-a-symlink"* ]] && [[ ! -e "$derived" ]]; then
    pass "legacy-lock symlink(b): a DANGLING symlink is neither verified absence nor a malformed lock — it refuses with the symlink cause"
  else
    fail "legacy-lock-symlink-b: rc=$rc derived-exists=$([[ -e "$derived" ]] && echo yes || echo no) out=[$out]"
  fi
  if [[ -L "$legacy" ]]; then
    pass "legacy-lock symlink(b): the dangling link itself was not removed"
  else
    fail "legacy-lock-symlink-b-destroyed: the dangling link at $legacy is gone"
  fi

  # ---- (c) a symlink AT `$legacy/pid` pointing to a VALID dead pid file -------------------------
  # The lock directory is genuine; only the `pid` path is a link. `-f`/`-r` follow it, so the pid reads
  # as a well-formed dead pid and the reclaim would move the real directory aside and delete it.
  rm -rf "$legacy" "$derived"
  mkdir -p "$legacy" "$tmp/pidsource"
  printf '%s\n' "$dead" >"$tmp/pidsource/pid"
  ln -s "$tmp/pidsource/pid" "$legacy/pid"
  out="$(legacy_lock_drive "$tmp" "$lane")"; rc=$?
  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ "$out" == *"pid-path-is-a-symlink"* ]] && [[ ! -e "$derived" ]]; then
    pass "legacy-lock symlink(c): a symlink AT the pid path with a valid dead pid behind it REFUSES with its OWN cause token (distinct from pid-file-not-a-readable-file)"
  else
    fail "legacy-lock-symlink-c: rc=$rc derived-exists=$([[ -e "$derived" ]] && echo yes || echo no) out=[$out] — expected a refusal naming pid-path-is-a-symlink"
  fi
  aside_count="$(find "$tmp" -maxdepth 1 -name '*.aside.*' 2>/dev/null | wc -l | tr -d ' ')"
  if [[ -d "$legacy" && -L "$legacy/pid" ]] \
     && [[ -f "$tmp/pidsource/pid" && "$(cat "$tmp/pidsource/pid")" == "$dead" ]] \
     && [[ "$aside_count" == "0" ]]; then
    pass "legacy-lock symlink(c): the lock directory, the pid LINK and the linked-to pid file all survive, with no renamed-aside residue"
  else
    fail "legacy-lock-symlink-c-destroyed: dir=$([[ -d "$legacy" ]] && echo yes || echo GONE) pid-link=$([[ -L "$legacy/pid" ]] && echo yes || echo GONE) source-pid=[$(cat "$tmp/pidsource/pid" 2>/dev/null || echo GONE)] aside-residue=$aside_count"
  fi

  # ---- (d) a DANGLING symlink at `$legacy/pid` --------------------------------------------------
  # `-e "$legacy/pid"` is false here, so the pid check must be reached BEFORE it: otherwise this is
  # reported as `pid-file-missing`, a wrong cause for a shape we refuse to follow.
  rm -rf "$legacy" "$derived"
  mkdir -p "$legacy"
  ln -s "$tmp/no-such-pid" "$legacy/pid"
  out="$(legacy_lock_drive "$tmp" "$lane")"; rc=$?
  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ "$out" == *"pid-path-is-a-symlink"* ]] && [[ "$out" != *"pid-file-missing"* ]] && [[ ! -e "$derived" ]]; then
    pass "legacy-lock symlink(d): a DANGLING pid symlink is reported as the symlink shape, not as pid-file-missing"
  else
    fail "legacy-lock-symlink-d: rc=$rc out=[$out] — expected pid-path-is-a-symlink and not pid-file-missing"
  fi
  if [[ -L "$legacy/pid" && -d "$legacy" ]]; then
    pass "legacy-lock symlink(d): the dangling pid link and its directory were not removed"
  else
    fail "legacy-lock-symlink-d-destroyed: dir=$([[ -d "$legacy" ]] && echo yes || echo GONE) pid-link=$([[ -L "$legacy/pid" ]] && echo yes || echo GONE)"
  fi

  # ORDERING PROOF, structural: both `-L` tests must appear BEFORE the predicates that follow the link,
  # because a passing behavioural case cannot distinguish "tested first" from "tested at all" once the
  # branch exists. A future reorder that moves either below its `-d`/`-f` would still pass every case
  # above only if the reorder were harmless — it is not, so pin the order in source.
  local body
  body="$(sed -n '/^supervisor_legacy_lock_state()/,/^}/p' "$SUPERVISOR")"
  if [[ "$(printf '%s\n' "$body" | grep -n '\-L "\$legacy"' | tail -1 | cut -d: -f1)" \
        -lt "$(printf '%s\n' "$body" | grep -n '\-d "\$legacy"' | head -1 | cut -d: -f1)" ]] \
     && [[ "$(printf '%s\n' "$body" | grep -n '\-L "\$legacy/pid"' | head -1 | cut -d: -f1)" \
        -lt "$(printf '%s\n' "$body" | grep -n '\-e "\$legacy/pid"' | head -1 | cut -d: -f1)" ]]; then
    pass "legacy-lock symlink: both -L rejections precede the link-following predicates in source (-d for the lock, -e/-f for the pid)"
  else
    fail "legacy-lock-symlink-order: a -L test does not precede the predicate that follows the link in supervisor_legacy_lock_state"
  fi

  rm -rf "$legacy" "$derived"
}

test_legacy_global_lock_override_skips_check() {
  local d tmp lane legacy explicit live out rc
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"; lane="lane3549ovr$$"
  mkdir -p "$tmp"
  legacy="$tmp/cqlite-worker-supervisor.lock"
  explicit="$d/explicit.lock"

  # AC4: an operator who NAMES the lock has taken the placement decision; the compatibility check is
  # about OUR DEFAULT colliding with the OLD DEFAULT, so an explicit path skips it entirely. A LIVE
  # legacy holder is present — the strongest form of the check — and must be neither honoured nor
  # touched.
  sleep 300 &
  live=$!
  mkdir -p "$legacy"
  printf '%s\n' "$live" >"$legacy/pid"
  out="$(legacy_lock_drive "$tmp" "$lane" "$explicit")"; rc=$?
  kill "$live" 2>/dev/null || true
  wait "$live" 2>/dev/null || true

  if [[ "$rc" -eq 0 && "$out" == *"ACQUIRED=$explicit"* && "$out" != *"LEGACY GLOBAL supervisor lock"* ]]; then
    pass "legacy-lock AC4: an explicit SUPERVISOR_LOCK skips the legacy check entirely, even with a LIVE legacy holder present"
  else
    fail "legacy-lock-override: rc=$rc out=[$out] — an explicit lock must be honoured and the legacy check skipped"
  fi
  if [[ -d "$legacy" && -f "$legacy/pid" && "$(cat "$legacy/pid")" == "$live" ]]; then
    pass "legacy-lock AC4: the legacy lock is UNTOUCHED by an override run (not reclaimed, not rewritten)"
  else
    fail "legacy-lock-override-touched: the legacy lock at $legacy was modified by an override run"
  fi
  rm -rf "$legacy"
}

test_legacy_global_lock_removal_condition_recorded() {
  local block
  # AC6: the check is REMOVABLE, and the condition under which it may be dropped is RECORDED IN THE
  # CODE — not in a commit message that nobody reads at deletion time. Light on purpose: this pins the
  # RECORD, not its prose.
  block="$(sed -n '/LEGACY GLOBAL LOCK COMPATIBILITY/,/^supervisor_legacy_lock_guard()/p' "$SUPERVISOR")"
  if [[ "$block" == *"REMOVAL CONDITION"* && "$block" == *"#3467"* && "$block" == *"#3549"* ]]; then
    pass "legacy-lock AC6: the guard records its own removal condition (every checkout at or past #3467) with both issue numbers"
  else
    fail "legacy-lock-removal-condition: the guard does not record a removal condition naming #3467 and #3549"
  fi
  # The guard must be DERIVED-DEFAULT-GATED on the RECORDED flag, never on a re-detection of
  # `SUPERVISOR_LOCK` emptiness — which is unconditionally non-empty by the time the guard runs, so
  # `[[ -n "$SUPERVISOR_LOCK" ]]` would read "explicit" always and disable the guard outright.
  local guard_body
  guard_body="$(sed -n '/^supervisor_legacy_lock_guard()/,/^}/p' "$SUPERVISOR")"
  if [[ "$guard_body" == *"SUPERVISOR_LOCK_DERIVED"* ]]; then
    pass "legacy-lock: the guard is gated on the RECORDED derivation flag, not on a re-detection of SUPERVISOR_LOCK"
  else
    fail "legacy-lock-derivation-flag: supervisor_legacy_lock_guard does not consult SUPERVISOR_LOCK_DERIVED"
  fi
}

t test_legacy_global_lock_refuses_live_holder
t test_legacy_global_lock_reclaims_confirmed_stale
t test_legacy_global_lock_unknown_shapes_refuse
t test_legacy_global_lock_symlink_shapes_refuse
t test_legacy_global_lock_override_skips_check
t test_legacy_global_lock_removal_condition_recorded

# ---------------------------------------------------------------------------
# Tests 44..48-lock (#3549, roborev job 178 High): THE STALE-RECLAIM REPLACEMENT RACE.
#
# `mv` is atomic with respect to the NAME, not the OBJECT. Between classifying the legacy lock
# `stale <pidX>` and renaming it aside, a fresh pre-#3467 supervisor can reclaim it itself and `mkdir`
# a NEW lock recording its own LIVE pidY at the same name. The pre-fix guard then renamed THAT live
# directory aside and `rm -rf`d it — destroying a live holder's lock and co-running with it.
#
# DRIVEN DETERMINISTICALLY, NEVER RACED. These cases do not start a competitor and hope for an
# interleaving (a sleep-race test is flaky by construction). They interpose at the seam: a shell
# function named `mv`, defined in the driving shell BEFORE the supervisor is sourced, so the SHIPPED
# guard's own `mv` call is the one that runs the racer's actions and then performs the real rename.
# The revalidation under test is therefore the shipped code, and every pid involved is a REAL process.
# ---------------------------------------------------------------------------

# legacy_lock_mv_shim <file> — write the interposing `mv`. Fires ONCE (the guard's reclaim rename) and
# is a pass-through afterwards. Behaviour selected by SHIM_MODE:
#   replace          racer replaces the object at the legacy name BEFORE our rename (pid: SHIM_PID)
#   repid            the object keeps its name but records a DIFFERENT pid (SHIM_PID) than we judged
#   repid-occupy     repid, and the freed name is re-occupied (SHIM_PID2) while we hold the aside
#   repid-restorefail repid, and the RESTORE `mv` is failed DETERMINISTICALLY at this seam
#   retake           our rename is honoured, then the freed name is retaken by a LIVE holder (SHIM_PID)
#   decoy-file|decoy-dir|decoy-link
#                    a preserved aside from an EARLIER run already occupies the PID-DERIVED destination
#                    `$SHIM_LEGACY.stale.$$` (as a file / a directory / a symlink to a directory)
#   competitor       a REAL SECOND PROCESS performs the whole pre-#3467 reclaim-and-acquire during a
#                    FORCED PAUSE between the guard's classify and its act; the shim BLOCKS until that
#                    process holds the lock and is alive, then lets the real rename proceed
#   competitor-restore
#                    as `competitor`, and a SECOND real process takes the freed legacy name at the
#                    guard's RESTORE seam — whichever primitive that code reaches (the exclusive
#                    `mkdir`, or the `mv` a check-then-act restore would use)
#
# NOTHING IN THESE MODES DEPENDS ON FILESYSTEM PERMISSIONS, uid or umask (roborev job 179, Low):
# `chmod` is not a control on a root run and this suite supports root, so a case that needs an
# operation to fail makes it fail AT THE SEAM instead.
legacy_lock_mv_shim() {
  cat >"$1" <<'SHIM'
# _shim_race_in <ready-file> — run the REAL competitor process and BLOCK until it holds the lock. This
# is the controllable pause: the interleaving is FORCED, never raced against a sleep.
_shim_race_in() {
  local ready="$1" n=0
  command bash "$SHIM_COMPETITOR" "$SHIM_LEGACY" "$ready" >/dev/null 2>&1 &
  while [[ ! -s "$ready" ]] && (( n < 400 )); do command sleep 0.05; n=$((n + 1)); done
}

# The guard's RESTORE reaches an exclusive `mkdir` (the arbitrated form) — interpose there too, so the
# same forced interleaving is delivered to whichever primitive the code under test uses.
mkdir() {
  if [[ "${SHIM_MODE:-}" == competitor-restore && "${_SHIM_FIRED:-no}" == yes && "${_SHIM_RESTORE_RACED:-no}" == no ]]; then
    _SHIM_RESTORE_RACED=yes
    : >"$SHIM_MARK.restore"
    _shim_race_in "$SHIM_READY2"
  fi
  command mkdir "$@"
}

mv() {
  if [[ "${_SHIM_FIRED:-no}" == yes ]]; then
    # Every `mv` after the reclaim rename — on the paths that reach here, the guard's RESTORE.
    if [[ "${SHIM_MODE:-}" == repid-restorefail ]]; then
      : >"$SHIM_MARK.restore"
      printf 'mv: shimmed deterministic restore failure\n' >&2
      return 1
    fi
    if [[ "${SHIM_MODE:-}" == competitor-restore && "${_SHIM_RESTORE_RACED:-no}" == no ]]; then
      _SHIM_RESTORE_RACED=yes
      : >"$SHIM_MARK.restore"
      _shim_race_in "$SHIM_READY2"
    fi
    command mv "$@"; return $?
  fi
  _SHIM_FIRED=yes
  : >"$SHIM_MARK"
  case "${SHIM_MODE:-}" in
    replace)
      command rm -rf "$SHIM_LEGACY"
      command mkdir -p "$SHIM_LEGACY"
      printf '%s\n' "$SHIM_PID" >"$SHIM_LEGACY/pid"
      command mv "$@"
      ;;
    repid | repid-occupy | repid-restorefail)
      printf '%s\n' "$SHIM_PID" >"$SHIM_LEGACY/pid"
      command mv "$@" || return $?
      case "$SHIM_MODE" in
        repid-occupy)
          command mkdir -p "$SHIM_LEGACY"
          printf '%s\n' "$SHIM_PID2" >"$SHIM_LEGACY/pid"
          ;;
      esac
      ;;
    retake)
      command mv "$@" || return $?
      command mkdir -p "$SHIM_LEGACY"
      printf '%s\n' "$SHIM_PID" >"$SHIM_LEGACY/pid"
      ;;
    decoy-file | decoy-dir | decoy-link)
      # THIS SHELL IS THE GUARD'S OWN SHELL, so `$$` here is exactly the `$$` a pid-derived aside name
      # would interpolate. Plant a previously-preserved aside at that name and record where, so the
      # case can assert the guard neither read it nor touched it.
      _decoy="$SHIM_LEGACY.stale.$$"
      case "$SHIM_MODE" in
        decoy-file) printf 'preserved-by-an-earlier-run\n' >"$_decoy" ;;
        decoy-dir)
          command mkdir -p "$_decoy"
          printf '%s\n' "$SHIM_PID" >"$_decoy/pid"
          ;;
        decoy-link)
          command mkdir -p "$_decoy.target"
          command ln -s "$_decoy.target" "$_decoy"
          ;;
      esac
      printf '%s\n' "$_decoy" >"$SHIM_MARK.decoy"
      command mv "$@"
      ;;
    competitor | competitor-restore)
      _shim_race_in "$SHIM_READY"
      command mv "$@"
      ;;
    *) command mv "$@" ;;
  esac
}
SHIM
}

# legacy_lock_competitor_script <file> — a REAL SECOND PROCESS that runs the PRE-#3467 supervisor's own
# reclaim-and-acquire (`mkdir` fails -> read pid -> if dead `mv` aside -> `rm -rf` -> `mkdir` -> write
# its own pid) and then STAYS ALIVE holding the lock, so the pid recorded in the lock is genuinely live.
# Argv: <legacy-lock-path> <ready-file>. The ready file is written LAST and carries the live pid, which
# is what makes the interleaving observable rather than timed.
legacy_lock_competitor_script() {
  cat >"$1" <<'COMP'
#!/usr/bin/env bash
set -uo pipefail
L="$1"; READY="$2"
if ! mkdir "$L" 2>/dev/null; then
  p=""
  [[ -f "$L/pid" ]] && p="$(cat "$L/pid" 2>/dev/null || true)"
  if [[ -n "$p" ]] && kill -0 "$p" 2>/dev/null; then printf 'BLOCKED\n' >"$READY"; exit 2; fi
  if mv "$L" "$L.compaside.$$" 2>/dev/null; then rm -rf "$L.compaside.$$"; fi
  mkdir "$L" 2>/dev/null || { printf 'LOST\n' >"$READY"; exit 3; }
fi
printf '%s\n' "$$" >"$L/pid"
printf '%s\n' "$$" >"$READY"
# `exec` keeps the SAME pid alive, so the pid recorded in the lock stays live for the whole case.
exec sleep 300
COMP
}

# legacy_lock_drive_shimmed <tmp> <lane> <shim> <mode> <pid> [pid2] — as `legacy_lock_drive`, with the
# shim sourced FIRST so it is in scope for the guard's own `mv`.
legacy_lock_drive_shimmed() {
  local tmp="$1" lane="$2" shim="$3" mode="$4" pid="$5" pid2="${6:-}"
  local body='source "$2"; source "$1"; acquire_lock; printf "ACQUIRED=%s\n" "$SUPERVISOR_LOCK"; [[ -d "$SUPERVISOR_LOCK" ]] && printf "LOCKDIR=yes\n"; exit 0'
  env -u SUPERVISOR_LOCK TMPDIR="$tmp" LANE_ID="$lane" LOCK_CMD="" CLAIM_CMD="" \
    SHIM_MODE="$mode" SHIM_PID="$pid" SHIM_PID2="$pid2" \
    SHIM_LEGACY="$tmp/cqlite-worker-supervisor.lock" SHIM_MARK="$tmp/.shim-fired" \
    SHIM_COMPETITOR="${LEGACY_LOCK_COMPETITOR:-}" \
    SHIM_READY="$tmp/.competitor-ready" SHIM_READY2="$tmp/.competitor2-ready" \
    bash -c "$body" _ "$SUPERVISOR" "$shim" 2>&1
}

# A REAL reaped pid — started and waited, so the kernel has genuinely released it.
legacy_lock_reaped_pid() {
  local p
  sleep 0.1 &
  p=$!
  wait "$p" 2>/dev/null || true
  printf '%s\n' "$p"
}

test_legacy_global_lock_replacement_race_preserves_live_lock() {
  local d tmp lane legacy dead racer out rc derived aside
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"; lane="lane3549repl$$"
  mkdir -p "$tmp"
  legacy="$tmp/cqlite-worker-supervisor.lock"
  derived="$tmp/cqlite-worker-supervisor-$lane.lock"
  legacy_lock_mv_shim "$d/mvshim.sh"

  dead="$(legacy_lock_reaped_pid)"
  mkdir -p "$legacy"
  printf '%s\n' "$dead" >"$legacy/pid"
  sleep 300 &
  racer=$!

  # THE RACER WINS between classification and rename: the object at the legacy name is a DIFFERENT
  # lock recording a LIVE pid by the time our `mv` runs.
  out="$(legacy_lock_drive_shimmed "$tmp" "$lane" "$d/mvshim.sh" replace "$racer")"; rc=$?

  if [[ -e "$tmp/.shim-fired" ]]; then
    pass "legacy-lock replacement NON-VACUITY: the interposed rename FIRED, so the guard really executed its reclaim path with the racer's lock in place"
  else
    fail "legacy-lock-replacement-vacuous: the shim never fired; the guard did not reach its reclaim rename and this case measured nothing"
  fi
  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ "$out" == *"$racer"* ]]; then
    pass "legacy-lock replacement: a lock REPLACED between the check and the rename REFUSES the start, naming the foreign pid $racer it refused to delete"
  else
    fail "legacy-lock-replacement: rc=$rc out=[$out] — expected the LEGACY refusal naming the racer pid $racer"
  fi
  # THE PROPERTY: the live holder's lock is STILL THERE, unchanged. This is the byte the pre-fix code
  # deleted.
  if [[ -d "$legacy" && -f "$legacy/pid" && "$(cat "$legacy/pid")" == "$racer" ]]; then
    pass "legacy-lock replacement: the LIVE holder's lock survives at $legacy with its own pid $racer — the guard restored what it renamed aside instead of destroying it"
  else
    fail "legacy-lock-replacement-destroyed: the live lock at $legacy is gone or rewritten (pid now [$([[ -f "$legacy/pid" ]] && cat "$legacy/pid" || echo ABSENT)]); a live holder's lock was destroyed"
  fi
  if [[ "$out" != *"ACQUIRED="* && ! -e "$derived" ]]; then
    pass "legacy-lock replacement: the refusal acquired NOTHING (no ACQUIRED line, no per-lane lock)"
  else
    fail "legacy-lock-replacement-sideeffect: out=[$out] derived-exists=$([[ -e "$derived" ]] && echo yes || echo no)"
  fi
  aside="$(printf '%s\n' "$tmp"/*.aside.* 2>/dev/null)"
  if ! compgen -G "$tmp/*.aside.*" >/dev/null; then
    pass "legacy-lock replacement: no renamed-aside residue is left behind (the object went back to its own name)"
  else
    fail "legacy-lock-replacement-residue: a renamed-aside directory survives at [$aside]"
  fi

  kill "$racer" 2>/dev/null || true
  wait "$racer" 2>/dev/null || true
  rm -rf "$legacy" "$derived"
}

test_legacy_global_lock_identity_mismatch_restores() {
  local d tmp lane legacy dead other out rc derived
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"; lane="lane3549ident$$"
  mkdir -p "$tmp"
  legacy="$tmp/cqlite-worker-supervisor.lock"
  derived="$tmp/cqlite-worker-supervisor-$lane.lock"
  legacy_lock_mv_shim "$d/mvshim.sh"

  dead="$(legacy_lock_reaped_pid)"
  other="$(legacy_lock_reaped_pid)"
  mkdir -p "$legacy"
  printf '%s\n' "$dead" >"$legacy/pid"

  # IDENTITY ALONE: the pid the renamed-aside object records is ALSO dead, so only the identity check
  # can refuse here. A guard that merely re-tested liveness would delete someone else's lock.
  out="$(legacy_lock_drive_shimmed "$tmp" "$lane" "$d/mvshim.sh" repid "$other")"; rc=$?

  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ "$out" == *"$other"* && "$out" == *"RESTORED"* ]]; then
    pass "legacy-lock identity: a renamed-aside lock recording pid $other, NOT the pid $dead judged dead, REFUSES and is RESTORED — identity, not just liveness, is required before a delete"
  else
    fail "legacy-lock-identity: rc=$rc out=[$out] — expected a refusal naming pid $other and a RESTORED lock"
  fi
  if [[ -d "$legacy" && "$(cat "$legacy/pid" 2>/dev/null)" == "$other" ]]; then
    pass "legacy-lock identity: the unidentified lock is back at $legacy untouched (pid $other), and nothing was deleted"
  else
    fail "legacy-lock-identity-lost: $legacy holds [$(cat "$legacy/pid" 2>/dev/null || echo ABSENT)] (expected $other)"
  fi
  if [[ "$out" != *"ACQUIRED="* && ! -e "$derived" ]]; then
    pass "legacy-lock identity: the refusal acquired nothing"
  else
    fail "legacy-lock-identity-sideeffect: out=[$out]"
  fi
  rm -rf "$legacy" "$derived"
}

test_legacy_global_lock_restore_blocked_and_failed_refuse() {
  local d tmp lane legacy dead other racer out rc
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"; lane="lane3549rest$$"
  legacy_lock_mv_shim "$d/mvshim.sh"

  # (a) THE NAME IS RE-OCCUPIED while we hold the aside. `mv <dir> <existing dir>` would move ours
  # INSIDE the racer's lock, so the restore must REFUSE and PRESERVE, never clobber.
  tmp="$d/tmp-a"; mkdir -p "$tmp"
  legacy="$tmp/cqlite-worker-supervisor.lock"
  dead="$(legacy_lock_reaped_pid)"
  other="$(legacy_lock_reaped_pid)"
  mkdir -p "$legacy"; printf '%s\n' "$dead" >"$legacy/pid"
  sleep 300 &
  racer=$!
  out="$(legacy_lock_drive_shimmed "$tmp" "$lane" "$d/mvshim.sh" repid-occupy "$other" "$racer")"; rc=$?
  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ "$out" == *"PRESERVED at"* && "$out" == *".aside."* ]]; then
    pass "legacy-lock restore-blocked: when the freed name is re-occupied, the guard REFUSES and PRESERVES the renamed-aside lock, naming the path an operator must restore"
  else
    fail "legacy-lock-restore-blocked: rc=$rc out=[$out] — expected a refusal naming a PRESERVED aside path"
  fi
  if [[ "$(cat "$legacy/pid" 2>/dev/null)" == "$racer" ]] && compgen -G "$tmp/*.aside.*/lock/pid" >/dev/null; then
    pass "legacy-lock restore-blocked: the re-occupying LIVE lock (pid $racer) is NOT clobbered and the aside object still exists — nothing was deleted either way"
  else
    fail "legacy-lock-restore-blocked-clobber: legacy pid=[$(cat "$legacy/pid" 2>/dev/null || echo ABSENT)] aside-present=$(compgen -G "$tmp/*.aside.*/lock/pid" >/dev/null && echo yes || echo no)"
  fi
  kill "$racer" 2>/dev/null || true
  wait "$racer" 2>/dev/null || true

  # (b) THE RESTORE ITSELF FAILS. Refuse with a diagnostic that says the lock could not be put back
  # and names the aside path — never proceed, never delete.
  #
  # THE FAILURE IS INJECTED AT THE SEAM, NOT VIA `chmod` (roborev job 179, Low). Removing write
  # permission from the container is not a control on a PRIVILEGED run — root renames inside an
  # unwritable directory regardless — so under root the restore would SUCCEED and this case would
  # false-FAIL, on a suite that explicitly supports root elsewhere. The interposed `mv` already exists
  # for the replacement cases, so the restore call is failed there instead: deterministic, and
  # identical for every uid.
  tmp="$d/tmp-b"; mkdir -p "$tmp"
  legacy="$tmp/cqlite-worker-supervisor.lock"
  dead="$(legacy_lock_reaped_pid)"
  other="$(legacy_lock_reaped_pid)"
  mkdir -p "$legacy"; printf '%s\n' "$dead" >"$legacy/pid"
  out="$(legacy_lock_drive_shimmed "$tmp" "$lane" "$d/mvshim.sh" repid-restorefail "$other")"; rc=$?
  if [[ -e "$tmp/.shim-fired.restore" ]]; then
    pass "legacy-lock restore-failed NON-VACUITY: the guard really ATTEMPTED the restore rename (the shim failed that specific call), so this case measured the failed-restore branch"
  else
    fail "legacy-lock-restore-failed-vacuous: the restore mv was never attempted; this case measured nothing"
  fi
  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ "$out" == *"RESTORE FAILED"* && "$out" == *".aside."* ]]; then
    pass "legacy-lock restore-failed: a restore that cannot be performed REFUSES, says so, and names the preserved aside path"
  else
    fail "legacy-lock-restore-failed: rc=$rc out=[$out] — expected a RESTORE FAILED refusal naming the aside path"
  fi
  if compgen -G "$tmp/*.aside.*/lock/pid" >/dev/null; then
    pass "legacy-lock restore-failed: the aside object still exists, so an operator can put it back by hand"
  else
    fail "legacy-lock-restore-failed-deleted: the aside object was deleted after a failed restore"
  fi
  rm -rf "$d/tmp-a" "$d/tmp-b"
}

test_legacy_global_lock_recheck_after_reclaim() {
  local d tmp lane legacy dead racer out rc derived
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"; lane="lane3549retk$$"
  mkdir -p "$tmp"
  legacy="$tmp/cqlite-worker-supervisor.lock"
  derived="$tmp/cqlite-worker-supervisor-$lane.lock"
  legacy_lock_mv_shim "$d/mvshim.sh"

  dead="$(legacy_lock_reaped_pid)"
  mkdir -p "$legacy"; printf '%s\n' "$dead" >"$legacy/pid"
  sleep 300 &
  racer=$!

  # The reclaim itself is legitimate (the aside IS the dead lock we judged), but the freed name is
  # taken by a LIVE pre-#3467 supervisor immediately afterwards. The post-reclaim recheck must catch
  # it rather than co-run. This NARROWS the later-start window; see the RESIDUAL block in the guard.
  out="$(legacy_lock_drive_shimmed "$tmp" "$lane" "$d/mvshim.sh" retake "$racer")"; rc=$?

  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ "$out" == *"RETAKEN"* && "$out" == *"$racer"* ]]; then
    pass "legacy-lock recheck: a legacy name RETAKEN by a LIVE holder immediately after a legitimate reclaim refuses the start (recorded pid $racer)"
  else
    fail "legacy-lock-recheck: rc=$rc out=[$out] — expected a RETAKEN refusal naming pid $racer"
  fi
  if [[ "$(cat "$legacy/pid" 2>/dev/null)" == "$racer" && "$out" != *"ACQUIRED="* && ! -e "$derived" ]]; then
    pass "legacy-lock recheck: the retaking holder's lock is untouched and the refusal acquired nothing"
  else
    fail "legacy-lock-recheck-sideeffect: legacy pid=[$(cat "$legacy/pid" 2>/dev/null || echo ABSENT)] out=[$out]"
  fi
  kill "$racer" 2>/dev/null || true
  wait "$racer" 2>/dev/null || true
  rm -rf "$legacy" "$derived"
}

test_legacy_global_lock_residual_recorded() {
  local block
  # The guard REDUCES, and does not eliminate, the collision window (roborev job 178, Half B: a
  # pre-#3467 supervisor STARTING AFTER our check cannot be stopped without machine-global exclusion,
  # which #3393's owner ruling forbids). That is a documented risk only if it is actually written down
  # where the guard is read. Pins the RECORD, not its prose.
  block="$(sed -n '/^# RESIDUAL (#3549/,/^supervisor_legacy_lock_guard()/p' "$SUPERVISOR")"
  if [[ -n "$block" && "$block" == *"#3393"* && "$block" == *"REDUCES"* ]]; then
    pass "legacy-lock RESIDUAL: the guard records the unclosed later-start window, why #3393 forbids closing it, and that the guard reduces rather than eliminates the collision window"
  else
    fail "legacy-lock-residual: no RESIDUAL block naming #3393 and the reduction is recorded at the guard"
  fi
}

# The three COLLISION SHAPES at the aside destination (#3549, roborev job 179 Medium).
#
# THE DEFECT: the aside destination used to be `$legacy.stale.$$`, a PID-DERIVED name. The
# refuse-and-preserve paths deliberately LEAVE an aside behind, so after ordinary OS pid reuse a later
# run's destination ALREADY EXISTS — and `mv` does not fail on an existing directory, it MOVES THE
# SOURCE INSIDE IT. The pid re-identification then reads the PREVIOUS preserved lock's pid, and every
# decision after it is about the wrong object (including a restore that moves the whole nested thing
# back onto the legacy name).
#
# THE PROPERTY: the destination is a child of a FRESHLY CREATED private directory, so it is provably
# absent and a planted object at the pid-derived name is IRRELEVANT — the reclaim completes and the
# planted object is neither read nor touched. Each shape breaks the pid-derived scheme differently:
#   file     `mv <dir> <existing file>` FAILS outright ("cannot overwrite non-directory")
#   dir      NESTS silently — the re-identification reads the decoy's pid
#   symlink  follows the link and nests inside its TARGET, corrupting a third directory too
# The decoy pid is itself DEAD, so a pid-derived run reaches the identity branch rather than the
# liveness one: identity, not liveness, is what the shapes are about.
test_legacy_global_lock_aside_destination_collisions() {
  local d shape tmp lane legacy derived dead other out rc decoy
  d="$(new_case_dir)"
  common_env "$d"
  legacy_lock_mv_shim "$d/mvshim.sh"

  for shape in file dir link; do
    tmp="$d/tmp-$shape"; mkdir -p "$tmp"
    lane="lane3549col${shape}$$"
    legacy="$tmp/cqlite-worker-supervisor.lock"
    derived="$tmp/cqlite-worker-supervisor-$lane.lock"
    dead="$(legacy_lock_reaped_pid)"
    other="$(legacy_lock_reaped_pid)"
    mkdir -p "$legacy"; printf '%s\n' "$dead" >"$legacy/pid"

    out="$(legacy_lock_drive_shimmed "$tmp" "$lane" "$d/mvshim.sh" "decoy-$shape" "$other")"; rc=$?
    decoy="$(cat "$tmp/.shim-fired.decoy" 2>/dev/null || true)"

    if [[ -e "$tmp/.shim-fired" && -n "$decoy" ]]; then
      pass "legacy-lock collision ($shape) NON-VACUITY: the interposed rename FIRED and planted a preserved aside at the pid-derived destination [$decoy]"
    else
      fail "legacy-lock-collision-vacuous-$shape: the shim never fired or planted nothing (decoy=[$decoy]); this case measured nothing"
    fi

    # THE RECLAIM COMPLETES. Under a pid-derived destination this is where each shape breaks: the
    # `file` shape refuses ("stale reclaim FAILED"), and `dir`/`link` nest and then refuse on the
    # decoy's pid instead of ours.
    # LOCKDIR=yes is printed by the driver WHILE the lock is held: the supervisor's own EXIT trap
    # removes the per-lane lock, so a post-exit `-d` test could never see it.
    if [[ "$rc" -eq 0 ]] && [[ "$out" == *"ACQUIRED=$derived"* && "$out" == *"LOCKDIR=yes"* ]]; then
      pass "legacy-lock collision ($shape): a pre-existing object at the pid-derived aside name does NOT affect the reclaim — the guard's own destination was freshly created, so the start SUCCEEDS"
    else
      fail "legacy-lock-collision-$shape: rc=$rc out=[$out] — expected a completed reclaim and an ACQUIRED per-lane lock"
    fi

    # THE STALE LEGACY LOCK IS GONE and no aside residue survives a completed reclaim.
    if [[ ! -e "$legacy" ]] && ! compgen -G "$tmp/*.aside.*" >/dev/null; then
      pass "legacy-lock collision ($shape): the stale legacy lock is gone and the private aside directory was cleaned up"
    else
      fail "legacy-lock-collision-residue-$shape: legacy-exists=$([[ -e "$legacy" ]] && echo yes || echo no) aside-residue=$(compgen -G "$tmp/*.aside.*" >/dev/null && echo yes || echo no)"
    fi

    # THE PLANTED OBJECT IS UNTOUCHED — shape-exact, because "still exists" would pass on a directory
    # that had our lock nested inside it.
    case "$shape" in
      file)
        if [[ -f "$decoy" && ! -L "$decoy" && "$(cat "$decoy" 2>/dev/null)" == "preserved-by-an-earlier-run" ]]; then
          pass "legacy-lock collision (file): the planted FILE at the pid-derived name is still a regular file with its original content"
        else
          fail "legacy-lock-collision-file-touched: [$decoy] is no longer the planted regular file (content=[$(cat "$decoy" 2>/dev/null || echo ABSENT)])"
        fi
        ;;
      dir)
        if [[ -d "$decoy" && ! -L "$decoy" && "$(cat "$decoy/pid" 2>/dev/null)" == "$other" && ! -e "$decoy/cqlite-worker-supervisor.lock" && ! -e "$decoy/lock" ]]; then
          pass "legacy-lock collision (dir): the planted DIRECTORY still records its own pid $other and nothing was NESTED inside it"
        else
          fail "legacy-lock-collision-dir-nested: [$decoy] pid=[$(cat "$decoy/pid" 2>/dev/null || echo ABSENT)] contents=[$(ls -A "$decoy" 2>/dev/null | tr '\n' ' ')]"
        fi
        ;;
      link)
        if [[ -L "$decoy" && "$(readlink "$decoy")" == "$decoy.target" ]] && [[ -z "$(ls -A "$decoy.target" 2>/dev/null)" ]]; then
          pass "legacy-lock collision (link): the planted SYMLINK still points at its own target and nothing was nested THROUGH it into that target"
        else
          fail "legacy-lock-collision-link-followed: [$decoy] link=[$(readlink "$decoy" 2>/dev/null || echo NOT-A-LINK)] target-contents=[$(ls -A "$decoy.target" 2>/dev/null | tr '\n' ' ')]"
        fi
        ;;
    esac
    rm -rf "$tmp"
  done
}

# THE RACE ITSELF, with a REAL SECOND PROCESS and a FORCED ORDERING (#3549, lead ruling: "test the
# race, not just the outcome — a test that reclaims a stale lock and passes proves nothing about the
# interleaving").
#
# HOW THE ORDERING IS FORCED, AND WHY THERE IS NO SEAM IN THE SHIPPED SCRIPT. The pause is the
# interposed primitive itself: the guard's OWN `mv` (and, on the restore path, its own `mkdir`) runs the
# competitor and BLOCKS until that process has taken the lock and is alive. So the pause sits exactly
# between the guard's classify and its act, is observable (a ready file carrying the live pid) rather
# than timed, and the shipped `worker-supervisor.sh` gains NO test-only hook — a seam in the shipped
# script is one more thing a real invoker can set, and none is needed here.
#
# THE COMPETITOR IS A REAL PROCESS running the pre-#3467 reclaim-and-acquire and then `exec sleep`, so
# the pid recorded in the lock is genuinely live for the whole case — nothing is stubbed or simulated.
test_legacy_global_lock_real_competing_reclaim() {
  local d tmp lane legacy derived dead out rc comp comp2 aside
  d="$(new_case_dir)"
  common_env "$d"
  legacy_lock_mv_shim "$d/mvshim.sh"
  legacy_lock_competitor_script "$d/competitor.sh"
  LEGACY_LOCK_COMPETITOR="$d/competitor.sh"

  # (a) THE COMPETITOR ACTS FIRST. It reclaims the dead lock itself and installs its OWN live pid, so
  # our rename detaches a LIVE HOLDER'S LOCK. The property is not merely "we refuse": it is that the
  # live holder STILL HAS ITS LOCK afterwards — detaching it and failing to put it back would leave it
  # running lockless, which is the harm a later pre-#3467 supervisor would then co-run with.
  tmp="$d/tmp-a"; mkdir -p "$tmp"
  lane="lane3549raceA$$"
  legacy="$tmp/cqlite-worker-supervisor.lock"
  derived="$tmp/cqlite-worker-supervisor-$lane.lock"
  dead="$(legacy_lock_reaped_pid)"
  mkdir -p "$legacy"; printf '%s\n' "$dead" >"$legacy/pid"

  out="$(legacy_lock_drive_shimmed "$tmp" "$lane" "$d/mvshim.sh" competitor "")"; rc=$?
  comp="$(cat "$tmp/.competitor-ready" 2>/dev/null || true)"

  if [[ -e "$tmp/.shim-fired" ]] && [[ "$comp" =~ ^[0-9]+$ ]] && kill -0 "$comp" 2>/dev/null; then
    pass "legacy-lock RACE (a) NON-VACUITY: the forced pause fired and a REAL second process (pid $comp) completed the competing pre-#3467 reclaim and is ALIVE holding the lock"
  else
    fail "legacy-lock-race-a-vacuous: shim-fired=$([[ -e "$tmp/.shim-fired" ]] && echo yes || echo no) competitor=[$comp] alive=$(kill -0 "$comp" 2>/dev/null && echo yes || echo no) — the interleaving was not forced and this case measured nothing"
  fi
  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ "$out" == *"$comp"* ]]; then
    pass "legacy-lock RACE (a): a competing reclaim that wins the classify->act window REFUSES the start, naming the live pid $comp it would not delete"
  else
    fail "legacy-lock-race-a: rc=$rc out=[$out] — expected the LEGACY refusal naming the competitor pid $comp"
  fi
  # THE PROPERTY the lead named: the live holder is NOT left lockless.
  if [[ -d "$legacy" && "$(cat "$legacy/pid" 2>/dev/null)" == "$comp" ]] && kill -0 "$comp" 2>/dev/null; then
    pass "legacy-lock RACE (a): the live competitor (pid $comp) still holds its lock at $legacy — it was put back, not destroyed and not left detached"
  else
    fail "legacy-lock-race-a-lockless: legacy pid=[$(cat "$legacy/pid" 2>/dev/null || echo ABSENT)] competitor-alive=$(kill -0 "$comp" 2>/dev/null && echo yes || echo no) — a live holder was left without its lock"
  fi
  if [[ "$out" != *"ACQUIRED="* && ! -e "$derived" ]] && ! compgen -G "$tmp/*.aside.*" >/dev/null; then
    pass "legacy-lock RACE (a): the refusal acquired nothing and left no aside residue"
  else
    fail "legacy-lock-race-a-sideeffect: out=[$out] derived=$([[ -e "$derived" ]] && echo yes || echo no) residue=$(compgen -G "$tmp/*.aside.*" >/dev/null && echo yes || echo no)"
  fi
  kill "$comp" 2>/dev/null || true

  # (b) A SECOND REAL PROCESS TAKES THE FREED NAME AT THE RESTORE SEAM. The restore must be arbitrated
  # by an exclusive create, not by a pre-check: a check-then-act restore whose `mv` runs after the name
  # is retaken NESTS our aside INSIDE the new holder's lock.
  tmp="$d/tmp-b"; mkdir -p "$tmp"
  lane="lane3549raceB$$"
  legacy="$tmp/cqlite-worker-supervisor.lock"
  derived="$tmp/cqlite-worker-supervisor-$lane.lock"
  dead="$(legacy_lock_reaped_pid)"
  mkdir -p "$legacy"; printf '%s\n' "$dead" >"$legacy/pid"

  out="$(legacy_lock_drive_shimmed "$tmp" "$lane" "$d/mvshim.sh" competitor-restore "")"; rc=$?
  comp="$(cat "$tmp/.competitor-ready" 2>/dev/null || true)"
  comp2="$(cat "$tmp/.competitor2-ready" 2>/dev/null || true)"

  if [[ -e "$tmp/.shim-fired.restore" ]] && [[ "$comp2" =~ ^[0-9]+$ ]] && kill -0 "$comp2" 2>/dev/null; then
    pass "legacy-lock RACE (b) NON-VACUITY: the guard reached its RESTORE primitive and a REAL second process (pid $comp2) took the freed legacy name first and is ALIVE"
  else
    fail "legacy-lock-race-b-vacuous: restore-seam=$([[ -e "$tmp/.shim-fired.restore" ]] && echo yes || echo no) competitor2=[$comp2] alive=$(kill -0 "$comp2" 2>/dev/null && echo yes || echo no) — the restore interleaving was not forced"
  fi
  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ "$out" == *"PRESERVED at"* && "$out" == *".aside."* ]]; then
    pass "legacy-lock RACE (b): losing the freed name to a real process makes the RESTORE observe a primitive failure — the guard PRESERVES its aside and names it, rather than acting on the wrong object"
  else
    fail "legacy-lock-race-b: rc=$rc out=[$out] — expected a refusal naming a PRESERVED aside path"
  fi
  # THE PROPERTY: nothing was nested into, or written over, the new holder's lock.
  if [[ "$(cat "$legacy/pid" 2>/dev/null)" == "$comp2" ]] && [[ ! -e "$legacy/lock" ]] && [[ -z "$(command ls -A "$legacy" 2>/dev/null | command grep -v '^pid$' || true)" ]]; then
    pass "legacy-lock RACE (b): the new holder's lock still records ONLY its own pid $comp2 — our aside was not nested inside it and not clobbered over it"
  else
    fail "legacy-lock-race-b-nested: legacy pid=[$(cat "$legacy/pid" 2>/dev/null || echo ABSENT)] contents=[$(command ls -A "$legacy" 2>/dev/null | tr '\n' ' ')] — the restore acted on the wrong object"
  fi
  aside="$(compgen -G "$tmp/*.aside.*/lock/pid" || true)"
  if [[ -n "$aside" && "$(cat "$aside" 2>/dev/null)" == "$comp" ]]; then
    pass "legacy-lock RACE (b): the first competitor's lock (pid $comp) is PRESERVED intact in the aside — nothing was deleted on either side of the race"
  else
    fail "legacy-lock-race-b-preserved: aside pid file=[$aside] holds [$(cat "$aside" 2>/dev/null || echo ABSENT)] (expected $comp)"
  fi
  kill "$comp" "$comp2" 2>/dev/null || true

  unset LEGACY_LOCK_COMPETITOR
  rm -rf "$d/tmp-a" "$d/tmp-b"
}

t test_legacy_global_lock_replacement_race_preserves_live_lock
t test_legacy_global_lock_real_competing_reclaim
t test_legacy_global_lock_aside_destination_collisions
t test_legacy_global_lock_identity_mismatch_restores
t test_legacy_global_lock_restore_blocked_and_failed_refuse
t test_legacy_global_lock_recheck_after_reclaim
t test_legacy_global_lock_residual_recorded

# ---------------------------------------------------------------------------
# Test 43-lock (#3549): LIVENESS IS THREE-VALUED, and a failed `kill -0` DOES NOT MEAN DEAD.
#
# `kill -0` fails with ESRCH (dead) AND with EPERM (alive, owned by another user). A supervisor
# started by a different user during a rolling update is exactly the case the legacy guard exists for,
# so collapsing EPERM onto "dead" would RECLAIM A LIVE HOLDER'S LOCK — the worst outcome available.
#
# A REAL EPERM subject is available on any box where this suite runs unprivileged: pid 1. It is
# unambiguously alive and `kill -0 1` fails for a non-root user, so no stub is needed anywhere in this
# case. Under root there is no EPERM and the sub-assertion is SKIPped, never silently passed.
# ---------------------------------------------------------------------------
test_legacy_lock_liveness_is_three_valued() {
  local body live dead ans naive
  body="$T_LOCKFN/liveness.sh"
  mkdir -p "$T_LOCKFN"
  # The shipped function, read out of the supervisor at run time — never re-implemented here.
  {
    printf '%s\n' '#!/usr/bin/env bash'
    sed -n '/^supervisor_pid_liveness()/,/^}/p' "$SUPERVISOR"
    printf '%s\n' 'supervisor_pid_liveness "$1"'
  } >"$body"

  sleep 300 &
  live=$!
  ans=$(bash "$body" "$live")
  if [[ "$ans" == live ]]; then
    pass "liveness: a REAL running pid answers 'live'"
  else
    fail "liveness-live: pid $live answered [$ans]"
  fi
  kill "$live" 2>/dev/null || true
  wait "$live" 2>/dev/null || true

  sleep 0.1 &
  dead=$!
  wait "$dead" 2>/dev/null || true
  ans=$(bash "$body" "$dead")
  if [[ "$ans" == dead ]]; then
    pass "liveness: a REAL reaped pid answers 'dead' (affirmatively corroborated, not inferred from a failed kill -0)"
  else
    fail "liveness-dead: reaped pid $dead answered [$ans]"
  fi

  # THE EPERM CASE, with a real subject.
  if kill -0 1 2>/dev/null; then
    skip "liveness EPERM: this run can signal pid 1 (root), so no EPERM subject exists on this box"
  else
    ans=$(bash "$body" 1)
    if [[ "$ans" == live ]]; then
      pass "liveness AC-EPERM: pid 1 — alive but UNSIGNALLABLE by this user — answers 'live', so an EPERM holder's lock can never be reclaimed as stale"
    else
      fail "liveness-eperm: pid 1 answered [$ans]; a failed kill -0 was read as absence, which would reclaim a LIVE holder's lock"
    fi
    # NON-VACUITY: the naive one-oracle implementation DOES get pid 1 wrong, so the corroboration
    # above is doing real work rather than agreeing with the simpler thing.
    naive=$(kill -0 1 2>/dev/null && printf live || printf dead)
    if [[ "$naive" == dead ]]; then
      pass "liveness NON-VACUITY: a bare kill -0 calls pid 1 'dead' — the defect the corroboration removes, measured rather than asserted"
    else
      fail "liveness-nonvacuity: the bare kill -0 control answered [$naive] for pid 1; the comparison that motivates corroboration is not established"
    fi
  fi

  # Malformed / dangerous inputs are 'unknown', never a verdict: pid 0 signals the whole PROCESS
  # GROUP, and a leading-zero pid is a bash arithmetic error waiting to happen.
  local bad ok=yes
  for bad in "" 0 007 abc "12 34" -1; do
    ans=$(bash "$body" "$bad")
    [[ "$ans" == unknown ]] || { ok="no ([$bad] -> $ans)"; break; }
  done
  if [[ "$ok" == yes ]]; then
    pass "liveness: empty, 0, leading-zero, non-numeric, multi-token and negative pids all answer 'unknown' — never live, never dead"
  else
    fail "liveness-malformed: $ok"
  fi
}

t test_legacy_lock_liveness_is_three_valued

echo "=== $PASS_COUNT passed, $FAIL_COUNT failed, $SKIP_COUNT skipped ==="
[[ "$FAIL_COUNT" -eq 0 ]]
