#!/usr/bin/env bash
# aggregate-required-tiers.sh — make the single `required` context mean
# "every gating tier for this diff already passed" (issue #2910).
#
# Branch protection requires exactly ONE context, `required`, and a GitHub
# Actions job cannot `needs:` a job in another workflow. This script closes that
# hole from inside the `required` job: it polls the PULL REQUEST HEAD's sibling
# check runs and FAILS CLOSED on any tier declared in `.github/ci-gating-tiers.yml`
# that is failed, still non-terminal at the deadline, or ABSENT.
#
# Absence is never read as inapplicability. A registered tier always emits its
# context (an inapplicable tier reports an explicit success), so an absent
# registered context is unconditionally an error. Every failure mode of this
# script therefore reds the gate rather than opening it.
#
# BUT A FALSE RED IS ALSO AN OUTAGE. Failing closed is correct AT THE DEADLINE;
# mid-poll it is merely brittle, and a gate that reds legitimate PRs on one API
# blip trains people to re-run reflexively, which erodes the signal it exists to
# give. So a transient fetch failure is RETRIED with backoff inside the poll
# window and only fails closed once it persists (to the deadline, or past the
# consecutive-failure ceiling).
#
# The decision surface is pure and lives in scripts/ci/gating_registry.rb; this
# script owns only the polling loop, the deadline, self-exclusion inputs and the
# job summary. Every input is INJECTABLE so the whole thing runs offline against
# synthetic fixtures with no network and no sleeping (see
# scripts/tests/test_aggregate_required_tiers.sh).
#
# Usage:
#   scripts/ci/aggregate-required-tiers.sh [options]
#
# Options (all also settable by the matching environment variable):
#   --registry PATH            registry file            [.github/ci-gating-tiers.yml]
#   --repo OWNER/NAME          repository slug          [$GITHUB_REPOSITORY]
#   --head-sha SHA             PR head sha              [$PR_HEAD_SHA]
#   --pr-number N              PR number (live labels)  [$PR_NUMBER]
#   --run-id ID                this workflow run id     [$GITHUB_RUN_ID]
#   --labels CSV               fallback PR labels       [$PR_LABELS]
#   --waiver-events-cmd CMD    command printing `<label>\t<actor>\t<iso8601>` per
#                              `labeled` event, oldest first (waiver attribution
#                              and the pending-waiver horizon)      [gh api]
#   --deadline-minutes N       aggregation deadline     [registry effective max]
#   --deadline-epoch N         absolute deadline (unix seconds); wins over the above
#   --poll-attempts N          hard cap on fetches      [unbounded until deadline]
#   --poll-initial-seconds N   first backoff interval   [15]
#   --poll-max-seconds N       backoff ceiling          [60]
#   --max-fetch-failures N     consecutive transient fetch failures tolerated [6]
#   --core-context NAME        the gate's own compute job's context [pr-gate-core]
#   --core-result RESULT       `needs.<core>.result` in THIS run    [$CORE_RESULT]
#   --event-action ACTION      the pull_request activity type       [$EVENT_ACTION]
#   --core-runs-cmd CMD        command printing that context's check runs [gh api]
#   --event-workflows-dir DIR  workflow definitions of the tree THIS EVENT ran,
#                              for migration detection      [$EVENT_WORKFLOWS_DIR]
#   --base-ref REF             this pull request's base branch        [$BASE_REF]
#   --check-runs-cmd CMD       command printing check-run JSON/NDJSON [gh api]
#   --self-jobs-cmd CMD        command printing this run's job ids    [gh api]
#   --labels-cmd CMD           command printing the PR's CURRENT label names [gh api]
#   --now EPOCH                fixed clock for supersession ageing [date +%s per poll]
#   --summary-file PATH        markdown summary sink    [$GITHUB_STEP_SUMMARY]
#   --sleep-cmd CMD            sleep implementation     [sleep]
#
# Exit status: 0 only when every registered tier cleared (or was legitimately
# waived); non-zero otherwise. There is no other success path.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

REGISTRY="${GATING_REGISTRY:-$REPO_ROOT/.github/ci-gating-tiers.yml}"
REPO_SLUG="${GITHUB_REPOSITORY:-}"
HEAD_SHA="${PR_HEAD_SHA:-}"
PR_NUMBER_IN="${PR_NUMBER:-}"
RUN_ID="${GITHUB_RUN_ID:-}"
PR_LABELS_IN="${PR_LABELS:-}"
DEADLINE_MINUTES=""
DEADLINE_EPOCH=""
POLL_ATTEMPTS=""
POLL_INITIAL_SECONDS="${POLL_INITIAL_SECONDS:-15}"
POLL_MAX_SECONDS="${POLL_MAX_SECONDS:-60}"
MAX_FETCH_FAILURES="${MAX_FETCH_FAILURES:-6}"
CHECK_RUNS_CMD="${CHECK_RUNS_CMD:-}"
SELF_JOBS_CMD="${SELF_JOBS_CMD:-}"
LABELS_CMD="${LABELS_CMD:-}"
WAIVER_EVENTS_CMD="${WAIVER_EVENTS_CMD:-}"
CORE_CONTEXT="${CORE_CONTEXT:-pr-gate-core}"
CORE_RESULT_IN="${CORE_RESULT:-}"
EVENT_ACTION_IN="${EVENT_ACTION:-}"
CORE_RUNS_CMD="${CORE_RUNS_CMD:-}"
EVENT_WORKFLOWS_DIR="${EVENT_WORKFLOWS_DIR:-}"
BASE_REF_IN="${BASE_REF:-}"
NOW_OVERRIDE=""
SUMMARY_FILE="${GITHUB_STEP_SUMMARY:-}"
SLEEP_CMD="${SLEEP_CMD:-sleep}"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --registry) REGISTRY="$2"; shift 2 ;;
    --repo) REPO_SLUG="$2"; shift 2 ;;
    --head-sha) HEAD_SHA="$2"; shift 2 ;;
    --pr-number) PR_NUMBER_IN="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    --labels) PR_LABELS_IN="$2"; shift 2 ;;
    --deadline-minutes) DEADLINE_MINUTES="$2"; shift 2 ;;
    --deadline-epoch) DEADLINE_EPOCH="$2"; shift 2 ;;
    --poll-attempts) POLL_ATTEMPTS="$2"; shift 2 ;;
    --poll-initial-seconds) POLL_INITIAL_SECONDS="$2"; shift 2 ;;
    --poll-max-seconds) POLL_MAX_SECONDS="$2"; shift 2 ;;
    --max-fetch-failures) MAX_FETCH_FAILURES="$2"; shift 2 ;;
    --core-context) CORE_CONTEXT="$2"; shift 2 ;;
    --core-result) CORE_RESULT_IN="$2"; shift 2 ;;
    --event-action) EVENT_ACTION_IN="$2"; shift 2 ;;
    --core-runs-cmd) CORE_RUNS_CMD="$2"; shift 2 ;;
    --event-workflows-dir) EVENT_WORKFLOWS_DIR="$2"; shift 2 ;;
    --base-ref) BASE_REF_IN="$2"; shift 2 ;;
    --check-runs-cmd) CHECK_RUNS_CMD="$2"; shift 2 ;;
    --self-jobs-cmd) SELF_JOBS_CMD="$2"; shift 2 ;;
    --labels-cmd) LABELS_CMD="$2"; shift 2 ;;
    --waiver-events-cmd) WAIVER_EVENTS_CMD="$2"; shift 2 ;;
    --now) NOW_OVERRIDE="$2"; shift 2 ;;
    --summary-file) SUMMARY_FILE="$2"; shift 2 ;;
    --sleep-cmd) SLEEP_CMD="$2"; shift 2 ;;
    # Print the header block, however long it grows — a hard line range silently
    # starts printing code once someone documents a new option.
    -h|--help) awk 'NR > 1 { if (!/^#/) exit; print }' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "aggregate-required-tiers: unknown argument '$1'" >&2; exit 2 ;;
  esac
done

fail_closed() {
  echo "::error::required-tier aggregation could not run: $*" >&2
  echo "aggregate-required-tiers: FAIL (harness) — $*" >&2
  exit 2
}

command -v ruby >/dev/null 2>&1 || fail_closed "ruby is unavailable (needed to read the gating-tier registry)"
# THE RUBY FLOOR (issue #2910 round 4). Ruby is the single implementation path
# — the python3 fallbacks were removed — so its version floor became
# load-bearing. Probe it as a SCRIPT (exit 0/1) rather than letting a library
# require abort mid-flight, so the diagnostic names the floor and the remedy.
RUBY_FLOOR_RB="$REPO_ROOT/scripts/ci/gating_ruby_floor.rb"
[ -f "$RUBY_FLOOR_RB" ] ||
  fail_closed "scripts/ci/gating_ruby_floor.rb not found (the declared ruby floor cannot be checked)"
# stderr only; the script writes nothing to a file, so no checkout is dirtied.
FLOOR_MSG="$(ruby "$RUBY_FLOOR_RB" 2>&1 >/dev/null)" ||
  fail_closed "${FLOOR_MSG:-ruby is older than the declared gating floor}"
[ -f "$REGISTRY" ] || fail_closed "registry not found at $REGISTRY"

REGISTRY_RB="$REPO_ROOT/scripts/ci/gating_registry.rb"
[ -f "$REGISTRY_RB" ] || fail_closed "scripts/ci/gating_registry.rb not found"

# Shape-validate everything interpolated into a command string BEFORE it reaches
# `eval` (repo injection doctrine; roborev-lints mechanises the GHA half). These
# values arrive from the workflow's event context, so an allowlist is cheap
# insurance rather than a theoretical concern.
case "$REPO_SLUG" in
  "" ) : ;;
  *[!A-Za-z0-9._/-]*|*/*/*|/*|*/ ) fail_closed "--repo/GITHUB_REPOSITORY is not an OWNER/NAME slug: '$REPO_SLUG'" ;;
  */* ) : ;;
  * ) fail_closed "--repo/GITHUB_REPOSITORY is not an OWNER/NAME slug: '$REPO_SLUG'" ;;
esac
case "$HEAD_SHA" in
  "" ) : ;;
  *[!0-9a-fA-F]* ) fail_closed "--head-sha/PR_HEAD_SHA is not a hex commit sha: '$HEAD_SHA'" ;;
esac
case "$PR_NUMBER_IN" in
  "" ) : ;;
  *[!0-9]* ) fail_closed "--pr-number/PR_NUMBER is not a positive integer: '$PR_NUMBER_IN'" ;;
esac
case "$RUN_ID" in
  "" ) : ;;
  *[!0-9]* ) fail_closed "--run-id/GITHUB_RUN_ID is not a positive integer: '$RUN_ID'" ;;
esac
# A check-run name is free text but is interpolated into a URL query, so keep it
# to the shape a job `name:` can legitimately have.
case "$CORE_CONTEXT" in
  "" ) fail_closed "--core-context must name the gate's own compute job's context" ;;
  *[!A-Za-z0-9\ ._-]* ) fail_closed "--core-context is not a plain check-run name: '$CORE_CONTEXT'" ;;
esac
case "$EVENT_ACTION_IN" in
  "" ) : ;;
  *[!a-z_]* ) fail_closed "--event-action is not a pull_request activity type: '$EVENT_ACTION_IN'" ;;
esac
# WAIVER ATTRIBUTION (issue #2910 round 4). This used to name `$GITHUB_ACTOR` —
# the actor of the event that started THIS run (a pusher, or whoever hit re-run),
# NOT whoever applied `ci:waive:<tier-id>`. Labels are re-read live on every
# poll, so a waiver applied by one person could be attributed to another on the
# audit trail of a break-glass. The real labeller is resolved from the pull
# request's `labeled` events instead; the resolved login is allowlisted in
# gating_registry.rb before it reaches a `::warning::` workflow command, and an
# unreadable feed downgrades the diagnostic to "UNRESOLVED" rather than
# guessing. The same timestamps give a pending tier's waiver its horizon.

# --------------------------------------------------- migration detection ----
# Round 2 moved the registry to the BASE ref; that split WHERE THE REGISTRY LIVES
# from WHERE THE EMITTER LIVES. If the base registers a tier whose context the
# tree this event ran cannot emit, the context can never arrive and polling it to
# the deadline would burn a runner for an hour to reach a verdict already known.
# gating_head_emitability.rb answers that question from provable properties only,
# and the verdict is ALWAYS a failure — "the head cannot emit, therefore pass"
# would be a one-line bypass. See that file's header.
case "$BASE_REF_IN" in
  "" ) : ;;
  *[!A-Za-z0-9._/-]* ) fail_closed "--base-ref is not a branch ref: '$BASE_REF_IN'" ;;
esac
EVENT_ARGS=()
if [ -n "$EVENT_WORKFLOWS_DIR" ] && [ ! -d "$EVENT_WORKFLOWS_DIR" ]; then
  echo "::warning::the event tree's workflow definitions are not at '$EVENT_WORKFLOWS_DIR'; a registered" \
       "tier whose emitter this pull request renamed or removed cannot be detected early and will instead" \
       "wait out the aggregation deadline" >&2
  EVENT_WORKFLOWS_DIR=""
elif [ -z "$EVENT_WORKFLOWS_DIR" ] && [ -n "$RUN_ID" ]; then
  echo "::warning::no --event-workflows-dir was supplied, so this run cannot tell a registered tier that is" \
       "merely SLOW from one whose emitter does not exist in the tree this event ran" >&2
fi
[ -n "$EVENT_WORKFLOWS_DIR" ] && EVENT_ARGS+=(--event-workflows-dir "$EVENT_WORKFLOWS_DIR")
[ -n "$EVENT_ACTION_IN" ] && EVENT_ARGS+=(--event-action "$EVENT_ACTION_IN")
[ -n "$BASE_REF_IN" ] && EVENT_ARGS+=(--base-ref "$BASE_REF_IN")

# Default data sources. Check runs are keyed to the PULL REQUEST HEAD sha — NOT
# github.sha, which for a pull_request event is the synthesised merge commit and
# carries no sibling check runs at all. `filter=latest` asks GitHub for the newest
# run per name; gating_registry.rb additionally keeps the highest check-run id, so
# a re-run always supersedes the attempt it replaced in BOTH directions.
if [ -z "$CHECK_RUNS_CMD" ]; then
  [ -n "$REPO_SLUG" ] || fail_closed "--repo/GITHUB_REPOSITORY is required without --check-runs-cmd"
  [ -n "$HEAD_SHA" ] || fail_closed "--head-sha/PR_HEAD_SHA is required without --check-runs-cmd"
  CHECK_RUNS_CMD="gh api --paginate \"repos/${REPO_SLUG}/commits/${HEAD_SHA}/check-runs?filter=latest&per_page=100\" --jq '.check_runs[]'"
fi
# Self-exclusion inputs: this run's own job ids. An Actions job and its check run
# share the same numeric id, so the id set is a name-independent identity.
if [ -z "$SELF_JOBS_CMD" ]; then
  if [ -n "$REPO_SLUG" ] && [ -n "$RUN_ID" ]; then
    SELF_JOBS_CMD="gh api --paginate \"repos/${REPO_SLUG}/actions/runs/${RUN_ID}/jobs?per_page=100\" --jq '.jobs[].id'"
  else
    SELF_JOBS_CMD="true"
  fi
fi
# LIVE labels (issue #2910). The event payload is a snapshot taken when the run
# started, and re-running a workflow replays that same payload — so a waiver
# label added to a wedged PR would be invisible to both. Re-reading the PR on
# every poll makes `ci:waive:<tier-id>` effective without a re-run at all. The
# payload labels remain the fallback when the read fails; a failed read can only
# ever withhold a waiver, never grant one.
if [ -z "$LABELS_CMD" ] && [ -n "$REPO_SLUG" ] && [ -n "$PR_NUMBER_IN" ]; then
  LABELS_CMD="gh api \"repos/${REPO_SLUG}/pulls/${PR_NUMBER_IN}\" --jq '.labels[].name'"
fi
# WHO applied a waiver, and WHEN (issue #2910 round 4). The issues-events API is
# the authoritative record of the `labeled` events on this pull request, returned
# OLDEST FIRST — the order gating_registry.rb expects (last matching event wins).
# Read only when a waiver label is actually present, so the ordinary path costs
# no extra API call.
if [ -z "$WAIVER_EVENTS_CMD" ] && [ -n "$REPO_SLUG" ] && [ -n "$PR_NUMBER_IN" ]; then
  WAIVER_EVENTS_CMD="gh api --paginate \"repos/${REPO_SLUG}/issues/${PR_NUMBER_IN}/events?per_page=100\" --jq '.[] | select(.event == \"labeled\") | [.label.name, (.actor.login // \"unknown\"), .created_at] | @tsv'"
fi
# The core context's ALREADY RECORDED result for this head sha. `filter=all` (not
# `latest`) is load-bearing: on a label-triggered run our own skipped core job
# mints the newest check run of that name, and `filter=latest` would return only
# that one, hiding the real result we need to honour.
if [ -z "$CORE_RUNS_CMD" ] && [ -n "$REPO_SLUG" ] && [ -n "$HEAD_SHA" ]; then
  CORE_RUNS_CMD="gh api --paginate \"repos/${REPO_SLUG}/commits/${HEAD_SHA}/check-runs?check_name=${CORE_CONTEXT// /%20}&filter=all&per_page=100\" --jq '.check_runs[]'"
fi

if [ -z "$DEADLINE_EPOCH" ]; then
  if [ -z "$DEADLINE_MINUTES" ]; then
    DEADLINE_MINUTES="$(ruby "$REGISTRY_RB" deadline --registry "$REGISTRY")" ||
      fail_closed "could not read the aggregation deadline from $REGISTRY"
  fi
  case "$DEADLINE_MINUTES" in
    ''|*[!0-9]*) fail_closed "deadline-minutes must be a non-negative integer (got '$DEADLINE_MINUTES')" ;;
  esac
  DEADLINE_EPOCH=$(( $(date +%s) + DEADLINE_MINUTES * 60 ))
fi
case "$DEADLINE_EPOCH" in
  ''|*[!0-9]*) fail_closed "deadline-epoch must be a non-negative integer (got '$DEADLINE_EPOCH')" ;;
esac

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/required-tiers.XXXXXX")"
trap 'rm -rf "$WORK_DIR"' EXIT

case "$MAX_FETCH_FAILURES" in
  ''|*[!0-9]*) fail_closed "max-fetch-failures must be a non-negative integer (got '$MAX_FETCH_FAILURES')" ;;
esac
case "$NOW_OVERRIDE" in
  '') : ;;
  *[!0-9]*) fail_closed "--now must be a unix timestamp (got '$NOW_OVERRIDE')" ;;
esac

# Self-exclusion input. A transient failure here gets the same treatment as one
# in the poll loop: retried, not instantly fatal. It still fails CLOSED once the
# retries are spent — without the job-id set the aggregator could wait on its own
# check run forever, and the details-URL fallback alone does not justify
# proceeding silently.
SELF_IDS_FILE="$WORK_DIR/self-job-ids.txt"
SELF_ATTEMPT=0
until eval "$SELF_JOBS_CMD" >"$SELF_IDS_FILE" 2>"$WORK_DIR/self-job-ids.err"; do
  SELF_ATTEMPT=$((SELF_ATTEMPT + 1))
  sed 's/^/  /' "$WORK_DIR/self-job-ids.err" >&2 || true
  if [ "$SELF_ATTEMPT" -ge 3 ]; then
    fail_closed "could not list this run's jobs (self-exclusion input) after ${SELF_ATTEMPT} attempts; command: $SELF_JOBS_CMD"
  fi
  echo "::warning::transient failure listing this run's jobs (attempt ${SELF_ATTEMPT}); retrying in ${POLL_INITIAL_SECONDS}s" >&2
  "$SLEEP_CMD" "$POLL_INITIAL_SECONDS" || true
done

echo "aggregate-required-tiers: registry=$REGISTRY deadline_epoch=$DEADLINE_EPOCH run_id=${RUN_ID:-none}"

# --------------------------------------------------- the gate's own compute --
# `required` NEVER masks its own workflow's compute job. Normally that is decided
# by this run's `needs.<core>.result`. The one exception is a LABEL-triggered run
# (issue #2910 round 2): a label mutation changes no file, so restarting the
# 30-minute core would make every `ci:perf` / board-mirror / `needs-decision`
# label — and the `ci:waive:<tier-id>` break-glass itself — cost a full gate
# re-run. The core job is skipped there, and this reads what it ALREADY concluded
# for this exact head sha instead. Skipping the WORK never skips the CHECK: an
# absent, pending, or non-success recorded result fails closed, so a label can
# never manufacture a green core.
LABEL_EVENT=false
case "$EVENT_ACTION_IN" in
  labeled|unlabeled) LABEL_EVENT=true ;;
esac

core_summary() {
  [ -n "$SUMMARY_FILE" ] && printf '%s\n' "$1" >>"$SUMMARY_FILE"
  return 0
}

verify_recorded_core() {
  [ -n "$CORE_RUNS_CMD" ] || return 1
  local attempt=0 rc=0 line=""
  while :; do
    attempt=$((attempt + 1))
    if eval "$CORE_RUNS_CMD" >"$WORK_DIR/core-runs.json" 2>"$WORK_DIR/core-runs.err"; then
      rc=0
      line="$(ruby "$REGISTRY_RB" recorded-result \
        --context "$CORE_CONTEXT" \
        --check-runs "$WORK_DIR/core-runs.json" \
        --exclude-ids-file "$SELF_IDS_FILE" \
        --run-id "${RUN_ID:-}")" || rc=$?
      CORE_OBSERVED="$line"
      return "$rc"
    fi
    sed 's/^/  /' "$WORK_DIR/core-runs.err" >&2 || true
    [ "$attempt" -ge 3 ] && return 1
    echo "::warning::transient failure reading the recorded ${CORE_CONTEXT} result (attempt ${attempt}); retrying" >&2
    "$SLEEP_CMD" "$POLL_INITIAL_SECONDS" || true
  done
}

CORE_OBSERVED=""
case "$CORE_RESULT_IN" in
  "" )
    # No core result injected at all (e.g. a standalone invocation): nothing to
    # assert, the tier aggregation below is the whole job.
    : ;;
  success )
    echo "${CORE_CONTEXT}: success (this run)" ;;
  skipped )
    if [ "$LABEL_EVENT" != "true" ]; then
      echo "::error::${CORE_CONTEXT} was skipped on a '${EVENT_ACTION_IN:-unknown}' event; only a label" \
           "mutation may skip it, so required fails closed." >&2
      core_summary "**required: FAILED — \`${CORE_CONTEXT}\` was skipped on a non-label event (\`${EVENT_ACTION_IN:-unknown}\`).**"
      exit 1
    fi
    CORE_RC=0
    verify_recorded_core || CORE_RC=$?
    case "$CORE_RC" in
      0) echo "${CORE_CONTEXT}: success (recorded for this head sha; ${CORE_OBSERVED})" ;;
      *)
        echo "::error::no successful ${CORE_CONTEXT} is recorded for this head sha (${CORE_OBSERVED:-lookup failed});" \
             "a '${EVENT_ACTION_IN}' event may reuse that result but never substitute for it." >&2
        core_summary "**required: FAILED — no successful \`${CORE_CONTEXT}\` recorded for this head sha; a label event cannot stand in for it.**"
        exit 1
        ;;
    esac
    ;;
  * )
    echo "::error::${CORE_CONTEXT} concluded '${CORE_RESULT_IN}'; required fails regardless of tier state." >&2
    core_summary "**required: FAILED — \`${CORE_CONTEXT}\` concluded \`${CORE_RESULT_IN}\`.**"
    exit 1
    ;;
esac

OBSERVATIONS="$WORK_DIR/observations.tsv"
ATTEMPT=0
FETCH_FAILURES=0
INTERVAL="$POLL_INITIAL_SECONDS"
VERDICT=""
LABELS_NOW="$PR_LABELS_IN"
WAIVER_EVENTS_FILE="$WORK_DIR/waiver-events.tsv"
: >"$WAIVER_EVENTS_FILE"
# WHY THE FEED WAS EMPTY (issue #3033). An empty label-event feed used to mean two
# unrelated things — "this pull request has nothing to waive" and "the API call for
# the `labeled` events FAILED" — and nothing downstream could tell them apart. The
# second silently disables the EVIDENCE half of `ci:waive:<tier-id>`
# (gating_registry.rb's waiver_bound_to_head? and waiver_supersedes_pending? can
# never be true without it), so the break-glass regresses to deadline-only with no
# trace. These carry the state forward to the reporting section, which names it.
#
# TWO COUNTS, NOT ONE (issue #3033 round 2). The default `--jq` selects
# `event == "labeled"` for EVERY label, while gating_registry.rb keys only on
# `ci:waive:<tier-id>` (WAIVER_LABEL_PATTERN). Reporting the feed total alone would
# claim "evidence read" off a feed of `needs-decision` events — the same
# unproven-claim defect this issue exists to remove, one layer up. So the
# waiver-relevant subset is counted separately and the report distinguishes "feed
# fine, no waiver events" from "feed fine, waiver events present".
WAIVER_EVIDENCE_STATE=none      # none | unconfigured | unreadable | read
WAIVER_EVIDENCE_DETAIL=""       # the LAST failure's condensed error; never cleared,
                                # so a `read`/`none` poll after a failed one can
                                # still say what went wrong earlier
WAIVER_EVIDENCE_COUNT=0         # `labeled` events in the feed, last successful read
WAIVER_EVIDENCE_HISTORY_COUNT=0 # of those, ones for ANY `ci:waive:` label, ever
WAIVER_EVIDENCE_INFORCE_COUNT=0 # of those, ones for a waiver label IN FORCE now
WAIVER_EVIDENCE_FAILURES=0      # polls whose read failed, however it ended up
WAIVER_UNCONFIGURED_WARNED=false # the unconfigured warning is emitted once per run

# WHETHER `LABELS_NOW` WAS ACTUALLY OBSERVED (issue #3033 round 4). The label
# read has always had a fallback — a failed live read reverts to the run-start
# event payload — but it left no durable trace, only a per-poll `::warning::`. The
# reporting section then stated absence ("no ci:waive: label is present") off a
# snapshot that predates the entire polling window, i.e. exactly the scenario the
# window exists for: a waiver applied WHILE this job waits. A confident false
# negative about the very thing this issue is about. These three carry the
# trustworthiness forward so the summary can report what was OBSERVED instead.
LABELS_READ_STATE=payload       # live | payload | fallback (see read_labels)
LABELS_READ_FAILURES=0          # polls whose live label read failed
LABELS_READ_DETAIL=""           # the LAST such failure, condensed; never cleared
# The `ci:waive:<tier-id>` labels in force per the labels this run is using,
# normalised EXACTLY as gating_registry.rb normalises them (comma split, strip,
# then the tier-id shape) so the evidence report cannot disagree with the verdict.
INFORCE_WAIVER_LABELS=""

# WHAT ACTUALLY WENT WRONG, condensed for a workflow command. The old code threw
# this away: EVERY failure mode — an authorization refusal, a secondary rate limit,
# a 5xx, an absent `gh`, a `--jq` syntax error — collapsed into one identical
# warning plus an empty file, which is also what an ordinary PR with nothing to
# waive produces. The HTTP status (when the client reported one) and the command's
# EXIT STATUS (when it did not) are the two facts that separate those cases, so
# both are surfaced. `gh` prints the status as "(HTTP 403)".
#
# The result lands inside a `::warning::` and in the job summary, so it is
# sanitised first: control characters go (it must stay one line), workflow-command
# syntax is defanged, and the text is truncated.
# condense_error <errfile> <exit-status>
condense_error() {
  local err="$1" rc="${2:-?}" status="" text=""
  if [ ! -s "$err" ]; then
    printf 'exit status %s, no error output' "$rc"
    return 0
  fi
  status="$(grep -o 'HTTP [0-9][0-9][0-9]' "$err" | head -n 1 || true)"
  # `|| true` is load-bearing on BOTH substitutions, not decoration: this runs under
  # `set -euo pipefail`, so a non-zero pipeline here would abort the whole aggregator
  # with no summary and a non-zero exit -- turning UNREADABLE evidence (which must
  # never change the verdict) into a RED `required`. That is reachable: BSD `tr`/`sed`
  # exit 1 with "Illegal byte sequence" on non-UTF-8 stderr under a UTF-8 locale, and
  # macOS is a first-class host for these scripts. A partial/empty detail string is
  # always preferable to killing the run.
  text="$(tr -d '[:cntrl:]' <"$err" | sed 's/::/__/g' | cut -c1-200 || true)"
  [ -n "$text" ] || text='(unprintable error output)'
  if [ -n "$status" ]; then
    printf '%s — %s' "$status" "$text"
  else
    printf 'exit status %s, no HTTP status reported — %s' "$rc" "$text"
  fi
}

# The waiver labels in force per `LABELS_NOW`, mirroring gating_registry.rb's
# `waived_tier_ids` normalisation (split on comma, strip, then
# /\Aci:waive:[a-z0-9][a-z0-9-]*\z/) so the evidence report counts the same labels
# the verdict does. Pure shell for the same reason as the counter below: an external
# tool here could fail and leave an EMPTY in-force set, which would silently become
# a reported zero — a claim of absence nobody observed. Shell parameter expansion
# has no such failure mode.
compute_inforce_waiver_labels() {
  local rest="$LABELS_NOW" item="" tier=""
  INFORCE_WAIVER_LABELS=""
  while [ -n "$rest" ]; do
    item="${rest%%,*}"
    if [ "$item" = "$rest" ]; then rest=""; else rest="${rest#*,}"; fi
    item="${item#"${item%%[![:space:]]*}"}"   # lstrip, as ruby's String#strip does
    item="${item%"${item##*[![:space:]]}"}"   # rstrip
    case "$item" in
      ci:waive:*) tier="${item#ci:waive:}" ;;
      *) continue ;;
    esac
    # /[a-z0-9][a-z0-9-]*\z/ — the tier-id shape, so anything that reaches the
    # comparison below is already allowlisted.
    case "$tier" in
      ''|[!a-z0-9]*|*[!a-z0-9-]*) continue ;;
    esac
    INFORCE_WAIVER_LABELS="${INFORCE_WAIVER_LABELS}${item},"
  done
  return 0
}

# How many `labeled` events in the feed are for a waiver label IN FORCE.
#
# PURE SHELL ON PURPOSE. An external counter (awk) would add a failure mode of its
# own — and a count that failed to compute may not be reported as zero, so it would
# need a third "uncomputable" state that no hermetic fixture can reach, i.e.
# untestable code guarding an untested claim. With no external command there is no
# such state: the only UNKNOWN left is the one that matters (a label set this run
# did not observe), and it is covered by a test. Nothing here can fail under
# `set -euo pipefail` either: `read` returning non-zero at EOF is the loop's
# condition, and the input file is written by the caller immediately above.
#
# IFS covers space/tab/CR so the label field is normalised the way
# gating_registry.rb strips it; no shape that can match a waiver label contains any
# of those, and an unmatched line only ever WITHHOLDS a claim.
count_inforce_waiver_events() {
  local count=0 label=""
  while IFS=$' \t\r' read -r label _ || [ -n "$label" ]; do
    # A blank feed line must not match the empty slot a trailing comma leaves.
    [ -n "$label" ] || continue
    case ",${INFORCE_WAIVER_LABELS}," in
      *",${label},"*) count=$((count + 1)) ;;
    esac
  done <"$WAIVER_EVENTS_FILE"
  printf '%s' "$count"
}

# The PR's CURRENT labels, re-read every poll so a waiver applied while this job
# waits takes effect without a re-run. A failed read falls back to the event
# payload: withholding a waiver is the safe direction, granting one is not.
#
# THE FALLBACK IS RECORDED, NOT JUST WARNED ABOUT (issue #3033 round 4). Three
# states, kept apart, because only the first one licenses a claim about what is on
# the pull request NOW:
#   live     — the live read succeeded this poll; `LABELS_NOW` is an observation.
#   fallback — a live read was configured and FAILED; `LABELS_NOW` is the
#              run-start payload snapshot, so a label applied or removed since the
#              run started is invisible. Absence must not be asserted from it.
#   payload  — no live source was configured at all; the payload is all there is,
#              and current labels were never looked for.
# The failure count and the last error persist for the whole run (like
# WAIVER_EVIDENCE_FAILURES) so a poll that later succeeds cannot erase the record.
read_labels() {
  if [ -z "$LABELS_CMD" ]; then
    LABELS_NOW="$PR_LABELS_IN"
    LABELS_READ_STATE=payload
    compute_inforce_waiver_labels
    return 0
  fi

  local rc=0 names="" trc=0
  eval "$LABELS_CMD" >"$WORK_DIR/labels.txt" 2>"$WORK_DIR/labels.err" || rc=$?
  if [ "$rc" -eq 0 ]; then
    # A normalisation failure (BSD `tr` exits 1 on non-UTF-8 input under a UTF-8
    # locale) is an UNTRUSTED read, not a live one: a truncated label list would
    # otherwise be reported as observed fact.
    names="$(tr '\n' ',' <"$WORK_DIR/labels.txt")" || trc=$?
    if [ "$trc" -eq 0 ]; then
      LABELS_NOW="$names"
      LABELS_READ_STATE=live
      compute_inforce_waiver_labels
      return 0
    fi
    printf 'the label list could not be normalised (tr exit %s)\n' "$trc" >"$WORK_DIR/labels.err"
    rc="$trc"
  fi
  sed 's/^/  /' "$WORK_DIR/labels.err" >&2 || true
  LABELS_READ_STATE=fallback
  LABELS_READ_FAILURES=$((LABELS_READ_FAILURES + 1))
  LABELS_READ_DETAIL="$(condense_error "$WORK_DIR/labels.err" "$rc")"
  LABELS_NOW="$PR_LABELS_IN"
  compute_inforce_waiver_labels
  echo "::warning::could not read the PR's current labels (${LABELS_READ_DETAIL}); falling back to the" \
       "run-start event payload labels, so a ci:waive:<tier-id> label applied since this run started is" \
       "INVISIBLE to it — this run cannot observe whether one is present. Issue #3033. The verdict still" \
       "comes from the tier evaluation alone" >&2
  return 0
}

# The `labeled` events behind any ACTIVE waiver label: who applied it and when.
# Two uses, both fail-safe — the attribution in the diagnostic, and the horizon
# that lets a waiver resolve a tier whose only check run the waiver's own label
# event minted. An empty file means "unresolved": the waiver still applies at the
# deadline exactly as before, and no name is claimed. This can never GRANT a
# waiver the labels did not already carry.
#
# THREE STATES, NEVER CONFLATED (issue #3033). "No waiver label present",
# "unreadable feed" and "read N events" all used to leave the same empty file and
# the same silence, so a genuinely BROKEN read was indistinguishable from an
# ordinary pull request with nothing to waive — and the failure it hides is total:
# without the events feed, waiver_bound_to_head? and waiver_supersedes_pending? in
# gating_registry.rb can never be true, so the break-glass silently degrades to
# deadline-only and nobody is named on its audit trail. Each state is now recorded
# and reported. NONE of them fails the job: an unreadable feed must still fall
# through to the ordinary polling path and be honoured at the deadline, because
# reding a break-glass PR for an API blip is the worse outage.
read_waiver_events() {
  : >"$WAIVER_EVENTS_FILE"
  case "$LABELS_NOW" in
    *ci:waive:*) ;;
    # No waiver label on THIS poll. `WAIVER_EVIDENCE_FAILURES` is deliberately NOT
    # reset: a label applied, read broken, then removed mid-run must not erase the
    # record that the reads were broken (the reporting section says so).
    *) WAIVER_EVIDENCE_STATE=none; return 0 ;;
  esac
  if [ -z "$WAIVER_EVENTS_CMD" ]; then
    WAIVER_EVIDENCE_STATE=unconfigured
    # ONCE PER RUN. Unlike an unreadable read, this condition cannot change while
    # the job runs — WAIVER_EVENTS_CMD is fixed at argument-parse time — so a
    # per-poll annotation would be a dozen identical lines saying the same thing.
    # The summary block carries the persistent record.
    if [ "$WAIVER_UNCONFIGURED_WARNED" != "true" ]; then
      WAIVER_UNCONFIGURED_WARNED=true
      echo "::warning::waiver evidence UNAVAILABLE: a ci:waive: label is present, but this run has no label-event" \
           "source (no --waiver-events-cmd, and --repo/--pr-number were not both supplied), so the waiver cannot be" \
           "bound to this head sha or attributed to whoever applied it; it will still apply at the aggregation" \
           "deadline" >&2
    fi
    return 0
  fi

  local rc=0
  eval "$WAIVER_EVENTS_CMD" >"$WAIVER_EVENTS_FILE" 2>"$WORK_DIR/waiver-events.err" || rc=$?
  if [ "$rc" -eq 0 ]; then
    WAIVER_EVIDENCE_STATE=read
    WAIVER_EVIDENCE_COUNT="$(grep -c '' "$WAIVER_EVENTS_FILE" || true)"
    # The label is field 1 of `<label>\t<actor>\t<iso8601>`, so anchoring at the
    # line start counts LABEL matches and cannot be fooled by an actor's login.
    # This is the whole HISTORY of waiver labellings on this pull request.
    WAIVER_EVIDENCE_HISTORY_COUNT="$(grep -c '^ci:waive:' "$WAIVER_EVENTS_FILE" || true)"
    # HISTORY IS NOT STATE (issue #3033 round 4). `labeled` events are IMMUTABLE:
    # a `ci:waive:alpha` applied months ago and removed the same day is in this feed
    # forever. Counting those kept the "the evidence is present" claim alive while
    # the label actually in force had no event of its own — bindability asserted for
    # a label whose evidence is missing, which is this issue's defect one layer up.
    # Only events for a label IN FORCE can bind or attribute anything, so the feed
    # is intersected with `INFORCE_WAIVER_LABELS`. When the label read was untrusted
    # that set is itself unobserved, and the reporting section says UNKNOWN rather
    # than reporting this number.
    WAIVER_EVIDENCE_INFORCE_COUNT="$(count_inforce_waiver_events)"
    return 0
  fi
  sed 's/^/  /' "$WORK_DIR/waiver-events.err" >&2 || true
  WAIVER_EVIDENCE_STATE=unreadable
  WAIVER_EVIDENCE_FAILURES=$((WAIVER_EVIDENCE_FAILURES + 1))
  WAIVER_EVIDENCE_DETAIL="$(condense_error "$WORK_DIR/waiver-events.err" "$rc")"
  # Say WHY the feed was empty, and how to tell the causes apart. The classes have
  # different remedies: 401/403/404 = the token running this workflow may not read
  # this pull request's events (the `permissions:` block); a 403 naming a rate limit
  # or any 5xx = transient, re-run; no HTTP status at all = the client itself failed
  # (`gh` absent, a bad --jq), which no re-run fixes.
  echo "::warning::waiver evidence UNREADABLE: the read of this pull request's label events FAILED" \
       "(${WAIVER_EVIDENCE_DETAIL}) — this is a BROKEN READ, NOT an absence of waiver labels, because the label" \
       "set this poll is using carries a ci:waive: label. 401/403/404 means the token running this workflow may not read this pull" \
       "request's events (check the workflow's permissions block); a 403 naming a rate limit, or a 5xx, is" \
       "transient; no HTTP status at all means the client failed (gh unavailable, or a bad --jq). Issue #3033." \
       "The waiver still applies at the aggregation deadline, but it cannot resolve a tier early and it will" \
       "not be attributed to whoever applied it" >&2
  : >"$WAIVER_EVENTS_FILE"
}

evaluate_once() {
  # $1 = "final" to spend the deadline (unresolved tiers become failures).
  local final_flag=""
  [ "${1:-}" = "final" ] && final_flag="--final"

  if ! eval "$CHECK_RUNS_CMD" >"$WORK_DIR/check-runs.json" 2>"$WORK_DIR/check-runs.err"; then
    sed 's/^/  /' "$WORK_DIR/check-runs.err" >&2 || true
    return 4
  fi
  read_labels
  read_waiver_events

  # `|| rc=$?` keeps this an OR-list, so `set -e` never kills the shell on a
  # deliberate non-zero verdict code (0 pass / 1 fail / 3 keep waiting).
  local rc=0
  # shellcheck disable=SC2086 # final_flag is a single deliberate literal flag.
  ruby "$REGISTRY_RB" evaluate \
    --registry "$REGISTRY" \
    --check-runs "$WORK_DIR/check-runs.json" \
    --exclude-ids-file "$SELF_IDS_FILE" \
    --run-id "${RUN_ID:-}" \
    --labels "$LABELS_NOW" \
    --waiver-events "$WAIVER_EVENTS_FILE" \
    --now "${NOW_OVERRIDE:-$(date +%s)}" \
    ${EVENT_ARGS[@]+"${EVENT_ARGS[@]}"} \
    $final_flag >"$OBSERVATIONS" 2>"$WORK_DIR/evaluate.err" || rc=$?
  if [ -s "$WORK_DIR/evaluate.err" ]; then
    sed 's/^/  /' "$WORK_DIR/evaluate.err" >&2
  fi
  return "$rc"
}

while :; do
  ATTEMPT=$((ATTEMPT + 1))
  RC=0
  evaluate_once || RC=$?

  case "$RC" in
    0) VERDICT=pass; break ;;
    1) VERDICT=fail; break ;;
    3) FETCH_FAILURES=0 ;; # unresolved tiers remain — keep waiting
    4)
      # A TRANSPORT failure, not a well-formed negative answer: one 5xx,
      # secondary-rate-limit or DNS blip must not red a PR that has 59 minutes of
      # budget left. Retried under the same backoff, fatal only once it persists.
      FETCH_FAILURES=$((FETCH_FAILURES + 1))
      if [ "$FETCH_FAILURES" -ge "$MAX_FETCH_FAILURES" ]; then
        fail_closed "could not read check runs ${FETCH_FAILURES} times in a row; command: $CHECK_RUNS_CMD"
      fi
      echo "::warning::transient check-run fetch failure ${FETCH_FAILURES}/${MAX_FETCH_FAILURES}; retrying" >&2
      ;;
    *) fail_closed "registry evaluation failed (exit $RC)" ;;
  esac

  NOW="$(date +%s)"
  BUDGET_SPENT=0
  if [ -n "$POLL_ATTEMPTS" ] && [ "$ATTEMPT" -ge "$POLL_ATTEMPTS" ]; then
    BUDGET_SPENT=1
  fi
  if [ "$NOW" -ge "$DEADLINE_EPOCH" ]; then
    BUDGET_SPENT=1
  fi

  if [ "$BUDGET_SPENT" -eq 1 ]; then
    # Deadline (or injected poll budget) spent. Re-evaluate in FINAL mode: every
    # unresolved tier becomes a failure unless a per-tier waiver excuses it.
    # Expiry is NEVER a pass. A fetch failure gets a couple of retries even here
    # (a blip on the LAST fetch would otherwise decide the verdict), but once
    # they are spent this fails CLOSED — that is what the deadline is for.
    FINAL_ATTEMPT=0
    while :; do
      FINAL_ATTEMPT=$((FINAL_ATTEMPT + 1))
      RC=0
      evaluate_once final || RC=$?
      case "$RC" in
        0) VERDICT=pass; break ;;
        1) VERDICT=fail; break ;;
        4)
          if [ "$FINAL_ATTEMPT" -ge 3 ]; then
            fail_closed "could not read check runs at the deadline (${FINAL_ATTEMPT} attempts); command: $CHECK_RUNS_CMD"
          fi
          echo "::warning::transient check-run fetch failure at the deadline (attempt ${FINAL_ATTEMPT}); retrying" >&2
          "$SLEEP_CMD" "$POLL_INITIAL_SECONDS" || true
          ;;
        *) fail_closed "final registry evaluation failed (exit $RC)" ;;
      esac
    done
    break
  fi

  echo "aggregate-required-tiers: unresolved tiers remain; re-polling in ${INTERVAL}s (attempt ${ATTEMPT})"
  "$SLEEP_CMD" "$INTERVAL" || true
  INTERVAL=$((INTERVAL * 2))
  [ "$INTERVAL" -gt "$POLL_MAX_SECONDS" ] && INTERVAL="$POLL_MAX_SECONDS"
done

# ---------------------------------------------------------------- reporting --
# The evidence behind a green `required` must be inspectable after the fact, so
# every registered context is listed with the check run that decided it.

emit() {
  echo "$1"
  [ -n "$SUMMARY_FILE" ] && printf '%s\n' "$1" >>"$SUMMARY_FILE"
  return 0
}

emit "## Required gating tiers (issue #2910)"
emit ""
emit "| tier | context | state | check run | status | conclusion | run |"
emit "| --- | --- | --- | --- | --- | --- | --- |"

FAILED_TIERS=""
WAIVED_TIERS=""
# UNIT SEPARATOR, not tab: bash `read` collapses runs of IFS *whitespace*, which
# would silently shift fields left for an absent tier (empty id/conclusion/url).
while IFS=$'\x1f' read -r state tier context check_id status conclusion url note; do
  [ -z "${state:-}" ] && continue
  emit "| \`${tier}\` | \`${context}\` | ${state} | ${check_id:-—} | ${status:-—} | ${conclusion:-—} | ${url:-—} |"
  case "$state" in
    fail)
      FAILED_TIERS="${FAILED_TIERS}${tier} "
      echo "::error::required gating tier '${tier}' (context '${context}') did not pass: ${note:-state=$state} ${url}" >&2
      ;;
    waived)
      WAIVED_TIERS="${WAIVED_TIERS}${tier} "
      # The note carries the RESOLVED labeller (or an explicit "UNRESOLVED"); this
      # line claims no actor of its own — see the attribution note above.
      echo "::warning::required gating tier '${tier}' WAIVED by ci:waive:${tier}: ${note}" >&2
      emit ""
      emit "> :warning: tier \`${tier}\` was waived by label \`ci:waive:${tier}\` — ${note}"
      ;;
  esac
done <"$OBSERVATIONS"

# ----------------------------------------- waiver evidence, always on record --
# The state is stated for EVERY run, including the ordinary "nothing to waive"
# one, so an abnormal state READS as abnormal instead of having to be inferred from
# a silence the healthy case produces too (issue #3033). One line in the ordinary
# case; the diagnosis only when there is something to diagnose. This block never
# changes the verdict.
#
# OBSERVATIONS, NOT CONCLUSIONS (issue #3033 round 4). This block states what was
# READ, what was FOUND and what is UNKNOWN. It asserts no downstream capability —
# not "a waiver can be bound", not "the evidence is present", not "no label is
# present" — unless every input to that assertion was observed ON THIS RUN. Where
# an input is unobserved or stale, the line itself says so. Three recurrences of
# the same defect (a claim outrunning its evidence) came from doing otherwise.

# WHERE THE LABELS BEHIND ALL OF THE ABOVE CAME FROM. `LABELS_NOW` is an
# observation only in the `live` state; the other two are the run-start payload,
# under which a label applied or removed mid-run is invisible.
emit_label_read_note() {
  case "$LABELS_READ_STATE" in
    fallback)
      emit ""
      emit "> :warning: Label read UNTRUSTED: the live read of this pull request's labels FAILED, so the labels"
      emit "> behind everything above are the RUN-START EVENT PAYLOAD (${LABELS_READ_FAILURES} failed read(s); ${LABELS_READ_DETAIL})."
      emit "> \`401\`/\`403\`/\`404\`: check the workflow's \`permissions:\` block; a \`403\` naming a rate limit or any"
      emit "> \`5xx\`: transient; no HTTP status: the client itself failed. Degraded, not fatal — the verdict came"
      emit "> from the tier evaluation either way."
      ;;
    payload)
      emit ""
      emit "> :information_source: Label read: this run had no live label source, so the labels behind everything above are the run-start event payload; the pull request's current labels were never read."
      ;;
    *)
      if [ "${LABELS_READ_FAILURES:-0}" -gt 0 ]; then
        emit ""
        emit "> :warning: ${LABELS_READ_FAILURES} earlier poll(s) could not read this pull request's labels (${LABELS_READ_DETAIL}); the last read succeeded, so the labels behind everything above are live."
      fi
      ;;
  esac
  return 0
}

# Was the in-force intersection taken against a label set this run OBSERVED? An
# untrusted label read is the one way it was not, and then the intersection is
# itself unknown — reporting it as zero would be a claim of absence again, one
# variable further down. (The count has no failure mode of its own; see
# count_inforce_waiver_events.)
INFORCE_UNKNOWN_REASON=""
if [ "$LABELS_READ_STATE" = "fallback" ]; then
  INFORCE_UNKNOWN_REASON="the live label read FAILED (${LABELS_READ_FAILURES} failed read(s); ${LABELS_READ_DETAIL}) and the labels fell back to the run-start event payload"
fi

emit ""
case "$WAIVER_EVIDENCE_STATE" in
  none)
    case "$LABELS_READ_STATE" in
      live)
        # Genuinely observed: the live read succeeded on this poll, so absence is
        # an observation and is stated as one.
        emit "Waiver evidence: n/a — no \`ci:waive:<tier-id>\` label is present on this pull request."
        ;;
      fallback)
        # THE FALSE NEGATIVE THIS REPLACES (issue #3033 round 4). This line used to
        # state absence flatly here — while the live read had failed and the labels
        # were a run-start snapshot, i.e. precisely when a waiver applied mid-run
        # (the reason the polling window exists) is invisible. Report the
        # observation and the gap in it instead.
        emit "Waiver evidence: **UNKNOWN (label read UNTRUSTED)** — no \`ci:waive:<tier-id>\` label was OBSERVED, but the live label read FAILED (${LABELS_READ_FAILURES} failed read(s); ${LABELS_READ_DETAIL}) and this run fell back to the run-start event payload, so a waiver label applied since the run started would be invisible here. Absence is NOT claimed."
        ;;
      *)
        emit "Waiver evidence: n/a — no \`ci:waive:<tier-id>\` label is in the run-start event payload, which is the only label set this run had (no live label source configured), so a label applied since the run started was never looked for."
        ;;
    esac
    # A label applied, its events unreadable, then the label removed mid-run: the
    # state is legitimately "nothing to waive" now, but the broken read is history
    # worth keeping, not history to discard.
    if [ "$WAIVER_EVIDENCE_FAILURES" -gt 0 ]; then
      emit ""
      emit "> :warning: no waiver label now, but ${WAIVER_EVIDENCE_FAILURES} earlier read(s) of this pull request's \`labeled\` events FAILED (${WAIVER_EVIDENCE_DETAIL}) while one was present."
    fi
    ;;
  read)
    # WHAT WAS OBSERVED, NOT WHAT IT IMPLIES (issue #3033 rounds 2 and 4). A feed of
    # `labeled` events for other labels proves the read works and nothing else; an
    # event for a waiver label since REMOVED proves even less, because `labeled`
    # events are immutable history; and even a matching, in-force event only BINDS if
    # its timestamp is at or after this head sha's first CI activity, which is
    # decided per tier in the rows above and is NOT observed here.
    if [ -n "$INFORCE_UNKNOWN_REASON" ]; then
      emit "Waiver evidence: READ OK, in-force match **UNKNOWN** — the \`labeled\` events read (${WAIVER_EVIDENCE_COUNT} event(s), ${WAIVER_EVIDENCE_HISTORY_COUNT} of them for a \`ci:waive:\` label), but ${INFORCE_UNKNOWN_REASON}, so this run did not observe which waiver labels are in force and cannot say whether any of those events is for one."
      emit ""
      emit "> :warning: Neither presence nor absence of usable evidence is claimed here. Degraded, not fatal:"
      emit "> the waiver is still honoured at the aggregation deadline exactly as before."
    elif [ "$WAIVER_EVIDENCE_INFORCE_COUNT" -gt 0 ]; then
      emit "Waiver evidence: READ OK — feed read (${WAIVER_EVIDENCE_COUNT} \`labeled\` event(s)), ${WAIVER_EVIDENCE_INFORCE_COUNT} of them for a \`ci:waive:\` label in force, so the evidence needed for attribution and head-binding was OBSERVED for that label. Whether a waiver actually resolves a tier — its event must be at or after this head sha's first CI activity, which this line does not observe — is decided per tier above."
    else
      emit "Waiver evidence: READ OK — feed read (${WAIVER_EVIDENCE_COUNT} \`labeled\` event(s)), ${WAIVER_EVIDENCE_HISTORY_COUNT} of them for a \`ci:waive:\` label, but **0 for a waiver label in force now**, so attribution and head-binding remain UNRESOLVED."
      emit ""
      emit "> :warning: The read succeeded, so this is neither a permission nor an API problem. \`labeled\` events are"
      emit "> IMMUTABLE HISTORY: an event for a waiver label since REMOVED stays in this feed forever and can bind"
      emit "> nothing, so only events for a label IN FORCE are counted. So either the feed carried no \`labeled\`"
      emit "> event for the waiver label now applied, or every waiver event in it names a label no longer applied."
      emit "> A waiver still applies at the aggregation deadline, but it cannot resolve a tier early and no applier"
      emit "> is named."
    fi
    if [ "$WAIVER_EVIDENCE_FAILURES" -gt 0 ]; then
      emit ""
      emit "> :warning: ${WAIVER_EVIDENCE_FAILURES} earlier poll(s) could not read the label events (${WAIVER_EVIDENCE_DETAIL}); the last read succeeded."
    fi
    ;;
  unreadable)
    emit "Waiver evidence: **UNREADABLE (broken read — authorization, API or client failure)** — the \`labeled\` events for this pull request could not be read (${WAIVER_EVIDENCE_DETAIL}); ${WAIVER_EVIDENCE_FAILURES} failed read(s)."
    emit ""
    emit "> :warning: This is NOT \"no waiver labels present\" — the label set this run is using carries a \`ci:waive:<tier-id>\` label."
    emit "> \`401\`/\`403\`/\`404\`: the token running this workflow may not read this pull request's events —"
    emit "> check the workflow's \`permissions:\` block. A \`403\` naming a rate limit, or any \`5xx\`: transient,"
    emit "> re-run. No HTTP status at all: the client itself failed (\`gh\` unavailable, a bad \`--jq\`), which a"
    emit "> re-run will not fix. Issue #3033."
    emit "> Degraded, not fatal: the waiver is still honoured at the aggregation deadline, but it cannot"
    emit "> resolve a tier early and is reported as \`applier UNRESOLVED\`."
    ;;
  unconfigured)
    emit "Waiver evidence: **UNAVAILABLE (no label-event source configured)** — the label set this run is using carries a \`ci:waive:<tier-id>\` label but this run was given no way to read the \`labeled\` events."
    emit ""
    emit "> :warning: The waiver is still honoured at the aggregation deadline, but it cannot resolve a tier"
    emit "> early and is reported as \`applier UNRESOLVED\`."
    ;;
esac
emit_label_read_note

emit ""
if [ "$VERDICT" = "pass" ]; then
  emit "**required: every registered gating tier reported success.**"
  [ -n "$WAIVED_TIERS" ] && emit "Waived tiers: \`${WAIVED_TIERS% }\`."
  echo "aggregate-required-tiers: PASS"
  exit 0
fi

emit "**required: FAILED — gating tiers not satisfied: \`${FAILED_TIERS% }\`.**"
emit ""
emit "An absent tier is an ERROR, not inapplicability: a registered tier always emits its"
emit "context, reporting inapplicability as an explicit success. Re-run the tier, then re-run"
emit "\`required\`. See \`.github/ci-gating-tiers.yml\`."
echo "aggregate-required-tiers: FAIL — ${FAILED_TIERS% }" >&2
exit 1
