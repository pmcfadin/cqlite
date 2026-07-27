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

# The PR's CURRENT labels, re-read every poll so a waiver applied while this job
# waits takes effect without a re-run. A failed read falls back to the event
# payload: withholding a waiver is the safe direction, granting one is not.
read_labels() {
  [ -n "$LABELS_CMD" ] || { LABELS_NOW="$PR_LABELS_IN"; return 0; }

  if eval "$LABELS_CMD" >"$WORK_DIR/labels.txt" 2>"$WORK_DIR/labels.err"; then
    LABELS_NOW="$(tr '\n' ',' <"$WORK_DIR/labels.txt")"
    return 0
  fi
  sed 's/^/  /' "$WORK_DIR/labels.err" >&2 || true
  echo "::warning::could not read the PR's current labels; falling back to the event payload labels" >&2
  LABELS_NOW="$PR_LABELS_IN"
}

# The `labeled` events behind any ACTIVE waiver label: who applied it and when.
# Two uses, both fail-safe — the attribution in the diagnostic, and the horizon
# that lets a waiver resolve a tier whose only check run the waiver's own label
# event minted. An empty file means "unresolved": the waiver still applies at the
# deadline exactly as before, and no name is claimed. This can never GRANT a
# waiver the labels did not already carry.
read_waiver_events() {
  : >"$WAIVER_EVENTS_FILE"
  case "$LABELS_NOW" in
    *ci:waive:*) ;;
    *) return 0 ;;
  esac
  [ -n "$WAIVER_EVENTS_CMD" ] || return 0

  if eval "$WAIVER_EVENTS_CMD" >"$WAIVER_EVENTS_FILE" 2>"$WORK_DIR/waiver-events.err"; then
    return 0
  fi
  sed 's/^/  /' "$WORK_DIR/waiver-events.err" >&2 || true
  echo "::warning::could not read this pull request's label events; a waiver will still apply at the" \
       "aggregation deadline, but it will not be attributed to whoever applied it" >&2
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
