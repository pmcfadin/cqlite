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
#   --run-id ID                this workflow run id     [$GITHUB_RUN_ID]
#   --labels CSV               PR labels (waivers)      [$PR_LABELS]
#   --actor NAME               waiver actor for the log [$GITHUB_ACTOR]
#   --deadline-minutes N       aggregation deadline     [registry effective max]
#   --deadline-epoch N         absolute deadline (unix seconds); wins over the above
#   --poll-attempts N          hard cap on fetches      [unbounded until deadline]
#   --poll-initial-seconds N   first backoff interval   [15]
#   --poll-max-seconds N       backoff ceiling          [60]
#   --check-runs-cmd CMD       command printing check-run JSON/NDJSON [gh api]
#   --self-jobs-cmd CMD        command printing this run's job ids    [gh api]
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
RUN_ID="${GITHUB_RUN_ID:-}"
PR_LABELS_IN="${PR_LABELS:-}"
ACTOR="${GITHUB_ACTOR:-unknown}"
DEADLINE_MINUTES=""
DEADLINE_EPOCH=""
POLL_ATTEMPTS=""
POLL_INITIAL_SECONDS="${POLL_INITIAL_SECONDS:-15}"
POLL_MAX_SECONDS="${POLL_MAX_SECONDS:-60}"
CHECK_RUNS_CMD="${CHECK_RUNS_CMD:-}"
SELF_JOBS_CMD="${SELF_JOBS_CMD:-}"
SUMMARY_FILE="${GITHUB_STEP_SUMMARY:-}"
SLEEP_CMD="${SLEEP_CMD:-sleep}"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --registry) REGISTRY="$2"; shift 2 ;;
    --repo) REPO_SLUG="$2"; shift 2 ;;
    --head-sha) HEAD_SHA="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    --labels) PR_LABELS_IN="$2"; shift 2 ;;
    --actor) ACTOR="$2"; shift 2 ;;
    --deadline-minutes) DEADLINE_MINUTES="$2"; shift 2 ;;
    --deadline-epoch) DEADLINE_EPOCH="$2"; shift 2 ;;
    --poll-attempts) POLL_ATTEMPTS="$2"; shift 2 ;;
    --poll-initial-seconds) POLL_INITIAL_SECONDS="$2"; shift 2 ;;
    --poll-max-seconds) POLL_MAX_SECONDS="$2"; shift 2 ;;
    --check-runs-cmd) CHECK_RUNS_CMD="$2"; shift 2 ;;
    --self-jobs-cmd) SELF_JOBS_CMD="$2"; shift 2 ;;
    --summary-file) SUMMARY_FILE="$2"; shift 2 ;;
    --sleep-cmd) SLEEP_CMD="$2"; shift 2 ;;
    -h|--help) sed -n '2,60p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "aggregate-required-tiers: unknown argument '$1'" >&2; exit 2 ;;
  esac
done

fail_closed() {
  echo "::error::required-tier aggregation could not run: $*" >&2
  echo "aggregate-required-tiers: FAIL (harness) — $*" >&2
  exit 2
}

command -v ruby >/dev/null 2>&1 || fail_closed "ruby is unavailable (needed to read the gating-tier registry)"
[ -f "$REGISTRY" ] || fail_closed "registry not found at $REGISTRY"

REGISTRY_RB="$REPO_ROOT/scripts/ci/gating_registry.rb"
[ -f "$REGISTRY_RB" ] || fail_closed "scripts/ci/gating_registry.rb not found"

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

SELF_IDS_FILE="$WORK_DIR/self-job-ids.txt"
if ! eval "$SELF_JOBS_CMD" >"$SELF_IDS_FILE" 2>"$WORK_DIR/self-job-ids.err"; then
  # Fail CLOSED: without the job-id set the aggregator could wait on its own
  # check run forever. The details-URL fallback alone is not enough to justify
  # proceeding silently.
  sed 's/^/  /' "$WORK_DIR/self-job-ids.err" >&2 || true
  fail_closed "could not list this run's jobs (self-exclusion input); command: $SELF_JOBS_CMD"
fi

echo "aggregate-required-tiers: registry=$REGISTRY deadline_epoch=$DEADLINE_EPOCH run_id=${RUN_ID:-none}"

OBSERVATIONS="$WORK_DIR/observations.tsv"
ATTEMPT=0
INTERVAL="$POLL_INITIAL_SECONDS"
VERDICT=""

evaluate_once() {
  # $1 = "final" to spend the deadline (unresolved tiers become failures).
  local final_flag=""
  [ "${1:-}" = "final" ] && final_flag="--final"

  if ! eval "$CHECK_RUNS_CMD" >"$WORK_DIR/check-runs.json" 2>"$WORK_DIR/check-runs.err"; then
    sed 's/^/  /' "$WORK_DIR/check-runs.err" >&2 || true
    return 4
  fi

  # `|| rc=$?` keeps this an OR-list, so `set -e` never kills the shell on a
  # deliberate non-zero verdict code (0 pass / 1 fail / 3 keep waiting).
  local rc=0
  # shellcheck disable=SC2086 # final_flag is a single deliberate literal flag.
  ruby "$REGISTRY_RB" evaluate \
    --registry "$REGISTRY" \
    --check-runs "$WORK_DIR/check-runs.json" \
    --exclude-ids-file "$SELF_IDS_FILE" \
    --run-id "${RUN_ID:-}" \
    --labels "$PR_LABELS_IN" \
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
    3) : ;; # unresolved tiers remain — keep waiting
    4) fail_closed "could not read check runs; command: $CHECK_RUNS_CMD" ;;
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
    # Expiry is NEVER a pass.
    RC=0
    evaluate_once final || RC=$?
    case "$RC" in
      0) VERDICT=pass ;;
      1) VERDICT=fail ;;
      4) fail_closed "could not read check runs at the deadline; command: $CHECK_RUNS_CMD" ;;
      *) fail_closed "final registry evaluation failed (exit $RC)" ;;
    esac
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
      echo "::warning::required gating tier '${tier}' WAIVED by ci:waive:${tier} (actor: ${ACTOR}): ${note}" >&2
      emit ""
      emit "> :warning: tier \`${tier}\` was waived by label \`ci:waive:${tier}\` (actor: ${ACTOR}) — ${note}"
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
