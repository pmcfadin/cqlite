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
#   --actor NAME               waiver actor for the log [$GITHUB_ACTOR]
#   --deadline-minutes N       aggregation deadline     [registry effective max]
#   --deadline-epoch N         absolute deadline (unix seconds); wins over the above
#   --poll-attempts N          hard cap on fetches      [unbounded until deadline]
#   --poll-initial-seconds N   first backoff interval   [15]
#   --poll-max-seconds N       backoff ceiling          [60]
#   --max-fetch-failures N     consecutive transient fetch failures tolerated [6]
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
ACTOR="${GITHUB_ACTOR:-unknown}"
DEADLINE_MINUTES=""
DEADLINE_EPOCH=""
POLL_ATTEMPTS=""
POLL_INITIAL_SECONDS="${POLL_INITIAL_SECONDS:-15}"
POLL_MAX_SECONDS="${POLL_MAX_SECONDS:-60}"
MAX_FETCH_FAILURES="${MAX_FETCH_FAILURES:-6}"
CHECK_RUNS_CMD="${CHECK_RUNS_CMD:-}"
SELF_JOBS_CMD="${SELF_JOBS_CMD:-}"
LABELS_CMD="${LABELS_CMD:-}"
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
    --actor) ACTOR="$2"; shift 2 ;;
    --deadline-minutes) DEADLINE_MINUTES="$2"; shift 2 ;;
    --deadline-epoch) DEADLINE_EPOCH="$2"; shift 2 ;;
    --poll-attempts) POLL_ATTEMPTS="$2"; shift 2 ;;
    --poll-initial-seconds) POLL_INITIAL_SECONDS="$2"; shift 2 ;;
    --poll-max-seconds) POLL_MAX_SECONDS="$2"; shift 2 ;;
    --max-fetch-failures) MAX_FETCH_FAILURES="$2"; shift 2 ;;
    --check-runs-cmd) CHECK_RUNS_CMD="$2"; shift 2 ;;
    --self-jobs-cmd) SELF_JOBS_CMD="$2"; shift 2 ;;
    --labels-cmd) LABELS_CMD="$2"; shift 2 ;;
    --now) NOW_OVERRIDE="$2"; shift 2 ;;
    --summary-file) SUMMARY_FILE="$2"; shift 2 ;;
    --sleep-cmd) SLEEP_CMD="$2"; shift 2 ;;
    -h|--help) sed -n '2,70p' "${BASH_SOURCE[0]}"; exit 0 ;;
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

OBSERVATIONS="$WORK_DIR/observations.tsv"
ATTEMPT=0
FETCH_FAILURES=0
INTERVAL="$POLL_INITIAL_SECONDS"
VERDICT=""
LABELS_NOW="$PR_LABELS_IN"

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

evaluate_once() {
  # $1 = "final" to spend the deadline (unresolved tiers become failures).
  local final_flag=""
  [ "${1:-}" = "final" ] && final_flag="--final"

  if ! eval "$CHECK_RUNS_CMD" >"$WORK_DIR/check-runs.json" 2>"$WORK_DIR/check-runs.err"; then
    sed 's/^/  /' "$WORK_DIR/check-runs.err" >&2 || true
    return 4
  fi
  read_labels

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
    --now "${NOW_OVERRIDE:-$(date +%s)}" \
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
