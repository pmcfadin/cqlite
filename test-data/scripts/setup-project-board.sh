#!/usr/bin/env bash
set -euo pipefail

# setup-project-board.sh — one-time setup of the CQLite Delivery claim board.
#
# Creates (or reuses) a GitHub Project (v2), ensures its single-select `Status`
# field carries the canonical options (Backlog / Ready / In Progress / In Review /
# Done), links the project to the repo, and prints the manual web-UI step for the
# built-in workflow automations (which cannot be configured via the CLI).
#
# Prerequisites (the owner's one-time action):
#   gh auth refresh -s project        # Projects v2 needs the `project` token scope
#
# Usage:
#   PROJECT_OWNER=pmcfadin REPO=pmcfadin/cqlite ./test-data/scripts/setup-project-board.sh
#
# Idempotent-ish: detects an existing project by title and reuses it; missing
# Status options are added, existing ones left alone.

OWNER="${PROJECT_OWNER:-pmcfadin}"
REPO="${REPO:-pmcfadin/cqlite}"
TITLE="${PROJECT_TITLE:-CQLite Delivery}"
STATUS_OPTIONS=("Backlog" "Ready" "In Progress" "In Review" "Done")

if ! command -v gh >/dev/null 2>&1; then
  echo "error: gh CLI not found on PATH" >&2
  exit 1
fi
# jq is used to parse every gh JSON response below — check it UP FRONT so we never
# create/link a remote project and then die on a missing-jq parse, leaving partial
# remote state (roborev).
if ! command -v jq >/dev/null 2>&1; then
  echo "error: jq not found on PATH — required to parse gh responses. Install jq and re-run." >&2
  exit 1
fi

# The `project` scope is required for every Projects v2 read/write below.
if ! gh auth status 2>&1 | grep -q "'project'"; then
  echo "error: gh token is missing the 'project' scope." >&2
  echo "       Run: gh auth refresh -s project" >&2
  exit 1
fi

echo "==> Looking for an existing project titled '${TITLE}' (owner ${OWNER})"
project_number=""
# Fail loud on a read/API/auth error — DON'T swallow it to '{}', which would look
# like "no such project" and wrongly proceed to create a duplicate (roborev).
if ! existing_json="$(gh project list --owner "${OWNER}" --format json --limit 200 2>&1)"; then
  echo "error: 'gh project list' failed (check the 'project' scope / auth / network):" >&2
  printf '%s\n' "${existing_json}" >&2
  exit 1
fi
project_number="$(
  printf '%s' "${existing_json}" \
    | jq -r --arg t "${TITLE}" '(.projects // [])[] | select(.title == $t) | .number' \
    | head -n1
)"

if [ -n "${project_number}" ]; then
  echo "    Found existing project #${project_number} — reusing it."
else
  echo "==> Creating project '${TITLE}'"
  create_json="$(gh project create --owner "${OWNER}" --title "${TITLE}" --format json)"
  project_number="$(printf '%s' "${create_json}" | jq -r '.number')"
  echo "    Created project #${project_number}."
fi

if [ -z "${project_number}" ] || [ "${project_number}" = "null" ]; then
  echo "error: could not determine the project number" >&2
  exit 1
fi

# Project node id (needed for GraphQL field mutations).
project_id="$(gh project view "${project_number}" --owner "${OWNER}" --format json | jq -r '.id')"

echo "==> Linking project #${project_number} to repo ${REPO}"
# Tolerate ONLY the known "already linked" case; fail loud on any other error so
# we never report success on an unlinked project (roborev).
# `gh project link --repo` expects the BARE repo name for --owner (per its examples:
# `--owner monalisa --repo my_repo`), so strip any "owner/" prefix from REPO.
repo_name="${REPO##*/}"
if ! link_out="$(gh project link "${project_number}" --owner "${OWNER}" --repo "${repo_name}" 2>&1)"; then
  if printf '%s' "${link_out}" | grep -qiE "already linked|already exists"; then
    echo "    (already linked)"
  else
    echo "error: failed to link project #${project_number} to ${REPO}:" >&2
    printf '%s\n' "${link_out}" >&2
    exit 1
  fi
fi

echo "==> Ensuring the 'Status' single-select field carries the canonical options"
fields_json="$(gh project field-list "${project_number}" --owner "${OWNER}" --format json --limit 100)"
status_field_id="$(
  printf '%s' "${fields_json}" \
    | jq -r '(.fields // [])[] | select(.name == "Status") | .id' | head -n1
)"

# Collect the options the Status field already has (empty if the field is absent).
existing_options=""
if [ -n "${status_field_id}" ]; then
  existing_options="$(
    printf '%s' "${fields_json}" \
      | jq -r '(.fields // [])[] | select(.name == "Status") | (.options // [])[] | .name'
  )"
fi

# Determine which canonical options are missing.
missing_options=()
for opt in "${STATUS_OPTIONS[@]}"; do
  if ! printf '%s\n' "${existing_options}" | grep -qxF "${opt}"; then
    missing_options+=("${opt}")
  fi
done

if [ -z "${status_field_id}" ]; then
  echo "    No 'Status' field found — creating it with all canonical options."
  # gh accepts a comma-separated option list for single-select creation.
  options_csv="$(IFS=,; echo "${STATUS_OPTIONS[*]}")"
  gh project field-create "${project_number}" --owner "${OWNER}" \
    --name "Status" --data-type SINGLE_SELECT \
    --single-select-options "${options_csv}" >/dev/null
  echo "    Created 'Status' with: ${options_csv}"
elif [ "${#missing_options[@]}" -gt 0 ]; then
  # NON-DESTRUCTIVE: a `gh api graphql updateProjectV2Field` with
  # singleSelectOptions REPLACES the whole option list — resending only the
  # canonical set would DROP any existing (incl. custom) options and could
  # detach items already assigned to them. So we DO NOT rewrite the field;
  # we warn and ask the owner to add the missing options in the web UI
  # (adding options there is safe and non-destructive) — roborev.
  echo "    ERROR: 'Status' field exists but is MISSING canonical option(s): ${missing_options[*]}" >&2
  echo "    Not rewriting (a rewrite would drop existing/custom options and detach assigned items)." >&2
  echo "    Add the missing option(s) in the web UI (Project -> Status field -> + add option), then re-run." >&2
  echo "    SETUP INCOMPLETE — the board is NOT reported ready, so flow-* keeps using the label fallback." >&2
  # Fail nonzero: a partially-configured Status field would let flow-* detect the board
  # as usable and then fail on writes to the missing options instead of falling back (roborev).
  exit 1
else
  echo "    'Status' already carries all canonical options — nothing to do."
fi

echo "==> Project board ready: #${project_number} ('${TITLE}'), id ${project_id}"
echo
cat <<EOF
========================================================================
MANUAL STEP REQUIRED (cannot be set via the gh CLI)
========================================================================
Open the project in the web UI and enable the built-in workflow
automations under  ...  ->  Workflows :

  1. "Item added to project"     -> set Status = Backlog
  2. "Item reopened"             -> set Status = (your choice)
  3. "Pull request merged"       -> set Status = Done
  4. "Issue closed"              -> set Status = Done
  5. "Auto-add / item assigned"  -> set Status = In Progress
     (use the assignment-driven workflow if available in your plan)

These server-side automations keep the board fresh even when an action
(merge, close) comes from GitHub mobile/web with no flow-* run.

Then export these so the flow-* skills find the board:
  export CQLITE_PROJECT_OWNER=${OWNER}
  export CQLITE_PROJECT_NUMBER=${project_number}
========================================================================
EOF
