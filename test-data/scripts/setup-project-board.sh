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

# The `project` scope is required for every Projects v2 read/write below.
if ! gh auth status 2>&1 | grep -q "'project'"; then
  echo "error: gh token is missing the 'project' scope." >&2
  echo "       Run: gh auth refresh -s project" >&2
  exit 1
fi

echo "==> Looking for an existing project titled '${TITLE}' (owner ${OWNER})"
project_number=""
existing_json="$(gh project list --owner "${OWNER}" --format json --limit 200 2>/dev/null || echo '{}')"
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
# link is idempotent; ignore "already linked" noise.
gh project link "${project_number}" --owner "${OWNER}" --repo "${REPO}" >/dev/null 2>&1 || true

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
  # Adding options to an existing single-select field is a GraphQL mutation; the
  # gh CLI exposes it via `gh api graphql`. We must resend the FULL desired option
  # set (the mutation replaces the option list), so build it from canonical order.
  echo "    'Status' exists; missing options: ${missing_options[*]} — normalizing the full option set."
  # Build the GraphQL options array in canonical order.
  options_gql="$(
    for opt in "${STATUS_OPTIONS[@]}"; do
      printf '{name: "%s", color: GRAY, description: ""},' "${opt}"
    done
  )"
  options_gql="[${options_gql%,}]"
  gh api graphql -f query="
    mutation {
      updateProjectV2Field(input: {
        fieldId: \"${status_field_id}\",
        singleSelectOptions: ${options_gql}
      }) { projectV2Field { ... on ProjectV2SingleSelectField { id name } } }
    }" >/dev/null
  echo "    Normalized 'Status' options to: ${STATUS_OPTIONS[*]}"
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
