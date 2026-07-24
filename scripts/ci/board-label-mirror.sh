#!/usr/bin/env bash
# scripts/ci/board-label-mirror.sh — the enforced one-way board→label mirror
# (issue #2855). Factored OUT of .github/workflows/project-board-sync.yml so the
# mapping + mirror + drift-detector logic is a single source that the workflow
# CALLS and the shell test (scripts/tests/test_board_label_mirror.sh) can source
# and drive directly with a stubbed `gh`/GraphQL layer.
#
# One-way projection: board Status -> status:* label. Board Status is the single
# source of truth; the `status:*` label is a DERIVED, enforced read-mirror for
# cheap discovery (never the claim authority — claim ref + fresh board read stay
# authoritative; see CLAUDE.md Path A).
#
# Board-derived mapping (the ONLY labels the mirror owns):
#   Ready        -> status:ready
#   In Progress  -> status:in-progress
#   In Review    -> status:in-review
#   Backlog/Done -> (no status:* label)
#
# status:spec-review / status:addressing are transient skill-managed sub-markers
# of In Progress and are NOT board Status options — the mirror NEVER touches them.
#
# Sourceable: sourcing this file only DEFINES functions (it must not change the
# caller's shell options), so `set` lives inside main(). Executing it dispatches
# a subcommand: mirror | mirror-one <N> | detect | desired <status> | require-token.

# The board-derived labels the mirror is authoritative for. spec-review /
# addressing are deliberately NOT in this set.
BLM_DERIVED_LABELS="status:ready status:in-progress status:in-review"

# blm_desired_label STATUS -> the status:* label the mirror sets for that board
# Status (empty for Backlog/Done/unknown/null). Single source of the mapping.
blm_desired_label() {
  case "${1:-}" in
    Ready) echo "status:ready" ;;
    "In Progress") echo "status:in-progress" ;;
    "In Review") echo "status:in-review" ;;
    Backlog | Done | "") echo "" ;;
    *) echo "" ;;
  esac
}

# _blm_csv_has CSV NEEDLE -> 0 if NEEDLE is an element of the comma-joined CSV.
_blm_csv_has() {
  case ",${1}," in
    *",${2},"*) return 0 ;;
    *) return 1 ;;
  esac
}

# _blm_iso_to_epoch ISO8601 -> epoch seconds (empty on parse failure). Uses jq's
# builtin fromdateiso8601 so it is portable across GNU and BSD `date`.
_blm_iso_to_epoch() {
  [ -n "${1:-}" ] || { echo ""; return 0; }
  jq -rn --arg d "$1" 'try ($d|fromdateiso8601|tostring) catch ""' 2>/dev/null
}

# blm_require_token — fail loud (::error:: + non-zero) when the project-scoped
# PAT is absent, mirroring the workflow's existing "Guard token" posture (#2655).
# The mirror/detector cannot read user Projects v2 without it, so a missing token
# must FAIL the run rather than silently skip.
blm_require_token() {
  if [ -z "${GH_TOKEN:-}" ]; then
    echo "::error::PROJECTS_TOKEN not set — board→label mirror is NOT running. Add a PAT (or GitHub App token) with 'project' scope as repo secret PROJECTS_TOKEN. Failing loudly (issue #2655/#2855) instead of silently skipping the mirror/detector."
    return 1
  fi
  return 0
}

# blm_board_stream [ISSUE_NUMBER] — emit one TSV row per OPEN issue on the board:
#   number \t status \t createdAt \t comma-joined-labels
# Paginates the Projects v2 items (Projects v2 has no server-side field filter,
# so an optional ISSUE_NUMBER is applied client-side). Requires PROJECT_ID in env
# and a working `gh`.
blm_board_stream() {
  local want="${1:-}"
  local cursor="" after resp has_next
  while :; do
    if [ -z "$cursor" ]; then after="null"; else after="\"$cursor\""; fi
    resp=$(gh api graphql -f query="
      query(\$p: ID!) {
        node(id: \$p) {
          ... on ProjectV2 {
            items(first: 100, after: $after) {
              pageInfo { hasNextPage endCursor }
              nodes {
                fieldValueByName(name: \"Status\") {
                  ... on ProjectV2ItemFieldSingleSelectValue { name }
                }
                content {
                  __typename
                  ... on Issue {
                    number state createdAt
                    labels(first: 50) { nodes { name } }
                  }
                }
              }
            }
          }
        }
      }" -f p="$PROJECT_ID")

    # Assert the response shape (a jq parse/shape failure FAILs the pipe under
    # pipefail; a genuinely empty board still satisfies `arrays`).
    echo "$resp" | jq -e '.data.node.items.nodes | arrays' >/dev/null

    echo "$resp" | jq -r --arg want "$want" '
      .data.node.items.nodes[]
      | select(.content.__typename=="Issue")
      | select(.content.state=="OPEN")
      | select($want=="" or (.content.number|tostring)==$want)
      | [ (.content.number|tostring),
          (.fieldValueByName.name // ""),
          (.content.createdAt // ""),
          ([.content.labels.nodes[].name] | join(",")) ]
      | @tsv'

    has_next=$(echo "$resp" | jq -r '.data.node.items.pageInfo.hasNextPage')
    cursor=$(echo "$resp" | jq -r '.data.node.items.pageInfo.endCursor')
    [ "$has_next" = "true" ] || break
  done
}

# blm_mirror_stream — read the board TSV on stdin and idempotently reconcile each
# issue's board-derived label via `gh issue edit`: add the desired label if absent,
# remove each OTHER board-derived label that is present. spec-review / addressing
# are never in BLM_DERIVED_LABELS, so they are never touched.
blm_mirror_stream() {
  local n status created labels desired lbl
  while IFS=$'\t' read -r n status created labels; do
    [ -n "$n" ] || continue
    desired=$(blm_desired_label "$status")
    if [ -n "$desired" ] && ! _blm_csv_has "$labels" "$desired"; then
      gh issue edit "$n" --add-label "$desired" >/dev/null
      echo "mirror: #$n Status='$status' +$desired"
    fi
    for lbl in $BLM_DERIVED_LABELS; do
      [ "$lbl" = "$desired" ] && continue
      if _blm_csv_has "$labels" "$lbl"; then
        gh issue edit "$n" --remove-label "$lbl" >/dev/null
        echo "mirror: #$n Status='$status' -$lbl"
      fi
    done
  done
}

# blm_detect_stream — read the board TSV on stdin and FAIL (exit 1 + ::error::) on
# any board-derived-label ≠ Status disagreement. Issues younger than the grace
# window (BLM_GRACE_SECS, default 600s) are skipped to avoid flapping on the
# auto-add / mirror-settle race the sweep already tolerates. spec-review /
# addressing are not board-derived and are ignored. Exit 0 when all consistent.
blm_detect_stream() {
  local grace cutoff now n status created labels desired lbl bad=0 cepoch
  grace="${BLM_GRACE_SECS:-600}"
  now=$(date -u +%s)
  cutoff=$((now - grace))
  while IFS=$'\t' read -r n status created labels; do
    [ -n "$n" ] || continue
    if [ -n "$created" ]; then
      cepoch=$(_blm_iso_to_epoch "$created")
      if [ -n "$cepoch" ] && [ "$cepoch" -gt "$cutoff" ]; then
        continue
      fi
    fi
    desired=$(blm_desired_label "$status")
    if [ -n "$desired" ] && ! _blm_csv_has "$labels" "$desired"; then
      echo "::error::issue #$n: board Status='$status' but label '$desired' is missing (labels: ${labels:-none})"
      bad=$((bad + 1))
    fi
    for lbl in $BLM_DERIVED_LABELS; do
      [ "$lbl" = "$desired" ] && continue
      if _blm_csv_has "$labels" "$lbl"; then
        echo "::error::issue #$n: board Status='$status' but carries stale board-derived label '$lbl'"
        bad=$((bad + 1))
      fi
    done
  done
  if [ "$bad" -gt 0 ]; then
    echo "drift-detector: $bad label/Status violation(s) — see ::error:: annotations above"
    return 1
  fi
  echo "drift-detector: all open issues' board-derived labels match board Status"
  return 0
}

main() {
  set -uo pipefail
  local cmd="${1:-}"
  case "$cmd" in
    desired)
      blm_desired_label "${2:-}"
      ;;
    require-token)
      blm_require_token
      ;;
    mirror)
      blm_require_token || return 1
      blm_board_stream | blm_mirror_stream
      ;;
    mirror-one)
      blm_require_token || return 1
      [ -n "${2:-}" ] || { echo "::error::mirror-one needs an issue number" >&2; return 2; }
      blm_board_stream "$2" | blm_mirror_stream
      ;;
    detect)
      blm_require_token || return 1
      blm_detect_stream < <(blm_board_stream)
      ;;
    *)
      echo "usage: board-label-mirror.sh {mirror|mirror-one <N>|detect|desired <status>|require-token}" >&2
      return 2
      ;;
  esac
}

# Sourceable guard: sourcing only defines functions; executing dispatches main.
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
  main "$@"
fi
