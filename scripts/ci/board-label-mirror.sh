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
# FAIL-CLOSED (issue #2855): a failed `gh api graphql` (expired/unscoped PAT,
# outage, wrong PROJECT_ID) or an unexpected response shape or an unknown board
# Status must FAIL the run, never degrade to a silent-green "all consistent" with
# zero rows — the exact outage class this mirror exists to catch. Every fail-close
# path is CHECKED explicitly (this script deliberately does NOT rely on `set -e`;
# process substitution masks a producer's exit status, so streams are materialized
# to temp files with a checked status instead).
#
# Sourceable: sourcing this file only DEFINES functions (it must not change the
# caller's shell options), so `set` lives inside main(). Executing it dispatches
# a subcommand: mirror | mirror-one <N> [NODE_ID] | detect | desired <status> |
# require-token.

# The board-derived labels the mirror is authoritative for. spec-review /
# addressing are deliberately NOT in this set.
BLM_DERIVED_LABELS="status:ready status:in-progress status:in-review"

# blm_desired_label STATUS -> echoes the status:* label the mirror sets for that
# board Status (empty for the KNOWN no-label statuses Backlog/Done/null). Returns
# non-zero (3) for an UNKNOWN non-empty Status so callers fail closed instead of
# defaulting an unrecognized/renamed board Status to "strip all labels" (F6). This
# is the single source of the mapping.
blm_desired_label() {
  case "${1:-}" in
    Ready) echo "status:ready" ;;
    "In Progress") echo "status:in-progress" ;;
    "In Review") echo "status:in-review" ;;
    Backlog | Done | "") echo "" ;;
    *) echo ""; return 3 ;;
  esac
  return 0
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
#
# FAIL-CLOSED: `gh api graphql`'s exit status is captured explicitly and a
# non-zero read RETURNS 1; an unexpected response shape (no .data.node.items.nodes
# array) prints ::error:: and RETURNS 1. Neither degrades to zero rows. ::error::
# goes to stderr so it never pollutes the TSV a caller materializes.
blm_board_stream() {
  local want="${1:-}"
  local cursor="" after resp has_next
  while :; do
    if [ -z "$cursor" ]; then after="null"; else after="\"$cursor\""; fi
    if ! resp=$(gh api graphql -f query="
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
      }" -f p="$PROJECT_ID"); then
      echo "::error::board→label mirror: 'gh api graphql' failed (token scope / PROJECT_ID / API outage). Refusing to proceed on a partial or empty board read (issue #2855)." >&2
      return 1
    fi

    # Assert the response shape. A genuinely empty board still satisfies `arrays`;
    # error JSON ({"errors":[…]}) or any malformed body FAILs closed here.
    if ! printf '%s' "$resp" | jq -e '.data.node.items.nodes | arrays' >/dev/null 2>&1; then
      echo "::error::board→label mirror: unexpected GraphQL response (no .data.node.items.nodes array — auth error / wrong PROJECT_ID?). Refusing to proceed (issue #2855)." >&2
      return 1
    fi

    printf '%s' "$resp" | jq -r --arg want "$want" '
      .data.node.items.nodes[]
      | select(.content.__typename=="Issue")
      | select(.content.state=="OPEN")
      | select($want=="" or (.content.number|tostring)==$want)
      | [ (.content.number|tostring),
          (.fieldValueByName.name // ""),
          (.content.createdAt // ""),
          ([.content.labels.nodes[].name] | join(",")) ]
      | @tsv' || return 1

    has_next=$(printf '%s' "$resp" | jq -r '.data.node.items.pageInfo.hasNextPage')
    cursor=$(printf '%s' "$resp" | jq -r '.data.node.items.pageInfo.endCursor')
    [ "$has_next" = "true" ] || break
  done
}

# blm_item_by_node NODE_ID — emit the single board TSV row for the issue with the
# given GraphQL node id, resolving its project item DIRECTLY (no whole-board scan)
# via node(id){ projectItems }. Selects only the item in OUR PROJECT_ID. Emits
# nothing when the issue is not on our board — the caller (mirror-one) treats an
# empty result as the "no board row" ::error:: case (F2). RETURNS 4 (no output)
# when the issue exists but is CLOSED — a benign routine triage label touch on a
# closed issue is NOT an error (issue #2855, G2). FAIL-CLOSED (return 1) on a
# gh/shape failure exactly like blm_board_stream.
blm_item_by_node() {
  local node="$1" resp state
  if ! resp=$(gh api graphql -f query='
    query($id: ID!) {
      node(id: $id) {
        ... on Issue {
          number state createdAt
          labels(first: 50) { nodes { name } }
          projectItems(first: 20) {
            nodes {
              project { id }
              fieldValueByName(name: "Status") {
                ... on ProjectV2ItemFieldSingleSelectValue { name }
              }
            }
          }
        }
      }
    }' -f id="$node"); then
    echo "::error::board→label mirror: 'gh api graphql' (node lookup) failed for node '$node' — refusing to proceed (issue #2855)." >&2
    return 1
  fi
  if ! printf '%s' "$resp" | jq -e '.data.node | (.==null) or has("projectItems")' >/dev/null 2>&1; then
    echo "::error::board→label mirror: unexpected GraphQL response for node '$node' — refusing to proceed (issue #2855)." >&2
    return 1
  fi
  # Distinguish CLOSED (benign, return 4, no row) from OPEN-off-board (empty row,
  # caller emits the ::error::). issues:[labeled,unlabeled] fires on closed-issue
  # triage tagging, which must not red the run (G2).
  state=$(printf '%s' "$resp" | jq -r '.data.node.state // ""' 2>/dev/null)
  if [ "$state" != "OPEN" ] && [ -n "$state" ]; then
    return 4
  fi
  printf '%s' "$resp" | jq -r --arg pid "$PROJECT_ID" '
    .data.node
    | select(. != null)
    | select(.state=="OPEN")
    | . as $iss
    | ( [ .projectItems.nodes[] | select(.project.id==$pid) ] | first ) as $item
    | select($item != null)
    | [ ($iss.number|tostring),
        ($item.fieldValueByName.name // ""),
        ($iss.createdAt // ""),
        ([$iss.labels.nodes[].name] | join(",")) ]
    | @tsv' || return 1
}

# blm_mirror_stream — read the board TSV on stdin and idempotently reconcile each
# issue's board-derived label via `gh issue edit`: add the desired label if absent,
# remove each OTHER board-derived label that is present. spec-review / addressing
# are never in BLM_DERIVED_LABELS, so they are never touched.
#
# FAIL-CLOSED: an UNKNOWN board Status never strips labels — it emits ::error:: and
# fails (F6). A failed `gh issue edit` emits ::error:: and fails rather than logging
# a success that never happened (F7). Returns non-zero if any row failed.
blm_mirror_stream() {
  local n status created labels desired dstat lbl rc=0
  while IFS=$'\t' read -r n status created labels; do
    [ -n "$n" ] || continue
    desired=$(blm_desired_label "$status"); dstat=$?
    if [ "$dstat" -ne 0 ]; then
      echo "::error::issue #$n: unknown board Status '$status' — mapping not updated. Refusing to strip labels (issue #2855). Update blm_desired_label if the board schema changed."
      rc=1
      continue
    fi
    if [ -n "$desired" ] && ! _blm_csv_has "$labels" "$desired"; then
      if gh issue edit "$n" --add-label "$desired" >/dev/null; then
        echo "mirror: #$n Status='$status' +$desired"
      else
        echo "::error::issue #$n: failed to add label '$desired' (gh issue edit)"
        rc=1
      fi
    fi
    for lbl in $BLM_DERIVED_LABELS; do
      [ "$lbl" = "$desired" ] && continue
      if _blm_csv_has "$labels" "$lbl"; then
        if gh issue edit "$n" --remove-label "$lbl" >/dev/null; then
          echo "mirror: #$n Status='$status' -$lbl"
        else
          echo "::error::issue #$n: failed to remove stale label '$lbl' (gh issue edit)"
          rc=1
        fi
      fi
    done
  done
  return "$rc"
}

# _blm_row_bad N STATUS LABELS [emit] -> return 0 if the row is consistent, 1 if
# drifted (or the Status is unknown). When the 4th arg is "emit", print an
# ::error:: annotation for each disagreement. Shared by the first-pass scan and
# the self-heal re-check so both apply the identical rule.
_blm_row_bad() {
  local n="$1" status="$2" labels="$3" emit="${4:-}" desired dstat lbl bad=0
  desired=$(blm_desired_label "$status"); dstat=$?
  if [ "$dstat" -ne 0 ]; then
    [ "$emit" = emit ] && echo "::error::issue #$n: unknown board Status '$status' — mapping not updated (issue #2855)"
    return 1
  fi
  if [ -n "$desired" ] && ! _blm_csv_has "$labels" "$desired"; then
    [ "$emit" = emit ] && echo "::error::issue #$n: board Status='$status' but label '$desired' is missing (labels: ${labels:-none})"
    bad=1
  fi
  for lbl in $BLM_DERIVED_LABELS; do
    [ "$lbl" = "$desired" ] && continue
    if _blm_csv_has "$labels" "$lbl"; then
      [ "$emit" = emit ] && echo "::error::issue #$n: board Status='$status' but carries stale board-derived label '$lbl'"
      bad=1
    fi
  done
  return "$bad"
}

# _blm_offboard_check BOARD_FILE -> FAIL (return 1 + ::error::) when an OPEN issue
# carries a board-derived label but has NO item on the board (F2). Such an issue
# (a documented auto-add miss — flow-groom step 5) keeps its creation-time
# status:* label forever and the board-only detector never sees it → stale label =
# wrong-grab hazard. Enumerated per label via `gh issue list`; a gh failure here
# FAILs closed (never a silent "no offenders").
_blm_offboard_check() {
  local board_file="$1" lbl listed num rc=0 limit count
  # Bound the per-label list explicitly: `gh issue list` defaults to 30 and would
  # SILENTLY truncate a larger set (there can be >30 status:ready in a burst),
  # re-opening the silent-green-partial-read hole (issue #2855). Ask for a large
  # cap and, if the returned count HITS that cap, fail closed — a full page means
  # the list may be truncated and the off-board scan cannot be trusted.
  # BLM_OFFBOARD_LIMIT overridable only for the test's non-truncation assertion.
  limit="${BLM_OFFBOARD_LIMIT:-1000}"
  for lbl in $BLM_DERIVED_LABELS; do
    if ! listed=$(gh issue list --state open --label "$lbl" --limit "$limit" --json number --jq '.[].number'); then
      echo "::error::board→label mirror: 'gh issue list --label $lbl' failed — refusing to proceed on a partial off-board scan (issue #2855)."
      rc=1
      continue
    fi
    count=$(printf '%s\n' "$listed" | grep -c '[0-9]' || true)
    if [ "$count" -ge "$limit" ]; then
      echo "::error::board→label mirror: 'gh issue list --label $lbl' returned $count issue(s) == --limit ($limit) — the off-board scan may be truncated. Refusing to proceed on a possibly-partial list (issue #2855)."
      rc=1
      continue
    fi
    while IFS= read -r num; do
      [ -n "$num" ] || continue
      if ! awk -F'\t' -v n="$num" '$1==n{found=1} END{exit found?0:1}' "$board_file" </dev/null; then
        echo "::error::issue #$num carries board-derived label '$lbl' but is NOT on the project board (auto-add miss?) — stale label = wrong-grab hazard. Add it to the board or strip the label (issue #2855)."
        rc=1
      fi
    done <<<"$listed"
  done
  return "$rc"
}

# blm_detect_stream BOARD_FILE — read the materialized board TSV FILE and FAIL
# (exit 1 + ::error::) on any board-derived-label ≠ Status disagreement. Issues
# younger than the grace window (BLM_GRACE_SECS, default 600s) are skipped to avoid
# flapping on the auto-add / mirror-settle race the sweep already tolerates.
#
# SELF-HEALING (F5): a first-pass violation on an OLDER issue is RE-READ fresh
# (its single row re-fetched from the board) and only counted if the disagreement
# PERSISTS — a Status flip that races between the mirror and this detect step (a
# worker claim, or mirror-one racing the sweep) no longer reds the run. Then also
# runs the off-board check (F2). Exit 0 only when everything is consistent.
blm_detect_stream() {
  local board_file="$1"
  local grace cutoff now n status created labels bad=0 cepoch fresh
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
    # First-pass: consistent -> nothing to do.
    if _blm_row_bad "$n" "$status" "$labels"; then
      continue
    fi
    # Candidate violation: self-heal by re-reading this one issue fresh. Only a
    # PERSISTENT disagreement is a real drift; a raced Status flip resolves here.
    if ! fresh=$(blm_board_stream "$n"); then
      echo "::error::issue #$n: drift-detector re-read (self-heal) failed — refusing to declare consistent (issue #2855)"
      bad=$((bad + 1))
      continue
    fi
    if [ -z "$fresh" ]; then
      # Vanished from the board between passes (closed/removed) — no longer a
      # board-vs-label drift; the off-board check below covers a lingering label.
      continue
    fi
    # fresh is a single TSV row; re-check with emit so the annotation reflects the
    # authoritative current state.
    local fn fstatus fcreated flabels
    IFS=$'\t' read -r fn fstatus fcreated flabels <<<"$fresh"
    if ! _blm_row_bad "$fn" "$fstatus" "$flabels" emit; then
      bad=$((bad + 1))
    fi
  done <"$board_file"

  if ! _blm_offboard_check "$board_file"; then
    bad=$((bad + 1))
  fi

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
  local bf rc
  case "$cmd" in
    desired)
      blm_desired_label "${2:-}"
      ;;
    require-token)
      blm_require_token
      ;;
    mirror)
      blm_require_token || return 1
      # Materialize with a CHECKED status: process substitution / a bare pipe
      # would mask blm_board_stream's exit status, re-opening the silent-green
      # hole (issue #2855). A failed read never reaches the mirror.
      bf=$(mktemp) || return 1
      if ! blm_board_stream >"$bf"; then rm -f "$bf"; return 1; fi
      blm_mirror_stream <"$bf"; rc=$?
      rm -f "$bf"
      return "$rc"
      ;;
    mirror-one)
      blm_require_token || return 1
      [ -n "${2:-}" ] || { echo "::error::mirror-one needs an issue number" >&2; return 2; }
      bf=$(mktemp) || return 1
      # With a node id (workflow path), resolve the issue's project item directly
      # (no whole-board scan, F4). Without one (test/manual), fall back to a
      # client-side-filtered board scan.
      if [ -n "${3:-}" ]; then
        blm_item_by_node "$3" >"$bf"; rc=$?
        if [ "$rc" = 4 ]; then
          rm -f "$bf"
          echo "mirror-one: issue #$2 is closed — routine label touch, nothing to mirror (issue #2855)."
          return 0
        fi
        if [ "$rc" != 0 ]; then rm -f "$bf"; return 1; fi
      else
        if ! blm_board_stream "$2" >"$bf"; then rm -f "$bf"; return 1; fi
      fi
      if [ ! -s "$bf" ]; then
        rm -f "$bf"
        echo "::error::mirror-one: issue #$2 produced no board row — it is not on the project board (auto-add miss?) or is closed. Not silently no-op'ing (issue #2855)."
        return 1
      fi
      blm_mirror_stream <"$bf"; rc=$?
      rm -f "$bf"
      return "$rc"
      ;;
    detect)
      blm_require_token || return 1
      bf=$(mktemp) || return 1
      if ! blm_board_stream >"$bf"; then rm -f "$bf"; return 1; fi
      blm_detect_stream "$bf"; rc=$?
      rm -f "$bf"
      return "$rc"
      ;;
    *)
      echo "usage: board-label-mirror.sh {mirror|mirror-one <N> [NODE_ID]|detect|desired <status>|require-token}" >&2
      return 2
      ;;
  esac
}

# Sourceable guard: sourcing only defines functions; executing dispatches main.
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
  main "$@"
fi
