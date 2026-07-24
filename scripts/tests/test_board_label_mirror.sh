#!/usr/bin/env bash
# scripts/tests/test_board_label_mirror.sh — fast, self-contained tests for the
# enforced board→label mirror (issue #2855). No cargo, no gate, no network: the
# `gh`/GraphQL layer is a stub script that SIMULATES the project board + issue
# labels from a per-test state file, modelled on test_worker_supervisor.sh's
# stubbing approach. Target: <10s total.
#
# The unit under test is the sourceable seam scripts/ci/board-label-mirror.sh
# (the same code the .github/workflows/project-board-sync.yml workflow calls).
set -uo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SELF_DIR/../.." && pwd)"
MIRROR="$REPO_ROOT/scripts/ci/board-label-mirror.sh"

PASS_COUNT=0
FAIL_COUNT=0
pass() {
  PASS_COUNT=$((PASS_COUNT + 1))
  echo "PASS: $1"
}
fail() {
  FAIL_COUNT=$((FAIL_COUNT + 1))
  echo "FAIL: $1"
}

if ! command -v jq >/dev/null 2>&1; then
  echo "SKIP: jq unavailable (the mirror + this test's board stub both need jq)"
  exit 0
fi

TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/cqlite-blm-test.XXXXXX")"
cleanup() { rm -rf "$TMP_ROOT" 2>/dev/null || true; }
trap cleanup EXIT

# ---------------------------------------------------------------------------
# gh stub: simulates the board + issue labels from a TSV state file.
#   STATE_FILE lines: number<TAB>status<TAB>createdAt<TAB>label,csv
#   - `gh api graphql ...` (items query) -> the Projects v2 JSON, one page.
#   - `gh issue edit N --add-label X`     -> add X to #N's label csv.
#   - `gh issue edit N --remove-label X`  -> remove X from #N's label csv.
# ---------------------------------------------------------------------------
write_gh_stub() {
  cat >"$1" <<'EOF'
#!/usr/bin/env bash
set -uo pipefail
: "${STATE_FILE:?STATE_FILE not set}"

# --- gh api graphql ... : emit the board items JSON from STATE_FILE ---------
if [ "${1:-}" = "api" ] && [ "${2:-}" = "graphql" ]; then
  # Build the items JSON with jq from the TSV state file.
  nodes=$(jq -Rn '
    [ inputs
      | select(length>0)
      | split("\t")
      | { fieldValueByName: (if (.[1]|length)>0 then {name: .[1]} else null end),
          content: {
            __typename: "Issue",
            number: (.[0]|tonumber),
            state: "OPEN",
            createdAt: (if (.[2]|length)>0 then .[2] else null end),
            labels: { nodes: ( if (.[3]|length)>0
                               then (.[3]|split(",")|map({name:.}))
                               else [] end ) }
          } } ]' "$STATE_FILE")
  jq -n --argjson nodes "$nodes" '
    { data: { node: { items: {
        pageInfo: { hasNextPage: false, endCursor: null },
        nodes: $nodes } } } }'
  exit 0
fi

# --- gh issue edit N --add-label X / --remove-label X -----------------------
if [ "${1:-}" = "issue" ] && [ "${2:-}" = "edit" ]; then
  num="${3:-}"
  op="" lbl=""
  shift 3
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --add-label) op="add"; lbl="$2"; shift 2 ;;
      --remove-label) op="remove"; lbl="$2"; shift 2 ;;
      *) shift ;;
    esac
  done
  [ -n "$op" ] || exit 0
  # record the edit for assertions
  printf '%s\t%s\t%s\n' "$num" "$op" "$lbl" >>"${EDIT_LOG:?EDIT_LOG not set}"
  # mutate the state file's label csv for #num
  tmp="$(mktemp)"
  while IFS=$'\t' read -r n st cr labels; do
    if [ "$n" = "$num" ]; then
      # normalize csv into a set
      newl=""
      IFS=',' read -ra arr <<<"$labels"
      declare -A seen=()
      for e in "${arr[@]}"; do [ -n "$e" ] && seen["$e"]=1; done
      if [ "$op" = "add" ]; then seen["$lbl"]=1; else unset "seen[$lbl]"; fi
      for k in "${!seen[@]}"; do newl="${newl:+$newl,}$k"; done
      printf '%s\t%s\t%s\t%s\n' "$n" "$st" "$cr" "$newl" >>"$tmp"
    else
      printf '%s\t%s\t%s\t%s\n' "$n" "$st" "$cr" "$labels" >>"$tmp"
    fi
  done <"$STATE_FILE"
  mv "$tmp" "$STATE_FILE"
  exit 0
fi

echo "gh stub: unhandled args: $*" >&2
exit 3
EOF
  chmod +x "$1"
}

# new_case DIR: set up a case dir with a gh stub on PATH + a fresh edit log.
# Writes STATE_FILE / EDIT_LOG / GH_TOKEN / PROJECT_ID into the current env.
new_case() {
  local d
  d="$(mktemp -d "$TMP_ROOT/case.XXXXXX")"
  mkdir -p "$d/bin"
  write_gh_stub "$d/bin/gh"
  export PATH="$d/bin:$PATH_ORIG"
  export STATE_FILE="$d/state.tsv"
  export EDIT_LOG="$d/edits.tsv"
  : >"$EDIT_LOG"
  export GH_TOKEN="stub-token"
  export PROJECT_ID="PVT_stub"
  # Old createdAt so the detector's grace window never skips test rows.
  export BLM_GRACE_SECS="0"
  echo "$d"
}
PATH_ORIG="$PATH"

# seed_state ROW... : write TSV rows (number<TAB>status<TAB>created<TAB>labels).
seed_state() {
  : >"$STATE_FILE"
  local r
  for r in "$@"; do printf '%s\n' "$r" >>"$STATE_FILE"; done
}

# labels_of N -> the current label csv for issue N from STATE_FILE.
labels_of() {
  awk -F'\t' -v n="$1" '$1==n {print $4}' "$STATE_FILE"
}
has_label() { case ",$(labels_of "$1")," in *",$2,"*) return 0 ;; *) return 1 ;; esac; }

OLD_TS="2020-01-01T00:00:00Z"

# ---------------------------------------------------------------------------
# 1. Ready -> status:ready set, other board labels removed, spec-review/addressing untouched
# ---------------------------------------------------------------------------
new_case >/dev/null
seed_state \
  "$(printf '101\tReady\t%s\tstatus:in-progress,status:spec-review' "$OLD_TS")"
bash "$MIRROR" mirror >/dev/null 2>&1
if has_label 101 "status:ready" \
  && ! has_label 101 "status:in-progress" \
  && has_label 101 "status:spec-review"; then
  pass "Ready sets status:ready, removes status:in-progress, keeps status:spec-review"
else
  fail "Ready mirror wrong: labels=$(labels_of 101)"
fi

# ---------------------------------------------------------------------------
# 2. In Progress -> status:in-progress; stale status:ready removed; addressing kept
# ---------------------------------------------------------------------------
new_case >/dev/null
seed_state "$(printf '102\tIn Progress\t%s\tstatus:ready,status:addressing' "$OLD_TS")"
bash "$MIRROR" mirror >/dev/null 2>&1
if has_label 102 "status:in-progress" \
  && ! has_label 102 "status:ready" \
  && has_label 102 "status:addressing"; then
  pass "In Progress sets status:in-progress, removes status:ready, keeps status:addressing"
else
  fail "In Progress mirror wrong: labels=$(labels_of 102)"
fi

# ---------------------------------------------------------------------------
# 3. Backlog -> all board-derived labels removed (no status:backlog invented)
# ---------------------------------------------------------------------------
new_case >/dev/null
seed_state "$(printf '103\tBacklog\t%s\tstatus:in-review' "$OLD_TS")"
bash "$MIRROR" mirror >/dev/null 2>&1
if ! has_label 103 "status:in-review" \
  && ! has_label 103 "status:ready" \
  && ! has_label 103 "status:in-progress"; then
  pass "Backlog removes all board-derived status labels"
else
  fail "Backlog mirror wrong: labels=$(labels_of 103)"
fi

# ---------------------------------------------------------------------------
# 4. Idempotency: a matching state -> second run makes NO edit
# ---------------------------------------------------------------------------
new_case >/dev/null
seed_state "$(printf '104\tIn Review\t%s\tstatus:in-review' "$OLD_TS")"
bash "$MIRROR" mirror >/dev/null 2>&1
: >"$EDIT_LOG"
bash "$MIRROR" mirror >/dev/null 2>&1
if [ ! -s "$EDIT_LOG" ]; then
  pass "Idempotent: second mirror run on a matching board makes no edit"
else
  fail "Idempotency broken: second run edited: $(cat "$EDIT_LOG")"
fi

# ---------------------------------------------------------------------------
# 5. mirror-one: only the named issue is reconciled
# ---------------------------------------------------------------------------
new_case >/dev/null
seed_state \
  "$(printf '105\tReady\t%s\t' "$OLD_TS")" \
  "$(printf '106\tReady\t%s\t' "$OLD_TS")"
bash "$MIRROR" mirror-one 105 >/dev/null 2>&1
if has_label 105 "status:ready" && ! has_label 106 "status:ready"; then
  pass "mirror-one reconciles only the named issue"
else
  fail "mirror-one leaked: #105=$(labels_of 105) #106=$(labels_of 106)"
fi

# ---------------------------------------------------------------------------
# 6. Detector: seeded mismatch -> non-zero exit + ::error::
# ---------------------------------------------------------------------------
new_case >/dev/null
seed_state "$(printf '107\tReady\t%s\tstatus:in-progress' "$OLD_TS")"
out="$(bash "$MIRROR" detect 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "::error::"; then
  pass "Detector exits non-zero + ::error:: on a seeded mismatch"
else
  fail "Detector did not fail on mismatch (rc=$rc): $out"
fi

# ---------------------------------------------------------------------------
# 7. Detector: a consistent board -> exit 0
# ---------------------------------------------------------------------------
new_case >/dev/null
seed_state \
  "$(printf '108\tReady\t%s\tstatus:ready' "$OLD_TS")" \
  "$(printf '109\tBacklog\t%s\t' "$OLD_TS")" \
  "$(printf '110\tIn Review\t%s\tstatus:in-review,status:spec-review' "$OLD_TS")"
if bash "$MIRROR" detect >/dev/null 2>&1; then
  pass "Detector exits 0 on a fully consistent board (spec-review ignored)"
else
  fail "Detector failed on a consistent board"
fi

# ---------------------------------------------------------------------------
# 8. Missing token -> fail loud (never a silent skip)
# ---------------------------------------------------------------------------
new_case >/dev/null
seed_state "$(printf '111\tReady\t%s\t' "$OLD_TS")"
unset GH_TOKEN
out="$(bash "$MIRROR" mirror 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "PROJECTS_TOKEN not set"; then
  pass "Missing PROJECTS_TOKEN fails loud (mirror)"
else
  fail "Missing token did not fail loud (rc=$rc): $out"
fi
out="$(bash "$MIRROR" detect 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "PROJECTS_TOKEN not set"; then
  pass "Missing PROJECTS_TOKEN fails loud (detect)"
else
  fail "Missing token did not fail loud on detect (rc=$rc): $out"
fi
export GH_TOKEN="stub-token"

# ---------------------------------------------------------------------------
# 9. Doctrine grep: no flow-* skill writes a board-derived status label
# ---------------------------------------------------------------------------
offenders=""
for f in "$REPO_ROOT"/.claude/skills/flow-*/SKILL.md; do
  [ -f "$f" ] || continue
  if grep -Eq -- '(add|remove)-label +status:(ready|in-progress|in-review)' "$f"; then
    offenders="${offenders} ${f}"
  fi
done
if [ -z "$offenders" ]; then
  pass "No flow-* skill writes status:ready/in-progress/in-review"
else
  fail "flow-* skill(s) still write a board-derived status label:${offenders}"
fi

# ---------------------------------------------------------------------------
# 10. Workflow injection lint clean on the edited board-sync workflow
# ---------------------------------------------------------------------------
INJ="$REPO_ROOT/scripts/ci/check-workflow-injection.sh"
if [ -x "$INJ" ] || [ -f "$INJ" ]; then
  if bash "$INJ" "$REPO_ROOT/.github/workflows/project-board-sync.yml" >/dev/null 2>&1; then
    pass "check-workflow-injection.sh clean on project-board-sync.yml"
  else
    fail "check-workflow-injection.sh flagged project-board-sync.yml"
  fi
else
  pass "check-workflow-injection.sh absent (skipped injection lint)"
fi

# ---------------------------------------------------------------------------
echo "----"
echo "board-label-mirror: PASS=$PASS_COUNT FAIL=$FAIL_COUNT"
[ "$FAIL_COUNT" -eq 0 ]
