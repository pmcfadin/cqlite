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
# gh stub: simulates the board + issue labels from a TSV state file. Written to
# be bash-3.2-safe (no `declare -A` / associative arrays) so it runs under stock
# macOS /bin/bash exactly like it does under CI's bash-5 (issue #2855, F8) — an
# `declare: -A: invalid option` death would red the whole tooling-tests component
# for an environmental reason, indistinguishable from a real regression.
#   STATE_FILE lines: number<TAB>status<TAB>createdAt<TAB>label,csv
#   - `gh api graphql ...` (items/node query) -> the Projects v2 JSON, one page.
#   - `gh issue edit N --add-label X`     -> add X to #N's label csv.
#   - `gh issue edit N --remove-label X`  -> remove X from #N's label csv.
#   - `gh issue list --state open --label L --json number --jq …` -> the numbers
#       of OPEN issues carrying label L, read from optional ISSUE_LIST_FILE
#       (lines: label<TAB>number); empty when unset — models off-board issues.
# Failure injection (fail-closed tests):
#   GH_FAIL_GRAPHQL=1 -> `gh api graphql` prints an errors JSON and exits 1.
#   GH_FAIL_EDIT=1    -> `gh issue edit` exits 1 without mutating state.
# ---------------------------------------------------------------------------
write_gh_stub() {
  cat >"$1" <<'EOF'
#!/usr/bin/env bash
set -uo pipefail
: "${STATE_FILE:?STATE_FILE not set}"

# --- gh api graphql ... : emit the board items JSON from STATE_FILE ---------
if [ "${1:-}" = "api" ] && [ "${2:-}" = "graphql" ]; then
  if [ -n "${GH_FAIL_GRAPHQL:-}" ]; then
    printf '%s\n' '{"errors":[{"message":"Bad credentials"}]}'
    exit 1
  fi
  # A node(id:) lookup (mirror-one by node id) — resolve one issue's project item
  # directly. Emit the single-issue node JSON. The stub keys the node id as
  # "node-<number>" so tests can pass a deterministic id.
  wants_node=0
  for a in "$@"; do
    case "$a" in id=node-*) wants_node=1; node_num="${a#id=node-}" ;; esac
  done
  if [ "$wants_node" = 1 ]; then
    # CLOSED_NODES (csv of issue numbers) models closed issues for the node lookup
    # (issue #2855, G2): a closed issue emits state "CLOSED" so mirror-one exits 0.
    node_state="OPEN"
    case ",${CLOSED_NODES:-}," in *",${node_num},"*) node_state="CLOSED" ;; esac
    jq -Rn --arg num "$node_num" --arg st "$node_state" '
      ( [ inputs | select(length>0) | split("\t") | select(.[0]==$num) ] | first ) as $row
      | if $row == null
        then { data: { node: null } }
        else { data: { node: {
            number: ($row[0]|tonumber),
            state: $st,
            createdAt: (if ($row[2]|length)>0 then $row[2] else null end),
            labels: { nodes: ( if ($row[3]|length)>0
                               then ($row[3]|split(",")|map({name:.})) else [] end ) },
            projectItems: { nodes: [ {
              project: { id: (env.PROJECT_ID // "PVT_stub") },
              fieldValueByName: (if ($row[1]|length)>0 then {name: $row[1]} else null end)
            } ] } } } }
        end' "$STATE_FILE"
    exit 0
  fi
  # Build the board items JSON with jq from the TSV state file.
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
  # Self-heal hook (issue #2855, G6): after the FIRST full-board emission (the
  # detector's materialize pass), swap STATE_FILE to SELF_HEAL_TO so the per-issue
  # self-heal RE-READ observes a mutated board — models a Status flip racing between
  # the mirror and the detect step. Counter file keeps it a one-shot.
  if [ -n "${SELF_HEAL_TO:-}" ]; then
    cf="$STATE_FILE.sh_calls"
    c=$(cat "$cf" 2>/dev/null || echo 0)
    echo $((c + 1)) >"$cf"
    if [ "$c" = 0 ]; then cp "$SELF_HEAL_TO" "$STATE_FILE"; fi
  fi
  exit 0
fi

# --- gh issue list --state open --label L --json number --jq … --------------
if [ "${1:-}" = "issue" ] && [ "${2:-}" = "list" ]; then
  want_label=""
  shift 2
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --label) want_label="$2"; shift 2 ;;
      *) shift ;;
    esac
  done
  if [ -n "${ISSUE_LIST_FILE:-}" ] && [ -f "$ISSUE_LIST_FILE" ]; then
    awk -F'\t' -v l="$want_label" '$1==l {print $2}' "$ISSUE_LIST_FILE"
  fi
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
  if [ -n "${GH_FAIL_EDIT:-}" ]; then
    exit 1
  fi
  # record the edit for assertions
  printf '%s\t%s\t%s\n' "$num" "$op" "$lbl" >>"${EDIT_LOG:?EDIT_LOG not set}"
  # mutate the state file's label csv for #num (portable CSV, no associative arrays)
  tmp="$(mktemp)"
  while IFS=$'\t' read -r n st cr labels; do
    if [ "$n" = "$num" ]; then
      newl=""
      # split existing csv, dropping the target label (remove) or any dup (add);
      # then append it once for add. `set -f` guards against globbing on labels.
      set -f
      old_ifs="$IFS"; IFS=','
      for e in $labels; do
        [ -n "$e" ] || continue
        [ "$e" = "$lbl" ] && continue
        newl="${newl:+$newl,}$e"
      done
      IFS="$old_ifs"; set +f
      if [ "$op" = "add" ]; then newl="${newl:+$newl,}$lbl"; fi
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
# The doctrine check, factored into ONE helper (issue #2855, G5) so the
# enforcement scan (test 9) AND the F3 regression (test 11) exercise the IDENTICAL
# pipeline: match a board-derived label WRITE — `--add-label`/`--remove-label` AND
# a bare `gh issue create … --label "status:ready"` — then drop lines that are READ
# commands (`gh issue list --label status:ready`, `gh search`, `gh pr list --label`,
# legitimate #2855 cheap discovery). The mirror DERIVES status:* from the board, so
# any doc SETTING one by hand is an offender.
#   blm_doctrine_hits FILE -> prints offending line(s); exit 0 iff any hit.
blm_doctrine_hits() {
  grep -En -- '--(add-|remove-)?label[[:space:]]+["'"'"']?status:(ready|in-progress|in-review)' "$1" \
    | grep -Ev -- '(issue|pr|search)[[:space:]]+(list|status)|gh[[:space:]]+search'
}

# ---------------------------------------------------------------------------
# 9. Doctrine grep: no flow-* skill / worker / flow-lead doc writes a board-derived
#    status label. Extended set (G5): commands/worker.md + agents/flow-lead.md, not
#    just skills. Counts scanned files and FAILs if ZERO were scanned (a vacuous
#    green if skills/docs move or rename).
# ---------------------------------------------------------------------------
offenders=""
scanned=0
for f in \
  "$REPO_ROOT"/.claude/skills/flow-*/SKILL.md \
  "$REPO_ROOT"/.claude/commands/worker.md \
  "$REPO_ROOT"/.claude/agents/flow-lead.md; do
  [ -f "$f" ] || continue
  scanned=$((scanned + 1))
  if blm_doctrine_hits "$f" | grep -q .; then
    offenders="${offenders} ${f}"
  fi
done
if [ "$scanned" -eq 0 ]; then
  fail "Doctrine scan matched ZERO files — flow-*/worker/flow-lead docs moved or renamed (vacuous green guard, G5)"
elif [ -z "$offenders" ]; then
  pass "No flow-*/worker/flow-lead doc writes a board-derived status label ($scanned file(s) scanned)"
else
  fail "doc(s) still write a board-derived status label:${offenders}"
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
# 11. F3 regression: the SAME helper (blm_doctrine_hits) catches a write
#     `gh issue create --label "status:ready"` AND does not flag a read-only
#     `gh issue list --label status:ready` — exercising the identical pipeline
#     (raw pattern + the -Ev read-command exclusion) the enforcement scan uses (G5).
# ---------------------------------------------------------------------------
tmp_skill_dir="$(mktemp -d "$TMP_ROOT/skill.XXXXXX")"
cat >"$tmp_skill_dir/SKILL.md" <<'EOF'
gh issue create --title "x" --body "y" --label "P2" --label "status:ready"
gh issue list --label status:ready --json number
EOF
hits="$(blm_doctrine_hits "$tmp_skill_dir/SKILL.md")"
if printf '%s' "$hits" | grep -q "issue create" \
  && ! printf '%s' "$hits" | grep -q "issue list"; then
  pass "F3/G5 helper catches the create-write but excludes the list-read (same pipeline)"
else
  fail "F3/G5 helper wrong: hits=[$hits]"
fi

# ---------------------------------------------------------------------------
# 12. F1: gh api graphql failure -> BOTH mirror AND detect exit non-zero (never
#     a silent-green "all consistent" with zero rows).
# ---------------------------------------------------------------------------
new_case >/dev/null
seed_state "$(printf '112\tReady\t%s\t' "$OLD_TS")"
export GH_FAIL_GRAPHQL=1
out_m="$(bash "$MIRROR" mirror 2>&1)"; rc_m=$?
out_d="$(bash "$MIRROR" detect 2>&1)"; rc_d=$?
unset GH_FAIL_GRAPHQL
if [ "$rc_m" -ne 0 ] && [ "$rc_d" -ne 0 ] \
  && printf '%s%s' "$out_m" "$out_d" | grep -q "::error::"; then
  pass "F1: gh api graphql failure fails BOTH mirror and detect (non-zero + ::error::)"
else
  fail "F1: graphql failure not fail-closed (mirror rc=$rc_m detect rc=$rc_d): $out_m | $out_d"
fi

# ---------------------------------------------------------------------------
# 13. F2: an OPEN issue carrying a board-derived label but NOT on the board ->
#     detector FAILs (off-board stale label = wrong-grab hazard).
# ---------------------------------------------------------------------------
d="$(new_case)"
# Board has only #113 (consistent). #199 is OPEN with status:ready but off-board.
seed_state "$(printf '113\tReady\t%s\tstatus:ready' "$OLD_TS")"
export ISSUE_LIST_FILE="$d/issuelist.tsv"
printf 'status:ready\t199\n' >"$ISSUE_LIST_FILE"
out="$(bash "$MIRROR" detect 2>&1)"; rc=$?
unset ISSUE_LIST_FILE
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "199"; then
  pass "F2: off-board OPEN issue with a board-derived label fails the detector"
else
  fail "F2: off-board issue not detected (rc=$rc): $out"
fi

# ---------------------------------------------------------------------------
# 14. F6: an unknown/renamed non-empty board Status -> mirror FAILs and does NOT
#     strip labels (a board-schema change must not silently delete labels).
# ---------------------------------------------------------------------------
new_case >/dev/null
seed_state "$(printf '114\tStaging\t%s\tstatus:ready' "$OLD_TS")"
out="$(bash "$MIRROR" mirror 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] \
  && printf '%s' "$out" | grep -q "unknown board Status" \
  && has_label 114 "status:ready"; then
  pass "F6: unknown board Status fails the mirror and does NOT strip labels"
else
  fail "F6: unknown Status not fail-closed (rc=$rc, labels=$(labels_of 114)): $out"
fi
# and the detector likewise fails on an unknown Status
out="$(bash "$MIRROR" detect 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "unknown board Status"; then
  pass "F6: unknown board Status also fails the detector"
else
  fail "F6: unknown Status not caught by detector (rc=$rc): $out"
fi

# ---------------------------------------------------------------------------
# 15. F7: a failing `gh issue edit` -> mirror exits non-zero + ::error:: (never a
#     log line asserting an edit that never happened).
# ---------------------------------------------------------------------------
new_case >/dev/null
seed_state "$(printf '115\tReady\t%s\t' "$OLD_TS")"
export GH_FAIL_EDIT=1
out="$(bash "$MIRROR" mirror 2>&1)"; rc=$?
unset GH_FAIL_EDIT
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "::error::" \
  && ! printf '%s' "$out" | grep -q "^mirror: #115 .*+status:ready"; then
  pass "F7: failed gh issue edit fails the mirror (no false success log)"
else
  fail "F7: failed edit not fail-closed (rc=$rc): $out"
fi

# ---------------------------------------------------------------------------
# 16. F4: mirror-one by NODE id resolves the item directly and reconciles it.
# ---------------------------------------------------------------------------
new_case >/dev/null
seed_state "$(printf '116\tReady\t%s\t' "$OLD_TS")"
bash "$MIRROR" mirror-one 116 node-116 >/dev/null 2>&1
if has_label 116 "status:ready"; then
  pass "F4: mirror-one resolves the item by node id and reconciles the label"
else
  fail "F4: mirror-one by node id did not reconcile: #116=$(labels_of 116)"
fi

# ---------------------------------------------------------------------------
# 17. F2 (mirror-one): a #N that is NOT on the board -> mirror-one exits non-zero
#     + ::error:: (never a silent no-op exit 0).
# ---------------------------------------------------------------------------
new_case >/dev/null
seed_state "$(printf '117\tReady\t%s\t' "$OLD_TS")"
out="$(bash "$MIRROR" mirror-one 900 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "::error::"; then
  pass "F2: mirror-one on an off-board issue fails (no silent no-op)"
else
  fail "F2: mirror-one off-board did not fail (rc=$rc): $out"
fi

# ---------------------------------------------------------------------------
# 18. G1: off-board `gh issue list` hits the --limit -> detector FAILs (a full
#     page means the list may be truncated; refuse a possibly-partial scan).
# ---------------------------------------------------------------------------
d="$(new_case)"
seed_state "$(printf '118\tReady\t%s\tstatus:ready' "$OLD_TS")"
export ISSUE_LIST_FILE="$d/issuelist.tsv"
# Return exactly BLM_OFFBOARD_LIMIT numbers for status:ready so count == limit.
: >"$ISSUE_LIST_FILE"
printf 'status:ready\t118\n' >>"$ISSUE_LIST_FILE"
printf 'status:ready\t201\n' >>"$ISSUE_LIST_FILE"
export BLM_OFFBOARD_LIMIT=2
out="$(bash "$MIRROR" detect 2>&1)"; rc=$?
unset ISSUE_LIST_FILE BLM_OFFBOARD_LIMIT
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "may be truncated"; then
  pass "G1: off-board list hitting --limit fails the detector (non-truncation assert)"
else
  fail "G1: limit-truncation not caught (rc=$rc): $out"
fi

# ---------------------------------------------------------------------------
# 19. G2: mirror-one on a CLOSED issue (routine triage label touch) -> exit 0, no
#     ::error:: (a benign closed-issue label event must not red the run).
# ---------------------------------------------------------------------------
new_case >/dev/null
seed_state "$(printf '119\tReady\t%s\t' "$OLD_TS")"
export CLOSED_NODES=119
out="$(bash "$MIRROR" mirror-one 119 node-119 2>&1)"; rc=$?
unset CLOSED_NODES
if [ "$rc" -eq 0 ] && ! printf '%s' "$out" | grep -q "::error::"; then
  pass "G2: mirror-one on a closed issue exits 0 with no error"
else
  fail "G2: closed-issue mirror-one not benign (rc=$rc): $out"
fi

# ---------------------------------------------------------------------------
# 20. G2: mirror-one on an OPEN but OFF-BOARD issue (via node id) still FAILs with
#     ::error:: — the real off-board hazard is unchanged by the closed-state fix.
# ---------------------------------------------------------------------------
new_case >/dev/null
# #900 is not in the board state at all -> node lookup returns null -> off-board.
seed_state "$(printf '120\tReady\t%s\t' "$OLD_TS")"
out="$(bash "$MIRROR" mirror-one 900 node-900 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "::error::"; then
  pass "G2: mirror-one on an open off-board issue still fails (::error::)"
else
  fail "G2: open off-board mirror-one should still fail (rc=$rc): $out"
fi

# ---------------------------------------------------------------------------
# 21. G4: many-row board -> mirror reconciles EVERY issue (a child consuming the
#     loop's stdin would silently skip rows; the dedicated-fd read must not).
# ---------------------------------------------------------------------------
new_case >/dev/null
seed_state \
  "$(printf '301\tReady\t%s\t' "$OLD_TS")" \
  "$(printf '302\tReady\t%s\t' "$OLD_TS")" \
  "$(printf '303\tReady\t%s\t' "$OLD_TS")" \
  "$(printf '304\tReady\t%s\t' "$OLD_TS")" \
  "$(printf '305\tReady\t%s\t' "$OLD_TS")"
bash "$MIRROR" mirror >/dev/null 2>&1
if has_label 301 "status:ready" && has_label 302 "status:ready" \
  && has_label 303 "status:ready" && has_label 304 "status:ready" \
  && has_label 305 "status:ready"; then
  pass "G4: mirror reconciles every row of a multi-issue board (no stdin-eaten skips)"
else
  fail "G4: some rows skipped: 301=$(labels_of 301) 302=$(labels_of 302) 303=$(labels_of 303) 304=$(labels_of 304) 305=$(labels_of 305)"
fi

# ---------------------------------------------------------------------------
echo "----"
echo "board-label-mirror: PASS=$PASS_COUNT FAIL=$FAIL_COUNT"
[ "$FAIL_COUNT" -eq 0 ]
