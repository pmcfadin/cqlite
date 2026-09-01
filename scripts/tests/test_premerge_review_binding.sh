#!/usr/bin/env bash
#
# Regression tests for scripts/flow/premerge-review-binding.sh and its wiring
# into scripts/flow/premerge-assert.sh (issue #3752).
#
# WHY A SEPARATE FILE. scripts/tests/test_premerge_assert.sh is already 2163
# lines, over the ~1500 test target, so adding here rather than there is the
# campsite rule (#1135), not a preference.
#
# HERMETIC. Every case builds a real, tiny git repository in scratch and
# PATH-shims the EXTERNAL binaries `gh` and `roborev`. Shimming an external is
# what the sibling suite already does; what is FORBIDDEN is a settable path to
# one of OUR OWN enforcer scripts (#3312: the constrained party must not choose
# its own enforcer), so a case needing different behaviour from a repo script
# SUBSTITUTES THE ARTIFACT in its own scratch copy of scripts/flow.
#
# THE CASE THAT MATTERS MOST is `rebase`: the reviewed commit is rewritten by a
# rebase, is still reflog-reachable so `git cat-file -t` answers `commit`, and
# only `merge-base --is-ancestor` fires. That case is what proves the ordering
# the `lane-3552` correction on #3752 demands.
#
# Run standalone:   bash scripts/tests/test_premerge_review_binding.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BINDING="$REPO_ROOT/flow/premerge-review-binding.sh"
SCANNER="$REPO_ROOT/flow/premerge-pr-scan.py"
ASSERT="$REPO_ROOT/flow/premerge-assert.sh"

PASSED=0
FAILED=0
ok()  { printf 'ok   - %s\n' "$1"; PASSED=$((PASSED + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAILED=$((FAILED + 1)); }

# The scratch dir is validated BEFORE any path is built from it (#3650 B5): an
# unchecked mktemp leaves $T empty and every "$T/..." resolves at the ROOT.
if ! T=$(mktemp -d "${TMPDIR:-/tmp}/premerge-review-binding-test.XXXXXX" 2>/dev/null) ||
  [ -z "$T" ] || [ ! -d "$T" ]; then
  printf 'FAIL - could not create a scratch directory under %s: refusing to run.\n' \
    "${TMPDIR:-/tmp}" >&2
  exit 1
fi
trap 'rm -rf "$T"' EXIT

for tool in git python3; do
  command -v "$tool" >/dev/null 2>&1 || {
    printf 'FAIL - %s is required for this suite and is not on PATH.\n' "$tool" >&2
    exit 1
  }
done

# --- the scratch copy of scripts/flow + scripts/ci ---------------------------
# The binding resolves its scanner, the job-facts parser and the classifier from
# its OWN directory with no override, so the only way to exercise it in scratch
# is to lay the real artifacts out the same way.
FLOW="$T/scripts/flow"
CI="$T/scripts/ci"
mkdir -p "$FLOW" "$CI"
for f in premerge-review-binding.sh premerge-pr-scan.py roborev-job-facts.py \
  premerge-assert.sh base-staleness.sh; do
  cp "$REPO_ROOT/flow/$f" "$FLOW/$f" || {
    printf 'FAIL - could not copy scripts/flow/%s into scratch.\n' "$f" >&2
    exit 1
  }
done
cp "$REPO_ROOT/ci/classify-docs-only.sh" "$CI/classify-docs-only.sh" || {
  printf 'FAIL - could not copy scripts/ci/classify-docs-only.sh into scratch.\n' >&2
  exit 1
}
# An IMMEDIATE advisory stub, so no case scans the ambient checkout.
cat >"$FLOW/base-staleness.sh" <<'ADV'
#!/usr/bin/env bash
printf 'BASE-STALENESS: immediate stub for the #3752 suite\n'
printf 'BASE-STALENESS: verdict NO-STALENESS-RECOGNISED\n'
exit 0
ADV
chmod +x "$FLOW"/*.sh "$FLOW"/*.py "$CI"/*.sh
SB="$FLOW/premerge-review-binding.sh"

# --- shims -------------------------------------------------------------------
# `gh` and `roborev` are EXTERNAL binaries; both are driven by files whose paths
# the case exports, so no case depends on network, auth or a local roborev DB.
BIN="$T/bin"
mkdir -p "$BIN"
cat >"$BIN/gh" <<'GH'
#!/usr/bin/env bash
# MOCK_GH_DIR holds one file per response, named by the call shape.
d="${MOCK_GH_DIR:-}"
case "$1 $2" in
  "pr view")
    case "$*" in
      *closingIssuesReferences*) f="$d/pr-hold.json" ;;
      *) f="$d/pr.json" ;;
    esac ;;
  "issue view") f="$d/issue-$3.json" ;;
  "api "*) f="$d/timeline.json" ;;
  *) f="$d/pr.json" ;;
esac
case "$*" in
  api*) f="$d/timeline.json" ;;
esac
[ -f "$f" ] || { echo "gh: no fixture $f" >&2; exit 1; }
cat "$f"
GH
chmod +x "$BIN/gh"

cat >"$BIN/roborev" <<'RB'
#!/usr/bin/env bash
# MOCK_ROBOREV_DIR holds job-<id>.json payloads; absent => an unretrievable record.
d="${MOCK_ROBOREV_DIR:-}"
case "$1" in
  show) f="$d/job-$2.json" ;;
  list) f="$d/list.json" ;;
  *) exit 1 ;;
esac
[ -f "$f" ] || exit 1
cat "$f"
RB
chmod +x "$BIN/roborev"

# --- fixture builders ---------------------------------------------------------
# roborev_job <id> <base40> <head40> -- a `roborev show` payload of the REAL
# shape: the job row NESTED under a "job" key (measured, issue #2964).
roborev_job() {
  mkdir -p "$MOCK_ROBOREV_DIR"
  python3 - "$MOCK_ROBOREV_DIR/job-$1.json" "$1" "$2" "$3" <<'PY'
import json, sys
out, job, base, head = sys.argv[1:5]
json.dump({"id": int(job), "job_id": int(job), "agent": "codex",
           "job": {"id": int(job), "git_ref": "%s..%s" % (base, head),
                   "status": "done", "model": "gpt-5.6-sol",
                   "token_usage": json.dumps({"input_tokens": 400000,
                                              "cached_input_tokens": 320000,
                                              "total_output_tokens": 5000})}},
          open(out, "w"))
PY
}

# pr_payload <out> <baseRefName> <body> -- a `gh pr view --json` payload.
pr_payload() {
  python3 - "$1" "$2" "$3" <<'PY'
import json, sys
out, base, body = sys.argv[1:4]
json.dump({"baseRefName": base, "body": body, "comments": []}, open(out, "w"))
PY
}

# roborev_block <job> -- the recorded block a closer posts on the PR.
roborev_block() {
  printf '```\n==== ROBOREV REVIEW SUMMARY ====\njob: %s      model: gpt-5.6-sol\nfindings: NONE\n%s\n==== END ROBOREV REVIEW SUMMARY ====\n```\n' \
    "$1" 'RESULT: PASS'
}

# run_binding <want-exit> <desc> <args...>
run_binding() {
  local want="$1" desc="$2"
  shift 2
  OUT=$(cd "$WORK" && PATH="$BIN:$PATH" bash "$SB" "$@" 2>&1)
  RC=$?
  if [ "$RC" -ne "$want" ]; then
    bad "$desc (exit $RC, wanted $want)"
    printf '     output: %s\n' "$OUT"
    return 1
  fi
  return 0
}

# --- the synthetic branch ------------------------------------------------------
# A real repository with a real rebase, because the whole subject is what git
# reports about a rewritten commit. Built ONCE and reused read-only.
WORK="$T/work"
mkdir -p "$WORK"
(
  cd "$WORK" || exit 1
  git init -q -b main .
  git config user.email t@example.com
  git config user.name T
  printf 'base\n' >README.md
  mkdir -p src
  printf 'fn main() {}\n' >src/lib.rs
  git add -A && git commit -qm base
  git branch -q -M main
  git checkout -q -b feature
  printf 'fn a() {}\n' >>src/lib.rs
  git add -A && git commit -qm "feature code"
  REVIEWED=$(git rev-parse HEAD)
  printf '%s\n' "$REVIEWED" >"$T/reviewed-pre-rebase"
  # main advances, then feature rebases -> the reviewed commit is REWRITTEN but
  # stays reflog-reachable, which is the whole point of the `rebase` case.
  git checkout -q main
  printf 'more\n' >>README.md
  git add -A && git commit -qm "main advances"
  git checkout -q feature
  git rebase -q main >/dev/null 2>&1
  git rev-parse HEAD >"$T/head-after-rebase"
) || {
  printf 'FAIL - could not build the synthetic repository.\n' >&2
  exit 1
}
REVIEWED_PRE=$(cat "$T/reviewed-pre-rebase")
HEAD_AFTER=$(cat "$T/head-after-rebase")
MOCK_GH_DIR="$T/gh"
MOCK_ROBOREV_DIR="$T/roborev"
mkdir -p "$MOCK_GH_DIR" "$MOCK_ROBOREV_DIR"
export MOCK_GH_DIR MOCK_ROBOREV_DIR

# NON-VACUITY: the case only proves what it claims if `cat-file -t` really does
# still answer `commit` for the rewritten sha. Assert that, or the headline case
# would pass for the wrong reason.
if [ "$(cd "$WORK" && git cat-file -t "$REVIEWED_PRE" 2>/dev/null)" = "commit" ]; then
  ok "fixture: the rebased-away commit is STILL a valid object (cat-file -t = commit)"
else
  bad "fixture: the rebased-away commit is already unreachable — the headline case would be vacuous"
fi
if (cd "$WORK" && git merge-base --is-ancestor "$REVIEWED_PRE" "$HEAD_AFTER" >/dev/null 2>&1); then
  bad "fixture: the rebased-away commit IS an ancestor — the headline case would be vacuous"
else
  ok "fixture: the rebased-away commit is NOT an ancestor (the only arm that fires)"
fi

# --- Case 1: the issue's own instance ------------------------------------------
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 304)"
roborev_job 304 "$(cd "$WORK" && git rev-parse main~1)" "$REVIEWED_PRE"
if run_binding 4 "rebase: a reviewed head rewritten by a rebase is UNBOUND" \
  review-binding 1 pmcfadin/cqlite "$HEAD_AFTER"; then
  case "$OUT" in
    *"verdict UNBOUND"*) ok "rebase: verdict UNBOUND" ;;
    *) bad "rebase: expected 'verdict UNBOUND' (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"cat-file -t\` reports commit"*)
      ok "rebase: cat-file is reported as a DIAGNOSTIC and still answers commit" ;;
    *) bad "rebase: the cat-file diagnostic did not name the still-valid object (got: $OUT)" ;;
  esac
fi

# --- Case 2: reviewed head EQUALS the certified head ---------------------------
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 400)"
roborev_job 400 "$(cd "$WORK" && git rev-parse main)" "$HEAD_AFTER"
if run_binding 0 "equal: reviewed head == certified head is BOUND" \
  review-binding 1 pmcfadin/cqlite "$HEAD_AFTER"; then
  case "$OUT" in
    *"verdict BOUND"*) ok "equal: verdict BOUND" ;;
    *) bad "equal: expected 'verdict BOUND' (got: $OUT)" ;;
  esac
fi

# --- Cases 3 + 4: an ancestor with prose / with code after it -------------------
(
  cd "$WORK" || exit 1
  git checkout -q -b prose-after feature
  printf 'prose\n' >>README.md
  git add -A && git commit -qm "docs only"
  git rev-parse HEAD >"$T/prose-head"
  git checkout -q -b code-after feature
  printf 'fn b() {}\n' >>src/lib.rs
  git add -A && git commit -qm "more code"
  git rev-parse HEAD >"$T/code-head"
) >/dev/null 2>&1
PROSE_HEAD=$(cat "$T/prose-head")
CODE_HEAD=$(cat "$T/code-head")

pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 401)"
roborev_job 401 "$(cd "$WORK" && git rev-parse main)" "$HEAD_AFTER"
if run_binding 0 "prose-after: only prose after the reviewed head is BOUND" \
  review-binding 1 pmcfadin/cqlite "$PROSE_HEAD"; then
  case "$OUT" in
    *"verdict BOUND"*) ok "prose-after: verdict BOUND" ;;
    *) bad "prose-after: expected 'verdict BOUND' (got: $OUT)" ;;
  esac
fi
if run_binding 4 "code-after: reviewable code after the reviewed head is UNBOUND" \
  review-binding 1 pmcfadin/cqlite "$CODE_HEAD"; then
  case "$OUT" in
    *"verdict UNBOUND"*) ok "code-after: verdict UNBOUND" ;;
    *) bad "code-after: expected 'verdict UNBOUND' (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"self-check   git merge-base --is-ancestor $HEAD_AFTER $CODE_HEAD"*)
      ok "code-after: the self-check is printed with the REAL shas, ancestor test FIRST (AC4)" ;;
    *) bad "code-after: the AC4 self-check was not printed with real shas (got: $OUT)" ;;
  esac
fi

# --- Case 5: a code-bearing PR with NO roborev record --------------------------
pr_payload "$MOCK_GH_DIR/pr.json" main "no review recorded here"
if run_binding 4 "no-record: a code-bearing PR with no recorded job is UNBOUND" \
  review-binding 1 pmcfadin/cqlite "$HEAD_AFTER"; then
  case "$OUT" in
    *"verdict UNBOUND"*) ok "no-record: verdict UNBOUND" ;;
    *) bad "no-record: expected 'verdict UNBOUND' (got: $OUT)" ;;
  esac
fi

# --- Case 6: a CODE-FREE PR diff is a DECLARED exemption -----------------------
(
  cd "$WORK" || exit 1
  git checkout -q -b prose-only main
  printf 'prose only\n' >>README.md
  git add -A && git commit -qm "prose only"
  git rev-parse HEAD >"$T/prose-only-head"
) >/dev/null 2>&1
PROSE_ONLY=$(cat "$T/prose-only-head")
pr_payload "$MOCK_GH_DIR/pr.json" main "no review recorded here"
if run_binding 0 "code-free: a code-free PR diff with no job is NOT-APPLICABLE" \
  review-binding 1 pmcfadin/cqlite "$PROSE_ONLY"; then
  case "$OUT" in
    *"verdict NOT-APPLICABLE"*) ok "code-free: verdict NOT-APPLICABLE" ;;
    *) bad "code-free: expected 'verdict NOT-APPLICABLE' (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"DECLARED exemption, not a silent skip"*)
      ok "code-free: the exemption is DECLARED loudly, not skipped silently" ;;
    *) bad "code-free: the exemption was not declared (got: $OUT)" ;;
  esac
fi

# --- Case 7: an unretrievable job record ---------------------------------------
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 999)"
rm -f "$MOCK_ROBOREV_DIR/job-999.json" "$MOCK_ROBOREV_DIR/list.json"
if run_binding 5 "record-absent: an unretrievable job record is UNMEASURED, never a skip" \
  review-binding 1 pmcfadin/cqlite "$HEAD_AFTER"; then
  case "$OUT" in
    *"verdict UNMEASURED"*) ok "record-absent: verdict UNMEASURED" ;;
    *) bad "record-absent: expected 'verdict UNMEASURED' (got: $OUT)" ;;
  esac
fi

# --- Case 8: a SINGLE-COMMIT job record ----------------------------------------
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 998)"
python3 - "$MOCK_ROBOREV_DIR/job-998.json" "$HEAD_AFTER" <<'PY'
import json, sys
out, head = sys.argv[1:3]
json.dump({"id": 998, "job": {"id": 998, "git_ref": head, "status": "done"}}, open(out, "w"))
PY
if run_binding 5 "single-commit: a single-sha git_ref is UNMEASURED (it certifies ONE commit)" \
  review-binding 1 pmcfadin/cqlite "$HEAD_AFTER"; then
  case "$OUT" in
    *"SINGLE-COMMIT record"*) ok "single-commit: the cause names the single-commit record" ;;
    *) bad "single-commit: the cause did not name it (got: $OUT)" ;;
  esac
fi

# --- Case 9: an unparseable git_ref --------------------------------------------
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 997)"
python3 - "$MOCK_ROBOREV_DIR/job-997.json" <<'PY'
import json, sys
json.dump({"id": 997, "job": {"id": 997, "git_ref": "deadbeef..notahex", "status": "done"}},
          open(sys.argv[1], "w"))
PY
if run_binding 5 "git_ref-unparseable: a non-hex head half is UNMEASURED" \
  review-binding 1 pmcfadin/cqlite "$HEAD_AFTER"; then
  case "$OUT" in
    *"verdict UNMEASURED"*) ok "git_ref-unparseable: verdict UNMEASURED" ;;
    *) bad "git_ref-unparseable: expected UNMEASURED (got: $OUT)" ;;
  esac
fi

# --- Case 10: gh unavailable ----------------------------------------------------
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 400)"
rm -f "$MOCK_GH_DIR/pr.json"
if run_binding 5 "gh-failure: an unreadable PR payload is UNMEASURED, never a binding" \
  review-binding 1 pmcfadin/cqlite "$HEAD_AFTER"; then
  case "$OUT" in
    *"verdict UNMEASURED"*) ok "gh-failure: verdict UNMEASURED" ;;
    *) bad "gh-failure: expected UNMEASURED (got: $OUT)" ;;
  esac
fi

# --- hold-check ------------------------------------------------------------------
iso_ago() { python3 -c 'import sys,datetime
print((datetime.datetime.now(datetime.timezone.utc)
       - datetime.timedelta(seconds=int(sys.argv[1]))).strftime("%Y-%m-%dT%H:%M:%SZ"))' "$1"; }

hold_payload() { # hold_payload <out> <json-array-of-comments>
  python3 - "$1" "$2" <<'PY'
import json, sys
out, comments = sys.argv[1:3]
json.dump({"body": "", "comments": json.loads(comments),
           "closingIssuesReferences": []}, open(out, "w"))
PY
}
timeline_payload() { printf '%s\n' "$1" >"$MOCK_GH_DIR/timeline.json"; }

run_hold() {
  local want="$1" desc="$2"
  OUT=$(cd "$WORK" && PATH="$BIN:$PATH" bash "$SB" hold-check 1 pmcfadin/cqlite 2>&1)
  RC=$?
  if [ "$RC" -ne "$want" ]; then
    bad "$desc (exit $RC, wanted $want)"
    printf '     output: %s\n' "$OUT"
    return 1
  fi
  return 0
}

timeline_payload '[]'
hold_payload "$MOCK_GH_DIR/pr-hold.json" '[]'
if run_hold 0 "hold: an empty thread is NO-HOLD-RECOGNISED"; then
  case "$OUT" in
    *"verdict NO-HOLD-RECOGNISED"*) ok "hold: verdict NO-HOLD-RECOGNISED on an empty thread" ;;
    *) bad "hold: expected NO-HOLD-RECOGNISED (got: $OUT)" ;;
  esac
fi

HOLD_AT=$(iso_ago 600)
hold_payload "$MOCK_GH_DIR/pr-hold.json" \
  "[{\"author\":{\"login\":\"pmcfadin\"},\"createdAt\":\"$HOLD_AT\",\"body\":\"HOLD: merge after #9999\"}]"
if run_hold 4 "hold: a column-zero HOLD: with no newer release is HOLD-FOUND"; then
  case "$OUT" in
    *"verdict HOLD-FOUND"*) ok "hold: verdict HOLD-FOUND" ;;
    *) bad "hold: expected HOLD-FOUND (got: $OUT)" ;;
  esac
fi

# An INDENTED / quoted copy is inert (#3312's column-zero anchoring rule).
hold_payload "$MOCK_GH_DIR/pr-hold.json" \
  "[{\"author\":{\"login\":\"pmcfadin\"},\"createdAt\":\"$HOLD_AT\",\"body\":\"> HOLD: quoted example\\n    HOLD: indented example\"}]"
if run_hold 0 "hold: a quoted/indented HOLD: copy is INERT (column-zero anchoring)"; then
  case "$OUT" in
    *"verdict NO-HOLD-RECOGNISED"*) ok "hold: a non-column-zero marker does not stop a merge" ;;
    *) bad "hold: a quoted marker was treated as control (got: $OUT)" ;;
  esac
fi

GO_AT=$(iso_ago 60)
hold_payload "$MOCK_GH_DIR/pr-hold.json" \
  "[{\"author\":{\"login\":\"pmcfadin\"},\"createdAt\":\"$HOLD_AT\",\"body\":\"HOLD: wait\"},{\"author\":{\"login\":\"pmcfadin\"},\"createdAt\":\"$GO_AT\",\"body\":\"GO: cleared\"}]"
if run_hold 0 "hold: a HOLD followed by a newer authorized GO is cleared"; then
  case "$OUT" in
    *"verdict NO-HOLD-RECOGNISED"*) ok "hold: latest-of-{HOLD,GO} decides, so a hold does not block forever" ;;
    *) bad "hold: the newer GO did not clear the hold (got: $OUT)" ;;
  esac
fi

# A release from a NON-allowlisted author is ignored: releasing is the PERMISSIVE
# direction, so it is honoured only from the hard-coded allowlist.
hold_payload "$MOCK_GH_DIR/pr-hold.json" \
  "[{\"author\":{\"login\":\"pmcfadin\"},\"createdAt\":\"$HOLD_AT\",\"body\":\"HOLD: wait\"},{\"author\":{\"login\":\"a-worker\"},\"createdAt\":\"$GO_AT\",\"body\":\"GO: I am done\"}]"
if run_hold 4 "hold: a GO from a non-allowlisted author does NOT clear the hold"; then
  case "$OUT" in
    *"IGNORED"*) ok "hold: the ignored release is REPORTED, not silently dropped" ;;
    *) bad "hold: the ignored release was not reported (got: $OUT)" ;;
  esac
fi

# A lead disarm inside / outside the 30-minute window.
hold_payload "$MOCK_GH_DIR/pr-hold.json" '[]'
timeline_payload "[{\"event\":\"auto_merge_disabled\",\"created_at\":\"$(iso_ago 300)\",\"actor\":{\"login\":\"pmcfadin\"}}]"
if run_hold 4 "disarm: an auto_merge_disabled 5 minutes ago is a stop order"; then
  case "$OUT" in
    *"verdict HOLD-FOUND"*) ok "disarm: a disarm inside the window is HOLD-FOUND" ;;
    *) bad "disarm: expected HOLD-FOUND (got: $OUT)" ;;
  esac
fi
timeline_payload "[{\"event\":\"auto_merge_disabled\",\"created_at\":\"$(iso_ago 5400)\",\"actor\":{\"login\":\"pmcfadin\"}}]"
if run_hold 0 "disarm: an auto_merge_disabled 90 minutes ago is outside the window"; then
  case "$OUT" in
    *"verdict NO-HOLD-RECOGNISED"*) ok "disarm: a disarm older than the window does not stop a merge" ;;
    *) bad "disarm: an old disarm still blocked (got: $OUT)" ;;
  esac
fi

# An unreadable thread is a HOLD, never a clearance.
timeline_payload '[]'
rm -f "$MOCK_GH_DIR/pr-hold.json"
if run_hold 5 "hold: an unreadable PR thread is UNMEASURED"; then
  case "$OUT" in
    *"verdict UNMEASURED"*) ok "hold: an unreadable thread is UNMEASURED, read as a refusal" ;;
    *) bad "hold: expected UNMEASURED (got: $OUT)" ;;
  esac
fi

# --- the window is a committed constant with NO env override --------------------
if grep -qE '^PREMERGE_DISARM_WINDOW_SECS=1800$' "$BINDING"; then
  ok "window: 1800s is a NAMED COMMITTED CONSTANT"
else
  bad "window: the disarm window is not a committed constant named PREMERGE_DISARM_WINDOW_SECS=1800"
fi
if grep -vE '^[[:space:]]*#' "$BINDING" | grep -qE 'PREMERGE_DISARM_WINDOW_SECS=\$\{|:-.*DISARM'; then
  bad "window: the disarm window has an env override — the constrained party must not set it (#3312)"
else
  ok "window: the disarm window has NO env override (#3312)"
fi

# --- the output anchor (#3650 D2, reused) ----------------------------------------
grep -v '^[[:space:]]*#' "$BINDING" >"$T/binding-code.txt"
code_lines=$(grep -c . "$T/binding-code.txt" | tr -d ' ')
all_lines=$(grep -c . "$BINDING" | tr -d ' ')
if [ "$code_lines" -lt "$all_lines" ] && [ "$code_lines" -gt 60 ] &&
  grep -q 'verdict UNMEASURED' "$T/binding-code.txt" &&
  grep -q 'self-check' "$T/binding-code.txt"; then
  ok "template: the comment-stripped source ($code_lines of $all_lines lines) still holds the templates"
else
  bad "template: the comment strip left no usable template text ($code_lines of $all_lines) — vacuous"
fi
tmpl_bad=0
for tok in PASS OK 'RESULT:'; do
  if grep -q -- "$tok" "$T/binding-code.txt"; then
    bad "template: the script's own static text contains '$tok': $(grep -m1 -- "$tok" "$T/binding-code.txt")"
    tmpl_bad=1
  fi
done
[ "$tmpl_bad" -eq 0 ] &&
  ok "template: the script's STATIC text carries none of PASS, OK, RESULT: (structural)"

# Every emitted line carries the anchor prefix.
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 304)"
roborev_job 304 "$(cd "$WORK" && git rev-parse main~1)" "$REVIEWED_PRE"
OUT=$(cd "$WORK" && PATH="$BIN:$PATH" bash "$SB" review-binding 1 pmcfadin/cqlite "$HEAD_AFTER" 2>&1)
unanchored=$(printf '%s\n' "$OUT" | grep -cv '^PREMERGE: REVIEW-BINDING ')
if [ "$unanchored" -eq 0 ]; then
  ok "anchor: every emitted line begins with the leg's prefix"
else
  bad "anchor: $unanchored line(s) carry no PREMERGE: REVIEW-BINDING prefix"
fi

# --- no test-only seam into our own enforcer scripts (#3312) ----------------------
if grep -vE '^[[:space:]]*#' "$BINDING" | grep -qE '\$\{[A-Z_]*(SCAN|FACTS|CLASSIFY)[A-Z_]*:?-'; then
  bad "seam: the binding resolves one of its own enforcers through an overridable variable (#3312)"
else
  ok "seam: every own-enforcer path is resolved from OWN_DIR with no override (#3312)"
fi

# --- premerge-assert wiring --------------------------------------------------------
# The binding must be REACHED by the shipped assert, and its refusal must refuse
# the merge. A leg nothing calls is a guard that never fires.
if grep -q 'premerge-review-binding.sh' "$ASSERT"; then
  ok "wiring: premerge-assert.sh names the review-binding helper"
else
  bad "wiring: premerge-assert.sh does not invoke premerge-review-binding.sh"
fi

# --- the scanner's own contract -----------------------------------------------------
scan_pr="$T/scan-pr.json"
python3 - "$scan_pr" <<'PY'
import json, sys
json.dump({"body": "==== ROBOREV REVIEW SUMMARY ====\njob: 304\nRESULT: PASS\n==== END ROBOREV REVIEW SUMMARY ====",
           "comments": [{"body": "job: 999 outside any block"}]}, open(sys.argv[1], "w"))
PY
scan_out=$(python3 "$SCANNER" jobs "$scan_pr" 2>&1)
if printf '%s\n' "$scan_out" | grep -qx 'job=304' &&
  ! printf '%s\n' "$scan_out" | grep -qx 'job=999'; then
  ok "scanner: a job id is read ONLY from inside a roborev block, never from loose prose"
else
  bad "scanner: block scoping is wrong (got: $scan_out)"
fi
if printf '%s\n' "$scan_out" | grep -q '^recorded-verdict=304:'; then
  ok "scanner: the recorded terminal verdict is reported informationally"
else
  bad "scanner: the recorded terminal verdict was not reported (got: $scan_out)"
fi

# --- CASE FLOOR (#3544) ---------------------------------------------------------------
# A span-replacing edit that silently deletes cases leaves a GREEN tally over a
# SHRUNKEN suite. The floor is what makes that a red.
CASE_FLOOR=34
TOTAL=$((PASSED + FAILED))
if [ "$TOTAL" -lt "$CASE_FLOOR" ]; then
  bad "case floor: only $TOTAL assertions ran, below the committed floor of $CASE_FLOOR — cases were deleted"
  FAILED=$((FAILED + 1))
else
  ok "case floor: $TOTAL assertions ran, at or above the committed floor of $CASE_FLOOR"
fi

printf '\n=== premerge-review-binding: %d passed, %d failed ===\n' "$PASSED" "$FAILED"
[ "$FAILED" -eq 0 ]
