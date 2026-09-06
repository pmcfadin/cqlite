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

# THREE-VALUED FILE PROBES (#3752, lane-3752 audit). Every structural assert
# below reads a file, and "matched" / "did not match" / "could not be read" are
# three facts a `grep -q` inside an `if/else` reduces to two — folding the third
# onto whichever branch was written second, which under the ABSENCE asserts here
# is the PERMISSIVE one. The helpers also remove a MEASURED false FAIL: the
# `--paginate` assert below reddened once and then passed three times over a
# byte-identical file, because this suite runs under `set -o pipefail` and a
# `producer | grep -q` pipeline reports the producer's SIGPIPE when `grep -q`
# matches and exits before the producer has finished writing. See the library
# header for the full account.
. "$SCRIPT_DIR/lib/tristate-file-probe.bash"

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
# roborev-waiver-scan.py and roborev-review-oracles.sh joined this list with
# roborev job 59 finding 1: the leg re-verifies a findings DEFERRAL through the
# same scanner the wrapper uses, and reads the author allowlist from the one
# committed definition — both resolved from its OWN directory with no override,
# so scratch must lay them out identically or the deferral path cannot run at
# all. An ABSENT enforcer makes `deferral_authorized` refuse, which looks
# exactly like "no authorization was posted", so omitting them here would have
# made the authorized-deferral case pass for the wrong reason.
for f in premerge-review-binding.sh premerge-pr-scan.py roborev-job-facts.py \
  roborev-waiver-scan.py roborev-review-oracles.sh \
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
# THE SHARED FINDINGS-COUNT RECOGNISER (#4050) is resolved from the script's OWN `lib/`
# directory with no override, so scratch must lay it out identically or the leg refuses
# UNMEASURED on every case — a setup failure that would look like the leg's own refusal.
mkdir -p "$FLOW/lib"
cp "$REPO_ROOT/flow/lib/roborev-findings-count.sh" "$FLOW/lib/" || {
  printf 'FAIL - could not copy scripts/flow/lib/roborev-findings-count.sh into scratch.\n' >&2
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
#
# SINCE roborev JOB 59 FINDING 2 the leg reads a thread in TWO calls per
# subject: the VIEW (`pr view`/`issue view --json <fields>`, WITHOUT comments,
# because that connection is BOUNDED and drops a stop order on a later page)
# and then the COMPLETE comment list from
# `gh api --paginate repos/<r>/issues/<n>/comments`.
#
# THE COMMENT ENDPOINT IS DERIVED, NOT A SECOND FIXTURE. It re-serves the
# `comments` array of the fixture this mock answered for the IMMEDIATELY
# PRECEDING view call — which is exactly the leg's order, view then comments,
# per subject. So a case still writes ONE payload with its comments inline and
# needs no companion file and no wiring: every pre-existing case stays
# byte-identical, and a case added later cannot forget to split its comments
# out. A case wanting a genuinely MULTI-PAGE stream writes that stream to
# `$MOCK_GH_DIR/comment-pages.json` and it is served verbatim.
d="${MOCK_GH_DIR:-}"
f=""
case "$*" in
  *"/timeline"*) f="$d/timeline.json" ;;
  *"/comments"*)
    if [ -f "$d/comment-pages.json" ]; then
      cat "$d/comment-pages.json"
      exit 0
    fi
    last=""
    [ -f "$d/.last-view" ] && last=$(cat "$d/.last-view")
    python3 -c 'import json,sys
try:
    with open(sys.argv[1]) as h:
        p = json.load(h)
except Exception:
    p = {}
c = p.get("comments") if isinstance(p, dict) else None
sys.stdout.write(json.dumps(c if isinstance(c, list) else []))' "$last"
    exit $?
    ;;
esac
if [ -z "$f" ]; then
  case "$1 $2" in
    "pr view")
      case "$*" in
        *closingIssuesReferences*) f="$d/pr-hold.json" ;;
        *) f="$d/pr.json" ;;
      esac ;;
    "issue view") f="$d/issue-$3.json" ;;
    *) f="$d/pr.json" ;;
  esac
  printf '%s' "$f" >"$d/.last-view"
fi
[ -f "$f" ] || { echo "gh: no fixture $f" >&2; exit 1; }
# An `!ABSENT` fixture reproduces GitHub'"'"'s OWN not-found diagnostic. That text is the
# only thing separating ISSUE-ABSENT from a could-not-ask, because `gh issue view`
# exits 1 for BOTH — the two-valued predicate the oracle exists to refuse.
if [ "$(head -c 7 "$f" 2>/dev/null)" = "!ABSENT" ]; then
  echo "GraphQL: Could not resolve to an issue or pull request with the number of $3." >&2
  exit 1
fi
cat "$f"
GH
chmod +x "$BIN/gh"

# issue_state_fixture <n> <state|!ABSENT> — the gh mock `cat`s fixtures verbatim and
# the oracle asks with `--jq '"\(.number) \(.state)"'`, so the fixture holds that
# rendered line rather than a JSON object. Omitting a fixture entirely is a THIRD
# case (an unrecognised diagnostic => could-not-ask), used deliberately below.
issue_state_fixture() {
  if [ "$2" = "!ABSENT" ]; then
    printf '!ABSENT\n' >"$MOCK_GH_DIR/issue-$1.json"
  else
    printf '%s %s' "$1" "$2" >"$MOCK_GH_DIR/issue-$1.json"
  fi
}

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
# The 4th argument is the RECORD's structured verdict letter, `P` clean / `F`
# findings, as `roborev show --json` synthesises it from `reviews.verdict_bool`.
# It DEFAULTS to `P` because that is the shape of a real clean record, and since
# roborev job 59 finding 1 the binding decision READS it: a record with no
# readable verdict no longer binds, so a fixture omitting it would be asserting
# the unknown-verdict path by accident. Pass `-` for a record carrying NO
# verdict field at all.
# roborev_job <id> <base> <head> [<verdict>] [<status>] [<started_at>] [<review-text>]
#
# <verdict>    P clean / F findings / - omit the field entirely
# <status>     the JOB's terminal state; `done` is the observed success token
#              (measured on live records 59 and 78 on this box). `-` omits it.
# <started_at> the F2 chronology key. `-` omits it. Defaults are chosen so every
#              pre-F2 caller keeps its meaning: one covering record, terminal and
#              stamped, i.e. unambiguously the latest.
# <review-text> the RECORD'S OWN REVIEW OUTPUT, on the review row as `output` — the
#              real shape, measured on this box's jobs 120/116/115 (834/789/835
#              bytes, retrievable days after the review). Since #4050 the merge
#              point derives the findings count from it, so a fixture that omits it
#              is asserting the "no count could be DERIVED" path. It DEFAULTS TO
#              ABSENT deliberately: that keeps every pre-#4050 caller's meaning
#              (they all exercise the UNMEASURED path), so a case wanting a
#              countable record has to say so. `\n` escapes are rendered.
roborev_job() {
  mkdir -p "$MOCK_ROBOREV_DIR"
  python3 - "$MOCK_ROBOREV_DIR/job-$1.json" "$1" "$2" "$3" "${4:-P}" \
    "${5:-done}" "${6:-2026-09-02T10:00:00Z}" "${7:-}" <<'PYJOB'
import json, sys
out, job, base, head, verdict, status, started, review = sys.argv[1:9]
row = {"id": int(job), "git_ref": "%s..%s" % (base, head),
       "model": "gpt-5.6-sol",
       "token_usage": json.dumps({"input_tokens": 400000,
                                  "cached_input_tokens": 320000,
                                  "total_output_tokens": 5000})}
if verdict != "-":
    row["verdict"] = verdict
if status != "-":
    row["status"] = status
if started != "-":
    row["started_at"] = started
payload = {"id": int(job), "job_id": int(job), "agent": "codex", "job": row}
if review != "":
    payload["output"] = review.replace("\\n", "\n")
json.dump(payload, open(out, "w"))
PYJOB
}

# findings_review_text <n> — a review transcript reporting exactly <n> findings, in the
# shape the shared recogniser counts (a `## Findings` heading, one `**Severity**:` marker
# per finding, a line-initial `## Summary` terminator). Built here rather than hard-coded
# so a case says how many findings it means and the count is not a magic string.
findings_review_text() {
  local n="$1" i=1 out="## Findings"
  while [ "$i" -le "$n" ]; do
    out="$out\n- **Severity**: Medium\n  Problem: finding number $i.\n  Fix: address it."
    i=$((i + 1))
  done
  printf '%s\n## Summary\n%s finding(s) reported.' "$out" "$n"
}

# defer_marker <issues> <count> <base> <head> <job> <reason> — the authorization
# marker, which grants only as the SOLE nonblank content of a top-level comment.
defer_marker() {
  printf 'roborev-defer: findings issues=%s count=%s base=%s head=%s job=%s reason=%s' \
    "$1" "$2" "$3" "$4" "$5" "$6"
}

# pr_payload_with_comment <out> <baseRefName> <body> <author> <comment-body>
pr_payload_with_comment() {
  python3 - "$1" "$2" "$3" "$4" "$5" <<'PY'
import json, sys
out, base, body, author, comment = sys.argv[1:6]
json.dump({"baseRefName": base, "body": body,
           "comments": [{"author": {"login": author}, "body": comment,
                         "createdAt": "2026-09-02T00:00:00Z",
                         "updatedAt": "2026-09-02T00:00:00Z"}]},
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
assert_src_present \
  "window: 1800s is a NAMED COMMITTED CONSTANT" \
  "window: the disarm window is not a committed constant named PREMERGE_DISARM_WINDOW_SECS=1800" \
  "$BINDING" '^PREMERGE_DISARM_WINDOW_SECS=1800$'
assert_src_absent \
  "window: the disarm window has NO env override (#3312)" \
  "window: the disarm window has an env override — the constrained party must not set it (#3312)" \
  "$BINDING" 'PREMERGE_DISARM_WINDOW_SECS=\$\{|:-.*DISARM' code

# --- the output anchor (#3650 D2, reused) ----------------------------------------
# The DERIVATION is measured too: a failed comment strip used to leave an empty
# file, and every probe below would then have measured the derivation instead of
# the subject — the token loop reporting "no forbidden token" about a file that
# holds nothing at all, which is the vacuous pass this block exists to prevent.
tmpl_bad=0
if ! probe_write_code_lines "$BINDING" "$T/binding-code.txt"; then
  bad "template: UNMEASURED — could not derive the comment-stripped source ($PROBE_WHY)"
  tmpl_bad=1
else
  code_lines=$(probe_count "$T/binding-code.txt" nonblank) || code_lines=-1
  all_lines=$(probe_count "$BINDING" nonblank) || all_lines=-1
  if [ "$code_lines" -lt 0 ] || [ "$all_lines" -lt 0 ]; then
    bad "template: UNMEASURED — the line census could not be taken ($PROBE_WHY)"
    tmpl_bad=1
  else
    probe_file_fixed "$T/binding-code.txt" 'verdict UNMEASURED'
    have_verdict=$?
    probe_file_fixed "$T/binding-code.txt" 'self-check'
    have_selfcheck=$?
    if [ "$have_verdict" -eq 2 ] || [ "$have_selfcheck" -eq 2 ]; then
      bad "template: UNMEASURED — the stripped source could not be re-read ($PROBE_WHY)"
      tmpl_bad=1
    elif [ "$code_lines" -lt "$all_lines" ] && [ "$code_lines" -gt 60 ] &&
      [ "$have_verdict" -eq 0 ] && [ "$have_selfcheck" -eq 0 ]; then
      ok "template: the comment-stripped source ($code_lines of $all_lines lines) still holds the templates"
    else
      bad "template: the comment strip left no usable template text ($code_lines of $all_lines) — vacuous"
      tmpl_bad=1
    fi
  fi
  for tok in PASS OK 'RESULT:'; do
    probe_file_fixed "$T/binding-code.txt" "$tok"
    case "$?" in
      0)
        bad "template: the script's own static text contains '$tok'"
        tmpl_bad=1
        ;;
      1) : ;;
      *)
        bad "template: UNMEASURED — could not scan the stripped source for '$tok' ($PROBE_WHY)"
        tmpl_bad=1
        ;;
    esac
  done
fi
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

# The HOLD leg's output is anchored too — the anchor is a property of BOTH legs,
# and asserting it on one only would leave the other's unpinned.
hold_payload "$MOCK_GH_DIR/pr-hold.json" \
  "[{\"author\":{\"login\":\"pmcfadin\"},\"createdAt\":\"$HOLD_AT\",\"body\":\"HOLD: stop\"}]"
printf '[]\n' >"$MOCK_GH_DIR/timeline.json"
HOUT=$(cd "$WORK" && PATH="$BIN:$PATH" bash "$SB" hold-check 1 pmcfadin/cqlite 2>&1)
hold_unanchored=$(printf '%s\n' "$HOUT" | grep -cv '^PREMERGE: HOLD-CHECK ')
if [ "$hold_unanchored" -eq 0 ]; then
  ok "anchor: every hold-check line begins with the leg's prefix"
else
  bad "anchor: $hold_unanchored hold-check line(s) carry no PREMERGE: HOLD-CHECK prefix"
fi

# An UNCLASSIFIABLE range after an ancestor review is a MEASUREMENT failure, not
# an absence of coverage: the two need different operator actions, so they must
# not collapse onto one verdict. Forced by substituting an ALWAYS-FAILING
# classifier artifact in a scratch copy of the tree (never a path variable).
UNCLS="$T/uncls"
mkdir -p "$UNCLS/scripts/flow" "$UNCLS/scripts/ci"
cp "$FLOW"/*.sh "$FLOW"/*.py "$UNCLS/scripts/flow/" 2>/dev/null
mkdir -p "$UNCLS/scripts/flow/lib"
cp "$FLOW/lib/roborev-findings-count.sh" "$UNCLS/scripts/flow/lib/" 2>/dev/null
# The stub must fail ONLY on the SECOND call. An always-failing classifier is
# consumed by the leg's FIRST use — the PR-diff code-free check — so the case
# would pass on a different cause than the one it names, which is a test green
# for the wrong reason. A counter targets the post-review call exactly.
cat >"$UNCLS/scripts/ci/classify-docs-only.sh" <<'CLS'
#!/usr/bin/env bash
cat >/dev/null
n=$(cat "$UNCLS_COUNT" 2>/dev/null || echo 0)
n=$((n + 1))
printf '%s' "$n" >"$UNCLS_COUNT"
[ "$n" -ge 2 ] && exit 7
exit 1
CLS
chmod +x "$UNCLS/scripts/flow"/*.sh "$UNCLS/scripts/ci"/*.sh
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 401)"
roborev_job 401 "$(cd "$WORK" && git rev-parse main)" "$HEAD_AFTER"
UNCLS_COUNT="$T/uncls-count"
rm -f "$UNCLS_COUNT"
UOUT=$(cd "$WORK" && PATH="$BIN:$PATH" UNCLS_COUNT="$UNCLS_COUNT" \
  bash "$UNCLS/scripts/flow/premerge-review-binding.sh" \
  review-binding 1 pmcfadin/cqlite "$CODE_HEAD" 2>&1)
URC=$?
if [ "$URC" -eq 5 ]; then
  ok "unclassifiable: an unclassifiable post-review range is UNMEASURED, not UNBOUND"
else
  bad "unclassifiable: expected exit 5, got $URC: $UOUT"
fi
# NON-VACUITY: it must be the POST-REVIEW range that could not be classified,
# not the PR diff — the two causes are textually distinct on purpose.
case "$UOUT" in
  *"a recorded round IS an ancestor of the certified head, but the range after it"*)
    ok "unclassifiable: the cause names the POST-REVIEW range, not the PR diff" ;;
  *) bad "unclassifiable: the verdict fired on the wrong cause (got: $UOUT)" ;;
esac

# --- no test-only seam into our own enforcer scripts (#3312) ----------------------
assert_src_absent \
  "seam: every own-enforcer path is resolved from OWN_DIR with no override (#3312)" \
  "seam: the binding resolves one of its own enforcers through an overridable variable (#3312)" \
  "$BINDING" '\$\{[A-Z_]*(SCAN|FACTS|CLASSIFY)[A-Z_]*:?-' code

# --- premerge-assert wiring --------------------------------------------------------
# A leg nothing calls is a guard that never fires, so the wiring is asserted
# BEHAVIOURALLY (the shipped assert really refuses on an unbound review), not
# just by grepping for the helper's name.
assert_src_present_fixed \
  "wiring: premerge-assert.sh names the review-binding helper" \
  "wiring: premerge-assert.sh does not invoke premerge-review-binding.sh" \
  "$ASSERT" 'premerge-review-binding.sh'
assert_src_present_fixed \
  "wiring: premerge-assert.sh invokes the hold-check leg too" \
  "wiring: premerge-assert.sh does not invoke the hold-check leg" \
  "$ASSERT" 'hold-check'

# A minimal but REAL gate-of-record block at the certified sha, so the assert
# gets past its own offline gate check and reaches the #3752 legs.
gate_block() { # gate_block <out> <sha>
  {
    printf '==== AGENT-GATE SUMMARY ====\n'
    printf 'run-id: /tmp/agent-gate.test3752\n'
    printf 'commit: %.7s branch: issue-3752 dirty: no\n' "$2"
    printf 'tree-start: %.12s dirty: no digest: 671a6275687c\n' "$2"
    printf 'tree-end: %.12s dirty: no digest: 671a6275687c\n' "$2"
    printf 'tree-integrity: PASS\n'
    printf 'file-size:         PASS (0s)\n'
    printf 'logs: /tmp/agent-gate.test3752\n'
    printf 'RESULT: PASS\n'
    printf '==== END AGENT-GATE SUMMARY ====\n'
  } >"$1"
}

# The gh mock must also answer the assert's own head/state call.
#
# THIS IS THE SECOND `$BIN/gh` IN THIS FILE AND IT REPLACES THE FIRST FOR EVERY
# CASE BELOW IT. That is a real trap and it cost a debugging round: this mock
# routed EVERY `api` call to the timeline fixture (`case "$1" in api)`), so when
# the leg gained its paginated COMMENT fetch (roborev job 59 finding 2) every
# case after this point silently received the TIMELINE payload as its comment
# thread — an empty comment list — and five cases failed with correct code.
# Both mocks must therefore agree on the endpoint routing; the comment endpoint
# is matched on the ENDPOINT PATH, never on `$1` alone.
cat >"$BIN/gh" <<'GH2'
#!/usr/bin/env bash
d="${MOCK_GH_DIR:-}"
# Endpoint-path routing FIRST, and `/comments` is derived from the fixture this
# mock served for the immediately preceding VIEW call — the same contract as the
# first mock above.
case "$*" in
  *"/timeline"*)
    f="$d/timeline.json"
    [ -f "$f" ] || { echo "gh: no fixture $f" >&2; exit 1; }
    cat "$f"
    exit 0
    ;;
  *"/comments"*)
    if [ -f "$d/comment-pages.json" ]; then
      cat "$d/comment-pages.json"
      exit 0
    fi
    last=""
    [ -f "$d/.last-view" ] && last=$(cat "$d/.last-view")
    python3 -c 'import json,sys
try:
    with open(sys.argv[1]) as h:
        p = json.load(h)
except Exception:
    p = {}
c = p.get("comments") if isinstance(p, dict) else None
sys.stdout.write(json.dumps(c if isinstance(c, list) else []))' "$last"
    exit $?
    ;;
esac
case "$*" in
  *closingIssuesReferences*) f="$d/pr-hold.json" ;;
  *baseRefName*) f="$d/pr.json" ;;
  *) f="" ;;
esac
case "$1 $2" in "issue view") f="$d/issue-$3.json" ;; esac
if [ -z "$f" ]; then
  printf '%s
' "${MOCK_GH_OUT:-}"
  exit 0
fi
[ -f "$f" ] || { echo "gh: no fixture $f" >&2; exit 1; }
# Same `!ABSENT` support as the first mock. THIS mock replaces that one for every
# case below its heredoc, so a capability added only there is invisible here —
# the trap that cost a debugging round already.
if [ "$(head -c 7 "$f" 2>/dev/null)" = "!ABSENT" ]; then
  echo "GraphQL: Could not resolve to an issue or pull request with the number of $3." >&2
  exit 1
fi
printf '%s' "$f" >"$d/.last-view"
cat "$f"
GH2
chmod +x "$BIN/gh"

E2E_GATE="$T/e2e-gate.txt"
gate_block "$E2E_GATE" "$HEAD_AFTER"
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 304)"
roborev_job 304 "$(cd "$WORK" && git rev-parse main~1)" "$REVIEWED_PRE"
hold_payload "$MOCK_GH_DIR/pr-hold.json" '[]'
printf '[]\n' >"$MOCK_GH_DIR/timeline.json"
E2E=$(cd "$WORK" && PATH="$BIN:$PATH" MOCK_GH_OUT="$HEAD_AFTER OPEN" \
  bash "$FLOW/premerge-assert.sh" 1 "$HEAD_AFTER" "$E2E_GATE" 2>&1)
E2E_RC=$?
if [ "$E2E_RC" -eq 2 ]; then
  ok "wiring: the shipped assert REFUSES (exit 2) when the recorded review was rebased away"
else
  bad "wiring: the shipped assert did not refuse an unbound review (exit $E2E_RC): $E2E"
fi
case "$E2E" in
  *"PREMERGE: REVIEW-UNBOUND — REFUSING TO MERGE"*)
    ok "wiring: the refusal carries its own distinct marker" ;;
  *) bad "wiring: the refusal did not carry the REVIEW-UNBOUND marker (got: $E2E)" ;;
esac
# THE REPORT MUST TRAVEL WITH THE REFUSAL, AND THE EXIT CODE MUST BE A DECISION.
# The first wiring wrapped the leg in a command substitution. Two consequences,
# and they are recorded with DIFFERENT evidence because they have different
# evidence — the honest distinction matters more than a uniform claim:
#   * REPRODUCED RED: the anchored report was SWALLOWED by that substitution
#     while the refusal block said it was "on stdout above" (measured against
#     the pre-fix artifact: 0 report lines reached the caller).
#   * NOT reproduced, pinned DEFENSIVELY: the exit code. Under `set -e` a bare
#     `x=$(cmd)` terminates AT THE ASSIGNMENT, so the `case` that translates 4/5
#     into the documented 2 never ran; the observed code was 2 anyway, by an
#     accident of how `set -e` unwound the subshell. It was right for the wrong
#     reason, which is exactly the kind of thing that changes under a refactor.
case "$E2E" in
  *"PREMERGE: REVIEW-BINDING verdict UNBOUND"*)
    ok "wiring: the leg's anchored report travels WITH the refusal, not swallowed" ;;
  *) bad "wiring: the refusal discarded the report it says is above (got: $E2E)" ;;
esac
# The SAME must hold for an UNMEASURED leg, whose raw exit is 5: it must still
# arrive as the documented refusal (2), not as a bare 5 from an unwound subshell.
rm -f "$MOCK_ROBOREV_DIR/job-304.json" "$MOCK_ROBOREV_DIR/list.json"
E2E5=$(cd "$WORK" && PATH="$BIN:$PATH" MOCK_GH_OUT="$HEAD_AFTER OPEN" \
  bash "$FLOW/premerge-assert.sh" 1 "$HEAD_AFTER" "$E2E_GATE" 2>&1)
E2E5_RC=$?
if [ "$E2E5_RC" -eq 2 ]; then
  ok "wiring: an UNMEASURED leg (raw exit 5) arrives as the documented refusal exit 2"
else
  bad "wiring: an UNMEASURED leg leaked its raw exit ($E2E5_RC) instead of refusing with 2: $E2E5"
fi
case "$E2E5" in
  *"PREMERGE: REVIEW-BINDING verdict UNMEASURED"*)
    ok "wiring: the UNMEASURED report also travels with its refusal" ;;
  *) bad "wiring: the UNMEASURED report was swallowed (got: $E2E5)" ;;
esac
roborev_job 304 "$(cd "$WORK" && git rev-parse main~1)" "$REVIEWED_PRE"

# And the success path: a bound review reaches PREMERGE: OK with both reports.
roborev_job 304 "$(cd "$WORK" && git rev-parse main)" "$HEAD_AFTER"
E2E_OK=$(cd "$WORK" && PATH="$BIN:$PATH" MOCK_GH_OUT="$HEAD_AFTER OPEN" \
  bash "$FLOW/premerge-assert.sh" 1 "$HEAD_AFTER" "$E2E_GATE" 2>&1)
E2E_OK_RC=$?
if [ "$E2E_OK_RC" -eq 0 ]; then
  ok "wiring: a bound review + a clear thread still reaches exit 0"
else
  bad "wiring: a bound review did not reach exit 0 (exit $E2E_OK_RC): $E2E_OK"
fi
case "$E2E_OK" in
  *"PREMERGE: REVIEW-BINDING verdict BOUND"*"PREMERGE: HOLD-CHECK verdict NO-HOLD-RECOGNISED"*)
    ok "wiring: BOTH legs' anchored reports travel to the merge point, in order" ;;
  *) bad "wiring: the legs' reports did not both reach the success output (got: $E2E_OK)" ;;
esac

# --- G1: the SHIPPED assert must REFUSE on a stop order ----------------------
# AC7's headline claim is that a lead's HOLD: is MECHANICAL. Every other case
# proves the leg refuses through its OWN CLI; none drove the shipped
# premerge-assert.sh to refuse BECAUSE of a hold, so the claim rested on a
# composition argument rather than on a public surface. The review is left BOUND
# here so the only thing that can refuse is the hold.
hold_payload "$MOCK_GH_DIR/pr-hold.json" \
  "[{\"author\":{\"login\":\"pmcfadin\"},\"createdAt\":\"$HOLD_AT\",\"body\":\"HOLD: merge after #9999\"}]"
E2E_HOLD=$(cd "$WORK" && PATH="$BIN:$PATH" MOCK_GH_OUT="$HEAD_AFTER OPEN" \
  bash "$FLOW/premerge-assert.sh" 1 "$HEAD_AFTER" "$E2E_GATE" 2>&1)
E2E_HOLD_RC=$?
if [ "$E2E_HOLD_RC" -eq 2 ]; then
  ok "wiring: the shipped assert REFUSES (exit 2) on a column-zero HOLD: comment (AC7)"
else
  bad "wiring: a HOLD: order did not stop the shipped assert (exit $E2E_HOLD_RC): $E2E_HOLD"
fi
case "$E2E_HOLD" in
  *"PREMERGE: HOLD"*)
    ok "wiring: the hold refusal carries its own distinct marker to the merge point" ;;
  *) bad "wiring: the hold refusal reached the merge point unnamed (got: $E2E_HOLD)" ;;
esac
# NON-VACUITY CONTROL: a refusal-only case is satisfied by a leg that is broken
# SHUT, so an allowlisted release over the same payload must reach exit 0. This
# is the pair that makes the two assertions above mean something.
hold_payload "$MOCK_GH_DIR/pr-hold.json" \
  "[{\"author\":{\"login\":\"pmcfadin\"},\"createdAt\":\"$HOLD_AT\",\"body\":\"HOLD: merge after #9999\"},{\"author\":{\"login\":\"pmcfadin\"},\"createdAt\":\"$GO_AT\",\"body\":\"GO: cleared\"}]"
E2E_GO=$(cd "$WORK" && PATH="$BIN:$PATH" MOCK_GH_OUT="$HEAD_AFTER OPEN" \
  bash "$FLOW/premerge-assert.sh" 1 "$HEAD_AFTER" "$E2E_GATE" 2>&1)
E2E_GO_RC=$?
if [ "$E2E_GO_RC" -eq 0 ]; then
  ok "wiring: an allowlisted GO: releases the hold and the shipped assert reaches exit 0"
else
  bad "wiring: a released hold still blocked the shipped assert (exit $E2E_GO_RC): $E2E_GO"
fi
hold_payload "$MOCK_GH_DIR/pr-hold.json" '[]'

# --- the scanner's own contract -----------------------------------------------------
scan_pr="$T/scan-pr.json"
python3 - "$scan_pr" <<'PY'
import json, sys
json.dump({"body": "==== ROBOREV REVIEW SUMMARY ====\njob: 304\nRESULT: PASS\n==== END ROBOREV REVIEW SUMMARY ====",
           "comments": [{"body": "job: 999 outside any block"}]}, open(sys.argv[1], "w"))
PY
scan_out=$(python3 "$SCANNER" jobs "$scan_pr" 2>&1)
# Here-strings, not pipes: `grep -qx` exits on its match, and under this
# suite's `set -o pipefail` a pipeline then reports the producer's SIGPIPE
# rather than the match (#3752, lane-3752). A one-shot `printf` of a small
# variable happens to be safe today because the whole string lands in one
# write, but that is a property of the DATA, not of the construct.
if grep -qx 'job=304' <<<"$scan_out" &&
  ! grep -qx 'job=999' <<<"$scan_out"; then
  ok "scanner: a job id is read ONLY from inside a roborev block, never from loose prose"
else
  bad "scanner: block scoping is wrong (got: $scan_out)"
fi
if grep -q '^recorded-verdict=304:' <<<"$scan_out"; then
  ok "scanner: the recorded terminal verdict is reported informationally"
else
  bad "scanner: the recorded terminal verdict was not reported (got: $scan_out)"
fi


# ==============================================================================
# BLOCKER 1 (roborev, HIGH) — THE `git_ref` BASE HALF IS PART OF THE BINDING
# ==============================================================================
# Validating only the HEAD half reopens CLAUDE.md's own recorded T4 vacuity
# class one level down: "a SINGLE-SHA review covers ONE COMMIT — a PARTIAL
# review whose enqueued sha EQUALS HEAD, so no sha check can see it." A record
# of `HEAD~1..HEAD` has a head EQUAL to the certified sha and leaves every
# earlier reviewable commit on the branch unreviewed. The wrapper asserts
# against that at REVIEW time; this leg must assert it at MERGE time.
#
# The expected base is the MERGE-BASE, never the base ref's TIP (#3392): a
# tip-expecting assert false-FAILs deterministically on any branch whose main
# advanced, and that was misdiagnosed as a race twice. Case `base-mb-not-tip`
# below is the falsifying control for that.
(
  cd "$WORK" || exit 1
  MB=$(git rev-parse main)
  printf '%s\n' "$MB" >"$T/b1-mb"
  git checkout -q -b b1-partial main
  printf 'fn c() {}\n' >>src/lib.rs
  git add -A && git commit -qm "b1 first code"
  git rev-parse HEAD >"$T/b1-mid"
  printf 'fn d() {}\n' >>src/lib.rs
  git add -A && git commit -qm "b1 second code"
  git rev-parse HEAD >"$T/b1-head"
  git checkout -q -b b1-prose main
  printf 'notes\n' >>README.md
  git add -A && git commit -qm "b1 prose prefix"
  git rev-parse HEAD >"$T/b1-prose-mid"
  printf 'fn e() {}\n' >>src/lib.rs
  git add -A && git commit -qm "b1 code after prose"
  git rev-parse HEAD >"$T/b1-prose-head"
  # A branch whose merge-base is NOT the base ref's tip: main advances AFTER
  # the branch point, exactly the #3392 shape.
  git checkout -q -b b1-mb main
  printf 'fn f() {}\n' >>src/lib.rs
  git add -A && git commit -qm "b1 mb-branch code"
  git rev-parse HEAD >"$T/b1-mb-head"
  # main's later advance carries CODE on purpose: it is what makes the
  # `base-off-branch` case below non-vacuous.
  git checkout -q main
  printf 'fn on_main() {}\n' >>src/lib.rs
  git add -A && git commit -qm "main advances again, with code"
) >/dev/null 2>&1
B1_MB=$(cat "$T/b1-mb")
B1_MID=$(cat "$T/b1-mid")
B1_HEAD=$(cat "$T/b1-head")
B1_PROSE_MID=$(cat "$T/b1-prose-mid")
B1_PROSE_HEAD=$(cat "$T/b1-prose-head")
B1_MB_HEAD=$(cat "$T/b1-mb-head")

# NON-VACUITY for the #3392 control: the merge-base must really differ from the
# base ref's tip, or `base-mb-not-tip` would prove nothing.
if [ "$B1_MB" != "$(cd "$WORK" && git rev-parse main)" ]; then
  ok "fixture: main advanced past the branch point, so merge-base != base-ref tip (#3392)"
else
  bad "fixture: merge-base equals main's tip — the #3392 control would be vacuous"
fi

# --- base-partial: a PARTIAL range whose head EQUALS the certified sha ----------
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 600)"
roborev_job 600 "$B1_MID" "$B1_HEAD"
if run_binding 4 "base-partial: a <head~1>..<head> record leaves earlier code unreviewed" \
  review-binding 1 pmcfadin/cqlite "$B1_HEAD"; then
  case "$OUT" in
    *"verdict UNBOUND"*) ok "base-partial: verdict UNBOUND" ;;
    *) bad "base-partial: expected UNBOUND — the head half alone cannot bind (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"reviewed BASE"*"unreviewed"*|*"unreviewed"*"reviewed BASE"*)
      ok "base-partial: the report names the unreviewed prefix, not just the head" ;;
    *) bad "base-partial: the refusal did not name the unreviewed prefix (got: $OUT)" ;;
  esac
fi

# --- base-prose-prefix: an omitted prefix that is CODE-FREE still binds ---------
# The fail-closed direction must not red correct input: a round that starts
# after a prose-only prefix has reviewed every reviewable commit on the branch.
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 601)"
roborev_job 601 "$B1_PROSE_MID" "$B1_PROSE_HEAD"
if run_binding 0 "base-prose-prefix: an omitted prefix that is code-free still binds" \
  review-binding 1 pmcfadin/cqlite "$B1_PROSE_HEAD"; then
  case "$OUT" in
    *"verdict BOUND"*) ok "base-prose-prefix: verdict BOUND" ;;
    *) bad "base-prose-prefix: a code-free omitted prefix was refused (got: $OUT)" ;;
  esac
fi

# --- base-mb-not-tip: the expected base is the MERGE-BASE, never the tip -------
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 602)"
roborev_job 602 "$B1_MB" "$B1_MB_HEAD"
if run_binding 0 "base-mb-not-tip: a base equal to the MERGE-BASE binds though main moved (#3392)" \
  review-binding 1 pmcfadin/cqlite "$B1_MB_HEAD"; then
  case "$OUT" in
    *"verdict BOUND"*) ok "base-mb-not-tip: verdict BOUND (the tip is not the expected base)" ;;
    *) bad "base-mb-not-tip: a correct merge-base-anchored review was refused (got: $OUT)" ;;
  esac
fi

# --- base-superset: a base BEHIND the merge-base reviewed MORE, not less -------
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 603)"
roborev_job 603 "$(cd "$WORK" && git rev-parse "$B1_MB^")" "$B1_MB_HEAD"
if run_binding 0 "base-superset: a base BEHIND the merge-base is more coverage, not less" \
  review-binding 1 pmcfadin/cqlite "$B1_MB_HEAD"; then
  case "$OUT" in
    *"verdict BOUND"*) ok "base-superset: verdict BOUND" ;;
    *) bad "base-superset: a superset review was refused (got: $OUT)" ;;
  esac
fi

# --- base-off-branch: a base OFF this branch skips NONE of its commits --------
# The skipped prefix is a COMMIT SET, not a path diff against the recorded base.
# A round recorded against the base ref's TIP skips none of the PR's own
# commits — none of them is an ancestor of it — even though the diff between
# the merge-base and that tip is full of main's code. Measuring the path diff
# directly refuses this shape, which is a FALSE FAIL on a merge gate; the
# sibling suite's end-to-end fixture caught exactly that, and it is pinned here
# too so the property lives in the leg's own suite.
MAIN_TIP=$(cd "$WORK" && git rev-parse main)
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 606)"
roborev_job 606 "$MAIN_TIP" "$B1_MB_HEAD"
if run_binding 0 "base-off-branch: a base off the branch skips none of its commits" \
  review-binding 1 pmcfadin/cqlite "$B1_MB_HEAD"; then
  case "$OUT" in
    *"verdict BOUND"*) ok "base-off-branch: verdict BOUND (main's own code is not a skipped prefix)" ;;
    *) bad "base-off-branch: a round covering the whole branch was refused (got: $OUT)" ;;
  esac
fi

# --- base-unresolvable: a base half naming no object here is UNMEASURED --------
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 604)"
roborev_job 604 0000000000000000000000000000000000000000 "$B1_HEAD"
if run_binding 5 "base-unresolvable: a base half resolving to no object is UNMEASURED" \
  review-binding 1 pmcfadin/cqlite "$B1_HEAD"; then
  case "$OUT" in
    *"verdict UNMEASURED"*) ok "base-unresolvable: verdict UNMEASURED, never a binding" ;;
    *) bad "base-unresolvable: expected UNMEASURED (got: $OUT)" ;;
  esac
fi

# --- base-nonhex: a base half that is not hex is its OWN named refusal ---------
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 605)"
python3 - "$MOCK_ROBOREV_DIR/job-605.json" "$B1_HEAD" <<'PY'
import json, sys
out, head = sys.argv[1:3]
json.dump({"id": 605, "job": {"id": 605, "git_ref": "notahex..%s" % head, "status": "done"}},
          open(out, "w"))
PY
if run_binding 5 "base-nonhex: a non-hex BASE half is UNMEASURED, never ignored" \
  review-binding 1 pmcfadin/cqlite "$B1_HEAD"; then
  case "$OUT" in
    *"base half"*) ok "base-nonhex: the cause names the BASE half specifically" ;;
    *) bad "base-nonhex: the base half was silently discarded (got: $OUT)" ;;
  esac
fi


# ==============================================================================
# BLOCKER 2 (roborev, MED) — ONE BAD RECORD MUST NOT END THE SCAN
# ==============================================================================
# A multi-round PR legitimately leaves rounds 1..n-1 behind, so short-circuiting
# on the FIRST unretrievable record refuses a PR that DOES carry a later
# covering round — and a false rationale in a gate artifact is what stops the
# next person looking.
#
# RESOLUTION RULE, asserted here and stated beside the code: an unresolved
# record can only change the answer while nothing has been DECIDED. So an
# unresolved record is decisive only when no covering round decided the
# question — never permissive, never unconditionally fatal.
#
# NOTE the rule about the COVERING set changed with job 78's finding F2 and is
# no longer "any covering round suffices": among the covering rounds the LATEST
# decides, and it must itself be bindable (see the `latest:` cases below). What
# these cases pin is the orthogonal property — that one bad record does not END
# the scan.

# pr_payload_comments <out> <baseRefName> <comment-body>... — several recorded
# blocks, one per top-level comment, in order.
pr_payload_comments() {
  local out="$1" base="$2"
  shift 2
  python3 - "$out" "$base" "$@" <<'PY'
import json, sys
out, base = sys.argv[1:3]
json.dump({"baseRefName": base, "body": "",
           "comments": [{"body": b} for b in sys.argv[3:]]}, open(out, "w"))
PY
}

pr_payload_comments "$MOCK_GH_DIR/pr.json" main "$(roborev_block 700)" "$(roborev_block 701)"
rm -f "$MOCK_ROBOREV_DIR/job-700.json" "$MOCK_ROBOREV_DIR/list.json"
roborev_job 701 "$B1_MB" "$B1_MB_HEAD"
if run_binding 0 "multi-round: an unretrievable FIRST record does not hide a covering second" \
  review-binding 1 pmcfadin/cqlite "$B1_MB_HEAD"; then
  case "$OUT" in
    *"verdict BOUND"*) ok "multi-round: verdict BOUND — coverage wins over an unresolved sibling" ;;
    *) bad "multi-round: a covering later round was not reached (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"700"*"could not be retrieved"*)
      ok "multi-round: the unresolved record is REPORTED, not silently dropped" ;;
    *) bad "multi-round: the unresolved record vanished from the report (got: $OUT)" ;;
  esac
fi

# When NOTHING binds, an unresolved record COULD have been the covering one, so
# the verdict is UNMEASURED — and the non-vacuity requirement is that the SECOND
# record was really examined, which a short-circuit could never show.
pr_payload_comments "$MOCK_GH_DIR/pr.json" main "$(roborev_block 700)" "$(roborev_block 702)"
roborev_job 702 "$B1_MB" "$B1_MID"
if run_binding 5 "multi-round: no coverage plus an unresolved record is UNMEASURED" \
  review-binding 1 pmcfadin/cqlite "$B1_HEAD"; then
  case "$OUT" in
    *"REVIEWABLE CODE was added after it"*)
      ok "multi-round: the second record WAS examined (no short-circuit on the first)" ;;
    *) bad "multi-round: the scan stopped at the first bad record (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"verdict UNMEASURED"*)
      ok "multi-round: an unresolved record that could have bound is UNMEASURED, not UNBOUND" ;;
    *) bad "multi-round: expected UNMEASURED (got: $OUT)" ;;
  esac
fi

# ==============================================================================
# FINDING F2 (roborev job 78) — THE LATEST COVERING ROUND DECIDES
# ==============================================================================
# The scan used to stop at the FIRST bindable record, so an earlier CLEAN round
# stayed sufficient even when a LATER recorded round at the same certified head
# reported findings or failure: a known, newer, adverse review result was simply
# ignored because an older favourable one was encountered first.
#
# The rule is now: among the records that COVER the certified head and whose
# result could be READ, the LATEST decides, and that round must itself be
# bindable. Chronology comes from the record's own `started_at`, never from
# PR-comment order (a comment can be posted out of order or edited) and never
# from the job id (nothing guarantees ids are monotonic across agents).
#
# NOT changed, and deliberately: an UNRETRIEVABLE record keeps the previous
# round's treatment — REPORTED, and decisive only when nothing bound. The
# finding is about KNOWN newer results being ignored; demanding retrievability
# of every historical record would red a correct multi-round PR whose early
# rounds have aged out of `roborev list --limit`. That residual is stated beside
# the code.

# --- earlier CLEAN + later FINDINGS, both covering: must NOT bind -------------
pr_payload_comments "$MOCK_GH_DIR/pr.json" main "$(roborev_block 710)" "$(roborev_block 711)"
roborev_job 710 "$B1_MB" "$B1_MB_HEAD" P done 2026-09-02T10:00:00Z
roborev_job 711 "$B1_MB" "$B1_MB_HEAD" F done 2026-09-02T11:00:00Z
if run_binding 4 "latest: an earlier clean round does not survive a later findings round" \
  review-binding 1 pmcfadin/cqlite "$B1_MB_HEAD"; then
  case "$OUT" in
    *"verdict UNBOUND"*)
      ok "latest: the LATEST covering round decides, not the first one encountered" ;;
    *) bad "latest: an older clean round still bound past a newer adverse one (got: $OUT)" ;;
  esac
  case "$OUT" in
    *711*) ok "latest: the deciding round is NAMED, so the operator knows which one" ;;
    *) bad "latest: the refusal did not name the deciding round (got: $OUT)" ;;
  esac
fi

# --- CONTROL, reversed order: later CLEAN over earlier FINDINGS DOES bind -----
# Without this the case above is satisfied by "any findings record anywhere
# refuses", which is a different (and wrong) rule.
pr_payload_comments "$MOCK_GH_DIR/pr.json" main "$(roborev_block 712)" "$(roborev_block 713)"
roborev_job 712 "$B1_MB" "$B1_MB_HEAD" F done 2026-09-02T10:00:00Z
roborev_job 713 "$B1_MB" "$B1_MB_HEAD" P done 2026-09-02T11:00:00Z
if run_binding 0 "latest: a later CLEAN round supersedes an earlier findings round" \
  review-binding 1 pmcfadin/cqlite "$B1_MB_HEAD"; then
  case "$OUT" in
    *"verdict BOUND"*)
      ok "latest: fixing findings and re-reviewing at the same head still binds" ;;
    *) bad "latest: a later clean round failed to supersede an earlier one (got: $OUT)" ;;
  esac
fi

# --- comment ORDER must not decide: adverse round listed FIRST ----------------
# Same two records as the refusal case, with the blocks in the opposite comment
# order. If the leg were ordering by encounter it would flip; by `started_at` it
# cannot.
pr_payload_comments "$MOCK_GH_DIR/pr.json" main "$(roborev_block 715)" "$(roborev_block 714)"
roborev_job 714 "$B1_MB" "$B1_MB_HEAD" P done 2026-09-02T10:00:00Z
roborev_job 715 "$B1_MB" "$B1_MB_HEAD" F done 2026-09-02T11:00:00Z
if run_binding 4 "latest: PR-comment order does not decide which round is latest" \
  review-binding 1 pmcfadin/cqlite "$B1_MB_HEAD"; then
  case "$OUT" in
    *"verdict UNBOUND"*)
      ok "latest: chronology comes from the record, not from comment order" ;;
    *) bad "latest: comment order changed the verdict (got: $OUT)" ;;
  esac
fi

# --- a covering record with NO chronology key: UNMEASURED, never a guess ------
pr_payload_comments "$MOCK_GH_DIR/pr.json" main "$(roborev_block 716)" "$(roborev_block 717)"
roborev_job 716 "$B1_MB" "$B1_MB_HEAD" P done 2026-09-02T10:00:00Z
roborev_job 717 "$B1_MB" "$B1_MB_HEAD" F done -
if run_binding 5 "latest: a covering record with no started_at is UNMEASURED" \
  review-binding 1 pmcfadin/cqlite "$B1_MB_HEAD"; then
  case "$OUT" in
    *"verdict UNMEASURED"*)
      ok "latest: an unorderable covering set is UNMEASURED — the order is not guessed" ;;
    *) bad "latest: expected UNMEASURED for an unorderable covering set (got: $OUT)" ;;
  esac
fi

# --- a malformed chronology key is NOT sortable as text: UNMEASURED -----------
# Lexicographic ordering is only sound for the fixed-width ISO-8601 UTC form, so
# anything else must refuse rather than sort wrongly.
pr_payload_comments "$MOCK_GH_DIR/pr.json" main "$(roborev_block 718)" "$(roborev_block 719)"
roborev_job 718 "$B1_MB" "$B1_MB_HEAD" P done 2026-09-02T10:00:00Z
roborev_job 719 "$B1_MB" "$B1_MB_HEAD" F done "yesterday afternoon"
if run_binding 5 "latest: a non-ISO started_at is UNMEASURED, not sorted as text" \
  review-binding 1 pmcfadin/cqlite "$B1_MB_HEAD"; then
  case "$OUT" in
    *"verdict UNMEASURED"*)
      ok "latest: only the fixed-width ISO-8601 UTC form is accepted as an order key" ;;
    *) bad "latest: a malformed stamp was accepted (got: $OUT)" ;;
  esac
fi

# --- A TIE AT THE MAXIMUM IS NOT A CHRONOLOGY (roborev job 82) ----------------
# The selection comparison is STRICT, so on equal `started_at` the FIRST
# ENCOUNTERED index survived — i.e. PR-record order decided, which is exactly
# what the case above proves must not happen. Measured on this box, every
# chronology field the record carries (`enqueued_at`/`started_at`/`finished_at`/
# `created_at`) is SECOND-resolution and the record `uuid` is v4, so there is no
# finer key to break a tie with: a tie carrying disagreement must refuse.

# tie, clean vs FINDINGS: must NOT bind
pr_payload_comments "$MOCK_GH_DIR/pr.json" main "$(roborev_block 720)" "$(roborev_block 721)"
roborev_job 720 "$B1_MB" "$B1_MB_HEAD" P done 2026-09-02T12:00:00Z
roborev_job 721 "$B1_MB" "$B1_MB_HEAD" F done 2026-09-02T12:00:00Z
if run_binding 5 "tie: a clean round tied with a findings round is UNMEASURED" \
  review-binding 1 pmcfadin/cqlite "$B1_MB_HEAD"; then
  case "$OUT" in
    *"verdict UNMEASURED"*)
      ok "tie: an equally-stamped adverse round is not outvoted by record order" ;;
    *) bad "tie: a tied clean round still bound (got: $OUT)" ;;
  esac
  case "$OUT" in
    *720*721*|*721*720*)
      ok "tie: BOTH tied job ids are named, so the operator can see the ambiguity" ;;
    *) bad "tie: the tied jobs were not both named (got: $OUT)" ;;
  esac
fi

# tie, clean vs NON-TERMINAL: must NOT bind (the other non-bindable class)
pr_payload_comments "$MOCK_GH_DIR/pr.json" main "$(roborev_block 722)" "$(roborev_block 723)"
roborev_job 722 "$B1_MB" "$B1_MB_HEAD" P done    2026-09-02T12:00:00Z
roborev_job 723 "$B1_MB" "$B1_MB_HEAD" P running 2026-09-02T12:00:00Z
if run_binding 5 "tie: a clean round tied with a NON-TERMINAL round is UNMEASURED" \
  review-binding 1 pmcfadin/cqlite "$B1_MB_HEAD"; then
  case "$OUT" in
    *"verdict UNMEASURED"*)
      ok "tie: an unconcluded tied round also blocks, not just a findings one" ;;
    *) bad "tie: a tie with an unconcluded round still bound (got: $OUT)" ;;
  esac
fi

# tie where EVERY tied round binds: sound, so it must NOT red correct input.
# Two concurrent reviewers can legitimately start inside one second; with no
# disagreement there is nothing for an ordering to resolve.
pr_payload_comments "$MOCK_GH_DIR/pr.json" main "$(roborev_block 724)" "$(roborev_block 725)"
roborev_job 724 "$B1_MB" "$B1_MB_HEAD" P done 2026-09-02T12:00:00Z
roborev_job 725 "$B1_MB" "$B1_MB_HEAD" P done 2026-09-02T12:00:00Z
if run_binding 0 "tie: all-bindable tied rounds still bind (no gratuitous refusal)" \
  review-binding 1 pmcfadin/cqlite "$B1_MB_HEAD"; then
  case "$OUT" in
    *"verdict BOUND"*)
      ok "tie: a tie with no disagreement is not treated as an ambiguity" ;;
    *) bad "tie: two clean tied rounds were refused (got: $OUT)" ;;
  esac
fi

# CONTROL: the ordinary DISTINCT-stamp path still binds, so none of the above is
# satisfied by "more than one covering round always refuses".
pr_payload_comments "$MOCK_GH_DIR/pr.json" main "$(roborev_block 726)" "$(roborev_block 727)"
roborev_job 726 "$B1_MB" "$B1_MB_HEAD" F done 2026-09-02T12:00:00Z
roborev_job 727 "$B1_MB" "$B1_MB_HEAD" P done 2026-09-02T12:00:01Z
if run_binding 0 "tie: a ONE-SECOND gap is a real order and still resolves" \
  review-binding 1 pmcfadin/cqlite "$B1_MB_HEAD"; then
  case "$OUT" in
    *"verdict BOUND"*)
      ok "tie: the tie rule fires only on EQUAL stamps, not on adjacent ones" ;;
    *) bad "tie: a distinct-stamp pair was treated as a tie (got: $OUT)" ;;
  esac
fi

# Two RESOLVABLE records, the first not covering: the definite refusal is
# UNBOUND, not UNMEASURED — nothing was unmeasurable.
pr_payload_comments "$MOCK_GH_DIR/pr.json" main "$(roborev_block 703)" "$(roborev_block 704)"
roborev_job 703 "$B1_MB" "$B1_MID"
roborev_job 704 "$B1_MB" "$B1_MID"
if run_binding 4 "multi-round: every record readable and none covering is UNBOUND" \
  review-binding 1 pmcfadin/cqlite "$B1_HEAD"; then
  case "$OUT" in
    *"verdict UNBOUND"*) ok "multi-round: a fully-measured absence of coverage is UNBOUND" ;;
    *) bad "multi-round: expected UNBOUND (got: $OUT)" ;;
  esac
fi

# ==============================================================================
# BLOCKER 3 (roborev, MED) — THE DISARM TIMELINE MUST BE READ IN FULL
# ==============================================================================
# One page of 100 events is not the timeline. On a longer PR a recent
# `auto_merge_disabled` sits on a later page, and reporting `clear` from a
# signal that was never fully read is the affirmative-measurement rule violated
# directly — a false PASS on exactly AC6's scenario (#3735 merged three minutes
# after the lead disarmed it).
# THIS IS THE ASSERT THAT FLAKED (#3752, lane-3752). It was
# `grep -vE '^ *#' "$BINDING" | grep -qE 'gh api .*--paginate'`, and it reddened
# once and then passed three times over a byte-identical helper: under this
# suite's `set -o pipefail` the consumer exits on its match while the producer
# is still writing, the producer takes SIGPIPE, and the pipeline reports 141 —
# a FALSE FAIL on correct input, i.e. the guard agents learn to waive.
assert_src_present \
  "timeline: the disarm timeline is requested with --paginate (structural)" \
  "timeline: the timeline request takes one page only — a later disarm is invisible" \
  "$BINDING" 'gh api .*--paginate' code

# `gh api --paginate` emits ONE JSON ARRAY PER PAGE, concatenated — not one
# array. Every page must be decoded before any verdict is reached.
paged_timeline() { # paged_timeline <page2-json-array>
  python3 - "$MOCK_GH_DIR/timeline.json" "$1" <<'PY'
import json, sys
out, page2 = sys.argv[1:3]
page1 = [{"event": "subscribed", "created_at": "2026-01-01T00:00:0%dZ" % (i % 10),
          "actor": {"login": "someone"}} for i in range(100)]
with open(out, "w") as fh:
    json.dump(page1, fh)
    fh.write("\n")
    fh.write(page2)
    fh.write("\n")
PY
}

hold_payload "$MOCK_GH_DIR/pr-hold.json" '[]'
paged_timeline "[{\"event\":\"auto_merge_disabled\",\"created_at\":\"$(iso_ago 180)\",\"actor\":{\"login\":\"pmcfadin\"}}]"
if run_hold 4 "timeline: a disarm on PAGE 2 is found (AC6's own scenario)"; then
  case "$OUT" in
    *"verdict HOLD-FOUND"*) ok "timeline: a second-page disarm inside the window stops the merge" ;;
    *) bad "timeline: a second-page disarm did not stop the merge (got: $OUT)" ;;
  esac
fi

# Decoding every page must not turn into a blanket hold: an OLD disarm on page 2
# is still outside the window.
paged_timeline "[{\"event\":\"auto_merge_disabled\",\"created_at\":\"$(iso_ago 5400)\",\"actor\":{\"login\":\"pmcfadin\"}}]"
if run_hold 0 "timeline: an OLD disarm on page 2 is decoded and correctly outside the window"; then
  case "$OUT" in
    *"verdict NO-HOLD-RECOGNISED"*) ok "timeline: reading every page did not become a blanket hold" ;;
    *) bad "timeline: an out-of-window page-2 disarm still blocked (got: $OUT)" ;;
  esac
fi

# A pagination that cannot be completed is UNMEASURED — read as a hold.
printf '[]\n{ this is not json\n' >"$MOCK_GH_DIR/timeline.json"
if run_hold 5 "timeline: a pagination that cannot be decoded in full is UNMEASURED"; then
  case "$OUT" in
    *"verdict UNMEASURED"*) ok "timeline: an undecodable later page is UNMEASURED, never clear" ;;
    *) bad "timeline: expected UNMEASURED (got: $OUT)" ;;
  esac
fi

# ==============================================================================
# THE THREE-VALUED PROBE ITSELF (#3752, lane-3752 audit)
# ==============================================================================
# Every structural assert above reads a FILE, and a `grep -q` inside an
# `if/else` answers a three-valued question two-valued: "matched", "did not
# match" and "could not be read" are three facts, and the two-valued form folds
# the third onto whichever branch was written second. Under the ABSENCE asserts
# here (no env override, no enforcer seam, no PASS in the static text) that
# branch is `ok` — an unreadable subject CERTIFIES the property, which is a
# false PASS in a merge-gate test.
#
# SUBSTITUTING THE ARTIFACT, never a path variable into repo code (#3312): each
# case builds its own scratch subject and probes THAT.
PROBE_T="$T/probe"
mkdir -p "$PROBE_T/adir"
printf 'gh api --paginate "repos/x/y"\n' >"$PROBE_T/present.txt"
printf '# gh api --paginate "repos/x/y"\n' >"$PROBE_T/comment-only.txt"
printf 'gh api "repos/x/y"\n' >"$PROBE_T/absent.txt"
: >"$PROBE_T/empty.txt"

probe_file_match "$PROBE_T/present.txt" 'gh api .*--paginate' code
case "$?" in
  0) ok "probe: a readable subject that MATCHES returns 0" ;;
  *) bad "probe: a readable matching subject did not return 0" ;;
esac
probe_file_match "$PROBE_T/absent.txt" 'gh api .*--paginate' code
case "$?" in
  1) ok "probe: a readable subject that does NOT match returns 1" ;;
  *) bad "probe: a readable non-matching subject did not return 1" ;;
esac
# The `code` mode is not decoration: it is what stops this file's own prose
# about --paginate satisfying an assert about the helper's CODE.
probe_file_match "$PROBE_T/comment-only.txt" 'gh api .*--paginate' code
case "$?" in
  1) ok "probe: a match that lives ONLY in a comment is not code (mode=code)" ;;
  *) bad "probe: a commented-out match was read as code" ;;
esac
for _sub in "$PROBE_T/no-such-file" "$PROBE_T/adir" "$PROBE_T/empty.txt"; do
  probe_file_match "$_sub" 'gh api .*--paginate' code
  if [ "$?" -eq 2 ]; then
    ok "probe: an unmeasurable subject ($(basename "$_sub")) returns 2, never the permissive 1"
  else
    bad "probe: $(basename "$_sub") collapsed onto a match verdict — the fold is back"
  fi
done

# THE ROUTING, which is where the false PASS actually lived. `ok`/`bad` are
# rebound to counters via their OWN saved definitions (`declare -f`), so the
# restore cannot drift from the originals and the self-test's own probes do not
# print into the suite's tally.
_OK_DEF=$(declare -f ok)
_BAD_DEF=$(declare -f bad)
SELF_OK=0
SELF_BAD=0
SELF_MSG=""
ok() { SELF_OK=$((SELF_OK + 1)); SELF_MSG="$1"; }
bad() { SELF_BAD=$((SELF_BAD + 1)); SELF_MSG="$1"; }
assert_src_absent "probe-selftest: absent" "probe-selftest: present" \
  "$PROBE_T/no-such-file" 'anything'
ABS_OK=$SELF_OK
ABS_BAD=$SELF_BAD
ABS_MSG=$SELF_MSG
SELF_OK=0
SELF_BAD=0
PRES_ABSENT_MSG="probe-present: the pattern is NOT there"
assert_src_present "probe-present: found" "$PRES_ABSENT_MSG" \
  "$PROBE_T/no-such-file" 'anything'
PRES_OK=$SELF_OK
PRES_BAD=$SELF_BAD
PRES_MSG=$SELF_MSG
SELF_OK=0
SELF_BAD=0
assert_count "probe-selftest: count" "probe-selftest: got %s" \
  "$PROBE_T/no-such-file" line-exact 'x' 0
CNT_OK=$SELF_OK
CNT_BAD=$SELF_BAD
CNT_MSG=$SELF_MSG
eval "$_OK_DEF"
eval "$_BAD_DEF"

if [ "$ABS_BAD" -eq 1 ] && [ "$ABS_OK" -eq 0 ]; then
  ok "probe: an ABSENCE assert over an unreadable subject FAILS — it can no longer self-certify"
else
  bad "probe: an unreadable subject satisfied an ABSENCE assert (ok=$ABS_OK bad=$ABS_BAD) — false PASS"
fi
case "$ABS_MSG" in
  *UNMEASURED*"could not read"*)
    ok "probe: the third state is REPORTED AS ITSELF, not as the absence of the property" ;;
  *) bad "probe: the unmeasurable case borrowed another verdict's wording (got: $ABS_MSG)" ;;
esac
# The PRESENCE direction was already fail-closed; what it lacked was a cause.
# It must not reuse the "the pattern is NOT there" wording, because "the helper
# lost its pagination" and "I could not read the helper" send an operator to
# two different places.
if [ "$PRES_BAD" -eq 1 ] && [ "$PRES_OK" -eq 0 ]; then
  case "$PRES_MSG" in
    "$PRES_ABSENT_MSG")
      bad "probe: an unreadable subject reported the ABSENT cause — the two failures are conflated" ;;
    *UNMEASURED*"could not read"*)
      ok "probe: a PRESENCE assert names UNMEASURED, never the absent-property cause" ;;
    *) bad "probe: a PRESENCE assert over an unreadable subject named no cause: $PRES_MSG" ;;
  esac
else
  bad "probe: the presence routing did not fail closed (ok=$PRES_OK bad=$PRES_BAD)"
fi
# A COUNT is the same trap one step out: `[ "$(probe_count ...)" -eq 0 ]` on an
# unreadable subject compares an EMPTY string and would have to be handled by
# the caller, so the helper refuses rather than returning a number it did not
# measure. The expected value here is 0 ON PURPOSE — an unmeasured subject must
# not satisfy an assert that a count is zero.
if [ "$CNT_BAD" -eq 1 ] && [ "$CNT_OK" -eq 0 ]; then
  ok "probe: an unreadable subject does not satisfy a 'count is 0' assert"
else
  bad "probe: a count over an unreadable subject was read as 0 (ok=$CNT_OK bad=$CNT_BAD)"
fi

# NO PIPELINE MAY DECIDE A VERDICT IN THIS SUITE. This is the structural half of
# the measured false FAIL: under `set -o pipefail` a `producer | grep -q` reports
# the producer's SIGPIPE when the consumer matches and exits early, so the
# construct itself is the defect and its absence is the property. The needle is
# assembled so this guard cannot match its own line (the sibling suite's idiom).
_pipe_a='| grep -'
_pipe_b='q'
assert_src_absent_fixed \
  "pipeline: no verdict in this suite is derived from a pipe into an early-exiting grep" \
  "pipeline: a pipe into an early-exiting grep is back — under pipefail such a pipeline reports the PRODUCER's SIGPIPE, so a correct file reds intermittently" \
  "${BASH_SOURCE[0]}" "$_pipe_a$_pipe_b" code

# ==============================================================================
# THE HOLD LEG'S ISSUE-THREAD DISCOVERY IS THREE-VALUED (#3752, lane-3752)
# ==============================================================================
# `issues=$(python3 -c ...) || issues=""` folded "this PR closes NO issue" and
# "which issues it closes could not be READ" onto one value, so an unreadable
# payload re-read NO issue thread and the leg could still report a CLEARANCE
# with a `HOLD:` sitting on the issue. The POSITIVE CONTROL comes first: without
# it the refusals below could all be firing on some other cause.
raw_pr_hold() { printf '%s\n' "$2" >"$1"; }

timeline_payload '[]'
raw_pr_hold "$MOCK_GH_DIR/pr-hold.json" \
  '{"body":"","comments":[],"closingIssuesReferences":[{"number":9001}]}'
python3 - "$MOCK_GH_DIR/issue-9001.json" "$(iso_ago 600)" <<'PY'
import json, sys
json.dump({"body": "", "comments": [
    {"author": {"login": "pmcfadin"}, "createdAt": sys.argv[2],
     "body": "HOLD: merge after #9999"}]}, open(sys.argv[1], "w"))
PY
if run_hold 4 "issues: a HOLD on the CLOSED ISSUE's thread stops the merge (positive control)"; then
  case "$OUT" in
    *"also re-reading issue #9001"*)
      ok "issues: the issue thread named by closingIssuesReferences really is re-read" ;;
    *) bad "issues: the issue thread was never re-read — the refusals below prove nothing (got: $OUT)" ;;
  esac
fi

# ==============================================================================
# FINDING F3 (roborev job 78) — A RELEASE CLEARS ONLY ITS OWN THREAD
# ==============================================================================
# Every marker used to land in ONE global timeline, and `latest wins` was
# computed across the pool. So an authorized `GO:` on one closing issue cleared
# an unrelated, NEWER `HOLD:` on another thread purely by being later — a
# release nobody ever wrote for the thread that was held. Resolution is now per
# thread and the leg refuses while ANY thread is held. There is deliberately no
# cross-thread release.

# --- a newer authorized GO on issue B does NOT clear a HOLD on issue A --------
timeline_payload '[]'
raw_pr_hold "$MOCK_GH_DIR/pr-hold.json" \
  '{"body":"","comments":[],"closingIssuesReferences":[{"number":9101},{"number":9102}]}'
python3 - "$MOCK_GH_DIR/issue-9101.json" "$(iso_ago 900)" <<'PYA'
import json, sys
json.dump({"body": "", "comments": [
    {"author": {"login": "pmcfadin"}, "createdAt": sys.argv[2],
     "body": "HOLD: merge after #9999"}]}, open(sys.argv[1], "w"))
PYA
python3 - "$MOCK_GH_DIR/issue-9102.json" "$(iso_ago 60)" <<'PYB'
import json, sys
json.dump({"body": "", "comments": [
    {"author": {"login": "pmcfadin"}, "createdAt": sys.argv[2],
     "body": "GO: unrelated thread, cleared here"}]}, open(sys.argv[1], "w"))
PYB
if run_hold 4 "threads: a newer GO on ANOTHER issue does not clear this issue's HOLD"; then
  case "$OUT" in
    *"issue #9101 is HELD"*)
      ok "threads: the held thread is NAMED, so the operator knows where to post the release" ;;
    *) bad "threads: the cross-thread release cleared an unrelated hold (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"no cross-thread release"*)
      ok "threads: the report states a release clears only its own thread" ;;
    *) bad "threads: the refusal did not explain thread scoping (got: $OUT)" ;;
  esac
fi

# --- CONTROL: a newer authorized GO on the SAME thread DOES clear it ----------
# Without this the case above is satisfied by "a GO never clears anything".
rm -f "$MOCK_GH_DIR/issue-9102.json"
raw_pr_hold "$MOCK_GH_DIR/pr-hold.json" \
  '{"body":"","comments":[],"closingIssuesReferences":[{"number":9101}]}'
python3 - "$MOCK_GH_DIR/issue-9101.json" "$(iso_ago 900)" "$(iso_ago 60)" <<'PYC'
import json, sys
json.dump({"body": "", "comments": [
    {"author": {"login": "pmcfadin"}, "createdAt": sys.argv[2],
     "body": "HOLD: merge after #9999"},
    {"author": {"login": "pmcfadin"}, "createdAt": sys.argv[3],
     "body": "GO: cleared on the same thread"}]}, open(sys.argv[1], "w"))
PYC
if run_hold 0 "threads: a newer GO on the SAME thread does clear its own HOLD (control)"; then
  case "$OUT" in
    *"NO-HOLD-RECOGNISED"*)
      ok "threads: an in-thread release still works, so the scoping is not a mute button" ;;
    *) bad "threads: an in-thread release failed to clear (got: $OUT)" ;;
  esac
fi

# --- a PR-level GO does NOT clear an ISSUE-level HOLD -------------------------
rm -f "$MOCK_GH_DIR/issue-9101.json"
raw_pr_hold "$MOCK_GH_DIR/pr-hold.json" \
  "$(python3 -c '
import json, sys
json.dump({"body": "", "comments": [
    {"author": {"login": "pmcfadin"}, "createdAt": sys.argv[1],
     "body": "GO: from the PR thread"}],
    "closingIssuesReferences": [{"number": 9103}]}, sys.stdout)
' "$(iso_ago 60)")"
python3 - "$MOCK_GH_DIR/issue-9103.json" "$(iso_ago 900)" <<'PYD'
import json, sys
json.dump({"body": "", "comments": [
    {"author": {"login": "pmcfadin"}, "createdAt": sys.argv[2],
     "body": "HOLD: merge after #9999"}]}, open(sys.argv[1], "w"))
PYD
if run_hold 4 "threads: a PR-level GO does not clear an issue-level HOLD"; then
  case "$OUT" in
    *"issue #9103 is HELD"*)
      ok "threads: a PR release does not reach the issue thread it never named" ;;
    *) bad "threads: a PR-level GO cleared an issue-level hold (got: $OUT)" ;;
  esac
fi

rm -f "$MOCK_GH_DIR/issue-9103.json"

# The three unreadable SHAPES. Each is a payload that DECODES as JSON, so the
# leg's own `gh`/JSON guards do not fire: the only thing standing between them
# and a false clearance is the extractor refusing a shape it cannot read.
rm -f "$MOCK_GH_DIR/issue-9001.json"
raw_pr_hold "$MOCK_GH_DIR/pr-hold.json" \
  '{"body":"","comments":[],"closingIssuesReferences":"9001"}'
if run_hold 5 "issues: closingIssuesReferences that is not a LIST is UNMEASURED"; then
  case "$OUT" in
    *"closingIssuesReferences could not be read"*)
      ok "issues: the cause names the closingIssuesReferences read, not something else" ;;
    *) bad "issues: expected the closingIssuesReferences cause (got: $OUT)" ;;
  esac
fi
raw_pr_hold "$MOCK_GH_DIR/pr-hold.json" \
  '{"body":"","comments":[],"closingIssuesReferences":["9001"]}'
if run_hold 5 "issues: a closingIssuesReferences ENTRY that is not an object is UNMEASURED"; then
  case "$OUT" in
    *"verdict UNMEASURED"*)
      ok "issues: an unreadable entry is a refusal, never 'this PR closes nothing'" ;;
    *) bad "issues: expected UNMEASURED (got: $OUT)" ;;
  esac
fi
raw_pr_hold "$MOCK_GH_DIR/pr-hold.json" \
  '{"body":"","comments":[],"closingIssuesReferences":[{"number":"9001"}]}'
if run_hold 5 "issues: a non-integer issue number is UNMEASURED, not skipped"; then
  case "$OUT" in
    *"verdict UNMEASURED"*)
      ok "issues: a skipped entry would be the same fold one level in, so it refuses" ;;
    *) bad "issues: expected UNMEASURED (got: $OUT)" ;;
  esac
fi
# NON-VACUITY IN THE OTHER DIRECTION: a PR that genuinely closes nothing must
# still clear, or the fix would just be a blanket hold.
raw_pr_hold "$MOCK_GH_DIR/pr-hold.json" \
  '{"body":"","comments":[],"closingIssuesReferences":[]}'
if run_hold 0 "issues: a PR that closes NOTHING still clears (the fix is not a blanket hold)"; then
  case "$OUT" in
    *"verdict NO-HOLD-RECOGNISED"*) ok "issues: an empty closing list is a measured absence" ;;
    *) bad "issues: expected NO-HOLD-RECOGNISED (got: $OUT)" ;;
  esac
fi
# And an ABSENT key is an absence too: gh omits it on payloads that were not
# asked for it, and refusing there would red correct input.
raw_pr_hold "$MOCK_GH_DIR/pr-hold.json" '{"body":"","comments":[]}'
if run_hold 0 "issues: an ABSENT closingIssuesReferences key is an absence, not a refusal"; then
  case "$OUT" in
    *"verdict NO-HOLD-RECOGNISED"*) ok "issues: an absent key does not red a correct payload" ;;
    *) bad "issues: an absent key was read as unmeasurable (got: $OUT)" ;;
  esac
fi

# --- the scanner's comment bodies are three-valued too --------------------------
# A body that is neither a string nor null is a payload SHAPE the scanner does
# not understand, and skipping it can only ever SHRINK the hold set.
raw_pr_hold "$MOCK_GH_DIR/pr-hold.json" \
  "{\"body\":\"\",\"comments\":[{\"author\":{\"login\":\"pmcfadin\"},\"createdAt\":\"$(iso_ago 600)\",\"body\":42}],\"closingIssuesReferences\":[]}"
if run_hold 5 "scanner: a comment body of an unexpected TYPE is UNMEASURED"; then
  case "$OUT" in
    *"body was not a string"*)
      ok "scanner: the unreadable comment is named, not silently dropped from the hold set" ;;
    *) bad "scanner: expected the body-shape cause (got: $OUT)" ;;
  esac
fi
raw_pr_hold "$MOCK_GH_DIR/pr-hold.json" \
  "{\"body\":\"\",\"comments\":[{\"author\":{\"login\":\"pmcfadin\"},\"createdAt\":\"$(iso_ago 600)\",\"body\":null}],\"closingIssuesReferences\":[]}"
if run_hold 0 "scanner: a NULL body is a comment with no text, and still clears"; then
  case "$OUT" in
    *"verdict NO-HOLD-RECOGNISED"*)
      ok "scanner: text is the only place a column-zero marker can live, so null is an absence" ;;
    *) bad "scanner: a null body was read as unmeasurable (got: $OUT)" ;;
  esac
fi

# ==============================================================================
# FINDINGS 2 + 3 (roborev job 59) — THE WHOLE THREAD, ORDERED BY EDIT TIME
# ==============================================================================
# `gh pr view --json comments` returns a BOUNDED connection, so a persistent
# column-zero `HOLD:` outside the window produced a false NO-HOLD-RECOGNISED —
# the same defect already fixed for the disarm TIMELINE, still live for the
# artifact a lead actually posts a stop order in. And ordering markers by
# `createdAt` let an OLD comment EDITED to carry `HOLD:` lose to a `GO:` posted
# before the edit, so the hold visible on the thread right now was ignored.

# --- a HOLD on a LATER PAGE stops the merge -----------------------------------
# `gh api --paginate` emits ONE ARRAY PER PAGE, CONCATENATED. Page 1 is
# innocuous; the stop order is on page 2, which a single-page read drops.
timeline_payload '[]'
raw_pr_hold "$MOCK_GH_DIR/pr-hold.json" '{"body":"","comments":[],"closingIssuesReferences":[]}'
printf '%s%s' \
  "[{\"author\":{\"login\":\"someone\"},\"created_at\":\"$(iso_ago 900)\",\"updated_at\":\"$(iso_ago 900)\",\"body\":\"ordinary chatter\"}]" \
  "[{\"author\":{\"login\":\"pmcfadin\"},\"created_at\":\"$(iso_ago 600)\",\"updated_at\":\"$(iso_ago 600)\",\"body\":\"HOLD: merge after #9999\"}]" \
  >"$MOCK_GH_DIR/comment-pages.json"
if run_hold 4 "pages: a HOLD on the SECOND page stops the merge"; then
  case "$OUT" in
    *"verdict HOLD-FOUND"*)
      ok "pages: every page is decoded, so a stop order past page 1 is not dropped" ;;
    *) bad "pages: a second-page HOLD was missed (got: $OUT)" ;;
  esac
fi

# NON-VACUITY: the same stream WITHOUT the second page must clear, or the case
# above would pass on any two-page input at all.
printf '%s' \
  "[{\"author\":{\"login\":\"someone\"},\"created_at\":\"$(iso_ago 900)\",\"updated_at\":\"$(iso_ago 900)\",\"body\":\"ordinary chatter\"}]" \
  >"$MOCK_GH_DIR/comment-pages.json"
if run_hold 0 "pages: page 1 alone genuinely clears (non-vacuity control)"; then
  case "$OUT" in
    *"verdict NO-HOLD-RECOGNISED"*)
      ok "pages: the second-page case proves pagination, not a blanket hold" ;;
    *) bad "pages: expected a clearance without the second page (got: $OUT)" ;;
  esac
fi

# --- a stream that cannot be decoded IN FULL is UNMEASURED --------------------
printf '%s' \
  "[{\"author\":{\"login\":\"someone\"},\"created_at\":\"$(iso_ago 900)\",\"body\":\"fine\"}][{tru" \
  >"$MOCK_GH_DIR/comment-pages.json"
if run_hold 5 "pages: a stream with an undecodable page is UNMEASURED, not partially read"; then
  case "$OUT" in
    *"could not be normalised"* | *"read in full"*)
      ok "pages: a partially-decodable thread is a hold, never a clearance" ;;
    *) bad "pages: expected the incomplete-thread cause (got: $OUT)" ;;
  esac
fi

# --- REST spellings are accepted (the coupling that would green vacuously) ----
# `gh api` says `user.login`/`created_at`; `gh pr view --json` says
# `author.login`/`createdAt`. Read the wrong one and EVERY author is empty, so a
# `GO:` from the allowlist silently stops releasing and a deferral silently
# stops being granted — fail-closed, and WRONG ON CORRECT INPUT.
printf '%s' \
  "[{\"user\":{\"login\":\"pmcfadin\"},\"created_at\":\"$(iso_ago 600)\",\"updated_at\":\"$(iso_ago 600)\",\"body\":\"HOLD: merge after #9999\"},{\"user\":{\"login\":\"pmcfadin\"},\"created_at\":\"$(iso_ago 300)\",\"updated_at\":\"$(iso_ago 300)\",\"body\":\"GO: cleared\"}]" \
  >"$MOCK_GH_DIR/comment-pages.json"
if run_hold 0 "rest-shape: a REST-spelled allowlisted GO releases a REST-spelled HOLD"; then
  case "$OUT" in
    *"authorized release"*)
      ok "rest-shape: user.login is read, so an allowlisted release is still recognised" ;;
    *) bad "rest-shape: the REST author spelling was not read (got: $OUT)" ;;
  esac
fi
# And the release must still be REFUSED from a non-allowlisted REST author.
printf '%s' \
  "[{\"user\":{\"login\":\"pmcfadin\"},\"created_at\":\"$(iso_ago 600)\",\"updated_at\":\"$(iso_ago 600)\",\"body\":\"HOLD: merge after #9999\"},{\"user\":{\"login\":\"stranger\"},\"created_at\":\"$(iso_ago 300)\",\"updated_at\":\"$(iso_ago 300)\",\"body\":\"GO: cleared\"}]" \
  >"$MOCK_GH_DIR/comment-pages.json"
if run_hold 4 "rest-shape: a REST-spelled GO from a stranger does NOT release"; then
  case "$OUT" in
    *"IGNORED"*)
      ok "rest-shape: normalising the spelling did not widen who may release" ;;
    *) bad "rest-shape: a stranger's release was honoured (got: $OUT)" ;;
  esac
fi

# --- ORDERED BY EDIT TIME: an old comment EDITED to HOLD beats a newer GO -----
# The `GO:` was CREATED after the hold comment was created, but the hold
# comment was EDITED later still, so the currently-visible stop order is the
# hold. Keyed on createdAt, the GO wins and the merge proceeds.
printf '%s' \
  "[{\"user\":{\"login\":\"pmcfadin\"},\"created_at\":\"$(iso_ago 1200)\",\"updated_at\":\"$(iso_ago 60)\",\"body\":\"HOLD: merge after #9999\"},{\"user\":{\"login\":\"pmcfadin\"},\"created_at\":\"$(iso_ago 600)\",\"updated_at\":\"$(iso_ago 600)\",\"body\":\"GO: cleared\"}]" \
  >"$MOCK_GH_DIR/comment-pages.json"
if run_hold 4 "edited: a comment EDITED to carry HOLD supersedes an older-created GO"; then
  case "$OUT" in
    *"verdict HOLD-FOUND"*)
      ok "edited: markers are ordered by when the TEXT last changed, which is what a reader sees" ;;
    *) bad "edited: the edited-in hold was ignored (got: $OUT)" ;;
  esac
fi

# --- an unreadable EDIT timestamp on a marker-bearing comment is UNMEASURED ---
printf '%s' \
  "[{\"user\":{\"login\":\"pmcfadin\"},\"created_at\":\"$(iso_ago 600)\",\"updated_at\":\"not-a-timestamp\",\"body\":\"HOLD: merge after #9999\"}]" \
  >"$MOCK_GH_DIR/comment-pages.json"
if run_hold 5 "edited: an unreadable EDIT timestamp on a marker is UNMEASURED"; then
  case "$OUT" in
    *"unreadable edit timestamp"*)
      ok "edited: a marker that cannot be ORDERED is a refusal naming why, never ignored" ;;
    *) bad "edited: expected the edit-timestamp cause (got: $OUT)" ;;
  esac
fi
# ...and an unreadable edit timestamp on an ORDINARY comment does not red the run.
printf '%s' \
  "[{\"user\":{\"login\":\"someone\"},\"created_at\":\"$(iso_ago 600)\",\"updated_at\":\"not-a-timestamp\",\"body\":\"just chatter\"}]" \
  >"$MOCK_GH_DIR/comment-pages.json"
if run_hold 0 "edited: an unreadable edit timestamp on a NON-marker comment still clears"; then
  case "$OUT" in
    *"verdict NO-HOLD-RECOGNISED"*)
      ok "edited: the refusal is scoped to comments that actually carry a marker" ;;
    *) bad "edited: an ordinary comment's bad stamp red the run (got: $OUT)" ;;
  esac
fi
rm -f "$MOCK_GH_DIR/comment-pages.json"

# --- the normaliser REFUSES a shape it does not recognise --------------------
# A shorter comment list is indistinguishable from a quiet thread, which is the
# false clearance being fixed — so an unreadable shape is exit 1, never fewer
# comments.
norm_view="$T/norm-view.json"
norm_pages="$T/norm-pages.json"
norm_out="$T/norm-out.json"
printf '%s' '{"body":"","comments":[]}' >"$norm_view"
for shape in '{"not":"a list"}' '[{"user":{"login":42},"body":"x"}]' '["not an object"]' \
  '[{"created_at":42,"body":"x"}]'; do
  printf '%s' "$shape" >"$norm_pages"
  if python3 "$SCANNER" normalize "$norm_view" "$norm_pages" "$norm_out" >/dev/null 2>&1; then
    bad "normalize: the unrecognised shape $shape was ACCEPTED (a short thread is a false clearance)"
  else
    ok "normalize: the unrecognised shape $shape is REFUSED, not read as fewer comments"
  fi
done
# A DELETED ACCOUNT is a legitimate shape (both APIs answer null) and must not refuse.
printf '%s' '[{"user":null,"created_at":"2026-09-02T00:00:00Z","body":"x"}]' >"$norm_pages"
if python3 "$SCANNER" normalize "$norm_view" "$norm_pages" "$norm_out" >/dev/null 2>&1; then
  ok "normalize: a null author (deleted account) is a legitimate shape, not a refusal"
else
  bad "normalize: a null author was refused, which reds correct input"
fi

# --- STRUCTURAL: the leg no longer asks for the BOUNDED comment connection ---
assert_src_present \
  "pages: the leg reads comment threads through \`gh api --paginate\`" \
  "pages: the leg does not paginate its comment fetch" \
  "$BINDING" 'gh api --paginate' code
assert_src_absent \
  "pages: no fetch asks \`--json\` for the BOUNDED comments connection" \
  "pages: a fetch still requests --json comments, whose connection is bounded" \
  "$BINDING" '--json[ ]*[A-Za-z,]*comments' code

# ==============================================================================
# FINDING 1 (roborev job 59) — A RANGE MATCH ALONE MUST NOT BIND
# ==============================================================================
# The leg used to treat a matching `git_ref` as sufficient, with the recorded
# verdict REPORTED and nothing derived from it (a "declared residual"). So a
# block naming an in-progress, FAILED or findings-bearing job whose range
# matched the certified head BOUND the merge. That is an ACCIDENT route before a
# hostile one: this PR's own body records a job at a FAIL verdict, so a lane
# pasting its first failing round would have certified itself.
#
# The verdict now read is the JOB RECORD's structured letter, never the block's.

# --- verdict FINDINGS, no deferral: a MEASURED refusal, so UNBOUND ------------
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 610)"
roborev_job 610 "$(cd "$WORK" && git rev-parse main)" "$HEAD_AFTER" F
if run_binding 4 "result: a FINDINGS record covering the head does NOT bind" \
  review-binding 1 o/r "$HEAD_AFTER"; then
  case "$OUT" in
    *"verdict UNBOUND"*)
      ok "result: findings-with-no-deferral is UNBOUND — a measured refusal, not UNMEASURED" ;;
    *) bad "result: expected UNBOUND for a findings record (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"no authorized deferral"*)
      ok "result: the refusal NAMES the missing authorization, so the remedy is legible" ;;
    *) bad "result: the findings refusal did not name the deferral route (got: $OUT)" ;;
  esac
fi

# --- verdict ABSENT: never binds, and it is UNMEASURED (nothing was read) -----
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 611)"
roborev_job 611 "$(cd "$WORK" && git rev-parse main)" "$HEAD_AFTER" -
if run_binding 5 "result: a record with NO verdict field does NOT bind" \
  review-binding 1 o/r "$HEAD_AFTER"; then
  case "$OUT" in
    *"verdict UNMEASURED"*)
      ok "result: an unreadable record verdict is UNMEASURED — a range match is not a review" ;;
    *) bad "result: expected UNMEASURED for a verdict-less record (got: $OUT)" ;;
  esac
fi

# ==============================================================================
# FINDING F1 (roborev job 78) — A NON-TERMINAL JOB MUST NOT BIND
# ==============================================================================
# Job 59's finding 1 asked for affirmative structured evidence that the review
# CONCLUDED SUCCESSFULLY. The fix read the record's VERDICT and extracted
# `status` beside it — and then never consumed it, so the completion half was
# never implemented while the code read as though it were. A record carrying a
# clean letter with `status=running` (the letter is written before the row is
# finalised) or `status=failed` bound the merge.
#
# `done` is the observed terminal-success token, measured on this box's own live
# records (jobs 59 and 78 both report `status: done`). Anything NOT
# affirmatively recognised fails closed and is NAMED, because a status this code
# has never judged is exactly the unknown that must not inherit the permissive
# branch.

# --- status=running, verdict CLEAN: must NOT bind ----------------------------
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 620)"
roborev_job 620 "$(cd "$WORK" && git rev-parse main)" "$HEAD_AFTER" P running
if run_binding 5 "status: a RUNNING record with a clean letter does NOT bind" \
  review-binding 1 o/r "$HEAD_AFTER"; then
  case "$OUT" in
    *"verdict UNMEASURED"*)
      ok "status: a running record is UNMEASURED — the review has not concluded" ;;
    *) bad "status: expected UNMEASURED for a running record (got: $OUT)" ;;
  esac
  case "$OUT" in
    *running*)
      ok "status: the refusal NAMES the non-terminal status it saw" ;;
    *) bad "status: the refusal did not name the status (got: $OUT)" ;;
  esac
fi

# --- status=failed, verdict CLEAN: must NOT bind -----------------------------
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 621)"
roborev_job 621 "$(cd "$WORK" && git rev-parse main)" "$HEAD_AFTER" P failed
if run_binding 5 "status: a FAILED record with a clean letter does NOT bind" \
  review-binding 1 o/r "$HEAD_AFTER"; then
  case "$OUT" in
    *"verdict UNMEASURED"*)
      ok "status: a failed record is UNMEASURED, never a binding" ;;
    *) bad "status: expected UNMEASURED for a failed record (got: $OUT)" ;;
  esac
fi

# --- status ABSENT, verdict CLEAN: must NOT bind ------------------------------
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 622)"
roborev_job 622 "$(cd "$WORK" && git rev-parse main)" "$HEAD_AFTER" P -
if run_binding 5 "status: a record with NO status field does NOT bind" \
  review-binding 1 o/r "$HEAD_AFTER"; then
  case "$OUT" in
    *"verdict UNMEASURED"*)
      ok "status: an ABSENT status never binds — completion was not measured" ;;
    *) bad "status: expected UNMEASURED for a status-less record (got: $OUT)" ;;
  esac
fi

# --- status=done is the AFFIRMATIVE token: the control for the three above ----
# Without this the cases above are satisfied by a leg broken shut.
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 623)"
roborev_job 623 "$(cd "$WORK" && git rev-parse main)" "$HEAD_AFTER" P done
if run_binding 0 "status: a DONE record with a clean letter still binds (control)" \
  review-binding 1 o/r "$HEAD_AFTER"; then
  case "$OUT" in
    *"verdict BOUND"*)
      ok "status: \`done\` is the affirmative terminal token, so correct input is not red" ;;
    *) bad "status: a done+clean record failed to bind (got: $OUT)" ;;
  esac
fi

# --- status non-terminal + FINDINGS + an AUTHORIZED deferral: still no bind ---
# The status gate sits BEFORE the verdict branch, so an unconcluded job cannot
# reach EITHER bindable class. Pinned on the deferral class too, because that is
# the path an authorization could otherwise carry past the completion check.
MB_RUN=$(cd "$WORK" && git rev-parse main)
pr_payload_with_comment "$MOCK_GH_DIR/pr.json" main "$(roborev_block 624)" pmcfadin \
  "$(defer_marker 3602,3613 2 "$MB_RUN" "$HEAD_AFTER" 624 'deferred but the round never finished')"
roborev_job 624 "$MB_RUN" "$HEAD_AFTER" F running
if run_binding 5 "status: an unconcluded job does not bind even WITH a deferral" \
  review-binding 1 o/r "$HEAD_AFTER"; then
  case "$OUT" in
    *"verdict UNMEASURED"*)
      ok "status: the completion gate precedes BOTH bindable verdict classes" ;;
    *) bad "status: a running findings+deferral record was not UNMEASURED (got: $OUT)" ;;
  esac
fi

# --- FINDINGS + AUTHORIZED deferral, RECORD WITH NO REVIEW TEXT: UNMEASURED ---
# roborev job 103, and since #4050 this is specifically the NO-DERIVABLE-COUNT
# path (AC4c): `roborev_job` writes no `output` field, so the record carries no
# review text and no count can be DERIVED from it. The authorization itself is
# impeccable — allowlisted author, sole-content top-level marker, correct
# base/head/job scope, both tracking issues verified OPEN — and it STILL must not
# bind, because the marker's `count=` half is matched against the count the REVIEW
# observed. Declaring that gap and binding anyway let the merge gate honour a
# marker the review-time path would REJECT: an allowlisted human can post a fresh
# marker afterwards carrying any count at all. The positive control that this is
# about the MISSING TEXT and not about the authorization is the #4050 case below,
# where the SAME authorization over a record that DOES carry a countable review
# binds.
MB_MAIN=$(cd "$WORK" && git rev-parse main)
pr_payload_with_comment "$MOCK_GH_DIR/pr.json" main "$(roborev_block 612)" pmcfadin \
  "$(defer_marker 3602,3613 2 "$MB_MAIN" "$HEAD_AFTER" 612 'both filed and lead-deferred')"
roborev_job 612 "$MB_MAIN" "$HEAD_AFTER" F
# `issues=` names two tracking issues, so BOTH must be OPEN issues GitHub confirms.
issue_state_fixture 3602 OPEN
issue_state_fixture 3613 OPEN
if run_binding 5 "result: FINDINGS + an authorized deferral over a record with NO review text is UNMEASURED, not BOUND (job 103)" \
  review-binding 1 o/r "$HEAD_AFTER"; then
  case "$OUT" in
    *"verdict UNMEASURED"*)
      ok "result: a deferral cannot be verified at the merge point, so it cannot clear it" ;;
    *) bad "result: expected UNMEASURED for a findings+deferral round (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"verdict BOUND"*)
      bad "result: a findings record reached BOUND — the count= gap is back (got: $OUT)" ;;
    *) ok "result: no findings record reaches BOUND on any path" ;;
  esac
  # The cause must name the COUNT as the unverifiable half, and must NOT read as
  # "your authorization is bad" — that would send a lead to re-post a marker that
  # was already correct, which is the wrong-remedy defect job 102 closed.
  case "$OUT" in
    *"count="*"CANNOT BE VERIFIED"* | *"count="*"cannot be verified"*)
      ok "result: the cause names the count= half as the unverifiable evidence" ;;
    *) bad "result: the cause did not name the count= half (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"AUTHORIZED by @pmcfadin"*)
      ok "result: the good authorization is still REPORTED, so the remedy is not 're-post the marker'" ;;
    *) bad "result: the cause did not record that an authorization was found (got: $OUT)" ;;
  esac
  # ...and the cause must name the DERIVATION as what failed. A record with NO `output` field
  # and one carrying only whitespace are INDISTINGUISHABLE at this layer (the shared parser
  # writes an empty file for both), so the cause names both rather than picking one — pinned
  # here on the absent-field fixture and in 4050(c) on the whitespace one.
  case "$OUT" in
    *"no usable review text"*)
      ok "result: the cause names the absent review text as what stopped the derivation" ;;
    *) bad "result: the cause did not name the missing review text (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"count= half is NOT re-verified here"*)
      bad "result: the stale DECLARATION of the count gap survived (got: $OUT)" ;;
    *) ok "result: the old 'declared rather than implied' count line is gone" ;;
  esac
  case "$OUT" in
    *"obtain a clean covering round"* | *"re-run the review"*)
      ok "result: the remedy is a clean round, not a fixed box and not a fresh marker" ;;
    *) bad "result: the cause carried no usable remedy (got: $OUT)" ;;
  esac
fi

# ===========================================================================
# ISSUE #4050 — A FULLY MEASURED DEFERRAL BINDS, AND EVERY UNMEASURABLE STATE
# KEEPS TODAY'S REFUSAL.
# ===========================================================================
# The record carries no findings-count FIELD, which is what made the case above
# UNMEASURED — but it DOES carry the review TEXT, and #4050 derives the count from
# it with the SAME recogniser the review-time gate uses. Before this, no sequence
# of actions could merge a validly deferred PR: three (#3859, #3858, #3816) were
# hard-blocked. The cases below pin BOTH directions.

# --- (a) count= EQUALS the derived count: BOUND -------------------------------
# THE CASE THAT UNBLOCKS THOSE PRs, and the positive control for every refusal
# below: it differs from them in ONE property at a time (the count, the review
# text, the marker's presence), so a refusal elsewhere cannot be a setup failure.
FC_REVIEW2=$(findings_review_text 2)
pr_payload_with_comment "$MOCK_GH_DIR/pr.json" main "$(roborev_block 640)" pmcfadin \
  "$(defer_marker 3602,3613 2 "$MB_MAIN" "$HEAD_AFTER" 640 'both filed and lead-deferred')"
roborev_job 640 "$MB_MAIN" "$HEAD_AFTER" F done 2026-09-02T10:00:00Z "$FC_REVIEW2"
issue_state_fixture 3602 OPEN
issue_state_fixture 3613 OPEN
if run_binding 0 "4050(a): a deferral whose count= EQUALS the DERIVED count BINDS" \
  review-binding 1 o/r "$HEAD_AFTER"; then
  case "$OUT" in
    *"verdict BOUND"*)
      ok "4050(a): a fully measured authorized deferral binds — the merge is obtainable again" ;;
    *) bad "4050(a): a matched deferral did not reach BOUND (got: $OUT)" ;;
  esac
  # The bind must SAY it rests on a deferral and name the matched count, or a
  # reader cannot tell it from a bind on a clean record — which is the whole
  # reason `findings:` reports DEFERRED and never NONE at review time.
  case "$OUT" in
    *"DEFERRED by an authorization from @pmcfadin"*)
      ok "4050(a): the bind records that it rests on an authorized deferral, not on a clean record" ;;
    *) bad "4050(a): the BOUND verdict did not disclose the deferral it rests on (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"count= EQUALS the 2 finding(s) DERIVED from job 640"*)
      ok "4050(a): the note names the equality that was actually measured, and where the count came from" ;;
    *) bad "4050(a): the bind did not name the measured count equality (got: $OUT)" ;;
  esac
fi

# --- (b) a MISMATCHED count= REFUSES, exit 4, NAMED ---------------------------
# roborev job 103's false green stays closed. ONE property differs from (a): the
# marker authorizes 1 finding while the record's review reports 2. The refusal
# must be exit 4 (a MEASURED refusal) and must NAME count-mismatch — folding it
# into the generic "no authorized deferral covers this job" would send a lead to
# re-post a marker that was already well-formed, when the action is to re-triage
# and re-authorize for the count actually observed.
pr_payload_with_comment "$MOCK_GH_DIR/pr.json" main "$(roborev_block 641)" pmcfadin \
  "$(defer_marker 3602 1 "$MB_MAIN" "$HEAD_AFTER" 641 'one filed, but the review found two')"
roborev_job 641 "$MB_MAIN" "$HEAD_AFTER" F done 2026-09-02T10:00:00Z "$FC_REVIEW2"
if run_binding 4 "4050(b): a MISMATCHED count= is a measured refusal (exit 4), not a bind" \
  review-binding 1 o/r "$HEAD_AFTER"; then
  case "$OUT" in
    *"verdict UNBOUND"*)
      ok "4050(b): a count mismatch is UNBOUND — measured and rejected, not unmeasurable" ;;
    *) bad "4050(b): a mismatched count did not reach UNBOUND (got: $OUT)" ;;
  esac
  case "$OUT" in
    *COUNT-MISMATCH*)
      ok "4050(b): the refusal NAMES count-mismatch as its own state" ;;
    *) bad "4050(b): the count mismatch was not named (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"authorizes 1 finding(s) but this job reports 2"*)
      ok "4050(b): the scanner's own detail travels, so both counts are visible to the authorizer" ;;
    *) bad "4050(b): the refusal did not report both counts (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"verdict BOUND"*)
      bad "4050(b): a mismatched deferral reached BOUND (got: $OUT)" ;;
    *) ok "4050(b): no mismatched deferral reaches BOUND on any path" ;;
  esac
fi

# --- (c) an EMPTY review text REFUSES, exit 5 ---------------------------------
# The absent-text half is the job-103 case above. This is the other spelling: the
# record HAS an `output` field and it is empty, which is a non-measurement and not
# a count of zero. Same authorization as (a), same count, so the only difference
# is the text.
pr_payload_with_comment "$MOCK_GH_DIR/pr.json" main "$(roborev_block 642)" pmcfadin \
  "$(defer_marker 3602,3613 2 "$MB_MAIN" "$HEAD_AFTER" 642 'both filed and lead-deferred')"
roborev_job 642 "$MB_MAIN" "$HEAD_AFTER" F done 2026-09-02T10:00:00Z '   '
if run_binding 5 "4050(c): an EMPTY recorded review text is UNMEASURED, never a bind" \
  review-binding 1 o/r "$HEAD_AFTER"; then
  case "$OUT" in
    *"verdict UNMEASURED"*)
      ok "4050(c): a record whose review text is unusable cannot be counted, so it cannot clear" ;;
    *) bad "4050(c): an empty review text was not UNMEASURED (got: $OUT)" ;;
  esac
  # DISCRIMINATING, not merely "some derivation failed": the EMPTY arm must be the one that
  # fired, or the case could pass on the contradiction arm and prove nothing about this input.
  case "$OUT" in
    *"no usable review text"*"no findings count could be DERIVED"*)
      ok "4050(c): the cause names the unusable review text and says the count could not be DERIVED — not that no count 'exists'" ;;
    *) bad "4050(c): the cause did not name an unusable review text as what failed (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"count exists"*)
      bad "4050(c): the cause still claims no count 'exists' — the record HAS no count field, but what failed here is the DERIVATION (got: $OUT)" ;;
    *) ok "4050(c): the corrected wording holds — nothing claims a count does not exist" ;;
  esac
  case "$OUT" in
    *"AUTHORIZED by @pmcfadin"*)
      ok "4050(c): the good authorization is still reported, so the remedy is not 're-post the marker'" ;;
    *) bad "4050(c): the cause did not record that an authorization was found (got: $OUT)" ;;
  esac
fi

# --- (d) a DERIVED count of 0 on a `verdict=F` record REFUSES, exit 5 ---------
# The structured verdict affirmatively says FINDINGS while the census over the
# review text finds no severity marker — a CONTRADICTION, not a measurement of
# this record's findings (the shape #3564 met twice: a findings review whose
# findings carry no recognised marker). Matching a marker against that 0 would let
# an authorization declaring `count=0` clear a findings-bearing record, so the
# marker below declares exactly that and must NOT bind.
pr_payload_with_comment "$MOCK_GH_DIR/pr.json" main "$(roborev_block 643)" pmcfadin \
  "$(defer_marker 3602 0 "$MB_MAIN" "$HEAD_AFTER" 643 'the review text carries no severity marker')"
roborev_job 643 "$MB_MAIN" "$HEAD_AFTER" F done 2026-09-02T10:00:00Z \
  '## Findings\n1. Something is wrong, stated in prose with no severity marker.\n## Summary\none issue'
if run_binding 5 "4050(d): a DERIVED count of 0 on a FINDINGS record is UNMEASURED, never a bind" \
  review-binding 1 o/r "$HEAD_AFTER"; then
  case "$OUT" in
    *"verdict UNMEASURED"*)
      ok "4050(d): a zero census against an F verdict is a contradiction, so nothing rests on it" ;;
    *) bad "4050(d): a zero derived count was not UNMEASURED (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"ZERO severity markers"*)
      ok "4050(d): the cause names the contradiction rather than reporting a count of zero" ;;
    *) bad "4050(d): the zero-vs-F contradiction was not named (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"verdict BOUND"*)
      bad "4050(d): a count=0 marker cleared a findings-bearing record (got: $OUT)" ;;
    *) ok "4050(d): a count=0 authorization cannot clear a findings-bearing record" ;;
  esac
fi

# --- (e) "no authorization" (4) stays DISTINCT from "count unverifiable" (5) --
# Both refuse the merge, so this is about the DIAGNOSIS: the remedies are
# different operator actions and collapsing them is the wrong-remedy defect job
# 102 closed one call over. The record here is COUNTABLE — same review text as
# (a) — so the only reason it cannot bind is that no marker was posted, which
# must read as a measured refusal and never as an unmeasurable count.
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 644)"
roborev_job 644 "$MB_MAIN" "$HEAD_AFTER" F done 2026-09-02T10:00:00Z "$FC_REVIEW2"
FC_E_RC4=""
if run_binding 4 "4050(e): a COUNTABLE findings record with NO authorization is UNBOUND (exit 4)" \
  review-binding 1 o/r "$HEAD_AFTER"; then
  FC_E_RC4="$OUT"
  case "$OUT" in
    *"no authorized deferral covers this job"*)
      ok "4050(e): the no-authorization refusal keeps its own wording on a countable record" ;;
    *) bad "4050(e): the no-authorization refusal was misworded (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"count= half CANNOT BE VERIFIED"* | *"could not be DERIVED"*)
      bad "4050(e): a missing marker was reported as an unverifiable COUNT — the two diagnoses collapsed (got: $OUT)" ;;
    *) ok "4050(e): a missing marker is never reported as an unverifiable count" ;;
  esac
fi
# ...and the converse half, re-run here so the two texts are compared in one place
# rather than across the file. Same job number, same countable-record absence, an
# authorization present: exit 5 and a DIFFERENT cause.
pr_payload_with_comment "$MOCK_GH_DIR/pr.json" main "$(roborev_block 645)" pmcfadin \
  "$(defer_marker 3602,3613 2 "$MB_MAIN" "$HEAD_AFTER" 645 'both filed and lead-deferred')"
roborev_job 645 "$MB_MAIN" "$HEAD_AFTER" F
if run_binding 5 "4050(e): an AUTHORIZED deferral over an uncountable record is UNMEASURED (exit 5)" \
  review-binding 1 o/r "$HEAD_AFTER"; then
  if [ -n "$FC_E_RC4" ] && [ "$OUT" != "$FC_E_RC4" ]; then
    ok "4050(e): the two refusals are numerically (4 vs 5) AND textually distinct"
  else
    bad "4050(e): the exit-4 and exit-5 refusals produced identical output, so the operator cannot tell them apart"
  fi
  case "$OUT" in
    *"no authorized deferral covers this job"*)
      bad "4050(e): an unverifiable count was reported as an unauthorized deferral — job 102's conflation (got: $OUT)" ;;
    *) ok "4050(e): an unverifiable count is never reported as a missing authorization" ;;
  esac
fi

# --- (l) a CLEAN-LOADING but INCOMPLETE recogniser is UNMEASURED and NAMES the gap
# roborev job 129's other half. The review-time consumer folded this state onto a permissive
# `findings: NONE` (measured: RESULT PASS, exit 0 — a false green); HERE the consequence was
# always a refusal, because derive_findings_count's failure routes to UNMEASURED. Both ends
# are fixed anyway, and the reason is this issue's soundness case: the two must agree byte
# for byte about what a usable recogniser IS, or "the same code over identical bytes" stops
# holding. What this case adds over (f) is that the CAUSE must name the MISSING FUNCTION —
# "the library did not load" sends an operator to a file that loads perfectly well.
FLOW_PARTLIB="$T/scripts/flow-partlib"
mkdir -p "$FLOW_PARTLIB/lib"
partlib_ready=1
for f in premerge-review-binding.sh premerge-pr-scan.py roborev-job-facts.py \
  roborev-waiver-scan.py roborev-review-oracles.sh base-staleness.sh; do
  cp "$FLOW/$f" "$FLOW_PARTLIB/$f" || partlib_ready=0
done
chmod +x "$FLOW_PARTLIB"/*.sh "$FLOW_PARTLIB"/*.py 2>/dev/null
# The REAL library's tail from the entry point onward: sources cleanly, entry point defined,
# helpers absent. Start line DERIVED, so moving the function cannot silently restage this.
partlib_start=$(grep -n '^roborev_findings_count()' "$FLOW/lib/roborev-findings-count.sh" | head -1 | cut -d: -f1)
if [ -n "$partlib_start" ]; then
  sed -n "${partlib_start},\$p" "$FLOW/lib/roborev-findings-count.sh" > "$FLOW_PARTLIB/lib/roborev-findings-count.sh"
fi
# AFFIRM ALL THREE HALVES, in a subshell: sources OK, entry point present, helper absent.
partlib_src=0; partlib_entry=0; partlib_helper_absent=0
if [ -s "$FLOW_PARTLIB/lib/roborev-findings-count.sh" ]; then
  ( . "$FLOW_PARTLIB/lib/roborev-findings-count.sh" ) >/dev/null 2>&1 && partlib_src=1
  [ "$( ( . "$FLOW_PARTLIB/lib/roborev-findings-count.sh" >/dev/null 2>&1; type -t roborev_findings_count ) 2>/dev/null )" = function ] && partlib_entry=1
  [ "$( ( . "$FLOW_PARTLIB/lib/roborev-findings-count.sh" >/dev/null 2>&1; type -t roborev_findings_block ) 2>/dev/null )" != function ] && partlib_helper_absent=1
fi
if [ "$partlib_ready" -ne 1 ]; then
  bad "recogniser-partial fixture: could not stage the substitute flow directory"
elif [ "$partlib_src" -eq 1 ] && [ "$partlib_entry" -eq 1 ] && [ "$partlib_helper_absent" -eq 1 ]; then
  ok "recogniser-partial fixture: the library SOURCES CLEANLY, defines the entry point, and is MISSING roborev_findings_block"
  pr_payload_with_comment "$MOCK_GH_DIR/pr.json" main "$(roborev_block 646)" pmcfadin \
    "$(defer_marker 3602,3613 2 "$MB_MAIN" "$HEAD_AFTER" 646 'both filed and lead-deferred')"
  roborev_job 646 "$MB_MAIN" "$HEAD_AFTER" F done 2026-09-02T10:00:00Z "$FC_REVIEW2"
  PL_OUT=$(cd "$WORK" && PATH="$BIN:$PATH" bash "$FLOW_PARTLIB/premerge-review-binding.sh" \
    review-binding 1 o/r "$HEAD_AFTER" 2>&1)
  PL_RC=$?
  if [ "$PL_RC" -ne 5 ]; then
    bad "4050(l): an INCOMPLETE shared recogniser did not refuse UNMEASURED (exit $PL_RC, wanted 5): $PL_OUT"
  else
    ok "4050(l): an INCOMPLETE shared recogniser is UNMEASURED (exit 5)"
    # THE ASSERTION MUST BE ANCHORED, AND MEASURING THAT COST A ROUND. A bare substring
    # test for the function name PASSED WITH THE FIX REVERTED: the unfixed path calls the
    # missing helper, and bash's OWN unanchored `roborev_findings_block: command not found`
    # lands in this captured output — so the assertion was satisfied by the DEFECT'S NOISE
    # rather than by the leg's diagnosis. That is #3400's lesson at a test site: key on the
    # narrowest thing that makes it OURS. `say` prefixes every line with `PREMERGE:
    # REVIEW-BINDING`, which bash's stderr can never produce, so the anchored form
    # discriminates the two states where the substring cannot.
    if grep -qE '^PREMERGE:.*roborev_findings_block' <<<"$PL_OUT"; then
      ok "4050(l): the leg's OWN anchored cause NAMES the missing function, so the operator is not sent to a library that loads fine"
    else
      bad "4050(l): no anchored PREMERGE: line named the missing function (got: $PL_OUT)"
    fi
    case "$PL_OUT" in
      *BOUND*) bad "4050(l): a run with an incomplete recogniser still BOUND (got: $PL_OUT)" ;;
      *) ok "4050(l): no bind is possible with an incomplete recogniser" ;;
    esac
  fi
else
  bad "recogniser-partial fixture: not in the expected state (start='$partlib_start' src=$partlib_src entry=$partlib_entry helper-absent=$partlib_helper_absent)"
fi

# --- (f) an ABSENT shared recogniser is UNMEASURED, never a bind and never a skip
# The library is resolved from this script's OWN `lib/` directory with no override, so the
# case SUBSTITUTES THE ARTIFACT in a scratch flow copy rather than pointing a variable
# somewhere — a path variable would be one more seam a real invoker could set. It must
# refuse UNMEASURED (an absent library says NOTHING about whether a human authorized the
# deferral) and it must NAME the library, or the operator cannot act on it.
FLOW_NOLIB="$T/scripts/flow-nolib"   # beside scripts/ci, which the leg resolves as ../ci
mkdir -p "$FLOW_NOLIB"
nolib_ready=1
for f in premerge-review-binding.sh premerge-pr-scan.py roborev-job-facts.py \
  roborev-waiver-scan.py roborev-review-oracles.sh base-staleness.sh; do
  cp "$FLOW/$f" "$FLOW_NOLIB/$f" || nolib_ready=0
done
chmod +x "$FLOW_NOLIB"/*.sh "$FLOW_NOLIB"/*.py 2>/dev/null
# Measured AFFIRMATIVELY: the SIBLING must be present (the copy ran, the directory reads)
# while the library is not, so an absent verdict cannot come from a mis-staged fixture.
nolib_sib=0
nolib_gone=0
[ -f "$FLOW_NOLIB/premerge-review-binding.sh" ] && nolib_sib=1
[ -e "$FLOW_NOLIB/lib/roborev-findings-count.sh" ] || nolib_gone=1
if [ "$nolib_ready" -ne 1 ]; then
  bad "recogniser-absent fixture: could not stage the substitute flow directory"
elif [ "$nolib_sib" -eq 1 ] && [ "$nolib_gone" -eq 1 ]; then
  ok "recogniser-absent fixture: the substitute reads (sibling present) and the shared recogniser is absent"
  pr_payload_with_comment "$MOCK_GH_DIR/pr.json" main "$(roborev_block 646)" pmcfadin \
    "$(defer_marker 3602,3613 2 "$MB_MAIN" "$HEAD_AFTER" 646 'both filed and lead-deferred')"
  roborev_job 646 "$MB_MAIN" "$HEAD_AFTER" F done 2026-09-02T10:00:00Z "$FC_REVIEW2"
  NL_OUT=$(cd "$WORK" && PATH="$BIN:$PATH" bash "$FLOW_NOLIB/premerge-review-binding.sh" \
    review-binding 1 o/r "$HEAD_AFTER" 2>&1)
  NL_RC=$?
  if [ "$NL_RC" -ne 5 ]; then
    bad "4050(f): an absent shared recogniser did not refuse UNMEASURED (exit $NL_RC, wanted 5): $NL_OUT"
  else
    ok "4050(f): an absent shared recogniser is UNMEASURED (exit 5) — it says nothing about the authorization"
    case "$NL_OUT" in
      *"lib/roborev-findings-count.sh"*)
        ok "4050(f): the cause NAMES the library, so a broken checkout is actionable" ;;
      *) bad "4050(f): the cause did not name the missing library (got: $NL_OUT)" ;;
    esac
    case "$NL_OUT" in
      *"verdict BOUND"*)
        bad "4050(f): a run that could not load the recogniser still BOUND (got: $NL_OUT)" ;;
      *) ok "4050(f): no bind is possible without the recogniser" ;;
    esac
  fi
  # POSITIVE CONTROL, one property different: the SAME fixture through the COMPLETE flow copy
  # binds. Without it, the refusal above could be caused by anything in the substitute tree.
  if run_binding 0 "4050(f) control: the same fixture through the COMPLETE flow copy BINDS" \
    review-binding 1 o/r "$HEAD_AFTER"; then
    case "$OUT" in
      *"verdict BOUND"*)
        ok "4050(f) control: the refusal above is attributable to the missing library alone" ;;
      *) bad "4050(f) control: the complete copy did not bind, so the refusal is unattributable (got: $OUT)" ;;
    esac
  fi
else
  bad "recogniser-absent fixture: the substitute was not in the expected state (sibling=$nolib_sib absent=$nolib_gone)"
fi

# --- (g) a READABLE but CORRUPT shared recogniser is UNMEASURED too -----------
# THE STATE (f)'s GUARD ADMITS (roborev job 123). A truncated library is `-f` AND `-r`, so
# the readability guard lets it through, and `.` then fails on a SYNTAX ERROR.
#
# WHAT THIS CASE DOES *NOT* CLAIM, because it was measured and is false here: there is no
# dead-shell hazard at THIS call site. premerge-review-binding.sh sets `-uo pipefail` with
# no `-e`, and premerge-assert.sh EXECUTES it (`bash "$REVIEW_BINDING_TOOL"`), so a bare
# source would return non-zero, continue, and be caught downstream. Reverting the
# conditional leaves every assertion below PASSING — verified — so this case has NO TEETH
# against that regression and must not be described as if it had. The fatal version of this
# hazard is real one file over (roborev-review-checks.sh, sourced under roborev-review.sh's
# `set -e`) and is pinned WITH teeth by case cor4050 in test_roborev_review_guard.sh, which
# goes from exit 1 to exit 2 when reverted.
#
# WHAT IT DOES PIN, and why it is still worth its lines: a corrupt library must refuse
# UNMEASURED with an ANCHORED verdict that NAMES the library, and must never bind. Those
# hold whatever the mechanism, so they are the properties a consumer depends on.
#
# WHERE THE TEETH ARE, NAMED (lead ruling on #4050 §4). A declaration that says only what
# it CANNOT detect leaves a reader to conclude the property is uncovered, which is false —
# so this note must point at its own complement. The bare-source regression IS pinned, with
# measured teeth, by case `cor4050` in scripts/tests/test_roborev_review_guard.sh: reverting
# the checks-side conditional moves it from `RESULT: FAIL` + exit 1 to exit 2, bash's
# syntax-error death under roborev-review.sh's `set -e`, with the wrapper's own verdict
# never emitted. That file is where the fatal version of this hazard lives, because that
# consumer is SOURCED under `-e` while this one is EXECUTED without it. Read the two cases
# as a pair: this one covers the properties, that one covers the death.
FLOW_CORRUPT="$T/scripts/flow-corruptlib"
mkdir -p "$FLOW_CORRUPT/lib"
corrupt_ready=1
for f in premerge-review-binding.sh premerge-pr-scan.py roborev-job-facts.py \
  roborev-waiver-scan.py roborev-review-oracles.sh base-staleness.sh; do
  cp "$FLOW/$f" "$FLOW_CORRUPT/$f" || corrupt_ready=0
done
chmod +x "$FLOW_CORRUPT"/*.sh "$FLOW_CORRUPT"/*.py 2>/dev/null
# Truncate an open function body: the realistic corruption (a partial write, a cut-off
# copy) and a guaranteed bash syntax error.
printf 'roborev_findings_count() {\n  echo unterminated\n' > "$FLOW_CORRUPT/lib/roborev-findings-count.sh"
# AFFIRM THE FIXTURE IS THE ONE THIS CASE IS ABOUT: readable as a regular file AND
# unsourceable. A file that merely fails to source proves nothing about the guard it must
# get past, and one that sources cleanly would make the case vacuous.
corrupt_readable=0
corrupt_unsourceable=0
[ -f "$FLOW_CORRUPT/lib/roborev-findings-count.sh" ] && [ -r "$FLOW_CORRUPT/lib/roborev-findings-count.sh" ] && corrupt_readable=1
( . "$FLOW_CORRUPT/lib/roborev-findings-count.sh" ) >/dev/null 2>&1 || corrupt_unsourceable=1
if [ "$corrupt_ready" -ne 1 ]; then
  bad "recogniser-corrupt fixture: could not stage the substitute flow directory"
elif [ "$corrupt_readable" -eq 1 ] && [ "$corrupt_unsourceable" -eq 1 ]; then
  ok "recogniser-corrupt fixture: the recogniser is readable as a regular file AND fails to source — the state (f)'s guard admits"
  pr_payload_with_comment "$MOCK_GH_DIR/pr.json" main "$(roborev_block 647)" pmcfadin \
    "$(defer_marker 3602,3613 2 "$MB_MAIN" "$HEAD_AFTER" 647 'both filed and lead-deferred')"
  roborev_job 647 "$MB_MAIN" "$HEAD_AFTER" F done 2026-09-02T10:00:00Z "$FC_REVIEW2"
  CL_OUT=$(cd "$WORK" && PATH="$BIN:$PATH" bash "$FLOW_CORRUPT/premerge-review-binding.sh" \
    review-binding 1 o/r "$HEAD_AFTER" 2>&1)
  CL_RC=$?
  if [ "$CL_RC" -ne 5 ]; then
    bad "4050(g): a corrupt shared recogniser did not refuse UNMEASURED (exit $CL_RC, wanted 5): $CL_OUT"
  else
    ok "4050(g): a corrupt shared recogniser is UNMEASURED (exit 5) — the refusal a consumer must treat as non-binding"
  fi
  # THE ANCHOR IS THE POINT: a shell killed mid-source emits no anchored line at all, so
  # this is what separates "refused" from "died with a status that looks like a refusal".
  case "$CL_OUT" in
    *"PREMERGE: REVIEW-BINDING verdict UNMEASURED"*)
      ok "4050(g): it emits its ANCHORED verdict, so a consumer keying on the anchor sees a refusal" ;;
    *) bad "4050(g): no anchored UNMEASURED verdict on a corrupt recogniser (got: $CL_OUT)" ;;
  esac
  case "$CL_OUT" in
    *"lib/roborev-findings-count.sh"*)
      ok "4050(g): the cause NAMES the library, so a corrupt checkout is actionable" ;;
    *) bad "4050(g): the cause did not name the library (got: $CL_OUT)" ;;
  esac
  case "$CL_OUT" in
    *"verdict BOUND"*)
      bad "4050(g): a run that could not load the recogniser still BOUND (got: $CL_OUT)" ;;
    *) ok "4050(g): no bind is possible with an unsourceable recogniser" ;;
  esac
  # POSITIVE CONTROL, one property different: replace the corrupt library with the REAL one
  # in the SAME substitute tree and require a BIND. Without it the refusal above could be
  # caused by anything in the copy — the same reason (f) carries one.
  cp "$FLOW/lib/roborev-findings-count.sh" "$FLOW_CORRUPT/lib/roborev-findings-count.sh" 2>/dev/null
  CG_OUT=$(cd "$WORK" && PATH="$BIN:$PATH" bash "$FLOW_CORRUPT/premerge-review-binding.sh" \
    review-binding 1 o/r "$HEAD_AFTER" 2>&1)
  case "$CG_OUT" in
    *"verdict BOUND"*)
      ok "4050(g) control: the same tree with an INTACT recogniser BINDS, so the refusal is attributable to the corruption alone" ;;
    *) bad "4050(g) control: the repaired tree did not bind, so the refusal is unattributable (got: $CG_OUT)" ;;
  esac
else
  bad "recogniser-corrupt fixture: not in the expected state (readable=$corrupt_readable unsourceable=$corrupt_unsourceable)"
fi

# --- (h)(i) #4090: an ABSENT recogniser must NOT affect paths that need no count ----
# LEAD RULING on roborev job 125 (#4050 §5): the recogniser used to load unconditionally in
# the preflight, so a missing or corrupt library refused questions it was never needed for.
# It is now loaded LAZILY inside the `findings)` arm. These two cases pin the paths that
# were wrongly affected; 4050(f) above already pins that the FINDINGS path still refuses.
#
# BOTH RUN THROUGH `$FLOW_NOLIB`, the substitute flow copy whose library was removed for
# case (f). Its state is RE-ASSERTED here rather than assumed: (f) is upstream in this file
# and a future edit could restore the library, which would make both cases pass while
# exercising nothing.
nolib_still_gone=0
nolib_still_sib=0
[ -f "$FLOW_NOLIB/premerge-review-binding.sh" ] && nolib_still_sib=1
[ -e "$FLOW_NOLIB/lib/roborev-findings-count.sh" ] || nolib_still_gone=1
if [ "$nolib_still_sib" -ne 1 ] || [ "$nolib_still_gone" -ne 1 ]; then
  bad "4090 fixture: \$FLOW_NOLIB is not in the expected state (sibling=$nolib_still_sib absent=$nolib_still_gone), so neither case below exercised an absent recogniser"
else
  ok "4090 fixture: the substitute flow copy still reads and its recogniser is still absent"

  # (h) A CODE-FREE PR diff needs no findings count at all. Its correct answer is the
  # loudly DECLARED NOT-APPLICABLE, and an absent recogniser must not turn that into
  # UNMEASURED. Exit 0, same as the library-present case above.
  pr_payload "$MOCK_GH_DIR/pr.json" main "no review recorded here"
  H_OUT=$(cd "$WORK" && PATH="$BIN:$PATH" bash "$FLOW_NOLIB/premerge-review-binding.sh" \
    review-binding 1 pmcfadin/cqlite "$PROSE_ONLY" 2>&1)
  H_RC=$?
  if [ "$H_RC" -ne 0 ]; then
    bad "4090(h): a code-free diff did not stay NOT-APPLICABLE with the recogniser absent (exit $H_RC, wanted 0): $H_OUT"
  else
    ok "4090(h): a code-free diff is still NOT-APPLICABLE (exit 0) with the recogniser absent"
  fi
  case "$H_OUT" in
    *"verdict NOT-APPLICABLE"*)
      ok "4090(h): and it is the DECLARED exemption, not some other exit-0 answer" ;;
    *) bad "4090(h): expected 'verdict NOT-APPLICABLE' (got: $H_OUT)" ;;
  esac
  case "$H_OUT" in
    *"roborev-findings-count.sh"*)
      bad "4090(h): a code-free diff still mentions the recogniser, so it is still being loaded on this path (got: $H_OUT)" ;;
    *) ok "4090(h): the recogniser is not even NAMED on the code-free path — it was never loaded" ;;
  esac

  # (i) A CLEAN recorded review binds through the STRUCTURED verdict letter and never asks
  # how many findings there were. An absent recogniser must not turn a legitimate bind into
  # UNMEASURED. Default verdict for `roborev_job` is `P`, i.e. clean.
  pr_payload_with_comment "$MOCK_GH_DIR/pr.json" main "$(roborev_block 648)" pmcfadin "irrelevant"
  roborev_job 648 "$MB_MAIN" "$HEAD_AFTER"
  I_OUT=$(cd "$WORK" && PATH="$BIN:$PATH" bash "$FLOW_NOLIB/premerge-review-binding.sh" \
    review-binding 1 o/r "$HEAD_AFTER" 2>&1)
  I_RC=$?
  if [ "$I_RC" -ne 0 ]; then
    bad "4090(i): a CLEAN record did not bind with the recogniser absent (exit $I_RC, wanted 0): $I_OUT"
  else
    ok "4090(i): a CLEAN record still BINDS (exit 0) with the recogniser absent — it needs no count"
  fi
  case "$I_OUT" in
    *"affirmatively CLEAN"*)
      ok "4090(i): and it binds for the right reason — the structured verdict, not a count" ;;
    *) bad "4090(i): the bind did not cite the structured CLEAN verdict (got: $I_OUT)" ;;
  esac
fi
reset_stub 2>/dev/null || true

# --- (j) #4050 job 126: an OLDER findings round must not sink a NEWER CLEAN one ------
# THE DEFECT THE LAZY LOAD INTRODUCED. `record_covering` collects per-record outcomes and
# decides ONCE afterwards from the LATEST covering round, so anything that EXITS mid-scan
# discards rounds not yet examined. The lazy `load_findings_count_lib` did exactly that:
# with the recogniser absent, an OLDER FINDINGS record examined first killed the process,
# and a NEWER CLEAN round — which binds through the structured verdict and needs no count —
# never got to decide. That is job 78's F2 defect (a newer favourable result lost to an
# earlier record) arriving through a process exit instead of a `break`.
#
# THE DECIDING ROUND HERE NEEDS NO RECOGNISER, so the absent library must not matter:
# expected BIND (exit 0), through `$FLOW_NOLIB`.
if [ "$nolib_still_sib" -eq 1 ] && [ "$nolib_still_gone" -eq 1 ]; then
  # Two covering rounds at the SAME head, ordered by `started_at`: 650 FINDINGS (older),
  # 651 CLEAN (newer). Both blocks on the PR so both are discovered.
  pr_payload_with_comment "$MOCK_GH_DIR/pr.json" main \
    "$(roborev_block 650)
$(roborev_block 651)" pmcfadin "no marker needed"
  roborev_job 650 "$MB_MAIN" "$HEAD_AFTER" F done 2026-09-02T10:00:00Z "$FC_REVIEW2"
  roborev_job 651 "$MB_MAIN" "$HEAD_AFTER" P done 2026-09-02T11:00:00Z
  J_OUT=$(cd "$WORK" && PATH="$BIN:$PATH" bash "$FLOW_NOLIB/premerge-review-binding.sh" \
    review-binding 1 o/r "$HEAD_AFTER" 2>&1)
  J_RC=$?
  if [ "$J_RC" -ne 0 ]; then
    bad "4050(j): an older FINDINGS round sank a newer CLEAN one when the recogniser was absent (exit $J_RC, wanted 0): $J_OUT"
  else
    ok "4050(j): the newer CLEAN round still decides and BINDS (exit 0) with the recogniser absent"
  fi
  # AFFIRMATIVE: the older findings round must be REPORTED as examined-and-nonbinding, not
  # silently skipped. If it never appears the fixture did not exercise the ordering at all.
  case "$J_OUT" in
    *"job 650"*) ok "4050(j): the older FINDINGS round WAS examined — the ordering is genuinely exercised" ;;
    *) bad "4050(j): job 650 is absent from the output, so the mixed-ordering path was not exercised (got: $J_OUT)" ;;
  esac
  case "$J_OUT" in
    *"affirmatively CLEAN"*) ok "4050(j): and it binds on the structured CLEAN verdict, which needs no count" ;;
    *) bad "4050(j): the bind did not cite the structured CLEAN verdict (got: $J_OUT)" ;;
  esac
else
  bad "4050(j): \$FLOW_NOLIB was not in the expected state, so the mixed-ordering case did not run"
fi
reset_stub 2>/dev/null || true

# --- G3: an authorized deferral naming a CLOSED issue must NOT bind -----------
# `gh issue view` EXITS 0 for a closed issue, so a number-only test made "the
# finding is tracked" satisfiable by an issue closed as a duplicate weeks ago —
# the finding permanently untracked while the block asserted it was filed.
pr_payload_with_comment "$MOCK_GH_DIR/pr.json" main "$(roborev_block 614)" pmcfadin \
  "$(defer_marker 3602 1 "$MB_MAIN" "$HEAD_AFTER" 614 'filed and lead-deferred')"
roborev_job 614 "$MB_MAIN" "$HEAD_AFTER" F
issue_state_fixture 3602 CLOSED
if run_binding 4 "result: a deferral naming a CLOSED issue does not bind" \
  review-binding 1 o/r "$HEAD_AFTER"; then
  case "$OUT" in
    *"ISSUE-CLOSED"*)
      ok "result: the CLOSED refusal is named as its own state, not folded into 'unauthorized'" ;;
    *) bad "result: a closed tracking issue was accepted or misnamed (got: $OUT)" ;;
  esac
fi

# --- G3: an authorized deferral naming a NON-EXISTENT issue must NOT bind -----
pr_payload_with_comment "$MOCK_GH_DIR/pr.json" main "$(roborev_block 615)" pmcfadin \
  "$(defer_marker 4242 1 "$MB_MAIN" "$HEAD_AFTER" 615 'filed and lead-deferred')"
roborev_job 615 "$MB_MAIN" "$HEAD_AFTER" F
issue_state_fixture 4242 '!ABSENT'
if run_binding 4 "result: a deferral naming a NON-EXISTENT issue does not bind" \
  review-binding 1 o/r "$HEAD_AFTER"; then
  case "$OUT" in
    *"ISSUE-ABSENT"*)
      ok "result: ABSENT is distinguished from could-not-ask, though gh exits 1 for both" ;;
    *) bad "result: a non-existent tracking issue was accepted or misnamed (got: $OUT)" ;;
  esac
fi

# --- job 102: an issue whose state could NOT BE ASKED is UNMEASURED, not UNBOUND
# No fixture at all => the mock fails with a diagnostic that does NOT say the
# issue is missing. That is a could-not-ask, and a could-not-ask never grants.
#
# THIS CASE PREVIOUSLY EXPECTED EXIT 4, and that was the defect roborev job 102
# found: an unreachable `gh` was reported as "no authorized deferral covers this
# job", which is a WRONG REMEDY — it sends a lead to re-post a marker that was
# already fine when the fix is restoring GitHub access. `absent` and `closed`
# are answers GitHub GAVE (exit 4); this is GitHub not answering (exit 5). Both
# refuse the merge — premerge-assert maps 4 and 5 alike to its exit-2 refusal —
# so the change is to the DIAGNOSIS, never to whether the merge is blocked.
pr_payload_with_comment "$MOCK_GH_DIR/pr.json" main "$(roborev_block 616)" pmcfadin \
  "$(defer_marker 3777 1 "$MB_MAIN" "$HEAD_AFTER" 616 'filed and lead-deferred')"
roborev_job 616 "$MB_MAIN" "$HEAD_AFTER" F
rm -f "$MOCK_GH_DIR/issue-3777.json"
if run_binding 5 "result: an UNASKABLE issue state is UNMEASURED, not a measured refusal" \
  review-binding 1 o/r "$HEAD_AFTER"; then
  case "$OUT" in
    *"could NOT BE ASKED"*)
      ok "result: the cause names GitHub not answering, not an unauthorized deferral" ;;
    *) bad "result: the unmeasured cause did not name what could not be asked (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"verdict UNMEASURED"*)
      ok "result: a could-not-ask carries the UNMEASURED verdict token" ;;
    *) bad "result: expected the UNMEASURED token for an unaskable issue (got: $OUT)" ;;
  esac
  # THE REMEDY IS THE POINT OF THE FIX, so it is asserted, not assumed.
  case "$OUT" in
    *"Do NOT re-post a deferral marker"*)
      ok "result: the unmeasured remedy tells the operator NOT to re-post a marker or re-triage" ;;
    *) bad "result: the unmeasured cause carries the wrong remedy (got: $OUT)" ;;
  esac
fi

# --- job 102: an ABSENT authorization SCANNER is UNMEASURED -------------------
# The scanner path is resolved from the script's OWN directory with no env
# override, deliberately (#3312: the constrained party must not choose its own
# enforcer). So this case SUBSTITUTES THE ARTIFACT — a second scratch flow dir
# with the scanner removed — rather than pointing a variable somewhere, which
# would be one more thing a real invoker could set.
FLOW_NOSCAN="$T/scripts/flow-noscan"   # beside scripts/ci, which the leg resolves as ../ci
mkdir -p "$FLOW_NOSCAN"
noscan_ready=1
for f in premerge-review-binding.sh premerge-pr-scan.py roborev-job-facts.py \
  roborev-review-oracles.sh base-staleness.sh; do
  cp "$FLOW/$f" "$FLOW_NOSCAN/$f" || noscan_ready=0
done
mkdir -p "$FLOW_NOSCAN/lib"
cp "$FLOW/lib/roborev-findings-count.sh" "$FLOW_NOSCAN/lib/" || noscan_ready=0
chmod +x "$FLOW_NOSCAN"/*.sh "$FLOW_NOSCAN"/*.py 2>/dev/null
# The absence is measured AFFIRMATIVELY, not with a bare `[ ! -f ]`: a plain
# negative file test folds "the directory is unreadable" onto "the file is
# absent", which is the two-valued collapse this whole suite exists to refuse.
# So the SIBLING must be present (proving the copy ran and the directory reads)
# while the scanner is not.
noscan_sib=0
noscan_gone=0
[ -f "$FLOW_NOSCAN/premerge-review-binding.sh" ] && noscan_sib=1
[ -e "$FLOW_NOSCAN/roborev-waiver-scan.py" ] || noscan_gone=1
if [ "$noscan_ready" -ne 1 ]; then
  bad "scanner-absent fixture: could not stage the substitute flow directory"
elif [ "$noscan_sib" -eq 1 ] && [ "$noscan_gone" -eq 1 ]; then
  ok "scanner-absent fixture: the substitute reads (sibling present) and the scanner is absent"
  pr_payload_with_comment "$MOCK_GH_DIR/pr.json" main "$(roborev_block 617)" pmcfadin \
    "$(defer_marker 3602 1 "$MB_MAIN" "$HEAD_AFTER" 617 'filed and lead-deferred')"
  roborev_job 617 "$MB_MAIN" "$HEAD_AFTER" F
  issue_state_fixture 3602 OPEN
  OUT=$(cd "$WORK" && PATH="$BIN:$PATH" bash "$FLOW_NOSCAN/premerge-review-binding.sh" \
    review-binding 1 o/r "$HEAD_AFTER" 2>&1)
  RC=$?
  if [ "$RC" -ne 5 ]; then
    bad "result: an absent authorization scanner should be UNMEASURED (exit $RC, wanted 5)"
    printf '     output: %s\n' "$OUT"
  else
    case "$OUT" in
      *"deferral scanner is absent"*)
        ok "result: an absent authorization scanner is UNMEASURED and names itself" ;;
      *) bad "result: the absent-scanner cause did not name the scanner (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"no authorized deferral covers"*)
        bad "result: an absent scanner was reported as an UNAUTHORIZED deferral — job 102's conflation" ;;
      *)
        ok "result: an absent scanner is NOT reported as an unauthorized deferral" ;;
    esac
  fi
else
  bad "scanner-absent fixture: could not verify the scanner is absent from the substitute"
fi

# --- job 102 (control): a scanner that RUNS and refuses stays UNBOUND ---------
# The counterpart to the two cases above: when the oracle IS available and says
# no, that is a measurement, and it must keep its exit-4 refusal. Without this
# control the fix could have made every deferral path unmeasured.
pr_payload_with_comment "$MOCK_GH_DIR/pr.json" main "$(roborev_block 618)" stranger \
  "$(defer_marker 3602 1 "$MB_MAIN" "$HEAD_AFTER" 618 'filed and lead-deferred')"
roborev_job 618 "$MB_MAIN" "$HEAD_AFTER" F
issue_state_fixture 3602 OPEN
if run_binding 4 "result: a scanner that RUNS and refuses is UNBOUND, not UNMEASURED" \
  review-binding 1 o/r "$HEAD_AFTER"; then
  case "$OUT" in
    *"verdict UNBOUND"*)
      ok "result: an evaluated-and-refused authorization keeps its MEASURED refusal" ;;
    *) bad "result: an evaluated refusal was not UNBOUND (got: $OUT)" ;;
  esac
fi

# --- the same marker from a NON-allowlisted author must NOT bind --------------
pr_payload_with_comment "$MOCK_GH_DIR/pr.json" main "$(roborev_block 613)" stranger \
  "$(defer_marker 3602 1 "$MB_MAIN" "$HEAD_AFTER" 613 'let me merge this please')"
roborev_job 613 "$MB_MAIN" "$HEAD_AFTER" F
if run_binding 4 "result: a deferral from a NON-allowlisted author does not bind" \
  review-binding 1 o/r "$HEAD_AFTER"; then
  case "$OUT" in
    *"verdict UNBOUND"*)
      ok "result: authorship is what separates an authorization from a stranger's comment" ;;
    *) bad "result: a stranger's deferral marker was honoured (got: $OUT)" ;;
  esac
fi

# --- a deferral naming a DIFFERENT review must NOT bind (scope binding) -------
pr_payload_with_comment "$MOCK_GH_DIR/pr.json" main "$(roborev_block 614)" pmcfadin \
  "$(defer_marker 3602 1 "$MB_MAIN" "$REVIEWED_PRE" 614 'authorized for the pre-rebase head')"
roborev_job 614 "$MB_MAIN" "$HEAD_AFTER" F
if run_binding 4 "result: a deferral naming a DIFFERENT head does not bind" \
  review-binding 1 o/r "$HEAD_AFTER"; then
  case "$OUT" in
    *"verdict UNBOUND"*)
      ok "result: a deferral may not outlive the review its authorizer judged" ;;
    *) bad "result: a stale-scoped deferral was honoured (got: $OUT)" ;;
  esac
fi

# --- the BLOCK's own verdict is REPORTED and never DERIVED FROM ---------------
# A block claiming a passing verdict cannot rescue a record that says findings:
# the two must stay textually distinct so a pasted log cannot be read as the
# other, and only the RECORD decides.
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 615)"
roborev_job 615 "$MB_MAIN" "$HEAD_AFTER" F
if run_binding 4 "result: a PASSing BLOCK cannot rescue a FINDINGS record" \
  review-binding 1 o/r "$HEAD_AFTER"; then
  case "$OUT" in
    *"the BLOCK's own claim"*)
      ok "result: the block's verdict is labelled as the block's claim, not as the decision" ;;
    *) bad "result: the block verdict was not labelled as untrusted (got: $OUT)" ;;
  esac
fi

# --- the leg no longer CLAIMS the residual it used to declare -----------------
assert_src_absent_fixed \
  "result: the header no longer declares the recorded verdict an unenforced residual" \
  "result: the header still declares the verdict a residual, which the fix falsified" \
  "$BINDING" "declared residual"
assert_src_present_fixed \
  "result: the enforcer and its allowlist are resolved with no env override (#3312)" \
  "result: the deferral enforcer is not resolved from the script's own directory" \
  "$BINDING" 'WAIVER_SCAN_TOOL="$OWN_DIR/roborev-waiver-scan.py"'
assert_src_absent \
  "result: no test-only seam into the deferral enforcer or its allowlist" \
  "result: the deferral enforcer or allowlist is overridable from the environment" \
  "$BINDING" '(WAIVER_SCAN_TOOL|ORACLES_FILE)=.*\$\{(WAIVER_SCAN_TOOL|ORACLES_FILE)' code

# ==============================================================================
# FINDING 4 (roborev job 59) — BASH 3.2 PORTABILITY
# ==============================================================================
# `mapfile`/`readarray` is bash 4+ and this repo states bash 3.2 support (stock
# macOS ships 3.2.57), so the enrolled suite silently degraded to "unmeasurable"
# on a SUPPORTED host. And `"${arr[@]}"` on an EMPTY array ABORTS under `set -u`
# on 3.2 — at the one site (`heads`) that is empty exactly when every job record
# was unretrievable, i.e. the leg exited with NO VERDICT on its fail-closed path.
#
# STRUCTURAL, because the defect is invisible on this host: bash 5 runs both
# spellings happily, so no behavioural case on this box can distinguish them.
for _f in "$REPO_ROOT/tests/lib/tristate-file-probe.bash" \
  "$REPO_ROOT/flow/premerge-review-binding.sh"; do
  assert_src_absent \
    "portability: ${_f##*/} uses no bash-4-only mapfile/readarray builtin" \
    "portability: ${_f##*/} still calls mapfile/readarray, which is absent on bash 3.2" \
    "$_f" '^[[:space:]]*(mapfile|readarray)[[:space:]]' code
  assert_src_absent \
    "portability: ${_f##*/} has no unguarded \"\${arr[@]}\" expansion" \
    "portability: ${_f##*/} expands an array unguarded, which aborts under set -u on bash 3.2" \
    "$_f" 'for [A-Za-z_]+ in "\$\{[A-Za-z_]+\[@\]\}"' code
done
unset _f

# BEHAVIOURAL: the consequence the finding names — with EVERY recorded job
# unretrievable, `heads` is empty and the documented UNMEASURED verdict must
# still be emitted rather than the shell aborting mid-unwind.
pr_payload "$MOCK_GH_DIR/pr.json" main "$(roborev_block 901)$(roborev_block 902)"
rm -f "$MOCK_ROBOREV_DIR/job-901.json" "$MOCK_ROBOREV_DIR/job-902.json" \
  "$MOCK_ROBOREV_DIR/list.json"
if run_binding 5 "portability: EVERY job record unretrievable still REACHES a verdict" \
  review-binding 1 o/r "$HEAD_AFTER"; then
  case "$OUT" in
    *"verdict UNMEASURED"*)
      ok "portability: the all-unretrievable run prints its UNMEASURED refusal (empty heads[])" ;;
    *) bad "portability: expected the UNMEASURED verdict on an empty heads[] (got: $OUT)" ;;
  esac
fi

# --- CASE FLOOR (#3544) ---------------------------------------------------------------
# A span-replacing edit that silently deletes cases leaves a GREEN tally over a
# SHRUNKEN suite. The floor is what makes that a red.
CASE_FLOOR=191
TOTAL=$((PASSED + FAILED))
if [ "$TOTAL" -lt "$CASE_FLOOR" ]; then
  bad "case floor: only $TOTAL assertions ran, below the committed floor of $CASE_FLOOR — cases were deleted"
  FAILED=$((FAILED + 1))
else
  ok "case floor: $TOTAL assertions ran, at or above the committed floor of $CASE_FLOOR"
fi

printf '\n=== premerge-review-binding: %d passed, %d failed ===\n' "$PASSED" "$FAILED"
[ "$FAILED" -eq 0 ]
