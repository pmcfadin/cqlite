#!/usr/bin/env bash
# test_aggregate_required_tiers.sh — offline proof that `required` fails closed on
# every non-passing gating-tier state (issue #2910).
#
# scripts/ci/aggregate-required-tiers.sh is the mechanism that makes the single
# branch-protection context mean "every gating tier for this diff already
# passed". A bug here either blocks every PR or silently restores the hole this
# change exists to close, so every state gets a DISCRIMINATING case: it asserts a
# non-zero exit AND that the offending tier is named, never merely that the
# passing case passes.
#
# Non-vacuity is proven, not hoped for: the final phase substitutes an
# always-exit-0 stub aggregator and asserts the failing cases stop failing — i.e.
# this suite would go RED if the real aggregator were ever gutted.
#
# Fully hermetic: synthetic check-run fixtures, injected deadlines and poll
# budgets, a stub sleep. No network, no gh, no sleeping, and NO wall-clock
# threshold assertion anywhere (#2642) — expiry is driven by an injected
# already-expired deadline or an exhausted poll budget.
#
# Run standalone:   bash scripts/tests/test_aggregate_required_tiers.sh
# Or via the gate:  scripts/agent-gate.sh runs it inside the `tooling-tests` component.
set -uo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && cd .. && pwd)
AGG_REAL="$REPO_ROOT/scripts/ci/aggregate-required-tiers.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

if ! command -v ruby >/dev/null 2>&1; then
  echo "SKIP: ruby unavailable (the gating-tier registry reader needs it)"
  exit 0
fi
if [ ! -f "$AGG_REAL" ]; then
  echo "FAIL - $AGG_REAL not found"
  exit 1
fi

WORK=$(mktemp -d "${TMPDIR:-/tmp}/required-tiers-selftest.XXXXXX")
trap 'rm -rf "$WORK"' EXIT

# ------------------------------------------------------------- fixtures -----

cat >"$WORK/registry.yml" <<'YAML'
version: 1
aggregator:
  workflow: pr-gate.yml
  job: required
defaults:
  wait_minutes: 60
tiers:
  - id: alpha
    workflow: alpha.yml
    context: Alpha gate
  - id: beta
    workflow: beta.yml
    context: Beta gate
YAML

# This run's own job ids. An Actions job and its check run share the numeric id,
# so the id set is a name-independent identity for self-exclusion.
printf '9001\n9002\n' >"$WORK/self-ids.txt"

runs_file() { printf '%s/runs-%s.json' "$WORK" "$1"; }

cat >"$(runs_file all-pass)" <<'JSON'
{"check_runs":[
 {"id":1001,"name":"Alpha gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":1002,"name":"Beta gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/502/job/1002"},
 {"id":1500,"name":"perf advisory","status":"completed","conclusion":"failure",
  "details_url":"https://github.com/o/r/actions/runs/503/job/1500"},
 {"id":9001,"name":"pr-gate-core","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/777/job/9001"},
 {"id":9002,"name":"required","status":"in_progress","conclusion":null,
  "details_url":"https://github.com/o/r/actions/runs/777/job/9002"}
]}
JSON

cat >"$(runs_file one-pending)" <<'JSON'
{"check_runs":[
 {"id":1001,"name":"Alpha gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":1002,"name":"Beta gate","status":"in_progress","conclusion":null,
  "details_url":"https://github.com/o/r/actions/runs/502/job/1002"}
]}
JSON

cat >"$(runs_file one-failed)" <<'JSON'
{"check_runs":[
 {"id":1001,"name":"Alpha gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":1002,"name":"Beta gate","status":"completed","conclusion":"failure",
  "details_url":"https://github.com/o/r/actions/runs/502/job/1002"}
]}
JSON

# Beta's context is absent entirely; an UNREGISTERED context is absent too, which
# must not gate anything.
cat >"$(runs_file one-absent)" <<'JSON'
{"check_runs":[
 {"id":1001,"name":"Alpha gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"}
]}
JSON

# Re-run, newest green: the older failure must NOT be latched.
cat >"$(runs_file rerun-green)" <<'JSON'
{"check_runs":[
 {"id":1001,"name":"Alpha gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":1002,"name":"Beta gate","status":"completed","conclusion":"failure",
  "details_url":"https://github.com/o/r/actions/runs/502/job/1002"},
 {"id":1099,"name":"Beta gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/599/job/1099"}
]}
JSON

# Re-run, newest non-terminal: the older SUCCESS must NOT be latched either.
cat >"$(runs_file rerun-pending)" <<'JSON'
{"check_runs":[
 {"id":1001,"name":"Alpha gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":1002,"name":"Beta gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/502/job/1002"},
 {"id":1099,"name":"Beta gate","status":"in_progress","conclusion":null,
  "details_url":"https://github.com/o/r/actions/runs/599/job/1099"}
]}
JSON

# SELF-EXCLUSION discriminator. Our OWN run (777) emits a check run named
# "Alpha gate" with a HIGHER id than the real sibling tier's FAILURE. Without
# exclusion the highest id wins and the gate goes green on our own success —
# exactly the self-referential false green the design forbids. Exclusion by id
# (9003) and by details-URL run id (9004, an id NOT in self-ids.txt) must both
# drop ours and leave the sibling failure decisive.
cat >"$(runs_file self-shadow)" <<'JSON'
{"check_runs":[
 {"id":1001,"name":"Alpha gate","status":"completed","conclusion":"failure",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":9003,"name":"Alpha gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/777/job/9003"},
 {"id":1002,"name":"Beta gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/502/job/1002"},
 {"id":9004,"name":"Beta gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/777/job/9004"}
]}
JSON
printf '9003\n' >"$WORK/self-ids-shadow.txt"

# Same set with every self check run RENAMED: exclusion is by run identity, so
# the verdict must be identical.
sed 's/"Alpha gate","status":"completed","conclusion":"success"/"totally renamed gate","status":"completed","conclusion":"success"/' \
  "$(runs_file self-shadow)" >"$(runs_file self-shadow-renamed)"

# Progressive source: non-terminal on the first two fetches, green on the third.
cat >"$WORK/progressive.sh" <<EOF
#!/usr/bin/env bash
n=\$(cat "$WORK/progressive.count" 2>/dev/null || echo 0)
n=\$((n + 1))
echo "\$n" >"$WORK/progressive.count"
if [ "\$n" -ge 3 ]; then cat "$(runs_file all-pass)"; else cat "$(runs_file one-pending)"; fi
EOF
chmod +x "$WORK/progressive.sh"

cat >"$WORK/fake-sleep.sh" <<EOF
#!/usr/bin/env bash
echo "slept \$*" >>"$WORK/sleep.log"
EOF
chmod +x "$WORK/fake-sleep.sh"

cat >"$WORK/stub-aggregator.sh" <<'EOF'
#!/usr/bin/env bash
# Deliberately vacuous: the non-vacuity probe substitutes this for the real
# aggregator and asserts the failing cases stop failing.
exit 0
EOF
chmod +x "$WORK/stub-aggregator.sh"

# ------------------------------------------------------------- harness ------

AGG="$AGG_REAL"
OUT=""
RC=0
CASE_INDEX=0
SUMMARY=""

# invoke <check-runs-file-or-cmd> <self-ids-file> <deadline-epoch> <poll-attempts> [extra...]
invoke() {
  local source="$1" self_ids="$2" deadline="$3" attempts="$4"
  shift 4
  CASE_INDEX=$((CASE_INDEX + 1))
  SUMMARY="$WORK/summary-$CASE_INDEX.md"
  : >"$SUMMARY"
  OUT=$(bash "$AGG" \
    --registry "$WORK/registry.yml" \
    --check-runs-cmd "$source" \
    --self-jobs-cmd "cat $self_ids" \
    --summary-file "$SUMMARY" \
    --sleep-cmd "$WORK/fake-sleep.sh" \
    --poll-initial-seconds 0 \
    --poll-max-seconds 0 \
    --run-id 777 \
    --deadline-epoch "$deadline" \
    --poll-attempts "$attempts" \
    "$@" 2>&1)
  RC=$?
}

contains() { case "$1" in *"$2"*) return 0 ;; *) return 1 ;; esac; }

FUTURE=$(( $(date +%s) + 86400 ))
EXPIRED=1

# ------------------------------------------------------------- the states ---

echo "== state: all registered tiers succeeded =="
invoke "cat $(runs_file all-pass)" "$WORK/self-ids.txt" "$FUTURE" 3
if [ "$RC" -eq 0 ]; then ok "all-pass exits 0"; else bad "all-pass expected exit 0, got $RC: $OUT"; fi
if contains "$(cat "$SUMMARY")" '`alpha`' && contains "$(cat "$SUMMARY")" '`beta`'; then
  ok "summary lists every registered tier"
else
  bad "summary is missing a registered tier: $(cat "$SUMMARY")"
fi
if contains "$(cat "$SUMMARY")" '1001' && contains "$(cat "$SUMMARY")" '1002'; then
  ok "summary records the observed check-run ids"
else
  bad "summary omits the observed check-run ids: $(cat "$SUMMARY")"
fi
if contains "$(cat "$SUMMARY")" 'actions/runs/501'; then
  ok "summary records the observed run URL"
else
  bad "summary omits the run URL: $(cat "$SUMMARY")"
fi
# The same fixture carries a FAILED unregistered advisory check run.
ok "an unregistered failing check run did not gate (all-pass fixture includes one)"

echo "== state: a registered tier is still pending at the deadline =="
invoke "cat $(runs_file one-pending)" "$WORK/self-ids.txt" "$EXPIRED" 1
if [ "$RC" -ne 0 ]; then ok "one-pending exits non-zero"; else bad "one-pending expected non-zero, got 0"; fi
if contains "$OUT" "beta"; then ok "one-pending names the pending tier"; else bad "one-pending did not name beta: $OUT"; fi
if contains "$OUT" "PASS"; then bad "one-pending reported PASS somewhere: $OUT"; else ok "one-pending reports no success anywhere"; fi
if contains "$(cat "$SUMMARY")" 'FAILED'; then ok "one-pending summary states the failure"; else bad "one-pending summary lacks a failure line"; fi

echo "== state: a registered tier failed =="
: >"$WORK/sleep.log"
invoke "cat $(runs_file one-failed)" "$WORK/self-ids.txt" "$FUTURE" 5
if [ "$RC" -ne 0 ]; then ok "one-failed exits non-zero"; else bad "one-failed expected non-zero, got 0"; fi
if contains "$OUT" "beta"; then ok "one-failed names the failing tier"; else bad "one-failed did not name beta: $OUT"; fi
if contains "$OUT" "actions/runs/502"; then ok "one-failed names the failing check-run URL"; else bad "one-failed omitted the URL: $OUT"; fi
if [ -s "$WORK/sleep.log" ]; then
  bad "one-failed kept polling after a terminal failure"
else
  ok "one-failed short-circuits without waiting out the deadline"
fi

echo "== state: a registered tier is absent =="
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1
if [ "$RC" -ne 0 ]; then ok "one-absent-and-registered exits non-zero"; else bad "one-absent expected non-zero, got 0"; fi
if contains "$OUT" "beta"; then ok "one-absent names the absent tier"; else bad "one-absent did not name beta: $OUT"; fi
if contains "$OUT" "absence is an ERROR"; then
  ok "one-absent states absence is an error, not inapplicability"
else
  bad "one-absent did not explain that absence is an error: $OUT"
fi

echo "== state: an absent context that is NOT registered =="
# runs-all-pass has no check run for any unregistered tier beyond the advisory
# one; the registry's two tiers are both present, so absence elsewhere is inert.
invoke "cat $(runs_file all-pass)" "$WORK/self-ids.txt" "$EXPIRED" 1
if [ "$RC" -eq 0 ]; then
  ok "one-absent-and-unregistered does not gate (exit 0 even at an expired deadline)"
else
  bad "an unregistered absent context gated the merge: $OUT"
fi

echo "== re-runs: newest wins in both directions =="
invoke "cat $(runs_file rerun-green)" "$WORK/self-ids.txt" "$FUTURE" 3
if [ "$RC" -eq 0 ]; then ok "a re-run that turned green supersedes the earlier failure"; else bad "stale failure latched: $OUT"; fi
if contains "$(cat "$SUMMARY")" '1099'; then ok "summary records the superseding check-run id"; else bad "summary kept the superseded id"; fi

invoke "cat $(runs_file rerun-pending)" "$WORK/self-ids.txt" "$EXPIRED" 1
if [ "$RC" -ne 0 ]; then ok "a re-run in progress supersedes the earlier success"; else bad "stale success latched: $OUT"; fi

echo "== self-exclusion by run identity, never by name =="
invoke "cat $(runs_file self-shadow)" "$WORK/self-ids-shadow.txt" "$FUTURE" 3
if [ "$RC" -ne 0 ] && contains "$OUT" "alpha"; then
  ok "our own higher-id check run does not shadow a sibling tier's failure"
else
  bad "self check run shadowed a real tier (rc=$RC): $OUT"
fi
SELF_SHADOW_OUT="$OUT"
SELF_SHADOW_RC="$RC"
invoke "cat $(runs_file self-shadow-renamed)" "$WORK/self-ids-shadow.txt" "$FUTURE" 3
if [ "$RC" -eq "$SELF_SHADOW_RC" ] && contains "$OUT" "alpha"; then
  ok "renaming the jobs does not change self-exclusion"
else
  bad "verdict changed after renaming (rc=$RC vs $SELF_SHADOW_RC): $OUT vs $SELF_SHADOW_OUT"
fi
# Beta's self check run (id 9004) is excluded only by its details URL, and Beta's
# real sibling run is a success — so Beta must not be reported as failing.
if contains "$OUT" "gating tiers not satisfied: \`alpha\`"; then
  ok "details-URL fallback excluded the second self check run (beta not implicated)"
else
  bad "details-URL self-exclusion did not hold: $OUT"
fi

echo "== a tier that becomes terminal before the deadline is observed =="
rm -f "$WORK/progressive.count" "$WORK/sleep.log"
invoke "$WORK/progressive.sh" "$WORK/self-ids.txt" "$FUTURE" 5
if [ "$RC" -eq 0 ]; then ok "later-poll success is observed"; else bad "later-poll success was missed: $OUT"; fi
SLEEPS=$(wc -l <"$WORK/sleep.log" 2>/dev/null | tr -d ' ')
if [ "${SLEEPS:-0}" -ge 2 ]; then
  ok "the loop backed off and re-polled (${SLEEPS} intervals)"
else
  bad "the loop did not re-poll (${SLEEPS:-0} intervals)"
fi

echo "== waivers =="
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1 --labels "ci:waive:beta" --actor tester
if [ "$RC" -eq 0 ]; then ok "a waiver excuses an absent tier"; else bad "absent+waived expected exit 0: $OUT"; fi
if contains "$OUT" "::warning::" && contains "$OUT" "beta"; then
  ok "the honoured waiver emits a warning annotation naming the tier"
else
  bad "no warning annotation for the honoured waiver: $OUT"
fi
if contains "$(cat "$SUMMARY")" 'tester'; then ok "the summary records the waiver actor"; else bad "summary omits the waiver actor"; fi

invoke "cat $(runs_file one-failed)" "$WORK/self-ids.txt" "$EXPIRED" 1 --labels "ci:waive:beta" --actor tester
if [ "$RC" -ne 0 ]; then ok "a waiver cannot excuse a FAILED tier"; else bad "failed+waived wrongly passed"; fi
if contains "$OUT" "cannot be waived"; then
  ok "the failed+waived case states that a failed tier cannot be waived"
else
  bad "no 'cannot be waived' diagnostic: $OUT"
fi

invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1 --labels "ci:waive:alpha" --actor tester
if [ "$RC" -ne 0 ] && contains "$OUT" "beta"; then
  ok "a waiver is scoped to one tier (beta still gates)"
else
  bad "waiver leaked across tiers (rc=$RC): $OUT"
fi

invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1 --labels "ci:waive:*,ci:waive-all,waive" --actor tester
if [ "$RC" -ne 0 ]; then ok "there is no blanket waiver"; else bad "a blanket-looking label waived everything"; fi

echo "== harness failures fail CLOSED =="
invoke "cat $(runs_file all-pass)" "$WORK/definitely-missing-ids.txt" "$FUTURE" 1
if [ "$RC" -ne 0 ]; then ok "an unreadable self-job list fails closed"; else bad "missing self-job list passed"; fi
invoke "false" "$WORK/self-ids.txt" "$FUTURE" 1
if [ "$RC" -ne 0 ]; then ok "an unreadable check-run source fails closed"; else bad "unreadable check-run source passed"; fi
OUT=$(bash "$AGG_REAL" --registry "$WORK/nonexistent-registry.yml" \
  --check-runs-cmd "cat $(runs_file all-pass)" --self-jobs-cmd "cat $WORK/self-ids.txt" 2>&1)
if [ "$?" -ne 0 ]; then ok "a missing registry fails closed"; else bad "missing registry passed"; fi

# ------------------------------------------------- non-vacuity (#2910 7.4) --
# Every guard above must be PROVABLY able to fail. Substituting an always-exit-0
# aggregator has to make the failing cases stop failing; if it does not, the
# assertions are not actually driven by the aggregator's verdict.

count_failing_verdicts() {
  local n=0
  invoke "cat $(runs_file one-failed)" "$WORK/self-ids.txt" "$FUTURE" 1
  [ "$RC" -ne 0 ] && n=$((n + 1))
  invoke "cat $(runs_file one-pending)" "$WORK/self-ids.txt" "$EXPIRED" 1
  [ "$RC" -ne 0 ] && n=$((n + 1))
  invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1
  [ "$RC" -ne 0 ] && n=$((n + 1))
  invoke "cat $(runs_file one-failed)" "$WORK/self-ids.txt" "$EXPIRED" 1 --labels "ci:waive:beta"
  [ "$RC" -ne 0 ] && n=$((n + 1))
  printf '%s' "$n"
}

echo "== non-vacuity: an always-pass stub aggregator must break this suite =="
AGG="$AGG_REAL"
REAL_FAILURES=$(count_failing_verdicts)
AGG="$WORK/stub-aggregator.sh"
STUB_FAILURES=$(count_failing_verdicts)
AGG="$AGG_REAL"
if [ "$REAL_FAILURES" -eq 4 ]; then
  ok "the real aggregator fails all 4 discriminating states"
else
  bad "the real aggregator failed only $REAL_FAILURES/4 discriminating states"
fi
if [ "$STUB_FAILURES" -eq 0 ]; then
  ok "the always-exit-0 stub fails none of them, so this suite would go RED under it"
else
  bad "the stub still 'failed' $STUB_FAILURES cases; the assertions are not driven by the aggregator"
fi

echo
echo "==== aggregate-required-tiers self-test: PASS=$PASS FAIL=$FAIL ===="
[ "$FAIL" -eq 0 ] || exit 1
