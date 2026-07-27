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

# Same set with our own SUCCEEDING self check run (id 9003, "Alpha gate")
# renamed: exclusion is by run identity, so the verdict must be identical. (The
# other self check run, 9004, keeps its name — it is excluded by details URL, and
# the two mechanisms are asserted separately below.)
sed 's/"Alpha gate","status":"completed","conclusion":"success"/"totally renamed gate","status":"completed","conclusion":"success"/' \
  "$(runs_file self-shadow)" >"$(runs_file self-shadow-renamed)"

# ---- supersession fixtures (issue #2910 P2) --------------------------------
# `cancel-in-progress` concurrency cancels a tier's in-flight run on every
# re-push, label change and ready-for-review, so `cancelled` is ROUTINE. A fixed
# completion timestamp + an injected `--now` make the grace window deterministic
# with no wall-clock dependence.
CANCELLED_AT="2026-01-01T00:00:00Z"
CANCELLED_EPOCH=1767225600
WITHIN_GRACE=$((CANCELLED_EPOCH + 60))
PAST_GRACE=$((CANCELLED_EPOCH + 4000))

cat >"$(runs_file beta-cancelled)" <<JSON
{"check_runs":[
 {"id":1001,"name":"Alpha gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":1002,"name":"Beta gate","status":"completed","conclusion":"cancelled",
  "completed_at":"$CANCELLED_AT",
  "details_url":"https://github.com/o/r/actions/runs/502/job/1002"}
]}
JSON

# The replacement run has minted its check run: the cancelled one is no longer
# latest, so supersession is detected POSITIVELY rather than guessed.
cat >"$(runs_file beta-superseded)" <<JSON
{"check_runs":[
 {"id":1001,"name":"Alpha gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":1002,"name":"Beta gate","status":"completed","conclusion":"cancelled",
  "completed_at":"$CANCELLED_AT",
  "details_url":"https://github.com/o/r/actions/runs/502/job/1002"},
 {"id":1099,"name":"Beta gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/599/job/1099"}
]}
JSON

# Cancelled first, replacement green on the third fetch: the real timeline of a
# supersession, observed across polls.
cat >"$WORK/superseding.sh" <<EOF
#!/usr/bin/env bash
n=\$(cat "$WORK/superseding.count" 2>/dev/null || echo 0)
n=\$((n + 1))
echo "\$n" >"$WORK/superseding.count"
if [ "\$n" -ge 3 ]; then cat "$(runs_file beta-superseded)"; else cat "$(runs_file beta-cancelled)"; fi
EOF
chmod +x "$WORK/superseding.sh"

# ---- transient-fetch fixtures (issue #2910 P3) -----------------------------
# One 5xx / secondary-rate-limit / DNS blip must not red a PR with an hour of
# budget left; a persistent outage still fails closed.
cat >"$WORK/blip-then-pass.sh" <<EOF
#!/usr/bin/env bash
n=\$(cat "$WORK/blip.count" 2>/dev/null || echo 0)
n=\$((n + 1))
echo "\$n" >"$WORK/blip.count"
if [ "\$n" -le 1 ]; then echo "503 Service Unavailable" >&2; exit 1; fi
cat "$(runs_file all-pass)"
EOF
chmod +x "$WORK/blip-then-pass.sh"

# ---- live-label fixtures (issue #2910 P1) ----------------------------------
# A waiver applied WHILE the aggregation waits: the event payload is a snapshot
# and a re-run replays it, so the labels must be re-read from the API each poll.
cat >"$WORK/labels-late.sh" <<EOF
#!/usr/bin/env bash
n=\$(cat "$WORK/labels.count" 2>/dev/null || echo 0)
n=\$((n + 1))
echo "\$n" >"$WORK/labels.count"
[ "\$n" -ge 2 ] && echo "ci:waive:beta"
exit 0
EOF
chmod +x "$WORK/labels-late.sh"

# A LONE check-run object (exactly one check run on the head, un-enveloped) is a
# shape variation, not a reason to red the gate.
cat >"$(runs_file lone-object)" <<'JSON'
{"id":1001,"name":"Alpha gate","status":"completed","conclusion":"success",
 "details_url":"https://github.com/o/r/actions/runs/501/job/1001"}
JSON

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

# ------------------------------------------------- supersession (#2910 P2) --
# BOTH directions are outages. `cancelled` used to be an instant hard fail, but
# supersession is routine: marking a draft ready for review, or adding any label,
# cancels the tier's in-flight run under `cancel-in-progress`. Reding `required`
# for that wedges ordinary PRs. It must re-poll — and must still fail when the
# cancellation was genuine.

echo "== a cancelled tier superseded by a green re-run passes =="
rm -f "$WORK/superseding.count" "$WORK/sleep.log"
invoke "$WORK/superseding.sh" "$WORK/self-ids.txt" "$FUTURE" 5 --now "$WITHIN_GRACE"
if [ "$RC" -eq 0 ]; then ok "superseded-then-green passes"; else bad "a superseded cancellation red the gate: $OUT"; fi
if contains "$(cat "$SUMMARY")" '1099'; then
  ok "the superseding check run is what decided the tier"
else
  bad "the summary did not record the superseding check run: $(cat "$SUMMARY")"
fi
SLEEPS=$(wc -l <"$WORK/sleep.log" 2>/dev/null | tr -d ' ')
if [ "${SLEEPS:-0}" -ge 2 ]; then
  ok "the cancelled tier was re-polled rather than hard-failed (${SLEEPS} intervals)"
else
  bad "the loop did not re-poll a cancelled tier (${SLEEPS:-0} intervals)"
fi

echo "== a cancelled tier with no successor still fails =="
invoke "cat $(runs_file beta-cancelled)" "$WORK/self-ids.txt" "$FUTURE" 5 --now "$PAST_GRACE"
if [ "$RC" -ne 0 ]; then ok "cancelled-with-no-successor fails once the grace lapses"; else bad "a genuine cancellation passed: $OUT"; fi
if contains "$OUT" "beta" && contains "$OUT" "no superseding run appeared"; then
  ok "it names the tier and says no superseding run appeared"
else
  bad "the cancellation diagnostic is missing: $OUT"
fi

invoke "cat $(runs_file beta-cancelled)" "$WORK/self-ids.txt" "$EXPIRED" 1 --now "$WITHIN_GRACE"
if [ "$RC" -ne 0 ]; then ok "a cancelled tier inside the grace still fails AT THE DEADLINE"; else bad "expiry passed a cancelled tier: $OUT"; fi

invoke "cat $(runs_file beta-cancelled)" "$WORK/self-ids.txt" "$EXPIRED" 1 --now "$PAST_GRACE" --labels "ci:waive:beta"
if [ "$RC" -ne 0 ] && contains "$OUT" "cannot be waived"; then
  ok "a waiver cannot excuse a cancelled tier either"
else
  bad "a cancelled tier was waived (rc=$RC): $OUT"
fi

# Near-miss: no `completed_at` to age against. Unknown age must NOT mean
# "instantly stale" (false red) NOR "wait forever" — it stays non-terminal and
# fails at the deadline.
CANCELLED_NO_TS=$(sed "s/\"completed_at\":\"$CANCELLED_AT\",//" "$(runs_file beta-cancelled)")
printf '%s\n' "$CANCELLED_NO_TS" >"$(runs_file beta-cancelled-no-ts)"
invoke "cat $(runs_file beta-cancelled-no-ts)" "$WORK/self-ids.txt" "$EXPIRED" 1 --now "$PAST_GRACE"
if [ "$RC" -ne 0 ] && contains "$OUT" "beta"; then
  ok "a cancellation with no timestamp fails closed at the deadline"
else
  bad "an untimestamped cancellation did not fail closed (rc=$RC): $OUT"
fi

# -------------------------------------------- transient fetch blips (P3) ----
echo "== a mid-poll API blip is retried, not fatal =="
rm -f "$WORK/blip.count" "$WORK/sleep.log"
invoke "$WORK/blip-then-pass.sh" "$WORK/self-ids.txt" "$FUTURE" 6
if [ "$RC" -eq 0 ]; then ok "one failed fetch mid-poll recovers and passes"; else bad "a single API blip red the gate: $OUT"; fi
if contains "$OUT" "transient check-run fetch failure"; then
  ok "the blip is reported as transient rather than silently swallowed"
else
  bad "no transient-failure warning: $OUT"
fi

echo "== a persistent fetch failure still fails CLOSED =="
invoke "false" "$WORK/self-ids.txt" "$FUTURE" 20 --max-fetch-failures 3
if [ "$RC" -eq 2 ] && contains "$OUT" "3 times in a row"; then
  ok "a fetch failure that never recovers fails closed at the ceiling"
else
  bad "a persistent fetch failure did not fail closed at the ceiling (rc=$RC): $OUT"
fi

# --------------------------------------------------- live labels (P1) ------
echo "== waiver labels are re-read while the aggregation waits =="
rm -f "$WORK/labels.count"
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$FUTURE" 5 \
  --labels-cmd "$WORK/labels-late.sh" --actor tester
if [ "$RC" -eq 0 ]; then
  ok "a waiver applied AFTER the run started is honoured without a re-run"
else
  bad "a late waiver was invisible — the documented break-glass would be unreachable: $OUT"
fi

invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1 \
  --labels-cmd "false" --labels "ci:waive:beta" --actor tester
if [ "$RC" -eq 0 ] && contains "$OUT" "falling back to the event payload labels"; then
  ok "an unreadable label source falls back to the payload labels and says so"
else
  bad "the label-read fallback did not hold (rc=$RC): $OUT"
fi

invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1 --labels-cmd "printf 'needs-decision\n'"
if [ "$RC" -ne 0 ] && contains "$OUT" "beta"; then
  ok "live labels that contain no waiver do not excuse anything"
else
  bad "a non-waiver label leaked a waiver (rc=$RC): $OUT"
fi

# ------------------------------------- waived ABSENT does not idle (P8) -----
echo "== a waived ABSENT tier resolves immediately; a waived PENDING one still waits =="
: >"$WORK/sleep.log"
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$FUTURE" 5 --labels "ci:waive:beta" --actor tester
if [ "$RC" -eq 0 ] && [ ! -s "$WORK/sleep.log" ]; then
  ok "a waived absent tier does not hold a runner for the whole deadline (no polls)"
else
  bad "a waived absent tier burned the poll budget (rc=$RC, sleeps=$(wc -l <"$WORK/sleep.log"))"
fi
: >"$WORK/sleep.log"
invoke "cat $(runs_file one-pending)" "$WORK/self-ids.txt" "$FUTURE" 3 --labels "ci:waive:beta" --actor tester
if [ "$RC" -eq 0 ] && [ -s "$WORK/sleep.log" ]; then
  ok "a waived PENDING tier is still waited out (it could still turn red)"
else
  bad "a waived pending tier short-circuited (rc=$RC, sleeps=$(wc -l <"$WORK/sleep.log"))"
fi

# ---------------------------------- registry self-check + shapes (P7/P10) ---
echo "== the aggregator refuses a registry that would aggregate nothing =="
for empty in 'tiers: []' 'tiers: oops' '# no tiers key at all'; do
  cat >"$WORK/registry-empty.yml" <<YAML
version: 1
aggregator: { workflow: pr-gate.yml, job: required }
defaults: { wait_minutes: 60 }
$empty
YAML
  OUT=$(bash "$AGG_REAL" --registry "$WORK/registry-empty.yml" \
    --check-runs-cmd "cat $(runs_file all-pass)" --self-jobs-cmd "cat $WORK/self-ids.txt" \
    --deadline-epoch "$FUTURE" --poll-attempts 1 --sleep-cmd "$WORK/fake-sleep.sh" 2>&1)
  RC=$?
  if [ "$RC" -eq 2 ]; then
    ok "a registry with '$empty' exits 2 rather than reporting a vacuous success"
  else
    bad "a registry with '$empty' produced rc=$RC: $OUT"
  fi
done

echo "== a lone check-run object is a shape variation, not a harness failure =="
invoke "cat $(runs_file lone-object)" "$WORK/self-ids.txt" "$EXPIRED" 1
if [ "$RC" -eq 1 ] && contains "$OUT" "beta" && ! contains "$OUT" "FAIL (harness)"; then
  ok "a single un-enveloped check run parses; only the genuinely absent tier gates"
else
  bad "a lone check-run object was mis-handled (rc=$RC): $OUT"
fi

echo "== harness failures fail CLOSED =="
invoke "cat $(runs_file all-pass)" "$WORK/definitely-missing-ids.txt" "$FUTURE" 1
if [ "$RC" -ne 0 ]; then ok "an unreadable self-job list fails closed"; else bad "missing self-job list passed"; fi
invoke "false" "$WORK/self-ids.txt" "$FUTURE" 1
if [ "$RC" -ne 0 ]; then ok "an unreadable check-run source fails closed"; else bad "unreadable check-run source passed"; fi
OUT=$(bash "$AGG_REAL" --registry "$WORK/nonexistent-registry.yml" \
  --check-runs-cmd "cat $(runs_file all-pass)" --self-jobs-cmd "cat $WORK/self-ids.txt" 2>&1)
if [ "$?" -ne 0 ]; then ok "a missing registry fails closed"; else bad "missing registry passed"; fi

# --------------------------------- the gate's own compute job (round 2) -----
# A label mutation must not restart the 30-minute core, so a label-triggered run
# skips it and `required` reuses the result already RECORDED for the same head
# sha. Skipping the WORK must never skip the CHECK: every way that reuse could
# manufacture a green core is a case here.

cat >"$(runs_file core-recorded-success)" <<'JSON'
{"check_runs":[
 {"id":8000,"name":"pr-gate-core","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/700/job/8000"},
 {"id":9001,"name":"pr-gate-core","status":"completed","conclusion":"skipped",
  "details_url":"https://github.com/o/r/actions/runs/777/job/9001"}
]}
JSON

cat >"$(runs_file core-recorded-failure)" <<'JSON'
{"check_runs":[
 {"id":8000,"name":"pr-gate-core","status":"completed","conclusion":"failure",
  "details_url":"https://github.com/o/r/actions/runs/700/job/8000"},
 {"id":9001,"name":"pr-gate-core","status":"completed","conclusion":"skipped",
  "details_url":"https://github.com/o/r/actions/runs/777/job/9001"}
]}
JSON

# THE NEAR-MISS: the ONLY `pr-gate-core` check run on the head is the one THIS
# label run skipped. It has the highest id, so without run-identity exclusion it
# would be read as the answer — and `skipped` is not `success`, but a future
# refactor that treated "found a check run" as sufficient would go green here.
cat >"$(runs_file core-only-self)" <<'JSON'
{"check_runs":[
 {"id":9001,"name":"pr-gate-core","status":"completed","conclusion":"skipped",
  "details_url":"https://github.com/o/r/actions/runs/777/job/9001"}
]}
JSON

cat >"$(runs_file core-absent)" <<'JSON'
{"check_runs":[
 {"id":7000,"name":"some other check","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/700/job/7000"}
]}
JSON

echo "== the gate's own compute job is never masked (#2910 round 2) =="
invoke "cat $(runs_file all-pass)" "$WORK/self-ids.txt" "$FUTURE" 3 \
  --core-result success --event-action synchronize
if [ "$RC" -eq 0 ]; then ok "a successful core passes through"; else bad "successful core rejected (rc=$RC): $OUT"; fi

invoke "cat $(runs_file all-pass)" "$WORK/self-ids.txt" "$FUTURE" 3 \
  --core-result failure --event-action synchronize
if [ "$RC" -ne 0 ] && contains "$OUT" "pr-gate-core"; then
  ok "a failed core fails required regardless of tier state"
else
  bad "a failed core did not fail required (rc=$RC): $OUT"
fi

invoke "cat $(runs_file all-pass)" "$WORK/self-ids.txt" "$FUTURE" 3 \
  --core-result skipped --event-action synchronize
if [ "$RC" -ne 0 ] && contains "$OUT" "synchronize"; then
  ok "a core skipped on a NON-label event fails closed (only a label mutation may reuse)"
else
  bad "a core skipped on a synchronize event was tolerated (rc=$RC): $OUT"
fi

invoke "cat $(runs_file all-pass)" "$WORK/self-ids.txt" "$FUTURE" 3 \
  --core-result skipped --event-action labeled --core-runs-cmd "cat $(runs_file core-recorded-success)"
if [ "$RC" -eq 0 ] && contains "$OUT" "recorded for this head sha"; then
  ok "a label event reuses the core result recorded for the same head sha"
else
  bad "a label event could not reuse the recorded core (rc=$RC): $OUT"
fi

invoke "cat $(runs_file all-pass)" "$WORK/self-ids.txt" "$FUTURE" 3 \
  --core-result skipped --event-action labeled --core-runs-cmd "cat $(runs_file core-recorded-failure)"
if [ "$RC" -ne 0 ]; then ok "a label event cannot reuse a FAILED core"; else bad "a label event reused a failed core"; fi

invoke "cat $(runs_file all-pass)" "$WORK/self-ids.txt" "$FUTURE" 3 \
  --core-result skipped --event-action labeled --core-runs-cmd "cat $(runs_file core-absent)"
if [ "$RC" -ne 0 ]; then ok "a label event with NO recorded core fails closed"; else bad "an absent recorded core passed"; fi

invoke "cat $(runs_file all-pass)" "$WORK/self-ids.txt" "$FUTURE" 3 \
  --core-result skipped --event-action labeled --core-runs-cmd "cat $(runs_file core-only-self)"
if [ "$RC" -ne 0 ]; then
  ok "the label run's OWN skipped core check run cannot stand in for the real one"
else
  bad "the run's own skipped core was accepted as the recorded result"
fi

invoke "cat $(runs_file all-pass)" "$WORK/self-ids.txt" "$FUTURE" 3 \
  --core-result skipped --event-action labeled --core-runs-cmd "false"
if [ "$RC" -ne 0 ]; then ok "an unreadable core lookup fails closed"; else bad "an unreadable core lookup passed"; fi

# The break-glass, end to end, on the event that applies it: a waiver label on a
# tier that never reported, during a run whose core was NOT re-executed.
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$FUTURE" 3 \
  --core-result skipped --event-action labeled \
  --core-runs-cmd "cat $(runs_file core-recorded-success)" --labels "ci:waive:beta" --actor tester
if [ "$RC" -eq 0 ] && contains "$OUT" "WAIVED"; then
  ok "a waiver applied by label takes effect in a run that did not re-execute the core"
else
  bad "the label-event waiver path did not clear (rc=$RC): $OUT"
fi

# ------------------------- the BASE ref's registry governs (round 2) --------
# A PR can edit `.github/ci-gating-tiers.yml`, so `required` reads the registry
# (and this script, and gating_registry.rb) from the pull request's BASE ref. The
# pair below is the discrimination: the SAME check-run evidence passes under the
# PR's edited registry and fails under the base one.
echo "== a PR moving its own tier to exempt: is still gated by the base registry =="
cat >"$WORK/registry-head-exempts-beta.yml" <<'YAML'
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
exempt:
  - workflow: beta.yml
    reason: Not merge-gating for this pull request, honest.
    issue: "#2910"
YAML
OUT=$(bash "$AGG_REAL" --registry "$WORK/registry-head-exempts-beta.yml" \
  --check-runs-cmd "cat $(runs_file one-absent)" --self-jobs-cmd "cat $WORK/self-ids.txt" \
  --deadline-epoch "$EXPIRED" --poll-attempts 1 --sleep-cmd "$WORK/fake-sleep.sh" 2>&1)
RC=$?
if [ "$RC" -eq 0 ]; then
  ok "the PR's edited registry would have let the absent tier through (the attack works, unmitigated)"
else
  bad "the head-registry control case did not pass, so the base-registry case proves nothing: $OUT"
fi
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1
if [ "$RC" -ne 0 ] && contains "$OUT" "beta"; then
  ok "evaluated against the BASE registry the same evidence still fails on the absent tier"
else
  bad "the base registry did not gate the absent tier (rc=$RC): $OUT"
fi

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
  invoke "cat $(runs_file beta-cancelled)" "$WORK/self-ids.txt" "$FUTURE" 5 --now "$PAST_GRACE"
  [ "$RC" -ne 0 ] && n=$((n + 1))
  invoke "false" "$WORK/self-ids.txt" "$FUTURE" 20 --max-fetch-failures 2
  [ "$RC" -ne 0 ] && n=$((n + 1))
  # round 2: the gate's own compute job, in both of its refusal shapes.
  invoke "cat $(runs_file all-pass)" "$WORK/self-ids.txt" "$FUTURE" 1 \
    --core-result skipped --event-action synchronize
  [ "$RC" -ne 0 ] && n=$((n + 1))
  invoke "cat $(runs_file all-pass)" "$WORK/self-ids.txt" "$FUTURE" 1 \
    --core-result skipped --event-action labeled --core-runs-cmd "cat $(runs_file core-only-self)"
  [ "$RC" -ne 0 ] && n=$((n + 1))
  printf '%s' "$n"
}

echo "== non-vacuity: an always-pass stub aggregator must break this suite =="
AGG="$AGG_REAL"
REAL_FAILURES=$(count_failing_verdicts)
AGG="$WORK/stub-aggregator.sh"
STUB_FAILURES=$(count_failing_verdicts)
AGG="$AGG_REAL"
if [ "$REAL_FAILURES" -eq 8 ]; then
  ok "the real aggregator fails all 8 discriminating states"
else
  bad "the real aggregator failed only $REAL_FAILURES/8 discriminating states"
fi
if [ "$STUB_FAILURES" -eq 0 ]; then
  ok "the always-exit-0 stub fails none of them, so this suite would go RED under it"
else
  bad "the stub still 'failed' $STUB_FAILURES cases; the assertions are not driven by the aggregator"
fi

echo
echo "==== aggregate-required-tiers self-test: PASS=$PASS FAIL=$FAIL ===="
[ "$FAIL" -eq 0 ] || exit 1
