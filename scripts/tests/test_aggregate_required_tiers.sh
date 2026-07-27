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

# THE DECLARED RUBY FLOOR (issue #2910 round 4). Ruby is the SINGLE
# implementation path — the python3 fallbacks were removed — so its version floor
# became load-bearing at the moment nothing was checking it (macOS system ruby is
# 2.6 and macOS is a first-class gate host). SKIP WITH THE REASON rather than
# mis-run: a `filter_map` NoMethodError swallowed by a parser's rescue, or an
# `aliases:` ArgumentError, would make this suite assert against garbage.
RUBY_FLOOR_RB="$REPO_ROOT/scripts/ci/gating_ruby_floor.rb"
if [ ! -f "$RUBY_FLOOR_RB" ]; then
  echo "FAIL - $RUBY_FLOOR_RB not found (the declared ruby floor cannot be checked)"
  exit 1
fi
if ! ruby "$RUBY_FLOOR_RB" >/dev/null 2>&1; then
  echo "SKIP: $(ruby "$RUBY_FLOOR_RB" 2>&1 >/dev/null)"
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
 {"id":1001,"app":{"slug":"github-actions","id":15368},"name":"Alpha gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":1002,"app":{"slug":"github-actions","id":15368},"name":"Beta gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/502/job/1002"},
 {"id":1500,"app":{"slug":"github-actions","id":15368},"name":"perf advisory","status":"completed","conclusion":"failure",
  "details_url":"https://github.com/o/r/actions/runs/503/job/1500"},
 {"id":9001,"app":{"slug":"github-actions","id":15368},"name":"pr-gate-core","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/777/job/9001"},
 {"id":9002,"app":{"slug":"github-actions","id":15368},"name":"required","status":"in_progress","conclusion":null,
  "details_url":"https://github.com/o/r/actions/runs/777/job/9002"}
]}
JSON

# THIS HEAD SHA'S FIRST CI ACTIVITY (issue #2910 round 5). A waiver is bound to
# the head it was applied for, and the anchor for that is the earliest
# `started_at` GitHub recorded on the head — so the fixtures carry the timestamps
# real check runs carry. `HEAD_CI_AT` is an hour BEFORE `WAIVER_AT` below: the
# ordinary case, a waiver applied to a head that is already running CI.
HEAD_CI_AT="2025-12-31T23:00:00Z"

cat >"$(runs_file one-pending)" <<JSON
{"check_runs":[
 {"id":1001,"app":{"slug":"github-actions","id":15368},"name":"Alpha gate","status":"completed","conclusion":"success",
  "started_at":"$HEAD_CI_AT",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":1002,"app":{"slug":"github-actions","id":15368},"name":"Beta gate","status":"in_progress","conclusion":null,
  "started_at":"$HEAD_CI_AT",
  "details_url":"https://github.com/o/r/actions/runs/502/job/1002"}
]}
JSON

cat >"$(runs_file one-failed)" <<'JSON'
{"check_runs":[
 {"id":1001,"app":{"slug":"github-actions","id":15368},"name":"Alpha gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":1002,"app":{"slug":"github-actions","id":15368},"name":"Beta gate","status":"completed","conclusion":"failure",
  "details_url":"https://github.com/o/r/actions/runs/502/job/1002"}
]}
JSON

# Beta's context is absent entirely; an UNREGISTERED context is absent too, which
# must not gate anything.
cat >"$(runs_file one-absent)" <<JSON
{"check_runs":[
 {"id":1001,"app":{"slug":"github-actions","id":15368},"name":"Alpha gate","status":"completed","conclusion":"success",
  "started_at":"$HEAD_CI_AT",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"}
]}
JSON

# Re-run, newest green: the older failure must NOT be latched.
cat >"$(runs_file rerun-green)" <<'JSON'
{"check_runs":[
 {"id":1001,"app":{"slug":"github-actions","id":15368},"name":"Alpha gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":1002,"app":{"slug":"github-actions","id":15368},"name":"Beta gate","status":"completed","conclusion":"failure",
  "details_url":"https://github.com/o/r/actions/runs/502/job/1002"},
 {"id":1099,"app":{"slug":"github-actions","id":15368},"name":"Beta gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/599/job/1099"}
]}
JSON

# Re-run, newest non-terminal: the older SUCCESS must NOT be latched either.
cat >"$(runs_file rerun-pending)" <<'JSON'
{"check_runs":[
 {"id":1001,"app":{"slug":"github-actions","id":15368},"name":"Alpha gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":1002,"app":{"slug":"github-actions","id":15368},"name":"Beta gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/502/job/1002"},
 {"id":1099,"app":{"slug":"github-actions","id":15368},"name":"Beta gate","status":"in_progress","conclusion":null,
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
 {"id":1001,"app":{"slug":"github-actions","id":15368},"name":"Alpha gate","status":"completed","conclusion":"failure",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":9003,"app":{"slug":"github-actions","id":15368},"name":"Alpha gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/777/job/9003"},
 {"id":1002,"app":{"slug":"github-actions","id":15368},"name":"Beta gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/502/job/1002"},
 {"id":9004,"app":{"slug":"github-actions","id":15368},"name":"Beta gate","status":"completed","conclusion":"success",
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
 {"id":1001,"app":{"slug":"github-actions","id":15368},"name":"Alpha gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":1002,"app":{"slug":"github-actions","id":15368},"name":"Beta gate","status":"completed","conclusion":"cancelled",
  "completed_at":"$CANCELLED_AT",
  "details_url":"https://github.com/o/r/actions/runs/502/job/1002"}
]}
JSON

# The replacement run has minted its check run: the cancelled one is no longer
# latest, so supersession is detected POSITIVELY rather than guessed.
cat >"$(runs_file beta-superseded)" <<JSON
{"check_runs":[
 {"id":1001,"app":{"slug":"github-actions","id":15368},"name":"Alpha gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":1002,"app":{"slug":"github-actions","id":15368},"name":"Beta gate","status":"completed","conclusion":"cancelled",
  "completed_at":"$CANCELLED_AT",
  "details_url":"https://github.com/o/r/actions/runs/502/job/1002"},
 {"id":1099,"app":{"slug":"github-actions","id":15368},"name":"Beta gate","status":"completed","conclusion":"success",
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

# ---- waiver-provenance fixtures (issue #2910 round 4) ---------------------
# WHO applied `ci:waive:<tier>`, and WHEN, from the PR's `labeled` events
# (oldest first, the order the issues-events API returns). The old diagnostic
# named $GITHUB_ACTOR — the actor of the event that started the RUN, which for a
# live-re-read waiver can be an entirely uninvolved person.
WAIVER_AT="2026-01-01T00:00:00Z"
WAIVER_EPOCH=1767225600
printf 'needs-decision\tpm-bot\t2025-12-30T00:00:00Z\nci:waive:beta\treal-labeller\t%s\n' \
  "$WAIVER_AT" >"$WORK/waiver-events.tsv"
# A label removed and re-applied: the LAST `labeled` event is the attribution.
printf 'ci:waive:beta\tfirst-labeller\t2025-12-31T00:00:00Z\nci:waive:beta\tsecond-labeller\t%s\n' \
  "$WAIVER_AT" >"$WORK/waiver-events-relabelled.tsv"
# The resolved login lands in a `::warning::` workflow command, so it keeps the
# allowlist treatment — now applied where the value is resolved (ruby), not to a
# value the shell merely passed through.
printf 'ci:waive:beta\tevil::error::injected\t%s\n' "$WAIVER_AT" >"$WORK/waiver-events-injection.tsv"
printf 'ci:waive:beta\tdependabot[bot]\t%s\n' "$WAIVER_AT" >"$WORK/waiver-events-bot.tsv"
WAIVER_EVENTS="cat $WORK/waiver-events.tsv"

# A PENDING beta whose check run was minted AFTER the waiver was applied — i.e.
# the run the waiver's own label event started. Waiting it out to the deadline
# would be the break-glass fighting itself.
cat >"$(runs_file beta-pending-after-waiver)" <<JSON
{"check_runs":[
 {"id":1001,"app":{"slug":"github-actions","id":15368},"name":"Alpha gate","status":"completed","conclusion":"success",
  "started_at":"$HEAD_CI_AT",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":1600,"app":{"slug":"github-actions","id":15368},"name":"Beta gate","status":"queued","conclusion":null,
  "started_at":"2026-01-01T00:00:30Z",
  "details_url":"https://github.com/o/r/actions/runs/601/job/1600"}
]}
JSON
# The same tier pending from a run that predates the waiver: ordinary semantics,
# the waiver waits for the deadline because the tier can still turn red.
cat >"$(runs_file beta-pending-before-waiver)" <<JSON
{"check_runs":[
 {"id":1001,"app":{"slug":"github-actions","id":15368},"name":"Alpha gate","status":"completed","conclusion":"success",
  "started_at":"$HEAD_CI_AT",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":1002,"app":{"slug":"github-actions","id":15368},"name":"Beta gate","status":"in_progress","conclusion":null,
  "started_at":"$HEAD_CI_AT",
  "details_url":"https://github.com/o/r/actions/runs/502/job/1002"}
]}
JSON
# Same head, same live waiver, but the pending run started FORTY MINUTES after the
# label event: it is not the run the waiver triggered, it is a later run carrying
# information the waiver's author did not have. The window discriminates.
cat >"$(runs_file beta-pending-late-run)" <<JSON
{"check_runs":[
 {"id":1001,"app":{"slug":"github-actions","id":15368},"name":"Alpha gate","status":"completed","conclusion":"success",
  "started_at":"$HEAD_CI_AT",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":1700,"app":{"slug":"github-actions","id":15368},"name":"Beta gate","status":"queued","conclusion":null,
  "started_at":"2026-01-01T00:40:00Z",
  "details_url":"https://github.com/o/r/actions/runs/701/job/1700"}
]}
JSON

# ---- THE ABUSE: waive, then push (issue #2910 round 5) ---------------------
# `ci:waive:<tier-id>` is a LABEL and a label SURVIVES A PUSH. On this NEW head
# sha every check run postdates the waiver — the label was applied for the
# PREVIOUS head. Beta has not minted its check run yet, which before round 5 was
# waived on the first poll ("nothing to wait for"), so the waiver won the race
# against the tier on every subsequent push: a permanent bypass.
NEW_HEAD_CI_AT="2026-01-01T01:00:00Z"
cat >"$(runs_file new-head-beta-absent)" <<JSON
{"check_runs":[
 {"id":2001,"app":{"slug":"github-actions","id":15368},"name":"Alpha gate","status":"completed","conclusion":"success",
  "started_at":"$NEW_HEAD_CI_AT",
  "details_url":"https://github.com/o/r/actions/runs/801/job/2001"}
]}
JSON
# The same new head once the tier has reported: it FAILED. A failed tier can
# never be waived — that is the invariant the permanent bypass defeated, because
# the tier never got to report before the waiver resolved it.
cat >"$(runs_file new-head-beta-failed)" <<JSON
{"check_runs":[
 {"id":2001,"app":{"slug":"github-actions","id":15368},"name":"Alpha gate","status":"completed","conclusion":"success",
  "started_at":"$NEW_HEAD_CI_AT",
  "details_url":"https://github.com/o/r/actions/runs/801/job/2001"},
 {"id":2002,"app":{"slug":"github-actions","id":15368},"name":"Beta gate","status":"completed","conclusion":"failure",
  "started_at":"$NEW_HEAD_CI_AT",
  "details_url":"https://github.com/o/r/actions/runs/802/job/2002"}
]}
JSON
# The real timeline on the new head: the tier is absent for the first two polls
# (its gate job is the LAST job in its workflow) and then reports its failure.
cat >"$WORK/new-head-timeline.sh" <<EOF
#!/usr/bin/env bash
n=\$(cat "$WORK/new-head.count" 2>/dev/null || echo 0)
n=\$((n + 1))
echo "\$n" >"$WORK/new-head.count"
if [ "\$n" -ge 3 ]; then cat "$(runs_file new-head-beta-failed)"; else cat "$(runs_file new-head-beta-absent)"; fi
EOF
chmod +x "$WORK/new-head-timeline.sh"
# SPOOFING THE ANCHOR. The head anchor is the earliest `started_at` on the head,
# so anything that can plant an ANCIENT timestamp there makes a stale waiver look
# bound again. `started_at` on an Actions check run is set by GitHub, but ANY app
# holding `checks:write` can mint a check run with any timestamp it likes — so
# only PROVENANCED runs contribute to the anchor. This fixture is the new head
# plus an unregistered forgery dated 2020.
cat >"$(runs_file new-head-forged-anchor)" <<JSON
{"check_runs":[
 {"id":2001,"app":{"slug":"github-actions","id":15368},"name":"Alpha gate","status":"completed","conclusion":"success",
  "started_at":"$NEW_HEAD_CI_AT",
  "details_url":"https://github.com/o/r/actions/runs/801/job/2001"},
 {"id":2500,"app":{"slug":"helpful-bot","id":424242},"name":"perf advisory","status":"completed","conclusion":"success",
  "started_at":"2020-01-01T00:00:00Z",
  "details_url":"https://example.invalid/not-an-actions-run"}
]}
JSON
# The label removed and RE-APPLIED after the push: the last `labeled` event wins,
# so the waiver is bound to the new head and the break-glass works again. This is
# the documented remedy, and it is what keeps the binding rule from being a
# one-way door.
printf 'ci:waive:beta\tfirst-labeller\t%s\nci:waive:beta\tsecond-labeller\t2026-01-01T01:30:00Z\n' \
  "$WAIVER_AT" >"$WORK/waiver-events-reapplied.tsv"

# ---- provenance fixtures (issue #2910 round 4) -----------------------------
# A check run is identified to branch protection by NAME ALONE, and anything
# holding `checks:write` can mint one. These are the impostors.
cat >"$(runs_file beta-foreign-app)" <<'JSON'
{"check_runs":[
 {"id":1001,"app":{"slug":"github-actions","id":15368},"name":"Alpha gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":9999,"app":{"slug":"helpful-bot","id":424242},"name":"Beta gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/502/job/9999"}
]}
JSON
cat >"$(runs_file beta-no-app)" <<'JSON'
{"check_runs":[
 {"id":1001,"app":{"slug":"github-actions","id":15368},"name":"Alpha gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":9999,"name":"Beta gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/502/job/9999"}
]}
JSON
cat >"$(runs_file beta-foreign-url)" <<'JSON'
{"check_runs":[
 {"id":1001,"app":{"slug":"github-actions","id":15368},"name":"Alpha gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":9999,"app":{"slug":"github-actions","id":15368},"name":"Beta gate","status":"completed","conclusion":"success",
  "details_url":"https://example.invalid/not-an-actions-run"}
]}
JSON
# An impostor must not SHADOW the genuine run either: here the real tier failed
# and a higher-id forgery reports success.
cat >"$(runs_file beta-forged-over-real-failure)" <<'JSON'
{"check_runs":[
 {"id":1001,"app":{"slug":"github-actions","id":15368},"name":"Alpha gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/501/job/1001"},
 {"id":1002,"app":{"slug":"github-actions","id":15368},"name":"Beta gate","status":"completed","conclusion":"failure",
  "details_url":"https://github.com/o/r/actions/runs/502/job/1002"},
 {"id":9999,"app":{"slug":"helpful-bot","id":424242},"name":"Beta gate","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/502/job/9999"}
]}
JSON

# A LONE check-run object (exactly one check run on the head, un-enveloped) is a
# shape variation, not a reason to red the gate.
cat >"$(runs_file lone-object)" <<'JSON'
{"id":1001,"app":{"slug":"github-actions","id":15368},"name":"Alpha gate","status":"completed","conclusion":"success",
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
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1 --labels "ci:waive:beta" --waiver-events-cmd "$WAIVER_EVENTS"
if [ "$RC" -eq 0 ]; then ok "a waiver excuses an absent tier"; else bad "absent+waived expected exit 0: $OUT"; fi
if contains "$OUT" "::warning::" && contains "$OUT" "beta"; then
  ok "the honoured waiver emits a warning annotation naming the tier"
else
  bad "no warning annotation for the honoured waiver: $OUT"
fi
if contains "$(cat "$SUMMARY")" 'real-labeller'; then
  ok "the summary records who applied the waiver"
else
  bad "summary omits the waiver applier: $(cat "$SUMMARY")"
fi

# ------------------------------------ waiver ATTRIBUTION (round 4, R6) -----
# The diagnostic used to name $GITHUB_ACTOR: the actor of the event that started
# THIS run — a pusher, or whoever hit re-run — not whoever applied the label.
# Since labels are re-read live on every poll, that could put the wrong name on
# the audit trail of a break-glass. THE MUTANT is the run actor itself: it is set
# to a name that must NOT appear anywhere in the output.
echo "== the waiver is attributed to the LABELLER, not to this run's actor =="
GITHUB_ACTOR=wrong-person \
  invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1 \
  --labels "ci:waive:beta" --waiver-events-cmd "$WAIVER_EVENTS"
if [ "$RC" -eq 0 ] && contains "$OUT" "real-labeller" && ! contains "$OUT" "wrong-person"; then
  ok "the resolved labeller is named and the run's actor is not (the old attribution was wrong)"
else
  bad "attribution named the run actor or lost the labeller (rc=$RC): $OUT"
fi

invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1 \
  --labels "ci:waive:beta" --waiver-events-cmd "cat $WORK/waiver-events-relabelled.tsv"
if [ "$RC" -eq 0 ] && contains "$OUT" "second-labeller" && ! contains "$OUT" "first-labeller"; then
  ok "a label removed and re-applied is attributed to the most recent labelling"
else
  bad "re-application was attributed to the wrong event (rc=$RC): $OUT"
fi

# An unreadable events feed must WITHHOLD the claim, not invent one — and must
# not withhold the waiver itself (that would red a PR for an API blip).
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1 \
  --labels "ci:waive:beta" --waiver-events-cmd "false"
if [ "$RC" -eq 0 ] && contains "$OUT" "UNRESOLVED"; then
  ok "an unreadable label-event feed still honours the waiver but claims no name"
else
  bad "the unresolved-attribution path misbehaved (rc=$RC): $OUT"
fi

# The RESOLVED login lands inside a `::warning::` workflow command, so it keeps
# the allowlist treatment; a real app login with a `[bot]` suffix must NOT be
# withheld (a false withholding would hide who waived).
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1 \
  --labels "ci:waive:beta" --waiver-events-cmd "cat $WORK/waiver-events-injection.tsv"
if [ "$RC" -eq 0 ] && ! contains "$OUT" "::error::injected"; then
  ok "an applier carrying a workflow-command payload is withheld, not interpolated"
else
  bad "the applier reached the annotation verbatim (rc=$RC): $OUT"
fi
if contains "$OUT" "applier withheld"; then
  ok "the withholding is stated rather than silent"
else
  bad "an off-shape applier was dropped with no diagnostic: $OUT"
fi
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1 \
  --labels "ci:waive:beta" --waiver-events-cmd "cat $WORK/waiver-events-bot.tsv"
if [ "$RC" -eq 0 ] && contains "$OUT" 'dependabot[bot]'; then
  ok "a legitimate app login keeps its [bot] suffix (no false withholding)"
else
  bad "a real app applier was withheld (rc=$RC): $OUT"
fi

# --------------- waiver evidence: every state reported distinctly (#3033) ---
# THE BUG THIS PINS. An empty label-event feed used to mean two unrelated things —
# "this pull request has nothing to waive" and "the read of the `labeled` events
# FAILED" — and both left the same empty file and the same silence. The second kills
# the entire EVIDENCE half of `ci:waive:<tier-id>` (head-binding and the
# pending-waiver horizon in gating_registry.rb), whose only symptom is a waiver that
# quietly takes until the deadline with nobody named. EVERY failure mode collapsed
# into that one silence: an authorization refusal, a rate limit, a 5xx, an absent
# `gh`, a bad `--jq`. The states are now named, and this asserts they are
# DISTINGUISHABLE — the discriminator is the pairwise difference of the reported
# line, not the presence of any single string.
#
# It also pins the direction of the degradation: unreadable evidence must never red
# the job. The waiver still has to be honoured at the deadline, because reding a
# break-glass PR for an API blip is the worse outage.
echo "== every state of the label-event feed is reported distinguishably =="

# An AUTHORIZATION refusal, in the shape `gh` reports one.
cat >"$WORK/waiver-events-403.sh" <<'EOF'
#!/usr/bin/env bash
echo "gh: Resource not accessible by integration (HTTP 403)" >&2
exit 1
EOF
chmod +x "$WORK/waiver-events-403.sh"
# A CLIENT failure: no HTTP status exists at all, and no re-run fixes it.
cat >"$WORK/waiver-events-client-fail.sh" <<'EOF'
#!/usr/bin/env bash
echo "bash: gh: command not found" >&2
exit 127
EOF
chmod +x "$WORK/waiver-events-client-fail.sh"
# A failure whose stderr survives sanitisation as NOTHING: control characters only.
# `[ -s ]` sees a non-empty file, so the detail builder takes the sanitising path and
# is left with an empty string -- the state that used to trail the diagnostic off into
# a dangling em dash. Also the portable stand-in for the abort hazard the round-3
# finding named: the sanitising pipeline must never be allowed to end the run.
printf '#!/usr/bin/env bash\nprintf "\\001\\002\\r" >&2\nexit 4\n' \
  >"$WORK/waiver-events-unprintable.sh"
chmod +x "$WORK/waiver-events-unprintable.sh"
# A HEALTHY read that carries NO waiver event. The default `--jq` selects
# `event == "labeled"` for every label, so this is the shape that made a feed of
# `needs-decision` events read as "waiver evidence read": the read works, and
# nothing in it can attribute or bind anything.
printf 'needs-decision\tpm-bot\t2025-12-30T00:00:00Z\n' >"$WORK/waiver-events-no-waiver.tsv"
# A label applied, its events UNREADABLE, then the label REMOVED mid-run. The final
# state is legitimately "nothing to waive", and the broken read must not vanish
# with the label.
cat >"$WORK/labels-then-none.sh" <<EOF
#!/usr/bin/env bash
n=\$(cat "$WORK/labels-drop.count" 2>/dev/null || echo 0)
n=\$((n + 1))
echo "\$n" >"$WORK/labels-drop.count"
[ "\$n" -le 1 ] && echo "ci:waive:beta"
exit 0
EOF
chmod +x "$WORK/labels-then-none.sh"

evidence_line() { grep 'Waiver evidence' "$1" | head -n 1 || true; }

# STATE 1: no waiver label at all — the ordinary PR.
invoke "cat $(runs_file all-pass)" "$WORK/self-ids.txt" "$FUTURE" 3
cp "$SUMMARY" "$WORK/evidence-none.md"
STATE_NONE=$(evidence_line "$WORK/evidence-none.md")
if [ "$RC" -eq 0 ] && contains "$STATE_NONE" "n/a" && ! contains "$STATE_NONE" "UNREADABLE"; then
  ok "a PR with no waiver label reports the 'nothing to waive' state explicitly"
else
  bad "the no-waiver state is not stated (rc=$RC): $STATE_NONE"
fi

# STATE 2: the feed read fine. The fixture carries two `labeled` events, of which
# exactly ONE is a `ci:waive:` labelling — the discriminator for round 2's finding:
# the feed total is NOT the number of events the evidence path can use.
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1 \
  --labels "ci:waive:beta" --waiver-events-cmd "$WAIVER_EVENTS"
cp "$SUMMARY" "$WORK/evidence-read.md"
STATE_READ=$(evidence_line "$WORK/evidence-read.md")
if [ "$RC" -eq 0 ] && contains "$STATE_READ" "READ OK" &&
   contains "$STATE_READ" '2 `labeled`' && contains "$STATE_READ" '1 of them'; then
  ok "a readable feed reports BOTH the feed total and the ci:waive: subset"
else
  bad "the readable state does not separate the feed total from the waiver events (rc=$RC): $STATE_READ"
fi
# And it must not claim the binding SUCCEEDED — a matching event only binds if its
# timestamp is at or after this head sha's first CI activity, which is a per-tier
# verdict, not a property of the read.
#
# This guard is deliberately anchored on the hedge the implementation MUST emit, not
# only on an over-claiming phrase it must not: a purely negative assertion against a
# string no code path produces passes unconditionally (the round-3 finding — the
# earlier `! contains "can be bound"` form was vacuous the moment round 2 reworded
# the line, so it would have accepted "the waiver IS bound to this head sha"). The
# positive half fails the moment the deferral to the per-tier verdict is dropped.
if contains "$STATE_READ" "decided per tier above" &&
   ! contains "$STATE_READ" "is bound" && ! contains "$STATE_READ" "can be bound"; then
  ok "the readable state defers the binding verdict per tier instead of asserting it"
else
  bad "the summary asserts a head-binding it did not observe: $STATE_READ"
fi

# STATE 3: a broken read — an authorization refusal, not an absence of labels.
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1 \
  --labels "ci:waive:beta" --waiver-events-cmd "$WORK/waiver-events-403.sh"
cp "$SUMMARY" "$WORK/evidence-unreadable.md"
STATE_BAD=$(evidence_line "$WORK/evidence-unreadable.md")
if contains "$STATE_BAD" "UNREADABLE" && contains "$STATE_BAD" "broken read"; then
  ok "an unreadable feed is reported as its own state in the job summary"
else
  bad "an unreadable feed is indistinguishable from having nothing to waive: $STATE_BAD"
fi
# THE MOST VALUABLE FACT IN THE DIAGNOSTIC: the HTTP status, which is what separates
# "the token may not read this" from "GitHub was briefly unwell".
if contains "$STATE_BAD" "HTTP 403" &&
   contains "$(cat "$WORK/evidence-unreadable.md")" 'permissions:'; then
  ok "the HTTP status is surfaced and an authorization status names the permissions block"
else
  bad "the summary does not identify a 403 as an authorization problem: $(cat "$WORK/evidence-unreadable.md")"
fi
if contains "$(cat "$WORK/evidence-unreadable.md")" 'rate limit' &&
   contains "$(cat "$WORK/evidence-unreadable.md")" '5xx'; then
  ok "the summary states which statuses are transient, so a re-run is not the reflex for all of them"
else
  bad "the summary does not separate transient statuses from authorization ones"
fi
if contains "$OUT" "::warning::" && contains "$OUT" "UNREADABLE" && contains "$OUT" "NOT an absence of waiver labels"; then
  ok "the warning annotation says WHY the feed was empty, not merely that it was"
else
  bad "the warning annotation is still ambiguous about the cause: $OUT"
fi
# THE GRACEFUL PATH, which must survive all of the above.
if [ "$RC" -eq 0 ] && contains "$OUT" "WAIVED" && contains "$OUT" "UNRESOLVED" && ! contains "$OUT" "FAIL (harness)"; then
  ok "an unreadable-evidence run still honours the waiver at the deadline and claims no name"
else
  bad "unreadable evidence changed the verdict instead of only the diagnostic (rc=$RC): $OUT"
fi

# The three lines must actually differ from one another; a shared prefix with a
# different tail is what made the states inferrable-only in the first place.
if [ -n "$STATE_NONE" ] && [ -n "$STATE_READ" ] && [ -n "$STATE_BAD" ] &&
   [ "$STATE_NONE" != "$STATE_READ" ] && [ "$STATE_NONE" != "$STATE_BAD" ] &&
   [ "$STATE_READ" != "$STATE_BAD" ]; then
  ok "the three states produce three different summary lines (pairwise distinct)"
else
  bad "two evidence states report identically: none='$STATE_NONE' read='$STATE_READ' bad='$STATE_BAD'"
fi

# A CLIENT failure reports no HTTP status at all: it is neither an authorization
# problem nor transient, and the diagnostic must not imply either. The command's
# exit status is the only fact available, so it is the one reported.
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1 \
  --labels "ci:waive:beta" --waiver-events-cmd "$WORK/waiver-events-client-fail.sh"
if [ "$RC" -eq 0 ] && contains "$OUT" "UNREADABLE" &&
   contains "$OUT" "no HTTP status reported" && contains "$OUT" "exit status 127" &&
   contains "$OUT" "command not found"; then
  ok "a client failure is reported with its exit status and message, claiming no HTTP status"
else
  bad "a client failure was mis-reported as an API status (rc=$RC): $OUT"
fi

# Stderr that sanitises down to nothing must still yield a legible diagnostic AND,
# above all, must not end the run: the verdict has to survive it exactly like every
# other unreadable shape (rc 0, WAIVED, tier UNRESOLVED).
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1 \
  --labels "ci:waive:beta" --waiver-events-cmd "$WORK/waiver-events-unprintable.sh"
if [ "$RC" -eq 0 ] && contains "$OUT" "UNREADABLE" &&
   contains "$OUT" "exit status 4" && contains "$OUT" "unprintable error output"; then
  ok "stderr that sanitises to nothing is named as such, and the run still completes"
else
  bad "unprintable stderr broke or blanked the diagnostic (rc=$RC): $OUT"
fi

# A silent non-zero exit (no stderr at all) still has to be distinguishable from
# success: the exit status is reported even when there is nothing else to report.
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1 \
  --labels "ci:waive:beta" --waiver-events-cmd "false"
if [ "$RC" -eq 0 ] && contains "$OUT" "UNREADABLE" && contains "$OUT" "exit status 1"; then
  ok "a silent non-zero read is still reported as unreadable, with its exit status"
else
  bad "a silent non-zero read was indistinguishable from a successful one (rc=$RC): $OUT"
fi

# STATE 4: a waiver label with no label-event source configured at all — the
# aggregator invoked without --repo/--pr-number. Same degradation, different cause,
# so it says so rather than borrowing the API-failure wording.
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1 --labels "ci:waive:beta"
STATE_UNCONF=$(evidence_line "$SUMMARY")
if [ "$RC" -eq 0 ] && contains "$STATE_UNCONF" "UNAVAILABLE" && ! contains "$STATE_UNCONF" "UNREADABLE"; then
  ok "a waiver with no configured event source is its own state, not an API failure"
else
  bad "the unconfigured-source state is missing or mislabelled (rc=$RC): $STATE_UNCONF"
fi
# ...and it says it ONCE. The condition cannot change during the run (the command is
# fixed at argument-parse time), so a per-poll annotation would be a dozen identical
# lines. THE DISCRIMINATOR is a multi-poll run: 3 polls, still one annotation.
invoke "cat $(runs_file one-pending)" "$WORK/self-ids.txt" "$FUTURE" 3 --labels "ci:waive:beta"
UNCONF_WARNINGS=$(printf '%s\n' "$OUT" | grep -c 'waiver evidence UNAVAILABLE' || true)
if [ "${UNCONF_WARNINGS:-0}" -eq 1 ] && contains "$(evidence_line "$SUMMARY")" "UNAVAILABLE"; then
  ok "the unconfigured warning is emitted once per run, not once per poll"
else
  bad "the unconfigured warning fired ${UNCONF_WARNINGS:-0} times across the poll loop"
fi

# STATE 5, and the reason round 2 exists: the read SUCCEEDED and carried no
# `ci:waive:` event at all. The feed total alone would have reported this as
# "evidence read" — an unproven claim in exactly the shape this issue exists to
# remove — while attribution and head-binding are as dead as under a 403.
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1 \
  --labels "ci:waive:beta" --waiver-events-cmd "cat $WORK/waiver-events-no-waiver.tsv"
STATE_NOWAIVE=$(evidence_line "$SUMMARY")
if contains "$STATE_NOWAIVE" "READ OK" && contains "$STATE_NOWAIVE" '0 `ci:waive:` labeled events' &&
   contains "$STATE_NOWAIVE" "UNRESOLVED"; then
  ok "a healthy feed with no waiver events is reported as unresolved, not as evidence read"
else
  bad "a feed of non-waiver events was reported as usable waiver evidence: $STATE_NOWAIVE"
fi
if contains "$(cat "$SUMMARY")" "neither a permission nor an API problem"; then
  ok "the no-waiver-events case rules out the causes it is not, instead of being mistaken for them"
else
  bad "the no-waiver-events case borrows the API-failure diagnosis: $(cat "$SUMMARY")"
fi
if [ "$RC" -eq 0 ] && contains "$OUT" "WAIVED" && contains "$OUT" "UNRESOLVED"; then
  ok "it too still honours the waiver at the deadline and claims no name"
else
  bad "the no-waiver-events case changed the verdict (rc=$RC): $OUT"
fi
if [ "$STATE_NOWAIVE" != "$STATE_READ" ] && [ "$STATE_NOWAIVE" != "$STATE_BAD" ]; then
  ok "the no-waiver-events line differs from both the usable-evidence and unreadable lines"
else
  bad "state 5 reports identically to another state: '$STATE_NOWAIVE'"
fi

# A BROKEN READ IS NOT ERASED BY THE LABEL GOING AWAY. Label present on poll 1 with
# an unreadable feed, removed by poll 2: the final state is legitimately "nothing to
# waive", and the earlier failures are still on the record. The tier is NOT waived
# (the label is gone), which is the correct verdict and the discriminator.
rm -f "$WORK/labels-drop.count"
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$FUTURE" 2 \
  --labels-cmd "$WORK/labels-then-none.sh" --waiver-events-cmd "$WORK/waiver-events-403.sh"
DROP_SUMMARY="$(cat "$SUMMARY")"
if [ "$RC" -ne 0 ] && contains "$DROP_SUMMARY" "no waiver label now, but" &&
   contains "$DROP_SUMMARY" "HTTP 403"; then
  ok "a waiver label removed mid-run keeps the record that its events were unreadable"
else
  bad "the broken-read history was discarded with the label (rc=$RC): $DROP_SUMMARY"
fi

invoke "cat $(runs_file one-failed)" "$WORK/self-ids.txt" "$EXPIRED" 1 --labels "ci:waive:beta" --waiver-events-cmd "$WAIVER_EVENTS"
if [ "$RC" -ne 0 ]; then ok "a waiver cannot excuse a FAILED tier"; else bad "failed+waived wrongly passed"; fi
if contains "$OUT" "cannot be waived"; then
  ok "the failed+waived case states that a failed tier cannot be waived"
else
  bad "no 'cannot be waived' diagnostic: $OUT"
fi

invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1 --labels "ci:waive:alpha" --waiver-events-cmd "$WAIVER_EVENTS"
if [ "$RC" -ne 0 ] && contains "$OUT" "beta"; then
  ok "a waiver is scoped to one tier (beta still gates)"
else
  bad "waiver leaked across tiers (rc=$RC): $OUT"
fi

invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1 --labels "ci:waive:*,ci:waive-all,waive" --waiver-events-cmd "$WAIVER_EVENTS"
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
  --labels-cmd "$WORK/labels-late.sh" --waiver-events-cmd "$WAIVER_EVENTS"
if [ "$RC" -eq 0 ]; then
  ok "a waiver applied AFTER the run started is honoured without a re-run"
else
  bad "a late waiver was invisible — the documented break-glass would be unreachable: $OUT"
fi

invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1 \
  --labels-cmd "false" --labels "ci:waive:beta" --waiver-events-cmd "$WAIVER_EVENTS"
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
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$FUTURE" 5 --labels "ci:waive:beta" --waiver-events-cmd "$WAIVER_EVENTS"
if [ "$RC" -eq 0 ] && [ ! -s "$WORK/sleep.log" ]; then
  ok "a waived absent tier does not hold a runner for the whole deadline (no polls)"
else
  bad "a waived absent tier burned the poll budget (rc=$RC, sleeps=$(wc -l <"$WORK/sleep.log"))"
fi
: >"$WORK/sleep.log"
invoke "cat $(runs_file one-pending)" "$WORK/self-ids.txt" "$FUTURE" 3 --labels "ci:waive:beta" --waiver-events-cmd "$WAIVER_EVENTS"
if [ "$RC" -eq 0 ] && [ -s "$WORK/sleep.log" ]; then
  ok "a waived PENDING tier is still waited out (it could still turn red)"
else
  bad "a waived pending tier short-circuited (rc=$RC, sleeps=$(wc -l <"$WORK/sleep.log"))"
fi

# ------------------------- the break-glass must not fight itself (round 4) --
# A registered tier subscribes to label events so its own opt-in label works, so
# applying `ci:waive:<tier>` can START the very run whose `queued` check run then
# holds the waiver hostage for the full hour. When the tier's only check run was
# minted at/after the waiver was applied, it cannot be information the waiver's
# author lacked, so the waiver resolves at once. The DISCRIMINATOR is the pair:
# the same fixture dated BEFORE the waiver must still be waited out.
echo "== a waiver resolves a tier whose only run its own label event minted =="
: >"$WORK/sleep.log"
invoke "cat $(runs_file beta-pending-after-waiver)" "$WORK/self-ids.txt" "$FUTURE" 3 \
  --labels "ci:waive:beta" --waiver-events-cmd "$WAIVER_EVENTS"
if [ "$RC" -eq 0 ] && [ ! -s "$WORK/sleep.log" ]; then
  ok "a pending run minted after the waiver resolves immediately (no polls burned)"
else
  bad "the waiver was held hostage by the run it started (rc=$RC, sleeps=$(wc -l <"$WORK/sleep.log"))"
fi
if contains "$OUT" "the one this waiver's own label event started"; then
  ok "the short-circuit states WHY it applied"
else
  bad "the horizon short-circuit is silent about its reason: $OUT"
fi
: >"$WORK/sleep.log"
invoke "cat $(runs_file beta-pending-before-waiver)" "$WORK/self-ids.txt" "$FUTURE" 3 \
  --labels "ci:waive:beta" --waiver-events-cmd "$WAIVER_EVENTS"
if [ "$RC" -eq 0 ] && [ -s "$WORK/sleep.log" ]; then
  ok "a run that PREDATES the waiver is still waited out (the horizon discriminates)"
else
  bad "the horizon swallowed a pre-existing in-flight tier (rc=$RC, sleeps=$(wc -l <"$WORK/sleep.log"))"
fi
: >"$WORK/sleep.log"
invoke "cat $(runs_file beta-pending-after-waiver)" "$WORK/self-ids.txt" "$FUTURE" 3 \
  --labels "ci:waive:beta" --waiver-events-cmd "false"
if [ "$RC" -eq 0 ] && [ -s "$WORK/sleep.log" ]; then
  ok "without a resolved label-event time there is no horizon (it can only withhold)"
else
  bad "an unresolved waiver time still short-circuited (rc=$RC, sleeps=$(wc -l <"$WORK/sleep.log"))"
fi
: >"$WORK/sleep.log"
invoke "cat $(runs_file beta-pending-late-run)" "$WORK/self-ids.txt" "$FUTURE" 3 \
  --labels "ci:waive:beta" --waiver-events-cmd "$WAIVER_EVENTS"
if [ "$RC" -eq 0 ] && [ -s "$WORK/sleep.log" ]; then
  ok "a run that started 40m after the waiver is NOT the run the waiver triggered (window bounds it)"
else
  bad "the horizon swallowed a run the waiver did not start (rc=$RC, sleeps=$(wc -l <"$WORK/sleep.log"))"
fi

# ------------- a waiver is bound to the head it was applied for (round 5) ---
# THE SHIP-BLOCKING ABUSE. `ci:waive:<tier-id>` is a label; a label survives a
# push. Read live on every poll AND honoured immediately for an absent tier, it
# waived every LATER head sha's tier in the seconds before that tier could mint
# its check run — so `required` went green in seconds and "a failed tier cannot
# be waived" became unenforceable, because the tier never got to report.
echo "== a waiver does not carry over to the next head sha =="
rm -f "$WORK/new-head.count"
: >"$WORK/sleep.log"
invoke "cat $(runs_file new-head-beta-absent)" "$WORK/self-ids.txt" "$FUTURE" 3 \
  --labels "ci:waive:beta" --waiver-events-cmd "$WAIVER_EVENTS"
if [ ! -s "$WORK/sleep.log" ]; then
  bad "a waiver applied BEFORE this head sha still resolved its absent tier on the first poll"
else
  ok "an absent tier on a head sha pushed after the waiver is polled, not waived on sight"
fi
if contains "$OUT" "bound to the head sha it was applied for"; then
  ok "the diagnostic says WHY the visible label did not resolve the tier"
else
  bad "a stale waiver is silently ineffective — worse than one that refuses out loud: $OUT"
fi

# THE DISCRIMINATOR, and the whole point: with the tier polled instead of waived,
# the tier gets to REPORT, and its failure reds the gate. Under the round-4 rule
# this run exited 0 before the failure ever appeared.
rm -f "$WORK/new-head.count"
invoke "$WORK/new-head-timeline.sh" "$WORK/self-ids.txt" "$FUTURE" 6 \
  --labels "ci:waive:beta" --waiver-events-cmd "$WAIVER_EVENTS"
if [ "$RC" -eq 1 ] && contains "$OUT" "a failed tier cannot be waived"; then
  ok "the tier reports its FAILURE on the new head and the stale waiver cannot excuse it"
else
  bad "a stale waiver bypassed a real tier failure on a new head sha (rc=$RC): $OUT"
fi

: >"$WORK/sleep.log"
invoke "cat $(runs_file new-head-forged-anchor)" "$WORK/self-ids.txt" "$FUTURE" 3 \
  --labels "ci:waive:beta" --waiver-events-cmd "$WAIVER_EVENTS"
if [ -s "$WORK/sleep.log" ]; then
  ok "an unprovenanced check run cannot back-date the head anchor and revive a stale waiver"
else
  bad "a forged 2020 timestamp dragged the head anchor back and re-enabled the bypass: $OUT"
fi

# NOT A ONE-WAY DOOR. Removing and re-applying the label produces a newer
# `labeled` event, which binds the waiver to this head — the documented remedy
# the diagnostic names, exercised.
: >"$WORK/sleep.log"
invoke "cat $(runs_file new-head-beta-absent)" "$WORK/self-ids.txt" "$FUTURE" 3 \
  --labels "ci:waive:beta" --waiver-events-cmd "cat $WORK/waiver-events-reapplied.tsv"
if [ "$RC" -eq 0 ] && [ ! -s "$WORK/sleep.log" ]; then
  ok "re-applying the label on the new head restores the immediate break-glass"
else
  bad "the documented remedy does not work (rc=$RC, sleeps=$(wc -l <"$WORK/sleep.log")): $OUT"
fi

# The deadline rule is UNCHANGED for a stale waiver: it delays a verdict, it
# never pre-empts one. (Absent at the deadline is still waived; a FAILURE at the
# deadline is not — asserted above.)
invoke "cat $(runs_file new-head-beta-absent)" "$WORK/self-ids.txt" "$EXPIRED" 1 \
  --labels "ci:waive:beta" --waiver-events-cmd "$WAIVER_EVENTS"
if [ "$RC" -eq 0 ] && contains "$OUT" "WAIVED"; then
  ok "a stale waiver still applies at the aggregation deadline (it delays, it does not pre-empt)"
else
  bad "the deadline rule was lost for a stale waiver (rc=$RC): $OUT"
fi

# ------------------------------------------- PROVENANCE (round 4, S4) ------
# A registered tier is satisfied by a check run with the declared NAME. Nothing
# stopped that check run coming from somewhere other than GitHub Actions:
# `context_uniqueness_errors` only rules out same-named jobs in other workflow
# FILES, and cannot see a check run minted through the Checks API by any app
# holding `checks:write`.
echo "== only GitHub Actions can satisfy a registered tier =="
provenance_case() {
  local fixture="$1" label="$2" needle="$3"
  invoke "cat $(runs_file "$fixture")" "$WORK/self-ids.txt" "$EXPIRED" 1
  if [ "$RC" -eq 1 ] && contains "$OUT" "beta" && contains "$OUT" "$needle"; then
    ok "$label"
  else
    bad "$label — expected a named red (rc=$RC): $OUT"
  fi
}
provenance_case beta-foreign-app \
  "a same-named check run from another app does NOT satisfy the tier" "not GitHub Actions"
provenance_case beta-no-app \
  "a check run with no identifiable producer fails closed" "cannot be established"
provenance_case beta-foreign-url \
  "an Actions-labelled run whose details_url is not an Actions run fails closed" "details_url"
provenance_case beta-forged-over-real-failure \
  "a higher-id forgery cannot SHADOW the genuine failed run" "Beta gate"

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
 {"id":8000,"app":{"slug":"github-actions","id":15368},"name":"pr-gate-core","status":"completed","conclusion":"success",
  "details_url":"https://github.com/o/r/actions/runs/700/job/8000"},
 {"id":9001,"app":{"slug":"github-actions","id":15368},"name":"pr-gate-core","status":"completed","conclusion":"skipped",
  "details_url":"https://github.com/o/r/actions/runs/777/job/9001"}
]}
JSON

cat >"$(runs_file core-recorded-failure)" <<'JSON'
{"check_runs":[
 {"id":8000,"app":{"slug":"github-actions","id":15368},"name":"pr-gate-core","status":"completed","conclusion":"failure",
  "details_url":"https://github.com/o/r/actions/runs/700/job/8000"},
 {"id":9001,"app":{"slug":"github-actions","id":15368},"name":"pr-gate-core","status":"completed","conclusion":"skipped",
  "details_url":"https://github.com/o/r/actions/runs/777/job/9001"}
]}
JSON

# THE NEAR-MISS: the ONLY `pr-gate-core` check run on the head is the one THIS
# label run skipped. It has the highest id, so without run-identity exclusion it
# would be read as the answer — and `skipped` is not `success`, but a future
# refactor that treated "found a check run" as sufficient would go green here.
cat >"$(runs_file core-only-self)" <<'JSON'
{"check_runs":[
 {"id":9001,"app":{"slug":"github-actions","id":15368},"name":"pr-gate-core","status":"completed","conclusion":"skipped",
  "details_url":"https://github.com/o/r/actions/runs/777/job/9001"}
]}
JSON

cat >"$(runs_file core-absent)" <<'JSON'
{"check_runs":[
 {"id":7000,"app":{"slug":"github-actions","id":15368},"name":"some other check","status":"completed","conclusion":"success",
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

# THE ADJACENT VARIANT of the case above: `ready_for_review` and `reopened` also
# change no bytes, so they are the obvious next candidates for reuse — and they
# are deliberately NOT granted it. The reuse surface is exactly the event class
# whose frequency motivated it (labels); everything else must re-run the core.
invoke "cat $(runs_file all-pass)" "$WORK/self-ids.txt" "$FUTURE" 3 \
  --core-result skipped --event-action ready_for_review \
  --core-runs-cmd "cat $(runs_file core-recorded-success)"
if [ "$RC" -ne 0 ] && contains "$OUT" "ready_for_review"; then
  ok "reuse is scoped to label events; ready_for_review may not skip the core"
else
  bad "ready_for_review reused the recorded core (rc=$RC): $OUT"
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
  --core-runs-cmd "cat $(runs_file core-recorded-success)" --labels "ci:waive:beta" --waiver-events-cmd "$WAIVER_EVENTS"
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

# ------------------------------------- THE MIGRATION STATE (round 3, R1) ----
# The trust-boundary fix reads the registry from the BASE ref while the emitting
# job comes from the tree THIS EVENT ran. When those disagree — a PR that renames
# a registered tier's context, deletes its workflow, or predates the tier — the
# context can NEVER arrive, and polling it to the deadline holds a runner for an
# hour to reach a verdict already known. Every case below asserts the FAST red
# AND the remedy, plus the two directions that must NOT fast-fail.

# event_tree <case> -> the workflows directory of a synthetic "tree this event
# ran". The caller writes the workflow files into it.
event_tree() { printf '%s/event-%s/.github/workflows' "$WORK" "$1"; }

mkdir -p "$(event_tree matching)" "$(event_tree renamed)" "$(event_tree deleted)" \
         "$(event_tree unparseable)" "$(event_tree computed-name)"

# The tree agrees with the base registry: both tiers' emitters are present.
for tree in matching unparseable computed-name; do
  cat >"$(event_tree "$tree")/alpha.yml" <<'YAML'
name: Alpha tier
on:
  pull_request:
    types: [opened, synchronize]
jobs:
  gate:
    name: Alpha gate
    steps:
      - run: 'true'
YAML
done
cat >"$(event_tree matching)/beta.yml" <<'YAML'
name: Beta tier
on:
  pull_request:
    types: [opened, synchronize]
jobs:
  gate:
    name: Beta gate
    steps:
      - run: 'true'
YAML
# THE RENAME: beta.yml exists but now emits a different context. The head is
# self-consistent (its own registry would name `Beta gate v2`), so `pr-gate-core`
# passes — this is the residual the base-ref fix created, and the only way to see
# it is to compare the two trees.
cat >"$(event_tree renamed)/alpha.yml" <<'YAML'
name: Alpha tier
on:
  pull_request:
    types: [opened, synchronize]
jobs:
  gate:
    name: Alpha gate
    steps:
      - run: 'true'
YAML
cat >"$(event_tree renamed)/beta.yml" <<'YAML'
name: Beta tier
on:
  pull_request:
    types: [opened, synchronize]
jobs:
  gate:
    name: Beta gate v2
    steps:
      - run: 'true'
YAML
# THE DELETION: beta.yml is not in the tree at all (an unrebased branch cut before
# the tier existed, or a PR that removed the workflow).
cp "$(event_tree renamed)/alpha.yml" "$(event_tree deleted)/alpha.yml"
# Unparseable beta.yml: INCONCLUSIVE, never a fast red.
printf 'name: Beta\non: [pull_request\n  jobs: ][\n' >"$(event_tree unparseable)/beta.yml"
# A computed job name cannot be resolved offline: also inconclusive.
cat >"$(event_tree computed-name)/beta.yml" <<'YAML'
name: Beta tier
on:
  pull_request:
    types: [opened, synchronize]
jobs:
  gate:
    name: ${{ format('Beta {0}', 'gate') }}
    steps:
      - run: 'true'
YAML

echo "== a base-registered tier the event tree cannot emit reds FAST =="
: >"$WORK/sleep.log"
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$FUTURE" 5 \
  --event-workflows-dir "$(event_tree renamed)" --event-action synchronize --base-ref main
if [ "$RC" -ne 0 ] && contains "$OUT" "MIGRATION STATE"; then
  ok "a renamed context is reported as a migration state, not polled to the deadline"
else
  bad "the renamed context did not produce a migration verdict (rc=$RC): $OUT"
fi
if [ ! -s "$WORK/sleep.log" ]; then
  ok "it reds on the FIRST poll — no runner held for an hour on a context that cannot arrive"
else
  bad "the migration state still burned the poll budget ($(wc -l <"$WORK/sleep.log") intervals)"
fi
if contains "$OUT" "rebase" && contains "$OUT" "ci:waive:beta"; then
  ok "the diagnostic names BOTH remedies (rebase, or waive a deliberate rename)"
else
  bad "the migration diagnostic does not name the remedy: $OUT"
fi

invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$FUTURE" 5 \
  --event-workflows-dir "$(event_tree deleted)" --event-action synchronize --base-ref main
if [ "$RC" -ne 0 ] && contains "$OUT" "carries no"; then
  ok "a tier whose workflow the event tree lacks entirely is a migration state too"
else
  bad "a missing tier workflow was not detected (rc=$RC): $OUT"
fi

echo "== detection can only ever FAIL, never pass =="
# A pull request controls this tree. If "the event tree cannot emit it" meant
# PASS, breaking your own tier workflow would be a one-line bypass. The evidence
# here is a tier that FAILED; the migration state must not launder it green.
invoke "cat $(runs_file one-failed)" "$WORK/self-ids.txt" "$FUTURE" 5 \
  --event-workflows-dir "$(event_tree renamed)" --event-action synchronize --base-ref main
if [ "$RC" -ne 0 ]; then
  ok "a failed tier stays failed even when the event tree could not emit it"
else
  bad "the migration check turned a failed tier green — that is a bypass"
fi

echo "== a deliberate rename can still be shipped, via the documented waiver =="
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$FUTURE" 5 \
  --event-workflows-dir "$(event_tree renamed)" --event-action synchronize --base-ref main \
  --labels "ci:waive:beta" --waiver-events-cmd "$WAIVER_EVENTS"
if [ "$RC" -eq 0 ] && contains "$OUT" "WAIVED"; then
  ok "ci:waive:<tier-id> clears a migration state (a registry change takes effect once merged)"
else
  bad "the break-glass could not clear a migration state (rc=$RC): $OUT"
fi

# THE INTERACTION of round 5's binding with round 3's migration state: a waiver
# left over from an earlier head does NOT clear a migration state, and the
# diagnostic has to say so — a label that is plainly applied and plainly not
# working is the most confusing failure this mechanism can produce.
invoke "cat $(runs_file new-head-beta-absent)" "$WORK/self-ids.txt" "$FUTURE" 5 \
  --event-workflows-dir "$(event_tree renamed)" --event-action synchronize --base-ref main \
  --labels "ci:waive:beta" --waiver-events-cmd "$WAIVER_EVENTS"
if [ "$RC" -ne 0 ] && contains "$OUT" "MIGRATION STATE" &&
   contains "$OUT" "bound to the head sha it was applied for"; then
  ok "a waiver from an earlier head does not clear a migration state, and the red explains why"
else
  bad "a stale waiver silently cleared (or silently failed to clear) a migration state (rc=$RC): $OUT"
fi

echo "== inconclusive evidence must NOT fast-fail (a false red is an outage too) =="
for tree in matching unparseable computed-name; do
  : >"$WORK/sleep.log"
  invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$FUTURE" 2 \
    --event-workflows-dir "$(event_tree "$tree")" --event-action synchronize --base-ref main
  if contains "$OUT" "MIGRATION STATE"; then
    bad "the '$tree' event tree produced a migration verdict; that reds a legitimate PR"
  else
    ok "the '$tree' event tree yields no migration verdict (the tier is polled normally)"
  fi
done
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$FUTURE" 2 \
  --event-workflows-dir "$WORK/no-such-event-tree" --event-action synchronize --base-ref main
if ! contains "$OUT" "MIGRATION STATE" && contains "$OUT" "::warning::"; then
  ok "an unavailable event tree warns and falls back to polling rather than reding"
else
  bad "a missing event tree was mishandled: $OUT"
fi

# ---- emitability is a property of the HEAD SHA, not of this event (round 5) --
# THE THREE-FIX INTERACTION. P1 subscribed the aggregator to `labeled`/
# `unlabeled` so the break-glass works on a wedged PR; round 3 added emitability
# detection keyed to "does the tier's `types:` include THIS event's activity
# type?"; the two together made every label-triggered aggregator run declare a
# perfectly healthy `types: [opened, synchronize]` tier "provably unemittable"
# and red the gate. Check runs accumulate on the HEAD SHA from whichever event
# minted them — the `matching` tree here is the same one the `synchronize` case
# above treats as healthy, so the ONLY difference is the event that started the
# aggregator.
for action in labeled unlabeled ready_for_review reopened; do
  invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$FUTURE" 2 \
    --event-workflows-dir "$(event_tree matching)" --event-action "$action" --base-ref main
  if contains "$OUT" "MIGRATION STATE"; then
    bad "a '$action' run called a synchronize-emitting tier unemittable — every label change reds the PR"
  else
    ok "a '$action' aggregator run does not mis-read a tier that emits from synchronize"
  fi
done
# Still a PROVABLE negative when it really is one: a head tree that narrowed
# `types:` to label events only can never emit the context for a head sha, and
# the enrolment rule would reject that tier outright.
mkdir -p "$(event_tree label-only)"
cp "$(event_tree matching)/alpha.yml" "$(event_tree label-only)/alpha.yml"
cat >"$(event_tree label-only)/beta.yml" <<'YAML'
name: Beta tier
on:
  pull_request:
    types: [labeled]
jobs:
  gate:
    name: Beta gate
    steps:
      - run: 'true'
YAML
invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$FUTURE" 2 \
  --event-workflows-dir "$(event_tree label-only)" --event-action synchronize --base-ref main
if contains "$OUT" "MIGRATION STATE" && contains "$OUT" "excludes every activity type"; then
  ok "a head tree whose tier fires ONLY on label events is still a provable migration state"
else
  bad "the types rule stopped detecting a genuinely unreachable context: $OUT"
fi

# The base-ref shape is validated before it reaches the registry reader.
invoke "cat $(runs_file all-pass)" "$WORK/self-ids.txt" "$FUTURE" 1 --base-ref 'main;rm -rf /'
if [ "$RC" -eq 2 ] && contains "$OUT" "not a branch ref"; then
  ok "an off-shape --base-ref fails closed as a harness error"
else
  bad "an off-shape --base-ref was accepted (rc=$RC): $OUT"
fi

# ------------------- the wiring these fixtures CANNOT prove (issue #3033) ---
# HONEST SCOPE, stated where it is easy to forget. Every case above injects
# `--waiver-events-cmd`, so this suite proves the STATE MACHINE and nothing about
# production: no fixture can show that the real
# `gh api repos/{slug}/issues/{n}/events` call succeeds with the token pr-gate.yml
# hands the aggregator. Only a live pull request carrying a `ci:waive:<tier-id>`
# label can show that, and as of #3033 none ever had — the read is skipped unless
# such a label is present, so the call had never executed in production. The states
# above are what make that first execution legible; they are not a substitute for it.
#
# The one mechanisable half — that pr-gate.yml still grants the `pull-requests: read`
# which authorizes that endpoint for a PR number — is asserted ONCE, in
# scripts/tests/test_classify_docs_only.sh (the suite that owns pr-gate.yml's
# structural contract). It is deliberately not duplicated here: one invariant, one
# home, and a copy here would have had to re-derive the workflow's shape to say
# anything true about WHY it failed.

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
  # round 3: the migration state, and the shape guard in front of it.
  invoke "cat $(runs_file one-absent)" "$WORK/self-ids.txt" "$FUTURE" 1 \
    --event-workflows-dir "$(event_tree renamed)" --event-action synchronize --base-ref main
  [ "$RC" -ne 0 ] && n=$((n + 1))
  invoke "cat $(runs_file all-pass)" "$WORK/self-ids.txt" "$FUTURE" 1 --base-ref 'main;rm -rf /'
  [ "$RC" -ne 0 ] && n=$((n + 1))
  # round 4: a check run that carries the tier's name but not its provenance.
  invoke "cat $(runs_file beta-foreign-app)" "$WORK/self-ids.txt" "$EXPIRED" 1
  [ "$RC" -ne 0 ] && n=$((n + 1))
  invoke "cat $(runs_file beta-forged-over-real-failure)" "$WORK/self-ids.txt" "$EXPIRED" 1
  [ "$RC" -ne 0 ] && n=$((n + 1))
  # round 5: a waiver left over from an earlier head sha must not pre-empt this
  # head's tier — the tier reports its failure and the label cannot excuse it.
  rm -f "$WORK/new-head.count"
  invoke "$WORK/new-head-timeline.sh" "$WORK/self-ids.txt" "$FUTURE" 6 \
    --labels "ci:waive:beta" --waiver-events-cmd "$WAIVER_EVENTS"
  [ "$RC" -ne 0 ] && n=$((n + 1))
  printf '%s' "$n"
}

echo "== non-vacuity: an always-pass stub aggregator must break this suite =="
AGG="$AGG_REAL"
REAL_FAILURES=$(count_failing_verdicts)
AGG="$WORK/stub-aggregator.sh"
STUB_FAILURES=$(count_failing_verdicts)
AGG="$AGG_REAL"
if [ "$REAL_FAILURES" -eq 13 ]; then
  ok "the real aggregator fails all 13 discriminating states"
else
  bad "the real aggregator failed only $REAL_FAILURES/13 discriminating states"
fi
if [ "$STUB_FAILURES" -eq 0 ]; then
  ok "the always-exit-0 stub fails none of them, so this suite would go RED under it"
else
  bad "the stub still 'failed' $STUB_FAILURES cases; the assertions are not driven by the aggregator"
fi

echo
echo "==== aggregate-required-tiers self-test: PASS=$PASS FAIL=$FAIL ===="
[ "$FAIL" -eq 0 ] || exit 1
