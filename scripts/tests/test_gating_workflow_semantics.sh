#!/usr/bin/env bash
# test_gating_workflow_semantics.sh — what CONCLUSION does a registered tier's
# gate job actually report, and can the aggregator's supersession grace ever fire?
# (issue #2910 round 3.)
#
# WHY THIS EXISTS. Round 2 fixed the aggregator so a `cancelled` tier is
# re-polled instead of hard-failed, and proved it with fixtures carrying
# `"conclusion":"cancelled"`. Those fixtures were SYNTHETIC. The real workflow
# could not produce that conclusion: the gate job carried `if: always()`, which
# runs it even while the run is BEING CANCELLED — at which point every
# `needs.<job>.result` is `cancelled`, the gate's own `case` maps that to
# `exit 1`, and the check run concludes `failure`. A correct fix, unreachable in
# production, with a green suite over it.
#
# So this suite asserts the CHAIN, not either end of it:
#
#   workflow YAML -> (job condition under cancellation) -> job conclusion
#                 -> gating_registry.evaluate -> required's verdict
#
# The only step it cannot execute is GitHub's own scheduler; that step is
# modelled explicitly and narrowly (see the model note below), and the MUTANT
# proves the model discriminates — put `always()` back and the chain ends in
# `failure` and an instant red, exactly as it did in production.
#
# It also carries the PORTABILITY lint for this change's shell (last phase):
# #2926 mechanised a GNU-only-construct lint, but scoped it to the gate's
# `_tree_*` functions, so it cannot see these files — and its rule set has no
# rule for either construct that actually bit here.
#
# Hermetic: no network, no gh, no GitHub, no python3. Run standalone:
#   bash scripts/tests/test_gating_workflow_semantics.sh
# Or via the gate: scripts/agent-gate.sh, `tooling-tests` component.
set -uo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && cd .. && pwd)
FLIGHT_WF="$REPO_ROOT/.github/workflows/flight-ci.yml"
REGISTRY_RB="$REPO_ROOT/scripts/ci/gating_registry.rb"
REGISTRY_YML="$REPO_ROOT/.github/ci-gating-tiers.yml"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

if ! command -v ruby >/dev/null 2>&1; then
  echo "SKIP: ruby unavailable (the workflow model and the registry reader are ruby)"
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
for f in "$FLIGHT_WF" "$REGISTRY_RB" "$REGISTRY_YML"; do
  [ -f "$f" ] || { echo "FAIL - $f not found"; exit 1; }
done

WORK=$(mktemp -d "${TMPDIR:-/tmp}/gating-semantics.XXXXXX")
trap 'rm -rf "$WORK"' EXIT

contains() { case "$1" in *"$2"*) return 0 ;; *) return 1 ;; esac; }

# ------------------------------------------------------- the workflow model --
# simulate <workflow.yml> <context> <scenario> prints the check-run conclusion
# GitHub would record for the job that emits <context>.
#
# THE MODEL, stated so it can be argued with:
#   * a job-level condition is evaluated with `cancelled()` TRUE and `always()`
#     TRUE during a run cancellation — that documented behaviour is precisely what
#     makes `always()` the hazard here;
#   * a job whose condition is false during a cancellation does NOT run, and
#     GitHub records `cancelled` or `skipped` for it. Which of the two is not
#     something this repo can verify offline, so the model reports `not-run` and
#     the aggregator is asserted against BOTH spellings below, plus the case where
#     no check run appears at all;
#   * a job that DOES run executes its `run:` steps with `needs.<job>.result`
#     bound as the scenario says; a non-zero step means conclusion `failure`.
# Only the condition the enrolment rule permits and the one it now rejects are
# modelled; anything else aborts rather than guessing.
cat >"$WORK/simulate.rb" <<'RUBY'
require "yaml"

workflow_path, context, scenario, run_tier_override = ARGV
wf = YAML.load_file(workflow_path, aliases: true)
jobs = wf["jobs"] or abort("simulate: #{workflow_path} has no jobs")
_job_id, job = jobs.find { |id, j| j.is_a?(Hash) && (j["name"] || id).to_s == context }
abort("simulate: no job emits #{context.inspect}") if job.nil?

needs = Array(job["needs"]).map(&:to_s)
results =
  case scenario
  when "cancelled"    then needs.each_with_object({}) { |n, h| h[n] = "cancelled" }
  when "all-success"  then needs.each_with_object({}) { |n, h| h[n] = "success" }
  when "inapplicable", "claimed-but-skipped"
    needs.each_with_object({}) { |n, h| h[n] = n == "classify" ? "success" : "skipped" }
  when "one-failed"   then needs.each_with_object({}) { |n, h| h[n] = n == "classify" ? "success" : "failure" }
  # ONE named job failed and everything else succeeded (issue #3640). This is what
  # makes a "the gate is blind to job X" mutant discriminating: under `one-failed`
  # every leg fails, so a gate that ignored one of them would still red for the
  # others and the mutant would prove nothing.
  when /\Aonly-(.+)-failed\z/
    target = Regexp.last_match(1)
    abort("simulate: #{target.inspect} is not a dependency of #{context.inspect}") unless needs.include?(target)
    needs.each_with_object({}) { |n, h| h[n] = n == target ? "failure" : "success" }
  else abort("simulate: unknown scenario #{scenario.inspect}")
  end
# The classifier's applicability verdict. `claimed-but-skipped` is the shape
# where the verdict says the tier applies but none of the work ran; an explicit
# override models a verdict that was never written or is not a boolean.
default_verdict = scenario == "inapplicable" ? "false" : "true"
outputs = { "run_tier" => (run_tier_override || default_verdict),
            "reason" => "simulated #{scenario}" }

condition = job["if"].to_s.gsub(/\s+/, "").downcase
condition = Regexp.last_match(1) while condition.match(/\A\$\{\{(.+)\}\}\z/)
runs =
  case condition
  when "", "always()" then true
  when "!cancelled()" then scenario != "cancelled"
  else abort("simulate: refusing to model the condition #{condition.inspect}")
  end

unless runs
  # The job never starts, so no step of it can exit non-zero.
  puts "not-run"
  exit 0
end

def expand(value, results, outputs)
  value.to_s
       .gsub(/\$\{\{\s*needs\.([A-Za-z0-9_-]+)\.result\s*\}\}/) { results[Regexp.last_match(1)].to_s }
       .gsub(/\$\{\{\s*needs\.[A-Za-z0-9_-]+\.outputs\.([A-Za-z0-9_-]+)\s*\}\}/) do
         outputs[Regexp.last_match(1)].to_s
       end
end

work = ENV.fetch("SIM_WORK")
summary = File.join(work, "step-summary.md")
File.write(summary, "")
Array(job["steps"]).each_with_index do |step, index|
  next unless step.is_a?(Hash) && step["run"]

  script = File.join(work, "step-#{index}.sh")
  File.write(script, step["run"].to_s)
  env = { "GITHUB_STEP_SUMMARY" => summary, "GITHUB_OUTPUT" => File.join(work, "out.txt") }
  [job["env"], step["env"]].each do |source|
    next unless source.is_a?(Hash)

    source.each { |k, v| env[k.to_s] = expand(v, results, outputs) }
  end
  unless system(env, "bash", script, out: File::NULL, err: File::NULL)
    puts "failure"
    exit 0
  end
end
puts "success"
RUBY

simulate() {
  SIM_WORK="$WORK" ruby "$WORK/simulate.rb" "$1" "$2" "$3" ${4+"$4"} 2>"$WORK/simulate.err"
}

FLIGHT_CONTEXT="Flight tier gate"

echo "== the simulator reproduces the gate job's ordinary conclusions =="
for pair in "all-success:success" "one-failed:failure" "inapplicable:success"; do
  scenario="${pair%%:*}"
  expected="${pair#*:}"
  got=$(simulate "$FLIGHT_WF" "$FLIGHT_CONTEXT" "$scenario")
  if [ "$got" = "$expected" ]; then
    ok "scenario '$scenario' concludes '$got' (the model executes the real gate script)"
  else
    bad "scenario '$scenario' concluded '$got', expected '$expected': $(cat "$WORK/simulate.err")"
  fi
done

echo "== a CANCELLED run must not conclude 'failure' (the round-2 near-miss) =="
CANCELLED_CONCLUSION=$(simulate "$FLIGHT_WF" "$FLIGHT_CONTEXT" cancelled)
if [ "$CANCELLED_CONCLUSION" = "not-run" ]; then
  ok "the real Flight tier gate does not run during a cancellation, so it cannot report 'failure'"
elif [ "$CANCELLED_CONCLUSION" = "failure" ]; then
  bad "the real Flight tier gate LAUNDERS a cancellation into 'failure'; the grace can never fire"
else
  bad "unexpected conclusion '$CANCELLED_CONCLUSION': $(cat "$WORK/simulate.err")"
fi

# THE MUTANT: restore `always()` — the exact configuration round 2 shipped — and
# the chain must end in `failure`. Without this, the assertion above could be
# satisfied by a model that never reports `failure` at all.
cp "$FLIGHT_WF" "$WORK/flight-always.yml"
ruby -e '
  path = ARGV[0]
  text = File.read(path)
  needle = ARGV[1]
  abort("mutant: #{path} no longer carries #{needle.strip.inspect}") unless text.include?(needle)
  File.write(path, text.sub(needle, "    if: always()\n"))
' "$WORK/flight-always.yml" '    if: ${{ !cancelled() }}
' || bad "could not build the always() mutant"
MUTANT_CONCLUSION=$(simulate "$WORK/flight-always.yml" "$FLIGHT_CONTEXT" cancelled)
if [ "$MUTANT_CONCLUSION" = "failure" ]; then
  ok "MUTANT: with 'always()' restored the same cancellation concludes 'failure' (the model discriminates)"
else
  bad "MUTANT: the always() variant concluded '$MUTANT_CONCLUSION'; this suite would not have caught the bug"
fi

# ------------------------------ UNKNOWN MUST NOT READ AS PASS (round 4, S2) --
# The gate treats `skipped` as a pass because that is how an INAPPLICABLE tier
# reports itself — a claim about `run_tier`. If the classifier concludes success
# but `run_tier` is empty or not a boolean (a renamed step id, an output never
# written, a future refactor), every downstream job is `skipped`, the loop reads
# them all as passes, and the tier reports SUCCESS with "not applicable to this
# diff": the exact silent-green defect this whole issue exists to close,
# reproduced inside the tier's own gate job.
echo "== an unreadable applicability verdict reds the tier =="
for verdict in "" "maybe" "TRUE" "1"; do
  got=$(simulate "$FLIGHT_WF" "$FLIGHT_CONTEXT" inapplicable "$verdict")
  if [ "$got" = "failure" ]; then
    ok "run_tier='${verdict}' concludes 'failure' (unknown is not inapplicability)"
  else
    bad "run_tier='${verdict}' concluded '$got'; an unreadable verdict passed as 'not applicable'"
  fi
done
for verdict in "true" "false"; do
  got=$(simulate "$FLIGHT_WF" "$FLIGHT_CONTEXT" inapplicable "$verdict")
  expected=success
  [ "$verdict" = "true" ] && expected=failure   # run_tier=true with skipped work
  if [ "$got" = "$expected" ]; then
    ok "run_tier='${verdict}' with skipped work concludes '$got'"
  else
    bad "run_tier='${verdict}' with skipped work concluded '$got', expected '$expected'"
  fi
done

# THE MUTANT: delete the verdict validation and the very first case must go
# green again — otherwise the assertions above could be satisfied by a gate that
# reds for some other reason.
cp "$FLIGHT_WF" "$WORK/flight-unvalidated.yml"
cat >"$WORK/unvalidate-scope.rb" <<'RUBY'
# Restore the round-3 tail of the gate script: report the scope, trust it, pass.
path = ARGV[0]
text = File.read(path)
start = text.index("          # UNKNOWN MUST NOT READ AS PASS")
abort("mutant: the run_tier validation block is gone from #{path}") if start.nil?
tail = "          echo \"Flight tier gate: PASS\""
stop = text.index(tail, start)
abort("mutant: could not find the gate's final PASS line") if stop.nil?
text[start...stop] = <<~ROUND3
  #{'          '}if [ "${RUN_TIER:-}" != "true" ]; then
  #{'            '}echo "Flight tier not applicable to this diff (${SCOPE_REASON:-}); reporting success."
  #{'          '}fi
ROUND3
File.write(path, text)
RUBY
ruby "$WORK/unvalidate-scope.rb" "$WORK/flight-unvalidated.yml" || bad "could not build the unvalidated mutant"
MUTANT_SCOPE=$(simulate "$WORK/flight-unvalidated.yml" "$FLIGHT_CONTEXT" inapplicable "")
if [ "$MUTANT_SCOPE" = "success" ]; then
  ok "MUTANT: without the validation an empty run_tier reports SUCCESS (the bug is real and caught)"
else
  bad "MUTANT: the unvalidated gate concluded '$MUTANT_SCOPE'; this suite would not have caught S2"
fi
MUTANT_CLAIMED=$(simulate "$WORK/flight-unvalidated.yml" "$FLIGHT_CONTEXT" inapplicable "true")
if [ "$MUTANT_CLAIMED" = "success" ]; then
  ok "MUTANT: without the consistency check run_tier=true with skipped work also passes"
else
  bad "MUTANT: the unvalidated gate concluded '$MUTANT_CLAIMED' for the claimed-but-skipped shape"
fi

# ------------------------------------- the Node tier's gate job (issue #3640) --
# node-ci.yml was enrolled as a second registered tier, so the SAME chain has to
# hold for its gate job. This is not a copy of the assertions above with a
# different filename: the two gate jobs have different dependency sets (the Node
# one carries three label-gated release jobs that legitimately report `skipped` on
# a routine pull request) and therefore different consistency logic, and the model
# executes the REAL shell of whichever workflow it is pointed at.
NODE_WF="$REPO_ROOT/.github/workflows/node-ci.yml"
NODE_CONTEXT="Node.js CI Quality Gate"
if [ ! -f "$NODE_WF" ]; then
  bad "$NODE_WF not found (the Node tier cannot be simulated)"
else
  echo "== the Node tier gate job reproduces the ordinary conclusions =="
  for pair in "all-success:success" "one-failed:failure" "inapplicable:success"; do
    scenario="${pair%%:*}"
    expected="${pair#*:}"
    got=$(simulate "$NODE_WF" "$NODE_CONTEXT" "$scenario")
    if [ "$got" = "$expected" ]; then
      ok "Node scenario '$scenario' concludes '$got' (the model executes the real gate script)"
    else
      bad "Node scenario '$scenario' concluded '$got', expected '$expected': $(cat "$WORK/simulate.err")"
    fi
  done

  echo "== a CANCELLED run must not conclude 'failure' for the Node tier either =="
  NODE_CANCELLED=$(simulate "$NODE_WF" "$NODE_CONTEXT" cancelled)
  if [ "$NODE_CANCELLED" = "not-run" ]; then
    ok "the real Node tier gate does not run during a cancellation, so it cannot report 'failure'"
  else
    bad "the real Node tier gate concluded '$NODE_CANCELLED' under cancellation: $(cat "$WORK/simulate.err")"
  fi

  # The same silent-green shape as the Flight tier: every downstream job is
  # `skipped`, which the gate reads as a pass — legitimate ONLY if the classifier
  # actually said the tier does not apply. An unreadable verdict must red.
  echo "== an unreadable Node applicability verdict reds the tier =="
  for verdict in "" "maybe" "TRUE" "1"; do
    got=$(simulate "$NODE_WF" "$NODE_CONTEXT" inapplicable "$verdict")
    if [ "$got" = "failure" ]; then
      ok "Node run_tier='${verdict}' concludes 'failure' (unknown is not inapplicability)"
    else
      bad "Node run_tier='${verdict}' concluded '$got'; an unreadable verdict passed as 'not applicable'"
    fi
  done
  # run_tier=true with the platform legs skipped is the shape a mandating diff
  # must never report green on — that IS the hole #3640 closed.
  NODE_CLAIMED=$(simulate "$NODE_WF" "$NODE_CONTEXT" inapplicable "true")
  if [ "$NODE_CLAIMED" = "failure" ]; then
    ok "Node run_tier=true with skipped platform legs concludes 'failure'"
  else
    bad "Node run_tier=true with skipped platform legs concluded '$NODE_CLAIMED'"
  fi
  NODE_INAPPLICABLE=$(simulate "$NODE_WF" "$NODE_CONTEXT" inapplicable "false")
  if [ "$NODE_INAPPLICABLE" = "success" ]; then
    ok "Node run_tier=false reports the inapplicable tier as an explicit SUCCESS, not as absence"
  else
    bad "Node run_tier=false concluded '$NODE_INAPPLICABLE'; an inapplicable tier must report success"
  fi

  # THE PROPERTY THE WHOLE ENROLMENT RESTS ON: a failure of the three-platform
  # `test` job — and of that job ALONE — must red the context. That is what the
  # deleted `continue-on-error: ${{ matrix.os == 'windows-latest' }}` prevented,
  # one layer down.
  echo "== a platform-leg failure alone reds the Node tier =="
  NODE_TEST_ONLY=$(simulate "$NODE_WF" "$NODE_CONTEXT" only-test-failed)
  if [ "$NODE_TEST_ONLY" = "failure" ]; then
    ok "the Node gate reds when only the three-platform test job failed"
  else
    bad "the Node gate concluded '$NODE_TEST_ONLY' when only the test job failed: a platform red would not block a merge"
  fi

  # THE MUTANT: bind TEST_RESULT to a constant `success` — the gate-job form of
  # `continue-on-error` — and the assertion above must go green again. Without
  # this, that assertion could be satisfied by a gate that reds for some other
  # reason entirely.
  cp "$NODE_WF" "$WORK/node-blind.yml"
  ruby -e '
    path = ARGV[0]
    text = File.read(path)
    needle = "          TEST_RESULT: ${{ needs.test.result }}\n"
    abort("mutant: #{path} no longer binds needs.test.result") unless text.include?(needle)
    File.write(path, text.sub(needle, "          TEST_RESULT: success\n"))
  ' "$WORK/node-blind.yml" || bad "could not build the blind-to-test mutant"
  NODE_BLIND=$(simulate "$WORK/node-blind.yml" "$NODE_CONTEXT" only-test-failed)
  if [ "$NODE_BLIND" = "success" ]; then
    ok "MUTANT: with TEST_RESULT hard-coded the same failure reports SUCCESS (the assertion discriminates)"
  else
    bad "MUTANT: the blinded gate concluded '$NODE_BLIND'; this suite would not catch a gate blind to the platform legs"
  fi
fi

# ------------------------------------- the other half of the chain: required --
# Whatever GitHub spells the not-run conclusion, the aggregator must treat it as
# NON-TERMINAL inside the grace and as a FAILURE once the grace — or the deadline
# — is spent. `absent` covers the third possibility: no check run at all.

cat >"$WORK/registry.yml" <<'YAML'
version: 1
aggregator:
  workflow: pr-gate.yml
  job: required
defaults:
  wait_minutes: 60
tiers:
  - id: flight
    workflow: flight-ci.yml
    context: Flight tier gate
YAML

CANCELLED_AT="2026-01-01T00:00:00Z"
CANCELLED_EPOCH=1767225600
WITHIN_GRACE=$((CANCELLED_EPOCH + 60))
PAST_GRACE=$((CANCELLED_EPOCH + 4000))

runs_with_conclusion() {
  # $1 = conclusion; empty means "no check run at all".
  if [ -z "$1" ]; then
    printf '{"check_runs":[]}\n' >"$WORK/runs.json"
    return
  fi
  cat >"$WORK/runs.json" <<JSON
{"check_runs":[
 {"id":4242,"app":{"slug":"github-actions","id":15368},"name":"Flight tier gate","status":"completed","conclusion":"$1",
  "completed_at":"$CANCELLED_AT",
  "details_url":"https://github.com/o/r/actions/runs/900/job/4242"}
]}
JSON
}

# evaluate <now> [--final] -> 0 pass / 1 fail / 3 keep waiting
evaluate() {
  local now="$1"
  shift
  ruby "$REGISTRY_RB" evaluate --registry "$WORK/registry.yml" \
    --check-runs "$WORK/runs.json" --now "$now" "$@" >"$WORK/observations.tsv" 2>&1
}

echo "== every spelling of 'the gate job did not run' reaches the grace path =="
for conclusion in cancelled skipped; do
  runs_with_conclusion "$conclusion"
  evaluate "$WITHIN_GRACE"
  rc=$?
  if [ "$rc" -eq 3 ]; then
    ok "a '$conclusion' tier is non-terminal inside the grace (required waits for the replacement)"
  else
    bad "a '$conclusion' tier gave verdict $rc inside the grace; a routine supersession would red required"
  fi
  evaluate "$PAST_GRACE"
  rc=$?
  if [ "$rc" -eq 1 ]; then
    ok "a '$conclusion' tier FAILS once the grace lapses (the wait is bounded, not an opening)"
  else
    bad "a '$conclusion' tier gave verdict $rc past the grace; that would be a silent green"
  fi
  evaluate "$WITHIN_GRACE" --final
  rc=$?
  if [ "$rc" -eq 1 ]; then
    ok "a '$conclusion' tier FAILS at the deadline even inside the grace"
  else
    bad "a '$conclusion' tier gave verdict $rc at the deadline; expiry must never pass"
  fi
done

runs_with_conclusion ""
evaluate "$PAST_GRACE"
if [ "$?" -eq 3 ]; then
  ok "no check run at all is non-terminal (the replacement run has not minted one yet)"
else
  bad "an absent context did not stay non-terminal before the deadline"
fi
evaluate "$PAST_GRACE" --final
if [ "$?" -eq 1 ]; then
  ok "no check run at all FAILS at the deadline"
else
  bad "an absent context did not fail at the deadline"
fi

echo "== and the laundered 'failure' is what the grace can NEVER excuse =="
runs_with_conclusion "failure"
evaluate "$WITHIN_GRACE"
if [ "$?" -eq 1 ]; then
  ok "a 'failure' conclusion is terminal immediately — which is why the mutant above wedges PRs"
else
  bad "a failed tier was treated as non-terminal; the grace has swallowed a real failure"
fi

echo "== the enrolment rule rejects the always() mutant by name =="
mkdir -p "$WORK/mutant-tree/workflows"
cp "$REPO_ROOT/.github/workflows/pr-gate.yml" "$WORK/mutant-tree/workflows/pr-gate.yml"
cp "$WORK/flight-always.yml" "$WORK/mutant-tree/workflows/flight-ci.yml"
ruby -ryaml -e '
  registry = YAML.load_file(ARGV[0], aliases: true)
  # The synthetic tree holds two workflows, so the exemptions (which name files
  # that are not there) would otherwise dominate the error list.
  registry["exempt"] = []
  File.write(ARGV[1], YAML.dump(registry))
' "$REGISTRY_YML" "$WORK/mutant-tree/registry.yml"
OUT=$(ruby "$REGISTRY_RB" policy --workflows-dir "$WORK/mutant-tree/workflows" \
  --registry "$WORK/mutant-tree/registry.yml" 2>&1)
RC=$?
if [ "$RC" -ne 0 ] && contains "$OUT" "LAUNDERS"; then
  ok "the enrolment rule reds the always() gate job, so the fix cannot be silently reverted"
else
  bad "the enrolment rule accepted the always() gate job (rc=$RC): $OUT"
fi

# ------------------------------------------------- PORTABILITY (#2926 class) --
# macOS is a FIRST-CLASS gate host. #2926 mechanised a GNU-only-construct lint,
# but it is scoped to `_tree_*` functions inside scripts/agent-gate.sh, so it
# cannot see these files — and this round found TWO constructs its rule set does
# not cover at all: a newline escape in a sed REPLACEMENT (BSD sed emits a literal
# `n`) and the GNU relative range address form (BSD sed errors). Both are here.
#
# SCOPE, honestly: this lints the shell THIS change owns, not scripts/ci/** and
# scripts/tests/** wholesale. A blanket sweep is a real refactor, not a small
# change — the existing 12 rules alone hit ~85 pre-existing sites across those two
# trees (55 of them `echo -e`), each needing an audit or an allowlist entry. That
# belongs with issue #2981 (extending the file-size ratchet to shell), together
# with the two new rules below.
GATING_SHELL="$REPO_ROOT/scripts/ci/aggregate-required-tiers.sh
$REPO_ROOT/scripts/tests/test_aggregate_required_tiers.sh
$REPO_ROOT/scripts/tests/test_gating_registry_policy.sh
$REPO_ROOT/scripts/tests/test_gating_workflow_semantics.sh"

LINT_RULES="bre-escape sed-newline-replacement sed-relative-range sed-in-place grep-perl date-d \
readlink-f sort-version sort-nul stat-gnu xargs-r find-printf mktemp-p echo-e"

lint_rule_pat() {
  case "$1" in
    bre-escape)              printf '%s' '(sed|grep)[[:space:]].*\\[tsSwWdb+|]' ;;
    sed-newline-replacement) printf '%s' 'sed[[:space:]].*s[|/#].*[|/#].*\\n' ;;
    sed-relative-range)      printf '%s' 'sed[[:space:]].*,\+[0-9]' ;;
    sed-in-place)            printf '%s' 'sed[[:space:]]+(-[a-zA-Z]+[[:space:]]+)*-i([[:space:]]|$)' ;;
    grep-perl)               printf '%s' 'grep[[:space:]]+(-[a-zA-Z]+[[:space:]]+)*-[a-zA-Z]*P([[:space:]]|$)' ;;
    date-d)                  printf '%s' 'date[[:space:]]+(-d([[:space:]]|$)|--date)' ;;
    readlink-f)              printf '%s' 'readlink[[:space:]]+-[a-zA-Z]*f' ;;
    sort-version)            printf '%s' 'sort[[:space:]]+(-[a-zA-Z]*V([[:space:]]|$)|--version-sort)' ;;
    sort-nul)                printf '%s' 'sort[[:space:]]+(-[a-zA-Z]*z([[:space:]]|$)|--zero-terminated)' ;;
    stat-gnu)                printf '%s' 'stat[[:space:]]+-c' ;;
    xargs-r)                 printf '%s' 'xargs[[:space:]]+(-[a-zA-Z]*r([[:space:]]|$)|--no-run-if-empty)' ;;
    find-printf)             printf '%s' 'find[[:space:]].*-printf' ;;
    mktemp-p)                printf '%s' 'mktemp[[:space:]]+(-p([[:space:]]|$)|--tmpdir)' ;;
    echo-e)                  printf '%s' 'echo[[:space:]]+-e([[:space:]]|$)' ;;
  esac
}

# Comments go, and so does the MUTANT CORPUS between the markers: this file
# deliberately CONTAINS one example of every banned construct, and a lint that
# flagged its own test data would be unusable.
lint_body() {
  ruby -e '
    text = File.read(ARGV[0])
    text = text.gsub(/^# lint-corpus-begin$.*?^# lint-corpus-end$/m, "")
    puts text.lines.map { |line| line.sub(/[[:space:]]*#.*$/, "") }.join
  ' "$1"
}

lint_file() {
  local file="$1" rule hit found="" body
  body=$(lint_body "$file")
  for rule in $LINT_RULES; do
    hit=$(printf '%s\n' "$body" | grep -nE "$(lint_rule_pat "$rule")" | head -1)
    [ -n "$hit" ] && found="${found}${file##*/}/${rule}: ${hit}"$'\n'
  done
  [ -z "$found" ] || { printf '%s' "$found"; return 1; }
  return 0
}

echo "== no GNU-only construct in this change's shell =="
lint_found=""
while IFS= read -r file; do
  [ -n "$file" ] || continue
  hits=$(lint_file "$file") || lint_found="${lint_found}${hits}"
done <<EOF
$GATING_SHELL
EOF
n_lint_rules=$(printf '%s\n' $LINT_RULES | grep -c . | tr -d ' ')
if [ -z "$lint_found" ]; then
  ok "PORTABILITY: the gating shell is clean under all $n_lint_rules rules (4 files)"
else
  bad "PORTABILITY: GNU-only construct(s) in the gating shell:"
  printf '%s' "$lint_found"
fi

# A lint nobody has seen fail is not a lint: one mutant per rule, each checked
# against ITS OWN rule so no rule can inherit another's discrimination.
lint_caught=0
lint_total=0
while IFS='|' read -r rule line; do
  [ -n "$rule" ] || continue
  # The corpus markers are comments, not mutants.
  case "$rule" in \#*) continue ;; esac
  lint_total=$((lint_total + 1))
  printf 'f() {\n  %s\n}\n' "$line" >"$WORK/lint-mutant.sh"
  saved="$LINT_RULES"
  LINT_RULES="$rule"
  if lint_file "$WORK/lint-mutant.sh" >/dev/null 2>&1; then
    LINT_RULES="$saved"
    bad "PORTABILITY: the lint does NOT catch the '$rule' mutant ($line)"
  else
    LINT_RULES="$saved"
    lint_caught=$((lint_caught + 1))
  fi
done <<'MUTANTS'
# lint-corpus-begin
bre-escape|comm -3 a b | sed 's/^\t//'
sed-newline-replacement|sed "s|  pull_request:|  pull_request:\n    branches: [main]|" f
sed-relative-range|sed '/^    paths-ignore:$/,+1d' f
sed-in-place|sed -i 's/a/b/' f
grep-perl|grep -P '[0-9]' f
date-d|date -d @1 +%s
readlink-f|readlink -f "$1"
sort-version|sort -V f
sort-nul|git ls-files -z | sort -z
stat-gnu|stat -c '%Y' -- "$1"
xargs-r|xargs -r echo
find-printf|find . -printf '%p'
mktemp-p|mktemp -p /tmp
echo-e|echo -e "a"
# lint-corpus-end
MUTANTS
if [ "$lint_caught" -eq "$lint_total" ] && [ "$lint_total" -eq "$n_lint_rules" ] && [ "$lint_total" -eq 14 ]; then
  ok "PORTABILITY: all $lint_total rules are proved discriminating (one mutant each)"
else
  bad "PORTABILITY: only $lint_caught of $lint_total rules caught their mutant (inventory: $n_lint_rules)"
fi

# ANTI-DRIFT against #2926: this rule set must stay a SUPERSET of the gate's own,
# or a class covered there would be uncovered here while looking covered.
UPSTREAM_LINT="$REPO_ROOT/scripts/tests/test_agent_gate_tree_portability.sh"
if [ -f "$UPSTREAM_LINT" ]; then
  upstream=$(ruby -e '
    text = File.read(ARGV[0])
    block = text[/^GNU_RULES="(.*?)"/m, 1]
    abort("could not extract GNU_RULES") if block.nil?
    # A trailing backslash is a shell line continuation, not a rule name.
    puts block.split(/\s+/).reject { |rule| rule.empty? || rule == "\\" }.join(" ")
  ' "$UPSTREAM_LINT" 2>/dev/null)
  if [ -z "$upstream" ]; then
    bad "could not read #2926's GNU_RULES inventory from $UPSTREAM_LINT"
  else
    missing=""
    for rule in $upstream; do
      case " $LINT_RULES " in *" $rule "*) ;; *) missing="${missing:+$missing }$rule" ;; esac
    done
    if [ -z "$missing" ]; then
      ok "the rule set is a superset of #2926's gate lint, plus the two sed classes it lacks"
    else
      bad "#2926 lints rules this suite does not: $missing"
    fi
  fi
else
  bad "$UPSTREAM_LINT not found; the superset assertion cannot be made"
fi

echo
echo "==== gating workflow-semantics self-test: PASS=$PASS FAIL=$FAIL ===="
[ "$FAIL" -eq 0 ] || exit 1
