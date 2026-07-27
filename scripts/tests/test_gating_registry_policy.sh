#!/usr/bin/env bash
# test_gating_registry_policy.sh — the ENROLMENT rule must be unbypassable
# (issue #2910).
#
# `required` derives what it waits on from `.github/ci-gating-tiers.yml` alone.
# That registry is only as good as the rule that forces workflows into it, and
# that rule (scripts/ci/gating_policy_rules.rb, wired into
# scripts/ci/validate-workflows.rb, which runs as a step in the `pr-gate-core` job
# that the branch-protection context `required` needs and treats as an
# unconditional failure unless it succeeded) is what makes a forgotten tier a red
# instead of a silent hole.
#
# Every case here is DISCRIMINATING: it asserts a non-zero exit AND that the
# offending workflow/entry is named. The final phase proves non-vacuity by
# substituting an always-pass stub enrolment rule and asserting the negative
# cases stop failing — including through `validate-workflows.rb`'s own
# `require_relative`, so the WIRING is proven, not just the rule in isolation.
#
# Hermetic: synthetic workflow trees under a per-run mktemp namespace. No network.
#
# Run standalone:   bash scripts/tests/test_gating_registry_policy.sh
# Or via the gate:  scripts/agent-gate.sh runs it inside the `tooling-tests` component.
set -uo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && cd .. && pwd)
REGISTRY_RB="$REPO_ROOT/scripts/ci/gating_registry.rb"
VALIDATOR_RB="$REPO_ROOT/scripts/ci/validate-workflows.rb"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

if ! command -v ruby >/dev/null 2>&1; then
  echo "SKIP: ruby unavailable (the enrolment rule is ruby)"
  exit 0
fi
for f in "$REGISTRY_RB" "$VALIDATOR_RB"; do
  [ -f "$f" ] || { echo "FAIL - $f not found"; exit 1; }
done

WORK=$(mktemp -d "${TMPDIR:-/tmp}/gating-policy-selftest.XXXXXX")
trap 'rm -rf "$WORK"' EXIT

# ------------------------------------------------------------- base tree ----

BASE="$WORK/base"
mkdir -p "$BASE/workflows"

cat >"$BASE/workflows/pr-gate.yml" <<'YAML'
name: Required PR Gate
on:
  pull_request:
    types: [opened, synchronize, reopened, ready_for_review, labeled, unlabeled]
permissions:
  contents: read
concurrency:
  group: pr-gate
jobs:
  pr-gate-core:
    name: pr-gate-core
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - run: 'true'
  required:
    name: required
    needs: [pr-gate-core]
    if: always()
    runs-on: ubuntu-latest
    timeout-minutes: 75
    steps:
      # The trust boundary (issue #2910 round 2): everything that decides this
      # context is read from the BASE ref, never from the PR being gated.
      - uses: actions/checkout@v5
        with:
          ref: ${{ github.event.pull_request.base.sha }}
          path: base-gating
      - env:
          GATING_DIR: base-gating
        run: bash "$GATING_DIR/scripts/ci/aggregate-required-tiers.sh"
YAML

cat >"$BASE/workflows/alpha.yml" <<'YAML'
name: Alpha tier
on:
  pull_request:
    paths-ignore:
      - '__required_ci_context_never_matches__'
permissions:
  contents: read
concurrency:
  group: alpha
jobs:
  classify:
    name: Classify alpha
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - run: echo classify
  work:
    name: alpha work
    needs: classify
    if: needs.classify.outputs.run == 'true'
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - run: echo work
  gate:
    name: Alpha gate
    needs: [classify, work]
    if: always()
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - env:
          CLASSIFY_RESULT: ${{ needs.classify.result }}
          WORK_RESULT: ${{ needs.work.result }}
        run: |
          [ "$CLASSIFY_RESULT" = success ] || exit 1
          case "$WORK_RESULT" in success|skipped) ;; *) exit 1 ;; esac
YAML

cat >"$BASE/workflows/advisory.yml" <<'YAML'
name: Advisory lane
on:
  pull_request:
    paths:
      - 'docs/**'
permissions:
  contents: read
concurrency:
  group: advisory
jobs:
  advise:
    name: advise
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - run: echo advisory
YAML

cat >"$BASE/registry.yml" <<'YAML'
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
  - workflow: advisory.yml
    reason: Advisory docs lane that must never block a merge.
    issue: "#2910"
YAML

new_case() {
  # NOTE: `local a=1 b=$a` cannot be collapsed — `local`'s arguments are all
  # expanded before any assignment happens (and `set -u` then trips).
  local name="$1"
  local dir="$WORK/case-$name"
  rm -rf "$dir"
  cp -R "$BASE" "$dir"
  printf '%s' "$dir"
}

OUT=""
RC=0
RULE="$REGISTRY_RB"

run_policy() {
  local dir="$1"
  OUT=$(ruby "$RULE" policy --workflows-dir "$dir/workflows" --registry "$dir/registry.yml" 2>&1)
  RC=$?
}

contains() { case "$1" in *"$2"*) return 0 ;; *) return 1 ;; esac; }

expect_fail_named() {
  local label="$1" needle="$2"
  if [ "$RC" -eq 0 ]; then
    bad "$label: expected a non-zero exit, got 0 — $OUT"
    return
  fi
  if contains "$OUT" "$needle"; then
    ok "$label (names '$needle')"
  else
    bad "$label: failed but did not name '$needle' — $OUT"
  fi
}

# ------------------------------------------------------------- the rules ----

echo "== the clean base tree is enrolled =="
DIR=$(new_case clean)
run_policy "$DIR"
if [ "$RC" -eq 0 ]; then ok "a fully enrolled tree passes"; else bad "clean tree rejected: $OUT"; fi

echo "== a new PR-triggered workflow that is neither registered nor exempt =="
DIR=$(new_case unenrolled)
cat >"$DIR/workflows/brand-new-tier.yml" <<'YAML'
name: Brand new tier
on:
  pull_request:
permissions:
  contents: read
concurrency:
  group: brand-new
jobs:
  run:
    name: brand new
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - run: echo new
YAML
run_policy "$DIR"
expect_fail_named "an unenrolled PR workflow reds the gate" "brand-new-tier.yml"

echo "== a pull_request_target workflow must enrol too =="
DIR=$(new_case unenrolled-target)
cat >"$DIR/workflows/target-lane.yml" <<'YAML'
name: Target lane
on:
  pull_request_target:
    types: [closed]
permissions:
  contents: read
concurrency:
  group: target
jobs:
  run:
    name: target
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - run: echo target
YAML
run_policy "$DIR"
expect_fail_named "an unenrolled pull_request_target workflow reds the gate" "target-lane.yml"

echo "== exemptions need a reason and an issue =="
DIR=$(new_case exempt-no-reason)
cat >"$DIR/registry.yml" <<'YAML'
version: 1
aggregator: { workflow: pr-gate.yml, job: required }
defaults: { wait_minutes: 60 }
tiers:
  - { id: alpha, workflow: alpha.yml, context: Alpha gate }
exempt:
  - { workflow: advisory.yml, issue: "#2910" }
YAML
run_policy "$DIR"
expect_fail_named "an exemption without a reason is rejected" "advisory.yml"

DIR=$(new_case exempt-no-issue)
cat >"$DIR/registry.yml" <<'YAML'
version: 1
aggregator: { workflow: pr-gate.yml, job: required }
defaults: { wait_minutes: 60 }
tiers:
  - { id: alpha, workflow: alpha.yml, context: Alpha gate }
exempt:
  - { workflow: advisory.yml, reason: Advisory docs lane that must never block a merge. }
YAML
run_policy "$DIR"
expect_fail_named "an exemption without an issue reference is rejected" "issue"

echo "== a dangling registry entry =="
DIR=$(new_case dangling)
sed 's/context: Alpha gate/context: No such context/' "$BASE/registry.yml" >"$DIR/registry.yml"
run_policy "$DIR"
expect_fail_named "a context no job emits is rejected as dangling" "DANGLING"

echo "== the registry cannot register the required gate itself =="
DIR=$(new_case self-register)
cat >"$DIR/registry.yml" <<'YAML'
version: 1
aggregator: { workflow: pr-gate.yml, job: required }
defaults: { wait_minutes: 60 }
tiers:
  - { id: alpha, workflow: alpha.yml, context: Alpha gate }
  - { id: selfgate, workflow: pr-gate.yml, context: required }
exempt:
  - { workflow: advisory.yml, reason: Advisory docs lane that must never block a merge., issue: "#2910" }
YAML
run_policy "$DIR"
expect_fail_named "registering pr-gate.yml is rejected" "never wait on itself"

echo "== a registered workflow with a blocking trigger filter =="
DIR=$(new_case blocking-paths)
sed "s|    paths-ignore:|    paths:|; s|__required_ci_context_never_matches__|cqlite-flight/**|" \
  "$BASE/workflows/alpha.yml" >"$DIR/workflows/alpha.yml"
run_policy "$DIR"
expect_fail_named "a blocking paths: filter on a registered tier is rejected" "blocking"

DIR=$(new_case blocking-paths-ignore)
sed "s|__required_ci_context_never_matches__|docs/**|" "$BASE/workflows/alpha.yml" >"$DIR/workflows/alpha.yml"
run_policy "$DIR"
expect_fail_named "a non-sentinel paths-ignore on a registered tier is rejected" "paths-ignore"

# A `branches:` filter is the near-miss variant of the same hole: the workflow
# looks unfiltered but never fires for a PR with another base, so the context is
# permanently absent and `required` deadlocks that PR for the whole deadline.
DIR=$(new_case blocking-branches)
sed "s|  pull_request:|  pull_request:\\n    branches: [main]|" "$BASE/workflows/alpha.yml" >"$DIR/workflows/alpha.yml"
run_policy "$DIR"
expect_fail_named "a branches: filter on a registered tier is rejected" "branches"

# --------------------------------------------------------------------------
# `types:` is the near-miss SIBLING of the `branches:`/`paths:` fields above
# (issue #2910 P4). The branches fix was not generalised to it, which is exactly
# the shape that produced this class twice — so both directions are covered here,
# and so is the degenerate case of no pull_request trigger at all.
echo "== a registered tier must fire on every event that mints a head sha =="
DIR=$(new_case types-too-narrow)
sed "s|  pull_request:|  pull_request:\\n    types: [ready_for_review]|" "$BASE/workflows/alpha.yml" >"$DIR/workflows/alpha.yml"
run_policy "$DIR"
expect_fail_named "a tier that fires only on ready_for_review is rejected" "types"

# Near-miss of the near-miss: ONE of the two mandatory types present.
DIR=$(new_case types-missing-synchronize)
sed "s|  pull_request:|  pull_request:\\n    types: [opened]|" "$BASE/workflows/alpha.yml" >"$DIR/workflows/alpha.yml"
run_policy "$DIR"
expect_fail_named "a tier missing 'synchronize' is rejected" "synchronize"

DIR=$(new_case no-pr-trigger)
# `push:` keeps the workflow triggerable (so the rejection is about the MISSING
# pull_request trigger, not about a workflow with no `on:` at all) and drops the
# paths-ignore sentinel, which is meaningless off a PR trigger.
sed "s|  pull_request:|  push:|; /^    paths-ignore:\$/,+1d" "$BASE/workflows/alpha.yml" >"$DIR/workflows/alpha.yml"
run_policy "$DIR"
expect_fail_named "a registered tier with no pull_request trigger at all is rejected" "no \`pull_request\`"

echo "== a registered tier must not fire where the aggregator is not watching =="
DIR=$(new_case types-unobserved)
sed "s|  pull_request:|  pull_request:\\n    types: [opened, synchronize, milestoned]|" "$BASE/workflows/alpha.yml" >"$DIR/workflows/alpha.yml"
run_policy "$DIR"
expect_fail_named "a tier firing on an event the aggregator ignores is rejected" "milestoned"

DIR=$(new_case types-observed-subset)
sed "s|  pull_request:|  pull_request:\\n    types: [opened, synchronize, labeled]|" "$BASE/workflows/alpha.yml" >"$DIR/workflows/alpha.yml"
run_policy "$DIR"
if [ "$RC" -eq 0 ]; then
  ok "a tier whose types are a subset of the aggregator's is accepted (no false red)"
else
  bad "a legitimate types: subset was rejected — that wedges PRs: $OUT"
fi

echo "== the waiver break-glass must be reachable (#2910 P1) =="
# Labels are applied AFTER a run starts, and a re-run replays the original event
# payload — so an aggregator that does not subscribe to `labeled`/`unlabeled` has
# a documented escape hatch that cannot be exercised on the PRs it has wedged.
DIR=$(new_case aggregator-no-label-events)
sed "s|    types: \[opened, synchronize, reopened, ready_for_review, labeled, unlabeled\]||" \
  "$BASE/workflows/pr-gate.yml" >"$DIR/workflows/pr-gate.yml"
run_policy "$DIR"
expect_fail_named "an aggregator that ignores label events is rejected" "ci:waive"

echo "== the emitting job must be unconditional =="
DIR=$(new_case conditional-gate)
sed "s|    if: always()|    if: github.event_name == 'push'|" "$BASE/workflows/alpha.yml" >"$DIR/workflows/alpha.yml"
run_policy "$DIR"
expect_fail_named "a conditional emitting job is rejected" "always()"

# The near-miss that the include?("always()") form accepted (issue #2910 P5):
# a COMPOUND condition passes a substring test yet still skips the gate job — on
# every draft PR here — leaving the context permanently absent.
DIR=$(new_case compound-always)
sed "s|    if: always()|    if: always() \&\& github.event.pull_request.draft == false|" \
  "$BASE/workflows/alpha.yml" >"$DIR/workflows/alpha.yml"
run_policy "$DIR"
expect_fail_named "a compound 'always() && ...' condition is rejected" "compound condition"

DIR=$(new_case wrapped-always)
sed "s|    if: always()|    if: \${{ always() }}|" "$BASE/workflows/alpha.yml" >"$DIR/workflows/alpha.yml"
run_policy "$DIR"
if [ "$RC" -eq 0 ]; then
  ok "the equivalent '\${{ always() }}' form is accepted (no false red)"
else
  bad "'\${{ always() }}' was rejected — that wedges a legitimate workflow: $OUT"
fi

# GitHub expression function names are case-insensitive, so this is the SAME
# condition, not a near-miss to reject.
DIR=$(new_case cased-always)
sed "s|    if: always()|    if: Always()|" "$BASE/workflows/alpha.yml" >"$DIR/workflows/alpha.yml"
run_policy "$DIR"
if [ "$RC" -eq 0 ]; then
  ok "'Always()' is accepted (expression functions are case-insensitive)"
else
  bad "'Always()' was rejected — a needless false red: $OUT"
fi

echo "== the emitting job must reflect the tier's result (no always-green gate) =="
DIR=$(new_case blind-gate)
cat >"$DIR/workflows/alpha.yml" <<'YAML'
name: Alpha tier
on:
  pull_request:
    paths-ignore:
      - '__required_ci_context_never_matches__'
permissions:
  contents: read
concurrency:
  group: alpha
jobs:
  classify:
    name: Classify alpha
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - run: echo classify
  work:
    name: alpha work
    needs: classify
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - run: echo work
  gate:
    name: Alpha gate
    needs: [classify, work]
    if: always()
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - run: echo "always green, inspects nothing"
YAML
run_policy "$DIR"
expect_fail_named "a gate job that inspects no needs.<job>.result is rejected" "has no step that reads"

# --------------------------------------------------------------------------
# THE MUTANT the previous /exit\s+1/ substring rule accepted (issue #2910 P6):
# a gate job that binds every needs.<job>.result in env: and only ECHOES them,
# with the token `exit 1` present in a COMMENT. It reads every result, so the
# inspection rule is satisfied; it can never fail, so the tier is always green —
# a can't-fail guard inside the mechanism built to stop can't-fail tiers.
write_echo_only_gate() {
  # $1 = target workflow path, $2 = the line that must not count as a failing path
  cat >"$1" <<YAML
name: Alpha tier
on:
  pull_request:
    paths-ignore:
      - '__required_ci_context_never_matches__'
permissions:
  contents: read
concurrency:
  group: alpha
jobs:
  classify:
    name: Classify alpha
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - run: echo classify
  work:
    name: alpha work
    needs: classify
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - run: echo work
  gate:
    name: Alpha gate
    needs: [classify, work]
    if: always()
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - env:
          CLASSIFY_RESULT: \${{ needs.classify.result }}
          WORK_RESULT: \${{ needs.work.result }}
        run: |
          $2
          echo "classify=\$CLASSIFY_RESULT work=\$WORK_RESULT"
YAML
}

echo "== an always-green gate job cannot satisfy the failing-path rule =="
DIR=$(new_case echo-only-gate-comment)
write_echo_only_gate "$DIR/workflows/alpha.yml" '# on failure this would exit 1'
run_policy "$DIR"
expect_fail_named "a gate whose only 'exit 1' is in a COMMENT is rejected" "can exit non-zero"

DIR=$(new_case echo-only-gate-string)
write_echo_only_gate "$DIR/workflows/alpha.yml" 'echo "would exit 1 here"'
run_policy "$DIR"
expect_fail_named "a gate whose only 'exit 1' is inside a quoted string is rejected" "can exit non-zero"

# ...and the inverse must NOT be a false red: a real failing path on a line that
# merely CONTAINS a '#' inside quotes still counts.
DIR=$(new_case gate-hash-in-string)
write_echo_only_gate "$DIR/workflows/alpha.yml" '[ "$CLASSIFY_RESULT" = success ] || { echo "job #1 failed"; exit 1; }'
run_policy "$DIR"
if [ "$RC" -eq 0 ]; then
  ok "a real 'exit 1' on a line containing a quoted '#' still counts (no false red)"
else
  bad "a legitimate failing path was mis-read as a comment — that wedges a workflow: $OUT"
fi

echo "== every job in a registered workflow must feed the gate =="
DIR=$(new_case uncovered-job)
cat "$BASE/workflows/alpha.yml" >"$DIR/workflows/alpha.yml"
cat >>"$DIR/workflows/alpha.yml" <<'YAML'
  orphan:
    name: orphan job
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - run: echo orphan
YAML
run_policy "$DIR"
expect_fail_named "a job outside the gate's needs closure is rejected" "orphan"

echo "== the deadline must be strictly less than the aggregating job's timeout =="
DIR=$(new_case deadline-too-long)
sed 's/wait_minutes: 60/wait_minutes: 75/' "$BASE/registry.yml" >"$DIR/registry.yml"
run_policy "$DIR"
expect_fail_named "deadline == timeout is rejected" "strictly less"

DIR=$(new_case deadline-per-tier-override)
cat >"$DIR/registry.yml" <<'YAML'
version: 1
aggregator: { workflow: pr-gate.yml, job: required }
defaults: { wait_minutes: 10 }
tiers:
  - { id: alpha, workflow: alpha.yml, context: Alpha gate, wait_minutes: 90 }
exempt:
  - { workflow: advisory.yml, reason: Advisory docs lane that must never block a merge., issue: "#2910" }
YAML
run_policy "$DIR"
expect_fail_named "the effective deadline is the MAX over tiers, not the default" "90m"

echo "== an empty tiers list would make required green vacuously =="
DIR=$(new_case no-tiers)
cat >"$DIR/registry.yml" <<'YAML'
version: 1
aggregator: { workflow: pr-gate.yml, job: required }
defaults: { wait_minutes: 60 }
tiers: []
exempt:
  - { workflow: alpha.yml, reason: Advisory docs lane that must never block a merge., issue: "#2910" }
  - { workflow: advisory.yml, reason: Advisory docs lane that must never block a merge., issue: "#2910" }
YAML
run_policy "$DIR"
expect_fail_named "an empty tiers list is rejected" "at least one gating tier"

echo "== a context name is global to the commit, so it must be unique =="
DIR=$(new_case context-clash)
cat >"$DIR/workflows/impostor.yml" <<'YAML'
name: Impostor lane
on:
  pull_request:
    paths:
      - 'docs/**'
permissions:
  contents: read
concurrency:
  group: impostor
jobs:
  sneaky:
    name: Alpha gate
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - run: echo "same check-run name as the registered tier"
YAML
cat >"$DIR/registry.yml" <<'YAML'
version: 1
aggregator: { workflow: pr-gate.yml, job: required }
defaults: { wait_minutes: 60 }
tiers:
  - { id: alpha, workflow: alpha.yml, context: Alpha gate }
exempt:
  - { workflow: advisory.yml, reason: Advisory docs lane that must never block a merge., issue: "#2910" }
  - { workflow: impostor.yml, reason: Advisory docs lane that must never block a merge., issue: "#2910" }
YAML
run_policy "$DIR"
expect_fail_named "a context emitted by a job in another workflow is rejected" "impostor.yml"

echo "== a workflow cannot be both a tier and an exemption =="
DIR=$(new_case both)
cat >"$DIR/registry.yml" <<'YAML'
version: 1
aggregator: { workflow: pr-gate.yml, job: required }
defaults: { wait_minutes: 60 }
tiers:
  - { id: alpha, workflow: alpha.yml, context: Alpha gate }
exempt:
  - { workflow: alpha.yml, reason: Advisory docs lane that must never block a merge., issue: "#2910" }
  - { workflow: advisory.yml, reason: Advisory docs lane that must never block a merge., issue: "#2910" }
YAML
run_policy "$DIR"
expect_fail_named "a workflow listed twice is rejected" "both a gating tier and an exemption"

echo "== the real repository tree is enrolled =="
OUT=$(ruby "$REGISTRY_RB" policy --workflows-dir "$REPO_ROOT/.github/workflows" \
  --registry "$REPO_ROOT/.github/ci-gating-tiers.yml" 2>&1)
if [ "$?" -eq 0 ]; then ok "the real .github tree satisfies the enrolment rule"; else bad "real tree unenrolled: $OUT"; fi

# --------------------------------------------- round-2 structural rules -----
# Each of these is the mechanised form of a hole found by review: the near-miss
# is expressed as a synthetic tree, and the rule must NAME it.

echo "== a tier gated on more than one classifier output is rejected =="
DIR=$(new_case two-scopes)
cat >"$DIR/workflows/alpha.yml" <<'YAML'
name: Alpha tier
on:
  pull_request:
    paths-ignore:
      - '__required_ci_context_never_matches__'
permissions:
  contents: read
concurrency:
  group: alpha
jobs:
  classify:
    name: Classify alpha
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - run: echo classify
  work:
    name: alpha work
    needs: classify
    if: needs.classify.outputs.run_cheap == 'true'
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - run: echo cheap
  heavy:
    name: alpha heavy
    needs: [classify, work]
    if: needs.classify.outputs.run_full == 'true'
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - run: echo heavy
  gate:
    name: Alpha gate
    needs: [classify, work, heavy]
    if: always()
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - env:
          CLASSIFY_RESULT: ${{ needs.classify.result }}
          WORK_RESULT: ${{ needs.work.result }}
          HEAVY_RESULT: ${{ needs.heavy.result }}
        run: |
          [ "$CLASSIFY_RESULT" = success ] || exit 1
          case "$WORK_RESULT" in success|skipped) ;; *) exit 1 ;; esac
          case "$HEAVY_RESULT" in success|skipped) ;; *) exit 1 ;; esac
YAML
run_policy "$DIR"
expect_fail_named "two applicability outputs behind one context are rejected" "MORE THAN ONE classifier output"

echo "== a documented mandate path the classifier never mentions is rejected =="
DIR=$(new_case mandate-drift)
cat >>"$DIR/registry.yml" <<'YAML'
YAML
python3 - "$DIR/registry.yml" <<'PY' 2>/dev/null || ruby -e '
  path = ARGV[0]
  text = File.read(path)
  text = text.sub("    context: Alpha gate\n", "    context: Alpha gate\n    mandate_paths:\n      - cqlite-core/**\n")
  File.write(path, text)
' "$DIR/registry.yml"
import sys
path = sys.argv[1]
text = open(path).read()
text = text.replace("    context: Alpha gate\n",
                    "    context: Alpha gate\n    mandate_paths:\n      - cqlite-core/**\n")
open(path, "w").write(text)
PY
run_policy "$DIR"
expect_fail_named "a mandate_paths entry absent from the workflow is rejected" "drifted"

echo "== the aggregator must not cancel and restart on every label mutation =="
DIR=$(new_case label-churn)
python3 - "$DIR/workflows/pr-gate.yml" <<'PY' 2>/dev/null || ruby -e '
  path = ARGV[0]
  text = File.read(path)
  File.write(path, text.sub("  group: pr-gate\n", "  group: pr-gate\n  cancel-in-progress: true\n"))
' "$DIR/workflows/pr-gate.yml"
import sys
path = sys.argv[1]
text = open(path).read()
open(path, "w").write(text.replace("  group: pr-gate\n", "  group: pr-gate\n  cancel-in-progress: true\n"))
PY
run_policy "$DIR"
expect_fail_named "label events plus cancel-in-progress: true is rejected" "cancel-in-progress"

echo "== the aggregator must evaluate the mechanism from the BASE ref =="
DIR=$(new_case head-evaluated)
python3 - "$DIR/workflows/pr-gate.yml" <<'PY' 2>/dev/null || ruby -e '
  path = ARGV[0]
  text = File.read(path)
  text = text.sub(/      # The trust boundary.*\n(?:.*\n)*?      - env:\n          GATING_DIR: base-gating\n      run/, "      - run")
  File.write(path, text)
' "$DIR/workflows/pr-gate.yml"
import sys
path = sys.argv[1]
lines = open(path).read().splitlines(True)
kept = [line for line in lines
        if "base-gating" not in line
        and "actions/checkout" not in line
        and not line.strip().startswith(("# The trust boundary", "# context is read", "with:", "ref:", "path:", "env:"))]
text = "".join(kept).replace('run: bash "$GATING_DIR/scripts/ci/aggregate-required-tiers.sh"',
                             "run: bash scripts/ci/aggregate-required-tiers.sh")
open(path, "w").write(text)
PY
run_policy "$DIR"
expect_fail_named "an aggregator reading its own PR's copy is rejected" "BASE ref"

# ---------------------------------------- the real tree, round-2 properties --
# The rules above are structural; these assert the actual shipped configuration,
# because a rule that is satisfied by a DIFFERENT shape than the one we ship
# proves nothing about this repository.

echo "== the real pr-gate.yml does not restart the core on a label mutation =="
PR_GATE="$REPO_ROOT/.github/workflows/pr-gate.yml"
CORE_IF=$(ruby -ryaml -e '
  wf = YAML.load_file(ARGV[0], aliases: true)
  print wf.dig("jobs", "pr-gate-core", "if").to_s
' "$PR_GATE")
if contains "$CORE_IF" "labeled" && contains "$CORE_IF" "unlabeled"; then
  ok "pr-gate-core is skipped for label events (no 30-minute restart)"
else
  bad "pr-gate-core still runs on label events: if='$CORE_IF'"
fi
CANCEL=$(ruby -ryaml -e '
  wf = YAML.load_file(ARGV[0], aliases: true)
  print wf.dig("concurrency", "cancel-in-progress").to_s
' "$PR_GATE")
if [ "$CANCEL" != "true" ] && contains "$CANCEL" "labeled"; then
  ok "cancellation is conditional on the event action, so label runs queue instead of cancelling"
else
  bad "pr-gate.yml cancel-in-progress is '$CANCEL'; a label mutation would cancel the in-flight gate"
fi

echo "== the real Flight tier mandates the direction Flight breaks from =="
FLIGHT_WF="$REPO_ROOT/.github/workflows/flight-ci.yml"
MANDATE_RE=$(sed -n "s/^ *mandate_regex='\(.*\)'$/\1/p" "$FLIGHT_WF")
if [ -n "$MANDATE_RE" ]; then
  ok "the Flight classifier exposes a single extractable mandate regex"
else
  bad "could not extract mandate_regex from $FLIGHT_WF"
fi
for mandated in cqlite-core/src/storage/sstable/reader.rs cqlite-flight/tests/point_read_corpus_parity_test.rs \
                Cargo.toml Cargo.lock rust-toolchain.toml test-data/datasets/x.jsonl \
                .github/actions/setup-rust-ci/action.yml .github/workflows/flight-ci.yml; do
  if printf '%s\n' "$mandated" | grep -Eq "$MANDATE_RE"; then
    ok "a diff touching $mandated mandates the Flight tier"
  else
    bad "$mandated does NOT mandate the Flight tier — the #2906 class is open for it"
  fi
done
for unmandated in docs/README.md cqlite-cli/src/main.rs bindings/python/src/lib.rs; do
  if printf '%s\n' "$unmandated" | grep -Eq "$MANDATE_RE"; then
    bad "$unmandated mandates the Flight tier; the mandate is indiscriminate, so the cases above prove nothing"
  else
    ok "a diff touching $unmandated does not mandate the Flight tier"
  fi
done
# The mandate must reach the job that owns the END-TO-END tests, not just the
# cheap one — that gap is exactly what round 2 found.
FULL_IF=$(ruby -ryaml -e '
  wf = YAML.load_file(ARGV[0], aliases: true)
  jobs = wf["jobs"]
  full = jobs.find { |_, job| Array(job["steps"]).any? { |s| s["run"].to_s.match?(/cargo test --package cqlite-flight\s*$/) } }
  print full ? full[1]["if"].to_s : "(no full-package test job)"
' "$FLIGHT_WF")
TEST_IF=$(ruby -ryaml -e '
  wf = YAML.load_file(ARGV[0], aliases: true)
  jobs = wf["jobs"]
  cheap = jobs.find { |_, job| Array(job["steps"]).any? { |s| s["run"].to_s.include?("cargo test --package cqlite-flight --lib") } }
  print cheap ? cheap[1]["if"].to_s : "(no lib test job)"
' "$FLIGHT_WF")
if [ -n "$FULL_IF" ] && [ "$FULL_IF" = "$TEST_IF" ]; then
  ok "the full-package (end-to-end) job and the --lib job share one applicability verdict: $FULL_IF"
else
  bad "the end-to-end job's condition ('$FULL_IF') differs from the --lib job's ('$TEST_IF'); a diff can reach one and not the other"
fi

# ------------------------------------------------------------- the wiring ---
# The rule only bites if validate-workflows.rb actually calls it, because THAT is
# what runs in `pr-gate-core` — the job `required` needs and refuses to pass
# without.

echo "== validate-workflows.rb carries the enrolment rule =="
DIR=$(new_case wiring)
# Deliberately clean under EVERY other validate-workflows.rb rule (scoped trigger,
# permissions, concurrency, timeouts), so the only thing that can red it is the
# enrolment rule — which is what makes the stub substitution below discriminating.
cat >"$DIR/workflows/brand-new-tier.yml" <<'YAML'
name: Brand new tier
on:
  pull_request:
    paths:
      - 'docs/**'
permissions:
  contents: read
concurrency:
  group: brand-new
jobs:
  run:
    name: brand new
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - run: echo new
YAML
run_validator() {
  local dir="$1" validator="$2"
  OUT=$(ruby "$validator" --workflows-dir "$dir/workflows" --gating-registry "$dir/registry.yml" 2>&1)
  RC=$?
}
run_validator "$DIR" "$VALIDATOR_RB"
expect_fail_named "validate-workflows.rb reds on an unenrolled workflow" "brand-new-tier.yml"

# ------------------------------------------------- non-vacuity (#2910 7.4) --

echo "== non-vacuity: an always-pass stub enrolment rule must break this suite =="
STUB_DIR="$WORK/stub-ci"
mkdir -p "$STUB_DIR"
cat >"$STUB_DIR/gating_registry.rb" <<'RUBY'
# frozen_string_literal: true
# Deliberately vacuous stand-in for the enrolment rule: it approves everything.
module GatingRegistry
  DEFAULT_REGISTRY = ".github/ci-gating-tiers.yml"
  module_function

  def policy_errors(workflows_dir: nil, registry_path: nil)
    []
  end
end

if __FILE__ == $PROGRAM_NAME
  exit 0
end
RUBY
cp "$VALIDATOR_RB" "$STUB_DIR/validate-workflows.rb"

count_rule_rejections() {
  local n=0 dir
  for dir in "$WORK"/case-unenrolled "$WORK"/case-dangling "$WORK"/case-self-register \
             "$WORK"/case-blind-gate "$WORK"/case-deadline-too-long \
             "$WORK"/case-types-too-narrow "$WORK"/case-types-unobserved \
             "$WORK"/case-compound-always "$WORK"/case-echo-only-gate-comment \
             "$WORK"/case-aggregator-no-label-events \
             "$WORK"/case-two-scopes "$WORK"/case-mandate-drift \
             "$WORK"/case-label-churn "$WORK"/case-head-evaluated; do
    run_policy "$dir"
    [ "$RC" -ne 0 ] && n=$((n + 1))
  done
  printf '%s' "$n"
}

RULE="$REGISTRY_RB"
REAL_REJECTIONS=$(count_rule_rejections)
RULE="$STUB_DIR/gating_registry.rb"
STUB_REJECTIONS=$(count_rule_rejections)
RULE="$REGISTRY_RB"

if [ "$REAL_REJECTIONS" -eq 14 ]; then
  ok "the real rule rejects all 14 discriminating registries"
else
  bad "the real rule rejected only $REAL_REJECTIONS/14"
fi
if [ "$STUB_REJECTIONS" -eq 0 ]; then
  ok "the always-pass stub rejects none, so this suite would go RED under it"
else
  bad "the stub still rejected $STUB_REJECTIONS; the assertions are not driven by the rule"
fi

run_validator "$WORK/case-wiring" "$STUB_DIR/validate-workflows.rb"
if [ "$RC" -eq 0 ]; then
  ok "validate-workflows.rb with the stub rule stops rejecting; the wiring assertion is real"
else
  bad "the stub-wired validator still failed, so the wiring assertion is not discriminating: $OUT"
fi

echo
echo "==== gating-registry policy self-test: PASS=$PASS FAIL=$FAIL ===="
[ "$FAIL" -eq 0 ] || exit 1
