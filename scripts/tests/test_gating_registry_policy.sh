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
      # Migration detection (issue #2910 round 3): the tree THIS EVENT ran, so a
      # base-registered tier whose emitter the PR renamed or removed reds fast
      # instead of polling a context that can never arrive.
      - uses: actions/checkout@v5
        with:
          ref: ${{ github.sha }}
          path: event-tree
      - env:
          GATING_DIR: base-gating
          EVENT_WORKFLOWS_DIR: event-tree/.github/workflows
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
    if: ${{ !cancelled() }}
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

# subst <src> <dst> <find> <replace> [<find> <replace> …] — LITERAL text
# substitution, portable and fail-loud (issue #2910 round 3).
#
# Every synthetic near-miss below is made by editing a fixture, and the two
# obvious tools both have a trap:
#   * BSD/macOS `sed` (macOS is a FIRST-CLASS gate host) emits a literal `n` for a
#     `\n` in the REPLACEMENT and rejects the GNU `addr,+N` range form, so a
#     multi-line edit silently produces the wrong fixture and the case then proves
#     nothing. That is #2926's G1 class.
#   * a `python3 … || ruby …` pair means the fallback is never exercised, so it
#     rots into a path that cannot work while a doc claims it does.
# So: ONE path, ruby, which this suite already hard-requires (it SKIPs without it),
# with no python3 anywhere. And every substitution ASSERTS its needle exists — a
# fixture edit that quietly matched nothing is the same silent-no-op class.
subst() {
  local src="$1" dst="$2"
  shift 2
  ruby -e '
    src, dst = ARGV.shift(2)
    text = File.read(src)
    ARGV.each_slice(2) do |find, replace|
      abort("subst: #{src}: no occurrence of #{find.inspect}") unless text.include?(find)
      text = text.sub(find, replace)
    end
    File.write(dst, text)
  ' "$src" "$dst" "$@" || bad "subst produced no fixture for $dst (a case built on it proves nothing)"
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
subst "$BASE/registry.yml" "$DIR/registry.yml" "context: Alpha gate" "context: No such context"
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
subst "$BASE/workflows/alpha.yml" "$DIR/workflows/alpha.yml" \
  "    paths-ignore:" "    paths:" \
  "__required_ci_context_never_matches__" "cqlite-flight/**"
run_policy "$DIR"
expect_fail_named "a blocking paths: filter on a registered tier is rejected" "blocking"

DIR=$(new_case blocking-paths-ignore)
subst "$BASE/workflows/alpha.yml" "$DIR/workflows/alpha.yml" \
  "__required_ci_context_never_matches__" "docs/**"
run_policy "$DIR"
expect_fail_named "a non-sentinel paths-ignore on a registered tier is rejected" "paths-ignore"

# A `branches:` filter is the near-miss variant of the same hole: the workflow
# looks unfiltered but never fires for a PR with another base, so the context is
# permanently absent and `required` deadlocks that PR for the whole deadline.
DIR=$(new_case blocking-branches)
subst "$BASE/workflows/alpha.yml" "$DIR/workflows/alpha.yml" \
  "  pull_request:" "  pull_request:
    branches: [main]"
run_policy "$DIR"
expect_fail_named "a branches: filter on a registered tier is rejected" "branches"

# --------------------------------------------------------------------------
# `types:` is the near-miss SIBLING of the `branches:`/`paths:` fields above
# (issue #2910 P4). The branches fix was not generalised to it, which is exactly
# the shape that produced this class twice — so both directions are covered here,
# and so is the degenerate case of no pull_request trigger at all.
echo "== a registered tier must fire on every event that mints a head sha =="
DIR=$(new_case types-too-narrow)
subst "$BASE/workflows/alpha.yml" "$DIR/workflows/alpha.yml" \
  "  pull_request:" "  pull_request:
    types: [ready_for_review]"
run_policy "$DIR"
expect_fail_named "a tier that fires only on ready_for_review is rejected" "types"

# Near-miss of the near-miss: ONE of the two mandatory types present.
DIR=$(new_case types-missing-synchronize)
subst "$BASE/workflows/alpha.yml" "$DIR/workflows/alpha.yml" \
  "  pull_request:" "  pull_request:
    types: [opened]"
run_policy "$DIR"
expect_fail_named "a tier missing 'synchronize' is rejected" "synchronize"

DIR=$(new_case no-pr-trigger)
# `push:` keeps the workflow triggerable (so the rejection is about the MISSING
# pull_request trigger, not about a workflow with no `on:` at all) and drops the
# paths-ignore sentinel, which is meaningless off a PR trigger.
subst "$BASE/workflows/alpha.yml" "$DIR/workflows/alpha.yml" \
  "  pull_request:" "  push:" \
  "    paths-ignore:
      - '__required_ci_context_never_matches__'
" ""
run_policy "$DIR"
expect_fail_named "a registered tier with no pull_request trigger at all is rejected" "no \`pull_request\`"

echo "== a registered tier must not fire where the aggregator is not watching =="
DIR=$(new_case types-unobserved)
subst "$BASE/workflows/alpha.yml" "$DIR/workflows/alpha.yml" \
  "  pull_request:" "  pull_request:
    types: [opened, synchronize, milestoned]"
run_policy "$DIR"
expect_fail_named "a tier firing on an event the aggregator ignores is rejected" "milestoned"

DIR=$(new_case types-observed-subset)
subst "$BASE/workflows/alpha.yml" "$DIR/workflows/alpha.yml" \
  "  pull_request:" "  pull_request:
    types: [opened, synchronize, labeled]"
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
subst "$BASE/workflows/pr-gate.yml" "$DIR/workflows/pr-gate.yml" \
  "    types: [opened, synchronize, reopened, ready_for_review, labeled, unlabeled]
" ""
run_policy "$DIR"
expect_fail_named "an aggregator that ignores label events is rejected" "ci:waive"

echo "== the emitting job must be unconditional =="
DIR=$(new_case conditional-gate)
subst "$BASE/workflows/alpha.yml" "$DIR/workflows/alpha.yml" \
  '    if: ${{ !cancelled() }}' "    if: github.event_name == 'push'"
run_policy "$DIR"
expect_fail_named "a conditional emitting job is rejected" "must be unconditional"

# THE CANCELLATION-LAUNDERING NEAR-MISS (issue #2910 round 3). `always()` looks
# like the safest possible condition and is the one this rule used to MANDATE —
# but it runs the gate job while the run is BEING CANCELLED, when every
# `needs.<job>.result` is `cancelled`, and the gate's own `case` maps that to
# `exit 1`. The tier then concludes `failure`, not `cancelled`, so the
# aggregator's supersession grace can never fire and a routine supersession reds
# `required`. The rule must reject it BY NAME, or the fix is one careless edit
# from being undone.
DIR=$(new_case always-launders-cancellation)
subst "$BASE/workflows/alpha.yml" "$DIR/workflows/alpha.yml" \
  '    if: ${{ !cancelled() }}' '    if: always()'
run_policy "$DIR"
expect_fail_named "a bare 'always()' emitting job is rejected" "LAUNDERS"
if contains "$OUT" '!cancelled()'; then
  ok "the rejection names the condition to use instead"
else
  bad "the always() rejection does not say what to write instead: $OUT"
fi

# The near-miss that an `include?` form would accept (issue #2910 P5): a COMPOUND
# condition passes a substring test yet still skips the gate job — on every draft
# PR here — leaving the context permanently absent.
DIR=$(new_case compound-condition)
subst "$BASE/workflows/alpha.yml" "$DIR/workflows/alpha.yml" \
  '    if: ${{ !cancelled() }}' '    if: ${{ !cancelled() && github.event.pull_request.draft == false }}'
run_policy "$DIR"
expect_fail_named "a compound '!cancelled() && ...' condition is rejected" "must be unconditional"

DIR=$(new_case unwrapped-cancelled)
subst "$BASE/workflows/alpha.yml" "$DIR/workflows/alpha.yml" \
  '    if: ${{ !cancelled() }}' '    if: "!cancelled()"'
run_policy "$DIR"
if [ "$RC" -eq 0 ]; then
  ok "the equivalent quoted-scalar '!cancelled()' form is accepted (no false red)"
else
  bad "'!cancelled()' without the \${{ }} wrapper was rejected — that wedges a legitimate workflow: $OUT"
fi

# GitHub expression function names are case-insensitive, so this is the SAME
# condition, not a near-miss to reject.
DIR=$(new_case cased-cancelled)
subst "$BASE/workflows/alpha.yml" "$DIR/workflows/alpha.yml" \
  '    if: ${{ !cancelled() }}' '    if: ${{ !Cancelled() }}'
run_policy "$DIR"
if [ "$RC" -eq 0 ]; then
  ok "'!Cancelled()' is accepted (expression functions are case-insensitive)"
else
  bad "'!Cancelled()' was rejected — a needless false red: $OUT"
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
    if: ${{ !cancelled() }}
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
    if: \${{ !cancelled() }}
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
    if: ${{ !cancelled() }}
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
subst "$BASE/registry.yml" "$DIR/registry.yml" \
  "    context: Alpha gate
" "    context: Alpha gate
    mandate_paths:
      - cqlite-core/**
"
run_policy "$DIR"
expect_fail_named "a mandate_paths entry absent from the workflow is rejected" "drifted"

echo "== the aggregator must not cancel and restart on every label mutation =="
DIR=$(new_case label-churn)
subst "$BASE/workflows/pr-gate.yml" "$DIR/workflows/pr-gate.yml" \
  "  group: pr-gate
" "  group: pr-gate
  cancel-in-progress: true
"
run_policy "$DIR"
expect_fail_named "label events plus cancel-in-progress: true is rejected" "cancel-in-progress"

# ROUND 4: the same rule on a REGISTERED TIER, where the consequence is worse
# than a wasted re-run — applying `ci:waive:<tier>` to a wedged PR CANCELS the
# tier's in-flight run and mints a fresh `queued` check run, and a pending tier's
# waiver is only honoured at the deadline. The break-glass fights the tier it is
# waiving. And the round-2 form could not see it: it rejected only the literal
# `true`, while `github.event_name == 'pull_request'` is TRUE for `labeled`.
echo "== a registered tier must not cancel its own in-flight run on a label event =="
tier_cancellation_case() {
  local name="$1" expr="$2" outcome="$3"
  local dir
  dir=$(new_case "$name")
  subst "$BASE/workflows/alpha.yml" "$dir/workflows/alpha.yml" \
    "on:
  pull_request:
    paths-ignore:" \
    "on:
  pull_request:
    types: [opened, synchronize, reopened, labeled, unlabeled]
    paths-ignore:" \
    "concurrency:
  group: alpha" \
    "concurrency:
  group: alpha
  cancel-in-progress: $expr"
  run_policy "$dir"
  if [ "$outcome" = reject ]; then
    expect_fail_named "a tier cancelling on label events ($expr) is rejected" "break-glass"
  elif [ "$RC" -eq 0 ]; then
    ok "a tier whose cancellation is action-aware ($expr) is accepted"
  else
    bad "a label-safe tier cancellation ($expr) was rejected: $OUT"
  fi
}
tier_cancellation_case tier-cancel-literal 'true' reject
# The EXACT expression the real flight-ci.yml carried, and the reason the
# round-2 rule was not enough.
tier_cancellation_case tier-cancel-event-name \
  "\${{ github.event_name == 'pull_request' }}" reject
tier_cancellation_case tier-cancel-action-aware \
  "\${{ github.event.action != 'labeled' && github.event.action != 'unlabeled' }}" accept
tier_cancellation_case tier-cancel-false 'false' accept

# And the REAL tier, not just a synthetic one: the rule is only worth having if
# the shipped workflow obeys it.
# Asked of the RULE itself, not of a string match: the shipped tier must be
# label-safe by the same predicate the gate enforces.
if ruby -e '
  require ARGV[0]
  wf = YAML.load_file(ARGV[1], aliases: true)
  exit(GatingRegistry.label_safe_cancellation?(wf.dig("concurrency", "cancel-in-progress")) ? 0 : 1)
' "$REPO_ROOT/scripts/ci/gating_registry.rb" "$REPO_ROOT/.github/workflows/flight-ci.yml" 2>/dev/null; then
  ok "the real flight-ci.yml does not cancel its in-flight run on a label event"
else
  bad "the real flight-ci.yml would cancel the very tier a ci:waive label is waiving"
fi

echo "== the aggregator must evaluate the mechanism from the BASE ref =="
DIR=$(new_case head-evaluated)
subst "$BASE/workflows/pr-gate.yml" "$DIR/workflows/pr-gate.yml" \
  "      - uses: actions/checkout@v5
        with:
          ref: \${{ github.event.pull_request.base.sha }}
          path: base-gating
" "" \
  "          GATING_DIR: base-gating
" "" \
  'bash "$GATING_DIR/scripts/ci/aggregate-required-tiers.sh"' "bash scripts/ci/aggregate-required-tiers.sh"
run_policy "$DIR"
expect_fail_named "an aggregator reading its own PR's copy is rejected" "BASE ref"

# ---------------------------------------------- the MIGRATION STATE (round 3)
# The base-ref fix split WHERE THE REGISTRY LIVES from WHERE THE EMITTER LIVES.
# Detecting the disagreement needs two things wired into the aggregating job, and
# either can be dropped by a future edit without any other test noticing — so
# each is its own discriminating case.
echo "== the aggregator must read the tree THIS EVENT ran, and hand it over =="
DIR=$(new_case no-event-tree-checkout)
subst "$BASE/workflows/pr-gate.yml" "$DIR/workflows/pr-gate.yml" \
  "      - uses: actions/checkout@v5
        with:
          ref: \${{ github.sha }}
          path: event-tree
" ""
run_policy "$DIR"
expect_fail_named "an aggregator that never reads the event tree is rejected" "THIS EVENT RAN"

DIR=$(new_case no-event-dir-input)
subst "$BASE/workflows/pr-gate.yml" "$DIR/workflows/pr-gate.yml" \
  "          EVENT_WORKFLOWS_DIR: event-tree/.github/workflows
" ""
run_policy "$DIR"
expect_fail_named "checking out the event tree but never passing it is rejected" "silently do nothing"

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

# ---------------------------------------------- the Node tier (issue #3640) ---
# node-ci.yml was `required`-EXEMPT on the grounds that the local agent gate's
# node-bindings component covers the merge-relevant content — true, and true ON
# LINUX ONLY, while this workflow tests ubuntu-latest, macos-14 and
# windows-latest. Owner ruling (2026-08-31): enrol with ALL THREE platforms
# gating. The structural rules above already prove the SHAPE of the enrolment
# (always-fire trigger, one unconditional emitter, closure over every job, one
# applicability output, mandate_paths <-> classifier agreement); what they cannot
# see is whether the three platform legs actually GATE, which is the entire point
# of the enrolment. So that is asserted here, against the real workflow.

echo "== the real Node tier gates all three platforms (issue #3640) =="
NODE_WF="$REPO_ROOT/.github/workflows/node-ci.yml"
if [ -f "$NODE_WF" ]; then
  ok "node-ci.yml exists"
else
  bad "node-ci.yml not found at $NODE_WF"
fi

NODE_TEST_JSON=$(ruby -ryaml -rjson -e '
  wf = YAML.load_file(ARGV[0], aliases: true)
  job = wf.dig("jobs", "test") || {}
  matrix = job.dig("strategy", "matrix") || {}
  # Both matrix spellings, so a refactor between `os: [...]` and `include:` cannot
  # silently empty this set (an empty set would report every platform missing —
  # loud, but for the wrong reason).
  oses = Array(matrix["os"]).map(&:to_s) +
         Array(matrix["include"]).map { |e| e.is_a?(Hash) ? e["os"].to_s : "" }
  print JSON.generate("if" => job["if"].to_s.gsub(/\s+/, " ").strip,
                      "coe" => job.key?("continue-on-error"),
                      "oses" => oses,
                      "needs" => Array(job["needs"]).map(&:to_s))
' "$NODE_WF" 2>/dev/null)

for platform in ubuntu-latest macos-14 windows-latest; do
  if contains "$NODE_TEST_JSON" "\"$platform\""; then
    ok "the Node tier's test matrix covers $platform"
  else
    bad "the Node tier's test matrix does NOT cover $platform; the enrolment gates fewer platforms than ruled"
  fi
done

# THE DEFECT THE RULING NAMES. `continue-on-error` greens a matrix leg whatever
# its tests did, so a workflow can carry a windows leg, report it, and gate on
# nothing — exactly "gating one platform would add nothing". No structural rule
# can see it: the gate job reads `needs.test.result`, which continue-on-error has
# already turned into `success`.
if contains "$NODE_TEST_JSON" '"coe":false'; then
  ok "the Node tier's test job carries no continue-on-error, so a platform red reds the tier"
else
  bad "the Node tier's test job carries continue-on-error; a failing platform leg would report success"
fi

# A LABEL-GATED MERGE GATE IS NOT A MERGE GATE: on a routine unlabeled pull
# request a label condition skips every platform, so the tier would report success
# having tested nothing on macOS or Windows. The condition must be the diff verdict.
if contains "$NODE_TEST_JSON" "\"if\":\"needs.classify.outputs.run_tier == 'true'\""; then
  ok "the Node tier's test job is gated on the diff verdict, not on ci:bindings-full"
else
  bad "the Node tier's test job condition is not the classifier verdict: $NODE_TEST_JSON"
fi

echo "== the real Node tier's mandate reaches the binding and the engine =="
NODE_MANDATE_RE=$(sed -n "s/^ *mandate_regex='\(.*\)'$/\1/p" "$NODE_WF")
if [ -n "$NODE_MANDATE_RE" ]; then
  ok "the Node classifier exposes a single extractable mandate regex"
else
  bad "could not extract mandate_regex from $NODE_WF"
fi
for mandated in bindings/node/src/lib.rs bindings/node/__test__/database.test.js \
                cqlite-core/src/storage/sstable/reader.rs Cargo.toml Cargo.lock \
                .github/workflows/node-ci.yml; do
  if printf '%s\n' "$mandated" | grep -Eq "$NODE_MANDATE_RE"; then
    ok "a diff touching $mandated mandates the Node tier"
  else
    bad "$mandated does NOT mandate the Node tier"
  fi
done
# The complement, or the cases above would prove nothing: an indiscriminate
# mandate matches everything and runs the cross-platform matrix on every docs PR.
for unmandated in docs/README.md cqlite-cli/src/main.rs bindings/python/src/lib.rs; do
  if printf '%s\n' "$unmandated" | grep -Eq "$NODE_MANDATE_RE"; then
    bad "$unmandated mandates the Node tier; the mandate is indiscriminate"
  else
    ok "a diff touching $unmandated does not mandate the Node tier"
  fi
done

# The label-rule exception that lets the merge-gating matrix job be diff-gated is
# scoped to a workflow that is ACTUALLY enrolled (issue #3640). Un-enrol it and
# the ci:bindings-full requirement must come back, or the registry entry and the
# workflow's applicability conditions could drift apart silently.
echo "== the binding-matrix label exception is scoped to real enrolment =="
DIR=$(new_case node-unenrolled)
cp "$NODE_WF" "$DIR/workflows/node-ci.yml"
cp "$REPO_ROOT/.github/workflows/pr-gate.yml" "$DIR/workflows/pr-gate.yml"
ruby -ryaml -e '
  reg = YAML.load_file(ARGV[0], aliases: true)
  reg["tiers"] = reg["tiers"].reject { |t| t["workflow"].to_s == "node-ci.yml" }
  (reg["exempt"] ||= []) << { "workflow" => "node-ci.yml",
                              "reason" => "temporarily un-enrolled by this test case",
                              "issue" => "#3640" }
  File.write(ARGV[1], YAML.dump(reg))
' "$REPO_ROOT/.github/ci-gating-tiers.yml" "$DIR/registry.yml"
UNENROLLED_OUT=$(cd "$REPO_ROOT" && ruby scripts/ci/validate-workflows.rb \
  --workflows-dir "$DIR/workflows" --gating-registry "$DIR/registry.yml" 2>&1 || true)
if contains "$UNENROLLED_OUT" "binding matrix job test must be gated on PR label ci:bindings-full"; then
  ok "un-enrolling node-ci.yml restores the ci:bindings-full requirement on its matrix job"
else
  bad "an un-enrolled node-ci.yml kept the tier-gated matrix exception: $UNENROLLED_OUT"
fi
ENROLLED_OUT=$(cd "$REPO_ROOT" && ruby scripts/ci/validate-workflows.rb 2>&1 || true)
if contains "$ENROLLED_OUT" "binding matrix job test must be gated"; then
  bad "the real, ENROLLED tree reports the label error; the exception does not work"
else
  ok "the real, enrolled tree accepts the diff-gated matrix job (the case discriminates)"
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

# ---------------------------------------- the declared ruby floor (round 4) --
# Round 4 deleted the python3 fallbacks, making ruby the SINGLE implementation
# path — and thereby promoting its version floor to load-bearing at the moment
# nothing verified it. macOS system ruby is 2.6; `filter_map` is 2.7 and
# `YAML.load_file(aliases:)` is psych 3.3 / ruby 3.0.

echo "== the ruby floor is declared in one place and discriminates =="
FLOOR_RB="$REPO_ROOT/scripts/ci/gating_ruby_floor.rb"
FLOOR=$(ruby -e 'require ARGV[0]; print GatingRubyFloor::FLOOR' "$FLOOR_RB" 2>/dev/null)
if [ "$FLOOR" = "3.0.0" ]; then
  ok "the declared floor is ruby $FLOOR"
else
  bad "could not read the declared floor (got '${FLOOR:-}')"
fi

# THE MUTANT: the predicate must reject every interpreter below the floor and
# accept the ones at or above it. Without both directions "satisfied?" could be a
# constant.
floor_verdicts=""
for probe in 2.6.10:no 2.7.8:no 3.0.0:yes 3.2.3:yes 4.0.0:yes garbage:no; do
  version="${probe%%:*}"
  want="${probe#*:}"
  got=$(ruby -e 'require ARGV[0]; print(GatingRubyFloor.satisfied?(ARGV[1]) ? "yes" : "no")' \
    "$FLOOR_RB" "$version" 2>/dev/null)
  [ "$got" = "$want" ] || floor_verdicts="${floor_verdicts}${version}(got ${got:-?}, want $want) "
done
if [ -z "$floor_verdicts" ]; then
  ok "the floor predicate accepts >= 3.0 and rejects 2.6/2.7 and an unparseable version"
else
  bad "the floor predicate mis-judged: $floor_verdicts"
fi

# The message must NAME the remedy: a version gate that only says "no" trains
# people to delete it.
FLOOR_MSG=$(ruby -e 'require ARGV[0]; print GatingRubyFloor.message("2.6.10")' "$FLOOR_RB" 2>/dev/null)
if contains "$FLOOR_MSG" "3.0.0" && contains "$FLOOR_MSG" "2.6" && contains "$FLOOR_MSG" "filter_map"; then
  ok "the floor diagnostic names the floor, the macOS trap and the constructs that set it"
else
  bad "the floor diagnostic is not actionable: $FLOOR_MSG"
fi

# ANTI-DRIFT: every gating ruby entry point must go through that one declaration,
# or a new file using a 3.0 construct would reintroduce the silent mis-run.
missing_floor=""
for f in gating_registry.rb gating_policy_rules.rb gating_event_rules.rb \
         gating_head_emitability.rb validate-workflows.rb; do
  contains "$(cat "$REPO_ROOT/scripts/ci/$f")" 'require_relative "gating_ruby_floor"' ||
    missing_floor="${missing_floor}$f "
done
if [ -z "$missing_floor" ]; then
  ok "every gating ruby file requires the single floor declaration"
else
  bad "these gating ruby files bypass the declared floor: $missing_floor"
fi

# ---- the two type sets that must not drift apart (round 5) ------------------
# The migration check's `types:` branch is only SAFE because the enrolment rule
# forces every registered tier to subscribe to `opened` + `synchronize`
# (MANDATORY_TIER_PR_TYPES): that is what guarantees a compliant tier can never
# be called "unemittable" by a label-triggered aggregator run. If someone ever
# shrinks HEAD_PRODUCING_TYPES below the mandated set, the branch starts firing
# on compliant tiers again — the exact false red round 5 removed. Assert the
# containment rather than leaving it as a comment.
TYPE_DRIFT=$(ruby -e '
require ARGV[0]
require ARGV[1]
missing = GatingRegistry::MANDATORY_TIER_PR_TYPES -
          GatingRegistry::HeadEmitability::HEAD_PRODUCING_TYPES
print missing.sort.inspect unless missing.empty?
' "$REPO_ROOT/scripts/ci/gating_event_rules.rb" "$REPO_ROOT/scripts/ci/gating_head_emitability.rb" 2>&1)
if [ -z "$TYPE_DRIFT" ]; then
  ok "HEAD_PRODUCING_TYPES covers every activity type a registered tier must subscribe to"
else
  bad "the migration check can fast-red a COMPLIANT tier: mandated types missing from the head-producing set: $TYPE_DRIFT"
fi

# ---- every gating ruby file declares the stdlib IT uses (round 5) -----------
# THE NEAR-MISS OF THE FLOOR FIX ITSELF. `needs_closure` in gating_policy_rules.rb
# built a `Set` while only gating_registry.rb required "set" — it worked purely
# because that file requires "set" BEFORE it `require_relative`s this one. `Set`
# is not autoloaded until ruby 3.1 and the declared floor is 3.0, so on the FLOOR
# interpreter a changed load order, or loading the file standalone, raises
# NameError inside a merge gate. The check is STATIC and load-order-independent:
# if a file's own (comment-stripped) code names a stdlib constant, that file must
# require it — inheriting the require from a sibling does not count.
STDLIB_LINT="$WORK/stdlib-require-lint.rb"
cat >"$STDLIB_LINT" <<'RUBY'
# Prints one line per gating file that uses a stdlib without requiring it.
# GLOBBED, not listed: a hardcoded list is the same drift one level up — the
# next `gating_*.rb` would be added without anyone noticing it is unlinted.
root = ARGV[0]
files = (Dir.glob(File.join(root, "scripts", "ci", "gating_*.rb")) +
         [File.join(root, "scripts", "ci", "validate-workflows.rb")]).map { |p| File.basename(p) }.uniq
# require name => the constants it provides, matched only where they are USED
# (followed by `.` or `::`), never inside prose.
rules = {
  "set" => /(?<![\w:.])Set[.:\[]|\.to_set\b/,
  "yaml" => /(?<![\w:.])(?:YAML|Psych)[.:]/,
  "json" => /(?<![\w:.])JSON[.:]/,
  "time" => /(?<![\w:.])Time[.:]/,
  "optparse" => /(?<![\w:.])OptionParser[.:]/
}
files.each do |name|
  path = File.join(root, "scripts", "ci", name)
  next unless File.file?(path)

  source = File.read(path)
  # Comments and string literals are PROSE, not use: gating_ruby_floor.rb names
  # `YAML.load_file` in the diagnostic that explains the floor without ever
  # calling it. Literals are stripped LINE BY LINE (the char classes exclude
  # newlines) so a lone quote inside another literal — `text.count('"')` — can
  # only ever garble its own line, never swallow the rest of the file and turn
  # this lint into a silent constant pass.
  code = source.lines.reject { |line| line.strip.start_with?("#") }
               .map { |line| line.gsub(/"(?:[^"\\\n]|\\.)*"/, '""').gsub(/'(?:[^'\\\n]|\\.)*'/, "''") }
               .join
  declared = source.scan(/^\s*require\s+["']([a-z_\/]+)["']/).flatten
  rules.each do |lib, pattern|
    next unless code.match?(pattern)
    next if declared.include?(lib)

    puts "#{name} uses `#{lib}` but never requires it"
  end
end
RUBY
STDLIB_GAPS=$(ruby "$STDLIB_LINT" "$REPO_ROOT" 2>&1)
if [ -z "$STDLIB_GAPS" ]; then
  ok "every gating ruby file requires the stdlib its own code uses"
else
  bad "implicit stdlib dependency (breaks on the declared 3.0 floor / a standalone load): $STDLIB_GAPS"
fi

# THE MUTANT: delete the require that round 5 added and the lint must name it.
# Without this the check could be a constant "no problems".
MUTANT_TREE="$WORK/stdlib-mutant"
mkdir -p "$MUTANT_TREE/scripts/ci"
cp "$REPO_ROOT"/scripts/ci/gating_*.rb "$REPO_ROOT/scripts/ci/validate-workflows.rb" "$MUTANT_TREE/scripts/ci/"
grep -v '^require "set"$' "$REPO_ROOT/scripts/ci/gating_policy_rules.rb" \
  >"$MUTANT_TREE/scripts/ci/gating_policy_rules.rb"
MUTANT_GAPS=$(ruby "$STDLIB_LINT" "$MUTANT_TREE" 2>&1)
if contains "$MUTANT_GAPS" "gating_policy_rules.rb uses \`set\`"; then
  ok "dropping \`require \"set\"\` is caught by the lint (it is not a constant pass)"
else
  bad "the stdlib lint did not catch the deleted require: '${MUTANT_GAPS:-<nothing>}'"
fi

# ------------------------------------------- CODEOWNERS exists (round 4) -----
# The trust-boundary rationale in design.md closed by naming "CODEOWNERS on
# .github/ + scripts/ci/" as the complementary control for its one acknowledged
# residual. No CODEOWNERS file existed ANYWHERE in the repo, so
# `require_code_owner_reviews` had nothing to resolve and the named control did
# not exist. It exists now, and this keeps it from silently disappearing again.
echo "== the trust boundary has code owners =="
CODEOWNERS=""
for candidate in "$REPO_ROOT/.github/CODEOWNERS" "$REPO_ROOT/CODEOWNERS" "$REPO_ROOT/docs/CODEOWNERS"; do
  [ -f "$candidate" ] && CODEOWNERS="$candidate" && break
done
if [ -n "$CODEOWNERS" ]; then
  ok "a CODEOWNERS file exists at a location GitHub honours (${CODEOWNERS#"$REPO_ROOT"/})"
else
  bad "no CODEOWNERS at .github/CODEOWNERS, /CODEOWNERS or docs/CODEOWNERS"
fi

# `covers <file> <path>` — is there a rule whose pattern governs <path>, with at
# least one owner? Deliberately literal: only the directory-prefix forms GitHub
# honours are recognised, so a typo does not read as coverage.
codeowners_covers() {
  ruby -e '
    path, target = ARGV
    rules = File.readlines(path).filter_map do |line|
      body = line.sub(/#.*/, "").strip
      next if body.empty?

      pattern, *owners = body.split(/\s+/)
      next if owners.empty? || owners.none? { |o| o.start_with?("@") }

      pattern
    end
    prefixes = [target, "/#{target}", "#{target}**", "/#{target}**", "*"]
    exit(rules.any? { |r| prefixes.include?(r) } ? 0 : 1)
  ' "$1" "$2"
}
if [ -n "$CODEOWNERS" ]; then
  uncovered=""
  for tree in .github/ scripts/ci/; do
    codeowners_covers "$CODEOWNERS" "$tree" || uncovered="${uncovered}${tree} "
  done
  if [ -z "$uncovered" ]; then
    ok "CODEOWNERS covers both trust-boundary trees with a real owner"
  else
    bad "CODEOWNERS does not cover: $uncovered"
  fi

  # THE MUTANT: drop the `.github/` rule and the assertion above must red — a
  # coverage check that passes on a file without the rule proves nothing.
  subst "$CODEOWNERS" "$WORK/codeowners-mutant" "/.github/ @pmcfadin" "# (rule removed)"
  if codeowners_covers "$WORK/codeowners-mutant" ".github/"; then
    bad "MUTANT: the coverage check still passed with the .github/ rule removed"
  else
    ok "MUTANT: removing the .github/ rule reds the coverage check (it discriminates)"
  fi
  # An owner-less pattern is not coverage either.
  printf '/.github/\n' >"$WORK/codeowners-ownerless"
  if codeowners_covers "$WORK/codeowners-ownerless" ".github/"; then
    bad "MUTANT: a pattern with no owner counted as coverage"
  else
    ok "MUTANT: a pattern with no owner does not count as coverage"
  fi
fi

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
# The ruby FLOOR is not the thing being stubbed — validate-workflows.rb requires
# it in its own right (issue #2910 round 4) — so the real one comes along.
cp "$REPO_ROOT/scripts/ci/gating_ruby_floor.rb" "$STUB_DIR/gating_ruby_floor.rb"
cp "$VALIDATOR_RB" "$STUB_DIR/validate-workflows.rb"

count_rule_rejections() {
  local n=0 dir
  for dir in "$WORK"/case-unenrolled "$WORK"/case-dangling "$WORK"/case-self-register \
             "$WORK"/case-blind-gate "$WORK"/case-deadline-too-long \
             "$WORK"/case-types-too-narrow "$WORK"/case-types-unobserved \
             "$WORK"/case-compound-condition "$WORK"/case-always-launders-cancellation \
             "$WORK"/case-echo-only-gate-comment \
             "$WORK"/case-aggregator-no-label-events \
             "$WORK"/case-two-scopes "$WORK"/case-mandate-drift \
             "$WORK"/case-label-churn "$WORK"/case-head-evaluated \
             "$WORK"/case-no-event-tree-checkout "$WORK"/case-no-event-dir-input \
             "$WORK"/case-tier-cancel-literal "$WORK"/case-tier-cancel-event-name; do
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

if [ "$REAL_REJECTIONS" -eq 19 ]; then
  ok "the real rule rejects all 19 discriminating registries"
else
  bad "the real rule rejected only $REAL_REJECTIONS/19"
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
