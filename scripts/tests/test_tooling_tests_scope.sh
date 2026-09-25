#!/usr/bin/env bash
#
# Regression tests for the tooling-tests diff-scoping decision (issue #4266):
# scripts/lib/tooling-tests-scope.sh + its wiring into run_tooling_tests() in
# scripts/agent-gate.sh.
#
# Three layers, cheapest first:
#   1. The declared harness-path set is pinned against the LITERAL list #4266's
#      acceptance criteria name, sourced directly from the library file (no git,
#      no gate invocation) — a change to the array is caught here.
#   2. Pure classification (agent-gate.sh --tooling-tests-classify, stdin paths,
#      no git) over representative positive/negative paths, exercising the
#      SHIPPED hook rather than a re-implementation of the pattern match.
#   3. The three AC1/AC2/AC3 branches driven against REAL git state via
#      agent-gate.sh --tooling-tests-scope-line, over a scratch fixture repo
#      (never this checkout — see build_fixture below) so the git-dependent
#      base-resolution and diff-reading code paths are exercised for real.
#
# Hermetic: no network, no cargo, no datasets. A local `git init` scratch repo
# only. AGENT_GATE_SUMMARY_FILE is unset so a nested invocation can never
# clobber a parent gate's summary (#2751/#2874).
#
# Run standalone:   bash scripts/tests/test_tooling_tests_scope.sh
set -uo pipefail
unset AGENT_GATE_SUMMARY_FILE 2>/dev/null || true

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
GATE="$REPO_ROOT/scripts/agent-gate.sh"
LIB="$REPO_ROOT/scripts/lib/tooling-tests-scope.sh"
WORKFLOW="$REPO_ROOT/.github/workflows/gate.yml"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

[ -f "$LIB" ] || { echo "FATAL: $LIB not found"; exit 1; }
[ -f "$GATE" ] || { echo "FATAL: $GATE not found"; exit 1; }

# ==== Layer 1: the declared set, pinned literally (#4266 AC5) ================
# Sourced in a subshell so the array assignment cannot leak into this script.
declared_got=$(
  . "$LIB"
  printf '%s\n' "${TOOLING_TESTS_SCOPE_PATTERNS[@]}"
)
declared_want='scripts/*
.github/*
.claude/*
.roborev.toml
rust-toolchain.toml
Cargo.lock
Cargo.toml
.gitignore
CLAUDE.md
process_improvements.md
docs/*
website/src/content/docs/*
test-data/scripts/*
test-data/*.yml
test-data/*.env
test-data/perf-corpus-*
tools/*
cqlite-flight/Dockerfile
bindings/node/__test__/*'
if [ "$declared_got" = "$declared_want" ]; then
  ok "declared harness-path set matches the #4266-issue-declared list exactly (order included)"
else
  bad "declared set drifted from the issue-declared list — got:
$declared_got
wanted:
$declared_want"
fi

# ==== Layer 2: pure classification via the shipped CLI hook ==================
# classify <path> <want RUN|SKIP> <description>
classify() {
  local path="$1" want="$2" desc="$3" out verdict
  out=$(printf '%s\n' "$path" | bash "$GATE" --tooling-tests-classify 2>/dev/null)
  verdict=$(printf '%s\n' "$out" | sed -n 's/^VERDICT: //p')
  if [ "$verdict" = "$want" ]; then
    ok "$desc"
  else
    bad "$desc (got VERDICT=$verdict, wanted $want; full: $out)"
  fi
}

# Positive: every declared class, including a NESTED path under a wildcard root
# (scripts/tests/*.sh, .claude/skills/*/SKILL.md) so the glob's recursive match
# is exercised, not just a top-level literal.
classify 'scripts/agent-gate.sh'                 RUN "scripts/agent-gate.sh -> RUN"
classify 'scripts/tests/test_foo.sh'             RUN "nested scripts/tests/*.sh -> RUN"
classify 'scripts/lib/tooling-tests-scope.sh'    RUN "nested scripts/lib/*.sh -> RUN"
classify '.github/workflows/gate.yml'            RUN ".github/workflows/*.yml -> RUN"
classify '.claude/skills/sstable-parsing/SKILL.md' RUN "nested .claude/skills/*/SKILL.md -> RUN"
classify '.roborev.toml'                         RUN ".roborev.toml -> RUN"
classify 'rust-toolchain.toml'                   RUN "rust-toolchain.toml -> RUN"
classify 'Cargo.lock'                            RUN "Cargo.lock -> RUN"
classify 'Cargo.toml'                            RUN "Cargo.toml (root) -> RUN"
classify 'test-data/scripts/check-dataset-manifest.sh' RUN "test-data/scripts/* (#3493 manifest guard) -> RUN"
classify 'tools/ws0-corpus-gen/src/measurement_corpus.rs' RUN "tools/* (ws0-corpus-gen determinism oracle) -> RUN"
classify 'cqlite-flight/Dockerfile'              RUN "cqlite-flight/Dockerfile (#2870 rust-pin lockstep) -> RUN"
classify 'bindings/node/__test__/parity-utils.js' RUN "bindings/node/__test__/* -> RUN"
classify '.gitignore'                            RUN ".gitignore (test_gate_detached.sh subject derivation) -> RUN"
classify 'CLAUDE.md'                             RUN "CLAUDE.md (completion-grammar drift guard) -> RUN"
classify 'process_improvements.md'               RUN "process_improvements.md -> RUN"
classify 'docs/development/dev-cookbook.md'      RUN "nested docs/* (#4266 round-2: was wrongly SKIP) -> RUN"
classify 'docs/development/fleet-runbook.md'     RUN "docs/* (test_worker_supervisor.sh subject) -> RUN"
classify 'website/src/content/docs/index.mdx'    RUN "nested website/src/content/docs/* -> RUN"
classify 'test-data/dataset-pin.env'             RUN "test-data/*.env (schemas-preflight fixture) -> RUN"
classify 'test-data/cassandra-parity-manifest.yml' RUN "test-data/*.yml (parity-report manifest) -> RUN"
classify 'test-data/perf-corpus-bti-sample.json' RUN "test-data/perf-corpus-* -> RUN"

# Negative: production/doc/test-data paths outside the declared set, including
# three near-miss traps — `.agents/` (NOT `.claude/`), a nested `Cargo.toml`
# under a workspace member (only the exact top-level name is declared; the
# pattern is a literal, not `*/Cargo.toml`), and a repo-root doc that is
# NEITHER `CLAUDE.md` NOR under `docs/`/`website/src/content/docs/` (proving
# the new `docs/*` class did not silently swallow every markdown file).
classify 'cqlite-core/src/lib.rs'                SKIP "cqlite-core/src/** -> SKIP (AC1 subject)"
classify 'cqlite-cli/src/main.rs'                SKIP "cqlite-cli/** -> SKIP (AC1 subject)"
classify 'README.md'                             SKIP "repo-root README.md (not CLAUDE.md, not under docs/) -> SKIP"
classify 'test-data/schemas/basic.cql'           SKIP "test-data/** non-yml/env/perf-corpus -> SKIP"
classify 'bindings/python/tests/test_x.py'       SKIP "bindings/python/tests/** -> SKIP"
classify '.agents/skills/rust-patterns/SKILL.md' SKIP ".agents/** (not .claude/**) -> SKIP"
classify 'cqlite-core/Cargo.toml'                SKIP "nested Cargo.toml (not root) -> SKIP"

# ==== Layer 3: real git state via --tooling-tests-scope-line ==================
# Scratch fixture: a git repo whose OWN COPY of agent-gate.sh is the ONLY thing
# under test. Never runs against this checkout's repo (REPO_ROOT="$PWD" in
# agent-gate.sh, so a copy is required for the git-dependent branches to read a
# repo we fully control — same pattern as test_dep_duplicates_ratchet.sh's `$wt`).
TMPROOT=$(mktemp -d "${TMPDIR:-/tmp}/tooling_tests_scope.XXXXXX") || { echo "FATAL: mktemp failed"; exit 1; }
trap 'rm -rf "$TMPROOT"' EXIT
wt="$TMPROOT/wt"
mkdir -p "$wt/scripts/lib" || { echo "FATAL: mkdir $wt failed"; exit 1; }
cp "$GATE" "$wt/scripts/agent-gate.sh" || { echo "FATAL: could not copy gate into fixture"; exit 1; }
cp "$LIB" "$wt/scripts/lib/tooling-tests-scope.sh" || { echo "FATAL: could not copy lib into fixture"; exit 1; }
if [ -f "$REPO_ROOT/scripts/perf-capability.sh" ]; then
  cp "$REPO_ROOT/scripts/perf-capability.sh" "$wt/scripts/perf-capability.sh"
fi

scope_line() { # run the fixture's own gate, print DECISION/CAUSE
  ( cd "$wt" && bash scripts/agent-gate.sh --tooling-tests-scope-line 2>/dev/null )
}
decision_of() { printf '%s\n' "$1" | sed -n 's/^DECISION: //p'; }
cause_of()    { printf '%s\n' "$1" | sed -n 's/^CAUSE: //p'; }

if ! ( cd "$wt" \
    && git init -q -b main . \
    && mkdir -p cqlite-core/src \
    && echo base >cqlite-core/src/lib.rs \
    && git -c user.email=t@t -c user.name=t add -A \
    && git -c user.email=t@t -c user.name=t commit -q -m base \
    && git checkout -q -b feature ); then
  bad "could not build the layer-3 fixture repo — skipping all real-git cases"
else
  # G1 / AC1: a diff that touches ONLY cqlite-core/src/** and cqlite-cli/** must
  # SKIP, and the SUMMARY (the CAUSE line) must name the declared set. This
  # COMMITTED state (not-in-scope) is the baseline every following case builds
  # on top of, in order, so each case's fixture state is legible from the case
  # above it rather than requiring the whole file to be read to know what "HEAD"
  # is at any given point.
  ( cd "$wt" && mkdir -p cqlite-cli/src && echo x >>cqlite-core/src/lib.rs && echo x >cqlite-cli/src/main.rs \
      && git add -A && git -c user.email=t@t -c user.name=t commit -q -m "AC1: src-only diff" )
  out=$(scope_line); dec=$(decision_of "$out"); cause=$(cause_of "$out")
  if [ "$dec" = SKIP ]; then
    ok "AC1: cqlite-core/src/** + cqlite-cli/** only -> SKIP"
  else
    bad "AC1: expected SKIP, got '$dec' ($out)"
  fi
  case "$cause" in
    *"declared set: "*) ok "AC1: SKIP cause names the declared set" ;;
    *) bad "AC1: SKIP cause does not name the declared set ($cause)" ;;
  esac

  # G3: an UNTRACKED, uncommitted harness-path file also RUNs — the case a
  # roborev review on this issue found the first cut could not see at all (a
  # brand-new self-test would never itself be recognised as in-scope). Run this
  # from AC1's COMMITTED state (not-in-scope) so the working-tree leg is
  # PROVABLY what flips the decision, not a residual committed change from a
  # later case (the review's other finding on this case: the original ordering
  # put it after AC2's own commit had already put scripts/agent-gate.sh into
  # base..HEAD, so it passed for the wrong reason). Revert to the untouched
  # state afterwards and re-assert SKIP, so the RUN above is shown to come from
  # the file's presence and not from some other leftover.
  mkdir -p "$wt/scripts/tests"
  ( cd "$wt" && echo wt-only >scripts/tests/new_wt_only.sh )
  out=$(scope_line); dec=$(decision_of "$out")
  if [ "$dec" = RUN ]; then
    ok "G3: untracked, uncommitted harness path -> RUN"
  else
    bad "G3: expected RUN for an untracked harness path, got '$dec' ($out)"
  fi
  ( cd "$wt" && rm -f scripts/tests/new_wt_only.sh )
  out=$(scope_line); dec=$(decision_of "$out")
  if [ "$dec" = SKIP ]; then
    ok "G3: removing the untracked harness path reverts to SKIP (proves it was the cause)"
  else
    bad "G3: expected SKIP after removing the untracked path, got '$dec' ($out)"
  fi

  # G3b: an uncommitted change to a TRACKED harness-path file (not merely a new
  # untracked one) also RUNs — mirrors run_file_size's committed+working-tree
  # scope (a dirty tree must not be able to sneak a harness change past the
  # gate). scripts/agent-gate.sh is already tracked in the fixture (copied in
  # before the `git init`/first commit above).
  ( cd "$wt" && echo wt-tracked-only >>scripts/agent-gate.sh )
  out=$(scope_line); dec=$(decision_of "$out")
  if [ "$dec" = RUN ]; then
    ok "G3b: uncommitted change to a TRACKED harness path -> RUN"
  else
    bad "G3b: expected RUN for an uncommitted tracked harness-path change, got '$dec' ($out)"
  fi
  ( cd "$wt" && git checkout -q -- scripts/agent-gate.sh )
  out=$(scope_line); dec=$(decision_of "$out")
  if [ "$dec" = SKIP ]; then
    ok "G3b: reverting the tracked working-tree change reverts to SKIP"
  else
    bad "G3b: expected SKIP after reverting, got '$dec' ($out)"
  fi

  # G2 / AC2: COMMITTING a change to scripts/agent-gate.sh must RUN.
  ( cd "$wt" && echo touched >>scripts/agent-gate.sh \
      && git add -A && git -c user.email=t@t -c user.name=t commit -q -m "AC2: touch agent-gate.sh" )
  out=$(scope_line); dec=$(decision_of "$out")
  if [ "$dec" = RUN ]; then
    ok "AC2: scripts/agent-gate.sh in diff -> RUN"
  else
    bad "AC2: expected RUN, got '$dec' ($out)"
  fi

  # G4 / AC3: no base resolves at all (origin/main, main, origin/master, master
  # all absent) -> fail-closed RUN, cause names the fail-closed reason.
  ( cd "$wt" && git branch -D main -q )
  out=$(scope_line); dec=$(decision_of "$out"); cause=$(cause_of "$out")
  if [ "$dec" = RUN ]; then
    ok "AC3: unresolvable base -> RUN (fail-closed)"
  else
    bad "AC3: expected RUN, got '$dec' ($out)"
  fi
  case "$cause" in
    *"fail-closed"*) ok "AC3: RUN cause names fail-closed" ;;
    *) bad "AC3: RUN cause does not say fail-closed ($cause)" ;;
  esac

  # G5 / AC4: rebuild "main" pointing at HEAD itself (a clean, resolvable,
  # EMPTY diff — the exact shape the nightly gate.yml deep-check job runs
  # under, since it gates `main` against `origin/main`, which there is the same
  # commit). NEGATIVE HALF FIRST (a roborev review on this issue found the
  # positive-only case vacuous: CQLITE_TOOLING_TESTS_ALWAYS_RUN=1 returns
  # before any git call, so the fixture's git state is decorative unless the
  # UNFORCED run is also asserted to SKIP right here) — without the env var, a
  # clean/self diff must SKIP; only then does forcing it prove the override is
  # load-bearing rather than redundant with what scoping would have decided
  # anyway.
  ( cd "$wt" && git checkout -q -b main2 && git branch -m main2 main )
  out=$(scope_line); dec=$(decision_of "$out")
  if [ "$dec" = SKIP ]; then
    ok "G5: a clean/self diff (HEAD is its own merge-base) -> SKIP without the override"
  else
    bad "G5: expected SKIP for a clean/self diff, got '$dec' ($out)"
  fi
  out=$(cd "$wt" && CQLITE_TOOLING_TESTS_ALWAYS_RUN=1 bash scripts/agent-gate.sh --tooling-tests-scope-line 2>/dev/null)
  dec=$(decision_of "$out")
  if [ "$dec" = RUN ]; then
    ok "G5: CQLITE_TOOLING_TESTS_ALWAYS_RUN=1 forces RUN on that SAME clean/self diff"
  else
    bad "G5: expected RUN under CQLITE_TOOLING_TESTS_ALWAYS_RUN=1, got '$dec' ($out)"
  fi
fi

# ==== Wiring evidence (never runs the real ~80-suite body) ===================
# G6: run_tooling_tests() must call the scope decision BEFORE its first heavy
# self-test invocation, so a scoped SKIP genuinely short-circuits the ~80-suite
# body rather than merely being computed and ignored.
fn_body=$(awk '/^run_tooling_tests\(\) \{/{f=1} f{print; if (/^}/ && NR>1) exit}' "$GATE")
scope_ln=$(printf '%s\n' "$fn_body" | grep -n '_tooling_tests_scope_decide' | head -1 | cut -d: -f1)
first_suite_ln=$(printf '%s\n' "$fn_body" | grep -n 'test_generator_keyspace_scoping.sh' | head -1 | cut -d: -f1)
if [ -n "$scope_ln" ] && [ -n "$first_suite_ln" ] && [ "$scope_ln" -lt "$first_suite_ln" ]; then
  ok "G6: the scope decision is wired before the first tooling-tests self-test invocation"
else
  bad "G6: could not confirm the scope decision runs before the first self-test (scope_ln=$scope_ln first_suite_ln=$first_suite_ln)"
fi

# G7: `--only tooling-tests` bypasses scoping — the ONLY guard's early `return 0`
# for a NOT-selected component sits above the scope block, and the scope block
# itself is gated on `[ -z "$ONLY" ]`, so an explicit selection always runs.
# Anchored to `$fn_body` (the extracted run_tooling_tests body, same extraction
# G6 uses), NOT the whole 28k-line `$GATE` (a roborev review on this issue found
# the unanchored form would stay green even if THIS function's own ONLY guard
# were removed, as long as some unrelated guard elsewhere in the file happened
# to match the same literal text) — and requires the guard to appear BEFORE the
# scope-decision call within that body, not merely to exist somewhere in it.
only_guard_ln=$(printf '%s\n' "$fn_body" | grep -n 'if \[ -z "\$ONLY" \]; then' | head -1 | cut -d: -f1)
if [ -n "$only_guard_ln" ] && [ -n "$scope_ln" ] && [ "$only_guard_ln" -lt "$scope_ln" ]; then
  ok "G7: run_tooling_tests's OWN scope block is gated on ONLY being empty, before the scope decision runs"
else
  bad "G7: could not confirm the ONLY-empty guard precedes the scope decision within run_tooling_tests (only_guard_ln=$only_guard_ln scope_ln=$scope_ln)"
fi

# ==== AC4: the nightly workflow forces the unconditional lane ================
if [ -f "$WORKFLOW" ] && grep -q "CQLITE_TOOLING_TESTS_ALWAYS_RUN: '1'" "$WORKFLOW"; then
  ok "AC4: .github/workflows/gate.yml sets CQLITE_TOOLING_TESTS_ALWAYS_RUN=1 on its nightly full-gate step"
else
  bad "AC4: .github/workflows/gate.yml does not set CQLITE_TOOLING_TESTS_ALWAYS_RUN=1"
fi
if [ -f "$WORKFLOW" ] && grep -q "cron:" "$WORKFLOW" && grep -q 'run: bash scripts/agent-gate.sh$' "$WORKFLOW"; then
  ok "AC4: the nightly workflow is schedule-triggered and runs the FULL gate (never --only)"
else
  bad "AC4: could not confirm a schedule-triggered full-gate run in gate.yml"
fi

printf '\n%s\n' "tooling-tests-scope: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
