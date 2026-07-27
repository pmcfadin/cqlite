#!/usr/bin/env bash
# Self-test for the docs-only PR-gate classifier (issue #2645, epic #2636).
#
# Proves the three acceptance properties of scripts/ci/classify-docs-only.sh:
#   (a) a docs-/board-only changed set short-circuits (exit 0 => "docs-only"),
#       so the required gate can return green in seconds;
#   (b) any Rust / Cargo / test-data manifest / .github workflow / scripts /
#       config / unknown-extension file forces the FULL path (exit 1 => "full");
#   (c) FAIL-CLOSED: an empty/ambiguous changed set forces the full path, and a
#       *.md file living under a sensitive dir (.github/, scripts/, test-data/)
#       does NOT short-circuit.
#
# Also asserts the workflow contract that makes the required status ALWAYS
# report: pr-gate.yml uses NO paths/paths-ignore trigger filter, always runs the
# classifier step, and gates every heavy step (including the #2644
# query-semantics oracle) on the classifier output.
#
# Hermetic: pure shell, no cargo/Docker/datasets/network. A failure FAILs the
# tooling-tests gate component.
#
# Run standalone:   bash scripts/tests/test_classify_docs_only.sh
# Or via the gate:  scripts/agent-gate.sh runs it inside `tooling-tests`.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
CLASSIFY="$REPO_ROOT/scripts/ci/classify-docs-only.sh"
WORKFLOW="$REPO_ROOT/.github/workflows/pr-gate.yml"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

[ -f "$CLASSIFY" ] || { echo "FATAL: classifier missing: $CLASSIFY" >&2; exit 1; }

# assert_docs_only <label> <newline-file-list>
# Expects exit 0 and STDOUT verdict "docs-only".
assert_docs_only() {
  local label="$1" input="$2" out rc
  out=$(printf '%s' "$input" | bash "$CLASSIFY" 2>/dev/null)
  rc=$?
  if [ "$rc" -eq 0 ] && [ "$out" = "docs-only" ]; then
    ok "$label (short-circuit: exit 0, verdict=docs-only)"
  else
    bad "$label (expected exit 0/docs-only, got exit $rc/'$out')"
  fi
}

# assert_full <label> <newline-file-list>
# Expects exit 1 and STDOUT verdict "full".
assert_full() {
  local label="$1" input="$2" out rc
  out=$(printf '%s' "$input" | bash "$CLASSIFY" 2>/dev/null)
  rc=$?
  if [ "$rc" -eq 1 ] && [ "$out" = "full" ]; then
    ok "$label (full path: exit 1, verdict=full)"
  else
    bad "$label (expected exit 1/full, got exit $rc/'$out')"
  fi
}

echo "== (a) docs-/board-only sets short-circuit to green =="
assert_docs_only "single markdown"        $'README.md\n'
assert_docs_only "docs tree"              $'docs/development/dev-cookbook.md\n'
assert_docs_only "image asset"            $'docs/img/diagram.png\n'
assert_docs_only "multiple docs files"    $'docs/a.md\nREADME.md\ndocs/img/x.svg\nCHANGELOG.markdown\n'
assert_docs_only "license"                $'LICENSE\n'
assert_docs_only "trailing blank lines"   $'docs/a.md\n\n\n'

echo "== (b) code-relevant sets force the FULL path =="
assert_full "rust source"                 $'cqlite-core/src/lib.rs\n'
assert_full "rust among docs"             $'docs/a.md\ncqlite-core/src/reader.rs\n'
assert_full "cargo manifest"              $'cqlite-core/Cargo.toml\n'
assert_full "cargo lockfile"              $'Cargo.lock\n'
assert_full ".github workflow"            $'.github/workflows/pr-gate.yml\n'
assert_full ".github action"              $'.github/actions/setup-rust-ci/action.yml\n'
assert_full "scripts dir"                 $'scripts/agent-gate.sh\n'
assert_full "test-data manifest"          $'test-data/cassandra-parity-manifest.yml\n'
assert_full "test-data fixture"           $'test-data/datasets/sstables/x/nb-1-big-Data.db\n'

echo "== (c) fail-closed on ambiguous / smuggled changes =="
assert_full "empty set"                   ''
assert_full "blank lines only"            $'\n\n'
assert_full "unknown extension"           $'tools/helper.py\n'
assert_full "no-extension top-level"      $'Makefile\n'
assert_full "md under .github (smuggle)"  $'.github/README.md\n'
assert_full "md under scripts (smuggle)"  $'scripts/notes.md\n'
assert_full "md under test-data (smuggle)" $'test-data/README.md\n'
assert_full "docs + smuggled workflow md" $'docs/ok.md\n.github/CONTRIBUTING.md\n'

echo "== workflow contract: required status ALWAYS reports =="
if [ -f "$WORKFLOW" ]; then
  # No path filter on the trigger (would stop the required check from firing).
  if grep -Eq '^\s*(paths|paths-ignore)\s*:' "$WORKFLOW"; then
    bad "pr-gate.yml must NOT use paths/paths-ignore (required check must always fire)"
  else
    ok "pr-gate.yml trigger has no paths/paths-ignore filter"
  fi

  # The classifier step itself must have no `if:` gate (always runs).
  if command -v ruby >/dev/null 2>&1; then
    ruby - "$WORKFLOW" <<'RUBY'
      require "yaml"
      wf = YAML.load_file(ARGV[0])
      # The docs-only classifier lives in `pr-gate-core`; `required` (issue #2910)
      # is the sibling-tier aggregator and is asserted separately below.
      steps = wf.dig("jobs", "pr-gate-core", "steps") || []
      classify = steps.find { |s| s.is_a?(Hash) && s["id"] == "classify" }
      abort("no classify step") unless classify
      # Always runs (no if:), and heavy steps are gated on its output.
      abort("classify step must not be gated") if classify.key?("if")
      gated = steps.select { |s| s.is_a?(Hash) && s["if"].to_s.include?("steps.classify.outputs.docs_only") }
      # Heavy steps = everything the docs-only path must SKIP (all gated steps
      # except the docs-only informational summary, which runs on == 'true').
      heavy = gated.reject { |s| s["if"].to_s.include?("== 'true'") }
      names = heavy.map { |s| s["name"].to_s }
      abort("oracle step not gated on classifier") unless names.any? { |n| n.include?("oracle") }
      abort("build step not gated on classifier") unless names.any? { |n| n.include?("build") }
      abort("no heavy steps gated on classifier") if heavy.empty?
      # Every heavy step must gate with != 'true' (fail-closed: default = full run).
      bad_gate = heavy.find { |s| !s["if"].to_s.include?("!= 'true'") }
      abort("heavy step gate not fail-closed (!= 'true'): #{bad_gate&.fetch("name")}") if bad_gate
      # And a docs-only branch step must exist so the required status still
      # reports (green summary) when the heavy path is skipped.
      abort("no docs-only branch step (required status would not report)") unless gated.any? { |s| s["if"].to_s.include?("== 'true'") }
      # Issue #2910: `required` is the branch-protection context and must ALWAYS
      # report — never skipped when pr-gate-core fails — and must fail closed on
      # a non-success core result.
      required = wf.dig("jobs", "required")
      abort("no `required` job") unless required.is_a?(Hash)
      abort("`required` job must be named 'required'") unless required["name"].to_s == "required"
      abort("`required` must run with if: always() so the context always reports") unless required["if"].to_s.include?("always()")
      abort("`required` must depend on pr-gate-core") unless Array(required["needs"]).map(&:to_s).include?("pr-gate-core")
      body = Array(required["steps"]).map { |s| s.is_a?(Hash) ? [s["run"].to_s, (s["env"] || {}).values.join("\n")].join("\n") : "" }.join("\n")
      abort("`required` must fail when pr-gate-core did not succeed") unless body.include?("needs.pr-gate-core.result")
      abort("`required` must run the sibling-tier aggregator") unless body.include?("aggregate-required-tiers.sh")
RUBY
    if [ "$?" -eq 0 ]; then
      ok "classify step always runs; heavy steps (incl. #2644 oracle) gated fail-closed on its output"
      ok "required always reports, needs pr-gate-core, and aggregates sibling tiers (#2910)"
    else
      bad "pr-gate.yml classify/gating contract not satisfied"
    fi
  else
    printf 'info - ruby absent; skipped structural workflow assertion\n'
  fi
else
  bad "pr-gate.yml not found at $WORKFLOW"
fi

echo
echo "==== classify-docs-only self-test: PASS=$PASS FAIL=$FAIL ===="
[ "$FAIL" -eq 0 ]
