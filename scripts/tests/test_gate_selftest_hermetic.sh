#!/usr/bin/env bash
# Structural lint for issue #2874: gate self-tests under scripts/tests/ MUST be
# HERMETIC per run — every fixture/sentinel/temporary path a per-run mktemp namespace
# with a terminal `XXXXXX` template (macOS-safe), never a FIXED shared name. Two
# concurrent self-test lanes in one checkout must not be able to collide on a shared
# path (the residual #2874 kill surface: the parity-report self-test's fixed
# `.tmp-*-mutated` fixture, whose EXIT trap `rm`'d a peer lane's live fixture).
#
# This is a static regression guard so the class cannot silently return. It scans
# scripts/tests/*.sh for:
#   A. macOS-UNSAFE mktemp templates — a `X{3,}` run that is NOT trailing (macOS
#      mktemp requires the X's to be the LAST chars of the template).
#   B. FIXED `.tmp-*` fixture names — a `.tmp-<name>.<ext>` literal (the offending
#      convention), as opposed to a per-run `mktemp ....XXXXXX` name (no extension).
#
# Comment lines are ignored (so doc references to a retired fixed name don't trip it).
# Deliberate exceptions carry a trailing `# hermetic-lint-allow` marker on the line.
#
# Run standalone:   bash scripts/tests/test_gate_selftest_hermetic.sh
# Or via the gate:  scripts/agent-gate.sh runs it inside the `tooling-tests` component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
SELF=$(basename "$0")

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

# Two file-level grep passes (fast — no per-line subprocess fan-out) collect candidate
# `file:lineno:content` hits; a post-filter drops the linter itself, comment lines, and
# lines carrying the `# hermetic-lint-allow` marker.
#   Rule A: a mktemp template whose X-run is not TRAILING — a run of 3+ X's immediately
#           followed by another path/token char (digit, '.', '/', '-', '_', or a letter
#           OTHER THAN uppercase 'X', so the run's own trailing X's do not self-match).
#           macOS mktemp requires the X's to be the last chars of the template.
#   Rule B: a FIXED `.tmp-<name>.<ext>` fixture literal (the retired shared-name
#           convention). A per-run mktemp name ends in `XXXXXX` with no extension.
shopt -s nullglob
files=("$SCRIPT_DIR"/*.sh)

# report_hits <label> <grep-hits...on-stdin>: print bad lines for real violations.
report_hits() {
  local label="$1" line file base content trimmed
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    file=${line%%:*}
    base=$(basename "$file")
    [ "$base" = "$SELF" ] && continue                 # never flag the linter itself
    content=${line#*:}; content=${content#*:}         # strip "file:lineno:"
    case "$content" in *'# hermetic-lint-allow'*) continue ;; esac
    trimmed=${content#"${content%%[![:space:]]*}"}    # left-trim
    case "$trimmed" in '#'*) continue ;; esac         # full-line comment
    bad "$label in ${base}:$(printf '%s' "$line" | cut -d: -f2)"
    printf '        %s\n' "$trimmed"
    violations=$((violations + 1))
  done
}

violations=0
report_hits "macOS-unsafe mktemp template (X's not trailing)" < <(
  grep -HnE 'mktemp' "${files[@]}" 2>/dev/null | grep -E 'X{3,}[0-9A-WY-Za-z._/-]'
)
report_hits "fixed '.tmp-*' fixture name (use a per-run mktemp XXXXXX)" < <(
  grep -HnE '\.tmp-[A-Za-z0-9_-]+\.(yml|yaml|json|txt|md|db|cql)' "${files[@]}" 2>/dev/null
)

if [ "$violations" -eq 0 ]; then
  ok "no macOS-unsafe mktemp templates or fixed '.tmp-*' fixture names in scripts/tests/*.sh"
fi

echo "----"
echo "passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ]
