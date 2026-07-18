#!/usr/bin/env bash
# docs-site-failure-issue.sh — page a human when the docs-site build/plumbing
# validation fails on `main` (issue #2654).
#
# WHY: `validate-agent-plumbing.mjs` runs inside the docs-site build job. It reds
# the (advisory, NOT branch-protection-required) `Build docs site` check on
# docs-touching PRs — good enough to make a bad PR visible. But once code is on
# `main`, a build/plumbing FAILURE makes the `deploy` job (which `needs:
# [build, smoke]`) SKIP silently: the site simply stops deploying and nobody is
# paged. This script closes that gap by filing/updating ONE deduplicated
# tracking issue so a main-branch docs-site failure is VISIBLE.
#
# It does NOT add a required merge gate — per the owner decision on #2654 the
# plumbing check stays ADVISORY (only #2644's oracle became `required` this
# epic). This is the "red on main pages someone" half of the acceptance.
#
# DESIGN (mirrors parity-failure-issue.yml's fail-open, dedup-by-marker posture):
#   - Dedup by a stable body marker `<!-- DOCS-SITE-FAIL -->` + a label; UPDATE
#     the open issue with a new comment, never open a duplicate.
#   - NON-GATING / fail-open: absent issue-write token or any gh error →
#     `::warning::` + exit 0. This script never changes the docs-site job's
#     result; the failing build step already reds the run.
#
# SECURITY: no untrusted event field is interpolated into a shell; the caller
# passes trusted values (run URL, branch) via quoted env vars.
#
# Usage (from docs-site.yml, on a push-to-main failure):
#   RUN_URL=... FAILED_JOBS="build" bash scripts/ci/docs-site-failure-issue.sh
#
# Env:
#   RUN_URL      — html_url of the failing run (optional; a placeholder is used if absent)
#   FAILED_JOBS  — space/comma list of failed job names (optional, informational)
#   GH_TOKEN     — required for the gh writes; absent → fail-open notice + exit 0
#   DOCS_FAIL_LABEL — label to tag/dedup on (default: docs-site-failure)

set -euo pipefail

MARKER='<!-- DOCS-SITE-FAIL -->'
LABEL="${DOCS_FAIL_LABEL:-docs-site-failure}"
RUN_URL="${RUN_URL:-<run url unavailable>}"
FAILED_JOBS="${FAILED_JOBS:-docs-site build}"

notice() { echo "::notice::docs-site-failure-issue: $*"; }
warn()   { echo "::warning::docs-site-failure-issue: $*"; }

# Fail-open: without a token we cannot write. Surface loudly, never red the run.
if [ -z "${GH_TOKEN:-}" ] && [ -z "${GITHUB_TOKEN:-}" ]; then
  warn "no issue-write token available — cannot file the docs-site failure issue (non-gating). A human should check the failing run: ${RUN_URL}"
  exit 0
fi

TITLE='docs-site: build / agent-plumbing validation FAILED on main (deploy skipped)'

read -r -d '' BODY <<EOF || true
${MARKER}

The **docs-site** build failed on \`main\`, so the GitHub Pages **deploy job was
skipped** (it \`needs: [build, smoke]\`). The published site is now STALE until
this is fixed. This issue exists so the otherwise-silent skip pages a human
(issue #2654).

- Failing run: ${RUN_URL}
- Failed job(s): ${FAILED_JOBS}

**Most common cause — the website-page cross-link rule (issue #2480).** A new
page reachable only via the sidebar fails \`website/scripts/validate-agent-plumbing.mjs\`:
the \`starlight-llms-txt\` plugin strips sidebar nav, so a page only lands in
\`llms-full.txt\` when some page's **body prose** links it. Cross-link the new
page from an existing page's body (e.g. the \`user-docs/index.md\` Topics list).
See \`website/README.md\` → "Adding pages".

Reproduce locally: \`bash scripts/docs-site-check.sh\`.

This check is ADVISORY (not a required branch-protection context); this issue is
the alerting mechanism, filed/updated automatically by
\`scripts/ci/docs-site-failure-issue.sh\`.
EOF

# Ensure the dedup label exists (idempotent; fail-open).
gh label create "${LABEL}" \
  --description "docs-site build/deploy failure on main (auto-filed, issue #2654)" \
  --color B60205 >/dev/null 2>&1 || true

# Find an existing open issue by marker (dedup). Search is best-effort.
EXISTING=""
if EXISTING_JSON="$(gh issue list --state open --label "${LABEL}" --search "${MARKER}" --json number --jq '.[0].number' 2>/dev/null)"; then
  EXISTING="${EXISTING_JSON}"
fi

TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

if [ -n "${EXISTING}" ] && [ "${EXISTING}" != "null" ]; then
  notice "updating existing docs-site failure issue #${EXISTING}"
  gh issue comment "${EXISTING}" --body "Recurred at ${TS}. Failing run: ${RUN_URL} (jobs: ${FAILED_JOBS})." \
    || warn "failed to comment on issue #${EXISTING} (non-gating)"
else
  notice "filing new docs-site failure issue"
  gh issue create --title "${TITLE}" --label "${LABEL}" --body "${BODY}" \
    || warn "failed to create docs-site failure issue (non-gating)"
fi

exit 0
