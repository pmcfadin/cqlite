#!/usr/bin/env bash
# docs-site-check.sh — local mirror of the docs-site.yml CI checks.
#
# Runs:
#   1. npm ci (install pinned deps)
#   2. npm run build (Astro build + starlight-links-validator internal link check
#                    + emit-raw-markdown.mjs postbuild)
#   3. Agent-plumbing validation (llms.txt coverage + raw .md endpoint coverage)
#   4. [optional] docs-examples-smoke.sh (CLI recipe smoke tests)
#
# Emits a machine-checkable PASS/FAIL summary block at the end (modelled on
# scripts/agent-gate.sh). Exit code 0 = all checks passed; non-zero = failure.
#
# Usage:
#   bash scripts/docs-site-check.sh              # fast: site build + link check only
#   bash scripts/docs-site-check.sh --with-smoke # also run CLI recipe smoke tests
#   WITH_SMOKE=1 bash scripts/docs-site-check.sh # same, via env var
#
# The smoke step requires:
#   - CQLITE_DATASETS_ROOT set to the test dataset root (or test-data/datasets)
#   - The cqlite CLI binary built (cargo build --package cqlite-cli --features write-support)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WEBSITE_DIR="$REPO_ROOT/website"

# ── Argument parsing ──────────────────────────────────────────────────────────

WITH_SMOKE="${WITH_SMOKE:-0}"

for arg in "$@"; do
  case "$arg" in
    --with-smoke) WITH_SMOKE=1 ;;
    *) echo "Unknown argument: $arg"; exit 2 ;;
  esac
done

# ── Helpers ───────────────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
RESET='\033[0m'

pass() { echo -e "${GREEN}[PASS]${RESET} $*"; }
fail() { echo -e "${RED}[FAIL]${RESET} $*"; }
info() { echo -e "${YELLOW}[INFO]${RESET} $*"; }
banner() { echo -e "\n${BOLD}══ $* ══${RESET}"; }

FAILURES=()

run_check() {
    local name="$1"; shift
    info "Running: $name"
    if "$@"; then
        pass "$name"
    else
        fail "$name"
        FAILURES+=("$name")
    fi
}

# ── Pre-flight ────────────────────────────────────────────────────────────────

banner "docs-site-check pre-flight"

if ! command -v node &>/dev/null; then
    fail "node not found — install Node.js 20+"
    exit 1
fi

NODE_VERSION=$(node --version)
info "Node: $NODE_VERSION"

if ! command -v npm &>/dev/null; then
    fail "npm not found"
    exit 1
fi

info "npm: $(npm --version)"

if [[ ! -d "$WEBSITE_DIR" ]]; then
    fail "website/ directory not found at $WEBSITE_DIR"
    exit 1
fi

if [[ "$WITH_SMOKE" == "1" ]]; then
    info "Smoke tests: ENABLED (--with-smoke)"
else
    info "Smoke tests: DISABLED (pass --with-smoke to enable)"
fi

# ── Checks ────────────────────────────────────────────────────────────────────

banner "Step 1: Install dependencies"
(
    cd "$WEBSITE_DIR"
    if [[ -f package-lock.json ]]; then
        run_check "npm ci" npm ci
    else
        run_check "npm install" npm install
    fi
)

banner "Step 2: Build site (includes internal link validation + raw markdown emit)"
(
    cd "$WEBSITE_DIR"
    run_check "npm run build" npm run build
)

banner "Step 3: Agent-plumbing validation (llms.txt + raw .md endpoints)"
(
    cd "$WEBSITE_DIR"
    run_check "validate-agent-plumbing" node scripts/validate-agent-plumbing.mjs
)

if [[ "$WITH_SMOKE" == "1" ]]; then
    banner "Step 4: Example smoke tests (CLI recipes)"
    run_check "docs-examples-smoke" bash "$SCRIPT_DIR/docs-examples-smoke.sh"
fi

# ── Summary ───────────────────────────────────────────────────────────────────

banner "docs-site-check summary"

if [[ ${#FAILURES[@]} -eq 0 ]]; then
    echo ""
    echo "┌─────────────────────────────────────────┐"
    echo "│  docs-site-check: ALL CHECKS PASSED      │"
    echo "└─────────────────────────────────────────┘"
    echo ""
    echo "DOCS_SITE_CHECK=PASS"
    exit 0
else
    echo ""
    echo "┌─────────────────────────────────────────┐"
    echo "│  docs-site-check: FAILED                 │"
    echo "└─────────────────────────────────────────┘"
    echo ""
    echo "Failed checks:"
    for f in "${FAILURES[@]}"; do
        echo "  - $f"
    done
    echo ""
    echo "DOCS_SITE_CHECK=FAIL"
    exit 1
fi
