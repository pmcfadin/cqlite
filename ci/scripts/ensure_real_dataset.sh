#!/bin/bash
# Dataset provenance gate: fail on synthetic/mocked inputs (hard M1 gate)
# Issue #79 - Ensures parity jobs only use real datasets from Cassandra

set -euo pipefail

# Function to log messages
log() {
    echo "[$0] $1" >&2
}

# Function to fail with error message
fail() {
    echo "❌ $1" >&2
    exit 1
}

# Check if any arguments provided
if [ $# -eq 0 ]; then
    log "Usage: $0 <arguments-to-check>"
    log "Example: $0 \"test-data/datasets/sstables/test_basic\""
    exit 1
fi

INPUTS="$*"
log "🔍 Checking dataset provenance for: $INPUTS"

# Patterns to detect synthetic/mock datasets
FORBIDDEN_PATTERNS=(
    "test-data/generated"
    "synthetic"
    "mock"
    "fixture"
    "fake"
    "dummy"
    "stub"
    "test-with-mock"
    "mock-cqlite"
    "unit-tests-only"
)

# Check command line arguments
log "📋 Scanning command line arguments..."
for pattern in "${FORBIDDEN_PATTERNS[@]}"; do
    if echo "$INPUTS" | grep -E -i "$pattern" >/dev/null 2>&1; then
        fail "Synthetic or mock dataset reference detected in arguments: '$pattern'"
    fi
done

# Check environment variables
log "🌍 Scanning environment variables..."
while IFS='=' read -r name value; do
    # Skip empty values and common system vars
    [[ -z "$value" || "$name" =~ ^(PATH|HOME|USER|LANG|TERM)$ ]] && continue

    for pattern in "${FORBIDDEN_PATTERNS[@]}"; do
        if echo "$value" | grep -E -i "$pattern" >/dev/null 2>&1; then
            fail "Synthetic or mock dataset reference detected in environment variable $name: '$pattern'"
        fi
    done
done < <(env)

# Check for known mock scripts in the current execution context
log "📜 Checking for mock script execution..."
MOCK_SCRIPTS=(
    "scripts/testing/test-with-mock-cqlite.sh"
    "scripts/mock"
    "scripts/synthetic"
)

for script in "${MOCK_SCRIPTS[@]}"; do
    if [[ "$0" == *"$script"* ]] || echo "$INPUTS" | grep -E "$script" >/dev/null 2>&1; then
        fail "Mock script execution detected: $script"
    fi
done

# Check if we're in a test-data/generated directory context
if pwd | grep -E "test-data/generated" >/dev/null 2>&1; then
    fail "Current working directory is under test-data/generated/"
fi

# Verify we're using real datasets by checking for expected markers
log "✅ Verifying real dataset markers..."

# Look for real dataset indicators
REAL_DATASET_INDICATORS=(
    "test-data/datasets"
    "cassandra5-small-refs-only"
    "real-sstables"
    "production-data"
)

FOUND_REAL_INDICATOR=false
for indicator in "${REAL_DATASET_INDICATORS[@]}"; do
    if echo "$INPUTS" | grep -E "$indicator" >/dev/null 2>&1; then
        FOUND_REAL_INDICATOR=true
        log "✓ Found real dataset indicator: $indicator"
        break
    fi
done

# Additional check: verify test-data/datasets exists and has content
if [[ -d "test-data/datasets" ]]; then
    JSONL_COUNT=$(find test-data/datasets -name "*.jsonl" 2>/dev/null | wc -l)
    STATS_COUNT=$(find test-data/datasets -name "*-Statistics.db.txt" 2>/dev/null | wc -l)

    if [[ $JSONL_COUNT -gt 0 || $STATS_COUNT -gt 0 ]]; then
        FOUND_REAL_INDICATOR=true
        log "✓ Found real reference files: $JSONL_COUNT JSONL, $STATS_COUNT Statistics"
    fi
fi

# For parity tests, we require real dataset indicators
if echo "$INPUTS" | grep -E "(parity|sstabledump)" >/dev/null 2>&1; then
    if [[ "$FOUND_REAL_INDICATOR" == false ]]; then
        fail "❌ PARITY TEST FAILURE - No real dataset indicators found.

🔍 Issue #80 requires all M1-critical integration paths to use REAL datasets only.

📋 Required actions:
  1. Ensure test-data/datasets/ contains real Cassandra 5 SSTables
  2. Download datasets: gh release download datasets-v2 --pattern 'cassandra5-small-refs-only-v2.tar.gz'
  3. Verify SHA256: 1cfd054d7236132417fc93e91d17f660bbb96f6c5562f19ddc5c12e50bfbf2df
  4. Extract to project root: tar -xzf cassandra5-small-refs-only-v2.tar.gz

💡 For unit tests with mocks, enable the 'unit-tests-only' feature flag.
🚫 Mock/synthetic datasets are prohibited in CI integration paths per Issue #80."
    fi
fi

log "✅ Provenance check passed (real datasets only)"
echo "✅ Provenance check passed (real datasets only)"