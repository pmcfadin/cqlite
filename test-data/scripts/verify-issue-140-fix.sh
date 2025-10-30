#!/usr/bin/env bash
# Minimal verification script for Issue #140 fix
# Quickly verifies that SELECT * queries return non-empty JSON objects
#
# Usage:
#   ./verify-issue-140-fix.sh

set -euo pipefail

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

echo -e "${BOLD}${BLUE}Verifying Issue #140 Fix: Non-Empty JSON Output${NC}"
echo ""

cd "${WORKSPACE_ROOT}"

# Build if needed
if [[ ! -x "target/debug/cqlite" ]]; then
    echo -e "${BLUE}Building CLI...${NC}"
    cargo build --package cqlite-cli --bin cqlite --quiet
fi

# Run a SELECT * query
echo -e "${BLUE}Running SELECT * query...${NC}"
OUTPUT=$(./target/debug/cqlite \
    --schema test-data/schemas/basic-types.cql \
    --dataset test_basic \
    --execute "SELECT * FROM test_basic.simple_table LIMIT 3" \
    --format json 2>/dev/null)

echo ""
echo -e "${BOLD}Query Output:${NC}"
echo "$OUTPUT"
echo ""

# Check if output contains actual data (not empty objects)
if echo "$OUTPUT" | grep -q '"id"'; then
    echo -e "${GREEN}${BOLD}✓ SUCCESS: JSON contains column data (found 'id' field)${NC}"

    if echo "$OUTPUT" | grep -q '"name"'; then
        echo -e "${GREEN}✓ SUCCESS: JSON contains 'name' field${NC}"
    fi

    # Check for empty objects
    if echo "$OUTPUT" | grep -q '{}'; then
        echo -e "${RED}${BOLD}✗ FAILURE: Output contains empty objects '{}'${NC}"
        exit 1
    else
        echo -e "${GREEN}✓ SUCCESS: No empty objects '{}' found${NC}"
    fi

    echo ""
    echo -e "${GREEN}${BOLD}========================================${NC}"
    echo -e "${GREEN}${BOLD}Issue #140 Fix Verified Successfully!${NC}"
    echo -e "${GREEN}${BOLD}========================================${NC}"
    echo ""
    echo "The fix is working correctly. SELECT * queries return non-empty JSON objects."
    echo ""
    echo "Next steps:"
    echo "  1. Run full validation: ./test-data/scripts/quick-pre-push-check.sh"
    echo "  2. Or comprehensive: ./test-data/scripts/validate-issue-140-fix.sh"
    echo ""
    exit 0
else
    echo -e "${RED}${BOLD}✗ FAILURE: JSON does not contain expected column data${NC}"
    echo -e "${RED}Expected to find 'id' field in output${NC}"
    echo ""
    echo "This indicates the Issue #140 fix is not working properly."
    echo "Review the changes in: cqlite-core/src/query/select_executor.rs"
    echo ""
    exit 1
fi
