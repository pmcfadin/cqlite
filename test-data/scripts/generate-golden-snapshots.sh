#!/usr/bin/env bash
#
# generate-golden-snapshots.sh
#
# Generates golden snapshot files for CLI integration tests (Issue #140)
# This script runs the CQLite CLI with various queries and saves outputs
# as golden reference files for regression testing.
#
# Usage:
#   ./generate-golden-snapshots.sh [--release]
#
# Options:
#   --release    Use release build instead of debug build

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEST_DATA_ROOT="$PROJECT_ROOT/test-data"
GOLDEN_DIR="$TEST_DATA_ROOT/golden"
SCHEMAS_DIR="$TEST_DATA_ROOT/schemas"
DATASETS_DIR="$TEST_DATA_ROOT/datasets"

# Default to debug build
BUILD_TYPE="debug"
CLI_BIN="$PROJECT_ROOT/target/debug/cqlite"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --release)
      BUILD_TYPE="release"
      CLI_BIN="$PROJECT_ROOT/target/release/cqlite"
      shift
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--release]"
      exit 1
      ;;
  esac
done

echo -e "${BLUE}=== CQLite Golden Snapshot Generator ===${NC}"
echo ""
echo "Project root:    $PROJECT_ROOT"
echo "Build type:      $BUILD_TYPE"
echo "CLI binary:      $CLI_BIN"
echo "Golden dir:      $GOLDEN_DIR"
echo "Schemas dir:     $SCHEMAS_DIR"
echo "Datasets dir:    $DATASETS_DIR"
echo ""

# Step 1: Build the CLI
echo -e "${YELLOW}Step 1: Building CLI ($BUILD_TYPE)...${NC}"
if [[ "$BUILD_TYPE" == "release" ]]; then
  cargo build --release --package cqlite-cli --bin cqlite
else
  cargo build --package cqlite-cli --bin cqlite
fi

if [[ ! -x "$CLI_BIN" ]]; then
  echo -e "${RED}Error: CLI binary not found at $CLI_BIN${NC}"
  exit 1
fi
echo -e "${GREEN}✓ CLI built successfully${NC}"
echo ""

# Step 2: Create golden directory
echo -e "${YELLOW}Step 2: Creating golden directory...${NC}"
mkdir -p "$GOLDEN_DIR"
echo -e "${GREEN}✓ Golden directory ready: $GOLDEN_DIR${NC}"
echo ""

# Step 3: Verify test data exists
echo -e "${YELLOW}Step 3: Verifying test data...${NC}"
if [[ ! -d "$DATASETS_DIR/sstables/test_basic" ]]; then
  echo -e "${RED}Error: test_basic dataset not found at $DATASETS_DIR/sstables/test_basic${NC}"
  echo "Please run the test data generation scripts first."
  exit 1
fi

if [[ ! -d "$DATASETS_DIR/sstables/test_collections" ]]; then
  echo -e "${RED}Error: test_collections dataset not found at $DATASETS_DIR/sstables/test_collections${NC}"
  echo "Please run the test data generation scripts first."
  exit 1
fi

# P0-4: Use .cql schema files instead of .json
if [[ ! -f "$SCHEMAS_DIR/basic-types.cql" ]]; then
  echo -e "${RED}Error: basic-types.cql schema not found at $SCHEMAS_DIR/basic-types.cql${NC}"
  exit 1
fi

if [[ ! -f "$SCHEMAS_DIR/collections.cql" ]]; then
  echo -e "${RED}Error: collections.cql schema not found at $SCHEMAS_DIR/collections.cql${NC}"
  exit 1
fi

echo -e "${GREEN}✓ Test data verified${NC}"
echo ""

# Helper function to run CLI and save output
run_and_save() {
  local name="$1"
  local schema="$2"
  local query="$3"
  local format="$4"
  local output_file="$GOLDEN_DIR/${name}.${format}"

  echo -e "  ${BLUE}→${NC} Generating ${name}.${format}..."

  # P0-6: Pass correct data directory path with /sstables suffix
  # Run CLI command
  if "$CLI_BIN" \
    --schema "$schema" \
    --data-dir "$DATASETS_DIR/sstables" \
    -e "$query" \
    --format "$format" \
    > "$output_file" 2>&1; then

    local line_count=$(wc -l < "$output_file" | tr -d ' ')
    echo -e "    ${GREEN}✓${NC} Saved (${line_count} lines)"
    return 0
  else
    echo -e "    ${RED}✗${NC} Failed (check output at $output_file)"
    return 1
  fi
}

# Step 4: Generate golden snapshots
echo -e "${YELLOW}Step 4: Generating golden snapshots...${NC}"

# Track success/failure counts
TOTAL=0
SUCCESS=0
FAILED=0

# P0-4: Use .cql schema files
# Basic test - JSON format
((TOTAL++))
if run_and_save \
  "basic_select_json" \
  "$SCHEMAS_DIR/basic-types.cql" \
  "SELECT * FROM test_basic.simple_table LIMIT 5" \
  "json"; then
  ((SUCCESS++))
else
  ((FAILED++))
fi

# Basic test - CSV format
((TOTAL++))
if run_and_save \
  "basic_select_csv" \
  "$SCHEMAS_DIR/basic-types.cql" \
  "SELECT * FROM test_basic.simple_table LIMIT 5" \
  "csv"; then
  ((SUCCESS++))
else
  ((FAILED++))
fi

# Basic test - Table format (for reference)
((TOTAL++))
if run_and_save \
  "basic_select_table" \
  "$SCHEMAS_DIR/basic-types.cql" \
  "SELECT * FROM test_basic.simple_table LIMIT 5" \
  "table"; then
  ((SUCCESS++))
else
  ((FAILED++))
fi

# Collections test - JSON format
((TOTAL++))
if run_and_save \
  "collections_select" \
  "$SCHEMAS_DIR/collections.cql" \
  "SELECT * FROM test_collections.collection_table LIMIT 3" \
  "json"; then
  ((SUCCESS++))
else
  ((FAILED++))
fi

# Collections test - CSV format (for reference)
((TOTAL++))
if run_and_save \
  "collections_select_csv" \
  "$SCHEMAS_DIR/collections.cql" \
  "SELECT * FROM test_collections.collection_table LIMIT 3" \
  "csv"; then
  ((SUCCESS++))
else
  ((FAILED++))
fi

# Collections test - Table format (for reference)
((TOTAL++))
if run_and_save \
  "collections_select_table" \
  "$SCHEMAS_DIR/collections.cql" \
  "SELECT * FROM test_collections.collection_table LIMIT 3" \
  "table"; then
  ((SUCCESS++))
else
  ((FAILED++))
fi

# Subset of columns - JSON format
((TOTAL++))
if run_and_save \
  "basic_select_columns_json" \
  "$SCHEMAS_DIR/basic-types.cql" \
  "SELECT id, name, age FROM test_basic.simple_table LIMIT 3" \
  "json"; then
  ((SUCCESS++))
else
  ((FAILED++))
fi

# Subset of columns - CSV format
((TOTAL++))
if run_and_save \
  "basic_select_columns_csv" \
  "$SCHEMAS_DIR/basic-types.cql" \
  "SELECT id, name, age FROM test_basic.simple_table LIMIT 3" \
  "csv"; then
  ((SUCCESS++))
else
  ((FAILED++))
fi

echo ""
echo -e "${GREEN}✓ Snapshot generation complete${NC}"
echo ""

# Step 5: Summary
echo -e "${YELLOW}Step 5: Summary${NC}"
echo "  Total snapshots: $TOTAL"
echo -e "  ${GREEN}Successful: $SUCCESS${NC}"
if [[ $FAILED -gt 0 ]]; then
  echo -e "  ${RED}Failed: $FAILED${NC}"
fi
echo ""
echo "Golden snapshots saved to: $GOLDEN_DIR"
echo ""

# List generated files
echo -e "${YELLOW}Generated files:${NC}"
ls -lh "$GOLDEN_DIR" | tail -n +2 | awk '{printf "  %s %s %s\n", $9, $5, ""}'
echo ""

# Step 6: Final instructions
echo -e "${YELLOW}Next steps:${NC}"
echo "  1. Review the generated golden snapshot files in $GOLDEN_DIR"
echo "  2. Verify the outputs are correct and match expected behavior"
echo "  3. Commit the golden snapshot files to the repository"
echo "  4. Run integration tests to ensure they pass with the new snapshots"
echo ""

if [[ $FAILED -gt 0 ]]; then
  echo -e "${RED}Warning: Some snapshots failed to generate. Please review the errors above.${NC}"
  exit 1
else
  echo -e "${GREEN}All snapshots generated successfully!${NC}"
fi
