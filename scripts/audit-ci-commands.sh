#!/bin/bash
# audit-ci-commands.sh
# Extracts all cargo commands from CI workflows for auditing
# Helps identify what CI actually runs vs what we test locally

set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  CQLite - CI Workflow Command Audit                         ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Function to extract commands from a workflow
audit_workflow() {
    local workflow_file="$1"
    local workflow_name=$(basename "$workflow_file" .yml)

    echo -e "${GREEN}═══ $workflow_name ═══${NC}"
    echo -e "${YELLOW}File: $workflow_file${NC}"
    echo ""

    # Extract cargo build commands
    local build_commands=$(grep -E "cargo (build|test|clippy|fmt)" "$workflow_file" | sed 's/^[ \t]*//' | grep -v "^#" || true)

    if [ -n "$build_commands" ]; then
        echo -e "${BLUE}Cargo Commands:${NC}"
        echo "$build_commands" | while IFS= read -r line; do
            echo "  $line"
        done
    else
        echo "  No cargo commands found"
    fi

    echo ""
}

# Check active workflows directory
WORKFLOWS_DIR=".github/workflows"

if [ ! -d "$WORKFLOWS_DIR" ]; then
    echo "Error: $WORKFLOWS_DIR not found"
    echo "Run this script from the repository root"
    exit 1
fi

echo -e "${YELLOW}Active Workflows:${NC}"
echo ""

# Audit each workflow file (excluding archived/disabled)
for workflow in "$WORKFLOWS_DIR"/*.yml; do
    # Skip archived workflows
    if [[ "$workflow" == *"archive"* ]] || [[ "$workflow" == *"disabled"* ]]; then
        continue
    fi

    audit_workflow "$workflow"
done

echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}Audit complete.${NC}"
echo ""
echo "Review the cargo commands above and ensure scripts/test-all-ci-locally.sh"
echo "covers all of them for complete local CI parity."
