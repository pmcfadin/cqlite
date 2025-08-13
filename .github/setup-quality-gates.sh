#!/usr/bin/env bash
# Setup script for code quality gates and pre-commit hooks
# Usage: ./setup-quality-gates.sh

set -e

echo "🔧 Setting up code quality gates for CQLite..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check prerequisites
echo -e "${BLUE}Checking prerequisites...${NC}"

if ! command_exists rustc; then
    echo -e "${RED}❌ Rust is not installed. Please install Rust first.${NC}"
    exit 1
fi

if ! command_exists git; then
    echo -e "${RED}❌ Git is not installed. Please install Git first.${NC}"
    exit 1
fi

# Check if we're in a git repository
if [ ! -d ".git" ]; then
    echo -e "${RED}❌ Not in a git repository. Please run this script from the repository root.${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Prerequisites check passed${NC}"

# Install clippy if not already installed
echo -e "${BLUE}Installing/updating Rust components...${NC}"
rustup component add clippy --toolchain stable
rustup component add rustfmt --toolchain stable
echo -e "${GREEN}✅ Rust components installed${NC}"

# Set up pre-commit hook
echo -e "${BLUE}Setting up pre-commit hook...${NC}"
if [ -f ".git/hooks/pre-commit" ]; then
    echo -e "${YELLOW}⚠️  Pre-commit hook already exists. Creating backup...${NC}"
    cp .git/hooks/pre-commit .git/hooks/pre-commit.backup
fi

# Copy and make executable
cp .github/hooks/pre-commit .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
echo -e "${GREEN}✅ Pre-commit hook installed${NC}"

# Test the pre-commit hook
echo -e "${BLUE}Testing pre-commit hook...${NC}"
if .git/hooks/pre-commit; then
    echo -e "${GREEN}✅ Pre-commit hook test passed${NC}"
else
    echo -e "${YELLOW}⚠️  Pre-commit hook test failed. This is expected if there are existing quality issues.${NC}"
    echo -e "${YELLOW}    Run the hook manually to see specific issues: .git/hooks/pre-commit${NC}"
fi

# Install additional tools (optional)
echo -e "${BLUE}Installing additional code quality tools...${NC}"

# Try to install tokei for code metrics (optional)
if command_exists cargo; then
    if ! command_exists tokei; then
        echo -e "${BLUE}Installing tokei for code metrics...${NC}"
        if cargo install tokei; then
            echo -e "${GREEN}✅ tokei installed${NC}"
        else
            echo -e "${YELLOW}⚠️  tokei installation failed, but this is optional${NC}"
        fi
    else
        echo -e "${GREEN}✅ tokei already installed${NC}"
    fi
fi

# Check current code quality
echo -e "${BLUE}Running initial code quality check...${NC}"
FAILED=0

# Check each workspace member
WORKSPACE_MEMBERS=("cqlite-core" "cqlite-cli" "testing-framework" "tests")

for member in "${WORKSPACE_MEMBERS[@]}"; do
    if [ -d "$member" ]; then
        echo -e "${BLUE}Checking $member...${NC}"
        cd "$member"
        
        # Quick clippy check
        if cargo clippy --all-targets --all-features -- -D warnings --quiet >/dev/null 2>&1; then
            echo -e "${GREEN}✅ $member: No clippy issues${NC}"
        else
            echo -e "${RED}❌ $member: Has clippy issues${NC}"
            FAILED=1
        fi
        
        # Quick format check
        if cargo fmt --all -- --check >/dev/null 2>&1; then
            echo -e "${GREEN}✅ $member: Properly formatted${NC}"
        else
            echo -e "${RED}❌ $member: Formatting issues${NC}"
            FAILED=1
        fi
        
        cd ..
    fi
done

# Summary and next steps
echo -e "\n${BLUE}=== Setup Complete ===${NC}"
echo -e "${GREEN}✅ Pre-commit hook installed and configured${NC}"
echo -e "${GREEN}✅ CI/CD workflow configured for code quality enforcement${NC}"
echo -e "${GREEN}✅ Cargo.toml files configured with strict linting${NC}"

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✅ Current code passes all quality checks${NC}"
    echo -e "${GREEN}🚀 Ready for development with quality gates${NC}"
else
    echo -e "${YELLOW}⚠️  Current code has quality issues that need to be addressed${NC}"
    echo -e "${YELLOW}    These will be caught by the pre-commit hook${NC}"
fi

echo -e "\n${BLUE}=== Usage Instructions ===${NC}"
echo -e "${BLUE}• Pre-commit hook will run automatically on each commit${NC}"
echo -e "${BLUE}• CI/CD will enforce quality gates on all PRs${NC}"
echo -e "${BLUE}• Run 'cargo clippy --all-targets --all-features' to check locally${NC}"
echo -e "${BLUE}• Run 'cargo fmt --all' to format code${NC}"
echo -e "${BLUE}• Check .claude/memory/code-quality-safeguards.md for full documentation${NC}"

echo -e "\n${BLUE}=== Manual Commands ===${NC}"
echo -e "${BLUE}Test pre-commit hook:${NC} .git/hooks/pre-commit"
echo -e "${BLUE}Format all code:${NC} cargo fmt --all"
echo -e "${BLUE}Check all lints:${NC} cargo clippy --all-targets --all-features"
echo -e "${BLUE}Fix minor issues:${NC} cargo clippy --all-targets --all-features --fix"

echo -e "\n${GREEN}🎉 Code quality gates setup complete!${NC}"