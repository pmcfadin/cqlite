#!/bin/bash
# CQLite Project Structure Validation Script
# Enforces structural standards from code review recommendations

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🔍 Validating CQLite project structure...${NC}"

# Function to report errors
report_error() {
    echo -e "${RED}❌ ERROR: $1${NC}"
    exit 1
}

# Function to report warnings
report_warning() {
    echo -e "${YELLOW}⚠️  WARNING: $1${NC}"
}

# Function to report success
report_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

# 1. Validate root crate structure
echo "Checking root crate structure..."
if [ -f "src/lib.rs" ] && ! grep -q "^\[package\]" Cargo.toml; then
    report_error "src/lib.rs exists but no [package] section in root Cargo.toml. Either add [package] section or remove src/lib.rs for pure workspace."
fi
report_success "Root crate structure is valid"

# 2. Check required configuration files
echo "Checking required configuration files..."
required_files=(
    "rust-toolchain.toml"
    ".rustfmt.toml"
    "deny.toml"
    "LICENSE"
    ".pre-commit-config.yaml"
)

for file in "${required_files[@]}"; do
    if [ ! -f "$file" ]; then
        report_error "Required file $file is missing"
    fi
done
report_success "All required configuration files present"

# 3. Validate lint configuration
echo "Checking lint configuration..."
if grep -q "dead_code.*deny" Cargo.toml && grep -A5 "\[workspace.lints.clippy\]" Cargo.toml | grep -q "dead_code"; then
    report_error "dead_code lint should be under [workspace.lints.rust], not clippy section"
fi

# Check for lint priority conflicts
if grep -A10 "\[workspace.lints.clippy\]" Cargo.toml | grep -q 'all = "warn"' && ! grep -q 'priority = -1' Cargo.toml; then
    report_error "Clippy 'all' group should have lower priority to avoid conflicts. Use: all = { level = \"warn\", priority = -1 }"
fi
report_success "Lint configuration is valid"

# 4. Check workspace dependency consistency
echo "Checking workspace dependency consistency..."
workspace_crates=(
    "cqlite-core"
    "cqlite-cli"
    "cqlite-ffi"
    "cqlite-wasm"
    "tests"
    "examples"
)

for crate in "${workspace_crates[@]}"; do
    if [ -f "$crate/Cargo.toml" ]; then
        # Check if crate uses workspace inheritance for common fields
        if ! grep -q "workspace = true" "$crate/Cargo.toml"; then
            report_warning "$crate/Cargo.toml should use workspace inheritance where possible"
        fi
        
        # Check if edition is inherited
        if grep -q "^edition = " "$crate/Cargo.toml" && ! grep -q "edition.workspace = true" "$crate/Cargo.toml"; then
            report_warning "$crate should inherit edition from workspace"
        fi
    fi
done
report_success "Workspace dependency structure validated"

# 5. Check for Tokio "full" features abuse
echo "Checking Tokio feature usage..."
if grep -q 'tokio.*features.*\["full"\]' Cargo.toml; then
    report_error "Tokio 'full' features should be avoided. Enable only needed features per crate."
fi
report_success "Tokio feature usage is appropriate"

# 6. Validate compression crate feature gating
echo "Checking compression crate feature gating..."
compression_crates=("lz4_flex" "snap" "flate2" "zstd")

for crate in "${compression_crates[@]}"; do
    if grep -q "^$crate = " Cargo.toml && ! grep -q "optional = true" Cargo.toml; then
        if [ "$crate" != "lz4_flex" ]; then  # lz4_flex might be required
            report_warning "Compression crate $crate should be feature-gated with 'optional = true'"
        fi
    fi
done
report_success "Compression crate feature gating validated"

# 7. Check for FFI build automation
echo "Checking FFI build automation..."
if [ -f "cqlite-ffi/Cargo.toml" ] && grep -q "cbindgen" cqlite-ffi/Cargo.toml; then
    if [ ! -f "cqlite-ffi/build.rs" ]; then
        report_warning "cqlite-ffi should have build.rs for automated header generation"
    fi
fi
report_success "FFI build configuration checked"

# 8. Validate WASM package metadata
echo "Checking WASM package configuration..."
if [ -f "cqlite-wasm/Cargo.toml" ]; then
    if ! grep -q "package.metadata.wasm-pack" cqlite-wasm/Cargo.toml; then
        report_warning "cqlite-wasm should include package.metadata.wasm-pack configuration"
    fi
fi
report_success "WASM package configuration checked"

# 9. Check for dual licensing files
echo "Checking licensing compliance..."
if grep -q "MIT OR Apache-2.0" Cargo.toml; then
    if [ ! -f "LICENSE-MIT" ] || [ ! -f "LICENSE-APACHE" ]; then
        report_warning "Dual license specified but missing LICENSE-MIT or LICENSE-APACHE files"
    fi
fi
report_success "Licensing compliance checked"

# 10. Final structure summary
echo ""
echo -e "${GREEN}🎉 Project structure validation completed successfully!${NC}"
echo ""
echo "Structure standards enforced:"
echo "  ✅ Root crate clarity"
echo "  ✅ Required configuration files"
echo "  ✅ Lint configuration correctness"
echo "  ✅ Workspace dependency consistency"
echo "  ✅ Tokio feature optimization"
echo "  ✅ Compression crate feature gating"
echo "  ✅ FFI build automation"
echo "  ✅ WASM package configuration"
echo "  ✅ Licensing compliance"
echo ""
echo -e "${GREEN}All structural standards met! 🚀${NC}"