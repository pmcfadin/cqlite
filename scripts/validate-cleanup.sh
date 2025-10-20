#!/bin/bash

# Issue #168: Validate cleanup script
# This script validates core reading features, checks for warnings, and builds release binary

set -e

echo "==================================="
echo "Issue #168 Validation Script"
echo "==================================="
echo ""

# 1. Test core reading features
echo "Step 1: Testing core reading features (no default features, lib tests only)..."
echo "Command: cargo test --package cqlite-core --lib --no-default-features --features=all-compression"
echo ""
cargo test --package cqlite-core --lib --no-default-features --features=all-compression
if [ $? -ne 0 ]; then
    echo "ERROR: Core reading feature tests failed"
    exit 1
fi
echo ""
echo "Core reading tests passed!"
echo ""

# 2. Check clippy warnings
echo "Step 2: Checking clippy warnings (must be <= 50)..."
echo "Command: cargo clippy --workspace --all-targets --all-features 2>&1"
echo ""
CLIPPY_OUTPUT=$(cargo clippy --workspace --all-targets --all-features 2>&1)
WARNING_COUNT=$(echo "$CLIPPY_OUTPUT" | grep -c "warning:" || true)
echo "Found $WARNING_COUNT clippy warnings"
echo ""
if [ "$WARNING_COUNT" -gt 50 ]; then
    echo "ERROR: Too many clippy warnings ($WARNING_COUNT > 50)"
    echo "$CLIPPY_OUTPUT"
    exit 1
fi
echo "Clippy warnings within acceptable range!"
echo ""

# 3. Check for unused imports
echo "Step 3: Checking for unused imports in build output..."
echo "Command: cargo build --workspace 2>&1"
echo ""
BUILD_OUTPUT=$(cargo build --workspace 2>&1)
UNUSED_IMPORT_COUNT=$(echo "$BUILD_OUTPUT" | grep -c "unused import" || true)
echo "Found $UNUSED_IMPORT_COUNT unused imports"
echo ""
if [ "$UNUSED_IMPORT_COUNT" -gt 0 ]; then
    echo "ERROR: Found unused imports in build output"
    echo "$BUILD_OUTPUT" | grep "unused import"
    exit 1
fi
echo "No unused imports found!"
echo ""

# 4. Build release binary and report size
echo "Step 4: Building release binary and reporting size..."
echo "Command: cargo build --release --bin cqlite"
echo ""
cargo build --release --bin cqlite
if [ $? -ne 0 ]; then
    echo "ERROR: Release build failed"
    exit 1
fi
echo ""
BINARY_PATH="target/release/cqlite"
if [ -f "$BINARY_PATH" ]; then
    BINARY_SIZE=$(du -h "$BINARY_PATH" | cut -f1)
    echo "Release binary built successfully!"
    echo "Binary location: $BINARY_PATH"
    echo "Binary size: $BINARY_SIZE"
else
    echo "ERROR: Release binary not found at $BINARY_PATH"
    exit 1
fi
echo ""

# 5. Success message
echo "==================================="
echo "SUCCESS: All validation checks passed!"
echo "==================================="
echo ""
echo "Summary:"
echo "  - Core reading tests: PASSED"
echo "  - Clippy warnings: $WARNING_COUNT (acceptable)"
echo "  - Unused imports: $UNUSED_IMPORT_COUNT"
echo "  - Release binary: $BINARY_SIZE"
echo ""
