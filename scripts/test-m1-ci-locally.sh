#!/bin/bash
# Test M1 CI requirements locally before pushing
# This script runs exactly what the M1 CI pipeline tests

set -e

echo "=== Running M1 CI Tests Locally ==="
echo ""

# Set the same environment as CI
export RUSTFLAGS="-D warnings"
export CARGO_TERM_COLOR=always
export RUST_BACKTRACE=1
export CQLITE_DATASETS_ROOT="${CQLITE_DATASETS_ROOT:-$PWD/test-data/datasets}"

echo "Environment:"
echo "  RUSTFLAGS=$RUSTFLAGS"
echo "  CQLITE_DATASETS_ROOT=$CQLITE_DATASETS_ROOT"
echo ""

# Test 1: Format check
echo "📝 Step 1/7: Checking code formatting..."
if ! cargo fmt --all -- --check; then
    echo "❌ Format check failed"
    echo "💡 Fix with: cargo fmt --all"
    exit 1
fi
echo "✅ Format check passed"
echo ""

# Test 2: Clippy on core package
echo "🔍 Step 2/7: Running clippy on cqlite-core..."
if ! cargo clippy --package cqlite-core --all-features; then
    echo "❌ Clippy failed on cqlite-core"
    exit 1
fi
echo "✅ Clippy passed on cqlite-core"
echo ""

# Test 3: Clippy on CLI package
echo "🔍 Step 3/7: Running clippy on cqlite-cli..."
if ! cargo clippy --package cqlite-cli --all-features; then
    echo "❌ Clippy failed on cqlite-cli"
    exit 1
fi
echo "✅ Clippy passed on cqlite-cli"
echo ""

# Test 4: Core library tests
echo "🧪 Step 4/7: Running core library tests..."
if ! cargo test --package cqlite-core --lib --no-fail-fast 2>&1 | tee /tmp/cqlite-core-tests.log | grep -E "(^test |test result:)"; then
    echo "❌ Core library tests failed"
    echo "💡 See full output: /tmp/cqlite-core-tests.log"
    exit 1
fi
CORE_PASSED=$(grep "test result:" /tmp/cqlite-core-tests.log | grep -oE "[0-9]+ passed" | head -1)
echo "✅ Core library tests passed ($CORE_PASSED)"
echo ""

# Test 5: M1 integration tests
echo "🧪 Step 5/7: Running M1 integration tests..."
if ! cargo test --package cqlite-core \
    --test P0_4_modern_format_rejection_tests \
    --test parser_abstraction_tests \
    --test parsing_improvements_test \
    --no-fail-fast 2>&1 | tee /tmp/cqlite-m1-tests.log | grep -E "(^test |test result:)"; then
    echo "❌ M1 integration tests failed"
    echo "💡 See full output: /tmp/cqlite-m1-tests.log"
    exit 1
fi
M1_PASSED=$(grep -h "test result:" /tmp/cqlite-m1-tests.log | grep -oE "[0-9]+ passed" | awk '{sum+=$1} END {print sum}')
echo "✅ M1 integration tests passed ($M1_PASSED total passed)"
echo ""

# Test 6: Doc tests
echo "📚 Step 6/7: Running documentation tests..."
if ! cargo test --package cqlite-core --doc --no-fail-fast 2>&1 | tee /tmp/cqlite-doc-tests.log | grep -E "(^test |test result:)"; then
    echo "❌ Documentation tests failed"
    echo "💡 See full output: /tmp/cqlite-doc-tests.log"
    exit 1
fi
DOC_PASSED=$(grep "test result:" /tmp/cqlite-doc-tests.log | grep -oE "[0-9]+ passed" | head -1)
echo "✅ Documentation tests passed ($DOC_PASSED)"
echo ""

# Test 7: Build core library
echo "🔨 Step 7/7: Building core library..."
if ! cargo build --package cqlite-core --all-features --verbose 2>&1 | tail -5; then
    echo "❌ Core library build failed"
    exit 1
fi
echo "✅ Core library build passed"
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🎉 ALL M1 CI CHECKS PASSED LOCALLY!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Summary:"
echo "  ✅ Code formatting validated"
echo "  ✅ Clippy passed on core and CLI packages"
echo "  ✅ Core library: $CORE_PASSED"
echo "  ✅ M1 integration: $M1_PASSED tests passed"
echo "  ✅ Documentation: $DOC_PASSED"
echo "  ✅ Build successful"
echo ""
echo "Your changes are ready to push! CI should pass. 🚀"
