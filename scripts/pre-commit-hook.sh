#!/bin/bash
# Pre-commit hook for CQLite project
# Phase 3: Infrastructure Hardening

set -e

echo "🚀 Running pre-commit checks..."

# 1. Format code
echo "📝 Formatting code with rustfmt..."
cargo fmt --all

# 2. Fix auto-fixable issues
echo "🔧 Running cargo fix..."
cargo fix --allow-dirty --allow-staged

# 3. Run clippy with strict settings
echo "📋 Running clippy with strict checks..."
cargo clippy --all-targets --all-features -- -D warnings

# 4. Check for unused dependencies
echo "🔍 Checking for unused dependencies..."
if command -v cargo-machete &> /dev/null; then
    cargo machete
else
    echo "⚠️  cargo-machete not installed, skipping unused dependency check"
fi

# 5. Run basic tests
echo "🧪 Running quick test suite..."
cargo test --lib --bins

# 6. Check compilation
echo "🏗️  Checking compilation..."
cargo check --all-targets --all-features

echo "✅ Pre-commit checks passed!"