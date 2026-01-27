# CQLite Development Makefile
# Includes comprehensive testing and coverage enforcement

.PHONY: all build test coverage test-coverage coverage-html coverage-report clean fmt clippy docs check-all install-tools help

# Default target
all: check-all

# Build the project
build:
	@echo "🔨 Building CQLite..."
	cargo build --all-features

# Build in release mode
build-release:
	@echo "🚀 Building CQLite (release)..."
	cargo build --release --all-features

# Run all tests
test:
	@echo "🧪 Running tests..."
	cargo test --all-features -- --test-threads=1

# Run tests with output
test-verbose:
	@echo "🧪 Running tests (verbose)..."
	cargo test --all-features -- --test-threads=1 --nocapture

# Run coverage analysis with ≥90% enforcement
coverage: install-coverage-tools
	@echo "📊 Running coverage analysis with ≥90% threshold enforcement..."
	@chmod +x scripts/coverage.sh
	@./scripts/coverage.sh

# Generate test coverage without enforcement (for development)
test-coverage: install-coverage-tools
	@echo "📊 Generating test coverage report..."
	cargo llvm-cov --workspace --lcov --output-path target/coverage/lcov.info --ignore-filename-regex='(test|spec|bench)' -- --test-threads=1

# Generate HTML coverage report
coverage-html: install-coverage-tools
	@echo "🌐 Generating HTML coverage report..."
	cargo llvm-cov --workspace --html --output-dir target/coverage/html --ignore-filename-regex='(test|spec|bench)' -- --test-threads=1
	@echo "📄 Coverage report available at: target/coverage/html/index.html"

# Generate comprehensive coverage report
coverage-report: coverage coverage-html
	@echo "📋 Comprehensive coverage analysis completed"
	@echo "📊 LCOV report: target/coverage/lcov.info"
	@echo "🌐 HTML report: target/coverage/html/index.html"

# Run property-based tests
test-property:
	@echo "🎲 Running property-based tests..."
	cargo test --release property_ -- --nocapture --test-threads=1

# Run stress tests
test-stress:
	@echo "💪 Running stress tests..."
	cargo test --release stress_ -- --nocapture --test-threads=1

# Run determinism validation tests
test-determinism:
	@echo "🔄 Running determinism validation tests..."
	@echo "Running test suite 5 times to verify deterministic behavior..."
	@for i in 1 2 3 4 5; do \
		echo "Determinism test run $$i/5..."; \
		cargo test --workspace -- --test-threads=1 > /dev/null 2>&1 || exit 1; \
	done
	@echo "✅ All test runs completed successfully - tests are deterministic"

# Run edge case tests specifically
test-edge-cases:
	@echo "🔍 Running edge case tests..."
	cargo test --package cqlite-core edge_case -- --nocapture
	cargo test --package cqlite-core comprehensive -- --nocapture

# Install required tools
install-tools: install-coverage-tools
	@echo "📦 Installing development tools..."
	rustup component add clippy rustfmt
	cargo install cargo-audit
	@echo "✅ Development tools installed"

# Install coverage tools
install-coverage-tools:
	@echo "📦 Installing coverage tools..."
	@if ! command -v cargo-llvm-cov >/dev/null 2>&1; then \
		echo "Installing cargo-llvm-cov..."; \
		cargo install cargo-llvm-cov; \
	fi
	@echo "✅ Coverage tools ready"

# Format code
fmt:
	@echo "🎨 Formatting code..."
	cargo fmt --all

# Check formatting
fmt-check:
	@echo "🔍 Checking code formatting..."
	cargo fmt --all -- --check

# Run clippy lints
clippy:
	@echo "📎 Running clippy lints..."
	cargo clippy --all-features --all-targets -- -D warnings

# Check for security vulnerabilities
audit:
	@echo "🔐 Running security audit..."
	cargo audit

# Generate documentation
docs:
	@echo "📚 Generating documentation..."
	cargo doc --all-features --no-deps --open

# Run comprehensive checks
check-all: fmt-check clippy test audit
	@echo "✅ All checks passed!"

# Run CI-style validation (what runs on PRs)
ci-check: install-tools coverage test-determinism
	@echo "🚦 CI-style validation completed"

# Quick development check (faster than full CI)
dev-check: fmt clippy test-coverage
	@echo "⚡ Development check completed"

# Run specific test by name
test-specific:
	@echo "🎯 Running specific test: $(TEST)"
	@if [ -z "$(TEST)" ]; then \
		echo "❌ Please specify TEST=<test_name>"; \
		exit 1; \
	fi
	cargo test $(TEST) -- --nocapture

# Benchmark core reading performance
benchmark:
	@echo "⚡ Running performance benchmarks..."
	cargo test benchmark_critical_reading_paths -- --nocapture
	cargo test --release --package cqlite-core bench -- --nocapture

# Clean build artifacts
clean:
	@echo "🧹 Cleaning build artifacts..."
	cargo clean
	rm -rf target/coverage

# Clean everything including coverage data
clean-all: clean
	@echo "🗑️  Cleaning all artifacts..."
	rm -rf target
	rm -f *.profraw

# Verify project structure and dependencies
verify:
	@echo "🔍 Verifying project structure..."
	cargo check --all-features
	cargo tree
	@echo "✅ Project structure verified"

# Show help
help:
	@echo "CQLite Development Commands:"
	@echo ""
	@echo "Building:"
	@echo "  build              Build the project"
	@echo "  build-release      Build in release mode"
	@echo ""
	@echo "Testing:"
	@echo "  test               Run all tests"
	@echo "  test-verbose       Run tests with output"
	@echo "  test-property      Run property-based tests"
	@echo "  test-stress        Run stress tests"
	@echo "  test-determinism   Verify test determinism"
	@echo "  test-edge-cases    Run edge case tests"
	@echo "  test-specific      Run specific test (use TEST=name)"
	@echo ""
	@echo "Coverage (≥90% enforcement):"
	@echo "  coverage           Run coverage with threshold enforcement"
	@echo "  test-coverage      Generate coverage without enforcement"
	@echo "  coverage-html      Generate HTML coverage report"
	@echo "  coverage-report    Generate comprehensive coverage"
	@echo ""
	@echo "Code Quality:"
	@echo "  fmt                Format code"
	@echo "  fmt-check          Check formatting"
	@echo "  clippy             Run clippy lints"
	@echo "  audit              Security audit"
	@echo ""
	@echo "Validation:"
	@echo "  check-all          Run all checks"
	@echo "  ci-check           CI-style validation"
	@echo "  dev-check          Quick development check"
	@echo "  verify             Verify project structure"
	@echo ""
	@echo "Performance:"
	@echo "  benchmark          Run performance benchmarks"
	@echo ""
	@echo "Utilities:"
	@echo "  install-tools      Install development tools"
	@echo "  docs               Generate documentation"
	@echo "  clean              Clean build artifacts"
	@echo "  clean-all          Clean everything"
	@echo "  help               Show this help"
	@echo ""
	@echo "Examples:"
	@echo "  make coverage                    # Run coverage with ≥90% enforcement"
	@echo "  make test-specific TEST=vint     # Run vint-related tests"
	@echo "  make ci-check                    # Full CI validation"

# Default help if no target specified
.DEFAULT_GOAL := help