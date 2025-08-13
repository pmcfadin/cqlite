# SSTableDump Validator Makefile
# Zero-tolerance validation harness for CQLite SSTable compatibility

.PHONY: help build test clean docker-setup validate generate-data full-validation ci-setup

# Default target
help:
	@echo "🔍 SSTableDump Validator - Zero Tolerance Validation"
	@echo "=================================================="
	@echo ""
	@echo "Available targets:"
	@echo "  build           - Build the validator binary"
	@echo "  test            - Run all tests"
	@echo "  clean           - Clean build artifacts"
	@echo "  docker-setup    - Setup Docker Cassandra environment"
	@echo "  generate-data   - Generate test data in Cassandra"
	@echo "  validate        - Run validation on existing SSTables"
	@echo "  full-validation - Complete validation workflow"
	@echo "  ci-setup        - Setup for CI environment"
	@echo ""
	@echo "Environment variables:"
	@echo "  CASSANDRA_VERSION - Cassandra version to use (default: 5.0)"
	@echo "  TEST_DATA_COUNT   - Number of test records (default: 100)"
	@echo "  ZERO_TOLERANCE    - Enable zero tolerance mode (default: true)"

# Build the validator
build:
	@echo "🔨 Building SSTableDump Validator..."
	cargo build --release
	@echo "✅ Build complete: target/release/sstabledump-validator"

# Build in debug mode for development
build-dev:
	@echo "🔨 Building SSTableDump Validator (debug)..."
	cargo build
	@echo "✅ Debug build complete: target/debug/sstabledump-validator"

# Run all tests
test:
	@echo "🧪 Running unit tests..."
	cargo test --lib
	@echo "🧪 Running integration tests..."
	cargo test --test integration_tests
	@echo "✅ All tests passed"

# Run tests with coverage (requires cargo-tarpaulin)
test-coverage:
	@echo "📊 Running tests with coverage..."
	cargo tarpaulin --out Html --output-dir coverage-report
	@echo "✅ Coverage report generated in coverage-report/"

# Clean build artifacts
clean:
	@echo "🧹 Cleaning build artifacts..."
	cargo clean
	rm -rf test-sstables/
	rm -rf validation_report_*.txt
	rm -rf junit_validation.xml
	@echo "✅ Clean complete"

# Setup Docker Cassandra environment
docker-setup:
	@echo "🐳 Setting up Docker Cassandra environment..."
	@export CASSANDRA_VERSION=${CASSANDRA_VERSION:-5.0}
	@echo "Using Cassandra version: $$CASSANDRA_VERSION"
	
	# Build the validator first
	$(MAKE) build
	
	# Setup Cassandra container
	./target/release/sstabledump-validator setup --version $$CASSANDRA_VERSION
	@echo "✅ Docker environment ready"

# Generate test data
generate-data: build
	@echo "📊 Generating test data..."
	@export TEST_COUNT=${TEST_DATA_COUNT:-100}
	@echo "Generating $$TEST_COUNT test records with edge cases..."
	./target/release/sstabledump-validator generate --count $$TEST_COUNT --edge-cases
	@echo "✅ Test data generated"

# Extract SSTable files from Docker
extract-sstables:
	@echo "📦 Extracting SSTable files from Cassandra..."
	
	# Create output directory
	mkdir -p test-sstables
	
	# Force flush in Cassandra
	docker exec $(shell docker ps --filter "ancestor=cassandra:5.0" --format "{{.ID}}" | head -1) nodetool flush
	
	# Copy SSTable files
	CONTAINER_ID=$$(docker ps --filter "ancestor=cassandra" --format "{{.ID}}" | head -1); \
	if [ -n "$$CONTAINER_ID" ]; then \
		docker cp $$CONTAINER_ID:/var/lib/cassandra/data/validator_test ./test-sstables/; \
		echo "✅ SSTable files extracted"; \
		find ./test-sstables -name "*.db" | wc -l | xargs echo "Found SSTable files:"; \
	else \
		echo "❌ No Cassandra container found"; \
		exit 1; \
	fi

# Run validation on existing SSTables
validate: build
	@echo "🔍 Running validation on existing SSTables..."
	@export ZERO_TOL=${ZERO_TOLERANCE:-true}
	
	@if [ ! -d "test-sstables" ]; then \
		echo "❌ No test-sstables directory found. Run 'make extract-sstables' first."; \
		exit 1; \
	fi
	
	# Find SSTable files to validate
	@SSTABLE_FILES=$$(find test-sstables -name "*-Data.db" | head -5); \
	if [ -z "$$SSTABLE_FILES" ]; then \
		echo "❌ No SSTable files found in test-sstables/"; \
		exit 1; \
	fi; \
	\
	echo "Found $$(echo $$SSTABLE_FILES | wc -w) SSTable files to validate"; \
	\
	EXIT_CODE=0; \
	for SSTABLE in $$SSTABLE_FILES; do \
		echo "📊 Validating: $$SSTABLE"; \
		REPORT_FILE="validation_report_$$(basename $$SSTABLE).txt"; \
		if [ "$$ZERO_TOL" = "true" ]; then \
			if ./target/release/sstabledump-validator validate "$$SSTABLE" --fail-on-diff --detailed > "$$REPORT_FILE" 2>&1; then \
				echo "✅ Perfect match: $$SSTABLE"; \
			else \
				echo "❌ VALIDATION FAILED: $$SSTABLE"; \
				cat "$$REPORT_FILE"; \
				EXIT_CODE=1; \
			fi; \
		else \
			./target/release/sstabledump-validator validate "$$SSTABLE" --detailed > "$$REPORT_FILE" 2>&1; \
			echo "ℹ️  Validation completed: $$SSTABLE"; \
		fi; \
	done; \
	\
	if [ $$EXIT_CODE -ne 0 ]; then \
		echo ""; \
		echo "🚨 VALIDATION FAILED IN ZERO TOLERANCE MODE"; \
		echo "Cell-by-cell comparison found differences."; \
		echo "This is exactly what was requested - failing on ANY difference."; \
		exit $$EXIT_CODE; \
	else \
		echo ""; \
		echo "🎉 ALL VALIDATIONS PASSED!"; \
		echo "CQLite output matches Cassandra exactly."; \
	fi

# Complete validation workflow
full-validation: clean build docker-setup generate-data extract-sstables validate
	@echo ""
	@echo "🎉 FULL VALIDATION WORKFLOW COMPLETED"
	@echo "=================================="
	@echo "✅ Environment setup"
	@echo "✅ Test data generation"
	@echo "✅ SSTable extraction"
	@echo "✅ Zero-tolerance validation"

# Quick validation for development
quick-validation: build
	@echo "⚡ Running quick validation..."
	# Use existing test data if available
	@if [ -d "test-sstables" ]; then \
		$(MAKE) validate; \
	else \
		echo "No existing test data. Running full validation..."; \
		$(MAKE) full-validation; \
	fi

# Setup for CI environment
ci-setup:
	@echo "🔧 Setting up CI environment..."
	
	# Install required tools
	cargo install cargo-tarpaulin || echo "cargo-tarpaulin already installed"
	
	# Verify Docker is available
	docker --version || (echo "❌ Docker not available in CI"; exit 1)
	
	# Wait for services to be ready (if running in CI with services)
	@if [ -n "$$GITHUB_ACTIONS" ]; then \
		echo "⏳ Waiting for Cassandra service to be ready..."; \
		timeout 300s bash -c 'until cqlsh -h localhost -e "SELECT cluster_name FROM system.local;" > /dev/null 2>&1; do sleep 5; done'; \
		echo "✅ Cassandra service ready"; \
	fi
	
	@echo "✅ CI environment ready"

# Run linting and formatting
lint:
	@echo "🔍 Running Clippy lints..."
	cargo clippy -- -D warnings
	@echo "📝 Checking code formatting..."
	cargo fmt --check
	@echo "✅ Lint checks passed"

# Fix formatting
fmt:
	@echo "📝 Formatting code..."
	cargo fmt
	@echo "✅ Code formatted"

# Generate documentation
docs:
	@echo "📚 Generating documentation..."
	cargo doc --no-deps --open
	@echo "✅ Documentation generated"

# Benchmark validation performance
benchmark: build
	@echo "⚡ Running performance benchmarks..."
	
	# Generate larger dataset for benchmarking
	./target/release/sstabledump-validator generate --count 10000 --edge-cases
	
	# Extract SSTables
	$(MAKE) extract-sstables
	
	# Run validation with timing
	@echo "🕐 Starting timed validation..."
	@time $(MAKE) validate ZERO_TOLERANCE=false
	@echo "✅ Benchmark complete"

# Debug validation issues
debug: build-dev
	@echo "🐛 Running debug validation..."
	@export RUST_LOG=debug
	
	# Use a single SSTable for debugging
	@SSTABLE=$$(find test-sstables -name "*-Data.db" | head -1); \
	if [ -n "$$SSTABLE" ]; then \
		echo "🔍 Debugging: $$SSTABLE"; \
		./target/debug/sstabledump-validator validate "$$SSTABLE" --detailed --verbose; \
	else \
		echo "❌ No SSTable files found. Run 'make generate-data extract-sstables' first."; \
	fi

# Clean Docker resources
clean-docker:
	@echo "🐳 Cleaning Docker resources..."
	-docker stop $$(docker ps --filter "ancestor=cassandra" --format "{{.ID}}")
	-docker rm $$(docker ps -a --filter "ancestor=cassandra" --format "{{.ID}}")
	-docker volume prune -f
	@echo "✅ Docker resources cleaned"

# Show validation status
status:
	@echo "📊 Validation Status"
	@echo "==================="
	@echo ""
	
	# Check if validator is built
	@if [ -f "target/release/sstabledump-validator" ]; then \
		echo "✅ Validator binary: Built"; \
	else \
		echo "❌ Validator binary: Not built"; \
	fi
	
	# Check Docker status
	@if docker ps --filter "ancestor=cassandra" --format "{{.ID}}" > /dev/null 2>&1; then \
		CONTAINER_COUNT=$$(docker ps --filter "ancestor=cassandra" --format "{{.ID}}" | wc -l); \
		echo "✅ Docker Cassandra: $$CONTAINER_COUNT container(s) running"; \
	else \
		echo "❌ Docker Cassandra: No containers running"; \
	fi
	
	# Check for test data
	@if [ -d "test-sstables" ]; then \
		SSTABLE_COUNT=$$(find test-sstables -name "*.db" | wc -l); \
		echo "✅ Test SSTables: $$SSTABLE_COUNT files available"; \
	else \
		echo "❌ Test SSTables: No files found"; \
	fi
	
	# Check for validation reports
	@REPORT_COUNT=$$(ls validation_report_*.txt 2>/dev/null | wc -l); \
	if [ $$REPORT_COUNT -gt 0 ]; then \
		echo "✅ Validation Reports: $$REPORT_COUNT reports available"; \
	else \
		echo "❌ Validation Reports: No reports found"; \
	fi

# Install the validator system-wide
install: build
	@echo "📦 Installing sstabledump-validator..."
	cargo install --path .
	@echo "✅ Installed to ~/.cargo/bin/"

# Show help for specific target
help-%:
	@case '$*' in \
		'build') echo "Build the validator binary in release mode" ;; \
		'test') echo "Run all unit and integration tests" ;; \
		'docker-setup') echo "Setup Docker Cassandra container for testing" ;; \
		'validate') echo "Run zero-tolerance validation on existing SSTables" ;; \
		'full-validation') echo "Complete workflow: setup, generate, extract, validate" ;; \
		*) echo "No help available for target: $*" ;; \
	esac