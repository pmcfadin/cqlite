# CQLite Development Justfile
# https://github.com/casey/just

# Default recipe to display help
default:
    @just --list

# Development setup
setup:
    @echo "Setting up CQLite development environment..."
    cargo install cargo-watch cargo-tarpaulin cargo-audit wasm-pack
    rustup target add wasm32-unknown-unknown
    @echo "✅ Development environment ready!"

# Build all targets
build:
    @echo "Building all targets..."
    cargo build --all-targets --all-features

# Build with optimizations
build-release:
    @echo "Building release targets..."
    cargo build --release --all-targets --all-features

# Run all tests
test:
    @echo "Running all tests..."
    cargo test --all-features --workspace

# Run tests with coverage
test-coverage:
    @echo "Running tests with coverage..."
    cargo tarpaulin --all-features --workspace --timeout 120

# Run specific crate tests
test-core:
    cargo test --package cqlite-core --all-features

test-cli:
    cargo test --package cqlite-cli --all-features

test-ffi:
    cargo test --package cqlite-ffi --all-features

test-wasm:
    cargo test --package cqlite-wasm --all-features

# Code quality checks
check:
    @echo "Running code quality checks..."
    cargo fmt --all -- --check
    cargo clippy --all-targets --all-features -- -D warnings
    cargo audit

# Fix code formatting and issues
fix:
    @echo "Fixing code formatting and issues..."
    cargo fmt --all
    cargo clippy --all-targets --all-features --fix --allow-dirty --allow-staged

# Run benchmarks
bench:
    @echo "Running benchmarks..."
    cargo bench --all-features

# Build and test WASM
wasm:
    @echo "Building WASM package..."
    wasm-pack build cqlite-wasm --target web --out-dir pkg
    wasm-pack test cqlite-wasm --node

# Build FFI library
ffi:
    @echo "Building FFI library..."
    cargo build --package cqlite-ffi --release
    @echo "Generating C headers..."
    cd cqlite-ffi && cargo run --bin cbindgen_gen

# Development watch mode
watch:
    @echo "Starting watch mode..."
    cargo watch -x "build --all-features" -x "test --all-features"

# Watch specific crate
watch-core:
    cargo watch -x "build --package cqlite-core" -x "test --package cqlite-core"

watch-cli:
    cargo watch -x "build --package cqlite-cli" -x "test --package cqlite-cli"

# Clean build artifacts
clean:
    @echo "Cleaning build artifacts..."
    cargo clean
    rm -rf cqlite-wasm/pkg
    rm -rf target/

# Install CLI locally
install:
    @echo "Installing CQLite CLI..."
    cargo install --path cqlite-cli --force

# Run CLI in development mode
dev-cli *args:
    cargo run --package cqlite-cli -- {{args}}

# Start development database
dev-db:
    @echo "Starting development database..."
    cargo run --package cqlite-cli -- --database dev.db repl

# Generate documentation
docs:
    @echo "Generating documentation..."
    cargo doc --all-features --workspace --no-deps --open

# Run security audit
audit:
    @echo "Running security audit..."
    cargo audit

# Check dependencies for updates
deps-check:
    @echo "Checking for dependency updates..."
    cargo outdated

# Update dependencies
deps-update:
    @echo "Updating dependencies..."
    cargo update

# Docker development environment
docker-build:
    @echo "Building development Docker image..."
    docker build -t cqlite-dev .

docker-run:
    @echo "Running development container..."
    docker run -it --rm -v $(pwd):/workspace cqlite-dev

# Performance profiling
profile:
    @echo "Running performance profiling..."
    cargo build --release --package cqlite-core
    perf record --call-graph=dwarf target/release/examples/benchmark
    perf report

# Memory profiling with valgrind
valgrind:
    @echo "Running memory profiling..."
    cargo build --package cqlite-core
    valgrind --tool=memcheck --leak-check=full target/debug/examples/benchmark

# Database examples
example-create:
    @echo "Creating example database..."
    cargo run --package cqlite-cli -- --database examples/example.db -c "CREATE KEYSPACE example"

example-load:
    @echo "Loading example data..."
    cargo run --package cqlite-cli -- --database examples/example.db import examples/sample_data.csv --format csv

# Continuous Integration locally
ci-local:
    @echo "Running full CI pipeline locally..."
    just check
    just test
    just build-release
    just wasm
    just ffi
    just docs

# Release preparation
release-prep version:
    @echo "Preparing release {{version}}..."
    # Update version in Cargo.toml files
    sed -i.bak 's/version = ".*"/version = "{{version}}"/' Cargo.toml
    sed -i.bak 's/version = ".*"/version = "{{version}}"/' */Cargo.toml
    # Run full test suite
    just ci-local
    # Create git tag
    git add .
    git commit -m "Release {{version}}"
    git tag -a "v{{version}}" -m "Release {{version}}"
    @echo "Release {{version}} prepared. Push with: git push origin main --tags"