#!/bin/bash
#
# Coverage testing script for CQLite core reading modules
# Enforces >=90% coverage threshold for critical reading paths
#

set -euo pipefail

# Configuration
COVERAGE_THRESHOLD=90
CORE_MODULES="cqlite-core/src/storage/sstable cqlite-core/src/parser cqlite-core/src/schema"
REPORT_DIR="target/coverage"
LCOV_FILE="${REPORT_DIR}/lcov.info"

echo "🔍 Starting CQLite Core Reading Module Coverage Analysis"
echo "📊 Target Coverage Threshold: ${COVERAGE_THRESHOLD}%"
echo "🎯 Core Modules: ${CORE_MODULES}"
echo

# Clean previous coverage data
echo "🧹 Cleaning previous coverage data..."
cargo clean
rm -rf ${REPORT_DIR}
mkdir -p ${REPORT_DIR}

# Install coverage tools if needed
if ! command -v cargo-llvm-cov &> /dev/null; then
    echo "📦 Installing cargo-llvm-cov..."
    cargo install cargo-llvm-cov
fi

# Run tests with coverage instrumentation
echo "🧪 Running tests with coverage instrumentation..."
export CARGO_INCREMENTAL=0
export RUSTFLAGS="-Cinstrument-coverage"
export LLVM_PROFILE_FILE="cargo-test-%p-%m.profraw"

# Run all tests for core modules
echo "🚀 Executing test suite..."
cargo llvm-cov --workspace \
    --lcov --output-path ${LCOV_FILE} \
    --ignore-filename-regex='(test|spec|bench)' \
    -- --test-threads=1

# Generate HTML report
echo "📝 Generating HTML coverage report..."
cargo llvm-cov --workspace \
    --html --output-dir ${REPORT_DIR}/html \
    --ignore-filename-regex='(test|spec|bench)' \
    -- --test-threads=1

# Parse coverage percentage for core modules
echo "📊 Analyzing coverage for core reading modules..."
python3 - << EOF
import re
import sys

def parse_lcov_coverage(lcov_file, modules):
    """Parse LCOV file and extract coverage for specific modules."""
    try:
        with open(lcov_file, 'r') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"❌ Coverage file not found: {lcov_file}")
        return None
    
    # Extract per-file coverage
    files_data = {}
    current_file = None
    
    for line in content.split('\n'):
        if line.startswith('SF:'):
            current_file = line[3:]  # Remove 'SF:'
        elif line.startswith('LH:'):
            lines_hit = int(line[3:])
        elif line.startswith('LF:'):
            lines_found = int(line[3:])
            if current_file and lines_found > 0:
                coverage = (lines_hit / lines_found) * 100
                files_data[current_file] = {
                    'lines_hit': lines_hit,
                    'lines_found': lines_found,
                    'coverage': coverage
                }
    
    # Filter for core modules
    module_coverage = {}
    for module in modules:
        module_files = [f for f in files_data.keys() if module in f]
        if module_files:
            total_hit = sum(files_data[f]['lines_hit'] for f in module_files)
            total_found = sum(files_data[f]['lines_found'] for f in module_files)
            if total_found > 0:
                module_coverage[module] = (total_hit / total_found) * 100
    
    return module_coverage, files_data

# Parse coverage
modules = "${CORE_MODULES}".split()
coverage_data, all_files = parse_lcov_coverage("${LCOV_FILE}", modules)

if not coverage_data:
    print("❌ Failed to parse coverage data")
    sys.exit(1)

print("\n📊 Core Reading Module Coverage Results:")
print("=" * 60)

overall_hit = 0
overall_found = 0
all_passing = True

for module, coverage in coverage_data.items():
    status = "✅" if coverage >= ${COVERAGE_THRESHOLD} else "❌"
    print(f"{status} {module:<40} {coverage:6.2f}%")
    
    if coverage < ${COVERAGE_THRESHOLD}:
        all_passing = False
    
    # Calculate overall coverage
    module_files = [f for f in all_files.keys() if module in f]
    for f in module_files:
        overall_hit += all_files[f]['lines_hit']
        overall_found += all_files[f]['lines_found']

if overall_found > 0:
    overall_coverage = (overall_hit / overall_found) * 100
    overall_status = "✅" if overall_coverage >= ${COVERAGE_THRESHOLD} else "❌"
    print("=" * 60)
    print(f"{overall_status} Overall Core Reading Coverage:        {overall_coverage:6.2f}%")
    print(f"📈 Threshold Required:                   {${COVERAGE_THRESHOLD}:6.2f}%")
    print(f"📝 Lines Covered: {overall_hit}/{overall_found}")
else:
    print("❌ No coverage data found for core modules")
    all_passing = False

print("\n🔍 Detailed File Coverage (files < ${COVERAGE_THRESHOLD}%):")
print("-" * 60)

low_coverage_files = []
for file_path, data in all_files.items():
    if any(module in file_path for module in modules) and data['coverage'] < ${COVERAGE_THRESHOLD}:
        low_coverage_files.append((file_path, data['coverage'], data['lines_hit'], data['lines_found']))

if low_coverage_files:
    for file_path, coverage, hit, found in sorted(low_coverage_files, key=lambda x: x[1]):
        print(f"❌ {file_path:<50} {coverage:6.2f}% ({hit}/{found})")
else:
    print("✅ All core reading files meet coverage threshold!")

if not all_passing:
    print(f"\n❌ Coverage threshold not met! Core reading modules must have ≥{${COVERAGE_THRESHOLD}}% coverage.")
    print("📝 Please add tests for the files listed above.")
    sys.exit(1)
else:
    print(f"\n✅ All core reading modules meet the {${COVERAGE_THRESHOLD}}% coverage threshold!")
    print("🎉 Coverage gate passed!")

EOF

coverage_exit_code=$?

echo
echo "📄 Coverage reports generated:"
echo "   📊 LCOV: ${LCOV_FILE}"
echo "   🌐 HTML: ${REPORT_DIR}/html/index.html"
echo

if [ $coverage_exit_code -eq 0 ]; then
    echo "✅ Coverage analysis completed successfully!"
    echo "🎯 All core reading modules meet the ${COVERAGE_THRESHOLD}% threshold"
else
    echo "❌ Coverage analysis failed!"
    echo "📈 Some core reading modules are below the ${COVERAGE_THRESHOLD}% threshold"
    exit 1
fi