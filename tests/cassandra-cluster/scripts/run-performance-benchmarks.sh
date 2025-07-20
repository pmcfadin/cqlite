#!/bin/bash

# CQLite Performance Benchmarking Against Real Cassandra 5+ Data
# Validates performance targets and regression testing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DATA_DIR="/opt/test-data"
RESULTS_DIR="/opt/test-data/performance-results"
CQLITE_SOURCE="/opt/cqlite/source"

echo "⚡ Starting CQLite Performance Benchmarking"
echo "🎯 Target: Validate performance against Cassandra 5+ data"

# Setup performance test environment
mkdir -p $RESULTS_DIR
cd $CQLITE_SOURCE

# Ensure optimized build
echo "🔨 Building CQLite with optimizations..."
cargo build --release

# Run comprehensive performance benchmarks
echo "📊 Running comprehensive performance benchmark suite..."

# Benchmark 1: SSTable parsing performance
echo "   🔍 Benchmark 1: SSTable parsing performance..."
cargo bench --bench compatibility_testing -- \
    --warm-up-time 10 \
    --measurement-time 30 \
    sstable_parsing 2>&1 | tee $RESULTS_DIR/sstable-parsing-bench.log

# Benchmark 2: Data type serialization/deserialization
echo "   🔄 Benchmark 2: Data type serialization performance..."
cargo bench --bench compatibility_testing -- \
    --warm-up-time 5 \
    --measurement-time 20 \
    type_serialization 2>&1 | tee $RESULTS_DIR/type-serialization-bench.log

# Benchmark 3: Large dataset processing
echo "   💾 Benchmark 3: Large dataset processing..."
cargo bench --bench load_testing -- \
    --warm-up-time 15 \
    --measurement-time 60 \
    large_dataset 2>&1 | tee $RESULTS_DIR/large-dataset-bench.log

# Benchmark 4: Concurrent read performance
echo "   🔀 Benchmark 4: Concurrent read performance..."
cargo bench --bench performance_suite -- \
    --warm-up-time 10 \
    --measurement-time 45 \
    concurrent_reads 2>&1 | tee $RESULTS_DIR/concurrent-reads-bench.log

# Benchmark 5: Memory efficiency testing
echo "   🧠 Benchmark 5: Memory efficiency testing..."
/usr/bin/time -v cargo run --release --bin cqlite-cli -- \
    memory-stress-test \
    --input-dir $TEST_DATA_DIR/sstables \
    --max-memory 1GB \
    --iterations 1000 \
    --concurrent-readers 8 2>&1 | tee $RESULTS_DIR/memory-efficiency-bench.log

# Benchmark 6: Real-world workload simulation
echo "   🌍 Benchmark 6: Real-world workload simulation..."
python3 << 'EOF'
import time
import json
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

results_dir = '/opt/test-data/performance-results'

def run_cqlite_operation(operation_type, data_path, thread_id):
    """Run a CQLite operation and measure performance"""
    start_time = time.time()
    
    try:
        cmd = [
            'cargo', 'run', '--release', '--bin', 'cqlite-cli', '--',
            operation_type,
            '--input-dir', data_path,
            '--thread-id', str(thread_id)
        ]
        
        result = subprocess.run(cmd, 
            capture_output=True, 
            text=True, 
            cwd='/opt/cqlite/source',
            timeout=120
        )
        
        end_time = time.time()
        
        return {
            'thread_id': thread_id,
            'operation': operation_type,
            'duration_seconds': end_time - start_time,
            'success': result.returncode == 0,
            'output_size': len(result.stdout) if result.stdout else 0,
            'error': result.stderr if result.returncode != 0 else None
        }
        
    except subprocess.TimeoutExpired:
        return {
            'thread_id': thread_id,
            'operation': operation_type,
            'duration_seconds': 120,
            'success': False,
            'error': 'Operation timed out'
        }
    except Exception as e:
        return {
            'thread_id': thread_id,
            'operation': operation_type,
            'duration_seconds': time.time() - start_time,
            'success': False,
            'error': str(e)
        }

# Simulate concurrent workload
print("🔀 Running concurrent workload simulation...")

test_scenarios = [
    ('read-sstables', '/opt/test-data/sstables/primitive_types'),
    ('read-sstables', '/opt/test-data/sstables/collections'), 
    ('read-sstables', '/opt/test-data/sstables/large_data'),
    ('parse-metadata', '/opt/test-data/sstables/time_series'),
    ('validate-format', '/opt/test-data/sstables/udts'),
    ('analyze-structure', '/opt/test-data/sstables/indexed_data')
]

# Run with different concurrency levels
concurrency_levels = [1, 4, 8, 16]
workload_results = {}

for concurrency in concurrency_levels:
    print(f"   Testing with {concurrency} concurrent operations...")
    
    start_time = time.time()
    results = []
    
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        # Submit tasks
        futures = []
        for i in range(concurrency):
            operation, data_path = test_scenarios[i % len(test_scenarios)]
            future = executor.submit(run_cqlite_operation, operation, data_path, i)
            futures.append(future)
        
        # Collect results
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"   Error in concurrent operation: {e}")
    
    total_time = time.time() - start_time
    successful_ops = sum(1 for r in results if r['success'])
    
    workload_results[f'concurrency_{concurrency}'] = {
        'total_operations': len(results),
        'successful_operations': successful_ops,
        'total_time_seconds': total_time,
        'operations_per_second': successful_ops / total_time if total_time > 0 else 0,
        'average_operation_time': sum(r['duration_seconds'] for r in results) / len(results) if results else 0,
        'detailed_results': results
    }

# Save workload results
with open(os.path.join(results_dir, 'workload-simulation.json'), 'w') as f:
    json.dump(workload_results, f, indent=2)

print("✅ Concurrent workload simulation completed")
EOF

# Benchmark 7: Compression algorithm performance
echo "   🗜️ Benchmark 7: Compression algorithm performance..."
python3 << 'EOF'
import os
import time
import json
import subprocess

results_dir = '/opt/test-data/performance-results'

# Test different compression algorithms if available
compression_results = {}
test_file = '/opt/test-data/sstables/large_data'

if os.path.exists(test_file):
    for compression in ['none', 'lz4', 'snappy', 'deflate']:
        print(f"   Testing {compression} compression...")
        
        start_time = time.time()
        
        try:
            cmd = [
                'cargo', 'run', '--release', '--bin', 'cqlite-cli', '--',
                'test-compression',
                '--algorithm', compression,
                '--input-dir', test_file,
                '--iterations', '10'
            ]
            
            result = subprocess.run(cmd,
                capture_output=True,
                text=True,
                cwd='/opt/cqlite/source',
                timeout=60
            )
            
            end_time = time.time()
            
            compression_results[compression] = {
                'duration_seconds': end_time - start_time,
                'success': result.returncode == 0,
                'output': result.stdout if result.returncode == 0 else result.stderr
            }
            
        except subprocess.TimeoutExpired:
            compression_results[compression] = {
                'duration_seconds': 60,
                'success': False,
                'error': 'Compression test timed out'
            }
        except Exception as e:
            compression_results[compression] = {
                'duration_seconds': time.time() - start_time,
                'success': False,
                'error': str(e)
            }

# Save compression results
with open(os.path.join(results_dir, 'compression-performance.json'), 'w') as f:
    json.dump(compression_results, f, indent=2)

print("✅ Compression performance testing completed")
EOF

# Generate performance regression analysis
echo "📈 Generating performance regression analysis..."
python3 << 'EOF'
import json
import os
import re
from datetime import datetime

results_dir = '/opt/test-data/performance-results'

# Parse benchmark results from log files
def parse_criterion_output(log_content):
    """Parse Criterion benchmark output for performance metrics"""
    results = {}
    
    # Look for benchmark results
    pattern = r'(\w+)\s+time:\s+\[([0-9.]+)\s+([a-z]+)\s+([0-9.]+)\s+([a-z]+)\s+([0-9.]+)\s+([a-z]+)\]'
    matches = re.findall(pattern, log_content)
    
    for match in matches:
        benchmark_name = match[0]
        lower_bound = float(match[1])
        lower_unit = match[2]
        estimate = float(match[3])
        estimate_unit = match[4]
        upper_bound = float(match[5])
        upper_unit = match[6]
        
        # Convert to standardized units (microseconds)
        def to_microseconds(value, unit):
            if unit == 'ns':
                return value / 1000
            elif unit == 'us':
                return value
            elif unit == 'ms':
                return value * 1000
            elif unit == 's':
                return value * 1000000
            return value
        
        results[benchmark_name] = {
            'estimate_microseconds': to_microseconds(estimate, estimate_unit),
            'lower_bound_microseconds': to_microseconds(lower_bound, lower_unit),
            'upper_bound_microseconds': to_microseconds(upper_bound, upper_unit),
            'unit': estimate_unit
        }
    
    return results

performance_analysis = {
    'analysis_timestamp': datetime.now().isoformat(),
    'benchmarks': {},
    'performance_targets': {
        'sstable_parsing_per_mb': 100000,  # microseconds per MB
        'type_serialization_per_op': 10,   # microseconds per operation
        'memory_usage_per_mb': 50,         # MB memory per MB data
        'concurrent_throughput': 1000      # operations per second
    },
    'regression_analysis': {}
}

# Parse all benchmark log files
benchmark_files = [
    ('sstable_parsing', 'sstable-parsing-bench.log'),
    ('type_serialization', 'type-serialization-bench.log'),
    ('large_dataset', 'large-dataset-bench.log'),
    ('concurrent_reads', 'concurrent-reads-bench.log')
]

for benchmark_name, log_file in benchmark_files:
    log_path = os.path.join(results_dir, log_file)
    if os.path.exists(log_path):
        with open(log_path, 'r') as f:
            content = f.read()
            parsed_results = parse_criterion_output(content)
            performance_analysis['benchmarks'][benchmark_name] = parsed_results

# Load additional results
additional_files = [
    'workload-simulation.json',
    'compression-performance.json'
]

for file_name in additional_files:
    file_path = os.path.join(results_dir, file_name)
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                performance_analysis['benchmarks'][file_name.replace('.json', '')] = data
        except Exception as e:
            print(f"Warning: Could not load {file_name}: {e}")

# Regression analysis
targets = performance_analysis['performance_targets']
benchmarks = performance_analysis['benchmarks']

for category, target in targets.items():
    if category in benchmarks:
        actual = benchmarks[category]
        
        # Simple regression check - compare against targets
        performance_analysis['regression_analysis'][category] = {
            'target': target,
            'status': 'unknown',
            'details': 'Analysis needs actual vs target comparison logic'
        }

# Save performance analysis
with open(os.path.join(results_dir, 'performance-regression-analysis.json'), 'w') as f:
    json.dump(performance_analysis, f, indent=2)

print("📈 Performance regression analysis completed")
EOF

# Display performance summary
echo ""
echo "⚡ CQLite Performance Benchmarking Complete!"
echo "="
echo "📊 Performance Results Summary:"

# Check if performance analysis exists
if [ -f "$RESULTS_DIR/performance-regression-analysis.json" ]; then
    python3 -c "
import json
with open('$RESULTS_DIR/performance-regression-analysis.json', 'r') as f:
    analysis = json.load(f)
    print('   • Benchmark Categories:', len(analysis['benchmarks']))
    print('   • Performance Targets:', len(analysis['performance_targets']))
    
    # Display key metrics if available
    if 'workload_simulation' in analysis['benchmarks']:
        workload = analysis['benchmarks']['workload_simulation']
        if 'concurrency_8' in workload:
            ops_per_sec = workload['concurrency_8'].get('operations_per_second', 0)
            print(f'   • Concurrent Throughput (8 threads): {ops_per_sec:.1f} ops/sec')
"
else
    echo "   • Performance analysis not available - check benchmark logs"
fi

echo ""
echo "📁 Performance Results Available:"
echo "   • SSTable parsing: $RESULTS_DIR/sstable-parsing-bench.log"
echo "   • Type serialization: $RESULTS_DIR/type-serialization-bench.log"
echo "   • Large dataset: $RESULTS_DIR/large-dataset-bench.log"
echo "   • Concurrent reads: $RESULTS_DIR/concurrent-reads-bench.log"
echo "   • Memory efficiency: $RESULTS_DIR/memory-efficiency-bench.log"
echo "   • Workload simulation: $RESULTS_DIR/workload-simulation.json"
echo "   • Compression performance: $RESULTS_DIR/compression-performance.json"
echo "   • Regression analysis: $RESULTS_DIR/performance-regression-analysis.json"
echo ""

# Performance target validation
if [ -f "$RESULTS_DIR/workload-simulation.json" ]; then
    echo "🎯 Performance Target Validation:"
    
    # Check if we meet performance targets
    concurrent_ops=$(python3 -c "
import json
try:
    with open('$RESULTS_DIR/workload-simulation.json', 'r') as f:
        data = json.load(f)
        if 'concurrency_8' in data:
            print(data['concurrency_8'].get('operations_per_second', 0))
        else:
            print(0)
except:
    print(0)
")
    
    if (( $(echo "$concurrent_ops >= 100" | bc -l) )); then
        echo "   ✅ Concurrent throughput: $concurrent_ops ops/sec (target: >100)"
    else
        echo "   ❌ Concurrent throughput: $concurrent_ops ops/sec (target: >100)"
    fi
else
    echo "   ⚠️  Could not validate performance targets"
fi

echo ""
echo "🚀 Performance benchmarking complete!"
echo "📊 Use results for performance regression testing and optimization planning"