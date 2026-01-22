"""Memory and Performance Tests for Python Bindings - Issue #310.

TDD tests for:
1. Memory profiling - streaming stays under 128MB budget
2. Throughput benchmarks - execute() vs execute_streaming()
3. Memory leak detection via tracemalloc snapshots

Performance Targets:
- Memory: Streaming stays under 128MB peak
- Throughput: >10,000 rows/second for standard queries
- Latency: First row in <100ms for streaming queries
"""

import gc
import time
import tracemalloc

import pytest

import cqlite


# Table constants for performance testing
LARGE_TABLE = "test_basic.simple_table"  # 999 rows, 632 KB (most columns)
BENCHMARK_TABLE = "test_basic.simple_table"  # Same table, highest data volume
STREAMING_TABLE = "test_timeseries.sensor_data"  # 2000 rows (most rows)

# Performance thresholds
MEMORY_BUDGET_BYTES = 128 * 1024 * 1024  # 128 MB
MIN_THROUGHPUT_ROWS_PER_SEC = 10_000
MIN_STREAMING_THROUGHPUT = 5_000
MAX_FIRST_ROW_LATENCY_SEC = 0.1  # 100ms
MAX_LEAK_GROWTH_BYTES = 10 * 1024 * 1024  # 10 MB


# db and db_timeseries fixtures are provided by conftest.py


@pytest.mark.slow
class TestStreamingMemoryBudget:
    """Test that streaming stays under 128MB memory budget."""

    def test_streaming_memory_budget(self, db_timeseries):
        """TDD Test: Streaming should stay under 128MB peak memory.

        From Issue #310 TDD signature.
        """
        tracemalloc.start()
        row_count = 0

        for row in db_timeseries.execute_streaming(
            f"SELECT * FROM {STREAMING_TABLE}"
        ):
            row_count += 1
            current, peak = tracemalloc.get_traced_memory()
            assert peak < MEMORY_BUDGET_BYTES, (
                f"Memory exceeded 128MB at row {row_count}: "
                f"peak={peak / 1024 / 1024:.1f}MB"
            )

        tracemalloc.stop()
        # Ensure we actually processed rows
        assert row_count > 0, "No rows processed - test data may be missing"

    def test_streaming_memory_vs_execute(self, db):
        """Streaming should use bounded memory compared to execute()."""
        # Measure execute() memory
        gc.collect()
        tracemalloc.start()
        result = db.execute(f"SELECT * FROM {LARGE_TABLE}")
        _ = list(result.rows)  # Force full materialization
        _, execute_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        # Force GC before streaming test
        gc.collect()

        # Measure streaming memory
        tracemalloc.start()
        for row in db.execute_streaming(f"SELECT * FROM {LARGE_TABLE}"):
            pass  # Just iterate, don't store
        _, streaming_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        # Streaming should use bounded memory (not exceed execute by much)
        print(f"\nMemory comparison:")
        print(f"  execute() peak:   {execute_peak / 1024 / 1024:.2f} MB")
        print(f"  streaming peak:   {streaming_peak / 1024 / 1024:.2f} MB")

        # Soft assertion: streaming should stay under budget regardless
        assert streaming_peak < MEMORY_BUDGET_BYTES, (
            f"Streaming exceeded memory budget: "
            f"{streaming_peak / 1024 / 1024:.2f}MB > 128MB"
        )


@pytest.mark.slow
class TestExecutePerformance:
    """Test execute() throughput performance."""

    def test_execute_performance(self, db):
        """TDD Test: Execute should achieve >10,000 rows/second.

        From Issue #310 TDD signature.
        """
        start = time.perf_counter()
        result = db.execute(f"SELECT * FROM {BENCHMARK_TABLE}")
        elapsed = time.perf_counter() - start

        row_count = len(result.rows)
        if row_count == 0:
            pytest.skip("No rows returned - test data may be missing")

        rows_per_second = row_count / elapsed

        print(f"\nExecute performance:")
        print(f"  Rows: {row_count}")
        print(f"  Time: {elapsed * 1000:.2f} ms")
        print(f"  Throughput: {rows_per_second:.0f} rows/second")

        assert rows_per_second > MIN_THROUGHPUT_ROWS_PER_SEC, (
            f"Throughput {rows_per_second:.0f} rows/s below "
            f"{MIN_THROUGHPUT_ROWS_PER_SEC} minimum"
        )

    def test_streaming_first_row_latency(self, db):
        """TDD Test: First row should arrive in <100ms for streaming."""
        start = time.perf_counter()
        iterator = db.execute_streaming(f"SELECT * FROM {BENCHMARK_TABLE}")

        try:
            first_row = next(iter(iterator))
            first_row_latency = time.perf_counter() - start
        except StopIteration:
            pytest.skip("No rows returned - test data may be missing")

        print(f"\nFirst row latency: {first_row_latency * 1000:.2f} ms")

        assert first_row_latency < MAX_FIRST_ROW_LATENCY_SEC, (
            f"First row latency {first_row_latency * 1000:.2f}ms "
            f"exceeds {MAX_FIRST_ROW_LATENCY_SEC * 1000:.0f}ms"
        )

    def test_streaming_throughput(self, db_timeseries):
        """Streaming throughput should meet minimum threshold."""
        start = time.perf_counter()
        row_count = 0
        for row in db_timeseries.execute_streaming(
            f"SELECT * FROM {STREAMING_TABLE}"
        ):
            row_count += 1
        elapsed = time.perf_counter() - start

        if row_count == 0:
            pytest.skip("No rows returned - test data may be missing")

        rows_per_second = row_count / elapsed

        print(f"\nStreaming throughput:")
        print(f"  Rows: {row_count}")
        print(f"  Time: {elapsed * 1000:.2f} ms")
        print(f"  Throughput: {rows_per_second:.0f} rows/second")

        # Streaming may be slower due to per-row Python overhead
        assert rows_per_second > MIN_STREAMING_THROUGHPUT, (
            f"Streaming throughput {rows_per_second:.0f} rows/s "
            f"below {MIN_STREAMING_THROUGHPUT} minimum"
        )


@pytest.mark.slow
class TestMemoryLeakDetection:
    """Detect memory leaks using tracemalloc snapshots."""

    def test_no_memory_leak_execute(self, db):
        """Multiple execute() calls should not accumulate memory."""
        gc.collect()
        tracemalloc.start()

        # Warm up
        db.execute(f"SELECT * FROM {BENCHMARK_TABLE} LIMIT 10")
        gc.collect()

        # Take baseline snapshot
        snapshot1 = tracemalloc.take_snapshot()

        # Run multiple queries
        for _ in range(10):
            result = db.execute(f"SELECT * FROM {BENCHMARK_TABLE}")
            _ = len(result.rows)

        gc.collect()
        snapshot2 = tracemalloc.take_snapshot()

        # Compare snapshots
        top_stats = snapshot2.compare_to(snapshot1, "lineno")

        # Calculate total memory growth
        total_growth = sum(
            stat.size_diff for stat in top_stats if stat.size_diff > 0
        )

        tracemalloc.stop()

        print(f"\nMemory growth after 10 queries: {total_growth / 1024:.2f} KB")

        # Allow some growth but flag significant leaks
        assert total_growth < MAX_LEAK_GROWTH_BYTES, (
            f"Possible memory leak: {total_growth / 1024 / 1024:.2f}MB growth"
        )

    def test_no_memory_leak_streaming(self, db):
        """Multiple streaming iterations should not accumulate memory."""
        gc.collect()
        tracemalloc.start()

        # Warm up
        for row in db.execute_streaming(
            f"SELECT * FROM {BENCHMARK_TABLE} LIMIT 10"
        ):
            pass
        gc.collect()

        snapshot1 = tracemalloc.take_snapshot()

        # Run multiple streaming queries
        for _ in range(10):
            for row in db.execute_streaming(f"SELECT * FROM {BENCHMARK_TABLE}"):
                pass

        gc.collect()
        snapshot2 = tracemalloc.take_snapshot()

        top_stats = snapshot2.compare_to(snapshot1, "lineno")
        total_growth = sum(
            stat.size_diff for stat in top_stats if stat.size_diff > 0
        )

        tracemalloc.stop()

        print(
            f"\nMemory growth after 10 streaming iterations: "
            f"{total_growth / 1024:.2f} KB"
        )

        assert total_growth < MAX_LEAK_GROWTH_BYTES, (
            f"Possible memory leak: {total_growth / 1024 / 1024:.2f}MB growth"
        )

    def test_iterator_cleanup_no_leak(self, db):
        """Abandoned iterators should not leak memory."""
        gc.collect()
        tracemalloc.start()

        snapshot1 = tracemalloc.take_snapshot()

        # Create and abandon iterators without fully consuming
        for _ in range(100):
            iterator = db.execute_streaming(f"SELECT * FROM {BENCHMARK_TABLE}")
            # Consume just one row then abandon
            try:
                next(iter(iterator))
            except StopIteration:
                pass
            del iterator

        gc.collect()
        snapshot2 = tracemalloc.take_snapshot()

        top_stats = snapshot2.compare_to(snapshot1, "lineno")
        total_growth = sum(
            stat.size_diff for stat in top_stats if stat.size_diff > 0
        )

        tracemalloc.stop()

        print(
            f"\nMemory growth after 100 abandoned iterators: "
            f"{total_growth / 1024:.2f} KB"
        )

        # Should not grow significantly (< 5MB for 100 iterators)
        assert total_growth < 5 * 1024 * 1024, (
            f"Iterator cleanup leak: {total_growth / 1024 / 1024:.2f}MB growth"
        )


@pytest.mark.slow
class TestPerformanceSummary:
    """Generate a performance summary report (informational)."""

    def test_performance_report(self, db, db_timeseries):
        """Print comprehensive performance metrics."""
        print("\n" + "=" * 60)
        print("Python Bindings Performance Report - Issue #310")
        print("=" * 60)

        # Execute performance
        start = time.perf_counter()
        result = db.execute(f"SELECT * FROM {LARGE_TABLE}")
        execute_time = time.perf_counter() - start
        execute_rows = len(result.rows)

        # Streaming performance
        start = time.perf_counter()
        streaming_rows = 0
        for row in db_timeseries.execute_streaming(
            f"SELECT * FROM {STREAMING_TABLE}"
        ):
            streaming_rows += 1
        streaming_time = time.perf_counter() - start

        # Memory for execute
        gc.collect()
        tracemalloc.start()
        result = db.execute(f"SELECT * FROM {LARGE_TABLE}")
        _ = list(result.rows)
        _, execute_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        # Memory for streaming
        gc.collect()
        tracemalloc.start()
        peak_during_stream = 0
        for row in db_timeseries.execute_streaming(
            f"SELECT * FROM {STREAMING_TABLE}"
        ):
            _, current_peak = tracemalloc.get_traced_memory()
            peak_during_stream = max(peak_during_stream, current_peak)
        tracemalloc.stop()

        # First row latency
        start = time.perf_counter()
        iterator = db.execute_streaming(f"SELECT * FROM {BENCHMARK_TABLE}")
        try:
            next(iter(iterator))
            first_row_latency = time.perf_counter() - start
        except StopIteration:
            first_row_latency = 0

        print(f"\n{'Metric':<35} {'Value':<20} {'Target':<15}")
        print("-" * 70)
        print(f"{'execute() rows':<35} {execute_rows:<20}")
        print(f"{'execute() time':<35} {execute_time*1000:.2f} ms")
        if execute_time > 0:
            throughput = execute_rows / execute_time
            print(
                f"{'execute() throughput':<35} "
                f"{throughput:.0f} rows/s{'':<5} >10,000"
            )
        print(f"{'execute() peak memory':<35} {execute_peak/1024/1024:.2f} MB")
        print()
        print(f"{'streaming rows':<35} {streaming_rows:<20}")
        print(f"{'streaming time':<35} {streaming_time*1000:.2f} ms")
        if streaming_time > 0:
            stream_throughput = streaming_rows / streaming_time
            print(
                f"{'streaming throughput':<35} "
                f"{stream_throughput:.0f} rows/s{'':<5} >5,000"
            )
        print(
            f"{'streaming peak memory':<35} "
            f"{peak_during_stream/1024/1024:.2f} MB{'':<5} <128 MB"
        )
        print()
        print(
            f"{'first row latency':<35} "
            f"{first_row_latency*1000:.2f} ms{'':<8} <100 ms"
        )
        print("=" * 60)

        # This test is informational - always passes
        assert True
