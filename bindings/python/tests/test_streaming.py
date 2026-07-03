"""Tests for Database.execute_streaming() - Issue #294.

TDD tests for streaming query execution with memory-bounded iteration.

Tests verify:
1. execute_streaming() returns StreamingIterator
2. Iteration yields Row objects one at a time
3. Custom StreamingConfig is respected
4. Early termination works correctly
5. Memory usage stays bounded
"""

import sys
import threading
import time

import pytest

import cqlite

from conftest import (
    DATASETS,
    SCHEMA_WIDE_ROWS,
    require_test_data,
)


class TestStreamingGilRelease:
    """Issue #1441: streaming __next__ must release the GIL during its blocking
    buffer refill so other Python threads make progress."""

    def test_streaming_next_releases_gil(self):
        """A concurrent Python thread makes progress ONLY while the streaming
        iterator's ``__next__`` has the GIL released.

        Design (why this discriminates the bug):

        * Python's periodic thread switch is disabled for the test window via
          ``sys.setswitchinterval(1000)``. With voluntary preemption off, the
          *only* way the busy-spin thread (B) can run while the streaming thread
          (A) is executing is if A **explicitly releases the GIL**. B still
          yields cooperatively (``time.sleep(0)`` always drops the GIL), so A is
          never starved.
        * Thread A iterates a wide table with ``buffer_size=1`` so every row is a
          separate blocking channel refill (``block_on(next_async())``).
        * On unmodified ``main``, ``__next__`` holds the GIL across that
          ``block_on``, so B is frozen for the whole iteration → its counter
          barely moves past its pre-iteration baseline.
        * With the fix, each refill runs inside ``py.allow_threads(...)``; B runs
          during every refill and its counter advances far past the floor.

        The assertion measures B's progress *delta* strictly between A's first
        and last row so B's small pre-start head start does not leak in.
        """
        # Fail loudly (not skip) under strict fixture mode (issue #1230).
        require_test_data(SCHEMA_WIDE_ROWS)

        # No-GIL (free-threaded) builds have no GIL to release; the concurrency
        # claim under test is GIL-specific.
        if not getattr(sys, "_is_gil_enabled", lambda: True)():
            pytest.skip("free-threaded build: no GIL to release")

        counter = {"n": 0}
        rows = {"n": 0, "error": None}
        baseline = {"n": None}
        stop = threading.Event()
        a_started = threading.Event()
        a_done = threading.Event()

        def spin():
            # Cooperative busy loop. sleep(0) always releases the GIL, so A is
            # never blocked by B; but with the switch interval raised, B only
            # gets scheduled when SOME thread hands off the GIL.
            while not stop.is_set():
                counter["n"] += 1
                time.sleep(0)

        def iterate():
            try:
                with cqlite.open(DATASETS, schema=SCHEMA_WIDE_ROWS) as database:
                    # wide_partition_table is the largest wide-row fixture.
                    # buffer_size=1 forces one blocking refill per row.
                    config = cqlite.StreamingConfig(buffer_size=1)
                    n = 0
                    for _row in database.execute_streaming(
                        "SELECT * FROM test_wide_rows.wide_partition_table",
                        config=config,
                    ):
                        if n == 0:
                            # Snapshot B's counter at the first row, then signal:
                            # everything after this is attributable to GIL state
                            # during refills, not to B's pre-start head start.
                            baseline["n"] = counter["n"]
                            a_started.set()
                        n += 1
                    rows["n"] = n
            except Exception as exc:  # noqa: BLE001 - surfaced in main thread
                rows["error"] = exc
            finally:
                a_started.set()
                a_done.set()

        old_interval = sys.getswitchinterval()
        sys.setswitchinterval(1000)  # effectively disable periodic preemption
        b = threading.Thread(target=spin, daemon=True)
        a = threading.Thread(target=iterate, daemon=True)
        try:
            b.start()
            a.start()
            a.join(timeout=30)
            after = counter["n"]
        finally:
            sys.setswitchinterval(old_interval)
            stop.set()

        b.join(timeout=5)

        assert a_done.is_set(), "streaming iteration did not finish in time"
        assert rows["error"] is None, f"streaming thread failed: {rows['error']}"
        # Guard against a trivially-green run: A must have done real multi-refill
        # blocking work for the concurrency claim to mean anything. A dropped or
        # unfetched fixture would otherwise let B spin freely and pass vacuously.
        assert rows["n"] > 1, (
            f"streaming thread yielded {rows['n']} rows; need a multi-row wide "
            "fixture for a meaningful GIL-release assertion"
        )
        assert baseline["n"] is not None, "did not capture B baseline at first row"

        # Progress B made strictly DURING A's blocking refills.
        during = after - baseline["n"]

        # Floor: generous vs CI flake, but far above the ~0 progress a
        # GIL-starved B makes on unmodified main (where B is frozen across every
        # refill). Empirically the fixed build advances B into the many
        # thousands during the ~100-row iteration; unmodified main leaves it near
        # zero.
        floor = 500
        assert during > floor, (
            f"concurrent thread advanced only {during} increments during "
            f"streaming (floor {floor}); GIL appears held across refill "
            f"(baseline={baseline['n']}, after={after})"
        )


class TestStreamingImports:
    """Test that streaming-related types are importable."""

    def test_streaming_iterator_importable(self):
        """StreamingIterator class should be importable from cqlite."""
        assert hasattr(cqlite, "StreamingIterator")
        assert cqlite.StreamingIterator is not None

    def test_streaming_config_importable(self):
        """StreamingConfig class should be importable from cqlite."""
        assert hasattr(cqlite, "StreamingConfig")
        assert cqlite.StreamingConfig is not None

    def test_types_in_all(self):
        """Streaming types should be in __all__."""
        assert "StreamingIterator" in cqlite.__all__
        assert "StreamingConfig" in cqlite.__all__


class TestStreamingBasic:
    """Basic execute_streaming() functionality."""

    def test_execute_streaming_returns_iterator(self, db):
        """execute_streaming() should return StreamingIterator."""
        result = db.execute_streaming("SELECT * FROM test_basic.simple_table LIMIT 5")
        assert isinstance(result, cqlite.StreamingIterator)

    def test_streaming_iteration_yields_rows(self, db):
        """Streaming should yield Row objects."""
        count = 0
        for row in db.execute_streaming("SELECT * FROM test_basic.simple_table LIMIT 5"):
            assert isinstance(row, cqlite.Row)
            count += 1
            if count >= 5:
                break
        # May be 0 if no data, but iteration should work
        assert count >= 0

    def test_streaming_for_loop(self, db):
        """Should work in for loop."""
        rows = []
        for row in db.execute_streaming("SELECT * FROM test_basic.simple_table LIMIT 3"):
            rows.append(row)
            if len(rows) >= 3:
                break
        assert all(isinstance(r, cqlite.Row) for r in rows)

    def test_streaming_list_conversion(self, db):
        """Should be convertible to list."""
        iterator = db.execute_streaming("SELECT * FROM test_basic.simple_table LIMIT 3")
        rows = list(iterator)
        assert isinstance(rows, list)
        assert all(isinstance(r, cqlite.Row) for r in rows)

    def test_streaming_on_closed_db_raises(self, db):
        """execute_streaming() on closed database should raise RuntimeError."""
        db.close()
        with pytest.raises(RuntimeError):
            db.execute_streaming("SELECT * FROM test_basic.simple_table")

    def test_streaming_invalid_query_raises(self, db):
        """Invalid SQL should raise QueryError or ParseError."""
        with pytest.raises((cqlite.ParseError, cqlite.QueryError)):
            list(db.execute_streaming("SELEKT * FORM users"))


class TestStreamingWithConfig:
    """Test execute_streaming() with custom StreamingConfig."""

    def test_streaming_with_default_config(self, db):
        """Streaming with default config should work."""
        config = cqlite.StreamingConfig()
        result = db.execute_streaming(
            "SELECT * FROM test_basic.simple_table LIMIT 5",
            config=config
        )
        assert isinstance(result, cqlite.StreamingIterator)

    def test_streaming_with_custom_buffer_size(self, db):
        """Streaming with custom buffer_size should work."""
        config = cqlite.StreamingConfig(buffer_size=512)
        result = db.execute_streaming(
            "SELECT * FROM test_basic.simple_table LIMIT 5",
            config=config
        )
        rows = list(result)
        assert isinstance(rows, list)

    def test_streaming_with_custom_chunk_size(self, db):
        """Streaming with custom chunk_size should work."""
        config = cqlite.StreamingConfig(chunk_size=1000)
        result = db.execute_streaming(
            "SELECT * FROM test_basic.simple_table LIMIT 5",
            config=config
        )
        rows = list(result)
        assert isinstance(rows, list)

    def test_streaming_with_full_config(self, db):
        """Streaming with all config options should work."""
        config = cqlite.StreamingConfig(buffer_size=256, chunk_size=500)
        result = db.execute_streaming(
            "SELECT * FROM test_basic.simple_table LIMIT 5",
            config=config
        )
        rows = list(result)
        assert isinstance(rows, list)


class TestStreamingEarlyTermination:
    """Test early termination (break from loop)."""

    def test_break_from_loop(self, db):
        """Breaking from iteration should work without errors."""
        count = 0
        for row in db.execute_streaming("SELECT * FROM test_basic.simple_table"):
            count += 1
            if count >= 2:
                break
        # Should have stopped at 2 rows (or fewer if table is small)
        assert count <= 2 or count == 0  # 0 if no data

    def test_partial_iteration(self, db):
        """Partial iteration followed by abandonment should be safe."""
        iterator = db.execute_streaming("SELECT * FROM test_basic.simple_table LIMIT 10")
        # Get just one row
        try:
            first_row = next(iter(iterator))
            assert isinstance(first_row, cqlite.Row)
        except StopIteration:
            pass  # No data is fine
        # Iterator goes out of scope - should clean up gracefully
        del iterator


class TestStreamingIteratorAttributes:
    """Test StreamingIterator attributes and methods."""

    def test_rows_received_attribute(self, db):
        """StreamingIterator should track rows_received."""
        iterator = db.execute_streaming("SELECT * FROM test_basic.simple_table LIMIT 5")
        initial_count = iterator.rows_received
        assert isinstance(initial_count, int)
        assert initial_count >= 0

        # Consume some rows
        for row in iterator:
            break
        # rows_received may have increased (or stayed 0 if no data)
        assert iterator.rows_received >= initial_count

    def test_progress_percent_attribute(self, db):
        """StreamingIterator should have progress_percent (may be None)."""
        iterator = db.execute_streaming("SELECT * FROM test_basic.simple_table LIMIT 5")
        progress = iterator.progress_percent
        # May be None if total is unknown, or a float if known
        assert progress is None or isinstance(progress, float)

    def test_repr(self, db):
        """StreamingIterator should have meaningful repr."""
        iterator = db.execute_streaming("SELECT * FROM test_basic.simple_table LIMIT 5")
        repr_str = repr(iterator)
        assert "StreamingIterator" in repr_str


class TestStreamingRowAccess:
    """Test Row access from streaming results."""

    def test_row_dict_access(self, db):
        """Rows from streaming should support dict-style access."""
        for row in db.execute_streaming("SELECT * FROM test_basic.simple_table LIMIT 1"):
            keys = row.keys()
            assert isinstance(keys, list)
            if keys:
                # Access should work for existing columns
                _ = row[keys[0]]
            break

    def test_row_to_dict(self, db):
        """Rows from streaming should support to_dict()."""
        for row in db.execute_streaming("SELECT * FROM test_basic.simple_table LIMIT 1"):
            d = row.to_dict()
            assert isinstance(d, dict)
            break


class TestStreamingMemory:
    """Test memory behavior of streaming (informational)."""

    def test_streaming_memory_bounded(self, db):
        """Streaming should use bounded memory (informational test)."""
        import tracemalloc

        tracemalloc.start()

        # Consume some rows via streaming
        count = 0
        for row in db.execute_streaming("SELECT * FROM test_basic.simple_table"):
            count += 1
            if count >= 100:
                break

        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        # Informational: log the peak memory
        # This is not a hard assertion since memory depends on many factors
        # but peak should be well under 128MB for streaming
        assert peak < 128 * 1024 * 1024, f"Peak memory {peak / 1024 / 1024:.1f}MB exceeded 128MB"
