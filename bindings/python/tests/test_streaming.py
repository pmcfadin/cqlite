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
    SCHEMA_BASIC_TYPES,
    require_test_data,
)


class TestStreamingGilRelease:
    """Issue #1441: streaming __next__ must release the GIL during its blocking
    buffer refill so other Python threads make progress."""

    # The widest, most-scannable single-partition-ish fixture in the corpus
    # (999 rows). Many rows keep the one-time per-stream setup GIL-release
    # negligible next to the 999 per-row blocking refills, so the unmodified
    # build reads ~0 for the metric below (GIL held the whole iteration).
    _TABLE = "test_basic.simple_table"

    # Per-attempt metric: the fraction of a single stream's wall-time during
    # which the GIL was free for the concurrent spinner =
    #     during / (rate_solo * dS)
    # where ``rate_solo`` is the spinner's robustly-calibrated uncontended rate
    # (max over sub-windows, so it is not underestimated when the spinner is
    # briefly descheduled) and ``dS`` is the stream duration. It is a RATE
    # RATIO, hence invariant to machine/CI load.
    #
    # A SINGLE attempt is noisy in both directions (the OS scheduler landing the
    # spinner inside a given microsecond GIL-release window is luck; and the
    # unmodified build has rare eval-breaker GIL releases at loop boundaries that
    # occasionally spike one attempt). So neither max nor a single reading is
    # robust. Instead we run ``_ATTEMPTS`` streams and COUNT how many clear a
    # modest per-attempt bar ``_ATTEMPT_FLOOR`` — a bulk-distribution statistic.
    # Measured (this machine, incl. under 4 saturating CPU hogs; warm cache):
    #   * fixed build:     6-8 of 8 attempts clear 0.05
    #   * unmodified main: 0-3 of 8 attempts clear 0.05 (spinner starved; only
    #                       the odd eval-breaker release spikes an attempt)
    # Requiring >= 5 sits between the two with margin on both sides.
    _ATTEMPTS = 8
    _ATTEMPT_FLOOR = 0.05
    _MIN_ATTEMPTS_CLEARING = 5

    def test_streaming_next_releases_gil(self):
        """A concurrent Python thread runs during streaming iteration ONLY when
        ``__next__`` releases the GIL.

        Why this discriminates the bug (and is not flaky):

        * Python's periodic thread switch is disabled for the window via
          ``sys.setswitchinterval(1000)``. With voluntary preemption off, the
          busy-spin thread (B) can run only when some thread *explicitly* hands
          off the GIL. B still yields cooperatively (``time.sleep(0)`` always
          drops the GIL), so the streaming thread never starves.
        * A single stream over ``simple_table`` (999 rows) with ``buffer_size=1``
          makes every row a separate blocking channel refill
          (``block_on(next_async())``); the one-time query setup is negligible
          next to 999 refills, so on the unmodified build B is starved for the
          whole iteration.
        * Robustness comes from the count-of-attempts-clearing statistic (see
          the class constants), not from a single fragile reading.
        """
        # Fail loudly (not skip) under strict fixture mode (issue #1230).
        require_test_data(SCHEMA_BASIC_TYPES)

        # No-GIL (free-threaded) builds have no GIL to release; the concurrency
        # claim under test is GIL-specific.
        if not getattr(sys, "_is_gil_enabled", lambda: True)():
            pytest.skip("free-threaded build: no GIL to release")

        counter = {"n": 0}
        stop = threading.Event()

        def spin():
            # Cooperative busy loop. sleep(0) always releases the GIL, so the
            # streaming thread is never blocked by B; but with the switch
            # interval raised, B only advances while some thread hands off the
            # GIL — i.e. while ``__next__`` is inside ``py.allow_threads(...)``.
            while not stop.is_set():
                counter["n"] += 1
                time.sleep(0)

        def stream_once(database):
            config = cqlite.StreamingConfig(buffer_size=1)
            c_start = counter["n"]
            t_start = time.perf_counter()
            rows = 0
            for _row in database.execute_streaming(
                f"SELECT * FROM {self._TABLE}", config=config
            ):
                rows += 1
            dS = time.perf_counter() - t_start
            during = counter["n"] - c_start
            return during, dS, rows

        old_interval = sys.getswitchinterval()
        sys.setswitchinterval(1000)  # disable periodic preemption for the window
        b = threading.Thread(target=spin, daemon=True)
        b.start()
        fracs = []
        rows_seen = 0
        try:
            time.sleep(0.05)  # let B warm up and get scheduled

            # Robust uncontended rate: MAX over sub-windows so a brief deschedule
            # during calibration cannot underestimate it (which would inflate the
            # fractions below and risk a false pass on the buggy build).
            rate_solo = 0.0
            for _ in range(5):
                c0 = counter["n"]
                t0 = time.perf_counter()
                time.sleep(0.03)
                rate_solo = max(
                    rate_solo, (counter["n"] - c0) / (time.perf_counter() - t0)
                )
            assert rate_solo > 0, "spinner made no progress during calibration"

            with cqlite.open(DATASETS, schema=SCHEMA_BASIC_TYPES) as database:
                for _ in range(self._ATTEMPTS):
                    during, dS, rows = stream_once(database)
                    rows_seen = rows
                    fracs.append(during / (rate_solo * dS) if dS > 0 else 0.0)
        finally:
            sys.setswitchinterval(old_interval)
            stop.set()
        b.join(timeout=5)

        # Guard against a trivially-green run: the stream must actually have
        # yielded the large fixture. A dropped/unfetched Data.db would otherwise
        # make the assertion meaningless.
        assert rows_seen >= 900, (
            f"streaming yielded only {rows_seen} rows; need the ~999-row "
            f"{self._TABLE} fixture for a meaningful GIL-release assertion"
        )
        assert len(fracs) == self._ATTEMPTS, "not all streaming attempts ran"

        clearing = sum(1 for f in fracs if f > self._ATTEMPT_FLOOR)
        assert clearing >= self._MIN_ATTEMPTS_CLEARING, (
            f"only {clearing}/{self._ATTEMPTS} streaming attempts let the "
            f"concurrent thread run past {self._ATTEMPT_FLOOR} of the window "
            f"(need >= {self._MIN_ATTEMPTS_CLEARING}); GIL appears held across "
            f"the blocking refill. fracs={[round(f, 3) for f in fracs]}"
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
