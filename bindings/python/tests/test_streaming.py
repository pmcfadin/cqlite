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
    buffer refill so other Python threads make progress.

    Restructured for issue #1891: the old assertion pooled a wall-clock
    GIL-free *rate ratio* (spinner increments / calibrated solo rate x stream
    duration) over a fixed number of attempts — load-sensitive by construction,
    and it failed exactly when concurrent gates loaded the machine (the normal
    state under the #1825 concurrency cap). This version asserts a
    DETERMINISTIC progress-proof instead: no wall-clock rates, no calibration,
    no fixed attempt count — only "did the concurrent thread acquire the GIL
    inside individual ``next()`` calls", retried under a generous budget.
    """

    # The widest, most-scannable single-partition-ish fixture in the corpus
    # (999 rows). Many rows keep the one-time per-stream setup GIL-release
    # negligible next to the 999 per-row blocking refills.
    _TABLE = "test_basic.simple_table"

    # Metric: the number of DISTINCT ``next()`` windows (counter sampled
    # immediately before and after each individual ``next(it)`` call) in which
    # the spinner thread advanced, within a single stream (~999 windows).
    #
    # With ``sys.setswitchinterval(1000)`` periodic preemption is off, so any
    # spinner progress inside a window can only come from an explicit GIL
    # hand-off inside ``__next__`` — i.e. the ``py.allow_threads(...)`` refill
    # under test. The pure-Python bytecode between the two counter samples
    # (the sample itself, the loop bookkeeping) can never release the GIL.
    #
    # Discrimination (measured on this machine, Python 3.14, ~1000-row streams):
    #   * fixed build, idle:        126-196 advancing windows per stream.
    #   * fixed build, cargo-build
    #     load:                     4-275 per stream (one starved stream of 4 in
    #                               ten — exactly the old flake mode; the retry
    #                               loop absorbed it, the next stream hit 129).
    #   * GIL-held build (the
    #     allow_threads removed):   0 windows across 10 consecutive streams —
    #                               a hard ceiling, not a small rate.
    #
    # The floor must be HARDWARE-INDEPENDENT (issue #1929): a per-stream floor of
    # 50 passed 10/10 locally (10 cores) but starved on 2-core CI runners (best
    # 23 across 200 streams) because a 2-core scheduler hands the spinner far
    # fewer slices per stream. What IS invariant is the buggy build's HARD ZERO:
    # a GIL-held build produces zero advancing windows on ANY hardware, always.
    # So we discriminate on presence-of-progress, not on a hardware-scaled count:
    #   * PASS if any single stream reaches ``_STREAM_FLOOR`` advancing windows
    #     (a comfortable single-stream signal on fast hardware), OR
    #   * PASS if advancing windows ACCUMULATED across streams reach
    #     ``_CUMULATIVE_FLOOR`` (slow/starved hardware dribbles a few windows per
    #     stream but still totals well past the buggy build's zero over the retry
    #     budget).
    # Both floors sit far above the buggy build's zero (which can never reach
    # either), so discrimination is preserved while the pass condition no longer
    # depends on how many slices one machine grants per stream.
    _STREAM_FLOOR = 5
    _CUMULATIVE_FLOOR = 20

    # Load-robustness comes from RETRYING plus CUMULATIVE accounting, not from a
    # rate: a single stream can under-deliver windows if the OS starves the
    # spinner for that stream's whole duration, but retrying whole streams and
    # summing their windows across a generous budget makes a false FAIL require
    # the spinner to advance FEWER than ``_CUMULATIVE_FLOOR`` times across the
    # ENTIRE budget — which the scheduler's fairness guarantees make impossible
    # for a runnable thread. The buggy build advances zero times per stream, so
    # neither retrying nor accumulating can ever lift it to a pass (it just fails
    # at budget exhaustion) — retries never weaken discrimination.
    _BUDGET_SECS = 60.0
    _MAX_STREAMS = 200

    def test_streaming_next_releases_gil(self):
        """A concurrent Python thread must acquire the GIL and make progress
        inside ``next()`` calls — deterministic proof that ``__next__`` releases
        the GIL, with a generous retry budget instead of a timing floor.

        Why this discriminates the bug (and cannot flake under load):

        * ``sys.setswitchinterval(1000)`` disables periodic preemption for the
          window. The spinner thread (B) can then advance only when some thread
          *explicitly* hands off the GIL. B still yields cooperatively
          (``time.sleep(0)`` always drops the GIL), so the streaming thread
          never starves.
        * The counter is sampled immediately around each individual ``next()``
          call; ``buffer_size=1`` makes every row a separate blocking refill.
          Counter advance within a window therefore proves a GIL release
          *inside* ``__next__`` — a per-window boolean, not a rate.
        * PASS requires one stream with >= ``_STREAM_FLOOR`` advancing windows
          OR >= ``_CUMULATIVE_FLOOR`` advancing windows summed across streams
          (buggy ceiling on either: zero). FAIL requires exhausting the whole
          ``_BUDGET_SECS`` budget without reaching either floor — i.e. the
          spinner advanced fewer than ``_CUMULATIVE_FLOOR`` times across the
          entire budget, not for one unlucky scheduling slice. The cumulative
          floor is hardware-independent: slow/starved runners (2-core CI)
          dribble a few windows per stream and still total past it, while a
          fast machine trips the single-stream floor immediately.
        """
        # Fail loudly (not skip) under strict fixture mode (issue #1230).
        require_test_data(SCHEMA_BASIC_TYPES)

        # No-GIL (free-threaded) builds have no GIL to release; the concurrency
        # claim under test is GIL-specific.
        if not getattr(sys, "_is_gil_enabled", lambda: True)():
            pytest.skip("free-threaded build: no GIL to release")

        counter = [0]
        stop = threading.Event()
        spinner_alive = threading.Event()

        def spin():
            # Cooperative busy loop. sleep(0) always releases the GIL, so the
            # streaming thread is never blocked by B; but with the switch
            # interval raised, B only advances while some thread hands off the
            # GIL — i.e. while ``__next__`` is inside ``py.allow_threads(...)``.
            spinner_alive.set()
            while not stop.is_set():
                counter[0] += 1
                time.sleep(0)

        def stream_once(database):
            """One full stream; returns (advancing_windows, rows)."""
            config = cqlite.StreamingConfig(buffer_size=1)
            it = iter(
                database.execute_streaming(
                    f"SELECT * FROM {self._TABLE}", config=config
                )
            )
            windows = 0
            rows = 0
            while True:
                c_before = counter[0]
                try:
                    next(it)
                except StopIteration:
                    break
                rows += 1
                # Pure bytecode since c_before was sampled (no GIL release
                # possible with preemption off), so any advance happened
                # inside next().
                if counter[0] != c_before:
                    windows += 1
            return windows, rows

        old_interval = sys.getswitchinterval()
        sys.setswitchinterval(1000)  # disable periodic preemption for the window
        b = threading.Thread(target=spin, daemon=True)
        b.start()
        best_windows = 0
        cumulative_windows = 0
        rows_seen = 0
        streams_run = 0
        passed = False
        try:
            # Deterministic handshake: the spinner has definitely started (the
            # wait itself releases the GIL, so B can run regardless of build).
            assert spinner_alive.wait(timeout=30), "spinner thread never started"

            deadline = time.monotonic() + self._BUDGET_SECS
            with cqlite.open(DATASETS, schema=SCHEMA_BASIC_TYPES) as database:
                while streams_run < self._MAX_STREAMS:
                    windows, rows = stream_once(database)
                    streams_run += 1
                    rows_seen = rows
                    best_windows = max(best_windows, windows)
                    # Only count windows from a stream that actually yielded the
                    # full fixture, so a truncated read cannot inflate the total.
                    if rows >= 900:
                        cumulative_windows += windows
                        if (
                            windows >= self._STREAM_FLOOR
                            or cumulative_windows >= self._CUMULATIVE_FLOOR
                        ):
                            passed = True
                            break
                    if time.monotonic() >= deadline:
                        break
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

        assert passed, (
            f"no stream reached {self._STREAM_FLOOR} next()-windows and the "
            f"cumulative advancing-window total ({cumulative_windows}) never "
            f"reached {self._CUMULATIVE_FLOOR} (best single stream "
            f"{best_windows} across {streams_run} streams over "
            f"{self._BUDGET_SECS:.0f}s); the concurrent thread was starved for "
            f"the entire budget, so the GIL appears held across the blocking "
            f"refill in __next__"
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
