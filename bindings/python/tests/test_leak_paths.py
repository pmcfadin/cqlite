"""Exception-path and abandoned-iterator LEAK BUDGET tests (issue #1465, parent #1436).

Error paths are where leaks hide. When a query raises, or a streaming iterator is
abandoned partway through, the native (Rust) side may have allocated buffers,
channel state, or Python objects that never get freed -- and no test noticed
steady growth across repeated failures. A long-running Python service hitting
errors in a loop would slowly bloat. These two tests put a BUDGET on exactly
those paths.

WHAT IS ASSERTED (and what is deliberately NOT): total tracked-allocation growth
across N iterations must stay under a documented budget. Growth is NEVER asserted
to be zero -- interpreter noise, one-time interned strings, cached constructors
and allocator behaviour all make a zero assertion flaky by construction. Each
loop body is warmed up first so one-time allocations land BEFORE the first
snapshot; what remains is per-iteration behaviour, which is what a real leak
shows up in.

NON-VACUITY IS ASSERTED EXPLICITLY (the most likely defect in a budget test):
a loop body that silently no-ops -- a "bad" CQL string that does not raise, or a
streaming query that yields 0 rows -- would make the budget trivially pass while
testing nothing. So every iteration is counted: the error-path test asserts it
observed exactly one ``cqlite.QueryError`` per iteration, and the stream test
asserts it pulled exactly ``STREAM_ROWS`` rows per iteration. Those counts are
checked OUTSIDE the measurement window so the checks cost the budget nothing.

Dataset rule (issue #1230): the stream test is dataset-dependent, and reuses the
existing ``conftest`` fixtures/guards -- it invents no dataset path. It FAILS
LOUDLY on a present-but-empty/unreadable corpus (via ``require_test_data`` under
strict mode, plus the non-vacuity row count) rather than skipping.

There is deliberately NO wall-clock/elapsed-time assertion anywhere in this file:
these are MEMORY budgets. A timing threshold in a correctness test is a known
flake class (#2642).
"""

import gc
import tracemalloc

import pytest

from conftest import (
    DATASETS,
    SCHEMA_WIDE_ROWS,
    require_test_data,
)

import cqlite

# ---------------------------------------------------------------------------
# Test parameters
# ---------------------------------------------------------------------------

# Widest fixture in the corpus (~101 declared columns, 50 rows), the same table
# the conversion-budget ratchet uses. A wide row means the abandoned stream has
# really built and dropped a non-trivial per-row value graph, so a leak of that
# graph would be visible rather than lost in noise.
WIDE_TABLE = "test_wide_rows.many_columns_table"

# Rejected at query-planning time -> cqlite.QueryError. Chosen deliberately over
# a nonexistent-table SELECT, which returns 0 rows WITHOUT raising (measured
# 2026-08-30) and would make the error-path loop a silent no-op.
BAD_CQL = "THIS IS NOT VALID CQL"

# Iterations of the measured loop (issue: "e.g. 500").
ITERATIONS = 500

# Warm-up iterations run BEFORE the first snapshot so one-time allocations
# (interned strings, cached type objects, first-touch native buffers, the
# streaming machinery's one-time setup) are not counted as growth.
WARMUP_ITERATIONS = 10

# Rows pulled before abandoning the stream. Must be < the fixture's row count
# (50) so the iterator is genuinely abandoned mid-stream, never exhausted.
STREAM_ROWS = 5

# ---------------------------------------------------------------------------
# BUDGETS (issue #1465) -- MEASURED, never guessed. Linux x86_64, Python 3.12,
# maturin develop --profile dev, CQLITE_DATASETS_ROOT=/data/datasets, 500
# iterations + 10 warm-up, 8 consecutive samples per path (2026-08-30):
#
#   error path:  32, 32, 32, 32, 32, 32, 32 bytes warm; 4,340 bytes on the FIRST
#                (cold) sample of a fresh process -> observed max 4,340.
#   stream path: 10,332 .. 15,750 bytes warm; 54,085 bytes on the first (cold)
#                sample -> observed max 54,085.
#
# The cold first sample is what a real pytest run measures (one process, one
# sample per test), so the budget must clear IT, not the warm floor.
#
# Budgets, and what each one BITES -- verified by planting a synthetic
# per-iteration retention INTO THESE EXACT TEST BODIES and observing the
# committed assertions fail (RED control, 2026-08-30):
#   ERROR_BUDGET_BYTES  =  64 KiB (131 bytes/iteration) -- 15x the observed cold
#       max. Planting a retained 256-byte bytearray per iteration measured
#       164,872 bytes (329.7/iteration) and TRIPS it by 2.5x. The real leak shape
#       (retaining the abandoned iterator itself, 5,132 bytes/iteration) measured
#       2.57 MB and trips it by 39x.
#   STREAM_BUDGET_BYTES = 256 KiB (524 bytes/iteration) -- 4.7x the observed cold
#       max. Looser than the error budget because this path's noise is genuinely
#       an order of magnitude larger (a per-iteration stream setup over a
#       ~101-column table). Planting a retained 1 KiB bytearray per iteration
#       measured 635,079 bytes (1,270.2/iteration) and TRIPS it by 2.4x; a
#       planted 256-byte retention (205 KB) does NOT -- stated honestly rather
#       than overclaimed. Every leak shape that retains stream/row state (>= one
#       row of a wide table) trips it by a wide margin.
# ---------------------------------------------------------------------------
ERROR_BUDGET_BYTES = 64 * 1024
STREAM_BUDGET_BYTES = 256 * 1024

# Traces from these files are noise from the measurement/reporting machinery
# rather than from the loop body under test.
_NOISE_FILTERS = (
    tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
    tracemalloc.Filter(False, "<frozen importlib._bootstrap_external>"),
    tracemalloc.Filter(False, "<unknown>"),
    tracemalloc.Filter(False, tracemalloc.__file__),
    tracemalloc.Filter(False, "*/linecache.py"),
)


def _measure_growth_bytes(body, iterations=ITERATIONS, warmup=WARMUP_ITERATIONS):
    """Return total tracked-allocation growth (bytes) over ``iterations`` of ``body``.

    ``body`` is a zero-argument callable executed ``warmup`` times before the
    first snapshot, then ``iterations`` times inside the measurement window. It
    is responsible for its own non-vacuity counting; this helper only measures.
    """
    for _ in range(warmup):
        body()

    gc.collect()
    tracemalloc.start()
    try:
        first = tracemalloc.take_snapshot().filter_traces(_NOISE_FILTERS)
        for _ in range(iterations):
            body()
        gc.collect()
        second = tracemalloc.take_snapshot().filter_traces(_NOISE_FILTERS)
    finally:
        tracemalloc.stop()

    # Net delta across every (file, line) group: sums retained growth and
    # subtracts anything freed, which is the quantity a leak accumulates in.
    return sum(stat.size_diff for stat in second.compare_to(first, "lineno"))


@pytest.fixture(scope="module")
def leak_db():
    """One database, opened once, shared by both leak tests (issue mandate).

    Reuses the existing conftest dataset guard: under strict mode a missing or
    empty corpus FAILS rather than skipping.
    """
    require_test_data(SCHEMA_WIDE_ROWS)
    with cqlite.open(DATASETS, schema=SCHEMA_WIDE_ROWS) as database:
        yield database


def test_error_path_no_leak(leak_db):
    """Repeatedly raising a query error must not grow tracked memory unboundedly."""
    raised = 0

    def body():
        nonlocal raised
        try:
            leak_db.execute(BAD_CQL)
        except cqlite.QueryError:
            raised += 1

    growth = _measure_growth_bytes(body)

    # NON-VACUITY: every single iteration (warm-up included) must have raised.
    # If BAD_CQL ever stops raising, this loop degenerates into a no-op and the
    # budget below would pass while measuring nothing.
    assert raised == ITERATIONS + WARMUP_ITERATIONS, (
        f"error path did not raise on every iteration: {raised} QueryError(s) "
        f"over {ITERATIONS + WARMUP_ITERATIONS} iterations of {BAD_CQL!r} — the "
        "leak budget below would be vacuous (issue #1465)"
    )

    # BOUNDED, not zero (see module docstring).
    assert growth < ERROR_BUDGET_BYTES, (
        f"error-path allocation grew {growth} bytes over {ITERATIONS} raising "
        f"queries ({growth / ITERATIONS:.1f} bytes/iteration), exceeding the "
        f"{ERROR_BUDGET_BYTES}-byte budget — the exception path is likely retaining "
        "allocations per failure (issue #1465)"
    )


def test_abandoned_stream_no_leak(leak_db):
    """Abandoning a streaming iterator mid-stream must not grow memory unboundedly."""
    rows_pulled = 0

    def body():
        nonlocal rows_pulled
        iterator = leak_db.execute_streaming(f"SELECT * FROM {WIDE_TABLE}")
        pulled = 0
        for _row in iterator:
            pulled += 1
            if pulled >= STREAM_ROWS:
                break  # abandoned mid-stream: the iterator is NOT exhausted
        rows_pulled += pulled
        del iterator

    growth = _measure_growth_bytes(body)

    # NON-VACUITY: a 0-row (or short) stream would make the abandonment a no-op.
    # This is also the FAIL-LOUDLY check for a present-but-unreadable corpus —
    # it fails, it never skips.
    expected_rows = STREAM_ROWS * (ITERATIONS + WARMUP_ITERATIONS)
    assert rows_pulled == expected_rows, (
        f"abandoned-stream loop pulled {rows_pulled} rows, expected "
        f"{expected_rows} ({STREAM_ROWS} per iteration): fixture {WIDE_TABLE} is "
        "unreadable, empty, or shorter than STREAM_ROWS — the leak budget below "
        "would be vacuous (issues #1230, #1465)"
    )

    # BOUNDED, not zero (see module docstring).
    assert growth < STREAM_BUDGET_BYTES, (
        f"abandoned-stream allocation grew {growth} bytes over {ITERATIONS} "
        f"abandoned iterators ({growth / ITERATIONS:.1f} bytes/iteration), "
        f"exceeding the {STREAM_BUDGET_BYTES}-byte budget — an abandoned stream "
        "is likely retaining its buffer/channel state (issue #1465)"
    )
