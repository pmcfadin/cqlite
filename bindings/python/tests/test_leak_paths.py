"""Exception-path and abandoned-iterator LEAK BUDGET tests (issue #1465, parent #1436).

Error paths are where leaks hide. When a query raises, or a streaming iterator is
abandoned partway through, buffers / channel state / Python objects may never get
freed -- and no test noticed steady growth across repeated failures. A
long-running Python service hitting errors in a loop would slowly bloat. These
two tests put budgets on exactly those paths.

WHAT EACH INSTRUMENT CAN AND CANNOT SEE (issue #1465 review, stated up front
because the earlier version of this docstring overclaimed):

  * ``tracemalloc`` traces ONLY allocations made through the PYTHON allocator.
    That covers every Python object the binding hands back -- rows, cells, error
    objects, iterator state -- but it is BLIND to an ordinary Rust-side
    allocation. A leaked ``Vec<u8>``, a retained streaming channel or an
    un-dropped reader buffer on the native side stays completely FLAT in
    tracemalloc while process memory climbs. So the primary budget below bounds
    the PYTHON-VISIBLE half of these paths, which is a real and previously
    unguarded half -- not the whole leak surface.
  * ``resource.getrusage().ru_maxrss`` DOES see native allocations, because it is
    the OS's peak resident-set figure for the whole process. It is coarse
    (page-granular, monotone, and perturbed by the allocator), so the secondary
    budget on it is deliberately LOOSE: it catches GROSS native retention only.

  Consequence, recorded honestly: a SMALL per-iteration native leak (below the
  ru_maxrss budget's ~22 KiB/iteration resolution) is invisible to BOTH
  instruments here. The proper oracle for that class -- an isolated process, RSS
  measured against a calibrated NATIVE retention control, or native live-resource
  counters -- is issue #3585 and is deliberately not built here.

WHAT IS ASSERTED (and what is deliberately NOT): tracked-allocation growth across
N iterations must stay under a documented budget, and peak-RSS growth under a
much looser one. Growth is NEVER asserted to be zero -- interpreter noise,
one-time interned strings, cached constructors and allocator behaviour all make a
zero assertion flaky by construction. Each loop body is warmed up first so
one-time allocations land BEFORE the first snapshot; what remains is
per-iteration behaviour, which is what a real leak shows up in.

NON-VACUITY IS ASSERTED EXPLICITLY (the most likely defect in a budget test):
a loop body that silently no-ops -- a "bad" CQL string that does not raise, or a
streaming query that yields 0 rows -- would make the budget trivially pass while
testing nothing. So every iteration is counted: the error-path test asserts it
observed exactly one ``cqlite.QueryError`` per iteration, and the stream test
asserts it pulled exactly ``STREAM_ROWS`` rows per iteration AND (separately,
outside the measurement) that the fixture holds MORE than ``STREAM_ROWS`` rows,
without which "abandoned mid-stream" would silently mean "exhausted". Those
checks run OUTSIDE the measurement window so they cost the budget nothing.

Dataset rule (issue #1230): the stream test is dataset-dependent, and reuses the
existing ``conftest`` fixtures/guards -- it invents no dataset path. It FAILS
LOUDLY on a present-but-empty/unreadable corpus (via ``require_test_data`` under
strict mode, plus the non-vacuity row count) rather than skipping.

These tests carry NO pytest marker on purpose: the gate's python tier runs
``pytest bindings/python/tests -m 'not slow' -q``, so a ``slow`` marker would
silently remove them from the merge-gating set (issue #1465 review).

There is deliberately NO wall-clock/elapsed-time assertion anywhere in this file:
these are MEMORY budgets. A timing threshold in a correctness test is a known
flake class (#2642).
"""

import gc
import resource
import sys
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

# Iterations of the measured loop. Raised from the issue's suggested 500 to 1500
# (issue #1465 review): the dominant noise term is the ONE-TIME cold cost (~54 KB
# on the stream path), so tripling the iterations at an unchanged byte budget
# triples per-iteration sensitivity for ~4s of extra runtime. Measured total
# runtime of this file at 1500: ~6s, so it stays UNMARKED and therefore inside
# the gate's `-m 'not slow'` python tier.
ITERATIONS = 1500

# Warm-up iterations run BEFORE the first snapshot so one-time allocations
# (interned strings, cached type objects, first-touch native buffers, the
# streaming machinery's one-time setup) are not counted as growth.
WARMUP_ITERATIONS = 10

# Rows pulled before abandoning the stream. Must be < the fixture's row count
# (50) so the iterator is genuinely abandoned mid-stream, never exhausted.
STREAM_ROWS = 5

# ---------------------------------------------------------------------------
# BUDGETS (issue #1465) -- MEASURED, never guessed. Linux x86_64, Python 3.12,
# maturin develop --profile dev, CQLITE_DATASETS_ROOT=/data/datasets, 10 warm-up
# iterations, 8 consecutive samples per path (2026-08-30, measured at 500
# iterations; the budgets below are UNCHANGED at 1500, which is what makes the
# per-iteration sensitivity 3x better):
#
#   error path:  32 bytes warm (x7); 4,340 bytes on the FIRST (cold) sample of a
#                fresh process -> observed max 4,340.
#   stream path: 10,332 .. 15,750 bytes warm; 54,085 bytes on the first (cold)
#                sample -> observed max 54,085.
#
# The cold first sample is what a real pytest run measures (one process, one
# sample per test), so the budget must clear IT, not the warm floor.
#
# Budgets, and what each one BITES -- verified by planting a synthetic
# per-iteration retention INTO THESE EXACT TEST BODIES and observing the
# committed assertions fail (RED control, 2026-08-30, at 500 iterations). NOTE
# WHAT THE CONTROL ESTABLISHES: the planted objects are PYTHON allocations
# (``bytearray``), so it proves the instrument is sensitive to PYTHON-VISIBLE
# retention on these code paths. It establishes NOTHING about sensitivity to a
# native (Rust-allocator) leak, which tracemalloc cannot see at all -- see the
# module docstring and issue #3585.
#   ERROR_BUDGET_BYTES  =  64 KiB (43 bytes/iteration at 1500) -- 15x the observed
#       cold max. Planting a retained 256-byte bytearray per iteration measured
#       164,872 bytes (329.7/iteration) and TRIPS it by 2.5x. Retaining the
#       abandoned iterator itself (5,132 bytes/iteration) measured 2.57 MB.
#   STREAM_BUDGET_BYTES = 256 KiB (175 bytes/iteration at 1500) -- 4.7x the
#       observed cold max. Looser than the error budget because this path's noise
#       is genuinely an order of magnitude larger (a per-iteration stream setup
#       over a ~101-column table). Planting a retained 1 KiB bytearray per
#       iteration measured 635,079 bytes (1,270.2/iteration) and TRIPS it by 2.4x;
#       a planted 256-byte retention (205 KB) does NOT -- stated honestly rather
#       than overclaimed.
#
# SECONDARY, LOOSE, NATIVE-VISIBLE BUDGET (issue #1465 review): peak RSS growth
# (``ru_maxrss``) across the same loop. Measured growth on this machine: 0 bytes
# (error path, both 500 and 1500 iterations) and 0..131,072 bytes (stream path).
# Budget = 32 MiB, i.e. ~250x the largest observed value, so allocator/page
# behaviour on a slower or smaller CI runner cannot red it.
#   WHAT IT CATCHES: gross native retention -- at 1500 iterations it trips on
#       roughly >= 22 KiB/iteration held on the native heap (e.g. an un-dropped
#       per-stream row buffer over a ~101-column table).
#   WHAT IT DOES NOT CATCH: anything smaller than that, and -- because
#       ``ru_maxrss`` is a monotone PEAK -- growth that stays below a peak the
#       process already reached earlier in the pytest session. It can never
#       report a decrease. It is a backstop for the gross case, not an oracle.
# ---------------------------------------------------------------------------
ERROR_BUDGET_BYTES = 64 * 1024
STREAM_BUDGET_BYTES = 256 * 1024
RSS_BUDGET_BYTES = 32 * 1024 * 1024

# Traces from these files are noise from the measurement/reporting machinery
# rather than from the loop body under test.
_NOISE_FILTERS = (
    tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
    tracemalloc.Filter(False, "<frozen importlib._bootstrap_external>"),
    tracemalloc.Filter(False, "<unknown>"),
    tracemalloc.Filter(False, tracemalloc.__file__),
    tracemalloc.Filter(False, "*/linecache.py"),
)


def _maxrss_bytes() -> int:
    """Peak resident-set size of this process, in BYTES.

    ``ru_maxrss`` is KILOBYTES on Linux and BYTES on macOS/BSD -- normalising
    here rather than at each call site keeps the budget one number on every
    platform. This is the only instrument in this file that can see a NATIVE
    (Rust-allocator) allocation at all; see the module docstring for its limits.
    """
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024


def _measure_growth_bytes(body, iterations=ITERATIONS, warmup=WARMUP_ITERATIONS):
    """Measure ``iterations`` of ``body`` and return ``(tracked, peak_rss)`` growth.

    ``tracked`` is Python-allocator growth (tracemalloc), ``peak_rss`` is growth
    of the process's peak RSS in bytes -- the loose, native-visible backstop.

    ``body`` is a zero-argument callable executed ``warmup`` times before the
    first snapshot, then ``iterations`` times inside the measurement window. It
    is responsible for its own non-vacuity counting; this helper only measures.
    """
    for _ in range(warmup):
        body()

    gc.collect()
    rss_before = _maxrss_bytes()
    tracemalloc.start()
    try:
        first = tracemalloc.take_snapshot().filter_traces(_NOISE_FILTERS)
        for _ in range(iterations):
            body()
        gc.collect()
        second = tracemalloc.take_snapshot().filter_traces(_NOISE_FILTERS)
    finally:
        tracemalloc.stop()
    rss_growth = _maxrss_bytes() - rss_before

    # Net delta across every (file, line) group: sums retained growth and
    # subtracts anything freed, which is the quantity a leak accumulates in.
    tracked = sum(stat.size_diff for stat in second.compare_to(first, "lineno"))
    return tracked, rss_growth


def _assert_rss_under_budget(label: str, rss_growth: int) -> None:
    """Loose, native-visible backstop: peak-RSS growth over the measured loop."""
    assert rss_growth < RSS_BUDGET_BYTES, (
        f"{label}: peak RSS grew {rss_growth} bytes over {ITERATIONS} iterations "
        f"({rss_growth / ITERATIONS:.1f} bytes/iteration), exceeding the loose "
        f"{RSS_BUDGET_BYTES}-byte native-visible budget. Unlike the tracemalloc "
        "budget this one SEES Rust-side allocations, so a trip here points at "
        "gross native retention on this path (issue #1465)"
    )


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

    growth, rss_growth = _measure_growth_bytes(body)

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
    _assert_rss_under_budget("error path", rss_growth)


def test_abandoned_stream_is_really_abandoned(leak_db):
    """Contract pin: the fixture must hold MORE rows than the loop pulls.

    Without this, a fixture holding exactly ``STREAM_ROWS`` rows would EXHAUST
    instead of abandoning, every count in the budget test below would still line
    up, and the test would pass having measured the wrong path (issue #1465
    review). Mirrors the Node lane's ``expect(total).toBeGreaterThan(STREAM_ROWS)``.
    """
    total = sum(1 for _row in leak_db.execute_streaming(f"SELECT * FROM {WIDE_TABLE}"))
    assert total > STREAM_ROWS, (
        f"fixture {WIDE_TABLE} yielded {total} rows, which is not MORE than "
        f"STREAM_ROWS={STREAM_ROWS}: the budget test below would exhaust the "
        "iterator instead of abandoning it mid-stream (issues #1230, #1465)"
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

    growth, rss_growth = _measure_growth_bytes(body)

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
    _assert_rss_under_budget("abandoned stream", rss_growth)
