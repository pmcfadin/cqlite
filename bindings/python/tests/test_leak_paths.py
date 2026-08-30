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
  * PROCESS RSS *does* see native allocations, because it is the OS's
    resident-set figure for the whole process. It is read live from
    ``/proc/self/statm`` where that exists -- which covers every MERGE-GATING
    execution of this file, because .github/ci-gating-tiers.yml records
    python-ci.yml as an EXEMPTION whose merge-gating half is the local gate's
    Linux ``python-bindings`` component -- and falls
    back to the monotone peak ``ru_maxrss`` elsewhere, with the failure message
    naming which instrument spoke -- the peak can UNDER-report growth that stays
    below a peak the session already reached, which was measured at small scale
    (see ``_rss_instrument()``), so it is the fallback and not the default. RSS is
    coarse either way (page-granular, allocator-perturbed), so the secondary
    budget on it is deliberately LOOSE: it catches GROSS native retention only.

  Consequence, recorded honestly: a SMALL per-iteration native leak (below the
  RSS budget's measured 16-24 KiB/iteration floor) is invisible to BOTH
  instruments here -- and on a platform with NEITHER RSS instrument (Windows),
  the native half is not measured at all, which the test SAYS rather than
  silently passing over. The proper oracle for that class -- an isolated process, RSS
  measured against a calibrated NATIVE retention control, or native live-resource
  counters -- is issue #3585 and is deliberately not built here.

WHAT IS ASSERTED (and what is deliberately NOT): tracked-allocation growth across
N iterations must stay under a documented budget, and process-RSS growth under a
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
without which "abandoned mid-stream" would silently mean "exhausted", AND that the
query->consumer channel is too small to swallow the fixture (a CONFIGURATION
property -- see ``STREAM_BUFFER_SIZE`` for exactly what that does and does not
establish). Those
checks run OUTSIDE the measurement window so they cost the budget nothing.

Dataset rule (issue #1230): the stream test is dataset-dependent, and reuses the
existing ``conftest`` fixtures/guards -- it invents no dataset path. It FAILS
LOUDLY on a present-but-empty/unreadable corpus (via ``require_test_data`` under
strict mode, plus the non-vacuity row count) rather than skipping.

These tests carry NO pytest marker on purpose. Both gate tiers would drop a
``slow``-marked test from the merge-gating set, by different mechanisms:
the FULL gate's ``python-bindings`` component runs
``pytest bindings/python/tests -q`` under ``RUN_SLOW_TESTS="${RUN_SLOW_TESTS:-0}"``
(``run_python_bindings`` in scripts/agent-gate.sh), which
``conftest.pytest_collection_modifyitems`` turns into a skip for ``slow`` items;
``--lite``'s python tier runs ``pytest bindings/python/tests -m 'not slow' -q``
(``PYTHON_LITE_PYTEST_CMD`` in scripts/agent-gate.sh) under ``RUN_SLOW_TESTS=0``,
which deselects them.
Unmarked, they execute in both. Measured runtime of this whole file: ~6s.

There is deliberately NO wall-clock/elapsed-time assertion anywhere in this file:
these are MEMORY budgets. A timing threshold in a correctness test is a known
flake class (#2642).
"""

import gc
import mmap
import sys
import tracemalloc
import warnings

import pytest

# ``resource`` is POSIX-only. python-ci.yml runs this whole directory on a
# ``windows-latest`` matrix leg (`pytest bindings/python/tests/ -v --tb=short -m
# "not slow"`, its "Run pytest (non-slow)" step), where a module-level Unix-only
# import is a COLLECTION ERROR that reds the job. Import it conditionally so the
# tracemalloc budgets -- pure stdlib and platform-independent -- still run
# everywhere, and degrade ONLY the RSS backstop where the instrument is missing.
try:
    import resource
except ImportError:  # pragma: no cover - Windows only
    resource = None

from conftest import (
    DATASETS,
    SCHEMA_WIDE_ROWS,
    require_test_data,
)

import cqlite

# ---------------------------------------------------------------------------
# Test parameters
# ---------------------------------------------------------------------------

# Widest fixture in the corpus: 101 declared columns (id + col_001..col_100, per
# test-data/schemas/wide-rows.cql) and 50 rows on disk (both counted, not
# estimated), the same table the conversion-budget ratchet uses. A wide row means the abandoned stream has
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
# runtime of this file at 1500: ~6s, so it stays UNMARKED and therefore executes
# in BOTH gate tiers (mechanisms quoted in the module docstring).
ITERATIONS = 1500

# Warm-up iterations run BEFORE the first snapshot so one-time allocations
# (interned strings, cached type objects, first-touch native buffers, the
# streaming machinery's one-time setup) are not counted as growth.
WARMUP_ITERATIONS = 10

# Rows pulled before abandoning the stream. Must be < the fixture's row count
# (50 on disk) so the iterator is genuinely abandoned mid-stream, never exhausted.
STREAM_ROWS = 5

# THE ABANDONMENT MUST LEAVE THE QUERY PRODUCER IN FLIGHT (issue #1465 rounds 5-6).
# Scope this claim carefully -- the first version of it overreached, and the
# corrected version is narrower than it looks.
#
# WHAT `buffer_size` IS: the capacity of the OUTER, query->consumer channel
# (`mpsc::channel(config.buffer_size)` in cqlite-core's select_executor). The query
# task builds rows from a scan batch and pushes them one at a time into that
# channel with `tx.send(..).await`, so with capacity 2 it is PARKED in that send
# after a handful of rows, still owning every remaining row it has built. With the
# DEFAULT 1024 -- larger than this fixture's 50 rows -- it sends all 50 and
# RETURNS, so there is nothing outstanding anywhere and the budget below would
# measure a fully drained pipeline. That difference is the whole point of pinning
# the capacity here.
#
# WHAT THIS DOES *NOT* ESTABLISH, verified in core rather than assumed: the
# reader-level scan may ALREADY BE COMPLETE at the break. Downstream of that outer
# channel the reader's batching subsystem holds its own pool, documented as
# INDEPENDENT of `buffer_size`:
# `MAX_INFLIGHT_BATCH_ROWS = (BATCH_CHANNEL_CAP + 2) * BATCH_EMIT_ROWS = (2 + 2) *
# 256 = 1024` rows (cqlite-core's reader/scan_stream_windowed), and the batched
# surface's channel holds `ceil(buffer_size / BATCH_EMIT_ROWS)` = ONE batch of up
# to 256 rows (`batched_channel_capacity` in reader/data_access/batched_scan_stream).
# A 50-row fixture fits entirely inside one batch, so the native scan can finish
# before the consumer's first pull no matter what `buffer_size` is. The property
# pinned here is therefore OUTSTANDING WORK AT THE QUERY->CONSUMER BOUNDARY, not
# "the native scan was still running".
#
# AND THE ASSERTION BELOW IS A CONFIGURATION PROPERTY, NOT A RUNTIME ONE. It is
# computed from a configured constant and one measured row count, so it proves the
# capacity was not raised above the fixture size -- the regression that silently
# drained the pipeline. Only an OBSERVATION could prove what was in flight at the
# instant of the break, and python exposes no such observation:
# `StreamingIterator.rows_received` is a CONSUMER-side count (measured
# [1, 2, 3, 4, 5] for every buffer_size from 1 to 1024). The NODE lane does carry
# the runtime property, because `stream.rowsReceived` there is the NATIVE
# iterator's fetch counter and that lane MEASURES it at the break (and reads 0
# after close, which is its closure proof). Do not "port" those assertions here;
# there is no signal behind them in this binding.
STREAM_BUFFER_SIZE = 2

# ---------------------------------------------------------------------------
# BUDGETS (issue #1465) -- MEASURED, never guessed. Linux x86_64, Python 3.12,
# maturin develop --profile dev, CQLITE_DATASETS_ROOT=/data/datasets, 10 warm-up
# iterations, 8 consecutive samples per path (2026-08-30, measured at 500
# iterations; the budgets below are UNCHANGED at 1500, which is what makes the
# per-iteration sensitivity 3x better):
#
#   error path:  32 bytes warm (x7); 4,340 bytes on the FIRST (cold) sample of a
#                fresh process -> observed max 4,340.
#   stream path: 16,631 .. 19,623 bytes warm; 67,841 bytes on the first (cold)
#                sample -> observed max 67,841. RE-MEASURED for round 5's
#                `buffer_size=2` (it was 10,332 .. 54,085 with the default 1024-row
#                channel: a smaller channel means slightly MORE per-iteration
#                bookkeeping, so this path got noisier, not quieter).
#
# The cold first sample is what a real pytest run measures (one measured window per
# test in one process), so the budget must clear IT, not the warm floor.
#
# ONE COLD SAMPLE, NOT A MULTI-PASS STATISTIC -- unlike the Node lane, deliberately
# and for a measured reason: tracemalloc counts allocations exactly rather than
# sampling a GC'd heap, so the spread here is ~4x (16.6-67.8 KB) where Node's was
# ~800x, and a 9-pass statistic would cost ~50s in a lane that currently costs 6s.
# If this file ever needs passes, the runtime is why it does not have them today.
#
# NO MIN/MEDIAN PAIR either, and for a structural reason rather than an oversight
# (issue #1465 round 5 class sweep): the Node lane asserts BOTH the minimum and the
# median of 9 passes because a single most-favourable sample could otherwise carry
# the verdict. This lane takes ONE sample and it is the PESSIMISTIC one (the cold
# first window of a fresh process), so there is no favourable sample to hide behind
# and nothing for a median to add.
#
# NO THREE-STATE INSTRUMENT GUARD ON THE NODE SIDE, symmetrically: this file's RSS
# reader can be absent (Windows) or degrade (no /proc), which is why it is
# three-valued and why a mid-window failure raises a NAMED error; Node's
# `process.memoryUsage().rss` is always present and cannot fail, so its lane needs
# neither. Same property, different exposure.
#
# NO CI BUDGET MULTIPLIER either, unlike the Node lane: the tracemalloc budgets do
# not depend on GC timing at all, and the one platform-sensitive number (the RSS
# backstop) already carries ~34x headroom and degrades to a NAMED weaker instrument
# where /proc is missing. Adding a multiplier would weaken the merge-gating lane
# (the gate's python tier IS the merge-gating half) to buy nothing measurable.
#
# Budgets, and what each one BITES -- verified by planting a synthetic
# per-iteration retention INTO THESE EXACT TEST BODIES and observing the committed
# assertions fail (RED control, re-run at `buffer_size=2` and 1500 iterations;
# round-4's numbers do not transfer and were re-measured, not copied). NOTE WHAT
# THE PYTHON-OBJECT CONTROLS ESTABLISH: the planted objects are PYTHON allocations
# (``bytearray``), so they prove sensitivity to PYTHON-VISIBLE retention on these
# code paths. Sensitivity to a NATIVE leak is established separately, by the
# ``libc.malloc`` control on the RSS backstop below.
#   ERROR_BUDGET_BYTES  =  64 KiB (43 bytes/iteration at 1500) -- 15x the observed
#       cold max. A retained 256-byte bytearray per iteration measured 486,320
#       bytes (324.2/iteration) and TRIPS it by 7.4x. Retaining the abandoned
#       iterator itself (5,132 bytes/iteration) measured 2.57 MB.
#   STREAM_BUDGET_BYTES = 256 KiB (175 bytes/iteration at 1500) -- 3.9x the observed
#       cold max (was 4.7x before the re-measure; UNCHANGED budget, so this is a
#       tightening of the margin by measurement, never a loosening to fit).
#       Measured floor, bracketed: a retained 64-byte bytearray per iteration
#       measured 286,015 bytes (190.7/iteration) and TRIPS; 32 bytes/iteration
#       PASSES. So this path now catches a Python-visible retention somewhere
#       between 32 and 64 bytes/iteration -- an order of magnitude better than
#       round 3's ">512 B/iter", won by raising ITERATIONS to 1500 rather than by
#       touching the budget. For the record at larger sizes: 128 B/iter ->
#       372,849 (1.4x over), 256 B/iter -> 572,002 (2.2x), 1 KiB/iter ->
#       1,701,339 (6.5x).
#
# SECONDARY, LOOSE, NATIVE-VISIBLE BUDGET (issue #1465 review): process RSS growth
# across the same loop, read LIVE from /proc/self/statm (peak ``ru_maxrss`` only as
# a named fallback -- see ``_rss_instrument()``). Measured growth of the LIVE
# instrument inside the measured window, 1500 iterations:
#   file alone:            0 bytes (error path), 4,096 .. 970,752 bytes (stream
#                          path, 6 consecutive samples at `buffer_size=2`; the
#                          maximum is the first, cold sample)
#   inside the whole 570-test suite (this file's real position in a gate run):
#                          0 bytes (error path),  28,672 bytes (stream path)
# Budget = 32 MiB, i.e. ~34x the largest observed value, so allocator/page
# behaviour on a slower or smaller CI runner cannot red it.
#   WHAT IT CATCHES, and this control is the one that validates the RIGHT
#       allocator (issue #1465 round 2): planting a retained ``libc.malloc`` +
#       ``memset`` buffer per iteration -- a genuine NATIVE-heap allocation that
#       tracemalloc cannot see at all -- reds ONLY this assertion, exactly as the
#       docstring's blind-spot claim predicts:
#         * 64 KiB/iteration -> live RSS grew 98,750,464 B (error path) /
#           99,332,096 B (stream path): TRIPS, ~3x over budget, while BOTH
#           tracemalloc budgets stayed green.
#         * 24 KiB/iteration -> 37,314,560 B: TRIPS (the floor, measured; re-run
#           at `buffer_size=2` measured 37,294,080 B -- same floor).
#         * 16 KiB/iteration -> PASSES (both configurations).
#       So the detection floor is between 16 and 24 KiB/iteration at 1500
#       iterations, bracketing the ~22 KiB the budget arithmetic predicts.
#   WHAT IT DOES NOT CATCH: any native retention below that floor -- e.g.
#       512 B/iteration, which is 2.8 MB per 5,000 error responses in a real
#       service. It is a backstop for the gross case, not an oracle; the oracle is
#       issue #3585. On the degraded PEAK fallback (no /proc) it can additionally
#       under-report growth that stays below an earlier session peak, and it can
#       never report a decrease.
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


def _maxrss_bytes():
    """PEAK resident-set size of this process, in BYTES.

    ``ru_maxrss`` is KILOBYTES on Linux and BYTES on macOS/BSD -- normalising
    here rather than at each call site keeps the budget one number on every
    platform.

    Returns ``None`` where the POSIX ``resource`` module does not exist (Windows).

    This is a MONOTONE HIGH-WATER MARK, which is why it is only the FALLBACK
    instrument: growth that stays below a peak the process already reached
    earlier in the pytest session is invisible to it. That masking is measured,
    not hypothetical -- see ``_rss_instrument()``.
    """
    if resource is None:
        return None
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024


def _live_rss_bytes():
    """CURRENT resident-set size in BYTES, or ``None`` where unavailable.

    Reads field 2 (resident pages) of ``/proc/self/statm``. Unlike ``ru_maxrss``
    this is a live figure, so a delta across the measured loop cannot be masked
    by an earlier session peak. Returns ``None`` off Linux (no ``/proc``), which
    is what puts this file on the documented fallback path there.
    """
    try:
        with open("/proc/self/statm", "rb") as handle:
            resident_pages = int(handle.read().split()[1])
    except (OSError, IndexError, ValueError):
        return None
    return resident_pages * mmap.PAGESIZE


def _rss_instrument():
    """Return ``(reader, kind)`` for the native-visible backstop.

    ``kind`` is one of THREE values, deliberately -- a two-valued answer would
    have to fold "cannot measure" onto one of the measuring answers, and folding
    it onto the permissive side is the silent-pass shape this file exists to
    avoid:
      * ``"live"``        -- a true RSS delta from ``/proc/self/statm``. Preferred,
                             and what every merge-gating (Linux) lane uses.
      * ``"peak"``        -- ``ru_maxrss``. Degraded fallback where ``/proc`` is
                             absent but POSIX ``resource`` exists (macOS/BSD).
      * ``"unavailable"`` -- neither instrument exists (Windows). ``reader`` is
                             ``None``; the caller must then state that the native
                             half was NOT measured rather than pass silently.

    WHY THIS MATTERS, measured on this branch (2026-08-30, probing the real
    pytest process at the moment these tests run):
      * file alone:        peak == live to within 1 MiB, so the peak instrument
                           was live, not masked.
      * inside the full 570-test suite run (this file's real position in a gate
        run):              peak was ~1.5 MiB ABOVE live, and the stream loop's
                           28,672-byte live growth registered as a peak delta of
                           ZERO -- small-scale masking, demonstrated.
    So the peak instrument is not inert in today's ordering (a 32 MiB leak would
    still push a new peak), but the headroom it depends on is a property of TEST
    ORDERING that nobody controls: one memory-hungry test placed before this file
    could open a multi-hundred-MiB gap and silently neuter the backstop. The live
    reader removes that dependency entirely, so it is preferred wherever it
    exists and the fallback names itself in the failure message.
    """
    if _live_rss_bytes() is not None:
        return _live_rss_bytes, "live"
    if _maxrss_bytes() is not None:
        return _maxrss_bytes, "peak"
    return None, "unavailable"


def _measure_growth_bytes(body, iterations=ITERATIONS, warmup=WARMUP_ITERATIONS):
    """Measure ``iterations`` of ``body``; return ``(tracked, rss_growth, rss_kind)``.

    ``tracked`` is Python-allocator growth (tracemalloc). ``rss_growth`` is
    process RSS growth in bytes -- the loose, native-visible backstop -- measured
    with the best instrument this platform offers, named by ``rss_kind``
    (``"live"``, the degraded ``"peak"``, or ``"unavailable"`` -- in which case
    ``rss_growth`` is ``None``; see ``_rss_instrument()``).

    ``body`` is a zero-argument callable executed ``warmup`` times before the
    first snapshot, then ``iterations`` times inside the measurement window. It
    is responsible for its own non-vacuity counting; this helper only measures.
    """
    for _ in range(warmup):
        body()

    gc.collect()
    rss_reader, rss_kind = _rss_instrument()
    rss_before = rss_reader() if rss_reader is not None else None
    tracemalloc.start()
    try:
        first = tracemalloc.take_snapshot().filter_traces(_NOISE_FILTERS)
        for _ in range(iterations):
            body()
        gc.collect()
        second = tracemalloc.take_snapshot().filter_traces(_NOISE_FILTERS)
    finally:
        tracemalloc.stop()
    # G6 (issue #1465 round 4): the instrument was readable at probe time, but a
    # mid-window failure is still possible (a /proc read denied by a sandbox, a
    # container remount). A bare `rss_reader() - rss_before` would then raise
    # `TypeError: unsupported operand type(s) for -: 'NoneType' and 'int'` -- loud,
    # but naming neither the instrument nor when it failed. Fail loudly AND
    # legibly instead.
    rss_growth = None
    if rss_reader is not None:
        rss_after = rss_reader()
        if rss_after is None or rss_before is None:
            which = "at the START of" if rss_before is None else "at the END of"
            raise RuntimeError(
                f"the {rss_kind!r} RSS instrument failed {which} the measured "
                f"window (before={rss_before}, after={rss_after}). It answered at "
                "probe time, so this is a mid-run failure of the reader itself "
                "(e.g. /proc/self/statm became unreadable), not an unsupported "
                "platform -- which the three-valued _rss_instrument() reports "
                "instead. No RSS verdict can be given for this run (issue #1465)."
            )
        rss_growth = rss_after - rss_before

    # Net delta across every (file, line) group: sums retained growth and
    # subtracts anything freed, which is the quantity a leak accumulates in.
    tracked = sum(stat.size_diff for stat in second.compare_to(first, "lineno"))
    return tracked, rss_growth, rss_kind


def _assert_rss_under_budget(label: str, rss_growth, rss_kind: str) -> None:
    """Loose, native-visible backstop: RSS growth over the measured loop.

    When no RSS instrument exists (Windows) this STATES that the native half was
    not measured, via a ``warnings.warn`` that pytest surfaces in its default
    warnings summary, and asserts nothing about it. It never reports a
    native-visible verdict it did not measure. The tracemalloc budget in the
    caller has already run and gated on every platform.
    """
    if rss_kind == "unavailable":
        warnings.warn(
            f"{label}: the native-visible RSS backstop did NOT run on this "
            "platform (no POSIX `resource` module and no /proc/self/statm). The "
            "tracemalloc budget DID run and gated this test; the native half is "
            "UNMEASURED here, so this test's verdict covers Python-visible "
            "retention only (issues #1465, #3585).",
            stacklevel=2,
        )
        return
    degraded = (
        ""
        if rss_kind == "live"
        else " NOTE: measured with the degraded PEAK instrument (no /proc on this "
        "platform), which can under-report -- never over-report -- growth"
    )
    assert rss_growth < RSS_BUDGET_BYTES, (
        f"{label}: {rss_kind} RSS grew {rss_growth} bytes over {ITERATIONS} "
        f"iterations ({rss_growth / ITERATIONS:.1f} bytes/iteration), exceeding "
        f"the loose {RSS_BUDGET_BYTES}-byte native-visible budget. Unlike the "
        "tracemalloc budget this one SEES Rust-side allocations, so a trip here "
        f"points at gross native retention on this path (issue #1465).{degraded}"
    )


def _stream(db):
    """Open the abandoned-stream query with the capacity-bounded config.

    One helper, used by BOTH the contract test and the measured loop, so the two
    can never drift apart on the property the contract test pins.
    """
    return db.execute_streaming(
        f"SELECT * FROM {WIDE_TABLE}",
        config=cqlite.StreamingConfig(buffer_size=STREAM_BUFFER_SIZE),
    )


@pytest.fixture(scope="module")
def leak_db():
    """One database, opened once, shared by every test in this file (issue mandate).

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

    growth, rss_growth, rss_kind = _measure_growth_bytes(body)

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
    _assert_rss_under_budget("error path", rss_growth, rss_kind)


def test_abandoned_stream_is_really_abandoned(leak_db):
    """Contract pin: the fixture must hold MORE rows than the loop pulls.

    Without this, a fixture holding exactly ``STREAM_ROWS`` rows would EXHAUST
    instead of abandoning, every count in the budget test below would still line
    up, and the test would pass having measured the wrong path (issue #1465
    review). Mirrors the Node lane's ``expect(total).toBeGreaterThan(STREAM_ROWS)``.
    """
    total = sum(1 for _row in _stream(leak_db))
    assert total > STREAM_ROWS, (
        f"fixture {WIDE_TABLE} yielded {total} rows, which is not MORE than "
        f"STREAM_ROWS={STREAM_ROWS}: the budget test below would exhaust the "
        "iterator instead of abandoning it mid-stream (issues #1230, #1465)"
    )

    # THE OUTER-CHANNEL BOUND (issue #1465 rounds 5-6). A CONFIGURATION property:
    # every term is a configured constant or the measured row count, so what it
    # proves is that the query->consumer channel cannot swallow the whole fixture
    # -- i.e. the query task is still parked in `tx.send` holding the remainder
    # when the loop breaks. It does NOT prove what the reader-level scan was doing;
    # see STREAM_BUFFER_SIZE for why (MAX_INFLIGHT_BATCH_ROWS = 1024, independent
    # of buffer_size).
    channel_ceiling = STREAM_ROWS + STREAM_BUFFER_SIZE
    assert channel_ceiling < total, (
        f"the query->consumer channel can hold at most {channel_ceiling} of "
        f"{total} rows before the break (STREAM_ROWS={STREAM_ROWS} consumed + "
        f"buffer_size={STREAM_BUFFER_SIZE} of capacity), which is not below the "
        f"fixture's {total} rows: raise the fixture or lower buffer_size, or the "
        "query task drains into the channel and returns, leaving nothing "
        "outstanding to abandon (issue #1465)"
    )

    # And the consumer really did stop at STREAM_ROWS: `rows_received` is
    # python's CONSUMER-side counter (see STREAM_BUFFER_SIZE), so this pins the
    # abandonment point itself, not the producer.
    iterator = _stream(leak_db)
    pulled = 0
    for _row in iterator:
        pulled += 1
        if pulled >= STREAM_ROWS:
            break
    assert pulled == STREAM_ROWS
    assert iterator.rows_received == STREAM_ROWS, (
        f"consumer-side rows_received={iterator.rows_received} at the break, "
        f"expected STREAM_ROWS={STREAM_ROWS} — the abandonment point is not "
        "where this file thinks it is (issue #1465)"
    )
    del iterator


def test_abandoned_stream_no_leak(leak_db):
    """Abandoning a streaming iterator mid-stream must not grow memory unboundedly."""
    rows_pulled = 0

    def body():
        nonlocal rows_pulled
        iterator = _stream(leak_db)
        pulled = 0
        for _row in iterator:
            pulled += 1
            if pulled >= STREAM_ROWS:
                break  # abandoned mid-stream: the iterator is NOT exhausted
        rows_pulled += pulled
        del iterator

    growth, rss_growth, rss_kind = _measure_growth_bytes(body)

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
    _assert_rss_under_budget("abandoned stream", rss_growth, rss_kind)
