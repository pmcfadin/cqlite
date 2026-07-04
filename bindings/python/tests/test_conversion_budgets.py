"""Per-row allocation BUDGET tests for the Python binding conversion path.

Issue #1449 (parent #1433). The per-row conversion wins landed in this epic —
#1445 (interned/ordered keys), #1446 (ordered rows + one-time column-name
interning), #1447 (clone -> move), #1448 (Set/Map constructor caching) — reduce
allocations per converted row. Nothing pinned those wins, so a future refactor
could silently re-introduce O(rows x columns) allocation without any test
noticing. This module makes the allocation win a RATCHET: it materializes a wide
result and asserts the per-row allocation peak stays under a MEASURED budget.

This reuses the existing `tracemalloc` machinery already used by
``test_performance.py`` — it does NOT stand up a second measurement harness.

Dataset rule (issue #1230): the budget query is dataset-dependent. It asserts
``len(result) > 0`` and FAILS LOUDLY on a present-but-empty/unreadable dataset
rather than skipping, so a dropped fixture reds CI instead of false-greening.
"""

import tracemalloc

import pytest

from conftest import (
    DATASETS,
    SCHEMA_WIDE_ROWS,
    require_test_data,
    skip_if_no_schema,
)

import cqlite

# ---------------------------------------------------------------------------
# Wide table chosen deliberately: test_wide_rows.many_columns_table has ~101
# columns, so it exercises the per-column conversion cost that the W1-W4 wins
# target. A regression to O(rows x columns) allocation blows the per-row peak
# on this table faster than any narrow table would.
# ---------------------------------------------------------------------------
WIDE_TABLE = "test_wide_rows.many_columns_table"
ROW_LIMIT = 200

# MEASURED BASELINE (2026-07-04, macOS arm64, maturin --profile dev):
#   MEASUREMENT WINDOW (issue #1449, roborev fix): the tracemalloc window now
#   wraps BOTH `db.execute(...)` AND the `[dict(r) for r in result.rows]`
#   materialization. Python's conversion of every row/cell to a PyObject is
#   EAGER inside `execute()` (see result.rs `Row::from_core` / eager materialize),
#   so the earlier window — which started tracemalloc AFTER execute() returned —
#   measured only the dict re-packaging and left the W1-W4 conversion allocations
#   (interned/ordered keys, one-time column-name interning) OUTSIDE the ratchet.
#   With execute() now inside the window we capture the actual conversion cost:
#   -> 50 rows x 101 columns present in the fixture
#   -> traced peak observed across 10 consecutive runs = 8,967.0 .. 9,078.2
#      bytes/row (vs the old post-execute-only ~8,243 — the ~800 bytes/row delta
#      is the previously-unmeasured conversion allocation now inside the window).
# Budget = 14,000 bytes/row (~1.54x the observed 9,078 max) — modest headroom for
# interpreter/allocator variance across platforms and Python versions while
# still tripping on a regression that roughly doubles per-row allocation (e.g.
# re-interning every column key per row instead of the one-time #1445/#1446
# interning). Documented per the issue mandate: pinned to a measured number,
# never a guess.
BUDGET_BYTES_PER_ROW = 14_000


def test_per_row_alloc_budget():
    """Per-row allocation peak on a wide result must stay under the budget.

    Pins the W1-W4 conversion allocation wins so a future refactor cannot
    silently re-introduce O(rows x columns) allocation.
    """
    require_test_data(SCHEMA_WIDE_ROWS)
    skip_if_no_schema(SCHEMA_WIDE_ROWS)

    with cqlite.open(DATASETS, schema=SCHEMA_WIDE_ROWS) as db:
        # Measure the CONVERSION PATH itself (issue #1449 roborev fix): the
        # window MUST wrap execute() — Python converts every row/cell to a
        # PyObject eagerly inside execute(), so measuring only the later
        # `dict(r)` re-packaging left the W1-W4 conversion allocations
        # unmeasured and a regression there would not trip this ratchet.
        tracemalloc.start()
        result = db.execute(f"SELECT * FROM {WIDE_TABLE} LIMIT {ROW_LIMIT}")
        materialized = [dict(r) for r in result.rows]
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        # Keep the materialized data alive across the measurement so nothing is
        # freed before `peak` is read. The length checks below run AFTER
        # tracemalloc.stop() so they add no allocation to the measured window.
        n = len(result)
        # FAIL LOUDLY (never skip) on a present-but-empty/unreadable dataset:
        # a zero-row result would make the per-row math divide-by-zero and
        # would false-green the ratchet.
        assert n > 0, (
            f"fixture {WIDE_TABLE} present but returned 0 rows — datasets "
            "unreadable or table dropped (see issue #1230)"
        )
        assert len(materialized) == n

        per_row = peak / n
        assert per_row < BUDGET_BYTES_PER_ROW, (
            f"per-row allocation {per_row:.1f} bytes exceeded budget "
            f"{BUDGET_BYTES_PER_ROW} bytes (rows={n}, peak={peak}). "
            "A W1-W4 conversion win likely regressed (O(rows x columns) "
            "allocation re-introduced) — see issue #1449."
        )


if __name__ == "__main__":  # pragma: no cover - manual measurement helper
    pytest.main([__file__, "-v", "-s"])
