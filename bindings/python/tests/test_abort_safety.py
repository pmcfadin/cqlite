"""Abort-safety regression harness (issue #1437).

Drives a corrupt/truncated SSTable through every Python entry point and proves
the host interpreter SURVIVES: each call must terminate in a catchable state
(a normal return, a ``cqlite.CqliteError``, or — under a ``panic=unwind``
build — a caught panic surfaced as a Python exception) rather than aborting
the process.

Why a subprocess?  The workspace *release* profile is ``panic = "abort"``
(``Cargo.toml``), so a panic inside ``cqlite-core`` during a scan through PyO3
does not become a catchable exception — it kills the whole interpreter with a
signal.  The only way to observe that kill as a test FAILURE (instead of the
pytest runner itself dying) is to run each driver in a child process and assert
the child exited 0 while emitting a terminal sentinel on stdout.

DEBUG vs RELEASE (important):
  Under the normal debug/test profile used by the agent gate, ``[profile.dev]``
  is ``panic = "unwind"``, so PyO3 converts any core panic into a catchable
  Python exception and the child survives — these tests therefore PASS
  trivially in debug and do NOT by themselves prove the release guarantee.
  The release-profile proof (flipping the release panic strategy so the same
  child survives a *release* wheel) lands with issue #1440.  This harness is
  the machinery that #1440 turns green on a release build; it FAILS on an
  unmodified ``main`` release wheel because the abort kills the child.

Two fixture flavors are exercised (see ``corrupt_fixture``):
  * ``compressed`` — the exact issue recipe (mutate the Snappy-compressed
    Data.db). The decompression layer contains the corruption, so all three
    entry points survive today in BOTH debug and release. This proves graceful
    containment on the compressed path.
  * ``uncompressed`` — additionally drops ``CompressionInfo.db`` so the corrupt
    bytes reach the raw VInt/row parser, which is where the audited
    corrupt-input panics live. ``execute``/``streaming`` panic here: caught by
    PyO3 under debug (survives, green) but SIGABRT under an unmodified ``main``
    RELEASE wheel (this is the DoD "fails on release" proof); #1440 turns it
    green on release too. ``parquet`` is intentionally NOT run on this flavor:
    it triggers a SEPARATE non-panic HANG (infinite loop / pathological
    allocation) that ``panic=unwind`` does not fix — reported for follow-up,
    out of scope for this harness.

These tests actually run and assert (no silent skip) whenever the dataset is
present; a present-but-empty source Data.db FAILs loudly per issue #1437.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile

import pytest

from conftest import (
    SCHEMA_BASIC_TYPES,
    DATASETS,
    _require_fixtures_strict,
    skip_if_no_schema,
)
from corrupt_fixture import MODES, make_corrupt_fixture, source_table_dir

# Entry points exercised per fixture. Each name selects a branch in the driver.
ENTRY_POINTS = ("execute", "streaming", "parquet")

# The child driver. Runs one entry point against a corrupt fixture and prints a
# terminal sentinel. ``import cqlite`` sits OUTSIDE the try so a broken build
# (ImportError) crashes non-zero and surfaces as a setup failure, never a false
# green. A hard abort prints no terminal sentinel and exits with a signal.
_DRIVER = r"""
import sys, os, tempfile
import cqlite

root, schema, entry = sys.argv[1], sys.argv[2], sys.argv[3]
QUERY = "SELECT * FROM test_basic.simple_table"
try:
    db = cqlite.open(root, schema=schema)
    print("OPENED", flush=True)
    if entry == "execute":
        n = sum(1 for _ in db.execute(QUERY))
        print("RETURNED rows=%d" % n, flush=True)
    elif entry == "streaming":
        n = sum(1 for _ in db.execute_streaming(QUERY))
        print("RETURNED rows=%d" % n, flush=True)
    elif entry == "parquet":
        out = os.path.join(tempfile.mkdtemp(), "out.parquet")
        rows = db.export_parquet(QUERY, out)
        print("RETURNED rows=%d" % rows, flush=True)
    else:
        print("BADENTRY %s" % entry, flush=True)
        raise SystemExit(3)
except cqlite.CqliteError as exc:
    print("RAISED_CQLITE %s" % type(exc).__name__, flush=True)
except SystemExit:
    raise
except BaseException as exc:  # includes PyO3 PanicException under panic=unwind
    print("RAISED_OTHER %s" % type(exc).__name__, flush=True)
"""


def _require_source_or_skip():
    """FAIL loudly on a broken source, skip only when the dataset is absent."""
    skip_if_no_schema(SCHEMA_BASIC_TYPES)
    src = source_table_dir(DATASETS)
    if src is None:
        msg = f"No test_basic.simple_table SSTable under {DATASETS}"
        if _require_fixtures_strict():
            pytest.fail(msg, pytrace=False)
        pytest.skip(msg)
    data = src / "nb-1-big-Data.db"
    if data.stat().st_size == 0:
        pytest.fail(f"Source {data} present but empty (issue #1437)", pytrace=False)


def _run_driver(mode, entry, *, expose_uncompressed):
    with tempfile.TemporaryDirectory(prefix="cqlite-abort-") as tmp:
        root = str(
            make_corrupt_fixture(
                tmp, DATASETS, mode, expose_uncompressed=expose_uncompressed
            )
        )
        return subprocess.run(
            [sys.executable, "-c", _DRIVER, root, str(SCHEMA_BASIC_TYPES), entry],
            capture_output=True,
            text=True,
            timeout=120,
        )


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("entry", ENTRY_POINTS)
def test_compressed_corrupt_sstable_survives(mode, entry):
    """Exact issue recipe: a corrupt compressed Data.db must not kill the host.

    Green in both debug and release today (Snappy decompression contains the
    corruption). Covers all three entry points and both mutation modes.
    """
    _require_source_or_skip()
    result = _run_driver(mode, entry, expose_uncompressed=False)
    ctx = (
        f"mode={mode} entry={entry} rc={result.returncode}\n"
        f"stdout={result.stdout!r}\nstderr={result.stderr!r}"
    )
    # Liveness: an abort exits with a signal (negative rc) or non-zero code.
    assert result.returncode == 0, f"child process did not survive corrupt input\n{ctx}"
    # And it reached a catchable terminal state rather than hanging or aborting
    # silently. RAISED_OTHER (e.g. a PanicException under panic=unwind) still
    # proves the boundary contained the panic and the process lived on — which
    # is exactly the guarantee #1440 extends to release builds.
    terminal = ("RETURNED", "RAISED_CQLITE", "RAISED_OTHER")
    assert any(tok in result.stdout for tok in terminal), (
        f"no terminal sentinel on stdout (possible abort/hang)\n{ctx}"
    )


# The uncompressed flavor reaches the raw-parser panic. parquet is excluded
# because it hangs there (a separate non-panic bug); see the module docstring.
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("entry", ("execute", "streaming"))
def test_uncompressed_corrupt_sstable_panic_is_contained(mode, entry):
    """The raw parse path panics on corrupt input; the boundary must contain it.

    RELEASE-vs-DEBUG: this ASSERTS the interpreter survives (rc==0). Under the
    debug/test profile (``panic=unwind``) PyO3 converts the core panic into a
    catchable ``PanicException`` and the child exits 0 -> PASS. Under an
    unmodified ``main`` RELEASE wheel (``panic=abort``) the same panic raises
    SIGABRT, the child dies with a signal, ``returncode != 0`` -> this test
    FAILS. That failure is the point (issue #1437 DoD): it proves the harness
    exercises a real abort. Issue #1440 flips the release panic strategy so
    this goes green on release too.
    """
    _require_source_or_skip()
    result = _run_driver(mode, entry, expose_uncompressed=True)
    ctx = (
        f"mode={mode} entry={entry} rc={result.returncode}\n"
        f"stdout={result.stdout!r}\nstderr={result.stderr!r}"
    )
    assert result.returncode == 0, (
        "child process aborted on corrupt input (expected on unmodified `main` "
        "release; #1440 fixes it). If this failed under a DEBUG build the "
        f"boundary regressed.\n{ctx}"
    )
    terminal = ("RETURNED", "RAISED_CQLITE", "RAISED_OTHER")
    assert any(tok in result.stdout for tok in terminal), (
        f"no terminal sentinel on stdout (possible abort/hang)\n{ctx}"
    )
