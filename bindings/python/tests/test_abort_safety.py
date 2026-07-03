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
# terminal sentinel. Setup -- ``import cqlite``, ``cqlite.open``, and the
# entry-method lookup -- sits OUTSIDE the success ``try``: a broken build
# (ImportError), missing schema, failed ``open``, or a renamed/absent API method
# crashes non-zero and surfaces as a setup failure, never a false-green
# ``RAISED_*``. Only an EXPECTED native error thrown FROM the entry-point call
# (``cqlite.CqliteError`` or a PyO3 ``PanicException`` under panic=unwind) is
# accepted as a terminal ``RAISED_*``; any other exception re-raises and aborts
# non-zero. The driver emits ``OPENED`` then ``CALLING <entry>`` before any
# terminal sentinel so the parent can prove corrupt input was driven through the
# entry point. A hard abort prints no terminal sentinel and exits with a signal.
_DRIVER = r"""
import sys, os, tempfile
import cqlite

root, schema, entry = sys.argv[1], sys.argv[2], sys.argv[3]
QUERY = "SELECT * FROM test_basic.simple_table"

# entry name -> underlying Database method that must exist on the instance.
METHODS = {"execute": "execute", "streaming": "execute_streaming", "parquet": "export_parquet"}

# --- setup: NOT catchable as success ---
db = cqlite.open(root, schema=schema)
print("OPENED", flush=True)

method_name = METHODS.get(entry)
if method_name is None or not callable(getattr(db, method_name, None)):
    print("BADENTRY %s" % entry, flush=True)
    raise SystemExit(3)


def _call():
    if entry == "execute":
        return sum(1 for _ in db.execute(QUERY))
    if entry == "streaming":
        return sum(1 for _ in db.execute_streaming(QUERY))
    out = os.path.join(tempfile.mkdtemp(), "out.parquet")
    return db.export_parquet(QUERY, out)


print("CALLING %s" % entry, flush=True)
# --- ONLY the entry-point call is catchable as success ---
try:
    n = _call()
    print("RETURNED rows=%d" % n, flush=True)
except cqlite.CqliteError as exc:
    print("RAISED_CQLITE %s" % type(exc).__name__, flush=True)
except BaseException as exc:
    # Accept ONLY a PyO3 PanicException (the panic=unwind containment path).
    # Any other unexpected exception must NOT read as a passing terminal state.
    if type(exc).__name__ == "PanicException":
        print("RAISED_OTHER %s" % type(exc).__name__, flush=True)
    else:
        raise
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


def _assert_drove_entry_point(result, entry, ctx):
    """Prove corrupt input was driven THROUGH the entry point.

    The driver must have emitted ``OPENED`` then ``CALLING <entry>`` before any
    terminal sentinel. This defeats a false-green where a setup/open/lookup
    failure would otherwise surface as a passing ``RAISED_*``.
    """
    lines = result.stdout.splitlines()
    assert "OPENED" in lines, f"driver never opened the DB (setup failed?)\n{ctx}"
    calling = "CALLING %s" % entry
    assert calling in lines, f"driver never reached the {entry} entry point\n{ctx}"
    opened_idx = lines.index("OPENED")
    calling_idx = lines.index(calling)
    assert calling_idx > opened_idx, f"CALLING before OPENED\n{ctx}"
    terminal = ("RETURNED", "RAISED_CQLITE", "RAISED_OTHER")
    term_idx = next(
        (i for i, ln in enumerate(lines) if any(ln.startswith(t) for t in terminal)),
        None,
    )
    assert term_idx is not None, (
        f"no terminal sentinel on stdout (possible abort/hang)\n{ctx}"
    )
    assert term_idx > calling_idx, (
        f"terminal sentinel appeared before the entry-point call\n{ctx}"
    )


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
    # silently -- AFTER actually driving corrupt input through the entry point.
    # RAISED_OTHER (e.g. a PanicException under panic=unwind) still proves the
    # boundary contained the panic and the process lived on — which is exactly
    # the guarantee #1440 extends to release builds.
    _assert_drove_entry_point(result, entry, ctx)


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
    _assert_drove_entry_point(result, entry, ctx)
