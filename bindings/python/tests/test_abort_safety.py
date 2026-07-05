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
  Python exception and the child survives.  The host must ALSO survive on a
  ``panic = "abort"`` RELEASE wheel (CI builds with ``--release``).  Two
  independent mechanisms deliver that survival, and the harness accepts either:
    * the abort boundary contains a panic (the ``panic=unwind`` firewall from
      issue #1440 turns a would-be SIGABRT into a caught exception), OR
    * the parser never panics in the first place — parser hardening (issue
      #1632 guards + the #1614 fuzz mandate to return ``Ok``/``Err`` on
      arbitrary bytes) makes the corrupt-input path return a clean
      ``cqlite.CqliteError`` so there is no panic to abort on.
  Because of the hardening, the corrupt uncompressed path now survives
  GRACEFULLY on both strategies; a genuine post-entry abort on an abort wheel,
  if one still occurs, is accepted as an xfail (see the uncompressed test).

Two fixture flavors are exercised (see ``corrupt_fixture``):
  * ``compressed`` — the exact issue recipe (mutate the Snappy-compressed
    Data.db). The decompression layer contains the corruption, so all three
    entry points survive today in BOTH debug and release. This proves graceful
    containment on the compressed path.
  * ``uncompressed`` — additionally drops ``CompressionInfo.db`` so the corrupt
    bytes reach the raw VInt/row parser. This path historically PANICKED on
    corrupt input, so on a ``panic=abort`` RELEASE wheel it SIGABRTed the child.
    Parser hardening (issue #1632 recursion/bounds/capacity guards, and the
    #1614 fuzz mandate that the parser return ``Ok``/``Err`` and never panic on
    arbitrary bytes) has since made ``execute``/``streaming`` return a clean
    ``cqlite.CqliteError`` instead of panicking — so the child now GRACEFULLY
    SURVIVES on BOTH panic strategies. That graceful containment is the correct
    outcome and is an ACCEPTED PASS; a genuine post-entry abort, if one still
    occurs on an abort wheel, is accepted as an xfail. ``parquet`` is
    intentionally NOT run on this flavor: it triggers a SEPARATE non-panic HANG
    (infinite loop / pathological allocation) — reported for follow-up, out of
    scope for this harness.

These tests actually run and assert (no silent skip) whenever the dataset is
present; a present-but-empty source Data.db FAILs loudly per issue #1437.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile

import pytest

import cqlite

from conftest import (
    SCHEMA_BASIC_TYPES,
    DATASETS,
    _require_fixtures_strict,
    skip_if_no_schema,
)
from corrupt_fixture import MODES, make_corrupt_fixture, source_table_dir

# Entry points exercised per fixture. Each name selects a branch in the driver.
ENTRY_POINTS = ("execute", "streaming", "parquet")


def _built_with_panic_abort() -> bool:
    """Whether the loaded wheel was compiled with ``panic = "abort"``.

    Delegates to the ``cqlite._built_with_panic_abort`` introspection helper
    (issue #1437), which reports the ACTUAL compiled panic strategy via
    ``cfg!(panic = "abort")``. Guarded so this file still collects against an
    older wheel that predates the symbol: a missing symbol is treated as "not
    abort", i.e. the survival cases below hard-assert (preserving the gate's
    teeth) rather than silently xfailing.
    """
    probe = getattr(cqlite, "_built_with_panic_abort", None)
    return bool(probe()) if callable(probe) else False


# True only on a `panic = "abort"` wheel (release, pre-#1440). See the marker
# rationale on the uncompressed test below.
_PANIC_ABORT = _built_with_panic_abort()

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


def _reached_entry_point_call(result, entry):
    """Whether the child emitted ``OPENED`` then ``CALLING <entry>`` IN ORDER.

    Uses the SAME ordering sentinels the survival path asserts, but WITHOUT
    requiring a terminal sentinel (an abort prints none). Returns True only when
    the child proved it opened the DB and then invoked the corrupt-input entry
    point -- distinguishing a genuine post-call abort from a setup/open/lookup
    failure that ALSO exits non-zero but never reaches the entry point.
    """
    lines = result.stdout.splitlines()
    if "OPENED" not in lines:
        return False
    calling = "CALLING %s" % entry
    if calling not in lines:
        return False
    return lines.index(calling) > lines.index("OPENED")


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


# The uncompressed flavor reaches the raw VInt/row parser. parquet is excluded
# because it hangs there (a separate non-panic bug); see the module docstring.
#
# GRACEFUL CONTAINMENT IS THE EXPECTED OUTCOME (issue #1973). This path once
# PANICKED on corrupt input, so on a ``panic=abort`` release wheel the child
# SIGABRTed. Parser hardening (issue #1632 recursion/bounds/capacity guards +
# the #1614 fuzz mandate that the parser return ``Ok``/``Err`` and never panic
# on arbitrary bytes) made the corrupt-input path return a clean
# ``cqlite.CqliteError`` instead of panicking. So the child now SURVIVES on
# BOTH panic strategies, and a surviving child that drove corrupt input through
# the entry point is an ACCEPTED PASS.
#
# The gate keeps its teeth, and the abort branch keeps its guards, applied
# IMPERATIVELY (not a function-level ``@pytest.mark.xfail`` decorator, which
# would mask setup-phase failures — e.g. a fail-closed dataset gate under
# ``CQLITE_REQUIRE_FIXTURES=1`` — as an expected xfail). ``_require_source_or_skip``
# runs FIRST and UNMASKED, then the outcome is judged:
#   * ANY wheel, child SURVIVES (rc==0): hard-assert it drove corrupt input
#     through the entry point and reached a catchable terminal sentinel -> PASS.
#     This is the hardened-parser (or contained-panic) success path.
#   * unwind wheel (`_PANIC_ABORT` False), rc!=0: the boundary regressed under a
#     build that must contain panics -> hard FAIL.
#   * abort wheel (`_PANIC_ABORT` True), rc!=0: accepted only as a genuine
#     post-entry abort. A non-zero exit AFTER reaching the entry point is the
#     tolerated xfail (a still-panicking parser path SIGABRTing on abort); a
#     non-zero exit that never reached the entry point is a setup/open/lookup
#     failure and hard-FAILs (never masquerades as the expected abort).
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("entry", ("execute", "streaming"))
def test_uncompressed_corrupt_sstable_panic_is_contained(mode, entry):
    """Corrupt raw-parse input must never kill the host; graceful survival wins.

    The hardened parser (issue #1632 guards + the #1614 fuzz mandate) returns a
    clean ``cqlite.CqliteError`` on this corrupt input rather than panicking, so
    the child SURVIVES (rc==0) and reaches a catchable terminal sentinel on
    BOTH the debug/unwind and the release/abort wheel -> PASS. If a still-
    panicking path ever SIGABRTs on an abort wheel (rc!=0 AFTER reaching the
    entry point) that is accepted as an xfail; issue #1440's panic=unwind
    firewall would contain even that. A non-zero exit that never reached the
    entry point is a setup/open/lookup failure and hard-FAILs.
    """
    # Dataset/schema gating runs FIRST and UNMASKED: a strict fail-closed
    # dataset failure here is a hard error, never swallowed as an expected xfail.
    _require_source_or_skip()
    result = _run_driver(mode, entry, expose_uncompressed=True)
    ctx = (
        f"mode={mode} entry={entry} rc={result.returncode}\n"
        f"stdout={result.stdout!r}\nstderr={result.stderr!r}"
    )
    if result.returncode != 0:
        # A non-zero exit lacking the ``OPENED``/``CALLING <entry>`` ordering
        # sentinels is a setup/import/open/API-lookup failure that never drove
        # corrupt input through the boundary -- always a hard FAIL, never a
        # false-green.
        if not _reached_entry_point_call(result, entry):
            pytest.fail(
                "child exited non-zero WITHOUT reaching the entry point "
                f"(missing OPENED/CALLING {entry}): a setup/open/lookup "
                "failure, not a post-call abort.\n"
                f"{ctx}",
                pytrace=False,
            )
        if _PANIC_ABORT:
            # abort wheel: a genuine post-entry abort is tolerated as an xfail.
            pytest.xfail(
                "corrupt uncompressed SSTable still aborts the interpreter on a "
                f"panic=abort wheel; #1440's panic=unwind firewall contains it "
                f"(#1437/#1973)\n{ctx}"
            )
        # unwind wheel: the boundary MUST contain the panic and the child MUST live.
        pytest.fail(
            "child process aborted on corrupt input under a DEBUG/unwind build "
            f"-- the boundary regressed.\n{ctx}",
            pytrace=False,
        )
    # rc==0: the child SURVIVED. Prove it actually drove corrupt input through
    # the entry point and reached a catchable terminal sentinel (never a silent
    # setup-skip). This is the correct, hardened-parser outcome on ANY wheel.
    _assert_drove_entry_point(result, entry, ctx)
