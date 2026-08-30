"""Drop safety net for ``cqlite.Database`` (issue #1461).

``Database.close()`` does three things: closes the write engine (which flushes
any remaining memtable to a real SSTable), shuts down the read-side storage
engine, and flushes buffered telemetry.  Before this issue there was no
``impl Drop``, so a handle that was garbage-collected without ``close()`` —
plenty of real code neither uses ``with`` nor a ``finally:`` — skipped all
three.  These tests pin the *mechanic*: a dropped handle runs ``close()``'s
path best-effort, exactly once, and can never take the interpreter down while
doing it.

Observable side effect used throughout: closing a writable handle with a
non-empty memtable materializes ``write_dir/data/<ks>/<table>/*-Data.db``.  If
Drop runs the write-engine close, that file appears after ``del`` + a GC pass;
if it does not, the directory stays empty.  That is a *file on disk*, not an
introspection hook, so it cannot pass vacuously.

No dataset fixtures are needed: every case writes its own schema and SSTables
into ``tmp_path``.  The one thing that could be "present but unreadable" is the
temp directory itself, and that is asserted (``pytest.fail``, never ``skip``).

Deliberately no wall-clock threshold asserts anywhere in this file: cleanup is
asserted by its *effect*, never by how long it took.
"""

from __future__ import annotations

import errno
import gc
import os
import signal
import subprocess
import sys
from pathlib import Path

import pytest

import cqlite

SCHEMA_TEXT = """\
CREATE KEYSPACE IF NOT EXISTS drop_test
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE drop_test;

CREATE TABLE IF NOT EXISTS items (
    id    INT PRIMARY KEY,
    name  TEXT,
    value INT
);
"""


@pytest.fixture()
def schema_file(tmp_path: Path) -> Path:
    """A minimal writable schema on disk.

    FAIL LOUDLY (never skip): if the temp dir is present-but-unwritable the
    whole premise of these tests is gone, and a skip would hide it.
    """
    path = tmp_path / "drop-schema.cql"
    try:
        path.write_text(SCHEMA_TEXT)
    except OSError as exc:  # pragma: no cover - environment failure
        pytest.fail(f"cannot write schema fixture to {path}: {exc}")
    if not path.is_file():
        pytest.fail(f"schema fixture missing after write: {path}")
    return path


def _count_data_db(write_dir: Path) -> int:
    """Number of flushed generation files under ``write_dir``.

    Flushed SSTables land under ``data/<keyspace>/<table>/``, so recurse rather
    than globbing the top level (same helper shape as
    ``test_auto_flush_cliff.py``).
    """
    data_path = Path(write_dir) / "data"
    if not data_path.exists():
        return 0
    return len(list(data_path.rglob("*-Data.db")))


def _open_writable(tmp_path: Path, schema_file: Path, name: str):
    """Open a writable handle over freshly-created empty dirs."""
    data_dir = tmp_path / f"{name}-data"
    data_dir.mkdir(exist_ok=True)
    write_dir = tmp_path / f"{name}-wd"
    db = cqlite.open(
        str(data_dir),
        schema=str(schema_file),
        writable=True,
        write_dir=str(write_dir),
    )
    return db, write_dir


def test_drop_without_close_runs_cleanup(tmp_path, schema_file):
    """A handle dropped WITHOUT ``close()`` still flushes the memtable.

    This is the regression the issue exists for: on ``main`` (no ``impl Drop``)
    the post-GC Data.db count stays 0.
    """
    db, write_dir = _open_writable(tmp_path, schema_file, "nodclose")

    result = db.execute(
        "INSERT INTO drop_test.items (id, name, value) VALUES (1, 'dropped', 42)"
    )
    assert result.rows_affected == 1

    # Precondition: the row is still only in the memtable — nothing on disk yet,
    # so a post-GC Data.db can only have come from the cleanup path.
    assert _count_data_db(write_dir) == 0, (
        "precondition failed: memtable already flushed before drop, so this "
        "test could not attribute a Data.db to the Drop hook"
    )
    assert not db.is_closed

    # Drop WITHOUT close(). `del` removes the only strong reference; the GC pass
    # makes collection deterministic even if a cycle is involved.
    del db
    del result
    gc.collect()

    assert _count_data_db(write_dir) >= 1, (
        "Drop did not run close()'s path: expected at least one flushed "
        f"*-Data.db under {write_dir / 'data'}, found none "
        f"(contents: {sorted(str(p) for p in Path(write_dir).rglob('*'))})"
    )


def test_double_cleanup_is_safe(tmp_path, schema_file):
    """``close()`` then drop must not error and must not flush twice.

    WHAT THIS TEST CAN AND CANNOT SEE — stated because the name promises more
    than any Python-visible assertion can deliver.  It pins: no exception, and
    no second generation file.  It does NOT prove the Rust ``AtomicBool`` guard
    is what prevented the second cleanup, because
    ``WriteEngine::close`` is ALSO internally idempotent (its own ``closed``
    swap in ``cqlite-core/src/storage/write_engine/mod.rs``), so a second call
    would return ``Ok`` without flushing even if the binding-level guard were
    removed entirely.  The guard's effect is therefore not observable from
    Python, and this is a no-crash / no-extra-flush regression guard rather than
    proof of the guard.  Treating it as proof is the mistake this docstring
    exists to prevent.
    """
    db, write_dir = _open_writable(tmp_path, schema_file, "double")

    db.execute(
        "INSERT INTO drop_test.items (id, name, value) VALUES (2, 'closed', 7)"
    )
    db.close()
    assert db.is_closed

    after_close = _count_data_db(write_dir)
    assert after_close >= 1, (
        f"close() did not flush: expected a *-Data.db under {write_dir / 'data'}"
    )

    # Idempotent explicit close, then drop. Neither may raise, and neither may
    # produce a second generation file.
    db.close()
    del db
    gc.collect()

    assert _count_data_db(write_dir) == after_close, (
        "drop after close() ran cleanup a second time: generation-file count "
        f"went {after_close} -> {_count_data_db(write_dir)}"
    )


# The child body for the teardown test. Constructs a writable handle, writes a
# row, and exits IMMEDIATELY without close() — so the Rust Drop runs during
# interpreter finalization, which is the hazardous moment (the tokio runtime may
# be gone; a panic there is a process abort under `panic = "abort"`).
_TEARDOWN_DRIVER = """\
import sys

import cqlite

data_dir, write_dir, schema = sys.argv[1], sys.argv[2], sys.argv[3]
db = cqlite.open(
    data_dir, schema=schema, writable=True, write_dir=write_dir
)
db.execute("INSERT INTO drop_test.items (id, name, value) VALUES (3, 'teardown', 9)")

# Report the pre-exit generation count so the parent can prove the Data.db it
# checks for came from the teardown drop and not from an auto-flush that had
# already crossed its threshold (the same precondition test 1 asserts).
from pathlib import Path as _Path

_data = _Path(write_dir) / "data"
_pre = len(list(_data.rglob("*-Data.db"))) if _data.exists() else 0
print(f"PREFLUSH={_pre}", flush=True)
print("CONSTRUCTED", flush=True)
# No close(), no del: fall off the end of the script so the handle is dropped by
# interpreter finalization.
"""


def test_drop_does_not_raise_at_teardown(tmp_path, schema_file):
    """Dropping at interpreter teardown must not abort, panic, or raise.

    Run in a child process because an abort would kill the pytest runner itself.

    WHICH ASSERT DETECTS WHAT — worth stating, because the intuitive reading is
    wrong. The wheel ships ``--profile release-unwind`` (``panic = "unwind"``,
    gate component ``binding-unwind-profile``, #1440), and pyo3's ``tp_dealloc``
    trampoline catches an escaping panic and reports it via
    ``sys.unraisablehook``. So a panic in ``Drop`` does NOT change the exit code:
    the child still exits 0. The detector for a panic is therefore the
    ``"panicked at"`` stderr needle: Rust's default hook writes the message and
    location to fd 2 before unwinding begins, so it survives interpreter
    finalization and is independent of ``catch_unwind``. ``"panicexception"``
    carries it too. ``"exception ignored in"`` is kept as a cheap extra but is
    NOT reliable here — pyo3's trampoline passes a null context object, and
    CPython prints that prefix only when it has a context or an ``err_msg``.
    ``returncode`` detects a hard abort/segfault only.
    """
    data_dir = tmp_path / "teardown-data"
    data_dir.mkdir()
    write_dir = tmp_path / "teardown-wd"

    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            _TEARDOWN_DRIVER,
            str(data_dir),
            str(write_dir),
            str(schema_file),
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )

    ctx = (
        f"rc={proc.returncode}\nstdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    )
    assert "CONSTRUCTED" in proc.stdout, (
        f"child never reached the drop point, so this test proved nothing:\n{ctx}"
    )
    assert "PREFLUSH=0" in proc.stdout, (
        "precondition failed: the child had already flushed before exiting, so a "
        f"Data.db here could not be attributed to the teardown drop:\n{ctx}"
    )
    # More specific diagnostic first: a hard abort is rc == -SIGABRT, and saying
    # so beats "did not exit cleanly" when it fires.
    assert proc.returncode != -signal.SIGABRT, f"child died on SIGABRT:\n{ctx}"
    assert proc.returncode == 0, f"child did not exit cleanly:\n{ctx}"

    lowered = proc.stderr.lower()
    for needle in (
        "panicked at",
        "panicexception",
        "exception ignored in",
        "fatal python error",
        "segmentation fault",
    ):
        assert needle not in lowered, (
            f"child stderr reports a {needle!r} during teardown:\n{ctx}"
        )

    # Non-vacuity: a clean exit alone is also what a MISSING Drop hook looks
    # like, so additionally require that the finalization-time drop actually did
    # the work. CPython clears ``__main__``'s globals during finalization, which
    # deallocates the handle and runs the Rust ``Drop``; the flushed generation
    # file is the proof it ran there, not merely that nothing crashed.
    assert _count_data_db(write_dir) >= 1, (
        "child exited cleanly but the teardown drop ran no cleanup: expected a "
        f"flushed *-Data.db under {write_dir / 'data'}\n{ctx}"
    )


def test_writable_iterator_survives_drop(tmp_path, schema_file):
    """An iterator outliving a WRITABLE ``Database`` keeps working after the drop.

    This inverts an earlier version of this test, and the reason is the point.
    That version asserted ``RuntimeError``, justified by #1462's contract for an
    explicit ``close()``.  Two independent review rounds showed the
    justification was false: a ``StreamingIterator`` reads an ``mpsc::Receiver``
    fed by a detached task holding its OWN ``Arc<StorageEngine>``, so closing the
    WRITE engine cannot invalidate it, and ``cqlite_core::Database`` has no
    ``Drop`` — nothing the drop does can stop the stream.  Setting the shared
    ``closed`` flag would only have broken a working pattern in exchange for
    nothing, so the drop now READS that flag instead of claiming it, and this
    safety net is purely additive: no user-visible iterator behavior changes.

    Hermetic: the empty stream isolates *which exception ends the iteration*.
    ``StopIteration`` means the iterator is still valid and merely exhausted;
    ``RuntimeError: Database is closed`` would mean the drop invalidated it.
    """
    db, write_dir = _open_writable(tmp_path, schema_file, "iterdrop")

    # Round 4's claim is a CONJUNCTION — the teardown still happens AND the flag
    # is not flipped — so this test pins both halves. Without the row, it would
    # pass with `impl Drop` deleted entirely.
    db.execute(
        "INSERT INTO drop_test.items (id, name, value) VALUES (5, 'conj', 12)"
    )
    assert _count_data_db(write_dir) == 0, "precondition: nothing flushed yet"

    iterator = db.execute_streaming("SELECT * FROM drop_test.items")

    # Drop the parent WITHOUT close(); the iterator keeps the Python reference.
    del db
    gc.collect()

    # Half 1: the teardown RAN (the memtable reached an SSTable).
    assert _count_data_db(write_dir) >= 1, (
        "the writable drop did not flush, so this test would pass with the Drop "
        "impl deleted and proves nothing about the flag"
    )

    # Half 2: and it did NOT invalidate the iterator.
    with pytest.raises(StopIteration):
        next(iterator)


def test_readonly_iterator_survives_drop(tmp_path):
    """A READ-ONLY handle's iterator keeps working after the handle is dropped.

    The counterpart to the test above, and the regression guard for review
    finding B1.  A read-only ``Database`` has no write engine, and
    ``StorageEngine::shutdown()`` is a documented no-op ("Nothing to shutdown -
    read-only storage layer"), so a drop tears down NOTHING.  If it claimed the
    shared ``closed`` flag anyway it would invalidate every outstanding
    iterator — silently breaking this pattern, which works today, in exchange
    for no cleanup at all:

        def rows(path):
            db = cqlite.open(path)
            return db.execute_streaming("SELECT ...")   # db drops at return

    So the drop must READ that flag without claiming it.

    This case uses a NON-EMPTY corpus on purpose: an empty stream would only show
    that no ``RuntimeError`` is raised, which is weaker than the claim being
    made.  Here rows are written by a first (writable) handle, then read back by
    a second, read-only handle whose iterator must keep DELIVERING rows across
    the drop — the property the README promises.
    """
    # Phase 1: produce a real SSTable to read.
    write_dir = tmp_path / "ro-src-wd"
    data_dir = tmp_path / "ro-src-data"
    data_dir.mkdir()
    schema = tmp_path / "ro-schema.cql"
    schema.write_text(SCHEMA_TEXT)
    producer = cqlite.open(
        str(data_dir), schema=str(schema), writable=True, write_dir=str(write_dir)
    )
    # Enough rows that a buffer_size=1 stream CANNOT have them all in flight, so
    # the detached producer task must still be running after the handle dies.
    # With 3 rows and the default 1024-row buffer, `list(iterator)` would just
    # drain a full buffer and prove nothing about producer survival.
    row_count = 50
    for i in range(row_count):
        producer.execute(
            f"INSERT INTO drop_test.items (id, name, value) VALUES ({i}, 'r{i}', {i})"
        )
    producer.close()

    sstable_dir = write_dir / "data"
    if not any(sstable_dir.rglob("*-Data.db")):
        pytest.fail(
            f"fixture setup failed: no *-Data.db under {sstable_dir}, so this test "
            "could not read anything back and would prove nothing"
        )

    # Phase 2: read-only handle, pull one row, then drop the handle mid-stream.
    db = cqlite.open(str(sstable_dir), schema=str(schema))
    # buffer_size=1 keeps at most one row in flight, so the rows that arrive
    # after the handle is collected can only have come from a producer task that
    # is STILL RUNNING — which is the actual claim (it owns its own
    # Arc<StorageEngine>, so handle teardown cannot stop it).
    iterator = db.execute_streaming(
        "SELECT * FROM drop_test.items",
        config=cqlite.StreamingConfig(buffer_size=1),
    )

    first = next(iterator)
    assert isinstance(first, cqlite.Row)

    del db
    gc.collect()

    # The remaining rows must still ARRIVE — not merely "no RuntimeError".
    rest = list(iterator)
    assert len(rest) == row_count - 1, (
        "read-only drop broke a live iterator: expected the remaining "
        f"{row_count - 1} rows after the handle was collected, got {len(rest)}"
    )


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork() is Unix-only")
def test_fork_child_drop_does_not_touch_parent_state(tmp_path, schema_file):
    """A forked child's drop must NOT act on the parent's inherited descriptors.

    The one DESTRUCTIVE hazard adding a ``Drop`` could introduce, so it gets a
    real regression test rather than a comment.  ``fork()`` copies the memtable
    and the ``Database`` object and SHARES the file descriptors, and
    ``multiprocessing``'s default start method on Linux is ``fork`` — so a child
    exiting or collecting the inherited handle is ordinary Python.  If the
    child's drop ran the write-engine close it would, through the PARENT's fds:
    write a duplicate SSTable generation, TRUNCATE THE PARENT'S WAL (the write
    engine truncates after a successful flush), and release the parent's
    exclusive ``write_dir`` lock.

    The parent asserts all three stayed intact — no duplicate generation, WAL
    unchanged, and its exclusive ``write_dir`` lock still held — then closes
    normally to prove the guard suppressed only the child's teardown, not its own.

    The lock assertion is the load-bearing one, because an early ``return`` alone
    does NOT prevent that effect: ``WriteEngine``'s own ``Drop`` unlocks
    unconditionally, and returning from ``Database::drop`` is precisely when its
    fields get dropped. The guard therefore leaks the inherited engine.
    """
    db, write_dir = _open_writable(tmp_path, schema_file, "forked")
    db.execute(
        "INSERT INTO drop_test.items (id, name, value) VALUES (4, 'forked', 11)"
    )

    wal = write_dir / "wal" / "commitlog.wal"
    if not wal.is_file():
        pytest.fail(
            f"fixture setup failed: no WAL at {wal}, so this test could not "
            "detect a truncation and would prove nothing"
        )
    wal_size_before = wal.stat().st_size
    assert wal_size_before > 0, "WAL is empty before the fork; nothing to protect"
    assert _count_data_db(write_dir) == 0, "precondition: nothing flushed yet"

    pid = os.fork()
    if pid == 0:
        # CHILD. Drop the inherited handle deterministically, then leave without
        # running interpreter finalization or pytest teardown. _exit(0) skips
        # atexit/flush handlers on purpose: the Rust Drop above is the only thing
        # under test here.
        try:
            del db
            gc.collect()
            os._exit(0)
        except BaseException:
            os._exit(70)

    _waited_pid, status = os.waitpid(pid, 0)
    assert os.WIFEXITED(status), f"forked child did not exit normally: {status!r}"
    assert os.WEXITSTATUS(status) == 0, (
        f"forked child exited {os.WEXITSTATUS(status)} (70 = it raised)"
    )

    assert _count_data_db(write_dir) == 0, (
        "the forked child's drop flushed an SSTable into the PARENT's data dir: "
        f"{sorted(str(x) for x in (write_dir / 'data').rglob('*-Data.db'))}"
    )
    assert wal.stat().st_size == wal_size_before, (
        "the forked child's drop truncated the PARENT's WAL: "
        f"{wal_size_before} -> {wal.stat().st_size} bytes"
    )

    # THIRD effect, and the one an early `return` alone does NOT prevent:
    # `impl Drop for WriteEngine` releases the write_dir advisory lock
    # unconditionally, and flock ownership is per open-file-description — which
    # fork() SHARES — so an inherited engine's drop would unlock the lock the
    # PARENT holds. The guard therefore leaks the inherited engine instead.
    #
    # The probe must use a FRESH open(): a duplicated/inherited fd shares the
    # open-file-description and would not conflict with itself.
    # `fcntl` is Unix-only, so it is imported HERE rather than at module scope:
    # a top-level import fails COLLECTION on Windows, before the skipif on this
    # test can take effect (roborev job 174).
    import fcntl

    lock_path = write_dir / "wal" / ".lock"
    if not lock_path.is_file():
        pytest.fail(
            f"fixture setup failed: no advisory lock file at {lock_path}, so the "
            "lock half of this test could not be checked and would prove nothing"
        )
    probe_fd = os.open(str(lock_path), os.O_RDWR)
    try:
        with pytest.raises(OSError) as lock_attempt:
            fcntl.flock(probe_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert lock_attempt.value.errno in (errno.EACCES, errno.EAGAIN), (
            "expected the parent's exclusive write_dir lock to block this probe, "
            f"got errno={lock_attempt.value.errno}"
        )
    finally:
        os.close(probe_fd)

    # The parent's own cleanup must still work — the guard is about inherited
    # state, not a blanket disable.
    db.close()
    assert _count_data_db(write_dir) >= 1, (
        "parent close() after the fork failed to flush, so the fork guard is "
        "suppressing more than the child's teardown"
    )
