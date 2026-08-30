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

import gc
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
    """``close()`` then drop is a single cleanup, not two.

    The ``AtomicBool`` swap is the single source of "already cleaned up", so the
    drop after an explicit ``close()`` must be a silent no-op: no error, and —
    observably — no SECOND flush producing an extra generation file.
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
print("CONSTRUCTED", flush=True)
# No close(), no del: fall off the end of the script so the handle is dropped by
# interpreter finalization.
"""


def test_drop_does_not_raise_at_teardown(tmp_path, schema_file):
    """Dropping at interpreter teardown must not abort or raise.

    Run in a child process: an abort (SIGABRT, rc<0) or a panic during
    finalization would kill the pytest runner itself, so it can only be observed
    as a test failure from outside.
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
    assert proc.returncode == 0, f"child did not exit cleanly:\n{ctx}"

    lowered = proc.stderr.lower()
    for needle in ("panicked at", "abort", "fatal python error", "segmentation fault"):
        assert needle not in lowered, (
            f"child stderr reports a {needle!r} during teardown:\n{ctx}"
        )
