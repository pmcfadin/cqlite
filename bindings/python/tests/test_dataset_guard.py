"""Tests for the dataset-availability guard itself (issue #1458).

The guard IS the fail-loudly mechanism for missing SSTable fixtures, so it needs
its own coverage: a present-but-EMPTY datasets directory (the exact shape of the
original #773 failure) must HARD-FAIL under strict mode, never skip.

These tests drive the REAL ``conftest.skip_if_no_datasets()`` — they never
reimplement the counting logic — because a test that reimplements the helper it
asserts on is invariant to the bug (#3042).
"""

import pytest
from _pytest.outcomes import Failed, Skipped

import conftest

STRICT_ENV_VARS = ("CQLITE_REQUIRE_FIXTURES", "CQLITE_PARITY_REQUIRE_DATASETS")


def _empty_datasets_dir(tmp_path):
    """A datasets root that exists and has a keyspace dir, but zero *-Data.db."""
    datasets = tmp_path / "sstables"
    (datasets / "test_basic").mkdir(parents=True)
    return datasets


def _non_strict(monkeypatch):
    for var in STRICT_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


def _strict(monkeypatch):
    monkeypatch.setenv("CQLITE_REQUIRE_FIXTURES", "1")
    monkeypatch.delenv("CQLITE_PARITY_REQUIRE_DATASETS", raising=False)


def test_strict_fails_on_empty_datasets_dir(tmp_path, monkeypatch):
    """Strict mode + present-but-empty datasets dir => hard FAIL, not skip."""
    monkeypatch.setattr(conftest, "DATASETS", _empty_datasets_dir(tmp_path))
    _strict(monkeypatch)

    assert conftest._count_data_db() == 0

    with pytest.raises(Failed) as excinfo:
        conftest.skip_if_no_datasets()

    assert "0 *-Data.db" in str(excinfo.value)


def test_non_strict_skips_on_empty_datasets_dir(tmp_path, monkeypatch):
    """Negative control: without the strict flags local dev still SKIPS."""
    monkeypatch.setattr(conftest, "DATASETS", _empty_datasets_dir(tmp_path))
    _non_strict(monkeypatch)

    with pytest.raises(Skipped):
        conftest.skip_if_no_datasets()


def test_strict_passes_when_data_db_present(tmp_path, monkeypatch):
    """Negative control: strict mode + a *-Data.db present => no raise."""
    datasets = _empty_datasets_dir(tmp_path)
    (datasets / "test_basic" / "nb-1-big-Data.db").write_bytes(b"")
    monkeypatch.setattr(conftest, "DATASETS", datasets)
    _strict(monkeypatch)

    assert conftest._count_data_db() == 1

    conftest.skip_if_no_datasets()  # must not raise Failed or Skipped


def test_strict_fails_when_data_db_is_a_directory(tmp_path, monkeypatch):
    """A DIRECTORY named ``*-Data.db`` is not an SSTable binary.

    ``Path.glob`` yields directories as well as files, so an unfiltered count
    lets a placeholder dir satisfy strict mode with zero real fixtures -- the
    same false-green class this guard exists to close.
    """
    datasets = _empty_datasets_dir(tmp_path)
    (datasets / "test_basic" / "nb-1-big-Data.db").mkdir()
    monkeypatch.setattr(conftest, "DATASETS", datasets)
    _strict(monkeypatch)

    assert conftest._count_data_db() == 0

    with pytest.raises(Failed) as excinfo:
        conftest.skip_if_no_datasets()

    assert "0 *-Data.db" in str(excinfo.value)
