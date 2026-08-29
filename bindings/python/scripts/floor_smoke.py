#!/usr/bin/env python3
"""Floor smoke for the CQLite Python bindings (issue #1459).

`pyproject.toml` advertises `requires-python = ">=3.9"`, but CI only ever ran
3.12 — so the advertised floor (and the top of the supported range) was never
executed. This script is what the `smoke-floors` job in
`.github/workflows/python-ci.yml` runs on each advertised boundary interpreter
against the *already-built* abi3 wheel. It deliberately does NOT rebuild: the
wheel is `abi3-py39` (`bindings/python/Cargo.toml`), so ONE wheel must install
and import on every interpreter we claim to support. If it does not, the claim
is false and this job is the thing that says so.

Two checks, in order of increasing strength:

1. Import + `version()`. Catches an abi3 packaging break, a wheel-tag mistake,
   and any 3.9-incompatible syntax in the pure-Python layer
   (`python/cqlite/__init__.py`), which is imported eagerly.
2. One real query against the canonical corpus. Catches the far more
   interesting case where the module imports but the extension misbehaves on
   this interpreter. A query that returns ZERO rows is a FAILURE (exit 1), not
   a pass — a silently-empty result is exactly the shape of the bug this is
   meant to catch (see CLAUDE.md, "never let a dataset-dependent test pass on
   an empty dataset").

Fixture handling is TWO-MODE, because "the fixtures were missing" and "the
query ran and was fine" must never reach the same verdict:

  - ``CQLITE_FLOOR_STRICT_FIXTURES=1`` (set by CI, which restores the corpus
    first): an absent corpus/schema is a FAILURE. Otherwise a broken restore
    step would let this job report a green floor having never executed the query
    it exists to guarantee — the permissive-branch shape CLAUDE.md forbids ("a
    positive verdict requires an affirmative measurement").
  - unset (local invocation): absent fixtures SKIP the query check loudly and
    visibly with a `::warning::` naming the path, and check 1 still governs the
    exit status, so a developer without the corpus can still smoke the wheel.

Kept syntactically compatible with Python 3.9 (the advertised floor): no
`match`, no PEP 604 `X | Y` annotations, no PEP 585 builtin generics in
evaluated positions.
"""

import os
import sys

# Deliberately module-level and eager: `import cqlite` failing IS the first
# check, and a traceback from it is a more useful CI log than anything we
# could print around it.
import cqlite

# Any table in the canonical corpus would do; `test_basic.simple_table` is the
# one the pytest suite leans on hardest, so its shape is the best understood.
SMOKE_KEYSPACE_TABLE = "test_basic.simple_table"
SMOKE_QUERY = "SELECT * FROM " + SMOKE_KEYSPACE_TABLE + " LIMIT 1"
SMOKE_SCHEMA_FILE = "basic-types.cql"


def repo_root():
    """Return the repository root, derived from this file's location.

    scripts/floor_smoke.py -> bindings/python/scripts -> bindings/python ->
    bindings -> <repo root>.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.dirname(os.path.dirname(os.path.dirname(here)))


def resolve_datasets_dir():
    """Resolve the directory holding the per-keyspace SSTable trees.

    Mirrors `bindings/python/tests/conftest.py`: `CQLITE_DATASETS_ROOT` may
    point either at the documented corpus root (the parent of `sstables/`) or
    already at `sstables/` itself.
    """
    env_root = os.environ.get("CQLITE_DATASETS_ROOT")
    if env_root:
        nested = os.path.join(env_root, "sstables")
        return nested if os.path.isdir(nested) else env_root
    return os.path.join(repo_root(), "test-data", "datasets", "sstables")


def resolve_schema_file():
    """Resolve the CQL schema for the smoke query.

    The schemas are committed source resolved checkout-relative (issue #3148);
    `CQLITE_SCHEMAS_ROOT` is an optional out-of-tree override.
    """
    env_root = os.environ.get("CQLITE_SCHEMAS_ROOT")
    root = env_root if env_root else os.path.join(repo_root(), "test-data", "schemas")
    return os.path.join(root, SMOKE_SCHEMA_FILE)


def check_import():
    """Check 1: the wheel imports and reports a version on this interpreter."""
    reported = cqlite.version()
    if not reported:
        sys.stderr.write(
            "::error::cqlite.version() returned a falsy value (%r) on %s\n"
            % (reported, sys.version.split()[0])
        )
        return False
    print(
        "import OK: cqlite %s imported by CPython %s (%s)"
        % (reported, sys.version.split()[0], cqlite.__file__)
    )
    return True


def _missing_fixture(what, where, remedy):
    """Report an absent fixture; fail under strict mode, skip loudly otherwise.

    Returns True to leave the exit status alone, False to fail it.
    """
    if os.environ.get("CQLITE_FLOOR_STRICT_FIXTURES") == "1":
        sys.stderr.write(
            "::error::floor smoke: %s at %s, and CQLITE_FLOOR_STRICT_FIXTURES=1 "
            "— the real-query check could not run, so this job cannot certify "
            "the floor it claims to test. %s\n" % (what, where, remedy)
        )
        return False
    print(
        "::warning::floor smoke: SKIPPING the real-query check — %s at %s. The "
        "import check still ran. %s" % (what, where, remedy)
    )
    return True


def check_real_query():
    """Check 2: one real query. Zero rows fails; an absent corpus skips loudly.

    Returns True to keep the process exit status at 0, False to fail it.
    """
    datasets_dir = resolve_datasets_dir()
    schema_file = resolve_schema_file()

    if not os.path.isdir(datasets_dir):
        return _missing_fixture(
            "no corpus directory",
            datasets_dir,
            "Set CQLITE_DATASETS_ROOT or run test-data/scripts/fetch-datasets.sh.",
        )
    if not os.path.isfile(schema_file):
        return _missing_fixture(
            "no schema file",
            schema_file,
            "The CQL schemas are committed source; check CQLITE_SCHEMAS_ROOT.",
        )

    print("query check: %s (corpus %s)" % (SMOKE_QUERY, datasets_dir))
    with cqlite.open(datasets_dir, schema=schema_file) as database:
        result = database.execute(SMOKE_QUERY)
        row_count = len(result.rows)

    if row_count < 1:
        sys.stderr.write(
            "::error::floor smoke: %r returned 0 rows on CPython %s. The corpus "
            "is present, so an empty result is a real failure, not a skip.\n"
            % (SMOKE_QUERY, sys.version.split()[0])
        )
        return False

    print("query OK: %d row(s) from %s" % (row_count, SMOKE_KEYSPACE_TABLE))
    return True


def main():
    print("cqlite floor smoke on %s" % sys.version.replace("\n", " "))
    ok = check_import()
    # Run the query check even when the import check reported a problem: two
    # findings in one CI log beat one.
    ok = check_real_query() and ok
    if not ok:
        return 1
    print("floor smoke PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
