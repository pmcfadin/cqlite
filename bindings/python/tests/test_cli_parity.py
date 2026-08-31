"""CLI Parity Tests for Python Bindings - Issue #319.

Validates that Python binding output matches CLI JSON output exactly.
This is part of M4 Python Bindings Epic (#321).

Test Strategy:
    1. Execute same queries via Python bindings and CLI
    2. Normalize outputs for comparison (handle type differences)
    3. Compare row data ignoring key ordering (Python dict is unordered)

Key Differences Handled:
    - Python `bytes` → CLI `"0xhex"`
    - Python `datetime` → CLI `"YYYY-MM-DD HH:MM:SS.fff+0000"`
    - Python `date` → CLI `"YYYY-MM-DD"`
    - Python `time` → `int` nanoseconds (issue #1450); the CLI's
      `"HH:MM:SS.nnnnnnnnn"` string is parsed back to nanoseconds so both
      sides compare on the exact nanosecond value (no fake-zero padding)
    - Python `UUID` → CLI `"uuid-string"`
    - Python `cqlite.Duration` → CLI `"XmoYdZns"` (exact months/days/nanos, #1450)
    - Python `IPv4Address`/`IPv6Address` → CLI `"ip-string"`
    - Python `frozenset` → CLI JSON array
    - Python `dict` (for maps) → CLI array of `{"key": k, "value": v}`
    - Python `tuple` → CLI JSON array (same canonical shape as a `list`)

Collection Identity Asymmetries (issue #1454):
    The Python and Node bindings use different host containers for CQL
    collections, so the canonical form below is deliberately *lossier* than
    either binding. The authoritative table is
    `docs/development/M4_spec.md` §5.3 "Collection Identity Semantics"; the
    3-way golden parity harness (#1455, Y1) takes its canonicalization rules
    from that table, and `TestCollectionIdentityContract` in this file asserts
    that this normalizer implements every row of it. The three asymmetries the
    canonical form has to erase:

    - `set<frozen<udt>>` → Python `list`, **not** `frozenset` (`set_to_py`'s
      `contains_udt` fallback, kept for CLI parity — #804; its original reason,
      that a UDT was an unhashable `dict`, no longer holds since #3504: a
      `cqlite.Udt` IS hashable when its field values are), while Node
      returns a JS `Set` of objects. Canonical form: a JSON array either way —
      but **NOT a sorted one**, because the Python value is indistinguishable
      from a genuine `list<T>` here, so this row's canonical form is
      order-SENSITIVE (a documented limitation, see M4_spec §5.3).
      Since #3500 the fallback triggers whenever a UDT appears ANYWHERE in the
      element subtree (`contains_udt` is a full traversal), so
      `set<frozen<list<frozen<udt>>>>`, `set<frozen<set<frozen<udt>>>>` and
      `set<frozen<tuple<frozen<udt>, int>>>` are `list`s of ordinary UDT `dict`s
      too — see `test_udt_nested_deeper_in_a_projection_position_is_unsupported`.
    - `map<k,v>` → Python `dict`, whose keys **collapse** by hash/`__eq__`
      (structurally equal non-scalar keys merge, last value wins), while a Node
      `Map` compares object keys by *reference* so equal keys stay distinct.
      Canonical form: a sorted array of `{"key": k, "value": v}`. Python map
      keys additionally arrive as the hashable projection produced by
      `value_to_hashable_key` (`list`→`tuple`, `set`→`frozenset`,
      `udt`→`cqlite.Udt` since #3504). A UDT map key therefore has the SAME host
      shape as the same UDT in value position, and canonicalizes to the UDT
      *object* Node and the CLI render — it is **supported**. It was not before
      #3504, when the arm flattened a UDT into a `frozenset` of `(name, value)`
      pairs (instances a-1/a-3, now closed). A `map` in a projection position is
      still flattened to a tuple of pairs and remains **UNSUPPORTED** (a-2).
    - `tuple<...>` → Python `tuple` but a Node `Array`, i.e. Node cannot
      distinguish `tuple<...>` from `list<T>`. Canonical form: a JSON array, so
      tuple and list normalize identically.
"""

import json
import re
import subprocess
from datetime import date, datetime, timezone
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

import cqlite

from conftest import DATASETS, SCHEMAS, PROJECT_ROOT

# ONE implementation of numeric equality, shared with test_parity.py (#3505).
from numeric_compare import (
    float_equal as _float_equal,
    is_number as _is_number,
    numbers_equal as _numbers_equal,
)


# CLI renders CQL `time` as "HH:MM:SS.nnnnnnnnn" (9-digit nanoseconds). The
# Python binding returns exact `int` nanoseconds since midnight (issue #1450),
# so we parse the CLI string back to nanoseconds for an exact comparison rather
# than padding the Python value with fake zeros.
_CLI_TIME_RE = re.compile(r"^(\d{2}):(\d{2}):(\d{2})\.(\d{9})$")


def _cli_time_to_nanos(value: str) -> int:
    """Convert a CLI `HH:MM:SS.nnnnnnnnn` time string to nanoseconds since midnight."""
    match = _CLI_TIME_RE.match(value)
    hours, minutes, seconds, nanos = (int(g) for g in match.groups())
    return (hours * 3600 + minutes * 60 + seconds) * 1_000_000_000 + nanos


# =============================================================================
# Normalization Helpers
# =============================================================================


def normalize_python_value(value: Any, is_row_level: bool = True) -> Any:
    """Convert Python types to CLI JSON-compatible format.

    This transforms Python native types to match CLI JSON output format.

    Args:
        value: The value to normalize
        is_row_level: If True, we're at the row level (dict = row dict).
                     If False, we're inside a cell value (dict = CQL map).
    """
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        # Handle NaN and Infinity
        if isinstance(value, float):
            if value != value:  # NaN check
                return "NaN"
            if value == float("inf"):
                return "Infinity"
            if value == float("-inf"):
                return "-Infinity"
        return value

    if isinstance(value, bytes):
        # CLI outputs blobs as 0x-prefixed hex strings
        return f"0x{value.hex()}"

    if isinstance(value, UUID):
        # CLI outputs UUIDs as lowercase hyphenated strings
        return str(value).lower()

    if isinstance(value, datetime):
        # CLI format: "YYYY-MM-DD HH:MM:SS.fff+0000"
        # Convert to UTC if timezone-aware
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.strftime("%Y-%m-%d %H:%M:%S.") + f"{value.microsecond // 1000:03d}+0000"

    if isinstance(value, date) and not isinstance(value, datetime):
        # CLI format: "YYYY-MM-DD"
        return value.strftime("%Y-%m-%d")

    # CQL `time` now decodes to exact `int` nanoseconds (issue #1450); it is
    # handled by the int/float branch above and compared against the CLI time
    # string parsed to nanoseconds in `normalize_cli_value`.

    if isinstance(value, cqlite.Duration):
        # CLI format: "XmoYdZns" — matches cqlite ValueFormatter::format_duration.
        # The exact months/days/nanos components are preserved (no 30-day
        # collapse, no nanosecond truncation).
        parts = []
        if value.months != 0:
            parts.append(f"{value.months}mo")
        if value.days != 0:
            parts.append(f"{value.days}d")
        if value.nanos != 0:
            parts.append(f"{value.nanos}ns")
        return "".join(parts) if parts else "0ns"

    if isinstance(value, Decimal):
        # CLI outputs decimals as string representation
        return str(value)

    if isinstance(value, (IPv4Address, IPv6Address)):
        # CLI outputs IP addresses as strings
        return str(value)

    if isinstance(value, frozenset):
        # CLI outputs sets as JSON arrays (sorted for determinism).
        #
        # Issue #1454 / M4_spec §5.3: this branch used to have TWO sources —
        # `set<scalar>` (and `set<frozen<list|set|map>>`, whose elements stay
        # hashable via `value_to_hashable_key`), AND a UDT used as a MAP KEY, which
        # the old projection flattened into a `frozenset` of `(field_name, value)`
        # pairs including `_type`/`_keyspace`. That second source canonicalized to
        # a sorted array of `[name, value]` pairs instead of the UDT object the CLI
        # renders — instances a-1/a-3.
        #
        # Since #3504 the projection emits a `cqlite.Udt`, so a projected UDT is
        # handled by the UDT branch above and canonicalizes to the same object the
        # CLI produces. Only the `set<scalar>`/`set<frozen<collection>>` source
        # reaches here now. The `map`-in-projection divergence (a-2) is untouched:
        # `value_to_hashable_key`'s `Map` arm still flattens to a tuple of pairs.
        return sorted([normalize_python_value(v, is_row_level=False) for v in value], key=_sort_key)

    if isinstance(value, list):
        # `list<T>` and — per #1454 — `set<frozen<udt>>`, which `set_to_py`
        # returns as a Python `list` for CLI parity (#804; the original reason,
        # that a UDT `dict` was unhashable, no longer holds — a `cqlite.Udt` is
        # hashable — but the shape is unchanged). Both canonicalize to a JSON array;
        # `frozen<T>` is already unwrapped by the binding, so it needs no branch.
        return [normalize_python_value(v, is_row_level=False) for v in value]

    if isinstance(value, tuple):
        # `tuple<...>` → JSON array, the same canonical shape as `list<T>`,
        # because Node returns an `Array` for both and cannot tell them apart
        # (#1454; `Value::Tuple` delegates to `list_to_array`).
        return [normalize_python_value(v, is_row_level=False) for v in value]

    if isinstance(value, cqlite.Udt):
        # UDT — recognised STRUCTURALLY (#3504). The type identity arrives out of
        # band on a `cqlite.Udt`, so no content sniff is involved and no field name
        # can affect the classification. Canonical form is the DECLARED FIELDS AND
        # NOTHING ELSE: `{**fields}`.
        #
        # JUSTIFIED AGAINST THE GOLDEN, NOT AGAINST THE CLI'S PREVIOUS OUTPUT.
        # `cassandra-5.0.8`'s `UserType.toJSONString` writes `{"field": value, …}`
        # and emits NO type key, and the committed `sstabledump` golden for
        # `test-data/fixtures/issue_3504/` shows exactly that (the non-colliding
        # `p` cell dumps as `{"label": …, "real_field": 7}`). The CLI used to
        # inject `"_type"` ahead of the fields and this rule mirrored it; issue
        # #3629 removed the injection from both JSON renderers, so the canonical
        # form is now the reference tool's shape rather than a CQLite invention.
        #
        # The old `_keyspace` filter is GONE, and its removal is the observable
        # half of #3504. The binding used to INJECT `_keyspace` into the field
        # namespace, and this rule dropped that key because the CLI omits it for
        # UDTs — which silently discarded a genuine FIELD of that name. Nothing is
        # injected now, so every entry in `.fields` is a real field.
        #
        # CONSEQUENCE, recorded rather than worked around: `--format json` carries
        # no type channel at all, so two UDTs of DIFFERENT declared types with
        # identical field values canonicalize IDENTICALLY. That is true of
        # `sstabledump` too. The binding-side identity is unaffected:
        # `value.type_name`/`value.keyspace` are read from the instance, never
        # from the fields.
        return {
            str(k): normalize_python_value(v, is_row_level=False)
            for k, v in value.fields.items()
        }

    if isinstance(value, dict):
        # THE CALLER'S EXPLICIT SIGNAL BEATS SNIFFING THE CONTENT (#1454).
        # `is_row_level=True` is passed only by a caller that KNOWS it holds a
        # row (every such call site normalizes `row.to_dict()`), and a UDT is
        # always a CELL, so it can only ever arrive with `is_row_level=False`.
        # Checking the signal first therefore leaves the UDT branch untouched
        # while removing a real misclassification: `"_type"` and `"_keyspace"`
        # are legal (quoted) COLUMN names, and sniffing `"_type"` first made such
        # a row normalize as a UDT — silently DROPPING its `"_keyspace"` column.
        # At cell level no such signal exists (a `map<text,X>` and a UDT are both
        # a `dict`), which is why the ambiguity is fixed here but remains a
        # documented limitation there: M4_spec §5.3 instance b-2, tracked by #3497.
        if is_row_level:
            # This is a row dict - keep as dict, recurse into cell values
            return {str(k): normalize_python_value(v, is_row_level=False) for k, v in value.items()}

        # Cell level from here on.
        #
        # NO REAL UDT REACHES HERE ANY MORE (#3504): production emits a
        # `cqlite.Udt`, handled structurally above. What remains is exactly
        # LIMITATION b-2's cell-level site plus b-5: a `map<text,X>` — or a JSON
        # object — that happens to carry a literal `"_type"` key is still
        # canonicalized as an object rather than the documented `{key,value}`
        # array, with a `"_keyspace"` entry dropped. (Since #3629 that object is
        # not even the CLI's UDT shape any more, which renders declared fields
        # only — one more reason the sniff is a recorded GAP and not a rule.)
        # The sniff is KEPT rather than deleted because that is the behaviour those
        # two recorded gaps pin; it is no longer a UDT classifier, it is the
        # map/JSON misclassification itself. Requiring `_keyspace` too would only
        # pick a rarer delimiter on an ambiguous channel. The real fix is the
        # declared CQL type (#3497) — for which #3504 has now supplied the missing
        # structural signal (`isinstance(v, cqlite.Udt)`), so the remaining work is
        # typing the MAP side, not distinguishing a UDT.
        if "_type" in value:
            filtered = {k: v for k, v in value.items() if k != "_keyspace"}
            return {k: normalize_python_value(v, is_row_level=False) for k, v in filtered.items()}

        # This is a CQL map inside a cell - CLI outputs ALL maps as array of {"key": k, "value": v}
        # Sort by key for determinism (like sets) - Issue #336.
        # #1454: the `dict` has already collapsed structurally-equal keys
        # (last value wins); a Node `Map` would have kept equal OBJECT keys
        # distinct. Well-formed Cassandra data has no duplicate map keys, so
        # the canonical form is identical in practice — but it is why the
        # canonical form is a sorted array rather than a host map (instance b-3).
        return sorted(
            [{"key": normalize_python_value(k, is_row_level=False), "value": normalize_python_value(v, is_row_level=False)}
             for k, v in value.items()],
            key=lambda x: _sort_key(x["key"])
        )

    if isinstance(value, str):
        return value

    # Fallback: convert to string
    return str(value)


def _sort_key(value: Any) -> tuple:
    """Generate a sort key for heterogeneous values."""
    if value is None:
        return (0, "")
    if isinstance(value, bool):
        return (1, str(value))
    if isinstance(value, (int, float)):
        return (2, value)
    if isinstance(value, str):
        return (3, value)
    return (4, str(value))


def normalize_cli_value(value: Any) -> Any:
    """Normalize CLI JSON value for comparison.

    CLI JSON uses different representations for maps:
    - Maps with non-string keys: array of {"key": k, "value": v}
    - Maps with string keys: regular JSON object {"key1": v1, "key2": v2}

    We need to preserve the structure but recurse into nested values.
    """
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        return value

    if isinstance(value, str):
        # CQL `time` renders as "HH:MM:SS.nnnnnnnnn"; parse to exact nanoseconds
        # to match the Python binding's `int` nanoseconds (issue #1450).
        if _CLI_TIME_RE.match(value):
            return _cli_time_to_nanos(value)
        return value

    if isinstance(value, list):
        # Check if this is a map representation (array of {key, value} objects)
        if value and all(
            isinstance(item, dict) and set(item.keys()) == {"key", "value"}
            for item in value
        ):
            # This is a map with non-string keys - sort by key for determinism (like sets) - Issue #336
            return sorted(
                [{"key": normalize_cli_value(item["key"]), "value": normalize_cli_value(item["value"])}
                 for item in value],
                key=lambda x: _sort_key(x["key"])
            )
        # Regular list
        return [normalize_cli_value(v) for v in value]

    if isinstance(value, dict):
        # This is either a row dict, UDT, or a map with string keys
        return {k: normalize_cli_value(v) for k, v in value.items()}

    return value


def rows_equal(py_rows: list[dict], cli_rows: list[dict], strict_columns: bool = False) -> tuple[bool, str]:
    """Compare normalized row lists.

    Returns (is_equal, error_message).

    Args:
        py_rows: Rows from Python bindings
        cli_rows: Rows from CLI
        strict_columns: If True, require exact column match. If False (default),
                       only compare columns that exist in both outputs.
                       (Python may omit null columns, CLI always includes them)
    """
    if len(py_rows) != len(cli_rows):
        return False, f"Row count mismatch: Python={len(py_rows)}, CLI={len(cli_rows)}"

    for i, (py_row, cli_row) in enumerate(zip(py_rows, cli_rows)):
        py_keys = set(py_row.keys())
        cli_keys = set(cli_row.keys())

        if strict_columns:
            if py_keys != cli_keys:
                return False, f"Row {i}: Column mismatch - Python={py_keys}, CLI={cli_keys}"
            keys_to_compare = py_keys
        else:
            # Compare only columns that exist in both
            # Python may not include null columns
            keys_to_compare = py_keys & cli_keys

            # But Python shouldn't have columns that CLI doesn't have
            extra_py_keys = py_keys - cli_keys
            if extra_py_keys:
                return False, f"Row {i}: Python has extra columns not in CLI: {extra_py_keys}"

        # Compare values for common columns
        for key in keys_to_compare:
            py_val = py_row[key]
            cli_val = cli_row[key]

            if not values_equal(py_val, cli_val):
                return False, (
                    f"Row {i}, column '{key}': Value mismatch\n"
                    f"  Python: {py_val!r} ({type(py_val).__name__})\n"
                    f"  CLI:    {cli_val!r} ({type(cli_val).__name__})"
                )

    return True, ""


def values_equal(py_val: Any, cli_val: Any) -> bool:
    """Compare two normalized values with tolerance for floating point.

    For list values, first tries ordered comparison, then unordered (sorted)
    comparison.  This handles CQL SET columns that get serialised to lists in
    different orderings by Python (sorted with _sort_key) versus the CLI
    (which follows Cassandra's internal byte-order for the element type).
    """
    if py_val is None and cli_val is None:
        return True

    if type(py_val) != type(cli_val):
        # Allow int/float comparison, but NEVER at the cost of hiding precision
        # loss (issue #3505).  `bool` is excluded because `isinstance(True, int)`
        # is `True`, so `True` vs `1.0` used to coerce equal — a genuine type
        # mismatch a parity harness must report.
        if _is_number(py_val) and _is_number(cli_val):
            return _numbers_equal(py_val, cli_val)  # exact above 2**53
        return False

    if isinstance(py_val, float):
        return _float_equal(py_val, cli_val)

    if isinstance(py_val, dict):
        if set(py_val.keys()) != set(cli_val.keys()):
            return False
        return all(values_equal(py_val[k], cli_val[k]) for k in py_val)

    if isinstance(py_val, list):
        if len(py_val) != len(cli_val):
            return False
        # Ordered comparison first (correct for CQL LIST and map-repr arrays)
        if all(values_equal(pv, cv) for pv, cv in zip(py_val, cli_val)):
            return True
        # Unordered fallback for CQL SET columns that Python sorted differently
        # from the CLI.  Only applicable to lists of non-dict primitives (dicts
        # are map-repr arrays where order is already normalised by both sides).
        if not any(isinstance(v, dict) for v in py_val):
            try:
                py_sorted = sorted(py_val, key=lambda x: _sort_key(x))
                cli_sorted = sorted(cli_val, key=lambda x: _sort_key(x))
                return all(values_equal(pv, cv) for pv, cv in zip(py_sorted, cli_sorted))
            except TypeError:
                pass
        return False

    return py_val == cli_val


# =============================================================================
# CLI Execution
# =============================================================================


def run_cli_query(
    data_dir: Path, schema: Path, query: str, cli_binary: Path, timeout: int = 60
) -> list[dict]:
    """Run a query via the CLI and return parsed JSON rows.

    Uses pre-built binary instead of `cargo run` for performance (Issue #331).

    Args:
        data_dir: Path to the SSTable data directory.
        schema: Path to the CQL schema file.
        query: CQL query string.
        cli_binary: Path to the pre-built cqlite-cli binary.
        timeout: Timeout in seconds for CLI execution.

    Returns:
        List of row dictionaries parsed from JSON output.
    """
    cmd = [
        str(cli_binary),
        "--data-dir",
        str(data_dir),
        "--schema",
        str(schema),
        "--query",
        query,
        "--out",
        "json",
        # Override the CLI's default 1000-row cap so unlimited queries (no CQL
        # LIMIT) return all rows, matching Python binding behaviour.
        "--limit",
        "100000",
    ]

    try:
        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"CLI command failed with exit code {result.returncode}\n"
                f"Command: {' '.join(cmd)}\n"
                f"stderr: {result.stderr}\n"
                f"stdout: {result.stdout}"
            )

        # Parse JSON output
        output = result.stdout.strip()
        if not output:
            return []

        return json.loads(output)

    except subprocess.TimeoutExpired:
        raise RuntimeError(f"CLI command timed out after {timeout}s")
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse CLI JSON output: {e}\nOutput: {result.stdout}")


# =============================================================================
# Pytest Fixtures
# =============================================================================


# check_prerequisites fixture is provided by conftest.py


# CLI parity tests need (database, schema_file) tuples so they can invoke CLI
# with the same schema. We define local aliases that add the schema_file.
from conftest import (
    SCHEMA_BASIC_TYPES,
    SCHEMA_COLLECTIONS,
    SCHEMA_TIME_SERIES,
    SCHEMA_WIDE_ROWS,
    require_test_data,
)


@pytest.fixture(scope="module")
def db_basic(check_prerequisites):
    """Database fixture with basic-types schema - returns (db, schema_file) tuple."""
    require_test_data(SCHEMA_BASIC_TYPES)
    with cqlite.open(DATASETS, schema=SCHEMA_BASIC_TYPES) as database:
        yield database, SCHEMA_BASIC_TYPES


@pytest.fixture(scope="module")
def db_collections(check_prerequisites):
    """Database fixture with collections schema - returns (db, schema_file) tuple."""
    require_test_data(SCHEMA_COLLECTIONS)
    with cqlite.open(DATASETS, schema=SCHEMA_COLLECTIONS) as database:
        yield database, SCHEMA_COLLECTIONS


@pytest.fixture(scope="module")
def db_timeseries(check_prerequisites):
    """Database fixture with time-series schema - returns (db, schema_file) tuple."""
    require_test_data(SCHEMA_TIME_SERIES)
    with cqlite.open(DATASETS, schema=SCHEMA_TIME_SERIES) as database:
        yield database, SCHEMA_TIME_SERIES


@pytest.fixture(scope="module")
def db_wide_rows(check_prerequisites):
    """Database fixture with wide-rows schema - returns (db, schema_file) tuple."""
    require_test_data(SCHEMA_WIDE_ROWS)
    with cqlite.open(DATASETS, schema=SCHEMA_WIDE_ROWS) as database:
        yield database, SCHEMA_WIDE_ROWS


# =============================================================================
# Test Classes
# =============================================================================


@pytest.mark.slow
class TestCLIParityBasic:
    """CLI parity tests for test_basic keyspace."""

    @pytest.mark.parametrize(
        "table,limit",
        [
            ("simple_table", 10),
            ("composite_key_table", 10),
            ("multi_partition_table", 10),
            ("uncompressed_table", 10),
            ("compression_test_table", 10),
            # NOTE (issue #1935): the second tuple element is the query LIMIT, not
            # an expected row count. `ttl_test_table` KEEPS its TTL (the #1853
            # seam) so every fixture row is wall-clock-expired and BOTH the Python
            # binding and the CLI return 0 LIVE rows — this test asserts Python==CLI
            # equality, which holds at 0 (and will still hold post-regen). It never
            # hardcodes a row count.
            ("ttl_test_table", 10),
            ("static_columns_table", 10),  # Issue resolved
        ],
    )
    def test_basic_table_parity(self, db_basic, cli_binary, table: str, limit: int):
        """Verify Python and CLI produce identical output for basic tables."""
        db, schema_file = db_basic
        query = f"SELECT * FROM test_basic.{table} LIMIT {limit}"

        # Get Python result
        py_result = db.execute(query)
        py_rows = [normalize_python_value(row.to_dict()) for row in py_result]

        # Get CLI result
        cli_rows = run_cli_query(DATASETS, schema_file, query, cli_binary)
        cli_rows = [normalize_cli_value(row) for row in cli_rows]

        # Compare
        is_equal, error_msg = rows_equal(py_rows, cli_rows)
        assert is_equal, error_msg

    def test_counters_parity(self, db_basic, cli_binary):
        """Verify counter table parity."""
        db, schema_file = db_basic
        query = "SELECT * FROM test_basic.counters"

        py_result = db.execute(query)
        py_rows = [normalize_python_value(row.to_dict()) for row in py_result]

        cli_rows = run_cli_query(DATASETS, schema_file, query, cli_binary)
        cli_rows = [normalize_cli_value(row) for row in cli_rows]

        is_equal, error_msg = rows_equal(py_rows, cli_rows)
        assert is_equal, error_msg


@pytest.mark.slow
class TestCLIParityCollections:
    """CLI parity tests for test_collections keyspace."""

    @pytest.mark.parametrize(
        "table",
        [
            "collection_table",
            "collection_clustering_table",
            "collections_with_udts",
            "frozen_collections_table",  # Issue resolved
            "empty_collections_table",
            "large_collections_table",
            "nested_collections_table",
            "typed_collections_table",  # Issue resolved
        ],
    )
    def test_collection_table_parity(self, db_collections, cli_binary, table: str):
        """Verify Python and CLI produce identical output for collection tables."""
        db, schema_file = db_collections
        query = f"SELECT * FROM test_collections.{table}"

        py_result = db.execute(query)
        py_rows = [normalize_python_value(row.to_dict()) for row in py_result]

        cli_rows = run_cli_query(DATASETS, schema_file, query, cli_binary)
        cli_rows = [normalize_cli_value(row) for row in cli_rows]

        is_equal, error_msg = rows_equal(py_rows, cli_rows)
        assert is_equal, error_msg


@pytest.mark.slow
class TestCLIParityTimeseries:
    """CLI parity tests for test_timeseries keyspace."""

    @pytest.mark.parametrize(
        "table,limit",
        [
            ("sensor_data", 10),
            ("event_store", 10),
            ("user_sessions", 10),
            # NOTE (issue #1935): the second tuple element is the query LIMIT, not
            # an expected row count. `app_metrics`, `log_entries` and `tick_data`
            # had `default_time_to_live` REMOVED from the schema; until the corpus
            # binaries are regenerated WITHOUT TTL (CI-owned), the shipped fixtures
            # are wall-clock-expired so BOTH Python and CLI return 0 LIVE rows. This
            # test asserts Python==CLI equality, which holds at 0 today and will
            # still hold once the regenerated fixtures return their physical rows.
            ("app_metrics", 10),
            ("log_entries", 10),
            ("stock_prices", 10),
            ("tick_data", 10),
            ("time_bucketed_counters", 10),
            ("user_activity", 10),
        ],
    )
    def test_timeseries_table_parity(self, db_timeseries, cli_binary, table: str, limit: int):
        """Verify Python and CLI produce identical output for timeseries tables."""
        db, schema_file = db_timeseries
        query = f"SELECT * FROM test_timeseries.{table} LIMIT {limit}"

        py_result = db.execute(query)
        py_rows = [normalize_python_value(row.to_dict()) for row in py_result]

        cli_rows = run_cli_query(DATASETS, schema_file, query, cli_binary)
        cli_rows = [normalize_cli_value(row) for row in cli_rows]

        is_equal, error_msg = rows_equal(py_rows, cli_rows)
        assert is_equal, error_msg


@pytest.mark.slow
class TestCLIParityWideRows:
    """CLI parity tests for test_wide_rows keyspace."""

    @pytest.mark.parametrize(
        "table,limit",
        [
            ("wide_partition_table", 10),
            ("chat_messages", 10),
            ("document_versions", 10),
            ("large_blob_table", 10),
            ("many_columns_table", 10),
            ("multi_metric_timeseries", 10),
            ("product_catalog", 10),
            ("sparse_data_table", 10),
        ],
    )
    def test_wide_rows_table_parity(self, db_wide_rows, cli_binary, table: str, limit: int):
        """Verify Python and CLI produce identical output for wide row tables."""
        db, schema_file = db_wide_rows
        query = f"SELECT * FROM test_wide_rows.{table} LIMIT {limit}"

        py_result = db.execute(query)
        py_rows = [normalize_python_value(row.to_dict()) for row in py_result]

        cli_rows = run_cli_query(DATASETS, schema_file, query, cli_binary)
        cli_rows = [normalize_cli_value(row) for row in cli_rows]

        is_equal, error_msg = rows_equal(py_rows, cli_rows)
        assert is_equal, error_msg


# =============================================================================
# Integration Tests (from Issue #319 spec)
# =============================================================================


@pytest.mark.slow
class TestIssue319Spec:
    """Tests specified in Issue #319 acceptance criteria."""

    def test_basic_select(self, db_basic, cli_binary):
        """Test: SELECT * FROM test_basic.simple_table LIMIT 10"""
        db, schema_file = db_basic
        query = "SELECT * FROM test_basic.simple_table LIMIT 10"

        py_result = db.execute(query)
        py_rows = [normalize_python_value(row.to_dict()) for row in py_result]

        cli_rows = run_cli_query(DATASETS, schema_file, query, cli_binary)
        cli_rows = [normalize_cli_value(row) for row in cli_rows]

        is_equal, error_msg = rows_equal(py_rows, cli_rows)
        assert is_equal, f"Basic select parity failed: {error_msg}"

    def test_with_collections(self, db_collections, cli_binary):
        """Test: SELECT * FROM test_collections.collection_table"""
        db, schema_file = db_collections
        query = "SELECT * FROM test_collections.collection_table"

        py_result = db.execute(query)
        py_rows = [normalize_python_value(row.to_dict()) for row in py_result]

        cli_rows = run_cli_query(DATASETS, schema_file, query, cli_binary)
        cli_rows = [normalize_cli_value(row) for row in cli_rows]

        is_equal, error_msg = rows_equal(py_rows, cli_rows)
        assert is_equal, f"Collection table parity failed: {error_msg}"

    def test_timeseries_data(self, db_timeseries, cli_binary):
        """Test: SELECT * FROM test_timeseries.sensor_data LIMIT 10"""
        db, schema_file = db_timeseries
        query = "SELECT * FROM test_timeseries.sensor_data LIMIT 10"

        py_result = db.execute(query)
        py_rows = [normalize_python_value(row.to_dict()) for row in py_result]

        cli_rows = run_cli_query(DATASETS, schema_file, query, cli_binary)
        cli_rows = [normalize_cli_value(row) for row in cli_rows]

        is_equal, error_msg = rows_equal(py_rows, cli_rows)
        assert is_equal, f"Timeseries data parity failed: {error_msg}"

    def test_wide_partition(self, db_wide_rows, cli_binary):
        """Test: SELECT * FROM test_wide_rows.wide_partition_table LIMIT 10"""
        db, schema_file = db_wide_rows
        query = "SELECT * FROM test_wide_rows.wide_partition_table LIMIT 10"

        py_result = db.execute(query)
        py_rows = [normalize_python_value(row.to_dict()) for row in py_result]

        cli_rows = run_cli_query(DATASETS, schema_file, query, cli_binary)
        cli_rows = [normalize_cli_value(row) for row in cli_rows]

        is_equal, error_msg = rows_equal(py_rows, cli_rows)
        assert is_equal, f"Wide partition parity failed: {error_msg}"


@pytest.mark.slow
class TestColumnOrdering:
    """Tests for column ordering consistency."""

    def test_column_count_matches(self, db_basic, cli_binary):
        """Verify Python and CLI return same number of columns."""
        db, schema_file = db_basic
        query = "SELECT * FROM test_basic.simple_table LIMIT 1"

        py_result = db.execute(query)
        if len(py_result.rows) == 0:
            pytest.skip("No rows returned")

        py_row = py_result.rows[0].to_dict()
        py_cols = set(py_row.keys())

        cli_rows = run_cli_query(DATASETS, schema_file, query, cli_binary)
        if not cli_rows:
            pytest.skip("No CLI rows returned")

        cli_cols = set(cli_rows[0].keys())

        assert py_cols == cli_cols, f"Column mismatch: Python={py_cols}, CLI={cli_cols}"

    def test_columns_metadata_available(self, db_basic, cli_binary):
        """Verify column metadata is available in Python result."""
        db, schema_file = db_basic
        query = "SELECT * FROM test_basic.simple_table LIMIT 1"

        py_result = db.execute(query)
        assert hasattr(py_result, "columns"), "QueryResult should have columns attribute"
        assert len(py_result.columns) > 0, "Should have column metadata"

        # Verify each column has required attributes
        for col in py_result.columns:
            assert hasattr(col, "name"), "Column should have name"
            assert hasattr(col, "data_type"), "Column should have data_type"


# =============================================================================
# Row Count Parity Tests
# =============================================================================


@pytest.mark.slow
class TestRowCountParity:
    """Verify row counts match between Python and CLI."""

    # Known row count discrepancies (core library issues)
    # Note: As issues are resolved, tests are updated to remove xfail markers
    KNOWN_ROW_COUNT_ISSUES = {
        # All previously known issues have been resolved
    }

    @pytest.mark.parametrize(
        "keyspace,table,fixture",
        [
            ("test_basic", "simple_table", "db_basic"),
            ("test_basic", "composite_key_table", "db_basic"),
            ("test_collections", "collection_table", "db_collections"),
            ("test_timeseries", "sensor_data", "db_timeseries"),  # Issue resolved
            ("test_wide_rows", "wide_partition_table", "db_wide_rows"),
        ],
    )
    def test_row_count_matches(self, keyspace: str, table: str, fixture: str, cli_binary, request):
        """Verify Python and CLI return same row count."""
        db_fixture = request.getfixturevalue(fixture)
        db, schema_file = db_fixture
        query = f"SELECT * FROM {keyspace}.{table}"

        py_result = db.execute(query)
        py_count = len(py_result.rows)

        cli_rows = run_cli_query(DATASETS, schema_file, query, cli_binary)
        cli_count = len(cli_rows)

        assert py_count == cli_count, (
            f"Row count mismatch for {keyspace}.{table}: "
            f"Python={py_count}, CLI={cli_count}"
        )


# =============================================================================
# Collection Identity Contract (Issue #1454)
# =============================================================================


# A UDT as the Python binding renders it (`udt_to_py`): a `cqlite.Udt` carrying
# the type identity OUT OF BAND, with the named fields in `.fields` and nothing
# else (issue #3504). Before that change this was a flat dict seeded with
# `_type`/`_keyspace`, so a field of either name overwrote the marker.
def _udt(type_name: str, **fields: Any) -> cqlite.Udt:
    return cqlite.Udt(type_name, "test_collections", dict(fields))


class TestCollectionIdentityContract:
    """The collection-identity table of `docs/development/M4_spec.md` §5.3, executable.

    Issue #1454. Each test asserts that `normalize_python_value` produces the
    canonical shape documented for one row of that table, so the contract the
    3-way golden parity harness (#1455, Y1) consumes is verified rather than
    aspirational.

    The tests named `LIMITATION <id>` pin the cases §5.3 records as NOT
    canonicalizable — including one (b-4) that lives in the COMPARISON layer
    (`values_equal`) rather than the normalizer — family (a), lossy projection through
    `value_to_hashable_key`, which discards the CQL type; and family (b), two CQL
    types arriving as the same Python host shape. The two FAMILIES are closed;
    the list of INSTANCES is **not** — the families are generative, and nesting
    multiplies them, so these tests are a floor and not a ceiling. They record
    each divergent shape as a GAP, never as a desirable canonical form. Closing
    any of them requires the declared CQL type threaded into normalization, i.e.
    schema-aware normalization: a behavior change, out of scope for #1454,
    tracked as #3497.

    Two instances have since been CLOSED, and by a different route than the one
    predicted above. #3504 made `value_to_hashable_key` project a UDT to a
    `cqlite.Udt` instead of flattening it into a `frozenset` of pairs, so the
    projection is type-PRESERVING and family (a)'s UDT instances (**a-1**, **a-3**)
    canonicalize to the same UDT object Node and the CLI produce — no declared type
    was needed, because the loss was in the projection rather than in the
    normalizer. **a-2** (a `map` in a projection position) is untouched and remains
    the family's live instance. The site-"UDT fields" half of **b-2** is closed the
    same way: the markers are no longer injected into the field namespace at all.

    Pinned ELSEWHERE, and no longer a crash: the nested shapes that used to raise
    `TypeError: unhashable type` inside the binding
    (`set<frozen<tuple<frozen<udt>, int>>>`, `set<frozen<set<frozen<udt>>>>`).
    `contains_udt` and `value_to_hashable_key` are now TOTAL and exhaustive over
    `Value` (#3500), so those columns read successfully; they are pinned
    end-to-end against the real fixture in
    `bindings/python/tests/test_nested_udt_hashable.py`, which is the right level
    — the defect lived in `value.rs`, so a pure normalizer test could neither
    observe it nor certify its fix. What remains observable HERE is the shape
    those fixes produce, asserted below.

    That core-side gap is CLOSED: a MULTICELL map's composite key used to decode
    as an opaque `Blob` because `parse_cell_path_key` had a scalar-only allowlist
    (#3612). It now delegates to the structural decoder, so the multicell and
    frozen spellings of one map decode their keys the same way. The residual is
    narrower and tracked separately — a nested element's declared width is not
    checked exactly (#3723), which only bites on input Cassandra itself refuses
    to read.

    These tests are intentionally pure: they feed the normalizer the host values
    the Python binding is documented to produce (`bindings/python/src/value.rs`:
    `list_to_py`, `set_to_py`, `map_to_py`, `tuple_to_py`; and
    `bindings/python/src/value_hashable.rs`: `value_to_hashable_key`) and need no
    dataset and no query, so a failure here
    is unambiguously a contract violation and never a fixture problem.
    """

    def test_list_of_scalars_is_an_array(self):
        """`list<T>` → Python `list` → JSON array, order preserved."""
        assert normalize_python_value(["b", "a", "c"], is_row_level=False) == ["b", "a", "c"]

    def test_set_of_scalars_is_a_sorted_array(self):
        """`set<scalar>` → Python `frozenset` → **sorted** JSON array.

        Sorting is required: `frozenset` iteration is hash-ordered while a JS
        `Set` is insertion-ordered, so neither side's order may be asserted.
        """
        assert normalize_python_value(frozenset({3, 1, 2}), is_row_level=False) == [1, 2, 3]
        assert normalize_python_value(frozenset({"b", "a"}), is_row_level=False) == ["a", "b"]

    def test_set_of_hashable_collections_stays_a_frozenset(self):
        """`set<frozen<list<int>>>` → `frozenset` of tuples → sorted array of arrays.

        The `list` fallback in `set_to_py` triggers on UDTs only, so a nested
        collection with **no UDT anywhere inside it** still arrives as a
        frozenset. That scope matters post-#3500: `contains_udt` is now a FULL
        subtree traversal, so the fallback catches a UDT at any depth — the claim
        is "no UDT in the subtree", not "the outermost element is a collection"
        (an earlier wording said the latter, which was false the moment a UDT sat
        under the nested collection).
        """
        normalized = normalize_python_value(frozenset({(1, 2), (3,)}), is_row_level=False)
        assert sorted(normalized, key=_sort_key) == normalized
        assert sorted(normalized, key=len) == [[3], [1, 2]]

    def test_set_of_frozen_udt_is_a_list_not_a_frozenset(self):
        """`set<frozen<udt>>` → Python **`list`** (asymmetry row 1).

        `set_to_py` returns a `list` for CLI parity (#804); Node keeps a JS `Set`
        of objects. Canonical form on both sides is an array of UDT objects
        holding the declared fields and nothing else — the CLI's UDT JSON shape
        since #3629, and `UserType.toJSONString`'s shape all along. Since #3504
        the keyspace is not in the field namespace at all, so there is no
        `_keyspace` entry to drop either.
        """
        value = [_udt("address", street="1 Main St"), _udt("address", street="2 Oak Ave")]
        assert normalize_python_value(value, is_row_level=False) == [
            {"street": "1 Main St"},
            {"street": "2 Oak Ave"},
        ]

    def test_set_of_frozen_udt_canonical_form_is_order_sensitive(self):
        """LIMITATION b-1 (host-shape collision): `set<frozen<udt>>` is NOT sorted.

        Two structurally-equal UDT sets whose elements arrive in different orders
        normalize to DIFFERENT arrays and compare unequal. This is a documented
        gap (#1454, M4_spec §5.3 "EXCEPTION"), not a bug to fix here: the value
        reaches the normalizer as a plain Python `list` — the same host shape as
        a genuine `list<T>`, whose order is semantically meaningful — so sorting
        it would corrupt every `list<T>`. Distinguishing the two needs the
        declared CQL type, i.e. schema-aware normalization, which is a behavior
        change and out of scope for #1454 (tracked as #3497).

        #1455 must handle this row explicitly (compare it order-insensitively
        itself, or declare it unsupported); it may not assume the canonical form
        has erased set ordering.
        """
        a = [_udt("address", street="1 Main St"), _udt("address", street="2 Oak Ave")]
        b = list(reversed(a))
        na = normalize_python_value(a, is_row_level=False)
        nb = normalize_python_value(b, is_row_level=False)
        assert na != nb, (
            "order-sensitivity is the documented limitation; if this now passes "
            "order-insensitively the canonicalization rules in M4_spec §5.3 must be updated"
        )
        # ...and the ONLY difference is element order: the sets are equal as sets.
        assert sorted(na, key=_sort_key) == sorted(nb, key=_sort_key)

    def test_set_of_scalars_order_insensitivity_is_the_contrast_case(self):
        """`set<scalar>` DOES canonicalize order-insensitively — it is a `frozenset`.

        The contrast with the test above is the whole point: sorting is available
        exactly when the host shape (`frozenset`) proves the value is a set.
        """
        assert normalize_python_value(frozenset({"b", "a"}), is_row_level=False) == normalize_python_value(
            frozenset({"a", "b"}), is_row_level=False
        )

    def test_map_nested_in_a_set_element_is_an_unsupported_projected_shape(self):
        """LIMITATION a-2 (lossy projection): a `map` inside a set element does not canonicalize.

        A `set<frozen<map<text,int>>>` element is routed through
        `value_to_hashable_key`, whose `Value::Map` arm projects the map to a
        **tuple of pairs** — so it normalizes to `[["a", 1]]`, while Node and the
        CLI render that nested map as `[{"key": "a", "value": 1}]`. Different in
        kind, not in ordering.

        The contrast case below shows the same nested map in *value* position
        canonicalizing correctly (it goes through `value_to_py` -> `dict`), which
        is what makes this a projection defect rather than a map-rendering one.
        Pinned as a recorded gap for #1455 (must treat it as UNSUPPORTED), not as
        a desirable shape; the fix needs the declared type (#3497).
        """
        element = (("a", 1),)  # what value_to_hashable_key makes of {"a": 1}
        assert normalize_python_value(frozenset({element}), is_row_level=False) == [[["a", 1]]]

        # Contrast: the same nested map as a map VALUE canonicalizes correctly.
        assert normalize_python_value({"k": {"a": 1}}, is_row_level=False) == [
            {"key": "k", "value": [{"key": "a", "value": 1}]},
        ]

        # The enclosing set still SORTS (it is a frozenset); only the element shape diverges.
        two = normalize_python_value(frozenset({(("a", 1),), (("b", 2),)}), is_row_level=False)
        assert two == sorted(two, key=_sort_key)

    def test_udt_nested_deeper_in_a_projection_position_now_canonicalizes(self):
        """a-3 (was LIMITATION): a UDT inside a projected value canonicalizes again.

        The position that still forces a hashable projection is a MAP KEY:
        `map<frozen<list<frozen<udt>>>, int>` routes its key through
        `value_to_hashable_key` **unconditionally** — `map_to_py` has no
        `contains_udt` gate, because a Python `dict` key must be hashable whatever
        it holds — and its `List` arm recurses into the inner UDT. That inner
        projection used to FLATTEN the UDT into a `frozenset` of
        `(field_name, value)` pairs, so the key canonicalized to
        `[[["_keyspace", …], ["_type", …], ["street", …]]]`, an array of
        `[name, value]` pairs, while Node and the CLI produce
        `[[{"street": …}]]`, an array holding a UDT **object** (since #3629 that
        object holds the declared fields and nothing else, on every side).
        Different in kind, not in ordering.

        Issue #3504 replaced that projection with a `cqlite.Udt` instance, so the
        projected value keeps its type and the two shapes agree. The a-1 instance
        (a UDT directly as a map key) is the same projection one level up and is
        fixed by the same change.

        The FAMILY is not closed by this, and the criterion is unchanged: a lossy
        projection diverges iff the projected type's canonical form is not a plain
        JSON array. `map` still is such a type — `value_to_hashable_key`'s `Map`
        arm still flattens to a tuple of pairs — so **a-2 remains open**
        (`test_map_nested_in_a_set_element_is_an_unsupported_projected_shape`).
        What #3504 removed is the UDT half, by making the projection type-preserving
        rather than by teaching the normalizer a shape.

        SCOPE CORRECTION (#3500). This instance used to be asserted in a SET
        ELEMENT position (`set<frozen<list<frozen<udt>>>>`), and it is **no longer
        live there**: `contains_udt` now traverses the whole subtree, so
        `set_to_py` takes its `list` branch for that column and the inner UDT
        arrives as an ordinary `dict`. The second assertion below pins that new
        shape and shows it MATCHES the Node/CLI form — so a-3 is closed for the
        set-element position and survives only where a hashable projection is
        unavoidable, i.e. map keys. The change was taken deliberately (#3500 AC1
        over AC5) because it removes the nesting-dependent asymmetry that was the
        defect's own tell.

        The sibling shapes that used to RAISE
        (`set<frozen<tuple<frozen<udt>, int>>>`, `set<frozen<set<frozen<udt>>>>`)
        now read successfully and are pinned end-to-end in
        `bindings/python/tests/test_nested_udt_hashable.py`. Core-side multicell
        map keys (#3612) are decoded structurally too, so they are no longer a
        gap; the residual there is the narrower nested-element width check
        (#3723).
        """
        # What the binding hands over for a map KEY of `map<frozen<list<frozen<udt>>>, int>`:
        # a tuple (the projected inner list) holding the projected inner UDT, which
        # is now a `cqlite.Udt` whose field values have themselves been projected.
        projected_udt = cqlite.Udt("address", "test_collections", {"street": "1 Main St"})
        element = (projected_udt,)
        assert normalize_python_value({element: 7}, is_row_level=False) == [
            {"key": [{"street": "1 Main St"}], "value": 7}
        ]

        # CLOSED for the set-element position too, by the other half of the merge
        # (#3500): `set<frozen<list<frozen<udt>>>>` no longer reaches a `frozenset`
        # at all, because `contains_udt` sees the UDT under the inner list and
        # `set_to_py` takes its `list` branch. That column arrives as a `list` of
        # `list`s of UDTs. The rendered UDT holds its DECLARED FIELDS AND NOTHING
        # ELSE since #3629 — the CLI and `cqlite-core`'s `ToJson` stopped injecting
        # `_type`, so the canonical form no longer carries it on any side.
        assert normalize_python_value(
            [[_udt("address", street="1 Main St")]], is_row_level=False
        ) == [[{"street": "1 Main St"}]]

        # A `frozenset` of projected elements is still REACHABLE — one level deeper,
        # as a `map<frozen<set<frozen<list<frozen<udt>>>>>, int>` key, where
        # `value_to_hashable_key`'s `Set` arm builds it — and it canonicalizes to the
        # same shape as the non-projection route. Asserted as an EQUALITY rather than
        # as a shape somebody wrote down twice; the two used to differ in kind, and
        # that is the fix.
        assert normalize_python_value(
            frozenset({element}), is_row_level=False
        ) == normalize_python_value([[_udt("address", street="1 Main St")]], is_row_level=False)

        # The projection is only usable as a set element / map key because a
        # `cqlite.Udt` is hashable when its field values are — which is what the
        # `frozenset` above exercises.
        assert hash(projected_udt) == hash(
            cqlite.Udt("address", "test_collections", {"street": "1 Main St"})
        )

    def test_two_udt_types_with_identical_fields_stay_distinct_map_keys(self):
        """Site 4: identity participates in equality/hash, as the old pairs did.

        The pre-#3504 `frozenset` projection distinguished two UDTs of different
        declared types only because it INJECTED `_type`/`_keyspace` pairs into the
        pair set. Removing those pairs — the point of the fix — would have silently
        collapsed such keys into one had identity not moved onto the instance.
        """
        a = cqlite.Udt("point", "a", {"x": 1, "y": 2})
        b = cqlite.Udt("point", "b", {"x": 1, "y": 2})
        c = cqlite.Udt("other", "a", {"x": 1, "y": 2})
        assert a != b and a != c and b != c
        assert len({a, b, c}) == 3, "keyspace and type name must both participate"
        # Same identity and same fields → equal and interchangeable as a key.
        assert a == cqlite.Udt("point", "a", {"x": 1, "y": 2})
        assert len({a, cqlite.Udt("point", "a", {"y": 2, "x": 1})}) == 1, (
            "field ORDER must not affect equality or hash"
        )

    def test_udt_field_named_keyspace_survives(self):
        """b-2, SITE "UDT fields": **FIXED** (#3504) — the field is no longer lost.

        This test used to pin the DEFECT. Both bindings injected `_type` and
        `_keyspace` into the same namespace that carries user-controlled field
        names, markers first (`udt_to_py`: `set_item("_type")`,
        `set_item("_keyspace")`, then `set_item(field.name)`; `udt_to_object` did
        the identical thing), so a field named `_keyspace` — a legal quoted CQL
        identifier — OVERWROTE the marker. The canonical rule then dropped
        `_keyspace` because the CLI omits it for UDTs, and the FIELD was lost,
        while the CLI kept it.

        #3504 removed the shared channel: identity rides on `cqlite.Udt`, `.fields`
        holds only declared fields, and this normalizer therefore has nothing to
        filter. A field named `_keyspace` survives canonicalization and matches
        what the CLI emits.

        THE CLI-SIDE RESIDUAL THIS TEST USED TO PIN IS **NOW FIXED** (#3629). The
        CLI's JSON writer (`cqlite-cli/src/output/json.rs`, `Value::Udt`) and its
        second copy in `cqlite-core/src/query/result.rs` (`ToJson`) inserted
        `"_type"` ahead of the fields, so a field of that name overwrote the
        marker in the CLI's own output and the canonical form mirrored the
        collision. Both now render the declared fields and nothing else — the
        shape `cassandra-5.0.8`'s `UserType.toJSONString` writes and the committed
        `sstabledump` golden for `test-data/fixtures/issue_3504/` shows — so
        neither side has a marker left to collide with, and the expectations below
        are the golden's shape rather than a mirror of CQLite's old output.

        The binding-side identity is intact regardless of either field name, which
        is the property #3504 actually delivers — asserted last.
        """
        # A UDT whose field is genuinely named "_keyspace".
        colliding = cqlite.Udt("address", "test_collections", {"_keyspace": "user-supplied-value"})
        assert normalize_python_value(colliding, is_row_level=False) == {
            "_keyspace": "user-supplied-value",
        }, "the `_keyspace` FIELD now survives canonicalization (#3504)"

        # An ordinary field is unaffected, as before.
        assert normalize_python_value(
            _udt("address", street="1 Main St"), is_row_level=False
        ) == {"street": "1 Main St"}

        # And the field named `_type` is now just a FIELD, on both sides: it
        # displaces nothing, because nothing is injected beside it (#3629).
        type_field = cqlite.Udt("address", "test_collections", {"_type": "user-supplied-type"})
        assert normalize_python_value(type_field, is_row_level=False) == {
            "_type": "user-supplied-type",
        }, "a `_type` FIELD is carried as itself; no marker shares the namespace"

        # ...and the fix that matters: the BINDING's type identity is recoverable
        # in every one of those cases, from a namespace no field name can address.
        for udt in (colliding, type_field):
            assert udt.type_name == "address"
            assert udt.keyspace == "test_collections"
            assert len(udt) == 1, "exactly the declared fields, with no injected entry"
        # Mapping access reaches the FIELD, never the marker.
        assert colliding["_keyspace"] == "user-supplied-value"
        assert type_field["_type"] == "user-supplied-type"
        # And on a UDT that declares no such field the marker is simply absent —
        # the removed channel, asserted as removed.
        with pytest.raises(KeyError):
            _ = _udt("address", street="1 Main St")["_type"]

    def test_json_object_cell_normalizes_as_a_cql_map(self):
        """LIMITATION b-5 (host-shape collision): a JSON cell is not distinguishable.

        `Value::Json` reaches Python through `json_to_py`, which maps a JSON
        **object** to a `PyDict` and a JSON **array** to a `PyList`. So the `dict`
        row of the host-shape lattice (M4_spec §5.3) has TWO cell-level
        sources — `map<k,v>` and a JSON object (`udt` was a third until #3504 gave
        it its own host type) — and the normalizer, seeing only
        the host value, canonicalizes a JSON object as a **CQL map**: a sorted
        array of `{"key": ..., "value": ...}`, where the CLI keeps an object. A
        JSON object carrying a literal `"_type"` key is additionally read as a UDT.

        Reachability is stated in exactly ONE place — the `Value::Json` arm of
        `value_to_hashable_key` in `bindings/python/src/value_hashable.rs` — and this
        docstring asserts nothing about it (earlier wording here blamed fixture
        absence, which is not the blocker). What this test pins is the SHAPE
        divergence, which holds for any `dict`-shaped cell however it arrives:
        #1455 must exclude columns whose comparator is `"json"`; the fix is the
        declared type (#3497).

        Characterization only — this pins current behavior as a known gap.
        """
        json_object_cell = {"a": 1, "b": "two"}
        assert normalize_python_value(json_object_cell, is_row_level=False) == [
            {"key": "a", "value": 1},
            {"key": "b", "value": "two"},
        ], "a JSON object is canonicalized as a CQL map, not kept as an object (b-5)"

        # A JSON object carrying "_type" takes the UDT branch instead — the third
        # source colliding with the marker class (b-2).
        assert normalize_python_value({"_type": "x", "a": 1}, is_row_level=False) == {
            "_type": "x",
            "a": 1,
        }

        # A JSON array is indistinguishable from `list<T>`: both become an array.
        assert normalize_python_value([1, "two"], is_row_level=False) == [1, "two"]

    def test_map_with_literal_type_key_is_misclassified_as_a_udt(self):
        """LIMITATION b-2, SITE "cell-level map": a `map<text,X>` holding `"_type"` reads as a UDT.

        `"_type"` and `"_keyspace"` are legal `text` map keys, and a CQL `map`
        and a `udt` are both a Python `dict`, so the normalizer's
        `if "_type" in value:` branch cannot tell them apart: such a map
        normalizes to an **object** instead of the documented sorted array of
        `{"key": ..., "value": ...}`, and a `"_keyspace"` entry is silently
        **dropped** (the UDT branch filters it, since the CLI omits it for UDTs).

        This is scoped to CELL level. The row-level twin of this defect is FIXED,
        not documented: `normalize_python_value` now checks the caller's
        `is_row_level` signal BEFORE the `"_type"` sniff, so a row with a
        `"_type"` column is no longer read as a UDT — see
        `test_row_with_type_and_keyspace_columns_normalizes_as_a_row`. No such
        signal exists at cell level, which is why this half remains a limitation.

        Pinned as a recorded gap, not a desirable shape. It is deliberately NOT
        "fixed" by also requiring `_keyspace`: a legal map can carry both keys,
        so that would only pick a rarer delimiter on an already-ambiguous channel
        (the control/data lesson in CLAUDE.md). The real fix is the declared CQL
        type (#3497); until then #1455 must treat such a map as UNSUPPORTED.
        """
        # A genuine map that happens to use "_type" as a key.
        assert normalize_python_value({"_type": "not_a_udt", "a": 1}, is_row_level=False) == {
            "_type": "not_a_udt",
            "a": 1,
        }
        # ...and with a literal "_keyspace" entry, that entry is dropped outright.
        assert normalize_python_value(
            {"_type": "not_a_udt", "_keyspace": "ks", "a": 1}, is_row_level=False
        ) == {"_type": "not_a_udt", "a": 1}

        # Contrast: a map without "_type" canonicalizes as documented.
        assert normalize_python_value({"a": 1}, is_row_level=False) == [{"key": "a", "value": 1}]

    def test_map_is_a_sorted_array_of_key_value_objects(self):
        """`map<k,v>` → Python `dict` → sorted array of `{"key": k, "value": v}` (asymmetry row 2)."""
        assert normalize_python_value({"b": 2, "a": 1}, is_row_level=False) == [
            {"key": "a", "value": 1},
            {"key": "b", "value": 2},
        ]

    def test_benign_projection_list_and_set_keys_canonicalize_unchanged(self):
        """BENIGN (family a, non-instance): a `list`/`set`/`tuple` key projection does NOT diverge.

        `value_to_hashable_key` discards the host TYPE — a `map<frozen<list<int>>, text>` key
        arrives as a `tuple` — but the *canonical form* is unchanged, because a
        `list`, a `tuple` and a `frozenset` all canonicalize to a JSON array and
        Node/the CLI render those same keys as arrays too. So the criterion for
        family (a) is narrower than "a non-scalar in a projection position":

            a lossy projection diverges iff the projected type's canonical form
            is not a plain JSON array

        which is why only `map` (a-2) still generates instances. `udt` generated
        a-1/a-3 until #3504 made its projection type-preserving; those are closed
        and the criterion is what did not change.
        This test exists so those benign cases are demonstrated rather than left
        as an unremarked pass — #1455 must NOT special-case them
        (M4_spec §5.3, family (a) table).
        """
        # list key → projected to a tuple → array. Node/CLI: array. Identical.
        assert normalize_python_value({(1, 2): "x"}, is_row_level=False) == [
            {"key": [1, 2], "value": "x"},
        ]
        # set key → frozenset → SORTED array. Node/CLI: sorted array. Identical.
        assert normalize_python_value({frozenset({2, 1}): "x"}, is_row_level=False) == [
            {"key": [1, 2], "value": "x"},
        ]
        # A projected `list` key and a projected `set` key with the same elements
        # canonicalize to the SAME array: the lost host-type distinction costs
        # nothing at the canonical level, which is the whole point of "benign".
        assert normalize_python_value({(1, 2): "x"}, is_row_level=False) == normalize_python_value(
            {frozenset({1, 2}): "x"}, is_row_level=False
        )

    def test_duplicate_non_scalar_map_keys_are_unsupported(self):
        """LIMITATION b-3 (host-shape collision, key identity): duplicate non-scalar keys.

        A Python `dict` cannot hold two structurally-equal non-scalar keys at
        all — they collapse by hash/`__eq__`, last value wins — while a Node
        `Map` compares object keys by reference and keeps **both**, so the two
        canonical forms differ in LENGTH and no sorting reconciles them.

        The collapse itself happens in `map_to_py`, before any normalizer runs,
        so what is observable here is the consequence: one entry where Node would
        have two. Well-formed Cassandra data never produces duplicate map keys,
        so this is out of contract rather than a live read-path bug; closing it
        (dedup or rejection) is a behavior change tracked by #3497.
        """
        collapsed = {(1, 2): "first"}
        collapsed[(1, 2)] = "second"  # a structurally-equal key: collapses, last wins
        assert normalize_python_value(collapsed, is_row_level=False) == [
            {"key": [1, 2], "value": "second"},
        ], "Python collapses equal non-scalar keys; a Node Map would keep both entries"

    def test_map_with_a_udt_key_canonicalizes_as_a_udt_object(self):
        """a-1 (was LIMITATION): a UDT map key canonicalizes like any other UDT.

        `map_to_py` routes keys through `value_to_hashable_key`, whose `Udt` arm
        used to flatten the UDT into a `frozenset` of `(field_name, value)` pairs
        — so the key normalized to a sorted array of `[name, value]` pairs while
        Node and the CLI render the same key as a UDT **object**, a difference in
        KIND that no sorting reconciles. #3504 made the arm return a `cqlite.Udt`,
        so the projection is type-preserving and the key takes the UDT branch.

        THIS TEST'S PREDECESSOR WAS VACUOUS AND IS THE REASON THE ASSERTIONS BELOW
        ARE SHAPED AS THEY ARE. It hand-built the old `frozenset` and asserted the
        normalizer's array-of-pairs output — an input the binding can no longer
        produce — so it passed identically before and after the change and could
        never have observed the closure. The subject here is the shape the
        production arm actually returns (`build_udt` → `cqlite.Udt`, field values
        recursively projected), and the load-bearing assertion is an EQUALITY
        against the value-position canonical form rather than a shape written down
        twice. The Cassandra-written end of the same property — a real projected
        key decoded from `test-data/fixtures/issue_3504` — is
        `test_projected_map_key_holds_exactly_one_type_entry` in
        `tests/test_issue_3504_udt_field_namespace.py`; this class is
        deliberately pure (see its docstring), so it asserts the normalizer half.
        """
        key = cqlite.Udt("address", "test_collections", {"street": "1 Main St"})
        assert normalize_python_value({key: 7}, is_row_level=False) == [
            {"key": {"street": "1 Main St"}, "value": 7},
        ]

        # The property that makes it CANONICALIZABLE: key position and value
        # position now agree, which is exactly what differed in kind before.
        assert normalize_python_value({key: 7}, is_row_level=False)[0][
            "key"
        ] == normalize_python_value(key, is_row_level=False)

        # And the divergent shape is GONE, not merely unasserted: the key is an
        # object, never the array of `[name, value]` pairs the old projection
        # produced. Stated negatively because the predecessor's whole failure was
        # asserting a shape nothing produces.
        assert not isinstance(
            normalize_python_value({key: 7}, is_row_level=False)[0]["key"], list
        )

        # Identity discriminates ON THE INSTANCE, which is where #3504 put it: two
        # same-fields UDTs of DIFFERENT declared types remain distinct dict keys and
        # produce two entries.
        twin = cqlite.Udt("address_v2", "test_collections", {"street": "1 Main St"})
        both = normalize_python_value({key: 7, twin: 8}, is_row_level=False)
        assert len(both) == 2
        assert key != twin and len({key, twin}) == 2, (
            "the declared type participates in equality/hash on the instance"
        )

        # ...and it does NOT survive into the canonical form, because the CANONICAL
        # FORM MIRRORS `--format json`, WHICH CARRIES NO TYPE CHANNEL AT ALL
        # (#3629). `sstabledump` is the same: `UserType.toJSONString` emits declared
        # fields only, so a dump cannot tell these two apart either. Asserted rather
        # than left implicit — this is a real consequence of the fix, not an
        # oversight, and the place to notice it is here.
        assert both[0]["key"] == both[1]["key"], (
            "two UDT types with identical fields are indistinguishable in JSON, on "
            "every side including sstabledump"
        )

    def test_tuple_is_an_array(self):
        """`tuple<...>` → JSON array (asymmetry row 3)."""
        assert normalize_python_value(("x", 1, None), is_row_level=False) == ["x", 1, None]

    def test_tuple_and_list_canonicalize_identically(self):
        """Node returns an `Array` for both `tuple<...>` and `list<T>`.

        So the canonical form must erase the distinction Python preserves,
        otherwise a Node↔Python comparison can never agree (#1454, #1455).
        """
        assert normalize_python_value(("a", 1), is_row_level=False) == normalize_python_value(
            ["a", 1], is_row_level=False
        )

    def test_values_equal_accepts_a_reordered_scalar_list_or_tuple(self):
        """LIMITATION b-4 (host-shape collision at COMPARISON time): list AND tuple order is not verified.

        `values_equal` tries an ordered comparison and then falls back to an
        UNORDERED (sorted) one, so a reordered `list<int>` compares EQUAL — even
        though §5.3's `list<T>` row says "positional; order preserved on both
        sides".

        SCOPE, from the guard's semantics rather than by example: the guard is
        `not any(isinstance(v, dict) for v in py_val)`, which inspects only that
        level's IMMEDIATE elements, and the ordered path RECURSES. So the
        fallback applies independently at EVERY array level whose immediate
        elements hold no dict, at ANY nesting depth. A level that does hold
        dicts (a map-repr array) is ordered-only at that level — but its nested
        arrays are still swallowed.

        This pins the CURRENT behavior as a recorded gap, NOT as a desirable
        property. The fallback is a deliberate accommodation whose reason is
        recorded at the branch: a CQL `SET` is sorted by `_sort_key` on the
        Python side and emitted in Cassandra's internal byte-order by the CLI, so
        removing the fallback would red genuine set comparisons in the existing
        #319 suite — trading a false pass for a false failure. The canonical form
        merges sets and lists into arrays by design, so the comparison layer
        cannot tell them apart either; separating them needs the declared CQL
        type (#3497).

        This applies to `tuple<...>` too, and that is easy to miss: a tuple
        canonicalizes to the SAME array as a list (the deliberate benign merge,
        since Node cannot tell them apart), so the unordered fallback swallows a
        reordered tuple exactly as it does a reordered list. §5.3 calls BOTH rows
        positional, so scoping this limitation to lists alone would leave the
        contract self-contradicting for tuples.

        Consequence for #1455: a genuine `list<T>` OR `tuple<...>` ORDERING
        regression would not be caught by this comparison. A harness that must
        verify order has to compare those columns ordered-only, which requires
        schema information.
        """
        # A reordered scalar list compares EQUAL — the documented gap.
        assert values_equal([1, 2, 3], [3, 1, 2]) is True

        # A reordered TUPLE does too: it normalizes to the same array as a list,
        # so it reaches the same unordered fallback. Same gap, second CQL type.
        py_tuple = normalize_python_value((1, 2, 3), is_row_level=False)
        assert py_tuple == [1, 2, 3]
        assert values_equal(py_tuple, [3, 1, 2]) is True

        # ...and the normalized tuple is indistinguishable from the normalized
        # list, which is WHY one fallback covers both.
        assert py_tuple == normalize_python_value([1, 2, 3], is_row_level=False)

        # NESTED arrays are swallowed too — the guard inspects only immediate
        # elements and the ordered path recurses, so the INNER level applies the
        # fallback even though the outer element is a list, not a primitive.
        assert values_equal([[1, 2]], [[2, 1]]) is True

        # Contrast, pinning the guard's actual boundary: a level holding dicts
        # (a map-repr array) is ordered-only AT THAT LEVEL, so reordering it is
        # correctly caught. This is why maps are sorted by key during
        # normalization rather than left to the comparison layer.
        map_repr = [{"key": "a", "value": 1}, {"key": "b", "value": 2}]
        assert values_equal(map_repr, list(reversed(map_repr))) is False

        # The accommodation this exists for: a set, normalized/sorted differently
        # by the two sides, still compares equal.
        py_set = normalize_python_value(frozenset({3, 1, 2}), is_row_level=False)
        cli_set_order = [3, 1, 2]  # CLI follows Cassandra byte-order, not _sort_key
        assert values_equal(py_set, cli_set_order) is True

        # Guardrails on the fallback's scope, so a future widening is visible here:
        # differing LENGTH still fails, differing CONTENT still fails, and arrays of
        # map-repr dicts are compared without the unordered fallback.
        assert values_equal([1, 2, 3], [1, 2]) is False
        assert values_equal([1, 2, 3], [1, 2, 4]) is False
        assert values_equal([{"key": "a", "value": 1}], [{"key": "b", "value": 1}]) is False

    def test_row_with_type_and_keyspace_columns_normalizes_as_a_row(self):
        """REGRESSION (#1454): a row whose COLUMNS are named `_type`/`_keyspace` is a ROW.

        `"_type"` and `"_keyspace"` are legal column names (quoted identifiers).
        Before the fix, the `"_type"` content sniff ran ahead of the caller's
        `is_row_level` signal, so such a row normalized as a UDT and its
        `"_keyspace"` column was silently DROPPED.

        The caller's explicit signal beats sniffing the content: `is_row_level=True`
        comes from a caller that knows it holds a row, and a UDT is always a cell
        (so it always arrives with `is_row_level=False`). Both columns must survive,
        and cell values inside the row must still normalize normally.
        """
        row = {"_type": "user", "_keyspace": "ks", "pk": 1, "m": {"b": 2, "a": 1}}
        assert normalize_python_value(row, is_row_level=True) == {
            "_type": "user",
            "_keyspace": "ks",  # NOT dropped: this is a column, not UDT metadata
            "pk": 1,
            "m": [{"key": "a", "value": 1}, {"key": "b", "value": 2}],
        }

        # A row named only `_type` (no `_keyspace`) is likewise still a row.
        assert normalize_python_value({"_type": "user", "pk": 1}, is_row_level=True) == {
            "_type": "user",
            "pk": 1,
        }

        # ...while the SAME dict at CELL level is still read as a UDT, so there
        # `_keyspace` IS dropped — the observable contrast between the two levels,
        # and the reason b-2 remains a cell-level limitation.
        cell = {"_type": "user", "_keyspace": "ks", "pk": 1}
        assert normalize_python_value(cell, is_row_level=False) == {"_type": "user", "pk": 1}
        assert normalize_python_value(cell, is_row_level=True) == {
            "_type": "user",
            "_keyspace": "ks",
            "pk": 1,
        }

    def test_row_level_dict_stays_a_dict(self):
        """A row is a `dict` too — only *cell*-level dicts are CQL maps."""
        row = {"pk": 1, "m": {"b": 2, "a": 1}}
        assert normalize_python_value(row, is_row_level=True) == {
            "pk": 1,
            "m": [{"key": "a", "value": 1}, {"key": "b", "value": 2}],
        }

    def test_nested_collection_shapes(self):
        """`map<text, frozen<set<text>>>` and `list<frozen<udt>>` recurse correctly."""
        assert normalize_python_value({"k": frozenset({"z", "y"})}, is_row_level=False) == [
            {"key": "k", "value": ["y", "z"]},
        ]
        assert normalize_python_value([_udt("point", x=1)], is_row_level=False) == [
            {"x": 1},
        ]
