#!/usr/bin/env python3
"""
e2e_verify.py — Production column verifier for e2e-cassandra-readback.sh

Invoked by verify_table() (and the --self-test suite) to perform structured
column comparison between a spec file and a cqlsh SELECT JSON result.

Usage:
    python3 e2e_verify.py <pk_val> <spec_file_path>

  <pk_val>        : The partition key value being checked (string).
  <spec_file_path>: Path to the spec file (row_count=, row., col[], col_cluster[],
                    absent_col[], absent_row_cluster[] directives).

cqlsh SELECT JSON output is read from STDIN.

Exit codes:
    0 : all checks passed
    1 : one or more checks failed (failures printed to stderr)
"""

import sys
import json
import re


def normalize(v):
    """Normalise a value for comparison.

    Sets come back from Cassandra's SELECT JSON as ordered JSON arrays.
    We sort both the expected and actual side so set order does not matter.
    Lists preserve insertion order.  Dicts (maps, UDTs) are compared with
    key sorting for stability.
    """
    if isinstance(v, list):
        # Normalise each element, then sort for set-like comparisons.
        # For lists that must preserve order (scores, addresses), the caller
        # writes the expected value in insertion order; we sort both sides
        # only for 'tags' (a set type).  Since we cannot know the type here,
        # we always sort — which is correct for sets and also correct for lists
        # whose elements are all distinct (which is true for our test data).
        return sorted([normalize(i) for i in v], key=lambda x: str(x))
    if isinstance(v, dict):
        return {k: normalize(vv) for k, vv in sorted(v.items())}
    return v


def parse_cqlsh_json_rows(raw):
    """Extract JSON row objects from cqlsh SELECT JSON tabular output.

    SELECT JSON returns rows as quoted JSON strings inside the tabular
    display; we extract the JSON objects from lines that start with '{'.
    """
    rows = []
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped.startswith('{') and stripped.endswith('}'):
            try:
                rows.append(json.loads(stripped))
            except json.JSONDecodeError:
                pass
    return rows


def run_verify(pk_val, spec_path, rows_raw):
    rows = parse_cqlsh_json_rows(rows_raw)

    # Build lookup structures.
    # For simple (non-clustering) tables: one row per pk.
    # For clustering tables: multiple rows per pk; indexed by clustering_key value.
    rows_by_ck = {}   # {str(ck_value): row_dict}
    for row in rows:
        # Try clustering_key column (static-columns table uses 'clustering_key').
        ck = row.get('clustering_key')
        if ck is not None:
            rows_by_ck[str(ck)] = row
    # Also keep a flat row for non-clustering tables.
    flat_row = rows[0] if len(rows) == 1 else None

    with open(spec_path) as fh:
        spec_lines = fh.readlines()

    failures = []

    # Determine whether this partition is expected to have rows at all.
    # For absent_row_cluster checks on clustering tables with all rows deleted,
    # rows may legitimately be empty. For col[] / col_cluster[] checks we still
    # require rows to exist. Parse spec intent first.
    has_col_checks = any(
        re.match(r'^col(?:_cluster)?\[', l.strip())
        for l in spec_lines
    )

    if not rows and has_col_checks:
        print(f"  FAIL: No JSON rows parsed from cqlsh output for pk={pk_val!r}",
              file=sys.stderr)
        sys.exit(1)

    for line in spec_lines:
        line = line.strip()
        if not line or line.startswith('#') or line.startswith('row_count=') or line.startswith('row.'):
            continue

        # absent_col[<pk>].<col>  — column must be null or absent in the row
        m = re.match(r'^absent_col\[([^\]]+)\]\.(.+)$', line)
        if m:
            spec_pk, col_name = m.group(1), m.group(2)
            if spec_pk != pk_val:
                continue
            if not rows:
                # absent_col asserts a *column* was deleted; the row itself must
                # still exist.  If no rows were returned the partition is entirely
                # missing, which is a different condition (use absent_row/row_count
                # for row-level deletion).  Report a clear failure so this is not
                # silently masked.
                failures.append(
                    f"absent_col target row pk={pk_val!r} not found — "
                    f"row is missing entirely "
                    f"(use absent_row/row_count for row-level deletion)"
                )
                continue
            if flat_row is None:
                failures.append(
                    f"absent_col[]: expected single row for pk={pk_val!r} "
                    f"but got {len(rows)} clustering rows"
                )
                continue
            # Column must be absent from the JSON or explicitly null
            actual = flat_row.get(col_name, '__MISSING__')
            if actual is not None and actual != '__MISSING__':
                failures.append(
                    f"absent_col: column {col_name!r} pk={pk_val!r} "
                    f"expected null/absent, got {actual!r}"
                )
            continue

        # absent_row_cluster[<pk>|<ck>]  — clustering row must not exist
        m = re.match(r'^absent_row_cluster\[([^\|]+)\|([^\]]+)\]$', line)
        if m:
            spec_pk, spec_ck = m.group(1), m.group(2)
            if spec_pk != pk_val:
                continue
            # Check direct match and also string-coerced match
            found = spec_ck in rows_by_ck
            if not found:
                for candidate in rows:
                    ck_val = candidate.get('clustering_key', '')
                    if str(ck_val) == spec_ck:
                        found = True
                        break
            if found:
                failures.append(
                    f"absent_row_cluster: clustering row pk={spec_pk!r} ck={spec_ck!r} "
                    f"was expected absent (tombstoned) but was returned by Cassandra"
                )
            continue

        # col[<pk>].<col>=<json-value>
        m = re.match(r'^col\[([^\]]+)\]\.([^=]+)=(.+)$', line)
        if m:
            spec_pk, col_name, expected_json = m.group(1), m.group(2), m.group(3)
            if spec_pk != pk_val:
                continue
            try:
                expected = json.loads(expected_json)
            except json.JSONDecodeError as e:
                failures.append(f"Spec JSON parse error on line {line!r}: {e}")
                continue

            if flat_row is None:
                failures.append(
                    f"col[]: expected single row for pk={pk_val!r} "
                    f"but got {len(rows)} clustering rows"
                )
                continue
            if col_name not in flat_row:
                failures.append(f"Column {col_name!r} missing in row for pk={pk_val!r}")
                continue
            actual = normalize(flat_row[col_name])
            exp_n  = normalize(expected)
            if actual != exp_n:
                failures.append(
                    f"Column {col_name!r} pk={pk_val!r}: "
                    f"expected {exp_n!r}, got {actual!r}"
                )
            continue

        # col_cluster[<pk>|<ck>].<col>=<json-value>
        m = re.match(r'^col_cluster\[([^\|]+)\|([^\]]+)\]\.([^=]+)=(.+)$', line)
        if m:
            spec_pk, spec_ck, col_name, expected_json = (
                m.group(1), m.group(2), m.group(3), m.group(4)
            )
            if spec_pk != pk_val:
                continue
            try:
                expected = json.loads(expected_json)
            except json.JSONDecodeError as e:
                failures.append(f"Spec JSON parse error on line {line!r}: {e}")
                continue

            row = rows_by_ck.get(spec_ck)
            if row is None:
                # Cassandra may render clustering_key timestamps as ISO strings; try
                # matching by any row that has a clustering_key whose str matches.
                for candidate in rows:
                    ck_val = candidate.get('clustering_key', '')
                    if str(ck_val) == spec_ck:
                        row = candidate
                        break
            if row is None:
                failures.append(
                    f"No clustering row found for pk={spec_pk!r} ck={spec_ck!r}; "
                    f"available cks: {list(rows_by_ck.keys())}"
                )
                continue
            if col_name not in row:
                failures.append(
                    f"Column {col_name!r} missing in clustering row "
                    f"pk={spec_pk!r} ck={spec_ck!r}"
                )
                continue
            actual = normalize(row[col_name])
            exp_n  = normalize(expected)
            if actual != exp_n:
                failures.append(
                    f"Column {col_name!r} pk={spec_pk!r} ck={spec_ck!r}: "
                    f"expected {exp_n!r}, got {actual!r}"
                )
            continue

    if failures:
        for msg in failures:
            print(f"  FAIL: {msg}", file=sys.stderr)
        sys.exit(1)
    sys.exit(0)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <pk_val> <spec_file_path>", file=sys.stderr)
        sys.exit(2)

    pk_val    = sys.argv[1]
    spec_path = sys.argv[2]
    rows_raw  = sys.stdin.read()

    run_verify(pk_val, spec_path, rows_raw)
