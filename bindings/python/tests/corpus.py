"""Dynamic test-corpus enumeration (Issue #1229).

Replaces the hand-typed ``ALL_TABLES`` 33-tuple and the tautological
``len(ALL_TABLES) == 33`` assertions with enumeration of the **committed**
corpus on disk.

The corpus is discovered by walking
``test-data/datasets/sstables/<keyspace>/<table>-<uuid>/`` and is based on
the committed directory structure / JSONL goldens, NOT on ``Data.db``
presence (worktrees and clean checkouts lack the gitignored binaries).

The skip-set + rationale is the policy decision documented in
``test-data/corpus-coverage-policy.md`` — keep the two in sync.
"""

from __future__ import annotations

import re
from pathlib import Path

# UUID suffix appended to every table directory: ``<table>-<32 hex>``.
_TABLE_DIR_RE = re.compile(r"^(?P<table>.+)-[0-9a-f]{32}$")

# =============================================================================
# Skip-set policy (see test-data/corpus-coverage-policy.md)
# =============================================================================

# Keyspaces intentionally excluded from the comprehensive read-parity corpus.
# Each entry MUST carry a reason; do not silently drop a keyspace to make
# numbers pass. Mirrored in smoke-test-all-tables.sh (SKIP_KEYSPACES).
SKIP_KEYSPACES: dict[str, str] = {
    # Cassandra-internal metadata SSTables; no user-facing CQLite schema.
    "system": "Cassandra-internal metadata; not a read-parity target",
    "system_auth": "Cassandra-internal auth metadata; not a read-parity target",
    "system_schema": "Cassandra-internal schema catalog; not a read-parity target",
    # Write/compaction byte-parity fixtures validated by dedicated Rust tests.
    "test_writeparity": "write byte-parity fixtures (dedicated Rust parity tests)",
    "test_compactionparity": "compaction byte-parity fixtures (differential-compaction harness)",
    "test_compactionparityudt": "compaction-parity UDT fixtures (compaction harness; may be local-only)",
}

# In-scope keyspaces that are discovered + listed but not executed yet
# (binaries not in the published dataset asset). Not silently dropped.
SKIP_PENDING_KEYSPACES: dict[str, str] = {
    "test_deltas": "binaries not in published dataset asset yet (issue #701)",
}

# Explicit in-scope read-parity corpus (the documented list in
# test-data/corpus-coverage-policy.md). This is the AUTHORITATIVE classified
# set used by ``unclassified_keyspaces`` — it is NOT "everything not skipped",
# so a NEWLY-committed keyspace that nobody added here trips the integrity
# guard (rather than being silently absorbed as "in-scope" by construction).
# Includes the skip-pending keyspaces (they ARE in-scope; just not executed).
IN_SCOPE_KEYSPACES: dict[str, str] = {
    "test_basic": "simple-types read-parity corpus",
    "test_collections": "list/set/map read-parity corpus",
    "test_timeseries": "time-series read-parity corpus",
    "test_wide_rows": "wide-partition read-parity corpus",
    "test_oa": "Cassandra 5.0 oa-format read-parity corpus (#656)",
    "test_da": "BTI (da-format) read-parity corpus",
    "test_big": "large/wide-partition read-parity corpus",
    "test_comp": "compression read-parity corpus",
    "test_tomb": "tombstone read-parity corpus",
    "test_types": "extended CQL-type read-parity corpus",
    "test_deltas": "CDC-delta read-parity corpus (skip-pending, #701)",
}


def discover_keyspaces(sstables_dir: Path) -> list[str]:
    """Return every keyspace directory present under ``sstables_dir``.

    Based purely on directory structure (committed), independent of whether
    ``Data.db`` binaries are present.
    """
    if not sstables_dir.exists():
        return []
    return sorted(
        d.name for d in sstables_dir.iterdir() if d.is_dir() and not d.name.startswith(".")
    )


def discover_tables(sstables_dir: Path, keyspace: str) -> list[str]:
    """Return the table names (UUID suffix stripped) for one keyspace.

    Discovered from committed ``<table>-<uuid>/`` directories.
    """
    keyspace_dir = sstables_dir / keyspace
    if not keyspace_dir.exists():
        return []
    tables: list[str] = []
    for d in keyspace_dir.iterdir():
        if not d.is_dir():
            continue
        m = _TABLE_DIR_RE.match(d.name)
        if m:
            tables.append(m.group("table"))
    return sorted(tables)


def in_scope_keyspaces(sstables_dir: Path) -> list[str]:
    """Discovered keyspaces minus the documented skip-set."""
    return [k for k in discover_keyspaces(sstables_dir) if k not in SKIP_KEYSPACES]


def discover_table_dirs(sstables_dir: Path, keyspace: str) -> list[tuple[str, Path]]:
    """Return ``(table, dir_path)`` for EVERY ``<table>-<uuid>/`` under a keyspace.

    Unlike :func:`discover_tables`, this does NOT collapse the multiple
    generation directories that share one logical table name (e.g.
    ``test_deltas`` ships three UUID dirs per table). Each physical directory
    is returned separately so its golden is verified individually — collapsing
    by table name silently drops the later generations' JSONL files (#1229).
    """
    keyspace_dir = sstables_dir / keyspace
    if not keyspace_dir.exists():
        return []
    entries: list[tuple[str, Path]] = []
    for d in keyspace_dir.iterdir():
        if not d.is_dir():
            continue
        m = _TABLE_DIR_RE.match(d.name)
        if m:
            entries.append((m.group("table"), d))
    return sorted(entries, key=lambda e: e[1].name)


def discover_corpus(sstables_dir: Path) -> list[tuple[str, str, Path]]:
    """Return ``(keyspace, table, dir_path)`` for the in-scope read-parity corpus.

    Enumerated PER DIRECTORY, not per logical table: a keyspace/table with N
    generation directories contributes N entries, each carrying its exact
    ``<table>-<uuid>/`` directory so the JSONL golden of every generation is
    checked (not just the first one matching the table prefix — #1229).

    Excludes skip-set keyspaces (system + parity-fixture). Includes
    skip-pending keyspaces (they are discovered; the caller decides whether
    to execute them).
    """
    entries: list[tuple[str, str, Path]] = []
    for keyspace in in_scope_keyspaces(sstables_dir):
        for table, dir_path in discover_table_dirs(sstables_dir, keyspace):
            entries.append((keyspace, table, dir_path))
    return entries


def find_jsonl_in_dir(table_dir: Path) -> Path | None:
    """Return the JSONL golden inside one specific ``<table>-<uuid>/`` directory.

    Operates on the EXACT directory (no table-prefix search), so the correct
    generation's golden is verified. Globs ``*-Data.db.jsonl`` to stay
    format-agnostic (nb-/oa-/da-, any generation).
    """
    if not table_dir.is_dir():
        return None
    for jsonl_file in sorted(table_dir.glob("*-Data.db.jsonl")):
        if jsonl_file.exists():
            return jsonl_file
    return None


def unclassified_keyspaces(sstables_dir: Path) -> list[str]:
    """Discovered keyspaces classified into NONE of the explicit buckets.

    A keyspace is "classified" only if it appears in one of the explicit,
    hand-maintained sets:

      * ``IN_SCOPE_KEYSPACES`` — the documented read-parity corpus (includes
        the skip-pending keyspaces, which are in-scope but not executed yet),
      * ``SKIP_KEYSPACES`` — intentionally excluded (system + parity-fixture).

    This is deliberately NOT "discovered minus skip-set" (which can never be
    unclassified by construction — the tautology #1229 exists to kill). A
    newly-committed keyspace that nobody added to either explicit set is
    returned here, so the enumeration test reds the suite instead of silently
    absorbing it as in-scope.
    """
    classified = set(IN_SCOPE_KEYSPACES) | set(SKIP_KEYSPACES) | set(SKIP_PENDING_KEYSPACES)
    return [k for k in discover_keyspaces(sstables_dir) if k not in classified]
