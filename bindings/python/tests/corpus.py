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


def discover_corpus(sstables_dir: Path) -> list[tuple[str, str]]:
    """Return ``(keyspace, table)`` pairs for the in-scope read-parity corpus.

    Excludes skip-set keyspaces (system + parity-fixture). Includes
    skip-pending keyspaces (they are discovered; the caller decides whether
    to execute them).
    """
    pairs: list[tuple[str, str]] = []
    for keyspace in in_scope_keyspaces(sstables_dir):
        for table in discover_tables(sstables_dir, keyspace):
            pairs.append((keyspace, table))
    return pairs


def unclassified_keyspaces(sstables_dir: Path) -> list[str]:
    """Discovered keyspaces that are neither in-scope nor in any documented set.

    Should always be empty: every discovered keyspace is either in-scope
    (covered) or in SKIP_KEYSPACES. A non-empty result means a newly
    committed keyspace slipped in without a coverage decision — the
    enumeration tests fail loudly on this.
    """
    known = set(SKIP_KEYSPACES)
    return [k for k in discover_keyspaces(sstables_dir) if k not in known and k not in in_scope_keyspaces(sstables_dir)]
