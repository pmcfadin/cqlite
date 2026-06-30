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

def is_system_keyspace(keyspace: str) -> bool:
    """Return True for any ``system*`` keyspace.

    All ``system*`` keyspaces (``system``, ``system_auth``, ``system_schema``,
    ``system_distributed``, ``system_traces``, ``system_views``, ...) are
    Cassandra-internal metadata, not user-data read-parity targets. They are
    excluded by PREFIX so any future ``system*`` keyspace shipped in a dataset
    subset is auto-excluded (#1229). See test-data/corpus-coverage-policy.md.
    """
    return keyspace.startswith("system")


# Keyspaces intentionally excluded from the comprehensive read-parity corpus by
# EXACT name. Each entry MUST carry a reason; do not silently drop a keyspace to
# make numbers pass. ``system*`` keyspaces are excluded separately by prefix via
# is_system_keyspace() — do not enumerate them here. Mirrored in
# smoke-test-all-tables.sh (SKIP_KEYSPACE_NAMES).
SKIP_KEYSPACES: dict[str, str] = {
    # Write/compaction byte-parity fixtures validated by dedicated Rust tests.
    "test_writeparity": "write byte-parity fixtures (dedicated Rust parity tests)",
    "test_compactionparity": "compaction byte-parity fixtures (differential-compaction harness)",
    "test_compactionparityudt": "compaction-parity UDT fixtures (compaction harness; may be local-only)",
    "test_signed_coll": "signed set/map element-order byte-parity fixtures (dedicated Rust parity test issue_1295_*)",
}

# In-scope keyspaces that are discovered + listed but not executed through the
# comprehensive row-count corpus. Not silently dropped. This set MUST be
# identical across smoke-test-all-tables.sh (SKIP_PENDING_KEYSPACES),
# parity-utils.js (SKIP_PENDING_KEYSPACES), and corpus-coverage-policy.md.
SKIP_PENDING_KEYSPACES: dict[str, str] = {
    "test_deltas": "binaries not in published dataset asset yet (issue #701)",
    "test_tomb": (
        "tombstone parity fixtures with valid zero-live-row partitions; "
        "validated by dedicated Rust tombstone/TTL parity tests, not the "
        "comprehensive row-count corpus"
    ),
    "test_types": (
        "CQL-type/schema-evolution parity fixtures with valid zero-live-row "
        "cases (deleted-counter shadowing); validated by dedicated Rust "
        "CQL-type parity tests, not the comprehensive row-count corpus"
    ),
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
    """Discovered keyspaces minus the documented skip-set and ``system*``."""
    return [
        k
        for k in discover_keyspaces(sstables_dir)
        if k not in SKIP_KEYSPACES and not is_system_keyspace(k)
    ]


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


def find_jsonls_in_dir(table_dir: Path) -> list[Path]:
    """Return EVERY JSONL golden inside one ``<table>-<uuid>/`` directory.

    A single directory can ship multiple generation goldens
    (``nb-1-…``/``nb-2-…``/``nb-3-…``), e.g. ``test_tomb/dropped_regular_col``
    (nb-1+nb-2) and ``test_types/se_altered_then_dropped_column``
    (nb-1+nb-2+nb-3). Returning only the first one silently drops the later
    generations — the exact blind spot #1229 exists to remove. Globs
    ``*-Data.db.jsonl`` to stay format-agnostic (nb-/oa-/da-, any generation).
    """
    if not table_dir.is_dir():
        return []
    return [p for p in sorted(table_dir.glob("*-Data.db.jsonl")) if p.exists()]


def find_jsonl_in_dir(table_dir: Path) -> Path | None:
    """Return the FIRST JSONL golden inside one ``<table>-<uuid>/`` directory.

    Retained for callers (e.g. value-parity row sampling) that only need a
    single representative golden. Coverage/enumeration MUST instead use
    :func:`find_jsonls_in_dir` / :func:`discover_corpus` so every generation
    is checked — never collapse to first-match for coverage (#1229).
    """
    jsonls = find_jsonls_in_dir(table_dir)
    return jsonls[0] if jsonls else None


def discover_corpus(sstables_dir: Path) -> list[tuple[str, str, Path]]:
    """Return ``(keyspace, table, golden_path)`` for the in-scope read-parity corpus.

    Enumerated PER GOLDEN, not per logical table and not per directory: a
    keyspace/table directory with N ``*-Data.db.jsonl`` generation goldens
    contributes N entries, each carrying the exact golden ``Path`` so every
    generation is checked (not just the first one in the directory — #1229).
    Directories with NO golden still contribute a single ``(keyspace, table,
    dir_path)`` entry so a missing golden is reported rather than silently
    dropped.

    Excludes skip-set keyspaces (system + parity-fixture). Includes
    skip-pending keyspaces (they are discovered; the caller decides whether
    to execute them).
    """
    entries: list[tuple[str, str, Path]] = []
    for keyspace in in_scope_keyspaces(sstables_dir):
        for table, dir_path in discover_table_dirs(sstables_dir, keyspace):
            goldens = find_jsonls_in_dir(dir_path)
            if goldens:
                for golden in goldens:
                    entries.append((keyspace, table, golden))
            else:
                # No golden yet (skip-pending / overlooked): keep one entry so
                # the coverage check can report it instead of dropping it.
                entries.append((keyspace, table, dir_path))
    return entries


def unclassified_keyspaces(sstables_dir: Path) -> list[str]:
    """Discovered keyspaces classified into NONE of the explicit buckets.

    A keyspace is "classified" only if it appears in one of the explicit,
    hand-maintained sets:

      * ``IN_SCOPE_KEYSPACES`` — the documented read-parity corpus (includes
        the skip-pending keyspaces, which are in-scope but not executed yet),
      * ``SKIP_KEYSPACES`` — intentionally excluded parity-fixture keyspaces,

    plus any ``system*`` keyspace (classified by prefix via
    :func:`is_system_keyspace`, not enumerated).

    This is deliberately NOT "discovered minus skip-set" (which can never be
    unclassified by construction — the tautology #1229 exists to kill). A
    newly-committed keyspace that nobody added to either explicit set is
    returned here, so the enumeration test reds the suite instead of silently
    absorbing it as in-scope.
    """
    classified = set(IN_SCOPE_KEYSPACES) | set(SKIP_KEYSPACES) | set(SKIP_PENDING_KEYSPACES)
    # system* keyspaces are classified by prefix (Cassandra-internal metadata),
    # not enumerated in any explicit set.
    return [
        k
        for k in discover_keyspaces(sstables_dir)
        if k not in classified and not is_system_keyspace(k)
    ]
