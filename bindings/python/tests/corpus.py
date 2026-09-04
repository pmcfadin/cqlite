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
import subprocess
from functools import lru_cache
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
    "test_compaction_tombstone_ttl": "tombstone/TTL compaction byte-parity fixtures (dedicated Rust parity test issue_1387_*)",
    "test_comparator_order": "inet/time multicell-collection element/key ORDERING fixture (dedicated Rust ordering test issue_3790_*)",
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
    "test_nested_udt_keys": (
        "nested-UDT-in-a-hashable-position read-fidelity corpus (#3500): a UDT "
        "reached through a tuple or a nested collection inside a set element / "
        "map key. ENFORCED (not a skip): every partition has live rows, and the "
        "binding-side hashable projection is exactly what this fixture exists to "
        "exercise"
    ),
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


# The committed corpus is owned by THIS source tree (the repo that contains
# this harness + the corpus-coverage policy), NOT by whatever checkout
# ``CQLITE_DATASETS_ROOT`` happens to point at. A concurrent session can commit
# WIP fixtures into a *different* checkout's index (e.g. the main repo the
# datasets root points at) while this branch has not adopted them yet; the
# classification guard must reflect what THIS branch considers committed.
# corpus.py lives at <repo>/bindings/python/tests/corpus.py.
_SOURCE_TREE_SSTABLES = (
    Path(__file__).resolve().parents[3] / "test-data" / "datasets" / "sstables"
)


@lru_cache(maxsize=8)
def _git_tracked_table_dirs(sstables_dir: Path) -> frozenset[str]:
    """``"<keyspace>/<table-dir>"`` for each dir owning ANY git-tracked file.

    Issue #1319 / #1312 (committed = any tracked file): the
    classification/enforcement set is the COMMITTED corpus, NOT raw live-disk
    enumeration. A table DIRECTORY counts as "committed" iff git tracks AT
    LEAST ONE file under ``<keyspace>/<table>-<uuid>/`` — Data.db, TOC,
    Statistics, a JSONL golden, ANYTHING. This deliberately does NOT require a
    tracked ``*-Data.db.jsonl`` golden: a newly-committed table dir that ships
    SSTable metadata but is (regressionly) MISSING its JSONL golden must still
    count as committed so the coverage check can surface the missing golden and
    FAIL LOUDLY (the #1229 missing-golden guarantee), rather than be silently
    omitted as "uncommitted". The separate golden-presence check
    (:func:`find_jsonls_in_dir` / :func:`discover_corpus`) enforces that.

    This still ignores untracked WIP fixtures a concurrent session may have
    dropped into the live ``CQLITE_DATASETS_ROOT`` — at either keyspace
    granularity (a whole new keyspace, e.g. ``test_signed_coll``, ZERO tracked
    files) OR table granularity (a new untracked ``<table>-<uuid>/`` dir under
    an ALREADY-tracked keyspace) — so neither gets enforced.

    Tracked-ness is measured against THIS source tree's
    ``test-data/datasets/sstables`` (the repo that owns this harness + the
    corpus-coverage policy), NOT against ``sstables_dir`` — the live datasets
    root may be a *different* checkout whose index already contains a
    concurrent session's WIP. The ``sstables_dir`` argument is retained so the
    result keys on the (cached) live root for callers, but the query is rooted
    at the source tree.

    Determined with a single ``git ls-files`` call (no pathspec — ALL tracked
    files under the source tree), parsed into the set of ``keyspace/table-dir``
    (first two path segments). If ``git`` is unavailable / this is not a work
    tree, returns an empty set and callers fall back to treating everything
    discovered as committed (see :func:`committed_keyspaces` /
    :func:`_is_committed_table_dir`). In CI and local dev ``.git`` is present.
    """
    src = _SOURCE_TREE_SSTABLES
    if not src.exists():
        return frozenset()
    try:
        proc = subprocess.run(
            ["git", "-C", str(src), "ls-files", "-z"],
            capture_output=True,
            check=False,
        )
    except (OSError, ValueError):
        return frozenset()
    if proc.returncode != 0:
        return frozenset()
    table_dirs: set[str] = set()
    for raw in proc.stdout.split(b"\0"):
        if not raw:
            continue
        # Paths are relative to ``src``: ``<keyspace>/<table-dir>/<file>``.
        rel = raw.decode("utf-8", "surrogateescape")
        parts = rel.split("/")
        if len(parts) >= 3 and parts[0] and parts[1]:
            table_dirs.add(f"{parts[0]}/{parts[1]}")
    return frozenset(table_dirs)


def _git_tracked_keyspaces(sstables_dir: Path) -> frozenset[str]:
    """Keyspaces with at least one git-tracked file under a table dir.

    Derived from the table-granular tracked set
    (:func:`_git_tracked_table_dirs`): a keyspace is committed iff it owns at
    least one tracked table dir. Empty when git is unavailable (callers then
    fall back to treating all discovered keyspaces as committed).
    """
    return frozenset(
        td.split("/", 1)[0] for td in _git_tracked_table_dirs(sstables_dir)
    )


def _is_committed_table_dir(sstables_dir: Path, keyspace: str, table_dir_name: str) -> bool:
    """True if ``<keyspace>/<table_dir_name>`` owns ANY git-tracked file.

    "Committed" is deliberately decoupled from "has a JSONL golden" (#1312): a
    committed dir that is missing its golden must remain DISCOVERABLE so the
    coverage check fails loudly on the missing golden (#1229), not be silently
    dropped here as uncommitted.

    Graceful fallback: if git reports NO tracked files (git unavailable / not a
    work tree), every discovered table dir is treated as committed so the guard
    is not silently neutered. In CI and local dev ``.git`` is present.
    """
    tracked = _git_tracked_table_dirs(sstables_dir)
    if not tracked:
        return True
    return f"{keyspace}/{table_dir_name}" in tracked


def committed_keyspaces(sstables_dir: Path) -> list[str]:
    """Discovered keyspaces restricted to the COMMITTED (git-tracked) corpus.

    A keyspace is "committed" iff it has at least one git-tracked file under a
    table dir (see :func:`_git_tracked_keyspaces`) — not specifically a tracked
    golden, so a keyspace whose new table dir ships SSTable metadata but is
    missing its JSONL golden still counts (the golden gap is caught later,
    loudly, by the coverage check; #1312).
    Untracked-on-disk keyspaces (WIP fixtures a concurrent session dropped in)
    are excluded — they are neither enforced nor flagged as unclassified
    (#1319). Untracked table dirs UNDER a tracked keyspace are filtered out at
    table granularity by :func:`discover_table_dirs` / :func:`discover_tables`.

    Graceful fallback: if git reports NO tracked goldens (git unavailable, not
    a work tree, or a pure dataset asset checked out without ``.git``), every
    discovered keyspace is treated as committed so the guard is not silently
    neutered in those environments. In CI and local dev ``.git`` is present.
    """
    discovered = discover_keyspaces(sstables_dir)
    tracked = _git_tracked_keyspaces(sstables_dir)
    if not tracked:
        return discovered
    return [k for k in discovered if k in tracked]


def discover_tables(sstables_dir: Path, keyspace: str) -> list[str]:
    """Return the table names (UUID suffix stripped) for one keyspace.

    Discovered from committed ``<table>-<uuid>/`` directories. Filtered to the
    COMMITTED corpus at TABLE granularity (#1319): an untracked WIP
    ``<table>-<uuid>/`` dir (no git-tracked golden) under an already-tracked
    keyspace is IGNORED, not enumerated.
    """
    keyspace_dir = sstables_dir / keyspace
    if not keyspace_dir.exists():
        return []
    tables: list[str] = []
    for d in keyspace_dir.iterdir():
        if not d.is_dir():
            continue
        m = _TABLE_DIR_RE.match(d.name)
        if m and _is_committed_table_dir(sstables_dir, keyspace, d.name):
            tables.append(m.group("table"))
    return sorted(tables)


def in_scope_keyspaces(sstables_dir: Path) -> list[str]:
    """Committed keyspaces minus the documented skip-set and ``system*``.

    Enumerates the COMMITTED corpus (git-tracked goldens), NOT raw live-disk
    enumeration (#1319), so an untracked WIP keyspace dropped into
    ``CQLITE_DATASETS_ROOT`` is never enforced.
    """
    return [
        k
        for k in committed_keyspaces(sstables_dir)
        if k not in SKIP_KEYSPACES and not is_system_keyspace(k)
    ]


def discover_table_dirs(sstables_dir: Path, keyspace: str) -> list[tuple[str, Path]]:
    """Return ``(table, dir_path)`` for EVERY ``<table>-<uuid>/`` under a keyspace.

    Unlike :func:`discover_tables`, this does NOT collapse the multiple
    generation directories that share one logical table name (e.g.
    ``test_deltas`` ships three UUID dirs per table). Each physical directory
    is returned separately so its golden is verified individually — collapsing
    by table name silently drops the later generations' JSONL files (#1229).

    Filtered to the COMMITTED corpus at TABLE granularity (#1319): an untracked
    WIP ``<table>-<uuid>/`` dir (no git-tracked golden) under an already-tracked
    keyspace is IGNORED.
    """
    keyspace_dir = sstables_dir / keyspace
    if not keyspace_dir.exists():
        return []
    entries: list[tuple[str, Path]] = []
    for d in keyspace_dir.iterdir():
        if not d.is_dir():
            continue
        m = _TABLE_DIR_RE.match(d.name)
        if m and _is_committed_table_dir(sstables_dir, keyspace, d.name):
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

    The guard enumerates the COMMITTED corpus (keyspaces with git-tracked
    goldens), NOT raw live-disk enumeration (#1319): an untracked WIP keyspace
    a concurrent session dropped into ``CQLITE_DATASETS_ROOT`` (e.g.
    ``test_signed_coll``, goldens not yet committed) is IGNORED — neither
    enforced nor flagged. A genuinely-committed keyspace that is unclassified
    is still returned (the guard still reds).
    """
    classified = set(IN_SCOPE_KEYSPACES) | set(SKIP_KEYSPACES) | set(SKIP_PENDING_KEYSPACES)
    # system* keyspaces are classified by prefix (Cassandra-internal metadata),
    # not enumerated in any explicit set.
    return [
        k
        for k in committed_keyspaces(sstables_dir)
        if k not in classified and not is_system_keyspace(k)
    ]
