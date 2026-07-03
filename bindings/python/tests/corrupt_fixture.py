"""Runtime corrupt-SSTable fixture generation for the abort-safety harness.

Issue #1437. No large binaries are committed to the repo. Given a temp dir,
this module copies the real ``test_basic.simple_table`` SSTable directory and
mutates ``nb-1-big-Data.db`` in one of two modes, both of which the harness
exercises:

- ``truncate``: copy the SSTable dir, then truncate ``nb-1-big-Data.db`` to
  roughly 50% of its byte length.
- ``bitflip``: copy the whole SSTable dir, then XOR ``0x01`` into a byte in the
  middle of ``nb-1-big-Data.db``.

The sibling ``-Statistics.db`` / ``-Index.db`` / ``-Summary.db`` /
``-CompressionInfo.db`` / ``-TOC.txt`` components are kept intact so that
``cqlite.open()`` proceeds far enough to actually read the corrupt Data.db
during a scan (that is where the corrupt-input panic paths live).

Dataset rule (issue #1437): if the real source Data.db is present but empty or
unreadable, callers must FAIL LOUDLY — never skip. ``ensure_source_readable``
raises so the pytest layer can turn that into a hard failure.
"""

from __future__ import annotations

import shutil
from pathlib import Path

KEYSPACE = "test_basic"
TABLE = "simple_table"
DATA_COMPONENT = "nb-1-big-Data.db"
COMPRESSION_COMPONENT = "nb-1-big-CompressionInfo.db"
TOC_COMPONENT = "nb-1-big-TOC.txt"
MODES = ("truncate", "bitflip")

# Do not copy the multi-megabyte derived JSONL golden; the reader only consults
# the TOC-listed binary components. The small ``-TOC.txt`` MUST be kept (the
# issue requires it), so we scope the ignore to the JSONL only.
_IGNORE = shutil.ignore_patterns("*.jsonl")


def source_table_dir(datasets_root: Path) -> Path | None:
    """Return the real ``simple_table-<uuid>`` dir under ``datasets_root``.

    ``datasets_root`` is the ``sstables/`` directory (the conftest ``DATASETS``
    constant). Returns ``None`` when no matching table dir with a Data.db is
    present (dataset simply not fetched).
    """
    ks_dir = Path(datasets_root) / KEYSPACE
    if not ks_dir.is_dir():
        return None
    for candidate in sorted(ks_dir.glob(f"{TABLE}-*")):
        if (candidate / DATA_COMPONENT).is_file():
            return candidate
    return None


def ensure_source_readable(datasets_root: Path) -> Path:
    """Return the source table dir or raise loudly when it is broken.

    Raises ``FileNotFoundError`` when the dataset is absent (caller decides
    skip-vs-fail) and ``ValueError`` when the Data.db is present but empty —
    which is a corrupt fixture source, never a skip (issue #1437).
    """
    src = source_table_dir(datasets_root)
    if src is None:
        raise FileNotFoundError(
            f"No {KEYSPACE}.{TABLE} SSTable with {DATA_COMPONENT} under {datasets_root}"
        )
    data = src / DATA_COMPONENT
    if data.stat().st_size == 0:
        raise ValueError(f"Source {data} is present but empty (corrupt dataset)")
    return src


def _drop_compression_info(dest_table: Path) -> None:
    """Remove ``CompressionInfo.db`` (and its TOC line) from a copied table.

    This forces the reader down the *uncompressed* Data.db path, which parses
    raw VInt/row lengths directly. That is where the corrupt-input panics live
    (ZigZag lengths etc.); with the compression sidecar present, the Snappy
    decompression layer contains any corruption and the panics are never
    reached. A missing/absent CompressionInfo.db is itself a plausible
    real-world corruption, so this is a valid abort-safety scenario — not a
    contrivance.
    """
    comp = dest_table / COMPRESSION_COMPONENT
    if comp.exists():
        comp.unlink()
    toc = dest_table / TOC_COMPONENT
    if toc.exists():
        kept = [
            line
            for line in toc.read_text().splitlines()
            if "CompressionInfo" not in line
        ]
        toc.write_text("\n".join(kept) + "\n")


def make_corrupt_fixture(
    dest_parent: Path,
    datasets_root: Path,
    mode: str,
    *,
    expose_uncompressed: bool = False,
) -> Path:
    """Build a corrupt copy of the SSTable under ``dest_parent`` and return the
    ``sstables/`` root to pass to :func:`cqlite.open`.

    ``mode`` is one of :data:`MODES`. When ``expose_uncompressed`` is True the
    copy also drops ``CompressionInfo.db`` so the corrupt Data.db is read on the
    raw (uncompressed) parse path — see :func:`_drop_compression_info`.
    """
    if mode not in MODES:
        raise ValueError(f"unknown mode {mode!r}; expected one of {MODES}")

    src = ensure_source_readable(datasets_root)
    dest_root = Path(dest_parent) / "sstables"
    dest_table = dest_root / KEYSPACE / src.name
    shutil.copytree(src, dest_table, ignore=_IGNORE)

    if expose_uncompressed:
        _drop_compression_info(dest_table)

    data = dest_table / DATA_COMPONENT
    length = data.stat().st_size
    if mode == "truncate":
        with open(data, "r+b") as fh:
            fh.truncate(length // 2)
    else:  # bitflip
        offset = length // 2
        with open(data, "r+b") as fh:
            fh.seek(offset)
            (byte,) = fh.read(1)
            fh.seek(offset)
            fh.write(bytes([byte ^ 0x01]))

    return dest_root
