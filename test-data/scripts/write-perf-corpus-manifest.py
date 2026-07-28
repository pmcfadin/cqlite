#!/usr/bin/env python3
"""Emit the issue-#3068 perf-corpus manifest.

Everything recorded here is READ BACK FROM THE WRITTEN BYTES, never assumed:
the chunk length and compressor come out of the ``CompressionInfo.db``
component (see ``read-compression-info.py``), the sizes out of ``stat``, the
sha256 out of the file itself. A DDL string in a manifest is documentation; the
CompressionInfo header is the fact.

Usage:
    write-perf-corpus-manifest.py --corpus-root DIR --keyspace KS --image IMG \
        --table=<name>:<published-sstable-dir> [--table=...]
"""

from __future__ import annotations

import argparse
import datetime as _dt
import glob
import hashlib
import importlib.util
import json
import os
import subprocess
import sys

# "read-compression-info.py" is not an importable module name (hyphens), so the
# CompressionInfo parser is loaded by path — one parser, one source of truth.
_spec = importlib.util.spec_from_file_location(
    "read_compression_info",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "read-compression-info.py"),
)
_rci = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_rci)

# The 8 components Cassandra 5.0 writes for a compressed BIG ("nb") SSTable.
EXPECTED_COMPONENTS = [
    "CompressionInfo.db",
    "Data.db",
    "Digest.crc32",
    "Filter.db",
    "Index.db",
    "Statistics.db",
    "Summary.db",
    "TOC.txt",
]


def sha256_of(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(8 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def one_component(sstable_dir: str, suffix: str) -> str:
    hits = glob.glob(os.path.join(sstable_dir, f"nb-*-{suffix}"))
    if len(hits) != 1:
        raise SystemExit(
            f"expected exactly 1 '{suffix}' in {sstable_dir}, found {len(hits)}: {hits}"
        )
    return hits[0]


def describe_table(keyspace: str, table: str, image_container: str | None) -> str | None:
    """Best-effort live DDL read; the manifest records it verbatim when available."""
    if not image_container:
        return None
    try:
        out = subprocess.run(
            ["sudo", "-n", "docker", "exec", image_container, "cqlsh", "-e",
             f"DESCRIBE TABLE {keyspace}.{table};"],
            capture_output=True, text=True, timeout=120,
        )
        return out.stdout.strip() or None
    except Exception:
        return None


def build_table_record(keyspace: str, table: str, sstable_dir: str,
                       container: str | None) -> dict:
    components = {}
    for entry in sorted(os.listdir(sstable_dir)):
        components[entry] = os.path.getsize(os.path.join(sstable_dir, entry))

    suffixes = sorted(
        {e.split("-", 3)[-1] for e in components if e.startswith("nb-")}
    )
    missing = [c for c in EXPECTED_COMPONENTS if c not in suffixes]

    data_db = one_component(sstable_dir, "Data.db")
    comp_info = one_component(sstable_dir, "CompressionInfo.db")
    ci = _rci.parse(comp_info)

    data_size = os.path.getsize(data_db)
    uncompressed = ci["uncompressed_data_length_bytes"]

    return {
        "table": table,
        "sstable_dir": sstable_dir,
        "sstable_basename": os.path.basename(data_db).rsplit("-Data.db", 1)[0],
        "format": "nb (BIG, Cassandra 5.0 default storage_compatibility_mode=CASSANDRA_4)",
        "data_db_count": len(glob.glob(os.path.join(sstable_dir, "*-Data.db"))),
        "components": components,
        "missing_components": missing,
        "data_db_bytes": data_size,
        "data_db_gib": round(data_size / 1024**3, 3),
        "data_db_sha256": sha256_of(data_db),
        "compression": {
            "source": "read from CompressionInfo.db header (authoritative)",
            "compressor": ci["compressor"],
            "options": ci["options"],
            "chunk_length_bytes": ci["chunk_length_bytes"],
            "chunk_length_kb": ci["chunk_length_kb"],
            "chunk_count": ci["chunk_count"],
            "uncompressed_data_length_bytes": uncompressed,
            "compression_ratio_on_disk_over_logical": (
                round(data_size / uncompressed, 6) if uncompressed else None
            ),
        },
        "ddl": describe_table(keyspace, table, container),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus-root", required=True)
    ap.add_argument("--keyspace", required=True)
    ap.add_argument("--image", required=True)
    ap.add_argument("--container", default="cqlite-perf3068")
    ap.add_argument("--rows-per-partition", type=int, default=10)
    ap.add_argument("--table", action="append", default=[],
                    help="<table-name>:<published sstable dir>")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    tables = []
    for spec in args.table:
        name, _, path = spec.partition(":")
        tables.append(build_table_record(args.keyspace, name, path, args.container))

    manifest = {
        "issue": 3068,
        "purpose": (
            "Field-shaped LZ4-compressed multi-GB single-SSTable Cassandra 5.0 corpus "
            "for read-plane (scan window / large I/O) measurement."
        ),
        "generated_utc": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "generator": "test-data/scripts/gen-perf-corpus-3068.sh",
        "method": (
            "cassandra-stress user profile (ships in the cassandra image), UNLOGGED "
            "single-partition batches, 10 rows/partition; durable_writes=false and "
            "autocompaction disabled during load (generation-time only — neither "
            "changes SSTable bytes), then nodetool flush + nodetool compact (major) "
            "to a single SSTable."
        ),
        "cassandra_image": args.image,
        "keyspace": args.keyspace,
        "rows_per_partition": args.rows_per_partition,
        "corpus_root": args.corpus_root,
        "datasets_root_usage": f"CQLITE_DATASETS_ROOT={args.corpus_root}",
        "tables": tables,
    }

    out = args.out or os.path.join(args.corpus_root, "manifest-3068.json")
    with open(out, "w") as fh:
        json.dump(manifest, fh, indent=2, sort_keys=False)
        fh.write("\n")
    print(f"[manifest] wrote {out}")
    for t in tables:
        c = t["compression"]
        print(
            f"[manifest] {t['table']}: Data.db {t['data_db_bytes']} B "
            f"({t['data_db_gib']} GiB), {c['compressor']} "
            f"chunk_length={c['chunk_length_bytes']} B, "
            f"ratio={c['compression_ratio_on_disk_over_logical']}, "
            f"sstables={t['data_db_count']}, missing_components={t['missing_components']}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
