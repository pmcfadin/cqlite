#!/usr/bin/env python3
"""Emit the issue-#3068 perf-corpus manifest.

Everything recorded here is READ BACK FROM THE WRITTEN BYTES, never assumed:

* sizes come from ``stat``, the sha256 from hashing the file itself;
* the compressor / chunk length / chunk count come out of the
  ``CompressionInfo.db`` header (see ``read-compression-info.py``) -- NOT out of
  the table DDL, which a later ALTER or a Cassandra-side clamp could make a lie;
* the row count comes from Cassandra's OWN ``sstablemetadata`` reading
  ``Statistics.db`` (``totalRows``), run in a throwaway memory-capped container.

The row count is FAIL-CLOSED: if ``totalRows`` cannot be read back, the script
errors out rather than recording an unobserved number (a counter not observed is
an error, never a fabricated 0).

Usage:
    write-perf-corpus-manifest.py --corpus-root DIR --keyspace KS --image IMG \
        --table=<name>:<published-sstable-dir> [--table=...] [--out FILE]
"""

from __future__ import annotations

import argparse
import datetime as _dt
import glob
import hashlib
import importlib.util
import json
import os
import re
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


SSTABLEMETADATA = "/opt/cassandra/tools/bin/sstablemetadata"


def sstable_metadata(data_db: str, image: str, docker: list[str], mem: str) -> dict:
    """Read ``Statistics.db`` back with Cassandra's own offline ``sstablemetadata``.

    Runs in a throwaway container with the SSTable directory bind-mounted
    read-only, memory-capped, and with a small heap: the tool only reads the tiny
    Statistics component, so it must never be allowed to grow into the host's
    memory (see ``perf-run-contained.sh`` for why that matters on this corpus).
    """
    sstable_dir = os.path.dirname(os.path.abspath(data_db))
    cmd = [
        *docker, "run", "--rm",
        "--memory", mem, "--memory-swap", mem,
        "-e", "MAX_HEAP_SIZE=1G", "-e", "HEAP_NEWSIZE=256M",
        "-v", f"{sstable_dir}:/data:ro",
        "--entrypoint", SSTABLEMETADATA, image,
        f"/data/{os.path.basename(data_db)}",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
    text = proc.stdout
    rows = re.search(r"^totalRows:\s*(\d+)\s*$", text, re.M)
    if not rows:
        raise SystemExit(
            f"could not read totalRows from sstablemetadata for {data_db}\n"
            f"  exit={proc.returncode}\n  stderr={proc.stderr.strip()[:2000]}"
        )

    def _int(pattern: str) -> int | None:
        m = re.search(pattern, text, re.M)
        return int(m.group(1)) if m else None

    def _str(pattern: str) -> str | None:
        m = re.search(pattern, text, re.M)
        return m.group(1).strip() if m else None

    # Partition count = the sum of the "Partition Size:" histogram bucket counts.
    # Rendered as "   <size> (<human>) | <count> (<pct>) OOO..." under that header.
    partitions = None
    in_hist = False
    for line in text.splitlines():
        if line.startswith("Partition Size:"):
            in_hist = True
            continue
        if in_hist:
            if line.strip().startswith("Percentiles") or not line.startswith("   "):
                break
            bucket = re.match(r"\s*\d+.*?\|\s*(\d+)\s", line)
            if bucket:
                partitions = (partitions or 0) + int(bucket.group(1))
    return {
        "source": f"cassandra sstablemetadata ({image}) reading Statistics.db",
        "total_rows": int(rows.group(1)),
        "total_columns_set": _int(r"^totalColumnsSet:\s*(\d+)\s*$"),
        "partition_count": partitions,
        "partitioner": _str(r"^Partitioner:\s*(\S+)\s*$"),
        "cassandra_compression_ratio": _str(r"^Compression ratio:\s*(\S+)\s*$"),
        "sstable_level": _int(r"^SSTable Level:\s*(\d+)\s*$"),
        "estimated_droppable_tombstones": _str(
            r"^Estimated droppable tombstones:\s*(\S+)\s*$"
        ),
    }


def table_ddl(keyspace: str, table: str, sstable_dir: str,
              container: str | None, ddl_file: str | None) -> dict:
    """DDL for the manifest: prefer a live DESCRIBE, else the captured schema.cql.

    The DDL is documentation -- the CompressionInfo header remains the fact --
    so the source is recorded alongside it rather than silently interchanged.
    """
    if container:
        try:
            out = subprocess.run(
                ["sudo", "-n", "docker", "exec", container, "cqlsh", "-e",
                 f"DESCRIBE TABLE {keyspace}.{table};"],
                capture_output=True, text=True, timeout=120,
            )
            if out.returncode == 0 and out.stdout.strip():
                return {"source": f"live cqlsh DESCRIBE in container {container}",
                        "text": out.stdout.strip()}
        except Exception:
            pass

    path = ddl_file or os.path.join(sstable_dir, "schema.cql")
    if os.path.exists(path):
        with open(path) as fh:
            captured = fh.read()
        # Pull just this table's CREATE TABLE statement out of the captured file.
        # It ends at the first line-final ';' (the statement runs "CREATE TABLE
        # ks.t (...) WITH ... AND ...;", so the ')' is NOT the terminator).
        stmt = re.search(
            rf"^CREATE TABLE {re.escape(keyspace)}\.{re.escape(table)} \(.*?;[ \t]*$",
            captured, re.M | re.S,
        )
        return {
            "source": (
                f"{os.path.basename(path)} captured at generation time "
                "(alongside the SSTable components)"
            ),
            "text": (stmt.group(0) if stmt else captured).strip(),
            "extracted_statement": stmt is not None,
        }
    return {"source": "unavailable", "text": None, "extracted_statement": False}


def keyspace_ddl(keyspace: str, sstable_dirs: list[str]) -> str | None:
    """The CREATE KEYSPACE statement from the first captured schema.cql we find."""
    for sstable_dir in sstable_dirs:
        path = os.path.join(sstable_dir, "schema.cql")
        if not os.path.exists(path):
            continue
        with open(path) as fh:
            stmt = re.search(
                rf"^CREATE KEYSPACE {re.escape(keyspace)} .*?;[ \t]*$",
                fh.read(), re.M | re.S,
            )
        if stmt:
            return stmt.group(0).strip()
    return None


def build_table_record(keyspace: str, table: str, sstable_dir: str,
                       container: str | None, image: str, docker: list[str],
                       mem: str, ddl_file: str | None,
                       corpus_root: str) -> dict:
    # SSTable components only ("<version>-<gen>-<size>-<Component>"). Anything
    # else in the directory (a captured schema.cql, a stray tool artifact) is
    # listed separately so it can never masquerade as part of the SSTable.
    components, other_files = {}, {}
    for entry in sorted(os.listdir(sstable_dir)):
        size = os.path.getsize(os.path.join(sstable_dir, entry))
        target = components if re.match(r"^[a-z]{2}-\d+-big-", entry) else other_files
        target[entry] = size

    suffixes = sorted({e.split("-", 3)[-1] for e in components})
    missing = [c for c in EXPECTED_COMPONENTS if c not in suffixes]

    data_db = one_component(sstable_dir, "Data.db")
    comp_info = one_component(sstable_dir, "CompressionInfo.db")
    ci = _rci.parse(comp_info)

    data_size = os.path.getsize(data_db)
    uncompressed = ci["uncompressed_data_length_bytes"]
    stats = sstable_metadata(data_db, image, docker, mem)
    sstable_count = len(glob.glob(os.path.join(sstable_dir, "*-Data.db")))

    return {
        "table": table,
        "keyspace_table": f"{keyspace}.{table}",
        # Recorded relative to corpus_root: the manifest is committed and must not
        # pin one machine's absolute layout.
        "sstable_dir": os.path.relpath(os.path.abspath(sstable_dir),
                                       os.path.abspath(corpus_root)),
        "sstable_basename": os.path.basename(data_db).rsplit("-Data.db", 1)[0],
        "format": "nb",
        "format_detail": (
            "nb = BIG, Cassandra 5.0 default storage_compatibility_mode=CASSANDRA_4"
        ),
        "sstable_count": sstable_count,
        "single_sstable": sstable_count == 1,
        "data_db_count": sstable_count,
        "rows": stats["total_rows"],
        "statistics": stats,
        "components": components,
        "missing_components": missing,
        "non_component_files": other_files,
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
        "ddl": table_ddl(keyspace, table, sstable_dir, container, ddl_file),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus-root", required=True)
    ap.add_argument("--keyspace", required=True)
    ap.add_argument("--image", required=True)
    ap.add_argument("--container", default=None,
                    help="live container for a DESCRIBE TABLE read (optional; "
                         "schema.cql is used when absent or unreachable)")
    ap.add_argument("--docker", default="sudo -n docker",
                    help="docker invocation (default: 'sudo -n docker')")
    ap.add_argument("--metadata-mem", default="3g",
                    help="memory cap for the throwaway sstablemetadata container")
    ap.add_argument("--rows-per-partition", type=int, default=10)
    ap.add_argument("--table", action="append", default=[],
                    help="<table-name>:<published sstable dir>")
    ap.add_argument("--ddl-file", default=None,
                    help="schema.cql to source DDL from (default: "
                         "<sstable-dir>/schema.cql)")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    # Fail closed on an empty table list: an empty `tables` array is a manifest
    # that describes nothing, and writing one over the COMMITTED manifest would
    # silently destroy a provenance artifact (e.g. after a TABLES typo generated
    # no tables at all).
    if not args.table:
        raise SystemExit(
            "no --table given: refusing to write a manifest with an empty "
            "'tables' array (it would overwrite a real manifest with nothing). "
            "Pass at least one --table=<name>:<published sstable dir>."
        )

    docker = args.docker.split()
    tables, sstable_dirs = [], []
    for spec in args.table:
        name, sep, path = spec.partition(":")
        if not sep or not name.strip() or not path.strip():
            raise SystemExit(
                f"malformed --table {spec!r}: expected "
                "'<table-name>:<published sstable dir>'"
            )
        if not os.path.isdir(path):
            raise SystemExit(f"--table {name}: not a directory: {path}")
        sstable_dirs.append(path)
        tables.append(build_table_record(
            args.keyspace, name, path, args.container, args.image, docker,
            args.metadata_mem, args.ddl_file, args.corpus_root,
        ))

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
        "keyspace_ddl": keyspace_ddl(args.keyspace, sstable_dirs),
        "rows_per_partition": args.rows_per_partition,
        "corpus_root": args.corpus_root,
        "datasets_root_usage": f"CQLITE_DATASETS_ROOT={args.corpus_root}",
        "corpus_committed": False,
        "corpus_note": (
            "The corpus itself is multi-GB and is NOT committed. Regenerate it with "
            "the generator above, then re-run this script to reproduce this manifest; "
            "the Data.db sha256 recorded per table is the reproducibility check."
        ),
        "provenance": {
            "sizes": "os.stat of each component file",
            "data_db_sha256": "sha256 of the Data.db bytes",
            "compression": "CompressionInfo.db header (authoritative, not the DDL)",
            "rows": (
                "Cassandra sstablemetadata totalRows (Statistics.db); fail-closed, "
                "never a fabricated 0"
            ),
            "ddl": "recorded per table with its own source field",
        },
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
            f"[manifest] {t['table']}: rows={t['rows']}, "
            f"Data.db {t['data_db_bytes']} B ({t['data_db_gib']} GiB), "
            f"{c['compressor']} chunk_length={c['chunk_length_bytes']} B, "
            f"ratio={c['compression_ratio_on_disk_over_logical']}, "
            f"sstables={t['sstable_count']}, "
            f"missing_components={t['missing_components']}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
