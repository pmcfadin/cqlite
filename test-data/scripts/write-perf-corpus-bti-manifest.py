#!/usr/bin/env python3
"""Emit the issue-#3234 BTI (`da`) perf-corpus manifest.

Schema mirrors ``test-data/perf-corpus-3068-manifest.json`` (its BIG sibling) and
adds what a BTI corpus needs: one record per SSTable (the corpus is deliberately
multi-SSTable), the ``Partitions.db``/``Rows.db`` sizes, the recorded row-driver
seed, the observed rows-per-partition distribution, and the two cassandra.yaml
settings that were VERIFIED in the generating container.

Everything is READ BACK FROM THE WRITTEN BYTES, never assumed and NEVER inherited
from a previous manifest (issue #3234): this script does not read its own output
path, so a stale sha256 cannot survive a regeneration.

* sizes from ``os.stat``; the sha256 by hashing each ``Data.db``;
* compressor / chunk length / chunk count out of the ``CompressionInfo.db``
  header -- not the DDL, which a later ALTER could make a lie;
* row + partition counts from Cassandra's OWN ``sstablemetadata`` reading
  ``Statistics.db``, run per SSTable in a throwaway memory-capped container with
  the corpus dir bind-mounted READ-ONLY (so the manifest is regenerable from the
  corpus alone, with no live node);
* the rows-per-partition distribution from the row driver's plan records, which
  are counted while the CSV is written (observed, not requested).

FAIL-CLOSED: a missing ``totalRows``, a non-``da`` descriptor, an empty SSTable
list, or a plan-vs-Statistics.db row-count disagreement is an error -- never a
fabricated 0 and never a silently-partial manifest.

Usage:
    write-perf-corpus-bti-manifest.py --corpus-root DIR --keyspace KS --table T \
        --sstable-dir DIR --image IMG --seed S --rows-requested N --chunk-rows N \
        --payload-bytes N --widths SPEC --buckets LIST --mode smoke|production \
        --row-plan FILE [--yaml-verified FILE] [--dumped=BASE ...] [--out FILE]
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
from collections import Counter

_HERE = os.path.dirname(os.path.abspath(__file__))

# "read-compression-info.py" is not an importable module name (hyphens), so the
# CompressionInfo parser is loaded by path — one parser, one source of truth.
_spec = importlib.util.spec_from_file_location(
    "read_compression_info", os.path.join(_HERE, "read-compression-info.py")
)
_rci = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_rci)

# The 8 components Cassandra 5.0 writes for a compressed BTI ("da") SSTable.
# Note what is NOT here: Index.db and Summary.db are BIG-only.
EXPECTED_COMPONENTS = [
    "CompressionInfo.db",
    "Data.db",
    "Digest.crc32",
    "Filter.db",
    "Partitions.db",
    "Rows.db",
    "Statistics.db",
    "TOC.txt",
]
BIG_ONLY_COMPONENTS = ["Index.db", "Summary.db"]

SSTABLEMETADATA = "/opt/cassandra/tools/bin/sstablemetadata"
# Descriptor of an in-scope Cassandra 5.0 BTI SSTable: "da-<gen>-bti-<Component>".
DESCRIPTOR_RE = re.compile(r"^da-\d+-bti-")


def sha256_of(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(8 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def sstable_metadata(data_db: str, image: str, docker: list[str], mem: str) -> dict:
    """Read ``Statistics.db`` back with Cassandra's own offline ``sstablemetadata``."""
    sstable_dir = os.path.dirname(os.path.abspath(data_db))
    cmd = [
        *docker, "run", "--rm",
        "--memory", mem, "--memory-swap", mem,
        "-e", "MAX_HEAP_SIZE=1G", "-e", "HEAP_NEWSIZE=256M",
        "-v", f"{sstable_dir}:/data:ro",
        "--entrypoint", SSTABLEMETADATA, image,
        f"/data/{os.path.basename(data_db)}",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
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

    # Partition count = the sum of the "Partition Size:" histogram bucket counts,
    # rendered as "   <size> (<human>) | <count> (<pct>) OOO..." under that header.
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


def sstable_record(sstable_dir: str, basename: str, image: str, docker: list[str],
                   mem: str, dumped: set[str]) -> dict:
    """One record per SSTable generation, all of it read back from the bytes."""
    prefix = f"{basename}-"
    components = {
        e: os.path.getsize(os.path.join(sstable_dir, e))
        for e in sorted(os.listdir(sstable_dir))
        if e.startswith(prefix) and not e.endswith(".jsonl")
    }
    suffixes = {e[len(prefix):] for e in components}
    missing = [c for c in EXPECTED_COMPONENTS if c not in suffixes]
    if missing:
        raise SystemExit(f"{basename}: missing component(s) {missing}")
    present_big_only = [c for c in BIG_ONLY_COMPONENTS if c in suffixes]
    if present_big_only:
        raise SystemExit(
            f"{basename}: BIG-only component(s) {present_big_only} present — "
            "this is not a BTI SSTable"
        )

    toc_path = os.path.join(sstable_dir, f"{basename}-TOC.txt")
    with open(toc_path) as fh:
        toc = [ln.strip() for ln in fh if ln.strip()]
    for comp in BIG_ONLY_COMPONENTS:
        if comp in toc:
            raise SystemExit(f"{basename}: TOC.txt lists BIG-only component {comp}")
    for comp in ("Partitions.db", "Rows.db"):
        if comp not in toc:
            raise SystemExit(f"{basename}: TOC.txt does not list {comp}")

    data_db = os.path.join(sstable_dir, f"{basename}-Data.db")
    rows_db_bytes = components[f"{basename}-Rows.db"]
    if rows_db_bytes < 1:
        raise SystemExit(
            f"{basename}: Rows.db is EMPTY — no partition exceeded column_index_size, "
            "so there is no row-index trie in this SSTable"
        )
    ci = _rci.parse(os.path.join(sstable_dir, f"{basename}-CompressionInfo.db"))
    data_size = os.path.getsize(data_db)
    uncompressed = ci["uncompressed_data_length_bytes"]
    stats = sstable_metadata(data_db, image, docker, mem)
    golden = f"{basename}-Data.db.jsonl"
    golden_path = os.path.join(sstable_dir, golden)

    return {
        "sstable_basename": basename,
        "format": "da",
        "format_detail": (
            "da = BTI (trie-indexed), requires storage_compatibility_mode: NONE + "
            "sstable.selected_format: bti"
        ),
        "data_db_bytes": data_size,
        "data_db_mib": round(data_size / 1024**2, 3),
        "data_db_sha256": sha256_of(data_db),
        "partitions_db_bytes": components[f"{basename}-Partitions.db"],
        "rows_db_bytes": rows_db_bytes,
        "rows": stats["total_rows"],
        "statistics": stats,
        "components": components,
        "toc": toc,
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
        "sstabledump_golden": (
            {
                "file": golden,
                "bytes": os.path.getsize(golden_path),
                "tool": "cassandra sstabledump -l (line-delimited JSON, one partition per line)",
                "oracle": (
                    "Cassandra-WRITTEN bytes: usable as a parity oracle (issue #3042). "
                    "A CQLite-written round-trip fixture is not."
                ),
            }
            if basename in dumped and os.path.exists(golden_path)
            else None
        ),
    }


def aggregate_row_plan(path: str) -> dict:
    """Aggregate the row driver's per-chunk plan records (observed counts)."""
    widths: Counter[int] = Counter()
    buckets: Counter[int] = Counter()
    rows = partitions = 0
    chunks = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            rows += rec["rows"]
            partitions += rec["partitions"]
            for w, c in rec["rows_per_partition_histogram"].items():
                widths[int(w)] += c
            for b, c in rec["buckets_per_partition_histogram"].items():
                buckets[int(b)] += c
            chunks.append({
                "chunk": rec["chunk"],
                "seed_material": rec["seed_material"],
                "rows": rec["rows"],
                "partitions": rec["partitions"],
                "pk_min": rec["pk_min"],
                "pk_max": rec["pk_max"],
            })
    if not chunks:
        raise SystemExit(f"row plan {path} is empty — refusing to write a manifest")
    ordered = sorted(widths)
    return {
        "source": (
            "row driver plan records (gen-perf-corpus-bti-rows.py), counted while "
            "writing each CSV chunk — observed, not requested"
        ),
        "rows": rows,
        "partitions": partitions,
        "histogram": {str(w): widths[w] for w in ordered},
        "buckets_per_partition_histogram": {str(b): buckets[b] for b in sorted(buckets)},
        "min": ordered[0],
        "max": ordered[-1],
        "mean": round(rows / partitions, 3),
        "chunks": chunks,
    }


def ddl_from_schema(path: str, keyspace: str, table: str) -> dict:
    if not os.path.exists(path):
        return {"source": "unavailable", "keyspace_ddl": None, "table_ddl": None}
    with open(path) as fh:
        captured = fh.read()
    ks = re.search(rf"^CREATE KEYSPACE {re.escape(keyspace)} .*?;[ \t]*$",
                   captured, re.M | re.S)
    tbl = re.search(
        rf"^CREATE TABLE {re.escape(keyspace)}\.{re.escape(table)} \(.*?;[ \t]*$",
        captured, re.M | re.S,
    )
    return {
        "source": f"{os.path.basename(path)} captured at generation time "
                  "(cqlsh DESCRIBE KEYSPACE, alongside the SSTable components)",
        "keyspace_ddl": ks.group(0).strip() if ks else None,
        "table_ddl": tbl.group(0).strip() if tbl else None,
        "extracted_statements": bool(ks and tbl),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus-root", required=True)
    ap.add_argument("--keyspace", required=True)
    ap.add_argument("--table", required=True)
    ap.add_argument("--sstable-dir", required=True)
    ap.add_argument("--image", required=True)
    ap.add_argument("--docker", default="sudo -n docker")
    ap.add_argument("--metadata-mem", default="3g")
    ap.add_argument("--seed", required=True)
    ap.add_argument("--rows-requested", type=int, required=True)
    ap.add_argument("--chunk-rows", type=int, required=True)
    ap.add_argument("--payload-bytes", type=int, required=True)
    ap.add_argument("--widths", required=True)
    ap.add_argument("--buckets", required=True)
    ap.add_argument("--mode", required=True, choices=["smoke", "production"])
    ap.add_argument("--row-plan", required=True)
    ap.add_argument("--yaml-verified", default=None,
                    help="file holding the grep-verified cassandra.yaml lines")
    ap.add_argument("--dumped", action="append", default=[],
                    help="SSTable basename that has an sstabledump golden")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    sstable_dir = args.sstable_dir
    if not os.path.isdir(sstable_dir):
        raise SystemExit(f"--sstable-dir is not a directory: {sstable_dir}")

    # Fail closed on an empty SSTable list: an empty manifest describes nothing and
    # would overwrite a real provenance artifact with a lie.
    datas = sorted(glob.glob(os.path.join(sstable_dir, "da-*-bti-Data.db")))
    if not datas:
        raise SystemExit(f"no da-*-bti-Data.db in {sstable_dir} — refusing to write a manifest")
    foreign = [
        e for e in sorted(os.listdir(sstable_dir))
        if e.endswith(".db") and not DESCRIPTOR_RE.match(e)
    ]
    if foreign:
        raise SystemExit(
            f"non-BTI descriptor(s) in {sstable_dir}: {foreign} — a stock Cassandra 5.0 "
            "node emits 'nb' (BIG); both yaml settings must have been applied"
        )

    docker = args.docker.split()
    basenames = [os.path.basename(p)[: -len("-Data.db")] for p in datas]
    dumped = set(args.dumped)
    sstables = [
        sstable_record(sstable_dir, b, args.image, docker, args.metadata_mem, dumped)
        for b in basenames
    ]
    plan = aggregate_row_plan(args.row_plan)

    observed_rows = sum(s["rows"] for s in sstables)
    observed_partitions = sum(
        s["statistics"]["partition_count"] or 0 for s in sstables
    )
    if observed_rows != plan["rows"]:
        raise SystemExit(
            f"row-count cross-check FAILED: the row driver wrote {plan['rows']} rows, "
            f"Statistics.db across {len(sstables)} SSTable(s) accounts for "
            f"{observed_rows} — the load was partial or something was compacted away"
        )

    yaml_verified = None
    if args.yaml_verified and os.path.exists(args.yaml_verified):
        with open(args.yaml_verified) as fh:
            yaml_verified = [ln.rstrip("\n") for ln in fh if ln.strip()]

    schema_path = os.path.join(sstable_dir, "schema.cql")
    ddl = ddl_from_schema(schema_path, args.keyspace, args.table)
    largest = max(s["data_db_bytes"] for s in sstables)

    manifest = {
        "issue": 3234,
        "purpose": (
            "Profileable LZ4-compressed multi-SSTable Cassandra 5.0 BTI (`da`) corpus "
            "with wide partitions and a compound clustering key, for BTI read-path "
            "measurement (#3029 WS3, #3030 WS4) and as a Cassandra-written parity oracle."
        ),
        "generated_utc": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "generator": "test-data/scripts/gen-perf-corpus-bti.sh",
        "row_driver": "test-data/scripts/gen-perf-corpus-bti-rows.py",
        "mode": args.mode,
        "mode_note": (
            "smoke = the small end-to-end validation run (--smoke); it exercises every "
            "fail-closed assert but is NOT the profileable production corpus"
            if args.mode == "smoke" else
            "production = the full-scale corpus a read-path profile runs against"
        ),
        "method": (
            "recorded-seed deterministic CSV -> chunked cqlsh COPY -> explicit "
            "`nodetool flush` per chunk (one SSTable per chunk), with autocompaction "
            "DISABLED before the first load so STCS cannot merge the chunks. "
            "durable_writes=false is generation-time only and changes no SSTable byte. "
            "Deliberately NOT cassandra-stress (its row values are not reproducible "
            "from anything a manifest can record)."
        ),
        "cassandra_image": args.image,
        "cassandra_yaml_settings_required": {
            "storage_compatibility_mode": "NONE",
            "sstable.selected_format": "bti",
            "why": (
                "Cassandra 5.0 ships storage_compatibility_mode: CASSANDRA_4, which pins "
                "the BIG (`nb`) format. A miss on either setting emits `nb` with no error."
            ),
            "verified_in_container": yaml_verified,
            "verification": (
                "grep-verified in the generating container before the table was created; "
                "the emitted descriptors are then asserted to be da-*-bti-*"
            ),
        },
        "keyspace": args.keyspace,
        "table": args.table,
        "keyspace_table": f"{args.keyspace}.{args.table}",
        "keyspace_ddl": ddl["keyspace_ddl"],
        "seed": args.seed,
        "seed_note": (
            "the ROW SET (not the on-disk bytes) is reproducible from this seed alone: chunk "
            "N is generated with PRNG seed '<seed>:<N>' (see the row driver's determinism "
            "contract, and `reproducibility` for the write-timestamp caveat)"
        ),
        "row_driver_config": {
            "rows_requested": args.rows_requested,
            "chunk_rows": args.chunk_rows,
            "payload_bytes": args.payload_bytes,
            "widths": args.widths,
            "buckets": args.buckets,
        },
        "rows_per_partition": plan,
        "corpus_root": args.corpus_root,
        "datasets_root_usage": f"CQLITE_DATASETS_ROOT={args.corpus_root}",
        "corpus_committed": False,
        "corpus_note": (
            "The corpus itself is multi-GB and is NOT committed (.gitignore: *.db). "
            "Regenerate it with the generator + the recorded seed. Nothing in this "
            "manifest is inherited from a previous one; every number is re-read from "
            "the bytes on each run. See `reproducibility` for exactly what the seed "
            "does and does not reproduce."
        ),
        "reproducibility": {
            "reproduced_by_the_seed": (
                "the ROW SET: every pk/bucket/seq/payload value, the partition count, the "
                "rows-per-partition distribution and the chunk->SSTable split are a pure "
                "function of (seed, chunk-index) — see the row driver's determinism contract"
            ),
            "NOT_reproduced_by_the_seed": (
                "the Data.db BYTES. Cassandra stamps a wall-clock write timestamp on every "
                "row, serialized as an unsigned VInt DELTA from the Statistics.db "
                "min_timestamp baseline (docs/sstables-definitive-guide/chapters/"
                "05-data-db-format.md:77-78, :623), so a later run shifts some deltas across "
                "a VInt width boundary and the file length itself changes. MEASURED: two "
                "same-seed smoke runs of this generator produced Data.db of 19,474,015 B and "
                "19,474,397 B (a 382 B difference). Do NOT treat a sha256 mismatch after a "
                "regeneration as a defect."
            ),
            "what_the_sha256_is_for": (
                "identifying THIS corpus instance: a consumer can prove two measurements ran "
                "against the same bytes, and silent corruption or an accidental replacement "
                "is caught. It is an instance identity, not a regeneration check."
            ),
            "data_db_bytes_reproducible": False,
        },
        "provenance": {
            "sizes": "os.stat of each component file",
            "data_db_sha256": (
                "sha256 of the Data.db bytes, recomputed every run; identifies this corpus "
                "INSTANCE (see `reproducibility` — write timestamps make the bytes "
                "non-reproducible even at a fixed seed)"
            ),
            "compression": "CompressionInfo.db header (authoritative, not the DDL)",
            "rows": (
                "Cassandra sstablemetadata totalRows (Statistics.db) per SSTable; "
                "fail-closed, never a fabricated 0"
            ),
            "partitions": "sstablemetadata Partition Size histogram bucket counts",
            "rows_per_partition": "row driver plan records, counted while writing the CSV",
            "toc": "read from each SSTable's own TOC.txt",
            "ddl": "cqlsh DESCRIBE KEYSPACE captured at generation time (schema.cql)",
        },
        "tables": [
            {
                "table": args.table,
                "keyspace_table": f"{args.keyspace}.{args.table}",
                # Corpus-root-RELATIVE: the manifest is committed and must not pin
                # one machine's absolute layout.
                "sstable_dir": os.path.relpath(
                    os.path.abspath(sstable_dir), os.path.abspath(args.corpus_root)
                ),
                "format": "da",
                "sstable_count": len(sstables),
                "rows": observed_rows,
                "partitions": observed_partitions,
                "data_db_bytes_total": sum(s["data_db_bytes"] for s in sstables),
                "data_db_bytes_largest": largest,
                "data_db_largest_gib": round(largest / 1024**3, 4),
                "min_data_db_floor_bytes": 8 * 1024 * 1024,
                "meets_8mib_read_plane_floor": largest > 8 * 1024 * 1024,
                "rows_db_bytes_total": sum(s["rows_db_bytes"] for s in sstables),
                "every_rows_db_non_empty": all(s["rows_db_bytes"] > 0 for s in sstables),
                "clustering_key": ["bucket", "seq"],
                "clustering_arity": 2,
                "ddl": ddl,
                "sstables": sstables,
            }
        ],
        "row_count_cross_check": {
            "row_driver_rows": plan["rows"],
            "statistics_db_rows": observed_rows,
            "agree": True,
            "note": "fail-closed: a disagreement aborts before the manifest is written",
        },
    }

    out = args.out or os.path.join(args.corpus_root, "manifest-bti-3234.json")
    with open(out, "w") as fh:
        json.dump(manifest, fh, indent=2, sort_keys=False)
        fh.write("\n")
    print(f"[manifest] wrote {out}")
    print(
        f"[manifest] {args.keyspace}.{args.table} ({args.mode}): {len(sstables)} SSTable(s), "
        f"rows={observed_rows}, partitions={observed_partitions}, "
        f"largest Data.db={largest} B, rows/partition {plan['min']}..{plan['max']} "
        f"(mean {plan['mean']}), 8MiB floor met={largest > 8 * 1024 * 1024}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
