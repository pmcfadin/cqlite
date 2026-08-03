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

FAIL-CLOSED: a missing ``totalRows``, an unreadable ``Partition Size`` histogram, a
non-``da`` descriptor, an empty or unreadable row plan, an empty SSTable list, a
plan-vs-``Statistics.db`` disagreement on EITHER the row count or the partition
count, or a row plan that does not describe THIS run's ``--seed`` /
``--rows-requested`` / ``--chunk-rows`` (a stale plan) is an error -- never a
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


# --------------------------------------------------------------------------
# SCOPE OF THE AC3 FIGURE (issue #3234, owner-required, PART 3).
#
# The AC3 throughput number was taken through the GENERATION-MERGE STITCH, which
# EXCLUDES the BTI mmap/trie plane entirely: a full scan of this corpus is not
# partition-key-constrained, so it resolves to the fallback full scan and is served by
# `generation_merge::stream_generations_for_read` across all 27 generations -- and on
# that route each generation is re-opened by its own compaction-style producer with
# `use_mmap = false` / `DiskAccessMode::Buffered`
# (storage/write_engine/merge/producer_iter.rs:364-388) and walked sequentially
# through `Data.db` (`stream_all_partitions_for_compaction`). No MADV_RANDOM mapping
# is created and no Partitions.db/Rows.db trie descent happens inside the measured
# window. That is what `bti_perf_scan` prints at runtime, and this text must say the
# same thing (roborev #3234 M3: it previously claimed the trie/mmap work happened
# INSIDE the stitch and was merely un-isolated, which misattributes the figure).
# Recorded HERE and in docs/development/dev-cookbook.md so the limitation is not
# discoverable only by reading the issue thread.
#
# `recorded_figure` is a HISTORICAL measurement with its provenance attached, not a
# counter this run observed — hence `applies_to_this_corpus`, computed by comparing
# the corpus it was measured on against the corpus being described.
AC3_RECORDED_FIGURE = {
    "access_path": "fallback_full_scan (partition_key_not_fully_constrained)",
    "storage_route": "generation_merge::stream_generations_for_read",
    "generations": 27,
    "wall_seconds": 127.163,
    "rows": 13200000,
    "rows_per_second": 103804,
    "measured_by": (
        "cqlite-core/examples/bti_perf_scan (the AC3 warm-scan harness), against the "
        "27-generation production corpus described by this manifest"
    ),
    "measured_on_utc": "2026-08-03",
}


def measurement_scope(observed_rows: int, generations: int) -> dict:
    """The route the AC3 figure measures, and what it therefore does NOT measure."""
    applies = (
        observed_rows == AC3_RECORDED_FIGURE["rows"]
        and generations == AC3_RECORDED_FIGURE["generations"]
    )
    return {
        "what_the_ac3_figure_measures": (
            "the GENERATION-MERGE STITCH route, over BUFFERED I/O. A full scan of this "
            "corpus is not partition-key-constrained, so it takes the fallback full-scan "
            "access path and is served by generation_merge::stream_generations_for_read "
            "across every generation: one sequential compaction-style producer per "
            "generation, each of which RE-OPENS its SSTable with use_mmap=false / "
            "DiskAccessMode::Buffered (storage/write_engine/merge/producer_iter.rs:"
            "364-388) and walks Data.db via stream_all_partitions_for_compaction. So the "
            "figure is Data.db decode + k-way merge throughput over buffered reads."
        ),
        "LIMITATION": (
            "This route EXCLUDES the BTI mmap/trie plane, which is therefore ENTIRELY "
            "UNMEASURED -- not merely un-isolated. Because every producer re-opens its "
            "SSTable with buffered I/O and walks Data.db sequentially, no MADV_RANDOM "
            "mapping is created and NO Partitions.db/Rows.db trie descent happens inside "
            "the measured window (SSTable open, 0.033 s for 27 SSTables, is outside it). "
            "Quoting this number as a BTI index-plane baseline would make every A/B "
            "against it wrong by an unknown factor. The index plane needs its own "
            "measurement, on the single-generation scan_stream route where a BTI reader "
            "takes the trie branch (#3029 WS3 / #3030 WS4). cqlite-core/examples/"
            "bti_perf_scan.rs prints the same statement at runtime beside the number "
            "(access_path + storage_route)."
        ),
        "recorded_figure": AC3_RECORDED_FIGURE,
        "applies_to_this_corpus": applies,
        "applies_to_this_corpus_note": (
            "true only when the corpus described here has the same row count and "
            "generation count the figure was measured on; a regenerated corpus of a "
            "different shape makes the recorded figure historical only — re-measure with "
            "the harness rather than editing the number"
        ),
        "full_generation_golden": {
            "committed": False,
            "generated_on_demand": True,
            "bytes": 160752721,
            "mib": 153.3,
            "what": (
                "`sstabledump -l` of ONE generation (da-1-bti-Data.db) of this corpus, "
                "i.e. 500,000 rows across 711 partitions"
            ),
            "verified": (
                "correct: 711 partitions and EXACTLY 500,000 row objects, cross-checked "
                "against that SSTable's own Statistics.db (totalRows) by the generator's "
                "verify_dumped_row_counts step"
            ),
            "why_not_committed": (
                "153.3 MiB of derived JSON. It stays generated-on-demand "
                "(--dump-generations 1 during generation, or sstabledump -l against the "
                "corpus); the COMMITTABLE Cassandra-written BTI oracle is the separate "
                "small golden (gen-perf-corpus-bti.sh --small-golden)."
            ),
        },
    }


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
    # FAIL-CLOSED (issue #3234 review): a histogram this parser cannot read is an
    # UNOBSERVED counter, and an unobserved counter is an error — never a 0. The
    # previous shape returned None here and the caller coerced it with `or 0`, which
    # would have published "partitions: 0" (or a partial sum) as measured
    # provenance. CLAUDE.md: "a counter not observed is an error, never a
    # fabricated 0".
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
    if partitions is None:
        raise SystemExit(
            "could not read the 'Partition Size:' histogram from sstablemetadata for "
            f"{data_db} — refusing to publish an unobserved partition count\n"
            f"  exit={proc.returncode}\n  stderr={proc.stderr.strip()[:2000]}"
        )
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


PLAN_RECORD_FIELDS = (
    "chunk", "seed_material", "rows", "partitions", "pk_min", "pk_max",
    "rows_per_partition_histogram", "buckets_per_partition_histogram",
)


def aggregate_row_plan(path: str) -> dict:
    """Aggregate the row driver's per-chunk plan records (observed counts).

    Every malformed-input path is an actionable ``SystemExit``: a truncated or
    hand-edited plan line must name the file and the line number, not surface as a
    bare ``JSONDecodeError``/``KeyError`` traceback from inside the aggregation.
    """
    widths: Counter[int] = Counter()
    buckets: Counter[int] = Counter()
    rows = partitions = 0
    chunks = []
    with open(path) as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SystemExit(
                    f"row plan {path}:{lineno} is not valid JSON ({exc}) — refusing to "
                    "write a manifest from an unreadable plan"
                ) from None
            if not isinstance(rec, dict):
                raise SystemExit(
                    f"row plan {path}:{lineno} is not a JSON object — refusing to write "
                    "a manifest from an unreadable plan"
                )
            absent = [k for k in PLAN_RECORD_FIELDS if k not in rec]
            if absent:
                raise SystemExit(
                    f"row plan {path}:{lineno} is missing field(s) {absent} — the row "
                    "driver (gen-perf-corpus-bti-rows.py) writes all of "
                    f"{list(PLAN_RECORD_FIELDS)}; refusing to write a manifest from a "
                    "partial plan record"
                )
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
    if rows < 1 or partitions < 1:
        raise SystemExit(
            f"row plan {path} accounts for {rows} row(s) in {partitions} partition(s) — "
            "a plan that wrote nothing cannot describe a corpus"
        )
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


def assert_plan_describes_config(plan: dict, seed: str, rows_requested: int,
                                 chunk_rows: int, path: str) -> None:
    """Fail closed unless the row plan is the plan THIS configuration produced.

    The aggregate row/partition cross-checks against ``Statistics.db`` cannot see a
    STALE plan (roborev #3234 F3): a plan from an earlier run with a different seed
    can carry coincidentally-matching totals, and the manifest would then publish a
    declared ``seed`` and generation plan that do not describe the corpus. So every
    structural property the generator's own arithmetic fixes is re-derived here and
    compared against the plan records:

    * the chunk COUNT — ``ceil(rows_requested / chunk_rows)``;
    * the chunk INDEX SET — exactly ``0..N-1``, contiguous, no duplicates
      (a gap means a chunk's rows are in the corpus but not in the plan);
    * each record's ``seed_material`` — the row driver's ``"<seed>:<chunk>"``, which
      is the only thing that ties the plan to the DECLARED seed;
    * each record's row count — ``chunk_rows`` for every chunk but the last, whose
      remainder is fixed by the same arithmetic; and
    * the total — ``rows_requested``.
    """
    if chunk_rows < 1 or rows_requested < 1:
        raise SystemExit(
            "row-plan/config check FAILED: --rows-requested and --chunk-rows must both "
            f"be >= 1 (got {rows_requested} and {chunk_rows})"
        )
    expected_chunks = -(-rows_requested // chunk_rows)  # ceil
    records = plan["chunks"]
    problems: list[str] = []
    if len(records) != expected_chunks:
        problems.append(
            f"chunk count: the plan holds {len(records)} record(s), this configuration "
            f"({rows_requested} rows / {chunk_rows} per chunk) produces {expected_chunks}"
        )
    indices = [r["chunk"] for r in records]
    if sorted(indices) != list(range(expected_chunks)):
        problems.append(
            f"chunk index set: expected a contiguous 0..{expected_chunks - 1}, got "
            f"{sorted(indices)}"
        )
    for rec in records:
        idx = rec["chunk"]
        want_material = f"{seed}:{idx}"
        if str(rec["seed_material"]) != want_material:
            problems.append(
                f"chunk {idx}: seed_material is {rec['seed_material']!r}, this run declares "
                f"seed {seed!r} so it must be {want_material!r}"
            )
        if not isinstance(idx, int) or not 0 <= idx < expected_chunks:
            continue  # already reported by the index-set check
        want_rows = min(chunk_rows, max(rows_requested - idx * chunk_rows, 0))
        if rec["rows"] != want_rows:
            problems.append(
                f"chunk {idx}: the plan records {rec['rows']} row(s), this configuration "
                f"puts {want_rows} there"
            )
    if plan["rows"] != rows_requested:
        problems.append(
            f"total rows: the plan accounts for {plan['rows']}, --rows-requested is "
            f"{rows_requested}"
        )
    if problems:
        raise SystemExit(
            f"row-plan/config check FAILED for {path} — this plan does not describe the "
            "requested generation (a STALE plan file, or a plan from a different run: "
            "matching aggregate totals are NOT sufficient, so refusing to publish a "
            "manifest whose declared seed and generation plan do not describe the "
            "corpus):\n  - " + "\n  - ".join(problems)
        )


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
    ap.add_argument("--mode", required=True,
                    choices=["smoke", "production", "small_golden"])
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

    # The row plan is read and checked against THIS run's configuration BEFORE the
    # per-SSTable `sstablemetadata` containers: a stale plan is a hard failure, and
    # discovering it cheaply beats discovering it after N container starts.
    plan = aggregate_row_plan(args.row_plan)
    assert_plan_describes_config(
        plan, args.seed, args.rows_requested, args.chunk_rows, args.row_plan
    )

    docker = args.docker.split()
    basenames = [os.path.basename(p)[: -len("-Data.db")] for p in datas]
    dumped = set(args.dumped)
    sstables = [
        sstable_record(sstable_dir, b, args.image, docker, args.metadata_mem, dumped)
        for b in basenames
    ]

    observed_rows = sum(s["rows"] for s in sstables)
    # FAIL-CLOSED on an unobserved partition count. `sstable_metadata` already
    # refuses to return None, so this is the second half of the same contract: no
    # arithmetic here may coerce a missing measurement into a number (the previous
    # `or 0` did exactly that, publishing FALSE partition provenance).
    unobserved = [
        s["sstable_basename"] for s in sstables
        if not isinstance(s["statistics"]["partition_count"], int)
    ]
    if unobserved:
        raise SystemExit(
            f"partition count was NOT OBSERVED for {unobserved} — refusing to write a "
            "manifest that reports a fabricated partition count"
        )
    observed_partitions = sum(s["statistics"]["partition_count"] for s in sstables)
    if observed_rows != plan["rows"]:
        raise SystemExit(
            f"row-count cross-check FAILED: the row driver wrote {plan['rows']} rows, "
            f"Statistics.db across {len(sstables)} SSTable(s) accounts for "
            f"{observed_rows} — the load was partial or something was compacted away"
        )
    # Partitions cross-check, same shape as the rows one. Sound as an EQUALITY: the
    # row driver gives chunk N the partition-key range [N*PK_STRIDE, ...), so no two
    # chunks share a partition and no partition can be counted twice; and the
    # Partition Size histogram's BUCKET BOUNDARIES are estimated while its bucket
    # COUNTS are exact, so the sum is the exact partition count of that SSTable.
    # Verified against the production corpus: 17299 planned == 17299 observed over 27
    # SSTables. A disagreement means a partial load, an unexpected compaction, or a
    # plan that does not describe these bytes.
    if observed_partitions != plan["partitions"]:
        raise SystemExit(
            f"partition-count cross-check FAILED: the row driver wrote "
            f"{plan['partitions']} partitions, the Statistics.db partition-size "
            f"histograms across {len(sstables)} SSTable(s) account for "
            f"{observed_partitions} — the load was partial, something was compacted "
            "away, or this row plan does not describe these SSTables"
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
        "mode_note": {
            "smoke": (
                "smoke = the small end-to-end validation run (--smoke); it exercises every "
                "fail-closed assert but is NOT the profileable production corpus"
            ),
            "production": (
                "production = the full-scale corpus a read-path profile runs against"
            ),
            "small_golden": (
                "small_golden = the COMMITTABLE small Cassandra-written BTI (`da`) oracle "
                "(--small-golden). Same PRIMARY KEY (pk, bucket, seq) shape as the perf "
                "corpus at a committable size; it is a CORRECTNESS oracle (#3042: "
                "Cassandra-WRITTEN bytes), NOT a profile target — it is deliberately below "
                "the 8 MiB read-plane floor."
            ),
        }[args.mode],
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
            "partitions": (
                "sstablemetadata Partition Size histogram bucket counts, cross-checked "
                "against the row driver's planned partition count; fail-closed, never a "
                "fabricated 0"
            ),
            "rows_per_partition": "row driver plan records, counted while writing the CSV",
            "toc": "read from each SSTable's own TOC.txt",
            "ddl": "cqlsh DESCRIBE KEYSPACE captured at generation time (schema.cql)",
            "row_plan_matches_config": (
                "the row plan's chunk count, contiguous chunk index set, per-chunk row "
                "counts and per-chunk seed material were checked against --seed / "
                "--rows-requested / --chunk-rows before this manifest was written; a stale "
                "plan is a hard failure, because matching aggregate totals alone cannot "
                "prove the plan describes these bytes"
            ),
        },
        "read_path_measurement_scope": measurement_scope(observed_rows, len(sstables)),
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
            "row_driver_partitions": plan["partitions"],
            "statistics_db_partitions": observed_partitions,
            "agree": True,
            "note": (
                "fail-closed: a rows OR partitions disagreement aborts before the "
                "manifest is written, and an UNOBSERVED partition count is an error "
                "rather than a 0"
            ),
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
