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

FAIL-CLOSED: a NONZERO ``sstablemetadata`` exit (however complete its output looks), a
missing ``totalRows``, an unreadable ``Partition Size`` histogram, a non-``da``
descriptor, an empty or unreadable row plan, an empty SSTable list, an SSTable count or
generation set that is not one-per-planned-chunk, a plan-vs-``Statistics.db``
disagreement on EITHER the row count or the partition count, or a row plan that does not
describe THIS run's ``--seed`` / ``--rows-requested`` / ``--chunk-rows`` (a stale plan)
is an error -- never a fabricated 0 and never a silently-partial manifest.

A FIELD IS OBSERVED OR IT IS ABSENT — there is no third state, and no field is inferred
from a partial match (issue #3234 review rounds 9-10, whose findings were all one defect:
a claim asserted beyond what was checked). What that rule DELETED, rather than defended
with another guard: the fixed AC3 throughput figure and everything describing it (a
harness measurement, not derivable from any byte here — it lives in
docs/development/dev-cookbook.md); the fixed ``full_generation_golden`` block (already
recorded, observed, per SSTable); and ``corpus_committed`` / ``committed_copy`` /
``corpus_note``, prose inferred from a ``Data.db``-only hash comparison — reduced to the
one field ``data_db_sha256_also_match_at``, whose name states exactly what was compared.

Usage:
    write-perf-corpus-bti-manifest.py --corpus-root DIR --keyspace KS --table T \
        --sstable-dir DIR --image IMG --seed S --rows-requested N --chunk-rows N \
        --payload-bytes N --widths SPEC --buckets LIST \
        --mode smoke|production|small_golden --row-plan FILE \
        [--min-data-db-floor N] [--yaml-verified FILE] [--dumped=BASE ...] [--out FILE]
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


# ----------------------------------------------------------------------------
# WHY THERE IS NO THROUGHPUT FIGURE IN THIS MANIFEST (roborev #3234 M1).
#
# The AC3 warm-scan figure is a HISTORICAL measurement taken by a harness, not derivable
# from any byte on disk, so no corpus this script reads can substantiate it. It was a
# module constant guarded by a row + generation comparison, which let a corpus with a
# different seed, payload size or width mix INHERIT an unrelated number -- and an
# `applies_to_this_corpus: false` printed beside a present figure is the same defect
# wearing a label, since the number is still there to be quoted. So the figure, the
# identity comparator it would need, and the fixed `full_generation_golden` block (already
# recorded, OBSERVED, in the per-SSTable `sstabledump_golden` + `statistics` records) are
# DELETED rather than defended: A FIELD IS OBSERVED OR IT IS ABSENT. The figure and its
# LIMITATION live in docs/development/dev-cookbook.md, and cqlite-core/examples/
# bti_perf_scan prints them at runtime beside the number.
NO_MEASUREMENT_HERE = (
    "NONE. This manifest records only what was read back from the corpus bytes, and a "
    "throughput measurement is not one of those things (roborev #3234 M1: a fixed AC3 "
    "figure recorded here was inherited by any corpus with a matching row + generation "
    "count). The AC3 warm-scan figure, the route it measures and the BTI mmap/trie plane "
    "that route EXCLUDES are in docs/development/dev-cookbook.md, and "
    "cqlite-core/examples/bti_perf_scan prints them beside the number at runtime."
)


def sha256_of(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(8 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


# The checkout-relative datasets root the repo's committed fixtures live under, and
# the repo root this script sits in (test-data/scripts/<this file>).
COMMITTED_DATASETS_REL = "test-data/datasets"
REPO_ROOT = os.path.dirname(os.path.dirname(_HERE))


def data_db_sha256_match_path(sstable_dir: str, corpus_root: str,
                              sstables: list[dict]) -> str | None:
    """The checkout path where EVERY recorded ``Data.db`` sha256 was re-hashed and matched.

    The claim is exactly the check, and no wider (roborev #3234 M2). This used to be a
    `corpus_committed: true` + `committed_copy {files, bytes, verified: "describes the
    committed bytes"}` block built from a `Data.db`-ONLY hash comparison: it counted and
    summed files it never read, said nothing about git, and so reported "committed exact
    bytes" for a copy whose `Rows.db`/`Statistics.db`/`schema.cql` differed, or which was
    untracked in every commit -- partial verification reading as full verification.

    Growing the check to cover that claim would have meant a second validator inside a
    provenance writer, so the CLAIM was cut to the evidence instead: one field whose NAME
    states what was compared (`Data.db` sha256s) and nothing else, absent when no such path
    was found — never `false`, which invites reading it as "not committed", a thing
    this function cannot determine.
    """
    rel = os.path.relpath(os.path.abspath(sstable_dir), os.path.abspath(corpus_root))
    # An exact COMPONENT test, not `rel.startswith("..")`: the string form also rejects a
    # directory legitimately named `..something` (roborev #3234 F1/F2 audit -- prefix
    # semantics where a complete component is meant).
    if os.pardir in rel.split(os.sep):
        return None
    cand = os.path.join(REPO_ROOT, COMMITTED_DATASETS_REL, rel)
    if not os.path.isdir(cand):
        return None
    for s in sstables:
        data_db = os.path.join(cand, f"{s['sstable_basename']}-Data.db")
        if not os.path.exists(data_db) or sha256_of(data_db) != s["data_db_sha256"]:
            return None
    return f"{COMMITTED_DATASETS_REL}/{rel.replace(os.sep, '/')}"


PURPOSE = {
    "production": (
        "Profileable LZ4-compressed multi-SSTable Cassandra 5.0 BTI (`da`) corpus with "
        "wide partitions and a compound clustering key, for BTI read-path measurement "
        "(#3029 WS3, #3030 WS4) and as a Cassandra-written parity oracle."
    ),
    "smoke": (
        "Small end-to-end VALIDATION run of the BTI (`da`) perf-corpus generator: it "
        "exercises every fail-closed assert at throwaway scale. NOT the profileable "
        "production corpus and NOT a committed oracle."
    ),
    "small_golden": (
        "COMMITTABLE small Cassandra-written Cassandra 5.0 BTI (`da`) oracle with a wide "
        "partition and a compound clustering key, for BTI row/cell decode, `Rows.db` trie "
        "descent and compound-clustering-slice PARITY work (#3042: Cassandra-WRITTEN "
        "bytes). Deliberately NOT profileable — below the 8 MiB read-plane threshold."
    ),
}


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
    # FAIL-CLOSED ON THE EXIT STATUS, BEFORE PARSING ANYTHING (roborev #3234 M1).
    # The counts below are the manifest's authoritative row/partition provenance, and
    # `sstablemetadata` can print a plausible-looking `totalRows:` / `Partition Size:`
    # block and STILL fail afterwards — a partial read, a JVM error on a later
    # component, an OOM kill inside the memory-capped container. Parsing that output
    # would publish half-measured counts as measured, which is the same defect class as
    # the `partitions: 0` coercion this file already fixed: a counter not observed is an
    # error, never a fabricated value. EVERY nonzero code is rejected (a negative code is
    # a signal, i.e. the OOM case), and BOTH streams are reported so the failure is
    # diagnosable from the message alone.
    if proc.returncode != 0:
        raise SystemExit(
            f"sstablemetadata FAILED for {data_db} (exit {proc.returncode}) — refusing to "
            "read row/partition provenance out of the output of a command that did not "
            "succeed, however complete that output looks\n"
            f"  cmd={' '.join(cmd)}\n"
            f"  stdout={proc.stdout.strip()[:2000]}\n"
            f"  stderr={proc.stderr.strip()[:2000]}"
        )
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
    # An entry belongs to this SSTable when its DESCRIPTOR equals `basename` exactly --
    # split on the last `-`-separated descriptor boundary and compare, rather than
    # `e.startswith(f"{basename}-")` (issue #3234 F1/F2 audit). The prefix form decides
    # component MEMBERSHIP, i.e. which files this manifest describes, from a string
    # prefix; the split form cannot confuse one descriptor with another.
    def component_of(entry: str) -> str | None:
        m = re.match(r"^(?P<desc>da-\d+-bti)-(?P<comp>.+)$", entry)
        if m is None or m.group("desc") != basename or entry.endswith(".jsonl"):
            return None
        return m.group("comp")

    components = {
        e: os.path.getsize(os.path.join(sstable_dir, e))
        for e in sorted(os.listdir(sstable_dir))
        if component_of(e) is not None
    }
    suffixes = {component_of(e) for e in components}
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


GENERATION_RE = re.compile(r"^da-(\d+)-bti$")


def assert_one_sstable_per_chunk(basenames: list[str], plan: dict,
                                 sstable_dir: str) -> list[int]:
    """Fail closed unless the corpus is EXACTLY one SSTable per planned chunk.

    roborev #3234 M2: nothing verified the SSTable COUNT against the plan's chunk
    count. The aggregate row/partition cross-checks cannot see this — an unexpected
    flush split (two SSTables for one chunk) or a compaction (one SSTable for two
    chunks) preserves every row and every partition while VIOLATING the promised
    one-SSTable-per-chunk shape.

    It is not a cosmetic shape either: the GENERATION COUNT determines the scan route
    and is what the AC3 figure is attributed to ("27 generations, served by
    generation_merge::stream_generations_for_read"). A corpus that silently held a
    different number of generations would make that attribution wrong.

    The generation identifiers are additionally required to be exactly ``1..N``: that
    is the flush order a freshly-created table emits, so a GAP or an offset is direct
    evidence of the two failure modes above (a compaction promotes its output to a new,
    higher generation and removes the inputs). Returns the observed generations.
    """
    expected = len(plan["chunks"])
    problems: list[str] = []
    gens: list[int] = []
    unparsed: list[str] = []
    for b in basenames:
        m = GENERATION_RE.match(b)
        if m:
            gens.append(int(m.group(1)))
        else:
            unparsed.append(b)
    if unparsed:
        problems.append(
            f"generation identifier: {unparsed} do not match 'da-<gen>-bti' — the "
            "generation number is what the one-SSTable-per-chunk mapping is checked on"
        )
    if len(basenames) != expected:
        problems.append(
            f"SSTable count: {len(basenames)} SSTable(s) on disk, the row plan has "
            f"{expected} chunk(s) — the generator flushes ONCE PER CHUNK, so these two "
            "numbers must be equal"
        )
    if not unparsed and sorted(gens) != list(range(1, expected + 1)):
        problems.append(
            f"generation mapping: expected generations 1..{expected} (one per chunk, in "
            f"flush order), got {sorted(gens)}"
        )
    if problems:
        raise SystemExit(
            f"one-SSTable-per-chunk check FAILED for {sstable_dir} — this corpus does not "
            "have the shape this manifest would describe (an unexpected flush split, or a "
            "compaction merged chunks: autocompaction must be disabled BEFORE the first "
            "load). The generation COUNT also determines the scan route the AC3 figure is "
            "attributed to, so refusing to publish a manifest whose sstable_count does not "
            "match its own row plan:\n  - " + "\n  - ".join(problems)
        )
    return sorted(gens)


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
    ap.add_argument("--min-data-db-floor", type=int, default=8 * 1024 * 1024,
                    help="the largest-Data.db floor this run ENFORCED (the generator's "
                         "--min-data-db-bytes; 0 for --small-golden)")
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
    # The glob enumerates; DESCRIPTOR_RE decides (roborev #3234 F1/F2 audit). `da-*-bti-`
    # also matches a non-numeric generation, which the foreign-component check below
    # (same RE) would then call foreign -- two definitions of "a `da` descriptor" that
    # could disagree about the same file.
    datas = sorted(
        p for p in glob.glob(os.path.join(sstable_dir, "da-*-bti-Data.db"))
        if DESCRIPTOR_RE.match(os.path.basename(p))
    )
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
    # Also BEFORE the containers, and for the same reason: the shape check is pure
    # arithmetic over filenames, and a corpus that is not one-SSTable-per-chunk must not
    # cost N `sstablemetadata` starts before it is rejected.
    generations = assert_one_sstable_per_chunk(basenames, plan, sstable_dir)
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
    data_db_total = sum(s["data_db_bytes"] for s in sstables)
    match_path = data_db_sha256_match_path(sstable_dir, args.corpus_root, sstables)

    manifest = {
        "issue": 3234,
        "purpose": PURPOSE[args.mode],
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
        # WHERE this corpus was read from: the --corpus-root of THIS run, and the
        # env-var line that points a consumer at it. The former "IS COMMITTED / needs no
        # regeneration / multi-GB and NOT committed" narrative (`corpus_committed`,
        # `committed_copy`, `corpus_note`, `corpus_root_is_checkout_relative`) is gone:
        # it was prose inferred from a Data.db-only hash comparison, and inference is
        # not observation (roborev #3234 M2).
        "corpus_root": args.corpus_root,
        "datasets_root_usage": f"CQLITE_DATASETS_ROOT={args.corpus_root}",
        # Present only when a checkout path was found holding these exact Data.db bytes.
        # The key name IS the claim: Data.db sha256s, nothing else compared.
        **({"data_db_sha256_also_match_at": match_path} if match_path else {}),
        "reproducibility": {
            "reproduced_by_the_seed": (
                "the ROW SET: every pk/bucket/seq/payload value, the partition count, the "
                "rows-per-partition distribution and the chunk->SSTable split are a pure "
                "function of (seed, chunk-index) — see the row driver's determinism contract"
            ),
            "row_set_determinism_guaranteed_by": (
                "the row driver VENDORS its PRNG (MT19937 with CPython's str seeding) and "
                "both selection algorithms (weighted width draw, bucket sampling), so the "
                "seed identity recorded here does not depend on any standard-library "
                "implementation detail or on which `python3` runs it (roborev #3234 M2 — "
                "random.choices()/random.sample() are documented as changeable). Enforced "
                "by `gen-perf-corpus-bti-rows.py --self-check`: pinned CSV sha256 digests "
                "for fixed configurations, run as a case of "
                "scripts/tests/test_gen_perf_corpus_bti.sh. If that check ever fails, the "
                "seed identity recorded in this manifest is void — find what changed rather "
                "than re-pinning the digests."
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
            "one_sstable_per_chunk": (
                "the SSTable COUNT was checked against the row plan's chunk count and the "
                "generation identifiers against 1..N before this manifest was written; a "
                "flush split or a compaction preserves the row and partition totals while "
                "changing the generation count the scan route depends on, so a mismatch "
                "is a hard failure"
            ),
            "data_db_sha256_also_match_at": (
                "present only when a directory at the same corpus-relative path under the "
                f"checkout's {COMMITTED_DATASETS_REL} holds a Data.db per SSTable whose "
                "sha256 equals the one recorded here. That is ALL it asserts: no other "
                "component was compared and git tracking was not checked"
            ),
            "read_path_measurement": NO_MEASUREMENT_HERE,
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
                "sstable_generations": generations,
                "one_sstable_per_planned_chunk": True,
                "rows": observed_rows,
                "partitions": observed_partitions,
                "data_db_bytes_total": data_db_total,
                "data_db_bytes_largest": largest,
                "data_db_largest_gib": round(largest / 1024**3, 4),
                # The floor this run ENFORCED (--min-data-db-floor, 0 for the
                # small-golden oracle, which is deliberately below the read-plane
                # threshold) vs. the fixed 8 MiB MADV_RANDOM threshold itself. Emitting
                # 8 MiB as "the floor" for a run that enforced 0 read as a violated
                # requirement in the committed small-golden manifest (roborev #3234 L3).
                "min_data_db_floor_bytes": args.min_data_db_floor,
                "read_plane_threshold_bytes": 8 * 1024 * 1024,
                "meets_8mib_read_plane_floor": largest > 8 * 1024 * 1024,
                "rows_db_bytes_total": sum(s["rows_db_bytes"] for s in sstables),
                "every_rows_db_non_empty": all(s["rows_db_bytes"] > 0 for s in sstables),
                # NO hardcoded `clustering_key` / `clustering_arity`: they were literals
                # describing a schema this script never read. The captured DDL below IS
                # the observed schema — a consumer that needs the key shape reads it
                # there rather than trusting a constant (roborev #3234 M1/M2 rule: a
                # field is OBSERVED or ABSENT, and deriving these would cost a DDL parser
                # to substantiate two fields the DDL already states).
                "ddl": ddl,
                "sstables": sstables,
            }
        ],
        "row_count_cross_check": {
            "row_driver_rows": plan["rows"],
            "statistics_db_rows": observed_rows,
            "row_driver_partitions": plan["partitions"],
            "statistics_db_partitions": observed_partitions,
            # NO `agree: true`. It was a literal asserting what the four numbers beside it
            # already show, i.e. a claim in place of the evidence for it.
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
