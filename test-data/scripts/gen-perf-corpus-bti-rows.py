#!/usr/bin/env python3
"""Deterministic row driver for the issue-#3234 BTI (`da`) perf corpus.

Emits ONE chunk of CSV rows for `<keyspace>.<table>(pk, bucket, seq, payload)`
plus a one-line JSON *plan record* describing exactly what it emitted.

Why a recorded seed instead of `cassandra-stress` (the shape
`gen-perf-corpus-3068.sh` uses): `cassandra-stress`'s row values are not
reproducible from anything a manifest can record, so a regenerated corpus can
only ever be compared on aggregate counts. A recorded seed makes the ROW SET
itself reproducible, which is what lets the manifest's per-`Data.db` sha256 mean
something for a corpus nobody commits. See the generator's header.

Determinism contract:

* the PRNG is seeded from the string ``"<seed>:<chunk-index>"`` --
  ``random.Random`` hashes a ``str`` seed with sha512, which is stable across
  CPython versions and platforms, so chunk N of a given seed is byte-identical
  everywhere. Chunks are INDEPENDENT (no shared PRNG stream), so a chunk can be
  regenerated on its own.
* partition keys are ``chunk_index * pk_stride + i``, so no two chunks share a
  partition and every partition lives in exactly one SSTable. `pk` is a CQL
  ``int``, so the largest key is ``INT32_MAX``; :func:`plan_fits_int32` refuses an
  over-ceiling plan at validate time and the write loop re-checks per key.
* the plan record reports OBSERVED counts (rows/partitions actually written,
  the actual rows-per-partition histogram) -- never the requested numbers.

Shape (issue #3234 / #3029): ``PRIMARY KEY (pk, bucket, seq)`` is a COMPOUND
clustering key of two DIFFERING types (`text`, `int`). Bucket names have
distinct first bytes and heterogeneous lengths so the OSS50 byte-comparable
row-index separators branch immediately below the trie root instead of
degenerating into a chain of 2-byte single-transition nodes (the rationale
established by `gen-multiclustering-bti.sh` for #3032).

Usage:
    gen-perf-corpus-bti-rows.py --chunk-index 0 --rows 500000 --seed 20260803 \
        --payload-bytes 160 --widths 200:60,800:30,4000:10 \
        --buckets alpha,bo,charlie-extended-bucket,... \
        --out chunk-0.csv --plan-out row-plan.jsonl
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from collections import Counter

# `pk` is a CQL `int`, i.e. SIGNED 32-BIT: the largest partition key the table can
# hold is 2147483647. Since pk = chunk_index * PK_STRIDE + i, the stride bounds how
# many chunks a run may have -- and an over-large stride is NOT a harmless waste of
# key space, it is a hard failure part-way through a multi-GB load.
#
# MEASURED (issue #3234): with the original PK_STRIDE = 1_000_000_000, chunk 3's
# pk_base was 3_000_000_000 > INT32_MAX, so `cqlsh COPY` rejected EVERY row of that
# chunk with "'i' format requires -2147483648 <= number <= 2147483647" and the
# generator died at chunk 3 of 27 -- after ~4 minutes and 3 SSTables of work. The
# 2-chunk --smoke run never reached chunk 3, so the ceiling was invisible to it.
# Hence: a stride with generous but FINITE headroom, plus the two fail-closed
# asserts below (`plan_fits_int32` at validate time, the in-loop ceiling check at
# write time). The ceiling is a property of the COLUMN TYPE, so it is checked here
# rather than left to cqlsh to discover mid-load.
INT32_MAX = 2_147_483_647

# 1e6 partitions of headroom per chunk (~1400x the ~700 partitions a 500k-row chunk
# actually produces), which admits chunk indices 0..2147 -- i.e. a 2147-SSTable
# corpus, far beyond any profileable size. Partition keys still never collide
# across chunks (asserted below, not assumed).
PK_STRIDE = 1_000_000


def max_pk_of_plan(chunks: int, chunk_rows: int) -> int:
    """The largest partition key a (chunks, chunk_rows) plan can emit.

    Keys of chunk N span ``N * PK_STRIDE .. N * PK_STRIDE + partitions - 1`` and
    ``partitions <= rows`` always, so the last chunk's ceiling is
    ``(chunks - 1) * PK_STRIDE + chunk_rows - 1`` -- INCLUSIVE. The ``- 1`` is the
    whole point (roborev #3234 L4): omitting it rejected a plan whose final key is
    EXACTLY ``INT32_MAX``, which is a valid key.
    """
    return (chunks - 1) * PK_STRIDE + chunk_rows - 1


def plan_fits_int32(chunks: int, chunk_rows: int) -> None:
    """Fail closed if a (chunks, chunk_rows) plan would overflow the `pk int` column.

    Called by the generator's `validate_inputs` BEFORE any container starts, so an
    over-sized plan is refused up front instead of dying part-way through the load
    (issue #3234). The bound is INCLUSIVE (see :func:`max_pk_of_plan`): a plan whose
    largest key is exactly ``INT32_MAX`` fits and is accepted.
    """
    if chunks < 1 or chunk_rows < 1:
        raise SystemExit(f"plan_fits_int32: chunks={chunks} chunk_rows={chunk_rows} must be >= 1")
    max_pk = max_pk_of_plan(chunks, chunk_rows)
    if max_pk > INT32_MAX:
        # Largest chunk count that still fits, from the same inclusive arithmetic:
        # (c - 1) * STRIDE + chunk_rows - 1 <= INT32_MAX.
        max_chunks = (INT32_MAX - (chunk_rows - 1)) // PK_STRIDE + 1
        advice = (
            f"Use at most {max_chunks} chunks (raise --chunk-rows, or lower --rows)."
            if max_chunks >= 1
            else f"Even ONE chunk of {chunk_rows} rows cannot fit; lower --chunk-rows."
        )
        raise SystemExit(
            f"plan overflows the `pk int` column: {chunks} chunks x stride {PK_STRIDE} "
            f"reaches pk {max_pk} > INT32_MAX ({INT32_MAX}). cqlsh COPY would reject "
            f"every row of the first over-ceiling chunk. {advice}"
        )


def chunk_fits_int32(chunk_index: int, rows: int) -> int:
    """Return chunk `chunk_index`'s key base, failing closed if the chunk cannot fit.

    Same INCLUSIVE bound as :func:`plan_fits_int32`, applied to the ONE chunk this
    process is about to write: keys span ``pk_base .. pk_base + rows - 1`` at worst
    (one row per partition), so a chunk whose final key is exactly ``INT32_MAX``
    is accepted (roborev #3234 L4).
    """
    pk_base = chunk_index * PK_STRIDE
    if pk_base + rows - 1 > INT32_MAX:
        raise SystemExit(
            f"chunk {chunk_index}: pk_base {pk_base} + {rows} rows reaches pk "
            f"{pk_base + rows - 1} > INT32_MAX ({INT32_MAX}); the `pk int` column "
            f"cannot hold this chunk"
        )
    return pk_base


def parse_widths(spec: str) -> list[tuple[int, int]]:
    """``"200:60,800:30"`` -> ``[(200, 60), (800, 30)]`` (rows-per-partition, weight)."""
    out: list[tuple[int, int]] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        width, sep, weight = part.partition(":")
        if not sep:
            raise SystemExit(f"malformed --widths entry {part!r}: expected <rows>:<weight>")
        try:
            w, k = int(width), int(weight)
        except ValueError:
            raise SystemExit(f"malformed --widths entry {part!r}: non-integer") from None
        if w < 1 or k < 1:
            raise SystemExit(f"--widths entry {part!r}: rows and weight must both be >= 1")
        out.append((w, k))
    if not out:
        raise SystemExit("--widths is empty")
    return out


def parse_buckets(spec: str) -> list[str]:
    buckets = [b.strip() for b in spec.split(",") if b.strip()]
    if len(buckets) < 2:
        raise SystemExit("--buckets needs >= 2 names (a single bucket cannot branch the trie)")
    firsts = {b[0] for b in buckets}
    if len(firsts) != len(buckets):
        raise SystemExit(
            "--buckets names must have DISTINCT first bytes: they drive the row-index "
            "trie's depth-1 transition spread (see gen-multiclustering-bti.sh, #3032)"
        )
    for b in buckets:
        if any(c in b for c in ',"\n\\'):
            raise SystemExit(f"--buckets name {b!r} contains a CSV-hostile character")
    return buckets


def bucket_count_for(width: int, max_buckets: int) -> int:
    """How many clustering buckets a partition of `width` rows is split across.

    Deliberately width-dependent (not random): a wider partition spreads over
    more distinct first-byte transitions, so the per-partition tries differ in
    SHAPE and not merely in depth.
    """
    return max(2, min(max_buckets, width // 100))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunk-index", type=int, required=True)
    ap.add_argument("--rows", type=int, required=True,
                    help="rows to emit in THIS chunk (exact)")
    ap.add_argument("--seed", required=True,
                    help="corpus seed; the chunk PRNG is seeded '<seed>:<chunk-index>'")
    ap.add_argument("--payload-bytes", type=int, required=True)
    ap.add_argument("--widths", required=True,
                    help="rows-per-partition distribution, <rows>:<weight>[,...]")
    ap.add_argument("--buckets", required=True)
    ap.add_argument("--out", required=True, help="CSV destination")
    ap.add_argument("--plan-out", required=True,
                    help="JSONL file the plan record is APPENDED to")
    args = ap.parse_args()

    if args.chunk_index < 0:
        raise SystemExit("--chunk-index must be >= 0")
    if args.rows < 1:
        raise SystemExit("--rows must be >= 1")
    if args.payload_bytes < 8:
        raise SystemExit("--payload-bytes must be >= 8")

    widths = parse_widths(args.widths)
    buckets = parse_buckets(args.buckets)
    width_values = [w for w, _ in widths]
    width_weights = [k for _, k in widths]

    seed_material = f"{args.seed}:{args.chunk_index}"
    rnd = random.Random(seed_material)
    pk_base = chunk_fits_int32(args.chunk_index, args.rows)

    # Hex payload: one getrandbits() per row (fast, deterministic). Hex is ~2:1
    # LZ4-compressible, i.e. field-shaped rather than either incompressible noise
    # or a repeated string that would collapse to nothing on disk.
    hex_chars = args.payload_bytes
    bits = 4 * hex_chars

    remaining = args.rows
    rows_written = 0
    partitions = 0
    width_hist: Counter[int] = Counter()
    bucket_hist: Counter[int] = Counter()
    pk_max = pk_base

    with open(args.out, "w", newline="") as fh:
        write = fh.write
        while remaining > 0:
            if partitions >= PK_STRIDE:
                raise SystemExit(
                    f"chunk {args.chunk_index}: partition count reached the pk stride "
                    f"({PK_STRIDE}); lower --chunk-rows or raise the stride"
                )
            pk = pk_base + partitions
            # Absolute ceiling, independent of the stride: `pk` is a CQL `int`.
            # Without this, an over-ceiling key is only discovered by cqlsh, which
            # reports it as a per-batch ParseError mid-load (issue #3234).
            if pk > INT32_MAX:
                raise SystemExit(
                    f"chunk {args.chunk_index}: pk {pk} exceeds INT32_MAX "
                    f"({INT32_MAX}) — the `pk int` column cannot hold it"
                )
            # Partition-atomic, except the LAST partition of the chunk, which is
            # trimmed so the chunk emits EXACTLY --rows rows.
            width = min(rnd.choices(width_values, weights=width_weights, k=1)[0], remaining)
            nbuckets = min(bucket_count_for(width, len(buckets)), width)
            chosen = rnd.sample(buckets, nbuckets)
            base, extra = divmod(width, nbuckets)
            for bi, bucket in enumerate(chosen):
                n = base + (1 if bi < extra else 0)
                for seq in range(n):
                    payload = format(rnd.getrandbits(bits), f"0{hex_chars}x")
                    write(f"{pk},{bucket},{seq},{payload}\n")
            width_hist[width] += 1
            bucket_hist[nbuckets] += 1
            rows_written += width
            remaining -= width
            pk_max = pk
            partitions += 1

    if rows_written != args.rows:
        raise SystemExit(
            f"internal error: emitted {rows_written} rows, requested {args.rows}"
        )

    observed_widths = sorted(width_hist)
    plan = {
        "chunk": args.chunk_index,
        "seed": args.seed,
        "seed_material": seed_material,
        "rows": rows_written,
        "partitions": partitions,
        "pk_min": pk_base,
        "pk_max": pk_max,
        "pk_stride": PK_STRIDE,
        "payload_bytes": args.payload_bytes,
        "rows_per_partition_histogram": {str(w): width_hist[w] for w in observed_widths},
        "buckets_per_partition_histogram": {
            str(b): bucket_hist[b] for b in sorted(bucket_hist)
        },
        "rows_per_partition_min": observed_widths[0],
        "rows_per_partition_max": observed_widths[-1],
        "rows_per_partition_mean": round(rows_written / partitions, 3),
        "source": "counted while writing the CSV (observed, not requested)",
    }
    with open(args.plan_out, "a") as fh:
        fh.write(json.dumps(plan, sort_keys=False) + "\n")

    print(
        f"[rows] chunk {args.chunk_index}: {rows_written} rows / {partitions} partitions "
        f"(width {observed_widths[0]}..{observed_widths[-1]}, "
        f"pk {pk_base}..{pk_max}, seed_material {seed_material}) -> {args.out}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
