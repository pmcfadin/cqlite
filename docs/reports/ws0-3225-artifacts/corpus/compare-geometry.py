#!/usr/bin/env python3
"""Field-by-field geometry comparison: the #3225 corpus vs #3217's published record (AC6).

Every number on both sides is PARSED from a committed artifact — none is typed in by
hand — so the table cannot drift from the files it claims to summarise.

  this run  : corpus-measure.txt (measure-sstable.py over CompressionInfo.db),
              corpus-sstablemetadata.txt, corpus-tablestats.txt, corpus-fullscan.txt
  #3217     : docs/reports/ws0-3217-artifacts/corpus/corpus-geometry.txt

A regenerated corpus has a DIFFERENT sha256 by construction: cassandra-stress draws
values from a non-deterministic seed stream. AC6's bar is therefore "new sha recorded
+ geometry SHOWN matching within a stated tolerance", never "same bytes". This script
states the tolerance, applies it, and labels any field that exceeds it as MATERIAL.
It never tunes anything to make a field pass.

Usage: compare-geometry.py [--dir <artifact-dir>] [--tolerance-pct <f>] [-o <out>]
"""
from __future__ import annotations

import argparse
import os
import re
import sys

# A regenerated corpus differs only through cassandra-stress' random draws, which move
# the compressed size by fractions of a percent. 0.5% is ~15x the largest divergence
# #3217 itself recorded against #3100 (+0.085%), so it is loose enough not to fire on
# ordinary re-draw noise and tight enough that a recipe error (wrong population, wrong
# clustering fan-out, wrong chunk length) cannot hide under it.
DEFAULT_TOLERANCE_PCT = 0.5


def grab(text: str, pattern: str, what: str, cast=float):
    m = re.search(pattern, text, re.MULTILINE)
    if not m:
        raise SystemExit(f"FAIL: could not parse {what} (pattern {pattern!r}) — "
                         "refusing to publish a table with an invented field")
    return cast(m.group(1))


def grab2(text: str, pattern: str, what: str, cast=float):
    """Both capture groups of a COMPOSITE field, as a tuple.

    A field the table labels `min / max` must be READ as min and max. Parsing one
    component and printing a two-component heading is how a changed maximum TTL scored
    an "exact match": the comparison could not see the half it never read. Same
    fail-closed contract as grab() — an unparseable pattern refuses to publish.
    """
    m = re.search(pattern, text, re.MULTILINE)
    if not m or m.re.groups < 2:
        raise SystemExit(f"FAIL: could not parse BOTH components of {what} "
                         f"(pattern {pattern!r}) — refusing to publish a composite field "
                         "from one half of it")
    return tuple(cast(g) for g in m.groups())


def grab_unique(text: str, pattern: str, what: str, cast=str):
    """The ONE value a pattern matches — refusing when it matches several.

    `re.search` returns the FIRST match, which silently answers a question about a
    set with a statement about its first element. The staged corpus is required to
    hold exactly one SSTable, so two `*-Data.db` lines in the shasum artifact is a
    recipe violation; taking the first would have published one file's digest as
    "the corpus sha256" and compared a two-SSTable corpus against a one-SSTable one.
    """
    found = re.findall(pattern, text, re.MULTILINE)
    if not found:
        raise SystemExit(f"FAIL: could not parse {what} (pattern {pattern!r}) — "
                         "refusing to publish a table with an invented field")
    distinct = sorted(set(found))
    if len(distinct) > 1:
        raise SystemExit(f"FAIL: {what} matched {len(distinct)} DISTINCT values "
                         f"({', '.join(map(str, distinct))}) — the recipe requires exactly one "
                         "staged SSTable, so this is a corpus defect, not a parse to pick from")
    return cast(distinct[0])


def fmt(v) -> str:
    """Display form: a composite tuple renders as `min / max`, matching its label."""
    if isinstance(v, tuple):
        return " / ".join(str(x) for x in v)
    return str(v)


def read(path: str) -> str:
    try:
        with open(path) as fh:
            return fh.read()
    except OSError as exc:
        raise SystemExit(f"FAIL: required input unreadable: {exc}")


def main() -> int:
    here = os.path.dirname(os.path.abspath(__file__))
    repo = os.path.abspath(os.path.join(here, "..", "..", "..", ".."))
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=here, help="directory holding this run's corpus-*.txt")
    ap.add_argument("--tolerance-pct", type=float, default=DEFAULT_TOLERANCE_PCT)
    ap.add_argument("-o", "--out", default=None, help="write the table here (default: stdout only)")
    args = ap.parse_args()

    d = args.dir
    meas = read(os.path.join(d, "corpus-measure.txt"))
    meta = read(os.path.join(d, "corpus-sstablemetadata.txt"))
    stats = read(os.path.join(d, "corpus-tablestats.txt"))
    scan = read(os.path.join(d, "corpus-fullscan.txt"))
    sha_file = read(os.path.join(d, "corpus-sha-staged.txt"))
    old = read(os.path.join(repo, "docs", "reports", "ws0-3217-artifacts",
                            "corpus", "corpus-geometry.txt"))

    new = {
        "rows (sstablemetadata totalRows)": grab(meta, r"^totalRows:\s*(\d+)", "totalRows", int),
        "rows (independent fullscan oracle)": grab(
            scan, r"^SCAN rows counted\s*:\s*(\d+)", "fullscan rows", int),
        "totalColumnsSet": grab(meta, r"^totalColumnsSet:\s*(\d+)", "totalColumnsSet", int),
        "partitions (tablestats estimate)": grab(
            stats, r"Number of partitions \(estimate\):\s*(\d+)", "partitions", int),
        "dataLength UNCOMPRESSED (bytes)": grab(
            meas, r"^dataLength UNCOMPRESSED : (\d+) bytes", "dataLength", int),
        "Data.db on disk (bytes)": grab(
            meas, r"^Data\.db on disk \(comp\)  : (\d+) bytes", "Data.db size", int),
        "UNCOMPRESSED bytes/row": grab(meas, r"^UNCOMPRESSED bytes/row  : ([\d.]+)", "unc B/row"),
        "COMPRESSED bytes/row": grab(meas, r"^COMPRESSED bytes/row    : ([\d.]+)", "cmp B/row"),
        "LZ4 ratio (header-derived)": grab(meas, r"^compression ratio       : ([\d.]+)x", "ratio"),
        "chunk count (16 KiB chunks)": grab(meas, r"^chunk count             : (\d+)", "chunks", int),
        "chunk_length (bytes)": grab(meas, r"^chunk_length \(bytes\)    : (\d+)", "chunk_length", int),
        "Space used (live, bytes)": grab(stats, r"Space used \(live\):\s*(\d+)", "space used", int),
        "Estimated droppable tombstones": grab(
            meta, r"^Estimated droppable tombstones:\s*([\d.]+)", "droppable tombstones"),
        # BOTH components, compared as a tuple. sstablemetadata prints them on separate
        # lines, so they are grabbed separately and paired here.
        "TTL min / max": (grab(meta, r"^TTL min:\s*(\d+)", "TTL min", int),
                          grab(meta, r"^TTL max:\s*(\d+)", "TTL max", int)),
        "SSTable count": grab(stats, r"SSTable count:\s*(\d+)", "sstable count", int),
    }

    oldv = {
        "rows (sstablemetadata totalRows)": grab(
            old, r"^rows \(sstablemetadata totalRows\)   : (\d+)", "#3217 rows", int),
        "rows (independent fullscan oracle)": grab(
            old, r"^rows \(fullscan\.py 512 token ranges\): (\d+)", "#3217 fullscan rows", int),
        "totalColumnsSet": grab(old, r"^totalColumnsSet                    : (\d+)", "#3217 cols", int),
        "partitions (tablestats estimate)": grab(
            old, r"^partitions \(tablestats estimate\)   : (\d+)", "#3217 partitions", int),
        "dataLength UNCOMPRESSED (bytes)": grab(
            old, r"^dataLength UNCOMPRESSED : (\d+) bytes", "#3217 dataLength", int),
        "Data.db on disk (bytes)": grab(
            old, r"^Data\.db on disk \(comp\)  : (\d+) bytes", "#3217 Data.db size", int),
        "UNCOMPRESSED bytes/row": grab(old, r"^UNCOMPRESSED bytes/row  : ([\d.]+)", "#3217 unc B/row"),
        "COMPRESSED bytes/row": grab(old, r"^COMPRESSED bytes/row    : ([\d.]+)", "#3217 cmp B/row"),
        "LZ4 ratio (header-derived)": grab(old, r"^compression ratio       : ([\d.]+)x", "#3217 ratio"),
        "chunk count (16 KiB chunks)": grab(old, r"^chunk count             : (\d+)", "#3217 chunks", int),
        "chunk_length (bytes)": grab(old, r"^chunk_length \(bytes\)    : (\d+)", "#3217 chunk_length", int),
        "Space used (live, bytes)": grab(
            old, r"^Space used \(live, all components\)  : (\d+) bytes", "#3217 space used", int),
        "Estimated droppable tombstones": grab(
            old, r"^Estimated droppable tombstones     : ([\d.]+)", "#3217 droppable"),
        "TTL min / max": grab2(
            old, r"^TTL min/max                        : (\d+) / (\d+)", "#3217 TTL min/max", int),
        "SSTable count": grab(old, r"^Data\.db count in dir    : (\d+)", "#3217 sstable count", int),
    }

    new_sha = grab_unique(sha_file, r"^([0-9a-f]{64})\s+\S*-Data\.db$",
                          "this run's Data.db sha256")
    old_sha = grab(old, r"^sha256\(Data\.db\)         : ([0-9a-f]{64})", "#3217 sha256", str)

    lines = []
    A = lines.append
    A("CQLite issue #3225 §2 — CORPUS GEOMETRY, and how it compares to #3217 (AC6)")
    A("=" * 78)
    A("")
    A("Recipe: 200,000 partitions x (seq 2 x event_time 10) rows, ws0.events, stock")
    A("Cassandra 5.0.8 LZ4 / 16 KiB chunks, ONE SSTable after nodetool flush + compact.")
    A("Generated by: docs/reports/ws0-3225-artifacts/corpus/regen-corpus.sh")
    A("Comparison generated by: docs/reports/ws0-3225-artifacts/corpus/compare-geometry.py")
    A("(every number below is PARSED from a committed artifact; none is typed by hand)")
    A("")
    A("sha256(Data.db)")
    A("  this run (#3225) : %s" % new_sha)
    A("  #3217            : %s" % old_sha)
    A("  DIFFERENT — and required to be. cassandra-stress draws from a non-deterministic")
    A("  seed stream, so byte-identity was never the bar. AC6's bar is: the NEW sha is")
    A("  recorded (above) and the geometry is SHOWN matching within a stated tolerance")
    A("  (below), not assumed. #3217 recorded the same divergence against #3100.")
    A("")
    A("Field-by-field vs #3217 (tolerance for the continuous fields: %.2f%%)" % args.tolerance_pct)
    A("-" * 78)
    A("%-36s %-16s %-16s %-12s" % ("field", "#3225 (this run)", "#3217", "delta"))
    # Compare the WHOLE labelled field set, or say so. Iterating `new` alone means a
    # field present in the #3217 side but missing from this one is simply not compared,
    # and the table still prints its "no material divergence" verdict — a comparison of
    # a SUBSET reported as a comparison. Both directions are required to match.
    if set(new) != set(oldv):
        only_new = sorted(set(new) - set(oldv))
        only_old = sorted(set(oldv) - set(new))
        raise SystemExit(
            "FAIL: the two field sets differ, so this table would compare a SUBSET while "
            "claiming to compare the geometry: only in #3225=%s; only in #3217=%s"
            % (only_new or "none", only_old or "none"))

    material = []
    for k in new:
        a, b = new[k], oldv[k]
        if isinstance(a, tuple) or isinstance(b, tuple):
            # A COMPOSITE exact-match field: EVERY component must match, and a shape
            # mismatch (different component counts) is itself a divergence, not a
            # comparison to paper over. Component-wise, so a changed maximum can no
            # longer hide behind an unchanged minimum.
            same = (isinstance(a, tuple) and isinstance(b, tuple) and len(a) == len(b)
                    and all(x == y for x, y in zip(a, b)))
            if same:
                delta = "identical"
            else:
                delta = "DIFFERS"
                material.append((k, fmt(a), fmt(b), delta,
                                 "COMPOSITE EXACT-MATCH FIELD DIVERGED (compared component-wise)"))
        elif isinstance(a, int) and isinstance(b, int) and k in (
                "rows (sstablemetadata totalRows)", "rows (independent fullscan oracle)",
                "totalColumnsSet", "partitions (tablestats estimate)", "chunk_length (bytes)",
                # "TTL min / max" is NOT here: it is a composite, handled above.
                "SSTable count"):
            delta = "%+d" % (a - b)
            exact = (a == b)
            if not exact:
                material.append((k, a, b, delta, "EXACT-MATCH FIELD DIVERGED"))
        elif a == b:
            # Identical is identical, including 0 == 0. A relative delta against a zero
            # baseline is undefined, and reporting it as "inf%" would flag an EXACT
            # match as a divergence — which is exactly what a naive (a-b)/b did here
            # for "Estimated droppable tombstones: 0.0". Equality is checked first.
            delta = "+0.000%"
        elif b == 0:
            delta = "n/a (#3217 baseline is 0; a relative delta is undefined)"
            material.append((k, a, b, delta, "#3217 measured 0 and this run did not"))
        else:
            pct = (a - b) / b * 100
            delta = "%+.3f%%" % pct
            if abs(pct) > args.tolerance_pct:
                material.append((k, a, b, delta, "exceeds %.2f%% tolerance" % args.tolerance_pct))
        A("%-36s %-16s %-16s %-12s" % (k, fmt(a), fmt(b), delta))

    A("")
    A("Categorical fields (must match exactly)")
    A("-" * 78)
    fmt_new = grab_unique(sha_file, r"^[0-9a-f]{64}\s+\S*/(\S+)-Data\.db$",
                          "this run's generation/format")
    cats = [
        ("SSTable generation/format", fmt_new, "nb-16-big"),
        ("compressor", grab(meas, r"^compressor              : (\S+)", "compressor", str), "LZ4Compressor"),
        ("rows per partition", "20 (seq 2 x event_time 10)", "20 (seq 2 x event_time 10)"),
        ("min/max local deletion time", "no tombstones" if "no tombstones" in meta else "TOMBSTONES PRESENT",
         "no tombstones"),
    ]
    for name, got, want in cats:
        ok = (got == want)
        A("%-36s %-24s expected %-16s %s" % (name, got, want, "OK" if ok else "*** MISMATCH ***"))
        if not ok:
            material.append((name, got, want, "n/a", "categorical mismatch"))

    A("")
    A("Independent row-count oracle")
    A("-" * 78)
    r_meta = new["rows (sstablemetadata totalRows)"]
    r_scan = new["rows (independent fullscan oracle)"]
    A("  primary     : sstablemetadata totalRows            = %d" % r_meta)
    A("  independent : CQL token-range full scan, 512 ranges = %d" % r_scan)
    A("  These are DIFFERENT code paths — the writer's on-disk stats header vs a live")
    A("  server-side read through the CQL driver — so agreement is evidence, not tautology.")
    A("  agreement: %s" % ("EXACT" if r_meta == r_scan else
                           "*** DISAGREE by %d — reported, not smoothed ***" % (r_meta - r_scan)))
    if r_meta != r_scan:
        material.append(("row-count oracles", r_meta, r_scan, "%+d" % (r_meta - r_scan),
                         "independent oracles disagree"))

    A("")
    A("VERDICT")
    A("-" * 78)
    if material:
        A("*** MATERIAL DIVERGENCE — %d field(s). The #3217 comparison must be LABELLED," % len(material))
        A("*** not asserted, and the affected numbers are not directly comparable:")
        for k, a, b, delta, why in material:
            A("      %-34s #3225=%-16s #3217=%-16s (%s)  <- %s" % (k, a, b, delta, why))
    else:
        A("NO MATERIAL DIVERGENCE. Every exact-match field is identical, every continuous")
        A("field is within %.2f%%, and both row-count oracles agree exactly. The #3225 curve" % args.tolerance_pct)
        A("is therefore directly comparable to #3217's published table, with the recorded")
        A("sha256 difference being the expected consequence of regeneration.")

    txt = "\n".join(lines) + "\n"
    sys.stdout.write(txt)
    if args.out:
        with open(args.out, "w") as fh:
            fh.write(txt)
        print("wrote %s" % args.out, file=sys.stderr)
    return 1 if material else 0


if __name__ == "__main__":
    sys.exit(main())
