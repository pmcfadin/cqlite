#!/usr/bin/env python3
"""Parse a runqlat-bpfcc log2 histogram into JSON (issue #3217, AC5).

runqlat prints buckets like:

     usecs               : count     distribution
         0 -> 1          : 12       |*        |
         2 -> 3          : 340      |****     |

Percentiles from a log2 histogram are ESTIMATES bounded by the bucket, so each
is reported as the bucket interval it falls in, not as a false-precision scalar.
The mean is likewise a bucket-midpoint approximation and is labelled as such.

Usage: parse-runqlat.py <runqlat.txt> [--out json] [--label L]
"""
from __future__ import annotations

import argparse
import json
import re
import sys

ROW = re.compile(r"^\s*(\d+)\s*->\s*(\d+)\s*:\s*(\d+)\s")
UNIT = re.compile(r"^\s*(usecs|msecs|nsecs)\s*:")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("runqlat")
    ap.add_argument("--out")
    ap.add_argument("--label", default="")
    a = ap.parse_args()

    unit = "usecs"
    buckets = []
    for line in open(a.runqlat):
        m = UNIT.match(line)
        if m:
            unit = m.group(1)
            continue
        m = ROW.match(line)
        if m:
            lo, hi, cnt = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if cnt:
                buckets.append((lo, hi, cnt))

    total = sum(c for _, _, c in buckets)
    doc = {
        "schema": "ws0-3217.runqlat/v1",
        "label": a.label,
        "source_file": a.runqlat,
        "unit": unit,
        "total_wakeup_events": total,
        "buckets": [{"lo": lo, "hi": hi, "count": c} for lo, hi, c in buckets],
    }
    if total:
        mean = sum(((lo + hi) / 2.0) * c for lo, hi, c in buckets) / total
        doc["mean_approx"] = mean
        doc["mean_approx_note"] = "bucket-midpoint approximation over a log2 histogram"
        cum = 0
        pct = {}
        for p in (50, 90, 99):
            need = total * p / 100.0
            for lo, hi, c in buckets:
                cum += c
                if cum >= need:
                    pct["p%d_bucket_%s" % (p, unit)] = [lo, hi]
                    break
            cum = 0
        doc.update(pct)
        doc["percentile_note"] = (
            "log2 buckets bound each percentile to an interval; reported as [lo,hi] "
            "rather than a false-precision scalar")
    else:
        doc["mean_approx"] = None
        doc["mean_approx_note"] = "no wakeup events recorded in the window"

    text = json.dumps(doc, indent=1) + "\n"
    if a.out:
        open(a.out, "w").write(text)
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
