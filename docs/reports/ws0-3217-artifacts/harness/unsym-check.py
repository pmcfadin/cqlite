#!/usr/bin/env python3
"""Compute the unsymbolized-frame fraction of a folded stack file (AC3).

AC3 requires unknown/unsymbolized frames to be < 10% of samples, so this is a
GATE, not a report: exit 1 when the threshold is exceeded and say so loudly.

"Unsymbolized" is counted three ways because the phrase is ambiguous, and the
one that matters (frame-weighted) is the one gated:

  frame_weighted   sum(count * unknown_frames) / sum(count * frames)   <-- GATED
  sample_any       sum(count where the stack has ANY unknown frame) / sum(count)
  leaf_weighted    sum(count where the LEAF frame is unknown) / sum(count)

A frame counts as unsymbolized when it is `[unknown]`, a bare hex address, or an
`[unknown]`-in-a-module form such as `[unknown] (/usr/lib/...)`.

Usage: unsym-check.py <folded.txt> [--threshold 0.10] [--out json] [--label L]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter

HEX = re.compile(r"^0x[0-9a-fA-F]+$")


def is_unknown(frame: str) -> bool:
    f = frame.strip()
    if not f:
        return True
    if f.startswith("[unknown"):
        return True
    if HEX.match(f.split(" ")[0]):
        return True
    if f in ("[unknown]", "??", "[UNKNOWN]"):
        return True
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("folded")
    ap.add_argument("--threshold", type=float, default=0.10)
    ap.add_argument("--out")
    ap.add_argument("--label", default="")
    a = ap.parse_args()

    tot_frames = tot_unk = tot_samples = samples_any = samples_leaf = 0
    worst: Counter = Counter()
    for line in open(a.folded):
        line = line.rstrip("\n")
        if not line.strip():
            continue
        try:
            stack, cnt = line.rsplit(" ", 1)
            count = int(float(cnt))
        except ValueError:
            continue
        frames = stack.split(";")
        unk = [f for f in frames if is_unknown(f)]
        tot_samples += count
        tot_frames += count * len(frames)
        tot_unk += count * len(unk)
        if unk:
            samples_any += count
            for f in unk:
                worst[f.strip()] += count
        if frames and is_unknown(frames[-1]):
            samples_leaf += count

    def frac(n, d):
        return (n / d) if d else 0.0

    doc = {
        "schema": "ws0-3217.unsym-check/v1",
        "label": a.label,
        "folded_file": a.folded,
        "total_samples_or_weight": tot_samples,
        "total_frames_weighted": tot_frames,
        "unsymbolized_frames_weighted": tot_unk,
        "frame_weighted_unsym_fraction": frac(tot_unk, tot_frames),
        "sample_fraction_with_any_unknown_frame": frac(samples_any, tot_samples),
        "leaf_unsym_fraction": frac(samples_leaf, tot_samples),
        "threshold": a.threshold,
        "gated_metric": "frame_weighted_unsym_fraction",
        "top_unknown_frames": worst.most_common(10),
    }
    ok = doc["frame_weighted_unsym_fraction"] < a.threshold
    doc["verdict"] = "PASS" if ok else "FAIL"
    doc["verdict_note"] = (
        "AC3 satisfied: unsymbolized frames below threshold"
        if ok else
        "AC3 VIOLATED: unsymbolized frames >= %.1f%%. Frame-pointer unwinding needs the server "
        "built with -C force-frame-pointers=yes AND CARGO_PROFILE_RELEASE_STRIP=none "
        "CARGO_PROFILE_RELEASE_DEBUG=true; kernel frames additionally need "
        "kernel.perf_event_paranoid=-1 and kernel.kptr_restrict=0 (both SILENTLY REVERT on this box)."
        % (a.threshold * 100))

    text = json.dumps(doc, indent=1) + "\n"
    if a.out:
        open(a.out, "w").write(text)
    sys.stdout.write(text)
    if not ok:
        print("\n!!! AC3 GATE FAIL: frame-weighted unsymbolized fraction %.4f >= %.4f !!!"
              % (doc["frame_weighted_unsym_fraction"], a.threshold), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
