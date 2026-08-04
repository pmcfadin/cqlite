#!/usr/bin/env python3
"""Derive the per-row microarchitectural counters (`llc.json`) from COMMITTED inputs.

Issue #3217 Part B, P2. The "instructions/row flat +0.1%, cycles/row +34.1%, IPC
-25.4%" headline is the report's second-largest claim, and until this file existed it
was produced by an UNCOMMITTED ad-hoc script: only `llc-run.sh` (the capture) and the
raw `*.perf-stat.csv` were in the repo, so the derivation step was unreproducible —
a straight AC8 violation. This script closes that: every input it reads is committed
under `partB-results/counters/`, so the headline re-derives from the repo alone.

Inputs, all committed:
  counters/llc-<label>.perf-stat.csv   `perf stat -x,` output over the capture window
  counters/llc-<label>.step.jsonl      the loadgen step record (supplies rows_per_s)
  counters/llc-capture-config.json     per-label window_secs + server hw-thread count,
                                       as INVOKED (llc-run.sh's 4th arg / core table)

THE WINDOW IS DATA, NOT A LITERAL. Per-row normalisation divides a counter by
`rows_per_s * window_secs`, so the window is load-bearing. It is read from the capture
config (what `perf stat ... -- sleep <WINDOW>` was actually told to measure), never
hardcoded in the analysis. As a cross-check this script ALSO derives the window from
the CSV itself — `task-clock` nanoseconds / server hw threads — and emits both plus
their relative difference, so a mismatch is visible instead of silent.

Observed here: nominal 20 s vs derived 20.0034-20.0108 s, i.e. <=0.06%. That shifts
per-row figures by <=0.06% and shifts IPC by EXACTLY ZERO (IPC is a ratio of two raw
counters and has no window in it), so the published deltas are insensitive to the
choice. The nominal window is used for the published numbers; the derived window and
the sensitivity are emitted alongside so the reader can check that claim rather than
take it.

`<not supported>` counters (LLC-loads / LLC-load-misses on this virtualized host) are
recorded as null with the reason, NEVER as 0 — a counter that could not be programmed
is not a measurement of zero.

Usage:
  parse-llc-counters.py <counters-dir> [--out-json f] [--out-table f]
"""
from __future__ import annotations

import argparse
import json
import os
import sys

COUNTERS = ["cycles", "instructions", "cache-references", "cache-misses",
            "LLC-loads", "LLC-load-misses", "L1-dcache-loads", "L1-dcache-load-misses",
            "dTLB-load-misses", "branch-misses", "task-clock"]


def parse_perf_csv(path: str):
    """`perf stat -x,` rows: value,unit,event,run_time_ns,pct_enabled,...

    Returns (values, run_time_ns). An unsupported/unprogrammable counter yields None,
    which callers must propagate as null — not silently coerce to 0.0.
    """
    vals: dict[str, float | None] = {}
    run_ns: dict[str, float] = {}
    for line in open(path):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        f = line.split(",")
        if len(f) < 4:
            continue
        raw, ev = f[0].strip(), f[2].strip()
        if ev not in COUNTERS:
            continue
        if raw.startswith("<"):          # <not supported> / <not counted>
            vals[ev] = None
            continue
        try:
            vals[ev] = float(raw)
            run_ns[ev] = float(f[3])
        except ValueError:
            vals[ev] = None
    return vals, run_ns


def rows_per_s_from_step(path: str) -> float:
    """Last step record's aggregate rows/s — the same field Part A's curve uses."""
    last = None
    for line in open(path):
        line = line.strip()
        if line:
            last = json.loads(line)
    if last is None or "rows_per_s" not in last:
        raise SystemExit("no rows_per_s in %s" % path)
    return float(last["rows_per_s"])


def analyse(label: str, cdir: str, cfg: dict):
    csv = os.path.join(cdir, "llc-%s.perf-stat.csv" % label)
    step = os.path.join(cdir, "llc-%s.step.jsonl" % label)
    v, run_ns = parse_perf_csv(csv)
    rps = rows_per_s_from_step(step)
    window = float(cfg["window_secs"])
    hw = int(cfg["server_hw_threads"])
    # Cross-check only — NOT used for the published normalisation.
    derived = (v["task-clock"] / hw / 1e9) if v.get("task-clock") else None
    # An event absent from the CSV entirely is treated exactly like <not supported>:
    # null with a reason. Never a silent 0 and never a KeyError that hides the gap.
    for _ev in COUNTERS:
        v.setdefault(_ev, None)

    rows = rps * window

    def per_row(ev):
        return (v[ev] / rows) if v.get(ev) is not None else None

    ipc = ((v["instructions"] / v["cycles"])
           if (v.get("cycles") and v.get("instructions") is not None) else None)
    l1 = v.get("L1-dcache-load-misses")
    l1l = v.get("L1-dcache-loads")
    llcm = v.get("LLC-load-misses")
    return {
        "instr_per_row": per_row("instructions"),
        "cycles_per_row": per_row("cycles"),
        "ipc": ipc,
        "llc_miss_per_row": (llcm / rows) if llcm is not None else None,
        "llc_miss_pct": None,
        "l1d_miss_pct": (100.0 * l1 / l1l) if (l1 is not None and l1l) else None,
        "dtlb_miss_per_row": per_row("dTLB-load-misses"),
        "rows_per_s": rps,
        "window_secs_nominal": window,
        "window_secs_derived_from_task_clock": derived,
        "window_rel_diff_pct": (100.0 * (derived / window - 1)) if derived else None,
        "unsupported_counters": sorted(k for k in COUNTERS if v.get(k) is None),
        "unsupported_note": ("recorded as null, never 0 — a counter the host could not "
                             "program is not a measured zero"),
        "rows_in_window": rows,
        "inputs": {"perf_csv": os.path.relpath(csv, cdir),
                   "step_record": os.path.relpath(step, cdir)},
        "raw": {k: v.get(k) for k in COUNTERS},
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("counters_dir")
    ap.add_argument("--out-json")
    ap.add_argument("--out-table")
    a = ap.parse_args()
    cfgp = os.path.join(a.counters_dir, "llc-capture-config.json")
    cfg = json.load(open(cfgp))
    out = {}
    for label in cfg["captures"]:
        out["llc-%s" % label] = analyse(label, a.counters_dir, cfg["captures"][label])

    L = ["==== WS0 #3217 microarch counters (derived from COMMITTED inputs only) ====", ""]
    L.append("%-14s %12s %12s %8s %14s %14s %12s" % (
        "capture", "instr/row", "cyc/row", "IPC", "L1d-miss/row", "dTLB-miss/row", "rows/s"))
    for k, d in out.items():
        L.append("%-14s %12.0f %12.0f %8.2f %14.1f %14.1f %12.0f" % (
            k, d["instr_per_row"], d["cycles_per_row"], d["ipc"],
            (d["raw"]["L1-dcache-load-misses"] or 0) / d["rows_in_window"],
            (d["raw"]["dTLB-load-misses"] or 0) / d["rows_in_window"], d["rows_per_s"]))
    L.append("")
    L.append("window: nominal (as invoked) vs derived (task-clock / hw threads) — the")
    L.append("published per-row figures use the NOMINAL window; the derived one is a check.")
    for k, d in out.items():
        L.append("  %-14s nominal %.3f s   derived %.4f s   diff %+.3f%%" % (
            k, d["window_secs_nominal"], d["window_secs_derived_from_task_clock"],
            d["window_rel_diff_pct"]))
    L.append("  IPC is a pure counter ratio and is INVARIANT to the window choice.")
    # The endpoint comparison only exists when both endpoint captures are present
    # (they are in the real run; a smoke fixture may hold a single synthetic capture).
    a1 = out.get("llc-s1-N2")
    b1 = out.get("llc-s6-N16")
    if a1 and b1:
        L += ["",
              "S=6/N=16 vs S=1/N=2:  instructions/row %+.1f%%   cycles/row %+.1f%%   IPC %+.1f%%"
              % (100 * (b1["instr_per_row"] / a1["instr_per_row"] - 1),
                 100 * (b1["cycles_per_row"] / a1["cycles_per_row"] - 1),
                 100 * (b1["ipc"] / a1["ipc"] - 1)),
              "unsupported on this host: %s (null, not zero)"
              % ", ".join(a1["unsupported_counters"]),
              ""]
    t = "\n".join(L) + "\n"
    if a.out_json:
        open(a.out_json, "w").write(json.dumps(out, indent=1) + "\n")
    if a.out_table:
        open(a.out_table, "w").write(t)
    sys.stdout.write(t)
    return 0


if __name__ == "__main__":
    sys.exit(main())
