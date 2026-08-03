#!/usr/bin/env python3
"""AC3 roll-up: unsymbolized fraction + on-CPU cost centres per profile.

Reports the unsym fraction TWICE: over the whole `-C <server cpus>` capture (the
figure AC3's 10% gate is computed on) and restricted to the SERVER's own threads.
The whole-capture figure is diluted by `swapper` idle samples, which symbolize
trivially, so quoting only that number would flatter a real symbolization
problem. Both are reported; neither is hidden.

Usage: summarize-oncpu.py <profiles-root> <label>... [--out-json f] [--out-table f]
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import sys
from collections import Counter

HEX = re.compile(r"^0x[0-9a-fA-F]+$")
SERVER_COMMS = ("tokio-rt-worker", "cqlite-query-ro", "cqlite-flight",
                "cqlite-blocking", "HeapHelper")
BRACKETED = re.compile(r"^\[[^\]]+\]$")


def unknown(f: str) -> bool:
    f = f.strip()
    return (not f) or f.startswith("[unknown") or bool(HEX.match(f.split(" ")[0]))


def dso_only(f: str) -> bool:
    """A frame that resolved to a SHARED OBJECT but to no function symbol.

    `perf script` prints these as a bare `[libc.so.6]`: the address was mapped to a
    DSO, but that DSO ships no symbol covering it. These frames are NOT unsymbolized
    by AC3's gate metric (`unknown()` is false for them — they carry a DSO name), so
    a profile can pass the <10% unsymbolized gate while a large minority of its frame
    instances are still opaque at FUNCTION granularity. Quantifying that separately is
    the point of this function: the gate figure and the opacity figure are different
    questions and must not be conflated.

    Excludes `[unknown]` (already counted by the gate) and pseudo-DSOs like `[[vdso]]`
    / `[[anon:...]]`, which are kernel/JIT mappings rather than a missing symbol table.
    """
    f = f.strip()
    return bool(BRACKETED.match(f)) and not f.startswith("[unknown") and not f.startswith("[[")


def analyse(folded: str):
    by_comm: Counter = Counter()
    leaf: Counter = Counter()
    crate: Counter = Counter()
    tf = tu = 0
    sf = su = 0
    td = tl = sd = sl = 0          # DSO-only frame instances (any DSO / libc), all vs server
    opener = gzip.open if folded.endswith(".gz") else open
    for line in opener(folded, "rt"):
        line = line.rstrip("\n")
        if not line.strip():
            continue
        try:
            stack, v = line.rsplit(" ", 1)
            w = int(float(v))
        except ValueError:
            continue
        fr = stack.split(";")
        nunk = sum(1 for f in fr if unknown(f))
        ndso = sum(1 for f in fr if dso_only(f))
        nlibc = sum(1 for f in fr if f.strip() == "[libc.so.6]")
        tf += w * len(fr)
        tu += w * nunk
        td += w * ndso
        tl += w * nlibc
        by_comm[fr[0]] += w
        if fr[0] in SERVER_COMMS:
            sf += w * len(fr)
            su += w * nunk
            sd += w * ndso
            sl += w * nlibc
            leaf[fr[-1]] += w
            for f in reversed(fr):
                for c in ("cqlite_core", "cqlite_flight", "arrow", "lz4", "tonic",
                          "hyper", "h2", "tokio", "alloc", "std", "prost"):
                    if f.startswith(c) or ("::" + c + "::") in f or f.startswith("<" + c):
                        crate[c] += w
                        break
                else:
                    continue
                break
    tot = sum(by_comm.values())
    srv = sum(v for k, v in by_comm.items() if k in SERVER_COMMS)
    return {
        "total_weight": tot, "server_weight": srv,
        "server_share_of_capture": (srv / tot) if tot else 0.0,
        "frame_weighted_unsym_all": (tu / tf) if tf else 0.0,
        "frame_weighted_unsym_server_threads_only": (su / sf) if sf else 0.0,
        # DSO-only opacity — the figure the AC3 gate does NOT cover. Weighted frame
        # instances: sum(w * matching frames) / sum(w * frames). The SERVER-only basis
        # is the one to quote, because the claim is about the SERVER's opacity; the
        # all-frames basis is diluted by `swapper`/loadgen/bash stacks that carry no
        # libc frames at all. Both emitted so the denominator is never silently chosen.
        "frame_weighted_dso_only_any_all": (td / tf) if tf else 0.0,
        "frame_weighted_dso_only_any_server_threads_only": (sd / sf) if sf else 0.0,
        "frame_weighted_dso_only_libc_all": (tl / tf) if tf else 0.0,
        "frame_weighted_dso_only_libc_server_threads_only": (sl / sf) if sf else 0.0,
        "dso_only_definition": (
            "A frame printed as a bare '[<dso>]' — the address mapped to a shared object "
            "but to no function symbol. NOT counted by AC3's unsymbolized gate (it carries "
            "a DSO name), so a profile can PASS <10% unsymbolized while this fraction of its "
            "frame instances is opaque at function granularity. Excludes '[unknown]' (the "
            "gate's own metric) and pseudo-DSOs '[[vdso]]'/'[[anon:*]]'. Weighted by sample "
            "weight x frame instances, not by leaf."),
        "by_comm": [{"comm": k, "pct": 100.0 * v / tot} for k, v in by_comm.most_common(6)],
        "top_leaf_functions": [{"fn": k, "pct_of_server": 100.0 * v / srv}
                               for k, v in leaf.most_common(12)] if srv else [],
        "cost_centre_by_crate": [{"crate": k, "pct_of_server": 100.0 * v / srv}
                                 for k, v in crate.most_common(10)] if srv else [],
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("root")
    ap.add_argument("labels", nargs="+")
    ap.add_argument("--out-json")
    ap.add_argument("--out-table")
    a = ap.parse_args()
    out = {"schema": "ws0-3217.oncpu-summary/v2", "profiles": []}
    L = ["==== WS0 #3217 AC3 ON-CPU SUMMARY ====", ""]
    L.append("%-16s %11s %13s %15s %11s %14s" % (
        "profile", "unsym all", "unsym server", "gate(<10%)", "server%", "DSO-only srv%"))
    for lab in a.labels:
        # Prefer the raw capture root; fall back to the COMMITTED gzipped folded file, so
        # this roll-up is reproducible from the repo alone after the box is gone (AC8).
        cands = [os.path.join(a.root, lab, "oncpu.folded"),
                 os.path.join(a.root, lab, "oncpu.folded.gz"),
                 os.path.join(a.root, lab + ".folded.gz"),
                 os.path.join(a.root, lab + ".folded")]
        folded = next((c for c in cands if os.path.exists(c)), None)
        if folded is None:
            L.append("%-16s  MISSING (tried: %s)" % (lab, ", ".join(cands)))
            continue
        d = analyse(folded)
        d["label"] = lab
        gcands = [os.path.join(a.root, lab, "unsym-check.json"),
                  os.path.join(a.root, lab + ".unsym-check.json")]
        gj = next((c for c in gcands if os.path.exists(c)), None)
        d["harness_gate"] = json.load(open(gj)) if gj else None
        out["profiles"].append(d)
        L.append("%-16s %10.4f%% %12.4f%% %15s %10.1f%% %13.2f%%" % (
            lab, 100 * d["frame_weighted_unsym_all"],
            100 * d["frame_weighted_unsym_server_threads_only"],
            "PASS" if d["frame_weighted_unsym_all"] < 0.10 else "FAIL",
            100 * d["server_share_of_capture"],
            100 * d["frame_weighted_dso_only_libc_server_threads_only"]))
    L.append("")
    L.append("DSO-only srv%: [libc.so.6] frames as a fraction of SERVER-thread frame instances.")
    L.append("These are NOT counted by the <10% unsymbolized gate (they carry a DSO name), so a")
    L.append("PASS above does NOT mean 'fully symbolized'. All-frames basis, and any-DSO vs libc,")
    L.append("are both in the JSON; the server-only [libc.so.6] column is the quotable figure.")
    if out["profiles"]:
        _l = [100 * d["frame_weighted_dso_only_libc_server_threads_only"] for d in out["profiles"]]
        _a = [100 * d["frame_weighted_dso_only_libc_all"] for d in out["profiles"]]
        L.append("BAND: server-threads-only [libc.so.6] = %.2f%%-%.2f%%   (all-frames basis %.2f%%-%.2f%%)"
                 % (min(_l), max(_l), min(_a), max(_a)))
        out["dso_only_libc_band_server_threads_only_pct"] = [min(_l), max(_l)]
        out["dso_only_libc_band_all_frames_pct"] = [min(_a), max(_a)]
    L.append("")
    for d in out["profiles"]:
        L.append("--- %s : on-CPU cost centres (%% of SERVER-thread samples) ---" % d["label"])
        L.append("    by crate: " + ", ".join(
            "%s %.1f%%" % (c["crate"], c["pct_of_server"]) for c in d["cost_centre_by_crate"]))
        L.append("    top leaves: " + ", ".join(
            "%s %.1f%%" % (f["fn"][:44], f["pct_of_server"]) for f in d["top_leaf_functions"][:6]))
    t = "\n".join(L) + "\n"
    if a.out_json:
        open(a.out_json, "w").write(json.dumps(out, indent=1) + "\n")
    if a.out_table:
        open(a.out_table, "w").write(t)
    sys.stdout.write(t)
    return 0


if __name__ == "__main__":
    sys.exit(main())
