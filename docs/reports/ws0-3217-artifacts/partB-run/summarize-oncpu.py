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
import json
import os
import re
import sys
from collections import Counter

HEX = re.compile(r"^0x[0-9a-fA-F]+$")
SERVER_COMMS = ("tokio-rt-worker", "cqlite-query-ro", "cqlite-flight",
                "cqlite-blocking", "HeapHelper")


def unknown(f: str) -> bool:
    f = f.strip()
    return (not f) or f.startswith("[unknown") or bool(HEX.match(f.split(" ")[0]))


def analyse(folded: str):
    by_comm: Counter = Counter()
    leaf: Counter = Counter()
    crate: Counter = Counter()
    tf = tu = 0
    sf = su = 0
    for line in open(folded):
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
        tf += w * len(fr)
        tu += w * nunk
        by_comm[fr[0]] += w
        if fr[0] in SERVER_COMMS:
            sf += w * len(fr)
            su += w * nunk
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
    out = {"schema": "ws0-3217.oncpu-summary/v1", "profiles": []}
    L = ["==== WS0 #3217 AC3 ON-CPU SUMMARY ====", ""]
    L.append("%-16s %11s %13s %15s %11s" % (
        "profile", "unsym all", "unsym server", "gate(<10%)", "server%"))
    for lab in a.labels:
        folded = os.path.join(a.root, lab, "oncpu.folded")
        if not os.path.exists(folded):
            L.append("%-16s  MISSING (%s)" % (lab, folded))
            continue
        d = analyse(folded)
        d["label"] = lab
        gj = os.path.join(a.root, lab, "unsym-check.json")
        d["harness_gate"] = json.load(open(gj)) if os.path.exists(gj) else None
        out["profiles"].append(d)
        L.append("%-16s %10.4f%% %12.4f%% %15s %10.1f%%" % (
            lab, 100 * d["frame_weighted_unsym_all"],
            100 * d["frame_weighted_unsym_server_threads_only"],
            "PASS" if d["frame_weighted_unsym_all"] < 0.10 else "FAIL",
            100 * d["server_share_of_capture"]))
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
