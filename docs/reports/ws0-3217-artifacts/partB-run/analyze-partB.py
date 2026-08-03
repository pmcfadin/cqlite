#!/usr/bin/env python3
"""Consolidate every Part B capture into one analysis record + tables (#3217).

Mirrors Part A's `analyze-partA.py` output shape: a JSON record plus a plain-text
table set, both committed as artefacts (AC8). Reads ONLY measured files; a value
that was not measured is `null` with a stated reason, never a filled-in guess.
"""
from __future__ import annotations

import json
import os
import statistics
import sys

# P8: nothing here may hardcode one agent's worktree. ARTE defaults to the artefacts
# directory this script lives in (../ from partB-run), so the analysis runs from a
# fresh clone; PROF/ANA/RES stay overridable for a live measurement box.
ARTE = os.environ.get("WS0_ARTEFACTS",
                      os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
PROF = os.environ.get("WS0_PROFILES", "/data/ws0/profiles")
ANA = os.environ.get("WS0_ANALYSIS", "/data/ws0/analysis")
RES = os.environ.get("WS0_RESULTS", "/data/ws0/results")
OUT = os.environ.get("WS0_PARTB_OUT", os.path.join(ARTE, "partB-results"))


def arte(*p):
    """A path under the COMMITTED artefacts tree."""
    return os.path.join(ARTE, *p)


def prefer(*cands):
    """First existing path. Committed artefacts win over the box's scratch dirs, so a
    re-run from a clone reproduces the published numbers rather than silently reading
    whatever a stale /data/ws0 happens to hold."""
    return next((c for c in cands if os.path.exists(c)), cands[-1])
BUCKETS = ["egress_credit_acquire", "mpsc_send_park", "mpsc_recv_park",
           "tonic_grpc_socket_write", "disk_io", "tokio_scheduler", "other"]
PARK_SITES = ["do_get_mpsc_handoff", "egress_credit", "core_raw_chunk_chan",
              "core_query_rows_chan", "core_windowed_batch_chan",
              "glibc_malloc_arena_lock", "grpc_egress", "tokio_runtime_idle", "other"]


def jload(p):
    return json.load(open(p)) if os.path.exists(p) else None


def main() -> int:
    out = {"schema": "ws0-3217.partB-analysis/v1"}
    L = []

    # ---------------- AC3 ----------------
    onc = jload(prefer(arte("partB-results/oncpu/AC3-oncpu-summary.json"),
                       "%s/AC3-oncpu-summary.json" % ANA))
    out["ac3_oncpu"] = onc
    L += ["=== AC3: on-CPU flame graphs, unsymbolized-frame gate (<10%) ===",
          "%-14s %12s %14s %8s %10s" % ("profile", "unsym all", "unsym server", "gate", "server%")]
    for p in (onc or {}).get("profiles", []):
        L.append("%-14s %11.4f%% %13.4f%% %8s %9.1f%%" % (
            p["label"], 100 * p["frame_weighted_unsym_all"],
            100 * p["frame_weighted_unsym_server_threads_only"],
            "PASS" if p["frame_weighted_unsym_all"] < 0.10 else "FAIL",
            100 * p["server_share_of_capture"]))
    L.append("")

    # ---------------- AC4 ----------------
    out["ac4_offcpu"] = {}
    L += ["=== AC4: off-CPU blocked-time attribution (seconds; EXPLICIT 0 = measured absent) ===",
          "collector: patched offcputime-bpfcc (counts map 1e6; the stock 10240-key map",
          "SATURATED at N>=8 and silently dropped stacks). classifier: v2, leaf-first.",
          "%-14s %10s " % ("capture", "total") + " ".join("%-13s" % b[:13] for b in BUCKETS)]
    for s in ("s1", "s6"):
        for n in (1, 8, 16):
            d = jload(prefer(arte("partB-results/offcpu/offcpu2-%s-N%d.attribution-v2.json" % (s, n)),
                             "%s/offcpu2-%s-N%d.attribution-v2.json" % (ANA, s, n)))
            if not d:
                continue
            key = "offcpu2-%s-N%d" % (s, n)
            bm = {b["bucket"]: b["blocked_time_us"] for b in d["buckets"]}
            out["ac4_offcpu"][key] = {
                "total_blocked_s": d["total_blocked_time_us"] / 1e6,
                "unique_stacks": d["unique_stacks"],
                "buckets_s": {b: bm.get(b, 0) / 1e6 for b in BUCKETS},
                "channel_identity_s": {c["channel"]: c["blocked_time_us"] / 1e6
                                       for c in d["channel_identity"]["channels"]},
                "tokio_breakdown_s": {t["cause"]: t["blocked_time_us"] / 1e6
                                      for t in d["tokio_scheduler_breakdown"]},
            }
            L.append("%-14s %10.2f " % (key, d["total_blocked_time_us"] / 1e6)
                     + " ".join("%-13.4f" % (bm.get(b, 0) / 1e6) for b in BUCKETS))
    L += ["",
          "AC4 channel identity - WHICH bounded channel the send/recv parks belong to.",
          "The bypass read path stacks FOUR channels between SSTable and wire; only",
          "`do_get_batch` is the #3217 handoff.",
          "%-14s %14s %16s %16s %18s %14s" % ("capture", "do_get_batch", "core_raw_chunk",
                                              "core_query_rows", "core_windowed_batch",
                                              "unattributed")]
    for k, v in out["ac4_offcpu"].items():
        c = v["channel_identity_s"]
        L.append("%-14s %14.4f %16.4f %16.4f %18.4f %14.4f" % (
            k, c.get("do_get_batch", 0), c.get("core_raw_chunk", 0),
            c.get("core_query_rows", 0), c.get("core_windowed_batch", 0),
            c.get("unattributed_channel", 0)))
    L.append("")

    # ---------------- park counts ----------------
    out["park_counts"] = {}
    L += ["=== Park COUNTS by site (perf sched:sched_switch; EVENTS, not microseconds) ===",
          "offcputime charges duration, so a frequent-but-short park is invisible to it.",
          "This is the instrument for the 'parks per Flight batch' question.",
          "%-14s %11s %12s %13s" % ("capture", "vol/s", "invol/s", "parks/batch")]
    for lab in ("sched2-s1-N1", "sched2-s6-N1", "sched2-s6-N16"):
        d = jload(prefer(arte("partB-results/park-counts/%s.park-sites.json" % lab),
                         "%s/%s.json" % (ANA, lab)))
        if not d:
            continue
        sm = {s["site"]: s for s in d["sites"]}
        out["park_counts"][lab] = {
            "rows_per_s": d["rows_per_s"], "window_secs": d["window_secs"],
            "voluntary_per_s": d["voluntary_per_s"],
            "involuntary_per_s": d["involuntary_per_s"],
            "voluntary_parks_per_flight_batch": d["voluntary_parks_per_flight_batch"],
            "voluntary_parks_per_1k_rows": d["voluntary_parks_per_1k_rows"],
            "sites_parks_per_batch": {
                s: (sm[s]["parks_per_flight_batch"] if s in sm else 0.0) for s in PARK_SITES},
            "sites_pct_of_voluntary": {
                s: (sm[s]["pct_of_voluntary"] if s in sm else 0.0) for s in PARK_SITES},
        }
        L.append("%-14s %11.0f %12.0f %13.0f" % (
            lab, d["voluntary_per_s"], d["involuntary_per_s"],
            d["voluntary_parks_per_flight_batch"]))
    L += ["", "parks per 8192-row Flight batch, by site (EXPLICIT 0 = measured absent):",
          "%-26s" % "site" + " ".join("%14s" % k for k in out["park_counts"])]
    for s in PARK_SITES:
        L.append("%-26s" % s + " ".join(
            "%14.0f" % out["park_counts"][k]["sites_parks_per_batch"][s]
            for k in out["park_counts"]))
    L.append("")

    # ---------------- AC5 runqueue latency ----------------
    out["ac5_runqlat"] = {}
    L += ["=== AC5: run-queue latency per N (runqlat-bpfcc, log2 buckets, microseconds) ===",
          "%-10s %4s %14s %14s %16s %16s" % ("arm", "N", "p50", "p90", "p99", "wakeups")]
    for s in ("s1", "s6"):
        for n in (1, 8, 16):
            d = jload(prefer(arte("partB-results/scheduler/offcpu2-%s-N%d.runqlat.json" % (s, n)),
                             "%s/offcpu2-%s/runqlat-N%d.json" % (PROF, s, n)))
            if not d:
                continue
            f = lambda k: ("[%s,%s]" % tuple(d[k])) if d.get(k) else "n/a"
            out["ac5_runqlat"]["%s-N%d" % (s, n)] = {
                "p50_bucket_usecs": d.get("p50_bucket_usecs"),
                "p90_bucket_usecs": d.get("p90_bucket_usecs"),
                "p99_bucket_usecs": d.get("p99_bucket_usecs"),
                "total_wakeup_events": d.get("total_wakeup_events")}
            L.append("%-10s %4d %14s %14s %16s %16s" % (
                s, n, f("p50_bucket_usecs"), f("p90_bucket_usecs"), f("p99_bucket_usecs"),
                d.get("total_wakeup_events", "n/a")))
    L.append("")

    # ---------------- residual accounting ----------------
    # P2: produced by the COMMITTED partB-run/parse-llc-counters.py from the committed
    # counters/*.perf-stat.csv + *.step.jsonl + llc-capture-config.json, so the IPC
    # headline re-derives from the repo alone.
    llc = jload(prefer(arte("partB-results/counters/llc.json"), "%s/llc.json" % ANA))
    out["residual_accounting"] = {"counters": llc}
    if llc:
        a, b = llc["llc-s1-N2"], llc["llc-s6-N16"]
        pred = ((b["ipc"] / a["ipc"]) * (a["instr_per_row"] / b["instr_per_row"]))
        out["residual_accounting"]["marginal_efficiency_predicted_from_counters_excl_util"] = pred
        L += ["=== Residual inefficiency: MORE work, or SLOWER work? ===",
              "%-24s %12s %12s %8s %14s %14s %14s" % (
                  "point", "instr/row", "cyc/row", "IPC", "L1d-miss/row", "dTLB-miss/row",
                  "br-miss/row")]
        for lab, nm in (("llc-s1-N2", "S=1 N=2 (S=1 peak)"),
                        ("llc-s6-N16", "S=6 N=16 (S=6 peak)"),
                        ("llc-s6-N1", "S=6 N=1")):
            d = llc[lab]
            # Window read from the capture record the producer emitted (P2), never a
            # hardcoded 20.0. `rows_in_window` is rows_per_s x the window AS INVOKED;
            # the producer also records the task-clock-derived window and their <=0.06%
            # difference, which does not move IPC at all.
            r = d.get("rows_in_window") or (d["rows_per_s"] * d["window_secs_nominal"])
            raw = d["raw"]
            L.append("%-24s %12.0f %12.0f %8.2f %14.1f %14.1f %14.1f" % (
                nm, d["instr_per_row"], d["cycles_per_row"], d["ipc"],
                raw.get("L1-dcache-load-misses", 0) / r,
                raw.get("dTLB-load-misses", 0) / r,
                raw.get("branch-misses", 0) / r))
        L += ["",
              "S=6/N=16 vs S=1/N=2:  instructions/row %+.1f%%   cycles/row %+.1f%%   IPC %+.1f%%"
              % (100 * (b["instr_per_row"] / a["instr_per_row"] - 1),
                 100 * (b["cycles_per_row"] / a["cycles_per_row"] - 1),
                 100 * (b["ipc"] / a["ipc"] - 1)),
              "LLC-loads / LLC-load-misses / cache-references are <not supported> on this",
              "virtualized host, so the MICROARCHITECTURAL CAUSE of the IPC decay is only",
              "partially measured. Reported, not inferred.", ""]

    # ------------- closure: does the counter model reproduce the curve? -------
    # rows/s per physical core is proportional to  util * IPC / instructions_per_row
    # at a fixed clock and a fixed number of SMT threads per core. Both arms below
    # run 2 SMT threads per physical core, so the comparison is like-for-like.
    if llc:
        a, b = llc["llc-s1-N2"], llc["llc-s6-N16"]
        # P3: READ Part A's numbers, never transcribe them. Hand-copied constants are
        # correct exactly until Part A is re-run, and then they are silently wrong.
        pa = jload(prefer(arte("results/partA-analysis.json"),
                          "%s/partA-analysis.json" % RES))
        if not pa:
            raise SystemExit("closure needs partA-analysis.json (looked under %s and %s)"
                             % (arte("results"), RES))
        peak = {a2["arm"]: a2 for a2 in pa["cross_S_scaling"]["per_arm"]}
        s1, s6 = peak["cn-s1"], peak["cn-s6"]
        util1 = s1["server_util_at_peak"]
        util6 = s6["server_util_at_peak"]
        meas_refB = (s6["best_rows_per_s_median"] / s6["S_physical_cores"]) \
            / s1["best_rows_per_s_median"]
        meas_ac5 = (s6["best_rows_per_s_median"] / s6["S_physical_cores"]) \
            / peak["cn-s1-ac5"]["best_rows_per_s_median"]
        out["residual_accounting"]["partA_inputs"] = {
            "source": "results/partA-analysis.json cross_S_scaling.per_arm (read, not transcribed)",
            "s1_peak_rows_per_s": s1["best_rows_per_s_median"],
            "s1_ac5_peak_rows_per_s": peak["cn-s1-ac5"]["best_rows_per_s_median"],
            "s6_peak_rows_per_s": s6["best_rows_per_s_median"],
            "s1_util_at_peak": util1, "s6_util_at_peak": util6}
        f_ipc = b["ipc"] / a["ipc"]
        f_instr = a["instr_per_row"] / b["instr_per_row"]
        f_util = util6 / util1
        pred = f_ipc * f_instr * f_util
        out["residual_accounting"]["closure"] = {
            "measured_marginal_efficiency_vs_cn_s1_N2": meas_refB,
            "measured_marginal_efficiency_vs_cn_s1_ac5_N2": meas_ac5,
            "factor_ipc": f_ipc, "factor_instructions_per_row": f_instr,
            "factor_utilisation": f_util,
            "predicted_marginal_efficiency": pred,
            "closure_gap_pp": 100 * (pred - meas_refB),
            "note": ("rows/s per physical core ~ util * IPC / instructions_per_row at fixed "
                     "clock and fixed SMT threads per core; both arms run 2 SMT threads per "
                     "physical core."),
        }
        L += ["=== Closure: can the counters reproduce the C(N) marginal efficiency? ===",
              "measured  (S=6 N=16 per core) / (S=1 N=2 per core) = %.4f  [vs cn-s1]   %.4f  [vs cn-s1-ac5]"
              % (meas_refB, meas_ac5),
              "predicted = IPC %.4f  x  instr/row %.4f  x  util %.4f  =  %.4f"
              % (f_ipc, f_instr, f_util, pred),
              "closure gap = %+.2f percentage points" % (100 * (pred - meas_refB)),
              "",
              "So the residual splits as:  IPC decay %.1f pp | residual idle %.1f pp | extra"
              " instructions %.1f pp | unexplained %.1f pp"
              % (100 * (1 - f_ipc), 100 * f_ipc * (1 - f_util),
                 100 * f_ipc * f_util * (1 - f_instr), 100 * (pred - meas_refB)),
              ""]

    os.makedirs(OUT, exist_ok=True)
    open("%s/partB-analysis.json" % OUT, "w").write(json.dumps(out, indent=1) + "\n")
    open("%s/partB-analysis.txt" % OUT, "w").write("\n".join(L) + "\n")
    print("\n".join(L))
    return 0


if __name__ == "__main__":
    sys.exit(main())
