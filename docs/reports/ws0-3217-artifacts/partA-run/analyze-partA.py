#!/usr/bin/env python3
"""Part A (#3217) cross-S analysis: C(N) curves, AC2 shape check, AC5 ctxt switches, AC6 bases.

Reads every sweep's points.jsonl, plus the per-thread ctxt-switch sidecar, and emits
one consolidated JSON + text table. Saturated points are EXCLUDED from the headline
curve and listed separately (never quietly averaged in).
"""
from __future__ import annotations
import glob, json, os, statistics as st, sys

RESULTS = sys.argv[1] if len(sys.argv) > 1 else "/data/ws0/results"
SIDECAR_V1 = "/data/ws0/logs/ctxt/threads.jsonl"        # aggregate-only; NOT monotone (thread churn)
SIDECAR_V2 = "/data/ws0/logs/ctxt/threads-pertid.jsonl"  # per-TID; the usable one

# #3100's published S=1 pinned-core control (rows/s), for the AC2 shape comparison.
PUB_3100 = {1: 246940.0, 2: 287441.0, 4: 273438.0, 8: 248621.0, 16: 236734.0}


def load_sidecar():
    """Per-TID snapshots, time-ordered. v1's aggregate sum is deliberately unused."""
    rows = []
    try:
        for line in open(SIDECAR_V2):
            line = line.strip()
            if line:
                try: rows.append(json.loads(line))
                except json.JSONDecodeError: pass
    except OSError:
        pass
    rows.sort(key=lambda r: r["ts_unix_ms"])
    return rows


def sidecar_delta(sc, t0_ms, t1_ms):
    """In-window voluntary/nonvoluntary ctxt switches, summed PER TID.

    A live-thread aggregate is not a monotone counter: tokio retires threads, and a
    retiring thread's accumulated count leaves the sum, producing negative deltas
    (v1 measured -16925 vol/s). Summing per-TID (last - first) is churn-safe. It
    slightly UNDER-counts: a thread born and retired entirely between two 2s samples
    contributes nothing, and a thread first seen mid-window loses its pre-observation
    switches. Both push the figure down, never up.
    """
    win = [r for r in sc if t0_ms <= r["ts_unix_ms"] <= t1_ms]
    if len(win) < 2:
        return None
    pids = {r["pid"] for r in win}
    if len(pids) != 1:               # a server restart fell inside the window: counters reset
        return None
    first, last = {}, {}
    for snap in win:
        for tid, (v, n) in snap["tids"].items():
            if tid not in first:
                first[tid] = (v, n)
            last[tid] = (v, n)
    vol = nonvol = 0
    regressed = 0
    for tid in last:
        dv = last[tid][0] - first[tid][0]
        dn = last[tid][1] - first[tid][1]
        if dv < 0 or dn < 0:         # TID reuse; clamp rather than subtract
            regressed += 1
            dv = max(dv, 0); dn = max(dn, 0)
        vol += dv; nonvol += dn
    span = (win[-1]["ts_unix_ms"] - win[0]["ts_unix_ms"]) / 1000.0
    if span <= 0:
        return None
    retired = len(set(first) - set(win[-1]["tids"]))
    return {
        "vol": vol, "nonvol": nonvol,
        "vol_per_s": vol / span, "nonvol_per_s": nonvol / span,
        "threads": len(win[-1]["tids"]), "tids_seen_in_window": len(last),
        "tids_retired_in_window": retired, "tids_with_regressed_counter": regressed,
        "sample_span_s": span,
        "coverage_frac": span / max((t1_ms - t0_ms) / 1000.0, 1e-9),
    }


def main():
    sc = load_sidecar()
    sweeps = {}
    for pj in sorted(glob.glob(os.path.join(RESULTS, "*", "points.jsonl"))):
        label = os.path.basename(os.path.dirname(pj))
        pts = [json.loads(l) for l in open(pj) if l.strip()]
        if pts:
            sweeps[label] = pts

    out = {"schema": "ws0-3217.partA-analysis/v1", "published_3100_s1_rows_per_s": PUB_3100,
           "sweeps": {}}
    lines = []

    for label, pts in sweeps.items():
        S = pts[0].get("server_physical_cores_S")
        path = pts[0].get("merge_path")
        by_n = {}
        for p in pts:
            by_n.setdefault(p["target_concurrency_N"], []).append(p)

        # attach the sidecar AC5 delta to every point
        for p in pts:
            t0 = p["ts_unix_ms"]
            t1 = t0 + int((p.get("duration_s") or 0) * 1000) + 8000
            p["_ac5_thread_ctxt"] = sidecar_delta(sc, t0, t1)

        valid = {n: [p for p in ps if not p["client_saturated"]] for n, ps in by_n.items()}
        excluded = [p for p in pts if p["client_saturated"]]

        base = None
        if valid.get(1):
            base = st.median([p["rows_per_s_aggregate"] for p in valid[1]])

        rowsum = []
        for n in sorted(by_n):
            ps = valid.get(n) or []
            if not ps:
                continue
            r = sorted(p["rows_per_s_aggregate"] for p in ps)
            med = st.median(r)
            unavail = [p["requests_unavailable"] for p in ps]
            ac5 = [p["_ac5_thread_ctxt"] for p in ps if p["_ac5_thread_ctxt"]]
            rec = {
                "N": n, "reps_valid": len(ps), "reps_total": len(by_n[n]),
                "rows_per_s_min": r[0], "rows_per_s_median": med, "rows_per_s_max": r[-1],
                "dispersion_pct_of_median": (r[-1] - r[0]) / med * 100 if med else None,
                "rows_per_s_per_stream_median": med / n,
                "speedup_vs_N1": (med / base) if base else None,
                "marginal_efficiency_vs_N_times_N1": (med / (n * base)) if base else None,
                "requests_unavailable_all": unavail,
                "requests_unavailable_total": sum(x for x in unavail if x is not None),
                "admission_clean": all(x == 0 for x in unavail),
                "requests_error_total": sum(p.get("requests_error") or 0 for p in ps),
                "server_cpu_util_of_pinned_set_median": st.median(
                    [p["server_cpu_utilization_of_pinned_set"] for p in ps]),
                "client_cpu_util_of_pinned_set_median": st.median(
                    [p["client_cpu_utilization_of_pinned_set"] for p in ps]),
                "ctxt_switches_cpu_wide_per_s_median": st.median(
                    [p["context_switches_cpu_wide"] / p["duration_s"] for p in ps]),
                "cpu_migrations_cpu_wide_per_s_median": st.median(
                    [p["cpu_migrations_cpu_wide"] / p["duration_s"] for p in ps]),
                "server_thread_vol_ctxt_per_s_median": st.median(
                    [a["vol_per_s"] for a in ac5]) if ac5 else None,
                "server_thread_nonvol_ctxt_per_s_median": st.median(
                    [a["nonvol_per_s"] for a in ac5]) if ac5 else None,
                "server_threads_observed": max((a["threads"] for a in ac5), default=None),
                "server_tids_seen_in_window_max": max((a["tids_seen_in_window"] for a in ac5), default=None),
                "server_tids_retired_in_window_max": max((a["tids_retired_in_window"] for a in ac5), default=None),
                "ac5_source": ("per-TID delta sum over /proc/<pid>/task/*/status (0.5Hz sidecar); churn-safe, slight under-count"
                               if ac5 else "UNAVAILABLE: no clean sidecar window"),
                "cycles_per_row_median": st.median(
                    [p["cycles_per_row"] for p in ps if p.get("cycles_per_row")]) or None,
                "IPC_median": st.median([p["IPC"] for p in ps if p.get("IPC")]) or None,
                "rows_per_server_cpu_sec_median": st.median(
                    [p["rows_per_server_cpu_sec"] for p in ps if p.get("rows_per_server_cpu_sec")]),
                "read_bytes_total_all_reps": sum(
                    (p["server_io_delta"].get("read_bytes") or 0) for p in ps),
                "bytes_per_s_logical_uncompressed_median": st.median(
                    [p["bytes_per_s_logical_uncompressed"] for p in ps
                     if p.get("bytes_per_s_logical_uncompressed")]),
                "bytes_per_s_ondisk_compressed_median": st.median(
                    [p["bytes_per_s_ondisk_compressed"] for p in ps
                     if p.get("bytes_per_s_ondisk_compressed")]),
                "bytes_per_s_arrow_wire_capacity_median": st.median(
                    [p["bytes_per_s_arrow_wire_capacity"] for p in ps
                     if p.get("bytes_per_s_arrow_wire_capacity")]),
                "latency_p50_ms_median": st.median(
                    [p["latency_ms"]["p50"] for p in ps if p.get("latency_ms")]),
            }
            rowsum.append(rec)

        sw = {"S_physical_cores": S, "merge_path": path,
              "server_cpus": pts[0]["server_cpus"], "client_cpus": pts[0]["client_cpus"],
              "step_seconds_requested": pts[0]["step_seconds_requested"],
              "per_N": rowsum,
              "excluded_client_saturated": [
                  {"N": p["target_concurrency_N"], "rep": p["rep"],
                   "rows_per_s": p["rows_per_s_aggregate"],
                   "client_util": p["client_cpu_utilization_of_pinned_set"]} for p in excluded],
              "byte_basis_note": (
                  "logical/uncompressed = rows/s x 693.29 B/row (CompressionInfo.db dataLength); "
                  "on-disk compressed = rows/s x 196.09 B/row (sum of *-Data.db); "
                  "arrow-wire = flight-loadgen Arrow buffer CAPACITY, NOT gRPC-on-the-wire bytes."),
              "now_pinning": "N/A - the ws0.events fixture carries no TTL and no tombstones, "
                             "so no read-time reconciliation depends on `now`."}
        out["sweeps"][label] = sw

        lines.append("")
        lines.append("=== %s  (S=%s physical cores, cpus=%s, path=%s) ===" % (
            label, S, pts[0]["server_cpus"], path))
        lines.append("  (speedup/margeff below are SELF-normalised to THIS arm's own N=1 - NOT comparable across S; see the cross-S table)")
        lines.append("%-4s %-4s %-12s %-12s %-12s %-7s %-12s %-12s %-12s %-7s %-7s %-6s" % (
            "N", "reps", "rows/s min", "rows/s med", "rows/s max", "spr%",
            "rows/s/strm", "spdup/selfN1", "meff/selfN1", "srvUtl", "cliUtl", "unav"))
        for r in rowsum:
            lines.append("%-4d %-4d %-12.0f %-12.0f %-12.0f %-7.1f %-12.0f %-12.3f %-12.3f %-7.3f %-7.3f %-6d" % (
                r["N"], r["reps_valid"], r["rows_per_s_min"], r["rows_per_s_median"],
                r["rows_per_s_max"], r["dispersion_pct_of_median"],
                r["rows_per_s_per_stream_median"], r["speedup_vs_N1"] or 0,
                r["marginal_efficiency_vs_N_times_N1"] or 0,
                r["server_cpu_util_of_pinned_set_median"],
                r["client_cpu_util_of_pinned_set_median"],
                r["requests_unavailable_total"]))
        if excluded:
            lines.append("  !!! EXCLUDED - CLIENT SATURATED: %s" % sw["excluded_client_saturated"])

    # ---- AC2 shape comparison, normalised to each curve's own N=1 -------------
    s1 = out["sweeps"].get("cn-s1")
    if s1:
        b = next((r["rows_per_s_median"] for r in s1["per_N"] if r["N"] == 1), None)
        pb = PUB_3100[1]
        comp = []
        for r in s1["per_N"]:
            n = r["N"]
            if n in PUB_3100 and b:
                comp.append({"N": n, "measured_rows_per_s": r["rows_per_s_median"],
                             "measured_norm": r["rows_per_s_median"] / b,
                             "published_3100_rows_per_s": PUB_3100[n],
                             "published_norm": PUB_3100[n] / pb,
                             "abs_ratio_measured_over_published": r["rows_per_s_median"] / PUB_3100[n]})
        out["ac2_s1_shape_vs_3100"] = comp
        lines.append("")
        lines.append("=== AC2: S=1 shape vs #3100 published pinned-core control ===")
        lines.append("%-4s %-13s %-9s %-13s %-9s %-8s" % (
            "N", "measured", "norm", "#3100", "norm", "meas/pub"))
        for c in comp:
            lines.append("%-4d %-13.0f %-9.3f %-13.0f %-9.3f %-8.3f" % (
                c["N"], c["measured_rows_per_s"], c["measured_norm"],
                c["published_3100_rows_per_s"], c["published_norm"],
                c["abs_ratio_measured_over_published"]))

    # ---- AC5 table -----------------------------------------------------------
    lines.append("")
    lines.append("=== AC5: context switches / migrations per N ===")
    lines.append("per-ROW columns are the load-bearing ones: cs/s necessarily rises with throughput,")
    lines.append("so only per-row shows whether the per-unit-work scheduler cost actually grew.")
    lines.append("%-16s %-4s %-14s %-14s %-14s %-14s %-8s %-11s %-11s" % (
        "sweep", "N", "cs/s cpu-wide", "migr/s", "vol cs/s(thr)", "nonvol cs/s(thr)", "threads",
        "vol/1k rows", "nonvol/1k"))
    for label, sw in out["sweeps"].items():
        for r in sw["per_N"]:
            rs = r["rows_per_s_median"]
            v1k = (r["server_thread_vol_ctxt_per_s_median"] / rs * 1000
                   if r["server_thread_vol_ctxt_per_s_median"] is not None and rs else None)
            n1k = (r["server_thread_nonvol_ctxt_per_s_median"] / rs * 1000
                   if r["server_thread_nonvol_ctxt_per_s_median"] is not None and rs else None)
            r["server_thread_vol_ctxt_per_1k_rows"] = v1k
            r["server_thread_nonvol_ctxt_per_1k_rows"] = n1k
            lines.append("%-16s %-4d %-14.0f %-14.0f %-14s %-14s %-8s %-11s %-11s" % (
                label, r["N"], r["ctxt_switches_cpu_wide_per_s_median"],
                r["cpu_migrations_cpu_wide_per_s_median"],
                ("%.0f" % r["server_thread_vol_ctxt_per_s_median"])
                if r["server_thread_vol_ctxt_per_s_median"] is not None else "n/a",
                ("%.0f" % r["server_thread_nonvol_ctxt_per_s_median"])
                if r["server_thread_nonvol_ctxt_per_s_median"] is not None else "n/a",
                r["server_threads_observed"],
                ("%.1f" % v1k) if v1k is not None else "n/a",
                ("%.2f" % n1k) if n1k is not None else "n/a"))

    # ---- cross-S scaling against a COMMON reference -------------------------
    # Self-normalising each arm to its own N=1 systematically flatters the wide arms:
    # the N=1 baseline DECLINES as cores are added (a single stream cannot fill them),
    # so a wide arm is divided by a weaker denominator. The cross-S curve therefore
    # normalises every arm to one common single-physical-core reference.
    arms = [(sw["S_physical_cores"], lab, sw) for lab, sw in out["sweeps"].items()
            if sw["merge_path"] == "bypass" and sw["S_physical_cores"] and len(sw["per_N"]) >= 5]
    arms.sort()
    s1arm = next((a for a in arms if a[0] == 1), None)
    if s1arm:
        s1_pn = s1arm[2]["per_N"]
        ref_n1 = next(r["rows_per_s_median"] for r in s1_pn if r["N"] == 1)
        ref_peak_rec = max(s1_pn, key=lambda r: r["rows_per_s_median"])
        ref_peak = ref_peak_rec["rows_per_s_median"]
        rows = []
        for S, lab, sw in arms:
            best = max(sw["per_N"], key=lambda r: r["rows_per_s_median"])
            n1 = next(r["rows_per_s_median"] for r in sw["per_N"] if r["N"] == 1)
            rows.append({
                "S_physical_cores": S, "arm": lab, "hw_threads": sw["server_cpus"],
                "own_N1_rows_per_s_median": n1,
                "best_rows_per_s_median": best["rows_per_s_median"],
                "N_at_peak": best["N"],
                "server_util_at_peak": best["server_cpu_util_of_pinned_set_median"],
                "client_util_at_peak": best["client_cpu_util_of_pinned_set_median"],
                "speedup_vs_common_ref_S1_N1": best["rows_per_s_median"] / ref_n1,
                "marginal_efficiency_vs_common_ref_S1_N1":
                    best["rows_per_s_median"] / (S * ref_n1),
                "speedup_vs_common_ref_S1_PEAK": best["rows_per_s_median"] / ref_peak,
                "marginal_efficiency_vs_common_ref_S1_PEAK":
                    best["rows_per_s_median"] / (S * ref_peak),
            })
        out["cross_S_scaling"] = {
            "common_reference_A_S1_N1_rows_per_s": ref_n1,
            "common_reference_B_S1_PEAK_rows_per_s": ref_peak,
            "common_reference_B_peak_N": ref_peak_rec["N"],
            "reference_choice_note": (
                "Reference B (S=1's PEAK, N=%d) is the primary denominator: it is the most the "
                "engine achieves on ONE physical core, so it is the fair 'perfect scaling' unit, "
                "and it is the CONSERVATIVE choice (it yields LOWER efficiencies than reference A). "
                "Reference A (S=1 at N=1) is reported alongside because it is the naive baseline. "
                "Both are shown so the denominator is never silently chosen."
                % ref_peak_rec["N"]),
            "self_normalisation_warning": (
                "Per-arm speedup columns divide by that arm's OWN N=1, which DECLINES with core "
                "count (S=1 %.0f, S=2 %.0f, S=4 %.0f, S=6 %.0f rows/s) because a single stream "
                "cannot fill more cores. Those columns are NOT comparable across S."
                % tuple([r["own_N1_rows_per_s_median"] for r in rows][:4] + [0] * (4 - len(rows)))),
            "per_arm": rows,
        }
        lines.append("")
        lines.append("=== CROSS-S SCALING vs a COMMON reference (the deliverable table) ===")
        lines.append("common ref A = S=1 @ N=1   : %.0f rows/s" % ref_n1)
        lines.append("common ref B = S=1 @ N=%-3d : %.0f rows/s   <- PRIMARY (best on 1 physical core; conservative)"
                     % (ref_peak_rec["N"], ref_peak))
        lines.append("%-3s %-12s %-14s %-13s %-6s %-8s %-11s %-11s %-11s %-11s" % (
            "S", "arm", "own N=1", "best agg", "N@pk", "srvUtl", "spdup/refA", "meff/refA",
            "spdup/refB", "meff/refB"))
        for r in rows:
            lines.append("%-3d %-12s %-14.0f %-13.0f %-6d %-8.3f %-11.3f %-11.3f %-11.3f %-11.3f" % (
                r["S_physical_cores"], r["arm"], r["own_N1_rows_per_s_median"],
                r["best_rows_per_s_median"], r["N_at_peak"], r["server_util_at_peak"],
                r["speedup_vs_common_ref_S1_N1"], r["marginal_efficiency_vs_common_ref_S1_N1"],
                r["speedup_vs_common_ref_S1_PEAK"], r["marginal_efficiency_vs_common_ref_S1_PEAK"]))
        lines.append("NOTE: each arm's own N=1 DECLINES with core count, so per-arm self-normalised")
        lines.append("      speedup/margeff columns flatter the wide arms and are NOT cross-comparable.")

    # ---- merge vs bypass -----------------------------------------------------
    lines.append("")
    lines.append("=== merge vs bypass reference (N=1) ===")
    mrefs = []
    for s in ("s1", "s6"):
        bp = out["sweeps"].get("cn-%s" % s)
        mg = out["sweeps"].get("cn-%s-merge-n1" % s)
        if not (bp and mg):
            continue
        b = next((r for r in bp["per_N"] if r["N"] == 1), None)
        m = next((r for r in mg["per_N"] if r["N"] == 1), None)
        if not (b and m):
            continue
        d = {"S": s, "bypass_rows_per_s_median": b["rows_per_s_median"],
             "merge_rows_per_s_median": m["rows_per_s_median"],
             "merge_over_bypass": m["rows_per_s_median"] / b["rows_per_s_median"],
             "delta_pct": (m["rows_per_s_median"] / b["rows_per_s_median"] - 1) * 100,
             "bypass_dispersion_pct": b["dispersion_pct_of_median"],
             "merge_dispersion_pct": m["dispersion_pct_of_median"]}
        mrefs.append(d)
        lines.append("%-4s bypass=%-12.0f merge=%-12.0f  merge/bypass=%.4f  (%+.2f%%)" % (
            s, d["bypass_rows_per_s_median"], d["merge_rows_per_s_median"],
            d["merge_over_bypass"], d["delta_pct"]))
    out["merge_vs_bypass_n1"] = mrefs

    # ---- warmth --------------------------------------------------------------
    tot_rb = sum(sum((p["server_io_delta"].get("read_bytes") or 0) for p in pts)
                 for pts in sweeps.values())
    npts = sum(len(p) for p in sweeps.values())
    out["warmth"] = {"total_read_bytes_all_points": tot_rb, "points": npts,
                     "note": ("/proc/<pid>/io read_bytes summed across every point; 0 = every read "
                              "served from page cache. rchar/syscr are tiny because Data.db is "
                              "mmap'd, so page-cache hits are faults, not read() syscalls.")}
    lines.append("")
    lines.append("warmth: total read_bytes across all %d points = %d (0 == fully page-cached)" % (npts, tot_rb))

    txt = "\n".join(lines) + "\n"
    open(os.path.join(RESULTS, "partA-analysis.json"), "w").write(json.dumps(out, indent=1) + "\n")
    open(os.path.join(RESULTS, "partA-analysis.txt"), "w").write(txt)
    print(txt)


if __name__ == "__main__":
    main()
