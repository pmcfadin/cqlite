#!/usr/bin/env python3
"""CQLite issue #3225 §2 analysis: peak N by server width, and what over-admission costs.

Adapted from docs/reports/ws0-3217-artifacts/partA-run/analyze-partA.py. #3217 asked
"where does C(N) stop scaling on S cores"; #3225 asks "what should the DEFAULT
--max-concurrent-scans be", so this emits:

  1. the per-(S, N) median table with min/max dispersion              (AC1)
  2. the peak N per width, LABELLED CENSORED when it sits at the top
     of the ramp, with server utilisation beside it                   (AC1/AC5)
  3. the over-admission cost in BOTH currencies: throughput % lost,
     and the p50 latency MULTIPLE                                     (AC5)
  4. the admission-rejection total across every point                 (validity)
  5. the three named byte bases                                       (AC6)
  6. clamp(2 x P, 2, 64) evaluated per width, as a % of that width's
     MEASURED peak                                                    (gates §3)

Dropped from #3217's version: the per-TID context-switch sidecar (that round's AC5;
this round does not run the sampler), the merge-vs-bypass reference, and the #3100
shape comparison.

Usage:
  analyze-3225.py <results-dir> [-o <out-dir>] [--ramp-top N]
  analyze-3225.py --smoke [-o <out-dir>]

--smoke runs the whole pipeline against #3217's COMMITTED points.jsonl records and
cross-checks a handful of derived values against #3217's own partA-analysis.json, so
this script is known-executable and known-numerically-sane BEFORE the 5-6 h run
produces the data it is meant to analyse. #3217's ramp stops at 16, so under --smoke
every width is expected to report CENSORED and the N=64 columns are expected to be
absent — that is the correct answer for that input, not a defect.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import statistics as st
import sys

SCHEMA = "ws0-3225.analysis/v1"

# The hypothesis under test (design.md D2). Pure, so it is evaluated here exactly as
# the Rust it gates: clamp(2 x P, 2, 64) over P = HARDWARE THREADS.
DERIVED_MIN = 2
DERIVED_CEILING = 64
SCANS_PER_HW_THREAD = 2


def derive_max_concurrent_scans(p: int) -> int:
    """clamp(2 x P, 2, 64) — the formula §3 would ship. P is hardware threads."""
    if p < 1:
        raise ValueError(f"available parallelism must be >= 1, got {p}")
    return max(DERIVED_MIN, min(DERIVED_CEILING, SCANS_PER_HW_THREAD * p))


# --------------------------------------------------------------------- inputs --
def expand_cpulist(spec: str) -> set[int]:
    out: set[int] = set()
    for part in str(spec).split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            out |= set(range(int(a), int(b) + 1))
        else:
            out.add(int(part))
    return out


def load_topology(path: str):
    """Sibling groups from a sweep-written cpu-topology.json, or None."""
    try:
        with open(path) as fh:
            doc = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None
    pairs = doc.get("smt_sibling_pairs")
    if not pairs:
        return None
    return [set(int(x) for x in p) for p in pairs]


def resolve_width(pts, arm_dir: str, fallback_topology):
    """Physical-core width S for an arm, with the METHOD recorded.

    sweep.sh only stamps server_physical_cores_S for its s1|s2|s4|s6 shorthands; the
    S=3 arm goes through the LITERAL cpu-list form and stamps null. Rather than
    dividing the hw-thread count by an assumed SMT factor, S is re-derived from the
    sibling groups the sweep itself read out of sysfs. An unresolvable arm is
    reported as UNRESOLVED and excluded from the width table by name — never
    silently defaulted, because a wrong S would mislabel the whole curve.
    """
    stamped = pts[0].get("server_physical_cores_S")
    if stamped:
        return int(stamped), "sweep-stamped (s1|s2|s4|s6 shorthand)"

    cpus = expand_cpulist(pts[0]["server_cpus"])
    for topo, how in ((load_topology(os.path.join(arm_dir, "cpu-topology.json")), "this arm's cpu-topology.json"),
                      (fallback_topology, "a peer arm's cpu-topology.json (same box, same run)")):
        if topo:
            covered = [g for g in topo if g & cpus]
            if covered:
                return len(covered), f"derived from {how}: {len(covered)} sibling groups touched"
    return None, "UNRESOLVED: no server_physical_cores_S stamp and no cpu-topology.json to derive from"


def load_results(results_dir: str):
    sweeps = {}
    for pj in sorted(glob.glob(os.path.join(results_dir, "*", "points.jsonl"))):
        label = os.path.basename(os.path.dirname(pj))
        pts = []
        with open(pj) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    pts.append(json.loads(line))
        if pts:
            sweeps[label] = (pts, os.path.dirname(pj))
    return sweeps


# -------------------------------------------------------------------- analysis --
def median_or_none(vals):
    vals = [v for v in vals if v is not None]
    return st.median(vals) if vals else None


def per_n_records(pts):
    """One record per N: medians over the VALID reps, dispersion, utilisation."""
    by_n: dict[int, list] = {}
    for p in pts:
        by_n.setdefault(p["target_concurrency_N"], []).append(p)

    recs = []
    for n in sorted(by_n):
        all_reps = by_n[n]
        valid = [p for p in all_reps if not p.get("client_saturated")]
        if not valid:
            # Every rep at this N measured the client, not the engine. Recorded with
            # an explicit reason so the N is visibly absent from the curve, not
            # quietly missing from it.
            recs.append({
                "N": n, "reps_total": len(all_reps), "reps_valid": 0,
                "excluded": "ALL REPS CLIENT-SATURATED — no engine measurement at this N",
                "client_util_of_excluded": [p["client_cpu_utilization_of_pinned_set"] for p in all_reps],
                "requests_unavailable_total": sum(p.get("requests_unavailable") or 0 for p in all_reps),
            })
            continue
        rps = sorted(p["rows_per_s_aggregate"] for p in valid)
        med = st.median(rps)
        unavail = [p.get("requests_unavailable") for p in valid]
        recs.append({
            "N": n,
            "reps_total": len(all_reps),
            "reps_valid": len(valid),
            "rows_per_s_min": rps[0],
            "rows_per_s_median": med,
            "rows_per_s_max": rps[-1],
            "dispersion_pct_of_median": (rps[-1] - rps[0]) / med * 100 if med else None,
            "rows_per_s_per_stream_median": med / n if n else None,
            "latency_p50_ms_median": median_or_none(
                [p["latency_ms"]["p50"] for p in valid if p.get("latency_ms")]),
            "latency_p99_ms_median": median_or_none(
                [p["latency_ms"].get("p99") for p in valid if p.get("latency_ms")]),
            "server_cpu_util_of_pinned_set_median": median_or_none(
                [p.get("server_cpu_utilization_of_pinned_set") for p in valid]),
            "client_cpu_util_of_pinned_set_median": median_or_none(
                [p.get("client_cpu_utilization_of_pinned_set") for p in valid]),
            "requests_unavailable_total": sum(x for x in unavail if x is not None),
            "admission_clean": all(x == 0 for x in unavail),
            "requests_error_total": sum(p.get("requests_error") or 0 for p in valid),
            "cycles_per_row_median": median_or_none([p.get("cycles_per_row") for p in valid]),
            "IPC_median": median_or_none([p.get("IPC") for p in valid]),
            "bytes_per_s_logical_uncompressed_median": median_or_none(
                [p.get("bytes_per_s_logical_uncompressed") for p in valid]),
            "bytes_per_s_ondisk_compressed_median": median_or_none(
                [p.get("bytes_per_s_ondisk_compressed") for p in valid]),
            "bytes_per_s_arrow_wire_capacity_median": median_or_none(
                [p.get("bytes_per_s_arrow_wire_capacity") for p in valid]),
        })
    return recs


def curve_points(recs):
    return [r for r in recs if r.get("reps_valid")]


def analyse_arm(label, pts, arm_dir, fallback_topology, ramp_top):
    S, S_method = resolve_width(pts, arm_dir, fallback_topology)
    recs = per_n_records(pts)
    curve = curve_points(recs)
    hw_threads = pts[0].get("server_cpu_count_hw_threads") or len(expand_cpulist(pts[0]["server_cpus"]))

    ns_measured = [r["N"] for r in recs]
    top_of_ramp = ramp_top if ramp_top is not None else (max(ns_measured) if ns_measured else None)

    peak = max(curve, key=lambda r: r["rows_per_s_median"]) if curve else None
    censored = bool(peak and top_of_ramp is not None and peak["N"] >= top_of_ramp)

    excluded = [
        {"N": p["target_concurrency_N"], "rep": p["rep"],
         "rows_per_s": p["rows_per_s_aggregate"],
         "client_util": p["client_cpu_utilization_of_pinned_set"],
         "threshold": p.get("client_saturation_threshold")}
        for p in pts if p.get("client_saturated")
    ]

    arm = {
        "arm": label,
        "S_physical_cores": S,
        "S_resolution_method": S_method,
        "server_cpus": pts[0]["server_cpus"],
        "server_hw_threads_P": hw_threads,
        "client_cpus": pts[0]["client_cpus"],
        "merge_path": pts[0].get("merge_path"),
        "step_seconds_requested": pts[0].get("step_seconds_requested"),
        "ramp_measured": ns_measured,
        "ramp_top": top_of_ramp,
        "per_N": recs,
        "peak": None,
        "excluded_client_saturated": excluded,
        "excluded_client_saturated_count": len(excluded),
        "requests_unavailable_total_all_points": sum(
            p.get("requests_unavailable") or 0 for p in pts),
    }

    if peak is None:
        arm["peak_unavailable_reason"] = (
            "no valid (non-client-saturated) point in this arm — nothing to take a peak of")
        return arm

    arm["peak"] = {
        "N": peak["N"],
        "rows_per_s_median": peak["rows_per_s_median"],
        "rows_per_s_min": peak["rows_per_s_min"],
        "rows_per_s_max": peak["rows_per_s_max"],
        "dispersion_pct_of_median": peak["dispersion_pct_of_median"],
        "latency_p50_ms_median": peak["latency_p50_ms_median"],
        "server_cpu_util_of_pinned_set_median": peak["server_cpu_util_of_pinned_set_median"],
        "censored": censored,
        "censored_note": (
            "CENSORED: the peak sits at the TOP of the measured ramp (N=%d), so the true "
            "peak is at or ABOVE it and this width's optimum is a LOWER BOUND." % peak["N"]
        ) if censored else "uncensored: the curve turns over inside the measured ramp",
    }

    # ---- over-admission cost, in both currencies ----------------------------
    # Currency 1: throughput lost, as a % of this width's measured peak.
    # Currency 2: the p50 latency MULTIPLE. A % on a latency that grows 10x reads as
    # "+900%" and is unusable in guidance; the multiple is how an operator thinks.
    by_n = {r["N"]: r for r in curve}
    costs = []
    for n, rec in sorted(by_n.items()):
        if n <= peak["N"]:
            continue
        thr_pct = (rec["rows_per_s_median"] - peak["rows_per_s_median"]) / peak["rows_per_s_median"] * 100
        p50_pk = peak["latency_p50_ms_median"]
        p50_n = rec["latency_p50_ms_median"]
        costs.append({
            "N": n,
            "rows_per_s_median": rec["rows_per_s_median"],
            "throughput_pct_vs_peak": thr_pct,
            "latency_p50_ms_median": p50_n,
            "latency_p50_multiple_vs_peak": (p50_n / p50_pk) if (p50_pk and p50_n) else None,
            "server_cpu_util_of_pinned_set_median": rec["server_cpu_util_of_pinned_set_median"],
        })
    arm["over_admission_cost_vs_peak"] = costs
    arm["over_admission_cost_note"] = (
        "Both currencies are reported because they disagree in magnitude and an operator needs "
        "both: throughput as a % of this width's own measured peak, latency as a p50 MULTIPLE.")

    # ---- the shipped default (64) as a measured point -----------------------
    shipped = by_n.get(DERIVED_CEILING)
    if shipped:
        p50_pk = peak["latency_p50_ms_median"]
        arm["shipped_default_64"] = {
            "N": DERIVED_CEILING,
            "measured": True,
            "rows_per_s_median": shipped["rows_per_s_median"],
            "rows_per_s_min": shipped["rows_per_s_min"],
            "rows_per_s_max": shipped["rows_per_s_max"],
            "dispersion_pct_of_median": shipped["dispersion_pct_of_median"],
            "throughput_pct_vs_peak": (shipped["rows_per_s_median"] - peak["rows_per_s_median"])
                                      / peak["rows_per_s_median"] * 100,
            "latency_p50_ms_median": shipped["latency_p50_ms_median"],
            "latency_p50_multiple_vs_peak": (
                shipped["latency_p50_ms_median"] / p50_pk
                if (p50_pk and shipped["latency_p50_ms_median"]) else None),
            "requests_unavailable_total": shipped["requests_unavailable_total"],
        }
    else:
        arm["shipped_default_64"] = {
            "N": DERIVED_CEILING, "measured": False,
            "reason": "N=64 is not in this arm's measured ramp (%s); no value is inferred."
                      % ",".join(str(n) for n in ns_measured),
        }

    # ---- the formula under test --------------------------------------------
    if hw_threads:
        pred = derive_max_concurrent_scans(int(hw_threads))
        prec = by_n.get(pred)
        entry = {
            "P_hw_threads": int(hw_threads),
            "formula": "clamp(%d x P, %d, %d)" % (SCANS_PER_HW_THREAD, DERIVED_MIN, DERIVED_CEILING),
            "predicted_N": pred,
            "measured_peak_N": peak["N"],
            "predicted_N_measured": prec is not None,
        }
        if prec:
            entry.update({
                "rows_per_s_median_at_predicted_N": prec["rows_per_s_median"],
                "deviation_pct_of_measured_peak":
                    (prec["rows_per_s_median"] - peak["rows_per_s_median"]) / peak["rows_per_s_median"] * 100,
                "latency_p50_ms_median_at_predicted_N": prec["latency_p50_ms_median"],
            })
            shipped_thr = arm["shipped_default_64"].get("throughput_pct_vs_peak")
            if shipped_thr is not None:
                entry["better_than_shipped_constant_64"] = (
                    entry["deviation_pct_of_measured_peak"] >= shipped_thr)
                entry["gain_vs_shipped_constant_64_pct_points"] = (
                    entry["deviation_pct_of_measured_peak"] - shipped_thr)
        else:
            # No interpolation. A number nobody measured is not evidence.
            entry["deviation_pct_of_measured_peak"] = None
            entry["unmeasured_note"] = (
                "predicted N=%d is NOT in this arm's ramp (%s). The deviation is UNKNOWN, not 0 — "
                "no value is interpolated." % (pred, ",".join(str(n) for n in ns_measured)))
        arm["formula_evaluation"] = entry

    # ---- the three byte bases, at the peak ----------------------------------
    basis = pts[0].get("corpus_basis") or {}
    arm["byte_bases_at_peak"] = {
        "logical_uncompressed_bytes_per_s": by_n[peak["N"]]["bytes_per_s_logical_uncompressed_median"],
        "ondisk_compressed_bytes_per_s": by_n[peak["N"]]["bytes_per_s_ondisk_compressed_median"],
        "arrow_wire_capacity_bytes_per_s": by_n[peak["N"]]["bytes_per_s_arrow_wire_capacity_median"],
        "logical_uncompressed_bytes_per_row": pts[0].get("logical_uncompressed_bytes_per_row"),
        "ondisk_compressed_bytes_per_row": pts[0].get("ondisk_compressed_bytes_per_row"),
        "basis_note": (
            "logical/uncompressed = rows/s x the CompressionInfo.db dataLength basis; "
            "on-disk compressed = rows/s x the summed *-Data.db basis; "
            "arrow-wire = flight-loadgen Arrow buffer CAPACITY, NOT gRPC-on-the-wire bytes. "
            "The three are DIFFERENT quantities and are never mixed into one 'MB/s'."),
        "corpus_sha256_data_db": basis.get("sha256_data_db") or basis.get("data_db_sha256"),
    }
    return arm


def render(out):
    L = []
    A = L.append
    A("==== #3225 §2: peak concurrency by server width ====")
    A("schema: %s" % out["schema"])
    A("results: %s" % out["results_dir"])
    A("")

    for arm in out["arms"]:
        A("=== %s  (S=%s physical cores / P=%s hw threads, cpus=%s, path=%s) ===" % (
            arm["arm"], arm["S_physical_cores"], arm["server_hw_threads_P"],
            arm["server_cpus"], arm["merge_path"]))
        A("  S resolution: %s" % arm["S_resolution_method"])
        A("  %-4s %-5s %-12s %-12s %-12s %-7s %-12s %-10s %-8s %-8s %-6s" % (
            "N", "reps", "rows/s min", "rows/s med", "rows/s max", "spr%",
            "rows/s/strm", "p50 ms", "srvUtl", "cliUtl", "unav"))
        for r in arm["per_N"]:
            if not r.get("reps_valid"):
                A("  %-4d %-5s %s" % (r["N"], "0/%d" % r["reps_total"], r["excluded"]))
                continue
            A("  %-4d %-5s %-12.0f %-12.0f %-12.0f %-7.1f %-12.0f %-10s %-8.3f %-8.3f %-6d" % (
                r["N"], "%d/%d" % (r["reps_valid"], r["reps_total"]),
                r["rows_per_s_min"], r["rows_per_s_median"], r["rows_per_s_max"],
                r["dispersion_pct_of_median"] or 0.0, r["rows_per_s_per_stream_median"] or 0.0,
                ("%.1f" % r["latency_p50_ms_median"]) if r["latency_p50_ms_median"] is not None else "n/a",
                r["server_cpu_util_of_pinned_set_median"] or 0.0,
                r["client_cpu_util_of_pinned_set_median"] or 0.0,
                r["requests_unavailable_total"]))
        if arm["excluded_client_saturated"]:
            A("  !!! EXCLUDED — CLIENT SATURATED (%d rep(s)); these measured the CLIENT, not the engine:"
              % arm["excluded_client_saturated_count"])
            for e in arm["excluded_client_saturated"]:
                A("      N=%-4d rep=%-2d rows/s=%-12.0f client_util=%.3f (gate %.2f)" % (
                    e["N"], e["rep"], e["rows_per_s"], e["client_util"], e["threshold"] or 0.0))
        A("")

    A("=== PEAK N BY WIDTH (the deliverable) ===")
    A("%-14s %-3s %-4s %-6s %-11s %-14s %-9s %-8s" % (
        "arm", "S", "P", "peak N", "censored?", "rows/s median", "spr%", "srvUtl"))
    for arm in out["arms"]:
        pk = arm.get("peak")
        if not pk:
            A("%-14s %-3s %-4s  -- %s" % (arm["arm"], arm["S_physical_cores"],
                                          arm["server_hw_threads_P"],
                                          arm.get("peak_unavailable_reason", "no peak")))
            continue
        A("%-14s %-3s %-4s %-6d %-11s %-14.0f %-9.1f %-8.3f" % (
            arm["arm"], arm["S_physical_cores"], arm["server_hw_threads_P"], pk["N"],
            "CENSORED" if pk["censored"] else "uncensored",
            pk["rows_per_s_median"], pk["dispersion_pct_of_median"] or 0.0,
            pk["server_cpu_util_of_pinned_set_median"] or 0.0))
    A("CENSORED = the peak sits at the top of the measured ramp, so it is a LOWER BOUND on the")
    A("           true optimum for that width. It is NOT the same claim as an uncensored peak.")
    A("")

    A("=== OVER-ADMISSION COST, BOTH CURRENCIES (relative to each width's own peak) ===")
    A("%-14s %-4s %-14s %-16s %-11s %-12s" % (
        "arm", "N", "rows/s median", "throughput vs pk", "p50 ms", "p50 x peak"))
    any_cost = False
    for arm in out["arms"]:
        for c in arm.get("over_admission_cost_vs_peak", []):
            any_cost = True
            A("%-14s %-4d %-14.0f %-16s %-11s %-12s" % (
                arm["arm"], c["N"], c["rows_per_s_median"],
                "%+.1f%%" % c["throughput_pct_vs_peak"],
                ("%.1f" % c["latency_p50_ms_median"]) if c["latency_p50_ms_median"] is not None else "n/a",
                ("%.2fx" % c["latency_p50_multiple_vs_peak"]) if c["latency_p50_multiple_vs_peak"] else "n/a"))
    if not any_cost:
        A("  (no over-peak points: every width's peak sits at the top of its ramp — see CENSORED above)")
    A("")

    A("=== THE SHIPPED DEFAULT (--max-concurrent-scans 64) AS A MEASURED POINT ===")
    A("%-14s %-9s %-14s %-16s %-11s %-12s" % (
        "arm", "measured?", "rows/s median", "throughput vs pk", "p50 ms", "p50 x peak"))
    for arm in out["arms"]:
        s = arm.get("shipped_default_64")
        if not s:
            continue
        if not s["measured"]:
            A("%-14s %-9s %s" % (arm["arm"], "NO", s["reason"]))
            continue
        A("%-14s %-9s %-14.0f %-16s %-11s %-12s" % (
            arm["arm"], "yes", s["rows_per_s_median"],
            "%+.1f%%" % s["throughput_pct_vs_peak"],
            ("%.1f" % s["latency_p50_ms_median"]) if s["latency_p50_ms_median"] is not None else "n/a",
            ("%.2fx" % s["latency_p50_multiple_vs_peak"]) if s["latency_p50_multiple_vs_peak"] else "n/a"))
    A("")

    A("=== FORMULA UNDER TEST: clamp(2 x P, 2, 64), deviation as % of that width's MEASURED peak ===")
    A("%-14s %-4s %-8s %-8s %-16s %-14s %-16s" % (
        "arm", "P", "pred N", "peak N", "rows/s @ pred", "dev vs peak", "vs constant 64"))
    for arm in out["arms"]:
        f = arm.get("formula_evaluation")
        if not f:
            continue
        if not f["predicted_N_measured"]:
            A("%-14s %-4d %-8d %-8d %s" % (
                arm["arm"], f["P_hw_threads"], f["predicted_N"],
                f["measured_peak_N"], f["unmeasured_note"]))
            continue
        gain = f.get("gain_vs_shipped_constant_64_pct_points")
        A("%-14s %-4d %-8d %-8d %-16.0f %-14s %-16s" % (
            arm["arm"], f["P_hw_threads"], f["predicted_N"], f["measured_peak_N"],
            f["rows_per_s_median_at_predicted_N"],
            "%+.1f%%" % f["deviation_pct_of_measured_peak"],
            ("%+.1f pp" % gain) if gain is not None else "n/a"))
    A("A width where the formula is WORSE than the constant 64 BLOCKS §3 until the coefficient")
    A("is re-fitted (tasks.md §2). 'vs constant 64' is positive when the formula wins.")
    A("")

    A("=== ADMISSION REJECTIONS (requests_unavailable) ACROSS EVERY POINT ===")
    tot = 0
    for arm in out["arms"]:
        n = arm["requests_unavailable_total_all_points"]
        tot += n
        A("  %-14s %d" % (arm["arm"], n))
    A("  TOTAL across all arms and all points: %d" % tot)
    A("  A non-zero total means the admission ceiling BOUND during the sweep, so those points")
    A("  measured the gate rather than the curve. Expected 0: the sweep runs with")
    A("  --max-concurrent-scans 64 = the top of the ramp.")
    A("")

    A("=== THE THREE BYTE BASES, AT EACH WIDTH'S PEAK (AC6) ===")
    A("%-14s %-22s %-22s %-22s" % (
        "arm", "logical uncompressed", "on-disk compressed", "arrow wire CAPACITY"))
    for arm in out["arms"]:
        b = arm.get("byte_bases_at_peak")
        if not b:
            continue
        fmt = lambda v: ("%.1f MB/s" % (v / 1e6)) if v else "n/a"  # noqa: E731
        A("%-14s %-22s %-22s %-22s" % (
            arm["arm"], fmt(b["logical_uncompressed_bytes_per_s"]),
            fmt(b["ondisk_compressed_bytes_per_s"]),
            fmt(b["arrow_wire_capacity_bytes_per_s"])))
    A("  These are THREE DIFFERENT quantities measured over the same rows; a bare 'MB/s' would")
    A("  be ambiguous between them by a factor of ~3.5 (the LZ4 ratio). Never collapse them.")
    if out.get("smoke"):
        A("")
        A("=== SMOKE-TEST CROSS-CHECKS vs #3217's committed partA-analysis.json ===")
        for c in out["smoke"]["checks"]:
            A("  %-6s %s" % (c["verdict"], c["what"]))
        A("  %d checked, %d matched, %d mismatched" % (
            out["smoke"]["checked"], out["smoke"]["matched"], out["smoke"]["mismatched"]))
    return "\n".join(L) + "\n"


def smoke_cross_check(out, partA_json_path):
    """Re-derive a few numbers #3217 published and compare, to catch a wiring error.

    This does NOT assert throughput values against a threshold (CLAUDE.md bans
    wall-clock/perf asserts in a correctness path, and this is not a test anyway) —
    it recomputes #3217's OWN published medians from #3217's OWN points and reports
    agreement, which only fails if this script is reading the records wrongly.
    """
    with open(partA_json_path) as fh:
        pub = json.load(fh)
    checks, matched, mismatched = [], 0, 0
    ours = {a["arm"]: a for a in out["arms"]}
    for label, sw in pub.get("sweeps", {}).items():
        mine = ours.get(label)
        if not mine:
            checks.append({"verdict": "SKIP", "what": "%s: not present in the loaded results" % label})
            continue
        pub_by_n = {r["N"]: r for r in sw.get("per_N", [])}
        my_by_n = {r["N"]: r for r in mine["per_N"] if r.get("reps_valid")}
        for n, pr in sorted(pub_by_n.items()):
            mr = my_by_n.get(n)
            if not mr:
                checks.append({"verdict": "MISS", "what": "%s N=%d: absent from our curve" % (label, n)})
                mismatched += 1
                continue
            a, b = pr["rows_per_s_median"], mr["rows_per_s_median"]
            same = (a == b) or (abs(a - b) <= 1e-6 * max(abs(a), abs(b)))
            checks.append({
                "verdict": "OK" if same else "DIFF",
                "what": "%s N=%-3d median rows/s: published %.0f vs recomputed %.0f" % (label, n, a, b),
            })
            matched += same
            mismatched += (not same)
    return {"checks": checks, "checked": len(checks), "matched": matched, "mismatched": mismatched,
            "source": partA_json_path}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results_dir", nargs="?", help="directory holding <arm>/points.jsonl")
    ap.add_argument("-o", "--out-dir", help="where to write analysis-3225.{json,txt} "
                                            "(default: the results dir; /tmp under --smoke)")
    ap.add_argument("--ramp-top", type=int, default=None,
                    help="the intended top of the N ramp, for the CENSORED label "
                         "(default: the largest N actually measured in each arm)")
    ap.add_argument("--smoke", action="store_true",
                    help="run against #3217's committed results and cross-check its partA-analysis.json")
    args = ap.parse_args()

    here = os.path.dirname(os.path.abspath(__file__))
    repo = os.path.abspath(os.path.join(here, "..", "..", "..", ".."))
    partA_json = None

    if args.smoke:
        if args.results_dir:
            ap.error("--smoke takes no results_dir (it uses #3217's committed results)")
        results_dir = os.path.join(repo, "docs", "reports", "ws0-3217-artifacts", "results")
        partA_json = os.path.join(results_dir, "partA-analysis.json")
        out_dir = args.out_dir or "/tmp/3225-smoke"
        if not os.path.isfile(partA_json):
            print("ERROR: #3217 partA-analysis.json not found at %s" % partA_json, file=sys.stderr)
            return 1
    else:
        if not args.results_dir:
            ap.error("results_dir is required (or use --smoke)")
        results_dir = args.results_dir
        out_dir = args.out_dir or results_dir

    if not os.path.isdir(results_dir):
        print("ERROR: results dir not found: %s" % results_dir, file=sys.stderr)
        return 1

    sweeps = load_results(results_dir)
    if not sweeps:
        print("ERROR: no <arm>/points.jsonl under %s — nothing to analyse" % results_dir,
              file=sys.stderr)
        return 1

    # One shared fallback topology: every arm ran on the same box in the same round.
    fallback_topology = None
    for _lab, (_pts, d) in sorted(sweeps.items()):
        fallback_topology = load_topology(os.path.join(d, "cpu-topology.json"))
        if fallback_topology:
            break

    arms = []
    for label, (pts, arm_dir) in sorted(sweeps.items()):
        arms.append(analyse_arm(label, pts, arm_dir, fallback_topology, args.ramp_top))
    # Widest last: the width table reads naturally bottom-up, and an UNRESOLVED S
    # sorts to the end rather than colliding with a real width.
    arms.sort(key=lambda a: (a["S_physical_cores"] is None, a["S_physical_cores"] or 0, a["arm"]))

    out = {
        "schema": SCHEMA,
        "results_dir": os.path.abspath(results_dir),
        "formula_under_test": "clamp(%d x P, %d, %d)" % (
            SCANS_PER_HW_THREAD, DERIVED_MIN, DERIVED_CEILING),
        "arms": arms,
        "arms_with_unresolved_width": [a["arm"] for a in arms if a["S_physical_cores"] is None],
        "requests_unavailable_total_all_arms": sum(
            a["requests_unavailable_total_all_points"] for a in arms),
    }
    if args.smoke:
        out["smoke"] = smoke_cross_check(out, partA_json)

    os.makedirs(out_dir, exist_ok=True)
    txt = render(out)
    with open(os.path.join(out_dir, "analysis-3225.json"), "w") as fh:
        fh.write(json.dumps(out, indent=1) + "\n")
    with open(os.path.join(out_dir, "analysis-3225.txt"), "w") as fh:
        fh.write(txt)
    print(txt)
    print("wrote %s/analysis-3225.{json,txt}" % out_dir, file=sys.stderr)

    if args.smoke and out["smoke"]["mismatched"]:
        print("SMOKE: %d cross-check(s) did not match #3217's published values"
              % out["smoke"]["mismatched"], file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
