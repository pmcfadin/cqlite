#!/usr/bin/env python3
"""Text rendering for analyze-3225.py — the human-readable #3225 §2 analysis block.

Split out of analyze-3225.py to keep both files near the ~800-line campsite target
(CLAUDE.md). This module is PURE presentation: it reads the analysis dict that
analyze-3225.py builds and derives no new numbers, so a change here can never
alter a published value. Not a CLI — imported by name (underscored, unlike the
hyphenated harness executables) from the script beside it.
"""
from __future__ import annotations


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
    supp_labels = set(out.get("supplement_arms") or [])
    A("%-18s %-3s %-4s %-11s %-6s %-11s %-14s %-9s %-8s" % (
        "arm", "S", "P", "role", "peak N", "censored?", "rows/s median", "spr%", "srvUtl"))
    for arm in out["arms"]:
        role = "SUPPLEMENT" if arm["arm"] in supp_labels else "primary"
        pk = arm.get("peak")
        if not pk:
            A("%-18s %-3s %-4s %-11s -- %s" % (arm["arm"], arm["S_physical_cores"],
                                               arm["server_hw_threads_P"], role,
                                               arm.get("peak_unavailable_reason", "no peak")))
            continue
        A("%-18s %-3s %-4s %-11s %-6d %-11s %-14.0f %-9.1f %-8.3f" % (
            arm["arm"], arm["S_physical_cores"], arm["server_hw_threads_P"], role, pk["N"],
            "CENSORED" if pk["censored"] else "uncensored",
            pk["rows_per_s_median"], pk["dispersion_pct_of_median"] or 0.0,
            pk["server_cpu_util_of_pinned_set_median"] or 0.0))
    A("CENSORED = the peak sits at the top of the measured ramp, so it is a LOWER BOUND on the")
    A("           true optimum for that width. It is NOT the same claim as an uncensored peak.")
    A("SUPPLEMENT = a second, shorter run over the SAME cpus as its primary; it does not add a")
    A("           width. Read its peak only through the bridge analysis below.")
    A("")

    A("=== OVER-ADMISSION COST, BOTH CURRENCIES (relative to each width's own peak) ===")
    A("%-18s %-4s %-14s %-16s %-11s %-12s" % (
        "arm", "N", "rows/s median", "throughput vs pk", "p50 ms", "p50 x peak"))
    any_cost = False
    for arm in out["arms"]:
        for c in arm.get("over_admission_cost_vs_peak", []):
            any_cost = True
            A("%-18s %-4d %-14.0f %-16s %-11s %-12s" % (
                arm["arm"], c["N"], c["rows_per_s_median"],
                "%+.1f%%" % c["throughput_pct_vs_peak"],
                ("%.1f" % c["latency_p50_ms_median"]) if c["latency_p50_ms_median"] is not None else "n/a",
                ("%.2fx" % c["latency_p50_multiple_vs_peak"]) if c["latency_p50_multiple_vs_peak"] else "n/a"))
    if not any_cost:
        A("  (no over-peak points: every width's peak sits at the top of its ramp — see CENSORED above)")
    A("")

    A("=== THE SHIPPED DEFAULT (--max-concurrent-scans 64) AS A MEASURED POINT ===")
    A("%-18s %-9s %-14s %-16s %-11s %-12s" % (
        "arm", "measured?", "rows/s median", "throughput vs pk", "p50 ms", "p50 x peak"))
    for arm in out["arms"]:
        s = arm.get("shipped_default_64")
        if not s:
            continue
        if not s["measured"]:
            A("%-18s %-9s %s" % (arm["arm"], "NO", s["reason"]))
            continue
        A("%-18s %-9s %-14.0f %-16s %-11s %-12s" % (
            arm["arm"], "yes", s["rows_per_s_median"],
            "%+.1f%%" % s["throughput_pct_vs_peak"],
            ("%.1f" % s["latency_p50_ms_median"]) if s["latency_p50_ms_median"] is not None else "n/a",
            ("%.2fx" % s["latency_p50_multiple_vs_peak"]) if s["latency_p50_multiple_vs_peak"] else "n/a"))
    A("")

    A("=== FORMULA UNDER TEST: clamp(2 x P, 2, 64), deviation as % of that width's MEASURED peak ===")
    A("%-18s %-4s %-8s %-8s %-16s %-14s %-16s" % (
        "arm", "P", "pred N", "peak N", "rows/s @ pred", "dev vs peak", "vs constant 64"))
    for arm in out["arms"]:
        f = arm.get("formula_evaluation")
        if not f:
            continue
        if not f["predicted_N_measured"]:
            A("%-18s %-4d %-8d %-8d %s" % (
                arm["arm"], f["P_hw_threads"], f["predicted_N"],
                f["measured_peak_N"], f["unmeasured_note"]))
            continue
        gain = f.get("gain_vs_shipped_constant_64_pct_points")
        A("%-18s %-4d %-8d %-8d %-16.0f %-14s %-16s" % (
            arm["arm"], f["P_hw_threads"], f["predicted_N"], f["measured_peak_N"],
            f["rows_per_s_median_at_predicted_N"],
            "%+.1f%%" % f["deviation_pct_of_measured_peak"],
            ("%+.1f pp" % gain) if gain is not None else "n/a"))
    A("A width where the formula is WORSE than the constant 64 BLOCKS §3 until the coefficient")
    A("is re-fitted (tasks.md §2). 'vs constant 64' is positive when the formula wins.")
    A("")

    for br in out.get("bridges", []):
        A("=== CROSS-RUN BRIDGE: %s (primary) vs %s (supplement), S=%s ==="
          % (br["primary_arm"], br["supplement_arm"], br["S_physical_cores"]))
        A("  harness: primary %s | supplement %s"
          % (",".join(br["primary_harness_commits"]) or "unstamped",
             ",".join(br["supplement_harness_commits"]) or "unstamped"))
        if not br["bridge_points"]:
            A("  %s" % br["verdict"])
            A("")
            continue
        A("  %-5s %-16s %-9s %-16s %-9s %-11s %-10s" % (
            "N", "primary med", "spr%", "supplement med", "spr%", "offset", "exceeds spr?"))
        for b in br["bridge_points"]:
            A("  %-5d %-16.0f %-9.1f %-16.0f %-9.1f %-11s %-10s" % (
                b["N"], b["primary_rows_per_s_median"], b["primary_dispersion_pct"] or 0.0,
                b["supplement_rows_per_s_median"], b["supplement_dispersion_pct"] or 0.0,
                "%+.2f%%" % b["offset_pct_supplement_vs_primary"],
                "YES" if b["offset_exceeds_within_run_dispersion"] else "no"))
        A("  run-to-run uncertainty: %.2f%% (at N=%s)" % (
            br["cross_run_uncertainty_pct"] or 0.0, br.get("cross_run_uncertainty_at_N")))
        A("  VERDICT: %s" % br["verdict"])
        A("")

    for wf in out.get("supplemented_formula_verdicts", []):
        A("=== WITHIN-RUN FORMULA VERDICT AT S=%s (the out-of-fit falsification width) ==="
          % wf["width_S"])
        if not wf.get("resolved"):
            A("  UNRESOLVED: %s" % wf.get("reason"))
            A("")
            continue
        w, nv = wf["within_run"], wf["naive_cross_run_splice"]
        A("  predicted N=%d (P=%s): %.0f rows/s (spr %.1f%%) in %s" % (
            wf["predicted_N"], wf["P_hw_threads"], w["predicted_N_rows_per_s_median"],
            w["predicted_N_dispersion_pct"] or 0.0, w["arm"]))
        A("  comparator N=%d in the SAME run: %.0f rows/s (spr %.1f%%)" % (
            w["comparator_N"], w["comparator_rows_per_s_median"],
            w["comparator_dispersion_pct"] or 0.0))
        A("  WITHIN-RUN deviation: %+.2f%%   <- the number of record for this width" %
          w["predicted_vs_comparator_pct"])
        A("  naive cross-run splice (%s N=%d vs %s N=%d): %+.2f%%  [%s]" % (
            w["arm"], wf["predicted_N"], wf["primary_arm"], w["comparator_N"],
            nv["delta_pct"], "ACCEPTED" if nv["accepted"] else "REJECTED"))
        A("    %s" % nv["why"])
        A("  VERDICT: %s" % wf["verdict"])
        A("")

    ac5 = out.get("ac5") or {}
    A("=== AC5 — THE WIDEST WIDTH IN SCOPE ===")
    if not ac5.get("resolved"):
        A("  UNRESOLVED: %s" % ac5.get("reason", "no widest-width arm"))
    else:
        A("  arm %s: S=%s physical cores / P=%s hw threads. Widest IN SCOPE because %s" % (
            ac5["arm"], ac5["S_physical_cores"], ac5["P_hw_threads"], ac5["why_widest_in_scope"]))
        d = ac5["derived_point"]
        A("  derived default N=%d: median %.0f rows/s  (min %.0f / max %.0f, spr %.1f%%, %d reps, "
          "p50 %.1f ms, srvUtl %.3f)" % (
              d["N"], d["rows_per_s_median"], d["rows_per_s_min"], d["rows_per_s_max"],
              d["dispersion_pct"] or 0.0, d["reps_valid"], d["latency_p50_ms_median"] or 0.0,
              d["server_cpu_util_of_pinned_set_median"] or 0.0))
        A("  %-5s %-14s %-9s %-12s %-14s %-12s %s" % (
            "vs N", "median", "spr%", "derived gain", "> dispersion?", "reps disjoint?", "role"))
        for c in ac5["comparisons"]:
            if not c["measured"]:
                A("  %-5d %s" % (c["N"], c["reason"]))
                continue
            A("  %-5d %-14.0f %-9.1f %-12s %-14s %-12s %s" % (
                c["N"], c["rows_per_s_median"], c["dispersion_pct"] or 0.0,
                "%+.1f%%" % c["derived_gain_pct"],
                "YES (%.1f%%)" % c["max_dispersion_of_the_pair_pct"]
                if c["gain_exceeds_dispersion"] else "NO",
                "YES" if c["rep_ranges_disjoint"] else "no", c["role"]))
        A("  REGRESSION-FREE: %s" % ("yes — the derived default beats every measured alternative "
                                     "at this width" if ac5["regression_free"] else
                                     "NO — see the negative gain above"))
    A("")

    A("=== HARNESS PROVENANCE (read from each point's harness_commit stamp) ===")
    A("  %-18s %-14s %s" % ("arm", "harness", "uniform within the arm?"))
    for arm in out["arms"]:
        A("  %-18s %-14s %s" % (arm["arm"], ",".join(arm["harness_commits"]) or "unstamped",
                                "yes" if arm["harness_commit_uniform"] else
                                "NO — this arm was written by more than one revision"))
    A("")

    ci = out.get("corpus_identity") or {}
    A("=== CORPUS IDENTITY — one staged corpus, named (AC6) ===")
    A("  sha256(Data.db): %s" % (ci.get("sha256_data_db") or "UNAVAILABLE"))
    A("  source         : %s" % ci.get("sha256_source"))
    if ci.get("sha256_error"):
        A("  sha256 error   : %s" % ci["sha256_error"])
    ref = ci.get("reference") or {}
    if ref:
        A("  rows per scan  : %s" % ref.get("rows_per_scan_observed"))
        A("  staged         : %s (%s *-Data.db file(s))" % (ref.get("stage_dir"),
                                                            ref.get("data_db_files")))
        A("  bytes          : on-disk compressed %s / logical uncompressed %s" % (
            ref.get("ondisk_compressed_bytes"), ref.get("logical_uncompressed_bytes")))
    A("  arms checked   : %d" % len(ci.get("per_arm", [])))
    A("  VERDICT: %s" % ci.get("verdict", "not evaluated"))
    if out.get("smoke"):
        A("  (ADVISORY under --smoke: #3217's committed results predate the per-arm")
        A("   corpus-basis.json, so a FAIL here describes that input, not this round.)")
    A("")

    A("=== ADMISSION REJECTIONS (requests_unavailable) ACROSS EVERY POINT ===")
    tot = 0
    for arm in out["arms"]:
        n = arm["requests_unavailable_total_all_points"]
        tot += n
        A("  %-18s %d" % (arm["arm"], n))
    A("  TOTAL across all arms and all points: %d" % tot)
    A("  A non-zero total means the admission ceiling BOUND during the sweep, so those points")
    A("  measured the gate rather than the curve. Expected 0: the sweep runs with")
    A("  --max-concurrent-scans 64 = the top of the ramp.")
    A("")

    A("=== THE THREE BYTE BASES, AT EACH WIDTH'S PEAK (AC6) ===")
    A("%-18s %-22s %-22s %-22s" % (
        "arm", "logical uncompressed", "on-disk compressed", "arrow wire CAPACITY"))
    for arm in out["arms"]:
        b = arm.get("byte_bases_at_peak")
        if not b:
            continue
        fmt = lambda v: ("%.1f MB/s" % (v / 1e6)) if v else "n/a"  # noqa: E731
        A("%-18s %-22s %-22s %-22s" % (
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
