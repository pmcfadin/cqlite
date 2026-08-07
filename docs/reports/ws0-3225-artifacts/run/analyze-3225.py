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
  5. the three named byte bases, plus the corpus identity every arm ran
     against (one sha256, cross-checked arm-to-arm)                   (AC6)
  6. clamp(2 x P, 2, 64) evaluated per width, as a % of that width's
     MEASURED peak                                                    (gates §3)
  7. the CROSS-RUN BRIDGE analysis for a supplement arm: a width whose
     predicted N was absent from the first ramp is closed by a second
     run, and the N the two runs SHARE measures the run-to-run offset.
     Every cross-run delta smaller than that offset is reported as an
     ARTIFACT, and the width's formula verdict is taken from the
     WITHIN-run pair instead                                          (gates §3)
  8. the widest-width AC5 block, and the harness_commit provenance of
     every arm, read from the points rather than from prose           (AC5)

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
import statistics as st
import sys

from analyze_3225_render import render
from analyze_3225_validity import admission_ceiling, corpus_identity

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


def load_corpus_basis(arm_dir: str):
    """The arm's committed corpus-basis.json, or an explicit absence record.

    The basis is written per ARM by the sweep, not stamped into each point, so a
    reader that looks for it on the point silently gets None. Absence is reported
    by name here rather than rendering as a blank cell.
    """
    path = os.path.join(arm_dir, "corpus-basis.json")
    try:
        with open(path) as fh:
            doc = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        return {"present": False, "path": path, "reason": "%s: %s" % (type(exc).__name__, exc)}
    doc = dict(doc)
    doc.update({"present": True, "path": path})
    return doc


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


# run-3225.sh QUARANTINES a failed/partial arm to "<arm>.partial-<utc>" and re-runs
# that arm from rep 1. Such a directory is a DISCARDED ATTEMPT, not a measurement: its
# points.jsonl is the truncated prefix of a run that was thrown away. Admitting one is
# not merely a distorted curve — the bridge analysis pairs arms by `server_cpus`, and a
# quarantined arm carries the SAME pinned set as the real arm that replaced it, so it
# pairs as that arm's SUPPLEMENT and manufactures a cross-run bridge out of one run's
# own discarded prefix. Matched on the marker rather than the full timestamp shape so a
# hand-made or differently-stamped quarantine cannot slip past on a spelling.
QUARANTINE_MARKER = ".partial-"


def classify_result_dir(arm_dir: str):
    """(accept, reason) for one candidate <results-dir>/<name> directory.

    Fail closed, on AFFIRMATIVE evidence of a COMPLETED arm — never on the absence of
    a bad signal. Three things must all hold: the name is not a quarantine name, the
    points.jsonl is readable and yields at least one record, and a summary.json is
    present and parses. That last one is the same completion marker run-3225.sh's
    arm_complete() uses (summarize-sweep.py writes summary.json only after the final
    rep), so "complete" means here exactly what it means to the driver. Every refusal
    returns the reason, so the caller reports it by name instead of dropping it.
    """
    name = os.path.basename(arm_dir.rstrip(os.sep))
    if QUARANTINE_MARKER in name:
        return False, ("QUARANTINED partial arm (name contains %r) — a discarded attempt "
                       "run-3225.sh moved aside and re-ran; its points are a truncated "
                       "prefix, and it shares the real arm's server_cpus so it would "
                       "also pair as that arm's supplement" % QUARANTINE_MARKER)

    pj = os.path.join(arm_dir, "points.jsonl")
    if not os.path.isfile(pj):
        return False, "no points.jsonl — not a sweep arm directory"

    summary = os.path.join(arm_dir, "summary.json")
    if not os.path.isfile(summary):
        return False, ("no summary.json — the arm never reached its final rep, so this is "
                       "an INCOMPLETE arm, not a curve (run-3225.sh: summary.json is the "
                       "completion marker)")
    try:
        with open(summary) as fh:
            json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        return False, "summary.json does not parse (%s: %s) — completion unverifiable" % (
            type(exc).__name__, exc)

    return True, "accepted: not quarantined, points.jsonl present, summary.json parses"


def load_results(results_dir: str):
    """({label: (points, arm_dir)}, [refusal records]) for one results directory.

    Both halves are returned: an EXCLUSION IS A RESULT. A loader that quietly globbed
    every */points.jsonl is what let a quarantined attempt enter the analysis as a real
    arm, so every candidate directory is now either accepted with a stated basis or
    refused with a stated reason, and the caller publishes the refusals.
    """
    sweeps = {}
    excluded = []
    candidates = sorted(
        d for d in glob.glob(os.path.join(results_dir, "*"))
        if os.path.isdir(d))
    for arm_dir in candidates:
        label = os.path.basename(arm_dir)
        ok, reason = classify_result_dir(arm_dir)
        if not ok:
            excluded.append({"dir": label, "path": arm_dir, "reason": reason})
            continue
        pts = []
        # Initialised so the handler can name a line even when open() itself raised —
        # otherwise the diagnostic for an unreadable file is a NameError on lineno,
        # i.e. the refusal path crashes instead of refusing.
        lineno = 0
        try:
            with open(os.path.join(arm_dir, "points.jsonl")) as fh:
                for lineno, line in enumerate(fh, 1):
                    line = line.strip()
                    if line:
                        pts.append(json.loads(line))
        except (OSError, json.JSONDecodeError) as exc:
            where = "at line %d" % lineno if lineno else "before the first line (open failed)"
            excluded.append({
                "dir": label, "path": arm_dir,
                "reason": "points.jsonl unreadable %s (%s: %s) — a partially parsed arm "
                          "is not an arm" % (where, type(exc).__name__, exc)})
            continue
        if not pts:
            excluded.append({
                "dir": label, "path": arm_dir,
                "reason": "points.jsonl holds no records — the arm measured nothing"})
            continue
        sweeps[label] = (pts, arm_dir)
    return sweeps, excluded


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

    # Harness provenance, read from the POINTS: two arms of this round ran under
    # different harness revisions (a sweep.sh fix and a --list fix), so the report
    # states the split from the stamps instead of from memory. More than one stamp
    # inside ONE arm would mean the arm was written by two revisions — recorded.
    harness_commits = sorted({p.get("harness_commit") for p in pts if p.get("harness_commit")})

    arm = {
        "arm": label,
        "S_physical_cores": S,
        "S_resolution_method": S_method,
        "harness_commits": harness_commits,
        "harness_commit_uniform": len(harness_commits) == 1,
        "corpus_basis": load_corpus_basis(arm_dir),
        "rows_per_scan_observed": pts[0].get("rows_per_scan_observed"),
        # Read from EVERY point, not just the first: the admission ceiling lives inside
        # this string, and one point configured differently is a different measurement.
        # A "uniform" flag derived from all of them is what lets the ceiling check
        # speak for the arm rather than for point 0.
        "server_flags": pts[0].get("server_flags"),
        "server_flags_distinct": sorted({str(p.get("server_flags")) for p in pts}),
        "server_flags_uniform_across_points": len({str(p.get("server_flags")) for p in pts}) == 1,
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
    }
    return arm


# ------------------------------------------------------- cross-run bridging --
def pair_supplements(arms):
    """(primary, supplement) pairs: two arms measuring the SAME server pinned set.

    A width whose predicted N was absent from the first ramp is closed by a second,
    shorter run over the same CPUs. The arm with more measured N values is the
    PRIMARY curve; the others are SUPPLEMENTS to it. Derived from the recorded
    `server_cpus`, never from the arm's name.
    """
    groups: dict[str, list] = {}
    for a in arms:
        groups.setdefault(str(a["server_cpus"]), []).append(a)
    pairs = []
    for _cpus, members in sorted(groups.items()):
        if len(members) < 2:
            continue
        primary = max(members, key=lambda a: len(a["ramp_measured"]))
        for supp in members:
            if supp is not primary:
                pairs.append((primary, supp))
    return pairs


def _valid_by_n(arm):
    return {r["N"]: r for r in arm["per_N"] if r.get("reps_valid")}


def analyse_bridge(primary, supp):
    """What the SHARED N says about comparing the two runs at all.

    A supplement is only spliceable into the primary curve if the N they BOTH
    measured reproduces. When the between-run offset at that N is larger than
    either run's own dispersion, run-to-run variation dominates and no cross-run
    absolute delta of that size is evidence of anything — including the splice the
    supplement was run to enable.
    """
    p_by, s_by = _valid_by_n(primary), _valid_by_n(supp)
    shared = sorted(set(p_by) & set(s_by))
    out = {
        "primary_arm": primary["arm"],
        "supplement_arm": supp["arm"],
        "S_physical_cores": primary["S_physical_cores"],
        "server_cpus": primary["server_cpus"],
        "primary_harness_commits": primary["harness_commits"],
        "supplement_harness_commits": supp["harness_commits"],
        "bridge_N": shared,
        "bridge_points": [],
    }
    if not shared:
        out.update({
            "splice_safe": False,
            "cross_run_uncertainty_pct": None,
            "verdict": "NO BRIDGE POINT: the two runs share no N, so nothing measures the "
                       "between-run offset and NO cross-run comparison may be made.",
        })
        return out

    for n in shared:
        pr, sr = p_by[n], s_by[n]
        offset = (sr["rows_per_s_median"] - pr["rows_per_s_median"]) / pr["rows_per_s_median"] * 100
        within = max(abs(pr["dispersion_pct_of_median"] or 0.0),
                     abs(sr["dispersion_pct_of_median"] or 0.0))
        out["bridge_points"].append({
            "N": n,
            "primary_rows_per_s_median": pr["rows_per_s_median"],
            "primary_dispersion_pct": pr["dispersion_pct_of_median"],
            "supplement_rows_per_s_median": sr["rows_per_s_median"],
            "supplement_dispersion_pct": sr["dispersion_pct_of_median"],
            "offset_pct_supplement_vs_primary": offset,
            "within_run_dispersion_pct_max": within,
            "offset_exceeds_within_run_dispersion": abs(offset) > within,
        })

    worst = max(out["bridge_points"], key=lambda b: abs(b["offset_pct_supplement_vs_primary"]))
    out["cross_run_uncertainty_pct"] = abs(worst["offset_pct_supplement_vs_primary"])
    out["cross_run_uncertainty_at_N"] = worst["N"]
    out["splice_safe"] = not any(b["offset_exceeds_within_run_dispersion"]
                                for b in out["bridge_points"])
    out["verdict"] = (
        "BRIDGE REPRODUCES: at every shared N the between-run offset is within that N's own "
        "dispersion, so a cross-run splice is empirically supported."
        if out["splice_safe"] else
        "BRIDGE DISAGREES: the between-run offset at N=%d is %+.2f%%, LARGER than the %.1f%% "
        "within-run dispersion there. Run-to-run variation therefore exceeds within-run "
        "dispersion: any cross-run absolute delta below ~%.2f%% is an ARTIFACT, and this "
        "width's formula verdict must come from a WITHIN-run pair." % (
            worst["N"], worst["offset_pct_supplement_vs_primary"],
            worst["within_run_dispersion_pct_max"], out["cross_run_uncertainty_pct"]))
    return out


def supplemented_formula(primary, supp, bridge):
    """This width's formula verdict, taken from WITHIN the supplement run.

    The primary ramp omitted the predicted N, so the primary arm could not evaluate
    the formula at all (`deviation_pct_of_measured_peak: None`). The supplement
    measured the predicted N AND the primary's apparent peak in the SAME run, which
    is the comparison the bridge licenses. The naive cross-run splice is computed
    too — and labelled REJECTED when the bridge showed the offset dominates it —
    so the number that would have been reported is visible next to the reason it
    was not.
    """
    f = supp.get("formula_evaluation") or {}
    pred = f.get("predicted_N")
    p_peak = (primary.get("peak") or {}).get("N")
    p_by, s_by = _valid_by_n(primary), _valid_by_n(supp)
    out = {
        "width_S": primary["S_physical_cores"],
        "P_hw_threads": f.get("P_hw_threads"),
        "predicted_N": pred,
        "primary_apparent_peak_N": p_peak,
        "primary_arm": primary["arm"],
        "supplement_arm": supp["arm"],
    }
    if pred is None or pred not in s_by or p_peak is None or p_peak not in s_by:
        out["resolved"] = False
        out["reason"] = (
            "the supplement measured %s; a WITHIN-run verdict needs BOTH the predicted N (%s) "
            "and the primary's apparent peak N (%s) in that same run." % (
                ",".join(str(n) for n in sorted(s_by)), pred, p_peak))
        return out

    s_pred, s_cmp = s_by[pred], s_by[p_peak]
    within = (s_pred["rows_per_s_median"] - s_cmp["rows_per_s_median"]) / s_cmp["rows_per_s_median"] * 100
    naive = (s_pred["rows_per_s_median"] - p_by[p_peak]["rows_per_s_median"]) \
        / p_by[p_peak]["rows_per_s_median"] * 100
    out.update({
        "resolved": True,
        "within_run": {
            "arm": supp["arm"],
            "predicted_N_rows_per_s_median": s_pred["rows_per_s_median"],
            "predicted_N_dispersion_pct": s_pred["dispersion_pct_of_median"],
            "comparator_N": p_peak,
            "comparator_rows_per_s_median": s_cmp["rows_per_s_median"],
            "comparator_dispersion_pct": s_cmp["dispersion_pct_of_median"],
            "predicted_vs_comparator_pct": within,
            "formula_wins": within > 0,
        },
        "naive_cross_run_splice": {
            "predicted_N_rows_per_s_median_from_supplement": s_pred["rows_per_s_median"],
            "comparator_rows_per_s_median_from_primary": p_by[p_peak]["rows_per_s_median"],
            "delta_pct": naive,
            "accepted": bool(bridge.get("splice_safe")),
            "why": ("the bridge reproduced, so the splice is supported"
                    if bridge.get("splice_safe") else
                    "REJECTED: |%+.2f%%| is below the %.2f%% between-run offset the bridge "
                    "measured, so it carries no information" % (
                        naive, bridge.get("cross_run_uncertainty_pct") or 0.0)),
        },
        "verdict": (
            "WITHIN-run at %s: predicted N=%d is %+.2f%% against N=%d, the value the coarse ramp "
            "had called this width's peak. The formula %s." % (
                supp["arm"], pred, within, p_peak,
                "BEATS that peak" if within > 0 else "is below that peak")),
    })
    return out


def ac5_block(arms, supplement_labels, comparator_N=16):
    """AC5 at the widest width in scope: the derived default vs the alternatives.

    Compared against DISPERSION rather than against a bare percentage: the claim is
    only that the derived default wins if the gain exceeds the run's own spread. The
    strongest available form is also reported — whether the two N's REP RANGES are
    disjoint, which no dispersion average can fake.
    """
    primaries = [a for a in arms
                 if a["arm"] not in supplement_labels and a["S_physical_cores"] is not None]
    if not primaries:
        return {"resolved": False, "reason": "no primary arm with a resolved width S"}
    widest = max(primaries, key=lambda a: a["S_physical_cores"])
    f = widest.get("formula_evaluation") or {}
    by_n = _valid_by_n(widest)
    pred = f.get("predicted_N")
    out = {
        "arm": widest["arm"],
        "S_physical_cores": widest["S_physical_cores"],
        "P_hw_threads": widest["server_hw_threads_P"],
        "derived_N": pred,
        "why_widest_in_scope": (
            "the client needs 2 exclusive PHYSICAL cores on the same box and sweep.sh refuses a "
            "server set overlapping the client's, so 6 of the 8 physical cores is the widest "
            "server this rig can measure. It is NOT 'the whole box'."),
    }
    if pred is None or pred not in by_n:
        out["resolved"] = False
        out["reason"] = "the derived N (%s) is not in this arm's ramp (%s)" % (
            pred, ",".join(str(n) for n in sorted(by_n)))
        return out

    def cmp_rec(n, role):
        if n not in by_n:
            return {"N": n, "role": role, "measured": False,
                    "reason": "N=%d is not in this arm's ramp" % n}
        d, c = by_n[pred], by_n[n]
        gain = (d["rows_per_s_median"] - c["rows_per_s_median"]) / c["rows_per_s_median"] * 100
        spread = max(abs(d["dispersion_pct_of_median"] or 0.0),
                     abs(c["dispersion_pct_of_median"] or 0.0))
        return {
            "N": n, "role": role, "measured": True,
            "rows_per_s_median": c["rows_per_s_median"],
            "rows_per_s_min": c["rows_per_s_min"], "rows_per_s_max": c["rows_per_s_max"],
            "dispersion_pct": c["dispersion_pct_of_median"],
            "derived_gain_pct": gain,
            "max_dispersion_of_the_pair_pct": spread,
            "gain_exceeds_dispersion": gain > spread,
            "rep_ranges_disjoint": d["rows_per_s_min"] > c["rows_per_s_max"],
        }

    d = by_n[pred]
    out.update({
        "resolved": True,
        "derived_point": {
            "N": pred,
            "rows_per_s_median": d["rows_per_s_median"],
            "rows_per_s_min": d["rows_per_s_min"], "rows_per_s_max": d["rows_per_s_max"],
            "dispersion_pct": d["dispersion_pct_of_median"],
            "latency_p50_ms_median": d["latency_p50_ms_median"],
            "server_cpu_util_of_pinned_set_median": d["server_cpu_util_of_pinned_set_median"],
            "reps_valid": d["reps_valid"],
        },
        "comparisons": [cmp_rec(comparator_N, "#3217's censored peak / the misidentified default"),
                        cmp_rec(DERIVED_CEILING, "the SHIPPED default")],
    })
    out["regression_free"] = all(c.get("derived_gain_pct", 0) > 0
                                 for c in out["comparisons"] if c["measured"])
    return out


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
    ap.add_argument("--corpus-sha-file", default=None,
                    help="shasum artifact naming the corpus Data.db digest (AC6); default is the "
                         "committed ../corpus/corpus-sha-staged.txt beside this script")
    args = ap.parse_args()

    here = os.path.dirname(os.path.abspath(__file__))
    repo = os.path.abspath(os.path.join(here, "..", "..", "..", ".."))
    corpus_sha_file = args.corpus_sha_file or os.path.join(
        here, "..", "corpus", "corpus-sha-staged.txt")
    corpus_sha_file = os.path.abspath(corpus_sha_file)
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

    sweeps, excluded_dirs = load_results(results_dir)
    for e in excluded_dirs:
        print("EXCLUDED input dir %s: %s" % (e["dir"], e["reason"]), file=sys.stderr)
    if not sweeps:
        print("ERROR: no COMPLETE <arm>/points.jsonl under %s (%d candidate dir(s) refused, "
              "listed above) — nothing to analyse" % (results_dir, len(excluded_dirs)),
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

    pairs = pair_supplements(arms)
    bridges = [analyse_bridge(p, s) for p, s in pairs]
    supp_labels = [s["arm"] for _p, s in pairs]

    out = {
        "schema": SCHEMA,
        "results_dir": os.path.abspath(results_dir),
        "formula_under_test": "clamp(%d x P, %d, %d)" % (
            SCANS_PER_HW_THREAD, DERIVED_MIN, DERIVED_CEILING),
        "arms": arms,
        # Published, not merely logged: which candidate directories this analysis
        # REFUSED, and why. Named in the artifact so a reader can tell "the sweep wrote
        # 7 directories and 6 are curves" from "the loader silently saw 6".
        "input_dirs_accepted": sorted(sweeps),
        "input_dirs_excluded": excluded_dirs,
        "arms_with_unresolved_width": [a["arm"] for a in arms if a["S_physical_cores"] is None],
        "supplement_arms": supp_labels,
        "bridges": bridges,
        "supplemented_formula_verdicts": [
            supplemented_formula(p, s, br) for (p, s), br in zip(pairs, bridges)],
        "cross_run_uncertainty_pct_max": max(
            [b["cross_run_uncertainty_pct"] for b in bridges
             if b["cross_run_uncertainty_pct"] is not None], default=None),
        "ac5": ac5_block(arms, set(supp_labels)),
        "corpus_identity": corpus_identity(arms, corpus_sha_file),
        "admission_ceiling": admission_ceiling(arms),
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
    if args.smoke:
        # #3217's committed results predate BOTH per-arm checks below, so under --smoke
        # they describe that input, not this round. Reported, never silently skipped.
        print("SMOKE (advisory, describes #3217's input): corpus identity %s | admission "
              "ceiling %s" % (out["corpus_identity"]["state"],
                              "PASS" if out["admission_ceiling"]["ok"] else "FAIL"),
              file=sys.stderr)
        return 0

    rc = 0
    if not out["corpus_identity"]["ok"]:
        # Fail closed on BOTH a contradiction and an absent measurement. If the arms did
        # not all record the same bytes, the peak table may compare curves from different
        # corpora; if no digest was recorded, nobody knows either way, and "nobody knows"
        # is not the same claim as "they agree".
        print("CORPUS IDENTITY: %s" % out["corpus_identity"]["verdict"], file=sys.stderr)
        rc = 1
    if not out["admission_ceiling"]["ok"]:
        # Fail closed: a ceiling below an arm's largest N means those points measured the
        # admission gate. Nothing else in this analysis can see that — a throttled point
        # reports 0 rejections, because over-ceiling requests wait and then succeed.
        print("ADMISSION CEILING: %s" % out["admission_ceiling"]["verdict"], file=sys.stderr)
        rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main())
