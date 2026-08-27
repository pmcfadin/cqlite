#!/usr/bin/env python3
"""#3299 phase 2 — the CLIENT-BOUND falsification, and the same-corpus ratio.

Two jobs, both deliberately mechanical so the verdict is not a judgement call
made after seeing the numbers.

## 1. `--a <label> --b <label>` — is the `do_get` point CLIENT-BOUND?

Two runs with an IDENTICAL server set and the client halved. If halving the
client changes the aggregate by more than the points' own spread, the loadgen
was a limiting factor and **the number measures the client, not `do_get`**.

Why this matters more than it looks: a client-bound figure **understates
`do_get`**, which **overstates** the bare-scan-vs-`do_get` gap, which flatters
#3288 — the exact lever this issue exists to calibrate. It is this issue's
signature failure mode, so the objection is settled by measurement and the
result is published **either way, including if it refutes the objection**.

The threshold is the two points' own measured spread, not a fixed percentage —
the same rule the bare-scan bracketing uses, for the same reason (spread is
regime-dependent, so one global number is wrong in both directions).

## 2. `--ratio` — bare scan vs `do_get`, ON ONE CORPUS

The thing R1 promised and no existing figure provides: both arms on Corpus B, in
one session, at S=1. #3217's `do_get` is Corpus A (LZ4, 196.09 B/row) and this
target is Corpus B (uncompressed, 693.69 B/row); dividing across them is
forbidden, so it is not done here.
"""

import argparse
import json
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import guards  # noqa: E402  — ONE guard implementation, shared with the bare-scan arm


def load(results, label):
    """Every validated rep for one label, re-asserting the guards at read time."""
    out = {}
    man = os.path.join(results, "manifest.jsonl")
    if not os.path.exists(man):
        sys.exit(f"FATAL: {man} absent — nothing measured")
    for line in open(man):
        if not line.strip():
            continue
        rec = json.loads(line)
        if rec["label"] != label:
            continue
        rd = rec["rundir"]
        # Re-assert rather than trust that the run validated them: an aggregation
        # that CAN be pointed at an unvalidated tree eventually will be.
        counters = guards.parse_perf_csv(os.path.join(rd, "perf.csv"))
        for ev in ("instructions", "cycles", "task-clock"):
            val, pct = counters[ev]
            if "<not" in val:
                guards.fail("PERF_EVENT_NOT_COUNTED", f"{rd}: {ev} reads {val!r}")
            if float(pct) < 100.0:
                guards.fail("PERF_MULTIPLEXED", f"{rd}: {ev} at {pct}% — a scaled estimate")
        step = [json.loads(x) for x in open(os.path.join(rd, "step.jsonl")) if x.strip()]
        step = [x for x in step if "rows_total" in x][0]
        rows = int(step["rows_total"])
        if rows <= 0:
            guards.fail("FLIGHT_ZERO_ROWS", f"{rd}: {rows} rows")
        # task-clock is elapsed x nCPUs under CPU-wide counting, so it yields the
        # counted window without trusting a wall clock the loadgen also reported.
        ncpu = len(rec["server_cpus"].split(","))
        window_s = float(counters["task-clock"][0]) / 1e9 / ncpu
        out.setdefault(rec["n"], []).append(
            {
                "rows": rows,
                "window_s": window_s,
                "rows_per_s": rows / window_s,
                "cycles_per_row": float(counters["cycles"][0]) / rows,
                "instructions_per_row": float(counters["instructions"][0]) / rows,
                "server_cores": rec["server_cores"],
                "client_cores": rec["client_cores"],
            }
        )
    if not out:
        sys.exit(f"FATAL: no reps for label {label!r}")
    return out


def summarise(reps):
    r = [x["rows_per_s"] for x in reps]
    m = statistics.median(r)
    return m, (0.0 if m == 0 else (max(r) - min(r)) / m)


def cmd_falsify(args):
    a, b = load(args.results, args.a), load(args.results, args.b)
    shared = sorted(set(a) & set(b))
    if not shared:
        sys.exit(f"FATAL: {args.a} and {args.b} share no N value; the comparison needs the same N")
    print("## Client-bound falsification\n")
    print("Identical server set; client halved. Threshold is the two points' own spread.\n")
    print("| N | client cores (A / B) | A rows/s | B rows/s | delta | threshold | verdict |")
    print("|--:|---|--:|--:|--:|--:|---|")
    any_bound = False
    for n in shared:
        ma, sa = summarise(a[n])
        mb, sb = summarise(b[n])
        delta = (mb - ma) / ma
        thr = max(sa, sb)
        bound = abs(delta) > thr
        any_bound = any_bound or bound
        print(f"| {n} | {a[n][0]['client_cores']} / {b[n][0]['client_cores']} | {ma:,.0f} | {mb:,.0f} | "
              f"{delta:+.2%} | ±{thr:.2%} | **{'CLIENT-BOUND' if bound else 'not client-bound'}** |")
    print()
    if any_bound:
        print("**VERDICT: CLIENT-BOUND.** Halving the client moved the aggregate by more than "
              "the points' own spread, so the loadgen was a limiting factor and this point does "
              "not measure `do_get`. **The S=6 `do_get` figure is VOID** and is not published. "
              "The objection is confirmed by measurement rather than by argument.\n")
    else:
        print("**VERDICT: NOT client-bound.** Halving the client did not move the aggregate "
              "beyond the points' own spread, so the client was not a limiting factor and that "
              "objection is **FALSIFIED** — published as such, against the expectation that "
              "raised it. What remains is the machine-state asymmetry alone (bare-scan S=6 ran "
              "with 2 cores IDLE; this runs them BUSY), which is disclosable rather than "
              "disqualifying.\n")
    return 0


def cmd_ratio(args):
    dg = load(args.results, args.label)
    n_peak = max(dg, key=lambda n: statistics.median([x["rows_per_s"] for x in dg[n]]))
    m, sp = summarise(dg[n_peak])
    bare = args.bare_scan_rows_per_s
    print("## Same-corpus ratio — bare scan vs `do_get`, both on Corpus B, one session\n")
    print("| arm | rows/s | basis |")
    print("|---|--:|---|")
    print(f"| bare scan, S=1 best-N | {bare:,.0f} | {bare * 693.69 / 1e6:,.1f} MB/s logical/uncompressed |")
    print(f"| `do_get`, S=1 best-N={n_peak} | {m:,.0f} | {m * 693.69 / 1e6:,.1f} MB/s logical/uncompressed |")
    print(f"\n**`do_get` / bare scan = {m / bare:.3f}** (spread at the `do_get` point: {sp:.2%}). "
          f"The bar is ~1.3x, i.e. `do_get` >= bare/1.3 = {1/1.3:.3f}.\n")
    print("This is the **same-corpus** ratio R1 promised, which no prior figure provides: "
          "#3217's `do_get` is Corpus A (LZ4, 196.09 B/row) against this target's Corpus B "
          "(uncompressed, 693.69 B/row), and those are not divided into each other anywhere.\n")
    print("**Caveat, disclosed:** the bare-scan S=1 point ran with 7 physical cores idle; this "
          "`do_get` S=1 point loads 5 of 8 (1 server + 4 client). Not identical machine states — "
          "a smaller version of the asymmetry in report section 9.2, and far smaller than S=6's.\n")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", required=True)
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("falsify", help="client-bound falsification between two labels")
    f.add_argument("--a", required=True); f.add_argument("--b", required=True)
    f.set_defaults(fn=cmd_falsify)
    r = sub.add_parser("ratio", help="same-corpus bare-scan vs do_get ratio")
    r.add_argument("--label", required=True)
    r.add_argument("--bare-scan-rows-per-s", type=float, required=True)
    r.set_defaults(fn=cmd_ratio)
    args = ap.parse_args()
    sys.exit(args.fn(args))


if __name__ == "__main__":
    main()
