#!/usr/bin/env python3
"""Render the #4137 talk charts from the committed CSVs.

Every title carries the three things #4137 requires: measured row count, node shape, and the
image digest. Nothing here recomputes a number from scratch -- the values are read from the
committed CSV series so the chart and the raw data cannot drift apart.
"""
import csv
import statistics as st
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RES = Path(__file__).resolve().parents[2] / "docs/2024-09-meetup/results"
OUT = Path(__file__).resolve().parents[2] / "docs/2024-09-meetup/charts"
OUT.mkdir(parents=True, exist_ok=True)

DIGEST = "sha256:847c93ba"          # short form; full digest is in REPORT.md
SHAPE = "3x i4i.2xlarge db + 2x m6i.2xlarge app"
ROWS = "22,339,536 rows"
SUB = f"{ROWS} | {SHAPE} | cqlite-flight talk017 @ {DIGEST} | Cassandra 5.0.9 | Trino 481"

# Brand-neutral, colourblind-safe pair. cassandra = warm/grey, cqlite = blue.
C_CASS = "#B4654A"
C_CQL = "#3D6FB4"
GRID = dict(alpha=0.25, linewidth=0.6)


def data_rows(path):
    """Yield CSV rows, skipping the '>' provenance/comment lines."""
    with open(path) as fh:
        lines = [l for l in fh if not l.lstrip().startswith(">")]
    return list(csv.DictReader(lines))


def finish(fig, ax, title, ylabel, png, rows=ROWS):
    """`rows` is per-chart on purpose: the footer must name the row count of the table THIS chart
    measures. Defaulting every chart to keyvalue's 22,339,536 put the wrong count under the D3
    chart, which is sensor_data (23,703,992). #4137 requires the measured count for that table."""
    ax.set_title(title, fontsize=11.5, fontweight="bold", pad=14, wrap=True)
    ax.set_ylabel(ylabel, fontsize=10)
    ax.grid(axis="y", **GRID)
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    sub = f"{rows} | {SHAPE} | cqlite-flight talk017 @ {DIGEST} | Cassandra 5.0.9 | Trino 481"
    fig.text(0.5, 0.015, sub, ha="center", fontsize=6.5, color="#555")
    fig.tight_layout(rect=(0, 0.05, 1, 1))
    fig.savefig(OUT / png, dpi=170)
    plt.close(fig)
    print("wrote", png)


# ---------------------------------------------------------------- D2 warm scan
rows = data_rows(RES / "d2-scan-warm.csv")
med = {}
for cat in ("cassandra", "cqlite"):
    v = [int(r["elapsed_ms"]) for r in rows if r["catalog"] == cat]
    med[cat] = (st.median(v), min(v), max(v), len(v))

fig, ax = plt.subplots(figsize=(12.5, 5.0))
cats = ["cassandra", "cqlite"]
vals = [med[c][0] / 1000 for c in cats]
errs = [[(med[c][0] - med[c][1]) / 1000 for c in cats],
        [(med[c][2] - med[c][0]) / 1000 for c in cats]]
bars = ax.bar(["cassandra\n(stock CQL path)", "cqlite\n(Arrow Flight)"], vals,
              color=[C_CASS, C_CQL], width=0.55,
              yerr=errs, capsize=6, error_kw=dict(ecolor="#444", lw=1.2))
for b, c in zip(bars, cats):
    ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.45,
            f"{med[c][0]/1000:.1f} s", ha="center", fontweight="bold", fontsize=12)
ratio = med["cassandra"][0] / med["cqlite"][0]
ax.text(0.5, 0.88, f"{ratio:.2f}x faster", transform=ax.transAxes, ha="center",
        fontsize=15, fontweight="bold", color=C_CQL)
ax.set_ylim(0, max(vals) * 1.35)
finish(fig, ax,
       "Same SQL, two catalogs: full-table scan\nSELECT sum(length(value)) -- medians of 3, identical rows+bytes both sides",
       "wall time (s), lower is better", "d2-scan-warm.png")

# ------------------------------------------------------------- D1 p99 isolation
PH = ["baseline", "window_C", "quiet_1", "window_Q", "quiet_2"]
LBL = {"baseline": "baseline", "window_C": "analytic via\ncassandra",
       "quiet_1": "quiet", "window_Q": "analytic via\ncqlite", "quiet_2": "quiet"}
# The raw counter block itself contains commas, so csv.DictReader mis-splits it. Parse positionally:
# field 0 = timestamp, field 1 = phase, remainder = the stress client's counters, of which the
# 5th non-empty/non-pipe value is read p99 (write count, write p99, write rate, read count, read p99).
p99 = {}
_acc = {}
with open(RES / "d1-isolation.csv") as fh:
    for line in fh:
        if line.lstrip().startswith(">") or line.startswith("timestamp_utc"):
            continue
        parts = line.rstrip("\n").split(",")
        if len(parts) < 8:
            continue
        ph = parts[1]
        f = [x for x in parts[2:] if x not in ("", "|")]
        if len(f) < 5:
            continue
        try:
            _acc.setdefault(ph, []).append(float(f[4]))
        except ValueError:
            continue
for ph, v in _acc.items():
    if v:
        p99[ph] = st.median(v)
missing = [p for p in PH if p not in p99]
if missing:
    raise SystemExit(f"D1 chart refuses to render: phases absent from the CSV: {missing}")

fig, ax = plt.subplots(figsize=(13.0, 5.2))
colors = {"baseline": "#8A8A8A", "quiet_1": "#8A8A8A", "quiet_2": "#8A8A8A",
          "window_C": C_CASS, "window_Q": C_CQL}
ks = [k for k in PH if k in p99]
# Positional x, NOT categorical: quiet_1 and quiet_2 share the label "quiet", and passing duplicate
# category strings to ax.bar silently collapses them into ONE bar -- which dropped quiet_2, the bar
# that proves the cluster recovers after the cqlite window too.
xpos = list(range(len(ks)))
bars = ax.bar(xpos, [p99[k] for k in ks], color=[colors[k] for k in ks], width=0.6)
ax.set_xticks(xpos)
ax.set_xticklabels([LBL[k] for k in ks])
base = p99["baseline"]
span = max(p99.values())
for b, k in zip(bars, ks):
    ax.text(b.get_x() + b.get_width() / 2, b.get_height() + span * 0.045,
            f"{p99[k]:.2f} ms\n{p99[k]/base:.2f}x", ha="center", fontsize=10,
            linespacing=1.5,
            fontweight="bold" if k.startswith("window") else "normal")
ax.axhline(base, color="#444", ls="--", lw=1, zorder=0)
ax.set_ylim(0, max(p99.values()) * 1.3)
finish(fig, ax,
       "Your app doesn't notice: client read p99 while an analytic runs\n"
       "fixed 8k ops/s 80%-read load; throughput held ~6,400 reads/s in EVERY phase",
       "Cassandra client read p99 (ms)", "d1-isolation.png")

# ------------------------------------------------------------------ D6 cold start
rows = data_rows(RES / "d6-coldstart.csv")
g = {}
for m in ("limit5", "sum_scan"):
    v = [int(r["elapsed_ms"]) for r in rows if r["metric"] == m]
    if v:
        g[m] = st.median(v)
warm = st.median([int(r["elapsed_ms"]) for r in data_rows(RES / "d2-scan-warm.csv")
                  if r["catalog"] == "cqlite"])

fig, ax = plt.subplots(figsize=(12.5, 5.0))
labels = ["first LIMIT 5\nafter restart", "first full scan\nafter restart", "warm full scan\n(reference)"]
vals = [g["limit5"] / 1000, g["sum_scan"] / 1000, warm / 1000]
bars = ax.bar(labels, vals, color=[C_CQL, C_CQL, "#8A8A8A"], width=0.55)
for b, v in zip(bars, vals):
    ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.3,
            f"{v:.2f} s" if v < 1 else f"{v:.1f} s", ha="center", fontweight="bold", fontsize=11)
ax.set_ylim(0, max(vals) * 1.3)
finish(fig, ax,
       "Cold start: first query after a Flight restart (medians of 3)\n"
       "NOT drop_caches-cold -- the page-cache drop was ineffective, see d6-coldstart.csv",
       "wall time (s)", "d6-coldstart.png")

# ------------------------------------------------------- D2 LIMIT ladder (scaling)
rows = data_rows(RES / "d2-scan-limit-ladder.csv")
xs = [int(r["processed_rows"]) for r in rows]
ys = [int(r["elapsed_ms"]) / 1000 for r in rows]
fig, ax = plt.subplots(figsize=(12.5, 4.8))
ax.plot(xs, ys, marker="o", color=C_CQL, lw=2, ms=7)
for x, y in zip(xs, ys):
    ax.annotate(f"{y:.1f}s", (x, y), textcoords="offset points", xytext=(6, -10), fontsize=8)
ax.set_xscale("log")
ax.set_yscale("log")
ax.set_xlabel("rows actually processed (log)", fontsize=10)
finish(fig, ax,
       "cqlite scan scales linearly, no cliff\nSELECT * ... LIMIT n through the cqlite catalog",
       "wall time (s, log)", "d2-scan-limit-ladder.png")

# ---------------------------------------------------------------- D3 two catalogs
import re as _re
d3 = {}
for line in open(RES / "d3-timeseries.csv"):
    m = _re.match(r">\s*(q\d_\w+),(cassandra|cqlite),(\d*),(\d),(.*)", line.strip())
    if m:
        q, cat, ms, n, verdict = m.groups()
        d3[(q, cat)] = (int(ms) if ms else None, verdict.strip())

qs = ["q1_cross_partition_groupby", "q2_approx_percentile", "q3_window_function", "q4_join_two_tables"]
NICE = {"q1_cross_partition_groupby": "cross-partition\nGROUP BY",
        "q2_approx_percentile": "approx_percentile\nper sensor",
        "q3_window_function": "window function\n(row_number)",
        "q4_join_two_tables": "JOIN two\nCassandra tables"}
fig, ax = plt.subplots(figsize=(13.5, 5.4))
x = list(range(len(qs)))
w = 0.36
cass = [d3.get((q, "cassandra"), (None, ""))[0] for q in qs]
cql = [d3.get((q, "cqlite"), (None, ""))[0] for q in qs]
# A failed query has no bar; it gets an explicit annotation instead of a zero, so absence of a bar
# cannot be misread as "instant".
ax.bar([i - w / 2 for i in x], [(v or 0) / 1000 for v in cass], w, color=C_CASS, label="cassandra")
ax.bar([i + w / 2 for i in x], [(v or 0) / 1000 for v in cql], w, color=C_CQL, label="cqlite")
for i, q in enumerate(qs):
    for v, off, col in ((cass[i], -w / 2, C_CASS), (cql[i], w / 2, C_CQL)):
        if v is None:
            ax.text(i + off, 0.6, "FAILED\nout of\nmemory", ha="center", va="bottom",
                    fontsize=8, fontweight="bold", color="#A02020")
        else:
            ax.text(i + off, v / 1000 + 0.3, f"{v/1000:.1f}s", ha="center", fontsize=9)
    if cass[i] and cql[i]:
        r = cass[i] / cql[i]
        ax.text(i, max(cass[i], cql[i]) / 1000 + 1.8, f"{r:.2f}x",
                ha="center", fontsize=10, fontweight="bold",
                color=C_CQL if r > 1 else "#A02020")
ax.set_xticks(x)
ax.set_xticklabels([NICE[q] for q in qs], fontsize=9)
ax.legend(frameon=False, fontsize=9, loc="upper left")
ax.set_ylim(0, 17)
finish(fig, ax,
       "Analytics you can't write in CQL -- and where cqlite is SLOWER\n"
       "on sensor_data cqlite runs 1.3-1.4x slower; only the JOIN needs it",
       "wall time (s), medians of 3", "d3-timeseries.png",
       rows="23,703,992 rows (sensor_data, ~9 rows/partition)")

# ------------------------------------------------------------------- D4 ladder
rows = data_rows(RES / "d4-ladder.csv")
th = [int(r["threads"]) for r in rows]
qps = [float(r["qps"]) for r in rows]
p50 = [int(r["p50_ms"]) for r in rows]
p99 = [int(r["p99_ms"]) for r in rows]
fig, ax = plt.subplots(figsize=(13.0, 5.2))
ax.plot(th, qps, marker="o", color=C_CQL, lw=2.2, ms=7, label="qps")
ax.set_xlabel("concurrent clients", fontsize=10)
ax.set_ylabel("queries/sec", fontsize=10, color=C_CQL)
ax.tick_params(axis="y", labelcolor=C_CQL)
ax2 = ax.twinx()
ax2.plot(th, p99, marker="s", color=C_CASS, lw=2.2, ms=6, ls="--", label="p99 ms")
ax2.plot(th, p50, marker="^", color="#8A8A8A", lw=1.6, ms=6, ls=":", label="p50 ms")
ax2.set_ylabel("latency (ms)", fontsize=10, color=C_CASS)
ax2.tick_params(axis="y", labelcolor=C_CASS)
ax2.spines["top"].set_visible(False)
for xx, yy in zip(th, qps):
    ax.annotate(f"{yy:.1f}", (xx, yy), textcoords="offset points", xytext=(0, 8), fontsize=8, color=C_CQL)
h1, l1 = ax.get_legend_handles_labels()
h2, l2 = ax2.get_legend_handles_labels()
ax.legend(h1 + h2, l1 + l2, frameon=False, fontsize=9, loc="upper left")
ax.set_xticks(th)
finish(fig, ax,
       "Dashboards at scale: 0 errors and 0 restarts through 80 clients\n"
       "degrades in LATENCY, never by failing; Flight memory flat idle->80 clients",
       "queries/sec", "d4-ladder.png",
       rows="mixed bounded queries over keyvalue + sensor_data")

# ---------------------------------------------------------------- D5 freshness
d5 = {}
for line in open(RES / "d5-freshness.csv"):
    m = _re.match(r">\s*(stock_flush60|flush_period_10s),(\d+),(-?\d+),(-?\d+),(\d+),(\d+),", line.strip())
    if m:
        lab, n, medg, maxg, z, growth = m.groups()
        d5[lab] = dict(n=int(n), med=int(medg), mx=int(maxg), zero=int(z), growth=int(growth))
fig, ax = plt.subplots(figsize=(12.0, 5.0))
labs = ["stock_flush60", "flush_period_10s"]
NICE5 = {"stock_flush60": "forced flush\nevery 60s", "flush_period_10s": "memtable_flush_\nperiod = 10s"}
mx = [d5[l]["mx"] for l in labs]
bars = ax.bar([NICE5[l] for l in labs], mx, color=C_CQL, width=0.45)
for b, l in zip(bars, labs):
    dd = d5[l]
    ax.text(b.get_x() + b.get_width() / 2, 0.06,
            f"max gap\n{dd['mx']} rows\n\n(n={dd['n']} probes,\npartition grew\n+{dd['growth']} rows)",
            ha="center", va="bottom", fontsize=9)
ax.set_ylim(-1.2, 1.2)
ax.axhline(0, color="#444", lw=1)
finish(fig, ax,
       "Fresh within a flush: rows visible to Cassandra but NOT to CQLite\n"
       "0.17 freshness == flush cadence (#1807 is 0.18). At 2k writes/s memtable pressure flushed often enough that lag never appeared.",
       "staleness (rows), max over probes", "d5-freshness.png",
       rows="23,703,992 rows (sensor_data)")
