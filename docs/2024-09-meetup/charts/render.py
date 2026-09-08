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

RES = Path("/Users/pmcfadin/projects/cqlite-wt/issue-4137-talk-results/docs/2024-09-meetup/results")
OUT = Path("/Users/pmcfadin/projects/cqlite-wt/issue-4137-talk-results/docs/2024-09-meetup/charts")
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


def finish(fig, ax, title, ylabel, png):
    ax.set_title(title, fontsize=12, fontweight="bold", pad=14)
    ax.set_ylabel(ylabel, fontsize=10)
    ax.grid(axis="y", **GRID)
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    fig.text(0.5, 0.015, SUB, ha="center", fontsize=6.5, color="#555")
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

fig, ax = plt.subplots(figsize=(7.2, 4.4))
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

fig, ax = plt.subplots(figsize=(8.4, 4.6))
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

fig, ax = plt.subplots(figsize=(7.2, 4.4))
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
fig, ax = plt.subplots(figsize=(7.2, 4.2))
ax.plot(xs, ys, marker="o", color=C_CQL, lw=2, ms=7)
for x, y in zip(xs, ys):
    ax.annotate(f"{y:.1f}s", (x, y), textcoords="offset points", xytext=(6, -10), fontsize=8)
ax.set_xscale("log")
ax.set_yscale("log")
ax.set_xlabel("rows actually processed (log)", fontsize=10)
finish(fig, ax,
       "cqlite scan scales linearly, no cliff\nSELECT * ... LIMIT n through the cqlite catalog",
       "wall time (s, log)", "d2-scan-limit-ladder.png")
