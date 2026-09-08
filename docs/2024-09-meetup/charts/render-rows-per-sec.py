#!/usr/bin/env python3
"""Sustained rows/sec served by CQLite Flight, from the rescued VictoriaMetrics series.

Deliberately WIDE with single-line titles: the earlier chart set used 7-9in figures with two-line
bold titles and several were clipped at the figure edges.
"""
import statistics as st
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime as dt

RES = Path("/Users/pmcfadin/projects/cqlite-wt/issue-4137-talk-results/docs/2024-09-meetup/results")
MET = RES / "metrics"
OUT = Path("/Users/pmcfadin/projects/cqlite-wt/issue-4137-talk-results/docs/2024-09-meetup/charts")

C_CQL = "#3D6FB4"
C_SHADE = "#3D6FB4"

# The sustained window: D1's window_Q, when the cqlite analytic looped back-to-back for 8 min
# while Cassandra concurrently served ~6,400 OLTP reads/s.
WIN_START, WIN_END = 1788835365.0, 1788835365.0 + 34 * 15


def series(path, label=None):
    out = []
    for line in open(path):
        if line.startswith("unix_ts") or line.startswith("#"):
            continue
        p = line.rstrip("\n").split("\t")
        if len(p) < 3:
            continue
        if label and p[1] != label:
            continue
        try:
            out.append((float(p[0]), float(p[2])))
        except ValueError:
            continue
    out.sort()
    return out


agg = series(MET / "rows_served_per_sec.tsv")
win = [(t, v) for t, v in agg if WIN_START <= t <= WIN_END]
med = st.median([v for _, v in win])
pk = max(v for _, v in agg)

# ---------------------------------------------------------------- chart 1: sustained window
fig, ax = plt.subplots(figsize=(13.5, 5.6))
xs = [dt.datetime.fromtimestamp(t, dt.UTC) for t, _ in win]
ys = [v / 1e6 for _, v in win]
ax.plot(xs, ys, color=C_CQL, lw=2.6)
ax.fill_between(xs, 0, ys, color=C_SHADE, alpha=0.16)
ax.axhline(med / 1e6, color="#A02020", ls="--", lw=1.6)
ax.text(xs[1], med / 1e6 + 0.055, f"sustained median  {med:,.0f} rows/s",
        fontsize=13, fontweight="bold", color="#A02020")
ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=dt.UTC))
ax.set_ylim(0, max(ys) * 1.22)
ax.set_ylabel("rows/sec served (millions)", fontsize=11)
ax.set_xlabel("time (UTC) — 8.5 min continuous", fontsize=10)
ax.set_title("CQLite Flight: sustained rows/sec serving a looped full-table analytic",
             fontsize=14, fontweight="bold", pad=12)
ax.grid(axis="y", alpha=0.25, lw=0.6)
ax.set_axisbelow(True)
for s in ("top", "right"):
    ax.spines[s].set_visible(False)
fig.text(0.5, 0.018,
         "cqlite_rpc_rows_total rate[1m] | 22,339,536-row keyvalue table, LZ4 | 3x i4i.2xlarge db + 2x m6i.2xlarge app | "
         "cqlite-flight talk017 @ sha256:847c93ba | Cassandra 5.0.9 concurrently serving ~6,400 OLTP reads/s",
         ha="center", fontsize=7.5, color="#555")
fig.tight_layout(rect=(0, 0.055, 1, 1))
fig.savefig(OUT / "rows-per-sec-sustained.png", dpi=170)
plt.close(fig)
print(f"wrote rows-per-sec-sustained.png  (median {med:,.0f}, peak in window {max(v for _,v in win):,.0f})")

# ------------------------------------------------------- chart 2: per-pod, shows the fan-out gap
fig, ax = plt.subplots(figsize=(13.5, 5.2))
pods = sorted({l.split("\t")[1] for l in open(MET / "rows_served_per_pod.tsv")
               if not l.startswith("unix_ts") and len(l.split("\t")) >= 3})
for i, p in enumerate(pods):
    s = [(t, v) for t, v in series(MET / "rows_served_per_pod.tsv", p) if WIN_START <= t <= WIN_END]
    if not s:
        continue
    tot = st.median([v for _, v in s])
    ax.plot([dt.datetime.fromtimestamp(t, dt.UTC) for t, _ in s],
            [v / 1e6 for _, v in s], lw=2.2, label=f"{p}  (median {tot:,.0f} rows/s)")
ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=dt.UTC))
ax.set_ylabel("rows/sec served (millions)", fontsize=11)
ax.set_xlabel("time (UTC)", fontsize=10)
ax.set_title("Same window, per Flight pod: one pod carries it, two idle (issue #4175)",
             fontsize=14, fontweight="bold", pad=12)
ax.legend(frameon=False, fontsize=10)
ax.grid(axis="y", alpha=0.25, lw=0.6)
ax.set_axisbelow(True)
for s_ in ("top", "right"):
    ax.spines[s_].set_visible(False)
fig.text(0.5, 0.018,
         "So the sustained figure above is a SINGLE-POD result. Fanning splits across all three "
         "replicas should raise it; that is unmeasured. See issue #4175.",
         ha="center", fontsize=7.5, color="#555")
fig.tight_layout(rect=(0, 0.055, 1, 1))
fig.savefig(OUT / "rows-per-sec-per-pod.png", dpi=170)
plt.close(fig)
print("wrote rows-per-sec-per-pod.png")
