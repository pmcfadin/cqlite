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

RES = Path(__file__).resolve().parents[2] / "docs/2024-09-meetup/results"
MET = RES / "metrics"
OUT = Path(__file__).resolve().parents[2] / "docs/2024-09-meetup/charts"
OUT.mkdir(parents=True, exist_ok=True)

C_CQL = "#3D6FB4"
C_SHADE = "#3D6FB4"


def phase_bounds(path, phase):
    """First/last timestamp (epoch, UTC) of the named phase in a d1-isolation.csv-shaped file
    (`> `-prefixed provenance comments, a `timestamp_utc,phase,...` header, then
    `2026-09-08T02:41:41Z,<phase>,...` rows). Fails LOUDLY if the phase has no rows -- silently
    falling back to a different window would produce a wrong chart with nothing on screen to
    say so."""
    stamps = []
    for line in open(path):
        if line.lstrip().startswith(">") or line.startswith("timestamp_utc"):
            continue
        parts = line.rstrip("\n").split(",")
        if len(parts) < 2 or parts[1] != phase:
            continue
        try:
            t = dt.datetime.strptime(parts[0], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.UTC)
        except ValueError:
            continue
        stamps.append(t.timestamp())
    if not stamps:
        raise SystemExit(f"no '{phase}' rows found in {path} -- refusing to silently pick another window")
    return min(stamps), max(stamps)


# The recorded analytic phase: D1's window_Q, when the cqlite analytic looped back-to-back
# while Cassandra concurrently served ~6,400 OLTP reads/s. Bounds are the phase's own first/last
# timestamps in d1-isolation.csv, never a hardcoded epoch or a hand-picked stable subsection --
# the previous hardcoded WIN_START/WIN_END started 64s late and ran 101s into quiet_2.
WIN_START, WIN_END = phase_bounds(RES / "d1-isolation.csv", "window_Q")
WIN_DUR_MIN = (WIN_END - WIN_START) / 60


def series(path, label=None):
    out = []
    for line in open(path):
        if line.startswith("unix_ts") or line.startswith("#"):
            continue
        p = line.rstrip("\n").split(",")
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


agg = series(MET / "rows_served_per_sec.csv")
# Intentionally INCLUSIVE of the phase's ramp-up samples (including any that read 0 before the
# looped analytic's throughput actually starts) -- this is the recorded phase window, not a
# hand-trimmed "steady state" one.
win = [(t, v) for t, v in agg if WIN_START <= t <= WIN_END]
if not win:
    raise SystemExit(
        f"no {MET / 'rows_served_per_sec.csv'} samples fall inside window_Q "
        f"({WIN_START:.0f}..{WIN_END:.0f}) -- refusing to silently pick another window"
    )
med = st.median([v for _, v in win])
pk = max(v for _, v in agg)

# ---------------------------------------------------------------- chart 1: sustained window
fig, ax = plt.subplots(figsize=(13.5, 5.6))
xs = [dt.datetime.fromtimestamp(t, dt.UTC) for t, _ in win]
ys = [v / 1e6 for _, v in win]
ax.plot(xs, ys, color=C_CQL, lw=2.6)
ax.fill_between(xs, 0, ys, color=C_SHADE, alpha=0.16)
ax.axhline(med / 1e6, color="#A02020", ls="--", lw=1.6)
ax.text(xs[1], med / 1e6 + 0.055, f"window_Q phase median  {med:,.0f} rows/s",
        fontsize=13, fontweight="bold", color="#A02020")
ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=dt.UTC))
ax.set_ylim(0, max(ys) * 1.22)
ax.set_ylabel("rows/sec served (millions)", fontsize=11)
ax.set_xlabel(f"time (UTC) — {WIN_DUR_MIN:.1f} min recorded window_Q phase", fontsize=10)
ax.set_title("CQLite Flight: rows/sec during the recorded window_Q analytic phase (looped full-table scan)",
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
print(f"wrote rows-per-sec-sustained.png  (window_Q phase {WIN_END - WIN_START:.0f}s, "
      f"n={len(win)}, median {med:,.0f}, peak in window {max(v for _,v in win):,.0f})")

# ------------------------------------------------------- chart 2: every exported observation
# rows_served_per_pod.csv repeats the SAME series label ("cqlite-flight") for every concurrent
# reading at a timestamp -- there ARE multiple distinct readings per timestamp, but the export
# lost whatever identity (pod, instance, ...) distinguished them.
#
# That loss happened in HOW this CSV was written, not in the PromQL: `rate(cqlite_rpc_rows_total
# [1m])` has no `by()` and no `sum()`, so the underlying query would normally return one result
# vector PER label set, preserving identity. Do not repeat the (false) explanation that a missing
# `by(pod)` clause is why the data reads as one aggregated series -- the query shape isn't why;
# the export's label handling is.
#
# So: plot every observation as an UNCONNECTED point. Do not invent pod names, do not join same-
# timestamp readings into one line (that draws a zigzag mixing distinct readings, and implies a
# continuity that doesn't exist), and do not reduce them to a pooled median presented as any single
# pod's rate -- none of that is supported by what this export actually preserved.
obs = [(t, v) for t, v in series(MET / "rows_served_per_pod.csv") if WIN_START <= t <= WIN_END]
if not obs:
    raise SystemExit(
        f"no {MET / 'rows_served_per_pod.csv'} samples fall inside window_Q "
        f"({WIN_START:.0f}..{WIN_END:.0f}) -- refusing to silently pick another window"
    )
fig, ax = plt.subplots(figsize=(13.5, 5.2))
xs = [dt.datetime.fromtimestamp(t, dt.UTC) for t, _ in obs]
ys = [v / 1e6 for _, v in obs]
ax.scatter(xs, ys, color=C_CQL, s=24, alpha=0.75, edgecolors="none")
ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=dt.UTC))
ax.set_ylabel("rows/sec served (millions)", fontsize=11)
ax.set_xlabel(f"time (UTC) — {WIN_DUR_MIN:.1f} min recorded window_Q phase", fontsize=10)
ax.set_title("Same window_Q phase: every exported reading (export did not preserve per-pod identity)",
             fontsize=14, fontweight="bold", pad=12)
ax.grid(axis="y", alpha=0.25, lw=0.6)
ax.set_axisbelow(True)
for s_ in ("top", "right"):
    ax.spines[s_].set_visible(False)
fig.text(0.5, 0.018,
         f"{len(obs)} observations at {len(win)} timestamps, all labeled 'cqlite-flight' -- export "
         "did not preserve per-pod/instance identity, so no breakdown or single-pod claim follows. "
         "Fan-out itself is unmeasured (issue #4175).",
         ha="center", fontsize=7.5, color="#555")
fig.tight_layout(rect=(0, 0.055, 1, 1))
fig.savefig(OUT / "rows-per-sec-per-pod.png", dpi=170)
plt.close(fig)
print(f"wrote rows-per-sec-per-pod.png  ({len(obs)} observations)")
