#!/usr/bin/env python3
"""Aggregate an interleaved A/B/C(/C0) session set into the report tables #3551 needs.

WHAT THIS IS FOR. `ws0-baseline.sh` measures ONE configuration per invocation, so an A/B/C
comparison is a SET of its sessions. This reads those sessions and reports what method §3b
step 4 requires and no single session can produce: the PAIRED per-round deltas and the
within-round direction count, beside every median's own spread.

WHAT IT REFUSES TO DO. It never blends temperatures, never reports a CPU-share (#2877), never
compares across a round an arm is missing from, and never prints a delta without the control's
own movement beside it -- an arm delta is only readable against a control that did not move.

THE CONTROL. Every arm pins the BARE-SCAN leg to the same `--server-cpus` and varies only the
Flight leg, so the bare scan is code-identical AND pin-identical across arms. Its movement
across arms is therefore drift plus contamination and nothing else, which is exactly the
control §3b step 3 asks for and the committed rig does not provide. If the control moves as
much as the treatment, there is NO READABLE RESULT and this tool says so rather than
publishing the treatment's delta.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import statistics
import sys

SCAN_ARM = "bare_scan"


class Unreadable(Exception):
    """A session that cannot be aggregated. Never downgraded to a warning."""


def _num(value, what):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise Unreadable(f"{what} is {value!r}, not a number")
    return float(value)


def load_session(d: pathlib.Path) -> dict:
    """One `ws0-baseline.sh` results.json, reduced to the fields this report prints.

    Every field is REQUIRED. An absent one is an Unreadable, never a default: a missing
    counter reported as 0 is the fabrication the rig's own integrity contract forbids.
    """
    p = d / "results.json"
    try:
        raw = json.loads(p.read_text())
    except (OSError, ValueError) as exc:
        raise Unreadable(f"{p}: {exc}") from None
    out = {"dir": str(d), "warm": {}}
    for m in raw.get("measurements", []):
        if m.get("temperature") != "warm":
            continue
        arm = m.get("arm")
        if not arm:
            raise Unreadable(f"{p}: a measurement carries no `arm`")
        reps = m.get("reps") or []
        if not reps:
            raise Unreadable(f"{p}: arm {arm} carries no reps")
        out["warm"][arm] = {
            "rows_per_sec": _num(m["rows_per_sec"]["median"], f"{arm} rows_per_sec"),
            "cycles_per_row": _num(m["cycles_per_row"]["median"], f"{arm} cycles_per_row"),
            "ipc": _num(m["ipc"]["median"], f"{arm} ipc"),
            "spread_pct": _num(m["rows_per_sec"]["spread_pct_of_median"], f"{arm} spread"),
            "rows": m.get("row_denominator_total"),
            "n": m["rows_per_sec"]["n"],
        }
    if SCAN_ARM not in out["warm"]:
        raise Unreadable(f"{p}: no warm {SCAN_ARM} leg — the drift control is ABSENT, so no"
                         " delta from this session is readable")
    flight = [a for a in out["warm"] if a != SCAN_ARM]
    if len(flight) != 1:
        raise Unreadable(f"{p}: expected exactly one warm flight arm, found {flight!r}")
    out["flight_arm"] = flight[0]
    pin = raw.get("pinning") or {}
    out["pinning"] = {
        "server_cpus": pin.get("server_cpus"),
        "flight_server_cpus": pin.get("flight_server_cpus", pin.get("server_cpus")),
        "counter_mode": pin.get("counter_mode"),
        "flight_pin_mode": pin.get("flight_pin_mode"),
        "flight_allocator": pin.get("flight_allocator"),
        "flight_malloc_arena_max": pin.get("flight_malloc_arena_max"),
    }
    out["quiescence"] = raw.get("quiescence_verdict") or raw.get("quiescence")
    return out


def spread_pct(values):
    if len(values) < 2:
        return 0.0
    med = statistics.median(values)
    return 100.0 * (max(values) - min(values)) / med if med else float("nan")


def table(rows, headers):
    widths = [max(len(str(r[i])) for r in [headers] + rows) for i in range(len(headers))]
    sep = "|" + "|".join("-" * (w + 2) for w in widths) + "|"
    def line(r):
        return "| " + " | ".join(str(v).ljust(w) for v, w in zip(r, widths)) + " |"
    return "\n".join([line(headers), sep] + [line(r) for r in rows])


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--root", required=True,
                    help="directory holding r<N>-<arm>/ session dirs")
    ap.add_argument("--arms", required=True,
                    help="comma-separated arm labels in report order, e.g. A,B,C0,C")
    ap.add_argument("--baseline", required=True, help="the arm every delta is taken against")
    ap.add_argument("--out", default=None, help="write the markdown here as well as stdout")
    args = ap.parse_args(argv)

    arms = [a.strip() for a in args.arms.split(",") if a.strip()]
    if args.baseline not in arms:
        print(f"REFUSED: baseline {args.baseline!r} is not in --arms {arms!r}", file=sys.stderr)
        return 2
    root = pathlib.Path(args.root)

    # rounds x arms, loaded strictly. A round missing ANY arm is dropped WHOLE and named:
    # a paired delta over a round that lacks its baseline is not a paired delta.
    sessions, rounds = {}, []
    for d in sorted(root.glob("r*-*")):
        if not d.is_dir():
            continue
        stem = d.name
        rnd, _, arm = stem.partition("-")
        if arm not in arms:
            continue
        try:
            rn = int(rnd.lstrip("r"))
        except ValueError:
            continue
        sessions[(rn, arm)] = load_session(d)
        if rn not in rounds:
            rounds.append(rn)
    rounds.sort()
    complete = [r for r in rounds if all((r, a) in sessions for a in arms)]
    dropped = [r for r in rounds if r not in complete]
    if not complete:
        print("REFUSED: no round carries every arm; nothing is pairable", file=sys.stderr)
        return 2

    lines = []
    lines.append(f"Rounds pairable: {complete}"
                 + (f"  (DROPPED, incomplete: {dropped})" if dropped else ""))
    lines.append("")

    # --- the control, FIRST, because the treatment is unreadable without it -------------
    lines.append("## The drift control (bare scan — code-identical AND pin-identical in every arm)")
    lines.append("")
    ctl_rows = []
    for a in arms:
        cyc = [sessions[(r, a)]["warm"][SCAN_ARM]["cycles_per_row"] for r in complete]
        rps = [sessions[(r, a)]["warm"][SCAN_ARM]["rows_per_sec"] for r in complete]
        ipc = [sessions[(r, a)]["warm"][SCAN_ARM]["ipc"] for r in complete]
        ctl_rows.append([a, f"{statistics.median(rps):,.0f}", f"{spread_pct(rps):.2f}%",
                         f"{statistics.median(cyc):,.0f}", f"{spread_pct(cyc):.2f}%",
                         f"{statistics.median(ipc):.4f}"])
    lines.append(table(ctl_rows, ["arm", "rows/s", "spread", "cycles/row", "spread", "IPC"]))
    lines.append("")
    ctl_med = [statistics.median([sessions[(r, a)]["warm"][SCAN_ARM]["cycles_per_row"]
                                  for r in complete]) for a in arms]
    ctl_move = 100.0 * (max(ctl_med) - min(ctl_med)) / statistics.median(ctl_med)
    lines.append(f"**Control movement across arms: {ctl_move:.2f}% on cycles/row.** The control is"
                 " identical code on identical CPUs in every arm, so this is drift plus"
                 " contamination and nothing else. Any treatment delta smaller than it is NOT"
                 " READABLE.")
    lines.append("")

    # --- Layer 1, the invariant layer ---------------------------------------------------
    lines.append("## Layer 1 — the INVARIANT layer (cycles/row, IPC, ratio)")
    lines.append("")
    l1 = []
    base_cyc = {r: sessions[(r, args.baseline)]["warm"][
        sessions[(r, args.baseline)]["flight_arm"]]["cycles_per_row"] for r in complete}
    for a in arms:
        cyc = [sessions[(r, a)]["warm"][sessions[(r, a)]["flight_arm"]]["cycles_per_row"]
               for r in complete]
        ipc = [sessions[(r, a)]["warm"][sessions[(r, a)]["flight_arm"]]["ipc"] for r in complete]
        ratio = [sessions[(r, a)]["warm"][sessions[(r, a)]["flight_arm"]]["cycles_per_row"]
                 / sessions[(r, a)]["warm"][SCAN_ARM]["cycles_per_row"] for r in complete]
        # PAIRED per round, never median-of-medians: the pairing is the control for drift.
        per_round = [100.0 * (sessions[(r, a)]["warm"][sessions[(r, a)]["flight_arm"]]
                              ["cycles_per_row"] - base_cyc[r]) / base_cyc[r] for r in complete]
        pos = sum(1 for d in per_round if d > 0)
        l1.append([a, f"{statistics.median(cyc):,.0f}", f"{spread_pct(cyc):.2f}%",
                   f"{statistics.median(ipc):.4f}", f"{statistics.median(ratio):.4f}x",
                   ("baseline" if a == args.baseline
                    else f"{statistics.median(per_round):+.2f}%"),
                   ("—" if a == args.baseline
                    else f"{pos}/{len(per_round)} up")])
    lines.append(table(l1, ["arm", "cycles/row", "spread", "IPC", "ratio bare/flight",
                            f"paired vs {args.baseline}", "direction"]))
    lines.append("")

    # --- Layer 2, absolutes ------------------------------------------------------------
    lines.append("## Layer 2 — the ABSOLUTE layer (rows/s; no cross-session absolute is reusable)")
    lines.append("")
    l2 = []
    base_rps = {r: sessions[(r, args.baseline)]["warm"][
        sessions[(r, args.baseline)]["flight_arm"]]["rows_per_sec"] for r in complete}
    for a in arms:
        rps = [sessions[(r, a)]["warm"][sessions[(r, a)]["flight_arm"]]["rows_per_sec"]
               for r in complete]
        per_round = [100.0 * (sessions[(r, a)]["warm"][sessions[(r, a)]["flight_arm"]]
                              ["rows_per_sec"] - base_rps[r]) / base_rps[r] for r in complete]
        pos = sum(1 for d in per_round if d > 0)
        rows = sessions[(complete[0], a)]["warm"][
            sessions[(complete[0], a)]["flight_arm"]]["rows"]
        l2.append([a, f"{statistics.median(rps):,.0f}", f"{spread_pct(rps):.2f}%",
                   ("baseline" if a == args.baseline
                    else f"{statistics.median(per_round):+.2f}%"),
                   ("—" if a == args.baseline else f"{pos}/{len(per_round)} up"),
                   f"{rows:,}" if rows else "NOT RECORDED"])
    lines.append(table(l2, ["arm", "rows/s", "spread", f"paired vs {args.baseline}",
                            "direction", "row denominator"]))
    lines.append("")

    # --- what each arm actually WAS, read back from its own artifacts -------------------
    lines.append("## Configuration, read back from each session's own recorded pinning")
    lines.append("")
    cfg = []
    for a in arms:
        s = sessions[(complete[0], a)]["pinning"]
        cfg.append([a, s.get("server_cpus"), s.get("flight_server_cpus"),
                    s.get("flight_pin_mode"), s.get("flight_allocator"),
                    s.get("flight_malloc_arena_max") or "—",
                    (s.get("counter_mode") or "NOT RECORDED")])
    lines.append(table(cfg, ["arm", "scan pin", "flight pin", "pin mode", "allocator",
                             "arena max", "counter mode"]))
    lines.append("")
    lines.append("Every figure above is rows/s AND cycles/row; **no CPU-share is reported**"
                 " (#2877: a share shift with rows/s unmoved is a FAIL, not a win).")

    text = "\n".join(lines)
    print(text)
    if args.out:
        pathlib.Path(args.out).write_text(text + "\n")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Unreadable as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        sys.exit(2)
