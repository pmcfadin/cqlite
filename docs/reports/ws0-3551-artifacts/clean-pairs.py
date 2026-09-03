#!/usr/bin/env python3
"""Pool the CLEAN within-round pairs across several A/B/C sets.

WHY THIS EXISTS. `ws0_abc_aggregate.py` aggregates a SET: every pairable round of every arm.
That is the right instrument for a quiet box. This box is shared with nine other delivery
lanes, and measured over this issue's three sets a 40-minute window free of compilers did not
occur — set 1 was clean, set 2 lost 9 of 15 sessions to a peer gate, set 3 lost its window
5.5 minutes in. Discarding a set because part of it was contaminated throws away the sessions
that were not.

THE PROPERTY THAT MAKES THIS SOUND, and its limit. Method §3b step 4 takes the difference
WITHIN a round; a pair is (baseline arm, treatment arm) inside ONE round. So a round whose
baseline AND treatment both ran under a zero census is a valid pair no matter what happened in
any other round or any other arm. What is LOST relative to a fully clean set is statistical
power (fewer pairs) and the cross-arm control table over a whole set — NOT the pairing itself.
What is NOT bought is protection from foreign load the census cannot see (#3551 D3): these
pairs are clean by the same definition the in-run gate uses, no stronger.

EVERY PAIR CARRIES ITS OWN CONTROL. The two sessions of a pair each ran the bare-scan leg on
the same CPUs with the same binary, so their bare-scan disagreement is that pair's own drift
bound. A pair whose control disagrees by more than the treatment delta is reported and
EXCLUDED from the medians, because there is nothing to read the delta against — the same rule
the set-level aggregator applies, at pair granularity.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import statistics
import sys

# The COVERAGE rule is IMPORTED from the committed judge, never restated here. A second copy of
# `MAX_SAMPLE_GAP_S` is a second thing to keep true, and this tool's whole claim is that its
# pairs are clean by the IN-RUN GATE's definition — which is only true if it uses the gate's own
# numbers. `scripts/perf` is resolved from this file's location so the import cannot silently
# bind to some other checkout.
_PERF = pathlib.Path(__file__).resolve().parents[3] / "scripts" / "perf"
if not (_PERF / "ws0_quiescence.py").is_file():
    raise SystemExit(f"REFUSED: cannot locate ws0_quiescence.py under {_PERF}; the coverage rule "
                     "is imported from the committed judge and is not restated here")
sys.path.insert(0, str(_PERF))
from ws0_quiescence import MAX_SAMPLE_GAP_S, SAMPLER_CADENCE_S  # noqa: E402

SCAN = "bare_scan"


class Unreadable(Exception):
    pass


def series(path: pathlib.Path) -> list[dict]:
    rows = [json.loads(l) for l in path.read_text().splitlines() if l.strip()]
    if not rows:
        raise Unreadable(f"{path} holds no samples")
    return rows


def session(d: pathlib.Path) -> dict:
    rj, wj = d / "results.json", d / "abc-window.json"
    for p in (rj, wj):
        if not p.is_file():
            raise Unreadable(f"{p} is absent")
    raw = json.loads(rj.read_text())
    win = json.loads(wj.read_text())
    warm = {}
    for m in raw.get("measurements", []):
        if m.get("temperature") != "warm":
            continue
        warm[m["arm"]] = {
            "rps": float(m["rows_per_sec"]["median"]),
            "cpr": float(m["cycles_per_row"]["median"]),
            "ipc": float(m["ipc"]["median"]),
        }
    if SCAN not in warm:
        raise Unreadable(f"{rj} has no warm {SCAN} leg — no control, so no readable pair")
    flight = [a for a in warm if a != SCAN]
    if len(flight) != 1:
        raise Unreadable(f"{rj} has {len(flight)} warm flight arms, expected 1")
    return {"dir": d, "arm": win["arm"], "round": win["round"], "pos": win["position_in_round"],
            "started": win["started"], "ended": win["ended"], "exit": win.get("exit"),
            "scan": warm[SCAN], "flight": warm[flight[0]]}


def _iso(ts: str) -> float:
    """Seconds-resolution epoch from the sampler's `%Y-%m-%dT%H:%M:%SZ`."""
    import calendar
    import time
    return calendar.timegm(time.strptime(ts, "%Y-%m-%dT%H:%M:%SZ"))


def coverage_gap(sess: dict, win: list[dict]) -> float | None:
    """Largest UNOBSERVED stretch inside the session window, boundaries included.

    Same shape as the judge's own coverage check: the gap from window start to the first
    sample, every gap between consecutive samples, and the gap from the last sample to window
    end. Returns None when it cannot be computed at all.
    """
    if not win:
        return None
    lo, hi = _iso(sess["started"]), _iso(sess["ended"])
    inst = sorted(_iso(r["ts"]) for r in win)
    gaps = [inst[0] - lo]
    gaps += [inst[i + 1] - inst[i] for i in range(len(inst) - 1)]
    gaps.append(hi - inst[-1])
    return max(gaps)


def clean(sess: dict, rows: list[dict]) -> tuple[str, int, int, float | None]:
    """Three-valued: `clean` / `contaminated` / `undercovered`.

    A NON-EMPTY sample set is NOT coverage, and treating it as coverage was a real defect in
    this tool's first version (roborev, #3551 round 2): one zero-census sample anywhere in a
    window made the session `clean`, so a mostly UNOBSERVED session could enter the medians as
    clean. That is the same "a positive verdict requires an affirmative measurement" rule this
    file's docstring already claimed to follow — implemented for the empty case and not for the
    undercovered one, which is the harder half. The bound is the JUDGE's own MAX_SAMPLE_GAP_S,
    imported rather than restated.
    """
    win = [r for r in rows if sess["started"] <= r["ts"] <= sess["ended"]]
    comp = sum(1 for r in win if r.get("competing_count"))
    gap = coverage_gap(sess, win)
    if comp:
        return "contaminated", comp, len(win), gap
    if gap is None or gap > MAX_SAMPLE_GAP_S:
        return "undercovered", comp, len(win), gap
    return "clean", comp, len(win), gap


def _append_excluded(out: list[str], excluded: list[str]) -> None:
    """The EXCLUDED pairs, appended wherever the report ends.

    It is a SECTION and not an aside because "no pair was readable" and "a pair existed and
    its own drift control was larger than the effect" are different operator facts. The
    no-pairs branch used to return BEFORE this, so a run whose EVERY pair was excluded printed
    a bare `NO CLEAN PAIRS` and dropped the reason — the one case where the reason is the whole
    of the information. Measured before the fix on a two-session fixture: the exclusion line
    was absent from the output entirely.
    """
    if not excluded:
        return
    out.append("")
    out.append(f"### {len(excluded)} clean pair(s) EXCLUDED — control ≥ treatment")
    out.append("")
    for e in excluded:
        out.append(f"* {e}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--set", action="append", required=True, metavar="LABEL=DIR",
                    help="repeatable; e.g. --set set1=/data/ws0-3551/abc")
    ap.add_argument("--timeseries", required=True)
    ap.add_argument("--baseline", default="A")
    ap.add_argument("--out", default=None)
    args = ap.parse_args(argv)

    rows = series(pathlib.Path(args.timeseries))
    sets: dict[str, pathlib.Path] = {}
    for spec in args.set:
        if "=" not in spec:
            raise Unreadable(f"--set {spec!r} is not LABEL=DIR")
        label, _, path = spec.partition("=")
        sets[label] = pathlib.Path(path)

    # (set, round) -> arm -> session
    grid: dict[tuple[str, int], dict[str, dict]] = {}
    total = 0
    for label, root in sets.items():
        for d in sorted(root.glob("r*-*")):
            if not (d / "results.json").is_file():
                continue
            s = session(d)
            total += 1
            s["set"] = label
            s["state"], s["comp"], s["nsamp"], s["gap"] = clean(s, rows)
            s["clean"] = s["state"] == "clean"
            grid.setdefault((label, s["round"]), {})[s["arm"]] = s

    pairs: dict[str, list[dict]] = {}
    excluded: list[str] = []
    for (label, rnd), arms in sorted(grid.items()):
        base = arms.get(args.baseline)
        if base is None or not base["clean"]:
            continue
        for arm, s in sorted(arms.items()):
            if arm == args.baseline or not s["clean"]:
                continue
            # THIS PAIR'S OWN DRIFT BOUND: the two bare-scan legs ran identical code on
            # identical CPUs, so their disagreement is what a treatment delta must beat.
            ctl = 100.0 * abs(s["scan"]["cpr"] - base["scan"]["cpr"]) / base["scan"]["cpr"]
            d_cpr = 100.0 * (s["flight"]["cpr"] - base["flight"]["cpr"]) / base["flight"]["cpr"]
            d_rps = 100.0 * (s["flight"]["rps"] - base["flight"]["rps"]) / base["flight"]["rps"]
            rec = {"set": label, "round": rnd, "arm": arm, "ctl": ctl,
                   "d_cpr": d_cpr, "d_rps": d_rps, "ipc": s["flight"]["ipc"]}
            if ctl >= abs(d_cpr):
                excluded.append(f"{label} r{rnd} {arm}: control moved {ctl:.2f}% vs "
                                f"treatment {d_cpr:+.2f}% — nothing to read it against")
                continue
            pairs.setdefault(arm, []).append(rec)

    out = []
    states = [s["state"] for g in grid.values() for s in g.values()]
    nclean = states.count("clean")
    out.append(f"Sessions examined: {total} across {len(sets)} set(s); "
               f"**{nclean} clean**, {states.count('contaminated')} contaminated, "
               f"{states.count('undercovered')} UNDERCOVERED (an unobserved window is "
               f"could-not-measure, never clean: the bound is the judge's own "
               f"MAX_SAMPLE_GAP_S = {MAX_SAMPLE_GAP_S:.0f}s at a {SAMPLER_CADENCE_S:.0f}s "
               f"cadence).")
    out.append("")
    out.append(f"A pair is (baseline `{args.baseline}`, treatment) inside ONE round, with BOTH "
               "sessions clean. Method §3b step 4 differences within a round, so such a pair is "
               "valid regardless of any other round or arm.")
    out.append("")
    if not pairs:
        out.append("**NO CLEAN PAIRS.** Nothing here is readable; do not derive a delta from it.")
        _append_excluded(out, excluded)
        text = "\n".join(out)
        print(text)
        if args.out:
            pathlib.Path(args.out).write_text(text + "\n")
        return 0
    out.append("| arm | clean pairs | median Δcycles/row | median Δrows/s | direction (rows/s) | "
               "worst pair-control | median IPC |")
    out.append("|---|--:|--:|--:|--:|--:|--:|")
    for arm, ps in sorted(pairs.items()):
        dc = [p["d_cpr"] for p in ps]
        dr = [p["d_rps"] for p in ps]
        up = sum(1 for v in dr if v > 0)
        out.append(f"| {arm} | {len(ps)} | {statistics.median(dc):+.2f}% | "
                   f"{statistics.median(dr):+.2f}% | {up}/{len(dr)} up | "
                   f"{max(p['ctl'] for p in ps):.2f}% | "
                   f"{statistics.median(p['ipc'] for p in ps):.4f} |")
    out.append("")
    out.append("`worst pair-control` is the largest bare-scan disagreement inside any counted "
               "pair — identical code on identical CPUs, so it is that pair's own drift bound.")
    out.append("")
    out.append("### Every counted pair, individually")
    out.append("")
    out.append("| set | round | arm | Δcycles/row | Δrows/s | pair-control |")
    out.append("|---|--:|---|--:|--:|--:|")
    for arm, ps in sorted(pairs.items()):
        for p in ps:
            out.append(f"| {p['set']} | {p['round']} | {p['arm']} | {p['d_cpr']:+.2f}% | "
                       f"{p['d_rps']:+.2f}% | {p['ctl']:.2f}% |")
    _append_excluded(out, excluded)
    text = "\n".join(out)
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
