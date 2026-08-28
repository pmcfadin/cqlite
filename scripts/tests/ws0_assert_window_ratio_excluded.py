#!/usr/bin/env python3
"""Assert the window/lifetime ratio is NOT one of ws0_clock.py's occupancy sources.

#3299 mistook the counting window over perf's own process lifetime (20s/25s = 0.80) for a
measured occupancy and used it to license a comparison. The ratio is still recorded — a
reader wants to see it — but it must never take part in the agreement check that gates
printing a clock. Exit 0 = correctly excluded.
"""
import sys

def main(path: str) -> int:
    src = open(path, encoding="utf-8").read()
    try:
        start = src.index("occupancy_sources: Dict[str, float] = {")
        end = src.index("if len(occupancy_sources) < 2")
    except ValueError as exc:
        print(f"cannot locate the occupancy_sources block: {exc}")
        return 2
    if "window_over_lifetime" in src[start:end]:
        print("window_over_lifetime appears inside the occupancy_sources block")
        return 1
    return 0

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: ws0_assert_window_ratio_excluded.py <ws0_clock.py>")
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
