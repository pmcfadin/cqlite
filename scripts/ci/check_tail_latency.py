#!/usr/bin/env python3
"""Report mixed-load tail-latency ratios against thresholds; advisory-first (Issue #1563).

Reads the tail-latency harness JSON (emitted by the `tail_latency` bench) and the
gate policy JSON, prints each derived ratio against its `max` threshold with a
status, and picks the exit code by the gate mode:

  - ADVISORY (policy `advisory: true` and no `--enforce`): breaches are REPORTED
    with an advisory status, but the checker ALWAYS exits 0 (never fails CI). This
    records the current convoy so the C2/F1/F3 tail fixes can be shown red-then-green.
  - ENFORCING (policy `advisory: false`, or the `--enforce` flag): any ratio that
    exceeds its `max` exits non-zero; otherwise exit 0.

Ratios checked (top-level keys of the harness JSON):
  - p99_over_p50               — tail spread within the mixed load (mixed.p99/mixed.p50)
  - p99_mixed_over_scan_free   — convoy inflation (mixed.p99 / scan_free.p99)

All thresholds are intra-run RATIOS, never wall-clock absolutes, so shared-runner
noise cannot flap the gate.

Flip-to-enforcing: once the C2/F1/F3 tail fixes land and p99_mixed_over_scan_free
drops, tighten the `max` values in cqlite-core/benches/tail-latency-gate.json to
the new floor and set `advisory: false` (or pass --enforce in CI). See
cqlite-core/benches/README.md.

Usage:
    check_tail_latency.py <harness_json> <gate_json> [--enforce]

Example:
    check_tail_latency.py /tmp/tail.json cqlite-core/benches/tail-latency-gate.json
"""

import json
import math
import sys

# Ratios the gate knows about (top-level keys in the harness JSON).
RATIO_KEYS = ("p99_over_p50", "p99_mixed_over_scan_free")


def main(argv):
    positional = [a for a in argv[1:] if not a.startswith("--")]
    flags = {a for a in argv[1:] if a.startswith("--")}
    if len(positional) != 2:
        print(__doc__)
        return 2

    harness_path, gate_path = positional
    enforce_flag = "--enforce" in flags

    with open(harness_path) as fh:
        harness = json.load(fh)
    with open(gate_path) as fh:
        gate = json.load(fh)

    advisory = bool(gate.get("advisory", True))
    enforcing = enforce_flag or not advisory
    ratios_cfg = gate.get("ratios", {})

    mode = "ENFORCING" if enforcing else "ADVISORY (reported only)"
    print(f"Tail-latency ratio gate [{mode}]\n")
    col = 30
    header = f"{'ratio':<{col}} {'value':>12} {'max':>12}  status"
    print(header)
    print("-" * len(header))

    breaches = []
    # A ratio the gate has a `max` for but that is absent OR non-finite/malformed in
    # the harness JSON. In enforcing mode this is a fail-closed error (stale/malformed
    # harness output — including NaN, which makes `value > threshold` false — must NOT
    # bypass the gate); in advisory mode it is a reported SKIP.
    missing_required = []

    def _finite_number(v):
        # Reject None, bool (a subclass of int), and non-finite floats (NaN/inf).
        return (
            isinstance(v, (int, float))
            and not isinstance(v, bool)
            and math.isfinite(float(v))
        )

    for key in RATIO_KEYS:
        value = harness.get(key)
        cfg = ratios_cfg.get(key)
        has_threshold = cfg is not None and cfg.get("max") is not None

        if not _finite_number(value):
            reason = "absent from harness JSON" if value is None else f"malformed ({value!r})"
            if enforcing and has_threshold:
                missing_required.append(key)
                print(f"{key:<{col}} {str(value):>12.12} {float(cfg['max']):>12.3f}  INVALID (required)")
            else:
                print(f"{key:<{col}} {'-':>12} {'-':>12}  SKIP ({reason})")
            continue
        if not has_threshold:
            print(f"{key:<{col}} {float(value):>12.3f} {'-':>12}  SKIP (no threshold)")
            continue

        value = float(value)
        threshold = float(cfg["max"])
        if value > threshold:
            breaches.append((key, value, threshold))
            status = "BREACH" if enforcing else "BREACH (advisory — reported only)"
        else:
            status = "ok"
        print(f"{key:<{col}} {value:>12.3f} {threshold:>12.3f}  {status}")

    print()
    # Fail-closed: enforcing mode must not exit 0 when a required (thresholded)
    # ratio is missing from the harness JSON — that would let stale/malformed
    # output silently pass the gate.
    if missing_required:
        print(
            f"❌ {len(missing_required)} required tail ratio(s) missing or malformed in harness "
            f"JSON (enforcing): {', '.join(missing_required)}"
        )
        print("   Regenerate the harness JSON (cargo bench --bench tail_latency) before gating.")
        return 1

    if breaches:
        if enforcing:
            print(f"❌ {len(breaches)} tail ratio(s) exceeded threshold:")
            for key, value, threshold in breaches:
                print(f"   - {key}: {value:.3f} > {threshold:.3f}")
            return 1
        print(f"⚠️  {len(breaches)} tail ratio(s) exceeded threshold (advisory — not failing CI):")
        for key, value, threshold in breaches:
            print(f"   - {key}: {value:.3f} > {threshold:.3f}")
        print(
            "\nAdvisory gate: reported only. Flip to enforcing by setting "
            "advisory:false in tail-latency-gate.json or passing --enforce."
        )
        return 0

    print("✅ All tail ratios within threshold.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
