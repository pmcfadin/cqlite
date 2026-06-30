#!/usr/bin/env python3
"""extract-corruption-oracle.py — project the HUMAN-AUTHORED oracle fields out of
corruption-manifest.yml (issue #1294 Item 2).

WHY
---
The compression/corruption-parity CI lane REGENERATES corruption-manifest.yml from
generate-corruption-corpus.sh before running the parity tests. The generator hard-
codes the Cassandra-verdict oracle in its FIXTURES table, so a PR that edits ONLY a
committed oracle field (a captured cassandra_verdict, a verdict_parity, an
expected_error_class, a captured-against sha, ...) would be SILENTLY OVERWRITTEN at
CI time — the tests would then run against the script's hard-coded oracle instead of
the committed change. The CI guard diffs this projection of the COMMITTED manifest
against the same projection of the REGENERATED manifest and fails on any drift, so a
human-committed oracle edit is authoritative, not silently regenerated away.

SCOPE OF THIS CI DRIFT-GUARD (issue #1294 roborev follow-up)
------------------------------------------------------------
The guard protects ONLY the HUMAN-AUTHORED oracle fields — the ones a PR author
edits by hand and that the generator emits VERBATIM (never rebinds) on the default
regeneration path. Regeneration-DEPENDENT machine-computed hashes are deliberately
NOT in this projection: their integrity is enforced deterministically by the PARITY
TEST (cqlite-core/tests/sstable_parity_corruption_verify.rs::check_full_dir_binding)
against the committed byte-stable git tree — that is where a committed tamper is
caught. Guarding a regeneration-rebound value here would make the guard contradict
the generator: generate-corruption-corpus.sh intentionally REBINDS
verdict_captured_for_dir_sha256 to the freshly-regenerated dir hash on benign
sibling drift (VERIFY_ONLY=0), so diffing the rebound value against the committed
value would FAIL CI on a drift the generator (correctly) treats as benign. RULE:
only guard fields that the generator does NOT rebind during regeneration.

WHAT IS PROJECTED (the authoritative, human-edited oracle — deterministic, NOT
regeneration-rebound):
  name, manifest_key, status, component, expected_failing_component,
  expected_error_class, rationale, cassandra_verdict, verdict_parity, verdict_note,
  verdict_captured_for_sha256, verdict_byte_stable

WHAT IS DELIBERATELY EXCLUDED (drifts/rebinds across regeneration — NOT a guardable
human-authored oracle field):
  corrupted_sha256, original_sha256, fixture_dir_sha256, *_size_bytes,
  byte_offset, *_bytes_hex, clean_source_path (regenerated table UUID),
  corrupted_path (the OBSERVED bytes), AND verdict_captured_for_dir_sha256 — the
  latter is a regeneration-DEPENDENT hash the generator REBINDS to the fresh
  full-dir hash on benign drift, so it cannot be guarded here without contradicting
  the generator; its integrity is enforced by the parity test instead.

  NOTE on verdict_captured_for_sha256 (KEPT in the projection): the generator treats
  the per-component verdict sha-binding as advisory but NEVER rebinds it — on every
  regeneration path it emits the COMMITTED value verbatim (deterministic mutation ->
  stable mutated-component sha). It is therefore regeneration-STABLE and IS a
  human-authored oracle field safe to guard. Only the FULL-DIR sha is rebound, hence
  only it is excluded.

Usage:
  extract-corruption-oracle.py <manifest.yml>     # prints the canonical projection
Reads the manifest from a path, or from stdin when the path is '-'.
"""
import sys
import re

# Ordered list of oracle keys to project (stable output ordering).
ORACLE_KEYS = [
    "manifest_key",
    "status",
    "component",
    "expected_failing_component",
    "expected_error_class",
    "rationale",
    "cassandra_verdict",
    "verdict_parity",
    "verdict_note",
    "verdict_captured_for_sha256",
    # verdict_captured_for_dir_sha256 is DELIBERATELY OMITTED: the generator rebinds
    # it to the freshly-regenerated full-dir hash on benign sibling drift, so it is
    # regeneration-dependent and cannot be guarded here without contradicting the
    # generator. Its integrity is enforced by the parity test
    # (sstable_parity_corruption_verify.rs::check_full_dir_binding), not this guard.
    "verdict_byte_stable",
]


def main() -> int:
    if len(sys.argv) != 2:
        sys.stderr.write("usage: extract-corruption-oracle.py <manifest.yml|->\n")
        return 2
    src = sys.argv[1]
    txt = sys.stdin.read() if src == "-" else open(src, encoding="utf-8").read()

    # Split into per-fixture blocks (the manifest is a flat list of fixtures).
    blocks = re.split(r"\n  - name: ", txt)
    out_lines = []
    for b in blocks[1:]:
        name = b.splitlines()[0].strip()
        out_lines.append(f"fixture: {name}")

        def field(key: str) -> str:
            m = re.search(rf"^    {re.escape(key)}: (.*)$", b, re.M)
            if not m:
                return "<absent>"
            # Normalize: strip surrounding quotes + trailing whitespace so a
            # quoting/whitespace-only change is not flagged as an oracle drift.
            return m.group(1).strip().strip('"')

        for k in ORACLE_KEYS:
            out_lines.append(f"  {k}: {field(k)}")

    # Sort fixtures by name so manifest ordering changes do not cause spurious
    # diffs; within a fixture the key order is the fixed ORACLE_KEYS order.
    # Re-group: each fixture starts with a "fixture: " line.
    fixtures = []
    cur = []
    for ln in out_lines:
        if ln.startswith("fixture: ") and cur:
            fixtures.append(cur)
            cur = []
        cur.append(ln)
    if cur:
        fixtures.append(cur)
    fixtures.sort(key=lambda blk: blk[0])
    for blk in fixtures:
        print("\n".join(blk))
    return 0


if __name__ == "__main__":
    sys.exit(main())
