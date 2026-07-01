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
  verdict_byte_stable

WHAT IS DELIBERATELY EXCLUDED (drifts/rebinds across regeneration — NOT a guardable
human-authored oracle field):
  corrupted_sha256, original_sha256, fixture_dir_sha256, *_size_bytes,
  byte_offset, *_bytes_hex, clean_source_path (regenerated table UUID),
  corrupted_path (the OBSERVED bytes), AND BOTH machine-computed verdict-bindings:
  verdict_captured_for_dir_sha256 AND verdict_captured_for_sha256.

  Both bindings are regeneration-DEPENDENT hashes the generator REBINDS on the MODE 2
  (VERIFY_ONLY=0) live-Cassandra regeneration path (issue #1294 roborev Finding 1):
  the full-dir sha rebinds on any non-deterministic SIBLING drift, and the mutated-
  component sha rebinds when the MUTATED component is itself non-byte-reproducible
  (the BTI `da` trie fixtures bti_partitions_footer_flip -> Partitions.db and
  bti_rows_truncation -> Rows.db serialize non-deterministically; Statistics.db
  embeds wall-clock metadata). Guarding either here would make this guard contradict
  the generator (a benign regeneration would false-fail CI). Their integrity is
  enforced in MODE 1 instead — against the committed byte-stable tree — by the parity
  test (sstable_parity_corruption_verify.rs::check_full_dir_binding, both hashes
  FATAL), the generator's own `--verify-only` branch, and validate-committed-dir-
  binding.py (committed dir-sha, PRE-regeneration).

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
    # BOTH verdict_captured_for_sha256 AND verdict_captured_for_dir_sha256 are
    # DELIBERATELY OMITTED (issue #1294 roborev Finding 1): the generator REBINDS both
    # on the MODE 2 (VERIFY_ONLY=0) regeneration path — the dir-sha on any
    # non-deterministic sibling drift, and the mutated-component sha when the mutated
    # component is itself non-byte-reproducible (BTI `da` trie / Statistics.db). A
    # rebound field is regeneration-dependent and cannot be guarded here without
    # contradicting the generator. Their integrity is enforced in MODE 1 by the parity
    # test (sstable_parity_corruption_verify.rs::check_full_dir_binding — both FATAL),
    # the generator's `--verify-only` branch, and validate-committed-dir-binding.py.
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
