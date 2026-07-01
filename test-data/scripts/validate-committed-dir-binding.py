#!/usr/bin/env python3
"""validate-committed-dir-binding.py — pre-regeneration enforcement of the COMMITTED
full-fixture-dir binding (issue #1294 roborev Finding 2).

WHY
---
The compression/corruption-parity and exhaustive-regeneration CI lanes REGENERATE
corruption-manifest.yml from generate-corruption-corpus.sh, which REBINDS
verdict_captured_for_dir_sha256 to the freshly-regenerated full-dir hash on benign
live-Cassandra sibling drift. The drift-guard projection (extract-corruption-oracle.py)
therefore — correctly — OMITS verdict_captured_for_dir_sha256, otherwise a benign
regeneration would false-fail CI. But that left the COMMITTED dir-binding ungated: a PR
that edits ONLY the committed verdict_captured_for_dir_sha256 (inconsistent with the
committed fixture bytes) would be silently overwritten at regeneration time and pass.

This validator closes that hole. It runs BEFORE regeneration overwrites anything,
against the COMMITTED manifest and the on-disk (committed/fetched) fixture binaries,
recomputing each active fixture's full-dir hash with the EXACT algorithm used by
generate-corruption-corpus.sh::fixture_dir_sha256 and
sstable_parity_corruption_verify.rs::fixture_dir_sha256, and FAILS CLOSED on any
mismatch. A committed dir-sha edit inconsistent with the bytes is caught here, before
regeneration can rebind it away.

INVARIANT C is preserved: this runs PRE-regeneration against the committed bytes the
committed dir-sha was authored against, so a later benign live-Cassandra regeneration
(which the post-regeneration drift guard deliberately tolerates) is irrelevant here.

Fixture-gating (issue #1094): when a fixture's binaries are NOT present on disk
(gitignored, not fetched/regenerated in this lane) the fixture is skipped — there is
nothing to validate the committed dir-sha against. With --require, a fixture whose dir
is present-but-incomplete is a hard failure. Pre-#1294 fixtures (empty
verdict_captured_for_dir_sha256) are skipped (no binding recorded).

Usage:
  validate-committed-dir-binding.py <committed-manifest.yml> <corpus-root-dir> [--require]

  <corpus-root-dir> is the directory holding the per-fixture subdirectories
  (e.g. .../corruption/test_comp_corrupt). Each active fixture's bytes are read from
  <corpus-root-dir>/<fixture-name>/.
"""
import sys
import os
import re
import hashlib


def fixture_dir_sha256(d: str) -> str:
    """Byte-for-byte identical to generate-corruption-corpus.sh::fixture_dir_sha256
    and sstable_parity_corruption_verify.rs::fixture_dir_sha256."""
    names = sorted(
        n for n in os.listdir(d) if os.path.isfile(os.path.join(d, n))
    )
    h = hashlib.sha256()
    for n in names:
        h.update(n.encode("utf-8"))
        h.update(b"\x00")
        data = open(os.path.join(d, n), "rb").read()
        h.update(len(data).to_bytes(8, "big"))
        h.update(data)
    h.update(len(names).to_bytes(8, "big"))
    return h.hexdigest()


def has_data_db(d: str) -> bool:
    try:
        return any(n.endswith("-Data.db") for n in os.listdir(d))
    except OSError:
        return False


def main() -> int:
    args = [a for a in sys.argv[1:] if a != "--require"]
    require = "--require" in sys.argv[1:]
    if len(args) != 2:
        sys.stderr.write(
            "usage: validate-committed-dir-binding.py "
            "<committed-manifest.yml> <corpus-root-dir> [--require]\n"
        )
        return 2
    manifest_path, corpus_root = args

    txt = open(manifest_path, encoding="utf-8").read()
    blocks = re.split(r"\n  - name: ", txt)

    checked = 0
    skipped = 0
    failures = []

    for b in blocks[1:]:
        name = b.splitlines()[0].strip()

        def field(key: str) -> str:
            m = re.search(rf"^    {re.escape(key)}: (.*)$", b, re.M)
            return m.group(1).strip().strip('"') if m else ""

        if field("status") != "active":
            continue
        committed_dir_sha = field("verdict_captured_for_dir_sha256")
        if not committed_dir_sha:
            # pre-#1294 fixture: no full-dir binding recorded.
            continue

        fixture_dir = os.path.join(corpus_root, name)
        if not os.path.isdir(fixture_dir) or not has_data_db(fixture_dir):
            # Binaries gitignored and not fetched/regenerated in this lane: nothing
            # to validate the committed dir-sha against.
            if require:
                failures.append(
                    f"{name}: --require set but fixture binaries absent at "
                    f"{fixture_dir} (no Data.db); cannot validate committed dir-sha"
                )
            else:
                sys.stderr.write(
                    f"[skip] {name}: binaries absent at {fixture_dir} "
                    f"(not fetched/regenerated)\n"
                )
                skipped += 1
            continue

        on_disk = fixture_dir_sha256(fixture_dir)
        if on_disk != committed_dir_sha:
            failures.append(
                f"{name}: COMMITTED verdict_captured_for_dir_sha256 "
                f"({committed_dir_sha}) does NOT match the on-disk full-dir hash "
                f"({on_disk}). The committed dir-binding is inconsistent with the "
                f"committed fixture bytes — a committed dir-sha edit would be silently "
                f"rebound by regeneration. Recompute it from the actual fixture bytes "
                f"(issue #1294 Item 2 / roborev Finding 2)."
            )
        else:
            checked += 1

    if failures:
        sys.stderr.write(
            "\nFATAL: committed full-fixture-dir binding(s) inconsistent with the "
            "committed bytes (issue #1294 roborev Finding 2):\n"
        )
        for f in failures:
            sys.stderr.write(f"  - {f}\n")
        return 1

    if checked == 0:
        msg = (
            f"validate-committed-dir-binding: 0 fixtures validated "
            f"({skipped} skipped, binaries absent)."
        )
        if require:
            sys.stderr.write(
                "FATAL: --require set but no committed dir-bindings could be "
                f"validated (binaries absent). {msg}\n"
            )
            return 1
        sys.stderr.write(f"[skip] {msg}\n")
        return 0

    print(
        f"validate-committed-dir-binding: {checked} committed dir-binding(s) match "
        f"the committed bytes ({skipped} skipped, binaries absent)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
