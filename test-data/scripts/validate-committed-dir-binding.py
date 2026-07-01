#!/usr/bin/env python3
"""validate-committed-dir-binding.py — pre-regeneration enforcement of BOTH COMMITTED
byte-bindings: the full-fixture-dir binding (verdict_captured_for_dir_sha256) AND the
mutated-component binding (verdict_captured_for_sha256) (issue #1294 roborev Findings
1 & 2).

WHY
---
The compression/corruption-parity and exhaustive-regeneration CI lanes REGENERATE
corruption-manifest.yml from generate-corruption-corpus.sh, which REBINDS BOTH
verdict_captured_for_dir_sha256 AND verdict_captured_for_sha256 to the
freshly-regenerated hashes on benign live-Cassandra sibling drift. The drift-guard
projection (extract-corruption-oracle.py) therefore — correctly — OMITS BOTH of those
rebound hashes, otherwise a benign regeneration would false-fail CI. But that left BOTH
COMMITTED bindings ungated: a PR that edits ONLY the committed
verdict_captured_for_dir_sha256 OR ONLY the committed verdict_captured_for_sha256
(inconsistent with the committed fixture bytes) would be silently overwritten at
regeneration time and pass.

This validator closes that hole for BOTH bindings. It runs BEFORE regeneration
overwrites anything, against the COMMITTED manifest and the on-disk (committed/fetched)
fixture binaries:
  * recomputes each active fixture's full-dir hash with the EXACT algorithm used by
    generate-corruption-corpus.sh::fixture_dir_sha256 /
    sstable_parity_corruption_verify.rs::fixture_dir_sha256, and
  * recomputes each active fixture's MUTATED-component hash (plain SHA-256 of
    <fixture-dir>/<component>) with the EXACT algorithm used by the generator's
    sha256() / sstable_parity_corruption_verify.rs::sha256_file,
and FAILS CLOSED on any mismatch of either binding. A committed dir-sha OR
mutated-component-sha edit inconsistent with the bytes is caught here, before
regeneration can rebind it away.

INVARIANT C is preserved: this runs PRE-regeneration against the committed bytes the
committed dir-sha was authored against, so a later benign live-Cassandra regeneration
(which the post-regeneration drift guard deliberately tolerates) is irrelevant here.

Fixture-gating (issue #1094): when a fixture's binaries are NOT present on disk
(gitignored, not fetched/regenerated in this lane) the fixture is skipped — there is
nothing to validate the committed bindings against. With --require, a fixture whose dir
is present-but-incomplete is a hard failure. Pre-#1294 fixtures (both committed bindings
empty) are skipped (no binding recorded). A fixture counts as validated when at least
one of its committed bindings is present and matches the on-disk bytes.

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


def component_sha256(path: str) -> str:
    """Plain SHA-256 of a single component file — byte-for-byte identical to the
    generator's sha256() (sha256sum/shasum -a 256) and
    sstable_parity_corruption_verify.rs::sha256_file, which is what
    verdict_captured_for_sha256 is bound to (the MUTATED component's corrupted sha)."""
    h = hashlib.sha256()
    h.update(open(path, "rb").read())
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
        committed_component_sha = field("verdict_captured_for_sha256")
        component = field("component")
        if not committed_dir_sha and not committed_component_sha:
            # pre-#1294 fixture: no committed binding recorded at all.
            continue

        fixture_dir = os.path.join(corpus_root, name)
        if not os.path.isdir(fixture_dir) or not has_data_db(fixture_dir):
            # Binaries gitignored and not fetched/regenerated in this lane: nothing
            # to validate the committed bindings against.
            if require:
                failures.append(
                    f"{name}: --require set but fixture binaries absent at "
                    f"{fixture_dir} (no Data.db); cannot validate committed bindings"
                )
            else:
                sys.stderr.write(
                    f"[skip] {name}: binaries absent at {fixture_dir} "
                    f"(not fetched/regenerated)\n"
                )
                skipped += 1
            continue

        matched_a_binding = False

        # Committed full-dir binding (verdict_captured_for_dir_sha256).
        if committed_dir_sha:
            on_disk = fixture_dir_sha256(fixture_dir)
            if on_disk != committed_dir_sha:
                failures.append(
                    f"{name}: COMMITTED verdict_captured_for_dir_sha256 "
                    f"({committed_dir_sha}) does NOT match the on-disk full-dir hash "
                    f"({on_disk}). The committed dir-binding is inconsistent with the "
                    f"committed fixture bytes — a committed dir-sha edit would be "
                    f"silently rebound by regeneration. Recompute it from the actual "
                    f"fixture bytes (issue #1294 Item 2 / roborev Finding 2)."
                )
            else:
                matched_a_binding = True

        # Committed MUTATED-component binding (verdict_captured_for_sha256). The
        # post-regen drift guard (extract-corruption-oracle.py) also EXCLUDES this
        # rebound hash, so without this check a PR editing ONLY the committed
        # mutated-component sha is silently overwritten on regeneration.
        if committed_component_sha:
            component_file = os.path.join(fixture_dir, component)
            if not component:
                failures.append(
                    f"{name}: committed verdict_captured_for_sha256 present but "
                    f"the 'component' field is empty; cannot locate the mutated "
                    f"component to validate against."
                )
            elif not os.path.isfile(component_file):
                failures.append(
                    f"{name}: --require set but the mutated component "
                    f"'{component}' is absent at {component_file}; cannot validate "
                    f"committed verdict_captured_for_sha256."
                )
            else:
                on_disk_component = component_sha256(component_file)
                if on_disk_component != committed_component_sha:
                    failures.append(
                        f"{name}: COMMITTED verdict_captured_for_sha256 "
                        f"({committed_component_sha}) does NOT match the on-disk "
                        f"mutated-component ('{component}') hash "
                        f"({on_disk_component}). The committed mutated-component "
                        f"binding is inconsistent with the committed fixture bytes — "
                        f"a committed mutated-sha edit would be silently rebound by "
                        f"regeneration. Recompute it from the actual component bytes "
                        f"(issue #1294 Item 1 / roborev Finding 1)."
                    )
                else:
                    matched_a_binding = True

        if matched_a_binding:
            checked += 1

    if failures:
        sys.stderr.write(
            "\nFATAL: committed byte-binding(s) inconsistent with the committed "
            "bytes (issue #1294 roborev Findings 1 & 2):\n"
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
                "FATAL: --require set but no committed bindings could be "
                f"validated (binaries absent). {msg}\n"
            )
            return 1
        sys.stderr.write(f"[skip] {msg}\n")
        return 0

    print(
        f"validate-committed-dir-binding: {checked} fixture(s) with committed "
        f"binding(s) (dir-sha and/or mutated-component-sha) match the committed "
        f"bytes ({skipped} skipped, binaries absent)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
