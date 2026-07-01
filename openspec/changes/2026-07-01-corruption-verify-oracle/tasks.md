# Tasks: corruption-verify-oracle (core)

## 1. Retain the converged core (already on branch — verify, do not rewrite)
- [ ] Confirm `generate-corruption-corpus.sh` full-directory binding (`fixture_dir_sha256`,
      `verdict_captured_for_dir_sha256`) is intact.
- [ ] Confirm `cqlite-core/tests/sstable_parity_corruption_verify.rs` validates the full-dir binding for
      byte-stable fixtures, matches captured verdicts per class, and fails closed on empty fixtures.
- [ ] Confirm `sha2 = "0.10"` is under `[dev-dependencies]` only.
- [ ] Confirm `corruption-manifest.yml` carries both `verdict_captured_for_dir_sha256` and
      `verdict_captured_for_sha256`.

## 2. Remove the contested PR-time guard (unblock the two red checks)
- [ ] Remove the two guard steps from `.github/workflows/compression-corruption-parity.yml`
      ("committed byte-bindings match on-disk bytes" and "committed corruption oracle must not be silently
      regenerated") and the associated `paths:` addition for `extract-corruption-oracle.py`.
- [ ] Remove the PR-lane-only guard wiring from `.github/workflows/exhaustive-regeneration.yml` added by
      this branch.
- [ ] Remove `test-data/scripts/validate-committed-dir-binding.py` and
      `test-data/scripts/extract-corruption-oracle.py` from this PR (preserved in branch history; #1373
      lifts them into the nightly lane).
- [ ] Trim any generator code that exists solely to feed the removed CI guard (keep the full-dir binding).

## 3. Validate
- [ ] `bash scripts/agent-gate.sh` (with `CQLITE_DATASETS_ROOT` → main repo datasets) → PASS; paste the
      AGENT-GATE SUMMARY block.
- [ ] Push; confirm "Compression / corruption parity" and "Dependency isolation + correctness + overhead"
      PR checks go green.
- [ ] spec-auditor (C) anchored to `openspec/changes/2026-07-01-corruption-verify-oracle/specs/**` → PASS.
- [ ] roborev `/roborev-review-branch --base origin/main` → clean.

## 4. Finalize
- [ ] Merge on green (gate PASS + C PASS + roborev clean).
- [ ] `openspec archive 2026-07-01-corruption-verify-oracle`; remove worktree/branch; close #1294 with a
      comment pointing to #1373 for acceptance #2.
