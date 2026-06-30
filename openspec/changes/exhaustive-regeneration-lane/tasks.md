# Tasks: exhaustive-regeneration-lane

## 1. Corpus-audit subcommand (audit surface)
- [ ] 1.1 Add a `cassandra-parity corpus-audit` subcommand (`tools/cassandra-parity/src/`) that takes the regenerated corpus root + `--manifest` + the test index, reusing `model::Manifest` and `coverage::analyze`. *Surface exercised:* `cargo run -p cassandra-parity -- corpus-audit --corpus <dir> --manifest test-data/cassandra-parity-manifest.yml`.
- [ ] 1.2 Implement the four failure classes (design D3): missing reference, stale reference, unclassified high-relevance file (reuse `coverage --strict`), and unexpected component change (presence/checksum vs expected manifest entry set). Exit non-zero naming the offending reference/component.
- [ ] 1.3 Implement the provenance check (design D4): assert the run's recorded Cassandra version/ref/git-sha matches the manifest's `cassandra_source` (+ `evidence.cassandra_version`/`cassandra_git_sha`); fail on a version the manifest does not declare. *Surface exercised:* the audit subcommand reading the provenance record.
- [ ] 1.4 Unit tests mirroring `tier-contract-check`: a clean fixture (corpus ↔ manifest agree) plus one drifted fixture per failure class (missing, stale, unclassified-high, unexpected-component, version-mismatch). No Docker/live Cassandra.

## 2. Provenance record (provenance surface)
- [ ] 2.1 Define the per-run provenance record (JSON/YAML) holding Cassandra version + source ref/git-sha, Docker image tag, the generator commands invoked, the `package_datasets.sh` asset name, and the asset SHA256. *Surface exercised:* the record file the workflow writes and the audit (§1.3) reads.
- [ ] 2.2 Wire `package_datasets.sh` (asset name) + a SHA256 step into the lane so the asset name and checksum are captured into the provenance record (no publish).

## 3. Regeneration workflow (CI surface)
- [ ] 3.1 Add `.github/workflows/exhaustive-regeneration.yml` (tier `exhaustive_regeneration`) with `workflow_dispatch` + a slow `schedule:` cron (design D5; cron value pending owner Q); no `pull_request` trigger. *Surface exercised:* the workflow file.
- [ ] 3.2 Orchestrate the three regeneration steps: `exhaustive.regenerate.all_formats` → `regenerate-datasets.sh`; `exhaustive.regenerate.test_deltas` → `generate-deltas.sh`; `exhaustive.regenerate.corruption_fixtures` → `generate-corruption-corpus.sh`.
- [ ] 3.3 Assert the corruption step covers all seven component types (Data.db, Index.db, Summary.db, Statistics.db, CompressionInfo.db, TOC.txt, Digest.crc32) — fail the lane if any is missing.
- [ ] 3.4 Run `corpus-audit` (`exhaustive.audit.manifest_coverage` + `exhaustive.audit.generated_references`) after regeneration; fail the lane on any audit error.
- [ ] 3.5 Upload ONE report artifact (provenance record + audit report + generator logs) via `actions/upload-artifact`; perform NO `git commit`/`git push` of regenerated binaries and NO release publish (design D6).

## 4. Manifest wiring
- [ ] 4.1 Ensure the five manifest entries the lane exercises (`exhaustive.regenerate.all_formats`, `exhaustive.regenerate.test_deltas`, `exhaustive.regenerate.corruption_fixtures`, `exhaustive.audit.manifest_coverage`, `exhaustive.audit.generated_references`) are present/consistent at tier `exhaustive_regeneration`; `cassandra-parity lint` stays green. *Surface exercised:* `test-data/cassandra-parity-manifest.yml`.

## 5. Doctrine + docs cross-link
- [ ] 5.1 Cross-link the lane + `corpus-audit` command from the `exhaustive_regeneration` section of `docs/development/parity-ci-tiers.md` and note them in `CLAUDE.md` (same-change doctrine rule).

## 6. Quality gate (definition of done)
- [ ] 6.1 Run `scripts/agent-gate.sh` (with `CQLITE_DATASETS_ROOT` → main repo `test-data/datasets`) and paste the AGENT-GATE SUMMARY block verbatim — must PASS.
- [ ] 6.2 Intent audit **C**: run `spec-auditor` anchored to `openspec/changes/exhaustive-regeneration-lane/specs/**` — every requirement `satisfied` with public-surface evidence (the workflow file, the `corpus-audit` tests, the provenance record). Must report PASS.
- [ ] 6.3 roborev: `/roborev-review-branch --base origin/main` clean (run `/roborev-fix` for findings).
- [ ] 6.4 Push branch, open PR linking #1026; do NOT merge (owner's Seam 2).
