## 1. Commission Cassandra-written corrupted fixtures

- [ ] 1.1 Add a fixture-commissioning script (extend the existing `generate-corruption-corpus.sh`
      convention) that writes a clean source SSTable via **Cassandra 5.0.2 Docker** flush, then applies one
      deterministic mutation per in-scope corruption class. Surface exercised: the fixture generator +
      `test-data/cassandra-parity-manifest.yml`.
- [ ] 1.2 Commit the mutation manifest (`{fixture, target_component, mutation, offset, expected_error_class,
      cassandra_verdict}`), SHA-256 sums, text components (`TOC.txt`, `Digest.crc32`), and a README; keep
      `*.db` binaries gitignored. Verify byte-for-byte regeneration matches committed SHAs in a clean checkout.
- [ ] 1.3 Capture each fixture's actual Cassandra 5.0.2 `nodetool verify --extended-verify` verdict and
      commit it as the parity oracle with `cassandra_version: 5.0.2` + matching `cassandra_git_sha`.

## 2. Parity test against the public verify surface

- [ ] 2.1 Add `sstable_parity_corruption_verify` test that calls `verify_sstable(dir, VerifyMode::Full, …)`
      (the `cqlite verify --mode full` call path — wiring-evidence) on each corrupted fixture and the clean
      baseline.
- [ ] 2.2 Assert class match (`VerifyReport.findings` contains the fixture's `expected_error_class`) AND
      verdict match (CQLite corrupt/clean agrees with the captured Cassandra verdict); clean baseline = zero
      findings.
- [ ] 2.3 Fixture-gate: skip-clean when binaries absent; FAIL present-but-wrong; FAIL on zero-evaluated;
      honor `CQLITE_REQUIRE_FIXTURES=1` + `CQLITE_DATASETS_ROOT`.

## 3. Promote the manifest scenario + CI

- [ ] 3.1 Promote `cass.corruption_verify.component_corruption_detection` `planned → mirrored` with the
      fixture-backed evidence block (`cassandra_version`, `cassandra_git_sha`, `storage_format_version`,
      `fixture_generation_command`, `comparison_command`, `reference_paths`, `failure_artifacts`); keep
      scope-honest `known_limitations` (out-of-order / negative-LDT / scrub remain out of scope).
- [ ] 3.2 Point `ci.workflow` at a real workflow that invokes the `comparison_command` — extend
      `.github/workflows/compression-corruption-parity.yml` to run the new parity test.
- [ ] 3.3 Regenerate `docs/reports/cassandra-test-parity.md`; run `cassandra-parity` lint to green.

## 4. Follow-ups (file, do not implement here)

- [ ] 4.1 File: scrub / recovery parity (`ScrubResult` good/bad/empty, out-of-order rewrite, skip-corrupted,
      TTL-overflow fix) — new write-path surface, design-driven.
- [ ] 4.2 File: add `VerifyErrorClass` for out-of-order key/row + negative/overflowed local-deletion-time,
      with fixtures, to extend Verifier parity.
- [ ] 4.3 File: consolidate `verify::verify_sstable` vs `reader/integrity::perform_integrity_check`.

## 5. Quality gates (definition of done)

- [ ] 5.1 `scripts/agent-gate.sh` PASS — paste the AGENT-GATE SUMMARY block verbatim.
- [ ] 5.2 Intent audit **C** (`spec-auditor` anchored to `openspec/changes/issue-1236-scrub-verify-parity/specs/**`)
      reports PASS — every requirement satisfied with public-surface test evidence.
- [ ] 5.3 roborev (`--agent codex --base origin/main`) clean.
- [ ] 5.4 PR opened; merge-on-green; then `flow-finalize` (archive change, sync specs, cleanup, close #1236).
