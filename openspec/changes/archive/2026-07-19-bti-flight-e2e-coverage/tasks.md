# Tasks — BTI (da) end-to-end coverage through Flight do_get

## 1. Make the BTI corpus CI-real
- [ ] 1.1 Force-commit the `test_da/{simple,collection,ttl,wide}_table` BTI component sets
      (`git add -f test-data/datasets/sstables/test_da/**/da-2-bti-*`), copying the real binaries from
      the main checkout's `test-data/datasets` into the worktree first. Surface exercised: the tracked
      dataset tree / `git ls-files`.
- [ ] 1.2 Confirm each committed table has a complete component set (`Data.db`, `Partitions.db`,
      `Rows.db`, `Filter.db`, `Statistics.db`, `CompressionInfo.db`, `Digest.crc32`, `TOC.txt`) and the
      existing `da-2-bti-Data.db.jsonl` golden.

## 2. Shared fixture-path helper
- [ ] 2.1 Add a shared helper in `cqlite-flight/tests/` that resolves a corpus table dir + component
      path from `CQLITE_DATASETS_ROOT` (both BIG and BTI). Surface: `cqlite-flight` test support.
- [ ] 2.2 Refactor the three existing BIG tests (`do_get_transport_test.rs`,
      `point_read_corpus_parity_test.rs`, `collection_collapse_parity_test.rs`) to resolve fixtures via
      the helper — no behavior change, just path resolution.

## 3. BTI do_get integration tests (TDD)
- [ ] 3.1 **Full scan** — `do_get`-over-transport against a `test_da` BTI table; assert row-level
      parity vs `da-2-bti-Data.db.jsonl`; assert rows > 0. Surface: Flight `do_get`.
- [ ] 3.2 **Point lookup** — `do_get` for a known PK; assert rows match the golden for that key.
- [ ] 3.3 **LIMIT-k** — `do_get` with LIMIT; assert ≤ k rows, each matching the golden.
- [ ] 3.4 **Fail-open pruning** — assert every golden row is returned despite no `-Summary.db`
      (no assertion that pruning occurred). Surface: `producer.rs` token-prune path via `do_get`.
- [ ] 3.5 Gate all four skip-on-presence like the BIG tests; because task 1 commits the binaries, they
      execute in CI. Keep 0-rows-when-present a hard failure.

## 4. If the first run is RED (expected-possible)
- [ ] 4.1 Triage per the audit warning: a real `do_get` BTI correctness failure (e.g. fail-open turned
      fail-closed) is a discovered bug — fix it in `cqlite-flight/src/producer.rs` (and/or the warm path)
      with the failing test as the oracle. If green, record that fail-open is confirmed correct-but-unpruned.

## 5. Verify & certify
- [ ] 5.1 Each fix round: `AGENT_GATE_SUMMARY_FILE=... bash scripts/agent-gate.sh --lite > gate.log 2>&1`
      (summary-file redirect), with `CQLITE_DATASETS_ROOT` at the main repo's `test-data/datasets`.
- [ ] 5.2 Verify parity assertions against the **committed tree** (`git worktree add --detach HEAD`),
      not the dirty tree (#1190 lesson) — catches a missing `git add -f`.
- [ ] 5.3 Review-first: `rust-reviewer` + roborev on the lite-green diff before any full gate.
- [ ] 5.4 Open PR (`Closes #2372`). Then flow-closer: ONE full `agent-gate.sh` → C (spec-auditor,
      anchored to `openspec/changes/bti-flight-e2e-coverage/specs/**`) → final roborev → merge-on-green →
      finalize.

## 6. File follow-ups at merge time
- [ ] 6.1 Trino-testbed BTI provisioning (docker `sstable_format: bti` + BTI table in e2e CQL +
      `e2e-test.sh`).
- [ ] 6.2 Token-prune fail-open optimization: give BTI a real boundary-key source (`Partitions.db`) so
      BTI tables get token-pruned instead of fail-open.
