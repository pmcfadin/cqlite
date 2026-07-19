# Design — BTI (da) end-to-end coverage through Flight do_get

## Decisions

### D1. Corpus: force-commit the small `test_da` BTI binaries (chosen)
The `test_da/{simple,collection,ttl,wide}_table` tables already exist locally with full `da-2-bti`
component sets, but the binaries are gitignored (`*.db` in `.gitignore:45`) and excluded from the
parity manifest (`cassandra-parity-manifest.yml:2077`). A stock CI checkout lacks them, so any test
pinned to them SKIPs — leaving the hole open exactly where it matters (CI).

**Chosen:** force-commit the BTI component sets with `git add -f`, the established pattern — **118
`.db` binaries are already tracked this way** (e.g. `test_big/wide_partition`, `test_comp_corrupt`).
Sizes are tiny: simple/collection/ttl are 16K each, wide_table is 1.9M (≈2.0M total). Commit the full
BTI component set per table (`Data.db`, `Partitions.db`, `Rows.db`, `Filter.db`, `Statistics.db`,
`CompressionInfo.db`, `Digest.crc32`, `TOC.txt`) so `do_get` opens a complete SSTable.

**Beat — re-cut the release tarball + bump `dataset-pin.env`:** correct but far heavier (regenerate +
repackage + re-upload the pinned asset + update the SHA256 pin) and orthogonal to this issue; the
force-commit path gives CI the exact same bytes with a reviewable diff and no release-asset dance.

**Beat — leave gitignored, gate skip-on-presence:** rejected by owner — the tests would SKIP in stock
CI and the coverage hole would stay open where it counts.

**Verification obligation (#1190 lesson):** the byte/parity assertions must be verified against the
**committed tree** (a fresh `git worktree add --detach HEAD`), not the dirty working tree — a dirty
tree masks a missing `git add -f`. The gate's `missing-fixtures` fail-closed (#2078) plus a
committed-tree check are the guard.

### D2. Test shape: mirror the three existing BIG tests, one shared path helper
Add BTI counterparts alongside the existing tests, driving real `do_get` over the transport:
- **point lookup** — mirror `point_read_corpus_parity_test.rs` against a `test_da` table with a known PK.
- **full scan** — mirror `do_get_transport_test.rs`; assert row-level parity vs the `da-2-bti-Data.db.jsonl`
  golden.
- **LIMIT-k** — bounded result assertion.
Extract a shared fixture-path helper (the three BIG tests each hardcode their own join today) so both
BIG and BTI fixtures resolve through one function. Gate skip-on-presence like the BIG tests — but
because D1 commits the binaries, the gate is satisfied in CI (0-rows-when-present stays a **failure**,
per the dataset doctrine).

### D3. Assert correct results under BTI fail-open, do not fix pruning here
The tests assert BTI `do_get` returns the **same rows/values/order** as the core reader for the same
query — i.e. correctness is independent of the missing Summary.db-based token-prune. They must NOT
assert that pruning happened (it doesn't for BTI — that's the fail-open the audit flagged). Making BTI
prunable is a deliberate Non-goal (perf follow-up). This pins the contract that matters: **fail-open
must never become fail-closed** (silently dropping BTI rows).

## Risk: the first run may be RED — that is the point
The audit (`issue-2363-coverage-matrix-audit.md:55-56`) warns the SummaryReader path "may be
functionally broken or fallback-dependent for BTI — the E2E test may be RED on first run." If the new
BTI `do_get` tests fail, that is a **discovered correctness bug**, not a test defect: triage it
(TDD — the failing test is the oracle), fix the fail-open→fail-closed defect if present, and land the
fix with the test. If instead they pass, the fail-open path is confirmed correct-but-unpruned and the
tests lock it in.

## Surfaces touched
- `cqlite-flight/tests/` — new BTI `do_get` integration tests + a shared fixture-path helper.
- `test-data/datasets/sstables/test_da/**` — force-commit the BTI binaries (`git add -f`).
- Possibly `cqlite-flight/src/producer.rs` (`sstable_token_span`/prune loop) **only if** the first run
  reveals a fail-closed defect — otherwise untouched.
