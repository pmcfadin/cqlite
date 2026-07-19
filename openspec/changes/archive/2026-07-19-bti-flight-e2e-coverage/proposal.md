# BTI (da) end-to-end coverage through Flight do_get

## Milestone
0.15 (cqlite-trino latency/throughput/operations theme, epic #2403). **Design-driven** — this change
adds a new public-facing test surface (Flight `do_get` integration tests over the BTI corpus) and
changes the shipped test corpus, so it goes through OpenSpec rather than the oracle-driven bug path.

Closes #2372. Source of truth for the hole: `docs/architecture/issue-2363-coverage-matrix-audit.md`
(BTI axis, HOLE-BTI-1).

## Why (confirmed hole)
The #2363 coverage-matrix audit found that BTI (`da`) is well covered at the core reader layer but has
**zero end-to-end coverage through the Flight `do_get` path**:

- Every `cqlite-flight` integration test hardcodes a BIG (`nb-*-big`) fixture —
  `do_get_transport_test.rs:311`, `point_read_corpus_parity_test.rs:197`,
  `collection_collapse_parity_test.rs:257`. Each rolls its own fixture path and gates on
  `nb-1-big-Data.db`.
- The only BTI reference in the entire crate is a byte-math LRU unit test (`warm/budget.rs:143`) that
  writes empty stub files and never opens or reads a BTI SSTable.
- No `sstable_format`/BTI reference exists anywhere under `trino-connector/` or `easy-db-lab-kits/`;
  the testbed provisions stock `cassandra:5.0`, which writes BIG.

**Latent structural risk this exposes.** Flight boundary-key resolution derives a `-Summary.db` sibling
and calls `SummaryReader::open` (`producer.rs:289-319`). Summary.db is **BIG-only** — a BTI table has
none — so the call returns `None` and the token-prune loop (`producer.rs:766-772`) maps `None => true`:
a BTI table is **never token-pruned** (fail-open). The warm path is analogous (`endpoint_tokens` is
`None` for BTI — `reader/mod.rs:901-910`, `producer_warm.rs:216`). Results are correct; pruning is
silently absent. **Neither fail-open branch is exercised E2E** — so a regression that turned fail-open
into fail-closed (dropping BTI rows) would ship green. The new tests pin the correct behavior:
BTI `do_get` returns the same rows as the core reader, pruning-absence notwithstanding.

The reader layer itself is format-agnostic: `SSTableReader::open` auto-detects `nb`/`da` from the
filename tag (`VersionGates::from_path`, `reader/mod.rs:504`) and routes BTI reads through
`reader/data_access/bti.rs`. So the gap is purely that no Flight path ever feeds it a real `da`
SSTable — not a missing capability.

## What ships (scope — owner-approved 2026-07-19)
1. **Flight `do_get`-over-transport integration tests pinned to the `test_da` BTI corpus** — point
   lookup, full scan, and LIMIT-k, over the real transport, asserting row-level parity against the
   committed `da-2-bti-Data.db.jsonl` goldens. Mirrors the three existing BIG tests.
2. **Make the BTI corpus CI-real.** The `test_da/{simple,collection,ttl,wide}_table` BTI binaries exist
   locally but are gitignored, and `cassandra-parity-manifest.yml:2077` excludes them — so a stock CI
   checkout lacks them and any new test would SKIP. Force-commit the small BTI component sets
   (`git add -f`, the established #1190 pattern — 118 `.db` binaries are already tracked this way) so
   the tests **execute** in CI, not skip.
3. **A shared Flight-test fixture-path helper** so the four tests (three existing BIG + the new BTI)
   resolve corpus paths one way instead of four hardcoded copies.

## Non-goals (filed as follow-ups)
- **Trino-testbed BTI provisioning** (docker `sstable_format: bti` + a BTI table in the e2e CQL +
  `e2e-test.sh` coverage). Heavier, docker-dependent, flakier — separate issue.
- **The token-prune fail-open optimization** (giving BTI a real boundary-key source via `Partitions.db`
  so BTI tables get token-pruned instead of fail-open). This change *pins* the correct-results contract
  under fail-open; making BTI prunable is a perf follow-up — separate issue.
- Changing the reader/core BTI decode path (already covered at core).

## Doctrine impact
None. No new public API, no CLAUDE.md/website doctrine change. Reinforces the wiring-evidence rule
(a feature is done only when its public surface — here Flight `do_get` — exercises it E2E) and the
parity-is-truth rule (assert against the committed sstabledump JSONL goldens).
