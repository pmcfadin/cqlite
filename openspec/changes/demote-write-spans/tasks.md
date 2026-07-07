# Tasks — demote-write-spans

> BLOCKED until #1706 merges (shared `info!` lines in cqlite-core/src/query).

## 1. Fail-first tests (TDD)
- [ ] 1.1 Counting-subscriber span test: subscriber at INFO, write N mutations,
      assert INFO span count O(1) not ≥3N. Confirm RED on current tree.
- [ ] 1.2 SELECT info-count test: subscriber at INFO, one SELECT, assert ≤1 info
      line. Confirm RED on current tree.

## 2. Demote spans
- [ ] 2.1 Add `level = "debug"` to the write-side/compaction `#[tracing::instrument]`
      attrs (22 sites in cqlite-core/src/storage). Names/keys/skips unchanged.
      (Owner-selected scope: uniform-all default, or hot-only if chosen at Seam 1.)

## 3. Demote SELECT chatter (AFTER #1706 merges; rebase first)
- [ ] 3.1 Demote the ~8 `tracing::info!` sites in cqlite-core/src/query to
      `debug!`, leaving ≤1 info line per SELECT. Content unchanged.

## 4. Subscriber-on bench
- [ ] 4.1 Add a subscriber-on variant to the overhead gate (fmt subscriber at
      INFO); record its number alongside the subscriber-less one. Advisory-first.

## 5. Gate + evidence
- [ ] 5.1 `scripts/agent-gate.sh` PASS — paste SUMMARY in the PR.
- [ ] 5.2 PR states the demotion scope taken (uniform vs hot-only) + the two
      overhead numbers.

## Non-goals
- No span deletion; no name/key changes; no message content edits; read side untouched.
