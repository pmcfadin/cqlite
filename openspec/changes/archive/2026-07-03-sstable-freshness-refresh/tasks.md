# Tasks: sstable-freshness-refresh

> All tasks start **after** owner spec approval (Seam 1). TDD: each functional task
> lands its failing test first. Each task names the public surface it exercises.

## 1. Core refresh mechanism (surface: `Database::refresh()` / `StorageEngine`)
- [ ] Failing integration test (real corpus binaries, `CQLITE_DATASETS_ROOT`):
      stale-until-refresh — copy a second generation in; same SELECT unchanged before
      `refresh()`, includes the new partition after; report `readers_added == 1`.
      Must fail (not skip) on absent-but-expected datasets.
- [ ] `SSTableManager::refresh_tables()` — rediscover via the same discovery `open`
      uses; open added generations **outside** the write guard; diff-and-swap keyed by
      canonical Data.db path under the write guard; keep unchanged `Arc`s
      (`Arc::ptr_eq` no-op test); return `RefreshReport`. No `unwrap`/`expect`.
- [ ] `StorageEngine::refresh()` + `Database::refresh()` public API (async), rustdoc
      stating the contract (snapshot-at-open, explicit refresh, in-flight isolation).
- [ ] Removal test: delete one of two generations → `readers_removed == 1`, subsequent
      SELECT correct, no panic.
- [ ] Atomicity test: corrupt `Statistics.db` in the new generation → typed error,
      pre-refresh result set fully intact (#1626 posture inherited).
- [ ] No-op test: unchanged dir → zero-delta report, reader `Arc`s pointer-identical.

## 2. Concurrency (surface: streaming scan + `refresh()` overlap)
- [ ] Test: long/streaming scan overlapped with a refresh completes with exactly the
      pre-refresh result; post-refresh query sees the new set. Content assertions only
      — no wall-clock/timing assertions (telemetry-retro recurring flake class).

## 3. Bindings (surface: `db.refresh()` Python / `await db.refresh()` Node)
- [ ] Python: pyclass `RefreshReport`, GIL released during the async op; `__init__.pyi`
      stub updated; e2e test drives stale→refresh→fresh via `db.execute`.
- [ ] Node: napi method + TS definition (`readersAdded` etc.); Jest e2e test, same cycle.

## 4. Docs + posture record (surface: user docs site, issue #1477)
- [ ] "Read surfaces and freshness" docs page: three-surface contract table,
      torn-window semantics, auto-refresh explicitly a non-goal follow-up.
- [ ] Limitations page cross-link.
- [ ] Post the recorded Flight torn-window posture (Decision 1) on #1477 as a
      consumed-decision comment.

## 5. Quality gates (in order; all in this worktree)
- [ ] Minimal-features build compiles (refresh not `state_machine`-gated; watch the
      ungated-test-item CI gotcha).
- [ ] `scripts/agent-gate.sh` PASS — paste the AGENT-GATE SUMMARY block verbatim
      (`AGENT_GATE_SUMMARY_FILE` set; `CQLITE_DATASETS_ROOT` pointed at the main
      checkout's datasets from this worktree).
- [ ] **C intent audit**: `spec-auditor` anchored to
      `openspec/changes/sstable-freshness-refresh/specs/**` — every requirement
      `satisfied` with a public-surface test as evidence.
- [ ] roborev clean (pre-empt recurring classes: no wall-clock races, no
      `manual_range_contains`, no heuristics).
- [ ] PR referencing #1749; merge per autonomy model; then `flow-finalize`
      (archive change, remove worktree/branch, close issue, telemetry record).
