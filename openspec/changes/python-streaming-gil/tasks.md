# Tasks

## 1. Implementation (surface: `bindings/python/src/result.rs` `StreamingIterator`)
- [ ] 1.1 Change `inner: Mutex<QueryResultIterator>` → `inner: std::sync::Arc<std::sync::Mutex<QueryResultIterator>>` (field `~:441`) and the constructor (`~:472`) `Mutex::new(iter)` → `Arc::new(Mutex::new(iter))`.
- [ ] 1.2 Restructure `__next__` (`~:517-528`): clone the `Arc`, run `lock()` + `block_on(iter.next_async())` **inside** `py.allow_threads(move || ...)`; return a sentinel on lock-poison and convert to `PyRuntimeError` after re-acquiring the GIL. Keep `Row::from_core` / `finalize_span()` / `PyStopIteration` handling OUTSIDE the closure. Drop the guard before `finalize_span()` (it re-locks) as today.
- [ ] 1.3 No `unwrap()`/`expect()` added to library code; span guard NOT held across the released-GIL section.

## 2. Test (surface: `bindings/python/tests/test_streaming.py`)
- [ ] 2.1 Add `test_streaming_next_releases_gil` — counter-thread pattern: thread A streams a wide table (`test_wide_rows.<wide_table>`), thread B increments a counter tight-loop; assert B advances past a nonzero floor *during* A's iteration. Floor generous enough to avoid flake, strict enough to fail on `main`.
- [ ] 2.2 Use `_require_fixtures_strict()` (`conftest.py:64`) so present-but-unreadable datasets FAIL loudly, not skip.
- [ ] 2.3 Verify the test fails on unmodified `main` (revert src, re-run) and passes after the fix — value/behavior regression, not count-only.

## 3. Gate + audits (Definition of Done)
- [ ] 3.1 `scripts/agent-gate.sh` PASS (run with `CQLITE_DATASETS_ROOT=~/local_projects/cqlite/test-data/datasets`) — paste the AGENT-GATE SUMMARY block verbatim.
- [ ] 3.2 Build/run the Python streaming tests: `env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets RUN_SLOW_TESTS=1 pytest bindings/python/tests/test_streaming.py -v` (maturin develop first).
- [ ] 3.3 `RUSTFLAGS="-D warnings"` clean; file-size ratchet respected (split if a touched file crosses ~800L).
- [ ] 3.4 **C — spec-auditor** PASS against `openspec/changes/python-streaming-gil/specs/**` (every requirement satisfied with a public-surface test as evidence).
- [ ] 3.5 roborev clean (`/roborev-review-branch --base origin/main`, `--agent claude-code --model opus`), fix findings.

## 4. Land
- [ ] 4.1 Push branch, open PR (links #1441).
- [ ] 4.2 Merge on green (gate PASS + C PASS + roborev clean), `--squash --delete-branch`.
- [ ] 4.3 `flow-finalize 1441` — archive this change, remove worktree, delete origin lock, close issue.
