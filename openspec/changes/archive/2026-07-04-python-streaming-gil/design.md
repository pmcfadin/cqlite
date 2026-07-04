# Design

## Problem restated

`StreamingIterator.__next__` (`bindings/python/src/result.rs:517-528`) blocks on
`block_on(iter.next_async())` while holding both the GIL and a `MutexGuard<QueryResultIterator>`. The
guard is `!Send`, so the usual fix — wrap the blocking work in `py.allow_threads(closure)` — is rejected
by the compiler because the closure must be `Send`/`Ungil`. That single borrow constraint is *why*
streaming is the only blocking binding path that never releases the GIL.

## Options considered

### Option A — `Arc<Mutex<QueryResultIterator>>`, lock **inside** the released-GIL closure  ✅ CHOSEN
Change `inner: Mutex<QueryResultIterator>` → `inner: Arc<Mutex<QueryResultIterator>>`. In `__next__`,
clone the `Arc` (cheap, `Send`), then inside `py.allow_threads(move || { let mut it = arc.lock()?;
block_on(it.next_async()) })` acquire the guard and run the blocking `recv`. Only the `Send` `Arc`
crosses the boundary; the `!Send` guard lives and dies *inside* the closure. `Row` construction, span
finalization, and `PyErr` conversion happen after the GIL is re-acquired.

- **Pros:** smallest diff; entirely contained in `bindings/python/src/result.rs`; **no cross-crate
  change**; mirrors the exact `allow_threads(|| block_on(..))` shape `execute` already uses
  (`database.rs:279-282`); GIL still serializes Python-side access to one iterator, so `rows_received`
  and the receiver stay single-threaded.
- **Cons:** `Arc` clone per `__next__` (one atomic inc/dec per row) — negligible vs a disk `recv`;
  the lock is functionally uncontended (the GIL already single-threads a given iterator), so the
  `Mutex` is belt-and-suspenders, not a hot path.

### Option B — new `Send`-future / `receiver_mut()` accessor on core `QueryResultIterator`
Add e.g. `pub fn next_future(&mut self) -> impl Future<Output=..> + Send + '_` or
`pub fn receiver_mut(&mut self) -> &mut mpsc::Receiver<..>` to `cqlite-core/src/query/result.rs`, so the
binding drives the blocking `recv` on the `Send` receiver half under `allow_threads`.

- **Pros:** exposes an explicitly-`Send` handle; no `Arc` wrapper in the binding.
- **Cons:** widens the **public core API surface** for a binding-only concern; the receiver is already
  reachable via the guard, so the extra surface earns nothing Option A can't do internally. Rejected as
  gratuitous public surface (semver liability) for zero functional gain.

## Decision

**Option A.** It restores the invariant "every blocking FFI path releases the GIL" with the minimum
public surface — zero cross-crate change — and reuses the binding's own established
`allow_threads(|| block_on(..))` pattern. The `!Send` guard constraint is resolved by moving *where* the
lock is taken (inside the `Send` closure), not by adding new core API.

**Invariant preserved (binding contract, applies whichever shape the code lands on):** the blocking
`receiver.recv().await` MUST execute inside `py.allow_threads(...)` with the GIL released, and the
iterator's `rows_received`/receiver state MUST remain consistent and accessed by one thread at a time
(the GIL guarantees single-threaded Python access to a given iterator).

## Correctness & safety notes

- **No `unwrap`/`expect` added.** A poisoned lock maps to a sentinel inside the closure and is converted
  to `PyRuntimeError` after the GIL is re-acquired (cannot build a `PyErr` without `py`).
- **Span discipline:** the tracing span guard is NOT held across the released-GIL section; the
  exhaustion branch drops the iterator guard before `finalize_span()` (which re-locks), exactly as today.
- **Semantics unchanged:** same rows, same order, same `rows_received`; only the GIL-hold window shrinks.

## Test strategy

`test_streaming_next_releases_gil` (counter-thread pattern): thread A iterates a wide table via
`db.execute_streaming("SELECT * FROM test_wide_rows.<wide_table>")`; thread B spins a tight
counter-increment loop; assert B advances past a generous-but-nonzero floor *during* A's iteration. On
unmodified `main` B makes ~no progress while A blocks; after the fix B progresses. Correctness
regression: existing streaming tests still pass (rows/order/`rows_received`). Dataset rule: reuse
`_require_fixtures_strict()` (`conftest.py:64`) so a present-but-unreadable root FAILS loudly, never skips.
Run: `env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets RUN_SLOW_TESTS=1 pytest bindings/python/tests/test_streaming.py -v`.
