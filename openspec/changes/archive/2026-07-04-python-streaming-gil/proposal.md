## Why

The Python streaming iterator's `__next__` is the **one** blocking FFI entry point that does not
release the GIL. Every other blocking path in the PyO3 binding (`execute`, `execute_streaming` setup,
`export_parquet`, `prepare`, `stats`, `open`) already wraps its blocking work in `py.allow_threads(...)`;
streaming is the outlier. When the streaming iterator refills its bounded mpsc buffer it blocks on disk
I/O **while holding the GIL** (`bindings/python/src/result.rs:517-528`), so every other Python thread
freezes for that disk latency. This throws away the concurrency issue #815 bought in core (independent
per-scan `ScanCursor`s) and silently re-serializes multi-threaded readers at the FFI boundary.

The reason streaming is the lone hold-out is a concrete borrow-checker constraint, not an oversight:
`__next__` holds a `std::sync::MutexGuard<QueryResultIterator>` across `block_on`, and a `MutexGuard`
is `!Send`, so it cannot be moved into the `Send`-requiring `py.allow_threads` closure. The `receiver`
*inside* the iterator is `Send`; the guard around it is not.

- **Milestone:** Release: bug-clear (pre-v0.13) — P0 concurrency correctness (parent epic #1432,
  bindings/FFI audit `docs/reports/bindings-ffi-performance-audit-2026-07-01.md`).
- **Design-driven:** the fix requires a small cross-crate API decision (how the binding drives the
  blocking `recv` under a released GIL without moving a `!Send` guard) — reviewed as a public core
  surface. No Cassandra SSTable format oracle is decoded here.
- Adds a new `binding-streaming-concurrency` capability.

## What Changes

- **Release the GIL around the blocking streaming `recv`.** Restructure Python `StreamingIterator.__next__`
  so the blocking `receiver.recv().await` executes **inside** `py.allow_threads(...)` (GIL released),
  while the iterator's state (`rows_received`, receiver) stays single-threaded (the GIL already
  serializes Python-side access to a given iterator). `Row` construction, span finalization, and error
  conversion stay outside the closure (they need `py`).
- **Make the iterator shareable across the closure boundary.** Change the field from
  `Mutex<QueryResultIterator>` to `Arc<Mutex<QueryResultIterator>>` so the lock is acquired *inside* the
  `Send` closure (only the `Send` `Arc` is captured) rather than held across it. `Arc<Mutex<..>>` is
  `Send` because `QueryResultIterator: Send`.
- **Add a GIL-release regression test** (`test_streaming_next_releases_gil`) using the counter-thread
  pattern: thread A streams a wide table, thread B increments a counter in a tight loop; assert thread
  B makes a progress floor of increments *during* A's iteration. Fails on `main` (GIL held), passes
  after the fix. The test fails loudly (not skips) when datasets are present-but-unreadable.

## Non-goals

- **Node streaming** GIL/loop behavior — that is #1443/#1442.
- **Write-path GIL hold** (`Python DML/flush across WAL fsync`) — that is #1444.
- Any change to buffering/backpressure semantics of the bounded mpsc channel — the audit confirmed
  backpressure is honest; only *where* the block happens relative to the GIL moves.
- Any change to streaming row values, order, or `rows_received` accounting.

## Doctrine impact

None to the published doctrine. This is an internal FFI-concurrency correctness fix; it strengthens the
"every blocking FFI path releases the GIL" invariant the rest of the binding already follows.
