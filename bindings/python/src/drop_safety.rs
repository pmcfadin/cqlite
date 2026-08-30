//! Best-effort cleanup when a `Database` handle is dropped without `close()`
//! (issue #1461).
//!
//! `Database::close()` does three things: closes the write engine (which
//! flushes any remaining memtable to a real SSTable), shuts down the read-side
//! storage engine, and force-flushes buffered telemetry. A handle that is
//! garbage-collected without `close()` — plenty of real Python code neither
//! uses `with` nor a `finally:` — used to skip all three, because `Database`
//! had no `Drop`. This module is that safety net, and nothing else: it changes
//! no `close()` semantics and adds no new cleanup policy.
//!
//! ## Why this lives in its own file
//!
//! `database.rs` is already over the campsite file-size threshold, so the hook
//! goes here and reads `Database`'s `pub(crate)` fields. `impl Drop` for a type
//! declared in a sibling module of the same crate is legal Rust; the coherence
//! rule is per-crate, not per-module.
//!
//! ## Panic-freedom is STRUCTURAL, not `catch_unwind`
//!
//! The release profile is `panic = "abort"`, so a panic here is a process
//! abort and `catch_unwind` would catch nothing. There is therefore no
//! `catch_unwind` anywhere below: every operation that *could* panic is instead
//! replaced by a fallible/non-panicking form, and the guard is the choice of
//! API rather than a recovery handler.
//!
//! * **`block_on` re-entrancy** — `Runtime::block_on` panics when called from
//!   inside a runtime context ("cannot block the current thread from within a
//!   runtime"). `Handle::try_current()` answers whether we are in one, so a
//!   re-entrant drop skips cleanup and returns.
//! * **Runtime construction** — this path calls
//!   [`crate::runtime::existing_runtime`] (a plain `OnceLock::get`), never
//!   `try_get_runtime`. Building a multi-threaded runtime during interpreter
//!   finalization is the hazard issue #1461 step 3 forbids. `None` ⇒ no async
//!   cleanup was ever possible in this process, so skip.
//! * **The write-engine mutex** — the field is a `tokio::sync::Mutex`, whose
//!   `blocking_lock()` *panics* in an async context. `try_lock()` cannot panic,
//!   so contention degrades to a skipped step instead.
//! * No `unwrap()`, `expect()`, slicing, or arithmetic appears below, so there
//!   is no implicit panic site left.
//!
//! ## No GIL is taken
//!
//! `Drop` runs without a `Python<'_>` token and this code deliberately never
//! acquires one: no `Python::with_gil`, and no `py.allow_threads` (there is no
//! token to call it on). None of the three cleanup steps touch a Python object
//! — they are plain Rust calls on `cqlite_core` handles — so a GIL is not
//! needed. Keeping the GIL surface at exactly zero removes a failure mode
//! instead of guarding it.
//!
//! **The known cost, stated because it is real: this cleanup runs with the GIL
//! HELD.** CPython frees a pyclass from its deallocator while the GIL is held,
//! so the flush + fsync + shutdown below block every other Python thread for
//! their duration — where an explicit `close()` releases the GIL around the
//! same work via `py.allow_threads`. Reviewers have raised this twice, so the
//! answer is recorded here rather than re-argued: **releasing the GIL requires
//! first ACQUIRING a token, and every mechanism for that can PANIC in `drop`,
//! which is a process ABORT under `panic = "abort"`.** In pyo3 0.23.5,
//! `Python::with_gil` panics "If the `auto-initialize` feature is not enabled
//! and the Python interpreter is not initialized" (`src/marker.rs`, and this
//! crate does NOT enable `auto-initialize`), which is precisely the
//! interpreter-teardown case; and GIL acquisition panics outright with "Access
//! to the GIL is prohibited while a `__traverse__` implmentation is running"
//! (`src/gil.rs`, `LockGIL::bail`), which a handle freed during a GC traversal
//! can reach. `Python::assume_gil_acquired` is not an escape either: it is
//! `unsafe` and its precondition is FALSE whenever the last reference is
//! released from a Rust thread holding no GIL.
//!
//! So the trade is a bounded stall on the *implicit* cleanup path versus a
//! possible abort — and abort is the exact failure class this issue exists to
//! prevent. The mitigation is the one the class docs already recommend:
//! `close()`, or the `with` block, both of which release the GIL properly. This
//! hook is the net for code that forgot, not the recommended path.
//!
//! ## Why native `Drop` and not `__del__`
//!
//! Native `Drop` is the reliable hook: it runs whenever the Rust object is
//! freed, including at interpreter shutdown. A `#[pymethods] __del__` is
//! optional sugar and is deliberately NOT added — CPython does not guarantee
//! `__del__` runs for a pyclass that does not define one, so it would add no
//! coverage, and a second cleanup entry point is one more thing that has to
//! stay consistent with the `AtomicBool`.
//!
//! ## Alignment with cqlite-core's own durability contract
//!
//! `cqlite_core::Database::close` documents that "`Drop` is NOT a flush — Tokio
//! has no async drop, so dropping a handle cannot await a flush and any
//! un-flushed writer state is left to recovery (WAL replay)"
//! (`cqlite-core/src/lib.rs`, issue #1693). This module does not contradict
//! that: a *synchronous* binding may block, so on the normal path it drives the
//! flush to completion via `block_on` rather than leaving it to recovery. And
//! where it CANNOT block safely (the re-entrancy branch), it falls back to
//! exactly the outcome core specifies — un-flushed state left in the WAL for
//! replay. The two documents agree; only the mechanism differs.
//!
//! ## LIMITATION — a FAILED `close()` disables this safety net
//!
//! `closed` records that cleanup *started*, not that it *completed*:
//! `close()` swaps the flag before its fallible steps and returns early on the
//! first error. So a `close()` whose write-engine flush fails leaves the flag
//! set with the write-engine close never completed, and this `Drop` then skips
//! the engine teardown. Note precisely what is and is not lost: the read-side
//! `shutdown()` call is skipped but is today a documented no-op, so nothing is
//! lost there; the unflushed memtable stays in the WAL, which is replayable;
//! the telemetry flush is skipped.
//!
//! This is NOT a regression — before this module a dropped handle ran nothing at
//! all — and it is deliberately not fixed here, because both available fixes are
//! forbidden by issue #1461's own "Do NOT" list: resetting the flag on failure
//! would *change `close()`'s semantics*, and a separate completion flag would
//! stop the `AtomicBool` being *the single source of "already cleaned up"*. The
//! flag lying after a failed `close()` is really a `close()` defect that
//! predates this issue; it belongs in its own change.
//!
//! ## Flush POLICY is out of scope
//!
//! Whether a dropped *writable* handle should silently flush unwritten data or
//! emit a warning is a write-path policy call owned by the N–T write-path
//! epics; this module deliberately reproduces exactly the behavior `close()`
//! has today (it flushes). Refine the warn-vs-flush choice there, not here.
//!
//! ## Coverage
//!
//! The executing lane for this file is the pytest tier
//! (`bindings/python/tests/test_drop_safety.py`, run by the gate's
//! `python-bindings` component and by `--lite`'s python tier). There are no
//! Rust unit tests in this crate's `--lib` target on purpose: `cqlite-py` is a
//! pyo3 `cdylib` built with `extension-module`, so `cargo test -p cqlite-py`
//! cannot link libpython and never runs (the gate documents this exclusion).
//! A Rust test added here would execute nowhere.
//!
//! **DECLARED GAP — the re-entrancy branch (step 2) is covered NOWHERE.** It is
//! unreachable from the only lane that executes this file: a pytest thread is
//! never inside a tokio runtime context, so `Handle::try_current()` is always
//! `Err` there and the branch is never taken. Reaching it needs CPython
//! embedded in a tokio application (or a pyo3 callback invoked on a runtime
//! worker thread), which nothing in this repository builds, and the crate has
//! no Rust test target that could construct one. The runtime-absent branch
//! (step 3) is equally unexercised: any `Database` that exists at all was
//! opened through `block_on`, so the runtime is always already built. Both are
//! stated rather than left implicit, because an untested branch whose absence
//! is unmentioned reads as a covered one — and these two are precisely the
//! branches whose failure mode is a process abort.
//!
//! Completing that census by the same standard: the `try_lock()` `Err` arm and
//! both `Err(err)` log arms are likewise unexercised and, as far as can be
//! determined, UNREACHABLE today — pyo3 holds a strong reference to the
//! `Database` for the duration of any `&self` pymethod, so no other caller can
//! be inside `close()`/`with_write_engine` on this object while its `Drop` runs,
//! and the engine `Arc` is never handed to another Python object. They stay as
//! defence in depth, not as covered code.

use std::sync::atomic::Ordering;

use tokio::runtime::Handle;

use crate::database::Database;
use crate::runtime::existing_runtime;

impl Drop for Database {
    fn drop(&mut self) {
        // (1) There are TWO cases here, and collapsing them was a real defect
        // (rust-reviewer B1). The `closed` flag is not private to cleanup: it is
        // the SAME `Arc<AtomicBool>` every `StreamingIterator` this handle
        // handed out observes (issue #1462), so flipping it makes those
        // iterators raise `RuntimeError` from `__next__`. That is right when
        // real teardown happened, and pure loss when none did.
        //
        // For a READ-ONLY handle none does: `write_engine` is `None`, and
        // `StorageEngine::shutdown()` is a documented no-op ("Nothing to
        // shutdown - read-only storage layer",
        // `cqlite-core/src/storage/mod.rs`). Claiming the flag there would buy
        // nothing and would silently break a pattern that works today —
        // `return db.execute_streaming(...)` from a function, letting `db` drop
        // at return, then iterating. An explicit `close()` invalidating an
        // iterator is a user stating intent; a GC pass is not.
        //
        // So: claim the flag only where there is teardown to guard, and
        // otherwise READ it without claiming. If `StorageEngine::shutdown` ever
        // becomes real teardown, this condition MUST change with it — the
        // coupling is deliberate and stated rather than silent.
        if self.write_engine.is_some() {
            if !self.closed.swap(true, Ordering::SeqCst) {
                self.drop_teardown();
            }
        } else if !self.closed.load(Ordering::SeqCst) {
            // Read-only, not already closed: nothing to tear down, but the
            // telemetry this handle's queries buffered should still be
            // exported. `load` rather than `swap` is the whole point —
            // outstanding iterators stay usable, exactly as before this module.
            crate::observability::flush();
        }
    }
}

impl Database {
    /// The engine half of the drop cleanup: everything that must happen at most
    /// once, and only when this drop won the `closed` race.
    ///
    /// Takes `&self` — nothing here mutates through the reference (`swap`,
    /// `try_lock` and `block_on` all take `&`).
    fn drop_teardown(&self) {
        // (2) Re-entrancy guard. `Runtime::block_on` PANICS when the calling
        // thread is already inside a runtime context, and a panic in `drop`
        // aborts the process under `panic = "abort"`. If we are inside a
        // runtime there is no safe way to drive the async cleanup from here, so
        // skip it silently. The flag is already set, so the handle is
        // consistently "closed" either way.
        //
        // Skipping is not merely the lesser evil, and the tempting alternative
        // is WORSE. Dispatching the cleanup onto the live runtime
        // (`Handle::spawn`) would not panic — but a detached task is
        // CANCELLABLE, and it would be cancelled at runtime shutdown, which is
        // exactly when an interpreter-teardown drop tends to happen. The flush
        // writes its SSTable components DIRECTLY into the published data
        // directory: only the compaction path stages into a tmp dir and
        // republishes by rename behind a TOC barrier (see
        // `cqlite-core/src/storage/sstable/writer/finish.rs`). So a cancelled
        // flush can leave a PARTIAL, DISCOVERABLE SSTable with no publication
        // barrier. Skipping instead leaves the WAL (`write_dir/wal/`) intact,
        // which is the write engine's designed idempotent replay marker — a
        // recoverable state, where a torn SSTable is not. Trading a valid
        // replay marker for a possibly-truncated published SSTable is a bad
        // trade even when it flushes successfully most of the time.
        if Handle::try_current().is_ok() {
            tracing::debug!(
                "cqlite: Database dropped inside a tokio runtime context; \
                 skipping teardown cleanup (block_on would panic)"
            );
            return;
        }

        // (3) Runtime availability. Never build one here — see the module docs.
        let Some(runtime) = existing_runtime() else {
            tracing::debug!(
                "cqlite: Database dropped with no tokio runtime built; \
                 skipping teardown cleanup"
            );
            return;
        };

        // (4) Write engine: close (which flushes any remaining memtable), the
        // same call `close()` makes. `try_lock` rather than `blocking_lock`:
        // the latter panics in an async context, and a panic here is fatal.
        //
        // Contention is an ACCEPTABLE skip: a held lock means another live
        // caller is inside a write operation on this same engine, and that
        // caller — or the `close()`/drop of whichever handle still owns the
        // engine `Arc` — runs its own cleanup. Best-effort is the contract of
        // this hook; the durable guarantee remains explicit `close()`.
        if let Some(engine_arc) = self.write_engine.as_ref() {
            match engine_arc.try_lock() {
                Ok(mut guard) => match runtime.block_on(guard.inner.close()) {
                    Ok(()) => {}
                    // Swallowed AFTER being attempted: `drop` has no caller to
                    // return an error to, and propagating (panicking) would
                    // abort the interpreter.
                    Err(err) => tracing::debug!(
                        "cqlite: write-engine close failed during Database drop: {err}"
                    ),
                },
                Err(_) => tracing::debug!(
                    "cqlite: write engine busy during Database drop; \
                     skipping its close (another holder will clean up)"
                ),
            }
        }

        // (5) Read-side storage engine shutdown. Attempted INDEPENDENTLY of
        // step (4): a write-engine failure must not cost us this one.
        if let Err(err) = runtime.block_on(self.inner.shutdown()) {
            tracing::debug!("cqlite: storage shutdown failed during Database drop: {err}");
        }

        // (6) Telemetry flush, independent of (4) and (5), and LAST.
        //
        // Round 2 of review moved this OUT to run on every drop path including
        // the two skip branches. That was wrong and is reverted (rust-reviewer
        // B2), on measured grounds: with the `observability` feature on it
        // reaches `BatchSpanProcessor::force_flush`, a `sync_channel` +
        // `recv_timeout` against a HARD-CODED 5-second `forceflush_timeout`
        // (`opentelemetry_sdk-0.30.0/src/trace/span_processor.rs`). So it can
        // block ~5s per call WITH THE GIL HELD, and running it unconditionally
        // made the RECOMMENDED `with cqlite.open(...)` pattern pay that twice —
        // once in `close()`, again when the GC freed the same handle. Trading a
        // multi-second stall on the recommended path for telemetry on the
        // failed-`close()` path is a bad trade; that gap is a follow-up.
        //
        // Keeping it inside the guards is positively right, not just
        // conservative: in the re-entrancy branch that 5s `recv_timeout` would
        // park a tokio WORKER thread, and at interpreter finalization there is
        // no reason to stall teardown on an unreachable collector.
        crate::observability::flush();
    }
}
