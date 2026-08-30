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
//! needed, and `Python::with_gil` during CPython finalization is itself a
//! hazard (the interpreter may already be past the point where a thread state
//! can be attached). Keeping the GIL surface at exactly zero removes that
//! failure mode instead of guarding it.
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

use std::sync::atomic::Ordering;

use tokio::runtime::Handle;

use crate::database::Database;
use crate::runtime::existing_runtime;

impl Drop for Database {
    fn drop(&mut self) {
        // (1) Claim the cleanup FIRST. The `AtomicBool` is the single source of
        // "already cleaned up" shared with `close()`, so an explicit `close()`
        // makes this drop a no-op and vice versa — never a double shutdown.
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }

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

        // (6) Telemetry flush, independent of (4) and (5). Process-global,
        // idempotent, and a no-op when observability was never initialised.
        crate::observability::flush();
    }
}
