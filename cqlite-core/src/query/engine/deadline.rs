//! The ONE query-execution time bound (issue #1695).
//!
//! # Why this module exists
//!
//! `Config.query.max_execution_time` (default 300s, set by the CLI's
//! `performance.query_timeout_ms`) was enforced NOWHERE: a runaway query ran
//! forever and the operator's knob was a placebo. This module is the enforcement
//! point, and it is deliberately the ONLY one.
//!
//! # Shape: one wrapper per public entry point, at the engine boundary
//!
//! Every public `async` entry point of [`AdvancedQueryEngine`] that can drive a
//! scan is a thin wrapper here over a private `*_inner` body in the parent
//! module, and the wrapper is the single place a `tokio::time::timeout` is
//! applied:
//!
//! | public entry point     | inner body                    | bound covers                  |
//! |------------------------|-------------------------------|-------------------------------|
//! | `execute`              | `execute_inner`               | parse, plan, execute, collect |
//! | `execute_streaming`    | `execute_streaming_inner`     | setup (see the scope note)    |
//! | `execute_with_params`  | `execute_with_params_inner`   | bind, plan, execute, collect  |
//! | `execute_prepared`     | `execute_prepared_inner`      | bind, execute, collect        |
//!
//! There are deliberately NO ad-hoc clock checks inside the scan loop: a deadline
//! sampled at N places is N places that can drift, mis-round, or be forgotten on
//! a new path. One `timeout` at the boundary bounds every path underneath it,
//! including ones that do not exist yet.
//!
//! Inner bodies delegate to each OTHER'S inner form (e.g. `execute_with_params_inner`
//! → `execute_inner` for a markerless SELECT), never back through a public
//! wrapper, so one caller-visible query is bounded by exactly ONE budget instead
//! of restarting the clock at an internal hop.
//!
//! # The `Duration::ZERO` sentinel
//!
//! `max_execution_time == Duration::ZERO` means **no timeout** (unbounded): the
//! future is awaited directly, with no timer registered at all. It is an
//! explicitly legal configuration — [`crate::Config::validate`] never rejects it
//! — and it is the only way to disable the bound, since the field is a
//! `Duration` rather than an `Option`.
//!
//! # Cancellation
//!
//! On elapse, `tokio::time::timeout` DROPS the inner future. That is the whole
//! cancellation mechanism: the materializing path holds its readers and buffers
//! inside that future, so dropping it releases them, and the streaming path's
//! spawned producer observes its receiver go away and exits (its `send` fails).
//! Nothing is detached and left running — see
//! `tests/issue_1695_query_timeout.rs`.
//!
//! ONE place needs more than the drop, and has it: a `spawn_blocking` closure
//! CANNOT be cancelled by dropping its `JoinHandle`. The multi-generation
//! materializing merges build their result inside such a closure with no channel
//! send that could fail, so each one arms a per-call flag whose guard lives in the
//! dropped future's scope and abandons the merge at its next partition — see
//! `storage::sstable::generation_merge::merge_cancel`. Any FUTURE blocking work
//! reached from a bounded entry point owes the same treatment; the drop alone is
//! not enough for it.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// The engine type; `query::mod` re-exports it as `AdvancedQueryEngine`.
use super::QueryEngine as AdvancedQueryEngine;
#[cfg(feature = "state_machine")]
use crate::query::result::{QueryResultIterator, StreamingConfig};
use crate::query::{executor::QueryResult, prepared::PreparedQuery};
use crate::{Error, Result, Value};

/// A query future handed to [`bound`], type-erased behind one heap allocation.
///
/// Erasure is deliberate and load-bearing, not stylistic: a generic `F` would be
/// INLINED into the wrapper's state machine, so every bounded entry point would
/// nest one more deep async layout inside its caller — enough to push rustc's
/// layout-depth query over its default limit in downstream async blocks (the
/// `#![recursion_limit]` class of failure, issue #1990). Behind `dyn Future` the
/// wrapper's own layout is a pointer, so the depth it adds is CONSTANT. The single
/// allocation per bounded call is immaterial next to parse + plan + scan.
pub(crate) type BoundedFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Bound `fut` by `limit`, mapping an elapse to [`Error::QueryTimeout`].
///
/// PURE (no engine state) so the contract is unit-testable on its own:
/// `Duration::ZERO` ⇒ awaited unbounded with no timer; an inner `Ok`/`Err`
/// passes through untouched; an elapse DROPS `fut` and reports the operation,
/// the configured limit, and the time actually spent.
///
/// # Two halves, one deadline
///
/// The single deadline is checked two ways, because neither alone is sufficient:
///
/// 1. **At every poll boundary** (`Instant::now() >= deadline`, before the inner
///    future is polled). This is what bounds a *CPU-bound* query: the read path
///    does long synchronous stretches per poll and only becomes `Pending` at its
///    cooperative checkpoints ([`crate::storage::scan_cancel::ScanCancel::checkpoint`]),
///    and a woken task is NOT guaranteed a timer-driver turn — tokio's scheduler
///    consults the driver only every `event_interval` polls, so a scan with a
///    handful of yield points could run to completion with an expired timer
///    unfired. Checking the clock where we are already being polled makes the
///    bound deterministic at the FIRST yield point past the deadline.
/// 2. **`tokio::time::timeout`**, which supplies the WAKE-UP a poll-boundary
///    check cannot: a future parked on something that never happens (an empty
///    channel, a stalled read) is never polled again, so only a timer can end it.
///
/// Both halves carry the same `limit` and produce the same error, and the check
/// lives HERE — at the one chokepoint — never in a scan loop.
pub(crate) async fn bound<T>(
    limit: Duration,
    operation: &str,
    fut: BoundedFuture<'_, T>,
) -> Result<T> {
    bound_tracked(limit, operation, fut).await.0
}

/// [`bound`] plus the one fact the caller's STATISTICS need: whether the inner
/// future was ever polled (issue #1695, roborev).
///
/// Every `*_inner` body counts itself with `inc_total_queries()` as its first
/// statement, i.e. synchronously on its FIRST poll. So `started == true` ⟺ the
/// query is already in `total_queries`. When the budget is ALREADY EXPIRED at the
/// first poll boundary this returns the timeout WITHOUT ever polling `inner`, so
/// nothing counted the query — and a caller that then counts the error would
/// publish `error_queries > total_queries`, an impossible statistic. Returning
/// `started` lets [`AdvancedQueryEngine::bounded`] keep the two counters coherent
/// without the inner body having to run.
async fn bound_tracked<T>(
    limit: Duration,
    operation: &str,
    fut: BoundedFuture<'_, T>,
) -> (Result<T>, bool) {
    // The documented "no timeout" sentinel: await directly rather than arming a
    // timer with a zero deadline (which would elapse at the first yield point).
    if limit.is_zero() {
        return (fut.await, true);
    }

    let started = Instant::now();
    // Overflow-safe: `Instant + Duration` PANICS when the sum is unrepresentable,
    // and `limit` is operator input (a `Duration::MAX` budget must not abort the
    // process). An unrepresentable deadline is astronomically far away, so it means
    // the same thing as the ZERO sentinel — unbounded — and is treated as such.
    let Some(deadline) = started.checked_add(limit) else {
        return (fut.await, true);
    };
    // `elapsed` is MEASURED, never assumed equal to `limit`: a starved or blocked
    // poll can overshoot the deadline substantially, and the real figure is what
    // an operator needs in order to tell "budget too tight" from "one decode unit
    // is uninterruptibly slow".
    let expired = |operation: &str| Error::QueryTimeout {
        operation: operation.to_string(),
        elapsed: started.elapsed(),
        limit,
    };

    // Set on the first poll that actually REACHES `inner`. Shared with the caller
    // (rather than a captured `bool`) because `poll_fn` is moved into `timeout` and
    // dropped there, so its captures are gone by the time the verdict is read.
    let inner_started = Arc::new(AtomicBool::new(false));

    let mut inner = fut;
    let checked = std::future::poll_fn({
        let inner_started = Arc::clone(&inner_started);
        move |cx| {
            if Instant::now() >= deadline {
                // Abandon WITHOUT polling further; `inner` is dropped with `checked`,
                // which IS the cancellation.
                return std::task::Poll::Ready(Err(expired(operation)));
            }
            inner_started.store(true, Ordering::Relaxed);
            inner.as_mut().poll(cx)
        }
    });

    let out = match tokio::time::timeout(limit, checked).await {
        Ok(inner) => inner,
        // The timer half: `checked` was dropped by `timeout`, taking the query
        // future with it.
        Err(_elapsed) => Err(Error::QueryTimeout {
            operation: operation.to_string(),
            elapsed: started.elapsed(),
            limit,
        }),
    };
    (out, inner_started.load(Ordering::Relaxed))
}

impl AdvancedQueryEngine {
    /// Bound one query future by the configured `query.max_execution_time`,
    /// counting an elapse as a query error in the engine stats and recording it
    /// on the observability error stream (a timeout is a real query failure, and
    /// its own telemetry category — never `corruption`).
    async fn bounded<T>(&self, operation: &str, fut: BoundedFuture<'_, T>) -> Result<T> {
        let (out, inner_started) =
            bound_tracked(self.config.query.max_execution_time, operation, fut).await;
        out.inspect_err(|e| {
            if matches!(e, Error::QueryTimeout { .. }) {
                // An ALREADY-EXPIRED budget returns before the inner body runs, so
                // nothing counted the query: `*_inner` bodies call
                // `inc_total_queries()` as their first statement (issue #1695,
                // roborev). Count it here instead, so a timed-out query is always a
                // query — never an error against a total that never saw it, which
                // would publish `error_queries > total_queries`.
                if !inner_started {
                    self.inc_total_queries();
                }
                self.inc_error_queries();
                crate::observability::record_error(e, "query");
            }
        })
    }

    /// Execute a CQL query.
    ///
    /// Bounded by `config.query.max_execution_time` (issue #1695): the whole
    /// execution — parse, plan, scan, and result collection — runs inside one
    /// `tokio::time::timeout`, and an elapsed budget returns
    /// [`Error::QueryTimeout`] with the inner future dropped. `Duration::ZERO`
    /// disables the bound.
    ///
    /// This is the parent of the query span tree (epic #1031, issue #1035): the
    /// `query.execute` span created here is the context every read-path span
    /// (issue #1034) and SELECT sub-span nests under. Bounded span attributes
    /// (plan type, access path, rows returned) are recorded once the result is
    /// known via `update_execution_stats`; the query text is never attached.
    #[tracing::instrument(
        name = "query.execute",
        skip(self, cql),
        fields(
            cqlite.query.plan_type = tracing::field::Empty,
            cqlite.query.access_path = tracing::field::Empty,
            cqlite.query.rows = tracing::field::Empty,
        )
    )]
    pub async fn execute(&self, cql: &str) -> Result<QueryResult> {
        self.bounded("query.execute", Box::pin(self.execute_inner(cql)))
            .await
    }

    /// Execute a CQL query with streaming results (Issue #280).
    ///
    /// Returns a `QueryResultIterator` that yields rows incrementally via a
    /// bounded channel, enabling memory-efficient processing of large result
    /// sets.
    ///
    /// # Timeout scope (issue #1695) — read this before relying on it
    ///
    /// `config.query.max_execution_time` bounds THIS future in its entirety, and
    /// nothing after it:
    ///
    /// * **Bounded** — statement parse, optimization, schema resolution, stream
    ///   setup, and every await this call makes before handing back the iterator.
    ///   For the plan shapes that materialize before streaming (any aggregate,
    ///   `ORDER BY`/`GROUP BY`, a projection trim, a `FROM`-less select) the
    ///   ENTIRE query executes inside this future, so the whole scan is bounded.
    /// * **NOT bounded** — the caller's later row consumption. On the incremental
    ///   path the iterator is returned as soon as the producer task is spawned, so
    ///   `iterator.next_async()` — including the wait for the FIRST batch — runs
    ///   outside the budget. That is deliberate: the pace of a bounded channel is
    ///   set by the consumer, and a slow consumer is not a runaway query.
    ///
    /// A caller that needs an end-to-end budget must bound its own consumption
    /// loop (e.g. `tokio::time::timeout` around `next_async()`); a per-batch bound
    /// inside the engine is possible future work, not this contract.
    /// `Duration::ZERO` disables the setup bound entirely.
    ///
    /// # Errors
    ///
    /// Returns an error if the query is not a SELECT, the CQL is invalid,
    /// execution fails, or the execution budget elapses during setup
    /// ([`Error::QueryTimeout`]).
    ///
    /// # Memory Budget
    ///
    /// The streaming approach stays within the 128MB target by using bounded
    /// channels and processing rows incrementally rather than materializing all
    /// results.
    #[cfg(feature = "state_machine")]
    pub async fn execute_streaming(
        &self,
        cql: &str,
        config: StreamingConfig,
    ) -> Result<QueryResultIterator> {
        self.bounded(
            "query.execute_streaming",
            Box::pin(self.execute_streaming_inner(cql, config)),
        )
        .await
    }

    /// Execute a query with positional `?` parameters (Issue #961).
    ///
    /// Bounded by `config.query.max_execution_time` (issue #1695) exactly like
    /// [`Self::execute`]; a markerless SELECT delegates to the same inner body,
    /// so the two APIs share ONE budget rather than restarting the clock.
    ///
    /// The supplied `params` are bound, in source order, into the `?` placeholders
    /// of the parsed statement *before* planning and execution, so the bound
    /// values participate in partition-key classification, encoding, and typed
    /// coercion. A `WHERE pk = ?` therefore engages the same partition-targeted
    /// fast path (#949/#956) as the equivalent literal query.
    ///
    /// Binding is currently supported for SELECT statements only. A non-SELECT
    /// CQL with parameters, or any use of named (`:name`) parameters, is rejected
    /// with a clear error (named-parameter binding is intentionally out of scope:
    /// the SELECT grammar only tokenizes positional `?`).
    ///
    /// # Routing parity with `execute` (Finding 1)
    ///
    /// When the parsed SELECT has **zero** bind markers and `params` is empty,
    /// this delegates straight back to [`Self::execute`]'s body so that a
    /// markerless `execute_with_params(sql, &[])` is byte-for-byte equivalent to
    /// `execute(sql)` — which since issue #1750 routes every literal SELECT
    /// through the modern SELECT optimizer + executor. Only when markers are
    /// present (`> 0`) is the statement bound and driven through that pipeline.
    ///
    /// Arity stays strict in both directions: markers `> 0` with a wrong
    /// `params.len()` is an error, and markers `== 0` with a **non-empty**
    /// `params` is also an error (a supplied parameter with no placeholder is a
    /// caller bug).
    pub async fn execute_with_params(&self, cql: &str, params: &[Value]) -> Result<QueryResult> {
        self.bounded(
            "query.execute_with_params",
            Box::pin(self.execute_with_params_inner(cql, params)),
        )
        .await
    }

    /// Execute a prepared query.
    ///
    /// Bounded by `config.query.max_execution_time` (issue #1695). NOTE: this is
    /// the ENGINE's prepared entry point, but it is not the only bounded one:
    /// [`PreparedQuery::execute`](crate::query::prepared::PreparedQuery::execute)
    /// and `execute_with_context`, called directly on a handle obtained from
    /// `prepare()`, carry the engine's budget and bound themselves via
    /// [`bound`]. So a prepared query is bounded on EITHER route — and bounded
    /// exactly ONCE on each, because the shared inner body is the `pub(crate)`
    /// unbounded one. Do not add a second wrapper here.
    pub async fn execute_prepared(
        &self,
        prepared: &PreparedQuery,
        params: &[Value],
    ) -> Result<QueryResult> {
        self.bounded(
            "query.execute_prepared",
            Box::pin(self.execute_prepared_inner(prepared, params)),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `Duration::ZERO` sentinel means UNBOUNDED: a future that only
    /// completes after several yields still returns its value, and no timer is
    /// consulted (a zero deadline would otherwise elapse at the first yield).
    #[tokio::test]
    async fn zero_limit_is_unbounded() {
        let out: Result<u32> = bound(
            Duration::ZERO,
            "test.zero",
            Box::pin(async {
                for _ in 0..64 {
                    tokio::task::yield_now().await;
                }
                Ok(7)
            }),
        )
        .await;
        assert_eq!(out.expect("ZERO must not bound anything"), 7);
    }

    /// An inner future that never completes must surface `Error::QueryTimeout`
    /// naming the operation and the configured limit — never a corruption or a
    /// generic I/O timeout, and never a hang.
    #[tokio::test]
    async fn elapsed_budget_yields_query_timeout() {
        let limit = Duration::from_millis(1);
        let out: Result<u32> = bound(limit, "test.pending", Box::pin(std::future::pending())).await;
        match out {
            Err(Error::QueryTimeout {
                operation,
                limit: reported,
                ..
            }) => {
                assert_eq!(operation, "test.pending");
                assert_eq!(reported, limit);
            }
            other => panic!("expected Error::QueryTimeout, got {other:?}"),
        }
    }

    /// The elapsed budget must classify as its OWN telemetry category and must
    /// NOT be recoverable-by-retry (the same query re-elapses).
    #[tokio::test]
    async fn timeout_error_is_distinct_from_corruption() {
        let err = bound::<u32>(
            Duration::from_millis(1),
            "test.classify",
            Box::pin(std::future::pending()),
        )
        .await
        .expect_err("must elapse");
        assert_eq!(
            err.obs_category(),
            crate::observability::ErrorCategory::Timeout
        );
        assert_ne!(
            err.obs_category(),
            crate::observability::ErrorCategory::Corruption
        );
        assert_eq!(err.category(), crate::error::ErrorCategory::Query);
        assert!(!err.is_recoverable());
    }

    /// A future that completes inside the budget passes its value through
    /// untouched, and an inner `Err` is relayed as itself (never rewritten into a
    /// timeout).
    #[tokio::test]
    async fn inner_outcome_passes_through() {
        let ok: Result<u32> = bound(
            Duration::from_secs(300),
            "test.ok",
            Box::pin(async { Ok(3) }),
        )
        .await;
        assert_eq!(ok.expect("must pass through"), 3);

        let err: Result<u32> = bound(
            Duration::from_secs(300),
            "test.err",
            Box::pin(async { Err(Error::corruption("inner")) }),
        )
        .await;
        assert!(matches!(
            err.expect_err("inner Err must be relayed"),
            Error::Corruption(_)
        ));
    }

    /// An ALREADY-EXPIRED budget reports that the inner future was never STARTED,
    /// which is the fact the engine's statistics need: the `*_inner` bodies count
    /// themselves on their first poll, so an unstarted query is one nothing counted.
    /// Without this signal `bounded` would record an error against a total that
    /// never saw the query (issue #1695, roborev: `error_queries > total_queries`).
    #[tokio::test]
    async fn an_expired_budget_reports_the_inner_future_as_unstarted() {
        let polled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen = std::sync::Arc::clone(&polled);
        // `Duration::from_nanos(1)` is expired by the time the first poll runs.
        let (out, started): (Result<u32>, bool) = bound_tracked(
            Duration::from_nanos(1),
            "test.unstarted",
            Box::pin(async move {
                seen.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(1)
            }),
        )
        .await;
        assert!(matches!(out, Err(Error::QueryTimeout { .. })));
        assert!(
            !polled.load(std::sync::atomic::Ordering::SeqCst),
            "precondition: the expired budget must short-circuit BEFORE the body runs"
        );
        assert!(
            !started,
            "an inner future that was never polled must be reported as unstarted"
        );
    }

    /// The complement: a future that DID run is reported as started (so its own
    /// `inc_total_queries()` is not double-counted), on the completing path and on
    /// the unbounded sentinel alike.
    #[tokio::test]
    async fn a_polled_inner_future_is_reported_as_started() {
        let (out, started): (Result<u32>, bool) = bound_tracked(
            Duration::from_secs(300),
            "test.started",
            Box::pin(async { Ok(5) }),
        )
        .await;
        assert_eq!(out.expect("must complete"), 5);
        assert!(started, "a polled inner future must be reported as started");

        let (zero, zero_started): (Result<u32>, bool) =
            bound_tracked(Duration::ZERO, "test.zero", Box::pin(async { Ok(6) })).await;
        assert_eq!(zero.expect("must complete"), 6);
        assert!(
            zero_started,
            "the unbounded sentinel awaits the inner future directly, so it started"
        );
    }

    /// Dropping the inner future IS the cancellation: when the budget elapses,
    /// the inner future's resources are released rather than detached.
    #[tokio::test]
    async fn elapse_drops_the_inner_future() {
        struct DropFlag(std::sync::Arc<std::sync::atomic::AtomicBool>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = DropFlag(std::sync::Arc::clone(&dropped));
        let out: Result<u32> = bound(
            Duration::from_millis(1),
            "test.drop",
            Box::pin(async move {
                let _held = flag;
                std::future::pending::<()>().await;
                Ok(0)
            }),
        )
        .await;
        assert!(matches!(out, Err(Error::QueryTimeout { .. })));
        assert!(
            dropped.load(std::sync::atomic::Ordering::SeqCst),
            "the timed-out future must be DROPPED (releasing what it held), not detached"
        );
    }
}
