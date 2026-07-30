//! The windowed scan's FORWARDER task and its join verdict (issue #3124, site 4).
//!
//! # The boundary this closes
//!
//! The windowed streaming scan ([`super::scan_stream_windowed`]) is a three-hop
//! pipeline: an I/O + decode feed → a blocking stitch+parse task → this forwarder,
//! which adapts the parse half's internal `Vec`-batched channel to the caller's
//! chosen public surface (per-row flatten or batched straight-through, issue #1592).
//!
//! The driver already mapped a `JoinError` from the PARSE task to an error, but the
//! forwarder was joined as `let _ = forwarder.await;` — its `JoinError` deliberately
//! DISCARDED on the grounds that the forwarder "only flattens already-decoded rows,
//! so it cannot fail the scan". That reasoning covers a forwarder that returns
//! normally; it does not cover one that DIES. A forwarder that unwinds drops both
//! its own end of the caller's channel and `batch_rx` — so the parse half's next
//! `blocking_send` fails and it terminates as if the consumer had walked away, the
//! driver returns `Ok(())`, and every row the forwarder had not yet forwarded is
//! silently GONE: a successful scan with fewer rows and no error. That is exactly
//! the #3106 defect class on the compressed (chunk-stitching) branch, which is the
//! branch every real Cassandra fixture in the tree takes.
//!
//! [`forwarder_verdict`] closes it: the join outcome is now OBSERVED, and a dead
//! forwarder becomes an [`crate::Error::Internal`] that the enclosing scan reports
//! as a terminal stream item.
//!
//! # Error precedence (unchanged where it was already right)
//!
//! An I/O error and a parse error remain CANONICAL — they are the root cause, and
//! the forwarder's death is usually a downstream symptom. The forwarder verdict is
//! consulted only when neither of those failed, i.e. exactly the case that used to
//! be reported as a clean, complete scan.
//!
//! Split into its own module (rather than added to `scan_stream_windowed.rs`) because
//! that file is already over the ~800-line campsite threshold (epic #1116).

use tokio::task::{JoinError, JoinHandle};

use super::scan_stream_windowed::WindowedOut;
use crate::storage::producer_fault::{FaultScope, ScanTaskSite};
use crate::types::ScanRow;
use crate::{Error, Result, RowKey};

/// Spawn the forwarder that drains the internal `Vec`-batched channel into the
/// caller's public surface (issue #1592, Epic F/F2).
///
/// This is the ONE place per-row and batched delivery diverge; everything upstream
/// (I/O feed, blocking decompress+parse, the internal batch channel, backpressure)
/// is shared. The per-row arm is a thin flattening adapter over the same batched
/// stream the batched arm forwards straight through, so the two surfaces are
/// guaranteed to yield identical rows in identical order (the batched arm's output
/// flattened equals the per-row arm's output).
///
/// Both arms preserve backpressure: a stalled consumer blocks the `send().await`
/// here, stopping the drain of `batch_rx`, which (bounded) blocks the parse half's
/// `blocking_send`, all the way back to disk reads. On consumer drop the `send` fails
/// and the forwarder returns, dropping `batch_rx` so the parse half terminates.
///
/// `fault_scope` carries the reader identity for the ONE test-only fault checkpoint
/// of this task (issue #3124); it is a zero-sized no-op in a production build, so the
/// spawned task's environment is unchanged there.
pub(super) fn spawn_windowed_forwarder(
    out: WindowedOut,
    mut batch_rx: tokio::sync::mpsc::Receiver<Result<Vec<(RowKey, ScanRow)>>>,
    fault_scope: FaultScope,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Issue #3124 (site 4): the task's single fault checkpoint, at entry — above
        // both output arms, so killing it reproduces "the forwarder died with rows
        // still in flight" for either surface. Compiles to nothing in production.
        fault_scope.checkpoint(ScanTaskSite::WindowedForwarder);
        match out {
            // Per-row (historical) surface: FLATTEN each confirmed batch back
            // into single `(RowKey, ScanRow)` items. One send per row.
            WindowedOut::PerRow(tx) => {
                while let Some(batch) = batch_rx.recv().await {
                    match batch {
                        Ok(rows) => {
                            for entry in rows {
                                if tx.send(Ok(entry)).await.is_err() {
                                    return; // consumer dropped
                                }
                            }
                        }
                        Err(e) => {
                            // Parse half surfaced a mid-stream error; forward it
                            // as a terminal item and stop. The parse half also
                            // returns the same error via its `Result`.
                            let _ = tx.send(Err(e)).await;
                            return;
                        }
                    }
                }
            }
            // Batched (F2) surface: FORWARD each confirmed batch straight
            // through — one send per batch, not per row. A terminal error is
            // forwarded as one item then the stream ends.
            WindowedOut::Batched(tx) => {
                while let Some(batch) = batch_rx.recv().await {
                    let terminal = batch.is_err();
                    if tx.send(batch).await.is_err() {
                        return; // consumer dropped
                    }
                    if terminal {
                        return;
                    }
                }
            }
        }
    })
}

/// The forwarder's join outcome, as the scan's `Result` (issue #3124, site 4).
///
/// A forwarder that returned normally is `Ok(())`. A forwarder that DIED — panicked,
/// or was aborted — is an [`Error::Internal`] naming the truncation, because the rows
/// it was holding never reached the caller and the channel it dropped is
/// indistinguishable, at the caller, from a finished scan. `Internal` (not
/// `Corruption`): nothing suggests the `Data.db` is bad, an internal invariant was
/// violated, and `Internal` is honestly non-recoverable.
pub(super) fn forwarder_verdict(joined: std::result::Result<(), JoinError>) -> Result<()> {
    joined.map_err(|join_err| {
        Error::internal(format!(
            "windowed scan forwarder: the forwarder task DIED without reporting \
             ({join_err}) — rows it had not yet delivered are LOST, so the result set \
             is TRUNCATED and cannot be reported as a complete scan (issue #3124)"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The verdict mapping itself: a clean join is `Ok`, a dead task is an `Err` that
    /// NAMES the truncation. The end-to-end pin over a real compressed fixture lives
    /// in `scan_stream_windowed_forwarder_panic_tests.rs`; this is the decision guard,
    /// so the mapping cannot be inverted without a dataset-independent failure.
    #[tokio::test]
    async fn a_dead_forwarder_is_an_error_and_a_finished_one_is_not() {
        let finished = tokio::spawn(async {}).await;
        assert!(
            forwarder_verdict(finished).is_ok(),
            "a forwarder that returned normally must not fail the scan"
        );

        let died = {
            let _silence = crate::storage::producer_fault::silence_injected_panics();
            tokio::spawn(async {
                panic!("{}", crate::storage::producer_fault::INJECTED_PANIC_MESSAGE);
            })
            .await
        };
        let err = forwarder_verdict(died)
            .expect_err("a forwarder that DIED must fail the scan, not complete it");
        let msg = err.to_string();
        assert!(
            msg.contains("DIED without reporting") && msg.contains("TRUNCATED"),
            "the error must name the dead task and the truncation, got: {msg}"
        );
    }
}
