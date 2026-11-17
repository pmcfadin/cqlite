//! Deadline unit tests for `export.rs` (moved out for the #1116 file-size ratchet; module path unchanged).

use super::check_export_deadline;
use std::time::Duration;

/// The `Duration::ZERO` sentinel reaches this as `None` and must never fail a
/// chunk pull — an unbounded export is a legal operator configuration.
#[test]
fn no_deadline_never_stops_an_export() {
    assert!(
        check_export_deadline(None, std::time::Instant::now(), Duration::ZERO).is_ok(),
        "an unbounded export must not be interrupted"
    );
}

/// An expired deadline must stop the export BEFORE the next chunk is pulled, and
/// must say so as the distinct timeout error carrying the export-side operation —
/// not the query-side one, so an operator can tell which loop elapsed.
///
/// Deterministic by construction: the deadline is a full second in the past, so no
/// clock has to advance and no chunk has to be slow.
#[tokio::test]
async fn an_expired_deadline_stops_the_export_before_the_next_chunk() {
    let budget = Duration::from_secs(30);
    let err = check_export_deadline(
        Some(tokio::time::Instant::now() - Duration::from_secs(1)),
        std::time::Instant::now(),
        budget,
    )
    .expect_err("an expired export deadline must stop the chunk loop");

    match err.downcast_ref::<cqlite_core::Error>() {
        Some(cqlite_core::Error::QueryTimeout {
            operation, limit, ..
        }) => {
            assert_eq!(
                operation, "cli.export.collect",
                "must name the EXPORT loop, distinct from `cli.query.collect`"
            );
            assert_eq!(limit, &budget);
        }
        other => panic!("expected QueryTimeout, got {other:?}"),
    }
}
