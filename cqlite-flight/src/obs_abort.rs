//! Fine-grained, site-stamped `do_get` abort taxonomy (issue #2681).
//!
//! Split out of [`crate::obs`] (campsite rule, epic #1116): the abort
//! classification is a cohesive concern — a closed [`AbortReason`] enum, the
//! log/trace attribution [`AbortContext`], and the reason-carrying recording
//! hook [`record_do_get_abort`] — that the per-RPC metrics module ([`crate::obs`])
//! consumes but does not otherwise depend on.
//!
//! Every server-side `do_get` failure previously collapsed into the coarse
//! `cqlite.error.category = "other"` bucket (see [`crate::obs::record_status_error`]),
//! so a benign abort (a split torn down under a streaming reader, a client that
//! hung up, an admission shed) was indistinguishable in-field from a genuine
//! internal fault. The gRPC `Code` is itself lossy — a client disconnect and a
//! cooperative merge-cancel both surface as `Aborted`; a snapshot teardown and a
//! genuine panic both surface as `Internal` — so the abort *reason* is only
//! knowable at the site that raises it. The classification is therefore STAMPED
//! at each abort construction site and NEVER inferred from the gRPC code or the
//! error message text (no-heuristics #28).

use cqlite_core::observability::{self as obs, catalog, AttrValue};

use crate::obs::SUBSYSTEM;

/// The fine-grained, site-stamped `do_get` abort taxonomy (issue #2681).
///
/// A closed value set naming every known `do_get` abort path, stamped at the
/// abort construction site from authoritative local knowledge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbortReason {
    /// A split/snapshot torn down or superseded by a newer generation under a
    /// streaming reader (issue #2452). Benign.
    SupersededSplit,
    /// Client disconnect / stream dropped before completion, or cooperative
    /// merge-cancel. Benign.
    ClientCancel,
    /// Rejected at max-concurrent-scans capacity (issue #2420). Benign.
    AdmissionShed,
    /// The resolved snapshot generation was retired before/while serving
    /// (issue #2452). Benign.
    SnapshotRetired,
    /// A genuine internal fault (merge/convert/predicate/discovery/panic/egress).
    Internal,
    /// A malformed/rejected ticket (`Code::InvalidArgument`). Client fault.
    TicketInvalid,
}

impl AbortReason {
    /// The bounded `cqlite.flight.abort_reason` attribute value — a `&'static str`
    /// from this closed set, safe as a metric label.
    pub fn label(self) -> &'static str {
        match self {
            AbortReason::SupersededSplit => "superseded_split",
            AbortReason::ClientCancel => "client_cancel",
            AbortReason::AdmissionShed => "admission_shed",
            AbortReason::SnapshotRetired => "snapshot_retired",
            AbortReason::Internal => "internal",
            AbortReason::TicketInvalid => "ticket_invalid",
        }
    }

    /// The valid, closed [`cqlite_core::observability::ErrorCategory`] label this
    /// reason maps onto for the canonical `cqlite.error.category` dimension of
    /// `cqlite.errors.total`, so existing category rollups keep working while the
    /// new `cqlite.flight.abort_reason` attribute carries the fine detail.
    ///
    /// Only `internal → "other"` and `ticket_invalid → "query"` are preserved
    /// from [`crate::obs::record_status_error`]'s old code mapping. The benign
    /// aborts (`client_cancel`, `admission_shed`, `snapshot_retired`,
    /// `superseded_split`) are DELIBERATELY re-categorized from the old catch-all
    /// `"other"` bucket to `"cancelled"` (matching the `catalog.rs` doc): they are
    /// expected terminal states under load, not faults. Consequence, intended and
    /// not a regression: any dashboard keying flight error-rate on
    /// `category="other"` will see that bucket SHRINK as benign aborts drain to
    /// `"cancelled"` — `cqlite.errors.total` still increments exactly once per
    /// abort, and the new `cqlite.flight.abort_reason` attribute carries the
    /// authoritative fine-grained reason.
    fn error_category(self) -> obs::ErrorCategory {
        use obs::ErrorCategory;
        match self {
            // Benign terminal states: expected under load, not a fault.
            AbortReason::SupersededSplit
            | AbortReason::ClientCancel
            | AbortReason::AdmissionShed
            | AbortReason::SnapshotRetired => ErrorCategory::Cancelled,
            // A malformed/rejected ticket is a client (query) fault.
            AbortReason::TicketInvalid => ErrorCategory::Query,
            // A genuine server fault stays in the catch-all category.
            AbortReason::Internal => ErrorCategory::Other,
        }
    }

    /// The `tracing` level this reason logs at, replacing the code-driven level
    /// for the `do_get` abort path (issue #2681): benign aborts at `debug`, a
    /// genuine internal fault at `error`, a client-fault ticket at `warn`.
    fn log_level(self) -> tracing::Level {
        match self {
            AbortReason::SupersededSplit
            | AbortReason::ClientCancel
            | AbortReason::AdmissionShed
            | AbortReason::SnapshotRetired => tracing::Level::DEBUG,
            AbortReason::TicketInvalid => tracing::Level::WARN,
            AbortReason::Internal => tracing::Level::ERROR,
        }
    }
}

/// The high-cardinality attribution context carried on the abort log/trace event
/// (issue #2681) — NEVER on a metric label. Identifies WHICH split/snapshot
/// aborted so an operator can attribute it in-field, without polluting the
/// bounded `cqlite.errors.total` cardinality.
#[derive(Debug, Clone, Default)]
pub struct AbortContext {
    /// The ticket/split identity available at the abort site (e.g.
    /// `keyspace/table[/snapshot]`), or empty when the site raised before a
    /// ticket was parsed. Owned so any site can supply what it knows.
    pub ticket_id: String,
    /// The resolved snapshot generation, when known at the site.
    pub snapshot_generation: Option<u64>,
}

impl AbortContext {
    /// An empty context (no ticket/split identity known yet at the site).
    pub fn empty() -> Self {
        Self::default()
    }

    /// A context carrying a ticket/split identity string.
    pub fn with_ticket(ticket_id: impl Into<String>) -> Self {
        Self {
            ticket_id: ticket_id.into(),
            snapshot_generation: None,
        }
    }
}

/// Record a `do_get` abort with its authoritative, site-stamped [`AbortReason`]
/// (issue #2681).
///
/// This is the reason-carrying entry point that replaces
/// [`crate::obs::record_status_error`] on the `do_get` abort path. It:
///
/// * increments `cqlite.errors.total` once with `cqlite.subsystem = "flight"`, a
///   VALID closed `cqlite.error.category` (derived from the reason — see
///   [`AbortReason::error_category`] for the intentional benign→`"cancelled"`
///   re-categorization), AND the NEW bounded attribute
///   `cqlite.flight.abort_reason = reason.label()`; and
/// * emits a structured `tracing` event at the reason-appropriate LEVEL
///   (benign → `debug`, `internal` → `error`, `ticket_invalid` → `warn`),
///   carrying the `abort_reason`, the ticket/split identity, and the snapshot
///   generation from `cx`.
///
/// The classification is passed by the CALLER from authoritative local knowledge
/// — it is never derived from `status.code()` or `status.message()`
/// (no-heuristics #28). The status message is logged (the log is not a bounded
/// cardinality surface) but never recorded on the metric.
pub fn record_do_get_abort(status: &tonic::Status, reason: AbortReason, cx: AbortContext) {
    let err = category_placeholder_error(reason.error_category());
    obs::record_error_with_attrs(
        &err,
        SUBSYSTEM,
        &[(
            catalog::attr::FLIGHT_ABORT_REASON,
            AttrValue::StaticStr(reason.label()),
        )],
    );

    let code = status.code();
    let message = status.message();
    let abort_reason = reason.label();
    let ticket_id = cx.ticket_id.as_str();
    let snapshot_generation = cx.snapshot_generation;
    match reason.log_level() {
        tracing::Level::DEBUG => tracing::debug!(
            subsystem = SUBSYSTEM,
            cqlite.flight.abort_reason = abort_reason,
            ticket_id,
            snapshot_generation,
            %code,
            message,
            "do_get aborted"
        ),
        tracing::Level::WARN => tracing::warn!(
            subsystem = SUBSYSTEM,
            cqlite.flight.abort_reason = abort_reason,
            ticket_id,
            snapshot_generation,
            %code,
            message,
            "do_get rejected (client fault)"
        ),
        _ => tracing::error!(
            subsystem = SUBSYSTEM,
            cqlite.flight.abort_reason = abort_reason,
            ticket_id,
            snapshot_generation,
            %code,
            message,
            "do_get failed (internal fault)"
        ),
    }
}

/// Build the representative `cqlite_core::Error` whose `obs_category()` equals
/// `category`, so [`obs::record_error_with_attrs`] keys `cqlite.errors.total` on
/// the intended bounded category. Only the category is used (the message is a
/// fixed placeholder — never recorded on the metric), mirroring
/// [`crate::obs::record_status_error`]'s code→error mapping.
fn category_placeholder_error(category: obs::ErrorCategory) -> cqlite_core::Error {
    use cqlite_core::Error;
    use obs::ErrorCategory;
    match category {
        // `Error::Cancelled.obs_category() == Cancelled`.
        ErrorCategory::Cancelled => Error::Cancelled,
        // `Error::invalid_input(..).obs_category() == Query`.
        ErrorCategory::Query => Error::invalid_input("flight"),
        // `Error::internal(..).obs_category() == Other`.
        _ => Error::internal("flight"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abort_reason_labels_are_the_closed_set() {
        // The label set is the exact closed value set the spec pins (issue #2681);
        // a new variant must update this assertion as a deliberate decision.
        assert_eq!(AbortReason::SupersededSplit.label(), "superseded_split");
        assert_eq!(AbortReason::ClientCancel.label(), "client_cancel");
        assert_eq!(AbortReason::AdmissionShed.label(), "admission_shed");
        assert_eq!(AbortReason::SnapshotRetired.label(), "snapshot_retired");
        assert_eq!(AbortReason::Internal.label(), "internal");
        assert_eq!(AbortReason::TicketInvalid.label(), "ticket_invalid");
    }

    #[test]
    fn abort_reason_maps_to_valid_closed_error_category() {
        use cqlite_core::observability::ErrorCategory;
        // Benign aborts reuse the existing `cancelled` category; a ticket fault
        // keeps `query`; a genuine fault stays `other` — every mapping is a valid
        // closed `ErrorCategory` so existing category rollups never break.
        for (reason, cat) in [
            (AbortReason::SupersededSplit, ErrorCategory::Cancelled),
            (AbortReason::ClientCancel, ErrorCategory::Cancelled),
            (AbortReason::AdmissionShed, ErrorCategory::Cancelled),
            (AbortReason::SnapshotRetired, ErrorCategory::Cancelled),
            (AbortReason::TicketInvalid, ErrorCategory::Query),
            (AbortReason::Internal, ErrorCategory::Other),
        ] {
            assert_eq!(reason.error_category(), cat, "{reason:?}");
            // The placeholder error's obs_category must equal the intended
            // category, so `record_error_with_attrs` keys the counter correctly.
            assert_eq!(
                category_placeholder_error(reason.error_category()).obs_category(),
                cat,
                "placeholder for {reason:?} must key the intended category"
            );
        }
    }

    #[test]
    fn abort_reason_log_level_is_reason_driven() {
        use tracing::Level;
        for reason in [
            AbortReason::SupersededSplit,
            AbortReason::ClientCancel,
            AbortReason::AdmissionShed,
            AbortReason::SnapshotRetired,
        ] {
            assert_eq!(reason.log_level(), Level::DEBUG, "{reason:?} is benign");
        }
        assert_eq!(AbortReason::TicketInvalid.log_level(), Level::WARN);
        assert_eq!(AbortReason::Internal.log_level(), Level::ERROR);
    }

    #[test]
    fn record_do_get_abort_is_callable_in_any_build() {
        // The hook must drive its counter/log without panicking in any build
        // (no-op OTel when the core observability feature is off).
        record_do_get_abort(
            &tonic::Status::aborted("client gone"),
            AbortReason::ClientCancel,
            AbortContext::with_ticket("ks/tbl"),
        );
        record_do_get_abort(
            &tonic::Status::internal("boom"),
            AbortReason::Internal,
            AbortContext::empty(),
        );
    }
}
