//! The authoritative primary-key liveness marker of a COMPACTED row (issue #4581).
//!
//! A flush derives a row's liveness from the cells it writes. A compaction instead READS the
//! marker off the inputs, and it must re-emit exactly that marker: its own write timestamp, its
//! TTL and its on-disk `localExpirationTime` (Cassandra `LivenessInfo`, `cassandra-5.0.8`
//! `db/LivenessInfo.java:42-72`; serialized by `UnfilteredSerializer.serializeRowBody`). Deriving
//! them from the cells (or from the compaction wall clock) is what wrote a marker-only row with
//! ts=0 and no TTL.

use crate::storage::sstable::reader::compaction_row::RowLiveness;

/// Primary-key liveness marker carried verbatim onto a [`Mutation`](super::Mutation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RowMarker {
    /// The marker's write timestamp (µs), `LivenessInfo.timestamp()`.
    pub timestamp_micros: i64,
    /// `LivenessInfo.ttl()` in seconds; `None` for a marker with no TTL.
    pub ttl_seconds: Option<u32>,
    /// `LivenessInfo.localExpirationTime()` (GC-clock seconds, the unsigned on-disk value carried
    /// as a wrapped `i32`). `None` when the marker has no TTL, or the source did not surface it
    /// (the writer then keeps its historical `now + ttl` derivation).
    pub local_expiration_time: Option<i32>,
}

/// Who decides a mutation's primary-key liveness marker (#4581).
///
/// A flush or WAL replay derives it from the cells it writes ([`MarkerMode::Derive`]). A
/// compaction READS the reconciled marker off the inputs and re-emits exactly that, including
/// "no marker": an UPDATE-created row, a marker purged by `purge_marker` or a marker shadowed by
/// a deletion all serialize `LivenessInfo.EMPTY` (no `HAS_TIMESTAMP`), however new the surviving
/// cells are (`UnfilteredSerializer.serializeRowBody` sets the flags only when
/// `!pkLiveness.isEmpty()`; `BTreeRow.purge` replaces a purgeable marker with `EMPTY`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MarkerMode {
    /// The writer derives liveness from the mutation's cells (flush / WAL).
    #[default]
    Derive,
    /// The compaction path's reconciled marker, `None` meaning the row has no marker. The writer
    /// never derives liveness from cells for such a mutation.
    Explicit(Option<RowMarker>),
}

impl MarkerMode {
    /// The explicit marker, if this mode carries one.
    pub fn marker(self) -> Option<RowMarker> {
        match self {
            Self::Derive => None,
            Self::Explicit(m) => m,
        }
    }

    /// True when the compaction path decided the marker (possibly "none").
    pub fn is_explicit(self) -> bool {
        matches!(self, Self::Explicit(_))
    }
}

impl RowMarker {
    /// The marker a reconciled row carries, or `None` when it has no marker (an UPDATE-created
    /// row) or the source did not surface the marker's write timestamp. Authoritative on-disk
    /// fields only (no-heuristics, #28).
    pub fn from_liveness(liveness: &RowLiveness) -> Option<Self> {
        let timestamp_micros = liveness.marker_timestamp.filter(|_| liveness.has_marker)?;
        let ttl_seconds = liveness
            .ttl_seconds
            .and_then(|t| u32::try_from(t).ok())
            .filter(|t| *t != 0);
        Some(Self {
            timestamp_micros,
            ttl_seconds,
            local_expiration_time: ttl_seconds
                .and(liveness.expires_at_seconds)
                .map(|e| e as u32 as i32),
        })
    }
}
