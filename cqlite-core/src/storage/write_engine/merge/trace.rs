//! Reconciliation decision-trail types.
//!
//! The trace is a side channel from the existing merge rules. Keeping the
//! sink in this unconditional module makes the public type surface available
//! to callers regardless of the write-support feature; the merger wires it
//! only when that feature is enabled.

use crate::storage::write_engine::mutation::{ClusteringBound, ClusteringKey};
use crate::types::Value;
use serde::{Deserialize, Serialize};

/// A closed classification for a cell version's reconciliation outcome.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Verdict {
    Winner,
    ShadowedByTimestamp,
    ShadowedByTombstone(TombstoneKind),
    Expired,
    Purgeable,
    DroppedColumn,
}

/// The tombstone class that made a cell non-winning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TombstoneKind {
    Partition,
    Range,
    Row,
    Cell,
    Collection,
}

/// The fact that decided a cell version's verdict.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "decision")]
pub enum DecidedBy {
    Winner {
        run_index: usize,
        writetime: i64,
    },
    Tombstone {
        kind: TombstoneKind,
        run_index: usize,
        deletion_time: i64,
        local_deletion_time: i32,
        droppable_at_now: bool,
    },
    DropTime(i64),
    Expiry {
        expires_at: i64,
        now: i64,
    },
    GcGrace {
        ldt: i32,
        gc_before: i64,
        now: i64,
    },
    None,
}

/// One cell version observed by reconciliation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CellDecision {
    pub run_index: usize,
    pub clustering: Option<ClusteringKey>,
    pub column: String,
    pub value: Option<Value>,
    pub writetime: i64,
    /// The CQL TTL is rendered as a signed value in the public trace schema,
    /// matching the accepted OpenSpec shape.  The on-disk cell model stores
    /// this as `u32`; the merge boundary performs the checked-width conversion
    /// before emitting the event.
    pub ttl: Option<i32>,
    pub expires_at: Option<i64>,
    pub verdict: Verdict,
    pub decided_by: DecidedBy,
}

/// One tombstone marker observed while reconciling a partition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TombstoneRecord {
    pub kind: TombstoneKind,
    pub run_index: usize,
    pub clustering: Option<ClusteringKey>,
    pub column: Option<String>,
    pub deletion_time: i64,
    pub local_deletion_time: i32,
    pub range_start: Option<ClusteringBound>,
    pub range_end: Option<ClusteringBound>,
    pub droppable_at_now: bool,
}

/// Outcome of probing one input generation for a requested partition key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProbeOutcome {
    Hit,
    Absent,
    Scanned,
}

/// Static-dispatch destination for reconciliation observations.
pub trait TraceSink {
    /// Whether constructing a trace event can have an observable effect.
    ///
    /// This associated constant lets the merge skip trace-only cloning and
    /// allocation for [`NoTrace`] before constructing an event.
    const ENABLED: bool = true;

    fn cell(&mut self, decision: CellDecision);
    fn tombstone(&mut self, tombstone: TombstoneRecord);
    fn generation_probe(&mut self, run_index: usize, outcome: ProbeOutcome);
}

/// The default sink. It is a zero-sized type and all methods inline to no-op.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoTrace;

impl TraceSink for NoTrace {
    const ENABLED: bool = false;

    #[inline]
    fn cell(&mut self, _decision: CellDecision) {}

    #[inline]
    fn tombstone(&mut self, _tombstone: TombstoneRecord) {}

    #[inline]
    fn generation_probe(&mut self, _run_index: usize, _outcome: ProbeOutcome) {}
}

/// An in-memory sink used by the explain surface and integration tests.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct RecordingSink {
    cells: Vec<CellDecision>,
    tombstones: Vec<TombstoneRecord>,
    probes: Vec<(usize, ProbeOutcome)>,
}

impl RecordingSink {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn cells(&self) -> &[CellDecision] {
        &self.cells
    }

    #[must_use]
    pub fn tombstones(&self) -> &[TombstoneRecord] {
        &self.tombstones
    }

    #[must_use]
    pub fn probes(&self) -> &[(usize, ProbeOutcome)] {
        &self.probes
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        Vec<CellDecision>,
        Vec<TombstoneRecord>,
        Vec<(usize, ProbeOutcome)>,
    ) {
        (self.cells, self.tombstones, self.probes)
    }
}

impl TraceSink for RecordingSink {
    fn cell(&mut self, decision: CellDecision) {
        self.cells.push(decision);
    }

    fn tombstone(&mut self, tombstone: TombstoneRecord) {
        self.tombstones.push(tombstone);
    }

    fn generation_probe(&mut self, run_index: usize, outcome: ProbeOutcome) {
        self.probes.push((run_index, outcome));
    }
}

impl<T: TraceSink + ?Sized> TraceSink for &mut T {
    const ENABLED: bool = T::ENABLED;

    #[inline]
    fn cell(&mut self, decision: CellDecision) {
        (**self).cell(decision);
    }

    #[inline]
    fn tombstone(&mut self, tombstone: TombstoneRecord) {
        (**self).tombstone(tombstone);
    }

    #[inline]
    fn generation_probe(&mut self, run_index: usize, outcome: ProbeOutcome) {
        (**self).generation_probe(run_index, outcome);
    }
}
