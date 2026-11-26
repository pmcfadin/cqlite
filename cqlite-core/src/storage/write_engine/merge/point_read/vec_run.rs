//! In-memory runs for the single-partition point read (split out of `point_read.rs`, issue #1116
//! campsite file-size ratchet; `empty_run` is new for #4629).

use super::super::{MergeEntry, SSTableRowIterator};
use crate::Result;
use std::collections::VecDeque;

/// A run backed by a pre-built `Vec` of merge entries — the seeked single
/// partition. Yields each entry once, in order, then reports exhaustion.
///
/// Only the default `not(tombstones)` build seeks (the alternate build always
/// scan-falls-back), so this is dead there.
#[cfg_attr(feature = "tombstones", allow(dead_code))]
pub(super) struct VecRun {
    pub(super) entries: VecDeque<MergeEntry>,
}

impl SSTableRowIterator for VecRun {
    fn next(&mut self) -> Option<Result<MergeEntry>> {
        self.entries.pop_front().map(Ok)
    }
}

/// A run that yields nothing: the slot of a candidate that holds none of the requested keys (#4629).
pub(super) fn empty_run() -> Box<dyn SSTableRowIterator> {
    Box::new(VecRun {
        entries: VecDeque::new(),
    })
}
