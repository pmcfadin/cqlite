//! The one closed rule for "what is this CommitLog mutation's writetime" (issue #4205, D4).
//!
//! A [`PartitionUpdate`]'s writetime is the MAXIMUM timestamp it contributes: every row
//! liveness timestamp, every cell timestamp, and every row-deletion `markedForDeleteAt`.
//! Anything the decoder did not model (a partition deletion, a clustered table, a complex
//! column, a missing schema, …) is [`Writetime::Unmeasured`] naming the cause — never a
//! guessed or zero value (#4159 doctrine).

use crate::storage::commitlog::mutation::PartitionUpdate;

/// A mutation's writetime, or the stated reason it could not be measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Writetime {
    /// Microseconds since the Unix epoch (Cassandra's cell timestamp unit).
    Known(i64),
    /// The decoder does not model this mutation's shape; the cause is stable text.
    Unmeasured(&'static str),
}

/// Compute `update`'s writetime. See the module doc for the rule.
pub fn mutation_writetime(update: &PartitionUpdate) -> Writetime {
    if !update.rows_decoded {
        return Writetime::Unmeasured(update.undecoded_cause.unwrap_or("rows not decoded"));
    }
    let mut max: Option<i64> = None;
    let mut bump = |t: Option<i64>| {
        if let Some(t) = t {
            max = Some(max.map_or(t, |m| m.max(t)));
        }
    };
    for row in &update.rows {
        bump(row.writetime);
        bump(row.deletion_timestamp);
        for cell in &row.cells {
            bump(cell.writetime);
        }
    }
    match max {
        Some(t) => Writetime::Known(t),
        None => Writetime::Unmeasured("mutation carries no timestamped rows"),
    }
}
