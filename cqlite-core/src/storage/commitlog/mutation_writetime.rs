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

/// One fact a CommitLog mutation must have in the SSTables for the mutation to count as flushed
/// (issue #4205, roborev job 299 M1): the SSTables must hold the same column at a timestamp
/// STRICTLY above the mutation's, or an EXACTLY equal cell (see the `state` fields), or a
/// covering deletion at or above it. At an equal timestamp Cassandra resolves cells by
/// tombstone/expiring state, local deletion time, TTL and then value bytes
/// (`Cells.resolveRegular`, `LivenessInfo.supersedes`, cassandra-5.0.8), so an equal timestamp
/// alone proves nothing. Only meaningful for a table without clustering columns (the only shape
/// the decoder models).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Need {
    /// A cell (or cell tombstone) of `column` written at `timestamp`.
    Cell {
        /// Column name.
        column: String,
        /// Cell timestamp.
        timestamp: i64,
        /// `Some(value bytes)` for a live, non-expiring cell; `None` for a tombstone or an
        /// expiring cell (whose TTL / local deletion time the decoder does not retain), which
        /// can never be proven equal to an SSTable cell at the same timestamp.
        value: Option<Vec<u8>>,
    },
    /// The row's primary-key liveness marker written at this timestamp.
    Liveness {
        /// Marker timestamp.
        timestamp: i64,
        /// Whether the marker carries a TTL (an expiring marker cannot be proven equal).
        expiring: bool,
    },
    /// A row deletion with this `markedForDeleteAt`.
    Deletion(i64),
}

/// The [`Need`]s of `update`. `Err(cause)` when the update has a shape the coverage proof does not
/// model (undecoded rows, a clustered row, a cell without a timestamp): the verdict is then
/// unmeasured with that cause, never a guess.
pub fn mutation_needs(update: &PartitionUpdate) -> Result<Vec<Need>, &'static str> {
    if !update.rows_decoded {
        return Err(update.undecoded_cause.unwrap_or("rows not decoded"));
    }
    let mut needs = Vec::new();
    for row in &update.rows {
        if !row.clustering.is_empty() {
            return Err("clustered row: SSTable cell coverage is not modeled");
        }
        if let Some(t) = row.writetime {
            needs.push(Need::Liveness {
                timestamp: t,
                expiring: row.liveness_expiring,
            });
        }
        if let Some(t) = row.deletion_timestamp {
            needs.push(Need::Deletion(t));
        }
        for cell in &row.cells {
            let Some(timestamp) = cell.writetime else {
                return Err("a cell carries no timestamp");
            };
            needs.push(Need::Cell {
                column: cell.column.clone(),
                timestamp,
                value: if cell.deleted || cell.expiring {
                    None
                } else {
                    Some(cell.value.clone().unwrap_or_default())
                },
            });
        }
    }
    Ok(needs)
}
