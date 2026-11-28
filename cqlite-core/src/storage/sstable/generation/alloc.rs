//! Next-id allocation (requirements G6, G7).
//!
//! `cassandra-5.0.8` `db/Directories.java:1394-1405` (`getUIDGenerator`) lists the directory with
//! `OnTxnErr.IGNORE` and `includeBackups(true)` and does NOT skip temporary files, then
//! `SequenceBasedSSTableId.Builder.generator` (`:97-107`) takes the maximum sequence id, ignoring
//! UUID ids, and starts one above it (zero when there is none). BIG and BTI, and every version
//! letter, share the one id space because the id is the second filename token regardless of
//! format (`Descriptor.java:402`).

use std::path::Path;

use super::error::{GenerationError, GenerationResult};
use super::id::{GenerationId, MAX_SEQUENCE_ID};
use super::listing::list_generations;
use super::model::{Scope, TocPolicy};

/// The sequence id one above `highest`, or 1 when there is none.
///
/// Refuses (G7) rather than saturate or wrap when the result would exceed the Java `int` range,
/// which is what `new SequenceBasedSSTableId(int)` requires.
pub fn next_sequence_after(highest: Option<u32>) -> GenerationResult<u32> {
    let highest = highest.unwrap_or(0);
    match highest.checked_add(1) {
        Some(next) if next <= MAX_SEQUENCE_ID => Ok(next),
        _ => Err(GenerationError::SequenceIdExhausted { highest }),
    }
}

/// The id a writer must use for the next generation in `table_dir`: the maximum sequence id over
/// the Live set and `backups/`, plus one (G6).
///
/// Every descriptor counts, whatever its format, version, component set or state: an Orphan, a
/// below-floor generation and a file a transaction log hides still occupy their id, as they do in
/// Cassandra's lister, so an allocation can never reuse one. Snapshots do not count and UUID ids
/// do not contribute. Corrupt transaction-log records never block allocation (they are only
/// notes); an inconsistent transaction state fails it, as it fails Cassandra's lister.
pub fn next_generation_id(table_dir: &Path) -> GenerationResult<GenerationId> {
    let listing = list_generations(table_dir, &Scope::LiveAndBackups, TocPolicy::Accept)?;
    let highest = listing
        .live
        .iter()
        .chain(listing.orphans.iter())
        .map(|g| g.id())
        .chain(listing.hidden_ids.iter())
        .filter_map(GenerationId::sequence)
        .max();
    next_sequence_after(highest).map(GenerationId::Sequence)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_id_is_one() {
        assert_eq!(next_sequence_after(None).unwrap(), 1);
    }

    #[test]
    fn next_is_max_plus_one() {
        assert_eq!(next_sequence_after(Some(6)).unwrap(), 7);
    }

    /// G7.2
    #[test]
    fn one_below_the_limit_allocates_the_limit() {
        assert_eq!(
            next_sequence_after(Some(2_147_483_646)).unwrap(),
            2_147_483_647
        );
    }

    /// G7.1
    #[test]
    fn at_the_limit_it_refuses_and_names_the_highest() {
        match next_sequence_after(Some(2_147_483_647)) {
            Err(GenerationError::SequenceIdExhausted { highest }) => {
                assert_eq!(highest, 2_147_483_647)
            }
            other => panic!("expected SequenceIdExhausted, got {other:?}"),
        }
    }
}
