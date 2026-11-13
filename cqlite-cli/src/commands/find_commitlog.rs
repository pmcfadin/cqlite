//! The `--commitlog` half of `cqlite find` (issue #4205): every mutation across every `*.log`
//! segment in a directory that touches the key, with segment, byte position, writetime and the
//! fail-closed `unflushed` verdict. An unreadable segment is its OWN named entry.

use std::path::Path;

use cqlite_core::schema::TableSchema;
use cqlite_core::storage::commitlog::{
    mutation_writetime, parse_table_id, ColumnSpec, CommitLogReader, CommitLogSchema, Mutation,
    SchemaSet, Writetime,
};

use super::find::{unflushed, GenRow, Unflushed};

/// One line of the CommitLog section.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitEntry {
    /// A mutation touching the key.
    Mutation {
        /// Segment file name.
        segment: String,
        /// Byte offset of the record within the segment file.
        position: u64,
        /// The mutation's writetime or why it is unmeasured.
        writetime: Writetime,
        /// The fail-closed unflushed verdict.
        unflushed: Unflushed,
    },
    /// A segment that could not be (fully) read; the cause is stated.
    SegmentFailure {
        /// Segment file name.
        segment: String,
        /// Cause, verbatim.
        cause: String,
    },
    /// The segment ends in a torn tail: records before it were reported, but a mutation in the
    /// torn bytes cannot be seen.
    SegmentTruncated {
        /// Segment file name.
        segment: String,
    },
}

/// Map a table schema to the CommitLog decoder's schema (partition key / clustering / regular
/// columns by CQL type name).
fn commitlog_schema(schema: &TableSchema) -> CommitLogSchema {
    let key_names: std::collections::HashSet<&str> = schema
        .partition_keys
        .iter()
        .map(|k| k.name.as_str())
        .chain(schema.clustering_keys.iter().map(|k| k.name.as_str()))
        .collect();
    CommitLogSchema {
        keyspace: schema.keyspace.clone(),
        table: schema.table.clone(),
        partition_key: schema
            .partition_keys
            .iter()
            .map(|k| ColumnSpec::new(k.name.clone(), k.data_type.clone()))
            .collect(),
        clustering: schema
            .clustering_keys
            .iter()
            .map(|k| ColumnSpec::new(k.name.clone(), k.data_type.clone()))
            .collect(),
        columns: schema
            .columns
            .iter()
            .filter(|c| !key_names.contains(c.name.as_str()))
            .map(|c| ColumnSpec::new(c.name.clone(), c.data_type.clone()))
            .collect(),
    }
}

/// Scan every `*.log` in `dir` (sorted by name). Never aborts: a failure becomes an entry.
pub fn scan_commitlog(
    dir: &Path,
    schema: &TableSchema,
    table_id_dashed: &str,
    key: &[u8],
    generations: &[GenRow],
) -> Vec<CommitEntry> {
    let mut out = Vec::new();
    let Some(table_id) = parse_table_id(table_id_dashed) else {
        out.push(CommitEntry::SegmentFailure {
            segment: dir.display().to_string(),
            cause: format!("table id {table_id_dashed} is not a UUID"),
        });
        return out;
    };
    let mut schemas = SchemaSet::new();
    schemas.insert(table_id, commitlog_schema(schema));

    let mut segments: Vec<std::path::PathBuf> = Vec::new();
    match std::fs::read_dir(dir) {
        Ok(rd) => {
            for entry in rd {
                match entry {
                    Ok(e) => {
                        let p = e.path();
                        if p.extension().is_some_and(|x| x == "log") {
                            segments.push(p);
                        }
                    }
                    // An entry that cannot be enumerated might be a segment: name it.
                    Err(e) => out.push(CommitEntry::SegmentFailure {
                        segment: dir.display().to_string(),
                        cause: format!("directory entry could not be read: {e}"),
                    }),
                }
            }
        }
        Err(e) => {
            out.push(CommitEntry::SegmentFailure {
                segment: dir.display().to_string(),
                cause: format!("cannot list directory: {e}"),
            });
            return out;
        }
    }
    segments.sort();
    for path in segments {
        let segment = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let reader = match CommitLogReader::open_with_schemas(&path, schemas.clone()) {
            Ok(r) => r,
            Err(e) => {
                out.push(CommitEntry::SegmentFailure {
                    segment,
                    cause: format!("open failed: {e}"),
                });
                continue;
            }
        };
        let mut it = reader.mutations_with_positions();
        for (position, res) in it.by_ref() {
            match res {
                Ok(mutation) => {
                    let position = position.unwrap_or(0);
                    if membership_undecidable(&mutation, &table_id, key) {
                        out.push(CommitEntry::SegmentFailure {
                            segment: segment.clone(),
                            cause: format!(
                                "mutation at position {position} touches this key but declares \
                                 updates this decoder cannot walk past; whether it also updates \
                                 the target table is undecidable"
                            ),
                        });
                    }
                    for update in &mutation.updates {
                        if update.table_id != table_id || update.partition_key != key {
                            continue;
                        }
                        let writetime = mutation_writetime(update);
                        out.push(CommitEntry::Mutation {
                            segment: segment.clone(),
                            position,
                            writetime,
                            unflushed: unflushed(writetime, generations),
                        });
                    }
                }
                Err(e) => {
                    let at = position
                        .map_or_else(|| "framing error".to_string(), |p| format!("position {p}"));
                    out.push(CommitEntry::SegmentFailure {
                        segment: segment.clone(),
                        cause: format!("record at {at} could not be read: {e}"),
                    });
                    break;
                }
            }
        }
        if it.truncated() {
            out.push(CommitEntry::SegmentTruncated { segment });
        }
    }
    out
}

/// Whether an INCOMPLETE mutation leaves "does it update the target table for this key?"
/// undecidable.
///
/// A Cassandra `Mutation` is ONE keyspace + ONE partition key with at most one
/// `PartitionUpdate` per table (`Mutation(String, DecoratedKey, ImmutableMap<TableId,
/// PartitionUpdate>, …)`, cassandra-5.0.8), so every update — decoded or hidden behind the
/// decoder's stopping point — carries the first visible update's partition key. A mutation for
/// another key therefore cannot match; one for THIS key whose visible updates do not include
/// the target table might still carry it in a hidden update.
pub(crate) fn membership_undecidable(m: &Mutation, table_id: &[u8; 16], key: &[u8]) -> bool {
    if m.updates_complete {
        return false;
    }
    let same_key = m.updates.first().is_some_and(|u| u.partition_key == key);
    let target_visible = m.updates.iter().any(|u| &u.table_id == table_id);
    same_key && !target_visible
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqlite_core::storage::commitlog::PartitionUpdate;

    fn upd(table: u8, key: &[u8]) -> PartitionUpdate {
        PartitionUpdate {
            table_id: [table; 16],
            partition_key: key.to_vec(),
            columns_read: false,
            column_names: vec![],
            has_partition_deletion: false,
            rows: vec![],
            rows_decoded: false,
            min_timestamp: None,
            undecoded_cause: Some("table schema not supplied"),
        }
    }

    fn mutation(updates: Vec<PartitionUpdate>, complete: bool) -> Mutation {
        Mutation {
            updates,
            updates_complete: complete,
        }
    }

    #[test]
    fn complete_mutations_never_leave_membership_undecidable() {
        let m = mutation(vec![upd(1, b"k")], true);
        assert!(!membership_undecidable(&m, &[9; 16], b"k"));
    }

    #[test]
    fn incomplete_mutation_for_another_key_cannot_match() {
        // All updates of a Mutation share the key, so hidden updates cannot be for `k`.
        let m = mutation(vec![upd(1, b"other")], false);
        assert!(!membership_undecidable(&m, &[9; 16], b"k"));
    }

    #[test]
    fn incomplete_mutation_for_this_key_without_the_target_table_is_undecidable() {
        let m = mutation(vec![upd(1, b"k")], false);
        assert!(membership_undecidable(&m, &[9; 16], b"k"));
    }

    #[test]
    fn incomplete_mutation_whose_visible_update_is_the_target_table_is_decided() {
        // One update per table per mutation: the target table is already visible.
        let m = mutation(vec![upd(9, b"k")], false);
        assert!(!membership_undecidable(&m, &[9; 16], b"k"));
    }
}
