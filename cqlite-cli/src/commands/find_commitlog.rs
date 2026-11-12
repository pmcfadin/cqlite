//! The `--commitlog` half of `cqlite find` (issue #4205): every mutation across every `*.log`
//! segment in a directory that touches the key, with segment, byte position, writetime and the
//! fail-closed `unflushed` verdict. An unreadable segment is its OWN named entry.

use std::path::Path;

use cqlite_core::schema::TableSchema;
use cqlite_core::storage::commitlog::{
    mutation_writetime, parse_table_id, ColumnSpec, CommitLogReader, CommitLogSchema, SchemaSet,
    Writetime,
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

    let mut segments: Vec<_> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "log"))
            .collect(),
        Err(e) => {
            out.push(CommitEntry::SegmentFailure {
                segment: dir.display().to_string(),
                cause: format!("cannot list directory: {e}"),
            });
            return out;
        }
    };
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
        let mut it = reader.mutations();
        for (position, res) in it.by_ref() {
            match res {
                Ok(mutation) => {
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
                    out.push(CommitEntry::SegmentFailure {
                        segment: segment.clone(),
                        cause: format!("record at position {position} could not be read: {e}"),
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
