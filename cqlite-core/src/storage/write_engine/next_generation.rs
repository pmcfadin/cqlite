//! The write engine's next SSTable id (issue #4626, requirements G6 and G7).
//!
//! Delegates to the generation module: the maximum sequence id over the table directory's Live set
//! and `backups/`, across BIG and BTI and every version letter, plus one. The directory is
//! `<data_dir>/<keyspace>/<table>`, which is where `SSTableWriter` publishes a flush.

use std::path::Path;

use crate::error::{Error, Result};
use crate::schema::TableSchema;
use crate::storage::sstable::generation::{next_generation_id, GenerationId, MAX_SEQUENCE_ID};

/// The id the first flush of an engine over `data_dir` uses (1 when the table has no directory yet).
pub(super) fn next_flush_generation(data_dir: &Path, schema: &TableSchema) -> Result<u64> {
    let table_dir = data_dir.join(&schema.keyspace).join(&schema.table);
    if !table_dir.is_dir() {
        return Ok(1);
    }
    match next_generation_id(&table_dir) {
        Ok(GenerationId::Sequence(n)) => Ok(u64::from(n)),
        Ok(other) => Err(Error::Storage(format!(
            "next generation id for {} is not a sequence id: {other}",
            table_dir.display()
        ))),
        Err(e) => Err(Error::Storage(format!(
            "cannot allocate the next SSTable generation in {}: {e}",
            table_dir.display()
        ))),
    }
}

/// Refuse a flush whose id would leave the Java `int` range (G7).
pub(super) fn ensure_flushable(generation: u64) -> Result<()> {
    if generation > u64::from(MAX_SEQUENCE_ID) {
        return Err(Error::Storage(format!(
            "no sequence id is left: generation {generation} exceeds the Java int limit 2147483647"
        )));
    }
    Ok(())
}
