//! Output generation for `compact` (issue #4623): the default is the generation module's next id,
//! and an explicit id is refused when it would publish onto an existing SSTable.

use anyhow::Result;
use cqlite_core::storage::sstable::generation::{
    ensure_sequence_id_free, next_generation_id, GenerationId,
};
use std::path::Path;

/// The generation `compact` writes: `requested` after a preflight, else the next id in `dest_dir`
/// (1 when it does not exist). Reads the directory, writes nothing.
pub(crate) fn resolve_generation(dest_dir: &Path, requested: Option<u64>) -> Result<u64> {
    if let Some(id) = requested {
        ensure_sequence_id_free(dest_dir, id).map_err(|e| {
            anyhow::anyhow!("refusing --generation {id}: {e} (compact never overwrites an SSTable)")
        })?;
        return Ok(id);
    }
    if !dest_dir.is_dir() {
        return Ok(1);
    }
    match next_generation_id(dest_dir) {
        Ok(GenerationId::Sequence(n)) => Ok(u64::from(n)),
        Ok(other) => anyhow::bail!("next generation id {other} is not a sequence id"),
        Err(e) => anyhow::bail!(
            "cannot choose an output generation in {}: {e} (pass --generation to override)",
            dest_dir.display()
        ),
    }
}
