//! Output generation for `compact` (issue #4623): the default is the generation module's next id,
//! and an explicit id is refused when it would publish onto an existing SSTable.

use anyhow::{Context, Result};
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

/// Merge `inputs` into a stage under `--output` and publish it (#4623).
///
/// The id is resolved and checked BEFORE any byte is written; `StagedPublish::publish` then
/// refuses, with nothing renamed, if any final name already exists. Returns the report and the
/// published Data.db path. Fully-expired inputs are removed only AFTER the publish (#4666).
pub(crate) async fn compact_and_publish(
    inputs: Vec<std::path::PathBuf>,
    args: &crate::cli_types::CompactArgs,
    schema: &cqlite_core::schema::TableSchema,
    purge_safe: bool,
    udt_registry: &cqlite_core::schema::UdtRegistry,
) -> Result<(
    cqlite_core::storage::write_engine::merge::CompactReport,
    std::path::PathBuf,
)> {
    use cqlite_core::storage::write_engine::rebuild::no_clobber::MAX_PUBLISH_ATTEMPTS;
    use cqlite_core::storage::write_engine::rebuild::staged_publish::{
        LeftoverPolicy, StagedPublish,
    };

    let dest_dir = args.output.join(&schema.keyspace).join(&schema.table);
    let generation = resolve_generation(&dest_dir, args.generation)?;
    // The stage lives in the destination TABLE directory (unique tag, so concurrent compactions
    // do not share a stage name) and only that directory is checked for a leftover stage.
    let tag = format!("compact-{}-{generation}", std::process::id());
    let stage = StagedPublish::begin_in(&args.output, &dest_dir, &tag, LeftoverPolicy::Refuse)
        .with_context(|| format!("cannot stage compaction output in {}", dest_dir.display()))?;
    let report = cqlite_core::storage::write_engine::merge::compact_sstables_with_registry(
        inputs,
        stage.dir(),
        schema,
        generation,
        args.gc_before,
        args.now_sec,
        purge_safe,
        Some(udt_registry),
    )
    .await
    .with_context(|| "compaction failed")?;
    // A default id may lose a race with another writer: re-allocate and retry. An explicit id is
    // the operator's choice and is never silently changed.
    let attempts = if args.generation.is_some() {
        1
    } else {
        MAX_PUBLISH_ATTEMPTS
    };
    let published = stage
        .publish_retargeting(
            generation,
            attempts,
            &mut |taken| match next_generation_id(&dest_dir) {
                Ok(GenerationId::Sequence(n)) if u64::from(n) > taken => Ok(u64::from(n)),
                Ok(_) => Err(cqlite_core::Error::Storage(format!(
                    "no id above {taken} is available in {}",
                    dest_dir.display()
                ))),
                Err(e) => Err(cqlite_core::Error::Storage(e.to_string())),
            },
        )
        .with_context(|| format!("cannot publish compaction output to {}", dest_dir.display()))?;
    // Published and fsynced (TOC last): only now may the dropped inputs go (#4666).
    report.reclaim_dropped_inputs();
    let output_path = report
        .output
        .data_path
        .file_name()
        .and_then(|n| n.to_str())
        .map(|n| {
            dest_dir.join(
                cqlite_core::storage::write_engine::rebuild::no_clobber::retarget_name(
                    n, generation, published,
                ),
            )
        })
        .unwrap_or_else(|| report.output.data_path.clone());
    Ok((report, output_path))
}
