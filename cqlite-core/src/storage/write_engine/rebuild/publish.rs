//! Issue #4529: `rebuild_components` publishes by rename.
//!
//! Every component is written under a hidden staging directory and renamed into `--out` only once
//! the whole set is on disk (Data.db last), so a crash can never leave a final-named partial set that
//! Cassandra's `SSTableLister` would discover. See [`super::staged_publish`] for the Cassandra
//! citations and the publish order.

use super::components::rebuild_generation_capturing_stats;
use super::staged_publish::StagedPublish;
use super::{Component, RebuildOptions, RebuildReport};
use crate::error::Result;
use crate::schema::TableSchema;
use crate::storage::sstable::generation::Generation;
use crate::storage::sstable::writer::StatisticsMetadata;

pub(super) async fn rebuild_generation_published(
    generation: &Generation,
    schema: &TableSchema,
    requested: &[Component],
    options: &RebuildOptions,
) -> Result<RebuildReport> {
    // The floor first: a refusal must not even create `--out` (GD1).
    super::generation_input::check_floor(generation)?;
    let tag = generation.base_name();
    // A refused or failed run drops `stage` (removing it, and `--out` if this run created it).
    let stage = StagedPublish::begin(&options.out_dir, &tag)?;
    let mut staged_options = options.clone();
    staged_options.out_dir = stage.dir().to_path_buf();
    let mut discarded = StatisticsMetadata::default();
    let mut report = rebuild_generation_capturing_stats(
        generation,
        schema,
        requested,
        &staged_options,
        &mut discarded,
    )
    .await?;
    report.output = options.out_dir.display().to_string();
    if report.refused.is_none() {
        stage.publish()?;
    }
    Ok(report)
}
