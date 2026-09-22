//! Top-level `rebuild_components` orchestration (design D1-D5).
//!
//! Two-pass structure over the partition-dependent components
//! (Index/Summary/Filter/Statistics), and why it is unavoidable:
//! Cassandra's `SerializationHeader.EncodingStats` (`min_timestamp`/
//! `min_ttl`/`min_local_deletion_time`) is ONE value per whole SSTable,
//! known upfront from the memtable at flush time, and used to delta-encode
//! EVERY row's timestamp/TTL/LDT VInts. Rebuild is reading an ALREADY-
//! written file, so it must first WALK every partition once to reconstruct
//! that same whole-table baseline (pass 1) before it can re-measure any
//! single row's promoted-index byte width against a scratch `DataWriter`
//! seeded with that baseline (pass 2) — seeding with the wrong baseline
//! would silently change VInt widths and desync every computed byte offset.

use super::{
    boundaries, decode, simple, statistics, Component, FieldProvenance, RebuildOptions,
    RebuildReport, Refusal, RefusalReason, SkippedComponent,
};
use crate::error::{Error, Result};
use crate::schema::TableSchema;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::directory::types::SSTableComponent;
use crate::storage::sstable::reader::{extract_sstable_base_name, SSTableReader};
use crate::storage::sstable::version_gate::{SsTableDescriptor, SsTableFormat};
use crate::storage::sstable::writer::data_writer::PartitionEmitCounts;
use crate::storage::sstable::writer::stats_fold::fold_mutation_stats;
use crate::storage::sstable::writer::{
    DataWriter, FilterWriter, IndexWriter, StatisticsMetadata, StatisticsWriter, SummaryWriter,
};
use std::collections::{BTreeMap, HashSet};
use std::path::Path;

async fn open_reader(input: &Path) -> Result<SSTableReader> {
    use crate::config::DiskAccessMode;
    use crate::platform::Platform;
    use crate::Config;
    use std::sync::Arc;

    let mut config = Config::default();
    config.storage.use_mmap = false;
    config.storage.disk_access_mode = DiskAccessMode::Buffered;
    let platform = Arc::new(Platform::new(&config).await?);
    SSTableReader::open(input, &config, platform).await
}

/// The total decompressed data-section length — needed only to derive the
/// LAST partition's serialized size for the `estimated_partition_size`
/// histogram (Statistics.db). Compressed input carries this directly in
/// `CompressionInfo.data_length`; uncompressed input derives it from the
/// file's own length minus the header.
fn partition_section_len(reader: &SSTableReader, data_db_path: &Path) -> Result<u64> {
    if let Some(ci) = reader.compression_info.as_deref() {
        Ok(ci.data_length)
    } else {
        let file_len = std::fs::metadata(data_db_path)?.len();
        Ok(file_len.saturating_sub(reader.calculate_header_size() as u64))
    }
}

/// A partially-streamed `Index.db` is the only file this function can have
/// written to disk before a pass-2 decode failure (`IndexWriter::with_sink`
/// streams as it goes; every other writer only persists at `finish()`) —
/// remove it so a refusal never leaves a byte behind (design R5.2).
fn cleanup_partial_output(out_dir: &Path, base: &str) {
    let _ = std::fs::remove_file(out_dir.join(format!("{base}-Index.db")));
}

fn data_corrupt_refusal(detail: impl std::fmt::Display, offset: Option<u64>) -> Refusal {
    Refusal {
        reason: RefusalReason::DataCorrupt,
        remedy: format!(
            "Data.db cannot be trusted as the source of truth: {detail} — remedy: cqlite \
             salvage (issue #4196) to recover a fresh generation from what is still decodable"
        ),
        offset,
    }
}

/// Regenerate `requested` derived components of `data_db_path` (design D1;
/// spec R1-R6). Opens `Data.db` READ-ONLY and never modifies it or any
/// component this run does not itself write to `options.out_dir`.
///
/// Returns `Err` only for a USAGE error (an empty `requested`, or `index`
/// requested against a BTI (`da`) input — see the module doc's scope note);
/// a damaged `Data.db` is reported as `Ok(report)` with `report.refused`
/// populated (design D3), never an `Err`.
pub async fn rebuild_components(
    data_db_path: &Path,
    schema: &TableSchema,
    requested: &[Component],
    options: &RebuildOptions,
) -> Result<RebuildReport> {
    if requested.is_empty() {
        return Err(Error::InvalidInput(
            "rebuild_components: `requested` must name at least one component".to_string(),
        ));
    }
    let descriptor = SsTableDescriptor::parse(data_db_path)?;
    let is_bti = descriptor.format == SsTableFormat::Bti;
    if is_bti && requested.contains(&Component::Index) {
        return Err(Error::UnsupportedFormat(
            "BTI (`da`) Partitions.db/Rows.db rebuild is not implemented in this change (issue \
             #4197 scope note, follow-up under epic #4192); request summary/filter/digest/toc/\
             crc/statistics instead"
                .to_string(),
        ));
    }

    let dir = data_db_path.parent().ok_or_else(|| {
        Error::InvalidInput(format!(
            "input path has no parent directory: {}",
            data_db_path.display()
        ))
    })?;
    let base = extract_sstable_base_name(data_db_path).ok_or_else(|| {
        Error::InvalidInput(format!(
            "cannot derive an SSTable base name from {}",
            data_db_path.display()
        ))
    })?;
    let compressed_input = dir.join(format!("{base}-CompressionInfo.db")).exists();
    let format_label = if is_bti { "da" } else { "nb" };
    let now = chrono::Utc::now().to_rfc3339();
    let cqlite_version = env!("CARGO_PKG_VERSION").to_string();
    let requested_labels: Vec<String> = requested
        .iter()
        .map(|c| c.manifest_label().to_string())
        .collect();

    let make_report = |refused: Option<Refusal>| RebuildReport {
        input: data_db_path.display().to_string(),
        output: options.out_dir.display().to_string(),
        format: format_label.to_string(),
        compressed_input,
        requested: requested_labels.clone(),
        regenerated: Vec::new(),
        skipped_not_applicable: Vec::new(),
        classification: BTreeMap::new(),
        refused,
        now: now.clone(),
        cqlite_version: cqlite_version.clone(),
    };

    let reader = open_reader(data_db_path).await?;

    if let Some(ci) = reader.compression_info.as_deref() {
        if let Err(refusal) = boundaries::compressed_chunk_preflight(data_db_path, ci) {
            return Ok(make_report(Some(refusal)));
        }
    }

    let entries = match boundaries::enumerate_partitions(&reader).await {
        Ok(e) => e,
        Err(err) => return Ok(make_report(Some(data_corrupt_refusal(err, None)))),
    };

    std::fs::create_dir_all(&options.out_dir)?;
    let mut report = make_report(None);

    // Format-not-applicable skips (design D5) — recorded even when NOT
    // requested is irrelevant here; this loop only inspects what WAS asked
    // for, so a component nobody requested never appears.
    for c in requested {
        let reason = match c {
            Component::Summary if is_bti => Some("BTI (`da`) has no Summary.db"),
            Component::Crc if is_bti => {
                Some("BTI (`da`) has no CRC.db (inline per-chunk CRC only)")
            }
            Component::Crc if compressed_input => {
                Some("compressed input uses inline chunk CRC, no CRC.db")
            }
            _ => None,
        };
        if let Some(reason) = reason {
            report.skipped_not_applicable.push(SkippedComponent {
                component: c.manifest_label().to_string(),
                reason: reason.to_string(),
            });
        }
    }
    let skipped: HashSet<String> = report
        .skipped_not_applicable
        .iter()
        .map(|s| s.component.clone())
        .collect();
    let want = |c: Component| requested.contains(&c) && !skipped.contains(c.manifest_label());

    let want_index = want(Component::Index);
    let want_summary = want(Component::Summary);
    let want_filter = want(Component::Filter);
    let want_statistics = want(Component::Statistics);
    let needs_partition_pass = want_index || want_summary || want_filter || want_statistics;

    let mut stats_acc = StatisticsMetadata::default();

    if needs_partition_pass {
        let scan_cancel = ScanCancel::new();

        // PASS 1 (only when the scratch-DataWriter pass below needs a
        // baseline): fold every mutation into `stats_acc` and refuse the
        // WHOLE run — before writing a single byte — on any decode failure.
        if want_index || want_summary || want_statistics {
            for (i, (offset, key)) in entries.iter().enumerate() {
                let end_bound = entries.get(i + 1).map(|(o, _)| *o);
                match decode::decode_one_partition(&reader, *offset, end_bound, key, schema, &scan_cancel)
                    .await
                {
                    Ok(Some((_, mutations))) => {
                        for m in &mutations {
                            fold_mutation_stats(&mut stats_acc, m);
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        return Ok(make_report(Some(data_corrupt_refusal(e, Some(*offset)))));
                    }
                }
            }
        }
        if let (Some((_, first_key)), Some((_, last_key))) = (entries.first(), entries.last()) {
            stats_acc.first_key = Some(first_key.clone());
            stats_acc.last_key = Some(last_key.clone());
        }

        // PASS 2: Data.db is now proven fully decodable and the baseline is
        // known — drive the real component writers.
        let (fp_chance, fp_provenance) = statistics::bloom_filter_fp_chance(schema);
        let (min_interval, min_interval_provenance) = statistics::min_index_interval();

        let mut index_writer = if want_index {
            Some(IndexWriter::with_sink(
                options.out_dir.join(format!("{base}-Index.db")),
            ))
        } else if want_summary {
            Some(IndexWriter::counting())
        } else {
            None
        };
        let mut filter_writer = if want_filter {
            Some(FilterWriter::new(
                options.out_dir.join(format!("{base}-Filter.db")),
                entries.len().max(1),
                fp_chance,
            )?)
        } else {
            None
        };
        let mut summary_writer = if want_summary {
            Some(SummaryWriter::new(min_interval))
        } else {
            None
        };
        let mut summary_sample_counter: usize = 0;
        let sample_interval = (min_interval as usize).max(1);

        let section_len = partition_section_len(&reader, data_db_path)?;
        let baseline_seed = StatisticsMetadata {
            min_timestamp: stats_acc.min_timestamp,
            min_ttl: stats_acc.min_ttl,
            min_local_deletion_time: stats_acc.min_local_deletion_time,
            ..StatisticsMetadata::default()
        };

        for (i, (offset, key)) in entries.iter().enumerate() {
            let end_bound = entries.get(i + 1).map(|(o, _)| *o);
            let this_end = end_bound.unwrap_or(section_len);
            let decoded =
                decode::decode_one_partition(&reader, *offset, end_bound, key, schema, &scan_cancel)
                    .await;
            let (decorated_key, mut mutations) = match decoded {
                Ok(Some(pair)) => pair,
                Ok(None) => continue,
                Err(e) => {
                    cleanup_partial_output(&options.out_dir, &base);
                    return Ok(make_report(Some(data_corrupt_refusal(e, Some(*offset)))));
                }
            };

            // Mirrors `SSTableWriter::write_partition`'s own sort — the
            // promoted-index block boundaries below must be computed over
            // rows in the SAME clustering order Cassandra wrote them in.
            mutations.sort_by(|a, b| match (&a.clustering_key, &b.clustering_key) {
                (None, None) => std::cmp::Ordering::Equal,
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(_), None) => std::cmp::Ordering::Greater,
                (Some(ck_a), Some(ck_b)) => ck_a
                    .compare(ck_b, schema)
                    .unwrap_or_else(|_| ck_a.cmp(ck_b)),
            });
            let partition_tombstone = mutations
                .iter()
                .filter_map(|m| m.partition_tombstone.as_ref())
                .max_by_key(|pt| pt.deletion_time)
                .cloned();
            let range_tombstones: Vec<_> = mutations
                .iter()
                .flat_map(|m| m.range_tombstones.iter())
                .cloned()
                .collect();

            let need_scratch = index_writer.is_some() || want_statistics;
            let (blocks, emit) = if need_scratch {
                let mut scratch = DataWriter::new(baseline_seed.clone());
                let (_, blocks, emit) = scratch.write_partition_with_index_blocks(
                    &decorated_key,
                    &mutations,
                    schema,
                    partition_tombstone.as_ref(),
                    &range_tombstones,
                )?;
                (blocks, emit)
            } else {
                (Vec::new(), PartitionEmitCounts::default())
            };

            if let Some(iw) = index_writer.as_mut() {
                let entry_info = iw.add_partition_with_promoted(&decorated_key, *offset, &blocks)?;
                if let Some(sw) = summary_writer.as_mut() {
                    sw.note_partition(&decorated_key);
                    if summary_sample_counter % sample_interval == 0 {
                        sw.add_entry(&decorated_key, entry_info.index_offset)?;
                    }
                    summary_sample_counter += 1;
                }
            }
            if let Some(fw) = filter_writer.as_mut() {
                fw.add_key(&decorated_key);
            }
            if want_statistics {
                stats_acc.row_count += emit.rows;
                stats_acc.column_count += emit.columns;
                stats_acc.increment_partition_count();
                stats_acc.update_key_range(&decorated_key.key);
                let serialized_size = this_end.saturating_sub(*offset);
                stats_acc.record_partition(serialized_size, emit.columns);
            }
        }

        if let Some(iw) = index_writer {
            if want_index {
                iw.finish_streaming()?;
                report
                    .regenerated
                    .push(Component::Index.manifest_label().to_string());
            }
            // else: counting mode — nothing was persisted, nothing to finish.
        }
        if let Some(fw) = filter_writer {
            fw.finish().await?;
            report
                .regenerated
                .push(Component::Filter.manifest_label().to_string());
            let mut fields = BTreeMap::new();
            fields.insert(
                "bloom_filter_fp_chance".to_string(),
                fp_provenance.manifest_label().to_string(),
            );
            report.classification.insert("filter".to_string(), fields);
        }
        if let Some(sw) = summary_writer {
            let bytes = sw.finish()?;
            std::fs::write(options.out_dir.join(format!("{base}-Summary.db")), bytes)?;
            report
                .regenerated
                .push(Component::Summary.manifest_label().to_string());
            let mut fields = BTreeMap::new();
            fields.insert(
                "min_index_interval".to_string(),
                min_interval_provenance.manifest_label().to_string(),
            );
            fields.insert(
                "sampling_level".to_string(),
                FieldProvenance::Recovered.manifest_label().to_string(),
            );
            report.classification.insert("summary".to_string(), fields);
        }
    }

    if want_statistics {
        write_statistics_component(dir, &base, options, schema, is_bti, &mut stats_acc, &mut report)?;
    }

    if want(Component::Digest) {
        simple::write_digest(data_db_path, &options.out_dir, &base)?;
        report
            .regenerated
            .push(Component::Digest.manifest_label().to_string());
    }
    if want(Component::Crc) {
        simple::write_crc(data_db_path, &options.out_dir, &base)?;
        report
            .regenerated
            .push(Component::Crc.manifest_label().to_string());
    }

    let want_toc = want(Component::Toc);
    let regenerated: HashSet<String> = report.regenerated.iter().cloned().collect();
    let present = simple::copy_untouched_components(dir, &options.out_dir, &base, |component| {
        match component {
            SSTableComponent::TOC => want_toc,
            SSTableComponent::Index => regenerated.contains("index"),
            SSTableComponent::Summary => regenerated.contains("summary"),
            SSTableComponent::Filter => regenerated.contains("filter"),
            SSTableComponent::Digest => regenerated.contains("digest"),
            SSTableComponent::Crc => regenerated.contains("crc"),
            SSTableComponent::Statistics => regenerated.contains("statistics"),
            // Data, CompressionInfo, Partitions, Rows: this tool never
            // regenerates any of these — always copy the original verbatim.
            _ => false,
        }
    })?;
    if want_toc {
        simple::write_toc(&options.out_dir, &base, &present)?;
        report
            .regenerated
            .push(Component::Toc.manifest_label().to_string());
    }

    Ok(report)
}

fn write_statistics_component(
    dir: &Path,
    base: &str,
    options: &RebuildOptions,
    schema: &TableSchema,
    is_bti: bool,
    stats_acc: &mut StatisticsMetadata,
    report: &mut RebuildReport,
) -> Result<()> {
    let stats_path = options
        .statistics_recovery_source
        .clone()
        .unwrap_or_else(|| dir.join(format!("{base}-Statistics.db")));
    let repair = statistics::recover_repair_state(&stats_path);
    stats_acc.set_repair_state(repair.repaired_at, repair.pending_repair, repair.is_transient);

    let out_path = options.out_dir.join(format!("{base}-Statistics.db"));
    let writer = if is_bti {
        StatisticsWriter::new_bti(out_path)
    } else {
        StatisticsWriter::new(out_path)
    };
    writer.write(stats_acc, Some(schema))?;
    report
        .regenerated
        .push(Component::Statistics.manifest_label().to_string());

    let mut fields = BTreeMap::new();
    for field in [
        "min_timestamp",
        "max_timestamp",
        "min_local_deletion_time",
        "max_local_deletion_time",
        "min_ttl",
        "max_ttl",
        "partition_count",
        "row_count",
        "column_count",
        "first_key",
        "last_key",
        "has_partition_level_deletions",
    ] {
        fields.insert(
            field.to_string(),
            FieldProvenance::Recomputed.manifest_label().to_string(),
        );
    }
    for field in ["repaired_at", "pending_repair", "is_transient"] {
        fields.insert(field.to_string(), repair.provenance.manifest_label().to_string());
    }
    for field in ["origin_host", "compaction_ancestry"] {
        fields.insert(
            field.to_string(),
            FieldProvenance::Lost.manifest_label().to_string(),
        );
    }
    report.classification.insert("statistics".to_string(), fields);
    Ok(())
}
