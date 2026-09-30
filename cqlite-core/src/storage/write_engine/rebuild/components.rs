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
use crate::storage::sstable::writer::data_writer::{
    is_static_row_mutation, resolve_shadow_floor, PartitionEmitCounts,
};
use crate::storage::sstable::writer::stats_fold::{
    fold_single_mutation_row_group, fold_static_carrier_stats,
};
use crate::storage::sstable::writer::{
    DataWriter, FilterWriter, IndexWriter, SSTableWriter, StatisticsMetadata, StatisticsWriter,
    SummaryWriter,
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

    let entries = match boundaries::enumerate_partitions(&reader, schema).await {
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
        // baseline): reconstruct the whole-table `EncodingStats` MINIMA
        // (min_timestamp/min_ttl/min_local_deletion_time) via the same
        // fixed #729 two-pass baseline primitive `WriteEngine::
        // flush_internal_async` uses (`SSTableWriter::
        // compute_mutations_baseline_stats`) — NOT the old blind
        // `fold_mutation_stats` (issue #4246: that function is
        // `#[cfg(test)]`-only now precisely because folding every raw
        // mutation unconditionally reintroduces the phantom-fold bug).
        // The full per-mutation content fold (maxima, tombstone histogram,
        // flags) happens below in PASS 2, gated on the SAME shadow decision
        // the scratch `DataWriter` emission makes for each partition.
        // Refuse the WHOLE run — before writing a single byte — on any
        // decode failure.
        if want_index || want_summary || want_statistics {
            let mut baseline_min_ts = i64::MAX;
            let mut baseline_min_ldt = i32::MAX;
            let mut baseline_min_ttl = i32::MAX;
            for (i, (offset, key)) in entries.iter().enumerate() {
                let end_bound = entries.get(i + 1).map(|(o, _)| *o);
                match decode::decode_one_partition(
                    &reader,
                    *offset,
                    end_bound,
                    key,
                    schema,
                    &scan_cancel,
                )
                .await
                {
                    Ok(Some((_, mutations))) => {
                        let (min_ts, min_ldt, min_ttl) =
                            SSTableWriter::compute_mutations_baseline_stats(&mutations, schema);
                        baseline_min_ts = baseline_min_ts.min(min_ts);
                        baseline_min_ldt = baseline_min_ldt.min(min_ldt);
                        baseline_min_ttl = baseline_min_ttl.min(min_ttl);
                    }
                    Ok(None) => {}
                    Err(e) => {
                        return Ok(make_report(Some(data_corrupt_refusal(e, Some(*offset)))));
                    }
                }
            }
            stats_acc.min_timestamp = baseline_min_ts;
            stats_acc.min_local_deletion_time = baseline_min_ldt;
            stats_acc.min_ttl = baseline_min_ttl;
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
        // Declared gap, discovered empirically against real fixtures while
        // validating this change (beyond design.md's own §D2 table, which
        // named `bloom_filter_fp_chance` as the one recoverable Filter.db
        // parameter and implicitly assumed `expected_keys` == the final
        // distinct partition count): Cassandra's ACTUAL `Filter.db` bit-array
        // size is sized from `estimatedKeys` at WRITE time, which — for an
        // SSTable produced by compaction — is an ESTIMATE derived from the
        // INPUT sstables' own key counts (`getApproximateKeyCount`), not a
        // recount of the truly final distinct partition set. Two committed
        // fixtures verified empirically both carry a LARGER original bit
        // array than `entries.len()` would produce (e.g.
        // `test_basic.uncompressed_table`: original 16 longs vs 10 longs
        // computed here, same recovered fp_chance/hash_count). `expected_keys`
        // is therefore NOT reliably recoverable from Data.db alone for a
        // compaction-produced input, and rebuild uses the actual distinct
        // partition count instead — CORRECT for membership (a smaller filter
        // never produces a false NEGATIVE, only a possibly-different false-
        // positive rate than the original), but not necessarily byte-identical.
        // `bloom_filter_fp_chance`'s own classification below is unaffected —
        // it is a genuinely separate axis from this one.
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
            let decoded = decode::decode_one_partition(
                &reader,
                *offset,
                end_bound,
                key,
                schema,
                &scan_cancel,
            )
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

            if want_statistics {
                // The full per-mutation content fold (issue #4246): marker
                // fields (partition/range tombstone) are never row-shadowed
                // — they ARE the deletion — so fold them unconditionally
                // from the authoritative extracted values above, exactly
                // mirroring `SSTableWriter::write_partition`'s own fold.
                if let Some(pt) = partition_tombstone.as_ref() {
                    stats_acc.update_timestamp(pt.deletion_time);
                    stats_acc.update_local_deletion_time(pt.local_deletion_time);
                    stats_acc.mark_partition_level_deletion();
                }
                for rt in &range_tombstones {
                    stats_acc.update_timestamp(rt.deletion_time);
                    stats_acc.update_local_deletion_time(rt.local_deletion_time);
                }

                // Row content: `mutations` is decoded from an ALREADY-WRITTEN
                // Data.db via the same `KWayMerger` reconciliation
                // `compact_sstables`/`salvage_sstable` use (see `decode.rs`
                // doc comment), so each clustering key is already a single
                // fully-reconciled `Mutation` — the exact precondition
                // `fold_single_mutation_row_group` documents for
                // `KWayMerger::merge`'s own `PartitionEnd` handling and
                // `WriteEngine::maintenance_step`'s buffered `PartitionEnd`
                // drain (both post-merge streaming contexts, structurally
                // identical to rebuild's decode-from-disk context). A
                // marker-only mutation (no operations, no row deletion)
                // folds to nothing here — `merge_row_group` produces no row
                // for it — so this is safe to call unconditionally without
                // double-counting the marker fold above.
                let partition_floor = partition_tombstone.as_ref().map(|pt| pt.deletion_time);
                let schema_has_static = schema.columns.iter().any(|c| c.is_static);
                for mutation in &mutations {
                    if is_static_row_mutation(mutation, schema) {
                        fold_static_carrier_stats(&mut stats_acc, mutation);
                        continue;
                    }
                    let shadow_floor = resolve_shadow_floor(
                        partition_floor,
                        &range_tombstones,
                        mutation.clustering_key.as_ref(),
                        schema,
                    );
                    fold_single_mutation_row_group(
                        &mut stats_acc,
                        mutation,
                        schema,
                        schema_has_static,
                        shadow_floor,
                    );
                }
            }

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
                let entry_info =
                    iw.add_partition_with_promoted(&decorated_key, *offset, &blocks)?;
                if let Some(sw) = summary_writer.as_mut() {
                    sw.note_partition(&decorated_key);
                    if summary_sample_counter.is_multiple_of(sample_interval) {
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
            // roborev finding (Medium): `fp_chance == 1.0` disables the
            // bloom filter entirely (Cassandra's `AlwaysPresentFilter`) —
            // `finish()` writes NO `Filter.db` and removes any stale one at
            // that path. Reporting `filter` as `regenerated` regardless
            // would claim a component that does not exist on disk, and
            // `copy_untouched_components`'s `will_regenerate` closure would
            // then also skip copying the ORIGINAL Filter.db (if any),
            // leaving a `TOC.txt` naming a file `--out` never holds.
            let disabled = fw.is_disabled();
            fw.finish().await?;
            if disabled {
                report.skipped_not_applicable.push(SkippedComponent {
                    component: Component::Filter.manifest_label().to_string(),
                    reason: "bloom_filter_fp_chance == 1.0 disables the bloom filter \
                             (Cassandra's AlwaysPresentFilter) — no Filter.db component exists"
                        .to_string(),
                });
            } else {
                report
                    .regenerated
                    .push(Component::Filter.manifest_label().to_string());
            }
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
        write_statistics_component(
            dir,
            &base,
            options,
            schema,
            is_bti,
            &mut stats_acc,
            &mut report,
        )?;
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
    let mut present =
        simple::copy_untouched_components(dir, &options.out_dir, &base, |component| {
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
    // roborev finding (High): `copy_untouched_components` only walks the
    // INPUT directory, so a component that was ABSENT from the input and
    // rebuild just wrote fresh — the tool's headline use case, "a derived
    // component went missing" — was never added to `present`, and the
    // freshly-written TOC.txt silently omitted it (spec R1.3 violation).
    // Union every genuinely-regenerated component in explicitly.
    for component in [
        ("index", SSTableComponent::Index),
        ("summary", SSTableComponent::Summary),
        ("filter", SSTableComponent::Filter),
        ("digest", SSTableComponent::Digest),
        ("crc", SSTableComponent::Crc),
        ("statistics", SSTableComponent::Statistics),
    ]
    .into_iter()
    .filter_map(|(label, component)| regenerated.contains(label).then_some(component))
    {
        if !present.contains(&component) {
            present.push(component);
        }
    }
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
    stats_acc.set_repair_state(
        repair.repaired_at,
        repair.pending_repair,
        repair.is_transient,
    );

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
        fields.insert(
            field.to_string(),
            repair.provenance.manifest_label().to_string(),
        );
    }
    for field in ["origin_host", "compaction_ancestry"] {
        fields.insert(
            field.to_string(),
            FieldProvenance::Lost.manifest_label().to_string(),
        );
    }
    report
        .classification
        .insert("statistics".to_string(), fields);
    Ok(())
}
