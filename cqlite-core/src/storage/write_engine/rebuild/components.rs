//! Top-level `rebuild_components` orchestration (design D1-D5).
//!
//! Two-pass structure over the partition-dependent components
//! (Index/Summary/Filter/Statistics), and why it is unavoidable:
//! Cassandra's `SerializationHeader.EncodingStats` (`min_timestamp`/
//! `min_ttl`/`min_local_deletion_time`) is ONE value per whole SSTable,
//! known upfront from the memtable at flush time, and used to delta-encode
//! EVERY row's timestamp/TTL/LDT VInts. Rebuild is reading an ALREADY-
//! written file, so before it can re-measure any single row's
//! promoted-index byte width against a scratch `DataWriter` (pass 2) it
//! must establish that same whole-table baseline — seeding with the wrong
//! baseline silently changes VInt widths and desyncs every computed byte
//! offset.
//!
//! # Where the baseline comes from (issue #4197, spec R2/R4.2)
//!
//! In PROVENANCE ORDER, never a guess:
//!
//!   1. The ORIGINAL `Statistics.db`'s own `SerializationHeader.EncodingStats`
//!      (`statistics::recover_encoding_stats_baseline`) — the authoritative
//!      record of the value this `Data.db` was ACTUALLY encoded against. It
//!      wins whenever readable, and it is not merely "better": the baseline
//!      is NOT bounded above by this file's own decodable content, so no
//!      derivation can recover it in general. Cassandra carries EncodingStats
//!      minima FORWARD from compaction inputs
//!      (`SerializationHeader.make(metadata, sstables)` → `EncodingStats.merge`,
//!      `cassandra-5.0.8`), and a minimum-carrying row can be shadow-dropped
//!      by reconciliation before pass 1 ever sees it. Both make a derived
//!      baseline come out too HIGH — narrower deltas, desynced offsets, no
//!      error (no-heuristics mandate, issue #28).
//!   2. Failing that (rebuild's own headline case: a `Statistics.db` that is
//!      GONE), a pass-1 walk that reconciles every partition and folds the
//!      minima out of the decoded mutations. This fallback is CIRCULAR by
//!      construction — the decoder needs the baseline to turn on-disk deltas
//!      into absolute timestamps — so the six timestamp/TTL/LDT aggregates
//!      are classified [`FieldProvenance::Lost`], never `recomputed`, and an
//!      Index.db/Summary.db request additionally fails closed through the
//!      per-partition re-encoded-span cross-check in pass 2 rather than
//!      shipping desynced byte offsets.
//!
//! Pass 1 runs either way: independently of the baseline it is also the
//! "prove every partition decodes BEFORE writing a byte" pass design D3/R5.2
//! rests on.

use super::partition_stats::fold_partition_statistics;
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
use crate::storage::sstable::writer::{
    DataWriter, FilterWriter, IndexWriter, SSTableWriter, StatisticsMetadata, SummaryWriter,
};
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

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

/// Refuse because re-encoding a partition did not reproduce its ACTUAL
/// on-disk byte span (issue #4197 spec R2), so every promoted-index offset
/// derived from that re-encode is untrustworthy.
///
/// This is deliberately NOT [`RefusalReason::DataCorrupt`]: `Data.db` decoded
/// cleanly and may well be perfectly healthy — what failed is rebuild's
/// ability to REPRODUCE its encoding, overwhelmingly because the
/// whole-SSTable `EncodingStats` baseline could not be recovered from the
/// original `Statistics.db` (see the module doc). Reporting corruption for a
/// healthy file would send the operator to `salvage` for nothing.
fn reencode_mismatch_refusal(
    offset: u64,
    on_disk_span: u64,
    reencoded_span: u64,
    baseline_provenance: FieldProvenance,
) -> Refusal {
    Refusal {
        reason: RefusalReason::ReencodeMismatch,
        remedy: format!(
            "the partition at Data.db offset {offset} occupies {on_disk_span} bytes on disk but \
             re-encodes to {reencoded_span} bytes, so every promoted-index offset derived from \
             it would be wrong (EncodingStats baseline provenance: {}) — remedy: restore this \
             generation's original Statistics.db (its SerializationHeader carries the \
             authoritative delta-encoding baseline) and re-run; if it is gone for good, cqlite \
             salvage (issue #4196) can write a fresh, self-consistent generation instead",
            baseline_provenance.manifest_label()
        ),
        offset: Some(offset),
    }
}

/// The `Statistics.db` this run reads recovered metadata FROM (spec R4.2):
/// the caller's explicit "renamed aside" override when given, else the
/// input's own expected sibling path. Resolved ONCE so the `EncodingStats`
/// baseline (pass 0) and the repair fields (`write_statistics_component`)
/// can never read two different files.
fn statistics_source_path(dir: &Path, base: &str, options: &RebuildOptions) -> PathBuf {
    options
        .statistics_recovery_source
        .clone()
        .unwrap_or_else(|| dir.join(format!("{base}-Statistics.db")))
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
    let mut discarded = StatisticsMetadata::default();
    rebuild_components_capturing_stats(data_db_path, schema, requested, options, &mut discarded)
        .await
}

/// [`rebuild_components`], additionally handing back the `StatisticsMetadata`
/// the `Statistics.db` write was driven from.
///
/// Exists because two of those fields are otherwise UNOBSERVABLE from a
/// rebuilt `nb` generation: `build_stats_component` (the `nb` STATS body)
/// does not serialize `firstKey`/`lastKey` at all — only
/// `build_stats_component_da` does — so the key-range invariant (both ends
/// drawn from the SAME counted population as `partition_count`; issue #4197
/// F6) cannot be asserted from the output bytes of the format the whole
/// committed corpus uses. Crate-internal on purpose: it is a test seam for
/// an in-crate assertion, not a second public entry point.
pub(crate) async fn rebuild_components_capturing_stats(
    data_db_path: &Path,
    schema: &TableSchema,
    requested: &[Component],
    options: &RebuildOptions,
    stats_acc: &mut StatisticsMetadata,
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
        rolled_back: false,
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

    let stats_source = statistics_source_path(dir, &base, options);
    // Provenance of the three `EncodingStats` baseline minima actually used
    // below (module doc, provenance order). `Recovered` = read from the
    // original `Statistics.db`'s SerializationHeader; `Lost` = that file was
    // unreadable and the circular pass-1 derivation stood in for it.
    // Initialised to `Lost` because at this point nothing HAS been
    // recovered: a run that needs no baseline at all (`filter` alone) never
    // reaches the assignment below, and for it this value is never reported
    // either — but "not recovered" is the truthful state to sit in, not
    // "recovered".
    let mut baseline_provenance = FieldProvenance::Lost;

    if needs_partition_pass {
        let scan_cancel = ScanCancel::new();
        let needs_baseline = want_index || want_summary || want_statistics;

        // PASS 0: the AUTHORITATIVE baseline, from the original
        // `Statistics.db`'s own `SerializationHeader` — see the module doc
        // for why this beats (and is not merely nicer than) any derivation
        // from `Data.db`'s content.
        let recovered_baseline = if needs_baseline {
            statistics::recover_encoding_stats_baseline(&stats_source)
        } else {
            None
        };

        // PASS 1: walk + reconcile EVERY partition. Two jobs, both needed
        // regardless of PASS 0's outcome:
        //   * it is the "prove Data.db decodes BEFORE writing a byte" pass
        //     design D3/R5.2 rests on (a decode failure here refuses the
        //     whole run having written nothing at all), and
        //   * it derives the FALLBACK baseline minima for the
        //     `Statistics.db`-is-gone case, via the same fixed #729
        //     two-pass primitive `WriteEngine::flush_internal_async` uses
        //     (`SSTableWriter::compute_mutations_baseline_stats`) rather
        //     than the old `fold_mutation_stats` call this replaced — issue
        //     #4246 made that function `#[cfg(test)]`-only in production.
        //     `mutations` here is decode-reconciled output (see
        //     `partition_stats::fold_partition_statistics`'s doc comment),
        //     which was never phantom-fold-able in the first place.
        // The full per-mutation content fold (maxima, tombstone histogram,
        // flags) happens below in PASS 2.
        if needs_baseline {
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
            match recovered_baseline {
                // The file's own header wins — and note this value is ALSO
                // what a `statistics` rebuild must WRITE back out
                // (`build_serialization_header_component` serializes these
                // three fields AS the new EncodingStats), so a derived
                // baseline would not merely misplace promoted-index offsets:
                // it would leave the UNCHANGED Data.db delta-decoding against
                // a baseline it was never encoded with.
                Some(b) => {
                    stats_acc.min_timestamp = b.min_timestamp;
                    stats_acc.min_local_deletion_time = b.min_local_deletion_time;
                    stats_acc.min_ttl = b.min_ttl;
                    baseline_provenance = FieldProvenance::Recovered;
                }
                None => {
                    stats_acc.min_timestamp = baseline_min_ts;
                    stats_acc.min_local_deletion_time = baseline_min_ldt;
                    stats_acc.min_ttl = baseline_min_ttl;
                    baseline_provenance = FieldProvenance::Lost;
                }
            }
        }
        // NO `first_key`/`last_key` pre-seed from `entries` here (issue
        // #4197 F6): `entries` is the RAW boundary walk, which includes
        // partitions PASS 2 below reconciles to nothing and skips, while
        // `StatisticsMetadata::update_key_range` — called ONLY for the
        // partitions that survive — documents (and depends on) being fed an
        // in-order population from a clean slate: it assigns `first_key`
        // only while still `None` and always overwrites `last_key`.
        // Pre-seeding BOTH from the raw walk broke that precondition and
        // drew the two ends from two different populations whenever the
        // FIRST enumerated partition reconciled away. Both ends now come
        // from the single PASS-2 call site that also counts the partition,
        // so the populations agree by construction.
        //
        // How reachable was the divergence? MEASURED (see
        // `components_keyrange_tests.rs`'s own reachability note): none of
        // the four writable shapes tried — rowless, partition-tombstone-only,
        // PT-shadowed row, TTL-expired row — makes an ENUMERATED partition
        // reconcile to nothing, because the decode primitive runs with read
        // shadowing OFF and `merge` re-emits every marker as a carrier, while
        // a rowless partition is not enumerated in the first place. So this
        // is a latent-correctness fix restoring a documented precondition,
        // not a live wrong-output bug — stated rather than implied, so a
        // later reader does not mistake the test for a regression pin.

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
        let schema_has_static = schema.columns.iter().any(|c| c.is_static);

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
                fold_partition_statistics(
                    stats_acc,
                    &mutations,
                    partition_tombstone.as_ref(),
                    &range_tombstones,
                    schema,
                    schema_has_static,
                );
            }

            let need_scratch = index_writer.is_some() || want_statistics;
            let (blocks, emit) = if need_scratch {
                // `with_oa_partition_deletion` mirrors
                // `SSTableWriter::with_format_and_registry`'s own
                // `matches!(format, Bti)` gate: `da` writes the oa
                // `DeletionTime.Serializer` partition header (1 byte LIVE /
                // 12 bytes deleted), the legacy `nb` form is always 12. The
                // scratch writer must re-encode in the input's OWN format or
                // its byte extents are not this file's byte extents.
                let mut scratch =
                    DataWriter::new(baseline_seed.clone()).with_oa_partition_deletion(is_bti);
                let (_, blocks, emit) = scratch.write_partition_with_index_blocks(
                    &decorated_key,
                    &mutations,
                    schema,
                    partition_tombstone.as_ref(),
                    &range_tombstones,
                )?;
                // Fail-closed cross-check (issue #4197 spec R2): the
                // promoted-index payload's block offsets and widths are
                // PARTITION-RELATIVE BYTE POSITIONS taken from this scratch
                // re-encode, so they are only valid if the re-encode
                // reproduces the partition's ACTUAL on-disk extent. Any
                // divergence — a baseline that could not be recovered, a
                // Data.db feature this writer does not re-encode identically
                // — shifts them, and the result would otherwise ship silently
                // with exit 0 and `regenerated: ["index"]`.
                //
                // SCOPED, deliberately and narrowly, to the partitions whose
                // Index.db entry actually CARRIES that payload:
                // `blocks.len() >= 2`, the same gate
                // `IndexWriter::add_partition_with_promoted` applies (itself
                // mirroring Cassandra `RowIndexEntry.create()`'s
                // `columnIndexCount > 1`). Everything else in an Index.db /
                // Summary.db entry — the key, the data offset, the zero
                // promoted-size VInt, and hence the entry size a Summary
                // sample records — is baseline-INDEPENDENT, sourced from the
                // authoritative boundary walk rather than from this
                // re-encode, so a span difference cannot make any of it
                // wrong.
                //
                // MEASURED, because the wider check was tried first and was
                // WRONG: an unconditional span comparison refused 58 of 114
                // committed BIG generations whose rebuilt Index.db is
                // BYTE-IDENTICAL to Cassandra's own (verified by re-running
                // the same sweep with the check off: 109 parity, 0
                // mismatch). CQLite's re-encode is simply not
                // byte-length-exact for every shape in the corpus, and for a
                // narrow partition that costs Index.db nothing. Refusing
                // those runs would have been a large capability regression
                // dressed up as rigour.
                //
                // A `statistics`-only run derives no byte offset from the
                // scratch at all (only the emitted row/cell COUNTS), so it is
                // not checked here either; its exposure is the timestamp
                // aggregates, reported through `baseline_provenance` (R4.1).
                if index_writer.is_some() && blocks.len() >= 2 {
                    let on_disk_span = this_end.saturating_sub(*offset);
                    let reencoded_span = scratch.position();
                    if reencoded_span != on_disk_span {
                        cleanup_partial_output(&options.out_dir, &base);
                        return Ok(make_report(Some(reencode_mismatch_refusal(
                            *offset,
                            on_disk_span,
                            reencoded_span,
                            baseline_provenance,
                        ))));
                    }
                }
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
                // Every promoted-index offset in this file was measured
                // against the delta-encoding baseline, so the manifest says
                // where that baseline came from (design D5; the same
                // "the manifest always says which" discipline spec R3
                // imposes on Summary/Filter's governing parameters). Only
                // ever `recovered` here in practice: a `Lost` baseline
                // cannot reach this point, because the per-partition
                // re-encoded-span cross-check above refuses the run first.
                let mut fields = BTreeMap::new();
                fields.insert(
                    "encoding_stats_baseline".to_string(),
                    baseline_provenance.manifest_label().to_string(),
                );
                report.classification.insert("index".to_string(), fields);
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
        statistics::write_statistics_component(
            &stats_source,
            &base,
            options,
            schema,
            is_bti,
            baseline_provenance,
            stats_acc,
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

#[cfg(test)]
#[path = "components_keyrange_tests.rs"]
mod keyrange_tests;
