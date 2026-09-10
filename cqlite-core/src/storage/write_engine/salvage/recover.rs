//! The recovery loop: enumerate → chunk pre-flight → decode-at-offset →
//! reconcile → write-or-record-loss (design D1, D2, D3).

use super::boundaries::{enumerate_boundaries, BoundaryEntry};
use super::chunks::{chunks_for_range, compressed_chunk_preflight, uncompressed_chunk_preflight};
use super::{
    ComponentFinding, Loss, LossClass, PartitionTotals, Refusal, RefusalReason, SalvageOptions,
    SalvageReport,
};
use crate::error::{Error, Result};
use crate::schema::TableSchema;
use crate::storage::partition_key_codec::decode_partition_key_columns;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::{
    extract_sstable_base_name, PartitionAtOffsetOutcome, SSTableReader,
};
use crate::storage::sstable::version_gate::{SsTableDescriptor, SsTableFormat};
use crate::storage::sstable::writer::{SSTableFormat as WriterFormat, SSTableWriter};
use crate::storage::write_engine::merge::{
    classify_inputs, compute_baseline_min, KWayMerger, MergeEntry, MergeStep, SSTableRowIterator,
    SSTableRowIteratorAdapter,
};
use crate::storage::write_engine::mutation::{DecoratedKey, Mutation};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};

/// A run backed by a single already-decoded partition's `MergeEntry`s (the
/// salvage recovery loop's own source, one per partition, per run of
/// [`salvage_sstable`]) — the same shape the single-partition point-read path
/// (`write_engine::merge::point_read`) uses to feed
/// [`KWayMerger::from_row_iterators`], so a healthy partition reconciles
/// through the IDENTICAL machinery `compact_sstables` uses (design D1, R1).
struct SinglePartitionRun {
    entries: VecDeque<MergeEntry>,
}

impl SSTableRowIterator for SinglePartitionRun {
    fn next(&mut self) -> Option<Result<MergeEntry>> {
        self.entries.pop_front().map(Ok)
    }
}

/// Recover every completely-decodable partition of `input` (one `Data.db`
/// file — a table dir with multiple generations is the CLI's concern, spec
/// R7) into a fresh generation under `output_dir`, per `schema`. See the
/// module docs and `openspec/changes/sstable-salvage/design.md`.
pub async fn salvage_sstable(
    input: &Path,
    output_dir: &Path,
    schema: &TableSchema,
    _options: SalvageOptions,
) -> Result<SalvageReport> {
    let descriptor = SsTableDescriptor::parse(input)?;
    let is_bti = descriptor.format == SsTableFormat::Bti;
    let generation: u64 = descriptor.sstable_id.parse().map_err(|_| {
        Error::InvalidInput(format!(
            "salvage requires a sequential-integer SSTable id (got '{}'); UUID-form SSTable ids \
             are not yet supported",
            descriptor.sstable_id
        ))
    })?;
    let dir = input.parent().ok_or_else(|| {
        Error::InvalidInput(format!(
            "input path has no parent directory: {}",
            input.display()
        ))
    })?;
    let base = extract_sstable_base_name(input).ok_or_else(|| {
        Error::InvalidInput(format!(
            "cannot derive an SSTable base name from {}",
            input.display()
        ))
    })?;

    let compressed_input = dir.join(format!("{base}-CompressionInfo.db")).exists();
    let format_label = if is_bti { "da" } else { "nb" };
    let now = chrono::Utc::now().to_rfc3339();
    let cqlite_version = env!("CARGO_PKG_VERSION").to_string();

    let report_skeleton = |boundary_source: &str, generation: u64| SalvageReport {
        input: input.display().to_string(),
        output: output_dir.display().to_string(),
        format: format_label.to_string(),
        compressed_input,
        boundary_source: boundary_source.to_string(),
        generation,
        partitions: PartitionTotals::default(),
        losses: Vec::new(),
        component_findings: Vec::new(),
        refused: None,
        now: now.clone(),
        cqlite_version: cqlite_version.clone(),
    };

    // Design D3 / spec R4: boundaries come ONLY from the authoritative
    // source; a damaged one refuses before anything else is attempted.
    let boundaries = match enumerate_boundaries(dir, &base, is_bti) {
        Ok(b) => b,
        Err(refusal) => {
            let mut report = report_skeleton(if is_bti { "bti-trie" } else { "index" }, generation);
            report.refused = Some(refusal);
            return Ok(report);
        }
    };
    let boundary_label = boundaries.kind.manifest_label();

    // roborev, issue #4196 (batched finding b): the boundary source was fine
    // (we would already have refused above otherwise) — a failure HERE is a
    // DIFFERENT component (most likely `CompressionInfo.db`, since opening
    // reads it eagerly for a compressed input) being unreadable. That is a
    // classified refusal per design D3, not a hard `Err` that skips straight
    // past the manifest: a corrupt `CompressionInfo.db`/`Statistics.db` is
    // one of the likeliest damage modes an operator reaches for this tool
    // over, and it must still produce a manifest naming the cause.
    let reader = match open_reader(input).await {
        Ok(r) => r,
        Err(e) => {
            let mut report = report_skeleton(boundary_label, generation);
            report.refused = Some(component_unreadable_refusal("opening the input", &e));
            return Ok(report);
        }
    };

    // roborev, issue #4196 (round-4 Medium): the chunk pre-flight reads
    // `Data.db` (compressed and uncompressed) and, for the uncompressed
    // branch, `CRC.db` — a component failure HERE (a corrupt `CRC.db` header,
    // an oversized sidecar, or any I/O error opening `Data.db` for the
    // pre-flight) is the SAME defect class the `open_reader`/`classify_inputs`
    // refusal-classification above fixed: it must not `?`-propagate a hard
    // `Err` that skips the manifest entirely.
    let mut component_findings: Vec<ComponentFinding> = Vec::new();
    let (bad_chunks, chunk_size, data_length): (std::collections::BTreeSet<u64>, u64, u64) =
        if let Some(ci) = reader.compression_info.as_deref() {
            let preflight = match compressed_chunk_preflight(&reader_data_path(input), ci) {
                Ok(p) => p,
                Err(e) => {
                    let mut report = report_skeleton(boundary_label, generation);
                    report.refused = Some(component_unreadable_refusal(
                        "the compressed chunk pre-flight (Data.db)",
                        &Error::Io(e),
                    ));
                    return Ok(report);
                }
            };
            if let Some(f) = preflight.finding {
                component_findings.push(f);
            }
            (
                preflight.bad_chunks,
                preflight.chunk_size,
                preflight.data_length,
            )
        } else {
            let crc_path = dir.join(format!("{base}-CRC.db"));
            let preflight = match uncompressed_chunk_preflight(
                &reader_data_path(input),
                &crc_path,
                reader.calculate_header_size(),
            )
            .await
            {
                Ok(p) => p,
                Err(e) => {
                    let mut report = report_skeleton(boundary_label, generation);
                    report.refused = Some(component_unreadable_refusal(
                        "the uncompressed chunk pre-flight (Data.db/CRC.db)",
                        &e,
                    ));
                    return Ok(report);
                }
            };
            if let Some(f) = preflight.finding {
                component_findings.push(f);
            }
            (
                preflight.bad_chunks,
                preflight.chunk_size,
                preflight.data_length,
            )
        };

    let mut report = report_skeleton(boundary_label, generation);
    report.component_findings = component_findings;
    report.partitions.total = boundaries.entries.len();

    let mut writer_format = WriterFormat::Big;
    if is_bti {
        writer_format = WriterFormat::Bti;
    }
    // roborev, issue #4196 (batched finding b): construction failures are
    // classified refusals, same reasoning as `open_reader` above —
    // `report` already exists at this point (unlike the `open_reader` site),
    // so it is populated with what was already discovered (component
    // findings, `partitions.total`) rather than dropped.
    let mut writer = match SSTableWriter::with_format(
        output_dir.to_path_buf(),
        generation,
        schema,
        boundaries.entries.len().max(1),
        writer_format,
    ) {
        Ok(w) => w,
        Err(e) => {
            report.refused = Some(component_unreadable_refusal(
                "constructing the output writer",
                &e,
            ));
            return Ok(report);
        }
    };
    let input_paths = vec![input.to_path_buf()];
    let repair_state = match classify_inputs(&input_paths) {
        Ok(rs) => rs,
        Err(e) => {
            report.refused = Some(component_unreadable_refusal(
                "reading the input's repair state (Statistics.db)",
                &e,
            ));
            return Ok(report);
        }
    };
    writer.set_repair_state(
        repair_state.repaired_at,
        repair_state.pending_repair,
        repair_state.is_transient,
    );
    writer.mark_compaction_output();
    let (min_ts, min_ldt, min_ttl) = compute_baseline_min(&input_paths);
    writer.pre_seed_encoding_baselines(min_ts, min_ldt, min_ttl);

    let scan_cancel = ScanCancel::default();
    let mut recovered = 0usize;
    // Partitions actually passed to `writer.write_partition` — DISTINCT from
    // `recovered` (roborev, issue #4196): a partition that decoded but
    // reconciled to nothing (`Ok(None)`) counts toward `recovered` but not
    // `written`, so `finish()` below is gated on real write activity, not
    // merely "nothing failed to decode".
    let mut written = 0usize;
    let mut losses: Vec<Loss> = Vec::new();

    for (i, entry) in boundaries.entries.iter().enumerate() {
        let end_bound = boundaries.entries.get(i + 1).map(|e| e.data_offset);
        // For the LAST partition (no next boundary entry), the chunk-range
        // mapping's `end` comes from the preflight's own `data_length` (the
        // SAME end resolution `decode_partition_at_offset_for_salvage` uses),
        // never a synthetic `data_offset + 1` — that one-byte-wide window
        // only ever names the chunk containing the partition's START, so a
        // CRC failure in any LATER chunk the last partition's bytes actually
        // span was invisible to the pre-flight (roborev, issue #4196).
        let chunk_range_end = end_bound.unwrap_or_else(|| {
            if data_length > entry.data_offset {
                data_length
            } else {
                entry.data_offset + 1
            }
        });

        let touched_chunks: Vec<u64> = if chunk_size > 0 {
            chunks_for_range(entry.data_offset, chunk_range_end, chunk_size)
        } else {
            Vec::new()
        };
        let bad_touched: Vec<u64> = touched_chunks
            .iter()
            .copied()
            .filter(|c| bad_chunks.contains(c))
            .collect();
        if !bad_touched.is_empty() {
            losses.push(build_loss(
                entry,
                schema,
                bad_touched,
                LossClass::ChunkCrc,
                0,
                "partition's byte range intersects a chunk that failed CRC validation".to_string(),
            ));
            continue;
        }

        match recover_one_partition(&reader, entry, end_bound, schema, &scan_cancel).await {
            Ok(Some((key, mutations))) => {
                writer.write_partition(key, mutations)?;
                recovered += 1;
                written += 1;
            }
            Ok(None) => {
                // Every mutation shadowed itself out during reconciliation
                // (e.g. a lone range-tombstone carrier over an empty range) —
                // nothing to write, but not a loss: the partition decoded
                // completely and correctly. Counted in `recovered` (it DID
                // decode) but deliberately NOT in `written` (roborev, issue
                // #4196): `finish()` below is gated on `written`, not
                // `recovered`, so a file whose EVERY partition takes this
                // path (nothing genuinely written anywhere) still refuses
                // rather than emitting a phantom empty-but-valid component
                // set. For a BTI NARROW partition (`entry.expected_key ==
                // None`) this decode could not be key-validated at all — the
                // format carries nothing to check it against — so it is also
                // named as a component finding for operator visibility
                // rather than silently trusted.
                recovered += 1;
                if entry.expected_key.is_none() {
                    report.component_findings.push(ComponentFinding {
                        class: "UnverifiedEmptyDecode".to_string(),
                        component: "Data.db".to_string(),
                        detail: format!(
                            "partition at offset {} (BTI narrow leaf, no independently-known key \
                             to validate) decoded to zero rows; accepted as a legitimate empty \
                             reconciliation but could not be cross-checked",
                            entry.data_offset
                        ),
                    });
                }
            }
            Err((class, rows_before, message)) => {
                // roborev, issue #4196: name the chunks this partition's
                // range intersects on EVERY loss class when known (compressed
                // input), not just chunk-crc — useful for a manual look even
                // when the CRC itself was clean but the decode still failed.
                losses.push(build_loss(
                    entry,
                    schema,
                    touched_chunks,
                    class,
                    rows_before,
                    message,
                ));
            }
        }
    }

    // Spec R5.2 / design D3: zero partitions decodable REFUSES and writes NO
    // `Data.db`. `SSTableWriter` opens `Data.db` lazily on the FIRST
    // `write_partition` call (creating `output_dir/keyspace/table/` as
    // needed — see `SSTableWriter::with_format_and_registry`'s doc), so when
    // `written == 0` (roborev, issue #4196: gated on `written`, NOT
    // `recovered` — a file whose every partition reconciled to `Ok(None)`
    // has `recovered > 0` yet wrote nothing) no partition was ever written
    // and no file exists yet; calling `finish()` here would still emit an
    // empty-but-valid component set (Statistics.db, TOC.txt, ...) for zero
    // written partitions, which is exactly the "no Data.db" contract's
    // violation. Drop the writer unfinished instead — nothing on disk to
    // clean up in that case.
    if written > 0 {
        let output = writer.finish().await?;
        let _ = output; // SSTableInfo carries stats the manifest does not restate.
    } else {
        drop(writer);
    }

    report.partitions.recovered = recovered;
    report.partitions.lost = losses.len();
    report.losses = losses;

    if written == 0 {
        report.refused = Some(Refusal {
            reason: RefusalReason::NothingDecodable,
            remedy: "no partition could be recovered; inspect the losses above".to_string(),
        });
    }

    Ok(report)
}

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

/// `input` names the `Data.db` file itself already (the salvage contract) —
/// this alias exists only so the chunk-preflight call sites read clearly.
fn reader_data_path(input: &Path) -> PathBuf {
    input.to_path_buf()
}

/// Build a [`RefusalReason::ComponentUnreadable`] [`Refusal`] (roborev, issue
/// #4196, batched finding b) — `context` names WHAT was being attempted
/// (`"opening the input"`, `"reading the input's repair state
/// (Statistics.db)"`, ...) and `error` is the underlying failure's `Display`,
/// both folded into the remedy so an operator sees the actual cause rather
/// than a generic "refused" with no lead. Unlike
/// [`RefusalReason::BoundarySourceUnreadable`] this does NOT point at
/// `cqlite rebuild --components index` (#4197) — the boundary source was
/// fine here, so that remedy would send an operator at the wrong component;
/// `cqlite verify --mode full` is named instead, to let the operator
/// identify which component is actually damaged before deciding a next step.
fn component_unreadable_refusal(context: &str, error: &dyn std::fmt::Display) -> Refusal {
    Refusal {
        reason: RefusalReason::ComponentUnreadable,
        remedy: format!(
            "component unreadable while {context}: {error} — run `cqlite verify --mode full` on \
             this input to identify the damaged component; salvage cannot proceed without it"
        ),
    }
}

/// Decode + reconcile ONE partition. `Ok(Some((key, mutations)))` on success,
/// `Ok(None)` when the partition decoded but reconciled to nothing to write,
/// `Err((class, rows_decoded_before_failure, message))` names a loss.
async fn recover_one_partition(
    reader: &SSTableReader,
    entry: &BoundaryEntry,
    end_bound: Option<u64>,
    schema: &TableSchema,
    scan_cancel: &ScanCancel,
) -> std::result::Result<Option<(DecoratedKey, Vec<Mutation>)>, (LossClass, usize, String)> {
    let outcome = reader
        .decode_partition_at_offset_for_salvage(
            entry.data_offset,
            end_bound,
            entry.expected_key.as_deref(),
            Some(schema),
            scan_cancel,
        )
        .await
        .map_err(|e| (LossClass::Decode, 0, e.to_string()))?;

    let rows = match outcome {
        PartitionAtOffsetOutcome::Rows(rows) => rows,
        PartitionAtOffsetOutcome::KeyMismatch => {
            return Err((
                LossClass::KeyMismatch,
                0,
                "decoded key at this offset does not match the boundary source's key for this \
                 slot"
                    .to_string(),
            ));
        }
        PartitionAtOffsetOutcome::DecodeError {
            rows_decoded_before_failure,
            error,
        } => {
            return Err((
                LossClass::Decode,
                rows_decoded_before_failure,
                error.to_string(),
            ));
        }
        PartitionAtOffsetOutcome::Truncated => {
            return Err((
                LossClass::Truncated,
                0,
                "partition's authoritative byte range extends past Data.db's actual end"
                    .to_string(),
            ));
        }
    };

    let mut merge_entries = Vec::with_capacity(rows.len());
    for (idx, row) in rows.into_iter().enumerate() {
        match SSTableRowIteratorAdapter::build_merge_entry(0, row, schema) {
            Ok(me) => merge_entries.push(me),
            Err(e) => return Err((LossClass::Decode, idx, e.to_string())),
        }
    }

    let run = SinglePartitionRun {
        entries: merge_entries.into(),
    };
    let mut merger = KWayMerger::from_row_iterators(vec![Box::new(run)], schema)
        .map_err(|e| (LossClass::Decode, 0, e.to_string()))?;
    let reconciled = merger
        .step()
        .map_err(|e| (LossClass::Decode, 0, e.to_string()))?;
    let (key, entries) = match reconciled {
        MergeStep::Partition { key, rows } => (key, rows),
        MergeStep::Complete => return Ok(None),
    };
    if entries.is_empty() {
        return Ok(None);
    }

    let mut mutations = Vec::with_capacity(entries.len());
    for e in entries {
        let m = KWayMerger::merge_entry_to_mutation(e, schema)
            .map_err(|err| (LossClass::Decode, 0, err.to_string()))?;
        mutations.push(m);
    }
    Ok(Some((key, mutations)))
}

fn build_loss(
    entry: &BoundaryEntry,
    schema: &TableSchema,
    chunks: Vec<u64>,
    class: LossClass,
    rows_decoded_before_failure: usize,
    message: String,
) -> Loss {
    // roborev, issue #4196: a BTI narrow (`DataOffset`) leaf carries no raw
    // key at all (see `BoundaryEntry::diagnostic_prefix`'s doc) — reporting
    // an empty `key_hex` there gave an operator nothing to locate the slot
    // by. Fall back to the trie's byte-comparable prefix, clearly labelled
    // as such (never presented as the raw key).
    if let Some(key_bytes) = &entry.expected_key {
        // roborev, issue #4196, round-6 Low finding: the Rust `Debug`
        // spelling of the decoded column vector is unstable across refactors
        // and not documented anywhere in the JSON manifest's contract — use
        // `Value`'s own stable `Display` rendering instead, comma-joined
        // `name=value` per column (matches the CLI's own row-value output).
        let key = decode_partition_key_columns(key_bytes, schema).ok().map(
            |cols: Vec<(String, crate::Value)>| {
                cols.iter()
                    .map(|(name, value)| format!("{name}={value}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            },
        );
        Loss {
            key_hex: hex::encode(key_bytes),
            key,
            data_offset: entry.data_offset,
            chunks,
            class,
            rows_decoded_before_failure,
            message,
        }
    } else if let Some(prefix) = &entry.diagnostic_prefix {
        Loss {
            key_hex: hex::encode(prefix),
            key: Some(
                "(BTI trie byte-comparable prefix — the raw key is not carried by the boundary \
                 source for this narrow partition; see Data.db at this offset)"
                    .to_string(),
            ),
            data_offset: entry.data_offset,
            chunks,
            class,
            rows_decoded_before_failure,
            message,
        }
    } else {
        Loss {
            key_hex: String::new(),
            key: None,
            data_offset: entry.data_offset,
            chunks,
            class,
            rows_decoded_before_failure,
            message,
        }
    }
}
