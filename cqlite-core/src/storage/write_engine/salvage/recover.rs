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

    // roborev, issue #4196, round-11 Low finding: `output` previously
    // recorded `output_dir` (the `--out` ROOT) verbatim, but
    // `SSTableWriter` actually nests every generation under
    // `<out>/<keyspace>/<table>/` — an operator (or a script) reading the
    // manifest's `output` field to locate the recovered generation looked
    // in a directory holding only a keyspace subdirectory. Record the
    // RESOLVED path instead, so the field genuinely names where output
    // lands.
    let resolved_output = output_dir
        .join(&schema.keyspace)
        .join(&schema.table)
        .display()
        .to_string();
    let report_skeleton = |boundary_source: &str, generation: u64| SalvageReport {
        input: input.display().to_string(),
        output: resolved_output.clone(),
        format: format_label.to_string(),
        compressed_input,
        boundary_source: boundary_source.to_string(),
        generation,
        partitions: PartitionTotals::default(),
        losses: Vec::new(),
        component_findings: Vec::new(),
        attempted: false,
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
    // roborev, issue #4196, round-9 (spec R2.4/R3.1's own oracle: a
    // bit-flipped-but-still-parseable `Index.db`/`Partitions.db` entry can
    // be `data_offset`-ascending — `check_strictly_ascending` above already
    // guards that — while its DECODED KEY still fails to token-sort after
    // the partition BEFORE it, which only `SSTableWriter::write_partition`'s
    // own ordering check would have caught, deep in the write path, as a
    // hard `Err` that discarded every loss/finding gathered so far and left
    // an unpublished partial generation with NO manifest at all — three
    // roborev rounds (6, 7, 8) independently surfaced this. Mirroring
    // `write_partition`'s OWN check (`writer/mod.rs`: `key.token <=
    // last_token`) HERE, before ever calling it, converts that class of
    // corruption into an ordinary classified `Loss` like every other
    // boundary-source disagreement — never reaching the writer at all, so
    // it can never fail with a partition already-decoded-and-about-to-be-
    // written. The genuine, RESIDUAL failure mode this does NOT cover — a
    // real I/O error inside `write_partition`/`finish()` unrelated to
    // ordering — stays `?`-propagated, matching `compact_sstables`'s own
    // established posture at the identical call (`merge/mod.rs:1341`) and
    // round-4's explicit scoping note: building a writer abort/cleanup
    // mechanism for THAT residual case is disproportionate for a fix round.
    let mut last_written_token: Option<i64> = None;
    // roborev, issue #4196, round-9 Low finding: every EARLIER possible
    // refusal (boundary source, `open_reader`, chunk pre-flight, writer
    // construction, `classify_inputs`) has already returned by this point —
    // the per-partition loop is genuinely about to run, so this is the
    // single, correct place to mark the report as having been attempted.
    report.attempted = true;

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

        // roborev, issue #4196, round-9 High finding: for a NON-last
        // partition, `chunk_range_end` comes directly from the NEXT
        // boundary entry's `data_offset` — an unbounded, unvalidated VInt
        // (`parse_big_index_entry` reads it with no sanity check against
        // the file's real length, and `check_strictly_ascending` enforces
        // only ORDER, never plausibility). A single flipped byte in ANY
        // later entry can therefore make THIS partition's chunk range
        // computation try to materialize an astronomically large `Vec<u64>`
        // (measured: an offset near `u64::MAX` implies ~1.4e14 chunk
        // indices, ~1.1 PB) before a single partition is decoded — OOM,
        // rather than the classified refusal/manifest the whole design
        // promises. `data_length` (`CompressionInfo.data_length`, or the
        // uncompressed CRC.db scan's actual byte count) is the REAL,
        // independently-measured total, already size-bounded at its own
        // parse site — clamp `chunk_range_end` to it, and refuse to
        // materialize a chunk range at ALL when this partition's own
        // `data_offset` is already at or past it (an implausible position
        // no real chunk can hold; classified `Truncated` immediately,
        // matching `decode_partition_at_offset_for_salvage`'s own
        // past-EOF signal one layer up).
        if data_length > 0 && entry.data_offset >= data_length {
            losses.push(build_loss(
                entry,
                schema,
                Vec::new(),
                LossClass::Truncated,
                0,
                format!(
                    "partition's declared start offset {} is at or past the measured data \
                     length {} — the boundary source names an implausible position",
                    entry.data_offset, data_length
                ),
            ));
            continue;
        }
        let chunk_range_end = if data_length > 0 {
            chunk_range_end.min(data_length)
        } else {
            chunk_range_end
        };

        let touched_chunks: Vec<u64> = if chunk_size > 0 {
            chunks_for_range(entry.data_offset, chunk_range_end, chunk_size)
        } else {
            Vec::new()
        };
        // O(hits), not O(range): the only OTHER consumer of the full
        // `touched_chunks` Vec is `Loss.chunks` on a non-`chunk-crc` loss
        // below, which needs the materialized (now clamped, so bounded)
        // list; this filter does not, so query the `BTreeSet` directly
        // rather than re-scanning every touched index (roborev, issue
        // #4196, round-9 High finding).
        let bad_touched: Vec<u64> = match (touched_chunks.first(), touched_chunks.last()) {
            (Some(&first), Some(&last)) => bad_chunks.range(first..=last).copied().collect(),
            _ => Vec::new(),
        };
        if !bad_touched.is_empty() {
            // roborev, issue #4196, round-10 Low finding: `Loss.chunks` is
            // documented (`mod.rs`) as the chunks this partition's byte
            // range INTERSECTS — every OTHER loss class passes the full
            // `touched_chunks`, so this arm passing only the FAILING subset
            // (`bad_touched`) made the same JSON field mean two different
            // things depending on `class`. Pass the full intersecting range
            // here too, for consistency, and name the failing subset in the
            // message text instead (where a human/consumer wanting "why"
            // still finds it, distinct from "where").
            let bad_touched_desc = format!("{bad_touched:?}");
            losses.push(build_loss(
                entry,
                schema,
                touched_chunks,
                LossClass::ChunkCrc,
                0,
                format!(
                    "partition's byte range intersects a chunk that failed CRC validation \
                     (failing chunk(s): {bad_touched_desc})"
                ),
            ));
            continue;
        }

        match recover_one_partition(&reader, entry, end_bound, schema, &scan_cancel).await {
            Ok(Some((key, mutations))) => {
                // roborev, issue #4196, round-9 Low finding: bind `last`
                // via `if let` rather than re-`expect()`ing
                // `last_written_token` inside the branch — no `unwrap()`/
                // `expect()` in library code (project standard), and this
                // way the value used in the message is the SAME one
                // `token_out_of_order` compared against, not re-derived.
                if let Some(last) = last_written_token {
                    if token_out_of_order(Some(last), key.token) {
                        // Decoded cleanly, but this slot's key does not
                        // token-sort after the last partition actually
                        // written — the boundary source and the data
                        // disagree on ordering, which `write_partition`
                        // would otherwise reject as a hard `Err`.
                        // Classified here instead, as a loss, never
                        // reaching the writer.
                        losses.push(build_loss(
                            entry,
                            schema,
                            touched_chunks,
                            LossClass::KeyMismatch,
                            0,
                            format!(
                                "decoded key's token {} does not sort after the last partition \
                                 actually written (token {last}); the boundary source and the \
                                 data disagree on ordering",
                                key.token
                            ),
                        ));
                        continue;
                    }
                }
                let token = key.token;
                writer.write_partition(key, mutations)?;
                last_written_token = Some(token);
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
    // roborev, issue #4196, round-13 Medium finding: surface the
    // recovered-but-not-written residue (the `Ok(None)` arm above) in the
    // manifest so a partial silent-drop run — some partitions `Ok(None)`,
    // the rest written normally, `losses` still empty — is distinguishable
    // from a run that wrote every recovered partition's content, instead of
    // both reporting `recovered=N lost=0` and exiting 0.
    report.partitions.written = written;
    report.losses = losses;

    if written == 0 {
        // roborev, issue #4196, round-12 Low finding: the remedy text
        // unconditionally read "inspect the losses above", but `written ==
        // 0` has TWO distinct causes — genuine losses (a non-empty
        // `losses`), or every partition decoding CLEANLY and reconciling to
        // `Ok(None)` (`recovered == total`, `losses` affirmatively empty,
        // e.g. every partition holding only a shadowed range tombstone).
        // The second case's manifest previously printed `REFUSED:
        // nothing-decodable` directly next to `losses: 0 RECOGNISED` — a
        // self-contradictory pairing pointing the operator at a loss list
        // that has nothing in it. State which case this run is.
        let remedy = if report.losses.is_empty() {
            "every partition decoded successfully but reconciled to nothing to write (e.g. a \
             shadowed range tombstone with no live data); there is nothing to write, not a \
             decode failure"
                .to_string()
        } else {
            "no partition could be recovered; inspect the losses above".to_string()
        };
        report.refused = Some(Refusal {
            reason: RefusalReason::NothingDecodable,
            remedy,
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
    // roborev, issue #4196, round-7 Low finding: `step()` was previously
    // called exactly once and the merger dropped — every row here came from
    // ONE `decode_partition_at_offset_for_salvage` call for ONE boundary
    // slot, so this MUST drain to `Complete` on the next step. A second
    // `Partition` would mean the decoder fabricated rows spanning a
    // partition boundary (e.g. a corrupted `END_OF_PARTITION` marker whose
    // over-consumption still satisfied the compressed branch's `consumed <=
    // end - offset` bound), and silently dropping the merger here would
    // discard that second partition's rows from BOTH the output and the
    // loss manifest — the exact "every partition accounted for" contract
    // this tool exists to uphold.
    match merger.step() {
        Ok(MergeStep::Complete) => {}
        Ok(MergeStep::Partition { .. }) => {
            return Err((
                LossClass::Decode,
                entries.len(),
                "boundary slot decoded rows spanning more than one partition key".to_string(),
            ));
        }
        Err(e) => return Err((LossClass::Decode, entries.len(), e.to_string())),
    }
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

/// `true` when `candidate` does not token-sort strictly after
/// `last_written` — mirrors `SSTableWriter::write_partition`'s own ordering
/// check (`writer/mod.rs`: `key.token <= last_token`) so the SAME violation
/// is caught HERE, before ever calling it, and classified as an ordinary
/// `Loss` instead of surfacing as a hard `Err` deep in the write path
/// (roborev, issue #4196, round-9 — three prior rounds independently
/// surfaced the hard-`Err` propagation this prevents). Extracted as a pure
/// function so the comparison is unit-testable directly: end-to-end, this
/// code path is reachable ONLY via a corrupted BTI narrow leaf (no
/// independent key to cross-check — every OTHER corruption class that could
/// produce an out-of-order token is already caught EARLIER, either by
/// `check_strictly_ascending` (offset monotonicity) or by
/// `decode_partition_at_offset_for_salvage`'s own `expected_key` cross-check
/// — see this module's `token_out_of_order` unit tests for the reasoning),
/// which is hard to construct as a real end-to-end fixture; declared here
/// rather than silently left untested.
fn token_out_of_order(last_written: Option<i64>, candidate: i64) -> bool {
    last_written.is_some_and(|last| candidate <= last)
}

#[cfg(test)]
mod ordering_tests {
    use super::token_out_of_order;

    /// The common, healthy case: the first partition ever written has
    /// nothing to compare against.
    #[test]
    fn first_partition_is_never_out_of_order() {
        assert!(!token_out_of_order(None, i64::MIN));
        assert!(!token_out_of_order(None, 0));
        assert!(!token_out_of_order(None, i64::MAX));
    }

    /// A strictly-increasing token sequence — the normal case for every
    /// partition after the first — never flags.
    #[test]
    fn strictly_increasing_tokens_pass() {
        assert!(!token_out_of_order(Some(-100), -50));
        assert!(!token_out_of_order(Some(0), 1));
        assert!(!token_out_of_order(Some(i64::MIN), i64::MAX));
    }

    /// A DUPLICATE token — `SSTableWriter::write_partition` rejects `<=`,
    /// not just `<`, so a repeat must flag too (two boundary entries naming
    /// the same effective token, e.g. a Murmur3 hash collision on two
    /// distinct keys — Cassandra's own token-order writer would never
    /// legitimately produce this for the SAME table without an intervening
    /// key, so seeing it here IS the corruption signal).
    #[test]
    fn duplicate_token_is_out_of_order() {
        assert!(token_out_of_order(Some(42), 42));
    }

    /// A DECREASING token — the exact scenario this fix exists for: an
    /// offset-ascending, individually-key-matching boundary source (so
    /// NEITHER `check_strictly_ascending` NOR the per-entry key cross-check
    /// catches it) whose corrupted narrow leaf nonetheless decodes a token
    /// that sorts BEFORE what was already written.
    #[test]
    fn decreasing_token_is_out_of_order() {
        assert!(token_out_of_order(Some(1000), 999));
        assert!(token_out_of_order(Some(0), i64::MIN));
    }
}
