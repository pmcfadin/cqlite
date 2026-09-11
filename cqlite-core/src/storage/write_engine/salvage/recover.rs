//! The recovery loop: enumerate → chunk pre-flight → decode-at-offset →
//! reconcile → write-or-record-loss (design D1, D2, D3). Helper functions
//! (opening the reader, decoding + reconciling ONE partition, building a
//! `Loss`, the pre-write ordering check) live in the sibling
//! `recover_helpers` module (round 15, campsite rule / epic #1116).

use super::boundaries::enumerate_boundaries;
use super::chunks::{chunks_for_range, compressed_chunk_preflight, uncompressed_chunk_preflight};
use super::recover_helpers::{
    build_loss, component_unreadable_refusal, open_reader, reader_data_path, recover_one_partition,
    token_out_of_order,
};
use super::{
    ComponentFinding, Loss, LossClass, PartitionTotals, Refusal, RefusalReason, SalvageOptions,
    SalvageReport,
};
use crate::error::{Error, Result};
use crate::schema::TableSchema;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::extract_sstable_base_name;
use crate::storage::sstable::version_gate::{SsTableDescriptor, SsTableFormat};
use crate::storage::sstable::writer::{SSTableFormat as WriterFormat, SSTableWriter};
use crate::storage::write_engine::merge::{classify_inputs, compute_baseline_min};
use std::path::Path;

/// The largest number of [`Loss`] entries [`salvage_sstable`] holds resident
/// per generation (roborev, issue #4196, round-15 Medium finding 4 — an Opus
/// whole-module audit): the boundary-entry count is `O(Index.db size)` (a
/// ~4-byte minimum per entry), and each `Loss.key_hex` can independently be
/// up to 131,070 hex chars (a `u16` `key_len` field, so a 65,535-byte raw
/// key) — a damaged input where MOST partitions are lost (e.g. Medium
/// finding 2's `chunk_table_bound` collapse, or any corrupt-offset index)
/// made this `Vec<Loss>` grow WITHOUT bound: a ~10 MB corrupt index names
/// ~2.5M lost partitions, several hundred MB once each is a `Loss` struct
/// AND the manifest is serialized to a second, duplicate `String` via
/// `serde_json::to_string_pretty` — well past the crate's <128 MB target,
/// with no wrong data (D5's "every loss named" contract just needs a
/// COUNTED truncation instead of literal enumeration past this cap, exactly
/// like this same module's own `component_findings`/`losses: 0 RECOGNISED`
/// affirmative-zero convention already does for the EMPTY case). 500
/// entries leaves real headroom under the crate's <128 MB target even in
/// the maximally-adversarial case (500 * 131,070 bytes ≈ 65.5 MB, versus
/// the reader/writer's own buffers, the manifest's OWN second JSON copy,
/// and everything else the process needs) while remaining generous for the
/// overwhelming majority of real damaged inputs, whose partition counts and
/// key sizes are nowhere near this adversarial extreme.
const MAX_RESIDENT_LOSSES: usize = 500;

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
        losses_truncated: 0,
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
    // roborev, issue #4196, round-15 Medium finding 4: every loss beyond
    // `MAX_RESIDENT_LOSSES` is COUNTED here rather than held resident — see
    // that constant's doc for the memory-bound reasoning. Surfaced on
    // `report.losses_truncated`.
    let mut losses_truncated: usize = 0;
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
                // roborev, issue #4196, round-15 Medium finding 3 (an Opus
                // whole-module audit): `entry.data_offset` is an unsanitized
                // VInt (`parse_big_index_entry` places no upper bound on it,
                // and `check_strictly_ascending` enforces only ORDER, so
                // `u64::MAX` is representable and legitimately the LARGEST
                // value, satisfying strict ascent trivially). An uncompressed
                // BIG input with NO `CRC.db` (a supported input —
                // `ChunkCrcUnavailable` exists for exactly this) makes
                // `uncompressed_chunk_preflight` return `data_length == 0`,
                // which skips the `data_length > entry.data_offset` branch
                // above UNCONDITIONALLY (0 is never greater than anything) —
                // reaching this arm with `entry.data_offset == u64::MAX`
                // panicked on the plain `+ 1` in every debug build (every
                // test lane), instead of producing the classified
                // `Truncated` loss this whole module exists to produce.
                // `saturating_add` never overflows; the resulting
                // `chunk_range_end == u64::MAX` still correctly names an
                // implausible, unrepresentable range downstream (`chunks_for_range`
                // is never reached here regardless — `chunk_size == 0` in
                // lockstep with `data_length == 0`, round 14's fix).
                entry.data_offset.saturating_add(1)
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
            if losses.len() < MAX_RESIDENT_LOSSES {
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
            } else {
                losses_truncated += 1;
            }
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
            if losses.len() < MAX_RESIDENT_LOSSES {
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
            } else {
                losses_truncated += 1;
            }
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
                        if losses.len() < MAX_RESIDENT_LOSSES {
                            losses.push(build_loss(
                                entry,
                                schema,
                                touched_chunks,
                                LossClass::KeyMismatch,
                                0,
                                format!(
                                    "decoded key's token {} does not sort after the last \
                                     partition actually written (token {last}); the boundary \
                                     source and the data disagree on ordering",
                                    key.token
                                ),
                            ));
                        } else {
                            losses_truncated += 1;
                        }
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
                if losses.len() < MAX_RESIDENT_LOSSES {
                    losses.push(build_loss(
                        entry,
                        schema,
                        touched_chunks,
                        class,
                        rows_before,
                        message,
                    ));
                } else {
                    losses_truncated += 1;
                }
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
    // roborev, issue #4196, round-15 Medium finding 4: `partitions.lost`
    // is the TRUE total (resident `losses.len()` PLUS every one counted-
    // but-not-retained past `MAX_RESIDENT_LOSSES`) — `report.losses` below
    // holds only the resident subset, so keying this count off `losses.len()`
    // alone would silently UNDER-report the real loss count the moment
    // truncation engages, exactly the "every loss named" contract violation
    // a counted truncation exists to avoid.
    report.partitions.lost = losses.len() + losses_truncated;
    // roborev, issue #4196, round-13 Medium finding: surface the
    // recovered-but-not-written residue (the `Ok(None)` arm above) in the
    // manifest so a partial silent-drop run — some partitions `Ok(None)`,
    // the rest written normally, `losses` still empty — is distinguishable
    // from a run that wrote every recovered partition's content, instead of
    // both reporting `recovered=N lost=0` and exiting 0.
    report.partitions.written = written;
    report.losses = losses;
    report.losses_truncated = losses_truncated;

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
