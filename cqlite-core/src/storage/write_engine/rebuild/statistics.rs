//! Statistics.db rebuild helpers (design D2/§2; spec R4): repair-field and
//! `EncodingStats`-baseline recovery from the original `Statistics.db` (when
//! readable), and the `bloom_filter_fp_chance` / `min_index_interval`
//! schema-provenance helpers shared with the Filter.db / Summary.db
//! regenerators.

use super::{Component, FieldProvenance, RebuildOptions, RebuildReport};
use crate::error::Result;
use crate::parser::repair_metadata::{parse_repair_metadata, RepairField};
use crate::schema::TableSchema;
use crate::storage::sstable::version_gate::VersionGates;
use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
use std::collections::BTreeMap;
use std::path::Path;

/// One partition's recovered repair-coordination fields (spec R4.2/R4.3),
/// each with its own [`FieldProvenance`] — `recovered` only when the
/// original `Statistics.db` (or an explicit recovery-source override, R4.2)
/// parses; `lost` (default-valued, never guessed) otherwise.
pub(super) struct RecoveredRepairState {
    pub(super) repaired_at: i64,
    pub(super) pending_repair: Option<[u8; 16]>,
    pub(super) is_transient: bool,
    pub(super) provenance: FieldProvenance,
}

impl RecoveredRepairState {
    fn lost() -> Self {
        Self {
            repaired_at: 0,
            pending_repair: None,
            is_transient: false,
            provenance: FieldProvenance::Lost,
        }
    }
}

/// Read `repairedAt`/`pendingRepair`/`isTransient` from `stats_path` (design
/// D2's Statistics.db row; spec R4.2/R4.3). `stats_path` is EITHER the
/// input's own expected sibling `Statistics.db`, or an explicit
/// `--statistics-recovery-source`-style override naming a renamed-aside
/// file (R4.2) — this function does not care which, it only reads bytes at
/// the path it is given.
///
/// Never guesses: a file that cannot be read, or whose repair fields are
/// genuinely `Unparsed` (an unmodeled clustering comparator — see
/// [`crate::parser::repair_metadata`]'s module doc), is [`FieldProvenance::Lost`],
/// never silently reported as `recovered` with a default value (spec
/// R4.3's core assertion).
pub(super) fn recover_repair_state(stats_path: &Path) -> RecoveredRepairState {
    let Ok(bytes) = std::fs::read(stats_path) else {
        return RecoveredRepairState::lost();
    };
    let Ok(gates) = VersionGates::from_path(stats_path) else {
        return RecoveredRepairState::lost();
    };
    let Ok(md) = parse_repair_metadata(&bytes, Some(&gates)) else {
        return RecoveredRepairState::lost();
    };
    if !md.repaired_at_decoded {
        return RecoveredRepairState::lost();
    }
    let (pending_repair, pending_ok) = match md.pending_repair {
        RepairField::Decoded(v) => (v, true),
        RepairField::Unparsed => (None, false),
    };
    let (is_transient, transient_ok) = match md.is_transient {
        RepairField::Decoded(v) => (v, true),
        RepairField::Unparsed => (false, false),
    };
    if !pending_ok || !transient_ok {
        // A partially-decoded original is still safer reported as wholly
        // `lost` than mixing a genuinely-decoded `repaired_at` with a
        // fabricated `pending_repair`/`is_transient` default (spec R4.3).
        return RecoveredRepairState::lost();
    }
    RecoveredRepairState {
        repaired_at: md.repaired_at,
        pending_repair,
        is_transient,
        provenance: FieldProvenance::Recovered,
    }
}

/// The whole-SSTable delta-encoding baseline
/// (`SerializationHeader.EncodingStats`) recovered from the ORIGINAL
/// `Statistics.db`, with its provenance (issue #4197, spec R2/R4.2).
pub(super) struct RecoveredBaseline {
    pub(super) min_timestamp: i64,
    pub(super) min_local_deletion_time: i32,
    pub(super) min_ttl: i32,
}

/// Read the authoritative `EncodingStats` baseline triple out of
/// `stats_path`'s `SerializationHeader` — the three whole-SSTable minima
/// Cassandra delta-encoded EVERY row's timestamp/TTL/local-deletion-time
/// against when it wrote the sibling (and, for rebuild, UNCHANGED) `Data.db`.
///
/// # Why the file's own header beats re-deriving it from Data.db (spec R2)
///
/// The baseline is NOT recoverable from the decoded content in general, only
/// bounded by it: it is whatever value the writer happened to hold, and
/// Cassandra carries EncodingStats minima FORWARD from compaction inputs
/// (`SerializationHeader.make(metadata, sstables)` → `EncodingStats.merge`,
/// `cassandra-5.0.8`), so a compaction-produced SSTable's true baseline can
/// be lower than anything present in its own rows. A row that carried the
/// minimum can also be shadow-dropped by read-time reconciliation before
/// rebuild's own decode ever sees it. Either way a re-derivation can only
/// come out too HIGH, which silently NARROWS every VInt delta and desyncs
/// every promoted-index byte offset rebuild computes — with no error.
///
/// Returns `None` (never a fabricated default) when the file is absent or
/// its `SerializationHeader` does not parse; the caller then falls back to
/// the decode-derivation and classifies the affected fields `lost` — NEVER
/// `recomputed`, since that derivation is circular (rebuild exists partly to
/// regenerate a MISSING `Statistics.db`, so the fallback must stay; see
/// `components`'s module doc for why circularity, not mere imprecision, is
/// the reason).
pub(super) fn recover_encoding_stats_baseline(stats_path: &Path) -> Option<RecoveredBaseline> {
    let bytes = std::fs::read(stats_path).ok()?;
    let (min_timestamp, min_local_deletion_time, min_ttl) =
        crate::parser::enhanced_statistics_parser::read_encoding_stats_baseline(&bytes)?;
    // A value outside `i32` cannot be the baseline Cassandra wrote:
    // `EncodingStats.Serializer` round-trips both of these through
    // `writeUnsignedVInt32`/`readUnsignedVInt32`, whose `checkedCast`
    // REJECTS anything that does not fit a signed 32-bit int. Refuse the
    // recovery rather than truncate into a plausible-looking baseline.
    let min_local_deletion_time = i32::try_from(min_local_deletion_time).ok()?;
    let min_ttl = i32::try_from(min_ttl).ok()?;
    Some(RecoveredBaseline {
        min_timestamp,
        min_local_deletion_time,
        min_ttl,
    })
}

/// Resolve `bloom_filter_fp_chance` from the schema's `WITH` clause,
/// mirroring `writer::finish::bloom_filter_fp_chance` (`pub(super)` there,
/// so not directly callable — this is a deliberate, small duplicate rather
/// than widening that function's visibility for one caller). Falls back to
/// Cassandra's default `0.01` when the schema does not carry the option (or
/// it fails to parse), classified `recomputed` in that case, `recovered`
/// when the schema states it explicitly (design D2; spec R3.1).
pub(super) fn bloom_filter_fp_chance(schema: &TableSchema) -> (f64, FieldProvenance) {
    const DEFAULT_FP_CHANCE: f64 = 0.01;
    match schema.comments.get("bloom_filter_fp_chance") {
        Some(raw) => match raw.trim().parse::<f64>() {
            Ok(v) if v > 0.0 && v <= 1.0 => (v, FieldProvenance::Recovered),
            _ => (DEFAULT_FP_CHANCE, FieldProvenance::Recomputed),
        },
        None => (DEFAULT_FP_CHANCE, FieldProvenance::Recomputed),
    }
}

/// Resolve `min_index_interval` for the Summary.db rebuild (design D2's
/// documented gap, §D2/proposal.md §1; spec R3.2).
///
/// `SSTableWriter::with_format_and_registry` hardcodes
/// `summary_sample_interval = 128` unconditionally — there is no existing
/// plumbing (schema-comment or otherwise) threading a non-default
/// `min_index_interval` through ANY write path today, so this ALWAYS
/// returns Cassandra's default `128`, classified `recomputed`. This is a
/// DECLARED gap (tasks.md task 1.4), not a silent wrong-byte guess: when the
/// table's real `min_index_interval` was ever set to something other than
/// 128, the rebuilt Summary.db will NOT be byte-identical to the original,
/// and the manifest says so via this classification rather than claiming
/// byte parity it cannot back up.
pub(super) fn min_index_interval() -> (u32, FieldProvenance) {
    (128, FieldProvenance::Recomputed)
}

/// Write the regenerated `Statistics.db` and record its per-field
/// provenance (design D2/D5; spec R4).
///
/// `stats_path` is the ALREADY-RESOLVED recovery source
/// (`components::statistics_source_path`) — the same file pass 0 read the
/// `EncodingStats` baseline from, so the repair fields and the baseline can
/// never come from two different `Statistics.db`s.
///
/// `baseline_provenance` is that pass-0 outcome: `Recovered` when the
/// original header supplied the delta-encoding baseline, `Lost` when it was
/// unreadable and the circular decode-derivation stood in (see
/// `components`'s module doc).
// Eight parameters, each an independent fact about ONE run (paths, schema,
// format, provenance, the accumulator, the report). Bundling them into a
// context struct would add a type whose only purpose is to be destructured
// immediately at the single call site, so the lint is silenced rather than
// satisfied.
#[allow(clippy::too_many_arguments)]
pub(super) fn write_statistics_component(
    stats_path: &Path,
    base: &str,
    options: &RebuildOptions,
    schema: &TableSchema,
    is_bti: bool,
    baseline_provenance: FieldProvenance,
    stats_acc: &mut StatisticsMetadata,
    report: &mut RebuildReport,
) -> Result<()> {
    let repair = recover_repair_state(stats_path);
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
    // The six timestamp/TTL/local-deletion-time aggregates are exactly as
    // trustworthy as the delta-encoding baseline they are measured against
    // (issue #4197, spec R4.1) — but they are NOT that baseline:
    //
    //   * baseline RECOVERED — all six are a genuine fold over
    //     correctly-decoded content (`recomputed`). The three MINIMA were
    //     labelled `recovered` here until issue #4197's roborev job 124,
    //     which was wrong in BOTH directions: `StatsMetadata`'s minima are
    //     Cassandra's `MetadataCollector` fold over the cells and tombstones
    //     actually WRITTEN, while the recovered value is
    //     `SerializationHeader.EncodingStats` — merged forward from
    //     compaction INPUTS (`EncodingStats.merge`, `cassandra-5.0.8`) and so
    //     legitimately LOWER than any row in the file. Labelling the fold
    //     `recovered` claimed a verbatim copy that is true of the header and
    //     false of STATS; the code behind it also wrote the baseline INTO the
    //     STATS minima. `encoding_stats_baseline` below is where the recovered
    //     provenance is now reported, because the header is what actually
    //     carries that value.
    //   * baseline LOST — Data.db stores these fields as UNSIGNED DELTAS
    //     against the very value that is missing, so decoding them requires
    //     GUESSING the baseline first — the fold still runs and produces
    //     real, non-sentinel numbers, but they are absolute values
    //     reconstructed from a circular guess, not the file's true ones. All
    //     six are `lost` and NAMED so — plausible numbers, not sentinels, is
    //     exactly why they cannot be dressed up as `recomputed` (which this
    //     crate's own `FieldProvenance` defines as "derived from Data.db alone
    //     … never a guess").
    let aggregate_provenance = match baseline_provenance {
        FieldProvenance::Recovered => FieldProvenance::Recomputed,
        other => other,
    };
    for field in [
        "min_timestamp",
        "min_local_deletion_time",
        "min_ttl",
        "max_timestamp",
        "max_local_deletion_time",
        "max_ttl",
    ] {
        fields.insert(
            field.to_string(),
            aggregate_provenance.manifest_label().to_string(),
        );
    }
    // The regenerated SERIALIZATION_HEADER's own `EncodingStats` triple —
    // `recovered` when it came verbatim from the original header (the only
    // authoritative record of what the UNCHANGED Data.db was delta-encoded
    // against), `lost` when that file was unreadable and the circular
    // decode-derivation stood in. Reported under the same name the `index`
    // component uses for the same value.
    fields.insert(
        "encoding_stats_baseline".to_string(),
        baseline_provenance.manifest_label().to_string(),
    );
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recover_repair_state_is_lost_when_file_absent() {
        let missing = Path::new("/nonexistent/nb-1-big-Statistics.db");
        let state = recover_repair_state(missing);
        assert_eq!(state.provenance, FieldProvenance::Lost);
        assert_eq!(state.repaired_at, 0);
        assert_eq!(state.pending_repair, None);
        assert!(!state.is_transient);
    }

    #[test]
    fn bloom_filter_fp_chance_recomputed_when_schema_silent() {
        let schema = TableSchema::new_for_testing("ks", "t");
        let (fp, provenance) = bloom_filter_fp_chance(&schema);
        assert_eq!(fp, 0.01);
        assert_eq!(provenance, FieldProvenance::Recomputed);
    }

    #[test]
    fn bloom_filter_fp_chance_recovered_when_schema_states_it() {
        let mut schema = TableSchema::new_for_testing("ks", "t");
        schema
            .comments
            .insert("bloom_filter_fp_chance".to_string(), "0.05".to_string());
        let (fp, provenance) = bloom_filter_fp_chance(&schema);
        assert_eq!(fp, 0.05);
        assert_eq!(provenance, FieldProvenance::Recovered);
    }

    #[test]
    fn min_index_interval_is_always_recomputed_today() {
        let (value, provenance) = min_index_interval();
        assert_eq!(value, 128);
        assert_eq!(provenance, FieldProvenance::Recomputed);
    }
}
