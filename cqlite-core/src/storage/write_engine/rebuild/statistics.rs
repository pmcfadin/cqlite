//! Statistics.db rebuild helpers (design D2/§2; spec R4): repair-field
//! recovery from the original `Statistics.db` (when readable), and the
//! `bloom_filter_fp_chance` / `min_index_interval` schema-provenance helpers
//! shared with the Filter.db / Summary.db regenerators.

use super::FieldProvenance;
use crate::parser::repair_metadata::{parse_repair_metadata, RepairField};
use crate::schema::TableSchema;
use crate::storage::sstable::version_gate::VersionGates;
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
