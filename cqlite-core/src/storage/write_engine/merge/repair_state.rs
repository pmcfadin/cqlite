//! Repair-state classification + mixed-state rejection for compaction
//! (issue #1021, parent epic #973 compaction byte parity).
//!
//! Apache Cassandra refuses to compact SSTables that disagree on their repair
//! state — `CompactionTask`/`CompactionStrategyManager` partition candidates by
//! `(repairedAt, pendingRepair, isTransient)` so a single compaction never mixes
//! repaired, unrepaired, and pending-repair data (this is what
//! `CompactionTaskTest`'s reject-mixed-repair-state expectations assert). CQLite
//! cannot reproduce Cassandra's repair-boundary tombstone constraints, so it
//! MUST NOT silently merge across that boundary.
//!
//! This module reads the persisted repair state of each compaction input from
//! its `Statistics.db` STATS component (via
//! [`parse_repair_metadata`](crate::parser::repair_metadata::parse_repair_metadata),
//! authoritative metadata only — no heuristics, #28), classifies the set, and
//! either:
//!   * returns the single common [`RepairState`] to PRESERVE into the merged
//!     output's `Statistics.db` (compatible inputs — same repair state), or
//!   * returns a typed [`Error::Compaction`] naming the conflicting states
//!     (mixed inputs) so the caller fails closed instead of merging.
//!
//! Scope: parse + preserve + classify + reject ONLY. This establishes nothing
//! about repair-aware tombstone purging (a separate correctness concern); the
//! merged output simply carries the inputs' shared repair state forward.

use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::parser::repair_metadata::{parse_repair_metadata, RepairField};
use crate::storage::sstable::version_gate::VersionGates;

/// The persisted repair state of an SSTable, decoded from its `Statistics.db`
/// STATS component. Two SSTables are compaction-compatible iff their
/// `RepairState`s are equal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepairState {
    /// `repairedAt` (`0` = unrepaired).
    pub repaired_at: i64,
    /// `pendingRepair` session UUID (`None` = no pending repair).
    pub pending_repair: Option<[u8; 16]>,
    /// `isTransient` flag.
    pub is_transient: bool,
}

impl RepairState {
    /// The unrepaired / no-pending / non-transient state — the state of every
    /// SSTable produced by a fresh memtable flush.
    pub fn unrepaired() -> Self {
        RepairState {
            repaired_at: 0,
            pending_repair: None,
            is_transient: false,
        }
    }

    /// Human-readable classification (`unrepaired` / `repaired` / `pending-repair`).
    /// Test-only: the rejection message is built from the per-field decoded
    /// states in [`classify_inputs`] (issue #1021), not from a whole-`RepairState`
    /// render, so this is retained only to assert the classification kinds.
    #[cfg(test)]
    fn describe(&self) -> String {
        let kind = if self.pending_repair.is_some() {
            "pending-repair"
        } else if self.repaired_at != 0 {
            "repaired"
        } else {
            "unrepaired"
        };
        format!(
            "{kind}(repairedAt={}, pendingRepair={}, isTransient={})",
            self.repaired_at,
            match self.pending_repair {
                Some(u) => uuid_hex(&u),
                None => "--".to_string(),
            },
            self.is_transient,
        )
    }
}

fn uuid_hex(u: &[u8; 16]) -> String {
    let mut s = String::with_capacity(32);
    for b in u {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Derive the sibling `Statistics.db` path for an SSTable `Data.db` path.
fn stats_path_for(data_path: &Path) -> PathBuf {
    let filename = data_path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    let stats_filename = filename.replace("Data.db", "Statistics.db");
    data_path.parent().unwrap_or(data_path).join(stats_filename)
}

/// The AUTHORITATIVELY decoded repair fields of one compaction input, preserving
/// the `Decoded` vs `Unparsed` distinction from [`parse_repair_metadata`] so the
/// classifier can reason about what was actually proven from bytes rather than
/// treating "couldn't parse" as a concrete value.
///
/// `repaired_at` is always decoded (the STATS walk reads it before any
/// version-gated / comparator-encoded field). `pending_repair` / `is_transient`
/// are decoded authoritatively for the supported formats — the walk now skips
/// PAST the `improvedMinMax` covered-clustering `Slice` using each clustering
/// column's resolved `valueLengthIfFixed()` — and can only be
/// [`RepairField::Unparsed`] when a clustering comparator is not modeled (an
/// exotic / future type), which [`classify_inputs`] treats as fail-closed.
struct InputRepairFields {
    repaired_at: i64,
    pending_repair: RepairField<Option<[u8; 16]>>,
    is_transient: RepairField<bool>,
}

/// Read the authoritatively decoded repair fields of a single SSTable from its
/// `Statistics.db`. Requires the authoritative version gates so the full
/// version-gated walk decodes `pendingRepair` / `isTransient` whenever the format
/// AND its clustering layout permit (not just `repairedAt`).
///
/// The `Decoded` vs `Unparsed` distinction is preserved and resolved by
/// [`classify_inputs`]: a field that decodes authoritatively (the common case,
/// including clustered `oa`/`da`) is compared; a genuinely-`Unparsed` field (an
/// unmodeled clustering comparator) fails the classification closed there rather
/// than being defaulted.
///
/// # Errors
///
/// * the `Statistics.db` sibling cannot be read, or
/// * its descriptor / STATS component is malformed (parse fails closed).
fn read_repair_fields(data_path: &Path) -> Result<InputRepairFields> {
    let stats_path = stats_path_for(data_path);
    let bytes = std::fs::read(&stats_path).map_err(|e| {
        Error::Compaction(format!(
            "cannot read {stats_path:?} to classify repair state for compaction: {e}"
        ))
    })?;
    let gates = VersionGates::from_path(&stats_path).map_err(|e| {
        Error::Compaction(format!(
            "cannot derive version gates from {stats_path:?} for repair-state classification: {e:?}"
        ))
    })?;
    let md = parse_repair_metadata(&bytes, Some(&gates)).map_err(|e| {
        Error::Compaction(format!(
            "cannot decode repair metadata from {stats_path:?} for compaction: {e:?}"
        ))
    })?;

    Ok(InputRepairFields {
        repaired_at: md.repaired_at,
        pending_repair: md.pending_repair,
        is_transient: md.is_transient,
    })
}

/// Classify a set of compaction inputs by repair state.
///
/// Reads each input's authoritatively decoded repair fields and either returns
/// the single shared [`RepairState`] to preserve into the merged output, or a
/// typed [`Error::Compaction`] naming the conflicting states.
///
/// # How the mixed-state gate decides (issue #1021)
///
/// Every repair field is now decoded AUTHORITATIVELY from real bytes for the
/// supported formats: the version-gated STATS walk skips PAST the `improvedMinMax`
/// covered-clustering `Slice` by resolving each clustering column's
/// `valueLengthIfFixed()` from the persisted `clusteringTypes`, so clustered
/// `oa`/`da` SSTables no longer report `pendingRepair`/`isTransient` as
/// `Unparsed`. The gate compares those decoded values:
///
///   * `repairedAt` is ALWAYS decoded; a genuine mismatch is rejected.
///   * `pendingRepair` / `isTransient`: a decoded mismatch between any two inputs
///     (different UUIDs, `Some` vs decoded `None`, or `true` vs `false`) is
///     rejected — Cassandra never mixes inputs across a repair boundary.
///
/// ## Genuinely-Unparsed inputs fail closed (HIGH-safe, never default an unknown)
///
/// A field can still be [`RepairField::Unparsed`] only for a clustering column
/// whose `AbstractType` this decoder does not model (an exotic / future type whose
/// fixed-vs-variable length is unknown, so the covered-Slice cannot be skipped).
/// In that case the field's REAL value is unknown: it could be a pending-repair
/// session id or a transient flag. We therefore CANNOT prove the inputs share a
/// repair state, and — critically — we must NOT persist a fabricated `None` /
/// `false` default derived from an `Unparsed` field (that would silently emit a
/// real pending-repair / transient SSTable as unrepaired and merge it across a
/// real boundary, violating AC2/AC3). So any `Unparsed` `pendingRepair` /
/// `isTransient` on ANY input REJECTS the compaction (fail closed). This never
/// fires for the supported corpus, where every field decodes authoritatively.
///
/// The preserved output state carries the decoded `repairedAt` forward; for
/// `pendingRepair` / `isTransient`, the single agreed decoded value (all decoded
/// values agree by the checks above) is written — never a default synthesized
/// from an `Unparsed` field.
///
/// An empty input set returns the unrepaired state (a degenerate compaction
/// produces an unrepaired output).
///
/// # Errors
///
/// * any input's repair fields cannot be read/decoded from bytes (fails closed —
///   a corrupt/truncated `Statistics.db`), or
/// * any input's `pendingRepair` / `isTransient` is `Unparsed` (an unmodeled
///   clustering type whose real repair value cannot be proven — fail closed rather
///   than default an unknown), or
/// * the inputs disagree on an AUTHORITATIVELY decoded field (mixed `repairedAt`,
///   or a decoded `pendingRepair`/`isTransient` that another input's decoded state
///   contradicts) — Cassandra refuses such a compaction, so CQLite rejects it
///   rather than producing an output that silently merges across the repair
///   boundary.
pub fn classify_inputs(input_paths: &[PathBuf]) -> Result<RepairState> {
    // Authoritative repairedAt: always decoded, always compared.
    let mut common_repaired_at: Option<i64> = None;
    // The single agreed decoded pendingRepair / isTransient (all inputs agree by
    // the checks below). A genuinely-Unparsed field fails closed before reaching
    // these, so they are only ever populated from authoritative bytes.
    let mut decoded_pending: Option<Option<[u8; 16]>> = None;
    let mut decoded_transient: Option<bool> = None;

    for path in input_paths {
        let fields = read_repair_fields(path)?;

        // repairedAt: authoritative for every format → reject a genuine mismatch.
        match common_repaired_at {
            None => common_repaired_at = Some(fields.repaired_at),
            Some(prev) if prev == fields.repaired_at => {}
            Some(prev) => {
                return Err(repair_boundary_error(
                    path,
                    &format!("decoded repairedAt {}", fields.repaired_at),
                    &format!("a prior input's decoded repairedAt {prev}"),
                ));
            }
        }

        // pendingRepair: must be authoritatively decoded. An Unparsed field (an
        // unmodeled clustering type) has an UNKNOWN real value — we cannot prove
        // compatibility and must never default it, so fail closed (HIGH-safe,
        // AC2/AC3). A decoded mismatch is a real repair-boundary rejection.
        match fields.pending_repair {
            RepairField::Decoded(v) => match decoded_pending {
                None => decoded_pending = Some(v),
                Some(prev) if prev == v => {}
                Some(prev) => {
                    return Err(repair_boundary_error(
                        path,
                        &format!("decoded pendingRepair {}", describe_pending(&v)),
                        &format!(
                            "a prior input's decoded pendingRepair {}",
                            describe_pending(&prev)
                        ),
                    ));
                }
            },
            RepairField::Unparsed => return Err(unparsed_field_error(path, "pendingRepair")),
        }

        // isTransient: same authoritative-or-fail-closed rule.
        match fields.is_transient {
            RepairField::Decoded(v) => match decoded_transient {
                None => decoded_transient = Some(v),
                Some(prev) if prev == v => {}
                Some(prev) => {
                    return Err(repair_boundary_error(
                        path,
                        &format!("decoded isTransient {v}"),
                        &format!("a prior input's decoded isTransient {prev}"),
                    ));
                }
            },
            RepairField::Unparsed => return Err(unparsed_field_error(path, "isTransient")),
        }
    }

    Ok(RepairState {
        repaired_at: common_repaired_at.unwrap_or(0),
        // Every value here was decoded from real bytes (all decoded values agree);
        // a defaulted-from-Unparsed value can never reach this point.
        pending_repair: decoded_pending.flatten(),
        is_transient: decoded_transient.unwrap_or(false),
    })
}

/// Build the fail-closed error for an input whose `pendingRepair` / `isTransient`
/// could not be authoritatively decoded (an unmodeled clustering type). We refuse
/// to default an unknown repair field, so the compaction is rejected.
fn unparsed_field_error(path: &Path, field: &str) -> Error {
    Error::Compaction(format!(
        "refusing to compact: input {path:?} has a {field} that could not be decoded from its \
         Statistics.db (its clustering comparator is not modeled, so the covered-clustering \
         Slice could not be skipped to reach this field). Its real repair value is unknown; \
         CQLite will not default an unknown repair field to unrepaired (that could silently \
         merge a pending-repair / transient SSTable across a real repair boundary), so this \
         compaction is rejected (issue #1021)"
    ))
}

/// Render a `pendingRepair` value for an error/diagnostic message.
fn describe_pending(v: &Option<[u8; 16]>) -> String {
    match v {
        Some(u) => uuid_hex(u),
        None => "--".to_string(),
    }
}

/// Build the typed repair-boundary rejection error naming the conflicting,
/// AUTHORITATIVELY-decoded states.
fn repair_boundary_error(path: &Path, this: &str, prior: &str) -> Error {
    Error::Compaction(format!(
        "refusing to compact across a repair boundary: input {path:?} has {this} but \
         {prior} — Cassandra partitions compaction candidates by repair state and never \
         mixes them; CQLite cannot safely reproduce the repair-boundary tombstone \
         constraints, so this compaction is rejected (issue #1021)"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unrepaired_default_is_zero_none_false() {
        let s = RepairState::unrepaired();
        assert_eq!(s.repaired_at, 0);
        assert_eq!(s.pending_repair, None);
        assert!(!s.is_transient);
    }

    #[test]
    fn equal_states_are_compatible() {
        let a = RepairState {
            repaired_at: 123,
            pending_repair: None,
            is_transient: false,
        };
        let b = a;
        assert_eq!(a, b);
    }

    #[test]
    fn differing_repaired_at_is_incompatible() {
        let a = RepairState::unrepaired();
        let b = RepairState {
            repaired_at: 999,
            pending_repair: None,
            is_transient: false,
        };
        assert_ne!(a, b);
    }

    #[test]
    fn describe_classifies_kind() {
        assert!(RepairState::unrepaired()
            .describe()
            .starts_with("unrepaired"));
        assert!(RepairState {
            repaired_at: 5,
            pending_repair: None,
            is_transient: false,
        }
        .describe()
        .starts_with("repaired"));
        assert!(RepairState {
            repaired_at: 0,
            pending_repair: Some([0xab; 16]),
            is_transient: false,
        }
        .describe()
        .starts_with("pending-repair"));
    }

    #[test]
    fn empty_inputs_classify_unrepaired() {
        let state = classify_inputs(&[]).expect("empty inputs");
        assert_eq!(state, RepairState::unrepaired());
    }
}
