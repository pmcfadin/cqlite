//! Normalize the caller's schema against the INPUT generation's OWN
//! serialization header, before salvage decodes or writes anything (roborev,
//! issue #4196, round 23 High finding — confirmed by an independent
//! Cassandra-format expert review with a working reproduction against a real
//! Cassandra 5.0 fixture).
//!
//! # The defect this module exists to remove
//!
//! `compact_sstables` normalizes UNCONDITIONALLY before decode
//! (`merge/mod.rs`: `effective_compaction_schema` then
//! `apply_udt_marshals_from_inputs`). `salvage_sstable` did NEITHER — it
//! passed the caller's raw `--schema` straight into both `SSTableWriter` and
//! the per-partition decoder. The two normalizers cover INDEPENDENT hazards,
//! so skipping them was two live defects, not one:
//!
//!   * [`effective_compaction_schema`] re-adds a **static column present in
//!     the input's serialization header but missing from the caller's
//!     schema**. Reproduced on `test_basic/static_columns_table` (a real
//!     Cassandra 5.0-written fixture with NO UDT column anywhere, whose
//!     header declares `static_data TEXT STATIC`) against a hand-written
//!     `--schema` that simply omits `static_data` — an ordinary stale-schema
//!     operator mistake, which is close to the median reason someone reaches
//!     for a recovery tool. `compact` self-healed and said so; `salvage`
//!     printed nothing, wrote a SMALLER `Data.db`, reported
//!     `recovered=100 lost=0`, exited 0 — and its output's serialization
//!     header had no `static_data` field at all. The column was silently
//!     DROPPED, on the one-shot tool whose input may not exist afterwards.
//!   * [`apply_udt_marshals_from_inputs`] copies each UDT column's EXACT
//!     `UserType(...)` marshal out of the header so the decoder treats the
//!     column as complex (`is_complex_column`) and round-trips its per-field
//!     cells instead of misreading them as one opaque cell.
//!
//! # The UDT-registry residual (deliberate, and precisely bounded)
//!
//! [`apply_udt_marshals_from_inputs`] is called with `registry: None`. The
//! registry affects EXACTLY ONE case (see its own doc): a UDT column absent
//! from EVERY input header — i.e. added by schema evolution after the input
//! was written. Everything else derives entirely from the header bytes and is
//! therefore fully covered here. For such an absent column the input holds no
//! cells at all, so nothing can be misdecoded; the only consequence is that
//! the RECOVERED generation's own header carries that column bare rather than
//! as its `UserType(...)` marshal. Threading a registry from the CLI down
//! through [`SalvageOptions`](super::SalvageOptions) is follow-up work
//! (out of scope for this fix round, which must not touch `cqlite-cli/`).

use super::recover_helpers::component_unreadable_refusal;
use super::{ComponentFinding, Refusal};
use crate::schema::TableSchema;
use crate::storage::write_engine::merge::{
    apply_udt_marshals_from_inputs, effective_compaction_schema,
};
use std::path::PathBuf;

/// The `ComponentFinding::class` for a schema the input's header corrected.
///
/// DELIBERATELY NOT a member of the CLI's `is_verification_gap_class` set
/// (`cqlite-cli/src/commands/salvage/report.rs`): that predicate means "a
/// verification DID NOT RUN", and this is the opposite — a normalization that
/// RAN and succeeded, exactly as `compact_sstables` does for the same input
/// (which logs it at `info` and still exits 0). Folding it in would push a
/// fully-successful recovery off exit 0 for a condition salvage has already
/// handled correctly. It is recorded because the operator's `--schema` and
/// the recovered generation's header now differ from each other, which they
/// need to know; it is not an imperfection in the recovery.
const NORMALIZED_CLASS: &str = "SchemaNormalizedFromHeader";

/// How many column names a single finding's `detail` enumerates before
/// folding the rest into a count. `serialization_header_columns` comes from a
/// possibly-DAMAGED `Statistics.db`, so its length is NOT bounded by the real
/// table's width: it is bounded only by that file's size (the header parser
/// sanity-checks every name/type length against the remaining buffer, and each
/// column consumes at least two bytes), which is far more than a manifest
/// should print. Same discipline `recover.rs` applies to `losses` /
/// `Loss.chunks` — an affirmative count, never a silent drop.
const MAX_NAMED_COLUMNS: usize = 32;

/// The effective decode+write schema for a salvage run: `schema` normalized
/// against `input_paths`' own serialization header(s), plus the
/// [`ComponentFinding`]s naming whatever the normalization changed.
///
/// `input_paths` is the single generation being salvaged (salvage is
/// per-generation; the table dir is the CLI's concern, spec R7).
///
/// `Err(Refusal)` when the header cannot be normalized into ONE coherent
/// decode schema — a `RefusalReason::ComponentUnreadable`, so the caller
/// records it on the report and still emits a manifest (design D3) rather
/// than `?`-propagating a hard `Err` that would lose every loss and finding
/// gathered so far. A refusal is a legitimate salvage outcome; silently
/// decoding through a schema known to be wrong is not.
pub(super) fn effective_salvage_schema(
    schema: &TableSchema,
    input_paths: &[PathBuf],
) -> std::result::Result<(TableSchema, Vec<ComponentFinding>), Refusal> {
    let mut effective = effective_compaction_schema(schema, input_paths);
    if let Err(e) = apply_udt_marshals_from_inputs(&mut effective, input_paths, None) {
        // The shared normalizer's message says "compaction inputs" because
        // salvage normalizes through the very same function `compact_sstables`
        // uses — spelled out here so the operator is not sent looking for a
        // compaction they never ran.
        let cause = format!(
            "{e} (salvage passes exactly ONE input — this generation — so a disagreement means \
             the input's own serialization header declares a column with two incompatible \
             encodings)"
        );
        return Err(component_unreadable_refusal(
            "normalizing the schema against the input's serialization header (Statistics.db)",
            &cause,
        ));
    }
    let findings = normalization_findings(schema, &effective);
    Ok((effective, findings))
}

/// Name what normalization changed, by DIFFING the caller's schema against
/// the effective one — never by re-deriving either normalizer's own logic, so
/// this cannot drift from what actually happened.
fn normalization_findings(caller: &TableSchema, effective: &TableSchema) -> Vec<ComponentFinding> {
    let mut findings = Vec::new();

    let mut restored: Vec<&str> = effective
        .columns
        .iter()
        .filter(|c| !caller.columns.iter().any(|k| k.name == c.name))
        .map(|c| c.name.as_str())
        .collect();
    restored.sort_unstable();
    if !restored.is_empty() {
        findings.push(ComponentFinding {
            class: NORMALIZED_CLASS.to_string(),
            component: "Statistics.db".to_string(),
            detail: format!(
                "{} column(s) present in the input's serialization header but ABSENT from the \
                 supplied schema were re-added to the effective decode/write schema, so their \
                 cells are recovered instead of silently dropped: {} — the supplied schema is \
                 stale for this generation",
                restored.len(),
                render_names(&restored)
            ),
        });
    }

    let mut retyped: Vec<String> = effective
        .columns
        .iter()
        .filter_map(|c| {
            caller
                .columns
                .iter()
                .find(|k| k.name == c.name)
                .filter(|k| k.data_type != c.data_type)
                .map(|k| format!("{} ({} -> {})", c.name, k.data_type, c.data_type))
        })
        .collect();
    retyped.sort();
    if !retyped.is_empty() {
        let rendered: Vec<&str> = retyped.iter().map(String::as_str).collect();
        findings.push(ComponentFinding {
            class: NORMALIZED_CLASS.to_string(),
            component: "Statistics.db".to_string(),
            detail: format!(
                "{} column(s) took their exact type marshal from the input's serialization \
                 header rather than the supplied schema's rendering, so their cells decode with \
                 the encoding the input actually uses: {}",
                retyped.len(),
                render_names(&rendered)
            ),
        });
    }

    findings
}

/// Comma-join up to [`MAX_NAMED_COLUMNS`] names, folding any remainder into an
/// affirmative count (never a silent truncation).
fn render_names(names: &[&str]) -> String {
    let head = names
        .iter()
        .take(MAX_NAMED_COLUMNS)
        .copied()
        .collect::<Vec<_>>()
        .join(", ");
    if names.len() > MAX_NAMED_COLUMNS {
        format!(
            "{head} (+{} more, not enumerated to bound manifest size)",
            names.len() - MAX_NAMED_COLUMNS
        )
    } else {
        head
    }
}

#[cfg(test)]
mod tests {
    use super::{normalization_findings, MAX_NAMED_COLUMNS, NORMALIZED_CLASS};
    use crate::schema::{Column, TableSchema};

    fn schema(columns: &[(&str, &str)]) -> TableSchema {
        let mut s = TableSchema::new_for_testing("ks", "t");
        s.columns = columns
            .iter()
            .map(|(name, data_type)| Column {
                name: (*name).to_string(),
                data_type: (*data_type).to_string(),
                nullable: true,
                default: None,
                is_static: false,
            })
            .collect();
        s
    }

    /// The healthy case: an up-to-date schema normalizes to itself, so a clean
    /// run's manifest must not grow a finding it would have to explain.
    #[test]
    fn an_unchanged_schema_yields_no_finding() {
        let s = schema(&[("a", "text"), ("b", "int")]);
        assert!(normalization_findings(&s, &s).is_empty());
    }

    /// The reproduction's shape: a column the header declares and the caller's
    /// schema omits is named, with its own class.
    #[test]
    fn a_restored_column_is_named() {
        let caller = schema(&[("a", "text")]);
        let effective = schema(&[("a", "text"), ("static_data", "text")]);
        let findings = normalization_findings(&caller, &effective);
        assert_eq!(findings.len(), 1, "got {findings:?}");
        assert_eq!(findings[0].class, NORMALIZED_CLASS);
        assert_eq!(findings[0].component, "Statistics.db");
        assert!(
            findings[0].detail.contains("static_data"),
            "the finding must NAME the column; got {:?}",
            findings[0].detail
        );
    }

    /// A marshal adopted from the header names BOTH forms, so an operator can
    /// see what their schema said and what the input actually uses.
    #[test]
    fn a_retyped_column_names_both_forms() {
        let caller = schema(&[("u", "blob")]);
        let effective = schema(&[("u", "org.apache.cassandra.db.marshal.UserType(ks,75,...)")]);
        let findings = normalization_findings(&caller, &effective);
        assert_eq!(findings.len(), 1, "got {findings:?}");
        assert!(
            findings[0].detail.contains("blob") && findings[0].detail.contains("UserType"),
            "got {:?}",
            findings[0].detail
        );
    }

    /// Both kinds at once are reported as two separate findings — a run that
    /// hit both must not have one of them masked by the other.
    #[test]
    fn both_kinds_are_reported_separately() {
        let caller = schema(&[("u", "blob")]);
        let effective = schema(&[
            ("u", "org.apache.cassandra.db.marshal.UserType(ks,75)"),
            ("s", "text"),
        ]);
        let findings = normalization_findings(&caller, &effective);
        assert_eq!(findings.len(), 2, "got {findings:?}");
    }

    /// A damaged header can name arbitrarily many columns; the detail string
    /// stays bounded and says so affirmatively rather than truncating silently.
    #[test]
    fn a_huge_restored_set_is_counted_not_silently_truncated() {
        let names: Vec<String> = (0..MAX_NAMED_COLUMNS + 7)
            .map(|i| format!("c{i:03}"))
            .collect();
        let pairs: Vec<(&str, &str)> = names.iter().map(|n| (n.as_str(), "text")).collect();
        let findings = normalization_findings(&schema(&[]), &schema(&pairs));
        assert_eq!(findings.len(), 1, "got {findings:?}");
        assert!(
            findings[0].detail.contains("+7 more"),
            "the remainder must be COUNTED; got {:?}",
            findings[0].detail
        );
        assert!(
            findings[0]
                .detail
                .contains(&format!("{} column(s)", MAX_NAMED_COLUMNS + 7)),
            "the TRUE total must be stated; got {:?}",
            findings[0].detail
        );
    }
}
