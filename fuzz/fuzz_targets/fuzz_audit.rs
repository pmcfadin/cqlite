#![no_main]
//! Fuzz target: `cqlite verify --mode audit` (issue #4195) over a generation directory whose ONE
//! component is replaced by the fuzz bytes (issue #4502).
//!
//! Property (criterion 3): never panic / hang / exceed the memory budget (libFuzzer's flags), AND
//! never report success for a component that could not be read (the #4159 class): this target
//! ASSERTS on the audit's own result —
//! * every `Skip` names a non-empty cause, every `Fail` has a finding behind it;
//! * a vacuous audit (nothing measured) is never `is_ok()`;
//! * a component replaced by UNREADABLE bytes ([`support::is_unreadable`]) never lets a row that
//!   depends on it read `Pass`.
//!
//! Without `CQLITE_DATASETS_ROOT` (no base generation) the target is a no-op, like `fuzz_block_emit`.

use libfuzzer_sys::fuzz_target;

#[path = "support/mod.rs"]
mod support;

use cqlite_core::storage::sstable::verify::verify_sstable_generation_audit;
use cqlite_core::storage::sstable::verify_audit::AuditVerdict;
use cqlite_core::{platform::Platform, Config};

/// Audit rows whose verdict depends on a component, per invariant id. A row listed for a
/// component MUST NOT `Pass` when that component is unreadable.
fn dependent_rows(component: &str) -> &'static [&'static str] {
    match component {
        "Data.db" => &["index-data-identity", "digest-matches-data", "compression-coverage"],
        "Index.db" => &["index-data-identity", "summary-index-correlation", "bloom-no-false-negative"],
        "Summary.db" => &["summary-index-correlation"],
        "Filter.db" => &["bloom-no-false-negative"],
        "CompressionInfo.db" => &["compression-coverage"],
        "Digest.crc32" => &["digest-matches-data"],
        "TOC.txt" => &["toc-exact-listing"],
        "Partitions.db" | "Rows.db" => &["bti-trie-bounds"],
        // Statistics.db feeds only the `--deep` row, and a missing one is a documented Skip.
        _ => &[],
    }
}

fuzz_target!(|data: &[u8]| {
    let Some(input) = support::decode(data) else { return };
    // Base generation: BIG compressed (`test_comp/lz4_table`) or BTI (`test_da/wide_table`).
    let (ks, table) = if input.high { ("test_da", "wide_table") } else { ("test_comp", "lz4_table") };
    let Some(src) = support::table_dir(ks, table) else { return };
    let Some(tmp) = support::copy_generation(&src) else { return };
    if !support::apply(tmp.path(), input.component, input.bytes) {
        return;
    }
    let Some(data_db) = support::data_db(tmp.path()) else { return };

    let deep = input.flag;
    let report = support::runtime().block_on(async {
        let config = Config::default();
        let platform = std::sync::Arc::new(Platform::new(&config).await.ok()?);
        verify_sstable_generation_audit(&data_db, deep, &config, platform).await.ok()
    });
    // `None`: the platform could not start, or the audit returned `Err` — a named refusal, the
    // correct outcome for input that cannot be read.
    let Some(report) = report else { return };
    let Some(audit) = report.audit.as_ref() else {
        panic!("an audit-mode report must carry an audit summary");
    };

    for row in &audit.rows {
        match &row.verdict {
            AuditVerdict::Skip(cause) => {
                assert!(!cause.trim().is_empty(), "{}: Skip without a named cause", row.invariant)
            }
            AuditVerdict::Fail => assert!(
                !report.findings.is_empty(),
                "{}: Fail without any finding behind it",
                row.invariant
            ),
            AuditVerdict::Pass => {}
        }
    }
    if audit.vacuous() {
        assert!(!report.is_ok(), "a vacuous audit (0 invariants measured) must not read as OK");
    }
    if support::is_unreadable(input.component, input.bytes) {
        for id in dependent_rows(input.component) {
            if let Some(v) = audit.verdict(id) {
                assert!(
                    *v != AuditVerdict::Pass,
                    "{id} reports Pass although {} was replaced by {} unreadable byte(s)",
                    input.component,
                    input.bytes.len()
                );
            }
        }
    }
});
