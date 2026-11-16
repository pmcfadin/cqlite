//! `cqlite verify` — SSTable verifier contract enforcement (epic #970, #1000).
//!
//! Thin CLI wrapper over [`cqlite_core::storage::sstable::verify::verify_sstable`].
//! Renders the structured [`VerifyReport`] as text or JSON (for CI artifacts)
//! and maps a failing verification to a non-zero process exit.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::verify::{
    format_location, verify_sstable, verify_sstable_audit, Location, PartitionResolution,
    PhysicalAnchor, VerifyFinding, VerifyMode, VerifyReport,
};
use cqlite_core::storage::sstable::verify_audit::{AuditSummary, AuditVerdict};
use cqlite_core::Config;

use crate::cli_types::{VerifyModeArg, VerifyOutputArg};

/// Destructure a `Commands::Verify` and run it (keeps `main.rs`'s dispatch block
/// one line regardless of how many flags `verify` grows; file-size ratchet).
pub async fn dispatch_verify(cmd: &crate::cli_types::Commands) -> Result<()> {
    let crate::cli_types::Commands::Verify {
        path,
        mode,
        out,
        deep,
    } = cmd
    else {
        return Err(anyhow::anyhow!(
            "dispatch_verify called with a non-verify command"
        ));
    };
    execute_verify_command(path, *mode, *out, *deep).await
}

/// Execute `cqlite verify <path> --mode <quick|full> --out <text|json>`.
///
/// Returns `Err` only on an environmental failure (e.g. the directory has no
/// Data.db). A *data* corruption produces an `Ok(())` print of the report
/// followed by `std::process::exit(2)` so scripts and CI can branch on the exit
/// code while still capturing the serialized findings.
pub async fn execute_verify_command(
    path: &Path,
    mode: VerifyModeArg,
    out: VerifyOutputArg,
    deep: bool,
) -> Result<()> {
    // `--deep` is only meaningful for the audit: fail closed BEFORE any file is
    // opened (usage error, exit 1), rather than silently ignoring it (#4195).
    if deep && !matches!(mode, VerifyModeArg::Audit) {
        // Exit 1 = usage error, the same space `sweep` uses (a generic `Err`
        // would be mapped to an environment-failure code by `main`).
        eprintln!(
            "error: --deep requires --mode audit (it adds the full-scan invariants of the \
             audit); it has no meaning for --mode quick or --mode full"
        );
        std::process::exit(1);
    }
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);

    let report = match mode {
        VerifyModeArg::Quick => verify_sstable(path, VerifyMode::Quick, &config, platform).await?,
        VerifyModeArg::Full => verify_sstable(path, VerifyMode::Full, &config, platform).await?,
        VerifyModeArg::Audit => verify_sstable_audit(path, deep, &config, platform).await?,
    };

    match out {
        VerifyOutputArg::Text => print_text(&report),
        VerifyOutputArg::Json => print_json(&report),
    }

    if !report.is_ok() {
        // Distinct non-zero code so callers can distinguish "verification failed"
        // from a usage/environment error (exit 1 from anyhow).
        std::process::exit(2);
    }
    Ok(())
}

fn print_text(report: &VerifyReport) {
    println!("{}", report.summary_line());
    if let Some(audit) = &report.audit {
        print_audit_text(audit);
    }
    if !report.findings.is_empty() {
        println!("findings ({}):", report.findings.len());
        for f in &report.findings {
            println!("  - [{}] {}: {}", f.class.code(), f.component, f.detail);
            if let Some(loc) = &f.location {
                // Issue #4194, roborev round-1 LOW finding: this used to be a
                // second, near-identical renderer (`format_location_text`)
                // that had already drifted from core's own `format_location`
                // (different separator, different "0 partitions" wording).
                // `format_location` is `pub` and re-exported exactly so the
                // CLI never needs its own copy.
                println!("      location: {}", format_location(loc));
            }
        }
    }
}

/// The audit checklist: always eight rows, then the MEASURED count. A skip is
/// printed with its cause and is never rendered as a pass.
fn print_audit_text(audit: &AuditSummary) {
    println!(
        "audit (deep: {}): {} invariants MEASURED of {}",
        if audit.deep { "yes" } else { "no" },
        audit.measured(),
        audit.rows.len()
    );
    for row in &audit.rows {
        match &row.verdict {
            AuditVerdict::Pass => println!("  [PASS] {}", row.invariant),
            AuditVerdict::Fail => println!("  [FAIL] {}", row.invariant),
            AuditVerdict::Skip(cause) => println!("  [SKIP] {}: {}", row.invariant, cause),
        }
    }
    if audit.vacuous() {
        println!(
            "audit: 0 invariants MEASURED -- nothing was verified; this is not a clean result"
        );
    }
}

fn audit_to_json(audit: Option<&AuditSummary>) -> String {
    let Some(audit) = audit else {
        return "null".to_string();
    };
    let rows: Vec<String> = audit
        .rows
        .iter()
        .map(|r| {
            let (verdict, cause) = match &r.verdict {
                AuditVerdict::Pass => ("pass", "null".to_string()),
                AuditVerdict::Fail => ("fail", "null".to_string()),
                AuditVerdict::Skip(c) => ("skip", json_str(c)),
            };
            format!(
                "{{\"invariant\":{},\"verdict\":{},\"cause\":{}}}",
                json_str(r.invariant),
                json_str(verdict),
                cause
            )
        })
        .collect();
    format!(
        "{{\"deep\":{},\"measured\":{},\"rows\":[{}]}}",
        audit.deep,
        audit.measured(),
        rows.join(",")
    )
}

fn print_json(report: &VerifyReport) {
    // Hand-rolled JSON keeps the verifier free of a serde dependency in the
    // core API while still emitting a stable, CI-consumable artifact.
    let findings: Vec<String> = report.findings.iter().map(finding_to_json).collect();
    let toc: Vec<String> = report.toc_components.iter().map(|c| json_str(c)).collect();
    let rows = report
        .rows_scanned
        .map(|n| n.to_string())
        .unwrap_or_else(|| "null".to_string());

    println!(
        "{{\"directory\":{},\"base_name\":{},\"format\":{},\"mode\":{},\"ok\":{},\"rows_scanned\":{},\"toc_components\":[{}],\"findings\":[{}],\"audit\":{}}}",
        json_str(&report.directory.display().to_string()),
        json_str(&report.base_name),
        json_str(report.format.as_str()),
        json_str(report.mode.as_str()),
        report.is_ok(),
        rows,
        toc.join(","),
        findings.join(","),
        audit_to_json(report.audit.as_ref()),
    );
}

/// One `VerifyFinding` as a JSON object, `location` included when present
/// (issue #4194) — shared with `sweep`'s JSON report so the two verbs never
/// disagree on a finding's shape.
pub(crate) fn finding_to_json(f: &VerifyFinding) -> String {
    let location = f
        .location
        .as_ref()
        .map(location_to_json)
        .unwrap_or_else(|| "null".to_string());
    format!(
        "{{\"class\":{},\"component\":{},\"detail\":{},\"location\":{}}}",
        json_str(f.class.code()),
        json_str(&f.component),
        json_str(&f.detail),
        location,
    )
}

/// One `Location` as a JSON object (issue #4194): `chunk_index` is JSON
/// `null` when the finding has no chunk grid; `partitions` is either
/// `{"resolved":[...]}` or `{"unresolved":"<cause>"}` — the two states are
/// never conflated into a bare array that could not tell "zero intersecting
/// partitions" from "could not resolve".
fn location_to_json(loc: &Location) -> String {
    let chunk_index = loc
        .chunk_index
        .map(|c| c.to_string())
        .unwrap_or_else(|| "null".to_string());
    let partitions = match &loc.partitions {
        PartitionResolution::Resolved { keys, truncated } => {
            let entries: Vec<String> = keys
                .iter()
                .map(|k| {
                    let rendered = k
                        .rendered
                        .as_deref()
                        .map(json_str)
                        .unwrap_or_else(|| "null".to_string());
                    format!(
                        "{{\"key_hex\":{},\"rendered\":{}}}",
                        json_str(&k.key_hex),
                        rendered
                    )
                })
                .collect();
            // `truncated` (issue #4194, roborev round-2 MEDIUM finding): how
            // many more intersecting partitions exist beyond the resolver's
            // MAX_RESOLVED_KEYS cap, `0` when nothing was omitted.
            format!(
                "{{\"resolved\":[{}],\"truncated\":{}}}",
                entries.join(","),
                truncated
            )
        }
        PartitionResolution::Unresolved(cause) => {
            format!("{{\"unresolved\":{}}}", json_str(cause))
        }
    };
    // `anchor` (roborev job 92 MEDIUM): a JSON consumer sees the same two
    // physical fields the text renderer does, so it needs the same disclosure —
    // fixing only the human channel would leave the machine channel asserting
    // "these bytes are damaged" about an offset past EOF.
    let anchor = match loc.anchor {
        PhysicalAnchor::DamagedExtent => "damaged_extent",
        PhysicalAnchor::DeclaredRecord => "declared_record",
    };
    format!(
        "{{\"component\":{},\"byte_offset\":{},\"byte_len\":{},\"anchor\":{},\"chunk_index\":{},\"partitions\":{}}}",
        json_str(&loc.component),
        loc.byte_offset,
        loc.byte_len,
        json_str(anchor),
        chunk_index,
        partitions,
    )
}

/// Minimal JSON string escaper (quotes, backslashes, control chars).
pub(crate) fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
