//! `cqlite verify` — SSTable verifier contract enforcement (epic #970, #1000).
//!
//! Thin CLI wrapper over [`cqlite_core::storage::sstable::verify::verify_sstable`].
//! Renders the structured [`VerifyReport`] as text or JSON (for CI artifacts)
//! and maps a failing verification to a non-zero process exit.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::verify::{verify_sstable, VerifyMode, VerifyReport};
use cqlite_core::Config;

use crate::cli_types::{VerifyModeArg, VerifyOutputArg};

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
) -> Result<()> {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);

    let mode = match mode {
        VerifyModeArg::Quick => VerifyMode::Quick,
        VerifyModeArg::Full => VerifyMode::Full,
    };

    let report = verify_sstable(path, mode, &config, platform).await?;

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
    if !report.is_ok() {
        println!("findings ({}):", report.findings.len());
        for f in &report.findings {
            println!("  - [{}] {}: {}", f.class.code(), f.component, f.detail);
        }
    }
}

fn print_json(report: &VerifyReport) {
    // Hand-rolled JSON keeps the verifier free of a serde dependency in the
    // core API while still emitting a stable, CI-consumable artifact.
    let findings: Vec<String> = report
        .findings
        .iter()
        .map(|f| {
            format!(
                "{{\"class\":{},\"component\":{},\"detail\":{}}}",
                json_str(f.class.code()),
                json_str(&f.component),
                json_str(&f.detail)
            )
        })
        .collect();
    let toc: Vec<String> = report.toc_components.iter().map(|c| json_str(c)).collect();
    let rows = report
        .rows_scanned
        .map(|n| n.to_string())
        .unwrap_or_else(|| "null".to_string());

    println!(
        "{{\"directory\":{},\"base_name\":{},\"format\":{},\"mode\":{},\"ok\":{},\"rows_scanned\":{},\"toc_components\":[{}],\"findings\":[{}]}}",
        json_str(&report.directory.display().to_string()),
        json_str(&report.base_name),
        json_str(report.format.as_str()),
        json_str(report.mode.as_str()),
        report.is_ok(),
        rows,
        toc.join(","),
        findings.join(","),
    );
}

/// Minimal JSON string escaper (quotes, backslashes, control chars).
fn json_str(s: &str) -> String {
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
