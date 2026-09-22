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
    verify_sstable, Location, PartitionResolution, VerifyFinding, VerifyMode, VerifyReport,
};
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
            if let Some(loc) = &f.location {
                println!("      location: {}", format_location_text(loc));
            }
        }
    }
}

/// Human-readable one-line rendering of a [`Location`] for `--out text`
/// (issue #4194): names the chunk index and every resolved partition key, or
/// the named cause when unresolved — never a silent empty line.
fn format_location_text(loc: &Location) -> String {
    let chunk = loc
        .chunk_index
        .map(|c| format!("chunk {c}, "))
        .unwrap_or_default();
    let partitions = match &loc.partitions {
        PartitionResolution::Resolved(keys) if keys.is_empty() => {
            "0 intersecting partitions".to_string()
        }
        PartitionResolution::Resolved(keys) => format!(
            "{} partition(s): {}",
            keys.len(),
            keys.iter()
                .map(|k| k.key_hex.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        PartitionResolution::Unresolved(cause) => format!("partitions unresolved ({cause})"),
    };
    format!(
        "{} {}offset 0x{:x} len {} — {}",
        loc.component, chunk, loc.byte_offset, loc.byte_len, partitions
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
        PartitionResolution::Resolved(keys) => {
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
            format!("{{\"resolved\":[{}]}}", entries.join(","))
        }
        PartitionResolution::Unresolved(cause) => {
            format!("{{\"unresolved\":{}}}", json_str(cause))
        }
    };
    format!(
        "{{\"component\":{},\"byte_offset\":{},\"byte_len\":{},\"chunk_index\":{},\"partitions\":{}}}",
        json_str(&loc.component),
        loc.byte_offset,
        loc.byte_len,
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
