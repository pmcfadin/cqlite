//! `cqlite extract` command (issue #4199, epic #4192) — pull one partition /
//! token-range / key-set out of a table into a fresh output directory. See
//! `openspec/changes/sstable-extract-split/{proposal,design}.md`.
//!
//! Wired like `salvage.rs`: operates directly on files, needs no
//! `Database`/ingestion, and is dispatched by `main.rs` BEFORE database
//! initialization.

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::path::Path;

use cqlite_core::storage::write_engine::extract_split::{
    extract_partitions, parse_key_literal, ExtractOptions, ExtractReport, Selection,
};

use crate::cli_types::{ExtractArgs, SalvageOutFormatArg};
use crate::commands::write_guard::{WriteGuard, OUT_REMEDY};

const MANIFEST_REMEDY: &str = "point --manifest at a path of its own, outside both the input and \
                                the extracted output, e.g. <--out>/extract.json";

/// Execute `cqlite extract`. Owns its whole exit-code space (design D5 /
/// R-CLI-1) via direct [`std::process::exit`] calls, mirroring
/// `execute_salvage_command`'s established pattern — never returns.
pub async fn execute_extract_command(schema_path: Option<&Path>, args: &ExtractArgs) {
    let Some(schema_path) = schema_path else {
        eprintln!("cqlite extract: --schema is required (the global --schema flag)");
        std::process::exit(1);
    };

    let selection_flags = [
        args.partition.is_some(),
        args.token_range.is_some(),
        args.keys_file.is_some(),
    ]
    .iter()
    .filter(|b| **b)
    .count();
    if selection_flags != 1 {
        eprintln!(
            "cqlite extract: exactly one of --partition, --token-range, --keys-file is required \
             (got {selection_flags})"
        );
        std::process::exit(1);
    }

    let out_guard = match WriteGuard::new(&args.table_dir, None) {
        Ok(guard) => guard,
        Err(why) => {
            eprintln!("cqlite extract: {why}");
            std::process::exit(1);
        }
    };
    if let Err(collision) = out_guard.assert_disjoint("--out", &args.out, OUT_REMEDY) {
        eprintln!("cqlite extract: {collision}");
        std::process::exit(1);
    }
    match std::fs::read_dir(&args.out) {
        Ok(mut rd) => {
            if rd.next().is_some() {
                eprintln!("cqlite extract: --out {} is not empty", args.out.display());
                std::process::exit(1);
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            eprintln!(
                "cqlite extract: --out {} could not be read: {e}",
                args.out.display()
            );
            std::process::exit(1);
        }
    }

    let target_table = match &args.table {
        Some(t) => t.clone(),
        None => match super::table_name_from_dir(&args.table_dir) {
            Some(t) => t,
            None => {
                eprintln!(
                    "cqlite extract: could not derive a table name from {} — name it explicitly \
                     with --table",
                    args.table_dir.display()
                );
                std::process::exit(1);
            }
        },
    };
    let schema = match crate::commands::write::load_compaction_table_schema_for_table(
        schema_path,
        Some(&target_table),
    ) {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "cqlite extract: failed to resolve a schema for table '{target_table}' from \
                 {}: {e:#}",
                schema_path.display()
            );
            std::process::exit(1);
        }
    };

    // Re-derive the guard now that the run's OWN planned output directory
    // (`<--out>/<keyspace>/<table>/`, per `SSTableWriter`'s nesting
    // convention) is known, so `--manifest` is refused when it would land
    // inside it too (design D7, mirroring salvage's round-23 finding F1).
    let output_dir = args.out.join(&schema.keyspace).join(&schema.table);
    let write_guard = match WriteGuard::new(&args.table_dir, Some(&output_dir)) {
        Ok(guard) => guard,
        Err(why) => {
            eprintln!("cqlite extract: {why}");
            std::process::exit(1);
        }
    };
    if let Some(manifest) = &args.manifest {
        if let Err(collision) =
            write_guard.assert_disjoint("--manifest", manifest, MANIFEST_REMEDY)
        {
            eprintln!("cqlite extract: {collision}");
            std::process::exit(1);
        }
    }

    let selection = if let Some(literal) = &args.partition {
        match parse_key_literal(literal, &schema).await {
            Ok(k) => Selection::Key(k),
            Err(e) => {
                eprintln!("cqlite extract: --partition '{literal}': {e}");
                std::process::exit(1);
            }
        }
    } else if let Some(range) = &args.token_range {
        match parse_token_range(range) {
            Ok((a, b)) => Selection::TokenRange(a, b),
            Err(e) => {
                eprintln!("cqlite extract: --token-range '{range}': {e}");
                std::process::exit(1);
            }
        }
    } else {
        let path = args
            .keys_file
            .as_ref()
            .expect("exactly one selection flag was validated above");
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("cqlite extract: --keys-file {}: {e}", path.display());
                std::process::exit(1);
            }
        };
        let mut keys = Vec::new();
        for (lineno, line) in content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match parse_key_literal(line, &schema).await {
                Ok(k) => keys.push(k),
                Err(e) => {
                    eprintln!(
                        "cqlite extract: --keys-file {} line {}: {e}",
                        path.display(),
                        lineno + 1
                    );
                    std::process::exit(1);
                }
            }
        }
        if keys.is_empty() {
            eprintln!(
                "cqlite extract: --keys-file {} names no keys",
                path.display()
            );
            std::process::exit(1);
        }
        Selection::KeySet(keys)
    };

    let options = ExtractOptions {
        out_dir: args.out.clone(),
        raw: args.raw,
    };
    let report = match extract_partitions(&args.table_dir, selection, &schema, options).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("cqlite extract: {e:#}");
            std::process::exit(1);
        }
    };

    render(&report, args.out_format);
    if let Some(manifest_path) = &args.manifest {
        if let Err(e) = write_manifest(&report, manifest_path) {
            eprintln!("cqlite extract: failed to write --manifest {}: {e}", manifest_path.display());
            std::process::exit(1);
        }
    }

    if report.refused.is_some() {
        std::process::exit(2);
    }
    if !report.not_found.is_empty() {
        std::process::exit(3);
    }
    std::process::exit(0);
}

fn parse_token_range(spec: &str) -> Result<(i64, i64), String> {
    let (a, b) = spec
        .split_once(',')
        .ok_or_else(|| format!("expected `A,B`, got '{spec}'"))?;
    let a: i64 = a
        .trim()
        .parse()
        .map_err(|_| format!("'{}' is not a valid i64 token", a.trim()))?;
    let b: i64 = b
        .trim()
        .parse()
        .map_err(|_| format!("'{}' is not a valid i64 token", b.trim()))?;
    if a >= b {
        return Err(format!("token range must satisfy a < b; got ({a}, {b}]"));
    }
    Ok((a, b))
}

fn render(report: &ExtractReport, format: SalvageOutFormatArg) {
    match format {
        SalvageOutFormatArg::Json => {
            match serde_json::to_string_pretty(report) {
                Ok(s) => println!("{s}"),
                Err(e) => eprintln!("cqlite extract: failed to render JSON manifest: {e}"),
            }
        }
        SalvageOutFormatArg::Text => {
            eprintln!("extract: input={} output={} mode={}", report.input, report.output, report.mode);
            eprintln!(
                "  selection: kind={} detail={}",
                report.selection.kind, report.selection.detail
            );
            for g in &report.generations_written {
                eprintln!(
                    "  generation {}: {} partitions, {} rows",
                    g.generation, g.partitions, g.rows
                );
            }
            if !report.not_found.is_empty() {
                eprintln!("  not_found ({}): {}", report.not_found.len(), report.not_found.join(", "));
            }
            if let Some(refused) = &report.refused {
                eprintln!(
                    "  REFUSED: reason={:?} detail={} remedy={}",
                    refused.reason, refused.detail, refused.remedy
                );
            }
        }
    }
}

fn write_manifest(report: &ExtractReport, path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string_pretty(report)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    std::fs::write(path, json)
}
