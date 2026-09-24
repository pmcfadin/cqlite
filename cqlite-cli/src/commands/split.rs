//! `cqlite split` command (issue #4199, epic #4192) — divide one SSTable
//! generation into N (or byte-bounded) parts. See
//! `openspec/changes/sstable-extract-split/{proposal,design}.md`.
//!
//! Wired like `salvage.rs`/`extract.rs`: operates directly on files, needs
//! no `Database`/ingestion, and is dispatched by `main.rs` BEFORE database
//! initialization.

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::path::Path;

use cqlite_core::storage::write_engine::extract_split::{
    split_sstable, SplitBoundary, SplitReport,
};

use crate::cli_types::{SalvageOutFormatArg, SplitArgs};
use crate::commands::write_guard::{WriteGuard, OUT_REMEDY};

const MANIFEST_REMEDY: &str = "point --manifest at a path of its own, outside both the input and \
                                the split output, e.g. <--out>/split.json";

/// Execute `cqlite split`. Owns its whole exit-code space (design D5 /
/// R-CLI-2) via direct [`std::process::exit`] calls — never returns.
pub async fn execute_split_command(schema_path: Option<&Path>, args: &SplitArgs) {
    let Some(schema_path) = schema_path else {
        eprintln!("cqlite split: --schema is required (the global --schema flag)");
        std::process::exit(1);
    };

    let boundary = match (args.parts, args.max_bytes) {
        (Some(n), None) => SplitBoundary::Parts(n),
        (None, Some(b)) => SplitBoundary::MaxBytes(b),
        (None, None) => {
            eprintln!("cqlite split: exactly one of --parts, --max-bytes is required");
            std::process::exit(1);
        }
        (Some(_), Some(_)) => {
            eprintln!("cqlite split: --parts and --max-bytes are mutually exclusive");
            std::process::exit(1);
        }
    };

    let out_guard = match WriteGuard::new(&args.input, None) {
        Ok(guard) => guard,
        Err(why) => {
            eprintln!("cqlite split: {why}");
            std::process::exit(1);
        }
    };
    if let Err(collision) = out_guard.assert_disjoint("--out", &args.out, OUT_REMEDY) {
        eprintln!("cqlite split: {collision}");
        std::process::exit(1);
    }
    match std::fs::read_dir(&args.out) {
        Ok(mut rd) => {
            if rd.next().is_some() {
                eprintln!("cqlite split: --out {} is not empty", args.out.display());
                std::process::exit(1);
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            eprintln!(
                "cqlite split: --out {} could not be read: {e}",
                args.out.display()
            );
            std::process::exit(1);
        }
    }

    let target_table = match &args.table {
        Some(t) => t.clone(),
        None => match super::table_name_from_dir(&args.input) {
            Some(t) => t,
            None => {
                eprintln!(
                    "cqlite split: could not derive a table name from {} — name it explicitly \
                     with --table",
                    args.input.display()
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
                "cqlite split: failed to resolve a schema for table '{target_table}' from \
                 {}: {e:#}",
                schema_path.display()
            );
            std::process::exit(1);
        }
    };

    // `split` writes one `part-NNNN/<keyspace>/<table>/` nested dir per part
    // (`SSTableWriter`'s own nesting) UNDER `--out`; protect that whole root.
    let write_guard = match WriteGuard::new(&args.input, Some(&args.out)) {
        Ok(guard) => guard,
        Err(why) => {
            eprintln!("cqlite split: {why}");
            std::process::exit(1);
        }
    };
    if let Some(manifest) = &args.manifest {
        if let Err(collision) = write_guard.assert_disjoint("--manifest", manifest, MANIFEST_REMEDY)
        {
            eprintln!("cqlite split: {collision}");
            std::process::exit(1);
        }
    }

    let report = match split_sstable(&args.input, boundary, &args.out, &schema).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("cqlite split: {e:#}");
            std::process::exit(1);
        }
    };

    render(&report, args.out_format);
    if let Some(manifest_path) = &args.manifest {
        if let Err(e) = write_manifest(&report, manifest_path) {
            eprintln!(
                "cqlite split: failed to write --manifest {}: {e}",
                manifest_path.display()
            );
            std::process::exit(1);
        }
    }

    if let Some(refused) = &report.refused {
        use cqlite_core::storage::write_engine::extract_split::RefusalReason;
        if refused.reason == RefusalReason::MultipleGenerations {
            std::process::exit(1);
        }
        std::process::exit(2);
    }
    std::process::exit(0);
}

fn render(report: &SplitReport, format: SalvageOutFormatArg) {
    match format {
        SalvageOutFormatArg::Json => match serde_json::to_string_pretty(report) {
            Ok(s) => println!("{s}"),
            Err(e) => eprintln!("cqlite split: failed to render JSON manifest: {e}"),
        },
        SalvageOutFormatArg::Text => {
            eprintln!(
                "split: input={} output={} boundary={}={}",
                report.input, report.output, report.boundary.kind, report.boundary.value
            );
            eprintln!(
                "  source: {} partitions, {} rows",
                report.source_partitions, report.source_rows
            );
            for p in &report.parts {
                eprintln!(
                    "  part gen={}: tokens=({}, {}] partitions={} rows={} bytes={} verify={}",
                    p.generation, p.min_token, p.max_token, p.partitions, p.rows, p.bytes, p.verify
                );
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

fn write_manifest(report: &SplitReport, path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string_pretty(report)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    std::fs::write(path, json)
}
