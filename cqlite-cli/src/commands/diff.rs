//! `cqlite diff`: compare two snapshots of one table (issue #4201; OpenSpec `snapshot-diff`).
//!
//! READ-ONLY. Nothing here opens an input for writing, creates a file or modifies either snapshot
//! directory; the report is rendered to stdout only (like `explain`, `diff` does not accept the
//! global `--output`/`--overwrite` file-write flags). Each side is resolved exactly as `explain`
//! resolves a table directory (`explain_resolve::{resolve_table_dir, resolve_generations}`:
//! live generations only, never `snapshots/`, `backups/` or index subdirectories).
//!
//! Exit codes: 0 = a report was rendered (divergence and resurrection risk are findings, not
//! failures); 1 = usage error; 2 = a generation on either side could not be read (the message
//! names the side and the path) or the report exceeded the result budget.

use crate::commands::diff_args::{DiffArgs, DiffOutput};
use crate::config::Config;
use anyhow::Result;

/// Resolve the `diff` verb's arguments from the parsed CLI and run it. Owns its exit-code space
/// and never returns an `Err` (a usage error routed through `?` would be reclassified by
/// `error::classify_error` into a different exit code).
pub async fn dispatch_diff(
    cli: &crate::cli_types::Cli,
    config: &Config,
    args: &DiffArgs,
) -> Result<()> {
    #[cfg(feature = "write-support")]
    {
        match run_diff(cli.schema.as_deref(), args, config).await {
            Ok(rendered) => {
                print!("{rendered}");
                Ok(())
            }
            Err(failure) => {
                eprintln!("cqlite diff: {}", failure.message);
                std::process::exit(failure.code);
            }
        }
    }
    #[cfg(not(feature = "write-support"))]
    {
        let _ = (cli, config, args);
        eprintln!(
            "cqlite diff requires a build with write support; rebuild with --features write-support"
        );
        std::process::exit(1);
    }
}

#[cfg(feature = "write-support")]
struct Failure {
    code: i32,
    message: String,
}

#[cfg(feature = "write-support")]
impl Failure {
    fn usage(message: impl Into<String>) -> Self {
        Self {
            code: 1,
            message: message.into(),
        }
    }
    fn read(message: impl Into<String>) -> Self {
        Self {
            code: 2,
            message: message.into(),
        }
    }
}

/// One resolved side: its table directory and live generations (newest first).
#[cfg(feature = "write-support")]
fn resolve_side(
    label: &str,
    root: &std::path::Path,
    keyspace: &str,
    table: &str,
) -> std::result::Result<cqlite_core::storage::write_engine::snapshot_diff::DiffSide, Failure> {
    use crate::commands::explain_resolve::{resolve_generations, resolve_table_dir};
    if !root.is_dir() {
        return Err(Failure::usage(format!(
            "side={label}: {} does not exist or is not a directory",
            root.display()
        )));
    }
    let dir = resolve_table_dir(root, keyspace, table)
        .map_err(|error| Failure::usage(format!("side={label}: {error}")))?;
    // GD6: each side is the generation module's Scope Live, `TocMissing` accepted. A condition
    // that refuses (below floor, `Corrupt`, `Unreadable`, an inconsistent transaction state) is
    // exit 2 naming the side.
    let resolved = resolve_generations(&dir)
        .map_err(|error| Failure::read(format!("side={label}: {error}")))?;
    crate::commands::generation_listing::print_listing_facts("diff", &resolved.listing);
    // A side holding an Orphan (Data.db missing or zero-length) is REFUSED: dropping it and
    // continuing could report a false divergence or resurrection (#4671).
    if !resolved.listing.orphans.is_empty() {
        return Err(Failure::read(format!(
            "side={label}: refusing {} with an Orphan generation (Data.db missing or zero-length): {}",
            dir.display(),
            resolved
                .listing
                .orphans
                .iter()
                .map(|o| crate::commands::generation_listing::data_path(o).display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    let listed_names = resolved.listed_names;
    let generations = resolved.input_paths;
    if generations.is_empty() {
        return Err(Failure::usage(format!(
            "side={label}: no published SSTable generations found under {}",
            dir.display()
        )));
    }
    Ok(
        cqlite_core::storage::write_engine::snapshot_diff::DiffSide {
            dir,
            generations,
            listed_names,
        },
    )
}

#[cfg(feature = "write-support")]
async fn run_diff(
    schema_path: Option<&std::path::Path>,
    args: &DiffArgs,
    config: &Config,
) -> std::result::Result<String, Failure> {
    use cqlite_core::storage::write_engine::snapshot_diff::{
        diff_partitions, diff_table, DiffError, DiffOptions,
    };

    let schema_path = schema_path.ok_or_else(|| Failure::usage("--schema is required for diff"))?;
    let mut parts = args.table.split('.');
    let (Some(keyspace), Some(table_name), None) = (parts.next(), parts.next(), parts.next())
    else {
        return Err(Failure::usage(format!(
            "--table must be a fully-qualified keyspace.table name, got '{}'",
            args.table
        )));
    };
    if keyspace.is_empty() || table_name.is_empty() {
        return Err(Failure::usage(format!(
            "--table must be a fully-qualified keyspace.table name, got '{}'",
            args.table
        )));
    }
    let side_a = resolve_side("a", &args.dir_a, keyspace, table_name)?;
    let side_b = resolve_side("b", &args.dir_b, keyspace, table_name)?;

    let schema = crate::commands::write::load_compaction_table_schema_in_keyspace(
        schema_path,
        Some(keyspace),
        Some(table_name),
    )
    .map_err(|error| Failure::usage(format!("could not load schema: {error}")))?;
    if !schema.keyspace.eq_ignore_ascii_case(keyspace) {
        return Err(Failure::usage(format!(
            "schema keyspace '{}' does not match requested keyspace '{}'",
            schema.keyspace, keyspace
        )));
    }
    let mut keys = Vec::new();
    for literal in &args.partition {
        let values = crate::commands::explain::parse_literal_values(literal).map_err(|error| {
            Failure::usage(format!("invalid --partition literal '{literal}': {error}"))
        })?;
        keys.push(
            cqlite_core::storage::partition_key_codec::encode_partition_key_columns(
                &values, &schema,
            )
            .map_err(|error| {
                Failure::usage(format!("invalid partition key '{literal}': {error}"))
            })?,
        );
    }
    let now = crate::commands::explain::resolve_now(args.now.as_deref())
        .map_err(|error| Failure::usage(error.to_string()))?;
    // The ONE bounded-result budget every materializing CLI surface shares.
    let max_result_bytes = crate::core_config::to_core_config(config)
        .map_err(|error| Failure::usage(format!("invalid configuration: {error:#}")))?
        .query
        .max_result_bytes;
    // The SAME registry a schema-aware read builds (#2349), so a frozen UDT inside a collection
    // decodes as it does for SELECT.
    let udt_registry =
        crate::commands::write::udt_registry_from_schema_file(schema_path, &schema.keyspace);
    let options = DiffOptions {
        now_secs: now.epoch_secs,
        max_result_bytes,
    };
    let outcome = if keys.is_empty() {
        diff_table(
            &side_a,
            &side_b,
            &args.table,
            &schema,
            Some(&udt_registry),
            &options,
        )
        .await
    } else {
        diff_partitions(
            &side_a,
            &side_b,
            &args.table,
            &schema,
            Some(&udt_registry),
            &options,
            &keys,
        )
        .await
    };
    let report = outcome.map_err(|error| match error {
        DiffError::InvalidNow { .. } => Failure::usage(error.to_string()),
        DiffError::SideRead { .. } | DiffError::CellSerialization { .. } => {
            Failure::read(error.to_string())
        }
        DiffError::Core(inner) => match inner {
            cqlite_core::Error::ResultTooLarge { .. }
            | cqlite_core::Error::Cancelled
            | cqlite_core::Error::Io(_) => Failure::read(inner.to_string()),
            other => Failure::usage(other.to_string()),
        },
    })?;
    let rendered =
        crate::commands::diff_render::render(&report, args.out == DiffOutput::Json, &now)
            .map_err(Failure::read)?;
    let limit = render_limit(max_result_bytes);
    if rendered.len() as u64 > limit {
        return Err(Failure::read(format!(
            "result set exceeded the {limit}-byte materialization budget (estimated {} bytes); restrict with --partition or raise max_result_bytes",
            rendered.len()
        )));
    }
    Ok(rendered)
}

/// The rendered report's limit: the configured budget, never above the core's effective cap
/// (`MAX_EFFECTIVE_BUDGET`), so the CLI and the library agree on one ceiling.
#[cfg(feature = "write-support")]
fn render_limit(configured: u64) -> u64 {
    configured.min(cqlite_core::storage::write_engine::snapshot_diff::MAX_EFFECTIVE_BUDGET)
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::render_limit;

    #[test]
    fn the_render_limit_never_exceeds_the_effective_cap() {
        let cap = cqlite_core::storage::write_engine::snapshot_diff::MAX_EFFECTIVE_BUDGET;
        assert_eq!(render_limit(u64::MAX), cap);
        assert_eq!(render_limit(cap + 1), cap);
        assert_eq!(render_limit(1000), 1000);
    }
}
