//! `cqlite find` — which generations of a table hold a partition key, how each was probed, and
//! (with `--commitlog`) which CommitLog mutations touch the key and whether any is unflushed
//! (issue #4205, forensics F4).
//!
//! Read-only. Fail closed (#4159): a generation or segment that could not be read is reported
//! with its cause and drives exit code 2 — never rendered as "not present". Unmeasurable values
//! print as `unmeasured(<cause>)`, never a guess or a zero.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::TableSchema;
use cqlite_core::storage::commitlog::Writetime;
use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::sstable::{GenerationProbe, SSTableReader, ScanOutcome};
use cqlite_core::Config;

use super::find_commitlog::{scan_commitlog, CommitEntry};
use crate::cli_types::{FindArgs, VerifyOutputArg};

/// What one generation produced: a probe verdict, or the cause it could not be probed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenOutcome {
    /// The generation was probed.
    Probe(GenerationProbe),
    /// The generation could not be opened/probed; the cause is verbatim.
    Unreadable(String),
}

/// One generation's row in the report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenRow {
    /// `Data.db` file name of the generation.
    pub basename: String,
    /// The generation's outcome.
    pub outcome: GenOutcome,
}

/// `unflushed` verdict for one CommitLog mutation (design D5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unflushed {
    /// Newer than everything any generation holds for the key.
    Yes,
    /// At or below the max writetime a generation holds.
    No,
    /// Not decidable; the cause is stated.
    Unmeasured(String),
}

/// The full report.
#[derive(Debug)]
pub struct FindReport {
    /// Per-generation rows in generation order.
    pub generations: Vec<GenRow>,
    /// CommitLog entries when `--commitlog` was given.
    pub commitlog: Option<Vec<CommitEntry>>,
}

impl FindReport {
    /// F5: 0 when the report completed (including "absent everywhere"), 2 when any generation
    /// or CommitLog segment could not be read.
    pub fn exit_code(&self) -> i32 {
        let gen_fail = self
            .generations
            .iter()
            .any(|g| matches!(g.outcome, GenOutcome::Unreadable(_)));
        let seg_fail = self.commitlog.as_ref().is_some_and(|c| {
            c.iter()
                .any(|e| matches!(e, CommitEntry::SegmentFailure { .. }))
        });
        if gen_fail || seg_fail {
            2
        } else {
            0
        }
    }
}

/// Max writetime any generation holds for the key (hits only).
fn max_generation_writetime(gens: &[GenRow]) -> Option<i64> {
    gens.iter()
        .filter_map(|g| match &g.outcome {
            GenOutcome::Probe(GenerationProbe::Hit(d))
            | GenOutcome::Probe(GenerationProbe::Scanned(ScanOutcome::Hit(d))) => {
                Some(d.max_writetime)
            }
            _ => None,
        })
        .max()
}

/// The fail-closed comparison of design D5.
///
/// A generation that could not be probed might hold a NEWER write, so it only blocks a `yes`;
/// a mutation already at or below a proven hit is `no` regardless.
pub fn unflushed(mutation: Writetime, gens: &[GenRow]) -> Unflushed {
    let w = match mutation {
        Writetime::Unmeasured(cause) => return Unflushed::Unmeasured(cause.to_string()),
        Writetime::Known(w) => w,
    };
    let max_hit = max_generation_writetime(gens);
    if max_hit.is_some_and(|m| w <= m) {
        return Unflushed::No;
    }
    if let Some(bad) = gens
        .iter()
        .find(|g| matches!(g.outcome, GenOutcome::Unreadable(_)))
    {
        return Unflushed::Unmeasured(format!("generation {} could not be probed", bad.basename));
    }
    Unflushed::Yes
}

/// Split a `<partition-key>` argument into its per-column CQL literals: top-level commas,
/// outside single-quoted strings (`''` is an escaped quote).
fn split_key_literals(arg: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut in_quote = false;
    for ch in arg.chars() {
        match ch {
            '\'' => {
                in_quote = !in_quote;
                cur.push(ch);
            }
            ',' if !in_quote => parts.push(std::mem::take(&mut cur)),
            _ => cur.push(ch),
        }
    }
    parts.push(cur);
    parts.into_iter().map(|p| p.trim().to_string()).collect()
}

/// Encode the user's partition-key literal(s) into the on-disk raw key bytes, using the same
/// SELECT literal parser and key codec `query` uses for `WHERE pk = <literal>`.
pub fn encode_partition_key(schema: &TableSchema, arg: &str) -> anyhow::Result<Vec<u8>> {
    use cqlite_core::query::select_ast::{ComparisonRightSide, SelectExpression, WhereExpression};
    let lits = split_key_literals(arg);
    if lits.len() != schema.partition_keys.len() {
        anyhow::bail!(
            "partition key has {} column(s) ({}); got {} comma-separated literal(s)",
            schema.partition_keys.len(),
            schema
                .partition_keys
                .iter()
                .map(|k| k.name.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            lits.len()
        );
    }
    let mut values = Vec::with_capacity(lits.len());
    for (col, lit) in schema.partition_keys.iter().zip(&lits) {
        let sql = format!("SELECT * FROM t WHERE {} = {lit}", col.name);
        let stmt = cqlite_core::query::parse_select(&sql)
            .map_err(|e| anyhow::anyhow!("cannot parse key literal {lit:?}: {e}"))?;
        let value = match stmt.where_clause {
            Some(WhereExpression::Comparison(c)) => match c.right {
                ComparisonRightSide::Value(SelectExpression::Literal(v)) => v,
                _ => anyhow::bail!("key literal {lit:?} is not a plain CQL literal"),
            },
            _ => anyhow::bail!("key literal {lit:?} is not a plain CQL literal"),
        };
        values.push(value);
    }
    cqlite_core::storage::partition_key_codec::encode_partition_key_columns(&values, schema)
        .map_err(|e| anyhow::anyhow!("cannot encode partition key for this schema: {e}"))
}

/// `<table>-<32 hex table id>` → `(table, dashed table id)`.
pub fn parse_table_dir_name(dir: &Path) -> Option<(String, String)> {
    let name = dir.file_name()?.to_str()?;
    let (table, id) = name.rsplit_once('-')?;
    if id.len() != 32 || !id.bytes().all(|b| b.is_ascii_hexdigit()) || table.is_empty() {
        return None;
    }
    let dashed = format!(
        "{}-{}-{}-{}-{}",
        &id[0..8],
        &id[8..12],
        &id[12..16],
        &id[16..20],
        &id[20..32]
    );
    Some((table.to_string(), dashed))
}

/// `*-Data.db` files of a table directory ordered by generation number (name tie-break).
fn list_generations(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut gens: Vec<(u64, PathBuf)> = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !name.ends_with("-Data.db") {
            continue;
        }
        // `<version>-<generation>-<format>-Data.db`; a non-numeric generation sorts last.
        let gen = name
            .split('-')
            .nth(1)
            .and_then(|g| g.parse::<u64>().ok())
            .unwrap_or(u64::MAX);
        gens.push((gen, path));
    }
    gens.sort();
    Ok(gens.into_iter().map(|(_, p)| p).collect())
}

/// Probe every generation. Never short-circuits: each failure becomes its own named row.
pub async fn probe_generations(gens: &[PathBuf], key: &[u8], schema: &TableSchema) -> Vec<GenRow> {
    let mut rows = Vec::with_capacity(gens.len());
    for path in gens {
        let basename = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| path.display().to_string());
        let outcome = probe_one(path, key, schema).await;
        rows.push(GenRow { basename, outcome });
    }
    rows
}

async fn probe_one(path: &Path, key: &[u8], schema: &TableSchema) -> GenOutcome {
    let config = Config::default();
    let platform = match Platform::new(&config).await {
        Ok(p) => Arc::new(p),
        Err(e) => return GenOutcome::Unreadable(format!("platform init failed: {e}")),
    };
    let reader = match SSTableReader::open(path, &config, platform).await {
        Ok(r) => r,
        Err(e) => return GenOutcome::Unreadable(format!("open failed: {e}")),
    };
    match reader
        .probe_generation(key, Some(schema), &ScanCancel::new())
        .await
    {
        Ok(p) => GenOutcome::Probe(p),
        Err(e) => GenOutcome::Unreadable(format!("probe failed: {e}")),
    }
}

fn usage(msg: &str) -> ! {
    eprintln!("cqlite find: {msg}");
    std::process::exit(1);
}

/// Entry point for `cqlite find`. Owns its exit-code space (0/1/2) via `process::exit`.
pub async fn execute_find_command(
    schema_path: Option<&Path>,
    args: &FindArgs,
) -> anyhow::Result<()> {
    let Some(schema_path) = schema_path else {
        usage("--schema is required (the global --schema flag)");
    };
    if !args.table_dir.is_dir() {
        usage(&format!("{} is not a directory", args.table_dir.display()));
    }
    let Some((table, table_id)) = parse_table_dir_name(&args.table_dir) else {
        usage(&format!(
            "cannot derive the table from {}: expected a `<table>-<32 hex table id>` directory",
            args.table_dir.display()
        ));
    };
    let schema =
        match super::write::load_compaction_table_schema_for_table(schema_path, Some(&table)) {
            Ok(s) => s,
            Err(e) => usage(&format!(
                "cannot resolve a schema for table '{table}': {e:#}"
            )),
        };
    let key = match encode_partition_key(&schema, &args.partition_key) {
        Ok(k) => k,
        Err(e) => usage(&format!("{e:#}")),
    };
    let gens = match list_generations(&args.table_dir) {
        Ok(g) if !g.is_empty() => g,
        Ok(_) => usage(&format!(
            "no *-Data.db generation in {}",
            args.table_dir.display()
        )),
        Err(e) => usage(&format!("cannot list {}: {e}", args.table_dir.display())),
    };
    let generations = probe_generations(&gens, &key, &schema).await;
    let commitlog = args
        .commitlog
        .as_ref()
        .map(|dir| scan_commitlog(dir, &schema, &table_id, &key, &generations));
    let report = FindReport {
        generations,
        commitlog,
    };
    match args.out {
        VerifyOutputArg::Text => println!("{}", super::find_render::render_text(&report)),
        VerifyOutputArg::Json => println!("{}", super::find_render::render_json(&report)),
    }
    std::process::exit(report.exit_code());
}

#[cfg(test)]
#[path = "find_tests.rs"]
mod tests;
