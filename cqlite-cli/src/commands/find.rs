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
use cqlite_core::storage::commitlog::{Need, Writetime};
use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::sstable::reader::{CellCoverage, GenerationProbe, ScanOutcome};
use cqlite_core::storage::sstable::SSTableReader;
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
    /// Every cell, liveness marker and deletion of the mutation is proven present in the SSTables.
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
    /// or CommitLog segment could not be read in full (a torn tail included).
    pub fn exit_code(&self) -> i32 {
        let gen_fail = self
            .generations
            .iter()
            .any(|g| matches!(g.outcome, GenOutcome::Unreadable(_)));
        // A torn tail hides records that might touch the key: fail closed like a failure.
        let seg_fail = self.commitlog.as_ref().is_some_and(|c| {
            c.iter().any(|e| {
                matches!(
                    e,
                    CommitEntry::SegmentFailure { .. } | CommitEntry::SegmentTruncated { .. }
                )
            })
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

/// The first of `needs` no hit generation proves present, or `None` when every one is proven.
///
/// A need is proven by ONE hit generation holding, per cell, a STRICTLY newer cell of the same
/// column, or a cell at the SAME timestamp in exactly the same reconciliation-relevant state
/// (both live and non-expiring, with identical value bytes); likewise a newer or an equal
/// non-expiring liveness marker; or a covering deletion at or above the mutation's timestamp.
/// An equal timestamp alone proves nothing: `Cells.resolveRegular` breaks ties by
/// tombstone/expiring state, local deletion time, TTL and value bytes (cassandra-5.0.8). The
/// comparison is per cell, never the partition's max timestamp: a later write of column `a`
/// says nothing about an earlier-timestamped write of column `b` (roborev job 299 M1, issue
/// #4205).
fn first_unproven<'a>(needs: &'a [Need], gens: &[GenRow]) -> Option<&'a Need> {
    let covers: Vec<&CellCoverage> = gens
        .iter()
        .filter_map(|g| match &g.outcome {
            GenOutcome::Probe(GenerationProbe::Hit(d))
            | GenOutcome::Probe(GenerationProbe::Scanned(ScanOutcome::Hit(d))) => Some(&d.coverage),
            _ => None,
        })
        .collect();
    let shadowed = |c: &CellCoverage, t: i64| c.deletion_timestamp.is_some_and(|d| d >= t);
    needs.iter().find(|n| {
        !covers.iter().any(|c| match n {
            Need::Cell {
                column,
                timestamp,
                value,
            } => {
                shadowed(c, *timestamp)
                    || match c.cell_timestamps.get(column) {
                        Some(t) if t > timestamp => true,
                        Some(t) if t == timestamp => {
                            value.is_some() && c.cell_values.get(column) == Some(value)
                        }
                        _ => false,
                    }
            }
            Need::Liveness {
                timestamp,
                expiring,
            } => {
                shadowed(c, *timestamp)
                    || match c.liveness_timestamp {
                        Some(l) if l > *timestamp => true,
                        Some(l) if l == *timestamp => c.liveness_plain && !*expiring,
                        _ => false,
                    }
            }
            Need::Deletion(t) => shadowed(c, *t),
        })
    })
}

/// The fail-closed verdict of design D5.
///
/// `Yes` (unflushed) needs only the newest write to be above everything any generation holds: its
/// own cell then cannot be in any SSTable. `No` (flushed) is a PROOF obligation: every cell,
/// liveness marker and deletion of the mutation must be present in the SSTables (see
/// [`first_unproven`]); a mutation at or below the partition's max timestamp whose cells are not
/// all proven is `Unmeasured`, never a guessed `No`. A generation that could not be probed might
/// hold a newer write, so it only blocks a `Yes`.
pub fn unflushed(
    mutation: Writetime,
    needs: Result<Vec<Need>, &'static str>,
    gens: &[GenRow],
) -> Unflushed {
    let w = match mutation {
        Writetime::Unmeasured(cause) => return Unflushed::Unmeasured(cause.to_string()),
        Writetime::Known(w) => w,
    };
    let max_hit = max_generation_writetime(gens);
    if max_hit.is_some_and(|m| w <= m) {
        return match needs {
            Err(cause) => Unflushed::Unmeasured(format!(
                "mutation coverage not proven by SSTable cells: {cause}"
            )),
            Ok(needs) => match first_unproven(&needs, gens) {
                None => Unflushed::No,
                Some(n) => Unflushed::Unmeasured(format!(
                    "mutation coverage not proven by SSTable cells: {}",
                    describe(n)
                )),
            },
        };
    }
    if let Some(bad) = gens
        .iter()
        .find(|g| matches!(g.outcome, GenOutcome::Unreadable(_)))
    {
        return Unflushed::Unmeasured(format!("generation {} could not be probed", bad.basename));
    }
    Unflushed::Yes
}

fn describe(n: &Need) -> String {
    match n {
        Need::Cell {
            column, timestamp, ..
        } => format!("column {column} @ {timestamp} (no newer or exactly equal SSTable cell)"),
        Need::Liveness { timestamp, .. } => {
            format!("row liveness marker @ {timestamp} (no newer or exactly equal SSTable marker)")
        }
        Need::Deletion(t) => format!("row deletion @ {t}"),
    }
}

/// Split a `<partition-key>` argument into its per-column CQL literals: commas at nesting depth
/// zero, outside quoted text. `(`/`[`/`{` nest (tuple, list/set, map/UDT literals); `'...'`
/// (with `''` escaped) and `"..."` quoted text is opaque.
pub(crate) fn split_key_literals(arg: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    let mut depth: usize = 0;
    for ch in arg.chars() {
        match (quote, ch) {
            (Some(q), c) if c == q => {
                // `''` inside a string toggles twice, which is exactly an escaped quote.
                quote = None;
                cur.push(c);
            }
            (Some(_), c) => cur.push(c),
            (None, '\'' | '"') => {
                quote = Some(ch);
                cur.push(ch);
            }
            (None, '(' | '[' | '{') => {
                depth = depth.saturating_add(1);
                cur.push(ch);
            }
            (None, ')' | ']' | '}') => {
                depth = depth.saturating_sub(1);
                cur.push(ch);
            }
            (None, ',') if depth == 0 => parts.push(std::mem::take(&mut cur)),
            (None, c) => cur.push(c),
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
        // A FIXED synthetic column name: the real name (quoted identifier, reserved word, spaces,
        // embedded quotes) never enters the query text, so it cannot break the parse. The
        // value is encoded against the schema column by position below.
        let _ = col;
        let sql = format!("SELECT * FROM t WHERE k = {lit}");
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

/// Component names (after the `<version>-<generation>-<format>-` prefix) that mark a file as part
/// of an SSTable generation.
const GENERATION_COMPONENTS: &[&str] = &[
    "Data.db",
    "Index.db",
    "Statistics.db",
    "Summary.db",
    "Filter.db",
    "TOC.txt",
    "CompressionInfo.db",
    "Digest.crc32",
    "CRC.db",
    "Partitions.db",
    "Rows.db",
];

/// The expected `*-Data.db` of EVERY generation that has any recognised component in `dir`,
/// ordered by generation number (name tie-break). A generation whose `Data.db` is gone but whose
/// `TOC.txt`/`Index.db`/`Statistics.db` remain is still listed — it is then probed and reported
/// as UNREADABLE, never silently omitted.
fn list_generations(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut prefixes: std::collections::BTreeSet<(u64, String)> = std::collections::BTreeSet::new();
    for entry in std::fs::read_dir(dir)? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        // `<version>-<generation>-<format>-<Component>`
        let mut parts = name.splitn(4, '-');
        let (Some(ver), Some(gen), Some(fmt), Some(component)) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        if !GENERATION_COMPONENTS.contains(&component) {
            continue;
        }
        // A non-numeric generation sorts last.
        let order = gen.parse::<u64>().unwrap_or(u64::MAX);
        prefixes.insert((order, format!("{ver}-{gen}-{fmt}")));
    }
    Ok(prefixes
        .into_iter()
        .map(|(_, prefix)| dir.join(format!("{prefix}-Data.db")))
        .collect())
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
    if !path.exists() {
        return GenOutcome::Unreadable(
            "Data.db is missing although other components of this generation are present"
                .to_string(),
        );
    }
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
        // A never-flushed table has no generation: a legitimate "where did my write go"
        // answer (everything is in the CommitLog), not a usage error.
        Ok(g) => g,
        // An operational read failure, not a usage error: exit 2 with the cause named.
        Err(e) => {
            eprintln!("cqlite find: cannot list {}: {e}", args.table_dir.display());
            std::process::exit(2);
        }
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
