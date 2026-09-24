//! `cqlite explain`: render reconciliation decisions for one partition.
//!
//! The command deliberately stops at the traced point-read merge boundary. It
//! does not open a [`Database`], build an ingestion registry, or write an
//! SSTable. That keeps forensic output tied to the same schema-aware merge
//! rules as a read while making the command useful against a directory that is
//! otherwise not a configured CQLite database.
//!
//! `explain` NEVER writes a file (`openspec/changes/forensics-explain/proposal.md`
//! Non-goals: "Any write. `explain` never creates, modifies or compacts a
//! file") — it always renders to stdout and, like `verify`, does not accept
//! the global `--output`/`--overwrite` file-write flags at all.

use crate::cli::OutputFormat;
#[cfg(feature = "write-support")]
use crate::commands::explain_render::{render_report, ExplainReport};
use crate::config::Config;
use anyhow::Result;
use std::path::Path;

/// Execute the explain command. The command owns its documented exit-code
/// space: 0 is a rendered report, 1 is an invocation/schema/table failure, and
/// 2 is a generation read or rendering failure.
pub async fn execute_explain_command(
    schema_path: Option<&Path>,
    data_dir: Option<&Path>,
    dataset: Option<&str>,
    table: &str,
    partition_key: &str,
    clustering: &[String],
    now: Option<&str>,
    out: OutputFormat,
    config: &Config,
) -> Result<()> {
    #[cfg(feature = "write-support")]
    {
        match run_explain(
            schema_path,
            data_dir,
            dataset,
            table,
            partition_key,
            clustering,
            now,
            out,
            config,
        ) {
            Ok(rendered) => {
                // Always stdout — explain never writes a file (Non-goals:
                // "Any write. `explain` never creates, modifies or compacts
                // a file"). Unlike `query`, it does not accept the global
                // --output/--overwrite flags at all.
                print!("{rendered}");
                Ok(())
            }
            Err(error) => {
                let code = error.exit_code();
                print_failure(error, code);
            }
        }
    }

    #[cfg(not(feature = "write-support"))]
    {
        let _ = (
            schema_path,
            data_dir,
            dataset,
            table,
            partition_key,
            clustering,
            now,
            out,
            config,
        );
        eprintln!(
            "cqlite explain requires a build with write support; rebuild with --features write-support"
        );
        std::process::exit(1);
    }
}

#[cfg(feature = "write-support")]
#[derive(Debug)]
enum ExplainFailure {
    Usage(String),
    Read(String),
}

#[cfg(feature = "write-support")]
impl ExplainFailure {
    fn exit_code(&self) -> i32 {
        match self {
            Self::Usage(_) => 1,
            Self::Read(_) => 2,
        }
    }
}

#[cfg(feature = "write-support")]
fn print_failure(error: ExplainFailure, code: i32) -> ! {
    let message = match error {
        ExplainFailure::Usage(message) | ExplainFailure::Read(message) => message,
    };
    eprintln!("cqlite explain: {message}");
    std::process::exit(code);
}

#[cfg(feature = "write-support")]
fn run_explain(
    schema_path: Option<&Path>,
    data_dir: Option<&Path>,
    dataset: Option<&str>,
    table: &str,
    partition_key: &str,
    clustering: &[String],
    now: Option<&str>,
    out: OutputFormat,
    config: &Config,
) -> std::result::Result<String, ExplainFailure> {
    if matches!(out, OutputFormat::Parquet) {
        return Err(ExplainFailure::Usage(
            "explain supports only table, json, and csv output".to_string(),
        ));
    }

    let schema_path = schema_path
        .ok_or_else(|| ExplainFailure::Usage("--schema is required for explain".to_string()))?;
    let (keyspace, table_name) = split_table_name(table)?;
    let root = resolve_data_root(data_dir, dataset)?;
    let table_dir = resolve_table_dir(&root, table_name)?;
    let input_paths = crate::commands::write::discover_input_sstables(&table_dir)
        .map_err(|error| read_failure(&input_paths_for_error(&table_dir), error))?;
    if input_paths.is_empty() {
        return Err(ExplainFailure::Usage(format!(
            "no published SSTable generations found under {}",
            table_dir.display()
        )));
    }

    let schema = crate::commands::write::load_compaction_table_schema_for_table(
        schema_path,
        Some(table_name),
    )
    .map_err(|error| ExplainFailure::Usage(format!("could not load schema: {error}")))?;
    if !schema.keyspace.eq_ignore_ascii_case(keyspace) {
        return Err(ExplainFailure::Usage(format!(
            "schema keyspace '{}' does not match requested keyspace '{}'",
            schema.keyspace, keyspace
        )));
    }

    let partition_values = parse_literal_values(partition_key).map_err(|error| {
        ExplainFailure::Usage(format!("invalid partition-key literal: {error}"))
    })?;
    let raw_partition_key =
        cqlite_core::storage::partition_key_codec::encode_partition_key_columns(
            &partition_values,
            &schema,
        )
        .map_err(|error| ExplainFailure::Usage(format!("invalid partition key: {error}")))?;

    let clustering_key = parse_clustering_key(clustering, &schema)
        .map_err(|error| ExplainFailure::Usage(format!("invalid clustering key: {error}")))?;
    let now_value = resolve_now(now).map_err(|error| ExplainFailure::Usage(error.to_string()))?;
    let effective_schema = cqlite_core::storage::write_engine::merge::effective_compaction_schema(
        &schema,
        &input_paths,
    );
    let gc_before = cqlite_core::storage::write_engine::merge::compute_gc_before(
        &effective_schema,
        now_value.epoch_secs,
    );
    let sink = BudgetedTraceSink::new(config.performance.max_result_bytes);
    let merger =
        cqlite_core::storage::write_engine::merge::build_single_partition_merger_with_trace(
            input_paths.clone(),
            &[raw_partition_key],
            &effective_schema,
            cqlite_core::storage::scan_cancel::ScanCancel::default(),
            sink,
        )
        .map_err(|error| read_failure(&input_paths, error))?;

    let (cells, tombstones, probes) = if let Some(merger) = merger {
        // These setters intentionally run on the traced merger. The core API
        // keeps the same settings on `KWayMerger<S>` for every trace sink, so
        // explain and the ordinary point-read path share TTL and purge clocks.
        let mut merger = merger
            .with_now_secs(Some(now_value.epoch_secs))
            .with_gc_before_secs(gc_before)
            .with_purge_safe(true);
        loop {
            match merger
                .step()
                .map_err(|error| read_failure(&input_paths, error))?
            {
                cqlite_core::storage::write_engine::merge::MergeStep::Complete => break,
                cqlite_core::storage::write_engine::merge::MergeStep::Partition { .. } => {}
            }
        }
        let sink = merger.into_trace_sink();
        if sink.overflowed {
            return Err(ExplainFailure::Read(format!(
                "explain trace exceeded the {}-byte materialization budget; add a clustering filter or raise max_result_bytes",
                config.performance.max_result_bytes
            )));
        }
        sink.into_parts()
    } else {
        // The point builder owns the probe sink while it constructs runs and
        // returns None only after every generation reported Absent. Keeping
        // this closed case explicit preserves one generation row per input.
        (
            Vec::new(),
            Vec::new(),
            input_paths
                .iter()
                .enumerate()
                .map(|(run_index, _)| {
                    (
                        run_index,
                        cqlite_core::storage::write_engine::merge::trace::ProbeOutcome::Absent,
                    )
                })
                .collect(),
        )
    };

    let report = ExplainReport::from_trace(
        now_value.epoch_secs,
        input_paths,
        probes,
        cells,
        tombstones,
        clustering_key.as_ref(),
    );
    let rendered = render_report(
        &report,
        out,
        &now_value,
        gc_grace_seconds(&effective_schema),
    )
    .map_err(ExplainFailure::Read)?;
    if rendered.len() as u64 > config.performance.max_result_bytes {
        return Err(ExplainFailure::Read(format!(
            "result set exceeded the {}-byte materialization budget (estimated {} bytes); add a clustering filter or raise max_result_bytes",
            config.performance.max_result_bytes,
            rendered.len()
        )));
    }
    Ok(rendered)
}

#[cfg(feature = "write-support")]
fn split_table_name(table: &str) -> std::result::Result<(&str, &str), ExplainFailure> {
    let mut parts = table.split('.');
    let keyspace = parts.next().unwrap_or_default();
    let table_name = parts.next().unwrap_or_default();
    if keyspace.is_empty() || table_name.is_empty() || parts.next().is_some() {
        return Err(ExplainFailure::Usage(format!(
            "table must be a fully-qualified keyspace.table name, got '{table}'"
        )));
    }
    Ok((keyspace, table_name))
}

#[cfg(feature = "write-support")]
fn resolve_data_root(
    data_dir: Option<&Path>,
    dataset: Option<&str>,
) -> std::result::Result<std::path::PathBuf, ExplainFailure> {
    match (data_dir, dataset) {
        (Some(_), Some(_)) => Err(ExplainFailure::Usage(
            "--data-dir and --dataset are mutually exclusive".to_string(),
        )),
        (Some(path), None) => {
            if path.is_dir() {
                Ok(path.to_path_buf())
            } else {
                Err(ExplainFailure::Usage(format!(
                    "data directory {} does not exist or is not a directory",
                    path.display()
                )))
            }
        }
        (None, Some(dataset)) => {
            if dataset.contains("..")
                || dataset.contains('/')
                || dataset.contains('\\')
                || dataset.starts_with('.')
            {
                return Err(ExplainFailure::Usage(format!(
                    "invalid dataset name '{dataset}'"
                )));
            }
            let datasets_root = std::env::var("CQLITE_DATASETS_ROOT")
                .ok()
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| std::path::PathBuf::from("test-data/datasets"));
            let path = datasets_root.join("sstables").join(dataset);
            if path.is_dir() {
                Ok(path)
            } else {
                Err(ExplainFailure::Usage(format!(
                    "dataset '{dataset}' not found under {}",
                    datasets_root.display()
                )))
            }
        }
        (None, None) => Err(ExplainFailure::Usage(
            "one of --data-dir or --dataset is required for explain".to_string(),
        )),
    }
}

#[cfg(feature = "write-support")]
fn resolve_table_dir(
    root: &Path,
    table_name: &str,
) -> std::result::Result<std::path::PathBuf, ExplainFailure> {
    let root_name_matches = root
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| table_directory_name_matches(name, table_name))
        .unwrap_or(false);
    if root_name_matches {
        let generations =
            crate::commands::write::discover_input_sstables(root).map_err(|error| {
                ExplainFailure::Usage(format!("could not inspect {}: {error}", root.display()))
            })?;
        if !generations.is_empty() {
            return Ok(root.to_path_buf());
        }
    }

    let mut candidates = Vec::new();
    collect_table_dirs(root, table_name, 8, &mut candidates)?;
    candidates.sort();
    candidates.dedup();
    match candidates.as_slice() {
        [] => Err(ExplainFailure::Usage(format!(
            "table '{}' has no published SSTable directory below {}",
            table_name,
            root.display()
        ))),
        [only] => Ok(only.clone()),
        many => Err(ExplainFailure::Usage(format!(
            "table '{}' resolves to multiple SSTable directories: {}",
            table_name,
            many.iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}

#[cfg(feature = "write-support")]
fn collect_table_dirs(
    root: &Path,
    table_name: &str,
    depth: usize,
    out: &mut Vec<std::path::PathBuf>,
) -> std::result::Result<(), ExplainFailure> {
    if depth == 0 {
        return Ok(());
    }
    let entries = std::fs::read_dir(root).map_err(|error| {
        ExplainFailure::Usage(format!(
            "could not read data directory {}: {error}",
            root.display()
        ))
    })?;
    for entry in entries {
        let entry = entry.map_err(|error| ExplainFailure::Usage(error.to_string()))?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let matches = path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| table_directory_name_matches(name, table_name))
            .unwrap_or(false);
        if matches {
            let generations =
                crate::commands::write::discover_input_sstables(&path).map_err(|error| {
                    ExplainFailure::Usage(format!("could not inspect {}: {error}", path.display()))
                })?;
            if !generations.is_empty() {
                out.push(path.clone());
            }
        }
        collect_table_dirs(&path, table_name, depth - 1, out)?;
    }
    Ok(())
}

#[cfg(feature = "write-support")]
fn table_directory_name_matches(name: &str, table_name: &str) -> bool {
    name.eq_ignore_ascii_case(table_name)
        || name
            .strip_prefix(table_name)
            .map(|suffix| suffix.starts_with('-'))
            .unwrap_or(false)
}

#[cfg(feature = "write-support")]
fn parse_literal_values(raw: &str) -> Result<Vec<cqlite_core::Value>> {
    use cqlite_core::query::select_ast::{SelectClause, SelectExpression};
    use cqlite_core::query::select_parser::parse_select;

    let raw = raw.trim();
    if raw.is_empty() {
        anyhow::bail!("literal list is empty");
    }
    let statement = parse_select(&format!("SELECT {raw}"))?;
    if statement.from_clause.is_some()
        || statement.where_clause.is_some()
        || statement.group_by.is_some()
        || statement.having_clause.is_some()
        || statement.order_by.is_some()
        || statement.limit.is_some()
        || statement.per_partition_limit.is_some()
        || statement.offset.is_some()
        || statement.allow_filtering
    {
        anyhow::bail!("only comma-separated literal projections are accepted");
    }
    let expressions = match statement.select_clause {
        SelectClause::Columns(expressions) => expressions,
        SelectClause::All | SelectClause::Distinct(_) => {
            anyhow::bail!("only literal projections are accepted")
        }
    };
    expressions
        .into_iter()
        .map(|expression| match expression {
            SelectExpression::Literal(value) => Ok(value),
            _ => anyhow::bail!("every key component must be a literal"),
        })
        .collect()
}

#[cfg(feature = "write-support")]
fn parse_clustering_key(
    raw_components: &[String],
    schema: &cqlite_core::schema::TableSchema,
) -> Result<Option<cqlite_core::storage::write_engine::ClusteringKey>> {
    if raw_components.is_empty() {
        return Ok(None);
    }
    if schema.clustering_keys.is_empty() {
        anyhow::bail!("the table has no clustering columns")
    }
    let mut values = Vec::new();
    for component in raw_components {
        values.extend(parse_literal_values(component)?);
    }
    if values.len() != schema.clustering_keys.len() {
        anyhow::bail!(
            "expected {} clustering literal(s), got {}",
            schema.clustering_keys.len(),
            values.len()
        );
    }
    let mut columns = Vec::with_capacity(values.len());
    for (column, value) in schema.clustering_keys.iter().zip(values) {
        let comparator = cqlite_core::types::ComparatorType::from_data_type(&column.data_type)?;
        let bytes = cqlite_core::storage::partition_key_codec::encode_single_component_key_typed(
            &value,
            &column.data_type,
        )?;
        let typed = cqlite_core::storage::partition_key_codec::deserialize_value_bytes(
            &bytes,
            &comparator,
        )?;
        columns.push((column.name.clone(), typed));
    }
    Ok(Some(
        cqlite_core::storage::write_engine::ClusteringKey::new(columns),
    ))
}

#[cfg(feature = "write-support")]
#[derive(Debug, Clone, Copy)]
pub(super) struct ExplainNow {
    pub(super) epoch_secs: i64,
    pub(super) wall_clock: bool,
}

#[cfg(feature = "write-support")]
fn resolve_now(raw: Option<&str>) -> Result<ExplainNow> {
    if let Some(raw) = raw {
        let raw = raw.trim();
        if let Ok(epoch_secs) = raw.parse::<i64>() {
            return Ok(ExplainNow {
                epoch_secs,
                wall_clock: false,
            });
        }
        let parsed = chrono::DateTime::parse_from_rfc3339(raw)
            .map_err(|error| anyhow::anyhow!("--now must be epoch seconds or RFC3339: {error}"))?;
        return Ok(ExplainNow {
            epoch_secs: parsed.timestamp(),
            wall_clock: false,
        });
    }
    let epoch_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| anyhow::anyhow!("could not read wall clock: {error}"))?
        .as_secs()
        .try_into()
        .map_err(|_| anyhow::anyhow!("wall clock is outside the supported epoch range"))?;
    Ok(ExplainNow {
        epoch_secs,
        wall_clock: true,
    })
}

#[cfg(feature = "write-support")]
fn gc_grace_seconds(schema: &cqlite_core::schema::TableSchema) -> i64 {
    schema
        .comments
        .get("gc_grace_seconds")
        .and_then(|value| value.trim().parse::<i64>().ok())
        .filter(|value| *value >= 0)
        .unwrap_or(864_000)
}

#[cfg(feature = "write-support")]
fn input_paths_for_error(dir: &Path) -> Vec<std::path::PathBuf> {
    vec![dir.to_path_buf()]
}

#[cfg(feature = "write-support")]
fn read_failure<T: std::fmt::Display>(paths: &[std::path::PathBuf], error: T) -> ExplainFailure {
    let names = paths
        .iter()
        .map(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| path.to_string_lossy().into_owned())
        })
        .collect::<Vec<_>>()
        .join(", ");
    ExplainFailure::Read(format!("generation read failed for [{names}]: {error}"))
}

#[cfg(feature = "write-support")]
struct BudgetedTraceSink {
    inner: cqlite_core::storage::write_engine::merge::trace::RecordingSink,
    budget: u64,
    estimated: u64,
    overflowed: bool,
}

#[cfg(feature = "write-support")]
impl BudgetedTraceSink {
    fn new(budget: u64) -> Self {
        Self {
            inner: cqlite_core::storage::write_engine::merge::trace::RecordingSink::new(),
            budget,
            estimated: 0,
            overflowed: false,
        }
    }

    fn reserve<T: serde::Serialize>(&mut self, event: &T) -> bool {
        if self.overflowed {
            return false;
        }
        let event_bytes = serde_json::to_vec(event)
            .map(|bytes| bytes.len() as u64)
            .unwrap_or(u64::MAX);
        if self.estimated.saturating_add(event_bytes) > self.budget {
            self.overflowed = true;
            return false;
        }
        self.estimated = self.estimated.saturating_add(event_bytes);
        true
    }

    fn into_parts(
        self,
    ) -> (
        Vec<cqlite_core::storage::write_engine::merge::trace::CellDecision>,
        Vec<cqlite_core::storage::write_engine::merge::trace::TombstoneRecord>,
        Vec<(
            usize,
            cqlite_core::storage::write_engine::merge::trace::ProbeOutcome,
        )>,
    ) {
        self.inner.into_parts()
    }
}

#[cfg(feature = "write-support")]
impl cqlite_core::storage::write_engine::merge::trace::TraceSink for BudgetedTraceSink {
    fn cell(&mut self, decision: cqlite_core::storage::write_engine::merge::trace::CellDecision) {
        if self.reserve(&decision) {
            cqlite_core::storage::write_engine::merge::trace::TraceSink::cell(
                &mut self.inner,
                decision,
            );
        }
    }

    fn tombstone(
        &mut self,
        tombstone: cqlite_core::storage::write_engine::merge::trace::TombstoneRecord,
    ) {
        if self.reserve(&tombstone) {
            cqlite_core::storage::write_engine::merge::trace::TraceSink::tombstone(
                &mut self.inner,
                tombstone,
            );
        }
    }

    fn generation_probe(
        &mut self,
        run_index: usize,
        outcome: cqlite_core::storage::write_engine::merge::trace::ProbeOutcome,
    ) {
        cqlite_core::storage::write_engine::merge::trace::TraceSink::generation_probe(
            &mut self.inner,
            run_index,
            outcome,
        );
    }
}
