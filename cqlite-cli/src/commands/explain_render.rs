//! Stable renderers for the explain decision report.

#[cfg(feature = "write-support")]
use super::explain::ExplainNow;
#[cfg(feature = "write-support")]
use crate::cli::OutputFormat;

#[cfg(feature = "write-support")]
#[derive(Debug, serde::Serialize)]
struct ExplainGeneration {
    run_index: usize,
    sstable: String,
    probe: String,
}

#[cfg(feature = "write-support")]
#[derive(Debug, serde::Serialize)]
struct ExplainCell {
    column: String,
    clustering: Option<cqlite_core::storage::write_engine::ClusteringKey>,
    generation: usize,
    writetime: i64,
    ttl: Option<i32>,
    expires_at: Option<i64>,
    value: Option<cqlite_core::Value>,
    verdict: String,
    decided_by: serde_json::Value,
}

#[cfg(feature = "write-support")]
#[derive(Debug, serde::Serialize)]
struct ExplainTombstone {
    kind: String,
    generation: usize,
    deletion_time: i64,
    local_deletion_time: i32,
    range_start: Option<cqlite_core::storage::write_engine::ClusteringBound>,
    range_end: Option<cqlite_core::storage::write_engine::ClusteringBound>,
    droppable_at_now: bool,
}

#[cfg(feature = "write-support")]
#[derive(Debug, serde::Serialize)]
pub(super) struct ExplainReport {
    now: i64,
    generations: Vec<ExplainGeneration>,
    cells: Vec<ExplainCell>,
    tombstones: Vec<ExplainTombstone>,
}

#[cfg(feature = "write-support")]
impl ExplainReport {
    pub(super) fn from_trace(
        now: i64,
        paths: Vec<std::path::PathBuf>,
        probes: Vec<(
            usize,
            cqlite_core::storage::write_engine::merge::trace::ProbeOutcome,
        )>,
        cells: Vec<cqlite_core::storage::write_engine::merge::trace::CellDecision>,
        tombstones: Vec<cqlite_core::storage::write_engine::merge::trace::TombstoneRecord>,
        clustering_filter: Option<&cqlite_core::storage::write_engine::ClusteringKey>,
    ) -> Self {
        let mut probe_by_run = std::collections::HashMap::new();
        for (run_index, probe) in probes {
            probe_by_run.insert(run_index, probe_name(probe));
        }
        let generations = paths
            .iter()
            .enumerate()
            .map(|(run_index, path)| ExplainGeneration {
                run_index,
                sstable: path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default()
                    .to_string(),
                probe: probe_by_run
                    .get(&run_index)
                    .cloned()
                    .unwrap_or_else(|| "absent".to_string()),
            })
            .collect();
        let cells = cells
            .into_iter()
            .filter(|cell| {
                clustering_filter.is_none() || cell.clustering.as_ref() == clustering_filter
            })
            .map(|cell| ExplainCell {
                column: cell.column,
                clustering: cell.clustering,
                generation: cell.run_index,
                writetime: cell.writetime,
                ttl: cell.ttl,
                expires_at: cell.expires_at,
                value: cell.value,
                verdict: verdict_name(&cell.verdict),
                decided_by: decided_by_json(&cell.decided_by),
            })
            .collect();
        let tombstones = tombstones
            .into_iter()
            .map(|tombstone| ExplainTombstone {
                kind: tombstone_kind_name(tombstone.kind),
                generation: tombstone.run_index,
                deletion_time: tombstone.deletion_time,
                local_deletion_time: tombstone.local_deletion_time,
                range_start: tombstone.range_start,
                range_end: tombstone.range_end,
                droppable_at_now: tombstone.droppable_at_now,
            })
            .collect();
        Self {
            now,
            generations,
            cells,
            tombstones,
        }
    }
}

#[cfg(feature = "write-support")]
fn probe_name(probe: cqlite_core::storage::write_engine::merge::trace::ProbeOutcome) -> String {
    match probe {
        cqlite_core::storage::write_engine::merge::trace::ProbeOutcome::Hit => "hit",
        cqlite_core::storage::write_engine::merge::trace::ProbeOutcome::Absent => "absent",
        cqlite_core::storage::write_engine::merge::trace::ProbeOutcome::Scanned => "scanned",
    }
    .to_string()
}

#[cfg(feature = "write-support")]
fn verdict_name(verdict: &cqlite_core::storage::write_engine::merge::trace::Verdict) -> String {
    use cqlite_core::storage::write_engine::merge::trace::{TombstoneKind, Verdict};
    match verdict {
        Verdict::Winner => "winner".to_string(),
        Verdict::ShadowedByTimestamp => "shadowed-by-timestamp".to_string(),
        Verdict::ShadowedByTombstone(kind) => {
            format!("shadowed-by-tombstone:{}", tombstone_kind_name(*kind))
        }
        Verdict::Expired => "expired".to_string(),
        Verdict::Purgeable => "purgeable".to_string(),
        Verdict::DroppedColumn => "dropped-column".to_string(),
    }
}

#[cfg(feature = "write-support")]
fn decided_by_json(
    decided_by: &cqlite_core::storage::write_engine::merge::trace::DecidedBy,
) -> serde_json::Value {
    use cqlite_core::storage::write_engine::merge::trace::DecidedBy;
    match decided_by {
        DecidedBy::Winner {
            run_index,
            writetime,
        } => serde_json::json!({
            "decision": "winner",
            "run_index": run_index,
            "writetime": writetime,
        }),
        DecidedBy::Tombstone {
            kind,
            run_index,
            deletion_time,
            local_deletion_time,
            droppable_at_now,
        } => serde_json::json!({
            "decision": "tombstone",
            "kind": tombstone_kind_name(*kind),
            "run_index": run_index,
            "deletion_time": deletion_time,
            "local_deletion_time": local_deletion_time,
            "droppable_at_now": droppable_at_now,
        }),
        DecidedBy::DropTime(drop_time) => serde_json::json!({
            "decision": "drop_time",
            "drop_time": drop_time,
        }),
        DecidedBy::Expiry { expires_at, now } => serde_json::json!({
            "decision": "expiry",
            "expires_at": expires_at,
            "now": now,
        }),
        DecidedBy::GcGrace {
            ldt,
            gc_before,
            now,
        } => serde_json::json!({
            "decision": "gc_grace",
            "ldt": ldt,
            "gc_before": gc_before,
            "now": now,
        }),
        DecidedBy::None => serde_json::json!({ "decision": "none" }),
    }
}

#[cfg(feature = "write-support")]
fn tombstone_kind_name(
    kind: cqlite_core::storage::write_engine::merge::trace::TombstoneKind,
) -> String {
    use cqlite_core::storage::write_engine::merge::trace::TombstoneKind;
    match kind {
        TombstoneKind::Partition => "partition",
        TombstoneKind::Range => "range",
        TombstoneKind::Row => "row",
        TombstoneKind::Cell => "cell",
        TombstoneKind::Collection => "collection",
    }
    .to_string()
}

#[cfg(feature = "write-support")]
pub(super) fn render_report(
    report: &ExplainReport,
    format: OutputFormat,
    now: &ExplainNow,
    gc_grace: i64,
) -> std::result::Result<String, String> {
    let metadata = metadata_line(report, now, gc_grace);
    match format {
        OutputFormat::Table => Ok(render_table(report, &metadata)),
        OutputFormat::Json => {
            let body = serde_json::to_string(report).map_err(|error| error.to_string())?;
            Ok(format!("{metadata}\n{body}\n"))
        }
        OutputFormat::Csv => render_csv(report, &metadata),
        OutputFormat::Parquet => Err("explain supports only table, json, and csv output".into()),
    }
}

#[cfg(feature = "write-support")]
fn metadata_line(report: &ExplainReport, now: &ExplainNow, gc_grace: i64) -> String {
    let rendered_time = chrono::DateTime::<chrono::Utc>::from_timestamp(report.now, 0)
        .map(|date| date.to_rfc3339())
        .unwrap_or_else(|| report.now.to_string());
    let clock_suffix = if now.wall_clock { " (wall clock)" } else { "" };
    let names = report
        .generations
        .iter()
        .map(|generation| generation.sstable.as_str())
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "now={} ({rendered_time}){clock_suffix}  generations={} [{}]  gc_grace_seconds={gc_grace}",
        report.now,
        report.generations.len(),
        names
    )
}

#[cfg(feature = "write-support")]
fn render_table(report: &ExplainReport, metadata: &str) -> String {
    let mut rendered = String::with_capacity(metadata.len() + 256);
    rendered.push_str(metadata);
    rendered.push('\n');
    if report.generations.iter().all(|generation| {
        generation.probe == "absent" || generation.probe == "scanned"
    }) && report.cells.is_empty()
    {
        rendered.push_str("0 generations hold this key\n");
    }
    for cell in &report.cells {
        let sstable = report
            .generations
            .get(cell.generation)
            .map(|generation| generation.sstable.as_str())
            .unwrap_or("<unknown>");
        let decided_by = serde_json::to_string(&cell.decided_by).unwrap_or_else(|_| "null".into());
        rendered.push_str(&format!(
            "{}  {}  writetime={}  {}  {}\n",
            cell.column, sstable, cell.writetime, cell.verdict, decided_by
        ));
    }
    for tombstone in &report.tombstones {
        let sstable = report
            .generations
            .get(tombstone.generation)
            .map(|generation| generation.sstable.as_str())
            .unwrap_or("<unknown>");
        rendered.push_str(&format!(
            "tombstone:{}  {}  deletion_time={}  local_deletion_time={}  droppable_at_now={}\n",
            tombstone.kind,
            sstable,
            tombstone.deletion_time,
            tombstone.local_deletion_time,
            tombstone.droppable_at_now
        ));
    }
    rendered
}

#[cfg(feature = "write-support")]
fn render_csv(report: &ExplainReport, metadata: &str) -> std::result::Result<String, String> {
    let mut writer = csv::Writer::from_writer(Vec::new());
    writer
        .write_record([
            "column",
            "clustering",
            "generation",
            "writetime",
            "ttl",
            "expires_at",
            "value",
            "verdict",
            "decided_by",
            "kind",
            "deletion_time",
            "local_deletion_time",
            "range_start",
            "range_end",
            "droppable_at_now",
        ])
        .map_err(|error| error.to_string())?;
    for cell in &report.cells {
        writer
            .write_record([
                cell.column.as_str(),
                &json_or_empty(&cell.clustering),
                &cell.generation.to_string(),
                &cell.writetime.to_string(),
                &optional_to_string(cell.ttl),
                &optional_to_string(cell.expires_at),
                &json_or_empty(&cell.value),
                cell.verdict.as_str(),
                &json_or_empty(&cell.decided_by),
                "",
                "",
                "",
                "",
                "",
                "",
            ])
            .map_err(|error| error.to_string())?;
    }
    for tombstone in &report.tombstones {
        writer
            .write_record([
                "",
                "",
                &tombstone.generation.to_string(),
                "",
                "",
                "",
                "",
                "",
                "",
                &format!("tombstone:{}", tombstone.kind),
                &tombstone.deletion_time.to_string(),
                &tombstone.local_deletion_time.to_string(),
                &json_or_empty(&tombstone.range_start),
                &json_or_empty(&tombstone.range_end),
                &tombstone.droppable_at_now.to_string(),
            ])
            .map_err(|error| error.to_string())?;
    }
    let body = writer
        .into_inner()
        .map_err(|error| error.into_error().to_string())?;
    let body = String::from_utf8(body).map_err(|error| error.to_string())?;
    Ok(format!("{metadata}\n{body}"))
}

#[cfg(feature = "write-support")]
fn json_or_empty<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

#[cfg(feature = "write-support")]
fn optional_to_string<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}
