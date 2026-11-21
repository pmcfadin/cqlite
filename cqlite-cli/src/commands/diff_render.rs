//! Stable renderers for the `cqlite diff` report (issue #4201; design.md §D4).

use cqlite_core::storage::write_engine::snapshot_diff::{
    CellDiffEntry, CellSide, DiffReport, TombstoneDiffEntry,
};

use super::explain::ExplainNow;

/// The first line of every output format: the clock, then each side's generations (oldest first).
fn first_line(report: &DiffReport, now: &ExplainNow) -> String {
    let rendered_time = chrono::DateTime::<chrono::Utc>::from_timestamp(now.epoch_secs, 0)
        .map(|date| date.to_rfc3339())
        .unwrap_or_else(|| now.epoch_secs.to_string());
    let clock_suffix = if now.wall_clock { " (wall clock)" } else { "" };
    let side = |label: &str, info: &cqlite_core::storage::write_engine::snapshot_diff::SideInfo| {
        format!(
            "side_{label}: generations={} [{}]",
            info.generations.len(),
            info.generations.join(", ")
        )
    };
    format!(
        "now={} ({rendered_time}){clock_suffix}  {}  {}  gc_grace_seconds={}",
        now.epoch_secs,
        side("a", &report.side_a),
        side("b", &report.side_b),
        report.gc_grace_seconds
    )
}

fn cell_side(side: &Option<CellSide>) -> String {
    match side {
        Some(cell) => format!("{}@{}", cell.value, cell.writetime),
        None => "-".to_string(),
    }
}

fn cell_line(partition: &str, cell: &CellDiffEntry) -> String {
    let status = serde_json::to_value(cell.status)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_default();
    format!(
        "cell  partition={partition}  clustering={}  column={}  {status}  a={}  b={}",
        cell.clustering.as_deref().unwrap_or("<static>"),
        cell.column,
        cell_side(&cell.a),
        cell_side(&cell.b)
    )
}

fn tombstone_line(partition: &str, tombstone: &TombstoneDiffEntry) -> String {
    let risk = &tombstone.resurrection_risk;
    let verdict = match risk.phase {
        Some(phase) => format!("resurrection_risk=yes ({phase})"),
        None => format!("resurrection_risk={}", risk.status),
    };
    let mut line = format!(
        "tombstone  partition={partition}  kind={}  only-{}",
        tombstone.kind, tombstone.side
    );
    if let Some(clustering) = &tombstone.clustering {
        line.push_str(&format!("  clustering={clustering}"));
    }
    if let Some(column) = &tombstone.column {
        line.push_str(&format!("  column={column}"));
    }
    if let (Some(start), Some(end)) = (&tombstone.range_start, &tombstone.range_end) {
        line.push_str(&format!("  range={start}..{end}"));
    }
    line.push_str(&format!(
        "  deletion_time={}  local_deletion_time={}  {verdict}",
        tombstone.deletion_time, tombstone.local_deletion_time
    ));
    if !risk.shadowed_cells.is_empty() {
        let cells = risk
            .shadowed_cells
            .iter()
            .map(|cell| {
                format!(
                    "{}:{}/{}",
                    cell.side,
                    cell.clustering.as_deref().unwrap_or("<static>"),
                    cell.column
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        line.push_str(&format!("  shadowed=[{cells}]"));
    }
    line
}

/// Render the report: the first line, then either one JSON object or one finding per line with the
/// summary last.
pub(super) fn render(
    report: &DiffReport,
    json: bool,
    now: &ExplainNow,
) -> std::result::Result<String, String> {
    let mut out = first_line(report, now);
    out.push('\n');
    if json {
        out.push_str(&serde_json::to_string(report).map_err(|error| error.to_string())?);
        out.push('\n');
        return Ok(out);
    }
    for partition in &report.partitions {
        for cell in &partition.cells {
            out.push_str(&cell_line(&partition.partition_key, cell));
            out.push('\n');
        }
        for tombstone in &partition.tombstones {
            out.push_str(&tombstone_line(&partition.partition_key, tombstone));
            out.push('\n');
        }
    }
    let s = &report.summary;
    out.push_str(&format!(
        "summary  partitions_compared={}  equal={}  only_a={}  only_b={}  newer_in_a={}  newer_in_b={}  tombstones_only_a={}  tombstones_only_b={}  resurrection_risk_before_gc_grace={}  resurrection_risk_past_gc_grace={}\n",
        s.partitions_compared,
        s.equal,
        s.only_a,
        s.only_b,
        s.newer_in_a,
        s.newer_in_b,
        s.tombstones_only_a,
        s.tombstones_only_b,
        s.resurrection_risk_before_gc_grace,
        s.resurrection_risk_past_gc_grace
    ));
    Ok(out)
}
