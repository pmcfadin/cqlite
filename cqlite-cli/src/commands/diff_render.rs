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
            info.generations
                .iter()
                .map(|name| field(name))
                .collect::<Vec<_>>()
                .join(", ")
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

/// One text field. A value made only of "plain" characters is emitted as is; anything else
/// (whitespace, control characters, quotes, backslashes, `=`, or empty) is double-quoted with
/// `\\`, `\"`, `\n`, `\r`, `\t` and `\u{..}` escapes, so a newline, a tab or the two-space
/// field separator inside DATA can neither split a finding across lines nor fuse two fields.
fn field(text: &str) -> String {
    let plain = !text.is_empty()
        && text
            .chars()
            .all(|c| !c.is_whitespace() && !c.is_control() && !matches!(c, '"' | '\\' | '='));
    if plain {
        return text.to_string();
    }
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{{{:x}}}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn cell_side(side: &Option<CellSide>) -> String {
    match side {
        Some(cell) => {
            let mut text = format!("{}@{}", field(&cell.value), cell.writetime);
            if let Some(ttl) = cell.ttl {
                text.push_str(&format!(";ttl={ttl}"));
            }
            if let Some(expires) = &cell.expires_at {
                text.push_str(&format!(";expires={expires}"));
            }
            if let Some(ldt) = &cell.local_deletion_time {
                text.push_str(&format!(";ldt={ldt}"));
            }
            text
        }
        None => "-".to_string(),
    }
}

fn cell_line(partition: &str, cell: &CellDiffEntry) -> String {
    let status = serde_json::to_value(cell.status)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_default();
    let path = cell
        .cell_path
        .as_ref()
        .map(|path| format!("  path={path}"))
        .unwrap_or_default();
    let resolves = cell
        .resolves_to
        .map(|side| format!("  resolves_to={side}"))
        .unwrap_or_default();
    format!(
        "cell  partition={}  clustering={}  column={}{path}  {status}{resolves}  a={}  b={}",
        field(partition),
        field(cell.clustering.as_deref().unwrap_or("<static>")),
        field(&cell.column),
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
        "tombstone  partition={}  kind={}  only-{}",
        field(partition),
        tombstone.kind,
        tombstone.side
    );
    if let Some(clustering) = &tombstone.clustering {
        line.push_str(&format!("  clustering={}", field(clustering)));
    }
    if let Some(column) = &tombstone.column {
        line.push_str(&format!("  column={}", field(column)));
    }
    if let Some(path) = &tombstone.cell_path {
        line.push_str(&format!("  path={path}"));
    }
    if let (Some(start), Some(end)) = (&tombstone.range_start, &tombstone.range_end) {
        line.push_str(&format!("  range={}..{}", field(start), field(end)));
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
                let path = cell
                    .cell_path
                    .as_deref()
                    .map(|p| format!("#{p}"))
                    .unwrap_or_default();
                format!(
                    "{}:{}/{}{path}",
                    cell.side,
                    field(cell.clustering.as_deref().unwrap_or("<static>")),
                    field(&cell.column)
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
        "summary  partitions_compared={}  equal={}  only_a={}  only_b={}  newer_in_a={}  newer_in_b={}  conflicts={}  tombstones_only_a={}  tombstones_only_b={}  resurrection_risk_before_gc_grace={}  resurrection_risk_past_gc_grace={}\n",
        s.partitions_compared,
        s.equal,
        s.only_a,
        s.only_b,
        s.newer_in_a,
        s.newer_in_b,
        s.conflicts,
        s.tombstones_only_a,
        s.tombstones_only_b,
        s.resurrection_risk_before_gc_grace,
        s.resurrection_risk_past_gc_grace
    ));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqlite_core::storage::write_engine::snapshot_diff::{
        CellStatus, DiffSummary, PartitionDiff, ResurrectionRisk, ShadowedCell, Side, SideInfo,
    };

    #[test]
    fn hostile_text_never_breaks_one_finding_per_line() {
        let hostile = "line1\nline2\tcol  b=c\r\u{7}\"q\\";
        let cell = CellDiffEntry {
            clustering: Some(hostile.to_string()),
            column: hostile.to_string(),
            cell_path: None,
            status: CellStatus::Conflict,
            resolves_to: Some(Side::A),
            a: Some(CellSide {
                value: hostile.to_string(),
                writetime: "t".into(),
                ttl: None,
                expires_at: None,
                local_deletion_time: None,
            }),
            b: None,
        };
        let tombstone = TombstoneDiffEntry {
            kind: "cell",
            clustering: Some(hostile.to_string()),
            column: Some(hostile.to_string()),
            cell_path: None,
            range_start: None,
            range_end: None,
            side: Side::A,
            deletion_time: "d".into(),
            local_deletion_time: "l".into(),
            resurrection_risk: ResurrectionRisk {
                status: "yes",
                phase: Some("past-gc-grace"),
                shadowed_cells: vec![ShadowedCell {
                    clustering: Some(hostile.to_string()),
                    column: hostile.to_string(),
                    cell_path: None,
                    side: Side::B,
                }],
            },
        };
        let report = DiffReport {
            now: "n".into(),
            table: "t".into(),
            gc_grace_seconds: 1,
            side_a: SideInfo {
                dir: "a".into(),
                generations: vec![],
            },
            side_b: SideInfo {
                dir: "b".into(),
                generations: vec![],
            },
            partitions: vec![PartitionDiff {
                partition_key: hostile.to_string(),
                cells: vec![cell],
                tombstones: vec![tombstone],
            }],
            summary: DiffSummary::default(),
        };
        let now = ExplainNow {
            epoch_secs: 1,
            wall_clock: false,
        };
        let text = render(&report, false, &now).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        // first line + one cell + one tombstone + summary: exactly four, whatever the data holds.
        assert_eq!(lines.len(), 4, "{text}");
        assert!(lines[1].starts_with("cell  partition=\"") && lines[2].starts_with("tombstone  "));
        // No raw control character or unquoted double space inside a data field survived.
        assert!(!lines[1].chars().any(|c| c.is_control()), "{}", lines[1]);
        assert!(
            lines[1].contains(r"line1\nline2\tcol  b=c\r\u{7}"),
            "{}",
            lines[1]
        );
    }

    #[test]
    fn hostile_generation_names_cannot_break_the_first_line() {
        let report = DiffReport {
            now: "n".into(),
            table: "t".into(),
            gc_grace_seconds: 1,
            side_a: SideInfo {
                dir: "a".into(),
                generations: vec!["nb-1\nforged  line".into(), "ok-2".into()],
            },
            side_b: SideInfo {
                dir: "b".into(),
                generations: vec![],
            },
            partitions: vec![],
            summary: DiffSummary::default(),
        };
        let now = ExplainNow {
            epoch_secs: 1,
            wall_clock: false,
        };
        let text = render(&report, false, &now).unwrap();
        assert_eq!(text.lines().count(), 2, "first line + summary: {text}");
        assert!(
            text.lines()
                .next()
                .unwrap()
                .contains(r#"["nb-1\nforged  line", ok-2]"#),
            "{text}"
        );
    }

    #[test]
    fn plain_values_are_left_readable() {
        assert_eq!(field("extra_1"), "extra_1");
        assert_eq!(field(""), "\"\"");
        assert_eq!(field("a b"), "\"a b\"");
    }
}
