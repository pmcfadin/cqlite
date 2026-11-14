//! Text and JSON rendering for `cqlite find` (issue #4205, design D6).

use cqlite_core::storage::commitlog::Writetime;
use cqlite_core::storage::sstable::reader::{ByteLength, GenerationProbe, HitDetail, ScanOutcome};
use serde_json::{json, Value};

use super::find::{FindReport, GenOutcome, Unflushed};
use super::find_commitlog::CommitEntry;

fn len_text(l: &ByteLength) -> String {
    match l {
        ByteLength::Known(n) => n.to_string(),
        ByteLength::ToEof => "to-eof".to_string(),
        ByteLength::Unmeasured(c) => format!("unmeasured({c})"),
    }
}

fn offset_text(d: &HitDetail) -> String {
    d.data_offset.map_or_else(
        || "unmeasured(scan reported no boundary)".to_string(),
        |o| o.to_string(),
    )
}

fn hit_text(d: &HitDetail) -> String {
    format!(
        "offset={} len={} max_writetime={} partition_deletion={}",
        offset_text(d),
        len_text(&d.byte_length),
        d.max_writetime,
        if d.has_partition_deletion {
            "yes"
        } else {
            "no"
        }
    )
}

fn wt_text(w: &Writetime) -> String {
    match w {
        Writetime::Known(t) => t.to_string(),
        Writetime::Unmeasured(c) => format!("unmeasured({c})"),
    }
}

fn unflushed_text(u: &Unflushed) -> String {
    match u {
        Unflushed::Yes => "yes".into(),
        Unflushed::No => "no".into(),
        Unflushed::Unmeasured(c) => format!("unmeasured({c})"),
    }
}

/// Render the report as text, one line per generation / CommitLog entry.
pub fn render_text(r: &FindReport) -> String {
    let mut lines = Vec::new();
    if r.generations.is_empty() {
        lines.push("(no SSTable generation in the table directory)".to_string());
    }
    for g in &r.generations {
        let body = match &g.outcome {
            GenOutcome::Probe(GenerationProbe::Hit(d)) => format!("hit  {}", hit_text(d)),
            GenOutcome::Probe(GenerationProbe::BloomNegative) => "bloom-negative".into(),
            GenOutcome::Probe(GenerationProbe::IndexMiss) => "index-miss".into(),
            GenOutcome::Probe(GenerationProbe::Scanned(ScanOutcome::Hit(d))) => {
                format!("scanned  result=hit  {}", hit_text(d))
            }
            GenOutcome::Probe(GenerationProbe::Scanned(ScanOutcome::Miss)) => {
                "scanned  result=miss".into()
            }
            GenOutcome::Unreadable(c) => format!("UNREADABLE  {c}"),
        };
        lines.push(format!("{}: {body}", g.basename));
    }
    if let Some(entries) = &r.commitlog {
        lines.push("commitlog:".to_string());
        if entries.is_empty() {
            lines.push("  (no mutation touches this key in the scanned segments)".to_string());
        }
        for e in entries {
            lines.push(match e {
                CommitEntry::Mutation {
                    segment,
                    position,
                    writetime,
                    unflushed,
                } => format!(
                    "  {segment}@{position}: writetime={}  unflushed={}",
                    wt_text(writetime),
                    unflushed_text(unflushed)
                ),
                CommitEntry::SegmentFailure { segment, cause } => {
                    format!("  {segment}: UNREADABLE  {cause}")
                }
                CommitEntry::SegmentTruncated { segment } => {
                    format!("  {segment}: torn tail -- mutations in the torn bytes cannot be seen")
                }
            });
        }
    }
    lines.join("\n")
}

fn hit_json(d: &HitDetail) -> Value {
    json!({
        "data_offset": d.data_offset,
        "byte_length": match &d.byte_length {
            ByteLength::Known(n) => json!(n),
            ByteLength::ToEof => json!("to-eof"),
            ByteLength::Unmeasured(c) => json!({"unmeasured": c}),
        },
        "max_writetime": d.max_writetime,
        "partition_deletion": d.has_partition_deletion,
    })
}

fn wt_json(w: &Writetime) -> Value {
    match w {
        Writetime::Known(t) => json!(t),
        Writetime::Unmeasured(c) => json!({"unmeasured": c}),
    }
}

/// Render the report as JSON (design D6 shape).
pub fn render_json(r: &FindReport) -> String {
    let gens: Vec<Value> = r
        .generations
        .iter()
        .map(|g| match &g.outcome {
            GenOutcome::Probe(GenerationProbe::Hit(d)) => {
                let mut v = hit_json(d);
                v["basename"] = json!(g.basename);
                v["probe"] = json!("hit");
                v
            }
            GenOutcome::Probe(GenerationProbe::BloomNegative) => {
                json!({"basename": g.basename, "probe": "bloom-negative"})
            }
            GenOutcome::Probe(GenerationProbe::IndexMiss) => {
                json!({"basename": g.basename, "probe": "index-miss"})
            }
            GenOutcome::Probe(GenerationProbe::Scanned(ScanOutcome::Hit(d))) => {
                let mut v = hit_json(d);
                v["basename"] = json!(g.basename);
                v["probe"] = json!("scanned-hit");
                v
            }
            GenOutcome::Probe(GenerationProbe::Scanned(ScanOutcome::Miss)) => {
                json!({"basename": g.basename, "probe": "scanned-miss"})
            }
            GenOutcome::Unreadable(c) => {
                json!({"basename": g.basename, "probe": "unreadable", "cause": c})
            }
        })
        .collect();
    let commitlog = r.commitlog.as_ref().map(|entries| {
        entries
            .iter()
            .map(|e| match e {
                CommitEntry::Mutation {
                    segment,
                    position,
                    writetime,
                    unflushed,
                } => json!({
                    "segment": segment,
                    "position": position,
                    "writetime": wt_json(writetime),
                    "unflushed": match unflushed {
                        Unflushed::Yes => json!("yes"),
                        Unflushed::No => json!("no"),
                        Unflushed::Unmeasured(c) => json!({"unmeasured": c}),
                    },
                }),
                CommitEntry::SegmentFailure { segment, cause } => {
                    json!({"segment": segment, "unreadable": cause})
                }
                CommitEntry::SegmentTruncated { segment } => {
                    json!({"segment": segment, "truncated": true})
                }
            })
            .collect::<Vec<_>>()
    });
    json!({"generations": gens, "commitlog": commitlog}).to_string()
}
