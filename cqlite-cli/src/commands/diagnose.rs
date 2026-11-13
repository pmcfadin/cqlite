//! `cqlite diagnose` — read-only SSTable health report (issue #4204, `openspec/changes/
//! sstable-diagnose`). Thin wrapper over [`cqlite_core::storage::sstable::diagnose::diagnose_table`].
//!
//! * **Exit codes (R6):** `0` for any successful report (a generation this run cannot read is a
//!   field of the report, `unreadable_generations`, never a refusal); `1` for usage errors
//!   (missing/non-directory path, no generations, bad `--now`, unreadable `--schema`).
//! * **`--out` is a rendering, never a destination (R6.4):** the report goes to STDOUT; this verb
//!   creates, modifies and deletes nothing.
//! * **JSON (R7.1, design D5):** field order is built here, explicitly, in D5's order
//!   (`serde_json` with `preserve_order`), then pinned against the compiled binary by
//!   `cqlite-cli/tests/diagnose_cli_tests.rs`.

use std::path::Path;

use cqlite_core::storage::sstable::diagnose::reclaim_prediction::UPPER_BOUND_REASON;
use cqlite_core::storage::sstable::diagnose::{
    diagnose_table, DeepGenerationReport, DiagnoseOptions, DiagnoseReport, FieldSource,
    GenerationDiagnosis, SourcedField,
};
use serde_json::{json, Map, Value};

use super::diagnose_args::DiagnoseArgs;
use super::schema_load::load_schema_file_with_status;
use crate::cli_types::VerifyOutputArg;

fn usage_error(msg: impl std::fmt::Display) -> ! {
    eprintln!("cqlite diagnose: {msg}");
    std::process::exit(1);
}

/// Parse `--now`: epoch seconds, or an RFC 3339 timestamp. `None` → the wall clock, sampled once.
fn resolve_now(raw: Option<&str>) -> Result<i64, String> {
    match raw {
        None => Ok(chrono::Utc::now().timestamp()),
        Some(s) => s
            .parse::<i64>()
            .or_else(|_| chrono::DateTime::parse_from_rfc3339(s).map(|d| d.timestamp()))
            .map_err(|_| format!("--now {s:?} is neither epoch seconds nor an RFC 3339 timestamp")),
    }
}

fn rfc3339(secs: i64) -> Result<String, String> {
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .ok_or_else(|| format!("instant {secs} is outside the representable range"))
}

/// Entry point; owns the exit codes (never returns an `Err`).
pub async fn execute_diagnose_command(schema_path: Option<&Path>, args: &DiagnoseArgs) {
    if !args.table_dir.is_dir() {
        usage_error(format!(
            "{} does not exist or is not a directory",
            args.table_dir.display()
        ));
    }
    let now_secs = resolve_now(args.now.as_deref()).unwrap_or_else(|e| usage_error(e));
    // The schema supplies gc_grace_seconds and drives --deep's dry compaction; absent, the cheap
    // tier uses Cassandra's 10-day default and --deep reports reclaim as `unmeasured`.
    let schema = schema_path.map(|p| {
        load_schema_file_with_status(p, false, None, false)
            .unwrap_or_else(|e| usage_error(format!("cannot load --schema {}: {e:#}", p.display())))
    });
    let gc_grace_seconds = schema
        .as_ref()
        .and_then(|s| s.comments.get("gc_grace_seconds"))
        .and_then(|v| v.trim().parse::<i64>().ok());
    let options = DiagnoseOptions {
        now_secs,
        deep: args.deep,
        top_n: args.top,
        gc_grace_seconds,
        schema,
    };
    let report = diagnose_table(&args.table_dir, &options)
        .await
        .unwrap_or_else(|e| usage_error(e));
    let rendered = match args.out {
        VerifyOutputArg::Json => render_json(&report),
        VerifyOutputArg::Text => render_text(&report),
    }
    .unwrap_or_else(|e| usage_error(e));
    println!("{rendered}");
    std::process::exit(0);
}

// ---------------------------------------------------------------- JSON (design D5 order)

/// `{"value": v, "source": s}` or `{"value": null, "source": "unmeasured", "cause": c}` — the
/// only shapes a possibly-absent leaf takes (R1: never a bare `0`/`null`).
fn sourced<T>(field: &SourcedField<T>, to_json: impl Fn(&T) -> Value) -> Map<String, Value> {
    let mut m = Map::new();
    match field {
        SourcedField::Measured { value, source } => {
            m.insert("value".into(), value.as_ref().map_or(Value::Null, to_json));
            m.insert("source".into(), json!(source.as_str()));
        }
        SourcedField::Unmeasured { cause } => {
            m.insert("value".into(), Value::Null);
            m.insert("source".into(), json!("unmeasured"));
            m.insert("cause".into(), json!(cause));
        }
    }
    m
}

fn hex(key: &[u8]) -> String {
    key.iter().map(|b| format!("{b:02x}")).collect()
}

/// `[lo, hi, count]` triples; the open top bucket's `hi` (`u64::MAX`) is `null` (unbounded).
fn histogram(h: &[(u64, u64, u64)]) -> Value {
    Value::Array(
        h.iter()
            .map(|(lo, hi, n)| {
                json!([
                    lo,
                    if *hi == u64::MAX {
                        Value::Null
                    } else {
                        json!(hi)
                    },
                    n
                ])
            })
            .collect(),
    )
}

fn ranked(
    list: &[cqlite_core::storage::sstable::diagnose::deep_scan::RankedPartition],
    metric: &str,
) -> Value {
    Value::Array(
        list.iter()
            .map(|p| {
                let mut m = Map::new();
                m.insert("key_hex".into(), json!(hex(&p.key)));
                m.insert(metric.into(), json!(p.metric));
                Value::Object(m)
            })
            .collect(),
    )
}

fn deep_json(d: &DeepGenerationReport) -> Value {
    let mut m = Map::new();
    m.insert(
        "partition_size_histogram".into(),
        histogram(&d.partition_size_histogram),
    );
    m.insert(
        "clustering_width_histogram".into(),
        histogram(&d.clustering_width_histogram),
    );
    m.insert(
        "top_largest_partitions".into(),
        ranked(&d.top_largest_partitions, "bytes"),
    );
    m.insert(
        "top_tombstone_heaviest_partitions".into(),
        ranked(&d.top_tombstone_heaviest_partitions, "tombstones"),
    );
    // ONE envelope `{value, source, cause?}` for measured and unmeasured alike (R1). A measured
    // prediction additionally carries `bound` + `bound_reason` (it is a per-generation estimate).
    let reclaim = {
        let mut env = sourced(&d.reclaim_at_now, |r| {
            let mut r_m = Map::new();
            r_m.insert("rows_in".into(), json!(r.rows_in));
            r_m.insert("rows_out".into(), json!(r.rows_out));
            // The dry merge reports rows, not a per-kind purge tally: stated, never a `0`.
            let mut purged = Map::new();
            purged.insert("value".into(), Value::Null);
            purged.insert("source".into(), json!("unmeasured"));
            purged.insert(
                "cause".into(),
                json!("the dry compaction counts rows in/out; it does not tally purged tombstones"),
            );
            r_m.insert("tombstones_purged".into(), Value::Object(purged));
            Value::Object(r_m)
        });
        if d.reclaim_at_now.is_measured() {
            env.insert("bound".into(), json!("upper"));
            env.insert("bound_reason".into(), json!(UPPER_BOUND_REASON));
        }
        Value::Object(env)
    };
    m.insert("reclaim_at_now".into(), reclaim);
    Value::Object(m)
}

fn generation_json(g: &GenerationDiagnosis) -> Value {
    let mut m = Map::new();
    // Classic integer ids are JSON numbers (D5); a UUID-based id is its exact on-disk string.
    m.insert(
        "generation".into(),
        g.generation
            .parse::<u64>()
            .map_or_else(|_| json!(g.generation), |n| json!(n)),
    );
    m.insert("format".into(), json!(g.format));
    m.insert("source".into(), json!(FieldSource::Statistics.as_str()));
    m.insert("min_timestamp".into(), json!(g.min_timestamp));
    m.insert(
        "max_timestamp".into(),
        Value::Object(sourced(&g.max_timestamp, |v| json!(v))),
    );
    m.insert(
        "min_local_deletion_time".into(),
        json!(g.min_local_deletion_time),
    );
    m.insert(
        "max_local_deletion_time".into(),
        json!(g.max_local_deletion_time),
    );
    m.insert(
        "estimated_partition_count".into(),
        Value::Object(sourced(&g.estimated_partition_count, |v| json!(v))),
    );
    let mut ratio = sourced(&g.estimated_droppable_tombstone_ratio, |v| json!(v));
    ratio.insert("gc_before".into(), json!(g.gc_before));
    m.insert(
        "estimated_droppable_tombstone_ratio".into(),
        Value::Object(ratio),
    );
    m.insert("repaired_at".into(), json!(g.repaired_at));
    m.insert(
        "pending_repair".into(),
        Value::Object(sourced(&g.pending_repair, |v| json!(v))),
    );
    m.insert(
        "compression_ratio".into(),
        Value::Object(sourced(&g.compression_ratio, |v| json!(v))),
    );
    m.insert("fully_expired_at_now".into(), json!(g.fully_expired_at_now));
    m.insert(
        "deep".into(),
        g.deep.as_ref().map_or(Value::Null, deep_json),
    );
    Value::Object(m)
}

pub(crate) fn render_json(r: &DiagnoseReport) -> Result<String, String> {
    let mut m = Map::new();
    m.insert("table_dir".into(), json!(r.table_dir.display().to_string()));
    m.insert("now".into(), json!(rfc3339(r.now_secs)?));
    m.insert("deep".into(), json!(r.deep));
    m.insert("top_n".into(), json!(r.top_n));
    m.insert(
        "generations".into(),
        Value::Array(r.generations.iter().map(generation_json).collect()),
    );
    let mut overlap = Map::new();
    overlap.insert("buckets".into(), json!(r.token_overlap.buckets));
    // Same `{value, source, cause?}` envelope as every other leaf; a partial histogram would
    // undercount, so unmeasured is the WHOLE value (cause names the generations).
    overlap.insert(
        "counts".into(),
        Value::Object(sourced(&r.token_overlap.counts, |c| json!(c))),
    );
    m.insert("token_overlap".into(), Value::Object(overlap));
    m.insert("now_used_for_gc_before".into(), json!(rfc3339(r.now_secs)?));
    // R6: not part of D5's list; appended LAST so D5's order is a prefix of the report.
    m.insert(
        "unreadable_generations".into(),
        Value::Array(
            r.unreadable_generations
                .iter()
                .map(
                    |u| json!({ "data_path": u.data_path.display().to_string(), "cause": u.cause }),
                )
                .collect(),
        ),
    );
    serde_json::to_string_pretty(&Value::Object(m)).map_err(|e| e.to_string())
}

// ---------------------------------------------------------------- text

fn text_sourced<T>(f: &SourcedField<T>, show: impl Fn(&T) -> String) -> String {
    match f {
        SourcedField::Measured {
            value: Some(v),
            source,
        } => format!("{} [{}]", show(v), source.as_str()),
        SourcedField::Measured {
            value: None,
            source,
        } => format!("none [{}]", source.as_str()),
        SourcedField::Unmeasured { cause } => format!("unmeasured: {cause}"),
    }
}

pub(crate) fn render_text(r: &DiagnoseReport) -> Result<String, String> {
    let mut o = String::new();
    o.push_str(&format!(
        "diagnose {}\n  now {} (gcBefore is evaluated at this instant); deep scan: {}\n",
        r.table_dir.display(),
        rfc3339(r.now_secs)?,
        if r.deep { "yes" } else { "no" }
    ));
    for g in &r.generations {
        o.push_str(&format!(
            "\ngeneration {} ({}) [statistics]\n",
            g.generation, g.format
        ));
        o.push_str(&format!("  min timestamp: {}\n", g.min_timestamp));
        o.push_str(&format!(
            "  max timestamp: {}\n",
            text_sourced(&g.max_timestamp, |v| v.to_string())
        ));
        o.push_str(&format!(
            "  local deletion time: min {} max {}\n",
            g.min_local_deletion_time, g.max_local_deletion_time
        ));
        o.push_str(&format!(
            "  estimated partitions: {}\n",
            text_sourced(&g.estimated_partition_count, |v| v.to_string())
        ));
        // TODO(#4200, owner ruling pending): R7.2 requires this line to name `cqlite tombstones`
        // as the authoritative, reconciled alternative. #4200 moved to 0.20, so the pointer text
        // is deliberately OMITTED until the owner rules; the description below only states what
        // the number is.
        o.push_str(&format!(
            "  estimated droppable tombstones: {} (a per-generation estimate from the Statistics.db \
             drop-time histogram at gcBefore {}; not reconciled across generations)\n",
            text_sourced(&g.estimated_droppable_tombstone_ratio, |v| format!("{v:.4}")),
            g.gc_before
        ));
        o.push_str(&format!("  repaired at: {}\n", g.repaired_at));
        o.push_str(&format!(
            "  pending repair: {}\n",
            text_sourced(&g.pending_repair, |v| v.clone())
        ));
        o.push_str(&format!(
            "  compression ratio: {}\n",
            text_sourced(&g.compression_ratio, |v| format!("{v:.4}"))
        ));
        o.push_str(&format!(
            "  fully expired at now: {}\n",
            g.fully_expired_at_now
        ));
        if let Some(d) = &g.deep {
            o.push_str("  deep scan [scan]:\n");
            o.push_str(&text_histogram(
                "partition size (bytes)",
                &d.partition_size_histogram,
            ));
            o.push_str(&text_histogram(
                "clustering width (rows)",
                &d.clustering_width_histogram,
            ));
            o.push_str(&text_ranking(
                "largest partitions (bytes)",
                &d.top_largest_partitions,
            ));
            // TODO(#4200, owner ruling pending): R7.2 — same omission as above; this ranking is
            // the RAW per-partition tombstone count (design D2), not a reconciled answer.
            o.push_str(&text_ranking(
                "tombstone-heaviest partitions (raw tombstone count, unreconciled)",
                &d.top_tombstone_heaviest_partitions,
            ));
            match &d.reclaim_at_now {
                SourcedField::Measured { value: Some(rc), .. } => o.push_str(&format!(
                    "    reclaim at now: {} rows in -> {} rows out (dry compaction; UPPER BOUND on reclaim: {}; purged-tombstone count unmeasured)\n",
                    rc.rows_in, rc.rows_out, UPPER_BOUND_REASON
                )),
                other => o.push_str(&format!("    reclaim at now: {}\n", text_sourced(other, |_| String::new()))),
            }
        }
    }
    o.push_str(&format!(
        "\ntoken overlap ({} buckets, generations per bucket): {}\n",
        r.token_overlap.buckets,
        text_sourced(&r.token_overlap.counts, |c| format!("{c:?}"))
    ));
    for u in &r.unreadable_generations {
        o.push_str(&format!(
            "unreadable generation {}: {}\n",
            u.data_path.display(),
            u.cause
        ));
    }
    Ok(o.trim_end().to_string())
}

fn text_histogram(title: &str, h: &[(u64, u64, u64)]) -> String {
    let mut s = format!("    {title}:\n");
    for (lo, hi, n) in h.iter().filter(|b| b.2 > 0) {
        let hi = if *hi == u64::MAX {
            "inf".to_string()
        } else {
            hi.to_string()
        };
        s.push_str(&format!("      [{lo}, {hi}) {n}\n"));
    }
    s
}

fn text_ranking(
    title: &str,
    l: &[cqlite_core::storage::sstable::diagnose::deep_scan::RankedPartition],
) -> String {
    let mut s = format!("    {title}:\n");
    if l.is_empty() {
        s.push_str("      (none)\n");
    }
    for p in l {
        s.push_str(&format!("      key {} -> {}\n", hex(&p.key), p.metric));
    }
    s
}
