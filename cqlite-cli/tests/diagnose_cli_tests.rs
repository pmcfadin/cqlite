//! Issue #4204 (spec R6, R7) — `cqlite diagnose` CLI surface, driven through the REAL compiled
//! binary (`CARGO_BIN_EXE_cqlite`): the verb owns its exit codes via `std::process::exit`, which
//! only a subprocess observes, and R7.1 is defined against the binary's actual output.
//!
//! Fixtures are Cassandra-written (datasets-v4) and resolved PER TABLE; an absent table is a
//! failure under `CQLITE_REQUIRE_FIXTURES=1` and a loud skip otherwise (never a silent pass).

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;

fn strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// `<root>/sstables/<keyspace>/<table>-<id>/` holding at least one `*-Data.db`.
fn table_dir(keyspace: &str, table: &str) -> Option<PathBuf> {
    let mut roots = Vec::new();
    if let Ok(r) = std::env::var("CQLITE_DATASETS_ROOT") {
        roots.push(PathBuf::from(r));
    }
    roots.push(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("repo")
            .join("test-data/datasets"),
    );
    for root in roots {
        let Ok(rd) = std::fs::read_dir(root.join("sstables").join(keyspace)) else {
            continue;
        };
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            let is_table = name
                .rsplit_once('-')
                .is_some_and(|(t, id)| t == table && id.len() == 32);
            if is_table && data_dbs(&e.path()) > 0 {
                return Some(e.path());
            }
        }
    }
    if strict() {
        panic!("{keyspace}.{table}: fixture missing under CQLITE_REQUIRE_FIXTURES");
    }
    eprintln!("SKIP {keyspace}.{table}: fixture not present");
    None
}

fn data_dbs(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .filter(|e| e.file_name().to_string_lossy().ends_with("-Data.db"))
                .count()
        })
        .unwrap_or(0)
}

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args(args)
        .output()
        .expect("spawn cqlite")
}

/// R1, one shape everywhere: any object carrying a `source` is the envelope `{value, source,
/// cause?}` — measured or unmeasured, no leaf-specific variants.
fn assert_envelopes(v: &Value, path: &str) {
    match v {
        Value::Object(m) => {
            // The generation object carries its own D5 `source` (a label, not a leaf).
            if m.contains_key("source") && !m.contains_key("generation") {
                assert_sourced(v, path);
            }
            for (k, c) in m {
                assert_envelopes(c, &format!("{path}.{k}"));
            }
        }
        Value::Array(a) => a
            .iter()
            .enumerate()
            .for_each(|(i, c)| assert_envelopes(c, &format!("{path}[{i}]"))),
        _ => {}
    }
}

fn run_json(dir: &Path, extra: &[&str]) -> Value {
    let mut args = vec![
        "diagnose",
        dir.to_str().expect("utf8"),
        "--now",
        "1794545000",
        "--out",
        "json",
    ];
    args.extend_from_slice(extra);
    let out = run(&args);
    assert_eq!(
        out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: Value = serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "stdout is not one JSON document: {e}\n{}",
            String::from_utf8_lossy(&out.stdout)
        )
    });
    assert_envelopes(&v, "$");
    v
}

fn keys(v: &Value) -> Vec<&str> {
    v.as_object()
        .expect("object")
        .keys()
        .map(String::as_str)
        .collect()
}

fn listing(dir: &Path) -> Vec<(String, u64)> {
    let mut v: Vec<_> = std::fs::read_dir(dir)
        .expect("dir")
        .flatten()
        .map(|e| {
            (
                e.file_name().to_string_lossy().into_owned(),
                e.metadata().expect("meta").len(),
            )
        })
        .collect();
    v.sort();
    v
}

fn copy_dir(src: &Path) -> tempfile::TempDir {
    let t = tempfile::tempdir().expect("tempdir");
    for e in std::fs::read_dir(src).expect("dir").flatten() {
        if e.path().is_file() {
            std::fs::copy(e.path(), t.path().join(e.file_name())).expect("copy");
        }
    }
    t
}

/// A leaf is `{value, source}` with source in the closed set, or `{value:null, source:
/// "unmeasured", cause}` — never bare.
fn assert_sourced(leaf: &Value, what: &str) {
    let src = leaf["source"]
        .as_str()
        .unwrap_or_else(|| panic!("{what}: no source in {leaf}"));
    assert!(
        ["statistics", "index", "scan", "unmeasured"].contains(&src),
        "{what}: source {src}"
    );
    assert!(
        leaf.get("value").is_some(),
        "{what}: no value key in {leaf}"
    );
    if src == "unmeasured" {
        assert!(
            leaf["value"].is_null() && leaf["cause"].as_str().is_some_and(|c| !c.is_empty()),
            "{what}: {leaf}"
        );
    }
}

/// R6.1 — default run over a healthy multi-generation table.
#[test]
fn cheap_tier_default_run_reports_every_generation_with_sources() {
    let Some(dir) = table_dir("test_tomb", "skipped_partition_delete") else {
        return;
    };
    let want = data_dbs(&dir);
    assert!(
        want >= 2,
        "precondition: a multi-generation table, got {want}"
    );
    let r = run_json(&dir, &[]);
    let gens = r["generations"].as_array().expect("generations");
    assert_eq!(gens.len(), want, "one entry per input generation");
    for g in gens {
        for f in [
            "max_timestamp",
            "estimated_partition_count",
            "estimated_droppable_tombstone_ratio",
            "pending_repair",
            "compression_ratio",
        ] {
            assert_sourced(&g[f], &format!("gen {} {f}", g["generation"]));
        }
        assert!(
            g["deep"].is_null(),
            "cheap tier must not carry a deep report"
        );
        assert!(
            g["min_timestamp"].is_i64()
                && g["repaired_at"].is_i64()
                && g["fully_expired_at_now"].is_boolean()
        );
    }
    assert_eq!(r["deep"], false);
    assert!(r["unreadable_generations"]
        .as_array()
        .expect("array")
        .is_empty());
}

/// R6.2 — `--deep --top 5` over `test_wide_rows`.
#[test]
fn deep_top_n_populates_histograms_and_bounded_rankings() {
    let Some(dir) = table_dir("test_wide_rows", "chat_messages") else {
        return;
    };
    let r = run_json(&dir, &["--deep", "--top", "5"]);
    let gens = r["generations"].as_array().expect("generations");
    assert_eq!(gens.len(), data_dbs(&dir));
    for g in gens {
        let d = &g["deep"];
        assert!(d.is_object(), "deep must be populated: {g}");
        assert!(!d["partition_size_histogram"]
            .as_array()
            .expect("hist")
            .is_empty());
        assert!(!d["clustering_width_histogram"]
            .as_array()
            .expect("hist")
            .is_empty());
        let largest = d["top_largest_partitions"].as_array().expect("largest");
        assert_eq!(largest.len(), 5, "50 partitions, top 5 → exactly 5");
        assert!(
            d["top_tombstone_heaviest_partitions"]
                .as_array()
                .expect("tomb")
                .len()
                <= 5
        );
        assert_sourced(&d["reclaim_at_now"], "reclaim_at_now (no --schema)");
        assert_eq!(
            d["reclaim_at_now"]["source"], "unmeasured",
            "reclaim without a schema is unmeasured, never 0"
        );
    }
}

/// R6.3 — usage errors exit 1 with the cause on stderr and write nothing.
#[test]
fn usage_errors_exit_one_with_cause_and_write_nothing() {
    let missing = run(&["diagnose", "/nonexistent/cqlite-4204/table"]);
    assert_eq!(missing.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&missing.stderr).contains("does not exist"),
        "{:?}",
        missing.stderr
    );
    assert!(missing.stdout.is_empty());

    let empty = tempfile::tempdir().expect("tempdir");
    let out = run(&["diagnose", empty.path().to_str().expect("utf8")]);
    assert_eq!(out.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("no SSTable generations"),
        "{:?}",
        out.stderr
    );
    assert_eq!(
        std::fs::read_dir(empty.path()).expect("dir").count(),
        0,
        "nothing written"
    );

    let Some(dir) = table_dir("test_basic", "simple_table") else {
        return;
    };
    let bad = run(&[
        "diagnose",
        dir.to_str().expect("utf8"),
        "--now",
        "yesterday",
    ]);
    assert_eq!(bad.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&bad.stderr).contains("--now"));
}

/// R6.4 — `--out` is a render format, never a destination.
#[test]
fn out_flag_is_a_render_format_not_a_destination() {
    let Some(src) = table_dir("test_basic", "simple_table") else {
        return;
    };
    let work = copy_dir(&src);
    let before = listing(work.path());
    let out = run(&[
        "diagnose",
        work.path().to_str().expect("utf8"),
        "--out",
        "json",
    ]);
    assert_eq!(out.status.code(), Some(0));
    serde_json::from_slice::<Value>(&out.stdout).expect("the JSON report is on stdout");
    assert_eq!(
        listing(work.path()),
        before,
        "no file created or modified under the table dir"
    );

    // A path in `--out` is not a destination: the flag only accepts a format name.
    let dest = tempfile::tempdir().expect("tempdir");
    let target = dest.path().join("report.json");
    let bad = run(&[
        "diagnose",
        work.path().to_str().expect("utf8"),
        "--out",
        target.to_str().expect("utf8"),
    ]);
    assert_ne!(bad.status.code(), Some(0), "--out <path> must be rejected");
    assert!(
        !target.exists(),
        "no file may be written to an --out-adjacent path"
    );
    assert_eq!(listing(work.path()), before);

    let help = String::from_utf8_lossy(&run(&["diagnose", "--help"]).stdout).to_lowercase();
    assert!(
        help.contains("never writes a file"),
        "help must say --out never writes a file: {help}"
    );
    assert!(
        help.contains("modifies nothing"),
        "help must say the verb modifies nothing: {help}"
    );
}

/// R7.1 — field order against the compiled binary's actual output.
#[test]
fn report_schema_matches_design() {
    let Some(dir) = table_dir("test_wide_rows", "chat_messages") else {
        return;
    };
    let r = run_json(&dir, &["--deep", "--top", "3"]);
    // design.md D5 order; `unreadable_generations` (R6) is appended last.
    assert_eq!(
        keys(&r),
        [
            "table_dir",
            "now",
            "deep",
            "top_n",
            "generations",
            "token_overlap",
            "now_used_for_gc_before",
            "unreadable_generations"
        ]
    );
    assert_eq!(keys(&r["token_overlap"]), ["buckets", "counts"]);
    assert_eq!(r["token_overlap"]["buckets"], 16);
    assert_sourced(&r["token_overlap"]["counts"], "token_overlap.counts");
    assert_eq!(
        r["token_overlap"]["counts"]["value"]
            .as_array()
            .expect("counts value")
            .len(),
        16
    );
    assert_eq!(r["now"], "2026-11-13T04:43:20Z");
    let g = &r["generations"][0];
    assert_eq!(
        keys(g),
        [
            "generation",
            "format",
            "source",
            "min_timestamp",
            "max_timestamp",
            "min_local_deletion_time",
            "max_local_deletion_time",
            "estimated_partition_count",
            "estimated_droppable_tombstone_ratio",
            "repaired_at",
            "pending_repair",
            "compression_ratio",
            "fully_expired_at_now",
            "deep"
        ]
    );
    assert_eq!(keys(&g["max_timestamp"])[..2], ["value", "source"]);
    assert_eq!(
        keys(&g["estimated_droppable_tombstone_ratio"])[..3],
        ["value", "source", "gc_before"][..]
    );
    assert_eq!(
        keys(&g["deep"]),
        [
            "partition_size_histogram",
            "clustering_width_histogram",
            "top_largest_partitions",
            "top_tombstone_heaviest_partitions",
            "reclaim_at_now"
        ]
    );
    let first = &g["deep"]["top_largest_partitions"][0];
    assert_eq!(keys(first), ["key_hex", "bytes"]);
    assert_eq!(
        g["deep"]["partition_size_histogram"][0]
            .as_array()
            .expect("triple")
            .len(),
        3
    );
}

/// R5.1 through the binary: the cheap tier succeeds when every Data.db is empty.
#[test]
fn cheap_tier_through_the_binary_never_needs_data_db() {
    let Some(src) = table_dir("test_basic", "simple_table") else {
        return;
    };
    let real = run_json(&src, &[]);
    let hollow = copy_dir(&src);
    for e in std::fs::read_dir(hollow.path()).expect("dir").flatten() {
        if e.file_name().to_string_lossy().ends_with("-Data.db") {
            std::fs::write(e.path(), b"").expect("empty");
        }
    }
    let h = run_json(hollow.path(), &[]);
    assert_eq!(
        real["generations"], h["generations"],
        "cheap-tier report must not depend on Data.db content"
    );
}

/// Text rendering is a rendering of the same report (every cheap field present, sources shown).
#[test]
fn text_rendering_carries_the_same_facts() {
    let Some(dir) = table_dir("test_tomb", "skipped_partition_delete") else {
        return;
    };
    let out = run(&[
        "diagnose",
        dir.to_str().expect("utf8"),
        "--now",
        "1794545000",
    ]);
    assert_eq!(out.status.code(), Some(0));
    let t = String::from_utf8_lossy(&out.stdout);
    for needle in [
        "generation 1",
        "generation 2",
        "max timestamp:",
        "estimated droppable tombstones:",
        "fully expired at now:",
        "token overlap (16 buckets",
    ] {
        assert!(t.contains(needle), "text lacks {needle:?}:\n{t}");
    }
    assert!(t.contains("[statistics]"));
}

/// R7.2 — text names `cqlite tombstones` where diagnose stops short.
///
/// TODO(#4200, owner ruling pending): #4200 moved to 0.20, so the pointer text is deliberately
/// not emitted yet. When the owner rules, delete the `#[ignore]` (and add the pointer to
/// `commands/diagnose.rs`'s two marked sites); this test is the one-line flip.
#[test]
#[ignore = "R7.2 pending owner ruling on #4200 (cqlite tombstones moved to 0.20)"]
fn text_names_cqlite_tombstones_where_diagnose_stops_short() {
    let Some(dir) = table_dir("test_tomb", "skipped_partition_delete") else {
        return;
    };
    let out = run(&["diagnose", dir.to_str().expect("utf8"), "--deep"]);
    let t = String::from_utf8_lossy(&out.stdout);
    assert!(t.contains("cqlite tombstones"), "{t}");
}

/// Measured reclaim uses the SAME envelope as every other leaf, and states it is an upper bound.
#[test]
fn measured_reclaim_is_enveloped_and_labelled_an_upper_bound() {
    let Some(dir) = table_dir("test_wide_rows", "chat_messages") else {
        return;
    };
    let cql = std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("repo")
            .join("test-data/schemas/wide-rows.cql"),
    )
    .expect("schema");
    let start = cql
        .find("CREATE TABLE IF NOT EXISTS chat_messages")
        .expect("table");
    let end = start + cql[start..].find(';').expect("terminator") + 1;
    let tmp = tempfile::tempdir().expect("tempdir");
    let schema = tmp.path().join("chat.cql");
    std::fs::write(&schema, &cql[start..end]).expect("write");
    let out = run(&[
        "--schema",
        schema.to_str().expect("utf8"),
        "diagnose",
        dir.to_str().expect("utf8"),
        "--now",
        "1794545000",
        "--deep",
        "--out",
        "json",
    ]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let r: Value = serde_json::from_slice(&out.stdout).expect("json");
    assert_envelopes(&r, "$");
    let rc = &r["generations"][0]["deep"]["reclaim_at_now"];
    assert_eq!(rc["source"], "scan", "{rc}");
    assert_eq!(rc["value"]["rows_in"], 50);
    assert_eq!(rc["value"]["rows_out"], 50);
    assert_eq!(rc["bound"], "upper");
    assert!(rc["bound_reason"]
        .as_str()
        .is_some_and(|b| b.contains("overlap gate")));
    assert_eq!(rc["value"]["tombstones_purged"]["source"], "unmeasured");
    let text = run(&[
        "--schema",
        schema.to_str().expect("utf8"),
        "diagnose",
        dir.to_str().expect("utf8"),
        "--deep",
    ]);
    assert!(
        String::from_utf8_lossy(&text.stdout).contains("UPPER BOUND"),
        "text must label the bound"
    );
}
