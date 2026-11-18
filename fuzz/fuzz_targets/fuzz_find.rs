#![no_main]
//! Fuzz target: `cqlite find` (issue #4205) — the per-generation probe and the CommitLog scan
//! (issue #4502).
//!
//! Two modes, chosen by the control byte (see `support`):
//! * `bit7 = 0` — SSTable probe: ONE component of a real generation (`bit6` picks BIG
//!   `test_comp/lz4_table` or BTI `test_da/wide_table`) is replaced by the fuzz bytes and
//!   `probe_generations` is run for a key the clean generation DOES hold.
//! * `bit7 = 1` — CommitLog scan: the fuzz bytes ARE the segment, scanned by `scan_commitlog`
//!   against the committed `commitlog_find` fixture's schema and table id.
//!
//! Property (criterion 3): never panic / hang / exceed the memory budget, AND never report
//! success for input that could not be read (the #4159 class), asserted on the tool's own result:
//! * an UNREADABLE `Data.db` (`< 4` bytes) must produce `Unreadable(cause)`, never a probe verdict;
//! * an unreadable `Filter.db` must not read as `BloomNegative`, an unreadable `Index.db` /
//!   `Partitions.db` not as `IndexMiss`, an unreadable `CompressionInfo.db` not as a `Hit`;
//! * a CommitLog segment shorter than a descriptor header must yield a `SegmentFailure`, never an
//!   empty ("nothing found") scan.
//!
//! Properties asserted on the tool's own result, with the `find-key-probe` OpenSpec requirement
//! each one pins (`openspec/changes/find-key-probe/specs/find-key-probe/spec.md`):
//! * F1: every generation answers EXACTLY ONE of hit / miss / filtered / skip ([`classify`] is an
//!   exhaustive match over `GenOutcome`);
//! * F5.2 / F5.5: a skip (`Unreadable`) names a non-empty cause and is NAMED in both renderings;
//! * F5 / F5.1 / F5.3 / F5.5: `FindReport::exit_code()` is `2` iff some generation is a skip or some
//!   segment is a `SegmentFailure` / `SegmentTruncated`, else `0`, and rendering never panics (JSON
//!   stays valid);
//! * F3.3: an unreadable segment is its own `SegmentFailure` line with a cause, never a silently
//!   shorter report;
//! * F4.2 / F4.3: with no generation holding the key a known-writetime mutation is `unflushed=yes`
//!   and an unmeasured writetime stays `unmeasured` (never a defaulted verdict);
//! * F6: only decoded structure answers (no header hunting) — the target never post-processes bytes.
//!
//! Without `CQLITE_DATASETS_ROOT` (and the committed fixtures) the target is a no-op.

use libfuzzer_sys::fuzz_target;

#[path = "support/mod.rs"]
mod support;

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use cqlite_cli::commands::find::{
    encode_partition_key, probe_generations, FindReport, GenOutcome, GenRow, Unflushed,
};
use cqlite_cli::commands::find_render::{render_json, render_text};
use cqlite_core::storage::commitlog::Writetime;
use cqlite_core::storage::sstable::reader::ScanOutcome;
use cqlite_cli::commands::find_commitlog::{scan_commitlog, CommitEntry};
use cqlite_core::schema::TableSchema;
use cqlite_core::storage::sstable::reader::GenerationProbe;

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

/// `CREATE TABLE ... <table> (...)` statement of `table` out of a committed schema file.
fn schema_of(file: &str, table: &str, keyspace: &str) -> Option<TableSchema> {
    let cql = std::fs::read_to_string(repo().join("test-data/schemas").join(file)).ok()?;
    let start = cql.find(&format!("CREATE TABLE IF NOT EXISTS {table}"))?;
    let end = start + cql[start..].find(';')? + 1;
    let (_, mut t) = cqlite_core::fuzz_support::parse_create_table(&cql[start..end]).ok()?;
    t.keyspace = keyspace.to_string();
    Some(t)
}

/// The first partition key literal of a generation's `sstabledump` golden.
fn first_key_literal(dir: &Path) -> Option<String> {
    for e in std::fs::read_dir(dir).ok()?.flatten() {
        if e.file_name().to_string_lossy().ends_with("-Data.db.jsonl") {
            let line = std::fs::read_to_string(e.path()).ok()?;
            let line = line.lines().next()?;
            let after = line.split("\"key\":[\"").nth(1)?;
            return Some(after.split('"').next()?.to_string());
        }
    }
    None
}

struct Base {
    dir: PathBuf,
    schema: TableSchema,
    key: Vec<u8>,
}

/// A base generation whose CLEAN probe is a `Hit` (otherwise the fixture drifted and the target
/// no-ops rather than asserting against a baseline it cannot trust).
fn base(big: bool) -> Option<&'static Base> {
    static BIG: OnceLock<Option<Base>> = OnceLock::new();
    static BTI: OnceLock<Option<Base>> = OnceLock::new();
    let cell = if big { &BIG } else { &BTI };
    cell.get_or_init(|| {
        let (ks, table, file) = if big {
            ("test_comp", "lz4_table", "compression-parity.cql")
        } else {
            ("test_da", "wide_table", "wide-table-bti.cql")
        };
        let dir = support::table_dir(ks, table)?;
        let schema = schema_of(file, table, ks)?;
        let key = encode_partition_key(&schema, &first_key_literal(&dir)?).ok()?;
        let copy = support::copy_generation(&dir)?;
        let data = support::data_db(copy.path())?;
        let rows = support::runtime().block_on(probe_generations(&[data], &key, &schema));
        let clean_hit = matches!(
            rows.first().map(|r| &r.outcome),
            Some(GenOutcome::Probe(GenerationProbe::Hit(_)))
        );
        clean_hit.then_some(Base { dir, schema, key })
    })
    .as_ref()
}

fn probe_mode(input: &support::Input<'_>) {
    let Some(b) = base(!input.flag) else { return };
    let Some(tmp) = support::copy_generation(&b.dir) else { return };
    if !support::apply(tmp.path(), input.component, input.bytes) {
        return;
    }
    let Some(data) = support::data_db(tmp.path()) else { return };
    let rows = support::runtime().block_on(probe_generations(&[data], &b.key, &b.schema));
    let Some(row) = rows.first() else { return };
    check_report(FindReport { generations: rows.clone(), commitlog: None });
    if !support::is_unreadable(input.component, input.bytes) {
        return;
    }
    match (&row.outcome, input.component) {
        (GenOutcome::Unreadable(cause), _) => assert!(!cause.trim().is_empty(), "Unreadable without a cause"),
        (_, "Data.db") => panic!(
            "a Data.db of {} byte(s) must be reported Unreadable, got {:?}",
            input.bytes.len(),
            row.outcome
        ),
        (GenOutcome::Probe(GenerationProbe::BloomNegative), "Filter.db") => {
            panic!("an unreadable Filter.db must not read as a bloom-negative answer")
        }
        (GenOutcome::Probe(GenerationProbe::IndexMiss), "Index.db" | "Partitions.db") => {
            panic!("an unreadable {} must not read as an index miss", input.component)
        }
        (GenOutcome::Probe(GenerationProbe::Hit(_)), "CompressionInfo.db") => {
            panic!("a Hit was reported from an unreadable CompressionInfo.db of a compressed table")
        }
        _ => {}
    }
}

/// F1: the four answers a generation can give. Exhaustive on purpose: a new `GenOutcome` /
/// `GenerationProbe` variant fails to compile until it is classified.
#[derive(Debug, PartialEq, Eq)]
enum Answer {
    Hit,
    Miss,
    Filtered,
    Skip,
}

fn classify(o: &GenOutcome) -> Answer {
    match o {
        GenOutcome::Probe(GenerationProbe::Hit(_))
        | GenOutcome::Probe(GenerationProbe::Scanned(ScanOutcome::Hit(_))) => Answer::Hit,
        GenOutcome::Probe(GenerationProbe::IndexMiss)
        | GenOutcome::Probe(GenerationProbe::Scanned(ScanOutcome::Miss)) => Answer::Miss,
        GenOutcome::Probe(GenerationProbe::BloomNegative) => Answer::Filtered,
        GenOutcome::Unreadable(_) => Answer::Skip,
    }
}

/// Assertions on a whole report, through the tool's own `exit_code` and renderers.
fn check_report(report: FindReport) {
    let mut expect_fail = false;
    for g in &report.generations {
        if let GenOutcome::Unreadable(cause) = &g.outcome {
            // F5.2: a skip names its cause.
            assert!(!cause.trim().is_empty(), "{}: Unreadable without a cause", g.basename);
            assert_eq!(classify(&g.outcome), Answer::Skip);
            expect_fail = true;
        }
    }
    if let Some(entries) = &report.commitlog {
        for e in entries {
            match e {
                CommitEntry::SegmentFailure { segment, cause } => {
                    assert!(!segment.is_empty() && !cause.trim().is_empty(), "unnamed SegmentFailure");
                    expect_fail = true;
                }
                CommitEntry::SegmentTruncated { segment } => {
                    assert!(!segment.is_empty(), "unnamed SegmentTruncated");
                    expect_fail = true;
                }
                CommitEntry::Mutation { writetime, unflushed, .. } => {
                    // F4.3: an unmeasured writetime never yields a defaulted verdict.
                    if matches!(writetime, Writetime::Unmeasured(_)) {
                        assert!(
                            matches!(unflushed, Unflushed::Unmeasured(_)),
                            "unmeasured writetime but unflushed={unflushed:?}"
                        );
                    }
                    // F4.2: no generation holds the key -> a known write is unconditionally unflushed.
                    if report.generations.is_empty() && matches!(writetime, Writetime::Known(_)) {
                        assert_eq!(*unflushed, Unflushed::Yes, "F4.2: absent everywhere must be unflushed=yes");
                    }
                }
            }
        }
    }
    // F5 / F5.5: exit 2 iff anything could not be read in full, else 0 (absent-everywhere is 0).
    assert_eq!(report.exit_code(), if expect_fail { 2 } else { 0 }, "exit-code semantics");
    // F5.2 / F5.5: the failure is NAMED in both renderings, and rendering never panics.
    let text = render_text(&report);
    let json = render_json(&report);
    assert!(serde_json::from_str::<serde_json::Value>(&json).is_ok(), "render_json emitted invalid JSON");
    for GenRow { basename, outcome } in &report.generations {
        if classify(outcome) == Answer::Skip {
            assert!(text.contains(basename.as_str()), "skipped generation {basename} not named in text");
            assert!(json.contains(basename.as_str()), "skipped generation {basename} not named in JSON");
        }
    }
}

/// The committed CommitLog fixture's schema, table id and key.
fn commitlog_base() -> Option<&'static (TableSchema, Vec<u8>)> {
    static C: OnceLock<Option<(TableSchema, Vec<u8>)>> = OnceLock::new();
    C.get_or_init(|| {
        let (_, mut t) = cqlite_core::fuzz_support::parse_create_table(
            "CREATE TABLE commitlog_test.users (id int PRIMARY KEY, name text, age int)",
        )
        .ok()?;
        t.keyspace = "commitlog_test".to_string();
        let key = encode_partition_key(&t, "1").ok()?;
        Some((t, key))
    })
    .as_ref()
}

const COMMITLOG_TABLE_ID: &str = "5ed10960-dee9-11f1-92e1-a170acc55494";
/// Smaller than any CommitLog descriptor header (version, id, params): no segment can be read.
const MIN_SEGMENT_BYTES: usize = 8;

fn commitlog_mode(input: &support::Input<'_>) {
    let Some((schema, key)) = commitlog_base() else { return };
    let Ok(tmp) = tempfile::tempdir() else { return };
    if std::fs::write(tmp.path().join("CommitLog-7-1.log"), input.bytes).is_err() {
        return;
    }
    let out = scan_commitlog(tmp.path(), schema, COMMITLOG_TABLE_ID, key, &[]);
    if input.bytes.len() < MIN_SEGMENT_BYTES {
        assert!(
            out.iter().any(|e| matches!(e, CommitEntry::SegmentFailure { .. })),
            "a {}-byte CommitLog segment cannot be read yet the scan reported {out:?}",
            input.bytes.len()
        );
    }
    for e in &out {
        if let CommitEntry::SegmentFailure { cause, .. } = e {
            assert!(!cause.trim().is_empty(), "SegmentFailure without a cause");
        }
    }
    check_report(FindReport { generations: Vec::new(), commitlog: Some(out) });
}

fuzz_target!(|data: &[u8]| {
    let Some(input) = support::decode(data) else { return };
    if input.high {
        commitlog_mode(&input);
    } else {
        probe_mode(&input);
    }
});
