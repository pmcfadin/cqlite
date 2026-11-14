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
//! Without `CQLITE_DATASETS_ROOT` (and the committed fixtures) the target is a no-op.

use libfuzzer_sys::fuzz_target;

#[path = "support/mod.rs"]
mod support;

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use cqlite_cli::commands::find::{encode_partition_key, probe_generations, GenOutcome};
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
    if !support::is_unreadable(input.component, input.bytes) {
        return;
    }
    let unreadable = matches!(row.outcome, GenOutcome::Unreadable(_));
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
    let _ = unreadable;
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
}

fuzz_target!(|data: &[u8]| {
    let Some(input) = support::decode(data) else { return };
    if input.high {
        commitlog_mode(&input);
    } else {
        probe_mode(&input);
    }
});
