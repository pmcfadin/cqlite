//! Parquet export fail-closed negative tests — issue #1490 (AD1), epic #1469.
//!
//! # What these assert, and why they are HERE rather than in `cqlite-core`
//!
//! Two fail-closed contracts landed in the CQL→Arrow converter:
//!
//!   * **AC1 / issue #1485** — a `Value` whose runtime type does not match the
//!     column's declared type is an ERROR, never a silently-NULLed cell.
//!   * **AC3 / issue #1487** — a `decimal` whose scale exceeds the fixed Arrow
//!     scale (`Decimal128(38, 9)`) is an ERROR, never a lossy truncation.
//!
//! `cqlite-core` already unit-tests both against `rows_to_record_batch`
//! (`cqlite-core/src/export/arrow_convert_tests.rs`). Those tests pin the
//! CONVERTER. This file covers the two layers ABOVE it, and says which is which
//! — an earlier revision called itself a "CLI surface" test while every case
//! stopped one layer short of the command (roborev, round 8).
//!
//! ## Layer 1 — the WRITER boundary (`writer_boundary` cases below)
//!
//! `cqlite export --format parquet` goes through
//! `cqlite_cli::output::parquet::create_streaming_parquet_writer` →
//! `StreamingWriter::write_chunk`, which is a different code path (streaming,
//! chunked, schema built once at construction) from the batch converter and
//! maps core errors into `OutputError`. A mapping that swallowed the error, or a
//! streaming path that skipped the check, would leave the core unit tests green.
//! These cases drive the EXACT constructor `commands/export.rs` calls, for BOTH
//! contracts and BOTH stages (`write_chunk` and `finalize`).
//!
//! ## Layer 2 — the COMMAND surface (`command_surface` cases below)
//!
//! Layer 1 still cannot see the command layer: `commands/export.rs` maps each
//! writer `Result` with `.map_err(...)?`, and turning one of those into
//! `let _ = …` would swallow the refusal, leave a half-written file on disk and
//! exit 0 with every Layer-1 test green. So the AC3 case is ALSO driven through
//! the real binary (`env!("CARGO_BIN_EXE_cqlite")`, spawned as a process — the
//! test FAILS, never skips, if the binary is not there), asserting the process
//! exits non-zero, that the diagnostic carries the command layer's OWN wrapper
//! text (`"Failed to finalize Parquet"` is stamped by the CLI command modules
//! and by nothing in `cqlite-core` or the output writers), and that no readable
//! Parquet file is left behind.
//!
//! ### The fixture, and why it is a COMMITTED one
//!
//! The command-surface cases drive `test_da.simple_table`, whose SSTable
//! components are GIT-TRACKED (`test-data/datasets/sstables/test_da/`, the same
//! `must_run` fixture class the parity cases in this lane classify as committed).
//! They resolve it CHECKOUT-relative — anchored on the workspace-root
//! `Cargo.toml`, never from `CQLITE_DATASETS_ROOT` — and COPY it into a
//! `TempDir` data root, so they always run on a clean checkout with no dataset
//! fetch, and cannot be perturbed by whatever corpus a machine happens to hold.
//! An earlier revision hard-required the FETCHED `test_basic.simple_table` and
//! therefore PANICKED on a clean checkout (roborev, round 9), contradicting both
//! this issue's "skip cleanly / prefer a committed fixture" criterion and the
//! optional-vs-`must_run` classification the parity cases already apply.
//!
//! ### Why only AC3 is driven end-to-end
//!
//! AC1's precondition is not reachable from the CLI's INPUT surface. The read
//! path builds every `Value` FROM the declared type, so a schema that disagrees
//! with the data yields a NULL or a value of the declared type — never a
//! mistyped one. Measured while writing these tests: eight deliberate
//! schema/data type disagreements over `test_basic.simple_table` and
//! `test_collections.collection_table` (text→int, text→boolean, text→uuid,
//! boolean→text, uuid→int, bigint→int, `set<text>`→int, `list<int>`→text) all
//! exported SUCCESSFULLY; none produced an AC1 refusal. AC1 is therefore a
//! defence-in-depth invariant against an internal decoder bug, and it travels
//! the SAME two `.map_err(…)?` statements in `commands/export.rs` that the AC3
//! case proves end-to-end. Inducing it at the command surface would require a
//! deliberately corrupt fixture, which is a bigger fixture question than this
//! issue — recorded here rather than left as an unstated gap.
//!
//! Every case pairs the negative with a POSITIVE CONTROL built the same way, so
//! a writer that rejected everything (or a helper that never wrote anything)
//! cannot make the negatives pass vacuously.

#![cfg(feature = "state_machine")]

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;

use cqlite_cli::config::OutputConfig;
use cqlite_cli::output::parquet::create_streaming_parquet_writer_from_writer;
use cqlite_cli::output::{ParquetWriter, StreamingWriter};
use cqlite_core::query::{ColumnInfo, QueryMetadata, QueryResult, QueryRow};
use cqlite_core::schema::CqlType;
use cqlite_core::types::{DataType, Value};
use cqlite_core::RowKey;

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

fn column(name: &str, data_type: DataType, cql_type: CqlType) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        data_type,
        nullable: true,
        position: 0,
        table_name: None,
        cql_type: Some(cql_type),
    }
}

fn metadata(columns: Vec<ColumnInfo>) -> QueryMetadata {
    QueryMetadata {
        columns,
        ..Default::default()
    }
}

fn row(name: &str, value: Value) -> QueryRow {
    let mut values: HashMap<std::sync::Arc<str>, Value> = HashMap::new();
    values.insert(name.into(), value);
    QueryRow {
        values,
        key: RowKey::new(vec![0]),
        metadata: Default::default(),
        cell_metadata: None,
    }
}

/// Drive the STREAMING writer the `export` subcommand uses, writing into an
/// in-memory buffer, and return the finished Parquet bytes.
///
/// The whole export is one unit: the streaming writer BUFFERS rows and converts
/// them when a row group flushes, so a rejected value surfaces from
/// `write_chunk` or from `finalize` depending on the row-group size. Asserting
/// on a particular call would pin an implementation detail; what the contract
/// says is that the EXPORT fails and says why. The stage is recorded in the
/// message so a diagnostic still names it.
fn stream_rows(columns: Vec<ColumnInfo>, rows: &[QueryRow]) -> Result<Vec<u8>, String> {
    let meta = metadata(columns);
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = create_streaming_parquet_writer_from_writer(&mut buf, &meta, 1_024)
            .map_err(|e| format!("constructor: {e}"))?;
        writer
            .write_chunk(rows)
            .map_err(|e| format!("write_chunk: {e}"))?;
        writer.finalize().map_err(|e| format!("finalize: {e}"))?;
    }
    Ok(buf)
}

/// Drive the BATCH writer the `-e … --out parquet` query path uses.
fn batch_rows(columns: Vec<ColumnInfo>, rows: Vec<QueryRow>) -> Result<Vec<u8>, String> {
    let result = QueryResult {
        rows,
        rows_affected: 0,
        execution_time_ms: 0,
        metadata: metadata(columns),
    };
    ParquetWriter::write(&result, &OutputConfig::default()).map_err(|e| e.to_string())
}

/// Read Parquet bytes back and return (row count, null count of column 0).
///
/// A control that only checked "the writer returned Ok" could not distinguish a
/// real value from the silently-NULLed cell both fail-closed contracts exist to
/// forbid, so every positive control reads its own output back.
fn readback_col0(bytes: &[u8]) -> (usize, usize) {
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.to_vec()))
        .expect("control output must be a readable Parquet file")
        .build()
        .expect("control output must build a record batch reader");
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("control output must decode");
    let rows = batches.iter().map(|b| b.num_rows()).sum();
    let nulls = batches
        .iter()
        .map(|b| b.column(0).null_count())
        .sum::<usize>();
    (rows, nulls)
}

/// A `decimal` value with the requested scale. `unscaled` is big-endian
/// two's-complement, exactly as the CQL decode path produces it.
fn decimal(scale: i32, unscaled: i64) -> Value {
    Value::Decimal {
        scale,
        unscaled: num_bigint::BigInt::from(unscaled).to_signed_bytes_be(),
    }
}

// ---------------------------------------------------------------------------
// AC1 (#1485) — a mistyped column value must ERROR, not become NULL
// ---------------------------------------------------------------------------

/// A `Text` value in an `int` column: the streaming export the `export`
/// subcommand drives must FAIL, and the diagnostic must name the column.
#[test]
fn ac1_streaming_export_rejects_type_mismatched_value() {
    let err = stream_rows(
        vec![column("age", DataType::Integer, CqlType::Int)],
        &[row("age", Value::Text("not-an-int".into()))],
    )
    .expect_err("a Text value in an int column must fail the Parquet export");
    assert!(
        err.contains("age") && err.contains("expected Int"),
        "the rejection must name the column and the expected type: {err}"
    );
}

/// The same mismatch through the batch writer behind `-e … --out parquet`.
#[test]
fn ac1_batch_export_rejects_type_mismatched_value() {
    let err = batch_rows(
        vec![column("age", DataType::Integer, CqlType::Int)],
        vec![row("age", Value::Text("not-an-int".into()))],
    )
    .expect_err("a Text value in an int column must fail the Parquet export");
    assert!(
        err.contains("age") && err.contains("expected Int"),
        "the rejection must name the column and the expected type: {err}"
    );
}

/// Positive control: the well-typed value exports AND reads back non-NULL.
///
/// Without this, a writer that rejected every row would make the negative above
/// pass for the wrong reason.
#[test]
fn ac1_control_well_typed_value_still_exports() {
    let cols = || vec![column("age", DataType::Integer, CqlType::Int)];

    let bytes = stream_rows(cols(), &[row("age", Value::Integer(41))])
        .expect("a well-typed int must export via the streaming writer");
    assert_eq!(readback_col0(&bytes), (1, 0), "streamed value must be live");
    let arr = int32_col0(&bytes);
    assert_eq!(
        arr,
        vec![Some(41)],
        "streamed value must round-trip exactly"
    );

    let bytes = batch_rows(cols(), vec![row("age", Value::Integer(41))])
        .expect("a well-typed int must export via the batch writer");
    assert_eq!(readback_col0(&bytes), (1, 0));
    assert_eq!(int32_col0(&bytes), vec![Some(41)]);
}

/// Regression guard (#1485): the fail-closed check must not have captured the
/// legitimate NULL path. An explicit `Value::Null` and an ABSENT column both
/// stay NULL — asserted at the CLI surface, where the earlier silent-NULL bug
/// would have been indistinguishable from these.
#[test]
fn ac1_control_null_is_not_a_type_mismatch() {
    let cols = || vec![column("age", DataType::Integer, CqlType::Int)];

    let bytes =
        stream_rows(cols(), &[row("age", Value::Null)]).expect("an explicit NULL must export");
    assert_eq!(readback_col0(&bytes), (1, 1), "explicit NULL stays NULL");

    let absent = QueryRow {
        values: HashMap::new(),
        key: RowKey::new(vec![0]),
        metadata: Default::default(),
        cell_metadata: None,
    };
    let bytes = stream_rows(cols(), &[absent]).expect("an absent column must export as NULL");
    assert_eq!(readback_col0(&bytes), (1, 1), "absent column stays NULL");
}

/// The collection element-dispatch arm: a well-formed `list<int>` whose ELEMENT
/// is mistyped must fail too — the per-element path is a separate arm from the
/// scalar one and had its own silent-NULL fallback.
#[test]
fn ac1_streaming_export_rejects_mistyped_collection_element() {
    let cols = || {
        vec![column(
            "scores",
            DataType::List,
            CqlType::List(Box::new(CqlType::Int)),
        )]
    };
    let err = stream_rows(
        cols(),
        &[row(
            "scores",
            Value::List(vec![Value::Integer(1), Value::Text("bad".into())]),
        )],
    )
    .expect_err("a mistyped list element must fail the Parquet export");
    assert!(
        err.contains("element") && err.contains("expected Int"),
        "the rejection must name the element and the expected type: {err}"
    );

    // Control: the same list with every element well-typed exports.
    let bytes = stream_rows(
        cols(),
        &[row(
            "scores",
            Value::List(vec![Value::Integer(1), Value::Integer(2)]),
        )],
    )
    .expect("a well-typed list must export");
    assert_eq!(readback_col0(&bytes), (1, 0));
}

// ---------------------------------------------------------------------------
// AC3 (#1487) — decimal scale > 9 must ERROR, not truncate
// ---------------------------------------------------------------------------

/// 123456789012 with scale 12 == 0.123456789012 — three fractional digits more
/// than the `Decimal128(38, 9)` target can hold. The pre-#1487 code path scaled
/// this down and succeeded LOSSILY, so the assertion is that the export fails
/// and says it refuses to truncate.
#[test]
fn ac3_streaming_export_rejects_decimal_scale_above_fixed() {
    let err = stream_rows(
        vec![column("amount", DataType::Blob, CqlType::Decimal)],
        &[row("amount", decimal(12, 123_456_789_012))],
    )
    .expect_err("a scale-12 decimal must fail the Parquet export, not truncate");
    assert!(
        err.contains("amount") && err.contains("scale 12") && err.contains("truncate"),
        "the rejection must name the column, the scale and the refusal: {err}"
    );
}

#[test]
fn ac3_batch_export_rejects_decimal_scale_above_fixed() {
    let err = batch_rows(
        vec![column("amount", DataType::Blob, CqlType::Decimal)],
        vec![row("amount", decimal(12, 123_456_789_012))],
    )
    .expect_err("a scale-12 decimal must fail the Parquet export, not truncate");
    assert!(
        err.contains("amount") && err.contains("scale 12") && err.contains("truncate"),
        "the rejection must name the column, the scale and the refusal: {err}"
    );
}

/// Positive control on BOTH sides of the boundary: scale 9 is the largest scale
/// the fixed export scale can represent and must still succeed exactly, and an
/// ordinary scale-3 value must rescale (123.456 → 123_456_000_000 at scale 9).
#[test]
fn ac3_control_decimal_at_or_below_fixed_scale_still_exports() {
    use arrow::array::Decimal128Array;

    for (scale, unscaled, expect) in [
        (3i32, 123_456i64, 123_456_000_000i128),
        (9i32, 123_456i64, 123_456i128),
    ] {
        let bytes = stream_rows(
            vec![column("amount", DataType::Blob, CqlType::Decimal)],
            &[row("amount", decimal(scale, unscaled))],
        )
        .unwrap_or_else(|e| panic!("scale-{scale} decimal must export: {e}"));
        assert_eq!(readback_col0(&bytes), (1, 0), "scale-{scale} must be live");

        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
            .expect("readable")
            .build()
            .expect("reader");
        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().expect("decode");
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("Decimal128Array");
        assert_eq!(
            arr.value(0),
            expect,
            "scale-{scale} decimal must rescale exactly to the fixed export scale"
        );
    }
}

/// Regression guard (#1487): the scale check must not disturb the NULL path.
#[test]
fn ac3_control_null_decimal_stays_null() {
    let bytes = stream_rows(
        vec![column("amount", DataType::Blob, CqlType::Decimal)],
        &[row("amount", Value::Null)],
    )
    .expect("a NULL decimal must export");
    assert_eq!(readback_col0(&bytes), (1, 1));
}

/// Read column 0 back as `Int32`, so a control asserts the VALUE, not merely
/// that some non-null cell exists.
fn int32_col0(bytes: &[u8]) -> Vec<Option<i32>> {
    use arrow::array::Int32Array;
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.to_vec()))
        .expect("readable Parquet")
        .build()
        .expect("record batch reader");
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().expect("decode");
    batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32Array")
                .clone();
            (0..a.len())
                .map(move |i| if a.is_null(i) { None } else { Some(a.value(i)) })
                .collect::<Vec<_>>()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Layer 2 — the COMMAND surface: `cqlite export --format parquet`, real binary
// ---------------------------------------------------------------------------

/// TABLE-granular corpus resolution (#3220): walk EVERY candidate root for the
/// table this lane needs, rather than committing to a root by keyspace.
// The parity HARNESS itself, so the fixture-discovery refusal below is asserted
// against the code the parity cases actually run (round 18) — and `datasets_root`
// comes from it rather than being included a second time.
#[path = "support/parquet_parity/mod.rs"]
mod parquet_parity;

use parquet_parity::datasets_root;
// The fail-closed directory primitives, SHARED with the harness rather than
// re-spelled here: `read_dir_completely` (a per-entry error is a refusal, never
// a short listing) and the fallible path-kind pair (`is_dir`/`is_file` answer
// `false` for "could not stat", which drops an entry from a census that is then
// asserted to hold exactly one).
use parquet_parity::fixture_root::{path_is_dir, path_is_file, read_dir_completely};

use std::path::{Path, PathBuf};
use std::process::Command;

/// The GIT-TRACKED fixture the command-surface cases drive, and its committed
/// schema. Both are checkout resident, so these cases never need a fetch.
const FIXTURE_KEYSPACE: &str = "test_da";
const FIXTURE_TABLE: &str = "simple_table";
const FIXTURE_QUALIFIED: &str = "test_da.simple_table";
const FIXTURE_SCHEMA_FILE: &str = "da-test.cql";

/// The committed CQL type declaration these cases rewrite, the rewrite, and the
/// column it renames the type of.
///
/// Bound to the committed schema on purpose: the substitution count is asserted,
/// so if `da-test.cql` ever stops declaring this column exactly this way the
/// case REDS instead of silently exporting an unmodified schema and passing for
/// the wrong reason.
const DECLARED_LINE: &str = "    name TEXT,";
const REDECLARED_LINE: &str = "    name DECIMAL,";
const REWRITTEN_COLUMN: &str = "name";

/// The `cqlite` binary under test.
///
/// `CARGO_BIN_EXE_<name>` is set by cargo for every integration test of the
/// package that declares the bin, so the binary is BUILT by the same
/// `cargo test` invocation that runs this file. The existence check exists so
/// that an unavailable binary is a NAMED failure rather than an opaque spawn
/// error — and never a skip, which is the vacuous pass this file exists to
/// prevent.
fn cqlite_binary() -> PathBuf {
    let path = PathBuf::from(env!("CARGO_BIN_EXE_cqlite"));
    // `is_file()` is two-valued, but here its permissive answer points the safe
    // way: a path it could not stat answers `false` and the assertion FIRES.
    assert!(
        path.is_file(),
        "the cqlite binary must exist at {} — cargo builds it for this test target, so its \
         absence is a build problem, not a reason to skip the command-surface assertions",
        path.display()
    );
    path
}

/// The committed SSTable directory for the fixture, resolved CHECKOUT-relative.
///
/// Deliberately NOT `datasets_root::sstables_root_for_table`: that walks
/// `CQLITE_DATASETS_ROOT` first, which would let an ambient corpus decide which
/// bytes these cases export. The fixture is git-tracked, so the checkout is the
/// only root that can serve it, and `checkout_test_data_dir` anchors on the
/// workspace-root `Cargo.toml` exactly as the committed schemas do (#3148).
///
/// Fail-closed: an absent or ambiguous fixture is a NAMED red, never a skip —
/// its binaries are committed, so absence is a checkout problem.
fn committed_fixture_dir() -> PathBuf {
    let keyspace_dir = datasets_root::fixture_roots::checkout_test_data_dir()
        .join("datasets")
        .join("sstables")
        .join(FIXTURE_KEYSPACE);
    let prefix = format!("{FIXTURE_TABLE}-");
    // The census below asserts there is EXACTLY ONE committed generation, so
    // every entry has to be accounted for. The `filter_map(|e| e.ok())`,
    // `p.is_dir()` and `to_str().unwrap_or(false)` this replaces each answered
    // "not a candidate" for an entry they could not READ, and a dropped entry
    // can only make the count SMALLER — which is how a second generation passes
    // the `assert_eq!(dirs.len(), 1)` and gets exported instead of the pinned
    // one. Fallible throughout, and the name is matched on BYTES so a non-UTF-8
    // generation directory COUNTS rather than vanishing.
    let mut dirs: Vec<PathBuf> = Vec::new();
    for entry in read_dir_completely(&keyspace_dir).unwrap_or_else(|e| {
        panic!(
            "{FIXTURE_QUALIFIED} is a git-tracked fixture; its keyspace directory must be \
                readable COMPLETELY: {e}"
        )
    }) {
        if !entry
            .file_name()
            .as_encoded_bytes()
            .starts_with(prefix.as_bytes())
        {
            continue;
        }
        let path = entry.path();
        if !path_is_dir(&path).unwrap_or_else(|e| panic!("{e}")) {
            continue;
        }
        if !dir_holds_data_db(&path).unwrap_or_else(|e| panic!("{e}")) {
            continue;
        }
        dirs.push(path);
    }
    dirs.sort();
    assert_eq!(
        dirs.len(),
        1,
        "expected exactly one committed {FIXTURE_QUALIFIED} generation carrying a *-Data.db \
         under {}, found {dirs:?} — these cases pin ONE fixture, so an added generation must \
         RED here rather than silently export a different one",
        keyspace_dir.display()
    );
    dirs.pop().expect("length asserted to be 1")
}

/// Does `dir` hold at least one `*-Data.db` component? `Err` when it cannot be
/// determined.
///
/// Presence is judged by the actual binary, never by directory existence: the
/// repo commits JSONL sidecars for fixtures whose binaries are gitignored, so a
/// `<table>-<uuid>/` can exist with no readable SSTable in it.
///
/// FALLIBLE because the three `unwrap_or(false)`s it replaces made "I could not
/// read this directory" indistinguishable from "there is no Data.db here", in
/// BOTH of its call positions and to opposite effect: in the generation census
/// it dropped a candidate (shrinking a count that is asserted to be 1), and in
/// the isolated-root assertion it claimed a missing fixture. Neither answer was
/// measured. The name is matched on BYTES so a non-UTF-8 `*-Data.db` counts.
fn dir_holds_data_db(dir: &Path) -> Result<bool, String> {
    Ok(read_dir_completely(dir)?
        .iter()
        .any(|e| e.file_name().as_encoded_bytes().ends_with(b"-Data.db")))
}

/// Copy the committed fixture into an ISOLATED data root under `tmp` and return
/// that root, for `--data-dir`.
///
/// Isolation is the point: the exported bytes then come from the checkout alone,
/// so neither an unset nor a differently-populated `CQLITE_DATASETS_ROOT` can
/// change what these cases assert, and the export cannot see sibling keyspaces.
fn isolated_data_root(tmp: &Path) -> PathBuf {
    let src = committed_fixture_dir();
    let generation = src
        .file_name()
        .expect("the fixture directory has a final component");
    let root = tmp.join("data");
    let dst = root.join(FIXTURE_KEYSPACE).join(generation);
    std::fs::create_dir_all(&dst).unwrap_or_else(|e| panic!("create {}: {e}", dst.display()));
    // Fail-closed: the `file_type().map(..).unwrap_or(false)` this replaces
    // answered "not a file" for a component whose kind it could not read, and
    // SILENTLY DID NOT COPY it. The export then reads an SSTable missing a
    // component — fewer rows, or an abort attributed to the CLI — with nothing
    // naming the omission. Only a verified `NotFound` may skip an entry now.
    for entry in read_dir_completely(&src).unwrap_or_else(|e| panic!("read {}: {e}", src.display()))
    {
        if !path_is_file(&entry.path()).unwrap_or_else(|e| panic!("{e}")) {
            continue;
        }
        let to = dst.join(entry.file_name());
        std::fs::copy(entry.path(), &to)
            .unwrap_or_else(|e| panic!("copy {} -> {}: {e}", entry.path().display(), to.display()));
    }
    assert!(
        dir_holds_data_db(&dst).unwrap_or_else(|e| panic!("{e}")),
        "the isolated data root must carry the fixture's *-Data.db: {}",
        dst.display()
    );
    root
}

/// The committed `test-data/schemas/da-test.cql`, read as a string.
fn committed_fixture_schema() -> String {
    let path = datasets_root::schema_path(FIXTURE_SCHEMA_FILE).unwrap_or_else(|| {
        panic!("committed schema test-data/schemas/{FIXTURE_SCHEMA_FILE} not found")
    });
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Run `cqlite --schema … --data-dir … export <out> --format parquet --table …`
/// as a child process and return `(succeeded, stderr)`.
fn run_export_command(schema: &Path, data_dir: &Path, out: &Path) -> (bool, String) {
    let output = Command::new(cqlite_binary())
        .args([
            "--schema",
            schema.to_str().expect("schema path is UTF-8"),
            "--data-dir",
            data_dir.to_str().expect("data dir path is UTF-8"),
            "export",
            out.to_str().expect("output path is UTF-8"),
            "--format",
            "parquet",
            "--table",
            FIXTURE_QUALIFIED,
        ])
        .output()
        .expect("the cqlite binary must be spawnable");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// Assert that `path` does NOT hold a valid Parquet file.
///
/// A refused export may legitimately leave the opened file behind (the writer
/// stamps the `PAR1` preamble before the first row group), but it must never
/// leave one a reader would accept: the footer is what makes a Parquet file
/// readable, so a *valid* file here would mean the command finished a lossy
/// export and reported failure — or reported success on a truncated one.
fn assert_no_valid_parquet_left(path: &Path) {
    // The `if !path.exists() { return; }` this replaces SKIPPED the whole
    // assertion for a path it could not stat: `exists()` answers `false` for a
    // permission denial or an I/O error just as it does for a genuine absence,
    // so "I could not tell whether the export left a file" became "the property
    // holds". Only a verified `NotFound` may take the early return; every other
    // error is a named failure of THIS assertion.
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return; // nothing written at all — the strongest form of the property
        }
        Err(e) => panic!(
            "cannot read the leftover export output {} to check it is not a valid Parquet file: \
             {e} — an unreadable path is UNKNOWN, not an absent file, and skipping the check \
             here would pass this case without measuring anything",
            path.display()
        ),
    };
    assert!(
        bytes.len() < 8 || &bytes[bytes.len() - 4..] != b"PAR1",
        "a refused export must not leave a footer-complete Parquet file at {} ({} bytes)",
        path.display(),
        bytes.len()
    );
    // `.is_ok()` here is the MEASUREMENT, not a collapsed error: "the Parquet
    // reader rejected these bytes" is exactly the property being asserted, so
    // the failure IS the affirmative answer rather than an unknown.
    let readable = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
        .and_then(|b| b.build())
        .is_ok();
    assert!(
        !readable,
        "a refused export must not leave a READABLE Parquet file at {}",
        path.display()
    );
}

/// AC3 (#1487) at the command surface: `cqlite export --format parquet` must
/// FAIL, name the refusal, and leave no readable Parquet behind.
///
/// # How the offending value is produced, and why it is legitimate
///
/// CQL `decimal` is arbitrary-scale, so a scale above the fixed export scale is
/// ordinary Cassandra data — but no committed fixture carries one (the corpus
/// decimals top out at scale 3) and CQLite's own writer cannot mint one, so the
/// value has to come from the schema side. This case exports the GIT-TRACKED
/// `test_da.simple_table` corpus — copied into an isolated `TempDir` data root —
/// under the committed `da-test.cql` with ONE type declaration rewritten to
/// `decimal`. The cell bytes then decode to a
/// genuine `Value::Decimal` whose scale exceeds `DECIMAL_FIXED_SCALE` — at the
/// converter boundary indistinguishable from one decoded out of a
/// Cassandra-written high-scale decimal, which is the condition AC3 governs.
/// (`rescale_decimal`'s own doc records the same condition arising from a
/// corrupt on-disk scale, issue #1755.)
///
/// The assertion on `"Failed to finalize Parquet"` is the WIRING evidence. That
/// wrapper text belongs to the CLI command layer — `commands/export.rs:470` for
/// the `export` subcommand this case invokes, and `commands/export_sstable.rs`
/// for its `export-sstable` sibling — and appears nowhere in `cqlite-core` or in
/// the output writers. Seeing it therefore proves the refusal travelled
/// `commands/export.rs`'s `.map_err(…)?` rather than being produced (and
/// possibly swallowed) below it.
#[test]
fn command_surface_ac3_export_fails_and_leaves_no_valid_parquet() {
    let tmp = tempfile::TempDir::new().expect("temp dir");
    let data_dir = isolated_data_root(tmp.path());

    let committed = committed_fixture_schema();
    assert_eq!(
        committed.matches(DECLARED_LINE).count(),
        1,
        "{FIXTURE_SCHEMA_FILE} must declare `{DECLARED_LINE}` exactly once — this case rewrites \
         that declaration, so a schema change must RED here rather than export an unmodified \
         schema"
    );
    let schema_path = tmp.path().join("fixture-schema-decimal.cql");
    std::fs::write(
        &schema_path,
        committed.replace(DECLARED_LINE, REDECLARED_LINE),
    )
    .expect("write the rewritten schema");

    let out = tmp.path().join("refused.parquet");
    let (ok, stderr) = run_export_command(&schema_path, &data_dir, &out);

    assert!(
        !ok,
        "the export command must FAIL on a decimal the fixed export scale cannot hold; \
         stderr was: {stderr}"
    );
    assert!(
        stderr.contains("Failed to finalize Parquet"),
        "the diagnostic must carry the COMMAND layer's own wrapper text, which is what proves \
         the refusal propagated through commands/export.rs: {stderr}"
    );
    assert!(
        stderr.contains(&format!("Column '{REWRITTEN_COLUMN}'"))
            && stderr.contains("exceeds the fixed export scale 9")
            && stderr.contains("refusing to truncate"),
        "the diagnostic must name the column and the AC3 refusal: {stderr}"
    );
    assert_no_valid_parquet_left(&out);
}

/// POSITIVE CONTROL for the case above: the SAME command, the SAME table and the
/// SAME corpus under the COMMITTED schema must succeed and write a readable
/// Parquet file with rows in it.
///
/// Without it, any unrelated breakage — a bad `--data-dir`, an unparsable
/// schema, a binary that fails on every invocation — would satisfy the negative.
#[test]
fn command_surface_control_export_succeeds_under_the_committed_schema() {
    let tmp = tempfile::TempDir::new().expect("temp dir");
    let data_dir = isolated_data_root(tmp.path());

    let schema_path = tmp.path().join(FIXTURE_SCHEMA_FILE);
    std::fs::write(&schema_path, committed_fixture_schema()).expect("write the schema");

    let out = tmp.path().join("control.parquet");
    let (ok, stderr) = run_export_command(&schema_path, &data_dir, &out);

    assert!(ok, "the control export must SUCCEED: {stderr}");
    let bytes = std::fs::read(&out).expect("the control export must write its output file");
    assert_eq!(
        &bytes[bytes.len() - 4..],
        b"PAR1",
        "the control output must be footer-complete"
    );
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
        .expect("the control output must be a readable Parquet file")
        .build()
        .expect("the control output must build a record batch reader");
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().expect("decode");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        rows > 0,
        "the control export must contain rows — a 0-row export would make the negative vacuous"
    );

    // The rewritten column must carry live values under its committed type: if
    // it were absent or all-NULL, the negative above could not be about it.
    let live = batches
        .iter()
        .filter_map(|b| {
            b.column_by_name(REWRITTEN_COLUMN)
                .map(|c| c.len() - c.null_count())
        })
        .sum::<usize>();
    assert!(
        live > 0,
        "`{REWRITTEN_COLUMN}` must export live values under its committed TEXT declaration"
    );
}

// ===========================================================================
// Round 18: FIXTURE DISCOVERY refuses an unreadable root, never skips it
// ===========================================================================

/// A candidate root the harness could not READ is a REFUSAL, never an absent
/// fixture — the third state `Option` could not carry (#1490 round 18).
///
/// # The hole, MEASURED before it is closed
///
/// Discovery used to start at `datasets_root::sstables_root_for_table`, an
/// `Option`-returning search whose `table_has_data` maps a failed `read_dir` to
/// `false` and drops per-entry errors with `filter_map(|e| e.ok())`. So the
/// three-state signal — here / verifiably not here / could not tell — collapsed
/// onto the PERMISSIVE value, and an unreadable corpus root read as an absent
/// fixture: `Ok(None)`, which optional cases report as a SKIP. The
/// complete-directory checks that would have caught it never ran, because
/// discovery had already concluded the table was not there.
///
/// Every case below therefore MEASURES the old permissive answer first (the
/// legacy helper really does say "no table here" about a directory it could not
/// read) and only then asserts the new refusal. Without the first half the
/// second proves nothing.
#[test]
fn an_unreadable_candidate_root_is_refused_never_read_as_absent() {
    use parquet_parity::datasets_root::table_has_data;
    use parquet_parity::fixture_root::first_candidate_root_with_table;
    use std::fs;
    use std::path::PathBuf;

    const KS: &str = "test_da";
    const TABLE: &str = "simple_table";

    let tmp = tempfile::TempDir::new().expect("temp dir");
    let root = |name: &str| -> PathBuf { tmp.path().join(name) };
    let search = |roots: &[PathBuf]| first_candidate_root_with_table(roots, KS, TABLE);

    // A root that HOLDS the fixture, with the golden its generation implies.
    let present = root("present");
    let gen_dir = present.join(KS).join(format!("{TABLE}-aaaa"));
    fs::create_dir_all(&gen_dir).expect("scratch dirs");
    fs::write(gen_dir.join("nb-1-big-Data.db"), b"not really an sstable").expect("scratch data");
    fs::write(gen_dir.join("nb-1-big-Data.db.jsonl"), b"{}\n").expect("scratch golden");
    assert_eq!(
        search(std::slice::from_ref(&present)).expect("a readable root is not a refusal"),
        Some(present.clone()),
        "a root carrying <keyspace>/<table>-*/*-Data.db must be selected"
    );

    // VERIFIED absences — the ONLY state that may legitimately become a skip.
    // Each is judged by an actual `*-Data.db`, never by directory existence.
    let no_keyspace = root("no-keyspace");
    fs::create_dir_all(&no_keyspace).expect("scratch dirs");
    let sidecar_only = root("sidecar-only");
    let sidecar_dir = sidecar_only.join(KS).join(format!("{TABLE}-bbbb"));
    fs::create_dir_all(&sidecar_dir).expect("scratch dirs");
    fs::write(sidecar_dir.join("nb-1-big-Data.db.jsonl"), b"{}\n").expect("scratch golden");
    let never_created = root("never-created");
    for absent in [&no_keyspace, &sidecar_only, &never_created] {
        assert_eq!(
            search(std::slice::from_ref(absent)).unwrap_or_else(|e| panic!(
                "{} is READABLE and simply lacks the table, which is an affirmative absence, \
                 not a refusal: {e}",
                absent.display()
            )),
            None,
            "{} holds no *-Data.db for {KS}.{TABLE}",
            absent.display()
        );
    }

    // UNREADABLE, in the shape that needs no permission games and so holds for
    // any uid (including a root-running CI): the keyspace PATH is a FILE, so
    // `read_dir` fails with something other than `NotFound` — "I cannot tell",
    // which the old code mapped to `false`.
    let ks_is_a_file = root("ks-is-a-file");
    fs::create_dir_all(&ks_is_a_file).expect("scratch dirs");
    fs::write(ks_is_a_file.join(KS), b"not a directory").expect("scratch file");
    // THE DEFECT, measured: the permissive helper reports the table ABSENT from a
    // root it could not inspect, which is what made an optional case SKIP.
    assert!(
        !table_has_data(&ks_is_a_file, KS, TABLE),
        "this case's premise is that the legacy Option-shaped helper answers \"no table here\" \
         about a candidate it could NOT read; if it now refuses, the assertion below is \
         guarding something else and must be re-derived"
    );
    let err = search(std::slice::from_ref(&ks_is_a_file))
        .expect_err("a candidate root the harness could not read must REFUSE, not answer None");
    assert!(
        err.contains(&ks_is_a_file.join(KS).display().to_string()),
        "the refusal must name the path it could not read: {err}"
    );
    assert!(
        err.contains("REFUSES") && err.contains("SKIP"),
        "…and say WHY an unreadable root is not an absent fixture: {err}"
    );

    // An unreadable root is refused even when a LATER candidate holds the table:
    // the harness cannot know the unreadable root did not hold a DIFFERENT
    // generation, and it refuses ambiguity it cannot measure rather than compare
    // against whichever root it happened to be able to read.
    search(&[ks_is_a_file.clone(), present.clone()])
        .expect_err("an unreadable EARLIER candidate must refuse, not fall through to a later one");
    // …and the order is not what decides it: a readable candidate first still
    // wins outright, so the refusal above is about readability, not about
    // position in the list.
    assert_eq!(
        search(&[present.clone(), ks_is_a_file.clone()]).expect("the first root serves the table"),
        Some(present.clone()),
        "a root that HOLDS the table ends the search before any later candidate is read"
    );

    // The generation directory is read too, so an unreadable one refuses as well
    // — the census inside it is what decides whether a `*-Data.db` is there.
    let gen_is_a_file = root("gen-is-a-file");
    let gen_ks = gen_is_a_file.join(KS);
    fs::create_dir_all(&gen_ks).expect("scratch dirs");
    fs::create_dir_all(gen_ks.join(format!("{TABLE}-cccc"))).expect("scratch dirs");
    fs::write(
        gen_ks
            .join(format!("{TABLE}-cccc"))
            .join("unreadable-child"),
        b"x",
    )
    .expect("scratch file");
    // (A readable-but-empty generation directory is an affirmative absence.)
    fs::remove_file(
        gen_ks
            .join(format!("{TABLE}-cccc"))
            .join("unreadable-child"),
    )
    .expect("scratch cleanup");
    assert_eq!(
        search(std::slice::from_ref(&gen_is_a_file))
            .expect("an empty generation directory is readable"),
        None,
        "a <table>-* directory with no *-Data.db is an affirmative absence"
    );

    // THE FIXTURE DOOR, not merely the search: the case-level resolution the
    // stages call must propagate the refusal rather than report `Ok(None)`,
    // which is what `run_case` turns into a skip.
    let case = &parquet_parity::cases::DA_SIMPLE;
    assert_eq!(
        (case.keyspace, case.table),
        (KS, TABLE),
        "the scratch layout above is built for this case's keyspace/table"
    );
    parquet_parity::fixture_root::resolve_fixture_in_roots(
        case,
        std::slice::from_ref(&ks_is_a_file),
    )
    .expect_err("an unreadable candidate root must REFUSE the fixture, never skip the case");
    let resolved = parquet_parity::fixture_root::resolve_fixture_in_roots(
        case,
        std::slice::from_ref(&present),
    )
    .expect("a readable scratch root carrying the fixture must resolve")
    .expect("…to Some(fixture)");
    assert_eq!(resolved.table_dir, gen_dir);
    assert_eq!(resolved.golden, gen_dir.join("nb-1-big-Data.db.jsonl"));
    assert!(
        parquet_parity::fixture_root::resolve_fixture_in_roots(
            case,
            std::slice::from_ref(&no_keyspace)
        )
        .expect("a readable root that simply lacks the table is not a refusal")
        .is_none(),
        "Ok(None) — the legitimate skip — is reachable ONLY from a verified absence"
    );

    // The permission-denied shape, when this process is not privileged enough to
    // read past a `chmod 000`. Additive: the file-shaped cases above cover the
    // same property for any uid, so this one asserts nothing when the probe shows
    // the mode did not actually make the directory unreadable (a root-running
    // CI), instead of failing for the wrong reason.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let denied = root("denied");
        let denied_ks = denied.join(KS);
        fs::create_dir_all(denied_ks.join(format!("{TABLE}-dddd"))).expect("scratch dirs");
        fs::write(
            denied_ks
                .join(format!("{TABLE}-dddd"))
                .join("nb-1-big-Data.db"),
            b"x",
        )
        .expect("scratch data");
        fs::set_permissions(&denied_ks, fs::Permissions::from_mode(0o000)).expect("chmod 000");
        // A PROBE, legitimately permissive: it asks whether `chmod 000` actually
        // made the directory unreadable FOR THIS PROCESS. Its permissive branch
        // omits an ADDITIVE case whose property is already asserted, for any
        // uid, by the file-shaped cases above — so nothing goes unmeasured.
        if fs::read_dir(&denied_ks).is_err() {
            assert!(
                !table_has_data(&denied, KS, TABLE),
                "the legacy helper reports a permission-denied root as lacking the table"
            );
            let err = search(std::slice::from_ref(&denied))
                .expect_err("a permission-denied candidate root must REFUSE, not answer None");
            assert!(
                err.contains(&denied_ks.display().to_string()),
                "the refusal must name the unreadable path: {err}"
            );
        }
        // Restored so the TempDir can be removed.
        fs::set_permissions(&denied_ks, fs::Permissions::from_mode(0o755)).expect("chmod restore");
    }
}
