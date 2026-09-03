//! Fuzz-support surface (issue #1614) — thin drivers over the internal parser
//! entry points for the external `fuzz/` cargo-fuzz crate.
//!
//! This module is compiled **only** under `--features fuzz` and is
//! `#[doc(hidden)]`, so the default public API of `cqlite-core` is unchanged and
//! nothing here appears in normal docs. It exposes *fuzz drivers*, not the
//! parsers themselves: each entry point either re-exports an already-public
//! function or wraps a `pub(crate)` internal decode path in a thin
//! `Result`-returning shim.
//!
//! Contract for every wrapper: arbitrary input decodes to `Ok` or is rejected as
//! `Err`. There is **no `unwrap()`/`expect()`/`panic!`** anywhere in this module
//! — a decode error is the expected, correct outcome for adversarial bytes, and
//! the fuzz targets treat both `Ok` and `Err` as a pass (the never-panic /
//! never-hang / never-OOM invariant is enforced by libFuzzer, not by asserts).

use crate::error::Result;
use crate::types::Value;

// ---------------------------------------------------------------------------
// 1. VInt — already public; re-export for a single, discoverable fuzz surface.
// ---------------------------------------------------------------------------
pub use crate::parser::vint::{parse_vint, parse_vint_length, parse_vuint};

// ---------------------------------------------------------------------------
// 2. Schema CQL parsing — already public; re-export.
// ---------------------------------------------------------------------------
pub use crate::schema::cql_parser::{cql_type_to_type_id, parse_create_table};

/// The fixed list of CQL type strings exercised by the `fuzz_value_decode`
/// target: every scalar the schema-typed decoder supports plus one of each
/// collection shape, a tuple, and a deeply-nested frozen collection. The target
/// decodes the same arbitrary bytes at every type in this list.
pub const FUZZ_VALUE_TYPES: &[&str] = &[
    // scalars
    "boolean",
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "float",
    "double",
    "text",
    "blob",
    "uuid",
    "timeuuid",
    "timestamp",
    "date",
    "time",
    "inet",
    "decimal",
    "varint",
    "ascii",
    "counter",
    "duration",
    // collections / composites
    "list<int>",
    "set<text>",
    "map<text,int>",
    "tuple<int,text>",
    "frozen<list<list<int>>>",
];

/// Decode arbitrary `bytes` at the CQL type named by `type_str` using the real
/// schema-typed decode path (`ComparatorType::from_type_string` →
/// `parse_value_with_comparator`).
///
/// Returns `Err` — never panics — when the type string is unrepresentable or the
/// bytes do not form a valid value of that type.
pub fn fuzz_decode_value(type_str: &str, bytes: &[u8]) -> Result<Value> {
    let comparator = crate::types::ComparatorType::from_type_string(type_str)?;
    crate::storage::sstable::reader::parsing::comparator_value_parsing::parse_value_with_comparator(
        bytes,
        &comparator,
    )
}

/// Exercise the module-private nom `cql_type` type parser plus the public
/// `cql_type_to_type_id` conversion against an arbitrary string. Proves the
/// `MAX_TYPE_NESTING_DEPTH` guard (issue #1690) returns `Err` on pathological
/// nesting instead of overflowing the stack. Both sub-calls' results are
/// discarded; only the never-panic property matters.
pub fn fuzz_cql_type(s: &str) -> Result<()> {
    // The nom parser returns `IResult`; a parse failure is a normal outcome.
    let _ = crate::schema::cql_parser::cql_type_fuzz(s);
    // The public conversion returns `crate::Result`; ignore Ok/Err alike.
    let _ = cql_type_to_type_id(s);
    Ok(())
}

/// Drive the real BTI node-decode + DFS traversal over arbitrary `bytes`.
///
/// The bytes are wrapped in an in-memory `std::io::Cursor` (the traversal needs
/// `Read + Seek`) and fed to the headerless footer-based loader entry
/// `iterate_partitions_in_bti_file`. Any structural problem — a too-small file, a
/// root offset past EOF, a malformed node, an attempt to seek past EOF — must
/// surface as `Err`, never a panic. Results are discarded.
pub fn fuzz_bti_traverse(bytes: &[u8]) -> Result<()> {
    let mut cursor = std::io::Cursor::new(bytes);
    let _entries =
        crate::storage::sstable::bti::parser::iterate_partitions_in_bti_file(&mut cursor)?;
    Ok(())
}

/// Feed `block` as a decompressed Data.db block to the real partition loop
/// (`V5CompressedLegacyParser::parse_block_emit`) under one fixed simple schema
/// (`test_basic.simple_table`). Emitted rows are discarded.
///
/// The block-emit path needs a real [`SSTableReader`] (it reads
/// `reader.header.columns` to resolve on-disk columns). A reader is built ONCE,
/// lazily, from the `test_basic/simple_table` fixture located via
/// `CQLITE_DATASETS_ROOT`, and reused across every fuzz iteration. When the
/// fixture is unavailable (e.g. a checkout without fetched datasets) this returns
/// `Err` and the fuzz target no-ops — the never-panic contract still holds; the
/// path is fully exercised whenever the dataset is present (local runs, CI lanes
/// that fetch datasets).
///
/// [`SSTableReader`]: crate::storage::sstable::SSTableReader
pub fn fuzz_block_emit(block: &[u8]) -> Result<()> {
    let ctx = block_emit_context()
        .ok_or_else(|| crate::error::Error::schema("fuzz_block_emit: no simple_table fixture"))?;

    let parser =
        crate::storage::sstable::reader::parsing::row_decoder::V5CompressedLegacyParser::new(
            "test_basic".to_string(),
            "simple_table".to_string(),
            0,
            0,
            None,
        );

    // #3782: the fuzzer's buffer is standalone — no continuation can ever
    // arrive — so `Complete` is the truthful declaration AND the stricter arm to
    // fuzz. Errors are expected here; the contract this target proves is that no
    // input panics, hangs or OOMs.
    parser.parse_block_emit(
        block,
        crate::storage::sstable::reader::parsing::row_decoder::BufferExtent::Complete,
        Some(&ctx.schema),
        &ctx.reader,
        |_entry| Ok(std::ops::ControlFlow::Continue(())),
    )
}

/// Cached reader + schema for [`fuzz_block_emit`].
struct BlockEmitContext {
    reader: crate::storage::sstable::SSTableReader,
    schema: crate::schema::TableSchema,
}

/// Bare `CREATE TABLE` for `test_basic.simple_table` (matches
/// `test-data/schemas/basic-types.cql`) used to build the fixed decode schema.
const SIMPLE_TABLE_DDL: &str = "CREATE TABLE test_basic.simple_table (\
    id UUID PRIMARY KEY, name TEXT, age INT, salary BIGINT, height FLOAT, \
    weight DOUBLE, active BOOLEAN, created TIMESTAMP, birth_date DATE, \
    work_time TIME, description BLOB, account_balance DECIMAL, \
    session_id TIMEUUID, ip_address INET, small_number TINYINT, \
    medium_number SMALLINT, duration_val DURATION, varchar_field VARCHAR, \
    ascii_field ASCII)";

/// Lazily build (once) and return the shared block-emit context, or `None` if
/// the fixture / schema could not be prepared. Never panics.
fn block_emit_context() -> Option<&'static BlockEmitContext> {
    use std::sync::OnceLock;
    static CTX: OnceLock<Option<BlockEmitContext>> = OnceLock::new();
    CTX.get_or_init(build_block_emit_context).as_ref()
}

fn build_block_emit_context() -> Option<BlockEmitContext> {
    let data_file = simple_table_data_file()?;

    // Parse the fixed schema; a parse failure means no context (never panic).
    let schema = match parse_create_table(SIMPLE_TABLE_DDL) {
        Ok((_, schema)) => schema,
        Err(_) => return None,
    };

    // Open the reader on a throwaway current-thread runtime. The runtime is only
    // needed for the async `open`; the subsequent `parse_block_emit` is sync and
    // only reads `reader.header`, so dropping the runtime afterwards is safe.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .ok()?;

    let reader = runtime.block_on(async {
        let config = crate::Config::default();
        let platform = crate::platform::Platform::new(&config).await.ok()?;
        crate::storage::sstable::SSTableReader::open(
            &data_file,
            &config,
            std::sync::Arc::new(platform),
        )
        .await
        .ok()
    })?;

    Some(BlockEmitContext { reader, schema })
}

/// Locate `sstables/test_basic/simple_table-*/*-Data.db` under
/// `CQLITE_DATASETS_ROOT`. Returns `None` (never panics) when the env var is
/// unset or the fixture is absent — this is a fuzz driver, not a test, so a
/// missing fixture is a graceful no-op rather than a hard failure.
fn simple_table_data_file() -> Option<std::path::PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let keyspace_dir = std::path::PathBuf::from(&root)
        .join("sstables")
        .join("test_basic");
    let entries = std::fs::read_dir(&keyspace_dir).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        let is_simple_table = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.starts_with("simple_table-"))
            .unwrap_or(false);
        if !is_simple_table {
            continue;
        }
        if let Ok(inner) = std::fs::read_dir(&path) {
            for df in inner.flatten() {
                let is_data_db = df
                    .file_name()
                    .to_str()
                    .map(|s| s.ends_with("-Data.db"))
                    .unwrap_or(false);
                if is_data_db {
                    return Some(df.path());
                }
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// 6. CommitLog segment parser (issue #2389) — descriptor, frame walk, mutation
//    decode. Contract: arbitrary bytes never panic/hang/OOM.
// ---------------------------------------------------------------------------
use crate::storage::commitlog::descriptor::CommitLogDescriptor;
use crate::storage::commitlog::frame::{FrameStep, FrameWalker};
use crate::storage::commitlog::mutation::decode_mutation;
use crate::storage::commitlog::schema::SchemaSet;

/// Drive every CommitLog parsing layer over arbitrary bytes.
///
/// Exercises (1) descriptor header parsing, (2) the sync-marker/record frame
/// walker, and (3) the mutation-body decoder — each on the raw fuzz input, with
/// a hard iteration cap so a crafted marker chain can never hang the fuzzer.
pub fn fuzz_commitlog_segment(data: &[u8]) {
    // 1. Descriptor header.
    let _ = CommitLogDescriptor::parse(data);

    // 2. Frame walk + mutation decode of each yielded record. Segment id and
    //    body-start are arbitrary here (0) — we are fuzzing the walker's
    //    robustness on hostile bytes, not a real segment's semantics.
    let schemas = SchemaSet::new();
    let mut walker = FrameWalker::new(data, 0, 0);
    let mut guard: u32 = 0;
    while guard < 1_000_000 {
        guard += 1;
        match walker.next_frame() {
            Ok(FrameStep::Record(body)) => {
                let _ = decode_mutation(body, &schemas);
            }
            _ => break,
        }
    }

    // 3. Decode the raw bytes directly as a mutation body.
    let _ = decode_mutation(data, &schemas);
}
