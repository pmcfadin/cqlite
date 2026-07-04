//! Issue #1240 (oracle-driven) — dedicated COMPACTION parity for FROZEN
//! COLLECTIONS that NEST a frozen UDT: `frozen<list<frozen<UDT>>>` and
//! `frozen<map<.., frozen<UDT>>>`.
//!
//! ## Why this slice exists (the gap #1240 closes)
//! #1234 wired the UDT registry into the compaction merge readers and pinned that
//! a `frozen<list<frozen<person>>>` column does not LOSE partitions through
//! compaction — but it only asserted partition/row/non-null-cell COUNTS, never the
//! nested VALUE content (the list elements' UDT field values), and never byte
//! parity. #1020 added whole-table byte parity for `udt_collections` (which does
//! contain these nested columns) but bundles them with plain `frozen<list<int>>` /
//! `frozen<map<text,int>>` columns into one table-level assertion framed around the
//! whole compacted output.
//!
//! Neither has a DEDICATED check that the NESTED-DECODE PATH for a frozen
//! collection nesting a frozen UDT round-trips its element VALUES correctly through
//! the compaction merge. A future regression in that nested-decode path (e.g.
//! decoding the inner UDT fields in the wrong order, dropping a list element, or
//! falling back to a blob) would slip past the count-only #1234 check, and #1020's
//! table-level byte diff would point at the whole Data.db rather than isolating the
//! nested columns. This test closes that gap.
//!
//! ## What it asserts (AC: value round-trip + byte parity where a reference exists)
//! The committed Cassandra 5.0.2 reference fixture
//! `test_compactionparityudt/udt_collections-*` carries exactly the two nested
//! columns this issue names — `lp frozen<list<frozen<person>>>` (a frozen list
//! nesting a UDT) and `ma frozen<map<text, frozen<address>>>` (a frozen map nesting
//! a UDT) — alongside plain `fl frozen<list<int>>` / `fm frozen<map<text,int>>`.
//!
//! CQLite re-produces the SAME two overlapping inputs Cassandra wrote (via its
//! public `WriteEngine` API + a `UdtRegistry` built from the SAME DDL committed in
//! `test-data/schemas/compaction-parity-udt.cql`), runs its own
//! `compact_sstables_with_registry`, then this test asserts three tiers:
//!
//! 1a. **Structural round-trip (the FLOOR).** Decodes BOTH CQLite's compacted
//!     output AND the Cassandra reference through the SAME `SSTableReader`
//!     compaction iterator. For `lp`/`ma` it asserts the OUTER frozen collection
//!     is a structured `List`/`Map` (NOT collapsed to a top-level blob), the
//!     element count + ORDER and the map key set are preserved, and the inner
//!     frozen-UDT element BYTES round-trip and EQUAL the reference. The compaction
//!     reader is byte-preserving for frozen elements, so the inner UDT survives as
//!     its frozen serialized bytes here; a top-level blob fallback or a
//!     dropped/reordered element would FAIL rather than pass silently.
//!
//! 1b. **Typed inner-UDT round-trip.** Pins the TYPED inner person/address field
//!     values (and field order) against the authoritative sstabledump JSONL
//!     golden — the typed half the byte-preserving reader leaves opaque.
//!
//! 2.  **Byte parity (CONDITIONAL — only because a Cassandra reference exists).**
//!     Diffs Data.db/Index.db/Summary.db/Digest.crc32 byte-for-byte against the
//!     committed compacted golden. The nested columns are part of that output, so
//!     byte equality of the whole compacted Data.db is byte parity for the nested
//!     frozen-collection-of-UDT layout against ground-truth Cassandra bytes.
//!
//! ## Dataset doctrine (issue #719)
//! - If `CQLITE_DATASETS_ROOT` is unset OR the reference compacted Data.db is
//!   genuinely absent, the test SKIPS (binaries not fetched). The
//!   `udt_collections` fixture is COMMITTED, so in a normal checkout it runs.
//! - A PRESENT-but-empty / present-but-incomplete fixture is a FAILURE.
//! - `CQLITE_REQUIRE_FIXTURES=1` turns a would-be SKIP into a PANIC.
//!
//! ## No-heuristics (issue #28)
//! The `UdtRegistry` is built from the DDL UDT definitions and passed to BOTH the
//! input writers and `compact_sstables_with_registry`. Decode is schema-driven; a
//! missing/incorrect registry would degrade the nested UDT to a blob and FAIL the
//! structural value assertion rather than silently pass.

#![cfg(feature = "write-support")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{Column, CqlType, KeyColumn, TableSchema, UdtRegistry};
use cqlite_core::storage::sstable::reader::compaction_row::CompactionRowData;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::storage::write_engine::merge::compact_sstables_with_registry;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::{UdtField, UdtFieldDef, UdtTypeDef, UdtValue, Value};
use cqlite_core::Config;
use tempfile::TempDir;

/// Fixed writetimes (micros). MUST match `T_A`/`T_B` in
/// `test-data/scripts/generate-compaction-parity-udt.sh` (same contract as #1020).
const T_A: i64 = 1000;
const T_B: i64 = 2000;

const KEYSPACE: &str = "test_compactionparityudt";
const TABLE: &str = "udt_collections";

/// Issue #1289: the dedicated table whose surviving nested-collection-UDT
/// elements every carry a NULL INNER FIELD (`person.last_name = null`,
/// `address.city = null`) — the one shape `udt_collections` cannot pin.
const TABLE_NULL: &str = "udt_null_inner";

/// Output generation passed to the compactor. Fixed for determinism; affects only
/// the on-disk filename, never component CONTENT bytes. Matches #1020.
const OUT_GENERATION: u64 = 3;

/// Fixed `gc_before` (secs). Irrelevant to output bytes for live cells.
const FIXED_GC_BEFORE: i64 = 1_700_000_000;

/// The two nested-frozen-collection-of-UDT columns this issue targets.
const NESTED_COLS: &[&str] = &["lp", "ma"];

/// Byte-parity components diffed against the Cassandra golden.
const BYTE_FOR_BYTE_COMPONENTS: &[&str] = &["Data.db", "Index.db", "Summary.db", "Digest.crc32"];

// ════════════════════════════════════════════════════════════════════════════
// Fixture resolution (skip-on-absence; present-but-broken is a failure)
// ════════════════════════════════════════════════════════════════════════════

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

fn reference_dir() -> Option<PathBuf> {
    reference_dir_for(TABLE)
}

fn reference_dir_for(table: &str) -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let base = Path::new(&root).join("sstables").join(KEYSPACE);
    let mut matches: Vec<PathBuf> = std::fs::read_dir(&base)
        .ok()?
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            if name.starts_with(&format!("{table}-")) {
                Some(e.path())
            } else {
                None
            }
        })
        .collect();

    match matches.len() {
        0 => None,
        1 => {
            let dir = matches.pop().unwrap();
            if single_data_db(&dir).is_none() {
                panic!(
                    "{KEYSPACE}.{table}: reference directory {dir:?} exists but contains no \
                     compacted nb-*-big-Data.db. Fixture is PRESENT-BUT-INCOMPLETE — regenerate \
                     with: bash test-data/scripts/generate-compaction-parity-udt.sh"
                );
            }
            Some(dir)
        }
        n => panic!(
            "{KEYSPACE}.{table}: found {n} matching `{table}-*` directories under {base:?} — \
             there must be EXACTLY ONE."
        ),
    }
}

fn single_data_db(dir: &Path) -> Option<PathBuf> {
    let mut found: Vec<PathBuf> = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if name.starts_with("nb-") && name.ends_with("-big-Data.db") {
                found.push(e.path());
            }
        }
    }
    match found.len() {
        0 => None,
        1 => Some(found.pop().unwrap()),
        n => panic!("{dir:?}: expected exactly ONE compacted nb-*-big-Data.db, found {n}"),
    }
}

fn descriptor_prefix(data_db: &Path) -> String {
    let name = data_db
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    name.trim_end_matches("Data.db").to_string()
}

fn read_component(dir: &Path, suffix: &str) -> Vec<u8> {
    let data = single_data_db(dir).unwrap_or_else(|| panic!("{dir:?}: no compacted Data.db"));
    let prefix = descriptor_prefix(&data);
    let path = dir.join(format!("{prefix}{suffix}"));
    std::fs::read(&path)
        .unwrap_or_else(|e| panic!("component {path:?} unreadable in a present fixture: {e}"))
}

fn first_diff(a: &[u8], b: &[u8]) -> Option<usize> {
    let n = a.len().max(b.len());
    (0..n).find(|&i| a.get(i) != b.get(i))
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

// ════════════════════════════════════════════════════════════════════════════
// UDT registry + schema (from compaction-parity-udt.cql; passed to writers AND
// the compactor — fail-loud on resolution, no blob fallback)
// ════════════════════════════════════════════════════════════════════════════

fn udt_registry() -> UdtRegistry {
    let mut reg = UdtRegistry::new();
    reg.register_udt(
        UdtTypeDef::new(KEYSPACE.to_string(), "person".to_string())
            .with_field("first_name".to_string(), CqlType::Text, true)
            .with_field("last_name".to_string(), CqlType::Text, true)
            .with_field("age".to_string(), CqlType::Int, true),
    );
    reg.register_udt(
        UdtTypeDef::new(KEYSPACE.to_string(), "address".to_string())
            .with_field("street".to_string(), CqlType::Text, true)
            .with_field("city".to_string(), CqlType::Text, true)
            .with_field("zip".to_string(), CqlType::Text, true),
    );
    reg
}

fn col(name: &str, ty: &str, nullable: bool) -> Column {
    Column {
        name: name.into(),
        data_type: ty.into(),
        nullable,
        default: None,
        is_static: false,
    }
}

fn collections_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.into(),
        table: TABLE.into(),
        partition_keys: vec![KeyColumn {
            name: "id".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            col("id", "int", false),
            col("fl", "frozen<list<int>>", true),
            col("fm", "frozen<map<text, int>>", true),
            col("lp", "frozen<list<frozen<person>>>", true),
            col("ma", "frozen<map<text, frozen<address>>>", true),
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

// ── Value builders (must match the #1020 fixture inputs exactly) ──────────────

fn person_inner(first: &str, last: &str, age: i32) -> Value {
    Value::Udt(UdtValue {
        type_name: "person".into(),
        keyspace: KEYSPACE.into(),
        fields: vec![
            UdtField {
                name: "first_name".into(),
                value: Some(Value::Text(first.into())),
            },
            UdtField {
                name: "last_name".into(),
                value: Some(Value::Text(last.into())),
            },
            UdtField {
                name: "age".into(),
                value: Some(Value::Integer(age)),
            },
        ],
    })
}

fn address_inner(street: &str, city: Option<&str>, zip: &str) -> Value {
    Value::Udt(UdtValue {
        type_name: "address".into(),
        keyspace: KEYSPACE.into(),
        fields: vec![
            UdtField {
                name: "street".into(),
                value: Some(Value::Text(street.into())),
            },
            UdtField {
                name: "city".into(),
                value: city.map(|s| Value::Text(s.into())),
            },
            UdtField {
                name: "zip".into(),
                value: Some(Value::Text(zip.into())),
            },
        ],
    })
}

fn flist(ns: &[i32]) -> Value {
    Value::Frozen(Box::new(Value::List(
        ns.iter().copied().map(Value::Integer).collect(),
    )))
}

fn fmap(kvs: &[(&str, i32)]) -> Value {
    Value::Frozen(Box::new(Value::Map(
        kvs.iter()
            .map(|(k, v)| (Value::Text((*k).into()), Value::Integer(*v)))
            .collect(),
    )))
}

fn flist_persons(ps: Vec<Value>) -> Value {
    Value::Frozen(Box::new(Value::List(ps)))
}

fn fmap_addrs(kvs: Vec<(&str, Value)>) -> Value {
    Value::Frozen(Box::new(Value::Map(
        kvs.into_iter()
            .map(|(k, v)| (Value::Text(k.into()), v))
            .collect(),
    )))
}

fn op(column: &str, value: Value) -> CellOperation {
    CellOperation::Write {
        column: column.into(),
        value,
    }
}

fn write_row(id: i32, ops: Vec<CellOperation>, ts: i64) -> Mutation {
    write_row_table(TABLE, id, ops, ts)
}

fn write_row_table(table: &str, id: i32, ops: Vec<CellOperation>, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KEYSPACE, table),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        ops,
        ts,
        None,
    )
}

/// The SAME two overlapping input groups #1020's `udt_collections` scenario uses,
/// so CQLite reproduces the exact inputs Cassandra wrote for the committed golden.
fn collections_groups() -> (Vec<Mutation>, Vec<Mutation>) {
    let group_a = vec![
        write_row(
            1,
            vec![
                op("fl", flist(&[1, 2, 3])),
                op("fm", fmap(&[("x", 10), ("y", 20)])),
                op(
                    "lp",
                    flist_persons(vec![person_inner("Ada", "Lovelace", 36)]),
                ),
                op(
                    "ma",
                    fmap_addrs(vec![(
                        "home",
                        address_inner("1 Navy Way", Some("Arlington"), "22201"),
                    )]),
                ),
            ],
            T_A,
        ),
        write_row(
            2,
            vec![
                op("fl", flist(&[9])),
                op("fm", fmap(&[("z", 99)])),
                op("lp", flist_persons(vec![person_inner("Old", "Val", 1)])),
                op(
                    "ma",
                    fmap_addrs(vec![("k", address_inner("old", Some("old"), "0"))]),
                ),
            ],
            T_A,
        ),
    ];
    let group_b = vec![
        write_row(
            2,
            vec![
                op("fl", flist(&[4, 5])),
                op("fm", fmap(&[("a", 1), ("b", 2)])),
                op(
                    "lp",
                    flist_persons(vec![
                        person_inner("Grace", "Hopper", 85),
                        person_inner("Alan", "Turing", 41),
                    ]),
                ),
                op(
                    "ma",
                    fmap_addrs(vec![(
                        "office",
                        address_inner("9 Apollo", Some("Hampton"), "23666"),
                    )]),
                ),
            ],
            T_B,
        ),
        write_row(
            3,
            vec![
                op("fl", flist(&[7, 8, 9])),
                op("fm", fmap(&[("q", 1)])),
                op(
                    "lp",
                    flist_persons(vec![person_inner("Katherine", "Johnson", 101)]),
                ),
                op(
                    "ma",
                    fmap_addrs(vec![(
                        "h",
                        address_inner("9 Apollo", Some("Hampton"), "23666"),
                    )]),
                ),
            ],
            T_B,
        ),
    ];
    (group_a, group_b)
}

// ── Issue #1289: null-inner-field nested-collection-UDT scenario ──────────────

/// Schema for `udt_null_inner` (matches compaction-parity-udt.cql): only the two
/// nested-collection-of-UDT columns, no plain `fl`/`fm`.
fn null_inner_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.into(),
        table: TABLE_NULL.into(),
        partition_keys: vec![KeyColumn {
            name: "id".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            col("id", "int", false),
            col("lp", "frozen<list<frozen<person>>>", true),
            col("ma", "frozen<map<text, frozen<address>>>", true),
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

/// The SAME two overlapping input groups the generator's `insert_null_inner`
/// writes, so CQLite reproduces the exact inputs Cassandra wrote. Every
/// SURVIVING element carries a null inner field: list persons have
/// `last_name = null`, map addresses have `city = null`.
fn null_inner_groups() -> (Vec<Mutation>, Vec<Mutation>) {
    let row = |id, ops, ts| write_row_table(TABLE_NULL, id, ops, ts);
    let group_a = vec![
        row(
            1,
            vec![
                op("lp", flist_persons(vec![person_null_last("Ada", 36)])),
                op(
                    "ma",
                    fmap_addrs(vec![("home", address_inner("1 Navy Way", None, "22201"))]),
                ),
            ],
            T_A,
        ),
        row(
            2,
            vec![
                op("lp", flist_persons(vec![person_inner("Old", "Val", 1)])),
                op(
                    "ma",
                    fmap_addrs(vec![("k", address_inner("old", Some("old"), "0"))]),
                ),
            ],
            T_A,
        ),
    ];
    let group_b = vec![
        row(
            2,
            vec![
                op(
                    "lp",
                    flist_persons(vec![
                        person_null_last("Grace", 85),
                        person_null_last("Alan", 41),
                    ]),
                ),
                op(
                    "ma",
                    fmap_addrs(vec![("office", address_inner("9 Apollo", None, "23666"))]),
                ),
            ],
            T_B,
        ),
        row(
            3,
            vec![
                op(
                    "lp",
                    flist_persons(vec![person_null_last("Katherine", 101)]),
                ),
                op(
                    "ma",
                    fmap_addrs(vec![("h", address_inner("9 Apollo", None, "23666"))]),
                ),
            ],
            T_B,
        ),
    ];
    (group_a, group_b)
}

/// A `person` with a NULL `last_name` middle field (the absent-field encoding
/// under test on the winning side of the merge).
fn person_null_last(first: &str, age: i32) -> Value {
    Value::Udt(UdtValue {
        type_name: "person".into(),
        keyspace: KEYSPACE.into(),
        fields: vec![
            UdtField {
                name: "first_name".into(),
                value: Some(Value::Text(first.into())),
            },
            UdtField {
                name: "last_name".into(),
                value: None,
            },
            UdtField {
                name: "age".into(),
                value: Some(Value::Integer(age)),
            },
        ],
    })
}

// ════════════════════════════════════════════════════════════════════════════
// Input building + compaction (CQLite candidate, registry-aware)
// ════════════════════════════════════════════════════════════════════════════

async fn cqlite_compact(group_a: Vec<Mutation>, group_b: Vec<Mutation>) -> (TempDir, PathBuf) {
    cqlite_compact_schema(collections_schema(), group_a, group_b).await
}

async fn cqlite_compact_schema(
    schema: TableSchema,
    group_a: Vec<Mutation>,
    group_b: Vec<Mutation>,
) -> (TempDir, PathBuf) {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("inputs");
    let wal_dir = temp.path().join("wal");
    let out_dir = temp.path().join("out");

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema.clone())
        .with_udt_registry(udt_registry());
    let mut engine = WriteEngine::new(config).expect("engine");

    for m in group_a {
        engine.write(m).expect("write A");
    }
    engine.flush().await.expect("flush A").expect("info A");

    for m in group_b {
        engine.write(m).expect("write B");
    }
    engine.flush().await.expect("flush B").expect("info B");
    engine.close().await.expect("close engine");

    let inputs = discover_inputs(&data_dir);
    assert_eq!(
        inputs.len(),
        2,
        "expected exactly 2 input SSTables, got {inputs:?}"
    );

    let registry = udt_registry();
    let report = compact_sstables_with_registry(
        inputs,
        &out_dir,
        &schema,
        OUT_GENERATION,
        Some(FIXED_GC_BEFORE),
        None,
        true,
        Some(&registry),
    )
    .await
    .expect("compaction must succeed");

    let table_dir = report
        .output
        .data_path
        .parent()
        .expect("data parent")
        .to_path_buf();
    (temp, table_dir)
}

fn discover_inputs(dir: &Path) -> Vec<PathBuf> {
    let mut found: Vec<(u64, PathBuf)> = Vec::new();
    collect(dir, &mut found, 8);
    found.sort_by(|a, b| b.0.cmp(&a.0));
    found.into_iter().map(|(_, p)| p).collect()
}

fn collect(dir: &Path, out: &mut Vec<(u64, PathBuf)>, depth: usize) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        if name.starts_with("nb-") && name.ends_with("-big-Data.db") {
            let base = name.trim_end_matches("-Data.db");
            if !path.with_file_name(format!("{base}-TOC.txt")).exists() {
                continue;
            }
            let generation = name
                .strip_prefix("nb-")
                .and_then(|s| s.split("-big-").next())
                .and_then(|g| g.parse::<u64>().ok())
                .unwrap_or(0);
            out.push((generation, path));
        } else if depth > 0 && path.is_dir() {
            collect(&path, out, depth - 1);
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Typed decode of a compacted SSTable (the nested-decode path under test)
// ════════════════════════════════════════════════════════════════════════════

/// `(pk int as string, cell_name) -> decoded Value` for every live cell of a
/// compacted SSTable directory, via the public `SSTableReader` compaction
/// iterator. `Value::Frozen` wrappers are peeled so a frozen and bare value of the
/// same content compare equal.
async fn decode_typed_cell_map(
    dir: &Path,
    schema: &TableSchema,
) -> BTreeMap<(String, String), Value> {
    let data_path =
        single_data_db(dir).unwrap_or_else(|| panic!("no compacted Data.db in {dir:?}"));
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("platform init for decode"),
    );
    let reader = SSTableReader::open(&data_path, &config, platform)
        .await
        .unwrap_or_else(|e| panic!("open {data_path:?} for decode failed: {e}"));
    let rows = reader
        .iterate_all_partitions_for_compaction(Some(schema))
        .await
        .unwrap_or_else(|e| panic!("compaction iterate of {data_path:?} failed: {e}"));

    let mut map: BTreeMap<(String, String), Value> = BTreeMap::new();
    for row in &rows {
        let pk = pk_repr(&row.key.0);
        if let CompactionRowData::Live { simple, .. } = &row.row_data {
            for cell in simple {
                if matches!(cell.value, Value::Tombstone(_)) {
                    continue;
                }
                map.insert((pk.clone(), cell.column.clone()), peel_frozen(&cell.value));
            }
        }
    }
    map
}

fn pk_repr(bytes: &[u8]) -> String {
    if bytes.len() == 4 {
        i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]).to_string()
    } else {
        hex(bytes)
    }
}

fn peel_frozen(v: &Value) -> Value {
    match v {
        Value::Frozen(inner) => peel_frozen(inner),
        other => other.clone(),
    }
}

/// Same as [`decode_typed_cell_map`] but wires a `UdtRegistry` (DDL-built person /
/// address defs) into the reader BEFORE decoding. Used by the #1340 equivalence
/// test to prove the header-marshal decode (registry-less) equals the registry
/// decode (spec Req 2 scenario 1).
async fn decode_typed_cell_map_with_registry(
    dir: &Path,
    schema: &TableSchema,
    registry: UdtRegistry,
) -> BTreeMap<(String, String), Value> {
    let data_path =
        single_data_db(dir).unwrap_or_else(|| panic!("no compacted Data.db in {dir:?}"));
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("platform init for decode"),
    );
    let mut reader = SSTableReader::open(&data_path, &config, platform)
        .await
        .unwrap_or_else(|e| panic!("open {data_path:?} for decode failed: {e}"));
    reader.set_udt_registry(registry);
    let rows = reader
        .iterate_all_partitions_for_compaction(Some(schema))
        .await
        .unwrap_or_else(|e| panic!("compaction iterate of {data_path:?} failed: {e}"));

    let mut map: BTreeMap<(String, String), Value> = BTreeMap::new();
    for row in &rows {
        let pk = pk_repr(&row.key.0);
        if let CompactionRowData::Live { simple, .. } = &row.row_data {
            for cell in simple {
                if matches!(cell.value, Value::Tombstone(_)) {
                    continue;
                }
                map.insert((pk.clone(), cell.column.clone()), peel_frozen(&cell.value));
            }
        }
    }
    map
}

/// A `text` UDT field def (nullable) for the equivalence-test registry.
fn tfield(name: &str) -> UdtFieldDef {
    UdtFieldDef {
        name: name.into(),
        field_type: CqlType::Text,
        nullable: true,
    }
}

/// DDL-equivalent `person` + `address` UDT registry for `test_compactionparityudt`.
fn collections_udt_registry() -> UdtRegistry {
    let mut reg = UdtRegistry::new();
    reg.register_udt(UdtTypeDef {
        keyspace: KEYSPACE.into(),
        name: "person".into(),
        fields: vec![
            tfield("first_name"),
            tfield("last_name"),
            UdtFieldDef {
                name: "age".into(),
                field_type: CqlType::Int,
                nullable: true,
            },
        ],
    });
    reg.register_udt(UdtTypeDef {
        keyspace: KEYSPACE.into(),
        name: "address".into(),
        fields: vec![tfield("street"), tfield("city"), tfield("zip")],
    });
    reg
}

// ════════════════════════════════════════════════════════════════════════════
// Structural nested-decode assertions (the dedicated #1240 value round-trip)
//
// Decode contract of the compaction reader (compaction_row.rs): the OUTER frozen
// collection is decoded STRUCTURALLY into a `List`/`Map` (proving it is NOT
// collapsed to a top-level blob). After issue #1340 each INNER `frozen<UDT>`
// element is ALSO decoded to a typed `Value::Udt` from the file's own on-disk
// SerializationHeader marshal type (registry-free), so the structural floor we
// assert via this path is: outer collection shape + element count/order + the
// per-element TYPED VALUE ROUND-TRIP and EQUALITY with the Cassandra reference
// (both decoded through the SAME reader, so equal typed `Value`s prove the
// header-marshal decode is identical across CQLite's output and Cassandra's).
// The typed inner-UDT FIELD values (first_name/zip/…) are additionally pinned
// against CQLite's OWN decode and the committed sstabledump JSONL golden below.
// ════════════════════════════════════════════════════════════════════════════

/// One decoded collection element, peeling `Frozen`. After #1340 an inner
/// `frozen<UDT>` decodes to a typed `Value::Udt`; an element the reader cannot
/// resolve stays a `Value::Blob`. Either way we return the peeled typed value so
/// `ours` and the Cassandra `reference` (decoded through the SAME reader) compare
/// equal at the typed level.
fn element_value(el: &Value) -> Value {
    peel_frozen(el)
}

/// Assert a decoded `lp` cell is a structured `List` (NOT a top-level blob) and
/// return its per-element typed values in order. Element COUNT + ORDER is the
/// nested-collection structure under test.
///
/// `lp` is `frozen<list<frozen<person>>>`, so we pin it STRICTLY as `Value::List`:
/// a regression that decoded an ordered list as a `Value::Set` would silently
/// pass an `List | Set` match yet drop the list's order guarantee, so a `Set`
/// (or any other variant) is a hard failure here.
fn assert_list_structure(pk: &str, v: &Value) -> Vec<Value> {
    let list = match peel_frozen(v) {
        Value::List(items) => items,
        Value::Set(_) => panic!(
            "lp[pk={pk}]: nested-decode produced a Value::Set, expected a Value::List — \
             `lp` is frozen<list<frozen<person>>> and its element ORDER is under test; \
             a Set has no order guarantee and must NOT satisfy this assertion"
        ),
        other => panic!(
            "lp[pk={pk}]: nested-decode produced {other:?}, expected a structured List \
             (top-level blob fallback / wrong outer-collection decode)"
        ),
    };
    assert!(
        !list.is_empty(),
        "lp[pk={pk}]: decoded to an EMPTY list — nested element decode dropped UDTs"
    );
    list.iter().map(element_value).collect()
}

/// Assert a decoded `ma` cell is a structured `Map<text, _>` (NOT a top-level
/// blob) and return `key -> typed-value` sorted by key.
fn assert_map_structure(pk: &str, v: &Value) -> BTreeMap<String, Value> {
    let entries = match peel_frozen(v) {
        Value::Map(m) => m,
        other => panic!(
            "ma[pk={pk}]: nested-decode produced {other:?}, expected a structured Map \
             (top-level blob fallback / wrong outer-collection decode)"
        ),
    };
    assert!(
        !entries.is_empty(),
        "ma[pk={pk}]: decoded to an EMPTY map — nested value decode dropped UDTs"
    );
    let mut out = BTreeMap::new();
    for (k, val) in entries {
        let key = match peel_frozen(&k) {
            Value::Text(s) => s,
            other => panic!("ma[pk={pk}]: map key is {other:?}, expected text"),
        };
        out.insert(key, element_value(&val));
    }
    out
}

// ── Typed inner-UDT field accessors (issue #1340: assert CQLite's OWN decode) ──

/// Peel to the inner `UdtValue` (through any `Frozen` wrappers). Panics loudly if
/// the element did NOT decode to a typed UDT — that is the #1340 regression guard.
fn as_udt(v: &Value) -> &UdtValue {
    match v {
        Value::Udt(u) => u,
        Value::Frozen(inner) => as_udt(inner),
        other => panic!(
            "expected a typed Value::Udt (issue #1340 typed inner-UDT decode), got {other:?}"
        ),
    }
}

/// Read a text (or explicit-null) UDT field by name from CQLite's OWN decode.
fn udt_text(u: &UdtValue, field: &str) -> Option<String> {
    match u.fields.iter().find(|f| f.name == field) {
        Some(f) => match &f.value {
            Some(Value::Text(s)) => Some(s.clone()),
            None => None,
            Some(other) => panic!(
                "UDT '{}' field '{field}': expected text or null, got {other:?}",
                u.type_name
            ),
        },
        None => panic!("UDT '{}' missing field '{field}'", u.type_name),
    }
}

/// Read an int UDT field by name from CQLite's OWN decode.
fn udt_int(u: &UdtValue, field: &str) -> i32 {
    match u.fields.iter().find(|f| f.name == field).map(|f| &f.value) {
        Some(Some(Value::Integer(i))) => *i,
        other => panic!(
            "UDT '{}' field '{field}': expected int, got {other:?}",
            u.type_name
        ),
    }
}

// ── Typed inner-UDT pinning via the committed sstabledump JSONL golden ────────

/// `(pk_repr, cell_name) -> canonical JSON value string` decoded by sstabledump,
/// the authoritative TYPED view of the inner UDT fields. Reading this golden pins
/// that the nested person/address fields decode to the right typed values (field
/// order, null fields) — the typed half of the value round-trip that the
/// byte-preserving compaction reader intentionally leaves opaque.
fn jsonl_typed_cells(ref_dir: &Path) -> BTreeMap<(String, String), String> {
    let data = single_data_db(ref_dir).expect("compacted Data.db");
    let jsonl = ref_dir.join(format!("{}Data.db.jsonl", descriptor_prefix(&data)));
    let text = std::fs::read_to_string(&jsonl)
        .unwrap_or_else(|e| panic!("committed JSONL golden {jsonl:?} unreadable: {e}"));
    let lines: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();
    assert!(
        !lines.is_empty(),
        "committed JSONL golden is present-but-empty — broken golden"
    );

    let mut seen: BTreeMap<(String, String), String> = BTreeMap::new();
    for (i, line) in lines.iter().enumerate() {
        let jv: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("JSONL partition {i} is not valid JSON: {e}"));
        let pk = jv
            .get("partition")
            .and_then(|p| p.get("key"))
            .and_then(|k| k.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|x| {
                        x.as_str()
                            .map(String::from)
                            .unwrap_or_else(|| x.to_string())
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        let rows = jv.get("rows").and_then(|r| r.as_array());
        assert!(
            rows.is_some_and(|r| !r.is_empty()),
            "JSONL partition {i} (pk={pk:?}) has no rows — broken golden"
        );
        for row in rows.unwrap() {
            if let Some(cells) = row.get("cells").and_then(|c| c.as_array()) {
                for cell in cells {
                    let name = cell
                        .get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or_default();
                    if let Some(val) = cell.get("value") {
                        seen.insert((pk.clone(), name.to_string()), val.to_string());
                    }
                }
            }
        }
    }
    seen
}

// ════════════════════════════════════════════════════════════════════════════
// THE TEST
// ════════════════════════════════════════════════════════════════════════════

/// Dedicated compaction parity for `frozen<list<frozen<UDT>>>` (`lp`) and
/// `frozen<map<.., frozen<UDT>>>` (`ma`).
///
/// FLOOR: nested-decode value round-trip (CQLite output == Cassandra reference,
/// each decoded as structured `List<Udt>` / `Map<text,Udt>`, NOT a blob).
/// CONDITIONAL: byte parity of the compacted output vs the committed Cassandra
/// 5.0.2 golden (a reference exists for this fixture, so byte parity is asserted).
#[tokio::test]
async fn nested_frozen_collection_of_udt_compaction_parity() {
    let Some(ref_dir) = reference_dir() else {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but the compacted reference for {KEYSPACE}.{TABLE} is \
                 absent; generate with bash test-data/scripts/generate-compaction-parity-udt.sh"
            );
        }
        eprintln!(
            "[issue_1240] reference for {KEYSPACE}.{TABLE} absent (dataset not fetched); skipping"
        );
        return;
    };

    let schema = collections_schema();
    let (group_a, group_b) = collections_groups();
    let (_guard, out_dir) = cqlite_compact(group_a, group_b).await;

    // ── 1a. Structural nested round-trip via the compaction reader (FLOOR) ────
    // Decode BOTH CQLite's compacted output AND the Cassandra reference through
    // the SAME reader and assert, for `lp`/`ma`: the outer frozen collection is a
    // structured List/Map (NOT a top-level blob), element count/ORDER and map keys
    // are preserved, and the inner frozen-UDT element BYTES round-trip and EQUAL
    // the reference. A top-level blob fallback would not produce a List/Map; a
    // dropped/reordered element or a corrupted inner-UDT byte run would mismatch.
    let reference = decode_typed_cell_map(&ref_dir, &schema).await;
    let ours = decode_typed_cell_map(&out_dir, &schema).await;

    assert!(
        !ours.is_empty(),
        "CQLite compacted output decoded to ZERO typed cells — blob fallback / unreadable output"
    );
    assert!(
        !reference.is_empty(),
        "Cassandra reference decoded to ZERO typed cells — broken golden"
    );

    // Expected element/key shape after last-write-wins compaction. PROVES real
    // rows were decoded (not 0-rows-when-present) and pins list element count and
    // map key set per surviving partition.
    let expected_lp_len: BTreeMap<&str, usize> = BTreeMap::from([("1", 1), ("2", 2), ("3", 1)]);
    let expected_ma_keys: BTreeMap<&str, &str> =
        BTreeMap::from([("1", "home"), ("2", "office"), ("3", "h")]);

    let mut checked_lp = 0usize;
    let mut checked_ma = 0usize;

    for pk in ["1", "2", "3"] {
        // lp: frozen<list<frozen<person>>>
        let our_lp = ours
            .get(&(pk.to_string(), "lp".to_string()))
            .unwrap_or_else(|| panic!("CQLite output missing lp cell for pk={pk}"));
        let ref_lp = reference
            .get(&(pk.to_string(), "lp".to_string()))
            .unwrap_or_else(|| panic!("Cassandra reference missing lp cell for pk={pk}"));

        let our_list = assert_list_structure(pk, our_lp);
        let ref_list = assert_list_structure(pk, ref_lp);
        assert_eq!(
            our_list.len(),
            expected_lp_len[pk],
            "lp[pk={pk}]: decoded list element count {} != expected {}",
            our_list.len(),
            expected_lp_len[pk]
        );
        assert_eq!(
            our_list, ref_list,
            "lp[pk={pk}]: CQLite nested frozen<list<frozen<person>>> typed element values \
             (count/order) != Cassandra reference"
        );
        checked_lp += 1;

        // ma: frozen<map<text, frozen<address>>>
        let our_ma = ours
            .get(&(pk.to_string(), "ma".to_string()))
            .unwrap_or_else(|| panic!("CQLite output missing ma cell for pk={pk}"));
        let ref_ma = reference
            .get(&(pk.to_string(), "ma".to_string()))
            .unwrap_or_else(|| panic!("Cassandra reference missing ma cell for pk={pk}"));

        let our_map = assert_map_structure(pk, our_ma);
        let ref_map = assert_map_structure(pk, ref_ma);
        assert!(
            our_map.contains_key(expected_ma_keys[pk]),
            "ma[pk={pk}]: decoded map missing expected key '{}' (have {:?})",
            expected_ma_keys[pk],
            our_map.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            our_map, ref_map,
            "ma[pk={pk}]: CQLite nested frozen<map<text,frozen<address>>> key set + typed value \
             values != Cassandra reference"
        );
        checked_ma += 1;
    }

    assert_eq!(checked_lp, 3, "expected to check lp on 3 partitions");
    assert_eq!(checked_ma, 3, "expected to check ma on 3 partitions");

    eprintln!(
        "[issue_1240] STRUCTURAL ROUND-TRIP PASS — nested {NESTED_COLS:?} columns decoded as \
         structured List/Map (outer-collection shape + element count/order + typed inner frozen-UDT \
         element values) from CQLite's compacted output and matched the Cassandra reference on all \
         3 partitions ({checked_lp} lp + {checked_ma} ma)."
    );

    // ── 1b. TYPED inner-UDT field values from CQLite's OWN decode (issue #1340) ─
    // The header-marshal decode (registry-free compaction reader) must produce the
    // right typed person/address FIELDS. Assert CQLite's OWN decoded `ours` cells
    // field-by-field against the values the sstabledump golden carries (the golden
    // itself is self-checked below). A blob fallback / wrong inner-UDT field decode
    // would fail `as_udt` or mismatch a field here.
    #[allow(clippy::type_complexity)]
    let expect_lp: &[(&str, &[(&str, &str, i32)])] = &[
        ("1", &[("Ada", "Lovelace", 36)]),
        ("2", &[("Grace", "Hopper", 85), ("Alan", "Turing", 41)]),
        ("3", &[("Katherine", "Johnson", 101)]),
    ];
    for (pk, people) in expect_lp {
        let list = assert_list_structure(
            pk,
            ours.get(&(pk.to_string(), "lp".to_string()))
                .unwrap_or_else(|| panic!("CQLite output missing lp cell for pk={pk}")),
        );
        assert_eq!(list.len(), people.len(), "lp[pk={pk}]: person count");
        for (el, (first, last, age)) in list.iter().zip(people.iter()) {
            let u = as_udt(el);
            assert_eq!(
                udt_text(u, "first_name").as_deref(),
                Some(*first),
                "lp[pk={pk}] first_name"
            );
            assert_eq!(
                udt_text(u, "last_name").as_deref(),
                Some(*last),
                "lp[pk={pk}] last_name"
            );
            assert_eq!(udt_int(u, "age"), *age, "lp[pk={pk}] age");
        }
    }
    let expect_ma: &[(&str, &str, (&str, &str, &str))] = &[
        ("1", "home", ("1 Navy Way", "Arlington", "22201")),
        ("2", "office", ("9 Apollo", "Hampton", "23666")),
        ("3", "h", ("9 Apollo", "Hampton", "23666")),
    ];
    for (pk, key, (street, city, zip)) in expect_ma {
        let map = assert_map_structure(
            pk,
            ours.get(&(pk.to_string(), "ma".to_string()))
                .unwrap_or_else(|| panic!("CQLite output missing ma cell for pk={pk}")),
        );
        let val = map
            .get(*key)
            .unwrap_or_else(|| panic!("ma[pk={pk}]: missing key '{key}'"));
        let u = as_udt(val);
        assert_eq!(
            udt_text(u, "street").as_deref(),
            Some(*street),
            "ma[pk={pk}] street"
        );
        assert_eq!(
            udt_text(u, "city").as_deref(),
            Some(*city),
            "ma[pk={pk}] city"
        );
        assert_eq!(udt_text(u, "zip").as_deref(), Some(*zip), "ma[pk={pk}] zip");
    }
    eprintln!(
        "[issue_1240] TYPED INNER-UDT (OWN DECODE) PASS — CQLite's registry-free decode produced \
         typed person/address fields for all nested-column cells."
    );

    // ── 1c. TYPED inner-UDT value round-trip via the sstabledump JSONL golden ─
    // Pins the TYPED inner fields against the authoritative sstabledump golden:
    // a blob fallback or wrong inner-UDT field decode would not produce these
    // structured typed values (incl. field order). Every committed `ma` address
    // here carries a present `city`; the fixture does not exercise a null nested
    // field, so this does not claim null-field coverage.
    let typed = jsonl_typed_cells(&ref_dir);
    let expected_typed: &[(&str, &str, &str)] = &[
        (
            "1",
            "lp",
            r#"[{"first_name":"Ada","last_name":"Lovelace","age":36}]"#,
        ),
        (
            "2",
            "lp",
            r#"[{"first_name":"Grace","last_name":"Hopper","age":85},{"first_name":"Alan","last_name":"Turing","age":41}]"#,
        ),
        (
            "3",
            "lp",
            r#"[{"first_name":"Katherine","last_name":"Johnson","age":101}]"#,
        ),
        (
            "1",
            "ma",
            r#"{"home":{"street":"1 Navy Way","city":"Arlington","zip":"22201"}}"#,
        ),
        (
            "2",
            "ma",
            r#"{"office":{"street":"9 Apollo","city":"Hampton","zip":"23666"}}"#,
        ),
        (
            "3",
            "ma",
            r#"{"h":{"street":"9 Apollo","city":"Hampton","zip":"23666"}}"#,
        ),
    ];
    for (pk, col, want) in expected_typed {
        let got = typed
            .get(&(pk.to_string(), col.to_string()))
            .unwrap_or_else(|| panic!("JSONL golden missing typed {col} for pk={pk}"));
        assert_eq!(
            got, want,
            "{col}[pk={pk}]: typed inner-UDT decode in the sstabledump golden does not match \
             expected nested value"
        );
    }
    eprintln!(
        "[issue_1240] TYPED INNER-UDT PASS — sstabledump golden pins the inner person/address \
         field values (incl. field order) for all {} nested-column cells.",
        expected_typed.len()
    );

    // ── 2. Byte parity (CONDITIONAL — a Cassandra reference exists) ──────────
    // The nested columns are part of the compacted Data.db, so byte equality of
    // the whole compacted output IS byte parity for the nested frozen-collection-
    // of-UDT layout against ground-truth Cassandra 5.0.2 bytes.
    for suffix in BYTE_FOR_BYTE_COMPONENTS {
        let reference = read_component(&ref_dir, suffix);
        let mine = read_component(&out_dir, suffix);
        assert!(
            !reference.is_empty(),
            "reference {suffix} is present-but-empty — broken golden"
        );
        if reference != mine {
            let at = first_diff(&reference, &mine);
            panic!(
                "{suffix} byte mismatch (cass={} ours={} bytes, first diff at {at:?})\n  \
                 cass={}\n  ours={}",
                reference.len(),
                mine.len(),
                hex(&reference),
                hex(&mine),
            );
        }
    }

    eprintln!(
        "[issue_1240] BYTE PARITY PASS — {BYTE_FOR_BYTE_COMPONENTS:?} of the compacted output are \
         byte-identical to the Cassandra 5.0.2 reference (nested frozen-collection-of-UDT layout)."
    );
}

/// Issue #1340 — equivalence: the fixture decoded via the header-marshal
/// mechanism (registry-LESS reader) equals the decode via the existing
/// `UdtRegistry` mechanism (spec Req 2 scenario 1). Same reader, same schema; the
/// only difference is whether a DDL-built registry is wired. If the two decodes
/// disagree, the header-marshal path is not producing the identical typed
/// `Value::Udt` the registry path produces.
#[tokio::test]
async fn nested_frozen_collection_marshal_equals_registry_decode() {
    let Some(ref_dir) = reference_dir() else {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but the compacted reference for {KEYSPACE}.{TABLE} is \
                 absent; generate with bash test-data/scripts/generate-compaction-parity-udt.sh"
            );
        }
        eprintln!(
            "[issue_1340] reference for {KEYSPACE}.{TABLE} absent (dataset not fetched); skipping"
        );
        return;
    };

    let schema = collections_schema();
    let marshal_only = decode_typed_cell_map(&ref_dir, &schema).await;
    let registry_wired =
        decode_typed_cell_map_with_registry(&ref_dir, &schema, collections_udt_registry()).await;

    assert!(
        !marshal_only.is_empty(),
        "registry-less decode produced ZERO cells — fixture present-but-zero-rows is a FAILURE"
    );
    assert_eq!(
        marshal_only, registry_wired,
        "issue #1340: header-marshal (registry-less) decode must equal the UdtRegistry decode"
    );
    // Prove the inner elements are actually TYPED (not both equally-Blob).
    let lp = marshal_only
        .get(&("1".to_string(), "lp".to_string()))
        .expect("lp cell for pk=1");
    let first = assert_list_structure("1", lp);
    let _ = as_udt(&first[0]); // panics if not a typed UDT
    eprintln!(
        "[issue_1340] EQUIVALENCE PASS — registry-less header-marshal decode == UdtRegistry decode \
         (typed Value::Udt inner elements) across all nested-column cells."
    );
}

// ════════════════════════════════════════════════════════════════════════════
// Issue #1289 — null INNER FIELD inside a nested-collection UDT
//
// The committed `udt_collections` golden carries a present `city` in every `ma`
// address, so it cannot pin the one shape that matters here: a SURVIVING
// nested-collection-UDT element whose inner field is NULL. The dedicated
// `udt_null_inner` fixture closes that gap — every surviving element has
// `person.last_name = null` (in `lp`) and `address.city = null` (in `ma`),
// exercising the `-1` absent-field encoding of a frozen UDT nested inside a
// frozen collection on the WINNING side of a compaction merge.
//
// Fail-closed: SKIPS only on genuine dataset absence (binaries not fetched);
// a PRESENT-but-empty / present-but-incomplete fixture, a decoded 0-cell output,
// or a non-null inner field is a FAILURE. CQLITE_REQUIRE_FIXTURES=1 turns the
// would-be SKIP into a PANIC.
// ════════════════════════════════════════════════════════════════════════════

/// Issue #1289: null-inner-field parity for `udt_null_inner`. Same three tiers as
/// the `udt_collections` test (structural round-trip + byte parity + typed JSONL
/// pin), but the typed pin asserts the NULL inner field is preserved through the
/// compaction merge and decodes to `null` in the authoritative sstabledump golden.
#[tokio::test]
async fn nested_collection_udt_null_inner_field_compaction_parity() {
    let Some(ref_dir) = reference_dir_for(TABLE_NULL) else {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but the compacted reference for {KEYSPACE}.{TABLE_NULL} \
                 is absent; generate with bash test-data/scripts/generate-compaction-parity-udt.sh"
            );
        }
        eprintln!(
            "[issue_1289] reference for {KEYSPACE}.{TABLE_NULL} absent (dataset not fetched); \
             skipping"
        );
        return;
    };

    let schema = null_inner_schema();
    let (group_a, group_b) = null_inner_groups();
    let (_guard, out_dir) = cqlite_compact_schema(schema.clone(), group_a, group_b).await;

    // ── 1a. Structural nested round-trip via the compaction reader (FLOOR) ────
    let reference = decode_typed_cell_map(&ref_dir, &schema).await;
    let ours = decode_typed_cell_map(&out_dir, &schema).await;
    assert!(
        !ours.is_empty(),
        "CQLite compacted output decoded to ZERO typed cells — blob fallback / unreadable output"
    );
    assert!(
        !reference.is_empty(),
        "Cassandra reference decoded to ZERO typed cells — broken golden"
    );

    // Surviving shape after last-write-wins: pk 1 (A), 2 (B), 3 (B).
    let expected_lp_len: BTreeMap<&str, usize> = BTreeMap::from([("1", 1), ("2", 2), ("3", 1)]);
    let expected_ma_keys: BTreeMap<&str, &str> =
        BTreeMap::from([("1", "home"), ("2", "office"), ("3", "h")]);

    let mut checked_lp = 0usize;
    let mut checked_ma = 0usize;
    for pk in ["1", "2", "3"] {
        let our_lp = ours
            .get(&(pk.to_string(), "lp".to_string()))
            .unwrap_or_else(|| panic!("CQLite output missing lp cell for pk={pk}"));
        let ref_lp = reference
            .get(&(pk.to_string(), "lp".to_string()))
            .unwrap_or_else(|| panic!("Cassandra reference missing lp cell for pk={pk}"));
        let our_list = assert_list_structure(pk, our_lp);
        let ref_list = assert_list_structure(pk, ref_lp);
        assert_eq!(
            our_list.len(),
            expected_lp_len[pk],
            "lp[pk={pk}]: decoded list element count {} != expected {}",
            our_list.len(),
            expected_lp_len[pk]
        );
        assert_eq!(
            our_list, ref_list,
            "lp[pk={pk}]: CQLite nested frozen<list<frozen<person>>> typed element values \
             (null-last_name) != Cassandra reference"
        );
        checked_lp += 1;

        let our_ma = ours
            .get(&(pk.to_string(), "ma".to_string()))
            .unwrap_or_else(|| panic!("CQLite output missing ma cell for pk={pk}"));
        let ref_ma = reference
            .get(&(pk.to_string(), "ma".to_string()))
            .unwrap_or_else(|| panic!("Cassandra reference missing ma cell for pk={pk}"));
        let our_map = assert_map_structure(pk, our_ma);
        let ref_map = assert_map_structure(pk, ref_ma);
        assert!(
            our_map.contains_key(expected_ma_keys[pk]),
            "ma[pk={pk}]: decoded map missing expected key '{}' (have {:?})",
            expected_ma_keys[pk],
            our_map.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            our_map, ref_map,
            "ma[pk={pk}]: CQLite nested frozen<map<text,frozen<address>>> key set + typed value \
             values (null-city) != Cassandra reference"
        );
        checked_ma += 1;
    }
    assert_eq!(checked_lp, 3, "expected to check lp on 3 partitions");
    assert_eq!(checked_ma, 3, "expected to check ma on 3 partitions");

    eprintln!(
        "[issue_1289] STRUCTURAL ROUND-TRIP PASS — {TABLE_NULL} nested {NESTED_COLS:?} columns \
         (null inner field) decoded as structured List/Map and matched the Cassandra reference on \
         all 3 partitions ({checked_lp} lp + {checked_ma} ma)."
    );

    // ── 1b0. NULL inner field survives CQLite's OWN typed decode (issue #1340) ─
    // The whole point of #1289: the registry-free header-marshal decode must
    // represent the null inner field as a null UDT field (not dropped, not an
    // error). Assert against CQLite's OWN decoded `ours` cells.
    let mut own_null_fields = 0usize;
    for pk in ["1", "2", "3"] {
        let list = assert_list_structure(
            pk,
            ours.get(&(pk.to_string(), "lp".to_string()))
                .unwrap_or_else(|| panic!("CQLite output missing lp cell for pk={pk}")),
        );
        for el in &list {
            let u = as_udt(el);
            assert!(
                udt_text(u, "first_name").is_some(),
                "lp[pk={pk}]: first_name should be present"
            );
            assert_eq!(
                udt_text(u, "last_name"),
                None,
                "lp[pk={pk}]: person.last_name must decode to NULL"
            );
            own_null_fields += 1;
        }
        let map = assert_map_structure(
            pk,
            ours.get(&(pk.to_string(), "ma".to_string()))
                .unwrap_or_else(|| panic!("CQLite output missing ma cell for pk={pk}")),
        );
        for val in map.values() {
            let u = as_udt(val);
            assert!(
                udt_text(u, "street").is_some(),
                "ma[pk={pk}]: street should be present"
            );
            assert_eq!(
                udt_text(u, "city"),
                None,
                "ma[pk={pk}]: address.city must decode to NULL"
            );
            own_null_fields += 1;
        }
    }
    assert!(
        own_null_fields >= 6,
        "expected null inner fields across all surviving nested cells, saw {own_null_fields}"
    );
    eprintln!(
        "[issue_1289] NULL INNER-FIELD (OWN DECODE) PASS — CQLite's registry-free decode \
         represented person.last_name / address.city as NULL in {own_null_fields} cells."
    );

    // ── 1b. TYPED inner-UDT pin: the NULL inner field decodes to `null` ───────
    // This is the slice this issue adds: the byte-preserving reader keeps the
    // inner frozen UDT opaque, so the authoritative TYPED view of the null inner
    // field comes from the sstabledump golden. `last_name` and `city` MUST render
    // as JSON `null`; a regression that dropped the field or mis-decoded it would
    // not produce these exact structured typed values.
    let typed = jsonl_typed_cells(&ref_dir);
    let expected_typed: &[(&str, &str, &str)] = &[
        (
            "1",
            "lp",
            r#"[{"first_name":"Ada","last_name":null,"age":36}]"#,
        ),
        (
            "2",
            "lp",
            r#"[{"first_name":"Grace","last_name":null,"age":85},{"first_name":"Alan","last_name":null,"age":41}]"#,
        ),
        (
            "3",
            "lp",
            r#"[{"first_name":"Katherine","last_name":null,"age":101}]"#,
        ),
        (
            "1",
            "ma",
            r#"{"home":{"street":"1 Navy Way","city":null,"zip":"22201"}}"#,
        ),
        (
            "2",
            "ma",
            r#"{"office":{"street":"9 Apollo","city":null,"zip":"23666"}}"#,
        ),
        (
            "3",
            "ma",
            r#"{"h":{"street":"9 Apollo","city":null,"zip":"23666"}}"#,
        ),
    ];
    let mut null_fields_seen = 0usize;
    for (pk, col, want) in expected_typed {
        let got = typed
            .get(&(pk.to_string(), col.to_string()))
            .unwrap_or_else(|| panic!("JSONL golden missing typed {col} for pk={pk}"));
        assert_eq!(
            got, want,
            "{col}[pk={pk}]: typed inner-UDT decode in the sstabledump golden does not match the \
             expected null-inner-field nested value"
        );
        // Fail-closed on the WHOLE POINT of #1289: the inner field must be null.
        assert!(
            got.contains(":null"),
            "{col}[pk={pk}]: expected a NULL inner field but the golden value has none: {got}"
        );
        null_fields_seen += 1;
    }
    assert_eq!(
        null_fields_seen,
        expected_typed.len(),
        "every nested-column cell must carry a null inner field"
    );
    eprintln!(
        "[issue_1289] NULL INNER-FIELD TYPED PASS — sstabledump golden pins person.last_name=null \
         and address.city=null for all {} nested-column cells.",
        expected_typed.len()
    );

    // ── 2. Byte parity (CONDITIONAL — a Cassandra reference exists) ──────────
    for suffix in BYTE_FOR_BYTE_COMPONENTS {
        let reference = read_component(&ref_dir, suffix);
        let mine = read_component(&out_dir, suffix);
        assert!(
            !reference.is_empty(),
            "reference {suffix} is present-but-empty — broken golden"
        );
        if reference != mine {
            let at = first_diff(&reference, &mine);
            panic!(
                "{suffix} byte mismatch (cass={} ours={} bytes, first diff at {at:?})\n  \
                 cass={}\n  ours={}",
                reference.len(),
                mine.len(),
                hex(&reference),
                hex(&mine),
            );
        }
    }
    eprintln!(
        "[issue_1289] BYTE PARITY PASS — {BYTE_FOR_BYTE_COMPONENTS:?} of the compacted \
         {TABLE_NULL} output are byte-identical to the Cassandra 5.0.2 reference (null inner field)."
    );
}
