//! Issue #1020 (epic #973): UDT / frozen-value COMPACTION byte-parity.
//!
//! The UDT / frozen-value extension of the foundational live-cell compaction
//! byte-parity slice (`issue_1017_live_cell_compaction_byte_parity.rs`). Instead
//! of plain scalar columns, the inputs carry FROZEN UDT values, NESTED UDT values
//! (a UDT containing a `frozen<address>`), and FROZEN collections — including
//! collections that CONTAIN UDT values. For each scenario CQLite re-produces the
//! SAME two overlapping inputs Cassandra wrote (via its public WriteEngine API,
//! given a [`UdtRegistry`] built from the SAME UDT definitions committed in
//! `test-data/schemas/compaction-parity-udt.cql`), runs its own
//! `compact_sstables_with_registry`, and the two COMPACTED outputs are diffed.
//!
//! ## Three scenarios (issue #1020 AC1) — each: 2 overlapping SSTables -> 1 output
//!   * `udt_frozen_person`  — `frozen<person>` with a fully-populated value, an
//!     empty-string-field value, and a SURVIVING null-middle-field value (id 5,
//!     written only at T_A and never overwritten), exercising UDT field ORDER +
//!     the `-1` null-field encoding ON THE WINNING SIDE of compaction + frozen
//!     value boundaries.
//!     → `cqlite.compaction_parity.udt.frozen_person`
//!   * `udt_nested`         — `frozen<employee>` which CONTAINS a `frozen<address>`,
//!     exercising NESTED UDT field decoding + the inner frozen value boundary
//!     (including an inner-UDT null field).
//!     → `cqlite.compaction_parity.udt.nested_udt`
//!   * `udt_collections`    — `frozen<list<int>>`, `frozen<map<text,int>>`,
//!     `frozen<list<frozen<person>>>`, `frozen<map<text, frozen<address>>>`:
//!     frozen collections + collections CONTAINING UDT values.
//!     → `cqlite.compaction_parity.udt.collections_with_udts`
//!
//! ## Determinism contract — identical to issue #1017
//!   * Every cell uses a fixed `USING TIMESTAMP` (`T_A`/`T_B`), so the
//!     EncodingStats.minTimestamp delta baseline is identical on both engines.
//!   * Overlapping writes resolve by last-write-wins BY TIMESTAMP (`T_B` > `T_A`).
//!   * Tables are UNCOMPRESSED, so Data.db is a direct byte slice and CRC.db is
//!     the per-chunk CRC of identical bytes.
//!   * Partition keys are `int`: identical big-endian key bytes + Murmur3 order.
//!   * No TTL / no DELETE: purge never fires; output is independent of `gcBefore`.
//!   * EVERY complex value is FROZEN, so it serializes as a SINGLE value cell (no
//!     multicell cell-paths), the smallest deterministic surface two independent
//!     compactors can byte-match for UDTs and frozen collections.
//!
//! ## No-heuristics / fail-loud (issue #28, AC5)
//! Each scenario builds a [`UdtRegistry`] from the DDL UDT definitions and passes
//! it to BOTH the input writers and `compact_sstables_with_registry`. If a UDT
//! cannot be resolved the WriteEngine would degrade a frozen-UDT column to a bare
//! BytesType cell, which would NOT decode to a typed UDT in sstabledump JSONL and
//! would NOT byte-match the typed Cassandra reference — so a missing/incorrect
//! registry FAILS the test rather than silently passing on a blob fallback.
//!
//! ## Which components are compared (AC3 + AC6) — same rule as issue #1017
//!   * Data.db, Index.db, Summary.db, Digest.crc32 → diffed BYTE-FOR-BYTE.
//!   * CRC.db → CQLite's bytes are a byte-identical PREFIX of Cassandra's (the
//!     sole divergence is Cassandra's compaction-only trailing empty-chunk
//!     CRC32 = 0; same documented follow-up as issue #1017).
//!   * Statistics.db, Filter.db → present on BOTH sides (asserted) but NOT
//!     byte-diffed (impl-specific histograms/HLL/bloom sizing).
//!   * NO SILENT OMISSION: any component Cassandra writes that CQLite does not
//!     FAILS; any spurious CQLite-only component FAILS.
//!
//! ## Honest classification (AC7)
//! A scenario whose Data.db/Index.db/Summary.db/Digest.crc32 byte-match the
//! Cassandra compacted reference is classified `byte_for_byte` in the manifest.
//! If byte parity does not hold for a scenario, the harness FALLS BACK to the
//! canonical-semantic assertion (typed JSONL equality after compaction) and that
//! scenario is recorded `canonical_semantic` in the manifest — never claimed
//! `byte_for_byte`. The mode is selected per-scenario by the `*_MODE` constants
//! below and the chosen mode is mirrored EXACTLY by the manifest evidence type.
//!
//! ## Dataset doctrine (issue #719 / parity mandate)
//!   * If `CQLITE_DATASETS_ROOT` is unset OR the reference compacted Data.db is
//!     genuinely absent (binaries not fetched), the test SKIPS.
//!   * A PRESENT-but-empty / present-but-incomplete fixture is a FAILURE.
//!   * `CQLITE_REQUIRE_FIXTURES=1` turns a would-be SKIP into a PANIC.

#![cfg(feature = "write-support")]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crc32fast::Hasher as Crc32Hasher;

use cqlite_core::schema::{Column, CqlType, KeyColumn, TableSchema, UdtRegistry};
use cqlite_core::storage::write_engine::merge::compact_sstables_with_registry;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::{UdtField, UdtTypeDef, UdtValue, Value};
use tempfile::TempDir;

/// Fixed writetimes (micros). MUST match `T_A`/`T_B` in
/// `test-data/scripts/generate-compaction-parity-udt.sh`.
const T_A: i64 = 1000;
const T_B: i64 = 2000;

const KEYSPACE: &str = "test_compactionparityudt";

/// Output generation passed to the compactor. Fixed for determinism. Affects only
/// the on-disk filename, never the component CONTENT bytes.
const OUT_GENERATION: u64 = 3;

/// Fixed `gc_before` (secs). Irrelevant to output bytes for live cells.
const FIXED_GC_BEFORE: i64 = 1_700_000_000;

/// Per-scenario parity strength. The manifest evidence type MUST mirror this.
/// Set to the strongest mode each scenario actually achieves against the
/// Cassandra 5.0.2 compacted reference (honest classification, AC7).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    /// Data.db/Index.db/Summary.db/Digest.crc32 must byte-match the reference
    /// (plus CRC.db prefix parity + typed JSONL cross-check).
    ByteForByte,
    /// Byte parity is NOT claimed for this scenario: only the canonical typed
    /// sstabledump JSONL equality is asserted after compaction.
    CanonicalSemantic,
}

// `udt_collections` (frozen UDTs only INSIDE frozen collections) compacts
// byte-for-byte against the Cassandra 5.0.2 reference today → byte_for_byte.
const COLLECTIONS_MODE: Mode = Mode::ByteForByte;

// `udt_frozen_person` / `udt_nested` have a TOP-LEVEL `frozen<UDT>` regular
// column (a frozen SCALAR / NESTED UDT). A frozen UDT is a SINGLE-cell value
// (like a frozen collection), so once flush advertises the authoritative
// `FrozenType(UserType(...))` SerializationHeader marshal (issue #1020 fix), the
// frozen UDT cell round-trips through compaction and the output is byte-identical
// to the Cassandra 5.0.2 compacted reference → byte_for_byte.
const FROZEN_PERSON_MODE: Mode = Mode::ByteForByte;
const NESTED_MODE: Mode = Mode::ByteForByte;

// ════════════════════════════════════════════════════════════════════════════
// Fixture resolution (skip-on-absence; present-but-broken is a failure)
// ════════════════════════════════════════════════════════════════════════════

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

fn reference_dir(table: &str) -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let base = Path::new(&root).join("sstables").join(KEYSPACE);
    let entries = std::fs::read_dir(&base).ok()?;

    let mut matches: Vec<PathBuf> = entries
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
                     compacted nb-*-big-Data.db. The fixture is PRESENT-BUT-INCOMPLETE — \
                     regenerate with:\n  \
                     bash test-data/scripts/generate-compaction-parity-udt.sh\n  \
                     git -C <repo> add -f \
                     test-data/datasets/sstables/{KEYSPACE}/{table}-*/*.db"
                );
            }
            Some(dir)
        }
        n => panic!(
            "{KEYSPACE}.{table}: found {n} matching `{table}-*` directories under {base:?} \
             ({matches:?}). There must be EXACTLY ONE."
        ),
    }
}

fn assert_digest_consistent_with_data(table: &str, ref_dir: &Path) {
    let data_bytes = read_component(ref_dir, "Data.db");
    let digest_bytes = read_component(ref_dir, "Digest.crc32");

    assert!(
        !data_bytes.is_empty(),
        "{table}: committed Data.db is present-but-empty — golden is broken"
    );
    assert!(
        !digest_bytes.is_empty(),
        "{table}: committed Digest.crc32 is present-but-empty — golden is broken"
    );

    let mut hasher = Crc32Hasher::new();
    hasher.update(&data_bytes);
    let actual_crc32 = hasher.finalize();

    let digest_str = std::str::from_utf8(&digest_bytes)
        .unwrap_or_else(|_| panic!("{table}: Digest.crc32 is not valid UTF-8: {digest_bytes:?}"))
        .trim();
    let committed_crc32: u32 = digest_str.parse().unwrap_or_else(|e| {
        panic!("{table}: Digest.crc32 '{digest_str}' is not a decimal u32: {e}")
    });

    assert_eq!(
        actual_crc32, committed_crc32,
        "{table}: committed Digest.crc32 ({committed_crc32}) does not match CRC32 of \
         committed Data.db ({actual_crc32}). The golden is HALF-UPDATED — regenerate with \
         generate-compaction-parity-udt.sh and `git add -f` the .db binaries."
    );
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
        n => panic!(
            "{dir:?}: expected exactly ONE compacted nb-*-big-Data.db, found {n} ({found:?})"
        ),
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

fn component_suffixes(dir: &Path) -> BTreeSet<String> {
    let mut set = BTreeSet::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if let Some(idx) = name.find("-big-") {
                set.insert(name[idx + 5..].to_string());
            }
        }
    }
    set.retain(|s| !s.ends_with(".jsonl") && !s.ends_with("Statistics.db.txt"));
    set
}

fn toc_set(toc_bytes: &[u8]) -> BTreeSet<String> {
    String::from_utf8_lossy(toc_bytes)
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect()
}

fn first_diff(a: &[u8], b: &[u8]) -> Option<usize> {
    let n = a.len().max(b.len());
    (0..n).find(|&i| a.get(i) != b.get(i))
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

// ════════════════════════════════════════════════════════════════════════════
// UDT registry + schemas (built from the DDL in compaction-parity-udt.cql)
// ════════════════════════════════════════════════════════════════════════════

/// The registry built from `test-data/schemas/compaction-parity-udt.cql`. Holds
/// `person`, `address`, and `employee` (which nests `frozen<address>`). Passed to
/// BOTH the input writers and the compactor — fail-loud on resolution (AC5).
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
    reg.register_udt(
        UdtTypeDef::new(KEYSPACE.to_string(), "employee".to_string())
            .with_field("name".to_string(), CqlType::Text, true)
            .with_field(
                "home".to_string(),
                CqlType::Frozen(Box::new(CqlType::Udt(
                    "address".to_string(),
                    vec![
                        ("street".to_string(), CqlType::Text),
                        ("city".to_string(), CqlType::Text),
                        ("zip".to_string(), CqlType::Text),
                    ],
                ))),
                true,
            )
            .with_field("level".to_string(), CqlType::Int, true),
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

fn pk_int() -> KeyColumn {
    KeyColumn {
        name: "id".into(),
        data_type: "int".into(),
        position: 0,
    }
}

fn frozen_person_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.into(),
        table: "udt_frozen_person".into(),
        partition_keys: vec![pk_int()],
        clustering_keys: vec![],
        columns: vec![col("id", "int", false), col("p", "frozen<person>", true)],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

fn nested_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.into(),
        table: "udt_nested".into(),
        partition_keys: vec![pk_int()],
        clustering_keys: vec![],
        columns: vec![col("id", "int", false), col("e", "frozen<employee>", true)],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

fn collections_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.into(),
        table: "udt_collections".into(),
        partition_keys: vec![pk_int()],
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

// ── Value builders ───────────────────────────────────────────────────────────

fn person(first: Option<&str>, last: Option<&str>, age: Option<i32>) -> Value {
    Value::Frozen(Box::new(Value::Udt(Box::new(UdtValue {
        type_name: "person".into(),
        keyspace: KEYSPACE.into(),
        fields: vec![
            UdtField {
                name: "first_name".into(),
                value: first.map(|s| Value::Text(s.into())),
            },
            UdtField {
                name: "last_name".into(),
                value: last.map(|s| Value::Text(s.into())),
            },
            UdtField {
                name: "age".into(),
                value: age.map(Value::Integer),
            },
        ],
    }))))
}

/// A bare (non-Frozen-wrapped) person UDT for use as a collection ELEMENT.
fn person_inner(first: &str, last: &str, age: i32) -> Value {
    Value::Udt(Box::new(UdtValue {
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
    }))
}

fn address_inner(street: &str, city: Option<&str>, zip: &str) -> Value {
    Value::Udt(Box::new(UdtValue {
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
    }))
}

fn employee(name: &str, home: Value, level: i32) -> Value {
    Value::Frozen(Box::new(Value::Udt(Box::new(UdtValue {
        type_name: "employee".into(),
        keyspace: KEYSPACE.into(),
        fields: vec![
            UdtField {
                name: "name".into(),
                value: Some(Value::Text(name.into())),
            },
            UdtField {
                name: "home".into(),
                value: Some(Value::Frozen(Box::new(home))),
            },
            UdtField {
                name: "level".into(),
                value: Some(Value::Integer(level)),
            },
        ],
    }))))
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

fn write_row(table: &str, id: i32, ops: Vec<CellOperation>, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KEYSPACE, table),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        ops,
        ts,
        None,
    )
}

fn write_one(table: &str, id: i32, column: &str, value: Value, ts: i64) -> Mutation {
    write_row(
        table,
        id,
        vec![CellOperation::Write {
            column: column.into(),
            value,
        }],
        ts,
    )
}

fn op(column: &str, value: Value) -> CellOperation {
    CellOperation::Write {
        column: column.into(),
        value,
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Input building + compaction (CQLite candidate, registry-aware)
// ════════════════════════════════════════════════════════════════════════════

async fn cqlite_compact(
    schema: &TableSchema,
    group_a: Vec<Mutation>,
    group_b: Vec<Mutation>,
) -> (TempDir, PathBuf) {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("inputs");
    let wal_dir = temp.path().join("wal");
    let out_dir = temp.path().join("out");

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone())
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
        schema,
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
// Shared parity assertion
// ════════════════════════════════════════════════════════════════════════════

const BYTE_FOR_BYTE_COMPONENTS: &[&str] = &["Data.db", "Index.db", "Summary.db", "Digest.crc32"];
const PRESENT_NOT_DIFFED: &[&str] = &["Statistics.db", "Filter.db"];

/// A row of the canonical JSONL golden: `(pk_repr, &[(cell_name, canonical_json)])`.
/// `canonical_json` is the EXACT `serde_json::Value::to_string()` of the
/// sstabledump cell's `value`, so typed UDT/frozen decoding (field order, null
/// fields, nested fields) is pinned, not just presence.
type ExpectedRows<'a> = &'a [(&'a str, &'a [(&'a str, &'a str)])];

async fn assert_udt_compaction_parity(
    table: &str,
    mode: Mode,
    schema: TableSchema,
    group_a: Vec<Mutation>,
    group_b: Vec<Mutation>,
    expected_partitions: usize,
    expected: ExpectedRows<'_>,
) {
    let Some(ref_dir) = reference_dir(table) else {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but the compacted reference for \
                 {KEYSPACE}.{table} is absent; generate it with \
                 bash test-data/scripts/generate-compaction-parity-udt.sh"
            );
        }
        eprintln!(
            "[issue_1020] reference for {KEYSPACE}.{table} absent (dataset not fetched); skipping"
        );
        return;
    };

    // Stale-golden drift guard.
    assert_digest_consistent_with_data(table, &ref_dir);

    let (_guard, out_dir) = cqlite_compact(&schema, group_a, group_b).await;

    // Typed JSONL equality after compaction (AC2/AC4): THE claim for
    // CanonicalSemantic, a cross-check for ByteForByte.
    assert_jsonl_typed(table, &ref_dir, expected_partitions, expected);

    // roborev #1020 Finding 3: the JSONL assertion above only reads the Cassandra
    // REFERENCE. Decode CQLITE'S COMPACTED OUTPUT too and assert its typed
    // (pk, cell) -> Value map equals the typed map decoded from the Cassandra
    // reference with the SAME reader. This validates CQLite's output is a real,
    // typed, decodable SSTable (UDT/frozen field decoding survived) rather than a
    // blob fallback — the previous test validated nothing about CQLite's output.
    assert_cqlite_output_decodes_like_reference(table, &schema, &ref_dir, &out_dir).await;

    if mode == Mode::CanonicalSemantic {
        eprintln!(
            "[issue_1020] {KEYSPACE}.{table}: CANONICAL-SEMANTIC parity PASS — typed sstabledump \
             JSONL (UDT/frozen field decoding) matches the Cassandra 5.0.2 compacted reference. \
             Byte parity NOT claimed for this scenario (recorded canonical_semantic in manifest)."
        );
        return;
    }

    // ── Byte-for-byte path (AC3 + AC6) ──
    let ref_components = component_suffixes(&ref_dir);
    let our_components = component_suffixes(&out_dir);
    assert!(
        !ref_components.is_empty(),
        "{table}: reference component set is empty (broken fixture)"
    );
    for needed in BYTE_FOR_BYTE_COMPONENTS
        .iter()
        .chain(PRESENT_NOT_DIFFED.iter())
        .chain(["CRC.db", "TOC.txt"].iter())
    {
        assert!(
            ref_components.contains(*needed),
            "{table}: reference missing required component {needed}; have {ref_components:?}"
        );
        assert!(
            our_components.contains(*needed),
            "{table}: CQLite output missing component {needed}; have {our_components:?}"
        );
    }
    let omitted: Vec<&String> = ref_components.difference(&our_components).collect();
    assert!(
        omitted.is_empty(),
        "{table}: Cassandra wrote component(s) CQLite SILENTLY OMITS: {omitted:?} \
         (ref={ref_components:?} ours={our_components:?})"
    );
    let spurious: Vec<&String> = our_components.difference(&ref_components).collect();
    assert!(
        spurious.is_empty(),
        "{table}: CQLite emitted spurious component(s): {spurious:?} \
         (ours={our_components:?} ref={ref_components:?})"
    );

    let ref_toc = toc_set(&read_component(&ref_dir, "TOC.txt"));
    let our_toc = toc_set(&read_component(&out_dir, "TOC.txt"));
    assert_eq!(
        ref_toc, our_toc,
        "{table}: TOC.txt component set differs (cass={ref_toc:?} ours={our_toc:?})"
    );

    for suffix in BYTE_FOR_BYTE_COMPONENTS {
        assert_component_bytes(table, &ref_dir, &out_dir, suffix);
    }
    assert_crc_db_prefix_parity(table, &ref_dir, &out_dir);

    for suffix in PRESENT_NOT_DIFFED {
        let r = read_component(&ref_dir, suffix);
        let o = read_component(&out_dir, suffix);
        assert!(
            !r.is_empty() && !o.is_empty(),
            "{table}: {suffix} present-but-empty on one side (cass={} ours={} bytes)",
            r.len(),
            o.len()
        );
    }

    eprintln!(
        "[issue_1020] {KEYSPACE}.{table}: UDT/frozen COMPACTION byte parity PASS — \
         {BYTE_FOR_BYTE_COMPONENTS:?} byte-identical to the Cassandra 5.0.2 compacted reference; \
         {PRESENT_NOT_DIFFED:?} present on both; typed JSONL cross-check OK."
    );
}

fn assert_component_bytes(table: &str, ref_dir: &Path, out_dir: &Path, suffix: &str) {
    let reference = read_component(ref_dir, suffix);
    let ours = read_component(out_dir, suffix);
    assert!(
        !reference.is_empty(),
        "{table}: reference {suffix} is present-but-empty — parity failure"
    );
    if reference != ours {
        let at = first_diff(&reference, &ours);
        let (cass_hex, ours_hex) = (hex(&reference), hex(&ours));
        panic!(
            "{table}: {suffix} byte mismatch (cass={} ours={} bytes, first diff at {at:?})\n  \
             cass={cass_hex}\n  ours={ours_hex}",
            reference.len(),
            ours.len(),
        );
    }
}

fn assert_crc_db_prefix_parity(table: &str, ref_dir: &Path, out_dir: &Path) {
    let cass = read_component(ref_dir, "CRC.db");
    let ours = read_component(out_dir, "CRC.db");
    assert!(
        !cass.is_empty() && !ours.is_empty(),
        "{table}: CRC.db present-but-empty (cass={} ours={} bytes)",
        cass.len(),
        ours.len()
    );
    assert!(
        ours.len() <= cass.len() && cass[..ours.len()] == ours[..],
        "{table}: CRC.db prefix mismatch (cass={} ours={} bytes)\n  cass={}\n  ours={}",
        cass.len(),
        ours.len(),
        hex(&cass),
        hex(&ours)
    );
    let suffix = &cass[ours.len()..];
    assert!(
        suffix.len() % 4 == 0 && suffix.iter().all(|&b| b == 0),
        "{table}: CRC.db divergent suffix is NOT trailing empty-chunk CRC32=0 groups: \
         suffix={} (cass={} ours={})",
        hex(suffix),
        hex(&cass),
        hex(&ours)
    );
}

/// Typed JSONL equality after compaction: the committed golden must exist, be
/// non-empty, carry exactly `expected_partitions` partitions, and the per-(pk,
/// cell-name) canonical JSON value map must EXACTLY equal `expected`. Pins typed
/// UDT/frozen decoding (field ORDER, null fields, nested fields) survived
/// compaction (AC2 + AC4). A blob fallback would not produce these typed values.
fn assert_jsonl_typed(
    table: &str,
    ref_dir: &Path,
    expected_partitions: usize,
    expected: ExpectedRows<'_>,
) {
    let data = single_data_db(ref_dir).expect("compacted Data.db");
    let jsonl = ref_dir.join(format!("{}Data.db.jsonl", descriptor_prefix(&data)));
    let text = std::fs::read_to_string(&jsonl)
        .unwrap_or_else(|e| panic!("{table}: committed JSONL golden {jsonl:?} unreadable: {e}"));
    let lines: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();
    assert!(
        !lines.is_empty(),
        "{table}: committed JSONL golden is present-but-empty — parity failure"
    );
    assert_eq!(
        lines.len(),
        expected_partitions,
        "{table}: JSONL golden partition count {} != expected {expected_partitions}",
        lines.len()
    );

    // (pk_repr, cell_name) -> canonical JSON string of the cell `value`.
    let mut seen: BTreeMap<(String, String), String> = BTreeMap::new();
    for (i, line) in lines.iter().enumerate() {
        let jv: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("{table}: JSONL partition {i} is not valid JSON: {e}"));
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
            "{table}: JSONL partition {i} (pk={pk:?}) has no rows — parity failure"
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

    let expected_map: BTreeMap<(String, String), String> = expected
        .iter()
        .flat_map(|(pk, cells)| {
            cells
                .iter()
                .map(move |(name, v)| ((pk.to_string(), name.to_string()), v.to_string()))
        })
        .collect();

    let wrong_or_missing: Vec<_> = expected_map
        .iter()
        .filter(|(k, v)| seen.get(*k).map(String::as_str) != Some(v.as_str()))
        .collect();
    let unexpected: Vec<_> = seen
        .iter()
        .filter(|(k, v)| expected_map.get(*k).map(String::as_str) != Some(v.as_str()))
        .collect();
    assert!(
        wrong_or_missing.is_empty() && unexpected.is_empty(),
        "{table}: JSONL typed (pk,cell)->value map does not match expected survivors\n  \
         wrong or missing: {wrong_or_missing:?}\n  \
         unexpected: {unexpected:?}\n  \
         full seen={seen:?}\n  full expected={expected_map:?}"
    );
}

/// roborev #1020 Finding 3: decode CQLite's compacted OUTPUT and the Cassandra
/// REFERENCE through the SAME `SSTableReader` + schema, then assert the typed
/// `(pk, cell) -> Value` maps are EQUAL.
///
/// The reader decodes a `frozen<udt>` column STRUCTURALLY from the on-disk
/// SerializationHeader marshal (issue #1080), so the bare `frozen<person>` schema
/// suffices. If CQLite emitted a blob fallback (the failure mode this slice
/// guards against), the decoded values would differ from the typed reference
/// values and FAIL here — not silently pass. Frozen wrappers are peeled before
/// comparison so a `Value::Frozen(Value::Udt)` and a bare `Value::Udt` of the
/// same content compare equal.
async fn assert_cqlite_output_decodes_like_reference(
    table: &str,
    schema: &TableSchema,
    ref_dir: &Path,
    out_dir: &Path,
) {
    let reference = decode_typed_cell_map(table, schema, ref_dir).await;
    let ours = decode_typed_cell_map(table, schema, out_dir).await;

    assert!(
        !ours.is_empty(),
        "{table}: CQLite compacted output decoded to ZERO typed cells — likely a \
         blob fallback or unreadable output (Finding 3 guard)"
    );

    let wrong_or_missing: Vec<_> = reference
        .iter()
        .filter(|(k, v)| ours.get(*k) != Some(*v))
        .collect();
    let unexpected: Vec<_> = ours
        .iter()
        .filter(|(k, v)| reference.get(*k) != Some(*v))
        .collect();
    assert!(
        wrong_or_missing.is_empty() && unexpected.is_empty(),
        "{table}: CQLite output typed (pk,cell)->Value map does not match the \
         Cassandra reference decoded by the same reader\n  \
         wrong or missing (ref side): {wrong_or_missing:?}\n  \
         unexpected (ours side): {unexpected:?}\n  \
         ours={ours:?}\n  reference={reference:?}"
    );

    eprintln!(
        "[issue_1020] {KEYSPACE}.{table}: CQLite OUTPUT decode parity PASS — \
         {} typed cells decode identically from CQLite's output and the Cassandra \
         reference (Finding 3).",
        ours.len()
    );
}

/// Decode a compacted SSTable directory into a typed `(pk_repr, cell_name) ->
/// Value` map via the public `SSTableReader` compaction iterator. `Value::Frozen`
/// wrappers are peeled so comparison is wrapper-shape-insensitive.
async fn decode_typed_cell_map(
    table: &str,
    schema: &TableSchema,
    dir: &Path,
) -> BTreeMap<(String, String), Value> {
    use cqlite_core::platform::Platform;
    use cqlite_core::storage::sstable::reader::compaction_row::CompactionRowData;
    use cqlite_core::storage::sstable::reader::SSTableReader;
    use cqlite_core::Config;
    use std::sync::Arc;

    let data_path =
        single_data_db(dir).unwrap_or_else(|| panic!("{table}: no compacted Data.db in {dir:?}"));

    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("platform init for output decode"),
    );
    let reader = SSTableReader::open(&data_path, &config, platform)
        .await
        .unwrap_or_else(|e| panic!("{table}: open {data_path:?} for decode failed: {e}"));
    let rows = reader
        .iterate_all_partitions_for_compaction(Some(schema))
        .await
        .unwrap_or_else(|e| panic!("{table}: compaction iterate of {data_path:?} failed: {e}"));

    let mut map: BTreeMap<(String, String), Value> = BTreeMap::new();
    for row in &rows {
        let pk = pk_repr(&row.key.0);
        if let CompactionRowData::Live { simple, .. } = &row.row_data {
            for cell in simple {
                // Skip the partition-key column surfaced as a cell (pk is the map
                // key already) and any cell tombstones.
                if matches!(cell.value, Value::Tombstone(_)) {
                    continue;
                }
                map.insert((pk.clone(), cell.column.clone()), peel_frozen(&cell.value));
            }
        }
    }
    map
}

/// Render a partition-key byte slice as the int `id` string used by the
/// `expected`/JSONL pk repr (the #1020 tables all use an `int` PK). Falls back to
/// hex for any non-4-byte key.
fn pk_repr(bytes: &[u8]) -> String {
    if bytes.len() == 4 {
        i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]).to_string()
    } else {
        hex(bytes)
    }
}

/// Recursively peel `Value::Frozen` wrappers so a frozen and a bare value of the
/// same content compare equal.
fn peel_frozen(v: &Value) -> Value {
    match v {
        Value::Frozen(inner) => peel_frozen(inner),
        other => other.clone(),
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Scenario 1 — udt_frozen_person (frozen<person>, null/empty fields)
// ════════════════════════════════════════════════════════════════════════════

/// Manifest: `cqlite.compaction_parity.udt.frozen_person`
/// (`cass.compaction.udt_frozen_person`,
/// `cass.sstable_format.CQLSSTableWriterTest.frozen_udt_roundtrip`).
/// Shared inputs for the `udt_frozen_person` scenario (one builder, two tests).
fn frozen_person_groups() -> (Vec<Mutation>, Vec<Mutation>) {
    let t = "udt_frozen_person";
    let group_a = vec![
        write_one(
            t,
            1,
            "p",
            person(Some("Ada"), Some("Lovelace"), Some(36)),
            T_A,
        ),
        write_one(t, 2, "p", person(Some("Grace"), None, Some(85)), T_A),
        write_one(t, 3, "p", person(Some(""), Some("Turing"), Some(41)), T_A),
        // id 5 is written ONLY in group A and never overwritten, so a SURVIVING
        // value carrying a NULL middle field (last_name) reaches the compacted
        // output. This verifies the `-1` absent-field encoding on the WINNING
        // side of compaction (roborev #1020 Finding 2: every prior null-field
        // row was overwritten by a group-B full-field write, so the surviving
        // null-field encoding was never byte-verified).
        write_one(t, 5, "p", person(Some("Edsger"), None, Some(75)), T_A),
    ];
    let group_b = vec![
        write_one(
            t,
            2,
            "p",
            person(Some("Grace"), Some("Hopper"), Some(85)),
            T_B,
        ),
        write_one(
            t,
            3,
            "p",
            person(Some("Alan"), Some("Turing"), Some(41)),
            T_B,
        ),
        write_one(
            t,
            4,
            "p",
            person(Some("Katherine"), Some("Johnson"), Some(101)),
            T_B,
        ),
    ];
    (group_a, group_b)
}

const FROZEN_PERSON_EXPECTED: ExpectedRows = &[
    (
        "1",
        &[(
            "p",
            r#"{"first_name":"Ada","last_name":"Lovelace","age":36}"#,
        )],
    ),
    (
        "2",
        &[(
            "p",
            r#"{"first_name":"Grace","last_name":"Hopper","age":85}"#,
        )],
    ),
    (
        "3",
        &[(
            "p",
            r#"{"first_name":"Alan","last_name":"Turing","age":41}"#,
        )],
    ),
    (
        "4",
        &[(
            "p",
            r#"{"first_name":"Katherine","last_name":"Johnson","age":101}"#,
        )],
    ),
    // roborev #1020 Finding 2: a SURVIVING null-middle-field value. id 5 is
    // written only at T_A and never overwritten, so its `last_name:null` field
    // (the `-1` absent-field marker) reaches the compacted output and is
    // byte-verified here, unlike every prior null-field row (all overwritten).
    (
        "5",
        &[("p", r#"{"first_name":"Edsger","last_name":null,"age":75}"#)],
    ),
];

/// Manifest: `cqlite.compaction_parity.udt.frozen_person` (byte_for_byte).
///
/// A top-level `frozen<person>` regular column round-trips through compaction as
/// exactly ONE frozen single-cell value; the compacted output is byte-identical
/// to the Cassandra 5.0.2 reference (issue #1020 fix: flush advertises the
/// authoritative `FrozenType(UserType(...))` header marshal).
#[tokio::test]
async fn frozen_person_compaction_parity() {
    let (group_a, group_b) = frozen_person_groups();
    assert_udt_compaction_parity(
        "udt_frozen_person",
        FROZEN_PERSON_MODE,
        frozen_person_schema(),
        group_a,
        group_b,
        5,
        FROZEN_PERSON_EXPECTED,
    )
    .await;
}

// ════════════════════════════════════════════════════════════════════════════
// Scenario 2 — udt_nested (frozen<employee> containing frozen<address>)
// ════════════════════════════════════════════════════════════════════════════

/// Manifest: `cqlite.compaction_parity.udt.nested_udt`
/// (`cass.compaction.udt_nested`,
/// `cass.serialization.SerializationHeaderTest.udt_schema_resolution`).
/// Shared inputs for the `udt_nested` scenario (one builder, two tests).
fn nested_groups() -> (Vec<Mutation>, Vec<Mutation>) {
    let t = "udt_nested";
    let group_a = vec![
        write_one(
            t,
            1,
            "e",
            employee(
                "Grace",
                address_inner("1 Navy Way", Some("Arlington"), "22201"),
                9,
            ),
            T_A,
        ),
        write_one(
            t,
            2,
            "e",
            employee("NoCity", address_inner("5 Elm", None, "00000"), 0),
            T_A,
        ),
    ];
    let group_b = vec![
        write_one(
            t,
            2,
            "e",
            employee(
                "WithCity",
                address_inner("5 Elm", Some("Dover"), "00000"),
                2,
            ),
            T_B,
        ),
        write_one(
            t,
            3,
            "e",
            employee(
                "Katherine",
                address_inner("9 Apollo", Some("Hampton"), "23666"),
                11,
            ),
            T_B,
        ),
    ];
    (group_a, group_b)
}

const NESTED_EXPECTED: ExpectedRows = &[
    (
        "1",
        &[(
            "e",
            r#"{"name":"Grace","home":{"street":"1 Navy Way","city":"Arlington","zip":"22201"},"level":9}"#,
        )],
    ),
    (
        "2",
        &[(
            "e",
            r#"{"name":"WithCity","home":{"street":"5 Elm","city":"Dover","zip":"00000"},"level":2}"#,
        )],
    ),
    (
        "3",
        &[(
            "e",
            r#"{"name":"Katherine","home":{"street":"9 Apollo","city":"Hampton","zip":"23666"},"level":11}"#,
        )],
    ),
];

/// Manifest: `cqlite.compaction_parity.udt.nested_udt` (byte_for_byte).
///
/// The value column is a top-level `frozen<employee>` that nests a
/// `frozen<address>`. The flush header expands to the authoritative
/// `FrozenType(UserType(...,home:UserType(...),...))` marshal (inner UDT field
/// spelled as bare `UserType` per Cassandra), so the nested frozen UDT cell
/// (incl. the inner null `city` for the gen-A losing row) round-trips through
/// compaction byte-identical to the Cassandra 5.0.2 reference (issue #1020 fix).
#[tokio::test]
async fn nested_udt_compaction_parity() {
    let (group_a, group_b) = nested_groups();
    assert_udt_compaction_parity(
        "udt_nested",
        NESTED_MODE,
        nested_schema(),
        group_a,
        group_b,
        3,
        NESTED_EXPECTED,
    )
    .await;
}

// ════════════════════════════════════════════════════════════════════════════
// Scenario 3 — udt_collections (frozen collections + collections-of-UDT)
// ════════════════════════════════════════════════════════════════════════════

/// Manifest: `cqlite.compaction_parity.udt.collections_with_udts`
/// (`cass.compaction.udt_collections_with_udts`,
/// `cass.sstable_format.LegacySSTableTest.complex_udt_frozen_non_frozen`).
#[tokio::test]
async fn collections_with_udts_compaction_parity() {
    let t = "udt_collections";
    let group_a = vec![
        write_row(
            t,
            1,
            vec![
                op("fl", flist(&[1, 2, 3])),
                op("fm", fmap(&[("x", 10), ("y", 20)])),
                op(
                    "lp",
                    Value::Frozen(Box::new(Value::List(vec![person_inner(
                        "Ada", "Lovelace", 36,
                    )]))),
                ),
                op(
                    "ma",
                    Value::Frozen(Box::new(Value::Map(vec![(
                        Value::Text("home".into()),
                        address_inner("1 Navy Way", Some("Arlington"), "22201"),
                    )]))),
                ),
            ],
            T_A,
        ),
        write_row(
            t,
            2,
            vec![
                op("fl", flist(&[9])),
                op("fm", fmap(&[("z", 99)])),
                op(
                    "lp",
                    Value::Frozen(Box::new(Value::List(vec![person_inner("Old", "Val", 1)]))),
                ),
                op(
                    "ma",
                    Value::Frozen(Box::new(Value::Map(vec![(
                        Value::Text("k".into()),
                        address_inner("old", Some("old"), "0"),
                    )]))),
                ),
            ],
            T_A,
        ),
    ];
    let group_b = vec![
        write_row(
            t,
            2,
            vec![
                op("fl", flist(&[4, 5])),
                op("fm", fmap(&[("a", 1), ("b", 2)])),
                op(
                    "lp",
                    Value::Frozen(Box::new(Value::List(vec![
                        person_inner("Grace", "Hopper", 85),
                        person_inner("Alan", "Turing", 41),
                    ]))),
                ),
                op(
                    "ma",
                    Value::Frozen(Box::new(Value::Map(vec![(
                        Value::Text("office".into()),
                        address_inner("9 Apollo", Some("Hampton"), "23666"),
                    )]))),
                ),
            ],
            T_B,
        ),
        write_row(
            t,
            3,
            vec![
                op("fl", flist(&[7, 8, 9])),
                op("fm", fmap(&[("q", 1)])),
                op(
                    "lp",
                    Value::Frozen(Box::new(Value::List(vec![person_inner(
                        "Katherine",
                        "Johnson",
                        101,
                    )]))),
                ),
                op(
                    "ma",
                    Value::Frozen(Box::new(Value::Map(vec![(
                        Value::Text("h".into()),
                        address_inner("9 Apollo", Some("Hampton"), "23666"),
                    )]))),
                ),
            ],
            T_B,
        ),
    ];
    assert_udt_compaction_parity(
        t,
        COLLECTIONS_MODE,
        collections_schema(),
        group_a,
        group_b,
        3,
        &[
            (
                "1",
                &[
                    ("fl", "[1,2,3]"),
                    ("fm", r#"{"x":10,"y":20}"#),
                    (
                        "lp",
                        r#"[{"first_name":"Ada","last_name":"Lovelace","age":36}]"#,
                    ),
                    (
                        "ma",
                        r#"{"home":{"street":"1 Navy Way","city":"Arlington","zip":"22201"}}"#,
                    ),
                ],
            ),
            (
                "2",
                &[
                    ("fl", "[4,5]"),
                    ("fm", r#"{"a":1,"b":2}"#),
                    (
                        "lp",
                        r#"[{"first_name":"Grace","last_name":"Hopper","age":85},{"first_name":"Alan","last_name":"Turing","age":41}]"#,
                    ),
                    (
                        "ma",
                        r#"{"office":{"street":"9 Apollo","city":"Hampton","zip":"23666"}}"#,
                    ),
                ],
            ),
            (
                "3",
                &[
                    ("fl", "[7,8,9]"),
                    ("fm", r#"{"q":1}"#),
                    (
                        "lp",
                        r#"[{"first_name":"Katherine","last_name":"Johnson","age":101}]"#,
                    ),
                    (
                        "ma",
                        r#"{"h":{"street":"9 Apollo","city":"Hampton","zip":"23666"}}"#,
                    ),
                ],
            ),
        ],
    )
    .await;
}
