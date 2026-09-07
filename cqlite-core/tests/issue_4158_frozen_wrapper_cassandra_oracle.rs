//! Issue #4158 — the SerializationHeader's `FrozenType(...)` wrapper, against a
//! CASSANDRA-WRITTEN oracle.
//!
//! # The defect
//!
//! For a `frozen<UDT>` column CQLite's write path emitted
//!
//! ```text
//! org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.BytesType)
//! ```
//!
//! `BytesType` is `blob` — a SCALAR — and Apache Cassandra can never write
//! `FrozenType(<scalar>)`: `CQL3Type.Raw::freeze()` throws for every
//! non-collection/tuple/UDT/vector (pinned `cassandra-5.0.8`,
//! `CQL3Type.java:647-651`), and structurally only four marshal types carry the
//! `includeFrozenType = !ignoreFreezing && !isMultiCell` branch that PRINTS the
//! wrapper at all — `ListType.java:195-207`, `SetType.java:185-197`,
//! `MapType.java:310-321`, `UserType.java:436-448`. Everything else inherits
//! `AbstractType.toString() == getClass().getName()` (`AbstractType.java:741`,
//! with `toString(boolean) == toString()` at `:466`), and `TupleType.toString()`
//! is `getClass().getName() + stringifyTypeParameters(types, true)`
//! (`TupleType.java:557-560`) — a CQL tuple is already frozen and is never
//! wrapped.
//!
//! # Why a round-trip could not catch it
//!
//! A CQLite-written + CQLite-read round-trip is INVARIANT to a uniform header
//! defect (#3042): the reader's marker-search fallback recovered from the bad
//! header, so the round-trip closed for months. Every expectation in this file
//! is therefore derived from **Cassandra-written bytes** or from Cassandra's own
//! writer source — never from CQLite's output — and every assertion is made on
//! the **emitted `Statistics.db` bytes**, never on what CQLite reads back.
//!
//! # The three halves
//!
//! 1. [`cassandra_never_wrote_a_frozentype_around_a_non_freezable_type`] takes a
//!    census of every `FrozenType(` Apache Cassandra wrote across the corpus and
//!    pins the oracle SET from those bytes.
//! 2. [`cqlite_emits_no_frozentype_cassandra_cannot_write`] writes a battery of
//!    frozen/composite column spellings through the public write API and asserts
//!    every `FrozenType(` in the EMITTED header draws its inner from that set.
//! 3. [`frozen_udt_header_matches_the_cassandra_written_string`] pins the exact
//!    byte string, and re-derives it from Cassandra's own `Statistics.db` when
//!    the corpus is reachable — so the committed literal is provably Cassandra's
//!    and not a transcription of CQLite's own output.

#![cfg(feature = "write-support")]

use cqlite_core::schema::{Column, CqlType, KeyColumn, TableSchema, UdtRegistry};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::{UdtTypeDef, Value};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

#[path = "support/datasets_root.rs"]
mod datasets_root;

/// The marshal simple names that may appear as the immediate inner of a
/// `FrozenType(` — i.e. the types whose `toString(boolean)` has the
/// `includeFrozenType` branch (see the module docs for the pinned
/// `cassandra-5.0.8` line numbers).
///
/// Half 1 of this file re-derives exactly this set from Cassandra-written bytes;
/// half 2 holds CQLite's emitted headers to it.
const FREEZE_WRAPPED_HEADS: [&str; 4] = ["ListType", "SetType", "MapType", "UserType"];

const MARSHAL_PREFIX: &str = "org.apache.cassandra.db.marshal.";

/// Every `FrozenType(` in `hay`, reduced to the marshal SIMPLE NAME of its
/// immediate inner (`FrozenType(org.apache.cassandra.db.marshal.ListType(…)` →
/// `"ListType"`; the impossible `FrozenType(…BytesType)` → `"BytesType"`).
///
/// An inner that is not a recognisable marshal identifier yields the empty
/// string, which is never in [`FREEZE_WRAPPED_HEADS`] — an unreadable inner is
/// reported as a violation rather than skipped.
fn frozen_inner_heads(hay: &str) -> Vec<String> {
    hay.match_indices("FrozenType(")
        .map(|(idx, needle)| {
            let rest = &hay[idx + needle.len()..];
            let rest = rest.strip_prefix(MARSHAL_PREFIX).unwrap_or(rest);
            rest.chars()
                .take_while(|c| c.is_ascii_alphanumeric())
                .collect::<String>()
        })
        .collect()
}

/// `true` when a missing corpus must FAIL rather than skip (the gate pins this
/// for its dataset-bearing components).
fn fixtures_required() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").ok().as_deref(),
        Some("1")
    )
}

/// Every `*-Statistics.db` under `root`, recursively.
fn statistics_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Statistics.db"))
            {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

// ---------------------------------------------------------------------------
// Half 1 — the oracle, taken from Cassandra-written bytes.
// ---------------------------------------------------------------------------

/// Census every `FrozenType(` Apache Cassandra wrote into the corpus and assert
/// its immediate inner is ALWAYS one of [`FREEZE_WRAPPED_HEADS`].
///
/// This is what makes the writer-side assertion in half 2 an oracle rather than
/// a preference: the set is not asserted from CQLite's behaviour, it is measured
/// from bytes Cassandra produced. Measured on this fleet's corpus (144
/// `Statistics.db`, `CQLITE_DATASETS_ROOT=/data/datasets`): 60 occurrences —
/// MapType 25, ListType 16, UserType 10, SetType 9. Never a scalar, and never
/// `TupleType`.
#[test]
fn cassandra_never_wrote_a_frozentype_around_a_non_freezable_type() {
    let mut files = 0usize;
    let mut census: BTreeMap<String, usize> = BTreeMap::new();
    let mut violations: Vec<String> = Vec::new();

    for root in datasets_root::sstables_root_candidates() {
        for path in statistics_files(&root) {
            let Ok(bytes) = std::fs::read(&path) else {
                continue;
            };
            files += 1;
            let hay = String::from_utf8_lossy(&bytes);
            for head in frozen_inner_heads(&hay) {
                *census.entry(head.clone()).or_default() += 1;
                if !FREEZE_WRAPPED_HEADS.contains(&head.as_str()) {
                    violations.push(format!("{}: FrozenType({head})", path.display()));
                }
            }
        }
    }

    if files == 0 {
        assert!(
            !fixtures_required(),
            "CQLITE_REQUIRE_FIXTURES=1 but no *-Statistics.db was found in any \
             candidate root — the Cassandra oracle cannot be measured.\n{}",
            datasets_root::describe_roots()
        );
        eprintln!(
            "[#4158] SKIP: no *-Statistics.db in any candidate root; the \
             Cassandra-written oracle census could not be taken.\n{}",
            datasets_root::describe_roots()
        );
        return;
    }

    assert!(
        violations.is_empty(),
        "Cassandra-written headers must never carry a FrozenType(...) around a \
         non-freezable type; {} violation(s): {:?}",
        violations.len(),
        violations
    );

    // Affirmative zero (CLAUDE.md): a census reports what it RECOGNISED, and an
    // unmeasured check must not read like a clean one. A corpus with
    // Statistics.db files but no FrozenType at all would prove nothing.
    let total: usize = census.values().sum();
    assert!(
        total > 0,
        "{files} Statistics.db file(s) scanned but 0 FrozenType( occurrences \
         RECOGNISED — the oracle set would be vacuous.\n{}",
        datasets_root::describe_roots()
    );
    eprintln!(
        "[#4158] Cassandra oracle: {total} FrozenType( occurrence(s) RECOGNISED \
         over {files} Statistics.db file(s); inner heads = {census:?}"
    );
}

// ---------------------------------------------------------------------------
// Writer harness.
// ---------------------------------------------------------------------------

fn schema_with(keyspace: &str, table: &str, cols: &[(&str, &str)]) -> TableSchema {
    let mut columns = vec![Column {
        name: "pk".to_string(),
        data_type: "int".to_string(),
        nullable: false,
        default: None,
        is_static: false,
    }];
    columns.extend(cols.iter().map(|(name, ty)| Column {
        name: (*name).to_string(),
        data_type: (*ty).to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }));
    TableSchema {
        keyspace: keyspace.to_string(),
        table: table.to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns,
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Flush one row through the public write path and return the emitted
/// `Statistics.db` as a lossy string.
///
/// Only the partition key is written: the SerializationHeader's regular-column
/// types come from the SCHEMA, so no cell value is needed to exercise the emit
/// path under test.
async fn emitted_statistics_header(
    temp_dir: &TempDir,
    schema: &TableSchema,
    registry: Option<UdtRegistry>,
) -> String {
    let mut config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );
    if let Some(registry) = registry {
        config = config.with_udt_registry(registry);
    }
    let mut engine = WriteEngine::new(config).expect("engine creation should succeed");

    let mutation = Mutation::new(
        TableId::new(&schema.keyspace, &schema.table),
        PartitionKey::single("pk", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "pk".to_string(),
            value: Value::Integer(1),
        }],
        1_000_000,
        None,
    );
    engine
        .write_async(mutation)
        .await
        .expect("write should succeed");
    let info = engine
        .flush()
        .await
        .expect("flush should succeed")
        .expect("flush should return SSTableInfo");

    let stats_path = info.data_path.with_file_name(
        info.data_path
            .file_name()
            .and_then(|n| n.to_str())
            .expect("Data.db file name")
            .replace("Data.db", "Statistics.db"),
    );
    let bytes = std::fs::read(&stats_path).expect("Statistics.db should be readable");
    String::from_utf8_lossy(&bytes).into_owned()
}

/// `test_oa.address_type` exactly as Apache Cassandra declared it for the
/// committed `test_oa/udt_table` fixture (`test-data/schemas/oa-test.cql:40-45`).
fn address_type_registry() -> UdtRegistry {
    let mut registry = UdtRegistry::new();
    registry.register_udt(
        UdtTypeDef::new("test_oa".to_string(), "address_type".to_string())
            .with_field("street".to_string(), CqlType::Text, true)
            .with_field("city".to_string(), CqlType::Text, true)
            .with_field("country".to_string(), CqlType::Text, true)
            .with_field("postal_code".to_string(), CqlType::Text, true),
    );
    registry
}

// ---------------------------------------------------------------------------
// Half 2 — CQLite's emitted headers, held to the oracle set.
// ---------------------------------------------------------------------------

/// Every `FrozenType(` CQLite emits must draw its immediate inner from the set
/// half 1 measured on Cassandra-written bytes.
///
/// The battery deliberately includes the shapes that used to fabricate the
/// impossible wrapper: a `frozen<udt>` whose name NO registry can resolve
/// (`FrozenType(BytesType)` — the #4158 report), a `frozen<scalar>`, a
/// `frozen<tuple<..>>` (`TupleType` is never wrapped), and frozen elements of a
/// collection.
#[tokio::test]
async fn cqlite_emits_no_frozentype_cassandra_cannot_write() {
    // Spellings whose UDT names resolve NOWHERE — the writer must degrade to a
    // legal scalar spelling, never to `FrozenType(<scalar>)`.
    let unresolvable = [
        ("c_frozen_udt", "frozen<person>"),
        ("c_frozen_int", "frozen<int>"),
        ("c_frozen_text", "frozen<text>"),
        ("c_frozen_tuple", "frozen<tuple<int, text>>"),
        ("c_tuple_frozen_list", "tuple<int, frozen<list<int>>>"),
        ("c_frozen_list", "frozen<list<int>>"),
        ("c_frozen_set", "frozen<set<text>>"),
        ("c_frozen_map", "frozen<map<text, text>>"),
        ("c_list_frozen_scalar", "list<frozen<int>>"),
        ("c_set_frozen_udt", "set<frozen<person>>"),
        ("c_map_frozen_udt", "map<text, frozen<person>>"),
        ("c_frozen_list_frozen_udt", "frozen<list<frozen<person>>>"),
        ("c_frozen_frozen_list", "frozen<frozen<list<int>>>"),
    ];
    // The same shapes with a registry that DOES resolve the UDT.
    let resolvable = [
        ("r_frozen_udt", "frozen<address_type>"),
        ("r_list_frozen_udt", "list<frozen<address_type>>"),
        (
            "r_frozen_list_frozen_udt",
            "frozen<list<frozen<address_type>>>",
        ),
        (
            "r_frozen_map_frozen_udt",
            "frozen<map<text, frozen<address_type>>>",
        ),
        ("r_bare_udt", "address_type"),
    ];

    for (label, cols, registry) in [
        ("no-registry", &unresolvable[..], None),
        (
            "with-registry",
            &resolvable[..],
            Some(address_type_registry()),
        ),
    ] {
        let temp_dir = TempDir::new().expect("temp dir");
        let schema = schema_with("test_oa", "frozen_wrapper_probe", cols);
        let header = emitted_statistics_header(&temp_dir, &schema, registry).await;

        let heads = frozen_inner_heads(&header);
        let bad: Vec<&String> = heads
            .iter()
            .filter(|h| !FREEZE_WRAPPED_HEADS.contains(&h.as_str()))
            .collect();
        assert!(
            bad.is_empty(),
            "[{label}] CQLite emitted {} FrozenType(...) wrapper(s) Apache \
             Cassandra can never write: {bad:?}. Column types under test: {:?}",
            bad.len(),
            cols
        );
        eprintln!(
            "[#4158] {label}: {} column spelling(s) emitted, {} FrozenType( \
             occurrence(s) RECOGNISED, all within the Cassandra oracle set",
            cols.len(),
            heads.len()
        );
    }
}

// ---------------------------------------------------------------------------
// Half 3 — the exact byte string, re-derived from Cassandra's own header.
// ---------------------------------------------------------------------------

/// The exact `frozen<address_type>` marshal string Apache Cassandra 5.0 wrote
/// for `test_oa.udt_table`'s `address` column.
///
/// Extraction recipe (reproducible):
/// ```sh
/// strings -a "$CQLITE_DATASETS_ROOT"/sstables/test_oa/udt_table-*/oa-1-big-Statistics.db \
///   | grep -o 'org\.apache\.cassandra\.db\.marshal\.FrozenType(.*'
/// ```
/// Shape (`UserType.java:436-448` + `TypeParser.stringifyUserTypeParameters`):
/// plain-text keyspace, LOWERCASE-HEX UDT name, then `<hex-field-name>:<marshal>`
/// per field in DECLARED order, no spaces. 616464726573735f74797065 =
/// "address_type", 737472656574 = "street", 63697479 = "city",
/// 636f756e747279 = "country", 706f7374616c5f636f6465 = "postal_code".
const CASSANDRA_FROZEN_ADDRESS_TYPE: &str = "org.apache.cassandra.db.marshal.FrozenType(\
org.apache.cassandra.db.marshal.UserType(test_oa,616464726573735f74797065,\
737472656574:org.apache.cassandra.db.marshal.UTF8Type,\
63697479:org.apache.cassandra.db.marshal.UTF8Type,\
636f756e747279:org.apache.cassandra.db.marshal.UTF8Type,\
706f7374616c5f636f6465:org.apache.cassandra.db.marshal.UTF8Type))";

/// CQLite's emitted header for a top-level `frozen<UDT>` column must be
/// byte-identical to Cassandra's.
///
/// The committed literal is re-derived from Cassandra's OWN `Statistics.db`
/// whenever the corpus is reachable, so it can never silently become a
/// transcription of CQLite's output (#3042).
#[tokio::test]
async fn frozen_udt_header_matches_the_cassandra_written_string() {
    // (a) Re-derive the literal from Cassandra's bytes when the fixture is here.
    match datasets_root::resolve_table_generation_dir("test_oa", "udt_table") {
        Ok(dir) => {
            let mut found = false;
            for path in statistics_files(&dir) {
                let bytes = std::fs::read(&path).expect("Statistics.db readable");
                let hay = String::from_utf8_lossy(&bytes);
                if let Some(idx) = hay.find(&format!("{MARSHAL_PREFIX}FrozenType(")) {
                    let cassandra = &hay[idx..idx + CASSANDRA_FROZEN_ADDRESS_TYPE.len()];
                    assert_eq!(
                        cassandra,
                        CASSANDRA_FROZEN_ADDRESS_TYPE,
                        "the committed oracle literal no longer matches the \
                         Cassandra-written header at {}",
                        path.display()
                    );
                    found = true;
                }
            }
            assert!(
                found,
                "test_oa/udt_table resolved to {} but no FrozenType( was found \
                 in its Statistics.db",
                dir.display()
            );
            eprintln!(
                "[#4158] oracle literal RE-DERIVED from Cassandra bytes at {}",
                dir.display()
            );
        }
        Err(why) => {
            assert!(
                !fixtures_required(),
                "CQLITE_REQUIRE_FIXTURES=1 but test_oa/udt_table is absent: {why}"
            );
            eprintln!(
                "[#4158] oracle literal NOT re-derived (test_oa/udt_table absent: \
                 {why}); the writer assertion below still runs against the \
                 committed literal"
            );
        }
    }

    // (b) CQLite must emit exactly that string for the same declaration.
    let temp_dir = TempDir::new().expect("temp dir");
    let schema = schema_with(
        "test_oa",
        "udt_table_writer_parity",
        &[("address", "frozen<address_type>")],
    );
    let header = emitted_statistics_header(&temp_dir, &schema, Some(address_type_registry())).await;
    assert!(
        header.contains(CASSANDRA_FROZEN_ADDRESS_TYPE),
        "emitted Statistics.db does not carry Cassandra's frozen<UDT> marshal \
         string.\nexpected: {CASSANDRA_FROZEN_ADDRESS_TYPE}\nFrozenType inner \
         heads emitted: {:?}",
        frozen_inner_heads(&header)
    );
}
