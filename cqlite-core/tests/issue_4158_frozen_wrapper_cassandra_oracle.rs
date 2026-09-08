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
/// from bytes Cassandra produced.
///
/// THE READ-SIDE COMPLEMENT ("the read gate never refuses a byte Cassandra wrote")
/// cannot live here: `validate_marshal_frozen` is `pub(crate)`, so it is pinned by
/// the unit test `frozen_scalar_tests::
/// the_header_gate_never_refuses_a_frozentype_cassandra_wrote`, which walks the same
/// corpus and asserts the read gate ACCEPTS every complete `FrozenType(...)` string
/// it finds (#4158 review, blocker C's over-refusal guard). This half stays the
/// WRITER oracle. Measured on this fleet's corpus (144
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
    try_emitted_statistics_header(temp_dir, schema, registry)
        .await
        .unwrap_or_else(|e| panic!("the write path should emit a Statistics.db: {e}"))
}

/// The fallible form, for the halves that assert the writer REFUSES (#4104): a
/// refusal is the behaviour under test, so it must not reach the harness as a
/// panic.
async fn try_emitted_statistics_header(
    temp_dir: &TempDir,
    schema: &TableSchema,
    registry: Option<UdtRegistry>,
) -> Result<String, String> {
    let mut config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );
    if let Some(registry) = registry {
        config = config.with_udt_registry(registry);
    }
    let mut engine = WriteEngine::new(config).map_err(|e| format!("engine creation: {e}"))?;

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
        .map_err(|e| format!("write: {e}"))?;
    let info = engine
        .flush()
        .await
        .map_err(|e| format!("flush: {e}"))?
        .ok_or_else(|| "flush returned no SSTableInfo".to_string())?;

    let stats_path = info.data_path.with_file_name(
        info.data_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| "Data.db file name".to_string())?
            .replace("Data.db", "Statistics.db"),
    );
    let bytes = std::fs::read(&stats_path).map_err(|e| format!("reading {stats_path:?}: {e}"))?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
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

    // PER-LEG WRAPPER FLOOR — the affirmative-zero half of this assertion (#4158
    // review). `bad.is_empty()` alone passes VACUOUSLY if the writer regresses to
    // emitting no `FrozenType(` at all, which is what the #4158 defect looked like
    // from one side. The floors are the wrappers each battery must produce:
    //
    //   no-registry (5): `frozen<list<int>>`, `frozen<set<text>>`,
    //     `frozen<map<text,text>>`, `frozen<list<frozen<person>>>` and
    //     `frozen<frozen<list<int>>>` each wrap ONCE (`ListType` x3, `SetType`,
    //     `MapType`). The rest wrap nothing: `frozen<int>`/`frozen<text>` are
    //     scalars, `frozen<person>` resolves nowhere, a `TupleType` is never
    //     wrapped, and a wrapper's own children print with `ignoreFreezing = true`.
    //   with-registry (3): `frozen<address_type>` -> `FrozenType(UserType(..))`,
    //     `frozen<list<frozen<address_type>>>` -> `FrozenType(ListType(..))`,
    //     `frozen<map<text, frozen<address_type>>>` -> `FrozenType(MapType(..))`.
    //
    // FLOORS (`>=`), deliberately not equalities: `list<frozen<udt>>` is a MULTICELL
    // parent, so Cassandra writes `ListType(FrozenType(UserType(..)))` for it —
    // corpus-attested in `test_collections/collections_with_udts` and
    // `test_types/cx_nested_frozen_collections` — while CQLite currently emits the
    // wrapper-less `ListType(UserType(..))`, because the registry-aware column
    // dispatch is scoped to a TOP-LEVEL `frozen<...>` (`schema_helpers`'s own scope
    // note, issue #1020). That is a MISSING wrapper, the opposite direction from the
    // impossible wrapper this file exists to pin, and it is not asserted here.
    for (label, cols, registry, min_wrappers) in [
        ("no-registry", &unresolvable[..], None, 5usize),
        (
            "with-registry",
            &resolvable[..],
            Some(address_type_registry()),
            3usize,
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
        assert!(
            heads.len() >= min_wrappers,
            "[{label}] only {} FrozenType( occurrence(s) RECOGNISED ({heads:?}), \
             below the floor of {min_wrappers} this battery must emit — the \
             assertion above would then be vacuously satisfied. Column types under \
             test: {:?}",
            heads.len(),
            cols
        );
        eprintln!(
            "[#4158] {label}: {} column spelling(s) emitted, {} FrozenType( \
             occurrence(s) RECOGNISED {heads:?} (floor {min_wrappers}), all within \
             the Cassandra oracle set",
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
                    // `get`, never `&hay[a..b]`: the window can run past the end of
                    // the buffer, and `from_utf8_lossy` emits 3-byte U+FFFD over
                    // binary so an index need not be a char boundary. A slice panic
                    // there would replace a real oracle MISMATCH with an opaque
                    // "byte index is not a char boundary" (#4158 review).
                    let cassandra = hay.get(idx..idx + CASSANDRA_FROZEN_ADDRESS_TYPE.len());
                    let cassandra = cassandra.unwrap_or_else(|| {
                        panic!(
                            "the FrozenType( at byte {idx} of {} does not span {} \
                             readable bytes (buffer is {} long) — the header is \
                             truncated or the literal no longer matches",
                            path.display(),
                            CASSANDRA_FROZEN_ADDRESS_TYPE.len(),
                            hay.len()
                        )
                    });
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

// ---------------------------------------------------------------------------
// Half 3 — the PASS-THROUGH route into the header (#4104, roborev job 121).
// ---------------------------------------------------------------------------

/// `TableSchema::data_type` is a `String`, so a schema can declare a column type
/// that is ALREADY a marshal string — and such a string was returned verbatim by
/// the converter before any validation. This is half 2's assertion made against
/// the route half 2 cannot reach: every spelling in its battery is CQL
/// (`frozen<int>`), so none of them exercises the pass-through at all.
///
/// The writer must REFUSE, and nothing under the write directory may end up
/// carrying the impossible wrapper.
#[tokio::test]
async fn a_schema_declaring_a_marshal_frozentype_scalar_is_refused_by_the_writer() {
    const IMPOSSIBLE: &str =
        "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.Int32Type)";

    let temp_dir = TempDir::new().expect("temp dir");
    let schema = schema_with(
        "test_oa",
        "frozen_passthrough_probe",
        &[("c_marshal_frozen_scalar", IMPOSSIBLE)],
    );

    let err = match try_emitted_statistics_header(&temp_dir, &schema, None).await {
        Err(err) => err,
        Ok(header) => panic!(
            "the writer ACCEPTED a schema declaring {IMPOSSIBLE}; the emitted \
             Statistics.db carries {} FrozenType( occurrence(s) {:?}, and Apache \
             Cassandra can print a wrapper only around {FREEZE_WRAPPED_HEADS:?}",
            frozen_inner_heads(&header).len(),
            frozen_inner_heads(&header)
        ),
    };
    assert!(
        err.contains("FrozenType"),
        "the refusal must name the type it refused: {err}"
    );

    // And no bytes escaped: a refusal that still leaves the header on disk is
    // not a refusal.
    for path in statistics_files(temp_dir.path()) {
        let bytes = std::fs::read(&path).expect("Statistics.db should be readable");
        let hay = String::from_utf8_lossy(&bytes);
        let heads = frozen_inner_heads(&hay);
        let bad: Vec<&String> = heads
            .iter()
            .filter(|h| !FREEZE_WRAPPED_HEADS.contains(&h.as_str()))
            .collect();
        assert!(
            bad.is_empty(),
            "{} was written despite the refusal and carries {bad:?}",
            path.display()
        );
    }
}

/// The counterpart, so the refusal above is not a blanket ban on the
/// pass-through: a LEGAL already-marshaled `FrozenType(ListType(..))` — a shape
/// Cassandra really writes, per half 1's census — still reaches the emitted
/// header BYTE FOR BYTE, mixed case intact.
///
/// Case matters on this route: the marshal grammar is case-sensitive
/// (`TypeParser` resolves class names verbatim), and a case-folded pass-through
/// is a defect this area has already had (a lowercased `usertype(...)`, #4158).
#[tokio::test]
async fn a_legal_marshal_frozentype_reaches_the_header_verbatim() {
    const LEGAL: &str = "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type))";

    let temp_dir = TempDir::new().expect("temp dir");
    let schema = schema_with(
        "test_oa",
        "frozen_passthrough_legal",
        &[("c_marshal_frozen_list", LEGAL)],
    );
    let header = emitted_statistics_header(&temp_dir, &schema, None).await;

    assert!(
        header.contains(LEGAL),
        "the emitted SerializationHeader must carry the declared marshal string \
         unchanged and un-case-folded; it does not"
    );
    let heads = frozen_inner_heads(&header);
    assert_eq!(
        heads,
        vec!["ListType".to_string()],
        "exactly the one wrapper this column declares, drawn from the Cassandra \
         oracle set — an affirmative census, not merely the absence of a bad one"
    );
}
