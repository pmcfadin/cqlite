//! Issue #3805 slice 2 — a Cassandra-legal EMPTY FIXED-WIDTH multicell MAP KEY
//! must reach a `SELECT` as the TYPED [`Value::Empty`] sentinel.
//!
//! # The defect this lane covers
//! A non-frozen `map<K,V>` is MULTICELL: the KEY travels in each cell's CellPath
//! as `[VInt length][bare serialized key]`, so a ZERO-LENGTH cell path means the
//! key's serialized form is the EMPTY buffer. Cassandra's fixed-width serializers
//! spell their width check `size != N && !isEmpty`, so that buffer is LEGAL data —
//! and #3612's width table already admitted it. What was missing was somewhere to
//! put it: no `Value` carried an empty fixed-width scalar, so #3747 applied this
//! reader's policy for an unmodellable key (an OPAQUE `Value::Blob` plus a
//! per-column `warn!`) and recorded the seam in the decoder's own comment
//! ("Typed: #3805"). #3805 slice 1 added `Value::Empty(EmptyValueType)`; slice 2
//! (roborev job 447, finding F1) spends it in the decoder.
//!
//! # Oracle — Cassandra-WRITTEN bytes, never CQLite's own output
//! Fixture:
//! `test-data/fixtures/issue_3805/test_empty_fixedwidth_key/empty_fixedwidth_map_key-*/`
//! written by cassandra:5.0.2, with its `sstabledump` golden
//! `nb-1-big-Data.db.jsonl`; schema
//! `test-data/schemas/issue-3805-empty-fixedwidth-map-key.cql`; measured
//! behaviour in `docs/round-artifacts/issue-3805-cassandra-oracle.md`. Every
//! expectation below is quoted from that golden or from `cassandra-5.0.8` source.
//! A CQLite-written + CQLite-read round trip could NOT settle this (CLAUDE.md,
//! #3042): both sides would make the identical framing mistake and the round trip
//! would close while real Cassandra data still read wrong.
//!
//! Golden, id 1 (the SUBJECT row) — every map column carries an EMPTY key
//! alongside a NON-EMPTY sibling, so a failure is legible (a map SHORT ONE ENTRY
//! is distinguishable from a missing column):
//! ```text
//! m_int    path:[""]=7  path:["42"]=1
//! m_bigint path:[""]=7  path:["99"]=1
//! m_uuid   path:[""]=7  path:["123e4567-e89b-12d3-a456-426614174000"]=1
//! m_bool   path:[""]=7  path:["true"]=1
//! m_inet   path:[""]=7  path:["10.0.0.1"]=1
//! m_dec    path:[""]=7  path:["1.5"]=1
//! m_text   path:[""]=7  path:["k"]=1
//! m_frozen value:{"":1000,"7":2000}          (ONE inline cell, no CellPath)
//! ```
//! Golden, id 2 (the CONTRAST row) — no empty key anywhere.
//!
//! # Which families are typed, which stay NATIVE, and which stay REFUSED
//! The line is `EmptyValueType::for_cql_type`, drawn on Cassandra's `validate()`
//! and never on decodability:
//!   * `int`/`bigint`/`uuid`/`boolean` (and the rest of the `N`-or-`0` set) →
//!     `Value::Empty(tag)`. THE SUBJECT.
//!   * `text` → `Value::Text(b"")`. An empty buffer is a legal, MEANINGFUL value
//!     for a text type (`AbstractTextSerializer.java:72-77` overrides `isNull`
//!     precisely to say so), represented natively, NEVER as a sentinel. This is
//!     the contrast that proves the change did not go too wide.
//!   * `inet` → `Value::Inet(b"")`. Pre-existing and unchanged: its serializer
//!     RETURNS EARLY on empty, so the decode succeeds and never reaches the arm
//!     slice 2 changed. It must never be described as a second instance of the
//!     defect.
//!   * `tinyint`/`smallint`/`date`/`time` are ABSENT from the fixture BY
//!     CONSTRUCTION — `blobAsX(0x)` is refused by cqlsh for exactly those four,
//!     so Cassandra cannot write such a key. This lane says nothing about them
//!     (the width table refuses them; that is unit-covered).
//!
//! # WHAT MUST NOT BE ASSERTED (MEASURED)
//! Each multicell column also carries a collection tombstone (an INSERT of a whole
//! non-frozen collection REPLACES it) whose `local_delete_time` is a WALL CLOCK no
//! CQL clause can pin. Never assert on it and never byte-compare the golden across
//! regenerations. Entry SET membership is the assertion subject, not cell order.

#![cfg(feature = "lz4")]
//  ^ SOURCE-level gate, deliberately NOT a `[[test]] required-features` entry —
//  same reasoning as `issue_3747_empty_map_key.rs`: the fixture is real Cassandra
//  output and LZ4-compressed (`CompressionInfo.db` present), so reading it needs
//  `lz4`, and without the feature the target would compile and then fail at
//  runtime on a blob it cannot decompress — a false red, not a signal. A
//  `[[test]]` entry is wrong because this file is in the package `exclude` list
//  (it fails closed on a fixture outside the crate), and the repo pattern is
//  exclude OR declare, never both. `lz4` arrives via the DEFAULT `all-compression`
//  set, so no lane silently executes zero tests here (the #3375 hazard).

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

#[cfg(feature = "cli-helpers")]
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::types::{EmptyValueType, Value};
#[cfg(feature = "cli-helpers")]
use cqlite_core::Config;

// TABLE-granular fixture resolution (#3220). `first_root_with_table` is the PURE
// form of `sstables_root_for_table`, factored precisely so the selection rule can
// be applied to a candidate list this lane supplies — which it must, because this
// fixture is COMMITTED SOURCE under `test-data/fixtures/`, not part of the fetched
// `test-data/datasets` corpus. Selecting by KEYSPACE and committing to that root
// is the #3220 defect; selecting by EVIDENCE (which root actually carries
// `<keyspace>/<table>-*/*-Data.db`) is what this helper does.
#[path = "support/datasets_root.rs"]
mod datasets_root;

const KEYSPACE: &str = "test_empty_fixedwidth_key";
const TABLE: &str = "empty_fixedwidth_map_key";
const SCHEMA: &str = "issue-3805-empty-fixedwidth-map-key.cql";

/// THE DECLARED BLOCKERS — the two out-of-scope decode refusals that make this
/// fixture unreadable through EVERY public read surface today, each with the
/// issue that owns it.
///
/// # Why this list exists instead of a silently-skipped test
/// Both are HARD `ColumnDecode` errors out of `Database::execute` (NOT the
/// `row_data.rs` complex-column swallow), so they fail the WHOLE `SELECT` and no
/// projection avoids them — MEASURED on both `Database::execute` and
/// `SSTableReader::iterate_all_partitions_for_compaction`. Neither is caused by
/// the cell-path arm slice 2 changed, and neither may be fixed here:
///
///   * **m_frozen** `frozen<map<int,int>>` — its empty key is an INLINE
///     length-prefixed element with no CellPath at all, refused by
///     `row_decoder/raw_value/reporting.rs::require_fixed_width`. That
///     function's own docblock states the refusal is pre-existing and that
///     widening it "is a behaviour change with its own oracle and its own corpus
///     measurement: **issue #3847**. Do not 'fix' it by relaxing this guard
///     alone." Tracked for the frozen/inline path as **#4071**.
///   * **m_dec** `map<decimal,int>` — `decimal`'s allowed widths are the EMPTY
///     slice (it is variable-width: Cassandra accepts `0` or `>= 4`), so the
///     cell-path arm's guard `allowed.contains(&0)` is false and `decimal` never
///     reaches it. Typing it would move that arm's ADMISSION GATE from the width
///     table to the tag table — `EmptyValueType::for_cql_type(&CqlType::Decimal)`
///     is already `Some`, and both `empty_value.rs` and this fixture's schema
///     record that "empty decimal is corrupt" was a WRONG committed claim — and
///     that is a deliberate scope decision, not an oversight.
///
/// So this lane asserts the FULL golden the moment the read succeeds, and until
/// then it requires the failure to be EXACTLY one of these. It can therefore
/// never green vacuously, and it REDS as soon as either blocker lands, which is
/// what forces the full assertions to be turned on rather than remembered.
const DECLARED_BLOCKERS: &[(&str, &str, &str)] = &[
    (
        "m_frozen",
        "need 4 byte(s) for int, got 0",
        "#3847/#4071 — inline frozen element, require_fixed_width refuses a legal \
         empty fixed-width value",
    ),
    (
        "m_dec",
        "decimal too short (0 bytes)",
        "#3805 residual — decimal is variable-width so its allowed-width slice is \
         empty, and the cell-path arm's admission gate is that slice",
    ),
];

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core always has a workspace parent directory")
        .to_path_buf()
}

/// Candidate "sstables roots" for this table, in order: this issue's COMMITTED
/// fixture tree first, then whatever the machine's dataset roots are. The order
/// only breaks ties — [`datasets_root::first_root_with_table`] picks by EVIDENCE.
fn candidate_roots() -> Vec<PathBuf> {
    let mut roots = vec![repo_root().join("test-data/fixtures/issue_3805")];
    roots.extend(datasets_root::sstables_root_candidates());
    roots
}

/// The root that really carries this table's bytes. FAIL-CLOSED, never a SKIP:
/// the fixture is git-tracked source (its binaries are force-added), so an
/// absence is a broken checkout, not a missing optional corpus (#3220).
fn fixture_root() -> PathBuf {
    datasets_root::first_root_with_table(&candidate_roots(), KEYSPACE, TABLE)
        .map(Path::to_path_buf)
        .unwrap_or_else(|| {
            panic!(
                "no *-Data.db for {KEYSPACE}.{TABLE} under any candidate root {:?} — this \
                 fixture is git-tracked source and must never be absent",
                candidate_roots()
            )
        })
}

fn schema_path() -> PathBuf {
    datasets_root::schema_path(SCHEMA).unwrap_or_else(|| {
        panic!("committed CQL schema {SCHEMA} is unreadable (#3148 resolves it checkout-relative)")
    })
}

/// `(id, column) -> Value` for every fixture row via the PUBLIC
/// `Database::execute` SELECT surface, or the decode error verbatim.
#[cfg(feature = "cli-helpers")]
async fn select_star() -> Result<Vec<(i32, HashMap<String, Value>)>, String> {
    let config = IngestionConfig {
        schema_paths: vec![schema_path()],
        data_dir: fixture_root(),
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: None,
    };
    let db = ingest(config)
        .await
        .map_err(|e| format!("ingest of the committed fixture failed: {e:?}"))?
        .database;
    // `SELECT *`, never a named subset: a projection over named columns cannot
    // see a TRUNCATED row, because a cell that failed to decode is simply ABSENT
    // from the row's map and a `get(col)` over the columns you named never
    // notices (#3890).
    let result = db
        .execute(&format!("SELECT * FROM {KEYSPACE}.{TABLE}"))
        .await
        .map_err(|e| format!("{e:?}"))?;
    if result.rows.is_empty() {
        return Err(
            "committed fixture decoded ZERO rows (0-rows-when-present is a failure)".to_string(),
        );
    }
    Ok(result
        .rows
        .iter()
        .map(|r| {
            let id = match r.values.get("id") {
                Some(Value::Integer(i)) => *i,
                other => panic!("row without an integer id: {other:?}"),
            };
            (
                id,
                r.values
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.clone()))
                    .collect(),
            )
        })
        .collect())
}

/// Every column the golden carries, so BOTH DIRECTIONS of the column set can be
/// asserted (#3890): no golden column absent from the decoded row, and no
/// decoded column the golden does not have.
const GOLDEN_COLUMNS: &[&str] = &[
    "id", "m_int", "m_bigint", "m_uuid", "m_bool", "m_inet", "m_dec", "m_text", "m_frozen",
];

/// The map entries of `column` in row `id`, keyed by a rendering that ORDERS the
/// empty key first, so membership is compared without depending on cell order.
/// Fail-closed: a missing row, a missing column, a NULL cell or a non-map value
/// is a decode regression, never a skip.
#[cfg(feature = "cli-helpers")]
fn map_entries(
    rows: &[(i32, HashMap<String, Value>)],
    id: i32,
    column: &str,
) -> BTreeMap<String, Value> {
    let row = rows
        .iter()
        .find(|(k, _)| *k == id)
        .map(|(_, r)| r)
        .unwrap_or_else(|| panic!("fixture row id={id} missing"));
    let v = row
        .get(column)
        .unwrap_or_else(|| panic!("row id={id} has no column {column}"));
    let entries = match peel_frozen(v) {
        Value::Map(entries) => entries,
        other => panic!("row id={id} column {column} is not a Map: {other:?}"),
    };
    entries
        .into_iter()
        .map(|(k, val)| (format!("{k:?}"), val))
        .collect()
}

fn peel_frozen(v: &Value) -> Value {
    match v {
        Value::Frozen(inner) => peel_frozen(inner),
        other => other.clone(),
    }
}

/// Assert BOTH directions of the column set, naming any missing column (#3890).
#[cfg(feature = "cli-helpers")]
fn assert_column_set(rows: &[(i32, HashMap<String, Value>)]) {
    for (id, cols) in rows {
        let missing: Vec<&str> = GOLDEN_COLUMNS
            .iter()
            .copied()
            .filter(|c| !cols.contains_key(*c))
            .collect();
        assert!(
            missing.is_empty(),
            "row id={id}: golden columns MISSING from the decoded row: {missing:?} — a cell \
             that fails to decode is simply absent from the row map, so this is the only \
             assertion that can see a truncated row"
        );
        let extra: Vec<&str> = cols
            .keys()
            .map(String::as_str)
            .filter(|c| !GOLDEN_COLUMNS.contains(c))
            .collect();
        assert!(
            extra.is_empty(),
            "row id={id}: decoded columns the golden does not carry: {extra:?}"
        );
    }
}

/// Which declared blocker (if any) this decode error is, by NAMED column AND
/// message substring — never by column alone, so a DIFFERENT failure of the same
/// column is not excused.
fn declared_blocker(err: &str) -> Option<&'static (&'static str, &'static str, &'static str)> {
    DECLARED_BLOCKERS
        .iter()
        .find(|(col, needle, _)| err.contains(col) && err.contains(needle))
}

// ════════════════════════════════════════════════════════════════════════════
// THE SUBJECT — the empty fixed-width key through the public SELECT surface
// ════════════════════════════════════════════════════════════════════════════

/// The headline case. When the fixture reads, every fixed-width family's empty
/// key must be the TYPED sentinel with its OWN tag, `m_text`'s must be the NATIVE
/// empty text, and the column set must be complete in both directions.
///
/// Until then the read must fail with EXACTLY one of [`DECLARED_BLOCKERS`] —
/// which it does today (m_frozen, #3847/#4071). Any other error, or a partial
/// row, FAILS.
///
/// MEASURED post-fix through this same surface, with the two declared blockers
/// stepped past by a throwaway schema (m_frozen/m_dec re-declared
/// `map<blob,int>`, NOT committed — the schema is the fixture's oracle and is not
/// edited to make a test pass): `m_int = Map([(Empty(Int), 7), (Integer(42), 1)])`,
/// `m_bigint = Map([(Empty(BigInt), 7), (BigInt(99), 1)])`,
/// `m_uuid = Map([(Empty(Uuid), 7), (Uuid(123e4567-…), 1)])`,
/// `m_bool = Map([(Empty(Boolean), 7), (Boolean(true), 1)])`,
/// `m_inet = Map([(Inet(b""), 7), (Inet(10.0.0.1), 1)])`,
/// `m_text = Map([(Text(b""), 7), (Text("k"), 1)])`. Pre-fix the four sentinels
/// were each `Blob(b"")` — one opaque spelling for four distinct declared types.
#[cfg(feature = "cli-helpers")]
#[tokio::test]
async fn an_empty_fixed_width_map_key_reads_as_the_typed_sentinel() {
    let rows = match select_star().await {
        Ok(rows) => rows,
        Err(err) => {
            let Some((column, _, owner)) = declared_blocker(&err) else {
                panic!(
                    "the committed fixture failed to read with an UNDECLARED error — this is \
                     not one of the two known out-of-scope blockers, so it is a regression: \
                     {err}"
                );
            };
            eprintln!(
                "DECLARED BLOCKER: the {column} column still refuses its empty key ({owner}), \
                 so the per-column assertions below are NOT YET REACHED. This lane REDS the \
                 moment that blocker lands, which is what turns them on."
            );
            return;
        }
    };

    assert_column_set(&rows);

    // The SUBJECT row. Each empty key is the sentinel of its OWN declared family:
    // four distinct values where the pre-fix decoder produced one opaque `Blob`.
    let expected: &[(&str, EmptyValueType, Value)] = &[
        ("m_int", EmptyValueType::Int, Value::Integer(42)),
        ("m_bigint", EmptyValueType::BigInt, Value::BigInt(99)),
        ("m_bool", EmptyValueType::Boolean, Value::Boolean(true)),
    ];
    for (column, tag, sibling) in expected {
        let entries = map_entries(&rows, 1, column);
        let empty = entries
            .get(&format!("{:?}", Value::Empty(*tag)))
            .unwrap_or_else(|| {
                panic!(
                    "row id=1 {column}: the golden's path:[\"\"] entry must decode to \
                     Empty({tag:?}) — got {entries:?}"
                )
            });
        assert_eq!(
            *empty,
            Value::Integer(7),
            "row id=1 {column}: the golden's empty key carries value 7"
        );
        // The NON-EMPTY sibling is what makes a failure legible: a map short one
        // entry is distinguishable from a missing column.
        assert!(
            entries.contains_key(&format!("{sibling:?}")),
            "row id=1 {column}: the non-empty sibling key {sibling:?} must still decode: \
             {entries:?}"
        );
        // DISTINCT FROM NULL and from the type's ZERO value — the whole point of a
        // sentinel (oracle §4b.3: Cassandra returns the key PRESENT and NOT NULL).
        assert!(
            !entries.contains_key(&format!("{:?}", Value::Null)),
            "row id=1 {column}: an empty key is NOT null: {entries:?}"
        );
    }

    // uuid separately: its 16-byte sibling is spelled as a byte array.
    let m_uuid = map_entries(&rows, 1, "m_uuid");
    assert!(
        m_uuid.contains_key(&format!("{:?}", Value::Empty(EmptyValueType::Uuid))),
        "row id=1 m_uuid: the empty key must be Empty(Uuid): {m_uuid:?}"
    );
    assert_eq!(m_uuid.len(), 2, "row id=1 m_uuid: golden has two entries");

    // NATIVE, NOT a sentinel — the contrast that proves the change did not go too
    // wide. An empty buffer is a legal MEANINGFUL value for a text type, and
    // `inet`'s serializer returns early on empty so its decode never reached the
    // arm slice 2 changed.
    let m_text = map_entries(&rows, 1, "m_text");
    assert_eq!(
        m_text.get(&format!("{:?}", Value::text(""))),
        Some(&Value::Integer(7)),
        "row id=1 m_text: the empty key is the NATIVE empty text, never the sentinel: \
         {m_text:?}"
    );
    for tag in [
        EmptyValueType::Int,
        EmptyValueType::BigInt,
        EmptyValueType::Uuid,
        EmptyValueType::Boolean,
    ] {
        assert!(
            !m_text.contains_key(&format!("{:?}", Value::Empty(tag))),
            "row id=1 m_text must carry NO sentinel of any family: {m_text:?}"
        );
    }
    let m_inet = map_entries(&rows, 1, "m_inet");
    assert_eq!(
        m_inet.get(&format!("{:?}", Value::inet(Vec::<u8>::new()))),
        Some(&Value::Integer(7)),
        "row id=1 m_inet: pre-existing and UNCHANGED — Inet(b\"\"), never the sentinel: \
         {m_inet:?}"
    );

    // THE CONTRAST ROW. No empty key anywhere in the golden, so no sentinel may
    // appear: the fix is a property of the empty-key path, not of the data.
    for column in ["m_int", "m_bigint", "m_uuid", "m_bool", "m_inet", "m_text"] {
        let entries = map_entries(&rows, 2, column);
        assert_eq!(
            entries.len(),
            1,
            "row id=2 {column}: the golden carries exactly one entry: {entries:?}"
        );
        assert!(
            !entries.keys().any(|k| k.starts_with("Empty(")),
            "row id=2 {column}: the CONTRAST row has no empty key, so no sentinel may \
             appear: {entries:?}"
        );
    }
}

// ════════════════════════════════════════════════════════════════════════════
// The declared-blocker list is itself asserted, so it cannot rot into an excuse
// ════════════════════════════════════════════════════════════════════════════

/// A blocker entry must NAME a column and a message substring and cite an owner,
/// and the list must stay SHORT — an entry is an admission that this fixture's
/// read is incomplete, so the set is pinned by a FLOOR *and* a CEILING (#3544's
/// case-floor lesson: a green tally over a shrunken table is no evidence).
///
/// This needs no fixture and no features, so it executes in every lane that runs
/// this package.
#[test]
fn the_declared_blocker_list_is_specific_and_bounded() {
    assert_eq!(
        DECLARED_BLOCKERS.len(),
        2,
        "exactly two blockers are declared (m_frozen: #3847/#4071, m_dec: #3805 \
         residual). ADDING one is an admission that this lane covers LESS; REMOVING \
         one means the read now works and the assertions must be turned on."
    );
    for (column, needle, owner) in DECLARED_BLOCKERS {
        assert!(
            column.starts_with("m_"),
            "a blocker must name a fixture COLUMN, got {column:?}"
        );
        assert!(
            needle.len() > 10,
            "a blocker's message substring must be specific enough that a DIFFERENT \
             failure of the same column is not excused, got {needle:?}"
        );
        assert!(
            owner.contains('#'),
            "a blocker must cite the issue that owns it, got {owner:?}"
        );
    }
    // The matcher requires BOTH the column and the message; a column name alone
    // must never excuse a failure.
    assert!(
        declared_blocker("ColumnDecode { column: \"m_frozen\", source: Corruption(\"boom\") }")
            .is_none(),
        "a DIFFERENT failure of a declared column must NOT be excused"
    );
    assert!(
        declared_blocker(
            "ColumnDecode { column: \"m_frozen\", source: Corruption(\"Frozen element \
             'map 'm_frozen' key 0': need 4 byte(s) for int, got 0\") }"
        )
        .is_some(),
        "the real m_frozen blocker must be recognised"
    );
}

/// The committed fixture and its schema must both be present and usable — a
/// dataset-dependent lane that can pass on an absent corpus is a failure, and
/// this fixture is git-tracked source rather than an optional fetched corpus.
#[test]
fn the_committed_fixture_and_schema_are_present() {
    let root = fixture_root();
    let generations = datasets_root::table_generation_dirs(&root, KEYSPACE, TABLE);
    assert_eq!(
        generations.len(),
        1,
        "expected exactly one {TABLE}-* generation carrying a *-Data.db under {root:?}, \
         got {generations:?}"
    );
    let schema = schema_path();
    let cql = std::fs::read_to_string(&schema)
        .unwrap_or_else(|e| panic!("committed schema {schema:?} unreadable: {e}"));
    for column in GOLDEN_COLUMNS {
        assert!(
            cql.contains(column),
            "the committed schema must declare {column}; it is the oracle for this lane \
             and is never edited to make a test pass"
        );
    }
}
