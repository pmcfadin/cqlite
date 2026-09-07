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
//!   * `inet` → `Value::Empty(Inet)`, and `varint` likewise. Their DECODE always
//!     succeeded (`InetAddressSerializer.java:52-55` returns early on empty;
//!     `IntegerSerializer.java:31-34` accepts everything), so slice 2's gate —
//!     which fired only on a decode FAILURE — never reached them and they kept
//!     the family's own native empty spelling. `for_cql_type` ADMITS both, so
//!     that was a SECOND spelling of one value (#4079), and roborev job 449
//!     finding C closed it by consulting the gate BEFORE the decode. Note what
//!     this is and is not: it is a REPRESENTATION defect, never the
//!     refuse-or-blob defect the committed schema's `m_inet` note denies — CQLite
//!     always decoded an empty `inet` key, and this lane still proves it does.
//!     It matters because the representation was USER-VISIBLE: both bindings
//!     REJECT a zero-length `Inet` (`cqlite_ffi_common::inet::inet_kind` admits
//!     only 4 and 16, with no passthrough branch by #28 mandate; pinned by
//!     `inet/malformed-empty` in `cqlite-ffi-common/src/vectors/tables.rs`, a
//!     table driven through the FULL Python and Node value dispatches), while
//!     `Value::Empty(_)` renders as `""` on both
//!     (`bindings/python/src/value.rs:52`, `bindings/node/src/value.rs:217`) —
//!     which is what `sstabledump` and `SELECT JSON` emit.
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

// THE DECLARED-BLOCKER MACHINERY IS GONE, and its REMOVAL is the point.
//
// Until #3847 landed, `m_frozen`'s empty key — an INLINE length-prefixed
// element with no CellPath at all — was refused by
// `row_decoder/raw_value/reporting.rs::require_fixed_width` with a HARD
// `ColumnDecode` that failed the WHOLE `SELECT`, so no projection avoided it and
// no lane could reach a single per-column assertion under the COMMITTED schema.
// This file therefore carried three pieces of machinery: a `DECLARED_BLOCKERS`
// table naming that ONE refusal by column AND message substring, a
// throwaway-schema substitution (`m_frozen frozen<map<int, int>>` →
// `frozen<map<blob, int>>`, which routed the column away from the fixed-width
// guard while keeping the row layout) so the subject lane could assert the seven
// MULTICELL columns anyway, and a SIGNPOST test asserting that the committed
// declaration still failed with exactly that blocker — so this file would RED
// the moment the blocker was fixed.
//
// It did. #3847 (merged as PR #4017, `a5171a5ba`) widened `require_fixed_width`
// to admit a legal empty fixed-width value; the signpost fired, and the
// substitution was therefore REMOVED rather than remembered — the only outcome
// the signpost existed to force. The subject lane below now reads the COMMITTED
// schema, unsubstituted. `m_dec` was the other blocker (its allowed widths are
// the EMPTY slice, so the WIDTH table could never admit its empty buffer) and
// slice 2 closed it by moving the cell-path arm's admission gate from the width
// table to the TAG table (`for_cql_type`, derived from `validate()`); both
// columns are now read at the type the oracle declares.
//
// The `m_inet` → `varint` substitution that SURVIVES has a different and
// unrelated justification — no committed Cassandra-written fixture in this
// repository carries a `map<varint, …>` column, see [`INET_DECL_AS_VARINT`]. It
// is not a blocker workaround and nothing about it was obsoleted by #3847.

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

/// THE ONE REMAINING DECLARED SUBSTITUTION, used by ONE lane: `m_inet`
/// re-declared as `map<varint, int>`. It is applied to a COPY of the committed
/// schema — never to the committed schema itself, which is this fixture's oracle
/// and is never edited to make a test pass.
///
/// # Why a substitution rather than a fixture column
/// `varint` is the OTHER family roborev job 449 finding C normalizes (#4079),
/// and no committed Cassandra-written fixture in this repository carries a
/// `map<varint, …>` column with an empty key — regenerating fixtures is out of
/// this change's scope. What the substitution reinterprets is real
/// Cassandra-written bytes, and for the SUBJECT of this lane it reinterprets
/// nothing at all: **the empty cell path is zero bytes under every declared key
/// type**, so "Cassandra wrote a zero-length cell path and CQLite must render it
/// as `Empty(Varint)` when the declared key type is `varint`" is tested on the
/// real thing. Only the SIBLING entry is a reinterpretation — the four bytes
/// `0a 00 00 01` that mean `10.0.0.1` under `inet` mean the integer 167772161
/// under `varint` — so this lane asserts that sibling only as the RAW BYTES it
/// is, and asserts nothing that depends on `inet` semantics.
///
/// The unit-level counterpart, on synthesised bytes, is
/// `regression_3747_empty_map_key_tests::varint_and_inet_empty_keys_are_the_typed_sentinel_closing_4079`.
#[cfg(feature = "cli-helpers")]
const INET_DECL_COMMITTED: &str = "m_inet   map<inet, int>";
#[cfg(feature = "cli-helpers")]
const INET_DECL_AS_VARINT: &str = "m_inet   map<varint, int>";

/// Write a COPY of the committed schema into `dir`, with each `(from, to)`
/// applied EXACTLY ONCE, and return its path.
///
/// Isolation: the result is handed to `IngestionConfig::schema_paths`, a
/// PER-READ parameter, so nothing process-wide is touched. `CQLITE_SCHEMAS_ROOT`
/// is deliberately NOT used — it is a process-wide, must-be-absolute env var, so
/// a test setting it would poison every peer test in the same binary (#3382's
/// process-global-state lesson) and would need `#[serial_test::serial]` plus a
/// restore to be merely survivable.
///
/// FAIL-CLOSED on every substitution: EXACTLY ONE occurrence of each `from` must
/// be found and none may survive (a reflowed or renamed schema must RED here,
/// never silently produce an unsubstituted copy), and `m_dec`'s real `decimal`
/// declaration must survive whatever the caller asked for — the admission-gate
/// move is asserted against the committed type in every lane.
#[cfg(feature = "cli-helpers")]
fn schema_with_substitutions(dir: &Path, subs: &[(&str, &str)]) -> PathBuf {
    let committed = schema_path();
    let mut cql = std::fs::read_to_string(&committed)
        .unwrap_or_else(|e| panic!("committed schema {committed:?} unreadable: {e}"));
    for (from, to) in subs {
        assert_eq!(
            cql.matches(from).count(),
            1,
            "expected EXACTLY ONE {from:?} in {committed:?} — if the committed schema was \
             reflowed or the column renamed, this substitution must be re-derived rather \
             than silently skipped"
        );
        cql = cql.replace(from, to);
        assert!(
            !cql.contains(from),
            "the substitution {from:?} -> {to:?} did not take"
        );
    }
    assert!(
        cql.contains("m_dec    map<decimal, int>"),
        "m_dec must keep its REAL decimal declaration: the admission-gate move is \
         asserted against the committed type, never a substituted one"
    );
    let path = dir.join(SCHEMA);
    std::fs::write(&path, cql).expect("writing the throwaway schema");
    path
}

/// `(id, column) -> Value` for every fixture row via the PUBLIC
/// `Database::execute` SELECT surface, or the decode error verbatim.
///
/// The schema is a PARAMETER because `IngestionConfig::schema_paths` is a
/// per-read input: the subject lane below hands it the COMMITTED schema while
/// the `varint` lane hands it a throwaway copy, rather than either setting the
/// process-wide `CQLITE_SCHEMAS_ROOT`, so two tests in this binary can read the
/// same fixture under different declarations without racing.
#[cfg(feature = "cli-helpers")]
async fn select_star(schema: PathBuf) -> Result<Vec<(i32, HashMap<String, Value>)>, String> {
    let config = IngestionConfig {
        schema_paths: vec![schema],
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

// ════════════════════════════════════════════════════════════════════════════
// THE SUBJECT — the empty fixed-width key through the public SELECT surface
// ════════════════════════════════════════════════════════════════════════════

/// THE ACTIVE REGRESSION LANE (roborev job 448 finding B). Every fixed-width
/// family's empty key must reach `SELECT` as the TYPED sentinel with its OWN
/// tag, `m_text`'s must be the NATIVE empty text, and the column set must be
/// complete in both directions.
///
/// It reads the committed Cassandra-written fixture through the public
/// `Database::execute` surface under the COMMITTED schema, with NO substitution
/// of any kind: every column — `m_dec` and `m_frozen` alike — is read at the
/// type the oracle declares, so the admission-gate move is asserted against the
/// committed declaration and not a convenient one.
///
/// That was impossible until #3847 (PR #4017) widened `require_fixed_width`:
/// before it, `m_frozen`'s inline empty key was a HARD `ColumnDecode` that
/// failed the whole `SELECT`, and this lane stepped past it with a throwaway
/// re-declaration of that ONE key type. Earlier still, this file's only fixture
/// lane RETURNED `Ok` on that blocker, so not one per-column assertion executed
/// and reverting the decoder change left it green: no active coverage at all.
/// What this lane claims about `m_frozen` is exactly what it asserts — that the
/// column is PRESENT in the decoded row (`assert_column_set`, so the committed
/// read is whole); the inline frozen path's own values are #3847's subject and
/// have their own oracle there.
///
/// MEASURED post-fix through this same surface:
/// `m_int = Map([(Empty(Int), 7), (Integer(42), 1)])`,
/// `m_bigint = Map([(Empty(BigInt), 7), (BigInt(99), 1)])`,
/// `m_uuid = Map([(Empty(Uuid), 7), (Uuid(123e4567-…), 1)])`,
/// `m_bool = Map([(Empty(Boolean), 7), (Boolean(true), 1)])`,
/// `m_inet = Map([(Empty(Inet), 7), (Inet(10.0.0.1), 1)])`,
/// `m_text = Map([(Text(b""), 7), (Text("k"), 1)])`, and
/// `m_dec = Map([(Empty(Decimal), 7), (Decimal{scale:1,unscaled:[0x0f]}, 1)])`.
/// Pre-fix the four fixed-width sentinels were each `Blob(b"")` — one opaque
/// spelling for four distinct declared types — and m_dec was an outright refusal
/// that failed the whole SELECT.
#[cfg(feature = "cli-helpers")]
#[tokio::test]
async fn an_empty_fixed_width_map_key_reads_as_the_typed_sentinel() {
    // FAIL-CLOSED: this lane has no excusing arm at all, and now no
    // substitution either. Any error is a failure, because an arm that returns
    // `Ok` on a blocker is exactly the defect this lane replaces.
    let rows = select_star(schema_path())
        .await
        .unwrap_or_else(|err| panic!("the committed fixture failed to read: {err}"));

    assert_column_set(&rows);

    // The SUBJECT row. Each empty key is the sentinel of its OWN declared family:
    // four distinct values where the pre-fix decoder produced one opaque `Blob`.
    let expected: &[(&str, EmptyValueType, Value)] = &[
        ("m_int", EmptyValueType::Int, Value::Integer(42)),
        ("m_bigint", EmptyValueType::BigInt, Value::BigInt(99)),
        ("m_bool", EmptyValueType::Boolean, Value::Boolean(true)),
        // m_dec is the family the admission-gate move exists for: `decimal` is
        // VARIABLE-width, so no width table can admit its empty buffer, and only
        // the tag table (`validate()`-derived) can. Its golden sibling is the
        // decimal `1.5`, whose serialized form is `[i32 scale][unscaled]`.
        (
            "m_dec",
            EmptyValueType::Decimal,
            Value::Decimal {
                scale: 1,
                unscaled: vec![0x0f],
            },
        ),
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
    // `inet` IS a sentinel family (roborev job 449 finding C, #4079) — its
    // DECODE always succeeded, which is why the gate had to move above the
    // decode to reach it. Both halves are asserted: the empty key is
    // `Empty(Inet)`, and the NON-EMPTY sibling keeps its native `Inet` spelling,
    // so the normalization is a property of the EMPTY cell path and not of the
    // family.
    let m_inet = map_entries(&rows, 1, "m_inet");
    assert_eq!(
        m_inet.get(&format!("{:?}", Value::Empty(EmptyValueType::Inet))),
        Some(&Value::Integer(7)),
        "row id=1 m_inet: the empty key is the TYPED sentinel — a zero-length Inet is \
         REJECTED by both bindings, so the native spelling was a user-visible defect \
         (#4079): {m_inet:?}"
    );
    assert!(
        !m_inet.contains_key(&format!("{:?}", Value::inet(Vec::<u8>::new()))),
        "row id=1 m_inet: the zero-length Inet spelling must be GONE, not merely \
         joined by the sentinel: {m_inet:?}"
    );
    assert_eq!(
        m_inet.get(&format!("{:?}", Value::inet(vec![10u8, 0, 0, 1]))),
        Some(&Value::Integer(1)),
        "row id=1 m_inet: the golden's 10.0.0.1 sibling keeps its NATIVE Inet \
         spelling: {m_inet:?}"
    );

    // THE CONTRAST ROW. No empty key anywhere in the golden, so no sentinel may
    // appear: the fix is a property of the empty-key path, not of the data.
    for column in [
        "m_int", "m_bigint", "m_uuid", "m_bool", "m_inet", "m_dec", "m_text",
    ] {
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

/// The `varint` half of finding C, through the same public `Database::execute`
/// surface (#4079).
///
/// `varint` and `inet` are the two families whose empty buffer DECODES
/// successfully — `IntegerSerializer.java:31-34` returns null on empty and its
/// `validate` body is the comment `// no invalid integers.`, so nothing about a
/// zero-length varint is refusable — which is exactly why the pre-fix gate, sited
/// in the decoder's `Err` arm, could never reach either of them. `inet` is
/// asserted on its own committed column above; `varint` has no committed column,
/// so this lane re-declares `m_inet`'s KEY TYPE (see [`INET_DECL_AS_VARINT`] for
/// what that does and does NOT reinterpret — the empty cell path is zero bytes
/// under every declared key type, so the SUBJECT is untouched Cassandra output).
///
/// # RED-verified
/// With the admission gate returned to the decoder's `Err` arm this lane fails
/// with `Varint(b"")` in place of `Empty(Varint)`.
#[cfg(feature = "cli-helpers")]
#[tokio::test]
async fn an_empty_varint_map_key_reads_as_the_typed_sentinel() {
    let dir = tempfile::tempdir().expect("a temp dir for the substituted schema");
    let schema =
        schema_with_substitutions(dir.path(), &[(INET_DECL_COMMITTED, INET_DECL_AS_VARINT)]);
    // FAIL-CLOSED: no excusing arm. Any error is a failure.
    let rows = select_star(schema)
        .await
        .unwrap_or_else(|err| panic!("the fixture failed to read under map<varint,int>: {err}"));

    let entries = map_entries(&rows, 1, "m_inet");
    assert_eq!(
        entries.get(&format!("{:?}", Value::Empty(EmptyValueType::Varint))),
        Some(&Value::Integer(7)),
        "row id=1 m_inet-as-varint: the golden's path:[\"\"] entry must be Empty(Varint), \
         never the native Varint(b\"\") second spelling (#4079): {entries:?}"
    );
    assert!(
        !entries.contains_key(&format!("{:?}", Value::varint(Vec::<u8>::new()))),
        "row id=1 m_inet-as-varint: the zero-length Varint spelling must be GONE: \
         {entries:?}"
    );
    // The SIBLING is asserted only as the RAW BYTES it is — `0a 00 00 01`, which
    // this declaration reads as a varint. Its VALUE means nothing here; its
    // presence is what makes a failure legible (a map short one entry is
    // distinguishable from a missing column), and its NATIVE spelling is what
    // shows the normalization is a property of the EMPTY cell path.
    assert_eq!(
        entries.get(&format!(
            "{:?}",
            Value::varint(vec![0x0a, 0x00, 0x00, 0x01])
        )),
        Some(&Value::Integer(1)),
        "row id=1 m_inet-as-varint: the non-empty sibling keeps its NATIVE Varint \
         spelling: {entries:?}"
    );
    // THE CONTRAST ROW carries no empty key, so no sentinel may appear.
    let contrast = map_entries(&rows, 2, "m_inet");
    assert!(
        !contrast.keys().any(|k| k.starts_with("Empty(")),
        "row id=2 m_inet-as-varint: the CONTRAST row has no empty key: {contrast:?}"
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
