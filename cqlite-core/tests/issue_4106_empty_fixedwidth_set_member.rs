//! Issue #4106 — a Cassandra-legal EMPTY FIXED-WIDTH MULTICELL SET MEMBER must
//! reach a user-facing `SELECT` with the set's FULL ARITY, and must be
//! WRITEABLE BACK.
//!
//! # The defect
//! A non-frozen `set<T>` is MULTICELL and its member travels IN the cell path:
//! at `cassandra-5.0.8`, `cql3/Sets.java:407` is
//! `params.addCell(column, CellPath.create(bb), ByteBufferUtil.EMPTY_BYTE_BUFFER)`
//! — the element IS the path and the cell VALUE is empty (`HAS_EMPTY_VALUE`).
//! Cassandra validates a cell path with `schema/ColumnMetadata.java:457-467`
//! (`((CollectionType)type).nameComparator().validate(path.get(0))`), and
//! `nameComparator()` is the ELEMENTS type of a `SetType`
//! (`db/marshal/SetType.java:101-104`); the fixed-width serializers spell their
//! width check `size != N && !isEmpty` (e.g. `serializers/Int32Serializer.java`),
//! so the EMPTY buffer is LEGAL data, not corruption.
//!
//! CQLite's multicell-set branch carried its own `!path_bytes.is_empty()` guard,
//! which yielded "no member" for a zero-length path and DROPPED it: a `SELECT`
//! returned the set **one member SHORT**, with no error and no log line. That is
//! why both tests below assert the ARITY and not merely the presence of a
//! sentinel — a set short exactly one member is the defect's signature, and it is
//! distinguishable from a missing column only if the count is checked.
//!
//! Symmetrically, `write_set_complex_cells` routed every element through the
//! type-blind `serialize_value_into`, whose sentinel refusal is correct there
//! (no declared type ⇒ it cannot tell a legal empty `text` from the corruption
//! Cassandra's `validate` throws on for `tinyint`), so a set CQLite had
//! legitimately decoded **could not be written back at all** — a compaction of
//! that SSTable failed outright. That claim is
//! `writer/data_writer/cell_path.rs`'s and it is what the second test closes.
//!
//! # Oracle — Cassandra-WRITTEN bytes and `cassandra-5.0.8` source, never CQLite
//! Fixture:
//! `test-data/fixtures/issue_4106/test_empty_fixedwidth_set/empty_fixedwidth_set_member-*/`,
//! Cassandra-written, with its `sstabledump` golden `nb-1-big-Data.db.jsonl`;
//! schema `test-data/schemas/issue-4106-empty-fixedwidth-set-member.cql` (which
//! records what each column is for and what the fixture CANNOT carry). Every
//! expectation below is quoted from that golden or derived from the pinned
//! Cassandra source; none is read off CQLite's own prior output.
//!
//! A CQLite-written + CQLite-read round trip could NOT settle either half
//! (CLAUDE.md, #3042): both sides would make the identical framing mistake, the
//! round trip would close, and real Cassandra data would still read wrong while
//! CQLite's output stayed unreadable by Cassandra. So the SUBJECT of the write
//! test is a value DECODED FROM CASSANDRA BYTES, and its byte expectation comes
//! from `CollectionType.cellPathSerializer`
//! (`db/marshal/CollectionType.java:361-382`), whose whole body is
//! `ByteBufferUtil.writeWithVIntLength(path.get(0))` — so an EMPTY component is
//! the single byte `0x00` and NOTHING after it.
//!
//! Golden, id 1 (THE SUBJECT ROW) — every set column carries an EMPTY member
//! ALONGSIDE a non-empty sibling:
//! ```text
//! s_int    path:[""]  path:["42"]
//! s_bigint path:[""]  path:["99"]
//! s_uuid   path:[""]  path:["123e4567-e89b-12d3-a456-426614174000"]
//! s_bool   path:[""]  path:["true"]
//! s_inet   path:[""]  path:["10.0.0.1"]
//! s_varint path:[""]  path:["7"]
//! s_dec    path:[""]  path:["1.5"]
//! s_text   path:[""]  path:["k"]
//! s_frozen value:["",7]          (ONE inline cell, no CellPath at all)
//! ```
//! Golden, id 2 (THE CONTRAST ROW) — one member per column, no empty anywhere.
//!
//! # WHAT MUST NOT BE ASSERTED (recorded in the schema, honoured here)
//! Each multicell column also carries a collection tombstone (an INSERT of a
//! whole non-frozen collection REPLACES it) whose `local_delete_time` is a WALL
//! CLOCK no CQL clause can pin. Nothing below asserts on it, and this golden is
//! never byte-compared across regenerations.
//!
//! `tinyint`/`smallint`/`date`/`time` are ABSENT from the fixture BY
//! CONSTRUCTION (cqlsh refuses `blobAsTinyint(0x)` &c., so Cassandra cannot write
//! such a member) — this file says nothing about them; CQLite's refusal for those
//! four is unit-covered in `regression_4106_empty_set_member_tests`.

#![cfg(all(feature = "lz4", feature = "cli-helpers"))]
//  ^ SOURCE-level gate, deliberately NOT a `[[test]] required-features` entry —
//  the same reasoning as `issue_3805_empty_fixedwidth_map_key.rs`.
//
//  `lz4`: the fixture is real Cassandra output and LZ4-compressed
//  (`CompressionInfo.db` present), so reading it needs `lz4`, and without the
//  feature the target would compile and then fail at runtime on a blob it cannot
//  decompress — a false red, not a signal. `lz4` arrives via the DEFAULT
//  `all-compression` set.
//
//  `cli-helpers`: BOTH tests read the fixture through `cqlite_core::ingestion`,
//  which that feature gates, so EVERY item in this file is unreachable without
//  it. The gate is at FILE level rather than per-test precisely because of that:
//  with the feature off there is nothing left to keep, and per-test gating would
//  leave every helper below dead — `cargo clippy -p cqlite-core --all-targets`
//  at default features reports 13 `never used` errors under `-D warnings` for
//  exactly that shape (a sibling, `issue_3809_tombstone_clustering_identity`,
//  already does). `cli-helpers` is NON-default, so a lane without it executes
//  ZERO tests here (the #3375 hazard); the full gate's `core-tests` component
//  runs `cargo test -p cqlite-core --features cli-helpers`, which is the lane
//  that executes them.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::types::{EmptyValueType, Value};
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

const KEYSPACE: &str = "test_empty_fixedwidth_set";
const TABLE: &str = "empty_fixedwidth_set_member";
const SCHEMA: &str = "issue-4106-empty-fixedwidth-set-member.cql";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core always has a workspace parent directory")
        .to_path_buf()
}

/// Candidate "sstables roots" for this table, in order: this issue's COMMITTED
/// fixture tree first, then whatever the machine's dataset roots are. The order
/// only breaks ties — [`datasets_root::first_root_with_table`] picks by EVIDENCE,
/// because neither root is a superset of the other (#3220).
fn candidate_roots() -> Vec<PathBuf> {
    let mut roots = vec![repo_root().join("test-data/fixtures/issue_4106")];
    roots.extend(datasets_root::sstables_root_candidates());
    roots
}

/// The root that really carries this table's bytes. FAIL-CLOSED, never a SKIP:
/// the fixture is git-tracked source (its binaries are force-added), so an
/// absence is a broken checkout, not a missing optional corpus. Both tests below
/// route through this, so neither can pass on an absent fixture — which is why
/// this file needs no separate "is the fixture there" test and no suite-wide
/// `assert!(ran > 0)`, the construct that cannot see one case skipping behind its
/// siblings (#3220).
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

/// Every column the golden carries, so BOTH DIRECTIONS of the column set can be
/// asserted (#3890): no golden column absent from the decoded row, and no
/// decoded column the golden does not have.
const GOLDEN_COLUMNS: &[&str] = &[
    "id", "s_int", "s_bigint", "s_uuid", "s_bool", "s_inet", "s_varint", "s_dec", "s_text",
    "s_frozen",
];

/// The eight MULTICELL set columns — the ones whose member travels in a CellPath.
/// `s_frozen` is deliberately not here: a frozen set is ONE inline
/// length-prefixed cell with no CellPath at all, decoded by a different path
/// entirely, so it is the ENCODING CONTROL and never a subject.
const MULTICELL_COLUMNS: &[&str] = &[
    "s_int", "s_bigint", "s_uuid", "s_bool", "s_inet", "s_varint", "s_dec", "s_text",
];

/// `(id, column) -> Value` for every fixture row via the PUBLIC
/// `Database::execute` SELECT surface, or the failure verbatim.
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

fn peel_frozen(v: &Value) -> Value {
    match v {
        Value::Frozen(inner) => peel_frozen(inner),
        other => other.clone(),
    }
}

/// The members of set `column` in row `id`, as the decoder produced them.
/// Fail-closed: a missing row, a missing column, a NULL cell or a non-set value
/// is a decode regression, never a skip.
fn set_members(rows: &[(i32, HashMap<String, Value>)], id: i32, column: &str) -> Vec<Value> {
    let row = rows
        .iter()
        .find(|(k, _)| *k == id)
        .map(|(_, r)| r)
        .unwrap_or_else(|| panic!("fixture row id={id} missing"));
    let v = row
        .get(column)
        .unwrap_or_else(|| panic!("row id={id} has no column {column}"));
    match peel_frozen(v) {
        Value::Set(members) => members,
        other => panic!("row id={id} column {column} is not a Set: {other:?}"),
    }
}

/// Membership as a set of renderings, so an assertion never depends on cell
/// order. ARITY is asserted from the `Vec` itself, never from this — collapsing
/// a duplicate here must not be able to hide a dropped member.
fn rendered(members: &[Value]) -> BTreeSet<String> {
    members.iter().map(|m| format!("{m:?}")).collect()
}

// ════════════════════════════════════════════════════════════════════════════
// TEST 1 — the empty member through the public `SELECT` surface
// ════════════════════════════════════════════════════════════════════════════

/// THE READ SUBJECT. For every MULTICELL family the fixture carries, the golden's
/// `path:[""]` member must reach a user-facing `SELECT` — at the FULL ARITY the
/// golden records, with its own declared family's spelling, and alongside its
/// non-empty sibling.
///
/// Expectation provenance, per assertion:
///  * the ARITY (2 in row id 1, 1 in row id 2) and every SIBLING value are read
///    off the `sstabledump` golden quoted in this file's header;
///  * which families come back as `Value::Empty(tag)` rather than a native empty
///    is `EmptyValueType::for_cql_type`'s table, derived arm-by-arm from
///    Cassandra's `validate()` at `cassandra-5.0.8` — `int`/`bigint`/`uuid`/
///    `boolean` from the `size != N && !isEmpty` spelling, `inet` from
///    `InetAddressSerializer.java:52-55`'s early return, `varint` from
///    `IntegerSerializer.java:31-34` (whose `validate` body is the comment
///    `// no invalid integers.`), and `decimal` from
///    `DecimalSerializer.java:58-63`, which throws only
///    `if (!isEmpty && size < 4)` and whose message reads "Expected 0 or at
///    least 4 bytes";
///  * `s_text` is the NATIVE contrast: an empty buffer is a legal MEANINGFUL
///    value for a text type — `AbstractTextSerializer.java:72-77` overrides
///    `isNull` precisely to say so — so it must come back as the empty text and
///    NEVER as a sentinel. This is what proves the change did not go too wide.
///
/// # RED-verified
/// With the pre-fix guard restored in `complex_column.rs` (skip the cell when
/// `path_bytes.is_empty()`), this fails on the FIRST arity assertion:
/// `row id=1 s_int: the golden carries TWO members ... got 1 ([Integer(42)])`.
#[tokio::test]
async fn an_empty_fixed_width_set_member_reaches_select_at_the_full_arity() {
    // FAIL-CLOSED: no excusing arm anywhere. A committed fixture is `must_run`,
    // and an arm that returns early on a decode failure is exactly the defect
    // this lane exists to catch.
    let rows = select_star()
        .await
        .unwrap_or_else(|err| panic!("the committed fixture failed to read: {err}"));

    // Both directions of the column set, naming any missing column (#3890).
    for (id, cols) in &rows {
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

    // ── THE SUBJECT ROW (golden id 1) ────────────────────────────────────────
    // ARITY FIRST, for every multicell column: the pre-fix decoder returned each
    // of these one member SHORT, so the count is the defect's signature.
    for column in MULTICELL_COLUMNS {
        let members = set_members(&rows, 1, column);
        assert_eq!(
            members.len(),
            2,
            "row id=1 {column}: the golden carries TWO members — the empty one \
             (path:[\"\"]) and its non-empty sibling — and the #4106 defect returned the \
             set ONE MEMBER SHORT with no error and no log line; got {} ({members:?})",
            members.len()
        );
    }

    // The TYPED sentinel families, each with the sibling the golden records.
    let typed: &[(&str, EmptyValueType, Value)] = &[
        ("s_int", EmptyValueType::Int, Value::Integer(42)),
        ("s_bigint", EmptyValueType::BigInt, Value::BigInt(99)),
        ("s_bool", EmptyValueType::Boolean, Value::Boolean(true)),
        // golden "123e4567-e89b-12d3-a456-426614174000"
        (
            "s_uuid",
            EmptyValueType::Uuid,
            Value::Uuid([
                0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17,
                0x40, 0x00,
            ]),
        ),
        // golden "10.0.0.1" — `InetAddressSerializer` serializes the raw address
        // bytes, so a v4 address is four bytes.
        (
            "s_inet",
            EmptyValueType::Inet,
            Value::inet(vec![0x0a, 0x00, 0x00, 0x01]),
        ),
        // golden "7" — `IntegerSerializer` is `BigInteger.toByteArray()`.
        (
            "s_varint",
            EmptyValueType::Varint,
            Value::varint(vec![0x07]),
        ),
        // golden "1.5" — `DecimalSerializer` is `[i32 scale][unscaled
        // BigInteger bytes]`, so 15 x 10^-1 is scale 1, unscaled 0x0f.
        (
            "s_dec",
            EmptyValueType::Decimal,
            Value::Decimal {
                scale: 1,
                unscaled: vec![0x0f],
            },
        ),
    ];
    for (column, tag, sibling) in typed {
        let members = set_members(&rows, 1, column);
        let seen = rendered(&members);
        assert!(
            seen.contains(&format!("{:?}", Value::Empty(*tag))),
            "row id=1 {column}: the golden's path:[\"\"] member must decode to \
             Empty({tag:?}) — the tag table is derived from Cassandra's own validate(); \
             got {members:?}"
        );
        // The non-empty sibling is what makes a failure legible: a set short one
        // member is distinguishable from a missing column.
        assert!(
            seen.contains(&format!("{sibling:?}")),
            "row id=1 {column}: the non-empty sibling {sibling:?} must still decode: \
             {members:?}"
        );
        // DISTINCT FROM NULL — Cassandra has no null set member at all
        // (`Sets.java:407` writes the element INTO the path and cannot express
        // one), so a `Null` here would be a second, wrong spelling.
        assert!(
            !seen.contains(&format!("{:?}", Value::Null)),
            "row id=1 {column}: an empty member is NOT null: {members:?}"
        );
    }

    // THE NATIVE CONTRAST. `s_text`'s empty member is the empty TEXT and must
    // carry no sentinel of any family — the assertion that proves the admission
    // did not widen past the families Cassandra's `validate()` admits.
    let s_text = set_members(&rows, 1, "s_text");
    let text_seen = rendered(&s_text);
    assert!(
        text_seen.contains(&format!("{:?}", Value::text(""))),
        "row id=1 s_text: the empty member is the NATIVE empty text \
         (AbstractTextSerializer.java:72-77 overrides isNull to make it meaningful), \
         never the sentinel: {s_text:?}"
    );
    assert!(
        text_seen.contains(&format!("{:?}", Value::text("k"))),
        "row id=1 s_text: the golden's \"k\" sibling must still decode: {s_text:?}"
    );
    assert!(
        !text_seen.iter().any(|m| m.starts_with("Empty(")),
        "row id=1 s_text must carry NO sentinel of any family: {s_text:?}"
    );

    // THE ENCODING CONTROL. `s_frozen` is ONE inline length-prefixed cell with no
    // CellPath at all, so it discriminates the MULTICELL path (this issue) from
    // the FROZEN one. Asserted here: the ARITY the golden records and the
    // non-empty sibling, i.e. that the inline empty element is not DROPPED
    // either. The inline empty element's REPRESENTATION is #3847's subject and
    // has its own oracle there, so it is deliberately not pinned by this lane.
    let s_frozen = set_members(&rows, 1, "s_frozen");
    assert_eq!(
        s_frozen.len(),
        2,
        "row id=1 s_frozen: the golden's inline value is [\"\",7] — two elements: \
         {s_frozen:?}"
    );
    assert!(
        rendered(&s_frozen).contains(&format!("{:?}", Value::Integer(7))),
        "row id=1 s_frozen: the golden's 7 element must decode: {s_frozen:?}"
    );

    // ── THE CONTRAST ROW (golden id 2) ───────────────────────────────────────
    // No empty member anywhere, so no sentinel may appear: the fix is a property
    // of the empty-member path and not of the data. Siblings from the golden.
    let contrast: &[(&str, Value)] = &[
        ("s_int", Value::Integer(5)),
        ("s_bigint", Value::BigInt(6)),
        ("s_bool", Value::Boolean(false)),
        (
            "s_uuid",
            Value::Uuid([
                0x22, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17,
                0x41, 0x11,
            ]),
        ),
        ("s_inet", Value::inet(vec![0x0a, 0x00, 0x00, 0x02])),
        ("s_varint", Value::varint(vec![0x08])),
        (
            "s_dec",
            Value::Decimal {
                scale: 1,
                unscaled: vec![0x19],
            },
        ),
        ("s_text", Value::text("w")),
        ("s_frozen", Value::Integer(9)),
    ];
    for (column, only) in contrast {
        let members = set_members(&rows, 2, column);
        assert_eq!(
            members.len(),
            1,
            "row id=2 {column}: the golden carries exactly one member: {members:?}"
        );
        assert_eq!(
            rendered(&members),
            BTreeSet::from([format!("{only:?}")]),
            "row id=2 {column}: the golden's only member is {only:?}: {members:?}"
        );
        assert!(
            !rendered(&members).iter().any(|m| m.starts_with("Empty(")),
            "row id=2 {column}: the CONTRAST row has no empty member, so no sentinel may \
             appear: {members:?}"
        );
    }
}

// ════════════════════════════════════════════════════════════════════════════
// TEST 2 — decode → encode: the same member WRITES BACK, as `0x00` and nothing
// ════════════════════════════════════════════════════════════════════════════

// Cassandra cell flags (`db/rows/Cell.Serializer` at `cassandra-5.0.8`).
#[cfg(feature = "write-support")]
const CELL_HAS_EMPTY_VALUE: u8 = 0x04;
#[cfg(feature = "write-support")]
const CELL_USE_ROW_TIMESTAMP: u8 = 0x08;
#[cfg(feature = "write-support")]
const END_OF_PARTITION: u8 = 0x01;

/// Read a Cassandra unsigned VInt; returns `(value, new_pos)`.
///
/// `utils/vint/VIntCoding.java` at `cassandra-5.0.8`: the count of extra bytes is
/// the number of leading ONE bits of the first byte, and the first byte
/// contributes its remaining low bits — none at all in the 9-byte form
/// (`0xff`), which the LIVE complex deletion below really does take.
#[cfg(feature = "write-support")]
fn read_vuint(data: &[u8], pos: usize) -> (u64, usize) {
    let first = data[pos];
    let extra = first.leading_ones() as usize;
    let mut value = if extra >= 8 {
        0
    } else {
        (first as u64) & (0xFFu64 >> (extra + 1))
    };
    for i in 0..extra {
        value = (value << 8) | data[pos + 1 + i] as u64;
    }
    (value, pos + 1 + extra)
}

/// THE WRITE SUBJECT, closing the claim recorded in
/// `writer/data_writer/cell_path.rs`: before #4106 a set CQLite had legitimately
/// DECODED from Cassandra-written bytes "could not be written back — a compaction
/// of that SSTable failed outright".
///
/// So this test takes the value the READER produced from the CASSANDRA fixture —
/// `Set([Empty(Int), Integer(42)])` for `s_int`, row id 1 — and pushes it through
/// the public write surface (`WriteEngine::write_async` + `flush`, which reaches
/// `write_complex_column` → `write_set_complex_cells` →
/// `serialize_set_cell_path_element_into`), then asserts the EMITTED BYTES.
///
/// # The byte expectation is CASSANDRA-DERIVED, never CQLite's earlier output
/// A #3042 round-trip (write with CQLite, read with CQLite, compare values) would
/// prove nothing: both sides would make the identical framing mistake and the
/// loop would still close. So the expected bytes come from source:
///  * ONE `CollectionType.cellPathSerializer` serializes every collection's cell
///    path (`db/marshal/CollectionType.java:55`, `:361-382`) and its whole body
///    is `ByteBufferUtil.writeWithVIntLength(path.get(0))` — which writes the
///    length as an unsigned VInt and then the bytes, so an EMPTY component is the
///    single byte `0x00` **and nothing after it**;
///  * a live set cell carries NO value: `cql3/Sets.java:407` passes
///    `ByteBufferUtil.EMPTY_BYTE_BUFFER` as the value, and `Cell.Serializer`
///    derives `hasValue = !flag(HAS_EMPTY_VALUE_MASK)`, so the flag byte must set
///    `HAS_EMPTY_VALUE` (0x04) and no value-length VInt may follow;
///  * the element's timestamp equals the row's, and `Cell.Serializer.serialize`
///    sets `USE_ROW_TIMESTAMP_MASK` (0x08) in exactly that case, so the flag byte
///    is `0x0c` for both cells and neither carries a timestamp delta;
///  * the ORDER — empty member first — is Cassandra-written evidence about this
///    exact case: the golden emits `s_int path:[""]` before `path:["42"]`.
///  * `00 00 00 2a` is `Int32Serializer`'s big-endian 42, the golden's sibling.
///
/// # RED-verified
/// Reverting `serialize_set_cell_path_element_into` to the pre-#4106 type-blind
/// route (`serialize_value_into(element, out)`) makes the write REFUSE the
/// sentinel and this test fails at `flush`, which is precisely the "compaction
/// failed outright" symptom.
#[cfg(feature = "write-support")]
#[tokio::test]
async fn the_decoded_empty_member_writes_back_as_a_zero_length_cell_path() {
    use cqlite_core::schema::{Column, KeyColumn, TableSchema};
    use cqlite_core::storage::write_engine::{
        CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
    };

    // ── decode: the value comes from CASSANDRA-WRITTEN bytes ─────────────────
    let rows = select_star()
        .await
        .unwrap_or_else(|err| panic!("the committed fixture failed to read: {err}"));
    let members = set_members(&rows, 1, "s_int");
    assert_eq!(
        members.len(),
        2,
        "the write subject must be the WHOLE decoded set — if the reader already \
         dropped the empty member there is nothing here to write back: {members:?}"
    );
    assert!(
        rendered(&members).contains(&format!("{:?}", Value::Empty(EmptyValueType::Int))),
        "the write subject must carry the empty member decoded from Cassandra bytes: \
         {members:?}"
    );
    let subject = Value::Set(members);

    // ── encode: through the PUBLIC write surface, one partition, one row, one
    // column, so the emitted Data.db layout is fully determined ──────────────
    let schema = TableSchema {
        keyspace: "issue4106".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![Column {
            name: "s_int".to_string(),
            data_type: "set<int>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };
    let temp = tempfile::TempDir::new().expect("temp dir");
    let engine_config = WriteEngineConfig::new(
        temp.path().join("data"),
        temp.path().join("wal"),
        schema.clone(),
    );
    let mut engine = WriteEngine::new(engine_config).expect("write engine");
    engine
        .write_async(Mutation::new(
            TableId::new("issue4106", "t"),
            PartitionKey::single("id", Value::Integer(1)),
            None,
            vec![CellOperation::Write {
                column: "s_int".to_string(),
                value: subject,
            }],
            1_000,
            None,
        ))
        .await
        .expect(
            "writing the decoded set must be ACCEPTED — the pre-#4106 writer refused \
                 the sentinel outright, which is what failed a compaction of the very \
                 SSTable it came from",
        );
    let info = engine
        .flush()
        .await
        .expect("flush must succeed: a set CQLite decoded must be writeable back")
        .expect("flush produced an sstable");
    let data = std::fs::read(&info.data_path).expect("read the emitted Data.db");

    // ── walk to the complex column, then assert its bytes EXACTLY ────────────
    // Partition header: key length (u16) + key + partition deletion
    // (localDeletionTime i32 + markedForDeleteAt i64).
    assert_eq!(
        u16::from_be_bytes([data[0], data[1]]),
        4,
        "an int partition key is four bytes"
    );
    let mut pos = 2 + 4 + 4 + 8;
    // Row flags: HAS_TIMESTAMP (0x04) | HAS_ALL_COLUMNS (0x20) |
    // HAS_COMPLEX_DELETION (0x40) — the one column is complex and present.
    assert_eq!(
        data[pos], 0x64,
        "row flags must be HAS_TIMESTAMP | HAS_ALL_COLUMNS | HAS_COMPLEX_DELETION"
    );
    pos += 1;
    let (row_size, p) = read_vuint(&data, pos);
    pos = p;
    let row_body_start = pos;
    let (_prev_unfiltered_size, p) = read_vuint(&data, pos);
    pos = p;
    let (_liveness_ts_delta, p) = read_vuint(&data, pos);
    pos = p;
    // HAS_ALL_COLUMNS ⇒ no columns-subset VInt. The complex column then opens
    // with its complex deletion (markedForDeleteAt delta, then
    // localDeletionTime delta), which for a LIVE collection is
    // `DeletionTime.LIVE` — asserted nowhere, per this file's header: it is
    // derived from the writer's own encoding baselines, not from the golden.
    let (_complex_mfda_delta, p) = read_vuint(&data, pos);
    pos = p;
    let (_complex_ldt_delta, p) = read_vuint(&data, pos);
    pos = p;

    // THE ASSERTION. From the cell count to the last cell byte, EXACTLY:
    //   02          cell count: two cells, the empty member and its sibling
    //   0c 00       HAS_EMPTY_VALUE|USE_ROW_TIMESTAMP, then writeWithVIntLength
    //               of an EMPTY buffer — the single byte 0x00 and NOTHING after
    //   0c 04 ..    the sibling: same flags, length 4, big-endian 42
    let expected: &[u8] = &[
        0x02,
        CELL_HAS_EMPTY_VALUE | CELL_USE_ROW_TIMESTAMP,
        0x00,
        CELL_HAS_EMPTY_VALUE | CELL_USE_ROW_TIMESTAMP,
        0x04,
        0x00,
        0x00,
        0x00,
        0x2a,
    ];
    let end = pos + expected.len();
    assert!(
        end <= data.len(),
        "the emitted Data.db is too short to hold the cell section: {} bytes from {pos}",
        data.len()
    );
    assert_eq!(
        &data[pos..end],
        expected,
        "the emitted cell section must be `writeWithVIntLength` of the EMPTY buffer \
         (a single 0x00, with NO value bytes after it — HAS_EMPTY_VALUE is set) \
         followed by the four-byte sibling; got {:02x?}",
        &data[pos..data.len().min(end + 8)]
    );
    pos = end;

    // Nothing else may follow: the row body must close exactly here, and the
    // partition must end. A stray value-length VInt after the empty cell path is
    // exactly the desync `Cell.Serializer` would suffer, so this bound is part of
    // the assertion and not decoration.
    assert_eq!(
        pos - row_body_start,
        row_size as usize,
        "row_size must cover the row body exactly — a trailing byte after the empty \
         cell path would desync a strict reader"
    );
    assert_eq!(
        data[pos], END_OF_PARTITION,
        "the partition must end immediately after the two cells"
    );
    assert_eq!(pos + 1, data.len(), "no trailing bytes after the partition");
}
