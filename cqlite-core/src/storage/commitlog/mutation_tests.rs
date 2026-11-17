//! Unit tests for `mutation.rs` (moved out for the #1116 file-size ratchet; module path unchanged).

use super::*;
use crate::storage::commitlog::schema::{parse_table_id, ColumnSpec};
use std::collections::HashMap;

// The first user mutation body captured from the real Cassandra 5.0.2
// segment (commitlog_test.users, id=1 => alice/30). Verified end-to-end.
const ALICE_BODY: &str = "01d6de7150844811f1bf5abf4cbc47cb4a040000000110fd36c1327cb48c00000203616765046e616d65012400080000001e0805616c69636501";

fn users_schema() -> CommitLogSchema {
    CommitLogSchema {
        keyspace: "commitlog_test".into(),
        table: "users".into(),
        partition_key: vec![ColumnSpec::new("id", "int")],
        clustering: vec![],
        columns: vec![
            ColumnSpec::new("age", "int"),
            ColumnSpec::new("name", "text"),
        ],
    }
}

#[test]
fn decodes_structural_fields_without_schema() {
    let body = hex(ALICE_BODY);
    let m = decode_mutation(&body, &HashMap::new()).expect("decode");
    assert_eq!(m.updates.len(), 1);
    let u = &m.updates[0];
    assert_eq!(u.table_id_uuid(), "d6de7150-8448-11f1-bf5a-bf4cbc47cb4a");
    assert_eq!(u.partition_key, vec![0, 0, 0, 1]);
    assert_eq!(u.column_names, vec!["age", "name"]);
    assert!(!u.rows_decoded);
}

#[test]
fn decodes_cell_values_with_schema() {
    let body = hex(ALICE_BODY);
    let id = parse_table_id("d6de7150-8448-11f1-bf5a-bf4cbc47cb4a").unwrap();
    let mut schemas: SchemaSet = HashMap::new();
    schemas.insert(id, users_schema());
    let m = decode_mutation(&body, &schemas).expect("decode");
    // The unmodified ALICE_BODY declares num_updates=1 and the single
    // update decodes fully under schema, so both updates are present and
    // updates_complete is true (the positive branch of the field).
    assert!(m.updates_complete);
    let u = &m.updates[0];
    assert!(u.rows_decoded);
    assert_eq!(u.rows.len(), 1);
    let row = &u.rows[0];
    assert!(row.clustering.is_empty());
    // Cells in header order: age (int 30), name (text "alice").
    let age = row.cells.iter().find(|c| c.column == "age").unwrap();
    assert_eq!(age.value, Some(vec![0, 0, 0, 30]));
    let name = row.cells.iter().find(|c| c.column == "name").unwrap();
    assert_eq!(name.value.as_deref(), Some(b"alice".as_ref()));
}

#[test]
fn clustered_schema_bails_honestly_instead_of_misaligning() {
    // A schema.clustering entry is enough to trip the bail (decode_rows
    // checks the schema, not the wire bytes) — regression for the
    // clustering-prefix-header gap: decoding a clustered table's rows
    // without reading Cassandra's ClusteringPrefix presence-header first
    // silently misaligns every field after it. Rather than decode wrong
    // values, the schema-declares-clustering case must bail to the
    // honest structural-only path, exactly like partition deletions and
    // static rows already do.
    let body = hex(ALICE_BODY);
    let id = parse_table_id("d6de7150-8448-11f1-bf5a-bf4cbc47cb4a").unwrap();
    let mut schema = users_schema();
    schema.clustering = vec![ColumnSpec::new("ck", "int")];
    let mut schemas: SchemaSet = HashMap::new();
    schemas.insert(id, schema);
    let m = decode_mutation(&body, &schemas).expect("decode");
    let u = &m.updates[0];
    assert!(
        !u.rows_decoded,
        "clustered schema must bail to structural-only, never emit misaligned rows"
    );
    assert!(u.rows.is_empty());
    // Structural fields (table id, partition key) still surface correctly
    // — only the row body is honestly withheld.
    assert_eq!(u.partition_key, vec![0, 0, 0, 1]);
}

#[test]
fn collection_column_schema_bails_honestly_instead_of_misaligning() {
    // Regression: a schema with a collection/tuple/UDT column must bail
    // to structural-only, never attempt to decode it as a simple Cell —
    // Cassandra's complex-column wire shape is categorically different
    // (roborev finding, review-first pass; the module doc previously
    // claimed this was already handled and it was not).
    //
    // Calls decode_rows() directly rather than through decode_mutation():
    // the bail is purely schema-driven (checked before any cursor read),
    // but the only real fixture's wire bytes declare just age/name as
    // column_names — no fixture exists with an actual collection column
    // on the wire, and hand-crafting one would just be guessing bytes,
    // exactly what this module avoids. Testing decode_rows in isolation
    // proves the guard fires without needing to fabricate wire bytes it
    // never even reads.
    let mut schema = users_schema();
    schema.columns.push(ColumnSpec::new("tags", "list<text>"));
    let column_names = vec!["age".to_string(), "name".to_string(), "tags".to_string()];
    let mut c = Cursor::new(&[]);
    let result = decode_rows(&mut c, &schema, &column_names, 0);
    assert!(
        result.is_err(),
        "a collection-typed column in column_names must bail, never emit misaligned rows"
    );
}

#[test]
fn counter_column_no_longer_misreads_as_fixed_length() {
    // Regression: cql_fixed_len("counter") used to return Some(8),
    // silently consuming 8 raw bytes where a vint-length-prefixed
    // CounterContext blob actually sits. This doesn't assert a specific
    // decoded value (a CounterContext isn't a plain scalar this module
    // interprets further) — it asserts the schema-driven bail no longer
    // treats a counter column as fixed-8, i.e. it's on the
    // simple-scalar allowlist via the vint-length path, not excluded
    // from decode entirely (roborev finding, review-first pass).
    use crate::storage::commitlog::schema::is_simple_scalar_type;
    assert!(is_simple_scalar_type("counter"));
    assert_eq!(cql_fixed_len("counter"), None);
}

#[test]
fn tinyint_cell_stays_aligned_into_following_int_cell() {
    // Regression (PR #2797 blocker): tinyint is vint-length-prefixed, not
    // fixed-1. Before the fix, cql_fixed_len("tinyint") == Some(1) read the
    // uvint LENGTH byte (0x01) as the value and left the cursor one byte
    // behind, corrupting the following int cell. Hand-encode one row with a
    // tinyint cell (value 5) followed by an int cell (value 42) and prove
    // both decode with the cursor staying aligned across the tinyint.
    let schema = CommitLogSchema {
        keyspace: "commitlog_test".into(),
        table: "gauges".into(),
        partition_key: vec![ColumnSpec::new("id", "int")],
        clustering: vec![],
        columns: vec![
            ColumnSpec::new("flag", "tinyint"),
            ColumnSpec::new("count", "int"),
        ],
    };
    let column_names = vec!["flag".to_string(), "count".to_string()];

    let bytes: Vec<u8> = [
        // Row flags: all columns present, nothing else.
        ROW_HAS_ALL_COLUMNS,
        // Cell 1 ("flag", tinyint): use-row-timestamp, then vint-length(1) + 0x05.
        CELL_USE_ROW_TIMESTAMP,
        0x01, // uvint length = 1
        0x05, // the tinyint byte value 5
        // Cell 2 ("count", int): use-row-timestamp, then fixed-4 big-endian 42.
        CELL_USE_ROW_TIMESTAMP,
        0x00,
        0x00,
        0x00,
        0x2A,
        // Terminator.
        ROW_END_OF_PARTITION,
    ]
    .to_vec();

    let mut c = Cursor::new(&bytes);
    let rows = match decode_rows(&mut c, &schema, &column_names, 0) {
        Ok(rows) => rows,
        Err(_) => panic!("row with a variable-length tinyint cell must decode"),
    };
    assert_eq!(rows.len(), 1);
    let cells = &rows[0].cells;
    let flag = cells.iter().find(|c| c.column == "flag").unwrap();
    assert_eq!(
        flag.value.as_deref(),
        Some([0x05].as_ref()),
        "tinyint value must be the single byte 5, not the vint length"
    );
    let count = cells.iter().find(|c| c.column == "count").unwrap();
    assert_eq!(
        count.value.as_deref(),
        Some([0x00, 0x00, 0x00, 0x2A].as_ref()),
        "int cell must stay aligned after the variable-length tinyint"
    );
}

#[test]
fn batch_declaring_more_updates_than_present_reports_incomplete() {
    // ALICE_BODY declares num_updates=1 (leading uvint 0x01). Flip it to
    // 0x02: the mutation now claims 2 partition updates but only one's
    // worth of bytes exists. Decoded WITHOUT a schema, the first
    // decode_partition_update structurally bails (consumed_fully=false)
    // before a second iteration is attempted, so the loop stops after one
    // update and honestly reports the shortfall (regression: this field
    // was silently absent — a multi-table batch reported only its first
    // update with no signal more existed).
    let mut body = hex(ALICE_BODY);
    body[0] = 0x02;
    let m = decode_mutation(&body, &HashMap::new()).expect("decode");
    assert_eq!(m.updates.len(), 1);
    assert!(
        !m.updates_complete,
        "declared 2 updates but only 1 present must report updates_complete == false"
    );
}

#[test]
fn static_row_bail_leaves_column_names_empty() {
    // Minimal partition-update body: 16-byte table id + pk_len=0 +
    // iter_flags=ITER_HAS_STATIC_ROW + the three EncodingStats uvints (read
    // unconditionally BEFORE the static check). The static-row path is not
    // modeled: it must bail to structural-only BEFORE reading any
    // column-names block, so column_names stays empty rather than holding
    // misparsed bytes surfaced as if authoritative (roborev finding).
    let mut bytes = vec![0u8; 16]; // table_id
    bytes.push(0x00); // pk_len = 0
    bytes.push(ITER_HAS_STATIC_ROW); // iter_flags = 0x08
    bytes.extend_from_slice(&[0x00, 0x00, 0x00]); // EncodingStats: min ts, ldt, ttl
    let (update, consumed_fully) =
        decode_partition_update(&mut Cursor::new(&bytes), &SchemaSet::new()).expect("decode");
    assert!(
        !consumed_fully,
        "static-row path must not report full consume"
    );
    assert!(!update.rows_decoded);
    assert!(
        update.column_names.is_empty(),
        "static-row bail must happen before any column-names read"
    );
}

#[test]
fn empty_partition_fast_path_surfaces_partition_deletion() {
    // Minimal body: 16-byte table id + pk_len=0 +
    // iter_flags=ITER_IS_EMPTY|ITER_HAS_PARTITION_DELETION (0x01|0x04 =
    // 0x05). has_partition_deletion is set from iter_flags at struct
    // construction, BEFORE the ITER_IS_EMPTY fast-return — this proves the
    // fast-return path still surfaces the deletion flag (regression: the
    // early returns previously left it false even when the partition
    // carried a deletion).
    let mut bytes = vec![0u8; 16]; // table_id
    bytes.push(0x00); // pk_len = 0
    bytes.push(ITER_IS_EMPTY | ITER_HAS_PARTITION_DELETION); // 0x05
    let (update, consumed_fully) =
        decode_partition_update(&mut Cursor::new(&bytes), &SchemaSet::new()).expect("decode");
    assert!(
        update.has_partition_deletion,
        "empty-partition fast path must still surface the partition deletion"
    );
    assert!(update.rows_decoded, "empty partition is fully consumed");
    assert!(consumed_fully);
}

#[test]
fn read_column_names_rejects_hostile_count() {
    // A CRC-valid body could declare a huge column count with too few
    // trailing bytes to back it; the count must be rejected against the
    // cursor's remaining bytes BEFORE spinning the loop, not spun `count`
    // times (each zero-length name is 1 byte, so count > remaining is
    // provably impossible). Encode u64::MAX as an unsigned vint (0xFF then
    // eight 0xFF bytes) with no trailing name bytes.
    let bytes = [0xFFu8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
    let err = read_column_names(&mut Cursor::new(&bytes)).unwrap_err();
    assert!(
        matches!(err, Error::CorruptCommitLogFrame(_)),
        "hostile column count must be rejected as a corrupt frame, got {err:?}"
    );
}

fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

// ---- #4205: writetime reconstruction -----------------------------------------------------

#[test]
fn alice_body_writetime_is_epoch_plus_stats_plus_delta() {
    // Hand-decoded from ALICE_BODY: EncodingStats.minTimestamp uvint = 0xfd 36c1327cb48c
    // = (1<<48)|0x36c1327cb48c = 341678380332172; + TIMESTAMP_EPOCH (cassandra-5.0.8
    // EncodingStats: 2015-09-22T00:00:00Z = 1442880000000000) = 1784558380332172. The row
    // carries ROW_HAS_TIMESTAMP with delta 0, and both cells set CELL_USE_ROW_TIMESTAMP.
    const EXPECTED: i64 = 1_784_558_380_332_172;
    let body = hex(ALICE_BODY);
    let id = parse_table_id("d6de7150-8448-11f1-bf5a-bf4cbc47cb4a").unwrap();
    let mut schemas: SchemaSet = HashMap::new();
    schemas.insert(id, users_schema());
    let m = decode_mutation(&body, &schemas).expect("decode");
    let u = &m.updates[0];
    assert_eq!(u.min_timestamp, Some(EXPECTED));
    assert_eq!(u.rows[0].writetime, Some(EXPECTED));
    assert!(u.rows[0]
        .cells
        .iter()
        .all(|c| c.writetime == Some(EXPECTED)));
    assert_eq!(
        crate::storage::commitlog::mutation_writetime(u),
        crate::storage::commitlog::Writetime::Known(EXPECTED)
    );
}

#[test]
fn pre_epoch_min_timestamp_decodes_with_cassandra_wrapping_semantics() {
    // cassandra-5.0.8 `EncodingStats.Serializer`: `writeUnsignedVInt(minTimestamp - TIMESTAMP_EPOCH)`
    // / `readUnsignedVInt() + TIMESTAMP_EPOCH` as Java longs, so a timestamp 1_000_000 us BEFORE
    // the epoch is a delta of -1_000_000, written as its 64-bit two's-complement pattern (a 9-byte
    // vint: 0xFF + 8 big-endian bytes). Built from ALICE_BODY by replacing ONLY its 7-byte
    // minTimestamp uvint (`fd36c1327cb48c`); every other byte is the real Cassandra-written one.
    const EXPECTED: i64 = 1_442_880_000_000_000 - 1_000_000;
    let mut hexs = ALICE_BODY.to_string();
    let at = hexs
        .find("fd36c1327cb48c")
        .expect("ALICE_BODY carries the minTimestamp uvint");
    let mut repl = String::from("ff");
    for b in (-1_000_000i64 as u64).to_be_bytes() {
        repl.push_str(&format!("{b:02x}"));
    }
    hexs.replace_range(at..at + 14, &repl);
    let body = hex(&hexs);
    let id = parse_table_id("d6de7150-8448-11f1-bf5a-bf4cbc47cb4a").unwrap();
    let mut schemas: SchemaSet = HashMap::new();
    schemas.insert(id, users_schema());
    let m = decode_mutation(&body, &schemas).expect("decode");
    let u = &m.updates[0];
    assert_eq!(u.min_timestamp, Some(EXPECTED));
    assert_eq!(u.rows[0].writetime, Some(EXPECTED));
}

#[test]
fn undecodable_shapes_report_a_named_cause_never_a_value() {
    use crate::storage::commitlog::{mutation_writetime, Writetime};
    let body = hex(ALICE_BODY);
    let id = parse_table_id("d6de7150-8448-11f1-bf5a-bf4cbc47cb4a").unwrap();

    // No schema supplied.
    let m = decode_mutation(&body, &HashMap::new()).expect("decode");
    assert_eq!(
        mutation_writetime(&m.updates[0]),
        Writetime::Unmeasured("table schema not supplied")
    );

    // Clustered declaration of the same table.
    let mut clustered = users_schema();
    clustered.clustering = vec![ColumnSpec::new("seq", "int")];
    let mut schemas: SchemaSet = HashMap::new();
    schemas.insert(id, clustered);
    let m = decode_mutation(&body, &schemas).expect("decode");
    assert_eq!(
        mutation_writetime(&m.updates[0]),
        Writetime::Unmeasured("clustered table not decoded")
    );

    // Complex (collection) column declared for a name the header carries.
    let mut complex = users_schema();
    complex.columns = vec![
        ColumnSpec::new("age", "list<int>"),
        ColumnSpec::new("name", "text"),
    ];
    let mut schemas: SchemaSet = HashMap::new();
    schemas.insert(id, complex);
    let m = decode_mutation(&body, &schemas).expect("decode");
    assert_eq!(
        mutation_writetime(&m.updates[0]),
        Writetime::Unmeasured("complex column not decoded")
    );
}

#[test]
fn partition_deletion_flag_yields_the_partition_deletion_cause() {
    use crate::storage::commitlog::{mutation_writetime, Writetime};
    // ALICE_BODY with the iterator flags byte (offset 22) changed from 0x10 to 0x14: the
    // HAS_PARTITION_DELETION bit. decode_partition_update bails before reading rows.
    let mut body = hex(ALICE_BODY);
    assert_eq!(body[22], 0x10, "fixture shape");
    body[22] |= 0x04;
    let id = parse_table_id("d6de7150-8448-11f1-bf5a-bf4cbc47cb4a").unwrap();
    let mut schemas: SchemaSet = HashMap::new();
    schemas.insert(id, users_schema());
    let m = decode_mutation(&body, &schemas).expect("decode");
    assert!(m.updates[0].has_partition_deletion);
    assert_eq!(
        mutation_writetime(&m.updates[0]),
        Writetime::Unmeasured("partition-deletion timestamp not decoded")
    );
}

#[test]
fn a_body_truncated_mid_row_is_an_error_not_an_unmeasured_update() {
    // ALICE_BODY with its trailing END_OF_PARTITION + last cell bytes cut: the row decoder runs
    // out of bytes. That is malformed input, so `decode_mutation` must fail (find => exit 2),
    // not return an incomplete update with an `unmeasured` writetime.
    let id = parse_table_id("d6de7150-8448-11f1-bf5a-bf4cbc47cb4a").unwrap();
    let mut schemas: SchemaSet = HashMap::new();
    schemas.insert(id, users_schema());
    let full = hex(ALICE_BODY);
    for cut in 1..=6 {
        let body = &full[..full.len() - cut];
        let err =
            decode_mutation(body, &schemas).expect_err("a truncated row body must be an error");
        assert!(
            matches!(err, Error::CorruptCommitLogFrame(_)),
            "cut {cut}: {err:?}"
        );
    }
}
