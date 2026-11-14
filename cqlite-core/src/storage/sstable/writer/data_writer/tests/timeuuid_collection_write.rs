//! Issue #4534 — a multicell `set<timeuuid>` / `map<timeuuid,…>` is written in `TimeUUIDType`
//! order (the DECLARED element/key type decides), while `set<uuid>` keeps `UUIDType` order.
//!
//! Two v1 UUIDs share one timestamp (identical msb) and differ only in the lsb's first byte,
//! 0x7f vs 0x80: `UUIDType` compares the lsb UNSIGNED (0x7f first), `TimeUUIDType` SIGNED
//! (0x80 = -128 first) — cassandra-5.0.8 `UUIDType.java:118` vs `AbstractTimeUUIDType.java:84-86`.

use super::super::*;
use super::support::*;
use crate::types::Value;

fn v1(lsb0: u8) -> [u8; 16] {
    let mut u = [0u8; 16];
    u[0..4].copy_from_slice(&0x1234_5678u32.to_be_bytes());
    u[4..6].copy_from_slice(&0x9abcu16.to_be_bytes());
    u[6..8].copy_from_slice(&0x1def_u16.to_be_bytes()); // version 1 + time_hi
    u[8] = lsb0;
    u[9..].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7]);
    u
}

fn col(data_type: &str) -> Column {
    Column {
        name: "c".to_string(),
        data_type: data_type.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

fn cell_paths(data_type: &str, value: &Value) -> Vec<Vec<u8>> {
    let writer = DataWriter::new(create_test_stats());
    let mut buf = Vec::new();
    writer
        .write_complex_column(
            &mut buf,
            &col(data_type),
            value,
            1_700_000_000_000_000,
            None,
            TEST_NOW_SECONDS,
        )
        .expect("collection writes");
    let (_ts, _ldt, cells) = decode_complex_column(&buf);
    cells.into_iter().map(|c| c.cell_path).collect()
}

#[test]
fn set_of_timeuuid_is_written_in_timeuuid_order_and_set_of_uuid_in_uuid_order() {
    let (a, b) = (v1(0x7f), v1(0x80));
    let value = Value::Set(vec![Value::Uuid(a), Value::Uuid(b)]);
    assert_eq!(
        cell_paths("set<timeuuid>", &value),
        vec![b.to_vec(), a.to_vec()],
        "timeuuid: signed lsb, 0x80 (-128) sorts before 0x7f"
    );
    assert_eq!(
        cell_paths("set<uuid>", &value),
        vec![a.to_vec(), b.to_vec()],
        "uuid: unsigned lsb, 0x7f before 0x80 (negative control: the declared type decides)"
    );
}

#[test]
fn map_with_timeuuid_keys_is_written_in_timeuuid_order() {
    let (a, b) = (v1(0x7f), v1(0x80));
    let value = Value::Map(vec![
        (Value::Uuid(a), Value::Integer(1)),
        (Value::Uuid(b), Value::Integer(2)),
    ]);
    assert_eq!(
        cell_paths("map<timeuuid, int>", &value),
        vec![b.to_vec(), a.to_vec()]
    );
    assert_eq!(
        cell_paths("map<uuid, int>", &value),
        vec![a.to_vec(), b.to_vec()]
    );
}

#[test]
fn declared_type_detection_looks_through_frozen_and_ignores_values_and_non_uuid() {
    use crate::storage::sstable::writer::data_writer::declared_element_is_timeuuid as d;
    assert!(d("set<timeuuid>", false));
    assert!(d("frozen<set<timeuuid>>", false));
    assert!(d("map<timeuuid, text>", true));
    assert!(
        !d("map<text, timeuuid>", true),
        "only the KEY decides a map's order"
    );
    assert!(!d("set<uuid>", false));
    assert!(!d("set<int>", false));
    assert!(!d("not a type", false));
}

/// ORACLE: the Cassandra-5.0.8-written `set<timeuuid>` cell paths of the committed
/// `timeuuid_ties` fixture (tied v1 timestamps, lsb bytes across 0x7f/0x80) are in Cassandra's
/// order in the `sstabledump` golden. The writer, given the same elements SHUFFLED, must emit
/// exactly that order for a declared `set<timeuuid>`; declared `set<uuid>` must NOT (negative
/// control — a flag swap or dropped signed-lsb compare turns this red).
#[test]
fn writer_reproduces_cassandra_set_timeuuid_order_from_the_committed_golden() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../test-data/fixtures/issue_4531/test_uuid_bti");
    let table_dir = std::fs::read_dir(&dir)
        .expect("fixture dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with("timeuuid_ties-"))
        })
        .expect("committed timeuuid_ties fixture");
    let golden = std::fs::read_to_string(table_dir.join("da-2-bti-Data.db.jsonl")).expect("golden");
    let parse = |s: &str| -> [u8; 16] {
        let hex: String = s.chars().filter(|c| *c != '-').collect();
        let mut u = [0u8; 16];
        for (i, b) in u.iter_mut().enumerate() {
            *b = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).expect("hex");
        }
        u
    };
    let (mut rows, mut differs_for_uuid) = (0, 0);
    for line in golden.lines().filter(|l| !l.trim().is_empty()) {
        let v: serde_json::Value = serde_json::from_str(line).expect("json");
        for r in v["rows"].as_array().expect("rows") {
            let in_cassandra_order: Vec<[u8; 16]> = r["cells"]
                .as_array()
                .expect("cells")
                .iter()
                .filter(|c| c["name"] == "tags" && c.get("path").is_some())
                .map(|c| parse(c["path"][0].as_str().expect("path")))
                .collect();
            assert_eq!(in_cassandra_order.len(), 4, "four tied elements per row");
            let mut shuffled = in_cassandra_order.clone();
            shuffled.reverse();
            shuffled.rotate_left(1);
            let value = Value::Set(shuffled.iter().map(|u| Value::Uuid(*u)).collect());
            let want: Vec<Vec<u8>> = in_cassandra_order.iter().map(|u| u.to_vec()).collect();
            assert_eq!(cell_paths("set<timeuuid>", &value), want, "row {rows}");
            if cell_paths("set<uuid>", &value) != want {
                differs_for_uuid += 1;
            }
            rows += 1;
        }
    }
    assert!(
        rows >= 400,
        "golden must carry the big partition's rows, got {rows}"
    );
    assert!(
        differs_for_uuid > 0,
        "negative control: the uuid order must differ from Cassandra's timeuuid order on SOME row"
    );
    eprintln!(
        "set<timeuuid> golden: {rows} rows checked, uuid-order differs on {differs_for_uuid}"
    );
}
