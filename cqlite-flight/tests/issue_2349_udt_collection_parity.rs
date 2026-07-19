//! Issue #2349: the Flight read path (BOTH cold and warm) must decode a
//! `frozen<UDT>` cell inside a collection STRUCTURALLY — the #1234 silent
//! data-loss class — once the table's UDT registry is resolved from the ticket
//! DDL's `CREATE TYPE` statements and threaded onto every merge reader.
//!
//! Oracle: the sstabledump JSONL golden for `test_collections.collections_with_udts`
//! (`LIST<FROZEN<address_type>>` etc.). Before this fix the cold path rendered
//! `addresses` as an opaque `List<Utf8>` (the `Custom("udt:address_type")` type
//! never resolved), losing the per-field structure; now it is a
//! `List<Struct{street,city,state,zip_code,country}>` whose values match the
//! golden field-for-field. The warm path (`WarmTableRegistry`) opens its shared
//! readers with the SAME resolved registry, so it decodes IDENTICALLY — the
//! warm-vs-cold parity the issue requires.
//!
//! Partition `e94f10e8-…` carries a two-element `addresses` list; both elements'
//! five text fields are asserted verbatim against the golden. The set-element
//! (`contacts SET<FROZEN<contact_info>>`) column is intentionally NOT projected:
//! a composite SET element is the separate merged-read limitation tracked by
//! #2339 (a key-position composite the assembler still fails closed on), out of
//! this issue's value-position-UDT scope.
//!
//! Skips (never fails) when `CQLITE_DATASETS_ROOT` is unset or the `Data.db`
//! binary is absent (a worktree without `fetch-datasets.sh`), but asserts the
//! target row IS found and decodes structurally whenever it runs — never a silent
//! 0-row false pass.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Array, FixedSizeBinaryArray, ListArray, StringArray, StructArray};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;

use cqlite_core::schema::{
    udt_registry_from_cql, ClusteringColumn, Column, KeyColumn, TableSchema,
};
use cqlite_flight::cancel::CancelFlag;
use cqlite_flight::filter::ScanSpec;
use cqlite_flight::producer::{DirSource, MergeProducer, SstableSource};
use cqlite_flight::warm::{ddl_hash, TableKey, WarmTableRegistry};

/// The CREATE TYPE + CREATE TABLE DDL a Trino connector would send for the table
/// (only the `CREATE TYPE`s drive registry resolution).
const DDL: &str = "\
CREATE TYPE address_type (street text, city text, state text, zip_code text, country text); \
CREATE TYPE contact_info (email text, phone text, address frozen<address_type>); \
CREATE TABLE collections_with_udts (user_id uuid PRIMARY KEY, addresses list<frozen<address_type>>, \
contacts set<frozen<contact_info>>, locations_visited map<date, frozen<address_type>>, \
emergency_contacts map<text, frozen<contact_info>>)";

const TARGET_UUID: &str = "e94f10e8-6d74-4da3-ae2f-e3d92cf68976";

/// The two `addresses` UDT structs for `TARGET_UUID`, verbatim from the golden
/// JSONL (`nb-1-big-Data.db.jsonl`). Order-insensitive: compared as a sorted set.
fn golden_addresses() -> Vec<[&'static str; 5]> {
    vec![
        [
            "07372 Mary Shoals Suite 758",
            "Alyssafurt",
            "IL",
            "79107",
            "British Indian Ocean Territory (Chagos Archipelago)",
        ],
        [
            "13898 Adam Port Suite 788",
            "East Veronica",
            "IL",
            "30919",
            "Philippines",
        ],
    ]
}

fn col(name: &str, ty: &str) -> Column {
    Column {
        name: name.into(),
        data_type: ty.into(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

fn schema() -> TableSchema {
    TableSchema {
        keyspace: "test_collections".into(),
        table: "collections_with_udts".into(),
        partition_keys: vec![KeyColumn {
            name: "user_id".into(),
            data_type: "uuid".into(),
            position: 0,
        }],
        clustering_keys: Vec::<ClusteringColumn>::new(),
        columns: vec![
            col("user_id", "uuid"),
            col("addresses", "list<frozen<address_type>>"),
            col("contacts", "set<frozen<contact_info>>"),
            col("locations_visited", "map<date, frozen<address_type>>"),
            col("emergency_contacts", "map<text, frozen<contact_info>>"),
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Projection excludes the composite-SET column `contacts` (#2339), keeping the
/// value-position UDT columns this issue fixes.
fn spec() -> ScanSpec {
    ScanSpec {
        token: None,
        filter: None,
        projection: Some(vec!["user_id".into(), "addresses".into()]),
        limit: None,
    }
}

fn uuid_bytes(s: &str) -> [u8; 16] {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    let mut out = [0u8; 16];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).expect("hex");
    }
    out
}

fn table_dir() -> Option<PathBuf> {
    let root = std::env::var_os("CQLITE_DATASETS_ROOT")?;
    let dir = PathBuf::from(&root)
        .join("sstables")
        .join("test_collections")
        .join("collections_with_udts-6bc2bae0a25111f0a3fef1a551383fb9");
    if dir.join("nb-1-big-Data.db").is_file() {
        Some(dir)
    } else {
        None
    }
}

fn producer() -> MergeProducer {
    MergeProducer::with_spec(schema(), 64, spec())
        .unwrap()
        .with_udt_registry(udt_registry_from_cql(DDL, "test_collections"))
}

/// Extract the `addresses` `List<Struct{...}>` value for the target partition as a
/// sorted set of `[street,city,state,zip_code,country]` rows.
fn decoded_addresses(combined: &RecordBatch, target: &[u8; 16]) -> Vec<[String; 5]> {
    let id_idx = combined.schema().index_of("user_id").expect("user_id");
    let ids = combined
        .column(id_idx)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("user_id FixedSizeBinary(16)");
    let row = (0..combined.num_rows())
        .find(|&r| ids.value(r) == target.as_slice())
        .expect("target partition row present (never a 0-row false pass)");

    let addr_idx = combined.schema().index_of("addresses").expect("addresses");
    let list = combined
        .column(addr_idx)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("addresses must decode as a List (registry resolved), not opaque bytes");
    let structs = list.value(row);
    let structs = structs
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("addresses elements must be Struct (frozen<UDT> resolved), not Utf8/Binary");

    let field = |name: &str| -> StringArray {
        structs
            .column_by_name(name)
            .unwrap_or_else(|| panic!("UDT field '{name}' present in the Struct"))
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("UDT field '{name}' is Utf8"))
            .clone()
    };
    let (street, city, state, zip, country) = (
        field("street"),
        field("city"),
        field("state"),
        field("zip_code"),
        field("country"),
    );
    let mut out: Vec<[String; 5]> = (0..structs.len())
        .map(|i| {
            [
                street.value(i).to_string(),
                city.value(i).to_string(),
                state.value(i).to_string(),
                zip.value(i).to_string(),
                country.value(i).to_string(),
            ]
        })
        .collect();
    out.sort();
    out
}

fn expected_sorted() -> Vec<[String; 5]> {
    let mut want: Vec<[String; 5]> = golden_addresses()
        .into_iter()
        .map(|a| a.map(String::from))
        .collect();
    want.sort();
    want
}

/// COLD path (`produce_from_paths` → `KWayMerger::new_with_gc_and_registry_cancellable`).
#[test]
fn cold_path_decodes_udt_in_collection_matching_golden() {
    let Some(dir) = table_dir() else {
        eprintln!("collections_with_udts Data.db absent — skipping (run fetch-datasets.sh)");
        return;
    };
    let producer = producer();
    let batches = producer
        .produce_from_paths(DirSource::new(&dir).data_paths().unwrap())
        .unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        rows > 0,
        "cold scan must return rows (never a 0-row false pass)"
    );

    let arrow_schema = producer.arrow_schema().unwrap();
    let combined = concat_batches(&arrow_schema.into(), &batches).unwrap();
    assert_eq!(
        decoded_addresses(&combined, &uuid_bytes(TARGET_UUID)),
        expected_sorted(),
        "cold path: addresses frozen<UDT> list must decode field-for-field to the golden"
    );
}

/// WARM path (`WarmTableRegistry` shared readers → `produce_streaming_from_readers`).
/// Opens the warm reader set WITH the same resolved registry, proving warm and
/// cold decode IDENTICALLY (issue #2349 parity requirement).
#[test]
fn warm_path_decodes_udt_in_collection_matching_cold_and_golden() {
    let Some(dir) = table_dir() else {
        eprintln!("collections_with_udts Data.db absent — skipping (run fetch-datasets.sh)");
        return;
    };
    let producer = producer();
    let schema = schema();
    let registry = udt_registry_from_cql(DDL, "test_collections");

    let warm = WarmTableRegistry::new();
    let key = TableKey::new("test_collections", "collections_with_udts");
    let set = warm
        .warm_readers(
            &key,
            ddl_hash(DDL),
            &schema,
            Some(&registry),
            &dir,
            None,
            &CancelFlag::new(),
        )
        .expect("warm readers");
    assert!(!set.readers.is_empty(), "warm set has readers");
    for r in &set.readers {
        assert!(
            r.has_udt_registry(),
            "warm readers must carry the resolved registry (parity with cold, #2349)"
        );
    }

    let readers: Vec<Arc<_>> = set.readers.clone();
    let batches = producer
        .produce_streaming_from_readers_to_vec(readers, &CancelFlag::new())
        .unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        rows > 0,
        "warm scan must return rows (never a 0-row false pass)"
    );

    let arrow_schema = producer.arrow_schema().unwrap();
    let combined = concat_batches(&arrow_schema.into(), &batches).unwrap();
    assert_eq!(
        decoded_addresses(&combined, &uuid_bytes(TARGET_UUID)),
        expected_sorted(),
        "warm path: addresses frozen<UDT> list must decode identically to cold + golden"
    );
}
