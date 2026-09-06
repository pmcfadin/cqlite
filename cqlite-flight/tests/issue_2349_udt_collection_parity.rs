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
//! five text fields are asserted verbatim against the golden.
//!
//! Issue #2339 CLOSED the composite SET-element half, so
//! `contacts SET<FROZEN<contact_info>>` — a NESTED UDT in KEY position, carried in
//! each element's `cell_path` — is now projected too and asserted against the same
//! golden, field for field, including the inner `address` struct. It was excluded
//! while the merged-read assembler failed closed on it.
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

/// The two `contacts` elements for `TARGET_UUID`, verbatim from the golden JSONL
/// (issue #2339). Each is a `contact_info` whose third field is a NESTED
/// `frozen<address_type>`; the element lives entirely in the cell PATH (the cell
/// value is empty, `"value":""`), which is why this is the KEY-position composite
/// shape #2339 closed. sstabledump renders the same bytes as
/// `alyssa23\@example.com:(223)342-2641:423 Michael View Suite 577\:Smithfurt\:…`
/// — `:` separating `contact_info` fields, `\:` the inner `address_type` fields.
///
/// `[email, phone, street, city, state, zip_code, country]`, flattened so the
/// nested struct is compared field for field rather than as a formatted string.
fn golden_contacts() -> Vec<[&'static str; 7]> {
    vec![
        [
            "alyssa23@example.com",
            "(223)342-2641",
            "423 Michael View Suite 577",
            "Smithfurt",
            "CT",
            "83376",
            "Northern Mariana Islands",
        ],
        [
            "michaelmartinez@example.com",
            "542.210.8439",
            "169 Green Meadows",
            "Port Stephaniefurt",
            "TN",
            "96351",
            "Moldova",
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

/// Projection covers the value-position UDT column this issue fixes
/// (`addresses`) AND the KEY-position composite one #2339 closed (`contacts`).
fn spec() -> ScanSpec {
    ScanSpec {
        token: None,
        filter: None,
        projection: Some(vec![
            "user_id".into(),
            "addresses".into(),
            "contacts".into(),
        ]),
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

/// Resolve the `collections_with_udts-<cfid>` table dir by GLOB (issue #2349,
/// roborev job 1924 blocker 4) — never pinned to one CFID, so a dataset regen
/// (which mints a new CFID) does not silently turn this into a permanent skip.
///
/// Returns `None` (a legitimate skip) ONLY when `CQLITE_DATASETS_ROOT` is unset or
/// the keyspace dir is absent (a worktree without `fetch-datasets.sh`). When the
/// keyspace dir DOES exist but no `collections_with_udts-*` dir with a `Data.db` is
/// found, it PANICS — the fixture moved/regressed, which must fail loudly, not skip.
fn table_dir() -> Option<PathBuf> {
    let root = std::env::var_os("CQLITE_DATASETS_ROOT")?;
    let ks_dir = PathBuf::from(&root)
        .join("sstables")
        .join("test_collections");
    if !ks_dir.is_dir() {
        return None;
    }
    let mut found: Option<PathBuf> = None;
    for entry in std::fs::read_dir(&ks_dir)
        .expect("read test_collections dir")
        .flatten()
    {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        // Glob both the CFID (a regen mints a new one) AND the Data.db component
        // name (`nb-*`/`da-*`, any generation) so a fixture regen can never turn
        // this into a hard false failure (roborev job 1925 item 3).
        if name.starts_with("collections_with_udts-") && has_data_db(&entry.path()) {
            found = Some(entry.path());
            break;
        }
    }
    assert!(
        found.is_some(),
        "test_collections dir exists but no collections_with_udts-*/*-Data.db found \
         at {ks_dir:?} — the UDT-in-collection fixture moved or regressed (do NOT silently skip)"
    );
    found
}

/// Whether `dir` holds any `*-Data.db` SSTable component (any version/generation).
fn has_data_db(dir: &std::path::Path) -> bool {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .any(|e| e.file_name().to_string_lossy().ends_with("-Data.db"))
}

fn producer() -> MergeProducer {
    MergeProducer::with_spec(schema(), 64, spec())
        .unwrap()
        .with_udt_registry(udt_registry_from_cql(DDL, "test_collections"))
}

/// The batch row index of `target`'s partition — asserted present, so a corpus
/// that decoded no rows can never pass vacuously.
fn target_row(combined: &RecordBatch, target: &[u8; 16]) -> usize {
    let id_idx = combined.schema().index_of("user_id").expect("user_id");
    let ids = combined
        .column(id_idx)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("user_id FixedSizeBinary(16)");
    (0..combined.num_rows())
        .find(|&r| ids.value(r) == target.as_slice())
        .expect("target partition row present (never a 0-row false pass)")
}

/// Extract the `addresses` `List<Struct{...}>` value for the target partition as a
/// sorted set of `[street,city,state,zip_code,country]` rows.
fn decoded_addresses(combined: &RecordBatch, target: &[u8; 16]) -> Vec<[String; 5]> {
    let row = target_row(combined, target);
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

/// Extract the `contacts` `List<Struct{email,phone,address:Struct{…}}>` value for
/// the target partition, flattened to `[email, phone, street, city, state,
/// zip_code, country]` per element, in the order the reader returned them
/// (issue #2339 — element ORDER is part of the property: Cassandra orders a
/// composite set element by its TYPE comparator, `UserType.compare`, so
/// `alyssa23…` precedes `michaelmartinez…`).
fn decoded_contacts(combined: &RecordBatch, target: &[u8; 16]) -> Vec<[String; 7]> {
    let row = target_row(combined, target);
    let idx = combined.schema().index_of("contacts").expect("contacts");
    let list = combined
        .column(idx)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("contacts must decode as a List, not opaque bytes (issue #2339)");
    let structs = list.value(row);
    let structs = structs
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("contacts elements must be Struct (frozen<UDT> resolved from the cell_path)");
    let text = |sa: &StructArray, name: &str| -> StringArray {
        sa.column_by_name(name)
            .unwrap_or_else(|| panic!("contact_info field '{name}' present"))
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("contact_info field '{name}' is Utf8"))
            .clone()
    };
    let email = text(structs, "email");
    let phone = text(structs, "phone");
    let address = structs
        .column_by_name("address")
        .expect("contact_info.address present")
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("contact_info.address must be a NESTED Struct, not a formatted string")
        .clone();
    let (street, city, state, zip, country) = (
        text(&address, "street"),
        text(&address, "city"),
        text(&address, "state"),
        text(&address, "zip_code"),
        text(&address, "country"),
    );
    (0..structs.len())
        .map(|i| {
            [
                email.value(i).to_string(),
                phone.value(i).to_string(),
                street.value(i).to_string(),
                city.value(i).to_string(),
                state.value(i).to_string(),
                zip.value(i).to_string(),
                country.value(i).to_string(),
            ]
        })
        .collect()
}

fn expected_contacts() -> Vec<[String; 7]> {
    golden_contacts()
        .into_iter()
        .map(|c| c.map(String::from))
        .collect()
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
    // Issue #2339: the KEY-position composite. `produce_from_paths` is the COLD
    // path, which ALWAYS merges (no bypass_reason call at all), so this asserts the
    // MERGED-READ assembler decoded the nested `contact_info` — including its inner
    // `address_type` struct — from each element's cell_path.
    assert_eq!(
        decoded_contacts(&combined, &uuid_bytes(TARGET_UUID)),
        expected_contacts(),
        "cold path: contacts set<frozen<contact_info>> must decode field-for-field \
         (nested UDT included) in Cassandra's element order (issue #2339)"
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
    // Issue #2339, arm honesty: this fixture has ONE generation, and since #2339
    // made a RESOLVABLE composite set element decodable on both arms, the warm
    // route's bypass predicate now SELECTS the single-generation fast arm for this
    // schema. So this assertion pins warm-vs-cold-vs-golden agreement, NOT the
    // merged-read assembler — that is the cold case above (measured: with composite
    // decode disabled the cold case fails and this one still passes) and the
    // unprojected arm differential in `issue_3058_forced_path_differential.rs`,
    // which forces BOTH arms and asserts the bypass leg built zero mergers.
    assert_eq!(
        decoded_contacts(&combined, &uuid_bytes(TARGET_UUID)),
        expected_contacts(),
        "warm path: contacts set<frozen<contact_info>> must decode identically to \
         cold + golden (issue #2339)"
    );
}

/// Aggregate/group-by UDT-column resolution (roborev job 1925 item 4). A
/// `GROUP BY` on a top-level `frozen<address_type>` column puts a UDT-typed column
/// in the PARTIAL output. Under the PRODUCTION order (`with_udt_registry` THEN
/// `with_aggregation`, as `service.rs` builds it) that partial column must resolve
/// to an Arrow `Struct`, not opaque `Utf8` — else the aggregate schema silently
/// disagrees with the emitted arrays. The reverse order must yield an IDENTICAL
/// schema. Pure schema assertion — no SSTable read, so it never skips.
#[test]
fn aggregate_udt_group_by_column_resolves_to_struct_both_orders() {
    use arrow::datatypes::DataType;

    // id uuid PRIMARY KEY, home frozen<address_type>.
    let schema = TableSchema {
        keyspace: "test_collections".into(),
        table: "udt_agg".into(),
        partition_keys: vec![KeyColumn {
            name: "id".into(),
            data_type: "uuid".into(),
            position: 0,
        }],
        clustering_keys: Vec::<ClusteringColumn>::new(),
        columns: vec![col("id", "uuid"), col("home", "frozen<address_type>")],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };
    // GROUP BY home, count(*) AS cnt.
    let aggregation: cqlite_flight::ticket::Aggregation =
        serde_json::from_value(serde_json::json!({
            "group_by": ["home"],
            "aggregates": [{"func": "Count", "column": null, "output": "cnt"}]
        }))
        .expect("aggregation");
    let registry = || udt_registry_from_cql(DDL, "test_collections");

    // PRODUCTION order: registry first, then aggregation.
    let prod = MergeProducer::with_spec(schema.clone(), 64, ScanSpec::default())
        .unwrap()
        .with_udt_registry(registry())
        .with_aggregation(&aggregation)
        .unwrap();
    let prod_schema = prod.arrow_schema().unwrap();
    let home = prod_schema
        .field_with_name("home")
        .expect("home in partial schema");
    assert!(
        matches!(home.data_type(), DataType::Struct(_)),
        "production order: UDT group-by column must resolve to Struct, got {:?}",
        home.data_type()
    );

    // REVERSE order: aggregation first, then registry — must be identical.
    let rev = MergeProducer::with_spec(schema, 64, ScanSpec::default())
        .unwrap()
        .with_aggregation(&aggregation)
        .unwrap()
        .with_udt_registry(registry());
    assert_eq!(
        prod_schema,
        rev.arrow_schema().unwrap(),
        "both builder orders must yield an identical aggregate Arrow schema (#2349)"
    );
}
