//! Issue #4205 (`cqlite find`) — corpus oracle for `SSTableReader::probe_generation`.
//!
//! Oracle: Cassandra-written `*-Data.db.jsonl` goldens (sstabledump 5.0.8). For every key in
//! ANY generation's golden, against EVERY generation of the table:
//!
//!   * `Hit` iff that generation's golden contains the key, else a non-`Hit` index-layer
//!     verdict (`BloomNegative` / `IndexMiss`, never `Scanned`, never `Hit`) — F1.1;
//!   * on a hit, `data_offset` = golden `partition.position` minus the partition header
//!     (`2 + key_len + 12`, see `support/partition_start.rs` for why), `byte_length` = the
//!     distance to the next golden partition start or `ToEof` for the last — F2.1/F2.2,
//!     `max_writetime` = max of every `tstamp` / `marked_deleted` in the partition JSON,
//!     `has_partition_deletion` = `partition.deletion_info` present — F2.1.
//!
//! Never compares against CQLite's own scan output. Every case asserts per (key, generation)
//! and counts its probes; a case that probed nothing fails (#3220).

#![cfg(all(feature = "state_machine", not(feature = "tombstones")))]

#[path = "support/datasets_root.rs"]
mod datasets_root;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::cql_parser::split_cql_statements;
use cqlite_core::schema::{parse_cql_schema, TableSchema};
use cqlite_core::storage::partition_key_codec::encode_partition_key_columns;
use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::sstable::{ByteLength, GenerationProbe, SSTableReader};
use cqlite_core::{Config, Value};
use serde_json::Value as Json;

struct Case {
    keyspace: &'static str,
    table: &'static str,
    schema: &'static str,
    /// `nb` BIG or `da` BTI — asserted against the fixture's file names.
    bti: bool,
}

const CASES: &[Case] = &[
    Case {
        keyspace: "test_basic",
        table: "simple_table",
        schema: "basic-types.cql",
        bti: false,
    },
    Case {
        keyspace: "test_basic",
        table: "multi_partition_table",
        schema: "basic-types.cql",
        bti: false,
    },
    Case {
        keyspace: "test_tomb",
        table: "skipped_partition_delete",
        schema: "tombstone-parity.cql",
        bti: false,
    },
    Case {
        keyspace: "test_types",
        table: "ct_multi_sstable_merge",
        schema: "cql-type-parity.cql",
        bti: false,
    },
    Case {
        keyspace: "test_da",
        table: "simple_table",
        schema: "da-test.cql",
        bti: true,
    },
];

fn strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

fn load_schema(schema_file: &str, table: &str) -> TableSchema {
    let path = datasets_root::schema_path(schema_file)
        .unwrap_or_else(|| panic!("committed schema {schema_file} not found"));
    let cql = std::fs::read_to_string(&path).expect("read schema");
    for stmt in split_cql_statements(&cql) {
        if let Ok(s) = parse_cql_schema(&stmt) {
            if s.table == table {
                return s;
            }
        }
    }
    panic!("table {table} not found in {schema_file}");
}

/// ISO-8601 (`2025-10-06T01:12:05.394120Z`) to microseconds since the Unix epoch.
fn micros(iso: &str) -> i64 {
    let dt = chrono::DateTime::parse_from_rfc3339(iso)
        .unwrap_or_else(|e| panic!("golden timestamp {iso:?}: {e}"));
    dt.timestamp() * 1_000_000 + i64::from(dt.timestamp_subsec_micros())
}

/// Max of every `tstamp` / `marked_deleted` string anywhere in the partition JSON.
fn max_ts(v: &Json, acc: &mut Option<i64>) {
    match v {
        Json::Object(m) => {
            for (k, child) in m {
                if (k == "tstamp" || k == "marked_deleted") && child.is_string() {
                    let t = micros(child.as_str().unwrap_or_default());
                    *acc = Some(acc.map_or(t, |a| a.max(t)));
                } else {
                    max_ts(child, acc);
                }
            }
        }
        Json::Array(a) => a.iter().for_each(|c| max_ts(c, acc)),
        _ => {}
    }
}

fn key_value(cql_type: &str, j: &Json) -> Value {
    let s = j
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| j.to_string());
    match cql_type.to_ascii_lowercase().as_str() {
        "uuid" => Value::Uuid(*uuid::Uuid::parse_str(&s).expect("uuid key").as_bytes()),
        "int" => Value::Integer(s.parse().expect("int key")),
        "text" | "varchar" | "ascii" => Value::text(s),
        other => panic!("test does not map partition key type {other}"),
    }
}

fn encode_key(schema: &TableSchema, parts: &[Json]) -> Vec<u8> {
    assert_eq!(parts.len(), schema.partition_keys.len(), "golden key arity");
    let values: Vec<Value> = schema
        .partition_keys
        .iter()
        .zip(parts)
        .map(|(c, j)| key_value(&c.data_type, j))
        .collect();
    encode_partition_key_columns(&values, schema).expect("encode golden key")
}

struct GoldenPartition {
    key: Vec<u8>,
    start: u64,
    max_ts: i64,
    deleted: bool,
}

/// One generation's golden: partitions in file order with authoritative starts.
fn read_golden(jsonl: &Path, schema: &TableSchema, bti: bool) -> Vec<GoldenPartition> {
    let text = std::fs::read_to_string(jsonl).expect("read golden");
    let mut out = Vec::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let v: Json = serde_json::from_str(line).expect("golden line json");
        let part = &v["partition"];
        let key_parts = part["key"].as_array().expect("key array");
        let key = encode_key(schema, key_parts);
        let position = part["position"].as_u64().expect("position");
        let deleted = part.get("deletion_info").is_some();
        // Partition header = 2-byte key length + key + DeletionTime. `nb` writes the
        // 12-byte (ldt + mfda) form; `da`/`oa` write the 1-byte LIVE flag (cassandra-5.0.8
        // `DeletionTime.Serializer`) — asserted live here because the deleted `da` form is
        // not exercised by any committed `da` golden.
        assert!(
            !(bti && deleted),
            "a deleted `da` partition has no verified header size"
        );
        let header = 2 + key.len() as u64 + if bti { 1 } else { 12 };
        let mut m = None;
        max_ts(&v, &mut m);
        out.push(GoldenPartition {
            key,
            start: position
                .checked_sub(header)
                .expect("position covers the partition header"),
            max_ts: m.expect("every golden partition carries a timestamp"),
            deleted,
        });
    }
    assert!(!out.is_empty(), "golden {jsonl:?} is empty");
    out
}

async fn open(path: &Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    SSTableReader::open(path, &config, platform)
        .await
        .expect("open generation")
}

fn data_dbs(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("read table dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.to_string_lossy().ends_with("-Data.db"))
        .collect();
    v.sort();
    v
}

fn resolve(case: &Case) -> Option<PathBuf> {
    match datasets_root::resolve_table_generation_dir(case.keyspace, case.table) {
        Ok(d) => Some(d),
        Err(e) if strict() => panic!(
            "{}.{}: fixture missing under CQLITE_REQUIRE_FIXTURES: {e}",
            case.keyspace, case.table
        ),
        Err(e) => {
            eprintln!("SKIP {}.{}: {e}", case.keyspace, case.table);
            None
        }
    }
}

#[tokio::test]
async fn probe_generation_matches_sstabledump_goldens_per_key_per_generation() {
    let mut cases_run = 0;
    for case in CASES {
        let Some(dir) = resolve(case) else { continue };
        let schema = load_schema(case.schema, case.table);
        let gens = data_dbs(&dir);
        assert!(
            !gens.is_empty(),
            "{}.{}: no generation",
            case.keyspace,
            case.table
        );
        let bti_files = gens.iter().all(|g| g.to_string_lossy().contains("-bti-"));
        assert_eq!(
            bti_files, case.bti,
            "{}.{}: format of fixture",
            case.keyspace, case.table
        );

        // Per generation: golden partitions + reader.
        let mut goldens: Vec<Vec<GoldenPartition>> = Vec::new();
        let mut readers = Vec::new();
        for g in &gens {
            let jsonl = PathBuf::from(format!("{}.jsonl", g.display()));
            goldens.push(read_golden(&jsonl, &schema, case.bti));
            readers.push(open(g).await);
        }
        // Universe: every key of every generation, deduplicated.
        let mut universe: BTreeMap<Vec<u8>, ()> = BTreeMap::new();
        for g in &goldens {
            for p in g {
                universe.insert(p.key.clone(), ());
            }
        }
        let mut hits = 0usize;
        let mut negatives = 0usize;
        let cancel = ScanCancel::new();
        for key in universe.keys() {
            for (gi, reader) in readers.iter().enumerate() {
                let probe = reader
                    .probe_generation(key, Some(&schema), &cancel)
                    .await
                    .unwrap_or_else(|e| panic!("{}.{} gen {gi}: {e}", case.keyspace, case.table));
                let idx = goldens[gi].iter().position(|p| &p.key == key);
                match (idx, probe) {
                    (Some(i), GenerationProbe::Hit(d)) => {
                        let p = &goldens[gi][i];
                        assert_eq!(
                            d.data_offset,
                            Some(p.start),
                            "{}.{} gen {gi}: offset",
                            case.keyspace,
                            case.table
                        );
                        let want = goldens[gi]
                            .iter()
                            .map(|q| q.start)
                            .filter(|s| *s > p.start)
                            .min()
                            .map_or(ByteLength::ToEof, |next| ByteLength::Known(next - p.start));
                        assert_eq!(
                            d.byte_length, want,
                            "{}.{} gen {gi}: length",
                            case.keyspace, case.table
                        );
                        assert_eq!(
                            d.max_writetime, p.max_ts,
                            "{}.{} gen {gi}: max_writetime",
                            case.keyspace, case.table
                        );
                        assert_eq!(
                            d.has_partition_deletion, p.deleted,
                            "{}.{} gen {gi}: partition deletion",
                            case.keyspace, case.table
                        );
                        hits += 1;
                    }
                    (None, GenerationProbe::BloomNegative) if !case.bti => negatives += 1,
                    (None, GenerationProbe::IndexMiss) => negatives += 1,
                    (idx, probe) => panic!(
                        "{}.{} gen {gi}: golden contains key = {}, probe = {probe:?}",
                        case.keyspace,
                        case.table,
                        idx.is_some()
                    ),
                }
            }
        }
        assert!(
            hits > 0,
            "{}.{}: no Hit was ever produced",
            case.keyspace,
            case.table
        );
        // Only a multi-generation table can produce a cross-generation negative; the others
        // are covered by the absent-key test below.
        if gens.len() > 1 && goldens.iter().any(|g| g.len() != universe.len()) {
            assert!(
                negatives > 0,
                "{}.{}: expected cross-generation negatives",
                case.keyspace,
                case.table
            );
        }
        cases_run += 1;
    }
    if strict() {
        assert_eq!(
            cases_run,
            CASES.len(),
            "every case must run under CQLITE_REQUIRE_FIXTURES"
        );
    }
}

/// F1.2 (+F1.4 for BTI): keys present in NO generation never report `Hit` or `Scanned`.
#[tokio::test]
async fn absent_keys_are_index_layer_negatives_never_hit_or_scanned() {
    for case in CASES {
        let Some(dir) = resolve(case) else { continue };
        let schema = load_schema(case.schema, case.table);
        let gens = data_dbs(&dir);
        let mut present = std::collections::HashSet::new();
        for g in &gens {
            for p in read_golden(
                &PathBuf::from(format!("{}.jsonl", g.display())),
                &schema,
                case.bti,
            ) {
                present.insert(p.key);
            }
        }
        let mut readers = Vec::new();
        for g in &gens {
            readers.push(open(g).await);
        }
        let cancel = ScanCancel::new();
        let mut probed = 0;
        let mut collisions = 0;
        for n in 0..4096u32 {
            let parts: Vec<Json> = schema
                .partition_keys
                .iter()
                .map(|c| match c.data_type.to_ascii_lowercase().as_str() {
                    "uuid" => {
                        Json::String(uuid::Uuid::from_u128(0xA5A5_0000 + u128::from(n)).to_string())
                    }
                    "int" => Json::String((900_000 + n).to_string()),
                    _ => Json::String(format!("absent-{n}")),
                })
                .collect();
            let key = encode_key(&schema, &parts);
            if present.contains(&key) {
                continue;
            }
            for g in &gens {
                let reader = open(g).await;
                let probe = reader
                    .probe_generation(&key, Some(&schema), &cancel)
                    .await
                    .expect("probe");
                assert!(
                    matches!(
                        probe,
                        GenerationProbe::BloomNegative | GenerationProbe::IndexMiss
                    ),
                    "{}.{}: absent key reported {probe:?}",
                    case.keyspace,
                    case.table
                );
                if case.bti && matches!(reader.lookup_partition_via_bti_trie(&key), Ok(Some(_))) {
                    // F1.4: the trie returned a CANDIDATE (prefix collision) for an absent key,
                    // and the verdict is still IndexMiss with no scan.
                    collisions += 1;
                }
                probed += 1;
            }
        }
        assert!(
            probed > 0,
            "{}.{}: probed nothing",
            case.keyspace,
            case.table
        );
        if case.bti {
            assert!(
                collisions > 0,
                "{}.{}: no BTI prefix collision was exercised",
                case.keyspace,
                case.table
            );
        }
    }
}

/// F1.3: with `Filter.db` deleted (temp copy) an absent key is `IndexMiss`, never unmeasured.
#[tokio::test]
async fn absent_filter_still_yields_index_miss() {
    let case = &CASES[0];
    let Some(dir) = resolve(case) else { return };
    let schema = load_schema(case.schema, case.table);
    let tmp = tempfile::tempdir().expect("tempdir");
    for e in std::fs::read_dir(&dir).expect("read dir") {
        let p = e.expect("entry").path();
        if p.is_file() && !p.to_string_lossy().ends_with("-Filter.db") {
            std::fs::copy(&p, tmp.path().join(p.file_name().expect("name"))).expect("copy");
        }
    }
    let gens = data_dbs(tmp.path());
    assert!(!gens.is_empty());
    let key = encode_key(
        &schema,
        &[Json::String(uuid::Uuid::from_u128(0xBEEF).to_string())],
    );
    let reader = open(&gens[0]).await;
    let probe = reader
        .probe_generation(&key, Some(&schema), &ScanCancel::new())
        .await
        .expect("probe");
    assert_eq!(probe, GenerationProbe::IndexMiss);
}
