//! Shared helpers for the issue #4199 (`extract`/`split`) test lanes:
//! resolving the committed `test_basic.composite_key_table` fixture's
//! schema, and a full compaction-row decode used as the physical-dump
//! oracle for a single-generation source (the same decode-equality oracle
//! `issue_4196_salvage_partition_atomicity.rs` uses).
#![allow(dead_code)]

use std::path::{Path, PathBuf};

#[path = "datasets_root.rs"]
pub mod datasets_root;

pub const KS: &str = "test_basic";
pub const TABLE: &str = "composite_key_table";
pub const SCHEMA_FILE: &str = "basic-types.cql";

pub fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

pub fn table_schema() -> cqlite_core::schema::TableSchema {
    let schema_path = datasets_root::schema_path(SCHEMA_FILE).expect("committed CQL schema");
    let cql = std::fs::read_to_string(schema_path).expect("read schema");
    let start = cql
        .find(&format!("CREATE TABLE IF NOT EXISTS {TABLE}"))
        .expect("CREATE TABLE statement");
    let end = start + cql[start..].find(';').expect("statement terminator") + 1;
    let mut t = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end])
        .expect("parse CREATE TABLE");
    t.keyspace = KS.to_string();
    t
}

pub async fn decode_all_rows(
    data_db: &Path,
    schema: &cqlite_core::schema::TableSchema,
) -> Vec<cqlite_core::storage::sstable::reader::CompactionRow> {
    use cqlite_core::platform::Platform;
    use cqlite_core::storage::sstable::SSTableReader;
    use std::sync::Arc;

    let config = cqlite_core::Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let reader = SSTableReader::open(data_db, &config, platform)
        .await
        .expect("open reader");
    reader
        .iterate_all_partitions_for_compaction(Some(schema))
        .await
        .expect("decode all partitions")
}

pub fn single_data_db(dir: &Path) -> PathBuf {
    let mut found = Vec::new();
    for e in std::fs::read_dir(dir).expect("read dir").flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if name.ends_with("-Data.db") {
            found.push(e.path());
        }
    }
    match found.len() {
        1 => found.pop().unwrap(),
        n => panic!("{dir:?}: expected exactly ONE Data.db, found {n} ({found:?})"),
    }
}

/// A distinct sha256 per top-level component file in `dir` (for R5.1's
/// before/after input-immutability comparison).
pub fn component_hashes(dir: &Path) -> std::collections::BTreeMap<String, String> {
    use sha2::{Digest, Sha256};
    let mut out = std::collections::BTreeMap::new();
    for e in std::fs::read_dir(dir).expect("read dir").flatten() {
        let path = e.path();
        if !path.is_file() {
            continue;
        }
        let bytes = std::fs::read(&path).expect("read component");
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let digest = hex::encode(hasher.finalize());
        out.insert(path.file_name().unwrap().to_string_lossy().to_string(), digest);
    }
    out
}
