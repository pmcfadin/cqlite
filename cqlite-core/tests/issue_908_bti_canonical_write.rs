//! Integration tests for issue #908 (epic #872): Cassandra-canonical BTI write.
//!
//! These prove the BTI writer emits a true `da`-format BTI component set instead
//! of the phase-1 `nb-*-big-*` hybrid:
//!
//! 1. Component filenames use the `da` version letter and `bti` format segment
//!    (`da-<gen>-bti-<Component>`), parsed back via `SsTableDescriptor`.
//! 2. A BTI SSTable has `Data.db` + `Partitions.db` and a TOC, but **no**
//!    `Index.db` and **no** `Summary.db`.
//! 3. `TOC.txt` lists exactly the BTI component set (Data, Partitions, Filter,
//!    Statistics, Digest, TOC) — no Index/Summary, no Rows yet (#910) — and
//!    self-references TOC.txt.
//! 4. The default BIG writer is unchanged (still `nb-*-big-*` with Index/Summary).
//!
//! All tests require the `write-support` feature.

#![cfg(feature = "write-support")]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::version_gate::{SsTableDescriptor, SsTableFormat};
use cqlite_core::storage::sstable::writer::{SSTableFormat, SSTableWriter};
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, Mutation, PartitionKey, TableId,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use std::path::Path;
use tempfile::TempDir;

fn int_pk_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
    }
}

fn int_mutation(id: i32, name: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new("test_ks", "t"),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        }],
        ts,
        None,
    )
}

/// List the component filenames present in the table directory.
fn component_filenames(table_dir: &Path) -> Vec<String> {
    std::fs::read_dir(table_dir)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect()
}

async fn write_bti(dir: &Path, gen: u64) -> cqlite_core::storage::sstable::writer::SSTableInfo {
    let schema = int_pk_schema();
    let mut writer =
        SSTableWriter::with_format(dir.to_path_buf(), gen, &schema, 16, SSTableFormat::Bti)
            .unwrap();

    let mut keyed: Vec<_> = (0..6)
        .map(|i| {
            let m = int_mutation(i, &format!("name{i}"), 1_000_000 + i as i64);
            let key = m.decorated_key(&schema).unwrap();
            (key, m)
        })
        .collect();
    keyed.sort_by_key(|(k, _)| k.token);
    for (key, m) in keyed {
        writer.write_partition(key, vec![m]).unwrap();
    }
    writer.finish().await.unwrap()
}

/// AC#1 + AC#2: every emitted BTI component uses the `da-<gen>-bti-<Component>`
/// descriptor and the directory contains Data.db + Partitions.db but neither
/// Index.db nor Summary.db.
#[tokio::test]
async fn bti_components_use_da_bti_descriptor_and_omit_big_index() {
    let dir = TempDir::new().unwrap();
    let info = write_bti(dir.path(), 1).await;

    let table_dir = dir.path().join("test_ks").join("t");
    let names = component_filenames(&table_dir);
    assert!(!names.is_empty(), "BTI SSTable should produce components");

    // Every component parses as a `da`/`bti` descriptor (version letter + format
    // segment in the correct order per SsTableDescriptor::parse).
    for name in &names {
        let desc = SsTableDescriptor::parse_filename(name)
            .unwrap_or_else(|e| panic!("component {name:?} is not a valid descriptor: {e}"));
        assert_eq!(
            desc.version, "da",
            "component {name:?} must use `da` version"
        );
        assert_eq!(
            desc.format,
            SsTableFormat::Bti,
            "component {name:?} must use `bti` format segment"
        );
        assert_eq!(desc.sstable_id, "1", "generation must be the id segment");
        assert!(
            name.starts_with("da-1-bti-"),
            "component {name:?} must start with da-1-bti-"
        );
    }

    // Required BTI components exist with the canonical names.
    assert!(names.iter().any(|n| n == "da-1-bti-Data.db"));
    assert!(names.iter().any(|n| n == "da-1-bti-Partitions.db"));
    assert!(names.iter().any(|n| n == "da-1-bti-Filter.db"));
    assert!(names.iter().any(|n| n == "da-1-bti-Statistics.db"));
    assert!(names.iter().any(|n| n == "da-1-bti-Digest.crc32"));
    assert!(names.iter().any(|n| n == "da-1-bti-TOC.txt"));

    // No BIG-only components, and no Rows.db (deferred to #910).
    assert!(
        !names.iter().any(|n| n.contains("Index.db")),
        "BTI must not emit Index.db, got {names:?}"
    );
    assert!(
        !names.iter().any(|n| n.contains("Summary.db")),
        "BTI must not emit Summary.db, got {names:?}"
    );
    assert!(
        !names.iter().any(|n| n.contains("Rows.db")),
        "Rows.db is deferred to #910, got {names:?}"
    );

    // SSTableInfo reflects the omission.
    assert!(
        info.index_path.is_none(),
        "BTI SSTableInfo.index_path must be None"
    );
    assert!(
        info.summary_path.is_none(),
        "BTI SSTableInfo.summary_path must be None"
    );
    assert!(
        info.partitions_path.is_some(),
        "BTI SSTableInfo.partitions_path must be Some"
    );
    // Reported paths use the canonical descriptor.
    assert_eq!(
        info.data_path.file_name().unwrap().to_str().unwrap(),
        "da-1-bti-Data.db"
    );
    assert_eq!(
        info.toc_path.file_name().unwrap().to_str().unwrap(),
        "da-1-bti-TOC.txt"
    );
}

/// AC#3: TOC.txt lists exactly the BTI component set and self-references TOC.txt.
#[tokio::test]
async fn bti_toc_lists_exact_component_set() {
    let dir = TempDir::new().unwrap();
    let info = write_bti(dir.path(), 7).await;

    let toc = std::fs::read_to_string(&info.toc_path).unwrap();
    let listed: std::collections::BTreeSet<&str> = toc
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .collect();

    let expected: std::collections::BTreeSet<&str> = [
        "Data.db",
        "Partitions.db",
        "Filter.db",
        "Statistics.db",
        "Digest.crc32",
        "TOC.txt",
    ]
    .into_iter()
    .collect();

    assert_eq!(
        listed, expected,
        "BTI TOC must list exactly the canonical component set (no Index/Summary/Rows)"
    );
    // Explicit self-reference + explicit exclusions.
    assert!(toc.contains("TOC.txt"), "TOC must self-reference");
    assert!(!toc.contains("Index.db"), "TOC must not list Index.db");
    assert!(!toc.contains("Summary.db"), "TOC must not list Summary.db");
    assert!(!toc.contains("Rows.db"), "TOC must not list Rows.db (#910)");
}

/// AC#4: the default BIG writer is unchanged — `nb-*-big-*` with Index/Summary,
/// no Partitions.db, and the TOC still lists Index/Summary.
#[tokio::test]
async fn big_default_format_unchanged() {
    let dir = TempDir::new().unwrap();
    let schema = int_pk_schema();

    let mut writer = SSTableWriter::new(dir.path().to_path_buf(), 1, &schema).unwrap();
    let m = int_mutation(1, "alice", 1_000_000);
    let key = m.decorated_key(&schema).unwrap();
    writer.write_partition(key, vec![m]).unwrap();
    let info = writer.finish().await.unwrap();

    let table_dir = dir.path().join("test_ks").join("t");
    let names = component_filenames(&table_dir);

    // BIG descriptor on every component.
    for name in &names {
        let desc = SsTableDescriptor::parse_filename(name).unwrap();
        assert_eq!(desc.version, "nb", "BIG component {name:?} must use `nb`");
        assert_eq!(desc.format, SsTableFormat::Big);
        assert!(name.starts_with("nb-1-big-"), "got {name:?}");
    }

    assert!(names.iter().any(|n| n == "nb-1-big-Index.db"));
    assert!(names.iter().any(|n| n == "nb-1-big-Summary.db"));
    assert!(
        !names.iter().any(|n| n.contains("Partitions.db")),
        "BIG must not emit Partitions.db"
    );

    assert!(info.index_path.is_some(), "BIG must report Index.db path");
    assert!(
        info.summary_path.is_some(),
        "BIG must report Summary.db path"
    );
    assert!(info.partitions_path.is_none());

    let toc = std::fs::read_to_string(&info.toc_path).unwrap();
    assert!(toc.contains("Index.db"));
    assert!(toc.contains("Summary.db"));
    assert!(!toc.contains("Partitions.db"));
}
