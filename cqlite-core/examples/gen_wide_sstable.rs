/// Generate a wide-partition SSTable at `/tmp/cqlite-sstabledump-test/` for
/// real Cassandra sstabledump validation of the promoted-index fix (#752).
///
/// Run:
/// ```bash
/// cargo run -p cqlite-core --example gen_wide_sstable --features write-support
/// ```

#[cfg(feature = "write-support")]
#[tokio::main]
async fn main() -> cqlite_core::error::Result<()> {
    use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
    use cqlite_core::storage::write_engine::{
        CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine,
        WriteEngineConfig,
    };
    use cqlite_core::types::Value;
    use std::collections::HashMap;

    let out = std::path::PathBuf::from("/tmp/cqlite-sstabledump-test");
    if out.exists() {
        std::fs::remove_dir_all(&out).ok();
    }

    let schema = TableSchema {
        keyspace: "test_roundtrip".to_string(),
        table: "wide_check".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "text".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
                data_type: "text".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "data".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let config = WriteEngineConfig::new(out.join("data"), out.join("wal"), schema.clone());
    let mut engine = WriteEngine::new(config)?;

    // Write 1000 rows under a single partition key (pk=42) to produce a wide partition
    // (>128 KiB → at least 2 index blocks).
    let padding: String = "x".repeat(190);
    for i in 0..1000usize {
        let ck = format!("ck_{:06}", i);
        let data = format!("data_{}_{}", i, padding);
        let mutation = Mutation {
            table: TableId {
                keyspace: "test_roundtrip".to_string(),
                table: "wide_check".to_string(),
            },
            partition_key: PartitionKey::single("pk", Value::Integer(42)),
            clustering_key: Some(ClusteringKey::single("ck", Value::Text(ck))),
            operations: vec![CellOperation::Write {
                column: "data".to_string(),
                value: Value::Text(data),
            }],
            timestamp_micros: 1_000_000 + i as i64,
            ttl_seconds: None,
            partition_tombstone: None,
            range_tombstones: vec![],
            local_deletion_time: None,
            row_tombstone: None,
            cell_write_timestamps: None,
        };
        engine.write_async(mutation).await?;
    }

    let info = engine
        .flush()
        .await?
        .expect("flush must return SSTableInfo");

    println!("SSTable written to:");
    println!("  Data.db:  {}", info.data_path.display());
    println!(
        "  Index.db: {}",
        info.index_path.as_ref().unwrap().display()
    );
    println!("  Dir:      {}", info.data_path.parent().unwrap().display());

    // Verify that promoted index was emitted
    let index_bytes = std::fs::read(info.index_path.as_ref().unwrap())?;
    let key_len = u16::from_be_bytes([index_bytes[0], index_bytes[1]]) as usize;
    let mut pos = 2 + key_len;
    // skip data_offset vint
    while index_bytes[pos] >= 0x80 {
        pos += 1;
    }
    pos += 1;
    // read promoted_len vint
    let promoted_len_byte = index_bytes[pos];
    println!(
        "  promoted_index_size first byte: 0x{:02X} ({}zero)",
        promoted_len_byte,
        if promoted_len_byte > 0 { "non-" } else { "" }
    );

    if promoted_len_byte == 0 {
        eprintln!("ERROR: promoted_index_size is 0 — wide partition did not trigger index blocks!");
        std::process::exit(1);
    }
    println!("  promoted index present: YES");
    Ok(())
}

#[cfg(not(feature = "write-support"))]
fn main() {
    eprintln!("Run with --features write-support");
}
