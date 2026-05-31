//! Example: construct a `Mutation` and write it via `WriteEngine::write`.
//!
//! This is the minimal end-to-end write path a downstream consumer needs. It is
//! kept as a compiling example (rather than a doc snippet) so it can't drift from
//! the real API — see `docs/using-cqlite-core-as-a-dependency.md`.
//!
//! `write-support` is a default feature as of #558. On a release that predates
//! that, enable it explicitly:
//!
//! ```bash
//! cargo run -p cqlite-core --example write_a_mutation --features write-support
//! ```

#[cfg(feature = "write-support")]
fn main() -> cqlite_core::error::Result<()> {
    use cqlite_core::schema::{Column, KeyColumn, TableSchema};
    use cqlite_core::storage::write_engine::{
        CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
    };
    use cqlite_core::types::Value;
    use std::collections::HashMap;

    // 1. Describe the target table. `is_static` is `#[serde(default)]` (false) when
    //    a schema is deserialized from JSON/CQL; in a Rust literal you set it.
    let schema = TableSchema {
        keyspace: "demo".to_string(),
        table: "users".to_string(),
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
    };

    // 2. Create the write engine. `data_dir` holds flushed SSTables; `wal_dir`
    //    holds the write-ahead log.
    let base = std::env::temp_dir().join("cqlite-write-a-mutation");
    let config = WriteEngineConfig::new(base.join("data"), base.join("wal"), schema);
    let mut engine = WriteEngine::new(config)?;

    // 3. Build a mutation equivalent to:
    //    INSERT INTO demo.users (id, name) VALUES (1, 'Alice')
    //    Note: the cell op is `Write` — there is no `Insert` variant.
    let mutation = Mutation::new(
        TableId::new("demo", "users"),
        PartitionKey::single("id", Value::Integer(1)),
        None, // no clustering key on this table
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text("Alice".to_string()),
        }],
        1_704_067_200_000_000, // write timestamp, microseconds since Unix epoch
        None,                  // no TTL
    );

    // 4. Write it. `write` is SYNCHRONOUS, takes `&mut self`, and fsyncs the WAL on
    //    every call (durability-first). Concurrent writers must serialize — see the
    //    concurrency model section in the dependency doc.
    engine.write(mutation)?;

    // 5. `flush` and `close` are async; flush persists the memtable to an SSTable.
    let rt = tokio::runtime::Runtime::new().expect("create tokio runtime");
    rt.block_on(async {
        match engine.flush().await? {
            Some(_info) => println!("Flushed memtable to a new SSTable in {base:?}"),
            None => println!("Nothing to flush (memtable was empty)"),
        }
        engine.close().await
    })?;

    Ok(())
}

#[cfg(not(feature = "write-support"))]
fn main() {
    eprintln!("This example requires the `write-support` feature.");
    eprintln!(
        "Run with: cargo run -p cqlite-core --example write_a_mutation --features write-support"
    );
    std::process::exit(1);
}
