//! Moved out of `merge/mod.rs` unchanged (#4658); the module keeps its name via `#[path]`.

use super::*;
use crate::schema::{Column, CqlType, KeyColumn, UdtRegistry};
use crate::storage::write_engine::mutation::{CellOperation, Mutation, PartitionKey, TableId};
use crate::types::{UdtField, UdtTypeDef, UdtValue, Value};

fn person_registry() -> UdtRegistry {
    let mut reg = UdtRegistry::new();
    reg.register_udt(
        UdtTypeDef::new("test_ks".to_string(), "person".to_string())
            .with_field("name".to_string(), CqlType::Text, true)
            .with_field("age".to_string(), CqlType::Int, true),
    );
    reg
}

// Schema whose UDT column is declared with the BARE name `person`.
fn bare_udt_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
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
                name: "addr".to_string(),
                data_type: "person".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    }
}

const PERSON_USERTYPE: &str = "org.apache.cassandra.db.marshal.UserType(test_ks,706572736f6e,6e616d65:org.apache.cassandra.db.marshal.UTF8Type,616765:org.apache.cassandra.db.marshal.Int32Type)";

fn header_has(stats_path: &std::path::Path, needle: &str) -> bool {
    let bytes = std::fs::read(stats_path).expect("read Statistics.db");
    bytes.windows(needle.len()).any(|w| w == needle.as_bytes())
}

#[tokio::test]
async fn bare_udt_column_survives_compaction_with_registry() {
    let schema = bare_udt_schema();
    let registry = person_registry();

    // 1. Flush an input SSTable WITH the registry: the column is normalized to
    //    UserType(...) and written as complex per-field cells.
    let in_dir = tempfile::TempDir::new().expect("in dir");
    let mut writer =
        crate::storage::sstable::writer::SSTableWriter::with_expected_partitions_and_registry(
            in_dir.path().to_path_buf(),
            1,
            &schema,
            1,
            Some(&registry),
        )
        .expect("input writer");
    let udt = Value::Udt(Box::new(UdtValue {
        type_name: "person".to_string(),
        keyspace: "test_ks".to_string(),
        fields: vec![
            UdtField {
                name: "name".to_string(),
                value: Some(Value::text("Alice".to_string())),
            },
            UdtField {
                name: "age".to_string(),
                value: Some(Value::Integer(30)),
            },
        ],
    }));
    let mutation = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "addr".to_string(),
            value: udt,
        }],
        1_000_000,
        None,
    );
    let key = mutation.decorated_key(&schema).expect("decorated key");
    writer
        .write_partition(key, vec![mutation])
        .expect("write partition");
    let input = writer.finish().await.expect("finish input");
    assert!(
        header_has(&input.stats_path, PERSON_USERTYPE),
        "input header must advertise the UDT column as UserType"
    );

    // 2. Compact WITH the registry, passing the BARE-name schema (as the engine
    //    does). The output must re-apply normalization, not degrade to BytesType.
    let out_dir = tempfile::TempDir::new().expect("out dir");
    let report = compact_sstables(
        vec![input.data_path.clone()],
        out_dir.path(),
        &schema,
        2,
        None,
        None,
        true,
    )
    .await
    .expect("compaction");

    assert!(
        header_has(&report.output.stats_path, PERSON_USERTYPE),
        "compaction output header must keep the UDT column as UserType (not degrade to BytesType)"
    );

    // 3. Read the compacted output back through the compaction path and assert
    //    the per-field UDT cells SURVIVED as a complex column with the original
    //    field values (not collapsed/dropped). The reader keys complex-vs-simple
    //    off the schema's data_type, so read with the normalized (UserType) form.
    let mut read_schema = schema.clone();
    crate::storage::sstable::writer::data_writer::normalize_schema_udts(
        &mut read_schema,
        &registry,
    );
    let config = crate::Config::default();
    let platform = std::sync::Arc::new(
        crate::platform::Platform::new(&config)
            .await
            .expect("platform"),
    );
    let reader = crate::storage::sstable::reader::SSTableReader::open(
        &report.output.data_path,
        &config,
        platform,
    )
    .await
    .expect("open compacted output");
    let rows = reader
        .iterate_all_partitions_for_compaction(Some(&read_schema))
        .await
        .expect("compaction iterator");

    let mut saw_addr = false;
    for row in &rows {
        if let crate::storage::sstable::reader::compaction_row::CompactionRowData::Live {
            complex,
            ..
        } = &row.row_data
        {
            if let Some(addr) = complex.iter().find(|c| c.column == "addr") {
                saw_addr = true;
                // Two per-field cells survived: name (idx 0) and age (idx 1).
                // The compaction read path surfaces per-element UDT values
                // byte-faithfully, so `age` may arrive typed (Integer) or as
                // its raw 4-byte big-endian form — both mean the value
                // survived rather than being dropped/collapsed.
                assert_eq!(
                    addr.elements.len(),
                    2,
                    "both UDT field cells must survive, got: {:?}",
                    addr.elements
                );
                let values: Vec<Option<Value>> =
                    addr.elements.iter().map(|e| e.value.clone()).collect();
                let name_ok = values.iter().any(|v| {
                    matches!(v, Some(Value::Text(s)) if s == "Alice")
                        || matches!(v, Some(Value::Blob(b)) if b.as_ref() == b"Alice")
                });
                assert!(
                    name_ok,
                    "UDT field `name` value must survive compaction, got: {values:?}"
                );
                let age_ok = values.iter().any(|v| {
                    matches!(v, Some(Value::Integer(30)))
                        || matches!(v, Some(Value::Blob(b)) if b.as_ref() == 30i32.to_be_bytes())
                });
                assert!(
                    age_ok,
                    "UDT field `age` value must survive compaction, got: {values:?}"
                );
            }
        }
    }
    assert!(
        saw_addr,
        "compacted output must contain the `addr` UDT as a COMPLEX column (per-field cells \
             survived rather than being dropped/collapsed)"
    );
}

/// #1013 safety: an input written WITHOUT a registry stores the bare UDT
/// column as a single simple `BytesType` cell. Compacting it WITH a registry
/// must NOT normalize/upgrade that column (the header gate sees it is not a
/// UserType in the input), so the reader does not misdecode the simple cell.
/// The output keeps the column simple (header has no UserType for it).
#[tokio::test]
async fn simple_cell_input_not_upgraded_on_compaction() {
    let schema = bare_udt_schema();

    // Input written WITHOUT a registry: `addr` is a single simple cell whose
    // header type is NOT a UserType marshal.
    let in_dir = tempfile::TempDir::new().expect("in dir");
    let mut writer = crate::storage::sstable::writer::SSTableWriter::with_expected_partitions(
        in_dir.path().to_path_buf(),
        1,
        &schema,
        1,
    )
    .expect("input writer");
    let udt = Value::Udt(Box::new(UdtValue {
        type_name: "person".to_string(),
        keyspace: "test_ks".to_string(),
        fields: vec![UdtField {
            name: "name".to_string(),
            value: Some(Value::text("Bob".to_string())),
        }],
    }));
    let mutation = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(2)),
        None,
        vec![CellOperation::Write {
            column: "addr".to_string(),
            value: udt,
        }],
        1_000_000,
        None,
    );
    let key = mutation.decorated_key(&schema).expect("decorated key");
    writer
        .write_partition(key, vec![mutation])
        .expect("write partition");
    let input = writer.finish().await.expect("finish input");
    assert!(
        !header_has(&input.stats_path, "UserType("),
        "input written without registry must store the column as a simple (non-UserType) cell"
    );

    // The header gate must report NO complex UDT columns for this input.
    let plan = udt_columns_eligible_for_normalization(std::slice::from_ref(&input.data_path));
    assert!(
        plan.eligible_marshals.is_empty() && plan.conflicts.is_empty(),
        "a simple-cell input must not be reported as eligible or conflicting"
    );

    // Compact WITH the registry: the gate prevents normalization, so the
    // output keeps the column simple (no spurious UserType upgrade, no
    // misdecode panic).
    let out_dir = tempfile::TempDir::new().expect("out dir");
    let report = compact_sstables(
        vec![input.data_path.clone()],
        out_dir.path(),
        &schema,
        2,
        None,
        None,
        true,
    )
    .await
    .expect("compaction must not panic on a simple-cell input");
    assert!(
        !header_has(&report.output.stats_path, "UserType("),
        "compaction must not upgrade a simple-cell input's column to UserType"
    );
}

/// #1015 schema evolution: an older input that simply LACKS the UDT column
/// (absent from its header) must NOT veto normalization. Compacting it with a
/// newer input that DOES carry the complex UDT must still normalize the
/// column so the newer input's complex cells survive.
#[tokio::test]
async fn absent_column_in_older_input_does_not_veto_normalization() {
    let registry = person_registry();
    let full_schema = bare_udt_schema();

    // Input A (newer): complex `addr` written WITH the registry.
    let a_dir = tempfile::TempDir::new().expect("a dir");
    let mut wa =
        crate::storage::sstable::writer::SSTableWriter::with_expected_partitions_and_registry(
            a_dir.path().to_path_buf(),
            1,
            &full_schema,
            1,
            Some(&registry),
        )
        .expect("writer a");
    let udt = Value::Udt(Box::new(UdtValue {
        type_name: "person".to_string(),
        keyspace: "test_ks".to_string(),
        fields: vec![UdtField {
            name: "name".to_string(),
            value: Some(Value::text("Carol".to_string())),
        }],
    }));
    let ma = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "addr".to_string(),
            value: udt,
        }],
        1_000_000,
        None,
    );
    let ka = ma.decorated_key(&full_schema).expect("key a");
    wa.write_partition(ka, vec![ma]).expect("write a");
    let input_a = wa.finish().await.expect("finish a");

    // Input B (older): a schema that LACKS `addr` entirely (has a `note`
    // column instead). Its header never declares `addr`.
    let older_schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
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
                name: "note".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    };
    let b_dir = tempfile::TempDir::new().expect("b dir");
    let mut wb = crate::storage::sstable::writer::SSTableWriter::with_expected_partitions(
        b_dir.path().to_path_buf(),
        1,
        &older_schema,
        1,
    )
    .expect("writer b");
    let mb = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(2)),
        None,
        vec![CellOperation::Write {
            column: "note".to_string(),
            value: Value::text("x".to_string()),
        }],
        1_000_000,
        None,
    );
    let kb = mb.decorated_key(&older_schema).expect("key b");
    wb.write_partition(kb, vec![mb]).expect("write b");
    let input_b = wb.finish().await.expect("finish b");

    // `addr` is eligible: declared UserType in A, absent (not simple) in B.
    let plan = udt_columns_eligible_for_normalization(&[
        input_a.data_path.clone(),
        input_b.data_path.clone(),
    ]);
    assert!(
        plan.eligible_marshals.contains_key("addr") && plan.conflicts.is_empty(),
        "a column absent from an older input must remain eligible, got: {plan:?}"
    );

    // Compact both with the registry: `addr` must be normalized so A's complex
    // cells survive into the output (header advertises UserType).
    let out_dir = tempfile::TempDir::new().expect("out dir");
    let report = compact_sstables(
        vec![input_a.data_path.clone(), input_b.data_path.clone()],
        out_dir.path(),
        &full_schema,
        2,
        None,
        None,
        true,
    )
    .await
    .expect("compaction");
    assert!(
        header_has(&report.output.stats_path, PERSON_USERTYPE),
        "newer input's complex UDT column must survive compaction with an older input \
             that lacks the column"
    );
}

/// #1017 mixed encoding: the SAME column stored as complex `UserType` in one
/// input and a simple cell in another cannot be represented by a single
/// decode schema. Compaction must FAIL (not silently drop/corrupt the complex
/// values) so the operator can rewrite the simple-cell SSTable first.
#[tokio::test]
async fn mixed_encoding_inputs_fail_compaction() {
    let registry = person_registry();
    let schema = bare_udt_schema();

    // Input A: complex `addr` (written WITH the registry).
    let a_dir = tempfile::TempDir::new().expect("a dir");
    let mut wa =
        crate::storage::sstable::writer::SSTableWriter::with_expected_partitions_and_registry(
            a_dir.path().to_path_buf(),
            1,
            &schema,
            1,
            Some(&registry),
        )
        .expect("writer a");
    let udt = Value::Udt(Box::new(UdtValue {
        type_name: "person".to_string(),
        keyspace: "test_ks".to_string(),
        fields: vec![UdtField {
            name: "name".to_string(),
            value: Some(Value::text("Dave".to_string())),
        }],
    }));
    let ma = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "addr".to_string(),
            value: udt,
        }],
        1_000_000,
        None,
    );
    let ka = ma.decorated_key(&schema).expect("key a");
    wa.write_partition(ka, vec![ma]).expect("write a");
    let input_a = wa.finish().await.expect("finish a");

    // Input B: same `addr` column as a SIMPLE cell (written WITHOUT registry).
    let b_dir = tempfile::TempDir::new().expect("b dir");
    let mut wb = crate::storage::sstable::writer::SSTableWriter::with_expected_partitions(
        b_dir.path().to_path_buf(),
        1,
        &schema,
        1,
    )
    .expect("writer b");
    let udt_b = Value::Udt(Box::new(UdtValue {
        type_name: "person".to_string(),
        keyspace: "test_ks".to_string(),
        fields: vec![UdtField {
            name: "name".to_string(),
            value: Some(Value::text("Eve".to_string())),
        }],
    }));
    let mb = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(2)),
        None,
        vec![CellOperation::Write {
            column: "addr".to_string(),
            value: udt_b,
        }],
        1_000_000,
        None,
    );
    let kb = mb.decorated_key(&schema).expect("key b");
    wb.write_partition(kb, vec![mb]).expect("write b");
    let input_b = wb.finish().await.expect("finish b");

    // The plan flags `addr` as a conflict (UserType in A, simple in B).
    let plan = udt_columns_eligible_for_normalization(&[
        input_a.data_path.clone(),
        input_b.data_path.clone(),
    ]);
    assert!(
        plan.conflicts.contains("addr"),
        "mixed-encoding column must be reported as a conflict, got: {plan:?}"
    );

    // Compaction must refuse rather than corrupt.
    let out_dir = tempfile::TempDir::new().expect("out dir");
    let err = compact_sstables(
        vec![input_a.data_path.clone(), input_b.data_path.clone()],
        out_dir.path(),
        &schema,
        2,
        None,
        None,
        true,
    )
    .await
    .expect_err("mixed-encoding compaction must fail");
    let msg = format!("{err}");
    assert!(
        msg.contains("disagree on the encoding") && msg.contains("addr"),
        "error must explain the mixed-encoding conflict, got: {msg}"
    );
}

/// #1019 nested UDT: compaction copies the EXACT `UserType(...)` marshal from
/// the input header (which may contain a nested `UserType` the flush-time
/// renderer intentionally skips). A column whose on-disk marshal nests
/// another UserType must survive compaction byte-exact (header preserved) and
/// be decoded as complex, never dropped.
#[tokio::test]
async fn nested_usertype_column_survives_compaction() {
    // outer { i: inner { x: int } } — a UDT whose field is itself a UDT.
    const INNER_MARSHAL: &str =
            "org.apache.cassandra.db.marshal.UserType(test_ks,696e6e6572,78:org.apache.cassandra.db.marshal.Int32Type)";
    // outer(test_ks, hex "outer", hex "i" : <inner marshal>)
    let outer_marshal =
        format!("org.apache.cassandra.db.marshal.UserType(test_ks,6f75746572,69:{INNER_MARSHAL})");

    // Schema whose `addr` column already carries the full nested marshal, so
    // the writer treats it as complex without needing the registry to render
    // it (which it could not — nested UDTs are skipped at flush).
    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
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
                name: "addr".to_string(),
                data_type: outer_marshal.clone(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    };

    let in_dir = tempfile::TempDir::new().expect("in dir");
    let mut writer = crate::storage::sstable::writer::SSTableWriter::new(
        in_dir.path().to_path_buf(),
        1,
        &schema,
    )
    .expect("input writer");
    // `addr` = outer{ i: inner{ x: 5 } }.
    let addr = Value::Udt(Box::new(UdtValue {
        type_name: "outer".to_string(),
        keyspace: "test_ks".to_string(),
        fields: vec![UdtField {
            name: "i".to_string(),
            value: Some(Value::Udt(Box::new(UdtValue {
                type_name: "inner".to_string(),
                keyspace: "test_ks".to_string(),
                fields: vec![UdtField {
                    name: "x".to_string(),
                    value: Some(Value::Integer(5)),
                }],
            }))),
        }],
    }));
    let mutation = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "addr".to_string(),
            value: addr,
        }],
        1_000_000,
        None,
    );
    let key = mutation.decorated_key(&schema).expect("decorated key");
    writer.write_partition(key, vec![mutation]).expect("write");
    let input = writer.finish().await.expect("finish");
    assert!(
        header_has(&input.stats_path, &outer_marshal),
        "input header must carry the exact nested UserType marshal"
    );

    // The plan copies the nested marshal verbatim (NOT a registry re-render).
    let plan = udt_columns_eligible_for_normalization(std::slice::from_ref(&input.data_path));
    assert_eq!(
        plan.eligible_marshals.get("addr").map(String::as_str),
        Some(outer_marshal.as_str()),
        "compaction must copy the exact nested marshal from the input header"
    );

    // Compaction (header-driven, no registry needed) must preserve the nested
    // marshal byte-exact in the output header (not degraded to BytesType).
    let out_dir = tempfile::TempDir::new().expect("out dir");
    let report = compact_sstables(
        vec![input.data_path.clone()],
        out_dir.path(),
        &schema,
        2,
        None,
        None,
        true,
    )
    .await
    .expect("compaction");
    assert!(
        header_has(&report.output.stats_path, &outer_marshal),
        "nested UserType column must survive compaction byte-exact"
    );
}

/// #1023 schema evolution: a bare UDT column ADDED after the inputs were
/// written is absent from every input header, so the header gate leaves it
/// bare. The engine's configured registry must normalize it (it has no input
/// cells to misdecode) so compaction output is not emitted as BytesType.
#[tokio::test]
async fn newly_added_bare_udt_column_normalized_via_registry() {
    let registry = person_registry();

    // Input written with an OLDER schema lacking `addr` (only id + note).
    let older_schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
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
                name: "note".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    };
    let in_dir = tempfile::TempDir::new().expect("in dir");
    let mut writer = crate::storage::sstable::writer::SSTableWriter::with_expected_partitions(
        in_dir.path().to_path_buf(),
        1,
        &older_schema,
        1,
    )
    .expect("writer");
    let m = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "note".to_string(),
            value: Value::text("x".to_string()),
        }],
        1_000_000,
        None,
    );
    let k = m.decorated_key(&older_schema).expect("key");
    writer.write_partition(k, vec![m]).expect("write");
    let input = writer.finish().await.expect("finish");

    // The NEW schema adds the bare UDT column `addr` (absent from the input).
    let mut new_schema = bare_udt_schema();
    // `addr` is absent from the input header, so it is registry-normalized.
    crate::storage::write_engine::merge::apply_udt_marshals_from_inputs(
        &mut new_schema,
        std::slice::from_ref(&input.data_path),
        Some(&registry),
    )
    .expect("apply");
    let addr = new_schema
        .columns
        .iter()
        .find(|c| c.name == "addr")
        .expect("addr column");
    assert_eq!(
        addr.data_type, PERSON_USERTYPE,
        "a UDT column absent from all inputs must be registry-normalized, not left bare"
    );

    // Without a registry the column stays bare (free-fn / no-registry path).
    let mut bare_again = bare_udt_schema();
    crate::storage::write_engine::merge::apply_udt_marshals_from_inputs(
        &mut bare_again,
        std::slice::from_ref(&input.data_path),
        None,
    )
    .expect("apply no-registry");
    assert_eq!(
        bare_again
            .columns
            .iter()
            .find(|c| c.name == "addr")
            .unwrap()
            .data_type,
        "person",
        "without a registry an absent column stays bare"
    );
}

/// #1025 unknown header state: if an input header cannot be read/parsed, the
/// column's true encoding is unknown. The absent-column registry fallback
/// MUST NOT fire (it could misdecode an unreadable simple-cell input), so the
/// bare column is left bare.
#[tokio::test]
async fn unreadable_header_does_not_trigger_registry_normalization() {
    let registry = person_registry();

    // A path whose sibling Statistics.db does not exist -> header unverified.
    let dir = tempfile::TempDir::new().expect("dir");
    let bogus = dir.path().join("nb-1-big-Data.db");

    let plan = udt_columns_eligible_for_normalization(std::slice::from_ref(&bogus));
    assert!(
        !plan.headers_verified,
        "an unreadable header must leave the plan unverified"
    );

    let mut schema = bare_udt_schema();
    crate::storage::write_engine::merge::apply_udt_marshals_from_inputs(
        &mut schema,
        &[bogus],
        Some(&registry),
    )
    .expect("apply");
    assert_eq!(
        schema
            .columns
            .iter()
            .find(|c| c.name == "addr")
            .unwrap()
            .data_type,
        "person",
        "with unverified headers, a bare UDT column must NOT be registry-normalized"
    );
}
