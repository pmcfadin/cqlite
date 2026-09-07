//! Issue #2339: the MERGED-READ path must decode a COMPOSITE collection
//! key/element — `map<frozen<tuple/udt>, V>`'s KEY and `set<frozen<…>>`'s ELEMENT
//! — into a typed value, not fail closed.
//!
//! The composite identity lives in each cell's authoritative `cell_path` (a set
//! cell's value is empty; sstabledump prints `"value":""`), so the assembler has
//! to decode those bytes with the declared type. Before this fix
//! `write_engine/merge/read_assembly.rs` returned `UnsupportedFormat`, which made
//! a correctness outcome flip on SSTable generation count: one generation took the
//! #3058 single-generation bypass arm and decoded, two took the merge arm and
//! errored the whole request.
//!
//! ## Fixture and oracle
//!
//! `test_nested_udt_keys.nested_udt_keys` (issue #3500) — the repo's only
//! composite-MAP-KEY corpus, force-committed under
//! `test-data/datasets/sstables/`, so it is present in a stock checkout and is
//! NOT in a fetched `/data/datasets`-style root. It is therefore resolved across
//! EVERY candidate root by `fixture_support::table_dir_by_prefix` (issue #3220) and
//! is asserted UNCONDITIONALLY — a committed fixture that cannot be found is a
//! hard failure, never a skip, so this can never pass by omission.
//!
//! Every expectation below is read off that fixture's `sstabledump` golden
//! (`nb-1-big-Data.db.jsonl`), i.e. CASSANDRA-WRITTEN bytes — never CQLite's own
//! output (#3042):
//!
//! ```text
//! id=1  m_tuple_udt  ["charlie\:3:8"] 80        ["delta\:4:9"] 90
//! id=2  m_tuple_udt  ["\@\:\@:0"] 1             ["\:0:0"] 2
//! id=3  m_tuple_udt  ["solo\:99:42"] 7
//! id=1  s_set_udt    [00000002 00000011 00000005 "alpha" 00000004 00000001
//!                              00000010 00000004 "beta"  00000004 00000002]
//!                    [00000001 00000011 00000005 "gamma" 00000004 00000003]
//! id=2  s_set_udt    [00000002 00000008 ffffffff ffffffff
//!                              00000011 00000009 "nullrank2" ffffffff]
//! id=3  s_set_udt    [00000001 00000010 00000004 "solo" 00000004 00000063]
//! ```
//!
//! (`\@` is sstabledump's NULL marker; `\:` separates the inner `key_part`
//! fields. The `s_set_udt` paths are printed as raw hex because the element is a
//! frozen COLLECTION, and they show the frozen framing this fix also needed:
//! `i32-BE count` + `i32-BE length`-prefixed elements per the pinned
//! `cassandra-5.0.8` `CollectionSerializer.pack`/`writeValue`, NOT the VInt
//! framing a non-frozen collection cell uses.)
//!
//! ## Why the ORDER assertions matter
//!
//! Cassandra writes a complex column's cells in `cellPathComparator()` order —
//! the declared element/key TYPE's comparator — and for a composite that is NOT
//! unsigned byte order of the serialized form. Two inversions visible above and
//! asserted here: a 2-element frozen set precedes a 1-element one although its
//! `i32-BE` count prefix would sort second, and a NULL component precedes both an
//! empty string and any value (`TupleType.compareCustom` returns `-1` for
//! `sizeL < 0`).
//!
//! ## Which ARM this exercises
//!
//! `MergeProducer::produce_from_paths` is the COLD path, which never consults
//! `bypass_reason` and therefore ALWAYS merges — so these assertions are about the
//! merged-read assembler, not the single-generation decoder (the arm the fixture's
//! one generation would otherwise take). The complementary arm-vs-arm comparison
//! lives in `issue_3058_forced_path_differential.rs`.

mod fixture_support;

use arrow::array::{Array, Int32Array, ListArray, MapArray, StringArray, StructArray};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;

use cqlite_core::schema::{parse_cql_schema, udt_registry_from_cql};
use cqlite_flight::filter::ScanSpec;
use cqlite_flight::producer::{DirSource, MergeProducer, SstableSource};

/// The ticket DDL a connector would send. `CREATE TABLE` must come FIRST (the
/// schema parser reads the table from the head of the statement list), and the
/// `CREATE TYPE` is what makes `key_part` resolvable — without it the composite
/// element/key stays a bare `Custom` with no field list and the path (correctly)
/// fails closed.
const DDL: &str = "\
CREATE TABLE nested_udt_keys (\
id int PRIMARY KEY, \
m_tuple_udt map<frozen<tuple<frozen<key_part>, int>>, int>, \
s_set_udt set<frozen<set<frozen<key_part>>>>); \
CREATE TYPE key_part (label text, rank int);";

/// `m_tuple_udt` per partition id, verbatim from the golden, in DISK order.
fn golden_m_tuple_udt() -> Vec<(i32, &'static str)> {
    vec![
        (1, "{(charlie/3,8):80, (delta/4,9):90}"),
        // NULL label sorts before the EMPTY-STRING one
        // (`TupleType.compareCustom`: `sizeL < 0` ⇒ -1).
        (2, "{(NULL/NULL,0):1, (\"\"/0,0):2}"),
        (3, "{(solo/99,42):7}"),
    ]
}

/// `s_set_udt` per partition id, verbatim from the golden, in DISK order.
fn golden_s_set_udt() -> Vec<(i32, &'static str)> {
    vec![
        // The 2-element frozen set precedes the 1-element one (alpha < gamma
        // component-wise), the REVERSE of its i32-BE count prefix.
        (1, "[[alpha/1, beta/2], [gamma/3]]"),
        (2, "[[NULL/NULL, nullrank2/NULL]]"),
        (3, "[[solo/99]]"),
    ]
}

fn producer() -> MergeProducer {
    let schema = parse_cql_schema(DDL).expect("ticket DDL parses");
    // The registry must be keyed by the keyspace the reassembler looks UDTs up
    // under. This producer is built directly (no ticket), so that keyspace is the
    // parsed schema's own.
    let keyspace = schema.keyspace.clone();
    MergeProducer::with_spec(schema, 64, ScanSpec::default())
        .expect("producer")
        .with_udt_registry(udt_registry_from_cql(DDL, &keyspace))
}

/// Render one `key_part` struct row as `label/rank`, with `NULL` for a null field
/// and `""` for an empty string — so a null and an empty label can never compare
/// equal in an expectation.
fn key_part(sa: &StructArray, i: usize) -> String {
    let label = sa
        .column_by_name("label")
        .expect("key_part.label")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("key_part.label is Utf8");
    let rank = sa
        .column_by_name("rank")
        .expect("key_part.rank")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("key_part.rank is Int32");
    let l = if label.is_null(i) {
        "NULL".to_string()
    } else if label.value(i).is_empty() {
        "\"\"".to_string()
    } else {
        label.value(i).to_string()
    };
    let r = if rank.is_null(i) {
        "NULL".to_string()
    } else {
        rank.value(i).to_string()
    };
    format!("{l}/{r}")
}

fn struct_field<'a>(sa: &'a StructArray, name: &str) -> &'a StructArray {
    sa.column_by_name(name)
        .unwrap_or_else(|| panic!("field '{name}' present"))
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap_or_else(|| panic!("field '{name}' must be a Struct, not opaque bytes"))
}

/// `m_tuple_udt` for one row as `{(label/rank,int):value, …}`.
///
/// The downcasts ARE assertions: a `Map<Struct<Struct,Int32>, Int32>` is only
/// reachable if the composite KEY decoded structurally — a fail-closed or
/// opaque-bytes result could not produce this Arrow shape.
fn rendered_map(combined: &RecordBatch, row: usize) -> String {
    let idx = combined.schema().index_of("m_tuple_udt").expect("column");
    let map = combined
        .column(idx)
        .as_any()
        .downcast_ref::<MapArray>()
        .expect("m_tuple_udt must decode as a Map");
    let entries = map.value(row);
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("map key must be a Struct (the frozen tuple), issue #2339");
    let values = entries
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("map value is Int32");
    let inner = struct_field(keys, "field_0");
    let field_1 = keys
        .column_by_name("field_1")
        .expect("tuple field_1")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("tuple field_1 is Int32");
    let rendered: Vec<String> = (0..keys.len())
        .map(|i| {
            format!(
                "({},{}):{}",
                key_part(inner, i),
                field_1.value(i),
                values.value(i)
            )
        })
        .collect();
    format!("{{{}}}", rendered.join(", "))
}

/// `s_set_udt` for one row as `[[label/rank, …], …]`.
fn rendered_set_of_sets(combined: &RecordBatch, row: usize) -> String {
    let idx = combined.schema().index_of("s_set_udt").expect("column");
    let outer = combined
        .column(idx)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("s_set_udt must decode as a List");
    let elements = outer.value(row);
    let inner_lists = elements
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("each element must itself be a List (the frozen inner set), issue #2339");
    let rendered: Vec<String> = (0..inner_lists.len())
        .map(|e| {
            let members = inner_lists.value(e);
            let sa = members
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("inner set members must be key_part Structs");
            let parts: Vec<String> = (0..sa.len()).map(|i| key_part(sa, i)).collect();
            format!("[{}]", parts.join(", "))
        })
        .collect();
    format!("[{}]", rendered.join(", "))
}

#[test]
fn merged_read_decodes_composite_map_keys_and_nested_frozen_elements() {
    // Committed fixture: MUST resolve under one of the candidate roots. Not a
    // skip — that would be the #3220 green-by-omission this test exists to avoid.
    let fixture =
        fixture_support::table_dir_by_prefix("test_nested_udt_keys", "nested_udt_keys", "nb-1-big")
            .expect(
                "test_nested_udt_keys/nested_udt_keys-*/nb-1-big-Data.db is FORCE-COMMITTED under \
         test-data/datasets/sstables — a checkout that cannot resolve it is broken, not unfetched",
            );

    let producer = producer();
    let batches = producer
        .produce_from_paths(DirSource::new(&fixture.dir).data_paths().expect("paths"))
        .expect(
            "the merged-read path must DECODE the composite map key / nested frozen element, \
                 not fail closed (issue #2339)",
        );
    let arrow_schema = producer.arrow_schema().expect("arrow schema");
    let combined = concat_batches(&arrow_schema.into(), &batches).expect("concat");
    assert!(
        combined.num_rows() > 0,
        "the fixture must yield rows (never a 0-row false pass)"
    );

    let ids = combined
        .column(combined.schema().index_of("id").expect("id"))
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("id Int32");
    let row_of = |id: i32| -> usize {
        (0..combined.num_rows())
            .find(|&r| !ids.is_null(r) && ids.value(r) == id)
            .unwrap_or_else(|| panic!("partition id={id} present in the fixture"))
    };

    // Each golden partition is asserted individually (per-CASE, not a suite-wide
    // "at least one ran" — issue #3220), so one silently-missing partition cannot
    // hide behind its siblings.
    for (id, want) in golden_m_tuple_udt() {
        assert_eq!(
            rendered_map(&combined, row_of(id)),
            want,
            "id={id}: map<frozen<tuple<frozen<key_part>,int>>,int> must decode its \
             COMPOSITE KEY structurally, in Cassandra's cell-path order (issue #2339)"
        );
    }
    for (id, want) in golden_s_set_udt() {
        assert_eq!(
            rendered_set_of_sets(&combined, row_of(id)),
            want,
            "id={id}: set<frozen<set<frozen<key_part>>>> must decode its NESTED FROZEN \
             element with i32-BE framing, in Cassandra's cell-path order (issue #2339)"
        );
    }
}
