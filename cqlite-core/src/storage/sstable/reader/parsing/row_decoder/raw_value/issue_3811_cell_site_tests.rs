//! Issue #3811 / roborev round 4 — the two frozen-UDT consumption checks in
//! [`super::super::V5CompressedLegacyParser::decode_complex_cell_value`], driven
//! **at their production call site** rather than through a helper.
//!
//! # What this file closes, and why it is separate from the helper suite
//!
//! `issue_3811_consumption_demo_tests` covers every consumption guard whose
//! containing function can be called with a plainly-constructed parser. Two
//! could not be: the guards at `cell_value_complex.rs:128` (marshal-form frozen
//! UDT) and `:180` (registry-resolved frozen UDT). Their containing function
//! takes an `&SSTableReader`, and the sibling suite therefore reached the same
//! RULE only through `decode_frozen_udt_from_header_type` — a different caller.
//! Roborev's round-4 finding is precisely that: "removal or miswiring of either
//! check would go undetected". Filed as #3861; closed here.
//!
//! The stated blocker was that `SSTableReader` has no `Default` and no builder —
//! only four `async` `SSTableReader::open*` constructors needing a tokio runtime
//! and a real parseable SSTable on disk. That is true, and it is not a blocker:
//! `regression_1741h_tests.rs` already establishes the pattern in this same
//! directory — write a one-row SSTable with `SSTableWriter` into a `TempDir`
//! inside a `#[tokio::test]`, open it, and use the resulting reader as CONTEXT
//! for a hand-crafted-byte parse. Hence the `feature = "write-support"` gate (a
//! DEFAULT feature, so these run in every ordinary lane).
//!
//! # THE FIXTURE IS NOT THE ORACLE (#3042) — READ THIS BEFORE JUDGING THE SHAPE
//!
//! A test that both WRITES and READS with CQLite is invariant to a uniform
//! framing error and can never establish a format property. **Nothing here is a
//! round trip.** The SSTable written below holds `t(id int, name text)` — no
//! UDT, no frozen type, no relation whatsoever to the values under test. It
//! exists for ONE reason: to obtain a live `SSTableReader` to pass as the
//! `reader` argument, which both branches under test never dereference (it is
//! forwarded only by the frozen-inner recursion arm at
//! `cell_value_complex.rs:257`, which these vectors do not take). The reader is
//! PLUMBING, not evidence.
//!
//! Every byte-vector and every expectation is imported from
//! `issue_3811_consumption_demo_tests`, where they are derived from
//! `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/TupleType.java`
//! `split(...)` (`UserType extends TupleType`), transcribed in
//! `docs/round-artifacts/issue-3811-cassandra-oracle.md`. Sharing ONE definition
//! is deliberate: a second copy of the vectors would be a second place for the
//! oracle to drift, and the helper-level and call-site-level suites must not be
//! able to form two opinions about what Cassandra accepts.
//!
//! # Which production shape reaches which arm
//!
//! Both arms require `lowered.starts_with("frozen<")`, and `lowered` is
//! `column.data_type.to_lowercase()` (`CellKind::from_type`) — so the column
//! types below are computed through `CellKind::from_type` rather than
//! hand-spelled, and the tests assert the tag really is `CellKind::Complex`.
//!
//! - **`:128`, marshal-form** — taken when `Self::is_udt_type(&column.data_type)`,
//!   i.e. the declared type CONTAINS `org.apache.cassandra.db.marshal.UserType`.
//!   Combined with the `frozen<` prefix that is
//!   `frozen<org.apache.cassandra.db.marshal.UserType(...)>`, which is what
//!   `convert_marshal_type_to_cql` emits for an on-disk `FrozenType(UserType(…))`
//!   header type: the `FrozenType(` wrapper becomes `frozen<…>` while the inner
//!   `UserType(...)` is returned VERBATIM
//!   (`parser/enhanced_statistics_parser/marshal_type.rs`). This arm is checked
//!   BEFORE the registry, so it fires whether or not a registry is wired.
//! - **`:180`, registry-resolved** — taken for a bare inner name that
//!   `UdtRegistry::get_udt_qualified` resolves, i.e. the CQL short form
//!   `frozen<addr>` with a registry carrying `addr` (issue #502).
//!
//! # DISCRIMINATION LABELS (AC6)
//!
//! Same convention as the sibling suite. Each guard was disabled **ALONE** — the
//! two were never disabled together, because a joint disable cannot attribute
//! either (a guard with no test is indistinguishable from a guard whose test
//! passes; that error was made once on this issue in round 2 and is recorded
//! rather than quietly corrected). The measured red sets are in the sibling
//! suite's attribution table.

use super::super::{CellKind, V5CompressedLegacyParser};
use super::issue_3811_consumption_demo_tests as vectors;
use crate::schema::Column;
use crate::{Result, Value};

// ---------------------------------------------------------------------------
// Reader CONTEXT (see the #3042 note in the module header: plumbing, not oracle)
// ---------------------------------------------------------------------------

/// A live `SSTableReader` plus the temp dir backing it. The dir is held for the
/// lifetime of the reader so the open file is never a deleted inode.
struct ReaderContext {
    reader: crate::storage::sstable::reader::SSTableReader,
    _dir: tempfile::TempDir,
}

/// Write a one-row `t(id int, name text)` SSTable and open it. Deliberately the
/// SIMPLEST possible table: it shares no type, no column and no framing with the
/// values under test, so it cannot supply an expectation to anything below.
async fn reader_context() -> ReaderContext {
    use crate::schema::{KeyColumn, TableSchema};
    use crate::storage::sstable::writer::{SSTableFormat, SSTableWriter};
    use crate::storage::write_engine::mutation::{CellOperation, Mutation, PartitionKey, TableId};

    let schema = TableSchema {
        keyspace: "ks".to_string(),
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
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    };

    let dir = tempfile::TempDir::new().expect("temp dir");
    let mut writer =
        SSTableWriter::with_format(dir.path().to_path_buf(), 1, &schema, 16, SSTableFormat::Big)
            .expect("writer");
    let m = Mutation::new(
        TableId::new("ks", "t"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::text("hi"),
        }],
        1_000_000,
        None,
    );
    let key = m.decorated_key(&schema).expect("decorated key");
    writer.write_partition(key, vec![m]).expect("write");
    let info = writer.finish().await.expect("finish");

    let config = crate::Config::default();
    let platform = std::sync::Arc::new(
        crate::platform::Platform::new(&config)
            .await
            .expect("platform"),
    );
    let reader =
        crate::storage::sstable::reader::SSTableReader::open(&info.data_path, &config, platform)
            .await
            .expect("open the context reader");

    ReaderContext { reader, _dir: dir }
}

// ---------------------------------------------------------------------------
// Column shapes + the production driver
// ---------------------------------------------------------------------------

fn column(data_type: String) -> Column {
    Column {
        name: "c".to_string(),
        data_type,
        nullable: true,
        default: None,
        is_static: false,
    }
}

/// `frozen<org.apache.cassandra.db.marshal.UserType(...)>` — reaches
/// `cell_value_complex.rs:128`.
fn marshal_column() -> Column {
    column(format!("frozen<{}>", vectors::MARSHAL_UDT))
}

/// `frozen<addr>` with `addr` in the parser's registry — reaches
/// `cell_value_complex.rs:180`.
fn registry_column() -> Column {
    column(format!("frozen<{}>", vectors::REGISTRY_UDT))
}

/// Resolve the per-column dispatch tag exactly as `RowColumnResolution::build`
/// does, and assert the column really is routed to the complex ladder. Returns
/// the tag so callers can pass it down the production hot path.
fn complex_kind(column: &Column) -> CellKind {
    let kind = CellKind::from_type(&column.data_type);
    match &kind {
        CellKind::Complex(lowered) => assert!(
            lowered.starts_with("frozen<"),
            "the lowered dispatch string must keep the CQL frozen prefix, got {lowered:?}"
        ),
        other => panic!(
            "a frozen-UDT column must dispatch to CellKind::Complex (the ladder that owns \
             `decode_complex_cell_value`), got {other:?}"
        ),
    }
    kind
}

/// Drive the function that OWNS the two guards, with the same `lowered` string
/// production computes and the reader argument it really takes.
fn decode_at_call_site(
    ctx: &ReaderContext,
    column: &Column,
    blob: &[u8],
) -> Result<(Value, usize)> {
    let data = vectors::frozen_udt_cell(blob);
    let kind = complex_kind(column);
    let CellKind::Complex(lowered) = &kind else {
        unreachable!("complex_kind asserts the variant")
    };
    let mut offset = 0usize;
    let value = vectors::parser().decode_complex_cell_value(
        &data,
        &mut offset,
        lowered.as_ref(),
        column,
        None,
        &ctx.reader,
    )?;
    Ok((value, offset))
}

/// Drive the whole CELL entry point (`parse_cell_value_schema_order`) with the
/// precomputed tag, i.e. the exact hot-path call shape: `[flags][VUInt len][blob]`
/// with flags `0x08` = `USE_ROW_TIMESTAMP` (no conditional fields, non-empty
/// value), so the only work between the entry point and the guard is the
/// dispatch under test.
fn decode_through_cell_entry_point(
    ctx: &ReaderContext,
    column: &Column,
    blob: &[u8],
) -> Result<Value> {
    let mut data = vec![0x08u8];
    data.extend(vectors::frozen_udt_cell(blob));
    let kind = complex_kind(column);
    vectors::parser()
        .parse_cell_value_schema_order(&data, 0, column, None, Some(&kind), &ctx.reader)
        .map(|(value, _ts, _exp, _off)| value)
}

/// Both arms wrap their result in `Value::Frozen` (`cell_value_complex.rs:265`).
fn unwrap_frozen<'a>(value: &'a Value, ctx: &str) -> &'a Value {
    match value {
        Value::Frozen(inner) => inner,
        other => panic!("{ctx}: the frozen arms must wrap in Value::Frozen, got {other:?}"),
    }
}

/// The refusal must be THIS guard's, not the outer bounded wrapper's: the two
/// call sites pass the literal type tag `"frozen UDT"`.
fn assert_refused_by_the_frozen_udt_guard(
    result: Result<Value>,
    expected_consumed: usize,
    expected_len: usize,
    ctx: &str,
) {
    if let Err(e) = &result {
        let msg = e.to_string();
        assert!(
            msg.contains("type 'frozen UDT'"),
            "{ctx}: the refusal must come from a `frozen UDT` consumption check, got: {msg}"
        );
    }
    vectors::assert_refused_short(result, expected_consumed, expected_len, ctx);
}

// ---------------------------------------------------------------------------
// Routing control (no reader needed)
// ---------------------------------------------------------------------------

/// **CONTROL / NON-DISCRIMINATING.** Both column shapes dispatch to the complex
/// ladder. If this fails, the cases below are testing the wrong function and
/// their greens mean nothing.
#[test]
fn both_frozen_udt_column_shapes_route_to_the_complex_ladder() {
    complex_kind(&marshal_column());
    complex_kind(&registry_column());
    assert!(
        V5CompressedLegacyParser::is_udt_type(&marshal_column().data_type),
        "the marshal-form column must satisfy the `is_udt_type` predicate that selects \
         the :128 arm ahead of the registry lookup"
    );
    assert!(
        !V5CompressedLegacyParser::is_udt_type(&registry_column().data_type),
        "the bare-name column must NOT satisfy `is_udt_type`, or it would take the :128 \
         arm and the :180 arm would be untested"
    );
}

// ---------------------------------------------------------------------------
// ARM :128 — marshal-form frozen UDT, at its production call site
// ---------------------------------------------------------------------------

/// **CONTROL / NON-DISCRIMINATING.** A well-formed cell decodes and the offset
/// advances past the whole cell (1 VUInt byte + 18 blob bytes).
#[tokio::test]
async fn cell_site_marshal_arm_exact_decodes_ok() {
    let ctx = reader_context().await;
    let (value, off) = decode_at_call_site(&ctx, &marshal_column(), &vectors::case1_exact())
        .expect("a well-formed marshal-form frozen UDT cell");
    vectors::assert_both_fields(
        unwrap_frozen(&value, "cell-site/marshal/case1"),
        "marshal/case1",
    );
    assert_eq!(off, 19, "the offset must advance past the whole cell");
}

/// **CONTROL / NON-DISCRIMINATING.** `TupleType.split` rule 1: an absent
/// trailing field that ends the buffer exactly is LEGAL. This is the case a
/// naive "every declared field must be present" guard would break.
#[tokio::test]
async fn cell_site_marshal_arm_legally_short_decodes_ok() {
    let ctx = reader_context().await;
    let (value, off) =
        decode_at_call_site(&ctx, &marshal_column(), &vectors::case4_legally_short())
            .expect("a legally short marshal-form frozen UDT cell");
    vectors::assert_city_absent(
        unwrap_frozen(&value, "cell-site/marshal/case4"),
        "marshal/case4",
    );
    assert_eq!(off, 12, "the offset must advance past the whole cell");
}

/// **DISCRIMINATING — attributes to the `cell_value_complex.rs:128` guard alone.**
/// `TupleType.split` rule 4: `position(18) < length(19)` ⇒ "but got more".
/// `parse_udt_value` reports 18 of a 19-byte blob; before #3811 this caller
/// spelled `let (udt_value, _) = …` and accepted it.
#[tokio::test]
async fn cell_site_marshal_arm_trailing_garbage_is_refused() {
    let ctx = reader_context().await;
    assert_refused_by_the_frozen_udt_guard(
        decode_at_call_site(&ctx, &marshal_column(), &vectors::case2_trailing_garbage())
            .map(|(v, _)| v),
        18,
        19,
        "cell-site/marshal/trailing (rule 4)",
    );
}

/// **DISCRIMINATING — `cell_value_complex.rs:128`.** `TupleType.split` rule 2 at
/// component 1: `position(11) + 4 > length(12)` ⇒ "Not enough bytes to read 1th
/// component". A truncated component header is corruption, NOT an omitted field.
#[tokio::test]
async fn cell_site_marshal_arm_partial_prefix_is_refused() {
    let ctx = reader_context().await;
    assert_refused_by_the_frozen_udt_guard(
        decode_at_call_site(&ctx, &marshal_column(), &vectors::case3_partial_prefix())
            .map(|(v, _)| v),
        11,
        12,
        "cell-site/marshal/partial (rule 2)",
    );
}

/// **DISCRIMINATING — `cell_value_complex.rs:128`.** The same refusal reached
/// from the real per-cell entry point with the precomputed dispatch tag, so the
/// guard is pinned on the path a scan actually takes and not only on a direct
/// call to its containing function.
#[tokio::test]
async fn cell_site_marshal_arm_trailing_garbage_is_refused_from_the_cell_entry_point() {
    let ctx = reader_context().await;
    assert_refused_by_the_frozen_udt_guard(
        decode_through_cell_entry_point(
            &ctx,
            &marshal_column(),
            &vectors::case2_trailing_garbage(),
        ),
        18,
        19,
        "cell-entry/marshal/trailing (rule 4)",
    );
}

// ---------------------------------------------------------------------------
// ARM :180 — registry-resolved frozen UDT, at its production call site
//
// The SAME vectors as the arm above, because #3631's history on this codebase is
// a fix landing on one arm and not its sibling.
// ---------------------------------------------------------------------------

/// **CONTROL / NON-DISCRIMINATING.**
#[tokio::test]
async fn cell_site_registry_arm_exact_decodes_ok() {
    let ctx = reader_context().await;
    let (value, off) = decode_at_call_site(&ctx, &registry_column(), &vectors::case1_exact())
        .expect("a well-formed registry-resolved frozen UDT cell");
    vectors::assert_both_fields(
        unwrap_frozen(&value, "cell-site/registry/case1"),
        "registry/case1",
    );
    assert_eq!(off, 19, "the offset must advance past the whole cell");
}

/// **CONTROL / NON-DISCRIMINATING.** Rule 1 stays legal on this arm too.
#[tokio::test]
async fn cell_site_registry_arm_legally_short_decodes_ok() {
    let ctx = reader_context().await;
    let (value, off) =
        decode_at_call_site(&ctx, &registry_column(), &vectors::case4_legally_short())
            .expect("a legally short registry-resolved frozen UDT cell");
    vectors::assert_city_absent(
        unwrap_frozen(&value, "cell-site/registry/case4"),
        "registry/case4",
    );
    assert_eq!(off, 12, "the offset must advance past the whole cell");
}

/// **DISCRIMINATING — attributes to the `cell_value_complex.rs:180` guard alone.**
#[tokio::test]
async fn cell_site_registry_arm_trailing_garbage_is_refused() {
    let ctx = reader_context().await;
    assert_refused_by_the_frozen_udt_guard(
        decode_at_call_site(&ctx, &registry_column(), &vectors::case2_trailing_garbage())
            .map(|(v, _)| v),
        18,
        19,
        "cell-site/registry/trailing (rule 4)",
    );
}

/// **DISCRIMINATING — `cell_value_complex.rs:180`.**
#[tokio::test]
async fn cell_site_registry_arm_partial_prefix_is_refused() {
    let ctx = reader_context().await;
    assert_refused_by_the_frozen_udt_guard(
        decode_at_call_site(&ctx, &registry_column(), &vectors::case3_partial_prefix())
            .map(|(v, _)| v),
        11,
        12,
        "cell-site/registry/partial (rule 2)",
    );
}

/// **DISCRIMINATING — `cell_value_complex.rs:180`,** from the real per-cell entry
/// point with the precomputed dispatch tag.
#[tokio::test]
async fn cell_site_registry_arm_trailing_garbage_is_refused_from_the_cell_entry_point() {
    let ctx = reader_context().await;
    assert_refused_by_the_frozen_udt_guard(
        decode_through_cell_entry_point(
            &ctx,
            &registry_column(),
            &vectors::case2_trailing_garbage(),
        ),
        18,
        19,
        "cell-entry/registry/trailing (rule 4)",
    );
}

// ---------------------------------------------------------------------------
// The guard the AC6 sweep found UNCOVERED: `parse_udt_field_value`'s
// `CqlType::Udt` arm (`udt.rs:669`)
//
// Re-measuring every consumption guard site INDEPENDENTLY — rather than the four
// `parse_udt_value` callers jointly, as an earlier revision of the sibling
// suite's table did — gave this one a red set of ZERO. It was reachable from
// nowhere in the suite, which is roborev's round-4 finding at a third site: the
// joint disable could not see it, because the reds it produced came from the
// header-type caller and would have read identically with this arm untested.
//
// It is reached only through `parse_udt_value`'s FIELD LOOP (`udt.rs:525`) with
// a field whose `CqlType` is `Udt` — which `parse_cassandra_type_with_depth`
// produces for a NESTED marshal `UserType(...)` (`udt.rs:337-345`). The bounded
// entry point cannot get there: BOTH of `parse_value_from_raw_bytes`'s UDT arms
// delegate to `parse_raw_type_value`, whose nested-UDT handling calls
// `parse_inline_udt_value` instead. The route that does reach it is a frozen
// cell whose declared UDT nests another UDT — i.e. this file's subject.
// ---------------------------------------------------------------------------

/// `frozen<UserType(outer, label text, addr UserType(addr, street, city))>`.
fn marshal_outer_column() -> Column {
    column(format!("frozen<{}>", vectors::MARSHAL_OUTER))
}

/// The refusal must come from a PER-FIELD consumption guard rather than the
/// enclosing frozen-UDT one: the outer component prefix counts the nested bytes
/// exactly, so the outer `frozen UDT` check is satisfied and only a field-level
/// guard can refuse.
///
/// #3722 CHANGED WHICH field-level guard fires, without changing the property.
/// This originally required `type 'nested UDT'` (`udt.rs`). #3722 routed
/// `parse_inline_udt_value`'s fall-through through the ONE consolidated UDT-field
/// decoder instead of the deleted `parse_simple_udt_field_value`, so this input
/// now reaches the `inline UDT` guard first and is refused there. Both are
/// per-field full-consumption checks enforcing the same TupleType.split rules 2
/// and 4, so either satisfies what this test exists to prove — and
/// `assert_refused_short` below still pins the exact consumed/expected counts, so
/// widening the accepted SOURCE does not weaken the assertion about the OUTCOME.
///
/// DECLARED, not asserted: whether the `nested UDT` guard is still reachable on
/// some other input is not established here. If it is not, it is dead code rather
/// than a lost check — the refusal is unconditional either way — but that is worth
/// confirming separately rather than assumed by this test.
fn assert_refused_by_the_nested_udt_guard(
    result: Result<Value>,
    expected_consumed: usize,
    expected_len: usize,
    ctx: &str,
) {
    if let Err(e) = &result {
        let msg = e.to_string();
        assert!(
            msg.contains("type 'nested UDT'") || msg.contains("type 'inline UDT'"),
            "{ctx}: the refusal must come from a PER-FIELD consumption check \
             (nested UDT or inline UDT), not the enclosing frozen-UDT one, got: {msg}"
        );
    }
    vectors::assert_refused_short(result, expected_consumed, expected_len, ctx);
}

/// **CONTROL / NON-DISCRIMINATING.** The outer framing is sound.
#[tokio::test]
async fn cell_site_nested_udt_field_exact_decodes_ok() {
    let ctx = reader_context().await;
    let bytes = vectors::outer_with_nested(&vectors::case1_exact());
    let (value, _) = decode_at_call_site(&ctx, &marshal_outer_column(), &bytes)
        .expect("a well-formed nested-UDT frozen cell");
    match unwrap_frozen(&value, "cell-site/nested/case1") {
        Value::Udt(udt) => assert_eq!(udt.fields.len(), 2, "outer field count"),
        other => panic!("expected Value::Udt, got {other:?}"),
    }
}

/// **CONTROL / NON-DISCRIMINATING.** `TupleType.split` rule 1 is legal at depth,
/// so the guard must not reject an omitted trailing field of the NESTED value.
#[tokio::test]
async fn cell_site_nested_udt_field_legally_short_decodes_ok() {
    let ctx = reader_context().await;
    let bytes = vectors::outer_with_nested(&vectors::case4_legally_short());
    decode_at_call_site(&ctx, &marshal_outer_column(), &bytes)
        .expect("a legally short NESTED encoding is accepted (rule 1)");
}

/// **DISCRIMINATING — attributes to `udt.rs:669` alone.** Rule 4 inside the
/// nested field: the outer prefix says 19 bytes and 19 follow, so the enclosing
/// frozen-UDT check passes; the nested decode reads 18 of them.
#[tokio::test]
async fn cell_site_nested_udt_field_trailing_garbage_is_refused() {
    let ctx = reader_context().await;
    let bytes = vectors::outer_with_nested(&vectors::case2_trailing_garbage());
    assert_refused_by_the_nested_udt_guard(
        decode_at_call_site(&ctx, &marshal_outer_column(), &bytes).map(|(v, _)| v),
        18,
        19,
        "cell-site/nested/trailing (rule 4)",
    );
}

/// **DISCRIMINATING — `udt.rs:669`.** Rule 2 inside the nested field, one byte
/// from the legal omission above.
#[tokio::test]
async fn cell_site_nested_udt_field_partial_prefix_is_refused() {
    let ctx = reader_context().await;
    let bytes = vectors::outer_with_nested(&vectors::case3_partial_prefix());
    assert_refused_by_the_nested_udt_guard(
        decode_at_call_site(&ctx, &marshal_outer_column(), &bytes).map(|(v, _)| v),
        11,
        12,
        "cell-site/nested/partial (rule 2)",
    );
}
