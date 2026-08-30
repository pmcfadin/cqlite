//! Unit coverage for the GOLDEN READER half of the AD2 oracle (issue #1491):
//! `golden_rows` / `golden_row`, which turn `sstabledump` JSONL into the expected
//! rows.
//!
//! Split out of `golden_value_parity.rs` under the campsite rule (CLAUDE.md, epic
//! #1135), which also separates the reader's cases from the canonicalization
//! cases they were interleaved with.
//!
//! Every golden line here is transcribed from a shape the committed corpus
//! actually carries, or from `cassandra-5.0.8`
//! `org.apache.cassandra.tools.JsonTransformer` — never from CQLite's output.

use super::*;
use serde_json::json;

/// Collapsing two cells for the SAME map key would silently DROP a golden cell,
/// shrinking the oracle rather than reporting it — so the reader refuses such a
/// golden instead of comparing the part of it that survives (issue #1491 finding
/// J2's class, golden side).
#[test]
fn two_map_cells_with_the_same_key_are_refused_rather_than_collapsed() {
    let dup = concat!(
        r#"{"partition":{"key":["1"],"position":0},"rows":[{"type":"row","position":1,"#,
        r#""liveness_info":{"tstamp":"1970-01-01T00:00:00.001Z"},"cells":["#,
        r#"{"name":"m","path":["k"],"value":"1"},"#,
        r#"{"name":"m","path":["k"],"value":"2"}]}]}"#
    );
    let why = golden_rows(dup, &["id"], &[], &[("m", Multicell::Map)])
        .expect_err("a golden the reader must discard part of is not an oracle");
    assert!(
        why.contains("two cells for the key `k`") && why.contains("`m`"),
        "the refusal must name the collection and the duplicated key: {why}"
    );

    // Two DISTINCT keys are the ordinary shape, so the rule is about the
    // duplicate and not about multicell maps.
    let distinct = dup.replace(
        r#"{"name":"m","path":["k"],"value":"2"}"#,
        r#"{"name":"m","path":["k2"],"value":"2"}"#,
    );
    let rows = golden_rows(&distinct, &["id"], &[], &[("m", Multicell::Map)])
        .expect("distinct map keys are comparable");
    assert_eq!(
        rows.first()
            .and_then(|r| r.get("m"))
            .and_then(Value::as_object)
            .map(serde_json::Map::len),
        Some(2),
        "both map cells must survive into the expected row"
    );
}

/// The permissive-default sweep (Shape B), golden side: a `rows`/`cells`
/// field the reader cannot enumerate is REPORTED, never read as the empty
/// array. `and_then(Value::as_array).unwrap_or(&[])` collapsed "I could not
/// tell what this is" onto "there is nothing here", so such a partition
/// contributed ZERO rows (and such a row ZERO cells) while every surviving
/// sibling kept the comparison non-empty and green.
#[test]
fn a_non_array_rows_or_cells_field_is_reported_not_read_as_empty() {
    let live = concat!(
        r#"{"partition":{"key":["1"],"position":0},"rows":[{"type":"row","position":1,"#,
        r#""liveness_info":{"tstamp":"1970-01-01T00:00:00.001Z"},"cells":["#,
        r#"{"name":"v","value":"x"}]}]}"#
    );
    assert_eq!(
        golden_rows(live, &["id"], &[], &[])
            .expect("the baseline golden is comparable")
            .len(),
        1
    );

    // Each of these is well-formed JSON carrying the field as an OBJECT, so
    // the failure is attributable to the shape and not to a parse error.
    let broken_rows = r#"{"partition":{"key":["1"],"position":0},"rows":{"0":"x"}}"#;
    let broken_cells = concat!(
        r#"{"partition":{"key":["1"],"position":0},"rows":[{"type":"row","position":1,"#,
        r#""liveness_info":{"tstamp":"1970-01-01T00:00:00.001Z"},"cells":{"v":"x"}}]}"#
    );
    for (what, broken) in [("rows", broken_rows), ("cells", broken_cells)] {
        let why = golden_rows(broken, &["id"], &[], &[])
            .expect_err(&format!("a non-array `{what}` must be reported"));
        assert!(
            why.contains(&format!("`{what}` is an object, not an array")),
            "the refusal must name the field and its shape: {why}"
        );
    }

    // ABSENT is different from present-but-wrong, and stays legal: a partition
    // `sstabledump` wrote with no rows contributes none rather than failing.
    let no_rows = r#"{"partition":{"key":["1"],"position":0}}"#;
    assert!(golden_rows(no_rows, &["id"], &[], &[])
        .expect("an absent `rows` is the empty array")
        .is_empty());
}

/// The same sweep, one level down: a multicell MAP's key came from
/// `path_head` through `Value::to_string()` for any non-string, which INVENTED
/// a key (`true`, `1`, `null`) that a genuine `text` key of that spelling
/// would then compare EQUAL to. `sstabledump` writes every cell path with
/// `writeString(...)` (see [`Kinding`]), so a non-string path head means the
/// golden is not the document this reader understands.
#[test]
fn a_non_string_map_path_head_is_refused_rather_than_stringified() {
    let with_path = |path: &str| {
        format!(
            concat!(
                r#"{{"partition":{{"key":["1"],"position":0}},"rows":[{{"type":"row","#,
                r#""position":1,"liveness_info":{{"tstamp":"1970-01-01T00:00:00.001Z"}},"#,
                r#""cells":[{{"name":"m","path":[{path}],"value":"1"}}]}}]}}"#
            ),
            path = path
        )
    };
    let map = &[("m", Multicell::Map)];
    // The ordinary shape: a STRING path head keys the map.
    let rows = golden_rows(&with_path(r#""true""#), &["id"], &[], map)
        .expect("a string path head is the ordinary shape");
    assert_eq!(
        rows.first()
            .and_then(|r| r.get("m"))
            .and_then(Value::as_object)
            .map(|o| o.contains_key("true")),
        Some(true)
    );
    // A boolean, a number and null each used to be projected onto exactly the
    // text a `text` key could hold.
    for head in ["true", "1", "null"] {
        let why = golden_rows(&with_path(head), &["id"], &[], map)
            .expect_err("a non-string path head must be refused");
        assert!(
            why.contains("non-string path head") && why.contains("`m`"),
            "the refusal must name the collection: {why}"
        );
    }
}

/// One golden line carrying `cells`, with `id` as the partition key and no
/// clustering — the shape every case below varies.
fn one_row(cells: &str) -> String {
    format!(
        concat!(
            r#"{{"partition":{{"key":["1"],"position":0}},"rows":[{{"type":"row","#,
            r#""position":1,"liveness_info":{{"tstamp":"2026-06-20T10:33:54.968562Z"}},"#,
            r#""cells":[{cells}]}}]}}"#
        ),
        cells = cells
    )
}

/// L2: a DELETED multicell element must not appear in the expected value.
///
/// `serializeCell` (cassandra-5.0.8 `JsonTransformer`) writes a cell's
/// `deletion_info` INSTEAD of its `value` when `cell.isTombstone()`, and writes
/// the `path` either way — so a tombstoned element differs from a live one only
/// by which of the two fields it carries. Examining the path first collected the
/// tombstone as a live member: a deleted SET element was reconstructed as
/// PRESENT, and a deleted LIST/MAP element was reported as "no value".
///
/// The set line is transcribed verbatim from the committed corpus
/// (`test_deltas/collection_ops` `nb-1-big-Data.db.jsonl`, partition `3`), where
/// `remove_me` is a tombstone next to the live `keep_me`/`also_keep`.
#[test]
fn a_tombstoned_multicell_element_is_absent_from_the_expected_value() {
    let set_cells = concat!(
        r#"{"name":"tags","path":["also_keep"],"value":""},"#,
        r#"{"name":"tags","path":["keep_me"],"value":""},"#,
        r#"{"name":"tags","path":["remove_me"],"deletion_info":"#,
        r#"{"local_delete_time":"2026-06-20T10:33:54Z"},"#,
        r#""tstamp":"2026-06-20T10:33:54.981524Z"}"#
    );
    let rows = golden_rows(
        &one_row(set_cells),
        &["id"],
        &[],
        &[("tags", Multicell::Set)],
    )
    .expect("a set with a deleted element is comparable");
    assert_eq!(
        rows.first().and_then(|r| r.get("tags")),
        Some(&json!(["also_keep", "keep_me"])),
        "the deleted element must not be reconstructed as present"
    );

    // A LIST tombstone has no `value` at all, so before the fix it was not merely
    // wrong but unreadable: the reader reported `list cell has no value`.
    let list_cells = concat!(
        r#"{"name":"vals","path":["899a0200-6c93-11f1-ae1b-f55502e5fa53"],"value":1},"#,
        r#"{"name":"vals","path":["899a020a-6c93-11f1-ae1b-f55502e5fa53"],"#,
        r#""deletion_info":{"local_delete_time":"2026-06-20T10:33:54Z"}}"#
    );
    let rows = golden_rows(
        &one_row(list_cells),
        &["id"],
        &[],
        &[("vals", Multicell::List)],
    )
    .expect("a list with a deleted element is comparable");
    assert_eq!(
        rows.first().and_then(|r| r.get("vals")),
        Some(&json!([1])),
        "only the live list element may survive"
    );

    let map_cells = concat!(
        r#"{"name":"props","path":["k1"],"value":"v1"},"#,
        r#"{"name":"props","path":["k2"],"#,
        r#""deletion_info":{"local_delete_time":"2026-06-20T10:33:54Z"}}"#
    );
    let rows = golden_rows(
        &one_row(map_cells),
        &["id"],
        &[],
        &[("props", Multicell::Map)],
    )
    .expect("a map with a deleted entry is comparable");
    assert_eq!(
        rows.first().and_then(|r| r.get("props")),
        Some(&json!({"k1": "v1"})),
        "the deleted key must not appear in the expected map"
    );
}

/// L2, the boundary case: when EVERY cell of a multicell column is a tombstone
/// the column has no live cell, so it reconciles to `null` — the same state the
/// golden otherwise spells by omitting the column entirely (which the reader
/// already reads as the expected null). Reconstructing an empty container instead
/// would assert a value Cassandra does not return.
#[test]
fn a_fully_deleted_multicell_collection_expects_null() {
    let cells = concat!(
        r#"{"name":"tags","path":["gone"],"#,
        r#""deletion_info":{"local_delete_time":"2026-06-20T10:33:54Z"}}"#
    );
    let rows = golden_rows(&one_row(cells), &["id"], &[], &[("tags", Multicell::Set)])
        .expect("a fully deleted collection is comparable");
    assert_eq!(
        rows.first().and_then(|r| r.get("tags")),
        Some(&Value::Null),
        "a column with no live cell expects null, not an empty container"
    );
}

/// L2, the shape the reader must NOT resolve: a tombstone and another cell at the
/// same path would need timestamp arbitration. Within one row of one SSTable a
/// complex column's cells are keyed by `CellPath`, so this cannot arise from
/// Cassandra — and picking one silently is exactly the guess this lane refuses.
#[test]
fn a_tombstone_and_another_cell_at_the_same_path_are_refused() {
    let cells = concat!(
        r#"{"name":"tags","path":["same"],"value":""},"#,
        r#"{"name":"tags","path":["same"],"#,
        r#""deletion_info":{"local_delete_time":"2026-06-20T10:33:54Z"}}"#
    );
    let why = golden_rows(&one_row(cells), &["id"], &[], &[("tags", Multicell::Set)])
        .expect_err("arbitration is not this reader's job");
    assert!(
        why.contains("timestamp arbitration") && why.contains("`tags`") && why.contains("same"),
        "the refusal must name the collection and the path: {why}"
    );
}

/// One row with an explicit liveness spelling: `Some(tstamp)` writes a
/// `liveness_info`, `None` writes none at all — the shape a row created purely by
/// a collection UPDATE has, where `primaryKeyLivenessInfo` is empty and
/// `serializeCell` therefore stamps EVERY cell.
fn one_row_liveness(liveness: Option<&str>, cells: &str) -> String {
    let live = match liveness {
        Some(tstamp) => format!(r#""liveness_info":{{"tstamp":"{tstamp}"}},"#),
        None => String::new(),
    };
    format!(
        r#"{{"partition":{{"key":["1"],"position":0}},"rows":[{{"type":"row","position":1,{live}"cells":[{cells}]}}]}}"#
    )
}

/// A complex-column deletion marker for `tags`, as `serializeDeletion` writes one.
fn complex_marker(marked: &str) -> String {
    format!(
        r#"{{"name":"tags","deletion_info":{{"marked_deleted":"{marked}","local_delete_time":"2026-06-20T10:33:54Z"}}}}"#
    )
}

/// M2, the shape the committed corpus actually carries: Cassandra writes a complex
/// deletion just ahead of a full-collection INSERT (the sampled goldens show one
/// microsecond), so the marker is older than every cell and shadows nothing.
/// Measured across the fetched + committed goldens: of 11,072 complex markers, NONE
/// shadows a live cell of its column.
#[test]
fn a_complex_deletion_older_than_every_cell_is_ignorable() {
    let cells = format!(
        "{},{},{}",
        complex_marker("2026-06-20T10:33:54.968561Z"),
        r#"{"name":"tags","path":["a"],"value":""}"#,
        r#"{"name":"tags","path":["b"],"value":""}"#
    );
    let rows = golden_rows(
        &one_row_liveness(Some("2026-06-20T10:33:54.968562Z"), &cells),
        &["id"],
        &[],
        &[("tags", Multicell::Set)],
    )
    .expect("a marker older than every cell is comparable");
    assert_eq!(
        rows.first().and_then(|r| r.get("tags")),
        Some(&json!(["a", "b"])),
        "both live elements must survive the ignorable marker"
    );
}

/// M2's first half: a cell carries its OWN `tstamp`, so the row liveness does not
/// state its timestamp. `serializeCell` writes `tstamp` whenever
/// `cell.timestamp() != liveInfo.timestamp()` (cassandra-5.0.8 `JsonTransformer`),
/// and `DeletionTime.deletes(cell)` is `cell.timestamp() <= markedForDeleteAt()`
/// (`DeletionTime.java`) — so this marker DOES shadow `old` even though the row
/// liveness is newer than the marker. Comparing the marker with the row liveness
/// alone reconstructed `old` as a live element the CLI's result set drops.
#[test]
fn a_complex_deletion_shadowing_a_cell_by_its_own_tstamp_is_refused() {
    let shadowed = format!(
        "{},{},{}",
        complex_marker("2026-06-20T10:33:54.968561Z"),
        r#"{"name":"tags","path":["keep"],"value":""}"#,
        r#"{"name":"tags","path":["old"],"value":"","tstamp":"2026-06-20T10:33:50.000000Z"}"#
    );
    let why = golden_rows(
        &one_row_liveness(Some("2026-06-20T10:33:54.968562Z"), &shadowed),
        &["id"],
        &[],
        &[("tags", Multicell::Set)],
    )
    .expect_err("a shadowed live cell is not this reader's to reconcile");
    assert!(
        why.contains("shadows a live cell")
            && why.contains("`tags`")
            && why.contains("timestamp arbitration"),
        "the refusal must name the column and what it declines to do: {why}"
    );

    // The BOUNDARY: `deletes()` is `<=`, so a cell stamped EXACTLY at the marker is
    // shadowed too. One microsecond later it is not.
    for (cell_tstamp, shadows) in [
        ("2026-06-20T10:33:54.968561Z", true),
        ("2026-06-20T10:33:54.968562Z", false),
    ] {
        let cells = format!(
            r#"{},{{"name":"tags","path":["x"],"value":"","tstamp":"{cell_tstamp}"}}"#,
            complex_marker("2026-06-20T10:33:54.968561Z")
        );
        let read = golden_rows(
            &one_row_liveness(Some("2026-06-20T10:33:54.968562Z"), &cells),
            &["id"],
            &[],
            &[("tags", Multicell::Set)],
        );
        assert_eq!(
            read.is_err(),
            shadows,
            "a cell stamped {cell_tstamp} against a marker at .968561Z: \
             expected shadowed={shadows}, got {read:?}"
        );
    }
}

/// M2's second half: a row with a marker and NO liveness at all is the ordinary
/// shape of a full-collection `UPDATE` — `primaryKeyLivenessInfo` is empty, so
/// `serializeCell` stamps every cell and each one states its own timestamp. Such a
/// row is DECIDABLE and was refused unnecessarily.
#[test]
fn a_marker_only_row_with_no_liveness_is_decidable_from_the_cell_tstamps() {
    let cells = format!(
        "{},{}",
        complex_marker("2026-06-20T10:33:54.968561Z"),
        r#"{"name":"tags","path":["a"],"value":"","tstamp":"2026-06-20T10:33:54.968562Z"}"#
    );
    let rows = golden_rows(
        &one_row_liveness(None, &cells),
        &["id"],
        &[],
        &[("tags", Multicell::Set)],
    )
    .expect("a row whose cells state their own timestamps is comparable");
    assert_eq!(
        rows.first().and_then(|r| r.get("tags")),
        Some(&json!(["a"])),
        "the live element must survive"
    );

    // But a cell with NEITHER its own `tstamp` NOR a row liveness to inherit one
    // from cannot be timed at all, so it is REPORTED rather than assumed newer.
    let untimed = format!(
        "{},{}",
        complex_marker("2026-06-20T10:33:54.968561Z"),
        r#"{"name":"tags","path":["a"],"value":""}"#
    );
    let why = golden_rows(
        &one_row_liveness(None, &untimed),
        &["id"],
        &[],
        &[("tags", Multicell::Set)],
    )
    .expect_err("an untimeable cell must not be assumed to survive the marker");
    assert!(
        why.contains("no `tstamp`") && why.contains("liveness"),
        "the refusal must say which fact is missing: {why}"
    );
}

/// The precision that keeps the refusal from costing coverage: a cell the marker
/// shadows which is ITSELF a tombstone contributes nothing to the reconstructed
/// collection either way, so the marker cannot change the expectation for it and
/// the row stays comparable.
#[test]
fn a_shadowed_cell_that_is_itself_a_tombstone_does_not_refuse_the_row() {
    let cells = format!(
        "{},{},{}",
        complex_marker("2026-06-20T10:33:54.968561Z"),
        r#"{"name":"tags","path":["keep"],"value":""}"#,
        concat!(
            r#"{"name":"tags","path":["gone"],"#,
            r#""deletion_info":{"local_delete_time":"2026-06-20T10:33:54Z"},"#,
            r#""tstamp":"2026-06-20T10:33:50.000000Z"}"#
        )
    );
    let rows = golden_rows(
        &one_row_liveness(Some("2026-06-20T10:33:54.968562Z"), &cells),
        &["id"],
        &[],
        &[("tags", Multicell::Set)],
    )
    .expect("a shadowed tombstone changes no expectation");
    assert_eq!(
        rows.first().and_then(|r| r.get("tags")),
        Some(&json!(["keep"])),
        "the live element must survive and the deleted one must not"
    );
}

/// `marked_deleted` is what tells the two deletion spellings apart:
/// `serializeDeletion` writes it for a COMPLEX column, `serializeCell`'s tombstone
/// branch writes `local_delete_time` alone. So a `marked_deleted` on a column the
/// case declares scalar cannot be reconciled as a cell tombstone (which would set
/// the whole column to null on a guess).
#[test]
fn a_complex_marker_on_a_column_declared_scalar_is_refused() {
    let why = golden_rows(
        &one_row_liveness(
            Some("2026-06-20T10:33:54.968562Z"),
            &complex_marker("2026-06-20T10:33:54.968561Z"),
        ),
        &["id"],
        &[],
        &[],
    )
    .expect_err("the dump says the column is complex; the case says it is not");
    assert!(
        why.contains("complex deletion") && why.contains("`tags`"),
        "the refusal must name the column and the disagreement: {why}"
    );

    // A CELL tombstone — `local_delete_time` alone — is the shape that legitimately
    // reconciles to null on a scalar column, and it still does.
    let cell_tombstone =
        r#"{"name":"tags","deletion_info":{"local_delete_time":"2026-06-20T10:33:54Z"}}"#;
    let rows = golden_rows(
        &one_row_liveness(Some("2026-06-20T10:33:54.968562Z"), cell_tombstone),
        &["id"],
        &[],
        &[],
    )
    .expect("a scalar cell tombstone reconciles to null");
    assert_eq!(
        rows.first().and_then(|r| r.get("tags")),
        Some(&Value::Null),
        "a scalar cell tombstone expects null"
    );
}

/// L2, the two shapes `serializeCell` can never emit: it writes EXACTLY one of
/// `value` and `deletion_info` per cell. Either a cell with both or a cell with
/// neither means the golden is not the document this reader understands — and a
/// live multicell SET cell does carry `"value": ""`, so "no value" is not a
/// legitimate live shape.
#[test]
fn a_multicell_cell_must_carry_exactly_one_of_value_and_deletion() {
    let both = concat!(
        r#"{"name":"tags","path":["x"],"value":"","#,
        r#""deletion_info":{"local_delete_time":"2026-06-20T10:33:54Z"}}"#
    );
    let why = golden_rows(&one_row(both), &["id"], &[], &[("tags", Multicell::Set)])
        .expect_err("value and deletion together is not a dump shape");
    assert!(
        why.contains("both value and deletion") && why.contains("`tags`"),
        "{why}"
    );

    let neither = r#"{"name":"tags","path":["x"]}"#;
    let why = golden_rows(&one_row(neither), &["id"], &[], &[("tags", Multicell::Set)])
        .expect_err("a cell with neither is not a dump shape");
    assert!(
        why.contains("neither a value nor a deletion") && why.contains("`tags`"),
        "{why}"
    );
}
