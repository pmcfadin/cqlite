//! The GOLDEN's admission as an oracle — issue #1490 (AD1), epic #1469.
//!
//! Split out of `issue_1490_parquet_harness_units.rs` under the campsite rule
//! (that file reached 1839 lines against the ~1500 test target, #1135) along a
//! SUBJECT seam: everything here is about whether a committed sstabledump dump
//! may be USED as an oracle at all, decided BEFORE any value is compared —
//!
//!   * WHICH golden (it must be the dump of the very `*-Data.db` generation the
//!     export reads, or the harness refuses the pair),
//!   * whether its TEXT can be prepared without losing a `decimal` literal, and
//!     whether it carries a duplicate key, a placeholder or a structure the
//!     harness cannot read,
//!   * whether it is ELIGIBLE for physical-dump parity at all (#1742) — decided
//!     from the golden TEXT, because the shared parser silently turns a
//!     present-but-invalid `ttl`/`deletion_info`/`rows` into an absence.
//!
//! One rule runs through all of it: a value the harness did not authoritatively
//! obtain must not be presented as one it did. "Parsed without error", "absent"
//! and "the current fixture fits" are each NOT "verified".
//!
//! Like the units file, these need NO fetched corpus (the resolver cases build
//! scratch directories, the text cases are literals), so they run in every
//! checkout.

#![cfg(feature = "state_machine")]

#[path = "support/parquet_parity/mod.rs"]
mod parquet_parity;

use parquet_parity::golden_text::{placeholder_marker, preserve_exact_lexemes};

// ---------------------------------------------------------------------------
// The golden is BOUND to the Data generation it dumps (#1490 round 5)
//
// Resolving "one *-Data.db" and "one *-Data.db.jsonl" independently lets a
// partially regenerated fixture compare generation A's data against generation
// B's dump — which yields either a false failure or a false PASS, and the
// harness would flag neither. These controls exercise the real resolver against
// SCRATCH directories (nothing under `test-data/` is touched): a mismatched pair
// must be a NAMED refusal, and a matching pair must still resolve, so the
// refusal cannot pass by rejecting everything.
// ---------------------------------------------------------------------------

/// Build a scratch table directory holding the named files (contents are
/// irrelevant: the resolver decides on NAMES).
fn scratch_table_dir(names: &[&str]) -> tempfile::TempDir {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    for name in names {
        std::fs::write(tmp.path().join(name), b"").expect("write scratch fixture file");
    }
    tmp
}

/// A golden belonging to a DIFFERENT generation than the Data file must FAIL
/// CLOSED, with a message naming both files — never be compared.
#[test]
fn a_golden_from_another_generation_is_refused_not_compared() {
    let tmp = scratch_table_dir(&[
        "nb-2-big-Data.db",
        "nb-2-big-Statistics.db",
        // Left behind by a partial regeneration: the dump of generation 1.
        "nb-1-big-Data.db.jsonl",
    ]);
    let err = parquet_parity::fixture_root::fixture_in_table_dir("ks.t", tmp.path().to_path_buf())
        .expect_err("a golden from another generation must be refused");
    assert!(
        err.contains("nb-2-big-Data.db") && err.contains("nb-1-big-Data.db.jsonl"),
        "the refusal must name BOTH files so the stale one can be found: {err}"
    );
    assert!(
        err.contains("nb-2-big-Data.db.jsonl"),
        "…and name the golden that WOULD belong to this generation: {err}"
    );
}

/// The positive control: a CORRESPONDING pair still resolves, and the resolved
/// golden is the one derived from the Data file — so the refusal above is not a
/// resolver that rejects everything.
#[test]
fn a_corresponding_data_and_golden_pair_resolves() {
    let tmp = scratch_table_dir(&[
        "nb-2-big-Data.db",
        "nb-2-big-Data.db.jsonl",
        "nb-2-big-Statistics.db",
        // A `.txt` sidecar and an unrelated component must not confuse it.
        "nb-2-big-Statistics.db.txt",
    ]);
    let fixture =
        parquet_parity::fixture_root::fixture_in_table_dir("ks.t", tmp.path().to_path_buf())
            .expect("a corresponding pair must resolve");
    assert_eq!(
        fixture.golden,
        tmp.path().join("nb-2-big-Data.db.jsonl"),
        "the golden must be the one DERIVED from the selected Data file"
    );
}

/// The two absence/ambiguity refusals this resolver already owed, asserted here
/// so the binding above cannot be the only thing keeping them: no golden at all,
/// and two Data generations in one directory.
#[test]
fn a_missing_golden_or_a_second_generation_is_refused() {
    let no_golden = scratch_table_dir(&["nb-1-big-Data.db"]);
    let err =
        parquet_parity::fixture_root::fixture_in_table_dir("ks.t", no_golden.path().to_path_buf())
            .expect_err("a fixture with no golden has no oracle");
    assert!(err.contains("golden"), "{err}");

    let two_gens = scratch_table_dir(&[
        "nb-1-big-Data.db",
        "nb-2-big-Data.db",
        "nb-2-big-Data.db.jsonl",
    ]);
    let err =
        parquet_parity::fixture_root::fixture_in_table_dir("ks.t", two_gens.path().to_path_buf())
            .expect_err("a multi-generation table is not a single-generation dump");
    assert!(err.contains("*-Data.db generation"), "{err}");
}

// ---------------------------------------------------------------------------
// `golden_text.rs`: the golden TEXT preparation, and its REFUSALS. WHAT is
// quoted, at which POSITIONS, and end to end through the real load path lives in
// section 5 of `issue_1490_parquet_declaration_and_keys.rs`, beside the
// declared-type door that decides it. HERE is the module's own unit contract:
// the sstabledump STRUCTURE it relies on, and what it does when that structure
// does not hold.
// ---------------------------------------------------------------------------

/// The rewrite REFUSES what it cannot read, and the placeholder refusal the
/// harness took over from `canonical_jsonl` still fires.
#[test]
fn the_rewrite_fails_closed() {
    // Malformed JSON: refused by the parse, never waved through to the
    // untransformed text (which would silently restore the `f64` path).
    #[rustfmt::skip]
    let malformed = [
        r#"{"a":}"#, r#"{"a":1,}"#, r#"{"a":01x}"#, r#"{"a":1.}"#, r#"{"a":1e}"#,
        r#"{a:1}"#, r#"{"a":"unterminated"#, r#"{"a":1} trailing"#, r#"[1,2"#,
        r#"{"a":tru}"#,
    ];
    for bad in malformed {
        assert!(
            preserve_exact_lexemes(&format!("{bad}\n"), &[]).is_err(),
            "{bad:?} must be refused, never waved through to the untransformed text"
        );
    }

    // STRUCTURE the harness relies on. Each of these is well-formed JSON, so
    // only the structural expectation can refuse it — and each refusal must NAME
    // what it could not read.
    // The DUPLICATE-key case is here for a reason worth naming: this reader
    // keeps the FIRST occurrence and the shared `serde_json::Value` parse
    // downstream keeps the LAST, so choosing either would rewrite one value and
    // compare the other.
    const ROWS: &str = "`rows` must be an array of JSON objects";
    const CELLS: &str = "`cells` must be an array of JSON objects";
    const NAME: &str = "`name` must be a JSON string";
    #[rustfmt::skip]
    let structural = [
        (r#"[1,2]"#, "one JSON object"),
        (r#"{"rows":7}"#, ROWS),
        (r#"{"rows":[7]}"#, ROWS),
        (r#"{"rows":[{"cells":7}]}"#, CELLS),
        (r#"{"rows":[{"cells":[7]}]}"#, CELLS),
        (r#"{"rows":[{"cells":[{"value":1}]}]}"#, "has no `name`"),
        (r#"{"rows":[{"cells":[{"name":"a","value":1,"value":2}]}]}"#, "duplicate key"),
        (r#"{"rows":[{"cells":[{"name":7,"value":1}]}]}"#, NAME),
    ];
    for (bad, needle) in structural {
        let err = preserve_exact_lexemes(&format!("{bad}\n"), &[])
            .expect_err("a line whose sstabledump structure does not hold must be refused");
        assert!(
            err.contains(needle),
            "the refusal for {bad:?} must name what it could not read ({needle:?}), got: {err}"
        );
        assert!(
            err.starts_with("line 1:"),
            "the refusal must name the line, got: {err}"
        );
    }

    // Valid JSON the corpus uses, including escapes and unicode, round-trips.
    for good in [
        r#"{"a":"a \"quoted\" \\ é value","b":[null,true,false,-0.5,1E+3]}"#,
        r#"{"nested":{"deep":[{"x":1}]}}"#,
    ] {
        let out = preserve_exact_lexemes(&format!("{good}\n"), &[]).expect("valid JSON must parse");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(out.trim()).expect("re-emit"),
            serde_json::from_str::<serde_json::Value>(good).expect("original"),
            "the re-emitted text must denote the same JSON"
        );
    }

    assert_eq!(
        placeholder_marker(r#"{"cells":[{"value":"TODO"}]}"#),
        Some("\"TODO\"")
    );
    assert_eq!(placeholder_marker(r#"{"cells":[{"value":0.1}]}"#), None);
}

/// A DUPLICATE key is refused at EVERY depth — including inside a value no
/// descent opens, which is where the check used to have a hole.
///
/// The state this control exists for: duplicate-key detection used to live only
/// in the `RawObject` deserializer, which sees an object only when the lexeme
/// descent actually DESERIALIZES it. A UDT value is returned as the IDENTITY (its
/// field types are not declared to the harness), so its interior was never
/// opened and a duplicate key inside it was invisible — the later shared
/// `serde_json::Value` parse then silently selected ONE occurrence and the
/// golden passed this stage. That is worst for the #3556 known-gap cases, whose
/// value comparison is deferred: there the golden stage is the ONLY thing that
/// would notice a malformed golden at all.
#[test]
fn a_duplicate_key_is_refused_at_every_depth_including_inside_a_udt_value() {
    use parquet_parity::cql_type::parse_column;
    use parquet_parity::golden_text::preserve_exact_lexemes;

    // The declared columns of the line below, so the descent runs for real: a
    // UDT (returned as the identity — the arm with the hole) and a `decimal`
    // (rewritten, the arm that already had a local check).
    let columns = ["p decimal", "u profile"]
        .iter()
        .map(|d| {
            let (name, declared) = d.split_once(' ').expect("name and declared type");
            parse_column(name, declared, &["profile"]).expect("declared type must parse")
        })
        .collect::<Vec<_>>();

    let line = |cells: &str| {
        format!(r#"{{"partition":{{"key":["1"]}},"rows":[{{"type":"row","cells":{cells}}}]}}"#)
    };

    // Every one of these is well-formed JSON, so ONLY the duplicate-key refusal
    // can red on it. Each names a position the old local check did not reach.
    #[rustfmt::skip]
    let duplicated = [
        // Inside a UDT value — the identity arm, the hole itself.
        (r#"[{"name":"u","value":{"amount":1,"amount":2}}]"#, "amount"),
        // Deeper inside a UDT value: a nested object and an array element.
        (r#"[{"name":"u","value":{"inner":{"k":1,"k":2}}}]"#, "k"),
        (r#"[{"name":"u","value":{"xs":[{"k":1,"k":2}]}}]"#, "k"),
        // A cell of a column the case does NOT declare, which `rewrite_cell`
        // leaves untouched, so nothing there was ever deserialized either.
        (r#"[{"name":"undeclared","value":{"k":1,"k":2}}]"#, "k"),
        // Inside a DECLARED decimal's own cell object, and in the row and the
        // top-level object, which the descent does open — these must stay
        // refused too.
        (r#"[{"name":"p","value":1,"tstamp":"a","tstamp":"b"}]"#, "tstamp"),
    ];
    for (cells, key) in duplicated {
        let err = preserve_exact_lexemes(&format!("{}\n", line(cells)), &columns)
            .expect_err("a duplicate key must be REFUSED, never resolved to one occurrence");
        assert!(
            err.contains("duplicate key") && err.contains(key),
            "the refusal must name the duplicated key {key:?}, got: {err}"
        );
        assert!(
            err.starts_with("line 1:"),
            "the refusal must name the line, got: {err}"
        );
    }

    // A duplicate in the ROW object and in the TOP-LEVEL object.
    for (bad, key) in [
        (
            r#"{"partition":{"key":["1"]},"rows":[{"type":"row","type":"row","cells":[]}]}"#,
            "type",
        ),
        (
            r#"{"partition":{"key":["1"]},"partition":{"key":["2"]},"rows":[]}"#,
            "partition",
        ),
        // …and inside the partition KEY array's own container.
        (
            r#"{"partition":{"key":["1"],"key":["2"]},"rows":[]}"#,
            "key",
        ),
    ] {
        let err = preserve_exact_lexemes(&format!("{bad}\n"), &columns)
            .expect_err("a duplicate key must be REFUSED wherever it is");
        assert!(
            err.contains("duplicate key") && err.contains(key),
            "the refusal must name the duplicated key {key:?}, got: {err}"
        );
    }

    // The refusal names WHERE the duplicate is, not just which key repeated: a
    // key name alone does not locate one inside a nested UDT value.
    let err = preserve_exact_lexemes(
        &format!(
            "{}\n",
            line(r#"[{"name":"u","value":{"inner":{"k":1,"k":2}}}]"#)
        ),
        &columns,
    )
    .expect_err("refused");
    assert!(
        err.contains("rows[0].cells[0].value.inner"),
        "the refusal must name the JSON path of the offending object, got: {err}"
    );

    // CONTROL: the same lines WITHOUT the duplicate are accepted, so the check
    // reds on the duplicate and not on the shape.
    for good in [
        r#"[{"name":"u","value":{"amount":1,"other":2}}]"#,
        r#"[{"name":"u","value":{"inner":{"k":1,"j":2}}}]"#,
        r#"[{"name":"u","value":{"xs":[{"k":1},{"k":2}]}}]"#,
        r#"[{"name":"p","value":1.25,"tstamp":"a"}]"#,
    ] {
        preserve_exact_lexemes(&format!("{}\n", line(good)), &columns).unwrap_or_else(|e| {
            panic!("{good} carries no duplicate key and must be accepted: {e}")
        });
    }
}

// ---------------------------------------------------------------------------
// `golden_rows.rs`: ELIGIBILITY decided from the TEXT, not from what the lenient
// shared parser managed to parse out of it.
// ---------------------------------------------------------------------------

/// A PRESENT-BUT-INVALID eligibility field, and a missing/invalid required
/// container, must be REFUSED — and the control MEASURES the hole it closes by
/// showing the lenient shared parser accepts each one as live data.
///
/// The state this control exists for: the #1742 refusals (a deletion, a TTL) were
/// decided from fields the `cqlite-core`-owned parser had parsed SUCCESSFULLY,
/// and that parser reads every optional field through
/// `get(..).and_then(as_str/as_i64/as_array)` — so `"ttl": "3600"` became
/// `ttl_secs: None`, `"deletion_info": 7` became "no deletion" and `"rows": 7`
/// became an empty row list. A malformed golden therefore read as live data, and
/// the refusal that makes this oracle sound never fired.
#[test]
fn a_present_but_invalid_eligibility_field_is_refused_not_read_as_absent() {
    use parquet_parity::canonical_jsonl::{parse_document_str_with_keys, KeySpec};
    use parquet_parity::cql_type::parse_column;
    use parquet_parity::golden_rows::{project_golden, validate_golden_text};
    use std::path::Path;

    let columns = [("id", "int"), ("v", "text")]
        .iter()
        .map(|(n, d)| parse_column(n, d, &[]).expect("declared type must parse"))
        .collect::<Vec<_>>();
    // Does the LENIENT path accept this line as live data? (`Ok` = the hole.)
    let lenient_accepts = |line: &str| -> bool {
        let Ok(doc) = parse_document_str_with_keys(
            &format!("{line}\n"),
            Path::new("<synthetic>"),
            true,
            &KeySpec::from_cql_types(&["int"], &[]),
        ) else {
            return false;
        };
        // `.is_ok()` is the MEASUREMENT this closure exists to take — "does the
        // lenient path accept this line?" — not an unknown collapsed onto a
        // permissive answer. A refusal from either call IS the negative answer.
        project_golden(&doc, &columns, &["id"], &[]).is_ok()
    };

    // Each line is well-formed JSON carrying ONE malformed eligibility-bearing
    // field or container. `needle` is what the refusal must name.
    #[rustfmt::skip]
    let malformed: &[(&str, &str)] = &[
        // A NON-INTEGER row TTL — the sharpest one: a TTL can expire between
        // fixture generation and test time, which is why it disqualifies.
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"row","liveness_info":{"ttl":"3600"},"cells":[{"name":"v","value":"x"}]}]}"#, "a row TTL"),
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"row","liveness_info":{"ttl":true},"cells":[{"name":"v","value":"x"}]}]}"#, "a row TTL"),
        // A NON-INTEGER per-cell TTL.
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"v","value":"x","ttl":"60"}]}]}"#, "a TTL on column 'v'"),
        // A malformed PARTITION deletion marker: `parse_deletion_info` returns
        // `None` for it, so the partition read as undeleted.
        (r#"{"partition":{"key":["1"],"deletion_info":7},"rows":[{"type":"row","cells":[{"name":"v","value":"x"}]}]}"#, "a partition-level deletion"),
        (r#"{"partition":{"key":["1"],"deletion_info":{"marked_deleted":1700000000}},"rows":[{"type":"row","cells":[{"name":"v","value":"x"}]}]}"#, "a partition-level deletion"),
        // …and a malformed ROW deletion marker, the same way.
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"row","deletion_info":"nope","cells":[{"name":"v","value":"x"}]}]}"#, "a row-level deletion"),
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"row","deletion_info":{"local_delete_time":12345},"cells":[{"name":"v","value":"x"}]}]}"#, "a row-level deletion"),
        // A non-object `liveness_info`: every field inside it, including the TTL,
        // became an absence.
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"row","liveness_info":7,"cells":[{"name":"v","value":"x"}]}]}"#, "PRESENT but is not a JSON object"),
        // Required CONTAINERS. A non-array or absent one became an EMPTY one, so
        // the partition/row silently contributed nothing to the oracle.
        (r#"{"partition":{"key":["1"]},"rows":7}"#, "PRESENT but is not a JSON array"),
        (r#"{"partition":{"key":["1"]}}"#, "has no `rows`"),
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":7}]}"#, "PRESENT but is not a JSON array"),
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"row"}]}"#, "has no `cells`"),
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"row","clustering":7,"cells":[]}]}"#, "PRESENT but is not a JSON array"),
        // The TIMESTAMPS a collection-shell shadowing decision reads. A
        // non-string parses to `None`, which moves that decision rather than
        // reporting a malformed marker.
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"row","liveness_info":{"tstamp":17},"cells":[{"name":"v","value":"x"}]}]}"#, "PRESENT but is not a JSON string"),
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"v","value":"x","tstamp":17}]}]}"#, "PRESENT but is not a JSON string"),
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"v","value":"x","deletion_info":7}]}]}"#, "PRESENT but is not a JSON object"),
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"v","value":"x","deletion_info":{"local_delete_time":7}}]}]}"#, "PRESENT but is not a JSON string"),
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"v","value":"x","path":7}]}]}"#, "PRESENT but is not a JSON array"),
        // Shapes the harness cannot classify at all.
        (r#"{"partition":7,"rows":[]}"#, "PRESENT but is not a JSON object"),
        (r#"{"rows":[]}"#, "has no `partition`"),
        (r#"{"partition":{"key":7},"rows":[]}"#, "PRESENT but is not a JSON array"),
        (r#"{"partition":{"key":["1"]},"rows":[7]}"#, "`rows` must hold JSON objects"),
        (r#"{"partition":{"key":["1"]},"rows":[{"cells":[]}]}"#, "has no `type`"),
        (r#"{"partition":{"key":["1"]},"rows":[{"type":7,"cells":[]}]}"#, "PRESENT but is not a JSON string"),
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"sideways","cells":[]}]}"#, "unrecognized sstabledump entry type"),
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"static_block","cells":[]}]}"#, "a 'static_block' entry"),
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"range_tombstone_bound","cells":[]}]}"#, "a 'range_tombstone_bound' entry"),
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[7]}]}"#, "`cells` must hold JSON objects"),
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"value":"x"}]}]}"#, "has no `name`"),
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":7}]}]}"#, "PRESENT but is not a JSON string"),
    ];
    for (line, needle) in malformed {
        let err = validate_golden_text(&format!("{line}\n"))
            .expect_err("a present-but-invalid eligibility field must be REFUSED");
        assert!(
            err.contains(needle),
            "the refusal must name what it found ({needle:?}), got: {err}"
        );
        assert!(
            err.starts_with("line 1:"),
            "the refusal must name the line, got: {err}"
        );
    }

    // MEASURED, not argued: for each of these the LENIENT path — the one this
    // check runs in front of — accepts the line as live data. That is the hole,
    // and it is why the eligibility decision cannot be taken from parsed fields.
    #[rustfmt::skip]
    let lenient_holes: &[&str] = &[
        r#"{"partition":{"key":["1"]},"rows":[{"type":"row","liveness_info":{"ttl":"3600"},"cells":[{"name":"v","value":"x"}]}]}"#,
        r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"v","value":"x","ttl":"60"}]}]}"#,
        r#"{"partition":{"key":["1"],"deletion_info":7},"rows":[{"type":"row","cells":[{"name":"v","value":"x"}]}]}"#,
        r#"{"partition":{"key":["1"],"deletion_info":{"marked_deleted":1700000000}},"rows":[{"type":"row","cells":[{"name":"v","value":"x"}]}]}"#,
        r#"{"partition":{"key":["1"]},"rows":[{"type":"row","deletion_info":"nope","cells":[{"name":"v","value":"x"}]}]}"#,
        r#"{"partition":{"key":["1"]},"rows":[{"type":"row","liveness_info":7,"cells":[{"name":"v","value":"x"}]}]}"#,
        r#"{"partition":{"key":["1"]},"rows":7}"#,
        r#"{"partition":{"key":["1"]}}"#,
        r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":7}]}"#,
        r#"{"partition":{"key":["1"]},"rows":[{"type":"row"}]}"#,
    ];
    for line in lenient_holes {
        assert!(
            lenient_accepts(line),
            "this control's premise is that the lenient parser ACCEPTS {line} as live data; if \
             it no longer does, re-derive what this check is still protecting"
        );
    }

    // POSITIVE control: an eligible, well-formed line passes — so the check reds
    // on the defect and not on the shape of an ordinary golden.
    for good in [
        r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"v","value":"x"}]}]}"#,
        r#"{"partition":{"key":["1"],"position":0},"rows":[{"type":"row","position":9,"clustering":[],"liveness_info":{"tstamp":"2025-01-01T00:00:00Z"},"cells":[{"name":"v","value":"x","tstamp":"2025-01-01T00:00:00Z","path":["k"]}]}]}"#,
        // A collection-shell deletion is NOT disqualifying — `project_column`
        // decides shadowing from it — so a well-formed one must be accepted.
        r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"v","deletion_info":{"local_delete_time":"2025-01-01T00:00:00Z"},"tstamp":"2025-01-01T00:00:00Z"}]}]}"#,
        // Blank lines are skipped, as the loader skips them.
        "",
    ] {
        validate_golden_text(&format!("{good}\n"))
            .unwrap_or_else(|e| panic!("an eligible well-formed line must be accepted: {e}"));
    }
}

// ---------------------------------------------------------------------------
// A PRESENT cell with NO `value` is not a NULL (#1490 round 15)
//
// A Cassandra NULL is the ABSENCE OF A CELL: sstabledump omits the cell and the
// projection answers `Absent`. A cell that is PRESENT but carries no `value` is
// a DIFFERENT state, and the shared parser renders it as the SAME
// `CanonicalValue::Absent` (`parse_cell` maps a missing `value` key to it). Read
// that way, a golden that lost a value AGREES with an export that wrongly writes
// NULL — so the harness would report parity for the silent NULL-coercion it was
// built to catch (AC1, #1485).
//
// So `project_column` decides the two apart — presence IS representable, a
// present cell being an entry in the column's cell list — and REFUSES the second
// unless the cell is one of the shapes whose value legitimately lives elsewhere
// (a tombstone, a collection-shell deletion, or a non-frozen SET element whose
// value is its stringified `path`).
//
// Measured on the committed corpus, not argued: across all 162 `*-Data.db.jsonl`
// goldens, every one of the 5,812 cells carrying no `value` also carries a
// `deletion_info`. The refusal therefore reds on the defect, not on the shape of
// an ordinary golden.
// ---------------------------------------------------------------------------

/// The declared columns the value-bearing cases use: an `int` partition key, a
/// `text` scalar, a `frozen<map<text,text>>`, a `list<int>`, a `map<int,text>`
/// and a `set<int>` — one per POSITION at which a cell's value is consumed —
/// plus the three positions at which a NESTED value sits: a `frozen<list<int>>`
/// element, a `frozen<tuple<int,int>>` member and a `frozen<person>` UDT field.
/// The last two are the only positions where CQL permits a null, so they are
/// what keeps the null refusal from being "refuse every null".
fn shape_columns() -> Vec<parquet_parity::cql_type::ColumnType> {
    [
        ("id", "int"),
        ("v", "text"),
        ("fm", "frozen<map<text,text>>"),
        ("l", "list<int>"),
        ("m", "map<int,text>"),
        ("s", "set<int>"),
        ("fl", "frozen<list<int>>"),
        ("t", "frozen<tuple<int,int>>"),
        ("p", "frozen<person>"),
    ]
    .iter()
    .map(|(n, t)| {
        parquet_parity::cql_type::parse_column(n, t, &["person"]).expect("declared type parses")
    })
    .collect()
}

/// Project ONE synthetic sstabledump line through the REAL parser and the REAL
/// projection, over [`shape_columns`].
fn project_shape_line(line: &str) -> Result<Vec<parquet_parity::golden_rows::GoldenRow>, String> {
    use parquet_parity::canonical_jsonl::{parse_document_str_with_keys, KeySpec};

    let columns = shape_columns();
    let doc = parse_document_str_with_keys(
        &format!("{line}\n"),
        std::path::Path::new("<synthetic value-shape golden>"),
        true,
        &KeySpec::from_cql_types(&["int"], &[]),
    )
    .map_err(|e| format!("the synthetic golden must parse: {e}"))?;
    parquet_parity::golden_rows::project_golden(&doc, &columns, &["id"], &[])
}

/// THE finding: at every position where a cell's `value` is what carries the
/// column's data, a PRESENT cell with no `value` is REFUSED — never read as
/// NULL.
#[test]
fn a_present_cell_with_no_value_is_refused_rather_than_read_as_null() {
    // Each line is well-formed JSON whose ONE malformed cell is present and
    // carries neither `value` nor `deletion_info`. `column` is what the refusal
    // must name.
    #[rustfmt::skip]
    let malformed: &[(&str, &str)] = &[
        // A scalar `text` cell.
        (r#"{"partition":{"key":["7"]},"rows":[{"type":"row","cells":[{"name":"v"}]}]}"#, "v"),
        // A FROZEN collection is one cell too, and its value is that cell's.
        (r#"{"partition":{"key":["7"]},"rows":[{"type":"row","cells":[{"name":"fm"}]}]}"#, "fm"),
        // A non-frozen LIST element: its value is the cell's `value` (only the
        // element's identity is in the `path`).
        (r#"{"partition":{"key":["7"]},"rows":[{"type":"row","cells":[{"name":"l","path":["6ac52100-a251-11f0-a3fe-f1a551383fb9"]}]}]}"#, "l"),
        // A MAP entry: the KEY is the stringified `path`, the entry's VALUE is
        // the cell's `value` — so a map cell with no `value` has lost the value
        // half of the entry.
        (r#"{"partition":{"key":["7"]},"rows":[{"type":"row","cells":[{"name":"m","path":["5"]}]}]}"#, "m"),
        // A path-less cell on a non-frozen collection with no deletion marker is
        // neither a shell deletion nor an element: it would have contributed
        // NOTHING, leaving the whole collection to project as NULL.
        (r#"{"partition":{"key":["7"]},"rows":[{"type":"row","cells":[{"name":"m"}]}]}"#, "m"),
        (r#"{"partition":{"key":["7"]},"rows":[{"type":"row","cells":[{"name":"s"}]}]}"#, "s"),
    ];
    for (line, column) in malformed {
        let err = project_shape_line(line)
            .expect_err("a PRESENT cell carrying no `value` must be REFUSED, not read as NULL");
        assert!(
            err.contains("PRESENT but carries no `value`"),
            "the refusal must say what it found, got: {err}"
        );
        assert!(
            err.contains(&format!("column '{column}'")),
            "the refusal must name the column ('{column}'), got: {err}"
        );
        assert!(
            err.contains("partition 0 row 0"),
            "the refusal must name the partition and row, got: {err}"
        );
        assert!(
            err.contains("#1485"),
            "the refusal must name the defect it prevents (AC1, #1485), got: {err}"
        );
    }
}

/// The CONTROLS, in the same projection: each shape that legitimately carries no
/// `value` still projects exactly as before, so the refusal above reds on the
/// defect and not on an ordinary golden.
#[test]
fn an_absent_cell_a_tombstone_and_a_set_element_still_project_as_before() {
    use parquet_parity::canonical_jsonl::CanonicalValue;

    // CONTROL 1 — a Cassandra NULL is the ABSENCE of a cell: `v` has a cell,
    // every other column has none, and they project to `Absent`.
    let rows = project_shape_line(
        r#"{"partition":{"key":["7"]},"rows":[{"type":"row","cells":[{"name":"v","value":"x"}]}]}"#,
    )
    .expect("a row whose other columns have NO cell must still project");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cells.get("v"),
        Some(&CanonicalValue::Text("x".to_string()))
    );
    for null_column in ["fm", "l", "m", "s"] {
        assert_eq!(
            rows[0].cells.get(null_column),
            Some(&CanonicalValue::Absent),
            "a column with NO cell is a Cassandra NULL and must still project as Absent"
        );
    }

    // CONTROL 2 — a real collection-shell deletion (`deletion_info`, no `value`)
    // is still read as the shadowing marker it is: the element written AFTER it
    // survives, and the element at-or-before it is shadowed away.
    let rows = project_shape_line(
        r#"{"partition":{"key":["7"]},"rows":[{"type":"row","cells":[
             {"name":"m","deletion_info":{"local_delete_time":"2025-01-01T00:00:00Z"},"tstamp":"2025-01-01T00:00:00Z"},
             {"name":"m","path":["5"],"value":"after","tstamp":"2025-01-02T00:00:00Z"},
             {"name":"m","path":["6"],"value":"before","tstamp":"2024-12-31T00:00:00Z"}
           ]}]}"#
            .replace('\n', " ")
            .as_str(),
    )
    .expect("a well-formed collection-shell deletion must still project");
    assert_eq!(
        rows[0].cells.get("m"),
        Some(&CanonicalValue::Map(vec![(
            CanonicalValue::Int(5),
            CanonicalValue::Text("after".to_string())
        )])),
        "the shell deletion must still shadow the older element and keep the newer one"
    );

    // CONTROL 3 — a non-frozen SET element's value IS its stringified `path`, so
    // it legitimately carries no `value` and must still project from the path.
    let rows = project_shape_line(
        r#"{"partition":{"key":["7"]},"rows":[{"type":"row","cells":[{"name":"s","path":["5"]}]}]}"#,
    )
    .expect("a set element carries its value in its `path` and must still project");
    assert_eq!(
        rows[0].cells.get("s"),
        Some(&CanonicalValue::List(vec![CanonicalValue::Int(5)])),
        "a set element must still be projected from its path"
    );
}

/// THE NEGATIVE the fix exists for, measured end to end: a golden that lost a
/// value and an export that wrongly writes NULL used to compare EQUAL.
///
/// The first half MEASURES the false PASS — it hands `compare` exactly what the
/// pre-fix projection produced for the malformed cell (`Absent`) against a real
/// Arrow NULL, and the comparison PASSES, counting the cell as compared. The
/// second half shows the same golden TEXT can no longer reach `compare` at all:
/// the projection refuses it. Without the first half the fix is unverified;
/// without the second the harness is still blessing the defect.
#[test]
fn a_golden_missing_a_value_and_an_export_null_can_no_longer_compare_equal() {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet_parity::canonical_jsonl::CanonicalValue;
    use parquet_parity::cql_type::parse_column;
    use parquet_parity::golden_rows::GoldenRow;
    use parquet_parity::{compare, project_rows_for_test, CaseOutcome, ParityCase, SchemaCheck};

    const CASE: ParityCase = ParityCase {
        keyspace: "test_value_shape",
        table: "synthetic",
        schema: "da-test.cql",
        udts: &[],
        columns: &[("id", "int"), ("v", "text")],
        partition_key: &["id"],
        clustering: &[],
        schema_check: SchemaCheck::Synthetic {
            why: "a hand-built RecordBatch and a literal golden line — no committed schema \
                  declares it",
        },
        must_run: false,
        covers: "CONTROL: a golden cell with no `value` must not compare equal to an export NULL",
        known_gap: None,
        known_type_gaps: &[],
    };
    let columns = CASE
        .columns
        .iter()
        .map(|(n, t)| parse_column(n, t, &[]).expect("declared type parses"))
        .collect::<Vec<_>>();

    // The EXPORT side: `v` exported as a real Arrow NULL — the silent
    // NULL-coercion of AC1 (#1485).
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("v", DataType::Utf8, true),
    ]);
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![7i32])),
        Arc::new(StringArray::from(vec![None::<&str>])),
    ];
    let batch = RecordBatch::try_new(Arc::new(schema), cols).expect("synthetic batch");
    let exported = project_rows_for_test(&CASE, &[batch], &columns, &[], false)
        .expect("the export side must project");
    assert_eq!(
        exported[0].cell("v"),
        Some(&CanonicalValue::Absent),
        "this test's premise is that the export wrote NULL for 'v'"
    );

    // HALF 1 — the false PASS, measured: the value the PRE-FIX projection
    // produced for a cell with no `value` was `Absent`, and against the export's
    // NULL that compares EQUAL and is COUNTED as covered.
    let mut cells = std::collections::BTreeMap::new();
    cells.insert("id".to_string(), CanonicalValue::Int(7));
    cells.insert("v".to_string(), CanonicalValue::Absent);
    let pre_fix_golden = vec![GoldenRow {
        keys: vec![CanonicalValue::Int(7)],
        cells,
    }];
    match compare(&CASE, &columns, pre_fix_golden, exported)
        .expect("the premise of this test is that Absent-vs-NULL compares EQUAL")
    {
        CaseOutcome::Ran { rows, cells } => assert_eq!(
            (rows, cells),
            (1, 2),
            "the false PASS this fix removes: the malformed cell was compared and PASSED"
        ),
        CaseOutcome::Skipped(why) => panic!("must not skip: {why}"),
    }

    // HALF 2 — that golden TEXT can no longer produce those rows: the
    // projection REFUSES it, so the pair never reaches `compare`.
    use parquet_parity::canonical_jsonl::{parse_document_str_with_keys, KeySpec};
    let doc = parse_document_str_with_keys(
        r#"{"partition":{"key":["7"]},"rows":[{"type":"row","cells":[{"name":"v"}]}]}
"#,
        std::path::Path::new("<synthetic golden missing a value>"),
        true,
        &KeySpec::from_cql_types(&["int"], &[]),
    )
    .expect("the synthetic golden must parse");
    let err = parquet_parity::golden_rows::project_golden(&doc, &columns, &["id"], &[])
        .expect_err("the golden that lost its value must be REFUSED, not compared as NULL");
    assert!(
        err.contains("column 'v'") && err.contains("#1485"),
        "the refusal must name the column and the defect it prevents: {err}"
    );
}

// ---------------------------------------------------------------------------
// "NO AUTHORITATIVE VALUE" IS ONE STATE WITH TWO SPELLINGS (#1490 round 15)
//
// Round 14 refused a present cell carrying no `value` KEY. The same defect is
// reachable by a second spelling — `"value": null` — which the shared parser
// renders as `CanonicalValue::Null` and `fold_null` then folds into the very
// same `Absent`. So the refusal is now asked of the PROPERTY (does this position
// carry an authoritative value?) and of the POSITION (may a value be absent
// HERE?), not of the spelling: `golden_rows::carries_no_authoritative_value`
// covers both cell spellings, and `declared::Position::permits_absence` decides
// every nested position from a CLOSED set of three exceptions.
//
// The exceptions are CQL rules, not conveniences: a collection may not hold a
// null element, a map may not hold a null key or value, and no primary-key or
// collection-path component may be null — while a UDT field and a tuple member
// legitimately may, and the committed corpus contains one
// (`test_compactionparityudt/udt_frozen_person` dumps
// `{"first_name":"Edsger","last_name":null,"age":75}`).
// ---------------------------------------------------------------------------

/// THE finding: at every position CQL requires to hold a value, an explicit
/// `"value": null` is REFUSED — exactly like the missing-`value` spelling it
/// used to slip past.
#[test]
fn an_explicit_null_is_refused_wherever_cql_requires_a_value() {
    // (line, what the refusal must name, the phrase identifying WHICH refusal
    // fired) — the cell-level check owns a cell's own `value`, the
    // position-level check owns everything nested inside one.
    #[rustfmt::skip]
    let malformed: &[(&str, &str, &str)] = &[
        // A scalar `text` cell — the top-level cell position.
        (r#"{"partition":{"key":["7"]},"rows":[{"type":"row","cells":[{"name":"v","value":null}]}]}"#,
         "v", "PRESENT but carries no `value`"),
        // A FROZEN collection is one cell, and its whole value is that cell's.
        (r#"{"partition":{"key":["7"]},"rows":[{"type":"row","cells":[{"name":"fm","value":null}]}]}"#,
         "fm", "PRESENT but carries no `value`"),
        // A non-frozen LIST element — a COLLECTION MEMBER, dumped as its own
        // cell whose `value` is the element.
        (r#"{"partition":{"key":["7"]},"rows":[{"type":"row","cells":[{"name":"l","path":["6ac52100-a251-11f0-a3fe-f1a551383fb9"],"value":null}]}]}"#,
         "l", "PRESENT but carries no `value`"),
        // A non-frozen MAP entry — the entry's VALUE half.
        (r#"{"partition":{"key":["7"]},"rows":[{"type":"row","cells":[{"name":"m","path":["5"],"value":null}]}]}"#,
         "m", "PRESENT but carries no `value`"),
        // A FROZEN map's VALUE — a collection member NESTED inside one cell, so
        // the cell-level check cannot see it and the POSITION decides.
        (r#"{"partition":{"key":["7"]},"rows":[{"type":"row","cells":[{"name":"fm","value":{"a":null}}]}]}"#,
         "fm", "CQL requires a value at this position"),
        // A FROZEN list's ELEMENT, likewise nested.
        (r#"{"partition":{"key":["7"]},"rows":[{"type":"row","cells":[{"name":"fl","value":[1,null]}]}]}"#,
         "fl", "CQL requires a value at this position"),
        // A PRIMARY-KEY component: never null in Cassandra, so a null there is
        // a malformed golden however harmless it looks.
        (r#"{"partition":{"key":[null]},"rows":[{"type":"row","cells":[{"name":"v","value":"x"}]}]}"#,
         "id", "CQL requires a value at this position"),
    ];
    for (line, named, which) in malformed {
        let err = project_shape_line(line).expect_err(
            "an explicit `null` at a position CQL requires to hold a value must be REFUSED",
        );
        assert!(
            err.contains(which),
            "the refusal must be the one that owns this position ({which}), got: {err}"
        );
        assert!(
            err.contains(&format!("'{named}'")),
            "the refusal must name the column ('{named}'), got: {err}"
        );
        assert!(
            err.contains("#1485"),
            "the refusal must name the defect it prevents (AC1, #1485), got: {err}"
        );
    }
}

/// The CONTROLS for the refusal above: the positions where Cassandra
/// LEGITIMATELY emits a null still project, and a genuinely ABSENT cell (a
/// Cassandra NULL) is still `Absent`. Without these the refusal could be
/// "refuse every null", which would red on the committed corpus — which does
/// contain one (`udt_frozen_person` dumps `"last_name":null`).
#[test]
fn a_legitimate_nested_null_is_still_accepted_and_an_absent_cell_is_still_null() {
    use parquet_parity::canonical_jsonl::CanonicalValue;

    // CONTROL 1 — a UDT FIELD: CQL permits a null field, and sstabledump writes
    // it as an explicit `null` inside the UDT's one JSON object. This is the
    // shape the real `test_compactionparityudt/udt_frozen_person` golden
    // carries, verbatim.
    let rows = project_shape_line(
        r#"{"partition":{"key":["7"]},"rows":[{"type":"row","cells":[{"name":"p","value":{"first_name":"Edsger","last_name":null,"age":75}}]}]}"#,
    )
    .expect("a UDT field's legitimate nested null must still project");
    assert_eq!(
        rows[0].cells.get("p"),
        Some(&CanonicalValue::Tuple(vec![
            (
                "first_name".to_string(),
                CanonicalValue::Text("Edsger".to_string())
            ),
            ("last_name".to_string(), CanonicalValue::Absent),
            ("age".to_string(), CanonicalValue::Int(75)),
        ])),
        "a null UDT field must still fold to the one Arrow null"
    );

    // CONTROL 2 — a TUPLE MEMBER: CQL permits a null member, and sstabledump
    // writes it positionally.
    let rows = project_shape_line(
        r#"{"partition":{"key":["7"]},"rows":[{"type":"row","cells":[{"name":"t","value":[null,2]}]}]}"#,
    )
    .expect("a tuple member's legitimate null must still project");
    assert_eq!(
        rows[0].cells.get("t"),
        Some(&CanonicalValue::List(vec![
            CanonicalValue::Absent,
            CanonicalValue::Int(2)
        ])),
        "a null tuple member must still fold to the one Arrow null"
    );

    // CONTROL 3 — a genuinely ABSENT cell is the Cassandra NULL and must still
    // project as `Absent`: the refusal is about a cell that is THERE and carries
    // nothing, never about a column with no cell.
    let rows = project_shape_line(
        r#"{"partition":{"key":["7"]},"rows":[{"type":"row","cells":[{"name":"v","value":"x"}]}]}"#,
    )
    .expect("a row whose other columns have NO cell must still project");
    for null_column in ["fm", "l", "m", "s", "fl", "t", "p"] {
        assert_eq!(
            rows[0].cells.get(null_column),
            Some(&CanonicalValue::Absent),
            "a column with NO cell is a Cassandra NULL and must still project as Absent"
        );
    }
}

// ---------------------------------------------------------------------------
// A PRESERVED NUMBER LEXEME MUST BE UNFORGEABLE (#1490 round 15)
//
// `sstabledump` writes a `decimal`/`varint` cell as a bare JSON number, and
// `serde_json` would parse it into an `f64` — which cannot identify the value it
// came from. So the rewrite replaces such a number with the JSON STRING of its
// own literal, and `declared::type_scalar_golden` reads a `Text` at a declared
// `decimal`/`varint` position as that exact literal.
//
// That made a string the GOLDEN wrote indistinguishable from a lexeme the
// rewrite wrote: `"value":"1.2"` for a declared `decimal`, `"value":"123"` for a
// `varint`, even `"value":"decimal(1.2)"` (the canonical tag) canonicalized
// EXACTLY like the numbers they imitate, so a malformed golden compared EQUAL.
//
// A tag or prefix cannot fix it — it would be written into the same JSON text
// the golden controls, i.e. forgeable by exactly the input the defect is about.
// What fixes it is DISJOINTNESS established in the one traversal that does the
// rewriting: every NON-NUMBER token at a lexeme-preserving position is REFUSED,
// so afterwards such a position holds no string the rewrite did not itself
// write, and `NumberLexeme` (whose only constructor takes a verified JSON number
// token, and whose only output is the quoted lexeme) is the sole producer.
// ---------------------------------------------------------------------------

/// The declared columns for the lexeme cases: one `decimal` cell, one `varint`
/// cell, and the two NESTED lexeme positions (a frozen collection ELEMENT and a
/// frozen map VALUE), plus a `text` and a `double` column that must stay
/// untouched.
fn lexeme_columns() -> Vec<parquet_parity::cql_type::ColumnType> {
    [
        ("balance", "decimal"),
        ("big", "varint"),
        ("tags", "frozen<set<decimal>>"),
        ("rates", "frozen<map<text,decimal>>"),
        ("note", "text"),
        ("rate", "double"),
    ]
    .iter()
    .map(|(n, t)| parquet_parity::cql_type::parse_column(n, t, &[]).expect("declared type parses"))
    .collect()
}

/// One synthetic line carrying `cells`, through the REAL rewrite.
fn rewrite_cells(cells: &str) -> Result<String, String> {
    preserve_exact_lexemes(
        &format!(
            r#"{{"partition":{{"key":["k"]}},"rows":[{{"type":"row","clustering":[],"cells":[{cells}]}}]}}{}"#,
            "\n"
        ),
        &lexeme_columns(),
    )
}

/// THE finding: a value that was ALREADY a string at a declared
/// `decimal`/`varint` position is REFUSED, so it can never be read as a
/// preserved numeric lexeme.
#[test]
fn an_original_string_at_a_declared_decimal_or_varint_position_is_refused() {
    #[rustfmt::skip]
    let forged: &[(&str, &str)] = &[
        // The finding's three examples, verbatim.
        (r#"{"name":"balance","value":"1.2"}"#, "a JSON string"),
        (r#"{"name":"big","value":"123"}"#, "a JSON string"),
        // The canonical TAG itself: `is_canonical_text` would have waved this
        // straight through as an already-canonical exact decimal.
        (r#"{"name":"balance","value":"decimal(1.2)"}"#, "a JSON string"),
        // A NESTED lexeme position: a frozen collection's element…
        (r#"{"name":"tags","value":["1.5",2.25]}"#, "a JSON string"),
        // …and a frozen map's value.
        (r#"{"name":"rates","value":{"a":"1.5"}}"#, "a JSON string"),
        // Every other non-number token is refused too — the rule is "the token
        // must BE a number", not "the token must not be a string".
        (r#"{"name":"balance","value":true}"#, "a JSON boolean"),
        (r#"{"name":"balance","value":[1]}"#, "a JSON array"),
        (r#"{"name":"balance","value":{"x":1}}"#, "a JSON object"),
    ];
    for (cells, kind) in forged {
        let err = rewrite_cells(cells).expect_err(
            "a non-number token at a declared decimal/varint position must be REFUSED, never \
             read as a preserved lexeme",
        );
        assert!(
            err.contains(kind),
            "the refusal must name the token it found ({kind}), got: {err}"
        );
        assert!(
            err.contains("bare JSON NUMBER"),
            "the refusal must say what Cassandra writes there, got: {err}"
        );
        assert!(
            err.contains("unforgeable"),
            "the refusal must say why every non-number token is refused, got: {err}"
        );
    }

    // A `text` column may of course hold a decimal-looking string, and a
    // `double`'s literal must still reach serde_json untouched: the refusal is
    // keyed on the DECLARED type of the position, never on the value's spelling.
    let ok = rewrite_cells(
        r#"{"name":"note","value":"decimal(1.2)"},{"name":"rate","value":1014.5449131979983}"#,
    )
    .expect("a `text` column holding a decimal-looking string is not a lexeme position");
    assert!(
        ok.contains(r#"{"name":"note","value":"decimal(1.2)"}"#)
            && ok.contains(r#"{"name":"rate","value":1014.5449131979983}"#),
        "neither column may be rewritten: {ok}"
    );
}

/// THE NEGATIVE this fix exists for, measured: at the READER a forged string and
/// a preserved lexeme are the SAME value — and the forged one can no longer get
/// there.
///
/// HALF 1 measures the indistinguishability (without it the fix is unverified):
/// the canonicalization of `Text("1.2")` at a declared `decimal` cell EQUALS the
/// canonicalization of the lexeme the rewrite produces for the NUMBER `1.2`, and
/// `Text("decimal(1.2)")` lands on the same value again. HALF 2 shows the golden
/// TEXT carrying either string can no longer reach the reader at all.
#[test]
fn a_forged_decimal_string_can_no_longer_reach_the_reader_that_cannot_tell_it_apart() {
    use parquet_parity::canonical_jsonl::CanonicalValue;
    use parquet_parity::cql_type::parse_column;
    use parquet_parity::declared::{canonicalize_golden, Declared};

    let decimal = parse_column("balance", "decimal", &[]).expect("declared type parses");
    let at = || Declared::cell(&decimal.spec, "forged-vs-preserved decimal cell");
    let canon = |text: &str| {
        canonicalize_golden(CanonicalValue::Text(text.to_string()), &at())
            .expect("a Text at a declared decimal cell is read as its literal")
    };

    // HALF 1 — the reader CANNOT tell them apart, MEASURED against what the real
    // rewrite emits for a genuine number: the rewrite turns the NUMBER token
    // `1.2` into the JSON string `"1.2"`, which is byte-identical to what a
    // golden spelling `"value":"1.2"` would have handed the reader. So the
    // reader has nothing left to decide with, and both spellings canonicalize to
    // one value — including the canonical TAG, which `is_canonical_text` waves
    // through as already-canonical.
    let rewritten = rewrite_cells(r#"{"name":"balance","value":1.2}"#)
        .expect("a genuine number token is preserved");
    assert!(
        rewritten.contains(r#"{"name":"balance","value":"1.2"}"#),
        "premise: the rewrite hands the reader the JSON STRING \"1.2\", which is exactly what \
         a forged golden string would hand it: {rewritten}"
    );
    let preserved = canon("1.2");
    assert_eq!(
        canon("decimal(1.2)"),
        preserved,
        "premise: the canonical TAG is read as already-canonical, so it lands on the same value"
    );

    // HALF 2 — neither string can reach that reader: the ONE traversal that
    // rewrites lexemes refuses every non-number token at the position first.
    for forged in ["1.2", "decimal(1.2)"] {
        let err = rewrite_cells(&format!(r#"{{"name":"balance","value":"{forged}"}}"#))
            .expect_err("the forged string must be refused before anything reads a value");
        assert!(
            err.contains("a JSON string"),
            "the refusal must name the token it found, got: {err}"
        );
    }
}

/// The CONTROL: GENUINE numeric literals still survive verbatim and still
/// compare EXACTLY — including the round-10 `f64` collision the exact-decimal
/// path exists for. Without this the refusal above could be "refuse the whole
/// lexeme mechanism".
#[test]
fn genuine_numeric_literals_still_survive_verbatim_and_compare_exactly() {
    use parquet_parity::canonical_jsonl::CanonicalValue;
    use parquet_parity::cql_type::parse_column;
    use parquet_parity::declared::{canonicalize_golden, Declared};

    // Premise, asserted so this control cannot stop being about a collision:
    // these literals are ONE double, so only the preserved TEXT distinguishes
    // them.
    for (a, b) in [
        ("0.100000000000000001", "0.1"),
        ("9007199.254740001", "9007199.254740002"),
    ] {
        assert_eq!(
            a.parse::<f64>().expect("f64").to_bits(),
            b.parse::<f64>().expect("f64").to_bits(),
            "premise: {a} and {b} collide under f64"
        );
    }

    let rewritten = rewrite_cells(concat!(
        r#"{"name":"balance","value":9007199.254740001},"#,
        r#"{"name":"big","value":123456789012345678901234567890},"#,
        r#"{"name":"tags","value":[1.5,2.25]},"#,
        r#"{"name":"rates","value":{"a":0.100000000000000001}},"#,
        r#"{"name":"rate","value":1014.5449131979983}"#
    ))
    .expect("genuine number tokens must still be preserved");
    for expected in [
        r#"{"name":"balance","value":"9007199.254740001"}"#,
        r#"{"name":"big","value":"123456789012345678901234567890"}"#,
        r#"{"name":"tags","value":["1.5","2.25"]}"#,
        r#"{"name":"rates","value":{"a":"0.100000000000000001"}}"#,
        // Untouched: a `double` must reach serde_json's exact parser as written.
        r#"{"name":"rate","value":1014.5449131979983}"#,
    ] {
        assert!(
            rewritten.contains(expected),
            "the preserved lexemes must be verbatim; missing {expected}: {rewritten}"
        );
    }

    // And the two colliding scale-9 literals the export CAN carry stay DISTINCT
    // through the reader — the property the preserved text buys.
    let decimal = parse_column("balance", "decimal", &[]).expect("declared type parses");
    let read = |text: &str| {
        canonicalize_golden(
            CanonicalValue::Text(text.to_string()),
            &Declared::cell(&decimal.spec, "collision control"),
        )
        .expect("a scale-9 literal is representable")
    };
    assert_ne!(
        read("9007199.254740001"),
        read("9007199.254740002"),
        "two distinct decimals sharing one double must stay distinct"
    );
}

// ---------------------------------------------------------------------------
// The golden's structure is validated TOTALLY, not field by named field
// (#1490 round 17)
//
// Three review rounds each found another field the harness CONSUMED before
// anything validated it, and each was the same defect: the field's SHAPE was
// checked and its CONTENT was not. `partition.key` was confirmed to be an array
// without confirming its components are the JSON strings sstabledump writes; a
// `tstamp` was confirmed to be a string without confirming it PARSES. Patching
// the named fields would have produced three more next round, because the
// enumeration WAS the generator.
//
// So `golden_schema.rs` describes the WHOLE structure — JSON type, content
// grammar, requiredness, and, for the two positions it deliberately leaves to
// the declared-type descent, WHO validates them — and walks the golden against
// that description, refusing any field the description does not cover. The
// controls below are the three findings as INSTANCES, each with the FALSE PASS
// it closes MEASURED against the lenient path rather than argued.
// ---------------------------------------------------------------------------

/// One column list, parsed.
fn declared_columns(columns: &[(&str, &str)]) -> Vec<parquet_parity::cql_type::ColumnType> {
    columns
        .iter()
        .map(|(n, d)| {
            parquet_parity::cql_type::parse_column(n, d, &[]).expect("declared type must parse")
        })
        .collect()
}

/// Project ONE synthetic golden line through the LENIENT path — the path the
/// total validation runs in front of — with a single `int` partition key.
fn project_leniently(
    line: &str,
    columns: &[parquet_parity::cql_type::ColumnType],
    key_type: &'static str,
) -> Result<parquet_parity::golden_rows::GoldenRow, String> {
    use parquet_parity::canonical_jsonl::{parse_document_str_with_keys, KeySpec};
    use parquet_parity::golden_rows::project_golden;
    let doc = parse_document_str_with_keys(
        &format!("{line}\n"),
        std::path::Path::new("<synthetic>"),
        true,
        &KeySpec::from_cql_types(&[key_type], &[]),
    )
    .map_err(|e| e.to_string())?;
    let mut rows = project_golden(&doc, columns, &["id"], &[])?;
    assert_eq!(rows.len(), 1, "each control line carries exactly one row");
    Ok(rows.remove(0))
}

/// A `partition.key` or collection `path` component that is a BARE NUMBER or
/// BOOLEAN is a MALFORMED oracle, and is REFUSED — never canonicalized.
///
/// sstabledump writes every one of those components with `json.writeString` over
/// Cassandra's `AbstractType.getString` (`JsonTransformer.serializePartitionKey`
/// and `serializeCell`, cassandra-5.0.8), so a bare numeric or boolean component
/// cannot come from a real dump. The measured consequence of letting one through
/// is below: it canonicalizes to EXACTLY the value the correct stringified
/// spelling produces, so it compares equal to a correct export and the harness
/// reports parity for a golden that is not a dump.
#[test]
fn a_bare_numeric_or_boolean_key_or_path_component_is_refused_not_canonicalized() {
    use parquet_parity::golden_rows::validate_golden_text;

    #[rustfmt::skip]
    let malformed: &[&str] = &[
        // A bare numeric partition-key component.
        r#"{"partition":{"key":[1]},"rows":[{"type":"row","cells":[{"name":"v","value":"x"}]}]}"#,
        // …and a bare boolean one.
        r#"{"partition":{"key":[true]},"rows":[{"type":"row","cells":[{"name":"v","value":"x"}]}]}"#,
        // A bare numeric SET-element path (a set element's value IS its path).
        r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"s","path":[7],"value":""}]}]}"#,
        // …and a bare boolean MAP-key path.
        r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"m","path":[true],"value":3}]}]}"#,
        // A component of the RIGHT kind beside one of the wrong kind: the check
        // is per COMPONENT, not "the array holds at least one string".
        r#"{"partition":{"key":["1",2]},"rows":[{"type":"row","cells":[{"name":"v","value":"x"}]}]}"#,
        // A null component is not a string either.
        r#"{"partition":{"key":[null]},"rows":[{"type":"row","cells":[{"name":"v","value":"x"}]}]}"#,
    ];
    for line in malformed {
        let err = validate_golden_text(&format!("{line}\n"))
            .expect_err("a non-string key/path component must be REFUSED");
        assert!(
            err.contains("is not a JSON string"),
            "the refusal must name what it found: {err}"
        );
        assert!(
            err.contains("writeString") && err.contains("cassandra-5.0.8"),
            "…and cite the authority for the requirement, not CQLite's own code: {err}"
        );
    }

    // MEASURED, not argued: each malformed spelling canonicalizes to EXACTLY the
    // value the CORRECT spelling produces, so downstream nothing can tell them
    // apart — which is why the refusal has to happen here, on the text.
    let scalar = declared_columns(&[("id", "int"), ("v", "text")]);
    let boolkey = declared_columns(&[("id", "boolean"), ("v", "text")]);
    let set = declared_columns(&[("id", "int"), ("s", "set<int>")]);
    let map = declared_columns(&[("id", "int"), ("m", "map<boolean,int>")]);
    #[rustfmt::skip]
    let indistinguishable: &[(&str, &str, &[parquet_parity::cql_type::ColumnType], &str)] = &[
        (
            r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"v","value":"x"}]}]}"#,
            r#"{"partition":{"key":[1]},"rows":[{"type":"row","cells":[{"name":"v","value":"x"}]}]}"#,
            &scalar, "int",
        ),
        (
            r#"{"partition":{"key":["true"]},"rows":[{"type":"row","cells":[{"name":"v","value":"x"}]}]}"#,
            r#"{"partition":{"key":[true]},"rows":[{"type":"row","cells":[{"name":"v","value":"x"}]}]}"#,
            &boolkey, "boolean",
        ),
        (
            r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"s","path":["7"],"value":""}]}]}"#,
            r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"s","path":[7],"value":""}]}]}"#,
            &set, "int",
        ),
        (
            r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"m","path":["true"],"value":3}]}]}"#,
            r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"m","path":[true],"value":3}]}]}"#,
            &map, "boolean",
        ),
    ];
    // The tuple is (correct spelling, malformed spelling, declared columns, the
    // `id` column's declared type — what the key spec is built from).
    for (correct, malformed, columns, key_type) in indistinguishable {
        let good = project_leniently(correct, columns, key_type)
            .expect("the correct stringified spelling projects");
        let bad = project_leniently(malformed, columns, key_type)
            .expect("this control's premise is that the LENIENT path accepts the malformed one");
        assert_eq!(
            format!("{:?}", (good.keys, good.cells)),
            format!("{:?}", (bad.keys, bad.cells)),
            "this control's premise is that the malformed component canonicalizes to the SAME \
             value as the correct one — if it no longer does, re-derive what the refusal is \
             still protecting"
        );
    }

    // POSITIVE controls: every legitimate STRINGIFIED spelling is still
    // accepted, so the refusal reds on the defect and not on the shape. A
    // CLUSTERING component is deliberately NOT constrained — sstabledump writes
    // those with `writeRawValue(type.toJSONString(..))`, so a typed JSON number,
    // a nested array and the unset marker `"*"` are all correct there.
    for good in [
        r#"{"partition":{"key":["1","2"]},"rows":[{"type":"row","cells":[{"name":"v","value":"x"}]}]}"#,
        r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"s","path":["7"],"value":""}]}]}"#,
        r#"{"partition":{"key":["1"]},"rows":[{"type":"row","clustering":[7,"t",["a","b"],"*"],"cells":[{"name":"v","value":"x"}]}]}"#,
    ] {
        validate_golden_text(&format!("{good}\n"))
            .unwrap_or_else(|e| panic!("a legitimate golden line must be accepted: {e}"));
    }
}

/// Every timestamp a shadowing decision reads must PARSE, under the SAME grammar
/// the canonical parser uses — and an unparseable one is REFUSED rather than
/// read as an absent timestamp.
///
/// The fallback is the danger: `parse_cell` turns an unparseable cell `tstamp`
/// into `None`, and `project_column` then falls back to the ROW liveness
/// timestamp to decide whether a collection-shell deletion shadows the element.
/// The measurement below shows that fallback silently DROPPING a live element and
/// turning the whole column into a NULL — the AC1 (#1485) coercion this oracle
/// exists to catch, produced by the oracle itself.
#[test]
fn an_unparseable_timestamp_is_refused_and_never_falls_back_to_the_row_liveness_timestamp() {
    use parquet_parity::canonical_jsonl::{parse_timestamp_micros, CanonicalValue};
    use parquet_parity::golden_rows::validate_golden_text;

    #[rustfmt::skip]
    let malformed: &[&str] = &[
        // A cell writetime.
        r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"v","value":"x","tstamp":"not-a-timestamp"}]}]}"#,
        // The ROW writetime the shadowing decision falls back TO.
        r#"{"partition":{"key":["1"]},"rows":[{"type":"row","liveness_info":{"tstamp":"2025-01-01"},"cells":[{"name":"v","value":"x"}]}]}"#,
        // A collection shell's markedForDeleteAt…
        r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"m","deletion_info":{"marked_deleted":"whenever","local_delete_time":"2025-01-02T00:00:00Z"}}]}]}"#,
        // …and its local delete time.
        r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"m","deletion_info":{"local_delete_time":"2025-01-02"}}]}]}"#,
        // Shapes that are ALMOST the grammar: no `Z`, a 13th month, a
        // non-numeric fraction. Each parses to `None` in the canonical parser.
        r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"v","value":"x","tstamp":"2025-01-01T00:00:00"}]}]}"#,
        r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"v","value":"x","tstamp":"2025-13-01T00:00:00Z"}]}]}"#,
        r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"v","value":"x","tstamp":"2025-01-01T00:00:00.abcZ"}]}]}"#,
    ];
    for line in malformed {
        let err = validate_golden_text(&format!("{line}\n"))
            .expect_err("a timestamp that does not parse must be REFUSED");
        assert!(
            err.contains("timestamp"),
            "the refusal must name what it found: {err}"
        );
        assert!(
            err.contains("parse_timestamp_micros"),
            "…and name the ONE grammar it shares with the canonical parser, so the two can \
             never disagree about what a timestamp is: {err}"
        );
    }

    // An ABSENT timestamp is the same misclassification by the other route — the
    // parser reads it as `None` too — so a required one that is missing is
    // refused with the same consequence spelled out. Cassandra writes both
    // unconditionally (`serializeRow`, `serializeCell`), so neither can be
    // legitimately absent.
    #[rustfmt::skip]
    let absent: &[&str] = &[
        r#"{"partition":{"key":["1"]},"rows":[{"type":"row","liveness_info":{},"cells":[{"name":"v","value":"x"}]}]}"#,
        r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"m","deletion_info":{"marked_deleted":"2025-01-02T00:00:00Z"}}]}]}"#,
    ];
    for line in absent {
        let err = validate_golden_text(&format!("{line}\n"))
            .expect_err("a required timestamp that is absent must be REFUSED");
        assert!(
            err.contains("has no") && err.contains("absent timestamp"),
            "the refusal must say the absence is not read as one: {err}"
        );
    }

    // The strings above are refused by BOTH sides: each is one
    // `parse_timestamp_micros` returns `None` for, which is what makes the
    // fallback measurement below meaningful. The relation is DIRECTIONAL, not a
    // biconditional — the validator also refuses spellings the parser NORMALIZES
    // into a plausible instant (hour 24, 2025-02-30, a 7th fractional digit),
    // which is the whole subject of
    // `a_spelling_the_shared_parser_normalizes_is_refused_not_normalized`. The
    // direction that must hold unconditionally, and does, is the safe one:
    // nothing this pass ACCEPTS is a string the parser cannot reproduce.
    for refused in [
        "not-a-timestamp",
        "2025-01-01",
        "2025-01-01T00:00:00",
        "2025-13-01T00:00:00Z",
        "2025-01-01T00:00:00.abcZ",
    ] {
        assert!(
            parse_timestamp_micros(refused).is_none(),
            "{refused:?} must be unparseable for this control to mean anything"
        );
    }
    for accepted in [
        "2025-01-01T00:00:00Z",
        // The SPACE separator real sstabledump emits, and a sub-second fraction.
        "2025-10-06 01:12:07.265Z",
    ] {
        assert!(
            parse_timestamp_micros(accepted).is_some(),
            "{accepted:?} is a real sstabledump timestamp and must parse"
        );
    }

    // MEASURED, not argued: with the cell writetime unparseable, the lenient path
    // falls back to the ROW liveness timestamp — which here predates the
    // collection-shell deletion — and the live element is silently DROPPED,
    // turning the column into a NULL. Same golden, one broken timestamp.
    let columns = declared_columns(&[("id", "int"), ("m", "map<text,int>")]);
    let shell = r#"{"name":"m","deletion_info":{"marked_deleted":"2025-01-02T00:00:00Z","local_delete_time":"2025-01-02T00:00:00Z"}}"#;
    let live = format!(
        r#"{{"partition":{{"key":["1"]}},"rows":[{{"type":"row","liveness_info":{{"tstamp":"2025-01-01T00:00:00Z"}},"cells":[{shell},{{"name":"m","path":["k"],"value":7,"tstamp":"2025-01-03T00:00:00Z"}}]}}]}}"#
    );
    let broken = live.replace("2025-01-03T00:00:00Z", "not-a-timestamp");

    let good = project_leniently(&live, &columns, "int").expect("the well-formed golden projects");
    assert!(
        matches!(good.cells.get("m"), Some(CanonicalValue::Map(entries)) if entries.len() == 1),
        "the element's writetime is AFTER the shell deletion, so it is live: {:?}",
        good.cells.get("m")
    );
    let misclassified = project_leniently(&broken, &columns, "int")
        .expect("this control's premise is that the LENIENT path accepts the broken timestamp");
    assert_eq!(
        misclassified.cells.get("m"),
        Some(&CanonicalValue::Absent),
        "this control's premise is that an unparseable cell writetime falls back to the ROW \
         liveness timestamp and misclassifies the live element as shadowed, turning the column \
         into a NULL; if it no longer does, re-derive what the refusal is still protecting"
    );
    // …and the refusal is what stops that golden ever reaching the projection.
    validate_golden_text(&format!("{broken}\n"))
        .expect_err("the broken-timestamp golden must be refused before it is projected");
    validate_golden_text(&format!("{live}\n"))
        .expect("the well-formed golden must still be accepted");
}

/// A field the structure description does not cover is REFUSED — the property
/// that makes the pass TOTAL rather than a list.
///
/// Without it, a field nobody described is a field nobody validated, and the
/// three findings above could be re-created simply by a dump growing a fourth
/// one. The positive control is the full legitimate shape: every field a real
/// sstabledump writes for an eligible row, all at once.
#[test]
fn a_field_the_structure_description_does_not_cover_is_refused() {
    use parquet_parity::golden_rows::validate_golden_text;

    #[rustfmt::skip]
    let undescribed: &[(&str, &str)] = &[
        (r#"{"partition":{"key":["1"]},"rows":[],"surprise":1}"#, "surprise"),
        (r#"{"partition":{"key":["1"],"surprise":1},"rows":[]}"#, "surprise"),
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[],"surprise":1}]}"#, "surprise"),
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"row","liveness_info":{"tstamp":"2025-01-01T00:00:00Z","surprise":1},"cells":[]}]}"#, "surprise"),
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"v","value":"x","surprise":1}]}]}"#, "surprise"),
        (r#"{"partition":{"key":["1"]},"rows":[{"type":"row","cells":[{"name":"m","deletion_info":{"local_delete_time":"2025-01-02T00:00:00Z","surprise":1}}]}]}"#, "surprise"),
    ];
    for (line, needle) in undescribed {
        let err = validate_golden_text(&format!("{line}\n"))
            .expect_err("a field the description does not cover must be REFUSED");
        assert!(
            err.contains("unrecognized field") && err.contains(needle),
            "the refusal must name the field it does not know: {err}"
        );
    }

    // POSITIVE control: the FULL legitimate shape — every field cassandra-5.0.8's
    // `JsonTransformer` writes for an eligible partition — is accepted.
    let full = r#"{"table kind":"REGULAR","partition":{"key":["1","2"],"position":0},"rows":[{"type":"row","position":42,"clustering":[7,"*"],"liveness_info":{"tstamp":"2025-10-06 01:12:07.265Z"},"cells":[{"name":"v","value":"x","tstamp":"2025-10-06 01:12:07.265Z"},{"name":"m","path":["k"],"value":3},{"name":"m","deletion_info":{"marked_deleted":"2025-10-06 01:12:07.265Z","local_delete_time":"2025-10-06 01:12:07.265Z"}}]}]}"#;
    validate_golden_text(&format!("{full}\n"))
        .unwrap_or_else(|e| panic!("the full legitimate sstabledump shape must be accepted: {e}"));
}

/// A directory entry the harness cannot READ refuses the fixture — it is never
/// omitted from the census.
///
/// Discovery counts entries to decide the fixture is UNIQUE (one `*-Data.db`
/// generation, one golden, and the golden derived from THAT generation). A census
/// taken over an incomplete listing can only conclude "fewer", so an entry
/// silently dropped is exactly how a SECOND generation, or a golden belonging to
/// another one, passes as unique. An entry that cannot be read is UNKNOWN, not
/// ABSENT.
#[test]
fn a_directory_entry_the_harness_cannot_read_refuses_the_fixture() {
    // A non-UTF-8 entry name: the deterministic instance of "cannot read this
    // entry". It used to be dropped by a `filter_map(|e| e.file_name().to_str())`.
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        std::fs::write(tmp.path().join("nb-1-big-Data.db"), b"").expect("write");
        std::fs::write(tmp.path().join("nb-1-big-Data.db.jsonl"), b"").expect("write");
        // A name that is not valid UTF-8 — and, deliberately, one that WOULD be a
        // second Data generation if it could be read.
        let mut raw = b"nb-2-\xff-big-Data.db".to_vec();
        let unreadable = tmp.path().join(std::ffi::OsStr::from_bytes(
            std::mem::take(&mut raw).as_slice(),
        ));
        std::fs::write(&unreadable, b"").expect("write non-UTF-8 named entry");

        let err =
            parquet_parity::fixture_root::fixture_in_table_dir("ks.t", tmp.path().to_path_buf())
                .expect_err("an entry the harness cannot read must REFUSE the fixture");
        assert!(
            err.contains("not UTF-8"),
            "the refusal must name what it could not read: {err}"
        );
        assert!(
            err.contains("census"),
            "…and why an unreadable entry is not an absent one: {err}"
        );

        // CONTROL: with that entry removed the SAME directory resolves, so the
        // refusal reds on the unreadable entry and not on the directory.
        std::fs::remove_file(&unreadable).expect("remove");
        let fixture =
            parquet_parity::fixture_root::fixture_in_table_dir("ks.t", tmp.path().to_path_buf())
                .expect("the same directory without the unreadable entry must resolve");
        assert_eq!(fixture.golden, tmp.path().join("nb-1-big-Data.db.jsonl"));
    }

    // A directory that cannot be LISTED at all is the other half of the same
    // refusal. Skipped rather than asserted when the probe shows this process can
    // read it anyway (running as root, or a filesystem that ignores the mode) —
    // asserting there would be asserting the environment, not the harness.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let dir = tmp.path().join("locked");
        std::fs::create_dir(&dir).expect("mkdir");
        std::fs::write(dir.join("nb-1-big-Data.db"), b"").expect("write");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o000)).expect("chmod");
        // A PROBE, legitimately permissive: it asks whether `chmod 000` actually
        // made the directory unreadable for THIS process (it does not, as root,
        // or on a filesystem that ignores the mode). Its permissive branch omits
        // an ADDITIVE case; the non-UTF-8 half above asserts the same refusal
        // deterministically for any uid, so no property goes unmeasured.
        let listable = std::fs::read_dir(&dir).is_ok();
        if !listable {
            let err = parquet_parity::fixture_root::fixture_in_table_dir("ks.t", dir.clone())
                .expect_err("an unlistable fixture directory must be REFUSED");
            assert!(err.contains("cannot read"), "{err}");
        }
        // Restore so the TempDir can clean up.
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    }
}
