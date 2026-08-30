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
    let err = parquet_parity::fixture_in_table_dir("ks.t", tmp.path().to_path_buf())
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
    let fixture = parquet_parity::fixture_in_table_dir("ks.t", tmp.path().to_path_buf())
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
    let err = parquet_parity::fixture_in_table_dir("ks.t", no_golden.path().to_path_buf())
        .expect_err("a fixture with no golden has no oracle");
    assert!(err.contains("golden"), "{err}");

    let two_gens = scratch_table_dir(&[
        "nb-1-big-Data.db",
        "nb-2-big-Data.db",
        "nb-2-big-Data.db.jsonl",
    ]);
    let err = parquet_parity::fixture_in_table_dir("ks.t", two_gens.path().to_path_buf())
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
    use parquet_parity::golden_rows::{project_golden, reject_ineligible_or_malformed_text};
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
        let err = reject_ineligible_or_malformed_text(&format!("{line}\n"))
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
        reject_ineligible_or_malformed_text(&format!("{good}\n"))
            .unwrap_or_else(|e| panic!("an eligible well-formed line must be accepted: {e}"));
    }
}
