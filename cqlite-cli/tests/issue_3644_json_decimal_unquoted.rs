//! The JSON egress emits a `decimal` as an UNQUOTED number, digit for digit
//! (issue #3644 item 3).
//!
//! # The oracle
//!
//! Cassandra, read at the pinned tag — never CQLite's own output:
//!
//! * `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/DecimalType.java:314-317`
//!   — `toJSONString` returns
//!   `Objects.toString(getSerializer().deserialize(buffer), "\"\"")`, i.e. an
//!   UNQUOTED `BigDecimal.toString()`. It deliberately OVERRIDES
//!   `AbstractType.java:186-189`, which is the QUOTING form.
//! * `cassandra-5.0.8:tools/.../JsonTransformer.java:494` — a cell VALUE is
//!   written with `writeRawValue(cellType.toJSONString(...))`, so that text
//!   reaches the document unquoted.
//!
//! The expected DIGITS come from the committed
//! `test_signed_coll.signed_special_collections` `*-Data.db.jsonl`, which
//! `sstabledump` wrote: its `sd` (`set<decimal>`) members live in the cell PATH,
//! spelled by `writeString(type.getString(v))` (`JsonTransformer.java:452`), so
//! the golden carries each value's exact text. The QUOTES around them there are
//! that `getString` path's artifact and not the egress oracle — which is why this
//! test takes the golden's DIGITS and requires the egress to spell them WITHOUT
//! quotes.
//!
//! # Why this test exists beside the `issue_1491` parity lane
//!
//! That lane compares parsed values, and its JSON parse is `serde_json::Value`'s
//! (`i64`/`u64`/`f64`, no `arbitrary_precision`), so a `decimal` past a double's
//! precision — this fixture's 33 significant digits — is gone before the
//! comparison; the lane declares that as
//! `Divergence::ExactDecimalNotCarriedByThisLanesJsonParse`. This test reads the
//! emitted TEXT instead (`serde_json::value::RawValue` keeps each member's
//! lexeme), so the digits are checked exactly. Nothing here round-trips through
//! an `f64`, which would defeat the point.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::value::RawValue;

/// The one committed generation of the fixture this test is about.
///
/// Committed fixtures are resolved CHECKOUT-relative and are fail-closed: this
/// table is git-tracked, so an absent one is a broken checkout, never a skip
/// (CLAUDE.md, issue #3220 — a corpus-conditional skip behind a green suite is
/// the failure mode this rule exists for).
fn fixture_dir(checkout: &Path) -> PathBuf {
    let keyspace_dir = checkout
        .join("test-data")
        .join("datasets")
        .join("sstables")
        .join("test_signed_coll");
    let mut matches: Vec<PathBuf> = std::fs::read_dir(&keyspace_dir)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", keyspace_dir.display()))
        .map(|entry| entry.expect("directory entry").path())
        .filter(|path| {
            path.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("signed_special_collections-"))
        })
        .collect();
    matches.sort();
    assert_eq!(
        matches.len(),
        1,
        "expected exactly ONE committed generation of \
         test_signed_coll.signed_special_collections under {} (a second one would make \
         `--data-dir` ambiguous), found {matches:?}",
        keyspace_dir.display()
    );
    matches.remove(0)
}

/// Every `sd` cell-path token in the golden, in the order `sstabledump` wrote
/// them.
fn golden_sd_tokens(fixture: &Path) -> Vec<String> {
    let mut goldens: Vec<PathBuf> = std::fs::read_dir(fixture)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", fixture.display()))
        .map(|entry| entry.expect("directory entry").path())
        .filter(|path| {
            path.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db.jsonl"))
        })
        .collect();
    goldens.sort();
    assert_eq!(
        goldens.len(),
        1,
        "expected exactly ONE *-Data.db.jsonl in {}, found {goldens:?}",
        fixture.display()
    );
    let text = std::fs::read_to_string(&goldens[0])
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", goldens[0].display()));

    let mut tokens = Vec::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let doc: serde_json::Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("golden line is not JSON: {e}"));
        let rows = doc
            .get("rows")
            .and_then(|r| r.as_array())
            .unwrap_or_else(|| panic!("golden line has no `rows` array"));
        for row in rows {
            let cells = row
                .get("cells")
                .and_then(|c| c.as_array())
                .unwrap_or_else(|| panic!("golden row has no `cells` array"));
            for cell in cells {
                if cell.get("name").and_then(|n| n.as_str()) != Some("sd") {
                    continue;
                }
                // A multicell SET member IS its cell path; a cell carrying no
                // path is the column's complex deletion, which holds no value.
                let Some(path) = cell.get("path").and_then(|p| p.as_array()) else {
                    continue;
                };
                assert_eq!(
                    path.len(),
                    1,
                    "a set<decimal> member has exactly one path component: {path:?}"
                );
                tokens.push(
                    path[0]
                        .as_str()
                        .unwrap_or_else(|| panic!("cell path component is not a string: {path:?}"))
                        .to_string(),
                );
            }
        }
    }
    // Fail closed: an oracle that found nothing proves nothing.
    assert!(
        tokens.len() >= 4,
        "the committed golden must carry the fixture's four `sd` members, found {tokens:?}"
    );
    tokens
}

/// The raw LEXEME of every member of every row's `sd` array, exactly as the
/// egress spelled it.
///
/// `RawValue` is what keeps the digits: parsing these members into a
/// `serde_json::Value` would put each through an `f64` and destroy the very thing
/// under test.
fn cli_sd_lexemes(json: &str) -> Vec<String> {
    let rows: Vec<BTreeMap<String, Box<RawValue>>> = serde_json::from_str(json)
        .unwrap_or_else(|e| panic!("the CLI's JSON egress is not valid JSON: {e}\n{json}"));
    assert!(!rows.is_empty(), "the CLI emitted no rows:\n{json}");
    let mut lexemes = Vec::new();
    for row in &rows {
        let cell = row
            .get("sd")
            .unwrap_or_else(|| panic!("the CLI emitted no `sd` column:\n{json}"));
        let members: Vec<Box<RawValue>> = serde_json::from_str(cell.get())
            .unwrap_or_else(|e| panic!("`sd` is not a JSON array: {e} ({})", cell.get()));
        lexemes.extend(members.into_iter().map(|m| m.get().to_string()));
    }
    lexemes
}

#[test]
fn json_egress_spells_a_decimal_as_an_unquoted_number_digit_for_digit() {
    let checkout = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-cli has a parent directory")
        .to_path_buf();
    let fixture = fixture_dir(&checkout);
    let schema = checkout
        .join("test-data")
        .join("schemas")
        .join("signed-collection-parity.cql");
    assert!(
        schema.is_file(),
        "committed schema {} is missing (see #3148)",
        schema.display()
    );

    let out_dir = tempfile::tempdir().expect("temp dir");
    let out = out_dir.path().join("egress.json");
    let output = Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .arg("--schema")
        .arg(&schema)
        .arg("--data-dir")
        .arg(
            fixture
                .parent()
                .expect("keyspace dir")
                .parent()
                .expect("sstables dir"),
        )
        .arg("export")
        .arg(&out)
        .arg("--format")
        .arg("json")
        .arg("--table")
        .arg("test_signed_coll.signed_special_collections")
        .output()
        .unwrap_or_else(|e| panic!("cannot run the CLI: {e}"));
    assert!(
        output.status.success(),
        "export --format json failed ({:?})\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let json = std::fs::read_to_string(&out)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", out.display()));

    let mut expected = golden_sd_tokens(&fixture);
    let mut emitted = cli_sd_lexemes(&json);
    // Compared as SETS of lexemes: the dump writes a multicell set in comparator
    // order and the reader reads it in storage order, and member ORDER is the
    // `issue_1491` lane's subject, not this one's. The SPELLING is this one's.
    expected.sort();
    emitted.sort();
    assert_eq!(
        emitted, expected,
        "every `sd` member must be spelled exactly as the golden spells it, and \
         UNQUOTED — `DecimalType.toJSONString:314-317` returns a bare \
         BigDecimal.toString(). A quoted member would arrive here wrapped in \
         `\"`.\nemitted document:\n{json}"
    );
    // Belt: the assertion above would also pass if the golden's tokens were
    // themselves quoted, which they are not — state it directly.
    for lexeme in &emitted {
        assert!(
            !lexeme.starts_with('"'),
            "a decimal member must be a JSON NUMBER, not a string: {lexeme}"
        );
    }
}
