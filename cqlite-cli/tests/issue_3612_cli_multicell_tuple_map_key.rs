//! Issue #3612, AC 4 — THE CLI LEG for `m_tuple_udt`.
//!
//! AC 4 names one subject and four surfaces: a MULTICELL map whose key is a
//! `tuple<frozen<key_part>, int>` must read as a STRUCTURED TUPLE KEY through the
//! Rust core, the CLI, and both bindings. Core and Python were covered; this file
//! is the CLI half. The subject is `test_nested_udt_keys.nested_udt_keys`'s
//! `m_tuple_udt map<frozen<tuple<frozen<key_part>, int>>, int>`, whose keys live
//! in the cell PATH because the map is non-frozen.
//!
//! ## What is discriminating here, and what is not
//! `JSONWriter`'s `Map` arm renders each pair as `{"key": …, "value": …}` and
//! renders the KEY through the same `value_to_json` as any other value — so a
//! structurally-decoded key comes out as a JSON ARRAY `[{…}, n]` and an
//! undecoded one comes out as the CLI's `"0x…"` hex STRING. Before #3612 this
//! site returned `Value::Blob` for every composite cell path, so the assertions
//! below are RED-capable on `origin/main`: they are a type discrimination
//! (array vs string), not a formatting nicety.
//!
//! This is NOT true of `cqlite-core`'s own `ToJson`, whose `Map` arm
//! `Display`-stringifies keys — which is exactly why the CLI needs its own leg
//! rather than inheriting the core one's evidence.
//!
//! ## Oracle: the committed sstabledump golden, parsed at run time
//! Expectations are read from `nb-1-big-Data.db.jsonl` (Cassandra's own dump of
//! these bytes), never from what CQLite emits — doctrine #3042, and the same
//! oracle the core half uses. sstabledump renders a composite cell path as a
//! nested join: the tuple's components with `:`, the inner UDT's fields with an
//! escaped `\:`, and `\@` for a NULL field. So `charlie\:3:8` is
//! `tuple(key_part{label: "charlie", rank: 3}, 8)`.
//!
//! The golden parser below is a SECOND COPY of the core half's, deliberately: the
//! two halves assert different renderers over the same bytes, and #3629's CLI/core
//! split records why a shared harness is the wrong shape — a shared one cannot
//! catch the two renderers diverging. Both copies assert their parse ARITY, so a
//! change in sstabledump's escaping reds instead of silently mis-parsing.
//!
//! ## Fixtures are COMMITTED (git-tracked under the checkout) ⇒ fail closed
//! Resolved checkout-relative with no env var and no skip path (#3220): a
//! `CQLITE_DATASETS_ROOT` naming a fleet-local corpus is MEASURED not to carry
//! `test_nested_udt_keys`, so consulting it could only turn this into a silent
//! zero-row pass.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cqlite_cli::config::OutputConfig;
use cqlite_cli::output::JSONWriter;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use serde_json::Value as J;

const KEYSPACE: &str = "test_nested_udt_keys";
const TABLE: &str = "nested_udt_keys";
const COLUMN: &str = "m_tuple_udt";

/// One `m_tuple_udt` key, normalised so the golden and the CLI's JSON can be
/// compared directly: the UDT's `label` and `rank`, then the tuple's second
/// component. `None` is a NULL field.
type TupleKey = (Option<String>, Option<i64>, i64);

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-cli always has a workspace parent directory")
        .to_path_buf()
}

/// The committed corpus root, asserted intact. GLOBS the table directory: a
/// regeneration mints a fresh table UUID, so a hardcoded path would rot.
fn fixture_table_dir(sstables_root: &Path) -> PathBuf {
    let ks = sstables_root.join(KEYSPACE);
    let dirs: Vec<PathBuf> = std::fs::read_dir(&ks)
        .unwrap_or_else(|e| {
            panic!(
                "committed fixture keyspace dir unreadable ({ks:?}): {e} — \
                 {KEYSPACE} is git-tracked, so this is a checkout problem, not a skip"
            )
        })
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(&format!("{TABLE}-")))
        })
        .collect();
    assert_eq!(
        dirs.len(),
        1,
        "expected exactly one {TABLE}-* dir under {ks:?}, got {dirs:?}"
    );
    let has_data = std::fs::read_dir(&dirs[0])
        .unwrap_or_else(|e| panic!("fixture table dir unreadable: {e}"))
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().ends_with("-Data.db"));
    assert!(
        has_data,
        "no *-Data.db under {:?} — the binaries are force-added; a worktree \
         without them would make this test pass on zero rows",
        dirs[0]
    );
    dirs[0].clone()
}

/// Decode one sstabledump composite cell path into a [`TupleKey`].
fn parse_golden_path(path: &str) -> TupleKey {
    // Split on UNESCAPED ':' only; keep both bytes of an escape so `\:` cannot
    // end a component and `\@` survives for the null test below.
    let mut parts: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut chars = path.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            cur.push(c);
            if let Some(n) = chars.next() {
                cur.push(n);
            }
        } else if c == ':' {
            parts.push(std::mem::take(&mut cur));
        } else {
            cur.push(c);
        }
    }
    parts.push(cur);
    assert_eq!(
        parts.len(),
        2,
        "golden tuple path {path:?} must render exactly 2 tuple components"
    );
    let fields: Vec<&str> = parts[0].split("\\:").collect();
    assert_eq!(
        fields.len(),
        2,
        "golden UDT component {:?} must render exactly 2 fields",
        parts[0]
    );
    let unnull = |s: &str| (s != "\\@").then(|| s.to_string());
    let rank = unnull(fields[1]).map(|s| {
        s.parse::<i64>()
            .unwrap_or_else(|e| panic!("golden rank {s:?} is not an int: {e}"))
    });
    let second = parts[1]
        .parse::<i64>()
        .unwrap_or_else(|e| panic!("golden tuple component 2 {:?} is not an int: {e}", parts[1]));
    (unnull(fields[0]), rank, second)
}

/// `pk -> the golden's `m_tuple_udt` keys for that partition`.
fn golden_keys(table_dir: &Path) -> BTreeMap<String, Vec<TupleKey>> {
    let jsonl = std::fs::read_dir(table_dir)
        .expect("fixture table dir readable")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db.jsonl"))
        })
        .expect("the committed sstabledump golden must be present");
    let raw = std::fs::read_to_string(&jsonl).expect("golden readable");
    let mut out: BTreeMap<String, Vec<TupleKey>> = BTreeMap::new();
    for line in raw.lines().filter(|l| !l.trim().is_empty()) {
        let doc: J = serde_json::from_str(line).expect("golden json");
        let pk = doc["partition"]["key"][0]
            .as_str()
            .unwrap_or_default()
            .to_string();
        for row in doc["rows"].as_array().into_iter().flatten() {
            for cell in row["cells"].as_array().into_iter().flatten() {
                if cell["name"].as_str() != Some(COLUMN) {
                    continue;
                }
                if let Some(p) = cell["path"][0].as_str() {
                    out.entry(pk.clone())
                        .or_default()
                        .push(parse_golden_path(p));
                }
            }
        }
    }
    assert!(
        !out.is_empty(),
        "the golden must carry {COLUMN} entries; an empty expectation would make \
         this test vacuous"
    );
    out
}

/// One rendered `{"key": …, "value": …}` entry, asserted STRUCTURALLY and then
/// normalised. The structural asserts are the AC-4 property: an array of
/// [UDT object, int], never the `"0x…"` string an opaque key renders as.
fn rendered_key(entry: &J, id: i64) -> TupleKey {
    let key = entry
        .get("key")
        .unwrap_or_else(|| panic!("id={id}: the CLI Map arm emits {{key, value}}; got {entry}"));
    let items = key.as_array().unwrap_or_else(|| {
        panic!(
            "id={id}: a multicell TUPLE map key must render as a STRUCTURED JSON \
             array through the CLI writer (issue #3612 AC 4); got {key} — a \
             string here is the pre-#3612 opaque `Value::Blob` rendered as hex"
        )
    });
    assert_eq!(
        items.len(),
        2,
        "id={id}: tuple<frozen<key_part>, int> renders as 2 components; got {key}"
    );
    let udt = items[0].as_object().unwrap_or_else(|| {
        panic!(
            "id={id}: the tuple's first component is a frozen `key_part` UDT and \
             must render as a JSON object of its declared fields; got {}",
            items[0]
        )
    });
    // The FIELD NAMESPACE, as a set: exactly the two declared fields and nothing
    // injected. Emitted ORDER is #3629's subject, not AC 4's, and is asserted
    // there — pinning it again here would only add a way for this test to red for
    // a property it is not about.
    let mut names: Vec<&str> = udt.keys().map(String::as_str).collect();
    names.sort_unstable();
    assert_eq!(
        names,
        vec!["label", "rank"],
        "id={id}: the tuple-borne UDT must expose its two declared fields and \
         nothing else; got {}",
        items[0]
    );
    let label = match udt.get("label") {
        Some(J::String(s)) => Some(s.clone()),
        Some(J::Null) | None => None,
        other => panic!("id={id}: `label` is text or null; got {other:?}"),
    };
    let rank = match udt.get("rank") {
        Some(J::Number(n)) => Some(
            n.as_i64()
                .unwrap_or_else(|| panic!("id={id}: `rank` is an int; got {n}")),
        ),
        Some(J::Null) | None => None,
        other => panic!("id={id}: `rank` is an int or null; got {other:?}"),
    };
    let second = items[1]
        .as_i64()
        .unwrap_or_else(|| panic!("id={id}: the tuple's second component is an int; got {key}"));
    (label, rank, second)
}

/// THE AC-4 CLI ASSERTION: every `m_tuple_udt` key, rendered through the public
/// CLI JSON surface `JSONWriter::write`, is a structured tuple key equal to the
/// sstabledump golden component for component.
#[tokio::test]
async fn multicell_tuple_map_key_renders_structurally_through_the_cli_json_writer() {
    let sstables_root = repo_root().join("test-data/datasets/sstables");
    let expected = golden_keys(&fixture_table_dir(&sstables_root));

    let schema = repo_root().join("test-data/schemas/nested-udt-keys.cql");
    assert!(schema.is_file(), "committed schema missing: {schema:?}");

    let db = ingest(IngestionConfig {
        schema_paths: vec![schema],
        data_dir: sstables_root,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(KEYSPACE.to_string()),
    })
    .await
    .expect("ingesting the committed nested_udt_keys fixture must succeed")
    .database;

    let result = db
        .execute(&format!("SELECT id, {COLUMN} FROM {KEYSPACE}.{TABLE}"))
        .await
        .expect("SELECT over the committed fixture must succeed");

    let out = JSONWriter::write(&result, &OutputConfig::default())
        .expect("JSONWriter::write must succeed");
    let parsed: J = serde_json::from_str(&out)
        .unwrap_or_else(|e| panic!("CLI JSON output did not parse: {e}\n{out}"));
    let rows = parsed
        .as_array()
        .unwrap_or_else(|| panic!("CLI --format json emits a bare array; got {parsed}"))
        .clone();
    assert!(
        !rows.is_empty(),
        "zero rows from a PRESENT committed fixture is a decode failure, never a skip"
    );

    let mut checked = 0usize;
    for row in &rows {
        let id = row
            .get("id")
            .and_then(J::as_i64)
            .unwrap_or_else(|| panic!("row without an integer id: {row}"));
        let Some(want) = expected.get(&id.to_string()) else {
            // A partition the golden shows no `m_tuple_udt` cells for (id=4).
            continue;
        };
        let cell = row
            .get(COLUMN)
            .unwrap_or_else(|| panic!("id={id}: SELECT must project {COLUMN}"));
        let entries = cell.as_array().unwrap_or_else(|| {
            panic!("id={id}: a map renders as an array of {{key, value}}; got {cell}")
        });
        assert_eq!(
            entries.len(),
            want.len(),
            "id={id}: entry count must match the golden — a key COLLAPSE would show here"
        );
        let mut got: Vec<TupleKey> = entries.iter().map(|e| rendered_key(e, id)).collect();
        got.sort();
        let mut want_sorted = want.clone();
        want_sorted.sort();
        assert_eq!(
            got, want_sorted,
            "id={id}: the CLI's rendered tuple keys must match the sstabledump golden"
        );
        checked += 1;
    }
    assert_eq!(
        checked,
        expected.len(),
        "every golden partition carrying {COLUMN} must have been checked through \
         the CLI writer"
    );
}
