//! Issue #1717 (AK7): `read-sstable` renders values with the canonical
//! [`ValueFormatter`] — there is no second formatter.
//!
//! `cqlite-cli/src/commands/read_sstable.rs` used to hand-roll a local
//! `format_value` that rendered each cell with `Value`'s `Display` impl (a
//! debug-oriented, type-tagged form: `'text'`, `BLOB(n bytes)`,
//! `TIMESTAMP(1759713126059)`, `counter:41`, `UUID(<unhyphenated hex>)`), while
//! every other CLI writer used `ValueFormatter`. Two formatters =
//! N-copies-of-truth: a formatting fix landed in one and `read-sstable`
//! silently diverged.
//!
//! What is locked here, over REAL corpus fixtures (never synthesized values):
//!
//!   1. PARITY — for every cell of every row of every fixture table,
//!      `render_scan_row` output equals the rendering composed independently
//!      from `ValueFormatter::format_value`. This is the acceptance criterion
//!      "read_sstable's rendering == ValueFormatter's rendering for every value
//!      type present".
//!   2. THE FORK IS GONE — wherever the deleted `Display`-based rendering
//!      differs from the canonical one, the output must NOT be the legacy form.
//!      Reverting `read_sstable.rs` to `format!("{}", v)` fails this.
//!   3. TYPE COVERAGE — each fixture must actually surface the value variants it
//!      is here for (per-CASE assertion, so a case cannot silently contribute
//!      nothing behind a green suite; issue #3220).
//!   4. WIRING — the shipped `cqlite read-sstable --format json` stdout carries
//!      those canonical renderings and none of the legacy type tags, so the
//!      parity above is the behavior a user actually gets.
//!
//! `--raw` is deliberately unchanged: it is a structural `Debug` dump of the
//! scan carrier, not a value rendering (locked in AC 1's raw check).
//!
//! Gated on `state_machine` (as the sibling `read_sstable_stdout_tests.rs` is):
//! the `read-sstable` subcommand only exists with it.

#![cfg(feature = "state_machine")]

use cqlite_cli::commands::read_sstable::render_scan_row;
use cqlite_cli::output::value_fmt::ValueFormatter;
use cqlite_core::types::{ScanRow, Value};
use cqlite_core::{storage::sstable::reader::SSTableReader, Config as CoreConfig};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A corpus table this test reads, with the value variants it must surface.
struct Fixture {
    keyspace: &'static str,
    table: &'static str,
    /// Top-level cell variants this table MUST yield (measured from the corpus).
    /// Nested variants (a UDT inside a list, …) are covered transitively by the
    /// recursive parity assertion, so they are not listed here.
    required_variants: &'static [&'static str],
    /// `true` for a table of the four documented dataset keyspaces (always in
    /// the fetched corpus). A `false` table may be absent on an older dataset
    /// pin — but if its keyspace is present, the table must be too.
    must_run: bool,
}

/// Fixtures chosen so their union covers every value variant the corpus carries
/// at the top level: 23 of the 27 `Value` variants (`Null`, `Varint`, `Json` and
/// `Tuple` do not occur as top-level cells anywhere in the corpus).
const FIXTURES: &[Fixture] = &[
    Fixture {
        keyspace: "test_basic",
        table: "simple_table",
        required_variants: &[
            "BigInt",
            "Blob",
            "Boolean",
            "Date",
            "Decimal",
            "Duration",
            "Float",
            "Float32",
            "Inet",
            "Integer",
            "SmallInt",
            "Text",
            "Time",
            "Timestamp",
            "TinyInt",
            "Uuid",
        ],
        must_run: true,
    },
    Fixture {
        keyspace: "test_basic",
        table: "counters",
        required_variants: &["Counter"],
        must_run: true,
    },
    Fixture {
        keyspace: "test_collections",
        table: "collection_table",
        required_variants: &["List", "Map", "Set"],
        must_run: true,
    },
    Fixture {
        // Nested UDTs inside list/set/map values.
        keyspace: "test_collections",
        table: "collections_with_udts",
        required_variants: &["List", "Map", "Set"],
        must_run: true,
    },
    Fixture {
        keyspace: "test_collections",
        table: "frozen_collections_table",
        required_variants: &["Frozen", "Set"],
        must_run: true,
    },
    Fixture {
        keyspace: "test_wide_rows",
        table: "chat_messages",
        required_variants: &["Text", "Timestamp", "Tombstone", "Uuid"],
        must_run: true,
    },
    Fixture {
        // Top-level multicell UDT; only in the wider dataset pin.
        keyspace: "test_types",
        table: "cx_multicell_udt_collection_paths",
        required_variants: &["Udt"],
        must_run: false,
    },
];

/// The `Value` variant name, used for per-case type-coverage assertions.
fn variant_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "Null",
        Value::Boolean(_) => "Boolean",
        Value::TinyInt(_) => "TinyInt",
        Value::SmallInt(_) => "SmallInt",
        Value::Integer(_) => "Integer",
        Value::BigInt(_) => "BigInt",
        Value::Counter(_) => "Counter",
        Value::Float32(_) => "Float32",
        Value::Float(_) => "Float",
        Value::Text(_) => "Text",
        Value::Blob(_) => "Blob",
        Value::Timestamp(_) => "Timestamp",
        Value::Date(_) => "Date",
        Value::Time(_) => "Time",
        Value::Uuid(_) => "Uuid",
        Value::Varint(_) => "Varint",
        Value::Decimal { .. } => "Decimal",
        Value::Duration { .. } => "Duration",
        Value::Json(_) => "Json",
        Value::List(_) => "List",
        Value::Set(_) => "Set",
        Value::Map(_) => "Map",
        Value::Tuple(_) => "Tuple",
        Value::Udt(_) => "Udt",
        Value::Frozen(_) => "Frozen",
        Value::Tombstone(_) => "Tombstone",
        Value::Inet(_) => "Inet",
        // Issue #3805. Not currently produced by any fixture in this corpus —
        // the decoder wiring that emits it for a zero-length cell path is a
        // separate slice — so it appears in no `required_variants` list above.
        Value::Empty(_) => "Empty",
    }
}

/// Candidate dataset roots, in the order they are probed. EVERY root is walked
/// for EVERY table (issue #3220): neither root is a superset of the other, so
/// committing to one by keyspace can silently skip a table the other holds.
fn candidate_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Ok(env_root) = std::env::var("CQLITE_DATASETS_ROOT") {
        roots.push(PathBuf::from(env_root));
    }
    let checkout = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(|p| p.join("test-data/datasets"));
    if let Some(checkout) = checkout {
        if !roots.contains(&checkout) {
            roots.push(checkout);
        }
    }
    roots
}

/// Are this keyspace's SSTable BINARIES present under any candidate root?
///
/// Measured by EVIDENCE — the presence of a `*-Data.db` — never by directory
/// shape (#3220). `is_dir()` would be wrong here: `test-data/datasets/sstables/`
/// ships git-TRACKED metadata directories (e.g. `test_types/` carries 116 tracked
/// `.jsonl`/TOC/digest/Statistics files and ZERO committed `*-Data.db`), so a
/// directory test answers `true` on every checkout from metadata alone and would
/// turn an optional fixture's legitimate absence into a false failure.
fn keyspace_present(keyspace: &str) -> bool {
    first_data_db(keyspace, None).is_some()
}

/// Resolve a `*-Data.db` for `<keyspace>/<table>` by walking every candidate
/// root (with and without the optional `sstables/` layer) and every UUID-suffixed
/// table directory. Returns the lexicographically first match so the choice is
/// deterministic.
fn find_table_data_db(keyspace: &str, table: &str) -> Option<PathBuf> {
    first_data_db(keyspace, Some(&format!("{}-", table)))
}

/// The one directory walk both presence questions are answered from: the first
/// `*-Data.db` under `<candidate root>[/sstables]/<keyspace>/<table-dir>`, where
/// `table_prefix` (when given) restricts the UUID-suffixed table directories to
/// one table. `None` accepts any table, i.e. "does this keyspace have binaries".
fn first_data_db(keyspace: &str, table_prefix: Option<&str>) -> Option<PathBuf> {
    for root in candidate_roots() {
        for keyspace_dir in [root.join("sstables").join(keyspace), root.join(keyspace)] {
            let Ok(entries) = std::fs::read_dir(&keyspace_dir) else {
                continue;
            };
            let mut table_dirs: Vec<PathBuf> = entries
                .flatten()
                .map(|e| e.path())
                .filter(|p| {
                    p.is_dir()
                        && table_prefix.is_none_or(|prefix| {
                            p.file_name()
                                .map(|n| n.to_string_lossy().starts_with(prefix))
                                .unwrap_or(false)
                        })
                })
                .collect();
            table_dirs.sort();
            for table_dir in table_dirs {
                let Ok(files) = std::fs::read_dir(&table_dir) else {
                    continue;
                };
                let mut data_files: Vec<PathBuf> = files
                    .flatten()
                    .map(|e| e.path())
                    .filter(|p| p.to_string_lossy().ends_with("-Data.db"))
                    .collect();
                data_files.sort();
                if let Some(first) = data_files.into_iter().next() {
                    return Some(first);
                }
            }
        }
    }
    None
}

/// Read every entry of an SSTable exactly as `read-sstable` does.
async fn read_scan_rows(path: &Path) -> Vec<ScanRow> {
    let config = CoreConfig::default();
    let platform = Arc::new(
        cqlite_core::platform::Platform::new(&config)
            .await
            .expect("platform init"),
    );
    let reader = SSTableReader::open(path, &config, platform)
        .await
        .unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    reader
        .get_all_entries()
        .await
        .unwrap_or_else(|e| panic!("read entries of {}: {e}", path.display()))
        .into_iter()
        .map(|(_table_id, _key, row)| row)
        .collect()
}

/// The canonical expectation, composed INDEPENDENTLY of `read_sstable.rs`:
/// the `{name: value, …}` row shape with every value rendered by
/// `ValueFormatter::format_value`.
fn canonical_rendering(row: &ScanRow) -> String {
    let cells: Vec<String> = match row {
        ScanRow::Row(cells) => cells
            .iter()
            .map(|(name, v)| format!("{}: {}", name, ValueFormatter::format_value(v)))
            .collect(),
        ScanRow::RawRow(bytes) => vec![format!(
            "data: {}",
            ValueFormatter::format_value(&Value::blob(bytes.clone()))
        )],
        ScanRow::Marker(v) => return ValueFormatter::format_value(v),
    };
    format!("{{{}}}", cells.join(", "))
}

/// The DELETED fork, reproduced here ONLY as a negative oracle: `Value`'s
/// `Display` impl per cell. Nothing in `cqlite-cli` may render values this way
/// again — assertion 2 fails if `read_sstable.rs` returns to it.
fn legacy_display_rendering(row: &ScanRow) -> String {
    let cells: Vec<String> = match row {
        ScanRow::Row(cells) => cells
            .iter()
            .map(|(name, v)| format!("{}: {}", name, v))
            .collect(),
        ScanRow::RawRow(bytes) => vec![format!("data: {}", Value::blob(bytes.clone()))],
        ScanRow::Marker(v) => return format!("{}", v),
    };
    format!("{{{}}}", cells.join(", "))
}

/// Top-level cell values of a scan row (a marker's inner value included, since
/// that is what `read-sstable` renders for it).
fn top_level_values(row: &ScanRow) -> Vec<Value> {
    match row {
        ScanRow::Row(cells) => cells.iter().map(|(_n, v)| v.clone()).collect(),
        ScanRow::RawRow(bytes) => vec![Value::blob(bytes.clone())],
        ScanRow::Marker(v) => vec![v.clone()],
    }
}

/// AC 1/2/3: parity, fork-is-gone and per-case type coverage over the corpus.
#[tokio::test(flavor = "multi_thread")]
async fn read_sstable_value_rendering_is_value_formatter() {
    let resolved: Vec<(&Fixture, Option<PathBuf>)> = FIXTURES
        .iter()
        .map(|f| (f, find_table_data_db(f.keyspace, f.table)))
        .collect();

    // Corpus entirely absent (a checkout without the fetched Data.db binaries):
    // skip loudly. Any fixture found means the corpus IS present, and then every
    // `must_run` fixture must be found too — a partial corpus fails, it never
    // quietly narrows coverage.
    if resolved.iter().all(|(_f, p)| p.is_none()) {
        eprintln!(
            "SKIP: no fixture Data.db found under any candidate dataset root \
             (run test-data/scripts/fetch-datasets.sh and export the printed \
             CQLITE_DATASETS_ROOT)"
        );
        return;
    }

    let mut diverging_variants: BTreeSet<&'static str> = BTreeSet::new();

    for (fixture, path) in resolved {
        let path = match path {
            Some(path) => path,
            None if fixture.must_run => panic!(
                "fixture {}/{} not found under any candidate dataset root, but the \
                 corpus is present — partial corpus, coverage would silently narrow",
                fixture.keyspace, fixture.table
            ),
            None => {
                // Optional fixture: absent keyspace is fine, absent table is not.
                assert!(
                    !keyspace_present(fixture.keyspace),
                    "keyspace {} is present but table {} is missing from it",
                    fixture.keyspace,
                    fixture.table
                );
                eprintln!(
                    "NOTE: optional fixture {}/{} absent from this dataset pin",
                    fixture.keyspace, fixture.table
                );
                continue;
            }
        };

        let rows = read_scan_rows(&path).await;
        assert!(
            !rows.is_empty(),
            "fixture {}/{} ({}) yielded 0 rows — a dataset-dependent case must \
             never pass on an empty read",
            fixture.keyspace,
            fixture.table,
            path.display()
        );

        let mut observed_variants: BTreeSet<&'static str> = BTreeSet::new();
        let mut cells_checked = 0usize;

        for row in &rows {
            let rendered = render_scan_row(row, false);
            let canonical = canonical_rendering(row);
            assert_eq!(
                rendered, canonical,
                "read-sstable rendering diverges from ValueFormatter for {}/{}",
                fixture.keyspace, fixture.table
            );

            let legacy = legacy_display_rendering(row);
            if legacy != canonical {
                assert_ne!(
                    rendered, legacy,
                    "read-sstable fell back to the deleted Display-based fork for {}/{}",
                    fixture.keyspace, fixture.table
                );
            }

            // `--raw` is a structural dump of the carrier, unchanged by #1717.
            assert_eq!(render_scan_row(row, true), format!("{:?}", row));

            for value in top_level_values(row) {
                let name = variant_name(&value);
                observed_variants.insert(name);
                if ValueFormatter::format_value(&value) != format!("{}", value) {
                    diverging_variants.insert(name);
                }
                cells_checked += 1;
            }
        }

        assert!(
            cells_checked > 0,
            "fixture {}/{} produced rows but no cells to compare",
            fixture.keyspace,
            fixture.table
        );
        for required in fixture.required_variants {
            assert!(
                observed_variants.contains(required),
                "fixture {}/{} did not surface a {} value (observed: {:?}) — the \
                 case is no longer covering the type it exists for",
                fixture.keyspace,
                fixture.table,
                required,
                observed_variants
            );
        }
    }

    // The two formatters really do differ, so assertion 2 above is not vacuous.
    for expected in [
        "Blob",
        "Counter",
        "Date",
        "Decimal",
        "Duration",
        "Frozen",
        "Text",
        "Time",
        "Timestamp",
        "Tombstone",
        "Uuid",
    ] {
        assert!(
            diverging_variants.contains(expected),
            "expected {} to diverge between Display and ValueFormatter \
             (observed diverging: {:?})",
            expected,
            diverging_variants
        );
    }
}

/// AC 4 (wiring): the shipped binary's `--format json` stdout carries the
/// canonical renderings and none of the legacy type tags.
#[test]
fn read_sstable_json_stdout_carries_canonical_renderings() {
    let Some(path) = find_table_data_db("test_basic", "simple_table") else {
        eprintln!("SKIP: test_basic/simple_table Data.db fixture not present");
        return;
    };

    let rows = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(read_scan_rows(&path));
    assert!(!rows.is_empty(), "simple_table yielded 0 rows");
    let expected: BTreeSet<String> = rows.iter().map(canonical_rendering).collect();

    let output = assert_cmd::Command::cargo_bin("cqlite")
        .expect("cqlite binary should be built for integration tests")
        .args(["read-sstable"])
        .arg(&path)
        .args(["--format", "json", "--limit", "5"])
        .output()
        .expect("read-sstable should execute");
    assert!(
        output.status.success(),
        "read-sstable exited non-zero: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let entries: Vec<serde_json::Value> =
        serde_json::from_str(&stdout).unwrap_or_else(|e| panic!("stdout is not JSON: {e}"));
    assert!(!entries.is_empty(), "read-sstable emitted no JSON entries");

    for entry in &entries {
        let value = entry
            .get("value")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("entry without a string `value`: {entry}"));
        assert!(
            expected.contains(value),
            "read-sstable emitted a rendering that is not ValueFormatter's: {value}"
        );
    }

    // Legacy `Display` type tags that the deleted fork emitted for this table.
    for tag in [
        "BLOB(",
        "TIMESTAMP(",
        "DATE(",
        "DECIMAL(",
        "DURATION(",
        "UUID(",
        "TIME(",
    ] {
        assert!(
            !stdout.contains(tag),
            "legacy Display type tag {tag:?} is back in read-sstable stdout"
        );
    }
}
