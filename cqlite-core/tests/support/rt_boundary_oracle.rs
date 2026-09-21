//! Cassandra oracle and fail-closed controls for the open-ended boundary regression.

use super::*;
use crc32fast::Hasher as Crc32Hasher;
use std::collections::BTreeSet;

#[path = "datasets_root.rs"]
mod datasets_root;

// ════════════════════════════════════════════════════════════════════════════
// Criterion 5 - CASSANDRA BYTE ORACLE (fail-closed skip-on-absent)
// ============================================================================

/// The #1383/#4243 oracle extends the existing Cassandra 5.0.2 tombstone/TTL
/// fixture family. Cassandra 5.0.8 remains the pinned format/semantic authority;
/// the fixture version is recorded in the parity manifest so the two are never
/// silently mixed.
const ORACLE_KEYSPACE: &str = "test_compaction_tombstone_ttl";
const ORACLE_TABLE: &str = "rt_open_ended_boundary";
const BYTE_FOR_BYTE_COMPONENTS: &[&str] = &["Data.db", "Index.db", "Summary.db", "Digest.crc32"];
const PRESENT_NOT_DIFFED: &[&str] = &["Statistics.db", "Filter.db"];

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// Resolve exactly one committed fixture directory. Absence is a clean skip
/// outside strict mode; any present-but-incomplete or duplicate fixture fails.
fn reference_dir(table: &str) -> Option<PathBuf> {
    // Search each candidate for this table. An incomplete earlier match is an
    // error, not permission to silently substitute another root's golden.
    for root in datasets_root::sstables_root_candidates() {
        let base = root.join(ORACLE_KEYSPACE);
        let entries = match std::fs::read_dir(&base) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => panic!("oracle directory {base:?} unreadable: {error}"),
        };
        let mut matches = Vec::new();
        for entry in entries {
            let entry = entry.unwrap_or_else(|error| {
                panic!("oracle directory entry under {base:?} unreadable: {error}")
            });
            if entry
                .file_name()
                .to_string_lossy()
                .starts_with(&format!("{table}-"))
            {
                assert!(
                    entry.path().is_dir(),
                    "oracle fixture {:?} is not a directory",
                    entry.path()
                );
                matches.push(entry.path());
            }
        }
        match matches.len() {
            0 => continue,
            1 => {
                let dir = matches.pop().expect("one matching fixture");
                assert!(single_data_db(&dir).is_some(),
                    "{ORACLE_KEYSPACE}.{table}: fixture {dir:?} is present but has no single compacted nb-*-big-Data.db");
                return Some(dir);
            }
            n => panic!("{ORACLE_KEYSPACE}.{table}: expected exactly one fixture directory, found {n}: {matches:?}"),
        }
    }
    None
}

fn single_data_db(dir: &Path) -> Option<PathBuf> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("oracle directory {dir:?} unreadable: {error}"))
    {
        let entry =
            entry.unwrap_or_else(|error| panic!("oracle entry under {dir:?} unreadable: {error}"));
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with("nb-") && name.ends_with("-big-Data.db") {
            found.push(entry.path());
        }
    }
    match found.len() {
        0 => None,
        1 => found.pop(),
        n => panic!("{dir:?}: expected one compacted Data.db, found {n}: {found:?}"),
    }
}

fn descriptor_prefix(data_db: &Path) -> String {
    data_db
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .trim_end_matches("Data.db")
        .to_string()
}

fn read_component(dir: &Path, suffix: &str) -> Vec<u8> {
    let data = single_data_db(dir).unwrap_or_else(|| panic!("{dir:?}: no compacted Data.db"));
    let path = dir.join(format!("{}{suffix}", descriptor_prefix(&data)));
    std::fs::read(&path).unwrap_or_else(|error| panic!("component {path:?} unreadable: {error}"))
}

fn component_suffixes(dir: &Path) -> BTreeSet<String> {
    let mut set = BTreeSet::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(index) = name.find("-big-") {
                set.insert(name[index + 5..].to_string());
            }
        }
    }
    set.retain(|suffix| !suffix.ends_with(".jsonl") && !suffix.ends_with("Statistics.db.txt"));
    set
}

fn toc_set(bytes: &[u8]) -> BTreeSet<String> {
    String::from_utf8_lossy(bytes)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
}

fn first_diff(left: &[u8], right: &[u8]) -> Option<usize> {
    (0..left.len().max(right.len())).find(|&index| left.get(index) != right.get(index))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn assert_digest_consistent(table: &str, dir: &Path) {
    let data = read_component(dir, "Data.db");
    let digest = read_component(dir, "Digest.crc32");
    assert!(!data.is_empty(), "{table}: Data.db is empty");
    let mut hasher = Crc32Hasher::new();
    hasher.update(&data);
    let actual = hasher.finalize();
    let expected = std::str::from_utf8(&digest)
        .unwrap_or_else(|error| panic!("{table}: Digest.crc32 is not UTF-8: {error}"))
        .trim()
        .parse::<u32>()
        .unwrap_or_else(|error| panic!("{table}: Digest.crc32 is not a u32: {error}"));
    assert_eq!(
        actual, expected,
        "{table}: Digest.crc32 does not match Data.db"
    );
}

fn iso_to_epoch_secs(value: &str) -> Option<i32> {
    let timestamp = chrono::DateTime::parse_from_rfc3339(value).ok()?;
    // Cassandra emits whole-second local deletion times. Reject precision that
    // would otherwise be silently truncated, as well as out-of-range epochs.
    if timestamp.timestamp_subsec_nanos() != 0 {
        return None;
    }
    i32::try_from(timestamp.timestamp()).ok()
}

/// Return the authoritative Cassandra LDT for the old (marked_deleted=10) and
/// new (marked_deleted=20) range tombstones. The JSONL sidecar is the source of
/// truth for wall-clock localDeletionTime; no value is guessed in the test.
fn oracle_ldts(ref_dir: &Path) -> (i32, i32) {
    let data = single_data_db(ref_dir).expect("oracle Data.db");
    let jsonl = ref_dir.join(format!("{}Data.db.jsonl", descriptor_prefix(&data)));
    let text = std::fs::read_to_string(&jsonl)
        .unwrap_or_else(|error| panic!("oracle JSONL {jsonl:?} unreadable: {error}"));
    let mut old = BTreeSet::new();
    let mut new = BTreeSet::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let value: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|error| panic!("oracle JSONL {jsonl:?} is invalid: {error}"));
        let Some(rows) = value.get("rows").and_then(serde_json::Value::as_array) else {
            continue;
        };
        for row in rows {
            for side in ["start", "end"] {
                let Some(info) = row.get(side).and_then(|bound| bound.get("deletion_info")) else {
                    continue;
                };
                let marked = info
                    .get("marked_deleted")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|timestamp| {
                        chrono::DateTime::parse_from_rfc3339(timestamp)
                            .ok()
                            .map(|value| value.timestamp_micros())
                    })
                    .unwrap_or_else(|| panic!("oracle JSONL has malformed marked_deleted: {info}"));
                let ldt = info
                    .get("local_delete_time")
                    .and_then(serde_json::Value::as_str)
                    .and_then(iso_to_epoch_secs)
                    .unwrap_or_else(|| {
                        panic!("oracle JSONL has malformed local_delete_time: {info}")
                    });
                match marked {
                    10 => {
                        old.insert(ldt);
                    }
                    20 => {
                        new.insert(ldt);
                    }
                    other => panic!(
                        "oracle JSONL range marker has unexpected marked_deleted={other}; \
                         expected exactly 10 and 20 microseconds"
                    ),
                }
            }
        }
    }
    assert_eq!(
        old.len(),
        1,
        "oracle must carry one old-range LDT, got {old:?}"
    );
    assert_eq!(
        new.len(),
        1,
        "oracle must carry one new-range LDT, got {new:?}"
    );
    let old_ldt = *old.iter().next().expect("old LDT");
    let new_ldt = *new.iter().next().expect("new LDT");
    assert_ne!(old_ldt, new_ldt, "oracle range LDTs must be distinct");
    (old_ldt, new_ldt)
}

fn oracle_schema() -> TableSchema {
    TableSchema {
        keyspace: ORACLE_KEYSPACE.into(),
        table: ORACLE_TABLE.into(),
        partition_keys: vec![KeyColumn {
            name: "id".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".into(),
            data_type: "int".into(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "id".into(),
                data_type: "int".into(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".into(),
                data_type: "int".into(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "v".into(),
                data_type: "text".into(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

fn oracle_row(ck: i32, value: &str, timestamp: i64) -> Mutation {
    Mutation::new(
        TableId::new(ORACLE_KEYSPACE, ORACLE_TABLE),
        PartitionKey::single("id", Value::Integer(PID)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::Write {
            column: "v".into(),
            value: Value::text(value.to_string()),
        }],
        timestamp,
        None,
    )
}

fn oracle_range_delete(
    start: ClusteringBound,
    end: ClusteringBound,
    timestamp: i64,
    ldt: i32,
) -> Mutation {
    let mut mutation = Mutation::new(
        TableId::new(ORACLE_KEYSPACE, ORACLE_TABLE),
        PartitionKey::single("id", Value::Integer(PID)),
        None,
        vec![],
        timestamp,
        None,
    );
    mutation.range_tombstones.push(RangeTombstone {
        start,
        end,
        deletion_time: timestamp,
        local_deletion_time: ldt,
    });
    mutation
}

fn assert_component_bytes(table: &str, reference_dir: &Path, output_dir: &Path, suffix: &str) {
    let reference = read_component(reference_dir, suffix);
    let output = read_component(output_dir, suffix);
    assert!(
        !reference.is_empty(),
        "{table}: reference {suffix} is empty"
    );
    if reference != output {
        let at = first_diff(&reference, &output);
        panic!(
            "{table}: {suffix} differs (reference={} output={} bytes, first diff at {at:?})\n  \
             reference={}\n  output={}",
            reference.len(),
            output.len(),
            hex(&reference),
            hex(&output)
        );
    }
}

fn assert_oracle_components(reference_dir: &Path, output_dir: &Path) {
    assert_digest_consistent(ORACLE_TABLE, reference_dir);
    assert_digest_consistent(ORACLE_TABLE, output_dir);
    let reference_components = component_suffixes(reference_dir);
    let output_components = component_suffixes(output_dir);
    for needed in BYTE_FOR_BYTE_COMPONENTS
        .iter()
        .chain(PRESENT_NOT_DIFFED.iter())
        .chain(["TOC.txt", "CRC.db"].iter())
    {
        assert!(
            reference_components.contains(*needed),
            "{ORACLE_TABLE}: reference missing {needed}; have {reference_components:?}"
        );
        assert!(
            output_components.contains(*needed),
            "{ORACLE_TABLE}: CQLite output missing {needed}; have {output_components:?}"
        );
        assert!(
            !read_component(reference_dir, needed).is_empty()
                && !read_component(output_dir, needed).is_empty(),
            "{ORACLE_TABLE}: {needed} is present but empty"
        );
    }
    assert_eq!(
        reference_components, output_components,
        "{ORACLE_TABLE}: Cassandra and CQLite component sets differ"
    );
    assert_eq!(
        toc_set(&read_component(reference_dir, "TOC.txt")),
        toc_set(&read_component(output_dir, "TOC.txt")),
        "{ORACLE_TABLE}: TOC component sets differ"
    );
    for suffix in BYTE_FOR_BYTE_COMPONENTS {
        assert_component_bytes(ORACLE_TABLE, reference_dir, output_dir, suffix);
    }
}

/// Cassandra-major-compacted open-ended ranges must byte-match the same two
/// generations compacted through CQLite. The fixture LDTs are read from the
/// authoritative JSONL sidecar, because Cassandra derives them from wall clock.
#[test]
fn crit5_cassandra_oracle_two_gen_open_ended_boundary() {
    let Some(reference_dir) = reference_dir(ORACLE_TABLE) else {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but {ORACLE_KEYSPACE}.{ORACLE_TABLE} fixture is absent"
            );
        }
        eprintln!("[issue_1383] {ORACLE_KEYSPACE}.{ORACLE_TABLE} fixture absent; skipping");
        return;
    };
    let (old_ldt, new_ldt) = oracle_ldts(&reference_dir);
    let schema = oracle_schema();
    let temp = TempDir::new().expect("oracle tempdir");
    let (mut engine, runtime) = engine(&temp, "oracle", &schema);
    flush_batch(
        &mut engine,
        &runtime,
        vec![
            oracle_range_delete(ClusteringBound::Bottom, excl(5), 10, old_ldt),
            oracle_row(2, "ck2-ts15", 15),
            oracle_row(1, "ck1-ts5", 5),
            oracle_row(6, "ck6-ts25", 25),
        ],
    );
    flush_batch(
        &mut engine,
        &runtime,
        vec![
            oracle_range_delete(incl(3), ClusteringBound::Top, 20, new_ldt),
            oracle_row(4, "ck4-ts15", 15),
        ],
    );
    runtime
        .block_on(engine.close())
        .expect("close oracle engine");
    let inputs = discover_inputs(&temp.path().join("oracle-data"));
    assert_eq!(
        inputs.len(),
        2,
        "expected exactly two oracle input SSTables"
    );
    let output = compact(inputs, &temp.path().join("oracle-out"), &schema, 1_383_005);
    let output_dir = output.parent().expect("CQLite output parent").to_path_buf();
    assert_oracle_components(&reference_dir, &output_dir);
    eprintln!(
        "[issue_1383] {ORACLE_KEYSPACE}.{ORACLE_TABLE}: Cassandra 5.0.2 byte parity PASS for \
         Data.db/Index.db/Summary.db/Digest.crc32"
    );
}

fn copy_fixture(source: &Path, destination: &Path) {
    std::fs::create_dir_all(destination).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            std::fs::copy(entry.path(), destination.join(entry.file_name())).unwrap();
        }
    }
}

fn run_oracle_child(dataset: &Path, checkout: &Path, strict: bool) -> std::process::Output {
    std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "oracle::crit5_cassandra_oracle_two_gen_open_ended_boundary",
            "--nocapture",
        ])
        .env("CQLITE_DATASETS_ROOT", dataset)
        .env(datasets_root::CHECKOUT_SSTABLES_ROOT_OVERRIDE_ENV, checkout)
        .env("CQLITE_REQUIRE_FIXTURES", if strict { "1" } else { "0" })
        .output()
        .expect("run public oracle test in an isolated fixture environment")
}

#[test]
fn oracle_rejects_missing_incomplete_duplicate_and_malformed_fixtures() {
    let Some(source) = reference_dir(ORACLE_TABLE) else {
        assert!(
            !require_fixtures_strict(),
            "strict oracle controls require the Cassandra fixture"
        );
        eprintln!("oracle fixture absent; skipping negative controls");
        return;
    };
    for (case, expected) in [
        ("missing", "fixture is absent"),
        ("incomplete", "no single compacted"),
        ("duplicate", "expected exactly one fixture directory"),
        ("malformed-json", "is invalid"),
        ("malformed-ldt", "malformed local_delete_time"),
        ("bad-digest", "Digest.crc32 does not match"),
    ] {
        let temp = TempDir::new().unwrap();
        let checkout = temp.path().join("empty-checkout");
        std::fs::create_dir_all(&checkout).unwrap();
        let keyspace = temp.path().join("sstables").join(ORACLE_KEYSPACE);
        let fixture = keyspace.join(source.file_name().unwrap());
        if case != "missing" {
            copy_fixture(&source, &fixture);
            let data = single_data_db(&fixture).unwrap();
            let prefix = descriptor_prefix(&data);
            match case {
                "incomplete" => std::fs::remove_file(data).unwrap(),
                "duplicate" => {
                    copy_fixture(&source, &keyspace.join(format!("{ORACLE_TABLE}-duplicate")))
                }
                "malformed-json" => {
                    std::fs::write(fixture.join(format!("{prefix}Data.db.jsonl")), "{invalid\n")
                        .unwrap()
                }
                "malformed-ldt" => {
                    let path = fixture.join(format!("{prefix}Data.db.jsonl"));
                    let text = std::fs::read_to_string(&path).unwrap();
                    let mut lines: Vec<serde_json::Value> = text
                        .lines()
                        .filter(|line| !line.trim().is_empty())
                        .map(|line| serde_json::from_str(line).unwrap())
                        .collect();
                    let mut changed = false;
                    for partition in &mut lines {
                        for row in partition["rows"].as_array_mut().unwrap() {
                            for side in ["start", "end"] {
                                if let Some(info) = row
                                    .get_mut(side)
                                    .and_then(|bound| bound.get_mut("deletion_info"))
                                {
                                    info["local_delete_time"] = "2026-08-51T00:00:00Z".into();
                                    changed = true;
                                }
                            }
                        }
                    }
                    assert!(changed, "the Cassandra fixture must contain deletion times");
                    let text = lines
                        .iter()
                        .map(serde_json::Value::to_string)
                        .collect::<Vec<_>>()
                        .join("\n");
                    std::fs::write(path, text).unwrap();
                }
                "bad-digest" => {
                    std::fs::write(fixture.join(format!("{prefix}Digest.crc32")), "0\n").unwrap()
                }
                _ => unreachable!(),
            }
        }
        // A valid checkout copy must not conceal an incomplete external fixture.
        if case == "incomplete" {
            copy_fixture(
                &source,
                &checkout
                    .join(ORACLE_KEYSPACE)
                    .join(source.file_name().unwrap()),
            );
        }
        let result = run_oracle_child(temp.path(), &checkout, true);
        let output = format!(
            "{}{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(
            !result.status.success() && output.contains(expected),
            "{case}: {output}"
        );
        if case == "missing" {
            let result = run_oracle_child(temp.path(), &checkout, false);
            assert!(result.status.success());
            assert!(String::from_utf8_lossy(&result.stderr).contains("fixture absent; skipping"));
        }
    }
}

#[test]
fn oracle_uses_checkout_fixture_when_external_root_lacks_table() {
    let Some(source) = reference_dir(ORACLE_TABLE) else {
        assert!(
            !require_fixtures_strict(),
            "strict oracle fallback control requires the Cassandra fixture"
        );
        eprintln!("oracle fixture absent; skipping checkout fallback control");
        return;
    };
    let temp = TempDir::new().unwrap();
    let checkout = temp.path().join("checkout");
    copy_fixture(
        &source,
        &checkout
            .join(ORACLE_KEYSPACE)
            .join(source.file_name().unwrap()),
    );
    // Keep the keyspace present externally: selection must be table-granular.
    std::fs::create_dir_all(temp.path().join("sstables").join(ORACLE_KEYSPACE)).unwrap();
    let result = run_oracle_child(temp.path(), &checkout, true);
    assert!(
        result.status.success(),
        "checkout fallback oracle failed: {}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(String::from_utf8_lossy(&result.stderr).contains("Cassandra 5.0.2 byte parity PASS"));
}
