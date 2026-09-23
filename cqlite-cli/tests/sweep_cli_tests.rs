//! Issue #4194 (corruption-locator, spec cli-sweep) — `cqlite sweep <data-dir>`
//! CLI surface: the walk, severities, exit-code contract, `--jobs` determinism,
//! and JSON/text report shape.
//!
//! Drives the REAL compiled `cqlite` binary (`CARGO_BIN_EXE_cqlite`) — the
//! exit-code contract (`execute_sweep_command`'s `std::process::exit`) is only
//! observable through a subprocess.
//!
//! Every case here stages its own temp fixture from committed/fetched
//! Cassandra-written bytes (never a byte-pattern search of its own) so this
//! file's pass/fail does not depend on the completeness of whatever corpus
//! root happens to be fetched on a given box (measured: on at least one fleet
//! box, several `sstables/system/*` and `sstables/test_deltas/*` directories
//! are git-tracked but their `*-Data.db` binaries were never materialized —
//! itself a correct `unreadable` row, but not a stable "the WHOLE corpus is
//! clean" oracle for S1.1/S4.1).
//!
//! # Deviation from spec.md's literal S1.4 scenario
//!
//! spec.md's S1.4 illustration ("a `Data.db` file present but no `TOC.txt` and
//! no readable `Statistics.db`") does not actually reach `verify_sstable`'s
//! `Err` path — a present `Data.db` alone is sufficient for
//! `resolve_components` to succeed, so that shape returns `Ok(report)` with
//! `MissingComponent` findings, which `sweep`'s severity mapping (design.md
//! §D3, matching `verify_sstable`'s own documented contract: "the function
//! only returns `Err` for environmental problems … *data* corruption is
//! reported as findings") correctly classifies as `corrupt`, not
//! `unreadable`. Verified directly (`cargo run -- sweep`) before writing this
//! test. The `unreadable` case tested here — a directory with NO `*-Data.db`
//! at all — is the shape design.md's own §D3 pseudocode names
//! ("`verify_sstable(dir, mode)` fails to even resolve components ──►
//! unreadable") and is what actually exercises that branch.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;
use tempfile::TempDir;

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.is_dir())
}

/// The clean `test_comp.lz4_table` generation directory, gated per #1094
/// doctrine.
fn lz4_table_dir() -> Option<PathBuf> {
    let root = datasets_root()?;
    let base = root.join("sstables/test_comp");
    let mut candidates: Vec<PathBuf> = std::fs::read_dir(&base)
        .ok()?
        .flatten()
        .filter_map(|e| {
            let name = e.file_name();
            let name = name.to_str()?.to_string();
            (name.starts_with("lz4_table-") && e.path().join("nb-1-big-Data.db").is_file())
                .then(|| e.path())
        })
        .collect();
    candidates.sort();
    candidates.into_iter().next()
}

fn require_lz4_table() -> Option<PathBuf> {
    let dir = lz4_table_dir();
    if dir.is_none() {
        assert!(
            !require_fixtures_strict(),
            "CQLITE_REQUIRE_FIXTURES=1 but the clean lz4_table fixture is unavailable"
        );
        eprintln!("SKIP: clean lz4_table fixture unavailable (set CQLITE_DATASETS_ROOT)");
    }
    dir
}

fn copy_generation(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create staging dir");
    for e in std::fs::read_dir(src).expect("read fixture dir").flatten() {
        if e.path().is_file() {
            std::fs::copy(e.path(), dst.join(e.file_name())).expect("copy fixture component");
        }
    }
}

/// Like [`copy_generation`], but renames every copied component's base-name
/// PREFIX (e.g. `nb-1-big-` -> `nb-2-big-`), so a SECOND generation can be
/// staged into the SAME table directory as a first (roborev round-2 HIGH
/// finding's regression test — `discover_table_dirs` must enumerate BOTH).
fn copy_generation_renamed(src: &Path, dst: &Path, old_base: &str, new_base: &str) {
    std::fs::create_dir_all(dst).expect("create staging dir");
    let old_prefix = format!("{old_base}-");
    let new_prefix = format!("{new_base}-");
    for e in std::fs::read_dir(src).expect("read fixture dir").flatten() {
        if !e.path().is_file() {
            continue;
        }
        let name = e.file_name();
        let name = name.to_str().expect("utf8 filename");
        let renamed = name
            .strip_prefix(&old_prefix)
            .map(|suffix| format!("{new_prefix}{suffix}"))
            .unwrap_or_else(|| panic!("{name} does not start with {old_prefix}"));
        std::fs::copy(e.path(), dst.join(renamed)).expect("copy renamed fixture component");
    }
}

/// Flip one bit of the first byte of `Data.db` in `dir` in place — no CRC
/// recompute, so the chunk-CRC check fails (the same technique
/// `issue_4194_verify_location.rs`'s L2.2 case uses).
fn corrupt_data_db(dir: &Path) {
    corrupt_data_db_for_base(dir, "nb-1-big");
}

fn corrupt_data_db_for_base(dir: &Path, base_name: &str) {
    let path = dir.join(format!("{base_name}-Data.db"));
    let mut bytes = std::fs::read(&path).expect("read Data.db to corrupt");
    bytes[0] ^= 0x01;
    std::fs::write(&path, bytes).expect("write corrupted Data.db");
}

/// Flip the manifest-pinned `Filter.db` bit (byte 8, `0x10` -> `0x00`,
/// `corruption-manifest.yml`'s `filter_db_bit_flip` entry) directly on a
/// COPY of the clean fixture's own `Filter.db` — self-contained, does not
/// depend on the (possibly-unfetched) `filter_db_bit_flip` corpus fixture
/// directory being present.
fn corrupt_filter_db_false_negative(dir: &Path) {
    let path = dir.join("nb-1-big-Filter.db");
    let mut bytes = std::fs::read(&path).expect("read Filter.db to corrupt");
    assert!(
        bytes.len() > 8,
        "Filter.db too short to hold the manifest-pinned bit"
    );
    assert_eq!(
        bytes[8] & 0x10,
        0x10,
        "expected bit 0x10 set at byte 8 before the flip"
    );
    bytes[8] &= !0x10;
    std::fs::write(&path, bytes).expect("write corrupted Filter.db");
}

fn run_sweep(data_dir: &Path, out: &str, jobs: Option<usize>) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_cqlite"));
    cmd.args([
        "sweep",
        &data_dir.display().to_string(),
        "--mode",
        "full",
        "--out",
        out,
    ]);
    if let Some(j) = jobs {
        cmd.args(["--jobs", &j.to_string()]);
    }
    cmd.output().expect("spawn cqlite sweep")
}

fn parse_json(output: &Output) -> Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!(
            "cqlite sweep --out json did not emit valid JSON: {e}\nstdout: {stdout}\nstderr: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

// ---------------------------------------------------------------------------
// S1.1 / S4.1 — an all-healthy sweep, and the JSON report shape
// ---------------------------------------------------------------------------

#[test]
fn s1_1_and_s4_1_all_healthy_sweep_is_all_ok_with_affirmative_zero_totals() {
    let Some(clean) = require_lz4_table() else {
        return;
    };
    let staging = TempDir::new().expect("create staging dir");
    copy_generation(&clean, &staging.path().join("ks1").join("table-a"));
    copy_generation(&clean, &staging.path().join("ks1").join("table-b"));

    let output = run_sweep(staging.path(), "json", None);
    assert_eq!(
        output.status.code(),
        Some(0),
        "expected exit 0 for an all-healthy sweep"
    );
    let value = parse_json(&output);
    let rows = value["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 2, "expected one row per table dir: {value}");
    for row in rows {
        assert_eq!(row["severity"], "ok", "expected every row ok: {row}");
        assert!(
            row["cause"].is_null(),
            "an ok row must not carry a cause: {row}"
        );
    }
    // Affirmative-zero (S4.1): every severity key present even at 0.
    let totals = &value["totals"];
    assert_eq!(totals["ok"], 2);
    assert_eq!(totals["degraded"], 0);
    assert_eq!(totals["corrupt"], 0);
    assert_eq!(totals["unreadable"], 0);
}

// ---------------------------------------------------------------------------
// S1.2 — one corrupted copy makes exactly that row corrupt
// ---------------------------------------------------------------------------

#[test]
fn s1_2_one_corrupted_copy_makes_exactly_that_row_corrupt() {
    let Some(clean) = require_lz4_table() else {
        return;
    };
    let staging = TempDir::new().expect("create staging dir");
    let healthy_dir = staging.path().join("ks1").join("healthy-table");
    let corrupt_dir = staging.path().join("ks1").join("corrupt-table");
    copy_generation(&clean, &healthy_dir);
    copy_generation(&clean, &corrupt_dir);
    corrupt_data_db(&corrupt_dir);

    let output = run_sweep(staging.path(), "json", None);
    assert_eq!(
        output.status.code(),
        Some(2),
        "expected exit 2 with one corrupt row"
    );
    let value = parse_json(&output);
    let rows = value["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 2);

    let corrupt_row = rows
        .iter()
        .find(|r| r["path"].as_str().unwrap().contains("corrupt-table"))
        .expect("corrupt-table row present");
    assert_eq!(corrupt_row["severity"], "corrupt");
    let findings = corrupt_row["findings"].as_array().expect("findings array");
    assert!(
        findings
            .iter()
            .any(|f| f["class"] == "ChunkDecompressionError"),
        "corrupt row did not name ChunkDecompressionError: {corrupt_row}"
    );

    let healthy_row = rows
        .iter()
        .find(|r| r["path"].as_str().unwrap().contains("healthy-table"))
        .expect("healthy-table row present");
    assert_eq!(healthy_row["severity"], "ok");
}

// ---------------------------------------------------------------------------
// roborev round-2 HIGH finding — a table directory holding MULTIPLE
// generations gets one row PER GENERATION, not one row for the whole
// directory (which would silently report only the lexicographically-first
// generation's verdict).
// ---------------------------------------------------------------------------

#[test]
fn s1_5_a_table_directory_with_two_generations_reports_two_rows() {
    let Some(clean) = require_lz4_table() else {
        return;
    };
    let staging = TempDir::new().expect("create staging dir");
    let table_dir = staging.path().join("ks1").join("multi-gen-table");
    // Two generations in the SAME directory: nb-1-big-* (healthy) and
    // nb-2-big-* (corrupted) — the exact shape a real compacted table
    // directory has before the old generation is removed.
    copy_generation(&clean, &table_dir);
    copy_generation_renamed(&clean, &table_dir, "nb-1-big", "nb-2-big");
    corrupt_data_db_for_base(&table_dir, "nb-2-big");

    let output = run_sweep(staging.path(), "json", None);
    assert_eq!(
        output.status.code(),
        Some(2),
        "expected exit 2 with the second generation corrupted"
    );
    let value = parse_json(&output);
    let rows = value["rows"].as_array().expect("rows array");
    assert_eq!(
        rows.len(),
        2,
        "expected exactly one row PER GENERATION (two in this one directory): {value}"
    );

    let gen1_row = rows
        .iter()
        .find(|r| r["path"].as_str().unwrap().contains("nb-1-big-Data.db"))
        .unwrap_or_else(|| panic!("no row for the first generation: {value}"));
    assert_eq!(gen1_row["severity"], "ok");

    let gen2_row = rows
        .iter()
        .find(|r| r["path"].as_str().unwrap().contains("nb-2-big-Data.db"))
        .unwrap_or_else(|| panic!("no row for the second (corrupted) generation: {value}"));
    assert_eq!(gen2_row["severity"], "corrupt");
}

// ---------------------------------------------------------------------------
// S1.3 / S2.1 — a Filter.db-only finding is degraded, not corrupt, and alone
// does not trip the failing exit code
// ---------------------------------------------------------------------------

#[test]
fn s1_3_and_s2_1_filter_only_finding_is_degraded_and_does_not_fail_the_sweep() {
    let Some(clean) = require_lz4_table() else {
        return;
    };
    let staging = TempDir::new().expect("create staging dir");
    let dir = staging.path().join("ks1").join("filter-table");
    copy_generation(&clean, &dir);
    corrupt_filter_db_false_negative(&dir);

    let output = run_sweep(staging.path(), "json", None);
    assert_eq!(
        output.status.code(),
        Some(0),
        "degraded alone must not trip the failing exit code: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let value = parse_json(&output);
    let rows = value["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["severity"], "degraded");
    assert_eq!(value["totals"]["degraded"], 1);
    assert_eq!(value["totals"]["corrupt"], 0);
    assert_eq!(value["totals"]["unreadable"], 0);
}

// ---------------------------------------------------------------------------
// S1.4 — a directory with no `*-Data.db` at all is a row, never an omission
// (see this file's module doc for the deviation from spec.md's literal
// illustration)
// ---------------------------------------------------------------------------

#[test]
fn s1_4_a_directory_with_no_data_db_is_an_unreadable_row_never_an_omission() {
    let staging = TempDir::new().expect("create staging dir");
    let dir = staging.path().join("ks1").join("empty-table");
    std::fs::create_dir_all(&dir).expect("create empty table dir");
    std::fs::write(dir.join("stray.txt"), b"not an sstable component").expect("write stray file");

    let output = run_sweep(staging.path(), "json", None);
    assert_eq!(
        output.status.code(),
        Some(2),
        "expected exit 2 for an unreadable row"
    );
    let value = parse_json(&output);
    let rows = value["rows"].as_array().expect("rows array");
    assert_eq!(
        rows.len(),
        1,
        "the directory must still produce exactly one row: {value}"
    );
    assert_eq!(rows[0]["severity"], "unreadable");
    assert!(
        rows[0]["cause"]
            .as_str()
            .map(|c| !c.is_empty())
            .unwrap_or(false),
        "unreadable row must name a cause: {}",
        rows[0]
    );
}

// ---------------------------------------------------------------------------
// roborev round-1 MEDIUM finding — an UNREADABLE KEYSPACE directory is its
// own row, never a silent omission (the guarantee previously held only one
// level down, at the table dir).
// ---------------------------------------------------------------------------

#[cfg(unix)]
#[test]
fn an_unreadable_keyspace_directory_is_its_own_unreadable_row() {
    use std::os::unix::fs::PermissionsExt;

    let Some(clean) = require_lz4_table() else {
        return;
    };
    let staging = TempDir::new().expect("create staging dir");
    // One healthy table under a READABLE keyspace, so the sweep also proves
    // it did not stop at the first unreadable keyspace.
    copy_generation(&clean, &staging.path().join("ks_ok").join("table-a"));

    let locked_ks = staging.path().join("ks_locked");
    std::fs::create_dir_all(locked_ks.join("table-b")).expect("create locked keyspace dir");
    // Remove read+execute so `read_dir` on `ks_locked` itself fails (the
    // TABLE dir underneath stays populated but unreachable).
    std::fs::set_permissions(&locked_ks, std::fs::Permissions::from_mode(0o000))
        .expect("chmod 000 the keyspace dir");

    // roborev round-4 LOW finding: `chmod 000` is a no-op against DAC as
    // root (any containerized CI lane commonly runs as root) — `read_dir`
    // would then succeed, no `ks_locked` row would appear, and the test
    // would fail for a reason unrelated to the code under test. Probe
    // directly rather than asserting a permission model this process might
    // not be subject to.
    if std::fs::read_dir(&locked_ks).is_ok() {
        std::fs::set_permissions(&locked_ks, std::fs::Permissions::from_mode(0o755))
            .expect("restore keyspace dir permissions");
        eprintln!(
            "SKIP: chmod 000 did not make {} unreadable (running as root?)",
            locked_ks.display()
        );
        return;
    }

    let output = run_sweep(staging.path(), "json", None);
    // Always restore permissions before any assertion can panic/return, so
    // TempDir's own Drop cleanup can still remove the directory.
    std::fs::set_permissions(&locked_ks, std::fs::Permissions::from_mode(0o755))
        .expect("restore keyspace dir permissions");

    assert_eq!(
        output.status.code(),
        Some(2),
        "expected exit 2 with an unreadable keyspace present"
    );
    let value = parse_json(&output);
    let rows = value["rows"].as_array().expect("rows array");
    let locked_row = rows
        .iter()
        .find(|r| r["path"].as_str().unwrap().contains("ks_locked"))
        .unwrap_or_else(|| panic!("no row for the unreadable ks_locked directory: {value}"));
    assert_eq!(locked_row["severity"], "unreadable");
    assert!(
        locked_row["cause"]
            .as_str()
            .map(|c| !c.is_empty())
            .unwrap_or(false),
        "unreadable keyspace row must name a cause: {locked_row}"
    );
    let ok_row = rows
        .iter()
        .find(|r| r["path"].as_str().unwrap().contains("table-a"))
        .unwrap_or_else(|| panic!("readable keyspace's table row missing: {value}"));
    assert_eq!(ok_row["severity"], "ok");
}

// ---------------------------------------------------------------------------
// roborev round-1 MEDIUM finding — zero table directories found must not
// read as a clean sweep (affirmative-zero doctrine).
// ---------------------------------------------------------------------------

#[test]
fn zero_table_directories_found_is_not_a_clean_sweep() {
    let staging = TempDir::new().expect("create staging dir");
    // The data dir exists but has no keyspace/table subdirectories at all.
    let output = run_sweep(staging.path(), "json", None);
    assert_eq!(
        output.status.code(),
        Some(2),
        "a zero-row sweep must not exit 0"
    );
    assert!(
        output.stdout.is_empty(),
        "a zero-row sweep must not print a report claiming to have swept something: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("no table directories"),
        "stderr did not name the empty-sweep condition: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// S2.2 — usage error on a missing data dir
// ---------------------------------------------------------------------------

#[test]
fn s2_2_usage_error_on_a_missing_data_dir() {
    let output = Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args(["sweep", "/does/not/exist/cqlite-4194-sweep-test"])
        .output()
        .expect("spawn cqlite sweep");
    assert_eq!(
        output.status.code(),
        Some(1),
        "expected exit 1 on a missing data dir"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("does not exist") || stderr.contains("not a directory"),
        "stderr did not name the missing path: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// S3.2 — --jobs bounds concurrency, not correctness
// ---------------------------------------------------------------------------

#[test]
fn s3_2_jobs_bounds_concurrency_not_which_rows_appear() {
    let Some(clean) = require_lz4_table() else {
        return;
    };
    let staging = TempDir::new().expect("create staging dir");
    for i in 0..4 {
        let dir = staging.path().join("ks1").join(format!("table-{i}"));
        copy_generation(&clean, &dir);
        if i == 1 {
            corrupt_data_db(&dir);
        }
    }

    let out_1job = run_sweep(staging.path(), "json", Some(1));
    let out_4jobs = run_sweep(staging.path(), "json", Some(4));
    assert_eq!(out_1job.status.code(), out_4jobs.status.code());

    let mut rows_1job: Vec<(String, String)> = parse_json(&out_1job)["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            (
                r["path"].as_str().unwrap().to_string(),
                r["severity"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    let mut rows_4jobs: Vec<(String, String)> = parse_json(&out_4jobs)["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            (
                r["path"].as_str().unwrap().to_string(),
                r["severity"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    rows_1job.sort();
    rows_4jobs.sort();
    assert_eq!(
        rows_1job, rows_4jobs,
        "--jobs must bound throughput only, never which rows appear or their severities"
    );
}

// ---------------------------------------------------------------------------
// S4.2 — text rendering matches the JSON rows
// ---------------------------------------------------------------------------

#[test]
fn s4_2_text_rendering_matches_the_json_rows() {
    let Some(clean) = require_lz4_table() else {
        return;
    };
    let staging = TempDir::new().expect("create staging dir");
    let healthy_dir = staging.path().join("ks1").join("healthy-table");
    let corrupt_dir = staging.path().join("ks1").join("corrupt-table");
    copy_generation(&clean, &healthy_dir);
    copy_generation(&clean, &corrupt_dir);
    corrupt_data_db(&corrupt_dir);

    let json_output = run_sweep(staging.path(), "json", None);
    let text_output = run_sweep(staging.path(), "text", None);
    assert_eq!(json_output.status.code(), text_output.status.code());

    let value = parse_json(&json_output);
    let text_stdout = String::from_utf8_lossy(&text_output.stdout);
    // roborev round-1 MEDIUM finding: a whole-stdout `.contains(path) &&
    // .contains(severity)` is satisfied by the trailing `totals: ok=… …`
    // line ALONE (every severity token also appears there), so the original
    // form could not detect a row rendered with the WRONG severity. Assert
    // the pairing on the SPECIFIC per-row line instead: the row's own text
    // line (identified by containing its path, excluding the "totals:"
    // line) must START WITH that row's severity token (`print_text` always
    // renders `{severity:<11}{path}…` or `ok{pad}{path}` — severity first).
    let row_lines: Vec<&str> = text_stdout
        .lines()
        .filter(|l| !l.starts_with("totals:"))
        .collect();
    for row in value["rows"].as_array().unwrap() {
        let path = row["path"].as_str().unwrap();
        let severity = row["severity"].as_str().unwrap();
        let matching: Vec<&&str> = row_lines.iter().filter(|l| l.contains(path)).collect();
        assert_eq!(
            matching.len(),
            1,
            "expected exactly one text line for row {path}, found {}: {text_stdout}",
            matching.len()
        );
        assert!(
            matching[0].trim_start().starts_with(severity),
            "row line for {path} does not start with its severity {severity}: {:?}",
            matching[0]
        );
    }

    // Roborev finding (final round, #4194): this test previously checked only
    // row-level (path, severity) pairing between `--out text` and `--out
    // json` — it never asserted that a location-bearing finding's `location`
    // is actually PRESENT in the text output, the exact gap the sweep
    // text-renderer Medium fix closed (`--out text` is `sweep`'s DEFAULT
    // mode, so this was a real gap in the verb the issue adds). Assert it
    // directly: every JSON finding carrying a non-null `location` must have a
    // matching `location: ...` line in the text output naming the same
    // physical offset (and chunk index, when the finding has one).
    let mut checked_a_location = false;
    let location_lines: Vec<&str> = text_stdout
        .lines()
        .filter(|l| l.trim_start().starts_with("location:"))
        .collect();
    for row in value["rows"].as_array().unwrap() {
        for finding in row["findings"].as_array().unwrap() {
            let Some(loc) = finding["location"].as_object() else {
                continue;
            };
            checked_a_location = true;
            let byte_offset = loc["byte_offset"].as_u64().unwrap();
            let expected_offset_hex = format!("offset 0x{byte_offset:x}");
            let chunk_index = loc["chunk_index"].as_u64();
            let expected_chunk = chunk_index.map(|c| format!("chunk {c}, "));
            assert!(
                location_lines.iter().any(|l| {
                    l.contains(&expected_offset_hex)
                        && match &expected_chunk {
                            Some(c) => l.contains(c.as_str()),
                            None => true,
                        }
                }),
                "expected a text `location:` line containing {expected_offset_hex:?} \
                 (chunk {chunk_index:?}) but found: {location_lines:?}\nfull text: {text_stdout}"
            );
        }
    }
    assert!(
        checked_a_location,
        "expected at least one location-bearing finding in this sweep run — \
         corrupt_data_db (lz4-compressed fixture, --mode full) should produce \
         a chunk-CRC finding with `location` set; if this fixture stopped \
         producing one, the assertions above are vacuous"
    );
}
