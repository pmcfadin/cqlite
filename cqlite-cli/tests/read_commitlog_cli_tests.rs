//! End-to-end wiring evidence for the `read-commitlog` subcommand (issue #2389).
//!
//! Proves the full call chain CLI entry point → `CommitLogReader` → decoded
//! mutations by invoking the built `cqlite` binary against a real
//! Cassandra-5.0.2-produced segment fixture committed under
//! `test-data/datasets/commitlog/` (see `generate-commitlog-fixtures.sh`). The
//! fixtures are small committed reference binaries, so this test never silently
//! passes on missing data.

use assert_cmd::Command;
use std::io::Write;
use std::path::PathBuf;
use tempfile::NamedTempFile;

fn commitlog_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("test-data/datasets/commitlog")
}

fn clean_fixture() -> PathBuf {
    find_fixture("clean-")
}

fn corrupt_crc_fixture() -> PathBuf {
    find_fixture("corrupt-crc-")
}

fn find_fixture(prefix: &str) -> PathBuf {
    let dir = commitlog_dir();
    for entry in std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read commitlog dir {}: {e}", dir.display()))
        .flatten()
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(prefix) && name.ends_with(".log") {
            return entry.path();
        }
    }
    panic!("no {prefix}*.log fixture under {}", dir.display());
}

/// Ground-truth table id recorded alongside the fixture.
fn ground_truth_table_id() -> String {
    let path = commitlog_dir().join("commitlog-ground-truth.json");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read ground truth {}: {e}", path.display()));
    let v: serde_json::Value = serde_json::from_str(&text).expect("parse ground truth");
    v["table_id"]
        .as_str()
        .expect("ground-truth table_id")
        .to_string()
}

/// Requirement: CLI subcommand reads a real segment end to end.
#[test]
fn read_commitlog_json_reports_descriptor_and_mutations() {
    let fixture = clean_fixture();
    let output = Command::cargo_bin("cqlite")
        .expect("cqlite binary should be built for integration tests")
        .arg("--quiet")
        .arg("read-commitlog")
        .arg(&fixture)
        .arg("--format")
        .arg("json")
        .output()
        .expect("read-commitlog command should execute");

    assert!(
        output.status.success(),
        "read-commitlog exited non-zero: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout must be valid JSON");

    // Descriptor parsed authoritatively from the real segment.
    assert_eq!(parsed["descriptor"]["version"], 7, "descriptor version");
    assert!(
        parsed["descriptor"]["compression"].is_null(),
        "clean fixture is uncompressed"
    );
    assert_eq!(parsed["truncated"], false, "clean fixture not truncated");
    assert_eq!(parsed["decode_error"], false, "no decode error");

    let count = parsed["mutation_count"].as_u64().expect("mutation_count");
    assert!(count > 0, "segment must decode at least one mutation");

    // The full call chain reached our inserted table's partition updates.
    let table_id = ground_truth_table_id();
    let updates = parsed["updates"].as_array().expect("updates array");
    let saw_our_table = updates
        .iter()
        .any(|u| u["table_id"].as_str() == Some(table_id.as_str()));
    assert!(
        saw_our_table,
        "decoded stream must include the inserted table id {table_id}"
    );
}

/// `--limit 1` stops before the segment's true tail, so `limited` is reported
/// true and `truncated`/`decode_error` are JSON `null` (never a false-looking
/// `false`) since the tail state was never reached (roborev finding).
#[test]
fn read_commitlog_limit_one_reports_limited_and_null_truncated() {
    let fixture = clean_fixture();
    let output = Command::cargo_bin("cqlite")
        .expect("cqlite binary should be built for integration tests")
        .arg("--quiet")
        .arg("read-commitlog")
        .arg(&fixture)
        .arg("--format")
        .arg("json")
        .arg("--limit")
        .arg("1")
        .output()
        .expect("read-commitlog command should execute");

    assert!(
        output.status.success(),
        "read-commitlog --limit 1 exited non-zero: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout must be valid JSON");
    assert_eq!(parsed["limited"], true, "--limit 1 must report limited");
    assert!(
        parsed["truncated"].is_null(),
        "truncated must be null when limited, not a false-looking false"
    );
    assert!(
        parsed["decode_error"].is_null(),
        "decode_error must be null when limited"
    );
}

/// `--limit 0` yields zero mutations (its own short-circuit) and still reports
/// `limited` true.
#[test]
fn read_commitlog_limit_zero_reports_zero_mutations() {
    let fixture = clean_fixture();
    let output = Command::cargo_bin("cqlite")
        .expect("cqlite binary should be built for integration tests")
        .arg("--quiet")
        .arg("read-commitlog")
        .arg(&fixture)
        .arg("--format")
        .arg("json")
        .arg("--limit")
        .arg("0")
        .output()
        .expect("read-commitlog command should execute");

    assert!(
        output.status.success(),
        "read-commitlog --limit 0 exited non-zero: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout must be valid JSON");
    assert_eq!(
        parsed["mutation_count"], 0,
        "--limit 0 must decode zero mutations"
    );
    assert_eq!(parsed["limited"], true, "--limit 0 must report limited");
}

/// The text (default) format also succeeds end to end and reports the segment.
#[test]
fn read_commitlog_text_reports_segment_header() {
    let fixture = clean_fixture();
    let output = Command::cargo_bin("cqlite")
        .expect("cqlite binary should be built")
        .arg("--quiet")
        .arg("read-commitlog")
        .arg(&fixture)
        .output()
        .expect("read-commitlog command should execute");

    assert!(output.status.success(), "read-commitlog exited non-zero");
    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    assert!(
        stdout.contains("CommitLog segment"),
        "text output must report the segment header, got: {stdout}"
    );
    assert!(stdout.contains("version:     7"), "reports version 7");
}

/// Requirement: a mid-stream corrupt record must exit non-zero, not silently
/// succeed with a `decode_error` field buried in the output (roborev finding,
/// review-first pass) — a script piping this command must be able to detect
/// the failure from the exit code alone.
#[test]
fn read_commitlog_exits_non_zero_on_corrupt_stream() {
    let fixture = corrupt_crc_fixture();
    let output = Command::cargo_bin("cqlite")
        .expect("cqlite binary should be built")
        .arg("--quiet")
        .arg("read-commitlog")
        .arg(&fixture)
        .arg("--format")
        .arg("json")
        .output()
        .expect("read-commitlog command should execute");

    assert!(
        !output.status.success(),
        "a corrupt-CRC segment must exit non-zero, not silently succeed"
    );
    // The already-decoded prefix is still valid JSON on stdout.
    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout must still be valid JSON");
    assert_eq!(
        parsed["decode_error"], true,
        "decode_error must be reported"
    );
}

/// Build a small descriptor and one sync section using the same CRC32 wire
/// contracts as Cassandra. These generated segments exercise the real CLI
/// binary without relying on a parser-only unit test or a checked-in fixture.
fn synthetic_segment(records: &[Vec<u8>]) -> NamedTempFile {
    const SEGMENT_ID: i64 = 1_234;
    let version = 7_i32;
    let params = b"{}";
    let mut bytes = Vec::new();

    bytes.extend_from_slice(&version.to_be_bytes());
    bytes.extend_from_slice(&SEGMENT_ID.to_be_bytes());
    let params_len_u16 = u16::try_from(params.len()).expect("test descriptor params fit u16");
    bytes.extend_from_slice(&params_len_u16.to_be_bytes());
    bytes.extend_from_slice(params);

    let mut header_crc_input = Vec::new();
    header_crc_input.extend_from_slice(&version.to_be_bytes());
    let id = u64::try_from(SEGMENT_ID).expect("test segment id is non-negative");
    let id_low = u32::try_from(id & u64::from(u32::MAX)).expect("low id word fits u32");
    let id_high = u32::try_from(id >> 32).expect("high id word fits u32");
    header_crc_input.extend_from_slice(&id_low.to_be_bytes());
    header_crc_input.extend_from_slice(&id_high.to_be_bytes());
    let params_len_u32 = u32::try_from(params.len()).expect("test descriptor params fit u32");
    header_crc_input.extend_from_slice(&params_len_u32.to_be_bytes());
    header_crc_input.extend_from_slice(params);
    bytes.extend_from_slice(&crc32(&header_crc_input).to_be_bytes());

    let marker_pos = bytes.len();
    bytes.extend_from_slice(&0_i32.to_be_bytes()); // patched to end of section below
    bytes.extend_from_slice(
        &cqlite_core::storage::commitlog::frame::marker_crc(SEGMENT_ID, marker_pos).to_be_bytes(),
    );
    for record in records {
        append_framed_record(&mut bytes, record);
    }

    let section_end = i32::try_from(bytes.len()).expect("synthetic section offset fits i32");
    bytes[marker_pos..marker_pos + 4].copy_from_slice(&section_end.to_be_bytes());

    let mut file = NamedTempFile::new().expect("temporary CommitLog segment");
    file.write_all(&bytes)
        .expect("write synthetic CommitLog segment");
    file.flush().expect("flush synthetic CommitLog segment");
    file
}

fn append_framed_record(segment: &mut Vec<u8>, body: &[u8]) {
    let size = i32::try_from(body.len()).expect("synthetic mutation body fits i32");
    let size_bytes = size.to_be_bytes();
    segment.extend_from_slice(&size_bytes);
    segment.extend_from_slice(&crc32(&size_bytes).to_be_bytes());
    segment.extend_from_slice(body);

    let mut body_crc_input = Vec::with_capacity(size_bytes.len() + body.len());
    body_crc_input.extend_from_slice(&size_bytes);
    body_crc_input.extend_from_slice(body);
    segment.extend_from_slice(&crc32(&body_crc_input).to_be_bytes());
}

/// Reflected CRC-32/ISO-HDLC used by Cassandra's CommitLog framing.
fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let low_bit_mask = 0_u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xEDB8_8320 & low_bit_mask);
        }
    }
    !crc
}

/// Mutation whose partition updates take the empty-partition fast path, making
/// every update fully consumed without requiring any schema information.
fn mutation_with_empty_updates(update_count: usize) -> Vec<u8> {
    assert!(
        update_count < 128,
        "test helper uses one-byte unsigned VInts"
    );
    let mut body = vec![update_count as u8];
    for update_index in 0..update_count {
        let mut table_id = [0_u8; 16];
        table_id[15] = u8::try_from(update_index + 1).expect("small test update index");
        body.extend_from_slice(&table_id);
        body.push(1); // partition-key length
        body.push(u8::try_from(update_index).expect("small test partition-key byte"));
        body.push(0x01); // ITER_IS_EMPTY; no column-name block exists
    }
    body
}

/// Ordinary update with an encoded zero-count regular column-name block.
fn mutation_with_empty_columns() -> Vec<u8> {
    let mut body = vec![1]; // one partition update
    body.extend_from_slice(&[0xA5; 16]);
    body.push(0); // empty partition key
    body.push(0); // iter flags
    body.extend_from_slice(&[0, 0, 0]); // EncodingStats
    body.push(0); // parsed regular column count: empty
    body
}

/// Static-row update. The current decoder deliberately bails before guessing
/// where any column-name block would occur after these iterator flags.
fn mutation_with_static_row() -> Vec<u8> {
    let mut body = vec![1]; // one partition update
    body.extend_from_slice(&[0x5A; 16]);
    body.push(0); // empty partition key
    body.push(0x08); // ITER_HAS_STATIC_ROW
    body.extend_from_slice(&[0, 0, 0]); // EncodingStats, read before the bailout
    body
}

fn run_cli(
    segment: &NamedTempFile,
    format: Option<&str>,
    limit: Option<usize>,
) -> std::process::Output {
    let mut command = Command::cargo_bin("cqlite").expect("cqlite binary should be built");
    command
        .arg("--quiet")
        .arg("read-commitlog")
        .arg(segment.path());
    if let Some(format) = format {
        command.arg("--format").arg(format);
    }
    if let Some(limit) = limit {
        command.arg("--limit").arg(limit.to_string());
    }
    command
        .output()
        .expect("read-commitlog command should execute")
}

fn run_json(segment: &NamedTempFile, limit: Option<usize>) -> serde_json::Value {
    let output = run_cli(segment, Some("json"), limit);
    assert!(
        output.status.success(),
        "read-commitlog exited non-zero: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stdout must be valid JSON")
}

#[test]
fn read_commitlog_limit_reports_when_it_clips_a_multi_update_mutation() {
    let segment = synthetic_segment(&[mutation_with_empty_updates(2)]);

    let clipped = run_json(&segment, Some(1));
    assert_eq!(clipped["mutation_count"], 1, "count the encountered record");
    assert_eq!(clipped["limited"], true);
    assert_eq!(clipped["all_updates_complete"], false);
    assert_eq!(clipped["last_mutation_partial"], true);
    assert_eq!(clipped["updates"].as_array().unwrap().len(), 1);
    assert_eq!(clipped["updates"][0]["columns_read"], false);

    let exactly_all_updates = run_json(&segment, Some(2));
    assert_eq!(exactly_all_updates["limited"], true);
    assert_eq!(exactly_all_updates["all_updates_complete"], true);
    assert_eq!(exactly_all_updates["last_mutation_partial"], false);
    assert_eq!(exactly_all_updates["updates"].as_array().unwrap().len(), 2);

    let one_update_segment = synthetic_segment(&[mutation_with_empty_updates(1)]);
    let bounded_complete = run_json(&one_update_segment, Some(1));
    assert_eq!(bounded_complete["last_mutation_partial"], false);
    assert_eq!(bounded_complete["all_updates_complete"], true);
}

#[test]
fn read_commitlog_cli_distinguishes_parsed_empty_columns_from_static_bailout() {
    let segment = synthetic_segment(&[mutation_with_empty_columns(), mutation_with_static_row()]);
    let json = run_json(&segment, None);
    let updates = json["updates"].as_array().expect("updates array");
    assert_eq!(updates.len(), 2);
    assert_eq!(updates[0]["columns"], serde_json::json!([]));
    assert_eq!(updates[0]["columns_read"], true);
    assert_eq!(updates[1]["columns"], serde_json::json!([]));
    assert_eq!(updates[1]["columns_read"], false);

    let output = run_cli(&segment, None, None);
    assert!(output.status.success(), "text output should succeed");
    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    assert!(
        stdout.contains("columns: (empty)"),
        "parsed empty block is explicit"
    );
    assert!(
        stdout.contains("columns: unknown (not read)"),
        "static-row bailout must label the column names unknown"
    );
}
