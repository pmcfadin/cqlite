//! Issue #1391: WAL replay must surface lossiness instead of silently
//! returning a clean-looking `Vec` (and letting the next flush truncate the
//! evidence).
//!
//! Posture under test: **fail-fast-then-report**. Because the WAL framing has
//! no sync markers, once an entry fails CRC (or declares an implausible length)
//! the offset of the next entry cannot be recovered authoritatively. Replay
//! therefore recovers the valid prefix, STOPS at the first unrecoverable
//! corruption, and records the loss in a [`RecoveryReport`] — matching
//! Cassandra's `CommitLogReplayer`, which fails fast on unexpected corruption.
//!
//! These tests exercise `replay()` directly (via `open_existing`, which — for
//! corruption, as opposed to a torn tail — leaves the segment physically
//! intact). Criterion 5 (the engine-level flush guard) lives with the
//! `WriteEngine` tests in `write_engine/mod.rs`.
#![cfg(feature = "write-support")]

use cqlite_core::types::Value;
use cqlite_core::write_engine::{
    CellOperation, Mutation, PartitionKey, RecoveryReport, TableId, WriteAheadLog,
};
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// A mutation whose single Write op carries a distinguishing name, so recovered
/// entries can be matched against the originals (Mutation has no `PartialEq`).
fn tagged_mutation(id: i32, tag: &str) -> Mutation {
    Mutation::new(
        TableId::new("ks", "tbl"),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(tag.to_string()),
        }],
        1_700_000_000_000_000,
        None,
    )
}

fn tag_of(m: &Mutation) -> String {
    match &m.operations[0] {
        CellOperation::Write {
            value: Value::Text(t),
            ..
        } => t.clone(),
        other => panic!("expected Write op, got {other:?}"),
    }
}

/// Write `tags.len()` entries (each synced) and return the WAL path plus the
/// byte offset at the END of each entry (offsets[i] = end of entry i).
fn write_entries(dir: &TempDir, tags: &[&str]) -> (PathBuf, Vec<u64>) {
    let mut wal = WriteAheadLog::create(dir.path()).unwrap();
    let mut ends = Vec::with_capacity(tags.len());
    for (i, tag) in tags.iter().enumerate() {
        wal.append(&tagged_mutation(i as i32, tag)).unwrap();
        wal.sync().unwrap();
        ends.push(wal.size());
    }
    let path = wal.path().to_path_buf();
    drop(wal);
    (path, ends)
}

fn replay_file(path: &Path) -> RecoveryReport {
    // `open_existing` classifies the stop: it trims only a torn tail, leaving a
    // corrupt segment physically intact so replay can see (and report) it.
    let wal = WriteAheadLog::open_existing(path).unwrap();
    wal.replay().unwrap()
}

/// Overwrite `len` bytes at `at` in the WAL file (used to corrupt a field).
fn overwrite(path: &Path, at: u64, bytes: &[u8]) {
    let mut f = OpenOptions::new().write(true).open(path).unwrap();
    f.seek(SeekFrom::Start(at)).unwrap();
    f.write_all(bytes).unwrap();
    f.sync_all().unwrap();
}

/// Criterion 2: corruption-in-middle. Entries A, B, C with B's PAYLOAD
/// bit-flipped. Replay must recover the valid prefix [A], report B corrupt, and
/// mark the recovery lossy — never silently return [A] or [A, C] unreported.
#[test]
fn replay_middle_payload_corruption_reports_and_recovers_prefix() {
    let dir = TempDir::new().unwrap();
    let (path, ends) = write_entries(&dir, &["A", "B", "C"]);
    let end_a = ends[0];
    let total = ends[2];

    // Flip a bit in B's payload (first payload byte, just past B's 8-byte header).
    let b_payload_start = end_a + 8;
    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    f.seek(SeekFrom::Start(b_payload_start)).unwrap();
    let mut byte = [0u8; 1];
    use std::io::Read;
    f.read_exact(&mut byte).unwrap();
    f.seek(SeekFrom::Start(b_payload_start)).unwrap();
    f.write_all(&[byte[0] ^ 0x01]).unwrap();
    f.sync_all().unwrap();
    drop(f);

    let report = replay_file(&path);

    assert_eq!(
        report.mutations.iter().map(tag_of).collect::<Vec<_>>(),
        vec!["A".to_string()],
        "only the valid prefix [A] is recovered; C is NOT silently included"
    );
    assert!(!report.is_clean(), "middle corruption must be surfaced");
    assert_eq!(report.corrupt_entries, 1, "B must be reported corrupt");
    assert!(
        report.stopped_early,
        "replay must stop at the corrupt entry"
    );
    assert_eq!(
        report.bytes_skipped,
        total - end_a,
        "bytes_skipped must cover B and C (everything past the valid prefix)"
    );
}

/// Criterion 3: corrupt LENGTH field. B's length is overwritten with a huge
/// value. Replay must NOT misalign into garbage: it recovers [A], stops, and
/// makes C's fate explicit (unrecovered, counted in bytes_skipped).
#[test]
fn replay_corrupt_length_does_not_misalign() {
    let dir = TempDir::new().unwrap();
    let (path, ends) = write_entries(&dir, &["A", "B", "C"]);
    let end_a = ends[0];
    let total = ends[2];

    // B's 4-byte length prefix lives at offset end_a. Corrupt it to > 16 MiB.
    overwrite(&path, end_a, &0xFFFF_FFFFu32.to_le_bytes());

    let report = replay_file(&path);

    assert_eq!(
        report.mutations.iter().map(tag_of).collect::<Vec<_>>(),
        vec!["A".to_string()],
        "a corrupt length must not cause a misaligned garbage decode; only [A] survives"
    );
    assert!(!report.is_clean());
    assert_eq!(report.corrupt_entries, 1);
    assert!(report.stopped_early);
    assert_eq!(
        report.bytes_skipped,
        total - end_a,
        "C's fate is explicit: it is part of the unrecovered trailing bytes"
    );
}

/// Criterion 3 variant: length corrupted to a small in-file value that passes
/// the >16MiB guard AND does not overshoot EOF, so CRC verification is the
/// authoritative backstop. The CRC (computed over B's original full payload)
/// cannot match the truncated read, so replay stops and reports — it never
/// decodes the mis-framed bytes as a mutation.
#[test]
fn replay_subthreshold_bad_length_fails_crc_not_garbage() {
    let dir = TempDir::new().unwrap();
    let (path, ends) = write_entries(&dir, &["A", "B", "C"]);
    let end_a = ends[0];

    // A tiny but plausible length (3 bytes): readable within the file, below
    // the 16MiB guard, but wrong — the stored CRC (over the full payload) fails.
    overwrite(&path, end_a, &3u32.to_le_bytes());

    let report = replay_file(&path);

    assert_eq!(
        report.mutations.iter().map(tag_of).collect::<Vec<_>>(),
        vec!["A".to_string()],
    );
    assert!(!report.is_clean());
    assert!(report.stopped_early);
    assert_eq!(report.corrupt_entries, 1);
}

/// Criterion 4: oversize length must be SURFACED, not a silent break. A single
/// entry whose length is corrupted huge yields an empty-but-not-clean report
/// whose stop is visible to the caller.
#[test]
fn replay_oversize_length_is_reported_not_silently_broken() {
    let dir = TempDir::new().unwrap();
    let (path, ends) = write_entries(&dir, &["A"]);
    let total = ends[0];

    overwrite(&path, 0, &0xFFFF_FFFFu32.to_le_bytes());

    let report = replay_file(&path);

    assert!(report.mutations.is_empty());
    assert!(
        !report.is_clean(),
        "an oversize entry must be surfaced as non-clean, not silently dropped"
    );
    assert_eq!(report.corrupt_entries, 1);
    assert!(report.stopped_early);
    assert_eq!(report.bytes_skipped, total);
}

/// A clean multi-entry log must replay losslessly with a clean report
/// (regression guard so the corruption path does not over-trigger).
#[test]
fn replay_clean_log_is_clean() {
    let dir = TempDir::new().unwrap();
    let (path, _ends) = write_entries(&dir, &["A", "B", "C"]);

    let report = replay_file(&path);

    assert_eq!(
        report.mutations.iter().map(tag_of).collect::<Vec<_>>(),
        vec!["A".to_string(), "B".to_string(), "C".to_string()],
    );
    assert!(report.is_clean());
    assert_eq!(report.corrupt_entries, 0);
    assert!(!report.stopped_early);
    assert_eq!(report.bytes_skipped, 0);
}

/// Criterion 6: misdirected-skip property test. For a single-bit flip anywhere
/// in a multi-entry log, replay must NEVER return a mutation that was not
/// written. Under fail-fast this is guaranteed because CRC32 detects every
/// single-bit error, so a flipped entry is never decoded — the recovered set is
/// always an exact PREFIX of the originals.
#[test]
fn replay_never_returns_unwritten_mutation_under_single_bit_flip() {
    let dir = TempDir::new().unwrap();
    let tags = ["A", "B", "C"];
    let (path, _ends) = write_entries(&dir, &tags);

    // Canonical bytes; mutate a copy per bit position.
    let canonical = std::fs::read(&path).unwrap();
    let flip_path = dir.path().join("commitlog.wal"); // reuse the same WAL name

    for byte_idx in 0..canonical.len() {
        for bit in 0..8u8 {
            let mut bytes = canonical.clone();
            bytes[byte_idx] ^= 1 << bit;
            std::fs::write(&flip_path, &bytes).unwrap();

            let report = {
                let wal = WriteAheadLog::open_existing(&flip_path).unwrap();
                wal.replay().unwrap()
            };

            // The recovered mutations must be an exact prefix of the originals:
            // no garbage decode, no reordering, nothing that was not written.
            assert!(
                report.mutations.len() <= tags.len(),
                "flip at byte {byte_idx} bit {bit}: recovered more entries than were written"
            );
            for (i, m) in report.mutations.iter().enumerate() {
                assert_eq!(
                    tag_of(m),
                    tags[i],
                    "flip at byte {byte_idx} bit {bit}: recovered a mutation \
                     that was not written (or out of order) at index {i}"
                );
            }
        }
    }
}
