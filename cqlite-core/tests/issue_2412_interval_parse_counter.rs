//! Issue #2412 — the lazy Summary-guided `Index.db` interval accessor records a
//! bounded **interval** parse on the DISTINCT counter, never a full parse.
//!
//! Spec `lazy-big-partition-index` Requirement 5 (scenario "Lazy open reports zero
//! full parses; interval work is counted separately") + design §F: a single
//! Summary-bounded interval read increments
//! `cqlite.sstable.index_interval_parses_total` by exactly 1 and leaves
//! `cqlite.sstable.index_parses_total` (the field-round full-parse probe) at 0. This
//! guarantees a lazy-open regression that accidentally full-parses stays visible on
//! the full-parse counter.
//!
//! Separate integration-test process: the OTel capture harness installs a
//! PROCESS-GLOBAL meter provider, so this must not share cqlite-core's parallel
//! `--lib` unit-test binary (roborev #2163 / #2385 precedent).
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-core --features observability-testing \
//!   --test issue_2412_interval_parse_counter
//! ```

#![cfg(feature = "observability-testing")]

use cqlite_core::observability::{catalog, testing};
use cqlite_core::parser::vint::encode_vuint;
use cqlite_core::storage::sstable::summary_reader::interval::lookup_key_in_interval;
use cqlite_core::storage::sstable::summary_reader::SummaryInterval;

/// Encode one BIG `Index.db` entry: `[key_len u16 BE][key][data_offset vint][promoted_len vint=0]`.
fn encode_entry(key: &[u8], data_offset: u64) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(key.len() as u16).to_be_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(&encode_vuint(data_offset));
    out.extend_from_slice(&encode_vuint(0));
    out
}

fn build(keys_offsets: &[(&[u8], u64)]) -> Vec<u8> {
    let mut buf = Vec::new();
    for (k, off) in keys_offsets {
        buf.extend_from_slice(&encode_entry(k, *off));
    }
    buf
}

#[test]
fn interval_read_counts_one_interval_parse_and_zero_full_parses() {
    let mc = testing::metrics_capture();

    // Synthetic Index.db: [prefix][interval][trailing]; only the interval is read.
    let prefix = build(&[(b"before0", 0), (b"before1", 5)]);
    let interval = build(&[(b"target", 900), (b"neighbor", 950)]);
    let trailing = build(&[(b"after0", 1000)]);
    let mut file_bytes = prefix.clone();
    file_bytes.extend_from_slice(&interval);
    file_bytes.extend_from_slice(&trailing);

    let dir = std::env::temp_dir().join(format!(
        "cqlite-2412-counter-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).expect("mkdir");
    let path = dir.join("nb-1-big-Index.db");
    std::fs::write(&path, &file_bytes).expect("write index");

    let iv = SummaryInterval {
        start_position: prefix.len() as u64,
        end_position: Some((prefix.len() + interval.len()) as u64),
        sample_index: 1,
    };

    let rt = tokio::runtime::Runtime::new().expect("runtime");

    // Reset so the whole operation is measured from zero.
    mc.reset();
    let res = rt
        .block_on(lookup_key_in_interval(&path, iv, b"target", 128))
        .expect("interval read");
    assert!(
        res.entry.is_some(),
        "target must resolve inside the interval"
    );
    assert_eq!(res.entries_touched, 1, "bounded: matched the first entry");

    let collected = mc.flush_and_collect();
    let interval_parses = collected.counter_sum(catalog::INDEX_INTERVAL_PARSES_TOTAL);
    let full_parses = collected.counter_sum(catalog::INDEX_PARSES_TOTAL);

    assert_eq!(
        interval_parses, 1.0,
        "one bounded interval parse must be recorded (got {interval_parses})"
    );
    assert_eq!(
        full_parses, 0.0,
        "a lazy interval read must NOT record a full Index.db parse (got {full_parses})"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
