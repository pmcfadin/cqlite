//! Streaming channel / cursor mechanism tests for the k-way merge (issues #754 /
//! #2820).
//!
//! PURE CODE MOTION out of `merge/mod.rs` (issue #2820, per the #1116 campsite
//! rule): that file is ~12.5k lines, and these tests are the streaming-channel
//! MECHANISM tests for the very fan-in path issue #2820 rebatched. Nothing here
//! changed behaviourally in the move — the module body is byte-identical to its
//! pre-move form, still `use super::*` over `merge`.
//!
//! What they verify: the bounded `sync_channel` and `RunReader` cursor machinery
//! forward entries without deadlock, preserve ordering, and provide backpressure
//! between producer and consumer.
//!
//! What they do NOT verify: the end-to-end memory bound. That bound — the real
//! producer streaming its source via `stream_all_partitions_for_compaction`
//! (issue #827) — is asserted by the dhat test
//! `tests/test_issue_827_merge_streaming_memory.rs`. The batching subsystem's
//! own resident-rows bound is pinned in `egress_batch_tests.rs` (issue #2820).

use super::*;

/// A synthetic `SSTableRowIterator` backed by a bounded channel. The
/// producer thread is started immediately and blocks once the channel is
/// full, demonstrating true backpressure — memory is bounded to `capacity`
/// entries regardless of `count`.
struct SyntheticStreamingIterator {
    rx: std::sync::mpsc::Receiver<Result<MergeEntry>>,
    _tx_thread: std::thread::JoinHandle<()>,
}

impl SyntheticStreamingIterator {
    /// Produce `count` entries with sequential tokens and the given
    /// `run_index`, streamed through a channel of size `capacity`.
    fn new(count: usize, run_index: usize, capacity: usize) -> Self {
        let (tx, rx) = std::sync::mpsc::sync_channel(capacity);
        let tx_thread = std::thread::spawn(move || {
            for i in 0..count {
                let entry = MergeEntry::new(
                    run_index,
                    DecoratedKey::new(i as i64, vec![i as u8]),
                    None,
                    (i as i64) * 1000,
                    RowData::Live { cells: vec![] },
                );
                if tx.send(Ok(entry)).is_err() {
                    return;
                }
            }
        });
        Self {
            rx,
            _tx_thread: tx_thread,
        }
    }
}

impl SSTableRowIterator for SyntheticStreamingIterator {
    fn next(&mut self) -> Option<Result<MergeEntry>> {
        self.rx.recv().ok()
    }
}

/// Merge two synthetic streaming sources (channel capacity = 4, 20 entries
/// each) and assert that all 40 unique tokens survive and global order is
/// preserved.
///
/// This verifies that the RunReader / heap machinery correctly drains
/// bounded-channel sources: with capacity=4 the channel holds ≤ 4 entries
/// per source (≤ 8 total) while the test runs, demonstrating correct
/// ordering and completeness through a small-capacity channel.
///
/// NOTE: this test exercises the synthetic streaming-iterator path only; the
/// end-to-end memory bound for the real SSTableRowIteratorAdapter (whose
/// producer streams its source one partition at a time, issue #827) is
/// asserted by the dhat test tests/test_issue_827_merge_streaming_memory.rs.
#[test]
fn test_kway_merge_with_streaming_sources_preserves_order() {
    use crate::schema::{KeyColumn, TableSchema};
    use std::collections::HashMap;

    let schema = TableSchema {
        keyspace: "stream_ks".to_string(),
        table: "stream_tbl".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    // Two sources with disjoint tokens:
    //   source 0 → even tokens 0, 2, 4, …, 38
    //   source 1 → odd  tokens 1, 3, 5, …, 39
    // Channel capacity = 4 << total per source (20). At steady state
    // ≤ 4 entries per source live in the channel, ≤ 8 total.
    // (These are synthetic in-memory producers, not real SSTableReaders.)
    const N: usize = 20;
    const CHANNEL_CAP: usize = 4;

    let (tx0, rx0) = std::sync::mpsc::sync_channel::<Result<MergeEntry>>(CHANNEL_CAP);
    let (tx1, rx1) = std::sync::mpsc::sync_channel::<Result<MergeEntry>>(CHANNEL_CAP);

    // Producer thread 0: even tokens.
    std::thread::spawn(move || {
        for i in 0..N {
            let token = (i * 2) as i64;
            let entry = MergeEntry::new(
                0,
                DecoratedKey::new(token, vec![(i * 2) as u8]),
                None,
                1000,
                RowData::Live { cells: vec![] },
            );
            if tx0.send(Ok(entry)).is_err() {
                return;
            }
        }
    });

    // Producer thread 1: odd tokens.
    std::thread::spawn(move || {
        for i in 0..N {
            let token = (i * 2 + 1) as i64;
            let entry = MergeEntry::new(
                1,
                DecoratedKey::new(token, vec![(i * 2 + 1) as u8]),
                None,
                1000,
                RowData::Live { cells: vec![] },
            );
            if tx1.send(Ok(entry)).is_err() {
                return;
            }
        }
    });

    struct ChannelIterator(std::sync::mpsc::Receiver<Result<MergeEntry>>);
    impl SSTableRowIterator for ChannelIterator {
        fn next(&mut self) -> Option<Result<MergeEntry>> {
            self.0.recv().ok()
        }
    }

    let runs: Vec<RunReader> = vec![
        RunReader::new(Box::new(ChannelIterator(rx0))),
        RunReader::new(Box::new(ChannelIterator(rx1))),
    ];

    let mut merger = KWayMerger {
        runs,
        heap: BinaryHeap::new(),
        current_partition: None,
        gc_before_secs: None,
        now_secs: None,
        purge_safe: false,
        max_purgeable_timestamp: None,
        schema_arc: std::sync::Arc::new(schema.clone()),
        schema,
        _egress_slot: None,
    };

    // Drain all partitions and verify ordering + completeness.
    let mut token_set = std::collections::BTreeSet::new();
    let mut prev_token: Option<i64> = None;
    loop {
        match merger.step().expect("step must not fail") {
            MergeStep::Complete => break,
            MergeStep::Partition { key, .. } => {
                // Tokens must arrive in ascending order.
                if let Some(pt) = prev_token {
                    assert!(
                        key.token >= pt,
                        "out-of-order token {} after {}",
                        key.token,
                        pt
                    );
                }
                prev_token = Some(key.token);
                token_set.insert(key.token);
            }
        }
    }

    // All 2×N unique tokens must be present.
    assert_eq!(
        token_set.len(),
        N * 2,
        "expected {} unique partitions, got {}",
        N * 2,
        token_set.len()
    );
    for expected in 0..(N as i64 * 2) {
        assert!(
            token_set.contains(&expected),
            "token {} is missing from merged output",
            expected
        );
    }
}

/// Verify that the streaming adapter drains all entries correctly when the
/// channel capacity is smaller than the total number of entries (1000 entries,
/// capacity 256). This confirms the producer blocks on sends and the consumer
/// pulls them out one at a time without deadlock.
#[test]
fn test_streaming_iterator_drains_all_entries_with_backpressure() {
    const TOTAL: usize = 1000;
    // capacity < TOTAL: forces producer to block when channel is full.
    let mut iter = SyntheticStreamingIterator::new(TOTAL, 0, STREAMING_CHANNEL_CAPACITY);
    let mut count = 0usize;
    while let Some(result) = iter.next() {
        result.expect("entry must not be an error");
        count += 1;
    }
    assert_eq!(count, TOTAL, "all {} entries must be produced", TOTAL);
}

/// Verify the RunReader correctly wraps a streaming iterator: peek and
/// advance work, exhaustion is detected, buffer refills lazily even when
/// the channel capacity (4) is far smaller than the total entries (50).
#[test]
fn test_run_reader_with_streaming_source() {
    const N: usize = 50;
    // Channel capacity 4 << N: tests lazy refill under backpressure.
    let iter = SyntheticStreamingIterator::new(N, 0, 4);
    let mut reader = RunReader::new(Box::new(iter));

    let mut seen = 0usize;
    loop {
        match reader.peek().expect("peek must not error") {
            None => break,
            Some(_) => {
                reader.advance().expect("advance must not error");
                seen += 1;
            }
        }
    }

    assert_eq!(seen, N, "RunReader must surface all {} entries", N);
    assert!(
        reader.is_exhausted(),
        "RunReader must be exhausted after drain"
    );
}
