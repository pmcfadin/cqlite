//! BTI partition-trie peak-heap budget guard (issue #1679; dhat-gated).
//!
//! ## What it pins
//!
//! `PartitionsTrieWriter::finish()` serializes the `Partitions.db` partition
//! trie. Before #1679 it materialized the ENTIRE nested `BTreeMap` trie tree
//! (`O(partitions)` heap-allocated internal nodes) before writing a single byte.
//! Since the trie keys are fixed-length (9 bytes) and arrive pre-sorted, #1679
//! replaced that with a single left-to-right sweep + a depth-≤9 pending stack:
//! only the root-to-current-leaf path is ever resident.
//!
//! This test builds a trie over **1,000,000** synthetic partitions and asserts
//! the dhat **peak resident heap** (`HeapStats::max_bytes`) stays under a budget
//! that the whole-tree implementation cannot meet. The only `O(partitions)`
//! resident cost that legitimately remains is the accumulated `entries` `Vec`
//! (inherent to the writer's accumulate-then-`finish` model, out of #1679's
//! scope) plus the output `Vec` (removed separately by S3); the depth-≤9 stack
//! is negligible. The whole-tree `BTreeMap` was a SECOND `O(partitions)` cost on
//! top of those, so its removal is a large, deterministic peak-heap drop.
//!
//! ## Calibration (measured 2026-07-17, 1,000,000 partitions, 28 MB output)
//!
//!   - pre-#1679 (whole `BTreeMap` tree resident): peak ~1920.8 MiB (71.8x output)
//!   - post-#1679 (depth-≤9 sweep):                peak ~64.0 MiB   (2.4x output)
//!
//! `BUDGET_BYTES` (128 MiB) sits between the two: comfortably above the
//! post-#1679 peak (entries `Vec` + output `Vec` + tiny stack) yet ~15x below
//! the whole-tree peak, so this FAILS on `main` and PASSES after the fix.
//!
//! ## Run via
//! ```text
//! env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!   cargo test --package cqlite-core --features dhat-heap \
//!   --test issue_1679_bti_trie_alloc_budget -- --test-threads=1 --nocapture
//! ```
//! (`--test-threads=1` is mandatory: `dhat::Profiler` installs a process-global
//! allocator and permits only one live profiler per process.)

#![cfg(feature = "dhat-heap")]

// The dhat allocator must be the global allocator to observe every allocation.
// This test binary is separate from all others, so installing it here does not
// affect normal builds or other test binaries.
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use cqlite_core::storage::sstable::writer::partitions_writer::PartitionsTrieWriter;

/// Number of synthetic partitions to build the trie over.
const PARTITIONS: u64 = 1_000_000;

/// Peak-heap ceiling. The post-#1679 sweep peaks at the accumulated `entries`
/// `Vec` + output `Vec` + a depth-≤9 stack (~40 MB for 1M partitions on the
/// measurement machine); the pre-#1679 whole-`BTreeMap`-tree peak was several
/// times larger. 128 MiB sits well between them and is also the project's stated
/// large-file memory target.
const BUDGET_BYTES: usize = 128 * 1024 * 1024;

#[test]
fn bti_partition_trie_build_peak_is_bounded() {
    let _profiler = dhat::Profiler::builder().testing().build();

    let mut w = PartitionsTrieWriter::new();
    for i in 0u64..PARTITIONS {
        // 16-byte raw partition key; the writer derives the 9-byte trie key via
        // the real Murmur3 BTI encoding, so tokens are well-distributed.
        let mut k = vec![0u8; 16];
        k[0..8].copy_from_slice(&i.to_be_bytes());
        k[8..16].copy_from_slice(&i.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes());
        w.add_partition(&k, i);
    }
    let bytes = w.finish().expect("finish partition trie");

    let stats = dhat::HeapStats::get();
    let output_len = bytes.len();

    eprintln!(
        "issue #1679 BTI trie peak-heap: {PARTITIONS} partitions -> \
         output {output_len} bytes; peak {} bytes ({:.1} MiB); \
         budget {BUDGET_BYTES} bytes ({:.1} MiB); peak/output = {:.2}x",
        stats.max_bytes,
        stats.max_bytes as f64 / (1024.0 * 1024.0),
        BUDGET_BYTES as f64 / (1024.0 * 1024.0),
        stats.max_bytes as f64 / output_len as f64,
    );

    // Non-vacuity: the trie must actually have been produced.
    assert!(
        output_len > 0,
        "issue #1679: expected a non-empty Partitions.db for {PARTITIONS} partitions"
    );
    assert!(
        stats.max_bytes > output_len,
        "issue #1679: peak heap must at least cover the output buffer — a suspiciously \
         low peak means the workload did not run"
    );

    // The core guard: peak must NOT scale with a whole in-memory `BTreeMap` tree.
    assert!(
        stats.max_bytes < BUDGET_BYTES,
        "issue #1679: partition-trie build peak heap regressed to {} bytes (> {} budget). \
         A peak this high means the whole nested BTreeMap trie is resident again — the \
         single-sweep depth-≤9 emitter must hold only the root-to-leaf path.",
        stats.max_bytes,
        BUDGET_BYTES,
    );
}
