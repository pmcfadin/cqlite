//! Streaming-decode memory bound for the CommitLog reader (issue #2389, dhat-gated).
//!
//! Requirement (spec: bounded memory via streaming decode): the reader yields
//! mutations incrementally and does NOT materialize the whole segment's mutation
//! set as one collection. The real committed fixture is only ~63 KB — too small
//! to exercise a memory bound — so this test synthesizes a large, **validly
//! framed** segment by repeating the real Cassandra-produced mutation body bytes
//! under fresh sync-marker + per-record CRC framing (the framing formulas are the
//! ones verified byte-for-byte against the real segment; only the count is
//! synthetic, not the format).
//!
//! Under dhat, iterating the whole segment must keep peak live heap close to the
//! raw file size (which the reader loads once, bounded by `MAX_SEGMENT_BYTES`),
//! NOT raw + one `Mutation` per record retained. A materializing decoder that
//! collected every decoded mutation would exceed this bound.
//!
//! Gated on the `dhat-heap` feature (runs in the profiling job, not the normal
//! gate) — mirrors `test_issue_827_merge_streaming_memory.rs`.
#![cfg(feature = "dhat-heap")]

use cqlite_core::storage::commitlog::frame::marker_crc;
use cqlite_core::storage::commitlog::CommitLogReader;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

// The real first user mutation body (commitlog_test.users id=1 => alice/30),
// captured from the Cassandra 5.0.2 segment. Reused as the record payload.
const ALICE_BODY_HEX: &str = "01d6de7150844811f1bf5abf4cbc47cb4a040000000110fd36c1327cb48c00000203616765046e616d65012400080000001e0805616c69636501";

fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn be(v: i32) -> [u8; 4] {
    v.to_be_bytes()
}

/// Build a valid uncompressed segment with `record_count` copies of the real
/// mutation body, all inside one sync section.
fn build_segment(segment_id: i64, record_count: usize) -> Vec<u8> {
    let body = hex(ALICE_BODY_HEX);
    let mut out = Vec::new();

    // Descriptor header: version 7, id, params "{}", crc.
    let params = b"{}";
    out.extend_from_slice(&7i32.to_be_bytes());
    out.extend_from_slice(&segment_id.to_be_bytes());
    out.extend_from_slice(&(params.len() as u16).to_be_bytes());
    out.extend_from_slice(params);
    let mut h = crc32fast::Hasher::new();
    h.update(&7i32.to_be_bytes());
    let idu = segment_id as u64;
    h.update(&((idu & 0xFFFF_FFFF) as u32).to_be_bytes());
    h.update(&((idu >> 32) as u32).to_be_bytes());
    h.update(&(params.len() as u32).to_be_bytes());
    h.update(params);
    out.extend_from_slice(&h.finalize().to_be_bytes());

    let header_len = out.len();
    // Records: size, sizeCrc, body, bodyCrc.
    let record_len = 4 + 4 + body.len() + 4;
    let section_end = header_len + 8 + record_count * record_len;

    // Marker 0 at header_len -> section_end.
    out.extend_from_slice(&be(section_end as i32));
    out.extend_from_slice(&marker_crc(segment_id, header_len).to_be_bytes());

    for _ in 0..record_count {
        let size = body.len() as i32;
        out.extend_from_slice(&be(size));
        let mut sc = crc32fast::Hasher::new();
        sc.update(&be(size));
        out.extend_from_slice(&sc.finalize().to_be_bytes());
        out.extend_from_slice(&body);
        let mut bc = crc32fast::Hasher::new();
        bc.update(&be(size));
        bc.update(&body);
        out.extend_from_slice(&bc.finalize().to_be_bytes());
    }
    // Terminating marker with nextMarker == 0 -> clean end.
    out.extend_from_slice(&[0u8; 8]);
    out
}

#[test]
fn streaming_decode_peak_heap_tracks_file_not_mutation_set() {
    // ~300k records × ~74 bytes/record ≈ 22 MiB raw segment. Retaining a
    // `Mutation` per record (table id + pk + column-name strings) would add tens
    // of MiB on top; streaming keeps it near the raw file size.
    let record_count = 300_000;
    let bytes = build_segment(1_784_558_302_598, record_count);
    let raw_len = bytes.len();
    assert!(raw_len < 128 * 1024 * 1024, "stay under the reader cap");

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("CommitLog-7-synthetic.log");
    std::fs::write(&path, &bytes).expect("write segment");

    let profiler = dhat::Profiler::builder().testing().build();

    let reader = CommitLogReader::open(&path).expect("open synthetic segment");
    let mut decoded = 0usize;
    // Stream: drop each mutation immediately (never collect into a Vec).
    for res in reader.mutations() {
        let m = res.expect("valid record decodes");
        decoded += m.updates.len();
    }
    assert_eq!(decoded, record_count, "every record decoded");

    let stats = dhat::HeapStats::get();
    drop(profiler);

    // Peak must stay close to the raw file (loaded once) plus modest working
    // set — well under raw + one retained Mutation per record, and under the
    // 128 MiB budget. 2× raw is a generous ceiling that a materializing decoder
    // (raw + full decoded set) would blow past for 300k records.
    let ceiling = raw_len * 2;
    assert!(
        stats.max_bytes < ceiling,
        "peak heap {} must stay under {} (raw file {}); streaming, not materializing",
        stats.max_bytes,
        ceiling,
        raw_len
    );
}
