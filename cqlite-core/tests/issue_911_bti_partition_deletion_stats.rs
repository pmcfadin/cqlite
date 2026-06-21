//! Follow-up to #911: the canonical `da` (BtiFormat) `StatsMetadata` must
//! report `hasPartitionLevelDeletions` truthfully.
//!
//! `build_stats_component_da` originally hard-coded the
//! `hasPartitionLevelDeletions` boolean to `false`, so any `da` SSTable that
//! contained a partition-level delete lied about its deletion marker in
//! Statistics.db. This test suite proves the marker is now driven by the actual
//! mutations written:
//!
//! * a BTI SSTable carrying a partition-level tombstone serialises the marker
//!   byte as `0x01`;
//! * a BTI SSTable with no partition delete serialises `0x00`.
//!
//! ## Encoding authority (cassandra-5.0.0)
//!
//! `StatsMetadata.StatsMetadataSerializer.serialize` (StatsMetadata.java line
//! 495) emits the field as `out.writeBoolean(component.hasPartitionLevelDeletions)`,
//! gated by `version.hasPartitionLevelDeletionsPresenceMarker()` (true for
//! `BtiFormat.BtiVersion`). `DataOutput.writeBoolean` writes a single byte:
//! `0x01` for true, `0x00` for false. The field sits between `originatingHostId`
//! and the `hasKeyRange` first/last keys in the `da` STATS body.
//!
//! ## What runs without Docker
//!
//! The marker-byte assertions parse the writer-produced Statistics.db TOC,
//! extract the STATS component body, and compare the partition-deleted vs
//! non-deleted bodies. No Cassandra required.
//!
//! ## What is Docker-gated
//!
//! `partition_deletion_reads_back_under_cassandra5_sstabledump` runs Cassandra
//! 5's `sstabledump` against the writer-produced SSTable and asserts the
//! partition deletion is reflected in the dump (`partition.deletion_info`). It
//! SKIPS CLEANLY when Docker / a `cassandra:5.0` image is absent, exactly like
//! the #911 / #819 e2e paths.

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::writer::{SSTableFormat, SSTableInfo, SSTableWriter};
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, Mutation, PartitionKey, PartitionTombstone, TableId,
};
use cqlite_core::types::Value;
use tempfile::TempDir;

// ════════════════════════════════════════════════════════════════════════════
// Docker / Cassandra availability gate (skip cleanly when absent) — mirrors
// issue_911_bti_sstabledump_parity.rs.
// ════════════════════════════════════════════════════════════════════════════

const SSTABLEDUMP: &str = "/opt/cassandra/tools/bin/sstabledump";

fn cassandra_5_image() -> Option<String> {
    let info = Command::new("docker").arg("info").output().ok()?;
    if !info.status.success() {
        return None;
    }
    let images = Command::new("docker")
        .args(["images", "--format", "{{.Repository}}:{{.Tag}}"])
        .output()
        .ok()?;
    if !images.status.success() {
        return None;
    }
    let listing = String::from_utf8_lossy(&images.stdout);
    let mut candidate: Option<String> = None;
    for line in listing.lines() {
        let line = line.trim();
        if line == "cassandra:5.0" {
            return Some(line.to_string());
        }
        if line.starts_with("cassandra:5.0.") && candidate.is_none() {
            candidate = Some(line.to_string());
        }
    }
    candidate
}

fn run_sstabledump(image: &str, sstable_dir: &Path, data_file: &str) -> Result<String, String> {
    let mount = format!("{}:/data:ro", sstable_dir.display());
    let out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--entrypoint",
            SSTABLEDUMP,
            "-v",
            &mount,
            image,
            &format!("/data/{data_file}"),
        ])
        .output()
        .map_err(|e| format!("failed to spawn docker: {e}"))?;
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    if stderr.contains("Exception") || !stdout.trim_start().starts_with('[') {
        return Err(format!(
            "sstabledump did not produce a JSON dump.\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
        ));
    }
    Ok(stdout)
}

// ════════════════════════════════════════════════════════════════════════════
// Schema + mutations: simple(pk int PRIMARY KEY, payload text), no clustering.
// ════════════════════════════════════════════════════════════════════════════

fn simple_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "simple".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "payload".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn live_row(pk: i32, payload: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new("test_ks", "simple"),
        PartitionKey::single("pk", Value::Integer(pk)),
        None,
        vec![CellOperation::Write {
            column: "payload".to_string(),
            value: Value::Text(payload.to_string()),
        }],
        ts,
        None,
    )
}

/// A mutation whose only effect is a partition-level tombstone on `pk`.
fn partition_delete(pk: i32, deletion_micros: i64, local_secs: i32) -> Mutation {
    let mut m = Mutation::new(
        TableId::new("test_ks", "simple"),
        PartitionKey::single("pk", Value::Integer(pk)),
        None,
        vec![],
        deletion_micros,
        None,
    );
    m.partition_tombstone = Some(PartitionTombstone {
        deletion_time: deletion_micros,
        local_deletion_time: local_secs,
    });
    m
}

/// Write a single-partition BTI SSTable. When `with_partition_delete` is set the
/// partition (pk=1) carries a partition-level tombstone in addition to a live
/// row; otherwise it is just a live row. The partition key (and hence
/// first/last key) is identical in both cases so the only Statistics.db
/// difference is the `hasPartitionLevelDeletions` marker byte.
async fn write_simple_bti(dir: &Path, with_partition_delete: bool) -> SSTableInfo {
    let schema = simple_schema();
    let mut writer =
        SSTableWriter::with_format(dir.to_path_buf(), 1, &schema, 16, SSTableFormat::Bti).unwrap();

    let pk = 1;
    let mut muts = vec![live_row(pk, "alive", 1_000_000)];
    if with_partition_delete {
        // deletion_time (micros) > the live row ts so the partition is deleted.
        muts.push(partition_delete(pk, 2_000_000, 2));
    }
    let key = muts[0].decorated_key(&schema).unwrap();
    writer.write_partition(key, muts).unwrap();
    writer.finish().await.unwrap()
}

// ════════════════════════════════════════════════════════════════════════════
// Statistics.db TOC parsing — extract the STATS component body.
// ════════════════════════════════════════════════════════════════════════════

const METADATA_TYPE_STATS: u32 = 2;
const METADATA_TYPE_SERIALIZATION_HEADER: u32 = 3;

/// Parse the Statistics.db TOC and return the STATS component body (without its
/// trailing 4-byte CRC). The TOC layout is:
///   u32 count, u32 checksum, count×(u32 type, u32 offset), u32 checksum.
/// Each component body is `offset_next - offset_this - 4` bytes (the 4 being
/// this component's trailing CRC). The last component (SERIALIZATION_HEADER)
/// ends 4 bytes before EOF.
fn read_stats_component_body(stats_path: &Path) -> Vec<u8> {
    let bytes = std::fs::read(stats_path).unwrap();
    let rd_u32 = |b: &[u8], p: usize| u32::from_be_bytes([b[p], b[p + 1], b[p + 2], b[p + 3]]);

    let count = rd_u32(&bytes, 0) as usize;
    // offsets: count(4) + checksum(4) = 8, then entries.
    let mut entries: Vec<(u32, u32)> = Vec::with_capacity(count);
    let mut p = 8;
    for _ in 0..count {
        let ty = rd_u32(&bytes, p);
        let off = rd_u32(&bytes, p + 4);
        entries.push((ty, off));
        p += 8;
    }

    // The STATS body runs from its offset up to the SERIALIZATION_HEADER offset
    // (the next component) minus the 4-byte STATS CRC.
    let stats_off = entries
        .iter()
        .find(|(t, _)| *t == METADATA_TYPE_STATS)
        .map(|(_, o)| *o as usize)
        .expect("STATS component in TOC");
    let header_off = entries
        .iter()
        .find(|(t, _)| *t == METADATA_TYPE_SERIALIZATION_HEADER)
        .map(|(_, o)| *o as usize)
        .expect("SERIALIZATION_HEADER component in TOC");

    assert!(header_off > stats_off, "header follows stats");
    bytes[stats_off..header_off - 4].to_vec()
}

fn stats_path_for(info: &SSTableInfo) -> PathBuf {
    info.stats_path.clone()
}

/// Locate the `hasPartitionLevelDeletions` marker byte within a `da` STATS body.
///
/// The `da` STATS body tail (cassandra-5.0.0 `StatsMetadataSerializer.serialize`)
/// is, in order: `hasPartitionLevelDeletions` (1 byte), then `hasKeyRange`
/// firstKey + lastKey (each `ByteBufferUtil.writeWithVIntLength`: an unsigned
/// VInt length followed by the raw key bytes), then `tokenSpaceCoverage`
/// (8-byte double). We walk in from the tail: drop the 8-byte token coverage,
/// then parse lastKey and firstKey (VInt length + bytes) backwards is awkward, so
/// instead we anchor on the marker position computed from the known partition
/// key length (single 4-byte int => 1-byte VInt length each).
fn partition_deletion_marker(body: &[u8], key_len: usize) -> u8 {
    // Each writeWithVIntLength field with a <128-byte payload uses a single-byte
    // unsigned VInt length, so firstKey/lastKey occupy `1 + key_len` bytes each.
    let key_field = 1 + key_len;
    let tail = key_field + key_field + 8; // firstKey + lastKey + tokenSpaceCoverage
    let marker_idx = body
        .len()
        .checked_sub(tail + 1)
        .expect("STATS body long enough to contain the marker + key range + coverage");
    body[marker_idx]
}

// ════════════════════════════════════════════════════════════════════════════
// Marker-byte assertions (no Docker required).
// ════════════════════════════════════════════════════════════════════════════

/// A BTI SSTable that contains a partition-level tombstone must serialise
/// `hasPartitionLevelDeletions = 0x01`; an otherwise-identical SSTable with no
/// partition delete must serialise `0x00`. The two STATS bodies are identical
/// except for that single marker byte.
#[tokio::test]
async fn da_stats_marks_partition_level_deletion() {
    let no_del_dir = TempDir::new().unwrap();
    let del_dir = TempDir::new().unwrap();

    let no_del = write_simple_bti(no_del_dir.path(), false).await;
    let with_del = write_simple_bti(del_dir.path(), true).await;

    // Confirm the canonical da component set.
    assert_eq!(
        with_del
            .data_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap(),
        "da-1-bti-Data.db",
        "writer must emit a da-bti Data.db"
    );

    let no_del_stats = read_stats_component_body(&stats_path_for(&no_del));
    let with_del_stats = read_stats_component_body(&stats_path_for(&with_del));

    // pk=1 is a single 4-byte int partition key in both SSTables, so the
    // firstKey/lastKey fields (and hence the marker position relative to the
    // tail) are identical. The bodies differ in LENGTH because the partition
    // tombstone also populates the tombstone drop-time histogram — that is
    // expected and is why we anchor the marker by parsing the tail rather than
    // diffing the whole body.
    const PK_LEN: usize = 4; // i32 partition key

    let no_del_marker = partition_deletion_marker(&no_del_stats, PK_LEN);
    let with_del_marker = partition_deletion_marker(&with_del_stats, PK_LEN);

    assert_eq!(
        no_del_marker, 0x00,
        "no partition delete => hasPartitionLevelDeletions = 0x00"
    );
    assert_eq!(
        with_del_marker, 0x01,
        "partition delete present => hasPartitionLevelDeletions = 0x01"
    );

    eprintln!(
        "[#911 partition-deletion] da StatsMetadata hasPartitionLevelDeletions: 0x00 (no delete) \
         vs 0x01 (partition delete)."
    );
}

// ════════════════════════════════════════════════════════════════════════════
// LIVE (Docker-gated): Cassandra 5 reads the partition deletion back.
// ════════════════════════════════════════════════════════════════════════════

/// Cassandra 5's `sstabledump` opens the writer-produced `da` SSTable and
/// reports the partition-level deletion (`partition.deletion_info` with a
/// non-`LIVE` `marked_deleted`). This exercises the real `StatsComponent.load`
/// path that consumes `hasPartitionLevelDeletions`. Skipped cleanly without
/// Docker / a `cassandra:5.0` image.
#[tokio::test]
async fn partition_deletion_reads_back_under_cassandra5_sstabledump() {
    let Some(image) = cassandra_5_image() else {
        eprintln!(
            "[skip] partition_deletion_reads_back_under_cassandra5_sstabledump: Docker or a \
             cassandra:5.0 image is not available."
        );
        return;
    };

    let dir = TempDir::new().unwrap();
    let info = write_simple_bti(dir.path(), true).await;
    let sstable_dir = info
        .data_path
        .parent()
        .expect("Data.db parent dir")
        .to_path_buf();
    let data_file = info
        .data_path
        .file_name()
        .and_then(|n| n.to_str())
        .expect("Data.db file name")
        .to_string();

    let dump = run_sstabledump(&image, &sstable_dir, &data_file).unwrap_or_else(|e| {
        panic!("Cassandra 5 sstabledump FAILED to read the partition-deleted da-bti SSTable:\n{e}")
    });
    let json: serde_json::Value =
        serde_json::from_str(&dump).expect("sstabledump output must be valid JSON");
    let partitions = json.as_array().expect("dump is a JSON array of partitions");
    assert_eq!(partitions.len(), 1, "exactly one partition (pk=1)");

    let p = &partitions[0];
    let del = &p["partition"]["deletion_info"];
    assert!(
        del.is_object(),
        "partition.deletion_info must be present for a partition-level delete; got: {p}"
    );
    let marked = del["marked_deleted"]
        .as_str()
        .expect("deletion_info.marked_deleted present");
    assert!(
        !marked.is_empty() && marked != "1970-01-01T00:00:00Z",
        "marked_deleted must be a real (non-LIVE) deletion timestamp; got {marked:?}"
    );

    eprintln!(
        "[#911 partition-deletion PASS] Cassandra 5 ({image}) sstabledump reported the partition \
         deletion (marked_deleted={marked})."
    );
}
