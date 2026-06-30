//! Issue #911 (final child of epic #872): validate writer-produced canonical
//! `da-*-bti-*` BTI SSTables against Cassandra 5's `sstabledump`.
//!
//! Epic #872 made CQLite emit Cassandra-canonical BTI SSTables: a `da` descriptor
//! with `Data.db` + `Partitions.db` + `Rows.db`, no `Index.db`/`Summary.db`
//! (#908), the within-partition `Rows.db` clustering trie (#910), and
//! `SSTableReader` discovery + writer->reader roundtrip (#909). #911 closes the
//! loop by proving the output is also readable by a *real Cassandra 5 reader*:
//! `sstabledump` (which exercises the same `BtiTableReader` /
//! `StatsComponent` / `PartitionIndex` load path a live node uses).
//!
//! ## What is gated on Docker
//!
//! The live `sstabledump` comparison needs Docker + a `cassandra:5.0` (or
//! `cassandra:5.0.x`) image. Exactly like the other Cassandra e2e paths in this
//! repo (`issue_819_differential_compaction`'s `CQLITE_DIFFERENTIAL_CASSANDRA`
//! switch, `test-data/scripts/e2e-cassandra-readback.sh`), the live path is
//! SKIPPED CLEANLY when Docker or the image is absent — the test prints a skip
//! note and returns rather than failing. When Docker IS present it actually runs
//! `sstabledump` against a writer-produced `da` SSTable and asserts the dump is
//! well-formed and value-equivalent to what CQLite wrote.
//!
//! ## What runs WITHOUT Docker
//!
//! A structural cross-check that does not need Cassandra at all: the
//! writer-produced `da` component set + `Partitions.db` canonical footer are
//! compared against the **real committed Cassandra-produced `da` fixture**
//! (`test_da/simple_table-…`), so CI without Docker still verifies the on-disk
//! shape CQLite emits matches what Cassandra writes.
//!
//! All tests require the `write-support` feature.

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::writer::{SSTableFormat, SSTableInfo, SSTableWriter};
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId,
};
use cqlite_core::types::Value;
use tempfile::TempDir;

// ════════════════════════════════════════════════════════════════════════════
// Docker / Cassandra availability gate (skip cleanly when absent)
// ════════════════════════════════════════════════════════════════════════════

/// The `sstabledump` binary inside the Cassandra image lives under `tools/bin`
/// and is NOT on the entrypoint PATH, so we invoke it with an explicit
/// `--entrypoint`.
const SSTABLEDUMP: &str = "/opt/cassandra/tools/bin/sstabledump";

/// `true` when `CQLITE_REQUIRE_FIXTURES` is set to a truthy value ("1"/"true").
/// In strict mode (the `nightly_docker` parity lane, issue #1025) the live BTI
/// `sstabledump` checks are a HARD leg: a run that would otherwise SKIP because
/// Docker / the pinned `cassandra:5.0` image is unavailable must PANIC instead,
/// so the HARD leg can never vacuously pass without actually exercising the real
/// Cassandra 5 reader (issue #28 no-heuristics / #1024 fail-closed mandate).
/// Mirrors the `CQLITE_REQUIRE_FIXTURES` convention used by the Bloom leg.
fn require_live_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// Single source of truth for the Cassandra image the BTI leg validates against.
/// The `nightly_docker` workflow sets `CASSANDRA_IMAGE` (and pulls exactly that
/// tag) so the hard BTI leg can never validate against a DIFFERENT image than the
/// stated pin. Defaults to the corpus pin `cassandra:5.0.2` when unset (matching
/// docker-compose-cassandra5.yml / the committed corpus — NOT a second pin).
fn pinned_cassandra_image() -> String {
    std::env::var("CASSANDRA_IMAGE")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "cassandra:5.0.2".to_string())
}

/// Return `true` if the exact image tag is present locally.
fn image_present_locally(listing: &str, image: &str) -> bool {
    listing.lines().any(|line| line.trim() == image)
}

/// Resolve a usable Cassandra 5 image, returning the image tag and the reason it
/// is unavailable when it is not. `None` (an `Err`) means a clean local SKIP
/// outside strict mode.
///
/// In strict mode (`CQLITE_REQUIRE_FIXTURES=1`, the `nightly_docker` HARD leg) we
/// REQUIRE the EXACT pinned image (`CASSANDRA_IMAGE`, default `cassandra:5.0.2`):
/// we do NOT fall back to a looser `cassandra:5.0` (or any other `5.0.*`) tag, so
/// the hard leg validates against the stated pin and never a different image that
/// merely happens to be on the runner. Outside strict mode we keep the lenient
/// local-tag discovery (exact pin, then `cassandra:5.0`, then any `cassandra:5.0.*`)
/// for dev convenience.
fn try_cassandra_5_image() -> Result<String, String> {
    let pinned = pinned_cassandra_image();
    // 1. Docker daemon reachable?
    let info = Command::new("docker").arg("info").output();
    match info {
        Ok(out) if out.status.success() => {}
        Ok(_) => return Err("docker daemon not reachable (`docker info` failed)".to_string()),
        Err(_) => return Err("docker binary not available".to_string()),
    }
    // 2. List images present locally. (We do not pull — CI provisions the pin.)
    let images = Command::new("docker")
        .args(["images", "--format", "{{.Repository}}:{{.Tag}}"])
        .output();
    let images = match images {
        Ok(out) if out.status.success() => out,
        _ => return Err("`docker images` failed".to_string()),
    };
    let listing = String::from_utf8_lossy(&images.stdout);

    // STRICT: the exact pinned image MUST be present. No looser fallback — the
    // hard leg must validate against the stated pin, not a different local tag.
    if require_live_strict() {
        if image_present_locally(&listing, &pinned) {
            return Ok(pinned);
        }
        return Err(format!(
            "strict mode requires the EXACT pinned image '{pinned}' (CASSANDRA_IMAGE) but it is \
             not present locally; the lane must pull '{pinned}' before this leg (no fallback to a \
             looser cassandra:5.0 tag)"
        ));
    }

    // Non-strict (dev convenience): prefer the exact pin, then "cassandra:5.0",
    // then any "cassandra:5.0.*".
    if image_present_locally(&listing, &pinned) {
        return Ok(pinned);
    }
    let mut candidate: Option<String> = None;
    for line in listing.lines() {
        let line = line.trim();
        if line == "cassandra:5.0" {
            return Ok(line.to_string());
        }
        if line.starts_with("cassandra:5.0.") && candidate.is_none() {
            candidate = Some(line.to_string());
        }
    }
    candidate.ok_or_else(|| {
        format!("no usable Cassandra 5 image present locally (looked for '{pinned}', cassandra:5.0, cassandra:5.0.*)")
    })
}

/// Pick a usable `cassandra:5.0*` image, or `None` if Docker is unavailable or no
/// 5.0 image is present. Mirrors the honest gating of the other Cassandra e2e
/// paths: a missing daemon/image is a SKIP, never a failure — EXCEPT under
/// strict mode (`CQLITE_REQUIRE_FIXTURES=1`, the `nightly_docker` HARD leg, issue
/// #1025), where an unavailable Cassandra is a FAIL (panic) so the HARD leg can
/// never pass without actually running the live `sstabledump` check.
fn cassandra_5_image() -> Option<String> {
    match try_cassandra_5_image() {
        Ok(image) => Some(image),
        Err(reason) => {
            if require_live_strict() {
                panic!(
                    "CQLITE_REQUIRE_FIXTURES=1 (strict nightly_docker BTI leg) but the live \
                     Cassandra 5 sstabledump check cannot run: {reason}. The BTI HARD leg must \
                     not vacuously pass — the workflow must provision Docker + the pinned \
                     cassandra:5.0.2 image before this leg (issue #1025 fail-closed mandate)."
                );
            }
            None
        }
    }
}

/// Run `sstabledump <data>` inside the image with the SSTable directory mounted
/// read-only at `/data`. Returns the captured stdout on success, or an `Err`
/// carrying stderr (which is where Cassandra prints load-path exceptions).
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
    // sstabledump exits 0 and prints JSON to stdout on success. A load-path
    // failure surfaces as a Java exception on stderr (often still exit 0), so we
    // treat "no JSON array" or "Exception" on stderr as failure.
    if stderr.contains("Exception") || !stdout.trim_start().starts_with('[') {
        return Err(format!(
            "sstabledump did not produce a JSON dump.\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
        ));
    }
    Ok(stdout)
}

// ════════════════════════════════════════════════════════════════════════════
// Fixtures: write a canonical da BTI SSTable (narrow + wide partitions)
// ════════════════════════════════════════════════════════════════════════════

/// wide(pk int, ck int, payload text, PRIMARY KEY (pk, ck)).
fn wide_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "wide".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
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

fn row(pk: i32, ck: i32, payload: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new("test_ks", "wide"),
        PartitionKey::single("pk", Value::Integer(pk)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::Write {
            column: "payload".to_string(),
            value: Value::Text(payload.to_string()),
        }],
        ts,
        None,
    )
}

/// Rows in the WIDE partition; ~2 KiB payload each so the partition spans >= 2
/// column-index blocks and exercises the positive-`RowsOffset` / `Rows.db` path.
const WIDE_ROWS: i32 = 200;
const NARROW_PK: i32 = 1;
const WIDE_PK: i32 = 2;

/// Write a BTI SSTable with one narrow (pk=1, single small row) and one wide
/// (pk=2, 200 x ~2 KiB rows) partition. Returns the writer `SSTableInfo`.
async fn write_mixed_bti(dir: &Path) -> SSTableInfo {
    let schema = wide_schema();
    let mut writer =
        SSTableWriter::with_format(dir.to_path_buf(), 1, &schema, 16, SSTableFormat::Bti).unwrap();

    let payload_wide = "x".repeat(2048);
    let narrow: Vec<Mutation> = vec![row(NARROW_PK, 0, "small", 1_000_000)];
    let wide: Vec<Mutation> = (0..WIDE_ROWS)
        .map(|ck| row(WIDE_PK, ck, &payload_wide, 2_000_000 + ck as i64))
        .collect();

    let mut parts: Vec<(i32, Vec<Mutation>)> = vec![(NARROW_PK, narrow), (WIDE_PK, wide)];
    parts.sort_by_key(|(pk, _)| row(*pk, 0, "", 1).decorated_key(&schema).unwrap().token);
    for (_pk, muts) in parts {
        let key = muts[0].decorated_key(&schema).unwrap();
        writer.write_partition(key, muts).unwrap();
    }
    writer.finish().await.unwrap()
}

// ════════════════════════════════════════════════════════════════════════════
// AC: a writer-produced BTI SSTable reads back correctly under Cassandra 5's
//     sstabledump and matches the expected dump.
// ════════════════════════════════════════════════════════════════════════════

/// LIVE (Docker-gated): `sstabledump` opens a writer-produced `da` SSTable and
/// dumps every partition/row with the values CQLite wrote.
///
/// This exercises the *real* Cassandra 5 BTI load path — `StatsComponent.load`
/// (Statistics.db `da` `StatsMetadata`, incl. the covered-clustering `Slice`),
/// `PartitionIndex.load` (the canonical `Partitions.db` `[firstPos|keyCount|root]`
/// footer + first/last keys), and the BTI `Data.db` reader. Skipped cleanly when
/// Docker / a `cassandra:5.0` image is absent.
#[tokio::test]
async fn bti_writer_output_reads_under_cassandra5_sstabledump() {
    let Some(image) = cassandra_5_image() else {
        eprintln!(
            "[skip] bti_writer_output_reads_under_cassandra5_sstabledump: Docker or a \
             cassandra:5.0 image is not available. CI runs this via the sstabledump-validator \
             / e2e harness (see the test module docs)."
        );
        return;
    };

    let dir = TempDir::new().unwrap();
    let info = write_mixed_bti(dir.path()).await;
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
    assert_eq!(
        data_file, "da-1-bti-Data.db",
        "writer must emit a da-bti Data.db"
    );

    let dump = run_sstabledump(&image, &sstable_dir, &data_file).unwrap_or_else(|e| {
        panic!(
            "Cassandra 5 sstabledump FAILED to read the writer-produced da-bti SSTable. \
             This is the #911 acceptance gate. Detail:\n{e}"
        )
    });

    let json: serde_json::Value =
        serde_json::from_str(&dump).expect("sstabledump output must be valid JSON");
    let partitions = json.as_array().expect("dump is a JSON array of partitions");

    // Both partitions present.
    assert_eq!(
        partitions.len(),
        2,
        "sstabledump must report exactly 2 partitions (narrow pk=1 + wide pk=2)"
    );

    // Collect (partition-key -> row count, and the set of clustering values) so we
    // assert value parity with what CQLite wrote.
    let mut narrow_rows = 0usize;
    let mut wide_cks: Vec<i64> = Vec::new();
    let mut narrow_payload_ok = false;
    let wide_payload = "x".repeat(2048);
    let mut wide_payload_ok = true;

    for p in partitions {
        let key = p["partition"]["key"][0]
            .as_str()
            .expect("partition key rendered as string");
        let rows = p["rows"].as_array().cloned().unwrap_or_default();
        match key {
            "1" => {
                narrow_rows = rows.len();
                for r in &rows {
                    if r["cells"][0]["value"].as_str() == Some("small") {
                        narrow_payload_ok = true;
                    }
                }
            }
            "2" => {
                for r in &rows {
                    if let Some(ck) = r["clustering"][0].as_i64() {
                        wide_cks.push(ck);
                    }
                    if r["cells"][0]["value"].as_str() != Some(wide_payload.as_str()) {
                        wide_payload_ok = false;
                    }
                }
            }
            other => panic!("unexpected partition key {other:?} in dump"),
        }
    }

    assert_eq!(narrow_rows, 1, "narrow partition must have exactly one row");
    assert!(narrow_payload_ok, "narrow row payload must be 'small'");

    wide_cks.sort_unstable();
    assert_eq!(
        wide_cks.len(),
        WIDE_ROWS as usize,
        "wide partition must dump all {WIDE_ROWS} rows via the Rows.db path"
    );
    assert_eq!(
        wide_cks,
        (0..WIDE_ROWS as i64).collect::<Vec<_>>(),
        "wide partition clustering values must cover 0..{WIDE_ROWS}"
    );
    assert!(
        wide_payload_ok,
        "every wide row payload must match what CQLite wrote"
    );

    eprintln!(
        "[#911 PASS] Cassandra 5 ({image}) sstabledump read the writer-produced da-bti SSTable: \
         2 partitions, 1 narrow + {WIDE_ROWS} wide rows, values intact."
    );
}

/// LIVE (Docker-gated): a narrow-only BTI SSTable (no clustering, the
/// `simple_table` shape) also reads under sstabledump. Covers the
/// `coveredClustering = Slice.ALL` + empty `clusteringTypes` + 0-byte `Rows.db`
/// path, matching the real `da` `simple_table` fixture.
#[tokio::test]
async fn bti_narrow_only_writer_output_reads_under_cassandra5_sstabledump() {
    let Some(image) = cassandra_5_image() else {
        eprintln!(
            "[skip] bti_narrow_only_writer_output_reads_under_cassandra5_sstabledump: Docker / \
             cassandra:5.0 image not available."
        );
        return;
    };

    // simple(pk int PRIMARY KEY, name text) — no clustering.
    let schema = TableSchema {
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
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let dir = TempDir::new().unwrap();
    let mut writer =
        SSTableWriter::with_format(dir.path().to_path_buf(), 1, &schema, 16, SSTableFormat::Bti)
            .unwrap();
    let mut keyed: Vec<_> = (0..5)
        .map(|i| {
            let m = Mutation::new(
                TableId::new("test_ks", "simple"),
                PartitionKey::single("pk", Value::Integer(i)),
                None,
                vec![CellOperation::Write {
                    column: "name".to_string(),
                    value: Value::Text(format!("n{i}")),
                }],
                1_000_000 + i as i64,
                None,
            );
            let key = m.decorated_key(&schema).unwrap();
            (key, m)
        })
        .collect();
    keyed.sort_by_key(|(k, _)| k.token);
    for (key, m) in keyed {
        writer.write_partition(key, vec![m]).unwrap();
    }
    let info = writer.finish().await.unwrap();

    // Narrow-only: 0-byte Rows.db (matches the simple_table fixture).
    assert!(
        std::fs::read(info.rows_path.clone().unwrap())
            .unwrap()
            .is_empty(),
        "narrow-only BTI must emit a 0-byte Rows.db"
    );

    let sstable_dir = info.data_path.parent().unwrap().to_path_buf();
    let dump = run_sstabledump(&image, &sstable_dir, "da-1-bti-Data.db")
        .unwrap_or_else(|e| panic!("sstabledump failed on narrow-only da-bti SSTable:\n{e}"));
    let json: serde_json::Value = serde_json::from_str(&dump).unwrap();
    let partitions = json.as_array().unwrap();
    assert_eq!(partitions.len(), 5, "all 5 narrow partitions must dump");

    let mut names: Vec<String> = partitions
        .iter()
        .filter_map(|p| p["rows"][0]["cells"][0]["value"].as_str().map(String::from))
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec!["n0", "n1", "n2", "n3", "n4"],
        "every partition's name cell must match what CQLite wrote"
    );

    eprintln!(
        "[#911 PASS] sstabledump read narrow-only da-bti SSTable: 5 partitions, values intact."
    );
}

// ════════════════════════════════════════════════════════════════════════════
// No-Docker cross-check: writer da component shape vs the real committed fixture
// ════════════════════════════════════════════════════════════════════════════

/// Locate the committed real-Cassandra `da` `simple_table` fixture under
/// `CQLITE_DATASETS_ROOT` (or the in-repo default), or `None` if not fetched.
fn committed_simple_da_fixture_dir() -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("..")
                .join("test-data")
                .join("datasets")
        });
    let dir = root
        .join("sstables")
        .join("test_da")
        .join("simple_table-de1be8b064e711f19ad401a8c8227b11");
    dir.join("da-2-bti-Data.db").exists().then_some(dir)
}

/// Read the Cassandra `PartitionIndex` footer `[firstPos|keyCount|root]` (last 24
/// bytes) of a `Partitions.db`, returning `(first_pos, key_count, root)`.
fn read_partition_index_footer(partitions_db: &Path) -> (i64, i64, i64) {
    let bytes = std::fs::read(partitions_db).unwrap();
    assert!(
        bytes.len() >= 24,
        "Partitions.db must carry a 24-byte footer"
    );
    let f = &bytes[bytes.len() - 24..];
    let g = |o: usize| i64::from_be_bytes(f[o..o + 8].try_into().unwrap());
    (g(0), g(8), g(16))
}

/// NO-DOCKER: the writer-produced `da` `Partitions.db` uses the SAME canonical
/// Cassandra `PartitionIndex` footer shape as the real committed fixture
/// (`[firstPos|keyCount|root]` last 24 bytes, first/last keys at `firstPos`), and
/// the writer emits the same `da` component set. This verifies on-disk shape
/// parity on CI even when Cassandra/Docker is unavailable.
#[tokio::test]
async fn bti_writer_partition_index_footer_matches_cassandra_fixture_shape() {
    // Writer side.
    let dir = TempDir::new().unwrap();
    let info = write_mixed_bti(dir.path()).await;
    let parts = info.partitions_path.clone().expect("BTI Partitions.db");
    let (first_pos, key_count, root) = read_partition_index_footer(&parts);

    // Footer self-consistency: firstPos and root point inside the trie region
    // (before the footer), keyCount matches the partitions written.
    let total = std::fs::metadata(&parts).unwrap().len() as i64;
    assert_eq!(key_count, 2, "two partitions were written");
    assert!(first_pos > 0 && first_pos < total - 24, "firstPos in range");
    assert!(
        root >= 0 && root < first_pos,
        "root precedes first/last-key region"
    );

    // First/last keys at firstPos must be the pk=1 and pk=2 raw int bytes.
    let bytes = std::fs::read(&parts).unwrap();
    let mut p = first_pos as usize;
    let read_short = |b: &[u8], p: &mut usize| -> Vec<u8> {
        let len = u16::from_be_bytes([b[*p], b[*p + 1]]) as usize;
        *p += 2;
        let v = b[*p..*p + len].to_vec();
        *p += len;
        v
    };
    let first_key = read_short(&bytes, &mut p);
    let last_key = read_short(&bytes, &mut p);
    assert_eq!(
        first_key,
        NARROW_PK.to_be_bytes().to_vec(),
        "firstKey must be pk=1's raw int bytes"
    );
    assert_eq!(
        last_key,
        WIDE_PK.to_be_bytes().to_vec(),
        "lastKey must be pk=2's raw int bytes"
    );

    // Cross-check the SAME footer SHAPE against the real Cassandra fixture (when
    // fetched). The fixture has its own keys/offsets, but the layout invariants
    // (footer size, firstPos/root ordering, well-formed short-length keys) must
    // be identical — that is what makes CQLite output Cassandra-loadable.
    if let Some(fix_dir) = committed_simple_da_fixture_dir() {
        let fix_parts = fix_dir.join("da-2-bti-Partitions.db");
        let (ffp, fkc, fr) = read_partition_index_footer(&fix_parts);
        let ftotal = std::fs::metadata(&fix_parts).unwrap().len() as i64;
        assert_eq!(fkc, 3, "fixture simple_table has 3 partitions");
        assert!(ffp > 0 && ffp < ftotal - 24, "fixture firstPos in range");
        assert!(fr >= 0 && fr < ffp, "fixture root precedes key region");
        // Fixture first/last keys are also short-length-framed at firstPos.
        let fb = std::fs::read(&fix_parts).unwrap();
        let mut fp = ffp as usize;
        let fk1 = read_short(&fb, &mut fp);
        let fk2 = read_short(&fb, &mut fp);
        assert_eq!(fk1.len(), 16, "fixture keys are 16-byte UUIDs");
        assert_eq!(fk2.len(), 16);
        eprintln!(
            "[#911 shape-parity] writer footer (firstPos={first_pos}, keyCount={key_count}, \
             root={root}) and fixture footer (firstPos={ffp}, keyCount={fkc}, root={fr}) share \
             the canonical Cassandra PartitionIndex layout."
        );
    } else {
        eprintln!(
            "[#911 note] committed da simple_table fixture not present (run \
             test-data/scripts/fetch-datasets.sh); validated writer footer shape only."
        );
    }

    // The writer must emit the canonical da component set, no Index/Summary.
    assert!(info.index_path.is_none(), "BTI must not emit Index.db");
    assert!(info.summary_path.is_none(), "BTI must not emit Summary.db");
    assert!(info.rows_path.is_some(), "BTI must emit Rows.db");
}
