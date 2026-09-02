//! Issue #3782 measurement probe — **`#[ignore]`d, run by hand**.
//!
//! Records what a SINGLE-BYTE corruption of a `text` CLUSTERING-key value does
//! to the read and compaction paths on a REAL Cassandra 5.0 fixture
//! (`test_basic.composite_key_table`, `nb`/BIG, LZ4, `clustering_key2 TEXT`).
//!
//! The corruption is applied to the LZ4 **literal** carrying the value, then the
//! chunk's trailing CRC32 is recomputed — so the change is length-preserving,
//! provably a single decompressed byte (asserted), and invisible to integrity
//! checks. No CQLite-written bytes are involved (#3042).
//!
//! Measured on `main` @ 1023095ee (2026-09-02), `CQLITE_DATASETS_ROOT=/data/datasets`:
//!
//! | surface                                       | control | mutated | note |
//! |-----------------------------------------------|---------|---------|------|
//! | `Database::execute` (materializing)           | 100     | **23**  | `Ok`, no error |
//! | `Database::execute_streaming`                 | 100     | **23**  | `Ok`, 0 err items |
//! | `iterate_all_partitions_for_compaction`       | 100     | **102** | 2 keys LOST, 3 FABRICATED |
//! | `stream_all_partitions_for_compaction`        | 100     | **102** | same |
//! | `iterate_all_partitions` (#2302 index path)   | 100     | **23**  | emits the #2302 fallback WARN, then returns short |
//!
//! Run:
//! ```text
//! CQLITE_DATASETS_ROOT=/data/datasets \
//!   cargo test -p cqlite-core --features cli-helpers --test probe_3782 \
//!   -- --ignored --nocapture --test-threads=1
//! ```
#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::PathBuf;
use std::sync::Arc;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::platform::Platform;
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::Database;

const FIX_KS: &str = "test_basic";
const FIX_TABLE: &str = "composite_key_table";
const SCHEMA_FILE: &str = "basic-types.cql";
/// Clustering-key values known to be unique in this fixture; the first one that
/// also appears exactly once as a verbatim LZ4 literal is the mutation target.
const NEEDLES: &[&[u8]] = &[b"necessary", b"purpose", b"artist", b"region", b"glass"];

/// Collect WARN/ERROR tracing output into a shared buffer.
#[derive(Clone, Default)]
struct LogSink(Arc<std::sync::Mutex<Vec<u8>>>);
impl std::io::Write for LogSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogSink {
    type Writer = LogSink;
    fn make_writer(&'a self) -> LogSink {
        self.clone()
    }
}
impl LogSink {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap().clone()).to_string()
    }
}

fn datasets_root() -> PathBuf {
    PathBuf::from(std::env::var("CQLITE_DATASETS_ROOT").expect("CQLITE_DATASETS_ROOT"))
}

fn schemas_dir() -> PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("test-data")
        .join("schemas")
}

fn fixture_dir() -> PathBuf {
    let root = datasets_root().join("sstables").join(FIX_KS);
    for e in std::fs::read_dir(&root)
        .expect("read keyspace dir")
        .flatten()
    {
        let n = e.file_name().to_string_lossy().to_string();
        if n.starts_with(&format!("{FIX_TABLE}-")) && e.path().is_dir() {
            return e.path();
        }
    }
    panic!("fixture {FIX_KS}.{FIX_TABLE} not found under {root:?}");
}

fn comp_file(dir: &std::path::Path, suffix: &str) -> PathBuf {
    for e in std::fs::read_dir(dir).expect("read dir").flatten() {
        if e.file_name().to_string_lossy().ends_with(suffix) {
            return e.path();
        }
    }
    panic!("no {suffix} in {dir:?}");
}

/// `CompressionInfo.db` → (algorithm, chunk_offsets). Parsed independently of
/// the code under test so the layout is an on-disk fact, not a derived one.
fn parse_ci(p: &std::path::Path) -> (String, Vec<u64>) {
    let b = std::fs::read(p).expect("read CompressionInfo.db");
    let nlen = u16::from_be_bytes([b[0], b[1]]) as usize;
    let mut o = 2usize;
    let alg = String::from_utf8_lossy(&b[o..o + nlen]).to_string();
    o += nlen;
    let opt = u32::from_be_bytes(b[o..o + 4].try_into().unwrap()) as usize;
    o += 4;
    for _ in 0..opt {
        let kl = u16::from_be_bytes(b[o..o + 2].try_into().unwrap()) as usize;
        o += 2 + kl;
        let vl = u16::from_be_bytes(b[o..o + 2].try_into().unwrap()) as usize;
        o += 2 + vl;
    }
    o += 4 + 4 + 8; // chunk_length, max_compressed_length, data_length
    let n = u32::from_be_bytes(b[o..o + 4].try_into().unwrap()) as usize;
    o += 4;
    let offs = (0..n)
        .map(|i| u64::from_be_bytes(b[o + i * 8..o + i * 8 + 8].try_into().unwrap()))
        .collect();
    (alg, offs)
}

/// Copy the fixture to `dst`, flipping ONE byte of the LZ4 literal carrying one
/// of `NEEDLES` to `0xFF`, and fixing the chunk CRC32. Returns the changed
/// DECOMPRESSED offset. Asserts length-preservation and single-byte change.
fn mutate_clustering_utf8(src: &std::path::Path, dst: &std::path::Path) -> usize {
    std::fs::create_dir_all(dst).unwrap();
    for e in std::fs::read_dir(src).unwrap().flatten() {
        std::fs::copy(e.path(), dst.join(e.file_name())).unwrap();
    }
    let (alg, offs) = parse_ci(&comp_file(dst, "-CompressionInfo.db"));
    assert!(
        alg.to_uppercase().contains("LZ4"),
        "expected LZ4, got {alg}"
    );
    let data_path = comp_file(dst, "-Data.db");
    let mut data = std::fs::read(&data_path).unwrap();
    let file_len = data.len() as u64;

    for needle in NEEDLES {
        for (i, &start) in offs.iter().enumerate() {
            let end = offs.get(i + 1).copied().unwrap_or(file_len);
            let (lo, hi) = (start as usize, (end - 4) as usize);
            let before =
                lz4_flex::decompress_size_prepended(&data[lo..hi]).expect("decompress chunk");
            let dhits: Vec<usize> = (0..before.len().saturating_sub(needle.len()))
                .filter(|&k| &before[k..k + needle.len()] == *needle)
                .collect();
            let chits: Vec<usize> = (0..(hi - lo).saturating_sub(needle.len()))
                .filter(|&k| &data[lo + k..lo + k + needle.len()] == *needle)
                .collect();
            if dhits.len() != 1 || chits.len() != 1 {
                continue;
            }
            let (dpos, flip_at) = (dhits[0], lo + chits[0]);
            let orig = data[flip_at];
            data[flip_at] = 0xFF;
            let after =
                lz4_flex::decompress_size_prepended(&data[lo..hi]).expect("re-decompress chunk");
            assert_eq!(
                before.len(),
                after.len(),
                "mutation must be length-preserving"
            );
            let diffs: Vec<usize> = (0..before.len())
                .filter(|&k| before[k] != after[k])
                .collect();
            if diffs.as_slice() != [dpos] {
                data[flip_at] = orig; // not a clean single-byte change; try the next needle
                continue;
            }
            assert_eq!(after[dpos], 0xFF);
            let crc = crc32fast::hash(&data[lo..hi]).to_be_bytes();
            data[hi..hi + 4].copy_from_slice(&crc);
            std::fs::write(&data_path, &data).unwrap();
            eprintln!(
                "PROBE3782 mutated {:?} in chunk {i}: file_off={flip_at} decompressed_off={dpos} \
                 (0x{orig:02x} -> 0xFF), chunk CRC32 recomputed",
                String::from_utf8_lossy(needle)
            );
            return dpos;
        }
    }
    panic!("no needle occurs exactly once as a verbatim LZ4 literal in any chunk");
}

fn table_schema() -> cqlite_core::schema::TableSchema {
    let cql = std::fs::read_to_string(schemas_dir().join(SCHEMA_FILE)).unwrap();
    let start = cql
        .find(&format!("CREATE TABLE IF NOT EXISTS {FIX_TABLE}"))
        .expect("CREATE TABLE statement");
    let end = start + cql[start..].find(';').expect("statement terminator") + 1;
    let mut t = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end])
        .expect("parse CREATE TABLE");
    t.keyspace = FIX_KS.to_string();
    t
}

async fn open_db(data_dir: PathBuf) -> Database {
    ingest(IngestionConfig {
        schema_paths: vec![schemas_dir().join(SCHEMA_FILE)],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{FIX_KS}/")),
    })
    .await
    .expect("ingest")
    .database
}

/// Stage a pristine copy (`ctl`) and a single-byte-mutated copy (`mut`) of the
/// fixture under a unique temp root, each in `<root>/<leg>/sstables/<ks>/<dir>`.
fn stage() -> (PathBuf, PathBuf) {
    let src = fixture_dir();
    let name = src.file_name().unwrap().to_owned();
    let root = std::env::temp_dir().join(format!("probe3782-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let ctl = root.join("ctl").join("sstables").join(FIX_KS).join(&name);
    std::fs::create_dir_all(&ctl).unwrap();
    for e in std::fs::read_dir(&src).unwrap().flatten() {
        std::fs::copy(e.path(), ctl.join(e.file_name())).unwrap();
    }
    let mutated = root.join("mut").join("sstables").join(FIX_KS).join(&name);
    mutate_clustering_utf8(&src, &mutated);
    (ctl, mutated)
}

/// Q1 — the READ path. Control vs mutated row counts through the public
/// materializing and streaming surfaces, plus captured WARN/ERROR output.
#[tokio::test]
#[ignore = "measurement probe for issue #3782; run by hand with --ignored"]
async fn probe_3782_q1_read_path() {
    let sink = LogSink::default();
    let _ = tracing_subscriber::fmt()
        .with_writer(sink.clone())
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .try_init();

    let (ctl, mutated) = stage();
    let ctl_root = ctl.parent().unwrap().parent().unwrap().to_path_buf();
    let mut_root = mutated.parent().unwrap().parent().unwrap().to_path_buf();
    let sql = format!("SELECT * FROM {FIX_KS}.{FIX_TABLE}");

    let control = open_db(ctl_root)
        .await
        .execute(&sql)
        .await
        .expect("control read")
        .rows
        .len();
    eprintln!("PROBE3782 Q1 CONTROL execute -> Ok rows={control}");
    assert!(
        control > 0,
        "0-rows-when-present: the control read must return rows"
    );
    let after_control = sink.text().lines().count();
    eprintln!("PROBE3782 Q1 CONTROL warn/error lines={after_control}");

    let db = open_db(mut_root).await;
    match db.execute(&sql).await {
        Ok(r) => eprintln!(
            "PROBE3782 Q1 MUTATED execute -> Ok rows={} (control={control}, LOST={})",
            r.rows.len(),
            control.saturating_sub(r.rows.len())
        ),
        Err(e) => eprintln!("PROBE3782 Q1 MUTATED execute -> Err {e}"),
    }

    let cfg = StreamingConfig {
        buffer_size: 8,
        ..Default::default()
    };
    match db.execute_streaming(&sql, cfg).await {
        Ok(mut it) => {
            let (mut ok, mut err) = (0usize, 0usize);
            while let Some(item) = it.next_async().await {
                if item.is_ok() {
                    ok += 1
                } else {
                    err += 1
                }
            }
            eprintln!("PROBE3782 Q1 MUTATED streaming -> ok_rows={ok} err_items={err}");
        }
        Err(e) => eprintln!("PROBE3782 Q1 MUTATED streaming -> Err {e}"),
    }

    let logs = sink.text();
    eprintln!(
        "PROBE3782 Q1 warn/error lines total={} index-fallback-warns={}",
        logs.lines().count(),
        logs.matches("falling back to a full sequential scan")
            .count()
    );
}

/// Q2 — the COMPACTION path and the #2302 index-random-read path.
#[tokio::test]
#[ignore = "measurement probe for issue #3782; run by hand with --ignored"]
async fn probe_3782_q2_compaction_and_index_paths() {
    let sink = LogSink::default();
    let _ = tracing_subscriber::fmt()
        .with_writer(sink.clone())
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .try_init();

    let (ctl, mutated) = stage();
    let schema = table_schema();
    let config = cqlite_core::Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let mut keys: std::collections::BTreeMap<&str, Vec<Vec<u8>>> = Default::default();

    for (label, dir) in [("CONTROL", &ctl), ("MUTATED", &mutated)] {
        let reader = SSTableReader::open(&comp_file(dir, "-Data.db"), &config, platform.clone())
            .await
            .expect("open SSTableReader");

        let before = sink.text().lines().count();
        match reader
            .iterate_all_partitions_for_compaction(Some(&schema))
            .await
        {
            Ok(rows) => {
                eprintln!("PROBE3782 Q2 {label} compaction -> Ok rows={}", rows.len());
                keys.insert(
                    label,
                    rows.iter().map(|r| r.key.as_bytes().to_vec()).collect(),
                );
            }
            Err(e) => eprintln!("PROBE3782 Q2 {label} compaction -> Err {e}"),
        }
        eprintln!(
            "PROBE3782 Q2 {label} compaction warn/error lines={}",
            sink.text().lines().count() - before
        );

        let cancel = cqlite_core::storage::scan_cancel::ScanCancel::new();
        let mut n = 0usize;
        let r = reader
            .stream_all_partitions_for_compaction(Some(&schema), &cancel, |_row| {
                n += 1;
                Ok(std::ops::ControlFlow::Continue(()))
            })
            .await;
        match r {
            Ok(()) => eprintln!("PROBE3782 Q2 {label} stream_compaction -> Ok rows={n}"),
            Err(e) => {
                eprintln!("PROBE3782 Q2 {label} stream_compaction -> Err after {n} rows: {e}")
            }
        }

        let before = sink.text().lines().count();
        match reader.iterate_all_partitions().await {
            Ok(rows) => eprintln!(
                "PROBE3782 Q2 {label} iterate_all_partitions -> Ok partitions={}",
                rows.len()
            ),
            Err(e) => eprintln!("PROBE3782 Q2 {label} iterate_all_partitions -> Err {e}"),
        }
        let logs = sink.text();
        eprintln!(
            "PROBE3782 Q2 {label} iterate warn/error lines={} index-fallback-warns-cumulative={}",
            logs.lines().count() - before,
            logs.matches("falling back to a full sequential scan")
                .count()
        );
    }

    if let (Some(c), Some(m)) = (keys.get("CONTROL"), keys.get("MUTATED")) {
        assert!(
            !c.is_empty(),
            "0-rows-when-present: the control compaction must yield rows"
        );
        let cs: std::collections::BTreeSet<_> = c.iter().cloned().collect();
        let ms: std::collections::BTreeSet<_> = m.iter().cloned().collect();
        eprintln!(
            "PROBE3782 Q2 KEY DIFF control_rows={} mutated_rows={} LOST_KEYS={} FABRICATED_KEYS={}",
            c.len(),
            m.len(),
            cs.difference(&ms).count(),
            ms.difference(&cs).count()
        );
    }
}
