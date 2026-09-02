//! TEMPORARY measurement probe for issue #3782. Not for merge as-is.
#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::PathBuf;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::Database;

/// Collect WARN/ERROR tracing output into a shared buffer.
#[derive(Clone, Default)]
struct LogSink(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
impl std::io::Write for LogSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogSink {
    type Writer = LogSink;
    fn make_writer(&'a self) -> LogSink { self.clone() }
}

fn datasets_root() -> PathBuf {
    PathBuf::from(std::env::var("CQLITE_DATASETS_ROOT").expect("CQLITE_DATASETS_ROOT"))
}

fn schemas_dir() -> PathBuf {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.parent().unwrap().join("test-data").join("schemas")
}

async fn setup(keyspace: &str, schema_file: &str, data_dir: PathBuf) -> Database {
    let config = IngestionConfig {
        schema_paths: vec![schemas_dir().join(schema_file)],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{keyspace}/")),
    };
    ingest(config).await.expect("ingest").database
}

/// Q4: how many mid-stream (`at_final_chunk == false`) `None`s does a broad read
/// of WELL-FORMED corpus tables produce?
#[tokio::test]
async fn q4_wellformed_corpus_broad_read() {
    for streaming_leg in [false, true] {
    cqlite_core::probe3782::reset();
    let root = datasets_root().join("sstables");
    let cases: &[(&str, &str, &[&str])] = &[
        ("test_basic", "basic-types.cql", &["simple_table", "composite_key_table", "multi_partition_table", "uncompressed_table", "static_columns_table", "compression_test_table", "ttl_test_table"]),
        ("test_wide_rows", "wide-rows.cql", &["wide_partition_table", "many_columns_table", "large_blob_table", "chat_messages", "document_versions", "product_catalog", "sparse_data_table", "multi_metric_timeseries"]),
        ("test_timeseries", "time-series.cql", &["sensor_data", "app_metrics", "user_activity", "stock_prices", "log_entries", "event_store", "user_sessions", "tick_data", "time_bucketed_counters"]),
        ("test_collections", "collections.cql", &["collection_table", "large_collections_table", "nested_collections_table", "collections_with_udts", "frozen_collections_table", "typed_collections_table", "empty_collections_table", "collection_clustering_table"]),
        ("test_comp", "compression-parity.cql", &["lz4_table", "snappy_table", "deflate_table", "zstd_table", "uncompressed_table", "short_final_chunk"]),
        ("test_da", "da-test.cql", &["simple_table", "collection_table", "ttl_table"]),
        ("test_big", "wide-table-bti.cql", &["wide_partition"]),
    ];
    let mut total_rows = 0usize;
    let mut read = 0usize;
    let mut stream_rows = 0usize;
    for (ks, schema, tables) in cases {
        if !root.join(ks).exists() { eprintln!("PROBE3782 SKIP keyspace {ks}"); continue; }
        let db = setup(ks, schema, root.clone()).await;
        for t in *tables {
            let sql = format!("SELECT * FROM {ks}.{t}");
            if streaming_leg { } else {
            match db.execute(&sql).await {
                Ok(r) => { total_rows += r.rows.len(); read += 1; eprintln!("PROBE3782 read {ks}.{t} rows={}", r.rows.len()); }
                Err(e) => eprintln!("PROBE3782 ERR {ks}.{t}: {e}"),
            }
            }
            if streaming_leg {
            // Streaming (windowed) leg: buffer_size=1 forces per-row backpressure.
            for bufsz in [1usize, 8] {
                let cfg = StreamingConfig { buffer_size: bufsz, ..Default::default() };
                match db.execute_streaming(&sql, cfg).await {
                    Ok(mut it) => { let mut n = 0usize; while let Some(item) = it.next_async().await { if item.is_ok() { n += 1; } } stream_rows += n; }
                    Err(e) => eprintln!("PROBE3782 STREAM ERR {ks}.{t}: {e}"),
                }
            }
            }
        }
    }
    eprintln!("PROBE3782 total tables read={read} total_rows={total_rows} stream_rows={stream_rows}");
    assert!(total_rows > 0 || streaming_leg, "0-rows-when-present guard");
    cqlite_core::probe3782::dump(if streaming_leg { "q4-STREAMING" } else { "q4-MATERIALIZING" });
    }
}

// ---------------------------------------------------------------------------
// Q1/Q2: mutate ONE byte of a text CLUSTERING-key value into invalid UTF-8.
// Fixture: test_basic.composite_key_table (nb/BIG, LZ4, clustering_key2 TEXT).
// ---------------------------------------------------------------------------

const FIX_KS: &str = "test_basic";
const FIX_TABLE: &str = "composite_key_table";

fn fixture_dir(ks: &str, table: &str) -> PathBuf {
    let root = datasets_root().join("sstables").join(ks);
    for e in std::fs::read_dir(&root).expect("read ks dir").flatten() {
        let n = e.file_name().to_string_lossy().to_string();
        if n.starts_with(&format!("{table}-")) && e.path().is_dir() {
            return e.path();
        }
    }
    panic!("fixture {ks}.{table} not found under {root:?}");
}

fn comp_file(dir: &std::path::Path, suffix: &str) -> PathBuf {
    for e in std::fs::read_dir(dir).expect("read dir").flatten() {
        if e.file_name().to_string_lossy().ends_with(suffix) {
            return e.path();
        }
    }
    panic!("no {suffix} in {dir:?}");
}

/// Parse CompressionInfo.db → (algorithm, chunk_length, data_length, chunk_offsets).
fn parse_ci(p: &std::path::Path) -> (String, u32, u64, Vec<u64>) {
    let b = std::fs::read(p).expect("read CompressionInfo.db");
    let mut o = 0usize;
    let nlen = u16::from_be_bytes([b[0], b[1]]) as usize;
    o += 2;
    let alg = String::from_utf8_lossy(&b[o..o + nlen]).to_string();
    o += nlen;
    let opt = u32::from_be_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]]) as usize;
    o += 4;
    for _ in 0..opt {
        let kl = u16::from_be_bytes([b[o], b[o + 1]]) as usize; o += 2 + kl;
        let vl = u16::from_be_bytes([b[o], b[o + 1]]) as usize; o += 2 + vl;
    }
    let chunk_length = u32::from_be_bytes([b[o], b[o+1], b[o+2], b[o+3]]); o += 4;
    o += 4; // max_compressed_length
    let data_length = u64::from_be_bytes(b[o..o+8].try_into().unwrap()); o += 8;
    let n = u32::from_be_bytes([b[o], b[o+1], b[o+2], b[o+3]]) as usize; o += 4;
    let mut offs = Vec::with_capacity(n);
    for i in 0..n {
        offs.push(u64::from_be_bytes(b[o + i*8..o + i*8 + 8].try_into().unwrap()));
    }
    (alg, chunk_length, data_length, offs)
}

/// Copy the fixture to `dst`, flipping ONE byte of the LZ4 literal carrying
/// `needle` (a text clustering value) to 0xFF. Returns the decompressed offset
/// that changed. Asserts the change is length-preserving and single-byte.
fn mutate_clustering_utf8(src: &std::path::Path, dst: &std::path::Path, needles: &[&[u8]]) -> usize {
    std::fs::create_dir_all(dst).unwrap();
    for e in std::fs::read_dir(src).unwrap().flatten() {
        std::fs::copy(e.path(), dst.join(e.file_name())).unwrap();
    }
    let ci = comp_file(dst, "-CompressionInfo.db");
    let (alg, _clen, _dlen, offs) = parse_ci(&ci);
    assert!(alg.to_uppercase().contains("LZ4"), "expect LZ4, got {alg}");
    let data_path = comp_file(dst, "-Data.db");
    let mut data = std::fs::read(&data_path).unwrap();
    let file_len = data.len() as u64;

    for needle in needles {
    let needle: &[u8] = needle;
    for (i, &start) in offs.iter().enumerate() {
        let end = offs.get(i + 1).copied().unwrap_or(file_len);
        let comp = &data[start as usize..(end - 4) as usize];
        let before = lz4_flex::decompress_size_prepended(comp).expect("decompress chunk");
        let dhits: Vec<usize> = (0..before.len().saturating_sub(needle.len()))
            .filter(|&k| &before[k..k + needle.len()] == needle).collect();
        if dhits.len() != 1 { continue }
        let chits: Vec<usize> = (0..comp.len().saturating_sub(needle.len()))
            .filter(|&k| &comp[k..k + needle.len()] == needle).collect();
        if chits.len() != 1 { continue }
        let dpos = dhits[0];
        let flip_at = start as usize + chits[0];
        let orig = data[flip_at];
        data[flip_at] = 0xFF;
        let comp2 = &data[start as usize..(end - 4) as usize];
        let after = lz4_flex::decompress_size_prepended(comp2).expect("re-decompress");
        assert_eq!(before.len(), after.len(), "mutation must be length-preserving");
        let diffs: Vec<usize> = (0..before.len()).filter(|&k| before[k] != after[k]).collect();
        if diffs.len() != 1 || diffs[0] != dpos { data[flip_at] = orig; continue }
        assert_eq!(after[dpos], 0xFF);
        let crc = crc32fast::hash(comp2);
        let crc_at = (end - 4) as usize;
        data[crc_at..crc_at + 4].copy_from_slice(&crc.to_be_bytes());
        std::fs::write(&data_path, &data).unwrap();
        eprintln!("PROBE3782 mutated needle={:?} chunk {i}: file_off={flip_at} decompressed_off={dpos} (0x{:02x}->0xFF)", String::from_utf8_lossy(needle), orig);
        return dpos;
    }
    }
    panic!("no needle found as a UNIQUE verbatim LZ4 literal in any chunk");
}

async fn setup_dir(ks: &str, schema_file: &str, data_dir: PathBuf) -> Database {
    setup(ks, schema_file, data_dir).await
}

#[tokio::test]
async fn q1_read_path_mutated_clustering_text() {
    let src = fixture_dir(FIX_KS, FIX_TABLE);
    let tmp = std::env::temp_dir().join(format!("p3782-q1-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    let stage = tmp.join("sstables").join(FIX_KS).join(src.file_name().unwrap());
    // control copy
    let ctl = tmp.join("ctl").join("sstables").join(FIX_KS).join(src.file_name().unwrap());
    std::fs::create_dir_all(&ctl).unwrap();
    for e in std::fs::read_dir(&src).unwrap().flatten() {
        std::fs::copy(e.path(), ctl.join(e.file_name())).unwrap();
    }
    let sink = LogSink::default();
    let _ = tracing_subscriber::fmt()
        .with_writer(sink.clone())
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .try_init();
    let dpos = mutate_clustering_utf8(&src, &stage, &[b"necessary", b"purpose", b"artist", b"region", b"glass", b"unit", b"chair"]);
    eprintln!("PROBE3782 Q1 mutated decompressed offset {dpos}");

    // CONTROL: unmutated
    cqlite_core::probe3782::reset();
    let db = setup_dir(FIX_KS, "basic-types.cql", tmp.join("ctl").join("sstables")).await;
    let ctl_rows = match db.execute(&format!("SELECT * FROM {FIX_KS}.{FIX_TABLE}")).await {
        Ok(r) => { eprintln!("PROBE3782 Q1 CONTROL execute -> Ok rows={}", r.rows.len()); r.rows.len() }
        Err(e) => { eprintln!("PROBE3782 Q1 CONTROL execute -> Err {e}"); 0 }
    };
    cqlite_core::probe3782::dump("q1-control-execute");
    eprintln!("PROBE3782 Q1 log-lines-after-CONTROL={}", String::from_utf8_lossy(&sink.0.lock().unwrap().clone()).lines().count());

    // MUTATED: materializing
    cqlite_core::probe3782::reset();
    let db2 = setup_dir(FIX_KS, "basic-types.cql", tmp.join("sstables")).await;
    match db2.execute(&format!("SELECT * FROM {FIX_KS}.{FIX_TABLE}")).await {
        Ok(r) => eprintln!("PROBE3782 Q1 MUTATED execute -> Ok rows={} (control={ctl_rows})", r.rows.len()),
        Err(e) => eprintln!("PROBE3782 Q1 MUTATED execute -> Err {e}"),
    }
    cqlite_core::probe3782::dump("q1-mutated-execute");
    eprintln!("PROBE3782 Q1 log-lines-after-MUTATED-execute={}", String::from_utf8_lossy(&sink.0.lock().unwrap().clone()).lines().count());

    // MUTATED: streaming (windowed) path
    cqlite_core::probe3782::reset();
    let cfg = StreamingConfig { buffer_size: 8, ..Default::default() };
    match db2.execute_streaming(&format!("SELECT * FROM {FIX_KS}.{FIX_TABLE}"), cfg).await {
        Ok(mut it) => {
            let (mut ok, mut err) = (0usize, 0usize);
            let mut first_err = String::new();
            while let Some(item) = it.next_async().await {
                match item { Ok(_) => ok += 1, Err(e) => { err += 1; if first_err.is_empty() { first_err = e.to_string(); } } }
            }
            eprintln!("PROBE3782 Q1 MUTATED streaming -> ok_rows={ok} err_items={err} first_err={first_err}");
        }
        Err(e) => eprintln!("PROBE3782 Q1 MUTATED streaming -> Err {e}"),
    }
    cqlite_core::probe3782::dump("q1-mutated-streaming");
    let logs = String::from_utf8_lossy(&sink.0.lock().unwrap().clone()).to_string();
    let mut shapes: std::collections::BTreeMap<String, usize> = Default::default();
    for l in logs.lines() {
        // strip the leading RFC3339 timestamp, keep level+target+message head
        let rest = l.splitn(2, ' ').nth(1).unwrap_or(l);
        let k: String = rest.chars().take(150).collect();
        *shapes.entry(k).or_insert(0) += 1;
    }
    eprintln!("PROBE3782 Q1 WARN/ERROR log lines total={} distinct={}", logs.lines().count(), shapes.len());
    for (k, v) in shapes { eprintln!("PROBE3782 Q1 LOG {v:>4}  {k}"); }
    std::fs::write("/tmp/p3782/q1-warns.log", &logs).unwrap();
    eprintln!("PROBE3782 Q1 fallback-warn-count={}", logs.matches("falling back to a full sequential scan").count());
}

/// Q2: drive the SAME mutated fixture through the COMPACTION entry point, and
/// through `iterate_all_partitions` (the #2302 index-random-read path).
#[tokio::test]
async fn q2_compaction_path_mutated_clustering_text() {
    use std::sync::Arc;
    use cqlite_core::platform::Platform;
    use cqlite_core::storage::sstable::SSTableReader;

    let sink = LogSink::default();
    let _ = tracing_subscriber::fmt()
        .with_writer(sink.clone())
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .try_init();

    let src = fixture_dir(FIX_KS, FIX_TABLE);
    let tmp = std::env::temp_dir().join(format!("p3782-q2-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    let stage = tmp.join("mut");
    let ctl = tmp.join("ctl");
    std::fs::create_dir_all(&ctl).unwrap();
    for e in std::fs::read_dir(&src).unwrap().flatten() {
        std::fs::copy(e.path(), ctl.join(e.file_name())).unwrap();
    }
    mutate_clustering_utf8(&src, &stage, &[b"necessary", b"purpose", b"artist", b"region"]);

    let config = cqlite_core::Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));

    // Schema from the committed CQL fixture, parsed the way ingestion does.
    let schema = {
        let cql = std::fs::read_to_string(schemas_dir().join("basic-types.cql")).unwrap();
        let start = cql.find(&format!("CREATE TABLE IF NOT EXISTS {FIX_TABLE}")).expect("table stmt");
        let end = start + cql[start..].find(';').expect("stmt terminator") + 1;
        let mut t = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end]).expect("parse schema");
        t.keyspace = FIX_KS.to_string();
        t
    };

    for (label, dir) in [("CONTROL", &ctl), ("MUTATED", &stage)] {
        let data_db = comp_file(dir, "-Data.db");
        let reader = SSTableReader::open(&data_db, &config, platform.clone())
            .await
            .expect("open reader");
        cqlite_core::probe3782::reset();
        let before = String::from_utf8_lossy(&sink.0.lock().unwrap().clone()).lines().count();
        match reader.iterate_all_partitions_for_compaction(Some(&schema)).await {
            Ok(rows) => eprintln!("PROBE3782 Q2 {label} compaction -> Ok compaction_rows={}", rows.len()),
            Err(e) => eprintln!("PROBE3782 Q2 {label} compaction -> Err {e}"),
        }
        let after = String::from_utf8_lossy(&sink.0.lock().unwrap().clone()).lines().count();
        eprintln!("PROBE3782 Q2 {label} compaction warn/error log lines = {}", after - before);
        cqlite_core::probe3782::dump(&format!("q2-{label}-compaction"));

        cqlite_core::probe3782::reset();
        let before = String::from_utf8_lossy(&sink.0.lock().unwrap().clone()).lines().count();
        match reader.iterate_all_partitions().await {
            Ok(rows) => eprintln!("PROBE3782 Q2 {label} iterate_all_partitions -> Ok partitions={}", rows.len()),
            Err(e) => eprintln!("PROBE3782 Q2 {label} iterate_all_partitions -> Err {e}"),
        }
        let logs = String::from_utf8_lossy(&sink.0.lock().unwrap().clone()).to_string();
        eprintln!("PROBE3782 Q2 {label} iterate warn/error log lines = {}", logs.lines().count() - before);
        eprintln!("PROBE3782 Q2 {label} index-fallback-warn total-so-far = {}",
            logs.matches("falling back to a full sequential scan").count());
        cqlite_core::probe3782::dump(&format!("q2-{label}-iterate"));
    }
    let logs = String::from_utf8_lossy(&sink.0.lock().unwrap().clone()).to_string();
    std::fs::write("/tmp/p3782/q2-warns.log", &logs).unwrap();
}
