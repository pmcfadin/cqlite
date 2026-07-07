//! Architecture test: chunk decode in exactly ONE module (issue #1598, Epic G).
//!
//! Proves that `Compression::decompress` + inline chunk-CRC on the QUERY path resolve
//! to exactly one module (`reader/chunk_source.rs`), and no query path references the
//! retired `BulletproofReader` or `ChunkDecompressor`. Modeled on
//! `compile_time_heuristic_enforcement.rs` and the `parser_no_unwired_modules` guard.

use std::path::Path;

/// Static scan of `cqlite-core/src` for `Compression::decompress` call sites.
/// Excludes test code and the retired non-query modules.
fn decompress_call_sites_on_query_path() -> Vec<(String, usize)> {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let src_root = Path::new(manifest_dir).join("src");
    let mut sites = Vec::new();
    scan_dir(&src_root, &mut sites, &src_root);
    sites
}

fn scan_dir(dir: &Path, sites: &mut Vec<(String, usize)>, root: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            scan_dir(&path, sites, root);
        } else if path.extension().and_then(|s| s.to_str()) == Some("rs") {
            scan_file(&path, sites, root);
        }
    }
}

fn scan_file(path: &Path, sites: &mut Vec<(String, usize)>, root: &Path) {
    let rel = path.strip_prefix(root).unwrap_or(path);
    let rel_str = rel.display().to_string();

    // Exclude the retired non-query modules (documented non-goals) plus out-of-scope paths
    if rel_str.contains("chunk_decompressor.rs")
        || rel_str.contains("bulletproof_reader.rs")
        || rel_str.contains("benchmarks/") // benchmarks are not query-path
        || rel_str.contains("compaction.rs") // compaction read: out of scope (design.md)
        // parsing/mod.rs and parsing/block_entries.rs: iterate_all_partitions / sequential_scan
        // decode path (self.file + compression_reader model, not ReadAt+CompressionInfo+chunk-index).
        // Migrating them to ChunkSource is a scoped follow-up (see design.md "Deferred / follow-up").
        || rel_str.contains("parsing/mod.rs")
        || rel_str.contains("parsing/block_entries.rs")
    {
        return;
    }

    let Ok(contents) = std::fs::read_to_string(path) else {
        return;
    };

    let mut in_test_module = false;
    for (line_num, line) in contents.lines().enumerate() {
        let trimmed = line.trim();
        // Track #[cfg(test)] module boundaries
        if trimmed.starts_with("#[cfg(test)]") || trimmed.starts_with("#[test]") {
            in_test_module = true;
        }
        if trimmed.starts_with("mod ") && trimmed.contains("tests") {
            in_test_module = true;
        }
        // Reset on module end (heuristic: unindented closing brace after test)
        if in_test_module && trimmed == "}" && !line.starts_with(' ') {
            in_test_module = false;
        }

        // Skip test code
        if in_test_module {
            continue;
        }

        // Match `.decompress(` calls (method call on Compression)
        if trimmed.contains(".decompress(") {
            sites.push((rel_str.clone(), line_num + 1));
        }
    }
}

/// Extract the module name from a file path (e.g., "storage/sstable/reader/chunk_source.rs" → "chunk_source").
fn module_name(file_path: &str) -> &str {
    file_path
        .rsplit('/')
        .next()
        .unwrap_or(file_path)
        .strip_suffix(".rs")
        .unwrap_or(file_path)
}

#[test]
fn query_path_decompress_in_exactly_one_module() {
    let sites = decompress_call_sites_on_query_path();

    // Group by module
    let mut by_module: std::collections::HashMap<String, Vec<(String, usize)>> =
        std::collections::HashMap::new();
    for site in &sites {
        let module = module_name(&site.0).to_string();
        by_module.entry(module).or_default().push(site.clone());
    }

    // The query path must have exactly ONE module with decompress calls
    if by_module.is_empty() {
        panic!("No decompress calls found on query path — scan broken or all sites moved?");
    }

    if by_module.len() > 1 {
        eprintln!(
            "ERROR: decompress calls found in {} modules (expected exactly 1):",
            by_module.len()
        );
        for (module, module_sites) in &by_module {
            eprintln!("  Module: {}", module);
            for (file, line) in module_sites {
                eprintln!("    {}:{}", file, line);
            }
        }
        panic!(
            "Query-path decompress is duplicated across {} modules; \
             issue #1598 requires exactly ONE module (reader/chunk_source.rs)",
            by_module.len()
        );
    }

    // That one module must be chunk_source
    let (module, module_sites) = by_module.iter().next().unwrap();
    if module != "chunk_source" {
        panic!(
            "Query-path decompress is in module '{}', expected 'chunk_source'. Sites:\n{}",
            module,
            module_sites
                .iter()
                .map(|(f, l)| format!("  {}:{}", f, l))
                .collect::<Vec<_>>()
                .join("\n")
        );
    }

    eprintln!(
        "PASS: all {} query-path decompress call sites resolve to chunk_source.rs",
        module_sites.len()
    );
}

#[test]
fn query_path_does_not_reference_retired_readers() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let src_root = Path::new(manifest_dir).join("src");

    // Scan query-path modules for references to BulletproofReader or ChunkDecompressor
    let query_modules = [
        "storage/sstable/reader/data_access/mod.rs",
        "storage/sstable/reader/data_access/bti.rs",
        "storage/sstable/reader/data_access/sequential.rs",
        "storage/sstable/reader/data_access/big_point.rs",
        "storage/sstable/reader/scan_stream_windowed.rs",
        "storage/sstable/reader/partition_lookup.rs",
    ];

    let mut violations = Vec::new();
    for module in &query_modules {
        let path = src_root.join(module);
        let Ok(contents) = std::fs::read_to_string(&path) else {
            continue;
        };

        for (line_num, line) in contents.lines().enumerate() {
            if line.contains("BulletproofReader") || line.contains("ChunkDecompressor") {
                // Exclude comments mentioning the names
                let trimmed = line.trim();
                if trimmed.starts_with("//") {
                    continue;
                }
                violations.push((module.to_string(), line_num + 1, line.to_string()));
            }
        }
    }

    if !violations.is_empty() {
        eprintln!("ERROR: query path references retired readers:");
        for (file, line, content) in &violations {
            eprintln!("  {}:{}: {}", file, line, content.trim());
        }
        panic!(
            "Query path must NOT reference BulletproofReader or ChunkDecompressor \
             (retired by issue #1598); {} violations found",
            violations.len()
        );
    }

    eprintln!("PASS: query path does not reference retired readers");
}

/// Runtime check: reading the same chunk twice warm increments DECOMPRESS_CALLS exactly once.
#[tokio::test]
#[serial_test::serial]
async fn warm_cache_skips_decompress() {
    use cqlite_core::storage::sstable::index_reader::IndexReader;
    use cqlite_core::storage::sstable::SSTableReader;
    use cqlite_core::{Config, Platform, RowKey};
    use std::sync::Arc;

    let Some(data_db) = compressed_simple_table_data_db("warm_cache_skips_decompress") else {
        return;
    };

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));

    // Learn a genuinely-present raw partition key from Index.db (its entries are
    // keyed on the raw partition-key bytes since #552), so `get()` actually hits a
    // present row and exercises the decode path — `simple_table`'s PK is a UUID, so
    // a synthetic `"key1"` would resolve to absence and never decompress a chunk.
    let index_name = format!(
        "{}-Index.db",
        data_db
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_suffix("-Data.db"))
            .expect("Data.db name")
    );
    let index_path = data_db.with_file_name(index_name);
    let index_reader = IndexReader::open(&index_path, platform.clone())
        .await
        .expect("open Index.db");
    let raw_key = index_reader
        .get_partition_entries()
        .first()
        .map(|e| e.key_digest.to_vec())
        .expect("Index.db must have >=1 partition entry");

    let reader = SSTableReader::open(&data_db, &config, platform)
        .await
        .expect("open simple_table");

    // Query a partition via the point path (chunk-targeted BTI or Index.db)
    let table_id = cqlite_core::TableId::from("test_basic.simple_table");
    let key = RowKey::from(raw_key);
    SSTableReader::reset_decompress_calls();
    let cold = reader.get(&table_id, &key).await.expect("cold get");
    let cold_decompress = SSTableReader::decompress_call_count();
    assert!(cold.is_some(), "learned present key must resolve a row");
    assert!(cold_decompress >= 1, "cold read must decompress >=1 chunk");

    // Warm read: same key, cache hit, ZERO decompress
    SSTableReader::reset_decompress_calls();
    let warm = reader.get(&table_id, &key).await.expect("warm get");
    let warm_decompress = SSTableReader::decompress_call_count();
    assert_eq!(
        format!("{:?}", cold),
        format!("{:?}", warm),
        "cache must not change result"
    );
    assert_eq!(
        warm_decompress, 0,
        "warm read must skip decompress (cache hit)"
    );
}

/// Runtime check: warm windowed scan (repeat full scan) skips decompress.
///
/// Drives the WINDOWED scan plane (`scan_stream` → `run_scan_stream_windowed` →
/// `ChunkSource::new`), NOT `SSTableReader::scan()`: `scan()` on an nb/BIG SSTable
/// takes the legacy `self.file` + `compression_reader` sequential path
/// (`sequential_scan`) that is the scoped #2165 follow-up and never bumps
/// `DECOMPRESS_CALLS`. `simple_table` is nb + Snappy multi-chunk, so
/// `requires_chunk_stitching()` holds and `scan_stream` routes through the
/// ChunkSource windowed decode plane this change consolidates.
#[tokio::test]
#[serial_test::serial]
async fn warm_windowed_scan_skips_decompress() {
    use cqlite_core::storage::sstable::SSTableReader;
    use cqlite_core::{Config, Platform};
    use std::sync::Arc;

    let Some(data_db) = compressed_simple_table_data_db("warm_windowed_scan_skips_decompress")
    else {
        return;
    };

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let reader = Arc::new(
        SSTableReader::open(&data_db, &config, platform)
            .await
            .expect("open simple_table"),
    );

    let table_id = cqlite_core::TableId::from("test_basic.simple_table");

    // Cold windowed scan: drain the whole stream to populate the B1 chunk cache.
    SSTableReader::reset_decompress_calls();
    let cold_rows = drain_windowed_scan(reader.clone(), table_id.clone()).await;
    let cold_decompress = SSTableReader::decompress_call_count();
    assert!(!cold_rows.is_empty(), "fixture must have rows");
    assert!(
        cold_decompress >= 1,
        "cold windowed scan must decompress >=1 chunk through ChunkSource (got {})",
        cold_decompress
    );

    // Warm windowed scan: same reader, all chunks resident, ZERO decompress.
    SSTableReader::reset_decompress_calls();
    let warm_rows = drain_windowed_scan(reader.clone(), table_id).await;
    let warm_decompress = SSTableReader::decompress_call_count();
    assert_eq!(
        format!("{:?}", cold_rows),
        format!("{:?}", warm_rows),
        "cache must not change scan result"
    );
    assert_eq!(
        warm_decompress, 0,
        "warm windowed scan must skip decompress (all chunks resident)"
    );
}

/// Drain the full windowed scan stream (`scan_stream` → `run_scan_stream_windowed`)
/// into a sorted-by-key row list for stable comparison across cold/warm runs.
async fn drain_windowed_scan(
    reader: std::sync::Arc<cqlite_core::storage::sstable::SSTableReader>,
    table_id: cqlite_core::TableId,
) -> Vec<(cqlite_core::RowKey, cqlite_core::ScanRow)> {
    let mut rx = reader.scan_stream(table_id, None, None, None, 64);
    let mut rows = Vec::new();
    while let Some(item) = rx.recv().await {
        rows.push(item.expect("windowed scan item"));
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

fn datasets_root() -> Option<std::path::PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(std::path::PathBuf::from)
        .filter(|p| p.is_dir())
}

fn require_fixtures() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").ok().as_deref(),
        Some("1") | Some("true")
    )
}

/// Resolve the `test_basic/simple_table` COMPRESSED Data.db under the real fixture
/// layout, whose tables live in generation directories (`simple_table-<uuid>/`), not
/// a bare `simple_table/` dir. Requires a sibling `-CompressionInfo.db` so the warm
/// decode probe actually exercises a compressed chunk (the whole point of the test).
///
/// Returns `None` (after printing a SKIP) when datasets are absent; honors
/// `CQLITE_REQUIRE_FIXTURES` by panicking rather than skipping when the fixture is
/// required but missing — so this guard can never pass vacuously in a fixtures-present
/// lane.
fn compressed_simple_table_data_db(test: &str) -> Option<std::path::PathBuf> {
    let Some(root) = datasets_root() else {
        if require_fixtures() {
            panic!("[{test}] CQLITE_REQUIRE_FIXTURES=1 but datasets absent");
        }
        eprintln!("SKIP [{test}]: datasets absent");
        return None;
    };

    // Prefix-match the generation dir `simple_table-*` and require both `-Data.db`
    // and `-CompressionInfo.db` inside it (a compressed SSTable).
    let ks_dir = root.join("sstables/test_basic");
    let found = std::fs::read_dir(&ks_dir).ok().and_then(|entries| {
        entries.flatten().find_map(|e| {
            if !e.file_name().to_string_lossy().starts_with("simple_table-") {
                return None;
            }
            let dir = e.path();
            let files: Vec<String> = std::fs::read_dir(&dir)
                .ok()?
                .flatten()
                .map(|f| f.file_name().to_string_lossy().into_owned())
                .collect();
            let data = files.iter().find(|n| n.ends_with("-Data.db"))?;
            let has_compression = files.iter().any(|n| n.ends_with("-CompressionInfo.db"));
            has_compression.then(|| dir.join(data))
        })
    });

    if found.is_none() {
        if require_fixtures() {
            panic!(
                "[{test}] CQLITE_REQUIRE_FIXTURES=1 but compressed test_basic/simple_table \
                 (simple_table-*/*-Data.db + -CompressionInfo.db) not found under {}",
                ks_dir.display()
            );
        }
        eprintln!("SKIP [{test}]: compressed test_basic/simple_table fixture not present");
    }
    found
}
