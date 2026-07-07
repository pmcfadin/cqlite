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
        || rel_str.contains("parsing/") // parsing helpers: not decompress sites
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
        eprintln!("ERROR: decompress calls found in {} modules (expected exactly 1):", by_module.len());
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
            module_sites.iter()
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
    use cqlite_core::storage::sstable::SSTableReader;
    use cqlite_core::{Config, Platform};
    use std::sync::Arc;

    // Locate a compressed fixture
    let Some(datasets_root) = datasets_root() else {
        if require_fixtures() {
            panic!("CQLITE_REQUIRE_FIXTURES=1 but datasets absent");
        }
        eprintln!("SKIP: datasets absent");
        return;
    };

    let data_db = datasets_root
        .join("sstables/test_basic/simple_table")
        .read_dir()
        .ok()
        .and_then(|mut entries| {
            entries
                .find_map(|e| {
                    let p = e.ok()?.path();
                    p.file_name()?
                        .to_str()?
                        .ends_with("-Data.db")
                        .then_some(p)
                })
        });

    let Some(data_db) = data_db else {
        eprintln!("SKIP: simple_table Data.db not found");
        return;
    };

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let reader = SSTableReader::open(&data_db, &config, platform)
        .await
        .expect("open simple_table");

    // Query a partition via the point path (chunk-targeted BTI or Index.db)
    let table_id = cqlite_core::TableId::from("test_basic.simple_table");
    let key = cqlite_core::RowKey::from("key1");
    SSTableReader::reset_decompress_calls();
    let cold = reader.get(&table_id, &key).await.expect("cold get");
    let cold_decompress = SSTableReader::decompress_call_count();
    assert!(cold.is_some(), "fixture must have key1");
    assert!(
        cold_decompress >= 1,
        "cold read must decompress >=1 chunk"
    );

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
