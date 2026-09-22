//! Issue #4197 (spec R1) — `Digest.crc32`, `CRC.db` and `TOC.txt` are pure
//! functions of `Data.db`'s existing bytes, so rebuild must reproduce them
//! byte-identical (Digest/CRC.db) or set-identical (TOC.txt) to what
//! Cassandra itself wrote.
//!
//! Oracle: the fixture's OWN original component bytes — Cassandra wrote
//! them, this test deletes a COPY and rebuilds it, then compares against the
//! untouched original. This is a Cassandra-written oracle (CLAUDE.md #3042),
//! not a CQLite round trip: the comparison target was never produced by
//! CQLite.
//!
//! Dataset doctrine (issues #719/#3220): `test_basic.composite_key_table`
//! and `test_basic.uncompressed_table` carry committed JSONL sidecars but no
//! tracked `*.db` binary, so an absence skips (or, under
//! `CQLITE_REQUIRE_FIXTURES=1`, fails).

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::path::Path;

use cqlite_core::storage::write_engine::rebuild::{rebuild_components, Component, RebuildOptions};
use tempfile::TempDir;

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/rebuild_fixtures.rs"]
mod rebuild_fixtures;

use rebuild_fixtures::{
    component_exists, copy_fixture_dir, read_component, require_fixtures_strict, single_data_db,
    table_schema,
};

async fn run_r1(
    keyspace: &str,
    table: &str,
    schema_file: &str,
    components: &[&str],
    has_crc_db: bool,
) {
    let Some(root) = datasets_root::sstables_root_for_table(keyspace, table) else {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but {keyspace}.{table} is absent; {}",
                datasets_root::describe_search(keyspace, table)
            );
        }
        eprintln!("[issue_4197] {keyspace}.{table} fixture absent; skipping");
        return;
    };
    let fixture_dir = datasets_root::table_generation_dirs(&root, keyspace, table)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("{keyspace}.{table}: no usable generation directory"));

    let schema = table_schema(schema_file, table, keyspace);
    let temp = TempDir::new().expect("tempdir");
    let working = copy_fixture_dir(&fixture_dir, temp.path());
    let data_db = single_data_db(&working);

    for c in components {
        let suffix = match *c {
            "digest" => "Digest.crc32",
            "crc" => "CRC.db",
            "toc" => "TOC.txt",
            other => panic!("unhandled component in R1 test: {other}"),
        };
        let path = working.join(format!(
            "{}{suffix}",
            data_db
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap()
                .trim_end_matches("Data.db")
        ));
        // `crc` against a compressed input never had a `CRC.db` to begin
        // with (spec R1.2) — nothing to delete; the assertion below is
        // exactly that `skipped_not_applicable` fires with no file created.
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && *c == "crc" && !has_crc_db => {}
            Err(e) => panic!("delete {suffix}: {e}"),
        }
    }

    let out = temp.path().join("out");
    let requested: Vec<Component> = components
        .iter()
        .map(|c| Component::parse(c).unwrap())
        .collect();
    let options = RebuildOptions {
        out_dir: out.clone(),
        statistics_recovery_source: None,
    };
    let report = rebuild_components(&data_db, &schema, &requested, &options)
        .await
        .expect("rebuild_components must succeed on a healthy fixture");
    assert!(
        report.refused.is_none(),
        "{keyspace}.{table}: healthy fixture rebuild refused: {:?}",
        report.refused
    );

    let sha256 = |path: &Path| -> Vec<u8> {
        use sha2::{Digest, Sha256};
        let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("{path:?}: {e}"));
        Sha256::digest(bytes).to_vec()
    };

    if components.contains(&"digest") {
        let original = read_component(&fixture_dir, "Digest.crc32");
        let rebuilt = read_component(&out, "Digest.crc32");
        assert_eq!(
            original, rebuilt,
            "{keyspace}.{table}: Digest.crc32 not byte-identical after rebuild"
        );
    }
    if components.contains(&"crc") {
        if has_crc_db {
            let original = read_component(&fixture_dir, "CRC.db");
            let rebuilt = read_component(&out, "CRC.db");
            assert_eq!(
                original, rebuilt,
                "{keyspace}.{table}: CRC.db not byte-identical after rebuild"
            );
        } else {
            assert!(
                report
                    .skipped_not_applicable
                    .iter()
                    .any(|s| s.component == "crc"),
                "{keyspace}.{table}: crc must be skipped_not_applicable for a compressed input; \
                 report={report:?}"
            );
            assert!(!component_exists(&out, "CRC.db"));
        }
    }
    if components.contains(&"toc") {
        let original_data_sha = sha256(&single_data_db(&fixture_dir));
        let rebuilt_data_sha = sha256(&single_data_db(&out));
        assert_eq!(
            original_data_sha, rebuilt_data_sha,
            "{keyspace}.{table}: Data.db must be copied byte-identically into --out (R5.2)"
        );
        let toc_text =
            String::from_utf8(read_component(&out, "TOC.txt")).expect("TOC.txt is UTF-8");
        assert!(
            toc_text.contains("Data.db") && toc_text.contains("TOC.txt"),
            "{keyspace}.{table}: rebuilt TOC.txt must name Data.db and itself; got:\n{toc_text}"
        );
    }

    eprintln!(
        "[issue_4197] {keyspace}.{table}: rebuilt {components:?} byte/set-identical to the \
         Cassandra-written originals."
    );
}

#[tokio::test]
async fn rebuild_digest_and_toc_byte_parity_compressed() {
    run_r1(
        "test_basic",
        "composite_key_table",
        "basic-types.cql",
        &["digest", "toc"],
        false,
    )
    .await;
}

/// `crc` against a COMPRESSED input must be `skipped_not_applicable`, never
/// written (spec R1.2).
#[tokio::test]
async fn rebuild_crc_not_applicable_to_compressed_input() {
    run_r1(
        "test_basic",
        "composite_key_table",
        "basic-types.cql",
        &["crc"],
        false,
    )
    .await;
}

/// Uncompressed BIG: `Digest.crc32` AND `CRC.db` both regenerate
/// byte-identical (spec R1.1).
#[tokio::test]
async fn rebuild_digest_and_crc_byte_parity_uncompressed() {
    run_r1(
        "test_basic",
        "uncompressed_table",
        "basic-types.cql",
        &["digest", "crc", "toc"],
        true,
    )
    .await;
}
