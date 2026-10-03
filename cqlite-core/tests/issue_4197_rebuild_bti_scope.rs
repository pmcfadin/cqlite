//! Issue #4197 — the BTI (`da`) index-rebuild SCOPE BOUNDARY, pinned.
//!
//! BTI's own index (`Partitions.db`/`Rows.db`, [`Component::Index`]'s BTI
//! form) is deliberately NOT implemented in this change: the byte-extent
//! walk plus `PartitionsTrieWriter`/`RowsTrieWriter` wiring is substantial
//! enough to warrant its own review pass. The spec amendment that dropped
//! the original "R2.2 BTI Partitions.db/Rows.db byte parity" scenario defers
//! it to follow-up **issue #4336** (epic #4192).
//!
//! A deferral is only honest if it is also FAIL-CLOSED, so this file pins
//! the one observable consequence: asking for `index` against a `da` input
//! is a USAGE ERROR — `Error::UnsupportedFormat` — never a panic, never a
//! silent success, and never a wrong-format guess that writes a BIG
//! `Index.db` beside a BTI generation.
//!
//! # WHEN #4336 LANDS, THIS FILE IS THE ONE TO CHANGE
//!
//! Implementing BTI index rebuild makes these assertions WRONG BY DESIGN.
//! Replace them with the byte-parity assertions the dropped R2.2 scenario
//! described (`test_da` fixtures, `Partitions.db`/`Rows.db` byte-identical
//! to the Cassandra-written originals) rather than relaxing them.
//!
//! Oracle: real Cassandra 5.0.2-written `test_da` BTI fixtures from the
//! committed corpus — no synthetic input.

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::path::PathBuf;

use cqlite_core::storage::write_engine::rebuild::{rebuild_components, Component, RebuildOptions};
use cqlite_core::Error;
use tempfile::TempDir;

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/rebuild_fixtures.rs"]
mod rebuild_fixtures;

use rebuild_fixtures::{
    component_exists, copy_fixture_dir, read_component, require_fixtures_strict, single_data_db,
    table_schema,
};

/// Every committed `test_da` (BTI) table this boundary must hold for, with
/// the committed CQL schema each one is declared in.
const BTI_TABLES: &[(&str, &str, &str)] = &[
    ("test_da", "simple_table", "da-test.cql"),
    ("test_da", "collection_table", "da-test.cql"),
    ("test_da", "ttl_table", "da-test.cql"),
];

fn generation_dir(keyspace: &str, table: &str) -> Option<PathBuf> {
    let root = datasets_root::sstables_root_for_table(keyspace, table)?;
    datasets_root::table_generation_dirs(&root, keyspace, table)
        .into_iter()
        .next()
}

/// Asking for `index` against a `da` input returns `Error::UnsupportedFormat`
/// — the EXACT variant, with a message that names the tracking issue — and
/// writes nothing.
#[tokio::test]
async fn bti_index_rebuild_is_refused_as_unsupported_format_pending_4336() {
    let mut exercised = 0usize;

    for &(keyspace, table, schema_file) in BTI_TABLES {
        let Some(fixture_dir) = generation_dir(keyspace, table) else {
            if require_fixtures_strict() {
                panic!(
                    "CQLITE_REQUIRE_FIXTURES=1 but {keyspace}.{table} is absent; {}",
                    datasets_root::describe_search(keyspace, table)
                );
            }
            eprintln!("[issue_4197] {keyspace}.{table} fixture absent; skipping");
            continue;
        };

        // The fixture really is BTI — otherwise this test would "pass" by
        // asserting a BIG-input behaviour that has nothing to do with #4336.
        let data_db_name = single_data_db(&fixture_dir)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_string();
        assert!(
            data_db_name.contains("-bti-"),
            "{keyspace}.{table} must be a BTI generation for this boundary to mean anything; \
             got {data_db_name}"
        );

        let schema = table_schema(schema_file, table, keyspace);
        let temp = TempDir::new().expect("tempdir");
        let working = copy_fixture_dir(&fixture_dir, temp.path());
        let data_db = single_data_db(&working);
        let out = temp.path().join("out");
        let options = RebuildOptions {
            out_dir: out.clone(),
            statistics_recovery_source: None,
        };

        // (a) `index` alone, and (b) `index` mixed with components that ARE
        // supported for BTI — the second shape is the one a silent-success
        // or partial-write regression would slip through, since the run has
        // real work it could otherwise have done.
        for requested in [
            vec![Component::Index],
            vec![Component::Index, Component::Digest, Component::Toc],
            vec![Component::Filter, Component::Index],
        ] {
            let result = rebuild_components(&data_db, &schema, &requested, &options).await;

            let err = match result {
                Err(e) => e,
                Ok(report) => panic!(
                    "{keyspace}.{table}: requesting {requested:?} against a BTI input must be a \
                     USAGE ERROR while BTI index rebuild is deferred to #4336, not Ok; \
                     report={report:?}"
                ),
            };
            match &err {
                Error::UnsupportedFormat(msg) => {
                    assert!(
                        msg.contains("4336"),
                        "the refusal must name the tracking issue (#4336) so an operator can \
                         find the deferred work; got: {msg}"
                    );
                    assert!(
                        msg.to_lowercase().contains("bti"),
                        "the refusal must say WHICH format is unsupported; got: {msg}"
                    );
                }
                other => panic!(
                    "{keyspace}.{table}: a BTI `index` request must fail as \
                     Error::UnsupportedFormat (a usage error, so the CLI reports it as such and \
                     never as data corruption); got {other:?}"
                ),
            }

            // Fail CLOSED: a rejected request writes nothing at all — in
            // particular no BIG-shaped `Index.db`/`Summary.db` beside a BTI
            // generation, and none of the supported components either.
            assert!(
                !out.exists()
                    || std::fs::read_dir(&out)
                        .map(|mut d| d.next().is_none())
                        .unwrap_or(true),
                "{keyspace}.{table}: a refused BTI `index` request must leave --out empty; \
                 requested={requested:?}"
            );
        }

        // Control, so the assertions above cannot pass merely because EVERY
        // BTI rebuild errors: the same fixture, same out dir, without
        // `index`, succeeds and really does write the supported components.
        let report = rebuild_components(
            &data_db,
            &schema,
            &[Component::Digest, Component::Toc],
            &options,
        )
        .await
        .unwrap_or_else(|e| {
            panic!("{keyspace}.{table}: a BTI rebuild WITHOUT `index` must succeed; got {e:?}")
        });
        assert!(
            report.refused.is_none(),
            "{keyspace}.{table}: control run refused: {:?}",
            report.refused
        );
        assert!(
            report.regenerated.iter().any(|c| c == "digest"),
            "{keyspace}.{table}: control run must actually regenerate digest; {report:?}"
        );
        assert!(
            component_exists(&out, "Digest.crc32"),
            "{keyspace}.{table}: control run must write a real Digest.crc32"
        );
        assert!(
            !report.regenerated.iter().any(|c| c == "index"),
            "{keyspace}.{table}: `index` must never be reported as regenerated for a BTI input \
             while #4336 is open; {report:?}"
        );
        assert!(
            !component_exists(&out, "Index.db"),
            "{keyspace}.{table}: a BIG-shaped Index.db must never be written beside a BTI \
             generation"
        );
        // `Partitions.db`/`Rows.db` DO appear under --out, but only as
        // verbatim copies of the untouched originals
        // (`copy_untouched_components`). Byte-equality is what distinguishes
        // "copied" from "rebuilt" — the day #4336 rebuilds them, this
        // assertion stops being the right one.
        for bti_index in ["Partitions.db", "Rows.db"] {
            assert_eq!(
                read_component(&out, bti_index),
                read_component(&fixture_dir, bti_index),
                "{keyspace}.{table}: {bti_index} under --out must be a verbatim copy of the \
                 Cassandra-written original, never a CQLite-rebuilt trie, while #4336 is open"
            );
        }

        exercised += 1;
    }

    // Committed fixtures are `must_run` per case; this guards the whole-file
    // vacuity mode where the corpus resolved but named no BTI table at all.
    if require_fixtures_strict() {
        assert_eq!(
            exercised,
            BTI_TABLES.len(),
            "CQLITE_REQUIRE_FIXTURES=1 demands every committed BTI table be exercised"
        );
    } else if exercised == 0 {
        eprintln!("[issue_4197] no test_da BTI fixture reachable; BTI scope boundary unexercised");
    }
}
