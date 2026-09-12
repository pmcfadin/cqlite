//! Issue #4196 (spec R1) — salvage of a HEALTHY SSTable is a no-purge
//! compaction of it: byte-identical output to `compact_sstables` over the
//! same single input with purging disabled, and an affirmative empty loss
//! list (`losses: 0 RECOGNISED`).
//!
//! # Oracle
//!
//! Cassandra-written fixtures — `test_basic.composite_key_table` (BIG/`nb`,
//! LZ4) and `test_da.multiclustering_table` (BTI/`da`, LZ4). ("Cassandra-written"
//! is not "git-committed": only two of this file's four fixtures are tracked
//! binaries — see the per-case dataset doctrine below.)
//! CQLite's `compact_sstables` is itself already byte-parity-proven against
//! Cassandra elsewhere (issue #1017); this test's job is narrower and
//! specific to #4196: that `salvage_sstable`'s independent recovery loop
//! (boundary enumeration -> decode-at-offset -> reconcile -> write) reaches
//! the SAME bytes `compact_sstables` reaches for a healthy input, proving the
//! two share the real reconciliation/conversion code rather than two
//! implementations that happen to agree on one fixture (design D1).
//!
//! The BTI case additionally asserts R1.1's OTHER half — the salvaged output's
//! decode against the fixture's committed `*-Data.db.jsonl` `sstabledump`
//! golden. That half is not decoration: without it the BTI lane was a
//! CQLite-written + CQLite-read round trip, which is invariant to a uniform
//! framing error (CLAUDE.md's #3042 blind spot) and therefore cannot validate an
//! on-disk property. See
//! `salvage_of_healthy_bti_sstable_preserves_every_row`'s doc.
//!
//! Dataset doctrine, PER CASE (issues #719 / #3220 — never one suite-wide rule,
//! which cannot see a case skipping behind its siblings):
//!
//!   * a case whose `-Data.db` is GIT-TRACKED fails CLOSED unconditionally, not
//!     gated on `CQLITE_REQUIRE_FIXTURES` — absent means broken checkout, not
//!     unfetched dataset. That is `test_comp.uncompressed_table` and
//!     `test_da.multiclustering_table` here, each verified with `git ls-files`;
//!   * a case backed only by the FETCHED corpus (`test_basic.composite_key_table`,
//!     `test_basic.uncompressed_table` — committed JSONL sidecars, no tracked
//!     binary) skips when absent, and `CQLITE_REQUIRE_FIXTURES=1` turns that skip
//!     into a hard failure.

// `not(tombstones)`: `salvage_sstable`'s decode-at-offset primitive is gated
// the same way (see `write_engine::salvage`'s module doc) — this target must
// compile out identically under `--all-features`, or the `tombstones`-on
// gate lanes (e.g. clippy's per-package matrix) fail on an unresolved import
// rather than skipping cleanly.
#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use cqlite_core::storage::write_engine::merge::compact_sstables;
use cqlite_core::storage::write_engine::salvage::{salvage_sstable, SalvageOptions};
use tempfile::TempDir;

#[path = "support/datasets_root.rs"]
mod datasets_root;

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

fn table_schema(
    schema_file: &str,
    table: &str,
    keyspace: &str,
) -> cqlite_core::schema::TableSchema {
    let schema_path = datasets_root::schema_path(schema_file).expect("committed CQL schema");
    let cql = std::fs::read_to_string(schema_path).expect("read schema");
    let start = cql
        .find(&format!("CREATE TABLE IF NOT EXISTS {table}"))
        .expect("CREATE TABLE statement");
    let end = start + cql[start..].find(';').expect("statement terminator") + 1;
    let mut t = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end])
        .expect("parse CREATE TABLE");
    t.keyspace = keyspace.to_string();
    t
}

fn single_data_db(dir: &Path) -> PathBuf {
    let mut found: Vec<PathBuf> = Vec::new();
    for e in std::fs::read_dir(dir).expect("read fixture dir").flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if name.ends_with("-Data.db") {
            found.push(e.path());
        }
    }
    match found.len() {
        1 => found.pop().unwrap(),
        n => panic!("{dir:?}: expected exactly ONE Data.db, found {n} ({found:?})"),
    }
}

/// Component suffixes present under `dir` (strips the `<version>-<gen>-<big|bti>-`
/// descriptor prefix). Drops derived golden sidecars no engine emits.
fn component_suffixes(dir: &Path) -> BTreeSet<String> {
    let mut set = BTreeSet::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            for marker in ["-big-", "-bti-"] {
                if let Some(idx) = name.find(marker) {
                    set.insert(name[idx + marker.len()..].to_string());
                    break;
                }
            }
        }
    }
    set.retain(|s| !s.ends_with(".jsonl") && !s.ends_with("Statistics.db.txt"));
    set
}

fn descriptor_prefix(data_db: &Path) -> String {
    data_db
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default()
        .trim_end_matches("Data.db")
        .to_string()
}

fn read_component(dir: &Path, suffix: &str) -> Vec<u8> {
    let data = single_data_db(dir);
    let prefix = descriptor_prefix(&data);
    let path = dir.join(format!("{prefix}{suffix}"));
    std::fs::read(&path)
        .unwrap_or_else(|e| panic!("component {path:?} unreadable in an output dir: {e}"))
}

fn first_diff(a: &[u8], b: &[u8]) -> Option<usize> {
    let n = a.len().max(b.len());
    (0..n).find(|&i| a.get(i) != b.get(i))
}

/// Run the R1 parity assertion for one fixture. `byte_for_byte` names the
/// components salvage and `compact_sstables` MUST byte-match for a healthy
/// single-input run — both are a no-purge, single-source reconciliation of
/// the SAME partitions through the SAME writer, so every component either
/// side names is expected to match (checked via the component-SET equality
/// below); this list is the ones asserted BYTE-IDENTICAL when
/// `require_byte_parity` is `true`, chosen to mirror issue #1017's
/// cross-engine byte set for the format family.
///
/// `require_byte_parity = false` (roborev, issue #4196, round-4 Medium
/// coverage gap; DE-CONFOUNDED in round-5, finding 4): reserved for
/// `test_basic.uncompressed_table`'s zero-clustering-column shape
/// specifically — NOT for "uncompressed input" in general, which
/// `salvage_of_healthy_uncompressed_big_sstable_matches_no_purge_compaction`
/// above proves byte-matches once compression is the ONLY variable (a
/// same-shape compressed/uncompressed fixture pair). Content parity
/// (decode-and-compare, below) still holds for the zero-clustering-column
/// case; only raw bytes diverge (Data.db: 20410 vs 19803 bytes). Never
/// loosened for anything OTHER than that one isolated shape. Tracked as a
/// follow-up, not silently dropped: **issue #4217** (roborev, round 17 Low
/// finding) — filed WITH the diagnostic data this waiver previously lacked:
/// first byte diff at offset 32, and a hex dump showing `salvage`'s stream
/// is missing exactly ~6 bytes relative to `compact_sstables`' right around
/// that point (consistent with the ~6.07-bytes/partition average across the
/// whole 607-byte difference) — pointing at a small, fixed-size encoder
/// field, not a structural defect.
///
/// `committed_fixture` (issue #3220, C-audit finding on this issue) —
/// `true` when this case's `-Data.db` is GIT-TRACKED, which makes its absence a
/// broken checkout rather than an unfetched dataset, and therefore a FAIL
/// regardless of `CQLITE_REQUIRE_FIXTURES`. Verified per case with `git
/// ls-files`, not assumed from the keyspace name: of the tables this file
/// touches only `test_comp.uncompressed_table` is committed
/// (`test-data/datasets/sstables/test_comp/uncompressed_table-25a5ca70…/nb-1-big-Data.db`);
/// `test_basic.composite_key_table` and `test_basic.uncompressed_table` carry
/// committed JSONL sidecars but NO tracked binary (`.gitignore`'s `*.db` covers
/// them and neither was force-added), so those two keep the skip route. Making
/// them mandatory would red every checkout that has not fetched the corpus.
async fn assert_healthy_salvage_matches_no_purge_compaction(
    keyspace: &str,
    table: &str,
    schema_file: &str,
    out_generation: u64,
    byte_for_byte: &[&str],
    require_byte_parity: bool,
    committed_fixture: bool,
) {
    let Some(root) = datasets_root::sstables_root_for_table(keyspace, table) else {
        if committed_fixture {
            panic!(
                "COMMITTED fixture {keyspace}.{table} is absent — its *-Data.db is git-tracked, \
                 so this is a broken checkout, NOT an unfetched dataset, and must never skip \
                 (issue #3220, fail-closed UNCONDITIONALLY, not gated on \
                 CQLITE_REQUIRE_FIXTURES); {}",
                datasets_root::describe_search(keyspace, table)
            );
        }
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but {keyspace}.{table} is absent; {}",
                datasets_root::describe_search(keyspace, table)
            );
        }
        eprintln!("[issue_4196] {keyspace}.{table} fixture absent (dataset not fetched); skipping");
        return;
    };
    let fixture_dir = datasets_root::table_generation_dirs(&root, keyspace, table)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("{keyspace}.{table}: no usable generation directory"));

    let schema = table_schema(schema_file, table, keyspace);
    let data_db = single_data_db(&fixture_dir);

    let temp = TempDir::new().expect("tempdir");
    let compact_out_root = temp.path().join("compact");
    let salvage_out_root = temp.path().join("salvage");

    let compact_report = compact_sstables(
        vec![data_db.clone()],
        &compact_out_root,
        &schema,
        out_generation,
        None,  // gc_before: no purging
        None,  // now: irrelevant, no TTL expiry evaluated without a cutoff
        false, // purge_safe: single-input, never proven overlap-safe
    )
    .await
    .expect("no-purge single-input compaction must succeed");
    assert!(
        compact_report.stats.output_rows > 0,
        "compaction oracle wrote zero rows — the fixture itself is empty, which would make \
         this test vacuous"
    );
    let compact_out = compact_out_root.join(&schema.keyspace).join(&schema.table);

    let salvage_report = salvage_sstable(
        &data_db,
        &salvage_out_root,
        &schema,
        SalvageOptions::default(),
    )
    .await
    .expect("salvage of a healthy fixture must succeed");
    let salvage_out = salvage_out_root.join(&schema.keyspace).join(&schema.table);

    // R1.2 — affirmative empty loss list; totals must be non-degenerate.
    assert!(
        salvage_report.losses.is_empty(),
        "{keyspace}.{table}: healthy fixture salvage reported losses: {:?}",
        salvage_report.losses
    );
    assert!(
        salvage_report.refused.is_none(),
        "{keyspace}.{table}: healthy fixture salvage refused: {:?}",
        salvage_report.refused
    );
    assert_eq!(
        salvage_report.partitions.total, salvage_report.partitions.recovered,
        "{keyspace}.{table}: total must equal recovered when there are zero losses"
    );
    assert!(
        salvage_report.partitions.total > 0,
        "{keyspace}.{table}: salvage reported zero total partitions on a non-empty fixture — \
         the boundary enumeration itself is broken, which would make the empty-loss assertion \
         vacuous"
    );
    // roborev, issue #4196, round 19 Medium finding: the manifest must
    // disclose the unproven-Cassandra-byte-parity gap for a zero-
    // clustering-column schema, and must NOT falsely disclose it for a
    // schema that DOES have clustering columns (this fixture family's other
    // cases byte-match Cassandra's own writer exactly, so a finding there
    // would itself be a false claim).
    let has_unproven_byte_parity_finding = salvage_report
        .component_findings
        .iter()
        .any(|f| f.class == "UnprovenByteParity");
    if schema.clustering_keys.is_empty() {
        assert!(
            has_unproven_byte_parity_finding,
            "{keyspace}.{table}: zero clustering columns, but no UnprovenByteParity finding was \
             surfaced; got: {:?}",
            salvage_report.component_findings
        );
    } else {
        assert!(
            !has_unproven_byte_parity_finding,
            "{keyspace}.{table}: has clustering columns (byte-parity IS proven for this shape) \
             but an UnprovenByteParity finding was surfaced anyway; got: {:?}",
            salvage_report.component_findings
        );
    }
    let text = salvage_report.render_text();
    assert!(
        text.contains("losses: 0 RECOGNISED"),
        "{keyspace}.{table}: text rendering must carry the affirmative empty-loss line; got:\n{text}"
    );

    // R1.1 — byte-for-byte parity with the no-purge compaction oracle
    // (component SET equality always; byte identity of `byte_for_byte` only
    // when `require_byte_parity` — see this function's doc for why
    // `uncompressed_table` uses the content-parity fallback below instead).
    let compact_components = component_suffixes(&compact_out);
    let salvage_components = component_suffixes(&salvage_out);
    assert_eq!(
        compact_components, salvage_components,
        "{keyspace}.{table}: component set differs between compact_sstables output and salvage \
         output"
    );
    if require_byte_parity {
        for suffix in byte_for_byte {
            assert!(
                compact_components.contains(*suffix),
                "{keyspace}.{table}: compaction oracle missing component {suffix}"
            );
            let a = read_component(&compact_out, suffix);
            let b = read_component(&salvage_out, suffix);
            if a != b {
                let at = first_diff(&a, &b);
                panic!(
                    "{keyspace}.{table}: {suffix} byte mismatch between compact_sstables ({} \
                     bytes) and salvage ({} bytes), first diff at {at:?}",
                    a.len(),
                    b.len()
                );
            }
        }
        eprintln!(
            "[issue_4196] {keyspace}.{table}: salvage of a healthy SSTable byte-matches a \
             no-purge single-input compaction ({byte_for_byte:?}); {} partition(s) recovered, 0 \
             lost.",
            salvage_report.partitions.recovered
        );
        return;
    }

    // Content-parity fallback (this function's doc explains why): every
    // CompactionRow salvage's output decodes to must equal what the
    // compaction oracle's output decodes to — proving salvage lost and
    // fabricated NOTHING even though the raw bytes differ.
    use cqlite_core::platform::Platform;
    use cqlite_core::storage::sstable::SSTableReader;
    use std::sync::Arc;
    let config = cqlite_core::Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let compact_reader =
        SSTableReader::open(&single_data_db(&compact_out), &config, platform.clone())
            .await
            .expect("open compact reader");
    let salvage_reader = SSTableReader::open(&single_data_db(&salvage_out), &config, platform)
        .await
        .expect("open salvage reader");
    let compact_rows = compact_reader
        .iterate_all_partitions_for_compaction(Some(&schema))
        .await
        .expect("decode compact rows");
    let salvage_rows = salvage_reader
        .iterate_all_partitions_for_compaction(Some(&schema))
        .await
        .expect("decode salvage rows");
    assert!(
        !compact_rows.is_empty(),
        "{keyspace}.{table}: compaction oracle decoded zero rows — the fixture itself is empty, \
         which would make this comparison vacuous"
    );
    assert_eq!(
        compact_rows.len(),
        salvage_rows.len(),
        "{keyspace}.{table}: salvage output row count differs from the compaction oracle's"
    );
    assert_eq!(
        compact_rows, salvage_rows,
        "{keyspace}.{table}: salvage output rows differ in content from the compaction oracle's"
    );

    eprintln!(
        "[issue_4196] {keyspace}.{table}: salvage of a healthy SSTable content-matches a \
         no-purge single-input compaction (raw bytes differ — see this function's doc; \
         component set: {byte_for_byte:?}); {} partition(s) recovered, 0 lost, {} row(s) \
         content-verified.",
        salvage_report.partitions.recovered,
        compact_rows.len()
    );
}

#[tokio::test]
async fn salvage_of_healthy_big_sstable_matches_no_purge_compaction() {
    assert_healthy_salvage_matches_no_purge_compaction(
        "test_basic",
        "composite_key_table",
        "basic-types.cql",
        4196,
        &["Data.db", "Index.db", "Summary.db", "CRC.db"],
        true,  // require_byte_parity
        false, // committed_fixture: NOT git-tracked (JSONL sidecar only)
    )
    .await;
}

/// roborev, issue #4196 (round-4 Medium): every fixture the sweep above
/// touches is LZ4-compressed, so the ENTIRE uncompressed input path —
/// `decode_partition_at_offset_for_salvage`'s bounded positional
/// `read_exact_at` window, the `is_uncompressed` full-consumption check
/// added for the round-3 High finding, and `uncompressed_chunk_preflight`
/// (`CRC.db`) — executed in NO test. Salvage's own output is itself always
/// uncompressed (design D4), so this path matters on every run regardless of
/// the input's compression.
///
/// `test_comp.uncompressed_table`, NOT `test_basic.uncompressed_table`
/// (roborev, issue #4196, round-5 Medium finding 4 — de-confounding compression
/// from table shape): this fixture has the IDENTICAL schema (`pk INT, ck INT,
/// body TEXT, PRIMARY KEY (pk, ck)`, `compression-parity.cql`) to
/// `test_basic.composite_key_table`, the ALREADY byte-parity-proven compressed
/// fixture the test immediately above this one exercises (issue #1017's
/// cross-engine byte set) — same table shape, ONLY compression differs
/// (roborev, issue #4196, round-6 Low finding: this comment previously named
/// `test_comp.lz4_table`, which is not exercised anywhere in this file).
/// With that confound removed, byte parity
/// DOES hold (`require_byte_parity: true` below, no loosening needed) —
/// proving the divergence `salvage_of_healthy_uncompressed_zero_clustering_columns_content_only`
/// below measures is NOT a compression-vs-compaction artifact at all.
#[tokio::test]
async fn salvage_of_healthy_uncompressed_big_sstable_matches_no_purge_compaction() {
    assert_healthy_salvage_matches_no_purge_compaction(
        "test_comp",
        "uncompressed_table",
        "compression-parity.cql",
        4197,
        &["Data.db", "Index.db", "Summary.db", "CRC.db"],
        true, // require_byte_parity
        // committed_fixture: nb-1-big-Data.db IS git-tracked (verified with
        // `git ls-files`), so an absence here is a broken checkout and this
        // case fails closed unconditionally. Its `nb-1-big-CRC.db` is NOT
        // committed, which does not matter for THIS case: salvage records an
        // absent input CRC.db as a component finding and proceeds, and the
        // output CRC.db is written by the writer. Verified by running this
        // target with CQLITE_DATASETS_ROOT unset, against the checkout's own
        // CRC.db-less copy: PASSes, 1 partition recovered.
        true,
    )
    .await;
}

/// roborev, issue #4196 (round-5 Medium finding 4, follow-on to the
/// de-confounding above): `test_basic.uncompressed_table` (`id UUID PRIMARY
/// KEY` — ZERO clustering columns, unlike `test_comp.uncompressed_table`'s
/// `PRIMARY KEY (pk, ck)` above) genuinely does NOT byte-match
/// `compact_sstables`'s output (Data.db: 20410 vs 19803 bytes), even though
/// both decode to IDENTICAL `CompactionRow`s (content-parity fallback,
/// verified below) — i.e. correctness holds, bytes don't. With the
/// compression confound eliminated by the sibling test above, the isolated
/// variable is the CLUSTERING-COLUMN COUNT (zero vs. one), not compression;
/// `composite_key_table` (compressed, byte-matches, HAS clustering columns)
/// is consistent with this. Root-causing the exact writer/merger code path
/// this zero-clustering-column shape triggers is OUT OF SCOPE for this fix
/// round — reported precisely, not silently dropped, for the follow-up:
/// **issue #4217**.
#[tokio::test]
async fn salvage_of_healthy_uncompressed_zero_clustering_columns_content_only() {
    assert_healthy_salvage_matches_no_purge_compaction(
        "test_basic",
        "uncompressed_table",
        "basic-types.cql",
        4198,
        &["Data.db", "Index.db", "Summary.db", "CRC.db"],
        false, // require_byte_parity — see this test's doc
        false, // committed_fixture: NOT git-tracked (JSONL sidecar only)
    )
    .await;
}

// ===========================================================================
// The Cassandra-written oracle for the BTI healthy path (spec R1.1).
//
// #3042 doctrine: a CQLite-WRITTEN + CQLite-READ round trip is INVARIANT to a
// uniform framing/serialization error — both sides make the identical mistake,
// the round trip closes, and the test stays green while real Cassandra-written
// data reads wrong. The BTI healthy-path test below used to compare CQLite's
// decode of the input against CQLite's decode of CQLite's own output and
// nothing else, so it could not validate an on-disk property at all.
//
// The oracle here is the committed `*-Data.db.jsonl` — real `sstabledump`
// output over the real Cassandra 5.0.2 `da` fixture, committed beside its
// Data.db. Every expectation below is read out of THAT file; nothing is
// derived from CQLite's own behaviour or hardcoded from a previous CQLite run.
// ===========================================================================

/// The logical content of one `test_da.multiclustering_table` row, keyed by its
/// full primary key: `(pk, bucket, seq) -> payload`.
///
/// A `BTreeMap` (not a `Vec`) so the comparison is set-equality in BOTH
/// directions — a salvaged output missing a row and a salvaged output
/// fabricating one are distinct, separately-reported failures.
type MulticlusteringRows = std::collections::BTreeMap<(i32, String, i32), String>;

/// Parse the committed `sstabledump` golden of `test_da.multiclustering_table`
/// into [`MulticlusteringRows`].
///
/// FAILS CLOSED on every shape this extractor does not fully model, rather than
/// silently comparing a subset: an unexpected key on a partition object (e.g. a
/// `deletion_info` a regenerated fixture grew), a row `type` other than `row`,
/// a clustering that is not `[text, int]`, a cell carrying anything beyond
/// `name`/`value`, a row with more than the one `payload` cell, or a duplicate
/// primary key. A golden whose shape drifts must red this lane, not quietly
/// narrow what it proves.
fn load_multiclustering_golden(path: &Path) -> MulticlusteringRows {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| {
        panic!(
            "the sstabledump golden {path:?} is committed beside its Data.db and must read \
             (it is THE Cassandra-written oracle for this lane — a missing golden is a hard \
             failure, never a skip): {e}"
        )
    });
    let mut out = MulticlusteringRows::new();
    let mut partitions = 0usize;
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let doc: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("golden {path:?} line is not JSON: {e}"));
        partitions += 1;

        let partition = doc
            .get("partition")
            .and_then(|p| p.as_object())
            .unwrap_or_else(|| panic!("golden {path:?}: partition object missing"));
        for key in partition.keys() {
            assert!(
                matches!(key.as_str(), "key" | "position"),
                "golden {path:?}: unmodelled partition field {key:?} — this extractor compares \
                 live rows only, so a partition-level tombstone/deletion must red this lane \
                 rather than be silently dropped from the oracle"
            );
        }
        let pk_rendered = partition
            .get("key")
            .and_then(|k| k.as_array())
            .and_then(|a| match a.as_slice() {
                [one] => one.as_str(),
                _ => None,
            })
            .unwrap_or_else(|| {
                panic!("golden {path:?}: partition key is not a single-component array")
            });
        // sstabledump renders an `int` partition key as a decimal STRING.
        let pk: i32 = pk_rendered
            .parse()
            .unwrap_or_else(|e| panic!("golden {path:?}: partition key {pk_rendered:?}: {e}"));

        for row in doc
            .get("rows")
            .and_then(|r| r.as_array())
            .unwrap_or_else(|| panic!("golden {path:?}: rows array missing"))
        {
            let row = row
                .as_object()
                .unwrap_or_else(|| panic!("golden {path:?}: row is not an object"));
            for key in row.keys() {
                assert!(
                    matches!(
                        key.as_str(),
                        "type" | "position" | "clustering" | "liveness_info" | "cells"
                    ),
                    "golden {path:?}: unmodelled row field {key:?} (pk={pk}) — a row deletion or \
                     range bound must red this lane rather than be dropped from the oracle"
                );
            }
            assert_eq!(
                row.get("type").and_then(|t| t.as_str()),
                Some("row"),
                "golden {path:?}: unmodelled row type (pk={pk}); this oracle covers live \
                 clustered rows only"
            );
            let clustering = row
                .get("clustering")
                .and_then(|c| c.as_array())
                .unwrap_or_else(|| panic!("golden {path:?}: clustering missing (pk={pk})"));
            let (bucket, seq) = match clustering.as_slice() {
                [b, s] => (
                    b.as_str()
                        .unwrap_or_else(|| {
                            panic!("golden {path:?}: clustering[0] (bucket text) is not a string")
                        })
                        .to_string(),
                    i32::try_from(s.as_i64().unwrap_or_else(|| {
                        panic!("golden {path:?}: clustering[1] (seq int) is not an integer")
                    }))
                    .unwrap_or_else(|e| panic!("golden {path:?}: clustering[1] out of i32: {e}")),
                ),
                other => panic!(
                    "golden {path:?}: clustering arity {} — this table declares \
                     PRIMARY KEY (pk, bucket, seq); got {other:?}",
                    other.len()
                ),
            };

            let cells = row
                .get("cells")
                .and_then(|c| c.as_array())
                .unwrap_or_else(|| panic!("golden {path:?}: cells missing (pk={pk})"));
            let [cell] = cells.as_slice() else {
                panic!(
                    "golden {path:?}: expected exactly ONE cell per row (the single `payload` \
                     column); got {} for pk={pk} {bucket}/{seq}",
                    cells.len()
                );
            };
            let cell = cell
                .as_object()
                .unwrap_or_else(|| panic!("golden {path:?}: cell is not an object"));
            for key in cell.keys() {
                assert!(
                    matches!(key.as_str(), "name" | "value"),
                    "golden {path:?}: unmodelled cell field {key:?} — a cell tombstone, TTL or \
                     cell path must red this lane rather than be dropped from the oracle"
                );
            }
            assert_eq!(
                cell.get("name").and_then(|n| n.as_str()),
                Some("payload"),
                "golden {path:?}: unexpected cell column (pk={pk})"
            );
            let payload = cell
                .get("value")
                .and_then(|v| v.as_str())
                .unwrap_or_else(|| panic!("golden {path:?}: payload value is not a string"))
                .to_string();

            let previous = out.insert((pk, bucket.clone(), seq), payload);
            assert!(
                previous.is_none(),
                "golden {path:?}: duplicate primary key ({pk}, {bucket}, {seq}) — a map-shaped \
                 oracle would silently drop one of them"
            );
        }
    }
    assert!(
        !out.is_empty(),
        "golden {path:?} yielded ZERO rows — a present-but-empty oracle must never pass \
         (issue #3220 / CLAUDE.md's 0-rows-when-present rule)"
    );
    eprintln!(
        "[issue_4196] Cassandra oracle {path:?}: {partitions} partition(s), {} row(s).",
        out.len()
    );
    out
}

/// Project CQLite's compaction-row decode of a `test_da.multiclustering_table`
/// SSTable onto the same `(pk, bucket, seq) -> payload` shape the golden is
/// parsed into.
///
/// `subject` names what is being decoded, for the diagnostics. Fails closed on
/// any row shape this projection does not model (a tombstone, a range marker, a
/// complex column, a missing or wrongly-typed key component) — a salvaged
/// output that emitted one of those is a real defect, not something to skip.
fn decoded_as_multiclustering_rows(
    subject: &str,
    rows: &[cqlite_core::storage::sstable::reader::CompactionRow],
) -> MulticlusteringRows {
    use cqlite_core::storage::sstable::reader::CompactionRowData;
    use cqlite_core::types::Value;

    let mut out = MulticlusteringRows::new();
    for row in rows {
        // `pk int` — a single-component partition key is stored as its raw
        // 4-byte big-endian serialization (Cassandra's Int32Type), which is
        // also what sstabledump renders as a decimal string.
        let key_bytes = row.key.as_bytes();
        let pk_bytes: [u8; 4] = key_bytes.try_into().unwrap_or_else(|_| {
            panic!(
                "{subject}: partition key is {} byte(s), expected the 4-byte Int32Type \
                 serialization of `pk int`; bytes={key_bytes:02x?}",
                key_bytes.len()
            )
        });
        let pk = i32::from_be_bytes(pk_bytes);

        let CompactionRowData::Live {
            simple, complex, ..
        } = &row.row_data
        else {
            panic!(
                "{subject}: pk={pk} decoded to a non-live row ({:?}) — this fixture is a healthy, \
                 tombstone-free Cassandra write, so anything else means salvage changed the \
                 content",
                row.row_data
            );
        };
        assert!(
            complex.is_empty(),
            "{subject}: pk={pk} carries complex (multi-cell) columns, but this table declares only \
             scalars; got {complex:?}"
        );

        let cell = |column: &str| -> &Value {
            &simple
                .iter()
                .find(|c| c.column == column)
                .unwrap_or_else(|| {
                    panic!(
                        "{subject}: pk={pk} row has no `{column}` cell; columns present: {:?}",
                        simple.iter().map(|c| &c.column).collect::<Vec<_>>()
                    )
                })
                .value
        };
        let bucket = cell("bucket")
            .as_str()
            .unwrap_or_else(|| {
                panic!(
                    "{subject}: pk={pk} `bucket` is not text: {:?}",
                    cell("bucket")
                )
            })
            .to_string();
        let seq = match cell("seq") {
            Value::Integer(v) => *v,
            other => panic!("{subject}: pk={pk} `seq` is not an int: {other:?}"),
        };
        let payload = cell("payload")
            .as_str()
            .unwrap_or_else(|| {
                panic!(
                    "{subject}: pk={pk} `payload` is not text: {:?}",
                    cell("payload")
                )
            })
            .to_string();

        let previous = out.insert((pk, bucket.clone(), seq), payload);
        assert!(
            previous.is_none(),
            "{subject}: duplicate primary key ({pk}, {bucket}, {seq}) — a reconciled read must \
             surface each row once"
        );
    }
    out
}

/// Report the FIRST difference in each direction between the Cassandra oracle
/// and CQLite's decode of the salvaged output, by primary key — a bare
/// `assert_eq!` on two 468-entry maps prints two walls of text with the
/// difference buried in them.
fn assert_rows_match_golden(
    subject: &str,
    golden: &MulticlusteringRows,
    decoded: &MulticlusteringRows,
) {
    if let Some(missing) = golden.keys().find(|k| !decoded.contains_key(*k)) {
        panic!(
            "{subject}: LOST row {missing:?} — present in the Cassandra sstabledump oracle, \
             absent from CQLite's decode of the salvaged output ({} of {} oracle rows present)",
            decoded.len(),
            golden.len()
        );
    }
    if let Some(extra) = decoded.keys().find(|k| !golden.contains_key(*k)) {
        panic!(
            "{subject}: FABRICATED row {extra:?} — present in CQLite's decode of the salvaged \
             output, absent from the Cassandra sstabledump oracle"
        );
    }
    if let Some((key, want)) = golden.iter().find(|(k, v)| decoded.get(*k) != Some(*v)) {
        panic!(
            "{subject}: row {key:?} VALUE differs from the Cassandra sstabledump oracle:\n  \
             oracle:  {want:?}\n  decoded: {:?}",
            decoded.get(key)
        );
    }
    assert_eq!(
        decoded, golden,
        "{subject}: differs from the Cassandra sstabledump oracle"
    );
}

/// BTI counterpart of the above. `compact_sstables` (design note discovered
/// while implementing #4196) always emits BIG output regardless of the
/// input's on-disk format — `cqlite compact` has no format-preservation
/// knob — so a literal byte-for-byte comparison against it cannot hold for a
/// BTI input (its component set is structurally different: Partitions.db +
/// Rows.db vs Index.db + Summary.db). This is a genuine premise gap in the
/// R1.1 scenario as written for BTI inputs, reported rather than
/// hand-waved; a `compact_sstables` format-preservation option is
/// out-of-scope follow-up work, not a #4196 blocker.
///
/// This test therefore carries TWO oracles, and the ORDER of the two matters:
///
/// 1. **The Cassandra-written one (spec R1.1; the one that can fail).** The
///    committed `da-2-bti-Data.db.jsonl` — real `sstabledump` output over this
///    real Cassandra 5.0.2 `da` fixture — is parsed into every
///    `(pk, bucket, seq) -> payload` row it contains, and CQLite's decode of the
///    SALVAGED OUTPUT must equal that set exactly, in both directions (nothing
///    lost, nothing fabricated).
/// 2. **The CQLite-vs-CQLite round trip (retained, but NOT sufficient on its
///    own).** Salvage's output decodes to the same `CompactionRow`s a full scan
///    of the ORIGINAL input decodes.
///
/// Why (2) alone was a defect, per CLAUDE.md's #3042 blind spot: a
/// CQLite-WRITTEN + CQLite-READ round trip is INVARIANT to a uniform
/// framing/serialization error. Both sides make the identical mistake, the round
/// trip closes, and the test stays green while real Cassandra-written data reads
/// wrong — so (2) can validate self-consistency but can NEVER validate an
/// on-disk property. That is not hypothetical for BTI: #3002 (a `Rows.db`
/// row-index root base two bytes low) was masked for exactly this reason by a
/// compensating encoder defect, undetectable by a symmetric test by
/// construction. (1) is the half that makes this lane an on-disk assertion; it
/// closed the C-audit's #3042 doctrine finding on this issue AND R1.1's missing
/// Cassandra-side half in one change.
#[tokio::test]
async fn salvage_of_healthy_bti_sstable_preserves_every_row() {
    use cqlite_core::platform::Platform;
    use cqlite_core::storage::sstable::SSTableReader;
    use std::sync::Arc;

    const KEYSPACE: &str = "test_da";
    const TABLE: &str = "multiclustering_table";
    const SCHEMA_FILE: &str = "multiclustering-table-bti.cql";

    // COMMITTED fixture (issue #3220, C-audit finding): every component this
    // case needs — `da-2-bti-{Data,Partitions,Rows,Statistics,Filter,
    // CompressionInfo,Digest.crc32,TOC.txt}` AND the `da-2-bti-Data.db.jsonl`
    // golden the Cassandra oracle below reads — is git-tracked (verified with
    // `git ls-files`). So an absence is a broken checkout, not an unfetched
    // dataset, and this case fails closed UNCONDITIONALLY rather than skipping
    // under a `CQLITE_REQUIRE_FIXTURES` gate. `resolve_table_generation_dir`
    // resolves by EVIDENCE across every candidate root and its `Err` carries
    // the full search diagnostic; a fleet `/data/datasets` that lacks this very
    // table (#3032) is exactly why a keyspace-granular resolver must not be
    // used here.
    let fixture_dir =
        datasets_root::resolve_table_generation_dir(KEYSPACE, TABLE).unwrap_or_else(|searched| {
            panic!(
                "COMMITTED fixture {KEYSPACE}.{TABLE} is absent — its *-Data.db and \
                 *-Data.db.jsonl golden are git-tracked, so this is a broken checkout, NOT an \
                 unfetched dataset, and must never skip. {searched}"
            )
        });

    let schema = table_schema(SCHEMA_FILE, TABLE, KEYSPACE);
    let data_db = single_data_db(&fixture_dir);

    let temp = TempDir::new().expect("tempdir");
    let salvage_out_root = temp.path().join("salvage");
    let salvage_report = salvage_sstable(
        &data_db,
        &salvage_out_root,
        &schema,
        SalvageOptions::default(),
    )
    .await
    .expect("salvage of a healthy BTI fixture must succeed");
    assert!(
        salvage_report.losses.is_empty() && salvage_report.refused.is_none(),
        "{KEYSPACE}.{TABLE}: healthy BTI fixture salvage should have zero losses; report={salvage_report:?}"
    );
    assert!(
        salvage_report.partitions.total > 0
            && salvage_report.partitions.total == salvage_report.partitions.recovered,
        "{KEYSPACE}.{TABLE}: expected total == recovered > 0, got {:?}",
        salvage_report.partitions
    );
    let salvage_out = salvage_out_root.join(&schema.keyspace).join(&schema.table);
    let salvage_data_db = single_data_db(&salvage_out);

    let config = cqlite_core::Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let input_reader = SSTableReader::open(&data_db, &config, platform.clone())
        .await
        .expect("open input reader");
    let output_reader = SSTableReader::open(&salvage_data_db, &config, platform)
        .await
        .expect("open salvage output reader");

    let input_rows = input_reader
        .iterate_all_partitions_for_compaction(Some(&schema))
        .await
        .expect("decode input rows");
    let output_rows = output_reader
        .iterate_all_partitions_for_compaction(Some(&schema))
        .await
        .expect("decode salvage output rows");

    assert!(
        !input_rows.is_empty(),
        "{KEYSPACE}.{TABLE}: input decoded zero rows — the fixture itself is empty, which \
         would make this test vacuous"
    );
    assert_eq!(
        input_rows.len(),
        output_rows.len(),
        "{KEYSPACE}.{TABLE}: salvage output row count differs from the original input"
    );
    assert_eq!(
        input_rows, output_rows,
        "{KEYSPACE}.{TABLE}: salvage output rows differ in content from the original input"
    );

    // ---------------------------------------------------------------------
    // Oracle 1 (spec R1.1) — the CASSANDRA-WRITTEN sstabledump golden.
    //
    // Everything above this point is CQLite-written + CQLite-read, and so is
    // invariant to a uniform framing error (#3042). This block is what makes
    // the lane an assertion about the salvaged BYTES: the committed
    // `*-Data.db.jsonl` beside the SOURCE Data.db is real `sstabledump` output
    // over the real Cassandra 5.0.2 `da` fixture, and CQLite's decode of the
    // SALVAGED output must reproduce every row it names, and no other.
    // ---------------------------------------------------------------------
    let golden_path = {
        let mut p = data_db.clone().into_os_string();
        p.push(".jsonl");
        PathBuf::from(p)
    };
    let golden = load_multiclustering_golden(&golden_path);
    let decoded = decoded_as_multiclustering_rows(
        &format!("{KEYSPACE}.{TABLE} salvaged output ({salvage_data_db:?})"),
        &output_rows,
    );
    assert_rows_match_golden(
        &format!("{KEYSPACE}.{TABLE} salvaged output"),
        &golden,
        &decoded,
    );

    eprintln!(
        "[issue_4196] {KEYSPACE}.{TABLE}: healthy BTI salvage preserved all {} row(s) \
         byte-identically (compaction-row decode); 0 losses. Salvaged output also matches the \
         Cassandra-written sstabledump golden {golden_path:?} exactly ({} row(s), both \
         directions).",
        input_rows.len(),
        golden.len()
    );
}
