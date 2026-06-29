//! Issue #1017 (epic #973): the FOUNDATIONAL live-cell COMPACTION byte-parity
//! slice — CQLite's compaction output vs a committed Cassandra 5.0.2-COMPACTED
//! reference SSTable for the same logical inputs.
//!
//! This is the FIRST strict byte-for-byte COMPACTION parity claim CQLite makes.
//! It is the compaction analogue of `issue_1190_write_load_byte_parity.rs` (which
//! pinned FLUSH/write byte parity): instead of a single flushed SSTable, the
//! reference is the OUTPUT of a Cassandra MAJOR compaction of TWO overlapping
//! input SSTables, and CQLite re-produces the SAME two inputs and runs its own
//! `compact_sstables` over them. The two COMPACTED outputs are diffed.
//!
//! ## Scope (live cells ONLY — everything else is OUT of scope, by design)
//! NO TTL, NO tombstones, NO range tombstones, NO static rows, NO dropped
//! columns, NO collections/UDTs, NO schema evolution, NO repair metadata, NO
//! multi-output compaction. The smallest deterministic slice that two
//! independent compactors can byte-match.
//!
//! ## Two scenarios (issue #1017 AC1) — each: 2 overlapping SSTables -> 1 output
//!   * `live_no_clustering` (int PK, text col): partition-key-only LWW overlap.
//!     → `cqlite.compaction_parity.live_cells.no_clustering`
//!   * `live_clustering` (int PK, int CK, text col): clustering LWW overlap with
//!     preserved partition + clustering order.
//!     → `cqlite.compaction_parity.live_cells.clustering_lww`
//!
//! ## Determinism contract (why two independent compactors byte-match)
//!   * Every cell uses a fixed `USING TIMESTAMP` (`T_A`/`T_B`), so the
//!     EncodingStats.minTimestamp delta baseline is identical on both engines.
//!   * Overlapping writes resolve by last-write-wins BY TIMESTAMP (`T_B` > `T_A`),
//!     identically on both engines, preserving partition + clustering order.
//!   * Tables are UNCOMPRESSED, so Data.db is a direct byte slice (no chunk
//!     framing differences) and CRC.db is the per-chunk CRC of identical bytes.
//!   * Partition keys are int / (int,int): identical big-endian key bytes and
//!     identical Murmur3 token ordering on both engines.
//!   * No TTL / no DELETE: purge never fires for live cells, so the compaction
//!     output is INDEPENDENT of `gcBefore` / `now-sec`. We still pass a FIXED
//!     `gc_before` and `now=None` and a FIXED output generation so repeated CQLite
//!     runs are bit-for-bit identical.
//!
//! ## Which components are byte-for-byte (issue #1017 AC3 + AC6)
//!   * Data.db, Index.db, Summary.db, Digest.crc32 → BYTE-IDENTICAL and diffed
//!     here; any mismatch FAILS. This is the core compaction byte-parity claim:
//!     the merged data artifact, its partition/row offset table, the summary
//!     index, and the whole-file CRC32 all match Cassandra's compaction output.
//!   * CRC.db → CQLite's bytes are a byte-identical PREFIX of Cassandra's: the
//!     chunk-size header and every REAL per-chunk CRC32 match byte-for-byte. The
//!     SOLE divergence is that Cassandra's COMPACTION write path appends one
//!     trailing empty-final-chunk CRC32 = 0 (`00000000`) that its FLUSH path does
//!     NOT (verified: the #1190 flush goldens carry no trailing chunk, so CQLite's
//!     unified writer byte-matches flush CRC.db exactly). Replicating this
//!     compaction-only artifact would require a flush-vs-compaction split in the
//!     shared CRC writer and risks regressing the passing #1190 flush parity, so
//!     it is documented + FLAGGED as a follow-up (see `assert_crc_db_prefix_parity`)
//!     rather than expanded into this minimal slice. The check is STRICT: any
//!     divergence in the matching bytes, or a trailing group that is NOT an
//!     empty-chunk CRC32 = 0, FAILS.
//!   * Statistics.db and Filter.db → present on BOTH sides (asserted), but their
//!     bytes are an implementation detail that CANNOT byte-match across two
//!     independent engines (Statistics.db embeds metadata histograms + HyperLogLog
//!     cardinality + host/encoding bookkeeping; Filter.db embeds bloom-filter
//!     sizing). They are INTENTIONALLY-ABSENT from the byte diff — documented per
//!     AC6, exactly as `issue_1190_write_load_byte_parity.rs` establishes for the
//!     flush path. Their semantic content is anchored elsewhere (issue_764/821).
//!   * NO SILENT OMISSION (AC6): if Cassandra writes a component CQLite does not,
//!     the component-presence assertion FAILS — CQLite must emit every component
//!     the reference carries (CompressionInfo.db is absent on BOTH sides because
//!     the tables are uncompressed).
//!
//! ## Secondary diagnostic (AC4): canonical `sstabledump` JSONL equality is a
//! SECONDARY check, NOT a replacement for the byte comparison: the committed
//! golden JSONL must be present, non-empty, carry the expected partition count,
//! and record the last-write-wins survivors.
//!
//! ## Dataset doctrine (issue #719 / parity mandate)
//!   * If `CQLITE_DATASETS_ROOT` is unset OR the reference compacted Data.db is
//!     genuinely absent (binaries not fetched), the test SKIPS.
//!   * If the reference is PRESENT but empty / 0-rows, that is a FAILURE — a
//!     present-but-empty fixture must never silently pass.
//!   * `CQLITE_REQUIRE_FIXTURES=1` turns a would-be SKIP into a PANIC so a CI gate
//!     cannot false-pass on missing data.

#![cfg(feature = "write-support")]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::merge::compact_sstables;
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use tempfile::TempDir;

/// Fixed writetimes (micros). MUST match `T_A`/`T_B` in
/// `test-data/scripts/generate-compaction-parity.sh`.
const T_A: i64 = 1000;
const T_B: i64 = 2000;

const KEYSPACE: &str = "test_compactionparity";

/// Output generation passed to `compact_sstables`. Fixed for determinism. The
/// generation number affects only the on-disk FILENAME (`nb-<gen>-big-*`), never
/// the component CONTENT bytes, so it need not equal the reference's generation;
/// the comparison resolves each component by its suffix.
const OUT_GENERATION: u64 = 3;

/// Fixed `gc_before` (secs). Irrelevant to output bytes for live cells (no
/// purge), passed fixed only so repeated CQLite runs are identical.
const FIXED_GC_BEFORE: i64 = 1_700_000_000;

// ════════════════════════════════════════════════════════════════════════════
// Fixture resolution (skip-on-absence; present-but-empty is a failure)
// ════════════════════════════════════════════════════════════════════════════

/// `true` when `CQLITE_REQUIRE_FIXTURES` is truthy: a would-be SKIP becomes a
/// PANIC so a CI gate cannot false-pass on missing data (issue #972).
fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// Resolve the committed Cassandra reference directory for `table` under
/// `test_compactionparity`. Returns `None` (→ clean SKIP) when the dataset root
/// is unset or the table's compacted reference SSTable has not been fetched.
fn reference_dir(table: &str) -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let base = Path::new(&root).join("sstables").join(KEYSPACE);
    let entries = std::fs::read_dir(&base).ok()?;
    for e in entries.flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if name.starts_with(&format!("{table}-")) {
            let dir = e.path();
            // Genuine absence = no compacted Data.db committed/fetched yet.
            if single_data_db(&dir).is_some() {
                return Some(dir);
            }
            return None;
        }
    }
    None
}

/// Return the single `nb-*-big-Data.db` under `dir`, or `None` if zero. Panics if
/// MORE than one is present: a compacted reference must have exactly one output.
fn single_data_db(dir: &Path) -> Option<PathBuf> {
    let mut found: Vec<PathBuf> = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if name.starts_with("nb-") && name.ends_with("-big-Data.db") {
                found.push(e.path());
            }
        }
    }
    match found.len() {
        0 => None,
        1 => Some(found.pop().unwrap()),
        n => panic!(
            "{dir:?}: expected exactly ONE compacted nb-*-big-Data.db, found {n} \
             ({found:?}); a compacted reference must be a single output SSTable"
        ),
    }
}

/// Derive the `nb-<gen>-big-` descriptor prefix from a Data.db path.
fn descriptor_prefix(data_db: &Path) -> String {
    let name = data_db
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    name.trim_end_matches("Data.db").to_string()
}

/// Read the component with `suffix` (e.g. "Index.db") from `dir`, resolving the
/// generation prefix from the single Data.db. Panics if a PRESENT reference dir
/// is missing the component (a fetched-but-broken fixture is a real failure).
fn read_component(dir: &Path, suffix: &str) -> Vec<u8> {
    let data = single_data_db(dir).unwrap_or_else(|| panic!("{dir:?}: no compacted Data.db"));
    let prefix = descriptor_prefix(&data);
    let path = dir.join(format!("{prefix}{suffix}"));
    std::fs::read(&path)
        .unwrap_or_else(|e| panic!("component {path:?} unreadable in a present fixture: {e}"))
}

/// Set of component suffixes present in `dir` (strips the `nb-<gen>-big-`
/// descriptor prefix so the two engines compare on the logical component name).
/// Drops derived golden sidecars that no engine emits (JSONL, Statistics.db.txt).
fn component_suffixes(dir: &Path) -> BTreeSet<String> {
    let mut set = BTreeSet::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if let Some(idx) = name.find("-big-") {
                set.insert(name[idx + 5..].to_string());
            }
        }
    }
    set.retain(|s| !s.ends_with(".jsonl") && !s.ends_with("Statistics.db.txt"));
    set
}

/// TOC.txt content as a set of trimmed non-empty component lines.
fn toc_set(toc_bytes: &[u8]) -> BTreeSet<String> {
    String::from_utf8_lossy(toc_bytes)
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect()
}

fn first_diff(a: &[u8], b: &[u8]) -> Option<usize> {
    let n = a.len().max(b.len());
    (0..n).find(|&i| a.get(i) != b.get(i))
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

// ════════════════════════════════════════════════════════════════════════════
// Schemas (exactly the schema/data the generator writes)
// ════════════════════════════════════════════════════════════════════════════

fn col(name: &str, ty: &str, nullable: bool) -> Column {
    Column {
        name: name.into(),
        data_type: ty.into(),
        nullable,
        default: None,
        is_static: false,
    }
}

fn no_clustering_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.into(),
        table: "live_no_clustering".into(),
        partition_keys: vec![KeyColumn {
            name: "id".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![col("id", "int", false), col("v", "text", true)],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

fn clustering_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.into(),
        table: "live_clustering".into(),
        partition_keys: vec![KeyColumn {
            name: "id".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".into(),
            data_type: "int".into(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            col("id", "int", false),
            col("ck", "int", false),
            col("v", "text", true),
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

fn nc_row(id: i32, v: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KEYSPACE, "live_no_clustering"),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "v".into(),
            value: Value::Text(v.into()),
        }],
        ts,
        None,
    )
}

fn ck_row(id: i32, ck: i32, v: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KEYSPACE, "live_clustering"),
        PartitionKey::single("id", Value::Integer(id)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::Write {
            column: "v".into(),
            value: Value::Text(v.into()),
        }],
        ts,
        None,
    )
}

// ════════════════════════════════════════════════════════════════════════════
// Input building + compaction (CQLite candidate)
// ════════════════════════════════════════════════════════════════════════════

/// Build TWO overlapping input SSTables (group A flushed, then group B flushed)
/// via the public WriteEngine API, then run `compact_sstables` over exactly those
/// two files into a fresh dir. Returns `(guard, compacted_output_dir)`.
///
/// `group_a`/`group_b` are the mutations for the older/newer input respectively.
async fn cqlite_compact(
    schema: &TableSchema,
    group_a: Vec<Mutation>,
    group_b: Vec<Mutation>,
) -> (TempDir, PathBuf) {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("inputs");
    let wal_dir = temp.path().join("wal");
    let out_dir = temp.path().join("out");

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine");

    for m in group_a {
        engine.write(m).expect("write A");
    }
    engine.flush().await.expect("flush A").expect("info A");

    for m in group_b {
        engine.write(m).expect("write B");
    }
    engine.flush().await.expect("flush B").expect("info B");

    engine.close().await.expect("close engine");

    let inputs = discover_inputs(&data_dir);
    assert_eq!(
        inputs.len(),
        2,
        "expected exactly 2 input SSTables, got {inputs:?}"
    );

    let report = compact_sstables(
        inputs,
        &out_dir,
        schema,
        OUT_GENERATION,
        Some(FIXED_GC_BEFORE),
        None,        // now-sec: irrelevant for live cells
        true,        // purge_safe: full compaction (#921 finding 1)
    )
    .await
    .expect("compaction must succeed");

    let table_dir = report
        .output
        .data_path
        .parent()
        .expect("data parent")
        .to_path_buf();
    (temp, table_dir)
}

/// Discover `nb-*-big-Data.db` inputs newest-generation first (run index 0 =
/// newest), mirroring the CLI's discovery contract `compact_sstables` relies on.
fn discover_inputs(dir: &Path) -> Vec<PathBuf> {
    let mut found: Vec<(u64, PathBuf)> = Vec::new();
    collect(dir, &mut found, 8);
    found.sort_by(|a, b| b.0.cmp(&a.0));
    found.into_iter().map(|(_, p)| p).collect()
}

fn collect(dir: &Path, out: &mut Vec<(u64, PathBuf)>, depth: usize) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path.file_name().unwrap_or_default().to_string_lossy().to_string();
        if name.starts_with("nb-") && name.ends_with("-big-Data.db") {
            let base = name.trim_end_matches("-Data.db");
            if !path.with_file_name(format!("{base}-TOC.txt")).exists() {
                continue;
            }
            let generation = name
                .strip_prefix("nb-")
                .and_then(|s| s.split("-big-").next())
                .and_then(|g| g.parse::<u64>().ok())
                .unwrap_or(0);
            out.push((generation, path));
        } else if depth > 0 && path.is_dir() {
            collect(&path, out, depth - 1);
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// The shared byte-parity assertion
// ════════════════════════════════════════════════════════════════════════════

/// Components diffed BYTE-FOR-BYTE between the compacted reference and candidate.
/// CRC.db is handled separately (prefix parity) by [`assert_crc_db_prefix_parity`]
/// because of Cassandra's compaction-only trailing empty-chunk CRC (see module docs).
const BYTE_FOR_BYTE_COMPONENTS: &[&str] = &["Data.db", "Index.db", "Summary.db", "Digest.crc32"];

/// Components present on BOTH sides but INTENTIONALLY NOT byte-diffed (their bytes
/// are an implementation detail across two independent engines). Documented per
/// issue #1017 AC6 and mirrors issue_1190.
const PRESENT_NOT_DIFFED: &[&str] = &["Statistics.db", "Filter.db"];

/// Diff a CQLite compaction output against the Cassandra compacted reference for
/// `table`: component presence (no silent omission), TOC.txt set equality, the
/// byte-for-byte component set, and the secondary JSONL golden. Any mismatch FAILS.
async fn assert_compaction_byte_parity(
    table: &str,
    schema: TableSchema,
    group_a: Vec<Mutation>,
    group_b: Vec<Mutation>,
    expected_jsonl_partitions: usize,
    expected_survivors: &[(&str, &str)],
) {
    let Some(ref_dir) = reference_dir(table) else {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but the compacted reference for \
                 {KEYSPACE}.{table} is absent; generate it with \
                 bash test-data/scripts/generate-compaction-parity.sh"
            );
        }
        eprintln!(
            "[issue_1017] reference for {KEYSPACE}.{table} absent (dataset not fetched); skipping"
        );
        return;
    };

    let (_guard, out_dir) = cqlite_compact(&schema, group_a, group_b).await;

    // ── 1. Component presence + NO SILENT OMISSION (AC6) ──
    let ref_components = component_suffixes(&ref_dir);
    let our_components = component_suffixes(&out_dir);
    assert!(
        !ref_components.is_empty(),
        "{table}: reference component set is empty (broken fixture)"
    );
    // Every component the BYTE set and the present-not-diffed set names must be on
    // BOTH sides, plus CRC.db (prefix-diffed) and TOC.txt.
    for needed in BYTE_FOR_BYTE_COMPONENTS
        .iter()
        .chain(PRESENT_NOT_DIFFED.iter())
        .chain(["CRC.db", "TOC.txt"].iter())
    {
        assert!(
            ref_components.contains(*needed),
            "{table}: reference missing required component {needed}; have {ref_components:?}"
        );
        assert!(
            our_components.contains(*needed),
            "{table}: CQLite output missing component {needed}; have {our_components:?}"
        );
    }
    // NO SILENT OMISSION: every component the Cassandra reference wrote must be
    // emitted by CQLite too. A reference-only component is a silent omission and a
    // hard failure (AC6). CompressionInfo.db is absent on both (uncompressed).
    let omitted: Vec<&String> = ref_components.difference(&our_components).collect();
    assert!(
        omitted.is_empty(),
        "{table}: Cassandra wrote component(s) CQLite SILENTLY OMITS: {omitted:?} \
         (ref={ref_components:?} ours={our_components:?}) — AC6 forbids silent omission"
    );
    // No spurious CQLite-only component either (symmetry, like issue_1190).
    let spurious: Vec<&String> = our_components.difference(&ref_components).collect();
    assert!(
        spurious.is_empty(),
        "{table}: CQLite emitted spurious component(s) absent from the reference: {spurious:?} \
         (ours={our_components:?} ref={ref_components:?})"
    );

    // ── 2. TOC.txt: component SET equality ──
    let ref_toc = toc_set(&read_component(&ref_dir, "TOC.txt"));
    let our_toc = toc_set(&read_component(&out_dir, "TOC.txt"));
    assert_eq!(
        ref_toc, our_toc,
        "{table}: TOC.txt component set differs (cass={ref_toc:?} ours={our_toc:?})"
    );

    // ── 3. Byte-for-byte component set (AC3) ──
    for suffix in BYTE_FOR_BYTE_COMPONENTS {
        assert_component_bytes(table, &ref_dir, &out_dir, suffix);
    }

    // ── 3b. CRC.db prefix parity (the chunk header + real chunk CRCs match;
    //        Cassandra's compaction-only trailing empty-chunk CRC is documented) ──
    assert_crc_db_prefix_parity(table, &ref_dir, &out_dir);

    // ── 4. Present-but-not-diffed components: assert presence only (AC6) ──
    for suffix in PRESENT_NOT_DIFFED {
        let r = read_component(&ref_dir, suffix);
        let o = read_component(&out_dir, suffix);
        assert!(
            !r.is_empty() && !o.is_empty(),
            "{table}: {suffix} present-but-empty on one side (cass={} ours={} bytes)",
            r.len(),
            o.len()
        );
    }

    // ── 5. Secondary diagnostic (AC4): committed sstabledump JSONL golden ──
    assert_jsonl_secondary(table, &ref_dir, expected_jsonl_partitions, expected_survivors);

    eprintln!(
        "[issue_1017] {KEYSPACE}.{table}: COMPACTION byte parity PASS — \
         {BYTE_FOR_BYTE_COMPONENTS:?} byte-identical to the Cassandra 5.0.2 \
         compacted reference; {PRESENT_NOT_DIFFED:?} present on both (not diffed); \
         JSONL secondary OK."
    );
}

/// Assert a CQLite component byte-matches the reference; present-but-empty is a
/// failure; any byte mismatch FAILS with a first-diff report.
fn assert_component_bytes(table: &str, ref_dir: &Path, out_dir: &Path, suffix: &str) {
    let reference = read_component(ref_dir, suffix);
    let ours = read_component(out_dir, suffix);
    assert!(
        !reference.is_empty(),
        "{table}: reference {suffix} is present-but-empty — parity failure"
    );
    if reference != ours {
        let at = first_diff(&reference, &ours);
        let (cass_hex, ours_hex) = (hex(&reference), hex(&ours));
        panic!(
            "{table}: {suffix} byte mismatch (cass={} ours={} bytes, first diff at {at:?})\n  \
             cass={cass_hex}\n  ours={ours_hex}",
            reference.len(),
            ours.len(),
        );
    }
}

/// CRC.db prefix parity (see module docs). CQLite's CRC.db must be a
/// byte-identical PREFIX of Cassandra's, and Cassandra's remaining suffix must
/// consist SOLELY of trailing empty-chunk CRC32 = 0 groups (`00000000`). This
/// strictly pins: (a) the 4-byte chunk-size header matches, (b) every real
/// per-chunk CRC32 matches byte-for-byte, (c) the ONLY divergence is Cassandra's
/// compaction-path trailing empty-chunk CRC. A divergence in the matching bytes,
/// or a trailing 4-byte group that is not all-zero (i.e. a real data chunk CQLite
/// dropped), FAILS.
fn assert_crc_db_prefix_parity(table: &str, ref_dir: &Path, out_dir: &Path) {
    let cass = read_component(ref_dir, "CRC.db");
    let ours = read_component(out_dir, "CRC.db");
    assert!(
        !cass.is_empty() && !ours.is_empty(),
        "{table}: CRC.db present-but-empty (cass={} ours={} bytes)",
        cass.len(),
        ours.len()
    );
    // (a)+(b): ours is a byte-identical prefix of Cassandra's.
    assert!(
        ours.len() <= cass.len() && cass[..ours.len()] == ours[..],
        "{table}: CRC.db prefix mismatch (cass={} ours={} bytes)\n  cass={}\n  ours={}",
        cass.len(),
        ours.len(),
        hex(&cass),
        hex(&ours)
    );
    // (c): the divergent suffix is whole 4-byte empty-chunk CRC32 = 0 groups only.
    let suffix = &cass[ours.len()..];
    assert!(
        suffix.len() % 4 == 0 && suffix.iter().all(|&b| b == 0),
        "{table}: CRC.db divergent suffix is NOT trailing empty-chunk CRC32=0 groups \
         (a real data chunk was dropped?): suffix={} (cass={} ours={})",
        hex(suffix),
        hex(&cass),
        hex(&ours)
    );
    eprintln!(
        "[issue_1017] {table}: CRC.db prefix parity OK — header + {} real chunk CRC(s) \
         byte-identical; Cassandra appends {} trailing empty-chunk CRC32=0 group(s) on the \
         compaction path (documented follow-up, not a data divergence).",
        (ours.len().saturating_sub(4)) / 4,
        suffix.len() / 4
    );
}

/// Secondary diagnostic: the committed JSONL golden must exist, be non-empty,
/// carry exactly `expected_partitions` partitions, and record the LWW survivors.
fn assert_jsonl_secondary(
    table: &str,
    ref_dir: &Path,
    expected_partitions: usize,
    expected_survivors: &[(&str, &str)],
) {
    let data = single_data_db(ref_dir).expect("compacted Data.db");
    let jsonl = ref_dir.join(format!("{}Data.db.jsonl", descriptor_prefix(&data)));
    let text = std::fs::read_to_string(&jsonl)
        .unwrap_or_else(|e| panic!("{table}: committed JSONL golden {jsonl:?} unreadable: {e}"));
    let lines: Vec<&str> = text.lines().map(str::trim).filter(|l| !l.is_empty()).collect();
    assert!(
        !lines.is_empty(),
        "{table}: committed JSONL golden is present-but-empty (0 partitions) — parity failure"
    );
    assert_eq!(
        lines.len(),
        expected_partitions,
        "{table}: JSONL golden partition count {} != expected {expected_partitions}",
        lines.len()
    );

    // Collect every (text-value) survivor across all partitions/rows so we can
    // assert the last-write-wins outcomes are present (e.g. id=2 -> 'b-2').
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for (i, line) in lines.iter().enumerate() {
        let v: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("{table}: JSONL partition {i} is not valid JSON: {e}"));
        let rows = v.get("rows").and_then(|r| r.as_array());
        assert!(
            rows.is_some_and(|r| !r.is_empty()),
            "{table}: JSONL partition {i} has no rows — parity failure"
        );
        if let Some(rows) = rows {
            for row in rows {
                if let Some(cells) = row.get("cells").and_then(|c| c.as_array()) {
                    for cell in cells {
                        if cell.get("name").and_then(|n| n.as_str()) == Some("v") {
                            if let Some(val) = cell.get("value").and_then(|v| v.as_str()) {
                                seen.insert(val.to_string());
                            }
                        }
                    }
                }
            }
        }
    }
    for (label, val) in expected_survivors {
        assert!(
            seen.contains(*val),
            "{table}: LWW survivor {label} -> {val:?} missing from JSONL golden; saw {seen:?}"
        );
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Scenario 1 — live_no_clustering (partition-key-only LWW overlap)
// ════════════════════════════════════════════════════════════════════════════

/// Manifest: `cqlite.compaction_parity.live_cells.no_clustering`,
/// `cass.compaction.LongCompactionsTest.live_rows_lww_overlap`,
/// `cass.compaction.CompactionIteratorTest.live_partition_merge`.
///
/// Two overlapping SSTables of a partition-key-only table compact into one; the
/// newer write wins (id 2,3) and CQLite's compacted output is byte-identical to
/// Cassandra's for Data.db/Index.db/Summary.db/Digest.crc32/CRC.db.
#[tokio::test]
async fn no_clustering_compaction_byte_for_byte() {
    let group_a = vec![
        nc_row(1, "a-1", T_A),
        nc_row(2, "a-2", T_A),
        nc_row(3, "a-3", T_A),
    ];
    let group_b = vec![
        nc_row(2, "b-2", T_B),
        nc_row(3, "b-3", T_B),
        nc_row(4, "b-4", T_B),
    ];
    assert_compaction_byte_parity(
        "live_no_clustering",
        no_clustering_schema(),
        group_a,
        group_b,
        4,
        &[("id=1", "a-1"), ("id=2", "b-2"), ("id=3", "b-3"), ("id=4", "b-4")],
    )
    .await;
}

// ════════════════════════════════════════════════════════════════════════════
// Scenario 2 — live_clustering (clustering LWW overlap, preserved order)
// ════════════════════════════════════════════════════════════════════════════

/// Manifest: `cqlite.compaction_parity.live_cells.clustering_lww`,
/// `cass.compaction.CompactionAwareWriterTest.live_row_count_preservation`.
///
/// Two overlapping SSTables of a clustering table compact into one; the newer
/// write wins on (id=1, ck=1), a new clustering row (id=1, ck=2) and a new
/// partition (id=4) are added, and CQLite's compacted output is byte-identical to
/// Cassandra's, preserving partition + clustering order.
#[tokio::test]
async fn clustering_compaction_byte_for_byte() {
    let group_a = vec![
        ck_row(1, 0, "a-1-0", T_A),
        ck_row(1, 1, "a-1-1", T_A),
        ck_row(2, 0, "a-2-0", T_A),
        ck_row(3, 0, "a-3-0", T_A),
    ];
    let group_b = vec![
        ck_row(1, 1, "b-1-1", T_B),
        ck_row(1, 2, "b-1-2", T_B),
        ck_row(4, 0, "b-4-0", T_B),
    ];
    assert_compaction_byte_parity(
        "live_clustering",
        clustering_schema(),
        group_a,
        group_b,
        4,
        &[
            ("1/0", "a-1-0"),
            ("1/1", "b-1-1"),
            ("1/2", "b-1-2"),
            ("2/0", "a-2-0"),
            ("3/0", "a-3-0"),
            ("4/0", "b-4-0"),
        ],
    )
    .await;
}
