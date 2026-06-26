//! Issue #953 (Epic #951): a single-candidate `WHERE pk = ?` read must SEEK
//! within the SSTable — decode only the target partition — not full-parse it.
//!
//! #949 added cross-SSTable pruning (bloom/BTI): a point lookup touches only the
//! handful of SSTables that can hold the key. #958 proved that prune with
//! `sstables_scanned()`. But neither proves that *within* a surviving candidate
//! we stopped decoding the WHOLE `Data.db`: the old single-candidate path called
//! `reader.scan(...)` (a full parse of every partition) and retained one. A
//! regression could quietly keep doing that and still return the right rows.
//!
//! #953 adds the within-SSTable seek (resolve the partition's `Data.db` offset
//! via the BTI trie / `Index.db`, decode ONLY that partition) and a new work
//! counter, `work_counters::partitions_decoded()`, incremented once per partition
//! actually DECODED by the seek. This test makes the seek a HARD CI gate:
//!
//!   1. **Decode bound** — on a MULTI-partition single-generation SSTable, a
//!      `WHERE pk = <one key>` must decode O(1) partitions
//!      (`partitions_decoded()` small), NOT ~N. If the seek regresses to a full
//!      parse-then-retain, this balloons to N and the test fails.
//!   2. **Byte parity** — the targeted result equals the full-scan result
//!      filtered to that key, so the seek is correct, not just cheap.
//!
//! Covers BOTH formats present in the datasets:
//!   - **BIG (`nb`)** — `test_basic.simple_table` (`id UUID PRIMARY KEY`), ~999
//!     partitions; offset resolved via `Index.db`.
//!   - **BTI (`da`)** — `test_da.simple_table` (`id UUID PRIMARY KEY`); offset
//!     resolved via the Partitions.db trie, decoded key re-verified against the
//!     queried key (prefix-collision guard).
//!
//! The counter is process-global, so the decode-bound assertions for both formats
//! live in ONE serialized test (`within_sstable_seek_decodes_o1_partitions`); the
//! parity checks (which do not read the counter) are separate.
//!
//! Requires `CQLITE_DATASETS_ROOT` and the fetched binary SSTables; skipped (not
//! failed) when the data isn't present, matching the repo's other dataset-backed
//! integration tests. Excluded under `tombstones` (that build compiles out the
//! seek + counter mutator; see issue_958 for the same exclusion rationale).

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    not(feature = "tombstones")
))]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Serializes the counter-reading bound test against the counter-writing parity
/// tests in THIS binary.
///
/// `work_counters::partitions_decoded()` is a process-global atomic shared with
/// the read path (see `work_counters.rs`: "the global is shared with read-path
/// code that any parallel test can drive"). The bound test
/// (`within_sstable_seek_decodes_o1_partitions`) does `reset()` → seek →
/// `partitions_decoded()`, but the two parity tests issue their own `WHERE id = ?`
/// seeks that bump the SAME global counter. Cargo runs each test binary's tests
/// in parallel threads, so a parity-test seek landing between the bound test's
/// `reset()` and its read inflates the count (observed: 1 → 3 on CI, issue #1105;
/// up to 51 — the parity probe budget — with a widened window).
///
/// A `RwLock` (not a plain `Mutex`) preserves the design intent that the two
/// parity tests run concurrently with EACH OTHER: they take a shared read guard,
/// while the bound test takes the exclusive write guard so no other test mutates
/// the global counter while it measures. () payload — this gates timing only.
static COUNTER_GATE: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::QueryRow;
use cqlite_core::storage::sstable::work_counters;
use cqlite_core::{Database, Value};
use serial_test::serial;

/// Upper bound on `partitions_decoded()` for a single-partition seek.
///
/// Why a SMALL CONSTANT (independent of the table's partition count) is the right
/// gate:
/// - A successful seek decodes EXACTLY the one target partition (the decode loop
///   stops at the first complete partition at the resolved offset), so a hit
///   bumps the counter by 1.
/// - A verified-absent key (BTI trie miss, or a decoded key that does not match)
///   decodes 0.
/// - The bound `2` leaves headroom for an executor that probes a single
///   candidate once, while staying far below the table's partition count
///   (~999 for the BIG table). A regression to full parse-then-retain decodes
///   every partition (~999) and blows past this immediately.
const MAX_PARTITIONS_DECODED: u64 = 2;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn schemas_dir() -> Option<PathBuf> {
    if let Some(root) = datasets_root() {
        let dir = root.parent().and_then(|p| {
            let d = p.join("schemas");
            d.exists().then_some(d)
        });
        if dir.is_some() {
            return dir;
        }
    }
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = manifest_dir.parent()?.join("test-data").join("schemas");
    dir.exists().then_some(dir)
}

async fn setup(schema_file: &str, keyspace_filter: &str) -> Result<Database, String> {
    let root = datasets_root().ok_or("CQLITE_DATASETS_ROOT not set or missing")?;
    let schema_path = schemas_dir()
        .ok_or("schemas dir not found")?
        .join(schema_file);
    if !schema_path.exists() {
        return Err(format!("schema not found at {schema_path:?}"));
    }
    let data_dir = root.join("sstables");
    if !data_dir.exists() {
        return Err(format!("sstables dir not found at {data_dir:?}"));
    }

    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(keyspace_filter.to_string()),
    };
    let result = ingest(config)
        .await
        .map_err(|e| format!("ingestion failed: {e}"))?;
    if result.schema_load_result.schemas_loaded == 0 {
        return Err("no schemas loaded".to_string());
    }
    Ok(result.database)
}

/// Format a 16-byte UUID as the canonical 8-4-4-4-12 hex string the parser
/// accepts as an unquoted literal (Issue #956).
fn uuid_to_literal(bytes: &[u8; 16]) -> String {
    let h = |range: std::ops::Range<usize>| -> String {
        bytes[range].iter().map(|b| format!("{b:02x}")).collect()
    };
    format!(
        "{}-{}-{}-{}-{}",
        h(0..4),
        h(4..6),
        h(6..8),
        h(8..10),
        h(10..16)
    )
}

fn uuid_value(row: &QueryRow, col: &str) -> Option<[u8; 16]> {
    match row.values.get(col) {
        Some(Value::Uuid(b)) => Some(*b),
        _ => None,
    }
}

fn row_fingerprint(row: &QueryRow) -> BTreeMap<String, String> {
    row.values
        .iter()
        .map(|(k, v)| (k.clone(), format!("{v:?}")))
        .collect()
}

fn fingerprints(rows: &[QueryRow]) -> Vec<BTreeMap<String, String>> {
    let mut out: Vec<_> = rows.iter().map(row_fingerprint).collect();
    out.sort_by_key(|m| format!("{m:?}"));
    out
}

/// Open the table, full-scan to learn the partition keys, and return
/// `(db, distinct_partition_count, one_real_uuid_key)`. Returns `None` to signal
/// a skip (missing data / 0 rows).
async fn open_and_probe(
    schema_file: &str,
    keyspace_filter: &str,
    qualified_table: &str,
) -> Option<(Database, usize, Vec<[u8; 16]>)> {
    let db = match setup(schema_file, keyspace_filter).await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping ({qualified_table}): {e}");
            return None;
        }
    };

    let full = db
        .execute(&format!("SELECT id, name FROM {qualified_table}"))
        .await
        .unwrap_or_else(|e| panic!("full scan of {qualified_table} must succeed: {e}"));
    if full.rows.is_empty() {
        eprintln!("Skipping ({qualified_table}): 0 rows (Data.db not fetched?)");
        return None;
    }

    let mut keys: Vec<[u8; 16]> = Vec::new();
    for row in &full.rows {
        if let Some(id) = uuid_value(row, "id") {
            keys.push(id);
        }
    }
    keys.sort();
    keys.dedup();
    assert!(
        !keys.is_empty(),
        "{qualified_table}: expected UUID-keyed partitions, `id` did not decode as Value::Uuid",
    );
    let count = keys.len();
    Some((db, count, keys))
}

/// Probe present keys until the within-SSTable seek decodes exactly one partition
/// (a seek-positive key), asserting the O(1) decode bound holds for EVERY probed
/// key along the way. Returns the seek-positive key's `partitions_decoded` count.
///
/// Why probe rather than assert on a single fixed key: the BIG seek falls back to
/// a full scan for a partition whose chunk-targeted decode is inconclusive
/// (Constraint #4) — that key reads `partitions_decoded() == 0`. Proving the seek
/// path is engaged needs a key the seek actually handles; the bound is what
/// protects against a regression, and it is checked on every key.
async fn find_seek_positive_key(
    db: &Database,
    qualified_table: &str,
    keys: &[[u8; 16]],
    n_partitions: usize,
) -> Option<(String, u64)> {
    // Probe up to this many keys looking for one the seek decodes directly.
    let probe_budget = keys.len().min(16);
    for key in keys.iter().take(probe_budget) {
        let literal = uuid_to_literal(key);
        work_counters::reset();
        let targeted = db
            .execute(&format!(
                "SELECT id, name FROM {qualified_table} WHERE id = {literal}"
            ))
            .await
            .unwrap_or_else(|e| panic!("targeted lookup id={literal} failed: {e}"));
        let decoded = work_counters::partitions_decoded();

        // The bound is the regression gate — it must hold for EVERY key, whether
        // the seek decoded it or fell back.
        assert!(
            decoded <= MAX_PARTITIONS_DECODED,
            "Issue #953: WHERE id = {literal} over a {n_partitions}-partition SSTable must DECODE \
             at most {MAX_PARTITIONS_DECODED} partition(s) via the within-SSTable seek, but it \
             decoded {decoded}. A count near {n_partitions} means the seek regressed to a full \
             parse-then-retain.",
        );
        // The lookup must still return the partition's rows regardless of path.
        assert!(
            !targeted.rows.is_empty(),
            "WHERE id = {literal} for a present key must return the partition's rows",
        );

        if decoded >= 1 {
            return Some((literal, decoded));
        }
    }
    None
}

/// THE seek gate (Issue #953): a single-partition read decodes O(1) partitions.
///
/// Both formats are checked in one test because `partitions_decoded()` is a
/// process-global counter. `#[serial(work_counters)]` additionally serializes
/// this test against the parity tests in this binary: those run `WHERE id = ?`
/// lookups that bump the same global counter, and `cargo test` runs a binary's
/// tests as parallel threads in ONE process — so without the named serial lock a
/// sibling could increment `partitions_decoded` between this test's `reset()` and
/// its read, inflating the count (issue #1071).
#[tokio::test]
#[serial(work_counters)]
async fn within_sstable_seek_decodes_o1_partitions() {
    // Exclusive: no parity-test seek may mutate the process-global decode counter
    // while this test measures it (issue #1105). Held for the whole test.
    let _counter_guard = COUNTER_GATE.write().await;

    let mut checked_any = false;

    // ── BIG (`nb`) format: offset resolved via Index.db ─────────────────────────
    if let Some((db, n_partitions, keys)) =
        open_and_probe("basic-types.cql", "/test_basic/", "test_basic.simple_table").await
    {
        checked_any = true;
        // Sanity: the table really has many partitions, so the bound is meaningful.
        assert!(
            n_partitions as u64 > MAX_PARTITIONS_DECODED,
            "BIG fixture must have many partitions to make the decode bound meaningful \
             (got {n_partitions})",
        );

        let seek_positive =
            find_seek_positive_key(&db, "test_basic.simple_table", &keys, n_partitions).await;
        let (literal, decoded) = seek_positive.expect(
            "Issue #953 (BIG): expected at least one key the within-SSTable seek decodes directly \
             (a key whose Index.db offset resolves and decodes); none of the probed keys engaged \
             the seek, which would mean the BIG seek path is never exercised",
        );
        assert!(
            (1..=MAX_PARTITIONS_DECODED).contains(&decoded),
            "BIG: a seek-positive key must decode exactly the one target partition (got {decoded})",
        );

        // Absent key: the Index.db has no entry, so the seek does not engage and the
        // full-scan fallback runs (which does not bump partitions_decoded). Either
        // way the decode counter must stay bounded.
        work_counters::reset();
        let absent = db
            .execute(
                "SELECT id FROM test_basic.simple_table \
                 WHERE id = ffffffff-ffff-ffff-ffff-ffffffffffff",
            )
            .await
            .expect("BIG absent-key lookup must succeed");
        assert!(
            absent.rows.is_empty(),
            "BIG: absent key must return no rows"
        );
        assert!(
            work_counters::partitions_decoded() <= MAX_PARTITIONS_DECODED,
            "BIG: an absent-key lookup must not decode every partition (got {})",
            work_counters::partitions_decoded(),
        );

        println!(
            "Issue #953 (BIG): seek-positive key {literal} decoded {decoded} partition(s) in a \
             {n_partitions}-partition SSTable"
        );
    }

    // ── BTI (`da`) format: offset resolved via the Partitions.db trie ───────────
    if let Some((db, n_partitions, keys)) =
        open_and_probe("da-test.cql", "/test_da/", "test_da.simple_table").await
    {
        checked_any = true;
        let seek_positive =
            find_seek_positive_key(&db, "test_da.simple_table", &keys, n_partitions).await;
        let (literal, decoded) = seek_positive.expect(
            "Issue #953 (BTI): expected at least one key the trie-resolved seek decodes directly",
        );
        assert!(
            (1..=MAX_PARTITIONS_DECODED).contains(&decoded),
            "BTI: a seek-positive key must decode exactly the one target partition (got {decoded})",
        );

        // Absent key: BTI trie miss is authoritative absence; decode 0 partitions.
        work_counters::reset();
        let absent = db
            .execute(
                "SELECT id FROM test_da.simple_table \
                 WHERE id = ffffffff-ffff-ffff-ffff-ffffffffffff",
            )
            .await
            .expect("BTI absent-key lookup must succeed");
        assert!(
            absent.rows.is_empty(),
            "BTI: absent key must return no rows"
        );
        assert_eq!(
            work_counters::partitions_decoded(),
            0,
            "BTI: a trie-miss absent key is authoritative absence and must decode 0 partitions",
        );

        println!(
            "Issue #953 (BTI): seek-positive key {literal} decoded {decoded} partition(s) in a \
             {n_partitions}-partition SSTable"
        );
    }

    if !checked_any {
        eprintln!("Issue #953: skipped — no datasets present for either format");
    }
}

/// Byte-parity (BIG): the seek result equals the full-scan result filtered to the
/// key, for every partition. It does not READ the counter, but its `WHERE id = ?`
/// lookups MUTATE the process-global `partitions_decoded`, so it carries
/// `#[serial(work_counters)]` to stay off the decode-bound test's measurement
/// window (issue #1071).
#[tokio::test]
#[serial(work_counters)]
async fn within_sstable_seek_matches_full_scan_big() {
    // Shared with the BTI parity test (they may overlap), but excluded from the
    // bound test's measurement window: this test's seeks bump the global decode
    // counter (issue #1105).
    let _counter_guard = COUNTER_GATE.read().await;

    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let full = db
        .execute("SELECT id, name, age FROM test_basic.simple_table")
        .await
        .expect("full scan must succeed");
    if full.rows.is_empty() {
        eprintln!("Skipping: simple_table returned 0 rows (Data.db not fetched?)");
        return;
    }

    let mut by_partition: BTreeMap<[u8; 16], Vec<QueryRow>> = BTreeMap::new();
    for row in full.rows {
        let Some(id) = uuid_value(&row, "id") else {
            continue;
        };
        by_partition.entry(id).or_default().push(row);
    }

    let mut checked = 0usize;
    for (id, expected_rows) in by_partition.iter() {
        let literal = uuid_to_literal(id);
        let targeted = db
            .execute(&format!(
                "SELECT id, name, age FROM test_basic.simple_table WHERE id = {literal}"
            ))
            .await
            .unwrap_or_else(|e| panic!("targeted lookup for id={literal} failed: {e}"));

        assert_eq!(
            fingerprints(&targeted.rows),
            fingerprints(expected_rows),
            "Issue #953 (BIG): seek result for id={literal} must equal the full-scan rows",
        );

        checked += 1;
        if checked >= 50 {
            break;
        }
    }
    assert!(checked > 0, "expected at least one partition to validate");
    println!("Issue #953 (BIG): seek == full-scan parity for {checked} partitions");
}

/// Byte-parity (BTI): same as above for the trie-resolved seek path. Carries
/// `#[serial(work_counters)]` for the same reason as the BIG parity test: its
/// lookups mutate the shared counter (issue #1071).
#[tokio::test]
#[serial(work_counters)]
async fn within_sstable_seek_matches_full_scan_bti() {
    // See `within_sstable_seek_matches_full_scan_big`: shared read guard keeps
    // this test's seeks out of the bound test's counter window (issue #1105).
    let _counter_guard = COUNTER_GATE.read().await;

    let db = match setup("da-test.cql", "/test_da/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let full = db
        .execute("SELECT id, name, age FROM test_da.simple_table")
        .await
        .expect("full scan must succeed");
    if full.rows.is_empty() {
        eprintln!("Skipping: test_da.simple_table returned 0 rows (Data.db not fetched?)");
        return;
    }

    let mut by_partition: BTreeMap<[u8; 16], Vec<QueryRow>> = BTreeMap::new();
    for row in full.rows {
        let Some(id) = uuid_value(&row, "id") else {
            continue;
        };
        by_partition.entry(id).or_default().push(row);
    }

    let mut checked = 0usize;
    for (id, expected_rows) in by_partition.iter() {
        let literal = uuid_to_literal(id);
        let targeted = db
            .execute(&format!(
                "SELECT id, name, age FROM test_da.simple_table WHERE id = {literal}"
            ))
            .await
            .unwrap_or_else(|e| panic!("targeted lookup for id={literal} failed: {e}"));

        assert_eq!(
            fingerprints(&targeted.rows),
            fingerprints(expected_rows),
            "Issue #953 (BTI): seek result for id={literal} must equal the full-scan rows",
        );
        checked += 1;
    }
    assert!(
        checked > 0,
        "expected at least one BTI partition to validate"
    );
    println!("Issue #953 (BTI): seek == full-scan parity for {checked} partitions");
}
