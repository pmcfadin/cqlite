//! Issue #1570 (Epic B / B4): the key→partition-offset cache lets a repeated hot
//! point read skip the index/trie descent entirely (the Cassandra key-cache
//! analogue).
//!
//! Wiring evidence, both formats:
//!
//! - **BTI** (`test_da/simple_table`): read present key A, then present key B, then
//!   A again with `TRIE_WALKS` reset. A's second read hits the B4 LRU and descends
//!   the `Partitions.db` trie ZERO times. The interleave with B is deliberate — it
//!   proves the *multi-key* B4 cache served the hit, not the single-entry C3 memo
//!   (which after reading B holds B, so it could not serve A).
//! - **BIG** (`test_basic/simple_table`): call `lookup_partition_with_index` twice
//!   for the same present key at the reader level. The second call hits the B4 cache
//!   and probes `Index.db` ZERO times (`INDEX_PROBES == 0`), returning the SAME
//!   `(offset,size)` the first probe resolved (parity on a hit).
//!
//! Plus the negative (absent-key) counterpart for both formats: a key the SSTable
//! does NOT contain resolves to authoritative absence on every read and is NEVER
//! cached (positive-only insert discipline). BIG proves this via `INDEX_PROBES >= 1`
//! on the re-read (the absent key was never positively cached). BTI proves it
//! directly on the B4 cache: a repeated absent read registers NO B4 cache HIT — the
//! deterministic property, immune to the reader-local C3 single-walk memo that made
//! the old `TRIE_WALKS >= 1` proxy flaky (issue #2674).
//!
//! Compiled only with `--features work-counters` (the counter getters/`reset` live
//! behind it). Requires `CQLITE_DATASETS_ROOT`; each test self-skips (never fails)
//! when its optional fixture is absent. The BTI test is excluded under `tombstones`
//! (that build serves point reads by a full-scan filter, not the prune+seek path).

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    feature = "work-counters"
))]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::storage::sstable::read_work_counters as rwc;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

/// Locate the first `*-Data.db` under `<datasets>/sstables/<keyspace>/<table>-*/`.
fn find_data_db(keyspace: &str, table: &str) -> Option<PathBuf> {
    let root = datasets_root()?;
    let entries = std::fs::read_dir(root.join("sstables").join(keyspace)).ok()?;
    let prefix = format!("{table}-");
    for e in entries.flatten() {
        if !e.file_name().to_string_lossy().starts_with(&prefix) {
            continue;
        }
        if let Ok(files) = std::fs::read_dir(e.path()) {
            for f in files.flatten() {
                if f.file_name().to_string_lossy().ends_with("-Data.db") {
                    return Some(f.path());
                }
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// BIG wiring: INDEX_PROBES == 0 on a repeated Index.db resolution.
// ---------------------------------------------------------------------------

mod big {
    use super::*;
    use cqlite_core::storage::sstable::index_reader::IndexReader;
    use cqlite_core::storage::sstable::reader::SSTableReader;
    use serial_test::serial;

    /// Learn a known-present raw partition key from the Index.db (its entries are
    /// keyed on the raw partition-key bytes since #552).
    async fn learn_present_raw_key(
        data_db: &Path,
        platform: Arc<cqlite_core::platform::Platform>,
    ) -> Option<Vec<u8>> {
        // Derive the Index.db name from the DISCOVERED Data.db filename (swap the
        // `-Data.db` suffix for `-Index.db`) so the probe tracks whatever fixture
        // `find_data_db` actually found — not a hardcoded gen-1 `nb-big` name that
        // would silently self-skip on any other generation/format.
        let data_name = data_db.file_name()?.to_string_lossy();
        let index_name = format!("{}-Index.db", data_name.strip_suffix("-Data.db")?);
        let index_path = data_db.with_file_name(index_name);
        if !index_path.exists() {
            return None;
        }
        let index_reader = IndexReader::open(&index_path, platform).await.ok()?;
        let entry = index_reader.get_partition_entries().first()?;
        Some(entry.key_digest.to_vec())
    }

    #[tokio::test]
    #[serial]
    async fn big_repeated_index_lookup_skips_probe_and_matches() {
        let Some(data_db) = find_data_db("test_basic", "simple_table") else {
            eprintln!("Skipping (B4/BIG): test_basic/simple_table Data.db not present");
            return;
        };
        let config = cqlite_core::Config::default();
        let platform = Arc::new(
            cqlite_core::platform::Platform::new(&config)
                .await
                .expect("platform"),
        );
        let Some(raw_key) = learn_present_raw_key(&data_db, platform.clone()).await else {
            eprintln!("Skipping (B4/BIG): could not learn a present raw key (no Index.db)");
            return;
        };

        let reader = SSTableReader::open(&data_db, &config, platform)
            .await
            .expect("open BIG reader");

        // First resolution: a real Index.db probe (cache miss).
        rwc::reset();
        let first = reader
            .lookup_partition_with_index(&raw_key)
            .await
            .expect("first index lookup");
        let Some(first) = first else {
            eprintln!("Skipping (B4/BIG): raw-key Index.db lookup did not resolve on this fixture");
            return;
        };
        assert_eq!(
            rwc::index_probes(),
            1,
            "B4/BIG: the first (cold) resolution must perform exactly one Index.db probe"
        );

        // Second resolution: served from the B4 cache — zero Index.db probes.
        rwc::reset();
        let second = reader
            .lookup_partition_with_index(&raw_key)
            .await
            .expect("second index lookup")
            .expect("cached hit must resolve");
        assert_eq!(
            rwc::index_probes(),
            0,
            "B4/BIG: a repeated point read must skip the Index.db probe (key-cache hit); got {}",
            rwc::index_probes()
        );
        assert_eq!(
            first, second,
            "B4/BIG correctness: a cache hit must return the SAME (offset,size) a fresh probe does"
        );
    }

    /// Absent-key wiring evidence (issue #1570 C finding, R3 scenario 3): a
    /// partition key the SSTable does NOT contain must resolve to authoritative
    /// absence on EVERY read and must NEVER be cached (the positive-only insert
    /// discipline never fires for a miss). We prove this at the read-path surface:
    /// looking the absent key up TWICE returns `Ok(None)` both times, and the
    /// SECOND lookup still records `INDEX_PROBES >= 1` — had the absent key been
    /// cached, the second lookup would have short-circuited with `INDEX_PROBES == 0`
    /// (contrast `big_repeated_index_lookup_skips_probe_and_matches`, where a
    /// *present* key's re-read observes `== 0`). The `key_offset_cache` field is
    /// crate-private, so the counter is the public-surface proxy for "no positive
    /// cache entry was created".
    #[tokio::test]
    #[serial]
    async fn big_absent_key_never_cached_reprobes_index() {
        let Some(data_db) = find_data_db("test_basic", "simple_table") else {
            eprintln!("Skipping (B4/BIG absent): test_basic/simple_table Data.db not present");
            return;
        };
        let config = cqlite_core::Config::default();
        let platform = Arc::new(
            cqlite_core::platform::Platform::new(&config)
                .await
                .expect("platform"),
        );
        // Learn a genuinely-present raw key ONLY to confirm this fixture has an
        // Index.db (so a probe is actually recorded); we then derive an absent key
        // from it that can never match an exact-bytes Index.db entry.
        let Some(present) = learn_present_raw_key(&data_db, platform.clone()).await else {
            eprintln!("Skipping (B4/BIG absent): could not learn a present raw key (no Index.db)");
            return;
        };
        // An exact-match Index.db (raw bytes since #552) can never contain this:
        // it is a present key with extra suffix bytes, so its length/bytes differ
        // from every real entry — guaranteed authoritative absence.
        let mut absent_key = present.clone();
        absent_key.push(0x00);
        absent_key.extend_from_slice(b"cqlite-absent-partition-key");

        let reader = SSTableReader::open(&data_db, &config, platform)
            .await
            .expect("open BIG reader");

        // First lookup of the absent key: a real Index.db probe that MISSES.
        rwc::reset();
        let first = reader
            .lookup_partition_with_index(&absent_key)
            .await
            .expect("first absent index lookup");
        assert!(
            first.is_none(),
            "B4/BIG absent: an absent key must resolve to authoritative absence (Ok(None))"
        );
        assert!(
            rwc::index_probes() >= 1,
            "B4/BIG absent: the first absent lookup must perform a real Index.db probe; got {}",
            rwc::index_probes()
        );

        // Second lookup of the SAME absent key: because a miss is never cached, the
        // read must re-probe Index.db (INDEX_PROBES >= 1). A positive cache entry
        // would instead short-circuit here with INDEX_PROBES == 0.
        rwc::reset();
        let second = reader
            .lookup_partition_with_index(&absent_key)
            .await
            .expect("second absent index lookup");
        assert!(
            second.is_none(),
            "B4/BIG absent: the repeated absent lookup must still resolve to Ok(None)"
        );
        assert!(
            rwc::index_probes() >= 1,
            "B4/BIG absent: a repeated absent lookup must RE-PROBE Index.db (INDEX_PROBES >= 1), \
             proving the absent key was never cached as a hit; got {}",
            rwc::index_probes()
        );
    }

    /// Disabled-toggle wiring evidence (issue #1570 roborev): with
    /// `config.memory.block_cache.enabled == false` the reader builds a genuine
    /// no-op key cache (`build_key_offset_cache` → `disabled()`), so the point-read
    /// path re-probes `Index.db` on EVERY read — the toggle is real, not decorative.
    /// A SECOND repeated resolution therefore still records `INDEX_PROBES >= 1`
    /// (contrast the enabled test, which observes `== 0` on the hit).
    #[tokio::test]
    #[serial]
    async fn big_disabled_cache_reprobes_index_on_every_read() {
        let Some(data_db) = find_data_db("test_basic", "simple_table") else {
            eprintln!("Skipping (B4/BIG disabled): test_basic/simple_table Data.db not present");
            return;
        };
        // Same reader/config construction as the enabled test, but flip the B2
        // read-cache toggle OFF so the reader wires a disabled key-offset cache.
        let mut config = cqlite_core::Config::default();
        config.memory.block_cache.enabled = false;
        let platform = Arc::new(
            cqlite_core::platform::Platform::new(&config)
                .await
                .expect("platform"),
        );
        let Some(raw_key) = learn_present_raw_key(&data_db, platform.clone()).await else {
            eprintln!(
                "Skipping (B4/BIG disabled): could not learn a present raw key (no Index.db)"
            );
            return;
        };

        let reader = SSTableReader::open(&data_db, &config, platform)
            .await
            .expect("open BIG reader (cache disabled)");

        // First resolution: a real Index.db probe.
        rwc::reset();
        let first = reader
            .lookup_partition_with_index(&raw_key)
            .await
            .expect("first index lookup");
        let Some(_first) = first else {
            eprintln!(
                "Skipping (B4/BIG disabled): raw-key Index.db lookup did not resolve on this fixture"
            );
            return;
        };
        assert_eq!(
            rwc::index_probes(),
            1,
            "B4/BIG disabled: the first resolution must perform exactly one Index.db probe"
        );

        // Second resolution: with the cache disabled there is NO hit, so the read
        // must re-probe Index.db — the toggle genuinely bypasses key caching.
        rwc::reset();
        let _second = reader
            .lookup_partition_with_index(&raw_key)
            .await
            .expect("second index lookup")
            .expect("present key must still resolve");
        assert!(
            rwc::index_probes() >= 1,
            "B4/BIG disabled: a disabled key cache must RE-PROBE Index.db on the repeated read \
             (INDEX_PROBES >= 1), proving the toggle is not decorative; got {}",
            rwc::index_probes()
        );
    }
}

// ---------------------------------------------------------------------------
// BTI wiring: TRIE_WALKS == 0 on a repeated (interleaved) point read.
// ---------------------------------------------------------------------------

#[cfg(not(feature = "tombstones"))]
mod bti {
    use super::*;
    use cqlite_core::ingestion::{ingest, IngestionConfig};
    use cqlite_core::{Database, Value};
    use serial_test::serial;

    // ---- Why these guards assert on B4, not TRIE_WALKS (issue #2674) ----------
    //
    // The absent-key and disabled-toggle guards below originally asserted on
    // `TRIE_WALKS` (>= 1, or the disabled companion). That proxy was FLAKY
    // (~1-in-5): the C3 `bti_lookup_memo` (issue #1574) is *reader-local* and
    // legitimately stores the resolution — INCLUDING a `None` absence — of the last
    // key it resolved. Which pooled reader instance serves each `db.execute` is not
    // deterministic, so whether the memo still held the interleaved key or the
    // key-under-test varied run to run; when a consulted reader's memo already held
    // the key-under-test, the read returned the memoized resolution WITHOUT a trie
    // descent (`TRIE_WALKS == 0`) — a correct single-walk optimization, not a cache
    // bug. Root-cause proof (20 runs, counter deltas): the B4 cache behaves
    // IDENTICALLY on every run (the absent key is always a MISS, `len` never grows);
    // only `TRIE_WALKS` varied with the memo. The counter is a process-global atomic
    // (NOT thread-local), so this is NOT the #2451 thread-local-scoping class — it is
    // a wrong-proxy assertion. Both guards now assert the property they actually
    // exist to protect, directly on the deterministic, memo-immune B4 cache counters
    // (`hit_count`/`len`): the absent key is never served a B4 HIT; a disabled reader
    // never touches the global B4 cache at all.
    // ---------------------------------------------------------------------------

    fn schemas_dir() -> Option<PathBuf> {
        if let Some(root) = datasets_root() {
            if let Some(dir) = root.parent().and_then(|p| {
                let d = p.join("schemas");
                d.exists().then_some(d)
            }) {
                return Some(dir);
            }
        }
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let dir = manifest_dir.parent()?.join("test-data").join("schemas");
        dir.exists().then_some(dir)
    }

    async fn setup(keyspace: &str, schema_file: &str) -> Option<Database> {
        setup_with_core_config(keyspace, schema_file, cqlite_core::Config::default()).await
    }

    /// Like [`setup`] but with a caller-supplied `core_config`, so a test can flip
    /// the `block_cache.enabled` read-cache toggle that gates the key-offset cache.
    async fn setup_with_core_config(
        keyspace: &str,
        schema_file: &str,
        core_config: cqlite_core::Config,
    ) -> Option<Database> {
        let root = datasets_root()?;
        let schema_path = schemas_dir()?.join(schema_file);
        if !schema_path.exists() {
            return None;
        }
        let data_dir = root.join("sstables");
        if !data_dir.exists() {
            return None;
        }
        let config = IngestionConfig {
            schema_paths: vec![schema_path],
            data_dir,
            version_hint: Some("5.0".to_string()),
            core_config,
            table_directory_filter: Some(format!("/{keyspace}/")),
        };
        let result = ingest(config).await.ok()?;
        if result.schema_load_result.schemas_loaded == 0 {
            return None;
        }
        Some(result.database)
    }

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

    /// Learn two distinct present `id` UUIDs and build projected point-read SQL for
    /// each (>8 tokens → routes through the modern partition-targeted path).
    async fn learn_two_point_sqls(db: &Database, table: &str) -> Option<(String, String)> {
        let scan = db.execute(&format!("SELECT id FROM {table}")).await.ok()?;
        let mut ids: Vec<[u8; 16]> = Vec::new();
        for row in &scan.rows {
            if let Some(Value::Uuid(b)) = row.values.get("id") {
                if !ids.contains(b) {
                    ids.push(*b);
                }
            }
            if ids.len() == 2 {
                break;
            }
        }
        if ids.len() < 2 {
            return None;
        }
        let sql = |id: &[u8; 16]| {
            format!(
                "SELECT id, name FROM {table} WHERE id = {}",
                uuid_to_literal(id)
            )
        };
        Some((sql(&ids[0]), sql(&ids[1])))
    }

    /// Learn one present `id` UUID plus a deterministically-chosen UUID the fixture
    /// does NOT contain, and build a projected point-read SQL for each. The absent
    /// UUID is found by scanning ALL present ids into a set and walking `[0u8;16]`
    /// upward until a value not in the set is reached (guaranteed to terminate on a
    /// finite fixture) — so the "absent" key is provably absent, not merely assumed.
    async fn learn_present_and_absent_sqls(db: &Database, table: &str) -> Option<(String, String)> {
        let scan = db.execute(&format!("SELECT id FROM {table}")).await.ok()?;
        let mut ids: Vec<[u8; 16]> = Vec::new();
        for row in &scan.rows {
            if let Some(Value::Uuid(b)) = row.values.get("id") {
                if !ids.contains(b) {
                    ids.push(*b);
                }
            }
        }
        let present = *ids.first()?;
        // Find a UUID not present in the fixture by incrementing from all-zeros.
        let mut absent = [0u8; 16];
        while ids.contains(&absent) {
            for i in (0..16).rev() {
                if absent[i] == 0xFF {
                    absent[i] = 0;
                } else {
                    absent[i] += 1;
                    break;
                }
            }
        }
        let sql = |id: &[u8; 16]| {
            format!(
                "SELECT id, name FROM {table} WHERE id = {}",
                uuid_to_literal(id)
            )
        };
        Some((sql(&present), sql(&absent)))
    }

    /// Absent-key wiring evidence (issue #1570 C finding, R3 scenario 3): a BTI
    /// trie-MISS key must resolve to authoritative absence on every read and must
    /// NEVER be served from the B4 key cache as a false-positive HIT. The B4 cache
    /// is trie-HIT-only (positive-only insert discipline: `key_cache_insert` fires
    /// only on the `Some(DataOffset)` resolution arm, never on a trie miss), so a
    /// clean-miss absent key can never populate it — every repeated absent read is a
    /// B4 cache MISS, re-resolving authoritative absence.
    ///
    /// Asserts on the B4 hit counter, not `TRIE_WALKS` — see the module-level
    /// "Why these guards assert on B4, not TRIE_WALKS (issue #2674)" note above.
    #[tokio::test]
    #[serial]
    async fn bti_absent_key_never_cached_rewalks_trie() {
        if find_data_db("test_da", "simple_table").is_none() {
            eprintln!("Skipping (B4/BTI absent): optional test_da/simple_table not present");
            return;
        }
        let Some(db) = setup("test_da", "da-test.cql").await else {
            eprintln!("Skipping (B4/BTI absent): could not ingest test_da");
            return;
        };
        let Some((sql_present, sql_absent)) =
            learn_present_and_absent_sqls(&db, "test_da.simple_table").await
        else {
            eprintln!("Skipping (B4/BTI absent): fixture has no keys to derive an absent key from");
            return;
        };

        let gc = cqlite_core::storage::cache::GlobalKeyOffsetCache::global();

        // Start COLD: flush the shared process-global cache so no entry left resident
        // by a prior test/read can make the precondition below pass vacuously (an
        // already-resident candidate for the absent key would otherwise be invisible
        // to a len-delta check). Restoring this flush also fixes an intent regression
        // (issue #2674 roborev 1799): the clean-miss precondition must observe a
        // genuinely cold lookup.
        gc.invalidate_all();

        // First read of the absent key: definitive miss (zero rows) that must NOT
        // create a B4 entry (positive-only insert discipline).
        //
        // CLEAN-MISS PRECONDITION (issue #2674 roborev) — counter-based, so it is
        // immune to LRU eviction-vacuity and to an already-resident candidate:
        //   - `hit_count()` must NOT advance: a clean trie MISS is never served from
        //     B4 (nothing was cached for the absent key).
        //   - `miss_count()` MUST advance: the read really DID consult B4 and resolved
        //     as a miss (liveness — proves the assertion is not vacuously green because
        //     the path stopped consulting the cache).
        //   - `len()` unchanged is kept only as a SECONDARY witness: a clean miss
        //     resolves to `None` and never calls `key_cache_insert`.
        // If `hit_count()` advances (or `len()` grows), the fixture's chosen "absent"
        // key is a BTI prefix-collision CANDIDATE whose candidate offset IS positively
        // cached and re-verified downstream to absence — fixture drift (the derived
        // absent key collides on a trie prefix), NOT a cache bug; it would invalidate
        // the "no B4 hit" re-read assertion, so it is diagnosed explicitly here.
        let hits_before_absent = gc.hit_count();
        let misses_before_absent = gc.miss_count();
        let len_before_absent = gc.len();
        let r_absent = db
            .execute(&sql_absent)
            .await
            .expect("BTI absent point read");
        assert!(
            r_absent.rows.is_empty(),
            "B4/BTI absent: an absent key must return zero rows (authoritative absence)"
        );
        assert_eq!(
            gc.hit_count(),
            hits_before_absent,
            "B4/BTI absent precondition: a clean trie-MISS absent key must not be served a B4 \
             HIT. It was — the 'absent' key is a BTI prefix-collision CANDIDATE (its candidate \
             offset is cached): fixture drift, not a cache bug; choose an absent key with no \
             trie-prefix collision"
        );
        assert!(
            gc.miss_count() > misses_before_absent,
            "B4/BTI absent precondition liveness: the first absent read must actually CONSULT \
             the B4 cache and resolve as a MISS (miss_count must advance); it did not, so the \
             read path is not exercising the cache and the no-hit assertion would be vacuous"
        );
        assert_eq!(
            gc.len(),
            len_before_absent,
            "B4/BTI absent precondition (secondary witness): a clean trie-MISS absent key must \
             not insert a B4 entry (positive-only insert discipline)"
        );

        // Read present key A: exercises a real B4 populate for a DIFFERENT key, so a
        // stale positive entry for A cannot alias the absent key on the re-read.
        let r_present = db
            .execute(&sql_present)
            .await
            .expect("BTI present point read");
        assert!(
            !r_present.rows.is_empty(),
            "B4/BTI absent: interleave key A must be present"
        );

        // Re-read the absent key and prove it is NEVER served a B4 cache HIT. Capture
        // the process-global B4 hit counter immediately before the re-read; a correct
        // read resolves the absent key by a cache MISS (then re-descends / re-verifies
        // to absence) — the hit counter must not advance. Had the first absent read
        // negatively cached the key (the perf/correctness defect this guard exists to
        // catch), the re-read would register a B4 HIT and this delta would be > 0.
        //
        // SINGLE-LOOKUP WINDOW (issue #2674 roborev): `hit_count()` is process-global.
        // The window is safe from OTHER tests only against DEFAULT-KEY `#[serial]`
        // tests IN THIS BINARY — a bare `#[serial]` (all tests here use it) is one
        // global lock, so no bare-`#[serial]` peer runs concurrently; a *keyed*
        // `#[serial(k)]` group would NOT be mutually excluded (different key), and a
        // non-`#[serial]` test would not be either. There are none of those here. And
        // this is a current-thread runtime, so no background task advances the counter
        // within the test. The only B4 lookups between `hits_before` and `hits_after`
        // are thus this ONE point read's own — a lookup of the absent key against each
        // generation's reader (each a MISS, per the clean-miss precondition). A
        // non-zero delta therefore unambiguously means the absent key was served a hit.
        let hits_before = gc.hit_count();
        let misses_before = gc.miss_count();
        let len_before_reread = gc.len();
        let r_absent2 = db
            .execute(&sql_absent)
            .await
            .expect("BTI repeated absent point read");
        let hits_after = gc.hit_count();
        assert!(
            r_absent2.rows.is_empty(),
            "B4/BTI absent: the repeated absent read must still return zero rows"
        );
        assert_eq!(
            hits_after,
            hits_before,
            "B4/BTI absent: a repeated absent read must NOT be served from the B4 key cache \
             (positive-only insert discipline — a clean trie miss is never negatively cached); \
             the re-read registered {} B4 cache hit(s), indicating the absent key was cached \
             as a false-positive hit",
            hits_after - hits_before
        );
        // LIVENESS CONTROL (issue #2674 roborev 1799): the re-read must actually
        // CONSULT the B4 cache and resolve as a MISS — `miss_count` must advance. This
        // makes `hits_after == hits_before` a real differential: without it the no-hit
        // assertion would pass vacuously if the read path stopped consulting the cache
        // altogether (zero hits AND zero misses).
        assert!(
            gc.miss_count() > misses_before,
            "B4/BTI absent: the repeated absent read must CONSULT the B4 cache and resolve as a \
             MISS (miss_count must advance); it did not, so the no-hit assertion above is \
             vacuous — the read path is not exercising the cache"
        );
        // Belt-and-braces (issue #2674 roborev): the re-read also must not GROW the
        // cache — a clean trie miss inserts nothing, so a candidate for the absent key
        // is never added on the repeat either (the miss above and no-growth here
        // together bound the absent key out of B4 entirely).
        assert_eq!(
            gc.len(),
            len_before_reread,
            "B4/BTI absent: the repeated absent read must not insert a B4 entry for the absent \
             key (positive-only insert discipline)"
        );
    }

    /// Scenario: a repeated (interleaved) BTI point read performs ZERO trie walks on
    /// the hit — proving the multi-key B4 cache, not the single-entry C3 memo.
    #[tokio::test]
    #[serial]
    async fn bti_repeated_interleaved_point_read_skips_trie_walk() {
        if find_data_db("test_da", "simple_table").is_none() {
            eprintln!("Skipping (B4/BTI): optional test_da/simple_table not present");
            return;
        }
        let Some(db) = setup("test_da", "da-test.cql").await else {
            eprintln!("Skipping (B4/BTI): could not ingest test_da");
            return;
        };
        let Some((sql_a, sql_b)) = learn_two_point_sqls(&db, "test_da.simple_table").await else {
            eprintln!("Skipping (B4/BTI): fixture has fewer than two distinct keys");
            return;
        };

        // Read A (populates the B4 cache for A), then B (evicts A from the SINGLE
        // C3 memo, but not from the multi-key B4 LRU).
        let ra = db.execute(&sql_a).await.expect("BTI point read A");
        assert!(!ra.rows.is_empty(), "B4/BTI: key A must be present");
        let _ = db.execute(&sql_b).await.expect("BTI point read B");

        // Now re-read A. The single-entry memo holds B, so only the B4 cache can
        // serve A without a trie descent.
        rwc::reset();
        assert_eq!(rwc::trie_walks(), 0, "reset must zero TRIE_WALKS");
        let ra2 = db.execute(&sql_a).await.expect("BTI repeated point read A");
        assert!(
            !ra2.rows.is_empty(),
            "B4/BTI: repeated read of present key A returned zero rows"
        );
        assert_eq!(
            rwc::trie_walks(),
            0,
            "B4/BTI: a repeated point read of a cached key must descend the trie ZERO times \
             (key-cache hit, beyond the single-entry memo); got {}",
            rwc::trie_walks()
        );
    }

    /// Disabled-toggle wiring evidence (issue #1570 roborev): with
    /// `config.memory.block_cache.enabled == false` the reader builds a genuine
    /// no-op key cache ([`build_key_offset_cache`] hands back a
    /// `GlobalKeyOffsetCache::disabled()` instance instead of the process-global
    /// singleton), so NONE of this database's reads ever touch the global B4 cache —
    /// its hit counter and occupancy stay flat across an interleaved A, B, A read.
    ///
    /// Asserts on the global B4 cache, not `TRIE_WALKS` — see the module-level "Why
    /// these guards assert on B4, not TRIE_WALKS (issue #2674)" note above. A POSITIVE
    /// CONTROL (an enabled db over the same fixture + query shape, whose re-read of A
    /// DOES advance the hit counter) proves the counter is live here, so the disabled
    /// assertion is a real differential, not a vacuous "nothing happened".
    #[tokio::test]
    #[serial]
    async fn bti_disabled_cache_rewalks_trie_on_interleaved_reread() {
        if find_data_db("test_da", "simple_table").is_none() {
            eprintln!("Skipping (B4/BTI disabled): optional test_da/simple_table not present");
            return;
        }
        let mut core_config = cqlite_core::Config::default();
        core_config.memory.block_cache.enabled = false;
        let Some(db) = setup_with_core_config("test_da", "da-test.cql", core_config).await else {
            eprintln!("Skipping (B4/BTI disabled): could not ingest test_da");
            return;
        };
        let Some((sql_a, sql_b)) = learn_two_point_sqls(&db, "test_da.simple_table").await else {
            eprintln!("Skipping (B4/BTI disabled): fixture has fewer than two distinct keys");
            return;
        };

        // Capture the process-global B4 cache hit counter AND occupancy. Every read
        // below runs through this disabled database, whose readers hold a `disabled()`
        // no-op cache — so the GLOBAL singleton must be touched neither by a serve
        // (hit) nor a populate (len grows). `#[serial]` (bare key) on a current-thread
        // runtime means no other test/task advances these in the window.
        let gc = cqlite_core::storage::cache::GlobalKeyOffsetCache::global();
        let hits_before = gc.hit_count();
        let len_before = gc.len();

        // Interleaved point reads A, B, A. With an ENABLED cache the final re-read of
        // A is a B4 HIT (see `bti_repeated_interleaved_point_read_skips_trie_walk`);
        // with the cache DISABLED it must not be, because the reader never consults
        // the global cache at all.
        let ra = db.execute(&sql_a).await.expect("BTI point read A");
        assert!(
            !ra.rows.is_empty(),
            "B4/BTI disabled: key A must be present"
        );
        let _ = db.execute(&sql_b).await.expect("BTI point read B");
        let ra2 = db.execute(&sql_a).await.expect("BTI repeated point read A");
        assert!(
            !ra2.rows.is_empty(),
            "B4/BTI disabled: repeated read of present key A returned zero rows"
        );

        assert_eq!(
            gc.hit_count(),
            hits_before,
            "B4/BTI disabled: a disabled key cache must be a genuine no-op — none of the \
             disabled database's reads may serve from the process-global B4 cache. The global \
             hit counter advanced by {}, meaning the `block_cache.enabled == false` toggle is \
             decorative (the reader still consulted the shared cache)",
            gc.hit_count() - hits_before
        );
        // Second witness (issue #2674 roborev): a disabled reader also never POPULATES
        // the global cache, so occupancy is flat too.
        assert_eq!(
            gc.len(),
            len_before,
            "B4/BTI disabled: a disabled key cache must not populate the process-global B4 \
             cache; occupancy grew, so the toggle is decorative"
        );

        // POSITIVE CONTROL (issue #2674 roborev): prove the hit counter is LIVE for
        // this exact fixture + query shape, so the flat-counter assertions above are a
        // real differential and not vacuously green. Open an ENABLED db over the same
        // fixture and run the identical A, B, A — the re-read of A is a genuine B4 HIT
        // (the enabled reader shares the global singleton), so the hit counter MUST
        // advance. If it did not, the disabled assertions would prove nothing.
        let Some(db_enabled) = setup("test_da", "da-test.cql").await else {
            eprintln!("Skipping (B4/BTI disabled positive control): could not ingest test_da");
            return;
        };
        let hits_before_enabled = gc.hit_count();
        let ea = db_enabled
            .execute(&sql_a)
            .await
            .expect("enabled point read A");
        assert!(
            !ea.rows.is_empty(),
            "B4/BTI disabled positive control: key A must be present"
        );
        let _ = db_enabled
            .execute(&sql_b)
            .await
            .expect("enabled point read B");
        let ea2 = db_enabled
            .execute(&sql_a)
            .await
            .expect("enabled repeated point read A");
        assert!(
            !ea2.rows.is_empty(),
            "B4/BTI disabled positive control: repeated read of present key A returned zero rows"
        );
        assert!(
            gc.hit_count() > hits_before_enabled,
            "B4/BTI disabled positive control: an ENABLED db over the same fixture/query must \
             register at least one B4 cache HIT on the re-read of A — proving the hit counter \
             is live for this shape and the disabled no-op assertions above are a real \
             differential; hit counter did not advance (before={hits_before_enabled}, \
             after={})",
            gc.hit_count()
        );
    }
}
