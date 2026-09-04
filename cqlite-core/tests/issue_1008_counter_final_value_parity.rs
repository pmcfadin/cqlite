//! Issue #1008 — Counter final-value parity (CQLite ↔ Cassandra 5.0), epic #971.
//!
//! Proves CQLite decodes and MERGES Apache Cassandra counter cells to the SAME
//! final, user-visible value that `SELECT pk, c FROM …` returns in Cassandra,
//! across four real fixtures under `test_types/ct_*`:
//!
//!   * `ct_single_sstable` — one generation; the CounterContext shard sum must
//!     surface the logical total (pk1=30, pk2=5).
//!   * `ct_multi_sstable_merge` — TWO uncompacted generations that must be MERGED
//!     at read time (pk1=85, pk2=60).
//!   * `ct_deleted_counter_shadowing` — increment, flush, DELETE partition,
//!     flush. The partition tombstone must SHADOW the older counter (pk1 absent),
//!     leaving pk2=33.
//!   * `ct_compacted_final_value` — `nodetool compact` merged the sources away;
//!     only the compacted generation (nb-3) remains (pk1=210, pk2=40).
//!
//! ## What "the same final value" means for counters
//!
//! sstabledump renders a counter cell as the RAW CounterContext bytes
//! interpreted as a big-endian i64 (a large value such as `422212677445164`).
//! That is INTERNAL SHARD STATE, not the value a user sees, and because the
//! CounterContext embeds a per-node UUID it CHANGES every time the fixtures are
//! regenerated — so it must never be hardcoded as a fixed constant. The
//! user-visible value is the logical count Cassandra's `SELECT` returns,
//! captured in each fixture's committed `*.counter-select.txt` sidecar
//! (`pk | c` table). This lane therefore asserts CQLite's merged value against
//! the SIDECAR (the authoritative final values) and *separately* asserts that
//! CQLite never surfaces any raw shard rendering the golden actually carries —
//! derived dynamically from the golden, never matched against a fixed integer —
//! i.e. the CounterContext was actually decoded and summed, not passed through.
//!
//! ## Cross-generation counter merge model (derived, not guessed)
//!
//! Within one generation, a counter cell's CounterContext already carries the
//! running total for the shards it touched (CQLite sums the shard counts, mirror
//! of Cassandra's `CounterContext.total()`). Across generations Cassandra
//! reconciles per shard-id by clock and keeps the highest clock; for these
//! single-node fixtures every increment lands on the same shard-id, so the
//! generation with the LATEST cell writetime carries the fully-accumulated total
//! (verified empirically: ct_multi gen-1 pk1=100 → gen-2 pk1=85, final=85, i.e.
//! last-writetime-wins, NOT 100+85). A partition tombstone with `deleted_at`
//! newer than that winning cell shadows it entirely. This test implements that
//! merge over the public `scan_delta` records and asserts the result equals the
//! Cassandra `SELECT`.
//!
//! ## Discipline (no-heuristics / no-fake-pass)
//!
//! - "Is this a counter / what is the expected final value" is derived from the
//!   Statistics.db.txt (`CounterColumnType`) and the counter-select sidecar — NOT
//!   from the table-name path.
//! - The deleted counter (pk1) is asserted ABSENT/shadowed — never masked with
//!   `unwrap_or(0)`.
//! - The multi-SSTable merge ACTUALLY reads BOTH generations and merges them.
//! - SKIP when `CQLITE_DATASETS_ROOT` is unset or the binary Data.db is absent —
//!   UNLESS `CQLITE_REQUIRE_FIXTURES=1`, under which an absence is a hard, NAMED
//!   failure (issue #3725: this target is executed by the merge-gating
//!   `feature-iso-delta-scan` lane, which exports that flag on the full gate, and a
//!   target that ignored it could gate a merge having compared nothing). FAIL loudly
//!   when a committed sidecar/golden carries facts but none were matched.

#![cfg(feature = "delta-scan")]

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::reader::delta_scan::{scan_delta, DeltaRecord};
use cqlite_core::types::Value;

#[path = "support/canonical_jsonl.rs"]
mod canonical_jsonl;
use canonical_jsonl::{load_golden_document, CanonicalValue};

// ===========================================================================
// Dataset path helpers
// ===========================================================================

/// `CQLITE_REQUIRE_FIXTURES=1` turns a fixture absence into a hard failure.
///
/// The `feature-iso-delta-scan` lane (issue #3725) exports it on the FULL gate, because a
/// target that skip-passes with its corpus absent can merge-gate as "passed" having compared
/// nothing. Mirrors `issue_1007_complex_type_parity`'s helper of the same name.
fn require_fixtures_strict() -> bool {
    std::env::var("CQLITE_REQUIRE_FIXTURES")
        .map(|v| v == "1")
        .unwrap_or(false)
}

/// Skip an absent fixture — or FAIL by name under strict mode.
///
/// `subject` NAMES the keyspace and table: the gate's #2078 preflight probes only the
/// CANONICAL keyspace (`test_basic`), so a generic "fixtures absent" would send the reader
/// to a remedy that is already satisfied. These fixtures live in `test_types`.
fn skip_or_fail(subject: &str, reason: &str) {
    if require_fixtures_strict() {
        panic!(
            "CQLITE_REQUIRE_FIXTURES=1 but {subject} fixture unavailable: {reason}. \
             Remedy: bash test-data/scripts/fetch-datasets.sh (then export the \
             CQLITE_DATASETS_ROOT it prints)."
        );
    }
    println!("[SKIP] {subject}: {reason}");
}

fn datasets_root() -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let path = PathBuf::from(root).join("sstables");
    path.exists().then_some(path)
}

/// Locate the `test_types/<prefix>-<uuid>` fixture directory.
fn find_table_dir(prefix: &str) -> Option<PathBuf> {
    let ks_dir = datasets_root()?.join("test_types");
    let mut candidates: Vec<PathBuf> = std::fs::read_dir(&ks_dir)
        .ok()?
        .flatten()
        .filter_map(|e| {
            let name = e.file_name();
            let s = name.to_str()?;
            (s.starts_with(prefix) && !s.starts_with("._")).then(|| e.path())
        })
        .collect();
    candidates.sort();
    candidates.into_iter().next()
}

/// Sorted list of component prefixes (`nb-N-big-`) present in `dir`.
fn generation_prefixes(dir: &Path) -> Vec<String> {
    let mut gens = BTreeSet::new();
    for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let n = e.file_name().to_string_lossy().to_string();
        if n.starts_with("._") {
            continue;
        }
        if let Some(idx) = n.find("-big-") {
            gens.insert(n[..idx + 5].to_string());
        }
    }
    gens.into_iter().collect()
}

/// `true` when the fixture has at least one binary `Data.db` (so query tests can
/// actually run; JSONL-only checkouts SKIP).
fn has_binary_data_db(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .any(|e| {
            let n = e.file_name().to_string_lossy().to_string();
            n.ends_with("-Data.db") && !n.starts_with("._")
        })
}

/// Copy one generation's binary components into an isolated temp dir so
/// `scan_delta` (which opens the lexicographically-first Data.db in a dir) reads
/// EXACTLY that generation. Returns the temp dir path.
fn stage_generation(dir: &Path, prefix: &str) -> std::io::Result<PathBuf> {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    // Unique per invocation so concurrent tests staging the SAME fixture
    // generation never share (and race-delete) a temp directory.
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
        ^ u128::from(SEQ.fetch_add(1, Ordering::Relaxed));
    let tmp = std::env::temp_dir().join(format!(
        "cqlite-1008-{}-{}-{nonce}",
        dir.file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("fixture"),
        prefix.trim_end_matches('-')
    ));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp)?;
    for e in std::fs::read_dir(dir)?.flatten() {
        let n = e.file_name().to_string_lossy().to_string();
        // Only the binary SSTable components — skip JSONL / txt sidecars and
        // AppleDouble files.
        if n.starts_with(prefix)
            && !n.starts_with("._")
            && !n.ends_with(".jsonl")
            && !n.ends_with(".txt")
        {
            std::fs::copy(e.path(), tmp.join(&n))?;
        }
    }
    Ok(tmp)
}

// ===========================================================================
// Schema (derived from Statistics.db.txt: KeyType Int32Type, RegularColumns
// c:CounterColumnType, no clustering)
// ===========================================================================

fn counter_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_types".to_string(),
        table: "ct".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "c".to_string(),
                data_type: "counter".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Assert the fixture's Statistics dump declares the `c` column as a counter —
/// the authoritative "this is a counter table" signal (NOT the directory name).
fn assert_counter_table_via_statistics(dir: &Path) {
    let mut saw_stats = false;
    let mut saw_counter_type = false;
    for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let n = e.file_name().to_string_lossy().to_string();
        if n.ends_with("-Statistics.db.txt") && !n.starts_with("._") {
            saw_stats = true;
            let txt = std::fs::read_to_string(e.path()).unwrap_or_default();
            // Not every generation lists regular columns — a partition-tombstone
            // -only SSTable (e.g. the DELETE generation) has an empty
            // RegularColumns line. Require the counter type to be declared in AT
            // LEAST ONE generation's Statistics (still authoritative, never the
            // directory name).
            if txt.contains("CounterColumnType") {
                saw_counter_type = true;
            }
        }
    }
    assert!(
        saw_stats,
        "no *-Statistics.db.txt present in {:?}; cannot authoritatively confirm counter type",
        dir
    );
    assert!(
        saw_counter_type,
        "no generation's Statistics in {:?} declares CounterColumnType — fixture is not a counter \
         table (no-heuristics: the type comes from Statistics, not the path)",
        dir
    );
}

// ===========================================================================
// counter-select sidecar parser (the authoritative Cassandra SELECT result)
// ===========================================================================

/// Parse a `cqlsh`-style `pk | c` result table into `pk -> final_value` pairs.
/// Lines look like:
/// ```text
///  pk | c
/// ----+----
///   1 | 30
///   2 |  5
///
/// (2 rows)
/// ```
/// Returns the parsed rows AND the declared `(N rows)` count so a malformed or
/// truncated sidecar fails loudly rather than silently matching nothing.
fn parse_counter_select_sidecar(path: &Path) -> (Vec<(i64, i64)>, usize) {
    let content =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read sidecar {path:?}: {e}"));
    let mut rows = Vec::new();
    let mut declared_rows: Option<usize> = None;
    for line in content.lines() {
        let t = line.trim();
        if t.is_empty() {
            continue;
        }
        if let Some(rest) = t.strip_prefix('(') {
            // "(2 rows)" footer.
            if let Some(num) = rest.split_whitespace().next() {
                if let Ok(n) = num.parse::<usize>() {
                    declared_rows = Some(n);
                }
            }
            continue;
        }
        // Skip the header ("pk | c") and the separator ("----+----").
        if t.starts_with("pk") || t.starts_with('-') || t.starts_with('+') {
            continue;
        }
        let Some((pk_s, c_s)) = t.split_once('|') else {
            continue;
        };
        let (Ok(pk), Ok(c)) = (pk_s.trim().parse::<i64>(), c_s.trim().parse::<i64>()) else {
            continue;
        };
        rows.push((pk, c));
    }
    let declared =
        declared_rows.unwrap_or_else(|| panic!("sidecar {path:?} missing the `(N rows)` footer"));
    assert_eq!(
        rows.len(),
        declared,
        "sidecar {path:?} declared {declared} rows but parsed {} — sidecar parse is broken",
        rows.len()
    );
    (rows, declared)
}

/// Locate the `*.counter-select.txt` sidecar in a fixture dir.
fn find_sidecar(dir: &Path) -> PathBuf {
    for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let n = e.file_name().to_string_lossy().to_string();
        if n.ends_with(".counter-select.txt") && !n.starts_with("._") {
            return e.path();
        }
    }
    panic!("no *.counter-select.txt sidecar in {dir:?}");
}

// ===========================================================================
// Counter read + cross-generation merge over the public scan_delta path
// ===========================================================================

/// The merged counter state for a partition: the winning (latest-writetime)
/// counter total and the writetime that produced it.
#[derive(Debug, Clone, Copy)]
struct CounterState {
    value: i64,
    writetime: i64,
}

/// Read EVERY generation in `fixture_dir` (each staged in isolation so the
/// reader sees exactly one generation), decode the `c` counter cell via the
/// public `scan_delta` reader path, and MERGE across generations the way a query
/// over the table would:
///
///   * for each partition keep the counter cell with the greatest writetime
///     (its CounterContext already carries the accumulated total), and
///   * drop any partition whose newest partition tombstone `deleted_at` is newer
///     than that winning cell (shadowing).
///
/// Returns `pk -> final visible counter value` (partitions shadowed by a newer
/// tombstone are ABSENT from the map — never inserted as 0).
async fn read_and_merge_counters(fixture_dir: &Path) -> HashMap<i64, i64> {
    let prefixes = generation_prefixes(fixture_dir);
    assert!(
        !prefixes.is_empty(),
        "no SSTable generations found in {fixture_dir:?}"
    );

    let mut upserts: HashMap<i64, CounterState> = HashMap::new();
    let mut partition_deletes: HashMap<i64, i64> = HashMap::new();
    let mut saw_any_record = false;

    for prefix in &prefixes {
        let staged = stage_generation(fixture_dir, prefix)
            .unwrap_or_else(|e| panic!("stage generation {prefix} of {fixture_dir:?}: {e}"));

        let (mut rx, _summary) = scan_delta(staged.clone(), counter_schema(), 64);
        while let Some(rec) = rx.recv().await {
            let rec = rec.unwrap_or_else(|e| panic!("scan_delta error on {prefix}: {e}"));
            saw_any_record = true;
            match rec {
                DeltaRecord::Upsert { keys, cells, .. } => {
                    let pk = partition_key_i64(&keys.partition);
                    for (col, delta) in cells {
                        if col.name() != "c" {
                            continue;
                        }
                        let Some(value) = delta.value else {
                            // A cell tombstone on the counter — not expected in
                            // these fixtures; surface loudly if it ever appears.
                            panic!(
                                "unexpected counter cell tombstone for pk={pk} in {prefix} — \
                                 fixture/merge model assumption violated"
                            );
                        };
                        let counter = match value {
                            Value::Counter(c) => c,
                            other => panic!(
                                "column `c` decoded as {other:?}, expected Value::Counter \
                                 (counter context decode regression)"
                            ),
                        };
                        let entry = upserts.entry(pk).or_insert(CounterState {
                            value: counter,
                            writetime: delta.writetime,
                        });
                        // Latest-writetime wins (the winning generation's
                        // CounterContext holds the accumulated total).
                        if delta.writetime >= entry.writetime {
                            entry.value = counter;
                            entry.writetime = delta.writetime;
                        }
                    }
                }
                DeltaRecord::PartitionDelete {
                    partition_key,
                    deleted_at,
                } => {
                    let pk = partition_key_i64(&partition_key.partition);
                    let e = partition_deletes.entry(pk).or_insert(deleted_at);
                    if deleted_at > *e {
                        *e = deleted_at;
                    }
                }
                other => panic!(
                    "unexpected delta record {:?} for counter fixture {fixture_dir:?}",
                    other.op_name()
                ),
            }
        }
        let _ = std::fs::remove_dir_all(&staged);
    }

    assert!(
        saw_any_record,
        "scan_delta yielded ZERO records for {fixture_dir:?} — binary present but nothing decoded"
    );

    // Apply shadowing: a partition tombstone newer than the winning cell removes
    // the partition entirely (it must be ABSENT, not 0).
    let mut final_values: HashMap<i64, i64> = HashMap::new();
    for (pk, state) in upserts {
        let shadowed = partition_deletes
            .get(&pk)
            .map(|&del_at| del_at >= state.writetime)
            .unwrap_or(false);
        if !shadowed {
            final_values.insert(pk, state.value);
        }
    }
    final_values
}

fn partition_key_i64(pk: &[Value]) -> i64 {
    match pk.first() {
        Some(Value::Integer(i)) => *i as i64,
        other => panic!("partition key not a single int: {other:?}"),
    }
}

// ===========================================================================
// JSONL golden cross-checks (raw shard renderings — the no-shard-state guard)
// ===========================================================================

/// Every `c`-cell `Int` value rendered in the committed sstabledump JSONL golden
/// for `gen_prefix`. sstabledump NEVER decodes a CounterContext, so every counter
/// cell value it emits is raw shard state (a big-endian i64 of the context bytes),
/// NOT the logical total. The exact integer is regen-dependent (the context embeds
/// a per-node UUID), so callers must treat these values as opaque/dynamic — never
/// compare against a fixed constant.
fn golden_counter_raw_values(fixture_dir: &Path, gen_prefix: &str) -> Vec<i128> {
    // Generation prefix is e.g. "nb-1-big-"; the golden is "<prefix>Data.db.jsonl".
    let golden = fixture_dir.join(format!("{gen_prefix}Data.db.jsonl"));
    let doc = load_golden_document(&golden, false)
        .unwrap_or_else(|e| panic!("load golden {golden:?}: {e}"));
    let mut values = Vec::new();
    for part in &doc.partitions {
        for row in &part.rows {
            for cell in &row.cells {
                if cell.name == "c" {
                    if let CanonicalValue::Int(v) = &cell.value {
                        values.push(*v);
                    }
                }
            }
        }
    }
    values
}

/// Count of counter (`c`) cells the golden renders as a raw shard `Int` value.
/// Used to assert the golden actually carries raw shard renderings; the exact
/// integers are intentionally ignored (regen-dependent).
fn golden_raw_shard_cell_count(fixture_dir: &Path, gen_prefix: &str) -> usize {
    golden_counter_raw_values(fixture_dir, gen_prefix).len()
}

/// Union of every `c`-cell raw shard `Int` value across all generations of a
/// fixture (the dynamic "shard state" set the merged user value must never be in).
fn golden_raw_value_set(fixture_dir: &Path) -> BTreeSet<i128> {
    let mut set = BTreeSet::new();
    for g in generation_prefixes(fixture_dir) {
        set.extend(golden_counter_raw_values(fixture_dir, &g));
    }
    set
}

// ===========================================================================
// Per-fixture parity driver
// ===========================================================================

/// `expect_partitions`: the partition keys (`pk`) that MUST be present in the
/// final result (everything else must be absent). The expected value for each is
/// taken from the sidecar.
async fn run_fixture(manifest_id: &str, prefix: &str) {
    let Some(dir) = find_table_dir(prefix) else {
        skip_or_fail(
            &format!("test_types.{prefix}"),
            &format!("[{manifest_id}] table directory not found under the datasets root"),
        );
        return;
    };
    if !has_binary_data_db(&dir) {
        skip_or_fail(
            &format!("test_types.{prefix}"),
            &format!("[{manifest_id}] no binary Data.db (JSONL-only checkout)"),
        );
        return;
    }

    // Authoritative "this is a counter table" check (Statistics, not path).
    assert_counter_table_via_statistics(&dir);

    // Authoritative expected final values (Cassandra SELECT result).
    let sidecar = find_sidecar(&dir);
    let (expected_rows, declared) = parse_counter_select_sidecar(&sidecar);
    assert!(
        declared > 0,
        "[{manifest_id}] sidecar {sidecar:?} declares zero rows — nothing to prove"
    );
    let expected: HashMap<i64, i64> = expected_rows.iter().copied().collect();

    // CQLite: read every generation and merge as a query would.
    let actual = read_and_merge_counters(&dir).await;

    // Dynamic no-shard-state guard set: the raw shard renderings the golden
    // actually carries for this fixture's generations (regen-stable — we read
    // whatever the golden contains rather than matching a fixed integer).
    let raw_shard_values = golden_raw_value_set(&dir);

    // 1) Every expected partition must be present with the EXACT final value.
    for (pk, exp_c) in &expected {
        let got = actual.get(pk).copied();
        assert_eq!(
            got,
            Some(*exp_c),
            "[{manifest_id}] {prefix}: pk={pk} final counter mismatch — \
             Cassandra SELECT (sidecar {sidecar:?}) says {exp_c}, CQLite merged says {got:?}"
        );
        // No-shard-state guard: the merged user value must NOT be any raw
        // CounterContext rendering the golden carries.
        assert!(
            !raw_shard_values.contains(&(*exp_c as i128)),
            "[{manifest_id}] sidecar value {exp_c} appears among the golden's raw shard \
             renderings {raw_shard_values:?} — test setup error / shard state not distinct"
        );
        if let Some(g) = got {
            assert!(
                !raw_shard_values.contains(&(g as i128)),
                "[{manifest_id}] {prefix}: pk={pk} exposed a RAW shard context value {g} \
                 (one of {raw_shard_values:?}) as the user-facing value (CounterContext was \
                 not decoded/summed)"
            );
        }
    }

    // 2) No partition may appear that the sidecar does not list (a deleted /
    //    shadowed partition must be ABSENT, not resurrected as 0 or stale).
    let expected_keys: BTreeSet<i64> = expected.keys().copied().collect();
    let actual_keys: BTreeSet<i64> = actual.keys().copied().collect();
    assert_eq!(
        actual_keys, expected_keys,
        "[{manifest_id}] {prefix}: partition-set mismatch — Cassandra SELECT has {expected_keys:?}, \
         CQLite produced {actual_keys:?} (a deleted counter must be absent, never read as 0)"
    );
}

// ===========================================================================
// Tests — one per manifest scenario
// ===========================================================================

/// Manifest: cass.cql_types.counters.single_sstable_context_decode
///
/// Single generation. The CounterContext shard sum must surface the logical
/// total (pk1=30, pk2=5), NOT the raw shard bytes.
#[tokio::test]
async fn counters_single_sstable_context_decode() {
    run_fixture(
        "cass.cql_types.counters.single_sstable_context_decode",
        "ct_single_sstable",
    )
    .await;
}

/// Manifest: cass.cql_types.counters.multi_sstable_increment_decrement_merge
///
/// TWO uncompacted generations (gen-1 increments, gen-2 inc/dec). CQLite must
/// READ BOTH and merge to the final total (pk1=85, pk2=60) — not the sum of the
/// two cells, and not just gen-1.
#[tokio::test]
async fn counters_multi_sstable_increment_decrement_merge() {
    let prefix = "ct_multi_sstable_merge";
    // Guard: the fixture really must carry MULTIPLE generations, else this is not
    // exercising the cross-SSTable merge it advertises.
    if let Some(dir) = find_table_dir(prefix) {
        if has_binary_data_db(&dir) {
            let gens = generation_prefixes(&dir);
            assert!(
                gens.len() >= 2,
                "multi-SSTable merge fixture {prefix} has only {} generation(s); the merge test \
                 must read at least two generations",
                gens.len()
            );
        }
    }
    run_fixture(
        "cass.cql_types.counters.multi_sstable_increment_decrement_merge",
        prefix,
    )
    .await;
}

/// Manifest: cass.cql_types.counters.deleted_counter_shadowing
///
/// gen-1 increments pk1 & pk2; gen-2 is a partition DELETE of pk1. The tombstone
/// must SHADOW pk1 (absent), leaving only pk2=33.
#[tokio::test]
async fn counters_deleted_counter_shadowing() {
    let manifest = "cass.cql_types.counters.deleted_counter_shadowing";
    let prefix = "ct_deleted_counter_shadowing";

    // Explicit, focused shadowing assertion (in addition to the generic driver):
    // pk1 must be ABSENT from the merged result, pk2 must equal 33.
    if let Some(dir) = find_table_dir(prefix) {
        if has_binary_data_db(&dir) {
            let actual = read_and_merge_counters(&dir).await;
            assert!(
                !actual.contains_key(&1),
                "[{manifest}] {prefix}: pk=1 was DELETED in Cassandra (partition tombstone) but \
                 CQLite still reports it as {:?} — deleted counter not shadowed",
                actual.get(&1)
            );
            assert_eq!(
                actual.get(&2).copied(),
                Some(33),
                "[{manifest}] {prefix}: pk=2 should survive at 33"
            );
        }
    }

    run_fixture(manifest, prefix).await;
}

/// Manifest: cass.cql_types.counters.compacted_final_value
///
/// `nodetool compact` left only the compacted generation (nb-3). Its counter
/// cells already hold the merged total (pk1=210, pk2=40).
#[tokio::test]
async fn counters_compacted_final_value() {
    run_fixture(
        "cass.cql_types.counters.compacted_final_value",
        "ct_compacted_final_value",
    )
    .await;
}

/// Manifest: cass.cql_types.counters.canonical_jsonl_value
///
/// The committed sstabledump JSONL goldens render counter cells as RAW
/// CounterContext shard state (a large i64 such as `422212677445164`, but the
/// exact integer is regen-dependent because the context embeds a per-node UUID).
/// This lane proves two things, dynamically (never against a fixed constant):
///   1. the golden carries raw shard renderings for its counter cells (so the
///      JSONL value is internal shard state, not the user total), and
///   2. CQLite's decoded/merged value for the SAME partitions is NOT in that set
///      of raw shard renderings and equals the sidecar — i.e. CQLite decodes the
///      CounterContext rather than passing the raw bytes through.
#[tokio::test]
async fn counters_canonical_jsonl_value() {
    let manifest = "cass.cql_types.counters.canonical_jsonl_value";

    // Use the single-SSTable fixture (one golden, unambiguous mapping).
    let prefix = "ct_single_sstable";
    let Some(dir) = find_table_dir(prefix) else {
        skip_or_fail(
            &format!("test_types.{prefix}"),
            &format!("[{manifest}] table directory not found under the datasets root"),
        );
        return;
    };
    if !has_binary_data_db(&dir) {
        skip_or_fail(
            &format!("test_types.{prefix}"),
            &format!("[{manifest}] no binary Data.db (JSONL-only checkout)"),
        );
        return;
    }

    // (1) The golden carries raw shard renderings for its counter cells.
    let gens = generation_prefixes(&dir);
    let mut total_raw_hits = 0usize;
    for g in &gens {
        total_raw_hits += golden_raw_shard_cell_count(&dir, g);
    }
    assert!(
        total_raw_hits > 0,
        "[{manifest}] {prefix}: expected the JSONL golden to render counter cells as raw \
         shard state, found none — golden assumption broken"
    );

    // The dynamic set of raw shard renderings actually present in the golden
    // (read from the committed golden, regen-stable; not a hardcoded integer).
    let raw_shard_values = golden_raw_value_set(&dir);
    assert!(
        !raw_shard_values.is_empty(),
        "[{manifest}] {prefix}: golden raw shard value set is empty despite raw hits"
    );

    // (2) CQLite's decoded/merged values are NOT in the golden's raw shard set
    //     and match the authoritative sidecar.
    let sidecar = find_sidecar(&dir);
    let (expected_rows, _) = parse_counter_select_sidecar(&sidecar);
    let sidecar_values: BTreeSet<i128> = expected_rows.iter().map(|(_, c)| *c as i128).collect();

    // Soundness: the golden's raw shard renderings and the sidecar's logical
    // totals must be DISJOINT — raw renderings are huge (~4e14), logical totals
    // are tiny — which proves the golden carries shard state, not decoded values.
    assert!(
        raw_shard_values.is_disjoint(&sidecar_values),
        "[{manifest}] {prefix}: golden raw shard set {raw_shard_values:?} overlaps the sidecar \
         logical totals {sidecar_values:?} — golden does not carry distinct shard state"
    );

    let actual = read_and_merge_counters(&dir).await;
    for (pk, exp_c) in expected_rows {
        let got = actual.get(&pk).copied();
        assert_eq!(
            got,
            Some(exp_c),
            "[{manifest}] {prefix}: pk={pk} merged value {got:?} != sidecar {exp_c}"
        );
        if let Some(g) = got {
            assert!(
                !raw_shard_values.contains(&(g as i128)),
                "[{manifest}] {prefix}: pk={pk} returned a raw shard rendering {g} \
                 (one of {raw_shard_values:?}) instead of the decoded total"
            );
        }
    }
}
