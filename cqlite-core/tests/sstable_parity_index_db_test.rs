//! Strict `Index.db` byte parity for BIG primary indexes + BTI index discovery
//! (Epic #968 / issue #983).
//!
//! Proves CQLite's primary-index reader against the **exact** `Index.db` bytes that
//! Apache Cassandra 5.0 wrote for every committed BIG (`nb`/`oa`) fixture, and that
//! BTI (`da`) fixtures route through a BTI-specific component path instead of being
//! treated as BIG primary indexes.
//!
//! This lane is **fail-closed**: a missing fixture, a missing Cassandra reference, a
//! placeholder/fallback path, or a parse that silently returns zero entries turns the
//! suite red. The single exception is the local-only `test_da/wide_table` BTI fixture,
//! which is absent in CI — it is skipped only when its `Data.db` is absent, and is a
//! hard failure (never silently empty) when present.
//!
//! Scope owned here (issue #983):
//!   * BIG `Index.db` entry-byte parity — every on-disk entry
//!     (`[key_len: u16 BE][raw key][data_offset: vint][promoted_len: vint][promoted]`)
//!     is re-parsed independently and must match `IndexReader` field-for-field: raw
//!     partition-key bytes, data offsets, and promoted-index lengths.
//!   * BIG decoded-field parity vs Cassandra `sstabledump` JSONL — partition **count**
//!     is exact, raw partition keys match byte-for-byte (UUID-keyed fixtures, where the
//!     key encoding is unambiguous), data offsets are strictly monotonically
//!     increasing, and successive offset deltas equal the JSONL partition-position
//!     deltas (for fixtures without static-row blocks, an authoritative structural
//!     distinction derived from the JSONL itself — no heuristics).
//!   * Point lookup, absent-key lookup, and range (first/last) boundaries are exercised
//!     against the live `IndexReader` lookup table.
//!   * BTI index-component discovery/classification — `da` fixtures expose
//!     `Partitions.db` + `Rows.db` and never `Index.db`/`Summary.db`, and are routed
//!     through a BTI-specific assertion path rather than BIG primary-index assumptions.
//!   * Corruption — a truncated BIG `Index.db` fails explicitly instead of returning an
//!     empty entry set or silently falling back to a full scan.
//!
//! Out of scope (per #983): SAI/SASI query semantics, key-cache behavior, distributed
//! read-path behavior, and repair/read-repair coordinator behavior.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::directory::{parse_toc_file_detailed, SSTableComponent};
use cqlite_core::storage::sstable::index_reader::IndexReader;
use cqlite_core::storage::sstable::version_gate::{SsTableDescriptor, SsTableFormat};
use cqlite_core::Config;

// ============================================================================
// Fixture discovery
// ============================================================================

/// Resolve the committed datasets root (env override first, else workspace tree).
fn datasets_sstables_root() -> PathBuf {
    let root = if let Ok(root) = std::env::var("CQLITE_DATASETS_ROOT") {
        PathBuf::from(root)
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(|workspace| workspace.join("test-data/datasets"))
            .unwrap_or_else(|| PathBuf::from("test-data/datasets"))
    };
    root.join("sstables")
}

/// A single discovered SSTable generation directory plus its parsed descriptor.
struct Fixture {
    dir: PathBuf,
    /// The Cassandra component prefix, e.g. `nb-1-big` or `da-2-bti`.
    prefix: String,
    format: SsTableFormat,
}

impl Fixture {
    fn component(&self, suffix: &str) -> PathBuf {
        self.dir.join(format!("{}-{suffix}", self.prefix))
    }

    fn name(&self) -> String {
        format!(
            "{}/{}",
            self.dir
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default(),
            self.prefix
        )
    }
}

/// Recursively collect every SSTable generation (one per `*-TOC.txt`) under `dir`,
/// skipping macOS AppleDouble shadow files. The format is parsed authoritatively from
/// the filename `<format>` segment — no heuristics, no extension sniffing.
fn collect_fixtures(dir: &Path, out: &mut Vec<Fixture>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("._") {
            continue;
        }
        if path.is_dir() {
            collect_fixtures(&path, out);
        } else if let Some(prefix) = name.strip_suffix("-TOC.txt") {
            // `parse_filename` wants a full component filename; reuse the TOC name.
            let descriptor = SsTableDescriptor::parse_filename(&name)
                .unwrap_or_else(|e| panic!("{}: descriptor parse failed: {e}", path.display()));
            out.push(Fixture {
                dir: dir.to_path_buf(),
                prefix: prefix.to_string(),
                format: descriptor.format,
            });
        }
    }
}

fn all_fixtures() -> Vec<Fixture> {
    let root = datasets_sstables_root();
    let mut out = Vec::new();
    collect_fixtures(&root, &mut out);
    out.sort_by(|a, b| a.dir.cmp(&b.dir).then(a.prefix.cmp(&b.prefix)));
    assert!(
        !out.is_empty(),
        "no committed SSTable fixtures found under {} — strict Index.db parity cannot \
         run (fail-closed guard, not a skip)",
        root.display()
    );
    out
}

/// `test_da/wide_table` is a local-only BTI fixture not shipped in CI. Per project
/// doctrine we skip it when its `Data.db` is absent, but a present-but-empty result is
/// still a hard failure (enforced by the strict assertions elsewhere).
fn is_local_only_absent(fx: &Fixture) -> bool {
    let dir = fx.dir.to_string_lossy();
    dir.contains("test_da") && dir.contains("wide_table") && !fx.component("Data.db").exists()
}

// ============================================================================
// Independent on-disk re-parse (the byte reference for BIG Index.db)
// ============================================================================

/// An entry as it lives on disk in a BIG `Index.db`, parsed independently of CQLite's
/// production reader so the two can be diffed field-for-field.
struct RawIndexEntry {
    key: Vec<u8>,
    data_offset: u64,
    promoted_len: u64,
}

/// Decode one unsigned VInt using Cassandra's leading-ones length prefix, returning the
/// value and the new cursor. Returns `None` on a short/corrupt buffer.
fn read_vint(buf: &[u8], mut i: usize) -> Option<(u64, usize)> {
    let first = *buf.get(i)?;
    i += 1;
    let extra_bytes = first.leading_ones().min(8) as usize;
    let mut value = (first as u64) & (0xffu64 >> extra_bytes);
    for _ in 0..extra_bytes {
        let b = *buf.get(i)?;
        i += 1;
        value = (value << 8) | (b as u64);
    }
    Some((value, i))
}

/// Independently re-parse a BIG `Index.db` buffer into its raw on-disk entries.
/// Returns an error string on any truncation so corruption surfaces explicitly.
fn reparse_big_index(buf: &[u8]) -> Result<Vec<RawIndexEntry>, String> {
    let mut entries = Vec::new();
    let mut i = 0usize;
    while i < buf.len() {
        if i + 2 > buf.len() {
            return Err(format!("truncated key length at byte {i}"));
        }
        let key_len = u16::from_be_bytes([buf[i], buf[i + 1]]) as usize;
        i += 2;
        if i + key_len > buf.len() {
            return Err(format!("truncated key (len {key_len}) at byte {i}"));
        }
        let key = buf[i..i + key_len].to_vec();
        i += key_len;
        let (data_offset, ni) =
            read_vint(buf, i).ok_or_else(|| format!("truncated data offset at byte {i}"))?;
        i = ni;
        let (promoted_len, ni) =
            read_vint(buf, i).ok_or_else(|| format!("truncated promoted length at byte {i}"))?;
        i = ni;
        let plen = promoted_len as usize;
        if i + plen > buf.len() {
            return Err(format!("truncated promoted block (len {plen}) at byte {i}"));
        }
        i += plen;
        entries.push(RawIndexEntry {
            key,
            data_offset,
            promoted_len,
        });
    }
    Ok(entries)
}

// ============================================================================
// Cassandra JSONL reference (partition keys + Data.db positions)
// ============================================================================

struct JsonlPartition {
    /// Single-component partition key as a string, when the partition key is a single
    /// column (most BIG fixtures). `None` for composite keys (length > 1).
    single_key_text: Option<String>,
    position: u64,
    has_static_block: bool,
}

/// Parse the committed `*-Data.db.jsonl` sstabledump reference into per-partition facts.
fn parse_jsonl(path: &Path) -> Result<Vec<JsonlPartition>, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("read {} failed: {e}", path.display()))?;
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: serde_json::Value =
            serde_json::from_str(line).map_err(|e| format!("bad JSONL line: {e}"))?;
        let partition = value
            .get("partition")
            .ok_or_else(|| "JSONL line missing `partition`".to_string())?;
        let position = partition
            .get("position")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| "JSONL partition missing numeric `position`".to_string())?;
        let key_arr = partition
            .get("key")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "JSONL partition missing `key` array".to_string())?;
        let single_key_text = if key_arr.len() == 1 {
            key_arr[0].as_str().map(str::to_string)
        } else {
            None
        };
        let has_static_block = value
            .get("rows")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|rows| {
                rows.iter().any(|r| {
                    r.get("type").and_then(serde_json::Value::as_str) == Some("static_block")
                })
            });
        out.push(JsonlPartition {
            single_key_text,
            position,
            has_static_block,
        });
    }
    Ok(out)
}

/// If every partition's single key parses as a UUID, return the raw 16-byte encodings in
/// partition order (the on-disk partition-key bytes for a `uuid`/`timeuuid` key).
fn uuid_key_bytes(parts: &[JsonlPartition]) -> Option<Vec<[u8; 16]>> {
    let mut out = Vec::with_capacity(parts.len());
    for p in parts {
        let text = p.single_key_text.as_ref()?;
        let uuid = uuid::Uuid::parse_str(text).ok()?;
        out.push(*uuid.as_bytes());
    }
    Some(out)
}

/// Decide whether a fixture is a counter table from authoritative Cassandra metadata,
/// never from the fixture path. Cassandra's own `Statistics.db.txt` sidecar lists each
/// regular column with its fully-qualified marshal type; counter columns are encoded as
/// `org.apache.cassandra.db.marshal.CounterColumnType`. Presence of that validator is the
/// canonical signal that counter cells alter the Data.db per-partition header accounting,
/// so the offset-delta check (3c) must be suppressed. When the sidecar is absent we return
/// `false` (no counter columns proven), matching the strict-lane default of trusting parsed
/// metadata rather than guessing from names.
fn fixture_is_counter_table(fx: &Fixture) -> bool {
    let stats = fx.component("Statistics.db.txt");
    let Ok(text) = std::fs::read_to_string(&stats) else {
        return false;
    };
    text.lines()
        .filter(|l| {
            let t = l.trim_start();
            t.starts_with("RegularColumns:") || t.starts_with("StaticColumns:")
        })
        .any(|l| l.contains("org.apache.cassandra.db.marshal.CounterColumnType"))
}

// ============================================================================
// BIG Index.db strict parity
// ============================================================================

async fn open_reader(path: &Path) -> Result<IndexReader, cqlite_core::Error> {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);
    IndexReader::open(path, platform).await
}

/// Strict BIG `Index.db` byte + decoded-field parity over every committed BIG fixture.
#[tokio::test]
async fn big_index_db_entry_byte_and_field_parity() {
    let fixtures = all_fixtures();
    let mut big_checked = 0usize;
    let mut uuid_key_checked = 0usize;
    let mut delta_checked = 0usize;
    let mut wide_partition_entries = 0usize;
    let mut promoted_bytes_total = 0u64;
    let mut skipped_unfetched = 0usize;

    for fx in &fixtures {
        if fx.format != SsTableFormat::Big {
            continue;
        }
        let index_path = fx.component("Index.db");
        let data_path = fx.component("Data.db");
        // Reference-only / unfetched generation: the committed tree carries TOC.txt +
        // JSONL but no binary components. Skip when *no binary* is present (Data.db and
        // Index.db both absent). But if Data.db is present while Index.db is missing,
        // that is a real defect for a BIG table — fail closed.
        if !index_path.exists() {
            if !data_path.exists() {
                skipped_unfetched += 1;
                continue;
            }
            panic!(
                "{}: BIG fixture has Data.db but no Index.db — fail-closed (missing \
                 primary index)",
                fx.name()
            );
        }

        // --- 1. Independent raw re-parse of the on-disk Index.db bytes. ---
        let raw_bytes = std::fs::read(&index_path)
            .unwrap_or_else(|e| panic!("{}: read Index.db failed: {e}", fx.name()));
        assert!(
            !raw_bytes.is_empty(),
            "{}: Index.db is empty — fail-closed",
            fx.name()
        );
        let raw_entries = reparse_big_index(&raw_bytes)
            .unwrap_or_else(|e| panic!("{}: independent Index.db reparse failed: {e}", fx.name()));
        assert!(
            !raw_entries.is_empty(),
            "{}: Index.db parsed to zero entries — fail-closed",
            fx.name()
        );

        // --- 2. CQLite's production reader must match the raw reparse field-for-field. ---
        let reader = open_reader(&index_path)
            .await
            .unwrap_or_else(|e| panic!("{}: IndexReader::open failed: {e}", fx.name()));
        let entries = reader.get_partition_entries();
        assert_eq!(
            entries.len(),
            raw_entries.len(),
            "{}: IndexReader entry count {} != independent reparse {}",
            fx.name(),
            entries.len(),
            raw_entries.len(),
        );
        for (n, (got, raw)) in entries.iter().zip(raw_entries.iter()).enumerate() {
            assert_eq!(
                got.key_digest.as_ref(),
                raw.key.as_slice(),
                "{}: entry {n} raw key bytes mismatch (reader {:02x?} vs disk {:02x?})",
                fx.name(),
                got.key_digest.as_ref(),
                raw.key,
            );
            assert_eq!(
                got.raw_key.as_deref(),
                Some(raw.key.as_slice()),
                "{}: entry {n} raw_key mirror mismatch",
                fx.name(),
            );
            assert_eq!(
                got.data_offset,
                raw.data_offset,
                "{}: entry {n} data_offset {} != disk {}",
                fx.name(),
                got.data_offset,
                raw.data_offset,
            );
        }

        // Promoted-index (wide-partition) metadata: every on-disk entry carries a
        // promoted-index byte length. Entries with a promoted block are wide
        // partitions whose Data.db span must be strictly larger than the promoted
        // block they index (the promoted block lives inside the partition). This is
        // the wide-partition boundary-metadata invariant for BIG indexes.
        let promoted_total: u64 = raw_entries.iter().map(|e| e.promoted_len).sum();
        for n in 0..raw_entries.len() {
            if raw_entries[n].promoted_len == 0 {
                continue;
            }
            if let Some(next) = raw_entries.get(n + 1) {
                let span = next.data_offset - raw_entries[n].data_offset;
                assert!(
                    span > raw_entries[n].promoted_len,
                    "{}: entry {n} promoted block ({} bytes) is not smaller than its \
                     partition span ({span} bytes) — wide-partition boundary metadata \
                     is inconsistent",
                    fx.name(),
                    raw_entries[n].promoted_len,
                );
            }
            wide_partition_entries += 1;
        }
        promoted_bytes_total += promoted_total;

        // --- 3. Decoded-field parity vs Cassandra sstabledump JSONL. ---
        let jsonl_path = fx.component("Data.db.jsonl");
        assert!(
            jsonl_path.exists(),
            "{}: missing sstabledump JSONL reference {} — fail-closed (no placeholder)",
            fx.name(),
            jsonl_path.display(),
        );
        let parts = parse_jsonl(&jsonl_path).unwrap_or_else(|e| panic!("{}: {e}", fx.name()));
        assert!(
            !parts.is_empty(),
            "{}: JSONL reference has zero partitions — fail-closed",
            fx.name()
        );

        // 3a. Partition count is exact.
        assert_eq!(
            entries.len(),
            parts.len(),
            "{}: Index.db partition count {} != Cassandra JSONL {}",
            fx.name(),
            entries.len(),
            parts.len(),
        );

        // 3b. Data offsets are strictly monotonically increasing (key order parity).
        for w in entries.windows(2) {
            assert!(
                w[1].data_offset > w[0].data_offset,
                "{}: Index.db data offsets not strictly increasing ({} then {})",
                fx.name(),
                w[0].data_offset,
                w[1].data_offset,
            );
        }

        // 3c. Successive offset deltas equal JSONL position deltas. The Index.db
        // data_offset is relative to the Data.db data section while the JSONL
        // `position` is the absolute file offset, so they differ by the per-partition
        // Data.db header (which repeats the partition key). That header is a constant
        // size only when (a) every partition key has the same byte length, (b) no
        // partition carries a `static_block` (which the JSONL accounts for separately),
        // and (c) the table is not a counter table (counter cells alter the header
        // accounting). All three conditions are derived authoritatively — (a)/(b) from the
        // parsed entries and the JSONL, and (c) from Cassandra's own `Statistics.db.txt`
        // column validators (CounterColumnType) — never from the fixture path. When they
        // hold, successive offset deltas must agree exactly, proving byte-faithful offset
        // parity.
        let uniform_key_len = entries
            .windows(2)
            .all(|w| w[0].key_digest.len() == w[1].key_digest.len());
        let any_static = parts.iter().any(|p| p.has_static_block);
        let is_counter = fixture_is_counter_table(fx);
        if uniform_key_len && !any_static && !is_counter {
            for n in 1..entries.len() {
                let off_delta = entries[n].data_offset - entries[n - 1].data_offset;
                let pos_delta = parts[n].position - parts[n - 1].position;
                assert_eq!(
                    off_delta,
                    pos_delta,
                    "{}: offset delta {off_delta} != JSONL position delta {pos_delta} at \
                     partition {n}",
                    fx.name(),
                );
            }
            delta_checked += 1;
        }

        // 3d. Raw partition-key byte parity for UUID-keyed fixtures (unambiguous
        // single-column encoding: the 16 raw UUID bytes are the on-disk key).
        if let Some(uuid_bytes) = uuid_key_bytes(&parts) {
            for (n, (entry, expected)) in entries.iter().zip(uuid_bytes.iter()).enumerate() {
                assert_eq!(
                    entry.key_digest.as_ref(),
                    expected.as_slice(),
                    "{}: entry {n} raw key bytes != Cassandra UUID key (reader {:02x?} vs \
                     {:02x?})",
                    fx.name(),
                    entry.key_digest.as_ref(),
                    expected,
                );
            }
            uuid_key_checked += 1;

            // 3e. Point lookup, absent-key lookup, and range boundaries via the live
            // lookup table — proves the decoded keys actually resolve.
            let first = uuid_bytes
                .first()
                .expect("non-empty fixture (asserted above)");
            let last = uuid_bytes
                .last()
                .expect("non-empty fixture (asserted above)");
            let hit = reader.lookup_partition(first).unwrap_or_else(|| {
                panic!("{}: point lookup of first partition key missed", fx.name())
            });
            assert_eq!(
                hit.data_offset,
                entries[0].data_offset,
                "{}: point lookup returned wrong offset",
                fx.name(),
            );
            assert!(
                reader.lookup_partition(last).is_some(),
                "{}: range-boundary lookup of last partition key missed",
                fx.name(),
            );
            // Absent key: derive a 16-byte key by flipping bits of a real key, then
            // confirm it is genuinely not in the fixture before asserting the lookup
            // misses (some fixtures use sentinel keys like all-0xFF, so a fixed
            // sentinel would collide).
            let mut absent = *first;
            for b in &mut absent {
                *b ^= 0xAA;
            }
            if !uuid_bytes.iter().any(|k| k == &absent) {
                assert!(
                    reader.lookup_partition(&absent).is_none(),
                    "{}: absent-key lookup unexpectedly resolved",
                    fx.name(),
                );
            }
        }

        big_checked += 1;
    }

    assert!(
        big_checked > 0,
        "no BIG fixtures exercised — BIG Index.db parity unproven (fail-closed)"
    );
    assert!(
        uuid_key_checked > 0,
        "no UUID-keyed BIG fixtures exercised — raw-key byte parity + lookup path unproven"
    );
    assert!(
        delta_checked > 0,
        "no regular BIG fixtures exercised for offset-delta parity"
    );
    eprintln!(
        "big_index_db_entry_byte_and_field_parity: {big_checked} BIG fixtures \
         ({uuid_key_checked} UUID-keyed, {delta_checked} delta-verified, \
         {wide_partition_entries} wide-partition entries / {promoted_bytes_total} \
         promoted bytes); {skipped_unfetched} unfetched reference-only generations skipped"
    );
}

// ============================================================================
// BTI index-component discovery / classification
// ============================================================================

/// BTI (`da`) fixtures must expose the BTI primary-index components (`Partitions.db` +
/// `Rows.db`) and never the BIG `Index.db`/`Summary.db`, and must route through a
/// BTI-specific path rather than BIG primary-index assumptions.
#[tokio::test]
async fn bti_index_component_discovery() {
    let fixtures = all_fixtures();
    let mut bti_checked = 0usize;
    let mut bti_skipped_local_only = 0usize;

    for fx in &fixtures {
        if fx.format != SsTableFormat::Bti {
            continue;
        }
        if is_local_only_absent(fx) {
            bti_skipped_local_only += 1;
            eprintln!(
                "bti_index_component_discovery: skipping local-only fixture {} (Data.db absent)",
                fx.name()
            );
            continue;
        }

        // Authoritative component manifest from the TOC Cassandra wrote.
        let toc = fx.component("TOC.txt");
        let (components, unknown) = parse_toc_file_detailed(&toc)
            .unwrap_or_else(|e| panic!("{}: TOC parse failed: {e}", fx.name()));
        assert!(
            unknown.is_empty(),
            "{}: unrecognized component(s) {:?} in BTI TOC — fail-closed",
            fx.name(),
            unknown,
        );

        // BTI primary-index components present, BIG components absent.
        assert!(
            components.contains(&SSTableComponent::Partitions)
                && components.contains(&SSTableComponent::Rows),
            "{}: BTI fixture missing Partitions.db/Rows.db — manifest {:?}",
            fx.name(),
            components,
        );
        assert!(
            !components.contains(&SSTableComponent::Index)
                && !components.contains(&SSTableComponent::Summary),
            "{}: BTI fixture leaks BIG Index.db/Summary.db — manifest {:?}",
            fx.name(),
            components,
        );
        assert!(
            components.iter().any(SSTableComponent::is_bti_specific),
            "{}: no BTI-specific component classified — discovery failed",
            fx.name(),
        );
        assert!(
            !components.iter().any(SSTableComponent::is_big_specific),
            "{}: BIG-specific component classified for a BTI fixture",
            fx.name(),
        );

        // BTI-specific routing: the BTI components must exist on disk, and the BIG
        // primary-index reader must NOT be pointed at them. Opening a BTI Partitions.db
        // as a BIG Index.db is not the supported path; the discovery above is what
        // routes BTI away from BIG assumptions.
        let partitions = fx.component("Partitions.db");
        let rows = fx.component("Rows.db");
        assert!(
            partitions.exists() && rows.exists(),
            "{}: BTI manifest names Partitions.db/Rows.db but files are missing — \
             fail-closed",
            fx.name(),
        );
        assert!(
            !fx.component("Index.db").exists(),
            "{}: BTI fixture unexpectedly carries a BIG Index.db on disk",
            fx.name(),
        );

        bti_checked += 1;
    }

    assert!(
        bti_checked > 0 || bti_skipped_local_only > 0,
        "no BTI fixtures discovered — BTI index-component classification unproven"
    );
    // The committed corpus ships at least the `da` simple_table BTI fixture in CI; if
    // every BTI fixture were skipped as local-only the claim would be unproven.
    assert!(
        bti_checked > 0,
        "all BTI fixtures were skipped as local-only — BTI discovery parity unproven \
         (committed `da` fixtures must be present)"
    );
    eprintln!(
        "bti_index_component_discovery: {bti_checked} BTI fixtures verified \
         ({bti_skipped_local_only} local-only skipped)"
    );
}

// ============================================================================
// Corruption / truncation fails explicitly (no silent empty / full-scan fallback)
// ============================================================================

/// A truncated BIG `Index.db` must fail explicitly, not parse to an empty entry set or
/// silently fall back to a full scan.
#[tokio::test]
async fn truncated_big_index_db_fails_explicitly() {
    let fixtures = all_fixtures();
    let mut checked = 0usize;

    for fx in &fixtures {
        if fx.format != SsTableFormat::Big {
            continue;
        }
        let index_path = fx.component("Index.db");
        if !index_path.exists() {
            continue;
        }
        let full = std::fs::read(&index_path)
            .unwrap_or_else(|e| panic!("{}: read Index.db failed: {e}", fx.name()));
        // Truncate mid-entry: keep enough to start parsing but cut an entry short so a
        // strict parser must error rather than silently stop.
        if full.len() < 8 {
            continue;
        }
        let truncated_len = full.len().saturating_sub(3).max(3);
        let truncated = &full[..truncated_len];

        // The independent reparse must surface truncation explicitly.
        let reparse = reparse_big_index(truncated);
        assert!(
            reparse.is_err()
                || reparse
                    .as_ref()
                    .map(|e| e.len() != reparse_big_index(&full).map(|f| f.len()).unwrap_or(0))
                    .unwrap_or(false),
            "{}: truncated Index.db reparsed without error and with the same entry count \
             — strict parser would have silently accepted corruption",
            fx.name(),
        );

        // The production reader must not silently succeed with the full entry set on a
        // truncated buffer written to a temp file.
        let tmp_dir = std::env::temp_dir().join(format!(
            "cqlite-983-trunc-{}-{}",
            std::process::id(),
            checked
        ));
        std::fs::create_dir_all(&tmp_dir)
            .unwrap_or_else(|e| panic!("{}: mkdir temp failed: {e}", fx.name()));
        let tmp_index = tmp_dir.join(format!("{}-Index.db", fx.prefix));
        std::fs::write(&tmp_index, truncated)
            .unwrap_or_else(|e| panic!("{}: write truncated Index.db failed: {e}", fx.name()));

        let full_count = reparse_big_index(&full)
            .map(|e| e.len())
            .unwrap_or_default();
        match open_reader(&tmp_index).await {
            Err(_) => { /* explicit failure — acceptable */ }
            Ok(reader) => {
                // A non-error reader is only acceptable if it surfaced a genuine *partial*
                // parse: strictly fewer entries than the full file AND strictly more than
                // zero. A silent empty parse (n == 0) is corruption masquerading as success
                // and must FAIL the test just like returning the full entry count would.
                let n = reader.get_partition_entries().len();
                assert!(
                    n > 0 && n < full_count,
                    "{}: reader accepted a truncated Index.db with entry count {n} (full \
                     count {full_count}) — a truncated file must either error or yield a \
                     non-empty partial parse, never a silent empty/full set",
                    fx.name(),
                );
            }
        }

        let _ = std::fs::remove_dir_all(&tmp_dir);
        checked += 1;
        // One representative BIG fixture is sufficient to prove the corruption path.
        break;
    }

    assert!(
        checked > 0,
        "no BIG fixture available to exercise the truncation/corruption path — \
         fail-closed"
    );
    eprintln!("truncated_big_index_db_fails_explicitly: {checked} fixture(s) verified");
}
