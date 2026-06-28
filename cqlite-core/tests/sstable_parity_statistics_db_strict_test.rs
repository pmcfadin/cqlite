//! Strict `Statistics.db` core-metadata parity (Epic #968 / issue #985).
//!
//! Proves CQLite decodes the *core* SSTable metadata that Apache Cassandra 5.0
//! persisted into `Statistics.db`, byte-for-byte and field-for-field, against the
//! committed `*-Statistics.db.txt` reference dumps Cassandra's `sstablemetadata`
//! tool produced for every fixture — BIG (`nb`/`oa`) and BTI (`da`).
//!
//! Scope owned here (issue #985):
//!   * **Component-manifest / checksum parity** — the `Statistics.db` Table of
//!     Contents (component count + the four `MetadataType` entries
//!     VALIDATION/COMPACTION/STATS/HEADER, in order) and its two embedded CRC32
//!     checksums (the `crc32(num_components)` marker and the accumulated TOC CRC)
//!     are validated byte-for-byte. Corrupting any TOC byte breaks the CRC, which
//!     is how corruption fails closed with an explicit error in strict mode.
//!   * **Timestamp / TTL / local-deletion-time metadata** — the authoritative
//!     `EncodingStats` baselines (`minTimestamp`, `minLocalDeletionTime`,
//!     `minTTL`) CQLite delta-decodes from the binary are asserted equal to the
//!     exact integers in the reference dump.
//!   * **Serialization-header column metadata / ordering** — partition-key and
//!     clustering-key arity, clustering DESC (`ReversedType`) flags, and the set
//!     of regular-column names CQLite recovers from the embedded
//!     SerializationHeader match the reference dump. Exercised across primitive,
//!     collection, UDT, static, and reversed-clustering schemas in the corpus.
//!
//! Fail-closed contract:
//!   * The committed `*-Statistics.db.txt` references always exist, so the lane
//!     always has work; zero references found turns the lane red (not green).
//!   * The binary `*-Statistics.db` is a fetched fixture. When it is absent (CI
//!     without datasets), that single fixture is *skipped* (recorded), never
//!     silently counted as a pass. When present, every field is compared and any
//!     mismatch fails.
//!
//! Out of scope here (intentionally NOT claimed byte-for-byte, see manifest
//! `planned` entries and child issues):
//!   * `repairedAt` / `pendingRepair` repaired metadata — child issue #988.
//!   * `max` timestamp / `max` local-deletion-time, estimated histograms and
//!     partition/row-count estimates, and covered-clustering bounds: the minimal
//!     nb/oa/da Statistics parser does not yet decode the full STATS component,
//!     so these are not asserted here (tracked as `planned` manifest scenarios).

use std::path::{Path, PathBuf};

use cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback;

#[path = "parity_support/mod.rs"]
mod parity_support;
use parity_support::{
    parity_datasets_required, scenario, write_summary, LaneStatus, ParityFailure,
};

/// `crc32(num_components=4)` — the marker Cassandra writes at bytes 4..8 of
/// every `Statistics.db` (it is the CRC of the 4-byte component count, which is
/// always 4 for the VALIDATION/COMPACTION/STATS/HEADER set).
const STATISTICS_TOC_MARKER: u32 = 0x2629_1b05;

/// Cassandra `MetadataType` ordinals, in the order they appear in the TOC.
const EXPECTED_TOC_TYPES: [u32; 4] = [0, 1, 2, 3]; // VALIDATION, COMPACTION, STATS, HEADER

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

/// Recursively collect every committed `*-Statistics.db.txt` reference dump,
/// skipping macOS AppleDouble shadow files (`._*`).
fn collect_statistics_txt(dir: &Path, out: &mut Vec<PathBuf>) {
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
            collect_statistics_txt(&path, out);
        } else if name.ends_with("-Statistics.db.txt") {
            out.push(path);
        }
    }
}

fn all_statistics_txt() -> Vec<PathBuf> {
    let root = datasets_sstables_root();
    let mut out = Vec::new();
    collect_statistics_txt(&root, &mut out);
    out.sort();
    // Fail closed: a broken reference path must turn the strict lane red, not green.
    assert!(
        !out.is_empty(),
        "no committed *-Statistics.db.txt references found under {} — strict Statistics.db \
         parity cannot run (this is a fail-closed guard, not a skip)",
        root.display()
    );
    out
}

/// The fields decoded from a Cassandra `sstablemetadata` reference dump that this
/// strict lane compares against CQLite's decoder. Every value is sourced from the
/// authoritative parenthesised integer / explicit list — never inferred.
#[derive(Debug)]
struct ReferenceMetadata {
    min_timestamp: i64,
    min_local_deletion_time: i64,
    min_ttl: i64,
    /// Partition-key arity, or `None` when the KeyType is a `CompositeType`.
    ///
    /// The minimal nb/oa/da SerializationHeader decoder folds a composite
    /// partition key into a single synthetic column rather than splitting the
    /// `CompositeType(...)` components, so composite arity is not asserted
    /// byte-for-byte here (tracked as a `planned` manifest limitation). Single
    /// (non-composite) partition keys are asserted exactly.
    partition_key_arity: Option<usize>,
    clustering_arity: usize,
    /// Per-clustering-column reversed (DESC / `ReversedType`) flags, IN ORDER.
    ///
    /// Position `i` is `true` iff the `i`-th entry of the reference
    /// `ClusteringTypes: [...]` list is wrapped in
    /// `org.apache.cassandra.db.marshal.ReversedType(...)`. Length always equals
    /// `clustering_arity`. Comparing the ordered vector (not just the count)
    /// catches a regression that marks the WRONG clustering column reversed.
    clustering_reversed_flags: Vec<bool>,
    regular_column_names: Vec<String>,
}

/// Extract the trailing parenthesised integer, e.g. `... (1759713125983682)`.
fn paren_int(line: &str) -> Option<i64> {
    let open = line.rfind('(')?;
    let close = line[open..].find(')')? + open;
    line[open + 1..close].trim().parse().ok()
}

/// Extract a bare trailing integer after the last colon, e.g. `TTL min: 0` or
/// `EncodingStats minTTL: 86400 (1 day)` (the leading number).
fn trailing_int(line: &str) -> Option<i64> {
    let after = line.rsplit(':').next()?.trim();
    let token = after.split_whitespace().next()?;
    token.parse().ok()
}

/// Parse the comma-separated `name:type` pairs from a `RegularColumns:` /
/// `StaticColumns:` line, returning the bare column names.
///
/// The type half can itself contain commas (e.g. `MapType(A,B)`), so we split on
/// the *first* top-level comma between entries by tracking parenthesis depth.
fn parse_column_names(after_colon: &str) -> Vec<String> {
    let s = after_colon.trim();
    if s.is_empty() {
        return Vec::new();
    }
    let mut entries = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    let bytes = s.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b',' if depth == 0 => {
                entries.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    entries.push(&s[start..]);
    entries
        .into_iter()
        .filter_map(|e| {
            let e = e.trim();
            // name is everything before the first ':' (column names cannot contain ':')
            e.split_once(':').map(|(name, _)| name.trim().to_string())
        })
        .filter(|n| !n.is_empty())
        .collect()
}

/// Count comma-separated entries inside the `[...]` of a `ClusteringTypes:` line.
fn count_bracket_entries(line: &str) -> usize {
    let open = match line.find('[') {
        Some(o) => o,
        None => return 0,
    };
    let close = match line.rfind(']') {
        Some(c) => c,
        None => return 0,
    };
    if close <= open + 1 {
        return 0; // "[]" — empty
    }
    let inner = &line[open + 1..close];
    let mut depth = 0i32;
    let mut count = 1usize;
    for &b in inner.as_bytes() {
        match b {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b',' if depth == 0 => count += 1,
            _ => {}
        }
    }
    count
}

fn parse_reference(txt: &Path) -> ReferenceMetadata {
    let content = std::fs::read_to_string(txt)
        .unwrap_or_else(|e| panic!("read reference {} failed: {e}", txt.display()));

    let mut min_timestamp = None;
    let mut min_local_deletion_time = None;
    let mut min_ttl = None;
    let mut partition_key_arity = None;
    let mut clustering_arity = None;
    let mut clustering_reversed_flags: Vec<bool> = Vec::new();
    let mut regular_column_names: Option<Vec<String>> = None;

    for line in content.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("EncodingStats minTimestamp:") {
            // authoritative microsecond integer is in parentheses
            min_timestamp = paren_int(rest).or_else(|| paren_int(line));
        } else if let Some(rest) = line.strip_prefix("EncodingStats minLocalDeletionTime:") {
            min_local_deletion_time = paren_int(rest).or_else(|| paren_int(line));
        } else if line.starts_with("EncodingStats minTTL:") {
            min_ttl = trailing_int(line);
        } else if line.starts_with("KeyType:") {
            // A bare KeyType is a single partition column; CompositeType(...) is a
            // composite key the minimal decoder does not split (see field doc).
            partition_key_arity = Some(keytype_arity(line));
        } else if line.starts_with("ClusteringTypes:") {
            clustering_arity = Some(count_bracket_entries(line));
            clustering_reversed_flags = reversed_flags(line);
        } else if let Some(rest) = line.strip_prefix("RegularColumns:") {
            regular_column_names = Some(parse_column_names(rest));
        }
    }

    ReferenceMetadata {
        min_timestamp: min_timestamp
            .unwrap_or_else(|| panic!("{}: missing EncodingStats minTimestamp", txt.display())),
        min_local_deletion_time: min_local_deletion_time.unwrap_or_else(|| {
            panic!(
                "{}: missing EncodingStats minLocalDeletionTime",
                txt.display()
            )
        }),
        min_ttl: min_ttl
            .unwrap_or_else(|| panic!("{}: missing EncodingStats minTTL", txt.display())),
        partition_key_arity: partition_key_arity
            .unwrap_or_else(|| panic!("{}: missing KeyType", txt.display())),
        // (composite keys collapse to `None` above via keytype_arity)
        clustering_arity: clustering_arity
            .unwrap_or_else(|| panic!("{}: missing ClusteringTypes", txt.display())),
        clustering_reversed_flags,
        regular_column_names: regular_column_names.unwrap_or_default(),
    }
}

/// Partition-key arity from a `KeyType:` line. A bare comparator is a single
/// partition column (`Some(1)`); a `CompositeType(...)` is a composite key the
/// minimal decoder does not split, so we return `None` and skip the exact-arity
/// assertion for it (documented limitation).
fn keytype_arity(line: &str) -> Option<usize> {
    let after = match line.split_once(':') {
        Some((_, a)) => a.trim(),
        None => return Some(1),
    };
    let is_composite = after.starts_with("org.apache.cassandra.db.marshal.CompositeType(")
        || after.starts_with("CompositeType(");
    if is_composite {
        None
    } else {
        Some(1)
    }
}

/// Per-clustering-column reversed flags, IN ORDER, from a `ClusteringTypes:`
/// `[...]` line. Entry `i` is `true` iff the `i`-th top-level comparator is
/// wrapped in `ReversedType(...)` (DESC ordering). The returned vector length
/// equals the clustering arity, so callers can compare position-by-position
/// against the decoded `clustering_reversed` flags rather than only counts.
fn reversed_flags(line: &str) -> Vec<bool> {
    let open = match line.find('[') {
        Some(o) => o,
        None => return Vec::new(),
    };
    let close = match line.rfind(']') {
        Some(c) => c,
        None => return Vec::new(),
    };
    if close <= open + 1 {
        return Vec::new(); // "[]" — no clustering columns
    }
    let inner = &line[open + 1..close];
    // Split into top-level entries (parenthesis-depth aware), preserving order.
    let mut depth = 0i32;
    let mut start = 0usize;
    let mut entries: Vec<&str> = Vec::new();
    for (i, &b) in inner.as_bytes().iter().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b',' if depth == 0 => {
                entries.push(&inner[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    entries.push(&inner[start..]);
    entries
        .into_iter()
        .map(str::trim)
        .map(|e| {
            e.starts_with("org.apache.cassandra.db.marshal.ReversedType(")
                || e.starts_with("ReversedType(")
        })
        .collect()
}

/// Derive the binary `*-Statistics.db` path from its `*-Statistics.db.txt` dump.
fn binary_for(txt: &Path) -> PathBuf {
    let name = txt
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_else(|| panic!("non-UTF8 reference name: {}", txt.display()));
    let db_name = name
        .strip_suffix(".txt")
        .unwrap_or_else(|| panic!("reference not *.txt: {name}"));
    txt.with_file_name(db_name)
}

/// Strict, byte-for-byte validation of the `Statistics.db` Table of Contents and
/// its two embedded CRC32 checksums. Returns the four TOC component offsets.
///
/// This is the corruption gate: any flipped byte in the TOC (component count,
/// type, or offset) breaks the accumulated CRC and fails here with an explicit
/// metadata-corruption assertion — strict mode never accepts a placeholder.
fn validate_toc_and_checksums(bytes: &[u8], db: &Path) {
    assert!(
        bytes.len() >= 44,
        "{}: Statistics.db too small ({} bytes) to hold the metadata TOC — corrupt or truncated",
        db.display(),
        bytes.len(),
    );

    let num_components = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    assert_eq!(
        num_components,
        EXPECTED_TOC_TYPES.len() as u32,
        "{}: Statistics.db component count {} != 4 (VALIDATION/COMPACTION/STATS/HEADER) — corrupt metadata header",
        db.display(),
        num_components,
    );

    let marker = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    let expected_marker = crc32fast::hash(&bytes[0..4]);
    assert_eq!(
        marker, expected_marker,
        "{}: Statistics.db TOC marker 0x{:08x} != crc32(num_components) 0x{:08x} — corrupt metadata",
        db.display(),
        marker,
        expected_marker,
    );
    assert_eq!(
        marker,
        STATISTICS_TOC_MARKER,
        "{}: Statistics.db TOC marker 0x{:08x} != canonical Cassandra 0x{:08x}",
        db.display(),
        marker,
        STATISTICS_TOC_MARKER,
    );

    // The four TOC entries (type,offset) must be VALIDATION/COMPACTION/STATS/HEADER
    // in order, with strictly increasing offsets.
    let mut prev_off = 0u32;
    for (i, &expected_type) in EXPECTED_TOC_TYPES.iter().enumerate() {
        let s = 8 + i * 8;
        let ty = u32::from_be_bytes([bytes[s], bytes[s + 1], bytes[s + 2], bytes[s + 3]]);
        let off = u32::from_be_bytes([bytes[s + 4], bytes[s + 5], bytes[s + 6], bytes[s + 7]]);
        assert_eq!(
            ty,
            expected_type,
            "{}: TOC entry {} has MetadataType {} (expected {}) — corrupt metadata manifest",
            db.display(),
            i,
            ty,
            expected_type,
        );
        assert!(
            off > prev_off,
            "{}: TOC entry {} offset {} not strictly greater than previous {} — corrupt metadata",
            db.display(),
            i,
            off,
            prev_off,
        );
        assert!(
            (off as usize) < bytes.len(),
            "{}: TOC entry {} offset {} past end of file ({} bytes) — corrupt metadata",
            db.display(),
            i,
            off,
            bytes.len(),
        );
        prev_off = off;
    }

    // Accumulated CRC32 over [num_components ++ four TOC entries] at byte 40.
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&bytes[0..4]);
    for i in 0..EXPECTED_TOC_TYPES.len() {
        let s = 8 + i * 8;
        hasher.update(&bytes[s..s + 8]);
    }
    let expected_acc = hasher.finalize();
    let stored_acc = u32::from_be_bytes([bytes[40], bytes[41], bytes[42], bytes[43]]);
    assert_eq!(
        stored_acc, expected_acc,
        "{}: Statistics.db accumulated TOC CRC32 0x{:08x} != recomputed 0x{:08x} — corrupt metadata",
        db.display(),
        stored_acc,
        expected_acc,
    );
}

/// Strict core-metadata parity across every committed fixture.
///
/// Drives off the committed `*-Statistics.db.txt` references (always present);
/// for each, when the binary `*-Statistics.db` is fetched it compares CQLite's
/// decode against the reference field-for-field and byte-for-byte. Absent
/// binaries are recorded as skips (never silent passes).
#[test]
fn statistics_db_strict_core_metadata_parity() {
    let refs = all_statistics_txt();

    let mut compared = 0usize;
    let mut skipped = 0usize;
    // Coverage proof: assert the corpus exercised each interesting shape.
    let mut saw_ttl = false; // minTTL > 0
    let mut saw_tombstone = false; // minLocalDeletionTime != epoch sentinel
    let mut saw_clustering = false; // clustering_arity > 0
    let mut saw_reversed = false; // a DESC clustering column
    let mut saw_no_regular = false; // schema with zero regular columns
    let mut saw_multi_regular = false; // 2+ regular columns
    let mut format_nb = 0usize;
    let mut format_oa = 0usize;
    let mut format_da = 0usize;
    // Which formats have a fetched binary on disk, computed up-front so we can
    // require real coverage for every format the local dataset actually contains
    // (not just whichever ones happened to decode). Independent of decode success.
    let mut present_nb = false;
    let mut present_oa = false;
    let mut present_da = false;
    for txt in &refs {
        let db = binary_for(txt);
        if !db.exists() {
            continue;
        }
        match db.file_name().and_then(|n| n.to_str()) {
            Some(n) if n.starts_with("nb-") => present_nb = true,
            Some(n) if n.starts_with("oa-") => present_oa = true,
            Some(n) if n.starts_with("da-") => present_da = true,
            _ => {}
        }
    }

    for txt in &refs {
        let db = binary_for(txt);
        if !db.exists() {
            // Fetched fixture absent (e.g. CI without datasets): skip THIS one,
            // never count it as a pass.
            skipped += 1;
            continue;
        }

        let reference = parse_reference(txt);
        let bytes = std::fs::read(&db)
            .unwrap_or_else(|e| panic!("read binary {} failed: {e}", db.display()));

        // (1) Component-manifest + checksum byte parity (corruption gate).
        validate_toc_and_checksums(&bytes, &db);

        // (2) Decode and field-compare core metadata.
        let (_, stats) = parse_statistics_with_fallback(&bytes, None).unwrap_or_else(|e| {
            panic!(
                "{}: CQLite failed to decode Statistics.db core metadata: {e:?}",
                db.display()
            )
        });

        // Header marker mirrors the on-disk TOC marker.
        assert_eq!(
            stats.header.statistics_kind,
            STATISTICS_TOC_MARKER,
            "{}: decoded header marker 0x{:08x} != 0x{:08x}",
            db.display(),
            stats.header.statistics_kind,
            STATISTICS_TOC_MARKER,
        );

        // EncodingStats baselines: timestamp / local-deletion-time / TTL.
        assert_eq!(
            stats.timestamp_stats.min_timestamp,
            reference.min_timestamp,
            "{}: minTimestamp mismatch (cqlite {} vs cassandra {})",
            db.display(),
            stats.timestamp_stats.min_timestamp,
            reference.min_timestamp,
        );
        assert_eq!(
            stats.timestamp_stats.min_deletion_time,
            reference.min_local_deletion_time,
            "{}: minLocalDeletionTime mismatch (cqlite {} vs cassandra {})",
            db.display(),
            stats.timestamp_stats.min_deletion_time,
            reference.min_local_deletion_time,
        );
        let cqlite_min_ttl = stats.timestamp_stats.min_ttl.unwrap_or(0);
        assert_eq!(
            cqlite_min_ttl,
            reference.min_ttl,
            "{}: minTTL mismatch (cqlite {} vs cassandra {})",
            db.display(),
            cqlite_min_ttl,
            reference.min_ttl,
        );

        // Serialization-header column metadata / ordering.
        // Partition-key arity is asserted exactly for single (non-composite)
        // keys; composite keys (reference arity == None) are not split by the
        // minimal decoder, so we only require it recovered at least one key.
        match reference.partition_key_arity {
            Some(arity) => assert_eq!(
                stats.serialization_header_partition_keys.len(),
                arity,
                "{}: partition-key arity mismatch (cqlite {} vs cassandra {})",
                db.display(),
                stats.serialization_header_partition_keys.len(),
                arity,
            ),
            None => assert!(
                !stats.serialization_header_partition_keys.is_empty(),
                "{}: composite partition key not recovered (cqlite recovered 0 keys)",
                db.display(),
            ),
        }
        assert_eq!(
            stats.serialization_header_clustering_keys.len(),
            reference.clustering_arity,
            "{}: clustering arity mismatch (cqlite {} vs cassandra {})",
            db.display(),
            stats.serialization_header_clustering_keys.len(),
            reference.clustering_arity,
        );
        // Compare the per-clustering-column reversed (DESC / ReversedType) flags
        // IN ORDER — position-by-position — not just the total count. A regression
        // that marks the wrong clustering column reversed (same count, different
        // positions) must fail here.
        let cqlite_reversed_flags: Vec<bool> = stats
            .serialization_header_clustering_keys
            .iter()
            .map(|c| c.clustering_reversed)
            .collect();
        assert_eq!(
            cqlite_reversed_flags,
            reference.clustering_reversed_flags,
            "{}: reversed-clustering flag vector mismatch (cqlite {:?} vs cassandra {:?})",
            db.display(),
            cqlite_reversed_flags,
            reference.clustering_reversed_flags,
        );
        let reference_reversed_count = reference
            .clustering_reversed_flags
            .iter()
            .filter(|&&r| r)
            .count();

        // Regular-column name set (order- and type-mapping-independent).
        let mut cqlite_regular: Vec<String> = stats
            .serialization_header_columns
            .iter()
            .filter(|c| !c.is_static)
            .map(|c| c.name.clone())
            .collect();
        cqlite_regular.sort();
        let mut ref_regular = reference.regular_column_names.clone();
        ref_regular.sort();
        assert_eq!(
            cqlite_regular,
            ref_regular,
            "{}: regular-column name set mismatch (cqlite {:?} vs cassandra {:?})",
            db.display(),
            cqlite_regular,
            ref_regular,
        );

        // Coverage bookkeeping.
        if reference.min_ttl > 0 {
            saw_ttl = true;
        }
        // Cassandra's "no tombstones" sentinel is i64::MAX; anything finite that
        // is not the EncodingStats epoch (1442880000) is a real deletion time.
        if reference.min_local_deletion_time != 1_442_880_000 {
            saw_tombstone = true;
        }
        if reference.clustering_arity > 0 {
            saw_clustering = true;
        }
        if reference_reversed_count > 0 {
            saw_reversed = true;
        }
        if reference.regular_column_names.is_empty() {
            saw_no_regular = true;
        }
        if reference.regular_column_names.len() >= 2 {
            saw_multi_regular = true;
        }
        match db.file_name().and_then(|n| n.to_str()) {
            Some(n) if n.starts_with("nb-") => format_nb += 1,
            Some(n) if n.starts_with("oa-") => format_oa += 1,
            Some(n) if n.starts_with("da-") => format_da += 1,
            _ => {}
        }

        compared += 1;
    }

    eprintln!(
        "statistics_db_strict_core_metadata_parity: {compared} compared, {skipped} skipped \
         (binary absent) | formats nb={format_nb} oa={format_oa} da={format_da} \
         | present nb={present_nb} oa={present_oa} da={present_da}"
    );

    let any_present = present_nb || present_oa || present_da;
    if !any_present {
        // No fetched binaries in this environment: the lane has nothing to assert
        // against. This is the documented dataset-absent SKIP path (distinct from a
        // silent pass); the committed references were still validated for presence
        // above (all_statistics_txt fails closed).
        if parity_datasets_required() {
            ParityFailure::new(scenario::STATISTICS_DB)
                .lane("statistics_db")
                .cassandra_source("MetadataSerializer (Statistics.db core metadata)")
                .fixture(datasets_sstables_root())
                .components(["Statistics.db", "Statistics.db.txt"])
                .repro(
                    "bash test-data/scripts/fetch-datasets.sh && \
                     CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test -p cqlite-core \
                     --features write-support --test sstable_parity_statistics_db_strict_test \
                     statistics_db_strict_core_metadata_parity -- --nocapture",
                )
                .detail(format!(
                    "CQLITE_PARITY_REQUIRE_DATASETS=1 but no *-Statistics.db binaries were fetched \
                     ({skipped} references present without binaries) — required parity gate must \
                     not skip when datasets are mandated"
                ))
                .panic();
        }
        eprintln!(
            "statistics_db_strict_core_metadata_parity: SKIP — no *-Statistics.db binaries \
             fetched ({skipped} references present without binaries)"
        );
        return;
    }

    // Binaries ARE present: the lane must actually have compared something and must
    // have exercised every storage format the local dataset contains. A green run
    // with zero comparisons (or with a present format left uncompared) is a false pass.
    assert!(
        compared > 0,
        "Statistics.db binaries are present (nb={present_nb} oa={present_oa} da={present_da}) \
         but zero fixtures were compared — strict parity lane proved nothing"
    );
    if present_nb {
        assert!(
            format_nb > 0,
            "nb-* Statistics.db binaries are present but none were compared \
             (format_nb=0) — nb parity unproven"
        );
    }
    if present_oa {
        assert!(
            format_oa > 0,
            "oa-* Statistics.db binaries are present but none were compared \
             (format_oa=0) — oa parity unproven"
        );
    }
    if present_da {
        assert!(
            format_da > 0,
            "da-* Statistics.db binaries are present but none were compared \
             (format_da=0) — da parity unproven"
        );
    }

    // With binaries present, the corpus must exercise the metadata shapes the
    // acceptance criteria call out; otherwise the parity claim is unproven.
    assert!(
        saw_ttl,
        "no fixture with minTTL > 0 — TTL metadata parity unproven"
    );
    assert!(
        saw_tombstone,
        "no fixture with a non-epoch minLocalDeletionTime — deletion-time parity unproven"
    );
    assert!(
        saw_clustering,
        "no fixture with clustering columns — clustering-key metadata parity unproven"
    );
    assert!(
        saw_reversed,
        "no fixture with a DESC (ReversedType) clustering column — ordering parity unproven"
    );
    assert!(
        saw_no_regular,
        "no fixture without regular columns — empty-regular-column parity unproven"
    );
    assert!(
        saw_multi_regular,
        "no fixture with multiple regular columns — multi-column parity unproven"
    );

    let _ = write_summary(
        "statistics_db",
        LaneStatus::Pass,
        scenario::STATISTICS_DB,
        &[],
    );
}

/// Corrupted `Statistics.db` fixtures fail closed with explicit metadata-corruption
/// errors in strict mode — never silently accepted, never coerced to a placeholder.
///
/// Uses a fetched binary as the clean baseline (skips when none is fetched), then
/// mutates the metadata TOC and asserts the strict checksum gate rejects it.
#[test]
fn statistics_db_strict_corruption_fails_closed() {
    let refs = all_statistics_txt();
    let Some(db) = refs.iter().map(|p| binary_for(p)).find(|p| p.exists()) else {
        if parity_datasets_required() {
            ParityFailure::new(scenario::STATISTICS_DB)
                .lane("statistics_db")
                .cassandra_source("MetadataSerializer corruption rejection (Statistics.db)")
                .fixture(datasets_sstables_root())
                .components(["Statistics.db"])
                .repro(
                    "bash test-data/scripts/fetch-datasets.sh && \
                     CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test -p cqlite-core \
                     --features write-support --test sstable_parity_statistics_db_strict_test \
                     statistics_db_strict_corruption_fails_closed -- --nocapture",
                )
                .detail(
                    "CQLITE_PARITY_REQUIRE_DATASETS=1 but no *-Statistics.db binary was fetched to \
                     use as the corruption baseline — required parity gate must not skip when \
                     datasets are mandated",
                )
                .panic();
        }
        eprintln!(
            "statistics_db_strict_corruption_fails_closed: SKIP — no *-Statistics.db binary fetched"
        );
        return;
    };

    let clean = std::fs::read(&db).unwrap_or_else(|e| panic!("read {} failed: {e}", db.display()));

    // Sanity: the clean fixture passes the strict gate.
    validate_toc_and_checksums(&clean, &db);

    // (a) Flip a byte inside a TOC offset → accumulated CRC must reject it.
    {
        let mut corrupt = clean.clone();
        corrupt[15] ^= 0xff; // last byte of the first TOC entry's offset
        let result =
            std::panic::catch_unwind(|| validate_toc_and_checksums(&corrupt, Path::new("corrupt")));
        assert!(
            result.is_err(),
            "strict gate accepted a Statistics.db with a corrupted TOC offset — must fail closed"
        );
    }

    // (b) Corrupt the component count → explicit count/marker rejection.
    {
        let mut corrupt = clean.clone();
        corrupt[3] = 0x07; // num_components = 7
        let result =
            std::panic::catch_unwind(|| validate_toc_and_checksums(&corrupt, Path::new("corrupt")));
        assert!(
            result.is_err(),
            "strict gate accepted a Statistics.db with a corrupted component count — must fail closed"
        );
    }

    // (c) Truncate below the TOC → explicit too-small rejection (the decoder also
    // refuses to fabricate EncodingStats from a truncated buffer).
    {
        let truncated = &clean[..16.min(clean.len())];
        let result = std::panic::catch_unwind(|| {
            validate_toc_and_checksums(truncated, Path::new("truncated"))
        });
        assert!(
            result.is_err(),
            "strict gate accepted a truncated Statistics.db — must fail closed"
        );
        assert!(
            parse_statistics_with_fallback(truncated, None).is_err(),
            "decoder accepted a truncated Statistics.db instead of erroring"
        );
    }

    eprintln!(
        "statistics_db_strict_corruption_fails_closed: corruption rejected against {}",
        db.display()
    );
}
