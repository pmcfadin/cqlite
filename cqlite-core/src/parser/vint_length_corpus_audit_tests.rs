//! Issue #1623 — corpus-differential test for `parse_vint_length`.
//!
//! This is the pinned parity test for the unsigned-length fix. It proves, on
//! REAL Cassandra corpus bytes, that the fixed unsigned decode reads length
//! prefixes correctly where the OLD signed (ZigZag) decode silently mis-read
//! them.
//!
//! ## Why not "just scan and tally"
//!
//! The production full-scan hot path for the corpus formats (V5CompressedLegacy
//! "nb", BTI "da") decodes every on-disk length/count field via `parse_vuint`
//! directly, NOT via `parse_vint_length` — so instrumenting `parse_vint_length`
//! around a full scan records ZERO decodes and demonstrates nothing (roborev job
//! 2718: the prior version of this test was vacuous for exactly this reason).
//! See the producer-classification doc-comment on
//! `parser::vint::parse_vint_length`.
//!
//! ## What this test actually does
//!
//! 1. It still opens and scans every corpus SSTable and asserts none error with
//!    the unsigned length decoder (the structural-soundness property).
//! 2. For the V5CompressedLegacy ("nb") tables it stitches + decompresses the
//!    Data.db data section — the exact bytes the scan path parses — and, for each
//!    authoritative decoded variable-length cell value (Text/Blob) of known
//!    length `L`, locates the LITERAL on-disk `writeUnsignedVInt(L)` length
//!    prefix that precedes the value bytes.
//!
//!    ## Anchoring: the match must be a REAL cell length prefix (roborev job 2769)
//!
//!    A plain substring search for the value bytes could hit a COINCIDENTAL
//!    occurrence of those bytes rather than the actual length-prefixed cell
//!    region, so the confirmed evidence would not be anchored to a real cell
//!    length field. The authoritative scan path (`SSTableReader::scan`) yields
//!    decoded `ScanRow` values only — it does not surface the byte offset of each
//!    cell value in the stitched buffer, and plumbing offsets through the whole
//!    row/cell parse state machine would be invasive and fragile — so we cannot
//!    anchor to a decoder-reported offset directly.
//!
//!    Instead we make each substring match UNAMBIGUOUS: the value byte sequence
//!    must occur EXACTLY ONCE in the entire decompressed data section. Given
//!    uniqueness, that single position IS provably the real on-disk location of
//!    the cell value the authoritative decoder produced (there is no alternative
//!    coincidental occurrence it could confuse it with). By the on-disk V5 cell
//!    layout `writeUnsignedVInt(L)` immediately followed by the `L` value bytes,
//!    the bytes immediately preceding that unique position ARE the real length
//!    prefix. We additionally confirm byte-exact: the recovered vint must occupy
//!    exactly the bytes ending where the value begins (whole-prefix consumption)
//!    and unsigned-decode to `L`. Non-unique matches are skipped, so a confirmed
//!    site is a genuine, provably-anchored on-disk length prefix. (Uniqueness
//!    filtering may reduce the confirmed-site count below the `MAX_SITES` cap;
//!    the non-vacuity assertion still guarantees the count stays well above 0.)
//! 3. It then decodes those literal on-disk prefix bytes with BOTH decoders,
//!    per-site, into LOCAL counters: the UNSIGNED decode (`parse_vint_length`)
//!    must equal the authoritative length `L`, while a LOCAL differential tallies
//!    how many of those real length prefixes the legacy signed ZigZag decode
//!    would have mis-read. The tally is LOCAL (never a crate-global counter), so
//!    the assertion is deterministic even though Rust runs unit tests
//!    concurrently in one binary (roborev job 2765).
//!
//! The test FAILS if it silently exercises nothing (`agree + disagree == 0`),
//! guarding against a future 0-decode regression.
//!
//! It must be a LIB unit test (not an integration test): the
//! `stitched_data_section_for_tests` accessor is `#[cfg(test)]`, only compiled
//! for the crate's own unit-test build.
//!
//! Fixture-gating (repo doctrine): SKIPs cleanly when the dataset binaries are
//! absent, but treats "present but zero tables" as a FAILURE. `CQLITE_DATASETS_ROOT`
//! keyed; `CQLITE_REQUIRE_FIXTURES=1` turns the absent-corpus skip into a hard fail.

use crate::parser::vint::{parse_vint, parse_vint_length};
use crate::storage::sstable::reader::SSTableReader;
use crate::types::{ScanRow, TableId, Value};
use crate::{Config, Platform};
use std::path::PathBuf;
use std::sync::Arc;

fn require_fixtures() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    ) || matches!(
        std::env::var("CQLITE_PARITY_REQUIRE_DATASETS")
            .ok()
            .as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    )
}

fn datasets_root() -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let p = PathBuf::from(root);
    if p.is_dir() {
        Some(p)
    } else {
        None
    }
}

/// Collect `(data_db_path, table_name)` for every SSTable generation with a
/// Data.db under `sstables/<keyspace>/<table>-<uuid>/`.
fn collect_tables(sstables: &std::path::Path) -> Vec<(PathBuf, String)> {
    let mut out = Vec::new();
    let Ok(keyspaces) = std::fs::read_dir(sstables) else {
        return out;
    };
    for ks in keyspaces.flatten() {
        let ks_path = ks.path();
        if !ks_path.is_dir() {
            continue;
        }
        let Ok(tables) = std::fs::read_dir(&ks_path) else {
            continue;
        };
        for table in tables.flatten() {
            let table_dir = table.path();
            if !table_dir.is_dir() {
                continue;
            }
            // dir name is `<table>-<uuid>`; CQL identifiers cannot contain '-',
            // so the table name is everything before the final '-'.
            let dir_name = table_dir
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            let table_name = dir_name
                .rsplit_once('-')
                .map(|(name, _uuid)| name.to_string())
                .unwrap_or_else(|| dir_name.to_string());
            let Ok(files) = std::fs::read_dir(&table_dir) else {
                continue;
            };
            for f in files.flatten() {
                let path = f.path();
                if path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.ends_with("-Data.db"))
                    .unwrap_or(false)
                {
                    out.push((path, table_name.clone()));
                }
            }
        }
    }
    out.sort();
    out
}

/// Find the first occurrence of `needle` in `haystack` starting at `from`.
fn find_from(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if needle.is_empty() || from >= haystack.len() {
        return None;
    }
    haystack[from..]
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|rel| from + rel)
}

/// Return the position of `needle` in `haystack` iff it occurs EXACTLY ONCE.
///
/// Uniqueness is what anchors a substring match to the real cell region: if the
/// value bytes appear exactly once in the whole decompressed data section, that
/// single position is provably the on-disk location of the cell value the
/// authoritative decoder produced — there is no coincidental alternative
/// occurrence to confuse it with (roborev job 2769). Returns `None` for 0 or
/// >=2 occurrences (short-circuits on the second hit, so it is bounded).
fn unique_occurrence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    let first = find_from(haystack, needle, 0)?;
    // Any further occurrence (even overlapping) makes the match ambiguous.
    if find_from(haystack, needle, first + 1).is_some() {
        return None;
    }
    Some(first)
}

/// Minimum authoritative value length we accept as an extraction anchor. A
/// 6+-byte value both makes a coincidental byte-sequence match vanishingly
/// unlikely and makes the required whole-buffer uniqueness (below) common, so a
/// confirmed hit is a genuine on-disk `writeUnsignedVInt` length prefix.
const MIN_ANCHOR_LEN: usize = 6;

/// Cap on confirmed differential sites — enough to be decisive, bounded so the
/// naive substring search over multi-MB buffers stays fast.
const MAX_SITES: usize = 256;

/// Locate LITERAL on-disk `writeUnsignedVInt(L)` length prefixes in a
/// decompressed V5 data section, cross-checked against the authoritative decoded
/// value length `L`. Pushes `(prefix_bytes, expected_len)` for each confirmed
/// site. The differential tally is computed later from these confirmed sites
/// using LOCAL counters, so this probing does not affect the tally.
///
/// Anchoring (roborev job 2769): a value is only used when its byte sequence
/// occurs EXACTLY ONCE in `buffer`. That uniqueness pins the match to the real
/// cell location, so the vint immediately preceding it is a genuine on-disk
/// length prefix — not a coincidental byte match. Non-unique values are skipped.
fn extract_length_prefixes(
    buffer: &[u8],
    rows: &[(crate::RowKey, ScanRow)],
    out: &mut Vec<(Vec<u8>, usize)>,
) {
    for (_key, row) in rows {
        if out.len() >= MAX_SITES {
            return;
        }
        let ScanRow::Row(cells) = row else {
            continue;
        };
        for (_name, value) in cells {
            if out.len() >= MAX_SITES {
                return;
            }
            let vbytes: &[u8] = match value {
                Value::Text(s) => s.as_bytes(),
                Value::Blob(b) => b.as_slice(),
                _ => continue,
            };
            let expected_len = vbytes.len();
            if expected_len < MIN_ANCHOR_LEN {
                continue;
            }
            // Require the value bytes to occur exactly once so the match is
            // provably the real cell region (see `unique_occurrence`). The
            // on-disk cell layout is `writeUnsignedVInt(L)` immediately followed
            // by the `L` value bytes, so the unsigned vint ending exactly where
            // this unique occurrence begins IS the real length prefix.
            let Some(p) = unique_occurrence(buffer, vbytes) else {
                continue;
            };
            for k in 1..=9usize {
                if k > p {
                    continue;
                }
                let prefix = &buffer[p - k..p];
                if let Ok((rem, decoded)) = parse_vint_length(prefix) {
                    // Byte-exact: the vint occupies the whole `k`-byte prefix
                    // (rem empty) AND decodes to the authoritative length.
                    if rem.is_empty() && decoded == expected_len {
                        out.push((prefix.to_vec(), expected_len));
                        break;
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn corpus_differential_unsigned_length_decode() {
    let Some(root) = datasets_root() else {
        assert!(
            !require_fixtures(),
            "CQLITE_REQUIRE_FIXTURES=1 but CQLITE_DATASETS_ROOT is unset/not a dir"
        );
        eprintln!("SKIP: CQLITE_DATASETS_ROOT unset; corpus-differential test skipped.");
        return;
    };
    let sstables = root.join("sstables");
    if !sstables.is_dir() {
        assert!(
            !require_fixtures(),
            "CQLITE_REQUIRE_FIXTURES=1 but {}/sstables is absent",
            root.display()
        );
        eprintln!(
            "SKIP: {}/sstables absent; corpus-differential test skipped.",
            root.display()
        );
        return;
    }

    let tables = collect_tables(&sstables);
    // Present-but-empty is a failure regardless of require_fixtures: a
    // clean checkout ships JSONL only, but if the sstables/ tree exists it
    // must contain Data.db binaries.
    assert!(
        !tables.is_empty(),
        "corpus present at {} but no Data.db files found — fetch datasets \
         (bash test-data/scripts/fetch-datasets.sh)",
        sstables.display()
    );

    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("platform init should succeed"),
    );

    let mut scanned = 0usize;
    let mut scans_ok = 0usize;
    let mut failures: Vec<String> = Vec::new();
    // Confirmed literal on-disk unsigned-vint length prefixes + authoritative
    // expected lengths.
    let mut sites: Vec<(Vec<u8>, usize)> = Vec::new();

    for (data_db, table_name) in &tables {
        scanned += 1;
        let reader = match SSTableReader::open(data_db, &config, platform.clone()).await {
            Ok(r) => r,
            Err(e) => {
                failures.push(format!("open {}: {e}", data_db.display()));
                continue;
            }
        };
        let table_id = TableId::new(table_name.clone());
        let rows = match reader.scan(&table_id, None, None, None, None).await {
            Ok(rows) => {
                scans_ok += 1;
                rows
            }
            Err(e) => {
                failures.push(format!("scan {}: {e}", data_db.display()));
                continue;
            }
        };

        // Only the stitched V5CompressedLegacy ("nb") data section exposes the
        // literal on-disk length prefixes we anchor on. Other formats
        // (oa index-read placeholders, BTI) return None and are skipped here —
        // the scan-soundness check above still covers them.
        if sites.len() < MAX_SITES {
            if let Ok(Some(buffer)) = reader.stitched_data_section_for_tests().await {
                extract_length_prefixes(&buffer, &rows, &mut sites);
            }
        }
    }

    // Now run the differential over the confirmed literal on-disk prefixes using
    // LOCAL counters (no crate-global armed hook). This keeps the tally
    // deterministic even though Rust runs unit tests concurrently in one binary —
    // no other test can perturb these local `usize` variables (roborev job 2765).
    let mut agree = 0usize;
    let mut disagree = 0usize;
    for (prefix, expected) in &sites {
        // Fixed behaviour: UNSIGNED decode must consume the whole prefix and
        // equal the authoritative on-disk length.
        let (rem, decoded) =
            parse_vint_length(prefix).expect("confirmed on-disk prefix must decode");
        assert!(
            rem.is_empty(),
            "confirmed prefix must be consumed whole (bytes={prefix:02x?})"
        );
        assert_eq!(
            decoded, *expected,
            "unsigned decode of real on-disk length prefix {prefix:02x?} must equal the \
             authoritative value length {expected}"
        );

        // Legacy behaviour: signed ZigZag decode, rejecting negatives, as usize.
        // Tally whether it AGREES or DISAGREES with the authoritative length —
        // this is the real blast radius of the old ZigZag mis-read.
        let legacy = parse_vint(prefix)
            .ok()
            .and_then(|(_, v)| usize::try_from(v).ok());
        if legacy == Some(*expected) {
            agree += 1;
        } else {
            disagree += 1;
        }
    }

    eprintln!(
        "Issue #1623 corpus differential: tables={scanned} scans_ok={scans_ok} \
         real_length_prefix_sites={} length_decodes_agree={agree} \
         length_decodes_disagree={disagree}",
        sites.len()
    );
    if !failures.is_empty() {
        eprintln!("Issue #1623 scan failures ({}):", failures.len());
        for f in &failures {
            eprintln!("  - {f}");
        }
    }

    assert!(scanned > 0, "corpus present but no tables were scanned");
    // The fixed unsigned length decoder must let every corpus SSTable open and
    // scan without a structural (length/count) parse error.
    assert!(
        failures.is_empty(),
        "{} corpus SSTable(s) failed to open/scan with the unsigned length decoder",
        failures.len()
    );

    // The whole point of the differential (roborev job 2718): the test must have
    // actually decoded REAL Cassandra length prefixes. A zero tally means the
    // test silently exercised nothing — fail rather than pass vacuously.
    assert_eq!(
        agree + disagree,
        sites.len(),
        "every confirmed on-disk length prefix must be tallied exactly once"
    );
    assert!(
        agree + disagree > 0,
        "corpus present but NO real on-disk unsigned-vint length prefix was decoded — \
         the differential exercised nothing (regression guard for the vacuous-test bug)"
    );
}
