//! Lazy Summary-guided `Index.db` open (issue #2412, design §A).
//!
//! Split out of `index_reader/mod.rs` (campsite #1116) — the eager constructors,
//! struct definition, and sync accessors stay in `mod.rs` unchanged; this file adds
//! the NEW lazy-open primitives `IndexReader::open_lazy` /
//! `IndexReader::ensure_materialized` plus their internal `MaterializedIndex` cell
//! type, used only by `SSTableReader`'s BIG-open composition
//! (`reader::component_loading::load_index_reader`).

use super::parse::{parse_big_index_entry, parse_index_data_cancellable};
use super::{IndexData, IndexReader};
use crate::error::{Error, Result};
use crate::platform::Platform;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

/// Cap on the bounded validity-probe prefix [`IndexReader::open_lazy`] reads
/// (issue #2302 open-time-detection preservation through the lazy-open change).
/// Large enough to cover the WORST-CASE single `Index.db` entry — a `u16`
/// key-length prefix (max 65535 bytes) + the raw key + two small vints — so a
/// legitimately huge partition key can never false-trigger the probe; bounded so
/// the probe's cost stays O(1) at open time, never O(partition count).
const VALIDITY_PROBE_PREFIX_CAP: usize = 70_000;

/// The full parse result, materialized either eagerly (at construction, the
/// unchanged legacy behavior every direct [`IndexReader::open`] caller relies on)
/// or lazily (issue #2412, on the first [`IndexReader::ensure_materialized`] call).
pub(super) struct MaterializedIndex {
    pub(super) index_data: IndexData,
    /// Whether the entry parse consumed the ENTIRE Index.db file (issue #2302).
    /// The parser `break`s on the first unparseable entry and returns the parsed
    /// PREFIX, so a mid-entry-truncated file opens with leftover bytes. `true` ⟺
    /// no bytes remained — the authoritative signal the stream was not cut
    /// mid-entry (WHOLE trailing entries dropped at an exact boundary are caught
    /// separately at the enumeration site). Only the completeness-sensitive full
    /// enumeration consults it; point-lookup callers tolerate a partial prefix.
    pub(super) fully_parsed: bool,
}

impl IndexReader {
    /// Path to the backing `Index.db` file (issue #2412 §B). The Summary-guided
    /// point-lookup path (`reader::summary_point`) seeks bounded intervals from this
    /// path WITHOUT materializing the whole map. Lives here (not the parent module)
    /// as a lazy-open support accessor, keeping `index_reader/mod.rs` under the
    /// campsite line (#1116).
    pub(crate) fn index_path(&self) -> &Path {
        &self.file_path
    }

    /// Whether the full `Index.db` parse has already run (issue #2412 §B). A lazily
    /// opened reader ([`Self::open_lazy`]) reports `false` until the first
    /// [`Self::ensure_materialized`]; an eagerly opened reader reports `true` from
    /// construction. The Summary-guided point path uses this to prefer the resident
    /// map once it is already in memory (e.g. after a full scan) and the bounded
    /// interval read only while the map is still deferred.
    pub(crate) fn is_materialized(&self) -> bool {
        self.materialized.get().is_some()
    }

    /// Lazy Summary-guided open (issue #2412, design §A): checks the file EXISTS
    /// and is STRUCTURALLY VALID via a bounded O(1) prefix probe, but performs NO
    /// full `Index.db` parse. Used by `SSTableReader`'s BIG open composition when a
    /// usable `Summary.db` is present, so open cost is bounded by `Summary.db`
    /// size + the probe's fixed cap, not by `Index.db` partition count. The full
    /// parse is deferred to the first [`Self::ensure_materialized`] call from an
    /// internal consumer that genuinely needs the resident map (a full-enumeration
    /// scan, or the point-lookup fallback before Stage 3's interval-bounded lookup
    /// lands); a point-lookup-dominated workload may never pay it at all.
    ///
    /// ## Open-time present-but-unloadable detection (issue #2302 contract preserved)
    ///
    /// A present-but-EMPTY or present-but-structurally-broken `Index.db` (open/parse
    /// fails at the very first entry) MUST be distinguishable from a genuinely
    /// valid file AT OPEN TIME, not only when a later consumer happens to call
    /// [`Self::ensure_materialized`] — issue #2302 killed exactly this class of
    /// silent degradation (the pre-#2412 eager `open` always full-parsed at open,
    /// so a corrupt file was detected immediately; a lazy open that ONLY checked
    /// file existence would silently regress that guarantee, reporting a broken
    /// file as a usable lazy reader until something eventually materialized it —
    /// possibly never, for a point-lookup-only workload). This is NOT negotiable:
    /// open-time detection is the safer contract (`component_loading::load_index_reader`
    /// maps any non-`NotFound` `Err` here to the loud-WARN `PresentButUnloadable`
    /// path, exactly as the eager path always has).
    ///
    /// The probe: read a BOUNDED prefix (≤ [`VALIDITY_PROBE_PREFIX_CAP`] bytes, or
    /// the whole file when smaller) and confirm the file is non-empty AND its FIRST
    /// entry parses ([`parse_big_index_entry`]) — the SAME authoritative on-disk
    /// entry framing `ensure_materialized`'s full parse and the eager `open` both
    /// use (no heuristics, issue #28). This is intentionally NOT a full structural
    /// guarantee (a corruption deep inside the file, past the first entry, is still
    /// caught later by `is_fully_parsed()`/Signal A at first materializing use,
    /// exactly as before this change) — it is the SAME bounded confidence the
    /// original eager `open` effectively offered before its first byte was even
    /// consumed, at O(1) cost instead of O(partitions).
    ///
    /// Not counted on EITHER `index_parses_total` or `index_interval_parses_total`:
    /// this probe is neither a full parse nor a Summary-guided per-lookup interval
    /// read (design §F) — conflating it with either would muddy the field-round
    /// parse-count dashboards those counters exist for. It is deliberately silent
    /// on success (the common case) and loud only via the `PresentButUnloadable`
    /// WARN path on failure, matching #2302's existing observability contract.
    pub(crate) async fn open_lazy(path: &Path, platform: Arc<Platform>) -> Result<Self> {
        if !platform.fs().exists(path).await? {
            return Err(Error::not_found(format!(
                "Index.db file not found: {}",
                path.display()
            )));
        }

        let mut file = File::open(path).await?;
        let file_len = file.metadata().await?.len();
        if file_len == 0 {
            return Err(Error::corruption(format!(
                "Index.db file is empty: {}",
                path.display()
            )));
        }
        let probe_len = file_len.min(VALIDITY_PROBE_PREFIX_CAP as u64) as usize;
        let mut probe_buf = vec![0u8; probe_len];
        file.read_exact(&mut probe_buf).await?;
        if let Err(e) = parse_big_index_entry(&probe_buf) {
            return Err(Error::corruption(format!(
                "Index.db first entry failed to parse (present-but-unloadable) at {}: {:?}",
                path.display(),
                e
            )));
        }

        // `file_path`/`materialized` are private fields of `IndexReader`, defined in
        // the parent `index_reader` module — visible here because `lazy` is a
        // DESCENDANT module of it (Rust privacy: private items are visible to the
        // defining module and all its descendants), so no field-access bridge
        // methods are needed on the struct itself.
        Ok(Self {
            file_path: path.to_path_buf(),
            materialized: tokio::sync::OnceCell::new(),
            platform,
        })
    }

    /// Ensure the full `Index.db` parse has run, performing it exactly once
    /// (memoized) on first call — the deferred cost [`Self::open_lazy`] skips at
    /// open time (issue #2412). A no-op (immediate return) for a reader constructed
    /// via the eager constructors, since their cell is already populated. Cancel-
    /// aware: polls `cancel` the same way the eager parse does, so a cooperative
    /// cancellation still aborts a large deferred parse promptly.
    pub(crate) async fn ensure_materialized(
        &self,
        cancel: &crate::storage::scan_cancel::ScanCancel,
    ) -> Result<()> {
        self.materialized
            .get_or_try_init(|| async {
                cancel.check()?;
                let mut file = File::open(&self.file_path).await?;
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer).await?;
                if buffer.is_empty() {
                    return Err(Error::corruption(format!(
                        "Index.db file is empty: {}",
                        self.file_path.display()
                    )));
                }
                let (remaining, index_data) =
                    match parse_index_data_cancellable(&buffer, None, cancel) {
                        Ok(pair) => pair,
                        Err(e @ Error::Cancelled) => return Err(e),
                        Err(e) => {
                            return Err(Error::corruption(format!(
                                "Failed to parse Index.db: {:?}",
                                e
                            )));
                        }
                    };
                let fully_parsed = remaining.is_empty();
                Ok(MaterializedIndex {
                    index_data,
                    fully_parsed,
                })
            })
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_index_file(name_hint: &str, entry: &[u8]) -> (std::path::PathBuf, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "cqlite-2412-{name_hint}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("nb-1-big-Index.db");
        std::fs::write(&path, entry).unwrap();
        (dir, path)
    }

    /// Issue #2412 (Stage 2, §A): `open_lazy` performs ZERO `Index.db` parse work —
    /// no entries touched, `is_fully_parsed()` conservatively `false` — until
    /// `ensure_materialized` is called. Scale-free work probe: constructed from a
    /// synthetic single-entry `Index.db` so the assertion is about ORDER of
    /// operations, not entry count.
    #[tokio::test]
    async fn open_lazy_touches_zero_entries_until_materialized() {
        // One well-formed BIG entry: key_len(2) + key(4) + vint offset + vint promoted_len=0.
        let entry: Vec<u8> = vec![0x00, 0x04, 0xAA, 0xBB, 0xCC, 0xDD, 0x00, 0x00];
        let (dir, path) = temp_index_file("open-lazy", &entry);

        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("platform must initialize"),
        );

        let reader = IndexReader::open_lazy(&path, platform)
            .await
            .expect("open_lazy must succeed for an existing file");

        // Zero work at open: no entries visible, not reported fully-parsed.
        assert!(
            reader.get_partition_entries().is_empty(),
            "open_lazy must not materialize any entries before ensure_materialized"
        );
        assert!(
            !reader.is_fully_parsed(),
            "open_lazy must not report fully_parsed before materialization"
        );
        assert!(reader.lookup_partition(&[0xAA, 0xBB, 0xCC, 0xDD]).is_none());

        // First real use triggers exactly the deferred full parse.
        reader
            .ensure_materialized(&crate::storage::scan_cancel::ScanCancel::default())
            .await
            .expect("deferred materialize must succeed for a well-formed file");
        assert_eq!(reader.get_partition_entries().len(), 1);
        assert!(reader.is_fully_parsed());
        assert!(reader.lookup_partition(&[0xAA, 0xBB, 0xCC, 0xDD]).is_some());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Issue #2302 open-time contract, preserved through lazy open (roborev
    /// endgame finding): a present-but-EMPTY `Index.db` must be REJECTED by
    /// `open_lazy` itself — `Err`, never a silently "valid" lazy reader.
    /// `component_loading::load_index_reader` maps this `Err` to the loud-WARN
    /// `PresentButUnloadable` path (the field-level pin is
    /// `issue_2302_written_index_resolve::present_but_unloadable_index_warns_with_summary`).
    #[tokio::test]
    async fn open_lazy_rejects_an_empty_file() {
        let (dir, path) = temp_index_file("empty", &[]);

        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("platform must initialize"),
        );
        let result = IndexReader::open_lazy(&path, platform).await;
        let is_corruption = matches!(result, Err(Error::Corruption(_)));
        let err_display = result.as_ref().err().map(|e| e.to_string());
        assert!(
            is_corruption,
            "open_lazy must reject an empty Index.db as Corruption, got {err_display:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Companion to the empty-file case: a present, NON-EMPTY file whose FIRST
    /// entry is structurally unparseable (a dangling key-length header with no
    /// body — the SAME truncation shape `full_index_stream`'s
    /// `scan_stops_on_truncated_tail`-style tests use elsewhere) must ALSO be
    /// rejected by the bounded validity probe, not just the zero-byte case.
    #[tokio::test]
    async fn open_lazy_rejects_a_truncated_first_entry() {
        // Dangling key-length header (claims a 0x40-byte key) with zero body bytes.
        let entry: Vec<u8> = vec![0x00, 0x40];
        let (dir, path) = temp_index_file("truncated-first-entry", &entry);

        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("platform must initialize"),
        );
        let result = IndexReader::open_lazy(&path, platform).await;
        let is_corruption = matches!(result, Err(Error::Corruption(_)));
        let err_display = result.as_ref().err().map(|e| e.to_string());
        assert!(
            is_corruption,
            "open_lazy must reject a truncated first entry as Corruption, got {err_display:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `ensure_materialized` is idempotent/memoized: calling it twice does not
    /// re-parse (the second call is a cheap `OnceCell::get` under the hood) and
    /// yields the identical result both times.
    #[tokio::test]
    async fn ensure_materialized_is_memoized() {
        let entry: Vec<u8> = vec![0x00, 0x02, 0x11, 0x22, 0x00, 0x00];
        let (dir, path) = temp_index_file("memoized", &entry);

        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("platform must initialize"),
        );
        let reader = IndexReader::open_lazy(&path, platform).await.unwrap();
        let cancel = crate::storage::scan_cancel::ScanCancel::default();

        reader.ensure_materialized(&cancel).await.unwrap();
        let first_len = reader.get_partition_entries().len();
        reader.ensure_materialized(&cancel).await.unwrap();
        let second_len = reader.get_partition_entries().len();
        assert_eq!(first_len, second_len);
        assert_eq!(first_len, 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Eager constructors ([`IndexReader::open`]) are UNCHANGED: entries are
    /// visible immediately, no `ensure_materialized` call needed — the pre-#2412
    /// behavior every direct caller (including the many integration tests that
    /// construct `IndexReader` directly) relies on.
    #[tokio::test]
    async fn eager_open_is_immediately_materialized() {
        let entry: Vec<u8> = vec![0x00, 0x02, 0x33, 0x44, 0x00, 0x00];
        let (dir, path) = temp_index_file("eager", &entry);

        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("platform must initialize"),
        );
        let reader = IndexReader::open(&path, platform).await.unwrap();
        assert_eq!(reader.get_partition_entries().len(), 1);
        assert!(reader.is_fully_parsed());
        assert!(reader.lookup_partition(&[0x33, 0x44]).is_some());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Encode one BIG `Index.db` entry: `[key_len u16 BE][key][data_offset vint][promoted_len vint=0]`
    /// (mirrors `index_reader::stream`'s test helper — kept local since this
    /// module has no shared test-fixture crate to depend on without a cycle).
    fn encode_entry(key: &[u8], data_offset: u64) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(key.len() as u16).to_be_bytes());
        out.extend_from_slice(key);
        out.extend_from_slice(&crate::parser::vint::encode_vuint(data_offset));
        out.extend_from_slice(&crate::parser::vint::encode_vuint(0));
        out
    }

    /// Issue #2412 (coordinator-flagged regression, re-anchor of the #2383 fix-C
    /// guard): a cancellation tripped DURING [`IndexReader::ensure_materialized`]'s
    /// deferred full parse must abort promptly with `Error::Cancelled`, not run
    /// the whole O(entries) parse to completion.
    ///
    /// `ensure_materialized` is the SURVIVING big-parse site for a Summary-usable
    /// BIG reader (design §A/§C): open itself is now O(summary) and performs no
    /// parse at all, but a consumer that still needs the resident map — a full
    /// enumeration whose Summary-guided streaming walk `FellBack`, or a
    /// compaction full-ring scan — calls `ensure_materialized`, which reuses the
    /// SAME `parse_index_data_cancellable` entry loop the pre-#2412 eager `open`
    /// always ran (unchanged: `CANCEL_POLL_INTERVAL`-bounded cooperative poll,
    /// `index_reader/parse.rs`). This test proves that cancel-poll survived the
    /// move to the deferred/lazy call site.
    ///
    /// Same calibrated-margin convention as
    /// `cqlite-flight::warm::registry::spin_tests_2383::cancel_during_large_index_parse_aborts_promptly`
    /// (issue #2383 roborev-1653 NIT 5 — no wall-clock races: the margin is a
    /// small fraction of a JUST-measured baseline for this exact fixture on this
    /// exact host, not a fixed sleep constant that flakes on a fast host).
    #[tokio::test]
    async fn ensure_materialized_cancel_mid_parse_aborts_promptly() {
        use crate::storage::scan_cancel::ScanCancel;

        // Large enough to span several `CANCEL_POLL_INTERVAL` (65536) windows, so
        // the parse loop polls cancel multiple times before completing — proving
        // "aborts DURING the parse", not merely at the coarse pre-check.
        const N: usize = 400_000;
        let mut bytes = Vec::new();
        for i in 0..N {
            let key = format!("key{i:010}");
            bytes.extend_from_slice(&encode_entry(key.as_bytes(), i as u64));
        }
        let (dir, path) = temp_index_file("ensure-materialized-cancel", &bytes);

        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("platform must initialize"),
        );

        // Calibrate: fully materialize the SAME fixture once, uncancelled — also
        // warms the OS page cache ahead of the timed run below (biasing it
        // FASTER, never slower, than this baseline).
        let calib_start = std::time::Instant::now();
        let calib_reader = IndexReader::open_lazy(&path, platform.clone())
            .await
            .expect("calibration open_lazy");
        calib_reader
            .ensure_materialized(&ScanCancel::default())
            .await
            .expect("calibration materialize (uncancelled) completes");
        let baseline = calib_start.elapsed();
        // 1/20th of the measured baseline (same fraction as the flight-level
        // sibling test): small enough to land inside the timed run, large enough
        // to dwarf the coarse pre-parse check's same-thread, no-I/O comparison.
        let margin = baseline / 20;

        let reader = IndexReader::open_lazy(&path, platform)
            .await
            .expect("timed open_lazy");
        let cancel = ScanCancel::new();
        let canceller = {
            let cancel = cancel.clone();
            std::thread::spawn(move || {
                std::thread::sleep(margin);
                cancel.cancel();
            })
        };

        let result = reader.ensure_materialized(&cancel).await;
        canceller.join().expect("canceller");

        assert!(
            matches!(result, Err(Error::Cancelled)),
            "a cancel tripped DURING ensure_materialized's deferred Index.db parse \
             must abort promptly with Error::Cancelled (calibrated margin {margin:?} \
             from baseline {baseline:?}), got {result:?} (issue #2412 re-anchor of \
             #2383 fix C)"
        );
        // The aborted materialize must NOT have left a poisoned "materialized"
        // state: entries stay empty/not-fully-parsed, matching `open_lazy`'s
        // pre-materialize contract (never a half-parsed resident map).
        assert!(
            reader.get_partition_entries().is_empty(),
            "a cancelled materialize must not expose a partial resident map"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
