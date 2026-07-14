//! Lazy Summary-guided `Index.db` open (issue #2412, design §A).
//!
//! Split out of `index_reader/mod.rs` (campsite #1116) — the eager constructors,
//! struct definition, and sync accessors stay in `mod.rs` unchanged; this file adds
//! the NEW lazy-open primitives `IndexReader::open_lazy` /
//! `IndexReader::ensure_materialized` plus their internal `MaterializedIndex` cell
//! type, used only by `SSTableReader`'s BIG-open composition
//! (`reader::component_loading::load_index_reader`).

use super::parse::parse_index_data_cancellable;
use super::{IndexData, IndexReader};
use crate::error::{Error, Result};
use crate::platform::Platform;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

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
    /// Lazy Summary-guided open (issue #2412, design §A): checks only that the file
    /// EXISTS and performs ZERO `Index.db` parse work. Used by `SSTableReader`'s BIG
    /// open composition when a usable `Summary.db` is present, so open cost is
    /// bounded by `Summary.db` size, not by `Index.db` partition count. The full
    /// parse is deferred to the first [`Self::ensure_materialized`] call from an
    /// internal consumer that genuinely needs the resident map (a full-enumeration
    /// scan, or the point-lookup fallback before Stage 3's interval-bounded lookup
    /// lands); a point-lookup-dominated workload may never pay it at all.
    pub(crate) async fn open_lazy(path: &Path, platform: Arc<Platform>) -> Result<Self> {
        if !platform.fs().exists(path).await? {
            return Err(Error::not_found(format!(
                "Index.db file not found: {}",
                path.display()
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
}
