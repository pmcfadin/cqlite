//! The on-disk `*-Data.db` WALK — the one discovery both the constructors and
//! [`SSTableManager::refresh_tables`] run (split out of `sstable/mod.rs` and
//! `sstable/refresh.rs` per the campsite rule, epic #1116).
//!
//! Two entry points, one responsibility:
//!
//! * [`SSTableManager::find_data_files`] — the recursive base-path walk, used by
//!   `SSTableManager::new`'s `load_existing_sstables` and by the refresh's
//!   `DiscoverySource::BasePath` arm;
//! * [`SSTableManager::discover_data_file_paths`] — the refresh's re-discovery,
//!   which dispatches on the manager's recorded [`DiscoverySource`] so a refresh
//!   re-runs EXACTLY the discovery the manager was built with (issue #1749).
//!
//! # Every filesystem failure PROPAGATES (issue #4159)
//!
//! Both walks used to answer a `read_dir` / `file_type` failure by pretending the
//! directory was empty (`Err(_) => return Ok(results)`, `Err(_) => continue`,
//! `.unwrap_or(false)`). An unreadable directory then contributed no `*-Data.db`
//! paths, so its SSTables were silently absent from the reader map and the scan
//! surfaces reported that absence as an EMPTY SUCCESS — and on the refresh path it
//! was worse, because `refresh_tables` treats "not discovered" as "removed from
//! disk" and would DROP a live reader. Every read here now names its path and
//! propagates.

use std::path::{Path, PathBuf};

use super::MAX_SSTABLE_SCAN_DEPTH;
use super::{is_apple_double_sidecar, refresh::DiscoverySource, SSTableManager};
use crate::platform::Platform;
use crate::Result;

impl SSTableManager {
    /// Recursively find all *-Data.db files up to `max_depth` levels deep
    pub(super) fn find_data_files<'a>(
        platform: &'a Platform,
        dir: &'a Path,
        max_depth: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<PathBuf>>> + Send + 'a>>
    {
        let dir = dir.to_path_buf();
        Box::pin(async move {
            let mut results = Vec::new();

            // #4159: an unreadable directory is NOT an empty one. This used to
            // answer `Ok(results)` — so an EACCES/EIO on a table directory made
            // every SSTable under it silently absent from the reader map, and the
            // scan surfaces then reported that absence as an empty SUCCESS. The
            // error names the directory.
            let mut dir_entries = platform.fs().read_dir(&dir).await.map_err(|e| {
                crate::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to read SSTable directory {}: {e}", dir.display()),
                ))
            })?;

            while let Some(entry) = dir_entries.next_entry().await? {
                let path = entry.path();
                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    // Skip macOS AppleDouble sidecars via is_apple_double_sidecar().
                    // See Issue #481.
                    if filename.ends_with("-Data.db") && !is_apple_double_sidecar(filename) {
                        results.push(path);
                    } else if max_depth > 0 {
                        // Check if it's a directory and recurse.
                        //
                        // #4159: `.unwrap_or(false)` here meant a `stat` failure
                        // silently pruned a whole keyspace/table SUBTREE from
                        // discovery — the same "silently excluded from the scan
                        // set" defect as the `read_dir` swallow above, one level
                        // down. The failure is propagated with the path named.
                        let is_dir =
                            entry.file_type().await.map(|ft| ft.is_dir()).map_err(|e| {
                                crate::Error::Io(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    format!(
                                        "Failed to determine the file type of {} while scanning \
                                     for SSTables: {e}",
                                        path.display()
                                    ),
                                ))
                            })?;
                        if is_dir {
                            let sub_results =
                                Self::find_data_files(platform, &path, max_depth - 1).await?;
                            results.extend(sub_results);
                        }
                    }
                }
            }

            Ok(results)
        })
    }

    /// List the current on-disk `Data.db` paths using the manager's recorded
    /// [`DiscoverySource`] — the same discovery the manager was built with.
    pub(super) async fn discover_data_file_paths(&self) -> Result<Vec<PathBuf>> {
        match &self.discovery_source {
            DiscoverySource::BasePath => {
                if !self.platform.fs().exists(&self.base_path).await? {
                    return Ok(Vec::new());
                }
                SSTableManager::find_data_files(
                    &self.platform,
                    &self.base_path,
                    MAX_SSTABLE_SCAN_DEPTH,
                )
                .await
            }
            DiscoverySource::TableDirs(dirs) => {
                let mut out = Vec::new();
                for dir in dirs {
                    if !self.platform.fs().exists(dir).await? {
                        continue;
                    }
                    // #4159: `Err(_) => continue` here made a refresh SILENTLY
                    // FORGET every generation under a directory it could not read
                    // — and `refresh_tables` then treats "not discovered" as
                    // "removed from disk", so a live reader was dropped and the
                    // table's scan came back short under `Ok`. Refresh is
                    // fail-closed everywhere else (its opens abort the whole
                    // refresh and mutate nothing); this was the one hole in it.
                    let mut entries = self.platform.fs().read_dir(dir).await.map_err(|e| {
                        crate::Error::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!(
                                "Failed to read discovered table directory {} during \
                                 refresh: {e}",
                                dir.display()
                            ),
                        ))
                    })?;
                    while let Some(entry) = entries.next_entry().await? {
                        let path = entry.path();
                        if let Some(fname) = path.file_name().and_then(|n| n.to_str()) {
                            if fname.ends_with("-Data.db") && !is_apple_double_sidecar(fname) {
                                out.push(path);
                            }
                        }
                    }
                }
                Ok(out)
            }
        }
    }
}
