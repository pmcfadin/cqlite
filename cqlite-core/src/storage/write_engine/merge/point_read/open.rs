//! Open-and-probe wrappers for the single-partition point read (split out of `point_read.rs`,
//! issue #1116 campsite; unchanged code).

#[cfg(feature = "tombstones")]
use super::PathProbe;
use super::ProbeOutcome;
#[cfg(not(feature = "tombstones"))]
use super::{block_on_async, probe_reader_async};
#[cfg(not(feature = "tombstones"))]
use crate::config::DiskAccessMode;
#[cfg(not(feature = "tombstones"))]
use crate::platform::Platform;
use crate::schema::TableSchema;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::SSTableReader;
use crate::storage::write_engine::UdtRegistry;
#[cfg(not(feature = "tombstones"))]
use crate::Config;
use crate::Result;
use std::path::Path;
use std::sync::Arc;

/// Open `path` ONCE and probe it for every requested key, delegating to
/// [`probe_reader_async`] (issue #2346) once the reader is open.
///
/// Runs the async open + seek on the shared bridge runtime. The reader is opened
/// with buffered I/O (never mmap/direct — the file may be deleted after the
/// read), matching the full-scan producer's file-lifetime contract (issue #591).
#[cfg(not(feature = "tombstones"))]
pub(super) fn probe_path(
    path: &Path,
    keys: &[Vec<u8>],
    schema: &TableSchema,
    udt_registry: Option<&UdtRegistry>,
    scan_cancel: ScanCancel,
    collect_notes: bool,
) -> Result<ProbeOutcome> {
    let path = path.to_path_buf();
    let schema = schema.clone();
    let keys: Vec<Vec<u8>> = keys.to_vec();
    let udt_registry = udt_registry.cloned();
    block_on_async(async move {
        let mut config = Config::default();
        config.storage.use_mmap = false;
        config.storage.disk_access_mode = DiskAccessMode::Buffered;
        let platform = Arc::new(Platform::new(&config).await?);
        let mut reader = SSTableReader::open(&path, &config, platform).await?;
        // Issue #2349: decode a `frozen<UDT>`-in-collection cell structurally on the
        // point-read seek path too, matching the full-scan cold reader.
        if let Some(registry) = udt_registry {
            reader.set_udt_registry(registry);
        }
        probe_reader_async(&reader, &keys, &schema, &scan_cancel, collect_notes).await
    })
}

/// Probe an ALREADY-OPEN, possibly-SHARED `reader` (issue #2346) — the
/// reader-based analogue of [`probe_path`], used by
/// [`build_single_partition_merger_from_readers`]. `reader` is an `Arc` clone
/// (the caller keeps the original for the `NeedsScan` fail-safe path), moved
/// into the bridged future so no borrow needs to outlive this call.
#[cfg(not(feature = "tombstones"))]
pub(super) fn probe_reader(
    reader: Arc<SSTableReader>,
    keys: &[Vec<u8>],
    schema: &TableSchema,
    scan_cancel: ScanCancel,
    collect_notes: bool,
) -> Result<ProbeOutcome> {
    let schema = schema.clone();
    let keys: Vec<Vec<u8>> = keys.to_vec();
    block_on_async(async move {
        probe_reader_async(&reader, &keys, &schema, &scan_cancel, collect_notes).await
    })
}

/// Tombstones-build fallback: the single-partition SEEK machinery
/// (`successor_partition_offset`, `point_read_whole_section`, …) is
/// `not(tombstones)` only, so under the alternate `tombstones` feature every
/// candidate degrades to a full scan filtered to the key set — correct, just
/// without the seek speedup. Keeps the public builder API identical across builds.
#[cfg(feature = "tombstones")]
pub(super) fn probe_path(
    _path: &Path,
    _keys: &[Vec<u8>],
    _schema: &TableSchema,
    _udt_registry: Option<&UdtRegistry>,
    _scan_cancel: ScanCancel,
    _collect_notes: bool,
) -> Result<ProbeOutcome> {
    Ok(ProbeOutcome {
        probe: PathProbe::NeedsScan,
        notes: Vec::new(),
    })
}

/// Tombstones-build fallback for the reader-based probe (issue #2346) — mirrors
/// [`probe_path`]'s `tombstones` fallback above.
#[cfg(feature = "tombstones")]
pub(super) fn probe_reader(
    _reader: Arc<SSTableReader>,
    _keys: &[Vec<u8>],
    _schema: &TableSchema,
    _scan_cancel: ScanCancel,
    _collect_notes: bool,
) -> Result<ProbeOutcome> {
    Ok(ProbeOutcome {
        probe: PathProbe::NeedsScan,
        notes: Vec::new(),
    })
}
