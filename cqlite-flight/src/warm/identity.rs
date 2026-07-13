//! Inode-stable SSTable generation identity (issue #2310, WS1 #2345).
//!
//! Decision 1 (design.md): the warm-handle cache is keyed on the logical table
//! plus the SET of SSTable generations present, where each generation's identity
//! is **inode-stable** — the device+inode of its `Data.db`, cross-checked with
//! the parsed generation number. This is the ONLY key that gives a warm hit
//! across the per-query snapshot hardlink dirs the field actually runs (a fresh
//! `snapshots/<uuid>/` directory per request hardlinks the SAME inodes, so a
//! path key would miss every time) while NEVER serving parsed state for bytes
//! that changed. Explicitly NOT a directory path and NOT a TTL/time bucket
//! (spec Requirement 1).

use std::path::Path;

/// The inode-stable identity of one SSTable generation's `Data.db`.
///
/// `(device, inode)` is the ground truth for "are these two directory entries
/// the same on-disk bytes" — two snapshot hardlink dirs referencing the same
/// file share `(device, inode)`. `generation` (parsed from the Cassandra file
/// name, e.g. `nb-12-big-Data.db` → `12`) is carried as a cross-check and for
/// human-legible eviction/logging; the `(device, inode)` pair is what makes the
/// key correct across hardlink dirs. All three participate in equality/ordering
/// so a device+inode reuse after a delete (a fresh SSTable landing on a recycled
/// inode) with a different generation number is still a distinct key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GenerationId {
    /// Filesystem device id of the `Data.db` (`stat.st_dev`).
    pub device: u64,
    /// Inode number of the `Data.db` (`stat.st_ino`).
    pub inode: u64,
    /// Generation number parsed from the SSTable file name (best-effort; 0 when
    /// unparseable). A cross-check on top of the authoritative inode identity.
    pub generation: u64,
}

impl GenerationId {
    /// Resolve the inode-stable identity of a `Data.db` at `path`.
    ///
    /// `stat`s the file (following the hardlink to the real inode) and parses the
    /// generation number from the file name. Returns `None` when the file cannot
    /// be `stat`ed (missing/racing removal) so the caller treats it as
    /// not-present rather than fabricating an identity.
    pub fn resolve(path: &Path) -> Option<Self> {
        let generation = generation_of(path);
        let (device, inode) = device_inode(path)?;
        Some(Self {
            device,
            inode,
            generation,
        })
    }
}

/// Best-effort parse of the generation number from a Cassandra SSTable file name
/// such as `nb-12-big-Data.db` → `12`. Returns 0 when not parseable.
///
/// Mirrors `producer::generation_of` (kept independent so the warm module has no
/// dependency on producer internals). Cross-referenced against the authoritative
/// inode identity above, so a parse miss (0) never alone determines the key.
fn generation_of(path: &Path) -> u64 {
    path.file_name()
        .and_then(|n| n.to_str())
        .and_then(|name| name.split('-').find_map(|seg| seg.parse::<u64>().ok()))
        .unwrap_or(0)
}

#[cfg(unix)]
fn device_inode(path: &Path) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    // `metadata` (not `symlink_metadata`) so a Cassandra snapshot hardlink
    // resolves to the SAME (device, inode) as the live file it links — the whole
    // point of the inode-stable key (Decision 1).
    let md = std::fs::metadata(path).ok()?;
    Some((md.dev(), md.ino()))
}

#[cfg(not(unix))]
fn device_inode(path: &Path) -> Option<(u64, u64)> {
    // Non-unix has no stable inode identity; fall back to "file exists" and let
    // the generation number carry the key. CQLite's supported deployment targets
    // are unix (macOS/Linux); this keeps the crate compiling elsewhere without
    // claiming inode stability it cannot provide. Consequence (accepted,
    // unsupported-target degradation): with a constant `(device, inode) = (0, 0)`
    // here, `GenerationId` identity degrades to `(generation, size)` wherever a
    // size check accompanies it (e.g. the #2383 rebind-by-inode gate,
    // `rebuild::rebind_matches`) — never a silent correctness claim on a target
    // CQLite does not support.
    std::fs::metadata(path).ok().map(|_| (0, 0))
}

/// A sorted, de-duplicated set of generation identities — the generation-set half
/// of the warm cache key. Sorted so two snapshot dirs enumerated in different
/// `read_dir` orders over the SAME inodes compare EQUAL (order-independent).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GenerationSet {
    ids: Vec<GenerationId>,
}

impl GenerationSet {
    /// Build a set from resolved identities, sorting + de-duplicating so equality
    /// is order-independent across directory-listing orders.
    pub fn from_ids(mut ids: Vec<GenerationId>) -> Self {
        ids.sort_unstable();
        ids.dedup();
        Self { ids }
    }

    /// The identities in sorted order.
    pub fn ids(&self) -> &[GenerationId] {
        &self.ids
    }

    /// Whether the set is empty (no SSTables present).
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// Whether `id` is a member of this set.
    pub fn contains(&self, id: &GenerationId) -> bool {
        self.ids.binary_search(id).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(dev: u64, ino: u64, gen: u64) -> GenerationId {
        GenerationId {
            device: dev,
            inode: ino,
            generation: gen,
        }
    }

    #[test]
    fn set_equality_is_order_independent() {
        // Two different read_dir orders over the SAME inodes (the cross-snapshot
        // case) MUST compare equal — the whole basis of the warm-hit key.
        let a = GenerationSet::from_ids(vec![id(1, 10, 1), id(1, 20, 2)]);
        let b = GenerationSet::from_ids(vec![id(1, 20, 2), id(1, 10, 1)]);
        assert_eq!(a, b, "generation set equality must be order-independent");
    }

    #[test]
    fn set_dedups_repeated_identities() {
        let s = GenerationSet::from_ids(vec![id(1, 10, 1), id(1, 10, 1)]);
        assert_eq!(s.ids().len(), 1, "duplicate identities collapse");
    }

    #[test]
    fn distinct_inode_or_generation_is_distinct_identity() {
        // A recycled inode with a fresh generation number is a DIFFERENT key —
        // never a stale warm hit for changed bytes.
        assert_ne!(id(1, 10, 1), id(1, 10, 2), "generation differs");
        assert_ne!(id(1, 10, 1), id(1, 11, 1), "inode differs");
        assert_ne!(id(1, 10, 1), id(2, 10, 1), "device differs");
    }

    #[test]
    fn resolve_reads_stable_inode_across_hardlinks() {
        // A hardlink resolves to the same (device, inode) as its target — the
        // property that makes a snapshot dir hit the live warm entry.
        let dir = tempfile::TempDir::new().unwrap();
        let original = dir.path().join("nb-7-big-Data.db");
        std::fs::write(&original, b"data").unwrap();
        let link = dir.path().join("snap-nb-7-big-Data.db");
        std::fs::hard_link(&original, &link).unwrap();

        let a = GenerationId::resolve(&original).expect("stat original");
        let b = GenerationId::resolve(&link).expect("stat hardlink");
        assert_eq!(a, b, "hardlink shares device+inode+generation identity");
        assert_eq!(a.generation, 7, "generation parsed from the name");
    }

    #[test]
    fn resolve_missing_file_is_none() {
        assert!(GenerationId::resolve(Path::new("/nonexistent/nb-1-big-Data.db")).is_none());
    }
}
