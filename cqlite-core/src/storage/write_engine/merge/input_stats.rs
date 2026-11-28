//! Compaction inputs resolved to generations, so every `Statistics.db` read in this module goes
//! through the generation module's trusted-metadata accessor (G11, issue #4626).
//!
//! The merge entry points take `Data.db` paths. [`InputStats::resolve`] lists each distinct input
//! directory once and pairs every path with the [`Generation`] the module lists for it. Cloning a
//! generation shares its metadata cache, so one run reads each input's `Statistics.db` once however
//! many helpers consult it (GD9.1). A path the listing does not return (hidden by a transaction
//! log, not an SSTable component name, an unreadable directory) resolves to nothing, which every
//! caller treats as "statistics unavailable" on its existing conservative path.

use crate::error::Error;
use crate::parser::repair_metadata::{RepairMetadata, StatsExtras};
use crate::parser::statistics::SSTableStatistics;
use crate::storage::sstable::generation::{
    list_generations, Generation, MetadataFailure, Scope, TocPolicy,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Why an input's trusted statistics are not available.
#[derive(Debug, Clone)]
pub(crate) enum Unavailable {
    /// The listing did not return the path as a generation.
    Unresolved,
    /// The accessor's typed failure.
    Failure(MetadataFailure),
}

impl Unavailable {
    /// True when the input simply has no `Statistics.db` (or no generation to ask).
    pub(crate) fn is_absent(&self) -> bool {
        matches!(
            self,
            Self::Unresolved | Self::Failure(MetadataFailure::Absent { .. })
        )
    }
}

impl std::fmt::Display for Unavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unresolved => f.write_str("not a generation the listing returns"),
            Self::Failure(failure) => write!(f, "{failure}"),
        }
    }
}

/// One input: its `Data.db` path and, when resolved, its generation.
#[derive(Clone, Copy)]
pub(crate) struct Input<'a> {
    path: &'a Path,
    generation: Option<&'a Generation>,
}

impl<'a> Input<'a> {
    /// The input's `Data.db` path.
    pub(crate) fn path(&self) -> &'a Path {
        self.path
    }

    fn with<T>(
        &self,
        f: impl FnOnce(&Generation) -> Result<T, MetadataFailure>,
    ) -> Result<T, Unavailable> {
        match self.generation {
            None => Err(Unavailable::Unresolved),
            Some(g) => f(g).map_err(Unavailable::Failure),
        }
    }

    /// The trusted typed statistics (one cached read per generation).
    pub(crate) fn statistics(&self) -> Result<Arc<SSTableStatistics>, Unavailable> {
        self.with(|g| g.trusted_metadata())
    }

    /// The repair fields decoded from the same read.
    pub(crate) fn repair_metadata(&self) -> Result<Arc<RepairMetadata>, Unavailable> {
        self.with(|g| g.repair_metadata())
    }

    /// The STATS-extras histograms decoded from the same read.
    pub(crate) fn stats_extras(&self) -> Result<Arc<StatsExtras>, Unavailable> {
        self.with(|g| g.stats_extras())
    }
}

/// The inputs of one merge run, resolved once.
#[derive(Clone, Default)]
pub(crate) struct InputStats {
    inputs: Vec<(PathBuf, Option<Generation>)>,
}

/// `.` for a bare relative file name.
fn listing_dir(file: &Path) -> Option<PathBuf> {
    match file.parent()? {
        p if p.as_os_str().is_empty() => Some(PathBuf::from(".")),
        p => Some(p.to_path_buf()),
    }
}

impl InputStats {
    /// Resolve every path to the generation the module lists for it (Scope Live, `TocMissing`
    /// accepted; an Orphan is kept so a statistics-only fixture still resolves).
    pub(crate) fn resolve(paths: &[PathBuf]) -> Self {
        let mut listings: HashMap<PathBuf, Vec<Generation>> = HashMap::new();
        let mut inputs = Vec::with_capacity(paths.len());
        for path in paths {
            let generation = listing_dir(path).and_then(|dir| {
                let found = listings.entry(dir.clone()).or_insert_with(|| {
                    match list_generations(&dir, &Scope::Live, TocPolicy::Accept) {
                        Ok(l) => l.live.into_iter().chain(l.orphans).collect(),
                        Err(_) => Vec::new(),
                    }
                });
                let file = path.file_name()?.to_str()?;
                found
                    .iter()
                    .find(|g| format!("{}-Data.db", g.base_name()) == file)
                    .cloned()
            });
            inputs.push((path.clone(), generation));
        }
        Self { inputs }
    }

    /// Inputs that are already generations (a listing's), so the run shares the metadata cache of
    /// the caller's clones: `diagnose --deep` reads each `Statistics.db` once for the cheap tier,
    /// the deep scan and the dry compaction (GD9.1).
    pub(crate) fn from_generations(inputs: Vec<(PathBuf, Generation)>) -> Self {
        Self {
            inputs: inputs.into_iter().map(|(p, g)| (p, Some(g))).collect(),
        }
    }

    /// The inputs for `paths`, reusing this run's already-resolved generations (and their cached
    /// metadata) where the path is known, resolving the rest.
    pub(crate) fn subset(&self, paths: &[PathBuf]) -> Self {
        let known: HashMap<&PathBuf, &Option<Generation>> =
            self.inputs.iter().map(|(p, g)| (p, g)).collect();
        let mut missing = Vec::new();
        for p in paths {
            if !known.contains_key(p) {
                missing.push(p.clone());
            }
        }
        let fresh = Self::resolve(&missing);
        let mut fresh_iter = fresh.inputs.into_iter();
        let inputs = paths
            .iter()
            .filter_map(|p| match known.get(p) {
                Some(g) => Some((p.clone(), (*g).clone())),
                None => fresh_iter.next(),
            })
            .collect();
        Self { inputs }
    }

    /// The inputs in order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = Input<'_>> {
        self.inputs.iter().map(|(path, generation)| Input {
            path,
            generation: generation.as_ref(),
        })
    }

    /// Refuse the whole input set unless every input that has a `Statistics.db` yields trusted
    /// metadata (#4612): the downstream helpers (baseline seeding, schema normalisation) must
    /// never run on a damaged file. A missing `Statistics.db` keeps its existing handling.
    pub(crate) fn require_trusted(&self) -> crate::error::Result<()> {
        for input in self.iter() {
            match input.statistics() {
                Ok(_) => {}
                Err(u) if u.is_absent() => {}
                Err(Unavailable::Failure(MetadataFailure::UnsupportedVersion {
                    version,
                    floor,
                })) => {
                    return Err(Error::UnsupportedVersion {
                        version,
                        floor: floor.to_string(),
                    })
                }
                Err(Unavailable::Failure(MetadataFailure::Unreadable { path, detail })) => {
                    return Err(Error::Io(std::io::Error::other(format!(
                        "Statistics.db {} could not be read: {detail}",
                        path.display()
                    ))))
                }
                Err(other) => return Err(Error::corruption(other.to_string())),
            }
        }
        Ok(())
    }
}
