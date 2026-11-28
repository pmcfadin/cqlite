//! Compaction inputs resolved to generations, so every `Statistics.db` read in this module goes
//! through the generation module's trusted-metadata accessor (G11, issue #4626).
//!
//! The merge entry points take `Data.db` paths. [`InputStats::resolve`] lists each distinct input
//! directory once and pairs every path with the [`Generation`] the module lists for it. Cloning a
//! generation shares its metadata cache, so one run reads each input's `Statistics.db` once however
//! many helpers consult it (GD9.1). A path the listing does not return (hidden by a transaction
//! log, a directory whose listing is refused, a name that is not an SSTable component) is NEVER
//! skipped: its sibling `Statistics.db` is read directly through the same checks (floor, bounded
//! read, strict integrity, one parse), so a damaged file still fails closed (#4612).

#[cfg(test)]
#[path = "input_stats_tests.rs"]
mod tests;

use crate::error::Error;
use crate::parser::repair_metadata::{RepairMetadata, StatsExtras};
use crate::parser::statistics::SSTableStatistics;
use crate::storage::sstable::generation::{
    list_generations, read_direct, DirectStatistics, Generation, MetadataFailure, Scope, TocPolicy,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

/// Why an input's trusted statistics are not available. Only a genuine
/// [`MetadataFailure::Absent`] counts as "no `Statistics.db`"; everything else is a refusal.
#[derive(Debug, Clone)]
pub(crate) struct Unavailable(MetadataFailure);

impl Unavailable {
    /// True only when the input has no `Statistics.db` file.
    pub(crate) fn is_absent(&self) -> bool {
        matches!(self.0, MetadataFailure::Absent { .. })
    }
}

impl std::fmt::Display for Unavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

type DirectCell = Arc<OnceLock<Result<DirectStatistics, MetadataFailure>>>;

/// How one input's statistics are reached: through its listed generation, or, for a path the
/// listing does not return, by a direct trusted read of the sibling `Statistics.db`
/// (floor, bounded read, strict integrity check, one parse; cached per input).
#[derive(Clone)]
enum Source {
    Listed(Generation),
    Direct {
        stats_path: Option<PathBuf>,
        cell: DirectCell,
    },
}

/// One input: its `Data.db` path and where its statistics come from.
#[derive(Clone, Copy)]
pub(crate) struct Input<'a> {
    path: &'a Path,
    source: &'a Source,
}

/// The sibling `Statistics.db` of a `Data.db` path; `None` when the name is not `*Data.db`.
fn sibling_statistics(data_path: &Path) -> Option<PathBuf> {
    let name = data_path.file_name()?.to_str()?;
    let stem = name.strip_suffix("Data.db")?;
    Some(data_path.with_file_name(format!("{stem}Statistics.db")))
}

impl<'a> Input<'a> {
    /// The input's `Data.db` path.
    pub(crate) fn path(&self) -> &'a Path {
        self.path
    }

    fn direct(&self) -> Result<&'a DirectStatistics, Unavailable> {
        let Source::Direct { stats_path, cell } = self.source else {
            unreachable_listed()
        };
        let outcome = cell.get_or_init(|| match stats_path {
            Some(p) => read_direct(p),
            None => Err(MetadataFailure::Absent {
                path: self.path.to_path_buf(),
            }),
        });
        outcome.as_ref().map_err(|e| Unavailable(e.clone()))
    }

    /// The trusted typed statistics (one cached read per input).
    pub(crate) fn statistics(&self) -> Result<Arc<SSTableStatistics>, Unavailable> {
        match self.source {
            Source::Listed(g) => g.trusted_metadata().map_err(Unavailable),
            Source::Direct { .. } => self.direct().map(|d| d.stats.clone()),
        }
    }

    /// The repair fields decoded from the same read.
    pub(crate) fn repair_metadata(&self) -> Result<Arc<RepairMetadata>, Unavailable> {
        match self.source {
            Source::Listed(g) => g.repair_metadata().map_err(Unavailable),
            Source::Direct { .. } => self
                .direct()?
                .repair
                .clone()
                .map_err(|detail| self.corrupt(detail)),
        }
    }

    /// The STATS-extras histograms decoded from the same read.
    pub(crate) fn stats_extras(&self) -> Result<Arc<StatsExtras>, Unavailable> {
        match self.source {
            Source::Listed(g) => g.stats_extras().map_err(Unavailable),
            Source::Direct { .. } => self
                .direct()?
                .extras
                .clone()
                .map_err(|detail| self.corrupt(detail)),
        }
    }

    fn corrupt(&self, detail: String) -> Unavailable {
        let path = match self.source {
            Source::Direct {
                stats_path: Some(p),
                ..
            } => p.clone(),
            _ => self.path.to_path_buf(),
        };
        Unavailable(MetadataFailure::Corrupt { path, detail })
    }
}

/// `Input::direct` is only reached for `Source::Direct`.
fn unreachable_listed() -> ! {
    unreachable!("direct() is called only for Source::Direct")
}

/// The inputs of one merge run, resolved once.
#[derive(Clone, Default)]
pub(crate) struct InputStats {
    inputs: Vec<(PathBuf, Source)>,
}

/// `.` for a bare relative file name.
fn listing_dir(file: &Path) -> Option<PathBuf> {
    match file.parent()? {
        p if p.as_os_str().is_empty() => Some(PathBuf::from(".")),
        p => Some(p.to_path_buf()),
    }
}

fn direct_source(path: &Path) -> Source {
    Source::Direct {
        stats_path: sibling_statistics(path),
        cell: Arc::default(),
    }
}

impl InputStats {
    /// Resolve every path to the generation the module lists for it (Scope Live, `TocMissing`
    /// accepted; an Orphan is kept so a statistics-only fixture still resolves). A path the
    /// listing does not return, or a directory whose listing fails, is read directly and still
    /// checked (#4612): nothing is skipped.
    pub(crate) fn resolve(paths: &[PathBuf]) -> Self {
        let mut listings: HashMap<PathBuf, Vec<Generation>> = HashMap::new();
        let mut inputs = Vec::with_capacity(paths.len());
        for path in paths {
            let generation = listing_dir(path).and_then(|dir| {
                let found = listings.entry(dir.clone()).or_insert_with(|| {
                    match list_generations(&dir, &Scope::Live, TocPolicy::Accept) {
                        Ok(l) => l.live.into_iter().chain(l.orphans).collect(),
                        // A refused listing leaves every input of this directory unresolved,
                        // which reads each sibling `Statistics.db` directly (never skipped).
                        Err(_) => Vec::new(),
                    }
                });
                let file = path.file_name()?.to_str()?;
                found
                    .iter()
                    .find(|g| format!("{}-Data.db", g.base_name()) == file)
                    .cloned()
            });
            let source = match generation {
                Some(g) => Source::Listed(g),
                None => direct_source(path),
            };
            inputs.push((path.clone(), source));
        }
        Self { inputs }
    }

    /// Inputs that are already generations (a listing's), so the run shares the metadata cache of
    /// the caller's clones: `diagnose --deep` reads each `Statistics.db` once for the cheap tier,
    /// the deep scan and the dry compaction (GD9.1).
    #[cfg(not(feature = "tombstones"))]
    pub(crate) fn from_generations(inputs: Vec<(PathBuf, Generation)>) -> Self {
        Self {
            inputs: inputs
                .into_iter()
                .map(|(p, g)| (p, Source::Listed(g)))
                .collect(),
        }
    }

    /// The inputs for `paths`, reusing this run's already-resolved sources (and their cached
    /// metadata) where the path is known, resolving the rest. Order and duplicates follow
    /// `paths`.
    pub(crate) fn subset(&self, paths: &[PathBuf]) -> Self {
        let known: HashMap<&PathBuf, &Source> = self.inputs.iter().map(|(p, s)| (p, s)).collect();
        let missing: Vec<PathBuf> = paths
            .iter()
            .filter(|p| !known.contains_key(p))
            .cloned()
            .collect();
        let mut fresh = Self::resolve(&missing).inputs.into_iter();
        let inputs = paths
            .iter()
            .filter_map(|p| match known.get(p) {
                Some(s) => Some((p.clone(), (*s).clone())),
                None => fresh.next(),
            })
            .collect();
        Self { inputs }
    }

    /// The inputs in order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = Input<'_>> {
        self.inputs
            .iter()
            .map(|(path, source)| Input { path, source })
    }

    /// Refuse the whole input set unless every input that has a `Statistics.db` yields trusted
    /// metadata (#4612): the downstream helpers (baseline seeding, schema normalisation) must
    /// never run on a damaged file. A missing `Statistics.db` keeps its existing handling.
    pub(crate) fn require_trusted(&self) -> crate::error::Result<()> {
        for input in self.iter() {
            match input.statistics() {
                Ok(_) => {}
                Err(u) if u.is_absent() => {}
                Err(Unavailable(MetadataFailure::UnsupportedVersion { version, floor })) => {
                    return Err(Error::UnsupportedVersion {
                        version,
                        floor: floor.to_string(),
                    })
                }
                Err(Unavailable(MetadataFailure::Unreadable { path, detail })) => {
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
