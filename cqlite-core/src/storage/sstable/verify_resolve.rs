//! Component resolution for the verifier, through the generation module (issue #4626, GD11).
//!
//! `verify` used to scan the directory itself, pick the lexicographically first `Data.db`, fall back
//! to filename heuristics when the descriptor did not parse and default to BIG. All of that is the
//! generation module's job now: this file lists the directory (Scope `Live`, which never enters
//! `snapshots/` or `backups/`), selects the generation and turns the module's typed outcomes into
//! the verifier's existing behaviour. It classifies nothing itself:
//!
//! - below the version floor: a refusal naming the floor, before any component is opened (GD11.2);
//! - `TocMissing`: the existing missing-`TOC.txt` finding, raised by `check_toc_and_presence`;
//! - `Corrupt` Statistics: the existing Statistics integrity finding, raised by `check_statistics`;
//! - `Orphan`: reported by name, never verified as live.

use crate::parser::statistics::SSTableStatistics;
use crate::storage::sstable::generation::check_floor;
use crate::storage::sstable::generation::{
    list_generations, read_direct_trusted, Condition, Generation, Listing, MetadataFailure, Scope,
    TocPolicy,
};
use crate::storage::sstable::reader::extract_sstable_base_name;
use crate::storage::sstable::version_gate::{SsTableDescriptor, SsTableFormat};
use crate::{Error, Result};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

const DATA_COMPONENT: &str = "Data.db";

/// Resolved set of component files for one SSTable generation in a directory.
/// `pub(crate)`: `verify_location::finalize_locations` reads `format`/`path()`.
pub(crate) struct ComponentSet {
    pub(crate) base_name: String,
    pub(crate) format: SsTableFormat,
    /// Map of bare component name (e.g. `Data.db`) -> absolute path on disk.
    pub(crate) present: BTreeMap<String, PathBuf>,
    pub(crate) data_path: PathBuf,
    /// The module's `TocMissing` condition: `Data.db` present, `TOC.txt` absent.
    pub(crate) toc_missing: bool,
    stats: StatsSource,
}

/// Where the trusted `Statistics.db` comes from.
type StatsCell = OnceLock<std::result::Result<Arc<SSTableStatistics>, MetadataFailure>>;

enum StatsSource {
    /// A listed generation: the module's cached accessor.
    Listed(Generation),
    /// An exact path the listing does not return as a `Data.db` (see [`resolve_exact`]): the
    /// module's trusted direct read of this sibling, or none if the sibling is absent.
    Direct(Option<PathBuf>, PathBuf, StatsCell),
}

impl ComponentSet {
    /// Convert a listed generation. No refusal happens here; [`resolve_first_live`] and
    /// [`resolve_exact`] decide what may be verified.
    pub(crate) fn from_generation(generation: Generation) -> Self {
        let base_name = generation.base_name();
        let present = generation
            .components()
            .iter()
            .map(|c| (c.name.clone(), c.path.clone()))
            .collect();
        let data_path = generation
            .component_path(DATA_COMPONENT)
            .map(Path::to_path_buf)
            .unwrap_or_else(|| {
                generation
                    .directory()
                    .join(format!("{base_name}-{DATA_COMPONENT}"))
            });
        Self {
            base_name,
            format: generation.format(),
            present,
            data_path,
            toc_missing: generation.has_condition(Condition::TocMissing),
            stats: StatsSource::Listed(generation),
        }
    }

    pub(crate) fn path(&self, dir: &Path, component: &str) -> PathBuf {
        dir.join(format!("{}-{}", self.base_name, component))
    }

    /// `true` when a component (e.g. `Statistics.db`) is present on disk for this generation, per
    /// the listing taken at resolution time.
    pub(crate) fn has(&self, component: &str) -> bool {
        self.present.contains_key(component)
    }

    /// The generation's `Statistics.db` through the module's one trusted-metadata accessor: read
    /// once, cached, value or typed failure.
    pub(crate) fn statistics(
        &self,
    ) -> std::result::Result<Arc<SSTableStatistics>, MetadataFailure> {
        match &self.stats {
            StatsSource::Listed(generation) => generation.trusted_metadata(),
            // Read once per resolution, like the listed generation's cache: every caller (the
            // Statistics check and the audit row) sees the same value or the same failure.
            StatsSource::Direct(path, would_be, cell) => cell
                .get_or_init(|| match path {
                    Some(path) => read_direct_trusted(path),
                    None => Err(MetadataFailure::Absent {
                        path: would_be.clone(),
                    }),
                })
                .clone(),
        }
    }
}

fn list_live(dir: &Path) -> Result<Listing> {
    list_generations(dir, &Scope::Live, TocPolicy::Accept).map_err(|e| {
        Error::invalid_path(format!("Cannot list SSTable dir {}: {}", dir.display(), e))
    })
}

/// The floor refusal (GD11.2), from the descriptor alone: no component has been opened.
fn refuse_below_floor(generation: &Generation) -> Result<()> {
    match generation.below_floor() {
        Some(below) => Err(Error::UnsupportedVersion {
            version: below.version.clone(),
            floor: below.floor.to_string(),
        }),
        None => Ok(()),
    }
}

fn refuse_orphan(generation: &Generation) -> Error {
    Error::not_found(format!(
        "{} in {} is an orphan (Data.db missing or empty), not a live SSTable",
        generation.base_name(),
        generation.directory().display()
    ))
}

/// The first live generation of `dir`, by Cassandra's id order. A directory input is Scope `Live`.
pub(crate) fn resolve_first_live(dir: &Path) -> Result<ComponentSet> {
    let listing = list_live(dir)?;
    let Some(generation) = listing.live.first() else {
        return Err(match listing.orphans.first() {
            Some(orphan) => refuse_orphan(orphan),
            None => Error::not_found(format!(
                "No *-Data.db component found in SSTable directory {}",
                dir.display()
            )),
        });
    };
    refuse_below_floor(generation)?;
    Ok(ComponentSet::from_generation(generation.clone()))
}

/// The EXACT generation whose `Data.db` is `data_path` (issue #1283): matched by the file name the
/// listing reports, so a relative, directory-less path resolves against the same directory.
pub(crate) fn resolve_exact(dir: &Path, data_path: &Path) -> Result<ComponentSet> {
    let listing = list_live(dir)?;
    let wanted = data_path.file_name();
    let is_it = |g: &Generation| {
        g.component_path(DATA_COMPONENT)
            .and_then(Path::file_name)
            .is_some_and(|n| Some(n) == wanted)
    };
    if let Some(generation) = listing.live.iter().find(|g| is_it(g)) {
        refuse_below_floor(generation)?;
        return Ok(ComponentSet::from_generation(generation.clone()));
    }
    if let Some(orphan) = listing.orphans.iter().find(|g| is_it(g)) {
        return Err(refuse_orphan(orphan));
    }
    resolve_noncanonical(dir, data_path, &listing)
}

/// The exact-path resolution for a path the listing does not return as a `Data.db` component
/// (issue #1283): `SSTableReader::open` accepts such names (it simply skips siblings it cannot
/// map), so a reader that opened must still get a verdict, never an error. The base name is the
/// reader's own derivation, the siblings are the files that actually exist (taken from the
/// module's listing, not a second scan), and `Statistics.db` is read through the module's trusted
/// direct read. The version floor is applied from the parsed descriptor when the name has one;
/// the format is the descriptor's when it parses and BIG otherwise, as the reader's own
/// nb-compatible default (no format is inferred from anything else).
fn resolve_noncanonical(dir: &Path, data_path: &Path, listing: &Listing) -> Result<ComponentSet> {
    if !data_path.is_file() {
        return Err(Error::not_found(format!(
            "SSTable Data.db component not found at {}",
            data_path.display()
        )));
    }
    let data_name = data_path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| Error::invalid_path("Data.db filename is not valid UTF-8"))?;
    let descriptor = SsTableDescriptor::parse_filename(data_name).ok();
    if let Some(d) = &descriptor {
        if let Err(below) = check_floor(&d.version, d.format) {
            return Err(Error::UnsupportedVersion {
                version: below.version,
                floor: below.floor.to_string(),
            });
        }
    }
    let base_name = data_name
        .strip_suffix("-Data.db")
        .map(str::to_string)
        .or_else(|| extract_sstable_base_name(data_path))
        .unwrap_or_else(|| {
            data_name
                .strip_suffix(".db")
                .unwrap_or(data_name)
                .to_string()
        });
    let prefix = format!("{base_name}-");
    let mut present = BTreeMap::new();
    let listed = listing
        .live
        .iter()
        .chain(&listing.orphans)
        .flat_map(|g| g.components().iter().map(|c| c.path.as_path()))
        .chain(listing.ignored.iter().map(PathBuf::as_path));
    for path in listed {
        if let Some(component) = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_prefix(&prefix))
        {
            present.insert(component.to_string(), path.to_path_buf());
        }
    }
    let stats_name = format!("{base_name}-Statistics.db");
    let stats_path = present.get("Statistics.db").cloned();
    Ok(ComponentSet {
        toc_missing: !present.contains_key("TOC.txt"),
        format: descriptor.map(|d| d.format).unwrap_or(SsTableFormat::Big),
        base_name,
        present,
        data_path: data_path.to_path_buf(),
        stats: StatsSource::Direct(stats_path, dir.join(stats_name), OnceLock::new()),
    })
}
