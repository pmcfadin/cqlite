//! Shared harness for the `fuzz_audit` / `fuzz_find` targets (issue #4502, criteria 2-4).
//!
//! Both targets fuzz a whole generation DIRECTORY, so each execution:
//! 1. copies a small, real, Cassandra-written BASE generation (resolved from
//!    `CQLITE_DATASETS_ROOT`; the target is a no-op without one, like `fuzz_block_emit`),
//! 2. replaces (or creates) exactly ONE component with the fuzz bytes, and
//! 3. drives the real tool entry point on the copy.
//!
//! Input layout: `data[0]` is a control byte (bit 7 = base/mode, bit 6 = deep/second flag,
//! bits 0..=3 = component index into [`COMPONENTS`], taken modulo its length); `data[1..]` is the
//! replacement bytes. An EMPTY `data` leaves the base untouched. The seed corpus
//! (`fuzz/seed_audit_find.py`) is generated from the corruption corpus
//! (`generate-corruption-corpus.sh` outputs under `corruption/test_comp_corrupt/`).

#![allow(dead_code)]

use std::path::{Path, PathBuf};

/// The component suffixes a generation may carry, in a FIXED order (the control byte indexes it).
pub const COMPONENTS: [&str; 10] = [
    "Data.db",
    "Index.db",
    "Summary.db",
    "Filter.db",
    "CompressionInfo.db",
    "Statistics.db",
    "Digest.crc32",
    "TOC.txt",
    "Partitions.db",
    "Rows.db",
];

/// A decoded fuzz input.
pub struct Input<'a> {
    pub high: bool,
    pub flag: bool,
    pub component: &'static str,
    pub bytes: &'a [u8],
}

pub fn decode(data: &[u8]) -> Option<Input<'_>> {
    let (&ctl, bytes) = data.split_first()?;
    Some(Input {
        high: ctl & 0x80 != 0,
        flag: ctl & 0x40 != 0,
        component: COMPONENTS[usize::from(ctl & 0x0f) % COMPONENTS.len()],
        bytes,
    })
}

/// Whether the replacement leaves `component` UNREADABLE by any parser (the #4159 class): a
/// binary component shorter than 4 bytes has no room for even a header word; an empty `TOC.txt`
/// lists nothing.
pub fn is_unreadable(component: &str, bytes: &[u8]) -> bool {
    if component == "TOC.txt" {
        bytes.is_empty()
    } else {
        bytes.len() < 4
    }
}

/// `<root>/sstables/<keyspace>/<table>-<id>/` of the first matching table directory.
pub fn table_dir(keyspace: &str, table: &str) -> Option<PathBuf> {
    let root = PathBuf::from(std::env::var_os("CQLITE_DATASETS_ROOT")?);
    let ks = root.join("sstables").join(keyspace);
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(ks)
        .ok()?
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.rsplit_once('-'))
                .is_some_and(|(t, id)| t == table && id.len() == 32)
        })
        .collect();
    dirs.sort();
    dirs.into_iter().next()
}

/// Copy every real component (not the `.jsonl`/`.txt` oracles) of `src` into a fresh tempdir.
pub fn copy_generation(src: &Path) -> Option<tempfile::TempDir> {
    let tmp = tempfile::tempdir().ok()?;
    for e in std::fs::read_dir(src).ok()?.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if COMPONENTS.iter().any(|c| name.ends_with(c)) && !name.ends_with(".txt.jsonl") {
            std::fs::copy(e.path(), tmp.path().join(&name)).ok()?;
        }
    }
    Some(tmp)
}

/// The path of `component` in the copied generation (created when the base lacks it): the
/// existing file whose name ends with the suffix, else `<data prefix><suffix>`.
pub fn component_path(dir: &Path, component: &str) -> Option<PathBuf> {
    let mut data_prefix = None;
    for e in std::fs::read_dir(dir).ok()?.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if name.ends_with(component) {
            return Some(e.path());
        }
        if let Some(p) = name.strip_suffix("Data.db") {
            data_prefix = Some(p.to_string());
        }
    }
    data_prefix.map(|p| dir.join(format!("{p}{component}")))
}

pub fn data_db(dir: &Path) -> Option<PathBuf> {
    component_path(dir, "Data.db")
}

/// Apply the replacement. Returns `false` when it could not be applied (the exec is skipped).
pub fn apply(dir: &Path, component: &str, bytes: &[u8]) -> bool {
    match component_path(dir, component) {
        Some(p) => std::fs::write(p, bytes).is_ok(),
        None => false,
    }
}

/// A process-wide current-thread runtime (one per fuzz process, never per exec).
pub fn runtime() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
    })
}
