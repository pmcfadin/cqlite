//! Generation discovery for `diagnose` (issue #4204): which `*-Data.db` files a table dir holds,
//! and which directory entries could not even be read. Never opens any SSTable component.

use std::io;
use std::path::{Path, PathBuf};

/// Outcome of listing a table directory: the generations found (sorted), and one cause string per
/// directory entry the OS could not hand back. An unreadable entry may have been a generation, so
/// it is reported (spec R6: a field of the report, never silently absent).
#[derive(Debug, Default)]
pub(crate) struct Discovered {
    pub data_paths: Vec<PathBuf>,
    pub unreadable_entries: Vec<String>,
}

/// Classify the entries of an already-opened directory listing. Generic over the entry iterator so
/// the per-entry error path is testable without an unreadable file system.
pub(crate) fn classify_entries(entries: impl Iterator<Item = io::Result<PathBuf>>) -> Discovered {
    let mut out = Discovered::default();
    for entry in entries {
        match entry {
            Ok(path) => {
                let is_data = path.is_file()
                    && path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.ends_with("-Data.db"));
                if is_data {
                    out.data_paths.push(path);
                }
            }
            Err(e) => out
                .unreadable_entries
                .push(format!("a directory entry could not be read: {e}")),
        }
    }
    out.data_paths.sort();
    out
}

/// List `table_dir` (no recursion). `Err` only when the directory itself cannot be listed.
pub(crate) fn discover(table_dir: &Path) -> io::Result<Discovered> {
    let rd = std::fs::read_dir(table_dir)?;
    Ok(classify_entries(rd.map(|e| e.map(|e| e.path()))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_entry_error_is_reported_not_swallowed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let good = dir.path().join("nb-1-big-Data.db");
        std::fs::write(&good, b"x").expect("write");
        let entries = vec![
            Ok(good.clone()),
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "injected EACCES",
            )),
            Ok(dir.path().join("nb-1-big-Index.db")),
        ];
        let found = classify_entries(entries.into_iter());
        assert_eq!(found.data_paths, vec![good]);
        assert_eq!(found.unreadable_entries.len(), 1);
        assert!(found.unreadable_entries[0].contains("injected EACCES"));
    }
}
