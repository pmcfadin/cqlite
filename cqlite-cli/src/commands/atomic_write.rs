//! Atomic file publication for the write verbs' `--manifest` output (issue #4549).
//!
//! `File::create` / `fs::write` truncate the destination IN PLACE, so a crash (or an ENOSPC / short
//! write) part-way through leaves a 0-byte or truncated manifest that looks like a finished one. The
//! file is instead written to a dot-prefixed temp sibling, fsynced, `rename`d over the final name
//! (atomic on POSIX), and the directory is fsynced so the rename itself is durable: a reader sees
//! the old manifest, no manifest, or the complete new one — never a torn one.
//!
//! This duplicates the minimal temp+fsync+rename+dir-fsync sequence `TocWriter::write` uses for
//! `TOC.txt` (#4526, `cqlite-core`): the two live in different crates and the branches are
//! deliberately not coupled.

use std::io::Write;
use std::path::Path;

/// Write `bytes` to `path` atomically (see the module doc). The parent directory must exist.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let name = path.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{} has no file name", path.display()),
        )
    })?;
    let tmp = path.with_file_name(format!(".{}.tmp", name.to_string_lossy()));
    let result = (|| -> std::io::Result<()> {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(bytes)?;
        file.flush()?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, path)
    })();
    if let Err(e) = result {
        // Never leave the temp sibling behind after a reported failure (best effort).
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    #[cfg(unix)]
    {
        let dir = match path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => Path::new("."),
        };
        std::fs::File::open(dir).and_then(|d| d.sync_all())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::write_atomic;

    #[test]
    fn publishes_whole_file_and_leaves_no_temp_sibling() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("manifest.json");
        std::fs::write(&p, b"old").unwrap();
        write_atomic(&p, br#"{"new":true}"#).unwrap();
        assert_eq!(std::fs::read(&p).unwrap(), br#"{"new":true}"#);
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            names,
            vec!["manifest.json".to_string()],
            "temp sibling left: {names:?}"
        );
    }

    #[test]
    fn failure_publishes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("no-such-dir").join("manifest.json");
        assert!(write_atomic(&p, b"x").is_err());
        assert!(!p.exists());
    }
}
