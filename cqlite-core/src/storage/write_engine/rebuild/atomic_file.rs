//! Atomic single-file publication (issues #4528, #4549), the one-file sibling of
//! [`StagedPublish`](super::staged_publish::StagedPublish): used for `--manifest` reports.
//!
//! The content is streamed into a UNIQUE temp file in the destination's directory, created with
//! `create_new` (`O_EXCL`, which never follows a symlink and never reuses an inode another writer
//! holds), flushed, fsynced, renamed over the destination, and the directory is fsynced. After a
//! fault at any point the destination is absent, the previous content, or the complete new
//! content, never a torn file.
//!
//! The temp name is `.<name>.<pid>-<counter>.wtmp`: dot-prefixed (never an SSTable component) and
//! not matching `staged_publish::is_stage_name` (so a leftover is never mistaken for a stage).

use crate::error::{Error, Result};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Bounded retries when a candidate temp name already exists.
const MAX_TEMP_ATTEMPTS: u32 = 32;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn create_unique_temp(dest: &Path, name: &str) -> Result<(std::fs::File, PathBuf)> {
    let pid = std::process::id();
    let mut last = None;
    for _ in 0..MAX_TEMP_ATTEMPTS {
        let n = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp = dest.with_file_name(format!(".{name}.{pid}-{n}.wtmp"));
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
        {
            Ok(f) => return Ok((f, tmp)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => last = Some((tmp, e)),
            Err(e) => {
                return Err(Error::Storage(format!(
                    "failed to create temp file {}: {e}",
                    tmp.display()
                )))
            }
        }
    }
    let (tmp, e) = last.expect("loop ran at least once");
    Err(Error::Storage(format!(
        "failed to create a unique temp file next to {} (last tried {}): {e}",
        dest.display(),
        tmp.display()
    )))
}

/// Publish ONE file atomically at `dest`, streaming its content through `write` into a buffered
/// temp file (no second resident copy). Order: write, flush, `sync_all`, `rename`, fsync the parent
/// directory. Every step's error is returned naming the path and cause; the temp is removed on any
/// failure before the rename. The parent must exist. On Unix a `dest` that is itself a symlink is
/// refused (the temp is never written through, and the link is never silently replaced).
pub fn write_file_atomic_with<F>(dest: &Path, write: F) -> Result<()>
where
    F: FnOnce(&mut dyn Write) -> std::io::Result<()>,
{
    let name = dest
        .file_name()
        .ok_or_else(|| Error::Storage(format!("{} has no file name", dest.display())))?
        .to_string_lossy()
        .into_owned();
    // Unix: refuse a symlink destination; for an existing regular file remember its mode so the
    // replacement keeps it (a 0600 manifest must not become 0644 under a 022 umask). Only the mode
    // is preserved, never ownership (no chown). A new destination keeps the umask default.
    #[cfg(unix)]
    let existing_mode = match std::fs::symlink_metadata(dest) {
        Ok(m) if m.file_type().is_symlink() => {
            return Err(Error::Storage(format!(
                "refusing to write {}: the destination is a symlink",
                dest.display()
            )));
        }
        Ok(m) if m.file_type().is_file() => {
            use std::os::unix::fs::PermissionsExt;
            Some(m.permissions().mode() & 0o7777)
        }
        _ => None,
    };
    let (file, tmp) = create_unique_temp(dest, &name)?;
    #[cfg(unix)]
    if let Some(mode) = existing_mode {
        use std::os::unix::fs::PermissionsExt;
        // Applied while the temp is still empty, before any content is written.
        if let Err(e) = file.set_permissions(std::fs::Permissions::from_mode(mode)) {
            let _ = std::fs::remove_file(&tmp);
            return Err(Error::Storage(format!(
                "failed to set mode {mode:o} on {}: {e}",
                tmp.display()
            )));
        }
    }
    let staged = (|| -> std::io::Result<()> {
        let mut w = std::io::BufWriter::new(file);
        write(&mut w)?;
        w.flush()?;
        w.get_ref().sync_all()
    })();
    if let Err(e) = staged {
        let _ = std::fs::remove_file(&tmp); // best effort; the write error is what is reported
        return Err(Error::Storage(format!(
            "failed to write and fsync {}: {e}",
            tmp.display()
        )));
    }
    if let Err(e) = std::fs::rename(&tmp, dest) {
        let _ = std::fs::remove_file(&tmp);
        return Err(Error::Storage(format!(
            "failed to rename {} -> {}: {e}",
            tmp.display(),
            dest.display()
        )));
    }
    let parent = match dest.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    fsync_dir(parent)
}

/// [`write_file_atomic_with`] for content already in memory.
pub fn write_file_atomic(dest: &Path, bytes: &[u8]) -> Result<()> {
    write_file_atomic_with(dest, |w| w.write_all(bytes))
}

/// fsync a directory so its entries are durable. Unix: `open` + `fsync`. Windows: open the
/// directory with `FILE_FLAG_BACKUP_SEMANTICS` and `FlushFileBuffers` (`sync_all`), which needs
/// GENERIC_WRITE. Any other platform returns an error so a caller that deletes data afterwards
/// refuses instead of proceeding without durability.
pub fn fsync_dir(dir: &Path) -> Result<()> {
    fsync_dir_impl(dir)
        .map_err(|e| Error::Storage(format!("failed to fsync directory {}: {e}", dir.display())))
}

#[cfg(unix)]
fn fsync_dir_impl(dir: &Path) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

#[cfg(windows)]
fn fsync_dir_impl(dir: &Path) -> std::io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(dir)?
        .sync_all()
}

#[cfg(not(any(unix, windows)))]
fn fsync_dir_impl(_dir: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "directory fsync is not supported on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(dir: &Path) -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        v.sort();
        v
    }

    #[test]
    fn publishes_whole_file_and_leaves_no_temp() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("manifest.json");
        std::fs::write(&p, b"old").unwrap();
        write_file_atomic(&p, b"{\"new\":true}").unwrap();
        assert_eq!(std::fs::read(&p).unwrap(), b"{\"new\":true}");
        assert_eq!(names(dir.path()), vec!["manifest.json".to_string()]);
    }

    #[test]
    fn failure_names_the_path_and_publishes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("no-such-dir").join("manifest.json");
        let err = write_file_atomic(&p, b"x").unwrap_err().to_string();
        assert!(err.contains("manifest.json"), "{err}");
        assert!(!p.exists());
    }

    #[test]
    fn callback_failure_removes_the_temp_and_keeps_the_old_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("manifest.json");
        std::fs::write(&p, b"old").unwrap();
        let err = write_file_atomic_with(&p, |w| {
            w.write_all(b"partial")?;
            Err(std::io::Error::other("boom"))
        })
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("manifest.json") && err.contains("boom"),
            "{err}"
        );
        assert_eq!(std::fs::read(&p).unwrap(), b"old");
        assert_eq!(names(dir.path()), vec!["manifest.json".to_string()]);
    }

    #[test]
    fn temp_name_is_not_a_stage_name_or_component() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("m.json");
        let mut seen = String::new();
        write_file_atomic_with(&p, |_| {
            seen = names(dir.path())
                .into_iter()
                .find(|n| n.ends_with(".wtmp"))
                .unwrap_or_default();
            Ok(())
        })
        .unwrap();
        assert!(seen.starts_with(".m.json."), "{seen}");
        assert!(!super::super::staged_publish::is_stage_name(&seen));
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_at_the_old_predictable_temp_name_is_not_followed() {
        let dir = tempfile::tempdir().unwrap();
        let victim = dir.path().join("victim.txt");
        std::fs::write(&victim, b"precious").unwrap();
        let p = dir.path().join("manifest.json");
        std::os::unix::fs::symlink(&victim, dir.path().join(".manifest.json.tmp")).unwrap();
        write_file_atomic(&p, b"new").unwrap();
        assert_eq!(std::fs::read(&victim).unwrap(), b"precious");
        assert_eq!(std::fs::read(&p).unwrap(), b"new");
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_at_a_candidate_temp_name_is_skipped_not_followed() {
        let dir = tempfile::tempdir().unwrap();
        let victim = dir.path().join("victim.txt");
        std::fs::write(&victim, b"precious").unwrap();
        let p = dir.path().join("manifest.json");
        let pid = std::process::id();
        // Pre-plant symlinks at the next candidates; create_new must refuse each and move on.
        let next = TEMP_COUNTER.load(Ordering::Relaxed);
        for n in next..next + 64 {
            let _ = std::os::unix::fs::symlink(
                &victim,
                dir.path().join(format!(".manifest.json.{pid}-{n}.wtmp")),
            );
        }
        // Other tests share the counter, so the window may be skipped entirely: the invariant is
        // only that no planted link is ever written through.
        let _ = write_file_atomic(&p, b"new");
        assert_eq!(std::fs::read(&victim).unwrap(), b"precious");
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_destination_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let victim = dir.path().join("victim.txt");
        std::fs::write(&victim, b"precious").unwrap();
        let p = dir.path().join("manifest.json");
        std::os::unix::fs::symlink(&victim, &p).unwrap();
        let err = write_file_atomic(&p, b"new").unwrap_err().to_string();
        assert!(err.contains("symlink"), "{err}");
        assert_eq!(std::fs::read(&victim).unwrap(), b"precious");
    }

    #[cfg(unix)]
    fn mode_of(p: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(p).unwrap().permissions().mode() & 0o7777
    }

    #[cfg(unix)]
    #[test]
    fn overwrite_preserves_the_existing_mode() {
        use std::os::unix::fs::PermissionsExt;
        for mode in [0o600, 0o640] {
            let dir = tempfile::tempdir().unwrap();
            let p = dir.path().join("manifest.json");
            std::fs::write(&p, b"old").unwrap();
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(mode)).unwrap();
            write_file_atomic(&p, b"new").unwrap();
            assert_eq!(std::fs::read(&p).unwrap(), b"new");
            assert_eq!(mode_of(&p), mode);
        }
    }

    #[cfg(unix)]
    #[test]
    fn new_destination_gets_the_umask_default() {
        let dir = tempfile::tempdir().unwrap();
        let reference = dir.path().join("reference");
        std::fs::File::create(&reference).unwrap(); // what plain creation yields under this umask
        let p = dir.path().join("manifest.json");
        write_file_atomic(&p, b"new").unwrap();
        assert_eq!(mode_of(&p), mode_of(&reference));
    }

    #[test]
    fn concurrent_writers_leave_one_complete_version_and_no_temp() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("manifest.json");
        let a = vec![b'a'; 1 << 20];
        let b = vec![b'b'; 1 << 20];
        std::thread::scope(|s| {
            for _ in 0..8 {
                s.spawn(|| write_file_atomic(&p, &a).unwrap());
                s.spawn(|| write_file_atomic(&p, &b).unwrap());
            }
        });
        let got = std::fs::read(&p).unwrap();
        assert!(got == a || got == b, "torn or mixed content");
        assert_eq!(names(dir.path()), vec!["manifest.json".to_string()]);
    }
}
