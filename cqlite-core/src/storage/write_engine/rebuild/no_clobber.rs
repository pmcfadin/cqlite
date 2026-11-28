//! No-clobber publication primitives shared by every writer of a NEW generation (#4623, #4626):
//! `StagedPublish` (CLI `compact`, flush, `rebuild`, `salvage`) and the engine's compaction publish.
//!
//! POSIX `rename(2)` silently replaces an existing destination, so two writers that picked the same
//! generation id would overwrite each other's `nb-<id>-big-*` files. `link(2)` fails with
//! `EEXIST` instead, so a file is published by linking the staged file to its final name (never
//! replacing anything) and the staged copy is dropped with the stage. A writer may therefore only
//! ever remove final names it linked itself.

use std::path::Path;

/// How many times a publish re-allocates an id after losing a race for it before it gives up with a
/// named error.
pub const MAX_PUBLISH_ATTEMPTS: u32 = 8;

/// Link `src` to `dst` WITHOUT replacing an existing `dst` (`ErrorKind::AlreadyExists`).
pub fn link_no_clobber(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::hard_link(src, dst)
}

/// `true` when `e` means "the destination already exists".
pub fn is_collision(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::AlreadyExists
}

/// `name` (`<version>-<old>-<format>-<component>`) with its id token replaced by `new`; `name`
/// unchanged when its id token is not `old`.
pub fn retarget_name(name: &str, old: u64, new: u64) -> String {
    let mut parts = name.splitn(3, '-');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(version), Some(id), Some(rest)) if id == old.to_string() => {
            format!("{version}-{new}-{rest}")
        }
        _ => name.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retarget_replaces_only_the_id_token() {
        assert_eq!(
            retarget_name("nb-3-big-Data.db", 3, 9),
            "nb-9-big-Data.db"
        );
        assert_eq!(retarget_name("nb-4-big-Data.db", 3, 9), "nb-4-big-Data.db");
        assert_eq!(retarget_name("TOC.txt", 3, 9), "TOC.txt");
    }

    #[test]
    fn link_refuses_an_existing_destination_and_keeps_its_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let (a, b) = (tmp.path().join("a"), tmp.path().join("b"));
        std::fs::write(&a, b"new").unwrap();
        std::fs::write(&b, b"existing").unwrap();
        let err = link_no_clobber(&a, &b).unwrap_err();
        assert!(is_collision(&err));
        assert_eq!(std::fs::read(&b).unwrap(), b"existing");
    }
}
