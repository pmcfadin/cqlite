//! Path-safety guards for attacker-controlled Flight ticket fields (issue #1430).
//!
//! A Flight ticket (or a `table_stats` request) carries `keyspace`, `table`, and
//! an optional `snapshot` name that the server resolves into filesystem paths
//! under its `data_dir`. Without validation an attacker can supply `../` or an
//! absolute component and escape the data directory — `Path::join` with an
//! absolute component silently replaces the whole path — to read arbitrary
//! `*-Data.db` files. These helpers are the shared, dependency-free guards that
//! reject such inputs at parse time and, as defense in depth, verify a resolved
//! path stays within the (canonicalized) data directory.
use std::path::{Path, PathBuf};

/// A rejected attacker-controlled path component.
#[derive(Debug, thiserror::Error)]
pub enum PathSafetyError {
    /// A field failed charset/emptiness validation.
    #[error("invalid {field}: {reason}")]
    InvalidField {
        /// Which ticket field was rejected (`keyspace`, `table`, `snapshot`, …).
        field: &'static str,
        /// Why it was rejected.
        reason: &'static str,
    },
    /// A resolved path escaped the data directory (e.g. via a symlink).
    #[error("resolved path for {field} escapes the data directory")]
    Escapes {
        /// Which ticket field produced the escaping path.
        field: &'static str,
    },
}

/// Validate a Cassandra unquoted identifier used as a path component.
///
/// Accepts a non-empty string whose every character is ASCII alphanumeric or
/// `_` — exactly the grammar of a Cassandra unquoted identifier. This inherently
/// rejects `.`, `/`, `\`, NUL, `-`, whitespace, and any absolute or relative
/// path syntax, so the value can never traverse out of or replace the data
/// directory when joined as a path component.
pub fn validate_identifier(field: &'static str, name: &str) -> Result<(), PathSafetyError> {
    if name.is_empty() {
        return Err(PathSafetyError::InvalidField {
            field,
            reason: "must not be empty",
        });
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(PathSafetyError::InvalidField {
            field,
            reason: "only ASCII letters, digits, and '_' are allowed",
        });
    }
    Ok(())
}

/// Validate a snapshot name used as a path component.
///
/// Like [`validate_identifier`] but also allows `-`, since Cassandra/cqlite
/// snapshot names contain hyphens (e.g. `cqlite-abc`). Still rejects `.`, `/`,
/// `\`, NUL, whitespace, and any absolute or relative path syntax.
pub fn validate_snapshot(name: &str) -> Result<(), PathSafetyError> {
    if name.is_empty() {
        return Err(PathSafetyError::InvalidField {
            field: "snapshot",
            reason: "must not be empty",
        });
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(PathSafetyError::InvalidField {
            field: "snapshot",
            reason: "only ASCII letters, digits, '_', and '-' are allowed",
        });
    }
    Ok(())
}

/// Defense-in-depth check that `target` stays within `root` after resolving
/// symlinks.
///
/// Charset validation ([`validate_identifier`]/[`validate_snapshot`]) already
/// prevents `../` and absolute components in ticket fields; this guards the
/// residual case where a symlink *inside* the data directory points outside it.
///
/// `target` need not exist yet — a legitimately-absent table directory must not
/// error here. We canonicalize the nearest existing ancestor of `target` (the
/// data directory itself always exists, so the walk terminates) and require it
/// to start with the canonicalized `root`.
pub fn assert_within(
    field: &'static str,
    root: &Path,
    target: &Path,
) -> Result<(), PathSafetyError> {
    let canon_root = root
        .canonicalize()
        .map_err(|_| PathSafetyError::InvalidField {
            field: "data_dir",
            reason: "data directory is not accessible",
        })?;

    // Walk up from `target` to the deepest ancestor that exists on disk and can
    // be canonicalized. `data_dir` always exists, so this cannot loop forever;
    // if we exhaust all components the path is unrelated to the data dir.
    let mut probe: PathBuf = target.to_path_buf();
    let canon_target = loop {
        match probe.canonicalize() {
            Ok(resolved) => break resolved,
            Err(_) => {
                if !probe.pop() {
                    return Err(PathSafetyError::Escapes { field });
                }
            }
        }
    };

    if canon_target.starts_with(&canon_root) {
        Ok(())
    } else {
        Err(PathSafetyError::Escapes { field })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifier_accepts_cassandra_names() {
        for ok in ["test_basic", "simple_table", "a", "A1", "x_9_Y", "items"] {
            assert!(
                validate_identifier("keyspace", ok).is_ok(),
                "{ok} should pass"
            );
        }
    }

    #[test]
    fn identifier_rejects_traversal_and_absolute_and_special() {
        for bad in [
            "", "a/b", "..", "../x", "a/../b", "/abs", "abs/", ".", "a.b", "a-b", "a b", "a\\b",
            "a\0b", "café",
        ] {
            assert!(
                validate_identifier("keyspace", bad).is_err(),
                "{bad:?} must be rejected"
            );
        }
    }

    #[test]
    fn snapshot_allows_hyphen_but_not_traversal() {
        for ok in ["cqlite-abc", "snap1", "2026-06-30", "a_b-c"] {
            assert!(validate_snapshot(ok).is_ok(), "{ok} should pass");
        }
        for bad in ["", "..", "../x", "a/b", "/abs", ".", "a.b", "a b", "a\0b"] {
            assert!(validate_snapshot(bad).is_err(), "{bad:?} must be rejected");
        }
    }

    #[test]
    fn within_allows_nonexistent_leaf_under_root() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path();
        // Non-existent table dir under a real root must be allowed (resolved
        // to its nearest existing ancestor = root itself).
        let target = root.join("realks").join("tbl");
        assert!(assert_within("keyspace", root, &target).is_ok());
    }

    #[test]
    #[cfg(unix)]
    fn within_rejects_symlink_escape() {
        use std::os::unix::fs::symlink;
        let root_tmp = tempfile::TempDir::new().unwrap();
        let outside_tmp = tempfile::TempDir::new().unwrap();
        let root = root_tmp.path();
        // root/evil -> <outside dir>
        symlink(outside_tmp.path(), root.join("evil")).unwrap();
        // A file under the outside dir, reachable via the symlink.
        std::fs::write(outside_tmp.path().join("secret"), b"x").unwrap();

        let escaping = root.join("evil").join("secret");
        let err = assert_within("keyspace", root, &escaping)
            .expect_err("symlink escape must be rejected");
        assert!(matches!(
            err,
            PathSafetyError::Escapes { field: "keyspace" }
        ));

        // A legitimate (non-existent) leaf under the real root still passes.
        let ok = root.join("realks").join("tbl");
        assert!(assert_within("keyspace", root, &ok).is_ok());
    }
}
