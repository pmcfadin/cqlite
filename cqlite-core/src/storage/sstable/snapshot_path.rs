//! Authoritative snapshot-aware SSTable path parsing (issue #2384).
//!
//! Cassandra lays SSTables out as
//! `.../{keyspace}/{table_name}-{table_id}/...-Data.db`. For a **snapshot** the
//! components live one level deeper, under
//! `.../{table_name}-{table_id}/snapshots/{snapshot_name}/...-Data.db`
//! (see `Directories.SNAPSHOT_SUBDIR` in Cassandra's `Directories.java`).
//!
//! Every in-crate parser that derives `{keyspace}.{table}` identity from a path —
//! header keyspace/table, schema-registry resolution, and `SSTableManager` keying —
//! MUST route through this module so a snapshot read resolves the REAL
//! `{keyspace}.{table}` rather than `snapshots`/`{snapshot_name}`.
//!
//! This is pure directory-segment STRUCTURE matching (a fixed Cassandra naming
//! shape), not a data/byte heuristic — it is compliant with the no-heuristics
//! mandate (issue #28).

use std::path::Path;

/// Return `true` when `name` matches Cassandra's `{table_name}-{table_id}` dir
/// shape: a non-empty table name, a `-`, then EXACTLY 32 lowercase hex chars.
///
/// Cassandra encodes the table UUID as 32 lowercase hex digits (no hyphens) in
/// the on-disk directory name (`TableMetadata`/`Directories.java`). Table names
/// may themselves contain hyphens, so only the final `-{32-hex}` suffix is the
/// id — we split on the LAST hyphen.
fn is_table_id_dir(name: &str) -> bool {
    match name.rsplit_once('-') {
        Some((table_name, id)) => {
            !table_name.is_empty()
                && id.len() == 32
                && id
                    .chars()
                    .all(|c| c.is_ascii_digit() || matches!(c, 'a'..='f'))
        }
        None => false,
    }
}

/// Resolve the real `{table_name}-{table_id}` directory for a Data.db `path`,
/// transparently walking up past a `snapshots/{snapshot_name}` layer.
///
/// For a normal SSTable the resolved directory is simply the Data.db file's
/// parent. For a snapshot (`.../{table}-{id}/snapshots/{tag}/...-Data.db`) it is
/// the grandparent-of-`snapshots` directory.
///
/// # Snapshot detection is STRUCTURAL and guarded
///
/// A path is only treated as a snapshot when BOTH hold:
/// 1. the grandparent directory is literally named `snapshots`, AND
/// 2. the directory ABOVE `snapshots` matches the `{table}-{32-hex}` shape
///    ([`is_table_id_dir`]).
///
/// Guard #2 prevents misresolving an ORDINARY table that happens to live in a
/// keyspace named `snapshots`
/// (`.../data/snapshots/{table}-{id}/...-Data.db`): there the dir above
/// `snapshots` is the keyspace root (e.g. `data`), which is NOT a table dir, so
/// the path is correctly treated as a normal (non-snapshot) layout.
///
/// # Known limitation — ID-less snapshot dirs (follow-up #2415)
///
/// The [`is_table_id_dir`] guard requires the `-{32-hex}` id suffix, so it only
/// fires for Cassandra-shaped ID-ful snapshots. CQLite's own write engine emits
/// ID-LESS table dirs `{ks}/{table}/` (no uuid suffix — `writer/mod.rs`); a
/// snapshot of one (`{ks}/{table}/snapshots/{tag}/...-Data.db`) does NOT match the
/// guard, so the walk-up is skipped and header identity misparses as
/// keyspace=`snapshots`, table=`{tag}`. This is inherently unresolvable from the
/// path alone — an ID-less snapshot and an ordinary table in a keyspace literally
/// named `snapshots` are structurally identical. The robust fix (threading
/// authoritative keyspace/table into the reader) is tracked as #2415. See the
/// `idless_snapshot_currently_unresolved_pending_followup` test below.
pub(crate) fn resolve_table_dir(path: &Path) -> Option<&Path> {
    // Parent of the Data.db file: `{table_name}-{table_id}` (normal) or
    // `{snapshot_name}` (snapshot layout).
    let parent = path.parent()?;

    if let Some(maybe_snapshots) = parent.parent() {
        let is_snapshots_dir =
            maybe_snapshots.file_name().and_then(|n| n.to_str()) == Some("snapshots");
        if is_snapshots_dir {
            // Only walk up past `snapshots` when the directory above it is a real
            // `{table}-{32-hex}` dir. Otherwise this is an ordinary table inside a
            // keyspace literally named `snapshots`.
            if let Some(real_table_dir) = maybe_snapshots.parent() {
                let looks_like_table = real_table_dir
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(is_table_id_dir)
                    .unwrap_or(false);
                if looks_like_table {
                    return Some(real_table_dir);
                }
            }
        }
    }

    Some(parent)
}

/// Extract the keyspace name for a Data.db `path` (snapshot-aware).
///
/// The keyspace is the parent of the resolved `{table_name}-{table_id}` dir.
/// Returns `None` when the path is too shallow to contain a keyspace directory.
pub(crate) fn extract_keyspace(path: &Path) -> Option<String> {
    resolve_table_dir(path)
        .and_then(|table_dir| table_dir.parent())
        .and_then(|keyspace_dir| keyspace_dir.file_name())
        .and_then(|n| n.to_str())
        .map(|s| s.to_string())
}

/// Extract the table name for a Data.db `path` (snapshot-aware), stripping the
/// trailing `-{table_id}` from the resolved directory when present.
///
/// When the resolved directory name has no `-{id}` suffix (unusual, e.g. a flat
/// test dir) the whole directory name is returned unchanged. Returns `None` when
/// the path has no directory component at all.
pub(crate) fn extract_table_name(path: &Path) -> Option<String> {
    let dir_name = resolve_table_dir(path)?.file_name()?.to_str()?;

    // Split on the LAST hyphen so table names containing hyphens survive; only a
    // trailing 32-hex id is stripped. Non-id suffixes are left intact.
    match dir_name.rsplit_once('-') {
        Some((table_name, id)) if is_table_id_suffix(id) => Some(table_name.to_string()),
        _ => Some(dir_name.to_string()),
    }
}

/// Return `true` when `id` is EXACTLY 32 lowercase hex chars (a Cassandra table
/// id suffix). Used to decide whether a trailing `-{id}` should be stripped.
fn is_table_id_suffix(id: &str) -> bool {
    id.len() == 32
        && id
            .chars()
            .all(|c| c.is_ascii_digit() || matches!(c, 'a'..='f'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normal_layout_resolves_keyspace_and_table() {
        let path = Path::new(
            "/var/lib/cassandra/data/myks/mytable-9f3a1c2d4e5f6071829304a5b6c7d8e9/nb-2-big-Data.db",
        );
        assert_eq!(extract_keyspace(path).as_deref(), Some("myks"));
        assert_eq!(extract_table_name(path).as_deref(), Some("mytable"));
    }

    #[test]
    fn snapshot_layout_resolves_real_keyspace_and_table() {
        let path = Path::new(
            "/var/lib/cassandra/data/myks/mytable-9f3a1c2d4e5f6071829304a5b6c7d8e9/\
             snapshots/cqlite-3b1e7f90/nb-2-big-Data.db",
        );
        assert_eq!(extract_keyspace(path).as_deref(), Some("myks"));
        assert_eq!(extract_table_name(path).as_deref(), Some("mytable"));
    }

    /// BLOCKER 1 regression (issue #2384): an ORDINARY table in a keyspace that is
    /// literally named `snapshots` must NOT be misparsed as a snapshot.
    #[test]
    fn ordinary_table_in_snapshots_named_keyspace_is_not_a_snapshot() {
        let path = Path::new(
            "/var/lib/cassandra/data/snapshots/mytable-9f3a1c2d4e5f6071829304a5b6c7d8e9/\
             nb-1-big-Data.db",
        );
        assert_eq!(
            extract_keyspace(path).as_deref(),
            Some("snapshots"),
            "keyspace literally named 'snapshots' must be preserved"
        );
        assert_eq!(
            extract_table_name(path).as_deref(),
            Some("mytable"),
            "table must resolve normally, not be misread as a snapshot tag"
        );
    }

    #[test]
    fn snapshot_tag_with_arbitrary_name_still_resolves() {
        let path = Path::new(
            "/data/analytics/events-00112233445566778899aabbccddeeff/\
             snapshots/pre-upgrade-2026/nb-5-big-Data.db",
        );
        assert_eq!(extract_keyspace(path).as_deref(), Some("analytics"));
        assert_eq!(extract_table_name(path).as_deref(), Some("events"));
    }

    #[test]
    fn table_name_with_internal_hyphen_survives() {
        let path = Path::new(
            "/mnt/disk3/cassandra/data/ks_ts/sensor-readings-abcdef0123456789abcdef0123456789/\
             da-11-bti-Data.db",
        );
        assert_eq!(extract_keyspace(path).as_deref(), Some("ks_ts"));
        assert_eq!(extract_table_name(path).as_deref(), Some("sensor-readings"));
    }

    #[test]
    fn is_table_id_dir_shape() {
        assert!(is_table_id_dir("t-9f3a1c2d4e5f6071829304a5b6c7d8e9"));
        assert!(is_table_id_dir("my-t-00112233445566778899aabbccddeeff"));
        // Wrong length.
        assert!(!is_table_id_dir("t-9f3a"));
        // Uppercase is not Cassandra's on-disk shape.
        assert!(!is_table_id_dir("t-9F3A1C2D4E5F6071829304A5B6C7D8E9"));
        // Non-hex.
        assert!(!is_table_id_dir("t-zzzz1c2d4e5f6071829304a5b6c7d8e9"));
        // No hyphen / empty table.
        assert!(!is_table_id_dir("snapshots"));
        assert!(!is_table_id_dir("-9f3a1c2d4e5f6071829304a5b6c7d8e9"));
    }

    /// PINS A KNOWN LIMITATION, NOT DESIRED BEHAVIOR (follow-up #2415).
    ///
    /// CQLite's own write engine emits ID-LESS table dirs `{ks}/{table}/` (no
    /// `-{uuid}` suffix — `writer/mod.rs`). A snapshot of such a table has the
    /// shape `{ks}/{table}/snapshots/{tag}/...-Data.db`. Because `is_table_id_dir`
    /// requires a `-{32-hex}` suffix, the walk-up past `snapshots` is NOT taken, so
    /// header identity currently resolves as keyspace=`snapshots`, table=`{tag}`.
    ///
    /// This is inherently unresolvable from the path alone (an ID-less snapshot and
    /// an ordinary table in a keyspace literally named `snapshots` are structurally
    /// identical); the robust fix threads authoritative keyspace/table into the
    /// reader and is tracked as #2415. When #2415 lands this assertion should FLIP
    /// (to keyspace=`myks`, table=`mytable`) — that visible break is intentional.
    #[test]
    fn idless_snapshot_currently_unresolved_pending_followup() {
        let path = Path::new("/data/myks/mytable/snapshots/cqlite-abc/nb-2-big-Data.db");
        // CURRENT (limited) behavior: the `snapshots` layer is not walked past
        // because `mytable` lacks a `-{32-hex}` id suffix.
        assert_eq!(
            extract_keyspace(path).as_deref(),
            Some("snapshots"),
            "known limitation (#2415): ID-less snapshot keyspace misparses as 'snapshots'"
        );
        assert_eq!(
            extract_table_name(path).as_deref(),
            Some("cqlite-abc"),
            "known limitation (#2415): ID-less snapshot table misparses as the snapshot tag"
        );
    }

    #[test]
    fn too_shallow_paths_return_none() {
        // No real parent dir: parent() is the empty path, which has no file_name.
        let path = Path::new("nb-1-big-Data.db");
        assert_eq!(extract_keyspace(path), None);
        assert_eq!(extract_table_name(path), None);
    }
}
