//! Issue #4581 — public-surface READ guard for the primary-key liveness marker.
//!
//! The compaction writer now decides the marker explicitly (`MarkerMode`), and the read-time
//! "is this marker-only row live" rule no longer needs a `MergePurpose` switch. This pins that rule
//! through `Database::execute` on a clustering-less table of CASSANDRA-WRITTEN SSTables
//! (`test_markerrows.purged_marker`, the generator's INPUTS; see
//! `generate-issue-4581-compaction-marker-rows.sh`):
//!
//! | id | content | at a `now` past the TTL |
//! |---|---|---|
//! | 1 | `INSERT (1,'m') TTL 3`@1000 then `UPDATE v='n'`@2000: EXPIRED marker + live data | visible, v='n' |
//! | 2 | `INSERT (2) TTL 3`@1000 then `UPDATE v='n'`@2000: EXPIRED marker + live data | visible, v='n' |
//! | 3 | `INSERT (3) TTL 3`@1000: EXPIRED, marker-only | HIDDEN |
//! | 4 | `INSERT (4)`@1000: live marker-only | visible, no `v` |
//!
//! `SELECT *` runs through the full-scan path, the partition-targeted POINT path (forced, so a
//! silent full scan cannot satisfy it) and the classifier's `auto` seek path, at a PINNED `now`
//! (`CQLITE_TTL_NOW_OVERRIDE_SECS`, never wall-clock). Each row's column set is asserted BOTH ways
//! (#3890): nothing outside `{id, v}`, `id` always, `v` exactly when the row has a value.

#![cfg(all(feature = "state_machine", feature = "cli-helpers", feature = "lz4"))]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serial_test::serial;

use cqlite_core::config::ReadPathMode;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::types::Value;
use cqlite_core::{Config, Database};

#[path = "support/datasets_root.rs"]
mod datasets_root;

const TTL_NOW_OVERRIDE_ENV: &str = "CQLITE_TTL_NOW_OVERRIDE_SECS";
/// Cassandra's compaction instant for this table (`compaction-instant.json`): every TTL (3 s,
/// expiring 1794876878-9) has expired.
const NOW_PAST_TTL: i64 = 1_794_877_003;
/// One minute BEFORE the first marker expires: id 3 is a live marker-only row then.
const NOW_BEFORE_TTL: i64 = 1_794_876_818;

struct PinnedClock(Option<std::ffi::OsString>);

impl PinnedClock {
    fn at(secs: i64) -> Self {
        let previous = std::env::var_os(TTL_NOW_OVERRIDE_ENV);
        std::env::set_var(TTL_NOW_OVERRIDE_ENV, secs.to_string());
        Self(previous)
    }
}

impl Drop for PinnedClock {
    fn drop(&mut self) {
        match self.0.take() {
            Some(v) => std::env::set_var(TTL_NOW_OVERRIDE_ENV, v),
            None => std::env::remove_var(TTL_NOW_OVERRIDE_ENV),
        }
    }
}

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace parent")
        .join("test-data/fixtures/issue_4581")
}

async fn open_db(mode: ReadPathMode) -> Database {
    let schema = datasets_root::schema_path("issue-4581-compaction-marker-rows.cql")
        .expect("committed CQL schema");
    let mut core_config = Config::default();
    core_config.query.forced_read_path = Some(mode);
    ingest(IngestionConfig {
        schema_paths: vec![schema],
        data_dir: fixture_root(),
        version_hint: None,
        core_config,
        table_directory_filter: Some("/test_markerrows/".to_string()),
    })
    .await
    .expect("ingest the Cassandra-written fixtures")
    .database
}

/// `(id, v)` per row, id-sorted. The column set is checked BOTH ways: no column outside `{id, v}`
/// (superset), `id` always present, and `v` present exactly when the row has a value for it (a
/// truncated row would drop a non-null `v`; the engine models a NULL cell as an absent key, as
/// `issue_4524_null_only_row` does, so the marker-only row has `v` absent).
fn rows_of(rows: &[cqlite_core::query::result::QueryRow]) -> Vec<(i32, Option<String>)> {
    let mut out: Vec<(i32, Option<String>)> = rows
        .iter()
        .map(|r| {
            let got: BTreeSet<&str> = r.values.keys().map(|k| k.as_ref()).collect();
            assert!(
                got.is_subset(&["id", "v"].into_iter().collect()),
                "extra column: {r:?}"
            );
            let id = match r.values.get("id") {
                Some(Value::Integer(i)) => *i,
                other => panic!("id decoded as {other:?} in {r:?}"),
            };
            let v = match r.values.get("v") {
                None | Some(Value::Null) => None,
                Some(Value::Text(s)) => Some(String::from_utf8_lossy(s).into_owned()),
                other => panic!("v decoded as {other:?}"),
            };
            assert_eq!(
                got.contains("v"),
                v.is_some(),
                "v present iff it has a value: {r:?}"
            );
            (id, v)
        })
        .collect();
    out.sort();
    out
}

async fn full_scan(mode: ReadPathMode) -> Vec<(i32, Option<String>)> {
    let db = open_db(mode).await;
    let res = db
        .execute("SELECT * FROM test_markerrows.purged_marker")
        .await
        .expect("SELECT *");
    rows_of(&res.rows)
}

async fn per_partition(mode: ReadPathMode) -> Vec<(i32, Option<String>)> {
    let db = open_db(mode).await;
    let mut all = Vec::new();
    for id in 1..=4 {
        let res = db
            .execute(&format!(
                "SELECT * FROM test_markerrows.purged_marker WHERE id = {id}"
            ))
            .await
            .unwrap_or_else(|e| panic!("point read of id {id} ({mode:?}): {e}"));
        all.extend(rows_of(&res.rows));
    }
    all.sort();
    all
}

fn expected_past_ttl() -> Vec<(i32, Option<String>)> {
    vec![
        (1, Some("n".into())),
        (2, Some("n".into())),
        (4, None), // live marker-only row: no value for `v`
    ]
}

#[tokio::test]
#[serial]
async fn expired_marker_only_row_is_hidden_and_data_rows_are_shown() {
    let _clock = PinnedClock::at(NOW_PAST_TTL);
    assert_eq!(
        full_scan(ReadPathMode::Full).await,
        expected_past_ttl(),
        "full scan"
    );
    assert_eq!(
        per_partition(ReadPathMode::Point).await,
        expected_past_ttl(),
        "forced point path"
    );
    assert_eq!(
        per_partition(ReadPathMode::Auto).await,
        expected_past_ttl(),
        "auto (classifier seek) path"
    );
}

/// The pin is what hides id 3: before its marker expires the same bytes read it as a live row.
#[tokio::test]
#[serial]
async fn the_same_marker_only_row_is_live_before_its_ttl() {
    let _clock = PinnedClock::at(NOW_BEFORE_TTL);
    let mut want = expected_past_ttl();
    want.push((3, None));
    want.sort();
    assert_eq!(full_scan(ReadPathMode::Full).await, want, "full scan");
    assert_eq!(per_partition(ReadPathMode::Auto).await, want, "auto path");
}
