//! Synthetic sstabledump goldens for the issue #4309 self-test lanes.
//!
//! The parity sweep's 27 fixture cases test the raw SSTable VIEW. Two
//! sibling lanes test the HARNESS itself, by feeding its oracle goldens
//! whose shape is known BY CONSTRUCTION:
//!
//!   * `issue_4309_raw_view_census_selftest.rs` — the coverage CENSUS: its
//!     shape tokens, its vocabulary, and `require_observed`'s guards.
//!   * `issue_4309_raw_view_oracle_selftest.rs` — the GOLDEN MODEL: complex
//!     columns, TTL, `classify_columns`, generation selection, and the
//!     fail-closed refusals.
//!
//! They are two files rather than one for the campsite rule (CLAUDE.md,
//! epic #1135): combined they exceeded the gate's 1500-line test-file
//! threshold and FAILed `file-size` (roborev job 65). The split follows the
//! same census/oracle seam `raw_view_parity.rs` and `raw_view_golden.rs`
//! already use, and this module holds what both lanes need so neither owns
//! a copy.
//!
//! Nothing here opens a database or reads the corpus, so every case in both
//! lanes is `must_run` on EVERY gate — which is the point: the derivations
//! these control are reachable only through `Discipline::FetchOnly`
//! fixtures that SKIP under the gate's corpus-less `core-tests`.

#![allow(dead_code)]

#[path = "raw_view_parity.rs"]
pub mod raw_view_parity;

use raw_view_parity::golden::{build_expectations, ColumnRoles, GoldenSstable};
use raw_view_parity::{Discipline, FixtureSpec};
use serde_json::json;
use std::collections::BTreeMap;
use std::path::PathBuf;

pub fn roles() -> ColumnRoles {
    roles_with(&["ck"], &[])
}

pub const SPEC: FixtureSpec = FixtureSpec {
    keyspace: "selftest",
    table: "census",
    schema_file: "unused-no-database-is-opened.cql",
    partition_key_columns: &["pk"],
    discipline: Discipline::GitCommitted,
};

/// One generation holding exactly the named partition keys, each with a
/// single live `row` entry — the minimum shape `build_expectations` models.
pub fn generation(data_db: &str, keys: &[&str]) -> GoldenSstable {
    GoldenSstable {
        data_db: data_db.to_string(),
        source_dir: PathBuf::from("/nonexistent/selftest"),
        partitions: keys
            .iter()
            .map(|key| {
                json!({
                    "partition": { "key": [key] },
                    "rows": [{
                        "type": "row",
                        "clustering": ["10"],
                        "liveness_info": { "tstamp": "2021-01-01T00:00:00Z" },
                        "cells": []
                    }]
                })
            })
            .collect(),
    }
}

/// `roles()` widened for a case that needs more clustering columns or a
/// real base column. The census tokens do not depend on these, but the
/// harness's own fail-closed guards do: `render_golden_clustering` asserts
/// one golden component per clustering column, and `row_entry_facts`
/// refuses a cell naming a column outside the contract.
pub fn roles_with(clustering: &[&str], simple: &[&str]) -> ColumnRoles {
    roles_full(clustering, simple, &[])
}

pub fn roles_full(clustering: &[&str], simple: &[&str], complex: &[&str]) -> ColumnRoles {
    ColumnRoles {
        clustering_columns: clustering.iter().map(|c| c.to_string()).collect(),
        simple_columns: simple.iter().map(|c| c.to_string()).collect(),
        complex_columns: complex.iter().map(|c| c.to_string()).collect(),
        compared_columns: Vec::new(),
    }
}

pub fn census(goldens: &[GoldenSstable]) -> BTreeMap<&'static str, usize> {
    census_with(goldens, &roles())
}

pub fn census_with(
    goldens: &[GoldenSstable],
    roles: &ColumnRoles,
) -> BTreeMap<&'static str, usize> {
    build_expectations(goldens, roles, &SPEC).1
}

/// A generation built from explicit partition objects, for the shapes
/// `generation()`'s one-live-row-per-key form cannot express.
pub fn generation_of(data_db: &str, partitions: Vec<serde_json::Value>) -> GoldenSstable {
    GoldenSstable {
        data_db: data_db.to_string(),
        source_dir: PathBuf::from("/nonexistent/selftest"),
        partitions,
    }
}

/// One range-tombstone bound side, as `JsonTransformer` renders it.
///
/// `inclusivity` is the BOUND's own `"type"` field — `"inclusive"` or
/// `"exclusive"` — which is NOT the same `"type"` as the enclosing ENTRY's
/// (`"range_tombstone_bound"` / `"range_tombstone_boundary"`). The two are
/// nested and both spelled `"type"` in sstabledump's JSON, so the parameter
/// is named for the one it sets (roborev job 70): passing an entry kind
/// here yields the unrelated "unexpected sstabledump bound type" panic.
///
/// `clustering` is passed through verbatim so a caller can write the
/// literal `"*"` sstabledump emits for an unspecified trailing component.
pub fn bound(inclusivity: &str, clustering: serde_json::Value) -> serde_json::Value {
    json!({
        "type": inclusivity,
        "clustering": clustering,
        "deletion_info": {
            "marked_deleted": "2021-01-01T00:00:00Z",
            "local_delete_time": "2021-01-01T00:00:00Z"
        }
    })
}
