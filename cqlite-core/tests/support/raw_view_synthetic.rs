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

use cqlite_core::query::result::ColumnInfo;
use raw_view_parity::golden::{
    build_expectations, classify_columns, ColumnRoles, GoldenSstable, DECLARED_GAP_COLUMNS,
    PARTITION_METADATA, RANGE_METADATA, ROW_LEVEL_METADATA,
};
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

/// Build the raw view's contract column set for a table with the given
/// keys, simple base columns and collection columns.
///
/// MODELS ONE ORDERING PROPERTY, not the whole contract order (roborev job
/// 64). What `classify_columns` depends on is reproduced exactly: KEYS
/// FIRST, then each base column immediately followed by its synthesized
/// metadata siblings — that is what its `take_while` key-boundary scan
/// reads. The trailing always-applicable block's INTERNAL order is NOT
/// modelled: production emits `row_kind` BETWEEN the partition-deletion
/// pair and `bound_inclusive` (`raw_view/columns.rs:347`), whereas this
/// helper appends it with the other `DECLARED_GAP_COLUMNS`. Nothing in
/// `classify_columns` reads that order today, so the difference is inert —
/// but an order-sensitive check added there later would be validated
/// against a shape production never emits, so fix this helper before
/// adding one.
pub fn contract_columns(keys: &[&str], simple: &[&str], complex: &[&str]) -> Vec<ColumnInfo> {
    let mut names: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
    for c in simple {
        names.push((*c).to_string());
        for suffix in ["_timestamp", "_ttl", "_local_deletion_time", "_tombstone"] {
            names.push(format!("{c}{suffix}"));
        }
    }
    for c in complex {
        names.push((*c).to_string());
        for suffix in [
            "_complex_deletion",
            "_complex_deletion_time",
            "_complex_deletion_timestamp",
        ] {
            names.push(format!("{c}{suffix}"));
        }
    }
    for n in ROW_LEVEL_METADATA
        .iter()
        .chain(PARTITION_METADATA)
        .chain(RANGE_METADATA)
        .chain(DECLARED_GAP_COLUMNS)
    {
        names.push((*n).to_string());
    }
    names
        .into_iter()
        .enumerate()
        .map(|(position, name)| ColumnInfo {
            name,
            data_type: cqlite_core::types::DataType::Text,
            nullable: true,
            position,
            table_name: None,
            cql_type: None,
        })
        .collect()
}

/// ONE derivation of the roles, not two (roborev job 75).
///
/// This used to re-implement `classify_columns`' `compared_columns`
/// assembly — the four simple suffixes, the three complex suffixes, then
/// the row/partition/range metadata — and claimed to do it "exactly as
/// `classify_columns` derives it". That was a claim about the code with
/// nothing pinning it, and `raw_view_parity.rs`'s module doc names a new
/// metadata column in `raw_view_columns` as the expected future change. So
/// the synthetic roles are now produced BY `classify_columns` itself, over
/// the synthetic contract above: there is one derivation, and it cannot
/// drift from the one the sweep uses.
pub fn roles_full(clustering: &[&str], simple: &[&str], complex: &[&str]) -> ColumnRoles {
    let mut keys: Vec<&str> = vec!["pk"];
    keys.extend_from_slice(clustering);
    classify_columns(&contract_columns(&keys, simple, complex), &SPEC)
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
