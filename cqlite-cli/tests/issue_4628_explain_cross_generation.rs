//! Issue #4628 (field round F8): `cqlite explain` must apply a tombstone that lives in a
//! different generation than the data it deletes.
//!
//! Oracle: the live Cassandra 5.0.8 read at CONSISTENCY ALL over the very SSTables committed
//! in `test-data/fixtures/issue_4628/` (`live-select-consistency-all.txt`), i.e. read
//! semantics, not a physical dump. Layout: `da-1` holds the data, `da-2` holds an unrelated
//! partition (so it is ABSENT for every probed key and contributes no merge run), `da-3`
//! holds a partition, a range, a row and a collection-overwrite deletion. Before the fix the
//! absent middle generation made the run index (candidate position) disagree with the run's
//! position inside the merger, so the merge stopped refilling the data generation after its
//! first row and every other covered cell was reported as a winner.

use assert_cmd::Command;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;

const NOW: &str = "1795680000";

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../test-data/fixtures/issue_4628")
}

/// `(key, c1, c2) -> (writetime(v), live list length)` for every row the live SELECT returned.
fn live_rows() -> BTreeMap<(i64, i64, i64), (i64, usize)> {
    let text = std::fs::read_to_string(fixture().join("live-select-consistency-all.txt"))
        .expect("committed live oracle");
    let mut rows = BTreeMap::new();
    for line in text.lines() {
        let cols: Vec<&str> = line.split('|').map(str::trim).collect();
        if cols.len() < 9 {
            continue;
        }
        let Ok(id) = cols[0].parse::<i64>() else {
            continue; // header / separator / footer
        };
        let list = cols[4].trim_matches(|c| c == '[' || c == ']');
        let list_len = if list.is_empty() || list == "null" {
            0
        } else {
            list.split(',').count()
        };
        rows.insert(
            (
                id,
                cols[1].parse().expect("c1"),
                cols[2].parse().expect("c2"),
            ),
            (cols[6].parse().expect("writetime(v)"), list_len),
        );
    }
    assert_eq!(rows.len(), 22, "live oracle must carry all 22 rows");
    rows
}

fn explain(key: i64) -> Value {
    let output = Command::cargo_bin("cqlite")
        .expect("cqlite binary is built")
        .args(["--schema"])
        .arg(fixture().join("schema.cql"))
        .args(["--data-dir"])
        .arg(fixture().join("sstables"))
        .args(["explain", "fix4628.t", &key.to_string(), "--now", NOW])
        .args(["--out", "json"])
        .output()
        .expect("explain starts");
    assert!(
        output.status.success(),
        "explain {key} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("UTF-8");
    let json = stdout
        .lines()
        .find(|l| l.starts_with('{'))
        .expect("JSON report line");
    serde_json::from_str(json).expect("valid JSON")
}

/// Per live-visible clustering: the winner `v` writetime and the winner `l` cell count.
fn winners(report: &Value, key: i64) -> BTreeMap<(i64, i64, i64), (i64, usize)> {
    let mut out: BTreeMap<(i64, i64, i64), (i64, usize)> = BTreeMap::new();
    for cell in report["cells"].as_array().expect("cells") {
        if cell["verdict"] != "winner" {
            continue;
        }
        let comps = cell["clustering"]["columns"]
            .as_array()
            .expect("clustering");
        let c = |i: usize| comps[i][1]["Integer"].as_i64().expect("int clustering");
        let entry = out.entry((key, c(0), c(1))).or_insert((0, 0));
        match cell["column"].as_str().expect("column") {
            "v" => entry.0 = cell["writetime"].as_i64().expect("writetime"),
            "l" => entry.1 += 1,
            _ => {}
        }
    }
    out
}

#[test]
fn explain_matches_live_cl_all_for_cross_generation_tombstones() {
    let live = live_rows();
    // 1 partition delete, 3 range delete, 4 row delete, 5 collection overwrite, 6 filler, 9 control.
    for key in [1_i64, 3, 4, 5, 6, 9] {
        let report = explain(key);
        let generations = report["generations"].as_array().expect("generations");
        assert_eq!(generations.len(), 3, "key {key}: three generations");
        if key != 6 {
            assert_eq!(
                generations[1]["probe"], "absent",
                "key {key}: the middle generation must be absent (the regression shape)"
            );
        }
        let got = winners(&report, key);
        let want: BTreeMap<_, _> = live
            .iter()
            .filter(|((id, _, _), _)| *id == key)
            .map(|(k, v)| (*k, *v))
            .collect();
        assert_eq!(
            got, want,
            "key {key}: explain winners differ from the live CL=ALL read"
        );
    }
}

#[test]
fn explain_partition_delete_leaves_no_winner_and_shadows_every_cell() {
    let report = explain(1);
    let cells = report["cells"].as_array().expect("cells");
    assert!(!cells.is_empty(), "key 1 has data in da-1");
    for cell in cells {
        assert_eq!(
            cell["verdict"], "shadowed-by-tombstone:partition",
            "every covered cell is shadowed by the partition tombstone: {cell}"
        );
    }
}
