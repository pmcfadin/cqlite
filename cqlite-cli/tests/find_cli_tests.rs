//! Issue #4205 — `cqlite find` end-to-end through the REAL compiled binary
//! (`CARGO_BIN_EXE_cqlite`): text + JSON rendering, exit codes, both SSTable formats, the
//! CommitLog half, and the fail-closed cases. Oracles:
//!
//!   * SSTable offsets / lengths: the committed Cassandra-written `*-Data.db.jsonl`
//!     (`partition.position` minus the partition header: `2 + key_len + 12` for `nb`, `+ 1`
//!     for the `da` live-deletion flag — see `cqlite-core/tests/support/partition_start.rs`);
//!   * CommitLog position / writetime / flushed: `commitlog-find-ground-truth.json`, whose
//!     positions come from an independent Python frame walker and whose timestamps are the
//!     explicit `USING TIMESTAMP` values inserted into real Cassandra 5.0.8.
//!
//! Every fixture used here is git-COMMITTED, so absence fails closed unconditionally (#3220).

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;
use tempfile::TempDir;

fn td() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../test-data")
}

fn only_dir(parent: &Path, prefix: &str) -> PathBuf {
    let mut v: Vec<PathBuf> = std::fs::read_dir(parent)
        .unwrap_or_else(|e| panic!("committed fixture dir {parent:?} missing: {e}"))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with(prefix))
        })
        .collect();
    v.sort();
    assert_eq!(v.len(), 1, "expected exactly one {prefix}* in {parent:?}");
    v.remove(0)
}

fn find(schema: &str, dir: &Path, key: &str, extra: &[&str]) -> (i32, String, String) {
    let out: Output = Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .arg("--schema")
        .arg(td().join("schemas").join(schema))
        .arg("find")
        .arg(dir)
        .arg(key)
        .args(extra)
        .output()
        .expect("spawn cqlite");
    (
        out.status.code().expect("exit code"),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn json_of(stdout: &str) -> Value {
    let line = stdout
        .lines()
        .find(|l| l.starts_with('{'))
        .unwrap_or_else(|| panic!("no JSON in stdout: {stdout}"));
    serde_json::from_str(line).expect("valid JSON")
}

/// Parse a committed `*-Data.db.jsonl`: genuine JSONL (one object per line) OR sstabledump's
/// raw pretty-printed JSON array — whichever the file actually is. Never empty.
fn golden_objects(jsonl: &Path) -> Vec<Value> {
    let text = std::fs::read_to_string(jsonl).unwrap_or_else(|e| panic!("{jsonl:?}: {e}"));
    let objs: Vec<Value> = if text.trim_start().starts_with('[') {
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("{jsonl:?} as a JSON array: {e}"))
    } else {
        text.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("{jsonl:?} line: {e}")))
            .collect()
    };
    assert!(
        !objs.is_empty(),
        "{jsonl:?} parsed to zero partitions: the oracle would be vacuous"
    );
    objs
}

fn golden(jsonl: &Path) -> Vec<(String, u64)> {
    golden_objects(jsonl)
        .iter()
        .map(|v| {
            (
                v["partition"]["key"][0].as_str().expect("key").to_string(),
                v["partition"]["position"].as_u64().expect("position"),
            )
        })
        .collect()
}

/// BIG (`nb`, LZ4-compressed): the only partition is a hit at offset 0 running ToEof (F2.2);
/// an absent key is an index-layer negative with exit 0 (F5.1).
#[test]
fn big_hit_offset_and_to_eof_match_the_golden_and_absent_key_exits_zero() {
    let dir = only_dir(&td().join("datasets/sstables/test_comp"), "lz4_table-");
    let g = golden(&dir.join("nb-1-big-Data.db.jsonl"));
    assert_eq!(g.len(), 1, "fixture shape: one partition");
    let (key, position) = &g[0];
    let want_offset = position - (2 + 4 + 12); // int key
    let (code, out, err) = find("compression-parity.cql", &dir, key, &["--out", "json"]);
    assert_eq!(code, 0, "stderr: {err}");
    let v = json_of(&out);
    let gen = &v["generations"][0];
    assert_eq!(gen["probe"], "hit");
    assert_eq!(gen["data_offset"], want_offset);
    assert_eq!(gen["byte_length"], "to-eof");
    assert_eq!(gen["max_writetime"], 1_609_459_200_000_000_i64); // 2021-01-01T00:00:00Z
    assert_eq!(gen["partition_deletion"], false);

    let (code, out, err) = find("compression-parity.cql", &dir, "424242", &[]);
    assert_eq!(code, 0, "an absent key still exits 0; stderr: {err}");
    assert!(
        out.contains("bloom-negative") || out.contains("index-miss"),
        "absent key must name the layer that said no: {out}"
    );
    assert!(!out.contains("hit"), "{out}");
}

/// BTI (`da`): both partitions are hits with offsets/lengths from the golden.
#[test]
fn bti_hits_match_the_golden_offsets_and_lengths() {
    let dir = only_dir(&td().join("datasets/sstables/test_da"), "simple_table-");
    let g = golden(&dir.join("da-2-bti-Data.db.jsonl"));
    assert!(g.len() >= 2, "fixture shape");
    let starts: Vec<(String, u64)> = g
        .iter()
        .map(|(k, p)| (k.clone(), p - (2 + 16 + 1)))
        .collect();
    for (i, (key, start)) in starts.iter().enumerate() {
        let (code, out, err) = find("da-test.cql", &dir, key, &["--out", "json"]);
        assert_eq!(code, 0, "stderr: {err}");
        let gen = &json_of(&out)["generations"][0];
        assert_eq!(gen["probe"], "hit", "key {key}: {out}");
        assert_eq!(gen["data_offset"], *start);
        match starts.iter().map(|s| s.1).filter(|s| s > start).min() {
            Some(next) => assert_eq!(gen["byte_length"], next - start, "key {i}"),
            None => assert_eq!(gen["byte_length"], "to-eof"),
        }
    }
    let (code, out, _) = find(
        "da-test.cql",
        &dir,
        "00000000-0000-0000-0000-00000000dead",
        &[],
    );
    assert_eq!(code, 0);
    assert!(
        out.contains("index-miss"),
        "BTI absence is the trie's answer: {out}"
    );
}

struct Truth {
    gt: Value,
    dir: PathBuf,
    commitlog: PathBuf,
}

fn truth() -> Truth {
    let root = td().join("datasets/commitlog_find");
    let gt: Value = serde_json::from_str(
        &std::fs::read_to_string(root.join("commitlog-find-ground-truth.json"))
            .expect("committed ground truth"),
    )
    .expect("ground truth json");
    let dir = root
        .join("sstables/commitlog_test")
        .join(gt["table_dir"].as_str().expect("table_dir"));
    Truth {
        gt,
        dir,
        commitlog: root.join("commitlog"),
    }
}

/// F3.1 / F4.1: per ground-truth mutation of one key, the reported (segment, position,
/// writetime, unflushed) equals the inserted-and-recorded values.
#[test]
fn commitlog_mutations_report_ground_truth_position_writetime_and_unflushed() {
    let t = truth();
    let inserts = t.gt["inserts"].as_array().expect("inserts");
    let seg = t.gt["segment"]["file"].as_str().expect("segment file");
    let mut checked = 0;
    for key in 1..=5 {
        let mine: Vec<&Value> = inserts.iter().filter(|i| i["id"] == key).collect();
        let flushed_max = mine
            .iter()
            .filter(|i| i["flushed"] == true)
            .map(|i| i["timestamp"].as_i64().expect("ts"))
            .max();
        let (code, out, err) = find(
            "commitlog-find.cql",
            &t.dir,
            &key.to_string(),
            &[
                "--commitlog",
                t.commitlog.to_str().expect("utf8"),
                "--out",
                "json",
            ],
        );
        assert_eq!(code, 0, "key {key}: {err}");
        let v = json_of(&out);
        let got = v["commitlog"].as_array().expect("commitlog array");
        assert_eq!(got.len(), mine.len(), "key {key}: one entry per mutation");
        for (g, want) in got.iter().zip(&mine) {
            let ts = want["timestamp"].as_i64().expect("ts");
            assert_eq!(g["segment"], seg);
            assert_eq!(g["position"], want["position"], "key {key} position");
            assert_eq!(g["writetime"], ts, "key {key} writetime");
            let expect_unflushed = if flushed_max.is_some_and(|m| ts <= m) {
                "no"
            } else {
                "yes"
            };
            assert_eq!(g["unflushed"], expect_unflushed, "key {key} @ {ts}");
            checked += 1;
        }
    }
    assert_eq!(
        checked,
        inserts.len(),
        "every ground-truth mutation was checked"
    );
}

/// F4.2: a key present in the CommitLog and in NO generation is unconditionally unflushed.
#[test]
fn key_absent_from_every_generation_is_unflushed_yes() {
    let t = truth();
    let (code, out, _) = find(
        "commitlog-find.cql",
        &t.dir,
        "4",
        &["--commitlog", t.commitlog.to_str().expect("utf8")],
    );
    assert_eq!(code, 0);
    assert!(
        out.contains("nb-1-big-Data.db: bloom-negative")
            || out.contains("nb-1-big-Data.db: index-miss"),
        "{out}"
    );
    assert!(out.contains("unflushed=yes"), "{out}");
    assert!(!out.contains("unflushed=no"), "{out}");
}

/// F3.2 / F4.3: the same segment decoded for a CLUSTERED declaration of the table reports
/// `unmeasured(clustered table not decoded)` for writetime AND unflushed — never a guess. The
/// table directory has no generation at all (a never-flushed table), which must still work.
#[test]
fn clustered_table_mutation_is_unmeasured_never_guessed() {
    let tmp = TempDir::new().expect("tempdir");
    let table_dir = tmp.path().join("users-dc2d1cf0dcc811f1be1b6d4cbb83f4d8");
    std::fs::create_dir(&table_dir).expect("mkdir");
    let cl = tmp.path().join("cl");
    std::fs::create_dir(&cl).expect("mkdir");
    let src = td().join("datasets/commitlog/clean-CommitLog-7-1794289002839.log");
    std::fs::copy(&src, cl.join("CommitLog-7-1794289002839.log")).expect("copy segment");
    let schema = tmp.path().join("clustered.cql");
    std::fs::write(
        &schema,
        "CREATE TABLE commitlog_test.users (id int, seq int, name text, age int, PRIMARY KEY (id, seq));\n",
    )
    .expect("schema");
    let out = Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .arg("--schema")
        .arg(&schema)
        .args(["find"])
        .arg(&table_dir)
        .args(["1", "--commitlog"])
        .arg(&cl)
        .output()
        .expect("spawn");
    let text = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        text.contains("writetime=unmeasured(clustered table not decoded)")
            && text.contains("unflushed=unmeasured(clustered table not decoded)"),
        "{text}"
    );
    assert!(text.contains("(no SSTable generation"), "{text}");
}

/// F3.3 + F5: a healthy and a corrupt segment in one directory — the healthy one's matches are
/// reported AND the corrupt one is its own named line, with exit 2.
#[test]
fn corrupt_segment_is_its_own_named_line_and_exits_two() {
    let t = truth();
    let tmp = TempDir::new().expect("tempdir");
    let seg = t.gt["segment"]["file"].as_str().expect("segment file");
    std::fs::copy(t.commitlog.join(seg), tmp.path().join(seg)).expect("copy healthy");
    std::fs::copy(
        td().join("datasets/commitlog/corrupt-crc-CommitLog-7-1794289002839.log"),
        tmp.path().join("corrupt-CommitLog-7-1794289002839.log"),
    )
    .expect("copy corrupt");
    let (code, out, _) = find(
        "commitlog-find.cql",
        &t.dir,
        "2",
        &["--commitlog", tmp.path().to_str().expect("utf8")],
    );
    assert_eq!(code, 2, "{out}");
    assert!(
        out.contains(&format!("{seg}@62415")),
        "healthy matches still reported: {out}"
    );
    assert!(
        out.contains("corrupt-CommitLog-7-1794289002839.log: UNREADABLE"),
        "the corrupt segment must be named: {out}"
    );
}

/// F5.2: a generation whose `Data.db` cannot be opened is a named exit-2 row, never omitted;
/// the readable sibling generation is still reported.
#[test]
fn unopenable_generation_is_named_and_exits_two() {
    let t = truth();
    let tmp = TempDir::new().expect("tempdir");
    let dir = tmp
        .path()
        .join(t.gt["table_dir"].as_str().expect("table_dir"));
    std::fs::create_dir(&dir).expect("mkdir");
    for e in std::fs::read_dir(&t.dir).expect("read fixture") {
        let p = e.expect("entry").path();
        std::fs::copy(&p, dir.join(p.file_name().expect("name"))).expect("copy");
    }
    std::fs::write(dir.join("nb-2-big-Data.db"), b"").expect("empty second generation");
    let (code, out, _) = find("commitlog-find.cql", &dir, "1", &[]);
    assert_eq!(code, 2, "{out}");
    assert!(out.contains("nb-2-big-Data.db: UNREADABLE"), "{out}");
    assert!(
        out.contains("nb-1-big-Data.db: hit"),
        "readable generation still reported: {out}"
    );
}

/// F5 usage errors: missing `--schema`, an unparsable key, a wrong-arity key all exit 1.
#[test]
fn usage_errors_exit_one() {
    let t = truth();
    let no_schema = Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .arg("find")
        .arg(&t.dir)
        .arg("1")
        .output()
        .expect("spawn");
    assert_eq!(no_schema.status.code(), Some(1));
    let (code, _, err) = find("commitlog-find.cql", &t.dir, "not a literal ((", &[]);
    assert_eq!(code, 1, "{err}");
    let (code, _, err) = find("commitlog-find.cql", &t.dir, "1, 2", &[]);
    assert_eq!(code, 1, "{err}");
    assert!(err.contains("partition key has 1 column"), "{err}");
}

/// The committed `commitlog_find` SSTable's own sstabledump golden is the oracle for what the
/// flushed generation holds: `find` hits exactly the golden's keys at the golden's offsets.
#[test]
fn commitlog_find_generation_hits_match_its_own_golden() {
    let t = truth();
    let golden_file = std::fs::read_dir(&t.dir)
        .expect("fixture dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| p.to_string_lossy().ends_with("-Data.db.jsonl"))
        .expect("committed golden");
    let g = golden(&golden_file);
    let flushed: Vec<String> = t.gt["inserts"]
        .as_array()
        .expect("inserts")
        .iter()
        .filter(|i| i["flushed"] == true)
        .map(|i| i["id"].to_string())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut golden_keys: Vec<String> = g.iter().map(|(k, _)| k.clone()).collect();
    golden_keys.sort();
    assert_eq!(
        golden_keys, flushed,
        "golden holds exactly the flushed keys"
    );
    for (key, position) in &g {
        let (code, out, err) = find("commitlog-find.cql", &t.dir, key, &["--out", "json"]);
        assert_eq!(code, 0, "{err}");
        let gen = &json_of(&out)["generations"][0];
        assert_eq!(gen["probe"], "hit", "key {key}");
        assert_eq!(gen["data_offset"], position - (2 + 4 + 12), "key {key}");
    }
    eprintln!("commitlog_find golden: {} partitions checked", g.len());
}

/// A generation whose `Data.db` is gone but whose other components remain is NOT silently
/// omitted: it is an UNREADABLE row (exit 2), and the generation that is complete is still probed.
#[test]
fn generation_missing_its_data_db_is_reported_unreadable() {
    let t = truth();
    let tmp = TempDir::new().expect("tempdir");
    let dir = tmp
        .path()
        .join(t.gt["table_dir"].as_str().expect("table_dir"));
    std::fs::create_dir(&dir).expect("mkdir");
    for e in std::fs::read_dir(&t.dir).expect("read fixture") {
        let p = e.expect("entry").path();
        let name = p.file_name().expect("name").to_string_lossy().into_owned();
        // Generation 1 complete; generation 2 = the same Cassandra-written components with
        // the Data.db withheld.
        std::fs::copy(&p, dir.join(&name)).expect("copy");
        if name.starts_with("nb-1-big-") && !name.ends_with(".jsonl") {
            std::fs::copy(&p, dir.join(name.replace("nb-1-big-", "nb-2-big-")))
                .expect("copy gen 2");
        }
    }
    std::fs::remove_file(dir.join("nb-2-big-Data.db")).expect("remove Data.db");
    let (code, out, _) = find("commitlog-find.cql", &dir, "1", &[]);
    assert_eq!(code, 2, "{out}");
    assert!(out.contains("nb-2-big-Data.db: UNREADABLE"), "{out}");
    assert!(out.contains("Data.db is missing"), "{out}");
    assert!(out.contains("nb-1-big-Data.db: hit"), "{out}");
}

/// Restores a directory's permissions when dropped, so a failing assertion cannot leave an
/// unlistable directory behind.
#[cfg(unix)]
struct RestoreMode(PathBuf, u32);
#[cfg(unix)]
impl Drop for RestoreMode {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(self.1));
    }
}

/// A table directory that cannot be listed is an operational read failure: exit 2 naming the
/// underlying error, not a usage error (exit 1).
#[cfg(unix)]
#[test]
fn unlistable_table_directory_exits_two_naming_the_error() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = TempDir::new().expect("tempdir");
    let dir = tmp.path().join("users-dc2d1cf0dcc811f1be1b6d4cbb83f4d8");
    std::fs::create_dir(&dir).expect("mkdir");
    let _restore = RestoreMode(dir.clone(), 0o755);
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o000)).expect("chmod");
    if std::fs::read_dir(&dir).is_ok() {
        // Running as a user that ignores permission bits (root): the failure cannot be staged.
        eprintln!("SKIP unlistable-dir case: directory permissions are not enforced here");
        return;
    }
    let (code, _, err) = find("commitlog-find.cql", &dir, "1", &[]);
    assert_eq!(code, 2, "{err}");
    assert!(
        err.contains("cannot list") && err.to_lowercase().contains("permission denied"),
        "{err}"
    );
}

/// DOCUMENTED BYTE EDIT of the Cassandra-written segment: the record for the last `users` insert
/// (`position` from the independent ground truth, key 2 @T6) has its body truncated by 6 bytes
/// (the row's trailing cell bytes + END_OF_PARTITION) and BOTH CRCs recomputed, so the frame is
/// perfectly valid while the mutation body ends mid-row. The bytes after it are zeroed (a clean
/// section end). `find` must report that segment UNREADABLE (exit 2) — never exit 0 with an
/// `unmeasured` writetime.
#[test]
fn truncated_row_body_is_a_segment_failure_not_an_unmeasured_success() {
    let t = truth();
    let seg = t.gt["segment"]["file"].as_str().expect("segment file");
    let mut bytes = std::fs::read(t.commitlog.join(seg)).expect("segment");
    let last = t.gt["inserts"]
        .as_array()
        .expect("inserts")
        .last()
        .expect("last insert");
    let pos = last["position"].as_u64().expect("position") as usize;
    let size = i32::from_be_bytes(bytes[pos..pos + 4].try_into().expect("size")) as usize;
    let new_size = size - 6;
    let size_bytes = (new_size as i32).to_be_bytes();
    let body = bytes[pos + 8..pos + 8 + new_size].to_vec();
    let size_crc = crc32fast::hash(&size_bytes);
    let mut h = crc32fast::Hasher::new();
    h.update(&size_bytes);
    h.update(&body);
    let body_crc = h.finalize();
    let end_old = pos + 8 + size + 4;
    bytes[pos..pos + 4].copy_from_slice(&size_bytes);
    bytes[pos + 4..pos + 8].copy_from_slice(&size_crc.to_be_bytes());
    bytes[pos + 8 + new_size..pos + 8 + new_size + 4].copy_from_slice(&body_crc.to_be_bytes());
    for b in &mut bytes[pos + 8 + new_size + 4..end_old] {
        *b = 0;
    }
    let tmp = TempDir::new().expect("tempdir");
    std::fs::write(tmp.path().join(seg), &bytes).expect("write edited segment");
    let (code, out, _) = find(
        "commitlog-find.cql",
        &t.dir,
        "2",
        &["--commitlog", tmp.path().to_str().expect("utf8")],
    );
    assert_eq!(code, 2, "{out}");
    assert!(out.contains(&format!("{seg}: UNREADABLE")), "{out}");
    assert!(out.contains("malformed mutation body"), "{out}");
    // The unedited earlier mutation for key 2 is still reported.
    assert!(out.contains(&format!("{seg}@62415")), "{out}");
}
