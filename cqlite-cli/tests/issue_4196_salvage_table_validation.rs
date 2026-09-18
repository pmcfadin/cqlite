//! Issue #4196 C-audit gap (R7.9) — `cqlite salvage`'s wrong-table `--schema`
//! guard, driven END-TO-END through the compiled binary for BOTH schema
//! formats it accepts.
//!
//! `load_compaction_table_schema_for_table` (`cqlite-cli/src/commands/
//! write.rs`) cross-checks a JSON `--schema` file's declared table against the
//! table salvage is actually processing via
//! [`crate::commands::schema_load::assert_table_matches`] — proven, for JSON,
//! only by the inline `#[cfg(test)]` unit test
//! `write::tests::json_schema_declaring_another_table_fails_closed_naming_both`
//! (no gate component executes an inline unit-test module). Before that fix,
//! a JSON `--schema` declaring table `b`'s column set was silently applied to
//! table `a`'s bytes and salvage reported a confidently clean recovery at exit
//! 0 (roborev, issue #4196, round-23 High finding, confirmed by an independent
//! Cassandra-format expert review).
//!
//! The CQL branch enforces the same property through a different code path
//! (`load_compaction_table_schema_selected`'s per-statement SELECTOR: a
//! `target_table` that matches no declared `CREATE TABLE` leaves `matched`
//! `None` and the function fails closed naming both the unresolved target and
//! every table the file DOES declare) — proven only by
//! `salvage_cli_tests::unmatched_directory_name_without_table_flag_fails_closed`,
//! which exercises table-name DERIVATION failing (no `--table`, an unmatched
//! directory name) rather than an EXPLICIT `--table` colliding with an
//! explicit wrong-table schema. Neither existing test drives the JSON format
//! through the CLI at all.
//!
//! Every case here forces `target_table` via `--table` rather than directory-
//! name derivation, so the property under test — "the DECLARED table must
//! match the table being processed" — is isolated from the unrelated
//! derivation question `unmatched_directory_name_without_table_flag_fails_closed`
//! already covers.

// `not(tombstones)`: mirrors `commands::salvage`'s own module gate — the binary
// this file drives via `CARGO_BIN_EXE_cqlite` has no `salvage` verb at all when
// `tombstones` is on.
#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::TempDir;

/// `test_comp.lz4_table` — BIG (`nb`), LZ4, real Cassandra 5.0 output. Every
/// component listed is git-TRACKED (`git ls-files
/// test-data/datasets/sstables/test_comp/`), so an absence is a broken
/// checkout and never an unfetched dataset. Which real table this fixture
/// happens to be is irrelevant here: every case forces `target_table` via
/// `--table a`, so the fixture is used purely as "some real SSTable bytes".
const FIXTURE_RELATIVE: &str = "sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77";
const FIXTURE_DIR_NAME: &str = "lz4_table-25801a0071a911f19b3225f9984c6a77";
const FIXTURE_COMPONENTS: &[&str] = &[
    "nb-1-big-Data.db",
    "nb-1-big-Index.db",
    "nb-1-big-Summary.db",
    "nb-1-big-Statistics.db",
    "nb-1-big-CompressionInfo.db",
    "nb-1-big-Filter.db",
    "nb-1-big-Digest.crc32",
    "nb-1-big-TOC.txt",
];

/// The table this run is FORCED to target, via `--table`, in every case below.
const TARGET_TABLE: &str = "a";

/// The table every wrong schema DECLARES instead.
const WRONG_TABLE: &str = "b";

/// Every candidate base root (the `CQLITE_DATASETS_ROOT` corpus, then the
/// checkout's own committed corpus — issue #3220: neither is a superset of the
/// other, so both are searched and the one carrying the fixture wins).
fn candidate_base_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Ok(env_root) = std::env::var("CQLITE_DATASETS_ROOT") {
        roots.push(PathBuf::from(env_root));
    }
    let checkout = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("repo root")
        .join("test-data/datasets");
    if !roots.contains(&checkout) {
        roots.push(checkout);
    }
    roots
}

/// The first candidate root carrying EVERY component of the fixture, or a hard
/// failure naming the roots searched.
///
/// Resolved per TABLE and asserted per CASE (issue #3220): a resolver that
/// commits to a root by KEYSPACE can pass without ever running, and a skip
/// that reads as a pass is the exact defect class this file's tests exist to
/// catch. Never gated on `CQLITE_REQUIRE_FIXTURES` — these binaries are
/// git-tracked.
fn resolve_committed_fixture() -> PathBuf {
    let roots = candidate_base_roots();
    for root in &roots {
        let dir = root.join(FIXTURE_RELATIVE);
        if FIXTURE_COMPONENTS.iter().all(|c| dir.join(c).is_file()) {
            return dir;
        }
    }
    panic!(
        "COMMITTED fixture {FIXTURE_RELATIVE} (components {FIXTURE_COMPONENTS:?}) is absent — its \
         binaries are git-tracked, so this is a broken checkout, NOT an unfetched dataset, and \
         must never skip (issue #3220, fail-closed UNCONDITIONALLY). Searched: {roots:?}"
    );
}

/// Copy the fixture's real components (never the `.jsonl`/`.db.txt` sidecars)
/// into a fresh staged table directory under `dest_parent`.
fn stage_input(clean_dir: &Path, dest_parent: &Path) -> PathBuf {
    let dest = dest_parent.join(FIXTURE_DIR_NAME);
    std::fs::create_dir_all(&dest).unwrap_or_else(|e| panic!("create {dest:?}: {e}"));
    for component in FIXTURE_COMPONENTS {
        std::fs::copy(clean_dir.join(component), dest.join(component))
            .unwrap_or_else(|e| panic!("copy {component}: {e}"));
    }
    dest
}

fn as_arg(path: &Path) -> &str {
    path.to_str()
        .unwrap_or_else(|| panic!("{path:?} is not utf-8"))
}

fn stdio(output: &Output) -> (String, String) {
    (
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

/// A JSON schema file declaring `table`, mirroring the shape
/// `write::tests::json_schema_declaring` uses.
fn json_schema_declaring(table: &str) -> tempfile::NamedTempFile {
    let mut f = tempfile::Builder::new()
        .suffix(".json")
        .tempfile()
        .expect("temp file");
    write!(
        f,
        r#"{{
            "keyspace": "test_ks",
            "table": "{table}",
            "columns": {{
                "id": {{ "type": "int", "kind": "PartitionKey" }},
                "n": {{ "type": "text", "kind": "Regular" }}
            }}
        }}"#
    )
    .expect("write schema");
    f
}

/// A CQL schema file declaring exactly one table, `table` — the CQL sibling of
/// [`json_schema_declaring`].
fn cql_schema_declaring(table: &str) -> tempfile::NamedTempFile {
    let mut f = tempfile::Builder::new()
        .suffix(".cql")
        .tempfile()
        .expect("temp file");
    write!(
        f,
        "CREATE KEYSPACE test_ks WITH replication = {{'class':'SimpleStrategy'}};\n\
         CREATE TABLE test_ks.{table} (id int PRIMARY KEY, n text);\n"
    )
    .expect("write schema");
    f
}

/// Every `*-Data.db` anywhere under `dir` (recursive) — a refused run must
/// leave none.
fn data_dbs_under(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir) else {
        return found;
    };
    for entry in rd.flatten() {
        let path = entry.path();
        if path.is_dir() {
            found.extend(data_dbs_under(&path));
        } else if path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.ends_with("-Data.db"))
            .unwrap_or(false)
        {
            found.push(path);
        }
    }
    found
}

fn assert_out_unpopulated(out: &Path) {
    let written = data_dbs_under(out);
    assert!(
        written.is_empty(),
        "nothing may be written under --out {out:?} when the run is refused as a usage error; \
         found {written:?}"
    );
}

/// `--schema <schema> salvage <input> --out <out> --table a` through the REAL
/// compiled binary — `execute_salvage_command` enforces its exit codes via
/// `std::process::exit`, which only a subprocess can observe.
fn run_salvage_for_table(schema: &Path, input: &Path, out: &Path, table: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args([
            "--schema",
            as_arg(schema),
            "salvage",
            as_arg(input),
            "--out",
            as_arg(out),
            "--table",
            table,
        ])
        .output()
        .expect("failed to execute cqlite binary")
}

/// roborev, issue #4196, round-23 High finding (confirmed by an independent
/// Cassandra-format expert review) — a JSON `--schema` declaring a DIFFERENT
/// table than the one `--table` names must be refused end-to-end: exit
/// non-zero, stderr naming BOTH tables, and nothing written under `--out`.
///
/// Before the fix this returned table `b`'s column set for table `a`'s data
/// and salvage reported a confidently clean recovery at exit 0 — the worst
/// failure shape for a data-recovery tool.
#[test]
fn json_schema_declaring_wrong_table_is_refused_and_out_stays_empty() {
    let clean_dir = resolve_committed_fixture();
    let temp = TempDir::new().expect("tempdir");
    let input_dir = stage_input(&clean_dir, temp.path());
    let out = temp.path().join("out");

    let schema = json_schema_declaring(WRONG_TABLE);
    let output = run_salvage_for_table(schema.path(), &input_dir, &out, TARGET_TABLE);
    let (stdout, stderr) = stdio(&output);

    assert!(
        !output.status.success(),
        "a JSON --schema declaring table '{WRONG_TABLE}' must be refused for --table \
         {TARGET_TABLE}, never silently applied to its data; stdout={stdout}\nstderr={stderr}"
    );
    assert!(
        stderr.contains(&format!("'{WRONG_TABLE}'")),
        "the refusal must NAME the DECLARED (wrong) table; got: {stderr}"
    );
    assert!(
        stderr.contains(&format!("'{TARGET_TABLE}'")),
        "the refusal must NAME the TARGET table so an operator can supply the right schema; got: \
         {stderr}"
    );

    assert_out_unpopulated(&out);
}

/// The CQL sibling of the JSON case above: a CQL `--schema` declaring only
/// table `b` must be refused, named, for `--table a` — proven at the CLI
/// level for the first time (the existing
/// `unmatched_directory_name_without_table_flag_fails_closed` exercises
/// DIRECTORY-NAME-DERIVED table resolution failing with no `--table`, a
/// different property, over a real multi-table schema file rather than a
/// single explicitly-wrong one).
#[test]
fn cql_schema_declaring_wrong_table_is_refused_and_out_stays_empty() {
    let clean_dir = resolve_committed_fixture();
    let temp = TempDir::new().expect("tempdir");
    let input_dir = stage_input(&clean_dir, temp.path());
    let out = temp.path().join("out");

    let schema = cql_schema_declaring(WRONG_TABLE);
    let output = run_salvage_for_table(schema.path(), &input_dir, &out, TARGET_TABLE);
    let (stdout, stderr) = stdio(&output);

    assert!(
        !output.status.success(),
        "a CQL --schema declaring only table '{WRONG_TABLE}' must be refused for --table \
         {TARGET_TABLE}, never silently applied to its data; stdout={stdout}\nstderr={stderr}"
    );
    assert!(
        stderr.contains(&format!("'{TARGET_TABLE}'")),
        "the refusal must NAME the unresolved TARGET table; got: {stderr}"
    );
    assert!(
        stderr.contains(&format!("present: {WRONG_TABLE}")),
        "the refusal must NAME at least one table the schema file actually declares, so an \
         operator can supply --table correctly; got: {stderr}"
    );

    assert_out_unpopulated(&out);
}
