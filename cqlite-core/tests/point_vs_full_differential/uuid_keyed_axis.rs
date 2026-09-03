//! Issue #3890: the point-vs-full differential over NON-INTEGER partition keys.
//!
//! The parent file's `CORPUS` is structurally limited to tables with a single
//! **INT** partition key (`probe_keys: &'static [i64]`, `discover_pk_ints`), so it
//! could not reach the fixture that actually exhibited #3890 —
//! `test_basic.simple_table`, whose partition key is a `UUID`. That limit is not a
//! detail: a lane that cannot express a fixture's key type reports nothing about
//! that fixture however green it is.
//!
//! This axis adds a UUID-keyed corpus over the SAME comparison
//! (`assert_point_full_equal`: rows, values AND order, at the parent's PINNED
//! `now` — never wall-clock) and the same SKIP contract, and deliberately covers
//! BOTH arms of the seek's parse-bound fix:
//!
//!   * `test_basic.simple_table` — BIG (`nb`), LZ4-compressed, 19 columns. The
//!     chunk-targeted arm. This is the fixture whose point read produced 17
//!     swallowed `invalid cell flags` errors before #3890 (`Cell 'active':
//!     invalid cell flags 0x37 at offset 1223` among them).
//!   * `test_basic.uncompressed_table` — BIG (`nb`) with NO `CompressionInfo.db`,
//!     so the point read takes the WHOLE-SECTION fallback arm. That arm had the
//!     identical unbounded shape and was green only because its overrun rows
//!     happened to decode; nothing else in the corpus exercises it against a
//!     point/full differential.
//!   * `test_da.simple_table` — BTI (`da`), so the trie-resolved successor bound
//!     (rather than the BIG index's) feeds the same parse bound.
//!
//! Every case here reads FETCHED (gitignored) binaries, so `must_run` is false and
//! an absent corpus SKIPs — unless `CQLITE_REQUIRE_FIXTURES=1`, under which the
//! parent's `skip_or_fail` fails closed. A present fixture that yields ZERO
//! partition keys is a hard FAIL, never a vacuous pass.

use serial_test::serial;

use cqlite_core::config::ReadPathMode;
use cqlite_core::types::Value;
use cqlite_core::Database;

use super::{
    assert_point_full_equal, describe_search, open_db, pin_read_clock, schema_path, skip_or_fail,
    sstables_root_for_table, MAX_KEYS_PER_TABLE,
};

/// One UUID-keyed table in this axis's corpus.
struct UuidKeyCase {
    keyspace: &'static str,
    table: &'static str,
    schema: &'static str,
    pk_column: &'static str,
    /// Which arm of the single-partition seek this fixture drives (issue #3890).
    /// Asserted collectively below so dropping a fixture reds the lane rather than
    /// silently narrowing it back to one arm.
    seek_arm: &'static str,
    /// `true` iff this fixture's SSTable binaries are COMMITTED to git (authority:
    /// `git ls-files 'test-data/datasets/sstables/**-Data.db'`). None of this
    /// axis's fixtures are, so a clean SKIP on a minimal checkout is legitimate.
    must_run: bool,
}

const CORPUS: &[UuidKeyCase] = &[
    UuidKeyCase {
        keyspace: "test_basic",
        table: "simple_table",
        schema: "basic-types.cql",
        pk_column: "id",
        seek_arm: "chunk_targeted_compressed",
        must_run: false,
    },
    UuidKeyCase {
        keyspace: "test_basic",
        table: "uncompressed_table",
        schema: "basic-types.cql",
        pk_column: "id",
        seek_arm: "whole_section_fallback",
        must_run: false,
    },
    UuidKeyCase {
        keyspace: "test_da",
        table: "simple_table",
        schema: "da-test.cql",
        pk_column: "id",
        seek_arm: "bti_chunk_targeted",
        must_run: false,
    },
];

/// Canonical unquoted 8-4-4-4-12 UUID literal the SELECT parser accepts (#956).
fn uuid_literal(bytes: &[u8; 16]) -> String {
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// Discover the DISTINCT UUID partition-key literals present in `table` via a
/// full-scan `SELECT` on the `full`-mode DB (so a full-table read is legal).
/// Sorted + deduplicated, so the probe set is deterministic.
///
/// A non-UUID decode is an ERROR rather than a skip: this axis exists precisely
/// because the integer corpus could not express these keys, so silently dropping a
/// key would recreate that blind spot one level down.
async fn discover_uuid_keys(
    db: &Database,
    keyspace: &str,
    table: &str,
    pk_column: &str,
) -> Result<Vec<String>, String> {
    let query = format!("SELECT {pk_column} FROM {keyspace}.{table}");
    let result = db
        .execute(&query)
        .await
        .map_err(|e| format!("discovery SELECT failed: {e}"))?;
    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for row in &result.rows {
        match row.values.get(pk_column) {
            Some(Value::Uuid(b)) => {
                seen.insert(uuid_literal(b));
            }
            Some(other) => {
                return Err(format!(
                    "partition key {pk_column} of {keyspace}.{table} decoded as {other:?}, not a \
                     UUID; this axis only handles UUID partition keys"
                ))
            }
            None => {
                return Err(format!(
                    "scanned row of {keyspace}.{table} carries no {pk_column} column — the \
                     partition key can never be absent from a decoded row"
                ))
            }
        }
    }
    Ok(seen.into_iter().take(MAX_KEYS_PER_TABLE).collect())
}

/// Run every point query for one table under both forced paths. `Ok(true)` = ran a
/// comparison, `Ok(false)` = SKIPped (absent fixture, non-fail-closed).
async fn run_case(case: &UuidKeyCase) -> Result<bool, String> {
    // TABLE-granular resolution (#3220): every candidate root is searched for THIS
    // table's `*-Data.db`, so a root holding the keyspace without the table falls
    // through instead of being committed to.
    let Some(root) = sstables_root_for_table(case.keyspace, case.table) else {
        return skip_or_fail(&describe_search(case.keyspace, case.table));
    };
    let Some(schema) = schema_path(case.schema) else {
        return skip_or_fail(&format!("schema {} absent", case.schema));
    };

    let full_db = open_db(&root, &schema, case.keyspace, ReadPathMode::Full).await?;
    let point_db = open_db(&root, &schema, case.keyspace, ReadPathMode::Point).await?;

    let keys = discover_uuid_keys(&full_db, case.keyspace, case.table, case.pk_column).await?;
    // Anti-empty-pass: a present fixture MUST yield at least one partition key,
    // else the lane runs zero comparisons and passes vacuously.
    if keys.is_empty() {
        return Err(format!(
            "case {}.{}: a present fixture yielded no partition keys to probe — 0 rows from a \
             present Data.db is a read regression, never a skip",
            case.keyspace, case.table
        ));
    }

    // `SELECT *` (never a projected subset — issue #3890 AC2): a point row that
    // lost its later cells shows up as a differing normalized row, which a
    // two-column projection cannot see.
    for k in &keys {
        let query = format!(
            "SELECT * FROM {}.{} WHERE {} = {}",
            case.keyspace, case.table, case.pk_column, k
        );
        let rows = assert_point_full_equal(&point_db, &full_db, &query).await?;
        if rows != 1 {
            return Err(format!(
                "case {}.{}: `{query}` returned {rows} rows on BOTH paths; a discovered \
                 single-row partition key must yield exactly one",
                case.keyspace, case.table
            ));
        }
    }

    // `IN (...)` over the whole discovered key set: the union of targeted lookups
    // (point) must equal the full scan filtered in memory.
    if keys.len() >= 2 {
        let list = keys.join(", ");
        let query = format!(
            "SELECT * FROM {}.{} WHERE {} IN ({})",
            case.keyspace, case.table, case.pk_column, list
        );
        assert_point_full_equal(&point_db, &full_db, &query).await?;
    }

    eprintln!(
        "PASS(uuid-axis) {}.{} — {} point queries + IN, point == full (seek arm: {})",
        case.keyspace,
        case.table,
        keys.len(),
        case.seek_arm
    );
    Ok(true)
}

/// PURE decision behind the must-run assertion (mirrors the parent's): every
/// `must_run` case absent from `ran`, by table name.
fn must_run_violations<'a>(cases: &'a [UuidKeyCase], ran: &[String]) -> Vec<&'a str> {
    cases
        .iter()
        .filter(|c| {
            c.must_run
                && !ran
                    .iter()
                    .any(|id| id == &format!("{}.{}", c.keyspace, c.table))
        })
        .map(|c| c.table)
        .collect()
}

/// `#[serial]`: writes the process-global pinned-clock seam the sibling axes in
/// this same binary also write.
#[tokio::test]
#[serial]
async fn uuid_keyed_point_vs_full_differential_equality() {
    // Corpus coverage: BOTH arms of the #3890 parse bound must be exercised. The
    // whole-section fallback is the arm nothing else in this target reaches, so
    // dropping it would leave the surviving instance of the identical defect
    // uncovered.
    let arms: std::collections::BTreeSet<&str> = CORPUS.iter().map(|c| c.seek_arm).collect();
    for required in [
        "chunk_targeted_compressed",
        "whole_section_fallback",
        "bti_chunk_targeted",
    ] {
        assert!(
            arms.contains(required),
            "corpus must exercise the {required:?} single-partition seek arm (issue #3890)"
        );
    }

    let _clock = pin_read_clock();

    let mut ran: Vec<String> = Vec::new();
    let mut failures: Vec<String> = Vec::new();
    for case in CORPUS {
        match run_case(case).await {
            Ok(true) => ran.push(format!("{}.{}", case.keyspace, case.table)),
            Ok(false) => {}
            Err(e) => failures.push(format!("{}.{}: {e}", case.keyspace, case.table)),
        }
    }
    assert!(
        failures.is_empty(),
        "issue #3890 UUID-keyed point-vs-full axis reported {} failure(s):\n{}",
        failures.len(),
        failures.join("\n")
    );

    let violations = must_run_violations(CORPUS, &ran);
    assert!(
        violations.is_empty(),
        "committed-fixture case(s) {violations:?} did not run — a SKIP of a committed fixture \
         means the lane failed to RESOLVE it (issue #3220)"
    );

    if ran.is_empty() {
        eprintln!(
            "SKIP(uuid-axis): no fixture of the UUID-keyed corpus was resolvable \
             (all cases read FETCHED binaries); set CQLITE_REQUIRE_FIXTURES=1 to fail closed"
        );
    }
}

/// The must-run guard has a proof it CAN fire (a fail-closed guard whose failing
/// branch is never exercised is indistinguishable from one that cannot fire).
#[test]
fn uuid_axis_must_run_violations_flags_a_committed_case_that_did_not_run() {
    let all: Vec<String> = CORPUS
        .iter()
        .map(|c| format!("{}.{}", c.keyspace, c.table))
        .collect();
    assert!(
        must_run_violations(CORPUS, &all).is_empty(),
        "with every case run there can be no violation"
    );
    // A synthetic committed case that did not run MUST be reported.
    let committed = [UuidKeyCase {
        keyspace: "test_basic",
        table: "synthetic_committed",
        schema: "basic-types.cql",
        pk_column: "id",
        seek_arm: "chunk_targeted_compressed",
        must_run: true,
    }];
    assert_eq!(
        must_run_violations(&committed, &[]),
        vec!["synthetic_committed"],
        "a must_run case absent from the ran set must be reported"
    );
    // And a FETCHED (must_run == false) case that skipped is NOT a violation.
    assert!(
        must_run_violations(CORPUS, &[]).is_empty(),
        "this axis declares no committed fixtures, so an all-skip run is not a violation"
    );
}
