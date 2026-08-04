//! Issue #3234 (owner decision 1C): a small **CQLite-written** BTI (`da`) fixture,
//! pinned as a **PERFORMANCE DRIFT CONTROL ONLY**.
//!
//! # READ THIS BEFORE USING THE FIXTURE THIS TEST DESCRIBES
//!
//! **It is NOT a correctness oracle, and it may never be used as one.** A corpus
//! that CQLite both WRITES and READS is *invariant to a uniform framing or
//! serialization error* (issue #3042): both halves make the identical mistake, the
//! round-trip closes, and the test stays green while real Cassandra-written data
//! reads wrong — and, symmetrically, CQLite's output is unreadable by Cassandra. For
//! BTI specifically that is not hypothetical: **#3002** (the `Rows.db` row-index root
//! base 2 bytes low, missing `writeWithShortLength`'s 2-byte prefix) hid behind
//! exactly such a symmetric test, masked by a second, compensating encoder defect.
//! Two defects that cancel are undetectable by a symmetric test *by construction*.
//!
//! The correctness oracle for BTI is **Cassandra-written bytes**:
//! `test-data/datasets/sstables/test_da/**` — including the committed small golden
//! `wide_multiclustering_small-*` added for this same issue
//! (`test-data/schemas/wide-multiclustering-small-bti.cql`) — or Cassandra 5.0.8
//! source. Never this.
//!
//! # What this fixture IS for
//!
//! A **drift control** for read-path performance work: a corpus CQLite can produce
//! anywhere, cheaply, with no container and no Cassandra, whose bytes are
//! **reproducible** — so two measurements taken weeks apart are known to have run on
//! the identical input. That is the property the Cassandra-written corpora cannot
//! offer: Cassandra stamps a wall-clock write timestamp into every row, so even a
//! same-seed regeneration changes the `Data.db` length (measured on the #3234 perf
//! corpus: 19,474,015 B vs 19,474,397 B). Here every timestamp is fixed by the
//! recorded seed, so byte-identity is achievable — and this test VERIFIES it rather
//! than asserting it by inspection: three independent writes are compared
//! component-by-component, byte for byte.
//!
//! # Shape and surface
//!
//! Written through the **production** write surface —
//! `SSTableWriter::with_format(dir, gen, &schema, expected_partitions,
//! SSTableFormat::Bti)` — on default features. **UNCOMPRESSED**: no
//! `CompressionInfo.db` and no `with_compression` call, because CQLite's production
//! write surface emits uncompressed SSTables only and configuring compressed
//! production writes is fail-closed (issue #1406). Partitions are written in
//! **Murmur3 token order**, which the writer requires.
//!
//! # The recorded identity
//!
//! `test-data/cqlite-written-bti-drift-control-identity.json` records this fixture's
//! OWN identity — per-component sha256 + byte size, the row/partition counts, and the
//! seed — never inheriting anything from the Cassandra perf corpus's manifest. A
//! deliberate writer change makes this test fail; re-record with
//! `CQLITE_RECORD_DRIFT_IDENTITY=1` and review the diff, which is precisely the drift
//! signal this control exists to raise.

#![cfg(feature = "write-support")]

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::writer::{SSTableFormat, SSTableWriter};
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId,
};
use cqlite_core::types::Value;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

const KS: &str = "drift_control";
const TBL: &str = "bti_written_small";
/// The recorded seed. Every value below — payload bytes and write timestamps
/// included — is a pure function of it, which is what makes the bytes reproducible.
const SEED: u64 = 3234;
/// `(pk, rows-in-partition, payload bytes)` — deliberately non-uniform, mirroring the
/// perf corpus's wide/narrow mix at a drift-control size. Partition 1 is WIDE on
/// purpose: 200 x ~2 KiB rows span several 64 KiB row-index blocks, so `Rows.db` is
/// non-empty and the fixture exercises the BTI row-index writer rather than only the
/// partition trie. (A drift control whose `Rows.db` is 0 bytes would silently exclude
/// the plane most BTI read-path work touches.)
const PARTITION_PLAN: &[(i32, i32, usize)] =
    &[(1, 200, 2048), (2, 25, 64), (3, 15, 64), (4, 8, 64)];
/// Fixed timestamp base: a wall-clock `now()` here would defeat byte-identity.
const TS_BASE: i64 = 1_700_000_000_000_000;
/// The buckets the `bucket text` clustering component cycles through. Distinct first
/// bytes and heterogeneous lengths (the #3032 shape).
const BUCKETS: &[&str] = &["alpha", "bo", "charlie-extended", "delta"];

/// The in-code DDL, asserted below to match the committed `.cql`.
const DDL: &str = "CREATE TABLE drift_control.bti_written_small (\n    \
                   pk int,\n    bucket text,\n    seq int,\n    payload text,\n    \
                   PRIMARY KEY (pk, bucket, seq)\n);";

const IDENTITY_REL: &str = "test-data/cqlite-written-bti-drift-control-identity.json";
const SCHEMA_REL: &str = "test-data/schemas/cqlite-written-bti-drift-control.cql";

fn repo_root() -> PathBuf {
    // cqlite-core/tests/<this file> -> workspace root.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core has a parent (the workspace root)")
        .to_path_buf()
}

/// SplitMix64 — a fixed, self-contained PRNG so the fixture does not depend on any
/// crate's generator staying stable across versions.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn payload(&mut self, len: usize) -> String {
        const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        (0..len)
            .map(|_| ALPHABET[(self.next_u64() % ALPHABET.len() as u64) as usize] as char)
            .collect()
    }
}

fn table_schema() -> TableSchema {
    let col = |name: &str, ty: &str| Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable: name == "payload",
        default: None,
        is_static: false,
    };
    TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![
            ClusteringColumn {
                name: "bucket".to_string(),
                data_type: "text".to_string(),
                position: 0,
                order: ClusteringOrder::Asc,
            },
            ClusteringColumn {
                name: "seq".to_string(),
                data_type: "int".to_string(),
                position: 1,
                order: ClusteringOrder::Asc,
            },
        ],
        columns: vec![
            col("pk", "int"),
            col("bucket", "text"),
            col("seq", "int"),
            col("payload", "text"),
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Every row of the fixture, as a pure function of `SEED`.
fn rows_from_seed() -> Vec<(i32, Vec<Mutation>)> {
    let mut rng = SplitMix64(SEED);
    let mut out = Vec::new();
    for (pk, row_count, payload_bytes) in PARTITION_PLAN {
        let mut mutations = Vec::new();
        // Clustering order must be ascending on (bucket, seq): iterate buckets in
        // sorted order, seq ascending within each.
        let mut buckets: Vec<&str> = BUCKETS.to_vec();
        buckets.sort_unstable();
        for (b_idx, bucket) in buckets.iter().enumerate() {
            for seq in 0..*row_count {
                if seq % buckets.len() as i32 != b_idx as i32 {
                    continue;
                }
                let payload = rng.payload(*payload_bytes);
                mutations.push(Mutation::new(
                    TableId::new(KS, TBL),
                    PartitionKey::single("pk", Value::Integer(*pk)),
                    Some(ClusteringKey::new(vec![
                        ("bucket".to_string(), Value::text((*bucket).to_string())),
                        ("seq".to_string(), Value::Integer(seq)),
                    ])),
                    vec![CellOperation::Write {
                        column: "payload".to_string(),
                        value: Value::text(payload),
                    }],
                    TS_BASE + (*pk as i64) * 1_000 + seq as i64,
                    None,
                ));
            }
        }
        assert!(
            !mutations.is_empty(),
            "partition {pk} planned {row_count} rows but produced none"
        );
        out.push((*pk, mutations));
    }
    out
}

/// Write the fixture into `dir` and return (component -> bytes, rows, partitions).
async fn write_fixture(dir: &Path) -> (BTreeMap<String, Vec<u8>>, usize, usize) {
    let schema = table_schema();
    let plan = rows_from_seed();
    let mut writer = SSTableWriter::with_format(
        dir.to_path_buf(),
        1,
        &schema,
        plan.len(),
        SSTableFormat::Bti,
    )
    .expect("BTI writer constructs on default features");

    // Murmur3 TOKEN order: the writer requires partitions in token order, and the
    // decorated key is the authority on that order (never the pk value).
    let mut rows = 0usize;
    let mut keyed: Vec<_> = plan
        .into_iter()
        .map(|(_pk, mutations)| {
            rows += mutations.len();
            let key = mutations[0]
                .decorated_key(&schema)
                .expect("decorated key for an int partition key");
            (key, mutations)
        })
        .collect();
    keyed.sort_by_key(|(k, _)| k.token);
    let partitions = keyed.len();
    for (key, mutations) in keyed {
        writer
            .write_partition(key, mutations)
            .expect("write_partition accepts token-ordered partitions");
    }
    let info = writer
        .finish()
        .await
        .expect("finish writes every component");
    assert_eq!(
        info.partition_count, partitions,
        "the writer must report the partitions it was given"
    );

    // The SSTable lands under <dir>/<keyspace>/<table>/.
    let table_dir = dir.join(KS).join(TBL);
    let mut components = BTreeMap::new();
    for entry in std::fs::read_dir(&table_dir).expect("component dir exists") {
        let entry = entry.expect("readable dir entry");
        let name = entry.file_name().to_string_lossy().to_string();
        components.insert(
            name,
            std::fs::read(entry.path()).expect("readable component"),
        );
    }
    assert!(
        components.contains_key("da-1-bti-Data.db"),
        "expected a da-1-bti-* component set, got {:?}",
        components.keys().collect::<Vec<_>>()
    );
    assert!(
        !components.contains_key("da-1-bti-CompressionInfo.db"),
        "the production write surface emits UNCOMPRESSED SSTables only (#1406)"
    );
    (components, rows, partitions)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    format!("{:x}", h.finalize())
}

/// OWNER REQUIREMENT 1C: byte-identity across three runs is **verified**, not
/// asserted by inspection — every component of three independent writes is compared
/// byte for byte.
#[tokio::test]
async fn three_runs_from_the_recorded_seed_are_byte_identical() {
    let dirs: Vec<TempDir> = (0..3).map(|_| TempDir::new().unwrap()).collect();
    let mut runs = Vec::new();
    for dir in &dirs {
        runs.push(write_fixture(dir.path()).await);
    }

    let (first, first_rows, first_partitions) = &runs[0];
    for (idx, (other, rows, partitions)) in runs.iter().enumerate().skip(1) {
        assert_eq!(
            first.keys().collect::<Vec<_>>(),
            other.keys().collect::<Vec<_>>(),
            "run {idx} emitted a different component SET than run 0"
        );
        assert_eq!(rows, first_rows, "run {idx} wrote a different row count");
        assert_eq!(
            partitions, first_partitions,
            "run {idx} wrote a different partition count"
        );
        for (name, bytes) in first {
            let theirs = &other[name];
            assert_eq!(
                (bytes.len(), sha256_hex(bytes)),
                (theirs.len(), sha256_hex(theirs)),
                "component {name} is NOT byte-identical between run 0 and run {idx} — \
                 this fixture's whole purpose is a reproducible drift control, so a \
                 nondeterministic byte here (a wall-clock stamp, a hash-map iteration \
                 order, an uninitialised tail) must be fixed in the writer, not waived"
            );
        }
    }
}

/// The fixture is pinned by its OWN recorded identity — per-component sha256 + byte
/// size + row/partition counts + the seed. Nothing here is inherited from the
/// Cassandra perf corpus's manifest.
#[tokio::test]
async fn recorded_identity_still_describes_what_the_writer_emits() {
    let dir = TempDir::new().unwrap();
    let (components, rows, partitions) = write_fixture(dir.path()).await;

    let mut observed = serde_json::Map::new();
    for (name, bytes) in &components {
        observed.insert(
            name.clone(),
            serde_json::json!({"bytes": bytes.len(), "sha256": sha256_hex(bytes)}),
        );
    }
    let observed = serde_json::json!({
        "issue": 3234,
        "what": "CQLite-WRITTEN BTI (`da`) drift-control fixture identity",
        "purpose_limit": "PERFORMANCE DRIFT CONTROL ONLY — never a correctness oracle \
                          (#3042; for BTI see #3002). The correctness oracle is \
                          Cassandra-written bytes under test-data/datasets/sstables/test_da/.",
        "written_by": "cqlite_core::storage::sstable::writer::SSTableWriter::with_format(.., SSTableFormat::Bti)",
        "compression": "none (uncompressed; CQLite's production write surface, #1406)",
        "generated_by_test": "cqlite-core/tests/issue_3234_cqlite_written_bti_drift_control.rs",
        "seed": SEED,
        "keyspace_table": format!("{KS}.{TBL}"),
        "rows": rows,
        "partitions": partitions,
        "partition_order": "Murmur3 token order (DecoratedKey::token)",
        "byte_shape": observed,
    });

    let identity_path = repo_root().join(IDENTITY_REL);
    if std::env::var("CQLITE_RECORD_DRIFT_IDENTITY").as_deref() == Ok("1") {
        std::fs::write(
            &identity_path,
            format!("{}\n", serde_json::to_string_pretty(&observed).unwrap()),
        )
        .expect("identity file is writable");
        return;
    }

    let recorded: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(&identity_path).unwrap_or_else(|e| {
            panic!(
                "committed identity {} is unreadable ({e}) — it is SOURCE, so its \
                 absence is a failure, never a skip",
                identity_path.display()
            )
        }),
    )
    .expect("committed identity is valid JSON");

    assert_eq!(
        recorded,
        observed,
        "the CQLite-written BTI drift-control fixture no longer matches its recorded \
         identity ({}). If the writer changed DELIBERATELY, re-record with \
         CQLITE_RECORD_DRIFT_IDENTITY=1 and review the diff — that diff IS the drift \
         signal this control exists to raise. Note what a mismatch does NOT mean: this \
         fixture is CQLite-written and CQLite-read, so it can say nothing about \
         Cassandra parity in either direction (#3042).",
        identity_path.display()
    );
}

/// The in-code DDL and the committed `.cql` must not drift apart: the schema the
/// writer is handed is the schema the committed file documents.
#[test]
fn in_code_ddl_matches_the_committed_cql() {
    let path = repo_root().join(SCHEMA_REL);
    let committed = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "committed schema {} is unreadable ({e}) — it is SOURCE, so its absence is \
             a failure, never a skip",
            path.display()
        )
    });
    let normalize = |s: &str| s.split_whitespace().collect::<Vec<_>>().join(" ");
    // Strip `--` comment lines first: the header is longer than the statement, and a
    // comment block ahead of it would otherwise be part of the first ';'-delimited chunk.
    let statements: String = committed
        .lines()
        .filter(|l| !l.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    let create = statements
        .split_inclusive(';')
        .map(str::trim)
        .find(|stmt| stmt.starts_with("CREATE TABLE"))
        .unwrap_or_else(|| panic!("{} holds no CREATE TABLE statement", path.display()));
    assert_eq!(
        normalize(create),
        normalize(DDL),
        "the committed {} CREATE TABLE and the in-code DDL have drifted apart",
        path.display()
    );

    // ... and the committed CQL really does describe the schema the writer is given.
    let parsed = cqlite_core::schema::cql_parser::parse_cql_schema(create)
        .expect("the committed CREATE TABLE parses");
    let in_code = table_schema();
    assert_eq!(parsed.keyspace, in_code.keyspace);
    assert_eq!(parsed.table, in_code.table);
    assert_eq!(
        parsed
            .partition_keys
            .iter()
            .map(|k| (k.name.as_str(), k.data_type.as_str()))
            .collect::<Vec<_>>(),
        in_code
            .partition_keys
            .iter()
            .map(|k| (k.name.as_str(), k.data_type.as_str()))
            .collect::<Vec<_>>(),
    );
    assert_eq!(
        parsed
            .clustering_keys
            .iter()
            .map(|c| (c.name.as_str(), c.data_type.as_str()))
            .collect::<Vec<_>>(),
        in_code
            .clustering_keys
            .iter()
            .map(|c| (c.name.as_str(), c.data_type.as_str()))
            .collect::<Vec<_>>(),
    );
}
