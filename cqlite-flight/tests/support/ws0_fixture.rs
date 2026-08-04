//! The `ws0.events` CI PERFORMANCE FIXTURE, self-contained (issue #3096).
//!
//! # What this is, and why it lives here
//!
//! `tests/issue_3096_arrow_buffer_digest.rs` (the Arrow-buffer digest oracle) and
//! `tests/issue_3096_framing_subphase.rs` (the IPC-framing attribution test) each
//! need a small, deterministic, uncompressed `ws0.events` SSTable they can build
//! inside the test — no fetched dataset, no external corpus, no skip.
//!
//! Those two tests were originally fed by the measurement rig's generator crate.
//! **That rig — the corpus-generator CLI, the bare-scan bench and the `perf`
//! driver scripts — is re-anchored to issue #3272** and is not part of this
//! change, so the fixture builder the CI tests depend on lives here instead: the
//! minimum needed to write the fixture, and nothing that only the measurement rig
//! needed (no CLI, no `sha256` corpus-identity record, no bench binary).
//!
//! # The null plan ([`NullPlan`]) and what depends on it
//!
//! [`CorpusSpec::nulls`] selects whether any cell is ABSENT. It defaults to
//! [`NullPlan::None`] — every non-key column written on every row, the exact row
//! synthesis this module shipped with — so a consumer that does not opt in gets
//! BYTE-IDENTICAL fixture bytes. `tests/issue_3096_framing_subphase.rs` is such a
//! consumer and is deliberately left on the default.
//!
//! [`NullPlan::Pinned`] adds the deterministic absent-cell pattern the
//! Arrow-buffer digest oracle needs to exercise VALIDITY BITMAPS at all (issue
//! #3096, roborev finding 2: with no nulls anywhere, a validity-bit defect is
//! invisible to a digest that folds the bitmap). Only
//! `tests/issue_3096_arrow_buffer_digest.rs` opts in, and it re-pins its digests
//! against it.
//!
//! # THIS FIXTURE IS A PERFORMANCE FIXTURE ONLY — NEVER A CORRECTNESS ORACLE
//!
//! It is **CQLite-written and CQLite-read**. Per issue #3042 that round trip is
//! INVARIANT to a uniform framing/serialization error: both sides make the
//! identical mistake, the round trip closes, and the test stays green while real
//! Cassandra-written data would read wrong. So:
//!
//! * **No on-disk framing or encoding correctness claim may rest on it** — not
//!   row/cell framing, not VInt encoding, not the index or statistics layout.
//! * Correctness stays anchored to the Cassandra-written fixtures
//!   (`test-data/datasets/`, the `nb`/`da` goldens, the sstabledump JSONL
//!   references).
//! * What it IS good for: holding the BYTES CONSTANT while something ELSE varies —
//!   the merge arm vs the bypass arm, or one sub-phase's attribution vs another's.
//!
//! # Uncompressed by construction (issue #1406)
//!
//! CQLite's production write surface emits UNCOMPRESSED SSTables only and never a
//! `CompressionInfo.db`. [`generate`] ASSERTS that absence rather than assuming
//! it, and reports it in [`FixtureIdentity`] so the consuming test can too.
//!
//! No-heuristics (issue #28): every column name and type below comes from the
//! committed `ws0.events` DDL. Nothing is inferred from bytes or from a file name.

// Each consuming integration-test binary includes this whole module but uses only
// the part it needs (the framing test needs no `has_data_db`, for instance), so
// unused-item warnings here are a property of the include, not of the code.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

use cqlite_core::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::writer::SSTableWriter;
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId,
};
use cqlite_core::storage::write_engine::DecoratedKey;
use cqlite_core::types::Value;

// ---------------------------------------------------------------------------
// The pinned schema
// ---------------------------------------------------------------------------

/// Keyspace of the pinned fixture table.
pub const KEYSPACE: &str = "ws0";
/// Table of the pinned fixture table.
pub const TABLE: &str = "events";

/// The pinned DDL, byte-identical to the committed
/// `docs/reports/ws0-3100-artifacts/ws0-h2h/schemas/ws0-events.cql` (asserted by
/// [`assert_ddl_matches_the_committed_pin`]). Written next to the fixture so a
/// consumer reads the exact schema the fixture was written from.
pub const DDL: &str = "CREATE TABLE ws0.events (part_id text, seq int, event_time timestamp, blob_a blob, blob_b blob, device_id uuid, metric_a int, metric_b bigint, metric_c double, payload text, region text, status text, PRIMARY KEY (part_id, seq, event_time)) WITH CLUSTERING ORDER BY (seq ASC, event_time ASC);";

/// The twelve `(name, cql_type)` pairs in DDL declaration order.
pub const COLUMNS: [(&str, &str); 12] = [
    ("part_id", "text"),
    ("seq", "int"),
    ("event_time", "timestamp"),
    ("blob_a", "blob"),
    ("blob_b", "blob"),
    ("device_id", "uuid"),
    ("metric_a", "int"),
    ("metric_b", "bigint"),
    ("metric_c", "double"),
    ("payload", "text"),
    ("region", "text"),
    ("status", "text"),
];

/// The three PRIMARY KEY column names: partition key first, then the two
/// clustering columns in order.
const PK_COLUMNS: [&str; 3] = ["part_id", "seq", "event_time"];

/// Non-key columns per row — the twelve [`COLUMNS`] less the three
/// [`PK_COLUMNS`]. The census a [`NullPlan`] takes cells away from.
pub const NON_KEY_COLUMNS: u64 = (COLUMNS.len() - PK_COLUMNS.len()) as u64;

/// The pinned `ws0.events` [`TableSchema`], built from [`COLUMNS`] so the schema
/// and the emitted DDL can never drift.
pub fn ws0_events_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: TABLE.to_string(),
        partition_keys: vec![KeyColumn {
            name: "part_id".to_string(),
            data_type: "text".to_string(),
            position: 0,
        }],
        clustering_keys: vec![
            ClusteringColumn {
                name: "seq".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: Default::default(),
            },
            ClusteringColumn {
                name: "event_time".to_string(),
                data_type: "timestamp".to_string(),
                position: 1,
                order: Default::default(),
            },
        ],
        columns: COLUMNS
            .iter()
            .map(|(name, ty)| Column {
                name: (*name).to_string(),
                data_type: (*ty).to_string(),
                nullable: !PK_COLUMNS.contains(name),
                default: None,
                is_static: false,
            })
            .collect(),
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

/// Assert the in-code [`DDL`] is still byte-identical (modulo trailing
/// whitespace) to the committed artifact, so a fixture can never be built from a
/// DDL that silently drifted from the schema the measurement artifacts cite.
///
/// Called by the consuming tests rather than being a `#[test]` of its own: this
/// file is a support module included into test binaries, not a test target.
pub fn assert_ddl_matches_the_committed_pin() {
    // `cqlite-flight/tests/support` -> `cqlite-flight/tests` -> `cqlite-flight`
    // -> repo root.
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-flight has a parent")
        .to_path_buf();
    let pin = repo_root.join("docs/reports/ws0-3100-artifacts/ws0-h2h/schemas/ws0-events.cql");
    let committed = std::fs::read_to_string(&pin)
        .unwrap_or_else(|e| panic!("read committed DDL pin {}: {e}", pin.display()));
    assert_eq!(
        committed.trim(),
        DDL.trim(),
        "the fixture's pinned DDL diverged from {}",
        pin.display()
    );
}

// ---------------------------------------------------------------------------
// Deterministic row synthesis
// ---------------------------------------------------------------------------

/// Deterministic, portable PRNG (SplitMix64).
///
/// Deliberately hand-rolled rather than taken from the `rand` crate: the fixture's
/// bytes are what a PINNED digest rests on, so the stream must not depend on a
/// dependency's choice of generator or on a version bump changing it. SplitMix64
/// is fully specified by the three constants below.
#[derive(Debug, Clone)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    /// Seed the generator. Any `u64` (including 0) is a valid seed.
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Next 64 pseudo-random bits.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Fill `buf` with pseudo-random bytes.
    pub fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let word = self.next_u64().to_le_bytes();
            let n = chunk.len();
            chunk.copy_from_slice(&word[..n]);
        }
    }

    /// A value in `0..n` (`n > 0`). Modulo bias is irrelevant here: this picks
    /// fixture labels from small fixed tables, not cryptographic material.
    pub fn below(&mut self, n: u64) -> u64 {
        debug_assert!(n > 0, "below(0) has no valid result");
        self.next_u64() % n.max(1)
    }
}

/// `blob_a` width in bytes.
pub const BLOB_A_LEN: usize = 96;
/// `blob_b` width in bytes.
pub const BLOB_B_LEN: usize = 96;
/// `payload` width in characters.
pub const PAYLOAD_LEN: usize = 414;

/// Base of the synthetic `event_time` clustering value, in milliseconds since the
/// epoch. A FIXED constant (2023-11-14T22:13:20Z), never wall-clock: a
/// wall-clock-derived value would make the fixture non-reproducible.
pub const EVENT_TIME_BASE_MS: i64 = 1_700_000_000_000;

/// Base write timestamp in MICROseconds. Also fixed — the writer folds it into
/// `Statistics.db` min/max, so a wall-clock value would change the on-disk bytes
/// run to run.
pub const WRITE_TS_BASE_MICROS: i64 = 1_700_000_000_000_000;

/// The `region` label set (a low-cardinality text column).
const REGIONS: [&str; 6] = [
    "us-east-1",
    "us-west-2",
    "eu-west-1",
    "eu-central-1",
    "ap-south-1",
    "ap-northeast-1",
];

/// The `status` label set.
const STATUSES: [&str; 4] = ["OK", "WARN", "ERROR", "UNKNOWN"];

/// Printable alphabet for `payload`, so a fixture dump stays greppable.
const PAYLOAD_ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz012345";

/// The partition key text for partition `p`. Fixed width (`p` + 7 digits) so
/// every partition key serializes to the same length.
pub fn part_id(p: u64) -> String {
    format!("p{p:07}")
}

// ---------------------------------------------------------------------------
// The deterministic null plan (issue #3096, roborev finding 2)
// ---------------------------------------------------------------------------

/// Which cells, if any, are ABSENT from a written row.
///
/// An absent cell reads back as a NULL, so this is what puts content into the
/// Arrow validity bitmaps the digest oracle folds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NullPlan {
    /// Every non-key column written on every row: 12 non-null cells per row, no
    /// validity-bitmap content anywhere. The original synthesis; the DEFAULT, so
    /// a consumer that does not opt in keeps byte-identical fixture bytes.
    #[default]
    None,
    /// The pinned absent-cell pattern, keyed on the row's index WITHIN its
    /// partition (`r`) so it is a pure function of the fixture spec — no
    /// wall-clock, no RNG draw, no dependence on which partition landed where in
    /// token order.
    ///
    /// # Why these positions
    ///
    /// The rule that carries the coverage is `metric_a`'s stride of 8. A batch's
    /// validity bitmap is bit-packed 8 rows to the byte, and a partition's rows
    /// enter the stream at offset `k * rows_per_partition` for token-order rank
    /// `k`. With `rows_per_partition = 100` (≡ 4 mod 8), a stride-8 rule inside
    /// the partition therefore lands on **bit 0 of a byte** for even `k` and on
    /// **bit 4 of a byte** for odd `k` — a byte-aligned null AND a null at a
    /// non-boundary offset, in the SAME column. A misplaced validity bit moves
    /// the digest instead of disappearing into bitmap padding.
    ///
    /// The other three rules widen the shape rather than the alignment:
    ///
    /// * `region` (`r % 8 == 3`) — a VAR-WIDTH column, so a null must also be
    ///   consistent with its offsets buffer.
    /// * `payload` (`r % 40 == 17`) — a wide var-width column at a stride
    ///   coprime with 8, so its nulls sweep several distinct bit offsets.
    /// * `device_id` (last row of each partition) — a FIXED-SIZE-BINARY column
    ///   nulled at the partition tail, which for the final partition is the last
    ///   valid bit of the final batch's last bitmap byte, immediately adjacent to
    ///   the padding bits.
    ///
    /// Every rule leaves at least six of the nine non-key columns written, so no
    /// row degenerates to a bare row marker.
    Pinned,
}

/// Is `column` ABSENT for row `r` of a partition of `rows_per_partition` rows?
///
/// The partition-key and clustering columns (`part_id`, `seq`, `event_time`) can
/// never be absent and are not named here — CQL forbids a null key component.
pub fn column_is_absent(plan: NullPlan, column: &str, r: u64, rows_per_partition: u64) -> bool {
    match plan {
        NullPlan::None => false,
        NullPlan::Pinned => match column {
            "metric_a" => r % 8 == 0,
            "region" => r % 8 == 3,
            "payload" => r % 40 == 17,
            // The LAST row of each partition. Expressed against
            // `rows_per_partition` rather than a literal so the rule stays the
            // "partition tail" it claims to be at any partition size.
            "device_id" => r + 1 == rows_per_partition,
            _ => false,
        },
    }
}

/// The per-row PRNG, seeded from `(seed, p, r)` by mixing the three through
/// SplitMix64's own avalanche so adjacent rows do not share a stream prefix.
fn row_rng(seed: u64, p: u64, r: u64) -> SplitMix64 {
    let mut mixer = SplitMix64::new(seed ^ p.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    // Advance by a value derived from `r` without a loop over `r` (a per-row loop
    // would make generation O(rows_per_partition^2)).
    let base = mixer.next_u64();
    SplitMix64::new(base ^ r.wrapping_mul(0xD6E8_FEB8_6659_FD93))
}

/// Build the [`Mutation`] for row `r` of partition `p`.
///
/// One mutation per clustering row: `PRIMARY KEY (part_id, seq, event_time)` with
/// up to nine non-key columns written as simple cells — the ones `plan` marks
/// absent are OMITTED, and read back as nulls. `global_row` orders the write
/// timestamps deterministically across the whole fixture.
///
/// Every PRNG draw happens unconditionally, BEFORE the absent cells are dropped,
/// so a cell that survives the plan carries exactly the value it carries under
/// [`NullPlan::None`]: the two plans differ by absent cells alone, never by a
/// shifted random stream.
pub fn row_mutation(
    seed: u64,
    p: u64,
    r: u64,
    global_row: u64,
    plan: NullPlan,
    rows_per_partition: u64,
) -> Mutation {
    let mut rng = row_rng(seed, p, r);

    let mut blob_a = vec![0u8; BLOB_A_LEN];
    rng.fill(&mut blob_a);
    let mut blob_b = vec![0u8; BLOB_B_LEN];
    rng.fill(&mut blob_b);
    let mut device_id = [0u8; 16];
    rng.fill(&mut device_id);

    let metric_a = rng.next_u64() as u32 as i32;
    let metric_b = rng.next_u64() as i64;
    // A finite double with no NaN/Inf: those would be legal CQL but would make an
    // Arrow-buffer digest sensitive to NaN bit patterns for no measurement value.
    let metric_c = (rng.next_u64() % 1_000_000_000) as f64 / 1_000.0;

    let mut payload = vec![0u8; PAYLOAD_LEN];
    rng.fill(&mut payload);
    for b in payload.iter_mut() {
        *b = PAYLOAD_ALPHABET[(*b as usize) % PAYLOAD_ALPHABET.len()];
    }
    let payload = String::from_utf8(payload).unwrap_or_else(|_| {
        // Unreachable: every byte was just mapped into a 32-char ASCII alphabet.
        // Kept total rather than panicking so a future alphabet edit degrades to a
        // visibly-wrong-but-valid fixture instead of aborting the run.
        "a".repeat(PAYLOAD_LEN)
    });

    let region = REGIONS[rng.below(REGIONS.len() as u64) as usize];
    let status = STATUSES[rng.below(STATUSES.len() as u64) as usize];

    let event_time = EVENT_TIME_BASE_MS + (r as i64) * 1_000;

    let mut cells = vec![
        write("blob_a", Value::Blob(blob_a.into())),
        write("blob_b", Value::Blob(blob_b.into())),
        write("device_id", Value::Uuid(device_id)),
        write("metric_a", Value::Integer(metric_a)),
        write("metric_b", Value::BigInt(metric_b)),
        write("metric_c", Value::Float(metric_c)),
        write("payload", Value::text(payload)),
        write("region", Value::text(region)),
        write("status", Value::text(status)),
    ];
    // Drop the cells `plan` marks absent. `NullPlan::None` retains all nine, so
    // this is a no-op there and the fixture bytes are unchanged.
    cells.retain(|cell| match cell {
        CellOperation::Write { column, .. } => {
            !column_is_absent(plan, column, r, rows_per_partition)
        }
        // Every cell built above is a `Write`; a future non-`Write` operation is
        // retained rather than silently dropped by a wildcard that guessed.
        _ => true,
    });

    Mutation::new(
        TableId::new(KEYSPACE, TABLE),
        PartitionKey::single("part_id", Value::text(part_id(p))),
        Some(ClusteringKey::new(vec![
            ("seq".to_string(), Value::Integer(r as i32)),
            ("event_time".to_string(), Value::Timestamp(event_time)),
        ])),
        cells,
        WRITE_TS_BASE_MICROS + global_row as i64,
        None,
    )
}

fn write(column: &str, value: Value) -> CellOperation {
    CellOperation::Write {
        column: column.to_string(),
        value,
    }
}

// ---------------------------------------------------------------------------
// Writing the fixture
// ---------------------------------------------------------------------------

/// The recorded seed. Changing it changes the fixture bytes and therefore every
/// digest pinned against them.
pub const DEFAULT_SEED: u64 = 30_960_001;

/// Boxed error alias — this is fixture tooling, not library code on a hot path.
pub type GenResult<T> = Result<T, Box<dyn std::error::Error>>;

/// What to generate and where.
#[derive(Debug, Clone)]
pub struct CorpusSpec {
    /// Fixture root; the SSTable lands at `<out>/ws0/events/`.
    pub out: PathBuf,
    /// Total rows. Must be an exact multiple of `rows_per_partition`.
    pub rows: u64,
    /// Rows per partition.
    pub rows_per_partition: u64,
    /// Generation seed.
    pub seed: u64,
    /// Which cells, if any, are absent. See [`NullPlan`].
    pub nulls: NullPlan,
}

impl CorpusSpec {
    /// A cheap CI-sized fixture: `rows` rows in 100-row partitions, every cell
    /// present ([`NullPlan::None`]).
    pub fn small(out: PathBuf, rows: u64) -> Self {
        Self {
            out,
            rows,
            rows_per_partition: 100,
            seed: DEFAULT_SEED,
            nulls: NullPlan::None,
        }
    }

    /// Select a [`NullPlan`]. Consumes and returns `self` for chaining.
    pub fn with_null_plan(mut self, nulls: NullPlan) -> Self {
        self.nulls = nulls;
        self
    }

    /// Directory the SSTable components land in.
    pub fn table_dir(&self) -> PathBuf {
        self.out.join(KEYSPACE).join(TABLE)
    }
}

/// What [`generate`] confirmed about the fixture it wrote.
///
/// Every field is MEASURED from the write, never assumed, so a consuming test can
/// assert non-vacuity (a 0-row fixture would make its own assertions trivially
/// true) in the same currency.
#[derive(Debug, Clone)]
pub struct FixtureIdentity {
    /// Rows actually written.
    pub rows: u64,
    /// Partitions the writer confirmed.
    pub partitions: u64,
    /// Non-key cells actually handed to the writer, COUNTED from the emitted
    /// mutations. With `NullPlan::None` this is `rows * 9`.
    pub cells_written: u64,
    /// Non-key cells the [`NullPlan`] dropped, COUNTED the same way. Zero under
    /// `NullPlan::None`; a consumer that opted into a null-bearing plan asserts
    /// this is non-zero rather than assuming the plan took effect.
    pub cells_absent: u64,
    /// `Data.db` size in bytes (non-zero, asserted).
    pub data_db_bytes: u64,
    /// Whether a `CompressionInfo.db` was emitted. Always `false` — the write
    /// surface is uncompressed-only (#1406) — and asserted, not assumed.
    pub compression_info_present: bool,
}

/// Write the fixture, returning what was confirmed about it.
///
/// Fails closed on every anti-vacuity condition: a zero/short row count, a
/// partition count the writer did not confirm, an empty `Data.db`, or a
/// `CompressionInfo.db` (the write surface is uncompressed-only, issue #1406).
pub async fn generate(spec: &CorpusSpec) -> GenResult<FixtureIdentity> {
    if spec.rows == 0 || spec.rows_per_partition == 0 {
        return Err(
            "rows and rows-per-partition must both be > 0 — a 0-row fixture \
                    would let every downstream assertion pass vacuously"
                .into(),
        );
    }
    if spec.rows % spec.rows_per_partition != 0 {
        return Err(format!(
            "rows ({}) must be an exact multiple of rows-per-partition ({})",
            spec.rows, spec.rows_per_partition
        )
        .into());
    }
    let partitions = spec.rows / spec.rows_per_partition;
    let schema = ws0_events_schema();
    let table_dir = spec.table_dir();

    if table_dir.exists() {
        std::fs::remove_dir_all(&table_dir)?;
    }
    std::fs::create_dir_all(&spec.out)?;

    let keyed = token_ordered_keys(spec, partitions, &schema)?;

    let mut writer =
        SSTableWriter::with_expected_partitions(spec.out.clone(), 1, &schema, partitions as usize)?;
    let mut rows_written: u64 = 0;
    let mut cells_written: u64 = 0;
    for (key, p) in keyed.iter() {
        let mut mutations = Vec::with_capacity(spec.rows_per_partition as usize);
        for r in 0..spec.rows_per_partition {
            let global_row = p * spec.rows_per_partition + r;
            let mutation = row_mutation(
                spec.seed,
                *p,
                r,
                global_row,
                spec.nulls,
                spec.rows_per_partition,
            );
            // COUNTED from the mutation actually built, so the reported cell
            // census can never disagree with what the writer received.
            cells_written += mutation.operations.len() as u64;
            mutations.push(mutation);
        }
        rows_written += mutations.len() as u64;
        writer.write_partition(key.clone(), mutations)?;
    }
    // Nine non-key columns per row is the full census; whatever was not written
    // was dropped by the plan.
    let cells_absent = rows_written
        .saturating_mul(NON_KEY_COLUMNS)
        .saturating_sub(cells_written);
    let info = writer.finish().await?;

    if rows_written != spec.rows {
        return Err(format!(
            "asserted row count failed: wrote {rows_written}, planned {}",
            spec.rows
        )
        .into());
    }
    if info.partition_count as u64 != partitions {
        return Err(format!(
            "asserted partition count failed: writer reported {}, planned {partitions}",
            info.partition_count
        )
        .into());
    }
    if info.compression_info_path.is_some() {
        return Err(
            "a CompressionInfo.db was emitted — the production write surface is \
                    UNCOMPRESSED-ONLY (issue #1406)"
                .into(),
        );
    }

    // The DDL travels WITH the fixture so every consumer reads the exact schema it
    // was written from (no ambient schema lookup, no inference — issue #28).
    std::fs::write(spec.out.join("ws0-events.cql"), format!("{DDL}\n"))?;

    if std::fs::read_dir(&table_dir)?.flatten().any(|e| {
        e.file_name()
            .to_string_lossy()
            .ends_with("CompressionInfo.db")
    }) {
        return Err(format!(
            "a CompressionInfo.db exists in {} — the fixture must be uncompressed (#1406)",
            table_dir.display()
        )
        .into());
    }
    let data_db_bytes = std::fs::metadata(&info.data_path)?.len();
    if data_db_bytes == 0 {
        return Err("Data.db is empty — refusing to report a vacuous fixture identity".into());
    }

    // A plan that claims nulls must have produced some: a rule that silently
    // matched nothing (a renamed column, a stride that missed) would leave the
    // fixture null-free while every consumer believed otherwise.
    if spec.nulls != NullPlan::None && cells_absent == 0 {
        return Err(format!(
            "null plan {:?} dropped ZERO cells over {rows_written} rows — the plan \
             matched nothing, so the fixture carries no validity-bitmap content at all",
            spec.nulls
        )
        .into());
    }
    if spec.nulls == NullPlan::None && cells_absent != 0 {
        return Err(format!(
            "NullPlan::None dropped {cells_absent} cells — the default plan must \
             write every non-key cell, or an existing pinned digest moved silently"
        )
        .into());
    }

    Ok(FixtureIdentity {
        rows: rows_written,
        partitions: info.partition_count as u64,
        cells_written,
        cells_absent,
        data_db_bytes,
        compression_info_present: false,
    })
}

/// Build every partition's `DecoratedKey` and sort by (Murmur3 token, key bytes).
///
/// Token order is a HARD writer precondition. Row CONTENT does not depend on this
/// order (it is a pure function of `(seed, p, r)`), so sorting cannot change the
/// fixture's logical content — only the physical partition order.
fn token_ordered_keys(
    spec: &CorpusSpec,
    partitions: u64,
    schema: &TableSchema,
) -> GenResult<Vec<(DecoratedKey, u64)>> {
    let mut keyed: Vec<(DecoratedKey, u64)> = Vec::with_capacity(partitions as usize);
    for p in 0..partitions {
        // The decorated key is a function of the PARTITION KEY alone, and no plan
        // can make a key component absent, so `spec.nulls` cannot change it — it
        // is threaded through only because it is part of the row's identity.
        let probe = row_mutation(spec.seed, p, 0, 0, spec.nulls, spec.rows_per_partition);
        keyed.push((probe.decorated_key(schema)?, p));
    }
    keyed.sort_by(|a, b| {
        a.0.token
            .cmp(&b.0.token)
            .then_with(|| a.0.key.cmp(&b.0.key))
    });
    // A duplicate token would be rejected by the writer's strict ordering check;
    // detect it here with an actionable message rather than as an opaque write
    // failure.
    for w in keyed.windows(2) {
        if w[0].0.token == w[1].0.token {
            return Err(format!(
                "Murmur3 token collision between partitions {} and {} (token {}) — \
                 pick a different seed or partition count",
                w[0].1, w[1].1, w[0].0.token
            )
            .into());
        }
    }
    Ok(keyed)
}

/// Whether `dir` holds at least one `*-Data.db`. Used by every consumer to fail
/// closed instead of scanning an empty corpus and reporting 0 rows as a pass.
pub fn has_data_db(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .map(|d| {
            d.flatten()
                .any(|e| e.file_name().to_string_lossy().ends_with("-Data.db"))
        })
        .unwrap_or(false)
}
