//! Corpus generation, as a LIBRARY function.
//!
//! The binary is a thin CLI over [`generate`], and the in-repo Arrow-buffer digest
//! oracle (`cqlite-flight/tests/issue_3096_arrow_buffer_digest.rs`) builds its
//! small CI fixture through the SAME function. One implementation, so the fixture
//! the oracle pins and the corpus the measurement runs over can never be produced
//! by two subtly different code paths.

use std::path::{Path, PathBuf};

use cqlite_core::storage::sstable::writer::SSTableWriter;
use cqlite_core::storage::write_engine::DecoratedKey;

use crate::identity::{
    scan_components, sha256_file, Component, CorpusIdentity, DIFFERS_FROM_PRIOR_CORPUS,
    NOT_A_CORRECTNESS_ORACLE,
};
use crate::rows::row_mutation;
use crate::schema::{ws0_events_schema, COLUMNS, DDL, KEYSPACE, TABLE};

/// The recorded seed. Changing it changes the corpus and therefore its identity;
/// this is what the committed `corpus-identity.json` was generated from.
pub const DEFAULT_SEED: u64 = 30_960_001;

/// Boxed error alias — this is operator tooling, not library code on a hot path.
pub type GenResult<T> = Result<T, Box<dyn std::error::Error>>;

/// What to generate and where.
#[derive(Debug, Clone)]
pub struct CorpusSpec {
    /// Corpus root; the SSTable lands at `<out>/ws0/events/`.
    pub out: PathBuf,
    /// Total rows. Must be an exact multiple of `rows_per_partition`.
    pub rows: u64,
    /// Rows per partition.
    pub rows_per_partition: u64,
    /// Generation seed.
    pub seed: u64,
    /// Refuse to overwrite a non-empty table dir.
    pub no_clobber: bool,
    /// Progress line every N partitions on stderr (0 = silent).
    pub progress_every: u64,
}

impl CorpusSpec {
    /// The full 4,000,000-row WS0 shape (40,000 partitions x 100 rows).
    pub fn full(out: PathBuf) -> Self {
        Self {
            out,
            rows: 4_000_000,
            rows_per_partition: 100,
            seed: DEFAULT_SEED,
            no_clobber: false,
            progress_every: 2_000,
        }
    }

    /// A cheap CI-sized corpus of the SAME shape (same schema, same widths, same
    /// generator) — small enough to build inside a test.
    pub fn small(out: PathBuf, rows: u64) -> Self {
        Self {
            out,
            rows,
            rows_per_partition: 100,
            seed: DEFAULT_SEED,
            no_clobber: false,
            progress_every: 0,
        }
    }

    /// Directory the SSTable components land in.
    pub fn table_dir(&self) -> PathBuf {
        self.out.join(KEYSPACE).join(TABLE)
    }
}

/// Generate the corpus, returning its recorded identity.
///
/// Fails closed on every anti-vacuity condition: a zero/short row count, a
/// partition count the writer did not confirm, an empty `Data.db`, or a
/// `CompressionInfo.db` (the write surface is uncompressed-only, issue #1406).
pub async fn generate(spec: &CorpusSpec) -> GenResult<CorpusIdentity> {
    if spec.rows == 0 || spec.rows_per_partition == 0 {
        return Err(
            "rows and rows-per-partition must both be > 0 — a 0-row corpus \
                    would let every downstream measurement pass vacuously"
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
        let occupied = std::fs::read_dir(&table_dir)?.next().is_some();
        if occupied && spec.no_clobber {
            return Err(format!(
                "{} is non-empty and no_clobber was set",
                table_dir.display()
            )
            .into());
        }
        std::fs::remove_dir_all(&table_dir)?;
    }
    std::fs::create_dir_all(&spec.out)?;

    let keyed = token_ordered_keys(spec, partitions, &schema)?;

    let mut writer =
        SSTableWriter::with_expected_partitions(spec.out.clone(), 1, &schema, partitions as usize)?;
    let mut rows_written: u64 = 0;
    let start = std::time::Instant::now();
    for (i, (key, p)) in keyed.iter().enumerate() {
        let mut mutations = Vec::with_capacity(spec.rows_per_partition as usize);
        for r in 0..spec.rows_per_partition {
            let global_row = p * spec.rows_per_partition + r;
            mutations.push(row_mutation(spec.seed, *p, r, global_row));
        }
        rows_written += mutations.len() as u64;
        writer.write_partition(key.clone(), mutations)?;
        if spec.progress_every > 0 && (i as u64 + 1) % spec.progress_every == 0 {
            eprintln!(
                "  {} / {partitions} partitions ({rows_written} rows) in {:.1}s",
                i + 1,
                start.elapsed().as_secs_f64()
            );
        }
    }
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

    // The DDL travels WITH the corpus so every consumer reads the exact schema it
    // was written from (no ambient schema lookup, no inference — issue #28).
    std::fs::write(spec.out.join("ws0-events.cql"), format!("{DDL}\n"))?;

    let components = scan_components(&table_dir)?;
    if components.keys().any(|n| n.ends_with("CompressionInfo.db")) {
        return Err(format!(
            "a CompressionInfo.db exists in {} — the corpus must be uncompressed (#1406)",
            table_dir.display()
        )
        .into());
    }
    let (data_sha, data_bytes) = sha256_file(&info.data_path)?;
    if data_bytes == 0 {
        return Err("Data.db is empty — refusing to record a vacuous corpus identity".into());
    }

    Ok(CorpusIdentity {
        issue: "#3096".to_string(),
        seed: spec.seed,
        table: format!("{KEYSPACE}.{TABLE}"),
        rows: rows_written,
        partitions: info.partition_count as u64,
        rows_per_partition: spec.rows_per_partition,
        cells_per_row: COLUMNS.len(),
        data_db_bytes: data_bytes,
        data_db_sha256: data_sha,
        bytes_per_row: data_bytes as f64 / rows_written as f64,
        total_component_bytes: components.values().map(|c: &Component| c.bytes).sum(),
        components,
        compression_info_present: false,
        not_a_correctness_oracle: NOT_A_CORRECTNESS_ORACLE.to_string(),
        differs_from_prior_corpus: DIFFERS_FROM_PRIOR_CORPUS.to_string(),
    })
}

/// Build every partition's `DecoratedKey` and sort by (Murmur3 token, key bytes).
///
/// Token order is a HARD writer precondition. Row CONTENT does not depend on this
/// order (it is a pure function of `(seed, p, r)`), so sorting cannot change the
/// corpus's logical content — only the physical partition order.
fn token_ordered_keys(
    spec: &CorpusSpec,
    partitions: u64,
    schema: &cqlite_core::schema::TableSchema,
) -> GenResult<Vec<(DecoratedKey, u64)>> {
    let mut keyed: Vec<(DecoratedKey, u64)> = Vec::with_capacity(partitions as usize);
    for p in 0..partitions {
        let probe = row_mutation(spec.seed, p, 0, 0);
        keyed.push((probe.decorated_key(schema)?, p));
    }
    keyed.sort_by(|a, b| {
        a.0.token
            .cmp(&b.0.token)
            .then_with(|| a.0.key.cmp(&b.0.key))
    });
    // A duplicate token would be rejected by the writer's strict ordering check;
    // detect it here with an actionable message rather than as an opaque write
    // failure 30,000 partitions in.
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Token ordering is STRICTLY increasing and covers every partition exactly
    /// once — the writer precondition, asserted rather than assumed.
    #[test]
    fn keys_are_strictly_token_ordered_and_complete() {
        let spec = CorpusSpec::small(PathBuf::from("/nonexistent"), 500);
        let schema = ws0_events_schema();
        let keyed = token_ordered_keys(&spec, 5, &schema).expect("keys");
        assert_eq!(keyed.len(), 5);
        for w in keyed.windows(2) {
            assert!(
                w[0].0.token < w[1].0.token,
                "tokens must strictly increase: {} then {}",
                w[0].0.token,
                w[1].0.token
            );
        }
        let mut seen: Vec<u64> = keyed.iter().map(|(_, p)| *p).collect();
        seen.sort_unstable();
        assert_eq!(seen, vec![0, 1, 2, 3, 4], "every partition exactly once");
    }

    /// A non-divisible row count is rejected rather than silently truncated.
    #[tokio::test]
    async fn a_non_divisible_row_count_is_rejected() {
        let dir = std::env::temp_dir().join("ws0-gen-reject-test");
        let mut spec = CorpusSpec::small(dir, 150);
        spec.rows_per_partition = 100;
        let err = generate(&spec).await.expect_err("must reject");
        assert!(err.to_string().contains("exact multiple"), "got {err}");
    }

    /// A zero-row request is rejected — a 0-row corpus would let every downstream
    /// measurement pass vacuously.
    #[tokio::test]
    async fn a_zero_row_request_is_rejected() {
        let dir = std::env::temp_dir().join("ws0-gen-zero-test");
        let spec = CorpusSpec::small(dir, 0);
        let err = generate(&spec).await.expect_err("must reject");
        assert!(err.to_string().contains("must both be > 0"), "got {err}");
    }
}
