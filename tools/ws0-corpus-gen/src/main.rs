//! `ws0-corpus-gen` — the committed, deterministic `ws0.events` PERFORMANCE
//! FIXTURE generator (issue #3096, requirement R4).
//!
//! Drives the PRODUCTION `cqlite_core` `SSTableWriter` (never a test helper) and
//! records the corpus's own identity in-tree. Read `src/lib.rs` and `README.md`
//! before treating anything it produces as evidence: the corpus is CQLite-written
//! and CQLite-read and is therefore a PERFORMANCE FIXTURE ONLY, never a
//! correctness oracle for on-disk framing (issue #3042).
//!
//! ```text
//! ws0-corpus-gen --out /data/ws0-3096 --rows 4000000 --rows-per-partition 100
//! ws0-corpus-gen --out /data/ws0-3096-b --verify-against docs/reports/ws0-3096-artifacts/corpus-identity.json
//! ```

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

use cqlite_core::storage::sstable::writer::SSTableWriter;
use ws0_corpus_gen::identity::{
    scan_components, sha256_file, Component, CorpusIdentity, DIFFERS_FROM_PRIOR_CORPUS,
    NOT_A_CORRECTNESS_ORACLE,
};
use ws0_corpus_gen::rows::row_mutation;
use ws0_corpus_gen::schema::{ws0_events_schema, COLUMNS, DDL, KEYSPACE, TABLE};

/// The recorded seed. Changing it changes the corpus and therefore its identity;
/// the default is what the committed `corpus-identity.json` was generated from.
const DEFAULT_SEED: u64 = 30_960_001;

#[derive(Parser, Debug)]
#[command(
    name = "ws0-corpus-gen",
    about = "Deterministic ws0.events PERFORMANCE FIXTURE generator (issue #3096) — never a correctness oracle (#3042)"
)]
struct Cli {
    /// Corpus root. The SSTable lands at `<out>/ws0/events/`, which is exactly the
    /// write-engine layout both `cqlite-flight --data-dir` and the bare-scan arm
    /// resolve. Corpus binaries are NOT committed — write to scratch (e.g. /data).
    #[arg(long)]
    out: PathBuf,

    /// Total rows. Default 4,000,000 (the WS0 shape); lower it for a cheap smoke run.
    #[arg(long, default_value_t = 4_000_000)]
    rows: u64,

    /// Rows per partition. `rows` must be an exact multiple of this.
    #[arg(long, default_value_t = 100)]
    rows_per_partition: u64,

    /// Generation seed. Recorded in the identity artifact.
    #[arg(long, default_value_t = DEFAULT_SEED)]
    seed: u64,

    /// Where to write the corpus identity JSON. Defaults to the corpus root's
    /// `corpus-identity.json`; point it in-tree to record a corpus for the repo.
    #[arg(long)]
    identity_out: Option<PathBuf>,

    /// Compare the generated identity against a previously recorded one and exit
    /// non-zero on ANY divergence. This is the determinism check.
    #[arg(long)]
    verify_against: Option<PathBuf>,

    /// Refuse to overwrite a non-empty `<out>/ws0/events/`. Off by default so a
    /// re-run is cheap; the generator always removes a stale table dir first.
    #[arg(long)]
    no_clobber: bool,

    /// Progress line every N partitions (0 = silent).
    #[arg(long, default_value_t = 2_000)]
    progress_every: u64,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("ws0-corpus-gen: ERROR: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<ExitCode, Box<dyn std::error::Error>> {
    if cli.rows == 0 || cli.rows_per_partition == 0 {
        return Err(
            "rows and rows-per-partition must both be > 0 — a 0-row corpus \
                    would let every downstream measurement pass vacuously"
                .into(),
        );
    }
    if cli.rows % cli.rows_per_partition != 0 {
        return Err(format!(
            "rows ({}) must be an exact multiple of rows-per-partition ({})",
            cli.rows, cli.rows_per_partition
        )
        .into());
    }
    let partitions = cli.rows / cli.rows_per_partition;

    let schema = ws0_events_schema();
    let table_dir = cli.out.join(KEYSPACE).join(TABLE);
    if table_dir.exists() {
        let occupied = std::fs::read_dir(&table_dir)?.next().is_some();
        if occupied && cli.no_clobber {
            return Err(format!(
                "{} is non-empty and --no-clobber was given",
                table_dir.display()
            )
            .into());
        }
        std::fs::remove_dir_all(&table_dir)?;
    }
    std::fs::create_dir_all(&cli.out)?;

    eprintln!(
        "ws0-corpus-gen: seed={} rows={} partitions={} rows/partition={} out={}",
        cli.seed,
        cli.rows,
        partitions,
        cli.rows_per_partition,
        cli.out.display()
    );

    // ---- Murmur3 token order (a writer precondition, validated by the writer) --
    //
    // Build every partition's DecoratedKey first, then sort by (token, key bytes).
    // The row CONTENT does not depend on this order (it is a pure function of
    // (seed, p, r)), so sorting here cannot change the corpus's logical content —
    // only the physical partition order the writer requires.
    let mut keyed: Vec<(cqlite_core::storage::write_engine::DecoratedKey, u64)> =
        Vec::with_capacity(partitions as usize);
    for p in 0..partitions {
        let probe = row_mutation(cli.seed, p, 0, 0);
        keyed.push((probe.decorated_key(&schema)?, p));
    }
    keyed.sort_by(|a, b| {
        a.0.token
            .cmp(&b.0.token)
            .then_with(|| a.0.key.cmp(&b.0.key))
    });
    // Strictly-increasing tokens are a hard writer precondition; a collision is
    // astronomically unlikely over 64-bit Murmur3 but is detected, not assumed.
    for w in keyed.windows(2) {
        if w[0].0.token == w[1].0.token {
            return Err(format!(
                "Murmur3 token collision between partitions {} and {} (token {}) — \
                 pick a different --seed or partition count",
                w[0].1, w[1].1, w[0].0.token
            )
            .into());
        }
    }

    // ---- Write, through the PRODUCTION writer ---------------------------------
    let mut writer =
        SSTableWriter::with_expected_partitions(cli.out.clone(), 1, &schema, partitions as usize)?;
    let mut rows_written: u64 = 0;
    let start = std::time::Instant::now();
    for (i, (key, p)) in keyed.iter().enumerate() {
        let mut mutations = Vec::with_capacity(cli.rows_per_partition as usize);
        for r in 0..cli.rows_per_partition {
            let global_row = p * cli.rows_per_partition + r;
            mutations.push(row_mutation(cli.seed, *p, r, global_row));
        }
        rows_written += mutations.len() as u64;
        writer.write_partition(key.clone(), mutations)?;
        if cli.progress_every > 0 && (i as u64 + 1) % cli.progress_every == 0 {
            eprintln!(
                "  {} / {} partitions ({} rows) in {:.1}s",
                i + 1,
                partitions,
                rows_written,
                start.elapsed().as_secs_f64()
            );
        }
    }
    let info = writer.finish().await?;
    eprintln!(
        "ws0-corpus-gen: wrote {} rows in {} partitions in {:.1}s",
        rows_written,
        info.partition_count,
        start.elapsed().as_secs_f64()
    );

    // ---- Fail closed on every anti-vacuity condition --------------------------
    if rows_written != cli.rows {
        return Err(format!(
            "asserted row count failed: wrote {rows_written}, planned {}",
            cli.rows
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

    // ---- Record the identity ---------------------------------------------------
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
    let identity = CorpusIdentity {
        issue: "#3096".to_string(),
        seed: cli.seed,
        table: format!("{KEYSPACE}.{TABLE}"),
        rows: rows_written,
        partitions: info.partition_count as u64,
        rows_per_partition: cli.rows_per_partition,
        cells_per_row: COLUMNS.len(),
        data_db_bytes: data_bytes,
        data_db_sha256: data_sha,
        bytes_per_row: data_bytes as f64 / rows_written as f64,
        total_component_bytes: components.values().map(|c: &Component| c.bytes).sum(),
        components,
        compression_info_present: false,
        not_a_correctness_oracle: NOT_A_CORRECTNESS_ORACLE.to_string(),
        differs_from_prior_corpus: DIFFERS_FROM_PRIOR_CORPUS.to_string(),
    };

    // The DDL travels WITH the corpus so both measurement arms read the exact
    // schema it was written from (no ambient schema lookup).
    std::fs::write(cli.out.join("ws0-events.cql"), format!("{DDL}\n"))?;

    let identity_path = cli
        .identity_out
        .unwrap_or_else(|| cli.out.join("corpus-identity.json"));
    identity.write_json(&identity_path)?;

    println!("corpus:        {}", table_dir.display());
    println!(
        "ddl:           {}",
        cli.out.join("ws0-events.cql").display()
    );
    println!("identity:      {}", identity_path.display());
    println!("rows:          {}", identity.rows);
    println!("partitions:    {}", identity.partitions);
    println!("cells/row:     {}", identity.cells_per_row);
    println!("Data.db bytes: {}", identity.data_db_bytes);
    println!("bytes/row:     {:.2}", identity.bytes_per_row);
    println!("Data.db sha256:{}", identity.data_db_sha256);
    println!(
        "components:    {} (no CompressionInfo.db)",
        identity.components.len()
    );

    // ---- Optional determinism verification -------------------------------------
    if let Some(prior_path) = cli.verify_against {
        let prior_json = std::fs::read_to_string(&prior_path)?;
        let prior: CorpusIdentity = serde_json::from_str(&prior_json)?;
        let diffs = identity.diff(&prior);
        if diffs.is_empty() {
            println!(
                "determinism:   PASS — reproduced {} exactly",
                prior_path.display()
            );
        } else {
            eprintln!(
                "determinism:   FAIL against {} ({} divergence(s)):",
                prior_path.display(),
                diffs.len()
            );
            for d in &diffs {
                eprintln!("  - {d}");
            }
            return Ok(ExitCode::FAILURE);
        }
    }

    Ok(ExitCode::SUCCESS)
}
