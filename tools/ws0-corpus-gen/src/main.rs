//! `ws0-corpus-gen` — the committed, deterministic `ws0.events` PERFORMANCE
//! FIXTURE generator (issue #3096, requirement R4).
//!
//! A thin CLI over [`ws0_corpus_gen::generate::generate`], which drives the
//! PRODUCTION `cqlite_core` `SSTableWriter` (never a test helper). Read
//! `src/lib.rs` and `README.md` before treating anything it produces as evidence:
//! the corpus is CQLite-written and CQLite-read and is therefore a PERFORMANCE
//! FIXTURE ONLY, never a correctness oracle for on-disk framing (issue #3042).
//!
//! ```text
//! ws0-corpus-gen --out /data/ws0-3096 --rows 4000000 --rows-per-partition 100
//! ws0-corpus-gen --out /data/ws0-3096-b --verify-against docs/reports/ws0-3096-artifacts/corpus-identity.json
//! ```

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

use ws0_corpus_gen::generate::{generate, CorpusSpec, GenResult, DEFAULT_SEED};
use ws0_corpus_gen::identity::CorpusIdentity;

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

    /// Refuse to overwrite a non-empty `<out>/ws0/events/`.
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

async fn run(cli: Cli) -> GenResult<ExitCode> {
    let spec = CorpusSpec {
        out: cli.out.clone(),
        rows: cli.rows,
        rows_per_partition: cli.rows_per_partition,
        seed: cli.seed,
        no_clobber: cli.no_clobber,
        progress_every: cli.progress_every,
    };

    eprintln!(
        "ws0-corpus-gen: seed={} rows={} rows/partition={} out={}",
        spec.seed,
        spec.rows,
        spec.rows_per_partition,
        spec.out.display()
    );
    let start = std::time::Instant::now();
    let identity = generate(&spec).await?;
    eprintln!(
        "ws0-corpus-gen: wrote {} rows in {} partitions in {:.1}s",
        identity.rows,
        identity.partitions,
        start.elapsed().as_secs_f64()
    );

    let identity_path = cli
        .identity_out
        .unwrap_or_else(|| spec.out.join("corpus-identity.json"));
    identity.write_json(&identity_path)?;

    println!("corpus:         {}", spec.table_dir().display());
    println!(
        "ddl:            {}",
        spec.out.join("ws0-events.cql").display()
    );
    println!("identity:       {}", identity_path.display());
    println!("rows:           {}", identity.rows);
    println!("partitions:     {}", identity.partitions);
    println!("cells/row:      {}", identity.cells_per_row);
    println!("Data.db bytes:  {}", identity.data_db_bytes);
    println!("bytes/row:      {:.2}", identity.bytes_per_row);
    println!("Data.db sha256: {}", identity.data_db_sha256);
    println!(
        "components:     {} (no CompressionInfo.db)",
        identity.components.len()
    );

    if let Some(prior_path) = cli.verify_against {
        let prior_json = std::fs::read_to_string(&prior_path)?;
        let prior: CorpusIdentity = serde_json::from_str(&prior_json)?;
        let diffs = identity.diff(&prior);
        if diffs.is_empty() {
            println!(
                "determinism:    PASS — reproduced {} exactly",
                prior_path.display()
            );
        } else {
            eprintln!(
                "determinism:    FAIL against {} ({} divergence(s)):",
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
