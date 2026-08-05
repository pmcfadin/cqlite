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

use std::path::{Path, PathBuf};
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

/// Where this run will write an identity, for the aliasing check below.
fn identity_write_targets(cli: &Cli) -> Vec<PathBuf> {
    let mut targets = vec![cli.out.join("corpus-identity.json")];
    if let Some(p) = cli.identity_out.as_ref() {
        targets.push(p.clone());
    }
    targets
}

/// Compare two paths for the "same file" relation as robustly as this can be done
/// WITHOUT the file existing.
///
/// `canonicalize` is tried first and is authoritative when it succeeds (it resolves
/// symlinks, `..` and duplicate separators). It fails for a path that does not exist yet —
/// which `--identity-out` typically is — so the fallback canonicalizes the PARENT
/// directory (which does exist, or the write would fail anyway) and compares that against
/// the file name. A lexical comparison alone would miss `./x` vs `x`, and treating a
/// failed canonicalize as "different" would be the permissive branch on an unmeasured
/// state — the exact shape #3272 exists to remove — so an unresolvable parent falls back
/// to the lexical comparison rather than to `false`.
///
/// # What `canonicalize` does NOT see (#3272 review round 3 nit)
///
/// Two aliases it cannot resolve, both of which reach the same bytes by a different path:
///
/// * a **HARDLINK**. Two directory entries, one inode; `canonicalize` resolves symlinks
///   and returns each name unchanged. So `--verify-against prior.json` hardlinked to
///   `<out>/corpus-identity.json` compares unequal.
/// * a **CASE-INSENSITIVE filesystem** (APFS by default, NTFS). `<out>/CORPUS-IDENTITY.JSON`
///   and `<out>/corpus-identity.json` are ONE file on macOS and two strings here.
///
/// [`same_file`] closes both by comparing the FILE IDENTITY (device + inode) when both paths
/// exist, which is what "the same file" actually means. Reading the prior BEFORE generation
/// already makes the COMPARISON honest whatever the paths are — that is #3272 R5 and it
/// stands — but it does not save the operator's recorded artifact from being truncated by
/// `identity.write_json`, which is the half this closes.
fn same_path(a: &Path, b: &Path) -> bool {
    if same_file(a, b) {
        return true;
    }
    if let (Ok(ra), Ok(rb)) = (a.canonicalize(), b.canonicalize()) {
        return ra == rb;
    }
    let resolve = |p: &Path| -> PathBuf {
        match (p.parent(), p.file_name()) {
            (Some(parent), Some(name)) => match parent.canonicalize() {
                Ok(real) => real.join(name),
                Err(_) => p.to_path_buf(),
            },
            _ => p.to_path_buf(),
        }
    };
    resolve(a) == resolve(b)
}

/// Do `a` and `b` name the SAME FILE — one inode reached by two names?
///
/// The authoritative test, and the only one that sees a HARDLINK or a case-insensitive
/// spelling (#3272 review round 3 nit). Both paths must EXIST; a path that does not exist
/// is not an alias of anything, so `false` here is a measured answer rather than a
/// permissive default — and `same_path`'s canonicalize/lexical fallbacks handle the
/// not-yet-created `--identity-out` case that this deliberately cannot.
///
/// Uses `std::fs::metadata` (which FOLLOWS symlinks, so a symlink to the target is caught
/// here too) and the Unix `dev`+`ino` pair. On a non-Unix target the pair is unavailable,
/// so this answers `false` and `same_path`'s path comparison is the whole check — stated
/// rather than silently degraded, and honest: the rig runs on Linux.
fn same_file(a: &Path, b: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        match (std::fs::metadata(a), std::fs::metadata(b)) {
            (Ok(ma), Ok(mb)) => ma.dev() == mb.dev() && ma.ino() == mb.ino(),
            _ => false,
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (a, b);
        false
    }
}

/// The PRIOR identity `--verify-against` names, read BEFORE anything is generated.
///
/// # Why this cannot happen after generation (issue #3272 review R5)
///
/// It used to. The generated identity was written to `<out>/corpus-identity.json` (and to
/// `--identity-out`) and only THEN was `--verify-against` read — so if the verification
/// path aliased either output, the "prior" that was read back was the identity THIS RUN
/// had just written. `diff` then compared the new identity against itself and reported
///
///     determinism:    PASS — reproduced <path> exactly
///
/// having reproduced nothing. That is a CIRCULAR SELF-COMPARISON presented as the
/// determinism check, and the determinism check is the single thing that makes every
/// figure measured against this corpus comparable to a recorded one.
///
/// Two independent closures, because either alone leaves a hole:
///
/// * READ FIRST. The prior is loaded and deserialized before `generate()` runs, so what
///   the comparison sees cannot be a product of this run whatever the paths are. This
///   also fails FAST — an unreadable or malformed prior is reported before minutes of
///   generation rather than after.
/// * REFUSE THE ALIAS. Reading first makes the comparison honest, but a verification path
///   that aliases an output is still a mistake worth naming: the operator asked to compare
///   against a file this run is about to overwrite, so the artifact they wanted to keep
///   would be destroyed and a re-run would compare against the new bytes. Refused with
///   the remedy, rather than silently doing something defensible.
fn load_prior_identity(cli: &Cli) -> GenResult<Option<(PathBuf, CorpusIdentity)>> {
    let Some(prior_path) = cli.verify_against.as_ref() else {
        return Ok(None);
    };
    for target in identity_write_targets(cli) {
        if same_path(prior_path, &target) {
            return Err(format!(
                "--verify-against {} is the SAME FILE this run writes its own identity to \
                 ({}). Comparing a generated identity against itself is not a determinism \
                 check — it is a circular self-comparison that reports `determinism: PASS` \
                 having reproduced nothing, and it would also DESTROY the recorded artifact \
                 the comparison was supposed to be against. Point --verify-against at a \
                 committed record (e.g. \
                 docs/reports/ws0-3096-artifacts/corpus-identity.json), or generate into a \
                 different --out (issue #3272).",
                prior_path.display(),
                target.display()
            )
            .into());
        }
    }
    // READ AND DESERIALIZE NOW, before a single byte of corpus exists.
    let prior_json = std::fs::read_to_string(prior_path).map_err(|e| {
        format!(
            "--verify-against {} could not be read: {e}. The prior identity is read BEFORE \
             generation so the comparison cannot be against this run's own output, and so \
             an unusable prior fails in seconds rather than after a multi-GB write.",
            prior_path.display()
        )
    })?;
    let prior: CorpusIdentity = serde_json::from_str(&prior_json).map_err(|e| {
        format!(
            "--verify-against {} is not a corpus identity: {e}",
            prior_path.display()
        )
    })?;
    Ok(Some((prior_path.clone(), prior)))
}

async fn run(cli: Cli) -> GenResult<ExitCode> {
    // BEFORE generation, and before any identity is written (issue #3272 review R5).
    let prior = load_prior_identity(&cli)?;

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

    // ALWAYS beside the corpus: every consumer (the measurement driver, the
    // digest oracle's big-corpus case) reads the identity from the corpus root to
    // learn the row count it must not be vacuous against. `--identity-out` writes
    // an ADDITIONAL copy (the in-tree record), it does not move this one.
    let corpus_identity_path = spec.out.join("corpus-identity.json");
    identity.write_json(&corpus_identity_path)?;
    let identity_path = cli.identity_out.unwrap_or(corpus_identity_path.clone());
    if identity_path != corpus_identity_path {
        identity.write_json(&identity_path)?;
    }

    println!("corpus:         {}", spec.table_dir().display());
    println!(
        "ddl:            {}",
        spec.out.join("ws0-events.cql").display()
    );
    println!("identity:       {}", corpus_identity_path.display());
    println!("identity (rec): {}", identity_path.display());
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

    // `prior` was read BEFORE `generate()` ran, so it cannot be this run's own output
    // whatever the paths are (issue #3272 review R5).
    if let Some((prior_path, prior)) = prior {
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
