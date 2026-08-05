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
use ws0_corpus_gen::identity::{CorpusIdentity, IdentityVerdict};

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

/// Refuse an `--identity-out` that would OVERWRITE A GENERATED CORPUS INPUT (#3272 R3).
///
/// # The finding
///
/// `--identity-out` was written verbatim. Nothing stopped it naming a path INSIDE the
/// generated table directory (`<out>/ws0/events/nb-1-big-Index.db`) or the emitted DDL
/// (`<out>/ws0-events.cql`), so `identity.write_json` REPLACED a generated input **after its
/// digest had been recorded in that very identity** — and generation still exited 0. The
/// artifact then describes a corpus that no longer exists on disk: the identity says
/// `Index.db` is N bytes with digest D, while `Index.db` is now the identity JSON.
///
/// Both consumers are then wrong in the confident direction. The measurement driver verifies
/// the corpus against this identity and finds a component that disagrees (a refusal, at least
/// loud) — but a *reporting* path handed the identity alone would cite recorded digests for
/// bytes that were overwritten by the record of them.
///
/// # The mechanism, and why it is the SAME one
///
/// This reuses [`same_path`]/[`same_file`] — round 5's alias detection — rather than adding a
/// second comparison. That matters for more than duplication: `same_file` is the only test
/// that sees a **HARDLINK** or a **case-insensitive spelling** (`<out>/WS0-EVENTS.CQL` and
/// `<out>/ws0-events.cql` are ONE file on APFS), and a hand-written second mechanism would
/// re-acquire exactly the holes round 5 closed.
///
/// Two questions, because one does not imply the other:
///
/// * is the identity path INSIDE the generated table directory? Component names are not known
///   until the writer has run, so a name comparison is impossible pre-generation — but
///   CONTAINMENT is decidable now, and it is the stronger question anyway: nothing may be
///   written into that directory, whatever it would be called.
/// * does it ALIAS a named generated input (the DDL, the ticket template, the corpus root's
///   own `corpus-identity.json`)? Those paths ARE known, so they are compared by file identity.
///
/// Checked BEFORE generation, like `load_prior_identity`: an operator who mistyped a path
/// learns in milliseconds instead of after a multi-GB, minutes-long write.
fn reject_identity_out_aliasing_inputs(cli: &Cli) -> GenResult<()> {
    let Some(identity_out) = cli.identity_out.as_ref() else {
        return Ok(());
    };
    // 1. CONTAINMENT in the generated table directory. Resolved through the deepest EXISTING
    //    ancestor, so `<out>/ws0/events/x.json` is caught before `<out>/ws0/events` exists.
    let table_dir = cli.out.join("ws0").join("events");
    if path_is_inside(identity_out, &table_dir) {
        return Err(format!(
            "--identity-out {} resolves INSIDE the generated table directory ({}). Writing the \
             identity there REPLACES a generated SSTable component after its size and digest \
             have been recorded in that same identity, so the artifact would describe a corpus \
             that no longer exists on disk — and generation would still exit 0. Write the \
             identity outside the corpus (e.g. an in-tree \
             docs/reports/ws0-3096-artifacts/corpus-identity.json); the copy beside the data is \
             written automatically (issue #3272 R3).",
            identity_out.display(),
            table_dir.display()
        )
        .into());
    }
    // 2. ALIASING a NAMED generated input. `same_path` sees hardlinks and case-insensitive
    //    spellings (via `same_file`), which a string comparison cannot.
    for (name, path) in generated_input_paths(cli) {
        if same_path(identity_out, &path) {
            return Err(format!(
                "--identity-out {} is the SAME FILE as {} ({}), which this run GENERATES. \
                 Writing the identity over it would destroy a measurement input after its \
                 digest was recorded, leaving both arms reading something other than the \
                 corpus the identity describes — and generation would still exit 0. Name a \
                 path outside the corpus directory (issue #3272 R3).",
                identity_out.display(),
                name,
                path.display()
            )
            .into());
        }
    }
    Ok(())
}

/// The generated files, by name, that `--identity-out` must not alias.
///
/// `corpus-identity.json` is deliberately EXCLUDED: `--identity-out` naming it is the
/// documented no-op (the code writes that path unconditionally and skips the second write when
/// they are equal), not a mistake.
fn generated_input_paths(cli: &Cli) -> Vec<(&'static str, PathBuf)> {
    vec![
        (
            "the emitted DDL ws0-events.cql",
            cli.out.join("ws0-events.cql"),
        ),
        (
            "the Flight ticket template ticket-template.json",
            cli.out.join("ticket-template.json"),
        ),
    ]
}

/// Is `path` inside `dir`, deciding it WITHOUT either having to exist yet?
///
/// Both sides are resolved through their deepest EXISTING ancestor (so symlinks and `..` in the
/// real part are resolved by the OS) with the not-yet-existing tail appended lexically and
/// normalized for `.`/`..`. That is what makes the answer available BEFORE generation, which is
/// the whole point: the table directory does not exist when this runs.
///
/// Errs toward INSIDE on an unresolvable path: an unmeasured state must not take the permissive
/// branch (#3272). The cost of a false "inside" is a refusal an operator can see and correct;
/// the cost of a false "outside" is a silently destroyed measurement input.
fn path_is_inside(path: &Path, dir: &Path) -> bool {
    let (Some(p), Some(d)) = (resolve_best_effort(path), resolve_best_effort(dir)) else {
        return true;
    };
    p.starts_with(&d)
}

/// A path resolved as far as the filesystem can, with the rest normalized lexically.
///
/// `None` only when there is nothing to resolve against at all, which callers treat as the
/// fail-closed case rather than as "not related".
fn resolve_best_effort(path: &Path) -> Option<PathBuf> {
    let mut existing = path.to_path_buf();
    let mut tail: Vec<std::ffi::OsString> = Vec::new();
    loop {
        if let Ok(real) = existing.canonicalize() {
            let mut out = real;
            for part in tail.iter().rev() {
                if part == "." {
                    continue;
                }
                if part == ".." {
                    out.pop();
                    continue;
                }
                out.push(part);
            }
            return Some(out);
        }
        match (existing.parent(), existing.file_name()) {
            (Some(parent), Some(name)) => {
                tail.push(name.to_os_string());
                existing = parent.to_path_buf();
            }
            // A relative path with no resolvable ancestor: normalize lexically against CWD so
            // the comparison is still made rather than abandoned.
            _ => {
                let mut out = std::env::current_dir().ok()?;
                for part in tail.iter().rev() {
                    if part == "." {
                        continue;
                    }
                    if part == ".." {
                        out.pop();
                        continue;
                    }
                    out.push(part);
                }
                return Some(out);
            }
        }
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
    // ...and an `--identity-out` that would OVERWRITE A GENERATED INPUT is refused here too,
    // for the same reason: milliseconds instead of after a multi-GB write (#3272 R3).
    reject_identity_out_aliasing_inputs(&cli)?;

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
        let cmp = identity.compare(&prior);
        // THREE outcomes, not two (#3272 review round 7, F1). `PARTIAL` exists because a
        // prior recorded before a field was pinned cannot be compared on that field, and a
        // comparison that could not see a field must not print `PASS`: an unverified field
        // silently folded into "matched" is the fail-open shape this issue exists to remove.
        // It exits NON-ZERO for the same reason — a caller scripting `--verify-against` reads
        // the exit code, and a zero exit IS a pass claim however the text is worded.
        match cmp.verdict() {
            IdentityVerdict::Reproduced => {
                println!(
                    "determinism:    PASS — reproduced {} exactly (every recorded field compared)",
                    prior_path.display()
                );
            }
            IdentityVerdict::PartialUnverified => {
                eprintln!(
                    "determinism:    PARTIAL against {} — every field that COULD be compared \
                     agreed, but {} field(s) are UNVERIFIED because the recorded identity does \
                     not carry them. This is NOT a pass:",
                    prior_path.display(),
                    cmp.unverified.len()
                );
                for u in &cmp.unverified {
                    eprintln!("  ? {u}");
                }
                eprintln!(
                    "                A field the recorded identity does not carry was not \
                     checked, and a check that did not run prints exactly like one that passed \
                     (#3272). Exiting non-zero so a scripted caller cannot read this as \
                     reproduction."
                );
                return Ok(ExitCode::FAILURE);
            }
            IdentityVerdict::Diverged => {
                eprintln!(
                    "determinism:    FAIL against {} ({} divergence(s)):",
                    prior_path.display(),
                    cmp.divergences.len()
                );
                for d in &cmp.divergences {
                    eprintln!("  - {d}");
                }
                // An unverified field is reported EVEN on the FAIL path: the operator is about
                // to act on this output, and "these fields also could not be compared" changes
                // what the divergence list means.
                for u in &cmp.unverified {
                    eprintln!("  ? {u}");
                }
                return Ok(ExitCode::FAILURE);
            }
        }
    }

    Ok(ExitCode::SUCCESS)
}
