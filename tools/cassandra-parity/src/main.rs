//! `cassandra-parity` — lint, coverage, and report tooling for the CQLite ↔
//! Apache Cassandra parity manifest (epics #966/#967).
//!
//! No Docker, live Cassandra, or downloaded dataset binaries required: this tool
//! only reads the manifest, the repository tree, and the assessment report.
//!
//! Usage:
//!   cassandra-parity lint     [--manifest PATH]
//!   cassandra-parity coverage [--manifest PATH] [--strict]
//!   cassandra-parity report   [--manifest PATH] [--output PATH] [--check] [--json PATH]

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{bail, Context, Result};

use cassandra_parity::lint::Level;
use cassandra_parity::model::Manifest;
use cassandra_parity::{coverage, enums, lint, report, tier_contract};

const DEFAULT_MANIFEST: &str = "test-data/cassandra-parity-manifest.yml";
const DEFAULT_OUTPUT: &str = "docs/reports/cassandra-test-parity.md";
const DEFAULT_TIER_DOC: &str = "docs/development/parity-ci-tiers.md";
const DEFAULT_SCHEMA: &str = "test-data/cassandra-parity-manifest.schema.json";

const USAGE: &str = "\
cassandra-parity — CQLite ↔ Cassandra parity manifest tooling

USAGE:
  cassandra-parity lint               [--manifest PATH]
  cassandra-parity coverage           [--manifest PATH] [--strict]
  cassandra-parity report             [--manifest PATH] [--output PATH] [--check] [--json PATH]
  cassandra-parity tier-contract-check [--manifest PATH] [--tier-doc PATH] [--schema PATH]
";

struct Args {
    manifest: PathBuf,
    output: PathBuf,
    json: Option<PathBuf>,
    tier_doc: PathBuf,
    schema: PathBuf,
    strict: bool,
    check: bool,
}

fn parse_args(rest: &[String]) -> Result<Args> {
    let mut args = Args {
        manifest: PathBuf::from(DEFAULT_MANIFEST),
        output: PathBuf::from(DEFAULT_OUTPUT),
        json: None,
        tier_doc: PathBuf::from(DEFAULT_TIER_DOC),
        schema: PathBuf::from(DEFAULT_SCHEMA),
        strict: false,
        check: false,
    };
    let mut it = rest.iter();
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--manifest" => args.manifest = PathBuf::from(next_val(&mut it, "--manifest")?),
            "--output" => args.output = PathBuf::from(next_val(&mut it, "--output")?),
            "--json" => args.json = Some(PathBuf::from(next_val(&mut it, "--json")?)),
            "--tier-doc" => args.tier_doc = PathBuf::from(next_val(&mut it, "--tier-doc")?),
            "--schema" => args.schema = PathBuf::from(next_val(&mut it, "--schema")?),
            "--strict" => args.strict = true,
            "--check" => args.check = true,
            other => bail!("unknown argument: {other}\n\n{USAGE}"),
        }
    }
    Ok(args)
}

fn next_val<'a>(it: &mut impl Iterator<Item = &'a String>, flag: &str) -> Result<String> {
    it.next()
        .cloned()
        .with_context(|| format!("{flag} requires a value"))
}

/// Repo root = the parent of the `test-data/` directory holding the manifest.
fn repo_root(manifest: &Path) -> PathBuf {
    let canon = manifest
        .canonicalize()
        .unwrap_or_else(|_| manifest.to_path_buf());
    canon
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."))
}

fn load(manifest: &Path) -> Result<Manifest> {
    let text = std::fs::read_to_string(manifest)
        .with_context(|| format!("reading manifest {}", manifest.display()))?;
    Manifest::from_yaml(&text).with_context(|| format!("parsing manifest {}", manifest.display()))
}

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<ExitCode> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let Some((sub, rest)) = argv.split_first() else {
        eprintln!("{USAGE}");
        return Ok(ExitCode::FAILURE);
    };
    let args = parse_args(rest)?;

    match sub.as_str() {
        "lint" => cmd_lint(&args),
        "coverage" => cmd_coverage(&args),
        "report" => cmd_report(&args),
        "tier-contract-check" => cmd_tier_contract_check(&args),
        "-h" | "--help" | "help" => {
            println!("{USAGE}");
            Ok(ExitCode::SUCCESS)
        }
        other => {
            eprintln!("unknown subcommand: {other}\n\n{USAGE}");
            Ok(ExitCode::FAILURE)
        }
    }
}

fn cmd_lint(args: &Args) -> Result<ExitCode> {
    let m = load(&args.manifest)?;
    let root = repo_root(&args.manifest);
    let findings = lint::lint(&m, Some(&root));
    let errors = findings.iter().filter(|f| f.level == Level::Error).count();
    let warns = findings.iter().filter(|f| f.level == Level::Warn).count();
    for f in &findings {
        let tag = match f.level {
            Level::Error => "ERROR",
            Level::Warn => "warn",
        };
        println!("{tag} [{}] {}: {}", f.id, f.field, f.message);
    }
    if errors == 0 {
        println!(
            "lint: OK — {} scenarios, 0 errors, {warns} warnings",
            m.scenarios.len()
        );
        Ok(ExitCode::SUCCESS)
    } else {
        eprintln!("lint: FAILED — {errors} errors, {warns} warnings");
        Ok(ExitCode::FAILURE)
    }
}

/// Cross-check the documented tier enum (`docs/development/parity-ci-tiers.md`)
/// against `enums::CI_TIER` and the manifest schema, and validate that every
/// manifest `ci.tier` is a documented tier. No Docker/datasets/live Cassandra.
fn cmd_tier_contract_check(args: &Args) -> Result<ExitCode> {
    let doc = std::fs::read_to_string(&args.tier_doc)
        .with_context(|| format!("reading tier doc {}", args.tier_doc.display()))?;
    let schema = std::fs::read_to_string(&args.schema)
        .with_context(|| format!("reading schema {}", args.schema.display()))?;
    let manifest = std::fs::read_to_string(&args.manifest)
        .with_context(|| format!("reading manifest {}", args.manifest.display()))?;

    let report = tier_contract::check(&doc, &schema, enums::CI_TIER, &manifest)
        .context("running tier-contract cross-check")?;

    if report.ok() {
        println!(
            "tier-contract-check: OK — documented enum == code enum == schema enum, \
             all manifest ci.tier values documented"
        );
        Ok(ExitCode::SUCCESS)
    } else {
        eprintln!("{}", report.render());
        eprintln!(
            "tier-contract-check: FAILED — {} enum divergence(s), {} unknown manifest tier(s)",
            report.enum_divergences.len(),
            report.unknown_manifest_tiers.len()
        );
        Ok(ExitCode::FAILURE)
    }
}

fn cmd_coverage(args: &Args) -> Result<ExitCode> {
    let m = load(&args.manifest)?;
    let root = repo_root(&args.manifest);
    let index_path = root.join(&m.cassandra_source.index);
    let index_text = std::fs::read_to_string(&index_path)
        .with_context(|| format!("reading index {}", index_path.display()))?;
    let cov = coverage::analyze(&m, &index_text);
    println!(
        "coverage: {}/{} high-relevance Cassandra files classified",
        cov.high_classified, cov.high_total
    );
    for f in &cov.unclassified_high {
        let tag = if args.strict { "ERROR" } else { "warn" };
        println!("{tag} [unclassified-high] {f}");
    }
    if args.strict && !cov.unclassified_high.is_empty() {
        eprintln!(
            "coverage: FAILED (strict) — {} high-relevance files unclassified",
            cov.unclassified_high.len()
        );
        return Ok(ExitCode::FAILURE);
    }
    Ok(ExitCode::SUCCESS)
}

fn cmd_report(args: &Args) -> Result<ExitCode> {
    let m = load(&args.manifest)?;
    let root = repo_root(&args.manifest);
    // Re-lint before rendering: never publish a report from an invalid manifest.
    let errors = lint::lint(&m, Some(&root))
        .iter()
        .filter(|f| f.level == Level::Error)
        .count();
    if errors > 0 {
        bail!("manifest has {errors} lint errors; run `lint` and fix before reporting");
    }

    let manifest_display = args
        .manifest
        .strip_prefix(&root)
        .unwrap_or(&args.manifest)
        .to_string_lossy()
        .replace('\\', "/");
    let rendered = report::render(&m, &manifest_display);

    if args.check {
        let existing = std::fs::read_to_string(&args.output).unwrap_or_default();
        if existing != rendered {
            eprintln!(
                "report: STALE — {} differs from a fresh render. Regenerate with:\n  cargo run -p cassandra-parity -- report --output {}",
                args.output.display(),
                args.output.display()
            );
            return Ok(ExitCode::FAILURE);
        }
        println!("report: up to date ({})", args.output.display());
        return Ok(ExitCode::SUCCESS);
    }

    if let Some(parent) = args.output.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    std::fs::write(&args.output, &rendered)
        .with_context(|| format!("writing report {}", args.output.display()))?;
    println!("report: wrote {}", args.output.display());

    if let Some(json_path) = &args.json {
        let counts = serde_json::json!({
            "manifest": manifest_display,
            "scenarios": m.scenarios.len(),
            "status": status_counts(&m),
            "evidence": evidence_counts(&m),
        });
        if let Some(parent) = json_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        std::fs::write(json_path, serde_json::to_string_pretty(&counts)?)
            .with_context(|| format!("writing json {}", json_path.display()))?;
        println!("report: wrote {}", json_path.display());
    }
    Ok(ExitCode::SUCCESS)
}

fn status_counts(m: &Manifest) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for status in enums::STATUS {
        let n = m.scenarios.iter().filter(|x| x.status == *status).count();
        map.insert((*status).to_string(), serde_json::json!(n));
    }
    serde_json::Value::Object(map)
}

fn evidence_counts(m: &Manifest) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for ev in enums::EVIDENCE_TYPE {
        let n = m
            .scenarios
            .iter()
            .filter(|x| x.evidence.kind == *ev)
            .count();
        map.insert((*ev).to_string(), serde_json::json!(n));
    }
    serde_json::Value::Object(map)
}
