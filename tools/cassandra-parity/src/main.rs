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

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{bail, Context, Result};

use cassandra_parity::claim_scan::{self, ScanInput};
use cassandra_parity::corpus_audit::{
    self, CorpusInventory, CorruptionFixture, ExpectedInventory, Provenance,
};
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
  cassandra-parity corpus-audit       [--manifest PATH] --corpus DIR [--provenance JSON]
                                      [--checksums FILE] [--expected-inventory FILE]
                                      [--corruption-manifest YML] [--index MD]
";

struct Args {
    manifest: PathBuf,
    output: PathBuf,
    json: Option<PathBuf>,
    tier_doc: PathBuf,
    schema: PathBuf,
    strict: bool,
    check: bool,
    // corpus-audit (issue #1026)
    corpus: Option<PathBuf>,
    provenance: Option<PathBuf>,
    checksums: Option<PathBuf>,
    expected_inventory: Option<PathBuf>,
    corruption_manifest: Option<PathBuf>,
    index: Option<PathBuf>,
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
        corpus: None,
        provenance: None,
        checksums: None,
        expected_inventory: None,
        corruption_manifest: None,
        index: None,
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
            "--corpus" => args.corpus = Some(PathBuf::from(next_val(&mut it, "--corpus")?)),
            "--provenance" => {
                args.provenance = Some(PathBuf::from(next_val(&mut it, "--provenance")?))
            }
            "--checksums" => {
                args.checksums = Some(PathBuf::from(next_val(&mut it, "--checksums")?))
            }
            "--expected-inventory" => {
                args.expected_inventory =
                    Some(PathBuf::from(next_val(&mut it, "--expected-inventory")?))
            }
            "--corruption-manifest" => {
                args.corruption_manifest =
                    Some(PathBuf::from(next_val(&mut it, "--corruption-manifest")?))
            }
            "--index" => args.index = Some(PathBuf::from(next_val(&mut it, "--index")?)),
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
        "corpus-audit" => cmd_corpus_audit(&args),
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
    let mut findings = lint::lint(&m, Some(&root));

    // Claim-scan over release-facing docs (issue #1023): a file that is absent
    // from this checkout is skipped rather than failing, so the lint stays usable
    // from sub-trees; the curated file set lives in `claim_scan::RELEASE_FILES`.
    let texts: Vec<(String, String)> = claim_scan::RELEASE_FILES
        .iter()
        .filter_map(|rel| {
            std::fs::read_to_string(root.join(rel))
                .ok()
                .map(|t| ((*rel).to_string(), t))
        })
        .collect();
    let inputs: Vec<ScanInput<'_>> = texts
        .iter()
        .map(|(p, t)| ScanInput {
            path: p.as_str(),
            text: t.as_str(),
        })
        .collect();
    findings.extend(claim_scan::scan_docs(&m, &inputs));

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

/// Audit the regenerated corpus + run provenance against the manifest
/// (issue #1026). Hard-fails (non-zero exit) and names the offender on any
/// finding — no report-but-pass mode (owner-pinned strictness).
fn cmd_corpus_audit(args: &Args) -> Result<ExitCode> {
    let corpus = args
        .corpus
        .as_ref()
        .context("corpus-audit requires --corpus <regenerated corpus root>")?;
    let m = load(&args.manifest)?;
    let root = repo_root(&args.manifest);

    let index_path = args
        .index
        .clone()
        .unwrap_or_else(|| root.join(&m.cassandra_source.index));
    let index_text = std::fs::read_to_string(&index_path)
        .with_context(|| format!("reading index {}", index_path.display()))?;

    // Regenerated component inventory: every repo-relative file under --corpus.
    let mut inventory = CorpusInventory {
        files: walk_relative(corpus)?,
        checksums: BTreeMap::new(),
    };
    if let Some(p) = &args.checksums {
        inventory.checksums = read_sha256_file(p)?;
    }

    let mut expected = ExpectedInventory {
        components: BTreeMap::new(),
    };
    if let Some(p) = &args.expected_inventory {
        expected.components = read_sha256_file(p)?;
    }

    let provenance = match &args.provenance {
        Some(p) => {
            let text = std::fs::read_to_string(p)
                .with_context(|| format!("reading provenance {}", p.display()))?;
            Some(Provenance::from_json(&text).with_context(|| {
                format!("parsing provenance record {} (expected JSON)", p.display())
            })?)
        }
        None => None,
    };

    let corruption_fixtures = match &args.corruption_manifest {
        Some(p) => read_corruption_fixtures(p)?,
        None => Vec::new(),
    };

    let report = corpus_audit::audit(
        &m,
        &index_text,
        &inventory,
        &expected,
        provenance.as_ref(),
        &corruption_fixtures,
    );

    if report.ok() {
        println!(
            "corpus-audit: OK — {} corpus files, manifest references resolved, provenance matches \
             the manifest pin, all required corruption components covered",
            inventory.files.len()
        );
        Ok(ExitCode::SUCCESS)
    } else {
        eprintln!("{}", report.render());
        eprintln!(
            "corpus-audit: FAILED — {} finding(s)",
            report.findings.len()
        );
        Ok(ExitCode::FAILURE)
    }
}

/// Directory names skipped while walking a corpus: VCS metadata, build output,
/// and the regeneration lane's own report dir. Recursing into `.git/`/`target/`
/// would add tens of thousands of irrelevant paths (and a misleading
/// `inventory.files.len()`) when the tool is pointed at a checkout root via
/// `--corpus .`; `exhaustive-regeneration-report` is the lane's report dir, which
/// lives inside the checkout and holds the audit's own inputs/outputs
/// (`*.sha256`, `provenance.json`, `*.log`, `dataset-asset.tar.gz`) — walking it
/// would re-enumerate them as corpus files. `--corpus .` (repo root) is required
/// because manifest references are repo-root-relative (`test-data/datasets/...`),
/// so we prune the report dir by name rather than scoping the walk (issue #1026,
/// Findings 1 and 2).
const WALK_SKIP_DIRS: &[&str] = &[
    ".git",
    "target",
    ".hg",
    ".svn",
    "node_modules",
    "exhaustive-regeneration-report",
];

/// Recursively collect every file under `dir` as a `/`-separated path relative
/// to `dir` (matching the repo-relative form of manifest references when `dir`
/// is the checkout root). Symlinks are not followed; [`WALK_SKIP_DIRS`] are
/// pruned so the walk is robust regardless of the caller's `--corpus` argument.
fn walk_relative(dir: &Path) -> Result<BTreeSet<String>> {
    let mut out = BTreeSet::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let entries = std::fs::read_dir(&d)
            .with_context(|| format!("reading corpus directory {}", d.display()))?;
        for entry in entries {
            let entry = entry?;
            let ft = entry.file_type()?;
            let path = entry.path();
            if ft.is_dir() {
                let name = entry.file_name();
                if WALK_SKIP_DIRS.iter().any(|s| name == *s) {
                    continue;
                }
                stack.push(path);
            } else if ft.is_file() {
                if let Ok(rel) = path.strip_prefix(dir) {
                    out.insert(rel.to_string_lossy().replace('\\', "/"));
                }
            }
        }
    }
    Ok(out)
}

/// Parse a `sha256sum`-format file into a `path -> sha256` map. Each line is
/// `<hash><sp><mode><path>`, where `<mode>` is a space (text mode) or `*`
/// (binary mode). Parsing is POSITIONAL — the path is taken verbatim after the
/// single separator so runs of spaces/tabs inside a path are preserved (issue
/// #1026, Finding 3); splitting on whitespace would collapse them and mismatch
/// the keys produced by [`walk_relative`]. Blank/comment lines are ignored.
///
/// ASSUMPTION: corpus paths contain neither a backslash nor a newline. GNU
/// coreutils `sha256sum` escapes any filename containing one of those by
/// prefixing the WHOLE line with a single `\` and backslash-escaping the path
/// (`\` -> `\\`, newline -> `\n`). The audit consumes only `test-data/datasets/`
/// component paths, which never contain such characters, so rather than carry a
/// full unescaper we reject a `\`-marked line loudly: folding the leading `\`
/// into the hash token (then `replace('\\','/')`-ing the escapes) would silently
/// corrupt both the hash and the path key (issue #1026, Finding LOW B).
fn read_sha256_file(path: &Path) -> Result<BTreeMap<String, String>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading checksums file {}", path.display()))?;
    let mut map = BTreeMap::new();
    for raw in text.lines() {
        // Trim only the trailing line terminator / whitespace; never touch the
        // interior of the path.
        let line = raw.trim_end();
        if line.is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        // A leading `\` marks a GNU-escaped line (path has a backslash/newline).
        if line.starts_with('\\') {
            bail!(
                "checksums file {} contains a GNU sha256sum-escaped path (line starts with `\\`): \
                 corpus component paths must not contain a backslash or newline",
                path.display()
            );
        }
        // The hash is the leading token up to the first space separator.
        let Some(sep) = line.find(' ') else {
            continue;
        };
        let (sha, rest) = line.split_at(sep);
        // `rest` begins with the separating space; after it is the mode
        // indicator (`*` binary, or a second space for text mode). Drop exactly
        // the separator + one mode char, then take the remainder verbatim.
        let rest = &rest[1..];
        let path_part = rest
            .strip_prefix('*')
            .or_else(|| rest.strip_prefix(' '))
            .unwrap_or(rest);
        let rel = path_part.replace('\\', "/");
        if sha.is_empty() || rel.is_empty() {
            continue;
        }
        map.insert(rel, sha.to_string());
    }
    Ok(map)
}

/// Parse the corruption fixtures (targeted component + on-disk `corrupted_path` +
/// `status`) from a committed `corruption-manifest.yml`. The audit cross-checks
/// each declared `corrupted_path` against the walked corpus inventory, so a
/// fixture declared but absent on disk is caught (spec R4: on-disk reality, not
/// merely a manifest declaration; issue #1026). Each `  - name:` line opens a new
/// fixture block; `corrupted_path`/`status`/`expected_failing_component` are
/// captured within it. A block with no `expected_failing_component` is skipped.
fn read_corruption_fixtures(path: &Path) -> Result<Vec<CorruptionFixture>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading corruption manifest {}", path.display()))?;

    let mut fixtures = Vec::new();
    let mut component: Option<String> = None;
    let mut corrupted_path: Option<String> = None;
    let mut status: Option<String> = None;

    // Emit the in-progress fixture (if it declared a component) and reset all
    // three accumulators for the next block.
    fn flush(
        fixtures: &mut Vec<CorruptionFixture>,
        component: &mut Option<String>,
        corrupted_path: &mut Option<String>,
        status: &mut Option<String>,
    ) {
        let comp = component.take();
        let cpath = corrupted_path.take();
        let st = status.take();
        if let Some(component) = comp {
            fixtures.push(CorruptionFixture {
                component,
                corrupted_path: cpath.unwrap_or_default(),
                status: st.unwrap_or_default(),
            });
        }
    }

    for line in text.lines() {
        let line = line.trim();
        if line.starts_with("- name:") {
            flush(
                &mut fixtures,
                &mut component,
                &mut corrupted_path,
                &mut status,
            );
        } else if let Some(rest) = line.strip_prefix("expected_failing_component:") {
            let v = rest.trim().trim_matches('"');
            if !v.is_empty() {
                component = Some(v.to_string());
            }
        } else if let Some(rest) = line.strip_prefix("corrupted_path:") {
            let v = rest.trim().trim_matches('"');
            if !v.is_empty() {
                corrupted_path = Some(v.to_string());
            }
        } else if let Some(rest) = line.strip_prefix("status:") {
            let v = rest.trim().trim_matches('"');
            if !v.is_empty() {
                status = Some(v.to_string());
            }
        }
    }
    flush(
        &mut fixtures,
        &mut component,
        &mut corrupted_path,
        &mut status,
    );
    Ok(fixtures)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// Findings 1 + 2: the corpus walk must prune `.git`/`target` and the
    /// regeneration lane's own `exhaustive-regeneration-report` dir so pointing
    /// `--corpus .` at a checkout root does not enumerate VCS/build noise or the
    /// audit's own inputs/outputs.
    #[test]
    fn walk_relative_skips_vcs_and_build_dirs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        fs::create_dir_all(root.join(".git/objects")).expect("mkdir .git");
        fs::create_dir_all(root.join("target/debug")).expect("mkdir target");
        fs::create_dir_all(root.join("exhaustive-regeneration-report")).expect("mkdir report");
        fs::create_dir_all(root.join("test-data/datasets/test_basic")).expect("mkdir corpus");
        fs::write(root.join(".git/objects/abc"), b"pack").expect("write git");
        fs::write(root.join("target/debug/bin"), b"elf").expect("write target");
        fs::write(
            root.join("exhaustive-regeneration-report/actual.sha256"),
            b"deadbeef  x",
        )
        .expect("write report artifact");
        fs::write(
            root.join("test-data/datasets/test_basic/nb-1-big-Data.db"),
            b"data",
        )
        .expect("write corpus");

        let files = walk_relative(root).expect("walk");

        assert!(
            files.contains("test-data/datasets/test_basic/nb-1-big-Data.db"),
            "corpus file must be enumerated, got: {files:?}"
        );
        assert!(
            !files.iter().any(|p| p.starts_with(".git/")),
            ".git must be pruned, got: {files:?}"
        );
        assert!(
            !files.iter().any(|p| p.starts_with("target/")),
            "target must be pruned, got: {files:?}"
        );
        assert!(
            !files
                .iter()
                .any(|p| p.starts_with("exhaustive-regeneration-report/")),
            "the report dir must be pruned, got: {files:?}"
        );
    }

    /// Finding 3: positional parsing preserves a path containing a double space
    /// and a tab; whitespace-splitting would collapse them and mismatch
    /// `walk_relative` keys.
    #[test]
    fn read_sha256_file_preserves_internal_whitespace() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("sums.sha256");
        let dbl = "a".repeat(64);
        let tab = "b".repeat(64);
        let bin = "c".repeat(64);
        // text mode (two spaces sep) with a double space inside the path;
        // text mode with a TAB inside the path; binary mode (` *`) line.
        let body = format!(
            "{dbl}  dir/with  double space.jsonl\n\
             {tab}  dir/with\ttab.jsonl\n\
             {bin} *dir/binary mode.jsonl\n\
             # a comment line\n\
             \n"
        );
        fs::write(&file, body).expect("write sums");

        let map = read_sha256_file(&file).expect("parse");

        assert_eq!(
            map.get("dir/with  double space.jsonl").map(String::as_str),
            Some(dbl.as_str()),
            "double space in path must be preserved, got: {map:?}"
        );
        assert_eq!(
            map.get("dir/with\ttab.jsonl").map(String::as_str),
            Some(tab.as_str()),
            "tab in path must be preserved, got: {map:?}"
        );
        assert_eq!(
            map.get("dir/binary mode.jsonl").map(String::as_str),
            Some(bin.as_str()),
            "binary-mode `*` prefix must be stripped, got: {map:?}"
        );
        assert_eq!(map.len(), 3, "comment/blank lines ignored, got: {map:?}");
    }

    /// Finding LOW B: GNU `sha256sum` escapes a path containing a backslash or
    /// newline by prefixing the WHOLE line with `\`. The parser must reject such
    /// a line loudly rather than fold the leading `\` into the hash token and
    /// corrupt the key — corpus paths never contain those characters.
    #[test]
    fn read_sha256_file_rejects_gnu_escaped_backslash_line() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("sums.sha256");
        let hash = "a".repeat(64);
        // GNU emits a leading `\` and escapes the in-path backslash as `\\`.
        let body = format!("\\{hash}  dir/with\\\\backslash.jsonl\n");
        fs::write(&file, body).expect("write sums");

        let err = read_sha256_file(&file).expect_err("escaped line must error");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("GNU sha256sum-escaped") && msg.contains("backslash"),
            "expected a clear escaped-path error, got: {msg}"
        );
    }

    /// LOW 2: the corruption-manifest parser must capture per-fixture component,
    /// on-disk `corrupted_path`, and `status` so the audit can cross-check each
    /// declared fixture against the regenerated corpus (spec R4). A `planned`
    /// fixture (declared, no clean source) is still parsed; its on-disk absence
    /// is what the audit catches downstream.
    #[test]
    fn read_corruption_fixtures_captures_component_path_and_status() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("corruption-manifest.yml");
        let body = "\
schema_version: 1
fixtures:
  - name: data_db_bit_flip
    status: active
    component: nb-1-big-Data.db
    corrupted_path: \"corruption/test_comp_corrupt/data_db_bit_flip/nb-1-big-Data.db\"
    expected_failing_component: Data.db
  - name: bti_rows_truncation
    status: planned
    corrupted_path: \"corruption/test_comp_corrupt/bti_rows_truncation/__BTI_ROWS__\"
    expected_failing_component: Rows.db
";
        fs::write(&file, body).expect("write manifest");

        let fixtures = read_corruption_fixtures(&file).expect("parse");
        assert_eq!(fixtures.len(), 2, "got: {fixtures:?}");

        let data = &fixtures[0];
        assert_eq!(data.component, "Data.db");
        assert_eq!(
            data.corrupted_path,
            "corruption/test_comp_corrupt/data_db_bit_flip/nb-1-big-Data.db"
        );
        assert_eq!(data.status, "active");

        let rows = &fixtures[1];
        assert_eq!(rows.component, "Rows.db");
        assert_eq!(rows.status, "planned");
    }
}
