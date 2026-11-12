//! Issue #4498 — crash-consistency harness for the SSTable WRITE verbs (`salvage`,
//! `rebuild`; each later write verb adds a `Verb` entry).
//!
//! # Mechanism
//!
//! The shipped `cqlite` binary is run under `tests/support/crash_fault_shim.c`, an
//! `LD_PRELOAD` interposer on the libc calls every writer bottoms out in (`write`,
//! `pwrite`, `copy_file_range`/`sendfile` for `fs::copy`, `fsync`, `ftruncate`,
//! `rename`, `open(O_CREAT)`, `unlink`, `mkdir`). A counting run enumerates EVERY such
//! boundary under the work directory (so the set is enumerated from the real run, never
//! sampled); then one run per `(boundary, mode)` faults exactly one of them:
//!
//! * `crash`  — SIGKILL before the boundary takes effect (no destructor or cleanup runs),
//! * `enospc` — the boundary fails with `ENOSPC`,
//! * `short`  — a write persists half its bytes and returns the short count, after which the
//!   disk is "full" (`write_all` retries a bare short write, so the full disk is what makes
//!   it observable).
//!
//! No fault code exists in the product: nothing to compile out of release builds, and
//! `release_cli_binary_contains_no_fault_hook` proves it on a release build (AC7).
//!
//! # What is asserted after every injected fault
//!
//! * AC3  the input is byte-identical;
//! * AC5  `enospc`/`short` exit non-zero NAMING the cause ("No space left on device");
//!   `crash` dies by SIGKILL (proving the fault fired);
//! * AC2  the CQLite publication contract: a `*-TOC.txt` is the publication barrier
//!   (`discover_salvage_inputs`; rebuild design D2/D4 "always written LAST"), so if any
//!   TOC exists the generation next to it must be COMPLETE (byte-equal to an uninterrupted
//!   run) and `verify --mode full`-clean;
//! * AC4  re-running the verb into the same `--out` after the failure exits 0 and leaves
//!   exactly the uninterrupted run's bytes.
//!
//! # Cassandra's discoverability rule (cassandra-5.0.8)
//!
//! `Directories.SSTableLister.getFilter` groups EVERY file whose name parses as an SSTable
//! component into a `Descriptor`; there is no "needs a TOC" filter — only the lifecycle
//! transaction log (`LifecycleTransaction`) decides what is live, and `TOCComponent.loadOrCreate`
//! REBUILDS a missing TOC from whatever components are on disk. So against Cassandra's rules
//! the TOC barrier is a CQLite convention, and any final-named component left behind by a
//! crash is discoverable as a (partial) SSTable. `cassandra_discoverable_partial_sets_*`
//! measures that stricter property.

// Linux on x86_64/aarch64 only: the shim's fixed-arity `syscall` interposer and its raw-offset
// decoding (`pwritev`/`pwritev2` split offsets) are written for the 64-bit SysV argument order.
// Every other platform is named by `issue_4498_non_linux_notice`.
#![cfg(all(
    feature = "write-support",
    not(feature = "tombstones"),
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]

use std::collections::BTreeMap;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::OnceLock;

use tempfile::TempDir;

#[derive(Clone, Copy)]
struct Fixture {
    /// Directory under `sstables/` (committed generation directory).
    relative: &'static str,
    schema_file: &'static str,
    table: &'static str,
    /// The Data.db to rebuild.
    data_db: &'static str,
    /// `rebuild --components` for this format (BTI has no Index/Summary/CRC here).
    rebuild_components: &'static str,
}

const BIG_SMALL: Fixture = Fixture {
    relative: "sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77",
    schema_file: "compression-parity.cql",
    table: "lz4_table",
    data_db: "nb-1-big-Data.db",
    rebuild_components: "index,summary,filter,digest,toc,statistics",
};
const BTI_SMALL: Fixture = Fixture {
    relative: "sstables/test_da/multiclustering_table-fd74ad508d2311f1a29b6d2c15dcffdf",
    schema_file: "multiclustering-table-bti.cql",
    table: "multiclustering_table",
    data_db: "da-2-bti-Data.db",
    rebuild_components: "filter,digest,toc,statistics",
};
const BIG_LARGE: Fixture = Fixture {
    relative: "sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294",
    schema_file: "wide-partition-big.cql",
    table: "wide_partition",
    data_db: "nb-2-big-Data.db",
    rebuild_components: "index,summary,filter,digest,toc,statistics",
};

/// Nightly tier knob: `CQLITE_CRASH_CONSISTENCY_LARGE=1` adds the large fixtures.
fn fixtures() -> Vec<Fixture> {
    let mut v = vec![BIG_SMALL, BTI_SMALL];
    if std::env::var("CQLITE_CRASH_CONSISTENCY_LARGE").as_deref() == Ok("1") {
        v.push(BIG_LARGE);
    }
    v
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verb {
    Salvage,
    Rebuild,
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("repo root")
        .to_path_buf()
}

/// COMMITTED fixture: absence is a broken checkout and fails closed unconditionally (#3220).
fn fixture_dir(f: &Fixture) -> PathBuf {
    let mut roots = Vec::new();
    if let Ok(r) = std::env::var("CQLITE_DATASETS_ROOT") {
        roots.push(PathBuf::from(r));
    }
    roots.push(repo_root().join("test-data/datasets"));
    for r in roots {
        let d = r.join(f.relative);
        if d.join(f.data_db).is_file() {
            return d;
        }
    }
    panic!(
        "COMMITTED fixture {} is absent under every candidate root",
        f.relative
    );
}

/// Compile the fault shim once per test process (fails closed: no `cc`, no harness).
fn shim() -> &'static Path {
    static SHIM: OnceLock<(TempDir, PathBuf)> = OnceLock::new();
    &SHIM
        .get_or_init(|| {
            let dir = TempDir::new().expect("shim tempdir");
            let so = dir.path().join("crash_fault_shim.so");
            let src =
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/support/crash_fault_shim.c");
            let out = Command::new("cc")
                .args(["-shared", "-fPIC", "-O1", "-o"])
                .arg(&so)
                .arg(&src)
                .arg("-ldl")
                .output()
                .unwrap_or_else(|e| {
                    panic!(
                        "FAIL CLOSED: the C compiler `cc` is required to build the #4498 crash-fault \
                         shim and could not be run ({e}); install a C toolchain (gcc/clang)"
                    )
                });
            assert!(
                out.status.success(),
                "crash_fault_shim.c failed to compile:\n{}",
                String::from_utf8_lossy(&out.stderr)
            );
            (dir, so)
        })
        .1
}

/// The staged INPUT, created once per fixture and shared by every run. Sharing matters for
/// determinism: `rebuild` copies the untouched components in `read_dir` order, which depends on
/// the directory's on-disk state, so re-staging per run would renumber the boundaries.
struct Staged {
    _tmp: TempDir,
    input: PathBuf,
}

fn stage_input(f: &Fixture) -> Staged {
    let tmp = TempDir::new().expect("tempdir");
    // canonicalize: the shim compares the process's real (/proc) paths against the root.
    let base = tmp.path().canonicalize().expect("canonical tempdir");
    let input = base.join("input").join(f.table);
    std::fs::create_dir_all(&input).expect("mkdir input");
    for e in std::fs::read_dir(fixture_dir(f))
        .expect("read fixture")
        .flatten()
    {
        if e.path().is_file() {
            std::fs::copy(e.path(), input.join(e.file_name())).expect("stage copy");
        }
    }
    Staged { _tmp: tmp, input }
}

/// One run's work area: `work/` is the fault root (holds `--out` and the manifest); the
/// boundary log sits next to it, outside the root.
struct Area<'a> {
    _tmp: TempDir,
    input: &'a Path,
    work: PathBuf,
}

fn new_area(staged: &Staged) -> Area<'_> {
    let tmp = TempDir::new().expect("tempdir");
    let base = tmp.path().canonicalize().expect("canonical tempdir");
    let work = base.join("work");
    std::fs::create_dir_all(&work).expect("mkdir work");
    Area {
        _tmp: tmp,
        input: &staged.input,
        work,
    }
}

/// Every file under `dir` (recursive) -> bytes. Used for input immutability and output sets.
fn snapshot(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(base: &Path, dir: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(base, &p, out);
            } else {
                out.insert(
                    p.strip_prefix(base).unwrap().to_path_buf(),
                    std::fs::read(&p).unwrap_or_default(),
                );
            }
        }
    }
    let mut m = BTreeMap::new();
    walk(dir, dir, &mut m);
    m
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Crash,
    Enospc,
    Short,
}

impl Mode {
    fn env(self) -> &'static str {
        match self {
            Mode::Crash => "crash",
            Mode::Enospc => "enospc",
            Mode::Short => "short",
        }
    }
}

struct Run {
    out: Output,
}

impl Run {
    fn code(&self) -> Option<i32> {
        self.out.status.code()
    }
    fn signal(&self) -> Option<i32> {
        self.out.status.signal()
    }
    fn text(&self) -> String {
        format!(
            "{}{}",
            String::from_utf8_lossy(&self.out.stdout),
            String::from_utf8_lossy(&self.out.stderr)
        )
    }
}

fn out_dir(area: &Area<'_>) -> PathBuf {
    area.work.join("out")
}

/// Run `verb` on the staged fixture into `work/out` (+ `work/manifest.json`), optionally under
/// the shim.
fn run_verb(
    verb: Verb,
    f: &Fixture,
    area: &Area<'_>,
    shim_env: Option<(&Path, Option<(usize, Mode)>)>,
) -> Run {
    let schema = repo_root().join("test-data/schemas").join(f.schema_file);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_cqlite"));
    cmd.arg("--schema").arg(&schema);
    match verb {
        Verb::Salvage => {
            cmd.arg("salvage").arg(area.input);
        }
        Verb::Rebuild => {
            cmd.arg("rebuild")
                .arg(area.input.join(f.data_db))
                .args(["--components", f.rebuild_components]);
        }
    }
    cmd.args(["--table", f.table, "--out"])
        .arg(out_dir(area))
        .arg("--manifest")
        .arg(area.work.join("manifest.json"))
        .args(["--out-format", "json"]);
    if let Some((so, fault)) = shim_env {
        cmd.env("LD_PRELOAD", so)
            .env("CQLITE_FAULT_ROOT", &area.work)
            .env(
                "CQLITE_FAULT_LOG",
                area.work.parent().unwrap().join("boundaries.log"),
            );
        if let Some((at, mode)) = fault {
            cmd.env("CQLITE_FAULT_AT", at.to_string())
                .env("CQLITE_FAULT_MODE", mode.env());
        }
    }
    Run {
        out: cmd.output().expect("run cqlite"),
    }
}

/// The `(kind, path-relative-to-work)` the shim logged for boundary number `n` in THIS run, if
/// any. Lenient (no contiguity requirement): a fault run stops mid-sequence.
fn boundary_hit(area: &Area<'_>, n: usize) -> Option<(String, String)> {
    let log = area.work.parent().unwrap().join("boundaries.log");
    let text = std::fs::read_to_string(&log).ok()?;
    let prefix = area.work.display().to_string();
    text.lines().find_map(|l| {
        let mut it = l.splitn(3, ' ');
        let num: usize = it.next()?.parse().ok()?;
        if num != n {
            return None;
        }
        let kind = it.next()?.to_string();
        let path = it
            .next()?
            .replace(&prefix, "")
            .trim_start_matches('/')
            .to_string();
        Some((kind, path))
    })
}

/// The boundary list of an uninterrupted run: `(kind, path-relative-to-work)`, indexed by the
/// boundary NUMBER the shim assigned (`CQLITE_FAULT_AT` addresses that number). The shim assigns
/// numbers atomically but appends log lines separately, so concurrent threads can write lines out
/// of numeric order; the parser therefore sorts by the RECORDED number and requires the sequence
/// to be exactly `1..=N` (a gap or duplicate means a lost or doubled log line, and would let a
/// fault index point at the wrong operation), failing loudly otherwise.
fn boundaries(area: &Area<'_>) -> Vec<(String, String)> {
    let log = area.work.parent().unwrap().join("boundaries.log");
    let text = std::fs::read_to_string(&log).unwrap_or_default();
    let prefix = area.work.display().to_string();
    let mut rows: Vec<(usize, String, String)> = text
        .lines()
        .map(|l| {
            let mut it = l.splitn(3, ' ');
            let n: usize = it
                .next()
                .and_then(|x| x.parse().ok())
                .unwrap_or_else(|| panic!("unparseable boundary log line: {l:?}"));
            let kind = it.next().unwrap_or("?").to_string();
            let path = it
                .next()
                .unwrap_or("?")
                .replace(&prefix, "")
                .trim_start_matches('/')
                .to_string();
            (n, kind, path)
        })
        .collect();
    rows.sort_by_key(|(n, _, _)| *n);
    for (i, (n, _, _)) in rows.iter().enumerate() {
        assert_eq!(
            *n,
            i + 1,
            "boundary log numbers must be contiguous 1..=N (line {} carries number {n}): a lost or \
             duplicated log line would misattribute faults",
            i + 1
        );
    }
    rows.into_iter().map(|(_, k, p)| (k, p)).collect()
}

/// The `--manifest` JSON of a run, normalised for comparison across runs. Exactly two things are
/// normalised, each provably not a property of the tool's output: (1) the run-specific temp
/// directories (`<WORK>`/`<INPUT>`), which differ per harness area by construction, and (2) every
/// `"now"` field, the wall-clock RFC 3339 timestamp the report stamps at run time
/// (verified by diffing two clean runs: it is the only differing field). `None` = no manifest.
fn manifest_normalised(area: &Area<'_>) -> Option<String> {
    let text = std::fs::read_to_string(area.work.join("manifest.json")).ok()?;
    let mut v: serde_json::Value = serde_json::from_str(&text).ok()?;
    fn scrub(v: &mut serde_json::Value) {
        match v {
            serde_json::Value::Object(m) => {
                for (k, x) in m.iter_mut() {
                    if k == "now" {
                        *x = serde_json::Value::String("<now>".into());
                    } else {
                        scrub(x);
                    }
                }
            }
            serde_json::Value::Array(a) => a.iter_mut().for_each(scrub),
            _ => {}
        }
    }
    scrub(&mut v);
    let out = serde_json::to_string_pretty(&v).ok()?;
    Some(
        out.replace(&area.work.display().to_string(), "<WORK>")
            .replace(&area.input.display().to_string(), "<INPUT>"),
    )
}

/// `None` when the post-fault manifest is acceptable: absent, or complete valid JSON equal to the
/// reference. `Some(reason)` for a truncated/malformed file or a complete-but-different one.
fn manifest_violation(area: &Area<'_>, reference: &Option<String>) -> Option<String> {
    let path = area.work.join("manifest.json");
    let Ok(raw) = std::fs::read_to_string(&path) else {
        return if path.exists() {
            Some("manifest.json exists but is unreadable".to_string())
        } else {
            None
        };
    };
    match manifest_normalised(area) {
        None => Some(format!(
            "manifest.json is TORN/malformed ({} bytes, not valid JSON)",
            raw.len()
        )),
        Some(m) if reference.as_ref() != Some(&m) => {
            Some("manifest.json is valid JSON but differs from the uninterrupted run's".to_string())
        }
        Some(_) => None,
    }
}

/// Component files (not the manifest) of the output tree, relative to `out/`.
fn output_set(area: &Area<'_>) -> BTreeMap<PathBuf, Vec<u8>> {
    snapshot(&out_dir(area))
}

fn tocs(set: &BTreeMap<PathBuf, Vec<u8>>) -> Vec<PathBuf> {
    set.keys()
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-TOC.txt"))
        })
        .cloned()
        .collect()
}

fn verify_clean(dir: &Path, f: &Fixture) -> bool {
    Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .arg("--schema")
        .arg(repo_root().join("test-data/schemas").join(f.schema_file))
        .args(["verify"])
        .arg(dir)
        .args(["--mode", "full"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Whether an uninterrupted run is expected to contain a `rename` boundary (the atomic-publication
/// step). Flips to `true` with the TOC fix (#4526).
const EXPECT_RENAME: bool = false;

/// The enumerated boundaries must contain every syscall CATEGORY an SSTable write verb performs,
/// per output file, so a shim that stopped seeing (say) `copy_file_range` or vectored writes
/// cannot shrink the matrix unnoticed behind a "enough boundaries" count:
/// every output component file needs a `create` and, when non-empty, at least one `write`/`pwrite`/`copy`, the
/// manifest needs the same, and the run needs >= 1 `mkdir`, >= 1 `fsync` (and a `rename` when
/// [`EXPECT_RENAME`]).
fn assert_expected_categories(
    verb: Verb,
    bounds: &[(String, String)],
    reference: &BTreeMap<PathBuf, Vec<u8>>,
) {
    let has = |kinds: &[&str], path: &str| {
        bounds
            .iter()
            .any(|(k, p)| kinds.contains(&k.as_str()) && p == path)
    };
    let mut files: Vec<(String, bool)> = reference
        .iter()
        .map(|(rel, bytes)| (format!("out/{}", rel.display()), !bytes.is_empty()))
        .collect();
    files.push(("manifest.json".to_string(), true));
    for (f, non_empty) in &files {
        assert!(
            has(&["create"], f),
            "{verb:?}: no `create` boundary enumerated for {f} — shim blind to file creation?"
        );
        // A legitimately EMPTY output (e.g. an empty BTI Rows.db) has a create and no write.
        assert!(
            !non_empty || has(&["write", "pwrite", "copy"], f),
            "{verb:?}: no write-class boundary (write/pwrite/copy) enumerated for {f} — shim blind \
             to the call this writer uses?"
        );
    }
    let any = |kind: &str| bounds.iter().any(|(k, _)| k == kind);
    assert!(any("mkdir"), "{verb:?}: no `mkdir` boundary enumerated");
    assert!(any("fsync"), "{verb:?}: no `fsync` boundary enumerated");
    if EXPECT_RENAME {
        assert!(any("rename"), "{verb:?}: no `rename` boundary enumerated");
    }
}

/// Which slice of the boundary x mode matrix a sweep exercises.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Scope {
    /// Everything EXCEPT the listed [`DEFECTS`]: the property the CLI must hold today. Any
    /// failure not listed there reds the test.
    Enforced,
    /// ONLY the cells [`DEFECTS`] lists for this issue (driven by the `#[ignore]`d reproducers).
    KnownDefects(u32),
    /// Only the `write` boundaries on paths ending in this suffix, in this mode (a named
    /// regression for a fixed defect; the cells run are NOT in [`DEFECTS`]).
    Only(&'static str, Mode),
    /// Crash mode only, against Cassandra's discoverability rule (see module doc; #4529).
    CassandraStrict,
}

/// One boundary x mode cell with a confirmed, un-fixed product defect, keyed by
/// `(verb, fixture table, boundary kind + path suffix, mode, issue)`. The enforced sweep skips
/// exactly these cells and nothing else, so an unlisted failure always reds; each listed issue is
/// reproduced by its own `#[ignore = "#<issue>"]` test below. Fixing an issue means deleting its
/// entries and its `#[ignore]`.
struct Defect {
    verb: Verb,
    fixture: &'static str,
    /// `write` boundary on a path ending in this suffix.
    path_suffix: &'static str,
    modes: &'static [Mode],
    issue: u32,
}

/// Fixture tables a defect applies to (all the harness fixtures: Filter/TOC/manifest writes are
/// format-independent).
const ALL_FIXTURES: [&str; 3] = ["lz4_table", "multiclustering_table", "wide_partition"];

fn defects() -> Vec<Defect> {
    let mut v = Vec::new();
    for verb in [Verb::Rebuild, Verb::Salvage] {
        for fixture in ALL_FIXTURES {
            // #4526: TOC.txt is created in place and written non-atomically, so a crash or short
            // write mid-write publishes a partial barrier.
            v.push(Defect {
                verb,
                fixture,
                path_suffix: "-TOC.txt",
                modes: &[Mode::Crash, Mode::Short],
                issue: 4526,
            });
            // #4527: an ENOSPC on Filter.db's write is not propagated (exit 0, TOC published over
            // an incomplete set).
            v.push(Defect {
                verb,
                fixture,
                path_suffix: "-Filter.db",
                modes: &[Mode::Enospc],
                issue: 4527,
            });
        }
    }
    for fixture in ALL_FIXTURES {
        // TORN_MANIFEST (#4549): `--manifest` is written in place
        // (`File::create`/`fs::write`, not temp + rename), so a crash mid-write leaves a 0-byte or
        // truncated manifest, and rebuild also leaves one after an ENOSPC/short write.
        v.push(Defect {
            verb: Verb::Salvage,
            fixture,
            path_suffix: "manifest.json",
            modes: &[Mode::Crash],
            issue: TORN_MANIFEST,
        });
        v.push(Defect {
            verb: Verb::Rebuild,
            fixture,
            path_suffix: "manifest.json",
            modes: &[Mode::Crash, Mode::Enospc, Mode::Short],
            issue: TORN_MANIFEST,
        });
    }
    v
}

/// Issue number of the torn-`--manifest` defect.
const TORN_MANIFEST: u32 = 4549;

fn known_defect(verb: Verb, f: &Fixture, kind: &str, path: &str, mode: Mode) -> Option<u32> {
    if !matches!(kind, "write" | "pwrite") {
        return None;
    }
    defects()
        .into_iter()
        .find(|d| {
            d.verb == verb
                && d.fixture == f.table
                && path.ends_with(d.path_suffix)
                && d.modes.contains(&mode)
        })
        .map(|d| d.issue)
}

/// The per-verb, per-fixture sweep. Returns `(boundary_count, runs, failures)`.
fn sweep(verb: Verb, f: &Fixture, scope: Scope) -> (usize, usize, Vec<String>) {
    // Reference: counting run, twice (the boundary list must be deterministic or the
    // enumeration is meaningless).
    let staged = stage_input(f);
    let area = new_area(&staged);
    let r = run_verb(verb, f, &area, Some((shim(), None)));
    assert_eq!(
        r.code(),
        Some(0),
        "uninterrupted {verb:?} must succeed: {}",
        r.text()
    );
    let bounds = boundaries(&area);
    let reference = output_set(&area);
    assert!(
        !reference.is_empty() && !tocs(&reference).is_empty(),
        "reference run produced no TOC"
    );
    let area2 = new_area(&staged);
    let r2 = run_verb(verb, f, &area2, Some((shim(), None)));
    assert_eq!(r2.code(), Some(0));
    assert_eq!(
        bounds,
        boundaries(&area2),
        "boundary enumeration must be deterministic"
    );
    assert_eq!(
        reference,
        output_set(&area2),
        "two uninterrupted runs must produce identical bytes"
    );
    let reference_manifest = manifest_normalised(&area);
    assert!(
        reference_manifest.is_some(),
        "the uninterrupted run wrote no parseable --manifest"
    );
    assert_eq!(
        reference_manifest,
        manifest_normalised(&area2),
        "two uninterrupted runs must produce the same manifest (after normalising only <now> and \
         the temp dirs)"
    );
    assert_expected_categories(verb, &bounds, &reference);

    let mut failures = Vec::new();
    let mut runs = 0usize;
    for (i, (kind, path)) in bounds.iter().enumerate() {
        let n = i + 1;
        let is_write = matches!(kind.as_str(), "write" | "pwrite" | "copy");
        for mode in [Mode::Crash, Mode::Enospc, Mode::Short] {
            if mode == Mode::Short && !is_write {
                continue; // identical to Enospc for a non-write boundary
            }
            let defect = known_defect(verb, f, kind, path, mode);
            match scope {
                Scope::Enforced if defect.is_some() => continue,
                Scope::KnownDefects(issue) if defect != Some(issue) => continue,
                Scope::Only(suffix, m) if !(is_write && path.ends_with(suffix) && mode == m) => {
                    continue
                }
                Scope::CassandraStrict if mode != Mode::Crash => continue,
                _ => {}
            }
            runs += 1;
            let tag = format!("{verb:?}/{}#{n} {kind} {path} [{}]", f.table, mode.env());
            // The fault is addressed by boundary NUMBER; boundary numbering is assigned atomically
            // but the numbered operation can differ between runs if threads interleave. So the shim's
            // own log of THIS run must show boundary `n` is the same (kind, path) the reference
            // enumerated; on a mismatch the run is retried once on a fresh area, then fails closed
            // naming both (roborev r4).
            let mut attempt = 0;
            let (a, run, input_before) = loop {
                let a = new_area(&staged);
                let input_before = snapshot(a.input);
                let run = run_verb(verb, f, &a, Some((shim(), Some((n, mode)))));
                let hit = boundary_hit(&a, n);
                if hit.as_ref() == Some(&(kind.clone(), path.clone())) {
                    break (a, run, input_before);
                }
                attempt += 1;
                assert!(
                    attempt < 2,
                    "{tag}: boundary numbering is not stable: the reference enumerated boundary {n} \
                     as ({kind}, {path}) but the faulted run's own log shows {hit:?} (retried once)"
                );
            };

            // AC5 / the fault actually fired.
            match mode {
                Mode::Crash => {
                    if run.signal() != Some(9) {
                        failures.push(format!(
                            "{tag}: expected SIGKILL, got code={:?} signal={:?}",
                            run.code(),
                            run.signal()
                        ));
                    }
                }
                Mode::Enospc | Mode::Short => {
                    if run.code().is_none_or(|c| c == 0) {
                        failures.push(format!(
                            "{tag}: injected ENOSPC but exit was {:?}/{:?} (must be non-zero)",
                            run.code(),
                            run.signal()
                        ));
                    } else if !run.text().contains("No space left on device") {
                        failures.push(format!("{tag}: non-zero exit {:?} but the cause (\"No space left on device\") is not named: {}", run.code(), run.text().lines().last().unwrap_or("")));
                    }
                }
            }
            // AC3
            if snapshot(a.input) != input_before {
                failures.push(format!("{tag}: INPUT CHANGED"));
            }
            // The --manifest after a fault: absent, or a COMPLETE valid JSON document equal (after
            // the normalisation documented on `manifest_normalised`) to the uninterrupted run's.
            // Never truncated or malformed (roborev r3).
            if let Some(why) = manifest_violation(&a, &reference_manifest) {
                failures.push(format!("{tag}: {why}"));
            }
            // AC2 (CQLite publication contract)
            let after = output_set(&a);
            for toc in tocs(&after) {
                let gen = toc.parent().unwrap_or(Path::new(""));
                let have: BTreeMap<_, _> = after
                    .iter()
                    .filter(|(p, _)| p.parent() == Some(gen))
                    .collect();
                let want: BTreeMap<_, _> = reference
                    .iter()
                    .filter(|(p, _)| p.parent() == Some(gen))
                    .collect();
                if have != want {
                    failures.push(format!("{tag}: TOC published ({}) but the generation is not the complete reference set", toc.display()));
                } else if !verify_clean(&out_dir(&a).join(gen), f) {
                    failures.push(format!("{tag}: TOC published and set complete but `verify --mode full` is not clean"));
                }
            }
            if scope == Scope::CassandraStrict {
                // Cassandra (5.0.8) groups EVERY component-named file into a Descriptor, so any
                // final-named file left behind is a discoverable (partial) SSTable.
                if !after.is_empty() && after != reference {
                    failures.push(format!("{tag}: crash left {} final-named file(s) = a Cassandra-discoverable PARTIAL SSTable", after.len()));
                }
                continue;
            }
            // AC4: re-run after the failure. Both verbs REFUSE a non-empty --out by contract
            // ("--out ... is not empty"), so the operator clears it first; the retry must then
            // succeed and reproduce the uninterrupted run byte-for-byte.
            let leftover = run_verb(verb, f, &a, Some((shim(), None)));
            if after.is_empty() {
                // nothing left behind: the retry must simply succeed
            } else if leftover.code() == Some(0) {
                failures.push(format!("{tag}: retry into a populated --out unexpectedly succeeded (the non-empty guard is part of the contract)"));
            } else if !leftover.text().contains("not empty") {
                failures.push(format!(
                    "{tag}: retry into a populated --out failed without naming the cause: {}",
                    leftover.text().lines().last().unwrap_or("")
                ));
            }
            let _ = std::fs::remove_dir_all(out_dir(&a));
            let _ = std::fs::remove_file(a.work.join("manifest.json"));
            let again = run_verb(verb, f, &a, Some((shim(), None)));
            if again.code() != Some(0) {
                failures.push(format!(
                    "{tag}: re-run after failure did not succeed: code={:?} {}",
                    again.code(),
                    again.text().lines().last().unwrap_or("")
                ));
            } else if output_set(&a) != reference {
                failures.push(format!(
                    "{tag}: re-run output differs from the uninterrupted run"
                ));
            } else if manifest_normalised(&a) != reference_manifest {
                failures.push(format!(
                    "{tag}: re-run --manifest differs from (or is missing against) the \
                     uninterrupted run's"
                ));
            }
        }
    }
    (bounds.len(), runs, failures)
}

fn report(verb: Verb, f: &Fixture, n: usize, runs: usize, failures: &[String]) {
    eprintln!(
        "CRASH-CONSISTENCY {verb:?} {}: {n} boundaries, {runs} injected runs, {} failure(s)",
        f.table,
        failures.len()
    );
    for x in failures {
        eprintln!("  FAIL {x}");
    }
}

fn check(verb: Verb, scope: Scope, fixtures: Vec<Fixture>) {
    check_verbs(&[verb], scope, fixtures);
}

fn check_verbs(verbs: &[Verb], scope: Scope, fixtures: Vec<Fixture>) {
    let mut all = Vec::new();
    for verb in verbs {
        for f in &fixtures {
            let (n, runs, failures) = sweep(*verb, f, scope);
            report(*verb, f, n, runs, &failures);
            all.extend(failures);
        }
    }
    assert!(all.is_empty(), "{} failure(s); see stderr", all.len());
}

#[test]
fn rebuild_survives_a_fault_at_every_boundary() {
    check(Verb::Rebuild, Scope::Enforced, fixtures());
}

#[test]
fn salvage_survives_a_fault_at_every_boundary() {
    check(Verb::Salvage, Scope::Enforced, fixtures());
}

// ---- Confirmed product defects (issues named per test), each reproduced by running ONLY the
// matrix cells `defects()` lists for it. Removing `#[ignore]` (and the entries) after a fix
// proves the fix.

#[test]
#[ignore = "#4526 TOC.txt is written in place, non-atomically"]
fn issue_4526_torn_toc() {
    check_verbs(
        &[Verb::Rebuild, Verb::Salvage],
        Scope::KnownDefects(4526),
        fixtures(),
    );
}

#[test]
#[ignore = "#4527 an ENOSPC on Filter.db's write exits 0"]
fn issue_4527_filter_write_error_swallowed() {
    check_verbs(
        &[Verb::Rebuild, Verb::Salvage],
        Scope::KnownDefects(4527),
        fixtures(),
    );
}

/// Regression for #4528 (fixed): a failed `--manifest` write (ENOSPC / short write) must exit
/// non-zero naming the cause; a `BufWriter` dropped unflushed used to swallow it.
#[test]
fn issue_4528_salvage_manifest_write_error_exits_nonzero() {
    check(
        Verb::Salvage,
        Scope::Only("manifest.json", Mode::Enospc),
        fixtures(),
    );
    check(
        Verb::Salvage,
        Scope::Only("manifest.json", Mode::Short),
        fixtures(),
    );
}

#[test]
#[ignore = "#4549 --manifest is written in place: a crash (or rebuild's ENOSPC/short write) leaves a torn manifest.json"]
fn issue_4549_torn_manifest_after_a_fault() {
    check_verbs(
        &[Verb::Rebuild, Verb::Salvage],
        Scope::KnownDefects(TORN_MANIFEST),
        fixtures(),
    );
}

#[test]
#[ignore = "#4529 final SSTable names in place: a crash leaves a Cassandra-discoverable partial set (owner decision pending)"]
fn issue_4529_cassandra_discoverable_partial_sets_rebuild() {
    check(Verb::Rebuild, Scope::CassandraStrict, vec![BIG_SMALL]);
}

#[test]
#[ignore = "#4529 final SSTable names in place: a crash leaves a Cassandra-discoverable partial set (owner decision pending)"]
fn issue_4529_cassandra_discoverable_partial_sets_salvage() {
    check(Verb::Salvage, Scope::CassandraStrict, vec![BIG_SMALL]);
}

// ---- Shim coverage guard ---------------------------------------------------------------

/// Run `nm -D` and return the symbol names (version suffix stripped). Fails closed, naming `nm`.
fn dynamic_symbols(bin: &Path, which: &str) -> Vec<String> {
    let out = Command::new("nm")
        .args(["-D", which])
        .arg(bin)
        .output()
        .unwrap_or_else(|e| {
            panic!("FAIL CLOSED: `nm` (binutils) is required for the shim-coverage guard ({e})")
        });
    assert!(
        out.status.success(),
        "nm -D {which} {} failed",
        bin.display()
    );
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| l.split_whitespace().last())
        .map(|n| n.split('@').next().unwrap_or(n).to_string())
        .collect()
}

/// Imported libc symbols that mutate or may mutate the filesystem. Anything the CLI imports that
/// matches is a potential write boundary and MUST be interposed by the shim, or be listed in
/// [`NOT_A_WRITE_BOUNDARY`] with a reason.
fn is_io_mutating(sym: &str) -> bool {
    [
        "write",
        "pwrite",
        "fwrite",
        "fsync",
        "fdatasync",
        "rename",
        "open",
        "creat",
        "unlink",
        "mkdir",
        "rmdir",
        "truncate",
        "ftruncate",
        "copy_file_range",
        "sendfile",
        "splice",
        "link",
        "symlink",
        "fallocate",
        "posix_fallocate",
        "chmod",
        "fchmod",
        "chown",
        "fchown",
        "lchown",
        "utime",
        "futimens",
        "utimensat",
        "mmap",
        "msync",
        "remove",
        "syscall",
        "aio_",
        "io_uring",
        "fputs",
        "fputc",
        "putc",
        "fprintf",
        "dprintf",
    ]
    .iter()
    .any(|k| sym.starts_with(k))
}

/// Matched by [`is_io_mutating`] but not a data/directory-entry write boundary, each with the reason.
const NOT_A_WRITE_BOUNDARY: &[(&str, &str)] = &[
    ("opendir", "directory read"),
    ("fdopendir", "directory read"),
    ("readlink", "read-only"),
    ("fchmod", "permission bits only; no data or directory-entry change a crash could tear"),
    ("mmap64", "read-only mappings (memmap2::Mmap) for the readers; no MAP_SHARED writable map in the verbs"),
    ("mmap", "as mmap64"),
    ("munmap", "unmaps"),
    ("posix_spawn_file_actions_addchdir_np", "process spawn plumbing"),
    ("posix_spawn_file_actions_addchdir", "process spawn plumbing"),
    ("posix_spawn_file_actions_adddup2", "process spawn plumbing"),
    ("posix_spawn_file_actions_destroy", "process spawn plumbing"),
    ("posix_spawn_file_actions_init", "process spawn plumbing"),
    ("posix_spawnattr_destroy", "process spawn plumbing"),
    ("posix_spawnattr_init", "process spawn plumbing"),
    ("posix_spawnattr_setflags", "process spawn plumbing"),
    ("posix_spawnattr_setpgroup", "process spawn plumbing"),
    ("posix_spawnattr_setsigdefault", "process spawn plumbing"),
    ("posix_spawnp", "process spawn"),
    ("posix_spawn", "process spawn"),
];

/// `syscall` is listed as interposed but the shim handles only the mutating raw numbers it has
/// wrappers for; every OTHER mutating number aborts the run (exit 97, see
/// `unhandled_mutating_raw_syscall_aborts_the_run`), so it can never be a silent blind spot.
///
/// The CLI binary must not import an I/O-mutating libc symbol the shim does not interpose: a
/// future writer using a new call (`pwritev2`, `linkat`, ...) would otherwise go invisible to the
/// whole matrix. Lists what it checked on stderr.
#[test]
fn every_imported_io_symbol_is_interposed_by_the_shim() {
    let cli = PathBuf::from(env!("CARGO_BIN_EXE_cqlite"));
    let imported: Vec<String> = dynamic_symbols(&cli, "--undefined-only")
        .into_iter()
        .filter(|s| is_io_mutating(s))
        .collect();
    let interposed = dynamic_symbols(shim(), "--defined-only");
    let mut checked = Vec::new();
    let mut unaccounted = Vec::new();
    for sym in &imported {
        if interposed.contains(sym) {
            checked.push(format!("{sym}=interposed"));
        } else if let Some((_, why)) = NOT_A_WRITE_BOUNDARY.iter().find(|(n, _)| n == sym) {
            checked.push(format!("{sym}=exempt({why})"));
        } else {
            unaccounted.push(sym.clone());
        }
    }
    eprintln!("shim coverage: {}", checked.join(", "));
    assert!(
        !imported.is_empty() && !checked.is_empty(),
        "no I/O imports found in the CLI: the guard's symbol scan is broken"
    );
    assert!(
        unaccounted.is_empty(),
        "the cqlite binary imports I/O symbol(s) the fault shim does NOT interpose and that are \
         not in NOT_A_WRITE_BOUNDARY: {unaccounted:?} — add them to tests/support/crash_fault_shim.c \
         (or, if provably not a write boundary, to NOT_A_WRITE_BOUNDARY with the reason)"
    );
}

// ---- `syscall(2)` interposer: raw-ABI decode and the blind-spot abort --------------------

/// Compile `src` (a one-file C program) and run it under the shim with the fault root `root`.
/// Returns `(exit status, boundary log text)`.
fn run_c_under_shim(
    tag: &str,
    src: &str,
    root: &Path,
) -> (std::process::ExitStatus, String, String) {
    let tmp = TempDir::new().expect("tempdir");
    let c = tmp.path().join(format!("{tag}.c"));
    let exe = tmp.path().join(tag);
    std::fs::write(&c, src).expect("write c");
    let out = Command::new("cc")
        .arg(&c)
        .arg("-o")
        .arg(&exe)
        .output()
        .unwrap_or_else(|e| panic!("FAIL CLOSED: `cc` could not be run ({e})"));
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let log = tmp.path().join("log");
    let run = Command::new(&exe)
        .env("LD_PRELOAD", shim())
        .env("CQLITE_FAULT_ROOT", root)
        .env("CQLITE_FAULT_LOG", &log)
        .output()
        .expect("run c program");
    (
        run.status,
        std::fs::read_to_string(&log).unwrap_or_default(),
        String::from_utf8_lossy(&run.stderr).into_owned(),
    )
}

/// A raw `syscall(SYS_pwritev2, ...)` is decoded with the KERNEL's argument order (flags is the
/// sixth argument, after the split offset) and counted as one write-class boundary; the data is
/// still written.
#[test]
fn raw_pwritev2_syscall_is_decoded_with_the_kernel_abi_and_counted() {
    let dir = TempDir::new().expect("tempdir");
    let root = dir.path().canonicalize().unwrap();
    let file = root.join("f.bin");
    let src = format!(
        r#"#define _GNU_SOURCE
#include <fcntl.h>
#include <stdio.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <unistd.h>
int main(void) {{
  int fd = open("{}", O_CREAT | O_WRONLY, 0644);
  struct iovec iov[2] = {{ {{ "ab", 2 }}, {{ "cd", 2 }} }};
  long r = syscall(SYS_pwritev2, fd, iov, 2, 0L, 0L, 0L);
  return r == 4 ? 0 : 3;
}}"#,
        file.display()
    );
    let (status, log, _) = run_c_under_shim("pwritev2", &src, &root);
    assert!(status.success(), "pwritev2 program failed: {status:?}");
    assert!(
        log.lines()
            .any(|l| l.contains(" pwrite ") && l.ends_with("f.bin")),
        "raw pwritev2 was not counted as a write-class boundary: {log:?}"
    );
    assert_eq!(std::fs::read(&file).unwrap(), b"abcd");
}

/// A raw syscall that is neither handled nor on the non-mutating allowlist must ABORT the run
/// loudly (exit 97, number named), never pass unseen. The allowlist is an ALLOWlist: mutating
/// calls nobody thought of (mknodat, fchmodat, fchownat, setxattr, ...) are covered by default.
#[test]
fn unlisted_raw_syscalls_abort_the_run_including_mutating_ones_nobody_listed() {
    for (name, call) in [
        ("truncate", r#"syscall(SYS_truncate, "ROOT/x", 0L)"#),
        (
            "mknodat",
            r#"syscall(SYS_mknodat, AT_FDCWD, "ROOT/n", 0100644, 0L)"#,
        ),
        (
            "fchmodat",
            r#"syscall(SYS_fchmodat, AT_FDCWD, "ROOT/x", 0600, 0L)"#,
        ),
        (
            "fchownat",
            r#"syscall(SYS_fchownat, AT_FDCWD, "ROOT/x", 0L, 0L, 0L)"#,
        ),
        (
            "setxattr",
            r#"syscall(SYS_setxattr, "ROOT/x", "user.k", "v", 1L, 0L)"#,
        ),
    ] {
        let dir = TempDir::new().expect("tempdir");
        let root = dir.path().canonicalize().unwrap();
        let src = format!(
            r#"#define _GNU_SOURCE
#include <fcntl.h>
#include <sys/syscall.h>
#include <unistd.h>
int main(void) {{ {} ; return 0; }}"#,
            call.replace("ROOT", &root.display().to_string())
        );
        let (status, _, stderr) = run_c_under_shim(name, &src, &root);
        assert_eq!(
            status.code(),
            Some(97),
            "{name}: expected the blind-spot abort, got {status:?}"
        );
        assert!(
            stderr.contains("UNHANDLED raw syscall"),
            "{name}: {stderr:?}"
        );
    }
}

/// The allowlist forwards a futex untouched (the one raw number the real verbs issue).
#[test]
fn allowlisted_raw_syscall_is_forwarded() {
    let dir = TempDir::new().expect("tempdir");
    let root = dir.path().canonicalize().unwrap();
    let src = r#"#define _GNU_SOURCE
#include <sys/syscall.h>
#include <unistd.h>
int main(void) { return syscall(SYS_gettid) > 0 ? 0 : 5; }"#;
    let (status, _, _) = run_c_under_shim("gettid", src, &root);
    assert!(status.success(), "{status:?}");
}

// ---- AC7: the shipped CLI contains no fault hook ----------------------------------------

/// Env-var names of the fault protocol (`tests/support/crash_fault_shim.c`). The product has no
/// fault code at all (the shim interposes libc from outside), so none of these may appear in
/// the `cqlite` binary. A hook added to the product later would have to read some env var or
/// name a symbol; the scan catches the protocol names, and the positive control proves the scan
/// CAN see them (it finds them in the shim itself).
const HOOK_MARKERS: &[&str] = &[
    "CQLITE_FAULT_ROOT",
    "CQLITE_FAULT_AT",
    "CQLITE_FAULT_MODE",
    "CQLITE_FAULT_LOG",
    "crash_fault_shim",
];

fn contains(haystack: &[u8], needle: &str) -> bool {
    let n = needle.as_bytes();
    haystack.windows(n.len()).any(|w| w == n)
}

fn assert_no_markers(bin: &Path) {
    let bytes = std::fs::read(bin).unwrap_or_else(|e| panic!("read {}: {e}", bin.display()));
    for m in HOOK_MARKERS {
        assert!(
            !contains(&bytes, m),
            "{} contains fault-hook marker {m:?}",
            bin.display()
        );
    }
}

/// The RELEASE `cqlite` the AC7 scan reads, resolved deterministically and never skipped:
/// 1. `CQLITE_RELEASE_CLI_BINARY` (a CI job that already built it), else
/// 2. `<target dir>/release/cqlite` built NOW with `cargo build --release -p cqlite-cli --bin
///    cqlite` (incremental after the first build; default features = what ships).
fn release_cli_binary() -> PathBuf {
    if let Ok(p) = std::env::var("CQLITE_RELEASE_CLI_BINARY") {
        let p = PathBuf::from(p);
        assert!(
            p.is_file(),
            "CQLITE_RELEASE_CLI_BINARY={} is not a file",
            p.display()
        );
        return p;
    }
    let cli = PathBuf::from(env!("CARGO_BIN_EXE_cqlite"));
    // <target>/<profile>/cqlite -> <target>
    let target = cli
        .parent()
        .and_then(Path::parent)
        .expect("CARGO_BIN_EXE_cqlite has a target dir")
        .to_path_buf();
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let status = Command::new(cargo)
        .args(["build", "--release", "-p", "cqlite-cli", "--bin", "cqlite"])
        .env("CARGO_TARGET_DIR", &target)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .unwrap_or_else(|e| panic!("AC7: could not run cargo to build the release CLI: {e}"));
    assert!(
        status.success(),
        "AC7: `cargo build --release -p cqlite-cli --bin cqlite` failed; set \
         CQLITE_RELEASE_CLI_BINARY to a release build to skip building"
    );
    let bin = target.join("release").join("cqlite");
    assert!(
        bin.is_file(),
        "AC7: release CLI not found at {} after building",
        bin.display()
    );
    bin
}

#[test]
fn shipped_cli_contains_no_fault_hook() {
    // Positive control: the scan must find every marker in the shim, or it proves nothing.
    let shim_bytes = std::fs::read(shim()).expect("read shim");
    for m in HOOK_MARKERS.iter().filter(|m| **m != "crash_fault_shim") {
        assert!(
            contains(&shim_bytes, m),
            "scan positive control: {m} not found in the shim"
        );
    }
    // The CLI under test (test profile) AND the release build: both MANDATORY.
    assert_no_markers(&PathBuf::from(env!("CARGO_BIN_EXE_cqlite")));
    assert_no_markers(&release_cli_binary());
}
