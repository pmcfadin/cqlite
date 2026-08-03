//! The authoritative row count AND the ingest scope, read out of a #3234 corpus
//! manifest — fail-closed in both directions.
//!
//! Two properties this module exists to hold, both of them earned:
//!
//! 1. **A manifest that is present but unreadable must never degrade into "assert
//!    off"** (rust-reviewer B1) — nor into "assert against a DIFFERENT manifest"
//!    (roborev job 27 B1). That is exactly how a truncated scan measures as a
//!    PASS: the guard that catches a `#3124`-class producer panic (short row count on
//!    a clean end-of-stream) is the only signal there is. Candidate presence is
//!    therefore OBSERVED with `symlink_metadata` (`locate_manifest`), because
//!    `Path::exists()` reports an unreadable path — a broken symlink, an untraversable
//!    parent — as absent, and "absent" is the one state that legitimately falls through
//!    to the next candidate.
//! 2. **Partial verification must not read as full verification** (roborev #3234 L3).
//!    The cross-check block used to be validated with a `match` whose unmatched arms
//!    fell through, so a `row_count_cross_check` object with a missing or non-integer
//!    field passed silently — a corrupted cross-check reported as a satisfied one.
//!    Now: if the object exists, ALL FOUR of its counts must be unsigned integers,
//!    both pairs must agree, and both pairs must equal the corresponding
//!    `rows_per_partition` total.
//! 3. **The manifest's `tables[]` record documents the WORKLOAD, not just the directory,
//!    and both are enforced** (roborev job 27 B2). `sstable_count` +
//!    `sstable_generations` are read here and compared against the observed descriptors
//!    in `scope::resolve` before anything is scanned: an extra generation carrying the
//!    same logical rows leaves the row count identical while changing the merge workload
//!    and the storage route the AC3 figure is attributed to, so the row-count assert
//!    cannot see it.

use std::path::{Path, PathBuf};

/// Presence of this key marks an IN-PROGRESS (or FAILED) generation, written by
/// `gen-perf-corpus-bti.sh` into the authoritative manifest position BEFORE it mutates
/// the published corpus (roborev #3234 M2). It carries no keyspace/table/row count by
/// construction, and its PRESENCE — not its value — is what makes this a refusal: "in
/// progress" is not a row count, and a field is observed or absent.
pub const IN_PROGRESS_KEY: &str = "generation_in_progress";

/// The two `(row-driver, Statistics.db)` count pairs a production manifest records,
/// each with the `rows_per_partition` total it must also equal.
const CROSS_CHECK_PAIRS: [(&str, &str, &str); 2] = [
    ("row_driver_rows", "statistics_db_rows", "rows"),
    (
        "row_driver_partitions",
        "statistics_db_partitions",
        "partitions",
    ),
];

/// How the measured row count is verified. ON by default (`Manifest`).
pub enum RowsAssert {
    /// Read from a manifest: `(rows, provenance)`.
    Manifest(u64, String),
    /// Operator-supplied `--expect-rows N`.
    Explicit(u64),
    /// Operator opted out with `--no-expect-rows`.
    Disabled,
}

impl RowsAssert {
    pub fn expected(&self) -> Option<u64> {
        match self {
            RowsAssert::Manifest(n, _) | RowsAssert::Explicit(n) => Some(*n),
            RowsAssert::Disabled => None,
        }
    }

    pub fn describe(&self) -> String {
        match self {
            RowsAssert::Manifest(n, src) => format!("{n} (authoritative: {src})"),
            RowsAssert::Explicit(n) => {
                format!("{n} (--expect-rows, OPERATOR-SUPPLIED — not the committed manifest)")
            }
            RowsAssert::Disabled => "*** DISABLED (--no-expect-rows) — a SILENTLY TRUNCATED scan \
                                     CANNOT be detected; this measurement is unverified ***"
                .to_string(),
        }
    }
}

/// The corpus directory a manifest DOCUMENTS its counts as having been read from
/// (`tables[].sstable_dir`, corpus-root-relative). Ingestion is scoped to exactly this
/// directory, so the harness's input is exactly what the manifest describes (roborev
/// #3234 M1).
pub struct ManifestScope {
    pub manifest: PathBuf,
    pub sstable_dir_rel: String,
    /// The generation set that directory is documented to hold, compared against the
    /// observed descriptors BEFORE anything is scanned (roborev job 27 B2).
    pub expected: ExpectedGenerations,
}

/// The generation set a `tables[]` record DOCUMENTS for its directory (roborev job 27 B2).
///
/// The reader used to ignore `sstable_count`/`sstable_generations` entirely, and that is
/// the sharpest form of "the measurement describes something the manifest does not":
/// adding a generation that carries the SAME logical rows leaves the reconciled row count
/// IDENTICAL — so the row-count assert stays green — while the **generation count selects
/// the storage route** (`generations > 1 && schema` picks
/// `generation_merge::stream_generations_for_read`), which is exactly what an AC3
/// throughput figure is attributed to. The harness would print `RESULT: PASS` for a corpus
/// its own authority no longer describes.
///
/// Both fields are REQUIRED of a `tables[]` record, for the same reason the record's
/// `sstable_dir` is: a record that scopes the measurement but does not document the
/// workload inside that scope cannot gate it, and an optional gate is the "partial
/// verification reading as full verification" shape this module exists to prevent. A
/// hand-written minimal manifest that cannot supply them simply carries no `tables` array
/// (the sole-directory resolution in `scope::resolve` then applies).
pub struct ExpectedGenerations {
    /// `tables[].sstable_count` — the number of `*-Data.db` generations.
    pub count: usize,
    /// `tables[].sstable_generations`, sorted ascending and verified duplicate-free: the
    /// EXACT identifier set, so swapping one generation for another (which preserves the
    /// count) is caught too.
    pub ids: Vec<u64>,
}

/// A `tables[]` record's two authoritative statements about the bytes to be measured:
/// WHICH directory, and WHICH generations inside it.
struct DocumentedTable {
    sstable_dir_rel: String,
    expected: ExpectedGenerations,
}

/// Candidate manifests, most specific first: the corpus's own manifest (written by
/// the generator for *these* bytes), then the committed one resolved from the
/// checkout this binary was built in, then a CWD-relative fallback.
pub fn manifest_candidates(corpus: &Path) -> Vec<PathBuf> {
    vec![
        corpus.join("manifest-bti-3234.json"),
        PathBuf::from(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../test-data/perf-corpus-bti-manifest.json"
        )),
        PathBuf::from("test-data/perf-corpus-bti-manifest.json"),
    ]
}

/// A manifest's `tables[].sstable_dir`, validated as a corpus-relative path that
/// really does name *this* keyspace and table.
///
/// Fail-closed because this string SELECTS THE BYTES THAT GET MEASURED: an absolute
/// path, a `..` escape or another table's directory would silently redirect the scan.
///
/// **The check IS the claim (roborev #3234 F2).** Round 11's version told the reader it
/// required `sstables/<keyspace>/<table>-<uuid>` and then accepted any `<table>-*` —
/// so `<table>-backup` passed, which is a non-Cassandra backup copy sitting inside the
/// keyspace directory, and it slipped past the ambiguity guard too (that guard only ever
/// sees real table directories). A comment stricter than its code is the defect class
/// this whole round is about, so the final component is now validated with
/// `scope::is_table_dir` — the single definition of `<table>-<32 hex>`, shared with the
/// directory scan, so the manifest path and the corpus scan cannot disagree about what
/// a table directory is.
fn validated_sstable_dir(
    rel: &str,
    keyspace: &str,
    table: &str,
    path: &Path,
) -> std::result::Result<String, String> {
    let bad = |why: &str| {
        format!(
            "{}: `tables[].sstable_dir` = {rel:?} {why}. This path selects the bytes the \
             measurement runs on, so it must be a corpus-relative \
             `sstables/<keyspace>/<table>-<id>` directory naming {keyspace}.{table}, where \
             `<id>` is exactly {} hex digits (a Cassandra table id).",
            path.display(),
            crate::scope::TABLE_ID_HEX_LEN
        )
    };
    let parts: Vec<&str> = rel.split('/').collect();
    if rel.is_empty() || rel.starts_with('/') {
        return Err(bad("is empty or absolute"));
    }
    if parts
        .iter()
        .any(|p| p.is_empty() || *p == "." || *p == "..")
    {
        return Err(bad("contains an empty, `.` or `..` component"));
    }
    if parts.len() != 3 || parts[0] != "sstables" || parts[1] != keyspace {
        return Err(bad("is not `sstables/<keyspace>/<dir>` for this keyspace"));
    }
    if !crate::scope::is_table_dir(parts[2], table) {
        return Err(bad(
            "does not name this table's Cassandra directory (`<table>-<32 hex>`)",
        ));
    }
    Ok(rel.to_string())
}

/// Read the authoritative row count (and the documented ingest scope) out of a #3234
/// corpus manifest.
///
/// Returns `(keyspace, table, rows, documented_table)`. Every failure is an `Err`.
fn read_manifest(
    path: &Path,
) -> std::result::Result<(String, String, u64, Option<DocumentedTable>), String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let json: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| format!("{}: not valid JSON: {e}", path.display()))?;

    // The in-progress marker is checked FIRST: it is a well-formed JSON object that is
    // deliberately not a manifest, and the point of it is that nothing reads numbers
    // out of a directory whose generation did not finish.
    if json.get(IN_PROGRESS_KEY).is_some() {
        return Err(format!(
            "{}: this is an IN-PROGRESS GENERATION MARKER, not a manifest (`{IN_PROGRESS_KEY}` \
             is present). A corpus generation was started in that directory and did not reach \
             the manifest write, so the bytes there have no provenance and nothing can verify a \
             measurement over them.\n  remedy: re-run the generator to completion, or pass \
             --manifest PATH for a finished manifest.",
            path.display()
        ));
    }

    let string_field = |name: &str| -> std::result::Result<String, String> {
        json.get(name)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| format!("{}: missing string field `{name}`", path.display()))
    };
    let keyspace = string_field("keyspace")?;
    let table = string_field("table")?;

    // `rows_per_partition.rows` is the count OBSERVED while writing the CSV chunks
    // (the manifest records its own provenance as "observed, not requested"), which
    // is why it — and not `row_driver_config.rows_requested` — is authoritative.
    let total = |name: &str| -> Option<u64> {
        json.get("rows_per_partition")
            .and_then(|v| v.get(name))
            .and_then(|v| v.as_u64())
    };
    let rows = total("rows").ok_or_else(|| {
        format!(
            "{}: missing unsigned-integer field `rows_per_partition.rows`",
            path.display()
        )
    })?;
    if rows == 0 {
        return Err(format!(
            "{}: `rows_per_partition.rows` is 0 — a manifest describing an empty corpus \
             cannot verify a measurement",
            path.display()
        ));
    }

    check_cross_check(&json, path, rows, &total)?;
    let documented = documented_table(&json, path, &keyspace, &table)?;
    Ok((keyspace, table, rows, documented))
}

/// The generator's fail-closed cross-check (row-driver plan vs each `Statistics.db`),
/// validated COMPLETELY whenever the object exists (roborev #3234 L3).
///
/// The previous version read the four numbers through a `match` with a catch-all arm,
/// so a MISSING or NON-INTEGER field simply fell through and a partially corrupted
/// cross-check passed as a verified one — the same "partial verification reading as
/// full" shape as the `corpus_committed` claim two rounds earlier. There is no
/// half-present state here: a manifest that carries a cross-check carries all four
/// counts, both pairs agree, and each pair equals its `rows_per_partition` total.
fn check_cross_check(
    json: &serde_json::Value,
    path: &Path,
    rows: u64,
    total: &dyn Fn(&str) -> Option<u64>,
) -> std::result::Result<(), String> {
    let Some(x) = json.get("row_count_cross_check") else {
        return Ok(());
    };
    if !x.is_object() {
        return Err(format!(
            "{}: `row_count_cross_check` is present but is not an object",
            path.display()
        ));
    }
    let uint = |name: &str| -> std::result::Result<u64, String> {
        match x.get(name) {
            None => Err(format!(
                "{}: `row_count_cross_check` is present but `{name}` is MISSING. A cross-check \
                 is verified in full or not at all — a partially present one cannot be read as \
                 a satisfied one.",
                path.display()
            )),
            Some(v) => v.as_u64().ok_or_else(|| {
                format!(
                    "{}: `row_count_cross_check.{name}` is {v} — expected an unsigned integer. \
                     A non-integer count verifies nothing.",
                    path.display()
                )
            }),
        }
    };
    for (a, b, total_name) in CROSS_CHECK_PAIRS {
        // Pair first, then the total: a pair that disagrees WITH ITSELF is reported as
        // that, whatever else the manifest is or is not carrying.
        let (va, vb) = (uint(a)?, uint(b)?);
        if va != vb {
            return Err(format!(
                "{}: `row_count_cross_check.{a}` = {va} disagrees with `{b}` = {vb} — the \
                 manifest itself reports a cross-check disagreement",
                path.display()
            ));
        }
        let expected = match total_name {
            "rows" => rows,
            other => total(other).ok_or_else(|| {
                format!(
                    "{}: `row_count_cross_check` is present but `rows_per_partition.{other}` is \
                     missing or not an unsigned integer. The cross-check asserts that count, so \
                     the total it is checked against must be present.",
                    path.display()
                )
            })?,
        };
        if va != expected {
            return Err(format!(
                "{}: `row_count_cross_check.{a}`/`{b}` = {va} disagrees with \
                 `rows_per_partition.{total_name}` = {expected}",
                path.display()
            ));
        }
    }
    Ok(())
}

/// The generation set a `tables[]` record documents, parsed fail-closed (roborev job 27
/// B2). Both fields are required; see [`ExpectedGenerations`] for why an optional one
/// would be no gate at all.
fn expected_generations(
    entry: &serde_json::Value,
    path: &Path,
    table: &str,
) -> std::result::Result<ExpectedGenerations, String> {
    let bad = |why: String| {
        format!(
            "{}: `tables[]` entry for `{table}` {why}. That record documents the bytes this \
             measurement runs on, so it must state BOTH `sstable_count` (an unsigned integer >= \
             1) and `sstable_generations` (exactly that many distinct unsigned-integer \
             generation ids). The generation COUNT selects the storage route (`generations > 1 && \
             schema` => `generation_merge::stream_generations_for_read`), which is what an AC3 \
             figure is attributed to — and an extra generation carrying the SAME logical rows \
             changes the measured merge workload while leaving the row count identical, so the \
             row-count assert cannot see it.",
            path.display()
        )
    };
    let count = match entry.get("sstable_count") {
        None => return Err(bad("has no `sstable_count`".to_string())),
        Some(v) => v.as_u64().ok_or_else(|| {
            bad(format!(
                "has `sstable_count` = {v}, not an unsigned integer"
            ))
        })?,
    };
    if count == 0 {
        return Err(bad(
            "has `sstable_count` = 0 — a directory documented to hold no generation cannot be \
             the corpus of a measurement"
                .to_string(),
        ));
    }
    let count = usize::try_from(count).map_err(|_| {
        bad(format!(
            "has `sstable_count` = {count}, which does not fit in usize"
        ))
    })?;

    let gens = match entry.get("sstable_generations") {
        None => return Err(bad("has no `sstable_generations`".to_string())),
        Some(v) => v
            .as_array()
            .ok_or_else(|| bad(format!("has `sstable_generations` = {v}, not an array")))?,
    };
    let mut ids: Vec<u64> = Vec::with_capacity(gens.len());
    for (i, v) in gens.iter().enumerate() {
        ids.push(v.as_u64().ok_or_else(|| {
            bad(format!(
                "has `sstable_generations[{i}]` = {v}, not an unsigned integer"
            ))
        })?);
    }
    if ids.len() != count {
        return Err(bad(format!(
            "has `sstable_count` = {count} but {} `sstable_generations` entr(y/ies) — a record \
             that disagrees with ITSELF about how many generations it documents cannot gate \
             what is on disk",
            ids.len()
        )));
    }
    ids.sort_unstable();
    if let Some(dup) = ids.windows(2).find(|w| w[0] == w[1]) {
        return Err(bad(format!(
            "lists generation {} TWICE in `sstable_generations` — a multiset is not a generation \
             set, and one duplicate hides one missing generation from the comparison",
            dup[0]
        )));
    }
    Ok(ExpectedGenerations { count, ids })
}

/// The `tables[]` entry for this manifest's own table: the corpus-relative directory it
/// documents AND the generation set inside it. `None` only when the manifest carries no
/// `tables` array at all (a hand-written minimal manifest); a `tables` array that exists
/// must describe this table, and must do so completely.
fn documented_table(
    json: &serde_json::Value,
    path: &Path,
    keyspace: &str,
    table: &str,
) -> std::result::Result<Option<DocumentedTable>, String> {
    let Some(v) = json.get("tables") else {
        return Ok(None);
    };
    let arr = v.as_array().ok_or_else(|| {
        format!(
            "{}: `tables` is present but is not an array",
            path.display()
        )
    })?;
    // EXACTLY ONE matching record, not "the first one" (roborev #3234 round-12 F3).
    // `.find()` accepted a `tables` array holding SEVERAL entries for this table and
    // silently took the earliest, so two records carrying DIFFERENT `sstable_dir`
    // values made the authoritative ingest scope a function of ARRAY ORDER: the
    // measurement would be confined to whichever copy happened to be written first,
    // with the other one — describing a different directory, and therefore possibly a
    // different generation count and a different scan route — never mentioned. A
    // manifest that describes this table twice does not have one authoritative answer
    // to "which directory were these counts read from", so it is refused with the
    // count it found rather than resolved by position.
    let matches: Vec<&serde_json::Value> = arr
        .iter()
        .filter(|t| t.get("table").and_then(|n| n.as_str()) == Some(table))
        .collect();
    let entry = match matches.as_slice() {
        [one] => *one,
        [] => {
            return Err(format!(
                "{}: `tables[]` has no entry for `{table}`, the table this manifest's own \
                 `table` field names — so the directory its counts were read from is unknown",
                path.display()
            ))
        }
        many => {
            return Err(format!(
                "{}: `tables[]` has {} entries for `{table}`, the table this manifest's own \
                 `table` field names — exactly one is required. The entry's `sstable_dir` is \
                 what SCOPES ingestion, so with duplicates the measured directory would depend \
                 on array ORDER; the dirs named are:\n    {}\n  Refusing.",
                path.display(),
                many.len(),
                many.iter()
                    .map(|t| t
                        .get("sstable_dir")
                        .and_then(|s| s.as_str())
                        .unwrap_or("(no string sstable_dir)")
                        .to_string())
                    .collect::<Vec<_>>()
                    .join("\n    ")
            ))
        }
    };
    let rel = entry
        .get("sstable_dir")
        .and_then(|s| s.as_str())
        .ok_or_else(|| {
            format!(
                "{}: `tables[]` entry for `{table}` has no string `sstable_dir`",
                path.display()
            )
        })?;
    // The directory is validated first, so a record naming an impossible path is
    // diagnosed as that rather than as a missing generation field.
    let sstable_dir_rel = validated_sstable_dir(rel, keyspace, table, path)?;
    Ok(Some(DocumentedTable {
        sstable_dir_rel,
        expected: expected_generations(entry, path, table)?,
    }))
}

/// The FIRST candidate manifest that is PRESENT, observed with `symlink_metadata` —
/// never `Path::exists()` (roborev job 27 B1).
///
/// `Path::exists()` answers `false` for a path it cannot look at, not just for one that is
/// absent: a broken symlink, a component that is not a directory, a parent directory it
/// may not traverse. Every one of those used to read as "this candidate is absent", so a
/// **present-but-unreadable corpus-local manifest silently degraded to the committed
/// fallback** — and the committed fallback describes a DIFFERENT corpus, so the harness
/// would then verify a row count against a manifest that does not describe the bytes it
/// scanned. An authority that cannot be read is a refusal, never a reason to pick a
/// different authority.
///
/// So exactly ONE outcome means "absent": `ErrorKind::NotFound`. Every other `lstat`
/// error, and every entry that is not a REGULAR FILE (a symlink — including a dangling
/// one — a directory, a fifo), is an authoritative manifest failure. A symlink is refused
/// rather than followed for the same reason `scope.rs` requires real directories: the file
/// that decides which numbers are authoritative must be the real thing, not a redirection.
fn locate_manifest(candidates: &[PathBuf]) -> std::result::Result<&PathBuf, String> {
    let mut absent: Vec<String> = Vec::new();
    for path in candidates {
        let md = match std::fs::symlink_metadata(path) {
            Ok(md) => md,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                absent.push(format!("    {} (absent)", path.display()));
                continue;
            }
            Err(e) => {
                return Err(format!(
                    "{}: cannot determine whether this manifest candidate exists (lstat failed: \
                     {e}). A candidate that cannot be OBSERVED must not be treated as absent: \
                     skipping it would fall through to another manifest, which describes a \
                     DIFFERENT corpus, and the row count would then be verified against bytes it \
                     does not describe.\n  remedy: fix the path/permissions, or pass --manifest \
                     PATH explicitly.",
                    path.display()
                ))
            }
        };
        if md.file_type().is_file() {
            return Ok(path);
        }
        let what = if md.file_type().is_symlink() {
            let target = std::fs::read_link(path)
                .map(|t| format!(" -> {}", t.display()))
                .unwrap_or_default();
            let dangling = if std::fs::metadata(path).is_err() {
                ", which does not resolve (DANGLING)"
            } else {
                ""
            };
            format!("a symbolic link{target}{dangling}")
        } else if md.file_type().is_dir() {
            "a directory".to_string()
        } else {
            format!(
                "neither a regular file nor a directory ({:?})",
                md.file_type()
            )
        };
        return Err(format!(
            "{}: this manifest candidate is PRESENT but is not a regular file — it is {what}. \
             `Path::exists()` reports such a path as absent, which used to fall through to the \
             next candidate: a manifest describing a DIFFERENT corpus would then have verified \
             this scan. An authority that cannot be read is a refusal.\n  remedy: replace it with \
             the real manifest file, or pass --manifest PATH explicitly.",
            path.display()
        ));
    }
    Err(format!(
        "no #3234 corpus manifest found, so the authoritative row count is unknown and a \
         truncated scan could not be detected.\n  looked at:\n{}\n  \
         remedy: pass --manifest PATH, or --expect-rows N, or --no-expect-rows to measure \
         without the truncation guard (reported loudly).",
        absent.join("\n")
    ))
}

/// Resolve the row-count assert and the documented ingest scope, fail-closed.
/// `Err(message)` => exit `MANIFEST_UNREADABLE`.
pub fn resolve_rows_assert(
    args: &crate::Args,
) -> std::result::Result<(RowsAssert, Option<ManifestScope>), String> {
    if args.expect_rows_off {
        return Ok((RowsAssert::Disabled, None));
    }
    if let Some(n) = args.expect_rows {
        return Ok((RowsAssert::Explicit(n), None));
    }

    let candidates: Vec<PathBuf> = match &args.manifest {
        Some(p) => vec![p.clone()],
        None => manifest_candidates(&args.corpus),
    };
    // The FIRST candidate that is PRESENT is authoritative, and "present" is OBSERVED
    // (`symlink_metadata`), not guessed at with `Path::exists()` (roborev job 27 B1). A
    // present-but-unreadable manifest is an error, never a reason to fall through to
    // another one — that fall-through is how a manifest describing a different corpus
    // ends up verifying this scan.
    let found = locate_manifest(&candidates)?;

    let (ks, tbl, rows, documented) = read_manifest(found)?;
    if ks != args.keyspace || tbl != args.table {
        return Err(format!(
            "{} describes {ks}.{tbl}, but this run scans {}.{} — its row count is not \
             authoritative here.\n  remedy: pass --manifest PATH for the right corpus, or \
             --expect-rows N, or --no-expect-rows.",
            found.display(),
            args.keyspace,
            args.table
        ));
    }
    Ok((
        RowsAssert::Manifest(rows, format!("{} rows_per_partition.rows", found.display())),
        documented.map(|d| ManifestScope {
            manifest: found.clone(),
            sstable_dir_rel: d.sstable_dir_rel,
            expected: d.expected,
        }),
    ))
}
