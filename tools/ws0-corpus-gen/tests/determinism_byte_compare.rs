//! REGENERATE-AND-BYTE-COMPARE: the determinism self-test the corpus's committed
//! claim rests on (issue #3272, item 8).
//!
//! # What was missing, and why prose was not enough
//!
//! "Byte-identical across three generations" is load-bearing for every future
//! comparison against this corpus — a lever measured against the recorded
//! `4a903f6f…` numbers is comparing against nothing if the generator is not
//! deterministic. But the claim existed only as PROSE (`README.md` §Determinism,
//! and the #3096 artifacts). The mechanism (`--verify-against`, fail-closed) and
//! the row-content unit tests were committed, and `identity::diff` is exhaustive
//! by destructure — but **nothing regenerated the corpus and byte-compared two
//! independent generations.** A generator that quietly acquired a wall-clock
//! field, a HashMap iteration order, or a thread-count-dependent buffer boundary
//! would keep every one of those green.
//!
//! This file closes that: it runs the SAME `generate()` twice, into two
//! independent output roots, and compares the RAW BYTES of every emitted file.
//!
//! # What it proves, and what it does NOT (the scaling, stated explicitly)
//!
//! It runs at [`SMALL_ROWS`] rows, **not** the measurement corpus's 4,000,000.
//! It cannot: a 4M-row generation writes ~2.8 GB and takes minutes, which no gate
//! component may do. So, precisely:
//!
//! * **PROVES**: the generator is a pure function of its inputs on the SAME code
//!   path the measurement corpus uses — same `generate()`, same `SSTableWriter`,
//!   same seed plumbing, same row synthesis, same token ordering, same component
//!   set. A non-determinism introduced anywhere in that path (wall-clock in a
//!   cell or in `Statistics.db`, an unordered map in the writer's metadata, a
//!   dependency's RNG, an uninitialised buffer tail) reproduces at 1,000 rows
//!   exactly as it would at 4,000,000, because none of those defects is
//!   size-conditional.
//! * **DOES NOT PROVE**: that the 4,000,000-row corpus reproduces its recorded
//!   `4a903f6f…` digest. That is a SIZE-SPECIFIC fact about a specific artifact,
//!   and it is pinned separately — see `measurement_corpus_pin.rs` and
//!   [`ws0_corpus_gen::measurement_corpus`], whose full-size verification is an
//!   `#[ignore]`d operator procedure. A defect that only appears past some
//!   multi-GB threshold (a 32-bit offset overflow, a chunk-boundary edge at a
//!   size this test never reaches) is out of this test's reach BY CONSTRUCTION.
//!
//! # A DETERMINISM oracle, never a CORRECTNESS one (issue #3042)
//!
//! Everything here is CQLite-written and CQLite-read, so per #3042 it is
//! INVARIANT to a uniform framing/serialization error: both generations make the
//! identical mistake and the comparison closes. That is FINE for the property
//! under test — "the same inputs produce the same bytes" is a self-consistency
//! claim, and self-consistency is exactly what a symmetric fixture CAN establish.
//! It is emphatically NOT a claim that those bytes are the RIGHT bytes. The
//! corpus is a **PERFORMANCE FIXTURE ONLY**; on-disk framing correctness stays
//! anchored to the Cassandra-written fixtures (`test-data/datasets/`).
//!
//! # Non-vacuity (per #3249: a guard not observed failing is not evidence)
//!
//! A byte comparison that would pass on ANY two inputs proves nothing, and
//! `assert_eq!` on two `Vec<u8>` is precisely the kind of assertion that looks
//! like evidence while being satisfiable by a bug that makes both sides empty.
//! So this file proves its own comparator can FAIL, three ways:
//!
//! * [`a_different_seed_produces_different_bytes`] — a perturbed INPUT diverges,
//!   so the equality above is not vacuous on the generator side.
//! * [`the_comparator_reports_a_single_flipped_byte`] — a one-bit perturbation of
//!   an otherwise identical tree is REPORTED, so the equality is not vacuous on
//!   the comparator side.
//! * [`the_comparator_reports_a_missing_or_extra_component`] — set-level
//!   divergence is reported, so "Data.db matched" cannot stand in for
//!   "the corpus matched".
//!
//! And [`the_recorded_digest_matches_an_independently_computed_one`] closes the
//! circularity the issue names: every comparison here is over bytes THIS TEST
//! hashed off disk, never over a digest the generator computed about itself, and
//! that test is what proves the generator's self-report agrees with an
//! independent hash of the file it describes.

use std::collections::BTreeMap;
use std::path::Path;

use sha2::{Digest, Sha256};

use ws0_corpus_gen::generate::{generate, has_data_db, CorpusSpec, DEFAULT_SEED};

/// Rows per generation. 10 partitions x 100 rows — the SAME
/// `rows_per_partition` as the measurement corpus, so the per-partition write
/// path (and its clustering-row loop) is the production one; only the partition
/// COUNT is scaled down. ~700 KB per generation, ~0.1 s.
const SMALL_ROWS: u64 = 1_000;

/// One emitted file, as THIS TEST measured it off disk.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FileBytes {
    len: u64,
    sha256: String,
    bytes: Vec<u8>,
}

/// Read every regular file directly inside `dir`, keyed by file name.
///
/// Deliberately reads the RAW BYTES rather than asking the generator for a
/// digest: a self-computed digest cannot corroborate itself. `sha256` is carried
/// alongside only so a failure message can NAME the divergence compactly — the
/// comparison itself is over `bytes`.
fn read_tree(dir: &Path) -> BTreeMap<String, FileBytes> {
    let mut out = BTreeMap::new();
    let entries = std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {dir:?}: {e}"));
    for entry in entries {
        let entry = entry.expect("dir entry");
        if !entry.file_type().expect("file type").is_file() {
            continue;
        }
        let path = entry.path();
        let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        out.insert(
            entry.file_name().to_string_lossy().to_string(),
            FileBytes {
                len: bytes.len() as u64,
                sha256: format!("{:x}", hasher.finalize()),
                bytes,
            },
        );
    }
    out
}

/// Every way two trees differ, as human-readable lines. EMPTY means byte-identical.
///
/// Reports set differences AND content differences, in that order, so a missing
/// component can never be masked by a matching `Data.db`.
fn diff_trees(a: &BTreeMap<String, FileBytes>, b: &BTreeMap<String, FileBytes>) -> Vec<String> {
    let mut out = Vec::new();
    for (name, fa) in a {
        match b.get(name) {
            None => out.push(format!("{name}: present in A, MISSING from B")),
            Some(fb) if fa.len != fb.len => out.push(format!(
                "{name}: {} bytes in A, {} bytes in B",
                fa.len, fb.len
            )),
            Some(fb) if fa.bytes != fb.bytes => {
                let at = fa
                    .bytes
                    .iter()
                    .zip(fb.bytes.iter())
                    .position(|(x, y)| x != y)
                    .unwrap_or(0);
                out.push(format!(
                    "{name}: {} bytes both sides but BYTES DIFFER, first at offset {at} \
                     (A sha256 {}, B sha256 {})",
                    fa.len, fa.sha256, fb.sha256
                ));
            }
            Some(_) => {}
        }
    }
    for name in b.keys() {
        if !a.contains_key(name) {
            out.push(format!("{name}: MISSING from A, present in B"));
        }
    }
    out
}

/// Generate into a fresh temp root and return `(tempdir, spec, identity)`.
///
/// `tempfile::TempDir` is returned (not dropped) so the caller controls the
/// lifetime — a dropped `TempDir` deletes the corpus the caller is about to hash.
async fn gen_at(
    rows: u64,
    seed: u64,
) -> (
    tempfile::TempDir,
    CorpusSpec,
    ws0_corpus_gen::identity::CorpusIdentity,
) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut spec = CorpusSpec::small(dir.path().to_path_buf(), rows);
    spec.seed = seed;
    let identity = generate(&spec)
        .await
        .unwrap_or_else(|e| panic!("generate rows={rows} seed={seed}: {e}"));
    (dir, spec, identity)
}

/// THE determinism assertion: two INDEPENDENT generations, byte-compared.
///
/// Two output roots, two `generate()` calls, no shared state beyond the pinned
/// inputs. Every emitted component — `Data.db`, `Index.db`, `Summary.db`,
/// `Filter.db`, `Statistics.db`, `CRC.db`, `Digest.crc32`, `TOC.txt` — must be
/// byte-identical, and the component SET must match, so a determinism defect
/// localized to (say) `Statistics.db`'s min/max timestamps cannot hide behind a
/// matching `Data.db`.
#[tokio::test]
async fn two_independent_generations_are_byte_identical() {
    let (dir_a, spec_a, id_a) = gen_at(SMALL_ROWS, DEFAULT_SEED).await;
    let (dir_b, spec_b, id_b) = gen_at(SMALL_ROWS, DEFAULT_SEED).await;

    // FAIL CLOSED on a vacuous corpus: an equality over two EMPTY trees is the
    // canonical vacuous pass, and it is the shape a broken generator would take.
    assert!(
        has_data_db(&spec_a.table_dir()),
        "generation A wrote no *-Data.db — an empty-vs-empty comparison is not evidence"
    );
    assert_eq!(id_a.rows, SMALL_ROWS, "generation A row count");
    assert!(id_a.data_db_bytes > 0, "generation A Data.db is empty");

    let a = read_tree(&spec_a.table_dir());
    let b = read_tree(&spec_b.table_dir());
    assert!(
        a.len() >= 8,
        "expected the full BIG component set (>=8 files), got {:?}",
        a.keys().collect::<Vec<_>>()
    );
    assert!(
        !a.keys().any(|n| n.ends_with("CompressionInfo.db")),
        "the corpus must be UNCOMPRESSED (issue #1406); got {:?}",
        a.keys().collect::<Vec<_>>()
    );

    let diffs = diff_trees(&a, &b);
    assert!(
        diffs.is_empty(),
        "two generations from seed {DEFAULT_SEED} at {SMALL_ROWS} rows are NOT byte-identical \
         — the committed determinism claim is false:\n  {}",
        diffs.join("\n  ")
    );

    // The DDL emitted beside the corpus is part of what a consumer reads, so it is
    // compared too (both arms of the measurement derive their ticket from it).
    let ddl_a = std::fs::read(spec_a.out.join("ws0-events.cql")).expect("DDL A");
    let ddl_b = std::fs::read(spec_b.out.join("ws0-events.cql")).expect("DDL B");
    assert_eq!(ddl_a, ddl_b, "the emitted DDL differs between generations");

    // And the recorded identities agree field-for-field (the `diff` that
    // `--verify-against` uses, exercised over two REAL generations rather than
    // hand-built structs).
    let id_diffs = id_b.diff(&id_a);
    assert!(
        id_diffs.is_empty(),
        "the recorded identities of two identical generations diverge: {id_diffs:?}"
    );

    eprintln!(
        "determinism: {} components byte-identical across 2 generations at {} rows \
         (Data.db {} B, sha256 {})",
        a.len(),
        SMALL_ROWS,
        id_a.data_db_bytes,
        id_a.data_db_sha256
    );
    drop((dir_a, dir_b));
}

/// A THIRD generation, because the committed claim says "three generations".
///
/// Separate from the pairwise test on purpose: transitivity is not free. A
/// generator with a two-state alternation (a static `AtomicBool`, a cached
/// buffer reused on even calls) reproduces on generations 1 and 3 while
/// generation 2 differs, and a pairwise-only test picked from the wrong pair
/// would miss it.
#[tokio::test]
async fn three_generations_are_mutually_byte_identical() {
    let (d1, s1, _) = gen_at(SMALL_ROWS, DEFAULT_SEED).await;
    let (d2, s2, _) = gen_at(SMALL_ROWS, DEFAULT_SEED).await;
    let (d3, s3, _) = gen_at(SMALL_ROWS, DEFAULT_SEED).await;
    let trees = [
        ("1", read_tree(&s1.table_dir())),
        ("2", read_tree(&s2.table_dir())),
        ("3", read_tree(&s3.table_dir())),
    ];
    assert!(!trees[0].1.is_empty(), "generation 1 emitted no files");
    for i in 0..trees.len() {
        for j in (i + 1)..trees.len() {
            let diffs = diff_trees(&trees[i].1, &trees[j].1);
            assert!(
                diffs.is_empty(),
                "generations {} and {} differ:\n  {}",
                trees[i].0,
                trees[j].0,
                diffs.join("\n  ")
            );
        }
    }
    drop((d1, d2, d3));
}

/// NON-VACUITY, input side: a DIFFERENT SEED must produce DIFFERENT bytes.
///
/// Without this, `two_independent_generations_are_byte_identical` would also pass
/// against a generator that ignored its seed entirely — or one that wrote a
/// constant corpus, or nothing at all. It rules out a comparison satisfiable by
/// any two inputs.
#[tokio::test]
async fn a_different_seed_produces_different_bytes() {
    let (d_a, s_a, id_a) = gen_at(SMALL_ROWS, DEFAULT_SEED).await;
    let (d_b, s_b, id_b) = gen_at(SMALL_ROWS, DEFAULT_SEED + 1).await;
    let a = read_tree(&s_a.table_dir());
    let b = read_tree(&s_b.table_dir());

    let diffs = diff_trees(&a, &b);
    assert!(
        !diffs.is_empty(),
        "two DIFFERENT seeds produced byte-identical corpora — the byte comparison in this \
         file would pass on any two inputs and is therefore not evidence of determinism"
    );
    // And specifically the artifact both measurement arms read.
    let data_name = a
        .keys()
        .find(|n| n.ends_with("-Data.db"))
        .expect("a Data.db")
        .clone();
    assert_ne!(
        a[&data_name].bytes, b[&data_name].bytes,
        "Data.db is identical across two different seeds"
    );
    assert_ne!(
        id_a.data_db_sha256, id_b.data_db_sha256,
        "the recorded Data.db digest is identical across two different seeds"
    );
    drop((d_a, d_b));
}

/// NON-VACUITY, comparator side: a ONE-BYTE perturbation is REPORTED.
///
/// The generator is not involved. This mutates a COPY of an otherwise identical
/// tree and asserts `diff_trees` names the file and the offset — so the empty
/// `diffs` above means "checked and equal", not "the comparator cannot see".
#[tokio::test]
async fn the_comparator_reports_a_single_flipped_byte() {
    let (dir, spec, _) = gen_at(SMALL_ROWS, DEFAULT_SEED).await;
    let a = read_tree(&spec.table_dir());
    let data_name = a
        .keys()
        .find(|n| n.ends_with("-Data.db"))
        .expect("a Data.db")
        .clone();

    // Flip ONE bit in the middle of Data.db, in memory, leaving the length equal —
    // so the length check cannot be what catches it.
    let mut b = a.clone();
    let entry = b.get_mut(&data_name).expect("Data.db entry");
    let mid = entry.bytes.len() / 2;
    entry.bytes[mid] ^= 0x01;
    let mut hasher = Sha256::new();
    hasher.update(&entry.bytes);
    entry.sha256 = format!("{:x}", hasher.finalize());
    assert_eq!(
        entry.len, a[&data_name].len,
        "the perturbation must not change the length"
    );

    let diffs = diff_trees(&a, &b);
    assert_eq!(
        diffs.len(),
        1,
        "expected exactly one reported divergence, got {diffs:?}"
    );
    assert!(
        diffs[0].contains(&data_name) && diffs[0].contains("BYTES DIFFER"),
        "the divergence must name the file and say the bytes differ: {diffs:?}"
    );
    assert!(
        diffs[0].contains(&format!("offset {mid}")),
        "the divergence must name the first differing offset {mid}: {diffs:?}"
    );
    drop(dir);
}

/// NON-VACUITY, set side: a MISSING and an EXTRA component are both reported.
///
/// The failure this rules out is the one `identity::diff` was hardened for at the
/// field level (#3096 finding 3): "Data.db matched" reported as "the corpus
/// reproduced" while the component SET moved — e.g. a stray `CompressionInfo.db`
/// (#1406) or a dropped `Summary.db`.
#[tokio::test]
async fn the_comparator_reports_a_missing_or_extra_component() {
    let (dir, spec, _) = gen_at(SMALL_ROWS, DEFAULT_SEED).await;
    let a = read_tree(&spec.table_dir());
    let summary = a
        .keys()
        .find(|n| n.ends_with("-Summary.db"))
        .expect("a Summary.db")
        .clone();

    let mut b = a.clone();
    b.remove(&summary);
    let diffs = diff_trees(&a, &b);
    assert!(
        diffs
            .iter()
            .any(|m| m.contains(&summary) && m.contains("MISSING from B")),
        "a removed component must be reported: {diffs:?}"
    );

    let mut c = a.clone();
    c.insert(
        "nb-1-big-CompressionInfo.db".to_string(),
        FileBytes {
            len: 3,
            sha256: "unused".to_string(),
            bytes: vec![1, 2, 3],
        },
    );
    let diffs = diff_trees(&a, &c);
    assert!(
        diffs
            .iter()
            .any(|m| m.contains("CompressionInfo.db") && m.contains("MISSING from A")),
        "an added component must be reported: {diffs:?}"
    );
    drop(dir);
}

/// The generator's SELF-REPORTED digest is corroborated by an INDEPENDENT hash.
///
/// This is the anti-circularity assertion the issue asks for: every comparison in
/// this file is over bytes read off disk here, and this test is what establishes
/// that the `data_db_sha256` the generator records — the value the committed
/// `corpus-identity.json` and every `--verify-against` run compare against — is a
/// true statement about the file, not a number the generator made up about
/// itself. `bytes` and `bytes_per_row` are checked in the same currency.
#[tokio::test]
async fn the_recorded_digest_matches_an_independently_computed_one() {
    let (dir, spec, id) = gen_at(SMALL_ROWS, DEFAULT_SEED).await;
    let tree = read_tree(&spec.table_dir());
    let data = tree
        .iter()
        .find(|(n, _)| n.ends_with("-Data.db"))
        .map(|(_, f)| f)
        .expect("a Data.db");

    assert_eq!(
        id.data_db_sha256, data.sha256,
        "the RECORDED Data.db sha256 disagrees with an independent hash of the file"
    );
    assert_eq!(
        id.data_db_bytes, data.len,
        "the RECORDED Data.db byte count disagrees with the file's actual length"
    );
    let expect_bpr = data.len as f64 / SMALL_ROWS as f64;
    assert!(
        (id.bytes_per_row - expect_bpr).abs() < 1e-9,
        "recorded bytes_per_row {} != measured {expect_bpr}",
        id.bytes_per_row
    );
    // And the whole component census, so a recorded component digest cannot be
    // wrong while Data.db happens to be right.
    for (name, recorded) in &id.components {
        let observed = tree
            .get(name)
            .unwrap_or_else(|| panic!("recorded component {name} is not on disk"));
        assert_eq!(
            recorded.sha256, observed.sha256,
            "recorded sha256 for {name} disagrees with an independent hash"
        );
        assert_eq!(
            recorded.bytes, observed.len,
            "recorded size for {name} disagrees with the file"
        );
    }
    assert_eq!(
        id.components.len(),
        tree.len(),
        "the recorded component set and the on-disk set differ in size"
    );
    drop(dir);
}
