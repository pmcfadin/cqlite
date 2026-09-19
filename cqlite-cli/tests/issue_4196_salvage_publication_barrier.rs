//! Issue #4196 — CLI exit-code contracts through the compiled `cqlite` binary:
//! the `-TOC.txt` PUBLICATION BARRIER (roborev round-22 Low finding, R7.7) and
//! R7.2's PARTIAL-loss arm (the C intent audit's B2), each with its own control
//! leg so no exit code can be dismissed as its fixture's permanent outcome.
//!
//! `discover_salvage_inputs`'s single-FILE branch used to return with no
//! `-TOC.txt` probe at all, while its DIRECTORY branch names a barrier-less
//! generation as a `SkippedInput` that lands in the manifest and forces exit 3.
//! So `salvage ./ks/t-<id>/nb-3-big-Data.db` salvaged an unpublished generation
//! SILENTLY, while `salvage ./ks/t-<id>/` on the same file named it and refused —
//! an asymmetry nothing declared.
//!
//! The resolution (design D3, and `discover_salvage_inputs`'s own doc): an
//! explicit file path OVERRIDES the barrier — recovering an unpublished,
//! partially-flushed generation is a legitimate thing for an operator to ask for
//! — but the absence is RECORDED as an `UnpublishedInputGeneration` component
//! finding and counts as a verification gap, so `$?` is the same 3 for the same
//! file whichever way it is named. The difference is that the file form still
//! recovers the data.
//!
//! # Why its own test target
//!
//! `salvage_cli_tests.rs` is the home of this issue's CLI cases, but it sits at
//! ~1420 of the ~1500-line test-file threshold the gate's `file-size` ratchet
//! enforces (epic #1135), so these cases would push it over. Extracting that
//! file's helper block into a shared `tests/support/` module is a separate,
//! purely mechanical change; until then the helpers below are local. They
//! are deliberately minimal — the fixture-resolution DOCTRINE (issue #3220:
//! committed fixtures fail closed, per case) is the part that matters and is
//! preserved.

// `not(tombstones)`: mirrors `commands::salvage`'s own module gate — the binary
// this file drives via `CARGO_BIN_EXE_cqlite` has no `salvage` verb at all when
// `tombstones` is on.
#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::TempDir;

/// A fixture generation whose every component is git-TRACKED, so its absence is
/// a broken checkout and never an unfetched dataset — the resolver below fails
/// closed UNCONDITIONALLY, not gated on `CQLITE_REQUIRE_FIXTURES` (issue #3220).
struct CommittedFixture {
    /// Path to the generation directory, relative to a datasets base root.
    relative: &'static str,
    /// Every component the staging copy needs. "The directory resolved" is not
    /// "the fixture is usable", so each one is asserted present.
    components: &'static [&'static str],
    /// The committed CQL schema file under `test-data/schemas/`.
    schema_file: &'static str,
}

/// `test_comp.lz4_table` — BIG (`nb`), LZ4, ONE partition.
const LZ4_TABLE: CommittedFixture = CommittedFixture {
    relative: "sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77",
    components: &[
        "nb-1-big-Data.db",
        "nb-1-big-Index.db",
        "nb-1-big-Summary.db",
        "nb-1-big-Statistics.db",
        "nb-1-big-CompressionInfo.db",
        "nb-1-big-Filter.db",
        "nb-1-big-Digest.crc32",
        "nb-1-big-TOC.txt",
    ],
    schema_file: "compression-parity.cql",
};

/// `test_da.multiclustering_table` — BTI (`da`), LZ4, THREE partitions. The only
/// committed fixture in this crate's reach that can exhibit a PARTIAL loss (see
/// `damaged_input_exits_3_with_losses_and_a_complete_generation_set`).
const BTI_MULTICLUSTERING: CommittedFixture = CommittedFixture {
    relative: "sstables/test_da/multiclustering_table-fd74ad508d2311f1a29b6d2c15dcffdf",
    components: &[
        "da-2-bti-Data.db",
        "da-2-bti-Partitions.db",
        "da-2-bti-Rows.db",
        "da-2-bti-Statistics.db",
        "da-2-bti-CompressionInfo.db",
        "da-2-bti-Filter.db",
        "da-2-bti-Digest.crc32",
        "da-2-bti-TOC.txt",
    ],
    schema_file: "multiclustering-table-bti.cql",
};

/// Every candidate base root (the `CQLITE_DATASETS_ROOT` corpus, then the
/// checkout's own committed corpus — issue #3220: neither is a superset).
fn candidate_base_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Ok(env_root) = std::env::var("CQLITE_DATASETS_ROOT") {
        roots.push(PathBuf::from(env_root));
    }
    let checkout = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("repo root")
        .join("test-data/datasets");
    if !roots.contains(&checkout) {
        roots.push(checkout);
    }
    roots
}

/// The first candidate root carrying EVERY named component, or a hard failure
/// naming the roots searched.
fn resolve_committed_fixture(fixture: &CommittedFixture) -> PathBuf {
    let roots = candidate_base_roots();
    for root in &roots {
        let dir = root.join(fixture.relative);
        if fixture.components.iter().all(|c| dir.join(c).is_file()) {
            return dir;
        }
    }
    panic!(
        "COMMITTED fixture {} (components {:?}) is absent — its binaries are git-tracked, so this \
         is a broken checkout, NOT an unfetched dataset, and must never skip (issue #3220). \
         Searched: {roots:?}",
        fixture.relative, fixture.components
    );
}

/// Copy the fixture's real components (never the `.jsonl`/`.db.txt` sidecars)
/// into a fresh staged table directory named as Cassandra would.
fn stage_generation(fixture: &CommittedFixture, clean_dir: &Path, dest_parent: &Path) -> PathBuf {
    let dir_name = Path::new(fixture.relative)
        .file_name()
        .expect("fixture path names a generation directory");
    let dest = dest_parent.join(dir_name);
    std::fs::create_dir_all(&dest).unwrap_or_else(|e| panic!("create {dest:?}: {e}"));
    for component in fixture.components {
        std::fs::copy(clean_dir.join(component), dest.join(component))
            .unwrap_or_else(|e| panic!("copy {component}: {e}"));
    }
    dest
}

fn schema_path(fixture: &CommittedFixture) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("repo root")
        .join("test-data/schemas")
        .join(fixture.schema_file)
}

fn run_salvage_with(fixture: &CommittedFixture, input: &Path, out: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args([
            "--schema",
            schema_path(fixture).to_str().expect("schema path is utf-8"),
            "salvage",
            input.to_str().expect("input path is utf-8"),
            "--out",
            out.to_str().expect("out path is utf-8"),
            "--out-format",
            "json",
        ])
        .output()
        .expect("failed to execute cqlite binary")
}

fn run_salvage(input: &Path, out: &Path) -> Output {
    run_salvage_with(&LZ4_TABLE, input, out)
}

/// A single-`Data.db` input's manifest is a bare OBJECT (design D5's shape),
/// never an array.
fn manifest_object(stdout: &str) -> serde_json::Value {
    let manifest: serde_json::Value = serde_json::from_str(stdout)
        .unwrap_or_else(|e| panic!("stdout is not JSON: {e}\n{stdout}"));
    assert!(
        manifest.is_object(),
        "a single-Data.db input must emit a bare manifest OBJECT (D5); got {manifest}"
    );
    manifest
}

fn finding_classes(manifest: &serde_json::Value) -> Vec<String> {
    manifest
        .get("component_findings")
        .and_then(|f| f.as_array())
        .unwrap_or_else(|| panic!("manifest missing 'component_findings': {manifest}"))
        .iter()
        .map(|f| {
            f.get("class")
                .and_then(|c| c.as_str())
                .unwrap_or_else(|| panic!("finding missing 'class': {f}"))
                .to_string()
        })
        .collect()
}

/// The gap leg: an explicitly-named `Data.db` whose `-TOC.txt` sibling is gone
/// is SALVAGED, the missing barrier is NAMED in the manifest, and the run exits
/// 3 rather than 0.
#[test]
fn explicit_data_db_without_toc_txt_is_salvaged_named_and_exits_3() {
    let clean_dir = resolve_committed_fixture(&LZ4_TABLE);
    let temp = TempDir::new().expect("tempdir");
    let input_dir = stage_generation(&LZ4_TABLE, &clean_dir, temp.path());
    let toc = input_dir.join("nb-1-big-TOC.txt");
    std::fs::remove_file(&toc).unwrap_or_else(|e| panic!("remove {toc:?}: {e}"));
    assert!(
        !toc.exists(),
        "the gap leg requires the barrier to be ABSENT"
    );

    let out = temp.path().join("out");
    let output = run_salvage(&input_dir.join("nb-1-big-Data.db"), &out);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(
        output.status.code(),
        Some(3),
        "an unpublished generation is salvaged, but its unverifiable publication barrier makes the \
         run imperfect — the same file reached through its table DIRECTORY already exits 3, so a 0 \
         here means `$?` depends on how the operator spelled the input; stdout={stdout}\n\
         stderr={stderr}"
    );

    let manifest = manifest_object(&stdout);
    let classes = finding_classes(&manifest);
    assert!(
        classes.iter().any(|c| c == "UnpublishedInputGeneration"),
        "the manifest must NAME the absent publication barrier — the exit code alone does not tell \
         an operator which premise went unverified; classes={classes:?} manifest={manifest}"
    );
    // The barrier's absence is not a LOSS: every partition is still recovered
    // and the generation is written.
    assert_eq!(
        manifest
            .get("losses")
            .and_then(|l| l.as_array())
            .map(Vec::len),
        Some(0),
        "an absent TOC.txt must not be reported as a partition loss; manifest={manifest}"
    );
    assert!(
        data_db_exists_under(&out),
        "the data must still be recovered — an explicit file path OVERRIDES the barrier, it does \
         not refuse; nothing found under {out:?}"
    );
}

/// The control leg: the SAME staged generation WITH its `-TOC.txt` exits 0 and
/// records no barrier finding. Without it, "exit 3" could be this fixture's
/// permanent outcome and the gap leg would prove nothing.
#[test]
fn explicit_data_db_with_toc_txt_exits_0_and_records_no_barrier_finding() {
    let clean_dir = resolve_committed_fixture(&LZ4_TABLE);
    let temp = TempDir::new().expect("tempdir");
    let input_dir = stage_generation(&LZ4_TABLE, &clean_dir, temp.path());
    assert!(
        input_dir.join("nb-1-big-TOC.txt").is_file(),
        "the control leg requires the barrier to be PRESENT"
    );

    let out = temp.path().join("out");
    let output = run_salvage(&input_dir.join("nb-1-big-Data.db"), &out);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        output.status.code(),
        Some(0),
        "a published, healthy generation must exit 0; stdout={stdout}\nstderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let classes = finding_classes(&manifest_object(&stdout));
    assert!(
        !classes.iter().any(|c| c == "UnpublishedInputGeneration"),
        "a published generation has no absent barrier to report; classes={classes:?}"
    );
    assert!(data_db_exists_under(&out));
}

// ---------------------------------------------------------------------------
// R7.2's PARTIAL-loss arm — the C-audit's B2.
//
// Design D3's central row is "some partitions lost, some recovered -> exit 3
// AND a complete generation set is still written". `salvage_cli_tests.rs`'s
// R7.2 case accepts `2 || 3` and its fixture (`data_db_bit_flip`, ONE
// partition entirely inside the flipped chunk) yields 2, so the exit-3 branch
// there NEVER executes and D3's central row was unexercised end-to-end.
//
// # The fixture, and why the loss set is DERIVED and not observed
//
// `test_da.multiclustering_table` is the only fully git-tracked fixture in this
// crate's reach with more than one partition: THREE, whose uncompressed
// data-section positions Cassandra's own `sstabledump` recorded in the
// committed `da-2-bti-Data.db.jsonl` golden (0, 22380, 41844 over a 57548-byte
// section, 16384-byte compressed chunks). So the FIRST chunk whose decompressed
// range starts at or after the LAST partition's own start intersects THAT
// PARTITION AND NO OTHER — a range fact from two Cassandra-written components
// (the golden and `CompressionInfo.db`), computed before the corruption and
// never read back out of CQLite's output (#3041/#3042).
//
// Corrupting one byte of that chunk's payload while leaving its trailing CRC32
// alone is exactly what bit-rot on disk looks like to the reader, and is the
// same corruption model the core-level R2.1 case uses. It needs no
// decompression and no CRC arithmetic, so no new dependency: the mutation is a
// single byte written into a staged copy.
// ---------------------------------------------------------------------------

/// `(chunk_length, data_length, chunk_offsets)` from `CompressionInfo.db`.
///
/// Authority: `org.apache.cassandra.io.compress.CompressionMetadata`
/// (cassandra-5.0.8) — compressor name (`writeUTF`), the options map, then
/// `chunkLength`, `maxCompressedLength`, `dataLength` and the chunk-offset
/// array. The parse is asserted to consume the file EXACTLY, so a fixture whose
/// header shape changed fails by name instead of yielding plausible garbage.
fn compression_chunk_table(path: &Path) -> (usize, u64, Vec<u64>) {
    let b = std::fs::read(path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    let be16 = |o: usize| u16::from_be_bytes([b[o], b[o + 1]]) as usize;
    let be32 = |o: usize| {
        u32::from_be_bytes(b[o..o + 4].try_into().expect("4-byte big-endian field")) as usize
    };
    let be64 = |o: usize| u64::from_be_bytes(b[o..o + 8].try_into().expect("8-byte field"));
    let name_len = be16(0);
    let mut o = 2 + name_len;
    let options = be32(o);
    o += 4;
    for _ in 0..options {
        let kl = be16(o);
        o += 2 + kl;
        let vl = be16(o);
        o += 2 + vl;
    }
    let chunk_length = be32(o);
    o += 4 + 4; // chunkLength, maxCompressedLength
    let data_length = be64(o);
    o += 8;
    let count = be32(o);
    o += 4;
    let offsets: Vec<u64> = (0..count).map(|i| be64(o + i * 8)).collect();
    assert_eq!(
        o + count * 8,
        b.len(),
        "{path:?}: the chunk table parse consumed {} of {} bytes — the header shape is not the one \
         this derivation assumes, so every offset below would be wrong",
        o + count * 8,
        b.len()
    );
    assert!(
        chunk_length > 0 && !offsets.is_empty(),
        "{path:?}: degenerate chunk table (chunk_length={chunk_length}, {} offsets)",
        offsets.len()
    );
    (chunk_length, data_length, offsets)
}

/// Every `(partition key as the declared `int` pk, uncompressed data-section
/// position)` pair the committed `sstabledump` golden records, in file order.
///
/// This is Cassandra's OWN record of where each partition starts. Strict: a
/// drifted golden shape, a non-ascending position list or fewer than two
/// partitions each FAIL by name rather than yield a plausible-but-wrong needle.
fn golden_partition_positions(fixture_dir: &Path) -> Vec<(i32, u64)> {
    let golden = std::fs::read_dir(fixture_dir)
        .unwrap_or_else(|e| panic!("read {fixture_dir:?}: {e}"))
        .flatten()
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.ends_with("-Data.db.jsonl"))
                .unwrap_or(false)
        })
        .unwrap_or_else(|| {
            panic!(
                "{fixture_dir:?}: no *-Data.db.jsonl sstabledump golden — it is git-tracked beside \
                 the Data.db and is THE Cassandra-written oracle for these positions"
            )
        });
    let text = std::fs::read_to_string(&golden).unwrap_or_else(|e| panic!("read {golden:?}: {e}"));
    let mut out: Vec<(i32, u64)> = Vec::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let v: serde_json::Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("{golden:?}: not JSON: {e}"));
        let partition = v
            .get("partition")
            .unwrap_or_else(|| panic!("{golden:?}: partition object missing"));
        let key = partition
            .get("key")
            .and_then(|k| k.as_array())
            .unwrap_or_else(|| panic!("{golden:?}: partition key missing"));
        assert_eq!(
            key.len(),
            1,
            "{golden:?}: this table declares a SINGLE-column `pk int` partition key; a \
             {}-component key means this derivation does not apply to this fixture",
            key.len()
        );
        let pk: i32 = key[0]
            .as_str()
            .unwrap_or_else(|| panic!("{golden:?}: sstabledump renders an `int` pk as a string"))
            .parse()
            .unwrap_or_else(|e| panic!("{golden:?}: pk is not an i32: {e}"));
        let position = partition
            .get("position")
            .and_then(|p| p.as_u64())
            .unwrap_or_else(|| panic!("{golden:?}: partition {pk} has no `position`"));
        if let Some((prev_pk, prev)) = out.last() {
            assert!(
                position > *prev,
                "{golden:?}: positions must ASCEND (pk {pk} at {position} follows pk {prev_pk} at \
                 {prev})"
            );
        }
        out.push((pk, position));
    }
    assert!(
        out.len() >= 2,
        "{golden:?}: this case needs at least TWO partitions — with one, 'some lost, some \
         recovered' is unobservable and the exit code could only ever be 2; got {}",
        out.len()
    );
    out
}

/// Flip one byte inside compressed chunk `chunk_index`'s PAYLOAD, leaving that
/// chunk's trailing CRC32 untouched — so the chunk fails its integrity check
/// exactly as a rotted disk byte would. Returns the absolute `Data.db` offset
/// changed.
fn break_one_chunk_crc(data_db: &Path, offsets: &[u64], chunk_index: usize) -> u64 {
    let mut bytes = std::fs::read(data_db).unwrap_or_else(|e| panic!("read {data_db:?}: {e}"));
    let start = offsets[chunk_index] as usize;
    let end = offsets
        .get(chunk_index + 1)
        .map(|&o| o as usize)
        .unwrap_or(bytes.len());
    // Each chunk is `[compressed payload][4-byte CRC32]`.
    assert!(
        end > start + 4,
        "{data_db:?}: chunk {chunk_index} spans [{start}, {end}) — too short to hold a payload \
         byte plus its 4-byte CRC trailer"
    );
    let at = start + (end - 4 - start) / 2;
    let before = bytes[at];
    bytes[at] = !before;
    std::fs::write(data_db, &bytes).unwrap_or_else(|e| panic!("write {data_db:?}: {e}"));
    at as u64
}

/// The `<out>/<keyspace>/<table>/` directory the writer nests its generation in.
fn output_table_dir(out: &Path, keyspace: &str, table: &str) -> PathBuf {
    let dir = out.join(keyspace).join(table);
    assert!(
        dir.is_dir(),
        "expected the salvaged generation under {dir:?}; --out holds {:?}",
        std::fs::read_dir(out)
            .map(|rd| rd.flatten().map(|e| e.file_name()).collect::<Vec<_>>())
            .unwrap_or_default()
    );
    dir
}

/// R7.2 / design D3's central row: an input with SOME partitions lost and SOME
/// recovered exits **3** — not 2, not 0 — writes the manifest naming every
/// loss, and STILL writes a complete generation set.
///
/// Both premises that make the assertion meaningful are themselves asserted:
/// the fixture really holds more than one partition (from the golden), and the
/// corrupted chunk's decompressed range really begins at or after the last
/// partition's own start (from `CompressionInfo.db`), so exactly one partition
/// can intersect it. Nothing here reads its expectation back out of CQLite's
/// own output.
#[test]
fn damaged_input_exits_3_with_losses_and_a_complete_generation_set() {
    let clean_dir = resolve_committed_fixture(&BTI_MULTICLUSTERING);
    let golden = golden_partition_positions(&clean_dir);
    let (last_pk, last_pos) = *golden
        .last()
        .expect("at least two partitions, asserted above");

    let temp = TempDir::new().expect("tempdir");
    let input_dir = stage_generation(&BTI_MULTICLUSTERING, &clean_dir, temp.path());
    let data_db = input_dir.join("da-2-bti-Data.db");
    let (chunk_length, data_length, offsets) =
        compression_chunk_table(&input_dir.join("da-2-bti-CompressionInfo.db"));

    // The FIRST chunk whose decompressed range starts at or after the LAST
    // partition's start: it can intersect that partition and no other.
    let chunk_index = (last_pos as usize).div_ceil(chunk_length);
    let chunk_start = chunk_index * chunk_length;
    assert!(
        chunk_index < offsets.len(),
        "derivation failed: chunk {chunk_index} (decompressed {chunk_start}) is past the {} \
         declared chunks — pick a fixture whose last partition is not in the final chunk",
        offsets.len()
    );
    assert!(
        chunk_start >= last_pos as usize,
        "derivation failed: chunk {chunk_index} starts at decompressed {chunk_start}, BEFORE the \
         last partition's own start {last_pos} — corrupting it would also hit an earlier partition \
         and the expected loss set below would be wrong"
    );
    assert!(
        (chunk_start as u64) < data_length,
        "derivation failed: chunk {chunk_index} starts at {chunk_start}, past the {data_length}-byte \
         data section, so no partition intersects it and this case would assert a loss that cannot \
         happen"
    );
    let flipped_at = break_one_chunk_crc(&data_db, &offsets, chunk_index);

    let out = temp.path().join("out");
    let output = run_salvage_with(&BTI_MULTICLUSTERING, &data_db, &out);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(
        output.status.code(),
        Some(3),
        "design D3: SOME partitions lost and SOME recovered is exit 3 — 2 would claim nothing was \
         recoverable and 0 would hide the loss from a script branching on `$?`. Corrupted chunk \
         {chunk_index} (decompressed [{chunk_start}, ..), Data.db byte {flipped_at}); \
         stdout={stdout}\nstderr={stderr}"
    );

    let manifest = manifest_object(&stdout);
    let losses = manifest
        .get("losses")
        .and_then(|l| l.as_array())
        .unwrap_or_else(|| panic!("manifest missing 'losses' array: {manifest}"));
    assert!(
        !losses.is_empty(),
        "R7.2: exit 3 must come with the losses NAMED in the manifest, not just a code; \
         manifest={manifest}"
    );
    assert_eq!(
        losses.len(),
        1,
        "exactly the LAST partition intersects chunk {chunk_index}; losses={losses:?}"
    );
    let loss = &losses[0];
    assert_eq!(
        loss.get("key_hex").and_then(|k| k.as_str()),
        // A single-column `int` partition key serializes as its 4-byte
        // big-endian value (`Int32Type`), with no composite framing.
        Some(hex::encode(last_pk.to_be_bytes()).as_str()),
        "the loss must name the LAST partition (pk={last_pk}); loss={loss}"
    );
    assert_eq!(
        loss.get("class").and_then(|c| c.as_str()),
        Some("chunk-crc"),
        "a chunk whose CRC no longer validates classifies chunk-crc; loss={loss}"
    );
    assert_eq!(
        loss.get("data_offset").and_then(|o| o.as_u64()),
        Some(last_pos),
        "the loss must report the partition's own data-section offset, which the golden records \
         as {last_pos}; loss={loss}"
    );
    assert!(
        loss.get("chunks")
            .and_then(|c| c.as_array())
            .map(|c| c.iter().any(|v| v.as_u64() == Some(chunk_index as u64)))
            .unwrap_or(false),
        "the loss must name the corrupted chunk {chunk_index} among the chunks it intersects; \
         loss={loss}"
    );

    let partitions = manifest
        .get("partitions")
        .unwrap_or_else(|| panic!("manifest missing 'partitions': {manifest}"));
    assert_eq!(
        partitions.get("total").and_then(|t| t.as_u64()),
        Some(golden.len() as u64),
        "the boundary source must enumerate every partition the golden records; \
         partitions={partitions}"
    );
    assert_eq!(
        partitions.get("recovered").and_then(|r| r.as_u64()),
        Some(golden.len() as u64 - 1),
        "every partition EXCEPT the one intersecting the corrupted chunk must be recovered — this \
         is what makes the loss PARTIAL rather than total; partitions={partitions}"
    );
    assert_eq!(
        manifest.get("refused"),
        Some(&serde_json::Value::Null),
        "a partial loss with real survivors must not refuse; manifest={manifest}"
    );
    // Exit 3 here must be attributable to the LOSSES: the staged generation
    // keeps its `-TOC.txt`, so no publication-barrier gap can be supplying it.
    let classes = finding_classes(&manifest);
    assert!(
        !classes.iter().any(|c| c == "UnpublishedInputGeneration"),
        "the staged input is published, so exit 3 must come from the losses and not from a \
         barrier gap; classes={classes:?}"
    );

    // R7.2's "a complete generation set is still written despite the losses".
    let table_dir = output_table_dir(&out, "test_da", "multiclustering_table");
    let toc_path = std::fs::read_dir(&table_dir)
        .unwrap_or_else(|e| panic!("read {table_dir:?}: {e}"))
        .flatten()
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.ends_with("-TOC.txt"))
                .unwrap_or(false)
        })
        .unwrap_or_else(|| {
            panic!(
                "no *-TOC.txt under {table_dir:?} — an output generation without its own \
                 publication barrier is not a COMPLETE generation set, whatever else was written"
            )
        });
    let toc =
        std::fs::read_to_string(&toc_path).unwrap_or_else(|e| panic!("read {toc_path:?}: {e}"));
    let named: Vec<&str> = toc
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();
    assert!(
        named.len() >= 2,
        "{toc_path:?}: a generation set names more than one component; got {named:?}"
    );
    let prefix = toc_path
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.strip_suffix("TOC.txt"))
        .expect("TOC.txt file name carries the generation prefix")
        .to_string();
    for component in &named {
        let path = table_dir.join(format!("{prefix}{component}"));
        assert!(
            path.is_file(),
            "{toc_path:?} names {component} but {path:?} was not written — the generation set is \
             INCOMPLETE, so the output is not publishable even though salvage exited 3"
        );
    }
    assert!(
        named.contains(&"Data.db"),
        "{toc_path:?} must name Data.db — the surviving partitions have to be somewhere; \
         got {named:?}"
    );

    eprintln!(
        "[issue_4196] R7.2 partial arm: chunk {chunk_index} (decompressed [{chunk_start}, ..), \
         Data.db byte {flipped_at}) corrupted -> exit 3, 1 loss (pk={last_pk}, chunk-crc), {} of \
         {} partitions recovered, generation set {named:?} written.",
        golden.len() - 1,
        golden.len()
    );
}

/// The control leg: the SAME staged generation, UNCORRUPTED, exits 0 with an
/// empty loss list. Without it, exit 3 above could be this fixture's permanent
/// outcome (a BTI input salvage simply cannot do cleanly) and the partial-loss
/// case would prove nothing.
#[test]
fn undamaged_bti_input_exits_0_with_no_losses() {
    let clean_dir = resolve_committed_fixture(&BTI_MULTICLUSTERING);
    let golden = golden_partition_positions(&clean_dir);
    let temp = TempDir::new().expect("tempdir");
    let input_dir = stage_generation(&BTI_MULTICLUSTERING, &clean_dir, temp.path());

    let out = temp.path().join("out");
    let output = run_salvage_with(
        &BTI_MULTICLUSTERING,
        &input_dir.join("da-2-bti-Data.db"),
        &out,
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        output.status.code(),
        Some(0),
        "an intact, published generation must exit 0; stdout={stdout}\nstderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let manifest = manifest_object(&stdout);
    assert_eq!(
        manifest
            .get("losses")
            .and_then(|l| l.as_array())
            .map(Vec::len),
        Some(0),
        "an intact input has nothing to lose; manifest={manifest}"
    );
    assert_eq!(
        manifest
            .get("partitions")
            .and_then(|p| p.get("recovered"))
            .and_then(|r| r.as_u64()),
        Some(golden.len() as u64),
        "every partition the golden records must be recovered; manifest={manifest}"
    );
}

/// `true` iff any `*-Data.db` exists anywhere under `dir` (the writer nests
/// `<out>/<keyspace>/<table>/`, so a top-level listing sees no components).
fn data_db_exists_under(dir: &Path) -> bool {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return false;
    };
    for entry in rd.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if data_db_exists_under(&path) {
                return true;
            }
        } else if path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.ends_with("-Data.db"))
            .unwrap_or(false)
        {
            return true;
        }
    }
    false
}

/// `CQLITE_REQUIRE_FIXTURES=1` turns a SKIP below into a hard failure (issue
/// #719 dataset doctrine) — the two cases below use the NON-committed
/// `index_db_bit_flip_big` corruption fixture (only its `TOC.txt` and
/// `Digest.crc32` are git-tracked; `Data.db`/`Index.db`/etc. are gitignored),
/// unlike every other fixture in this file.
fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

fn skip_or_require(what: &str, reason: &str) {
    if require_fixtures_strict() {
        panic!("CQLITE_REQUIRE_FIXTURES=1 but {what} unavailable: {reason}");
    }
    eprintln!("[SKIP] {what}: {reason}");
}

/// Like [`resolve_committed_fixture`], but for a fixture whose binaries are
/// NOT all git-tracked: `None` rather than a panic when no candidate root
/// carries it, so the caller can SKIP (unless `CQLITE_REQUIRE_FIXTURES=1`).
fn resolve_optional_fixture(relative: &str) -> Option<PathBuf> {
    candidate_base_roots()
        .into_iter()
        .map(|root| root.join(relative))
        .find(|dir| dir.join("nb-1-big-Data.db").is_file())
}

/// `--schema compression-parity.cql salvage --table lz4_table <input> --out
/// <out>` — every case below explicitly names `--table` since the synthetic
/// `input` directories they build are never named after a real table.
fn run_cli_for_table_dir(input: &Path, out: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args([
            "--schema",
            schema_path(&LZ4_TABLE)
                .to_str()
                .expect("schema path is utf-8"),
            "salvage",
            "--table",
            "lz4_table",
            input.to_str().expect("input path is utf-8"),
            "--out",
            out.to_str().expect("out path is utf-8"),
        ])
        .output()
        .expect("failed to execute cqlite binary")
}

/// job 3759 roborev finding — `execute_salvage_command`'s own documented
/// exit-3 contract explicitly lists "a published generation was skipped at
/// discovery" as one of the exit-3 causes, alongside "another generation
/// refused outright". `all_refused` (the exit-2 arm) used to consult only
/// `hard_errors` and `reports`, never `discovery.skipped` — so a table-dir
/// run where every ATTEMPTED generation refused AND a SIBLING generation was
/// SKIPPED at discovery (no `-TOC.txt`) exited 2 ("EVERY generation
/// refused"), contradicting that same documented contract for the skip that
/// was sitting right beside it.
#[test]
fn all_attempted_generations_refused_plus_a_skipped_sibling_exits_3_not_2() {
    const CORRUPT_FIXTURE: &str = "corruption/test_comp_corrupt/index_db_bit_flip_big";
    let Some(corrupt_dir) = resolve_optional_fixture(CORRUPT_FIXTURE) else {
        skip_or_require(
            "index_db_bit_flip_big fixture",
            &format!("no candidate root carries {CORRUPT_FIXTURE}"),
        );
        return;
    };
    let clean_dir = resolve_committed_fixture(&LZ4_TABLE);
    let temp = TempDir::new().expect("tempdir");
    let input_dir = temp.path().join("input");
    std::fs::create_dir_all(&input_dir).expect("create input dir");

    // Generation 1: the corrupt (refusing) fixture, kept at its own
    // numbering — ATTEMPTED, and refused (boundary-source-unreadable).
    for entry in std::fs::read_dir(&corrupt_dir)
        .expect("read corrupt dir")
        .flatten()
    {
        let name = entry.file_name();
        if name.to_string_lossy().starts_with("nb-1-big-") {
            std::fs::copy(entry.path(), input_dir.join(&name)).expect("copy corrupt component");
        }
    }

    // Generation 2: a real, healthy generation renumbered nb-2-big-*, with
    // its -TOC.txt DELIBERATELY OMITTED — SKIPPED at discovery, never
    // attempted at all.
    for component in LZ4_TABLE.components {
        if *component == "nb-1-big-TOC.txt" {
            continue;
        }
        let Some(suffix) = component.strip_prefix("nb-1-big-") else {
            continue;
        };
        std::fs::copy(
            clean_dir.join(component),
            input_dir.join(format!("nb-2-big-{suffix}")),
        )
        .unwrap_or_else(|e| panic!("copy {component}: {e}"));
    }
    assert!(
        !input_dir.join("nb-2-big-TOC.txt").exists(),
        "the case needs generation 2 to have NO -TOC.txt sibling"
    );

    let out = temp.path().join("out");
    let output = run_cli_for_table_dir(&input_dir, &out);
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    assert_eq!(
        output.status.code(),
        Some(3),
        "every ATTEMPTED generation refused, plus a SKIPPED sibling, must exit 3 (the documented \
         exit-3 cause \"a published generation was skipped at discovery\"), never 2 (\"every \
         generation refused\" — which was true only of the ATTEMPTED set, never the whole table \
         dir); stdout={stdout}\nstderr={stderr}"
    );
    assert!(
        stderr.contains("nb-2-big-Data.db") || stdout.contains("nb-2-big-Data.db"),
        "the skipped generation 2 must be NAMED somewhere in the run's output; \
         stdout={stdout}\nstderr={stderr}"
    );
    assert!(
        !data_db_exists_under(&out),
        "neither the refused generation 1 nor the never-attempted, skipped generation 2 may have \
         written a Data.db under --out"
    );
}

/// The CONTROL leg: the SAME two refused generations, with NO skip anywhere
/// — must still exit 2, unchanged by the fix above. Without this, the fix
/// could have made every multi-generation refusal exit 3 regardless of
/// whether a skip actually occurred, which would just move the bug rather
/// than fix it.
#[test]
fn all_attempted_generations_refused_with_no_skip_still_exits_2() {
    const CORRUPT_FIXTURE: &str = "corruption/test_comp_corrupt/index_db_bit_flip_big";
    let Some(corrupt_dir) = resolve_optional_fixture(CORRUPT_FIXTURE) else {
        skip_or_require(
            "index_db_bit_flip_big fixture",
            &format!("no candidate root carries {CORRUPT_FIXTURE}"),
        );
        return;
    };
    let temp = TempDir::new().expect("tempdir");
    let input_dir = temp.path().join("input");
    std::fs::create_dir_all(&input_dir).expect("create input dir");

    // Two generations, both copies of the same corrupt (refusing) fixture —
    // no generation is skipped at discovery.
    for gen_label in ["nb-1-big-", "nb-2-big-"] {
        for entry in std::fs::read_dir(&corrupt_dir)
            .expect("read corrupt dir")
            .flatten()
        {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(suffix) = name_str.strip_prefix("nb-1-big-") {
                std::fs::copy(entry.path(), input_dir.join(format!("{gen_label}{suffix}")))
                    .expect("copy fixture component");
            }
        }
    }

    let out = temp.path().join("out");
    let output = run_cli_for_table_dir(&input_dir, &out);
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    assert_eq!(
        output.status.code(),
        Some(2),
        "every generation refused, with NO skip anywhere, must still exit 2 (unchanged control); \
         stdout={stdout}\nstderr={stderr}"
    );
    assert!(
        !data_db_exists_under(&out),
        "no generation may have written a Data.db when every one refused"
    );
}
