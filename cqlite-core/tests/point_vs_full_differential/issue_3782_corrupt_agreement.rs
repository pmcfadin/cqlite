//! Issue #3782 — the point/full differential over a CORRUPT fixture.
//!
//! The parent lane compares the two forced read paths over WELL-FORMED fixtures,
//! where they agree. That is precisely why it could not see #3782: before the fix
//! the two arms agreed *by both truncating*, so a fixture differing from a real
//! Cassandra SSTable by ONE decompressed byte silently dropped 77 of 100 rows on
//! both arms and the differential stayed green. Agreement is necessary and not
//! sufficient — WHAT the arms agree on matters.
//!
//! # What this case asserts
//!
//! 1. The FULL read path now REFUSES at least one partition of the corrupt
//!    fixture, with the decode error's kind preserved (it names the offending
//!    clustering column), instead of returning a short result set.
//! 2. Neither arm ever FABRICATES: any partition either arm still answers `Ok`
//!    for must return exactly the pristine fixture's rows for that partition —
//!    compared as a MULTISET, so a surplus DUPLICATE of a legitimate row counts
//!    as fabrication (roborev job 57 finding 1; duplication is how the measured
//!    pre-fix compaction result grew to 102 rows while losing two partitions).
//!
//! # The change's tracked residuals — ONE is declared here, THREE existed
//!
//! The "GAP N of 1" label below counts the gaps declared IN THIS LANE that are
//! still OPEN, not #3782's residuals in total: #3782 had **three** tracked
//! residuals touching read routes, and the others are recorded where their
//! route is exercised rather than duplicated here.
//!
//! | residual | route | declared in |
//! |----------|-------|-------------|
//! | **#3922** | the POINT read path still answers `Ok` short | GAP 1 of 1 below |
//! | **#3928** | the partition-HEADER arm resynced one byte | **CLOSED** — see the note below and `issue_3928_corrupt_header_refusal.rs` |
//! | **#3949** | AC2 half (b): the index-random-read refusal still arrives via #2302's Signal-B WARN + sequential re-walk | `issue_3782_corrupt_row_refusal.rs` module doc |
//!
//! Nothing in this lane reaches #3949's route: both arms here go through
//! `Database::execute` under a forced read path, never
//! `SSTableReader::iterate_all_partitions`, so declaring it as a third gap of
//! THIS case would claim a coverage boundary this case does not have.
//!
//! # DECLARED GAP 1 of 1 (in this lane) — point and full do NOT yet agree here (#3782, #3922)
//!
//! The POINT arm still answers `Ok` with a SHORT row set for the damaged
//! partition. That is not an oversight in the fix: the BIG-promoted and BTI point
//! readers use the tolerant row-decode break as their *chunk-straddle protocol*
//! — `bti_point.rs`'s "a DIFFERENT decoded key means the window tail was
//! truncated mid-partition … pull the next chunk" — so refusing there would
//! break a legitimate, load-bearing control flow rather than fix a defect. They
//! decode a chunk-covering WINDOW, not a proven-complete buffer, so the
//! `BufferExtent::Complete` contract the stitched read path uses does not apply
//! (they declare `BufferExtent::Window`, which is the truth about their buffer).
//! Closing it needs the point readers bounded to the target partition's own
//! extent first; that is a separate change to a different subsystem, tracked as
//! **#3922**, and it is declared here rather than left to be rediscovered from a
//! green suite.
//!
//! Assertion 2 is written so it stays TRUE after that follow-up (a refusing arm
//! simply never enters the `Ok` branch), so this case does not pin the gap.
//!
//! **Carry-forward for #3922, and the reason this case cannot be its oracle:**
//! the point arm here runs under `if let Ok(r) = point.execute(...)` and asserts
//! only NON-FABRICATION, so it will never assert point/full AGREEMENT — not
//! today, and not after #3922 lands. A point arm that starts REFUSING simply
//! skips the `Ok` branch and this case stays green either way, which is exactly
//! what makes it safe to commit now and useless as a completion signal. So
//! **#3922 must add the positive `point == full` assertion itself** (or convert
//! this `if let Ok` into a required-refusal match); if it does not, #3782's AC4
//! — the two read paths agreeing on the corrupt fixture — stays untested
//! forever behind a green suite.
//!
//! # CLOSED — the partition-HEADER arm no longer resyncs (#3928)
//!
//! #3782 fixed the ROW arm only, and this lane declared the header half as its
//! second gap: the header arm still `tracing::warn!`ed and advanced ONE byte to
//! resynchronise, so a corrupted header byte in a proven-complete section could
//! both DROP that partition and FABRICATE one by landing the resync on
//! misaligned bytes, while returning `Ok`.
//!
//! **#3928 closed it.** Five arms now refuse where no further bytes can arrive
//! — the two in the block-emit walk (keyed on `BufferExtent`), the one now
//! SHARED by the two sliding drivers (keyed on `at_final_chunk`, and
//! UNCONDITIONALLY for a `Ready`-then-`Err` header, which cannot straddle), and
//! the bare `Err(_) => break` on the BTI stitched-scan route. Its own lane is
//! `issue_3928_corrupt_header_refusal.rs`, which stages the SAME fixtures
//! through the SAME harness with a HEADER-byte mutation instead of a clustering
//! one.
//!
//! This note is kept rather than deleted because the multiset comparison below
//! is still justified by that mechanism: fabrication-by-DUPLICATION is a shape a
//! membership test cannot see, whoever produces it.

use std::collections::BTreeMap;
use std::path::Path;

use cqlite_core::config::ReadPathMode;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::QueryRow;
use cqlite_core::Config;

#[path = "../support/corrupt_byte_fixture.rs"]
mod fixture;
#[path = "../support/multiset.rs"]
mod multiset;

use super::datasets_root;

/// Open the fixture at `root` with one forced read path.
async fn open_db(root: &Path, schema: &Path, mode: ReadPathMode) -> cqlite_core::Database {
    let mut core_config = Config::default();
    core_config.query.forced_read_path = Some(mode);
    ingest(IngestionConfig {
        schema_paths: vec![schema.to_path_buf()],
        data_dir: root.to_path_buf(),
        version_hint: None,
        core_config,
        table_directory_filter: Some(format!("/{}/", fixture::FIX_KS)),
    })
    .await
    .expect("ingest")
    .database
}

/// The fixture GENERATION directory, resolved per TABLE across every candidate
/// root (#3220) — this BIG subject is a FETCHED fixture (its checkout directory
/// holds sidecars only), so a checkout-first or env-first preference is wrong for
/// one root or the other (#3104); evidence decides — and selected
/// DETERMINISTICALLY among the generations that actually carry a `*-Data.db`.
///
/// That second half is not theoretical here (roborev job 57 finding 2): the
/// checkout's `composite_key_table-…/` holds four sidecars and no `Data.db`, so a
/// selection taking the first `read_dir` hit without requiring the component can
/// bind to a directory nothing can be read from. Absence is a loud named panic,
/// never a skip.
fn fixture_dir() -> std::path::PathBuf {
    datasets_root::resolve_table_generation_dir(fixture::FIX_KS, fixture::FIX_TABLE).unwrap_or_else(
        |why| {
            panic!(
                "fixture {}.{} has no usable generation directory: {why}",
                fixture::FIX_KS,
                fixture::FIX_TABLE
            )
        },
    )
}

/// Render a `uuid` partition-key value as the hyphenated CQL literal
/// (`8-4-4-4-12`).
///
/// `Display` on `Value` renders `UUID(<hex>)`, which is a DIAGNOSTIC form and not
/// valid CQL. Interpolating it made every query fail to PARSE — and the case then
/// "passed" having compared nothing, which is the vacuous-pass shape this repo's
/// fixture doctrine exists to remove. Measured, not guessed: with `Display` this
/// case reported every one of 100 partitions erroring on the UNFIXED tree.
fn uuid_literal(v: &cqlite_core::types::Value) -> Option<String> {
    let cqlite_core::types::Value::Uuid(bytes) = v else {
        return None;
    };
    let h = hex::encode(bytes);
    Some(format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    ))
}

/// Row set → an ORDER-INSENSITIVE multiset of per-row renderings, each a
/// sorted-by-column-name `Debug` of the row's values (the parent lane's
/// `normalize`, sorted so a subset comparison is order-independent).
fn normalize(rows: &[QueryRow]) -> Vec<String> {
    let mut out: Vec<String> = rows
        .iter()
        .map(|row| {
            let sorted: BTreeMap<&str, String> = row
                .values
                .iter()
                .map(|(k, v)| (k.as_ref(), format!("{v:?}")))
                .collect();
            format!("{sorted:?}")
        })
        .collect();
    out.sort();
    out
}

#[tokio::test]
async fn full_read_refuses_a_corrupt_partition_and_neither_arm_fabricates() {
    let schema =
        datasets_root::schema_path(fixture::SCHEMA_FILE).expect("committed CQL schema (#3148)");
    let staged = fixture::stage_control_and_mutated(&fixture_dir(), "ptfull");

    let control = open_db(&staged.control_root, &schema, ReadPathMode::Full).await;
    let discovery = control
        .execute(&format!(
            "SELECT partition_key FROM {}.{}",
            fixture::FIX_KS,
            fixture::FIX_TABLE
        ))
        .await
        .expect("control discovery SELECT")
        .rows;
    let mut keys: Vec<String> = discovery
        .iter()
        .filter_map(|r| r.values.get("partition_key").and_then(uuid_literal))
        .collect();
    keys.sort();
    keys.dedup();
    assert!(
        !keys.is_empty(),
        "0-rows-when-present: the control fixture must yield partition keys"
    );

    let point = open_db(&staged.mutated_root, &schema, ReadPathMode::Point).await;
    let full = open_db(&staged.mutated_root, &schema, ReadPathMode::Full).await;

    let mut full_errored = 0usize;
    let mut named_the_column = false;
    let mut carried_the_kind = false;
    for key in &keys {
        let sql = format!(
            "SELECT * FROM {}.{} WHERE partition_key = {key}",
            fixture::FIX_KS,
            fixture::FIX_TABLE
        );
        let expected = normalize(
            &control
                .execute(&sql)
                .await
                .unwrap_or_else(|e| panic!("the pristine fixture must read partition {key}: {e}"))
                .rows,
        );
        assert!(
            !expected.is_empty(),
            "0-rows-when-present: control partition {key} yielded no rows"
        );
        let expected_counts = multiset::multiset(expected.iter().cloned());

        match full.execute(&sql).await {
            Err(e) => {
                full_errored += 1;
                // AC1 is about the error's KIND, so assert the VARIANT — a message
                // substring stays green through a refactor that re-wraps the decode
                // error in another variant while forwarding the text (and reading
                // bytes of a rendered string to decide a property is the
                // no-heuristics shape). The column name is kept as an ADDITIONAL,
                // weaker signal: it is what tells a human WHICH decode failed.
                carried_the_kind |= matches!(e, cqlite_core::Error::Corruption(_));
                named_the_column |= e.to_string().contains("clustering_key2");
            }
            // `normalize` sorts, so comparing the two Vecs IS exact multiset
            // equality: neither a missing row nor a surplus DUPLICATE of a
            // legitimate one can pass here.
            Ok(r) => assert_eq!(
                normalize(&r.rows),
                expected,
                "the FULL path answered partition {key} with a multiset that is not the \
                 pristine one"
            ),
        }

        if let Ok(r) = point.execute(&sql).await {
            // MULTISET, not membership (roborev job 57 finding 1). A membership
            // test asks "is every returned row one the control also has", which
            // N duplicate copies of a legitimate row satisfy — and duplication
            // is precisely one of the shapes fabrication takes here: the
            // pre-#3928 partition-HEADER resync advanced one byte and could
            // RE-EMIT a partition already emitted, which is how the measured
            // pre-fix compaction result reached 102 rows while LOSING two
            // partitions. That arm now refuses (#3928), but the comparison stays
            // a multiset: duplication is a fabrication shape whoever causes it,
            // and the point arm this case guards is still #3922's residual. Comparing occurrence COUNTS reports the surplus copy;
            // membership cannot (proved in `support/multiset.rs`).
            let got = multiset::multiset(normalize(&r.rows));
            let fabricated = multiset::surplus(&got, &expected_counts);
            assert!(
                fabricated.is_empty(),
                "the POINT path FABRICATED rows for partition {key} — {} surplus \
                 occurrence(s) beyond the pristine multiset: {}",
                fabricated.iter().map(|(_, n)| n).sum::<usize>(),
                multiset::describe(&fabricated)
            );
        }
    }

    assert!(
        full_errored > 0,
        "the corrupt fixture must make the FULL read path refuse at least one of the {} \
         partitions; all of them read cleanly, which is the #3782 silent-truncation shape",
        keys.len()
    );
    assert!(
        carried_the_kind,
        "the refusal must carry the DECODE error's own KIND (Error::Corruption), not a \
         re-wrapped generic — that preservation is the whole of #3782 AC1"
    );
    assert!(
        named_the_column,
        "the refusal should also NAME the offending clustering column, so a human can \
         locate the damage; the KIND assert above is the load-bearing one"
    );
}
