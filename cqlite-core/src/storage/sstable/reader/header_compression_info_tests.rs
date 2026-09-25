//! Issue #4159 (audit S25/S26): `load_nb_compression_info` must distinguish an
//! ABSENT `CompressionInfo.db` from an UNREADABLE one.
//!
//! Its own file rather than inline in `header_minimal.rs`: the campsite rule (epic
//! #1116/#1135) keeps source files small, and `reader/` already uses sibling
//! `*_tests.rs` modules for exactly this.

use super::header_minimal::load_nb_compression_info;

/// Issue #4159 (audit S25/S26): ABSENT and UNREADABLE are different answers.
///
/// The two `create_minimal_*_header` builders answered both with
/// `algorithm = "NONE"` under one `Err` arm commented "Assuming no compression".
/// Absence is the legitimate shape of an UNCOMPRESSED SSTable (#1406), so it must
/// keep reporting `None`; a component that is THERE and unparseable must refuse,
/// because `"NONE"` over compressed chunk data decodes to nothing.
#[tokio::test]
async fn an_absent_compression_info_is_none_not_an_error() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let data = dir.path().join("nb-1-big-Data.db");
    std::fs::write(&data, b"irrelevant").expect("write Data.db");
    let got = load_nb_compression_info(&data)
        .await
        .expect("an ABSENT CompressionInfo.db is not an error");
    assert!(
        got.is_none(),
        "absence must be reported as `None` so the caller can say \"uncompressed\""
    );
}

#[tokio::test]
async fn a_present_but_unparseable_compression_info_refuses() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let data = dir.path().join("nb-1-big-Data.db");
    std::fs::write(&data, b"irrelevant").expect("write Data.db");
    std::fs::write(
        dir.path().join("nb-1-big-CompressionInfo.db"),
        b"not a CompressionInfo.db",
    )
    .expect("stage the component");
    let e = load_nb_compression_info(&data)
        .await
        .expect_err("a PRESENT but unparseable component must refuse, never report None");
    assert!(
        !format!("{e}").is_empty(),
        "the refusal must be renderable: {e:?}"
    );
}

/// A `Data.db` whose descriptor does not parse is NOT evidence that the SSTable
/// is uncompressed — we simply cannot say where its sidecar would be — so the
/// probe refuses rather than reporting absence.
#[tokio::test]
async fn an_underivable_base_name_refuses_rather_than_reporting_absence() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let data = dir.path().join("not-an-sstable-name");
    std::fs::write(&data, b"irrelevant").expect("write file");
    let e = load_nb_compression_info(&data)
        .await
        .expect_err("an underivable base name must refuse");
    assert!(
        format!("{e}").contains("base name"),
        "the refusal must say what it could not derive: {e}"
    );
}
