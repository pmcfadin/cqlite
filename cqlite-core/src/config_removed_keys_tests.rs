//! Unit tests for the removed-key report over the core `Config` schema (#1696).

use super::*;
use crate::Config;

/// A pre-change core `Config` document: the CURRENT surviving shape with every
/// key #1696 removed put back into it, exactly as a Python caller's saved config
/// still looks.
///
/// The surviving half comes from a serialized `Config::default()` — `Config` is
/// not `#[serde(default)]`, so a document must be COMPLETE, and deriving that half
/// keeps this fixture from rotting on an unrelated schema change. The removed half
/// is written out LITERALLY rather than generated from `REMOVED_KEYS`: using the
/// table under test to build its own fixture would prove only that the table
/// equals itself.
fn old_shape_json() -> String {
    let mut document =
        serde_json::to_value(Config::default()).expect("serialize the default config");

    let storage = document
        .get_mut("storage")
        .and_then(serde_json::Value::as_object_mut)
        .expect("the default config has a storage object");
    // Surviving keys, set to non-default values so the assertions below prove
    // they still take effect rather than matching the default by accident.
    storage.insert("memtable_size_threshold".into(), 33_554_432.into());
    // The removed knobs, in their pre-#1696 spelling.
    storage.insert("max_sstable_size".into(), 268_435_456.into());
    storage.insert("block_size".into(), 65_536.into());
    storage.insert("enable_bloom_filters".into(), true.into());
    storage.insert("bloom_filter_fp_rate".into(), 0.01.into());
    storage.insert("io_threads".into(), 8.into());
    storage.insert("sync_mode".into(), "Normal".into());
    storage.insert(
        "compaction".into(),
        serde_json::json!({ "auto_compaction": false }),
    );

    let query = document
        .get_mut("query")
        .and_then(serde_json::Value::as_object_mut)
        .expect("the default config has a query object");
    query.insert("plan_cache_size".into(), 1000.into());
    query.insert("enable_optimization".into(), true.into());
    query.insert(
        "parallel".into(),
        serde_json::json!({ "enabled": true, "max_threads": 4, "min_parallel_rows": 1000 }),
    );

    let root = document
        .as_object_mut()
        .expect("a serialized config is an object");
    root.insert(
        "performance".into(),
        serde_json::json!({
            "enable_metrics": true,
            "metrics_interval": { "secs": 60, "nanos": 0 },
            "enable_profiling": false,
            "background_tasks": {
                "enable_stats": true,
                "stats_interval": { "secs": 300, "nanos": 0 },
                "enable_cleanup": true,
                "cleanup_interval": { "secs": 3600, "nanos": 0 }
            }
        }),
    );

    serde_json::to_string_pretty(&document).expect("re-serialize the old-shape document")
}

/// The property: an old-shape document LOADS (no hard failure, no migration
/// cliff) and WARNS by name for every removed key it sets.
#[test]
fn an_old_shape_document_loads_and_warns_for_every_removed_key() {
    let json = old_shape_json();
    let (config, warning) = Config::from_json_str_reporting_removed(&json, "config dict")
        .expect("an old-shape config must still LOAD: the posture is parse-and-ignore");

    // Surviving keys still take effect — the removed ones were ignored, not fatal.
    assert_eq!(config.storage.memtable_size_threshold, 33_554_432);
    assert_eq!(
        config.memory.max_memory,
        Config::default().memory.max_memory
    );
    assert!(!config.storage.compaction.auto_compaction);

    let warning = warning.expect("a document naming removed keys MUST warn, never be silent");
    assert!(
        warning.contains("config dict"),
        "the warning must name the source: {warning}"
    );
    for removed in REMOVED_KEYS {
        assert!(
            warning.contains(removed.path),
            "the warning must name {} so the user can find and delete it: {warning}",
            removed.path
        );
    }
}

/// A current-shape document is SILENT: the signal must not become noise attached
/// to every load.
#[test]
fn a_current_shape_document_is_silent() {
    let json = serde_json::to_string(&Config::default()).expect("serialize default config");
    let (_, warning) = Config::from_json_str_reporting_removed(&json, "config dict")
        .expect("a round-tripped default config must load");
    assert!(
        warning.is_none(),
        "a config naming no removed key must warn about nothing: {warning:?}"
    );
}

/// Matching is by DOTTED PATH, not by leaf name: a live key that happens to share
/// a name with a removed one under a different parent must not be reported.
#[test]
fn matching_is_scoped_to_the_dotted_path() {
    let document = serde_json::json!({
        "block_size": 4096,
        "query": { "block_size": 4096, "io_threads": 2 },
        "storage": { "compaction": { "block_size": 4096 } }
    });
    assert!(
        !json_has_path(&document, "storage.block_size"),
        "a `block_size` elsewhere is not `storage.block_size`"
    );
    assert!(
        !json_has_path(&document, "storage.io_threads"),
        "a `query.io_threads` is not `storage.io_threads`"
    );
    assert!(json_has_path(&document, "query.block_size"));
}

/// Key PRESENCE is what matters: `null` is still someone believing they
/// configured something.
#[test]
fn a_null_valued_removed_key_is_still_reported() {
    let document = serde_json::json!({ "storage": { "sync_mode": serde_json::Value::Null } });
    assert!(json_has_path(&document, "storage.sync_mode"));
}

/// A failed load yields no warning at all.
///
/// Not because the text makes any promise about the load — since #1696 roborev r5
/// F1 it deliberately makes none — but because this API returns the warning
/// INSIDE its `Ok`, so a caller of the reporting constructor cannot be handed a
/// removed-key report for a document that never became a `Config`. That is the
/// shape the test pins; the warning's own text is independently safe wherever it
/// is emitted (see `deprecation_warning`).
#[test]
fn a_failed_load_produces_no_warning() {
    // Names a removed key AND is missing the required `memory` section, so it
    // parses as JSON but cannot deserialize into `Config`.
    let json = r#"{ "storage": { "block_size": 65536 } }"#;
    assert!(
        warning_for_json("config dict", json).is_some(),
        "fixture must name a removed key, else this test proves nothing"
    );
    let outcome = Config::from_json_str_reporting_removed(json, "config dict");
    assert!(
        outcome.is_err(),
        "an incomplete document must still fail the load"
    );
    assert!(
        outcome
            .map(|(_, warning)| warning)
            .unwrap_or(None)
            .is_none(),
        "the reporting constructor must not return a warning alongside an Err"
    );
}

/// Unparseable content is left entirely to the real parse: the scan must never be
/// the thing that rejects a document.
#[test]
fn unparseable_content_is_not_this_scans_error() {
    assert!(warning_for_json("config dict", "{ not json").is_none());
    assert!(Config::from_json_str("{ not json").is_err());
}

/// Every removed key carries a note that says WHY it is gone and cites the issue,
/// because a warning naming a key without telling the user what to do instead is
/// half a signal.
#[test]
fn every_removed_key_documents_its_removal() {
    for removed in REMOVED_KEYS {
        assert!(
            removed.note.contains("#1696"),
            "{} must cite the issue that removed it",
            removed.path
        );
        assert!(
            removed.note.len() > 30,
            "{} needs a real explanation, not a stub: {}",
            removed.path,
            removed.note
        );
    }
}

/// A DIRECT serde deserialization of `Config` reports NOTHING — the one surface
/// the #1696 rule does not reach, pinned as an honest, tested fact (issue #3520).
///
/// `Config` derives `Deserialize`, so `serde_json::from_str::<Config>` /
/// `from_value::<Config>` bypass the reporting constructors entirely and serde
/// DISCARDS the removed keys in silence. That is a real residual, NOT a decision
/// this test endorses: closing it needs a custom `Deserialize` capturing unknown
/// keys across every nested config struct, which is an architectural change well
/// outside AH3's decorative-knob purge (#1696 roborev r2 F3, scoped by owner
/// decision rather than fixed).
///
/// It is pinned rather than left implicit so the gap is recorded where the next
/// person meets it, and so the day #3520 lands, THIS test is the thing that tells
/// them the behavior they changed: a `Deserialize` that reports (or rejects)
/// removed keys makes the plain `from_str` below stop being silent, and the fix
/// is to invert this case rather than delete it.
#[test]
fn direct_serde_deserialization_is_the_unreported_surface() {
    let json = old_shape_json();

    // The REPORTING constructor names the dead keys...
    let (_, warning) = Config::from_json_str_reporting_removed(&json, "config dict")
        .expect("the fixture is a loadable document");
    let warning = warning.expect("the reporting constructor must name the removed keys");
    assert!(warning.contains("storage.block_size"));

    // ...while plain serde on the SAME document succeeds with no signal at all.
    // There is nowhere for a warning to appear here: `from_str` returns only the
    // config, which is exactly why an optional constructor cannot enforce the
    // rule at this boundary (#3520).
    let direct: Config =
        serde_json::from_str(&json).expect("serde accepts the removed keys silently");
    // The removed keys are gone from the struct — they were DISCARDED, not
    // rejected and not reported — while the surviving keys took effect.
    assert_eq!(
        direct.storage.memtable_size_threshold, 33_554_432,
        "the surviving half of the document must still be honoured, else this \
         test is about a failed parse instead of a silent discard"
    );
}
