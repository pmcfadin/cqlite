//! Unit tests for [`crate::config`].
//!
//! Loaded via `#[path]` from `config.rs` so the production module stays under
//! the campsite-rule size target (epic #1135).

use super::*;

#[test]
fn test_default_config() {
    let config = Config::default();
    assert!(config.storage.compression.enabled);
    assert!(config.storage.enable_bloom_filters);
    assert!(config.memory.block_cache.enabled);
}

#[test]
fn test_memory_optimized_config() {
    let config = Config::memory_optimized();
    assert!(
        config.storage.memtable_size_threshold < Config::default().storage.memtable_size_threshold
    );
    assert!(config.memory.max_memory < Config::default().memory.max_memory);
}

#[test]
fn test_performance_optimized_config() {
    let config = Config::performance_optimized();
    assert!(
        config.storage.memtable_size_threshold > Config::default().storage.memtable_size_threshold
    );
    assert!(config.memory.max_memory > Config::default().memory.max_memory);
}

/// Issue #1582: a `QueryConfig` serialized BEFORE the byte-budget fields
/// existed (e.g. a pre-upgrade Python JSON/dict config) has no
/// `max_result_bytes`/`max_result_rows` keys. The `#[serde(default = ...)]`
/// on both fields must let it deserialize, taking the shipped defaults rather
/// than failing with a missing-field error.
#[test]
fn budget_fields_deserialize_with_serde_default_when_absent() {
    // Serialize a default QueryConfig, then STRIP both budget fields to
    // emulate an old serialized config that predates them.
    let mut value = serde_json::to_value(QueryConfig::default()).expect("serialize QueryConfig");
    let obj = value
        .as_object_mut()
        .expect("QueryConfig serializes as object");
    obj.remove("max_result_bytes");
    obj.remove("max_result_rows");
    assert!(
        !obj.contains_key("max_result_bytes") && !obj.contains_key("max_result_rows"),
        "both fields must be absent for this regression to be meaningful"
    );

    let restored: QueryConfig =
        serde_json::from_value(value).expect("old config (no budget fields) must deserialize");
    assert_eq!(
        restored.max_result_bytes, DEFAULT_MAX_RESULT_BYTES,
        "absent max_result_bytes must take the serde default"
    );
    assert_eq!(
        restored.max_result_rows,
        default_max_result_rows(),
        "absent max_result_rows must take the serde default"
    );
}

#[test]
fn test_config_validation() {
    let mut config = Config::default();
    assert!(config.validate().is_ok());

    // Test invalid max_memory
    config.memory.max_memory = 0;
    assert!(config.validate().is_err());

    // Reset and test invalid cache sizes
    config = Config::default();
    config.memory.block_cache.max_size = config.memory.max_memory + 1;
    assert!(config.validate().is_err());
}

/// Issue #1918: `forced_read_path` defaults to `None` (auto), round-trips its
/// lowercase encoding, and a config serialized before the field existed still
/// deserializes (absent → `None`).
#[test]
fn forced_read_path_defaults_absent_and_roundtrips() {
    // Default is None (auto).
    assert_eq!(QueryConfig::default().forced_read_path, None);

    // A config predating the field (key absent) deserializes to None.
    let mut value = serde_json::to_value(QueryConfig::default()).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .remove("forced_read_path")
        .expect("field present when serialized");
    let restored: QueryConfig = serde_json::from_value(value).unwrap();
    assert_eq!(restored.forced_read_path, None);

    // Explicit values round-trip via the lowercase serde encoding.
    for (mode, tag) in [
        (ReadPathMode::Point, "point"),
        (ReadPathMode::Full, "full"),
        (ReadPathMode::Auto, "auto"),
    ] {
        let mut cfg = QueryConfig::default();
        cfg.forced_read_path = Some(mode);
        let json = serde_json::to_string(&cfg).unwrap();
        assert!(
            json.contains(tag),
            "mode {mode:?} must serialize as {tag:?}: {json}"
        );
        let restored: QueryConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.forced_read_path, Some(mode));
    }
}

#[test]
fn test_storage_validation_errors() {
    let mut config = Config::default();

    // Test invalid block_size (should trigger line 573-574)
    config.storage.block_size = 0;
    let result = config.validate();
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("block_size must be greater than 0"));

    // Reset and test invalid memtable_size_threshold (should trigger line 579-580)
    config = Config::default();
    config.storage.memtable_size_threshold = 0;
    let result = config.validate();
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("memtable_size_threshold must be greater than 0"));

    // Reset and test invalid bloom filter false positive rate (should trigger line 589-590)
    config = Config::default();
    config.storage.enable_bloom_filters = true;
    config.storage.bloom_filter_fp_rate = 0.0; // Invalid: exactly 0
    let result = config.validate();
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("bloom_filter_fp_rate must be between 0 and 1"));

    // Test another invalid bloom filter false positive rate
    config.storage.bloom_filter_fp_rate = 1.0; // Invalid: exactly 1
    let result = config.validate();
    assert!(result.is_err());

    // Test bloom filter rate above 1
    config.storage.bloom_filter_fp_rate = 1.5; // Invalid: greater than 1
    let result = config.validate();
    assert!(result.is_err());

    // Test bloom filter rate below 0
    config.storage.bloom_filter_fp_rate = -0.1; // Invalid: less than 0
    let result = config.validate();
    assert!(result.is_err());
}

#[test]
fn test_valid_bloom_filter_config() {
    let mut config = Config::default();
    config.storage.enable_bloom_filters = true;
    config.storage.bloom_filter_fp_rate = 0.01; // Valid rate
    assert!(config.validate().is_ok());

    config.storage.bloom_filter_fp_rate = 0.5; // Valid rate
    assert!(config.validate().is_ok());

    config.storage.bloom_filter_fp_rate = 0.99; // Valid rate
    assert!(config.validate().is_ok());
}

#[test]
fn test_storage_config_deserializes_without_mmap_fields() {
    // Backward compatibility: a config payload serialized before the mmap
    // fields existed omits `use_mmap` / `mmap_min_size_bytes`. It must still
    // deserialize, defaulting to the safe buffered backend.
    let mut value = serde_json::to_value(StorageConfig::default()).unwrap();
    let obj = value.as_object_mut().unwrap();
    obj.remove("use_mmap");
    obj.remove("mmap_min_size_bytes");
    assert!(!obj.contains_key("use_mmap"));

    let restored: StorageConfig =
        serde_json::from_value(value).expect("old payload must still deserialize");
    assert!(!restored.use_mmap, "missing use_mmap must default to false");
    assert_eq!(
        restored.mmap_min_size_bytes, 4096,
        "missing mmap_min_size_bytes must default to one page"
    );
}

#[test]
fn test_full_config_deserializes_without_mmap_fields() {
    // Same guarantee through the top-level Config, mirroring how the Python
    // bindings parse a JSON/dict payload into `cqlite_core::Config`.
    let mut value = serde_json::to_value(Config::default()).unwrap();
    let storage = value
        .get_mut("storage")
        .and_then(|s| s.as_object_mut())
        .unwrap();
    storage.remove("use_mmap");
    storage.remove("mmap_min_size_bytes");

    let restored: Config =
        serde_json::from_value(value).expect("old Config payload must still deserialize");
    assert!(!restored.storage.use_mmap);
    assert_eq!(restored.storage.mmap_min_size_bytes, 4096);
    restored.validate().expect("restored config must validate");
}

#[test]
fn test_mmap_fields_roundtrip_when_present() {
    // When the fields ARE present (e.g. a user opting in), they round-trip.
    let mut config = StorageConfig::default();
    config.use_mmap = true;
    config.mmap_min_size_bytes = 8192;
    let json = serde_json::to_string(&config).unwrap();
    let restored: StorageConfig = serde_json::from_str(&json).unwrap();
    assert!(restored.use_mmap);
    assert_eq!(restored.mmap_min_size_bytes, 8192);
}

#[test]
fn test_disk_access_defaults() {
    // The new fields default to the size-aware Auto backend with Auto
    // prefetch, a half-RAM direct-I/O threshold, and a 1 MiB window.
    let config = StorageConfig::default();
    assert_eq!(config.disk_access_mode, DiskAccessMode::Auto);
    assert_eq!(config.prefetch, PrefetchMode::Auto);
    assert_eq!(config.direct_io_memory_fraction, 0.5);
    assert_eq!(config.direct_io_prefetch_bytes, 1024 * 1024);
}

#[test]
fn test_storage_config_deserializes_without_disk_access_fields() {
    // Backward compatibility: a payload predating the disk-access fields must
    // still deserialize, defaulting to Auto / Auto / 0.5 / 1 MiB.
    let mut value = serde_json::to_value(StorageConfig::default()).unwrap();
    let obj = value.as_object_mut().unwrap();
    for key in [
        "disk_access_mode",
        "direct_io_memory_fraction",
        "prefetch",
        "direct_io_prefetch_bytes",
    ] {
        obj.remove(key);
    }

    let restored: StorageConfig =
        serde_json::from_value(value).expect("old payload must still deserialize");
    assert_eq!(restored.disk_access_mode, DiskAccessMode::Auto);
    assert_eq!(restored.prefetch, PrefetchMode::Auto);
    assert_eq!(restored.direct_io_memory_fraction, 0.5);
    assert_eq!(restored.direct_io_prefetch_bytes, 1024 * 1024);
}

#[test]
fn test_disk_access_fields_roundtrip() {
    // Explicit selections round-trip, including the lowercase enum encoding.
    let mut config = StorageConfig::default();
    config.disk_access_mode = DiskAccessMode::Direct;
    config.prefetch = PrefetchMode::WillNeed;
    config.direct_io_memory_fraction = 0.25;
    config.direct_io_prefetch_bytes = 2 * 1024 * 1024;
    let json = serde_json::to_string(&config).unwrap();
    assert!(json.contains("\"direct\""), "enum must serialize lowercase");
    assert!(json.contains("\"willneed\""));
    let restored: StorageConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(restored.disk_access_mode, DiskAccessMode::Direct);
    assert_eq!(restored.prefetch, PrefetchMode::WillNeed);
    assert_eq!(restored.direct_io_memory_fraction, 0.25);
    assert_eq!(restored.direct_io_prefetch_bytes, 2 * 1024 * 1024);
}

#[test]
fn test_bloom_filter_disabled() {
    let mut config = Config::default();
    config.storage.enable_bloom_filters = false;
    config.storage.bloom_filter_fp_rate = 0.0; // Should be ignored when bloom filters disabled
    assert!(config.validate().is_ok());

    config.storage.bloom_filter_fp_rate = 1.0; // Should be ignored when bloom filters disabled
    assert!(config.validate().is_ok());

    config.storage.bloom_filter_fp_rate = -1.0; // Should be ignored when bloom filters disabled
    assert!(config.validate().is_ok());
}

// ---- issue #1568 (Epic B / B2): dead-cache config collapse ----

/// Spec: "A config using a removed knob is rejected." The pre-change
/// `MemoryConfig` shape (`row_cache` / `query_cache` / `allocator` alongside
/// `block_cache`) must FAIL CLOSED after the collapse — those keys are gone,
/// and `#[serde(deny_unknown_fields)]` rejects them instead of silently
/// ignoring them (which would suggest they still have effect).
#[test]
fn removed_memory_knobs_fail_closed() {
    // The full OLD memory-config shape: valid on pre-change code (RED),
    // rejected after the collapse (GREEN).
    let old_shape = serde_json::json!({
        "max_memory": 1_073_741_824u64,
        "block_cache": { "enabled": true, "max_size": 268_435_456u64, "policy": "Lru" },
        "row_cache":   { "enabled": true, "max_size": 134_217_728u64, "policy": "Lru" },
        "query_cache": { "enabled": true, "max_size": 67_108_864u64,  "policy": "Lru" },
        "allocator":   { "use_custom": false, "small_pool_size": 1u64, "large_pool_size": 2u64 }
    });
    assert!(
        serde_json::from_value::<MemoryConfig>(old_shape).is_err(),
        "a MemoryConfig naming removed knobs (row_cache/query_cache/allocator) must fail closed"
    );

    // Each removed knob, added to the retained-only base, is rejected.
    for removed in ["row_cache", "query_cache", "allocator"] {
        let mut v = serde_json::json!({
            "max_memory": 1_073_741_824u64,
            "block_cache": { "enabled": true, "max_size": 268_435_456u64, "policy": "Lru" },
        });
        v.as_object_mut()
            .unwrap()
            .insert(removed.to_string(), serde_json::json!({ "enabled": true }));
        assert!(
            serde_json::from_value::<MemoryConfig>(v).is_err(),
            "a MemoryConfig naming the removed `{removed}` knob must fail closed"
        );
    }
}

/// Spec: "A config using a removed knob is rejected" (CachePolicy variants).
/// The never-selected `Lfu` / `Arc` variants are gone, so a cache config
/// naming them fails to deserialize (unknown variant) rather than silently
/// mapping to some default.
#[test]
fn removed_cache_policy_variants_fail_closed() {
    for variant in ["Lfu", "Arc"] {
        let v = serde_json::json!({ "enabled": true, "max_size": 1u64, "policy": variant });
        assert!(
            serde_json::from_value::<CacheConfig>(v).is_err(),
            "a CacheConfig naming the removed CachePolicy::{variant} variant must fail closed"
        );
    }
    // The retained `Lru` variant still deserializes.
    let ok = serde_json::json!({ "enabled": true, "max_size": 1u64, "policy": "Lru" });
    assert!(serde_json::from_value::<CacheConfig>(ok).is_ok());
}

/// Spec: "The retained budget knob still deserializes and validates." A
/// config specifying only `max_memory` and `block_cache` deserializes, passes
/// `Config::validate()`, and its `block_cache.max_size` is the retained knob.
#[test]
fn retained_budget_knob_deserializes_and_validates() {
    let mem: MemoryConfig = serde_json::from_value(serde_json::json!({
        "max_memory": 1_073_741_824u64,
        "block_cache": { "enabled": true, "max_size": 268_435_456u64, "policy": "Lru" },
    }))
    .expect("retained-only MemoryConfig must deserialize");
    assert_eq!(mem.block_cache.max_size, 268_435_456);

    // A default Config (which now carries only the collapsed MemoryConfig)
    // still validates, and the block-cache budget is the wired knob.
    let config = Config::default();
    assert!(config.validate().is_ok());
    assert!(config.memory.block_cache.max_size > 0);
}

// ---- issue #1697 (AH4): the write-path knobs the public Config newly owns ----

/// Item 5: `memtable_hard_limit > memtable_size_threshold`, STRICTLY. A ceiling
/// at or below the flush threshold wedges the engine — `check_admission` rejects
/// the write before a flush can relieve the memtable — so it must be rejected at
/// config time.
#[test]
fn hard_limit_not_above_flush_threshold_is_rejected_and_names_both_values() {
    let mut config = Config::default();
    config.storage.memtable_size_threshold = 64 * 1024 * 1024;
    config.storage.memtable_hard_limit = 32 * 1024 * 1024;

    let err = config
        .validate()
        .expect_err("a ceiling below the flush threshold wedges the engine")
        .to_string();
    // An operator needs the two colliding numbers, not just the rule name.
    assert!(err.contains(&(64 * 1024 * 1024).to_string()), "{err}");
    assert!(err.contains(&(32 * 1024 * 1024).to_string()), "{err}");

    // Equality is REJECTED (#1697 roborev r2). It was previously accepted as
    // "tight but coherent"; that was wrong, and measurably so. With zero headroom
    // the wedge window for a mutation of `m` bytes is `m` bytes wide, so an
    // ORDINARY 4 KiB write livelocks whenever the memtable sits within 4 KiB
    // below the threshold — a state normal operation passes through routinely:
    // admission rejects at the ceiling, the memtable never reaches the flush
    // trigger, and retrying never recovers. Equality also asks the engine to
    // flush at exactly the size at which it must instead reject, which no
    // legitimate configuration wants.
    config.storage.memtable_hard_limit = config.storage.memtable_size_threshold;
    let err = config
        .validate()
        .expect_err("hard_limit == memtable_size_threshold leaves no headroom")
        .to_string();
    assert!(
        err.contains("strictly greater"),
        "the rejection must say the bound is STRICT, not merely name the rule: {err}"
    );

    // ...and one byte of headroom is accepted, so the rule is a boundary and not
    // a blanket refusal. This is deliberately NOT a wedge-freedom claim: a
    // mutation larger than the headroom still wedges, which is #3404's subject,
    // not this rule's.
    config.storage.memtable_hard_limit = config.storage.memtable_size_threshold + 1;
    config
        .validate()
        .expect("one byte of headroom is a coherent, if tight, configuration");
}

/// Item 5: `compaction.min_threshold > 0`. STCS with a zero eligibility bar is
/// meaningless; `STCSPolicy::new` rejects it too, but failing here surfaces it
/// at config time rather than at engine construction.
#[test]
fn zero_compaction_min_threshold_is_rejected() {
    let mut config = Config::default();
    config.storage.compaction.min_threshold = 0;
    let err = config
        .validate()
        .expect_err("min_threshold 0 must be rejected")
        .to_string();
    assert!(err.contains("compaction.min_threshold"), "{err}");

    config.storage.compaction.min_threshold = 1;
    config.validate().expect("min_threshold 1 must be accepted");
}

/// Item 5, the CONDITION on both compaction rules (#1697 roborev r4): they apply
/// only when `auto_compaction` is on.
///
/// Both fields are documented as "Ignored when `auto_compaction` is `false`", and
/// that is literally true of the code — `WriteEngine::new` builds
/// `STCSPolicy::new(min, max, ..)` inside `if config.auto_compaction` and leaves
/// the policy unset otherwise. Judging them unconditionally rejected
/// configurations that work, since the thresholds are never read.
#[test]
fn compaction_thresholds_are_only_judged_when_auto_compaction_is_on() {
    // Values that are nonsense FOR STCS, on a config where STCS never runs.
    let mut config = Config::default();
    config.storage.compaction.auto_compaction = false;
    config.storage.compaction.min_threshold = 0;
    config.storage.compaction.max_threshold = 0;
    config
        .validate()
        .expect("thresholds documented as ignored must not be judged");

    // NON-VACUITY: the SAME thresholds with compaction ENABLED are still fatal,
    // so the condition did not disable the rules — it scoped them.
    config.storage.compaction.auto_compaction = true;
    let err = config
        .validate()
        .expect_err("with compaction on, a zero eligibility bar is still refused")
        .to_string();
    assert!(err.contains("compaction.min_threshold"), "{err}");

    // ...and the max-below-min rule is scoped the same way, checked separately so
    // one rule's condition cannot stand in as evidence for the other's.
    let mut config = Config::default();
    config.storage.compaction.auto_compaction = false;
    config.storage.compaction.min_threshold = 8;
    config.storage.compaction.max_threshold = 4;
    config
        .validate()
        .expect("an ignored merge-width cap below the bar must not be judged");
    config.storage.compaction.auto_compaction = true;
    let err = config
        .validate()
        .expect_err("with compaction on, max below min is still refused")
        .to_string();
    assert!(err.contains("compaction.max_threshold"), "{err}");
}

/// Item 5: `compaction.max_threshold >= min_threshold` — a merge-width cap below
/// the eligibility bar can never admit a merge.
#[test]
fn compaction_max_below_min_is_rejected_and_names_both_values() {
    let mut config = Config::default();
    config.storage.compaction.min_threshold = 8;
    config.storage.compaction.max_threshold = 4;

    let err = config
        .validate()
        .expect_err("max_threshold below min_threshold can never admit a merge")
        .to_string();
    assert!(err.contains('8'), "{err}");
    assert!(err.contains('4'), "{err}");

    // Equality is the boundary and is ALLOWED: exactly-N-way merges.
    config.storage.compaction.max_threshold = 8;
    config
        .validate()
        .expect("max_threshold == min_threshold must be accepted");
}

/// Item 6: a byte count above the target's `usize::MAX` must be REJECTED, not
/// clamped. Clamping lands on `usize::MAX`, where `should_flush` never fires AND
/// `check_admission`'s `projected > hard_limit` is unreachable (`saturating_add`
/// caps there) — never flush, never reject, grow until OOM.
///
/// Only reachable on a 32-bit/wasm32 target, so the test is written against the
/// target's own bound: on 64-bit there is no such `u64` and the rule is vacuous,
/// which the `is_none()` branch states explicitly rather than silently skipping.
#[test]
fn a_byte_count_above_the_targets_usize_max_is_rejected_not_clamped() {
    let usize_max_bytes = u64::try_from(usize::MAX).unwrap_or(u64::MAX);
    let Some(unaddressable) = usize_max_bytes.checked_add(1) else {
        // 64-bit: `usize::MAX == u64::MAX`, so every u64 is addressable and the
        // rule cannot fire. Assert exactly that, so this test never reads as
        // coverage it does not provide.
        assert_eq!(usize_max_bytes, u64::MAX);
        return;
    };

    for knob in ["memtable_size_threshold", "memtable_hard_limit"] {
        let mut config = Config::default();
        match knob {
            "memtable_size_threshold" => {
                config.storage.memtable_size_threshold = unaddressable;
                config.storage.memtable_hard_limit = unaddressable;
            }
            _ => config.storage.memtable_hard_limit = unaddressable,
        }
        let err = config
            .validate()
            .expect_err("an unaddressable byte count must be rejected")
            .to_string();
        assert!(err.contains("addressable"), "{knob}: {err}");
    }
}

/// Item 7: the three fields #1697 added carry `#[serde(default = ...)]`, and the
/// Python bindings are a serde JSON bridge that requires a COMPLETE dict — so a
/// config payload written before these fields existed must still deserialize.
/// Delete an attribute and this test fails; without it, every pre-upgrade
/// Python config dict would break silently.
#[test]
fn storage_config_deserializes_without_the_issue_1697_fields() {
    let mut value = serde_json::to_value(StorageConfig::default()).unwrap();
    let obj = value.as_object_mut().unwrap();
    obj.remove("memtable_hard_limit");
    let compaction = obj
        .get_mut("compaction")
        .and_then(|c| c.as_object_mut())
        .unwrap();
    compaction.remove("min_threshold");
    compaction.remove("max_threshold");
    assert!(!compaction.contains_key("min_threshold"));

    let restored: StorageConfig =
        serde_json::from_value(value).expect("a pre-#1697 payload must still deserialize");
    let defaults = StorageConfig::default();
    assert_eq!(
        restored.memtable_hard_limit, defaults.memtable_hard_limit,
        "absent memtable_hard_limit must default to the 256MB admission ceiling"
    );
    assert_eq!(
        restored.compaction.min_threshold, defaults.compaction.min_threshold,
        "absent compaction.min_threshold must default to STCS's 4"
    );
    assert_eq!(
        restored.compaction.max_threshold, defaults.compaction.max_threshold,
        "absent compaction.max_threshold must default to STCS's 32"
    );
}

/// Item 7, through the top-level `Config` — the shape the Python bindings
/// actually parse — and it must still VALIDATE, not merely deserialize.
#[test]
fn full_config_deserializes_and_validates_without_the_issue_1697_fields() {
    let mut value = serde_json::to_value(Config::default()).unwrap();
    let storage = value
        .get_mut("storage")
        .and_then(|s| s.as_object_mut())
        .unwrap();
    storage.remove("memtable_hard_limit");
    let compaction = storage
        .get_mut("compaction")
        .and_then(|c| c.as_object_mut())
        .unwrap();
    compaction.remove("min_threshold");
    compaction.remove("max_threshold");

    let restored: Config =
        serde_json::from_value(value).expect("a pre-#1697 Config payload must still deserialize");
    restored
        .validate()
        .expect("the defaulted fields must satisfy validate's new rules");
    assert_eq!(restored.storage.memtable_hard_limit, 256 * 1024 * 1024);
    assert_eq!(restored.storage.compaction.min_threshold, 4);
    assert_eq!(restored.storage.compaction.max_threshold, 32);
}

/// The #1697 fields round-trip when PRESENT, so the defaults above are a
/// fallback rather than an override that ignores what the caller wrote.
#[test]
fn the_issue_1697_fields_roundtrip_when_present() {
    let mut config = StorageConfig::default();
    config.memtable_hard_limit = 512 * 1024 * 1024;
    config.compaction.min_threshold = 6;
    config.compaction.max_threshold = 12;
    let json = serde_json::to_string(&config).unwrap();
    let restored: StorageConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(restored.memtable_hard_limit, 512 * 1024 * 1024);
    assert_eq!(restored.compaction.min_threshold, 6);
    assert_eq!(restored.compaction.max_threshold, 12);
}
