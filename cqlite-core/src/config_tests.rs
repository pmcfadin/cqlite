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
