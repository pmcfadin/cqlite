//! Issue #1696 (AH3, epic #1685 "config honesty"): the STANDING GUARD that every
//! surviving public [`Config`] knob has EVIDENCE it does something.
//!
//! # What this file is for
//!
//! Epic #1685 exists because CQLite shipped a public configuration surface whose
//! fields were, in large part, decoration: an embedder set `storage.block_size`,
//! `query.enable_optimization` or the whole `performance` tree and NOTHING in the
//! engine ever read them. #1696 deleted those. This file is what stops them from
//! coming back — and, more importantly, what stops a NEW knob from being added
//! without anyone asking whether it is read.
//!
//! # The contract
//!
//! Every leaf field of the public config structs must appear exactly once in
//! [`KNOBS`] with one of three dispositions:
//!
//! * [`Evidence::Here`] — a set-knob → assert-observable-difference test IN THIS
//!   FILE. The named function must exist here.
//! * [`Evidence::CoveredBy`] — that same test, but living in another compiled
//!   test target (usually the issue lane that wired the knob). The named file
//!   must exist and must declare the named function.
//! * [`Evidence::Reserved`] — NO observable difference is expressible from the
//!   public surface, plus the reason and the owning issue. This is the escape
//!   hatch and it is meant to read like an admission, not a pass.
//!
//! [`Evidence::Container`] is not a disposition: it marks a field that is itself
//! a struct of knobs (`storage`, `memory.block_cache`, …), whose own leaves are
//! registered separately.
//!
//! # Why the registry is checked against SOURCE
//!
//! A hand-maintained ledger rots silently, which is the exact failure mode this
//! epic is about. [`the_registry_names_every_public_config_field`] therefore reads
//! `src/config.rs` and asserts the registry and the struct definitions agree in
//! BOTH directions — a new `pub` field with no registry entry FAILS, and a
//! registry entry naming a field that no longer exists FAILS. It scans only the
//! seven NAMED structs, one line at a time, and **refuses rather than guesses**:
//! a struct it cannot find, or one whose body does not close, is a failure.
//!
//! # What `CoveredBy` does NOT prove (a deliberate, bounded proxy)
//!
//! [`Evidence::CoveredBy`] asserts that the named file exists and declares the
//! named function. It does **not** — and cannot — assert that that function
//! actually observes a behavioral difference. A test in another lane could be
//! weakened to a structural or round-trip assertion, keep its name, and this
//! guard would still pass.
//!
//! That is an accepted limit rather than an oversight: proving "some function in
//! another crate's test target asserts a behavioral difference" is not
//! mechanizable from here. It is recorded at the seam because the entire purpose
//! of this file is honesty about what is verified, and a guard that overstates
//! its own reach is the same defect class as a decorative knob. The mitigation is
//! social, not mechanical: `CoveredBy` names a specific function so a reviewer
//! deleting or weakening it can see what depended on it.
//!
//! # Scope boundaries (deliberate)
//!
//! * `storage.compaction.*` is owned by #1619, `memory.*` by #1568,
//!   `query.max_execution_time` by #1695. They are registered (their evidence is
//!   real and lives in those lanes) but not modified here.
//! * `wasm` is `#[cfg(target_arch = "wasm32")]` and was not in #1696's census.

use std::path::{Path, PathBuf};

use cqlite_core::config::{
    CachePolicy, CompressionAlgorithm, Config, DiskAccessMode, PrefetchMode, QueryConfig,
    StorageConfig,
};

// ---------------------------------------------------------------- the registry

/// How a knob proves it is not decoration.
#[derive(Debug, Clone, Copy)]
enum Evidence {
    /// Set-knob → observable-difference test in THIS file, by name.
    Here(&'static str),
    /// Set-knob → observable-difference test in another test target.
    CoveredBy {
        /// Path relative to `cqlite-core/`.
        file: &'static str,
        /// The `fn` that drives the knob and asserts the difference.
        test_fn: &'static str,
    },
    /// No observable difference is expressible from the public surface. The
    /// string must say WHY and name the owning issue.
    Reserved(&'static str),
    /// Not a knob: a nested struct whose own leaves are registered separately.
    Container,
}

/// One field of the public config surface.
#[derive(Debug, Clone, Copy)]
struct Knob {
    /// Dotted path from `Config`, e.g. `storage.memtable_hard_limit`.
    path: &'static str,
    /// The struct the field is declared on (must be one of [`SCANNED_STRUCTS`]).
    declared_on: &'static str,
    /// The field's own identifier.
    field: &'static str,
    evidence: Evidence,
}

/// The write-path lane that wired the memtable + STCS knobs (#1697).
const LANE_1697: &str = "tests/issue_1697_config_single_source.rs";
/// The result-budget lane (#1582).
const LANE_1582: &str = "tests/issue_1582_byte_bounded_result_budget.rs";
/// The query-timeout lane (#1695).
const LANE_1695: &str = "tests/issue_1695_query_timeout.rs";
/// The forced-read-path lane (#1918).
const LANE_1918: &str = "tests/read_path_forcing_schemaless_1918.rs";
/// The dead-cache-collapse lane (#1568), which owns `memory.block_cache.*`.
const LANE_1568: &str = "tests/dead_cache_delete_tests.rs";

/// EVERY leaf and container field of the public config surface.
const KNOBS: &[Knob] = &[
    // ---- Config ----
    Knob {
        path: "storage",
        declared_on: "Config",
        field: "storage",
        evidence: Evidence::Container,
    },
    Knob {
        path: "memory",
        declared_on: "Config",
        field: "memory",
        evidence: Evidence::Container,
    },
    Knob {
        path: "query",
        declared_on: "Config",
        field: "query",
        evidence: Evidence::Container,
    },
    Knob {
        path: "wasm",
        declared_on: "Config",
        field: "wasm",
        evidence: Evidence::Reserved(
            "cfg(target_arch = \"wasm32\") only; WasmConfig was NOT in #1696's census and \
             M6 (WASM bindings) has not landed, so nothing on any supported target reads \
             it. Owned by the M6 (WASM bindings) milestone; there is deliberately NO \
             follow-up issue, because a target-gated struct for an unlanded milestone is \
             not a decorative knob.",
        ),
    },
    // ---- StorageConfig ----
    Knob {
        path: "storage.memtable_size_threshold",
        declared_on: "StorageConfig",
        field: "memtable_size_threshold",
        evidence: Evidence::CoveredBy {
            file: LANE_1697,
            test_fn: "public_memtable_threshold_drives_flush",
        },
    },
    Knob {
        path: "storage.memtable_hard_limit",
        declared_on: "StorageConfig",
        field: "memtable_hard_limit",
        evidence: Evidence::CoveredBy {
            file: LANE_1697,
            test_fn: "public_memtable_hard_limit_knob_is_load_bearing",
        },
    },
    Knob {
        path: "storage.compaction",
        declared_on: "StorageConfig",
        field: "compaction",
        evidence: Evidence::Container,
    },
    Knob {
        path: "storage.compression",
        declared_on: "StorageConfig",
        field: "compression",
        evidence: Evidence::Container,
    },
    Knob {
        path: "storage.use_mmap",
        declared_on: "StorageConfig",
        field: "use_mmap",
        evidence: Evidence::Reserved(
            "LIVE (reader/mod.rs promotes an explicit Buffered request to Mmap) but its \
             effect is a PERFORMANCE property — which I/O backend serves Data.db — and \
             SSTableReader exposes no accessor for the resolved backend. The only \
             correctness property is EQUIVALENCE across backends, pinned by \
             tests/issue_1593_mmap_scan_parity.rs. Owning issue: #1593.",
        ),
    },
    Knob {
        path: "storage.mmap_min_size_bytes",
        declared_on: "StorageConfig",
        field: "mmap_min_size_bytes",
        evidence: Evidence::Reserved(
            "LIVE (gates the DiskAccessMode::Auto mmap threshold) but performance-only, \
             for the same reason as storage.use_mmap: no resolved-backend accessor. \
             Owning issue: #1593.",
        ),
    },
    Knob {
        path: "storage.disk_access_mode",
        declared_on: "StorageConfig",
        field: "disk_access_mode",
        evidence: Evidence::Reserved(
            "LIVE (selects buffered/mmap/direct at reader open) but performance-only: no \
             resolved-backend accessor exists, so the publicly observable property is \
             equivalence, pinned by tests/issue_1593_mmap_scan_parity.rs and \
             tests/issue_1143_mmap_prefetch_tail_guard.rs. Owning issues: #1593, #1143.",
        ),
    },
    Knob {
        path: "storage.direct_io_memory_fraction",
        declared_on: "StorageConfig",
        field: "direct_io_memory_fraction",
        evidence: Evidence::Here("out_of_range_direct_io_memory_fraction_is_rejected"),
    },
    Knob {
        path: "storage.prefetch",
        declared_on: "StorageConfig",
        field: "prefetch",
        evidence: Evidence::Reserved(
            "LIVE (madvise advice / direct-I/O read-ahead window) but performance-only; \
             the p99 property it exists for is pinned by \
             tests/issue_1143_mmap_prefetch_tail_guard.rs. Owning issue: #1143.",
        ),
    },
    Knob {
        path: "storage.direct_io_prefetch_bytes",
        declared_on: "StorageConfig",
        field: "direct_io_prefetch_bytes",
        evidence: Evidence::Reserved(
            "LIVE (direct-I/O read-ahead window size) but performance-only, and inert \
             unless the direct backend is selected. Owning issue: #1143.",
        ),
    },
    // ---- CompactionConfig (#1619 / #1697) ----
    Knob {
        path: "storage.compaction.auto_compaction",
        declared_on: "CompactionConfig",
        field: "auto_compaction",
        evidence: Evidence::CoveredBy {
            file: LANE_1697,
            test_fn: "public_auto_compaction_off_disables_compaction",
        },
    },
    Knob {
        path: "storage.compaction.min_threshold",
        declared_on: "CompactionConfig",
        field: "min_threshold",
        evidence: Evidence::CoveredBy {
            file: LANE_1697,
            test_fn: "public_compaction_thresholds_drive_stcs",
        },
    },
    Knob {
        path: "storage.compaction.max_threshold",
        declared_on: "CompactionConfig",
        field: "max_threshold",
        evidence: Evidence::CoveredBy {
            file: LANE_1697,
            test_fn: "public_compaction_max_threshold_caps_merge_width",
        },
    },
    // ---- CompressionConfig ----
    // FINDING (#1696): this whole struct is decorative. `Config::storage.compression`
    // has ZERO production readers — the `compression` on `SSTableHeader` is an
    // unrelated type, and the read path takes its algorithm from
    // `CompressionInfo.db`, never from config (the no-heuristics mandate requires
    // exactly that). It was NOT in #1696's census, so deleting it is out of this
    // lane's scope; it is registered here so the gap is on the record.
    Knob {
        path: "storage.compression.enabled",
        declared_on: "CompressionConfig",
        field: "enabled",
        evidence: Evidence::Reserved(
            "DECORATIVE: zero production readers. The read path takes its algorithm from \
             CompressionInfo.db, as the no-heuristics mandate requires, and the write \
             path is uncompressed-only per #1406 — so there is nothing for these knobs to \
             steer. Found while auditing #1696 but NOT in its census, so deleting it was \
             out of that lane's scope. NO dedicated issue exists; it belongs to the open \
             config-honesty epic #1685, which owns the remaining decorative surface.",
        ),
    },
    Knob {
        path: "storage.compression.algorithm",
        declared_on: "CompressionConfig",
        field: "algorithm",
        evidence: Evidence::Reserved(
            "DECORATIVE: zero production readers, same as storage.compression.enabled.",
        ),
    },
    Knob {
        path: "storage.compression.level",
        declared_on: "CompressionConfig",
        field: "level",
        evidence: Evidence::Reserved(
            "DECORATIVE: zero production readers, same as storage.compression.enabled.",
        ),
    },
    Knob {
        path: "storage.compression.min_block_size",
        declared_on: "CompressionConfig",
        field: "min_block_size",
        evidence: Evidence::Reserved(
            "DECORATIVE: zero production readers, same as storage.compression.enabled.",
        ),
    },
    // ---- MemoryConfig (#1568) ----
    Knob {
        path: "memory.max_memory",
        declared_on: "MemoryConfig",
        field: "max_memory",
        evidence: Evidence::Here("memory_max_memory_is_load_bearing_at_validation"),
    },
    Knob {
        path: "memory.block_cache",
        declared_on: "MemoryConfig",
        field: "block_cache",
        evidence: Evidence::Container,
    },
    Knob {
        path: "memory.block_cache.enabled",
        declared_on: "CacheConfig",
        field: "enabled",
        evidence: Evidence::CoveredBy {
            file: LANE_1568,
            test_fn: "stats_block_cache_disabled_yields_no_caching",
        },
    },
    Knob {
        path: "memory.block_cache.max_size",
        declared_on: "CacheConfig",
        field: "max_size",
        evidence: Evidence::CoveredBy {
            file: LANE_1568,
            test_fn: "config_block_cache_max_size_is_the_b1_budget",
        },
    },
    Knob {
        path: "memory.block_cache.policy",
        declared_on: "CacheConfig",
        field: "policy",
        evidence: Evidence::Reserved(
            "SINGLE-VARIANT: #1568 deleted the never-selected Lfu/Arc variants, so \
             CachePolicy::Lru is the only value and there is no alternative to observe a \
             difference against. Pinned as single-variant by \
             policy_is_a_single_variant_so_no_difference_is_expressible below. Owning \
             issue: #1568.",
        ),
    },
    // ---- QueryConfig ----
    Knob {
        path: "query.max_execution_time",
        declared_on: "QueryConfig",
        field: "max_execution_time",
        evidence: Evidence::CoveredBy {
            file: LANE_1695,
            test_fn: "budget_of_one_millisecond_times_out_a_large_scan",
        },
    },
    Knob {
        path: "query.forced_read_path",
        declared_on: "QueryConfig",
        field: "forced_read_path",
        evidence: Evidence::CoveredBy {
            file: LANE_1918,
            test_fn: "full_schemaless_sole_pk_lookup_fails_closed_not_silent_zero",
        },
    },
    Knob {
        path: "query.max_result_rows",
        declared_on: "QueryConfig",
        field: "max_result_rows",
        evidence: Evidence::CoveredBy {
            file: LANE_1582,
            test_fn: "max_result_rows_knob_is_load_bearing",
        },
    },
    Knob {
        path: "query.max_result_bytes",
        declared_on: "QueryConfig",
        field: "max_result_bytes",
        evidence: Evidence::CoveredBy {
            file: LANE_1582,
            test_fn: "max_result_bytes_knob_is_load_bearing",
        },
    },
    Knob {
        path: "query.query_cache_size",
        declared_on: "QueryConfig",
        field: "query_cache_size",
        evidence: Evidence::Reserved(
            "LIVE (query/engine.rs gates plan caching on it) but observable only through \
             QueryEngine::cache_stats(), which the public Database facade does not expose, \
             and only after a query has EXECUTED against a populated store. The in-crate \
             attempt at that observation (query::engine::tests::test_query_caching) is \
             #[ignore]d for hanging, so no non-flaky observation exists today. NO \
             dedicated issue exists for this facade-observability gap; it belongs to the \
             open config-honesty epic #1685.",
        ),
    },
    Knob {
        path: "query.query_parallelism",
        declared_on: "QueryConfig",
        field: "query_parallelism",
        evidence: Evidence::Here("query_parallelism_changes_the_planned_thread_count"),
    },
    Knob {
        path: "query.analyze_iterations",
        declared_on: "QueryConfig",
        field: "analyze_iterations",
        evidence: Evidence::Reserved(
            "LIVE (bounds the QueryEngine::analyze measurement loop) but QueryEngine::analyze \
             is not reachable from the public Database facade — there is no \
             Database::analyze — so the knob has no publicly observable effect. NO \
             dedicated issue exists for this facade-observability gap; it belongs to the \
             open config-honesty epic #1685.",
        ),
    },
];

// ------------------------------------------- set-knob -> observable-difference

/// AC2 (#1696): `storage.direct_io_memory_fraction` was LIVE but UNVALIDATED —
/// `resolve_disk_access_mode` silently CLAMPED a nonsense value (`<= 0.0`, NaN
/// and infinities fell back to `0.5`; anything `> 1.0` was pinned to `1.0`). An
/// operator who wrote `2.0` or `-1.0` got the default and no word about it,
/// which is the same dishonesty as a decorative knob wearing a different hat.
///
/// `Config::validate` now REJECTS the whole documented-illegal range, and the
/// message names the knob and the offending value.
///
/// SCOPE OF THIS TEST, stated because it was once overstated (#1696 roborev F2):
/// it pins the RULE, at the one place the rule is written
/// (`StorageConfig::validated_direct_io_memory_fraction`, via `validate`). It is
/// NOT the wiring evidence — a test that calls `validate()` by hand cannot show
/// that any public surface calls it, and for a while none did, so an operator
/// setting `2.0` through `Database::open` was still silently clamped with this
/// test green beside them. The PUBLIC-surface evidence is
/// `tests/issue_1696_direct_io_fraction_validation.rs`, which drives every case
/// through `Database::open` and `SSTableReader::open` and never through
/// `validate`. Both are needed: this one for the rule, that one for the reach.
#[test]
fn out_of_range_direct_io_memory_fraction_is_rejected() {
    // Every value the field doc calls illegal.
    for bad in [
        0.0,
        -0.0,
        -0.25,
        1.000_000_1,
        2.0,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ] {
        let mut config = Config::default();
        config.storage.direct_io_memory_fraction = bad;
        let err = config
            .validate()
            .expect_err("direct_io_memory_fraction outside (0.0, 1.0] must be REJECTED");
        let message = err.to_string();
        assert!(
            message.contains("direct_io_memory_fraction"),
            "the error must name the knob, got: {message}"
        );
    }

    // The documented-legal range still validates, endpoints included.
    for good in [f64::MIN_POSITIVE, 0.000_1, 0.25, 0.5, 1.0] {
        let mut config = Config::default();
        config.storage.direct_io_memory_fraction = good;
        config
            .validate()
            .unwrap_or_else(|e| panic!("{good} is inside (0.0, 1.0] and must validate: {e}"));
    }

    // And the shipped default is inside the range it enforces.
    Config::default()
        .validate()
        .expect("the shipped default must validate");
}

/// `memory.max_memory` (owned by #1568, out of #1696's delete scope) is not read
/// by any cache at runtime, but it IS load-bearing at validation: it is the
/// ceiling `block_cache.max_size` is judged against. Setting it changes what
/// `validate` accepts, which is an observable difference.
#[test]
fn memory_max_memory_is_load_bearing_at_validation() {
    // Zero is rejected outright.
    let mut config = Config::default();
    config.memory.max_memory = 0;
    assert!(
        config.validate().is_err(),
        "max_memory = 0 must be rejected"
    );

    // A cache budget legal under a large ceiling becomes illegal under a small
    // one — the SAME block_cache.max_size, two different verdicts.
    let cache_bytes = 128 * 1024 * 1024;

    let mut roomy = Config::default();
    roomy.memory.max_memory = cache_bytes * 2;
    roomy.memory.block_cache.max_size = cache_bytes;
    roomy
        .validate()
        .expect("a cache under the ceiling must validate");

    let mut cramped = Config::default();
    cramped.memory.max_memory = cache_bytes - 1;
    cramped.memory.block_cache.max_size = cache_bytes;
    assert!(
        cramped.validate().is_err(),
        "lowering max_memory below the cache budget must change the verdict"
    );
}

/// `query.query_parallelism` reaches the planner: it is the `suggested_threads`
/// every parallelizable execution step carries. Two different values must
/// produce two different plans.
#[tokio::test]
async fn query_parallelism_changes_the_planned_thread_count() {
    use cqlite_core::query::{QueryParser, QueryPlanner};
    use cqlite_core::schema::SchemaManager;

    let temp = tempfile::TempDir::new().expect("tempdir");
    let schema = std::sync::Arc::new(
        SchemaManager::new(temp.path())
            .await
            .expect("schema manager"),
    );

    // An explicit (non-`*`) projection guarantees a Project step, which is one
    // of the steps the planner builds with the configured thread count.
    const CQL: &str = "SELECT a FROM ks.t WHERE a = 1";

    async fn plan_threads(
        schema: std::sync::Arc<SchemaManager>,
        threads: usize,
        cql: &str,
    ) -> Vec<usize> {
        let mut config = Config::default();
        config.query.query_parallelism = Some(threads);
        let parsed = QueryParser::new(&config).parse(cql).expect("parse");
        let plan = QueryPlanner::new(schema, &config)
            .plan(&parsed)
            .await
            .expect("plan");
        plan.steps
            .iter()
            .filter(|s| s.parallelization.can_parallelize)
            .map(|s| s.parallelization.suggested_threads)
            .collect()
    }

    let three = plan_threads(schema.clone(), 3, CQL).await;
    let seven = plan_threads(schema, 7, CQL).await;

    assert!(
        !three.is_empty(),
        "the plan must contain at least one parallelizable step for this to mean anything"
    );
    assert!(
        three.iter().all(|&t| t == 3),
        "every parallelizable step must carry the configured thread count, got {three:?}"
    );
    assert!(
        seven.iter().all(|&t| t == 7),
        "every parallelizable step must carry the configured thread count, got {seven:?}"
    );
    assert_ne!(
        three, seven,
        "two different query_parallelism values must produce two different plans"
    );
}

/// Pins the premise of `memory.block_cache.policy`'s `Reserved` entry: the enum
/// really has exactly one variant, so "set the knob to something else" is not
/// expressible. If a second variant is ever added, this fails and the registry
/// entry has to be replaced with a real behavior test.
#[test]
fn policy_is_a_single_variant_so_no_difference_is_expressible() {
    let policy = Config::default().memory.block_cache.policy;
    // Exhaustive: adding a variant makes this match non-exhaustive and the
    // Reserved justification above stops being true.
    match policy {
        CachePolicy::Lru => {}
    }
}

// ------------------------------------------------------- registry consistency

/// The public config structs this file's registry claims to cover, in
/// declaration order. A struct missing from `src/config.rs` is a FAILURE, never
/// a skip.
const SCANNED_STRUCTS: &[&str] = &[
    "Config",
    "StorageConfig",
    "CompactionConfig",
    "MemoryConfig",
    "CacheConfig",
    "CompressionConfig",
    "QueryConfig",
];

fn crate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The `pub <field>:` identifiers declared directly on `struct <name>` in
/// `src/config.rs`.
///
/// Deliberately a BOUNDED, line-oriented read of ONE rustfmt-formatted file, and
/// it refuses rather than guesses: an absent struct header or a body that never
/// closes at column zero panics.
fn declared_fields(source: &str, name: &str) -> Vec<String> {
    let header = format!("pub struct {name} {{");
    let start = source
        .lines()
        .position(|l| l.trim_end() == header)
        .unwrap_or_else(|| {
            panic!(
                "struct {name} not found in src/config.rs (looked for the exact line {header:?}); \
                 refusing to guess"
            )
        });

    let mut fields = Vec::new();
    let mut closed = false;
    for line in source.lines().skip(start + 1) {
        if line == "}" {
            closed = true;
            break;
        }
        let trimmed = line.trim_start();
        if let Some(rest) = trimmed.strip_prefix("pub ") {
            if let Some(ident) = rest.split(':').next() {
                let ident = ident.trim();
                // A `pub fn`/`pub const` inside a struct body is impossible, so
                // anything else here is a shape we do not model.
                assert!(
                    !ident.is_empty() && ident.chars().all(|c| c.is_alphanumeric() || c == '_'),
                    "unmodelled field shape in struct {name}: {line:?}; refusing to guess"
                );
                fields.push(ident.to_string());
            }
        }
    }
    assert!(
        closed,
        "struct {name} body in src/config.rs does not close with a column-zero `}}`; \
         refusing to guess"
    );
    assert!(
        !fields.is_empty(),
        "struct {name} parsed as having no public fields; refusing to guess"
    );
    fields
}

/// THE anti-rot check: registry and source must agree in BOTH directions.
///
/// A newly added `pub` config field with no registry entry FAILS here — which is
/// the whole point of the file, because "nobody asked whether this knob is read"
/// is how epic #1685's backlog was created in the first place. A registry entry
/// naming a field that no longer exists also FAILS, so deletions cannot leave
/// stale ledger lines behind.
#[test]
fn the_registry_names_every_public_config_field() {
    let source = read(&crate_dir().join("src").join("config.rs"));

    for &struct_name in SCANNED_STRUCTS {
        let declared = declared_fields(&source, struct_name);
        let registered: Vec<&str> = KNOBS
            .iter()
            .filter(|k| k.declared_on == struct_name)
            .map(|k| k.field)
            .collect();

        for field in &declared {
            assert!(
                registered.contains(&field.as_str()),
                "{struct_name}.{field} is a public config knob with NO entry in KNOBS. \
                 Add one: a set-knob -> observable-difference test (Evidence::Here / \
                 Evidence::CoveredBy), or Evidence::Reserved saying why no difference is \
                 expressible."
            );
        }
        for field in &registered {
            assert!(
                declared.iter().any(|d| d == field),
                "KNOBS names {struct_name}.{field}, which src/config.rs no longer declares; \
                 remove the stale registry entry"
            );
        }
    }

    // Every registry entry must belong to a struct we actually scan, or it is
    // unchecked prose.
    for knob in KNOBS {
        assert!(
            SCANNED_STRUCTS.contains(&knob.declared_on),
            "{} is registered against unscanned struct {}; add it to SCANNED_STRUCTS",
            knob.path,
            knob.declared_on
        );
    }
}

/// Registry hygiene: no duplicate paths, and the dotted path's last segment must
/// be the field name (so a copy-paste cannot silently point at the wrong knob).
#[test]
fn registry_paths_are_unique_and_end_in_their_field() {
    let mut seen: Vec<&str> = Vec::new();
    for knob in KNOBS {
        assert!(
            !seen.contains(&knob.path),
            "duplicate registry entry for {}",
            knob.path
        );
        seen.push(knob.path);
        let last = knob.path.rsplit('.').next().unwrap_or(knob.path);
        assert_eq!(
            last, knob.field,
            "registry path {} must end in its field name {}",
            knob.path, knob.field
        );
    }
}

/// `Evidence::Here` must name a test that exists in THIS file. Without this the
/// strongest disposition is unverified prose.
#[test]
fn here_evidence_names_a_test_in_this_file() {
    let this_file = read(
        &crate_dir()
            .join("tests")
            .join("config_knob_behavior_guard.rs"),
    );
    for knob in KNOBS {
        if let Evidence::Here(test_fn) = knob.evidence {
            assert!(
                this_file.contains(&format!("fn {test_fn}(")),
                "{} claims Evidence::Here({test_fn}), but this file declares no such fn",
                knob.path
            );
        }
    }
}

/// `Evidence::CoveredBy` must name a file that exists and a `fn` it declares.
/// Renaming or deleting a covering test therefore REDS this guard instead of
/// silently leaving a knob unproven.
#[test]
fn covered_by_evidence_names_a_real_test() {
    for knob in KNOBS {
        if let Evidence::CoveredBy { file, test_fn } = knob.evidence {
            let path = crate_dir().join(file);
            assert!(
                path.is_file(),
                "{} claims coverage in {file}, which does not exist",
                knob.path
            );
            let body = read(&path);
            assert!(
                body.contains(&format!("fn {test_fn}(")),
                "{} claims coverage by {test_fn} in {file}, which declares no such fn",
                knob.path
            );
        }
    }
}

/// Every issue number this file's evidence strings may cite.
///
/// The point of the list is that adding a citation is a VISIBLE act. Three of the
/// original `Reserved` reasons cited an issue that did not exist — each said an
/// audit had been "filed off #1696" when nothing had been filed — which is
/// precisely the rot this file exists to prevent: an escape hatch whose
/// justification points at a tracking issue nobody can open is indistinguishable
/// from no justification at all.
///
/// Each entry below was checked against the tracker on 2026-08-29 to exist AND to
/// be the right owner for the knob citing it:
///
/// | issue | state | owns |
/// |-------|-------|------|
/// | #1143 | closed | mmap/prefetch p99 (`prefetch`, `direct_io_prefetch_bytes`) |
/// | #1406 | closed | uncompressed-write claim boundary |
/// | #1568 | closed | dead-cache collapse (`memory.*`) |
/// | #1582 | closed | byte-bounded result budget |
/// | #1593 | closed | blocking I/O off async workers (mmap backends) |
/// | #1619 | closed | STCS compaction wiring |
/// | #1685 | OPEN | epic AH — config/feature/dependency honesty |
/// | #1695 | closed | `query.max_execution_time` enforcement |
/// | #1696 | OPEN | this lane (AH3) |
/// | #1697 | closed | config source of truth (memtable + STCS knobs) |
/// | #1918 | closed | forced read path |
/// | #2632 | OPEN | wire murmur3 h2 into the `Filter.db` bloom plumbing |
///
/// A closed issue is a legitimate citation: it names the lane that WIRED the knob,
/// which is what the evidence is pointing at.
const VERIFIED_ISSUE_REFS: &[u32] = &[
    1143, 1406, 1568, 1582, 1593, 1619, 1685, 1695, 1696, 1697, 1918, 2632,
];

/// Every issue number cited anywhere in [`KNOBS`] must be in
/// [`VERIFIED_ISSUE_REFS`], and no evidence string may promise a FUTURE issue.
///
/// Both halves target one observed defect: an evidence string that names a
/// tracking issue which does not exist. The allowlist makes a new citation a
/// visible edit a reviewer can check; the phrase ban kills the specific shape the
/// three real instances took — describing an issue in the future tense ("filed
/// off #1696", "to be filed") rather than naming one that exists.
///
/// This cannot verify that an allowlisted number is still the right OWNER — that
/// is the same non-mechanizable limit `CoveredBy` has, and it is stated here for
/// the same reason.
#[test]
fn cited_issue_numbers_are_verified_and_never_promised() {
    const FORWARD_LOOKING: &[&str] = &[
        "filed off",
        "to be filed",
        "will be filed",
        "will file",
        "follow-up filed",
        "issue pending",
    ];

    for knob in KNOBS {
        let text = match knob.evidence {
            Evidence::Reserved(reason) => reason,
            _ => continue,
        };

        let lowered = text.to_ascii_lowercase();
        for phrase in FORWARD_LOOKING {
            assert!(
                !lowered.contains(phrase),
                "{}'s Reserved reason promises a FUTURE issue ({phrase:?}). Cite an issue                  that EXISTS, or say plainly that none does: {text:?}",
                knob.path
            );
        }

        // Every `#NNN...` must be an allowlisted, verified issue.
        let bytes = text.as_bytes();
        for (i, _) in text.char_indices().filter(|(_, c)| *c == '#') {
            let digits: String = bytes[i + 1..]
                .iter()
                .take_while(|b| b.is_ascii_digit())
                .map(|b| *b as char)
                .collect();
            if digits.is_empty() {
                continue; // a bare '#', e.g. inside `#[cfg(...)]`
            }
            let number: u32 = digits
                .parse()
                .unwrap_or_else(|e| panic!("{}: unparseable issue ref #{digits}: {e}", knob.path));
            assert!(
                VERIFIED_ISSUE_REFS.contains(&number),
                "{} cites #{number}, which is not in VERIFIED_ISSUE_REFS. Confirm the issue                  EXISTS and owns this knob, then add it to that list (and its table row) so                  the citation is a checked claim rather than prose.",
                knob.path
            );
        }
    }
}

/// `Evidence::Reserved` is the escape hatch, so it must at least be SUBSTANTIVE:
/// a real sentence naming a reason, not a placeholder.
#[test]
fn reserved_evidence_carries_a_substantive_reason() {
    for knob in KNOBS {
        if let Evidence::Reserved(reason) = knob.evidence {
            let trimmed = reason.trim();
            assert!(
                trimmed.len() >= 40,
                "{}'s Reserved reason is too thin to be a justification: {trimmed:?}",
                knob.path
            );
            let lowered = trimmed.to_ascii_lowercase();
            for placeholder in ["todo", "tbd", "fixme", "<why>", "unspecified"] {
                assert!(
                    !lowered.contains(placeholder),
                    "{}'s Reserved reason is a placeholder ({placeholder}): {trimmed:?}",
                    knob.path
                );
            }
        }
    }
}

// ------------------------------------------------- decorative knobs stay gone

/// #1696 deleted these knobs from `cqlite_core::Config` because nothing read
/// them. A Rust field cannot be "checked absent" by the type system from
/// outside, so this asserts it against the source of truth: `src/config.rs` must
/// not declare them again. (An embedder that still sets one gets a COMPILE
/// error, which is the loudest signal available and the documented posture.)
///
/// # "Nothing read them" is about the KNOBS, not about the bloom path
///
/// Worth stating for the two bloom knobs, because both loose versions of the
/// claim are false. `storage/sstable/bloom.rs` is a real, Cassandra-parity
/// `BloomFilter` — its double-hashing operand order and `Filter.db` binary
/// layout are verified against `BloomFilterSerializer.java` /
/// `OffHeapBitSet.java` in `storage/sstable/s4_verification_test.rs` — AND it is
/// WIRED: the production point-read paths consult a loaded `Filter.db`
/// (`reader/component_loading.rs` loads it; `reader/partition_lookup.rs` and
/// `reader/partition_successor.rs` prune an SSTable on `might_contain == false`).
/// So do NOT say the bloom path is unwired.
///
/// What had zero production readers is these two CONFIG FIELDS: bloom behaviour
/// follows from the SSTable's own metadata, never from a knob, so neither could
/// switch anything on or off or size any filter. They are deleted rather than
/// wired speculatively, since a knob should arrive WITH its consumer; #2632 is
/// the open issue that will introduce a bloom knob WITH one.
#[test]
fn deleted_decorative_knobs_are_not_reintroduced() {
    let source = read(&crate_dir().join("src").join("config.rs"));
    for gone in [
        "pub max_sstable_size:",
        "pub block_size:",
        "pub enable_bloom_filters:",
        "pub bloom_filter_fp_rate:",
        "pub io_threads:",
        "pub sync_mode:",
        "pub plan_cache_size:",
        "pub enable_optimization:",
        "pub parallel:",
        "pub performance:",
        "pub struct ParallelQueryConfig",
        "pub struct PerformanceConfig",
        "pub struct BackgroundTaskConfig",
        "pub enum SyncMode",
    ] {
        assert!(
            !source.contains(gone),
            "{gone:?} was deleted by #1696 as decoration (zero production readers). \
             If it is being reintroduced, it needs a set-knob -> \
             observable-difference test and a KNOBS entry first."
        );
    }
}

/// The surviving surface still has to be usable: the presets and the default all
/// validate. Cheap, and it catches a purge that left a preset setting a field
/// into an illegal state.
#[test]
fn every_shipped_preset_validates() {
    for (name, config) in [
        ("default", Config::default()),
        ("memory_optimized", Config::memory_optimized()),
        ("performance_optimized", Config::performance_optimized()),
    ] {
        config
            .validate()
            .unwrap_or_else(|e| panic!("preset {name} must validate: {e}"));
    }
}

/// Keeps the imports honest: the enums the surviving disk-access knobs use are
/// still part of the public surface, so a future purge cannot delete the type
/// while leaving the field.
#[test]
fn surviving_public_types_are_reachable() {
    let storage = StorageConfig::default();
    assert_eq!(storage.disk_access_mode, DiskAccessMode::Auto);
    assert_eq!(storage.prefetch, PrefetchMode::Auto);
    // Referenced so the purge cannot delete the type out from under the field.
    let _ = matches!(
        Config::default().storage.compression.algorithm,
        CompressionAlgorithm::Lz4
    );
    let _ = QueryConfig::default().max_result_rows;
}
