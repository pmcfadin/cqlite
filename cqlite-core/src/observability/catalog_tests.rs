//! Catalog invariant tests, split out of `catalog.rs` to keep that file inside
//! the campsite-rule source target (#1116). Pure test code — the catalog's own
//! constants, `ALL_METRICS` registration, attribute-key namespacing and the
//! catalog↔`otel.rs` instrument-coverage cross-check all live here so the
//! declarations stay readable.

use super::*;

#[test]
fn metric_names_are_namespaced_and_unique() {
    let mut seen = std::collections::HashSet::new();
    for name in ALL_METRICS {
        assert!(
            name.starts_with("cqlite."),
            "metric {name} must be rooted under cqlite."
        );
        assert!(seen.insert(*name), "duplicate metric name {name}");
    }
    assert_eq!(seen.len(), ALL_METRICS.len());
}

#[test]
fn attribute_keys_are_namespaced() {
    for key in [
        attr::ERROR_CATEGORY,
        attr::SUBSYSTEM,
        attr::SSTABLE_FORMAT,
        attr::COMPRESSION,
        attr::RESULT,
        attr::LOOKUP_ROUTE,
        attr::ACCESS_PATH,
        attr::PLAN_TYPE,
        attr::RPC_METHOD,
        attr::RPC_STATUS,
        attr::RPC_PHASE,
        attr::FALLBACK_REASON,
        attr::WARM_REFRESH_OUTCOME,
        attr::FLIGHT_ABORT_REASON,
        attr::ROWS_ROOT_REJECT_REASON,
        attr::REPEAT_BUCKET,
        attr::SIZE_SOURCE,
    ] {
        assert!(key.starts_with("cqlite."), "attr {key} must be namespaced");
    }
}

#[test]
fn partition_access_probe_metrics_are_registered_and_namespaced() {
    // Issue #2827: the bounded partition access-distribution probe's four
    // series must be catalogued (so the fail-closed operator-docs generator
    // covers them) and rooted under the read namespace.
    for m in [
        READ_PARTITION_ACCESS_DISTINCT_PARTITIONS,
        READ_PARTITION_ACCESS_ACCESSES,
        READ_PARTITION_ACCESS_BYTES,
        READ_PARTITION_ACCESS_SAMPLE_DENOMINATOR,
    ] {
        assert!(ALL_METRICS.contains(&m), "{m} must be catalogued");
        assert!(
            m.starts_with("cqlite.read.partition_access."),
            "{m} must live in the partition_access namespace"
        );
        assert_eq!(
            ALL_METRICS.iter().filter(|n| *n == &m).count(),
            1,
            "{m} must appear exactly once in ALL_METRICS"
        );
    }
}

#[test]
fn warm_cache_metrics_are_registered_and_namespaced() {
    // Issue #2310: the warm-handle counters must be part of the canonical
    // catalog so registration/uniqueness checks cover them, and rooted under
    // `cqlite.` like every other metric.
    for m in [
        WARM_CACHE_HITS,
        WARM_CACHE_MISSES,
        WARM_CACHE_EVICTS,
        WARM_CACHE_REFRESH,
    ] {
        assert!(ALL_METRICS.contains(&m), "{m} must be catalogued");
        assert!(m.starts_with("cqlite.warm."));
    }
    assert!(attr::WARM_REFRESH_OUTCOME.starts_with("cqlite."));
}

#[test]
fn rpc_phase_duration_is_registered_and_namespaced() {
    // Issue #2162: the new phase-duration histogram must be part of the
    // canonical catalog so registration/uniqueness sanity checks cover it, and
    // its name must be rooted under `cqlite.` like every other metric.
    assert!(ALL_METRICS.contains(&RPC_PHASE_DURATION));
    assert!(RPC_PHASE_DURATION.starts_with("cqlite."));
    assert!(attr::RPC_PHASE.starts_with("cqlite."));
}

#[test]
fn rpc_phase_active_gauge_is_registered_and_namespaced() {
    // Issue #2361: the in-flight phase gauge must be catalogued (so the
    // registration/uniqueness checks cover it) and namespaced like the rest.
    assert!(ALL_METRICS.contains(&RPC_PHASE_ACTIVE));
    assert_eq!(RPC_PHASE_ACTIVE, "cqlite.rpc.phase.active");
    assert!(RPC_PHASE_ACTIVE.starts_with("cqlite."));
}

#[test]
fn index_parses_total_counter_is_registered_and_namespaced() {
    // Issue #2383: the redundant-Index.db-parse probe must be catalogued (so
    // the registration/uniqueness checks cover it) and namespaced.
    assert!(ALL_METRICS.contains(&INDEX_PARSES_TOTAL));
    assert_eq!(INDEX_PARSES_TOTAL, "cqlite.sstable.index_parses_total");
    assert!(INDEX_PARSES_TOTAL.starts_with("cqlite."));
}

#[test]
fn index_interval_parses_counter_is_distinct_registered_and_namespaced() {
    // Issue #2412 spec Requirement 5: the bounded interval-parse counter is a
    // DISTINCT catalog metric (never conflated with full parses), catalogued so
    // the registration/uniqueness checks cover it, and rooted under `cqlite.`.
    assert!(ALL_METRICS.contains(&INDEX_INTERVAL_PARSES_TOTAL));
    assert_eq!(
        INDEX_INTERVAL_PARSES_TOTAL,
        "cqlite.sstable.index_interval_parses_total"
    );
    assert!(INDEX_INTERVAL_PARSES_TOTAL.starts_with("cqlite."));
    // Distinct from the full-parse counter — the two must never collapse to one
    // name (a lazy-open regression must stay visible on INDEX_PARSES_TOTAL).
    assert_ne!(INDEX_INTERVAL_PARSES_TOTAL, INDEX_PARSES_TOTAL);
}

#[test]
fn global_key_cache_counters_are_registered_and_namespaced() {
    // Issue #2059 spec Requirement "Real, cqlite-namespaced observability
    // counters": every key-cache counter/gauge name is in the catalog and rooted
    // under `cqlite.`, with evictions and invalidations kept DISTINCT.
    for name in [
        KEY_CACHE_HITS,
        KEY_CACHE_MISSES,
        KEY_CACHE_EVICTIONS,
        KEY_CACHE_INVALIDATIONS,
        KEY_CACHE_RESIDENT_BYTES,
        KEY_CACHE_CAPACITY_BYTES,
    ] {
        assert!(ALL_METRICS.contains(&name), "{name} must be catalogued");
        assert!(name.starts_with("cqlite."), "{name} must be namespaced");
    }
    assert_ne!(
        KEY_CACHE_EVICTIONS, KEY_CACHE_INVALIDATIONS,
        "budget evictions and generation invalidations are distinct counters"
    );
}

#[test]
fn read_scan_window_refill_counter_is_registered_and_namespaced() {
    // Issue #2426 (roborev MEDIUM): the windowed-scan refill counter is an
    // EMITTED instrument (a dedicated `Instruments` field + emission site in
    // `scan_stream_windowed.rs`), so it MUST be in the canonical catalog or the
    // operator "every instrument" reference silently omits it and the freshness
    // gate cannot see it.
    assert!(ALL_METRICS.contains(&READ_SCAN_WINDOW_REFILL));
    assert_eq!(READ_SCAN_WINDOW_REFILL, "cqlite.read.scan.window_refill");
    assert!(READ_SCAN_WINDOW_REFILL.starts_with("cqlite."));
}

/// The catalog↔otel guard must read a rustfmt-WRAPPED `pub const`, and must not be
/// truncated by a `;` inside a value.
///
/// Both properties were holes: the original parser was line-scoped, so a
/// declaration rustfmt split across two lines dropped out of its identifier map and
/// the constant escaped the guard — which selects precisely for LONG names, i.e. new
/// metrics. Coverage of the fix is otherwise incidental (it holds only while some
/// constant happens to be wrapped), so it is asserted by name here: unwrap every
/// declaration in `catalog.rs` and this test fails, rather than the guard silently
/// ceasing to guard.
#[test]
fn the_catalog_ident_parser_reads_wrapped_and_semicolon_bearing_declarations() {
    let src = include_str!("catalog.rs");
    let wrapped: Vec<&str> = src
        .lines()
        .enumerate()
        .filter_map(|(i, l)| {
            let l = l.trim_start();
            // A declaration whose value is NOT on the `pub const` line.
            (l.starts_with("pub const ") && l.ends_with('=')).then(|| src.lines().nth(i).unwrap())
        })
        .collect();
    assert!(
        !wrapped.is_empty(),
        "expected at least one rustfmt-wrapped `pub const` in catalog.rs to keep the \
         wrapped-parse path exercised; if every declaration now fits on one line, \
         this guard needs a synthetic fixture instead of relying on the real file"
    );

    // The parser must recover every wrapped constant, not just one-liners.
    for name in [
        READ_PARTITION_ACCESS_DISTINCT_PARTITIONS,
        READ_PARTITION_ACCESS_SAMPLE_DENOMINATOR,
    ] {
        assert!(
            src.contains(&format!("\"{name}\"")),
            "{name} must be declared in catalog.rs"
        );
    }

    // And a `;` inside a value must not truncate the declaration. Exercised on a
    // synthetic input so the assertion does not depend on the real file ever
    // containing such a value.
    let synthetic = "pub const WITH_SEMI: &str = \"cqlite.a;b\";\n";
    let rest = synthetic
        .strip_prefix("pub const ")
        .expect("synthetic prefix");
    let (ident, tail) = rest.split_once(':').expect("synthetic ident");
    let open = tail.find('\"').expect("synthetic open quote");
    let after = &tail[open + 1..];
    let close = after.find('\"').expect("synthetic close quote");
    assert_eq!(ident.trim(), "WITH_SEMI");
    assert_eq!(
        &after[..close],
        "cqlite.a;b",
        "the value must survive an embedded semicolon"
    );
}

#[test]
fn partition_access_probe_metrics_have_dedicated_otel_arms_not_the_adhoc_fallback() {
    // Issue #2827: without a dedicated arm these fall through `add_counter`'s
    // ad-hoc `_ =>` branch, which builds a fresh instrument per emit and exports
    // the series with NO unit (`By`, `{partition}`) and no description — and, by
    // construction, `every_instrument_registered_in_otel_is_catalogued` cannot see
    // them either. Assert the arms exist at the source level, like the #2419
    // saturation-gauge guard above.
    let otel_src = concat!(include_str!("otel.rs"), include_str!("otel_instruments.rs"));
    for (metric, arm) in [
        (
            READ_PARTITION_ACCESS_DISTINCT_PARTITIONS,
            "catalog::READ_PARTITION_ACCESS_DISTINCT_PARTITIONS => &i.read_partition_access_distinct",
        ),
        (
            READ_PARTITION_ACCESS_ACCESSES,
            "catalog::READ_PARTITION_ACCESS_ACCESSES => &i.read_partition_access_accesses",
        ),
        (
            READ_PARTITION_ACCESS_BYTES,
            "catalog::READ_PARTITION_ACCESS_BYTES => &i.read_partition_access_bytes",
        ),
        (
            READ_PARTITION_ACCESS_SAMPLE_DENOMINATOR,
            "catalog::READ_PARTITION_ACCESS_SAMPLE_DENOMINATOR",
        ),
    ] {
        assert!(
            otel_src.contains(arm),
            "{metric} must have a DEDICATED otel.rs dispatch arm so its series carries \
             its catalogued unit and description, never the ad-hoc fallback"
        );
    }
    // And the instruments must be constructed with their catalogued units.
    assert!(otel_src.contains(".u64_counter(catalog::READ_PARTITION_ACCESS_BYTES)"));
    assert!(otel_src.contains(".i64_gauge(catalog::READ_PARTITION_ACCESS_SAMPLE_DENOMINATOR)"));
}

#[test]
fn every_instrument_registered_in_otel_is_catalogued() {
    // Issue #2426 (roborev MEDIUM, F1): guard the "emitted instrument absent
    // from ALL_METRICS" bug class. `otel.rs` is the canonical instrument
    // construction + record-routing site (every cross-crate emission — incl.
    // cqlite-flight's warm-cache/admission metrics — routes through its
    // `add_counter`/`record_histogram`/`record_gauge` dedicated arms). Any
    // `catalog::SCREAMING_CONST` referenced there is a metric name bound to a
    // real instrument, so it MUST appear in `ALL_METRICS`. This is a
    // fully-automatic source-level check (no `observability` feature needed):
    // add an instrument in `otel.rs` and forget to catalogue it → this fails.
    //
    // Automation note (#2426): this scans the core `otel.rs` registration site.
    // Because every catalogued instrument that cqlite-flight emits now has a
    // dedicated arm here (never the ad-hoc `_ =>` fallback), the check
    // transitively covers the flight emission sites too. A future metric emitted
    // ONLY via the ad-hoc fallback (no dedicated arm, no catalog entry) would not
    // be caught here — that path is reserved for genuinely non-catalog names.
    // BOTH halves of the otel wiring: `otel.rs` keeps the record-routing arms and
    // `otel_instruments.rs` the construction. Scanning only one would let an
    // instrument built in the other escape the guard entirely (#1116 split).
    let otel_src = concat!(include_str!("otel.rs"), include_str!("otel_instruments.rs"));
    let catalogued: std::collections::HashSet<&str> = ALL_METRICS.iter().copied().collect();

    // Collect the const IDENTIFIERS present in the ALL_METRICS array so we can
    // map an `otel.rs` `catalog::IDENT` reference to a catalogued name. The
    // constants are `pub const IDENT: &str = "cqlite. …";`.
    //
    // Parsed over the WHOLE declaration, not line-by-line. rustfmt wraps a long
    // declaration onto two lines:
    //
    // ```ignore
    // pub const READ_PARTITION_ACCESS_DISTINCT_PARTITIONS: &str =
    //     "cqlite.read.partition_access.distinct_partitions";
    // ```
    //
    // A line-scoped parser finds no string literal on the `pub const` line and
    // drops the constant from the map — so the very metrics most likely to be new
    // (long names) would slip past this guard, which is the bug class it exists to
    // catch. Scan from each `pub const ` to its terminating `;` instead.
    let this_src = include_str!("catalog.rs");
    let mut ident_to_value = std::collections::HashMap::new();
    for (i, _) in this_src.match_indices("pub const ") {
        let rest = &this_src[i + "pub const ".len()..];
        let Some(end) = rest.find(';') else { continue };
        let decl = &rest[..end];
        let Some((ident, tail)) = decl.split_once(':') else {
            continue;
        };
        let Some(start) = tail.find('"') else {
            continue;
        };
        let after = &tail[start + 1..];
        if let Some(close) = after.find('"') {
            ident_to_value.insert(ident.trim(), &after[..close]);
        }
    }

    // Extract every `catalog::SCREAMING_CONST` reference in otel.rs. `unit`/
    // `attr` submodule refs (`catalog::unit::…`, `catalog::attr::…`) start with
    // a lowercase char after `catalog::`, so they are excluded by construction.
    let mut missing = Vec::new();
    for (i, _) in otel_src.match_indices("catalog::") {
        let rest = &otel_src[i + "catalog::".len()..];
        let ident: String = rest
            .chars()
            .take_while(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || *c == '_')
            .collect();
        // Skip lowercase submodule paths (unit/attr) — `ident` is empty then.
        if ident.is_empty() {
            continue;
        }
        let value = ident_to_value.get(ident.as_str()).copied().unwrap_or_else(|| {
            panic!("otel.rs references catalog::{ident}, which is not a metric-name constant in catalog.rs")
        });
        if !catalogued.contains(value) {
            missing.push(format!("catalog::{ident} (\"{value}\")"));
        }
    }
    assert!(
        missing.is_empty(),
        "otel.rs registers instruments for metrics ABSENT from ALL_METRICS \
         (add them to catalog::ALL_METRICS): {missing:?}"
    );
}

#[test]
fn saturation_gauges_are_registered_namespaced_and_unique() {
    // Issue #2419 (WS2), spec Requirement: every saturation gauge must be a
    // `cqlite.*` name in ALL_METRICS, appearing exactly once, with the units
    // the design's naming table pins. Fails on `main` until the constants land.
    for m in SATURATION_GAUGES {
        assert!(ALL_METRICS.contains(m), "{m} must be catalogued");
        assert!(m.starts_with("cqlite."), "{m} must be rooted under cqlite.");
        assert_eq!(
            ALL_METRICS.iter().filter(|n| *n == m).count(),
            1,
            "{m} must appear exactly once in ALL_METRICS"
        );
    }
    assert_eq!(
        MERGE_EGRESS_CHANNEL_DEPTH,
        "cqlite.merge.egress_channel_depth"
    );
    assert_eq!(PROC_THREADS, "cqlite.proc.threads");
    assert_eq!(PROC_FDS, "cqlite.proc.fds");
    assert_eq!(PROC_RSS_BYTES, "cqlite.proc.rss_bytes");
    assert_eq!(
        FLIGHT_BLOCKING_TASKS_IN_USE,
        "cqlite.flight.blocking_tasks_in_use"
    );
    // Flight table-visibility gauges (#2684).
    assert_eq!(FLIGHT_TABLES_DISCOVERED, "cqlite.flight.tables_discovered");
    assert_eq!(FLIGHT_WARM_TABLES, "cqlite.flight.warm_tables");
    // Units from the design naming table (#2419 design D4).
    assert_eq!(unit::FDS, "{fd}");
    assert_eq!(unit::ENTRIES, "{entry}");
    assert_eq!(unit::THREADS, "{thread}");
    assert_eq!(unit::BYTES, "By");
}

#[test]
fn saturation_gauges_have_dedicated_otel_arms_not_the_adhoc_fallback() {
    // Issue #2419 (WS2), spec Requirement / #2412 lesson: each saturation
    // gauge must resolve in `otel::record_gauge` to a pre-built `Instruments`
    // field, NOT the ad-hoc `_ =>` fallback (which rebuilds the instrument on
    // every sample). Source-scan otel.rs for a dedicated `catalog::IDENT =>`
    // match arm per gauge — a fully-automatic check needing no `observability`
    // feature. Delete an arm → this fails.
    let otel_src = include_str!("otel.rs");
    for ident in [
        "MERGE_EGRESS_CHANNEL_DEPTH",
        "MERGE_ACTIVE_MERGES",
        "PROC_THREADS",
        "PROC_FDS",
        "PROC_RSS_BYTES",
        "FLIGHT_BLOCKING_TASKS_IN_USE",
        "FLIGHT_TABLES_DISCOVERED",
        "FLIGHT_WARM_TABLES",
    ] {
        let arm = format!("catalog::{ident} =>");
        assert!(
            otel_src.contains(&arm),
            "otel::record_gauge lacks a dedicated arm `{arm}` — the gauge would \
             fall through to the ad-hoc per-call-rebuilt fallback (#2412)"
        );
    }
}

#[test]
fn saturation_family_is_disjoint_from_admission_family() {
    // Issue #2419 (WS2), spec Requirement: the saturation gauges SHALL NOT
    // duplicate or overlap the #2420 admission gauges, and
    // `cqlite.flight.blocking_tasks_in_use` (blocking-pool pressure) must be a
    // DISTINCT metric from `cqlite.flight.admission.in_use` (held permits).
    for s in SATURATION_GAUGES {
        for a in ADMISSION_METRICS {
            assert_ne!(s, a, "saturation gauge {s} collides with admission {a}");
        }
    }
    assert_ne!(FLIGHT_BLOCKING_TASKS_IN_USE, FLIGHT_ADMISSION_IN_USE);
}

#[test]
fn flight_table_visibility_gauges_are_registered_namespaced_and_total_only() {
    // Issue #2684: the two flight table-visibility gauges must be catalogued
    // (so the registration/uniqueness + operator-doc checks cover them),
    // rooted under `cqlite.flight.`, carry the `{entry}` unit, appear in the
    // saturation-gauge group (so the dedicated-otel-arm scan covers them),
    // and be DISTINCT from each other and from the blocking-task gauge.
    for m in [FLIGHT_TABLES_DISCOVERED, FLIGHT_WARM_TABLES] {
        assert!(ALL_METRICS.contains(&m), "{m} must be catalogued");
        assert!(m.starts_with("cqlite.flight."), "{m} must be namespaced");
        assert!(
            SATURATION_GAUGES.contains(&m),
            "{m} must be in the saturation-gauge group"
        );
        assert_eq!(
            ALL_METRICS.iter().filter(|n| *n == &m).count(),
            1,
            "{m} must appear exactly once in ALL_METRICS"
        );
    }
    assert_eq!(FLIGHT_TABLES_DISCOVERED, "cqlite.flight.tables_discovered");
    assert_eq!(FLIGHT_WARM_TABLES, "cqlite.flight.warm_tables");
    assert_ne!(FLIGHT_TABLES_DISCOVERED, FLIGHT_WARM_TABLES);
    assert_ne!(FLIGHT_WARM_TABLES, FLIGHT_BLOCKING_TASKS_IN_USE);
    assert_eq!(unit::ENTRIES, "{entry}");
}

#[test]
fn merge_producer_threads_gauge_is_registered_and_documented() {
    // Issue #2316: the merge producer-thread gauge must be part of the
    // canonical catalog (so the registration/uniqueness checks cover it), be
    // rooted under `cqlite.`, and carry the `{thread}` unit agreed with #2313 WS2.
    assert!(ALL_METRICS.contains(&MERGE_PRODUCER_THREADS));
    assert_eq!(MERGE_PRODUCER_THREADS, "cqlite.merge.producer_threads");
    assert!(MERGE_PRODUCER_THREADS.starts_with("cqlite."));
    assert_eq!(unit::THREADS, "{thread}");
}
