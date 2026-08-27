//! Catalog invariant tests, split out of `catalog.rs` to keep that file inside
//! the campsite-rule source target (#1116). Pure test code — the catalog's own
//! constants, `ALL_METRICS` registration, attribute-key namespacing and the
//! catalog↔`otel.rs` instrument-coverage cross-check all live here so the
//! declarations stay readable.

use super::*;

/// The otel source the instrument guards scan, as ONE string.
///
/// `otel.rs` holds the record-routing arms and `otel_instruments.rs` the
/// construction, so a guard reading either alone is blind to half the wiring.
fn otel_sources() -> String {
    concat!(include_str!("otel.rs"), include_str!("otel_instruments.rs")).to_string()
}

/// Fail if a future split adds an `otel*.rs` that [`otel_sources`] does not scan.
///
/// The file list is unavoidably hand-maintained (`include_str!` needs a literal), so
/// the completeness check has to come from the filesystem. Without this, splitting
/// `otel_instruments.rs` again would silently shrink every instrument guard's
/// coverage — the same failure mode the split itself just fixed.
fn assert_every_otel_source_is_scanned() {
    const SCANNED: [&str; 2] = ["otel.rs", "otel_instruments.rs"];
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/observability");
    let mut found: Vec<String> = std::fs::read_dir(&dir)
        .expect("observability source dir must be readable")
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| n.starts_with("otel") && n.ends_with(".rs"))
        .collect();
    found.sort();
    for name in &found {
        assert!(
            SCANNED.contains(&name.as_str()) || name == "otel_tests.rs",
            "{name} is an otel source that no instrument guard scans — add it to \
             `otel_sources()` (and to SCANNED here), or the guards go blind to it"
        );
    }
}

/// Parse `pub const IDENT: &str = "VALUE";` declarations out of Rust source.
///
/// **ONE implementation, called by the catalog↔otel guard AND by the test that
/// pins its behaviour.** An earlier version of that test hand-rolled a parallel
/// parser, so it asserted a program the guard did not run and passed whether or not
/// the guard was fixed — CLAUDE.md's "a port is a second implementation, and its
/// correctness is only knowable against the original", reproduced inside a test
/// written to close a guard hole.
///
/// Two shapes it must handle, both of which broke naive versions:
///
/// - **rustfmt-WRAPPED** declarations, where the value sits on the next line. A
///   line-scoped parser drops these — and wrapping selects for LONG names, i.e.
///   exactly the new metrics the guard exists to catch.
/// - **a `;` INSIDE the value** (`"cqlite.a;b"`). Scanning to the first `;` first
///   truncates the declaration, silently drops the constant, and makes the caller
///   fail later with a misleading message.
///
/// Hence: locate the string literal FIRST, then require the `;` after it. A
/// declaration whose first `;` precedes any quote has no string value at all
/// (`pub const N: usize = 5;`) and is skipped.
fn parse_str_consts(src: &str) -> std::collections::HashMap<&str, &str> {
    let mut out = std::collections::HashMap::new();
    for (i, _) in src.match_indices("pub const ") {
        let rest = &src[i + "pub const ".len()..];
        let Some((ident, tail)) = rest.split_once(':') else {
            continue;
        };
        let open = tail.find('"');
        let semi = tail.find(';');
        // No literal, or the declaration ends before one: not a `&str` const.
        let Some(open) = open else { continue };
        if semi.is_some_and(|s| s < open) {
            continue;
        }
        let after = &tail[open + 1..];
        let Some(close) = after.find('"') else {
            continue;
        };
        // A well-formed declaration terminates right after the literal.
        if !after[close + 1..].trim_start().starts_with(';') {
            continue;
        }
        out.insert(ident.trim(), &after[..close]);
    }
    out
}

/// The otel sources with Rust comments removed, so no guard can be satisfied by
/// PROSE (issue #1705, roborev B2).
///
/// Line comments run to end-of-line and block comments to their terminator. A `//`
/// inside a string literal would truncate that line — the failure direction is a
/// guard that stops seeing a real registration (a RED test), never one that accepts
/// a fake one. Neither otel source contains such a literal today.
fn strip_rust_comments(src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    let mut rest = src;
    loop {
        let line = rest.find("//");
        let block = rest.find("/*");
        if line.is_none() && block.is_none() {
            out.push_str(rest);
            return out;
        }
        // Whichever opener comes first wins; `usize::MAX` stands in for "absent".
        let at_line = line.unwrap_or(usize::MAX) < block.unwrap_or(usize::MAX);
        if at_line {
            let l = line.unwrap_or(usize::MAX);
            out.push_str(&rest[..l]);
            let nl = rest[l..].find('\n').map(|n| l + n).unwrap_or(rest.len());
            rest = &rest[nl..];
        } else {
            let b = block.unwrap_or(usize::MAX);
            out.push_str(&rest[..b]);
            let close = rest[b..]
                .find("*/")
                .map(|c| b + c + 2)
                .unwrap_or(rest.len());
            rest = &rest[close..];
        }
    }
}

/// [`otel_sources`], comment-free.
fn otel_sources_uncommented() -> String {
    strip_rust_comments(&otel_sources())
}

/// The OTel builder methods that BIND a metric name to a real instrument.
const INSTRUMENT_BUILDERS: [&str; 6] = [
    ".u64_counter(",
    ".f64_counter(",
    ".u64_histogram(",
    ".f64_histogram(",
    ".i64_gauge(",
    ".u64_gauge(",
];

/// The three name→instrument resolvers in `otel.rs`. Their match arms ARE the
/// dispatch table the emit path executes (`add_counter` / `record_histogram` /
/// `record_gauge` each call one), so an arm here is evidence that the name is
/// routed to a pre-built instrument rather than to the ad-hoc fallback.
const OTEL_RESOLVERS: [&str; 3] = ["fn counter_for", "fn histogram_for", "fn gauge_for"];

/// Catalog constants passed to an instrument-BUILDER call — i.e. names an
/// instrument is actually constructed for.
///
/// Narrow on purpose (issue #1705, roborev B2): the previous extractor accepted ANY
/// textual `catalog::CONST` occurrence, so a comment, a doc link or a dead
/// `let _ = catalog::X;` counted as proof that an instrument existed. Only the
/// construction call itself is authoritative for "an instrument was built".
fn otel_instrument_constructions(src: &str) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    for builder in INSTRUMENT_BUILDERS {
        for (i, _) in src.match_indices(builder) {
            // rustfmt may wrap between the `(` and the argument.
            let arg = src[i + builder.len()..].trim_start();
            let Some(rest) = arg.strip_prefix("catalog::") else {
                continue;
            };
            let ident: String = rest
                .chars()
                .take_while(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || *c == '_')
                .collect();
            if !ident.is_empty() {
                out.insert(ident);
            }
        }
    }
    out
}

/// Catalog constants that appear as a MATCH ARM inside one of the three
/// [`OTEL_RESOLVERS`] — i.e. names the emit path routes to a pre-built instrument.
///
/// Fail-closed on a missing resolver: renaming or deleting one must red this guard,
/// not silently empty it (a guard whose subject set shrinks to nothing passes
/// vacuously, which is the defect class this fix exists to remove).
fn otel_dispatch_arms(src: &str) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    for resolver in OTEL_RESOLVERS {
        let start = src.find(resolver).unwrap_or_else(|| {
            panic!(
                "`{resolver}` not found in the otel sources — the registration guards \
                 read the dispatch table out of these three resolvers, so renaming one \
                 must be reflected in OTEL_RESOLVERS rather than leaving the guard blind"
            )
        });
        let body = &src[start..];
        let end = body
            .find("\n}\n")
            .expect("a resolver must end at a column-0 closing brace");
        let body = &body[..end];
        for (i, _) in body.match_indices("catalog::") {
            let rest = &body[i + "catalog::".len()..];
            let ident: String = rest
                .chars()
                .take_while(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || *c == '_')
                .collect();
            if ident.is_empty() {
                continue;
            }
            // Only an arm PATTERN counts: `catalog::IDENT =>` (rustfmt may wrap
            // before the `=>`). A mention anywhere else in the body does not.
            if rest[ident.len()..].trim_start().starts_with("=>") {
                out.insert(ident);
            }
        }
    }
    out
}

/// Names with SOME instrument binding — constructed, or routed, or both.
///
/// Used by the FORWARD guard ("an instrument exists whose name is not catalogued"),
/// where the union is the fail-closed choice: a half-wired instrument still must be
/// catalogued.
fn otel_instrument_bindings(src: &str) -> std::collections::HashSet<String> {
    let mut out = otel_instrument_constructions(src);
    out.extend(otel_dispatch_arms(src));
    out
}

/// Names AFFIRMATIVELY registered as a live instrument: constructed **and** routed.
///
/// Used by the REVERSE guard ("a catalogued name no instrument is bound to"), where
/// the intersection is the fail-closed choice: half a wiring is not a scrapeable
/// series, so it must not excuse a catalog entry.
///
/// This is the always-compiled counterpart of the RUNTIME resolution asserted in
/// `otel_tests.rs` (which calls the very resolvers the emit path calls, but only
/// compiles under `--features observability`). Both must agree; the structural parse
/// exists so the default gate run is not blind.
fn otel_registered_instruments(src: &str) -> std::collections::HashSet<String> {
    let constructed = otel_instrument_constructions(src);
    otel_dispatch_arms(src)
        .into_iter()
        .filter(|ident| constructed.contains(ident))
        .collect()
}

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

/// The SHARED declaration parser — the one the catalog↔otel guard actually calls —
/// handles both shapes that broke earlier versions.
///
/// This asserts [`parse_str_consts`], not a copy of it: the previous version of this
/// test hand-rolled its own parser without the guard's `find(';')` step, so it
/// passed whether or not the guard was fixed.
#[test]
fn the_shared_catalog_const_parser_reads_wrapped_and_semicolon_bearing_declarations() {
    // A `;` inside the value must not truncate the declaration.
    let with_semi = parse_str_consts("pub const WITH_SEMI: &str = \"cqlite.a;b\";\n");
    assert_eq!(
        with_semi.get("WITH_SEMI"),
        Some(&"cqlite.a;b"),
        "a semicolon inside the value must not truncate the declaration"
    );

    // A rustfmt-wrapped declaration must still be found.
    let wrapped = parse_str_consts(
        "pub const A_VERY_LONG_METRIC_NAME_CONSTANT: &str =\n    \"cqlite.a.b.c\";\n",
    );
    assert_eq!(
        wrapped.get("A_VERY_LONG_METRIC_NAME_CONSTANT"),
        Some(&"cqlite.a.b.c"),
        "a wrapped declaration must not drop out of the map — wrapping selects for \
         long names, i.e. exactly the new metrics this guard exists to catch"
    );

    // A non-string constant must be skipped, not mis-parsed against a later literal.
    let mixed =
        parse_str_consts("pub const COUNT: usize = 5;\npub const NAME: &str = \"cqlite.n\";\n");
    assert_eq!(mixed.get("NAME"), Some(&"cqlite.n"));
    assert_eq!(mixed.len(), 1, "the usize const must not appear: {mixed:?}");

    // And it recovers the real catalog constants, including the wrapped ones.
    let real = parse_str_consts(include_str!("catalog.rs"));
    for name in ALL_METRICS {
        assert!(
            real.values().any(|v| v == name),
            "{name} must be recoverable from catalog.rs by the shared parser"
        );
    }
}

#[test]
fn partition_access_probe_metrics_have_dedicated_otel_arms_not_the_adhoc_fallback() {
    // Issue #2827: without a dedicated arm these fall through `add_counter`'s
    // ad-hoc `_ =>` branch, which builds a fresh instrument per emit and exports
    // the series with NO unit (`By`, `{partition}`) and no description — and, by
    // construction, `every_instrument_registered_in_otel_is_catalogued` cannot see
    // them either. Assert the arms exist at the source level, like the #2419
    // saturation-gauge guard above.
    assert_every_otel_source_is_scanned();
    let otel_src = otel_sources_uncommented();
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
    assert_every_otel_source_is_scanned();
    let otel_src = otel_sources_uncommented();
    let otel_src = otel_src.as_str();
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
    let ident_to_value = parse_str_consts(this_src);

    // Names with SOME instrument binding — constructed, routed, or both. The UNION
    // is the fail-closed choice HERE (#1705, roborev B2): a half-wired instrument
    // still must be catalogued. Only real construction/dispatch constructs count;
    // a comment or a dead reference is not an instrument.
    let mut missing = Vec::new();
    let mut refs: Vec<String> = otel_instrument_bindings(otel_src).into_iter().collect();
    refs.sort();
    for ident in refs {
        let value = ident_to_value.get(ident.as_str()).copied().unwrap_or_else(|| {
            panic!("otel.rs binds an instrument to catalog::{ident}, which is not a metric-name constant in catalog.rs")
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

/// The disclosure every [`STATS_ONLY_METRICS`] entry's operator-doc annotation
/// must carry, so the generated operator reference cannot advertise a scrapeable
/// instrument that does not exist (issue #1705).
const STATS_ONLY_DOC_DISCLOSURE: &str = "NOT emitted as a live OTel instrument";

/// `catalog::IDENT` -> the annotation block text that names it, from
/// `operator_docs_annotations.rs`.
///
/// Segmented on the `name: catalog::` field rather than by brace matching: each
/// segment runs from one entry's `name:` to the next, which is exactly the text
/// belonging to that entry.
fn annotation_blocks() -> std::collections::HashMap<String, String> {
    const NAME: &str = "name: catalog::";
    let src = include_str!("operator_docs_annotations.rs");
    let starts: Vec<usize> = src.match_indices(NAME).map(|(i, _)| i).collect();
    let mut out = std::collections::HashMap::new();
    for (n, &start) in starts.iter().enumerate() {
        let end = starts.get(n + 1).copied().unwrap_or(src.len());
        let seg = &src[start + NAME.len()..end];
        let ident: String = seg
            .chars()
            .take_while(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || *c == '_')
            .collect();
        if ident.is_empty() {
            continue;
        }
        out.insert(ident, seg.to_string());
    }
    out
}

/// `metric name value` -> `catalog::IDENT`, recovered from `catalog.rs`.
///
/// Fail-closed on a collision rather than letting the later declaration silently
/// win: this map is how the registration guards turn an `ALL_METRICS` *value* back
/// into the identifier they look for in the otel sources, so a shadowed entry would
/// make a guard check the WRONG constant — a false PASS. Two `&str` constants in
/// `catalog.rs` sharing a value (a metric name colliding with an `attr`/`unit`
/// value, or a duplicated name) is a catalog bug in its own right.
fn value_to_ident() -> std::collections::HashMap<&'static str, &'static str> {
    let ident_to_value = parse_str_consts(include_str!("catalog.rs"));
    let mut out: std::collections::HashMap<&str, &str> = std::collections::HashMap::new();
    for (ident, value) in ident_to_value {
        if let Some(prior) = out.insert(value, ident) {
            panic!(
                "catalog.rs declares two &str constants with the value {value:?} \
                 (catalog::{prior} and catalog::{ident}) — the registration guards \
                 resolve a metric name back to its identifier through this map, so a \
                 collision would silently point a guard at the wrong constant"
            );
        }
    }
    out
}

#[test]
fn every_catalogued_metric_is_otel_registered_or_declared_stats_only() {
    // Issue #1705 (AI5) — the REVERSE of
    // `every_instrument_registered_in_otel_is_catalogued`, and the half that was
    // missing: a PHANTOM catalog entry, i.e. a name in `ALL_METRICS` that no
    // instrument is ever bound to. `operator_docs` generates the operator-facing
    // metrics reference from `ALL_METRICS`, so a phantom entry advertises a series
    // an operator can never scrape — the observability-honesty failure epic #1686
    // exists to close.
    //
    // A name is accounted for in exactly one of two ways: an instrument is
    // AFFIRMATIVELY registered for it — constructed by a builder call AND routed by
    // a resolver match arm ([`otel_registered_instruments`]) — or it is DECLARED in
    // `catalog::STATS_ONLY_METRICS`. Nothing else passes.
    //
    // Strictness note (#1705, roborev B2): this used to accept any textual
    // `catalog::CONST` occurrence in the otel sources, so removing a registration
    // while leaving a comment or a dead reference behind kept the guard green. Only
    // the registration constructs are authoritative, comments are stripped first,
    // and half a wiring does not count.
    assert_every_otel_source_is_scanned();
    let refs = otel_registered_instruments(&otel_sources_uncommented());
    let value_to_ident = value_to_ident();
    let stats_only: std::collections::HashSet<&str> =
        STATS_ONLY_METRICS.iter().map(|m| m.name).collect();

    let mut phantom = Vec::new();
    for name in ALL_METRICS {
        let ident = value_to_ident.get(name).copied().unwrap_or_else(|| {
            panic!("ALL_METRICS entry {name:?} has no `pub const` declaration in catalog.rs")
        });
        if !refs.contains(ident) && !stats_only.contains(name) {
            phantom.push(format!("catalog::{ident} (\"{name}\")"));
        }
    }
    assert!(
        phantom.is_empty(),
        "ALL_METRICS names metrics with NO registered otel instrument and no \
         STATS_ONLY_METRICS declaration — either wire the instrument in \
         otel_instruments.rs/otel.rs, or declare it stats-only with its reason: \
         {phantom:?}"
    );
}

#[test]
fn stats_only_metrics_are_catalogued_and_never_otel_registered() {
    // Issue #1705: keep the exemption list from rotting in the OTHER direction.
    // Once an instrument IS wired for a name, its exemption must be deleted — a
    // stale entry would permanently excuse that name from the reverse guard.
    assert_every_otel_source_is_scanned();
    // UNION here: a stats-only metric must have NO instrument binding at all, not
    // merely an incomplete one.
    let refs = otel_instrument_bindings(&otel_sources_uncommented());
    let value_to_ident = value_to_ident();
    let mut seen = std::collections::HashSet::new();
    for name in STATS_ONLY_METRICS.iter().map(|m| m.name) {
        assert!(
            ALL_METRICS.contains(&name),
            "{name} is declared stats-only but is not in ALL_METRICS"
        );
        assert!(
            seen.insert(name),
            "duplicate STATS_ONLY_METRICS entry {name}"
        );
        let ident = value_to_ident.get(name).copied().unwrap_or_else(|| {
            panic!("STATS_ONLY_METRICS entry {name:?} has no `pub const` in catalog.rs")
        });
        assert!(
            !refs.contains(ident),
            "catalog::{ident} (\"{name}\") IS registered as an otel instrument — \
             remove it from STATS_ONLY_METRICS, or the reverse registration guard \
             carries a stale exemption"
        );
    }
}

#[test]
fn stats_only_declaration_matches_the_operator_docs() {
    // Issue #1705: ONE source of truth. `STATS_ONLY_METRICS` is the machine-
    // checkable declaration; the operator reference generated from
    // `operator_docs_annotations.rs` is what a human reads. Assert set equality
    // between the declaration and the annotations carrying the "not scrapeable"
    // disclosure, so the two cannot drift — a metric quietly demoted to
    // stats-only without updating its operator prose (or vice versa) fails here.
    let blocks = annotation_blocks();
    let value_to_ident = value_to_ident();

    let declared: std::collections::BTreeSet<String> = STATS_ONLY_METRICS
        .iter()
        .map(|m| m.name)
        .map(|name| {
            value_to_ident
                .get(name)
                .copied()
                .unwrap_or_else(|| panic!("no `pub const` for {name:?}"))
                .to_string()
        })
        .collect();
    let disclosed: std::collections::BTreeSet<String> = blocks
        .iter()
        .filter(|(_, seg)| seg.contains(STATS_ONLY_DOC_DISCLOSURE))
        .map(|(ident, _)| ident.clone())
        .collect();

    assert_eq!(
        declared, disclosed,
        "catalog::STATS_ONLY_METRICS and the operator-doc annotations disclosing \
         \"{STATS_ONLY_DOC_DISCLOSURE}\" must name the SAME metrics"
    );
    assert!(
        !declared.is_empty(),
        "the disclosure marker must still be findable — an annotation reword that \
         breaks this parse would make the comparison vacuously true"
    );
}

#[test]
fn read_partition_lookup_documents_the_attribute_keys_it_actually_emits() {
    // Issue #1705 (AI5 instance 2): the doc for this counter must name the
    // attribute keys the emission sites actually attach. It names
    // `attr::LOOKUP_ROUTE` (the storage-layer lookup route, #1034) and must NOT
    // name `attr::ACCESS_PATH`, which is the query-engine SELECT access path
    // (#1035) and is never attached to this metric. Pinned so the two
    // similarly-named bounded keys cannot be swapped back in the prose.
    let src = include_str!("catalog.rs");
    let start = src
        .find("/// `cqlite.read.partition_lookup.total`")
        .expect("the READ_PARTITION_LOOKUP doc block must exist");
    let end = src[start..]
        .find("pub const READ_PARTITION_LOOKUP")
        .expect("the doc block must precede its constant");
    let doc = &src[start..start + end];
    assert!(
        doc.contains("[`attr::LOOKUP_ROUTE`]"),
        "the READ_PARTITION_LOOKUP doc must name the emitted attr::LOOKUP_ROUTE key"
    );
    assert!(
        !doc.contains("ACCESS_PATH"),
        "the READ_PARTITION_LOOKUP doc must NOT name attr::ACCESS_PATH — that is \
         the query-engine SELECT access path (#1035), never attached here"
    );
    // And the emission sites must agree: the storage-layer lookup counter is
    // labelled with LOOKUP_ROUTE, not ACCESS_PATH.
    let lookup_src = concat!(
        include_str!("../storage/sstable/reader/partition_lookup.rs"),
        include_str!("../storage/sstable/reader/bti_lookup_memo.rs"),
    );
    assert!(lookup_src.contains("attr::LOOKUP_ROUTE"));
    assert!(
        !lookup_src.contains("attr::ACCESS_PATH"),
        "the partition-lookup emission sites must not attach attr::ACCESS_PATH"
    );
}

#[test]
fn compression_ratio_is_documented_write_side_only_and_emitted_only_there() {
    // Issue #1705 (AI5 instance 4): the doc used to describe a bare "per-chunk
    // compression ratio", which reads as a read-path signal an operator could use
    // to reason about the SSTables being READ. There is no such emission. Pin the
    // honesty claim to the code: the only emission site is the compressed-data
    // WRITER, and no reader/decompression path records this histogram.
    let src = include_str!("catalog.rs");
    let start = src
        .find("/// `cqlite.compression.ratio`")
        .expect("the COMPRESSION_RATIO doc block must exist");
    let end = src[start..]
        .find("pub const COMPRESSION_RATIO")
        .expect("the doc block must precede its constant");
    let doc = &src[start..start + end];
    assert!(
        doc.contains("WRITE-SIDE ONLY"),
        "the COMPRESSION_RATIO doc must state that it is write-side only"
    );

    // The claim, measured: every emission site of this metric across the crate
    // (outside the catalog + operator-doc declaration sites, which name it without
    // emitting) must be a writer.
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut emitters = Vec::new();
    let mut stack = vec![dir.clone()];
    while let Some(d) = stack.pop() {
        let entries = std::fs::read_dir(&d).expect("crate src must be readable");
        for e in entries.filter_map(|e| e.ok()) {
            let path = e.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().and_then(|x| x.to_str()) != Some("rs") {
                continue;
            }
            let rel = path
                .strip_prefix(&dir)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            if rel.starts_with("observability/") {
                continue; // declaration + annotation sites, not emitters
            }
            let text = std::fs::read_to_string(&path).expect("source must be readable");
            if text.contains("COMPRESSION_RATIO") {
                emitters.push(rel);
            }
        }
    }
    emitters.sort();
    assert_eq!(
        emitters,
        vec!["storage/sstable/writer/compressed_data_writer.rs".to_string()],
        "COMPRESSION_RATIO must be emitted ONLY from the compressed-data writer — a \
         new site (especially a read/decompression path) invalidates the \
         write-side-only wording in its catalog doc and operator annotation"
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
    // feature. Delete an arm → this fails. Scans BOTH otel sources (#1116 split):
    // reading `otel.rs` alone would miss an arm that moved with the construction.
    assert_every_otel_source_is_scanned();
    let otel_src = otel_sources_uncommented();
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

/// A synthetic otel source carrying all three resolvers, so the registration
/// parsers can be exercised on text this test controls. The parsers under test are
/// the ones the guards call — never a copy of them.
fn synthetic_otel_source(dispatch: &str, construction: &str) -> String {
    format!(
        "fn counter_for(i: &Instruments, name: &str) -> Option<&Counter<u64>> {{\n\
         \x20   Some(match name {{\n{dispatch}        _ => return None,\n    }})\n}}\n\
         fn histogram_for() {{\n    match name {{\n        _ => return None,\n    }}\n}}\n\
         fn gauge_for() {{\n    match name {{\n        _ => return None,\n    }}\n}}\n\
         fn build() {{\n{construction}}}\n"
    )
}

/// Parse a synthetic source the way the guards do: comments stripped first.
fn synthetic_registered(dispatch: &str, construction: &str) -> (bool, bool) {
    let src = strip_rust_comments(&synthetic_otel_source(dispatch, construction));
    (
        otel_instrument_bindings(&src).contains("GHOST"),
        otel_registered_instruments(&src).contains("GHOST"),
    )
}

#[test]
fn a_comment_or_a_dead_reference_cannot_pass_as_a_registered_instrument() {
    // Issue #1705 (roborev B2) — the defect this replaces: the old extractor took
    // ANY textual `catalog::CONST` occurrence as proof that an instrument existed,
    // so deleting a registration while leaving a mention behind kept the reverse
    // guard green. Prove that neither shape registers anything now.
    let commented_out = synthetic_registered(
        "        // catalog::GHOST => &i.ghost,\n",
        "    // .u64_counter(catalog::GHOST)\n",
    );
    assert_eq!(
        commented_out,
        (false, false),
        "a commented-out registration must register nothing"
    );

    let doc_link = synthetic_registered(
        "        /// see [`catalog::GHOST`]\n",
        "    /* catalog::GHOST */\n",
    );
    assert_eq!(
        doc_link,
        (false, false),
        "a doc link / block comment must register nothing"
    );

    let dead_code = synthetic_registered("", "    let _ = catalog::GHOST;\n");
    assert_eq!(
        dead_code,
        (false, false),
        "a dead reference that builds no instrument must register nothing"
    );
}

#[test]
fn registration_requires_both_construction_and_dispatch() {
    // The reverse guard's intersection rule: half a wiring is not a scrapeable
    // series, so it must not excuse a catalog entry — while the forward guard's
    // union still sees it (it is an instrument that must be catalogued).
    assert_eq!(
        synthetic_registered("", "    .u64_counter(catalog::GHOST)\n"),
        (true, false),
        "constructed but never routed: a binding, not a registered instrument"
    );
    assert_eq!(
        synthetic_registered("        catalog::GHOST => &i.ghost,\n", ""),
        (true, false),
        "routed but never constructed: a binding, not a registered instrument"
    );
    assert_eq!(
        synthetic_registered(
            "        catalog::GHOST => &i.ghost,\n",
            "    .u64_counter(catalog::GHOST)\n"
        ),
        (true, true),
        "constructed AND routed is what registration means"
    );
    // rustfmt wraps long calls/arms; both wrapped shapes must still be seen.
    assert_eq!(
        synthetic_registered(
            "        catalog::GHOST\n            => &i.ghost,\n",
            "    .u64_counter(\n        catalog::GHOST,\n    )\n"
        ),
        (true, true),
        "a wrapped construction/arm must not drop out — wrapping selects for LONG \
         names, i.e. exactly the new metrics these guards exist to catch"
    );
}

#[test]
fn a_missing_resolver_reds_the_dispatch_parser_instead_of_emptying_it() {
    // A guard whose subject set silently shrinks to nothing passes vacuously, so
    // renaming/removing a resolver must PANIC rather than yield an empty arm set.
    let no_gauge_resolver =
        "fn counter_for() {\n    match name {\n    }\n}\nfn histogram_for() {\n    match name {\n    }\n}\n";
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = std::panic::catch_unwind(|| otel_dispatch_arms(no_gauge_resolver));
    std::panic::set_hook(previous);
    assert!(
        outcome.is_err(),
        "a missing resolver must fail the parse, not quietly return an empty set"
    );
}

#[test]
fn the_real_otel_sources_register_wired_metrics_and_not_the_stats_only_ones() {
    // The affirmative half on the REAL source: the parsers must actually say YES
    // for wired metrics (one per instrument kind) and NO for the declared
    // stats-only ones — otherwise the two guards above could both be vacuous.
    assert_every_otel_source_is_scanned();
    let registered = otel_registered_instruments(&otel_sources_uncommented());
    for wired in ["READ_ROWS", "READ_DURATION", "SSTABLES_OPEN"] {
        assert!(
            registered.contains(wired),
            "catalog::{wired} is wired in the otel sources but the parser did not \
             see it registered"
        );
    }
    for stats_only in ["KEY_CACHE_HITS", "KEY_CACHE_CAPACITY_BYTES"] {
        assert!(
            !registered.contains(stats_only),
            "catalog::{stats_only} is declared stats-only, so nothing may register it"
        );
    }
    assert!(
        registered.len() > 60,
        "the registered set collapsed to {} entries — the parse is broken, and a \
         shrunken subject set is a vacuous guard",
        registered.len()
    );
}

#[test]
fn stats_only_probes_read_distinct_live_stats_fields() {
    // Issue #1705: the stats-only list must not be an unguarded waiver list. A bare
    // name list could silence the registration guard for a metric whose instrument
    // was simply forgotten — appending the name would be the whole cost. So each
    // entry carries a probe that READS its value out of a real
    // `Database::stats().memory_stats` snapshot, and this asserts the probes are
    // real: given a snapshot with a UNIQUE sentinel per key-cache field, every
    // probe must return its OWN sentinel.
    //
    // What that rules out: a probe that ignores the snapshot (constant / `0`), a
    // probe copied from a sibling entry (two entries reading one field), and — the
    // point of the exercise — a metric with no stats field at all, which cannot be
    // given a compiling probe in the first place.
    let stats = crate::memory::MemoryStats {
        key_cache_hits: 101,
        key_cache_misses: 102,
        key_cache_evictions: 103,
        key_cache_invalidations: 104,
        key_cache_resident_bytes: 105,
        key_cache_capacity_bytes: 106,
        // Neighbouring fields a sloppy probe might read instead must stay
        // distinguishable from the six above.
        block_cache_hits: 201,
        block_cache_misses: 202,
        block_cache_evictions: 203,
        block_cache_capacity_bytes: 204,
        ..Default::default()
    };

    assert!(
        !STATS_ONLY_METRICS.is_empty(),
        "the probe guard must have a subject — an empty declaration passes vacuously"
    );
    let mut seen: std::collections::HashMap<u64, &str> = std::collections::HashMap::new();
    for m in STATS_ONLY_METRICS {
        let observed = (m.stats_probe)(&stats);
        assert_ne!(
            observed,
            0,
            "catalog::…{name} declares stats_field {field} but its probe read 0 from a \
             fully-populated snapshot — the probe reads no live field, so the \
             stats-only exemption is unjustified",
            name = m.name,
            field = m.stats_field
        );
        if let Some(prior) = seen.insert(observed, m.name) {
            panic!(
                "{} and {} probe the SAME stats field (both read {observed}) — one of \
                 them is not actually surfaced on the stats path it claims \
                 ({})",
                prior, m.name, m.stats_field
            );
        }
    }

    // And the probes must run against the REAL public snapshot type, not only a
    // hand-built one: a default snapshot is readable through every probe.
    let live = crate::memory::MemoryStats::default();
    for m in STATS_ONLY_METRICS {
        // Reading must not panic; the value itself is legitimately 0 before use.
        let _ = (m.stats_probe)(&live);
    }
}
