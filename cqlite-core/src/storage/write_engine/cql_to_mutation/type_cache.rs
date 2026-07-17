//! Per-conversion parsed-`CqlType` cache for the mutation builders (issue #1677,
//! Epic R — serializer allocation discipline, audit finding R6).
//!
//! # Why this exists
//!
//! The mutation builders in [`super::builders`] / [`super::delta_helpers`] resolve
//! each written column's type by calling [`CqlType::parse`] on the *static* schema
//! type string (`column.data_type`). The schema is fixed for the whole conversion,
//! yet a `BEGIN BATCH` of N statements re-parses every column's type string N
//! times — for a 100-row batch that is 100× the parsing work that only needs to
//! happen once, and each [`CqlType::parse`] does ≥1 `to_lowercase` `String`
//! allocation. R6 caches the parse *output* so each distinct type string is parsed
//! at most once per conversion. It is purely a caching layer: it never changes what
//! [`CqlType::parse`] returns, never accepts new syntax, and preserves the parse
//! error path (an un-parseable type still returns `Err` from the mutation build).
//!
//! # Why a thread-local scope (not a field on the schema)
//!
//! [`crate::schema::TableSchema`] / `Column` are constructed by struct literal at
//! ~200 sites across the crate, so a cached `OnceCell<CqlType>` field on them would
//! require touching every one and would grow files already over the campsite-rule
//! size threshold. Instead the cache lives in a thread-local, activated by a
//! [`TypeCacheScope`] RAII guard for the duration of one CQL→mutation conversion
//! (opened in [`super`]'s entry points). This mirrors the established thread-local
//! work-counter / scope pattern (issue #2428) and is immune to cross-thread
//! pollution: the mutation builders run inline on the caller's thread, so the cache
//! only ever sees that conversion's own parses.
//!
//! When no scope is active (a builder called directly, e.g. a single non-batched
//! statement) [`cached_parse`] falls straight through to [`CqlType::parse`], so
//! behaviour is identical — a single statement already parses each of its columns
//! at most once.

#[cfg(feature = "write-support")]
use crate::schema::CqlType;
#[cfg(feature = "write-support")]
use crate::Error;
#[cfg(feature = "write-support")]
use std::cell::RefCell;
#[cfg(feature = "write-support")]
use std::collections::HashMap;

#[cfg(feature = "write-support")]
thread_local! {
    /// `Some(map)` while a [`TypeCacheScope`] is active on this thread, `None`
    /// otherwise. Keyed by the raw `data_type` string; the value is the parsed
    /// [`CqlType`]. Only successful parses are cached — an `Err` is re-derived on
    /// every call so the error path is preserved exactly.
    static PARSED_TYPES: RefCell<Option<HashMap<String, CqlType>>> = const { RefCell::new(None) };
}

/// RAII activation of the per-conversion parsed-type cache on the current thread.
///
/// Open one at the start of a CQL→mutation conversion; every [`cached_parse`] call
/// executed on this thread while it is alive shares one cache, so a batch parses
/// each distinct column type string at most once. Dropping it clears the cache.
///
/// Re-entrant: if a scope is already active (e.g. `convert_cql_to_mutations`
/// delegating to `convert_cql_to_mutation`), the inner guard is a no-op and only
/// the outermost guard clears the cache on drop.
#[cfg(feature = "write-support")]
pub(super) struct TypeCacheScope {
    /// Whether THIS guard installed the cache (and so must clear it on drop).
    owns: bool,
}

#[cfg(feature = "write-support")]
impl TypeCacheScope {
    /// Activate the cache on the current thread (idempotent: nesting is safe).
    pub(super) fn new() -> Self {
        let owns = PARSED_TYPES.with(|c| {
            let mut guard = c.borrow_mut();
            if guard.is_none() {
                *guard = Some(HashMap::new());
                true
            } else {
                false
            }
        });
        Self { owns }
    }
}

#[cfg(feature = "write-support")]
impl Drop for TypeCacheScope {
    fn drop(&mut self) {
        if self.owns {
            PARSED_TYPES.with(|c| *c.borrow_mut() = None);
        }
    }
}

/// Parse a schema type string, reusing the active per-conversion cache if one is
/// installed. Semantically identical to [`CqlType::parse`] — same `Ok`/`Err`, same
/// [`CqlType`] — it only avoids re-parsing a type string already seen in this
/// conversion.
#[cfg(feature = "write-support")]
pub(super) fn cached_parse(data_type: &str) -> Result<CqlType, Error> {
    PARSED_TYPES.with(|c| {
        let mut guard = c.borrow_mut();
        match guard.as_mut() {
            Some(map) => {
                if let Some(cached) = map.get(data_type) {
                    return Ok(cached.clone());
                }
                // Cache misses (and only successes) so an un-parseable type keeps
                // surfacing its `Err` from the mutation build, exactly as before.
                let parsed = CqlType::parse(data_type)?;
                map.insert(data_type.to_string(), parsed.clone());
                Ok(parsed)
            }
            None => CqlType::parse(data_type),
        }
    })
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    use crate::schema::work_counters;

    #[test]
    fn cached_parse_dedups_within_a_scope() {
        work_counters::reset();
        let _scope = TypeCacheScope::new();

        // Same type string parsed repeatedly -> exactly one CqlType::parse call.
        for _ in 0..50 {
            assert_eq!(cached_parse("int").unwrap(), CqlType::Int);
        }
        // A second distinct type adds exactly one more parse.
        for _ in 0..50 {
            assert_eq!(cached_parse("text").unwrap(), CqlType::Text);
        }
        assert_eq!(
            work_counters::parse_calls(),
            2,
            "two distinct type strings must parse exactly twice inside a scope"
        );
    }

    #[test]
    fn without_a_scope_every_call_parses() {
        work_counters::reset();
        // No TypeCacheScope active: fall straight through to CqlType::parse.
        for _ in 0..3 {
            assert_eq!(cached_parse("int").unwrap(), CqlType::Int);
        }
        assert_eq!(
            work_counters::parse_calls(),
            3,
            "with no scope, each call must parse (identical to CqlType::parse)"
        );
    }

    #[test]
    fn parse_error_is_not_cached_and_propagates_each_time() {
        work_counters::reset();
        let _scope = TypeCacheScope::new();

        // A map with the wrong arity is a hard parse error (not a Custom fallback).
        assert!(cached_parse("map<int>").is_err());
        assert!(cached_parse("map<int>").is_err());
        assert_eq!(
            work_counters::parse_calls(),
            2,
            "errors must NOT be cached — the parse error path is preserved per call"
        );
    }

    #[test]
    fn scope_clears_on_drop_and_nesting_is_safe() {
        work_counters::reset();
        {
            let _outer = TypeCacheScope::new();
            assert_eq!(cached_parse("int").unwrap(), CqlType::Int);
            {
                // Nested guard is a no-op: it shares the outer cache and must not
                // clear it on drop.
                let _inner = TypeCacheScope::new();
                assert_eq!(cached_parse("int").unwrap(), CqlType::Int);
            }
            // Still cached after the inner guard dropped.
            assert_eq!(cached_parse("int").unwrap(), CqlType::Int);
            assert_eq!(
                work_counters::parse_calls(),
                1,
                "nested scope shares one cache"
            );
        }
        // Outer scope dropped: cache gone, next call parses afresh.
        assert_eq!(cached_parse("int").unwrap(), CqlType::Int);
        assert_eq!(
            work_counters::parse_calls(),
            2,
            "the cache must be cleared when the owning scope drops"
        );
    }
}
