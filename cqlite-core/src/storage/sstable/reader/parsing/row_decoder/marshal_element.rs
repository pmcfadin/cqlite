//! Marshal collection-element type extraction (issue #1340).
//!
//! A `frozen<collection>` column's authoritative on-disk SerializationHeader
//! marshal type (`RowColumnResolution.header_type`) carries the full element type
//! for the collection, e.g. `frozen<list<frozen<person>>>` is written as
//! `FrozenType(ListType(FrozenType(UserType(ks,person,field-types...))))`. The
//! query/compaction decoders route a frozen collection through a per-element
//! decoder keyed on the CQL short form (`frozen<person>`), which cannot resolve
//! the inner UDT's fields without a wired `UdtRegistry`. Threading the marshal
//! element type down lets an inner `frozen<UDT>` decode to a typed
//! `Value::Frozen(Value::Udt(..))` from the file's OWN header — the most
//! authoritative metadata, exactly what the no-heuristics mandate (#28) asks for.
//!
//! This module owns the small paren-aware extractor that peels the outer
//! `FrozenType(...)` wrapper and pulls the `ListType`/`SetType` element type or
//! the `MapType` key/value types out of the marshal string. It NEVER panics on a
//! malformed marshal string — it returns `None` so callers fall back to the CQL
//! short form (then the registry, then an opaque `Value::Blob`).

use super::*;

/// Marshal element type(s) extracted from a frozen collection's marshal type.
///
/// Borrows directly from the input `header_type` str (no owned `String`s) so the
/// per-cell frozen-decode path stays allocation-free (<128MB budget).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum MarshalCollectionElements<'a> {
    /// `ListType(X)` / `SetType(X)` element marshal type `X`.
    Sequence(&'a str),
    /// `MapType(K, V)` key and value marshal types.
    Map(&'a str, &'a str),
}

const FROZEN_LOWER: &str = "org.apache.cassandra.db.marshal.frozentype(";
const LIST_LOWER: &str = "org.apache.cassandra.db.marshal.listtype(";
const SET_LOWER: &str = "org.apache.cassandra.db.marshal.settype(";
const MAP_LOWER: &str = "org.apache.cassandra.db.marshal.maptype(";

/// If `s` (case-insensitively) starts with `prefix_lower` — which MUST end in the
/// opening `(` — and the paren opened by that prefix is closed by the LAST byte of
/// `s`, return the balanced inner content (original case). Returns `None` for any
/// prefix mismatch, unbalanced parens, or trailing bytes after the matching close.
/// Never panics.
fn balanced_inner<'a>(s: &'a str, prefix_lower: &str) -> Option<&'a str> {
    let head = s.get(..prefix_lower.len())?;
    if !head.eq_ignore_ascii_case(prefix_lower) {
        return None;
    }
    let bytes = s.as_bytes();
    let inner_start = prefix_lower.len();
    // The prefix's own `(` is already open, so start at depth 1.
    let mut depth: usize = 1;
    let mut i = inner_start;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return if i == bytes.len() - 1 {
                        Some(&s[inner_start..i])
                    } else {
                        // Trailing bytes after the matching close: not a clean
                        // single top-level type — reject rather than guess.
                        None
                    };
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Split a `MapType` inner (`K, V`) at the first depth-0 comma, honoring both
/// paren `()` and angle `<>` nesting. Returns `None` if there is no top-level
/// comma. Never panics.
fn split_top_level_comma(inner: &str) -> Option<(&str, &str)> {
    let mut depth: usize = 0;
    for (i, ch) in inner.char_indices() {
        match ch {
            '(' | '<' => depth += 1,
            ')' | '>' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                return Some((&inner[..i], &inner[i + 1..]));
            }
            _ => {}
        }
    }
    None
}

impl V5CompressedLegacyParser {
    /// Extract the marshal element type(s) for a frozen collection from the
    /// column's authoritative SerializationHeader marshal type (issue #1340).
    ///
    /// Peels at most one leading `FrozenType(...)` wrapper, then matches
    /// `ListType`/`SetType` (→ [`MarshalCollectionElements::Sequence`]) or
    /// `MapType` (→ [`MarshalCollectionElements::Map`]). Returns `None` — never
    /// panics — for any non-collection or malformed marshal string, so the caller
    /// falls back to the CQL short form.
    ///
    /// Parse this ONCE per frozen cell (not per element): the result borrows from
    /// `header_type`, so the per-element hot loop gains no allocations (<128MB budget).
    pub(super) fn extract_marshal_collection_elements(
        header_type: &str,
    ) -> Option<MarshalCollectionElements<'_>> {
        let s = header_type.trim();
        // Strip at most one outer FrozenType(...) wrapper (a frozen collection
        // column's header is `FrozenType(ListType(...))` etc.); tolerate a bare
        // `ListType(...)` too.
        let inner = balanced_inner(s, FROZEN_LOWER).map(str::trim).unwrap_or(s);

        if let Some(elem) =
            balanced_inner(inner, LIST_LOWER).or_else(|| balanced_inner(inner, SET_LOWER))
        {
            return Some(MarshalCollectionElements::Sequence(elem.trim()));
        }
        if let Some(kv) = balanced_inner(inner, MAP_LOWER) {
            let (k, v) = split_top_level_comma(kv)?;
            return Some(MarshalCollectionElements::Map(k.trim(), v.trim()));
        }
        None
    }

    /// Pick the element type to decode with, honoring the no-heuristics precedence
    /// (issue #28): prefer the authoritative marshal element type ONLY when it is a
    /// UDT-bearing type (`UserType(...)` at any nesting), otherwise keep the CQL
    /// short form (which already decodes every primitive/collection element and
    /// preserves the existing byte-parity behavior). The registry and `Value::Blob`
    /// fallbacks live downstream in `parse_value_from_raw_bytes`.
    pub(super) fn prefer_udt_marshal_element<'a>(
        marshal: Option<&'a str>,
        schema_short: &'a str,
    ) -> &'a str {
        match marshal {
            Some(m) if Self::is_udt_type(m) => m,
            _ => schema_short,
        }
    }

    /// THE ONE RULE FOR A MAP KEY'S DECODE TYPE, shared by BOTH map readers
    /// (issue #3612, roborev round 8 finding 1).
    ///
    /// The multicell reader (`complex_column`'s map branch) and the frozen reader
    /// (`cell_value_complex`'s `map<` branch) must hand the SAME string to the same
    /// decoder, or they produce `Value` keys that compare and hash differently for
    /// one CQL value. They previously did not, and the difference was invisible for
    /// UDT keys and visible for COLLECTION keys:
    ///
    /// * multicell `map<frozen<set<frozen<U>>>, int>` → Cassandra records
    ///   `MapType(FrozenType(SetType(UserType(..))),..)`, so the key marshal KEEPS
    ///   its `FrozenType` — a multicell map key must be explicitly frozen;
    /// * frozen `frozen<map<frozen<set<frozen<U>>>, int>>` → Cassandra records
    ///   `FrozenType(MapType(SetType(UserType(..)),..))`, with the INNER marker
    ///   OMITTED, because everything inside a frozen collection is already frozen.
    ///
    /// Decoding those two strings yields `Frozen(Set(Udt))` and `Set(Udt)`. Hence
    /// the strip below: it normalizes the marshal form to the shape the frozen
    /// reader already receives, so both readers decode an identical string and
    /// parity holds BY CONSTRUCTION rather than by a second, value-level
    /// normalizer bolted onto one side (which is what round 3 did, and what left
    /// this hole one nesting level down).
    ///
    /// # WHY MARSHAL BEATS SCHEMA AT ALL — from CASSANDRA, not from our own code
    ///
    /// `SerializationHeader.getType` (cassandra-5.0.8,
    /// `src/java/org/apache/cassandra/db/SerializationHeader.java`) is:
    ///
    /// ```text
    /// public AbstractType<?> getType(ColumnMetadata column)
    /// {
    ///     return typeMap == null ? column.type : typeMap.get(column.name.bytes);
    /// }
    /// ```
    ///
    /// Cassandra's OWN read path decodes with the header's recorded type and falls
    /// back to the live schema's `column.type` only when the header carries no type
    /// map. The recorded type is not a guess — it is what the writer put beside
    /// these bytes, and after an `ALTER` the on-disk bytes conform to IT, not to the
    /// current schema. Decoding old bytes with a newer schema is how you mis-decode
    /// them.
    ///
    /// This does NOT contradict CLAUDE.md's "schema, else `Statistics.db`". That
    /// rule is about where type information may come from AT ALL — prefer declared
    /// metadata, never infer a type from byte patterns (issue #28). It is not a
    /// claim that a user-supplied schema overrides the writer's recorded type for
    /// the same column; `getType` settles that question the other way.
    ///
    /// We are deliberately NARROWER than `getType`: the marshal is taken ONLY when
    /// it is UDT-bearing, so no non-UDT map key changes behaviour. One consequence,
    /// correct rather than unfortunate: a user-supplied schema that DISAGREES with
    /// the recorded type on a UDT-keyed multicell map is ignored — exactly the case
    /// Cassandra resolves in the header's favour.
    ///
    /// The strip is applied ONLY to the marshal form. When
    /// `prefer_udt_marshal_element` keeps the SCHEMA short form — every non
    /// UDT-bearing key — both readers already receive that same unstripped string
    /// (e.g. `frozen<set<int>>` on both sides), so stripping it would break the
    /// parity it is meant to preserve.
    ///
    /// On the FROZEN reader this call is a NO-OP in both branches, by
    /// construction: its marshal key never carries the outer marker (Cassandra
    /// omits it, measured on `f_map_tuple_udt` and `fcm`), and its schema branch is
    /// untouched. It is wired there anyway so the rule has ONE home and cannot
    /// drift into two opinions.
    pub(super) fn map_key_type_for_decode(marshal: Option<&str>, schema_short: &str) -> String {
        match marshal {
            // This guard MUST stay identical to `prefer_udt_marshal_element`'s, since
            // it decides the same choice; the `debug_assert` pins them together so a
            // change to one cannot silently diverge from the other.
            Some(m) if Self::is_udt_type(m) => {
                debug_assert!(
                    std::ptr::eq(
                        Self::prefer_udt_marshal_element(marshal, schema_short),
                        m as &str
                    ),
                    "map_key_type_for_decode's guard diverged from prefer_udt_marshal_element"
                );
                Self::strip_one_outer_frozen_marshal(m).to_string()
            }
            _ => schema_short.to_string(),
        }
    }

    /// Strip at most ONE outer `FrozenType(..)` from a MARSHAL type string,
    /// case-insensitively; return the input unchanged if it carries none.
    fn strip_one_outer_frozen_marshal(marshal: &str) -> &str {
        const PREFIX: &str = "org.apache.cassandra.db.marshal.frozentype(";
        let t = marshal.trim();
        let lower = t.to_ascii_lowercase();
        if lower.starts_with(PREFIX) && lower.ends_with(')') {
            return t[PREFIX.len()..t.len() - 1].trim();
        }
        t
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type P = V5CompressedLegacyParser;

    #[test]
    fn list_marshal_element_extracted() {
        let ht = "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type))";
        assert_eq!(
            P::extract_marshal_collection_elements(ht),
            Some(MarshalCollectionElements::Sequence(
                "org.apache.cassandra.db.marshal.Int32Type"
            ))
        );
    }

    #[test]
    fn set_marshal_element_extracted() {
        let ht = "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type))";
        assert_eq!(
            P::extract_marshal_collection_elements(ht),
            Some(MarshalCollectionElements::Sequence(
                "org.apache.cassandra.db.marshal.UTF8Type"
            ))
        );
    }

    #[test]
    fn bare_listtype_without_frozen_wrapper() {
        // Tolerate a header that is not wrapped in FrozenType(...).
        let ht =
            "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)";
        assert_eq!(
            P::extract_marshal_collection_elements(ht),
            Some(MarshalCollectionElements::Sequence(
                "org.apache.cassandra.db.marshal.Int32Type"
            ))
        );
    }

    #[test]
    fn map_marshal_key_value_extracted() {
        // frozen<map<text, frozen<address>>>
        let ht = "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.UserType(ks,61,66:org.apache.cassandra.db.marshal.UTF8Type))))";
        let got = P::extract_marshal_collection_elements(ht).expect("map elements");
        match got {
            MarshalCollectionElements::Map(k, v) => {
                assert_eq!(k, "org.apache.cassandra.db.marshal.UTF8Type");
                assert!(
                    P::is_udt_type(v),
                    "map value marshal must carry UserType: {v}"
                );
                // The comma INSIDE the value's UserType(...) must NOT split the pair.
                assert!(v.starts_with("org.apache.cassandra.db.marshal.FrozenType("));
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[test]
    fn nested_frozen_list_of_frozen_udt() {
        // frozen<list<frozen<person>>> — the #1240 `lp` column shape.
        let ht = "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.UserType(ks,706572736f6e,66:org.apache.cassandra.db.marshal.UTF8Type))))";
        let got = P::extract_marshal_collection_elements(ht).expect("list elements");
        match got {
            MarshalCollectionElements::Sequence(elem) => {
                assert!(P::is_udt_type(elem), "element marshal must carry UserType");
                assert!(elem.starts_with("org.apache.cassandra.db.marshal.FrozenType("));
            }
            other => panic!("expected Sequence, got {other:?}"),
        }
    }

    #[test]
    fn tuple_in_collection_returns_tuple_marshal() {
        let ht = "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.UTF8Type)))";
        assert_eq!(
            P::extract_marshal_collection_elements(ht),
            Some(MarshalCollectionElements::Sequence(
                "org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.UTF8Type)"
            ))
        );
    }

    #[test]
    fn malformed_unbalanced_parens_return_none() {
        // Missing the closing paren — must NOT panic, must return None.
        let ht = "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type";
        assert_eq!(P::extract_marshal_collection_elements(ht), None);
    }

    #[test]
    fn non_collection_marshal_returns_none() {
        // A top-level frozen UDT is NOT a collection — the collection extractor
        // must decline it (that column takes the top-level frozen-UDT path).
        let ht = "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.UserType(ks,61,66:org.apache.cassandra.db.marshal.UTF8Type))";
        assert_eq!(P::extract_marshal_collection_elements(ht), None);
    }

    #[test]
    fn map_missing_comma_returns_none() {
        // MapType with a single (malformed) argument — no top-level comma.
        let ht =
            "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type)";
        assert_eq!(P::extract_marshal_collection_elements(ht), None);
    }

    #[test]
    fn empty_and_garbage_return_none() {
        assert_eq!(P::extract_marshal_collection_elements(""), None);
        assert_eq!(
            P::extract_marshal_collection_elements("not a marshal type"),
            None
        );
        assert_eq!(P::extract_marshal_collection_elements("ListType("), None);
    }

    #[test]
    fn unresolvable_inner_type_decodes_to_blob_no_panic() {
        // Spec Req 1 scenario 4 (no-heuristics #28): with NO header marshal type
        // and NO wired registry, an unresolvable inner UDT short-name must stay an
        // opaque `Value::Blob` carrying the exact bytes — never a byte-pattern
        // guess, never a panic.
        let parser = V5CompressedLegacyParser::new("ks".to_string(), "tbl".to_string(), 0, 0, None);
        let bytes = [0xDE, 0xAD, 0xBE, 0xEF];
        let val = parser
            .parse_value_from_raw_bytes(&bytes, "some_unregistered_udt", "col", 0)
            .expect("unresolved UDT must not error");
        assert_eq!(val, Value::blob(bytes.to_vec()));
    }

    #[test]
    fn prefer_marshal_only_for_udt_elements() {
        let udt = "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.UserType(ks,61))";
        // UDT-bearing marshal wins over the schema short form.
        assert_eq!(
            P::prefer_udt_marshal_element(Some(udt), "frozen<person>"),
            udt
        );
        // Non-UDT marshal (primitive) keeps the schema short form (byte-parity path).
        assert_eq!(
            P::prefer_udt_marshal_element(Some("org.apache.cassandra.db.marshal.Int32Type"), "int"),
            "int"
        );
        // Absent marshal keeps the schema short form.
        assert_eq!(P::prefer_udt_marshal_element(None, "int"), "int");
    }
}
