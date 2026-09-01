//! The collection/tuple arms of THE UDT-field decoder (issue #3722).
//!
//! These live beside [`super::udt_field`] rather than inside it for two
//! reasons: `udt_field.rs` sits at the 800-line campsite source target (epic
//! #1116), and the four arms share one property worth stating once —
//!
//! # Elements are decoded STRUCTURALLY, from `&CqlType`
//!
//! The first cut of #3722 rendered each element type to a CQL type STRING and
//! handed it to `frozen.rs`'s `&str`-typed payload parsers (the renderer added
//! for that is gone with it — nothing else called it).
//! That silently discarded inline UDT field definitions: `CqlType::Udt(name,
//! fields)` renders to a BARE NAME, so a field typed `list<frozen<inner_u>>`
//! resolved its element by name through the `UdtRegistry` and — with no
//! registry, i.e. a schema-less read — fell back to `Value::Blob`. It is the
//! exact defect that makes the `CqlType::Udt` FIELD arm recurse structurally,
//! one level down.
//!
//! So every element is decoded by [`V5CompressedLegacyParser::parse_udt_field_value`]
//! on the structured `&CqlType`, at `depth + 1`. Consequences worth knowing:
//! an element follows the UDT-field conventions rather than
//! `parse_value_from_raw_bytes`'s (a `float` element is a lossless
//! `Value::Float32`, fixed-width elements are strict `!= N`, a `date` element
//! carries the 2^31 epoch offset), and a UDT element keeps its inline field
//! defs instead of needing a registry.
//!
//! The byte framing is NOT re-implemented here — it is the one implementation
//! in [`super::frozen_framing`], parameterized by an element-decode callback.
//!
//! # Trailing bytes are corruption
//!
//! A UDT field is exact-length-bounded by its own `[i32 BE len]` prefix, so a
//! well-formed collection followed by extra bytes is a corrupt field, not a
//! value to accept silently. Every arm requires the framing to consume
//! `data.len()` EXACTLY — the same strictness the fixed-width arms get from
//! `require_len`'s `!= N`. That consistency is the point: one rule for "the
//! field's bytes are exactly this value's bytes", whatever the type.

use super::*;

impl V5CompressedLegacyParser {
    /// `list<E>` / `set<E>` UDT field (`as_set` picks the `Value` variant).
    pub(super) fn parse_udt_field_sequence(
        &self,
        data: &[u8],
        element: &CqlType,
        as_set: bool,
        depth: usize,
    ) -> Result<Value> {
        let (value, consumed) =
            self.parse_frozen_sequence_raw_with(data, 0, "udt field", as_set, &|elem_data| {
                self.parse_udt_field_value(elem_data, element, depth + 1)
            })?;
        Self::require_full_consumption(consumed, data.len(), if as_set { "Set" } else { "List" })?;
        Ok(value)
    }

    /// `map<K, V>` UDT field.
    pub(super) fn parse_udt_field_map(
        &self,
        data: &[u8],
        key_type: &CqlType,
        value_type: &CqlType,
        depth: usize,
    ) -> Result<Value> {
        let (value, consumed) = self.parse_frozen_map_raw_with(
            data,
            0,
            "udt field",
            &|key_data| self.parse_udt_field_value(key_data, key_type, depth + 1),
            &|val_data| self.parse_udt_field_value(val_data, value_type, depth + 1),
        )?;
        Self::require_full_consumption(consumed, data.len(), "Map")?;
        Ok(value)
    }

    /// `tuple<...>` UDT field.
    pub(super) fn parse_udt_field_tuple(
        &self,
        data: &[u8],
        element_types: &[CqlType],
        depth: usize,
    ) -> Result<Value> {
        let mut offset = 0usize;
        let elements = self.parse_tuple_elements_raw_with(
            data,
            &mut offset,
            data.len(),
            element_types.len(),
            "udt field",
            &|idx, elem_data, elem_desc| {
                // `idx < element_types.len()` by construction (that length IS
                // the element count passed above); indexed defensively so this
                // stays panic-free.
                let element = element_types.get(idx).ok_or_else(|| {
                    Error::corruption(format!("{}: element index out of range", elem_desc))
                })?;
                self.parse_udt_field_value(elem_data, element, depth + 1)
            },
        )?;
        Self::require_full_consumption(offset, data.len(), "Tuple")?;
        Ok(Value::Tuple(elements))
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::helpers::*;
    use super::*;

    fn parser() -> V5CompressedLegacyParser {
        V5CompressedLegacyParser::new("test_ks".to_string(), "test_table".to_string(), 0, 0, None)
    }

    /// `[i32 BE count]` then `[i32 BE len][bytes]` per element.
    fn frozen_seq(elements: &[Vec<u8>]) -> Vec<u8> {
        let mut out = (elements.len() as i32).to_be_bytes().to_vec();
        for e in elements {
            out.extend_from_slice(&(e.len() as i32).to_be_bytes());
            out.extend_from_slice(e);
        }
        out
    }

    /// The bytes of a `frozen<inner_u>` value with fields `(a int, b text)`.
    fn inner_udt(a: i32, b: &str) -> Vec<u8> {
        let mut out = 4i32.to_be_bytes().to_vec();
        out.extend_from_slice(&a.to_be_bytes());
        out.extend_from_slice(&(b.len() as i32).to_be_bytes());
        out.extend_from_slice(b.as_bytes());
        out
    }

    fn inner_u_type() -> CqlType {
        CqlType::Udt(
            "inner_u".to_string(),
            vec![
                ("a".to_string(), CqlType::Int),
                ("b".to_string(), CqlType::Text),
            ],
        )
    }

    fn assert_inner_udt(value: &Value, a: i32, b: &str) {
        match value {
            Value::Udt(udt) => {
                assert_eq!(udt.type_name, "inner_u");
                assert_eq!(udt.fields.len(), 2, "fields: {:?}", udt.fields);
                assert_eq!(udt.fields[0].value, Some(Value::Integer(a)));
                assert_eq!(udt.fields[1].value, Some(Value::text(b)));
            }
            other => panic!("expected a structurally decoded inner_u, got {other:?}"),
        }
    }

    /// BLOCKER 1 (roborev, #3722): a UDT element inside a collection must be
    /// decoded from the INLINE field defs. Rendering the element type to a
    /// string yields the bare name `inner_u`, which with no `UdtRegistry` —
    /// this parser has none — decodes to an opaque `Value::Blob`.
    #[test]
    fn list_of_udt_elements_decodes_structurally_without_a_registry() {
        let data = frozen_seq(&[inner_udt(11, "e1"), inner_udt(22, "e2")]);
        let value = parser()
            .parse_udt_field_value(&data, &CqlType::List(Box::new(inner_u_type())), 0)
            .expect("list<frozen<inner_u>> field must decode");
        match value {
            Value::List(elements) => {
                assert_eq!(elements.len(), 2);
                assert_inner_udt(&elements[0], 11, "e1");
                assert_inner_udt(&elements[1], 22, "e2");
            }
            other => panic!("expected a List, got {other:?}"),
        }
    }

    #[test]
    fn set_map_and_tuple_udt_elements_decode_structurally() {
        let p = parser();

        let set_data = frozen_seq(&[inner_udt(1, "s")]);
        match p
            .parse_udt_field_value(&set_data, &CqlType::Set(Box::new(inner_u_type())), 0)
            .expect("set element")
        {
            Value::Set(e) => assert_inner_udt(&e[0], 1, "s"),
            other => panic!("expected a Set, got {other:?}"),
        }

        // map<text, frozen<inner_u>>: [count][klen][k][vlen][v]
        let inner = inner_udt(2, "m");
        let mut map_data = 1i32.to_be_bytes().to_vec();
        map_data.extend_from_slice(&1i32.to_be_bytes());
        map_data.extend_from_slice(b"k");
        map_data.extend_from_slice(&(inner.len() as i32).to_be_bytes());
        map_data.extend_from_slice(&inner);
        match p
            .parse_udt_field_value(
                &map_data,
                &CqlType::Map(Box::new(CqlType::Text), Box::new(inner_u_type())),
                0,
            )
            .expect("map value element")
        {
            Value::Map(entries) => {
                assert_eq!(entries[0].0, Value::text("k"));
                assert_inner_udt(&entries[0].1, 2, "m");
            }
            other => panic!("expected a Map, got {other:?}"),
        }

        // tuple<frozen<inner_u>>: [len][bytes] per element, no count prefix.
        let inner = inner_udt(3, "t");
        let mut tuple_data = (inner.len() as i32).to_be_bytes().to_vec();
        tuple_data.extend_from_slice(&inner);
        match p
            .parse_udt_field_value(&tuple_data, &CqlType::Tuple(vec![inner_u_type()]), 0)
            .expect("tuple element")
        {
            Value::Tuple(e) => assert_inner_udt(&e[0], 3, "t"),
            other => panic!("expected a Tuple, got {other:?}"),
        }
    }

    /// Elements follow the UDT-field conventions, not
    /// `parse_value_from_raw_bytes`'s: a `float` element stays lossless
    /// (issue #1884).
    #[test]
    fn float_element_stays_float32() {
        let data = frozen_seq(&[0.1f32.to_be_bytes().to_vec()]);
        match parser()
            .parse_udt_field_value(&data, &CqlType::List(Box::new(CqlType::Float)), 0)
            .expect("list<float> field")
        {
            Value::List(e) => assert_eq!(e[0], Value::Float32(0.1f32)),
            other => panic!("expected a List, got {other:?}"),
        }
    }

    /// BLOCKER 2 (roborev, #3722): a valid collection followed by trailing
    /// bytes is corruption — the field's `[i32 BE len]` prefix bounds it
    /// exactly, so partial consumption must never be accepted silently.
    #[test]
    fn trailing_bytes_after_a_collection_are_corruption() {
        let p = parser();

        let mut list_data = build_frozen_list_int(&[1, 2]);
        list_data.push(0xFF);
        let err = p
            .parse_udt_field_value(&list_data, &CqlType::List(Box::new(CqlType::Int)), 0)
            .expect_err("trailing byte after a list must be rejected");
        assert!(err.to_string().contains("trailing"), "got: {err}");

        let mut set_data = build_frozen_list_text(&["a"]);
        set_data.extend_from_slice(&[0, 0]);
        assert!(p
            .parse_udt_field_value(&set_data, &CqlType::Set(Box::new(CqlType::Text)), 0)
            .is_err());

        let mut map_data = build_frozen_map_text_int(&[("k", 9)]);
        map_data.push(0x00);
        assert!(p
            .parse_udt_field_value(
                &map_data,
                &CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Int)),
                0
            )
            .is_err());

        let mut tuple_data = 4i32.to_be_bytes().to_vec();
        tuple_data.extend_from_slice(&11i32.to_be_bytes());
        tuple_data.push(0x7F);
        assert!(p
            .parse_udt_field_value(&tuple_data, &CqlType::Tuple(vec![CqlType::Int]), 0)
            .is_err());
    }

    /// The exact-length forms still decode — the check is `==`, not "reject
    /// anything with a payload".
    #[test]
    fn exactly_consumed_collections_still_decode() {
        let p = parser();
        assert_eq!(
            p.parse_udt_field_value(
                &build_frozen_list_int(&[1, 2]),
                &CqlType::List(Box::new(CqlType::Int)),
                0
            )
            .expect("list"),
            Value::List(vec![Value::Integer(1), Value::Integer(2)])
        );
        assert_eq!(
            p.parse_udt_field_value(
                &build_frozen_map_text_int(&[("k", 9)]),
                &CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Int)),
                0
            )
            .expect("map"),
            Value::Map(vec![(Value::text("k"), Value::Integer(9))])
        );
    }
}
