//! J2 decoder-consolidation equivalence net (issue #1636, Epic #1603).
//!
//! Issue #1636 collapses the duplicated `ComparatorType -> Value` decoders. Two
//! live decoders share their scalar decode logic but historically DIVERGED on the
//! structural (tuple / UDT) framing:
//!
//! * **decoder #1** — [`SSTableReader::parse_value_with_comparator`]
//!   (`value_parsing.rs`): tuple/UDT field lengths are 4-byte big-endian signed
//!   `i32` (`-1` == null), matching Cassandra `TupleType`/`UserType`
//!   (`accessor.putInt`), the write side, and the v5 frozen path.
//! * **decoder #2** — the free [`parse_value_with_comparator`]
//!   (`comparator_value_parsing.rs`, driven by the block-path
//!   `RowCellStateMachine`): tuple/UDT field lengths were VInt-encoded.
//!
//! Feeding the SAME canonical (Cassandra-`i32`-BE) tuple/UDT bytes through both
//! decoders produced DIFFERENT results — the exact "type fix must land in N places
//! or paths silently diverge" defect J2 removes. This module pins the equivalence:
//! after consolidation both decoders share one structural body (i32-BE for
//! tuple/UDT, VInt for the non-frozen collection element framing they already
//! agreed on) and the assertions below hold identically.
//!
//! Golden arbiter for the tuple/UDT verdict: Cassandra `TupleType.buildValue` /
//! `UserType` write 4-byte `i32` field lengths (`-1` for null) — the same framing
//! `value_parsing::parse_tuple_value_with` (decoder #1, the live block path) and
//! the v5 frozen path already use. The VInt tuple/UDT arm in decoder #2 was the
//! outlier (dead for the `nb` corpus, which routes tuples through decoder #1 /
//! the v5 ladder); i32-BE is the retained behavior.

use super::decoder_lockstep_tests::{open_reader, scalar_cases};
use crate::storage::sstable::reader::parsing::comparator_value_parsing::parse_value_with_comparator as decode_free;
use crate::storage::sstable::reader::types::SSTableReader;
use crate::types::{ComparatorType, Value};

/// Decode value bytes through decoder #1 (the `SSTableReader` method) using a
/// `ComparatorType` directly (no type-string round-trip).
fn decode_method(
    reader: &SSTableReader,
    comp: &ComparatorType,
    bytes: &[u8],
) -> crate::Result<Value> {
    reader.parse_value_with_comparator_at_depth(bytes, comp, 0)
}

/// Canonical Cassandra tuple/UDT field framing: 4-byte big-endian `i32` length
/// prefix (`-1` for null), then the field body.
fn push_i32_field(buf: &mut Vec<u8>, body: &[u8]) {
    buf.extend_from_slice(&(body.len() as i32).to_be_bytes());
    buf.extend_from_slice(body);
}

/// Scalar decode is byte-identical across both decoders today; this sweep is a
/// regression guard so the consolidation cannot silently move a scalar arm.
#[tokio::test]
async fn consolidation_scalars_method_eq_free() {
    let Some(reader) = open_reader().await else {
        return;
    };
    for case in scalar_cases() {
        let comp = ComparatorType::from_data_type(case.cql_type)
            .unwrap_or_else(|e| panic!("comparator for {} failed: {e:?}", case.cql_type));
        let a = decode_method(&reader, &comp, &case.value_bytes);
        let b = decode_free(&case.value_bytes, &comp);
        match (&a, &b) {
            (Ok(va), Ok(vb)) => assert_eq!(
                va, vb,
                "decoder #1 (method) vs #2 (free) disagree on scalar {}",
                case.cql_type
            ),
            (Err(_), Err(_)) => {}
            _ => panic!(
                "decoder #1 vs #2 Ok/Err mismatch on scalar {}: {a:?} vs {b:?}",
                case.cql_type
            ),
        }
    }
}

/// MUST-FAIL on main / pass after consolidation: the SAME canonical i32-BE tuple
/// bytes must decode IDENTICALLY through both decoders. On main decoder #2 used
/// VInt framing and diverged; after J2 both share the i32-BE structural body.
#[tokio::test]
async fn consolidation_tuple_i32_framing_method_eq_free() {
    let Some(reader) = open_reader().await else {
        return;
    };
    // tuple<int, text> = (5, "hi") in Cassandra i32-BE field framing.
    let mut bytes = Vec::new();
    push_i32_field(&mut bytes, &5i32.to_be_bytes());
    push_i32_field(&mut bytes, b"hi");
    let comp = ComparatorType::Tuple(vec![ComparatorType::Int, ComparatorType::Text]);

    let method = decode_method(&reader, &comp, &bytes).expect("decoder #1 decodes i32-BE tuple");
    let free = decode_free(&bytes, &comp).expect("decoder #2 must decode i32-BE tuple after J2");
    assert_eq!(
        method,
        Value::Tuple(vec![Value::Integer(5), Value::Text("hi".into())]),
        "decoder #1 tuple decode drifted"
    );
    assert_eq!(
        method, free,
        "LOCKSTEP: decoder #1 (method, i32-BE) vs decoder #2 (free) must agree on \
         canonical tuple bytes (J2 #1636 — VInt tuple arm collapsed to i32-BE)"
    );
}

/// MUST-FAIL on main / pass after: canonical i32-BE UDT bytes decode identically
/// through both decoders (framing convergence; both fabricate the same fields).
#[tokio::test]
async fn consolidation_udt_i32_framing_method_eq_free() {
    let Some(reader) = open_reader().await else {
        return;
    };
    // udt person { name text, age int } = { "bob", 30 } in i32-BE field framing.
    let mut bytes = Vec::new();
    push_i32_field(&mut bytes, b"bob");
    push_i32_field(&mut bytes, &30i32.to_be_bytes());
    let comp = ComparatorType::Udt {
        type_name: "person".to_string(),
        keyspace: Some("ks".to_string()),
        field_comparators: vec![
            ("name".to_string(), ComparatorType::Text),
            ("age".to_string(), ComparatorType::Int),
        ],
    };

    let method = decode_method(&reader, &comp, &bytes).expect("decoder #1 decodes i32-BE udt");
    let free = decode_free(&bytes, &comp).expect("decoder #2 must decode i32-BE udt after J2");
    // Field values must agree (keyspace/type_name provenance may differ by path;
    // the framing + field decode is what J2 unifies).
    let (Value::Udt(m), Value::Udt(f)) = (&method, &free) else {
        panic!("expected UDT values, got {method:?} / {free:?}");
    };
    let mvals: Vec<_> = m.fields.iter().map(|x| (&x.name, &x.value)).collect();
    let fvals: Vec<_> = f.fields.iter().map(|x| (&x.name, &x.value)).collect();
    assert_eq!(
        mvals, fvals,
        "LOCKSTEP: decoder #1 (method, i32-BE) vs decoder #2 (free) must agree on \
         canonical UDT field framing (J2 #1636 — VInt UDT arm collapsed to i32-BE)"
    );
    assert_eq!(
        m.fields[0].value.as_ref(),
        Some(&Value::Text("bob".into())),
        "UDT name field"
    );
    assert_eq!(
        m.fields[1].value.as_ref(),
        Some(&Value::Integer(30)),
        "UDT age field"
    );
}
