//! `varint` is a primitive CQL type for the type parser (#4237 R2.2): it must parse to
//! `CqlType::Varint`, not `Custom("varint")`, which typed varint columns in `SELECT *` metadata as
//! opaque blobs so Parquet/Vortex export failed with "expected Blob value, got Varint".

use cqlite_core::parser::complex_types::ComplexTypeParser;
use cqlite_core::schema::CqlType;

#[test]
fn varint_parses_as_a_primitive_not_custom() {
    let parser = ComplexTypeParser::new();
    assert_eq!(
        parser.parse_type("varint").unwrap().cql_type,
        CqlType::Varint
    );
    assert_eq!(
        parser.parse_type("frozen<list<varint>>").unwrap().cql_type,
        CqlType::Frozen(Box::new(CqlType::List(Box::new(CqlType::Varint))))
    );
}
