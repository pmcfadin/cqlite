//! SERIALIZATION_HEADER component builder (MetadataType ordinal 3).
//!
//! Serialises the table's EncodingStats baselines and schema (key type,
//! clustering types, static/regular columns) into the Statistics.db
//! SERIALIZATION_HEADER component.

use super::marshal::cql_type_to_marshal_type;
use super::metadata::StatisticsMetadata;
use super::{StatisticsWriter, DELETION_TIME_EPOCH, TIMESTAMP_EPOCH, TTL_EPOCH};
use crate::error::Result;
use crate::parser::vint::encode_vuint;
use crate::schema::TableSchema;
use std::io::Write;

impl StatisticsWriter {
    /// Build SERIALIZATION_HEADER component (MetadataType ordinal 3)
    ///
    /// Format (SerializationHeader.java Serializer, lines 594-603):
    /// - EncodingStats: 3 unsigned VInts (minTimestamp, minLocalDeletionTime, minTTL deltas from epochs)
    /// - keyType: VInt length + UTF-8 type string
    /// - clusteringTypes: unsigned VInt count + list of types
    /// - staticColumns: unsigned VInt count + map of (column name, type)
    /// - regularColumns: unsigned VInt count + map of (column name, type)
    ///
    /// When `schema` is Some, populates keyType, clustering types, and column
    /// names/types from the actual table schema. When None, falls back to a
    /// minimal stub (BytesType, zero columns).
    ///
    /// # Column-set encoding (>64 columns)
    ///
    /// The static/regular column sets are encoded exactly as Cassandra's
    /// `SerializationHeader.Serializer.writeColumnsWithTypes`
    /// (cassandra-5.0.0 `SerializationHeader.java` lines 489-497): an unsigned-VInt
    /// column count followed by `count` `(VInt-length name, VInt-length marshal type)`
    /// pairs. This path has **no** 64-column limit and never uses a bitmap.
    ///
    /// The 64-bit bitmap encoding lives in `Columns.serializer.serializeSubset`
    /// (`Columns.java` lines 503-531) and is only used to serialise a per-row column
    /// subset against a pre-shared superset (Data.db rows / inter-node messaging),
    /// where `supersetCount < 64` selects the bitmap and `>= 64` switches to a VInt
    /// delta list. It is never used for the SSTable SERIALIZATION_HEADER, so wide
    /// tables (>64 columns) round-trip losslessly here.
    pub(super) fn build_serialization_header_component(
        &self,
        schema: Option<&TableSchema>,
        metadata: &StatisticsMetadata,
    ) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        // EncodingStats: 3 unsigned VInts representing deltas from epochs.
        // These baselines MUST match the values used by DataWriter for delta encoding.
        // Cassandra: EncodingStats.Serializer.serialize() writes:
        //   writeUnsignedVInt(minTimestamp - TIMESTAMP_EPOCH)
        //   writeUnsignedVInt(minLocalDeletionTime - DELETION_TIME_EPOCH)
        //   writeUnsignedVInt(minTTL - TTL_EPOCH)

        // minTimestamp delta from epoch
        let min_ts = if metadata.min_timestamp == i64::MAX {
            // No data recorded: use epoch as baseline
            TIMESTAMP_EPOCH as u64
        } else {
            metadata.min_timestamp as u64
        };
        let min_ts_delta = min_ts.wrapping_sub(TIMESTAMP_EPOCH as u64);
        buffer.write_all(&encode_vuint(min_ts_delta))?;

        // minLocalDeletionTime delta from epoch.
        //
        // Cassandra: `EncodingStats.Serializer.serialize` writes the local-deletion
        // baseline with `writeUnsignedVInt32(minLocalDeletionTime - DELETION_TIME_EPOCH)`
        // (both operands are Java `int`s), and the reader recovers it with
        // `readUnsignedVInt32()`, which runs `VIntCoding.checkedCast` and REJECTS any
        // decoded value that does not round-trip through a SIGNED 32-bit `int`
        // (`(int)value != value`). The on-disk form therefore carries the SIGN-EXTENDED
        // delta: for a small LDT like 2, `2 - DELETION_TIME_EPOCH` is the negative int
        // `-1442879998`, written as the sign-extended 64-bit VInt `0xFFFFFFFFAA…` which
        // `readUnsignedVInt32` accepts and folds back to `2`. Truncating to a bare `u32`
        // (e.g. `2852087298`) is OUT OF RANGE for `checkedCast` and makes Cassandra's
        // `sstabledump` reject the SSTable (verified live against `cassandra:5.0`).
        //
        // Casting the i32 delta to `u64` sign-extends in Rust exactly as Java's
        // `writeUnsignedVInt32` requires, and it also handles a far-future LDT stored as
        // a negative i32 bit pattern identically (the bit pattern IS the signed int the
        // reader expects). This mirrors the DataWriter per-row deletion deltas.
        let min_ldt = if metadata.min_local_deletion_time == i32::MAX {
            // No deletions: use Integer.MAX_VALUE as baseline (DeletionTime.LIVE)
            i32::MAX
        } else {
            metadata.min_local_deletion_time
        };
        let min_del_delta = (min_ldt.wrapping_sub(DELETION_TIME_EPOCH) as i64) as u64;
        buffer.write_all(&encode_vuint(min_del_delta))?;

        // minTTL delta from TTL_EPOCH (TTL_EPOCH=0)
        let min_ttl = if metadata.min_ttl == i32::MAX {
            // No TTL: use 0 as baseline
            0u64
        } else {
            metadata.min_ttl as u64
        };
        let min_ttl_delta = min_ttl.wrapping_sub(TTL_EPOCH as u64);
        buffer.write_all(&encode_vuint(min_ttl_delta))?;

        match schema {
            Some(s) => {
                // keyType: single PK → simple type, composite PK → CompositeType(...)
                let key_marshal = if s.partition_keys.len() > 1 {
                    let inner: Vec<String> = s
                        .partition_keys
                        .iter()
                        .map(|pk| cql_type_to_marshal_type(&pk.data_type))
                        .collect();
                    format!(
                        "org.apache.cassandra.db.marshal.CompositeType({})",
                        inner.join(",")
                    )
                } else if !s.partition_keys.is_empty() {
                    cql_type_to_marshal_type(&s.partition_keys[0].data_type)
                } else {
                    "org.apache.cassandra.db.marshal.BytesType".to_string()
                };
                buffer.write_all(&encode_vuint(key_marshal.len() as u64))?;
                buffer.write_all(key_marshal.as_bytes())?;

                // clusteringTypes: VUInt count + for each CK: VUInt-length-prefixed marshal type
                buffer.write_all(&encode_vuint(s.clustering_keys.len() as u64))?;
                for ck in &s.clustering_keys {
                    let ck_marshal = cql_type_to_marshal_type(&ck.data_type);
                    buffer.write_all(&encode_vuint(ck_marshal.len() as u64))?;
                    buffer.write_all(ck_marshal.as_bytes())?;
                }

                // Collect partition key and clustering key names for filtering
                let pk_names: std::collections::HashSet<&str> =
                    s.partition_keys.iter().map(|k| k.name.as_str()).collect();
                let ck_names: std::collections::HashSet<&str> =
                    s.clustering_keys.iter().map(|k| k.name.as_str()).collect();

                // staticColumns: filter for is_static && not PK/CK, sorted alphabetically
                let mut static_cols: Vec<_> = s
                    .columns
                    .iter()
                    .filter(|c| {
                        c.is_static
                            && !pk_names.contains(c.name.as_str())
                            && !ck_names.contains(c.name.as_str())
                    })
                    .collect();
                static_cols.sort_by(|a, b| a.name.cmp(&b.name));
                buffer.write_all(&encode_vuint(static_cols.len() as u64))?;
                for col in &static_cols {
                    // Column name: VUInt length + UTF-8 bytes
                    buffer.write_all(&encode_vuint(col.name.len() as u64))?;
                    buffer.write_all(col.name.as_bytes())?;
                    // Column type: VUInt length + marshal type bytes
                    let col_marshal = cql_type_to_marshal_type(&col.data_type);
                    buffer.write_all(&encode_vuint(col_marshal.len() as u64))?;
                    buffer.write_all(col_marshal.as_bytes())?;
                }

                // regularColumns: filter for !is_static && not PK/CK, sorted alphabetically
                // Cassandra's SerializationHeader stores columns in natural order (alphabetical)
                let mut regular_cols: Vec<_> = s
                    .columns
                    .iter()
                    .filter(|c| {
                        !c.is_static
                            && !pk_names.contains(c.name.as_str())
                            && !ck_names.contains(c.name.as_str())
                    })
                    .collect();
                regular_cols.sort_by(|a, b| a.name.cmp(&b.name));
                buffer.write_all(&encode_vuint(regular_cols.len() as u64))?;
                for col in &regular_cols {
                    buffer.write_all(&encode_vuint(col.name.len() as u64))?;
                    buffer.write_all(col.name.as_bytes())?;
                    let col_marshal = cql_type_to_marshal_type(&col.data_type);
                    buffer.write_all(&encode_vuint(col_marshal.len() as u64))?;
                    buffer.write_all(col_marshal.as_bytes())?;
                }
            }
            None => {
                // Minimal stub: BytesType key, no clustering, no columns
                let key_type = b"org.apache.cassandra.db.marshal.BytesType";
                buffer.write_all(&encode_vuint(key_type.len() as u64))?;
                buffer.write_all(key_type)?;

                // clusteringTypes: 0
                buffer.write_all(&encode_vuint(0))?;
                // staticColumns: 0
                buffer.write_all(&encode_vuint(0))?;
                // regularColumns: 0
                buffer.write_all(&encode_vuint(0))?;
            }
        }

        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::vint::parse_vuint;
    use std::path::PathBuf;

    #[test]
    fn test_serialization_header_with_schema() {
        use crate::schema::{Column, KeyColumn, TableSchema};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "uuid".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "age".to_string(),
                    data_type: "int".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let writer = StatisticsWriter::new(PathBuf::from("test.db"));
        let meta = StatisticsMetadata::new();
        let result = writer.build_serialization_header_component(Some(&schema), &meta);
        assert!(result.is_ok());

        let bytes = result.unwrap();

        // Verify the header contains the UUIDType key type
        let header_str = String::from_utf8_lossy(&bytes);
        assert!(
            header_str.contains("UUIDType"),
            "Header should contain UUIDType for uuid partition key"
        );

        // Verify column names are present
        assert!(
            header_str.contains("name"),
            "Header should contain column 'name'"
        );
        assert!(
            header_str.contains("age"),
            "Header should contain column 'age'"
        );

        // Verify column types are present
        assert!(
            header_str.contains("UTF8Type"),
            "Header should contain UTF8Type for text column"
        );
        assert!(
            header_str.contains("Int32Type"),
            "Header should contain Int32Type for int column"
        );
    }

    #[test]
    fn test_serialization_header_composite_partition_key() {
        use crate::schema::{Column, KeyColumn, TableSchema};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "composite_table".to_string(),
            partition_keys: vec![
                KeyColumn {
                    name: "tenant".to_string(),
                    data_type: "text".to_string(),
                    position: 0,
                },
                KeyColumn {
                    name: "id".to_string(),
                    data_type: "uuid".to_string(),
                    position: 1,
                },
            ],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "tenant".to_string(),
                    data_type: "text".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "id".to_string(),
                    data_type: "uuid".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "value".to_string(),
                    data_type: "int".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let writer = StatisticsWriter::new(PathBuf::from("test.db"));
        let meta = StatisticsMetadata::new();
        let bytes = writer
            .build_serialization_header_component(Some(&schema), &meta)
            .unwrap();

        let header_str = String::from_utf8_lossy(&bytes);
        assert!(
            header_str.contains("CompositeType("),
            "Composite PK should produce CompositeType wrapper"
        );
        assert!(
            header_str.contains("UTF8Type"),
            "CompositeType should contain UTF8Type for text PK"
        );
        assert!(
            header_str.contains("UUIDType"),
            "CompositeType should contain UUIDType for uuid PK"
        );
    }

    /// Parse a Cassandra SSTable SERIALIZATION_HEADER column-set the way Cassandra's
    /// `SerializationHeader.Serializer.readColumnsWithType` does (cassandra-5.0.0
    /// `SerializationHeader.java` lines 510-520):
    ///
    /// ```text
    /// unsigned-vint  count
    /// repeat count times:
    ///   vint-length-prefixed UTF-8  column name
    ///   vint-length-prefixed UTF-8  marshal type
    /// ```
    ///
    /// Returns the parsed `(name, marshal_type)` pairs and the slice remaining after
    /// the column set, so chained sets (static then regular) can be parsed.
    fn parse_columns_with_types(input: &[u8]) -> (Vec<(String, String)>, &[u8]) {
        let (mut rest, count) = parse_vuint(input).expect("column count vint");
        let mut cols = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let (after_name_len, name_len) = parse_vuint(rest).expect("name length vint");
            let name =
                String::from_utf8(after_name_len[..name_len as usize].to_vec()).expect("name utf8");
            let after_name = &after_name_len[name_len as usize..];

            let (after_type_len, type_len) = parse_vuint(after_name).expect("type length vint");
            let marshal =
                String::from_utf8(after_type_len[..type_len as usize].to_vec()).expect("type utf8");
            rest = &after_type_len[type_len as usize..];

            cols.push((name, marshal));
        }
        (cols, rest)
    }

    /// Build a schema with `n` regular columns named `c00..c{n-1}` plus a uuid PK.
    fn wide_schema(n: usize) -> crate::schema::TableSchema {
        use crate::schema::{Column, KeyColumn, TableSchema};
        use std::collections::HashMap;

        let mut columns = vec![Column {
            name: "id".to_string(),
            data_type: "uuid".to_string(),
            nullable: false,
            default: None,
            is_static: false,
        }];
        for i in 0..n {
            columns.push(Column {
                name: format!("c{i:03}"),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            });
        }

        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "wide_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns,
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    /// Regression test for issue #763: a table with more than 64 regular columns
    /// must produce a SERIALIZATION_HEADER that encodes every column.
    ///
    /// Cassandra's on-disk header uses `writeColumnsWithTypes` (SerializationHeader.java
    /// lines 489-497): an unsigned-VInt count followed by `count` (name, type) pairs.
    /// There is NO 64-bit bitmap on this path — the bitmap (`Columns.serializeSubset`,
    /// Columns.java lines 503-531) is only used for per-row column subsets against a
    /// pre-shared superset, never for the SSTable header. So a 70-column table is a
    /// fully supported, lossless encoding.
    #[test]
    fn test_serialization_header_70_columns_roundtrip() {
        let schema = wide_schema(70);
        let writer = StatisticsWriter::new(PathBuf::from("test.db"));
        let meta = StatisticsMetadata::new();
        let bytes = writer
            .build_serialization_header_component(Some(&schema), &meta)
            .expect("build header for 70-column schema");

        // Skip the 3 EncodingStats VInts, the key type, and the clustering list to
        // reach the static/regular column sets.
        let (rest, _min_ts) = parse_vuint(&bytes).expect("encoding stats minTimestamp");
        let (rest, _min_ldt) = parse_vuint(rest).expect("encoding stats minLocalDeletionTime");
        let (rest, _min_ttl) = parse_vuint(rest).expect("encoding stats minTTL");

        // keyType: vint-length-prefixed UTF-8
        let (rest, key_len) = parse_vuint(rest).expect("key type length");
        let key_type = std::str::from_utf8(&rest[..key_len as usize]).expect("key type utf8");
        assert_eq!(key_type, "org.apache.cassandra.db.marshal.UUIDType");
        let rest = &rest[key_len as usize..];

        // clusteringTypes: vint count (0 here)
        let (rest, ck_count) = parse_vuint(rest).expect("clustering count");
        assert_eq!(ck_count, 0, "no clustering columns");

        // staticColumns then regularColumns
        let (statics, rest) = parse_columns_with_types(rest);
        assert_eq!(statics.len(), 0, "no static columns");

        let (regulars, rest) = parse_columns_with_types(rest);
        assert!(rest.is_empty(), "header fully consumed, no trailing bytes");

        // All 70 regular columns must be present (the PK `id` is excluded).
        assert_eq!(
            regulars.len(),
            70,
            "all 70 regular columns must be encoded, got {}",
            regulars.len()
        );

        // Columns are emitted in alphabetical order; verify a sample round-trips.
        assert_eq!(regulars[0].0, "c000");
        assert_eq!(regulars[0].1, "org.apache.cassandra.db.marshal.Int32Type");
        assert_eq!(regulars[69].0, "c069");

        // Every column name and type must be intact (lossless).
        let mut names: Vec<String> = regulars.iter().map(|(n, _)| n.clone()).collect();
        names.sort();
        for (i, name) in names.iter().enumerate().take(70) {
            assert_eq!(*name, format!("c{i:03}"));
        }
    }

    /// Verify the column-count field is encoded as a true unsigned VInt (not a single
    /// byte). For 200 columns the count 200 (0xC8) requires a 2-byte VInt, which is
    /// where a naive single-byte writer would silently corrupt the header.
    #[test]
    fn test_serialization_header_200_columns_count_is_vint() {
        let schema = wide_schema(200);
        let writer = StatisticsWriter::new(PathBuf::from("test.db"));
        let meta = StatisticsMetadata::new();
        let bytes = writer
            .build_serialization_header_component(Some(&schema), &meta)
            .expect("build header for 200-column schema");

        let (rest, _) = parse_vuint(&bytes).expect("minTimestamp");
        let (rest, _) = parse_vuint(rest).expect("minLocalDeletionTime");
        let (rest, _) = parse_vuint(rest).expect("minTTL");
        let (rest, key_len) = parse_vuint(rest).expect("key type length");
        let rest = &rest[key_len as usize..];
        let (rest, _ck) = parse_vuint(rest).expect("clustering count");
        let (statics, rest) = parse_columns_with_types(rest);
        assert_eq!(statics.len(), 0);

        // The regular-column count must be the 2-byte VInt encoding of 200.
        let expected_count_bytes = encode_vuint(200);
        assert_eq!(
            expected_count_bytes.len(),
            2,
            "200 must require a 2-byte VInt (sanity)"
        );
        assert_eq!(
            &rest[..expected_count_bytes.len()],
            expected_count_bytes.as_slice(),
            "regular-column count must be a multi-byte unsigned VInt"
        );

        let (regulars, tail) = parse_columns_with_types(rest);
        assert!(tail.is_empty());
        assert_eq!(regulars.len(), 200, "all 200 columns must be encoded");
    }

    /// Regression: the EncodingStats `minLocalDeletionTime` delta must be written
    /// as the SIGN-EXTENDED `int` delta `minLocalDeletionTime - DELETION_TIME_EPOCH`,
    /// because Cassandra recovers it with `readUnsignedVInt32()` →
    /// `VIntCoding.checkedCast`, which REJECTS any decoded value that does not
    /// round-trip through a signed 32-bit `int` (`(int)value != value`). A bare
    /// `u32` truncation of a small LDT (e.g. delta `2852087298` for LDT `2`) is out
    /// of range for `checkedCast` and makes `cassandra:5.0` `sstabledump` reject the
    /// SSTable (verified live; see tests/issue_911_bti_partition_deletion_stats.rs).
    ///
    /// A far-future LDT in `[2^31, 2^32)` is stored as a negative i32 bit pattern,
    /// which IS the signed int the reader expects, so the same sign-extending path
    /// handles it. This pins both: the small-LDT delta is a sign-extended 64-bit
    /// VInt, and the decoded value round-trips through a signed i32.
    #[test]
    fn test_serialization_header_ldt_baseline_sign_extends_for_checked_cast() {
        // Helper: decode the minLocalDeletionTime EncodingStats delta for a given
        // baseline and assert it round-trips through Cassandra's checkedCast (i.e.
        // the decoded u64, reinterpreted as i64, fits a signed i32).
        let decode_delta = |min_ldt: i32| -> u64 {
            let mut meta = StatisticsMetadata::new();
            meta.min_local_deletion_time = min_ldt;
            let writer = StatisticsWriter::new(PathBuf::from("test.db"));
            let bytes = writer
                .build_serialization_header_component(None, &meta)
                .expect("build header");
            let (rest, _min_ts_delta) = parse_vuint(&bytes).expect("minTimestamp delta");
            let (_rest, min_ldt_delta) = parse_vuint(rest).expect("minLocalDeletionTime delta");
            min_ldt_delta
        };

        // checkedCast accepts `value` iff `(int)value == value` — i.e. the decoded
        // u64, reinterpreted as i64, equals its own truncation to i32.
        let passes_checked_cast = |delta: u64| -> bool {
            let v = delta as i64;
            (v as i32) as i64 == v
        };

        // Small LDT (e.g. 2): the regression case. The delta is the negative int
        // `2 - DELETION_TIME_EPOCH`, sign-extended.
        let small = decode_delta(2);
        let expected_small = (2i32.wrapping_sub(DELETION_TIME_EPOCH) as i64) as u64;
        assert_eq!(small, expected_small);
        assert!(
            passes_checked_cast(small),
            "small-LDT delta must round-trip through a signed i32 (checkedCast), got {small:#x}"
        );

        // Far-future LDT in [2^31, 2^32): a negative i32 bit pattern. Same path.
        let far_future = ((1u32 << 31) + 5) as i32;
        assert!(far_future < 0, "sanity: far-future LDT is a negative i32");
        let far = decode_delta(far_future);
        let expected_far = (far_future.wrapping_sub(DELETION_TIME_EPOCH) as i64) as u64;
        assert_eq!(far, expected_far);
        assert!(
            passes_checked_cast(far),
            "far-future LDT delta must round-trip through a signed i32 (checkedCast), got {far:#x}"
        );
    }
}
