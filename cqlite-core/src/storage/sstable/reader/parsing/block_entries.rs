//! Block entry parsing for SSTable data
//!
//! This module handles parsing of block entries from decompressed block data,
//! including modern Cassandra 5+ format parsing with state machine integration
//! and legacy format support.

use super::BufferExtent;
use crate::{
    parser::vint::parse_vint_length, types::TableId, Error, Result, RowKey, ScanRow, Value,
};

use super::super::{
    super::{
        compression::Compression,
        row_cell_state_machine::{ParsedRow, RowCellStateMachine},
    },
    types::SSTableReader,
};

/// Wrap a single decoded, named column cell for the scan → query channel
/// (issue #1334). Thin borrowing wrapper over the crate-wide classifier
/// [`ScanRow::classify_cell`], so static/clustering cells obey the SAME
/// live-value-vs-marker invariant as every other producer: a genuinely absent
/// cell (`Value::Null`) or tombstone stays a suppressible [`ScanRow::Marker`];
/// any REAL decoded value becomes a live [`ScanRow::Row`] carrying the interned
/// `Arc<str>` column name so it surfaces in SELECT/export output.
fn live_cell_scan_row(column_name: &str, value: &Value) -> ScanRow {
    ScanRow::classify_cell(column_name, value.clone())
}

/// Classify the WHOLE-ROW raw value that the legacy `parse_block_entries*`
/// fallback decodes (issue #1334 / roborev round 8 finding 1).
///
/// The prior fix decoded the value via `parse_column_value_enhanced` and then
/// DISCARDED it, always emitting `ScanRow::RawRow` — so a schema-decoded typed
/// value silently degraded to the synthetic `"data"` blob. The authoritative
/// rule (no heuristic):
///
/// - A column name resolves from the key context (`extract_column_name_from_context`,
///   the same resolution `parse_column_value_enhanced` used to schema-decode) →
///   surface the DECODED value as a typed live cell via [`ScanRow::classify_cell`]
///   (a genuine `Value::Null`/tombstone stays a suppressed marker). The decoded
///   value is NEVER dropped.
/// - No column name resolves → the bytes remain effectively UNDECODED (blob
///   fallback / decode not possible); carry them with explicit RAW provenance
///   ([`ScanRow::RawRow`]) so a schema-aware consumer re-decodes and a no-schema
///   consumer surfaces the exact pre-#1334 `"data"` blob.
fn fallback_value_scan_row(column_name: Option<&str>, decoded: Value, raw_bytes: &[u8]) -> ScanRow {
    match column_name {
        Some(name) => ScanRow::classify_cell(name, decoded),
        None => ScanRow::RawRow(raw_bytes.to_vec()),
    }
}

impl SSTableReader {
    /// Parse block entries with optional schema parameter
    ///
    /// This method provides schema-aware parsing when a schema is provided via the parameter.
    /// It delegates to the four-tier schema lookup strategy in `get_table_schema()`.
    ///
    /// # Arguments
    /// * `block_data` - Raw block data to parse
    /// * `schema` - Optional schema from query executor (highest priority in lookup chain)
    ///
    /// # Schema Resolution Strategy
    /// The schema parameter is passed to `get_table_schema()` which implements:
    /// 0. Provided schema (from this parameter) - highest priority
    /// 1. SSTable header schema (V5.0+ formats)
    /// 2. Schema registry lookup (external schema files)
    /// 3. Header-constructed fallback schema
    ///
    /// `read_shadowing` (issue #1741): `true` for user-facing SELECT scans, `false`
    /// for physical consumers (see [`SSTableReader::build_v5_parser`]).
    pub(in crate::storage::sstable::reader) fn parse_block_entries_with_schema(
        &self,
        block_data: &[u8],
        schema: Option<&crate::schema::TableSchema>,
        read_shadowing: bool,
    ) -> Result<Vec<(TableId, RowKey, ScanRow)>> {
        // Pass the provided schema through to the parsing logic
        // This schema parameter flows through the call chain to get_table_schema()
        self.parse_block_entries(block_data, schema, read_shadowing)
    }

    /// Parse block entries from decompressed block data.
    ///
    /// `read_shadowing` (issue #1741): `true` for user-facing SELECT scans (applies
    /// partition/range-tombstone shadowing + TTL expiry), `false` for physical
    /// consumers that must see every on-disk row (see
    /// [`SSTableReader::build_v5_parser`]).
    pub(in crate::storage::sstable::reader) fn parse_block_entries(
        &self,
        block_data: &[u8],
        schema: Option<&crate::schema::TableSchema>,
        read_shadowing: bool,
    ) -> Result<Vec<(TableId, RowKey, ScanRow)>> {
        self.parse_block_entries_at_now(block_data, schema, read_shadowing, None)
    }

    /// [`parse_block_entries`](Self::parse_block_entries) with a caller-pinned
    /// read-time TTL clock (issue #3058).
    ///
    /// `now_secs`: `Some` pins the decoder's expiry instant to the one the caller
    /// already captured for the whole request (the Flight single-source fast
    /// path), instead of the ambient sample the parser takes at construction;
    /// `None` keeps that ambient sample. Only consulted when `read_shadowing`.
    pub(in crate::storage::sstable::reader) fn parse_block_entries_at_now(
        &self,
        block_data: &[u8],
        schema: Option<&crate::schema::TableSchema>,
        read_shadowing: bool,
        now_secs: Option<i64>,
    ) -> Result<Vec<(TableId, RowKey, ScanRow)>> {
        tracing::debug!(
            "parse_block_entries: Starting parse (data size: {} bytes, version: {:?})",
            block_data.len(),
            self.header.cassandra_version
        );

        // KNOWN FAIL-OPEN SEAM — issue #3108, deliberately NOT guarded here. The
        // `V5UncompressedOA` (BTI `da`) route calls `parse_block_entries_with_state_machine`,
        // which takes neither `read_shadowing` nor `now_secs` and silently DROPS both; the
        // single-source query path misses it only because `supports_streaming_query_scan()`
        // refuses BTI readers (implicit — #3108 owns making it explicit). #3109 NARROWED but
        // did NOT close the `read_shadowing = true` callers reaching here with a `da`
        // reader: the four SCAN surfaces (`scan`, `sequential_scan`, per-row/batched
        // streaming) now dispatch BTI to the trie walk first, but a FIFTH site stays
        // OPEN — `scan_for_key` (`data_access/sequential.rs`) parses with `true` and NO
        // BTI gate, reached by a `da` reader via `verify_presence_oracle_negative`
        // (`presence_verify.rs`) under opt-in presence verification. A #3108 guard CAN
        // therefore still fire today; that site is out of #3109's scope.

        let mut entries = Vec::new();

        // An empty block is a no-op regardless of compression: `read_next_block`
        // can yield `Ok(Some(Vec::new()))` for a zero-length block, and an empty
        // buffer is not valid compressed input. Short-circuit BEFORE any decompress
        // so the enclosing scan continues instead of failing closed on empty bytes.
        if block_data.is_empty() {
            return Ok(entries);
        }

        // Decompress block data if compression is enabled. Route through the single
        // chunk decode plane (`ChunkSource::decompress_only`, issue #2165 / G2) — the
        // stitch-path shape (`data_access/mod.rs`) — so `parsing/` no longer calls
        // `Compression::decompress` inline. A failed decompress FAILS CLOSED with a
        // corruption error; never a silent raw-bytes parse (no-heuristics, #28).
        //
        // Reachability invariant (#2165): this compressed branch is effectively
        // unreached on real files today (BTI+CompressionInfo → `bti_scan_with_metadata`;
        // nb multi-chunk → `requires_chunk_stitching` stitch path; uncompressed →
        // the `None` branch below), so fail-closed == corruption is currently safe.
        // A future re-wirer sending real compressed chunks here MUST first add the
        // stitch path's `len >= max_compressed_length` raw-passthrough (incompressible
        // chunks are stored uncompressed) — deliberately NOT duplicated here (dead today).
        let data = if let Some(compression_reader) = &self.compression_reader {
            tracing::debug!(
                "parse_block_entries: Attempting block decompression with algorithm: {:?}",
                compression_reader.algorithm()
            );
            let compression = Compression::new(*compression_reader.algorithm())?;
            super::super::chunk_source::ChunkSource::decompress_only(
                Some(&compression),
                block_data.to_vec(),
            )?
        } else {
            tracing::debug!("parse_block_entries: No compression, using raw block data");
            block_data.to_vec()
        };

        // Determine parsing strategy based on data format classification
        // V5_0DataFormat and related formats use compressed 'nb' with legacy serialization (u8 lengths)
        // V5_0NewBig/Bti use true 'oa' format with VInt encoding
        let data_format = self.header.cassandra_version.data_format();

        tracing::debug!(
            "parse_block_entries: Format: {:?}, DataFormat: {:?}",
            self.header.cassandra_version,
            data_format
        );

        // Use state machine ONLY for true V5 uncompressed OA format (VInt encoding)
        let use_state_machine = matches!(
            data_format,
            crate::parser::header::DataFormat::V5UncompressedOA
        );

        tracing::debug!(
            "parse_block_entries: use_state_machine: {}",
            use_state_machine
        );

        if use_state_machine {
            tracing::debug!("parse_block_entries: Using state machine for true V5.0 'oa' format (VInt encoding)");

            // Log schema availability - NB format files may not have embedded schema
            if let Some(schema) = &self.schema {
                tracing::debug!(
                    "parse_block_entries: Schema available: {}.{}",
                    schema.keyspace,
                    schema.table
                );
            } else {
                tracing::debug!(
                    "[DEBUG SSTableReader::parse_block_entries] No schema in header for {:?}, will use basic state machine",
                    self.header.cassandra_version
                );
            }

            let result = self.parse_block_entries_with_state_machine(&data, schema);
            match &result {
                Ok(entries) => {
                    tracing::debug!(
                        "parse_block_entries: State machine returned {} entries",
                        entries.len()
                    );
                }
                Err(e) => {
                    tracing::debug!(
                        "[DEBUG SSTableReader::parse_block_entries] State machine failed: {}",
                        e
                    );
                }
            }
            return result;
        }

        // V5CompressedLegacy formats use dedicated parser (u8 length prefixes, not VInt)
        if matches!(
            data_format,
            crate::parser::header::DataFormat::V5CompressedLegacy
        ) {
            // Extract keyspace/table from path (most reliable for V5CompressedLegacy)
            // SSTable path format: {keyspace}/{table_name}-{uuid}/nb-1-big-Data.db
            let file_path = self.file_path();
            let (keyspace, table_name) = super::extract_keyspace_table_from_path(&file_path)
                .unwrap_or_else(|_| {
                    // Fallback to header values if path extraction fails
                    (self.header.keyspace.clone(), self.header.table_name.clone())
                });

            tracing::debug!(
                "V5CompressedLegacy format detected, using dedicated parser for {}.{} (from path)",
                keyspace,
                table_name
            );

            // Validate metadata - V5CompressedLegacy REQUIRES valid keyspace/table extraction
            // Cannot fall back to VInt parser because format uses u8 length prefixes, not VInt
            if keyspace.is_empty() || table_name.is_empty() {
                return Err(Error::corruption(format!(
                    "V5CompressedLegacy format requires valid keyspace/table extraction, \
                     but got keyspace='{}', table_name='{}' from path {:?}. \
                     Cannot fall back to VInt parser (format uses u8 length prefixes, not VInt). \
                     This indicates a path parsing bug or malformed SSTable directory structure.",
                    keyspace, table_name, file_path
                )));
            }

            // Use dedicated V5CompressedLegacy parser with EncodingStats from Statistics.db
            let table_id = TableId::from(format!("{}.{}", keyspace, table_name));

            // Extract EncodingStats from statistics_reader (if available)
            let (min_timestamp, min_local_deletion_time, min_ttl) = if let Some(stats_reader) =
                &self.statistics_reader
            {
                let ts_stats = &stats_reader.statistics().timestamp_stats;
                (
                    ts_stats.min_timestamp,
                    ts_stats.min_deletion_time,
                    ts_stats.min_ttl,
                )
            } else {
                // No statistics reader - use zeros (may cause incorrect absolute values for delta-coded fields)
                tracing::warn!("V5CompressedLegacy: No statistics_reader available, delta-coded timestamps/TTLs will use zero baseline");
                (0, 0, None)
            };

            // Extract keyspace and table_name from table_id (format: "keyspace.table_name")
            let table_id_str = table_id.name();
            let (keyspace, table_name) = table_id_str.split_once('.').unwrap_or(("", table_id_str));

            // NOTE: this site intentionally does NOT route through
            // `self.build_v5_parser()`: that helper sources keyspace/table from
            // `self.header`, which can be empty for V5CompressedLegacy `nb` files
            // (see the non-empty validation above). This path uses the
            // path-extracted keyspace/table as the reliable source. We still must
            // thread the same VersionGates + read-shadowing + now_secs (captured in
            // `new`) that `build_v5_parser` applies, so oa/da deletion-time layouts
            // are gated correctly instead of falling back to DEFAULT gates.
            let parser = super::V5CompressedLegacyParser::new(
                keyspace.to_string(),
                table_name.to_string(),
                min_timestamp,
                min_local_deletion_time,
                min_ttl,
            )
            // VG1: thread VersionGates from SSTableReader down to the row parser
            // (mirrors `build_v5_parser`) so VG3 can flip gate-sensitive paths.
            .with_version_gates(self.version_gates.clone());
            // Issue #1741: apply SELECT-semantic read shadowing per the caller.
            let parser = parser.with_read_shadowing(read_shadowing);
            // Issue #3058: honor the caller's pinned reconciliation clock.
            let parser = match now_secs {
                Some(now) => parser.with_now_secs(now),
                None => parser,
            };
            // Add UDT registry if available for UDT-aware collection parsing (Issue #238)
            let parser = if let Some(ref registry) = self.udt_registry {
                parser.with_udt_registry(registry.clone())
            } else {
                parser
            };

            // Get schema using four-tier lookup (provided -> header -> registry -> fallback)
            let table_schema = self.get_table_schema(schema);

            // #3782: `data` is a decompressed BLOCK read by the block-by-block
            // scans, whose tail may cut a row that continues in the next block,
            // so the tolerant break is the correct straddle behaviour here.
            return parser.parse_block(&data, BufferExtent::Window, table_schema.as_ref(), self);
        }

        // Enhanced partition data parsing for legacy formats
        let mut offset = 0;
        while offset < data.len() {
            // Parse entry header with enhanced validation and error handling
            let (new_offset, table_id_len) = parse_vint_length(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Failed to parse table ID length at offset {}: {:?}",
                    offset, e
                ))
            })?;
            offset = data.len() - new_offset.len();

            // Validate table ID length to prevent buffer overrun
            if table_id_len > 256 || offset + table_id_len > data.len() {
                return Err(Error::corruption(format!(
                    "Invalid table ID length {} at offset {}, remaining: {}",
                    table_id_len,
                    offset,
                    data.len() - offset
                )));
            }

            // Parse table ID with enhanced validation for binary IDs
            let table_id_bytes = &data[offset..offset + table_id_len];
            let table_id = match String::from_utf8(table_id_bytes.to_vec()) {
                Ok(s) => TableId::new(s),
                Err(_) => {
                    // Handle binary table IDs in Cassandra 5.0
                    let hex_id = hex::encode(table_id_bytes);
                    TableId::new(format!("binary_{}", hex_id))
                }
            };
            offset += table_id_len;

            // Enhanced row key parsing with Cassandra 5.0 format support
            let (new_offset, key_len) = parse_vint_length(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Failed to parse key length at offset {}: {:?}",
                    offset, e
                ))
            })?;
            offset = data.len() - new_offset.len();

            // Validate key length
            if key_len > 65536 || offset + key_len > data.len() {
                return Err(Error::corruption(format!(
                    "Invalid key length {} at offset {}, remaining: {}",
                    key_len,
                    offset,
                    data.len() - offset
                )));
            }

            // Parse compound/composite keys properly
            let key_data = &data[offset..offset + key_len];
            let key = if key_len > 0 {
                self.parse_composite_key(key_data, schema)?
            } else {
                RowKey::new(Vec::new()) // Empty key
            };
            offset += key_len;

            // Enhanced column data extraction with proper type handling
            let (new_offset, value_len) = parse_vint_length(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Failed to parse value length at offset {}: {:?}",
                    offset, e
                ))
            })?;
            offset = data.len() - new_offset.len();

            // Handle different value encodings in Cassandra 5.0.
            let scan_row = if value_len == 0 {
                // Empty value → suppressible null marker.
                ScanRow::Marker(Value::Null)
            } else if value_len > 16777216 {
                // 16MB limit
                return Err(Error::corruption(format!(
                    "Value too large: {} bytes at offset {}",
                    value_len, offset
                )));
            } else if offset + value_len > data.len() {
                return Err(Error::corruption(format!(
                    "Incomplete value: need {} bytes at offset {}, have {}",
                    value_len,
                    offset,
                    data.len() - offset
                )));
            } else {
                let value_data = &data[offset..offset + value_len];
                // Enforce the modern-format contract: V5 without schema must NOT
                // fall back to a raw blob (`parse_column_value_enhanced` errors and
                // we propagate). When a schema DOES decode a typed value AND a
                // column name resolves from the key context, surface that decoded
                // value as a typed live cell (issue #1334 / roborev round 8 finding
                // 1 — never drop it). Only when no column name resolves do we hand
                // the raw bytes downstream with explicit RAW provenance so a
                // schema-aware consumer re-decodes and a no-schema consumer surfaces
                // a single "data" blob — no downstream shape-guessing.
                let decoded =
                    self.parse_column_value_enhanced(value_data, &table_id, &key, schema)?;
                let column_name = self.extract_column_name_from_context(&table_id, &key);
                fallback_value_scan_row(column_name.as_deref(), decoded, value_data)
            };
            offset += value_len;

            entries.push((table_id, key, scan_row));
        }

        Ok(entries)
    }

    /// Parse block entries using the Cassandra 5 'oa' format state machine
    pub(in crate::storage::sstable::reader) fn parse_block_entries_with_state_machine(
        &self,
        data: &[u8],
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Vec<(TableId, RowKey, ScanRow)>> {
        tracing::debug!("[DEBUG SSTableReader::parse_block_entries_with_state_machine] Starting");
        tracing::debug!(
            "[DEBUG SSTableReader::parse_block_entries_with_state_machine] Data size: {} bytes",
            data.len()
        );

        let mut entries = Vec::new();
        let mut offset = 0;

        // Process multiple rows in the block
        while offset < data.len() {
            tracing::debug!("[DEBUG SSTableReader::parse_block_entries_with_state_machine] Processing at offset {}/{}", offset, data.len());

            // Create state machine with schema information if available
            let has_schema = self.get_table_schema(schema).is_some();
            tracing::debug!(
                "[DEBUG SSTableReader::parse_block_entries_with_state_machine] Has schema: {}",
                has_schema
            );

            let state_machine_result: Result<RowCellStateMachine> = if let Some(_schema) =
                self.get_table_schema(schema)
            {
                tracing::debug!(
                    "[DEBUG SSTableReader::parse_block_entries_with_state_machine] Schema found"
                );
                // Modern formats should use schema-aware decode (registered schema) with proper comparators
                // NOTE: Only V5_0NewBig and V5_0Bti use true 'oa' format with VInt encoding
                // V5_0DataFormat uses compressed legacy format and should NOT reach this code path
                match self.header.cassandra_version {
                    crate::parser::header::CassandraVersion::V5_0NewBig
                    | crate::parser::header::CassandraVersion::V5_0Bti => {
                        tracing::debug!("[DEBUG SSTableReader::parse_block_entries_with_state_machine] True V5.0 'oa' format with VInt encoding");
                        // V5.0 true 'oa' formats: Use schema-aware state machine with partition key comparators
                        // These use VInt-encoded partition key component counts and lengths

                        // Use schema-aware state machine for V5.0 formats
                        match _schema.get_partition_key_comparators() {
                            Ok(comparators) if !comparators.is_empty() => {
                                tracing::debug!("Creating schema-aware state machine with {} partition key comparators", comparators.len());
                                // Use first comparator for now (composite keys handled internally)
                                Ok(RowCellStateMachine::with_schema(
                                    _schema.clone(),
                                    comparators[0].clone(),
                                ))
                            }
                            Ok(_) => {
                                tracing::debug!("Schema has no partition key comparators, using basic state machine");
                                Ok(RowCellStateMachine::new())
                            }
                            Err(e) => {
                                tracing::debug!("Failed to get partition key comparators: {}, using basic state machine", e);
                                Ok(RowCellStateMachine::new())
                            }
                        }
                    }
                    _ => {
                        // Legacy formats can use basic state machine as last resort
                        #[cfg(feature = "legacy-heuristics")]
                        {
                            tracing::debug!("[DEBUG SSTableReader::parse_block_entries_with_state_machine] Legacy format, using basic state machine");
                            Ok(RowCellStateMachine::new())
                        }
                        #[cfg(not(feature = "legacy-heuristics"))]
                        {
                            tracing::debug!("[DEBUG SSTableReader::parse_block_entries_with_state_machine] Legacy format but legacy-heuristics not enabled");
                            Err(Error::Schema(
                                "Basic state machine parsing requires legacy-heuristics feature for legacy compatibility.".to_string()
                            ))
                        }
                    }
                }
            } else {
                tracing::debug!("[DEBUG SSTableReader::parse_block_entries_with_state_machine] No schema available from header");
                // No schema available from header - check format restrictions
                // NOTE: Only V5_0NewBig and V5_0Bti use true 'oa' format with VInt encoding
                match self.header.cassandra_version {
                    crate::parser::header::CassandraVersion::V5_0NewBig
                    | crate::parser::header::CassandraVersion::V5_0Bti => {
                        tracing::debug!("[DEBUG SSTableReader::parse_block_entries_with_state_machine] True V5.0 'oa' format without header schema");
                        // V5.0 true 'oa' format without header schema - use basic state machine
                        // Schema may be provided later by Database layer
                        // Note: These formats use VInt encoding and don't require legacy-heuristics
                        tracing::debug!("[DEBUG SSTableReader::parse_block_entries_with_state_machine] Using basic state machine for V5.0 'oa' format (no schema available)");
                        Ok(RowCellStateMachine::new())
                    }
                    _ => {
                        #[cfg(feature = "legacy-heuristics")]
                        {
                            tracing::debug!("[DEBUG SSTableReader::parse_block_entries_with_state_machine] Legacy format without schema, using basic state machine");
                            Ok(RowCellStateMachine::new())
                        }
                        #[cfg(not(feature = "legacy-heuristics"))]
                        {
                            tracing::debug!("[DEBUG SSTableReader::parse_block_entries_with_state_machine] No schema and legacy-heuristics not enabled");
                            Err(Error::Schema(
                                "Schema-less parsing requires legacy-heuristics feature for legacy compatibility.".to_string()
                            ))
                        }
                    }
                }
            };

            tracing::debug!("[DEBUG SSTableReader::parse_block_entries_with_state_machine] State machine creation result: {}",
                      if state_machine_result.is_ok() { "OK" } else { "ERROR" });

            let mut _state_machine: RowCellStateMachine = state_machine_result?;

            // Set UDT registry for UDT-aware parsing in collections (Issue #238)
            if let Some(ref registry) = self.udt_registry {
                _state_machine.set_udt_registry(registry.clone());
            }

            // Process data starting from current offset
            let remaining_data = &data[offset..];
            match _state_machine.process(remaining_data) {
                Ok(consumed) => {
                    if consumed == 0 {
                        // No progress made, avoid infinite loop
                        tracing::warn!(
                            "State machine made no progress at offset {}, stopping",
                            offset
                        );
                        break;
                    }

                    if _state_machine.is_complete() {
                        if let Some(parsed_row) = _state_machine.take_parsed_row() {
                            // Convert parsed row to entries
                            let converted_entries =
                                self.convert_parsed_row_to_entries(&parsed_row)?;
                            entries.extend(converted_entries);
                            tracing::debug!(
                                "Successfully parsed row with {} clustering rows",
                                parsed_row.clustering_rows.len()
                            );
                        }
                    } else if _state_machine.has_error() {
                        tracing::warn!(
                            "State machine error: {}",
                            _state_machine.error_message().unwrap_or("Unknown error")
                        );
                        // Try to continue with legacy parsing for this portion
                        break;
                    }

                    offset += consumed;
                }
                Err(e) => {
                    tracing::warn!("State machine processing error: {}", e);
                    // Fall back to legacy parsing
                    break;
                }
            }
        }

        // If state machine didn't handle all data, fall back to legacy parsing for remainder
        if offset < data.len() {
            tracing::debug!(
                "Falling back to legacy parsing for remaining {} bytes",
                data.len() - offset
            );
            let legacy_entries = self.parse_block_entries_legacy(&data[offset..], schema)?;
            entries.extend(legacy_entries);
        }

        Ok(entries)
    }

    /// Convert a parsed row from the state machine to entries
    pub(in crate::storage::sstable::reader) fn convert_parsed_row_to_entries(
        &self,
        parsed_row: &ParsedRow,
    ) -> Result<Vec<(TableId, RowKey, ScanRow)>> {
        let mut entries = Vec::new();

        // Create table ID from keyspace and table name (would be better to get from header)
        let table_id = TableId::new(format!(
            "{}_{}",
            self.header.keyspace, self.header.table_name
        ));

        // Create partition key
        let _partition_key = RowKey::new(parsed_row.partition_key.key_bytes.clone());

        // Add static row if present
        if let Some(ref static_row) = parsed_row.static_row {
            for (column_name, value) in &static_row.columns {
                // Create a compound key for static columns
                let mut static_key_bytes = parsed_row.partition_key.key_bytes.clone();
                static_key_bytes.extend_from_slice(b"#static#");
                static_key_bytes.extend_from_slice(column_name.as_bytes());

                let static_key = RowKey::new(static_key_bytes);
                // Live static-row value decoded by the state machine (supported
                // da BTI / oa path): must surface as a `ScanRow::Row` cell, not a
                // suppressed marker (issue #1334 / roborev H).
                entries.push((
                    table_id.clone(),
                    static_key,
                    live_cell_scan_row(column_name, value),
                ));
            }
        }

        // Add clustering rows
        for clustering_row in &parsed_row.clustering_rows {
            for (column_name, value) in &clustering_row.columns {
                // Create compound key: partition_key + clustering_key + column_name
                let mut compound_key_bytes = parsed_row.partition_key.key_bytes.clone();
                compound_key_bytes.extend_from_slice(&clustering_row.clustering_key);
                compound_key_bytes.extend_from_slice(column_name.as_bytes());

                let compound_key = RowKey::new(compound_key_bytes);
                // Live clustering-bound value decoded by the state machine
                // (supported da BTI / oa path): surface as a `ScanRow::Row` cell
                // so it reaches SELECT/export output (issue #1334 / roborev H).
                entries.push((
                    table_id.clone(),
                    compound_key,
                    live_cell_scan_row(column_name, value),
                ));
            }
        }

        Ok(entries)
    }

    /// Legacy parsing method for backward compatibility
    pub(in crate::storage::sstable::reader) fn parse_block_entries_legacy(
        &self,
        data: &[u8],
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Vec<(TableId, RowKey, ScanRow)>> {
        let mut entries = Vec::new();
        let mut offset = 0;

        // Enhanced partition data parsing for legacy formats
        while offset < data.len() {
            // Parse entry header with enhanced validation and error handling
            let (new_offset, table_id_len) = parse_vint_length(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Failed to parse table ID length at offset {}: {:?}",
                    offset, e
                ))
            })?;
            offset = data.len() - new_offset.len();

            // Validate table ID length to prevent buffer overrun
            if table_id_len > 256 || offset + table_id_len > data.len() {
                return Err(Error::corruption(format!(
                    "Invalid table ID length {} at offset {}, remaining: {}",
                    table_id_len,
                    offset,
                    data.len() - offset
                )));
            }

            // Parse table ID with enhanced validation for binary IDs
            let table_id_bytes = &data[offset..offset + table_id_len];
            let table_id = match String::from_utf8(table_id_bytes.to_vec()) {
                Ok(s) => TableId::new(s),
                Err(_) => {
                    // Handle binary table IDs in Cassandra 5.0
                    let hex_id = hex::encode(table_id_bytes);
                    TableId::new(format!("binary_{}", hex_id))
                }
            };
            offset += table_id_len;

            // Enhanced row key parsing with Cassandra 5.0 format support
            let (new_offset, key_len) = parse_vint_length(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Failed to parse key length at offset {}: {:?}",
                    offset, e
                ))
            })?;
            offset = data.len() - new_offset.len();

            // Validate key length
            if key_len > 65536 || offset + key_len > data.len() {
                return Err(Error::corruption(format!(
                    "Invalid key length {} at offset {}, remaining: {}",
                    key_len,
                    offset,
                    data.len() - offset
                )));
            }

            // Parse compound/composite keys properly
            let key_data = &data[offset..offset + key_len];
            let key = if key_len > 0 {
                self.parse_composite_key(key_data, schema)?
            } else {
                RowKey::new(Vec::new()) // Empty key
            };
            offset += key_len;

            // Enhanced column data extraction with proper type handling
            let (new_offset, value_len) = parse_vint_length(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Failed to parse value length at offset {}: {:?}",
                    offset, e
                ))
            })?;
            offset = data.len() - new_offset.len();

            // Handle different value encodings.
            let scan_row = if value_len == 0 {
                // Empty value → suppressible null marker.
                ScanRow::Marker(Value::Null)
            } else if value_len > 16777216 {
                // 16MB limit
                return Err(Error::corruption(format!(
                    "Value too large: {} bytes at offset {}",
                    value_len, offset
                )));
            } else if offset + value_len > data.len() {
                return Err(Error::corruption(format!(
                    "Incomplete value: need {} bytes at offset {}, have {}",
                    value_len,
                    offset,
                    data.len() - offset
                )));
            } else {
                let value_data = &data[offset..offset + value_len];
                // Enforce the modern-format contract (V5 without schema errors and
                // we propagate). When a schema DOES decode a typed value AND a
                // column name resolves from the key context, surface that decoded
                // value as a typed live cell (issue #1334 / roborev round 8 finding
                // 1 — never drop it). Only when no column name resolves do we hand
                // the raw bytes downstream with explicit RAW provenance so a
                // schema-aware consumer re-decodes and a no-schema consumer surfaces
                // a single "data" blob — no shape guess.
                let decoded =
                    self.parse_column_value_enhanced(value_data, &table_id, &key, schema)?;
                let column_name = self.extract_column_name_from_context(&table_id, &key);
                fallback_value_scan_row(column_name.as_deref(), decoded, value_data)
            };
            offset += value_len;

            entries.push((table_id, key, scan_row));
        }

        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::vint::encode_vuint;
    use crate::storage::sstable::row_cell_state_machine::{
        ClusteringRow, PartitionKey, RowHeader, StaticRow,
    };
    use crate::Config;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    // ========================================================================
    // Helper Functions
    // ========================================================================

    /// Helper to create test ParsedRow for convert_parsed_row_to_entries tests
    fn create_test_parsed_row(
        partition_key_bytes: Vec<u8>,
        static_columns: Option<HashMap<String, Value>>,
        clustering_rows: Vec<(Vec<u8>, HashMap<String, Value>)>,
    ) -> ParsedRow {
        ParsedRow {
            header: RowHeader {
                flags: 0,
                timestamp: 0,
                ttl: None,
                local_deletion_time: None,
            },
            partition_key: PartitionKey {
                component_count: 1,
                key_bytes: partition_key_bytes.clone(),
                components: vec![partition_key_bytes],
            },
            deletion_info: None,
            static_row: static_columns.map(|cols| StaticRow {
                column_count: cols.len(),
                columns: cols,
            }),
            clustering_rows: clustering_rows
                .into_iter()
                .map(|(key, cols)| ClusteringRow {
                    clustering_key: key,
                    timestamp: 0,
                    deletion_info: None,
                    columns: cols,
                })
                .collect(),
            clustering_key: None,
            cells: Vec::new(),
        }
    }

    /// Helper to create minimal SSTableReader for testing using real files
    /// Returns None if test data is not available
    async fn create_test_reader(keyspace: &str, table: &str) -> Option<SSTableReader> {
        let datasets_root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
        let keyspace_dir = PathBuf::from(datasets_root).join("sstables").join(keyspace);

        // Find a table directory that starts with the table name (format: table-uuid)
        let table_prefix = format!("{}-", table);
        let entries = std::fs::read_dir(&keyspace_dir).ok()?;

        for entry in entries.flatten() {
            let path = entry.path();
            let file_name = path.file_name()?.to_str()?;
            if file_name.starts_with(&table_prefix) {
                let data_file = std::fs::read_dir(&path)
                    .ok()?
                    .flatten()
                    .find(|e| {
                        e.file_name()
                            .to_str()
                            .map(|s| s.ends_with("-Data.db"))
                            .unwrap_or(false)
                    })?
                    .path();

                let config = Config::default();
                let platform = Arc::new(
                    crate::platform::Platform::new(&config)
                        .await
                        .expect("Failed to create Platform"),
                );
                return SSTableReader::open(&data_file, &config, platform)
                    .await
                    .ok();
            }
        }

        None
    }

    // ========================================================================
    // Group 1: Validation/Error Handling Tests
    // ========================================================================

    #[tokio::test]
    async fn test_invalid_table_id_length_exceeds_256() {
        let reader = match create_test_reader("test_basic", "simple_table").await {
            Some(r) => r,
            None => {
                eprintln!("Skipping test: CQLITE_DATASETS_ROOT not set or test data unavailable");
                return;
            }
        };

        // Create data with table_id_len = 257 (exceeds 256 byte limit)
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vuint(257)); // table_id_len
        data.extend_from_slice(&vec![0x41; 257]); // 257 bytes of 'A'

        let result = reader.parse_block_entries_legacy(&data, None);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Invalid table ID length"),
            "Expected error about invalid table ID length, got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_invalid_key_length_exceeds_65536() {
        let reader = match create_test_reader("test_basic", "simple_table").await {
            Some(r) => r,
            None => {
                eprintln!("Skipping test: CQLITE_DATASETS_ROOT not set or test data unavailable");
                return;
            }
        };

        // Create data with valid table_id but key_len = 65537
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vuint(5)); // table_id_len
        data.extend_from_slice(b"test.table"); // Actually 10 bytes, but we only read 5
        data.extend_from_slice(&encode_vuint(65537)); // key_len exceeds limit

        let result = reader.parse_block_entries_legacy(&data, None);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Invalid key length") || err_msg.contains("Invalid table ID length"),
            "Expected error about invalid key length, got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_invalid_value_length_exceeds_16mb() {
        // Use counters table which has TEXT primary key (compatible with "key1" test data)
        let reader = match create_test_reader("test_basic", "counters").await {
            Some(r) => r,
            None => {
                eprintln!("Skipping test: CQLITE_DATASETS_ROOT not set or test data unavailable");
                return;
            }
        };

        // Create data with valid table_id, key, but value_len > 16MB
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vuint(10)); // table_id_len
        data.extend_from_slice(b"test.table");
        data.extend_from_slice(&encode_vuint(4)); // key_len
        data.extend_from_slice(b"key1");
        data.extend_from_slice(&encode_vuint(16777217)); // value_len = 16MB + 1

        let result = reader.parse_block_entries_legacy(&data, None);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Value too large"),
            "Expected error about value too large, got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_truncated_data_table_id() {
        let reader = match create_test_reader("test_basic", "simple_table").await {
            Some(r) => r,
            None => {
                eprintln!("Skipping test: CQLITE_DATASETS_ROOT not set or test data unavailable");
                return;
            }
        };

        // Create data with table_id_len = 10 but only 5 bytes provided
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vuint(10)); // table_id_len
        data.extend_from_slice(b"short"); // Only 5 bytes

        let result = reader.parse_block_entries_legacy(&data, None);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Invalid table ID length"),
            "Expected error about invalid/incomplete table ID, got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_truncated_data_value() {
        // Use counters table which has TEXT primary key (compatible with "key1" test data)
        let reader = match create_test_reader("test_basic", "counters").await {
            Some(r) => r,
            None => {
                eprintln!("Skipping test: CQLITE_DATASETS_ROOT not set or test data unavailable");
                return;
            }
        };

        // Create data with value_len = 100 but only 10 bytes available
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vuint(10)); // table_id_len
        data.extend_from_slice(b"test.table");
        data.extend_from_slice(&encode_vuint(4)); // key_len
        data.extend_from_slice(b"key1");
        data.extend_from_slice(&encode_vuint(100)); // value_len
        data.extend_from_slice(b"shortvalue"); // Only 10 bytes

        let result = reader.parse_block_entries_legacy(&data, None);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Incomplete value"),
            "Expected error about incomplete value, got: {}",
            err_msg
        );
    }

    // ========================================================================
    // Group 2: Row Conversion Tests
    // ========================================================================

    #[tokio::test]
    async fn test_convert_parsed_row_static_columns() {
        let reader = match create_test_reader("test_basic", "simple_table").await {
            Some(r) => r,
            None => {
                eprintln!("Skipping test: CQLITE_DATASETS_ROOT not set or test data unavailable");
                return;
            }
        };

        // Create ParsedRow with static columns
        let mut static_cols = HashMap::new();
        static_cols.insert("static_col1".to_string(), Value::text("value1".to_string()));
        static_cols.insert("static_col2".to_string(), Value::Integer(42));

        let parsed_row =
            create_test_parsed_row(b"partition_key".to_vec(), Some(static_cols), vec![]);

        let entries = reader
            .convert_parsed_row_to_entries(&parsed_row)
            .expect("Failed to convert parsed row");

        assert_eq!(entries.len(), 2, "Should have 2 static column entries");

        // Verify static marker in keys
        for (_, key, _) in &entries {
            let key_bytes = key.as_bytes();
            assert!(
                key_bytes.windows(8).any(|window| window == b"#static#"),
                "Key should contain #static# marker"
            );
        }
    }

    #[tokio::test]
    async fn test_convert_parsed_row_clustering_rows() {
        let reader = match create_test_reader("test_basic", "simple_table").await {
            Some(r) => r,
            None => {
                eprintln!("Skipping test: CQLITE_DATASETS_ROOT not set or test data unavailable");
                return;
            }
        };

        // Create ParsedRow with one clustering row
        let mut cols = HashMap::new();
        cols.insert("col1".to_string(), Value::text("text_value".to_string()));
        cols.insert("col2".to_string(), Value::Integer(123));

        let clustering_rows = vec![(b"clustering_key1".to_vec(), cols)];
        let parsed_row = create_test_parsed_row(b"partition_key".to_vec(), None, clustering_rows);

        let entries = reader
            .convert_parsed_row_to_entries(&parsed_row)
            .expect("Failed to convert parsed row");

        assert_eq!(entries.len(), 2, "Should have 2 column entries");

        // Verify keys contain partition + clustering + column name
        for (_, key, _) in &entries {
            let key_bytes = key.as_bytes();
            assert!(
                key_bytes.starts_with(b"partition_key"),
                "Key should start with partition key"
            );
        }
    }

    #[tokio::test]
    async fn test_convert_parsed_row_multiple_clustering() {
        let reader = match create_test_reader("test_basic", "simple_table").await {
            Some(r) => r,
            None => {
                eprintln!("Skipping test: CQLITE_DATASETS_ROOT not set or test data unavailable");
                return;
            }
        };

        // Create ParsedRow with multiple clustering rows
        let mut cols1 = HashMap::new();
        cols1.insert("col1".to_string(), Value::Integer(1));

        let mut cols2 = HashMap::new();
        cols2.insert("col1".to_string(), Value::Integer(2));

        let clustering_rows = vec![
            (b"clustering1".to_vec(), cols1),
            (b"clustering2".to_vec(), cols2),
        ];
        let parsed_row = create_test_parsed_row(b"pk".to_vec(), None, clustering_rows);

        let entries = reader
            .convert_parsed_row_to_entries(&parsed_row)
            .expect("Failed to convert parsed row");

        assert_eq!(
            entries.len(),
            2,
            "Should have 2 entries from 2 clustering rows"
        );
    }

    #[tokio::test]
    async fn test_convert_parsed_row_empty() {
        let reader = match create_test_reader("test_basic", "simple_table").await {
            Some(r) => r,
            None => {
                eprintln!("Skipping test: CQLITE_DATASETS_ROOT not set or test data unavailable");
                return;
            }
        };

        // Create empty ParsedRow
        let parsed_row = create_test_parsed_row(b"partition_key".to_vec(), None, vec![]);

        let entries = reader
            .convert_parsed_row_to_entries(&parsed_row)
            .expect("Failed to convert parsed row");

        assert_eq!(
            entries.len(),
            0,
            "Empty ParsedRow should produce no entries"
        );
    }

    /// Issue #1334 / roborev H: a live clustering-bound value decoded by the
    /// state machine and turned into an entry by `convert_parsed_row_to_entries`
    /// MUST reach user-visible output. Before the fix that value was wrapped as
    /// `ScanRow::Marker`, which `build_row_from_scan` suppresses via
    /// `into_cells()` — so the column silently disappeared from SELECT/export.
    /// This drives the real conversion path and asserts the value surfaces; it
    /// would fail (build_row_from_scan → None → panic) under marker-suppression.
    #[cfg(feature = "state_machine")]
    #[tokio::test]
    async fn convert_parsed_row_live_value_surfaces_via_build_row_from_scan() {
        let reader = match create_test_reader("test_basic", "simple_table").await {
            Some(r) => r,
            None => {
                eprintln!("Skipping test: CQLITE_DATASETS_ROOT not set or test data unavailable");
                return;
            }
        };

        let mut cols = HashMap::new();
        cols.insert("name".to_string(), Value::text("alice".to_string()));
        let clustering_rows = vec![(b"ck".to_vec(), cols)];
        let parsed_row = create_test_parsed_row(b"pk".to_vec(), None, clustering_rows);

        let entries = reader
            .convert_parsed_row_to_entries(&parsed_row)
            .expect("convert must succeed");
        assert_eq!(entries.len(), 1, "one live clustering column -> one entry");

        let (_, key, scan_row) = entries.into_iter().next().unwrap();
        assert!(
            matches!(scan_row, ScanRow::Row(_)),
            "a live decoded value must be a ScanRow::Row, not a suppressed Marker"
        );

        // Interned Arc<str> cell name carried straight through (no String realloc).
        let row = crate::query::build_row_from_scan(key, scan_row, &[], None)
            .expect("roborev H: a live row must NOT be suppressed by into_cells()");
        assert_eq!(
            row.values.get("name"),
            Some(&Value::text("alice".to_string())),
            "the real column value must survive the scan->query carrier"
        );
    }

    /// A genuinely absent cell (`Value::Null`) still maps to a suppressed
    /// `ScanRow::Marker` — the helper must only surface REAL values, so
    /// null/tombstone placeholders remain hidden from user output.
    #[test]
    fn live_cell_scan_row_suppresses_null_but_surfaces_real_value() {
        assert!(
            matches!(
                live_cell_scan_row("name", &Value::Null),
                ScanRow::Marker(Value::Null)
            ),
            "a null cell must stay a suppressed marker"
        );
        match live_cell_scan_row("name", &Value::text("bob".to_string())) {
            ScanRow::Row(cells) => {
                assert_eq!(cells.len(), 1);
                assert_eq!(&*cells[0].0, "name", "interned real column name");
                assert_eq!(cells[0].1, Value::text("bob".to_string()));
            }
            ScanRow::RawRow(_) => {
                panic!("classify_cell never yields a RawRow; a decoded value must be a live Row")
            }
            ScanRow::Marker(_) => panic!("a real value must be a live Row, not a marker"),
        }
    }

    /// Issue #1334 / roborev round 8 finding 1: when the legacy
    /// `parse_block_entries*` fallback SCHEMA-DECODES a typed value AND a column
    /// name resolves from the key context, that decoded value must surface as a
    /// TYPED `ScanRow::Row` cell — not be discarded and re-emitted as a synthetic
    /// `"data"` blob. Before the fix the site decoded then discarded the value and
    /// always emitted `ScanRow::RawRow`, so a schema-aware non-stitching scan lost
    /// the typed column value.
    #[cfg(feature = "state_machine")]
    #[test]
    fn fallback_decoded_value_with_name_surfaces_as_typed_row() {
        let decoded = Value::Integer(42);
        let raw = vec![0x00, 0x00, 0x00, 0x2a];

        let scan_row = fallback_value_scan_row(Some("age"), decoded.clone(), &raw);
        match &scan_row {
            ScanRow::Row(cells) => {
                assert_eq!(cells.len(), 1, "one decoded cell -> one row cell");
                assert_eq!(&*cells[0].0, "age", "resolved column name is carried");
                assert_eq!(
                    cells[0].1, decoded,
                    "the DECODED typed value is not dropped"
                );
            }
            other => panic!("expected a typed ScanRow::Row, got {:?}", other),
        }

        // Surfaced end-to-end through the public consumer as the typed column,
        // NOT a "data" blob (the pre-fix RawRow shape).
        let row =
            crate::query::build_row_from_scan(RowKey::new(b"pk".to_vec()), scan_row, &[], None)
                .expect("a decoded typed row must not be suppressed");
        assert_eq!(
            row.values.get("age"),
            Some(&Value::Integer(42)),
            "the typed value must surface under its resolved column name"
        );
        assert!(
            !row.values.contains_key("data"),
            "a decoded typed value must NOT degrade to a synthetic \"data\" blob"
        );
    }

    /// Companion to the finding-1 fix: with NO resolvable column name the bytes
    /// genuinely remain UNDECODED, so the fallback carries them with explicit RAW
    /// provenance (the true pre-#1334 raw-blob fallback), and a genuine null stays
    /// a suppressed marker.
    #[test]
    fn fallback_without_name_stays_raw_and_null_is_marker() {
        let raw = vec![0xde, 0xad, 0xbe, 0xef];
        assert_eq!(
            fallback_value_scan_row(None, Value::blob(raw.clone()), &raw),
            ScanRow::RawRow(raw.clone()),
            "no resolvable column name -> undecoded RAW provenance"
        );
        assert!(
            matches!(
                fallback_value_scan_row(Some("age"), Value::Null, &raw),
                ScanRow::Marker(Value::Null)
            ),
            "a genuine null decodes to a suppressed marker"
        );
    }

    /// Issue #1334: the legacy `parse_block_entries*` fallback push sites carry a
    /// row's RAW value bytes with explicit `ScanRow::RawRow` provenance (never a
    /// suppressible marker). A no-schema consumer surfaces those bytes as the
    /// `"data"` column via the public `build_row_from_scan` — the exact pre-#1334
    /// bare-`Value::Blob` behavior — so the legacy blob can never silently
    /// disappear from SELECT/export.
    #[cfg(feature = "state_machine")]
    #[test]
    fn raw_fallback_surfaces_via_build_row_from_scan() {
        let key = RowKey::new(b"pk".to_vec());
        let raw = vec![0xde, 0xad, 0xbe, 0xef];

        // The exact carrier the two `parse_block_entries*` fallback sites now emit.
        let scan_row = ScanRow::RawRow(raw.clone());
        assert!(
            matches!(scan_row, ScanRow::RawRow(_)),
            "the raw fallback must carry explicit RawRow provenance, never a Marker"
        );

        let row = crate::query::build_row_from_scan(key, scan_row, &[], None)
            .expect("a raw fallback row must NOT be suppressed by into_cells()");
        assert_eq!(
            row.values.get("data"),
            Some(&Value::Blob(raw.into())),
            "the raw fallback bytes must surface as the \"data\" column in SELECT/export"
        );
    }

    // ========================================================================
    // Group 3: Format Dispatch Tests (Integration)
    // ========================================================================

    #[tokio::test]
    async fn test_format_dispatch_row_decoder() {
        let reader = match create_test_reader("test_basic", "simple_table").await {
            Some(r) => r,
            None => {
                eprintln!("Skipping test: CQLITE_DATASETS_ROOT not set or test data unavailable");
                return;
            }
        };

        // Verify this is V5CompressedLegacy format
        let data_format = reader.header.cassandra_version.data_format();
        assert_eq!(
            data_format,
            crate::parser::header::DataFormat::V5CompressedLegacy,
            "Expected V5CompressedLegacy format"
        );

        // Create minimal valid V5CompressedLegacy block data
        // Format: [flags][key_len][key_bytes][deletion_time][unknown_8bytes][row_data...]
        let mut block_data = Vec::new();
        block_data.push(0x00); // flags
        block_data.push(0x04); // key_len = 4
        block_data.extend_from_slice(b"key1"); // partition key
        block_data.extend_from_slice(&[0, 0, 0, 0]); // deletion_time
        block_data.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]); // unknown 8-byte field

        // Compress the synthetic block with the reader's declared algorithm so the
        // fail-closed decompress (issue #2165) succeeds and the format dispatch this
        // test asserts on is actually reached. A raw/uncompressed buffer would now
        // error on decompress BEFORE any dispatch, silently voiding this coverage.
        let compression_reader = reader
            .compression_reader
            .as_ref()
            .expect("simple_table should be compressed");
        let compression =
            Compression::new(*compression_reader.algorithm()).expect("valid compression algorithm");
        let compressed = compression
            .compress(&block_data)
            .expect("compress synthetic block");

        // Try parsing - should route to V5CompressedLegacyParser
        let result = reader.parse_block_entries(&compressed, None, false);

        // We expect either success or a specific parsing error (not a dispatch error)
        match result {
            Ok(_) => {
                // Success is fine
            }
            Err(e) => {
                let err_msg = e.to_string();
                // Decompress must have SUCCEEDED — otherwise dispatch was never reached
                // and this test would pass vacuously (issue #2165: fail-closed decompress
                // errors before dispatch on a raw buffer).
                assert!(
                    !err_msg.contains("decompress"),
                    "block must decompress so dispatch is reached, got decompress error: {}",
                    err_msg
                );
                // Should not be a format dispatch error
                assert!(
                    !err_msg.contains("Unknown format") && !err_msg.contains("Not implemented"),
                    "Should route to V5CompressedLegacyParser, got: {}",
                    err_msg
                );
            }
        }
    }

    #[tokio::test]
    async fn test_format_dispatch_legacy_vint() {
        // Use counters table which has TEXT primary key (compatible with "key1" test data)
        let reader = match create_test_reader("test_basic", "counters").await {
            Some(r) => r,
            None => {
                eprintln!("Skipping test: CQLITE_DATASETS_ROOT not set or test data unavailable");
                return;
            }
        };

        // Create VInt-based legacy format data
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vuint(10)); // table_id_len
        data.extend_from_slice(b"test.table");
        data.extend_from_slice(&encode_vuint(4)); // key_len
        data.extend_from_slice(b"key1");
        data.extend_from_slice(&encode_vuint(0)); // empty value

        // Use legacy parser directly to test VInt fallback
        let result = reader.parse_block_entries_legacy(&data, None);

        // Should successfully parse with VInt format
        assert!(
            result.is_ok(),
            "Legacy VInt parsing should succeed, got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_decompression_failure_is_fail_closed() {
        // Issue #2165 / G2: a compressed block that fails to decompress must FAIL
        // CLOSED (corruption error) — it must NOT silently fall back to parsing the
        // raw compressed bytes as row data (no-heuristics, #28). This flips the former
        // `test_decompression_fallback_on_failure`, which asserted the silent fallback.
        let reader = match create_test_reader("test_basic", "compression_test_table").await {
            Some(r) => r,
            None => {
                eprintln!("Skipping test: CQLITE_DATASETS_ROOT not set or test data unavailable");
                return;
            }
        };

        // Verify this table has compression (drives the decompress branch)
        assert!(
            reader.compression_reader.is_some(),
            "Expected compression_test_table to have compression"
        );

        // Bytes that cannot be decompressed by the declared algorithm.
        let invalid_compressed_data = vec![0xFF; 100];

        let result = reader.parse_block_entries(&invalid_compressed_data, None, false);

        // FAIL CLOSED: the DECOMPRESS path must return a corruption error (not just
        // any error), and no rows may be produced from the raw bytes.
        match result {
            Ok(rows) => panic!(
                "decompress failure must fail closed with an error, got {} rows",
                rows.len()
            ),
            Err(e) => {
                assert!(
                    matches!(e, Error::Corruption(_)),
                    "decompress failure must surface as Error::Corruption, got: {:?}",
                    e
                );
                let err_msg = e.to_string();
                assert!(
                    err_msg.contains("decompress"),
                    "corruption error should name the decompress failure, got: {}",
                    err_msg
                );
            }
        }
    }

    #[tokio::test]
    async fn test_empty_block_on_compressed_reader_is_noop() {
        // Issue #2165: `read_next_block` can yield an empty block; on a COMPRESSED
        // reader the empty buffer must NOT reach decompress (which would fail closed
        // and abort the whole scan). It must short-circuit to Ok(empty entries) so
        // the enclosing scan continues.
        let reader = match create_test_reader("test_basic", "compression_test_table").await {
            Some(r) => r,
            None => {
                eprintln!("Skipping test: CQLITE_DATASETS_ROOT not set or test data unavailable");
                return;
            }
        };
        assert!(
            reader.compression_reader.is_some(),
            "Expected compression_test_table to have compression"
        );

        let empty: Vec<u8> = Vec::new();
        let result = reader.parse_block_entries(&empty, None, false);

        assert!(
            result.is_ok(),
            "empty block on a compressed reader must be a no-op, got: {:?}",
            result
        );
        assert_eq!(
            result.unwrap().len(),
            0,
            "empty block must yield zero entries"
        );
    }

    // ========================================================================
    // Group 4: Edge Cases
    // ========================================================================

    #[tokio::test]
    async fn test_empty_block_data() {
        let reader = match create_test_reader("test_basic", "simple_table").await {
            Some(r) => r,
            None => {
                eprintln!("Skipping test: CQLITE_DATASETS_ROOT not set or test data unavailable");
                return;
            }
        };

        let empty_data = Vec::new();
        let result = reader.parse_block_entries_legacy(&empty_data, None);

        assert!(result.is_ok(), "Empty block should parse successfully");
        let entries = result.unwrap();
        assert_eq!(entries.len(), 0, "Empty block should have no entries");
    }

    #[tokio::test]
    async fn test_binary_table_id_hex_encoding() {
        // Use counters table which has TEXT primary key (compatible with "key1" test data)
        let reader = match create_test_reader("test_basic", "counters").await {
            Some(r) => r,
            None => {
                eprintln!("Skipping test: CQLITE_DATASETS_ROOT not set or test data unavailable");
                return;
            }
        };

        // Create data with non-UTF8 table_id
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vuint(4)); // table_id_len
        data.extend_from_slice(&[0xFF, 0xFE, 0xFD, 0xFC]); // Invalid UTF-8
        data.extend_from_slice(&encode_vuint(4)); // key_len
        data.extend_from_slice(b"key1");
        data.extend_from_slice(&encode_vuint(0)); // empty value

        let result = reader.parse_block_entries_legacy(&data, None);

        assert!(
            result.is_ok(),
            "Binary table ID should be handled, got: {:?}",
            result
        );

        let entries = result.unwrap();
        assert_eq!(entries.len(), 1, "Should have parsed 1 entry");

        let (table_id, _, _) = &entries[0];
        let table_name = table_id.name();
        assert!(
            table_name.starts_with("binary_"),
            "Binary table ID should have 'binary_' prefix, got: {}",
            table_name
        );
        assert!(
            table_name.contains("fffefdfc"),
            "Binary table ID should contain hex encoding, got: {}",
            table_name
        );
    }

    #[tokio::test]
    async fn test_empty_key_handling() {
        let reader = match create_test_reader("test_basic", "simple_table").await {
            Some(r) => r,
            None => {
                eprintln!("Skipping test: CQLITE_DATASETS_ROOT not set or test data unavailable");
                return;
            }
        };

        // Create data with empty key (key_len = 0)
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vuint(10)); // table_id_len
        data.extend_from_slice(b"test.table");
        data.extend_from_slice(&encode_vuint(0)); // key_len = 0 (empty key)
        data.extend_from_slice(&encode_vuint(0)); // empty value

        let result = reader.parse_block_entries_legacy(&data, None);

        assert!(
            result.is_ok(),
            "Empty key should be handled, got: {:?}",
            result
        );

        let entries = result.unwrap();
        assert_eq!(entries.len(), 1, "Should have parsed 1 entry");

        let (_, key, _) = &entries[0];
        assert_eq!(
            key.as_bytes().len(),
            0,
            "Key should be empty (0 bytes), got {} bytes",
            key.as_bytes().len()
        );
    }
}
